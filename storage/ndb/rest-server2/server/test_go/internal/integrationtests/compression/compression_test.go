/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// Package compression checks the Content-Type and Content-Encoding headers
// of the six handlers that answer with JSON and let REST.UseCompression
// decide whether Drogon may gzip the body: pk-read, batch, batchwrite,
// batchdelete, feature_store and batch_feature_store (RONDB-813,
// RONDB-1160).
//
//   - Content-Type is always "application/json; charset=utf-8".
//   - With REST.UseCompression on, a client that accepts gzip gets
//     Content-Encoding: gzip.
//   - With REST.UseCompression off, the body is not encoded. Nor is a
//     feature vector with a complex (Avro) feature, whatever the setting.
//
// Every request here gets a response of at least 1024 bytes before any
// encoding. Drogon never compresses a smaller body, so a smaller one would
// pass the "not encoded" checks without testing them.
//
// The tests take REST.UseCompression from the config file in
// RDRS_CONFIG_FILE, the file the server under test was started with. The
// normal pass over all packages runs them against a server with the default
// (on). mysql-test/suite/rdrs2-golang/t/compression_off.test runs them
// against a server with the setting off.
package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	bfshelper "hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore"
	fshelper "hopsworks.ai/rdrs2/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

const (
	// What Drogon sends for CT_APPLICATION_JSON.
	jsonContentType = "application/json; charset=utf-8"

	// Drogon compresses no body shorter than this
	// (HttpResponseImpl::shouldBeCompressed).
	minCompressibleBody = 1024

	// rdrs_bench.table_3 has 100 VARCHAR columns next to its key id0. One
	// row read with no columns named is about 1.7 KiB of JSON.
	wideTable = "table_3"

	// Rows of db004.int_table that the batchwrite and batchdelete requests
	// write and delete. Each operation carries a 64 character operation id
	// that the response repeats, which puts the response over 1 KiB.
	intTable   = "int_table"
	firstRowID = 1160000
	numRows    = 32

	// Feature view sample_blob v1 serves fsdb002.sample_blob_1 (id bigint,
	// payload longblob), seeded with one row whose payload is NULL. The
	// payload is a plain binary feature: base64 in the response and no Avro
	// schema, so the handler leaves compression allowed.
	blobFeatureView = "sample_blob"
	blobTable       = "sample_blob_1"
	blobColumn      = "payload"
	blobRowID       = 1
	blobBytes       = 2048

	// Feature view sample_complex_type_512 v1 serves
	// fsdb002.sample_complex_type_512_1 (id varchar, embedding varbinary).
	// The embedding is a complex feature, an Avro array of 512 longs.
	complexFeatureView = "sample_complex_type_512"
	complexTable       = "sample_complex_type_512_1"
)

type request struct {
	verb string
	url  string
	body string
}

// handlerCase is one handler and a request to it whose response is at least
// minCompressibleBody bytes. The function sets up what the request needs.
type handlerCase struct {
	handler string
	request func(t *testing.T) request
}

// The requests where REST.UseCompression alone decides on compression.
//
// The scan handler is not here: on this branch it sets no content type and
// does not look at REST.UseCompression. It needs a case once it does.
var plainCases = []handlerCase{
	{"pk-read", pkReadRequest},
	{"batch", batchReadRequest},
	{"batchwrite", batchWriteRequest},
	{"batchdelete", batchDeleteRequest},
	{"feature_store", featureStoreRequest},
	{"batch_feature_store", batchFeatureStoreRequest},
}

// The requests for a feature vector with a complex feature.
var complexCases = []handlerCase{
	{"feature_store", complexFeatureStoreRequest},
	{"batch_feature_store", complexBatchFeatureStoreRequest},
}

func TestGzipWithUseCompression(t *testing.T) {
	if !config.GetAll().REST.UseCompression {
		t.Skip("the server runs with REST.UseCompression off")
	}
	runCases(t, plainCases, true)
}

func TestNoGzipWithoutUseCompression(t *testing.T) {
	if config.GetAll().REST.UseCompression {
		t.Skip("the server runs with REST.UseCompression on " +
			"(run via mtr rdrs2-golang.compression_off)")
	}
	runCases(t, plainCases, false)
}

// With REST.UseCompression on, TestGzipWithUseCompression shows that the
// same two handlers do gzip a feature vector without a complex feature.
func TestNoGzipForComplexFeature(t *testing.T) {
	runCases(t, complexCases, false)
}

func runCases(t *testing.T, cases []handlerCase, wantGzip bool) {
	client := newClient(t)
	for _, c := range cases {
		t.Run(c.handler, func(t *testing.T) {
			header, body := send(t, client, c.request(t))
			checkResponse(t, header, body, wantGzip)
		})
	}
}

// newClient returns a client that leaves compression to the test. Go's
// transport otherwise asks for gzip itself, decompresses the body and
// removes Content-Encoding from the response.
func newClient(t *testing.T) *http.Client {
	t.Helper()
	tlsConfig, err := testutils.GetClientTLSConfig()
	if err != nil {
		t.Fatalf("cannot get the TLS config of the client: %v", err)
	}
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:    tlsConfig,
			DisableCompression: true,
		},
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

// send sends r as a client that accepts gzip. It returns the headers and
// the body of the response as the server sent them.
func send(t *testing.T, client *http.Client, r request) (http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(r.verb, r.url, strings.NewReader(r.body))
	if err != nil {
		t.Fatalf("cannot create the request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	if config.GetAll().Security.APIKey.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", r.verb, r.url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: cannot read the response body: %v", r.verb, r.url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: status %d, want 200\nrequest body: %s\nresponse body: %s",
			r.verb, r.url, resp.StatusCode, r.body, body)
	}
	return resp.Header, body
}

// checkResponse checks that a response is JSON by its Content-Type and by
// its body, and that the body is gzipped exactly when wantGzip.
func checkResponse(t *testing.T, header http.Header, body []byte, wantGzip bool) {
	t.Helper()
	contentTypes := header.Values("Content-Type")
	if len(contentTypes) != 1 || contentTypes[0] != jsonContentType {
		t.Errorf("Content-Type: got %q, want %q", contentTypes, jsonContentType)
	}

	encodings := header.Values("Content-Encoding")
	gzipped := len(encodings) == 1 && encodings[0] == "gzip"
	if len(encodings) != 0 && !gzipped {
		t.Fatalf("Content-Encoding: got %q, want gzip or no such header", encodings)
	}
	if gzipped {
		body = gunzip(t, body)
	}

	// This one is about the request the test sends, not about the server.
	if len(body) < minCompressibleBody {
		t.Fatalf("the response body is %d bytes. It must be at least %d: "+
			"Drogon compresses nothing smaller, so this request cannot tell "+
			"whether the handler allows compression", len(body), minCompressibleBody)
	}
	if !json.Valid(body) {
		t.Errorf("the response body is not JSON: %.200q", body)
	}

	if wantGzip && !gzipped {
		t.Errorf("the response has no Content-Encoding, want gzip (%d byte body)", len(body))
	}
	if !wantGzip && gzipped {
		t.Errorf("the response has Content-Encoding: gzip, want no such header")
	}
}

func gunzip(t *testing.T, data []byte) []byte {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Content-Encoding is gzip, but the body is not: %v", err)
	}
	defer reader.Close()
	plain, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Content-Encoding is gzip, but the body does not decompress: %v", err)
	}
	return plain
}

func marshal(t *testing.T, v interface{}) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("cannot marshal the request body: %v", err)
	}
	return string(data)
}

func pkReadRequest(t *testing.T) request {
	body := api.PKReadBody{
		Filters:     testclient.NewFiltersKVs("id0", 0),
		OperationID: testclient.NewOperationID(64),
	}
	return request{
		verb: config.PK_HTTP_VERB,
		url:  testutils.NewPKReadURL(testdbs.Benchmark, wideTable),
		body: marshal(t, body),
	}
}

func batchReadRequest(t *testing.T) request {
	method := config.PK_HTTP_VERB
	relativeURL := testutils.NewBatchPKReadURL(testdbs.Benchmark, wideTable)
	operations := make([]api.BatchSubOp, 4)
	for i := range operations {
		operations[i] = api.BatchSubOp{
			Method:      &method,
			RelativeURL: &relativeURL,
			Body: &api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", i),
				OperationID: testclient.NewOperationID(64),
			},
		}
	}
	return request{
		verb: config.BATCH_HTTP_VERB,
		url:  testutils.NewBatchReadURL(),
		body: marshal(t, api.BatchOpRequest{Operations: &operations}),
	}
}

// writeRowsRequest writes the numRows rows of intTable.
func writeRowsRequest(t *testing.T) request {
	method := config.PK_HTTP_VERB
	relativeURL := testutils.NewBatchPKWriteURL(testdbs.DB004, intTable,
		config.PK_WRITE_OPERATION)
	operations := make([]api.BatchWriteSubOp, numRows)
	for i := range operations {
		id := firstRowID + i
		operations[i] = api.BatchWriteSubOp{
			Method:      &method,
			RelativeURL: &relativeURL,
			Body: &api.PKWriteBody{
				Filters:      testclient.NewFiltersKVs("id0", id, "id1", id),
				WriteColumns: testclient.NewWriteColumnsKVs("col0", i, "col1", i),
				OperationID:  testclient.NewOperationID(64),
			},
		}
	}
	return request{
		verb: config.BATCH_HTTP_VERB,
		url:  testutils.NewBatchWriteURL(),
		body: marshal(t, api.BatchWriteOpRequest{Operations: &operations}),
	}
}

// deleteRowsRequest deletes the numRows rows of intTable.
func deleteRowsRequest(t *testing.T) request {
	method := config.PK_HTTP_VERB
	relativeURL := testutils.NewBatchPKDeleteURL(testdbs.DB004, intTable)
	operations := make([]api.BatchDeleteSubOp, numRows)
	for i := range operations {
		id := firstRowID + i
		operations[i] = api.BatchDeleteSubOp{
			Method:      &method,
			RelativeURL: &relativeURL,
			Body: &api.PKDeleteBody{
				Filters:     testclient.NewFiltersKVs("id0", id, "id1", id),
				OperationID: testclient.NewOperationID(64),
			},
		}
	}
	return request{
		verb: config.BATCH_DELETE_HTTP_VERB,
		url:  testutils.NewBatchDeleteURL(),
		body: marshal(t, api.BatchDeleteOpRequest{Operations: &operations}),
	}
}

// sendPlain sends r through the ordinary test client and requires 200. It
// is for the requests that only set up or clean up.
func sendPlain(t *testing.T, r request) {
	t.Helper()
	testclient.SendHttpRequest(t, r.verb, r.url, r.body, "", http.StatusOK)
}

func batchWriteRequest(t *testing.T) request {
	// The rows are gone again when the subtest ends.
	t.Cleanup(func() { sendPlain(t, deleteRowsRequest(t)) })
	return writeRowsRequest(t)
}

func batchDeleteRequest(t *testing.T) request {
	sendPlain(t, writeRowsRequest(t))
	return deleteRowsRequest(t)
}

// setBlobFeature gives the row of blobTable a payload of blobBytes bytes
// until the subtest ends, which makes its feature vector over 1 KiB.
func setBlobFeature(t *testing.T) {
	t.Helper()
	testutils.SetBlob(t, testdbs.FSDB002, blobTable, blobColumn, blobRowID,
		testutils.DeterministicBytes(blobBytes, 1))
	t.Cleanup(func() {
		testutils.SetBlobNull(t, testdbs.FSDB002, blobTable, blobColumn, blobRowID)
	})
}

func featureStoreRequest(t *testing.T) request {
	setBlobFeature(t)
	fsReq := fshelper.CreateFeatureStoreRequest(
		testdbs.FSDB002,
		blobFeatureView,
		1,
		[]string{"id"},
		[]interface{}{[]byte("1")},
		nil,
		nil,
	)
	return request{
		verb: config.FEATURE_STORE_HTTP_VERB,
		url:  testutils.NewFeatureStoreURL(),
		body: fsReq.String(),
	}
}

func batchFeatureStoreRequest(t *testing.T) request {
	setBlobFeature(t)
	fsReq := bfshelper.CreateBatchFeatureStoreRequest(
		testdbs.FSDB002,
		blobFeatureView,
		1,
		[]string{"id"},
		[][]interface{}{{[]byte("1")}},
		nil,
		nil,
	)
	return request{
		verb: config.FEATURE_STORE_HTTP_VERB,
		url:  testutils.NewBatchFeatureStoreURL(),
		body: fsReq.String(),
	}
}

// complexEntries returns the names of the key columns of complexTable and
// the key values of n of its rows.
func complexEntries(t *testing.T, n int) ([]string, [][]interface{}) {
	t.Helper()
	rows, pks, cols, err := fshelper.GetSampleDataN(testdbs.FSDB002, complexTable, n)
	if err != nil {
		t.Fatalf("cannot get sample data: %v", err)
	}
	if len(rows) != n {
		t.Fatalf("got %d rows of %s, want %d", len(rows), complexTable, n)
	}
	return pks, *bfshelper.GetPkValues(&rows, &pks, &cols)
}

func complexFeatureStoreRequest(t *testing.T) request {
	pks, values := complexEntries(t, 1)
	fsReq := fshelper.CreateFeatureStoreRequest(
		testdbs.FSDB002,
		complexFeatureView,
		1,
		pks,
		values[0],
		nil,
		nil,
	)
	return request{
		verb: config.FEATURE_STORE_HTTP_VERB,
		url:  testutils.NewFeatureStoreURL(),
		body: fsReq.String(),
	}
}

func complexBatchFeatureStoreRequest(t *testing.T) request {
	pks, values := complexEntries(t, 2)
	fsReq := bfshelper.CreateBatchFeatureStoreRequest(
		testdbs.FSDB002,
		complexFeatureView,
		1,
		pks,
		values,
		nil,
		nil,
	)
	return request{
		verb: config.FEATURE_STORE_HTTP_VERB,
		url:  testutils.NewBatchFeatureStoreURL(),
		body: fsReq.String(),
	}
}
