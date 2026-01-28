/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026, 2026 Hopsworks AB
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

package batchdelete

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

func batchDeleteRESTTest(
	t testing.TB,
	testInfo api.BatchDeleteOperationTestInfo,
) {
	client := testutils.SetupHttpClient(t)
	batchDeleteRESTTestWithClient(t, client, testInfo)
}

func batchDeleteRESTTestWithClient(
	t testing.TB,
	client *http.Client,
	testInfo api.BatchDeleteOperationTestInfo,
) {
	httpCode, response := sendHttpBatchDeleteRequest(t, client, testInfo)

	if log.IsTrace() {
		log.Tracef("Http Response %s\n", string(response))
	}

	if httpCode == http.StatusOK {
		validateBatchDeleteResponseHttp(t, testInfo, response)
	}
}

func sendHttpBatchDeleteRequest(
	t testing.TB,
	client *http.Client,
	testInfo api.BatchDeleteOperationTestInfo,
) (httpCode int, response []byte) {
	subOps := []api.BatchDeleteSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchDeleteOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}
	url := testutils.NewBatchDeleteURL()
	httpCode, response = testclient.SendHttpRequestWithClient(
		t,
		client,
		config.BATCH_HTTP_VERB,
		url,
		string(body),
		testInfo.ErrMsgContains,
		testInfo.HttpCode[:]...,
	)
	return
}

func validateBatchDeleteResponseHttp(
	t testing.TB,
	testInfo api.BatchDeleteOperationTestInfo,
	response []byte,
) {
	t.Helper()

	validateBatchDeleteResponseOpIdsNCodeHttp(t, testInfo, response)
}

func validateBatchDeleteResponseOpIdsNCodeHttp(
	t testing.TB,
	testInfo api.BatchDeleteOperationTestInfo,
	resp []byte,
) {
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch delete response. Error %v", err)
	}

	if len(*res.Result) != len(testInfo.Operations) {
		t.Fatal("Wrong number of operation responses received")
	}

	for i, subResp := range *res.Result {
		checkDeleteOpIDandStatus(t, testInfo.Operations[i], subResp.Body.OperationID,
			int(*subResp.Code), subResp)
	}
}

// Helper function to write data using batchwrite
func writeTestData(t testing.TB, operations []api.BatchWriteSubOp) {
	client := testutils.SetupHttpClient(t)

	batch := api.BatchWriteOpRequest{Operations: &operations}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshall write request %v", err)
	}

	url := testutils.NewBatchWriteURL()
	httpCode, response := testclient.SendHttpRequestWithClient(
		t,
		client,
		config.BATCH_HTTP_VERB,
		url,
		string(body),
		"",
		http.StatusOK,
	)

	if httpCode != http.StatusOK {
		t.Fatalf("Failed to write test data. HTTP code: %d, Response: %s", httpCode, string(response))
	}
}

// Helper function to read data using batch (batchread)
func readTestData(t testing.TB, operations []api.BatchSubOp) (int, []byte) {
	client := testutils.SetupHttpClient(t)

	batch := api.BatchOpRequest{Operations: &operations}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshall read request %v", err)
	}

	url := testutils.NewBatchReadURL()
	httpCode, response := testclient.SendHttpRequestWithClient(
		t,
		client,
		config.BATCH_HTTP_VERB,
		url,
		string(body),
		"",
		http.StatusOK,
	)

	return httpCode, response
}

// Helper function to verify a row exists
func verifyRowExists(t testing.TB, db, table string, filters *[]api.Filter) bool {
	method := config.PK_HTTP_VERB
	relURL := testutils.NewBatchPKReadURL(db, table)

	readOps := []api.BatchSubOp{
		{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKReadBody{
				Filters: filters,
			},
		},
	}

	httpCode, response := readTestData(t, readOps)
	if httpCode != http.StatusOK {
		return false
	}

	var res api.BatchResponseJSON
	err := json.Unmarshal(response, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal read response: %v", err)
	}

	if len(*res.Result) > 0 && *(*res.Result)[0].Code == http.StatusOK {
		return true
	}
	return false
}

// Helper function to verify a row does not exist (was deleted)
func verifyRowDeleted(t testing.TB, db, table string, filters *[]api.Filter) bool {
	method := config.PK_HTTP_VERB
	relURL := testutils.NewBatchPKReadURL(db, table)

	readOps := []api.BatchSubOp{
		{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKReadBody{
				Filters: filters,
			},
		},
	}

	httpCode, response := readTestData(t, readOps)
	if httpCode != http.StatusOK {
		return false
	}

	var res api.BatchResponseJSON
	err := json.Unmarshal(response, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal read response: %v", err)
	}

	if len(*res.Result) > 0 && *(*res.Result)[0].Code == http.StatusNotFound {
		return true
	}
	return false
}
