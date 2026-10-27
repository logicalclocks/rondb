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
package featurestorescan

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// The star-schema feature view most tests use: fsdb002 / sample_1n2 v1,
// sample_1_1 (root, pk id1) joined with sample_2_1 (prefix fg2_) on id1.
const (
	fsStar    = testdbs.FSDB002
	fvStar    = "sample_1n2"
	fvVersion = 1
)

// Response of POST /feature_store_scan. `status` and `detailedStatus` are
// optional (options.includeStatus / includeDetailedStatus); rawStatus
// records whether the key was present at all.
type ScanResponse struct {
	Features       [][]interface{}         `json:"features"`
	Metadata       []*api.FeatureMetadata  `json:"metadata"`
	Status         []api.FeatureStatus     `json:"status"`
	DetailedStatus [][]*api.DetailedStatus `json:"detailedStatus"`
	Rows           int                     `json:"rows"`
	Warning        string                  `json:"warning"`
	rawKeys        map[string]json.RawMessage
}

func (r *ScanResponse) HasKey(key string) bool {
	_, ok := r.rawKeys[key]
	return ok
}

// Request builder: the body is assembled as a map so tests can express any
// shape (including invalid ones) without a typed struct.
type ScanRequest map[string]interface{}

func NewScanRequest(fsName, fvName string, fvVersion int) ScanRequest {
	return ScanRequest{
		"featureStoreName":   fsName,
		"featureViewName":    fvName,
		"featureViewVersion": fvVersion,
	}
}

func (r ScanRequest) With(key string, value interface{}) ScanRequest {
	r[key] = value
	return r
}

func (r ScanRequest) String() string {
	b, err := json.Marshal(map[string]interface{}(r))
	if err != nil {
		panic(err)
	}
	return string(b)
}

// Filter node builders (the /scan grammar: binary logic tree)
func Cmp(column, cond string, value interface{}) map[string]interface{} {
	return map[string]interface{}{"op": "CMP", "column": column, "cond": cond, "value": value}
}

func IsNull(column string) map[string]interface{} {
	return map[string]interface{}{"op": "ISNULL", "column": column}
}

func IsNotNull(column string) map[string]interface{} {
	return map[string]interface{}{"op": "ISNOTNULL", "column": column}
}

func Logic(op string, left, right map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"op": op, "args": []interface{}{left, right}}
}

// OrOf nests an n-ary OR into the binary tree the parser accepts
func OrOf(nodes ...map[string]interface{}) map[string]interface{} {
	if len(nodes) == 1 {
		return nodes[0]
	}
	return Logic("OR", nodes[0], OrOf(nodes[1:]...))
}

func AndOf(nodes ...map[string]interface{}) map[string]interface{} {
	if len(nodes) == 1 {
		return nodes[0]
	}
	return Logic("AND", nodes[0], AndOf(nodes[1:]...))
}

func GetScanResponse(t *testing.T, req ScanRequest) *ScanResponse {
	return GetScanResponseWithDetail(t, req, "", http.StatusOK)
}

func GetScanResponseWithDetail(t *testing.T, req ScanRequest, message string, status int) *ScanResponse {
	t.Helper()
	return getScanResponseAt(t, testutils.NewFeatureStoreScanURL(), req, message, status)
}

// getScanResponseAt is GetScanResponseWithDetail against the endpoint at
// url, for tests that target another mtr rdrs2 instance.
func getScanResponseAt(t *testing.T, url string, req ScanRequest, message string, status int) *ScanResponse {
	t.Helper()
	_, respBody := testclient.SendHttpRequest(t, config.FEATURE_STORE_HTTP_VERB,
		url, req.String(), message, status)
	if int(status/100) != 2 {
		return nil
	}
	resp := ScanResponse{}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		t.Fatalf("Unmarshal failed %s; body: %s", err, respBody)
	}
	if err := json.Unmarshal(respBody, &resp.rawKeys); err != nil {
		t.Fatalf("Unmarshal keys failed %s", err)
	}
	if resp.Rows != len(resp.Features) {
		t.Errorf("rows=%d but %d feature vectors", resp.Rows, len(resp.Features))
	}
	if resp.HasKey("status") && len(resp.Status) != len(resp.Features) {
		t.Errorf("%d status entries for %d feature vectors", len(resp.Status), len(resp.Features))
	}
	return &resp
}

// Reference: the same feature view read through /batch_feature_store for
// the given serving-key entries (one map per row, values as JSON text).
func GetBatchReference(t *testing.T, fsName, fvName string, fvVersion int,
	pks []string, values [][]interface{}) *api.BatchFeatureStoreResponse {
	t.Helper()
	req := batchfeaturestore.CreateBatchFeatureStoreRequest(fsName, fvName, fvVersion, pks, values, nil, nil)
	return batchfeaturestore.GetFeatureStoreResponse(t, req)
}

func canonical(t *testing.T, v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %s", err)
	}
	return string(b)
}

// ExpectSameVectors checks that the scan returned exactly the vectors of the
// batch reference, in the same order, with the same per-row status.
func ExpectSameVectors(t *testing.T, got *ScanResponse, ref *api.BatchFeatureStoreResponse) {
	t.Helper()
	if len(got.Features) != len(ref.Features) {
		t.Fatalf("got %d vectors, reference has %d\n got: %s\n ref: %s",
			len(got.Features), len(ref.Features), canonical(t, got.Features), canonical(t, ref.Features))
	}
	for i := range ref.Features {
		if canonical(t, got.Features[i]) != canonical(t, ref.Features[i]) {
			t.Errorf("vector %d differs\n got: %s\n ref: %s", i,
				canonical(t, got.Features[i]), canonical(t, ref.Features[i]))
		}
		if got.HasKey("status") && got.Status[i] != ref.Status[i] {
			t.Errorf("status %d: got %s, reference %s", i, got.Status[i], ref.Status[i])
		}
	}
}

// SortByFirstFeature orders vectors by their first (numeric) feature so
// unordered scan output can be compared with an ordered reference.
func SortByFirstFeature(features [][]interface{}) {
	sort.SliceStable(features, func(i, j int) bool {
		return features[i][0].(float64) < features[j][0].(float64)
	})
}

// Query helpers against the data cluster's MySQL server

type sampleRow struct {
	Id1   int64
	Data1 int64
}

func queryRows(t *testing.T, query string, scan func(rows interface {
	Scan(dest ...interface{}) error
}) error) {
	t.Helper()
	db, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		t.Fatalf("mysql connect: %s", err)
	}
	defer db.Close()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %s: %s", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := scan(rows); err != nil {
			t.Fatalf("scan: %s", err)
		}
	}
}

// Sample rows of <db>.sample_1_1 with a non-null data1. Every feature
// store database has its own sample_1_1 with different data, so tests must
// sample from the database that owns the feature view under test.
func sampleRowsIn(t *testing.T, db string, n int) []sampleRow {
	t.Helper()
	var out []sampleRow
	queryRows(t, fmt.Sprintf("SELECT id1, data1 FROM %s.sample_1_1 WHERE data1 IS NOT NULL ORDER BY RAND() LIMIT %d", db, n),
		func(rows interface {
			Scan(dest ...interface{}) error
		}) error {
			var r sampleRow
			if err := rows.Scan(&r.Id1, &r.Data1); err != nil {
				return err
			}
			out = append(out, r)
			return nil
		})
	if len(out) < n {
		t.Fatalf("only %d sample rows", len(out))
	}
	return out
}

func sampleRows(t *testing.T, n int) []sampleRow {
	return sampleRowsIn(t, fsStar, n)
}

// id1 of the rows of <db>.sample_1_1 matching `where`, ascending
func idsWhereIn(t *testing.T, db string, where string) []int64 {
	t.Helper()
	var ids []int64
	queryRows(t, "SELECT id1 FROM "+db+".sample_1_1 WHERE "+where+" ORDER BY id1",
		func(rows interface {
			Scan(dest ...interface{}) error
		}) error {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
			return nil
		})
	return ids
}

func idsWhere(t *testing.T, where string) []int64 {
	return idsWhereIn(t, fsStar, where)
}

func execSQL(t *testing.T, stmt string) {
	t.Helper()
	db, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		t.Fatalf("mysql connect: %s", err)
	}
	defer db.Close()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("exec %s: %s", stmt, err)
	}
}

func idEntries(ids []int64) [][]interface{} {
	var values [][]interface{}
	for _, id := range ids {
		values = append(values, []interface{}{[]byte(fmt.Sprintf("%d", id))})
	}
	return values
}

func equalJSON(t *testing.T, a, b interface{}) bool {
	t.Helper()
	var x, y interface{}
	if err := json.Unmarshal([]byte(canonical(t, a)), &x); err != nil {
		t.Fatalf("unmarshal: %s", err)
	}
	if err := json.Unmarshal([]byte(canonical(t, b)), &y); err != nil {
		t.Fatalf("unmarshal: %s", err)
	}
	return reflect.DeepEqual(x, y)
}
