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

package sharing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

// getFeatureStoreResponseWithKey mirrors
// feature_store.GetFeatureStoreResponseWithDetail but sends the request
// under a caller-chosen API key - the whole point of the sharing tests is
// that authorization differs per user key.
func getFeatureStoreResponseWithKey(t *testing.T, apiKey string,
	req *api.FeatureStoreRequest, message string, status int) *api.FeatureStoreResponse {
	respCode, respBody := testclient.SendHttpRequestWithAPIKey(
		t, apiKey, config.FEATURE_STORE_HTTP_VERB, testutils.NewFeatureStoreURL(),
		req.String(), message, status)
	if respCode == http.StatusOK {
		fsResp := api.FeatureStoreResponse{}
		err := json.Unmarshal(respBody, &fsResp)
		if err != nil {
			t.Fatalf("Unmarshal failed %s ", err)
		}
		return &fsResp
	}
	return nil
}

// getBatchFeatureStoreResponseWithKey is the batch-endpoint twin of
// getFeatureStoreResponseWithKey.
func getBatchFeatureStoreResponseWithKey(t *testing.T, apiKey string,
	req *api.BatchFeatureStoreRequest, message string, status int) *api.BatchFeatureStoreResponse {
	respCode, respBody := testclient.SendHttpRequestWithAPIKey(
		t, apiKey, config.FEATURE_STORE_HTTP_VERB, testutils.NewBatchFeatureStoreURL(),
		req.String(), message, status)
	if respCode == http.StatusOK {
		fsResp := api.BatchFeatureStoreResponse{}
		err := json.Unmarshal(respBody, &fsResp)
		if err != nil {
			t.Fatalf("Unmarshal failed %s ", err)
		}
		return &fsResp
	}
	return nil
}

// ronsqlQueryWithKey sends a RonSQL query under a caller-chosen API key and
// returns the response code and raw body. TEXT_NOHEADER output keeps the
// body to the bare result values so callers can compare them against mysqld
// exactly.
func ronsqlQueryWithKey(t *testing.T, apiKey string, database string,
	query string, message string, status ...int) (int, []byte) {
	body := fmt.Sprintf(`{"query": %q, "database": %q, "outputFormat": "TEXT_NOHEADER"}`,
		query, database)
	return testclient.SendHttpRequestWithAPIKey(
		t, apiKey, config.RONSQL_HTTP_VERB, testutils.NewRonSQLURL(),
		body, message, status...)
}

// pkReadWithKey mirrors pkread.pkRESTTest with a caller-chosen API key.
// On 200 the returned columns listed in testInfo.RespKVs are validated
// against mysqld (the existing CompareDataWithDB pattern).
func pkReadWithKey(t *testing.T, apiKey string, testInfo api.PKTestInfo) {
	t.Helper()
	url := testutils.NewPKReadURL(testInfo.Db, testInfo.Table)
	body, err := json.Marshal(testInfo.PkReq)
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}

	httpCode, response := testclient.SendHttpRequestWithAPIKey(
		t, apiKey, config.PK_HTTP_VERB, url, string(body),
		testInfo.ErrMsgContains, testInfo.HttpCode)
	if httpCode == http.StatusOK {
		var pkResponse api.PKReadResponseJSON
		err := json.Unmarshal(response, &pkResponse)
		if err != nil {
			t.Fatalf("Failed to unmarshal response object %v", err)
		}
		parsedData := testclient.ParseColumnDataFromJson(t, pkResponse, false)
		for i := 0; i < len(testInfo.RespKVs); i++ {
			key := string(testInfo.RespKVs[i].(string))
			jsonVal, found := parsedData[key]
			if !found {
				t.Fatalf("Key not found in the response. Key %s", key)
			}
			integrationtests.CompareDataWithDB(t, testInfo.Db, testInfo.Table,
				testInfo.PkReq.Filters, &key, jsonVal, false)
		}
	}
}

// batchPKReadOp is one sub-operation of a batch pk-read on db/table,
// reading readColumns (nil = all columns) for the given customer_id. On an
// authorized batch the columns in respKVs are validated against mysqld.
type batchPKReadOp struct {
	db          string
	table       string
	customerID  int
	readColumns []string
	respKVs     []interface{}
}

// batchPKReadWithKey sends a batch pk-read under a caller-chosen API key
// and asserts the overall HTTP status (an authorization failure rejects
// the whole batch). On 200 every sub-response is checked for sub-code 200
// and its respKVs columns are validated against mysqld (the existing
// CompareDataWithDB pattern).
func batchPKReadWithKey(t *testing.T, apiKey string, ops []batchPKReadOp,
	message string, status int) {
	t.Helper()
	operations := make([]api.BatchSubOp, len(ops))
	for i, op := range ops {
		operations[i] = batchSubOp(op)
	}
	batch := api.BatchOpRequest{Operations: &operations}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}
	httpCode, response := testclient.SendHttpRequestWithAPIKey(
		t, apiKey, config.BATCH_HTTP_VERB, testutils.NewBatchReadURL(),
		string(body), message, status)
	if httpCode != http.StatusOK {
		return
	}
	var batchResponse api.BatchResponseJSON
	err = json.Unmarshal(response, &batchResponse)
	if err != nil {
		t.Fatalf("Failed to unmarshal batch response %v", err)
	}
	if len(*batchResponse.Result) != len(ops) {
		t.Fatalf("expected %d sub-responses but got %d", len(ops),
			len(*batchResponse.Result))
	}
	for i, subResponse := range *batchResponse.Result {
		op := ops[i]
		if *subResponse.Code != http.StatusOK {
			t.Fatalf("sub-operation %d (%s): got code %d but expect %d",
				i, op.table, *subResponse.Code, http.StatusOK)
		}
		parsedData := testclient.ParseColumnDataFromJson(t, *subResponse.Body, false)
		for _, respKV := range op.respKVs {
			key := respKV.(string)
			jsonVal, found := parsedData[key]
			if !found {
				t.Fatalf("sub-operation %d (%s): key %s not found in the response",
					i, op.table, key)
			}
			integrationtests.CompareDataWithDB(t, op.db, op.table,
				testclient.NewFiltersKVs("customer_id", op.customerID),
				&key, jsonVal, false)
		}
	}
}

func batchSubOp(op batchPKReadOp) api.BatchSubOp {
	method := "POST"
	relativeURL := op.db + "/" + op.table + "/" + config.PK_DB_OPERATION
	pkReq := api.PKReadBody{
		Filters:     testclient.NewFiltersKVs("customer_id", op.customerID),
		ReadColumns: newReadColumns(op.readColumns),
	}
	return api.BatchSubOp{
		Method:      &method,
		RelativeURL: &relativeURL,
		Body:        &pkReq,
	}
}

// newReadColumns converts a plain column-name list into the request form;
// nil stays nil, which the server treats as "all columns".
func newReadColumns(cols []string) *[]api.ReadColumn {
	if cols == nil {
		return nil
	}
	readColumns := make([]api.ReadColumn, len(cols))
	for i, col := range cols {
		c := col
		readColumns[i].Column = &c
	}
	return &readColumns
}

// readRowFromMySQL fetches the given columns of one row directly from
// mysqld - the reference the RDRS response is validated against. Values are
// returned in text form.
func readRowFromMySQL(t *testing.T, dbName string, table string, customerID int,
	cols []string) []string {
	t.Helper()
	db, err := testutils.CreateMySQLConnectionDataCluster()
	if err != nil {
		t.Fatalf("failed to connect to db. Error: %v", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT `%s` FROM %s.%s WHERE customer_id = ?",
		strings.Join(cols, "`, `"), dbName, table)
	rawVals := make([][]byte, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range rawVals {
		scanArgs[i] = &rawVals[i]
	}
	err = db.QueryRow(query, customerID).Scan(scanArgs...)
	if err != nil {
		t.Fatalf("failed to read row from mysqld; query: '%s'; error: %v", query, err)
	}

	vals := make([]string, len(cols))
	for i, raw := range rawVals {
		vals[i] = string(raw)
	}
	return vals
}

// validateVectorWithMySQL compares the feature vector served by RDRS with
// the values read directly from mysqld. Both sides are compared in text
// form (JSON numbers print identically to their mysqld text values).
func validateVectorWithMySQL(t *testing.T, resp *api.FeatureStoreResponse, expected []string) {
	t.Helper()
	if resp == nil {
		t.Fatalf("expected a feature vector but got no response")
	}
	if resp.Status != api.FEATURE_STATUS_COMPLETE {
		t.Errorf("Got status %s but expect %s", resp.Status, api.FEATURE_STATUS_COMPLETE)
	}
	validateFeaturesWithMySQL(t, resp.Features, expected)
}

// validateBatchVectorsWithMySQL is the batch-endpoint twin: one expected
// mysqld row per batch entry, in request order.
func validateBatchVectorsWithMySQL(t *testing.T, resp *api.BatchFeatureStoreResponse,
	expected [][]string) {
	t.Helper()
	if resp == nil {
		t.Fatalf("expected feature vectors but got no response")
	}
	if len(resp.Features) != len(expected) {
		t.Fatalf("expected %d feature vectors but got %d", len(expected),
			len(resp.Features))
	}
	for i, features := range resp.Features {
		if resp.Status[i] != api.FEATURE_STATUS_COMPLETE {
			t.Errorf("entry %d: got status %s but expect %s", i,
				resp.Status[i], api.FEATURE_STATUS_COMPLETE)
		}
		validateFeaturesWithMySQL(t, features, expected[i])
	}
}

func validateFeaturesWithMySQL(t *testing.T, features []interface{}, expected []string) {
	t.Helper()
	if len(features) != len(expected) {
		t.Fatalf("expected %d features but got %d: %v", len(expected),
			len(features), features)
	}
	for i, feature := range features {
		got := fmt.Sprintf("%v", feature)
		if got != expected[i] {
			t.Errorf("feature %d: got %s but mysqld has %s", i, got, expected[i])
		}
	}
}
