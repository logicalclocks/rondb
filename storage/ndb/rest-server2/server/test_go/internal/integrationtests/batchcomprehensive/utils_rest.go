/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package batchcomprehensive

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

// Run write operation via REST API
func runWriteRESTTest(t *testing.T, testInfo api.BatchWriteOperationTestInfo) {
	t.Helper()
	client := testutils.SetupHttpClient(t)

	subOps := []api.BatchWriteSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchWriteOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal write request: %v", err)
	}

	url := testutils.NewBatchWriteURL()
	httpCode, response := testclient.SendHttpRequestWithClient(
		t, client, config.BATCH_HTTP_VERB, url, string(body),
		testInfo.ErrMsgContains, testInfo.HttpCode[:]...,
	)

	if log.IsTrace() {
		log.Tracef("Write Response: %s", string(response))
	}

	if httpCode == http.StatusOK {
		validateWriteResponse(t, testInfo, response)
	}
}

// Run read operation via REST API
func runReadRESTTest(t *testing.T, testInfo api.BatchOperationTestInfo) {
	t.Helper()
	client := testutils.SetupHttpClient(t)

	subOps := []api.BatchSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal read request: %v", err)
	}

	url := testutils.NewBatchReadURL()
	httpCode, response := testclient.SendHttpRequestWithClient(
		t, client, config.BATCH_HTTP_VERB, url, string(body),
		testInfo.ErrMsgContains, testInfo.HttpCode[:]...,
	)

	if log.IsTrace() {
		log.Tracef("Read Response: %s", string(response))
	}

	if httpCode == http.StatusOK {
		validateReadResponse(t, testInfo, response)
	}
}

// Run delete operation via REST API
func runDeleteRESTTest(t *testing.T, testInfo api.BatchDeleteOperationTestInfo) {
	t.Helper()
	client := testutils.SetupHttpClient(t)

	subOps := []api.BatchDeleteSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchDeleteOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal delete request: %v", err)
	}

	url := testutils.NewBatchDeleteURL()
	httpCode, response := testclient.SendHttpRequestWithClient(
		t, client, config.BATCH_HTTP_VERB, url, string(body),
		testInfo.ErrMsgContains, testInfo.HttpCode[:]...,
	)

	if log.IsTrace() {
		log.Tracef("Delete Response: %s", string(response))
	}

	if httpCode == http.StatusOK {
		validateDeleteResponse(t, testInfo, response)
	}
}

// Validate write response
func validateWriteResponse(t *testing.T, testInfo api.BatchWriteOperationTestInfo, resp []byte) {
	t.Helper()
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal write response: %v", err)
	}

	if len(*res.Result) != len(testInfo.Operations) {
		t.Fatalf("Wrong number of operation responses: expected %d, got %d",
			len(testInfo.Operations), len(*res.Result))
	}

	for i, subResp := range *res.Result {
		expectedStatus := testInfo.Operations[i].HttpCode
		gotStatus := int(*subResp.Code)
		if !containsInt(expectedStatus, gotStatus) {
			t.Fatalf("Operation %d: expected status %v, got %d. Body: %v",
				i, expectedStatus, gotStatus, subResp.String())
		}
	}
}

// Validate read response
func validateReadResponse(t *testing.T, testInfo api.BatchOperationTestInfo, resp []byte) {
	t.Helper()
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal read response: %v", err)
	}

	if len(*res.Result) != len(testInfo.Operations) {
		t.Fatalf("Wrong number of operation responses: expected %d, got %d",
			len(testInfo.Operations), len(*res.Result))
	}

	for i, subResp := range *res.Result {
		expectedStatus := testInfo.Operations[i].HttpCode
		gotStatus := int(*subResp.Code)
		if !containsInt(expectedStatus, gotStatus) {
			t.Fatalf("Operation %d: expected status %v, got %d. Body: %v",
				i, expectedStatus, gotStatus, subResp.String())
		}

		// Validate response key-value pairs if specified
		if len(testInfo.Operations[i].RespKVs) > 0 && gotStatus == http.StatusOK {
			validateRespKVs(t, i, testInfo.Operations[i].RespKVs, subResp.Body)
		}
	}
}

// Validate delete response
func validateDeleteResponse(t *testing.T, testInfo api.BatchDeleteOperationTestInfo, resp []byte) {
	t.Helper()
	var res api.BatchResponseJSON
	err := json.Unmarshal(resp, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal delete response: %v", err)
	}

	if len(*res.Result) != len(testInfo.Operations) {
		t.Fatalf("Wrong number of operation responses: expected %d, got %d",
			len(testInfo.Operations), len(*res.Result))
	}

	for i, subResp := range *res.Result {
		expectedStatus := testInfo.Operations[i].HttpCode
		gotStatus := int(*subResp.Code)
		if !containsInt(expectedStatus, gotStatus) {
			t.Fatalf("Operation %d: expected status %v, got %d. Body: %v",
				i, expectedStatus, gotStatus, subResp.String())
		}
	}
}

// Validate response key-value pairs
// Expected format: [key1, expectedValue1, key2, expectedValue2, ...]
func validateRespKVs(t *testing.T, opIdx int, expected []interface{}, body *api.PKReadResponseJSON) {
	t.Helper()
	if body == nil || body.Data == nil {
		t.Fatalf("Operation %d: expected data but got nil", opIdx)
	}

	data := *body.Data

	// Expected format: [key1, value1, key2, value2, ...]
	for i := 0; i < len(expected); i += 2 {
		key := expected[i].(string)
		expectedValue := expected[i+1]

		rawValue, ok := data[key]
		if !ok {
			t.Fatalf("Operation %d: missing key '%s' in response", opIdx, key)
		}

		// Parse the raw JSON value
		var gotValue interface{}
		if err := json.Unmarshal(*rawValue, &gotValue); err != nil {
			t.Fatalf("Operation %d: failed to unmarshal key '%s': %v", opIdx, key, err)
		}

		if !valuesEqual(expectedValue, gotValue) {
			t.Fatalf("Operation %d: key '%s' value mismatch. Expected: %v (%T), Got: %v (%T)",
				opIdx, key, expectedValue, expectedValue, gotValue, gotValue)
		}
	}
}

// Check if slice contains int
func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// Compare values accounting for type differences (e.g., int vs float64 from JSON)
func valuesEqual(expected, got interface{}) bool {
	// Handle nil comparison
	if expected == nil {
		return got == nil
	}

	// Handle numeric comparisons (JSON unmarshals numbers as float64)
	switch e := expected.(type) {
	case int:
		if g, ok := got.(float64); ok {
			return float64(e) == g
		}
	case int64:
		if g, ok := got.(float64); ok {
			return float64(e) == g
		}
	case float64:
		if g, ok := got.(float64); ok {
			return e == g
		}
	case string:
		if g, ok := got.(string); ok {
			return e == g
		}
	}

	// Default comparison
	return expected == got
}

// Verify row exists - returns true if row exists
func verifyRowExistsREST(t *testing.T, testInfo api.BatchOperationTestInfo) bool {
	t.Helper()
	client := testutils.SetupHttpClient(t)

	subOps := []api.BatchSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	url := testutils.NewBatchReadURL()
	_, response := testclient.SendHttpRequestWithClient(
		t, client, config.BATCH_HTTP_VERB, url, string(body),
		"", http.StatusOK,
	)

	var res api.BatchResponseJSON
	err = json.Unmarshal(response, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(*res.Result) == 0 {
		return false
	}

	// Check if the first operation returned 200 (row exists)
	return int(*(*res.Result)[0].Code) == http.StatusOK
}

// Verify row deleted - returns true if row was deleted (404)
func verifyRowDeletedREST(t *testing.T, testInfo api.BatchOperationTestInfo) bool {
	t.Helper()
	client := testutils.SetupHttpClient(t)

	subOps := []api.BatchSubOp{}
	for _, op := range testInfo.Operations {
		subOps = append(subOps, op.SubOperation)
	}
	batch := api.BatchOpRequest{Operations: &subOps}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	url := testutils.NewBatchReadURL()
	_, response := testclient.SendHttpRequestWithClient(
		t, client, config.BATCH_HTTP_VERB, url, string(body),
		"", http.StatusOK,
	)

	var res api.BatchResponseJSON
	err = json.Unmarshal(response, &res)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if len(*res.Result) == 0 {
		return false
	}

	// Check if the first operation returned 404 (row deleted)
	return int(*(*res.Result)[0].Code) == http.StatusNotFound
}
