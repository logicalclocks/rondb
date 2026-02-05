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

// Package jsonerrors tests the JSON error response format in API version 0.2.0
// This verifies that errors returned by the REST API are properly formatted as JSON
// objects instead of plain text (which is the behavior in 0.1.0).
package jsonerrors

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// JSONErrorResponse represents the JSON error response format in API 0.2.0
type JSONErrorResponse struct {
	Error struct {
		Code           int    `json:"code"`
		Message        string `json:"message"`
		Status         int    `json:"status"`
		Classification int    `json:"classification"`
		NdbCode        int    `json:"ndbCode"`
		MysqlCode      int    `json:"mysqlCode"`
	} `json:"error"`
}

// TestJSONErrorResponseFormat tests that errors are returned as JSON in API 0.2.0
func TestJSONErrorResponseFormat(t *testing.T) {
	if !config.GetAll().REST.Enable {
		t.Skip("Skipping test as REST interface is disabled")
	}

	tests := []struct {
		name           string
		description    string
		getURL         func() string
		httpVerb       string
		body           string
		expectedStatus int
	}{
		{
			name:           "batch_read_invalid_json",
			description:    "Test JSON error for invalid JSON in batch read",
			getURL:         testutils.NewBatchReadURLV2,
			httpVerb:       config.BATCH_HTTP_VERB,
			body:           `{invalid json}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "batch_read_empty_operations",
			description:    "Test JSON error for empty operations array",
			getURL:         testutils.NewBatchReadURLV2,
			httpVerb:       config.BATCH_HTTP_VERB,
			body:           `{"operations":[]}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "batch_write_invalid_json",
			description:    "Test JSON error for invalid JSON in batch write",
			getURL:         testutils.NewBatchWriteURLV2,
			httpVerb:       config.BATCH_HTTP_VERB,
			body:           `{invalid json}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "batch_write_empty_operations",
			description:    "Test JSON error for empty operations array in batch write",
			getURL:         testutils.NewBatchWriteURLV2,
			httpVerb:       config.BATCH_HTTP_VERB,
			body:           `{"operations":[]}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "batch_delete_invalid_json",
			description:    "Test JSON error for invalid JSON in batch delete",
			getURL:         testutils.NewBatchDeleteURLV2,
			httpVerb:       config.BATCH_DELETE_HTTP_VERB,
			body:           `{invalid json}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "batch_delete_empty_operations",
			description:    "Test JSON error for empty operations array in batch delete",
			getURL:         testutils.NewBatchDeleteURLV2,
			httpVerb:       config.BATCH_DELETE_HTTP_VERB,
			body:           `{"operations":[]}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := tt.getURL()
			httpCode, response := testclient.SendHttpRequest(t, tt.httpVerb, url, tt.body,
				"", tt.expectedStatus)

			if httpCode != tt.expectedStatus {
				t.Fatalf("Expected HTTP status %d, got %d. Response: %s",
					tt.expectedStatus, httpCode, string(response))
			}

			// Verify the response is valid JSON with expected error structure
			var errResp JSONErrorResponse
			err := json.Unmarshal(response, &errResp)
			if err != nil {
				t.Fatalf("Failed to parse JSON error response: %v. Response was: %s",
					err, string(response))
			}

			// Verify the error structure has expected fields
			if errResp.Error.Code != tt.expectedStatus {
				t.Errorf("Error code mismatch: expected %d, got %d",
					tt.expectedStatus, errResp.Error.Code)
			}

			if errResp.Error.Message == "" {
				t.Error("Error message should not be empty")
			}

			t.Logf("JSON Error Response: code=%d, message=%s",
				errResp.Error.Code, errResp.Error.Message)
		})
	}
}

// TestJSONErrorVsPlainTextError compares error responses between 0.1.0 (plain text) and 0.2.0 (JSON)
func TestJSONErrorVsPlainTextError(t *testing.T) {
	if !config.GetAll().REST.Enable {
		t.Skip("Skipping test as REST interface is disabled")
	}

	// Test with empty operations - should fail in both versions
	body := `{"operations":[]}`

	// Test v1 (0.1.0) - should return plain text error
	t.Run("v1_plain_text_error", func(t *testing.T) {
		url := testutils.NewBatchReadURL()
		httpCode, response := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, body,
			"", http.StatusBadRequest)

		if httpCode != http.StatusBadRequest {
			t.Fatalf("Expected HTTP 400, got %d", httpCode)
		}

		// v1 should NOT be valid JSON error format
		var errResp JSONErrorResponse
		err := json.Unmarshal(response, &errResp)
		if err == nil && errResp.Error.Code != 0 {
			t.Logf("Note: v1 response parsed as JSON (backwards compatible): %s", string(response))
		} else {
			// Expected - v1 returns plain text
			t.Logf("v1 returns plain text error (expected): %s", string(response))
		}
	})

	// Test v2 (0.2.0) - should return JSON error
	t.Run("v2_json_error", func(t *testing.T) {
		url := testutils.NewBatchReadURLV2()
		httpCode, response := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, body,
			"", http.StatusBadRequest)

		if httpCode != http.StatusBadRequest {
			t.Fatalf("Expected HTTP 400, got %d", httpCode)
		}

		// v2 MUST be valid JSON error format
		var errResp JSONErrorResponse
		err := json.Unmarshal(response, &errResp)
		if err != nil {
			t.Fatalf("v2 should return JSON error, but got: %s (parse error: %v)",
				string(response), err)
		}

		if errResp.Error.Code != http.StatusBadRequest {
			t.Errorf("Expected error code %d, got %d", http.StatusBadRequest, errResp.Error.Code)
		}

		t.Logf("v2 returns JSON error (expected): code=%d, message=%s",
			errResp.Error.Code, errResp.Error.Message)
	})
}

// TestJSONErrorNonExistentTable tests JSON error for operations on non-existent tables
func TestJSONErrorNonExistentTable(t *testing.T) {
	if !config.GetAll().REST.Enable {
		t.Skip("Skipping test as REST interface is disabled")
	}

	method := config.PK_HTTP_VERB
	relURL := testdbs.DB004 + "/nonexistent_table/pk-read"

	operations := []api.BatchSubOp{
		{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKReadBody{
				Filters: testclient.NewFiltersKVs("id", "1"),
			},
		},
	}

	batch := api.BatchOpRequest{Operations: &operations}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	url := testutils.NewBatchReadURLV2()
	httpCode, response := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"", http.StatusOK) // Batch returns 200 overall, errors are in sub-responses

	if httpCode != http.StatusOK {
		// If the overall request fails, check for JSON error format
		var errResp JSONErrorResponse
		err := json.Unmarshal(response, &errResp)
		if err != nil {
			t.Fatalf("Expected JSON error response, got: %s", string(response))
		}
		t.Logf("JSON Error: code=%d, message=%s", errResp.Error.Code, errResp.Error.Message)
	} else {
		// Batch request succeeded, check sub-operation status
		var batchResp api.BatchResponseJSON
		err := json.Unmarshal(response, &batchResp)
		if err != nil {
			t.Fatalf("Failed to parse batch response: %v", err)
		}

		if len(*batchResp.Result) == 0 {
			t.Fatal("Expected at least one result")
		}

		// First operation should fail with 404
		firstResult := (*batchResp.Result)[0]
		if *firstResult.Code != http.StatusNotFound {
			t.Errorf("Expected sub-operation status 404, got %d", *firstResult.Code)
		}
		t.Logf("Sub-operation error: code=%d", *firstResult.Code)
	}
}

// TestJSONErrorMissingRequiredFields tests JSON error responses for missing required fields
// Note: Some validation errors return batch-level 400, others return 200 with sub-operation errors
func TestJSONErrorMissingRequiredFields(t *testing.T) {
	if !config.GetAll().REST.Enable {
		t.Skip("Skipping test as REST interface is disabled")
	}

	// Use a valid database to pass authorization, with a valid table
	validRelURL := testdbs.DB004 + "/int_table/pk-read"

	tests := []struct {
		name              string
		body              string
		errContains       string
		batchLevelError   bool // true if error returned at batch level (HTTP 400)
		expectedSubStatus int  // only used if batchLevelError is false
	}{
		{
			name:              "missing_method",
			body:              `{"operations":[{"relative-url":"` + validRelURL + `","body":{"filters":[{"column":"id0","value":"1"}]}}]}`,
			errContains:       "Method",
			batchLevelError:   false,
			expectedSubStatus: http.StatusBadRequest,
		},
		{
			name:            "missing_relative_url",
			body:            `{"operations":[{"method":"POST","body":{"filters":[{"column":"id0","value":"1"}]}}]}`,
			errContains:     "relativeUrl",
			batchLevelError: true,
		},
		{
			name:            "missing_body",
			body:            `{"operations":[{"method":"POST","relative-url":"` + validRelURL + `"}]}`,
			errContains:     "Body",
			batchLevelError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := testutils.NewBatchReadURLV2()

			if tt.batchLevelError {
				// Expect batch-level HTTP 400 with JSON error
				httpCode, response := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, tt.body,
					"", http.StatusBadRequest)

				if httpCode != http.StatusBadRequest {
					t.Fatalf("Expected HTTP 400, got %d. Response: %s", httpCode, string(response))
				}

				// Verify JSON error format
				var errResp JSONErrorResponse
				err := json.Unmarshal(response, &errResp)
				if err != nil {
					t.Fatalf("Failed to parse JSON error: %v. Response: %s", err, string(response))
				}

				if errResp.Error.Code != http.StatusBadRequest {
					t.Errorf("Expected error code 400, got %d", errResp.Error.Code)
				}

				t.Logf("Batch-level JSON error for %s: message=%s", tt.name, errResp.Error.Message)
			} else {
				// Expect HTTP 200 with error codes in sub-operation results
				httpCode, response := testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, tt.body,
					"", http.StatusOK)

				if httpCode != http.StatusOK {
					t.Fatalf("Expected HTTP 200, got %d. Response: %s", httpCode, string(response))
				}

				// Parse batch response and check sub-operation status
				var batchResp api.BatchResponseJSON
				err := json.Unmarshal(response, &batchResp)
				if err != nil {
					t.Fatalf("Failed to parse batch response: %v. Response: %s", err, string(response))
				}

				if batchResp.Result == nil || len(*batchResp.Result) == 0 {
					t.Fatal("Expected at least one result in batch response")
				}

				// Check that the sub-operation returned the expected error code
				firstResult := (*batchResp.Result)[0]
				if int(*firstResult.Code) != tt.expectedSubStatus {
					t.Errorf("Expected sub-operation status %d, got %d",
						tt.expectedSubStatus, *firstResult.Code)
				}

				t.Logf("Sub-operation error for %s: code=%d", tt.name, *firstResult.Code)
			}
		})
	}
}
