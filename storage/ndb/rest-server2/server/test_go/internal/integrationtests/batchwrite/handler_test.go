/*
 * Copyright (C) 2026, 2026 Hopsworks AB
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

package batchwrite

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

// Test basic pk-write operations
func TestBatchWriteSimple(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"simple_write": { // single operation batch - pk-write (upsert)
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 100, "col1", 200),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
		"simple_update": { // single operation batch - pk-update
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_UPDATE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 101, "col1", 201),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
		"simple_write_2": { // another pk-write test with different values
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 102, "col1", 202),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test batch write with multiple operations
func TestBatchWriteMultiple(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"multi_write": { // multiple write operations
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 110, "col1", 210),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
				},
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB005 + "/bigint_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 111, "col1", 211),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "bigint_table",
					DB:       testdbs.DB005,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
		"mixed_operations": { // mix of pk-write and pk-update
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 120),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
				},
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_UPDATE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col1", 220),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test update on non-existent row
func TestBatchWriteUpdateNonExistent(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"update_nonexistent": { // pk-update on non-existent row should fail
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_UPDATE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 9999, "id1", 9999),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 999),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusNotFound}, // Row doesn't exist, update should fail
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test varchar/text column writes
func TestBatchWriteTextColumns(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"write_varchar": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB014 + "/table1/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", testclient.EncodePkValue("1", false, 50, false)),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "updated_text_value"),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "table1",
					DB:       testdbs.DB014,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test BLOB/TEXT column writes (using DB013 which has blob_table and text_table)
func TestBatchWriteBlobColumns(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"write_text_column": { // Write to TEXT column in text_table
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB013 + "/text_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", "1"),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "This is a TEXT column update via batchwrite"),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "text_table",
					DB:       testdbs.DB013,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
		"write_text_and_int": { // Write to both TEXT and INT columns
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB013 + "/text_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", "1"),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "Updated TEXT value", "col1", 42),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "text_table",
					DB:       testdbs.DB013,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
		"write_blob_column": { // Write to BLOB column (base64 encoded)
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB013 + "/blob_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", testclient.EncodePkValue("1", true, 255, false)),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "SGVsbG8gV29ybGQh"), // "Hello World!" base64 encoded
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "blob_table",
					DB:       testdbs.DB013,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
		"write_blob_and_int": { // Write to both BLOB and INT columns
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB013 + "/blob_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", testclient.EncodePkValue("1", true, 255, false)),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "VXBkYXRlZCBCTE9C", "col1", 99), // "Updated BLOB" base64 encoded
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "blob_table",
					DB:       testdbs.DB013,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test multiple BLOB operations in a single batch
func TestBatchWriteMultipleBlobOps(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"multi_blob_writes": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB013 + "/text_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", "1"),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "First TEXT write"),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "text_table",
					DB:       testdbs.DB013,
					HttpCode: []int{http.StatusOK},
				},
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB013 + "/text_table/" + config.PK_UPDATE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", "1"),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", "Second TEXT update"),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "text_table",
					DB:       testdbs.DB013,
					HttpCode: []int{http.StatusOK},
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test batch write error handling - missing writeColumns
func TestBatchWriteMissingReqField(t *testing.T) {
	if !config.GetAll().REST.Enable {
		t.Skip("Skipping test as REST interface is disabled")
	}
	url := testutils.NewBatchWriteURL()

	// Test missing operations
	operations := make([]api.BatchWriteSubOp, 0)
	operationsWrapper := api.BatchWriteOpRequest{Operations: &operations}
	body, _ := json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"No operations defined", http.StatusBadRequest)

	// Test missing method
	operations = NewWriteOperationsTBD(t, 3)
	operations[1].Method = nil
	operationsWrapper = api.BatchWriteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the Method section should be POST", http.StatusBadRequest)

	// Test missing relative URL
	operations = NewWriteOperationsTBD(t, 3)
	operations[1].RelativeURL = nil
	operationsWrapper = api.BatchWriteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the relativeUrl section is required", http.StatusBadRequest)

	// Test missing body
	operations = NewWriteOperationsTBD(t, 3)
	operations[1].Body = nil
	operationsWrapper = api.BatchWriteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"Field validation for 'Body' failed", http.StatusBadRequest)

	// Test missing filter in an operation
	operations = NewWriteOperationsTBD(t, 3)
	operations[1].Body.Filters = nil
	operationsWrapper = api.BatchWriteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the filters section is null", http.StatusBadRequest)

	// Test missing writeColumns in an operation
	operations = NewWriteOperationsTBD(t, 3)
	operations[1].Body.WriteColumns = nil
	operationsWrapper = api.BatchWriteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the writeColumns section is", http.StatusBadRequest)
}

// Test invalid URL suffix (not pk-write, pk-update, or pk-insert)
func TestBatchWriteInvalidURLSuffix(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"invalid_suffix": {
			HttpCode: []int{http.StatusBadRequest},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/pk-read"}[0], // Wrong suffix
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 100),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusBadRequest},
				},
			},
			ErrMsgContains: "pk-write, pk-update, or pk-insert",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test writing to non-existent table
func TestBatchWriteNonExistentTable(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"nonexistent_table": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/nonexistent_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("col0", 100),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "nonexistent_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusNotFound},
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Test writing to non-existent column
func TestBatchWriteNonExistentColumn(t *testing.T) {
	tests := map[string]api.BatchWriteOperationTestInfo{
		"nonexistent_column": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				{
					SubOperation: api.BatchWriteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/" + config.PK_WRITE_OPERATION}[0],
						Body: &api.PKWriteBody{
							Filters:      testclient.NewFiltersKVs("id0", 0, "id1", 0),
							WriteColumns: testclient.NewWriteColumnsKVs("nonexistent_col", 100),
							OperationID:  testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusBadRequest}, // Non-existent column returns 400
				},
			},
			ErrMsgContains: "",
		},
	}
	batchWriteTestMultiple(t, tests)
}

// Helper functions
func NewWriteOperationsTBD(t *testing.T, numOps int) []api.BatchWriteSubOp {
	operations := make([]api.BatchWriteSubOp, numOps)
	for i := 0; i < numOps; i++ {
		operations[i] = NewWriteOperationTBD(t)
	}
	return operations
}

func NewWriteOperationTBD(t *testing.T) api.BatchWriteSubOp {
	pkOp := testclient.NewPKWriteReqBodyTBD()
	method := "POST"
	relativeURL := testutils.NewBatchPKWriteURL("db", "table", config.PK_WRITE_OPERATION)

	return api.BatchWriteSubOp{
		Method:      &method,
		RelativeURL: &relativeURL,
		Body:        &pkOp,
	}
}
