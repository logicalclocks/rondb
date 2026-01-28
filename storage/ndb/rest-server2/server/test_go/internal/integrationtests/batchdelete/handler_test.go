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

package batchdelete

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

// Test basic delete of integer table rows
func TestBatchDeleteIntTable(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"

	// Use unique keys for this test to avoid conflicts with other tests
	id0, id1 := 8000, 8000

	// Step 1: Write test data
	writeOps := createWriteOp(db, table,
		testclient.NewFiltersKVs("id0", id0, "id1", id1),
		testclient.NewWriteColumnsKVs("col0", 100, "col1", 200))
	writeTestData(t, writeOps)

	// Step 2: Verify data exists
	filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after write")
	}

	// Step 3: Delete the row
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_int_row": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Step 4: Verify data was deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}
}

// Test delete of bigint table rows
func TestBatchDeleteBigintTable(t *testing.T) {
	db := testdbs.DB005
	table := "bigint_table"

	id0, id1 := int64(8001), int64(8001)

	// Write test data
	writeOps := createWriteOp(db, table,
		testclient.NewFiltersKVs("id0", id0, "id1", id1),
		testclient.NewWriteColumnsKVs("col0", 100, "col1", 200))
	writeTestData(t, writeOps)

	// Verify exists
	filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after write")
	}

	// Delete
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_bigint_row": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}
}

// Test delete of varchar table rows
func TestBatchDeleteVarcharTable(t *testing.T) {
	db := testdbs.DB014
	table := "table1"

	pkValue := testclient.EncodePkValue("delete_test_key", false, 50, false)

	// Write test data
	writeOps := createWriteOp(db, table,
		testclient.NewFiltersKVs("id0", pkValue),
		testclient.NewWriteColumnsKVs("col0", "test_value"))
	writeTestData(t, writeOps)

	// Verify exists
	filters := testclient.NewFiltersKVs("id0", pkValue)
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after write")
	}

	// Delete
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_varchar_row": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}
}

// Test delete of varbinary table rows
func TestBatchDeleteVarbinaryTable(t *testing.T) {
	db := testdbs.DB017
	table := "table1"

	pkValue := testclient.EncodePkValue("delete_bin_key", true, 100, false)

	// Write test data
	writeOps := createWriteOp(db, table,
		testclient.NewFiltersKVs("id0", pkValue),
		testclient.NewWriteColumnsKVs("col0", testclient.EncodePkValue("bin_value", true, 100, false)))
	writeTestData(t, writeOps)

	// Verify exists
	filters := testclient.NewFiltersKVs("id0", pkValue)
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after write")
	}

	// Delete
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_varbinary_row": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}
}

// Test delete of rows with TEXT columns (BLOB handling)
func TestBatchDeleteTextTable(t *testing.T) {
	db := testdbs.DB013
	table := "text_table"

	pkValue := "delete_text_key"

	// Write test data with TEXT column
	writeOps := createWriteOp(db, table,
		testclient.NewFiltersKVs("id0", pkValue),
		testclient.NewWriteColumnsKVs("col0", "This is TEXT data for delete test", "col1", 42))
	writeTestData(t, writeOps)

	// Verify exists
	filters := testclient.NewFiltersKVs("id0", pkValue)
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after write")
	}

	// Delete - this tests BLOB part deletion via NdbBlob
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_text_row": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}
}

// Test delete of rows with BLOB columns
func TestBatchDeleteBlobTable(t *testing.T) {
	db := testdbs.DB013
	table := "blob_table"

	pkValue := testclient.EncodePkValue("delete_blob_key", true, 255, false)

	// Write test data with BLOB column (base64 encoded)
	writeOps := createWriteOp(db, table,
		testclient.NewFiltersKVs("id0", pkValue),
		testclient.NewWriteColumnsKVs("col0", "SGVsbG8gQkxPQiBEYXRh", "col1", 99)) // "Hello BLOB Data" base64
	writeTestData(t, writeOps)

	// Verify exists
	filters := testclient.NewFiltersKVs("id0", pkValue)
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after write")
	}

	// Delete - this tests BLOB part deletion
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_blob_row": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}
}

// Test delete of non-existent row (should return 404)
func TestBatchDeleteNonExistent(t *testing.T) {
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_nonexistent": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 99999, "id1", 99999),
					http.StatusNotFound),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)
}

// Test multiple deletes in a single batch
func TestBatchDeleteMultiple(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"

	// Write multiple rows
	writeOps := []api.BatchWriteSubOp{}
	for i := 8100; i < 8105; i++ {
		ops := createWriteOp(db, table,
			testclient.NewFiltersKVs("id0", i, "id1", i),
			testclient.NewWriteColumnsKVs("col0", i*10, "col1", i*20))
		writeOps = append(writeOps, ops...)
	}
	writeTestData(t, writeOps)

	// Verify all rows exist
	for i := 8100; i < 8105; i++ {
		filters := testclient.NewFiltersKVs("id0", i, "id1", i)
		if !verifyRowExists(t, db, table, filters) {
			t.Fatalf("Row %d should exist after write", i)
		}
	}

	// Delete all rows in a single batch
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_multiple": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table, testclient.NewFiltersKVs("id0", 8100, "id1", 8100), http.StatusOK),
				createDeleteSubOp(db, table, testclient.NewFiltersKVs("id0", 8101, "id1", 8101), http.StatusOK),
				createDeleteSubOp(db, table, testclient.NewFiltersKVs("id0", 8102, "id1", 8102), http.StatusOK),
				createDeleteSubOp(db, table, testclient.NewFiltersKVs("id0", 8103, "id1", 8103), http.StatusOK),
				createDeleteSubOp(db, table, testclient.NewFiltersKVs("id0", 8104, "id1", 8104), http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify all rows were deleted
	for i := 8100; i < 8105; i++ {
		filters := testclient.NewFiltersKVs("id0", i, "id1", i)
		if !verifyRowDeleted(t, db, table, filters) {
			t.Fatalf("Row %d should not exist after delete", i)
		}
	}
}

// Test batch delete across different tables
func TestBatchDeleteAcrossTables(t *testing.T) {
	// Write data to different tables
	writeOps := []api.BatchWriteSubOp{}

	// int_table
	ops1 := createWriteOp(testdbs.DB004, "int_table",
		testclient.NewFiltersKVs("id0", 8200, "id1", 8200),
		testclient.NewWriteColumnsKVs("col0", 100, "col1", 200))
	writeOps = append(writeOps, ops1...)

	// bigint_table
	ops2 := createWriteOp(testdbs.DB005, "bigint_table",
		testclient.NewFiltersKVs("id0", int64(8200), "id1", int64(8200)),
		testclient.NewWriteColumnsKVs("col0", 100, "col1", 200))
	writeOps = append(writeOps, ops2...)

	writeTestData(t, writeOps)

	// Delete from both tables in a single batch
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_across_tables": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 8200, "id1", 8200),
					http.StatusOK),
				createDeleteSubOp(testdbs.DB005, "bigint_table",
					testclient.NewFiltersKVs("id0", int64(8200), "id1", int64(8200)),
					http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify both rows were deleted
	if !verifyRowDeleted(t, testdbs.DB004, "int_table",
		testclient.NewFiltersKVs("id0", 8200, "id1", 8200)) {
		t.Fatal("int_table row should not exist after delete")
	}
	if !verifyRowDeleted(t, testdbs.DB005, "bigint_table",
		testclient.NewFiltersKVs("id0", int64(8200), "id1", int64(8200))) {
		t.Fatal("bigint_table row should not exist after delete")
	}
}

// Test delete with missing required fields
func TestBatchDeleteMissingReqField(t *testing.T) {
	if !config.GetAll().REST.Enable {
		t.Skip("Skipping test as REST interface is disabled")
	}
	url := testutils.NewBatchDeleteURL()

	// Test missing operations
	operations := make([]api.BatchDeleteSubOp, 0)
	operationsWrapper := api.BatchDeleteOpRequest{Operations: &operations}
	body, _ := json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"No operations defined", http.StatusBadRequest)

	// Test missing method
	operations = NewDeleteOperationsTBD(t, 3)
	operations[1].Method = nil
	operationsWrapper = api.BatchDeleteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the Method section should be POST", http.StatusBadRequest)

	// Test missing relative URL
	operations = NewDeleteOperationsTBD(t, 3)
	operations[1].RelativeURL = nil
	operationsWrapper = api.BatchDeleteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the relativeUrl section is required", http.StatusBadRequest)

	// Test missing body
	operations = NewDeleteOperationsTBD(t, 3)
	operations[1].Body = nil
	operationsWrapper = api.BatchDeleteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"Field validation for 'Body' failed", http.StatusBadRequest)

	// Test missing filters
	operations = NewDeleteOperationsTBD(t, 3)
	operations[1].Body.Filters = nil
	operationsWrapper = api.BatchDeleteOpRequest{Operations: &operations}
	body, _ = json.Marshal(operationsWrapper)
	testclient.SendHttpRequest(t, config.BATCH_HTTP_VERB, url, string(body),
		"the Field section is null", http.StatusBadRequest)
}

// Test delete with non-existent table
func TestBatchDeleteNonExistentTable(t *testing.T) {
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"nonexistent_table": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "nonexistent_table",
					testclient.NewFiltersKVs("id0", 0),
					http.StatusNotFound),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)
}

// Test delete with invalid URL suffix (not pk-delete)
func TestBatchDeleteInvalidURLSuffix(t *testing.T) {
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"invalid_suffix": {
			HttpCode: []int{http.StatusBadRequest},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				{
					SubOperation: api.BatchDeleteSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{testdbs.DB004 + "/int_table/pk-read"}[0], // Wrong suffix
						Body: &api.PKDeleteBody{
							Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
							OperationID: testclient.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       testdbs.DB004,
					HttpCode: []int{http.StatusBadRequest},
				},
			},
			ErrMsgContains: "RelativeURL",
		},
	}
	batchDeleteTestMultiple(t, tests)
}

// Test delete and re-insert (idempotency)
func TestBatchDeleteAndReinsert(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"
	id0, id1 := 8300, 8300

	filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)

	// Write initial data
	writeOps := createWriteOp(db, table, filters,
		testclient.NewWriteColumnsKVs("col0", 100, "col1", 200))
	writeTestData(t, writeOps)

	// Verify exists
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after initial write")
	}

	// Delete
	tests := map[string]api.BatchDeleteOperationTestInfo{
		"delete_for_reinsert": {
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table, filters, http.StatusOK),
			},
		},
	}
	batchDeleteTestMultiple(t, tests)

	// Verify deleted
	if !verifyRowDeleted(t, db, table, filters) {
		t.Fatal("Row should not exist after delete")
	}

	// Re-insert with different values
	writeOps = createWriteOp(db, table, filters,
		testclient.NewWriteColumnsKVs("col0", 999, "col1", 888))
	writeTestData(t, writeOps)

	// Verify re-inserted
	if !verifyRowExists(t, db, table, filters) {
		t.Fatal("Row should exist after re-insert")
	}

	// Cleanup - delete the row again
	batchDeleteTestMultiple(t, tests)
}

// Helper function to create a write operation
func createWriteOp(db, table string, filters *[]api.Filter, writeColumns *[]api.WriteColumn) []api.BatchWriteSubOp {
	method := config.PK_HTTP_VERB
	relURL := testutils.NewBatchPKWriteURL(db, table, config.PK_WRITE_OPERATION)
	return []api.BatchWriteSubOp{
		{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKWriteBody{
				Filters:      filters,
				WriteColumns: writeColumns,
			},
		},
	}
}

// Helper function to create a delete sub-operation
func createDeleteSubOp(db, table string, filters *[]api.Filter, expectedStatus int) api.BatchDeleteSubOperationTestInfo {
	method := config.PK_HTTP_VERB
	relURL := testutils.NewBatchPKDeleteURL(db, table)
	return api.BatchDeleteSubOperationTestInfo{
		SubOperation: api.BatchDeleteSubOp{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKDeleteBody{
				Filters:     filters,
				OperationID: testclient.NewOperationID(64),
			},
		},
		Table:    table,
		DB:       db,
		HttpCode: []int{expectedStatus},
	}
}

// Helper functions for error testing
func NewDeleteOperationsTBD(t *testing.T, numOps int) []api.BatchDeleteSubOp {
	operations := make([]api.BatchDeleteSubOp, numOps)
	for i := 0; i < numOps; i++ {
		operations[i] = NewDeleteOperationTBD(t)
	}
	return operations
}

func NewDeleteOperationTBD(t *testing.T) api.BatchDeleteSubOp {
	pkOp := testclient.NewPKDeleteReqBodyTBD()
	method := "POST"
	relativeURL := testutils.NewBatchPKDeleteURL("db", "table")

	return api.BatchDeleteSubOp{
		Method:      &method,
		RelativeURL: &relativeURL,
		Body:        &pkOp,
	}
}
