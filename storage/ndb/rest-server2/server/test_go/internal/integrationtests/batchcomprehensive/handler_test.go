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
	"encoding/base64"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// ============================================================================
// SECTION 1: CRUD LIFECYCLE TESTS
// Tests the complete create-read-update-delete lifecycle for various data types
// ============================================================================

// Test complete CRUD lifecycle for integer table
func TestCRUDLifecycleInt(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"

	// Use unique keys for this test
	id0, id1 := 10000, 10000

	// Step 1: CREATE - Insert new row using pk-write
	t.Run("create", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 100, "col1", 200),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// Step 2: READ - Verify data was created with correct values
	t.Run("read_after_create", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
		readOp := api.BatchOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchSubOperationTestInfo{
				createReadSubOp(db, table, filters,
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 100, "col1", 200}),
			},
		}
		readAndVerifyData(t, readOp)
	})

	// Step 3: UPDATE - Modify column values using pk-update
	t.Run("update", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 150, "col1", 250),
					config.PK_UPDATE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// Step 4: READ - Verify update took effect
	t.Run("read_after_update", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
		readOp := api.BatchOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchSubOperationTestInfo{
				createReadSubOp(db, table, filters,
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 150, "col1", 250}),
			},
		}
		readAndVerifyData(t, readOp)
	})

	// Step 5: DELETE - Remove the row
	t.Run("delete", func(t *testing.T) {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Step 6: VERIFY DELETE - Confirm row no longer exists
	t.Run("verify_deleted", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
		if !verifyRowDeleted(t, db, table, filters) {
			t.Fatal("Row should not exist after delete")
		}
	})
}

// Test CRUD lifecycle for bigint table
func TestCRUDLifecycleBigint(t *testing.T) {
	db := testdbs.DB005
	table := "bigint_table"

	id0, id1 := int64(10001), int64(10001)

	// CREATE
	t.Run("create", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", int64(9223372036854775800), "col1", uint64(18446744073709551600)),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// READ - verify large values
	t.Run("read_large_values", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
		if !verifyRowExists(t, db, table, filters) {
			t.Fatal("Row should exist after write")
		}
	})

	// DELETE
	t.Run("delete", func(t *testing.T) {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// VERIFY DELETED
	t.Run("verify_deleted", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
		if !verifyRowDeleted(t, db, table, filters) {
			t.Fatal("Row should not exist after delete")
		}
	})
}

// Test CRUD lifecycle for varchar table
func TestCRUDLifecycleVarchar(t *testing.T) {
	db := testdbs.DB014
	table := "table1"

	pkValue := testclient.EncodePkValue("crud_test_key_10002", false, 50, false)

	// CREATE
	t.Run("create", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					testclient.NewWriteColumnsKVs("col0", "test_value_initial"),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// UPDATE with different string
	t.Run("update", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					testclient.NewWriteColumnsKVs("col0", "test_value_updated"),
					config.PK_UPDATE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// DELETE
	t.Run("delete", func(t *testing.T) {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})
}

// ============================================================================
// SECTION 2: BATCH OPERATIONS TESTS
// Tests multiple operations in a single batch request
// ============================================================================

// Test batch with multiple write operations
func TestBatchMultipleWrites(t *testing.T) {
	// Write to multiple tables in a single batch
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10100, "id1", 10100),
				testclient.NewWriteColumnsKVs("col0", 1001, "col1", 2001),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10101, "id1", 10101),
				testclient.NewWriteColumnsKVs("col0", 1002, "col1", 2002),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10102, "id1", 10102),
				testclient.NewWriteColumnsKVs("col0", 1003, "col1", 2003),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB005, "bigint_table",
				testclient.NewFiltersKVs("id0", int64(10100), "id1", int64(10100)),
				testclient.NewWriteColumnsKVs("col0", int64(1001), "col1", int64(2001)),
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, writeOp)

	// Cleanup
	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10100, "id1", 10100), http.StatusOK),
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10101, "id1", 10101), http.StatusOK),
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10102, "id1", 10102), http.StatusOK),
				createDeleteSubOp(testdbs.DB005, "bigint_table",
					testclient.NewFiltersKVs("id0", int64(10100), "id1", int64(10100)), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Verify all rows exist
	t.Run("verify_all_written", func(t *testing.T) {
		readOp := api.BatchOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchSubOperationTestInfo{
				createReadSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10100, "id1", 10100),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 1001, "col1", 2001}),
				createReadSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10101, "id1", 10101),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 1002, "col1", 2002}),
				createReadSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10102, "id1", 10102),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 1003, "col1", 2003}),
			},
		}
		readAndVerifyData(t, readOp)
	})
}

// Test batch with multiple read operations
func TestBatchMultipleReads(t *testing.T) {
	// First setup: write test data
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10200, "id1", 10200),
				testclient.NewWriteColumnsKVs("col0", 111, "col1", 222),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10201, "id1", 10201),
				testclient.NewWriteColumnsKVs("col0", 333, "col1", 444),
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, writeOp)

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10200, "id1", 10200), http.StatusOK),
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10201, "id1", 10201), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Batch read multiple rows
	readOp := api.BatchOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchSubOperationTestInfo{
			createReadSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10200, "id1", 10200),
				testclient.NewReadColumns("col", 2),
				http.StatusOK,
				[]interface{}{"col0", 111, "col1", 222}),
			createReadSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10201, "id1", 10201),
				testclient.NewReadColumns("col", 2),
				http.StatusOK,
				[]interface{}{"col0", 333, "col1", 444}),
		},
	}
	readAndVerifyData(t, readOp)
}

// Test batch with multiple delete operations
func TestBatchMultipleDeletes(t *testing.T) {
	// Setup: write test data
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10300, "id1", 10300),
				testclient.NewWriteColumnsKVs("col0", 100),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10301, "id1", 10301),
				testclient.NewWriteColumnsKVs("col0", 200),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10302, "id1", 10302),
				testclient.NewWriteColumnsKVs("col0", 300),
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, writeOp)

	// Batch delete all three rows
	deleteOp := api.BatchDeleteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchDeleteSubOperationTestInfo{
			createDeleteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10300, "id1", 10300), http.StatusOK),
			createDeleteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10301, "id1", 10301), http.StatusOK),
			createDeleteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10302, "id1", 10302), http.StatusOK),
		},
	}
	deleteTestData(t, deleteOp)

	// Verify all deleted
	t.Run("verify_all_deleted", func(t *testing.T) {
		for _, id := range []int{10300, 10301, 10302} {
			filters := testclient.NewFiltersKVs("id0", id, "id1", id)
			if !verifyRowDeleted(t, testdbs.DB004, "int_table", filters) {
				t.Fatalf("Row with id %d should be deleted", id)
			}
		}
	})
}

// ============================================================================
// SECTION 3: CROSS-TABLE OPERATIONS
// Tests operations spanning multiple tables and databases
// ============================================================================

// Test cross-table batch operations
func TestCrossTableBatchOperations(t *testing.T) {
	// Write to different tables in different databases
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10400, "id1", 10400),
				testclient.NewWriteColumnsKVs("col0", 400),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB005, "bigint_table",
				testclient.NewFiltersKVs("id0", int64(10400), "id1", int64(10400)),
				testclient.NewWriteColumnsKVs("col0", int64(4000)),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB006, "tinyint_table",
				testclient.NewFiltersKVs("id0", 40, "id1", 40),
				testclient.NewWriteColumnsKVs("col0", 40),
				config.PK_WRITE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB007, "smallint_table",
				testclient.NewFiltersKVs("id0", 10400, "id1", 10400),
				testclient.NewWriteColumnsKVs("col0", 400),
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, writeOp)

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10400, "id1", 10400), http.StatusOK),
				createDeleteSubOp(testdbs.DB005, "bigint_table",
					testclient.NewFiltersKVs("id0", int64(10400), "id1", int64(10400)), http.StatusOK),
				createDeleteSubOp(testdbs.DB006, "tinyint_table",
					testclient.NewFiltersKVs("id0", 40, "id1", 40), http.StatusOK),
				createDeleteSubOp(testdbs.DB007, "smallint_table",
					testclient.NewFiltersKVs("id0", 10400, "id1", 10400), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Verify all rows in all tables
	t.Run("verify_cross_table", func(t *testing.T) {
		if !verifyRowExists(t, testdbs.DB004, "int_table",
			testclient.NewFiltersKVs("id0", 10400, "id1", 10400)) {
			t.Fatal("DB004 row should exist")
		}
		if !verifyRowExists(t, testdbs.DB005, "bigint_table",
			testclient.NewFiltersKVs("id0", int64(10400), "id1", int64(10400))) {
			t.Fatal("DB005 row should exist")
		}
		if !verifyRowExists(t, testdbs.DB006, "tinyint_table",
			testclient.NewFiltersKVs("id0", 40, "id1", 40)) {
			t.Fatal("DB006 row should exist")
		}
		if !verifyRowExists(t, testdbs.DB007, "smallint_table",
			testclient.NewFiltersKVs("id0", 10400, "id1", 10400)) {
			t.Fatal("DB007 row should exist")
		}
	})
}

// ============================================================================
// SECTION 4: MIXED OPERATION TYPES
// Tests different operation types (write, update, insert) in same batch
// ============================================================================

// Test mixed pk-write and pk-update in same batch
func TestMixedWriteAndUpdate(t *testing.T) {
	// First create a row to update
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10500, "id1", 10500),
				testclient.NewWriteColumnsKVs("col0", 500, "col1", 501),
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, writeOp)

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10500, "id1", 10500), http.StatusOK),
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10501, "id1", 10501), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Mixed batch: update existing + write new
	mixedOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10500, "id1", 10500),
				testclient.NewWriteColumnsKVs("col0", 550), // Update existing
				config.PK_UPDATE_OPERATION, http.StatusOK),
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10501, "id1", 10501),
				testclient.NewWriteColumnsKVs("col0", 600), // Write new
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, mixedOp)

	// Verify both operations succeeded
	t.Run("verify_mixed", func(t *testing.T) {
		readOp := api.BatchOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchSubOperationTestInfo{
				createReadSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10500, "id1", 10500),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 550, "col1", 501}), // col0 updated, col1 unchanged
				createReadSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10501, "id1", 10501),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 600}), // new row
			},
		}
		readAndVerifyData(t, readOp)
	})
}

// Test pk-insert operation (insert only, fail if exists)
func TestPkInsertOperation(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"
	id0, id1 := 10600, 10600

	// First insert should succeed
	t.Run("first_insert", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 600),
					config.PK_INSERT_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Second insert should fail (duplicate key - returns 409 Conflict)
	t.Run("duplicate_insert", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode:       []int{http.StatusConflict}, // Batch returns 409 on constraint violation
			ErrMsgContains: "Tuple already existed",
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 601),
					config.PK_INSERT_OPERATION, http.StatusConflict),
			},
		}
		writeTestData(t, writeOp)
	})
}

// ============================================================================
// SECTION 5: EDGE CASES AND BOUNDARY VALUES
// Tests NULL values, min/max values, empty strings, special characters
// ============================================================================

// Test NULL value handling - read existing NULL values from test database
// Note: batchwrite API requires writeColumns, so we test reading pre-existing NULL rows
func TestNullValues(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"

	// The test database (db004) has a row at (1,1) with NULL values for col0/col1
	// This was inserted in the db004.sql setup file
	t.Run("read_existing_null_row", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", 1, "id1", 1)
		if !verifyRowExists(t, db, table, filters) {
			t.Fatal("Pre-existing NULL row should exist at (1,1)")
		}
	})
}

// Test boundary values for integers
func TestBoundaryValues(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"

	// Test INT max and min
	t.Run("int_max", func(t *testing.T) {
		id0, id1 := 2147483646, uint32(4294967294) // Near max values
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 2147483647, "col1", uint32(4294967295)), // Max values
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)

		// Cleanup
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1), http.StatusOK),
			},
		}
		t.Cleanup(func() { deleteTestData(t, deleteOp) })
	})

	t.Run("int_min", func(t *testing.T) {
		id0, id1 := -2147483647, uint32(1) // Near min values
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", -2147483648, "col1", uint32(0)), // Min values
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)

		// Cleanup
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1), http.StatusOK),
			},
		}
		t.Cleanup(func() { deleteTestData(t, deleteOp) })
	})
}

// Test special characters in varchar columns
func TestSpecialCharacters(t *testing.T) {
	db := testdbs.DB014
	table := "table1"

	testCases := []struct {
		name  string
		value string
	}{
		{"spaces", "hello world"},
		{"unicode", "日本語テスト"},
		{"emoji", "test🎉emoji"},
		{"special", "test@#$%^&*()"},
		{"quotes", `test"quotes'here`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pkValue := testclient.EncodePkValue("special_"+tc.name, false, 50, false)

			writeOp := api.BatchWriteOperationTestInfo{
				HttpCode: []int{http.StatusOK},
				Operations: []api.BatchWriteSubOperationTestInfo{
					createWriteSubOp(db, table,
						testclient.NewFiltersKVs("id0", pkValue),
						testclient.NewWriteColumnsKVs("col0", tc.value),
						config.PK_WRITE_OPERATION, http.StatusOK),
				},
			}
			writeTestData(t, writeOp)

			// Verify row exists
			filters := testclient.NewFiltersKVs("id0", pkValue)
			if !verifyRowExists(t, db, table, filters) {
				t.Fatalf("Row with %s should exist", tc.name)
			}

			// Cleanup
			deleteOp := api.BatchDeleteOperationTestInfo{
				HttpCode: []int{http.StatusOK},
				Operations: []api.BatchDeleteSubOperationTestInfo{
					createDeleteSubOp(db, table, filters, http.StatusOK),
				},
			}
			deleteTestData(t, deleteOp)
		})
	}
}

// ============================================================================
// SECTION 6: ERROR HANDLING
// Tests expected error conditions
// ============================================================================

// Test update on non-existent row
func TestUpdateNonExistent(t *testing.T) {
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 99999, "id1", 99999),
				testclient.NewWriteColumnsKVs("col0", 999),
				config.PK_UPDATE_OPERATION, http.StatusNotFound), // Expect 404
		},
	}
	writeTestData(t, writeOp)
}

// Test read on non-existent row
func TestReadNonExistent(t *testing.T) {
	readOp := api.BatchOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchSubOperationTestInfo{
			createReadSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 99998, "id1", 99998),
				testclient.NewReadColumns("col", 2),
				http.StatusNotFound, // Expect 404
				nil),
		},
	}
	readAndVerifyData(t, readOp)
}

// Test delete on non-existent row
func TestDeleteNonExistent(t *testing.T) {
	deleteOp := api.BatchDeleteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchDeleteSubOperationTestInfo{
			createDeleteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 99997, "id1", 99997),
				http.StatusNotFound), // Expect 404
		},
	}
	deleteTestData(t, deleteOp)
}

// Test partial batch failure (some succeed, some fail)
func TestPartialBatchFailure(t *testing.T) {
	// First create a row that exists
	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10800, "id1", 10800),
				testclient.NewWriteColumnsKVs("col0", 800),
				config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
	writeTestData(t, writeOp)

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(testdbs.DB004, "int_table",
					testclient.NewFiltersKVs("id0", 10800, "id1", 10800), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Batch with mix of success and failure
	readOp := api.BatchOperationTestInfo{
		HttpCode: []int{http.StatusOK}, // Overall batch succeeds
		Operations: []api.BatchSubOperationTestInfo{
			createReadSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 10800, "id1", 10800),
				testclient.NewReadColumns("col", 2),
				http.StatusOK, // This should succeed
				nil),
			createReadSubOp(testdbs.DB004, "int_table",
				testclient.NewFiltersKVs("id0", 99996, "id1", 99996),
				testclient.NewReadColumns("col", 2),
				http.StatusNotFound, // This should fail
				nil),
		},
	}
	readAndVerifyData(t, readOp)
}

// ============================================================================
// SECTION 7: BLOB AND TEXT OPERATIONS
// Tests large object handling
// ============================================================================

// Test TEXT column operations
func TestTextColumnOperations(t *testing.T) {
	db := testdbs.DB013
	table := "text_table"

	pkValue := testclient.EncodePkValue("text_test_key", false, 255, false)
	textValue := "This is a test text value with some content. " +
		"Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
		"Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."

	// Write TEXT column
	t.Run("write_text", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					testclient.NewWriteColumnsKVs("col0", textValue, "col1", 123),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Verify row exists
	t.Run("verify_text", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", pkValue)
		if !verifyRowExists(t, db, table, filters) {
			t.Fatal("TEXT row should exist")
		}
	})
}

// Test BLOB column operations
func TestBlobColumnOperations(t *testing.T) {
	db := testdbs.DB013
	table := "blob_table"

	pkValue := testclient.EncodePkValue("blob_test_key", true, 255, false)
	blobData := []byte("This is binary data for testing BLOB columns \x00\x01\x02\x03")
	blobBase64 := base64.StdEncoding.EncodeToString(blobData)

	// Write BLOB column
	t.Run("write_blob", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					testclient.NewWriteColumnsKVs("col0", blobBase64, "col1", 456),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Verify row exists
	t.Run("verify_blob", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", pkValue)
		if !verifyRowExists(t, db, table, filters) {
			t.Fatal("BLOB row should exist")
		}
	})
}

// ============================================================================
// SECTION 8: VARBINARY OPERATIONS
// Tests binary data handling
// ============================================================================

// Test varbinary column operations
func TestVarbinaryOperations(t *testing.T) {
	db := testdbs.DB017
	table := "table1"

	pkValue := testclient.EncodePkValue("varbinary_test", true, 100, false)
	binaryColValue := testclient.EncodePkValue("binary_col_data", true, 100, false)

	// Write varbinary row
	t.Run("write_varbinary", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue),
					testclient.NewWriteColumnsKVs("col0", binaryColValue),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", pkValue), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Verify exists
	t.Run("verify_varbinary", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", pkValue)
		if !verifyRowExists(t, db, table, filters) {
			t.Fatal("Varbinary row should exist")
		}
	})
}

// ============================================================================
// SECTION 9: LARGE BATCH OPERATIONS
// Tests with many operations in a single batch
// ============================================================================

// Test large batch with 100 operations
func TestLargeBatch(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"
	baseId := 20000
	numOps := 100

	// Create 100 write operations
	writeOps := make([]api.BatchWriteSubOperationTestInfo, numOps)
	for i := 0; i < numOps; i++ {
		id := baseId + i
		writeOps[i] = createWriteSubOp(db, table,
			testclient.NewFiltersKVs("id0", id, "id1", id),
			testclient.NewWriteColumnsKVs("col0", i*10),
			config.PK_WRITE_OPERATION, http.StatusOK)
	}

	writeOp := api.BatchWriteOperationTestInfo{
		HttpCode:   []int{http.StatusOK},
		Operations: writeOps,
	}
	writeTestData(t, writeOp)

	// Cleanup
	t.Cleanup(func() {
		deleteOps := make([]api.BatchDeleteSubOperationTestInfo, numOps)
		for i := 0; i < numOps; i++ {
			id := baseId + i
			deleteOps[i] = createDeleteSubOp(db, table,
				testclient.NewFiltersKVs("id0", id, "id1", id), http.StatusOK)
		}
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode:   []int{http.StatusOK},
			Operations: deleteOps,
		}
		deleteTestData(t, deleteOp)
	})

	// Verify some random rows exist
	t.Run("verify_large_batch", func(t *testing.T) {
		// Check first, middle, and last rows
		checkIds := []int{baseId, baseId + numOps/2, baseId + numOps - 1}
		for _, id := range checkIds {
			filters := testclient.NewFiltersKVs("id0", id, "id1", id)
			if !verifyRowExists(t, db, table, filters) {
				t.Fatalf("Row %d should exist", id)
			}
		}
	})
}

// ============================================================================
// SECTION 10: IDEMPOTENCY TESTS
// Tests that operations behave correctly when repeated
// ============================================================================

// Test pk-write idempotency (can be called multiple times)
func TestPkWriteIdempotency(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"
	id0, id1 := 10900, 10900

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// First write
	t.Run("first_write", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 900),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// Second write with same key (should succeed and update)
	t.Run("second_write", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 901),
					config.PK_WRITE_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// Verify latest value
	t.Run("verify_latest", func(t *testing.T) {
		readOp := api.BatchOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchSubOperationTestInfo{
				createReadSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 901}), // Should be the second write value
			},
		}
		readAndVerifyData(t, readOp)
	})
}

// Test delete and reinsert
func TestDeleteAndReinsert(t *testing.T) {
	db := testdbs.DB004
	table := "int_table"
	id0, id1 := 11000, 11000

	t.Cleanup(func() {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Insert
	t.Run("initial_insert", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 1000),
					config.PK_INSERT_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// Delete
	t.Run("delete", func(t *testing.T) {
		deleteOp := api.BatchDeleteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchDeleteSubOperationTestInfo{
				createDeleteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1), http.StatusOK),
			},
		}
		deleteTestData(t, deleteOp)
	})

	// Verify deleted
	t.Run("verify_deleted", func(t *testing.T) {
		filters := testclient.NewFiltersKVs("id0", id0, "id1", id1)
		if !verifyRowDeleted(t, db, table, filters) {
			t.Fatal("Row should be deleted")
		}
	})

	// Reinsert with different value
	t.Run("reinsert", func(t *testing.T) {
		writeOp := api.BatchWriteOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchWriteSubOperationTestInfo{
				createWriteSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewWriteColumnsKVs("col0", 2000),
					config.PK_INSERT_OPERATION, http.StatusOK),
			},
		}
		writeTestData(t, writeOp)
	})

	// Verify new value
	t.Run("verify_reinserted", func(t *testing.T) {
		readOp := api.BatchOperationTestInfo{
			HttpCode: []int{http.StatusOK},
			Operations: []api.BatchSubOperationTestInfo{
				createReadSubOp(db, table,
					testclient.NewFiltersKVs("id0", id0, "id1", id1),
					testclient.NewReadColumns("col", 2),
					http.StatusOK,
					[]interface{}{"col0", 2000}),
			},
		}
		readAndVerifyData(t, readOp)
	})
}
