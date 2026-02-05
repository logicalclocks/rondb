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
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

// Helper to create a write sub-operation
func createWriteSubOp(db, table string, filters *[]api.Filter, writeCols *[]api.WriteColumn, opType string, expectedStatus int) api.BatchWriteSubOperationTestInfo {
	method := config.PK_HTTP_VERB
	relURL := db + "/" + table + "/" + opType
	return api.BatchWriteSubOperationTestInfo{
		SubOperation: api.BatchWriteSubOp{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKWriteBody{
				Filters:      filters,
				WriteColumns: writeCols,
				OperationID:  testclient.NewOperationID(64),
			},
		},
		Table:    table,
		DB:       db,
		HttpCode: []int{expectedStatus},
	}
}

// Helper to create a read sub-operation
func createReadSubOp(db, table string, filters *[]api.Filter, readCols *[]api.ReadColumn, expectedStatus int, respKVs []interface{}) api.BatchSubOperationTestInfo {
	method := config.PK_HTTP_VERB
	relURL := testutils.NewBatchPKReadURL(db, table)
	return api.BatchSubOperationTestInfo{
		SubOperation: api.BatchSubOp{
			Method:      &method,
			RelativeURL: &relURL,
			Body: &api.PKReadBody{
				Filters:     filters,
				ReadColumns: readCols,
				OperationID: testclient.NewOperationID(64),
			},
		},
		Table:    table,
		DB:       db,
		HttpCode: []int{expectedStatus},
		RespKVs:  respKVs,
	}
}

// Helper to create a delete sub-operation
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

// Helper to create a full write operation for a table
func createWriteOp(db, table string, filters *[]api.Filter, writeCols *[]api.WriteColumn) api.BatchWriteOperationTestInfo {
	return api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{
			createWriteSubOp(db, table, filters, writeCols, config.PK_WRITE_OPERATION, http.StatusOK),
		},
	}
}

// Dispatch functions for tests
func runWriteTest(t *testing.T, testInfo api.BatchWriteOperationTestInfo) {
	if config.GetAll().REST.Enable {
		runWriteRESTTest(t, testInfo)
	}
}

func runReadTest(t *testing.T, testInfo api.BatchOperationTestInfo) {
	if config.GetAll().REST.Enable {
		runReadRESTTest(t, testInfo)
	}
}

func runDeleteTest(t *testing.T, testInfo api.BatchDeleteOperationTestInfo) {
	if config.GetAll().REST.Enable {
		runDeleteRESTTest(t, testInfo)
	}
}

// Write test data using batchwrite endpoint
func writeTestData(t *testing.T, testInfo api.BatchWriteOperationTestInfo) {
	t.Helper()
	runWriteTest(t, testInfo)
}

// Read and verify data using batch read endpoint
func readAndVerifyData(t *testing.T, testInfo api.BatchOperationTestInfo) {
	t.Helper()
	runReadTest(t, testInfo)
}

// Delete data using batchdelete endpoint
func deleteTestData(t *testing.T, testInfo api.BatchDeleteOperationTestInfo) {
	t.Helper()
	runDeleteTest(t, testInfo)
}

// Verify row exists by attempting to read it (expects 200)
func verifyRowExists(t *testing.T, db, table string, filters *[]api.Filter) bool {
	t.Helper()
	testInfo := api.BatchOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchSubOperationTestInfo{
			createReadSubOp(db, table, filters, nil, http.StatusOK, nil),
		},
	}
	return verifyRowExistsREST(t, testInfo)
}

// Verify row was deleted by attempting to read it (expects 404)
func verifyRowDeleted(t *testing.T, db, table string, filters *[]api.Filter) bool {
	t.Helper()
	testInfo := api.BatchOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchSubOperationTestInfo{
			createReadSubOp(db, table, filters, nil, http.StatusNotFound, nil),
		},
	}
	return verifyRowDeletedREST(t, testInfo)
}
