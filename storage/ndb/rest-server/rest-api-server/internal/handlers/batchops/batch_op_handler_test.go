/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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
package batchops

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/handlers"
	tu "hopsworks.ai/rdrs/internal/handlers/utils"
	"hopsworks.ai/rdrs/pkg/api"
)

func TestBatchSimple(t *testing.T) {

	tests := map[string]api.BatchOperationTestInfo{
		"simple1": { //single operation batch
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       "DB004",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
		"simple2": { //small batch operation
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       "DB004",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB005/bigint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "bigint_table",
					DB:       "DB005",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
		"simple3": { // bigger batch of numbers table
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       "DB004",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB005/bigint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 0, "id1", 0),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "bigint_table",
					DB:       "DB005",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB006/tinyint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", -128, "id1", 0),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "tinyint_table",
					DB:       "DB006",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB007/smallint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 32767, "id1", 65535),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "smallint_table",
					DB:       "DB007",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB007/smallint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 1, "id1", 1),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "smallint_table",
					DB:       "DB007",
					HttpCode: http.StatusOK,
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
		"notfound": { // a batch operation with operations throwing 404
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB004/int_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 100, "id1", 100),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "int_table",
					DB:       "DB004",
					HttpCode: http.StatusNotFound,
					RespKVs:  []interface{}{"col0", "col1"},
				},
				api.BatchSubOperationTestInfo{
					SubOperation: api.BatchSubOp{
						Method:      &[]string{config.PK_HTTP_VERB}[0],
						RelativeURL: &[]string{string("DB005/bigint_table/" + config.PK_DB_OPERATION)}[0],
						Body: &api.PKReadBody{
							Filters:     tu.NewFiltersKVs("id0", 100, "id1", 100),
							ReadColumns: tu.NewReadColumns("col", 2),
							OperationID: tu.NewOperationID(64),
						},
					},
					Table:    "bigint_table",
					DB:       "DB005",
					HttpCode: http.StatusNotFound,
					RespKVs:  []interface{}{"col0", "col1"},
				},
			},
			ErrMsgContains: "",
		},
	}

	tu.BatchTest(t, tests, false, getBatchHandler())
}

func TestBatchDate(t *testing.T) {

	tests := map[string]api.BatchOperationTestInfo{
		"date": {
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				createSubOperation(t, "date_table", "DB019", "1111-11-11", http.StatusOK),
				createSubOperation(t, "date_table", "DB019", "1111-11-11 00:00:00", http.StatusOK),
				createSubOperation(t, "date_table", "DB019", "1111-11-12", http.StatusOK),
			},
		},
		"wrong_sub_op": {
			HttpCode: http.StatusBadRequest,
			Operations: []api.BatchSubOperationTestInfo{
				createSubOperation(t, "date_table", "DB019", "1111-11-11", http.StatusOK),
				createSubOperation(t, "date_table", "DB019", "1111-11-11 00:00:00", http.StatusOK),
				createSubOperation(t, "date_table", "DB019", "1111-11-12", http.StatusOK),
				createSubOperation(t, "date_table", "DB019", "1111-13-12", http.StatusOK),
			},
		},
	}

	tu.BatchTest(t, tests, false, getBatchHandler())
}

func TestBatchDateTime(t *testing.T) {

	tests := map[string]api.BatchOperationTestInfo{
		"date": {
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				createSubOperation(t, "date_table0", "DB020", "1111-11-11 11:11:11", http.StatusOK),
				createSubOperation(t, "date_table3", "DB020", "1111-11-11 11:11:11.123", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-11-11 11:11:11.123456", http.StatusOK),
				createSubOperation(t, "date_table0", "DB020", "1111-11-11 11:11:11.123123", http.StatusOK),
				createSubOperation(t, "date_table3", "DB020", "1111-11-11 11:11:11.123000", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-11-11 -11:11:11.123456", http.StatusOK),
				createSubOperation(t, "date_table0", "DB020", "1111-11-12 11:11:11", http.StatusOK),
				createSubOperation(t, "date_table3", "DB020", "1111-11-12 11:11:11.123", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-11-12 11:11:11.123456", http.StatusOK),
			},
			ErrMsgContains: "",
		},
		"wrong_sub_op": {
			HttpCode: http.StatusBadRequest,
			Operations: []api.BatchSubOperationTestInfo{
				createSubOperation(t, "date_table0", "DB020", "1111-11-11 11:11:11", http.StatusOK),
				createSubOperation(t, "date_table3", "DB020", "1111-11-11 11:11:11.123", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-11-11 11:11:11.123456", http.StatusOK),
				createSubOperation(t, "date_table0", "DB020", "1111-11-11 11:11:11.123123", http.StatusOK),
				createSubOperation(t, "date_table3", "DB020", "1111-11-11 11:11:11.123000", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-11-11 -11:11:11.123456", http.StatusOK),
				createSubOperation(t, "date_table0", "DB020", "1111-11-12 11:11:11", http.StatusOK),
				createSubOperation(t, "date_table3", "DB020", "1111-11-12 11:11:11.123", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-11-12 11:11:11.123456", http.StatusOK),
				createSubOperation(t, "date_table6", "DB020", "1111-13-11 11:11:11", http.StatusOK), //wrong op
			},
			ErrMsgContains: "",
		},
	}

	tu.BatchTest(t, tests, false, getBatchHandler())
}

func TestBatchTime(t *testing.T) {

	tests := map[string]api.BatchOperationTestInfo{
		"date": {
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				createSubOperation(t, "time_table0", "DB021", "11:11:11", http.StatusOK),
				createSubOperation(t, "time_table3", "DB021", "11:11:11.123", http.StatusOK),
				createSubOperation(t, "time_table6", "DB021", "11:11:11.123456", http.StatusOK),
				createSubOperation(t, "time_table0", "DB021", "11:11:11.123123", http.StatusOK),
				createSubOperation(t, "time_table3", "DB021", "11:11:11.123000", http.StatusOK),
				createSubOperation(t, "time_table0", "DB021", "12:11:11", http.StatusOK),
				createSubOperation(t, "time_table3", "DB021", "12:11:11.123", http.StatusOK),
				createSubOperation(t, "time_table6", "DB021", "12:11:11.123456", http.StatusOK),
			},
			ErrMsgContains: "",
		},
		"wrong_sub_op": {
			HttpCode: http.StatusBadRequest,
			Operations: []api.BatchSubOperationTestInfo{
				createSubOperation(t, "time_table0", "DB021", "11:11:11", http.StatusOK),
				createSubOperation(t, "time_table3", "DB021", "11:11:11.123", http.StatusOK),
				createSubOperation(t, "time_table6", "DB021", "11:11:11.123456", http.StatusOK),
				createSubOperation(t, "time_table0", "DB021", "11:11:11.123123", http.StatusOK),
				createSubOperation(t, "time_table3", "DB021", "11:11:11.123000", http.StatusOK),
				createSubOperation(t, "time_table0", "DB021", "12:11:11", http.StatusOK),
				createSubOperation(t, "time_table3", "DB021", "12:11:11.123", http.StatusOK),
				createSubOperation(t, "time_table6", "DB021", "12:11:11.123456", http.StatusOK),
				createSubOperation(t, "time_table6", "DB021", "11:61:11", http.StatusOK),
			},
			ErrMsgContains: "",
		},
	}

	tu.BatchTest(t, tests, false, getBatchHandler())
}

func createSubOperation(t *testing.T, table string, database string, pk string, expectedStatus int) api.BatchSubOperationTestInfo {
	respKVs := []interface{}{"col0"}
	return api.BatchSubOperationTestInfo{
		SubOperation: api.BatchSubOp{
			Method:      &[]string{config.PK_HTTP_VERB}[0],
			RelativeURL: &[]string{string(database + "/" + table + "/" + config.PK_DB_OPERATION)}[0],
			Body: &api.PKReadBody{
				Filters:     tu.NewFiltersKVs("id0", pk),
				ReadColumns: tu.NewReadColumns("col", 1),
				OperationID: tu.NewOperationID(5),
			},
		},
		Table:    table,
		DB:       database,
		HttpCode: expectedStatus,
		RespKVs:  respKVs,
	}
}

func TestBatchArrayTableChar(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB012", false, 100, true)
}

func TestBatchArrayTableVarchar(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB014", false, 50, false)
}

func TestBatchArrayTableLongVarchar(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "db015", false, 256, false)
}

func TestBatchArrayTableBinary(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB016", true, 100, true)
}

func TestBatchArrayTableVarbinary(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB017", true, 100, false)
}

func TestBatchArrayTableLongVarbinary(t *testing.T) {
	ArrayColumnBatchTest(t, "table1", "DB018", true, 256, false)
}

func ArrayColumnBatchTest(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool) {

	tests := map[string]api.BatchOperationTestInfo{
		"simple1": { // bigger batch of array column table
			HttpCode: http.StatusOK,
			Operations: []api.BatchSubOperationTestInfo{
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "-1", http.StatusNotFound),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "1", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "2", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "3", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "4", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "这是一个测验", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "5", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "6", http.StatusOK),
			},
			ErrMsgContains: "",
		},
	}

	tu.BatchTest(t, tests, isBinary, getBatchHandler())
}

/*
* A bad sub operation fails the entire batch
 */
func TestBatchBadSubOp(t *testing.T) {
	table := "table1"
	database := "DB018"
	isBinary := true
	padding := false
	colWidth := 256

	tests := map[string]api.BatchOperationTestInfo{
		"simple1": { // bigger batch of array column table
			HttpCode: http.StatusBadRequest,
			Operations: []api.BatchSubOperationTestInfo{
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "-1", http.StatusNotFound),
				//This is bad operation. data is longer than the column width
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, *tu.NewOperationID(colWidth*4 + 1), http.StatusNotFound),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "1", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "2", http.StatusOK),
				arrayColumnBatchTestSubOp(t, table, database, isBinary, colWidth, padding, "3", http.StatusOK),
			},
			ErrMsgContains: "",
		},
	}

	tu.BatchTest(t, tests, isBinary, getBatchHandler())
}

func arrayColumnBatchTestSubOp(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool, pk string, expectedStatus int) api.BatchSubOperationTestInfo {
	respKVs := []interface{}{"col0"}
	return api.BatchSubOperationTestInfo{
		SubOperation: api.BatchSubOp{
			Method:      &[]string{config.PK_HTTP_VERB}[0],
			RelativeURL: &[]string{string(database + "/" + table + "/" + config.PK_DB_OPERATION)}[0],
			Body: &api.PKReadBody{
				Filters:     tu.NewFiltersKVs("id0", tu.Encode(pk, isBinary, colWidth, padding)),
				ReadColumns: tu.NewReadColumns("col", 1),
				OperationID: tu.NewOperationID(5),
			},
		},
		Table:    table,
		DB:       database,
		HttpCode: expectedStatus,
		RespKVs:  respKVs,
	}
}

func TestBatchMissingReqField(t *testing.T) {
	tu.WithDBs(t, []string{"DB000"},
		getBatchHandler(), func(tc common.TestContext) {
			url := tu.NewBatchReadURL()
			// Test missing method
			operations := NewOperationsTBD(t, 3)
			operations[1].Method = nil
			operationsWrapper := api.BatchOpRequest{Operations: &operations}
			body, _ := json.Marshal(operationsWrapper)
			tu.SendHttpRequest(t, tc, config.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Error:Field validation for 'Method' failed ")

			// Test missing relative URL
			operations = NewOperationsTBD(t, 3)
			operations[1].RelativeURL = nil
			operationsWrapper = api.BatchOpRequest{Operations: &operations}
			body, _ = json.Marshal(operationsWrapper)
			tu.SendHttpRequest(t, tc, config.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Error:Field validation for 'RelativeURL' failed ")

			// Test missing body
			operations = NewOperationsTBD(t, 3)
			operations[1].Body = nil
			operationsWrapper = api.BatchOpRequest{Operations: &operations}
			body, _ = json.Marshal(operationsWrapper)
			tu.SendHttpRequest(t, tc, config.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Error:Field validation for 'Body' failed ")

			// Test missing filter in an operation
			operations = NewOperationsTBD(t, 3)
			*&operations[1].Body.Filters = nil
			operationsWrapper = api.BatchOpRequest{Operations: &operations}
			body, _ = json.Marshal(operationsWrapper)
			tu.SendHttpRequest(t, tc, config.BATCH_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Error:Field validation for 'Filters' failed")
		})
}

func NewOperationsTBD(t *testing.T, numOps int) []api.BatchSubOp {
	operations := make([]api.BatchSubOp, numOps)
	for i := 0; i < numOps; i++ {
		operations[i] = NewOperationTBD(t)
	}
	return operations
}

func NewOperationTBD(t *testing.T) api.BatchSubOp {
	pkOp := tu.NewPKReadReqBodyTBD()
	method := "POST"
	relativeURL := tu.NewPKReadURL("db", "table")

	op := api.BatchSubOp{
		Method:      &method,
		RelativeURL: &relativeURL,
		Body:        &pkOp,
	}

	return op
}

func getBatchHandler() *handlers.AllHandlers {
	return &handlers.AllHandlers{
		Stater:   nil,
		Batcher:  GetBatcher(),
		PKReader: nil,
	}
}
