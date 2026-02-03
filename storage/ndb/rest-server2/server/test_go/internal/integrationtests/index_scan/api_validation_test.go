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

package index_scan

import (
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// TestAPIValidation_Filter tests filter-related validation errors
func TestAPIValidation_Filter(t *testing.T) {
	testDB := testdbs.DB029
	testTable := "tiny_tbl"

	tests := map[string]api.IndexTestInfo{
		// AND with empty args (missing filters)
		"filter_logic_missing_args": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:   "AND",
					Args: []*api.ScanFilter{}, // empty args
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Filter with empty op
		"filter_missing_op": { // TODO : Missing required field for the isnotnull op: column . this is not user friendly message
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "", // empty op
					Column: "pk",
					Cond:   "EQ",
					Value:  1,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Unknown filter op
		"filter_unknown_op": { // TODO : fix error message
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "FOOBAR",
					Column: "pk",
					Cond:   "EQ",
					Value:  1,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Invalid CMP condition
		"filter_cmp_invalid_cond": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "EQUALS", // invalid, should be EQ
					Value:  1,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// AND with only 1 child (requires 2)
		"filter_logic_one_child": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// AND with 3 children (requires exactly 2)
		"filter_logic_three_children": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op: "AND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "GT",
							Value:  0,
						},
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "LT",
							Value:  100,
						},
						{
							Op:     "CMP",
							Column: "val_1",
							Cond:   "EQ",
							Value:  1,
						},
					},
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// CMP without column (empty string)
		"filter_cmp_missing_column": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "", // missing
					Cond:   "EQ",
					Value:  1,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// CMP without cond (empty string)
		"filter_cmp_missing_cond": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "", // missing
					Value:  1,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// CMP without value (nil)
		"filter_cmp_missing_value": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "EQ",
					Value:  nil, // missing
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// ISNULL without column
		"filter_isnull_missing_column": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "ISNULL",
					Column: "", // missing
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// ISNOTNULL without column
		"filter_isnotnull_missing_column": { // TODO: bad error message "Unknown op"
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "ISNOTNULL",
					Column: "", // missing
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Filter on non-existent column
		"filter_nonexistent_column": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "nonexistent_column",
					Cond:   "EQ",
					Value:  1,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

// TestAPIValidation_FilterLogicOps tests NAND and NOR operations work correctly
func TestAPIValidation_FilterLogicOps(t *testing.T) {
	testDB := testdbs.DB029
	testTable := "tiny_tbl"

	tests := map[string]api.IndexTestInfo{
		// NAND operation should work
		"filter_nand_operation": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op: "NAND",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "val_1",
							Cond:   "EQ",
							Value:  10,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MAY_NOT_MATCH,
		},
		// NOR operation should work
		"filter_nor_operation": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op: "NOR",
					Args: []*api.ScanFilter{
						{
							Op:     "CMP",
							Column: "pk",
							Cond:   "EQ",
							Value:  1,
						},
						{
							Op:     "CMP",
							Column: "val_1",
							Cond:   "EQ",
							Value:  10,
						},
					},
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MAY_NOT_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

// TestAPIValidation_Index tests index-related validation errors
func TestAPIValidation_Index(t *testing.T) {
	testDB := testdbs.DB029
	testTable := "big_tbl"

	tests := map[string]api.IndexTestInfo{
		// Index with missing name (empty string)
		"index_missing_name": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
				Index: &api.IndexScan{
					Name:       "", // missing  // TODO bad error. Field validation for 'DB' failed on the 'min' tag:
					KeyColumns: []string{"val_1", "val_2"},
					Ranges: []api.RangeScan{
						{
							Lower: api.BoundedScan{
								Values:    []any{0, 0},
								Inclusive: true,
							},
							Upper: api.BoundedScan{
								Values:    []any{100, 100},
								Inclusive: false,
							},
						},
					},
					Order: "asc",
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Index with nil key_columns (missing)
		"index_key_columns_nil": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: nil, // nil key_columns
					Ranges: []api.RangeScan{
						{
							Lower: api.BoundedScan{
								Values:    []any{0, 0},
								Inclusive: true,
							},
							Upper: api.BoundedScan{
								Values:    []any{100, 100},
								Inclusive: false,
							},
						},
					},
					Order: "asc",
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Index with empty key_columns
		"index_key_columns_empty": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{}, // empty
					Ranges: []api.RangeScan{
						{
							Lower: api.BoundedScan{
								Values:    []any{0, 0},
								Inclusive: true,
							},
							Upper: api.BoundedScan{
								Values:    []any{100, 100},
								Inclusive: false,
							},
						},
					},
					Order: "asc",
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Index with invalid order
		"index_order_invalid": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges: []api.RangeScan{
						{
							Lower: api.BoundedScan{
								Values:    []any{0, 0},
								Inclusive: true,
							},
							Upper: api.BoundedScan{
								Values:    []any{100, 100},
								Inclusive: false,
							},
						},
					},
					Order: "ascending", // invalid, should be "asc" or "desc"
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Non-existent index name
		"index_nonexistent": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
				Index: &api.IndexScan{
					Name:       "nonexistent_index",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges: []api.RangeScan{
						{
							Lower: api.BoundedScan{
								Values:    []any{0, 0},
								Inclusive: true,
							},
							Upper: api.BoundedScan{
								Values:    []any{100, 100},
								Inclusive: false,
							},
						},
					},
					Order: "asc",
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
		// Index with empty ranges
		"index_ranges_empty": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "LT",
					Value:  10,
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     []api.RangeScan{}, // empty ranges
					Order:      "asc",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MAY_NOT_MATCH,
		},
		// Index bound with empty values
		"index_bound_values_empty": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "LT",
					Value:  10,
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges: []api.RangeScan{
						{
							Lower: api.BoundedScan{
								Values:    []any{}, // empty values
								Inclusive: true,
							},
							Upper: api.BoundedScan{
								Values:    []any{100, 100},
								Inclusive: false,
							},
						},
					},
					Order: "asc",
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

// TestAPIValidation_ReadColumns tests readColumns validation errors
func TestAPIValidation_ReadColumns(t *testing.T) {
	testDB := testdbs.DB029
	testTable := "tiny_tbl"

	nonexistentCol := "nonexistent_column"

	tests := map[string]api.IndexTestInfo{
		// readColumns with non-existent column
		"readcolumns_nonexistent_column": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				ReadColumns: &[]api.ReadColumn{
					{Column: &nonexistentCol},
				},
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
			},
			Table:               testTable,
			DB:                  testDB,
			ExpectedHttpCode:    http.StatusBadRequest,
			BodyContains:        EMPTY_STRING,
			SkipMySQLValidation: true,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

// TestAPIValidation_Limit tests limit field validation
func TestAPIValidation_Limit(t *testing.T) {
	testDB := testdbs.DB029
	testTable := "tiny_tbl"

	tests := map[string]api.IndexTestInfo{
		// limit=0 should return empty result
		"limit_zero": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 0,
				Filters: &api.ScanFilter{
					Op:     "CMP",
					Column: "pk",
					Cond:   "GT",
					Value:  0,
				},
			},
			Table:            testTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK, // limit=0 should return empty result
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MAY_NOT_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

// TestAPIValidation_MissingLimit tests that missing limit field returns error
// This test uses raw JSON because Go structs default int to 0
func TestAPIValidation_MissingLimit(t *testing.T) {
	url := NewIndexScanURL(testdbs.DB029, "tiny_tbl")

	// Request without limit field
	body := `{"filters": {"op": "CMP", "column": "pk", "cond": "EQ", "value": 1}}`
	respCode, _ := testclient.SendHttpRequest(t, http.MethodPost, url, body,
		EMPTY_STRING, http.StatusBadRequest)
	if respCode != http.StatusBadRequest {
		t.Fatalf("Expected %d for missing limit, got %d", http.StatusBadRequest, respCode)
	}
}

// TestAPIValidation_NullLimit tests that null limit field returns error
func TestAPIValidation_NullLimit(t *testing.T) {
	url := NewIndexScanURL(testdbs.DB029, "tiny_tbl")

	// Request with null limit
	body := `{"limit": null, "filters": {"op": "CMP", "column": "pk", "cond": "EQ", "value": 1}}`
	respCode, _ := testclient.SendHttpRequest(t, http.MethodPost, url, body,
		EMPTY_STRING, http.StatusBadRequest)
	if respCode != http.StatusBadRequest {
		t.Fatalf("Expected %d for null limit, got %d", http.StatusBadRequest, respCode)
	}
}
