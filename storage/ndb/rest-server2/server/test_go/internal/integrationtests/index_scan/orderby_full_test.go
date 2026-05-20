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

	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// Tests for ordered index scan when readColumns does NOT contain (all of) the
// index key columns. Before the SF_OrderByFull patch in rdrs_dal.cpp, every
// such request failed with NDB error 4341 ("Not all keys read when using
// option SF_OrderBy") because rdrs2 built the result-mask bitmap purely from
// readColumns, but SF_OrderBy requires the kernel-side merge-sort to receive
// the index key columns.
//
// Switching to SF_OrderByFull lets NDB inject the index keys into the
// internal result mask while rdrs2's JSON projection (driven by its own
// read_columns vector at rdrs_dal.cpp:2024 and :2170) keeps the response
// limited to whatever the user asked for. The "no key leakage" property is
// enforced implicitly by CompareResults: the MySQL helper SELECTs exactly the
// requested columns, so any leak from REST shows up as a column-count
// mismatch.
//
// Coverage matrix:
//   - secondary composite index (idx_val on val_1, val_2): ASC/DESC, with
//     zero / partial / all keys in readColumns, plus the read-all path
//   - primary index (PRIMARY on pk): ASC/DESC, with zero / all keys
//   - big_tbl variants exercise multi-fragment merge-sort (1000 rows over
//     two ndbmtds), which is the path SF_OrderByFull actually changes.

func TestOrderByFull_SecondaryIndex(t *testing.T) {
	testDB := testdbs.DB029
	tinyTable := "tiny_tbl"
	bigTable := "big_tbl"

	content := "content"
	val1 := "val_1"
	val2 := "val_2"

	// Wide range that covers every row in either table.
	wideRange := []api.RangeScan{{
		Lower: api.BoundedScan{Values: []any{0, 0}, Inclusive: true},
		Upper: api.BoundedScan{Values: []any{1000, 1000}, Inclusive: false},
	}}

	tests := map[string]api.IndexTestInfo{
		// 1) Zero index keys in readColumns, ASC. Pre-patch this returned NDB
		//    4341. Post-patch, response contains only `content`.
		"secondary_only_nonkey_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit:       10,
				ReadColumns: &[]api.ReadColumn{{Column: &content}},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     wideRange,
					Order:      "asc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 2) DESC variant of (1).
		"secondary_only_nonkey_desc": {
			IndexScanReq: api.IndexScanQuery{
				Limit:       10,
				ReadColumns: &[]api.ReadColumn{{Column: &content}},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     wideRange,
					Order:      "desc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 3) Partial keys: val_1 included, val_2 missing. Pre-patch this also
		//    returned 4341 because the mask is still incomplete.
		"secondary_partial_key_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				ReadColumns: &[]api.ReadColumn{
					{Column: &val1},
					{Column: &content},
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     wideRange,
					Order:      "asc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 4) Both keys present + extra non-key column. Baseline regression
		//    guard: the previously-working path must still work after the
		//    flag swap.
		"secondary_full_keys_plus_extra_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				ReadColumns: &[]api.ReadColumn{
					{Column: &val1},
					{Column: &val2},
					{Column: &content},
				},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     wideRange,
					Order:      "asc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 5) ReadColumns omitted entirely. rdrs2 passes a nullptr result mask,
		//    so the flag swap should not affect this path. Regression guard.
		"secondary_read_all_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     wideRange,
					Order:      "asc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 6) Multi-fragment merge-sort: 1000 rows on big_tbl across two
		//    ndbmtds, only `content` requested, first 50 ascending. This is
		//    the case where SF_OrderByFull's mask injection actually matters
		//    — without the index keys in the kernel-side result mask, the
		//    merge step has nothing to compare on.
		"secondary_merge_sort_only_nonkey_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit:       50,
				ReadColumns: &[]api.ReadColumn{{Column: &content}},
				Index: &api.IndexScan{
					Name:       "idx_val",
					KeyColumns: []string{"val_1", "val_2"},
					Ranges:     wideRange,
					Order:      "asc",
				},
			},
			Table:            bigTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}

func TestOrderByFull_PrimaryIndex(t *testing.T) {
	testDB := testdbs.DB029
	tinyTable := "tiny_tbl"
	bigTable := "big_tbl"

	pk := "pk"
	content := "content"

	// tiny_tbl has pk = 0..4
	tinyRange := []api.RangeScan{{
		Lower: api.BoundedScan{Values: []any{0}, Inclusive: true},
		Upper: api.BoundedScan{Values: []any{4}, Inclusive: true},
	}}
	// big_tbl has pk = 0..999
	bigRange := []api.RangeScan{{
		Lower: api.BoundedScan{Values: []any{0}, Inclusive: true},
		Upper: api.BoundedScan{Values: []any{1000}, Inclusive: false},
	}}

	tests := map[string]api.IndexTestInfo{
		// 7) Primary index, only non-key column, ASC. Pre-patch: NDB 4341.
		"primary_only_nonkey_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit:       10,
				ReadColumns: &[]api.ReadColumn{{Column: &content}},
				Index: &api.IndexScan{
					Name:       "PRIMARY",
					KeyColumns: []string{"pk"},
					Ranges:     tinyRange,
					Order:      "asc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 8) DESC PK variant.
		"primary_only_nonkey_desc": {
			IndexScanReq: api.IndexScanQuery{
				Limit:       10,
				ReadColumns: &[]api.ReadColumn{{Column: &content}},
				Index: &api.IndexScan{
					Name:       "PRIMARY",
					KeyColumns: []string{"pk"},
					Ranges:     tinyRange,
					Order:      "desc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 9) PK + extra non-key column, ASC. Baseline regression guard.
		"primary_full_key_plus_extra_asc": {
			IndexScanReq: api.IndexScanQuery{
				Limit: 10,
				ReadColumns: &[]api.ReadColumn{
					{Column: &pk},
					{Column: &content},
				},
				Index: &api.IndexScan{
					Name:       "PRIMARY",
					KeyColumns: []string{"pk"},
					Ranges:     tinyRange,
					Order:      "asc",
				},
			},
			Table:            tinyTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},

		// 10) PK merge-sort across fragments, only non-key column, DESC,
		//     first 50 by pk descending.
		"primary_merge_sort_only_nonkey_desc": {
			IndexScanReq: api.IndexScanQuery{
				Limit:       50,
				ReadColumns: &[]api.ReadColumn{{Column: &content}},
				Index: &api.IndexScan{
					Name:       "PRIMARY",
					KeyColumns: []string{"pk"},
					Ranges:     bigRange,
					Order:      "desc",
				},
			},
			Table:            bigTable,
			DB:               testDB,
			ExpectedHttpCode: http.StatusOK,
			BodyContains:     EMPTY_STRING,
			RowsOrder:        ROWS_ORDER_MUST_MATCH,
		},
	}

	indexScanTestMultiple(t, tests, DATA_DOES_NOT_NEED_BINARY_ENCODING)
}
