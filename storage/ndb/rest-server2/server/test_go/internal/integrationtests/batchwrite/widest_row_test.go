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

// RONDB-1144 on 26.10: a write that names every column of the widest table
// the limits allow (4095 columns here, 511 on 26.02) is the widest real
// row a write can carry. It must fit its view in
// the request chain, the row must come back intact through pk-read, and
// the pool must hold no buffer of ours afterwards.
func TestBatchWriteWidestRow(t *testing.T) {
	spec := testutils.WidestRowSpec(testutils.GetLimits(t))
	const table = "widest_write"
	const id = 7
	testutils.CreateWideRowTable(t, testdbs.DB004, table, spec, nil)
	before := testutils.TakePoolSnapshot(t)

	want := testutils.ExpectedWideValue(spec, 'b')
	kvs := make([]interface{}, 0, 2*spec.NCols)
	for i := 0; i < spec.NCols; i++ {
		kvs = append(kvs, spec.ColumnName(i), want)
	}
	batchWriteTest(t, api.BatchWriteOperationTestInfo{
		HttpCode: []int{http.StatusOK},
		Operations: []api.BatchWriteSubOperationTestInfo{{
			SubOperation: api.BatchWriteSubOp{
				Method:      &[]string{config.PK_HTTP_VERB}[0],
				RelativeURL: &[]string{testdbs.DB004 + "/" + table + "/" + config.PK_WRITE_OPERATION}[0],
				Body: &api.PKWriteBody{
					Filters:      testclient.NewFiltersKVs("id", id),
					WriteColumns: testclient.NewWriteColumnsKVs(kvs...),
					OperationID:  testclient.NewOperationID(8),
				},
			},
			Table:    table,
			DB:       testdbs.DB004,
			HttpCode: []int{http.StatusOK},
		}},
	})

	_, resp := testclient.SendHttpRequest(t, config.PK_HTTP_VERB, testutils.NewPKReadURL(testdbs.DB004, table),
		`{"filters":[{"column":"id","value":7}]}`, "", http.StatusOK)
	var read api.PKReadResponseJSON
	if err := json.Unmarshal(resp, &read); err != nil || read.Data == nil {
		t.Fatalf("read back: %v", err)
	}
	if got := len(*read.Data); got != spec.NCols+1 {
		t.Fatalf("read back %d columns, want %d", got, spec.NCols+1)
	}
	for i := 0; i < spec.NCols; i++ {
		raw := (*read.Data)[spec.ColumnName(i)]
		var s string
		if raw == nil || json.Unmarshal(*raw, &s) != nil || s != want {
			t.Fatalf("column %d differs after the widest write", i)
		}
	}
	testutils.AssertPoolBalanced(t, before)
}
