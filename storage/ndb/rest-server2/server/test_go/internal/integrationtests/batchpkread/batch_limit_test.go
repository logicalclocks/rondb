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
package batchpkread

import (
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1144: Internal.MaxOperationsPerBatchRequest bounds the number of
// operations in one batch request. It is checked while parsing, before any
// buffer is taken, and answered with 413. MaxOperationsPerNdbExecute is not
// a cap: it is the number of operations sent to NDB per execute round.

// intTableOps builds n identical reads of db004.int_table, each expected to
// succeed.
func intTableOps(n uint32) []api.BatchSubOperationTestInfo {
	ops := make([]api.BatchSubOperationTestInfo, n)
	for i := uint32(0); i < n; i++ {
		ops[i] = api.BatchSubOperationTestInfo{
			SubOperation: api.BatchSubOp{
				Method:      &[]string{config.PK_HTTP_VERB}[0],
				RelativeURL: &[]string{string(testdbs.DB004 + "/int_table/" + config.PK_DB_OPERATION)}[0],
				Body: &api.PKReadBody{
					Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
					ReadColumns: testclient.NewReadColumns("col", 2),
				},
			},
			Table:    "int_table",
			DB:       testdbs.DB004,
			HttpCode: []int{http.StatusOK},
			RespKVs:  []interface{}{"col0", "col1"},
		}
	}
	return ops
}

// A batch of exactly the limit succeeds and every sub-operation is answered.
// At the default limit the body is about 130 KB, which also crosses Drogon's
// 64 KiB in-memory threshold, so the file-buffered body path is covered.
func TestBatchAtOperationLimit(t *testing.T) {
	limit := uint32(testutils.GetLimits(t).MaxOperationsPerBatchRequest)
	tests := map[string]api.BatchOperationTestInfo{
		"at_limit": {
			HttpCode:   []int{http.StatusOK},
			Operations: intTableOps(limit),
		},
	}
	batchTestMultiple(t, tests, false, false)
}

// One operation more than the limit is rejected with 413 and a message that
// names the setting, before any sub-operation runs.
func TestBatchOverOperationLimit(t *testing.T) {
	limit := uint32(testutils.GetLimits(t).MaxOperationsPerBatchRequest)
	tests := map[string]api.BatchOperationTestInfo{
		"over_limit": {
			HttpCode:       []int{http.StatusRequestEntityTooLarge},
			ErrMsgContains: "exceeds MaxOperationsPerBatchRequest",
			Operations:     intTableOps(limit + 1),
		},
	}
	batchTestMultiple(t, tests, false, false)
}
