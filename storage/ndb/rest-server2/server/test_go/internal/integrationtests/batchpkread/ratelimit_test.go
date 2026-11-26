/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// rateLimitSubOp is one pk-read of (id0, id1) = (0, 0) in db.table, whose
// primary key every table used here has.
func rateLimitSubOp(db, table string) api.BatchSubOp {
	method := config.PK_HTTP_VERB
	relURL := db + "/" + table + "/" + config.PK_DB_OPERATION
	return api.BatchSubOp{
		Method:      &method,
		RelativeURL: &relURL,
		Body: &api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
			OperationID: testclient.NewOperationID(5),
		},
	}
}

func marshalBatch(t *testing.T, ops []api.BatchSubOp) string {
	t.Helper()
	body, err := json.Marshal(api.BatchOpRequest{Operations: &ops})
	if err != nil {
		t.Fatalf("marshal batch pk-read request: %v", err)
	}
	return string(body)
}

// TestRateLimit verifies that batch pk-read is subject to API key rate
// limiting (RONDB-978). The batch bundles several pk-read sub-operations,
// each of which becomes an NDB transaction tagged with the request identity.
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)

	ops := make([]api.BatchSubOp, 10)
	for i := range ops {
		ops[i] = rateLimitSubOp(testdbs.DB004, "int_table1")
	}
	reqBody := marshalBatch(t, ops)
	url := testutils.NewBatchReadURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client, config.BATCH_HTTP_VERB,
			url, reqBody, "", http.StatusOK, http.StatusTooManyRequests)
		return code
	}, testdbs.DB004)
}

// TestRateLimitMixedBatchBillsEachProject verifies that a batch over two
// projects bills each operation to its own project (RONDB-1158): capping
// db005 throttles a batch that lists a db004 read first. A transaction
// carries one identity, and before the per-identity transaction groups the
// whole batch was billed to the first operation's project, so a caller
// with a capped project could read it on an uncapped project's budget by
// prepending one of that project's reads. Only in username mode do the two
// databases bill to different identities.
func TestRateLimitMixedBatchBillsEachProject(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)
	if config.GetAll().RateLimit.Identity != "username" {
		t.Skip("Skipping: every database bills to the key's identity outside username mode")
	}
	ops := []api.BatchSubOp{rateLimitSubOp(testdbs.DB004, "int_table1")}
	for i := 0; i < 9; i++ {
		ops = append(ops, rateLimitSubOp(testdbs.DB005, "bigint_table"))
	}
	reqBody := marshalBatch(t, ops)
	url := testutils.NewBatchReadURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client, config.BATCH_HTTP_VERB,
			url, reqBody, "", http.StatusOK, http.StatusTooManyRequests)
		return code
	}, testdbs.DB005)
}
