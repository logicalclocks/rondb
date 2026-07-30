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

// TestRateLimit verifies that batch pk-read is subject to API key rate
// limiting (RONDB-978). The batch bundles several pk-read sub-operations,
// each of which becomes an NDB transaction tagged with the request identity.
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)

	method := config.PK_HTTP_VERB
	relURL := testdbs.DB004 + "/int_table1/" + config.PK_DB_OPERATION
	subOp := api.BatchSubOp{
		Method:      &method,
		RelativeURL: &relURL,
		Body: &api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
			OperationID: testclient.NewOperationID(5),
		},
	}
	ops := make([]api.BatchSubOp, 10)
	for i := range ops {
		ops[i] = subOp
	}
	body, err := json.Marshal(api.BatchOpRequest{Operations: &ops})
	if err != nil {
		t.Fatalf("marshal batch pk-read request: %v", err)
	}
	reqBody := string(body)
	url := testutils.NewBatchReadURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client, config.BATCH_HTTP_VERB,
			url, reqBody, "", http.StatusOK, http.StatusTooManyRequests)
		return code
	})
}
