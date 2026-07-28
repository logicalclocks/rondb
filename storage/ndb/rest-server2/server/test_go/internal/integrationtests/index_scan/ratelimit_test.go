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
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// TestRateLimit verifies that the index scan endpoint is subject to API key
// rate limiting (RONDB-978). A scan runs on a buddy transaction that must
// also carry the request identity. It reuses the package's canonical scan
// target (db029/tiny_tbl, filter val_1 >= 1) and its URL builder;
// ExecuteUsingRESTServer is not reused because it only accepts a single
// expected status, whereas a burst legitimately returns 200 or 429.
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)

	query := api.IndexScanQuery{
		Limit: 10,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "val_1",
			Cond:   "GE",
			Value:  1,
		},
	}
	body, err := json.Marshal(query)
	if err != nil {
		t.Fatalf("marshal scan query: %v", err)
	}
	reqBody := string(body)
	url := NewIndexScanURL(testdbs.DB029, "tiny_tbl")

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client, config.SCAN_HTTP_VERB,
			url, reqBody, "", http.StatusOK, http.StatusTooManyRequests)
		return code
	})
}
