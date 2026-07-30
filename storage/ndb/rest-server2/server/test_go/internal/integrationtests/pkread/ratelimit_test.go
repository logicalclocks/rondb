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

package pkread

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

// pkReadRateLimitSender builds one pk-read request against int_table1 (PK
// id0,id1) and returns a closure that fires it with a shared client and
// reports the HTTP status. It reuses the package's normal request building
// blocks (testclient.NewFiltersKVs / NewOperationID, testutils.NewPKReadURL).
func pkReadRateLimitSender(t *testing.T) func(*http.Client) int {
	t.Helper()
	url := testutils.NewPKReadURL(testdbs.DB004, "int_table1")
	body, err := json.Marshal(api.PKReadBody{
		Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
		OperationID: testclient.NewOperationID(5),
	})
	if err != nil {
		t.Fatalf("marshal pk-read request: %v", err)
	}
	reqBody := string(body)
	return func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client, config.PK_HTTP_VERB,
			url, reqBody, "", http.StatusOK, http.StatusTooManyRequests)
		return code
	}
}

// TestRateLimit verifies that pk-read is subject to API key rate limiting
// (RONDB-978): a low-limit burst is throttled with 429 and raising the limit
// restores service.
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)
	testutils.RunEndpointRateLimitTest(t, pkReadRateLimitSender(t))
}

// TestRateLimitZeroIsUnlimited verifies the kernel "rate_per_sec == 0 means no
// limit" path. This is endpoint-agnostic kernel behaviour, so it is exercised
// once here via pk-read rather than in every endpoint package.
func TestRateLimitZeroIsUnlimited(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)
	testutils.RunZeroRateIsUnlimitedTest(t, pkReadRateLimitSender(t))
}
