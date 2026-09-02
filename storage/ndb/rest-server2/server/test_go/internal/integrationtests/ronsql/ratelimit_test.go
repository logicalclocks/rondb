/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

package ronsql

import (
	"fmt"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// ronsqlBody builds a RonSQL request for an aggregation over db029/tiny_tbl,
// the same database the index-scan rate limit test bills to. TEXT_NOHEADER
// keeps the body to the bare aggregate value.
func ronsqlBody(database string, query string) string {
	return fmt.Sprintf(`{"query": %q, "database": %q, "outputFormat": "TEXT_NOHEADER"}`,
		query, database)
}

// TestRateLimit verifies that the RonSQL endpoint is subject to user rate
// limiting (RONDB-978) and, crucially, that a throttled aggregation is
// reported as HTTP 429 like every other endpoint rather than as a 500.
//
// RonSQL reaches the rate limit through two distinct paths and both must land
// on 429: the data nodes reject the aggregation scan itself, and once the
// client-side backoff window is open NdbTransaction::setUserId fails outright
// before any scan starts. Both surface as a std::runtime_error inside
// RonSQLPreparer::execute, which is why the Ndb error has to be classified in
// handle_ronsql_exception (as RonSQLRateLimitError) instead of being folded
// into the generic temporary/permanent verdict - the latter answered 500 and
// burned RonSQL's three retries against an already overflowing bucket.
func TestRateLimit(t *testing.T) {
	testutils.SkipIfRateLimitsDisabled(t)

	reqBody := ronsqlBody(testdbs.DB029, "SELECT SUM(val_1) FROM tiny_tbl;")
	url := testutils.NewRonSQLURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client,
			config.RONSQL_HTTP_VERB, url, reqBody, "",
			http.StatusOK, http.StatusTooManyRequests)
		return code
	})
}
