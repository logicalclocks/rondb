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

// ronsqlBody builds a RonSQL request for a query over db029, the same
// database the index-scan rate limit test bills to. TEXT_NOHEADER keeps the
// body to the bare result values.
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
	runRateLimitTest(t, "SELECT SUM(val_1) FROM tiny_tbl;")
}

// RONSQL-49: RonSQL tagged only the transaction of a single-table aggregate
// with the rate limit identity. A non-aggregate query and a join started
// their transactions untagged, so they were never throttled, and a WHERE
// subquery ran in an inner preparer without the identity.

// TestRateLimitPassthrough: a single-table non-aggregate query.
func TestRateLimitPassthrough(t *testing.T) {
	runRateLimitTest(t, "SELECT pk, val_1 FROM tiny_tbl;")
}

// TestRateLimitJoin: a join query.
func TestRateLimitJoin(t *testing.T) {
	runRateLimitTest(t, "SELECT SUM(b.val_2) AS s FROM tiny_tbl AS t "+
		"JOIN big_tbl AS b ON b.pk = t.pk;")
}

// TestRateLimitSubquery: an IN subquery. The subquery runs first, so it
// is often the one that is throttled, and that must be a 429 too, not a
// "Subquery execution failed" error.
func TestRateLimitSubquery(t *testing.T) {
	runRateLimitTest(t, "SELECT SUM(val_1) AS s FROM tiny_tbl "+
		"WHERE val_1 IN (SELECT MAX(val_1) FROM big_tbl);")
}

// RONDB-1124 T-5: the other executor paths.  A key read classifies its
// NDB error in throw_classified_ndb_error / throw_key_read_error, not in
// handle_ronsql_exception, so the rate limit has to reach 429 there too.

// TestRateLimitPkLookupAggregate: an aggregate over the key reads of an IN
// list (execute_pk_lookup_aggregate).
func TestRateLimitPkLookupAggregate(t *testing.T) {
	runRateLimitTest(t, "SELECT SUM(val_1) AS s FROM tiny_tbl "+
		"WHERE pk IN (1, 2, 3);")
}

// TestRateLimitPkLookup: a pass-through key read.
func TestRateLimitPkLookup(t *testing.T) {
	runRateLimitTest(t, "SELECT pk, val_1 FROM tiny_tbl WHERE pk = 1;")
}

// TestRateLimitPkLookupInList: pass-through key reads of an IN list.
func TestRateLimitPkLookupInList(t *testing.T) {
	runRateLimitTest(t, "SELECT pk, val_1 FROM tiny_tbl WHERE pk IN (1, 2);")
}

// TestRateLimitCountStar: the COUNT(*) shortcut, which reads the
// fragments' row counts instead of the rows.
func TestRateLimitCountStar(t *testing.T) {
	runRateLimitTest(t, "SELECT COUNT(*) AS n FROM tiny_tbl;")
}

// TestRateLimitCteJoin: a join of a table with an aggregating CTE.
func TestRateLimitCteJoin(t *testing.T) {
	runRateLimitTest(t, "WITH c AS (SELECT pk AS k, SUM(val_1) AS s "+
		"FROM big_tbl GROUP BY pk) SELECT SUM(c.s) AS s FROM tiny_tbl AS t "+
		"JOIN c ON c.k = t.pk;")
}

// TestRateLimitLastN: the last N rows of a table aggregated in RonSQL
// (the API-side aggregate over a pass-through scan).
func TestRateLimitLastN(t *testing.T) {
	runRateLimitTest(t, "WITH t AS (SELECT pk, val_1 FROM tiny_tbl "+
		"ORDER BY pk DESC LIMIT 5) SELECT COUNT(*) AS n, SUM(val_1) AS s "+
		"FROM t;")
}

// runRateLimitTest runs the shared endpoint rate limit test with query over
// db029: a low limit must give some 429s and nothing but 200 and 429.
func runRateLimitTest(t *testing.T, query string) {
	testutils.SkipIfRateLimitsDisabled(t)

	reqBody := ronsqlBody(testdbs.DB029, query)
	url := testutils.NewRonSQLURL()

	testutils.RunEndpointRateLimitTest(t, func(client *http.Client) int {
		code, _ := testclient.SendHttpRequestWithClient(t, client,
			config.RONSQL_HTTP_VERB, url, reqBody, "",
			http.StatusOK, http.StatusTooManyRequests)
		return code
	})
}
