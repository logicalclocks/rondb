/*
 * Copyright (C) 2025 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

package index_scan

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// scanTarget is one index scan exercised during the schema change.
type scanTarget struct {
	table string
	query api.IndexScanQuery
}

// scanStats records what the workers observed during the schema change: total
// requests, the HTTP status distribution, and the NDB error codes carried in
// the transient (non-200) error bodies.
type scanStats struct {
	ops         int
	statusCodes map[int]int // HTTP status code -> count
	rondbErrors map[int]int // NDB error code (parsed from error body) -> count
}

func newScanStats() scanStats {
	return scanStats{statusCodes: map[int]int{}, rondbErrors: map[int]int{}}
}

func (s *scanStats) merge(o scanStats) {
	s.ops += o.ops
	for code, n := range o.statusCodes {
		s.statusCodes[code] += n
	}
	for code, n := range o.rondbErrors {
		s.rondbErrors[code] += n
	}
}

// log prints the request total, the transient-error count, and the breakdowns
// by HTTP status and by NDB error code (both sorted by code).
func (s *scanStats) log(t *testing.T) {
	transient := s.ops - s.statusCodes[http.StatusOK]
	t.Logf("Scan requests during schema change: %d total, %d transient errors", s.ops, transient)
	t.Logf("HTTP status breakdown: %s", formatCounts(s.statusCodes))
	t.Logf("NDB error code breakdown (from transient bodies): %s", formatCounts(s.rondbErrors))
}

// formatCounts renders a code->count map as "code=count" pairs sorted by code.
func formatCounts(counts map[int]int) string {
	codes := make([]int, 0, len(counts))
	for code := range counts {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		parts = append(parts, strconv.Itoa(code)+"="+strconv.Itoa(counts[code]))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

// rondbErrorCodeRe matches the NDB error code in a server error body, which has
// the form "Error: ... Error: code: <N> MySQL Code: <M> Message: ..." (see
// __RS_ERROR_RONDB in status.hpp). The lowercase "code:" deliberately does not
// match "MySQL Code:".
var rondbErrorCodeRe = regexp.MustCompile(`code: (\d+)`)

func extractRonDBErrorCode(body []byte) (int, bool) {
	m := rondbErrorCodeRe.FindSubmatch(body)
	if m == nil {
		return 0, false
	}
	code, err := strconv.Atoi(string(m[1]))
	if err != nil {
		return 0, false
	}
	return code, true
}

// TestUnloadSchema is the concurrent counterpart to
// Test_SchemaVersionChangeNonConcurrent. It mirrors batchpkread's
// TestUnloadSchema: many worker goroutines hammer the index scan endpoint in a
// tight loop while the schema is dropped and recreated (DB025-Update.sql) on
// the main goroutine, maximising the chance of an in-flight scan hitting a
// table whose schema version is changing (NDB error 241).
//
// Two scans run concurrently: table_1 (VARCHAR primary key) and table_2 (INT
// primary key). The data of every successful response is verified against MySQL
// by reading the same scan from MySQL immediately afterwards and comparing with
// CompareResults: the schema change takes far longer than that REST->MySQL gap,
// so both observe the same committed schema version, and any difference is data
// corruption.
func TestUnloadSchema(t *testing.T) {
	// Reset database at start.
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
		t.Fatalf("failed to reset database. Error: %v", err)
	}
	defer func() { // reset database at end
		if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
			t.Fatalf("failed to re-set database. Error: %v", err)
		}
	}()

	db := testdbs.DB025
	targets := []scanTarget{
		{table: "table_1", query: idEqQuery("1")}, // VARCHAR primary key
		{table: "table_2", query: idEqQuery(1)},   // INT primary key
	}

	// Hammer both scans concurrently while the schema changes underneath them.
	numWorkers := 1
	var stop atomic.Bool
	done := make(chan scanStats, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go runScanLoad(t, i, db, targets, &stop, done)
	}

	// Let requests run so all REST server threads cache the schema.
	time.Sleep(5 * time.Second)

	t.Log("Changing schema...")
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025UpdateScheme); err != nil {
		t.Fatalf("failed to update schema. Error: %v", err)
	}
	t.Log("Schema changed")

	// Keep hammering for a bit after the change, then stop and drain workers.
	time.Sleep(5 * time.Second)
	stop.Store(true)
	total := newScanStats()
	for i := 0; i < numWorkers; i++ {
		total.merge(<-done)
	}
	total.log(t)
	if total.ops == 0 {
		t.Fatalf("no scans issued - test did not run correctly")
	}

	// Recovery check: both scans must return the correct new data after the change.
	for _, target := range targets {
		verifyScanAgainstMySQL(t, db, target.query, target.table)
	}
}

// idEqQuery builds an index scan selecting the row whose primary key id0 equals
// idValue (a string for table_1, an int for table_2).
func idEqQuery(idValue any) api.IndexScanQuery {
	return api.IndexScanQuery{
		Limit: 100,
		Filters: &api.ScanFilter{
			Op:     "CMP",
			Column: "id0",
			Cond:   "EQ",
			Value:  idValue,
		},
	}
}

// runScanLoad issues both scans in a tight loop until stop is set, verifying the
// data of every successful response against MySQL, and returns how many requests
// it made. It reuses testclient.SendHttpRequestWithClient - the shared sender
// that accepts a set of acceptable status codes - so the transient errors
// expected while the schema is changing don't abort the test.
func runScanLoad(t *testing.T, workerID int, db string, targets []scanTarget, stop *atomic.Bool, done chan<- scanStats) {
	client := testutils.SetupHttpClient(t)
	stats := newScanStats()
	defer func() { done <- stats }()

	for !stop.Load() {
		for _, target := range targets {
			body, err := json.Marshal(target.query)
			if err != nil {
				t.Errorf("failed to marshal %s query: %v", target.table, err)
				return
			}
			// 4xx/5xx are expected transients while the table is being recreated.
			code, respBody := testclient.SendHttpRequestWithClient(
				t, client, http.MethodPost, NewIndexScanURL(db, target.table), string(body),
				EMPTY_STRING,
				http.StatusOK, http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError,
			)
			stats.ops++
			stats.statusCodes[code]++
			if code != http.StatusOK {
				// Record the NDB error code carried in the error body, if any.
				if ndbCode, ok := extractRonDBErrorCode(respBody); ok {
					stats.rondbErrors[ndbCode]++
				}
				continue // tolerated transient
			}

			restRows, restCols, err := parseScanResponse(respBody)
			if err != nil {
				t.Errorf("worker %d: failed to parse %s response: %v", workerID, target.table, err)
				continue
			}
			if len(restRows) == 0 {
				continue // brief committed CREATE-before-INSERT window: empty table
			}

			// Read the same scan from MySQL right after and compare.
			query := target.query
			mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, db, target.table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
			if err != nil || len(mysqlRows) == 0 {
				continue // table mid-recreate on the MySQL side too; skip
			}
			CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
		}
	}
}

// parseScanResponse decodes a 200 index scan response body into ordered rows and
// column names, mirroring ExecuteUsingRESTServer's parsing (json.Number kept as
// string). ExecuteUsingRESTServer parses identically but re-issues the request
// and aborts on non-200, so it can't inspect a body already fetched with
// tolerant status codes.
func parseScanResponse(body []byte) ([][]any, []string, error) {
	var resp api.IndexScanResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		return nil, nil, err
	}
	if len(resp.Data) == 0 {
		return nil, nil, nil
	}
	cols, err := extractColumnNamesInOrder(body)
	if err != nil {
		return nil, nil, err
	}
	rows := make([][]any, len(resp.Data))
	for i, rowMap := range resp.Data {
		row := make([]any, len(cols))
		for j, col := range cols {
			if num, ok := rowMap[col].(json.Number); ok {
				row[j] = num.String()
			} else {
				row[j] = rowMap[col]
			}
		}
		rows[i] = row
	}
	return rows, cols, nil
}

// verifyScanAgainstMySQL runs the scan via REST and via MySQL and asserts they
// agree, reusing the package's existing helpers. Called from the test goroutine.
func verifyScanAgainstMySQL(t *testing.T, db string, query api.IndexScanQuery, table string) {
	t.Helper()
	mysqlRows, mysqlCols, err := ExecuteUsingMySQLServer(t, db, table, &query, DATA_DOES_NOT_NEED_BINARY_ENCODING)
	if err != nil {
		t.Fatalf("MySQL query for %s failed: %v", table, err)
	}
	restRows, restCols, _, err := ExecuteUsingRESTServer(t, db, table, &query, EMPTY_STRING, http.StatusOK)
	if err != nil {
		t.Fatalf("REST query for %s failed: %v", table, err)
	}
	CompareResults(t, mysqlRows, mysqlCols, restRows, restCols, ROWS_ORDER_MAY_NOT_MATCH)
}
