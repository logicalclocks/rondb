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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// reproTarget is one secondary-index scan plus the marker that, if it appears
// in the response, proves the scan read the OTHER table. db025.table_3 holds
// col1 marker "MARKER_TABLE_3" and table_4 holds "MARKER_TABLE_4" (both reachable
// via the same ix_col0 key col0='col0_data'), so a table_3 scan returning
// "MARKER_TABLE_4" (or a table_4 scan returning "MARKER_TABLE_3") is the bug.
type reproTarget struct {
	table       string
	otherMarker string
	query       api.IndexScanQuery
}

type reproResult struct {
	stats      scanStats
	wrongReads int
	sample     string
}

// Test_IndexScanReadsWrongTableAfterRecreate reproduces the customer report:
// after dropping and recreating two identical-schema tables, a secondary-index
// scan on one table returns the other table's data.
//
// table_3 / table_4 (added to db025) are an identical pair, each with one
// secondary index ix_col0. Workers hammer ix_col0 scans on both tables over
// PERSISTENT connections that span the recreate (the stale cache is per REST
// server thread, so reusing the connection keeps hitting the thread holding the
// pre-drop index object). The recreate is driven through DB025-Update.sql
// (reverse create order to remap the NDB table ids). Any response carrying the
// other table's marker is the bug. A table/filter scan (no secondary index) on
// the same tables does NOT leak, which pins the cached secondary-INDEX object
// as the source.
func Test_IndexScanReadsWrongTableAfterRecreate(t *testing.T) {
	db := testdbs.DB025

	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
		t.Fatalf("failed to reset database. Error: %v", err)
	}
	defer func() { // restore the standard db025 fixture
		if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
			t.Fatalf("failed to re-set database. Error: %v", err)
		}
	}()

	targets := []reproTarget{
		{table: "table_3", otherMarker: "MARKER_TABLE_4", query: indexScanByCol0("col0_data")},
		{table: "table_4", otherMarker: "MARKER_TABLE_3", query: indexScanByCol0("col0_data")},
	}

	numWorkers := 20
	var stop atomic.Bool
	done := make(chan reproResult, numWorkers)
	for i := 0; i < numWorkers; i++ {
		go runReproLoad(t, db, targets, &stop, done)
	}

	// Warm every REST server thread's dictionary cache (table + index objects).
	time.Sleep(5 * time.Second)

	t.Log("Dropping and recreating both tables (DB025-Update.sql, reverse order)...")
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025UpdateScheme); err != nil {
		t.Fatalf("failed to recreate tables. Error: %v", err)
	}
	t.Log("Recreated")

	// Keep scanning after the recreate, where a stale cached object can now
	// resolve to the other table.
	time.Sleep(5 * time.Second)
	stop.Store(true)

	total := newScanStats()
	totalWrong := 0
	sample := ""
	for i := 0; i < numWorkers; i++ {
		r := <-done
		total.merge(r.stats)
		totalWrong += r.wrongReads
		if sample == "" {
			sample = r.sample
		}
	}
	total.log(t)
	if total.ops == 0 {
		t.Fatalf("no scans issued - test did not run correctly")
	}

	if totalWrong > 0 {
		t.Errorf("REPRODUCED: %d index-scan responses returned the wrong table's data "+
			"(e.g. %s) out of %d requests", totalWrong, sample, total.ops)
	} else {
		t.Logf("No wrong-table reads observed in %d requests", total.ops)
	}
}

// indexScanByCol0 builds a secondary-index scan over ix_col0 selecting rows
// whose col0 equals the given value.
func indexScanByCol0(col0 string) api.IndexScanQuery {
	return api.IndexScanQuery{
		Limit: 100,
		Index: &api.IndexScan{
			Name:       "ix_col0",
			KeyColumns: []string{"col0"},
			Ranges: []api.RangeScan{
				{
					Lower: api.BoundedScan{Values: []any{col0}, Inclusive: true},
					Upper: api.BoundedScan{Values: []any{col0}, Inclusive: true},
				},
			},
			Order: "asc",
		},
	}
}

// runReproLoad scans both tables in a tight loop until stop is set, flagging any
// response whose data contains the other table's marker.
func runReproLoad(t *testing.T, db string, targets []reproTarget, stop *atomic.Bool, done chan<- reproResult) {
	client := testutils.SetupHttpClient(t)
	res := reproResult{stats: newScanStats()}
	defer func() { done <- res }()

	for !stop.Load() {
		for _, target := range targets {
			body, err := json.Marshal(target.query)
			if err != nil {
				t.Errorf("failed to marshal %s query: %v", target.table, err)
				return
			}
			// 4xx/5xx are expected transients while the tables are being recreated.
			code, respBody := testclient.SendHttpRequestWithClient(
				t, client, http.MethodPost, NewIndexScanURL(db, target.table), string(body),
				EMPTY_STRING,
				http.StatusOK, http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError,
			)
			res.stats.ops++
			res.stats.statusCodes[code]++
			if code != http.StatusOK {
				if ndbCode, ok := extractRonDBErrorCode(respBody); ok {
					res.stats.rondbErrors[ndbCode]++
				}
				continue
			}

			// A scan of one table must never contain the other table's marker.
			if strings.Contains(string(respBody), target.otherMarker) {
				res.wrongReads++
				if res.sample == "" {
					res.sample = fmt.Sprintf("scan of %s returned %q", target.table, snippet(respBody, target.otherMarker))
				}
			}
		}
	}
}

// snippet returns a short window of body around the first occurrence of token,
// for a readable failure message.
func snippet(body []byte, token string) string {
	s := string(body)
	i := strings.Index(s, token)
	if i < 0 {
		return s
	}
	start := i - 20
	if start < 0 {
		start = 0
	}
	end := i + len(token) + 20
	if end > len(s) {
		end = len(s)
	}
	return s[start:end]
}
