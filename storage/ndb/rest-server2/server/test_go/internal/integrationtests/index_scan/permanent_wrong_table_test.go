/*
 * Copyright (C) 2026 Hopsworks AB
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
	"net/http"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// Test_IndexScanWrongTablePermanent reproduces the customer's PERMANENT failure
// mode (distinct from the transient Test_IndexScanReadsWrongTableAfterRecreate):
// after two identical tables are dropped and recreated, an index scan keeps
// silently returning the OTHER table's data and never recovers (until rdrs
// restart), with no errors.
//
// Root cause: the NDB API keys its index cache by base-table id
// (sys/def/<tabid>/<index_name>). When the recreate reuses/swaps base-table ids,
// a stale cached base table for perm_a resolves its index to perm_b's storage;
// because the two tables are identical and recreated together they share a
// schema version, so the kernel raises no error - nothing triggers rdrs's
// reactive invalidation, so the stale cache is never refreshed.
//
// The test uses a SINGLE pinned connection (sequential requests reuse one
// connection -> one rdrs server thread, so the same dictionary cache is warmed
// and later re-checked). It warms the cache, recreates WITHOUT scanning during
// the change (so no transient error heals the cache), lets the schema fully
// SETTLE, and then asserts every scan returns its OWN table's data. A permanent
// stale cache makes perm_a keep returning PERM_MARKER_B after settle -> failure.
func Test_IndexScanWrongTablePermanent(t *testing.T) {
	db := testdbs.DB025

	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
		t.Fatalf("failed to reset database. Error: %v", err)
	}
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025PermScheme); err != nil {
		t.Fatalf("failed to create perm tables. Error: %v", err)
	}
	defer func() { // restore the standard db025 fixture (drops perm_a/perm_b)
		if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
			t.Fatalf("failed to re-set database. Error: %v", err)
		}
	}()

	query := indexScanByCol0("shared_key")

	// Pinned connection: one client, sequential requests reuse one connection,
	// hence one rdrs server thread (one dictionary cache) throughout.
	client := testutils.SetupHttpClient(t)

	// 1. Warm that server thread's cache for both tables and their ix_col0.
	for i := 0; i < 100; i++ {
		permScan(t, client, db, "perm_a", query)
		permScan(t, client, db, "perm_b", query)
	}
	t.Log("cache warmed; recreating both tables in reverse order (no scans during the change)...")

	// 2. Drop + recreate in reverse order (swap base-table ids). No scans run
	//    during the change, so no transient error invalidates the cache.
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025PermUpdateScheme); err != nil {
		t.Fatalf("failed to recreate perm tables. Error: %v", err)
	}

	// 3. Let the schema fully settle so any transient state has resolved.
	time.Sleep(8 * time.Second)

	// 4. Recovery check on the SAME connection: each table must return its own
	//    marker. If the cache is permanently stale, perm_a keeps returning
	//    perm_b's row (silently) and never recovers.
	wrongA, wrongB, okA := 0, 0, 0
	for i := 0; i < 300; i++ {
		if body, ok := permScan(t, client, db, "perm_a", query); ok {
			if strings.Contains(body, "PERM_MARKER_B") {
				wrongA++
			} else if strings.Contains(body, "PERM_MARKER_A") {
				okA++
			}
		}
		if body, ok := permScan(t, client, db, "perm_b", query); ok {
			if strings.Contains(body, "PERM_MARKER_A") {
				wrongB++
			}
		}
	}
	t.Logf("after settle: perm_a returned perm_b's data %d times, perm_b returned perm_a's data %d times, perm_a correct %d times",
		wrongA, wrongB, okA)

	if wrongA > 0 || wrongB > 0 {
		t.Errorf("PERMANENT wrong-table read: after the schema settled, an index scan kept "+
			"returning the other table's data (perm_a->B=%d, perm_b->A=%d) - rdrs did not recover",
			wrongA, wrongB)
	}
}

// permScan issues one ix_col0 scan over the pinned connection and returns the
// 200 response body (and whether the response was 200). Transient 4xx/5xx are
// tolerated (and not expected here, since scans only run before the change and
// after it has settled).
func permScan(t *testing.T, client *http.Client, db, table string, query api.IndexScanQuery) (string, bool) {
	body, err := json.Marshal(query)
	if err != nil {
		t.Errorf("failed to marshal %s query: %v", table, err)
		return "", false
	}
	code, resp := testclient.SendHttpRequestWithClient(
		t, client, http.MethodPost, NewIndexScanURL(db, table), string(body),
		EMPTY_STRING,
		http.StatusOK, http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError,
	)
	if code != http.StatusOK {
		return "", false
	}
	return string(resp), true
}
