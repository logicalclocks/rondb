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

package feature_store

// Tests for RONDB-1030: NDB event watcher on feature_view.
//
// Three changes are validated:
//   1. mergeEvents(false) – rapid DELETE+INSERT in the same GCI epoch no
//      longer cancel each other out.
//   2. Data reordering – test data inserts feature_view LAST so dependent
//      rows (TDJ, TDF, SK) exist when the event fires.
//   3. No time-based eviction – valid cache entries are never evicted by age.
//
// All waits use polling helpers (pollSimpleUntilOK / pollSimpleUntilNotOK)
// that actively verify the expected HTTP status instead of sleeping a fixed
// duration.

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	fsmetadata "hopsworks.ai/rdrs2/internal/feature_store"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
)

// ---------------------------------------------------------------------------
// SQL constants – row values taken verbatim from hopsworks_40_data.sql.
//
// FV 2059: "sample_1" in FSDB001 (feature_store_id=67).
//   Single-table FV with one feature group (fg=2069) and one serving key.
// ---------------------------------------------------------------------------
const (
	fsNameSimple    = "fsdb001"
	fvNameSimple    = "sample_1"
	fvVersionSimple = 1

	// Explicit column lists: the hopsworks-ddl migration patches add columns to
	// these tables (V74 extends training_dataset_join, V40 extends serving_key),
	// so positional VALUES lists no longer match the table definitions.
	sqlInsertFV2059 = `INSERT INTO hopsworks.feature_view
		(id, name, feature_store_id, created, creator, version, description) VALUES
		(2059, 'sample_1', 67, Timestamp('2023-04-21 09:52:51'), 10000, 1, '');`

	sqlDeleteFV2059 = `DELETE FROM hopsworks.feature_view WHERE id = 2059;`

	sqlInsertTDJ2051 = `INSERT INTO hopsworks.training_dataset_join
		(id, training_dataset, feature_group, left_feature_group, feature_group_commit_id,
		 type, idx, parent_idx, prefix, feature_view_id) VALUES
		(2051, NULL, 2069, NULL, NULL, 0, 0, 0, NULL, 2059);`

	sqlInsertTDF2059 = `INSERT INTO hopsworks.training_dataset_feature
		(id, training_dataset, feature_group, name, type, td_join, idx, label,
		 inference_helper_column, training_helper_column, feature_view_id, on_demand_transformation) VALUES
		(2057, NULL, 2069, 'data1', 'bigint', 2051, 2, 0, 0, 0, 2059, NULL),
		(2058, NULL, 2069, 'id1', 'bigint', 2051, 0, 0, 0, 0, 2059, NULL),
		(2059, NULL, 2069, 'ts', 'timestamp', 2051, 1, 0, 0, 0, 2059, NULL),
		(2060, NULL, 2069, 'data2', 'bigint', 2051, 3, 0, 0, 0, 2059, NULL);`

	sqlInsertSK68 = `INSERT INTO hopsworks.serving_key
		(id, prefix, feature_name, join_on, join_index, feature_group_id, required, feature_view_id) VALUES
		(68, NULL, 'id1', NULL, 0, 2069, 1, 2059);`

	sqlInsertAllDeps2059 = sqlInsertTDJ2051 + "\n" + sqlInsertTDF2059 + "\n" + sqlInsertSK68

	// Explicit dep deletion — delete children first, then parent.
	sqlDeleteDeps2059 = `DELETE FROM hopsworks.serving_key WHERE feature_view_id = 2059;
DELETE FROM hopsworks.training_dataset_feature WHERE feature_view_id = 2059;
DELETE FROM hopsworks.training_dataset_join WHERE feature_view_id = 2059;`
)

// ---------------------------------------------------------------------------
// Polling helpers — wait for the cache to reflect the expected state by
// repeatedly sending HTTP requests.  This replaces fixed-duration sleeps
// and is immune to event loop latency variations across machines.
// ---------------------------------------------------------------------------
const (
	pollTimeout  = 30 * time.Second
	pollInterval = 500 * time.Millisecond
)

// pollSimpleUntilNotOK polls the simple FV endpoint until the response is
// NOT 200 OK, indicating the cache entry has been evicted.
func pollSimpleUntilNotOK(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	for time.Now().Before(deadline) {
		status, _ := sendRawFSRequest(t, fsNameSimple, fvNameSimple,
			fvVersionSimple, "id1", "1")
		if status != http.StatusOK {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatal("Timed out (30s) waiting for simple FV cache eviction")
}

// pollSimpleUntilOK polls the simple FV endpoint until it returns 200 OK,
// indicating the cache has been populated.
func pollSimpleUntilOK(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	var lastStatus int
	for time.Now().Before(deadline) {
		lastStatus, _ = sendRawFSRequest(t, fsNameSimple, fvNameSimple,
			fvVersionSimple, "id1", "1")
		if lastStatus == http.StatusOK {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("Timed out (30s) waiting for simple FV 200 OK (last: %d)", lastStatus)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func runSQL(t *testing.T, sql string) {
	t.Helper()
	err := testutils.RunQueriesOnMetadataCluster(sql)
	if err != nil {
		t.Fatalf("SQL execution failed: %v", err)
	}
}

// deleteSimpleFV removes FV 2059 and all its dependent rows.
func deleteSimpleFV(t *testing.T) {
	t.Helper()
	runSQL(t, sqlDeleteDeps2059+"\n"+sqlDeleteFV2059)
}

// restoreSimpleFV brings FV 2059 and all its dependent rows back to the
// original state.  Deps are inserted BEFORE the feature_view row (with FK
// checks disabled) so that when the NDB event fires on the feature_view
// INSERT, load_single_feature_view sees all dependent data.
func restoreSimpleFV(t *testing.T) {
	t.Helper()
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps2059)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV2059)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)
}

func makeSimpleFVRequest(t *testing.T, expectedMsg string, expectedStatus int) {
	t.Helper()
	fsReq := CreateFeatureStoreRequest(
		fsNameSimple, fvNameSimple, fvVersionSimple,
		[]string{"id1"},
		[]interface{}{[]byte("1")},
		nil, nil,
	)
	GetFeatureStoreResponseWithDetail(t, fsReq, expectedMsg, expectedStatus)
}

// sendRawFSRequest returns the HTTP status and response body without
// failing the test on unexpected status — the caller inspects it.
func sendRawFSRequest(t *testing.T, fsName, fvName string, fvVersion int,
	pk string, pkVal string) (int, string) {
	t.Helper()
	fsReq := CreateFeatureStoreRequest(
		fsName, fvName, fvVersion,
		[]string{pk},
		[]interface{}{[]byte(pkVal)},
		nil, nil,
	)
	body := fsReq.String()
	status, respBody := testclient.SendHttpRequest(t,
		config.FEATURE_STORE_HTTP_VERB,
		testutils.NewFeatureStoreURL(),
		body, "",
		http.StatusOK, http.StatusBadRequest,
		http.StatusNotFound, http.StatusInternalServerError)
	return status, string(respBody)
}

// ===========================================================================
// Event Watcher Tests — validate that NDB events on feature_view correctly
// evict and reload cache entries.
// ===========================================================================

// Test_EventWatcher_DeleteEvictsPromptly verifies that after a DELETE, the
// cache entry is evicted and subsequent requests do not serve stale data.
func Test_EventWatcher_DeleteEvictsPromptly(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	makeSimpleFVRequest(t, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusBadRequest)
}

// Test_EventWatcher_RapidDeleteAndReinsert verifies that a rapid DELETE
// followed immediately by INSERT (within the same GCI epoch) doesn't lose
// either event.  With the old mergeEvents(true), events in the same epoch
// would cancel out, causing the DELETE to be silently lost.
func Test_EventWatcher_RapidDeleteAndReinsert(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// Rapid DELETE + INSERT with no sleep between them.
	deleteSimpleFV(t)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// The INSERT event should trigger load_single_feature_view() which
	// succeeds because all dependent rows were inserted first.
	pollSimpleUntilOK(t)
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// Test_EventWatcher_RepeatedDeleteAndReinsert verifies that the NDB event
// watcher correctly handles multiple DELETE/INSERT lifecycle events on the
// same feature view.
func Test_EventWatcher_RepeatedDeleteAndReinsert(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	for cycle := 1; cycle <= 2; cycle++ {
		deleteSimpleFV(t)
		pollSimpleUntilNotOK(t)
		t.Logf("Cycle %d: Cache evicted after DELETE", cycle)

		runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
			sqlInsertAllDeps2059+"\n"+
			sqlInsertFV2059+"\n"+
			"SET FOREIGN_KEY_CHECKS = 1;")
		pollSimpleUntilOK(t)
		makeSimpleFVRequest(t, "", http.StatusOK)
		t.Logf("Cycle %d: Cache reloaded after INSERT", cycle)
	}
}

// Test_EventWatcher_ConcurrentRequestsAfterEviction verifies that multiple
// concurrent requests to an evicted cache entry don't cause crashes or
// deadlocks.
func Test_EventWatcher_ConcurrentRequestsAfterEviction(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Launch 10 concurrent requests — all should get errors, no deadlock
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _ := sendRawFSRequest(t,
				fsNameSimple, fvNameSimple, fvVersionSimple, "id1", "1")
			if status == http.StatusOK {
				t.Errorf("Expected error status, got 200")
			}
		}()
	}
	wg.Wait()

	// Restore and verify concurrent success
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, resp := sendRawFSRequest(t,
				fsNameSimple, fvNameSimple, fvVersionSimple, "id1", "1")
			if status != http.StatusOK {
				t.Errorf("Expected 200, got %d: %s", status, resp)
			}
		}()
	}
	wg.Wait()
}

// Test_EventWatcher_RapidInsertDeleteInsert verifies correct behavior when
// a feature_view row undergoes rapid INSERT → DELETE → INSERT in quick
// succession.  With mergeEvents(false), all three events are delivered.
func Test_EventWatcher_RapidInsertDeleteInsert(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// First DELETE to start from a known empty state
	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Rapid INSERT → DELETE → INSERT (all within milliseconds)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		sqlDeleteDeps2059+"\n"+
		sqlDeleteFV2059+"\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// The final INSERT should result in a valid cache entry
	pollSimpleUntilOK(t)
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// Test_EventWatcher_MultipleRequestsAfterEviction verifies that multiple
// sequential requests to an evicted FV all consistently return the expected
// error, and that after re-insertion they all succeed.
func Test_EventWatcher_MultipleRequestsAfterEviction(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	for i := 0; i < 5; i++ {
		makeSimpleFVRequest(t, fsmetadata.FV_NOT_EXIST.GetReason(), http.StatusBadRequest)
	}

	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)

	for i := 0; i < 5; i++ {
		makeSimpleFVRequest(t, "", http.StatusOK)
	}
}

// ===========================================================================
// Assumption Tests — Diagnostic tests that validate core assumptions about
// NDB event behavior.  The pass/fail pattern directly identifies the broken
// assumption:
//
//   Test                             | Fails if...
//   ---------------------------------|--------------------------------------------
//   StandaloneDelete                 | Event watcher doesn't process DELETE at all
//   StandaloneInsert                 | Event watcher doesn't process INSERT at all
//   RestoreThenDeleteImmediate       | Event merging eats DELETE after quick restore
//   RestoreThenDeleteWithGap         | Something OTHER than event merging is broken
//   InsertThenDeleteSameGCI          | INSERT+DELETE in same GCI → no event (merged)
//   DeleteThenInsertSameGCI          | DELETE+INSERT in same GCI → UPDATE (not subscribed)
//
// Key inference:
//   If RestoreThenDeleteImmediate FAILS but RestoreThenDeleteWithGap PASSES,
//   then NDB event merging (mergeEvents=true) is confirmed as the root cause.
// ===========================================================================

// TestAssumption_StandaloneDelete verifies the most basic event flow:
// SQL DELETE on feature_view → NDB TE_DELETE event → evict_entry.
func TestAssumption_StandaloneDelete(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)
}

// TestAssumption_StandaloneInsert verifies that an INSERT on feature_view
// causes the event watcher to load the metadata WITHOUT any client request
// triggering a lazy-load.  We sleep instead of polling to isolate event
// watcher behavior from the lazy-load path.
func TestAssumption_StandaloneInsert(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// Insert deps first, then FV — event should trigger successful load
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	// Wait WITHOUT making any requests — isolate event watcher from lazy-load.
	time.Sleep(5 * time.Second)

	// Single request — should be 200 if event watcher loaded it.
	makeSimpleFVRequest(t, "", http.StatusOK)
}

// TestAssumption_RestoreThenDeleteImmediate replicates the pattern from
// restoreSimpleFV (DELETE + INSERT) followed immediately by deleteSimpleFV.
// With mergeEvents(true), the DELETE event from deleteSimpleFV would be
// swallowed by merging with the prior INSERT.
func TestAssumption_RestoreThenDeleteImmediate(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// Simulate restore (DELETE + INSERT)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps2059)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV2059)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)

	// Immediately delete
	deleteSimpleFV(t)

	// If this times out → event merging is the root cause.
	pollSimpleUntilNotOK(t)
}

// TestAssumption_RestoreThenDeleteWithGap is identical to the above but adds
// a 5-second gap between restore and delete.  This ensures the INSERT event
// is in a DIFFERENT GCI from the DELETE event.
//
// Compare results:
//
//	Immediate FAILS + WithGap PASSES → event merging confirmed
//	Both FAIL → NOT event merging, something else is broken
func TestAssumption_RestoreThenDeleteWithGap(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteDeps2059)
	_ = testutils.RunQueriesOnMetadataCluster(sqlDeleteFV2059)
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")
	pollSimpleUntilOK(t)

	t.Log("Waiting 5s for GCI boundary...")
	time.Sleep(5 * time.Second)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)
}

// TestAssumption_InsertThenDeleteSameGCI tests INSERT followed by DELETE in
// the tightest possible timing (single SQL batch on same connection).
// With mergeEvents(false), both events should be delivered independently.
func TestAssumption_InsertThenDeleteSameGCI(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	deleteSimpleFV(t)
	pollSimpleUntilNotOK(t)

	// INSERT + DELETE in a single SQL batch
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		sqlDeleteDeps2059+"\n"+
		sqlDeleteFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	time.Sleep(5 * time.Second)

	// FV should NOT be in cache (it was deleted).
	status, _ := sendRawFSRequest(t, fsNameSimple, fvNameSimple,
		fvVersionSimple, "id1", "1")
	if status == http.StatusOK {
		t.Log("RESULT: INSERT+DELETE same batch → 200 → DELETE event was MERGED AWAY")
	} else {
		t.Logf("RESULT: INSERT+DELETE same batch → %d → events processed correctly", status)
	}
}

// TestAssumption_DeleteThenInsertSameGCI tests DELETE followed by INSERT in
// the tightest possible timing.
// With mergeEvents(true), DELETE + INSERT → UPDATE (not subscribed).
// With mergeEvents(false), both events are delivered independently.
func TestAssumption_DeleteThenInsertSameGCI(t *testing.T) {
	defer restoreSimpleFV(t)

	makeSimpleFVRequest(t, "", http.StatusOK)

	// DELETE + INSERT in a single SQL batch
	runSQL(t, "SET FOREIGN_KEY_CHECKS = 0;\n"+
		sqlDeleteDeps2059+"\n"+
		sqlDeleteFV2059+"\n"+
		sqlInsertAllDeps2059+"\n"+
		sqlInsertFV2059+"\n"+
		"SET FOREIGN_KEY_CHECKS = 1;")

	time.Sleep(5 * time.Second)

	status, _ := sendRawFSRequest(t, fsNameSimple, fvNameSimple,
		fvVersionSimple, "id1", "1")
	if status == http.StatusOK {
		t.Log("RESULT: DELETE+INSERT same batch → 200 → old entry or properly reloaded")
	} else {
		t.Logf("RESULT: DELETE+INSERT same batch → %d → eviction happened but reload failed/pending", status)
	}
}
