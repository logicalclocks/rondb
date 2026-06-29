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

package pkread

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// Test_PkReadWrongTablePermanent is the PK-read mirror of the index-scan
// permanent test. Same tables (db025 perm_a/perm_b), same sequence: warm a
// pinned connection, drop+recreate in reverse order (swap base-table ids),
// settle, then re-read on the same connection and check each table returns its
// own marker.
//
// Hypothesis: PK read RECOVERS (no wrong data). Unlike the index scan, a PK read
// uses the base table directly and sends the STALE table's own (old) schema
// version, so the kernel rejects it with error 241 and rdrs refetches a fresh
// table. If this test instead shows perm_a returning perm_b's data (or vice
// versa), PK read is ALSO affected and the fix must cover it too.
func Test_PkReadWrongTablePermanent(t *testing.T) {
	db := testdbs.DB025

	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
		t.Fatalf("failed to reset database. Error: %v", err)
	}
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025PermScheme); err != nil {
		t.Fatalf("failed to create perm tables. Error: %v", err)
	}
	defer func() {
		if err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme); err != nil {
			t.Fatalf("failed to re-set database. Error: %v", err)
		}
	}()

	// Pinned connection: one client -> one rdrs server thread (one dict cache).
	client := testutils.SetupHttpClient(t)

	// 1. Warm the server thread's cache with both tables.
	for i := 0; i < 100; i++ {
		pkReadPerm(t, client, db, "perm_a")
		pkReadPerm(t, client, db, "perm_b")
	}
	t.Log("cache warmed; recreating both tables in reverse order (no reads during the change)...")

	// 2. Drop + recreate in reverse order (swap base-table ids). No reads during.
	if err := testutils.RunQueriesOnDataCluster(testdbs.DB025PermUpdateScheme); err != nil {
		t.Fatalf("failed to recreate perm tables. Error: %v", err)
	}

	// 3. Let the schema fully settle.
	time.Sleep(8 * time.Second)

	// 4. Recovery check on the SAME connection.
	wrongA, wrongB, okA := 0, 0, 0
	for i := 0; i < 300; i++ {
		if body, ok := pkReadPerm(t, client, db, "perm_a"); ok {
			if strings.Contains(body, "PERM_MARKER_B") {
				wrongA++
			} else if strings.Contains(body, "PERM_MARKER_A") {
				okA++
			}
		}
		if body, ok := pkReadPerm(t, client, db, "perm_b"); ok {
			if strings.Contains(body, "PERM_MARKER_A") {
				wrongB++
			}
		}
	}
	t.Logf("after settle: perm_a returned perm_b's data %d times, perm_b returned perm_a's data %d times, perm_a correct %d times",
		wrongA, wrongB, okA)

	if wrongA > 0 || wrongB > 0 {
		t.Errorf("PK read PERMANENT wrong-table read: a PK read kept returning the other table's "+
			"data after the schema settled (perm_a->B=%d, perm_b->A=%d)", wrongA, wrongB)
	}
}

// pkReadPerm issues one PK read (id=1, reading col0+col1) over the pinned
// connection and returns the 200 body.
func pkReadPerm(t *testing.T, client *http.Client, db, table string) (string, bool) {
	pkReq := api.PKReadBody{
		Filters:     testclient.NewFiltersKVs("id", 1),
		ReadColumns: testclient.NewReadColumns("col", 2),
		OperationID: testclient.NewOperationID(64),
	}
	body, err := json.Marshal(pkReq)
	if err != nil {
		t.Errorf("failed to marshal %s pk request: %v", table, err)
		return "", false
	}
	code, resp := testclient.SendHttpRequestWithClient(
		t, client, config.PK_HTTP_VERB, testutils.NewPKReadURL(db, table), string(body),
		"",
		http.StatusOK, http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError,
	)
	if code != http.StatusOK {
		return "", false
	}
	return string(resp), true
}
