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

// Package spillfixture is the one table the RONDB-1144 blob and text spill
// tests share, created at runtime inside db013, a database the seed
// registers as a Hopsworks project so the API-key server authorises it.
package spillfixture

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

const (
	DB    = testdbs.DB013
	Table = "spill_table"
	RowID = 1
	// The varchar after the blob, which proves blobs are written last.
	AfterBlob = "after-the-blob"
)

// SetupSpillTable creates db013.spill_table with one row (id 1, all large
// columns NULL) and drops it when the test ends.
func SetupSpillTable(t testing.TB) {
	t.Helper()
	stmts := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", DB, Table),
		fmt.Sprintf("CREATE TABLE %s.%s (id INT NOT NULL, b LONGBLOB, v VARCHAR(64) NOT NULL DEFAULT '%s', b2 LONGBLOB, t LONGTEXT CHARACTER SET latin1, PRIMARY KEY (id)) ENGINE=ndbcluster;", DB, Table, AfterBlob),
		fmt.Sprintf("INSERT INTO %s.%s (id, b, b2, t) VALUES (%d, NULL, NULL, NULL);", DB, Table, RowID),
	}
	// The runner drops everything after the last ';', hence the terminators.
	for _, stmt := range stmts {
		if err := testutils.RunQueriesOnDataCluster(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		_ = testutils.RunQueriesOnDataCluster(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", DB, Table))
	})
}

// ReadBody is the pk-read body for row 1 reading the given columns.
func ReadBody(t testing.TB, cols []string) string {
	t.Helper()
	readCols := make([]api.ReadColumn, len(cols))
	for i := range cols {
		c := cols[i]
		readCols[i] = api.ReadColumn{Column: &c}
	}
	body := api.PKReadBody{
		Filters:     testclient.NewFiltersKVs("id", RowID),
		ReadColumns: &readCols,
		OperationID: testclient.NewOperationID(3),
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(data)
}

// Read row 1 through pk-read and return the data map: a JSON string per
// column (base64 for binary), "null" for NULL.
func Read(t testing.TB, cols []string) map[string]*json.RawMessage {
	t.Helper()
	url := testutils.NewPKReadURL(DB, Table)
	_, resp := testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, ReadBody(t, cols), "", http.StatusOK)
	var pk api.PKReadResponseJSON
	if err := json.Unmarshal(resp, &pk); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if pk.Data == nil {
		t.Fatalf("response has no data: %s", string(resp))
	}
	return *pk.Data
}

// Value of column col in a data map as a Go string (JSON-decoded), or ok
// false when the column is null or missing.
func Value(t testing.TB, data map[string]*json.RawMessage, col string) (string, bool) {
	t.Helper()
	raw, found := data[col]
	if !found || raw == nil {
		return "", false
	}
	if string(*raw) == "null" {
		return "", false
	}
	var s string
	if err := json.Unmarshal(*raw, &s); err != nil {
		t.Fatalf("column %s is not a JSON string: %v", col, err)
	}
	return s, true
}

// ExpectBlob checks that column col of the data map decodes to data: same
// sha256 locally and in MySQL.
func ExpectBlob(t testing.TB, data map[string]*json.RawMessage, col string, want []byte) {
	t.Helper()
	s, ok := Value(t, data, col)
	if !ok {
		t.Fatalf("column %s is null", col)
	}
	got := testutils.DecodeBase64Value(t, s)
	if len(got) != len(want) {
		t.Fatalf("column %s: got %d bytes, want %d", col, len(got), len(want))
	}
	if testutils.SHA256Hex(got) != testutils.SHA256Hex(want) {
		t.Fatalf("column %s: content differs", col)
	}
	if db := testutils.DBSHA256(t, DB, Table, col, RowID); db != testutils.SHA256Hex(want) {
		t.Fatalf("column %s: MySQL holds a different value (sha %s)", col, db)
	}
}
