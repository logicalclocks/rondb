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
	"io"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1124: a RonSQL error carries its class in the HTTP status
// (400 syntax / semantic / unsupported, 413 too large, 503 resource,
// 500 internal), in the X-RonSQL-Error-Class header and as a "[class]"
// prefix of the text body; a successful statement is unaffected.
func TestErrorStatusByClass(t *testing.T) {
	url := testutils.NewRonSQLURL()
	client := testutils.SetupHttpClient(t)
	cases := []struct {
		name   string
		query  string
		status int
		class  string
		body   string
	}{
		{"success", "SELECT COUNT(*) AS n FROM tiny_tbl;", http.StatusOK, "", ""},
		{"syntax", "SELEC COUNT(*) FROM tiny_tbl;", http.StatusBadRequest, "syntax", "Syntax error"},
		{"semantic-table", "SELECT COUNT(*) AS n FROM no_such_table;", http.StatusBadRequest, "semantic", "Failed to get table"},
		{"semantic-column", "SELECT COUNT(no_such_column) AS n FROM tiny_tbl;", http.StatusBadRequest, "semantic", "Could not find column"},
		{"unsupported", "WITH t AS (SELECT pk, val_1 FROM tiny_tbl WHERE val_1 > 3) SELECT pk FROM t;",
			http.StatusBadRequest, "unsupported", "Non-aggregating CTE body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(config.RONSQL_HTTP_VERB, url,
				strings.NewReader(ronsqlBody(testdbs.DB029, tc.query)))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d; body: %s", resp.StatusCode, tc.status, body)
			}
			if got := resp.Header.Get("X-RonSQL-Error-Class"); got != tc.class {
				t.Errorf("X-RonSQL-Error-Class %q, want %q; body: %s", got, tc.class, body)
			}
			if tc.class == "" {
				return
			}
			if !strings.Contains(string(body), "["+tc.class+"] Caught exception:") {
				t.Errorf("body lacks the [%s] prefix: %s", tc.class, body)
			}
			if !strings.Contains(string(body), tc.body) {
				t.Errorf("body lacks %q: %s", tc.body, body)
			}
		})
	}
}
