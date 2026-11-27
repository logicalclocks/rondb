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
	"net/http"
	"reflect"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1124 (README finding T-12): the RonSQL Go tests used ASCII output
// names only. JSON writes a non-ASCII output name (an alias) as UTF-8 and
// JSON_ASCII \u-escapes it, as it does a non-ASCII value, and both bodies
// decode to the same rows. One case per printer that writes output names:
// the pass-through printer (a key lookup, and a range scan with ORDER BY),
// the aggregate printer (GROUP BY over the key reads of an IN list) and the
// COUNT(*) shortcut, plus an empty result, which writes no name. Each body
// is compared whole, so the framing is checked as well: RDRS puts the result
// between {"data":\n and }\n, the pass-through rows share one line and each
// aggregate row ends with a newline. db029.tiny_tbl holds pk 0..4 with
// val_1 = pk, val_2 = 2 * pk and content "<pk>_<2 * pk>_<3 * pk>".
func TestJSONNonASCIIAliases(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		json      string
		jsonASCII string
	}{
		{"passthrough-lookup",
			"SELECT pk, val_1 AS värde FROM tiny_tbl WHERE pk = 2;",
			`[{"pk":2,"värde":2}]` + "\n",
			`[{"pk":2,"v\u00e4rde":2}]` + "\n"},
		{"passthrough-scan-order-by",
			"SELECT pk AS nyckel_ö, content AS innehåll FROM tiny_tbl WHERE pk >= 3 ORDER BY pk;",
			`[{"nyckel_ö":3,"innehåll":"3_6_9"},{"nyckel_ö":4,"innehåll":"4_8_12"}]` + "\n",
			`[{"nyckel_\u00f6":3,"inneh\u00e5ll":"3_6_9"},{"nyckel_\u00f6":4,"inneh\u00e5ll":"4_8_12"}]` + "\n"},
		{"aggregate",
			"SELECT val_1 AS 数, SUM(val_2) AS summa_€ FROM tiny_tbl WHERE pk IN (1, 3) GROUP BY val_1;",
			`[{"数":1,"summa_€":2}` + "\n" + `,{"数":3,"summa_€":6}` + "\n" + `]` + "\n",
			`[{"\u6570":1,"summa_\u20ac":2}` + "\n" + `,{"\u6570":3,"summa_\u20ac":6}` + "\n" + `]` + "\n"},
		{"count-star",
			"SELECT COUNT(*) AS antal_é FROM tiny_tbl;",
			`[{"antal_é":5}` + "\n" + `]` + "\n",
			`[{"antal_\u00e9":5}` + "\n" + `]` + "\n"},
		{"empty",
			"SELECT pk AS nyckel_ö FROM tiny_tbl WHERE pk = 99;",
			"[]\n",
			"[]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			formats := []struct {
				format string
				result string
			}{
				{"JSON", tc.json},
				{"JSON_ASCII", tc.jsonASCII},
			}
			var rows [][]map[string]interface{}
			for _, f := range formats {
				status, header, body := ronsqlAvroRequest(t, testdbs.DB029, tc.query, f.format)
				if status != http.StatusOK {
					t.Fatalf("%s: status %d, want 200; body: %s", f.format, status, body)
				}
				want := "{\"data\":\n" + f.result + "}\n"
				if body != want {
					t.Errorf("%s: body %q, want %q", f.format, body, want)
				}
				if f.format == "JSON_ASCII" {
					if ct := header.Get("Content-Type"); !strings.Contains(ct, "charset=US-ASCII") {
						t.Errorf("Content-Type %q lacks charset=US-ASCII", ct)
					}
					for i := 0; i < len(body); i++ {
						if body[i] >= 0x80 {
							t.Errorf("byte %d of the JSON_ASCII body is not ASCII: %q", i, body)
							break
						}
					}
				}
				rows = append(rows, decodeRows(t, body))
			}
			if !reflect.DeepEqual(rows[0], rows[1]) {
				t.Errorf("JSON rows %v and JSON_ASCII rows %v differ", rows[0], rows[1])
			}
		})
	}
}
