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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

// RONDB-1135: AVRO(column) decodes a Hopsworks complex feature stored
// Avro-encoded in a VARBINARY column into the JSON value the feature_store
// endpoint returns for the feature.  The schema comes from the feature
// store metadata cached in RDRS: the table names the feature group, and a
// cached feature view serving the feature has its decoder.
//
// fsdb002.sample_complex_type_1 holds an array<bigint> feature `array` and
// a struct<int1:bigint,int2:bigint> feature `struct`, both served by
// feature view sample_complex_type.  fsdb002.avro_strings_1 holds an
// array<string> feature `tags` with non-ASCII strings, a NULL value and a
// null element, served by feature view avro_strings.  fsdb002.sample_1_1
// shares the bigint key id1 with sample_complex_type_1 (ids 9, 23, 56 and
// 73 are in both).  The expected values are the fixture bytes decoded by
// hand.

func ronsqlAvroRequest(t *testing.T, database, query, format string) (int, http.Header, string) {
	t.Helper()
	body := fmt.Sprintf(`{"query": %q, "database": %q, "outputFormat": %q}`,
		query, database, format)
	req, err := http.NewRequest(config.RONSQL_HTTP_VERB, testutils.NewRonSQLURL(),
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	resp, err := testutils.SetupHttpClient(t).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(respBody)
}

// ronsqlAvroRows runs query with output format JSON or JSON_ASCII and
// returns the decoded rows.
func ronsqlAvroRows(t *testing.T, query, format string) []map[string]interface{} {
	t.Helper()
	status, _, body := ronsqlAvroRequest(t, testdbs.FSDB002, query, format)
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", status, body)
	}
	return decodeRows(t, body)
}

func decodeRows(t *testing.T, body string) []map[string]interface{} {
	t.Helper()
	var result struct {
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatalf("response is not JSON: %v; body: %s", err, body)
	}
	return result.Data
}

func jsonRows(t *testing.T, rows string) []map[string]interface{} {
	t.Helper()
	var result []map[string]interface{}
	if err := json.Unmarshal([]byte(rows), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestAvroJSON(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"pk-lookup",
			"SELECT id1, AVRO(`array`) AS a, AVRO(`struct`) AS s FROM sample_complex_type_1 WHERE id1 = 6;",
			`[{"id1":6,"a":[91,65],"s":{"int1":36,"int2":17}}]`},
		// The raw column prints base64 next to its decoded value.
		{"raw-and-decoded",
			"SELECT `array` AS raw, AVRO(`array`) AS a FROM sample_complex_type_1 WHERE id1 = 6;",
			`[{"raw":"AgQCtgECggEA","a":[91,65]}]`},
		// Scan with a client-side sort; the default output name is the
		// expression text.
		{"scan-order-by",
			"SELECT id1, AVRO(`struct`) FROM sample_complex_type_1 WHERE id1 >= 3 AND id1 <= 6 ORDER BY id1;",
			`[{"id1":3,"AVRO(` + "`struct`" + `)":{"int1":51,"int2":53}},
			  {"id1":5,"AVRO(` + "`struct`" + `)":{"int1":26,"int2":39}},
			  {"id1":6,"AVRO(` + "`struct`" + `)":{"int1":36,"int2":17}}]`},
		// Function names are case insensitive.
		{"lower-case",
			"SELECT avro(t.`array`) AS a FROM sample_complex_type_1 AS t WHERE t.id1 = 5;",
			`[{"a":[55,14]}]`},
		// Pushed join: sample_1_1 scan, sample_complex_type_1 key lookup.
		{"inner-join",
			"SELECT s.id1, s.data1, AVRO(c.`array`) AS a, AVRO(c.`struct`) AS st " +
				"FROM sample_1_1 AS s JOIN sample_complex_type_1 AS c ON c.id1 = s.id1 " +
				"ORDER BY s.id1;",
			`[{"id1":9,"data1":2,"a":[5,25],"st":{"int1":15,"int2":41}},
			  {"id1":23,"data1":14,"a":[92,94],"st":{"int1":75,"int2":54}},
			  {"id1":56,"data1":12,"a":[86,22],"st":{"int1":14,"int2":86}},
			  {"id1":73,"data1":17,"a":[97,98],"st":{"int1":61,"int2":72}}]`},
		// Id 12 has no sample_complex_type_1 row: the NULL-extended row
		// decodes to null.
		{"left-join-null",
			"SELECT s.id1, AVRO(c.`array`) AS a " +
				"FROM sample_1_1 AS s LEFT JOIN sample_complex_type_1 AS c ON c.id1 = s.id1 " +
				"WHERE s.id1 >= 9 AND s.id1 <= 12 ORDER BY s.id1;",
			`[{"id1":9,"a":[5,25]},{"id1":12,"a":null}]`},
		{"stored-null",
			"SELECT id, AVRO(tags) AS tags FROM avro_strings_1 WHERE id = 2;",
			`[{"id":2,"tags":null}]`},
		{"null-element",
			"SELECT id, AVRO(tags) AS tags FROM avro_strings_1 WHERE id = 3;",
			`[{"id":3,"tags":["plain",null]}]`},
		{"non-ascii",
			"SELECT id, AVRO(tags) AS tags FROM avro_strings_1 WHERE id = 1;",
			`[{"id":1,"tags":["h\u00e9llo","\u65e5\u672c","\ud83d\ude00"]}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ronsqlAvroRows(t, tc.query, "JSON")
			want := jsonRows(t, tc.want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("rows %v, want %v", got, want)
			}
		})
	}
}

func TestAvroText(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		format string
		want   string
	}{
		{"header",
			"SELECT id1, AVRO(`array`) AS a, AVRO(`struct`) AS s FROM sample_complex_type_1 WHERE id1 = 3;",
			"TEXT",
			"id1\ta\ts\n3\t[72,84]\t{\"int1\":51,\"int2\":53}\n"},
		{"left-join-null",
			"SELECT s.id1, AVRO(c.`array`) AS a " +
				"FROM sample_1_1 AS s LEFT JOIN sample_complex_type_1 AS c ON c.id1 = s.id1 " +
				"WHERE s.id1 >= 9 AND s.id1 <= 12 ORDER BY s.id1;",
			"TEXT_NOHEADER",
			"9\t[5,25]\n12\tNULL\n"},
		{"stored-null-and-null-element",
			"SELECT id, AVRO(tags) AS tags FROM avro_strings_1 WHERE id >= 2 ORDER BY id;",
			"TEXT_NOHEADER",
			"2\tNULL\n3\t[\"plain\",null]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, body := ronsqlAvroRequest(t, testdbs.FSDB002, tc.query, tc.format)
			if status != http.StatusOK {
				t.Fatalf("status %d, want 200; body: %s", status, body)
			}
			if body != tc.want {
				t.Errorf("body %q, want %q", body, tc.want)
			}
		})
	}
}

// JSON_ASCII \u-escapes the non-ASCII characters of decoded strings,
// including a surrogate pair for a character above U+FFFF, and decodes to
// the same rows as JSON.
func TestAvroJSONASCII(t *testing.T) {
	query := "SELECT id, AVRO(tags) AS tags FROM avro_strings_1 ORDER BY id;"
	status, header, body := ronsqlAvroRequest(t, testdbs.FSDB002, query, "JSON_ASCII")
	if status != http.StatusOK {
		t.Fatalf("status %d, want 200; body: %s", status, body)
	}
	if ct := header.Get("Content-Type"); !strings.Contains(ct, "charset=US-ASCII") {
		t.Errorf("Content-Type %q lacks charset=US-ASCII", ct)
	}
	for i := 0; i < len(body); i++ {
		if body[i] >= 0x80 {
			t.Fatalf("byte %d of the body is not ASCII: %q", i, body)
		}
	}
	for _, escaped := range []string{`h\u00e9llo`, `\u65e5\u672c`, `\ud83d\ude00`} {
		if !strings.Contains(body, escaped) {
			t.Errorf("body lacks %s: %s", escaped, body)
		}
	}
	want := jsonRows(t, `[{"id":1,"tags":["héllo","日本","😀"]},
		{"id":2,"tags":null},
		{"id":3,"tags":["plain",null]}]`)
	if got := decodeRows(t, body); !reflect.DeepEqual(got, want) {
		t.Errorf("JSON_ASCII rows %v, want %v", got, want)
	}
	if got := ronsqlAvroRows(t, query, "JSON"); !reflect.DeepEqual(got, want) {
		t.Errorf("JSON rows %v, want %v", got, want)
	}
}

func TestAvroErrors(t *testing.T) {
	cases := []struct {
		name     string
		database string
		query    string
		status   int
		class    string
		body     string
	}{
		{"not-binary", testdbs.FSDB002,
			"SELECT AVRO(ts) FROM sample_complex_type_1 WHERE id1 = 6;",
			http.StatusBadRequest, "semantic", "requires a BINARY or VARBINARY column"},
		{"aggregate", testdbs.FSDB002,
			"SELECT AVRO(`array`) AS a, COUNT(*) AS n FROM sample_complex_type_1 GROUP BY `array`;",
			http.StatusBadRequest, "unsupported", "without aggregation"},
		{"cte-body", testdbs.FSDB002,
			"WITH c AS (SELECT id1, AVRO(`array`) AS a FROM sample_complex_type_1 ORDER BY id1 LIMIT 1) SELECT id1 FROM c;",
			http.StatusBadRequest, "unsupported", "outer query"},
		{"unknown-function", testdbs.FSDB002,
			"SELECT HEX(`array`) FROM sample_complex_type_1 WHERE id1 = 6;",
			http.StatusBadRequest, "syntax", "Unknown function"},
		// Not a feature group served by any feature view.
		{"no-cached-schema", testdbs.DB003,
			"SELECT AVRO(col4) FROM arrays_table WHERE id0 = 1;",
			http.StatusBadRequest, "semantic", "has no Avro schema for this column"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, header, body := ronsqlAvroRequest(t, tc.database, tc.query, "TEXT_NOHEADER")
			if status != tc.status {
				t.Fatalf("status %d, want %d; body: %s", status, tc.status, body)
			}
			if got := header.Get("X-RonSQL-Error-Class"); got != tc.class {
				t.Errorf("X-RonSQL-Error-Class %q, want %q; body: %s", got, tc.class, body)
			}
			if !strings.Contains(body, tc.body) {
				t.Errorf("body lacks %q: %s", tc.body, body)
			}
		})
	}
}
