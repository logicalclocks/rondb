/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package data

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
)

func TestGoldenRowsCorpus(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	layouts := map[string]bool{}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			out, err := fixture.CapturedOutput()
			if err != nil {
				t.Fatal(err)
			}
			if out.Kind != emit.GoldenStatements {
				return
			}
			for _, fg := range fixture.FGs {
				before, err := json.Marshal(fg)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := GoldenRows(fg)
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]int{"events": 11, "profiles": 9, "regions": 5, "countries": 3}[fg.Name]
				if _, composite := fg.Feature("currency"); composite {
					want = 22
				}
				if len(rows) != want || want == 0 {
					t.Fatalf("%s: got %d rows, want %d", fg.Name, len(rows), want)
				}
				columns, _ := json.Marshal(fg.Features)
				layouts[fg.Name+"|"+fg.EventTime+"|"+string(columns)] = true
				seen := map[string]bool{}
				for _, row := range rows {
					if len(row) != len(fg.Features) {
						t.Fatalf("%s: row width differs from captured schema", fg.Name)
					}
					var pk []string
					for i, feature := range fg.Features {
						if feature.Primary {
							if row[i] == "NULL" {
								t.Fatalf("%s: NULL primary key", fg.Name)
							}
							pk = append(pk, row[i])
						}
					}
					key := strings.Join(pk, "\x00")
					if len(pk) == 0 || seen[key] {
						t.Fatalf("%s: missing or duplicate primary key %v", fg.Name, pk)
					}
					seen[key] = true
				}
				again, err := GoldenRows(fg)
				if err != nil || !reflect.DeepEqual(rows, again) {
					t.Fatalf("%s: nondeterministic rows: %v", fg.Name, err)
				}
				rows[0][0] = "changed"
				if again[0][0] == "changed" {
					t.Fatal("calls share mutable row storage")
				}
				after, err := json.Marshal(fg)
				if err != nil || string(before) != string(after) {
					t.Fatal("data generation changed captured schema")
				}
			}
		})
	}
	if len(layouts) != 7 {
		t.Fatalf("covered %d layouts, want all 7 captured layouts", len(layouts))
	}
}

func TestGoldenEventCoverage(t *testing.T) {
	rows := goldenEventRows(false, false, false)
	for i, want := range map[int]string{
		1: "'2026-05-31 22:59:59'",
		2: "'2026-05-31 23:00:00'",
		3: "'2026-05-31 23:00:01'",
	} {
		if rows[i][1] != want {
			t.Fatalf("window boundary row %d: %s, want %s", i, rows[i][1], want)
		}
	}
	counts := map[string]int{}
	for _, row := range rows {
		counts[row[0]]++
	}
	if counts["1"] != 8 || counts["2"] != 1 || counts["3"] != 2 || counts["4"] != 0 {
		t.Fatalf("collect histories or missing-entity probe changed: %v", counts)
	}
	if rows[6][2] != "NULL" || rows[5][3] != "NULL" ||
		rows[9][2] != "NULL" || rows[10][2] != "NULL" ||
		rows[3][4] != "'O''Brien-first'" {
		t.Fatal("NULL or quoted-filter probes changed")
	}
	composite := goldenEventRows(false, true, false)
	if composite[0][1] != "'EUR'" || composite[0][3] != "-5" ||
		composite[11][1] != "'USD'" || composite[11][3] != "95" {
		t.Fatal("composite-key partners must have different values")
	}
	stringsData := goldenEventRows(true, false, false)
	if stringsData[0][0] != "'entity-1'" || stringsData[8][0] != "'O''Brien'" ||
		stringsData[9][0] != "'café'" || GoldenEntityKey(4, true) != "entity-4" {
		t.Fatal("string entity-key mapping changed")
	}
	sequence := goldenEventRows(false, false, true)
	if sequence[0][1] != "1" || sequence[7][1] != "8" {
		t.Fatal("explicit collect order must use sequence numbers")
	}
}

func TestGoldenRowsUnknownLayout(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{"type", "primary", "event-time", "offline", "online-type", "default", "extra-column"} {
		t.Run(change, func(t *testing.T) {
			fg := fixtures["aggregate_single"].FGs[0]
			fg.Features = append(fg.Features[:0:0], fg.Features...)
			switch change {
			case "type":
				fg.Features[0].Type = "double"
			case "primary":
				fg.Features[0].Primary = false
			case "event-time":
				fg.EventTime = ""
			case "offline":
				fg.Features[0].OfflineOnly = true
			case "online-type":
				fg.Features[0].OnlineType = "int"
			case "default":
				value := "1"
				fg.Features[0].DefaultValue = &value
			case "extra-column":
				fg.Features = append(fg.Features, fg.Features[0])
			}
			rows, err := GoldenRows(fg)
			if err == nil || rows != nil || !strings.Contains(err.Error(), fg.TableName()) {
				t.Fatalf("unsupported schema returned rows or lacked context: %v", err)
			}
		})
	}
}

func TestGoldenSnowflakeCoverage(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string][][]string{}
	for _, fg := range fixtures["snowflake_left"].FGs {
		rows, err := GoldenRows(fg)
		if err != nil {
			t.Fatal(err)
		}
		tables[fg.Name] = rows
	}
	regions, countries := map[string][]string{}, map[string][]string{}
	for _, row := range tables["regions"] {
		regions[row[0]+"|"+row[1]] = row
	}
	for _, row := range tables["countries"] {
		countries[row[0]] = row
	}
	want := map[string][2]bool{
		"1": {true, true}, "2": {true, true}, "3": {false, false},
		"4": {false, false}, "5": {true, false}, "6": {true, false},
		"7": {false, false}, "8": {false, false}, "9": {true, true},
	}
	for _, row := range tables["profiles"] {
		r := regions[row[1]+"|"+row[2]]
		var c []string
		if r != nil {
			c = countries[r[2]]
		}
		expected, ok := want[row[0]]
		if !ok || [2]bool{r != nil, c != nil} != expected {
			t.Fatalf("entity %s has unexpected join hits", row[0])
		}
		if row[0] == "9" && (row[3] != "NULL" || r[3] != "NULL" || c[1] != "NULL") {
			t.Fatal("matched NULL projections changed")
		}
		delete(want, row[0])
	}
	if len(want) != 0 {
		t.Fatalf("missing snowflake probes: %v", want)
	}
	if regions["10|'A'"][3] == regions["10|'B'"][3] {
		t.Fatal("composite region keys must have distinct projections")
	}
}
