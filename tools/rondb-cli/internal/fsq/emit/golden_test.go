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

package emit

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

type gateOrException struct {
	Code        string  `json:"code"`
	UserMessage string  `json:"userMessage"`
	Developer   *string `json:"developerMessage"`
}

func loadFixtures(t *testing.T) map[string]GoldenFixture {
	t.Helper()
	out, err := LoadGoldenFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// normalize round-trips a value through JSON so that maps, slices and
// pointers compare structurally (nil slice == JSON null, etc.).
func normalize(t *testing.T, v interface{}) interface{} {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// objectDiffs is diagnostic only; the caller fails independently whenever
// DeepEqual is false. Presence is significant even when a value is JSON null.
func objectDiffs(name string, got, want interface{}) []string {
	gm, gok := got.(map[string]interface{})
	wm, wok := want.(map[string]interface{})
	if gok && wok {
		keys := map[string]bool{}
		for k := range gm {
			keys[k] = true
		}
		for k := range wm {
			keys[k] = true
		}
		var sorted []string
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		var differences []string
		for _, k := range sorted {
			gv, gp := gm[k]
			wv, wp := wm[k]
			if gp != wp {
				differences = append(differences, fmt.Sprintf("%s.%s: field presence got=%t want=%t", name, k, gp, wp))
			} else if !reflect.DeepEqual(gv, wv) {
				differences = append(differences, objectDiffs(name+"."+k, gv, wv)...)
			}
		}
		return differences
	}
	ga, gok := got.([]interface{})
	wa, wok := want.([]interface{})
	if gok && wok && len(ga) == len(wa) {
		var differences []string
		for i := range ga {
			if !reflect.DeepEqual(ga[i], wa[i]) {
				differences = append(differences, objectDiffs(fmt.Sprintf("%s[%d]", name, i), ga[i], wa[i])...)
			}
		}
		return differences
	}
	if reflect.DeepEqual(got, want) {
		return nil
	}
	gj, _ := json.Marshal(got)
	wj, _ := json.Marshal(want)
	return []string{fmt.Sprintf("%s:\n   got: %s\n  want: %s", name, gj, wj)}
}

// TestGoldenConformance runs every Hopsworks golden fixture through the
// port and compares the complete statement set (or the named gate) with
// what the Java builder produced.
func TestGoldenConformance(t *testing.T) {
	fixtures := loadFixtures(t)
	names := make([]string, 0, len(fixtures))
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := fixtures[name]
		t.Run(name, func(t *testing.T) {
			res := ConformFixture(name, f)
			if res.Err != nil {
				t.Fatalf("unexpected error: %v", res.Err)
			}
			for _, difference := range res.Diffs {
				t.Error(difference)
			}
			if f.ExpectedError != nil && res.Gate != *f.ExpectedError {
				t.Errorf("gate %q, fixture expects %s", res.Gate, *f.ExpectedError)
			}
		})
	}
	// the shared helpers keep their unit semantics
	if d := objectDiffs("x", map[string]interface{}{"a": 1.0}, map[string]interface{}{"a": 2.0}); len(d) != 1 {
		t.Errorf("objectDiffs: %v", d)
	}
}

func TestParseStructFieldNames(t *testing.T) {
	got := ParseStructFieldNames("array<struct<event_time:timestamp,amount:decimal(10,2),nested:struct<a:int,b:int>>>")
	want := []string{"event_time", "amount", "nested"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	if n := ParseStructFieldNames("bigint"); len(n) != 0 {
		t.Errorf("non-struct type must yield no fields, got %v", n)
	}
}

func TestRenderRonsqlLiteral(t *testing.T) {
	cases := []struct {
		value, typ, want string
		ok               bool
	}{
		{"12", "bigint", "12", true},
		{"-3.5", "decimal(10,2)", "-3.5", true},
		{"abc", "int", "", false},
		{"O'Brien%", "string", "'O''Brien%'", true},
		{"back\\slash", "string", "", false},
		{"line\nbreak", "string", "", false},
		{"true", "boolean", "", false},
	}
	for _, c := range cases {
		got, ok := renderRonsqlLiteral(c.value, c.typ)
		if ok != c.ok || got != c.want {
			t.Errorf("renderRonsqlLiteral(%q, %q) = %q,%v want %q,%v", c.value, c.typ, got, ok, c.want, c.ok)
		}
	}
}
