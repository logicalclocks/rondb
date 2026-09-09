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
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// fixture is one Hopsworks golden document: the spec.View input plus the
// exporter's assertions and the captured expected output.
type fixture struct {
	spec.View
	ExpectedTemplates *int            `json:"expectedTemplates"`
	ExpectedError     *string         `json:"expectedError"`
	SchemaVersion     int             `json:"schemaVersion"`
	Source            string          `json:"source"`
	Expected          json.RawMessage `json:"expected"`
}

type gateOrException struct {
	Code        string  `json:"code"`
	UserMessage string  `json:"userMessage"`
	Developer   *string `json:"developerMessage"`
}

func loadFixtures(t *testing.T) map[string]fixture {
	t.Helper()
	dir := filepath.Join("..", "testdata", "hopsworks_golden")
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(paths) == 0 {
		t.Skipf("no golden fixtures under %s", dir)
	}
	out := map[string]fixture{}
	for _, p := range paths {
		if filepath.Base(p) == "manifest.json" {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var f fixture
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if f.SchemaVersion != 1 {
			t.Fatalf("%s: unsupported fixture schemaVersion %d", p, f.SchemaVersion)
		}
		out[strings.TrimSuffix(filepath.Base(p), ".json")] = f
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

// diffObjects reports, per statement and field, where got differs from want.
func diffObjects(t *testing.T, name string, got, want interface{}) {
	t.Helper()
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
		for _, k := range sorted {
			if !reflect.DeepEqual(gm[k], wm[k]) {
				diffObjects(t, name+"."+k, gm[k], wm[k])
			}
		}
		return
	}
	ga, gok := got.([]interface{})
	wa, wok := want.([]interface{})
	if gok && wok && len(ga) == len(wa) {
		for i := range ga {
			if !reflect.DeepEqual(ga[i], wa[i]) {
				diffObjects(t, name+"["+string(rune('0'+i))+"]", ga[i], wa[i])
			}
		}
		return
	}
	gj, _ := json.Marshal(got)
	wj, _ := json.Marshal(want)
	t.Errorf("%s:\n   got: %s\n  want: %s", name, gj, wj)
}

// TestGoldenConformance runs every Hopsworks golden fixture through the
// port and compares the complete statement set (or the named gate) with
// what the Java builder produced.
func TestGoldenConformance(t *testing.T) {
	for name, f := range loadFixtures(t) {
		f := f
		t.Run(name, func(t *testing.T) {
			var want map[string]interface{}
			if err := json.Unmarshal(f.Expected, &want); err != nil {
				t.Fatal(err)
			}
			got := map[string]interface{}{}
			var gateErr *spec.GateError
			if f.Definition != nil {
				res, err := spec.ValidateDefinition(&f.View)
				if err != nil {
					if ge, ok := err.(*spec.GateError); ok {
						gateErr = ge
					} else {
						t.Fatalf("unexpected error: %v", err)
					}
				} else {
					for k, v := range normalize(t, res).(map[string]interface{}) {
						got[k] = v
					}
				}
			} else {
				statements, err := Build(&f.View)
				if err != nil {
					if ge, ok := err.(*spec.GateError); ok {
						gateErr = ge
					} else {
						t.Fatalf("unexpected error: %v", err)
					}
				} else {
					got["statements"] = normalize(t, statements)
					if f.ExpectedTemplates != nil {
						n := 0
						for _, s := range statements {
							n += s.TemplateCount()
						}
						if n != *f.ExpectedTemplates {
							t.Errorf("template count %d, fixture expects %d", n, *f.ExpectedTemplates)
						}
					}
				}
			}
			if gateErr != nil {
				got["gateOrException"] = normalize(t, gateOrException{Code: gateErr.Code, UserMessage: gateErr.Message})
				if f.ExpectedError == nil || *f.ExpectedError != gateErr.Code {
					t.Errorf("gate %s (%s); fixture expects error %v", gateErr.Code, gateErr.Message, f.ExpectedError)
				}
			} else {
				got["gateOrException"] = nil
				if f.ExpectedError != nil {
					t.Errorf("no gate raised; fixture expects %s", *f.ExpectedError)
				}
			}
			gotN := normalize(t, got)
			if !reflect.DeepEqual(gotN, normalize(t, want)) {
				diffObjects(t, name, gotN, normalize(t, want))
			}
		})
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
