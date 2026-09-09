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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// fixture is one Hopsworks golden document: the spec.View input plus the
// exporter's assertions and the captured expected output.
type fixture struct {
	spec.View
	ExpectedTemplates *int                   `json:"expectedTemplates"`
	ExpectedError     *string                `json:"expectedError"`
	SchemaVersion     int                    `json:"schemaVersion"`
	Source            string                 `json:"source"`
	Provenance        map[string]interface{} `json:"provenance"`
	Expected          json.RawMessage        `json:"expected"`
}

type gateOrException struct {
	Code        string  `json:"code"`
	UserMessage string  `json:"userMessage"`
	Developer   *string `json:"developerMessage"`
}

type fixtureManifest struct {
	SchemaVersion int                    `json:"schemaVersion"`
	Provenance    map[string]interface{} `json:"provenance"`
	Files         map[string]string      `json:"files"`
}

var fixtureFilename = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*\.json$`)
var commitHash = regexp.MustCompile(`^[0-9a-f]{40}$`)

func loadFixtures(t *testing.T) map[string]fixture {
	t.Helper()
	out, err := readFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// readFixtures fails closed: the manifest is the corpus inventory, not an
// optional report. Dirty provenance is allowed, but never silently rewritten.
func readFixtures(dir string) (map[string]fixture, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read golden manifest: %w", err)
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("decode golden manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || len(manifest.Files) == 0 {
		return nil, fmt.Errorf("golden manifest must have schemaVersion 1 and a nonempty file inventory")
	}
	ref, _ := manifest.Provenance["hopsworksCommit"].(string)
	if !commitHash.MatchString(ref) || !strings.HasPrefix(ref, HopsworksRef) {
		return nil, fmt.Errorf("golden manifest commit %q does not match emitter reference %s", ref, HopsworksRef)
	}
	for name := range manifest.Files {
		if !fixtureFilename.MatchString(name) || name == "manifest.json" {
			return nil, fmt.Errorf("unsafe golden fixture filename %q", name)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read golden directory: %w", err)
	}
	out := map[string]fixture{}
	for _, entry := range entries {
		name := entry.Name()
		if name == "manifest.json" || !strings.HasSuffix(name, ".json") {
			continue
		}
		hash, listed := manifest.Files[name]
		if !listed || !entry.Type().IsRegular() {
			return nil, fmt.Errorf("unlisted or non-regular golden fixture %s", name)
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read golden fixture %s: %w", name, err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(raw)) != hash {
			return nil, fmt.Errorf("golden fixture %s: SHA-256 mismatch", name)
		}
		var f fixture
		if err := json.Unmarshal(raw, &f); err != nil {
			return nil, fmt.Errorf("decode golden fixture %s: %w", name, err)
		}
		if f.SchemaVersion != 1 || f.Source != "java-generated" {
			return nil, fmt.Errorf("golden fixture %s must have schemaVersion 1 and source java-generated", name)
		}
		if !reflect.DeepEqual(f.Provenance, manifest.Provenance) {
			return nil, fmt.Errorf("golden fixture %s: provenance differs from manifest", name)
		}
		stem := strings.TrimSuffix(name, ".json")
		if f.Name != stem {
			return nil, fmt.Errorf("golden fixture %s: input name %q differs from filename", name, f.Name)
		}
		var expected map[string]interface{}
		if err := json.Unmarshal(f.Expected, &expected); err != nil || len(expected) == 0 {
			return nil, fmt.Errorf("golden fixture %s: missing or invalid expected object", name)
		}
		out[stem] = f
	}
	for name := range manifest.Files {
		if _, ok := out[strings.TrimSuffix(name, ".json")]; !ok {
			return nil, fmt.Errorf("missing golden fixture %s", name)
		}
	}
	return out, nil
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
			gotN, wantN := normalize(t, got), normalize(t, want)
			if !reflect.DeepEqual(gotN, wantN) {
				t.Errorf("%s: complete expected object differs", name)
				for _, difference := range objectDiffs(name, gotN, wantN) {
					t.Log(difference)
				}
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
