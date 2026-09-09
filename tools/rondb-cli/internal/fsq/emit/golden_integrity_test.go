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
	"strings"
	"testing"
)

func TestObjectDiffs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		got, want interface{}
		different bool
	}{
		{"same", map[string]interface{}{"field": nil}, map[string]interface{}{"field": nil}, false},
		{"missing-null", map[string]interface{}{}, map[string]interface{}{"field": nil}, true},
		{"extra-null", map[string]interface{}{"field": nil}, map[string]interface{}{}, true},
		{"nested-null", []interface{}{map[string]interface{}{}}, []interface{}{map[string]interface{}{"field": nil}}, true},
		{"empty-vs-null", []interface{}{}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			diff := objectDiffs("fixture", tc.got, tc.want)
			if (len(diff) > 0) != tc.different {
				t.Fatalf("differences %v; expected different=%t", diff, tc.different)
			}
		})
	}
	got, want := make([]interface{}, 11), make([]interface{}, 11)
	want[10] = "changed"
	diff := objectDiffs("fixture", got, want)
	if len(diff) != 1 || !strings.Contains(diff[0], "fixture[10]") {
		t.Fatalf("incorrect array diagnostic: %v", diff)
	}
}

// Synthetic documents below test corpus integrity, not SQL conformance.
// They never enter testdata/hopsworks_golden or masquerade as captured SQL.
func TestReadFixturesIntegrity(t *testing.T) {
	update := func(field string, value interface{}) func(*fixtureManifest, map[string][]byte) {
		return func(m *fixtureManifest, files map[string][]byte) {
			var doc map[string]interface{}
			if err := json.Unmarshal(files["sample.json"], &doc); err != nil {
				t.Fatal(err)
			}
			doc[field] = value
			raw, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			files["sample.json"] = raw
			m.Files["sample.json"] = fmt.Sprintf("%x", sha256.Sum256(raw))
		}
	}
	for _, tc := range []struct {
		name, errorText string
		mutate          func(*fixtureManifest, map[string][]byte)
		missingManifest bool
	}{
		{name: "valid-dirty-provenance"},
		{name: "missing-manifest", missingManifest: true, errorText: "read golden manifest"},
		{name: "empty-inventory", errorText: "nonempty file inventory",
			mutate: func(m *fixtureManifest, _ map[string][]byte) { m.Files = nil }},
		{name: "missing-case", errorText: "missing golden fixture",
			mutate: func(_ *fixtureManifest, files map[string][]byte) { delete(files, "sample.json") }},
		{name: "unlisted-case", errorText: "unlisted",
			mutate: func(_ *fixtureManifest, files map[string][]byte) { files["extra.json"] = files["sample.json"] }},
		{name: "changed-bytes", errorText: "SHA-256 mismatch",
			mutate: func(_ *fixtureManifest, files map[string][]byte) {
				files["sample.json"] = append(files["sample.json"], '\n')
			}},
		{name: "transcribed", errorText: "source java-generated", mutate: update("source", "transcribed")},
		{name: "fixture-schema", errorText: "schemaVersion 1", mutate: update("schemaVersion", 2)},
		{name: "wrong-name", errorText: "differs from filename", mutate: update("name", "other")},
		{name: "missing-expected", errorText: "expected object", mutate: update("expected", nil)},
		{name: "provenance-drift", errorText: "provenance differs", mutate: update("provenance", map[string]interface{}{})},
		{name: "wrong-reference", errorText: "does not match emitter reference",
			mutate: func(m *fixtureManifest, _ map[string][]byte) {
				m.Provenance["hopsworksCommit"] = strings.Repeat("0", 40)
			}},
		{name: "unsafe-path", errorText: "unsafe golden fixture filename",
			mutate: func(m *fixtureManifest, _ map[string][]byte) { m.Files["../escape.json"] = strings.Repeat("0", 64) }},
		{name: "manifest-schema", errorText: "schemaVersion 1",
			mutate: func(m *fixtureManifest, _ map[string][]byte) { m.SchemaVersion = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provenance := map[string]interface{}{
				"hopsworksCommit":      HopsworksRef + strings.Repeat("0", 40-len(HopsworksRef)),
				"trackedWorktreeDirty": true,
			}
			doc := map[string]interface{}{
				"name": "sample", "schemaVersion": 1, "source": "java-generated",
				"provenance": provenance,
				"expected":   map[string]interface{}{"statements": []interface{}{}, "gateOrException": nil},
			}
			raw, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			files := map[string][]byte{"sample.json": raw}
			m := fixtureManifest{SchemaVersion: 1, Provenance: provenance,
				Files: map[string]string{"sample.json": fmt.Sprintf("%x", sha256.Sum256(raw))}}
			if tc.mutate != nil {
				tc.mutate(&m, files)
			}
			dir := t.TempDir()
			for name, data := range files {
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.missingManifest {
				data, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := readFixtures(dir)
			if tc.errorText != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errorText) {
					t.Fatalf("got error %v; want %q", err, tc.errorText)
				}
			} else if err != nil || len(got) != 1 {
				t.Fatalf("got %d fixtures, error %v", len(got), err)
			}
		})
	}
	t.Run("missing-directory", func(t *testing.T) {
		if _, err := readFixtures(filepath.Join(t.TempDir(), "absent")); err == nil {
			t.Fatal("missing corpus must fail, not skip")
		}
	})
}
