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

package cases

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
)

// TestRequirementsResolve is the manifest's self-check: every explicit
// case id, shape, spec shape and fixture it names must exist, every
// enumerated Hopsworks-origin case must be claimed by some requirement,
// and the status/acceptance rules must hold.
func TestRequirementsResolve(t *testing.T) {
	cfg := Config{DB: "test", Scale: data.NewScale(0.01)}
	all, err := Enumerate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	specs, err := Specs(cfg)
	if err != nil {
		t.Fatal(err)
	}
	resolved := ResolveRequirements(all, specs)
	claimed := map[string]bool{}
	ids := map[string]bool{}
	for _, r := range resolved {
		if ids[r.ID] {
			t.Errorf("duplicate requirement id %s", r.ID)
		}
		ids[r.ID] = true
		if len(r.Missing) > 0 {
			t.Errorf("%s: unresolved evidence %v", r.ID, r.Missing)
		}
		if len(r.CaseIDs) == 0 && len(r.Fixtures) == 0 {
			t.Errorf("%s: no evidence at all", r.ID)
		}
		for _, id := range r.CaseIDs {
			claimed[id] = true
		}
		if len(r.SpecShapes) > 0 && len(r.SpecIDs) == 0 {
			t.Errorf("%s: spec shapes %v resolve to no spec", r.ID, r.SpecShapes)
		}
	}
	for _, c := range all {
		if c.Origin == "hopsworks" && !claimed[c.ID] {
			t.Errorf("Hopsworks-origin case %s (%s) is not claimed by any requirement", c.ID, c.Shape)
		}
	}
	// fixtures named by the manifest exist in the corpus
	dir := filepath.Join("..", "testdata", "hopsworks_golden")
	for _, r := range Requirements() {
		for _, f := range r.Fixtures {
			if _, err := os.Stat(filepath.Join(dir, f+".json")); err != nil {
				t.Errorf("%s: fixture %s: %v", r.ID, f, err)
			}
		}
	}
	// status folding and acceptance
	emitted := Requirement{ID: "x", Emitted: true}
	gate := Requirement{ID: "g", Emitted: true, Gate: "G"}
	native := Requirement{ID: "n", Emitted: false}
	for _, tc := range []struct {
		r       Requirement
		classes []string
		missing int
		want    string
		accept  bool
	}{
		{emitted, []string{"pass", "pass"}, 0, ReqSupported, true},
		{emitted, []string{"pass", "unsupported"}, 0, ReqUnsupported, false},
		{emitted, []string{"pass", "untested"}, 0, ReqUntested, false},
		{emitted, []string{"unsupported", "failed"}, 0, ReqFailed, false},
		{emitted, []string{"pass"}, 1, ReqUntested, false},
		{gate, []string{"pass"}, 0, ReqGated, true},
		{gate, []string{"failed"}, 0, ReqFailed, false},
		{native, []string{"unsupported"}, 0, ReqUnsupported, true},
	} {
		got := RequirementStatus(tc.r, tc.classes, tc.missing)
		if got != tc.want || Accepted(tc.r, got) != tc.accept {
			t.Errorf("%s %v missing=%d: status %s accept %v (want %s %v)", tc.r.ID, tc.classes, tc.missing, got, Accepted(tc.r, got), tc.want, tc.accept)
		}
	}
	for st, want := range map[string]string{"PASS": "pass", "PASS(was-expected-reject)": "pass", "REJECT(expected)": "unsupported", "KNOWN-WRONG": "unsupported",
		"FAIL": "failed", "REJECT": "failed", "CRASH": "failed", "UNTESTED": "untested", "SKIP": "untested"} {
		if got := EvidenceClass(st); got != want {
			t.Errorf("EvidenceClass(%s)=%s want %s", st, got, want)
		}
	}
	if ManifestSummary() == "" {
		t.Error("empty manifest summary")
	}
}
