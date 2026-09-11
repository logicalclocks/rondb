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

// Java conformance as a callable check (RONDB-1121 E8): the emitter (or
// the definition validator) is run on a captured Hopsworks fixture and its
// complete output object is compared with the captured expected object.
// TestGoldenConformance and `.fs_verify --requirements` share this code so
// requirements evidence is exactly what the unit test asserts.

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// GateOrException mirrors the fixture's captured exception object.
type GateOrException struct {
	Code        string  `json:"code"`
	UserMessage string  `json:"userMessage"`
	Developer   *string `json:"developerMessage"`
}

// ConformResult is the outcome of ConformFixture.
type ConformResult struct {
	Name string
	// Gate is the code the emitter / validator raised ("" = none).
	Gate string
	// Templates is the number of RonSQL templates emitted (serving cases).
	Templates int
	// Diffs lists every difference from the captured expected object,
	// including a gate mismatch or a template-count mismatch; empty = conformant.
	Diffs []string
	// Err is a non-gate failure of the emitter (an internal error).
	Err error
}

// Conformant reports whether the fixture is reproduced exactly.
func (r ConformResult) Conformant() bool { return r.Err == nil && len(r.Diffs) == 0 }

// ConformFixture runs the emitter or the definition validator on the
// fixture input and compares the complete output with the captured
// expected object.
func ConformFixture(name string, f GoldenFixture) ConformResult {
	res := ConformResult{Name: name}
	var want map[string]interface{}
	if err := json.Unmarshal(f.Expected, &want); err != nil {
		res.Err = fmt.Errorf("expected object: %w", err)
		return res
	}
	got := map[string]interface{}{}
	var gateErr *spec.GateError
	view := f.View
	if f.Definition != nil {
		out, err := spec.ValidateDefinition(&view)
		if err != nil {
			ge, ok := err.(*spec.GateError)
			if !ok {
				res.Err = err
				return res
			}
			gateErr = ge
		} else {
			m, err := normalizeValue(out)
			if err != nil {
				res.Err = err
				return res
			}
			for k, v := range m.(map[string]interface{}) {
				got[k] = v
			}
		}
	} else {
		statements, err := Build(&view)
		if err != nil {
			ge, ok := err.(*spec.GateError)
			if !ok {
				res.Err = err
				return res
			}
			gateErr = ge
		} else {
			n, err := normalizeValue(statements)
			if err != nil {
				res.Err = err
				return res
			}
			got["statements"] = n
			for _, s := range statements {
				res.Templates += s.TemplateCount()
			}
			if f.ExpectedTemplates != nil && res.Templates != *f.ExpectedTemplates {
				res.Diffs = append(res.Diffs, fmt.Sprintf("%s: template count %d, fixture expects %d", name, res.Templates, *f.ExpectedTemplates))
			}
		}
	}
	if gateErr != nil {
		res.Gate = gateErr.Code
		n, _ := normalizeValue(GateOrException{Code: gateErr.Code, UserMessage: gateErr.Message})
		got["gateOrException"] = n
		if f.ExpectedError == nil || *f.ExpectedError != gateErr.Code {
			res.Diffs = append(res.Diffs, fmt.Sprintf("%s: gate %s (%s); fixture expects error %v", name, gateErr.Code, gateErr.Message, f.ExpectedError))
		}
	} else {
		got["gateOrException"] = nil
		if f.ExpectedError != nil {
			res.Diffs = append(res.Diffs, fmt.Sprintf("%s: no gate raised; fixture expects %s", name, *f.ExpectedError))
		}
	}
	gotN, err := normalizeValue(got)
	if err != nil {
		res.Err = err
		return res
	}
	wantN, err := normalizeValue(want)
	if err != nil {
		res.Err = err
		return res
	}
	if !reflect.DeepEqual(gotN, wantN) {
		res.Diffs = append(res.Diffs, ObjectDiffs(name, gotN, wantN)...)
		if len(res.Diffs) == 0 {
			res.Diffs = append(res.Diffs, name+": complete expected object differs")
		}
	}
	return res
}

// normalizeValue round-trips a value through JSON so that structs, maps
// and captured JSON compare with the same shapes.
func normalizeValue(v interface{}) (interface{}, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ObjectDiffs lists the differing fields between two normalized JSON
// values, field by field, with presence tracked separately from value.
func ObjectDiffs(name string, got, want interface{}) []string {
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
				differences = append(differences, ObjectDiffs(name+"."+k, gv, wv)...)
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
				differences = append(differences, ObjectDiffs(fmt.Sprintf("%s[%d]", name, i), ga[i], wa[i])...)
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
