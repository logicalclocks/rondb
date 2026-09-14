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

package shell

import (
	"context"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
)

func TestVerifyRejectionStatuses(t *testing.T) {
	for _, tc := range []struct {
		status string
		fail   bool
		shape  string
	}{
		{"PASS", false, "SUPPORTED"},
		{"REJECT(expected)", false, "UNSUPPORTED"},
		{"REJECT(allowed)", false, "UNSUPPORTED"},
		{"REJECT", true, "FAILED"},
		{"WRONG-RESULT", true, "FAILED"},
		{"ERROR", true, "FAILED"},
		{"TIMEOUT", true, "FAILED"},
		{"CRASH", true, "FAILED"},
	} {
		t.Run(tc.status, func(t *testing.T) {
			if got := isFailure(tc.status); got != tc.fail {
				t.Fatalf("isFailure=%v, want %v", got, tc.fail)
			}
			for _, statuses := range [][]string{{"PASS", tc.status}, {tc.status, "PASS"}} {
				if got := shapeStatus(statuses); got != tc.shape {
					t.Fatalf("shapeStatus(%v)=%s, want %s", statuses, got, tc.shape)
				}
			}
		})
	}
	for _, statuses := range [][]string{
		{"REJECT(allowed)", "WRONG-RESULT"},
		{"WRONG-RESULT", "REJECT(allowed)"},
	} {
		if got := shapeStatus(statuses); got != "FAILED" {
			t.Fatalf("allowed rejection must not mask a failure: %v => %s", statuses, got)
		}
	}
}

func TestVerifyAllowRejectFlag(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		var args []string
		if enabled {
			args = []string{"--allow-reject"}
		}
		opts, err := parseVerifyOpts(parseFSArgs(args))
		if err != nil {
			t.Fatal(err)
		}
		if opts.allowReject != enabled {
			t.Fatalf("allowReject=%v, want %v", opts.allowReject, enabled)
		}
	}
}

func TestEdgeShapeReporting(t *testing.T) {
	results := []caseResult{
		{Shape: "S7", Shapes: []string{"S7"}, Status: "PASS"},
		{Shape: "EDGE", Shapes: []string{"EDGE", "S7", "S8"}, Status: "REJECT(expected)"},
	}
	report := statusesByShape(results, nil)
	for _, shape := range []string{"EDGE", "S7", "S8"} {
		if got := shapeStatus(report[shape]); got != "UNSUPPORTED" {
			t.Fatalf("%s must include the binary rejection: %s", shape, got)
		}
	}
	if len(report["S7"]) != 2 {
		t.Fatal("each case must contribute once to a related shape")
	}
	for _, r := range results {
		if isFailure(r.Status) {
			t.Fatal("known limitations must remain non-failing")
		}
	}
	filtered := statusesByShape(results, map[string]bool{"S7": true})
	if len(filtered) != 1 || len(filtered["S7"]) != 2 {
		t.Fatal("shape selection must not advertise partially selected relatives")
	}
	c := cases.Case{ID: "hazard", Shape: "EDGE", RelatedShapes: []string{"S1"}, Hazard: "F1"}
	r := runCase(context.Background(), c, nil, nil, verifyOpts{})
	if r.Shape != "EDGE" || len(r.Shapes) != 2 || r.Shapes[1] != "S1" {
		t.Fatal("even a skipped hazard must retain its shape associations")
	}
	if got := shapeStatus(statusesByShape([]caseResult{r}, nil)["S1"]); got != "UNSUPPORTED" {
		t.Fatal("a skipped hazard must not disappear from its serving shape")
	}
}
