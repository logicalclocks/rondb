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
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
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
		{"UNEXPECTED-SUCCESS", true, "FAILED"},
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

type caseQueryFunc func(context.Context, string) exec.Response

func (f caseQueryFunc) Query(ctx context.Context, sql string) exec.Response {
	return f(ctx, sql)
}

func TestBigOverflowRequires1860(t *testing.T) {
	cs, err := cases.Enumerate(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	var c cases.Case
	for _, candidate := range cs {
		if candidate.ID == "EDGE-big-overflow" {
			c = candidate
		}
	}
	if !c.RequireReject || c.ExpectReject == nil || c.KnownWrong != nil || len(c.Statements) != 1 {
		t.Fatal("overflow must require a rejection without a wrong-result exemption")
	}
	result := func(value string) exec.Response {
		return exec.Response{Outcome: exec.OK, Result: &exec.Result{
			Columns: []string{"big_sum"}, Rows: [][]exec.Cell{{{Text: value}}},
		}}
	}
	ref := result("9223372036854775808")
	my := caseQueryFunc(func(context.Context, string) exec.Response { return ref })
	for _, tc := range []struct {
		name   string
		rsp    exec.Response
		status string
	}{
		{"overflow", exec.Response{Outcome: exec.CleanReject, Message: "NDB Permanent error 1860, Application error: arithmetic operation results overflow"}, "REJECT(expected)"},
		{"wrong-code", exec.Response{Outcome: exec.CleanReject, Message: "NDB Permanent error 4008, arithmetic operation results overflow"}, "REJECT"},
		{"missing-code", exec.Response{Outcome: exec.CleanReject, Message: "arithmetic operation results overflow"}, "REJECT"},
		{"wrapped", result("-9223372036854775808"), "UNEXPECTED-SUCCESS"},
		{"widened", ref, "UNEXPECTED-SUCCESS"},
		{"timeout", exec.Response{Outcome: exec.Timeout}, "TIMEOUT"},
		{"crash", exec.Response{Outcome: exec.Crash}, "CRASH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rd := caseQueryFunc(func(context.Context, string) exec.Response { return tc.rsp })
			for _, allow := range []bool{false, true} {
				got := runCase(context.Background(), c, my, rd, verifyOpts{allowReject: allow})
				if got.Status != tc.status {
					t.Fatalf("allowReject=%v: got %s, want %s", allow, got.Status, tc.status)
				}
				wantEvidence := "failed"
				if tc.status == "REJECT(expected)" {
					wantEvidence = "unsupported"
				}
				if cases.EvidenceClass(got.Status) != wantEvidence {
					t.Fatalf("incorrect requirements evidence for %s", got.Status)
				}
			}
		})
	}
	// Ordinary known limitations may still improve to a matching result.
	c.RequireReject = false
	got := runCase(context.Background(), c, my, my, verifyOpts{})
	if got.Status != "PASS(was-expected-reject)" {
		t.Fatalf("ordinary expected rejection: got %s", got.Status)
	}
}

func TestFloatDisplayRegressionIsFailure(t *testing.T) {
	cs, err := cases.Enumerate(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	var c cases.Case
	for _, candidate := range cs {
		if candidate.ID == "EDGE-float-rounding" {
			c = candidate
		}
	}
	if len(c.Statements) != 1 || c.KnownWrong != nil {
		t.Fatal("FLOAT display probe must run without a wrong-result exemption")
	}
	result := func(max string) exec.Response {
		return exec.Response{Outcome: exec.OK, Result: &exec.Result{
			Columns: []string{"ff_sum", "fd_sum", "ff_max", "dec_sum"},
			Types:   []string{"DOUBLE", "DOUBLE", "FLOAT", "DECIMAL"},
			Rows: [][]exec.Cell{{{Text: "123457.38906261639"},
				{Text: "123457.38900010001"}, {Text: max}, {Text: "123457.39"}}},
		}}
	}
	my := caseQueryFunc(func(context.Context, string) exec.Response { return result("123457") })
	for _, tc := range []struct{ value, status string }{
		{"123457", "PASS"},
		{"123456.7890625", "WRONG-RESULT"},
	} {
		t.Run(tc.status, func(t *testing.T) {
			rd := caseQueryFunc(func(context.Context, string) exec.Response { return result(tc.value) })
			got := runCase(context.Background(), c, my, rd, verifyOpts{})
			if got.Status != tc.status {
				t.Fatalf("got %s, want %s", got.Status, tc.status)
			}
		})
	}
}
