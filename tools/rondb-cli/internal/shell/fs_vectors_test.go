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
	"reflect"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

type vectorQueryFunc func(context.Context, string) exec.Response

func (f vectorQueryFunc) Query(ctx context.Context, sql string) exec.Response {
	return f(ctx, sql)
}

func TestSpecExpectedRejectPreservesEarlierMismatch(t *testing.T) {
	testSpecRejectionPreservesEarlierMismatch(t, false)
}

func TestSpecAllowedRejectPreservesEarlierMismatch(t *testing.T) {
	testSpecRejectionPreservesEarlierMismatch(t, true)
}

func testSpecRejectionPreservesEarlierMismatch(t *testing.T, allowReject bool) {
	t.Helper()
	specs, err := cases.Specs(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	var sp *cases.Spec
	for i := range specs {
		if specs[i].ID == "V-S6-cte-n5" {
			sp = &specs[i]
			break
		}
	}
	if sp == nil || sp.ExpectReject == nil {
		t.Fatal("missing expected-rejection spec")
	}
	rejectionMessage, rejectionStatus, reportedMessage := sp.ExpectReject.Pattern, "REJECT(expected)", sp.ExpectReject.Finding
	if allowReject {
		rejectionMessage, rejectionStatus, reportedMessage = "new unsupported shape", "REJECT(allowed)", "new unsupported shape"
	}
	// The first sampled key is customer 16, whose expected collect is empty.
	if keys, err := sp.SampleKeys(1, 1); err != nil || len(keys) < 2 || keys[0].Text() != "16" {
		t.Fatal("test needs an empty-history key followed by another unit")
	}
	result := func(prefix, amount string) *exec.Result {
		r := &exec.Result{
			Columns: []string{prefix + "event_time", prefix + "amount", prefix + "category"},
			Types:   []string{"TIMESTAMP", "BIGINT", "VARCHAR"},
		}
		if amount != "" {
			r.Rows = [][]exec.Cell{{{Text: "2026-05-31 00:00:00"}, {Text: amount}, {Text: "grocery"}}}
		}
		return r
	}
	for _, tc := range []struct {
		name, mysqlAmount, ronsqlAmount, status string
		mismatches, expectMismatches            int
	}{
		{"vector", "", "1", "FAIL", 1, 0},
		{"data-model", "1", "1", "EXPECT-FAIL", 0, 1},
		{"both", "1", "2", "FAIL", 1, 1},
		{"no-mismatch", "", "", rejectionStatus, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mysqlSQL, ronsqlSQL []string
			my := vectorQueryFunc(func(_ context.Context, sql string) exec.Response {
				mysqlSQL = append(mysqlSQL, sql)
				return exec.Response{Outcome: exec.OK, Result: result("tx_", tc.mysqlAmount)}
			})
			rd := vectorQueryFunc(func(_ context.Context, sql string) exec.Response {
				ronsqlSQL = append(ronsqlSQL, sql)
				if len(ronsqlSQL) == 1 {
					return exec.Response{Outcome: exec.OK, Result: result("", tc.ronsqlAmount)}
				}
				return exec.Response{Outcome: exec.CleanReject, Message: rejectionMessage}
			})
			r := runSpec(context.Background(), sp, my, rd, vectorOpts{
				verifyOpts: verifyOpts{allowReject: allowReject}, seed: 1, count: 1,
			})
			if len(mysqlSQL) != 2 || len(ronsqlSQL) != 2 || r.Units != 2 || r.Compared != 1 {
				t.Fatalf("must compare the first unit and stop at the second: %+v", r)
			}
			if r.Status != tc.status || r.Mismatches != tc.mismatches || r.ExpectMismatches != tc.expectMismatches {
				t.Fatalf("wrong classification or counters: %+v", r)
			}
			wantFailure := tc.mismatches+tc.expectMismatches > 0
			if isFailure(r.Status) != wantFailure {
				t.Fatalf("wrong exit/quiet-mode classification: %+v", r)
			}
			unit := 1
			if wantFailure {
				unit = 0
				if len(r.Detail) != tc.mismatches+tc.expectMismatches ||
					!strings.Contains(r.Message, "stopped after "+rejectionStatus+": "+reportedMessage) ||
					shapeStatus([]string{r.Status}) != "FAILED" {
					t.Fatalf("earlier failure evidence must remain visible: %+v", r)
				}
			} else if r.Message != reportedMessage || len(r.Detail) != 0 {
				t.Fatalf("an expected or allowed rejection alone stays non-failing: %+v", r)
			}
			if want := []string{mysqlSQL[unit], ronsqlSQL[unit]}; !reflect.DeepEqual(r.Statements, want) {
				t.Fatalf("statements = %v, want %v", r.Statements, want)
			}
		})
	}
}

func TestSpecSamplingErrorDoesNotQuery(t *testing.T) {
	specs, err := cases.Specs(cases.Config{DB: "test", Scale: data.NewScale(0.001)})
	if err != nil {
		t.Fatal(err)
	}
	engine := vectorQueryFunc(func(context.Context, string) exec.Response {
		t.Fatal("an invalid sample count must not issue queries")
		return exec.Response{}
	})
	for i := range specs {
		if specs[i].ID != "V-S1-agg" {
			continue
		}
		r := runSpec(context.Background(), &specs[i], engine, engine, vectorOpts{seed: 1, count: 120})
		if r.Status != "SAMPLE-ERROR" || !isFailure(r.Status) ||
			!strings.Contains(r.Message, "exceeds 102 unique base keys") ||
			r.Units != 0 || r.Keys != 0 || r.Compared != 0 {
			t.Fatalf("wrong sampling error report: %+v", r)
		}
		return
	}
	t.Fatal("missing aggregate spec")
}

func TestSpecMissingReferenceAndMySQLOnly(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		failure      bool
	}{
		{"mysql-only", "UNTESTED", false},
		{"missing-reference", "REFERENCE-ERROR", true},
		{"no-queries", "REFERENCE-ERROR", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			specs, err := cases.Specs(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
			if err != nil {
				t.Fatal(err)
			}
			var sp *cases.Spec
			for i := range specs {
				if specs[i].ID == "V-S1-agg" {
					sp = &specs[i]
					break
				}
			}
			if sp == nil || len(sp.DTOs()) != 1 {
				t.Fatal("test needs the single-DTO aggregate spec")
			}
			// Even an expected-rejection annotation must not turn a skipped
			// template into PASS(was-expected-reject).
			sp.ExpectReject = cases.Known["S6-cte"]
			dto := &sp.DTOs()[0]
			if tc.name != "missing-reference" {
				dto.QueryRonsql = nil
			}
			if tc.name != "mysql-only" {
				dto.QueryOnline = nil
			}
			engine := vectorQueryFunc(func(context.Context, string) exec.Response {
				t.Fatal("a skipped DTO or missing reference must not issue queries")
				return exec.Response{}
			})
			r := runSpec(context.Background(), sp, engine, engine, vectorOpts{seed: 1, count: 1})
			if r.Status != tc.status || isFailure(r.Status) != tc.failure || r.Compared != 0 {
				t.Fatalf("wrong coverage classification: %+v", r)
			}
			if tc.name == "mysql-only" {
				if r.Units == 0 || r.MySQLOnlyGroups != r.Units || shapeStatus([]string{r.Status}) != "UNTESTED" {
					t.Fatalf("MySQL-only coverage must be counted, not supported: %+v", r)
				}
			} else if r.MySQLOnlyGroups != 0 || !strings.Contains(r.Message, "lacks its production MySQL query") {
				t.Fatalf("a missing reference is not a legitimate skip: %+v", r)
			}
			if tc.name == "missing-reference" && len(r.Statements) != 1 {
				t.Fatal("retain the RonSQL statement whose reference is missing")
			}
		})
	}
}

func TestSpecExecutedWithoutComparedCells(t *testing.T) {
	specs, err := cases.Specs(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range specs {
		sp := &specs[i]
		if sp.ID != "V-S1-agg" {
			continue
		}
		// Simulate missing fold metadata: queries still exist, but neither
		// a declared output set nor an independent model supplies evidence.
		sp.DTOs()[0].AggregateFeatureNames = nil
		sp.Family = ""
		calls := 0
		engine := vectorQueryFunc(func(context.Context, string) exec.Response {
			calls++
			return exec.Response{Outcome: exec.OK, Result: &exec.Result{}}
		})
		r := runSpec(context.Background(), sp, engine, engine, vectorOpts{seed: 1, count: 1})
		if r.Status != "UNTESTED" || isFailure(r.Status) || r.Compared != 0 ||
			r.UncomparedGroups != r.Units || r.Units == 0 || calls != 2*r.Units {
			t.Fatalf("executed queries alone are not vector evidence: %+v", r)
		}
		return
	}
	t.Fatal("missing aggregate spec")
}

func TestVectorUntestedShapeStatus(t *testing.T) {
	for _, tc := range []struct {
		statuses []string
		want     string
	}{
		{[]string{"PASS", "UNTESTED"}, "UNTESTED"},
		{[]string{"UNTESTED", "PASS"}, "UNTESTED"},
		{[]string{"UNTESTED", "REJECT(expected)"}, "UNSUPPORTED"},
		{[]string{"REJECT(expected)", "UNTESTED"}, "UNSUPPORTED"},
		{[]string{"UNTESTED", "FAIL"}, "FAILED"},
		{[]string{"FAIL", "UNTESTED"}, "FAILED"},
	} {
		if got := shapeStatus(tc.statuses); got != tc.want {
			t.Fatalf("shapeStatus(%v)=%s, want %s", tc.statuses, got, tc.want)
		}
	}
}

func TestSpecAllowRejectOnlyAllowsCleanRejections(t *testing.T) {
	specs, err := cases.Specs(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	var sp *cases.Spec
	for i := range specs {
		if specs[i].ID == "V-S6-cte-n5" {
			sp = &specs[i]
			break
		}
	}
	if sp == nil || sp.ExpectReject == nil {
		t.Fatal("missing expected-rejection spec")
	}
	for _, tc := range []struct {
		name, message, status, shape string
		outcome                      exec.Outcome
		allow                        bool
	}{
		{"strict", "new unsupported shape", "REJECT", "FAILED", exec.CleanReject, false},
		{"allowed", "new unsupported shape", "REJECT(allowed)", "UNSUPPORTED", exec.CleanReject, true},
		{"expected", sp.ExpectReject.Pattern, "REJECT(expected)", "UNSUPPORTED", exec.CleanReject, true},
		{"malformed-json", "malformed JSON result: invalid token", "ERROR", "FAILED", exec.Error, true},
		{"other-error", "HTTP 400: invalid request", "ERROR", "FAILED", exec.Error, true},
		{"timeout", "deadline exceeded", "TIMEOUT", "FAILED", exec.Timeout, true},
		{"crash", "probe failed", "CRASH", "FAILED", exec.Crash, true},
		{"retry-exhausted", "retry limit", "RETRY-EXHAUSTED", "FAILED", exec.Retryable, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			my := vectorQueryFunc(func(context.Context, string) exec.Response {
				return exec.Response{Outcome: exec.OK, Result: &exec.Result{}}
			})
			rd := vectorQueryFunc(func(context.Context, string) exec.Response {
				return exec.Response{Outcome: tc.outcome, Message: tc.message}
			})
			r := runSpec(context.Background(), sp, my, rd, vectorOpts{
				verifyOpts: verifyOpts{allowReject: tc.allow}, seed: 1, count: 1,
			})
			if r.Status != tc.status || shapeStatus([]string{r.Status}) != tc.shape ||
				isFailure(r.Status) != (tc.shape == "FAILED") {
				t.Fatalf("wrong rejection policy: %+v", r)
			}
			wantMessage := tc.message
			if tc.status == "REJECT(expected)" {
				wantMessage = sp.ExpectReject.Finding
			}
			if r.Message != wantMessage || r.Units != 1 || r.Compared != 0 || len(r.Statements) != 2 {
				t.Fatalf("error context must be retained: %+v", r)
			}
		})
	}
}
