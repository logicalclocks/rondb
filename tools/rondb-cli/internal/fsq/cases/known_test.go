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
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

func TestMatchesWrong(t *testing.T) {
	for _, tc := range []struct {
		id, column, typ, mysql, ronsql, sum string
	}{
		{"F4", "ff_max", "FLOAT", "123457", "123456.7890625", "123457.39"},
		{"F5", "dec_max", "DECIMAL", "999999999999999.99", "1000000000000000", "0.00"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			result := func(value string) *exec.Result {
				return &exec.Result{
					Columns: []string{"dec_sum", tc.column},
					Types:   []string{"DECIMAL", tc.typ},
					Rows:    [][]exec.Cell{{{Text: tc.sum}, {Text: value}}},
				}
			}
			ref, got := result(tc.mysql), result(tc.ronsql)
			known := Known[tc.id]
			if canon.Compare(ref, got, canon.Options{}).Equal {
				t.Fatal("fixture must expose the recorded mismatch")
			}
			if !known.MatchesWrong(ref, got, canon.Options{}) {
				t.Fatal("recorded mismatch must remain non-failing")
			}
			if got.Rows[0][1].Text != tc.ronsql {
				t.Fatal("matching must not modify the original response")
			}
			if tc.id == "F5" {
				got.Rows[0][0].Text = "0"
				if !known.MatchesWrong(ref, got, canon.Options{}) {
					t.Fatal("unaffected DECIMAL cells must retain normal canonicalization")
				}
			}
			for _, mutation := range []struct {
				name string
				edit func(*exec.Result)
			}{
				{"other-value", func(r *exec.Result) { r.Rows[0][0].Text = "42" }},
				{"other-null", func(r *exec.Result) { r.Rows[0][0] = exec.Cell{Null: true} }},
				{"different-bug-value", func(r *exec.Result) { r.Rows[0][1].Text = "42" }},
				{"bug-value-null", func(r *exec.Result) { r.Rows[0][1] = exec.Cell{Null: true} }},
				{"alias", func(r *exec.Result) { r.Columns[0] = "changed" }},
				{"missing-row", func(r *exec.Result) { r.Rows = nil }},
				{"extra-row", func(r *exec.Result) { r.Rows = append(r.Rows, r.Rows[0]) }},
				{"row-width", func(r *exec.Result) { r.Rows[0] = r.Rows[0][:1] }},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					got := result(tc.ronsql)
					mutation.edit(got)
					if known.MatchesWrong(ref, got, canon.Options{RelaxedHeaders: true}) {
						t.Fatal("unrelated regression must not receive a known-bug exemption")
					}
				})
			}
			changedRef := result("42")
			if known.MatchesWrong(changedRef, result(tc.ronsql), canon.Options{}) {
				t.Fatal("a changed reference value must invalidate the signature")
			}
			if known.MatchesWrong(ref, result(tc.mysql), canon.Options{}) {
				t.Fatal("a fixed result is a normal pass, not a known mismatch")
			}
			var absent *Expect
			for _, e := range []*Expect{absent, Known["F9"]} {
				if e.MatchesWrong(ref, result(tc.ronsql), canon.Options{}) {
					t.Fatal("an absent signature must not grant an exemption")
				}
			}
		})
	}
}

func TestMatchesErrorF9(t *testing.T) {
	ref := &exec.Result{
		Columns: []string{"cnt", "d_min"},
		Types:   []string{"BIGINT", "DATE"},
		Rows:    [][]exec.Cell{{{Text: "3"}, {Text: "2000-01-01"}}},
	}
	response := func(body string) exec.Response {
		return exec.Response{Outcome: exec.Error, Message: "malformed JSON result: syntax error",
			Result: &exec.Result{Raw: body}}
	}
	for _, tc := range []struct {
		name, body string
		match      bool
	}{
		{"known", `{"data":[{"cnt":3,"d_min":2000-01-01}]}`, true},
		{"bare-array", `[{"cnt":3,"d_min":2000-01-01}]`, true},
		{"whitespace", `{ "data": [ { "cnt": 3, "d_min": 2000-01-01 } ] }`, true},
		{"fixed", `{"data":[{"cnt":3,"d_min":"2000-01-01"}]}`, false},
		{"truncated", `{"data":[{"cnt":3,"d_min":2000-01-01}]`, false},
		{"trailing-garbage", `{"data":[{"cnt":3,"d_min":2000-01-01}]} broken`, false},
		{"wrong-count", `{"data":[{"cnt":4,"d_min":2000-01-01}]}`, false},
		{"wrong-date", `{"data":[{"cnt":3,"d_min":2001-01-01}]}`, false},
		{"wrong-alias", `{"data":[{"changed":3,"d_min":2000-01-01}]}`, false},
		{"unknown-temporal", `{"data":[{"cnt":3,"other":2000-01-01}]}`, false},
		{"duplicate-name", `{"data":[{"cnt":3,"d_min":2000-01-01,"d_min":2000-01-01}]}`, false},
		{"missing-row", `{"data":[]}`, false},
		{"extra-row", `{"data":[{"cnt":3,"d_min":2000-01-01},{"cnt":3,"d_min":2000-01-01}]}`, false},
		{"null-count", `{"data":[{"cnt":null,"d_min":2000-01-01}]}`, false},
		{"malformed-metadata", `{"data":[{"cnt":3,"d_min":2000-01-01}],"metadata":{"d_min":2000-01-01}}`, false},
		{"duplicate-data", `{"data":[{"cnt":3,"d_min":2000-01-01}],"data":[]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := response(tc.body)
			if got := Known["F9"].MatchesError(ref, r, canon.Options{RelaxedHeaders: true}); got != tc.match {
				t.Fatalf("match=%v, want %v", got, tc.match)
			}
			if r.Outcome != exec.Error || r.Result.Raw != tc.body {
				t.Fatal("F9 matching must preserve the original error and raw response")
			}
		})
	}
	t.Run("quoted-text", func(t *testing.T) {
		ref := &exec.Result{
			Columns: []string{"d_min", "s"},
			Types:   []string{"DATE", "VARCHAR"},
			Rows:    [][]exec.Cell{{{Text: "2000-01-01"}, {Text: `{"d_min":2000-01-01}`}}},
		}
		r := response(`{"data":[{"d_min":2000-01-01,"s":"{\"d_min\":2000-01-01}"}]}`)
		if !Known["F9"].MatchesError(ref, r, canon.Options{}) {
			t.Fatal("text inside a quoted JSON string must remain untouched")
		}
	})
	for _, typ := range []string{"TIMESTAMP", "DATETIME"} {
		t.Run(typ, func(t *testing.T) {
			ref := &exec.Result{
				Columns: []string{"entity_id", "ts3_min", "ts6_min"},
				Types:   []string{"BIGINT", typ, typ},
				Rows: [][]exec.Cell{
					{{Text: "1"}, {Text: "2026-05-01 12:00:00.000"}, {Text: "2026-05-01 12:00:00.000001"}},
					{{Text: "2"}, {Null: true}, {Text: "2026-05-01 12:00:00.000000"}},
				},
			}
			r := response(`{"data":[{"entity_id":2,"ts3_min":null,"ts6_min":2026-05-01 12:00:00},{"entity_id":1,"ts3_min":2026-05-01 12:00:00.000,"ts6_min":2026-05-01 12:00:00.000001}]}`)
			if !Known["F9"].MatchesError(ref, r, canon.Options{}) {
				t.Fatal("timestamp fractions, NULLs and unordered batch rows must match")
			}
			if Known["F9"].MatchesError(ref, r, canon.Options{Ordered: true}) {
				t.Fatal("ordered comparison must still detect reordered rows")
			}
		})
	}
	r := response(`{"data":[{"cnt":3,"d_min":2000-01-01}]}`)
	ref.Types[1] = "VARCHAR"
	if Known["F9"].MatchesError(ref, r, canon.Options{}) {
		t.Fatal("an unquoted non-temporal value is not F9")
	}
	ref.Types[1] = "DATE"
	var absent *Expect
	for _, e := range []*Expect{absent, Known["F4"]} {
		if e.MatchesError(ref, r, canon.Options{}) {
			t.Fatal("a case without the F9 signature must not receive an exemption")
		}
	}
	for _, outcome := range []exec.Outcome{exec.OK, exec.CleanReject, exec.Retryable, exec.Timeout, exec.Crash} {
		r.Outcome = outcome
		if Known["F9"].MatchesError(ref, r, canon.Options{}) {
			t.Fatal("non-parse outcomes must not receive an F9 exemption")
		}
	}
	r.Outcome, r.Message = exec.Error, "HTTP 500: error"
	if Known["F9"].MatchesError(ref, r, canon.Options{}) {
		t.Fatal("unrelated errors must not receive an F9 exemption")
	}
}
