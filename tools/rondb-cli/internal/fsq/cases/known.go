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
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

// Known is the expectation table (random_generator.md §5.3, findings in
// mysql-test/suite/ronsql_fs/findings/smoke.md): the single place where
// "known engine outcome" lives.  A case referencing an entry reports
// REJECT(expected) / KNOWN-WRONG instead of FAIL; a case that
// unexpectedly passes reports PASS(was-...) so the entry can be retired.
var Known = map[string]*Expect{
	// DECIMAL(18,2) beyond 2^53 cents loses precision on RonSQL's DOUBLE path.
	"F5": {Finding: "F5", Pattern: "DECIMAL precision loss",
		Wrong: &WrongValue{Column: "dec_max", MySQL: "999999999999999.99", RonSQL: "1000000000000000"}},
	// Intentional 64-bit SUM range difference: local and merged overflow must
	// report 1860. MySQL widens to DECIMAL; wrapped values are never accepted.
	"F6": {Finding: "F6", Pattern: "NDB Permanent error 1860,"},
	// A snowflake template whose CTE body is keyed by a VARCHAR entity key
	// returns no rows through CTE_SCAN although the body alone returns its
	// group (E6, findings/spec_fuzz.md).  Detected structurally by the fuzzer.
	"F14": {Finding: "F14", Pattern: "snowflake CTE body keyed by a VARCHAR entity key returns no rows"},
}

// WrongValue pins the observed mismatch of a single-row fixture. All
// other cells, output names and row counts must still compare normally.
type WrongValue struct {
	Column, MySQL, RonSQL string
}

// MatchesWrong grants an exemption only for the recorded value pair.
// Work on a copy so the original response and diagnostic diff stay intact.
func (e *Expect) MatchesWrong(ref, got *exec.Result, opt canon.Options) bool {
	if e == nil || e.Wrong == nil || ref == nil || got == nil {
		return false
	}
	if len(ref.Rows) != 1 || len(got.Rows) != 1 ||
		len(ref.Columns) != len(got.Columns) ||
		len(ref.Rows[0]) != len(ref.Columns) ||
		len(got.Rows[0]) != len(ref.Columns) {
		return false
	}
	column := -1
	for i, name := range ref.Columns {
		if got.Columns[i] != name {
			return false
		}
		if name == e.Wrong.Column {
			column = i
		}
	}
	if column < 0 ||
		ref.Rows[0][column] != (exec.Cell{Text: e.Wrong.MySQL}) ||
		got.Rows[0][column] != (exec.Cell{Text: e.Wrong.RonSQL}) {
		return false
	}
	adjusted := *got
	adjusted.Rows = [][]exec.Cell{append([]exec.Cell(nil), got.Rows[0]...)}
	adjusted.Rows[0][column] = ref.Rows[0][column]
	opt.RelaxedHeaders = false
	return canon.Compare(ref, &adjusted, opt).Equal
}
