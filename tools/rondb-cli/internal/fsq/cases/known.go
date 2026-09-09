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
	"encoding/json"
	"regexp"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/canon"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

// Known is the expectation table (random_generator.md §5.3, findings in
// mysql-test/suite/ronsql_fs/findings/smoke.md): the single place where
// "known engine outcome" lives.  A case referencing an entry reports
// REJECT(expected) / KNOWN-WRONG / KNOWN-ERROR instead of FAIL; a case that
// unexpectedly passes reports PASS(was-...) so the entry can be retired.
var Known = map[string]*Expect{
	// Hopsworks collect CTE form: non-aggregating CTE body over a partial key (risk R1).
	"S6-cte": {Finding: "F0", Pattern: "Non-aggregating CTE body is not a single-row key lookup"},
	// MySQL prints FLOAT with display precision; RonSQL prints the exact binary32 value.
	"F4": {Finding: "F4", Pattern: "FLOAT display precision",
		Wrong: &WrongValue{Column: "ff_max", MySQL: "123457", RonSQL: "123456.7890625"}},
	// DECIMAL(18,2) beyond 2^53 cents loses precision on RonSQL's DOUBLE path.
	"F5": {Finding: "F5", Pattern: "DECIMAL precision loss",
		Wrong: &WrongValue{Column: "dec_max", MySQL: "999999999999999.99", RonSQL: "1000000000000000"}},
	// BIGINT SUM overflow is a clean NDB error (MySQL widens to DECIMAL).
	"F6": {Finding: "F6", Pattern: "arithmetic operation results overflow"},
	// VARBINARY cannot be projected by the pass-through printer.
	"F7": {Finding: "F7", Pattern: "Unsupported column type"},
	// MIN/MAX over a DATE/TIMESTAMP column is unquoted in JSON output (RDRS
	// default): the body is not JSON.  Verify-only (Case.KnownError).
	"F9": {Finding: "F9", Pattern: "malformed JSON result",
		UnquotedTemporal: []string{"d_min", "d_max", "ts3_min", "ts6_min", "ts0_min"}},
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

// Match complete JSON strings as well as bare temporal member values.
// The string alternative prevents edits inside an already quoted value.
var temporalJSONToken = regexp.MustCompile(
	`("(?:[^"\\]|\\.)*")(\s*:\s*)([0-9]{4}-[0-9]{2}-[0-9]{2}(?: [0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?)?)(\s*[,}])|"(?:[^"\\]|\\.)*"`)

// MatchesError recognizes F9 without converting the response into success:
// quoting only the named temporal fields must restore a complete result
// that agrees with MySQL. The original malformed body is never modified.
func (e *Expect) MatchesError(ref *exec.Result, response exec.Response, opt canon.Options) bool {
	if e == nil || len(e.UnquotedTemporal) == 0 || e.Pattern == "" ||
		ref == nil || response.Result == nil || response.Outcome != exec.Error ||
		!strings.HasPrefix(response.Message, e.Pattern+":") {
		return false
	}
	raw := response.Result.Raw
	if json.Valid([]byte(raw)) {
		return false
	}
	allowed := map[string]bool{}
	for i, name := range ref.Columns {
		if i >= len(ref.Types) {
			continue
		}
		switch strings.ToUpper(ref.Types[i]) {
		case "DATE", "TIMESTAMP", "DATETIME":
			for _, column := range e.UnquotedTemporal {
				if name == column {
					allowed[name] = true
				}
			}
		}
	}
	changed := false
	quoted := temporalJSONToken.ReplaceAllStringFunc(raw, func(token string) string {
		parts := temporalJSONToken.FindStringSubmatch(token)
		if parts[1] == "" {
			return token
		}
		var name string
		if json.Unmarshal([]byte(parts[1]), &name) != nil || !allowed[name] {
			return token
		}
		changed = true
		return parts[1] + parts[2] + `"` + parts[3] + `"` + parts[4]
	})
	if !changed {
		return false
	}
	// The recorded F9 envelope is either a bare row array or {"data":...}.
	// Do not excuse malformed metadata outside the result rows.
	if strings.HasPrefix(strings.TrimSpace(quoted), "{") {
		var envelope map[string]json.RawMessage
		if json.Unmarshal([]byte(quoted), &envelope) != nil ||
			len(envelope) != 1 || envelope["data"] == nil {
			return false
		}
	}
	columns, rows, err := exec.ParseJSONData([]byte(quoted))
	if err != nil {
		return false
	}
	opt.RelaxedHeaders = false
	return canon.Compare(ref, &exec.Result{Columns: columns, Rows: rows}, opt).Equal
}
