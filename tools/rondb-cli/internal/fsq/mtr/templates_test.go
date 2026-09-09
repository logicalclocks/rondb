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

package mtr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
)

func TestRenderRejectionRequiresExitAndMessage(t *testing.T) {
	c := cases.Case{
		ID: "rejected", Shape: "S6", Mode: "single", MTR: true,
		ExpectReject: cases.Known["S6-cte"],
		Statements:   []cases.Statement{{RonSQL: "SELECT 1;"}},
	}
	got := RenderTemplatesTest([]cases.Case{c}, "test")
	want := "--error 1\n" +
		"--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test --execute-file $QUERY_FILE > $MYSQL_TMP_DIR/rej_rejected_0.out 2>&1\n" +
		"--exec grep -qF 'Non-aggregating CTE body is not a single-row key lookup' $MYSQL_TMP_DIR/rej_rejected_0.out\n"
	if !strings.Contains(got, want) {
		t.Fatal("rejection must assert exit code 1 immediately before executing, then check the message")
	}
	if strings.Contains(got, "|| true") {
		t.Fatal("rejection exit status must not be discarded")
	}
	c.ExpectReject = nil
	if normal := RenderTemplatesTest([]cases.Case{c}, "test"); strings.Contains(normal, "--error 1") {
		t.Fatal("normal comparisons must not expect an execution error")
	}
}

func TestTemplatesGolden(t *testing.T) {
	cs, err := cases.Enumerate(cases.Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("..", "..", "..", "..", "..",
		"mysql-test", "suite", "ronsql_fs", "t", "ronsql_fs_templates.test")
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := RenderTemplatesTest(cs, "test"); got != string(want) {
		t.Fatal("checked-in ronsql_fs_templates.test differs from the case renderer; regenerate with .fs_emit_mtr --cases")
	}
}
