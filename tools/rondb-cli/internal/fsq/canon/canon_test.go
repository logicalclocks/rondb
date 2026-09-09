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

package canon

import (
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

func res(cols []string, types []string, rows ...[]exec.Cell) *exec.Result {
	return &exec.Result{Columns: cols, Types: types, Rows: rows}
}

func cell(s string) exec.Cell { return exec.Cell{Text: s} }

var null = exec.Cell{Null: true}

func TestCompareTypes(t *testing.T) {
	ref := res([]string{"d", "f", "ts", "s", "n"}, []string{"DECIMAL", "DOUBLE", "TIMESTAMP", "VARCHAR", "BIGINT"},
		[]exec.Cell{cell("10.50"), cell("0.5"), cell("2026-05-01 12:00:00.000"), cell("NULL"), null})
	got := res([]string{"d", "f", "ts", "s", "n"}, nil,
		[]exec.Cell{cell("10.5"), cell("0.5000"), cell("2026-05-01 12:00:00"), cell("NULL"), null})
	if r := Compare(ref, got, Options{}); !r.Equal {
		t.Errorf("typed equality expected: %s\n%s", r.Reason, r.Diff)
	}
	// the string "NULL" is not SQL NULL
	got2 := res([]string{"d", "f", "ts", "s", "n"}, nil,
		[]exec.Cell{cell("10.5"), cell("0.5000"), cell("2026-05-01 12:00:00"), null, null})
	if r := Compare(ref, got2, Options{}); r.Equal {
		t.Error("SQL NULL must differ from the string NULL")
	}
	// float tolerance
	ref3 := res([]string{"f"}, []string{"FLOAT"}, []exec.Cell{cell("123457")})
	got3 := res([]string{"f"}, nil, []exec.Cell{cell("123456.7890625")})
	if r := Compare(ref3, got3, Options{}); r.Equal {
		t.Error("1.7e-6 relative difference must fail at 1e-9")
	}
	if r := Compare(ref3, got3, Options{Tolerance: 1e-5}); !r.Equal {
		t.Error("must pass at 1e-5")
	}
}

func TestCompareOrderAndHeaders(t *testing.T) {
	a := []exec.Cell{cell("1")}
	b := []exec.Cell{cell("2")}
	ref := res([]string{"k"}, []string{"BIGINT"}, a, b)
	got := res([]string{"k"}, nil, b, a)
	if r := Compare(ref, got, Options{}); !r.Equal {
		t.Error("multiset compare must ignore order")
	}
	if r := Compare(ref, got, Options{Ordered: true}); r.Equal {
		t.Error("ordered compare must see the order")
	}
	got2 := res([]string{"K"}, nil, a, b)
	if r := Compare(ref, got2, Options{}); r.Equal || r.HeaderOnly {
		t.Error("alias mismatch must fail by default")
	}
	if r := Compare(ref, got2, Options{RelaxedHeaders: true}); !r.Equal || !r.HeaderOnly {
		t.Error("relaxed headers must report HEADER-ONLY and compare rows")
	}
	got3 := res([]string{"k"}, nil, a)
	if r := Compare(ref, got3, Options{}); r.Equal || r.Diff == "" {
		t.Error("row count mismatch must fail with a diff")
	}
	empty := res([]string{"k"}, []string{"BIGINT"})
	if r := Compare(empty, res(nil, nil), Options{}); !r.Equal || r.Note == "" {
		t.Error("two empty results must compare equal with a note")
	}
	if r := Compare(empty, res([]string{"x"}, nil), Options{}); r.Equal {
		t.Error("an empty result with a different header must still fail")
	}
	if r := Compare(ref, res(nil, nil), Options{}); r.Equal {
		t.Error("rows vs an empty result must fail")
	}
}
