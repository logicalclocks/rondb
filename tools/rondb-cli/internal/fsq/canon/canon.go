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

// Package canon compares two result sets after type-aware canonicalization
// (framework_design.md §7): explicit nullness, exact integers and strings,
// DECIMAL compared as exact rationals, DOUBLE/FLOAT with a relative
// tolerance, TIMESTAMP/DATETIME with trailing fractional zeros removed,
// production output names compared exactly, rows as ordered lists or as
// multisets.
package canon

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

// Options controls a comparison.
type Options struct {
	Ordered        bool
	Tolerance      float64 // relative tolerance for DOUBLE/FLOAT (default 1e-9)
	RelaxedHeaders bool    // envelope cases only: a name mismatch is HEADER-ONLY
}

// Report is the comparison outcome.
type Report struct {
	Equal      bool
	HeaderOnly bool
	Reason     string
	Diff       string
	Note       string // informational, set together with Equal
}

type kind int

const (
	kExact kind = iota
	kDecimal
	kFloat
	kTime
)

func kindOf(dbType string) kind {
	switch strings.ToUpper(dbType) {
	case "DECIMAL", "NEWDECIMAL":
		return kDecimal
	case "DOUBLE", "FLOAT":
		return kFloat
	case "TIMESTAMP", "DATETIME", "TIME":
		return kTime
	}
	return kExact
}

// normTime trims trailing fractional zeros: "12:00:00.000" -> "12:00:00".
func normTime(s string) string {
	if i := strings.LastIndexByte(s, '.'); i >= 0 && !strings.ContainsAny(s[i+1:], "-: ") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	return s
}

// canonical renders a cell for sorting and exact comparison.
func canonical(c exec.Cell, k kind) string {
	if c.Null {
		return "\x00NULL"
	}
	switch k {
	case kDecimal:
		if r, ok := new(big.Rat).SetString(c.Text); ok {
			return r.RatString()
		}
	case kFloat:
		if f, err := strconv.ParseFloat(c.Text, 64); err == nil {
			return strconv.FormatFloat(f, 'g', 15, 64)
		}
	case kTime:
		return normTime(c.Text)
	}
	return c.Text
}

func cellsEqual(a, b exec.Cell, k kind, tol float64) bool {
	if a.Null || b.Null {
		return a.Null && b.Null
	}
	switch k {
	case kDecimal:
		ra, oka := new(big.Rat).SetString(a.Text)
		rb, okb := new(big.Rat).SetString(b.Text)
		if oka && okb {
			return ra.Cmp(rb) == 0
		}
	case kFloat:
		fa, ea := strconv.ParseFloat(a.Text, 64)
		fb, eb := strconv.ParseFloat(b.Text, 64)
		if ea == nil && eb == nil {
			if fa == fb {
				return true
			}
			mag := math.Max(math.Abs(fa), math.Abs(fb))
			return math.Abs(fa-fb) <= tol*mag
		}
	case kTime:
		return normTime(a.Text) == normTime(b.Text)
	}
	return a.Text == b.Text
}

func rowKey(row []exec.Cell, kinds []kind) string {
	parts := make([]string, len(row))
	for i, c := range row {
		k := kExact
		if i < len(kinds) {
			k = kinds[i]
		}
		parts[i] = canonical(c, k)
	}
	return strings.Join(parts, "\x01")
}

func rowText(row []exec.Cell) string {
	parts := make([]string, len(row))
	for i, c := range row {
		if c.Null {
			parts[i] = "NULL"
		} else {
			parts[i] = c.Text
		}
	}
	return strings.Join(parts, "\t")
}

// Compare compares got (RonSQL) against ref (MySQL, which carries the
// column types).
func Compare(ref, got *exec.Result, opt Options) Report {
	if opt.Tolerance == 0 {
		opt.Tolerance = 1e-9
	}
	rep := Report{}
	// An empty RonSQL result carries no column list ({"data":[]} in JSON,
	// no header line in TEXT), so the output names of an empty result
	// cannot be verified: two empty results compare equal.
	if len(ref.Rows) == 0 && len(got.Rows) == 0 && len(got.Columns) == 0 {
		rep.Equal = true
		rep.Note = "empty result on both engines (RonSQL carries no column list)"
		return rep
	}
	if strings.Join(ref.Columns, "\x00") != strings.Join(got.Columns, "\x00") {
		if !opt.RelaxedHeaders || len(ref.Columns) != len(got.Columns) {
			rep.Reason = "output names differ"
			rep.Diff = fmt.Sprintf("- %s\n+ %s\n", strings.Join(ref.Columns, "\t"), strings.Join(got.Columns, "\t"))
			return rep
		}
		rep.HeaderOnly = true
	}
	kinds := make([]kind, len(ref.Columns))
	for i := range kinds {
		if i < len(ref.Types) {
			kinds[i] = kindOf(ref.Types[i])
		}
	}
	if len(ref.Rows) != len(got.Rows) {
		rep.Reason = fmt.Sprintf("row count %d vs %d", len(ref.Rows), len(got.Rows))
		rep.Diff = rowDiff(ref.Rows, got.Rows, kinds, opt)
		return rep
	}
	refRows, gotRows := ref.Rows, got.Rows
	if !opt.Ordered {
		refRows = sortedRows(refRows, kinds)
		gotRows = sortedRows(gotRows, kinds)
	}
	for i := range refRows {
		if len(refRows[i]) != len(gotRows[i]) {
			rep.Reason = fmt.Sprintf("row %d width %d vs %d", i, len(refRows[i]), len(gotRows[i]))
			rep.Diff = rowDiff(ref.Rows, got.Rows, kinds, opt)
			return rep
		}
		for c := range refRows[i] {
			if !cellsEqual(refRows[i][c], gotRows[i][c], kinds[c], opt.Tolerance) {
				col := ""
				if c < len(ref.Columns) {
					col = ref.Columns[c]
				}
				rep.Reason = fmt.Sprintf("column %q differs", col)
				rep.Diff = rowDiff(ref.Rows, got.Rows, kinds, opt)
				return rep
			}
		}
	}
	rep.Equal = true
	return rep
}

func sortedRows(rows [][]exec.Cell, kinds []kind) [][]exec.Cell {
	out := append([][]exec.Cell(nil), rows...)
	sort.SliceStable(out, func(a, b int) bool { return rowKey(out[a], kinds) < rowKey(out[b], kinds) })
	return out
}

// rowDiff lists rows only on one side (multiset difference, up to 20 lines).
func rowDiff(ref, got [][]exec.Cell, kinds []kind, opt Options) string {
	count := map[string]int{}
	text := map[string]string{}
	for _, r := range ref {
		k := rowKey(r, kinds)
		count[k]++
		text[k] = rowText(r)
	}
	for _, r := range got {
		k := rowKey(r, kinds)
		count[k]--
		if _, ok := text[k]; !ok {
			text[k] = rowText(r)
		}
	}
	var keys []string
	for k := range count {
		if count[k] != 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	lines := 0
	for _, k := range keys {
		n := count[k]
		sign := "-"
		if n < 0 {
			sign, n = "+", -n
		}
		for i := 0; i < n && lines < 20; i++ {
			b.WriteString(sign + " " + text[k] + "\n")
			lines++
		}
	}
	if lines == 0 && opt.Ordered {
		b.WriteString("(same rows, different order)\n")
	}
	return b.String()
}
