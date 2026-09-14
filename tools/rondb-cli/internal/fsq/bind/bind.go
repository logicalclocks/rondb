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

// Package bind substitutes the `?` markers of Hopsworks serving statements
// with typed literals (ServingPreparedStatementDTO: "the client substitutes
// typed literals since RonSQL has no parameter binding").  The same binder
// serves the RonSQL templates and the MySQL twins so both sides see the
// identical literal text.
//
// Rules (shape_catalog.md §0): integers/decimals/doubles bare; strings
// single-quoted with ” doubling; DATE 'YYYY-MM-DD'; TIMESTAMP
// 'YYYY-MM-DD HH:MM:SS[.ffffff]' in UTC; a batch value is a list rendered
// as a comma-separated sequence, wrapped in parentheses unless the marker
// already sits inside them (RonSQL `IN (?)` vs Calcite `IN ?`).
package bind

import (
	"fmt"
	"strings"
	"time"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// Arg is one bound argument: a scalar literal or a list of literals.
type Arg struct {
	Literal string
	List    []string
	IsList  bool
}

// Scalar wraps one rendered literal.
func Scalar(lit string) Arg { return Arg{Literal: lit} }

// List wraps a rendered literal list.
func List(lits []string) Arg { return Arg{List: lits, IsList: true} }

// Int renders an integer literal.
func Int(v int64) string { return fmt.Sprintf("%d", v) }

// Str renders a string literal with ” doubling (RonSQL and MySQL agree;
// backslashes are rejected upstream by the Hopsworks filter renderer, but
// keys may carry them, so they are doubled for MySQL's default sql_mode).
func Str(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

// Timestamp renders a TIMESTAMP literal in UTC; fractional seconds are
// printed only when non-zero (up to microseconds).
func Timestamp(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() == 0 {
		return "'" + t.Format("2006-01-02 15:04:05") + "'"
	}
	return "'" + strings.TrimRight(t.Format("2006-01-02 15:04:05.000000"), "0") + "'"
}

// Date renders a DATE literal.
func Date(t time.Time) string { return "'" + t.UTC().Format("2006-01-02") + "'" }

// LiteralFor renders a value for a feature's offline type: numeric types
// bare (validated like renderRonsqlLiteral), boolean as 0/1, temporal and
// string types quoted.
func LiteralFor(offlineType string, value string) string {
	base := spec.BaseType(offlineType)
	switch {
	case spec.IsNumericType(base):
		return value
	case base == "boolean":
		switch strings.ToLower(value) {
		case "true", "1":
			return "1"
		default:
			return "0"
		}
	default:
		return Str(value)
	}
}

// Bind replaces the k-th `?` outside string literals and backticked
// identifiers with args[k].  It fails when the counts differ.
func Bind(sql string, args []Arg) (string, error) {
	var out strings.Builder
	inStr, inIdent := false, false
	n := 0
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case inStr:
			out.WriteByte(c)
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					out.WriteByte('\'')
					i++
				} else {
					inStr = false
				}
			}
		case inIdent:
			out.WriteByte(c)
			if c == '`' {
				inIdent = false
			}
		case c == '\'':
			inStr = true
			out.WriteByte(c)
		case c == '`':
			inIdent = true
			out.WriteByte(c)
		case c == '?':
			if n >= len(args) {
				return "", fmt.Errorf("bind: statement has more than %d markers", len(args))
			}
			a := args[n]
			n++
			if !a.IsList {
				out.WriteString(a.Literal)
				continue
			}
			joined := strings.Join(a.List, ", ")
			if precededByParen(out.String()) {
				out.WriteString(joined)
			} else {
				out.WriteString("(" + joined + ")")
			}
		default:
			out.WriteByte(c)
		}
	}
	if n != len(args) {
		return "", fmt.Errorf("bind: statement has %d markers, %d arguments given", n, len(args))
	}
	return out.String(), nil
}

func precededByParen(s string) bool {
	for i := len(s) - 1; i >= 0; i-- {
		switch s[i] {
		case ' ', '\n', '\t':
			continue
		case '(':
			return true
		default:
			return false
		}
	}
	return false
}

// Count returns the number of bindable markers in the statement.
func Count(sql string) int {
	n := 0
	inStr, inIdent := false, false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case inStr:
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inStr = false
				}
			}
		case inIdent:
			if c == '`' {
				inIdent = false
			}
		case c == '\'':
			inStr = true
		case c == '`':
			inIdent = true
		case c == '?':
			n++
		}
	}
	return n
}
