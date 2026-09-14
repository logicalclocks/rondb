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

package mysqltwin

import (
	"strings"
	"testing"
)

func TestLiteral(t *testing.T) {
	for _, tc := range []struct {
		typ, value, want string
		bad              bool
	}{
		{"bigint", "12", "12", false},
		{"string", "O'Brien", "'O''Brien'", false},
		{"parameter", "?", "?", false},
		{"DATE", "2026-09-09", "DATE '2026-09-09'", false},
		{"timestamp", "2026-09-09 12:00:00", "TIMESTAMP '2026-09-09 12:00:00.000'", false},
		{"timestamp", "2026-09-09 12:00:00.1", "TIMESTAMP '2026-09-09 12:00:00.100'", false},
		{"timestamp", "2026-09-09 12:00:00.123456", "TIMESTAMP '2026-09-09 12:00:00.123'", false},
		{"timestamp", "2026-09-09 23:59:59.999999", "TIMESTAMP '2026-09-09 23:59:59.999'", false},
		{"date", "2026-13-09", "", true},
		{"date", "0000-01-01", "", true},
		{"date", "not a date", "", true},
		{"timestamp", "2026-09-09T12:00:00Z", "", true},
		// The pinned TimestampString(String) rejects fractional trailing
		// zeroes before SqlTimestampLiteral pads its formatted output.
		{"timestamp", "2026-09-09 12:00:00.120", "", true},
	} {
		t.Run(tc.typ+"/"+tc.value, func(t *testing.T) {
			got, err := Literal(tc.value, tc.typ)
			if (err != nil) != tc.bad || got != tc.want {
				t.Fatalf("got %q, %v; want %q, error=%t", got, err, tc.want, tc.bad)
			}
		})
	}
}

func TestDefaultExpression(t *testing.T) {
	for _, tc := range []struct{ typ, value, want string }{
		{"string", "O'Brien", "'O''Brien'"},
		{"int", "7", "7"},
		{"parameter", "0", "0"},
	} {
		value := tc.value
		c := Col{Alias: "fg0", Name: "amount", Out: "p_amount", Type: tc.typ, DefaultValue: &value}
		expr := "CASE WHEN `fg0`.`amount` IS NULL THEN " + tc.want + " ELSE `fg0`.`amount` END"
		if got := c.String(); got != expr+" AS `p_amount`" {
			t.Fatal(got)
		}
		if got := KeyWhere([]Col{c}, false); got != expr+" = ?" {
			t.Fatal(got)
		}
		if got := KeyWhere([]Col{c}, true); got != expr+" IN ?" {
			t.Fatal(got)
		}
	}
	keys := []Col{{Alias: "fg0", Name: "a"}, {Alias: "fg0", Name: "b"}}
	if got := KeyWhere(keys, false); got != "`fg0`.`a` = ? AND `fg0`.`b` = ?" {
		t.Fatal(got)
	}
	if got := KeyWhere(keys, true); got != "(`fg0`.`a`, `fg0`.`b`) IN ?" {
		t.Fatal(got)
	}
	value := "0"
	cs := []Col{{Alias: "fg0", Name: "amount", Out: "amount", Type: "int", DefaultValue: &value}}
	sql := Nested(cs, Table{DB: "fs", Name: "events_1", Alias: "fg0"}, nil, "1 = 1")
	if !strings.HasPrefix(sql, "SELECT CASE WHEN") {
		t.Fatal(sql)
	}
}
