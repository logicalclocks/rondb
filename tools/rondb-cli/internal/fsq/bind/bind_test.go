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

package bind

import (
	"testing"
	"time"
)

func TestBindScalarsAndLists(t *testing.T) {
	got, err := Bind("SELECT COUNT(*) FROM `t` WHERE `k` = ? AND `s` = 'a?b' AND `ts` >= ?;",
		[]Arg{Scalar("31"), Scalar(Timestamp(time.Date(2026, 5, 25, 0, 0, 0, 0, time.UTC)))})
	if err != nil || got != "SELECT COUNT(*) FROM `t` WHERE `k` = 31 AND `s` = 'a?b' AND `ts` >= '2026-05-25 00:00:00';" {
		t.Errorf("got %q err %v", got, err)
	}
	got, err = Bind("WHERE `k` IN (?) GROUP BY `k`", []Arg{List([]string{"1", "2"})})
	if err != nil || got != "WHERE `k` IN (1, 2) GROUP BY `k`" {
		t.Errorf("RonSQL list: %q %v", got, err)
	}
	got, err = Bind("WHERE `fg0`.`k` IN ?", []Arg{List([]string{"1", "2"})})
	if err != nil || got != "WHERE `fg0`.`k` IN (1, 2)" {
		t.Errorf("Calcite list: %q %v", got, err)
	}
	if _, err := Bind("a = ? AND b = ?", []Arg{Scalar("1")}); err == nil {
		t.Error("marker/argument count mismatch must fail")
	}
	if Count("x = ? AND y = 'q?' AND `z?` = ?") != 2 {
		t.Error("Count must skip markers inside literals and identifiers")
	}
}

func TestLiterals(t *testing.T) {
	if Str("O'Brien") != "'O''Brien'" || Str(`a\b`) != `'a\\b'` {
		t.Error("Str")
	}
	if Timestamp(time.Date(2026, 5, 1, 12, 0, 0, 1000, time.UTC)) != "'2026-05-01 12:00:00.000001'" {
		t.Error("Timestamp with fraction")
	}
	if LiteralFor("decimal(12,2)", "10.50") != "10.50" || LiteralFor("string", "x") != "'x'" || LiteralFor("boolean", "true") != "1" {
		t.Error("LiteralFor")
	}
}
