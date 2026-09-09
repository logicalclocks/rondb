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

package exec

import (
	"net/http"
	"testing"
)

func TestParseJSONData(t *testing.T) {
	body := []byte("{\"data\":\n[{\"c\":null,\"max(sint16)\":32566,\"d\":1.50,\"s\":\"O'Brien\"}\n,{\"c\":\"x\",\"max(sint16)\":-1,\"d\":0.5000000000,\"s\":\"\"}\n]\n}")
	cols, rows, err := ParseJSONData(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 4 || cols[1] != "max(sint16)" || len(rows) != 2 {
		t.Fatalf("cols %v rows %d", cols, len(rows))
	}
	if !rows[0][0].Null || rows[0][2].Text != "1.50" || rows[1][2].Text != "0.5000000000" || rows[1][3].Text != "" || rows[1][3].Null {
		t.Errorf("cells: %+v", rows)
	}
	if _, _, err := ParseJSONData([]byte(`[{"a":1,"a":2}]`)); err == nil {
		t.Error("duplicate output names must be rejected")
	}
	cols, rows, err = ParseJSONData([]byte("{\"data\":\n[]\n}"))
	if err != nil || cols != nil || len(rows) != 0 {
		t.Errorf("empty data: cols %v rows %v err %v", cols, rows, err)
	}
	if _, _, err := ParseJSONData([]byte(`[{"a":1}]`)); err != nil {
		t.Errorf("bare array (ronsql_cli): %v", err)
	}
	f9 := []byte(`{"data":[{"cnt":3,"d_min":1970-01-01}]}`)
	if _, _, err := ParseJSONData(f9); err == nil {
		t.Error("unquoted temporal aggregate (F9) must be a syntax error")
	} else if d := jsonErrDetail(f9, err); len(d) <= len(err.Error()) || d[len(d)-1] != '"' {
		t.Errorf("detail must quote the offending text: %s", d)
	}
}

func TestClassify(t *testing.T) {
	if o, _ := Classify(http.StatusOK, ""); o != OK {
		t.Error("200")
	}
	if o, _ := Classify(500, "CTE 't' ...\nCaught exception: Non-aggregating CTE body is not a single-row key lookup.\nError handling: RPE"); o != CleanReject {
		t.Error("permanent")
	}
	if o, _ := Classify(500, "Caught RonSQLRetryableError after 10 attempts: x"); o != Retryable {
		t.Error("retryable")
	}
	if o, _ := Classify(400, "bad database"); o != Error {
		t.Error("400")
	}
	if o, _ := Classify(429, "rate limited"); o != Retryable {
		t.Error("429")
	}
	if got := ParsePhases("parse=12,analyze=3,rows=5,attempts=1"); got["rows"] != 5 || got["parse"] != 12 {
		t.Errorf("phases %v", got)
	}
	if h := ParseTextHeader("a\tb\n1\t2\n"); len(h) != 2 || h[1] != "b" {
		t.Errorf("header %v", h)
	}
}
