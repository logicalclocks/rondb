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
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
)

func TestEnumerate(t *testing.T) {
	cfg := Config{DB: "test", Scale: data.NewScale(0.01)}
	a, err := Enumerate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Enumerate(cfg)
	if len(a) != len(b) {
		t.Fatal("non-deterministic")
	}
	ids := map[string]bool{}
	shapes := map[string]int{}
	for i, c := range a {
		if ids[c.ID] {
			t.Errorf("duplicate id %s", c.ID)
		}
		ids[c.ID] = true
		shapes[c.Shape]++
		if c.ID != b[i].ID || len(c.Statements) != len(b[i].Statements) || c.Statements[0].RonSQL != b[i].Statements[0].RonSQL {
			t.Errorf("case %s differs between runs", c.ID)
		}
		for _, s := range c.Statements {
			if strings.Contains(s.RonSQL, "?") && !strings.Contains(s.RonSQL, "'") {
				t.Errorf("%s: unbound marker in %s", c.ID, s.RonSQL)
			}
			if strings.Contains(s.RonSQL, "golden_") {
				t.Errorf("%s: golden database leaked into %s", c.ID, s.RonSQL)
			}
		}
	}
	for _, shape := range []string{"S1", "S2", "S3", "S4", "S6", "S6b", "S7", "S8", "S8b", "S9", "S10", "EDGE"} {
		if shapes[shape] == 0 {
			t.Errorf("no cases for shape %s", shape)
		}
	}
	if len(a) < 60 {
		t.Errorf("expected a fuller matrix, got %d cases", len(a))
	}
}

func TestBoundText(t *testing.T) {
	cases, err := Enumerate(Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]Case{}
	for _, c := range cases {
		byID[c.ID] = c
	}
	if s := byID["S1-k31"].Statements[0].RonSQL; !strings.HasSuffix(s, "FROM `transactions_1` WHERE `customer_id` = 31;") || !strings.Contains(s, "MAX(GREATEST(`amount`, `fee`)) AS `amount_fee_greatest`") {
		t.Errorf("S1-k31: %s", s)
	}
	if s := byID["S2-k111-w604800s"].Statements[0].RonSQL; !strings.HasSuffix(s, "AND `event_time` >= '2026-05-25 00:00:00';") {
		t.Errorf("S2 window binding: %s", s)
	}
	if s := byID["S3-b10"].Statements[0].RonSQL; !strings.Contains(s, "WHERE `customer_id` IN (16, 17, 20, 31, 111, 1001, 13, 29, 1, 2) GROUP BY `customer_id`;") {
		t.Errorf("S3 batch binding: %s", s)
	}
	if s := byID["S7-2hop-k21"].Statements[0].RonSQL; !strings.HasPrefix(s, "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = 21 GROUP BY `region_id`) SELECT `j2`.`region_name` AS `r_region_name`") {
		t.Errorf("S7 template: %s", s)
	}
	if c := byID["S8-chains-k21"]; len(c.Statements) != 2 {
		t.Errorf("S8 must have two chain templates, got %d", len(c.Statements))
	}
	if s := byID["S8b-k16"].Statements[0].RonSQL; !strings.Contains(s, "FROM `b` LEFT JOIN `regions_1` AS `j2`") || !strings.Contains(s, "LEFT JOIN `countries_1` AS `j3`") {
		t.Errorf("S8b LEFT rewrite: %s", s)
	}
	if s := byID["S6-cte-k21"].Statements[0].RonSQL; !strings.HasPrefix(s, "WITH t AS (SELECT `customer_id`, `event_time`, `amount`, `category` FROM `transactions_1` WHERE `customer_id` = 21 ORDER BY `event_time` DESC LIMIT 5)") {
		t.Errorf("S6 CTE template: %s", s)
	}
	if m := byID["S6-cte-k21"].Statements[0].MySQL; !strings.Contains(m, "WHERE hopsworks_collect_rank <= 5") || !strings.Contains(m, "`test`.`transactions_1`") {
		t.Errorf("collect MySQL twin binding: %s", m)
	}
	if s := byID["S9-a4-EUR-w90d"].Statements[0].RonSQL; !strings.Contains(s, "WHERE `account_id` = 4 AND `currency` = 'EUR' AND `event_time` >= '2026-03-03 00:00:00';") {
		t.Errorf("S9 composite + window: %s", s)
	}
	if s := byID["S10-O_Brien"].Statements[0].RonSQL; !strings.Contains(s, "WHERE `customer_key` = 'O''Brien';") {
		t.Errorf("S10 quoting: %s", s)
	}
	for id, want := range map[string]string{"S1-avg-k31": CanonNumeric, "S1-decimal-k31": CanonNumeric,
		"EDGE-null-decimal": CanonNumeric, "EDGE-null-avg": CanonNumeric, "S1-k31": "", "S3-b10": "", "S7-2hop-k21": ""} {
		if got := byID[id].Canon; got != want {
			t.Errorf("%s: Canon = %q, want %q", id, got, want)
		}
	}
	if ke := byID["EDGE-date-range"].KnownError; ke == nil || ke.Finding != "F9" || byID["EDGE-ts0-batch"].KnownError == nil || byID["EDGE-big-safe"].KnownError != nil {
		t.Error("F9 known-error marks: temporal MIN/MAX cases only")
	}
	if byID["S6-cte-k21"].ExpectReject == nil || byID["EDGE-F1-string-reuse"].Hazard != "F1" {
		t.Error("expectation table wiring")
	}
}
