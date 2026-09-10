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

package vector

import (
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/exec"
)

func row(vals ...string) []exec.Cell {
	out := make([]exec.Cell, len(vals))
	for i, v := range vals {
		if v == "\x00" {
			out[i] = exec.Cell{Null: true}
		} else {
			out[i] = exec.Cell{Text: v}
		}
	}
	return out
}

func res(cols []string, types []string, rows ...[]exec.Cell) *exec.Result {
	return &exec.Result{Columns: cols, Types: types, Rows: rows}
}

func strp(s string) *string { return &s }
func boolp(b bool) *bool    { return &b }
func intp(i int) *int       { return &i }

func aggDTO(prefix string, batch bool) emit.Statement {
	return emit.Statement{
		Prefix:                      strp(prefix),
		AggregateFeatureNames:       []string{prefix + "count", prefix + "amount_count", prefix + "amount_sum"},
		PreparedStatementParameters: []emit.Param{{Name: "customer_id", Index: 1}},
	}
}

func TestPlanFor(t *testing.T) {
	p := PlanFor(aggDTO("tx_", true), true, nil)
	if p.Kind != Aggregate || !p.Batch || p.Prefix != "tx_" || len(p.Params) != 1 {
		t.Fatalf("plan %+v", p)
	}
	if !p.CountOutputs["tx_count"] || !p.CountOutputs["tx_amount_count"] || p.CountOutputs["tx_amount_sum"] {
		t.Errorf("count outputs %v", p.CountOutputs)
	}
	c := PlanFor(emit.Statement{Prefix: strp("f_"), CollectN: intp(5), CollectFeatureName: strp("events_collect"),
		CollectOrderBy: strp("event_time"), CollectAscending: boolp(true)}, false, []string{"event_time", "amount"})
	if c.Kind != Collect || c.CollectFeature != "f_events_collect" || !c.Ascending || len(c.Fields) != 2 {
		t.Errorf("collect plan %+v", c)
	}
	if s := PlanFor(emit.Statement{SnowflakeTemplates: []string{"WITH `b` AS (SELECT `k`, COUNT(*) AS `hw_cnt` FROM `r` WHERE `id` IN (?) GROUP BY `k`) SELECT `j2`.`x` AS `p_x`, `b`.`id` AS `id` FROM `b` JOIN `c` AS `j2` ON `j2`.`k` = `b`.`k`;"},
		PreparedStatementParameters: []emit.Param{{Name: "id", Index: 1}}}, true, nil); s.Kind != Snowflake || strings.Join(s.Outputs, ",") != "p_x" {
		t.Errorf("snowflake plan %+v", s)
	}
	if s := PlanFor(emit.Statement{}, false, nil); s.Kind != PointRead {
		t.Errorf("point-read plan %+v", s)
	}
}

func TestAggregateSingleAndBatch(t *testing.T) {
	keys := []Key{{"31"}, {"16"}}
	p := PlanFor(aggDTO("tx_", true), true, nil)
	ron, err := FoldRonsql(p, []*exec.Result{res([]string{"customer_id", "count", "amount_count", "amount_sum"}, nil,
		row("31", "300", "300", "163350"))}, keys)
	if err != nil {
		t.Fatal(err)
	}
	my, err := FoldMysql(p, res([]string{"tx_customer_id", "tx_count", "tx_amount_count", "tx_amount_sum"},
		[]string{"BIGINT", "BIGINT", "BIGINT", "DECIMAL"}, row("31", "300", "300", "163350")), keys)
	if err != nil {
		t.Fatal(err)
	}
	if v := ron["16"]; v["tx_count"].Text != "0" || !v["tx_amount_sum"].Null {
		t.Errorf("batch defaults: %+v", v)
	}
	rep := Compare(p, my, ron, keys, map[string]string{"tx_amount_sum": "DECIMAL"}, Policy{MissingEqualsNull: true}, nil)
	if len(rep.Mismatches) != 0 || rep.Compared != 6 {
		t.Errorf("batch compare: %+v", rep)
	}
	// A GROUP BY row present on one side only is a mismatch, not a default.
	my2, _ := FoldMysql(p, res([]string{"tx_customer_id", "tx_count", "tx_amount_count", "tx_amount_sum"}, nil,
		row("31", "300", "300", "163350"), row("16", "1", "1", "7")), keys)
	if rep := Compare(p, my2, ron, keys, nil, Policy{MissingEqualsNull: true}, nil); len(rep.Mismatches) != 3 {
		t.Errorf("one-sided row must mismatch every output: %+v", rep.Mismatches)
	}
	// Single: prefix applied to the RonSQL outputs.
	ps := PlanFor(aggDTO("tx_", false), false, nil)
	ron1, _ := FoldRonsql(ps, []*exec.Result{res([]string{"count", "amount_count", "amount_sum"}, nil, row("0", "0", "\x00"))}, keys[:1])
	my1, _ := FoldMysql(ps, res([]string{"tx_count", "tx_amount_count", "tx_amount_sum"}, nil, row("0", "0", "\x00")), keys[:1])
	if rep := Compare(ps, my1, ron1, keys[:1], nil, Policy{}, nil); len(rep.Mismatches) != 0 || rep.Compared != 3 {
		t.Errorf("single compare: %+v", rep)
	}
	if _, err := FoldMysql(p, res([]string{"tx_count"}, nil, row("1")), keys); err == nil {
		t.Error("a batch result without the key column must be an error")
	}
}

func TestCollectFold(t *testing.T) {
	dto := emit.Statement{Prefix: strp("tx_"), CollectN: intp(5), CollectFeatureName: strp("transactions_collect"),
		CollectOrderBy: strp("event_time"), CollectAscending: boolp(false)}
	p := PlanFor(dto, false, []string{"event_time", "amount", "category"})
	keys := []Key{{"21"}}
	ron, err := FoldRonsql(p, []*exec.Result{res([]string{"customer_id", "event_time", "amount", "category"}, nil,
		row("21", "2026-05-31 06:00:00", "802", "grocery"),
		row("21", "2026-05-31 18:00:00", "768", "Travel"),
		row("21", "2026-05-31 12:00:00", "785", "online"))}, keys)
	if err != nil {
		t.Fatal(err)
	}
	my, err := FoldMysql(p, res([]string{"tx_customer_id", "tx_event_time", "tx_amount", "tx_category", RankColumn},
		[]string{"BIGINT", "TIMESTAMP", "BIGINT", "VARCHAR", "BIGINT"},
		row("21", "2026-05-31 18:00:00.000", "768", "Travel", "1"),
		row("21", "2026-05-31 12:00:00.000", "785", "online", "2"),
		row("21", "2026-05-31 06:00:00.000", "802", "grocery", "3")), keys)
	if err != nil {
		t.Fatal(err)
	}
	arr := ron["21"]["tx_transactions_collect"].Array
	if len(arr) != 3 || arr[0].Values[0].Text != "2026-05-31 18:00:00" || arr[2].Values[2].Text != "grocery" {
		t.Errorf("desc fold: %s", Render(ron["21"]["tx_transactions_collect"], true))
	}
	types := map[string]string{"tx_event_time": "TIMESTAMP", "tx_amount": "BIGINT", "tx_category": "VARCHAR"}
	if rep := Compare(p, my, ron, keys, types, Policy{}, nil); len(rep.Mismatches) != 0 || rep.Compared != 1 {
		t.Errorf("collect compare: %+v", rep)
	}
	// Ascending: oldest first on both sides.
	pa := p
	pa.Ascending = true
	rona, _ := FoldRonsql(pa, []*exec.Result{res([]string{"customer_id", "event_time", "amount", "category"}, nil,
		row("21", "2026-05-31 18:00:00", "768", "Travel"), row("21", "2026-05-31 06:00:00", "802", "grocery"))}, keys)
	if a := rona["21"]["tx_transactions_collect"].Array; a[0].Values[1].Text != "802" {
		t.Errorf("asc fold: %s", Render(rona["21"]["tx_transactions_collect"], true))
	}
	// Empty arrays are equal and not missing; a differing element is a mismatch.
	rone, _ := FoldRonsql(p, []*exec.Result{res(nil, nil)}, keys)
	mye, _ := FoldMysql(p, res(nil, nil), keys)
	if rep := Compare(p, mye, rone, keys, types, Policy{}, nil); len(rep.Mismatches) != 0 {
		t.Errorf("empty arrays: %+v", rep)
	}
	if rep := Compare(p, my, rone, keys, types, Policy{}, nil); len(rep.Mismatches) != 1 {
		t.Errorf("length mismatch expected: %+v", rep)
	}
	ronx, _ := FoldRonsql(p, []*exec.Result{res([]string{"customer_id", "event_time", "amount", "category"}, nil,
		row("21", "2026-05-31 06:00:00", "802", "grocery"), row("21", "2026-05-31 18:00:00", "769", "Travel"),
		row("21", "2026-05-31 12:00:00", "785", "online"))}, keys)
	if rep := Compare(p, my, ronx, keys, types, Policy{}, nil); len(rep.Mismatches) != 1 {
		t.Errorf("element mismatch expected: %+v", rep)
	}
	// Numeric order columns sort numerically.
	pn := PlanFor(emit.Statement{Prefix: strp(""), CollectN: intp(3), CollectFeatureName: strp("seq_collect"),
		CollectOrderBy: strp("sequence_no")}, false, []string{"sequence_no"})
	ronn, _ := FoldRonsql(pn, []*exec.Result{res([]string{"sequence_no"}, nil, row("9"), row("10"), row("2"))}, keys)
	if a := ronn["21"]["seq_collect"].Array; a[0].Values[0].Text != "10" || a[2].Values[0].Text != "2" {
		t.Errorf("numeric order: %s", Render(ronn["21"]["seq_collect"], true))
	}
}

func TestSnowflakeOverlayAndPolicy(t *testing.T) {
	t1 := "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = ? GROUP BY `region_id`) " +
		"SELECT `j2`.`region_name` AS `r_region_name`, `j2`.`population` AS `r_population` FROM `b` JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id`;"
	t2 := "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = ? GROUP BY `region_id`) " +
		"SELECT `j3`.`country_name` AS `c_country_name`, `j3`.`continent` AS `c_continent` FROM `b` JOIN `regions_1` AS `j2` ON `j2`.`region_id` = `b`.`region_id` JOIN `countries_1` AS `j3` ON `j3`.`country_id` = `j2`.`country_id`;"
	dto := emit.Statement{Prefix: strp("p_"), SnowflakeTemplates: []string{t1, t2},
		PreparedStatementParameters: []emit.Param{{Name: "customer_id", Index: 1}}}
	p := PlanFor(dto, false, nil)
	if got := strings.Join(p.Outputs, ","); got != "r_region_name,r_population,c_country_name,c_continent" {
		t.Fatalf("served aliases: %s", got)
	}
	keys := []Key{{"16"}}
	// Chain 1 hit, chain 2 (country) miss: RonSQL leaves c_* missing.
	ron, _ := FoldRonsql(p, []*exec.Result{
		res([]string{"r_region_name", "r_population"}, nil, row("Region 17", "209865")),
		res(nil, nil)}, keys)
	myRes := res([]string{"r_region_name", "r_population", "c_country_name", "c_continent", "p_tier"},
		[]string{"VARCHAR", "BIGINT", "VARCHAR", "VARCHAR", "VARCHAR"},
		row("Region 17", "209865", "\x00", "\x00", "gold"))
	my, _ := FoldMysql(p, myRes, keys)
	if ty := Types(myRes); ty["r_population"] != "BIGINT" || len(ty) != 5 {
		t.Errorf("types: %v", ty)
	}
	rep := Compare(p, my, ron, keys, Types(myRes), Policy{MissingEqualsNull: true}, nil)
	if len(rep.Mismatches) != 0 || rep.LeftMiss != 2 || rep.Compared != 4 {
		t.Errorf("LEFT miss policy: %+v", rep)
	}
	if len(rep.NotServed) != 1 || rep.NotServed[0] != "p_tier" {
		t.Errorf("root feature must be reported as not served: %v", rep.NotServed)
	}
	if rep := Compare(p, my, ron, keys, nil, Policy{}, nil); len(rep.Mismatches) != 2 {
		t.Errorf("policy off: %+v", rep.Mismatches)
	}
	// INNER: both sides drop the row entirely.
	roni, _ := FoldRonsql(p, []*exec.Result{res(nil, nil)}, keys)
	myi, _ := FoldMysql(p, res([]string{"r_region_name"}, nil), keys)
	// Every served alias is compared and missing on both sides.
	if rep := Compare(p, myi, roni, keys, nil, Policy{}, nil); len(rep.Mismatches) != 0 || rep.Compared != 4 {
		t.Errorf("both missing: %+v", rep)
	}
	// Batch: rows keyed by the appended root key on RonSQL, the prefixed key on MySQL.
	pb := PlanFor(dto, true, nil)
	bkeys := []Key{{"21"}, {"13"}}
	ronb, err := FoldRonsql(pb, []*exec.Result{res([]string{"r_region_name", "customer_id"}, nil, row("Region 22", "21"))}, bkeys)
	if err != nil {
		t.Fatal(err)
	}
	myb, err := FoldMysql(pb, res([]string{"r_region_name", "p_customer_id"}, nil, row("Region 22", "21")), bkeys)
	if err != nil {
		t.Fatal(err)
	}
	// Four served aliases for each of the two keys; key 13 has no row on either side.
	if rep := Compare(pb, myb, ronb, bkeys, nil, Policy{}, nil); len(rep.Mismatches) != 0 || rep.Compared != 8 {
		t.Errorf("batch overlay: %+v", rep)
	}
	if _, ok := ronb["13"]; ok {
		t.Error("an entity without rows must have no vector")
	}
	// `only` restricts the compared set (data-model expectations).
	exp := Vectors{"16": {"r_region_name": {Text: "Region 17"}}}
	if rep := Compare(p, exp, my, keys, nil, Policy{MissingEqualsNull: true}, map[string]bool{"r_region_name": true}); len(rep.Mismatches) != 0 || rep.Compared != 1 {
		t.Errorf("only: %+v", rep)
	}
}
