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
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/vector"
)

func specsByID(t *testing.T) map[string]*Spec {
	t.Helper()
	specs, err := Specs(Config{DB: "test", Scale: data.NewScale(0.01)})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*Spec{}
	for i := range specs {
		out[specs[i].ID] = &specs[i]
	}
	return out
}

func TestSpecsCatalog(t *testing.T) {
	specs := specsByID(t)
	if len(specs) < 20 {
		t.Fatalf("catalog has %d specs", len(specs))
	}
	shapes := map[string]bool{}
	for id, s := range specs {
		shapes[s.Shape] = true
		if len(s.DTOs()) == 0 || len(s.Params) == 0 {
			t.Errorf("%s: no DTOs or parameters", id)
		}
		if s.Family != "" && len(s.ExpectedFeatures()) == 0 {
			t.Errorf("%s: family %s without expected features", id, s.Family)
		}
	}
	for _, sh := range []string{"S1", "S2", "S3", "S4", "S6", "S6b", "S7", "S8", "S9", "S10"} {
		if !shapes[sh] {
			t.Errorf("shape %s has no spec", sh)
		}
	}
}

func TestSampleKeysClasses(t *testing.T) {
	specs := specsByID(t)
	sc := data.NewScale(0.01)
	keys := specs["V-S1-agg"].SampleKeys(1, 120)
	if len(keys) < 120 {
		t.Fatalf("%d keys", len(keys))
	}
	seen := map[string]bool{}
	for _, k := range keys {
		if seen[k.Text()] {
			t.Errorf("duplicate key %v", k)
		}
		seen[k.Text()] = true
	}
	for _, want := range []string{"16", "17", "13", "29", "1001"} {
		if !seen[want] {
			t.Errorf("key class representative %s missing", want)
		}
	}
	if k2 := specs["V-S1-agg"].SampleKeys(1, 120); k2[len(k2)-1].Text() != keys[len(keys)-1].Text() {
		t.Error("sampling must be deterministic for a seed")
	}
	if len(specs["V-S3-agg-b10"].Units(keys)) != (len(keys)+9)/10 {
		t.Error("batch units of 10")
	}
	hk := specs["V-S9-hist"].SampleKeys(1, 120)
	if len(hk) < 120 || len(hk[0]) != 2 || hk[0][1] != "USD" {
		t.Errorf("hist keys: %d, first %v", len(hk), hk[0])
	}
	sk := specs["V-S10-str"].SampleKeys(1, 120)
	if !strings.HasPrefix(sk[0][0], "cust-") && !strings.HasPrefix(sk[0][0], "CUST-") {
		t.Errorf("string keys: %v", sk[0])
	}
	if sc.StrCustomers != 100 {
		t.Errorf("StrCustomers at sf 0.01 = %d", sc.StrCustomers)
	}
}

func TestSpecBindAndPlan(t *testing.T) {
	specs := specsByID(t)
	groups, err := specs["V-S1-agg"].Bind([]vector.Key{{"31"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].MySQL == nil || len(groups[0].Statements) != 1 ||
		!strings.Contains(groups[0].Statements[0].RonSQL, "`customer_id` = 31") {
		t.Fatalf("bound S1: %+v", groups)
	}
	p := specs["V-S3-agg-b10"].Plan(groups[0].DTO)
	if p.Kind != vector.Aggregate || !p.Batch || !p.CountOutputs["tx_count"] || p.CountOutputs["tx_amount_sum"] {
		t.Errorf("S3 plan: %+v", p)
	}
	bg, err := specs["V-S3-agg-b10"].Bind([]vector.Key{{"31"}, {"16"}})
	if err != nil || !strings.Contains(bg[0].Statements[0].RonSQL, "IN (31, 16)") || !strings.Contains(*bg[0].MySQL, "IN (31, 16)") {
		t.Errorf("batch bind: %v %+v", err, bg)
	}
	cg, err := specs["V-S6b-direct-n5"].Bind([]vector.Key{{"21"}})
	if err != nil {
		t.Fatal(err)
	}
	direct := cg[0].Statements[0].RonSQL
	if !strings.HasPrefix(direct, "SELECT ") || strings.Contains(direct, "WITH") || !strings.HasSuffix(direct, "ORDER BY `event_time` DESC LIMIT 5;") {
		t.Errorf("direct collect: %s", direct)
	}
	cp := specs["V-S6b-direct-n5"].Plan(cg[0].DTO)
	if cp.Kind != vector.Collect || cp.CollectFeature != "tx_transactions_collect" || strings.Join(cp.Fields, ",") != "event_time,amount,category" {
		t.Errorf("collect plan: %+v", cp)
	}
	if g, err := specs["V-S6-cte-n5"].Bind([]vector.Key{{"21"}}); err != nil || !strings.HasPrefix(g[0].Statements[0].RonSQL, "WITH t AS") {
		t.Errorf("CTE form must stay for the S6 spec: %v", err)
	}
	sg, err := specs["V-S8-left-2hop"].Bind([]vector.Key{{"16"}})
	if err != nil || len(sg[0].Statements) != 2 || sg[0].MySQL == nil || !strings.Contains(*sg[0].MySQL, "LEFT JOIN") {
		t.Errorf("LEFT chains: %v %+v", err, sg)
	}
	if hg, err := specs["V-S9-hist"].Bind([]vector.Key{{"4", "EUR"}}); err != nil || !strings.Contains(hg[0].Statements[0].RonSQL, "`currency` = 'EUR'") {
		t.Errorf("composite bind: %v", err)
	}
	if tg, err := specs["V-S10-str"].Bind([]vector.Key{{"O'Brien"}}); err != nil || !strings.Contains(tg[0].Statements[0].RonSQL, "'O''Brien'") {
		t.Errorf("string bind: %v", err)
	}
}

func TestExpectedVectors(t *testing.T) {
	specs := specsByID(t)
	sc := data.NewScale(0.01)
	v, ok := specs["V-S1-agg"].Expected(vector.Key{"31"})
	if !ok || v["tx_count"].Text != "300" || v["tx_amount_count"].Text != "300" || v["tx_amount_max"].Text != "998" {
		t.Errorf("S1 k31: %+v", v)
	}
	if v, _ := specs["V-S1-agg"].Expected(vector.Key{"16"}); v["tx_count"].Text != "0" || !v["tx_amount_sum"].Null {
		t.Errorf("S1 empty: %+v", v)
	}
	if v, _ := specs["V-S1-agg"].Expected(vector.Key{"1001"}); v["tx_count"].Text != "0" {
		t.Errorf("S1 missing: %+v", v)
	}
	if v, _ := specs["V-S2-agg-w7d"].Expected(vector.Key{"111"}); v["tx_count"].Text != "7" {
		t.Errorf("S2 7-day window at exact day bounds: %+v", v)
	}
	if v, _ := specs["V-S4-eq-grocery"].Expected(vector.Key{"31"}); v["tx_count"].Text != "100" {
		t.Errorf("S4 collation filter (grocery + Grocery of 300 rows): %+v", v)
	}
	c, _ := specs["V-S6b-direct-n5"].Expected(vector.Key{"21"})
	arr := c["tx_transactions_collect"].Array
	if len(arr) != 5 || arr[0].Values[0].Text != data.FormatTimestamp(data.TxEventTime(21, 1)) || arr[0].Values[1].Text != "768" || arr[4].Values[1].Text != "836" {
		t.Errorf("collect k21: %s", vector.Render(c["tx_transactions_collect"], true))
	}
	a, _ := specs["V-S6b-direct-asc-n5"].Expected(vector.Key{"21"})
	if aa := a["tx_transactions_collect"].Array; aa[0].Values[0].Text != data.FormatTimestamp(data.TxEventTime(21, 5)) {
		t.Errorf("collect ascending: %s", vector.Render(a["tx_transactions_collect"], true))
	}
	if e, _ := specs["V-S6b-direct-n5"].Expected(vector.Key{"16"}); e["tx_transactions_collect"].Array == nil || len(e["tx_transactions_collect"].Array) != 0 {
		t.Errorf("collect empty: %+v", e)
	}
	if s, _ := specs["V-S7-inner-2hop"].Expected(vector.Key{"21"}); s["r_region_name"].Text != "Region 22" || s["c_country_name"].Text != "Country 22" {
		t.Errorf("snowflake k21: %+v", s)
	}
	if s, _ := specs["V-S7-inner-2hop"].Expected(vector.Key{"13"}); len(s) != 0 {
		t.Errorf("NULL region must serve nothing: %+v", s)
	}
	if s, _ := specs["V-S7-inner-2hop"].Expected(vector.Key{"16"}); len(s) != 0 {
		t.Errorf("INNER with a NULL country drops the row: %+v", s)
	}
	if s, _ := specs["V-S8-left-2hop"].Expected(vector.Key{"16"}); s["r_region_name"].Text != "Region 17" || len(s) != 2 {
		t.Errorf("LEFT with a NULL country keeps the region chain: %+v", s)
	}
	if s, _ := specs["V-S7-inner-1hop"].Expected(vector.Key{"16"}); s["r_region_name"].Text != "Region 17" {
		t.Errorf("1-hop: %+v", s)
	}
	if h, _ := specs["V-S9-hist"].Expected(vector.Key{"4", "EUR"}); h["b_count"].Text != "10" {
		t.Errorf("hist a4 EUR: %+v", h)
	}
	if h, _ := specs["V-S9-hist"].Expected(vector.Key{"5", "SEK"}); h["b_count"].Text != "0" || !h["b_delta_sum"].Null {
		t.Errorf("hist a5: %+v", h)
	}
	if h, _ := specs["V-S9-hist"].Expected(vector.Key{"4", "GBP"}); h["b_count"].Text != "0" {
		t.Errorf("hist unknown currency: %+v", h)
	}
	if s, _ := specs["V-S10-str"].Expected(vector.Key{"cust-00000010"}); s["s_count"].Text != "20" {
		t.Errorf("string key lower-case form of CUST-00000010: %+v", s)
	}
	if s, _ := specs["V-S10-str"].Expected(vector.Key{data.CustomerKey(sc.StrCustomers + 1)}); s["s_count"].Text != "0" {
		t.Errorf("string key beyond StrCustomers: %+v", s)
	}
	if s, _ := specs["V-S10-str"].Expected(vector.Key{"O'Brien"}); s["s_count"].Text != "0" {
		t.Errorf("string key O'Brien: %+v", s)
	}
}
