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

package emit

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

// These source-derived regressions supplement, not replace, the unchanged
// Java-generated corpus. A fresh decode keeps mutations local to each test.
func reviewView(t *testing.T, name string) spec.View {
	t.Helper()
	fixtures := loadFixtures(t)
	f, ok := fixtures[name]
	if !ok {
		t.Fatalf("missing baseline fixture %s", name)
	}
	return f.View
}

func TestBuildEmptySelections(t *testing.T) {
	for _, helpers := range []bool{false, true} {
		v := reviewView(t, "snowflake_inner")
		v.Joins = v.Joins[:1] // root contains a label, but no inference helpers
		v.Options.InferenceHelpers = helpers
		statements, err := Build(&v)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(statements)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != "[]" {
			t.Fatalf("empty statement set = %s; want []", raw)
		}
	}
}

func TestNestedSourceRecovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		index     int
		aggregate spec.AggSpec
		collect   bool
		selected  []spec.TDFeature
		want      []string
	}{
		{"aggregate-root", 1, spec.AggSpec{{Key: "*", Fns: []string{"count"}},
			{Key: "tier", Fns: []string{"count"}}}, false,
			[]spec.TDFeature{{Name: "count", Type: strp("bigint")}, {Name: "tier_count", Type: strp("bigint")}},
			[]string{"fg1.region_name", "fg2.country_name", "fg0.entity_id", "fg0.tier"}},
		{"aggregate-child-dedup", 2, spec.AggSpec{{Key: "country_id", Fns: []string{"sum", "max"}},
			{Key: "country_id,region_id", Fns: []string{"greatest"}}}, false,
			[]spec.TDFeature{{Name: "country_id_sum", Type: strp("bigint")},
				{Name: "country_id_max", Type: strp("int")},
				{Name: "country_id_region_id_greatest", Type: strp("int")}},
			[]string{"fg1.country_id", "fg1.region_id", "fg2.country_name", "fg0.tier"}},
		{"collect-child-order", 2, nil, true,
			[]spec.TDFeature{{Name: "regions_collect", Type: strp("array<struct<region_id:int,country_id:int,region_name:string>>")}},
			[]string{"fg1.region_id", "fg1.country_id", "fg1.region_name", "fg2.country_name", "fg0.tier"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := reviewView(t, "snowflake_inner")
			v.Joins[tc.index].Aggregate = tc.aggregate
			v.Joins[tc.index].Features = tc.selected
			if tc.collect {
				n := 5
				v.Joins[tc.index].CollectN = &n
				v.Joins[tc.index].OrderBy = strp("region_id")
			}
			if err := v.Normalize(); err != nil {
				t.Fatal(err)
			}
			b := &builder{v: &v, fgs: v.FGMap(), opt: v.Options}
			root := v.Joins[1]
			keys, err := b.validateExtractPrimaryFeatures(root)
			if err != nil {
				t.Fatal(err)
			}
			children, err := constructTree(root, v.JoinsSorted())
			if err != nil {
				t.Fatal(err)
			}
			q, err := b.nestedQueryOf(root, children, keys)
			if err != nil {
				t.Fatal(err)
			}
			got := make([]string, 0, len(q.features))
			for _, f := range q.features {
				if f == nil {
					t.Fatal("unresolved synthetic selection")
				}
				got = append(got, f.Alias+"."+f.Name)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sources %v; want %v", got, tc.want)
			}
			if _, err := Build(&v); err != nil {
				t.Fatalf("full serving path: %v", err)
			}
		})
	}
	t.Run("self-join-dedup-is-local", func(t *testing.T) {
		v := reviewView(t, "snowflake_inner")
		v.Joins[2].Aggregate = spec.AggSpec{{Key: "country_id", Fns: []string{"sum", "max"}}}
		v.Joins[2].Features = []spec.TDFeature{{Name: "country_id_sum", Type: strp("bigint")},
			{Name: "country_id_max", Type: strp("int")}}
		second := v.Joins[2]
		second.Index, second.Prefix = 4, strp("r2_")
		second.Features = append([]spec.TDFeature(nil), second.Features...)
		v.Joins = append(v.Joins, second)
		statements, err := Build(&v)
		if err != nil {
			t.Fatal(err)
		}
		sql := *statements[0].QueryOnline
		for _, fragment := range []string{"`fg1`.`country_id` AS `r_country_id`",
			"`fg3`.`country_id` AS `r2_country_id`"} {
			if strings.Count(sql, fragment) != 1 {
				t.Fatalf("expected one %s in %s", fragment, sql)
			}
		}
	})
	t.Run("missing-source-is-named-error", func(t *testing.T) {
		v := reviewView(t, "snowflake_inner")
		v.Joins[2].CollectN = new(int)
		v.Joins[2].Features = []spec.TDFeature{{Name: "regions_collect", Type: strp("array<struct<absent:int>>")}}
		_, err := Build(&v)
		gate, ok := err.(*spec.GateError)
		if !ok || gate.Code != spec.CodeFeatureDoesNotExist ||
			gate.Message != "Feature: absent not found in feature group: regions" {
			t.Fatalf("missing source returned %v", err)
		}
	})
}

func TestBuildDefaultExpressions(t *testing.T) {
	t.Run("string-point-projection", func(t *testing.T) {
		v := reviewView(t, "snowflake_inner")
		v.Joins = v.Joins[:1]
		v.Joins[0].Features[0].Label = false
		v.FGs[0].Features[3].DefaultValue = strp("O'Brien")
		got, err := Build(&v)
		if err != nil {
			t.Fatal(err)
		}
		want := "SELECT CASE WHEN `fg0`.`tier` IS NULL THEN 'O''Brien' ELSE `fg0`.`tier` END AS `tier`" +
			"\nFROM `golden_1_fs`.`profiles_1` AS `fg0`\nWHERE `fg0`.`entity_id` = ?"
		if len(got) != 1 || got[0].QueryOnline == nil || *got[0].QueryOnline != want {
			t.Fatalf("point statement = %+v; want %s", got, want)
		}
	})
	t.Run("collect-default-projection-and-filter", func(t *testing.T) {
		v := reviewView(t, "collect_desc")
		v.FGs[0].Features[2].DefaultValue = strp("7") // amount
		condition := spec.CondGreaterThanOrEqual
		v.Filters = []spec.Filter{{FG: 1, Logic: spec.LogicSingle,
			Feature: "amount", Condition: &condition, Value: strp("3")}}
		got, err := Build(&v)
		if err != nil {
			t.Fatal(err)
		}
		expr := "CASE WHEN `fg0`.`amount` IS NULL THEN 7 ELSE `fg0`.`amount` END"
		for _, sql := range []*string{got[0].QueryOnline, got[0].QueryOnlineScan} {
			if sql == nil || !strings.Contains(*sql, expr+" AS `f_amount`") ||
				!strings.Contains(*sql, expr+" >= 3") {
				t.Fatalf("missing default expression: %v", sql)
			}
		}
		if strings.Contains(*got[0].QueryRonsql, "CASE") ||
			!strings.Contains(*got[0].QueryRonsql, "AND `amount` >= 3") {
			t.Fatalf("RonSQL must retain Java's unwrapped filter: %s", *got[0].QueryRonsql)
		}
	})
	t.Run("bind-key-default", func(t *testing.T) {
		v := reviewView(t, "collect_desc")
		v.FGs[0].Features[0].DefaultValue = strp("0")
		got, err := Build(&v)
		if err != nil {
			t.Fatal(err)
		}
		want := "WHERE CASE WHEN `fg0`.`entity_id` IS NULL THEN 0 ELSE `fg0`.`entity_id` END = ?"
		if !strings.Contains(*got[0].QueryOnline, want) {
			t.Fatal(*got[0].QueryOnline)
		}
	})
}

func TestBuildCollectTemporalFilters(t *testing.T) {
	for _, tc := range []struct{ typ, value, want string }{
		{"timestamp", "2026-09-09 12:00:00.123456", "TIMESTAMP '2026-09-09 12:00:00.123'"},
		{"timestamp", "2026-09-09 12:00:00", "TIMESTAMP '2026-09-09 12:00:00.000'"},
		{"date", "2026-09-09", "DATE '2026-09-09'"},
	} {
		t.Run(tc.typ+"/"+tc.value, func(t *testing.T) {
			v := reviewView(t, "collect_desc")
			v.FGs[0].Features = append(v.FGs[0].Features, spec.Feature{Name: "bound", Type: tc.typ})
			condition := spec.CondGreaterThanOrEqual
			v.Filters = []spec.Filter{{FG: 1, Logic: spec.LogicSingle,
				Feature: "bound", Condition: &condition, Value: strp(tc.value)}}
			got, err := Build(&v)
			if err != nil {
				t.Fatal(err)
			}
			for _, sql := range []*string{got[0].QueryOnline, got[0].QueryOnlineScan} {
				if sql == nil || !strings.Contains(*sql, "AND `fg0`.`bound` >= "+tc.want) {
					t.Fatalf("missing Calcite temporal literal: %v", sql)
				}
			}
			if !strings.Contains(*got[0].QueryRonsql, "AND `bound` >= '"+tc.value+"'") {
				t.Fatalf("RonSQL literal changed: %s", *got[0].QueryRonsql)
			}
		})
	}
}
