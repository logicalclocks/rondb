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
	"flag"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/bind"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/fs_hw_registry.golden from the emitter")

func benchEntries(t *testing.T) map[string]BenchEntry {
	t.Helper()
	entries, err := BenchEntries(Config{DB: "fs_bench", Scale: data.NewScale(1)})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]BenchEntry{}
	for _, e := range entries {
		if _, dup := out[e.Name]; dup {
			t.Fatalf("duplicate entry %s", e.Name)
		}
		out[e.Name] = e
	}
	return out
}

func TestBenchEntriesCatalog(t *testing.T) {
	entries := benchEntries(t)
	if len(entries) != 25 {
		t.Fatalf("%d entries, want 25 (benchmarks.md §2)", len(entries))
	}
	for _, name := range []string{"fs_hw_floor", "fs_hw_agg_point", "fs_hw_agg_window7d", "fs_hw_agg_greatest", "fs_hw_agg_filter",
		"fs_hw_agg_batch10", "fs_hw_agg_batch100", "fs_hw_agg_batch1000", "fs_hw_agg_batch100_window", "fs_hw_collect5", "fs_hw_collect50",
		"fs_hw_collect5_cte", "fs_hw_collect5_twin", "fs_hw_snow1_point", "fs_hw_snow2_point", "fs_hw_snow1_batch100", "fs_hw_snow2_left_chain",
		"fs_hw_snow2_left_single", "fs_hw_snow1_twin", "fs_hw_snow2_twin", "fs_hw_strkey_point", "fs_hw_strkey_batch100", "fs_hw_composite_point",
		"fs_hw_hash_point", "fs_hw_sessions_window2h"} {
		if _, ok := entries[name]; !ok {
			t.Errorf("missing entry %s", name)
		}
	}
	check := func(name, want string) {
		if !strings.Contains(entries[name].SQL, want) {
			t.Errorf("%s: %q not in\n%s", name, want, entries[name].SQL)
		}
	}
	check("fs_hw_agg_point", "COUNT(`amount`) AS `amount_count`, SUM(`amount`) AS `amount_sum`, MAX(`amount`) AS `amount_max`, MIN(`fee`) AS `fee_min` FROM `transactions_1` WHERE `customer_id` = {KEY};")
	check("fs_hw_agg_window7d", "AND `event_time` >= {NOW-7d};")
	check("fs_hw_agg_greatest", "MAX(GREATEST(`amount`, `fee`)) AS `amount_fee_greatest`")
	check("fs_hw_agg_filter", "`category` = 'grocery' AND `amount` >= 300")
	check("fs_hw_agg_batch10", "WHERE `customer_id` IN ({KEYS:10}) GROUP BY `customer_id`;")
	check("fs_hw_agg_batch100_window", "IN ({KEYS:100}) AND `event_time` >= {NOW-30d} GROUP BY")
	check("fs_hw_collect5", "FROM `transactions_1` WHERE `customer_id` = {KEY} ORDER BY `event_time` DESC LIMIT 5;")
	check("fs_hw_collect50", "LIMIT 50;")
	check("fs_hw_collect5_cte", "WITH t AS (")
	check("fs_hw_collect5_twin", "ROW_NUMBER() OVER (PARTITION BY")
	check("fs_hw_collect5_twin", "`fs_bench`.`transactions_1`")
	check("fs_hw_snow1_point", "WITH `b` AS (SELECT `region_id`, COUNT(*) AS `hw_cnt` FROM `customers_1` WHERE `customer_id` = {KEY} GROUP BY `region_id`)")
	check("fs_hw_snow2_point", "JOIN `countries_1` AS `j3`")
	check("fs_hw_snow1_batch100", "IN ({KEYS:100})")
	check("fs_hw_snow1_batch100", "`b`.`customer_id` AS `customer_id`")
	check("fs_hw_snow2_left_chain", "`j3`.`country_name` AS `c_country_name`")
	check("fs_hw_snow2_left_single", " LEFT JOIN `regions_1` AS `j2`")
	check("fs_hw_snow2_left_single", " LEFT JOIN `countries_1` AS `j3`")
	check("fs_hw_snow1_twin", "INNER JOIN `fs_bench`.`regions_1`")
	check("fs_hw_strkey_point", "WHERE `customer_key` = {SKEY};")
	check("fs_hw_strkey_batch100", "IN ({SKEYS:100})")
	check("fs_hw_composite_point", "WHERE `account_id` = {ACCT} AND `currency` = {CUR} AND `event_time` >= {NOW-90d};")
	check("fs_hw_hash_point", "FROM `transactions_hash_1`")
	check("fs_hw_sessions_window2h", "FROM `sessions_1` WHERE `customer_id` = {KEY} AND `event_time` >= {NOW-2h};")
	if entries["fs_hw_collect5"].SQL == entries["fs_hw_collect5_cte"].SQL || strings.Contains(entries["fs_hw_collect5"].SQL, "WITH") {
		t.Error("collect5 must be the direct form")
	}
	for name, e := range entries {
		if e.MySQLOnly != strings.HasSuffix(name, "_twin") {
			t.Errorf("%s: MySQLOnly=%v", name, e.MySQLOnly)
		}
		resolved := ResolveBenchPlaceholders(e.SQL, rand.New(rand.NewSource(1)), 1000, data.FSNow)
		if strings.Contains(resolved, "{") {
			t.Errorf("%s: unresolved placeholder in %s", name, resolved)
		}
	}
}

func TestBenchRegistryGolden(t *testing.T) {
	entries, err := BenchEntries(Config{DB: "fs_bench", Scale: data.NewScale(1)})
	if err != nil {
		t.Fatal(err)
	}
	got := RenderBenchRegistry(entries)
	path := filepath.Join("testdata", "fs_hw_registry.golden")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden dump: run `go test ./internal/fsq/cases -run TestBenchRegistryGolden -update` (%v)", err)
	}
	if got == string(want) {
		return
	}
	gotLines, wantLines := strings.Split(got, "\n"), strings.Split(string(want), "\n")
	for i := 0; i < len(gotLines) || i < len(wantLines); i++ {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			t.Fatalf("fs_hw_registry.golden differs from the emitter at line %d (rerun with -update if intended)\n  golden:  %s\n  emitter: %s", i+1, w, g)
		}
	}
}

func TestResolveBenchPlaceholders(t *testing.T) {
	now := data.FSNow
	rng := rand.New(rand.NewSource(7))
	keys := regexp.MustCompile(`[0-9]+`)
	list := ResolveBenchPlaceholders("IN ({KEYS:10})", rng, 1000, now)
	seen := map[string]bool{}
	for _, k := range keys.FindAllString(list, -1) {
		if seen[k] {
			t.Fatalf("duplicate key in %s", list)
		}
		seen[k] = true
	}
	if len(seen) != 10 {
		t.Fatalf("%d keys in %s", len(seen), list)
	}
	if all := ResolveBenchPlaceholders("{KEYS:2000}", rng, 1000, now); len(keys.FindAllString(all, -1)) != 1000 {
		t.Error("a list larger than the domain must contain every key once")
	}
	if big := ResolveBenchPlaceholders("{KEYS:700}", rng, 1000, now); len(keys.FindAllString(big, -1)) != 700 {
		t.Error("partial permutation for large lists")
	}
	for i := 0; i < 50; i++ {
		s := ResolveBenchPlaceholders("`account_id` = {ACCT} AND `currency` = {CUR}", rng, 1000, now)
		m := regexp.MustCompile("= ([0-9]+) AND `currency` = '([A-Z]+)'").FindStringSubmatch(s)
		if m == nil {
			t.Fatalf("composite: %s", s)
		}
		a := 0
		for _, c := range m[1] {
			a = a*10 + int(c-'0')
		}
		if a < 1 || a > 500 {
			t.Fatalf("account out of 1..E/2: %s", s)
		}
		ok := false
		for j := 0; j < data.NCurrencies(int64(a)); j++ {
			ok = ok || data.Currencies[j] == m[2]
		}
		if !ok {
			t.Fatalf("currency %s is not one of account %d's: %s", m[2], a, s)
		}
	}
	if w := ResolveBenchPlaceholders("{NOW-7d}", rng, 1000, now); w != bind.Timestamp(now.AddDate(0, 0, -7)) {
		t.Errorf("window: %s", w)
	}
	if w := ResolveBenchPlaceholders("{NOW-2h}", rng, 1000, now); w != "'2026-05-31 22:00:00'" {
		t.Errorf("2h window: %s", w)
	}
	if s := ResolveBenchPlaceholders("{SKEY}", rng, 1000, now); !regexp.MustCompile(`^'(cust|CUST)-000000[0-9][0-9]'$`).MatchString(s) {
		t.Errorf("string key outside 1..E/10: %s", s)
	}
	if s := ResolveBenchPlaceholders("{SKEYS:100}", rng, 1000, now); strings.Count(s, "'") != 200 {
		t.Errorf("100 quoted string keys expected: %s", s)
	}
	if k := ResolveBenchPlaceholders("{KEY}", rng, 0, now); k != "1" {
		t.Errorf("empty domain must still produce key 1: %s", k)
	}
	a1 := ResolveBenchPlaceholders("{KEY} {KEYS:3}", rand.New(rand.NewSource(3)), 100, now)
	a2 := ResolveBenchPlaceholders("{KEY} {KEYS:3}", rand.New(rand.NewSource(3)), 100, now)
	if a1 != a2 {
		t.Error("resolution must be deterministic for a seed")
	}
}
