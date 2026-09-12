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

package shell

import (
	"math/rand"
	"strings"
	"testing"
)

func TestFSHWRegistry(t *testing.T) {
	if fsHWRegistryErr != nil {
		t.Fatal(fsHWRegistryErr)
	}
	names := map[string]int{}
	fshw, twins := 0, 0
	for _, q := range ronsqlBenchQueries {
		names[q.Name]++
		if q.Category != benchCatFSHW {
			continue
		}
		fshw++
		if q.MySQLOnly {
			twins++
		}
		if q.Database != fsBenchDB || q.Resolver == nil || q.Placeholders == "" || !strings.HasPrefix(q.Name, "fs_hw_") {
			t.Errorf("%s: incomplete registration %+v", q.Name, q)
		}
		if q.RandKey && (q.KeySQL != fsHWKeySQL || q.KeyDefault != fsHWKeyDefault) {
			t.Errorf("%s: key range must come from fs_bench.customers_1", q.Name)
		}
		sql := buildRonSQLBenchSQL(&q, rand.New(rand.NewSource(1)), 1000)
		if strings.Contains(sql, "{") {
			t.Errorf("%s: unresolved placeholder in %s", q.Name, sql)
		}
		if a, b := buildRonSQLBenchSQL(&q, rand.New(rand.NewSource(1)), 1000), sql; a != b {
			t.Errorf("%s: resolution must be deterministic for a seed", q.Name)
		}
	}
	if fshw != 25 || twins != 3 {
		t.Fatalf("fs_hw entries %d (twins %d), want 25 (3)", fshw, twins)
	}
	for name, n := range names {
		if n > 1 {
			t.Errorf("registry name %s registered %d times", name, n)
		}
	}
	if q := findRonSQLBenchQuery("fs_hw_collect5_twin"); q == nil || !q.MySQLOnly {
		t.Error("twins must be MySQL-only registry entries")
	}
	if q := findSQLBenchQuery("fs_hw_agg_point"); q == nil || q.sqlBenchName() != "fs_hw_agg_point" {
		t.Error("fs_hw entries keep their name in the .bench_sql namespace")
	}
}
