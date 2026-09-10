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

package data

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/emit"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/spec"
)

type goldenFakeSQL struct {
	calls  []string
	failAt int
	rows   map[string]int64
}

func (f *goldenFakeSQL) ExecContext(ctx context.Context, stmt string, _ ...interface{}) (sql.Result, error) {
	f.calls = append(f.calls, stmt)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(f.calls) == f.failAt {
		return nil, errors.New("injected SQL failure")
	}
	return driver.RowsAffected(f.rows[stmt]), nil
}

func goldenFixtureGroups(t *testing.T, name string) []spec.FeatureGroup {
	t.Helper()
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	f, ok := fixtures[name]
	if !ok {
		t.Fatalf("missing fixture %s", name)
	}
	return f.FGs
}

func fakeForGoldenPlan(plan goldenLoadPlan) *goldenFakeSQL {
	f := &goldenFakeSQL{rows: map[string]int64{}}
	for _, step := range plan.steps {
		if step.rows >= 0 {
			f.rows[step.sql] = step.rows
		}
	}
	return f
}

func TestGoldenLoadPlanCorpus(t *testing.T) {
	fixtures, err := emit.LoadGoldenFixtures(filepath.Join("..", "testdata", "hopsworks_golden"))
	if err != nil {
		t.Fatal(err)
	}
	for name, fixture := range fixtures {
		t.Run(name, func(t *testing.T) {
			out, err := fixture.CapturedOutput()
			if err != nil {
				t.Fatal(err)
			}
			if out.Kind != emit.GoldenStatements {
				return
			}
			plan, err := planGoldenLoad(fixture.FGs)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.databases) == 0 || len(plan.steps) != 2*len(fixture.FGs) {
				t.Fatalf("incomplete setup plan: %+v", plan)
			}
			for _, step := range plan.steps {
				if strings.Contains(step.sql, "IF NOT EXISTS") ||
					(!strings.HasPrefix(step.sql, "CREATE TABLE ") && !strings.HasPrefix(step.sql, "INSERT INTO ")) {
					t.Fatalf("unexpected or reusing setup SQL: %s", step.sql)
				}
			}
			fake := fakeForGoldenPlan(plan)
			load := &GoldenLoad{}
			if err := load.populate(context.Background(), fake, plan); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(load.Databases(), plan.databases) {
				t.Fatal("successful CREATEs were not tracked")
			}
			for i, db := range plan.databases {
				want := "CREATE DATABASE `" + db + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
				if fake.calls[i] != want {
					t.Fatalf("database reservation is not exclusive: %s", fake.calls[i])
				}
			}
			if err := load.cleanup(context.Background(), fake); err != nil || len(load.Databases()) != 0 {
				t.Fatalf("cleanup failed: %v", err)
			}
		})
	}
}

func TestGoldenLoadFailureOwnership(t *testing.T) {
	plan, err := planGoldenLoad(goldenFixtureGroups(t, "snowflake_cross_store_gated"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.databases, []string{"golden_1_fs", "golden_2_fs"}) {
		t.Fatalf("cross-store database inventory changed: %v", plan.databases)
	}
	for failAt := 1; failAt <= len(plan.databases)+len(plan.steps); failAt++ {
		fake := fakeForGoldenPlan(plan)
		fake.failAt = failAt
		load := &GoldenLoad{}
		if err := load.populate(context.Background(), fake, plan); err == nil || len(fake.calls) != failAt {
			t.Fatalf("setup must stop at failed operation %d: %v", failAt, err)
		}
		owned := failAt - 1
		if owned > len(plan.databases) {
			owned = len(plan.databases)
		}
		if len(load.Databases()) != owned {
			t.Fatalf("operation %d: claimed %v, want %d databases", failAt, load.Databases(), owned)
		}
		fake.failAt = 0
		before := len(fake.calls)
		if err := load.cleanup(context.Background(), fake); err != nil {
			t.Fatal(err)
		}
		var want []string
		for i := owned - 1; i >= 0; i-- {
			want = append(want, "DROP DATABASE IF EXISTS `"+plan.databases[i]+"`;")
		}
		got := fake.calls[before:]
		if len(got) != len(want) || strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("operation %d: cleanup touched an unowned database: %v", failAt, got)
		}
	}
}

func TestGoldenCleanupRetry(t *testing.T) {
	load := &GoldenLoad{owned: []string{"golden_1_fs", "golden_2_fs"}}
	snapshot := load.Databases()
	snapshot[0] = "not-owned"
	fake := &goldenFakeSQL{failAt: 1}
	if err := load.cleanup(context.Background(), fake); err == nil ||
		!reflect.DeepEqual(load.Databases(), []string{"golden_2_fs"}) {
		t.Fatalf("failed cleanup lost ownership or stopped other drops: %v", err)
	}
	fake.failAt = 0
	if err := load.cleanup(context.Background(), fake); err != nil || len(load.Databases()) != 0 {
		t.Fatalf("cleanup retry failed: %v", err)
	}
	before := len(fake.calls)
	if err := load.cleanup(context.Background(), fake); err != nil || len(fake.calls) != before {
		t.Fatal("completed cleanup must be idempotent")
	}
	if strings.Contains(strings.Join(fake.calls, "\n"), "not-owned") {
		t.Fatal("Databases exposed mutable ownership")
	}
}

func TestGoldenLoadRowCount(t *testing.T) {
	plan, err := planGoldenLoad(goldenFixtureGroups(t, "aggregate_single"))
	if err != nil {
		t.Fatal(err)
	}
	fake := fakeForGoldenPlan(plan)
	fake.rows[plan.steps[1].sql]--
	load := &GoldenLoad{}
	if err := load.populate(context.Background(), fake, plan); err == nil ||
		!strings.Contains(err.Error(), "inserted 10 rows, want 11") || len(load.Databases()) != 1 {
		t.Fatalf("wrong load count must fail without losing cleanup ownership: %v", err)
	}
}

func TestGoldenLoadPlanInvalid(t *testing.T) {
	for _, change := range []string{"empty", "duplicate-id", "duplicate-table", "database", "version", "store", "offline", "ttl", "layout", "ddl"} {
		t.Run(change, func(t *testing.T) {
			fgs := goldenFixtureGroups(t, "aggregate_single")
			switch change {
			case "empty":
				fgs = nil
			case "duplicate-id":
				fgs = append(fgs, fgs[0])
			case "duplicate-table":
				fgs = append(fgs, fgs[0])
				fgs[1].ID++
			case "database":
				fgs[0].OnlineDB = "existing_user_database"
			case "version":
				fgs[0].Version = 0
			case "store":
				fgs[0].FeaturestoreID = 0
			case "offline":
				value := false
				fgs[0].Online = &value
			case "ttl":
				value := int64(60)
				fgs[0].TTL = &value
			case "layout":
				fgs[0].Features[0].Type = "double"
			case "ddl":
				fgs[0].OnlineConfig.PrimaryKeyIndexType = "invalid"
			}
			plan, err := planGoldenLoad(fgs)
			if err == nil || !reflect.DeepEqual(plan, goldenLoadPlan{}) {
				t.Fatalf("invalid input returned a partial setup plan: %+v, %v", plan, err)
			}
		})
	}
}
