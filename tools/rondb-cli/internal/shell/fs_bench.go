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

// The fs_hw benchmark category (RONDB-1121 E5, benchmarks.md): the
// Hopsworks serving shapes over the feature-store data set (`fs_bench`,
// loaded by .fs_load), generated at init from the same emitter as the
// golden tests, with request-time placeholders resolved by fsq/cases.

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/cases"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/fsq/data"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

const (
	fsBenchDB       = "fs_bench"
	fsHWKeySQL      = "SELECT MAX(customer_id) FROM fs_bench.customers_1"
	fsHWKeyDefault  = 100000 // customers at scale factor 1
	fsHWProbeSQL    = "SELECT 1 FROM fs_bench.countries_1 LIMIT 1"
	fsHWListHeading = "Hopsworks feature-store serving shapes (fs_hw_*, database fs_bench: run .fs_load first)"
)

// fsHWRegistryErr records a generation failure so the listing can report
// it instead of silently missing the category.
var fsHWRegistryErr error

func init() {
	entries, err := cases.BenchEntries(cases.Config{DB: fsBenchDB, Scale: data.NewScale(1)})
	if err != nil {
		fsHWRegistryErr = err
		return
	}
	for _, e := range entries {
		ronsqlBenchQueries = append(ronsqlBenchQueries, RonSQLBenchQuery{
			Name:         e.Name,
			Category:     benchCatFSHW,
			MySQLOnly:    e.MySQLOnly,
			Description:  fmt.Sprintf("[%s] %s (rows/req %s)", e.Shape, e.Description, e.Rows),
			Database:     fsBenchDB,
			SQL:          e.SQL,
			RandKey:      strings.Contains(e.SQL, "{"),
			KeySQL:       fsHWKeySQL,
			KeyDefault:   fsHWKeyDefault,
			Resolver:     fsHWResolver,
			Placeholders: cases.BenchPlaceholderLegend,
			PlanPins:     e.PlanPins,
		})
	}
}

// fsHWResolver renders the fs_hw placeholders for one request; maxKey is
// the customer domain discovered through KeySQL.
func fsHWResolver(sql string, rng *rand.Rand, maxKey int) string {
	return cases.ResolveBenchPlaceholders(sql, rng, maxKey, data.FSNow)
}

// fsHWAvailable reports whether the fs_bench database is loaded, so `all`
// can skip the category on TPC-H-only setups.
func (s *Shell) fsHWAvailable() bool {
	if s.mysqlClient == nil {
		return false
	}
	_, _, _, err := s.mysqlClient.Query(fsHWProbeSQL)
	return err == nil
}

// printFSHWCategory prints the fs_hw listing section.
func printFSHWCategory(sqlNames bool) {
	fmt.Println("  " + fsHWListHeading + ":")
	if fsHWRegistryErr != nil {
		fmt.Println("    (registry generation failed: " + fsHWRegistryErr.Error() + ")")
		return
	}
	printBenchQueryCategory(benchCatFSHW, sqlNames)
	fmt.Println("    fs_hw                Run every fs_hw entry sequentially")
}

// runBenchFSHW runs every fs_hw entry (the RonSQL-capable ones on the
// RonSQL side) and reports the failures at the end.
func (s *Shell) runBenchFSHW(ronsql bool, numThreads, numOps int) error {
	if fsHWRegistryErr != nil {
		return fsHWRegistryErr
	}
	if !s.fsHWAvailable() {
		return fmt.Errorf("database %s is not loaded: run .fs_load <sf> first", fsBenchDB)
	}
	var failed []string
	total := 0
	for i := range ronsqlBenchQueries {
		q := &ronsqlBenchQueries[i]
		if q.Category != benchCatFSHW || (ronsql && q.MySQLOnly) {
			continue
		}
		total++
		var err error
		if ronsql {
			err = s.runBenchRonSQLQuery(q, numThreads, numOps)
		} else {
			err = s.runBenchSQLQuery(q, numThreads, numOps)
		}
		if err != nil {
			fmt.Println(ui.Error(fmt.Sprintf("Benchmark %s failed: %v", q.Name, err)))
			failed = append(failed, q.Name)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d of %d fs_hw benchmarks failed: %s", len(failed), total, strings.Join(failed, ", "))
	}
	return nil
}

// checkBenchPlanPins fetches the RonSQL EXPLAIN of the warmup statement,
// prints it, and warns about every missing plan pin (benchmarks.md §7): a
// latency shift on an entry with a pin warning is a plan change, not
// necessarily a code regression.  Warnings never fail the benchmark.
func (s *Shell) checkBenchPlanPins(c *client.RestClient, q *RonSQLBenchQuery, sqlText string) {
	req := RonSQLRequest{Query: sqlText, Database: q.Database, ExplainMode: "FORCE", OutputFormat: "TEXT"}
	body, _, err := c.Post("/"+APIVersion+"/ronsql", req)
	if err != nil {
		fmt.Println(ui.Warning("Plan pin check: EXPLAIN failed: " + err.Error()))
		return
	}
	plan := strings.TrimSpace(string(body))
	fmt.Println(ui.Info("Warmup plan (EXPLAIN):"))
	fmt.Println(plan)
	for _, pin := range q.PlanPins {
		if !strings.Contains(plan, pin) {
			fmt.Println(ui.Warning(fmt.Sprintf("Plan pin missing: %q — the plan differs from benchmarks.md §7; read a latency change on this entry as a plan change first", pin)))
		}
	}
	fmt.Println()
}
