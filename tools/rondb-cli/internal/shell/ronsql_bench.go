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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/readline"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/client"
	"github.com/logicalclocks/rondb/tools/rondb-cli/internal/ui"
)

// ronsqlBenchTimeout is the per-request timeout for RonSQL benchmark queries.
// Analytic queries over large scale factors can far exceed the default 30s
// REST timeout used for point operations.
const ronsqlBenchTimeout = 300 * time.Second

// RonSQLBenchQuery is a named RonSQL benchmark query. Queries run against
// the tpch database created by .load_tpch. A query may contain the {KEY}
// placeholder, which the benchmark runner substitutes with a random key
// in [1, maxKey] on every request; maxKey is discovered at benchmark start
// via KeySQL (over the MySQL connection) with KeyDefault as fallback.
type RonSQLBenchQuery struct {
	Name        string
	Description string
	Database    string
	SQL         string
	RandKey     bool
	KeySQL      string
	KeyDefault  int
}

// ronsqlBenchQueries is the registry of named RonSQL benchmark queries.
//
// Two families:
//
//   - fs_*: Feature-Store-style workloads. One or more CTEs compute
//     per-entity aggregate features (single table or a simple join in the
//     CTE body) and are joined to an entity table via scans and key lookups.
//
//   - tpch_*: TPC-H queries that originally contain subqueries or derived
//     tables, rewritten to use CTEs within RonSQL's supported envelope
//     (aggregating main SELECT, complete-key CTE joins, no ORDER BY/LIMIT/
//     HAVING/AVG; sorting is a client-side concern).
var ronsqlBenchQueries = []RonSQLBenchQuery{
	// ---------------------------------------------------------------
	// Feature-Store-style benchmarks
	// ---------------------------------------------------------------
	{
		Name:        "fs_point",
		Description: "On-demand feature vector for one random customer (CTE body filtered on entity key)",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(c_custkey) FROM tpch.customer",
		KeyDefault:  tpchCustomerBase,
		SQL: `WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_totalprice) AS total_spend,
         MIN(o_orderdate) AS first_order, MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey = {KEY} GROUP BY o_custkey)
SELECT MAX(cust_features.order_cnt), MAX(cust_features.total_spend),
       MIN(cust_features.first_order), MAX(cust_features.last_order)
FROM cust_features;`,
	},
	{
		Name:        "fs_batch",
		Description: "Batch feature computation: per-customer order features re-aggregated by market segment",
		Database:    "tpch",
		SQL: `WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_totalprice) AS total_spend,
         MAX(o_totalprice) AS max_order
  FROM orders GROUP BY o_custkey)
SELECT c.c_mktsegment, COUNT(*), SUM(cust_features.order_cnt),
       SUM(cust_features.total_spend), MAX(cust_features.max_order)
FROM customer AS c JOIN cust_features ON cust_features.k = c.c_custkey
GROUP BY c.c_mktsegment;`,
	},
	{
		Name:        "fs_multi",
		Description: "Two feature-group CTEs (lifetime + recent orders) joined to the customer entity table",
		Database:    "tpch",
		SQL: `WITH order_stats AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey),
recent_stats AS (
  SELECT o_custkey AS k2, COUNT(*) AS recent_cnt
  FROM orders WHERE o_orderdate >= '1998-01-01' GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(order_stats.cnt), SUM(order_stats.spend),
       SUM(recent_stats.recent_cnt)
FROM customer AS c
JOIN order_stats ON order_stats.k = c.c_custkey
JOIN recent_stats ON recent_stats.k2 = c.c_custkey
GROUP BY c.c_nationkey;`,
	},
	{
		Name:        "fs_join_body",
		Description: "Feature CTE over a two-table join (lineitem x orders) re-aggregated per supplier nation",
		Database:    "tpch",
		SQL: `WITH supp_features AS (
  SELECT l.l_suppkey AS sk, COUNT(*) AS item_cnt, SUM(l.l_quantity) AS total_qty
  FROM lineitem AS l JOIN orders AS o ON o.o_orderkey = l.l_orderkey
  WHERE o.o_orderstatus = 'F'
  GROUP BY l.l_suppkey)
SELECT s.s_nationkey, COUNT(*), SUM(supp_features.item_cnt), SUM(supp_features.total_qty)
FROM supplier AS s JOIN supp_features ON supp_features.sk = s.s_suppkey
GROUP BY s.s_nationkey;`,
	},
	{
		Name:        "fs_anti",
		Description: "Churn-style anti-join: customers with no recent orders, grouped by nation",
		Database:    "tpch",
		SQL: `WITH recent_orders AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt
  FROM orders WHERE o_orderdate >= '1998-01-01' GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), MAX(c.c_acctbal)
FROM customer AS c LEFT JOIN recent_orders ON recent_orders.k = c.c_custkey
WHERE recent_orders.cnt IS NULL
GROUP BY c.c_nationkey;`,
	},
	{
		Name:        "fs_scalar",
		Description: "Scalar reduce over a per-customer feature CTE (pure CTE materialization cost)",
		Database:    "tpch",
		SQL: `WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders GROUP BY o_custkey)
SELECT COUNT(*), SUM(cust_features.cnt), MAX(cust_features.spend), MIN(cust_features.spend)
FROM cust_features;`,
	},

	// ---------------------------------------------------------------
	// TPC-H queries rewritten with CTEs (replacing subqueries or
	// derived tables in the official formulation)
	// ---------------------------------------------------------------
	{
		Name:        "tpch_q2",
		Description: "Q2 minimum-cost supplier: correlated MIN(ps_supplycost) subquery as a per-part CTE",
		Database:    "tpch",
		SQL: `WITH min_cost AS (
  SELECT ps_partkey AS pk, MIN(ps_supplycost) AS mc, COUNT(*) AS supplier_cnt
  FROM partsupp GROUP BY ps_partkey)
SELECT p.p_mfgr, COUNT(*), MIN(min_cost.mc), MAX(min_cost.mc), SUM(min_cost.supplier_cnt)
FROM part AS p JOIN min_cost ON min_cost.pk = p.p_partkey
WHERE p.p_size = 15
GROUP BY p.p_mfgr;`,
	},
	{
		Name:        "tpch_q11",
		Description: "Q11 important stock: per-supplier stock value CTE joined to suppliers of one nation",
		Database:    "tpch",
		SQL: `WITH stock AS (
  SELECT ps_suppkey AS sk, SUM(ps_availqty) AS total_qty, COUNT(*) AS part_cnt
  FROM partsupp GROUP BY ps_suppkey)
SELECT s.s_nationkey, SUM(stock.total_qty), SUM(stock.part_cnt), COUNT(*)
FROM supplier AS s JOIN stock ON stock.sk = s.s_suppkey
WHERE s.s_nationkey = 7
GROUP BY s.s_nationkey;`,
	},
	{
		Name:        "tpch_q13",
		Description: "Q13 customer distribution: orders-per-customer derived table as a CTE, LEFT JOIN with NULL customers",
		Database:    "tpch",
		SQL: `WITH c_orders AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt FROM orders GROUP BY o_custkey)
SELECT COUNT(*), SUM(c_orders.order_cnt), MIN(c_orders.order_cnt), MAX(c_orders.order_cnt)
FROM customer AS c LEFT JOIN c_orders ON c_orders.k = c.c_custkey;`,
	},
	{
		Name:        "tpch_q15",
		Description: "Q15 top supplier: revenue view as a date-filtered CTE, scalar MAX subquery as the main SELECT",
		Database:    "tpch",
		SQL: `WITH revenue AS (
  SELECT l_suppkey AS sk, SUM(l_extendedprice) AS total_rev, COUNT(*) AS item_cnt
  FROM lineitem
  WHERE l_shipdate >= '1996-01-01' AND l_shipdate <= '1996-03-31'
  GROUP BY l_suppkey)
SELECT MAX(revenue.total_rev), MIN(revenue.total_rev), SUM(revenue.item_cnt), COUNT(*)
FROM revenue;`,
	},
	{
		Name:        "tpch_q22",
		Description: "Q22 global sales opportunity: NOT EXISTS orders as a CTE anti-join over positive-balance customers",
		Database:    "tpch",
		SQL: `WITH cust_orders AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(c.c_acctbal), MAX(c.c_acctbal)
FROM customer AS c LEFT JOIN cust_orders ON cust_orders.k = c.c_custkey
WHERE cust_orders.order_cnt IS NULL AND c.c_acctbal > 0.00
GROUP BY c.c_nationkey;`,
	},
}

// findRonSQLBenchQuery returns the registered query with the given name
// (case-insensitive), or nil.
func findRonSQLBenchQuery(name string) *RonSQLBenchQuery {
	for i := range ronsqlBenchQueries {
		if strings.EqualFold(ronsqlBenchQueries[i].Name, name) {
			return &ronsqlBenchQueries[i]
		}
	}
	return nil
}

// ronsqlBenchCompletions returns completer items for .bench_ronsql.
func ronsqlBenchCompletions() []readline.PrefixCompleterInterface {
	items := []readline.PrefixCompleterInterface{
		readline.PcItem("list"),
		readline.PcItem("all"),
	}
	for _, q := range ronsqlBenchQueries {
		items = append(items, readline.PcItem(q.Name))
	}
	return items
}

// listRonSQLBenchQueries prints the registry.
func (s *Shell) listRonSQLBenchQueries() {
	fmt.Println()
	fmt.Println(ui.Info("RonSQL benchmark queries (run with .bench_ronsql <name> [T] [N]):"))
	fmt.Println()
	fmt.Println("  Feature-Store-style (CTE feature groups joined to entity tables):")
	for _, q := range ronsqlBenchQueries {
		if strings.HasPrefix(q.Name, "fs_") {
			fmt.Printf("    %-14s %s\n", q.Name, q.Description)
		}
	}
	fmt.Println()
	fmt.Println("  TPC-H rewritten with CTEs:")
	for _, q := range ronsqlBenchQueries {
		if strings.HasPrefix(q.Name, "tpch_") {
			fmt.Printf("    %-14s %s\n", q.Name, q.Description)
		}
	}
	fmt.Println()
	fmt.Println("    all            Run every query sequentially")
	fmt.Println()
	fmt.Println("  Queries run against the tpch database - run .load_tpch first.")
	fmt.Println("  Defaults: T=1 thread, N=10 requests per thread.")
	fmt.Println()
}

// resolveRonSQLBenchKeyRange determines the max key for {KEY} substitution.
func (s *Shell) resolveRonSQLBenchKeyRange(q *RonSQLBenchQuery) int {
	maxKey := q.KeyDefault
	if s.mysqlClient == nil || q.KeySQL == "" {
		return maxKey
	}
	_, rows, _, err := s.mysqlClient.Query(q.KeySQL)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		fmt.Println(ui.Info(fmt.Sprintf("Could not determine key range (%v), using default max key %d", err, maxKey)))
		return maxKey
	}
	switch v := rows[0][0].(type) {
	case int64:
		maxKey = int(v)
	case []byte:
		if n, err := strconv.Atoi(string(v)); err == nil {
			maxKey = n
		}
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			maxKey = n
		}
	}
	if maxKey <= 0 {
		maxKey = q.KeyDefault
	}
	return maxKey
}

// buildRonSQLBenchSQL substitutes the {KEY} placeholder if present.
func buildRonSQLBenchSQL(q *RonSQLBenchQuery, rng *rand.Rand, maxKey int) string {
	if !q.RandKey {
		return q.SQL
	}
	key := rng.Intn(maxKey) + 1
	return strings.ReplaceAll(q.SQL, "{KEY}", strconv.Itoa(key))
}

// countRonSQLResultRows counts data rows in a TEXT (header + TSV) response.
func countRonSQLResultRows(data []byte) int {
	body := strings.TrimSpace(string(data))
	if body == "" {
		return 0
	}
	// First line is the header in TEXT format
	return len(strings.Split(body, "\n")) - 1
}

// runBenchRonSQL dispatches a named RonSQL benchmark (or "all"/"list").
func (s *Shell) runBenchRonSQL(name string, numThreads, numOps int) error {
	if name == "" || strings.EqualFold(name, "list") {
		s.listRonSQLBenchQueries()
		return nil
	}
	if strings.EqualFold(name, "all") {
		var failed []string
		for i := range ronsqlBenchQueries {
			if err := s.runBenchRonSQLQuery(&ronsqlBenchQueries[i], numThreads, numOps); err != nil {
				fmt.Println(ui.Error(fmt.Sprintf("Benchmark %s failed: %v", ronsqlBenchQueries[i].Name, err)))
				failed = append(failed, ronsqlBenchQueries[i].Name)
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("%d of %d benchmarks failed: %s",
				len(failed), len(ronsqlBenchQueries), strings.Join(failed, ", "))
		}
		return nil
	}
	q := findRonSQLBenchQuery(name)
	if q == nil {
		s.listRonSQLBenchQueries()
		return fmt.Errorf("unknown RonSQL benchmark query: %s", name)
	}
	return s.runBenchRonSQLQuery(q, numThreads, numOps)
}

// runBenchRonSQLQuery runs one named query numThreads x numOps times against
// the RonSQL REST endpoint and reports throughput and latency percentiles.
func (s *Shell) runBenchRonSQLQuery(q *RonSQLBenchQuery, numThreads, numOps int) error {
	if s.restClient == nil {
		return fmt.Errorf("REST API not connected. RonSQL benchmarks require REST API.")
	}

	totalOps := numThreads * numOps

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("RonSQL Benchmark %s: %d threads × %d requests = %d total requests",
		q.Name, numThreads, numOps, totalOps)))
	fmt.Println(ui.Info(q.Description))
	fmt.Println()
	fmt.Println(strings.TrimSpace(q.SQL))
	fmt.Println()

	maxKey := 0
	if q.RandKey {
		maxKey = s.resolveRonSQLBenchKeyRange(q)
		fmt.Println(ui.Info(fmt.Sprintf("Random key range: 1..%d", maxKey)))
	}

	// Create REST clients for each thread
	clients := make([]*client.RestClient, numThreads)
	for i := 0; i < numThreads; i++ {
		c, err := client.NewRestClientWithOptions(client.RestOptions{
			Host:    s.config.RDRSHost,
			Port:    s.config.RestPort,
			TLS:     s.config.RDRSTLS,
			APIKey:  s.config.RDRSAPIKey,
			Timeout: ronsqlBenchTimeout,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create REST client for thread %d: %w", i, err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			if c != nil {
				c.Close()
			}
		}
	}()

	endpoint := "/" + APIVersion + "/ronsql"
	warmupRng := rand.New(rand.NewSource(1))

	// Warmup: run the query once to validate it and prime dictionary caches.
	// An unsupported query shape fails here with the RonSQL error message
	// instead of producing a benchmark full of errors.
	warmupReq := RonSQLRequest{
		Query:        buildRonSQLBenchSQL(q, warmupRng, maxKey),
		Database:     q.Database,
		ExplainMode:  "ALLOW",
		OutputFormat: "TEXT",
	}
	data, warmupDur, err := clients[0].Post(endpoint, warmupReq)
	if err != nil {
		if len(data) > 0 {
			fmt.Println(ui.Error("RonSQL response:"))
			fmt.Println(strings.TrimSpace(string(data)))
		}
		return fmt.Errorf("warmup request failed: %w", err)
	}
	warmupRows := countRonSQLResultRows(data)
	fmt.Println(ui.Info(fmt.Sprintf("Warmup: %s, %d result rows", formatLatency(warmupDur), warmupRows)))
	if s.debug {
		fmt.Println(strings.TrimSpace(string(data)))
	}
	fmt.Println()

	var doneOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	benchStart := time.Now()

	// Progress reporting goroutine
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(&doneOps)
				errs := errorCollector.Count()
				elapsed := time.Since(benchStart)
				opsPerSec := float64(ops) / elapsed.Seconds()
				pct := float64(ops) / float64(totalOps) * 100
				fmt.Printf("   Progress: %d/%d requests (%.1f%%), errors=%d, %.1f queries/sec\n",
					ops, totalOps, pct, errs, opsPerSec)
			case <-stopProgress:
				return
			}
		}
	}()

	debugMode := s.debug
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, restClient *client.RestClient) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(threadID)*100003 + 7))
			for i := 0; i < numOps; i++ {
				req := RonSQLRequest{
					Query:        buildRonSQLBenchSQL(q, rng, maxKey),
					Database:     q.Database,
					ExplainMode:  "ALLOW",
					OutputFormat: "TEXT",
				}
				_, duration, err := restClient.Post(endpoint, req)
				latencyCollector.Record(duration)
				if debugMode {
					fmt.Printf("[DEBUG] %s op %d: %s err=%v\n", q.Name, i, formatLatency(duration), err)
				}
				if err != nil {
					errorCollector.Record(err)
				} else {
					atomic.AddInt64(&doneOps, 1)
				}
			}
		}(t, clients[t])
	}
	wg.Wait()
	close(stopProgress)
	benchDuration := time.Since(benchStart)

	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()

	fmt.Println()
	opsPerSec := float64(doneOps) / benchDuration.Seconds()
	fmt.Println(ui.Success(fmt.Sprintf("RonSQL Benchmark %s completed in %.2fs", q.Name, benchDuration.Seconds())))
	fmt.Printf("   Requests: %d (errors: %d)\n", doneOps, errorCollector.Count())
	fmt.Printf("   Throughput: %.2f queries/sec\n", opsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()

	return nil
}
