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
var ronsqlBenchTimeoutSeconds = "120"

func ronsqlBenchTimeout() time.Duration {
	seconds, err := strconv.Atoi(ronsqlBenchTimeoutSeconds)
	if err != nil || seconds <= 0 {
		seconds = 120
	}
	return time.Duration(seconds) * time.Second
}

// Benchmark query categories (listing sections).
const (
	benchCatFS           = "fs"            // online feature-store-style, filter-bounded
	benchCatOfflineFS    = "offline_fs"    // offline feature materialization (full-table CTEs)
	benchCatTPCHCte      = "tpch_cte"      // TPC-H rewritten with CTEs (RonSQL envelope)
	benchCatTPCHOfficial = "tpch_official" // official TPC-H formulation (MySQL only)
)

// RonSQLBenchQuery is a named analytics benchmark query over the tpch
// database created by .load_tpch.
//
// The same registry backs two commands:
//
//   - .bench_ronsql <Name>: runs the query through the RonSQL REST endpoint.
//     MySQLOnly entries are rejected (their shape is outside the RonSQL
//     envelope, e.g. correlated subqueries, comma joins, derived tables,
//     or non-aggregate SELECT).
//
//   - .bench_sql <sqlBenchName()>: runs the same SQL through the MySQL
//     server, as a comparative baseline. SQLName renames an entry in this
//     namespace (the CTE TPC-H rewrites appear as cte_tpch_q*, freeing the
//     tpch_q* names for the official formulations).
//
// A query may contain the {KEY} placeholder, substituted with a random key
// in [1, maxKey] on every request; maxKey is discovered at benchmark start
// via KeySQL (over the MySQL connection) with KeyDefault as fallback. When
// KeySpan > 0, {KEY2} is substituted with {KEY} + KeySpan and the random
// range shrinks so [KEY, KEY2) stays within [1, maxKey].
type RonSQLBenchQuery struct {
	Name        string
	SQLName     string // name in the .bench_sql namespace; empty = Name
	Category    string
	MySQLOnly   bool // shape outside the RonSQL envelope (correlated subqueries, comma joins, derived tables, non-aggregate SELECT)
	Description string
	Database    string
	SQL         string
	RandKey     bool
	KeySQL      string
	KeyDefault  int
	KeySpan     int // when > 0, {KEY2} = {KEY} + KeySpan
}

// sqlBenchName is the query's name in the .bench_sql namespace.
func (q *RonSQLBenchQuery) sqlBenchName() string {
	if q.SQLName != "" {
		return q.SQLName
	}
	return q.Name
}

// ronsqlBenchQueries is the registry of named analytics benchmark queries.
//
// Four families:
//
//   - fs_*: online Feature-Store-style workloads. CTEs compute per-entity
//     aggregate features and are joined to entity tables, with filters
//     bounding the work to hundreds .. tens of thousands of source rows
//     (online serving latencies, not full-table sweeps). RonSQL supports
//     ORDER BY / LIMIT on aggregate queries (targets must be GROUP BY
//     columns or aggregate aliases); fs_history stays MySQL-only until
//     RonSQL supports single-table non-aggregate SELECT (see
//     ronsql_orderby_limit_plan.md).
//
//   - offline_fs_*: offline feature materialization. Full-table CTEs
//     (per-customer / per-supplier aggregates over all orders or lineitems)
//     re-aggregated across the entity table.
//
//   - tpch_q* (.bench_ronsql) / cte_tpch_q* (.bench_sql): TPC-H queries that
//     originally contain subqueries or derived tables, rewritten to use CTEs
//     within RonSQL's supported envelope (aggregating main SELECT,
//     complete-key CTE joins, no HAVING/AVG; ORDER BY on GROUP BY columns
//     or aggregate aliases plus LIMIT are supported).
//
//   - tpch_q* (.bench_sql only): the official TPC-H formulations (region/
//     nation joins, correlated subqueries, HAVING, ORDER BY/LIMIT), with
//     literals adapted to the generated data. Comparing tpch_qN against
//     cte_tpch_qN on MySQL shows the cost of the CTE rewrite; comparing
//     cte_tpch_qN on MySQL against tpch_qN on .bench_ronsql shows
//     RonSQL vs MySQL on identical SQL.
var ronsqlBenchQueries = []RonSQLBenchQuery{
	// ---------------------------------------------------------------
	// Online Feature-Store-style benchmarks (filter-bounded)
	// ---------------------------------------------------------------
	{
		Name:        "fs_point",
		Category:    benchCatFS,
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
		Category:    benchCatFS,
		Description: "Batch serving: per-entity feature vectors for a random 100-customer segment (~1k orders)",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(c_custkey) FROM tpch.customer",
		KeyDefault:  tpchCustomerBase,
		KeySpan:     100,
		SQL: `WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_totalprice) AS total_spend,
         MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey >= {KEY} AND o_custkey < {KEY2}
  GROUP BY o_custkey)
SELECT c.c_custkey, COUNT(*), SUM(cust_features.order_cnt),
       SUM(cust_features.total_spend), MAX(cust_features.last_order)
FROM customer AS c JOIN cust_features ON cust_features.k = c.c_custkey
WHERE c.c_custkey >= {KEY} AND c.c_custkey < {KEY2}
GROUP BY c.c_custkey;`,
	},
	{
		Name:        "fs_freshness",
		Category:    benchCatFS,
		Description: "Two feature-group CTEs (lifetime + last-order) over a random 500-customer segment (~10k orders)",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(c_custkey) FROM tpch.customer",
		KeyDefault:  tpchCustomerBase,
		KeySpan:     500,
		SQL: `WITH lifetime AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders WHERE o_custkey >= {KEY} AND o_custkey < {KEY2}
  GROUP BY o_custkey),
recent AS (
  SELECT o_custkey AS k2, MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey >= {KEY} AND o_custkey < {KEY2}
  GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(lifetime.cnt), SUM(lifetime.spend),
       MAX(recent.last_order)
FROM customer AS c
JOIN lifetime ON lifetime.k = c.c_custkey
JOIN recent ON recent.k2 = c.c_custkey
WHERE c.c_custkey >= {KEY} AND c.c_custkey < {KEY2}
GROUP BY c.c_nationkey;`,
	},
	{
		Name:        "fs_supplier",
		Category:    benchCatFS,
		Description: "Supplier features over a 3-day shipment window (~7k lineitems via l_shipdate index) joined to one nation's suppliers",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(s_nationkey) FROM tpch.supplier",
		KeyDefault:  24,
		SQL: `WITH supp_recent AS (
  SELECT l_suppkey AS sk, COUNT(*) AS item_cnt, SUM(l_extendedprice) AS revenue
  FROM lineitem
  WHERE l_shipdate >= '1998-06-01' AND l_shipdate <= '1998-06-03'
  GROUP BY l_suppkey)
SELECT s.s_nationkey, COUNT(*), SUM(supp_recent.item_cnt),
       SUM(supp_recent.revenue), MAX(supp_recent.revenue)
FROM supplier AS s JOIN supp_recent ON supp_recent.sk = s.s_suppkey
WHERE s.s_nationkey = {KEY}
GROUP BY s.s_nationkey;`,
	},
	{
		Name:        "fs_nation",
		Category:    benchCatFS,
		Description: "Recent-window order features (~40k orders via o_orderdate index) joined to one nation's customers, per market segment",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(c_nationkey) FROM tpch.customer",
		KeyDefault:  24,
		SQL: `WITH recent AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders WHERE o_orderdate >= '1998-06-01'
  GROUP BY o_custkey)
SELECT c.c_mktsegment, COUNT(*), SUM(recent.cnt), SUM(recent.spend)
FROM customer AS c JOIN recent ON recent.k = c.c_custkey
WHERE c.c_nationkey = {KEY}
GROUP BY c.c_mktsegment;`,
	},
	{
		Name:        "fs_topk",
		Category:    benchCatFS,
		Description: "Top-100 recent spenders in one nation (aggregate-form CTE join, ORDER BY spend DESC LIMIT 100)",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(c_nationkey) FROM tpch.customer",
		KeyDefault:  24,
		// Aggregate-form equivalent of the natural projection-only query
		// (SELECT c_custkey, c_name, recent.cnt, recent.spend ... ORDER BY
		// recent.spend DESC): c_custkey is unique, so grouping by
		// (c_custkey, c_name) yields one group per customer and MAX() is
		// the identity. RonSQL supports ORDER BY/LIMIT on aggregate
		// queries only; the projection-only form needs
		// ronsql_orderby_limit_plan.md Phase 3.
		SQL: `WITH recent AS (
  SELECT o_custkey AS k, COUNT(*) AS cnt, SUM(o_totalprice) AS spend
  FROM orders WHERE o_orderdate >= '1998-06-01'
  GROUP BY o_custkey)
SELECT c.c_custkey, c.c_name, MAX(recent.cnt) AS cnt, MAX(recent.spend) AS top_spend
FROM customer AS c JOIN recent ON recent.k = c.c_custkey
WHERE c.c_nationkey = {KEY}
GROUP BY c.c_custkey, c.c_name
ORDER BY top_spend DESC
LIMIT 100;`,
	},
	{
		Name:        "fs_history",
		Category:    benchCatFS,
		MySQLOnly:   true,
		Description: "Order-history page for a random 200-customer segment (~2k orders, ORDER BY o_orderdate DESC LIMIT 1000)",
		Database:    "tpch",
		RandKey:     true,
		KeySQL:      "SELECT MAX(c_custkey) FROM tpch.customer",
		KeyDefault:  tpchCustomerBase,
		KeySpan:     200,
		SQL: `SELECT o_orderkey, o_custkey, o_orderdate, o_totalprice, o_orderstatus
FROM orders
WHERE o_custkey >= {KEY} AND o_custkey < {KEY2}
ORDER BY o_orderdate DESC, o_orderkey DESC
LIMIT 1000;`,
	},

	// ---------------------------------------------------------------
	// Offline Feature-Store-style benchmarks (full-table CTEs)
	// ---------------------------------------------------------------
	{
		Name:        "offline_fs_batch",
		Category:    benchCatOfflineFS,
		Description: "Full batch materialization: per-customer order features re-aggregated by market segment",
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
		Name:        "offline_fs_multi",
		Category:    benchCatOfflineFS,
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
		Name:        "offline_fs_join_body",
		Category:    benchCatOfflineFS,
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
		Name:        "offline_fs_anti",
		Category:    benchCatOfflineFS,
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
		Name:        "offline_fs_scalar",
		Category:    benchCatOfflineFS,
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
	// derived tables in the official formulation). Named tpch_q* in
	// .bench_ronsql and cte_tpch_q* in .bench_sql.
	// ---------------------------------------------------------------
	{
		Name:        "tpch_q2",
		SQLName:     "cte_tpch_q2",
		Category:    benchCatTPCHCte,
		Description: "Q2 minimum-cost supplier: correlated MIN(ps_supplycost) subquery as a per-part CTE",
		Database:    "tpch",
		SQL: `WITH min_cost AS (
  SELECT ps_partkey AS pk, MIN(ps_supplycost) AS mc, COUNT(*) AS supplier_cnt
  FROM partsupp GROUP BY ps_partkey)
SELECT p.p_mfgr, COUNT(*), MIN(min_cost.mc), MAX(min_cost.mc), SUM(min_cost.supplier_cnt)
FROM part AS p JOIN min_cost ON min_cost.pk = p.p_partkey
WHERE p.p_size = 15
GROUP BY p.p_mfgr
ORDER BY p.p_mfgr
LIMIT 100;`,
	},
	{
		Name:        "tpch_q11",
		SQLName:     "cte_tpch_q11",
		Category:    benchCatTPCHCte,
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
		SQLName:     "cte_tpch_q13",
		Category:    benchCatTPCHCte,
		Description: "Q13 customer distribution: orders-per-customer derived table as a CTE, LEFT JOIN with NULL customers",
		Database:    "tpch",
		SQL: `WITH c_orders AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt FROM orders GROUP BY o_custkey)
SELECT COUNT(*), SUM(c_orders.order_cnt), MIN(c_orders.order_cnt), MAX(c_orders.order_cnt)
FROM customer AS c LEFT JOIN c_orders ON c_orders.k = c.c_custkey;`,
	},
	{
		Name:        "tpch_q15",
		SQLName:     "cte_tpch_q15",
		Category:    benchCatTPCHCte,
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
		SQLName:     "cte_tpch_q22",
		Category:    benchCatTPCHCte,
		Description: "Q22 global sales opportunity: NOT EXISTS orders as a CTE anti-join over positive-balance customers",
		Database:    "tpch",
		SQL: `WITH cust_orders AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt FROM orders GROUP BY o_custkey)
SELECT c.c_nationkey, COUNT(*), SUM(c.c_acctbal), MAX(c.c_acctbal)
FROM customer AS c LEFT JOIN cust_orders ON cust_orders.k = c.c_custkey
WHERE cust_orders.order_cnt IS NULL AND c.c_acctbal > 0.00
GROUP BY c.c_nationkey
ORDER BY c.c_nationkey;`,
	},

	// ---------------------------------------------------------------
	// Official TPC-H formulations (MySQL only). Structure is faithful
	// (region/nation joins, correlated subqueries, HAVING, ORDER BY/
	// LIMIT); literals are adapted to the CLI-generated data where the
	// official ones would match nothing (see ronsql_cli_benchmarks.md).
	// ---------------------------------------------------------------
	{
		Name:        "tpch_q2_official",
		SQLName:     "tpch_q2",
		Category:    benchCatTPCHOfficial,
		MySQLOnly:   true,
		Description: "Q2 official: correlated MIN(ps_supplycost) over EUROPE, ORDER BY s_acctbal DESC LIMIT 100 (p_type LIKE '%STANDARD')",
		Database:    "tpch",
		SQL: `SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr,
       s.s_address, s.s_phone, s.s_comment
FROM part AS p, supplier AS s, partsupp AS ps, nation AS n, region AS r
WHERE p.p_partkey = ps.ps_partkey AND s.s_suppkey = ps.ps_suppkey
  AND p.p_size = 15 AND p.p_type LIKE '%STANDARD'
  AND s.s_nationkey = n.n_nationkey AND n.n_regionkey = r.r_regionkey
  AND r.r_name = 'EUROPE'
  AND ps.ps_supplycost = (
    SELECT MIN(ps2.ps_supplycost)
    FROM partsupp AS ps2, supplier AS s2, nation AS n2, region AS r2
    WHERE p.p_partkey = ps2.ps_partkey AND s2.s_suppkey = ps2.ps_suppkey
      AND s2.s_nationkey = n2.n_nationkey AND n2.n_regionkey = r2.r_regionkey
      AND r2.r_name = 'EUROPE')
ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey
LIMIT 100;`,
	},
	{
		Name:        "tpch_q11_official",
		SQLName:     "tpch_q11",
		Category:    benchCatTPCHOfficial,
		MySQLOnly:   true,
		Description: "Q11 official: GERMANY stock value with HAVING vs scalar subquery, ORDER BY value DESC",
		Database:    "tpch",
		SQL: `SELECT ps.ps_partkey, SUM(ps.ps_supplycost * ps.ps_availqty) AS value
FROM partsupp AS ps, supplier AS s, nation AS n
WHERE ps.ps_suppkey = s.s_suppkey AND s.s_nationkey = n.n_nationkey
  AND n.n_name = 'GERMANY'
GROUP BY ps.ps_partkey
HAVING SUM(ps.ps_supplycost * ps.ps_availqty) > (
  SELECT SUM(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
  FROM partsupp AS ps2, supplier AS s2, nation AS n2
  WHERE ps2.ps_suppkey = s2.s_suppkey AND s2.s_nationkey = n2.n_nationkey
    AND n2.n_name = 'GERMANY')
ORDER BY value DESC;`,
	},
	{
		Name:        "tpch_q13_official",
		SQLName:     "tpch_q13",
		Category:    benchCatTPCHOfficial,
		MySQLOnly:   true,
		Description: "Q13 official: orders-per-customer distribution histogram via derived table, ORDER BY custdist DESC",
		Database:    "tpch",
		SQL: `SELECT c_count, COUNT(*) AS custdist
FROM (SELECT c.c_custkey, COUNT(o.o_orderkey) AS c_count
      FROM customer AS c LEFT OUTER JOIN orders AS o
        ON c.c_custkey = o.o_custkey
       AND o.o_comment NOT LIKE '%special%requests%'
      GROUP BY c.c_custkey) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;`,
	},
	{
		Name:        "tpch_q15_official",
		SQLName:     "tpch_q15",
		Category:    benchCatTPCHOfficial,
		MySQLOnly:   true,
		Description: "Q15 official: revenue view (as CTE) with supplier join back to MAX(total_revenue), ORDER BY s_suppkey",
		Database:    "tpch",
		SQL: `WITH revenue0 AS (
  SELECT l_suppkey AS supplier_no,
         SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
  FROM lineitem
  WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
  GROUP BY l_suppkey)
SELECT s.s_suppkey, s.s_name, s.s_address, s.s_phone, r.total_revenue
FROM supplier AS s, revenue0 AS r
WHERE s.s_suppkey = r.supplier_no
  AND r.total_revenue = (SELECT MAX(total_revenue) FROM revenue0)
ORDER BY s.s_suppkey;`,
	},
	{
		Name:        "tpch_q22_official",
		SQLName:     "tpch_q22",
		Category:    benchCatTPCHOfficial,
		MySQLOnly:   true,
		Description: "Q22 official: NOT EXISTS anti-join over above-average-balance customers by phone country code",
		Database:    "tpch",
		SQL: `SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
FROM (SELECT SUBSTRING(c.c_phone, 1, 2) AS cntrycode, c.c_acctbal
      FROM customer AS c
      WHERE SUBSTRING(c.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c.c_acctbal > (
          SELECT AVG(c2.c_acctbal) FROM customer AS c2
          WHERE c2.c_acctbal > 0.00
            AND SUBSTRING(c2.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17'))
        AND NOT EXISTS (
          SELECT * FROM orders AS o WHERE o.o_custkey = c.c_custkey)
      ) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode;`,
	},
}

// findRonSQLBenchQuery returns the registered query with the given name in
// the .bench_ronsql namespace (case-insensitive), or nil. MySQLOnly entries
// are returned too so the caller can print a helpful redirection error.
func findRonSQLBenchQuery(name string) *RonSQLBenchQuery {
	for i := range ronsqlBenchQueries {
		if strings.EqualFold(ronsqlBenchQueries[i].Name, name) {
			return &ronsqlBenchQueries[i]
		}
	}
	return nil
}

// findSQLBenchQuery returns the registered query with the given name in the
// .bench_sql namespace (case-insensitive), or nil.
func findSQLBenchQuery(name string) *RonSQLBenchQuery {
	for i := range ronsqlBenchQueries {
		if strings.EqualFold(ronsqlBenchQueries[i].sqlBenchName(), name) {
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
		if !q.MySQLOnly {
			items = append(items, readline.PcItem(q.Name))
		}
	}
	return items
}

// sqlBenchCompletions returns completer items for the named .bench_sql form.
func sqlBenchCompletions() []readline.PrefixCompleterInterface {
	items := []readline.PrefixCompleterInterface{
		readline.PcItem("list"),
		readline.PcItem("all"),
	}
	for _, q := range ronsqlBenchQueries {
		items = append(items, readline.PcItem(q.sqlBenchName()))
	}
	return items
}

// printBenchQueryCategory prints all registry entries of one category,
// using the given namespace naming.
func printBenchQueryCategory(category string, sqlNames bool) {
	for _, q := range ronsqlBenchQueries {
		if q.Category != category {
			continue
		}
		name := q.Name
		if sqlNames {
			name = q.sqlBenchName()
		}
		suffix := ""
		if q.MySQLOnly && !sqlNames {
			suffix = " [.bench_sql only]"
		}
		fmt.Printf("    %-20s %s%s\n", name, q.Description, suffix)
	}
}

// listRonSQLBenchQueries prints the registry in the .bench_ronsql namespace.
func (s *Shell) listRonSQLBenchQueries() {
	fmt.Println()
	fmt.Println(ui.Info("RonSQL benchmark queries (run with .bench_ronsql <name> [T] [N]):"))
	fmt.Println()
	fmt.Println("  Online Feature-Store-style (filter-bounded CTE joins):")
	printBenchQueryCategory(benchCatFS, false)
	fmt.Println()
	fmt.Println("  Offline Feature-Store-style (full-table feature materialization):")
	printBenchQueryCategory(benchCatOfflineFS, false)
	fmt.Println()
	fmt.Println("  TPC-H rewritten with CTEs:")
	printBenchQueryCategory(benchCatTPCHCte, false)
	fmt.Println()
	fmt.Println("    all                  Run every RonSQL-capable query sequentially")
	fmt.Println()
	fmt.Println("  Queries run against the tpch database - run .load_tpch first.")
	fmt.Println("  Defaults: T=1 thread, N=10 requests per thread.")
	fmt.Println("  Run the same queries through the MySQL server with .bench_sql <name>.")
	fmt.Println("  Inspect a query with .query_ronsql <name>, explain it with .explain_ronsql <name>.")
	fmt.Println()
}

// listSQLBenchQueries prints the registry in the .bench_sql namespace.
func (s *Shell) listSQLBenchQueries() {
	fmt.Println()
	fmt.Println(ui.Info("SQL benchmark queries (run with .bench_sql <name> [T] [N], executed by the MySQL server):"))
	fmt.Println()
	fmt.Println("  Online Feature-Store-style (filter-bounded CTE joins):")
	printBenchQueryCategory(benchCatFS, true)
	fmt.Println()
	fmt.Println("  Offline Feature-Store-style (full-table feature materialization):")
	printBenchQueryCategory(benchCatOfflineFS, true)
	fmt.Println()
	fmt.Println("  TPC-H rewritten with CTEs (identical SQL to .bench_ronsql tpch_q*):")
	printBenchQueryCategory(benchCatTPCHCte, true)
	fmt.Println()
	fmt.Println("  TPC-H official formulations:")
	printBenchQueryCategory(benchCatTPCHOfficial, true)
	fmt.Println()
	fmt.Println("    all                  Run every query sequentially")
	fmt.Println()
	fmt.Println("  Queries run against the tpch database - run .load_tpch first.")
	fmt.Println("  Defaults: T=1 thread, N=10 requests per thread.")
	fmt.Println("  Compare with .bench_ronsql <name> for RonSQL pushdown execution.")
	fmt.Println("  Inspect a query with .query_sql <name>, explain it with .explain_sql <name>.")
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

// pickBenchKey picks a random key so that [key, key+KeySpan) stays within
// [1, maxKey].
func pickBenchKey(q *RonSQLBenchQuery, rng *rand.Rand, maxKey int) int {
	upper := maxKey - q.KeySpan
	if upper < 1 {
		upper = 1
	}
	return rng.Intn(upper) + 1
}

// substituteBenchKey replaces the {KEY} / {KEY2} placeholders with the given
// key and key+KeySpan.
func substituteBenchKey(q *RonSQLBenchQuery, key int) string {
	sql := strings.ReplaceAll(q.SQL, "{KEY2}", strconv.Itoa(key+q.KeySpan))
	return strings.ReplaceAll(sql, "{KEY}", strconv.Itoa(key))
}

// buildRonSQLBenchSQL substitutes the {KEY} / {KEY2} placeholders if present.
func buildRonSQLBenchSQL(q *RonSQLBenchQuery, rng *rand.Rand, maxKey int) string {
	if !q.RandKey {
		return q.SQL
	}
	return substituteBenchKey(q, pickBenchKey(q, rng, maxKey))
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

// benchProgressReporter starts a goroutine printing progress every 10s.
// Returns a stop function.
func benchProgressReporter(totalOps int, doneOps *int64, errorCollector *ErrorCollector, benchStart time.Time) func() {
	stopProgress := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ops := atomic.LoadInt64(doneOps)
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
	return func() { close(stopProgress) }
}

// printBenchResults prints the common completion report.
func printBenchResults(label, name string, doneOps int64, benchDuration time.Duration,
	latencyCollector *LatencyCollector, errorCollector *ErrorCollector) {
	minLat, maxLat, avgLat, p95Lat, p99Lat, p999Lat, _ := latencyCollector.GetTotalStats()
	fmt.Println()
	opsPerSec := float64(doneOps) / benchDuration.Seconds()
	fmt.Println(ui.Success(fmt.Sprintf("%s Benchmark %s completed in %.2fs", label, name, benchDuration.Seconds())))
	fmt.Printf("   Requests: %d (errors: %d)\n", doneOps, errorCollector.Count())
	fmt.Printf("   Throughput: %.2f queries/sec\n", opsPerSec)
	fmt.Printf("   Latency: min=%s avg=%s max=%s p95=%s p99=%s p99.9=%s\n",
		formatLatency(minLat), formatLatency(avgLat), formatLatency(maxLat),
		formatLatency(p95Lat), formatLatency(p99Lat), formatLatency(p999Lat))
	errorCollector.PrintErrors()
	fmt.Println()
}

// runBenchRonSQL dispatches a named RonSQL benchmark (or "all"/"list").
func (s *Shell) runBenchRonSQL(name string, numThreads, numOps int) error {
	if name == "" || strings.EqualFold(name, "list") {
		s.listRonSQLBenchQueries()
		return nil
	}
	if strings.EqualFold(name, "all") {
		var failed []string
		total := 0
		for i := range ronsqlBenchQueries {
			q := &ronsqlBenchQueries[i]
			if q.MySQLOnly {
				continue
			}
			total++
			if err := s.runBenchRonSQLQuery(q, numThreads, numOps); err != nil {
				fmt.Println(ui.Error(fmt.Sprintf("Benchmark %s failed: %v", q.Name, err)))
				failed = append(failed, q.Name)
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("%d of %d benchmarks failed: %s",
				len(failed), total, strings.Join(failed, ", "))
		}
		return nil
	}
	q := findRonSQLBenchQuery(name)
	if q == nil {
		s.listRonSQLBenchQueries()
		return fmt.Errorf("unknown RonSQL benchmark query: %s", name)
	}
	if q.MySQLOnly {
		return fmt.Errorf("%s uses ORDER BY/LIMIT outside the RonSQL envelope; run it with .bench_sql %s",
			q.Name, q.sqlBenchName())
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
			Timeout: ronsqlBenchTimeout(),
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
	stopProgress := benchProgressReporter(totalOps, &doneOps, errorCollector, benchStart)

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
	stopProgress()
	benchDuration := time.Since(benchStart)

	printBenchResults("RonSQL", q.Name, doneOps, benchDuration, latencyCollector, errorCollector)
	return nil
}

// runBenchSQLNamed dispatches a named MySQL benchmark (or "all"/"list").
// This is the comparative twin of runBenchRonSQL: the same queries executed
// by the MySQL server instead of RonSQL pushdown.
func (s *Shell) runBenchSQLNamed(name string, numThreads, numOps int) error {
	if name == "" || strings.EqualFold(name, "list") {
		s.listSQLBenchQueries()
		return nil
	}
	if strings.EqualFold(name, "all") {
		var failed []string
		for i := range ronsqlBenchQueries {
			q := &ronsqlBenchQueries[i]
			if err := s.runBenchSQLQuery(q, numThreads, numOps); err != nil {
				fmt.Println(ui.Error(fmt.Sprintf("Benchmark %s failed: %v", q.sqlBenchName(), err)))
				failed = append(failed, q.sqlBenchName())
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("%d of %d benchmarks failed: %s",
				len(failed), len(ronsqlBenchQueries), strings.Join(failed, ", "))
		}
		return nil
	}
	q := findSQLBenchQuery(name)
	if q == nil {
		s.listSQLBenchQueries()
		return fmt.Errorf("unknown SQL benchmark query: %s", name)
	}
	return s.runBenchSQLQuery(q, numThreads, numOps)
}

// runBenchSQLQuery runs one named query numThreads x numOps times through
// the MySQL server and reports throughput and latency percentiles.
func (s *Shell) runBenchSQLQuery(q *RonSQLBenchQuery, numThreads, numOps int) error {
	if s.mysqlClient == nil {
		return fmt.Errorf("MySQL not connected. SQL benchmarks require MySQL.")
	}

	totalOps := numThreads * numOps
	name := q.sqlBenchName()

	fmt.Println()
	fmt.Println(ui.Info(fmt.Sprintf("SQL Benchmark %s (MySQL server): %d threads × %d requests = %d total requests",
		name, numThreads, numOps, totalOps)))
	fmt.Println(ui.Info(q.Description))
	fmt.Println()
	fmt.Println(strings.TrimSpace(q.SQL))
	fmt.Println()

	maxKey := 0
	if q.RandKey {
		maxKey = s.resolveRonSQLBenchKeyRange(q)
		fmt.Println(ui.Info(fmt.Sprintf("Random key range: 1..%d", maxKey)))
	}

	// Create one MySQL connection per thread, with the query's database as
	// the default schema so unqualified table names resolve.
	clients := make([]*client.MySQLClient, numThreads)
	for i := 0; i < numThreads; i++ {
		c, err := client.NewMySQLClientWithOptions(client.MySQLOptions{
			Host:     s.config.MySQLHost,
			Port:     s.config.MySQLPort,
			User:     s.mysqlUser,
			Password: s.mysqlPass,
			TLS:      s.config.TLS,
			Database: q.Database,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				clients[j].Close()
			}
			return fmt.Errorf("failed to create MySQL client for thread %d: %w", i, err)
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

	warmupRng := rand.New(rand.NewSource(1))

	// Warmup: run the query once to validate it and warm server caches.
	_, warmupRows, warmupDur, err := clients[0].Query(buildRonSQLBenchSQL(q, warmupRng, maxKey))
	if err != nil {
		return fmt.Errorf("warmup query failed: %w", err)
	}
	fmt.Println(ui.Info(fmt.Sprintf("Warmup: %s, %d result rows", formatLatency(warmupDur), len(warmupRows))))
	fmt.Println()

	var doneOps int64
	var wg sync.WaitGroup
	latencyCollector := NewLatencyCollector()
	errorCollector := NewErrorCollector()
	benchStart := time.Now()
	stopProgress := benchProgressReporter(totalOps, &doneOps, errorCollector, benchStart)

	debugMode := s.debug
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadID int, mysqlClient *client.MySQLClient) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(threadID)*100003 + 7))
			for i := 0; i < numOps; i++ {
				_, _, duration, err := mysqlClient.Query(buildRonSQLBenchSQL(q, rng, maxKey))
				latencyCollector.Record(duration)
				if debugMode {
					fmt.Printf("[DEBUG] %s op %d: %s err=%v\n", name, i, formatLatency(duration), err)
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
	stopProgress()
	benchDuration := time.Since(benchStart)

	printBenchResults("SQL", name, doneOps, benchDuration, latencyCollector, errorCollector)
	return nil
}
