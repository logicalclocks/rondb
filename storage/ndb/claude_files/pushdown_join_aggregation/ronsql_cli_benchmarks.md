# RonSQL CLI Benchmarks (`.bench_ronsql`)

Benchmarks for RonSQL pushdown-aggregation / CTE queries, run from the
`rondb-cli` interactive shell against the TPC-H data model. The goal is to
measure end-to-end RonSQL query latency (microsecond precision) for
Feature-Store-style workloads and for TPC-H queries rewritten with CTEs,
as a foundation for understanding where time is spent in RonSQL phases and
in the RonDB data nodes.

## Implementation

| File | What |
|------|------|
| `tools/rondb-cli/internal/shell/ronsql_bench.go` | Query registry (`ronsqlBenchQueries`), runner (`runBenchRonSQL*`), completion |
| `tools/rondb-cli/internal/shell/repl.go` | `.bench_ronsql` dispatch case, `.help internal` text, autocomplete |
| `tools/rondb-cli/internal/shell/tpch.go` | Secondary indexes added to the TPC-H DDL (see below) |
| `tools/rondb-cli/internal/client/rest.go` | `RestOptions.Timeout` (benchmark clients use 300s instead of the default 30s) |

Queries execute over the RDRS REST endpoint `POST /0.2.0/ronsql` with
`database: "tpch"`, `outputFormat: TEXT`. The recorded latency is the
wall-clock time around the HTTP request (`RestClient.Post`), i.e. it includes
RDRS + RonSQL parse/prepare/execute + data-node execution + result transfer,
but not JSON marshalling in the CLI. The existing `LatencyCollector`
infrastructure reports min/avg/max and p95/p99/p99.9; values under 1 ms print
in microseconds.

## Usage

### Starting a benchmark cluster via MTR

`mysql-test/suite/rdrs2-ronsqltpch/t/ronsql_bench_setup.{test,cnf,result}`
provides a one-command cluster (mysqld + ndbmtd + RDRS, auth disabled) with a
benchmark-scale memory config (`TotalMemoryConfig=10G`, `DataMemory=4G`,
mirroring `suite/ndb/t/ndb_setup_large.cnf`):

```
cd mysql-test
./mtr --suite=rdrs2-ronsqltpch ronsql_bench_setup --start-and-exit
grep ServerPort var/rdrs.1.1_config.json   # RDRS REST port (no-TLS instance)
```

Connect the CLI with `--mysql-port <MASTER_MYPORT> --rdrs-port <ServerPort>
--no-rondis`.

### Running the benchmarks

```
cd tools/rondb-cli && go build -o rondb .   # build
./rondb                                     # start shell

.load_tpch 1 8 200          # load TPC-H scale factor 1 with 8 threads
.bench_ronsql                # list queries
.bench_ronsql fs_point 4 100 # 4 threads x 100 requests
.bench_ronsql tpch_q15       # default: 1 thread x 10 requests
.bench_ronsql all 1 10       # all queries sequentially
```

Every benchmark starts with a **warmup request** that validates the query
(unsupported shapes fail immediately with the RonSQL error message), primes
dictionary caches, and prints the warmup latency + result row count. The
timed run follows.

`fs_point` substitutes a random customer key per request (`{KEY}`
placeholder); the key range is discovered via
`SELECT MAX(c_custkey) FROM tpch.customer` over the MySQL connection at
benchmark start (fallback: 150000).

**Important:** if the `tpch` database was loaded with an older CLI build, run
`.drop_tpch` and reload — the DDL now includes secondary indexes
(`orders(o_custkey)`, `lineitem(l_suppkey)`, `lineitem(l_shipdate)`,
`supplier(s_nationkey)`, `customer(c_nationkey)`) that the CTE-body
index-scan planning (Phase I.9) relies on. `CREATE TABLE IF NOT EXISTS`
silently skips existing tables.

## Data model

The CLI loader (`.load_tpch [SF]`) creates database `tpch` with the 8
standard TPC-H tables (unprefixed names, no `o_orderyear`). At SF=1:
150k customers, 10k suppliers, 200k parts, 800k partsupp, 1.5M orders,
6M lineitems. Order keys are sparse (multiples of 4); `o_custkey` is
uniformly random, ~10 orders/customer; `o_orderdate` spans 1992-01-01 to
~1998-08; ~4 lineitems/order. Note this is a different physical schema from
`block_unit_test/load_tpch.cpp` (which uses `test.tpch_*` tables).

## Query set

All queries respect the RonSQL CTE envelope (see
`cte_test_authoring_guide.md`): aggregating main SELECT, complete-key CTE
equijoins, no ORDER BY / LIMIT / HAVING / AVG / DISTINCT. `SUM(DECIMAL)` in
CTE bodies is used freely — the D1 fix widens scale-0 to exact BIGINT and
scale>0 to DOUBLE.

### Feature-Store-style (`fs_*`)

These mimic feature-engineering workloads: CTEs compute per-entity aggregate
features (single table or a simple join) that are joined to an entity table
via scans and key lookups.

| Name | Shape | What it measures |
|------|-------|-------------------|
| `fs_point` | CTE body filtered `o_custkey = <random>`, scalar main agg | On-demand feature vector for one entity; index scan in CTE body + minimal aggregation. The online-feature-serving latency proxy |
| `fs_batch` | Per-customer CTE (COUNT/SUM/MAX over 1.5M orders) joined to 150k customers, `GROUP BY c_mktsegment` | Full batch feature materialization: large CTE + complete-key lookup join + re-aggregation |
| `fs_multi` | Two CTEs (lifetime stats + date-filtered recent stats) both joined to customer | Multiple feature groups joined to one entity table (J10/J11 shape) |
| `fs_join_body` | CTE body = `lineitem JOIN orders` with WHERE, `GROUP BY l_suppkey`; joined to supplier | Aggregate feature over a simple join inside the CTE body. **Exploratory**: real-table⋈real-table CTE bodies are grammatically supported but not covered by existing MTR; the warmup run reports cleanly if the shape is rejected |
| `fs_anti` | `customer LEFT JOIN recent-orders-CTE WHERE cnt IS NULL, GROUP BY nation` | Churn detection: anti-join (Phase K) + NULL-injection |
| `fs_scalar` | Scalar `COUNT/SUM/MAX/MIN` reduce over the per-customer CTE | Pure CTE materialization + redistribution cost, no parent join (Form B baseline) |

### TPC-H with CTEs (`tpch_*`)

Five TPC-H queries whose official formulation contains a subquery or derived
table, rewritten as CTEs. Deviations from the official queries (needed to
stay in the envelope) are listed per query.

| Name | Original subquery construct | Deviations |
|------|------------------------------|------------|
| `tpch_q2` | Correlated `ps_supplycost = (SELECT MIN(ps_supplycost)...)` → per-part `min_cost` CTE | The final self-join back to the supplier achieving the min cost needs a col-vs-CTE-output equijoin (unsupported); instead reports min/max min-cost per manufacturer for size-15 parts. Region/type filters dropped (kept: `p_size = 15`) |
| `tpch_q11` | `HAVING SUM(...) > (SELECT SUM(...) * fraction)` → per-supplier `stock` CTE | HAVING-vs-scalar comparison unsupported; stock value uses `SUM(ps_availqty)` (exact int) instead of `ps_supplycost * ps_availqty` (expression-of-DECIMAL SUM in CTE body unproven). Nation filter as main-query WHERE `s_nationkey = 7` (GERMANY) |
| `tpch_q13` | Derived table `(SELECT c_custkey, COUNT(o_orderkey) ... LEFT JOIN ... GROUP BY)` → `c_orders` CTE | The outer `GROUP BY c_count` (distribution histogram) groups by an aggregate output (unsupported); reports scalar distribution stats (COUNT/SUM/MIN/MAX) over the LEFT JOIN with NULL injection instead |
| `tpch_q15` | `revenue` view + `total_revenue = (SELECT MAX(total_revenue)...)` → date-filtered `revenue` CTE, main SELECT is the scalar MAX | Revenue is `SUM(l_extendedprice)` instead of `SUM(l_extendedprice * (1 - l_discount))` (expression-of-DECIMAL SUM in CTE body unproven); the supplier join back to the max is the same col-vs-CTE-output limitation as Q2 |
| `tpch_q22` | `NOT EXISTS (SELECT * FROM orders ...)` + scalar AVG subquery → `cust_orders` CTE anti-join | Faithful anti-join via `LEFT JOIN ... IS NULL`; the `c_acctbal > (SELECT AVG(...))` threshold is a fixed `> 0.00`; phone-prefix `SUBSTRING` filter replaced by `GROUP BY c_nationkey` |

## Known risks / scaling notes

- **High-cardinality CTE group keys**: `tpch_q2` groups by `ps_partkey`
  (200k × SF groups) and the per-customer CTEs produce 150k × SF groups at
  SF=1. The `ronsql_large` regression net proves 20k groups; the D6 DBSPJ
  assert (high-cardinality CTE, not yet fixed) may bite at larger SF. Start
  at SF=1; treat crashes at higher SF as kernel findings, not benchmark bugs.
- **`fs_join_body`** is deliberately exploratory (see table above).
- The REST timeout for benchmark clients is 300s; very large SFs may still
  need more for the coldest full-scan queries.

## Next steps: phase timing

These benchmarks measure end-to-end latency only. To break down microseconds
per phase, the natural follow-ups are:

1. **RonSQL-side**: instrument `RonSQLPreparer` / executor with per-phase
   timestamps (parse, prepare/plan, NDB dictionary access, execute,
   result-format) and return them in the RDRS response (e.g. an optional
   `"stats": true` request field); the CLI benchmark can then aggregate
   per-phase percentiles.
2. **Data-node-side**: correlate with existing counters (`m_rows_examined`,
   ScanFragConf `rowsExamined`) and add DBSPJ/DBLQH timing around
   CTE_SCAN/CTE_LOOKUP materialization, JOIN_AGG_COMPLETE merge, and
   redistribution, exposed via ndbinfo or DUMP.
