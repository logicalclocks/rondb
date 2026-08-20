# RonSQL CLI Benchmarks (`.bench_ronsql` / `.bench_sql <name>`)

Benchmarks for RonSQL pushdown-aggregation / CTE queries, run from the
`rondb-cli` interactive shell against the TPC-H data model. The goal is to
measure end-to-end RonSQL query latency (microsecond precision) for
Feature-Store-style workloads and for TPC-H queries rewritten with CTEs,
as a foundation for understanding where time is spent in RonSQL phases and
in the RonDB data nodes.

`.bench_sql <name>` is the comparative twin: the same named queries executed
by the **MySQL server** over the same tables. Three comparisons matter:

1. `.bench_ronsql X` vs `.bench_sql X` — RonSQL pushdown vs MySQL on
   identical SQL. If RonSQL is slower, there is a RonSQL performance issue
   to investigate.
2. `.bench_sql cte_tpch_qN` vs `.bench_sql tpch_qN` — the CTE rewrite vs the
   official TPC-H formulation, both on MySQL: the cost/benefit of the
   rewrite itself.
3. `.bench_ronsql tpch_qN` vs `.bench_sql tpch_qN` — RonSQL's best shape vs
   MySQL's official shape: the end-user view.

## Implementation

| File | What |
|------|------|
| `tools/rondb-cli/internal/shell/ronsql_bench.go` | Shared query registry (`ronsqlBenchQueries`), RonSQL runner (`runBenchRonSQL*`), MySQL runner (`runBenchSQLNamed`/`runBenchSQLQuery`), completion |
| `tools/rondb-cli/internal/shell/query_inspect.go` | Query inspection: `.list_query_ronsql`/`.list_query_sql`, `.query_ronsql`/`.query_sql <name>`, `.explain_ronsql`/`.explain_sql <name>` |
| `tools/rondb-cli/internal/shell/repl.go` | `.bench_ronsql` dispatch, `.bench_sql` named-form dispatch (non-numeric first arg), query-inspection dispatch, `.help internal` / `.help bench` text, autocomplete |
| `tools/rondb-cli/internal/shell/tpch.go` | Secondary indexes added to the TPC-H DDL (see below) |
| `tools/rondb-cli/internal/client/rest.go` | `RestOptions.Timeout` (benchmark clients use a build-time timeout instead of the default 30s) |
| `tools/rondb-cli/internal/client/mysql.go` | `MySQLOptions.Database` (per-benchmark connections default to the query's schema) |

RonSQL queries execute over the RDRS REST endpoint `POST /0.2.0/ronsql` with
`database: "tpch"`, `outputFormat: TEXT`; recorded latency is the wall-clock
time around the HTTP request (RDRS + RonSQL parse/prepare/execute +
data-node execution + result transfer). SQL queries execute over per-thread
MySQL connections (`database/sql` + go-sql-driver); recorded latency is the
wall-clock time around query execution including full result drain. The
shared `LatencyCollector` reports min/avg/max and p95/p99/p99.9; values
under 1 ms print in microseconds.

Both runners share the registry, {KEY}/{KEY2} substitution, warmup,
progress reporting, and the results format.

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

.load_tpch 1 8 200            # load TPC-H scale factor 1 with 8 threads
.bench_ronsql                 # list RonSQL queries
.bench_sql list               # list SQL (MySQL-executed) queries
.bench_ronsql fs_point 4 100  # 4 threads x 100 requests via RonSQL
.bench_sql fs_point 4 100     # the same query via the MySQL server
.bench_sql cte_tpch_q15       # Q15 CTE rewrite via MySQL
.bench_sql tpch_q15           # official TPC-H Q15 via MySQL
.bench_ronsql all 1 10        # all RonSQL-capable queries sequentially
.bench_sql all 1 10           # all queries via MySQL sequentially
```

`.bench_sql` keeps its original numeric form for the key-value benchmark
(`.bench_sql [T] [N] [R] [W]`); a non-numeric first argument selects the
named analytics form (`.bench_sql <name> [T] [N]`, default 1 thread x 10
requests).

Every benchmark starts with a **warmup request** that validates the query
(unsupported RonSQL shapes fail immediately with the RonSQL error message),
primes caches, and prints the warmup latency + result row count. The timed
run follows.

Queries with `RandKey` substitute a random key per request (`{KEY}`
placeholder; `{KEY2}` = `{KEY}` + span for segment queries). The key range
is discovered via `KeySQL` over the MySQL connection at benchmark start.

### Inspecting the queries

```
.list_query_ronsql            # query names + descriptions (RonSQL namespace)
.list_query_sql               # query names + descriptions (SQL namespace)
.query_ronsql fs_batch        # print the SQL fs_batch represents
.query_sql tpch_q15           # print the official Q15 SQL
.explain_ronsql fs_batch      # RonSQL EXPLAIN (plan only, not executed)
.explain_sql tpch_q15         # MySQL EXPLAIN + SHOW WARNINGS
```

`.query_*` prints the registry entry (category, database, cross-namespace
name, SQL template, `{KEY}`/`{KEY2}` semantics). `.explain_*` substitutes a
deterministic sample key (the same seed-1 key the benchmark warmup uses) and
asks the engine for its plan: `.explain_ronsql` posts the unmodified query
with `explainMode: FORCE` and `outputFormat: TEXT` (RonSQL prints the plan
without executing; JSON EXPLAIN output is not implemented), while
`.explain_sql` runs `EXPLAIN` on a dedicated connection with the query's
schema as default database, then `SHOW WARNINGS` — where NDB reports
pushed-join status and the reasons a join could not be pushed. MySQLOnly
entries are rejected by `.explain_ronsql` with a redirect to `.explain_sql`.

**Important:** if the `tpch` database was loaded with an older CLI build, run
`.drop_tpch` and reload — the DDL now includes secondary indexes
(`orders(o_custkey)`, `orders(o_orderdate)`, `lineitem(l_suppkey)`,
`lineitem(l_shipdate)`, `supplier(s_nationkey)`, `customer(c_nationkey)`)
that the CTE-body index-scan planning (Phase I.9) and the recent-window fs
queries rely on. `CREATE TABLE IF NOT EXISTS` silently skips existing
tables.

## Data model

The CLI loader (`.load_tpch [SF]`) creates database `tpch` with the 8
standard TPC-H tables (unprefixed names). At SF=1: 150k customers, 10k
suppliers, 200k parts, 800k partsupp, 1.5M orders, 6M lineitems, plus fixed
nation (25) and region (5). Order keys are sparse (multiples of 4);
`o_custkey` is uniformly random, ~10 orders/customer; `o_orderdate` spans
1992-01-01 to ~1998-08 (~625 orders/day, ~2500 lineitems/day); ~4
lineitems/order. Phone country codes are uniform in 10..34 (not
nation-derived). Note this is a different physical schema from
`block_unit_test/load_tpch.cpp` (which uses `test.tpch_*` tables).

## Query set

RonSQL-capable queries respect the RonSQL CTE envelope (see
`cte_test_authoring_guide.md`): aggregating main SELECT, complete-key CTE
equijoins, no HAVING / AVG / DISTINCT. ORDER BY (targets must be GROUP BY
columns or aggregate output aliases) and LIMIT ARE supported on aggregate
main SELECTs — the sort/limit runs in the RDRS ResultPrinter over the
aggregated groups (`ronsql_orderby_limit_plan.md` has the full shape
inventory; projection-only and single-table non-aggregate shapes remain
out). `SUM(DECIMAL)` in CTE bodies is used freely — the D1 fix widens
scale-0 to exact BIGINT and scale>0 to DOUBLE.

### Online Feature-Store-style (`fs_*`)

Online serving workloads: CTEs compute per-entity aggregate features joined
to entity tables, with **filters bounding the work** to hundreds .. tens of
thousands of source rows (RonDB is an online feature store; full-table
sweeps belong in `offline_fs_*`).

| Name | Shape | Source rows (SF=1) | Engines |
|------|-------|--------------------|---------|
| `fs_floor` | Single-table `COUNT(*)` over region (5 rows) — fixed-overhead floor; the denominator for phase-timing analysis | 5 | both |
| `fs_point` | CTE body filtered `o_custkey = <random>`, scalar main agg | ~10 orders | both |
| `fs_batch` | Per-entity feature vectors for a random 100-customer segment ({KEY}/{KEY2} range in CTE body + main WHERE), `GROUP BY c_custkey` | ~1k orders, 100 output rows | both |
| `fs_freshness` | Two CTEs (lifetime + last-order) over a random 500-customer segment, joined to the same customer range | ~10k orders | both |
| `fs_supplier` | Per-supplier features over a 3-day `l_shipdate` window (index scan), joined to one random nation's suppliers | ~7k lineitems, ~400 suppliers | both |
| `fs_nation` | Recent-window (`o_orderdate >= 1998-06-01`, index scan) per-customer CTE joined to one nation's customers, `GROUP BY c_mktsegment` | ~40k orders | both |
| `fs_topk` | Recent-spend CTE joined to customer in aggregate form (`GROUP BY c_custkey, c_name`; MAX = identity per unique key), `ORDER BY top_spend DESC LIMIT 100` | thousands | both |
| `fs_minmax` | Datatype-heavy MIN/MAX (DECIMAL prices, DATE ship dates, DECIMAL qty) over a 3-day `l_shipdate` window, re-aggregated per supplier nation | ~7k lineitems | both |
| `fs_dnf` | 4-disjunct DNF in the CTE body WHERE (`o_orderstatus`/`o_totalprice`/`o_orderpriority`) over a random 200-customer segment — interpreter filter cost | ~2k orders | both |
| `fs_history` | Order-history page for a 200-customer segment, `ORDER BY o_orderdate DESC LIMIT 1000` | ~2k orders | **MySQL only** |

`fs_topk` was rewritten from its natural projection-only form into the
equivalent aggregate form: `c_custkey` is unique, so grouping by
`(c_custkey, c_name)` yields one group per customer and `MAX()` is the
identity — same result set, but inside the aggregate ORDER BY/LIMIT
envelope. The projection-only original needs `ronsql_orderby_limit_plan.md`
Phase 3. `fs_history` is a single-table non-aggregate SELECT (Phase 4
there); `.bench_ronsql` rejects it with a pointer to `.bench_sql`. Note the
aggregate-path LIMIT is a print-time cutoff, not a scan reduction (early
close is Phase N / I.14).

### Offline Feature-Store-style (`offline_fs_*`)

Offline feature materialization: full-table per-entity CTEs re-aggregated
across the entity table (the former `fs_batch`/`fs_multi`/`fs_join_body`/
`fs_anti`/`fs_scalar`, renamed).

| Name | Shape |
|------|-------|
| `offline_fs_batch` | Per-customer CTE (COUNT/SUM/MAX over 1.5M orders) joined to 150k customers, `GROUP BY c_mktsegment` |
| `offline_fs_multi` | Two CTEs (lifetime stats + date-filtered recent stats) both joined to customer |
| `offline_fs_join_body` | CTE body = `lineitem JOIN orders` with WHERE, `GROUP BY l_suppkey`; joined to supplier. **Exploratory**: real-table⋈real-table CTE bodies are grammatically supported but not covered by existing MTR |
| `offline_fs_anti` | `customer LEFT JOIN recent-orders-CTE WHERE cnt IS NULL, GROUP BY nation` (churn detection, Phase K anti-join) |
| `offline_fs_wide` | 7-aggregate per-customer CTE re-aggregated into 10 output columns `GROUP BY c_mktsegment` (result-width cost) |
| `offline_fs_chain` | Three-level chained CTE-of-CTE (per-customer → rollup → scalar reduce): CTE materialization + redistribute cost per chain phase |
| `offline_fs_scalar` | Scalar `COUNT/SUM/MAX/MIN` reduce over the per-customer CTE (pure materialization + redistribution cost) |

### TPC-H with CTEs (`tpch_q*` in `.bench_ronsql`, `cte_tpch_q*` in `.bench_sql`)

Five TPC-H queries whose official formulation contains a subquery or derived
table, rewritten as CTEs. Deviations from the official queries (needed to
stay in the envelope) are listed per query.

| Name | Original subquery construct | Deviations |
|------|------------------------------|------------|
| `tpch_q2` | Correlated `ps_supplycost = (SELECT MIN(ps_supplycost)...)` → per-part `min_cost` CTE | The final self-join back to the supplier achieving the min cost needs a col-vs-CTE-output equijoin (unsupported); instead reports min/max min-cost per manufacturer for size-15 parts. Region/type filters dropped (kept: `p_size = 15`). `ORDER BY p_mfgr LIMIT 100` mirrors the official ordering + LIMIT |
| `tpch_q11` | `HAVING SUM(...) > (SELECT SUM(...) * fraction)` → per-supplier `stock` CTE | HAVING-vs-scalar comparison unsupported; stock value uses `SUM(ps_availqty)` (exact int) instead of `ps_supplycost * ps_availqty` (expression-of-DECIMAL SUM in CTE body unproven). Nation filter as main-query WHERE `s_nationkey = 7` (GERMANY) |
| `tpch_q13` | Derived table `(SELECT c_custkey, COUNT(o_orderkey) ... LEFT JOIN ... GROUP BY)` → `c_orders` CTE | The outer `GROUP BY c_count` (distribution histogram) groups by an aggregate output (unsupported); reports scalar distribution stats (COUNT/SUM/MIN/MAX) over the LEFT JOIN with NULL injection instead |
| `tpch_q15` | `revenue` view + `total_revenue = (SELECT MAX(total_revenue)...)` → date-filtered `revenue` CTE, main SELECT is the scalar MAX | Revenue is `SUM(l_extendedprice)` instead of `SUM(l_extendedprice * (1 - l_discount))` (expression-of-DECIMAL SUM in CTE body unproven); the supplier join back to the max is the same col-vs-CTE-output limitation as Q2 |
| `tpch_q22` | `NOT EXISTS (SELECT * FROM orders ...)` + scalar AVG subquery → `cust_orders` CTE anti-join | Faithful anti-join via `LEFT JOIN ... IS NULL`; the `c_acctbal > (SELECT AVG(...))` threshold is a fixed `> 0.00`; phone-prefix `SUBSTRING` filter replaced by `GROUP BY c_nationkey`. `ORDER BY c_nationkey` mirrors the official `ORDER BY cntrycode` |

### Official TPC-H (`tpch_q*` in `.bench_sql`, MySQL only)

The official formulations — region/nation joins, correlated subqueries,
HAVING-vs-scalar, derived tables, ORDER BY/LIMIT — with literals adapted to
the CLI-generated data where the official values would match nothing:

- **Q2**: `p_type LIKE '%STANDARD'` instead of `'%BRASS'` (the generator's
  type vocabulary is material+type from
  ANODIZED/BURNISHED/PLATED/POLISHED/BRUSHED × STANDARD/SMALL/MEDIUM/LARGE/
  ECONOMY/PROMO). Region EUROPE, `p_size = 15`, ORDER BY + LIMIT 100 kept.
- **Q11**: faithful (GERMANY via nation join, `ps_supplycost * ps_availqty`
  value, HAVING vs scalar subquery with fraction 0.0001, ORDER BY value).
- **Q13**: faithful shape; `o_comment NOT LIKE '%special%requests%'` is kept
  even though the generated comments are random words (the filter passes
  ~everything — the join/aggregation work is what is measured).
- **Q15**: the revenue view expressed as a CTE referenced twice (view
  creation per request would not benchmark the query); otherwise faithful
  including the MAX(total_revenue) join-back and ORDER BY.
- **Q22**: faithful (phone-prefix country codes 13/31/23/29/30/18/17 all
  exist in the generator's uniform 10..34 range; AVG subquery + NOT EXISTS
  anti-join + GROUP BY cntrycode).

## Known risks / scaling notes

- **High-cardinality CTE group keys**: `tpch_q2` groups by `ps_partkey`
  (200k × SF groups) and the per-customer CTEs produce 150k × SF groups at
  SF=1. The `ronsql_large` regression net proves 20k groups; the D6 DBSPJ
  assert (high-cardinality CTE, not yet fixed) may bite at larger SF. Start
  at SF=1; treat crashes at higher SF as kernel findings, not benchmark bugs.
- **`offline_fs_join_body`** is deliberately exploratory (see table above).
- The REST timeout for benchmark clients defaults to 120s (300s for debug
  builds, injected at build time); very large SFs may still need more for
  the coldest full-scan queries. MySQL-side queries have no client timeout.
- MySQL executes the CTE rewrites with its own plans (indexes above help);
  differences in result *values* between `.bench_ronsql X` and
  `.bench_sql X` indicate a correctness bug and take priority over any
  latency comparison.

## Phase timing (x-ronsql-phases)

**Implemented** (RonSQL-side; see `test_benchmark_extension_plan.md` Phase
B2/B3). When RDRS is built with `RONSQL_PHASE_STATS` (default on, kill
switch in `RonSQLPerf.hpp`), every successful `/ronsql` response carries
an `x-ronsql-phases` header with per-request phase timings in µs,
captured at the pre-existing PERF_TS boundaries via a `RonSQLPhaseStats`
sink on `RonSQLExecParams` (null for ronsql_cli — a null test per phase
boundary is the only cost for callers that don't ask):

```
parse=…,analyze=…,load=…,plan=…,compile=…,prepare=…,subquery=…,
ndbprep=…,send=…,firstbatch=…,drain=…,print=…,execute=…,rows=…,attempts=…
```

- `parse/analyze/load/plan/compile` split the RonSQLPreparer constructor
  (`prepare` = its total, captured in ronsql_op); `load` isolates NDB
  dictionary access (schema-cache effectiveness).
- `ndbprep` = NdbQueryBuilder::prepare (join/CTE) or scan-op definition
  (single-table); `send` = trans->execute; `firstbatch` = wait for the
  first result row — the closest API-side proxy for data-node execution
  incl. CTE materialization; `drain` = remaining rows; `rows` = drained
  row count. On the single-table path the NDB API fuses send+drain
  inside DoAggregation, reported as `firstbatch` (send/drain stay 0).
- `print` = ResultPrinter formatting; `execute` = execute() total.
- Retry semantics: **last attempt wins**; `attempts` counts ronsql_op
  attempts (1 = no retry).

`.bench_ronsql` reads the header per request and prints a per-phase
avg/p95/p99/max table after the end-to-end results (plus the warmup's
raw values); end-to-end latency minus `prepare`+`execute` approximates
RDRS/HTTP overhead. `fs_floor` is the fixed-cost floor to compare
against. Header format is pinned by
`mysql-test/suite/ronsql/t/ronsql_phase_stats.test` and parsed by
`ronsql_bench.go` (`parseRonSQLPhases`).

### Next step: data-node-side phase counters

Still open: correlate with existing counters (`m_rows_examined`,
ScanFragConf `rowsExamined`) and add DBSPJ/DBLQH timing around
CTE_SCAN/CTE_LOOKUP materialization, JOIN_AGG_COMPLETE merge, and
redistribution, exposed via ndbinfo or DUMP. The API-side `firstbatch`
value bounds the total data-node time but cannot attribute it to
kernel phases.
