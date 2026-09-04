# RonSQL vs MySQL vs compiled-interpreter benchmark matrix

Tooling for the question "what does the compiled interpreter (RONDB-1056)
buy on the RonSQL / MySQL analytics queries, and how do RonSQL and the
MySQL server compare" — measured with the rondb-cli benchmarks
(`.bench_ronsql <query> T N` / `.bench_sql <query> T N`, see
`../pushdown_join_aggregation/ronsql_cli_benchmarks.md`) over a
benchmark cluster started the ndbcrunch way (CPU binding through
`cpubind.cnf`) with an RDRS server.

| Piece | Where |
|---|---|
| Benchmark cluster (2 ndbmtd, 2 mysqld, RDRS with RonSQL, optional Rondis) | `mysql-test/suite/ronsqlcrunch/` (`my.cnf`, `t/setup.test`, `cpubind.cnf`, `rondis.cnf`, RDRS JSON templates) |
| RDRS honours `cpubind=` like mysqld / ndbd | `mysql-test/mysql-test-run.pl` (`rdrs_start`), `mysql-test/lib/My/ConfigFactory.pm` (`@rdrs_rules`) |
| Driver | `ronsql_bench_matrix.py` (this directory) |
| Runtime compiler toggle (no restart between arms) | `ndb_mgm -e "ALL SET CompiledInterpreter OFF|AUTO|ON"` (see `../set_config_param/reference.md`) |

## Running it

Build (once): `ndbmtd ndb_mgmd ndb_mgm mysqld mysql mysqladmin rdrs2
rondb-cli`. Then, from the repo root:

```sh
# smoke run: 1 thread, ~2 s per case, sf 0.1, all queries, all engines, OFF then ON
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py --build prod_build --quick

# the real one (Linux box): CPU-bound cluster, client on its own CPUs,
# 1 and 16 threads, ~10 s per case, sf 1
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --sf 1 --threads 1,16 --seconds 10 \
    --cpubind mysql-test/suite/ronsqlcrunch/cpubind.cnf --client-cpus 20-23 \
    --out ronsql_bench_out
```

Useful switches:

| Switch | Effect |
|---|---|
| `--queries fs` / `offline_fs` / `tpch_cte` / `tpch_official` / `a,b,c` | subset of the registry (`.bench_ronsql list`, `.bench_sql list`) |
| `--engines ronsql,mysqld,mysqld_nopush` | `mysqld` = aggregation pushdown ON (`ndb_pushdown_aggregate`, `ndb_join_pushdown_aggregate[_outer_join]` set GLOBAL), `mysqld_nopush` = all three OFF (mysqld does the aggregation itself; the data nodes only scan) |
| `--compiler off,on` | compiler arms, in order |
| `--repeat N` | run every case N times, interleaved; the report shows the median run per case (use 3+ on a noisy machine) |
| `--order query-major|arm-major` | query-major (default): OFF and ON run back to back per query (needs the runtime SET), so optimizer-statistics drift and cache warm-up cannot masquerade as a compiler effect; arm-major: all OFF cases, then all ON |
| `--toggle auto|set|restart` | how arms are switched: `ndb_mgm ALL SET CompiledInterpreter` (default when the build supports it) or cluster stop + start + reload |
| `--requests N` | fixed requests per thread instead of the `--seconds` budget (a 3-request probe sizes each case otherwise) |
| `--rondis` | also start Rondis inside RDRS (Rondis tables created through mysqld at startup) |
| `--rdrs-threads N` | RDRS `REST.NumThreads` (default 64) |
| `--no-start --mysql-port P --mysql-sock S --rdrs-port P --connectstring C` | use a cluster you started yourself (`./mtr --suite=ronsqlcrunch setup --start-and-exit ...`) |
| `--keep-cluster` / `--stop` | leave the cluster up after the run / stop it later |
| `--no-load` | skip `.load_tpch` (the driver already skips it when `tpch.lineitem` has the row count for `--sf`) |
| `--verbose` | stream every mtr / CLI line instead of the interesting ones |

What you see while it runs: the mtr start lines with ports, the
`.load_tpch` per-table progress, then per case a header
(`[12/276] compiler=OFF engine=ronsql query=fs_point threads=1
requests/thread=4000`), the CLI's warmup / progress / throughput /
latency / phase lines, a one-line summary (`=> 4103.2 q/s, avg 240us,
p95 310us, server prepare 60us + execute 150us, top phase firstbatch
120us, http+client 30us, jit compiled 0 reused 1 fallback 0 rows 1`)
and an ETA. Raw CLI output per case lands in `<out>/cases/*.txt`.

## What the report shows

`<out>/report.md` (also printed at the end; every number also in
`<out>/results.json`):

- **A. per engine, compiler OFF vs ON** — avg latency, OFF/ON ratio
  (>1 = compiled interpreter faster), throughput; one table per thread
  count.
- **B. RonSQL vs MySQL server** — avg latency side by side, ratio
  mysqld/ronsql, for both compiler arms and both mysqld modes.
- **C. RonSQL, where the time goes** — the server-side phases RDRS
  returns in the `x-ronsql-phases` header (`RONSQL_PHASE_STATS`,
  default on): parse, analyze, load (NDB dictionary), plan, compile,
  ndbprep, send, firstbatch (data-node execution until the first result
  row; for single-table aggregates the whole DoAggregation), drain,
  print, plus prepare / execute totals, `http+client` = client latency -
  prepare - execute, rows drained, and the top phase.
- **D. MySQL server, where the time goes** — `ndb wait` per request
  (`Ndb_api_wait_nanos_count`: time the connection waited for data
  nodes) versus the remainder in mysqld, scan batches and rows received
  per request, pushed (SPJ) queries per request, KB received.
- **E. ndbinfo.jit deltas** (compiler ON) — programs compiled / reused
  per request, fallbacks, rows executed, compile µs per request.
- **F. throughput scaling** across the thread counts.

Reading it: the compiled interpreter can only show in the data-node
share — `firstbatch` (RonSQL) and `ndb wait` (mysqld) — and only for
queries whose rows go through an interpreted program (E tells you:
`rows executed/req` is the row volume the JIT saw; a query with 0 there
is a lookup or a pass-through and cannot speed up). `http+client` and
the mysqld remainder are the fixed per-query costs that dominate small
feature-store lookups; the aggregation-heavy `offline_fs_*` and
`tpch_q*` queries are where OFF vs ON separates.

## Notes and known limits

- **mysqld pushdown defaults to OFF.** `ndb_pushdown_aggregate`,
  `ndb_join_pushdown_aggregate` and `ndb_join_pushdown_aggregate_outer_join`
  default to OFF in mysqld and rondb-cli's `.bench_sql` sets no session
  variables, so a plain `.bench_sql` never reaches the (compiled)
  interpreter. The driver sets them GLOBAL per engine; the suite my.cnf
  also turns them on for manual sessions.
- **Compiler toggle.** `CompiledInterpreter` is read once at data-node
  config read; the runtime `ALL SET CompiledInterpreter` command exists
  since 2026-09-04 (this branch). Older builds need `--toggle restart`
  (two starts, two loads). After a SET the driver prints
  `ndbinfo.config_values` for param 709 as confirmation. Programs the
  data node already compiled and pinned (RonSQL marks its programs
  reusable) stay in the JIT cache across a toggle: OFF routes new
  executions to the interpreter, ON resumes cache hits (E shows
  `reused/req` ≈ 1 for RonSQL under ON).
- **Optimizer statistics.** The driver runs `ANALYZE TABLE` on the tpch
  tables right after the load (and when the load is skipped) so mysqld
  plans with NDB index statistics from the first case. The first run
  (before this) showed plans changing between the OFF and ON arms
  (tpch_q2 614 -> 73 ms, fs_topk 4.7 -> 122 ms in BOTH mysqld modes) as
  the index-stat thread caught up: an order effect, not the JIT.
- **mysqld NDB wait is baseline-corrected.** `Ndb_api_wait_nanos_count`
  is global and mysqld's background threads (schema distribution,
  index stats, binlog injector) wait in the NDB API continuously, about
  1 s per second. The driver measures that idle rate for 3 s before the
  matrix and subtracts rate x case wall time; short cases make it
  approximate. Uncorrected it exceeded the request latency.
- **Sizing.** With `--seconds S` each case is sized from a probe
  (T=1, 3 requests): requests/thread = S / avg latency, clamped to
  `--min-requests`..`--max-requests`. The probe is not recorded.
- **Counters are global.** `SHOW GLOBAL STATUS` and `ndbinfo.jit` deltas
  cover everything that ran during the case (the CLI's warmup request
  and its key-range query included); do not run anything else against
  the cluster during a run.
- **CPU binding is Linux-only** (`taskset`); on macOS the cpubind lines
  are ignored by mtr and `--client-cpus` must not be given.
- **Rondis** is only started, not benchmarked (`.bench_rondis` is a
  different key-value workload); adding it is a matter of one more
  engine entry in the driver once a comparable query set exists.
- The dbtc-style block benches (`jit_bench_off_vs_on.py`) remain the
  tool for the NDB API TPC-H programs; this matrix is the SQL-level
  view.

## Findings worth following up (from the 2026-09-04 code survey)

- RDRS never wires `RonSQLExecParams::schema_cache` (`g_schema_cache`
  is created in `main.cc` but no request sets `params.schema_cache`), so
  every RonSQL request pays `dict->listIndexes()` — the ~400 µs that
  shows up as `load` in table C for the small queries. Wiring it is a
  one-liner in `ronsql_ctrl.cpp`; the cache has table-version
  invalidation, so verify with the ronsql suites before enabling.
- `Log.LogQueries` in the RDRS config is dead (no consumer).
- `ronsql_cli` has no phase output (`params.phase_stats` never set);
  only RDRS exposes the phases.

## First run (macOS, prod, sf 0.1, 1 thread, ~2 s/case, arm-major, 2026-09-04)

Compiler OFF vs ON: RonSQL 0.91-1.14x (noise around 1.0); mysqld showed
0.04x-8.4x swings that were IDENTICAL in the pushdown and no-pushdown
modes and came with changed rows/batches per request: plan drift between
the arms (index statistics), fixed by ANALYZE + query-major order above.
RonSQL vs mysqld: mysqld wins the small feature-store lookups (fs_point
133 us vs 799 us: RonSQL's `load` 130 us + data-node `firstbatch` 604 us),
RonSQL wins the aggregation-heavy shapes by 4-118x (tpch_q11 9.9 ms vs
1.17 s: mysqld pulls 484k rows up) except tpch_q22 (mysqld 2.6 ms, 4
rows/req; RonSQL 54 ms scanning 315k rows) and offline_fs_join_body
(RonSQL 996 ms vs 241 ms). JIT compile storms: mysqld tpch_q13 compiles
~50k programs per request (correlated pushed-join child filters defeat
the program cache; 36.7 ms = 3.6% of the request), tpch_q2 ~1k.
