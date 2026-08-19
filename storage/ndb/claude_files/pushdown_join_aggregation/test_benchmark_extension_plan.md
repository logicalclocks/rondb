# Test & Benchmark Extension Plan — Pushdown Join Aggregation + CTEs

**Status: SHIPPED (2026-08-19).** All tests recorded and passing in the
`ronsql`, `ronsql_cte` (+ topology siblings) and `ndb_push_agg` suites,
including the re-recorded pre-existing baselines (`ronsql_parsing`,
`ronsql_rdrs_basic` — ORDER BY display-fix fallout). `ronsql_phase_stats`
passing means the `x-ronsql-phases` header is verified end-to-end.
Remaining optional: CLI benchmark smoke (`go build`, `.bench_ronsql
fs_floor` for the phase-breakdown table). Findings ledger in §5b:
3 crashes (EXPLAIN SUM(CASE) abort **fixed**; DECIMAL scalar subquery
RDRS crash **probed**; big-06 8-node RDRS crash **probed**), 2 ORDER BY
explain mis-prints **fixed**, 1 hang **probed** (anti-join + INNER CTE
join), 2 wrong-results **probed** (NOT EXISTS × CTE-join; scalar-CTE
MIN merge polarity — all topologies), 1 clean-reject **asserted**
(explicit IN × CTE-join), 2 future-extension TODOs (§7), 2 capability
upgrades pinned as EXPLAIN regressions (I.16 rewrite, CTE shadowing).
The probed engine bugs are the natural input for a follow-up fix phase
in the cte_fix_plan.md style. Design decisions taken during review: phase-stats
capture behind a **compile flag** (`RONSQL_PHASE_STATS`, default on, kill
switch in RonSQLPerf.hpp), transport via a **custom `x-ronsql-phases`
header**, **last-attempt-wins** retry semantics, and **finer resolution**
(analyze/plan constructor splits, firstbatch/drain drain-split, rows
counter) — see the updated §B2.

Goal: extend MTR test coverage for pushdown join aggregate queries and CTE
queries (short + larger queries, data-type support, subqueries, parser), and
extend the benchmark facility with new queries plus the ability to measure
time spent in RonSQL query-execution phases.

---

## 1. Current coverage analysis

### 1.1 Feature inventory (what shipped and needs regression protection)

From the phase docs in this directory:

- **Kernel/API**: DBLQH/DBTUP join aggregation (all agg types, GROUP BY,
  eviction, mutex-free), DBSPJ/DBTC orchestration, star-schema fan-out,
  chained/mixed outer joins, CTE materialization (CTE_SCAN / CTE_LOOKUP,
  filters, NEXTREQ flow control, redistribute, scalar redistribute,
  mutex-free CTE build), string MIN/MAX (CHAR/VARCHAR), DECIMAL widening,
  temporal MIN/MAX (DATE/YEAR/DATETIME/TIME/TIMESTAMP incl. fsp),
  typed normal-interpreter registers, FRAGS_PER_WORKER.
- **RonSQL**: aggregating single-table queries; join queries; CTEs (lookup
  child complete-key INNER/LEFT, scan root, anti-join, LEFT→INNER promotion,
  chained CTE-of-CTE, sibling CTEs, scalar CTEs incl. watermark
  GREATEST/LEAST + comma cross-join, multi-key lookup, I.16 root rewrite);
  CTE-body WHERE (comparisons, AND, OR/DNF, IS [NOT] NULL, col-vs-col
  signed-int, CASE over CTE column projections, GREATEST/LEAST n-ary,
  float/double atoms); CTE-body index scans (single + composite bounds,
  MIN/MAX-via-index, hints FORCE/USE/IGNORE on root scans); main-root index
  scans on join queries; subqueries (EXISTS, IN, scalar, correlated scalar,
  per-entity SUBQUERY_AGG incl. joins + HAVING); ORDER BY (GROUP BY cols /
  aggregate aliases) + LIMIT on aggregate queries; scoped name resolution;
  DATE/temporal display; scale-preserving DECIMAL display.
- **MySQL handler**: `ndb_join_pushdown_aggregate` pushed aggregation
  (suite/ndb_push_agg + ndb_push_agg_dist).

### 1.2 Test suite inventory

| Suite | Content | Topologies |
|-------|---------|------------|
| `suite/ronsql` (52 tests) | Feature-focused: parsing, encoding, filtering, joins, CTE phases (basic/case/chained/colvscol/decimal/greatest_least v1-v6/index/minmax_index/multi_batch/name_resolution/or/outer_join/partial_key/pk_join/root_lookup/scalar/scan), subquery ×3, orderby ×2, timestamp, overflow, regressions | default only |
| `suite/ronsql_cte` + `ronsql_cte_ng1r3/ng2r2/ng2r3/ng4r2` | Data-driven families on realistic TPC-H-like data (smoke, agg incl. d17 temporals, filter, index, joins, mainmode, main_root_index, chain_scalar, frags_per_worker) + hang/crash probes (d2/d3/d4/d19) | ×5 (2/3/4/6/8 nodes) |
| `suite/ronsql_large` | 20k-group CTE regression net | default |
| `suite/ndb_push_agg` | Block unit-test wrappers (testJoinAgg*, testCte*, bench*) + MySQL-handler pushdown tests (`ndb_pushdown_agg`, `ndb_join_pushdown_agg{,_evict,_linked}`) | default |
| `suite/ndb_push_agg_dist` | Multi-node variants | multi-node |
| `suite/rdrs2-ronsqltpch` | `ronsql_bench_setup` --start-and-exit cluster for CLI benchmarks | bench config |

Harness: `suite/ronsql/include/ronsql_compare.inc` runs the same query via
mysql client (oracle) and RonSQL REST (+ optionally ronsql_cli), sorts and
diffs. `ronsql_explain.inc` asserts plans. Family pattern: body include in
`suite/ronsql_cte/include/body_<FAMILY>.inc` + one-line wrappers in each of
the 5 topology suites. Rejection-asserts run ronsql_cli with `--error 1`;
uncertain/hanging shapes are recorded as commented `# NEXT-PHASE` probes.

### 1.3 Gap analysis

1. **Data-type matrix**: temporals are covered (agg-d17a..j), strings and
   DECIMAL covered, but there is no systematic width × signedness ×
   boundary matrix: TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT each in signed
   AND unsigned form, min/max boundary values (INT64 extremes, unsigned
   max), NULL-heavy columns of every type, empty strings, multi-byte UTF-8
   in VARCHAR aggregates, SUM over each int width, SUM over DECIMAL
   scale-0 vs scale-2, GROUP BY keyed on each type family.
2. **Subquery × CTE interaction**: subquery tests (~70 cases) and CTE tests
   are disjoint. No coverage of `WITH ... SELECT ... WHERE x > (SELECT ...)`,
   EXISTS/IN alongside a CTE join, scalar subqueries over DECIMAL/DATE
   columns, or subquery inside a CTE body (likely unsupported — needs a
   recorded disposition either way).
3. **Parser**: `ronsql_parsing.test` covers the lexer + single-table grammar
   thoroughly (UTF-8, identifiers, precedence, ORDER BY/LIMIT errors), but
   none of the newer grammar: WITH clause forms, JOIN/ON forms, GREATEST/
   LEAST, CASE, IS NULL, index hints, FRAGS_PER_WORKER statement head,
   comment syntax, multi-CTE lists, error positions inside CTE bodies.
4. **Larger queries**: data-driven families are mostly 1–2 CTE short
   queries. Missing: 3–4 CTE queries, deep chains on realistic data, wide
   output lists (8+ aggregates), long DNF WHERE clauses, combinations
   (chained CTE + index body + DNF + multi-batch 300-group).
5. **MySQL-handler pushdown agg**: no data-type sweep for the pushed
   aggregation path (`ndb_join_pushdown_aggregate=ON` vs `OFF` result
   equality per type family).
6. **Benchmarks**: 17 registry queries; no fixed-overhead floor probe, no
   datatype-heavy or DNF or chained-CTE or wide-output benchmarks.
7. **Phase timing**: only compile-time stderr logging exists
   (`RonSQLPerf.hpp` PERF_TS/PERF_LOG, disabled by default). No per-request
   timings reach the client; the CLI can only measure end-to-end wall time.
   `ronsql_cli_benchmarks.md` "Next steps: phase timing" item 1 is the
   RonSQL-side breakdown; item 2 (data-node-side) is separate.

---

## 2. Test extension plan (MTR)

### Phase T1 — data-type matrix family (`body_dtwide.inc`, ×5 topologies)

New family in the data-driven CTE suite. Creates its own tables (the shared
cte_schema stays untouched), drops them at the end:

```sql
CREATE TABLE dtw (
  d_id INT NOT NULL,
  d_grp INT NOT NULL,            -- 20 groups, re-agg join key
  d_ti  TINYINT,          d_tiu TINYINT UNSIGNED,
  d_si  SMALLINT,         d_siu SMALLINT UNSIGNED,
  d_mi  MEDIUMINT,        d_miu MEDIUMINT UNSIGNED,
  d_i   INT,              d_iu  INT UNSIGNED,
  d_bi  BIGINT,           d_biu BIGINT UNSIGNED,
  d_f   FLOAT,            d_d   DOUBLE,
  d_dec0 DECIMAL(12,0),   d_dec2 DECIMAL(12,2),
  d_ch  CHAR(10),         d_vc  VARCHAR(24),
  d_dt  DATE,
  PRIMARY KEY USING HASH (d_id)
) ENGINE=NDB;
CREATE INDEX idx_dtw_grp ON dtw (d_grp);
CREATE TABLE dtw_dim (g_id INT PRIMARY KEY, g_name CHAR(8)) ENGINE=NDB;  -- 20 rows
```

Deterministic data (~600 rows): per-type ramps covering negatives, zero and
boundary rows (signed min/max and unsigned max per width, injected as
explicit rows), ~1/6 NULLs per nullable column, empty string + multi-byte
UTF-8 strings in d_vc.

MAIN cases (~20, all with aggregating main SELECT per the envelope):

- dtw-01..05: CTE `MIN/MAX` per signed+unsigned width pair (ti/tiu …
  bi/biu), re-aggregated through a CTE_LOOKUP join to dtw_dim.
- dtw-06: `SUM` over each signed int width in one CTE (boundares chosen so
  sums stay in Int64).
- dtw-07: `SUM` over unsigned widths.
- dtw-08: `SUM(d_dec0)` and `SUM(d_dec2)` (D1 widening: exact BIGINT vs
  DOUBLE path).
- dtw-09: MIN/MAX over FLOAT and DOUBLE (no SUM — float SUM is
  non-associative, not strict-diff-testable; documented in body_agg.inc).
- dtw-10: MIN/MAX over CHAR/VARCHAR incl. empty string + UTF-8 rows.
- dtw-11: MIN/MAX over DATE (complements agg-d17, here with NULLs).
- dtw-12: COUNT(*) vs COUNT(<nullable col>) per type family.
- dtw-13..16: GROUP BY keyed on TINYINT, CHAR, VARCHAR, DATE columns
  (hash/cmp paths per type), aggregating main.
- dtw-17: boundary-only WHERE slices (rows at INT64 min/max, unsigned max)
  through MIN/MAX and SUM.
- dtw-18: all-NULL group (every aggregate returns NULL).
- dtw-19: wide output — 10 aggregates over 8 distinct columns in one CTE.
- dtw-20: scalar CTE (no GROUP BY) over the boundary rows.

PROBES: `SUM(FLOAT)` strict-diff (NEXT-PHASE, non-associativity),
`MIN/MAX(TIMESTAMP)` inside this family (covered in agg-d17i/j; here only a
pointer), anything the recording pass surfaces.

Deliverables: `suite/ronsql_cte/include/body_dtwide.inc`,
`suite/ronsql_cte{,_ng1r3,_ng2r2,_ng2r3,_ng4r2}/t/ronsql_cte_dd_dtwide.test`
(one-line wrappers), `suite/ronsql_cte/findings/dtwide.md`.

### Phase T2 — larger-query family (`body_bigquery.inc`, ×5 topologies)

Larger/combined queries on the existing shared schema (orders 1500 /
lineitem 3000 / customer 300), ~12 MAIN cases:

- big-01: four sibling CTEs (per-customer orders, per-supplier lineitems,
  per-brand parts, per-nation customers) joined into one aggregating main.
- big-02: three-level chained CTE on orders (per-cust → per-cust rollup →
  scalar), mirrors ronsql_cte_chained Test 1 on realistic data.
- big-03: chained CTE + sibling CTE mixed in one WITH list.
- big-04: `GROUP BY o_custkey` (300 groups, crosses the 256-group API batch
  boundary) re-aggregated through a second CTE join, wide output.
- big-05: long DNF CTE-body WHERE (6+ disjuncts across o_orderstatus /
  o_shippriority / o_clerk IS NULL) + aggregating main.
- big-06: GREATEST/LEAST (3-arg, cols + consts) inside a big CTE +
  CASE-over-CTE-column in main-query filter position (supported envelope).
- big-07: anti-join (LEFT JOIN cte … IS NULL) + second positive CTE join in
  the same query.
- big-08: multi-key CTE (2-col virtual PK) complete-key lookup with
  composite-index body bounds.
- big-09: index-hint (FORCE INDEX on CTE-body root) + residual filter +
  re-agg.
- big-10: scalar CTE watermark (GREATEST over two scalar CTEs, comma
  cross-join, Test-20 pattern).
- big-11: main-root INDEX_SCAN (o_orderdate range) over a join with two
  CTE children.
- big-12: ORDER BY + LIMIT on an aggregate main over big-04's shape.

Long-query risks respected: no high-cardinality CTE key (l_orderkey) under
a large root scan (D6 crash, unfixed), no string MIN/MAX re-aggregation via
an ordered-index child (D18), and any 6/8-node divergence gets recorded as
a finding + disabled probe rather than a red test (D22 caution). Every case
uses only shapes the existing regression net proves, combined.

Deliverables: `suite/ronsql_cte/include/body_bigquery.inc`, 5 wrappers
`ronsql_cte_dd_bigquery.test`, `suite/ronsql_cte/findings/bigquery.md`.

### Phase T3 — parser coverage for the new grammar (`ronsql_parser_cte.test`)

New test in `suite/ronsql`, two sections:

**(a) Pure parse errors** via the existing no-cluster pattern
(`$RONSQL_CLI_EXE --output-format TEXT --explain-mode=FORCE -e '…'` +
`--error 1`), no schema needed:

- WITH errors: missing AS, missing parens, empty body, trailing comma in
  CTE list, unterminated body paren, WITH without main SELECT.
- Join errors: implicit table alias (`orders o` — must be rejected),
  missing ON, dangling JOIN, ON with non-equality where required form is
  violated syntactically.
- GREATEST/LEAST arity errors (0/1 arg), CASE without WHEN/END, IS NULL on
  malformed operand.
- Index-hint errors: hint on a joined table is a prepare-time reject (needs
  schema → section b), but malformed hint lists (empty FORCE INDEX (),
  missing parens) are parse errors here.
- FRAGS_PER_WORKER errors: missing value, non-integer, out-of-range,
  duplicated prefix.
- Comment forms: unterminated `/*`, nested weirdness.

**(b) Parse-accept + plan shape** via ronsql_cli **with**
`--connect-string $NDB_CONNECTSTRING -D test --explain-mode=FORCE` over two
tiny tables created by the test (join/CTE planning needs the dictionary;
FORCE prints the plan without executing):

- WITH single/multi CTE, name shadowing per I.23 scoping, keyword-case
  variants (`with`/`WITH`/`With`), backtick-quoted CTE names and aliases,
  `--`-comments and `/* */` comments interleaved with the WITH list,
  newline/whitespace torture, GREATEST/LEAST/CASE/IS NULL in CTE-body
  WHERE, index hints on root, FRAGS_PER_WORKER = 1/2/4/8 prefix incl.
  `EXPLAIN FRAGS_PER_WORKER = 4 SELECT`, ORDER BY/LIMIT on aggregate main.

Success cases print the EXPLAIN plan into the .result (recorded once,
stable on the default topology suite).

### Phase T4 — subquery × CTE + subquery type coverage (`ronsql_cte_subquery.test`)

New test in `suite/ronsql` using `ronsql_compare.inc` with `$strict_diff=yes`
on purpose-built small tables (reuse the sq_* naming style):

- Scalar subquery in the **main** WHERE of a WITH query
  (`WITH c AS (...) SELECT ... FROM t JOIN c ... WHERE t.x > (SELECT AVG… no —
  MAX(...) FROM t2)`).
- EXISTS / NOT EXISTS in the main WHERE alongside a CTE join.
- IN-subquery in the main WHERE alongside a CTE join.
- Correlated scalar subquery + CTE join.
- Scalar subqueries returning DECIMAL, FLOAT/DOUBLE and DATE compared
  against columns of the same type (type coverage for the subquery result
  substitution path).
- Multiple scalar subqueries + multiple CTEs in one statement.

Uncertain shapes — recorded, never executed-if-hangable:
- Subquery **inside a CTE body** → try as rejection-assert first; if it is
  not a clean prepare-time reject, ship as a commented `# NEXT-PHASE` probe.
- IN-subquery whose inner SELECT reads a CTE → same disposition.

### Phase T5 — MySQL-handler pushdown-agg datatype sweep (`ndb_join_pushdown_agg_types.test`)

New test in `suite/ndb_push_agg` following the existing OFF/ON pattern
(`SET ndb_join_pushdown_aggregate=OFF; <query>; SET …=ON; <query>;` with
`--sorted_result`): a 2-table PK-join with a fact table carrying the same
width×signedness×boundary matrix as T1 plus CHAR/VARCHAR/DATE/DATETIME/
DECIMAL, exercising SUM/COUNT/MIN/MAX/GROUP BY per type family through the
pushed path, plus NULL groups and empty-result WHERE. A thin multinode
companion `suite/ndb_push_agg_dist/t/ndb_join_pushdown_agg_types_dist.test`
sources the same body include if the dist suite's convention allows;
otherwise defer the dist variant.

---

## 3. Benchmark extension plan (rondb-cli)

### Phase B1 — new registry queries (`ronsql_bench.go`)

All within the proven envelope, both-engine unless noted:

| Name | Category | Shape / purpose |
|------|----------|-----------------|
| `fs_floor` | fs | `SELECT COUNT(*) FROM region` — single-table scalar agg over 5 rows: fixed-overhead floor, the denominator for phase-timing analysis |
| `fs_minmax` | fs | 3-day `l_shipdate` window CTE with MIN/MAX over DECIMAL (l_extendedprice), DATE (l_shipdate) and DECIMAL qty per supplier, joined to supplier — datatype-heavy kernel MIN/MAX cost |
| `fs_dnf` | fs | per-customer CTE over a `{KEY}` segment with a 4-disjunct DNF body WHERE (o_orderstatus/o_shippriority/o_clerk IS NULL) — interpreter filter cost |
| `offline_fs_wide` | offline_fs | per-customer CTE with 8 aggregate outputs re-aggregated to 10+ output columns GROUP BY c_mktsegment — result-width cost |
| `offline_fs_chain` | offline_fs | three-level chained CTE (per-cust → rollup → scalar), mirrors ronsql_cte_chained Test 1 at SF scale — CTE-of-CTE materialization + redistribute cost |

Docs table in `ronsql_cli_benchmarks.md` extended accordingly.

### Phase B2 — phase timing, server side (RonSQL + RDRS)

Design decisions (as decided in review):

- **Transport = custom response header** `x-ronsql-phases`, not a body
  change: no request-schema (simdjson) change, works identically for
  TEXT / JSON / EXPLAIN output, invisible to existing clients and to the
  MTR ronsql_compare harness (curl -s drops headers). (`Server-Timing`
  was considered and rejected: float-ms convention vs exact µs ints.)
- **Compile flag**: all capture sites and the header emit are gated on
  `RONSQL_PHASE_STATS` (RonSQLPerf.hpp), **enabled by default**; comment
  it out to compile everything out. In addition the capture is guarded
  at runtime on the sink pointer, so callers that pass no sink
  (ronsql_cli, the parse-only auth path) pay one null test per boundary
  even when compiled in.
- **Retry semantics = last attempt wins**: values reflect the last
  `ronsql_op` attempt; `attempts` reports the attempt count so the CLI
  can flag contaminated samples.
- **Finer resolution**: the constructor splits into parse / analyze /
  load / plan / compile; the drain splits into firstbatch (wait for the
  first result row ≈ data-node execution incl. CTE materialization) and
  drain (remaining transfer), plus a drained-rows counter.

Struct `RonSQLPhaseStats` (all µs, monotonic clock):

```
parse_us      lex + parse + ORDER BY alias resolution
analyze_us    CTE / subquery / SELECT-subquery analysis
load_us       NDB dictionary access
plan_us       CTE shape validation + index/filter planning
compile_us    agg-program compile + linked projections
prepare_us    RonSQLPreparer constructor total        (set by ronsql_op)
subquery_us   execute_subqueries + substitution
ndbprep_us    NdbQueryBuilder::prepare (join/CTE) or scan-op definition
              (single-table)
send_us       trans->execute (send + first wait); 0 on single-table
firstbatch_us wait for the first result row; single-table: DoAggregation
              (the NDB API fuses send+execute+drain there)
drain_us      remaining result rows; pass-through path includes row
              formatting (print_us stays 0 there)
print_us      ResultPrinter formatting
execute_us    RonSQLPreparer::execute total           (set by ronsql_op)
rows_drained  rows pulled from the NDB API
attempts      ronsql_op attempts (1 = no retry)
```

Instrumentation points (all already delimited by the existing PERF_TS
markers, which stay untouched):

| File | Point |
|------|-------|
| `RonSQLPerf.hpp` | always-compiled `ronsql_perf_now_us()` + `STAT_TS`/`STAT_SET` macros (guarded on the sink pointer) |
| `RonSQLCommon.hpp` | `struct RonSQLPhaseStats`, `RonSQLExecParams::phase_stats` |
| `RonSQLPreparer.cpp` ctor | parse/load/compile (existing PERF boundaries at lines ~168–195) |
| `RonSQLPreparer.cpp` `execute()` | subquery block; single-table scan block (fused → drain_us) + print |
| `RonSQLPreparer.cpp` `execute_join()` | qb->prepare → ndbprep_us; trans->execute → send_us; nextResult loop → drain_us; print_result → print_us; passthrough drain → drain_us |
| `RonSQLPreparer.cpp` `execute_single_table_passthrough()` | (added after the RONDB-1108 non-agg-phase0 rebase) scan arm: ndbprep/send/firstbatch/drain/rows like execute_passthrough_drain; PK-lookup arm: ndbprep = op definition, firstbatch = execute(Commit) fused, rows 0/1 |
| `ronsql_operation.cpp` `ronsql_op` | prepare_us / execute_us / attempts |
| `ronsql_ctrl.cpp` | stack `RonSQLPhaseStats`, wire into params, on success `resp->addHeader("x-ronsql-phases", "parse=…,load=…,compile=…,prepare=…,subquery=…,ndbprep=…,send=…,drain=…,print=…,execute=…,attempts=…")` |

### Phase B3 — phase timing, CLI side (Go)

- `internal/client/rest.go`: `PostWithHeader(endpoint, body, header)` →
  `([]byte, string, time.Duration, error)`; `doRequest` refactors onto a
  shared internal that optionally captures one response header.
- `internal/shell/ronsql_bench.go`:
  - parse `x-ronsql-phases` into `map[string]int64`;
  - per-phase `LatencyCollector`s fed from every timed request (and the
    warmup, printed inline: `Warmup phases: parse=…µs load=… …`);
  - after `printBenchResults`, print a phase-breakdown table
    (avg/p95/p99/max per phase, µs formatting via the existing
    `formatLatency`), plus retry count if any `attempts > 1`;
  - graceful no-op when the header is absent (old RDRS build) with a
    one-line note.
- `.help bench` text in `repl.go`: one line documenting the breakdown.

### Phase B4 — phase-header MTR test (`suite/ronsql/t/ronsql_phase_stats.test`)

Small table + one aggregate query POSTed with `curl -s -D <hdrfile>
-o <bodyfile>`; assert `grep -ci '^x-ronsql-phases:' = 1` and validate the
value with a perl regex (`^parse=\d+,load=\d+,…,attempts=\d+$` →
echo "format OK") so the .result stays stable. One CTE query variant checks
ndbprep/send/drain are nonzero-capable without printing raw numbers.

### Phase B5 — data-node-side phase counters (DEFERRED, separate plan)

`ronsql_cli_benchmarks.md` next-steps item 2: DBSPJ/DBLQH timing around CTE
materialization, JOIN_AGG_COMPLETE merge and redistribution via ndbinfo or
DUMP, correlated with `rowsExamined`. Out of scope for this pass; the
x-ronsql-phases `send/drain` split already isolates data-node time as seen
from the API.

---

## 4. Files to be added / modified

**New MTR files**
- `mysql-test/suite/ronsql_cte/include/body_dtwide.inc`
- `mysql-test/suite/ronsql_cte/include/body_bigquery.inc`
- `mysql-test/suite/ronsql_cte{,_ng1r3,_ng2r2,_ng2r3,_ng4r2}/t/ronsql_cte_dd_dtwide.test`
- `mysql-test/suite/ronsql_cte{,_ng1r3,_ng2r2,_ng2r3,_ng4r2}/t/ronsql_cte_dd_bigquery.test`
- `mysql-test/suite/ronsql_cte/findings/{dtwide,bigquery}.md`
- `mysql-test/suite/ronsql/t/ronsql_parser_cte.test`
- `mysql-test/suite/ronsql/t/ronsql_cte_subquery.test`
- `mysql-test/suite/ronsql/t/ronsql_phase_stats.test`
- `mysql-test/suite/ndb_push_agg/t/ndb_join_pushdown_agg_types.test`
- (optional) `mysql-test/suite/ndb_push_agg_dist/t/ndb_join_pushdown_agg_types_dist.test`

**Modified C++**
- `storage/ndb/src/ronsql/RonSQLPerf.hpp` (runtime STAT macros)
- `storage/ndb/src/ronsql/RonSQLCommon.hpp` (RonSQLPhaseStats + param)
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` (phase capture)
- `storage/ndb/rest-server2/server/src/db_operations/ronsql/ronsql_operation.cpp`
- `storage/ndb/rest-server2/server/src/ronsql_ctrl.cpp` (header emit)

**Modified Go**
- `tools/rondb-cli/internal/client/rest.go`
- `tools/rondb-cli/internal/shell/ronsql_bench.go`
- `tools/rondb-cli/internal/shell/repl.go` (help text)

**Docs**
- `storage/ndb/claude_files/pushdown_join_aggregation/ronsql_cli_benchmarks.md`
  (new queries + phase-timing usage; item 1 of "Next steps" implemented)
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md` (index line
  for this plan)

---

## 5. Verification (run by the maintainer; no builds/tests run by Claude)

```bash
# C++ (RDRS + ronsql_cli):
cd <debug_build> && make -j$(sysctl -n hw.ncpu) rdrs2 ronsql_cli

# Go CLI:
cd tools/rondb-cli && go build -o rondb . && go vet ./...

# Record new baselines (default topology):
cd mysql-test
./mtr --record --suite=ronsql ronsql_parser_cte ronsql_cte_subquery ronsql_phase_stats
./mtr --record --suite=ndb_push_agg ndb_join_pushdown_agg_types
./mtr --record --suite=ronsql_cte ronsql_cte_dd_dtwide ronsql_cte_dd_bigquery

# Topology siblings (after the default-suite recording looks sane):
for s in ronsql_cte_ng1r3 ronsql_cte_ng2r2 ronsql_cte_ng2r3 ronsql_cte_ng4r2; do
  ./mtr --record --suite=$s ronsql_cte_dd_dtwide ronsql_cte_dd_bigquery
done

# Re-run without --record to confirm stability, then benchmarks:
./mtr --suite=rdrs2-ronsqltpch ronsql_bench_setup --start-and-exit
# rondb shell: .load_tpch 1 8 200; .bench_ronsql fs_floor; .bench_ronsql fs_minmax 4 100; …
```

## 5b. Findings from the recording pass

- **RonSQL has no SQL comment support at all**: `RonSQLLexer.l` skips only
  whitespace — `--`, `/* */` and `#` are all syntax errors, in every
  position. Found by the first `ronsql_parser_cte` recording (the
  comments-interleaved-with-WITH accept-case failed); the test now asserts
  rejection for both block and line comments instead. Adding
  comment-skipping rules to the lexer is a future extension, NOT part of
  these phases — see §7 (TODO 7.2); MySQL clients and ORMs routinely
  prepend comments (hints, tracing tags) to otherwise-valid statements.
- **CASE is an aggregate-argument expression only**: the single grammar
  production (RonSQLParser.y:472, `arith_expr`) is
  `SUM(CASE WHEN <cond> THEN <arith> ELSE <arith> END)`-style; a CASE used
  as a WHERE predicate (`WHERE CASE … END = 1`) is not in the grammar in
  any position. Found by the second `ronsql_parser_cte` recording. The
  cte_test_authoring_guide capability matrix's "CTE-body WHERE: … CASE"
  wording is misleading on this point — what Phase I.4 shipped is CASE
  *inside an aggregate* whose WHEN condition may reference a CTE column
  projection. `ronsql_parser_cte` now asserts the WHERE-position
  rejection plus both supported aggregate forms, and big-06 in
  `body_bigquery.inc` was rewritten accordingly. Adding the missing
  support is a future extension, NOT part of these phases — see §7.
- **EXPLAIN of `SUM(CASE …)` crashed the process (FIXED)**: the third
  `ronsql_parser_cte` recording SIGABRTed ronsql_cli — the EXPLAIN
  expression printer (`AggregationAPICompiler::print(Expr*)`) had no arm
  for `ExprOp::Case`, so a CASE expression fell through to the
  binary-operator tail's `default: abort()`. The abort is unconditional,
  so a production RDRS answering `EXPLAIN SELECT SUM(CASE …)…` (any
  explainMode reaching print) would have killed the whole REST server.
  Execution of CASE aggregates was always fine (ronsql_cte_case.test) —
  only EXPLAIN printing crashed. Fixed by adding the Case print arm
  (`CASE WHEN <condition> THEN … ELSE … END`; the WHEN condition prints
  as a placeholder since it is a parse-tree ConditionalExpression, not
  an SVM Expr).
- **EXPLAIN ORDER BY alias targets printed garbage (FIXED, two sites)**:
  both ORDER BY explain printers ignored their tagged unions. The
  parse-tree printer (RonSQLPreparer.cpp print()) read
  `OrderbyColumns::col_idx` even for `Kind::OUTPUT_REF`, misreading
  `output_idx` as an index into m_columns (printed `C1:\`val\`` for
  `ORDER BY total`); the ResultPrinter "Result sorted by" line read
  `OrderbySpec::groupby_idx` even for `Kind::AGGREGATE`, misreading
  `agg_result_idx` as a group-by index (printed the wrong column name).
  Display-only — the actual sort switches on the kind. Both now print
  the SELECT output alias (`Out_N:\`alias\``, `\`total\` DESC`).
  Baseline fallout: two pre-existing recorded baselines contain the old
  rendering and need re-recording — `ronsql_parsing.result` (bare
  ORDER BY column names resolve output-first, so they were always
  OUTPUT_REF; the old `C0:` rendering only looked right because
  output 0 coincided with column 0) and `ronsql_rdrs_basic.result`.
  The new `Out_N:` rendering is the truthful one (it shows MySQL's
  select-list-first resolution order).
- **Partial-key INNER join to a multi-key CTE is NOT a rejection**: the
  fourth recording showed the I.16b/c pre-pass root-rewriting it
  (CTE_SCAN to root, original root demoted to an INDEX_SCAN child)
  exactly as `cte_filter_phase_i16.md` describes; only shapes the
  rewrite cannot handle (e.g. LEFT OUTER on the multikey-CTE join
  itself) hit the I.16a rejection. The `ronsql_parser_cte` case is now
  an accept-case pinning the rewrite in the plan output, plus a LEFT
  JOIN rejection case.
- **Emit-path rejections are invisible under EXPLAIN FORCE**: the I.16a
  partial-key LEFT-join rejection is raised in `emit_child_ops`, which
  only runs on the execute path — under `--explain-mode=FORCE` the
  planner prints the would-be plan (visibly partial: `Key: g1 = p.b`
  binding one of two virtual-PK columns) and exits 0. Rejection-asserts
  for emit-time rejects must therefore EXECUTE the query (the
  ronsql_cte suite's rejection-assert pattern already does);
  `ronsql_parser_cte` now uses a non-FORCE launcher for that case.
  Worth knowing when reading EXPLAIN output generally: a printed plan
  does not prove the shape executes.
- **WRONG RESULTS: correlated NOT EXISTS in the main WHERE of a CTE
  join** (`ronsql_cte_subquery` Test 4, now a NEXT-PHASE probe): RonSQL
  returned 4/8 where mysql returns 1/2 on a 6-order dataset — only
  o_id 1-2 were filtered, consistent with the EXISTS→IN substitution
  list being truncated to its first entries or the negated IN-list
  filter being mis-emitted on the join-root path. Bounding evidence:
  single-table NOT EXISTS is green (ronsql_subquery.test Tests 19-23),
  correlated EXISTS × CTE-join is green (Test 4's sibling Test 3, a
  1-element IN list), scalar subqueries × CTE-join are green (Tests
  1-2). Suspected area: substitute_subquery_results' NOT-IN expansion
  meeting the join-root filter emission (apply_filter_top_level /
  emit-side DNF). Needs a kernel/RonSQL fix pass — same disposition as
  the cte_fix_plan.md items; NOT a test bug.
- **Explicit IN-subquery × CTE-join is cleanly rejected** ("Not an
  aggregate query.", `ronsql_cte_subquery` Test 5, now a
  rejection-assert): the I_IN_SUBQUERY path trips the non-aggregate
  gate when a WITH clause is present, even though the main SELECT
  aggregates. Inconsistent with correlated EXISTS × CTE-join (accepted
  + correct results) and single-table IN (accepted). Together with the
  NOT EXISTS finding above, the subquery × CTE-join matrix reads:
  scalar ✓, EXISTS ✓, NOT EXISTS ✗ wrong results, explicit IN ✗ clean
  reject — the fix pass should unify these paths.
- **CRASH: DECIMAL-typed scalar subquery kills the RDRS process**
  (`ronsql_cte_subquery` Test 7, now a NEXT-PHASE probe): `SELECT
  COUNT(*), MIN(o_price), MAX(o_price) FROM sq_ord WHERE o_price <
  (SELECT MAX(o_price) FROM sq_ord)` with o_price DECIMAL(10,2) crashed
  rdrs.1.1 mid-test (curl exit 52, empty reply). The subquery execution
  is text-based and parses the DECIMAL text as a float, so the suspect
  is the substituted FLOAT constant meeting a DECIMAL column in the
  single-table filter path (encode_constant / NdbScanFilter double →
  DECIMAL encoding). Bisect with the substitution-free
  `WHERE o_price < 99.99` literal form: if that also crashes, the bug
  is the DECIMAL-vs-float-literal filter generally, and the exposure is
  any user query — same production severity class as the EXPLAIN CASE
  abort. The rdrs crash trace from the failed run pins the abort site.
  Positive results bounding this: correlated scalar subquery ×
  CTE-join (Test 6) is green, and the DOUBLE-typed scalar subquery
  (Test 8, `o_ratio <= (SELECT MAX(o_ratio) …)` on a DOUBLE column) is
  green — so the crash is specific to the DECIMAL column comparison,
  not to float-substituted constants in filters generally. The suite
  recorded green with Test 7 as the only probe of this finding.
- **HANG: anti-join + positive INNER CTE join in one main query**
  (`body_bigquery.inc` big-07, now a NEXT-PHASE probe): `customer LEFT
  JOIN hiprio … JOIN lifetime … WHERE hiprio.cnt IS NULL GROUP BY
  c_nationkey` stalls in the data nodes — every attempt dies on NDB
  error 274 (transaction deadlock timeout) through all 10 ronsql_op
  retries. Bounding: the Phase K anti-join alone is green
  (offline_fs_anti shape) and mixed LEFT+INNER without the IS NULL
  filter is green. Suspect: the ANTI_JOIN promotion meeting a second
  agg-feed CTE_LOOKUP child in the JoinAgg batch/completion protocol —
  same hang family as cte_fix_plan.md's D-findings; candidate for that
  plan's Phase 1 queue.
- **CRASH: big-06 kills the RDRS process at 8 nodes only**
  (`body_bigquery.inc` big-06, now a NEXT-PHASE probe with bisect
  halves): the GREATEST/LEAST-with-nullable-operand body WHERE + CASE
  main aggregate over a CTE join crashed rdrs.1.1 on ronsql_cte_ng4r2
  (8 nodes / 4 node groups) after recording green on the default,
  ng1r3, ng2r2 and ng2r3 topologies — big-01..05 passed in the same
  8-node run. The crash is in the RDRS process, pointing at API-side
  handling of the 8-node result stream (NdbAggregator parse / result
  merge) for this shape rather than kernel row processing. The probe
  comment carries the two bisect halves (body WHERE alone, CASE main
  aggregate alone); the rdrs crash trace from the failed run pins the
  abort site.
- **WRONG RESULTS: scalar-CTE MIN merge is polarity-inverted — ALL
  topologies** (`body_dtwide.inc` dtw-19; MIN removed from the
  accept-case, shape probed as dtw-19b): three recordings triangulate
  it. (1) ng4r2/8 nodes, full table: MIN(d_bi) returned -9500000665
  instead of Int64-min. (2) Default topology, `WHERE d_id <= 240`:
  returned -11700000819 (row 3's value) instead of -11900000833 (row
  1's). (3) Default topology, full table: CORRECT — by placement luck.
  The consistent explanation: the MIN arm of the scalar cross-node
  merge keeps the LARGER of the two local minima (inverted compare),
  so results are right only when the true global min is scanned on the
  owner node. MAX (Int64-max, Uint64-max) merges correctly in every
  run; the GROUPED MIN path (dtw-05/dtw-16, hash-group redistribute)
  is correct on every topology. Suspect: the MIN arm of
  `mergeScalarAccumulators` (cte_filter_phase_i17_redistribute.md,
  keyLen==0 scalar redistribute). Not multi-node-only — any scalar-CTE
  MIN in production can be silently wrong. Follow-up audit: other
  scalar-CTE MIN users (body_chain_scalar.inc, offline_fs_scalar,
  cte_tpch_q15's MIN(revenue.total_rev)) may be green only by row
  placement. High-priority kernel fix; dtw-19b is the regression case
  to re-enable.
- **A CTE named like a stored table shadows it — accepted, not
  ambiguous**: the fifth recording showed `WITH pt2 AS (… FROM pt1 …)`
  resolving the `pt2` join target to the CTE (`CTE_LOOKUP CTE:pt2`,
  body source pt1) — standard SQL / MySQL semantics. The I.23
  "ambiguous unqualified names" rejection concerns column references,
  not table names. The `ronsql_parser_cte` case is now an accept-case
  pinning the shadowing resolution in the plan output.

## 6. Risks & guardrails

- **Known-unfixed kernel findings**: D6 (high-cardinality CTE key under a
  large root scan — crash), D18 (string MIN/MAX re-agg via ordered-index
  child), D22 (COUNT under 6/8-node redistribution). New cases avoid the
  triggering shapes; anything new the recording pass surfaces becomes a
  disabled `# NEXT-PHASE` probe + findings entry, not a red test.
- **Float SUM**: never strict-diff `SUM(FLOAT/DOUBLE)` (non-associative);
  MIN/MAX/COUNT only for float columns.
- **TIMESTAMP**: only under `time_zone='+00:00'` conventions (agg-d17
  pattern); the dtwide family sticks to DATE and leaves TIMESTAMP where it
  is covered.
- **Recording burden**: 2 new families ×5 suites = 10 new .result files;
  the plan sequences default-suite recording first so family bugs are fixed
  before the sibling recordings.
- **Header stability**: the phase header is additive; old clients and the
  compare harness ignore it. The MTR format assert pins the field list, so
  adding a field later means updating one regex + one .result.

## 7. Future-extension TODOs (out of scope for these phases)

Recorded here per maintainer direction; deliberately NOT implemented in
the phases above. When one of these ships, flip the corresponding
`ronsql_parser_cte` rejection-asserts to accept-cases and extend the
compare-based coverage.

### TODO 7.1 — CASE as a general WHERE predicate / expression

Support `CASE WHEN <cond> THEN <expr> ELSE <expr> END` outside aggregate
arguments: as a comparison operand in the main-query WHERE and in the
CTE-body WHERE (e.g. `WHERE CASE WHEN val > 200 THEN 1 ELSE 0 END = 1`).

Current state: the single grammar production (RonSQLParser.y:472) makes
CASE an `arith_expr` reachable only inside aggregate arguments, lowered
via `AggregationAPICompiler::CaseExpr`; Phase I.4 extended the WHEN
condition to CTE column projections and Phase I.5 v2 introduced the
register-based CASE codegen (embedded normal-interpreter compare + Mov,
post-Phase M).

Implementation sketch:

- **Grammar**: a `cond_expr`-position production for CASE used as a
  comparison operand (`CASE … END <cmp> <const|col>`), plus AST plumbing
  in `ConditionalExpression`.
- **Constant-armed desugaring (cheapest path, no kernel work)**: when
  both arms are constants, `CASE WHEN c THEN a ELSE b END <cmp> k` is
  equivalent to `(c AND (a <cmp> k)) OR (NOT c AND (b <cmp> k))` with
  the arm comparisons folding to TRUE/FALSE at prepare time — a
  simplify_ce-style rewrite pre-pass can reduce it to `c`, `NOT c`,
  TRUE or FALSE and reuse the shipped OR/DNF filter pushdown (Phase
  I.2 / or_body) for both main-root and CTE-body WHERE. NULL semantics
  of `c` need SQL-comparison care (`NOT c` vs `c IS NOT TRUE`).
- **Column-armed general form**: needs the register-based CASE codegen
  (I.5 v2) emitted into the filter interpreter paths (jump-table
  interpreter for CTE_LOOKUP/CTE_SCAN filters, NdbInterpretedCode for
  root scans) rather than the aggregation program; larger surface,
  sequence after the constant-armed rewrite.
- **Docs/tests**: correct the `cte_test_authoring_guide.md` capability
  matrix wording ("CASE" under CTE-body WHERE → aggregate-argument
  only, until this TODO ships); flip the `ronsql_parser_cte`
  "CASE as a WHERE predicate is not in the grammar" rejection-assert;
  add compare-based cases to the filter family and restore a
  body-WHERE CASE conjunct to big-06.

### TODO 7.2 — SQL comment lexing

Add comment-skipping rules to `RonSQLLexer.l` (`-- …\n`, `/* … */`, and
optionally `# …\n`), matching MySQL semantics (`--` requires a following
space per MySQL; decide whether to mirror that quirk). Today every
comment form is a syntax error in every position (§5b), which breaks
clients and ORMs that prepend tracing tags or hint comments. On shipping:
flip the two `ronsql_parser_cte` comment rejection-asserts to
accept-cases (including comments interleaved with the WITH list) and
add an unterminated-`/*` error case.

## 8. Candidate future test phases (not yet planned in detail)

Ordered by expected value. P0 belongs to the bug-fix phase; P1 items are
justified by findings from THIS exercise; P2/P3 round out coverage.

### P0 — post-fix regression re-enables (rides the fix phase)
Each §5b probe carries its ready-made regression query: dtw-19b
(scalar-CTE MIN), ronsql_cte_subquery Test 4 (NOT EXISTS × CTE-join) and
Test 7 (DECIMAL scalar subquery), big-07 (anti-join + INNER CTE join),
big-06 (8-node RDRS crash). On each fix: re-enable, extend to the
shape's neighbors (e.g. after the MIN-merge fix: scalar MAX/SUM/COUNT/
string-MIN over adversarial placements), re-record ×5.

### P1a — scalar-CTE merge matrix family (`body_scalarmerge.inc`, ×5)
The MIN polarity bug proved `mergeScalarAccumulators` is under-tested:
it has ONE merge site per aggregate kind × type family and correctness
currently depends on row placement. A family that sweeps scalar CTEs
(no GROUP BY) over MIN/MAX/SUM/COUNT × int widths / DECIMAL(0 and 2) /
FLOAT-MINMAX / CHAR / VARCHAR / DATE, with the extreme value planted in
MANY different PK positions (different fragments → different scan
nodes) so every case forces cross-node merges regardless of topology.
Also: all-NULL-on-one-node cases, empty-CTE scalar cases, and the I.17
watermark GREATEST over two scalar CTEs at 6/8 nodes.

### P1b — subquery boundary + limits (`ronsql_subquery_limits.test`)
The 1000-row caps in execute_subqueries (IN-subquery values, correlated
pairs) are untested boundaries: assert 1000 passes and 1001 rejects
cleanly for both, plus a deep WITH-list count bound and a
many-CTE-columns bound (parser-stack analogues of the existing paren
nesting case).

### P1c — ORDER BY / LIMIT on aggregate join/CTE queries (×5)
ronsql_orderby_limit_plan.md Phase 1 remains open: ORDER BY on join/CTE
aggregate mains is only spot-tested (big-12). A family covering
multi-target ASC/DESC mixes, alias vs GROUP-BY-column targets, tie
determinism, LIMIT 0/1/exact-group-count boundaries, and string
GROUP-BY keys under charset ordering.

### P2 — coverage rounding
- **Charset/collation matrix**: MIN/MAX + GROUP BY keys + CTE string
  join keys over utf8mb4 vs latin1 and a case-insensitive collation
  (S.2 charset-aware merge is only default-collation-tested).
- **Empty/NULL shape matrix**: empty CTE body × {lookup child, scan
  root, LEFT child, anti-join} ×5 topologies (chain family covers only
  the chained variant).
- **FRAGS_PER_WORKER × new families**: K=2/4/8 wrapper variants of
  dtwide/bigquery via $RONSQL_PREFIX (bundling under the new shapes).
- **JSON output format**: the compare harness pins TEXT only; CTE/agg
  queries with outputFormat JSON/JSON_ASCII + operationId.
- **Go unit tests** (tools/rondb-cli): parseRonSQLPhases, key
  substitution, registry envelope sanity.

### P3 — hard-to-harness
- **Concurrency**: parallel RonSQL CTE queries against one RDRS
  (join-agg state pool contention, mutex-free build), teardown under
  load (4c), schema-change-during-query retry (RonSQLMaybeStaleSchema).
- **HA**: data-node restart during CTE materialization / JOIN_AGG
  phases — must produce clean retryable errors, never hangs; API
  disconnect mid-query (RELEASE cleanup).
- **Scale**: bigquery-family shapes at ronsql_large lg_* scale (20k+
  groups; D6 territory).
- **B5 data-node phase counters**: tests arrive with that feature.
