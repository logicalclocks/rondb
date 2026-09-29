# M3 — serving performance: the performance census (M3.0)

Written 2026-09-22. `ronsql_fs_support_plan.md` §3 defines M3 as the
milestone where the `fs_hw` targets of WP-F (F12, batch IN lists) and
WP-G (F13, snowflake round trips) are met. M3.0 is the step before any
engine work: measure the whole RonSQL feature set on the current engine,
latency and throughput, and let the numbers say which shapes need work
and in which order.

**Status: run 4 done 2026-09-22 on the benchmark computer, artifacts
archived 2026-09-23 (§6): the T=1 pass is complete, the T=8 pass ended
when the cluster went down under `offline_fs_batch` (F27, P0). Findings
F23–F27 in `findings/bench.md`; F13 closed; F26 folded into F25. Next:
the logs and the two experiments of §6.5, then the packages of §6.6.**

## 0. Why a census, and why now

- The only performance numbers on record are the three `fs_hw` runs of
  2026-09-11 (`benchmarks.md` §8, `findings/bench.md` F10–F13). They
  predate everything that changed the execution paths since: M1 (the
  collect CTE collapse F0, binary projections F7, error classes), M2
  (checked 64-bit SUM in the shared merge, AVG / FLOAT formatting, CTE
  result routing through TC) and the RONDB-1120 CTE work (identity
  addressing, `JOIN_AGG_SETUP` overlapped with execution, single-word
  feed signals, scan slots and lookup close). None of those was
  measured; any of them may have moved a shape in either direction.
- The `fs_hw` set answers the Hopsworks question only. The rest of the
  registry (`fs`, `offline_fs`, `tpch_cte`, 23 entries over TPC-H) was
  last run on the compiled-interpreter branch and never through the
  regression rule. Nothing isolates the engine primitives the shapes are
  built from, so a slow shape today needs a manual EXPLAIN-and-guess to
  attribute.
- `prod_build` is from 2026-09-11 (pre-M1). Numbers need a rebuild
  first; a debug build is only good for proving the plumbing.

## 1. The benchmark set

One matrix run covers every category of the rondb-cli registry
(`.bench_ronsql list`); the MySQL server on the identical statement is
the reference on every entry, and the production twins (`fs_hw_*_twin`,
`tpch_q*_official`) run on MySQL only.

| category | entries (RonSQL-capable + MySQL-only) | data set | what it answers |
|---|---|---|---|
| `core` (new, M3.0) | 10 | tpch sf 1 | engine primitives, one access path or execution stage each (§2) |
| `fs` | 11 | tpch | online feature-store CTE shapes: point / segment CTE bodies, CTE_LOOKUP children, projection over a CTE, ORDER BY / LIMIT on the pass-through path, DNF filters |
| `offline_fs` | 7 | tpch | full-table CTE materialization: scan + group + merge at 1.5–6 M rows, chained CTEs, anti-join, wide output |
| `tpch_cte` | 5 + 5 official | tpch | the analytics envelope: CTE rewrites of Q2 / Q11 / Q13 / Q15 / Q22 against MySQL on the same text and on the official form |
| `fs_hw` | 22 + 3 twins | fs_bench sf 1 | the Hopsworks serving shapes S1–S10 (`benchmarks.md` §2), batch sizes 10 / 100 / 1000, collect 5 / 50, snowflake depth 1 / 2, string keys, composite keys, hash-only PK |

55 RonSQL-capable entries. Every entry runs on `ronsql` and on
`mysqld_nopush` (the unpushed MySQL server is the reference baseline for
the Hopsworks shapes since run 3: the pushed arm crashes on F10 and fails
F11), at 1 thread (latency) and 8 threads (throughput on this laptop's
8-thread ceiling), with the compiled interpreter OFF and ON, interleaved
per query so cache warm-up cannot masquerade as a JIT effect.

### 1.1 The `core` category

`tools/rondb-cli/internal/shell/ronsql_bench.go` (category
`benchCatCore`). Every entry carries plan pins for the access path it is
meant to measure, so the warmup prints its EXPLAIN into the case log and a
`Plan pin missing` warning says the entry now measures something else.
`{KEY}` / `{KEYS:n}` on `orders` resolve to existing order keys
(`tpchOrderKeyResolver`: `.load_tpch` writes multiples of 4, so the plain
uniform draw would miss three requests in four); the customer-keyed
entries reuse the `fs_hw` resolver.

| entry | statement (shape) | rows touched at sf 1 | pins |
|---|---|---|---|
| `core_pk_lookup` | projection of one order by `o_orderkey = {KEY}` | 1 | `Execute as primary key lookup.` |
| `core_in_pk100` | `COUNT / SUM / MAX … WHERE o_orderkey IN ({KEYS:100})` | 100 | none: the run records the plan (batched PK reads or a filtered scan is the question) |
| `core_in_idx100` | `o_custkey, COUNT, SUM … WHERE o_custkey IN ({KEYS:100}) GROUP BY o_custkey` | ~1 k via `idx_orders_custkey` | none (F12 off the PK) |
| `core_idx_range` | one month of orders via `idx_orders_orderdate`, three aggregates | ~19 k | index scan on `idx_orders_orderdate` |
| `core_avg_range` | `COUNT, AVG, AVG, MIN` over a random 100-customer segment | ~1 k | index scan on `idx_orders_custkey` |
| `core_pass_range` | four columns of the same segment, no ORDER BY | ~1 k drained | index scan on `idx_orders_custkey` |
| `core_scan_agg` | five scalar aggregates over all of `lineitem` | 6 M | table scan, `No filters.` |
| `core_scan_filter` | `COUNT, SUM` over `lineitem` with a 3-conjunct filter on unindexed columns | 6 M scanned, ~4 % qualify | table scan, `FILTERS:` |
| `core_group_few` | all of `orders` `GROUP BY o_orderstatus` | 1.5 M, 3 groups | table scan |
| `core_group_2k` (added after run 4) | all of `orders` `GROUP BY o_orderdate` | 1.5 M, ~2.4 k groups | table scan |
| `core_group_many` | all of `orders` `GROUP BY o_custkey` | 1.5 M, ~100 k groups | table scan |

## 2. What the pairs isolate

The value of `core` is in the differences, read together with the shape
entries:

| difference | attributes |
|---|---|
| `core_pk_lookup` vs `fs_hw_agg_point` (index scan + aggregation on ~36 rows) | the cost of the aggregation program and the scan protocol over a plain PK read; both against the MySQL PK read |
| `core_in_pk100` vs `core_in_idx100` vs `fs_hw_agg_batch100` (PK prefix) | whether F12's table scan is specific to the PK-prefix IN list, to secondary indexes, or to every IN list; and whether a complete-PK list is already batched lookups |
| `core_idx_range` vs `core_avg_range` | the AVG path (M2.3) against COUNT / SUM / MAX on the same access path |
| `core_pass_range` vs `fs_history` (same rows, ORDER BY + LIMIT) | the client-side sort of the pass-through path |
| `core_pass_range` vs `core_avg_range` | result drain and printing of ~1 k rows against a one-row aggregate over the same scan |
| `core_scan_agg` OFF vs ON | the compiled interpreter on the row volume where it can show; `core_scan_filter` adds the per-row filter |
| `core_group_few` vs `core_group_2k` vs `core_group_many` | per-fragment group tables, the API-side partial merge and a 100 k-row result, at equal scan volume: the group-count curve (F24) |
| `core_group_many` vs `offline_fs_scalar` (the same grouping as a CTE body, scalar main) | CTE materialization of a 100 k-group body against printing it |
| `fs_hw_snow1_point` vs `core_pk_lookup` × 2 | F13: the CTE_SCAN + PK_LOOKUP round trips against two plain PK reads |

## 3. Run recipe

Prerequisites, in order:

1. Rebuild `prod_build` at HEAD (its binaries are dated 2026-09-11,
   before M1): `ndbmtd ndb_mgmd ndb_mgm mysqld mysql mysqladmin rdrs2
   rondb-cli`. The `core` entries exist only in a rebuilt `rondb-cli`.
2. Stop the other cluster on this machine (`rondb_2604_main/debug_build`,
   two `ndbmtd` at ~9 % CPU and 1.6 GB each, uptime three days at the
   time of writing): it shares the CPUs and the memory the census needs.
3. Nothing else heavy on the laptop for ~2 hours.

One run, both data sets, every category (`census.cnf` raises
`DataMemory` to 6 G so TPC-H sf 1 and fs_bench sf 1 fit side by side):

```
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --queries all --load both --sf 1 \
    --engines ronsql,mysqld_nopush --threads 1,8 --seconds 5 \
    --cpubind mysql-test/suite/ronsqlcrunch/census.cnf \
    --keep-cluster --out /tmp/census_run4
```

Sizing: 63 registry entries (55 RonSQL-capable + 8 MySQL-only) × up to 2
engines × 2 arms × 2 thread counts ≈ 460 cases of ~5 s plus the probe
per (engine, query, arm); the full scans (`core_scan_*`, `offline_fs_*`,
`core_group_*`) and `fs_hw_agg_batch1000` run at the 5-request minimum
and dominate the wall time. Expect 1.5–2.5 hours. `--keep-cluster`
leaves the loaded cluster up for re-runs of single entries
(`--no-start --no-load --mysql-port … --rdrs-port … --connectstring …`
as printed by the driver's last log line) and for `.explain_ronsql` on
anything the triage flags; stop it with `--stop`.

On a dedicated Linux benchmark computer, the same run with the cluster
bound to its own CPUs and the client on others, and a wider thread
sweep for the throughput ceiling (the machine, not the client, should
be the limit): copy `cpubind.cnf`'s `cpubind=` lines into `census.cnf`
(one file goes through `--cpubind`), set `NumCPUs` to the data-node set
size, and run

```
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --queries all --load both --sf 1 \
    --engines ronsql,mysqld_nopush --threads 1,8,32 --seconds 10 \
    --cpubind mysql-test/suite/ronsqlcrunch/census.cnf --client-cpus <list> \
    --keep-cluster --out /tmp/census_run4
```

The triage script uses the lowest thread count for latency and the
highest for throughput, whatever they are.

Then the triage:

```
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py /tmp/census_run4 \
    --baseline storage/ndb/claude_files/fs_ronsql/bench_results/2026-09-11-prod_build-run2 \
    --out /tmp/census_run4/triage.md
```

The baseline covers the `fs_hw` entries only (run 2 is the last complete
`ronsql` + `mysqld_nopush` matrix); the other categories get their
baseline from this run.

Archive: copy `report.md`, `results.json`, `triage.md` and the
`cases/*.txt` of every entry the triage lists to
`bench_results/2026-09-<dd>-prod_build-run4/`, fill §6 here and
`benchmarks.md` §8 (run 4), and add findings to `findings/bench.md`.

## 4. Triage rules

`ronsql_bench_triage.py` (next to the matrix driver) turns `results.json`
into the ranked list the census exists for:

- **Latency class** at the lowest thread count, interpreter arm:
  RonSQL avg / MySQL avg. `PARITY` ≤ 1.25×, `SLOW` ≤ 3×, `CRITICAL`
  above, `FAIL` when the RonSQL case did not run.
- **Throughput class** at the highest thread count: MySQL q/s / RonSQL
  q/s, same bands; plus the T8/T1 scaling of each engine, which separates
  "slow per request" from "serialises under load" (run 2: the batch IN
  lists scaled 1.3–2× against MySQL's 3×).
- **Attribution** per RonSQL case: execute share of the latency, the
  dominant server-side phase (`firstbatch` = data-node time, `drain` =
  result volume, `print` = formatting, `load` = RDRS dictionary), the
  `http+client` remainder, rows per request, the OFF/ON ratio, and any
  `Plan pin missing` line from the case log.
- **Regression rule** (`benchmarks.md` §8) against `--baseline`: avg
  worse by more than 15 % or p99 by more than 25 % on the same (query,
  engine, arm, threads) is `REGRESSION`; avg better by more than 15 % is
  `IMPROVED`. A pin warning on the same entry explains it as a plan
  change first.
- Section 1 of the report is the work list: FAIL, then CRITICAL, SLOW and
  regressions, ranked by the worse of the two ratios.

Reading the list into work packages:

| triage outcome | goes to |
|---|---|
| `fs_hw_agg_batch*`, `strkey_batch100`, `snow1_batch100`, `core_in_*` CRITICAL with `firstbatch` dominant and a table-scan plan | WP-F (F12), scoped by which IN-list forms are affected (§2) |
| `fs_hw_snow*` SLOW/CRITICAL with `firstbatch` ≫ two PK reads | WP-G (F13); `core_pk_lookup` gives the floor the design must approach |
| any entry whose `http+client` or `print` / `drain` dominates | RDRS / printer work, not the data node — a new package |
| `core_scan_*` or `offline_fs_*` with OFF/ON ≈ 1 | the compiled interpreter is not reaching that program; RONDB-1056 coverage |
| REGRESSION without a pin warning on an entry that was PARITY in run 2 | bisect over the M1 / M2 / RONDB-1120 commits before anything else |
| FAIL | a correctness or support gap: ledger entry first, benchmark second |

Expected picture, to be confirmed or overturned by run 4: point shapes
and collect at parity; F12 and F13 still open (no engine commit since
run 1 touched IN-list planning or the CTE_SCAN plan shape); the collect
CTE form (`fs_hw_collect5_cte`, `collect50_cte`) now at the cost of the
direct form (M1.3); `hash_point` measured for the first time
(`--load both` at sf 1 needs `--hash-twin` for it; otherwise it is
skipped with a log line).

## 5. Deliverables and verification (user-run)

Code written 2026-09-22:

| piece | where |
|---|---|
| `core` category: constant, ten entries, listing sections, plan-pin check, `tpchOrderKeyResolver` | `tools/rondb-cli/internal/shell/ronsql_bench.go` |
| help text | `tools/rondb-cli/internal/shell/repl.go` (query families) |
| driver: `--queries core`, `all` no longer picks up the `fs_hw` category-runner line as a query, help | `storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py` |
| triage script | `storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py` |
| memory for both data sets | `mysql-test/suite/ronsqlcrunch/census.cnf` |

```
# unit tests (the registry test checks name uniqueness across all categories)
cd tools/rondb-cli && go test ./internal/shell ./internal/fsq/...

# the listing: ten core_* entries under "Engine primitives" in both namespaces
cd ../../prod_build && make rondb-cli
runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis -e ".bench_ronsql list" | sed -n '/Engine primitives/,/fs_hw/p'
runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis -e ".query_ronsql core_in_pk100"

# plumbing on the debug build: sf 0.1, 1 thread, 2 s per case, the new category only
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build debug_build --queries core --quick --engines ronsql,mysqld_nopush --out /tmp/census_smoke
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py /tmp/census_smoke

# the census itself: §3
```

Expected from the smoke run: every `core_*` warmup prints its EXPLAIN
with no `Plan pin missing` warning except possibly `core_pk_lookup` (the
pin assumes the pass-through PK path prints the same line as the
aggregate one; if it warns, the case log has the wording to pin); no
FAIL rows; `core_in_pk100` and `core_in_idx100` show their plan in the
case log. The triage script was checked against runs 1 and 2 of
2026-09-11 (it reproduces F12 / F13 as the CRITICAL rows and flags the
p99-only movements between the two runs).

## 6. Results — run 4 (2026-09-22, benchmark computer)

Artifacts in `bench_results/2026-09-22-benchbox-run4/`: `results.json`,
`report.md`, `triage.md` (as pasted, first script version),
`triage_v2.md` (current script: coverage, cross-host, jit columns),
`cases/` (the 17 case logs cited below), `cluster_extra_off.cnf` (the
cluster configuration the run used). Linux x86_64, host
`localhost.localdomain`, tree `/home/mikael/mysql_trees/rondb_2604/prod_build`,
started 2026-09-22 20:17, sf 1, both data sets, `ronsql` +
`mysqld_nopush`, interpreter OFF and ON, threads 1 and 8, 285 cases.

### 6.1 Coverage, configuration and reading rules

- T=1 complete: 56 RonSQL-capable entries plus the 8 MySQL-only ones.
  T=8: the 12 `fs` entries, then `offline_fs_batch` on RonSQL failed and
  the cluster went down (F27); 52 queries have no 8-thread case.
- **Configuration.** The data nodes were bound to CPUs 0–14 and 16–29,
  but `NumCPUs` stayed at the suite default of 4, so each node ran the
  4-CPU automatic thread configuration on a 15-CPU set: 2 LDM threads
  per node, 4 fragments per table (the `ndbinfo.jit` "reused/req 4" of
  every scan). Every scan-bound number in this run is a 4-LDM number.
  mysqld and mysqltest were bound to 15–16 and 30–31 (16 overlaps data
  node 2's set); RDRS was unbound. The rerun sets `NumCPUs=15` and an
  RDRS set (`census.cnf` says so now).
- `fs_hw_hash_point`: the hash twin was not loaded (`--hash-twin` not
  passed); the driver now skips the entry under any selection.
- The baseline was the laptop; the current triage marks the
  cross-machine comparison and keeps it out of the ranking. Run 4 is the
  box's baseline.
- **A ~1 ms idle-wake stall runs through every round-trip-bound number
  of this run (F25).** Latency distributions are bimodal, a 40–110 µs
  mode and a 1.05–1.10 ms mode, and what differs between shapes is the
  share of requests in the slow mode. It inflates MySQL's side most
  (point aggregate 210 µs against 113 on the laptop, PK read 439 µs, the
  nested-join snowflakes ~1 ms = three lookups at ~30 % each,
  `snow1_batch100` 45 ms = 100 lookups) and RonSQL's all-fragment
  shapes, and it is gone at 8 threads (`fs_floor` p99 419 µs at T=8
  against 1.13 ms at T=1). Run-4 ratios are therefore not comparable
  with the laptop's, and a 1-thread average on this box has to be read
  next to its p95.
- MySQL's T=1 numbers for `fs_dnf`, `fs_freshness`, `fs_topk` (112 ms,
  122 ms, 990 ms) are 20–70× its own T=8 per-request latency: plan /
  index-statistics drift during the T=1 pass (`ronsql_bench_matrix.md`),
  not a baseline.

### 6.2 The needs-work list (T=1 unless stated)

| # | query | RonSQL | MySQL nopush | ratio | where | finding |
|---:|---|---:|---:|---:|---|---|
| 1 | `offline_fs_batch` @T8 | FAIL, cluster down | | | 20008 → 1869 → node failure | **F27, P0** |
| 2 | `core_in_pk100` (100 complete PKs) | 398 ms | 502 µs | 793× | table scan of `orders` | F23 |
| 3 | `fs_hw_agg_batch10` | 216 ms | 780 µs | 277× | table scan of `transactions_1` | F23 (F12) |
| 4 | `fs_hw_agg_batch100_window` | 981 ms | 6.5 ms | 150× | same | F23 (F12) |
| 5 | `fs_hw_agg_batch100` | 989 ms | 7.7 ms | 129× | same | F23 (F12) |
| 6 | `fs_hw_agg_batch1000` | 8.73 s | 95 ms | 92× | same, 1000-term OR chain | F23 (F12) |
| 7 | `core_in_idx100` (100 keys, secondary index) | 403 ms | 4.9 ms | 82× | table scan of `orders` | F23 |
| 8 | `fs_hw_strkey_batch100` | 743 ms | 11.3 ms | 66× | table scan of `transactions_str_1` | F23 (F12) |
| 9 | `tpch_q22` | 1.10 s | 217 ms | 5.1× | 150 k-group CTE body | F24 |
| 10 | `tpch_q2` | 1.12 s | 274 ms | 4.1× | 200 k-group CTE body | F24 |
| 11 | `core_group_many`, `offline_fs_scalar`, `offline_fs_chain`, `tpch_q13` | 1.11–1.14 s | 1.5–2.2 s | parity, ~1 s each | 150 k groups | F24 |
| 12 | `core_pk_lookup`, `fs_latest`, `fs_floor`, `fs_hw_floor`, `fs_hw_composite_point`, `fs_hw_sessions_window2h` | p95 / p99 1.05–1.17 ms | MySQL PK read p95 1.19 ms | | 3–35 % of requests in a ~1 ms mode | F25 |
| 13 | `fs_point` @T8 | 14.2 k q/s | 20.8 k q/s | 1.46× | RonSQL scales 3.3× T8/T1, MySQL 4.8× | throughput, §6.4 |
| 14 | `fs_floor` @T8 | 42.8 k q/s | 56.1 k q/s | 1.31× | the point-shape ceiling on this box | throughput, §6.4 |

### 6.3 Findings (details and repro in `findings/bench.md`)

**F23 — every IN-list form is a table scan with a per-row OR chain
(widens F12).** The EXPLAINs in the case logs say `Execute as table
scan.` for the complete-PK list (`core_in_pk100`, 398 ms; MySQL does 100
PK reads in 0.5 ms), the secondary-index list (`core_in_idx100`, 403 ms
vs 4.9 ms) and the PK-prefix lists (`transactions_1`: 216 / 989 /
8 730 ms for 10 / 100 / 1000 keys), and `Body root: TABLE_SCAN` for the
batch snowflake's CTE body. The marginal cost is ≈ 8.6 ms per extra key
over 3.6 M rows ≈ 2.4 ns per row per key: the OR chain evaluated
linearly per row; compiled or not (1.12× at 1000 keys). WP-F as
designed (PK prefix / secondary index → one index range per key;
complete PK → batched PK reads), plus a set probe for whatever scan
fallback remains, which alone takes the 1000-key case from 8.7 s to the
scan cost.

**F24 — many-group aggregation costs ~6 µs per group.** `core_group_many`
groups 1.5 M `orders` rows into 149 997 groups (every customer has
orders at sf 1): `firstbatch` 1.04 s, `print` 77 ms (0.5 µs per result
row, so RDRS formatting is not it), 1.13 s in all, against
`core_group_few`'s 160 ms for the same scan into 3 groups: +880 ms for
the groups ≈ 5.9 µs per group. Per LDM thread (4 fragments): 375 k rows
in 1.04 s = 2.8 µs per row, against 0.43 µs per row with 3 groups.
2.4 µs per row is not a lookup in a 150 k-entry hash table (a cache miss
is ~0.1 µs), so the prime suspect is the merge of the per-fragment
partials (4 × 150 k records; ~1.5 µs per record fits a record-by-record
merge), which the CTE forms pay as well: `tpch_q2` 1.12 s (200 k groups
from 800 k `partsupp` rows; MySQL 274 ms), `tpch_q13` / `tpch_q22` /
`offline_fs_scalar` / `offline_fs_chain` 1.10–1.12 s (150 k groups),
`offline_fs_batch` / `_multi` / `_wide` 1.5–2.0 s. With 15 LDM threads
per node the per-fragment tables shrink but the number of partials to
merge grows: the suspect gets worse, not better. `core_group_2k` (added)
gives the curve; a `perf record -g` of `ndbmtd` and `rdrs2` during
`.bench_ronsql core_group_many 1 20` on the box says where the time is.

**F25 — a ~1 ms idle-wake stall on a share of requests, on both
engines.** `core_pk_lookup`: min 40 µs, avg 404 µs, p95 1.08 ms, p99
1.09 ms, i.e. ~35 % of requests in a ~1.05 ms mode and the rest at
~45 µs; MySQL's PK read of the same rows: min 106 µs, avg 439, p95
1.19 ms, ~30 %. Same table, both clients, same stall: not a RonSQL path.
Share of requests in the slow mode by shape (from min, avg and p95 in
the T=1 case logs): PK lookup ~35 % (both engines); `fs_latest`
(all-fragment ordered scan with early close) ~25 %; `fs_floor` (all
fragments of `region`) ~13 %; `fs_hw_floor` (fragment-stats COUNT(*),
all fragments) ~9 %; `fs_hw_composite_point`, `fs_hw_sessions_window2h`
(pruned scans) 3–4 %; `fs_hw_agg_point`, `collect5`, `strkey_point`,
`agg_filter` (pruned scans on `transactions_1`) < 1 % (p99 123–148 µs,
p99.9 1.13 ms); the snowflakes (one SPJ tree) < 1 %. At 8 threads the
slow mode is gone (`fs_floor` p99 419 µs). Reading: a request pays ~1 ms
when it has to wake something that went idle between requests; the
35 % of PK reads against < 1 % of pruned scans is the clue to what: the
PK read picks its TC by key (half the requests land on data node 2's TC,
which nothing else uses at T=1), the RonSQL scans always start on node
1's TC. Two experiments on the box, in this order: (1) CPU idle states —
read the deepest C-state's exit latency (`cpupower idle-info`,
`/sys/devices/system/cpu/cpu0/cpuidle/state*/latency`), then hold
`/dev/cpu_dma_latency` at 0 (or `cpupower idle-set -D 10`) during
`.bench_ronsql core_pk_lookup 1 20000` — if p95 drops from 1.1 ms to
< 100 µs it is the hardware sleep, a benchmark-box setting and a note for
production tuning; (2) if not, data-node thread spinning (`SpinMethod` /
`SchedulerSpinTimer`) — if that removes it, it is the block-thread
sleep / wake; otherwise it sits in the NDB API receive path that RDRS
and mysqld share. Hopsworks reads p99, so this decides what the serving
p99 on production hardware is.

**F26 — folded into F25.** The 372 µs `firstbatch` of `core_pk_lookup` is
~45 µs plus 35 % × 1 ms; the plan is `Execute as primary key lookup.`
(no pin warning). Nothing to fix in the lookup path; re-measure after
F25.

**F27 — data node failure under eight concurrent many-group CTE queries
(new, P0).** `offline_fs_batch` at T=8, `cases/off_ronsql_offline_fs_batch_T8.txt`:
warmup 1.4 s; the first 14 concurrent requests took ~20 s each
(`firstbatch` avg 20.6 s, max 23.9 s, 13× the single-request time on the
4-LDM configuration); then `Join query failed: Query aborted due to out
of query memory (code 20008)`, then `Error in aggregation interpreter,
check error log on data node for more details (code 1869)` for the
remaining 24 attempts (29 retries); after the case mysqld's `ndbinfo`
query failed with error 157 `Connection to NDB failed`: the data nodes
were gone (71 s wall). Three items: (a) 20008 is a resource rejection —
the suite's budget (`SharedGlobalMemory=450M`, `TransactionMemory=200M`)
against eight concurrent queries × four fragments × 150 k-group tables
plus their merges — acceptable as a clean error, but the budget for
this workload needs sizing; (b) 1869 after 20008 means the aggregation
interpreter reports an internal error where the memory error should
propagate; (c) the node failure is the bug. Needs from the box:
`prod_build/mysql-test/var/log/ndb_1_cluster.log` (node failure reason
and time), the data nodes' `ndb_*_error.log` and `ndb_*_trace.log.*`
(under `var/log/ndbd.1.1/`, `ndbd.2.1/` or `var/mysql_cluster.1/`), and
the RDRS log around 2026-09-22 23:04. Until fixed, the T=8 rerun runs
`offline_fs` last, with `--requests 3` and the logs watched.

**Compiled interpreter.** The ON arm's `ndbinfo.jit` deltas
(`triage_v2.md` §4): `core_scan_agg` 6 000 000 rows executed per request
through 4 reused programs, 0 fallbacks, OFF/ON 0.99×; `core_scan_filter`
233 001 (the survivors of the filter), 0.99×; `offline_fs_join_body`
8.0 M rows, 0.96×; `core_group_few` / `_many` 1.5 M rows, 0.94–0.95×.
The programs ran compiled and changed nothing: on this workload the
interpreted program is not where scan time goes (input for RONDB-1056,
not an M3 item). Programs with embedded constants recompile per request:
`fs_hw_snow1_batch100` 1.97 compiles per request, 0.84× (ON slower).

### 6.4 Observations (not defects)

- **F13 closed.** Snowflake point reads 192–227 µs (laptop 483–531 µs,
  −60 %; the RONDB-1120 CTE work), p99 242–288 µs, and < 1 % in the F25
  slow mode; 2.5× the point read (80 µs). The WP-G single-SPJ-tree
  design stays as the way to the ~100 µs floor, at a lower priority.
- **F0 closed for performance too.** `collect5_cte` 76 µs and
  `collect50_cte` 84 µs against the direct forms' 77 / 78 µs.
- Every `fs_hw` point shape 76–93 µs (`firstbatch` 36–60 µs,
  `http+client` 28–29 µs), p99 123–150 µs outside the F25 set.
- `offline_fs`: RonSQL 1.1–2.0 s against MySQL 2.1–82 s
  (`offline_fs_batch` 1.54 s vs 78.7 s, `offline_fs_wide` 2.0 s vs 82 s);
  `fs_batch`, `fs_freshness`, `fs_dnf`, `fs_topk` 10–100× ahead (with the
  caveat on MySQL's T=1 plans).
- `core_scan_agg` 642 ms for 6 M rows on 4 LDM threads (428 ns per row
  per LDM, 107 ns per row in all); `core_scan_filter` 319 ms (the filter
  rejects 96 % early); `core_idx_range` 130 ns/row; `core_pass_range`
  drains 1 000 rows at 274 ns/row and `fs_history`'s client-side sort
  adds ~200 ns/row; AVG costs nothing extra (`core_avg_range` 257 µs).
- Throughput (`fs` only): the scan-heavy CTE shapes scale 1.7–2.7× from
  1 to 8 clients on 4 LDM threads (`fs_nation` 24.6 → 95.6 ms per
  request); MySQL's larger "scaling" there is its T=1 plan drift. The
  point shapes are the throughput item: `fs_point` 14.2 k vs 20.8 k q/s,
  `fs_floor` 42.8 k vs 56.1 k; the T=8 / T=32 pass over `fs_hw` and
  `core` on the 15-LDM configuration is needed to size it.
- `fs_hw_snow1_batch100` 37.7 ms is parity only because MySQL's nested
  join pays 100 lookups through the F25 stall; its CTE body is the F23
  table scan of `customers_1`.

### 6.5 Open items and the rerun

1. **F27 logs** from the box (§6.3) — before anything else, because the
   rerun will hit the same case.
2. **F25 experiments** (§6.3): the C-state check first, then spinning;
   `.bench_ronsql core_pk_lookup 1 20000` and `.bench_sql core_pk_lookup
   1 20000` before and after, p95 is the number.
3. **Configuration for the rerun**: `NumCPUs=15` in the box's
   `census.cnf`, mysqld off CPU 16, RDRS bound to its own set (e.g.
   30–31 or a larger set: it is the RonSQL client's server), and
   `--client-cpus` for rondb-cli outside all of them.
4. The rerun, merged with run 4 by the triage (`offline_fs` separately,
   last, until F27 is fixed):

```
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --queries fs,core,tpch_cte,fs_hw --load both --sf 1 --hash-twin \
    --engines ronsql,mysqld_nopush --threads 1,8,32 --seconds 10 \
    --cpubind mysql-test/suite/ronsqlcrunch/census.cnf --client-cpus <list> \
    --keep-cluster --out /tmp/census_run5
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --queries offline_fs --no-start --no-load \
    --mysql-port <p> --mysql-sock <s> --rdrs-port <r> --connectstring <c> \
    --threads 1,8 --requests 3 --out /tmp/census_run5_offline
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py \
    /tmp/census_run5 /tmp/census_run5_offline \
    --baseline storage/ndb/claude_files/fs_ronsql/bench_results/2026-09-22-benchbox-run4 \
    --out /tmp/census_run5/triage.md
```

   (`--queries` takes one category or a name list; run the four
   categories as four invocations of the first command on the kept
   cluster with `--no-start --no-load`, or one `--queries all` once F27
   is fixed.) With `NumCPUs=15` the run-4 baseline's scan-bound entries
   will show as "IMPROVED" — a configuration change, not code; the
   1-thread point shapes and the F25 shares are the comparable part.

### 6.6 Priorities for M3.1 onwards

Decision 2026-09-23 (the user): the experiments are documented in
`m3_experiments.md` and run on the benchmark computer as time allows;
the engine work starts with **F23 (WP-F, `m3_wpf_plan.md`)**; the F27
node failure after query-memory exhaustion must be investigated
regardless of the performance order (`m3_experiments.md` X4).

1. **F27** — correctness: a node failure under concurrent analytics
   queries; diagnose from the logs, then fix the failure and the 1869
   masking; size the query-memory budget for the census cluster.
2. **WP-F (F23 / F12)** — the only shapes RonSQL cannot serve today at
   any acceptable latency, and Hopsworks' batch serving. The rule (the
   user, 2026-09-23): an IN list on primary-key columns is served by a
   set of primary-key lookups, which scale with N and not with the
   fragment count; a multi-range index scan only where lookups are not
   an option (PK prefix, secondary index, CTE bodies); the scan
   fallback → set probe. Detail: `m3_wpf_plan.md`.
   Targets at sf 1 on this box (15 LDMs): `batch100` ≤ 15 ms,
   `batch1000` ≤ 200 ms, `batch10` ≤ 2 ms, `core_in_pk100` ≤ 1 ms.
3. **F24 many-group aggregation** — caps every analytics / offline shape
   at ~1 s per 150 k groups and puts `tpch_q2` / `q22` 4–5× behind
   MySQL. Profile first (the merge is the suspect), then a package;
   target `core_group_many` within 2× of `core_group_few`, `tpch_q2` ≤
   MySQL.
4. **F25 idle-wake stall** — diagnose with the two experiments; if it
   is the hardware sleep it becomes a tuning note (spinning /
   C-states) for the serving p99, otherwise an engine or API item.
5. **Throughput ceiling of the point shapes** — needs the T=8 / T=32
   data on the 15-LDM configuration.
6. **WP-G (F13)** — closed as a gap; optional 2× on the snowflakes.
