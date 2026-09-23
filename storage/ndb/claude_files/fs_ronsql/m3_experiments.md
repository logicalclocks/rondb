# M3 — experiment plans after census run 4 (2026-09-23)

The census (`m3_plan.md` §6) left four questions that need measurements
or logs before any engine change, and one decision already taken: the
engine work starts with **F23 (IN lists, WP-F)** — `m3_wpf_plan.md` —
while the experiments below run on the benchmark computer in parallel
or in between. F27 (data node failure after query-memory exhaustion) is
a correctness item that must be investigated regardless of the
performance order; its plan is X4.

Each experiment states what it decides, the exact commands, the number
to read, and the decision rule. The user runs them (the benchmark
computer is theirs); results go into the `Result` line of each section
and into `m3_plan.md` §6 when they change a finding.

| id | question | decides | blocks |
|---|---|---|---|
| X1 | F25: what wakes up slowly? | box tuning vs data-node thread config vs NDB API item; the serving p99 story | reading every 1-thread number on the box |
| X2 | F24: where do the ~6 µs per group go? | the F24 package (group tables vs partial merge vs CTE materialization) | M3.3 |
| X3 | throughput of the point shapes at 8 / 32 clients on the 15-LDM configuration | whether there is an RDRS / RonSQL ceiling below MySQL's | M3.5 |
| X4 | F27: why did the data nodes fail after 20008 / 1869? | the P0 fix and the query-memory budget | the `offline_fs` part of every rerun |
| X5 | how Hopsworks creates online feature-group tables (PK type, partitioning) | which IN-list mechanism serves batch serving (`m3_wpf_plan.md` §2.2) and whether prefix lists can be pruned per fragment | WP-F's F5 scoping |

## X0. Cluster configuration for all experiments

Run 4 used the suite default `NumCPUs=4` on 15-CPU binding sets (2 LDM
threads per node, 4 fragments per table). Every experiment below uses
the corrected `census.cnf` on the box:

```
[cluster_config.1]
TotalMemoryConfig=12G
DataMemory=6G
NumCPUs=15
[cluster_config.ndbd.1.1]
cpubind=0-14
[cluster_config.ndbd.2.1]
cpubind=16-30
[mysqld.1.1]
cpubind=15,31
[mysqld.2.1]
cpubind=15,31
[rdrs.1.1]
cpubind=32-35        # or whatever is free: RDRS is the RonSQL client's server
```

and `--client-cpus` for rondb-cli outside all of those. Start once,
keep the cluster (`--keep-cluster`, or `./mtr --suite=ronsqlcrunch setup
--start-and-exit --defaults-extra-file=...census.cnf` and `.load_tpch 1
8 200` / `.fs_load 1 8 500 --db fs_bench --hash-twin` through rondb-cli),
and run the experiments against it with `--no-start --no-load`. The
first thing to record on the new configuration is the 1-thread
`core_scan_agg` (642 ms on 4 LDMs; expect ~4× less on 15) — it is the
sanity check that the thread configuration took.

## X1. F25 — the ~1 ms idle-wake stall

**Question.** 3–35 % of 1-thread requests (both engines) pay a ~1 ms
quantum; it is gone at 8 threads. What goes idle and wakes slowly: the
CPUs (C-states), the data-node block threads (sleep / wake), or the NDB
API receive path in the clients?

**Measure.** The number is p95 of `.bench_ronsql core_pk_lookup 1 20000`
(1.08 ms in run 4; the slow share is `(avg − min) / 1 ms`) and of
`.bench_sql core_pk_lookup 1 20000` (1.19 ms). Record min / avg / p95 /
p99 of each step.

**Steps, in order; stop at the first that removes the slow mode.**

1. *Baseline on the X0 configuration.* Both commands as they are. If
   the slow share already dropped below 5 % with `NumCPUs=15`, the
   4-CPU thread configuration was the cause (a thread combining roles
   sleeping on a timer); record and skip to X1-5.
2. *CPU idle states.* Read the exit latencies:
   `cpupower idle-info` (or `cat /sys/devices/system/cpu/cpu0/cpuidle/state*/{name,latency}`)
   and the governor (`cpupower frequency-info`). Then keep every CPU out
   of deep idle for the duration of one run:
   `sudo sh -c 'exec 3<>/dev/cpu_dma_latency; printf "\\0\\0\\0\\0" >&3; sleep 600'`
   (or `sudo cpupower idle-set -D 10`), rerun both benchmarks.
   Decision: p95 < 100 µs ⇒ hardware sleep. Fix for the benchmark box:
   the governor `performance` and the idle limit; for production: a
   tuning note (RonDB's `SpinMethod` keeps threads busy and hides
   C-state exits). Restore the setting afterwards (`cpupower idle-set
   -E`).
3. *Data-node thread spinning.* If step 2 did not remove it: restart
   the cluster with `SpinMethod=StaticSpinning` (or
   `SchedulerSpinTimer=...`, whichever the build documents in
   `mgmapi_config_parameters.h`) in `census.cnf`, rerun. Decision: p95
   < 100 µs ⇒ the block threads' sleep / wake; the data-node default
   deserves a look (LatencyOptimisedSpinning on the benchmark
   configuration).
4. *The API side.* If neither: the stall is in what RDRS and mysqld
   share, the NDB API receive path. Check `ndb_recv_thread_activation_threshold`
   (mysqld) and RDRS's equivalent; run `.bench_ronsql core_pk_lookup 2
   20000` (two clients keep the receive path awake?) and compare; then
   `perf trace -s`-style syscall timing on `rdrs2` during the run
   (`epoll_wait` / `futex` durations near 1 ms are the signature).
5. *Which thread.* Whatever removed it: confirm the 35 % vs < 1 %
   difference between PK reads and pruned scans by forcing the PK
   read's TC to node 1 (`ndb_data_node_neighbour` / the API's
   `nodeSelection`) — if the share drops to the scan's, the idle thread
   was node 2's TC.

**Result.** (empty)

## X2. F24 — where the many-group cost is

**Question.** `core_group_many` spends ~880 ms more than
`core_group_few` for 150 k groups (~6 µs per group; per LDM 2.8 µs per
row against 0.43). Per-fragment group tables in the LDM, the merge of
the per-fragment partials (in the NDB API for the drained form, on the
data nodes for CTE bodies), or the CTE materialization?

**Measure.**

1. *The curve.* On the X0 cluster: `.bench_ronsql core_group_few 1 20`,
   `core_group_2k`, `core_group_many` (3 / 2.4 k / 150 k groups, same
   scan). Linear in groups ⇒ per-group cost (merge or result build);
   flat then a knee ⇒ cache / table size.
2. *The split.* `perf record -g -p $(pgrep -f 'ndbmtd.*1.1') -- sleep 30`
   during `.bench_ronsql core_group_many 1 20`, and the same for
   `rdrs2`; `perf report --no-children --sort symbol | head -40` of
   each. Read: the share in `DbtupAggregation` / the group hash table
   (LDM side) against `NdbAggregation` / `aggMerge*` / the record
   decode in the API (`rdrs2` side).
3. *CTE against drained.* `.bench_ronsql offline_fs_scalar 1 20` (the
   same grouping as a CTE body, scalar main) with the `perf` of both
   data nodes: where the CTE form pays its 1.1 s (redistribution,
   `DbspjMain` / the CTE materialization blocks) against the drained
   form's API merge.
4. *Fragments.* With 15 LDMs per node the per-fragment tables are
   ~7× smaller and the partials ~7× more; compare `core_group_many` on
   the X0 configuration with run 4's 1.13 s — a merge cost grows, a
   table cost shrinks.

**Decision rule.** The side with > 60 % of the samples gets the F24
package (`m3_plan.md` §6.6 item 3); target `core_group_many` ≤ 2×
`core_group_few`, `tpch_q2` ≤ MySQL.

**Result.** (empty)

## X3. Throughput of the point shapes

**Question.** At 8 clients on run 4's configuration `fs_point` reached
14.2 k q/s against MySQL's 20.8 k and `fs_floor` 42.8 k against 56.1 k;
the `fs_hw` and `core` categories never ran at 8 threads. Is there an
RDRS / RonSQL ceiling below MySQL's on the 15-LDM configuration, and
where is it (RDRS threads, the NDB API connection pool, the data
nodes)?

**Measure.** The rerun of `m3_plan.md` §6.5 (`fs`, `core`, `tpch_cte`,
`fs_hw` at threads 1, 8, 32 with `--seconds 10`; `offline_fs`
separately, last, until F27 is fixed), then
`ronsql_bench_triage.py` on the merged runs with run 4 as baseline.
Read section 3 of the triage: q/s at T=32 and the T32/T1 scaling per
engine for the point shapes (`fs_hw_agg_point`, `collect5`, `fs_point`,
`fs_floor`, `core_pk_lookup`, the snowflakes). If RonSQL's ceiling is
below MySQL's: `--rdrs-threads 128` and a second RDRS NDB connection
(`rdrs_config_template.json`), then the data nodes' `ndbinfo.threadstat`
during the run to see whether TC or LDM saturates.

**Result.** (empty)

## X4. F27 — the data node failure under concurrent many-group queries

**Question.** `offline_fs_batch` at 8 clients: 14 requests of ~20 s,
then `out of query memory (20008)`, then `Error in aggregation
interpreter (1869)`, then the data nodes gone (mysqld error 157). What
failed, and is the memory budget or the failure path the bug?

**Steps.**

1. *Logs from run 4* (the box, `prod_build/mysql-test/var/`):
   `log/ndb_1_cluster.log` (node failure time and reason; look for
   `Node 2: Forced node shutdown`, `Node 1 disconnected`, error codes
   2341 / 2303 / 6050 / 3000-series),
   the data nodes' `ndb_*_error.log` and `ndb_*_trace.log.*` (the
   signal trace ending in the failing block — `Dbtup`, `Dbspj`,
   `Dblqh` — and the `ndbrequire` line), and the RDRS log around
   2026-09-22 23:04. Copy them into
   `bench_results/2026-09-22-benchbox-run4/f27_logs/`.
2. *Classify.* (a) an `ndbrequire` in the aggregation / CTE path after
   an allocation failure — the P0 bug; (b) a watchdog / job-buffer
   overload (`Watchdog: Warning`, `Job buffer full`) from eight
   concurrent 150 k-group aggregations on 2 LDM threads — a
   resource-exhaustion failure that the 15-LDM configuration and a
   larger budget change but do not fix; (c) a transporter / heartbeat
   failure caused by (b).
3. *Reproduce on purpose, at the reduced size first*: on the X0 cluster
   `.bench_ronsql offline_fs_batch 4 3` then `8 3` with the cluster log
   tailed; then the budget: `SharedGlobalMemory` / `TransactionMemory`
   (and whatever the query-memory pool for RonSQL aggregation is —
   `QueryMemory`? check `ronsql` config docs) raised until 8 × 3 pass.
   Record the memory each configuration needs, and whether 1869 still
   appears once 20008 stops.
4. *The masking.* 1869 after 20008: the aggregation interpreter reports
   a generic error where the memory error should reach the client;
   engine fix regardless of the crash.

**Decision rule.** Any `ndbrequire` / abort in the trace ⇒ P0 fix
before the next census run. Pure exhaustion ⇒ document the budget in
`census.cnf` and `ronsqlcrunch/my.cnf`, keep the `--requests 3`
isolation for `offline_fs` until the memory reject is clean (no 1869,
no failure), and open the engine item for a graceful reject.

**Result.** (empty)

## X5. Hopsworks online table DDL

**Question.** WP-F serves a complete-PK IN list with primary-key lookups
and a prefix list with a multi-range scan on the ordered PK index; a
`USING HASH` PK has no ordered index, and per-fragment pruning of a
prefix list needs the entity key to be the distribution key. What does
Hopsworks actually create?

**Measure.** In the reference tree (`/Users/mikael/github/hopsworks_ronsql`)
find the online feature-group `CREATE TABLE` emitter (the online
feature store controller / facade, and the RonDB-specific DDL if any)
and record: the PRIMARY KEY column order (`event_time` last?), whether
`USING HASH` is emitted, any `PARTITION BY KEY (…)`, and the index
definitions. Then the same for a real Hopsworks-created table on a
cluster (`SHOW CREATE TABLE`), because the DDL may be adjusted at
deployment.

**Decision rule.** Ordered PK on `(entity, event_time)` ⇒ the plan as
written. `USING HASH` ⇒ complete-PK lists are lookups only (fine) and
prefix lists have no index at all ⇒ WP-F's F2 does not apply to those
tables; the aggregation over a batch of entities on a hash-only table
becomes N lookups only when the whole PK is listed, otherwise F4's
branch-tree filter is the ceiling — and the Hopsworks DDL should add
the ordered index. `PARTITION BY KEY (entity)` ⇒ F5's pruned scans
apply and are worth doing early.

**Result.** (empty)

## Order

X4-1 (collect the logs), X0 and X5 first, because they cost minutes and
everything else runs on the corrected cluster or shapes WP-F; then X1
(an hour); X2 and X3 as the rerun of §6.5 runs. Engine work in the
meantime: WP-F (`m3_wpf_plan.md`).
