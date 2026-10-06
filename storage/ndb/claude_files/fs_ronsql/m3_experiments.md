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

**Result.** No measurement yet. Code analysis 2026-10-05 (a hypothesis
that explains every observation; to verify):

- *Thread layout.* Runs 4 and 6 ran `AutomaticThreadConfig` with
  `NumCPUs=4`: `thr_config.cpp` gives 4 receive threads and no LDM, TC,
  main or send threads, so every block instance runs in a receive thread.
  An idle receive thread sleeps in `pollReceive(1)` (`mt.cpp`,
  `mt_receiver_thread_main`: `delay = 1; // 1 ms`), the only 1 ms timer
  on the request path; block threads sleep 10 ms, the API's poll and send
  threads 10 ms.
- *The lost send.* A primary-key read makes two threads of one data node
  send to the same API at almost the same moment: the thread with the LDM
  instance sends `TRANSID_AI`, the thread with the TC instance
  `TCKEYCONF`. The second one runs `do_send(must_send = false)` in the
  receive loop's busy branch; when `trylock(&sb->m_send_lock)` fails it
  only re-registers the transporter in its own pending list (no
  `m_force_send`), and with no progress `do_send` returns false. The lock
  holder's `TCP_Transporter::doSend` sends the snapshot it fetched before
  that data arrived and returns `remain > 0` about the snapshot only, so
  it sees no more work and, with `m_force_send` 0, unlocks.
- *The 1 ms sleep.* With `min_spin_timer_us == 0` the receive loop sets
  `delay = 1` even in an iteration that executed signals (`sum > 0`); with
  spinning it requires `sum == 0 && !has_received`. Spinning is adaptive
  (`LatencyOptimisedSpinning`) and is 0 when `thrman` sees no gain or a
  shared environment — the normal state at 1 thread. So the TC thread
  enters `epoll_wait(1 ms)` with `TCKEYCONF` still queued, nothing wakes
  it (the API is waiting for that reply), and its next loop sends after
  ~1.05 ms. Block threads never sleep in an iteration that executed
  signals and always run `do_send(must_send = true)` first, so layouts
  with separate block threads (NumCPUs >= 8) should not show it.
- *It fits the data:* a fixed ~1 ms quantum (a timeout, not a slow
  wake-up); both engines (the data node is shared); PK reads ~35 % (TC
  and LDM in different threads of the same node), pruned scans < 1 % (the
  TC's `SCAN_TABCONF` rarely overlaps the rows' send); gone at 8 threads
  (threads rarely sleep).

Fix `a22b016ab15` (the branch's net change against `26.10-main`): in
`mt_receiver_thread_main` a round that did work (executed signals or
received data) and whose busy-round `do_send` left transporters
registered with the thread (another thread held their send lock) does
not sleep; it loops once more, and the next idle round's
`do_send(must_send = true)` sets `m_force_send`, so the lock holder sends
the data. Every other round sleeps as before; a round that did no work
may sleep with sends registered (a full transporter), so that case cannot
busy-loop. It replaces the first variant `750c2d3f530` (no sleep in any
round that did work), arm B below, which made every working round pay
one more loop.

Verification (user-run). Configurations in `mysql-test/suite/ronsqlcrunch`,
all with NumCPUs=4 and no CPU binding (runs 4 / 6) unless noted:
`census.cnf` (adaptive spinning, the default), `census_nospin.cnf`
(`StaticSpinning`, `SchedulerSpinTimer=0`: spinning forced off, the F25
path every time), `census_spin50.cnf` (spinning forced on: the control),
`census_benchbox.cnf` (NumCPUs=8, pinned; separate block threads).

| arm | ndbmtd | config | expected at T=1 (`core_pk_lookup`, both engines) |
|---|---|---|---|
| A | base | `census_nospin.cnf` | ~1.2 ms events; on the Mac rare (< 1 %, max / p99.9 only) |
| B / X | fix (B `750c2d3f530`, X `a22b016ab15`) | `census_nospin.cnf` | no ~1 ms events; averages as A |
| C | base | `census_spin50.cnf` | gone (control: the mechanism needs spinning off) |
| D | base | `census.cnf` | as runs 4 / 6 (~35 % slow) on the benchmark computer |
| E | fix | `census.cnf` | gone |
| F | fix | `census_benchbox.cnf` | as run 7 at T=1 and T=8 (no regression) |

Per arm (base = `ndbmtd` of `26.10-main`, fix = this branch; only
`ndbmtd` differs, so rebuild just that target between arms):

```
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py \
    --build prod_build --load both --sf 1 \
    --queries core_pk_lookup,fs_floor,fs_latest,fs_hw_floor,fs_hw_agg_point \
    --engines ronsql,mysqld_nopush --compiler off \
    --threads 1,8 --requests 20000 \
    --cpubind mysql-test/suite/ronsqlcrunch/<config> \
    --out ~/f25_<arm>
```

(on the benchmark computer prefix `taskset -c 24-31` and add
`--client-cpus 24-31`; with `census_benchbox.cnf` also `--expect-ldm 4`).
Read `cases/off_{ronsql,mysqld_nopush}_<entry>_T1.txt`: `Latency: min= avg=
p95= p99=` and, for RonSQL, the `firstbatch` phase; the slow share is
about (avg − min) / 1 ms. At T=8 compare q/s in `report.md` §A (fix within
±5 % of base). A > 5 % slow share left in B or E means a second cause;
then X1 steps 2–4 (C-states first).

**Results, Mac (2026-10-05, MacBook Pro, Apple silicon, `census_nospin.cnf`,
sf 1, 20 000 requests per thread, one run per arm; `~/f25_{A,B,X}`).**
Arm A did not show a slow mode: with spinning off the stall is < 1 % of
requests on the Mac (35 % of PK reads on the benchmark computer), so it
appears only in the tail. At T=1 every A case has a ~1.2 ms request, no
B or X case has one:

| T=1, end to end | A p99.9 / max | B p99.9 / max | X p99.9 / max |
|---|---|---|---|
| RonSQL core_pk_lookup | 235 µs / 1.23 ms | 228 µs / 365 µs | 210 µs / 363 µs |
| RonSQL fs_floor | 1.24 ms / 1.38 ms | 404 µs / 690 µs | 324 µs / 536 µs |
| RonSQL fs_latest | 1.20 ms / 1.40 ms | 430 µs / 669 µs | 368 µs / 783 µs |
| RonSQL fs_hw_floor | 276 µs / 1.43 ms | 272 µs / 340 µs | 235 µs / 355 µs |
| RonSQL fs_hw_agg_point | 293 µs / 1.32 ms | 252 µs / 384 µs | 259 µs / 367 µs |
| MySQL core_pk_lookup | 187 µs / 1.28 ms | 161 µs / 282 µs | 158 µs / 266 µs |
| MySQL fs_floor | 253 µs / 1.22 ms | 241 µs / 410 µs | 276 µs / 464 µs |
| MySQL fs_latest | 2.03 ms / 3.89 ms | 1.49 ms / 1.70 ms | 1.13 ms / 1.38 ms |
| MySQL fs_hw_floor | 213 µs / 1.25 ms | 186 µs / 284 µs | 200 µs / 310 µs |
| MySQL fs_hw_agg_point | 238 µs / 1.29 ms | 226 µs / 323 µs | 215 µs / 337 µs |

Server side (RonSQL `firstbatch`, T=1) the maximum was 1.19–1.32 ms in
A, 0.20–0.35 ms in B, 0.14–0.40 ms in X. Averages: B was +1.5..+13 %
over A at T=1 (`fs_latest` `firstbatch` 109 → 124 µs) and -5..-15 % at
T=8; X is within about ±3 % of A or better everywhere (`fs_latest` 106
µs; RonSQL `fs_floor` -11 % and MySQL `fs_floor` +8 % at T=1, a small
scan that varies both ways between runs). At T=8 p99.9 agrees within
about ±3 % across the arms and the maxima (~1–1.7 ms) are queueing, not
the F25 quantum. Conclusion: X removes the ~1 ms events without the cost
of B. Open: arms D / E / F on the benchmark computer (the size of the
win where F25 is frequent, and the run-7 layout); a small T=1 cost below
~3 % would need interleaved repeats to rule out.

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
5. *Leak or load* (added 2026-09-24, after the node logs showed JoinAgg
   park-sweep aborts and 1869 from the join-aggregation per-row path
   before the failure, but no memory figures). `ronsql_bench_matrix.py`
   now reads `ndbinfo.resources` (QUERY_MEMORY, TRANSACTION_MEMORY)
   around every case: idle before, the peak sampled every
   `--mem-sample` seconds while it runs, idle `--mem-settle` seconds
   after; the summary line shows `query memory before/peak/after` and
   flags `RETAINED`, report §H lists every case in run order with the
   idle drift over the run. Rerun `offline_fs` (and `tpch_cte`) with
   it, e.g. `--queries offline_fs --threads 1,8 --requests 3
   --mem-sample 0.5`: a peak that falls back to the idle level is load
   (raise `SharedGlobalMemory` or lower concurrency); memory still held
   after the case, or an idle level climbing across cases, is a leak
   (aborted / swept JoinAgg states or self-continue requests not
   releasing query memory).

**Decision rule.** Any `ndbrequire` / abort in the trace ⇒ P0 fix
before the next census run. Pure exhaustion ⇒ document the budget in
`census.cnf` and `ronsqlcrunch/my.cnf`, keep the `--requests 3`
isolation for `offline_fs` until the memory reject is clean (no 1869,
no failure), and open the engine item for a graceful reject.

**Result.** (a) found 2026-09-24: DBSPJ `do_init` allocated the
Request's per-node arrays under `ndbrequire` — fixed in `914cbf9bbb7`
(OutOfQueryMemory REF, error insert 17534). What exhausts the query
memory (step 5) and the 1869 masking (step 4) are open.

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
