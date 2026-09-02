# TTL 899 probe tests — divergence detector, 899 control, copy-fragment churn probe

Companion to `findings.md` (analysis) and `impact_analysis.md` (sibling).
This file documents the three test artifacts built to (a) provide a reusable
replica-divergence detector, (b) validate the error-899 detection recipe, and
(c) empirically stress-probe the "defended" node-restart copy windows
(findings §5 S1/S2) for a live presence-divergence seed.

Date: 2026-08-01. Branch: 25.10-main. Build: `debug_build/`
(Debug, WITH_ERROR_INSERT=1). All artifacts are NEW files; no existing test
or kernel file was modified.

---

## 1. Artifacts

| File | What it is |
|------|------------|
| `mysql-test/include/ndb_assert_frag_replica_consistency.inc` | Reusable invariant: per-fragment physical row-count equality across replicas |
| `mysql-test/suite/ndb_ttl/t/ttl_rowid_899_control.test` (+ `r/*.result`) | Guaranteed-positive control for the 899 trap (ERROR_INSERT 4019); documents the SQL surfacing of 899 |
| `mysql-test/suite/ndb_ttl/t/ttl_nr_copy_churn.test` (+ `.cnf`, `r/*.result`) | Copy-fragment × TTL-churn race probe over 3 node-restart cycles |

How to run (distinct build-thread/vardir to coexist with sibling agents):

```
cd debug_build/mysql-test
./mysql-test-run.pl --suite=ndb_ttl ttl_rowid_899_control ttl_nr_copy_churn \
    --build-thread=320 --vardir=var-ttlprobe
```

---

## 2. Deliverable 1 — `ndb_assert_frag_replica_consistency.inc`

### What it asserts and why

For a given `$ndb_db.$ndb_table`, every fragment must be reported by exactly
`$ndb_expected_replicas` data nodes (default 2) in
`ndbinfo.memory_per_fragment`, with **identical** `fixed_elem_count` and
`var_elem_count` on all of them. Element counts are maintained at rowid
alloc/free time (`DbtupFixAlloc.cpp` `alloc_fix_rec`/`alloc_fix_rowid`/
`free_fix_rec`), so equality is exactly the "replica rowid layouts in
lockstep" invariant whose violation is the birth condition of error 899.
Expired-but-unpurged TTL rows are physically present and counted on all
replicas, so the comparison is exact — no tolerance.

Implementation notes:

- `ndbinfo.memory_per_fragment` is a view over `ndb$frag_mem_use` joined with
  `ndb$dict_obj_info` (`storage/ndb/plugin/ha_ndbinfo_sql.cc:475`). Relevant
  columns: `fq_name` (`<db>/def/<table>`), `parent_fq_name`, `type`,
  `node_id`, `block_instance`, `fragment_num`, `fixed_elem_count`,
  `var_elem_count`. The include SUMs over `block_instance` (a fragment lives
  on one LDM instance per node) and groups by `(fq_name, node_id,
  fragment_num)`.
- The replica-count guard (`COUNT(*) <> $ndb_expected_replicas` per fragment)
  prevents a *vacuous pass* when a node's ndbinfo rows are missing (node
  still starting), and a `>0 fragments visible` guard prevents a vacuous pass
  when the table has no ndbinfo rows at all yet.
- The check retries (default 60 × 0.5 s) before failing: ndbinfo reads are
  live, and an in-flight commit (commits run backup-first) or a
  freshly-rejoined node can make one sample differ transiently. A real fork
  is permanent and still fails on the last retry.
- Optional `$ndb_check_children=1` extends the scope to unique-hash-index
  fragments and BLOB part tables (replica-symmetric for the same reason).
  Ordered (TUX) indexes are always excluded: they are rebuilt locally on
  restart and their internal node counts are not replica-comparable.
- Optional `$ndb_dump_on_fail=1` additionally writes
  `$MYSQLTEST_VARDIR/log/narc_<table>.rowid.dump` via
  `ndb_select_all --rowid --gci` on failure (note: a plain scan, so on a TTL
  table it contains only non-expired rows).
- On violation it prints the full per-node per-fragment counts
  (`--vertical_results`) and `--die`s. Dependency-free (no other includes),
  so `ndb`, `ndb_ttl` and `ndb_ttl_purge` suites can all source it.

---

## 3. Deliverable 2 — `ttl_rowid_899_control.test`

### Purpose

Any 899-hunting test needs a guaranteed-positive control proving its trap
fires. ERROR_INSERT 4019 (`dbtup/DbtupExecQuery.cpp`, `handleInsertReq`,
rowid-specified arm) forces `terrorCode=899` on every **rowid-specified**
insert. Only backup-replica inserts are rowid-specified — the primary
allocates its own rowid (`alloc_fix_rec` arm, 4019 not consulted) and
forwards the chosen rowid. Arming ALL nodes (`ALL ERROR 4019`) therefore
makes the backup of any fragment fail exactly like a real rowid collision on
a forked backup, with no other path affected.

### Empirically established SQL surfacing of NDB error 899

Recorded from the actual run (this is the production symptom signature):

- The statement fails with **`ERROR HY000: Lock wait timeout exceeded; try
  restarting transaction`** — i.e. **`ER_LOCK_WAIT_TIMEOUT` (1205)**, because
  `ndberror.cpp` maps `{899, HA_ERR_LOCK_WAIT_TIMEOUT, TR, "Rowid already
  allocated"}`.
- `SHOW WARNINGS` contains exactly:

  ```
  Warning  1297  Got temporary error 899 'Rowid already allocated' from NDB
  Error    1205  Lock wait timeout exceeded; try restarting transaction
  ```

  1297 is `ER_GET_TEMPORARY_ERRMSG` (pushed by `ndb_to_mysql_error()`,
  `ha_ndbcluster.cc`). **The warning text is the only reliable 899
  signature** — the statement error alone is indistinguishable from a real
  lock-wait timeout, which is precisely why a production rowid fork
  masquerades as intermittent retriable lock timeouts ("899 hides behind
  lock-wait timeout").

### Flow

1. Create TTL table (TTL=300 s, rows stay live), baseline INSERT succeeds.
2. `ALL ERROR 4019` → INSERT fails with `ER_LOCK_WAIT_TIMEOUT`; `SHOW
   WARNINGS` recorded; a `query_get_value` assertion dies if the
   `Rowid already allocated` text is absent (trap-validation independent of
   result-file diffing).
3. Row verified absent (transaction aborted cleanly on all replicas).
4. `ALL ERROR 0` → INSERT succeeds again.
5. Final `ndb_assert_frag_replica_consistency.inc` — the aborted insert must
   leave no divergence.

Guarded by `include/have_ndb_debug.inc` (EI 4019 exists only in
WITH_ERROR_INSERT builds).

---

## 4. Deliverable 3 — `ttl_nr_copy_churn.test`

### What it probes

Findings §3.4: TTL tables forward insert-over-expired (`ZINSERT_TTL`, D1) and
every `ZWRITE` (D2) to backups **without a rowid**; a backup that misses the
row silently self-allocates a different rowid (permanent fork → delayed 899).
Findings §5 says the node-restart copy windows are DEFENDED (copy scan is
ttl-ignored; starting replica chained directly after the primary; live node
sends rowids for ALL ops during copy per `handle_nr_copy`). The sibling
impact analysis concurs the copy-phase self-allocation window is likely
closed. This test is the empirical check: it hammers all rowid-less channels
through three real restart windows and asserts no fork is born.

### Design

- Cluster: `ttl_nr_copy_churn.cnf` = `suite/ndb/my_2rpl.cnf` (2 ndbd,
  NoOfReplicas=2, one node group — same as the sibling restart tests).
- TTL table `churn_t` (TTL=300 s, `ttl_index` on the TTL column for the
  production purge shape, `PARTITION BY KEY (id) PARTITIONS 2`).
- Row pools, loaded in this order (copy scan walks rowid ≈ load order, so
  the last-loaded pool is copied LAST — churn there maximizes ops arriving
  for not-yet-copied rows):
  - A: PKs 1..800 EXPIRED (lowest rowids — "already copied" lap),
  - L: PKs 1001..1800 LIVE,
  - B: PKs 2001..2800 EXPIRED, never churned — inert expired-unpurged
    backlog the copy must materialize,
  - H: PKs 100001..100800 EXPIRED, loaded LAST (highest rowids) — the
    biased target.
- Churn runs on a second connection whose session has
  `ttl_expired_rows_visible_in_delete=1`, so SQL DELETE takes the same
  distributed ttl-ignored delete shape as the RDRS purge — note this
  session variable DEFAULTS TO OFF (`sql/sys_vars.cc`; the findings/task
  shorthand "SQL DELETE ignores TTL expiry" holds only with the variable
  on). Channels driven per supervisor round:
  1. `INSERT` over expired H PK → ACC dup-convert → `ZINSERT_TTL` (D1)
     [direct statement],
  2. `REPLACE` re-expiring that H PK → ZWRITE-resolved-to-update (D2)
     [direct],
  3. ttl-ignored `DELETE` of another expired H PK (purge shape) [direct],
  4. `REPLACE` re-creating the deleted H PK expired → ZWRITE-resolved-to-
     insert (D2) [direct],
  5.–6. same INSERT+re-expire lap on low-PK pool A (already-copied
     region) [direct],
  7. `REPLACE` over a live L row [SP `churn_run`, ×15 per round],
  8.–9. fresh-PK INSERT + trailing DELETE (rowid free-list cycling)
     [SP `churn_run`, ×15 per round].
- **Supervised split-channel bursts, not one long CALL** (empirically
  forced — see §5.4): each round (`suite/ndb_ttl/include/ttl_churn_burst.inc`)
  is (1) `CALL churn_run(15)` containing ONLY the never-fatal channels
  (live-row ZWRITE, fresh insert, trailing delete; session-var cursor
  `@churn_i` makes bursts continue where the previous stopped, so progress
  is guaranteed), plus (2) six individually `--error`-tolerated top-level
  statements driving the fatal-prone expired-row channels
  (INSERT-over-expired H and A, ttl-ignored DELETE H, and their
  re-expiring/re-creating REPLACEs). Node state is polled between rounds
  pure-SQL via `ndbinfo.nodes` (`status='STARTED'` row presence);
  poll-budget exhaustion only stops the churn — `ndb_waiter` afterwards is
  the authoritative barrier. This keeps every channel firing at the copy
  window through the whole stop + downtime + rejoin/copy sequence even
  though mysqltest is single-threaded.
- Error tolerance (three layers, all counted per cycle into
  `$MYSQLTEST_VARDIR/log/ttl_nr_copy_churn.counters`, which is
  timing-dependent and never enters the result file):
  - In-SP: the SQLEXCEPTION handler swallows every non-fatal statement
    error but scans the failed statement's complete diagnostics area
    (`GET STACKED DIAGNOSTICS`) and buckets it: `Rowid already allocated`
    → `@churn_899` (tallied for the cycle-end persistence verdict);
    `Tuple did not exist` / duplicate-key / 626 / 630 → `@churn_known`
    (the impact-analysis sibling's known copy-window transient: during an
    active NR copy an INSERT-over-expired on an ALREADY-COPIED range is
    626-aborted until copy mode ends, `DbtupExecQuery.cpp`
    original_op_type gate ~:2216/:2226 + :3498-3521); everything else
    (node-failure temporaries 4010/4025/1204 etc.) → `@churn_other`.
  - Supervisor level: **fatal engine errors bypass SP handlers**
    (`handler::is_fatal_error`, §5.4). A fatal that still reaches the CALL
    (rare, since the fatal-prone channels are outside the SP) is tolerated
    at the mysqltest level (`--error 0,1030,1032,1180,1296`) and counted in
    `$_call_fatal`; each direct fatal-prone statement carries its own
    tolerance list (`0,1030,1032,1062,1180,1205,1296,1297`) and failures
    count into `$_direct_err`. Every failure's warning stack is scanned for
    the 899 signature (`$_sig899_fatal`, with forensic lines appended to
    the counters file).
  - 899 classification (coordinator refinement): benign TRANSIENT 899s are
    possible on a healthy cluster around abort/TC-takeover windows and
    self-heal on retry, so 899 signatures seen during the bounce are only
    COUNTED (`transient_899` in the counters file). The POSITIVE result is
    **persistence**: a strict post-cycle sweep operation still failing
    after 3 retries on the quiesced cluster, or any replica count
    divergence — a real fork permanently shifts `fixed_elem_count`, so the
    invariant include is the authoritative persistence detector.
  A liveness floor (≥10 iterations/cycle) guards against a vacuously green
  probe.
- Restart cycles (churn running throughout stop + downtime + rejoin/copy):
  1. node 2 graceful (`2 RESTART -n` → waiter → `2 START` → waiter),
  2. node 1 graceful,
  3. node 2 **initial** (`RESTART -n -i`): wiped filesystem, full-table
     copy — the widest natural window, and the whole expired backlog must
     be re-materialized row-identically.
- **Copy-window widening lever: ERROR_INSERT 5106** (DBLQH,
  `DblqhMain.cpp` `execLQHKEYCONF` `COPY_CONNECTED` arm): on the copy
  SOURCE node, every 20th copy LQHKEYCONF is re-posted to itself with a
  500 ms delay — the exact "DelayedCopy" lever testNodeRestart uses
  (`restarter.insertErrorInAllNodes(5106)`). Armed (`ALL ERROR 5106`) for
  both graceful cycles; NOT armed for the initial cycle (a throttled
  full-table copy would take minutes; the full copy is naturally wide).
  Disarmed (`ALL ERROR 0`) after each cycle. The copy region of DBLQH has
  no other delay-type EI in this tree (5714 = copy markers logging only,
  5043 = kill at copy completion).
- After every cycle: counter collection + liveness check, then
  `ndb_assert_frag_replica_consistency.inc` (must pass), then a strict
  fresh-PK sweep — 150 inserts, 150 deletes, 150 re-inserts at the same
  PKs, which forces the primary to hand out just-freed rowids and the
  backup to accept them at the same positions. A sweep op hitting
  `ER_LOCK_WAIT_TIMEOUT` is retried 3× (0.5 s apart): clearing = counted
  transient; still failing = persistent → `SHOW WARNINGS` + rowid dump +
  die.
- Failure = positive finding. On ANY of (persistent sweep 899, invariant
  violation, poll-budget/waiter timeout): the test dies loudly; mtr
  preserves `var-ttlprobe/`; rowid dumps land in
  `var-ttlprobe/log/*.rowid.dump`.

### Expected outcome

GREEN — findings §5 and the impact analysis predict the defenses hold. A red
run is a live seed for the §6 amplifier and must be escalated with the
preserved vardir + dumps.

---

## 5. Results

### 5.1 Verdict: GREEN — the defended windows held under stress

All three artifacts pass; each recorded test was re-run green multiple
times (control: record + 3 verification runs; churn probe: record + 2
verification runs, plus the recording run itself). **No persistent 899, no
transient 899, and no replica count divergence was observed in any run** —
the findings §5 S1/S2 defense analysis survives empirical falsification at
this stress level.

Per-cycle counters were identical in PATTERN across all green runs
(`var-ttlprobe/log/ttl_nr_copy_churn.counters`), e.g. the final
verification run:

```
cycle 1 node=2 args=''   iters_delta=1830 known_transient=0 other_swallowed=0 call_fatal=0 direct_err=4 transient_899=0 sweep_transient=0
cycle 2 node=1 args=''   iters_delta=1755..2010 (runs vary)            ...   direct_err=4 transient_899=0 sweep_transient=0
cycle 3 node=2 args='-i' iters_delta=1710..1905                        ...   direct_err=0 transient_899=0 sweep_transient=0
```

- ~1700–2000 safe-channel churn iterations (≈3×that many statements) per
  cycle, plus 6 expired-channel statements per supervisor round.
- `direct_err` was **exactly 4 on every graceful cycle and 0 on every
  initial cycle, in every run** — the reproducible footprint of the
  626-on-expired copy-window gate hitting the expired-row channels while
  the copy is in flight (and its absence on an initial restart, where the
  starting node has no pre-existing expired rows for the gate to protect).
- `known_transient`/`other_swallowed`/`call_fatal` = 0 everywhere: with
  the fatal-prone channels outside the SP, the safe channels never failed
  even across stop/failover — replica-2 failover was seamless for them.
- 3 × 450 sweep operations/run (insert + delete + re-insert on fresh PKs,
  cycling primary free lists into backup required-rowid inserts): zero
  lock-wait/899, zero retries needed (`sweep_transient=0`).
- Invariant include: PASS at baseline, after each of the 3 cycles, and at
  final quiesce — 5 PASSes per run, exact per-fragment
  `fixed_elem_count`/`var_elem_count` equality on both replicas each time.

### 5.2 Window-coverage evidence (verification-run cluster log)

From `ndb_3_cluster.log` of the final green run (timestamps UTC):

| Cycle | Stop window (churned) | Synchronize/copy window (churned) | Start→Started |
|-------|----------------------|------------------------------------|---------------|
| 1: node 2 graceful | 22:50:00.8→22:50:18.9 (18.1 s) | 22:50:23.8→22:50:29.9 (6.1 s) | 28.3 s |
| 2: node 1 graceful | 22:50:52.3→22:51:07.9 (15.6 s) | 22:51:14.1→22:51:20.2 (6.1 s) | 29.5 s |
| 3: node 2 initial  | 22:51:42.6→22:51:58.2 (15.6 s) | 22:52:04.5→22:52:10.5 (6.0 s) | 29.5 s |

EI 5106 demonstrably fired: the copy-source data nodes logged
`LQH <n> delaying copy LQHKEYCONF` 174 times (node 1, cycle-1 source) and
84 times (node 2, cycle-2 source) — i.e. thousands of copy row-ops flowed
with every 20th CONF delayed 500 ms across the 4 LDM instances. The
~6 s synchronize windows carried multiple full supervisor rounds
(safe-channel burst + all six expired-channel statements) each.

Runtimes: churn probe 141–153 s per run; control test <0.5 s; a combined
suite invocation ≈4 min wall including cluster start — comfortably inside
the 15-minute mtr testcase budget.

### 5.3 mtr integration note

`check-testcase` lists `$MYSQLTEST_VARDIR/tmp` before/after each test, so
persistent artifacts (the counters file, failure dumps) must live in
`$MYSQLTEST_VARDIR/log/` instead — the first otherwise-green run failed
the post-test check purely because the counters file appeared in tmp/.

### 5.4 Incidental finding — copy-window transients surface as UNCATCHABLE SQL errors

The first probe implementation ran the churn as one long `CALL` with a
`CONTINUE HANDLER FOR SQLEXCEPTION`, expecting to swallow all bounce-window
transients. It died anyway:

```
Query 'reap' failed. ERROR 1032 (HY000): Can't find record in 'churn_t'
```

Root cause chain: during the bounce a churn statement on an expired row got
the row-not-found transient (the impact-analysis 626-on-expired copy-window
gate family); NDB's 626 maps to `HA_ERR_KEY_NOT_FOUND`;
`handler::is_fatal_error()` (`sql/handler.cc:4195`) treats the
`HA_ERR_KEY_NOT_FOUND` family as FATAL (only lock-wait/deadlock and a few
others are exempt), and `print_error` raises `ER_KEY_NOT_FOUND` (1032) with
`ME_FATALERROR` — **fatal errors bypass stored-program condition handlers
by design**.

The second iteration (short supervised CALLs, expired-row channels still
inside the SP) refined the picture: from the moment the rejoining node
enters the synchronize/copy phase, the expired-row INSERT at the head of
the churn loop failed with that fatal 1032 on EVERY burst (the CALL died in
milliseconds at its first statement, before the iteration cursor advanced,
so the same PK was retried and kept failing for the whole window — a
~12 s copy window burned a 400-round poll budget). Cluster-log
cross-check: node 2 was healthily mid-restart ("Synchronize start node
with live nodes" -> "Wait LCP to ensure durability") the whole time; the
churn, not the cluster, was the invalid part.

Practical consequences worth recording:

- Any application-side stored-procedure retry logic around TTL tables
  CANNOT absorb the copy-window row-not-found transients; only
  client-level retry can (the probe's split-channel supervisor pattern).
- For test authors: a stored-procedure churn driver that touches expired
  rows is structurally unable to run through a node bounce; the
  fatal-prone channels must be top-level statements with client-side
  `--error` tolerance.

---

## 6. Known limitations

- The probe drives churn through SQL (mysqld) — NdbAPI-only shapes (e.g.
  `OO_TTL_IGNORE` writes) are not exercised; findings §9.2 lists an NDBT
  purge-clone as the follow-up for single-actor purge control.
- No RDRS purger runs in MTR; the purge *shape* is emulated by ttl-ignored
  SQL DELETE (same distributed delete path, findings §3.3), but the real
  scan-driven purger's takeover-delete pipelining is not reproduced.
- NoOfReplicas=2 only; the 3-replica middle-forwarding amplification
  (findings D1, `Dblqh.hpp:5556`) needs the `ttl_replica3_write.cnf`-style
  config — a candidate extension.
- `ndb_select_all --rowid` dumps (fail paths) contain only non-expired rows
  (the tool has no ttl-ignore scan option in this tree).
