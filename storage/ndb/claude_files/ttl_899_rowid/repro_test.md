# TTL 899 natural-interleaving reproduction — trace verdict and the deterministic copy-window test

Companion to `findings.md` (defect mechanics), `impact_analysis.md` (fix review
basis), `normal_insert_analysis.md` (normal-operation abort/commit windows) and
`probe_tests.md` (divergence detector + 899 control + churn probe, built by the
probe sibling). This document covers the REDIRECTED "Test A" task: instead of a
state-fabricating error insert, find and drive a NATURAL interleaving in which
TTL causes a backup to use a rowid the primary did not dictate — or prove the
candidate windows closed and pin them with a regression test.

Date: 2026-08-01. Branch: 25.10-main. Build: `debug_build/` (Debug,
WITH_ERROR_INSERT=1). Line numbers from that date's tree (my working tree adds
the ERROR_INSERT 5113 hook, which shifts DblqhMain.cpp numbers below its edit
points by up to ~80 lines; symbol names are the durable anchors).

---

## 0. TL;DR

1. **The prime natural candidate — rowid-less D1/D2a operations hitting a
   starting node during the node-restart copy — is CLOSED on the current
   tree.** Three cooperating mechanisms defend it (section 2); my trace
   independently reached the same verdict as the impact-analysis sibling's
   §4.1 and the normal-insert sibling's copy-phase note, and additionally
   verified the three transition boundaries (section 3).
2. No other natural interleaving produces a backup-side rowid
   self-allocation on the audited tree: normal-operation candidates were
   ruled out by `normal_insert_analysis.md` (divergence impossible; only
   benign TRANSIENT 899s exist around abort/TC-takeover windows), and the
   restart candidates by findings §5 + this trace.
3. Deliverable therefore = the honest closure conclusion (this document)
   plus a **deterministic natural-race regression test**
   (`mysql-test/suite/ndb_ttl/t/ttl_nr_copy_window.test`) that REALLY
   commits D1/D2a/purge-shaped/fresh/delete traffic against a half-copied
   TTL fragment — provably inside the copy window — and asserts that no
   fork forms (replica-identical physical row counts; a rowid-recycling
   churn that would hit 899 on any self-allocation). It uses one NEW
   timing-only kernel ERROR_INSERT (5113) that merely slows the copy scan
   and logs copy start/complete markers.
4. Along the way the test pins TWO deterministic pre-existing behaviors:
   the mid-copy 626 transient on insert-over-expired for already-copied
   rows (surfaces as SQL 1032), and its self-healing after copy
   completion.

**ERROR_INSERT code claimed by this task: DBLQH 5113 (timing only).**
The fix sibling's branch separately reserves DBLQH 5118 for its
replica-rowid-mismatch autotest hard-stop; 5113 was registered in
`storage/ndb/src/kernel/blocks/ERROR_codes.txt` (with "Next DBLQH" bumped to
5114) — when merging with the fix branch, keep both entries and set
"Next DBLQH" to max(5114, 5119).

---

## 1. The hypothesis that was tested

During a node restart the starting node is inserted into the replica chain
directly after the primary and receives ALL live write traffic while the copy
scan is still filling its fragment in rowid order (chain-insertion design
comment `DblqhMain.cpp` execLQHKEYREQ ~:9601-9644). The TTL channels D1
(insert-over-expired → ZINSERT_TTL, forwarded as rowid-less ZINSERT;
findings §3.4) and D2a (TTL ZWRITE resolved to update, forwarded as rowid-less
ZWRITE) would — if they arrived rowid-less at a starting node that has not yet
received the row — be re-resolved by PK, miss, and self-allocate via
`alloc_fix_rec`: a natural seed requiring no error insert.

## 2. Why the window is closed — the three defenses (trace, file:line)

All in `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` unless noted.

**D-A. Pre-copy: the starting node ignores everything (AC_IGNORED).**
`PREPARE_COPY_FRAG_REQ` sets the fragment to `AC_IGNORED` and
`fragStatus = ACTIVE_CREATION` on the starting node (:21106-21121 — "ignore
transactions (but still pass them on to the next replica) before we have seen
the first copy row"). Every op received in that state is consumed
(TUP op aborted) and forwarded/CONFed without being applied
(dispatch :10107-10161, ignore branch :10140-10161; per-op `activeCreat`
seeded from the fragment state at :9572-9587). Ignored ops are safe because
the full-fragment copy that follows delivers the final row states: rows
touched while ignoring have rowGCI above the starting node's restore GCI, so
the GCI-filtered copy scan re-delivers them (delete-by-rowid covers deletes,
`handle_nr_copy` cases 4/7 :10405-10454).

**D-B. During copy: the live node attaches a rowid to EVERY forwarded op.**
At copy-scan start on the live node — which is the fragment's PRIMARY, i.e.
the node that forwards all replica traffic to the starting node —
`accScanConfCopyLab` sets `m_copy_started_state = AC_NR_COPY` with the comment
"Start sending ROWID for all operations from now on" (:21548-21555). From then
on `packLqhkeyreqLab` forces the rowid flag for every forwarded op:
`m_use_rowid |= (m_copy_started_state == AC_NR_COPY)` (:12448-12450). This
includes the TTL channels:

- For ZINSERT_TTL and ZWRITE-resolved-ZUPDATE, `m_row_id` holds a VALID
  frag-relative rowid: `acckeyconf_tupkeyreq` stores the ACC-found local key
  into `m_row_id` for every found-row op (:11699-11700), the same value
  every plain UPDATE forwards during copy. The wire words are written at
  :12593-12597.
- The starting node keys `handle_nr_copy` on that wire rowid
  (:10321-10591): match ⇒ in-place (op converted to ZWRITE, :10522-10536,
  skipping the replica-side checkTTL by design); no-match ⇒ delete-at-rowid
  + delete-by-PK + INSERT AT THE WIRE ROWID (:10550-10573 → `exec_acckeyreq`
  → TUP required-rowid allocation via the wire flag taken at :9099). The TTL
  ZWRITE new-key exception (:10501-10502, :10550-10551 — fix 310f19cbd03)
  makes the D2a shape take the same insert path instead of being ignored.
  Placement therefore always matches the primary; the later copy row for
  that rowid takes the match path (:10390-10404, INSERT→ZUPDATE). The
  normal-insert sibling independently confirmed a rowid-carrying insert
  during copy even HEALS a hypothetical pre-existing PK-at-wrong-rowid state
  (:10559-10573).
- The ACC dup-convert consumed by these paths is presence-based
  (`DbaccMain.cpp:1490-1515`), and the NR-copy insert path cannot
  accidentally dup-convert onto a stale element because `handle_nr_copy`
  deletes at-rowid and by-PK first (:10468-10494) — NoTTLDupConvert
  (:10197, restore-only) is not needed there (impact_analysis §4.1 suggests
  setting it anyway as future-proofing).

**D-C. Post-copy: rowid-less resumes only after every copy row is ACKed.**
The rowid-attachment state is cleared in `closeCopyLab` (:22104-22113
"Stop sending ROWID for all operations from now on") which is gated on
`copyCountWords == 0` (:22087-22096) — i.e. the live node waits until the
starting node has CONFIRMED every outstanding copy row before any rowid-less
op can be packed again. The starting node treats the first rowid-less op as
the end-of-copy sentinel (:10328-10343) — by then all copy rows are already
applied. A direct `COPY_FRAG_DONE_REP` (live→starting, :22115-22153; receiver
:28691-28708 tolerating all three states) additionally closes the historic
master-relayed COPY_FRAGCONF race for the rowid-carrying normal INSERT shape.

## 3. Boundary verification (the adversarial part)

1. **Flip-ON boundary.** `m_copy_started_state` is per-fragment (set through
   `fragptr` of the fragment being copied, :21542-21551). Ops PACKED before
   the flip are rowid-less but travel the same LDM→LDM signal path as the
   copy rows, which are only produced after the flip in the same LDM thread:
   FIFO ⇒ every rowid-less op arrives before the first copy row, i.e. while
   the starting node is still AC_IGNORED (enters AC_NR_COPY only on the
   first NrCopyFlag op, :9485-9508). Writes are never forwarded from query
   threads (`ndbassert(!m_is_in_query_thread)` :12591), and with the
   multi-transporter setup the LDMx(P)→LDMx(S) pair shares one ordered
   channel for both copy rows and forwarded ops.
2. **Flip-OFF boundary.** Covered by the `copyCountWords == 0` gate above:
   no reordering argument is even needed — completion is acknowledged, not
   inferred.
3. **Sender coverage.** The starting node receives LQHKEYREQ for a fragment
   only from the PREVIOUS replica in the chain, and the chain places the
   starting node DIRECTLY AFTER the primary (:9601-9644), which is also the
   copy source (COPY_FRAGREQ executes on the primary; same fragptr feeds
   :21551 and :12448). With 3 replicas the middle position is the starting
   node itself; the old backup (last in chain) receives its hop from the
   starting node, whose forward re-emits the wire rowid flag (:9099 sets
   `m_use_rowid` from the wire; ignored ops forward `m_row_id` unchanged,
   applied ops overwrite it with the same lockstep value :11699-11700). The
   last replica performs no sentinel interpretation, so the cross-channel
   (direct copy rows vs chained ops) ordering question does not arise for
   any node that interprets rowid-less arrivals.
4. **Old-dist-key stragglers.** Once the copy scan starts, transactions
   still using the pre-insertion distribution key are REFUSED by the primary
   (distribution-key check :9593-9599 + design comment) — no op can bypass
   the chain and miss the starting node.

**Verdict: the natural copy-window seed does not exist on the current tree.**
Consequence flavor if the protocol ever regresses (e.g. a future rowid-less
leak mid-copy): the sentinel (:10328) flips the half-copied fragment to
AC_NORMAL and the next copy row crashes the starting node on
`ndbrequire(rowidFlag && op == ZDELETE)` (:10120-10123) — loud, not silent;
the silent-fork flavor needs the leak to happen in the final in-window moments.
Both flavors are exactly what the new regression test would catch (crash ⇒
waiter timeout; fork ⇒ count divergence / 899 churn).

## 4. The deterministic natural-race regression test

Files:

- `mysql-test/suite/ndb_ttl/t/ttl_nr_copy_window.test` (+ `.cnf` including
  `suite/ndb/my_2rpl.cnf`, + recorded `r/ttl_nr_copy_window.result`)
- kernel: ERROR_INSERT **5113** (timing only) — see section 5.
- reuses `mysql-test/include/ndb_assert_frag_replica_consistency.inc`
  (probe sibling's detector) and the 899 SQL signature established by
  `ttl_rowid_899_control.test` (ER_LOCK_WAIT_TIMEOUT 1205 + warning 1297
  "Got temporary error 899 'Rowid already allocated' from NDB").

Choreography and WHY each step exists:

1. **Single-fragment TTL table** (`PARTITIONS 1`) — one primary node, one
   backup node, no cross-fragment noise; the restarted node is computed to
   be the BACKUP (`ndbinfo.table_fragments.current_primary`) so the
   surviving node keeps the allocator/copy-source role (restarting the
   primary would move primaryness mid-test and blur which node dictates
   rowids).
2. **Load 1500 pre-expired + 100 live rows WHILE the backup is down.**
   A graceful `RESTART -n` stop first; the rows therefore all have
   rowGCI > the backup's restore GCI, so the GCI-filtered NR copy must
   deliver EVERY row — the copy window has real width and known content
   (rows inserted before the stop would be restored locally and skipped by
   the copy scan). Insertion in id order makes rowid order ≈ id order
   (verified via the copy-position behaviors), so HIGH ids are copied LAST.
   Expired rows use `NOW() - 1h`, live `NOW() + 1h`, with the session pinned
   to UTC (`SET time_zone='+00:00'`) because checkTTL compares against the
   data node's UTC clock.
3. **Arm EI 5113 on the LIVE node, start the backup, wait for the
   copy-start log marker** ("Starting copy of tab(<id>,0)" in the live
   node's ndbd.log), then sleep 2 s so the crawling scan (~10-13 ms/row
   measured) is ~150 rows in: ids 1..~100 are certainly copied, ids
   ≥ ~1400 certainly not.
4. **The race (all inside the window):**
   - transient demo: INSERT over expired id 2 (already copied) must fail
     with 1032 — the deterministic mid-copy 626 transient;
   - D1: INSERTs over expired ids 1496-1500 (not yet copied) — must succeed;
   - D2a: REPLACEs over expired ids 1481-1485 (not yet copied) — must
     succeed;
   - purge-shaped ttl-ignored deletes (`ttl_expired_rows_visible_in_delete`)
     of one copied and one not-yet-copied expired row — the RDRS purge wire
     shape crossing the window;
   - fresh-key inserts (rowid-carrying control) and live-row deletes
     (backup-first dealloc crossing the window).
5. **Window proof:** assert the copy-COMPLETE marker has NOT yet appeared.
   This converts "hopefully raced" into "provably raced" — the step order is
   load-bearing for the test's value.
6. **Detection:** after the node rejoins — replica-identical per-fragment
   physical row counts (exact, no tolerance), the previously-failing
   insert-over-expired now succeeding (transient self-heal proof), and the
   **899 tripwire**: delete the race rows and refill the freed slots with
   fresh keys. The primary's page free-list PREPENDS freed slots
   (`Tup_fixsize_page::free_record`, `dbtup/tuppage.cpp:114-136`:
   `next_free_index = page_idx`), so the refill inserts reuse exactly the
   race rows' slots and are forwarded with required rowids — any
   self-allocation during the race would surface here as the 899 signature.
   The churn is strictly sequential on a quiesced healthy cluster, so ZERO
   tolerance is correct; under concurrency a fork hunt must instead require
   RECURRENCE of 899 plus layout evidence (benign transient 899s exist:
   `normal_insert_analysis.md`).
7. **Phase 2 — INITIAL restart** (`RESTART -n -i`): the backup is wiped, the
   copy delivers every row (restore GCI 0) on the initial-NR code path;
   race a slimmer op set in the (occurrence-counted) second window, re-run
   the detection and tripwire. This also restores a pristine backup at test
   end.

## 5. The timing-only kernel ERROR_INSERT 5113

Files touched (working tree, not committed):

- `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp` — new ERROR_INSERT-guarded
  CONTINUEB code `ZDELAY_NEXT_COPY_ROW` (45) and an ERROR_INSERT-only
  reentry member `m_delay_copy_reentry` (in-class initialized false).
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` —
  1. `nextRecordCopy` (the single choke point every next-copy-row fetch of a
     copy scan goes through, from all its callers): when 5113 is armed, the
     fragment being copied is a TTL table (`is_ttl_table` via the copy op's
     fragment) and the call is not the delayed continuation itself, the
     fetch is parked in a 20 ms delayed CONTINUEB instead. Nothing else
     changes: same states, same protocol, every row still delivered — a
     slow copy scan, which large fragments produce naturally.
     (First attempt parked in `copyCompletedLab`; empirically that only
     throttles the window-full path — the hot path advances via the direct
     `nextRecordCopy` calls after each row send, so a 1600-row fragment
     still copied in 66 ms. Parking inside `nextRecordCopy` throttles all
     paths uniformly.)
  2. `execCONTINUEB` case `ZDELAY_NEXT_COPY_ROW`: revalidates the copy
     process exactly like `resume_one_copy_fragment_process` (record still
     COPY_CONNECTED, scan still WAIT_LQHKEY_COPY; close if completed;
     `nextRecordCopy` itself handles the halt machinery) and re-invokes
     `nextRecordCopy` with `m_delay_copy_reentry` set so that one call runs
     for real. Guards make a stale parked continuation a no-op if another
     path already advanced the scan.
  3. The existing 5714 copy start/complete log markers also fire for 5113
     ("Starting copy of tab(T,F)" / "Copy of tab(T,F) complete") — needed
     because only ONE error insert value can be armed per block, so 5113
     must provide its own synchronization markers.
- `storage/ndb/src/kernel/blocks/ERROR_codes.txt` — 5113 documented,
  "Next DBLQH" bumped to 5114.

Why not the pre-existing levers: EI 5106 delays every 20th copy LQHKEYCONF by
500 ms — usable for widening, but it throttles ALL fragments (system tables
included), gives no per-fragment start/complete markers (cannot prove
in-window commits), and its 25 ms/row average is not tunable per-table. The
HALT_COPY_FRAG machinery is reachable only from LGMAN UNDO-overload
reporting, not commandable from tests.

## 6. How to run

```
cd debug_build && make -j10 ndbmtd          # once, after the kernel edit
cd mysql-test
./mysql-test-run.pl --suite=ndb_ttl ttl_nr_copy_window \
    --build-thread=305 --vardir=var-ttlwin
```

(Recorded with `--record` on the first pass; build-thread 305 chosen to
coexist with the probe sibling on 320 and other agents on 300.)

## 7. Observed results (recorded run, 2026-08-01)

Everything below is captured deterministically in
`mysql-test/suite/ndb_ttl/r/ttl_nr_copy_window.result`.

1. **Copy window (EI 5113):** "Starting copy of tab(14,0)" →
   "Copy of tab(14,0) complete" spanned **20.7 s** for 1600 rows
   (~13 ms/row) on the live node — vs **66 ms** un-throttled (run 1) —
   giving a wide, provable window. The end-of-race assertion (no
   completion marker yet) held in both phases.
2. **The natural race committed mid-copy without forming a fork:**
   - 5× D1 (INSERT over expired not-yet-copied ids 1496-1500) — success;
   - 5× D2a (REPLACE over expired not-yet-copied ids 1481-1485) — success;
   - purge-shaped ttl-ignored `DELETE ... IN (3, 1490)` (one copied, one
     not-yet-copied expired row) — `affected rows: 2`;
   - 5 fresh keys inserted, 5 live rows deleted — success;
   - phase 2 (initial NR): 2× D1, 2× D2a, 2 fresh, 2 deletes — success.
3. **Detection all green:** `ndb_assert_frag_replica_consistency` PASS at
   4 checkpoints; physical rows per replica exactly **1598** after each
   phase (both nodes identical); the rowid-recycling tripwire (delete all
   race rows, refill their slots with 19 fresh keys across 4 waves) hit
   **zero** 899 / lock-wait signatures; final visible-row count exactly
   115 with spot-checked contents.
4. **The known transient, demonstrated deterministically:**
   `INSERT INTO t1 VALUES (2, NOW(), ...)` over an expired ALREADY-copied
   row mid-copy failed with `ERROR HY000: Can't find record in 't1'`
   (1032 ← NDB 626 from the starting node's checkTTL gate on the
   ZWRITE-converted op with original_operation==ZINSERT), and the
   IDENTICAL insert succeeded after copy completion — matching the
   626-transient the impact-analysis sibling predicted statically and the
   probe sibling counted under churn (4 per graceful cycle there). It is
   an availability blip (symmetric abort), not a fork.
5. **Conclusion: no 899 was produced and none is producible through this
   window on the current tree** — the natural interleaving is closed by
   the D-A/D-B/D-C defenses; the test now pins them as a regression
   tripwire. (The 899 signature recipe itself is validated separately by
   `ttl_rowid_899_control.test`: statement fails ER_LOCK_WAIT_TIMEOUT
   1205, true cause only in Warning 1297 "Got temporary error 899 'Rowid
   already allocated' from NDB".)

### Empirical facts learned while stabilizing the timing lever

- Parking the copy continuation in `copyCompletedLab` throttles nothing:
  that path only runs when the outstanding-words window stalls; the hot
  path is the per-row `nextRecordCopy` call from `copyTupkeyConfLab`
  (1600 rows copied in 66 ms).
- A naive per-call park at the top of `nextRecordCopy` self-amplifies:
  every parked fetch spawns more parked fetches from the streaming
  LQHKEYCONFs (observed >1000 parked continuations per 150 ms — the delay
  collapses). Two mechanisms are required and now implemented: a
  single-slot pending park per copy process (`m_delay_copy_park_tc`) and
  a one-shot reentry grant (`m_delay_copy_reentry`) consumed at
  `nextRecordCopy` entry so the granted call's own downstream per-row
  fetch parks again.
- `ndbinfo.table_fragments.current_primary` + restarting only the BACKUP
  keeps the allocator role fixed without any result-file node-id
  dependence; `ndbinfo.memory_per_fragment.fixed_elem_count` equality is
  exact (expired rows are physical), 1598 on both replicas at every
  checkpoint.
- `SELECT COUNT(*) ... WHERE id > 0` on a TTL table correctly excludes
  expired rows (engine filters on read); the purge-shaped delete works
  from SQL with `SET SESSION ttl_expired_rows_visible_in_delete = 1` and
  reports exact affected-row counts.
- Marker synchronization via the live node's ndbd.log
  (`$MYSQLTEST_VARDIR/mysql_cluster.1/ndbd.<node>/ndbd.log`) with
  occurrence counting is reliable across both phases; only one EI value
  can be armed per block at a time, which is why 5113 carries its own
  markers (arming 5714 alongside would overwrite it).

## 8. Determinism runs

Recorded run PASSED (172 s wall, 120 s in the testcase), then three
consecutive verification runs of the recorded test back-to-back (same
binaries, build-thread 305, vardir var-ttlwin):

```
=== DETERMINISM RUN 1 ===  Completed: All 2 tests were successful.
=== DETERMINISM RUN 2 ===  Completed: All 2 tests were successful.
=== DETERMINISM RUN 3 ===  Completed: All 2 tests were successful.
```

Four consecutive passes total. Window stability across runs: phase-1 copy
20.7-20.8 s each time (park sampling: 1024 parks in 13.06 s = 12.8 ms/park),
phase-2 (initial NR) window equally wide; the in-window assertions never
tripped, i.e. the racing operations landed inside the copy window in every
run.

### Development iterations that produced the final EI shape

1. Park in `copyCompletedLab` → copy of 1600 rows still took 66 ms
   (that path only runs on outstanding-words stalls). Test failed at the
   transient step (insert ran after the copy had finished).
2. Park at top of `nextRecordCopy` with a scoped reentry flag around the
   handler's call → 654 ms (the granted call's whole EXECUTE_DIRECT chain
   ran with parking disabled: per-batch, not per-row throttle).
3. Consume the grant at `nextRecordCopy` entry → still ~372 ms and the
   diagnostic counter revealed a CONTINUEB storm (1024 parks per 150 ms:
   every parked fetch spawned more parked fetches from the streaming
   LQHKEYCONFs).
4. Single-slot pending park per copy process (`m_delay_copy_park_tc`) +
   consumed grant → stable 12.8 ms/row, 20.7 s window, all runs green.

### Open items / notes for the merge

- ERROR_INSERT code **5113** (this task) vs **5118** (fix sibling's
  replica-rowid-mismatch hard-stop): keep both `ERROR_codes.txt` entries,
  set "Next DBLQH" to at least 5119 on merge.
- The park-counter log line ("EI5113 parked copy fetch #N") samples
  `c_errorCounter`, which is shared with EI 5106 and not explicitly
  initialized (pre-existing); it only affects log-sampling alignment.
- `ttl_nr_copy_window` intentionally does NOT restart the fragment's
  PRIMARY: that would move the allocator role mid-test. The probe
  sibling's `ttl_nr_copy_churn` covers the primary-restart cycle
  probabilistically.
- If the test is ever seen failing at the in-window assertions
  ("TEST BROKEN: copy ... completed before the racing operations"), the
  EI pacing regressed (or the row volume shrank) — fix the window, do not
  widen the assertion.
