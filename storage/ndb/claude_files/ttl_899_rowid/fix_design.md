# Fix design: replica rowid forwarding + verification (TTL error 899 hardening)

Companions: `findings.md` (defect mechanics), `impact_analysis.md` (system-wide
impact audit; the review basis for this patch), `normal_insert_analysis.md`
(abort-ordering / dup-convert reachability proofs; source of the transient-899
constraints in §3 and §9). Line numbers below marked "pre-change" are as of
branch point 579ff6dd3c1 (25.10-main, 2026-07-31).

Delivered as two commits:

1. **Commit 1 — close the TTL amplifier (D1 + D2a)**: primary attaches the
   affected row's rowid to forwarded ZINSERT_TTL (wire op ZINSERT) and TTL
   ZWRITE-resolved-to-ZUPDATE (wire op stays ZWRITE); backup verifies the wire
   rowid against its own ACC resolution (found case) or heals/fails loudly via
   the pre-existing required-rowid insert path (not-found case). Includes the
   full receiver-side verification (also covering ZUPDATE/ZDELETE shapes), the
   new error code 1245, the version gate, and the defensive NoTTLDupConvert for
   NR-copy inserts.
2. **Commit 2 — full principle**: primary also attaches verification rowids to
   plain forwarded ZUPDATE and ZDELETE (any table). Sender-side condition
   change only; the commit-1 receiver already verifies these shapes.

## UNTESTED ASSUMPTIONS (no build environment in the worktree)

All edits are cross-referenced against the actual source, but **nothing was
compiled or executed**. Assumptions a reviewer/builder must discharge:

1. **Compiles**: `ndbd_replica_rowid_forwarding` is visible in `DblqhMain.cpp`
   (`ndb_version.h` is already used there: `ndbd_frag_lqhkeyreq` at :11934,
   `NDBD_RATE_LIMIT_VERSION` at :9126); `TupKeyRef` field writes from Dblqh are
   legal (friend class, precedent :11786-11789); `AccKeyReq::setNoTTLDupConvert`
   takes `(Uint32, bool)` and returns Uint32 (AccKeyReq.hpp:67); format-string
   argument promotion of `Uint8 seqNoReplica` to `%u` follows existing kernel
   logging practice.
2. **Synthesized TUPKEYREF unwind**: rejecting in `continueACCKEYCONF` (state
   WAIT_ACC → set WAIT_TUP → build TupKeyRef → `execTUPKEYREF`) is assumed
   exactly equivalent to the shipped disk-page failure path
   (`acckeyconf_load_diskpage` pre-change :11783-11792: same held ACC lock, same
   prepared-but-unexecuted TUP op, one call frame shallower). `execTUPKEYREF`
   WAIT_TUP → `abortErrorLab` (:6174-6197) skips its AC_NR_COPY bookkeeping
   because verification is gated to AC_NORMAL. Not exercised on a live cluster.
3. **ACCKEYCONF local key**: `signal->theData[3]/[4]` (and the
   `execACCKEYCONF` parameters derived from them) are assumed to be the found
   row's `(page_no, page_idx)` local key for every FOUND op, including after a
   lock-wait grant — consistent with their direct use in
   `prepareTUPKEYREQ(localKey1, localKey2, ...)` (:11473) and the
   `m_row_id = (localKey1, localKey2)` assignment (:11699-11700), but not
   independently traced through every DbaccMain path.
4. **Version cutoff**: `MYSQL_VERSION_PATCH=15` on this branch ⇒ the fix is
   gated at 25.10.16. If it misses that release train, bump
   `NDBD_REPLICA_ROWID_FORWARDING_2510` before merging.
5. **ERROR_INSERT 5118 is free** (grep: only 5112 used in the 5112-5119 range
   in dblqh). A sibling agent is concurrently adding a test-only error insert
   for the seeded repro — check for collision at merge time.
6. **Error code 1245 is free** (grep of `ndberror.cpp` and block headers;
   note: `impact_analysis.md` suggested 1244, which is in fact already taken by
   "Invalid fanout in partition hash definition" — stale suggestion). No mtr
   baseline is known to enumerate the full error table (unverified).
7. **m_use_rowid reader audit is complete**: senders/readers of
   `TcConnectionrec::m_use_rowid` are exactly :9099 (receiver set from wire),
   :11681 (local TupKeyReq capture — BEFORE the new attach point), :11711
   (INSERT/REFRESH attach), :12448-12450 (pack), :21636 (copy scan), :34452
   (replay derivation — replay-local), :38034 (TRACE print). TUP reads only its
   own `KeyReqStruct::m_use_rowid` copied from the TupKeyReq flag
   (`DbtupExecQuery.cpp:2140-2148`), and the `TupKeyConf::rowid` echo (:2872,
   :2885) is read by no LQH consumer (checked :6003-6082, :21830-21880).
   Setting `m_use_rowid` at pack time therefore has no effect other than the
   wire flag (+ trace output).
8. **Sender-set completeness**: ops reaching the packLqhkeyreqLab forwarding
   path with `getRowidFlag && seqNoReplica != 0 && AC_NORMAL && !NrCopyFlag &&
   !dirtyOp && op ∈ {ZUPDATE, ZDELETE, ZINSERT_TTL}` on the receiver can only
   be replica-chain steady-state ops whose wire rowid is authoritative.
   Enumerated senders (impact_analysis §2): DBTC (never sets rowid, assert
   :9310-9311), DBLQH forwarding (authoritative by construction), RESTORE
   (seqNoReplica 0), DBSPJ (reads only), REDO replay (no signal; reqinfo built
   by `initReqinfoExecSr` :33793-33864 carries neither RowidFlag nor
   NrCopyFlag).

## 1. Principle and mechanism

**Principle** (stated in the code comments at both new sites): backup replicas
must never receive a rowid-less operation for any INSERT, WRITE, UPDATE or
DELETE. The primary's rowid allocator is only safe if the backup never
allocates rowids on its own — any backup-side self-allocation means a slot can
be occupied on the backup that the primary believes free, so the primary can no
longer safely select rowids (invariant: `free(primary) ⊆ free(backup)` at
every point of the per-fragment operation stream).

Mechanically the fix is **send-side flag logic plus one backup-side check**:

- The wire already has the carrier (RowidFlag bit 31 + two `variableData`
  words, since NDBD_ROWID_VERSION), the receiver already turns the flag into
  `m_use_rowid` (:9099) and required-rowid TUP execution for inserts
  (:11681/:11689 → `DbtupExecQuery.cpp:2140-2148` → :4387-4421
  `alloc_fix_rowid` / `alloc_var_row(true)`), and the NR-copy mode already
  attaches rowids to ALL forwarded ops (:21548-21551 + :12448-12449). The fix
  extends the attach to steady state for the previously rowid-less shapes and
  adds the found-case comparison. **Zero DBTUP changes, zero signal-format
  changes, zero REDO-format changes.**

### Sender (packLqhkeyreqLab, pre-change :12448-12450)

New attach block between the NR-copy OR-in and `setRowidFlag`:

- Commit 1 condition: `!m_use_rowid && seqNoReplica == 0 &&
  (operation == ZINSERT_TTL || (operation == ZUPDATE &&
  original_operation == ZWRITE && is_ttl_table(tabRef))) &&
  ndbd_replica_rowid_forwarding(getNodeInfo(nextReplica).m_version)`
  → `m_use_rowid = 1`.
- Commit 2 widens the op condition to
  `operation ∈ {ZINSERT_TTL, ZUPDATE, ZDELETE}` (any table; subsumes the
  TTL-ZWRITE clause since that shape has `operation == ZUPDATE`).
- `m_row_id` is correct at this point for every covered shape: ACC-found local
  key for ZINSERT_TTL/ZUPDATE/ZDELETE (assigned in `acckeyconf_tupkeyreq`
  pre-change :11699-11700, or :11781-11782 / the load-diskpage callback for
  disk tables), TUP-chosen key for inserts (`accminupdate`,
  `Dblqh.hpp:5531-5557`).
- The wire OPERATION is untouched: D1 still travels as ZINSERT (backup
  re-derives ZINSERT_TTL via dup-convert), D2a still as ZWRITE (re-mask
  :12510-12519 unchanged — replicas must keep skipping checkTTL, see the
  [TTL Replication ZWRITE to replicas] design comment :11578-11596).
- Only the PRIMARY attaches. Middle replicas (NoOfReplicas ≥ 3) keep their
  `m_use_rowid` from the wire and forward `m_row_id`, which verification has
  pinned equal to the wire value (or, for the heal case, `accminupdate` set to
  the wire value) — so the SAME rowid propagates down the chain, never a
  locally-derived one. With an old-version upstream (no wire rowid) the middle
  forwards rowid-less, exactly as today.

### Receiver (continueACCKEYCONF, verification inserted between pre-change
:11619 and :11629)

Predicate: `getRowidFlag(reqinfo) && seqNoReplica != 0 &&
activeCreat == AC_NORMAL && !getNrCopyFlag(reqinfo) && dirtyOp == ZFALSE &&
operation ∈ {ZUPDATE, ZDELETE, ZINSERT_TTL}`.

- These three ops imply ACC FOUND the key (update/delete not-found never reach
  ACCKEYCONF — ACC REFs 626; ZINSERT_TTL exists only via dup-convert of a
  found key). `localKey1/localKey2` is the row's rowid on this replica;
  `m_row_id` still holds the wire rowid (the overwrite happens later, in
  `acckeyconf_tupkeyreq` — for disk tables the :11781 overwrite also happens
  later, which is why the check must sit here and not inside
  `acckeyconf_tupkeyreq`).
- Mismatch ⇒ `g_eventLogger->error` (instance, table, fragment, op, wire
  rowid, local rowid, transid, seqNoReplica), optional `ndbabort` under
  ERROR_INSERT 5118, then REF with error 1245 via the synthesized-TUPKEYREF
  unwind (assumption 2).
- Not-found insert-family ops need no code: `m_use_rowid` came in as 1 and the
  pre-existing backup insert path executes `alloc_fix_rowid` at the wire rowid
  — healing the missing row at the primary's exact slot, or failing with 899
  if the slot is occupied. This is the tripwire restored.
- The check runs on EVERY non-primary hop (middle + last), per
  impact_analysis R6. This is what kills the amplifier
  `normal_insert_analysis.md` §2.1 proved: under a pre-existing fork a middle
  replica absorbing a rowid-carrying insert forwards the flag still set but
  the rowid REWRITTEN to its own element's rowid (the :11699-11700 overwrite
  with the :11711 `|=` never clearing the wire flag), silently re-basing the
  third replica onto the MIDDLE replica's layout. With per-hop verification
  the middle either proves its rowid equals the primary's (forwarding is then
  faithful) or REFs before forwarding anything.
- The verification also bites for shapes that ALWAYS carried rowids: a plain
  ZINSERT that a TTL backup dup-converts (C2-found masking direction) is now
  rowid-checked, including when the sender is an OLD version (its insert rowid
  was always authoritative).

## 2. Touched decision points (pre-change file:line)

| Site | Pre-change | Change |
|---|---|---|
| D1 rowid-less skip | `DblqhMain.cpp:11702-11711` (comment + `m_use_rowid \|= (op==ZINSERT\|\|op==ZREFRESH)`) | code identical; comment rewritten to state why ZINSERT_TTL must NOT get local required-rowid semantics and where its wire rowid is attached instead |
| Forward attach | `:12448-12450` | new version-gated attach block + principle comment (commit 1 TTL shapes, commit 2 all U/D) |
| Send VM_TRACE whitelist | `:12477-12488` | asserts unchanged; comments explain rowid-carrying shapes and that rowid-less ZINSERT_TTL is now the LEGACY (old-receiver) shape, tightenable once the version floor guarantees new receivers |
| ZWRITE re-mask | `:12510-12519` | untouched (wire op must stay ZWRITE) |
| Receive whitelist | `:9310-9323` | asserts unchanged; comment documents legacy vs new shape and unconditional acceptance of both |
| Receiver flag/words intake | `:9099`, `:9293-9297` | untouched (already correct) |
| Verification | `continueACCKEYCONF`, between `:11619` and `:11629` | new block (see §1) |
| ZINSERT→ZINSERT_TTL dup-convert derivations | `:10230-10254`, `:11483-11507`; ACC `DbaccMain.cpp:1472-1515` | untouched — the dup-convert is the legitimate apply mechanism for backups, REDO replay and restore-adjacent paths (impact R3); NO is-backup guard added |
| checkTTL / ttl_ignore semantics | `DbtupExecQuery.cpp:3498-3547`, `:4709-4756` etc. | untouched — expiry handling is completely orthogonal to the rowid |
| NoTTLDupConvert | `:10197` (`exec_acckeyreq`) | extended to `m_restore_op \|\| getNrCopyFlag(reqinfo)` — defensive only (see §5) |
| Error code | `Dblqh.hpp:404-418`; `ndberror.cpp` IE section (1237 row at :451) | new `ZREPLICA_ROWID_MISMATCH 1245`; new `{1245, DMEC, IE, "Replica rowid mismatch detected"}` |
| Version predicate | `ndb_version.h.in` (pattern :1236-1270) | new `NDBD_REPLICA_ROWID_FORWARDING_2510` = 25.10.16 + `ndbd_replica_rowid_forwarding()` |
| EI catalogue | `ERROR_codes.txt` (LQH section) | 5118 documented |

## 3. Failure policy for a verification mismatch

**Chosen: REF with new permanent error 1245 (DMEC/IE, "Replica rowid mismatch
detected") + event-log error line; node-kill only under armed ERROR_INSERT
5118.** Rationale (follows impact_analysis §4.8):

- **Not 899**: 899 is classified TR and API-mapped to lock-wait-timeout
  (`ndberror.cpp:297`) — NdbAPI retry loops and mysqld absorb it, which is
  precisely the masking this fix exists to remove. A permanent IE code is
  non-maskable, self-describing, and distinguishable from replay-era 899s.
- **Not crash-the-backup by default**: the detected-inconsistency-kills-node
  precedents (REDO `progError`, `alloc_fix_rowid` default-arm `ndbabort`,
  COPY_FRAG 626→2303) are all RECOVERY contexts. In normal traffic a node kill
  converts a consistency bug into an availability incident, and worse: a
  REDO-based node restart can REPLAY a divergent history (GCI-skip copy,
  `PREPARE_COPY_FRAG_CONF` :21076-21140), producing a crash loop until an
  INITIAL restart. The transaction-abort keeps the cluster up while the
  event-log line names the exact fragment to repair (initial NR of the
  diverged backup).
- The REF travels the existing backup-error machinery unchanged:
  `execTUPKEYREF` → `abortErrorLab` → LQHKEYREF with ReplicaErrorFlag
  (:15497-15499) → TC aborts unconditionally (`DbtcMain.cpp:9202-9218`).
- The not-found arm intentionally KEEPS 899 from `alloc_fix_rowid`: that is
  pre-existing backup-insert semantics shared with NR copy and replay (whose
  tolerances key on 899, :31629-31637); converting it would require TUP
  changes and a re-audit, for little gain (deliberate divergence from the
  optional extra in impact_analysis §4.8-3). It is also REQUIRED to stay
  temporary: `normal_insert_analysis.md` §1.4 proved two benign transient-899
  windows on perfectly healthy clusters (W1 takeover-abort fan-out with
  cross-PK rowid reuse; W2 LOG_ABORT_QUEUED deferring an aborted op's rowid
  free past newly arriving inserts) — those legitimately hit exactly this arm
  and MUST remain retriable, which is likely why 899 was classified TR in the
  first place. See §9 for why the new permanent 1245 cannot fire from those
  windows.
- Debug/autotest: ERROR_INSERT 5118 escalates the mismatch to `ndbabort` at
  the detection point. Default debug builds still take the clean REF so mtr
  tests can assert on error 1245.

## 4. REDO safety proof

The change cannot alter REDO content, format, or replay semantics:

1. **What is logged**: `writeLogHeader` (pre-change :12753-12787) writes a
   fixed 8-word header — `[type, len, hash, operation, aiLen, keyLen,
   rowid.page_no, rowid.page_idx]` — for EVERY operation, unconditionally. It
   reads `m_row_id` and `operation`; it does **not** read `m_use_rowid`. The
   fix changes neither `m_row_id` assignments (untouched: :11699-11700,
   :11781-11782, `accminupdate`) nor `operation`; it only sets `m_use_rowid`
   at pack time, after local execution. Hence byte-identical REDO records.
   (On a backup, the heal case logs the wire rowid because `accminupdate`
   stored it — i.e. still "the rowid the op actually used on this node",
   which is the invariant REDO relies on.)
2. **How the rowid reaches the log without m_use_rowid**: `m_row_id` is
   populated for every op by ACC/TUP resolution (found key at
   `acckeyconf_tupkeyreq`, chosen key at `accminupdate`) before the log write
   — this is the pre-existing mechanism, now also reused for the wire.
3. **Replay**: replay ops are built from the log (`readLogHeader`
   :34407-34453; ZINSERT_TTL folds to ZINSERT with
   `m_use_rowid = (op == ZINSERT)` at :34452 — replay-local, untouched), get
   reqinfo from `initReqinfoExecSr` (:33793-33864: Dirty+Simple+GCI+operation
   only — **no RowidFlag, no NrCopyFlag**), run with `lastReplicaNo = 0`
   (never forwarded — `packLqhkeyreqLab` early-returns at :12367) and
   `seqNoReplica = 0`. The verification predicate is therefore doubly false
   for replay (no flag, seqNo 0), and the NoTTLDupConvert extension is inert
   for replay (no NrCopyFlag) — replay's REQUIRED dup-convert (:10184-10196
   design comment) is preserved. 899/630/626 replay tolerances
   (`logLqhkeyrefLab` :31616-31637) are untouched.

## 5. NR-copy interplay decisions

Verified first (with impact_analysis §4.1): the copy phase is NOT a live
instance of the defect today — from copy-scan start the live node attaches
rowids to ALL forwarded ops (:21548-21551 + :12448-12449), the starting node
ignores everything before the first copy row (AC_IGNORED, :9485-9508 /
:10140-10161), and the shared FIFO LDM path means a rowid-less op cannot reach
`handle_nr_copy` while the copy is active. Decisions per special case:

- **End-of-copy sentinel** (:10328-10343, rowid-less arrival ⇒ flip to
  AC_NORMAL): **KEPT unchanged**. Old primaries still stop attaching rowids at
  :22107, so the sentinel stays load-bearing for rolling upgrades. New
  primaries under commit 2 never send rowid-less ops, so the flip happens via
  `COPY_FRAG_DONE_REP` (:22115-22153 → :28691-28708) or COPY_ACTIVEREQ
  (:22422); until then ops keep taking the rowid-keyed `handle_nr_copy` path,
  which is correct for a fully-copied fragment (match ⇒ in-place, new key ⇒
  insert-at-rowid). Not tightened into an error.
- **TTL-ZWRITE-as-new-key-insert exception** (:10501-10502, :10550-10551):
  **KEPT** — the wire op remains ZWRITE (R2), so the exception is still
  required; its `nr_read_pk` matching consumes exactly the wire rowid the ops
  already carry during copy. No change in either commit.
- **Copy-row branch** (:10378-10494): untouched.
- **Verification gating**: OFF for `activeCreat != AC_NORMAL` (AC_NR_COPY
  reconciliation and AC_IGNORED pass-through must not be second-guessed) and
  OFF for NrCopyFlag ops. Copy-phase behavior is byte-identical; only the
  steady state is tightened. Ops on a THIRD replica (AC_NORMAL) behind a
  starting middle node during a copy are verified — safe, because the middle
  forwards the primary-attached wire rowid in every AC_NR_COPY shape
  (required-rowid execution or match-checked, or IGNORED-verbatim forwarding).
- **NoTTLDupConvert for NrCopyFlag inserts** (:10197): added as defensive
  hardening. Unreachable today — `handle_nr_copy` deletes at the target rowid
  AND by PK (`nr_copy_delete_row`, :10468-10494) before running a copy insert,
  so ACC cannot find the key — but the explicit term protects the copy
  protocol against future refactoring of the delete-first logic. Replay-safe
  per §4.3.

## 6. Version gating (mixed-version behavior)

Gate: `ndbd_replica_rowid_forwarding(getNodeInfo(nextReplica).m_version)`,
evaluated per hop at the attach site (precedent:
`ndbd_support_copy_frag_done` per-hop check at :22115-22116). Per-series
floors: 25.10.16 (`NDBD_REPLICA_ROWID_FORWARDING_2510`) and 26.02.9
(`NDBD_REPLICA_ROWID_FORWARDING_2602`, the forward-port target); every
series after 26.02 (26.04, 26.05, ...) returns true from its first release.
A 25.10.16 sender therefore keeps legacy rowid-less shapes toward 26.02.x
peers below 26.02.9 during a cross-series upgrade.

- **New primary → old backup**: predicate false ⇒ exactly today's shapes
  (rowid-less D1/D2a/U/D). The old backup's behavior is bit-identical to
  today. No protection on that hop until it upgrades. Shapes that always
  carried rowids (plain insert, refresh, D2b, NR copy) are unchanged in both
  directions.
- **Old primary → new backup**: rowid-less D1/D2a/U/D arrive; the receive
  whitelists (:9310-9323) still accept them; verification stays idle (no
  flag). The new backup DOES already verify the rowid-carrying shapes old
  primaries send (e.g. a plain ZINSERT dup-converted on a TTL backup), since
  those rowids were always authoritative. The full divergence-detection
  guarantee starts when ALL data nodes run ≥ 25.10.16.
- **Mixed 3-replica chains**: gating is per hop; each upgraded hop is
  protected independently. A middle old node forwards rowid-less onward
  (legacy behavior) — the downstream new node simply sees the legacy shape.
- Downgrade: new shapes stop being sent as soon as the sender is downgraded;
  receivers of any version always accepted both shapes, so no cleanup is
  needed.

## 7. Channel coverage

Per impact_analysis §2 channel table:

- **Covered by commit 1**: C3 (D1 ZINSERT_TTL), C4 (D2a TTL ZWRITE→update) —
  the entire backup self-allocation surface; found-case verification also
  covers C2-found (TTL dup-convert of a rowid-carrying insert — the reverse
  masking direction) and, opportunistically, any rowid-carrying U/D that
  exists pre-commit-2 (NR-copy-chain third replicas).
- **Covered by commit 2**: C7 (plain ZUPDATE/ZDELETE — placement-divergence
  detection between two present rows; not-found keeps today's loud 626), and
  the carry (not the verify) for C6 dirty writes whose resolution is
  UPDATE/DELETE (dirty ops are excluded from the found-case verification —
  documented non-consistent — but their not-found case now heals/trips
  through the required-rowid insert instead of self-allocating).
- **Already disciplined, untouched**: C2/C5 (insert shapes), C8 (ZREFRESH —
  carries a rowid; no found-case verification because found-ness is not
  classifiable from the op code; population is conflict-resolution refreshes),
  C9/C10 (NR copy), C11 (REDO replay), C12 (restore).
- **By design rowid-less**: C1 (TC→primary — the primary IS the allocator).
- **Unique-index fragments of TTL tables** (C13): covered automatically —
  `is_ttl_table` follows `primaryTableId` (:4590-4604), so their D1 shape gets
  commit-1 treatment; they have no ZWRITE channel.
- **Blob part tables** (C14): NOT TTL tables (DBDICT clears `primaryTableId`,
  `Dbdict.cpp:6899-6919`) — full insert discipline already applies; commit 2
  covers their U/D like any table.
- **Fully-replicated tables: out of scope by explicit user decision.** The
  impact analysis established the fix needs no FR-specific logic (rowid
  agreement is a within-fragment property; each copy-fragment replica set is
  covered independently); the adjacent FR TTL findings are documented as
  deferred items in `impact_analysis.md` (§4.4, §5.4).
- **Not covered**: pre-existing latent forks are detected on next touch, not
  repaired (repair = INITIAL node restart of the diverged backup — a
  REDO-based NR can preserve the fork); dirty-op found-case verification
  (waived); ZREFRESH found-case verification (see above); the within-node
  `is_ttl_table` disagreement hazard (findings §4 tail) is independent of
  rowids and remains open.

## 8. Validation plan

Build: full debug (VM_TRACE + ERROR_INSERT) and release builds — nothing here
was compiled (see assumptions).

Existing suites that must stay green (they exercise every touched path):

- `ndb_ttl` (esp. `ttl_replica3_write` — middle-hop forwarding;
  `ttl_disk_expired_write` — disk-table path through
  `acckeyconf_load_diskpage`; the blob tests `ttl_blob_*`), `ndb_ttl_purge`
  (purge deletes now carry verification rowids under commit 2),
  `ndb_ttl_rpl` (applier provenance / OO_TTL_IGNORE — affects checkTTL
  bypass only, not rowids).
- Node-restart / system-restart tests (NR copy interplay, REDO replay):
  the ndb restart mtr tests plus autotest NR suites; NoOfReplicas=3 runs.
- A mixed-version upgrade run exercising both gate directions (old primary /
  new backup and vice versa).

Sibling repro (seeded single-row drop on backup B of a TTL fragment; in
development concurrently). Expected flip with this fix, stated precisely:

- Step 4 (post-seed assert): `ndbinfo.memory_per_fragment.fixed_elem_count`
  still DIVERGES by 1 — the seed itself is unaffected by this fix.
- Step 5/6 (INSERT of the dropped PK k, which dup-converts on the primary):
  the forwarded ZINSERT now carries the expired row's rowid R.
  - Normal seed shape (R free on B): B executes a required-rowid insert at R
    — the transaction SUCCEEDS and **heals the layout at the correct slot**;
    `fixed_elem_count` re-equalizes; the subsequent churn loop stays clean
    (no 899, no 1245). The test's divergence assert after this step must flip
    from FAIL to PASS.
  - R occupied on B by an unrelated row (deeper fork): the insert fails
    LOUDLY at this step with 899 from `alloc_fix_rowid` — no silent success,
    no delayed churn-899.
  - B unexpectedly HAS k at a different rowid (two-sided fork): the op is
    REJECTED with the new error 1245 plus the event-log line naming the
    fragment and both rowids.
- In all three outcomes the defining pre-fix symptom — silent success at the
  amplifying op followed by delayed 899s in unrelated churn — is gone.
- Controls: EI 4019 (`DbtupExecQuery.cpp:4389-4393`) remains the
  guaranteed-positive 899 control; EI 5118 armed on B turns a constructed
  found-mismatch into an ndbabort (autotest hard stop).

Observability guidance (must accompany any 899 detector/alerting): a SINGLE
899 — in particular one coincident with a TC failover or REDO-log congestion —
is NOT fork evidence; the benign windows of §9 produce exactly that shape and
self-heal on retry. Only a RECURRING 899 on the same slot in steady state (or
any occurrence of the new permanent 1245) is the fork signature.

## 9. No false positives from the benign transient-899 windows

`normal_insert_analysis.md` §1 proved two transient windows on healthy
clusters in which a rowid is freed on the primary before the backup
(takeover-abort fan-out W1; LOG_ABORT_QUEUED W2 — the :11052 backup-first
dealloc rule covers only the COMMIT path; abort-path frees are
per-replica-local). Audit that the new PERMANENT error cannot fire from them:

1. Both windows are CROSS-PK slot-reuse races: the affected arriving op is an
   INSERT of a different key finding the wire slot still occupied — that is
   the not-found/`alloc_fix_rowid` arm, which this fix deliberately leaves on
   temporary 899 (§3). The 1245 check is on the FOUND path only.
2. The found-path verification compares against DBACC state AT GRANT TIME, and
   grant-time state is re-derived from the live element, never stale: a queued
   op promoted after the blocking op's abort/commit-delete has its conversion
   recomputed and its localdata either inherited from the live element
   (`startNext`, DbaccMain.cpp:2024) or explicitly INVALIDATED when the
   element disappeared (`release_lockowner` :6363/:6410), in which case
   `startNew` (:6541-6552) reverts the op to a genuine ZINSERT — which is NOT
   in the verified-op set {ZUPDATE, ZDELETE, ZINSERT_TTL}, so an op granted
   over a vanished element can never reach the comparison. The verification
   point (continueACCKEYCONF) runs strictly AFTER this grant-time
   re-derivation and after LQH's own grant-time ZINSERT_TTL re-derivation
   (:11483-11507), on both the synchronous and the lock-wait ACCKEYCONF
   paths.
3. A forwarded found-shape cannot legitimately observe a different-but-healthy
   rowid: a row's rowid changes only via committed DELETE + new INSERT (an
   insert-after-delete within one transaction REUSES the tuple location on
   every replica — `DbtupExecQuery.cpp` non-first-op insert branch), both
   replicated in per-row order through the ACC lock queues; and the primary
   only forwards ZUPDATE/ZDELETE/ZINSERT_TTL after ITS OWN grant found the
   row alive, so the backup's serialized grant for the same op sees the same
   generation of the row. Hence on a healthy cluster wire rowid == found
   rowid always; inequality proves divergence.
