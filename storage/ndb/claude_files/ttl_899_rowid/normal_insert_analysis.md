# Can a NORMAL INSERT cause "backup uses a rowid the primary doesn't"? — P1-P6 analysis

Extends findings.md (same directory). Scope: plain application-level INSERT (kernel
ZINSERT, rowid-carrying when forwarded), including the case where the primary
dup-converts it to ZINSERT_TTL because the PK matches an expired-but-unpurged row.
Question: can such an insert occupy a rowid on a backup replica that is not
occupied on the primary (or produce any replica rowid-layout divergence)?

Date: 2026-08-01. Branch 25.10-main, all line numbers from `git show HEAD:<file>`
(HEAD = 59b3b3cbdf1 lineage; DblqhMain.cpp verified unmodified in the working tree
at analysis time). Fully-replicated tables are OUT OF SCOPE by user decision.

## 0. Executive answer

**NO — not in steady state.** Every candidate window (abort ordering, TC-takeover
fan-out, queued-op grant on deleted elements, NR-copy, unique-index fragments)
collapses into one of three outcomes, none of which is silent divergence:

1. **Lockstep preserved** — the backup ends up inserting at exactly the wire
   rowid, because the ACC lock queue forces any insert that races a
   presence-changing transaction to WAIT, and the deferred-grant machinery
   re-derives the op from the element's post-event state (converted ZWRITE →
   genuine ZINSERT with the retained wire rowid).
2. **Loud global abort** — 630/626/899 REF → LQHKEYREF → the whole transaction
   aborts on all replicas symmetrically.
3. **TRANSIENT error 899** — a new finding: two small windows in which a
   perfectly healthy cluster can raise 899 on a backup (TC-takeover fan-out
   inversion; abort REDO-log-write queuing). Both self-heal on retry and cannot
   diverge the layout. This is almost certainly *why* 899 is classified as a
   temporary error (`ndberror.cpp:297`).

A normal INSERT can only put a rowid on the backup that the primary lacks as the
**amplifier of a pre-existing presence divergence** (findings §6, defect D1) —
it cannot create the divergence. The reverse direction (backup absorbs a
rowid-carrying insert as an update, leaving the wire rowid unoccupied on the
backup) is mechanically present in the code but **unreachable in steady state**
(P2 matrix, §2).

---

## 1. P1 — Abort-ordering window. VERDICT: divergence IMPOSSIBLE; transient 899 POSSIBLE

### 1.1 Abort propagation topology (previously untraced)

There are two distinct abort transports with opposite ordering properties:

**(A) TC-driven GSN_ABORT — a chain starting at the PRIMARY, but forward-first.**
- `Dbtc::releaseAndAbort` (DbtcMain.cpp:10455-10546) sends GSN_ABORT **only to
  the first alive replica** in `tcNodedata[]` order (= primary). Comment at
  10466-10468: *"if previous is alive, its LQH forwards abort to this node"*.
- `Dblqh::execABORT` (DblqhMain.cpp:14565-14816) **forwards the ABORT to
  `nextReplica` FIRST** (14771-14800, JBB, to the DBLQH instance derived from the
  fragment's `lqhInstanceKey` — the same instance pair that carries LQHKEYREQ
  forwards), and only **then** starts its own local abort
  (`abortStateHandlerLab`, 14815).

Consequence: for chain aborts, the forwarded ABORT(T1) is placed on the P→B LDM
signal link **before** the primary's own abort processing releases T1's row
lock; any later transaction T2 granted at P can only have its LQHKEYREQ sent
after that release, i.e. after the ABORT was already sent on the same link.
Same-priority signals between the same block-instance pair are FIFO, so **B
always receives ABORT(T1) before LQHKEYREQ(T2)**. The naive window "T2's insert
reaches B while T1's element still exists there" is pipeline-protected for chain
aborts (though it can still open at B if B's *processing* of the ABORT is
parked — see 1.4-W2 and the 5015/5096 levers).

**(B) Takeover GSN_ABORTREQ — direct per-replica fan-out, no chain, no FIFO
protection.**
- `Dbtc::toAbortHandlingLab` (DbtcMain.cpp:13874-13978) loops `replicaNo` from
  `lastReplicaNo` down to 0 and sends GSN_ABORTREQ **directly to each replica**
  (13927). Send order is backup-first, but these are sends to *different nodes*
  from the takeover TC — there is **no cross-node execution-order guarantee**.
- `Dblqh::execABORTREQ` (14822-14891) does **not** forward to the next replica.

Consequence: during TC takeover, P may execute T1's abort arbitrarily earlier
than B, and a new T2 (from any live TC) forwarded on the P→B link can reach B
**before** B has processed its ABORTREQ(T1). **This is the real transport for
the P1 window.** (Takeover COMMITREQ/COMPLETEREQ have the identical structure:
`toCommitHandlingLab` DbtcMain.cpp:14267-14346 sends GSN_COMMITREQ per replica;
same for COMPLETEREQ — vs. the normal chains, which run strictly backup-first:
TC sends GSN_COMMIT to `lastLqhNodeId` (`sendCommitLqh` DbtcMain.cpp:7650-7693)
and each LQH forwards COMMIT/COMPLETE backwards to `clientBlockref`
(DblqhMain.cpp:14143, 14172).)

### 1.2 When an aborted INSERT's element disappears and its rowid is freed

Per replica, on processing its abort signal (chain or fan-out), synchronously
within one LQH job unless noted:

1. `abortStateHandlerLab` → `abortCommonLab` → `abortContinueAfterBlockedLab`
   (DblqhMain.cpp:15018-15376) calls `Dbacc::execACC_ABORTREQ` (direct call,
   15357), which runs `abortOperation` synchronously (DbaccMain.cpp:3225-3274)
   except when a query-thread ACCKEYCONF is in flight (blocked case 3275-3300,
   resumed via `handlePendingAbort`).
2. `Dbacc::abortOperation` (5901-5998): a lock-owner insert has
   `OP_INSERT_IS_DONE`, so `OP_ELEMENT_DISAPPEARED` is set (5922-5926). Then:
   - **no queue**: `commitdelete` removes the hash element immediately (5980)
     and `trigger_dealloc` is called (5983);
   - **queue present**: `mark_pending_abort` (parallel-queue members of the
     same transaction only; a waiting foreign-transaction op in the serial
     queue is NOT marked) + `release_lockowner` (5936), which promotes the next
     op and *keeps the element in the hash* (re-pointed at the new owner,
     6474-6479), then `trigger_dealloc` (5944).
3. `trigger_dealloc` → **direct** `Dblqh::execTUP_DEALLOCREQ`
   (DbaccMain.cpp:4874-4922). For the abort case this is the "Trigger" form
   (theData[5]==RNIL): LQH sets the dealloc refcount to 1 — *"quick dealloc on
   complete of this op"* (DblqhMain.cpp:11331-11340). The actual
   `Dbtup::execTUP_DEALLOCREQ` → `free_fix_rec` happens when T1's LQH op record
   is released at the end of its local abort (`handleDeallocOp`/
   `decrDeallocRefCount` → 11156-11177).
4. `do_tup_abortreq(signal, 0)` (called from `cont_tup_abort`, 15375-15384)
   marks the tuple header FREE / invalidates the checksum but does **not** free
   the slot — the free requires the `ZABORT_DEALLOC` flag, which this call site
   does not pass (DbtupAbort.cpp:246-405, free at 388-402).
5. The abort REDO-log record can QUEUE (`LOG_ABORT_QUEUED`,
   DblqhMain.cpp:15411-15425) when the log part is congested. In that case the
   op release — and therefore the rowid free — is deferred to a later job.

**Answer to "does the ~11052 backup-first dealloc rule cover ABORT?"** — No.
The 11052-11080 comment and the refcount design order rowid release
backup-before-primary for **multi-op COMMIT deallocation** (notifications at
commit + trigger + release at COMPLETE, which chains backup-first). The ABORT
path reuses the same machinery but with a single local trigger per replica and
**no cross-replica ordering at all** — ordering comes solely from the abort
transport (chain FIFO, or nothing under takeover). That is exactly why the two
transient-899 windows below exist, and why they are transient: an abort frees
the same rowid on every replica eventually, so no permanent free-set divergence
can result.

### 1.3 The candidate interleaving, traced to its end (TTL table, takeover abort)

T1 = INSERT PK k (genuinely new), prepared on P (element at rowid R, X-locked)
and on B (element at R via the wire rowid, X-locked). T1's TC dies; takeover TC
decides abort.

1. Takeover TC sends ABORTREQ to B and P (13927). P processes first.
2. P: abortOperation → element for k removed (`commitdelete`), rowid R freed at
   P when T1's op record releases.
3. T2 (live TC): INSERT PK k. At P: ACC finds nothing → genuine insert path
   (`insertelementLab`, DbaccMain.cpp:1620-1627), TUP `alloc_fix_rec` picks R2
   (likely == R, free-list head). P forwards LQHKEYREQ op=ZINSERT,
   **rowid flag=1, rowid=R2** (m_use_rowid set at 11711, packed at 12448-12450).
4. B (ABORTREQ(T1) still queued): T2's LQHKEYREQ arrives first. execLQHKEYREQ
   stores `m_use_rowid=1` (9099) and `m_row_id=R2` (9295-9297). ACC: element
   for k FOUND → TTL dup-convert ZINSERT→ZWRITE **before** the lock check
   (DbaccMain.cpp:1490-1499) → element is locked → `accIsLockedLab`
   (2110-2226) → `placeWriteInLockQueue` → **SERIAL queue** (owner is a foreign
   transaction holding X). ACC returns RNIL; T2 waits in WAIT_ACC at B.
   **No absorption happens — the locked element forces queueing.**
5. B processes ABORTREQ(T1): abortOperation → OP_ELEMENT_DISAPPEARED;
   `release_lockowner` (queue case, serial-successor branch 6399-6440):
   T2 becomes lock owner, `OP_ELEMENT_DISAPPEARED` propagated (6460),
   **`newOwner.localdata.setInvalid()`** (6410), `trigger_dealloc_op=true`.
6. `startNew` (6511-6597): `deleted==true`, op==ZWRITE → **reverted to genuine
   ZINSERT** + `OP_INSERT_IS_DONE`, `OP_ELEMENT_DISAPPEARED` cleared
   (6541-6552) → ACCKEYCONF(op=ZINSERT, localdata invalid, ignore_ttl=0)
   sent JBB (6569-6570). T1's rowid R is freed by `trigger_dealloc` +
   op-release **within the same abort job** — i.e. before the JBB ACCKEYCONF
   job runs (exception: W2 below).
7. LQH at B, `execACCKEYCONF` (11377-11548): the ZINSERT_TTL derivation
   (11483-11507) requires ACC's op record to read ZUPDATE — it reads ZINSERT,
   so **the op stays a plain insert**. `acckeyconf_tupkeyreq` (11663-11733)
   passes the RETAINED `m_row_id=R2` / `m_use_rowid=1` into TupKeyReq
   (captured at 11679-11689, before the 11699-11700 overwrite) → TUP
   `handleInsertReq` takes the required-rowid branch → `alloc_fix_rowid(R2)`
   (DbtupExecQuery.cpp:4386-4402).

**Outcome: element and row land at R2 on B — exactly the primary's layout.
Lockstep holds.** The same trace with a non-TTL table differs only in step 4
(queued as ZINSERT via `insertExistElemLab`, DbaccMain.cpp:2260-2273, instead
of being converted) and converges identically in step 6 (`startNew` deleted
branch handles ZINSERT and ZWRITE the same way). If T1 instead COMMITS, the
grant finds the element alive: TTL → ZWRITE→ZUPDATE grant → ZINSERT_TTL → TUP
expiry gate 630 (live row) on B, and P reaches the same 630/absorb decision at
its own grant because P's grant equally happens after T1's outcome; non-TTL →
`ZWRITE_ERROR` 630 at grant (startNext 2009-2012 / startNew 6553-6556). All
symmetric.

### 1.4 The two real (transient) windows

- **W1 — takeover fan-out, cross-PK.** P completes T1's abort (or, for a
  DELETE, its takeover COMMITREQ+COMPLETEREQ) and frees rowid R while B has not
  yet processed its own fan-out signal. T2 = INSERT of a *different* PK m at P
  reuses R and forwards ZINSERT+R. At B, no ACC conflict for m exists, so
  nothing queues: `alloc_fix_rowid(R)` finds the slot still occupied by k →
  **899** (DbtupFixAlloc.cpp:294) → LQHKEYREF → T2 aborts globally. Retry after
  B catches up succeeds. Layout never diverges (both replicas converge on the
  freed slot).
- **W2 — abort log-write queuing, chain aborts.** B receives the (correctly
  FIFO-ordered) ABORT(T1) before LQHKEYREQ(T2), but B's abort parks in
  `LOG_ABORT_QUEUED` (15411-15425) before releasing the op / freeing the rowid
  — while ACC's part of the abort (element removal / regrant) has already run.
  A cross-PK T2 needing the same slot hits 899 exactly as in W1. For same-PK
  T2 the ACC element handling already completed, so same-PK traffic is
  unaffected. Self-heals when the log write drains.

Both windows exist for TTL and non-TTL tables alike and produce only the
retriable-899 outcome. They matter operationally: **a 899-trapping detector
(findings §9) must not treat a single 899 coincident with TC failover or redo
congestion as proof of divergence — only a *recurring* 899 on the same slot in
steady state is the fork signature.**

### 1.5 fb67986a382-area completeness verdict

`git show fb67986a382` adds recomputation of the same-transaction `ignore_ttl`
at the **startNext** deferred-grant point (now DbaccMain.cpp:2027-2080) and
deliberately not in **startNew**. Assessment of ALL op state across deferred
grants:

| State | Mechanism across deferred grant | Verdict |
|---|---|---|
| Op conversion (ZINSERT→ZWRITE→ZUPDATE/ZINSERT) | Not "preserved" but **re-derived from the element's state at grant time**: startNext 1998-2017 (post-ZDELETE → ZINSERT + disappeared cleared; ZWRITE → ZUPDATE; raw ZINSERT → 630), startNew 6541-6562 (deleted → ZINSERT + OP_INSERT_IS_DONE; ZWRITE → ZUPDATE; raw ZINSERT → 630). Correct-by-construction; a stale conversion can never execute against a vanished element. | SOUND |
| localdata (rowid ACC reports to LQH) | Inherited from predecessor when the element lives (startNext 2024), **explicitly invalidated** when it disappeared (release_lockowner 6363/6410). LQH's insert path ignores it; update paths get the live rowid. | SOUND |
| Wire rowid at LQH (`m_use_rowid`/`m_row_id`) | Set once at receive (9099, 9295-9297), never touched while queued, consumed at grant (11679-11689). | SOUND |
| ttl_ignore | Recomputed in startNext (fb67986a382); NOT in startNew. The commit's justification holds: startNew promotes only after the owner *and its entire parallel queue* have released, so the promoted op's transaction cannot still hold a lock on the row (its own earlier op would still be sitting in the parallel queue otherwise, forcing the startNext path instead). Verified against release_lockowner structure (6277-6508). LQH additionally only ever ORs ttl_ignore up (10258-10287, 11511-11540). | COMPLETE for this purpose |

No dropped-state hole affecting rowids was found in the deferred-grant paths.

---

## 2. P2 — Backup-side dup-convert of a rowid-CARRYING insert. Mechanism CONFIRMED; steady-state reachability: NONE

### 2.1 Mechanism (confirmed, plus one new detail)

If a plain ZINSERT+rowid arrives at a backup whose ACC finds an *unlocked*
element for the PK on a TTL table, the conversion at DbaccMain.cpp:1490-1499
(no is-backup guard; expiry plays no role) turns it into ZUPDATE at the FOUND
rowid (1509-1515); LQH derives ZINSERT_TTL (11483-11507 / 10230-10253); the
wire rowid is never looked at by TUP (update path); the wire slot stays free on
the backup while occupied on the primary. **New detail for NoOfReplicas>=3:**
because `m_use_rowid |=` never clears a wire-set flag (9099, 11711) and
`m_row_id` is overwritten with the ACC-found localkey at 11699-11700, a middle
replica that absorbs a rowid-carrying insert forwards ZINSERT with **rowid flag
still set but the rowid REWRITTEN to the middle replica's own element rowid**
(12448-12450). Under a pre-existing fork this silently re-bases the third
replica onto the *middle* replica's layout, not the primary's — a stronger
amplifier than findings D1 recorded. (The VM_TRACE whitelist at 12477-12488 has
its rowid-flag-set arm commented out, so even debug builds do not object.)

### 2.2 Reachability: element-lifecycle × signal-ordering matrix

Precondition for absorption: at T2's arrival, B holds an element for PK k that
P's ACC did **not** hold at T2's grant (else P would have dup-converted and the
forward would be rowid-less — a different, already-analyzed channel). Cells
below enumerate every way B-at-arrival can differ from P-at-grant in steady
state (no pre-existing divergence).

| # | Element event for k (transaction T1) | Ordering law for that event | Can B still hold the element after P stopped holding it? | State of B's element in that window | Outcome for rowid-carrying T2 at B |
|---|---|---|---|---|---|
| 1 | INSERT prepare (element created) | LQHKEYREQ chain P→B FIFO: created at P first, then B | No (B trails P in creation; the window has P-holding, B-not — the D1 direction, needs no rowid absorb) | — | not applicable (P had the element ⇒ P dup-converted ⇒ rowid-less forward) |
| 2 | INSERT abort (element removed) — TC chain GSN_ABORT | Forward-before-execute at P (14771-14815) + link FIFO ⇒ ABORT reaches B before any later T2 | Yes, but only until B processes the earlier-queued ABORT; element is **X-LOCKED** by T1 the whole time | LOCKED | T2 queues (1490-1499 → 2110-2226); grant after T1's abort → startNew deleted → genuine ZINSERT at wire rowid (§1.3). **No absorb** |
| 3 | INSERT abort — takeover ABORTREQ fan-out (13874-13927, no forward at 14822-14891) | None across nodes | Yes (arbitrarily long) | LOCKED | identical to #2 — the element cannot be absorbed while locked, and its removal is the very event that grants T2 |
| 4 | DELETE commit (element removed) — normal chain | GSN_COMMIT backup-first (TC→last replica 7650-7693; LQH backward chain 14143) ⇒ **B removes BEFORE P** | Never | — | impossible cell |
| 5 | DELETE commit — takeover COMMITREQ fan-out (14267-14346) | None across nodes; P can commit the delete first | Yes | LOCKED by T1's delete until B's COMMITREQ | T2 queues; grant at B's commit → commitDeleteCheck set OP_ELEMENT_DISAPPEARED (6019-6033) → promoted grant reverts T2 to ZINSERT at wire rowid. **No absorb** |
| 6 | DELETE abort (element kept) | any | Presence never differs (kept on both) | LOCKED until abort processed | P also has the element at T2's grant ⇒ P dup-converts ⇒ forward is rowid-less; consistent on both. Not this channel |
| 7 | COMPLETE phase | backward chain 14172 (normal) / COMPLETEREQ fan-out (takeover) | COMPLETE does not change element presence — only op release + deferred TUP slot free | — | only contributes to W1 transient 899 (slot-free timing), never to presence |
| 8 | GSN_LQH_TRANSREQ re-drive | produces only rows 3/5/7 signals (COMMITREQ/ABORTREQ/COMPLETEREQ); creates no ops | — | — | covered by rows 3/5/7 |

**Master invariant emerging from the matrix:** in steady state, *whenever B
holds an ACC element for k that P's grant-time state lacks, that element is
X-locked at B, and the lock-release event at B is precisely the event that
removed it at P.* An arriving rowid-carrying insert therefore always queues
(absorption requires an unlocked found element, execACCKEYREQ 1500-1607), and
its deferred grant re-derives insert semantics from the post-event state. The
absorb/masking path is reachable **only** from pre-existing divergence (a
committed, unlocked element on B for a PK the primary lacks) — i.e. it masks a
fork, it cannot create one. VERDICT: **IMPOSSIBLE in steady state.**

---

## 3. P3 — TC failover / node-failure re-drive. VERDICT: no divergence; fold is info-only; fan-out windows are P1-W1

- **The ZINSERT_TTL→ZINSERT fold is info-only, confirmed.**
  `LqhTransConf::setOperation` masks to the 3-bit field (LqhTransConf.hpp,
  setter around :203-216, with an explicit exemption comment). The takeover TC
  stores it (`initTcConnectFail`, DbtcMain.cpp:15020 `regTcPtr->operation =
  LqhTransConf::getOperation(reqinfo)`) but the decision inputs are
  `transStatus`/`failData` (Committed/Prepared/Aborted), and the re-drive
  signals — COMMITREQ (14328-14345), ABORTREQ (13921-13927), COMPLETEREQ —
  carry **no operation field at all**. Each replica commits/aborts the op form
  it locally prepared. The folded bits cannot change replica behavior.
- **Order degradation is the only takeover effect**: chained backup-first
  commit/complete and pipeline-protected abort become unordered per-replica
  fan-outs. All consequences were exhausted in §1/§2 (rows 3/5/7): same-PK
  races converge through the lock queue; cross-PK races can only hit transient
  899 (W1).
- **Mixed conversion states across replicas** (primary executed ZINSERT_TTL
  in-place update, backup executed a genuine insert after a P1-window regrant):
  by the §1.3 trace this happens exactly when the element was removed at both
  replicas before each op executed — the backup's insert lands at the wire
  rowid, and at takeover each replica commits its local TUP op (commit makes
  the already-executed change visible; no op is re-executed) → identical rows,
  identical rowids. No divergence lever found.

---

## 4. P4 — Normal INSERT during node-restart copy. VERDICT: IMPOSSIBLE — copy phase is STRONGER than steady state, and it heals PK-level forks

- `handle_nr_copy` (DblqhMain.cpp:10321-10590) runs **before ACC**
  (`exec_acckeyreq` is called only at its `run:` label, 10576-10578), so the
  rowid-matching logic pre-empts the TTL dup-convert entirely.
- During copy the primary forces the rowid onto **every** forwarded op —
  `m_use_rowid |= (m_copy_started_state == AC_NR_COPY)` at 12448-12449 — so
  even ZINSERT_TTL and TTL-ZWRITE forwards, rowid-less in steady state, carry
  the primary's row rowid to the starting node (rowid-less arrival instead
  means "copy finished", 10328-10343).
- **(a) not-yet-copied region:** `!match` + op==ZINSERT (or TTL new-key ZWRITE,
  10501-10521 exception + 10550-10551) → delete whatever occupies the wire
  rowid (10559-10565), delete any row holding the same PK at a different rowid
  (10571), then insert at the wire rowid. Applied-at-wire-rowid confirmed safe;
  the copy scan may redeliver it later (idempotent by design, comment
  10539-10548).
- **(b) starting node holds PK k at a DIFFERENT rowid than the wire rowid**
  (e.g. a rowid self-allocated earlier through the rowid-less channels): the
  same 10559-10573 sequence — `nr_copy_delete_row(...,0,0)` deletes the
  divergent-rowid copy of k, and the insert lands at the wire rowid. **The
  normal insert HEALS the fork for that PK instead of compounding it**, and the
  TTL dup-convert cannot absorb the mismatch because the element is deleted
  before ACC ever sees the op. (Contrast with steady state, where the same
  arrival would be silently absorbed at the wrong rowid — NR copy is the
  stricter regime.)
- Residual caveat (out of this path's scope): a fork can still *survive*
  restarts that replay it from REDO or skip it via the GCI-based copy
  optimization (see impact_analysis.md re :21076-21140 / DbtupScan.cpp
  :1261-1330); only rows actually re-touched or re-copied get healed.

---

## 5. P5 — Blob TTL tables. VERDICT: NOT EXPOSED — part tables are not TTL

- DBDICT **clears `primaryTableId` for blob part tables** once fragmentation is
  computed — Dbdict.cpp:6899-6919, comment: *"Blob tables come here with
  primaryTableId != RNIL but we only need it for creating the fragments so we
  set it to RNIL now ... to avoid other side effects"* (verified in-tree; the
  clearing is the `else` arm after ordered-index and hash-index handling, which
  both KEEP the linkage).
- `is_ttl_table` follows `primaryTableId` (Dblqh 4590-4604; Dbacc 10765-10768
  delegates to Dblqh; Dbtup analogous per findings §3.1) ⇒ part-table
  fragments answer **false** ⇒ no dup-convert, no ZINSERT_TTL, no rowid-less
  forwarding at part level. Part-row inserts keep the full required-rowid
  discipline of normal tables (any duplicate part row → loud 630, any occupied
  wire rowid → loud 899).
- A re-insert of an expired-unpurged main PK therefore exercises D1 only on the
  MAIN fragment; part-table traffic cannot widen the fork surface. Blob-part
  hygiene issues (e.g. the 00bb1386297 part-leak fix) are orthogonal to rowid
  discipline.

---

## 6. P6 — Unique-index fragments, ZREFRESH, upserts. VERDICT: index fragments share the FULL main-table exposure (nothing new mechanically)

- Unique-index (hash-index) tables **keep** `primaryTableId` (Dbdict.cpp hash
  branch clears only the fragmentation-request copy, not the table record), so
  `is_ttl_table` is TRUE for index fragments (Dblqh 4590-4604) — findings §3.1
  confirmed.
- Index maintenance ops are ordinary LQHKEYREQs on the index fragment's own
  replica chain. Hence, per this analysis: a plain index-entry insert carries a
  rowid to the index-fragment backup; an insert over an expired-unpurged index
  entry dup-converts on the index primary and travels **rowid-less** (D1 at
  index level — backup missing the entry self-allocates); every P1/P2/P3
  conclusion transfers verbatim because DBACC/DBLQH code is table-agnostic.
  The same-owner check (`ttlUniqueIndexSameOwnerCheck`,
  DbtupExecQuery.cpp:3278-3372) is primary-only enforcement (findings N4) and
  does not alter rowid flow. **Net: the index fragment's rowid layout enjoys
  exactly the main table's guarantees and exactly its D1 amplifier weakness —
  divergence still requires a pre-existing seed; a fork in an index fragment
  surfaces as 899 on index-maintenance inserts (seen by the application as
  temporary errors on arbitrary writes to indexed columns).**
- **ZREFRESH**: never dup-converted (the ACC conversion gate is `op == ZINSERT`
  only, 1490) and always rowid-carrying (11711) — discipline retained.
- **INSERT ... ON DUPLICATE KEY UPDATE / REPLACE** arrive as ZWRITE — that is
  defect D2 (findings M6), a rowid-less channel by construction, not a
  "normal insert" and not re-analyzed here. The P1/P2 queue-and-regrant
  guarantees apply to it identically (startNext/startNew treat ZWRITE as the
  convertible op).
- Fully-replicated-table trigger machinery: out of scope per user decision.

---

## 7. Closing — "Can a normal INSERT cause backup-uses-rowid-primary-doesn't?"

**Direct answer: No.** On the current tree, a plain INSERT — including one the
primary dup-converts over an expired-unpurged row — cannot make a backup occupy
a rowid the primary does not occupy (nor vice versa) from any steady-state
interleaving, including abort races, TC-takeover re-drives, and node-restart
copy. The protections, each verified at source level:

1. **Arrival ordering**: LQHKEYREQ FIFO P→B; chain aborts forwarded before
   local execution (14771-14815); commit/complete chains backup-first
   (7650-7693, 14143, 14172).
2. **Queueing, not absorption**: any element a backup holds "too long" is
   X-locked, and a rowid-carrying insert that meets it must wait
   (1490-1607, 2110-2226).
3. **Grant-time re-derivation**: startNext/startNew rebuild the op from the
   element's post-event state, invalidating stale rowids
   (1998-2024, 6363/6410, 6541-6552).
4. **Wire-rowid retention**: `m_use_rowid`/`m_row_id` survive queueing intact
   and reach `alloc_fix_rowid` (9099, 9295-9297, 11679-11689,
   DbtupExecQuery.cpp:4386-4402).

What a normal INSERT **can** do (all previously established or refined here):
amplify a pre-existing presence fork rowid-lessly (D1); mask a fork by
absorbing at the found rowid, and — new — re-base the third replica onto the
middle replica's rowid when doing so (§2.1); and raise **transient** 899s in
the two W1/W2 windows even on healthy clusters (also new).

### Findings that require test coverage

| Finding | Test shape |
|---|---|
| §1.3 convergence (takeover abort + racing same-PK insert, TTL and non-TTL) | 2-node, TC-node kill with prepared INSERT; EI 5016 on B; concurrent same-PK insert via second TC; assert success-or-clean-abort + `fixed_elem_count` equality + `ndb_select_all --rowid` equality |
| W1 transient 899 (takeover fan-out, cross-PK) | same harness, cross-PK insert storm; assert 899s occur, are retriable, and layouts equalize afterwards |
| W2 transient 899 (LOG_ABORT_QUEUED) | redo-log congestion (small redo + load, or EI 5083-family log-problem inserts) + abort storm + insert reuse; same assertions |
| §2.1 middle-replica rowid rewrite | NoOfReplicas=3 + a seeded fork (findings §9.1 error insert): verify third replica adopts middle's rowid (documents amplifier behavior; guards any future fix) |
| §1.5 startNew ignore_ttl non-recompute justification | lock-queue test: owner+full parallel queue release with waiting foreign-trans op on expired row → op must get 626/630, never a silent grant (regression fence around fb67986a382's deliberate omission) |
| P6 index-fragment fork symptom | seeded fork on unique-index fragment; assert 899 surfaces on index maintenance and `fixed_elem_count` divergence is visible per index fragment |

### Deterministic levers (all verified in-tree, DblqhMain.cpp / DbtcMain.cpp)

- **ABORT path**: EI **5015** (delay GSN_ABORT +2000 ms at receiving LQH,
  14614-14618 — set on B only: parks B's abort *processing* after arrival,
  opening the chain-abort variant of the window deterministically); **5095/5096**
  (arm/stall ABORT in a 10 ms self-resend loop, 14593-14607); **5016** (delay
  GSN_ABORTREQ +2000 ms, 14834-14838 — THE takeover-window lever); TC-side
  **8089/8105** (abort batching breaks, 10403-10405).
- **COMMIT/COMPLETE path**: **5011/5012** (delay GSN_COMMIT at LQH, 13261-13269);
  **5017** (delay GSN_COMMITREQ +2000 ms, execCOMMITREQ); **5013/5014** (COMPLETE
  variants, 13463-13469); **5018** (delay GSN_COMPLETEREQ +2000 ms,
  execCOMPLETEREQ); TC **8113** (discard GSN_COMMIT, 7658-7663).
- **Controls**: EI **4019** forces 899 on every rowid-carrying insert
  (DbtupExecQuery.cpp:4389-4393); `DEBUG_899_ERROR` (DbtupFixAlloc.cpp:37).
- Detector caveat from W1/W2: an 899 trap must whitelist occurrences that
  coincide with TC failover or redo congestion, or assert *recurrence* rather
  than single occurrence.
