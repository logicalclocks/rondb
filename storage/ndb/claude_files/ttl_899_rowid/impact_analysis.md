# Impact analysis: always-carry + verify replica rowids (TTL 899 hardening)

Status: analysis complete. Companion to `findings.md` (read that first for the defect
mechanics). This document is the REVIEW BASIS for the fix patch being developed in a
separate worktree.

Date: 2026-07-31, branch 25.10-main (HEAD near 59b3b3cbdf1). Line numbers are as read
today; sibling work is concurrently editing `DblqhMain.cpp` (test error insert), so
expect small drift there — function names are the durable anchors. All paths relative
to `storage/ndb/` unless noted.

Principle under analysis: *"Backup replicas should never receive any operation without
a rowid, for any INSERT, WRITE, UPDATE or DELETE. Any allocation of a rowid on the
backup means the primary replica can no longer be safe to select a rowid."*
Planned fix: the primary always attaches the operation's rowid to replica-forwarded
LQHKEYREQs; the backup verifies it against its ACC-found rowid (found case) or uses it
as a required rowid (insert-family not-found case).

---

## 1. Invariant formalization

### 1.1 The invariant

Per fragment, let `occupied(R)` be the set of rowids `(frag_page_id, page_idx)`
holding a row on replica R. The safety condition for primary-side allocation is:

```
occupied(backup) ⊆ occupied(primary)        equivalently  free(primary) ⊆ free(backup)
```

for every backup at every point where the primary picks a new rowid. The primary picks
rowids from its own free structures only — `Dbtup::alloc_fix_rec`
(`src/kernel/blocks/dbtup/DbtupFixAlloc.cpp:97`): first page on `thFreeFirst`, else a
new page. If the subset relation holds, the chosen rowid is guaranteed free on every
backup, and the backup's *required-rowid* insert `alloc_fix_rowid`
(`DbtupFixAlloc.cpp:265-328`) cannot fail. If a backup ever allocates a rowid the
primary did not dictate, `occupied(backup) ⊄ occupied(primary)` permanently: the
primary will eventually pick that rowid from its free list and the backup fails it
with 899 (`ZROWID_ALLOCATED`, `dbtup/Dbtup.hpp:217`) — arms at
`DbtupFixAlloc.cpp:294` (page `ZTH_MM_FREE`, slot taken) and `:319` (page
`ZTH_MM_FULL`). **`alloc_fix_rowid` is the layout-agreement tripwire; any rowid-less
insert on a backup routes around it into `alloc_fix_rec`
(`dbtup/DbtupExecQuery.cpp:4349-4386`, `!rowid` branch) which cannot fail and
self-chooses.** That is the whole defect class.

### 1.2 How the invariant is maintained today

1. **Allocation is dictated forward.** For ZINSERT/ZREFRESH the primary sets
   `m_use_rowid` after execution (`dblqh/DblqhMain.cpp:11711`) with `m_row_id` filled
   by `accminupdate` from the TUP-chosen key (`dblqh/Dblqh.hpp:5531-5557`,
   `regTcPtr.p->m_row_id = *key`); `packLqhkeyreqLab` puts it on the wire
   (`DblqhMain.cpp:12450` rowid flag, `:12593-12597` the two words). The receiver
   takes it at `DblqhMain.cpp:9099` (`m_use_rowid = getRowidFlag`), `:9295-9297`
   (words), passes it to TUP (`:11681`, `:11689`), and TUP inserts at exactly that
   rowid (`DbtupExecQuery.cpp:2140-2148` unpack; `:4387-4421` `alloc_fix_rowid` /
   `alloc_var_row(..., true)` branch).
2. **Deallocation is ordered backup-first.** Commit enters the replica chain at the
   LAST replica: `Dbtc::sendCommitLqh` targets `regTcPtr->lastLqhNodeId`
   (`dbtc/DbtcMain.cpp:7650-7655`) and COMMIT propagates backwards to the primary.
   On top of that, LQH enforces "It is important that ROWIDs are released on Backup
   replicas before the Primary replica" with the dealloc reference-count machinery
   (`DblqhMain.cpp:11042-11080` design comment; `incrDeallocRefCount` /
   `decrDeallocRefCount` `:11088-11196`; TUP told to free only at refcount 0,
   `:11170-11177`). Hence a rowid freed on the primary (thus eligible for reuse) is
   already free on all backups: `free(primary) ⊆ free(backup)` is re-established at
   every delete.
3. **Per-row serialization.** DBACC's lock queue admits one write at a time per
   element (design comment `dbacc/DbaccMain.cpp:1659-1740` at `execACCKEY_ORD`;
   found-row locking `:1500-1549`), and replica hops are FIFO per LDM-pair
   transporter path, so all replicas apply the same per-row operation order.
   Combined with (1) and (2), replica layouts stay in lockstep and 899 is
   "impossible" in normal traffic on rowid-disciplined tables.

### 1.3 What rowid-less forwarding removes

For the rowid-less channels (section 2: D1, D2a, dirty-WRITE), a backup that misses a
row the primary has does **not** hit the tripwire: ACC not-found converts
ZWRITE→ZINSERT (`DbaccMain.cpp:1612-1628`) or accepts the plain ZINSERT, and TUP
self-allocates (`DbtupExecQuery.cpp:4349-4386`). No error, permanent fork, delayed 899
in unrelated traffic (findings §6). In the reverse direction, a backup that
unexpectedly HAS the row absorbs a rowid-carrying ZINSERT silently via the TTL
dup-convert (`DbaccMain.cpp:1490-1499` — no is-backup guard), ignoring the wire rowid.
The fix restores the tripwire in both directions: not-found ⇒ required-rowid insert
(899 on conflict), found ⇒ explicit rowid comparison.

899's API classification makes the delayed symptom nearly undiagnosable:
`{899, HA_ERR_LOCK_WAIT_TIMEOUT, TR, "Rowid already allocated"}`
(`src/ndbapi/ndberror.cpp:297`) — temporary, surfaced as lock-wait-timeout, silently
retried by NdbAPI loops.

---

## 2. Complete channel enumeration

Wire senders of LQHKEYREQ: DBTC (`DbtcMain.cpp:5928`; never sets the rowid flag —
receiver asserts `rowid ⇒ sender != DBTC`, `DblqhMain.cpp:9310-9311`), DBLQH
(`packLqhkeyreqLab`, `DblqhMain.cpp:12430-12697`), RESTORE
(`src/kernel/blocks/restore.cpp:3062,3097,3128`), DBSPJ (reads only,
`dbspj/DbspjMain.cpp:4890`; keyinfo-less SPJ requests are refused with
ZNO_TUPLE_FOUND at `DblqhMain.cpp:9271-9277`). REDO replay produces internal ops (no
signal; `readLogHeader`, `DblqhMain.cpp:34407-34453`). SUMA sends none — the binlog
applier is an NdbApi client entering via TCKEYREQ (marked only by
`RI_REPLICA_APPLIER`, `include/kernel/signaldata/LqhKey.hpp:241`,
`DblqhMain.cpp:9124-9129`).

Receiver-side rowid intake: `DblqhMain.cpp:9099` (flag), `:9295-9297` (words),
shape whitelist `:9310-9323` (rowid-less ZINSERT from a non-TC sender asserted to be a
TTL table), no-keyinfo ops require rowid+NrCopy (`:9279-9281`, delete-by-rowid only).
Forward-side rowid attach: `:11711` (`op == ZINSERT || op == ZREFRESH`),
`:12448-12449` (`m_use_rowid |= m_copy_started_state == AC_NR_COPY` — **all** ops
carry rowid while a copy is active on the fragment), VM_TRACE shape assert
`:12477-12488`, TTL ZWRITE re-masking `:12510-12519`, dirty-op ZWRITE masking
`:12531-12537`.

| # | Channel (op on wire, sender→receiver) | Rowid carried? | Receiver behavior today | Tripwire? |
|---|---|---|---|---|
| C1 | Any op, TC→primary | never (by design; assert `:9310-9311`) | primary resolves/allocates | n/a (primary IS the allocator) |
| C2 | ZINSERT primary→backup (normal insert, incl. TTL not-found case) | **yes** (`:11711`) | not-found ⇒ `alloc_fix_rowid` at wire rowid; found ⇒ non-TTL: ACC 630 (`DbaccMain.cpp:1603-1607` → `insertExistElemLab`); TTL: silent dup-convert, wire rowid ignored (`DbaccMain.cpp:1490-1499`) | ON for not-found; **OFF (masked) for TTL found** |
| C3 | **D1**: ZINSERT_TTL primary→backup — wire is plain ZINSERT, flag 0 (`:11705-11711` deliberately skips; whitelist `:12477-12488` send, `:9310-9323` receive) | **no** | found ⇒ backup re-derives ZINSERT_TTL (`exec_acckeyreq` `:10230-10254`, no primary-only guard) and updates in place at ITS OWN rowid; not-found ⇒ `alloc_fix_rec` **self-allocation, no error** | OFF both directions |
| C4 | **D2a**: TTL ZWRITE the primary resolved to ZUPDATE, primary→backup (op re-masked to ZWRITE `:12510-12519`) | **no** (`:11711` doesn't fire for ZUPDATE) | found ⇒ ZUPDATE in place at own rowid; not-found ⇒ ACC ZWRITE→ZINSERT (`DbaccMain.cpp:1612-1628`) ⇒ self-allocation | OFF both directions |
| C5 | **D2b**: TTL ZWRITE the primary resolved to ZINSERT, primary→backup (same re-mask to ZWRITE, but `:11711` fired because post-conversion op==ZINSERT) | **yes** (refinement over findings §3.4) | not-found ⇒ required-rowid insert (tripwire ON); found ⇒ ZUPDATE at own rowid, **wire rowid ignored** | ON not-found; OFF (masked) found |
| C6 | Dirty write, primary→backup, any table — forwarded as ZWRITE (`:12531-12537`) | only if resolved-INSERT | same ZWRITE re-resolution as C4/C5 | pre-existing rowid-less hole; dirty writes documented non-consistent (`:12667-12686`) |
| C7 | ZUPDATE / ZDELETE primary→backup (plain) | **no — by design** | PK resolution; not-found ⇒ ACC 626 (`DbaccMain.cpp:1629-1636`) ⇒ REF ⇒ replica-error abort | LOUD (626) for presence divergence; no rowid check for placement divergence |
| C8 | ZREFRESH primary→backup | **yes** (`:11711`) | required-rowid semantics | ON |
| C9 | NR copy rows, live→starting (`initCopyTc` `:23173-23215`: ZINSERT/ZDELETE, NrCopyFlag, GCIFlag, dirty, single-hop) | **always** (`:21636` `m_use_rowid = true`; delete-by-rowid without keyinfo `:9279-9281`) | `handle_nr_copy` NrCopy branch `:10378-10494`: match⇒ZUPDATE; delete-at-rowid + delete-by-PK + reinsert otherwise | ON (plus its own reconcile logic) |
| C10 | Normal traffic live→starting during active copy (all op types, incl. D1/D2 shapes) | **always** — `:21548-21551` "Start sending ROWID for all operations from now on" + `:12448-12449`; stops at `closeCopyLab` `:22104-22107` | `handle_nr_copy` non-NrCopy branch `:10495-10574` keyed on the wire rowid; TTL-ZWRITE new-key exception `:10501-10502`, `:10550-10551` (fix 310f19cbd03) | ON (copy-phase matching) |
| C11 | REDO replay (internal) | **always** — prepare-log header always contains the rowid (`writeLogHeader` `:12753-12787`, words 6-7); `m_use_rowid=(op==ZINSERT)` after ZINSERT_TTL→ZINSERT folding (`:34449-34452`) | replay tolerates 899/630 on INSERT, 626 on UPDATE/DELETE (`logLqhkeyrefLab` `:31616-31637`), else `progError "You have found a bug!"` `:31656-31667` | ON but tolerant (commit-ordered full images re-converge) |
| C12 | LCP restore (RESTORE→LQH) | **always** (`restore.cpp:3062`), plus `AccKeyReq::NoTTLDupConvert` (`exec_acckeyreq` `:10197`, only `m_restore_op`) | required rowid; duplicates 630 + delete/reinsert recovery | ON |
| C13 | Unique-index fragments of TTL tables: UI maintenance is ZINSERT/ZDELETE built in TC (`DbtcMain.cpp:25240`, `:25335`; TE_UPDATE = delete+insert `:23601-23636`; deferred path normalizes ZINSERT_TTL→ZUPDATE first, `dbtup/DbtupTrigger.cpp:839-860`), ordinary TC-coordinated ops to the index fragment's primary | C1 into the fragment; C2/C3 to its backups | UI fragments ARE TTL (`is_ttl_table` follows `primaryTableId`, `DblqhMain.cpp:4590-4604`), so a UI ZINSERT over an expired index entry dup-converts ⇒ **D1 applies to UI fragments**. No ZWRITE ⇒ D2 does not | same as C2/C3 |
| C14 | Blob part tables (NDB$BLOB) of TTL main tables | C1/C2 only | **part tables are NOT TTL tables**: DBDICT clears `primaryTableId` for blob tables in `create_fragmentation` (`dbdict/Dbdict.cpp:6899-6919`), LQH then self-points it (`DblqhMain.cpp:2686-2687`) with `m_ttl_sec = RNIL` (`:2702-2703`), DBTC gets RNIL because blob parts are UserTables (`Dbdict.cpp:8356-8369`). No dup-convert, no D1/D2. Expired-image parts are cleaned by NdbBlob probe-delete (commit 00bb1386297, `src/ndbapi/NdbBlob.cpp:2731-2760`); the purge deletes parts implicitly via take-over blob handles (`src/ndbapi/NdbScanOperation.cpp:3188-3197`) | ON (full insert discipline preserved) |
| C15 | Fully-replicated propagation: FULLY_REPLICATED trigger → TC → one TCKEYREQ per copy fragment with ZINSERT/ZUPDATE/ZDELETE (`DbtcMain.cpp:25516-25700`, op mapping `:25576-25592`; TE mapping incl. ZINSERT_TTL→TE_INSERT `dbtup/DbtupTrigger.cpp:1655-1690`, `:1450-1458`), delivered to each copy fragment's primary and chained normally | C1 into each copy fragment | each copy-fragment replica set independently repeats C2-C8; DBLQH has zero fully-replicated code (grep: no hits) | per copy fragment, same as above |
| C16 | Table reorg / add-nodegroup data movement: TRIX→DBUTIL→TC as ZWRITE with SOF_REORG_COPY (`trix/Trix.cpp:1122-1159`, `dbutil/DbUtil.cpp:1706`, `DbtcMain.cpp:4365-4370`, anyNode=2 `:4874-4886`); reorg trigger uses ZWRITE for TE_UPDATE (`:25378-25414`, `optype = ZWRITE` at `:25407`) | C1 into target fragments | all TC-coordinated; **no private replica-forwarding path** (agent-verified). On a TTL table, reorg traffic is a large D2 population once inside the fragment | same as C4/C5 |
| C17 | Binlog applier / SUMA-sourced transactions | C1 (TCKEYREQ; `ReplicaApplierFlag` only) | primary-side entry — out of scope as a *channel*; its WRITE ops populate D2, and its TTL-ignore handling is SQL-layer (`plugin/ha_ndbcluster.h:746-758`, six gates in `ha_ndbcluster.cc`; commit 102151c53e7 — PK-less lookup is a normal scan, no kernel bypass) | n/a |

**Rowid-less today, in summary:** C3 (D1), C4 (D2a), C6 (dirty ZWRITE-resolved-update),
C7 (plain UPDATE/DELETE — loud on presence divergence but blind to placement
divergence), and C1 (by design). The fix's scope decision is exactly which of
C3/C4/C6/C7 to convert.

---

## 3. Where backup-side allocation/resolution is RELIED UPON today

Each site: current behavior → behavior under always-carry+verify → does anything
legitimate depend on the current silent behavior?

**R1. D1 backup not-found self-allocation** (`DbtupExecQuery.cpp:4349-4386` via
C3). Nothing legitimate depends on it — when presence agrees (the only correct
state), the backup always FINDS the row. Under the fix, not-found becomes a
required-rowid insert at the wire rowid: if the slot is free the missing row is
**healed at the primary's exact rowid** (the op's AttrInfo is the full insert
image, so content is right too); if occupied, 899/REF — loud at the point of
divergence. Both outcomes are strictly better than today.

**R2. TTL ZWRITE backup re-resolution** (C4/C5; `continueACCKEYCONF`
`:11568-11619`). The found/not-found re-resolution itself IS load-bearing and must
stay: replicas must receive ZWRITE precisely so they never run checkTTL on the
already-decided upsert (design comment `:11578-11596` — forwarding the resolved
ZUPDATE would 626 on a row that is expired by the replica's clock). The fix must
therefore keep ZWRITE as the wire op and only add the rowid + verify/required-rowid
semantics. Do NOT "simplify" by forwarding the resolved op — that reintroduces the
clock-skew abort class (findings M1) unless checkTTL suppression is redesigned too.

**R3. ACC dup-convert of a rowid-carrying ZINSERT on a non-primary replica**
(`DbaccMain.cpp:1490-1499`, no is-backup guard — the masking direction). This is
legitimately relied upon by three consumers and must NOT get an is-backup guard:
(a) it is exactly how a backup will apply insert-over-expired once D1 carries a
rowid (found at the SAME rowid ⇒ dup-convert ⇒ in-place refresh — the normal case
under the fix); (b) REDO replay REQUIRES the conversion (`exec_acckeyreq` comment
`:10184-10196`: suppressing it for replay would drop committed replacements and
abort recovery); (c) fully-replicated copy-fragment primaries re-derive ZINSERT_TTL
from the propagated TE_INSERT (`DbtupTrigger.cpp:1450-1458`). The masking problem
is solved in DBLQH instead: verify wire rowid == ACC-found local key. Only the
found-at-a-DIFFERENT-rowid case (provable divergence) turns into a REF.

**R4. `handle_nr_copy` TTL special cases** (`:10501-10502`, `:10550-10551`).
NOT removable under the fix: the wire op for TTL upserts remains ZWRITE (R2), so
the "TTL ZWRITE with !match is a new-key insert, don't ignore" exception is still
required. It is also unharmed: in the copy phase these ops already carry rowids
(C10), which is what its `nr_read_pk`-based matching consumes.

**R5. The rowid-less end-of-copy sentinel** (`handle_nr_copy` `:10328-10343`:
"Rowid not set, that mean that primary has finished copying" ⇒ flip fragment to
AC_NORMAL). Under always-carry from a new primary this sentinel never fires. That
is safe on current versions: the live node also sends an explicit
`COPY_FRAG_DONE_REP` at copy close (`closeCopyLab` `:22115-22153`, gated on
`ndbd_support_copy_frag_done(getNodeInfo(nextReplica).m_version)`), and the
receiver flips to AC_NORMAL there (`execCOPY_FRAG_DONE_REP` `:28691-28708`), with
COPY_ACTIVEREQ as the final fallback (`:22422`). The sentinel code must be KEPT for
rolling-upgrade compatibility with old primaries (which stop attaching rowids at
`:22107`), and the starting node staying in AC_NR_COPY slightly longer just means
ops keep taking the (correct, rowid-keyed) `handle_nr_copy` path until DONE_REP
arrives. Reviewers should confirm the fix does not tighten `:10328` into an error.

**R6. Middle-replica forwarding uses its OWN resolution.** On every replica,
`acckeyconf_tupkeyreq` overwrites `m_row_id` with the LOCAL ACC-found key
(`:11699-11700`) and `accminupdate` overwrites it with the LOCAL TUP-chosen key on
inserts (`Dblqh.hpp:5531-5557`); `packLqhkeyreqLab` then forwards *that* value.
So at NoOfReplicas>=3 the middle replica propagates its own layout, amplifying any
fork (`findings` §3.4). Under the fix this becomes sound **only if verification
runs on every non-primary hop** (`seqNoReplica != 0`), because then
forwarded == verified-equal-to-primary. The fix must not verify only on the last
replica. Note 50b509a8384 precedent: ZWRITE stays ZWRITE through the middle hop
(re-mask gate `seqNoReplica == 0`, `:12510-12519`).

**R7. Asserts/whitelists that encode today's shapes.** The send-side VM_TRACE
assert (`:12477-12488`, rowid-less ZINSERT allowed iff ZINSERT_TTL or next node
down) and the receive-side TTL whitelist (`:9310-9323`) both must be relaxed for
the transition (old-primary→new-backup still sends rowid-less D1) and can be
tightened to ndbrequire only after the version floor guarantees always-carry.

**R8. PK-less tables via the applier** (commit 102151c53e7). Entirely SQL-layer:
`find_row` scans with `SO_TTL_IGNORE`/`OO_TTL_IGNORE`
(`ha_ndbcluster.cc:3552-3554`, `:3823-3825`, `:3944-3946`; helper
`ha_ndbcluster.h:746-758`). Normal TC path; nothing exploits backup
re-resolution; unaffected by the fix.

---

## 4. Per-subsystem impact

### 4.1 Node-restart copy (including the today-reachability question)

**Is the copy phase a TODAY-reachable instance of backup self-allocation? NO — but
only because of an unverified sender-state protocol.** Trace:

- From copy-scan start the LIVE node attaches rowids to **all** forwarded ops on
  that fragment (`accScanConfCopyLab` `:21548-21551` sets `m_copy_started_state =
  AC_NR_COPY`; `packLqhkeyreqLab` `:12448-12449` ORs it into `m_use_rowid`). It
  stops only in `closeCopyLab` (`:22104-22107`), which runs after the scan is done
  AND all copy rows are confirmed by the starting node (`copyCountWords` gate
  `:22087-22096`).
- The starting node enters AC_NR_COPY only on the first NrCopyFlag op
  (`:9485-9508`), which is sent after `:21551`. Copy rows and normal LQHKEYREQs
  share the same LDM-to-LDM FIFO path, so ordering holds: **a rowid-less op cannot
  reach `handle_nr_copy` while the copy is active**; a rowid-less arrival implies
  the live node passed `:22107`, which is exactly what the `:10328-10343` sentinel
  assumes. Rowid-less D1/D2 shapes therefore exist only OUTSIDE the copy window
  (normal backups), consistent with findings §3.4/§5-S1/S2.
- Ops for **not-yet-copied ranges** during copy (rowid attached): `nr_read_pk`
  at the wire rowid returns len==0 ⇒ !match ⇒ ZINSERT and TTL-ZWRITE run as
  insert at the wire rowid (`:10550-10573` → `exec_acckeyreq` → TUP required-rowid
  insert via the wire flag from `:9099`). Placement matches the primary; the later
  copy row for that rowid takes the match path (`:10390-10404`, INSERT→ZUPDATE).
  Ops for **copied ranges**: match path, in-place. No self-allocation anywhere.
- **NoTTLDupConvert and copy inserts**: NR-copy ops do NOT set it
  (`exec_acckeyreq` `:10197` passes only `m_restore_op`) — but the dup-convert is
  UNREACHABLE for NrCopyFlag inserts, because `handle_nr_copy` always deletes at
  the target rowid and by PK before running the insert (`:10468-10494`), via
  `nr_copy_delete_row` (`:10640-10707`) + `Dbtup::nr_delete`
  (`DblqhMain.cpp:10750`, `DbtupExecQuery.cpp:10360` — physical delete, no
  checkTTL), so ACC cannot find the PK when the insert runs. The hypothesized
  "copy insert dup-converted onto a self-allocated rowid" cannot occur.
  RECOMMENDATION (defensive, zero-behavior-change): extend `:10197` to
  `m_restore_op || LqhKeyReq::getNrCopyFlag(reqinfo)` so the intent is explicit
  and the invariant survives future refactors of the delete-first logic.
- **Consequence flavors if the protocol ever leaks a rowid-less op mid-copy**
  (e.g. a future regression on the send side): the `:10328` sentinel flips the
  half-copied fragment to AC_NORMAL; the NEXT copy row then hits
  `ndbrequire(rowidFlag && op == ZDELETE)` at `:10121-10123` and CRASHES the
  starting node — loud, not a silent fork. The silent-fork flavor would require
  the flip plus no further copy rows, i.e. a leak in the final window, where the
  fragment is already complete. Net: today's exposure is a crash risk on protocol
  regression, not a reachable fork. The fix (always-carry) makes the leak
  impossible from new primaries and demotes the sentinel to legacy-compat (R5).
- **Transient side-finding (pre-existing, not a fork):** an insert-over-expired
  arriving mid-copy for an ALREADY-copied rowid takes the match path and becomes
  ZWRITE→ZUPDATE (`:10522-10536`), but `original_operation` stays ZINSERT (set
  once at `:9111`; TUP copies it at `DbtupExecQuery.cpp:2216,2226`), so the
  checkTTL gate applies (`:3498-3502`) and 626-aborts the transaction
  (`:3513-3521`) until the fragment leaves copy mode (after which
  `exec_acckeyreq` `:10230-10254` re-derives ZINSERT_TTL and it succeeds).
  Retriable, loud, self-healing — same family as findings note N2. The fix does
  not change this; listed so the fix's new REF is not confused with it in tests.
- **Verification gating**: the backup-side rowid verification MUST be restricted
  to `activeCreat == AC_NORMAL` ops. In AC_NR_COPY, `handle_nr_copy` deliberately
  reconciles mismatches (delete-at-rowid, delete-by-PK, ignore-below-scan-pointer)
  and in AC_IGNORED nothing is applied at all (`:10140-10161`); verifying there
  would abort legitimate recovery traffic.

### 4.2 REDO log

No format or size change. The prepare-record header has ALWAYS contained the rowid
for every op class (`writeLogHeader` `:12753-12787`, words 6-7 = `m_row_id`,
`ZLOG_HEAD_SIZE` fixed), because `m_row_id` is filled for every op after
ACC/TUP (`:11699-11700`, `accminupdate`). Attaching the same value to the wire
changes nothing in what is logged. Replay-originated ops are built from the log,
not from LQHKEYREQ (`readLogHeader` `:34407-34453`), never traverse
`packLqhkeyreqLab` toward a replica during replay, and keep their tolerances
(`logLqhkeyrefLab` `:31616-31637`). One reviewer check: the fix must not set
`m_use_rowid` in a way that alters `readLogHeader`'s derivation
(`m_use_rowid = (op == ZINSERT)`, `:34452`) — that is replay-local and correct.

### 4.3 LCP / restore

Restore inserts are already required-rowid + NoTTLDupConvert (C12;
`restore.cpp:3062`, `:10197`; `DbaccMain.cpp:1481-1499` exception comment), i.e.
restore already lives under the target discipline. No interaction: restore ops are
single-node (RESTORE→local LQH, REF routed back via `:15511-15523`), never
replica-forwarded. The fix's backup verification never sees them
(sender==RESTORE ⇒ not a replica hop; verification predicate should key on
`seqNoReplica != 0`, which restore ops don't set).

### 4.4 Fully-replicated tables

- **Propagation mechanism** (agent-verified): DBTUP fires FULLY_REPLICATED_TRIGGER
  (`DbtupTrigger.cpp:1424-1428`, gate `:1217-1254`) → FIRE_TRIG_ORD → TC
  `executeFullyReplicatedTrigger` (`DbtcMain.cpp:25516-25700`) iterates copy
  fragments via DIH's `nextCopyFragmentId` (`dbdih/DbdihMain.cpp:15699-15714`) and
  issues one ordinary TCKEYREQ per copy fragment with **ZINSERT/ZUPDATE/ZDELETE**
  (`:25576-25592`; never ZWRITE — that is the reorg trigger, `:25407`), routed to
  that fragment's primary (anyNode=3 `:4886-4888`) and chained to its backups
  normally. The only wire mark is NoTriggersFlag (`:5561-5563`).
- **Rowids across fragments: independent, and nothing assumes otherwise.** DBLQH
  contains zero fully-replicated code (grep of `DblqhMain.cpp`/`Dblqh.hpp`: no
  `fullyReplicated`/`FULLY_REPLICATED` hits); TC never transmits rowids
  (`:9310-9311`); each fragment's primary allocates independently
  (`DbtupExecQuery.cpp:4351-4386`). New nodegroups are populated by the reorg copy
  (TRIX/DBUTIL ZWRITE + trigger fan-out, `DbdihMain.cpp:16324-16353`,
  `:15946-15968`), not by CopyFragReq — no rowids carried. **Conclusion: the fix
  needs no fully-replicated-specific logic; rowid agreement is and remains a
  within-fragment property. The D1/D2 amplifier simply exists independently in
  EVERY copy-fragment replica set of a fully-replicated TTL table (TTL+FR is
  allowed — no validation anywhere; tests `ndb_ttl/ttl_read_locked.test:48,353`,
  `ttl_blob_scan_replica.test:61`), so the fix multiplies its coverage there
  automatically.**
- **NEW adjacent findings (flagged; not rowid bugs, need live verification):**
  1. `executeFullyReplicatedTrigger` builds the propagated TCKEYREQ with **no TTL
     flags** (no TTLIgnore set anywhere in `:25516-25700`; `regCachePtr->m_ttl_ignore`
     defaults 0 at `:4283` and is serialized at `:5551`). A purge take-over delete
     of an expired row commits on the MAIN fragment under its serialized
     ttl_ignore, but the propagated TE_DELETE runs `handleDeleteReq` on each copy
     fragment with `ttl_ignore==0` ⇒ checkTTL sees the row expired ⇒ 626
     (`DbtupExecQuery.cpp:4715-4756`) ⇒ TC aborts the WHOLE transaction — by
     explicit policy for copy-fragment failures (`DbtcMain.cpp:9273-9282`, "no
     error codes that makes this behaviour ok"). If confirmed live, **the TTL purge
     can never commit a delete on a fully-replicated TTL table**, the expired
     backlog grows unboundedly (feeding the S5 restart-capacity risk), and every
     applier/OO_TTL_IGNORE write to an expired row has the same problem. No
     ndb_ttl_purge/ndb_ttl_rpl test covers FULLY_REPLICATED (grep: only ndb_ttl
     scan/lock tests do). Needs a targeted test before or with the fix.
  2. Open in-code crash note for insert-over-expired on fully-replicated:
     `DbtupTrigger.cpp:1660-1666` ("crash on fully_replicated table here: 1. insert
     1 row 2. wait for it expires 3. insert the same row again").

### 4.5 Table reorg / copying ALTER

No replica-forwarding paths of their own (C16): all movement is TC-coordinated
(TRIX→DBUTIL ZWRITE copy, reorg-trigger ZWRITE/ZINSERT/ZDELETE), entering fragments
through C1 and fanning to backups through the standard chain. Impact of the fix:
reorg on a TTL table currently generates heavy D2 traffic (every copied row is a
ZWRITE); under the fix these become carried+verified — reorg of a TTL table becomes
a cluster-wide layout audit, which is good but means reorg is a stress test the fix
must pass (recommend running `ndb_rpl`/reorg + TTL tests together). Copying ALTER
(mysqld-side copy) is plain TCKEYREQ traffic — same story. The only
reorg-specific TTL coupling is the committed-changes counter already serialized via
TTLOnlyExpiredFlag (`:12467-12475`, `DbtupCommit.cpp:2672-2685` per findings).

### 4.6 Mixed-version rolling upgrade

- **Send-side gate**: decide per hop with the receiving replica's version, exactly
  like the adjacent precedent `ndbd_support_copy_frag_done(getNodeInfo(
  regTcPtr->nextReplica).m_version)` (`:22115-22116`) and short/long selection in
  `sendBatchedLqhkeyreq` (`:11929-11943`). Add an `ndbd_replica_rowid_forwarding()`
  helper in `include/ndb_version.h.in` following the multi-line RonDB pattern of
  `ndbd_support_drop_table_notification` (`ndb_version.h.in:1236-1260`: per-branch
  floors for 25.10.x / 26.2.x / 26.4.x).
- **New primary → old backup**: keep today's shape (rowid-less D1/D2a; C5 keeps its
  rowid — old receivers already accept rowid-carrying ZWRITE). What is lost: no
  protection on that hop until the backup upgrades. With 3 replicas and a mixed
  chain, gate per hop so upgraded hops are protected even when one is not.
- **Old primary → new backup**: must keep accepting rowid-less D1/D2 (whitelist
  `:9310-9323` stays; verification triggers only when the rowid flag is set).
  Fork risk remains open until the primary side is upgraded — document that the
  guarantee starts when ALL data nodes run the fix (same wording as other
  ndbd_* features). Optional: count rowid-less TTL-channel arrivals on new
  backups for observability during the window.
- The rowid words sit in the long-signal `variableData` keyed by the flag bit
  (RI_ROWID_SHIFT=31 already exists for every version since NDBD_ROWID_VERSION,
  `LqhKey.hpp:276,296-338`) — no new signal, no new section, so no
  signal-compat cliff; only semantics (which ops set the bit) changes.

### 4.7 Performance / size

- Wire: +2 words (8 bytes) per forwarded UPDATE/DELETE/resolved-update-WRITE
  LQHKEYREQ. Capacity is safe: `variableData[10]` (`LqhKey.hpp:100`); worst case
  after the fix = applAddr 2 + sameClient 1 + nodeAfterNext 1 + readlenAi 1 +
  rowid 2 + gci 1 = 8 ≤ 10; fixed 11 + 8 = 19 ≤ 25-word signal. LQH→LQH requests
  are long-signal on all current versions (`:12607-12655` sections;
  `sendBatchedLqhkeyreq` `:11929-11943`); relative overhead vs key+attrinfo
  sections is well under 1% for typical rows. INSERT/REFRESH already pay it.
- Backup CPU: found-case verification = one 2-word compare per write op
  (nanoseconds). Not-found insert-family ops switch `alloc_fix_rec` →
  `alloc_fix_rowid` only for D1/D2a shapes; D2b/C2 already use it. The rowid path
  does a page-map lookup by `frag_page_id` (`DbtupFixAlloc.cpp:275-284`) instead
  of a free-list pop — measurable only in microbenchmarks, and the not-found case
  on a healthy cluster occurs exactly for genuinely-new keys via D2b (already
  paying it today).
- REDO: zero delta (4.2). TC: zero delta (TC path untouched).

### 4.8 Failure policy for a verification mismatch on the backup

Options considered against precedent:

1. **REF with 899**: rejected. 899 is classified TR / lock-wait-timeout
   (`ndberror.cpp:297`) — NdbAPI retry loops and mysqld absorb it; the whole point
   of the fix is a non-maskable signal. (The not-found/slot-occupied arm will
   still naturally produce 899 from `alloc_fix_rowid`; acceptable there because it
   is the pre-existing insert semantics, but see recommendation below.)
2. **ndbrequire/ndbabort the backup**: precedent exists for
   detected-inconsistency-kills-node (REDO `progError "You have found a bug!"`
   `:31656-31667`; `alloc_fix_rowid` default-arm `ndbabort`
   `DbtupFixAlloc.cpp:325`; LCP restore row-count mismatch 2352; COPY_FRAG
   626→2303 starting-node shutdown, `initCopyTc` comment `:23188-23199`), but all
   of those are RECOVERY contexts. Killing the backup in normal traffic trades a
   consistency bug for an availability incident, and worse: a REDO-based node
   restart can REPLAY the divergent history (copy phase skips rows with GCI ≤ the
   starting node's `completedGci` — `PREPARE_COPY_FRAG_CONF` `:21076-21140`, TUP
   copy-scan GCI filter `dbtup/DbtupScan.cpp:1261-1330`), so the node can
   crash-loop until someone runs an INITIAL restart. Rejected as default;
   acceptable as an opt-in debug escalation.
3. **REF with a NEW permanent error code** — RECOMMENDED. Mechanics already exist
   end-to-end: a backup prepare-phase REF is abortable and travels upstream via
   `execLQHKEYREF` (`:5591`, marking `ABORT_FROM_LQH_REPLICA` at `:5652-5661`) →
   `LqhKeyRef::setReplicaErrorFlag` (`:15497-15499`) → TC aborts the transaction
   unconditionally ("If the error came from a backup replica ... abort ... in all
   cases", `DbtcMain.cpp:9202-9218`). Add one LQH error code (12xx block; 1233-1237
   in use in `Dblqh.hpp:404-417`, 1238-1243 taken in `ndberror.cpp:503-508` — use
   the next free, e.g. 1244 "Replica rowid mismatch detected") classified
   **DMEC, IE (permanent)** in `ndberror.cpp`, plus a `g_eventLogger->error` naming
   table, fragment, PK hash, wire rowid, and local rowid — that line is the
   diagnosis (fragment to repair with an initial NR of the backup). Optionally
   ALSO return this code (instead of 899) from the not-found/slot-occupied arm
   when the op came over the wire with a rowid on a non-copy op, so both mismatch
   flavors are permanent and distinguishable from replay-era 899s; leave
   REDO/NR-copy paths on 899 untouched (their tolerances key on it,
   `:31629-31637`).
   Debug builds: an `ndbrequire` under ERROR_INSERT/VM_TRACE at the same site
   gives autotest a hard stop without risking production availability.

---

## 5. Residual risks the fix does NOT cover

1. **Pre-existing latent forks.** The fix prevents new divergence and DETECTS old
   divergence only when a forked slot is next touched by a rowid-carrying op —
   at which point the symptom changes from intermittent fake lock-timeouts to a
   deterministic permanent error on specific keys. That is the intended tripwire
   behavior but is operationally a behavior change: release-note it, and run the
   detector BEFORE upgrading: per-fragment cross-replica
   `ndbinfo.memory_per_fragment.fixed_elem_count` comparison (counters maintained
   at `DbtupFixAlloc.cpp:141,238,305`; table `kernel/vm/NdbinfoTables.cpp:402-426`;
   expired-unpurged rows are physical and counted, so equality is exact). Repair
   story: **initial** node restart of the diverged backup (full copy, since
   `completedGci=0`); a plain/REDO NR can preserve the fork (GCI-skip copy,
   §4.8-2 evidence). `ndb_select_all --rowid` for offline slot-level diffs.
2. **Upgrade window** (§4.6): old-primary hops stay unprotected until the floor.
3. **Deliberately rowid-less remnants** if the fix ships TTL-channels-only:
   plain ZUPDATE/ZDELETE (C7 — loud on presence divergence but blind to placement
   divergence: a fork where BOTH replicas have the row at different rowids is
   detected only by insert-family traffic until C7 is converted) and the dirty
   ZWRITE channel (C6 — documented-inconsistent, negligible population; convert
   in the all-ops stage or explicitly waive).
4. **Fully-replicated TTL adjacent defects** (§4.4): the missing TTL-flag
   propagation through FULLY_REPLICATED triggers (purge-cannot-delete suspicion)
   and the `DbtupTrigger.cpp:1660-1666` crash note are NOT addressed by rowid
   forwarding and can independently produce data-integrity/availability incidents
   on TTL+FR tables. Track separately; verify with a new FR purge test.
5. **PK-less applier tables**: correct since 102151c53e7, SQL-layer only; the fix
   neither helps nor hurts. A regression there re-seeds divergence that the fix
   will then detect loudly (which is the design intent).
6. **Within-node `is_ttl_table` disagreement** (findings §4 tail): unchecked
   overwrite hazard, independent of rowids; still open.
7. **The verification cannot say WHICH replica is wrong** — it proves
   disagreement only. Policy (§4.8) treats the primary as authoritative because
   it is the allocator; a corrupted PRIMARY layout still wins until an initial
   restart rebuilds from it or a failover switches roles. Detection remains the
   fixed_elem_count watchdog's job.

---

## 6. Rollout recommendation

Ordered stages, each independently shippable:

1. **Detection + defensive hardening (no protocol change).**
   a. Test C invariant include in `mysql-test/suite/ndb_ttl_purge/` (per-fragment
      cross-node fixed_elem_count equality) + a production watchdog (RDRS or
      mgmd cron on `ndbinfo.memory_per_fragment`).
   b. `NoTTLDupConvert` for NrCopyFlag ops (`:10197`) — behavior-neutral (§4.1).
   c. The in-development ERROR_INSERT seed + Test A repro lands here (it must
      FAIL-silent on unfixed builds and turn into the new REF once stage 2 ships —
      that flip is the fix's acceptance test).
2. **TTL channels first (D1 + D2a), version-gated.** Smallest wire delta, covers
   the entire self-allocation surface (C3/C4; C5/C2 already carry). Send side:
   set `m_use_rowid` for ZINSERT_TTL and for TTL-ZWRITE-resolved-ZUPDATE before
   `packLqhkeyreqLab`, gated on `ndbd_replica_rowid_forwarding(getNodeInfo(
   nextReplica).m_version)`. Receive side: snapshot the wire rowid BEFORE
   `:11699-11700` overwrites `m_row_id`; verify found-case (`seqNoReplica != 0 &&
   getRowidFlag(reqinfo) && activeCreat == AC_NORMAL`), REF with the new permanent
   code on mismatch; not-found insert-family already flows into required-rowid TUP
   allocation through existing plumbing (`:9099` → `:11681/11689` →
   `DbtupExecQuery.cpp:2140-2148` → `:4387-4421`) — zero TUP changes. Keep R5
   sentinel and R7 whitelists.
3. **All ops (C7 + C6): carry + verify on ZUPDATE/ZDELETE/dirty-WRITE.** Turns
   every forwarded op into a layout probe (placement-divergence detection between
   two present rows). Same gate; consider a config/DUMP kill-switch for the
   verification (not the carrying) for the first release carrying it.
4. **Tighten**: promote `:12477-12488` and `:9310-9323` to hard requires once the
   version floor guarantees stage-2 senders; retire the R5 sentinel reliance.

Gating tests: full `ndb_ttl`, `ndb_ttl_purge`, `ndb_ttl_rpl` suites (regression:
`ttl_replica3_write` for the 3-replica middle hop, `ttl_blob_part_leak` /
`ttl_blob_expired_reinsert` / `ttl_blob_scan_replica` for part tables,
`ttl_disk_expired_write` for the disk path), Test A (seeded fork → REF, plus EI
4019 as guaranteed-positive control for the 899/REF trap,
`DbtupExecQuery.cpp:4389-4393`), Test B probes 1-2 (copy×purge stress at
NoOfReplicas=3; SR replay churn), Test C invariant everywhere, one NEW
fully-replicated TTL purge test (§4.4 finding), and a mixed-version
upgrade/downgrade run exercising both gate directions. Monitoring for rollout:
the fixed_elem_count watchdog BEFORE upgrading (assess latent forks), the new
error code's event-log line and its rate AFTER.
