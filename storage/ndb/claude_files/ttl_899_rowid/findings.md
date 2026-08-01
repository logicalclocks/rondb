# TTL "leftover rowid" / Error 899 Investigation — Findings and Test Plan

Status: investigation complete. CONTINUED 2026-08-01 — tests, fix, and validation
now exist; see the companion documents in this directory:
- `repro_test.md` — natural-window verdict (NR-copy window CLOSED, three
  independent traces) + the `ttl_nr_copy_window` regression test + timing-only
  ERROR_INSERT 5113. Supersedes the §9.1 seeded-drop design: per user
  direction, tests must use natural TTL interleavings (EI only as timing aid).
- `probe_tests.md` — replica-consistency invariant include, 899 detector
  control test (signature: SQL 1205 + Warning 1297 "error 899"), 3-cycle
  copy×churn probe (green).
- `impact_analysis.md` — always-carry-rowid principle blast radius; NR copy
  already forces rowids on all ops (the fix's precedent); fully-replicated
  findings DEFERRED by user decision.
- `normal_insert_analysis.md` — normal INSERT cannot create the fork; abort
  topology closes; two benign transient-899 windows exist on healthy clusters
  (899 detectors must assert recurrence, not single hits).
- `validation_report.md` — fix compiled clean, 33/33 tests green with the
  verification both dormant and force-enabled; zero 1245 false positives.
  CAVEAT: version gate 25.10.16 means the fix is DORMANT in 25.10.15 builds.
- Fix branch: `worktree-agent-a3f75365e3707f66b` (commits 517d6e8fa94 +
  d429fda457f; patches in /tmp/ttl_rowid_fix_v2/): rowid forwarding +
  per-hop verification, new permanent error 1245, ERROR_INSERT 5118
  (ERROR_codes merge rule: 5113 + 5118 registered, Next DBLQH 5119).

Date: 2026-07-31. Branch: 25.10-main (HEAD near 59b3b3cbdf1).
Method: four parallel code-audit subtasks (purge architecture map, normal-operation
interleaving hunt, restart/recovery-path audit, test-lever inventory) plus direct
verification of the rowid allocation and memory-manager code.

Line numbers are as of the date above and will drift; commit hashes and
function/symbol names are the durable anchors. Everything here is from reading
the code — no scenario has yet been reproduced on a live cluster.

---

## 0. TL;DR

**Question investigated:** can the TTL feature create a state where a rowid is
"left over" from a TTL purge, eventually producing error 899 (ZROWID_ALLOCATED)?

**Answer:**

1. The purge itself **cannot** leave a rowid behind. It is an ordinary
   replica-synchronized distributed transaction; the expiry verdict is made once,
   on the primary, under the row lock, and shipped to backups as a serialized
   `ttl_ignore` bit. All direct purge-divergence mechanisms were traced and ruled
   out (sections 4 and 5).
2. **However**, the TTL insert paths structurally remove the required-rowid
   discipline that makes 899 "impossible" on normal tables: an insert-over-expired
   row (`ZINSERT_TTL`) and every TTL-table `ZWRITE` (REPLACE/upsert) are forwarded
   to backup replicas **without a rowid** and are re-resolved by PK through the
   backup's own ACC. If a backup ever lacks a row the primary has, the backup
   **silently self-allocates a different rowid** (`alloc_fix_rec`) instead of
   failing. That is a permanent, self-amplifying replica rowid-layout fork whose
   delayed symptom is exactly recurring error 899 in normal traffic (section 6).
3. No way to create the initial presence divergence ("seed") was found on the
   current tree — but several such bugs existed during the feature's development
   and were fixed (section 7). TTL converts any residual or future seed bug into
   silent 899 forks instead of loud immediate failures.
4. Error 899 is classified as a **temporary** error, so in production a fork
   manifests as unexplained retriable "Lock wait timeout" on inserts — easy to
   misdiagnose (section 8).
5. A deterministic test case is feasible: one test-only ERROR_INSERT that drops a
   single copy-fragment row on a TTL table (re-creating the shape of fixed bug
   310f19cbd03) turns the amplifier into a reproducible 899. Detection
   infrastructure (per-replica `fixed_elem_count` comparison) can be added to the
   existing `ndb_ttl_purge` suite with no kernel changes (section 9).
6. A hardening fix that deletes the whole bug class is sketched in section 10:
   forward the rowid with `ZINSERT_TTL` / TTL `ZWRITE` and verify it on the
   backup.

---

## 1. Original problem statement

Working hypothesis from the field: "A new TTL feature can possibly have created a
state where a rowid is left over from a TTL purge" — i.e. after the purge deletes
an expired row, some replica still considers the rowid occupied, and a later
insert that must land on that rowid fails with error 899.

Two things needed to be established:

- Is such a state remotely possible, and by what mechanism?
- Can we construct a test case that forces it?

---

## 2. Background: rowid discipline and error 899 mechanics

- Rows live at rowids `(page_no, page_idx)` in DBTUP fixed-size pages.
  The PRIMARY replica chooses rowids in `Dbtup::alloc_fix_rec`
  (`storage/ndb/src/kernel/blocks/dbtup/DbtupFixAlloc.cpp:97`): first a page from
  the fragment free list `thFreeFirst`, else a new page via `allocFragPage`.
- BACKUP replicas, REDO replay, LCP restore, and node-restart copy insert at a
  **required** rowid via `Dbtup::alloc_fix_rowid`
  (`DbtupFixAlloc.cpp:265-328`). It returns error 899
  (`ZROWID_ALLOCATED`, `Dbtup.hpp:217`) in two arms:
  - page state `ZTH_MM_FREE` but `Fix_page::alloc_record(idx) != idx`
    (slot taken) — `DbtupFixAlloc.cpp:294`;
  - page state `ZTH_MM_FULL` — `DbtupFixAlloc.cpp:319`.
  Both sites originally had `DEB_899_ERROR` debug-only logging; on this
  branch it is promoted to always-on production logging
  (`Dbtup::log_rowid_already_allocated`): silent during recovery (REDO
  replay raises 899 by design), rate-limited to 2 lines per 10 s per
  instance with a suppressed count, written to the node log (full detail)
  and via warningEvent to the cluster log.
- Out-of-DataMemory is a DIFFERENT error: page allocation failure returns 827
  (`ZMEM_NOMEM_ERROR`) in both the rowid and non-rowid paths
  (`DbtupPageMap.cpp:757,862`; `DbtupExecQuery.cpp` label `mem_error`).
  899 always means "slot/page occupied", never "out of memory".
- REDO replay deliberately TOLERATES 899 (and 630) on replayed INSERTs:
  `Dblqh::logLqhkeyrefLab`, `DblqhMain.cpp:31629-31637`. This is correct for
  replay but can MASK divergence introduced elsewhere.
- The LCP keep list exists to avoid rowid-reuse (899) issues for disk tables
  during LCP (`DbtupScan.cpp:2804-2838`).
- `free_fix_rec` (`DbtupFixAlloc.cpp:231`) frees a slot at DELETE commit
  (`DbtupCommit.cpp:228` via `execTUP_DEALLOCREQ`) and releases the page when it
  becomes entirely free.
- Invariant that normally holds: per fragment, backup replicas apply the same
  operations in the same per-row order as the primary, so rowid layouts stay in
  lockstep and `alloc_fix_rowid` never fails in normal traffic.
  899 in normal traffic ⇒ replica layout divergence ⇒ a bug.
- API classification trap: `ndberror.cpp:297` —
  `{899, HA_ERR_LOCK_WAIT_TIMEOUT, TR, "Rowid already allocated"}`.
  `TR` = temporary. mysqld surfaces it as "Lock wait timeout"; NdbAPI retry
  loops (Hugo etc.) retry it silently. Any detector must trap code 899
  explicitly.

---

## 3. TTL feature architecture (as relevant to rowids)

### 3.1 Declaration and metadata

- SQL: `CREATE TABLE ... COMMENT='NDB_TABLE=TTL=<seconds>@<column>'`
  parsed in `storage/ndb/plugin/ha_ndbcluster.cc:9853-9930`; `TTL=OFF` disables.
  Column must be `DATETIME2(0)`/`TIMESTAMP2(0)`, in-memory, not hidden/virtual.
  NdbAPI: `NdbDictionary::Table::setTTLSec/setTTLColumnNo`
  (`NdbDictionary.hpp:1338-1360`). Wire: `DictTabInfo::TTLSec=164/TTLColumnNo=165`.
- Kernel per-block metadata `m_ttl_sec/m_ttl_col_no` in DBTC
  (`DbtcMain.cpp:1091-1099`), DBLQH (`DblqhMain.cpp:2701-2707`), DBTUP
  (`DbtupMeta.cpp:195-202`). `is_ttl_table()` follows `m_primary_table_id`, so a
  TTL table's unique-index fragments count as TTL too
  (`DblqhMain.cpp:4590-4603`) — source of the "Bug #2" family (section 5, N4).
- FKs on TTL tables are rejected (`ha_ndb_ddl_fk.cc:1384,1592`).

### 3.2 Expiry check and clocks

- `Dbtup::checkTTL` (`DbtupExecQuery.cpp:2896-3038`): row expired iff
  `ttl_col + ttl_sec (+ purge_window for only-expired scans) <= now`.
  Pure integer arithmetic shared with the unit test in
  `dbtup/ttl_expiry.hpp` / `ttl_expiry-t.cpp`.
- "now" source: scans sample the clock ONCE per scan batch in DBLQH
  (`set_scan_ttl_now_sec`, `DblqhMain.cpp:4606-4634`) and pass it via
  `req_struct->ttl_now_sec`; PK ops read `my_micro_time()` per-op inside
  checkTTL (`DbtupExecQuery.cpp:3016-3018`). There is NO cluster-global TTL
  clock and NO test hook to skew it — each node uses its own wall clock.
- Reads/scans filter expired rows with 626 (`DbtupExecQuery.cpp:3143-3197`);
  only-expired scans invert the filter (`3199-3212`).

### 3.3 The purge

- Driven ENTIRELY outside the data nodes by the REST server (RDRS2):
  class `TTLPurger`, `storage/ndb/rest-server2/server/src/ttl_purge.{hpp,cpp}`.
  Worker loop `ttl_purge.cpp:1852-2009` (index scan on an index that must be
  named exactly `ttl_index`; full-scan fallback `2103-2166`).
- Per round (default sleep 1500 ms) one batch (adaptive 5..50 rows) on one
  partition per TTL table, rotating. Scan: `NdbIndexScanOperation`,
  `setPartitionId`, `setTTLPurgeWindowSize(lag)`, flags
  `SF_KeyInfo|SF_OnlyExpiredScan`, `LM_Exclusive`, parallel=1, bounds
  `[infimum, now-(ttl_sec+lag)]`; per row `deleteCurrentTuple()`; then
  `execute(Commit)`.
- The takeover delete is a NORMAL distributed transaction:
  TCKEYREQ (TakeOverScan) → TC → primary LQH → backup LQH. No local
  per-replica delete path exists anywhere.
- Expiry is decided once, on the primary, under the scan's exclusive lock.
  DBACC detects same-transaction lock ownership and returns `ignore_ttl=1`
  (`DbaccMain.cpp:2110-2155`); DBLQH merges it (`DblqhMain.cpp:10258-10291`)
  and SERIALIZES it to the backup in LqhKeyReq:
  `setTTLIgnoreFlag` (`DblqhMain.cpp:12452-12465`) and
  `setTTLOnlyExpiredFlag` (`12467-12475`). The backup's `handleDeleteReq`
  therefore skips checkTTL (`DbtupExecQuery.cpp:4709-4747`) — a lagging backup
  clock cannot reject a purge delete the primary committed.
- Purge deletes are REDO-logged and LCP-visible exactly like normal deletes
  (`writeLogHeader` `DblqhMain.cpp:12753-12787` has nothing TTL-conditional).
  Only TTL specialization at commit: only-expired deletes are excluded from the
  committed-changes counter used by copying-ALTER change detection
  (`DbtupCommit.cpp:2672-2685`).
- Multi-purger coordination via optional `mysql.ttl_purge_nodes` lease table
  (partition sharding, `(hash+p)%n`); cluster-wide knobs in
  `mysql.ttl_purge_ctrl` (ctrl_id 1 = purge lag seconds; 2/3 = daily active
  window). Runtime REST API can disable/enable the purge and change batch
  sizes. No data-node config parameters and no DUMP codes exist for TTL.
- Purge progress is NOT persisted (in-memory diagnostics only; scans always
  start from the infimum).

### 3.4 Insert-over-expired-row conversion (ZINSERT_TTL) — the critical path

Primary replica:

1. User `ZINSERT` reaches DBACC. If the PK element physically EXISTS (expiry
   plays no role in ACC) and the table is TTL and `NoTTLDupConvert` is not set:
   ACC converts the op ZINSERT→ZWRITE→ZUPDATE and locks the row at its EXISTING
   rowid (`DbaccMain.cpp:1472-1515`).
2. DBLQH sees op==ZINSERT but ACC says ZUPDATE → sets internal op
   `ZINSERT_TTL` (= 10 = ZINSERT|0x08, kernel_types.h) —
   `DblqhMain.cpp:10230-10253`.
3. DBTUP executes it through `handleUpdateReq` (in-place overwrite, rowid
   preserved) but FIRST re-checks expiry: live row → 630 (duplicate key);
   expired row → overwrite proceeds (`DbtupExecQuery.cpp:3498-3547`).
   Gate is skipped when `m_ttl_owner_check_bypass` (provenance: recovery,
   replication apply, explicit OO_TTL_IGNORE — `Dbtup.hpp:2109-2115`).
4. REDO: the op is logged VERBATIM as ZINSERT_TTL together with the
   overwritten row's real rowid; replay folds it to ZINSERT with
   `m_use_rowid=true` at the logged rowid (`readLogHeader`,
   `DblqhMain.cpp:34435-34453`).

Backup replica — the two structural defects:

- **D1 (rowid-less ZINSERT forwarding).** `m_use_rowid` is set only for
  op==ZINSERT/ZREFRESH (`DblqhMain.cpp:11711`); for ZINSERT_TTL it stays false,
  so `packLqhkeyreqLab` sends a plain **ZINSERT with rowid flag 0**
  (`12448-12450`). A VM_TRACE assert whitelists exactly this shape
  (`12477-12488`), and the receiver whitelists rowid-less ZINSERT from a
  non-TC sender for TTL tables (`9310-9323`). The backup re-resolves by PK
  through its OWN ACC: element found → re-derives ZINSERT_TTL → in-place update
  at the backup's own rowid for that PK; element NOT found → falls through to
  `handleInsertReq` with `m_use_rowid=0` → **`alloc_fix_rec` self-allocates a
  backup-chosen rowid. No error is raised.** With NoOfReplicas>=3, accminupdate
  stores the self-chosen rowid and the middle replica forwards ITS OWN rowid
  onward (`Dblqh.hpp:5556`), amplifying the fork.
- **D2 (TTL ZWRITE forwarding).** Every TTL-table ZWRITE (REPLACE /
  INSERT..ON DUPLICATE KEY) is forwarded to replicas as **ZWRITE** so replicas
  never run checkTTL for it (`continueACCKEYCONF` `DblqhMain.cpp:11568-11619`,
  `packLqhkeyreqLab:12490-12519`). When the primary resolved it to ZUPDATE, no
  rowid is on the wire; a backup that misses the row does ZWRITE→ZINSERT with
  `m_use_rowid=0` → same self-allocation. This channel needs no expired row at
  all — its op population is every upsert on a TTL table.
- Reverse direction (divergence MASKING): a plain ZINSERT that DOES carry a
  rowid, arriving at a backup whose ACC unexpectedly finds the PK, is silently
  dup-converted into an in-place update — the wire rowid is ignored, and the
  630 that a non-TTL table would raise is suppressed (`DbaccMain.cpp:1490-1499`
  has no is-backup guard).

Net effect: on TTL tables, "ACC found/not-found agreement" replaces rowid
agreement as the replica-layout invariant, and disagreement is silently
absorbed in both directions instead of detected.

---

## 4. Normal-operation mechanisms — verdicts

| # | Mechanism | Verdict | Key evidence |
|---|-----------|---------|--------------|
| M1 | Per-replica clock skew on expiry decision | Spurious aborts only; CANNOT produce 899 | Backup re-runs checkTTL with own clock for ZUPDATE/ZINSERT_TTL (`DbtupExecQuery.cpp:3498-3547`, clock at `3016-3018`); all disagreement outcomes are TUPKEYREF→abort (630 at 3530, 626 at 3519/4754); no branch touches rowid allocation differently. `ttl_ignore` only ever OR-ed up on backup (`DblqhMain.cpp:9141-9157,10258-10287`). |
| M2 | Purge delete vs concurrent same-PK insert (rowid-reuse race) | IMPOSSIBLE | Commit travels backup-first (`DbtcMain.cpp:7650-7656`, `DblqhMain.cpp:13237-13243,14129-14145`); rowid frees refcounted with "backups release before primary" (`DblqhMain.cpp:11052-11196`); TTL never bypasses ACC lock-queue serialization (`DbaccMain.cpp:2260-2273,2846-2952`). Invariant free(primary) ⊆ free(backup) holds. |
| M3 | ZINSERT_TTL forwarded rowid-less (defect D1) | POSSIBLE as divergence AMPLIFIER — the main finding | Section 3.4; needs a seed (section 7). Once seeded: deterministic, permanent, silent until 899. |
| M4 | Purge deleting by rowid | IMPOSSIBLE | Takeover delete is PK-keyed (`takeOverScanOp`, `NdbScanOperation.cpp:3130-3155`); ZDELETE never sets m_use_rowid (`DblqhMain.cpp:11711`). Delete-by-rowid exists only in NR-copy/LCP-restore paths. |
| M5 | Varpart/page-release interaction | Benign | Backup releases pages first; `alloc_fix_rowid` recreates pages by explicit frag_page_id (`DbtupFixAlloc.cpp:275-284`). The ZTH_MM_FULL 899 arm only fires downstream of M3. M3 applies unchanged to VAR tables (`alloc_var_row` has the identical rowid-flag dependency, `DbtupExecQuery.cpp:4373` vs `4408`). |
| M6 | TTL ZWRITE forwarding (defect D2) | POSSIBLE — same amplifier, larger op population | Section 3.4. NR-copy already special-cases the shape (`handle_nr_copy` `DblqhMain.cpp:10501-10551`, fix 310f19cbd03). |

Also checked: `is_ttl_table` disagreement windows (inplace ALTER of TTL
seconds; DBLQH-vs-DBTUP view within one node). Cross-node disagreement yields a
spurious 630 abort (safe); within-node DBLQH-says-TTL/DBTUP-says-not makes
ZINSERT_TTL an UNCHECKED overwrite of a live row — a silent-lost-update hazard
(not a rowid/899 issue) worth an independent look.

## 5. Restart/recovery scenarios — verdicts

| # | Scenario | Verdict | Key evidence |
|---|----------|---------|--------------|
| S1 | Copy fragment filters expired rows | DEFENDED | Copy scan forces `m_ttl_ignore=1` (`DblqhMain.cpp:21363-21370`), enforced `ndbrequire` in initCopyTc (`23200-23202`). Starting node materializes ALL expired-unpurged rows at the live node's rowids. |
| S2 | Purge racing copy fragment (boundary row) | DEFENDED | Starting replica inserted DIRECTLY AFTER primary in the update chain (`DblqhMain.cpp:9613-9616`); per-LDM-link FIFO + row lock held until copy signal sent (`9627-9638`). No reorder window, including 3 replicas. |
| S3 | REDO replay of TTL ops | SOUND | ZINSERT_TTL logged verbatim with real rowid; replayed as ZINSERT+required-rowid (`34435-34453`); replay is expiry-blind (`initReqinfoExecSr` `33851-33863`); 899/630 tolerances re-converge because replay is commit-ordered with full images. Prior bugs here already fixed (a2fd6116fb7, ebc630e1f34). |
| S4 | LCP/backup/restore vs TTL | DEFENDED | LCP+backup scans set TTLIgnoreFragFlag (`Backup.cpp:8061-8069`); SUMA sync likewise (`Suma.cpp:3219-3222`); restore inserts set `AccKeyReq::NoTTLDupConvert` (`DblqhMain.cpp:10197`, `DbaccMain.cpp:1481-1499`) preventing LCP row-count mismatch 2352; QRESTORE covered (`DblqhInit.cpp:784,794`); ndb_restore uses set_ttl_ignore (`tools/restore/consumer_restore.cpp:4371`). |
| S5 | Expired-backlog restart memory exhaustion | **POSSIBLE — real, clean failure, NOT 899** | Recovery must materialize the whole unpurged backlog (everything expiry-blind); if it exceeds DataMemory+spare (spare usable until STTOR phase 8, `DbtupGen.cpp:704`), copy fails via `copyLqhKeyRefLab` (`22071-22082`) → COPY_FRAGREF → restart failure (827/2303 family). Capacity guidance item. |
| S6 | Purge-progress persistence mismatch after SR | N/A | No purge state persisted in the kernel; RDRS recomputes every round; deletes idempotent and replica-symmetric. |

Additional notes: N2 — a partial-column TTL ZWRITE that ran as UPDATE on the
primary can run as INSERT on a not-yet-copied starting-node rowid during NR
copy (missing-mandatory-column REF or transient defaults), always healed by the
copy scan still being behind that rowid. N3 — `ttl_only_expired` is not
REDO-logged, so replayed purge deletes count in the committed-changes counter;
benign (transient, symmetric). N4 — unique-index "same-owner" check
(`ttlUniqueIndexSameOwnerCheck`, `DbtupExecQuery.cpp:3278-3372,3590-3608`)
rejects cross-owner reuse of an expired unique-index entry by design because
the purge deletes index entries by key without owner-PK validation; enforced
only on the primary (backup exempt via wire bypass — safe because the primary
rejects before forwarding).

---

## 6. The end-to-end 899 scenario (amplifier, given a seed)

```
0. SEED: backup B lacks row k that primary P holds (see section 7).
   Row k on P is expired-but-unpurged at rowid R.
1. Client INSERTs PK k.
   P: ACC finds k -> dup-convert -> ZINSERT_TTL -> in-place update at R.
      m_use_rowid NOT set (DblqhMain.cpp:11711).
2. P forwards LQHKEYREQ: operation=ZINSERT, rowid flag = 0.
3. B: ACC found==ZFALSE -> plain insert path -> handleInsertReq with
   m_use_rowid=0 -> alloc_fix_rec picks rowid Rb from B's OWN free list.
   Rb is occupied on P by an unrelated live row Y. Transaction COMMITS.
   NO ERROR. Layout invariant occupied(B) ⊆ occupied(P) is now broken;
   B's fragment fixed_elem_count is +1 vs P (P updated, B inserted).
4. Later: DELETE of Y. P frees slot Rb; B deletes Y at Y's own (different)
   rowid. Rb stays occupied on B forever ("leftover rowid").
5. Later: any INSERT of a new PK on P reuses Rb from P's free list and
   forwards ZINSERT + rowid Rb (rowid flag set — normal insert).
6. B: alloc_fix_rowid(Rb) -> slot taken -> DbtupFixAlloc.cpp:294 -> 899
   -> TUPKEYREF -> LQHKEYREF -> transaction aborts. Repeats for that slot
   (and the whole page once it reaches ZTH_MM_FULL, line 319), during
   fully normal operation, no restart in flight.
```

The same fork also arises through D2 (ZWRITE channel) with no expired row
involved. Because step 3 is silent and steps 4-6 are ordinary traffic, the
observable symptom is far removed in time from the cause.

---

## 7. The seed question

Both hunters independently proved the presence-divergence seed is NOT
constructible on the current tree from normal operation or from the audited
restart paths (the lockstep invariants in sections 4-5 hold). The seed class
is exactly the feature's own fixed bug history:

- `310f19cbd03` — TTL silent row-loss during node-restart copy (ignored
  new-key ZWRITE).
- `102151c53e7` — TTL applier row-lookup gap on PK-less tables (silent replica
  divergence via the binlog applier).
- `a2fd6116fb7` — REDO-replay data loss on rows expired during node downtime.
- Related: `50b509a8384` (ZWRITE-forwarding crash on middle replica),
  `37f1c92c75e` (LCP-restore row-count mismatch 2352), `a6519fdcdd4`
  (NR copy-fragment 626→2303), `ca0c841472e` (copy-frag TTL-ignore invariant),
  `4dd86276102` (same-transaction duplicate-PK silent upsert),
  `14d61b47a19`/`813313ddefc`/`1bb22ad0c78` (unique-index "Bug #2" family),
  `423cf8443c7` (stale unique-index entries), `fb67986a382` (deferred ACC lock
  grant dropping ignore_ttl), `00bb1386297` (BLOB part leak),
  `e1609b6c239` (only-expired key-delete inversion).

Implication: any cluster that ever ran with one of the fixed divergence bugs —
or hits a future one — can carry a latent fork that TTL will never surface
except as 899. The fork also survives node restarts of the DIVERGED backup
only partially: an initial NR of the backup rebuilds it from the live node and
heals the fork; REDO-based NR replays the divergent history and can keep it.

---

## 8. Observability and why this hides in production

- 899 → `ndberror.cpp:297` maps to `HA_ERR_LOCK_WAIT_TIMEOUT`, class TR
  (temporary). SQL sees "Lock wait timeout exceeded"; NdbAPI retry loops
  swallow it. A diverged slot produces INTERMITTENT insert timeouts whose
  frequency grows as the fragment's free list cycles through the slot.
- Divergence is detectable AT BIRTH: `Fragrecord::m_fixedElemCount` is
  incremented in `alloc_fix_rec:141` / `alloc_fix_rowid:305` and decremented in
  `free_fix_rec:238`; exposed per node/table/fragment as
  `ndbinfo.memory_per_fragment.fixed_elem_count` (`NdbinfoTables.cpp:402-426`).
  Same fragment showing different fixed_elem_count on the two replicas ⇒ fork.
  (Expired-unpurged rows are physical and counted, so the comparison is exact.)
- `ndb_select_all --rowid` (`tools/select_all.cpp`) dumps ROWID/FRAGMENT per
  row for offline layout diffs.
- Logging (now always-on in production on this branch, see section 2;
  historically compile-time `DEBUG_899_ERROR` at `DbtupFixAlloc.cpp:37`) prints
  `"(inst)899 error FREE|FULL: tab(t,f) row(page,idx)"`; companion
  `DEBUG_ELEM_COUNT` traces element counts. Kernel+API TTL tracing:
  `TTL_DEBUG` / `TTL_RONDB_TRACE` (`ndb_global.h:283`).
- ERROR_INSERT 4019 (`DbtupExecQuery.cpp:4389-4393`) forces 899 on every
  rowid-carrying insert — a guaranteed-positive control for any detector.
- Eventual hard failures a fork can trip: `ndbabort()` in alloc_fix_rowid
  default arm (`DbtupFixAlloc.cpp:325`), free_record ndbrequires (`251`),
  COPY_FRAG 626→2303 starting-node shutdown (`DblqhMain.cpp:23192`),
  REDO `progError "You have found a bug!"` (`31656-31667`).

---

## 9. Test plan

### 9.1 Test A — deterministic amplifier repro (recommended first)

Goal: prove end-to-end that ONE missing row on a TTL-table backup becomes a
permanent silent fork ending in 899; pin the class with a regression test.

Requires one small kernel change: a NEW test-only ERROR_INSERT (suggest a free
DBLQH/DBTUP code near the copy-fragment apply path) that, on the STARTING node
during copy fragment, silently drops exactly ONE copied row for a TTL table
(consume the copy op, confirm it upstream, apply nothing) — re-creating the
310f19cbd03 shape under test control. Notes: dropping must keep the node
internally consistent (skip both ACC and TUP insert for that op), and only the
copy path may be affected.

Flow (ndbapi/NDBT under ATRT, or mtr with `$NDB_MGM -e "<node> ERROR <code>"`;
2 data nodes, NoOfReplicas=2):

1. Create TTL table (`COMMENT='NDB_TABLE=TTL=300@ttl_col'`) + ordered index
   named `ttl_index`; disable the purge (REST `PUT /ttl-purge/config
   {"enabled":false}` if RDRS is running, or simply run without RDRS).
2. Load N rows with `ttl_col = NOW() - INTERVAL 10 MINUTE` (pre-expired) into
   a single fragment (small N, e.g. 200 — keep the page population small so
   free-list reuse cycles fast).
3. Restart node B (`restartOneDbNode(nostart)` → arm error insert →
   `startNodes` → `waitClusterStarted`): B now lacks row k.
4. Checkpoint: assert `ndbinfo.memory_per_fragment.fixed_elem_count` EQUAL
   across nodes for the fragment (it will already differ by 1 here — this is
   the "divergence at birth" assertion; expected FAIL confirms the seed took).
5. INSERT PK k (plain insert; on P it dup-converts to ZINSERT_TTL, on B it
   self-allocates). Transaction must SUCCEED — that success is itself the
   defect demonstration. fixed_elem_count divergence persists.
6. Churn: loop { DELETE a batch of PKs; INSERT new PKs } on the fragment,
   trapping NdbError code == 899 explicitly (do NOT use generic retry
   helpers). Expected: 899 within a small number of cycles.
7. Control run: same test without the seed error insert must stay clean;
   separate control with ERROR 4019 validates the 899 trap itself.

Build with `-DDEBUG_899_ERROR` in debug builds to log the exact tab/frag/row.

### 9.2 Test B — no-code-change probes for the defended windows

These hunt for a LIVE seed; expected result is "no divergence" (they are
falsification tests for the section 4/5 analysis):

1. **Copy-fragment × purge stress**: halt copy mid-fragment (HALT_COPY_FRAG
   machinery, `DblqhMain.cpp:23303-23510`; EI 5714 logs per-fragment copy
   start/complete at `21552/22109`; EI 5043 kills the starting node exactly at
   copy completion), purge running with `ttl_purge_ctrl` lag=0 and small
   batches; resume; then fixed_elem_count assert + 899-hunt churn. Highest
   value with NoOfReplicas=3 (middle replica forwards self-chosen rowids).
2. **SR replay churn**: heavy insert-over-expired + purge between LCPs; system
   restart with EI 5020 (force REDO pages from disk, `DblqhMain.cpp:32680`) to
   widen replay; afterwards compare per-replica layouts
   (`ndb_select_all --rowid`) and fixed_elem_count; churn for 899.
3. **Clock-skew aborts** (autotest only; needs container/libfaketime — no
   in-tree clock hook exists): one data node's clock stepped behind; hammer
   INSERT-over-expired and UPDATE/DELETE at the expiry boundary
   (`ttl_col = NOW() - ttl + δ`). Expected: spurious 630/626 aborts ONLY.
   Any layout change falsifies the serialized-ttl_ignore analysis.
4. **Expired-backlog restart capacity** (S5): disable purge, fill a TTL table
   toward the DataMemory limit with expired rows, restart a node. Expected:
   clean 827/2303-family failure, never 899. Documents the capacity hazard.

### 9.3 Test C — reusable invariant for the existing suite

Add to `mysql-test/suite/ndb_ttl_purge/` (which today has NO restart tests and
NO rowid assertions) an include that asserts per-fragment
`fixed_elem_count` equality across data nodes, and call it at the end of every
test in the suite. Pure SQL against `ndbinfo.memory_per_fragment`; catches any
future silent fork the moment a test creates one.

Existing suite facts a new test can reuse: `include/ttl_purge_setup.inc`
(REST curl vars), `ttl_purge_create_table.inc`, `ttl_purge_insert_expired.inc`
(pre-expired rows), `ttl_purge_wait_rows_purged.inc` (poll purge metrics),
2 ndbd + 2 mysqld + 1 RDRS cluster via `[rdrs.1.1]` in the suite `my.cnf`;
a second RDRS via per-test `.cnf` (see `ttl_purge_multi_node_sharding.cnf`).
An NDBT program can instead BE its own purge worker (clone of
`ttl_purge.cpp:1852-2009` using `SF_OnlyExpiredScan`, `LM_Exclusive`,
`setTTLPurgeWindowSize`, `deleteCurrentTuple`; flags `OO_TTL_IGNORE 0x8000`,
`OO_TTL_ONLY_EXPIRED 0x40000`, `SO_TTL_IGNORE 0x100`, `SO_TTL_ONLY_EXPIRED
0x200`) — full single-actor control, no RDRS dependency.

---

## 10. Proposed hardening (removes the bug class)

Forward the rowid with the two rowid-less channels and VERIFY on the backup:

- Set `m_use_rowid` for `ZINSERT_TTL` (and for TTL ZWRITE ops the primary
  resolved to ZUPDATE, carry the updated row's rowid) in
  `DblqhMain.cpp:11711` / `packLqhkeyreqLab`.
- Backup semantics: ACC found ⇒ require the found rowid == wire rowid, else
  REF (new error or 899); ACC not-found ⇒ insert at the wire rowid via
  `alloc_fix_rowid` (which 899s on conflict) instead of `alloc_fix_rec`.
- Effect: any presence-divergence seed fails LOUDLY at the point of
  divergence — restoring for TTL tables the tripwire semantics normal tables
  have always had. Cost: rowid words in LQHKEYREQ for these ops.
- Compatibility: needs a version gate for rolling upgrades (older backups
  ignore/reject the extra rowid section); the `handle_nr_copy` special cases
  (`DblqhMain.cpp:10501-10551`) must be revisited to accept the now-carried
  rowid.
- Interim cheap mitigation: a periodic fixed_elem_count cross-replica check
  (ndbinfo query or a watchdog in ndb_mgmd/RDRS) alerting on divergence.

---

## 11. Open items checklist for continuation

- [ ] Decide error-insert number + exact placement for the Test A seed
      (copy-fragment apply path on starting node, TTL tables only).
- [ ] Implement Test A (NDBT program or mtr test), incl. 899 trap and
      fixed_elem_count assertions; validate detector with EI 4019 control.
- [ ] Add Test C invariant include to ndb_ttl_purge suite.
- [ ] Run Test B probes 1-2 on debug build with DEBUG_899_ERROR; NoOfReplicas=3
      variant of probe 1.
- [ ] Schedule clock-skew probe (B3) in autotest with libfaketime.
- [ ] Write capacity guidance for S5 (expired backlog vs DataMemory spare;
      relates to MinFreePct=5% spare usable only until STTOR phase 8).
- [ ] Evaluate/implement the section 10 hardening (rowid forwarding for
      ZINSERT_TTL + TTL ZWRITE) with upgrade version gate.
- [ ] Independent look at the within-node is_ttl mismatch lost-update hazard
      (section 4, last paragraph) — not a rowid issue but found during audit.

## 12. Quick reference — key code anchors

| What | Where |
|------|-------|
| 899 raise sites | `dbtup/DbtupFixAlloc.cpp:294` (slot taken), `:319` (page full); `ZROWID_ALLOCATED` `dbtup/Dbtup.hpp:217` |
| 899 API mapping (temporary!) | `ndbapi/ndberror.cpp:297` |
| Primary rowid choice / free | `alloc_fix_rec` `DbtupFixAlloc.cpp:97`; `free_fix_rec` `:231`; dealloc order rule `dblqh/DblqhMain.cpp:11052` |
| ZINSERT_TTL derivation | `dblqh/DblqhMain.cpp:10230-10253`; ACC dup-convert `dbacc/DbaccMain.cpp:1472-1515` |
| Rowid-less forwarding (D1) | `DblqhMain.cpp:11711`, `12448-12450`, whitelist `12477-12488`, receiver `9310-9323` |
| ZWRITE forwarding (D2) | `DblqhMain.cpp:11568-11619`, `12490-12519`; NR-copy special case `10501-10551` |
| Backup expiry re-check (aborts only) | `dbtup/DbtupExecQuery.cpp:3498-3547`; clock `3016-3018` |
| Purge serialized bits | `DblqhMain.cpp:12452-12465` (ttl_ignore), `12467-12475` (only_expired) |
| Purge worker | `rest-server2/server/src/ttl_purge.cpp:1852-2009`; ctrl tables `mysql.ttl_purge_ctrl`, `mysql.ttl_purge_nodes` |
| Recovery expiry-blindness | copy `DblqhMain.cpp:21363-21370,23200`; REDO `33851-33863`; LCP/backup `backup/Backup.cpp:8061-8069`; restore NoTTLDupConvert `DblqhMain.cpp:10197`, `DbaccMain.cpp:1481-1499` |
| REDO 899/630 tolerance | `DblqhMain.cpp:31629-31637`; ZINSERT_TTL replay `34435-34453` |
| Divergence detector | `ndbinfo.memory_per_fragment.fixed_elem_count` (`kernel/vm/NdbinfoTables.cpp:402-426`); counts maintained `DbtupFixAlloc.cpp:141,238,305` |
| Debug/logging | `DEBUG_899_ERROR` `DbtupFixAlloc.cpp:37`; `TTL_DEBUG` `ndb_global.h:283`; EI 4019 forces 899 `DbtupExecQuery.cpp:4389-4393` |
