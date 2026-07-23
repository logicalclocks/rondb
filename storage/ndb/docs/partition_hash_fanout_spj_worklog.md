# RONDB-1074 Phase 5 (DBSPJ interval pruning) — Working Notes

**Status: DEFERRED (2026-07-02).** Phase 5 will be implemented in a future
RonDB version. This document preserves the design and the machinery study for
that work. Companion to `partition_hash_fanout_plan.md`.

Extra findings from the (partial) step-1 verification, done before deferral:

* `computePartitionHash` (DbspjMain ~5581) xfrms only DKey-marked attrs from
  the prune-key section and hashes the result; a base-hash variant iterates
  keyAttr[0..base_count) instead and groups with `(h/z)*z`.
* `getNodes` (~5681) reads `dst.hashInfo[1]` as DIGETNODESREQ::hashValue with
  distr_key_indicator=0 — interval expansion can just set hashInfo[1]=H+i per
  call, no new variant needed.
* `scanFrag_parent_batch_complete` (~6824) discriminates on T_PRUNE_PATTERN
  ONLY: const-prune and non-pruned share the "ranges on first handle, scan all
  handles" branch — const-prune fanout expansion needs NO change there.
* `scanFrag_findFrag` (~6574) is a linear list search by fragId, returns
  DbspjErr::IndexFragNotFound.
* Wire bits: QN_ScanFragNode::SF_* occupy 0x10000..0x80000 (low 16 bits are
  DABits::NI_*, max 0x1000). 0x100000 is free for SF_PRUNE_INTERVAL. Unknown
  SF_* bits are ignored (no mask validation) — the API-side version gate is
  the only protection for old data nodes.

Working document for implementing DBSPJ fanout interval pruning. Delete when
Phase 5 is merged.

## Repo state (as of 2026-07-02)

* Branch `RONDB-1074-2level-hash`, rebased on 25.10-main @ 25.10.15
  (e5ed9c6a482), pushed to origin. Working tree clean.
* Commits on branch: plan doc, fanout implementation, plan update,
  lower-layer validation, interval signal mode (`a389d34465c`), distinct
  partition id scans (`87aa688a40f`).
* Phases 1–4 done. Signal conventions:
  * `ScanTabReq::storedProcId` high half = extended request-info bits
    (guaranteed zero from old senders; senders gate on
    `ndbd_support_partition_hash_fanout()` = 25.10.15+/26.02.6+/26.04.2+/
    >26.04 because old DBTC stores storedProcId unmasked).
  * bit 31 = DistributionKeyIntervalFlag (distributionKey = grouped base
    hash, DBTC scans `fanout` raw hash values H+i via DIH,
    distr_key_indicator=0). DBTC: `m_scan_dist_key_interval_flag`.
  * bit 30 = DistributionKeyPartIdFlag (distributionKey = distinct fragment
    id, distr_key_indicator=1, bounds-checked). DBTC:
    `m_scan_dist_key_part_id_flag`.
  * Error 2203 `ZSCAN_PRUNE_PARTITION_HASH_ERROR`: hash-valued one-partition
    prune rejected on fanout tables.
  * Routing hash composition: `((base_hash/z)*z) + (detail_hash%z)`;
    base = PK columns [0,x), detail = [x,x+y); DBACC keeps full-PK hash.

## Goal of Phase 5

DBSPJ pushed-join child scans (and SPJ root scans) currently do NOT prune on
fanout tables (disabled by bail-outs). Implement: one prune key on a fanout
table expands to `fanout` fragments (grouped base hash H; fragment i resolved
by DIH for raw hash H+i, distr_key_indicator=0), same bounds/params sent to
each.

## Established facts (file:line on current branch)

### DBSPJ metadata
* `Dbspj::TableRecord` has `m_partition_hash_{base_key_count,detail_key_count,
  fanout}` from TC_SCHVERREQ (Dbspj.hpp ~159, DbspjMain execTC_SCHVERREQ ~315).
* KeyDescriptor has partitionHash fields; `Dbspj::handle_partition_hash`
  (DbspjMain ~5460) computes the composed hash but requires FULL key
  (base+detail); used by `computeHash` for key ops. NOT usable for prune keys.
* `Dbspj::computePartitionHash` (~5590) computes the legacy distribution hash
  from a prune-key section (distkey values). Body not fully read yet — need to
  study xfrm/keyPartLen handling to model the new base-hash variant on it.

### Prune machinery
* Two prune modes on TreeNode::m_bits: `T_CONST_PRUNE` (0x800) and
  `T_PRUNE_PATTERN` (0x1000). Set in `parseScanFrag` (~6104–6157):
  pattern built into `data.m_prunePattern` (Local_pattern_store), const key
  expanded at parse into section `data.m_constPrunePtrI`.
* Prepare: `scanFrag_execDIH_SCAN_TAB_CONF` (~6270):
  * T_CONST_PRUNE: `fragCount = 1` (6284–6287), allocates 1 ScanFragHandle,
    `computePartitionHash(constPrunePtrI)` → `getNodes` → sets
    `fragPtr.m_fragId/m_ref` (6346–6373); releases m_constPrunePtrI.
  * T_PRUNE_PATTERN: allocates handles for ALL fragments (fragNo skewed by
    fragNoOffs), `pruned=true` → handles left unresolved (m_ref==0).
  * `fragCount==1` special case converts PRUNE_PATTERN → CONST_PRUNE
    (6374–6396). Note: fanout≥2 implies fragCount≥2 (create enforces
    partition_count % fanout == 0), so no conflict, but keep in mind.
  * Non-pruned: resolves all handles via `scanFrag_sendDihGetNodesReq`
    (~6437): DIGETNODESREQ with `hashValue=fragId, distr_key_indicator=ZTRUE,
    scan_indicator=ZTRUE`, batched by MAX_DIGETNODESREQS with CONTINUEB.
* Per parent row: `scanFrag_parent_row` (~6590):
  * T_PRUNE_PATTERN: `expand(pruneKeyPtrI, m_prunePattern, rowRef)` →
    NULL check (bail) → `computePartitionHash` → `getNodes` →
    `scanFrag_findFrag(list, fragPtr, tmp.fragId)` → updates
    `fragPtr.m_ref/m_next_ref` (6615–6673).
  * Else (const/non-pruned): `list.first(fragPtr)` — ranges stored on FIRST
    handle (6674–6681).
  * Then T_KEYINFO_CONSTRUCTED: expand keyPattern → `scanFrag_fixupBound`
    (embeds correlation id!) → `appendReaderToSection(fragPtr.m_rangePtrI)`,
    `fragPtr.m_rangeCnt++` (6683–6731).
  * Then T_ATTRINFO_CONSTRUCTED: appends param built from
    m_attrParamPattern to `fragPtr.m_paramPtrI` with per-param length word
    (6738–6775).
* Send: `scanFrag_send` two overloads (7081 driver, 7169 worker):
  * `prune` ⇒ each handle uses its OWN `m_rangePtrI`
    (`fragWithRangePtr = fragPtr`, releaseAtSend = !repeatable).
  * non-pruned ⇒ ranges shared from FIRST handle
    (`fragWithRangePtr = list.first`, released only at last frag)
    (7317–7338). keysToSend accounting differs likewise (7104–7118).
  * attrInfo duplicated per send when releaseAtSend or ATTRINFO_CONSTRUCTED
    (7345+). SORTED_ORDER forces bs_rows=1/parallelism=1 (7209).
* Fragment accounting: `m_frags_complete = m_fragCount` at prepare for pruned
  (6397); `scanFrag_parent_batch_complete` (6824–6882) recomputes: for
  pruned, walks handles and counts handles WITHOUT ranges as complete
  (verify exact logic 6844–6879 — partially read); non-pruned resets
  m_frags_complete=0 unless no ranges at all. `m_frags_not_started =
  fragCount - frags_complete` (6879). If all complete → node stays inactive.
* Repeat/NEXTREQ: 7976–8007 (keysToSend recomputed from FIRST handle for
  non-pruned — check pruned path), `scanFrag_send_NEXTREQ` 8203.
  Batch-complete per-frag: 7921ff, 8091ff.
* Release/cleanup: `scanFrag_release`? prune pattern released at ~8567–8578
  (T_PRUNE_PATTERN → pattern.release(); T_CONST_PRUNE → release
  m_constPrunePtrI). Range sections per handle released where? (verify:
  scanFrag_parent_batch_repeat / cleanup ~8478+).
* `getNodes(signal, BuildKeyReq&, tableId)` — verify body: presumably
  DIGETNODESREQ with hashValue=tmp.hashInfo[1], distr_key_indicator=0,
  returns tmp.fragId + tmp.receiverRef. For interval we call it fanout times
  with hashValue = H+i — check how hashInfo is passed so we can inject raw
  hash values (may need a variant taking explicit hash).

### API side (fanout bail-outs to remove in step 6)
* `NdbQueryBuilder.cpp`:
  * `checkPrunable` (~1505): `if (getTable().m_base_partition_fanout > 1)
    return 0;` — root scan prune disabled. Root prune value is sent via
    ScanTabReq::distributionKey (NdbQueryOperation.cpp ~3320 sets
    DistributionKeyFlag + m_pruneHashVal, tSignal length StaticLength+1).
    For fanout: compute grouped base hash when bounds have equality on the
    base-key prefix; set interval flag in storedProcId bit 31 (DBTC side
    ALREADY handles it — same path as ordered index scans). Gate on
    ndbd_support_partition_hash_fanout(getMinDbNodeVersion()).
  * `appendPrunePattern` (~2035): fanout bail — child prune pattern not
    pushed. For fanout: emit prune pattern covering the BASE-KEY prefix only
    (index must have base-key columns as prefix in PK order — same
    restriction as NdbIndexScanOperation::setBound fanout pruning), plus a
    NEW wire flag so DBSPJ knows it is a base-hash interval prune.
* Prune info wire format: QueryNode/param bits in
  `storage/ndb/include/kernel/signaldata/QueryTree.hpp` (DABits::NI_*,
  PI_*). parseScanFrag reads them (~6104). Find spare bit for
  "base-hash interval prune" — not yet located, part of step 1 remains.

## Design decisions

1. **Base-hash helper (task #5)**: new `Dbspj::computeBasePartitionHash`
   (modeled on computePartitionHash) hashing base-key columns
   keyAttr[0..base_count) from a prune-key section; returns GROUPED base
   hash `(h/z)*z`. Must handle charset/varlen via xfrm like the existing
   code.
2. **T_CONST_PRUNE fanout (task #2)**, prepare phase: fragCount = fanout
   (not 1); allocate fanout handles; grouped base hash H from const prune
   key; resolve handle i via DIH for raw hash H+i (distr_key_indicator=0),
   set m_fragId/m_ref per handle. Ranges: parent_row else-branch already
   stores on FIRST handle; scanFrag_send must SHARE first handle's ranges
   across the fanout handles (i.e. behave like non-pruned range sharing).
   Suggested: track "ranges shared" as (T_CONST_PRUNE) regardless of
   fragCount, i.e. change scanFrag_send's `prune` discriminator to
   T_PRUNE_PATTERN-only for per-handle ranges, and let const-prune use the
   shared-first-handle path (also fixes keysToSend accounting 7104–7118 and
   releaseAtSend lifetime). Verify NEXTREQ/repeat paths use the same
   discriminator (7976–8007).
3. **T_PRUNE_PATTERN fanout (task #3)**, per parent row: compute grouped H
   once; loop i in [0,fanout): getNodes(H+i) → findFrag → append the SAME
   expanded range + param to EACH target handle. Factor the range/param
   append (6683–6775) into a helper taking fragPtr; expand key/param ONCE
   and append copies (sections are appended per handle; the expanded key
   section is released after appends). Careful: scanFrag_fixupBound embeds
   correlation id — same for all copies (same parent row) → fine.
   Overlapping intervals from different parent rows: append accumulates
   per handle — already the model for same-fragment collisions today.
4. **Safety gate (task #6)**: DBSPJ `parseScanFrag`: if table fanout > 1 and
   prune info present WITHOUT the new base-hash-interval wire flag → drop
   prune info (scan all fragments; correct) — protects against
   fanout-unaware clients. With flag → set a new TreeNode bit (e.g.
   T_INTERVAL_PRUNE alongside T_CONST_PRUNE/T_PRUNE_PATTERN) or a
   ScanFragData field from TableRecord fanout.
5. **QueryBuilder (task #4)**: emit base-key prune + wire flag (children);
   root scans via grouped hash + storedProcId interval flag. All gated on
   ndbd_support_partition_hash_fanout(). Prune requires equality on base-key
   prefix only (not all distkeys).
6. SPJ knows fanout from its own TableRecord (m_primaryTableId for
   index scans → primary table's fanout; note treeNode m_tableOrIndexId vs
   m_primaryTableId distinction: hash uses primary table).

## Remaining step-1 verifications (task #1, in progress)

* Read `computePartitionHash` + `getNodes` bodies (~5590+) — how hashInfo is
  produced/consumed; how to inject raw hash H+i cleanly.
* `scanFrag_parent_batch_complete` exact pruned-handle logic (6844–6879):
  confirm handles without ranges are counted complete (interval expansion
  then works without changes there).
* Repeat (T_SCAN_REPEATABLE) + NEXTREQ paths for pruned scans with multiple
  active handles (7976–8007, 8203+): range lifetime, keysToSend.
* Cleanup paths ~8478–8600: per-handle m_rangePtrI/m_paramPtrI release,
  m_prunePattern/m_constPrunePtrI release — no leaks/double free with
  fanout handles.
* `scanFrag_findFrag` implementation (list search by fragId).
* QueryTree.hpp DABits: locate prune bits + find a spare flag bit for the
  base-hash interval prune; how parseScanFrag decides CONST vs PATTERN
  (paramDA vs treeDA).
* Whether SPJ ever receives prune info for the ROOT scan node (root is
  pruned by DBTC fragment lists — confirm parseScanFrag prune only applies
  to child scans / getOpNo()!=0, see appendPrunePattern early return for
  root).

## Task list mapping (session task ids)

1. Study machinery (in_progress) — finish verifications above.
2. T_CONST_PRUNE interval expansion (blocked by 1,5).
3. T_PRUNE_PATTERN interval expansion (blocked by 1,5).
4. Re-enable QueryBuilder prune for fanout (blocked by 2,3,6).
5. SPJ grouped base-hash helper.
6. SPJ safety gate / wire flag.
7. Plan doc + test checklist update (blocked by 4).

## Testing (after implementation; user runs builds/tests)

* Pushed join cases from plan doc "DBSPJ Tests": parent lookup/scan driving
  child scan by base key; multiple parent rows → same/overlapping/different
  intervals; child with extra ordered bounds; pushed vs non-pushed result
  equality; no section leaks (testSpj / testScanFilter style; mtr
  ndb_join_pushdown variants).
* SPJ error inserts 17060–17131 cover OutOfSectionMemory paths touched here.
