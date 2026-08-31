# Kernel-native single-row CTE materialization

**Status: Commit 1 (K1-K3 core + block tests 23-26) SHIPPED
(2026-08-31, testCteNdbApi 1-26 green; the single-node ndb_push_agg
cluster exercises the fast path — the multi-node owner-shipping
redistribute gets its coverage from the Commit 3 topology suites).
Commit 2 (subset-key CTE_LOOKUP + block tests 27-31) SHIPPED
(2026-08-31, testCteNdbApi 1-31 green after two first-run findings:
the API param serializer's unmasked packed numResultCols word — error
20005 — and the pre-existing empty-intermediate-projection API
starvation, fixed in the tests + named as a follow-up).  Commit 3
(RonSQL + MTR) in progress.**

## Why kernel-native

Direction from review: do NOT rewrite non-aggregating CTE bodies into
aggregate queries at the RonSQL front end (that approach was
implemented and rolled back) — adapt the data nodes to materialize
single-row CTEs natively, so general non-aggregate CTE bodies remain
reachable later without a semantic rewrite in the way.

The kernel was unusually well prepared:

- An unfinished `CTE_SINGLE_ROW` skeleton existed end-to-end:
  `defineCte(..., flags)` → `QN_CteSubtreeNode::CTE_SINGLE_ROW`
  (QueryTree.hpp:476) → KeyInfo CTE-def word 5 → `CteInfo::m_flags` →
  DBSPJ `CteContext::m_flags` (+ write-only `m_singleNodeId`).  DBTC
  set the state up on ONE node and skipped redistribution — which
  could never work: body rows found on other nodes had no state to
  feed, and skipping the COMPLETE flow left the state stuck before
  CTE_READY.  This plan keeps the flag plumbing and replaces the
  placement semantics.
- Zero aggregate slots are kernel-clean: no kernel assertion requires
  `n_agg_results > 0`; group records collapse to key-only
  (`allocGroupData(keyLen + 0)`); `emitCteGroupOutput`,
  `buildCteLinkedBuffer`, `mergeOneGroup`, `mergeFrom` and teardown
  all degrade to zero iterations.  The single blocker was the API-side
  `NdbAggregator::Finalize()` `kErrEmptyAggResult` check.
- The scalar-CTE (I.17e) machinery is the placement precedent: owner =
  the DBTC-co-located node (`refToNode(m_senderRef)`), the
  empty-section dummy-word workaround, and DBSPJ's scalar lookup
  routing to the DBTC node.

## Design

`WITH r AS (SELECT cols FROM t WHERE pk = const)` (RonSQL-guaranteed
≤ 1 row) materializes as:

- **Representation**: every projected column is a GROUP BY column,
  ZERO aggregate slots.  The row is stored as a key-only group record
  — `[AttrHeader][data]` per column in projection order, the exact
  format the emit walks, linked-buffer key walk, and redistribute wire
  already handle.  GB type metadata (ColumnMeta section 2 →
  `initGBTypesFromMetadata`) gives typed, charset-aware compare for
  every column.  Dedup-on-insert is harmless at ≤ 1 row.
- **Placement**: states on ALL nodes; feed stays node-local and
  untouched; the normal merge → redistribute → FINAL_REP →
  `checkCteReady` flow runs, but a single-row state's redistribute
  owner function is CONSTANT — `refToNode(m_senderRef)` (the DBTC
  node) — instead of the key hash.  A subset-key CTE_LOOKUP cannot
  recompute a full-row hash, so the row must live at a key-independent
  location every consumer can route to.
- **Lookup semantics** (commit 2): a CTE_LOOKUP against a single-row
  state binds ANY SUBSET of the projected columns (including none):
  empty state ⇒ MISS (`GROUP_NOT_FOUND`; the scalar always-emit arm
  must NOT apply — empty CTE means INNER drops / LEFT NULL-extends,
  like MySQL); row present ⇒ per-bound-column typed compare (NULL on
  either side ⇒ miss); zero keys ⇒ existence probe (the comma-join /
  watermark consumer, with exact empty semantics for free).
- **CTE_SCAN**: mechanics unchanged — DBSPJ request k scans node k's
  state (requests with `m_rootFragId >= numDataNodes` skip), so the
  owner emits the row exactly once and other nodes' empty states emit
  nothing.

## Commit 1 — implemented

- **NdbAggregator** (`NdbAggregator.hpp/.cpp`): `SetSingleRowMode()` +
  `single_row_mode()`; `Finalize()` accepts `n_agg_results_ == 0` in
  the mode (and rejects the mode with aggregate slots or zero GB
  columns).  GB-only programs already pass the empty-program gate
  (GB descriptor words advance `curr_prog_pos_`); header word 1
  `(n_gb_cols << 16) | 0` is representable end-to-end.
- **No version gate**: 26.04.0/26.04.1 were alpha releases with no
  upgrade support, so every deployable data node understands the new
  mode — no `ndbd_support_*` predicate needed (maintainer decision).
- **JoinAggSetupReq**: new `CTE_SINGLE_ROW_FLAG = 0x40000000` in
  `concurrencyStrategy` (bit 31 = CTE_MODE_FLAG; decoders mask both
  out of the strategy compare).
- **DBTC** (`DbtcMain.cpp`): single-row CTEs SETUP on all nodes (the
  one-node break removed) with the flag ORed in; the
  `sendCteCompleteReqsForPhase` skip removed so merge → redistribute →
  CTE_READY runs.  `CTE_SCAN_ALL_NODES` logic untouched (correct in
  both coverage modes).
- **DblqhProxy**: decodes the flag into new
  `JoinAggregationState::m_cte_single_row` (explicitly initialized on
  every SETUP — pool seize runs no constructors).
- **DBLQH** (`DblqhMain.cpp`): `continueJoinAggRedistribute` uses the
  constant DBTC-node owner for single-row states and sends a
  dummy-word value section when `valueLen == 0` (the scalar
  precedent; the receiver consumes `req->valueLen` and ignores the
  word).  Defensive single-row contract checks (API-controlled input
  ⇒ clean query failure per the tiered policy):
  `ZCTE_SINGLE_ROW_VIOLATION` (1272, Dblqh.hpp; 1270 is DBTC's
  batch-protocol error, 1271 reserved for LOCAL mode) at THREE sites:
  `abortCteRedistribution` when >1 group is found locally at
  redistribute entry; on the owner in `checkCteReady` before the
  CTE_READY transition (cross-node case: several nodes each shipped
  one row; inbound merges are complete by then — REQs precede
  FINAL_REP on the same signal path and merge directly in
  CTE_REDISTRIBUTING); and in `continueJoinAggMerge`'s single-node
  CTE tail (`m_cte_num_nodes <= 1` goes straight to CTE_READY,
  bypassing both other checks — Test 25's first run on the 1-node
  ndb_push_agg cluster returned 5 rows through exactly this hole).
- **Block tests** (`testCteNdbApi.cpp` Tests 23-26 + new one-row
  `cte_srow_src` / empty `cte_srow_empty` / `cte_virtual_srow`
  tables): 23 materialize + scanCte root emits the row; 24 empty body
  ⇒ 0 rows, clean completion; 25 violating body (5 distinct rows) ⇒
  clean error, 0 rows; 26 single-row CTE feeds a main aggregator
  through the CTE_SCAN agg feed (zero-agg-slot
  `buildCteLinkedBuffer`).

## Commit 2 — subset-key CTE_LOOKUP (implemented)

- **Wire**: bound-column positions ride `QN_CteLookupNode` — count in
  bits 16-23 of the `numResultCols` word
  (`NUM_RESULT_COLS_MASK`/`KEY_POSITIONS_SHIFT`/`KEY_POSITIONS_MASK`/
  `MaxKeyPositions = 16`), one word per key operand appended directly
  after the per-column virt-type block; 0 = not declared (the
  grouped/scalar virt-PK contract, wire image unchanged).
- **API**: `NdbQueryOptions::setCteKeyColumns(positions, count)`
  (options-impl fields + copy-ctor); `lookupCte` with positions
  declared validates exactly `count` keys and binds key i to the
  projected column `positions[i]` (typed via `bindOperand`) instead of
  the virt-PK loop — including the ZERO-key existence-probe form
  (`keys = {nullptr}` + `setParent`; the base lookup-def ctor's
  `assert(i > 0)` relaxed).  Serializer emits count + positions.
- **DBSPJ**: `cte_lookup_build` decodes/validates the positions into
  `CteLookupData::m_keyPositions` (numResultCols masked at every
  consumer — INCLUDING the API's param-side reader in
  `prepareAttrInfo`'s QN_CTE_LOOKUP case, which reads the count back
  from the SERIALIZED node to build the PI_ATTR_LIST projection: the
  first test run left it unmasked, so 2|(1<<16) columns exploded the
  list and wrapped the 16-bit param length — param-stream desync ⇒
  DBSPJ error 20005 on Tests 27/28).  `cte_lookup_send`: `CteContext::m_flags &
  CTE_SINGLE_ROW` ⇒ route to the constant `refToNode(m_senderRef)`
  owner (no key hash), stamp each key AttributeHeader with its TRUE
  column position and `writeToSection` the stamped buffer back (the
  grouped path only ever stamped a hash-local copy — DBLQH
  re-normalized independently, which the single-row arm must not);
  keyless probes send a 1-dummy-word key section with
  `req->keyLen = 0`.  Defensive `InvalidRequest` on positions against
  a non-single-row CTE and on a position/entry count mismatch.
  Write-only `CteContext::m_singleNodeId` retired.
- **DBLQH**: `cteLookupReqImpl` skips the sequential 0..k-1 key
  normalization for single-row states and probes via the new
  file-local `singleRowCteProbe`: empty map ⇒ MISS
  (`GROUP_NOT_FOUND` — the scalar always-emit arm is bypassed);
  `keyLen == 0` ⇒ existence HIT; else per-entry compare at the
  stamped position against the stored record (charset columns via
  `gb_types[pos].cmpFn`, others data-size + byte equality; NULL on
  EITHER side ⇒ MISS — SQL equality, deliberately not findInBucket's
  NULL==NULL group identity); malformed keys ⇒ REF
  `ZCTE_LOOKUP_ATTRINFO_MALFORMED`.  On a hit, `req.keyLen` is
  overridden to the STORED key length (the scalar override-to-0
  precedent) since `buildCteLinkedBuffer` / the accumulators pointer /
  `runCteFilter` all read it as the stored length.
- **Block tests 27-31**: subset key binding ONLY position 1 (the case
  sequential normalization would get wrong), position-0 value miss,
  zero-key existence probe hit, zero-key probe against an empty body
  (the empty-CTE MISS drops the parent — exact MySQL semantics),
  two-column subset hit.  First run surfaced a PRE-EXISTING API sharp
  edge (not a kernel defect — the probes themselves behaved exactly
  right in the ndbd log): an intermediate scan op with NO getValue
  projection produces no TRANSID_AI while DBSPJ/TC's completed-ops
  accounting still announces its rows, so the API waits forever
  (tests hung with the kernel fully completed).  Tests now project on
  the parent op too (the Test 2 convention); a defensive fix
  (suppress counting or reject empty intermediate projections on the
  pushed path) is a named follow-up.

## Commit 3 — RonSQL + MTR (planned)

No AST rewrite: parse-time candidacy (no aggregates / GROUP BY /
HAVING, single stored table, WHERE present, plain-column outputs, no
duplicate col_idx) marks the CTE single-row; plan-time enforcement in
`build_cte_scopes` requires full PK-equality with plain constants;
`analyze_ctes`' aggregate requirement gets a single-row exemption;
`cte_key_coverage` gains a SingleRowSubset arm (any subset incl. empty
⇒ comma joins admitted alongside scalar CTEs); emit programs
GroupBy/GroupByLinked for every output + `SetSingleRowMode` +
`defineCte(..., CTE_SINGLE_ROW)`; consumer `lookupCte` passes
bound-column positions; virt table all-columns with the existing
attr-size switch; EXPLAIN `[single-row]` annotation.  MTR
`body_single_row_cte.inc` (srb-*) ×5 topology suites incl. the
comma-join / watermark cases and pure-projection cross-join row-absent
(exact empty semantics — no guard tricks needed), LEFT JOIN
NULL-extension, multi-col PK, VARCHAR/DATE/nullable outputs,
subset-key joins, rejection probes.  gc-P3 / cs-probe-5 /
`ronsql_cte_scalar` Test 5 baselines stay byte-identical.

## Deferred (multi-row future — the design keeps these reachable)

- General non-aggregating CTEs: per-group multiplicity counter +
  emit-time expansion (dedup seam mapped at the `find`-before-`insert`
  in `ProcessRec`), or per-key row chains; hash distribution by a
  DECLARED key subset; multi-row CTE_LOOKUP replies (row-shaped
  precedent: `CTE_LOOKUP_OUTER_CHAIN_FLAG`).
- UK-equality single-row bodies; multi-table fully-keyed bodies;
  readTuple-rooted body access path (JOIN_AGG_SETUP over the lookup
  protocol); rondb-docs update (PR #104 documents "CTE bodies must
  aggregate").
