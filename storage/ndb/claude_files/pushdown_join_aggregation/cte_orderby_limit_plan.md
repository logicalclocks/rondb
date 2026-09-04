# ORDER BY / LIMIT in CTE Bodies (single-owner redistribution + top-N)

**Status: L1 + L2 + block test IMPLEMENTED and green (September 2026,
testCteNdbApiFilter Tests 1-27 in ndb_push_agg + ndb_push_agg_dist);
L4 RonSQL + L5 MTR IMPLEMENTED (pending user build + first --record);
L6 rondb-docs pending.**

L5 outcome notes:
- FOUND + FIXED BY THE SCALE CASE (ronsql_large_cte Q6, 100k groups —
  data-node SIGSEGV): `GBHashTable::iteratorAt` returned iterators with
  `m_prev_link = nullptr` (documented as not supporting eraseAndNext),
  and `finalizeLimitSlice`'s truncation phase resumes mid-bucket via
  iteratorAt then erases — the first RESUMED truncation slice's first
  non-kept entry hit `*m_prev_link = nxt` through NULL.  Invisible at
  block-test scale (5 groups = one slice, no resume); at 100k groups
  phase 2 always spans slices.  Fixed in iteratorAt itself: reconstruct
  m_prev_link by walking the saved bucket's chain (bounded ~size/1024
  per resume; falls back to the old read-only iterator if the raw
  pointer is not found), making every resume site erase-capable; the
  read-only users (CTE-scan resume, AVG finalize, LIMIT select phase)
  are unaffected.  The iteratorAt contract comment now states the real
  requirement: the SAVED entry must still be live on resume — erasing
  other entries is fine.
- FOUND + FIXED ON FIRST RECORD (obc-11): chained aggregates over a
  DECIMAL-widened CTE output lost the D15 display metadata everywhere
  along the chain — `b AS (SELECT k, SUM(s) FROM a ...)` over `a`'s
  `s = SUM(DECIMAL(12,2))` printed `1190598.5` vs MySQL's
  `1190598.50`.  Three layers, all fixed:
  (1) `resolve_chained_column_type`'s AGGREGATE arms explicitly zeroed
  `out_scale`/`out_precision` (they predate D15; the AVG output arm
  already carried) — the Decimal arms now return the source
  scale/precision like `build_cte_virtual_tables`' Decimal arms, and
  the SUM / MIN/MAX / AVG Float/Double arms carry when the argument is
  itself a CteResultColumn Double with 0 < scale <= 30 (level-3+
  chains; AVG applies its scale+4 rule);
  (2) `build_cte_virtual_tables`' SUM / MIN/MAX / AVG Float/Double
  arms got the same guarded carry so chained virt columns hold the
  metadata;
  (3) `build_result_column_metadata`'s CteResultColumn-without-
  dict_column branch (the C8 AVG special case) is generalized to every
  CTE output — a chained aggregate output plumbs no stored dict_column
  either, so `aggregate_arg_scale` found no metadata; the branch now
  resolves via (1) and fills scale/precision whenever rscale > 0
  (rscale == 0 leaves has_metadata false — integer/COUNT/string/
  temporal chains unchanged).  Level-1 outputs keep the dict_column
  plumb-back path (obc-2 byte-identical); real FLOAT/DOUBLE columns,
  including NOT_FIXED_DEC dictionary scales flowing through CTE COLUMN
  outputs, keep compact formatting via the CteResultColumn + scale
  guards.  Audit: no existing recorded baseline chains an aggregate
  over a non-integer CTE output (body_chain_scalar / bigquery
  big-02/03 / cte_multi_batch / cte_chained / name_resolution are all
  integer-typed), so the fix moves no recorded result.
- New family `body_orderby_limit_cte.inc` (obc-1..21 + obc-P1..P7),
  wrapper `ronsql_cte_dd_orderby_limit_cte` ×5 topology suites.
  Default sorted compare (main SELECTs never ORDER); set determinism
  via total-order keys — SUM/AVG(o_totalprice) strictly increasing in
  o_custkey, MIN(o_orderdate) = 1995-01-01 + k days, GB keys unique,
  and obc-5's deliberately tied first key carries a unique trailing
  tie-breaker.  Covers: agg/GB-key/string-GB/AVG/DATE order keys,
  multi-key, EXPLAIN pins (obc-1 line, obc-16 absence, obc-19
  single-row), LEFT-JOIN NULL-extend, i26 watermark over a LIMIT CTE,
  re-aggregation and chained-CTE consumers, LIMIT-without-ORDER-BY
  invariants, LIMIT 0 (INNER + LEFT + single-row comma probe), LIMIT >
  groups, spelling unification, scalar no-op, and the seven rejection
  probes (non-output column, string MIN/MAX key, scalar LIMIT 0, 9
  order cols, 26-bit LIMIT cap, OFFSET + LIMIT x,y syntax).
- `ronsql_cte_dd_orderby_limit_reject` re-authored: obl-1..3 body arms
  retired (pointer echoes), obl-4..7 subquery arms kept (message text
  re-records: "in a subquery", "main SELECT level and in CTE bodies").
- `body_single_row_cte.inc` srb-P6 flipped from rejection to accepted
  no-op (direct exec now records the row) — family re-records ×5.
- `body_orderby_limit.inc` ENVELOPE comment updated (comment-only,
  baseline-safe).
- Scale: `ronsql_large_cte.test` Q6 (100000-group GROUP BY lg_id,
  ORDER BY k DESC LIMIT 100 — sliced candidate walk + 99900 sliced
  erasures on the constant owner) and Q7 (20000-group aggregate-ordered
  SUM DESC + unique tie-breaker LIMIT 50 — the partial-merge rank case
  across node groups; per-cust DECIMAL sums are double-exact so both
  engines order identically).

L4 outcome notes:
- `analyze_ctes`'s Phase-0 blanket rejection replaced by
  `analyze_cte_body_orderby_limit`: LIMIT range check (26-bit kOpLimit
  immediate, max 67108863), ORDER BY column count capped at the kernel
  MAX_ORDER_COLS (8), scalar-body handling (ORDER BY / LIMIT >= 1
  accepted as provable no-ops with nothing emitted; LIMIT 0 rejected —
  the scalar result is not a group record so group-walk truncation
  cannot empty it), then `resolve_cte_body_orderby_aliases` converts
  unqualified ORDER BY names matching a body output to OUTPUT_REF
  BEFORE scope building (the scoped resolver visits TABLE_COLUMN ORDER
  BY entries and would fail on alias names; no m_col_is_alias marking —
  that machinery is main-scope-only, and a converted entry is simply no
  longer referenced from the body scope).  Subquery arms keep
  `reject_ignored_orderby_limit` (message updated to name CTE bodies as
  supported).
- Emit half `emit_cte_orderby_limit` (called between the aggregator
  programming — both the grouped `programAggregator_join` path and the
  single-row `SetSingleRowMode` branch — and `Finalize()`): resolves
  each ORDER BY entry to a body OUTPUT (OUTPUT_REF directly;
  TABLE_COLUMN via `same_resolved_column` against COLUMN outputs, so
  bare/qualified spellings unify), maps COLUMN outputs to their GROUP
  BY list position (single-row: output position — GroupBy registration
  order), AGGREGATE to `aggregate.agg_index`, AVG to `avg.
  agg_index_sum` (the finalize divide runs before the LIMIT select so
  AVG order keys compare as divided doubles); rejects non-output ORDER
  BY columns, subquery-output keys, and string (Char/Varchar/
  Longvarchar) MIN/MAX order keys (kernel comparator does not decode
  val_ptr slots — v1).  Emits OrderBy/Limit trailer words; returns
  true → `defineCte` gains `QN_CteSubtreeNode::CTE_LIMIT` (OR-composed
  with CTE_SINGLE_ROW).  ORDER BY without LIMIT emits nothing (set
  unchanged, derived-table order unspecified in MySQL too) and skips
  the flag so redistribution stays hash-distributed.
- EXPLAIN: `ORDER BY <name|col_N> [DESC][, ...] LIMIT n` line in the
  CTE-definitions section, printed exactly when a trailer is emitted.

L1/L2 outcome notes:
- Wire exactly as planned, zero new signals: `QN_CteSubtreeNode::
  CTE_LIMIT` (0x2) → DBTC → `JoinAggSetupReq::CTE_LIMIT_FLAG`
  (bit 29) → DblqhProxy → `JoinAggregationState::m_cte_limit`
  (strategy-mask validation extended); ORDER spec + limit ride the
  program as `kOpOrderBy` / `kOpLimit` trailer words (no-op exec arms;
  parsed by the Init walk into `m_order_spec[MAX_ORDER_COLS=8]` +
  `m_limit`/`m_has_limit` with range validation).
- Redistribution: the constant-owner arm now fires for
  `m_cte_single_row || m_cte_limit` (same DBTC-node owner).
- Probe routing: DBSPJ's grouped CTE_LOOKUP arm targets the DBTC node
  for limit CTEs (skipping the key hash; the key section stays
  un-normalized like the grouped path — DBLQH normalizes on its side);
  the retired debug ROUTE_FLAG forwarder gained a defensive
  constant-owner guard.  CTE_SCAN needs nothing: non-owner states are
  empty after constant-owner redistribution.
- Finalize: `compareGroupsByOrderSpec` (GB cols via AH-framed key
  entries + GBColTypeInfo::cmpFn with AH NULL flags — NULLs first
  ASC / negated DESC; aggregate slots as typed AggResItem incl. the
  mixed-signedness SUM case and double promotion) +
  `finalizeLimitSlice` two-phase sliced walk (bounded worst-at-root
  candidate heap, then bsearch-membership truncation via eraseAndNext
  + freeGroupData with string-slot freeing; candidate array from
  lc_ndbd_pool_malloc, freed in a new ~JoinAggInterpreter override so
  an aborted chain cannot leak it).  Hooked in checkCteReady AFTER the
  AVG divide via `ZCONTINUE_CTE_LIMIT_FINALIZE` (53) +
  `continueCteLimitFinalize` (alloc failure aborts the CTE with
  ZJOIN_AGG_STATE_ALLOC_FAILED).  `GBHashTable::dataKeyLen` added so
  the comparator can work from bare data pointers.
- Block test: testCteNdbApiFilter **Test 27** — GROUP BY grp, SUM,
  four sub-cases (SUM DESC LIMIT 2 ⇒ COUNT=3, SUM ASC LIMIT 1 ⇒ 2,
  GB-key DESC LIMIT 1 ⇒ 1, LIMIT 10 > groups ⇒ 5) pinning the
  post-merge rank correctness, GB-key ordering, and the
  limit-covers-all fast path; dropped groups verified via lookupCte
  misses.
- Known v1 caveat (documented): the candidate-array qsort at
  select-completion is a single slice — fine for typical top-N,
  noted for very large LIMIT values.

Lifts the Phase-0 rejection (`ronsql_orderby_limit_plan.md`:
"reject body/subquery ORDER BY+LIMIT") for CTE bodies, per the
maintainer's design: the redistribution phase sends every group to a
SINGLE node — the TC node, the constant-owner precedent shared with
scalar (I.17e) and single-row CTEs — and that node determines the kept
set using the ORDER BY columns, truncating to LIMIT N before the CTE
becomes consumable.

## Semantics

`WITH t AS (SELECT k, SUM(x) AS s FROM ... GROUP BY k ORDER BY s DESC
LIMIT 10) SELECT ...` — the CTE materializes exactly the top-10 groups
under the ORDER BY key.  MySQL defines the *set* (which rows survive),
not the iteration order of the derived table — main-query ORDER BY
handles presentation, and dropped groups are simply absent: CTE_LOOKUP
misses (GROUP_NOT_FOUND) make inner joins drop and LEFT JOINs
NULL-extend, exactly the single-row-CTE empty-case semantics.  LIMIT
without ORDER BY keeps an arbitrary N (allowed, per the maintainer:
ORDER BY is the practical companion but not absolutely necessary).
Ties at the cutoff are arbitrary in MySQL too — test data must use
total-order keys (the ob-family tie-breaker discipline).  OFFSET is
deferred.

## The correctness subtlety (drives the v1/v2 split)

Groups arrive at the owner as PER-SOURCE-NODE PARTIALS that merge on
arrival.  When the ORDER BY key is an AGGREGATE output (the common
`ORDER BY SUM(x) DESC` shape), a group's rank is unknown until every
partial has merged — eager bounded eviction is WRONG (two small
partials can sum into a top-N total after the group was evicted).  So:

- **v1 (this plan)**: keep ALL groups on the owner until the FINAL_REP
  barrier, then select the top-N at finalize — universally correct for
  any ORDER BY key, including AVG outputs (sequenced AFTER the AVG
  divide, which lives at the same hook).
- **v2 (follow-up)**: the maintainer's incremental ordered list with
  bounded eviction is SAFE when ordering solely by GROUP BY columns
  (a group's rank is fixed by its key; re-arrivals of evicted keys
  rank identically below the cutoff and are correctly discarded, and
  arrivals within the top-N merge into their kept entry).  That is the
  O(N)-memory optimization for GB-key-ordered LIMIT; aggregate-ordered
  LIMIT keeps the v1 shape.

The v1 finalize never sorts all groups either: a **bounded top-N
candidate structure** (insert-sorted array or heap of ≤ N group
pointers, compared by the ORDER BY key) is maintained across a
CONTINUEB-sliced walk of the group hash — O(G·log N) total, perfectly
sliceable — followed by a second sliced walk erasing every non-kept
group (`eraseAndNext` + `freeGroupData`).  This realizes "the node
keeps the list in ORDER BY column order" with N-bounded memory for the
ordered structure itself.

## Design

**Wire**: zero new signals.
- `JoinAggSetupReq::CTE_LIMIT_FLAG` (bit 29; bit 30 is
  CTE_SINGLE_ROW_FLAG) rides `defineCte(..., flags)` → DBTC → DBSPJ →
  DblqhProxy, switching the redistribute DESTINATION to the constant
  owner (the DBTC node — the scalar/single-row owner computation) for
  every group, and marking probe routing constant-owner
  (`cte_lookup_send`'s single-row precedent; CTE_SCAN reads via the
  scalar single-node emit precedent).
- The ORDER BY spec + limit value ride the AGGREGATION PROGRAM as a
  self-describing trailer (the kOpAvg pattern):
  `NdbAggregator::OrderBy(kind, idx, direction)` entries (kind =
  GROUP-BY column position or visible aggregate slot; direction
  ASC/DESC) + `NdbAggregator::Limit(n)`.  `JoinAggInterpreter::Init`
  parses them into `m_limit` / `m_order_spec[]`; no signal-format or
  version change (26.10 alpha precedent — no gate).

**Owner-side compare** (`compareGroupsByOrderSpec(a, b)`): GROUP BY
key columns compare via the existing `GBColTypeInfo::cmpFn` (typed,
charset-aware — string GB keys work); aggregate slots compare as typed
`AggResItem` (int64/uint64/double, `is_null` with MySQL NULL ordering:
NULLs first ASC, last DESC; AVG slots are already-divided DOUBLEs
because the AVG finalize runs FIRST).  String MIN/MAX slots (val_ptr
payloads) are decidable at implementation time — include via the
payload + charset if cheap, else clean RonSQL rejection in v1.

**Finalize sequencing** in `checkCteReady` (all owner-side, each
CONTINUEB-sliced, each idempotent):
1. AVG divide (shipped).
2. LIMIT top-N select (bounded candidate walk).
3. LIMIT truncation (erase non-kept groups).
4. CTE_READY + COMPLETE_CONF.
The single-node completion arm routes through checkCteReady since the
AVG work, so both cluster shapes share the sequence.  LIMIT 0
truncates everything (empty CTE — the srb-20 comma-join-empty
semantics).  Scalar / single-row bodies with LIMIT ≥ 1 are no-ops;
LIMIT 0 empties them.

**RonSQL** (`reject_ignored_orderby_limit` at the CTE-body arm
:5146 becomes shape-gated; subquery arms unchanged):
- Accept ORDER BY over body OUTPUT columns only (GROUP BY columns and
  aggregate outputs, by alias or column name — the resolution
  machinery from the main-level ob work); reject expressions,
  positional refs, and non-output columns.
- Accept LIMIT N (64-bit, 0 allowed); reject OFFSET.
- Emit: OrderBy/Limit trailer on the CTE aggregator +
  `defineCte(CTE_LIMIT_FLAG)`.
- EXPLAIN: `ORDER BY ... LIMIT n` line in the CTE-definitions section.
- Keep rejections: LIMIT in single-row bodies with subset-key
  consumers?  (No — single-row is ≤1 row; LIMIT is harmless.  Only
  gate what the kernel can't do.)

## Interactions / risks

- **Memory**: ALL groups of a LIMIT CTE transit ONE node before
  truncation (maintainer-accepted).  The chunk-allocator budget on the
  owner bears the full group set; the v2 GB-key eager eviction is the
  relief valve.  Watch item alongside the mutex-free lg_* budget note.
- **Redistribute queue growth** on the owner during the longer
  single-target window (existing watch item, amplified).
- **Probe routing** must not hash-route for LIMIT CTEs (flag-gated
  constant-owner, the 4d/single-row seam).
- **Chained CTEs** reading a LIMIT CTE: reads route constant-owner;
  a later CTE grouping over a LIMIT CTE works unchanged.
- **Ties**: nondeterministic kept-set across topologies — MTR uses
  total-order keys; document the MySQL-equivalent arbitrariness.
- **FRAGS_PER_WORKER**: scanCte queries stay K=1 (unchanged).

## Work items

- **L1 kernel**: program-trailer parse (Init: m_limit, m_order_spec);
  CTE_LIMIT_FLAG through SETUP → constant-owner redistribution
  destination; constant-owner probe/scan routing under the flag;
  `compareGroupsByOrderSpec`; sliced top-N select + sliced truncation
  in checkCteReady after the AVG divide; slot-count/teardown audit for
  the freed groups.
- **L2 API**: `NdbAggregator::OrderBy` / `Limit` builders;
  `defineCte` flag plumb (exists for single-row).
- **L3 block tests** (testCteNdbApi/Filter): multi-node ORDER BY
  SUM DESC LIMIT k (the partial-merge rank case — the correctness
  heart), GB-key-ordered LIMIT, LIMIT > group count, LIMIT 0,
  AVG-ordered LIMIT (finalize sequencing), probe-miss semantics of a
  dropped group (inner drop + LEFT NULL-extend).
- **L4 RonSQL**: gate rework + emit + EXPLAIN; message updates.
- **L5 MTR ×5** (`body_orderby_limit_cte.inc` or extend ob-family):
  top-N shapes with total-order keys, re-aggregation over a LIMIT CTE,
  watermark vs a LIMIT CTE output, chained CTE over LIMIT CTE,
  LIMIT-without-ORDER-BY pinned by COUNT only, rejection probes
  (OFFSET, expression ORDER BY, non-output column); `ronsql_large`
  case for the sliced finalize + truncation at 100k-group scale.
- **L6 docs** + `ronsql_orderby_limit_plan.md` Phase-0 note update.
