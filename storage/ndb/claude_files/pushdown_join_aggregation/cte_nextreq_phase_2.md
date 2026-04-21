# Phase 2 — SCAN_NEXTREQ flow control for `CTE_SCAN` root

## Goal

Replace the back-to-back DBSPJ→DBLQH continuation pattern (for
main-SELECT `CTE_SCAN` roots) with the standard
`SCAN_FRAGCONF`/`SCAN_NEXTREQ` cycle, so the API drives the pace at
which CTE rows are delivered and can apply back-pressure.

Depends on Phase 1 (correct `scanIterI` plumbing).

## Implementation

### 2.1 — Batch size from `SCAN_FRAGREQ`

In `cte_scan_build` (`DbspjMain.cpp:6722`) replace
`data.m_batchSize = 256` with a read of `batch_size_rows` from the
originating `SCAN_FRAGREQ`. Reuse the `ctx.m_start_signal` pattern at
`DbspjMain.cpp:1952-1954`. Keep 256 as an upper cap for safety.
`QN_CteScanParameters` does not need a new wire-format field.

### 2.2 — `execSCAN_NEXTREQ` handler

Add `Dbspj::cte_scan_execSCAN_NEXTREQ` (mirror
`scanFrag_execSCAN_NEXTREQ`). Wire into `g_CteScanOpInfo` at
`DbspjMain.cpp:6666` (replace the `0`).

Body:
- ndbassert `data.m_outstanding == 0`, `data.m_endOfData == false`,
  `treeNodePtr.p->m_state == TN_ACTIVE`.
- Reset per-batch counters: `data.m_rowsReceived = 0`,
  `data.m_rowsExpecting = 0`.
- For every slot with `!m_endOfData`, call `cte_scan_sendReq` (from
  Phase 1) with the stashed `scanIterI`.
- Bump `requestPtr.p->m_outstanding` by the number of REQs sent (see
  2.5).
- Clear `requestPtr.p->m_completed_tree_nodes` bit for this node.

### 2.3 — Cursor-list registration

`execSCAN_NEXTREQ` dispatches through `m_cursor_nodes`
(`DbspjMain.cpp:4653-4663`). `cte_scan_start` must `addFirst(treeNodePtr)`
to `m_cursor_nodes` (see `registerActiveCursor` at
`DbspjMain.cpp:3734-3750`). `cte_scan_cleanup` must remove it. Without
this, `execSCAN_NEXTREQ` silently does nothing.

Sanity check: add a `DEB_CTE` trace to confirm the dispatch hits
`cte_scan_execSCAN_NEXTREQ` the first time SCAN_NEXTREQ arrives.

### 2.4 — Rewire `execCTE_SCAN_CONF`

Remove the back-to-back continuation block at
`DbspjMain.cpp:7102-7151`. Replace with:

- On `EndOfData == false`:
  - Slot update from Phase 1.
  - Decrement `data.m_outstanding`.
  - Leave `m_state = TN_ACTIVE`; do **not** decrement `m_cnt_active`.
  - Call `checkBatchComplete(signal, requestPtr)`. Once all per-node
    REQs for this API batch have answered, `sendConf` at
    `DbspjMain.cpp:3752-3854` fires with `fragmentCompleted=0` and the
    activeMask bit set (`m_cteId == RNIL` filter at 3789/3810 leaves
    the main root alone).
- On `EndOfData == true`: keep the existing completion path at
  `DbspjMain.cpp:7167-7178` — TN_INACTIVE, decrement
  `m_cnt_active`, set `m_completed_tree_nodes`, eventually final
  `SCAN_FRAGCONF(fragmentCompleted=1)`.

### 2.5 — Outstanding accounting

`cte_scan_start` currently bumps `requestPtr.p->m_outstanding` by 1 for
the whole scan (`DbspjMain.cpp:6814`). For per-batch
`checkBatchComplete` to fire at every batch boundary (not only at final
completion), the accounting must match the scanFrag model:

- Bump `requestPtr.p->m_outstanding` per CTE_SCAN_REQ sent
  (`cte_scan_sendReq` is the right place — it already does
  `data.m_outstanding++`).
- Decrement per CONF in `execCTE_SCAN_CONF`.

Remove the single-unit `requestPtr.m_outstanding++` at
`DbspjMain.cpp:6814` and the corresponding final decrement at
`DbspjMain.cpp:7170`.

### 2.6 — Preserve the agg-feed back-to-back path

Case `data.m_joinAggStateKey != RNIL` — CTE-reads-CTE feed — routes
scanned groups directly to a downstream interpreter via
`cteScanAggFeed`, with no API round-trip. This case never needs
SCAN_NEXTREQ. Options:

- Keep a stripped-down back-to-back send inside `execCTE_SCAN_CONF`
  gated on `data.m_joinAggStateKey != RNIL`, or
- Assert `ndbrequire(endOfData || data.m_joinAggStateKey == RNIL)` so
  the new SCAN_NEXTREQ path is the only multi-batch flow for
  API-facing scans.

Recommended: keep the gated back-to-back send (path still in use by
nested CTEs today) and add the `ndbrequire` for the API-facing branch.

### 2.7 — Close / abort

If the API closes mid-scan (via
`ScanFragNextReq::getCloseFlag`) DBLQH holds a `CteScanIterState` pool
record that must be released. Options:

- Add `CteScanReq::CloseFlag` (one bit on an existing field or a new
  word — pick one that does not bump the length used by the first
  REQ), and a DBLQH close handler that calls `releaseCteScanIterState`.
- Or send a synthetic `CTE_SCAN_REQ(batchSize=0, scanIterI=X)` that
  DBLQH treats as close.

Recommended: explicit `CloseFlag`, because a zero-batch REQ collides
with EndOfData semantics if a real batch ever ends with no rows.

Add a close path in `cte_scan_abort` / `cte_scan_cleanup` that fires
one close REQ per slot with `scanIterI != RNIL && !m_endOfData`.

## Risk sanity checks (do early)

1. **Cursor-list dispatch.** Before rewiring anything else, confirm a
   single `DEB_CTE` line from `cte_scan_execSCAN_NEXTREQ` fires on the
   first API SCAN_NEXTREQ. If nothing fires, the cursor-list
   registration (2.3) is wrong.
2. **activeMask bit.** Enable `printSCAN_FRAGCONF` tracing and confirm
   the CTE_SCAN root's bit is set in `activeMask` at intermediate
   batches and cleared at the final batch.
3. **CTE state lifetime** (also tracked in Phase 3): while in the
   SCAN_NEXTREQ pause window, the source CTE's `JoinAggregationState`
   must not be released. If it is, the next CTE_SCAN_REQ returns
   `ZJOIN_AGG_STATE_NOT_FOUND` at `DblqhMain.cpp:20292-20296`.

## Acceptance

- Phase-4 Scenario-1 test (`testCteNdbApiNextReq`) produces the
  correct row count with `setBatchSize(100)` on ~1000 groups and shows
  ≥ 8 SCAN_NEXTREQ round-trips (via `NdbQueryOperation`'s internal
  counters or by observation of `GSN_SCAN_NEXTREQ` in trace logs).
- All Phase-1 and pre-existing CTE tests still pass.
- `testOuterJoinAggNdbApi`, `testMultiOuterJoinAggNdbApi`,
  `testJoinAggNdbApi` regression.
- No `CteScanIterState` pool leak on API-initiated close (verified by
  pool-usage watermark in debug build).

## Out of scope for Phase 2

- Real-table main scan + `CTE_LOOKUP` child scenario — expected to work
  without code changes; Phase 3 verifies, Phase 4 tests.
- Correlated `CTE_SCAN` child — not supported today.
