# Phase 3 — CTE state lifetime audit across `SCAN_NEXTREQ`

## Goal

Verify that state referenced by an in-flight main-SELECT survives the
API round-trip during a SCAN_NEXTREQ pause, for both the CTE_SCAN root
shape and the real-table + CTE_LOOKUP-child shape. Mostly findings;
any concrete fix is applied alongside the audit.

## 3.1 `JoinAggregationState` lifetime

**Status: OK.**

- Allocation: `execJOIN_AGG_SETUP_REQ` in `DblqhProxy.cpp` seizes from
  `c_joinAggStatePool`. Initial state is `SETUP_COMPLETE` /
  `MATERIALIZING` depending on mode.
- `CTE_READY` transition: the state machine moves forward after
  redistribution completes — single-node fast path at
  `DblqhMain.cpp:18642`, multi-node finalization in `checkCteReady()`
  at `DblqhMain.cpp:20974`. `JOIN_AGG_COMPLETE_CONF` to DBSPJ is sent
  immediately after the transition so DBSPJ only starts `CTE_SCAN_REQ`
  dispatch once `CTE_READY` is observable.
- `CTE_READY` check on every incoming request:
  `DblqhMain.cpp:20300` (CTE_SCAN_REQ) and `DblqhMain.cpp:19504`
  (CTE_LOOKUP_REQ) reject with `ZCTE_LOOKUP_STATE_NOT_READY` if the
  state is anything else.
- Release path: only `execJOIN_AGG_RELEASE_REQ`
  (`DblqhProxy.cpp:2756`) releases the record. DBTC sends
  `JOIN_AGG_RELEASE_REQ` once per query at final teardown — never
  mid-scan, never at intermediate `SCAN_FRAGCONF`.
- The node-failure sweep in `DblqhProxy.cpp::execJOIN_AGG_NODE_FAIL_REP`
  (~line 2859) only triggers release from states
  `NODE_FAIL_ABORT / SETUP_COMPLETE / COMPLETED / ERROR /
  WAITING_SEND_CONF`. **`CTE_READY` is deliberately not in that set**,
  so the state cannot be ripped out from under a paused main-SELECT
  even if the transaction coordinator fails during the pause.

No path releases or transitions `JoinAggregationState` between an
intermediate `SCAN_FRAGCONF(fragmentCompleted=0)` and the next
`SCAN_NEXTREQ`. Safe.

## 3.2 `CteScanIterState` pool record lifetime

**Status: Fix applied.** Two latent leaks in error paths were found
and fixed as part of this phase.

- Seize sites: `cteScanAggFeed` (at `DblqhMain.cpp:20372` — agg-feed
  CONTINUEB survival) and `cteScanEmitResults` (at
  `DblqhMain.cpp:20252` — main-SELECT multi-batch emit, the
  Phase-2-critical path).
- Normal release: `DblqhMain.cpp:20240` on `EndOfData=1` CONF.
- Close-path release (Phase 2.7): top of `execCTE_SCAN_REQ`
  (`DblqhMain.cpp:20303`) — triggered by `CteScanReq::CloseFlag`.
- Overflow buffer cleanup: `releaseCteScanIterState`
  (`DblqhMain.cpp:19673`) frees any `cinBufOverflow` via
  `lc_ndbd_pool_free` before releasing the pool record. Also
  no-ops on `RNIL`, so it's safe to call on first-batch entries.
- **Leaks (fixed)**: two `CTE_FILTER_ERROR` early returns in
  `cteScanEmitResults` (`DblqhMain.cpp:20043` per-group gate and
  `:20157` scalar gate) called `sendCteScanRef` + `return` without
  releasing the `scanIterI` pool record the function was holding
  from a prior continuation. This phase adds the missing
  `releaseCteScanIterState(scanIterI)` before each early return.
- SCAN_NEXTREQ pause window: the record sits in the `TransientPool`
  between CONF send and the next REQ. `TransientPool`'s idle-shrink
  only touches free-list entries, not in-use records, so no race.
- First-batch seize-failure path at `DblqhMain.cpp:19984-19988` was
  already leak-free (no pool record had been seized yet).

## 3.3 Real-table main-SELECT root + `CTE_LOOKUP` child

**Status: OK — no changes required.**

- `CTE_LOOKUP_REQ` is synchronous per parent row: the parent `scanFrag`
  iterates rows, fires a lookup per row, receives `CTE_LOOKUP_CONF`
  (with linked attrs), emits the joined row, moves on. All lookups for
  the just-emitted batch complete *before* the parent emits
  `SCAN_FRAGCONF`, so no in-flight CTE_LOOKUP state persists across a
  `SCAN_NEXTREQ` pause.
- `CTE_LOOKUP_REQ` handler (`DblqhMain.cpp:19504`) checks
  `state->m_state == CTE_READY` on every lookup. Provided `3.1`'s
  invariant holds (verified above), lookups just keep working after
  `SCAN_NEXTREQ`.
- TreeNode-level state in DBSPJ
  (`DbspjMain.cpp:lookup_parent_row` / `cte_lookup_send`) is populated
  per parent row and consumed before the batch boundary —
  `m_cteLookup_data.m_pendingCount` only increments while the CTE is
  not yet `CTE_READY`, and those queued lookups are drained on the
  `CTE_READY` transition, not on scan resumption. No persistent
  per-parent-row state to preserve.

## 3.4 Nested / chained CTEs with main-SELECT output

**Status: OK.**

- Each CTE in a chain gets its own `JoinAggregationState` record via
  its own `JOIN_AGG_SETUP_REQ`. Independent records, independent
  release.
- Every record transitions to `CTE_READY` at
  `DblqhMain.cpp:20974` when its redistribution completes; subsequent
  consumers (downstream CTE materialization, or the main SELECT)
  observe `CTE_READY` on every request (3.1).
- `execJOIN_AGG_RELEASE_REQ` is driven by DBTC per-query at final
  teardown. `cte1` is *not* released when `cte2`'s materialization
  scan completes — `cte1` stays `CTE_READY` for the rest of the query
  so anything else reading from it (including a main SELECT that
  scans `cte2` which internally referenced `cte1` during
  materialization) sees valid state.
- No per-CTE early release trigger exists; confirmed by reading every
  call site of `c_joinAggStatePool.release` and the proxy's release
  state whitelist.

## Abort / close-flow fix (Phase 2 regression)

Phase 2's initial close path was fire-and-forget (DBLQH released the
pool record and returned silently). Test 18 (`testCteScanRootEarlyClose`)
crashed with `ndbassert(is_complete)` at `DbspjMain.cpp:3448`: after
`cte_scan_abort` sent close REQs, the tree node was still `TN_ACTIVE`
with `m_cnt_active > 0`, so `batchComplete` observed `is_complete=false`
under `RS_ABORTING` and fired the assert.

Fix applied in this same change:

1. DBLQH's close handler
   (`DblqhMain.cpp::execCTE_SCAN_REQ`, ~line 20305) now round-trips:
   it releases the pool record *and* replies with
   `CteScanConf(EndOfData=1, numRows=0, scanIterI=RNIL)` so DBSPJ's
   normal accounting drains.
2. `cte_scan_sendCloseReq` now bumps `data.m_outstanding` and
   `requestPtr.m_outstanding` per close REQ (matching the per-REQ
   model in `cte_scan_sendReq`), so the close CONF drives
   decrement → `checkBatchComplete` → `batchComplete` with
   `is_complete=true`.
3. `cte_scan_abort` picks one of three paths per slot:
   - **Paused between batches** (`data.m_outstanding == 0` AND
     `slot->m_scanIterI != RNIL`): send close REQ now.
   - **In-flight REQ** (`data.m_outstanding > 0`, any slot with
     `!m_endOfData`): mark `slot->m_close_pending = true`. The CONF
     handler fires the close REQ on the CONF's `scanIterI`.
   - **Already finished** (`m_endOfData`): skip.
4. `cte_scan_abort` fall-through: when nothing needed closing and
   `data.m_outstanding == 0`, transition `TN_INACTIVE` directly and
   decrement `m_cnt_active` so `batchComplete` sees `cnt_active == 0`.
5. `execCTE_SCAN_CONF` now reacts to a CONF arriving for a
   `close_pending` slot while `RS_ABORTING` is set — it fires a close
   REQ for the CONF's `scanIterI` (which DBLQH just seized for
   continuation) so that pool record is released.

## Summary

| Check | Status |
|---|---|
| 3.1 `JoinAggregationState` lifetime | **OK** |
| 3.2 `CteScanIterState` lifetime | **Fix applied** — leaks at `DblqhMain.cpp:20043` / `:20157` closed |
| 3.3 Real-root + `CTE_LOOKUP` child | **OK** |
| 3.4 Nested / chained CTEs | **OK** |
| Abort / close-flow (Phase 2 regression) | **Fix applied** — Test 18 crash resolved |

Phase 2's SCAN_NEXTREQ flow control is safe against all four lifetime
invariants. The two concrete bugs surfaced during audit and testing —
`CteScanIterState` leaks on `CTE_FILTER_ERROR` and the
abort/close-flow completion gap — are fixed in the same commit as this
document.
