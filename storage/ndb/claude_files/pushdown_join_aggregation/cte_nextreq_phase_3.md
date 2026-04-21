# Phase 3 — CTE state lifetime audit across `SCAN_NEXTREQ`

## Goal

Verify that state referenced by an in-flight main-SELECT survives the
API round-trip during a SCAN_NEXTREQ pause, for both the CTE_SCAN root
shape and the real-table + CTE_LOOKUP-child shape. This phase is
primarily findings; concrete fixes (if any) land in Phase 2's close
path or in a follow-up.

Depends on Phase 1.

## Checks

### 3.1 `JoinAggregationState` lifetime

The CTE hash table is held by `JoinAggregationState` in the DblqhProxy
pool. It must stay in `CTE_READY` until the entire main query
completes. Trace:

- Allocation site(s) — `JOIN_AGG_SETUP_REQ` handler in `DblqhProxy.cpp`.
- `CTE_READY` transition site — `handleCtePhaseComplete` (or similar)
  after redistribution.
- Release sites — search for `releaseJoinAggState` /
  `c_joinAggStatePool.release` in
  `storage/ndb/src/kernel/blocks/dblqh/`.

Confirm: release happens only on `JOIN_AGG_RELEASE_REQ` (signalled by
DBTC at whole-request completion), not on intermediate
`SCAN_FRAGCONF`. If an earlier trigger exists (e.g.
`m_cteScansComplete == numCtes` combined with some other predicate),
document and schedule a fix.

### 3.2 `CteScanIterState` pool record lifetime

A partially-iterated CTE scan holds a `CteScanIterState` pool record
(see `Dblqh.hpp:5160-5180`). It is released in `cteScanEmitResults` at
`DblqhMain.cpp:20239` when `EndOfData == true`. Confirm:

- Record remains valid across the `SCAN_FRAGCONF` → `SCAN_NEXTREQ`
  window (nothing else touches the pool during the pause).
- The record is cleaned up on API abort / node failure via the close
  path added in Phase 2.7.
- `cinBufOverflow` buffers are freed on both the normal release path
  and the close path.

### 3.3 Real-table root + `CTE_LOOKUP` child

Walk through: normal `scanFrag` path iterates rows, for each row sends
`CTE_LOOKUP_REQ` to DBLQH, receives `CTE_LOOKUP_CONF` with linked
attrs, forwards the combined row to the API. CTE_LOOKUP is
synchronous per parent row — no state persists between parent rows.

Confirm during a SCAN_NEXTREQ pause:

- No `CTE_LOOKUP_REQ` can be in flight (scan is paused at a row
  boundary).
- Target `JoinAggregationState` is not released (same invariant as
  3.1).
- No TreeNode-level transient state needs preservation.

Expected outcome: this shape works without code changes. Phase 4 adds
a multi-batch NDB-API test that proves it.

### 3.4 Nested / chained CTEs with main-SELECT output

`WITH cte1 AS (...), cte2 AS (SELECT FROM cte1) SELECT FROM cte2` —
`cte1` and `cte2` are both held in `JoinAggregationState` records.
During the main-SELECT batch pause, both must stay in `CTE_READY`.
The main-SELECT itself is a `CTE_SCAN` over `cte2`; the Phase 2 path
covers its iteration.

Confirm via review that `cte1`'s state is not released when its
direct consumer (`cte2`'s materialization scan) completes — only at
the final `JOIN_AGG_RELEASE_REQ`.

## Deliverable

A short findings summary committed to this file (or split into a
follow-up note). For each of 3.1–3.4, one of:

- `OK — verified at <file:line>.`
- `Fix required — see phase_2.md §X / follow-up task.`

No code changes expected. If 3.1 or 3.4 uncover an early-release bug,
open a dedicated follow-up task rather than folding into Phase 2.

## Acceptance

- Written audit added to this file.
- No uncovered lifetime invariants remain unstated.
- If any fixes are required, they are tracked as named tasks.
