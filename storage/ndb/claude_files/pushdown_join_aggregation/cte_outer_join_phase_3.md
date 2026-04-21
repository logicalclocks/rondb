# Phase 3 — CTE_SCAN as outer-join child

## Goal

Support `scanTable(t) -> scanCte(0)` with LEFT JOIN semantics:
for every parent `t` row, drive a CTE_SCAN; if the CTE yields zero
matching rows for that parent, emit `t` cols + NULL CTE cols.

## Why this is bigger than Phase 1

CTE_SCAN today has `g_CteScanOpInfo.parent_row == NULL`
(`DbspjMain.cpp:6666`) — CTE_SCAN is a **root-only** op. Attaching it
as a non-root child requires a parent_row handler plus per-parent
scan-state tracking.

## Design outline (refined after Phase 1 lands)

### Build-time

- In `cte_scan_build`: honor `T_INNER_JOIN`. If this CTE_SCAN has a
  non-RNIL parent, set `T_BUFFER_MATCH` on the ancestor scan so its
  rows retain a match-bit bitmask.

### Runtime — new `cte_scan_parent_row`

- Called once per parent row.
- Stash parent correlation into a new per-parent slot in
  `CteScanData` (extend the existing `NodeSlot` array or add a
  parallel table keyed by parent correlation).
- Send a CTE_SCAN_REQ using the existing `cte_scan_sendReq` helper.
  The REQ already supports per-sender-data routing; each parent's
  scan gets its own `scanIterI` lifecycle.

### Runtime — match tracking

- On first TRANSID_AI (or first CONF with `numRows > 0`) for a
  parent, set the parent's match-bit in the ancestor's
  `RowPtr::m_matched`.
- Reuse the existing pattern in `lookup_execTRANSID_AI` /
  `scanFrag_execTRANSID_AI` for match-bit setting.

### Runtime — batch-completion sweep

- Hook into `parent_batch_complete` (via the existing OpInfo
  callback) on the parent node.
- Iterate buffered parent rows whose match-bit is clear for this
  CTE_SCAN node.
- For each unmatched parent:
  - API case: emit NULL TRANSID_AI (reuse Phase 1's
    `sendCteLookupApiNullRow` with correlation substituted).
  - Agg-feed case: call `sendJoinAggNullRow` (reuse Phase 1's
    `sendCteLookupAggNullRow`).

### Signal

- No CTE_SCAN wire change expected. CTE_SCAN_CONF already carries
  `senderData` (matches `TreeNode` pointer), and per-parent
  correlation is stashed DBSPJ-side.

## Alternatives considered

1. **Drive CTE_SCAN once per parent row vs. once per parent batch**:
   per-parent-row is cleaner (each parent's CTE_SCAN is an independent
   `scanIterI` lifecycle), and CTE data is already hash-resident so
   cost is bounded. Going per-batch would require filtering
   per-parent inside CTE_SCAN, which it doesn't support.

2. **Bypass parent_row, use a single CTE_SCAN per parent batch with
   a WHERE filter pushing parent keys into DBLQH**: substantial
   new wire shape; rejected.

## Risks

- CTE_SCAN per-parent pool pressure: N parents × per-parent scan
  state. Cap / flow-control via the existing `MildlyCongestedLimit`
  / `HighlyCongestedLimit` mechanism.
- Multi-node CTE_SCAN (`m_cteScanAllNodes`): per-parent and
  per-node cross product may need new accounting.
- Abort / close: reuse the existing `cte_scan_abort` + close-REQ
  round-trip logic; extend to cover per-parent slots.

## Tests (moved to Phase 4)

Phase 4 bundles all test coverage; Phase 3 adds the tests for this
specific shape (Tests 7-9 in Phase 4).
