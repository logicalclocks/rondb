# Phase 2 — scanCte as LEFT-side parent of outer join (verification)

## Goal

Confirm that when `scanCte(cteId)` is the root (or ancestor) and a
regular `readTuple` / `scanTable` child is attached with outer-join
semantics (no `setMatchType(MatchNonNull)`), NULL-row synthesis for
the child works out of the box.

Rationale: CTE_SCAN rows flow through DBSPJ the same way as regular
scanFrag rows — they end up in the shared row-buffer pool via
`cte_scan_emit` → TRANSID_AI bookkeeping — and the child-side
outer-join machinery (`lookup_parent_row`, NULL-row sweep, etc.) does
not care whether the parent was a real scan or a CTE scan.

## Expected outcome

No code changes. This phase is mostly test-driven verification.

## Work

1. Add tests under Phase 4's `testCteNdbApiOuterJoin.cpp` that exercise
   the shape:

   - `scanCte(0) -> readTuple(t, key=linkedValue(cteScan, col))` with
     no `MatchType` on the child. CTE has some groups whose linked
     key does not exist in `t`. Expect every CTE group to appear in
     the result; groups without `t` match carry NULL `t` columns.

   - Multi-batch variant: ensure `setBatchSize(small)` on the root
     still preserves NULL-row emission across batch boundaries.

2. If a test fails, investigate. Likely places to check:
   - `cte_scan_emit` — does it register emitted rows into the
     per-ancestor buffered-rows pool needed by the child's
     `parent_batch_complete` sweep?
   - `cte_scan_execSCAN_NEXTREQ` — does continuation preserve the
     child's outer-join match state across batches?

3. Document findings; if no code change, close the phase with a
   "status: OK, verified by tests N–M" note.

## Risks

- CTE_SCAN multi-node case (`m_cteScanAllNodes`) may need special
  handling if match tracking is per-node. Confirm during testing.
- SCAN_NEXTREQ boundaries: unmatched CTE parents must survive the
  pause; match-bitmask lives on buffered parent rows, which are
  retained across `SCAN_NEXTREQ` already (per Phase 3 of the
  SCAN_NEXTREQ plan).
