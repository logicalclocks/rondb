# Phase 1 — CTE_LOOKUP outer-join child (API + agg-feed)

## Goal

When a CTE_LOOKUP child is built with outer-join semantics and DBLQH
replies `CteLookupRef(GROUP_NOT_FOUND)` for a parent row, DBSPJ must
emit a NULL-padded row instead of silently dropping the parent.

Same code path handles both endpoints — the branch is on whether the
CTE_LOOKUP feeds an aggregator (CTE subtree case) or emits to the API
(main SELECT case). The discriminator is
`treeNodePtr.p->m_cteLookup_data.m_joinAggStateKey != RNIL`.

## Design

### DBSPJ build-time wiring

1. **Honor `DABits::NI_INNER_JOIN`** in `cte_lookup_build`
   (`DbspjMain.cpp:5741`). Mirror the existing `lookup_build` pattern
   that sets `TreeNode::T_INNER_JOIN` from the wire flag. Without this,
   every CTE_LOOKUP acts as if `MatchNonNull` was set.

### DBSPJ runtime NULL-row path

2. **Rewrite `execCTE_LOOKUP_REF`** (`DbspjMain.cpp:6435-6485`).
   After the outstanding-counter bookkeeping:

   ```cpp
   const bool groupNotFound =
       (ref->errorCode == CteLookupRef::GROUP_NOT_FOUND);
   const bool isOuterJoin =
       (treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN) == 0;

   if (groupNotFound && isOuterJoin) {
     if (treeNodePtr.p->m_cteLookup_data.m_joinAggStateKey != RNIL) {
       sendCteLookupAggNullRow(signal, requestPtr, treeNodePtr, ref);
     } else {
       sendCteLookupApiNullRow(signal, requestPtr, treeNodePtr, ref);
     }
     // fallthrough to completion checks
   } else if (!groupNotFound) {
     abort(signal, requestPtr, ref->errorCode);  // real error
     return;
   }
   // else: inner join + group missing → skip (legacy behavior)

   maybeResumeCongestedNodes(signal, requestPtr, treeNodePtr);
   checkBatchComplete(signal, requestPtr);
   ```

3. **New `sendCteLookupApiNullRow`** — builds a TRANSID_AI of NULL
   values for all virtual-table columns of the CTE, routes via
   `m_cteLookup_data.m_api_resultRef` / `m_api_resultData`. Copies
   the parent correlation from the REF (step 5). Implementation
   pattern to mirror: existing NULL-row synthesis in `lookup_build`
   family — grep for `AttributeHeader::setNULL` within `DbspjMain.cpp`.

4. **New `sendCteLookupAggNullRow`** — thin wrapper that constructs
   the `parentRowRef` expected by `sendJoinAggNullRow(signal,
   requestPtr, treeNodePtr, parentRowRef)` (`DbspjMain.cpp:8529+`).
   The existing helper handles both `T_AGGREGATE_LEAF` direct
   injection and the `T_AGGREGATE_ANCESTOR` deferred path.

### Correlation preservation across REF

5. **Extend `CteLookupRef`** with a `correlation` field (`Uint32`), and
   bump `CteLookupRef::SignalLength`. Pre-release branch, wire-compat
   is not a concern.
   - `DblqhMain.cpp::execCTE_LOOKUP_REQ`: echo `req->correlation` into
     the REF when `sendCteLookupRef` is called with `GROUP_NOT_FOUND`,
     `FILTER_*`, or any error path that preserves the REQ body.
   - Header: `storage/ndb/include/kernel/signaldata/CteLookup.hpp`.

### DBLQH side — no behavior change

The only change is echoing `correlation` in REF. The decision of
inner vs. outer join is entirely DBSPJ-side.

## Risks

- **`cte_lookup_start` pre-READY branch** (`DbspjMain.cpp:5926`):
  calls `cte_lookup_send` directly when CTE is already READY.
  NULL-row path must cover this too. Verify the REF handler is
  reached regardless of materialization timing.
- **Filter-reject path**: DBLQH maps filter-reject to
  `GROUP_NOT_FOUND` (`DblqhMain.cpp:19626-19643`). Phase 1 treats
  both "group truly missing" and "filter rejected" identically —
  both produce NULL in outer-join mode. Matches SQL `LEFT JOIN … ON
  cte_filter_cond` semantics.
- **Leaf-vs-non-leaf CTE_LOOKUP**: non-leaf lookups bump
  `requestPtr.p->m_rows`; NULL-row emit must mirror that.

## Test canary

`testCteNdbApiFilter.cpp::testCteLookupFilterLeftJoin` (Test 10) exists
in-tree and asserts `rowCount == 5`. Today it fails (returns 3 — the
2 filter-rejected parents are silently dropped). After Phase 1 it
must pass.

Pre-existing inner-join tests (Tests 1-9, 11-15) must stay green —
the `T_INNER_JOIN` branch preserves current behavior.

## Build / run (user runs)

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd
make -j$(sysctl -n hw.ncpu) testCteNdbApiFilter
./runtime_output_directory/testCteNdbApiFilter -c <cs> -m 3306 -v
```
