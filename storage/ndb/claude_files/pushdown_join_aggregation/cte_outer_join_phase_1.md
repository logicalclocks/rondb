# Phase 1 — CTE_LOOKUP outer-join child (API + agg-feed)

## Goal

When a CTE_LOOKUP child is built with outer-join semantics and DBLQH
replies `CteLookupRef(GROUP_NOT_FOUND)` for a parent row, make sure the
parent row survives the join.

## What actually ships in this phase

### API-direct outer join (main SELECT, lookupCte)

**No behavior change needed.** During implementation I verified by
testing that the API's `NdbResultStream::prepareResultSet` already
auto-fills NULL for parents whose correlation had no matching child
TRANSID_AI, as long as:

- `lookupCte` is built without `setMatchType(MatchNonNull)` —
  `NdbQueryCteLookupOperationDef` extends `NdbQueryLookupOperationDef`,
  so `isOuterJoin()` on the result stream returns true.
- DBSPJ's `execCTE_LOOKUP_REF` keeps the outstanding-counters
  consistent — matching `lookup_execLQHKEYREF` for scan-root regular
  outer joins, which also does not emit a synthetic NULL row.

Test 10 in `testCteNdbApiFilter.cpp` (the scan-root + `lookupCte`
LEFT JOIN case with filter-reject forcing REFs) now returns 5 rows
with no further code change.

### CTE_LOOKUP_REF wire — correlation echo

`CteLookupRef` gains a `correlation` field; DBLQH echoes
`req.correlation` back in every REF. This plumbs the parent-row
correlation through the REF path so Phase 3's CTE_SCAN-as-child
sweep can locate the unmatched parent in buffered-row storage.

`CteLookupRef::SignalLength` goes from 3 to 4. Pre-release branch, no
wire-compat constraint.

### DBSPJ REF handler

`execCTE_LOOKUP_REF` now documents the three modes:
- Inner join: silently drop (existing behavior).
- Outer join + direct-to-API: no emit — API auto-fills NULL.
- Outer join + agg-feed (`T_AGGREGATE_LEAF`): NULL-row propagation
  via `sendJoinAggNullRow` is **not yet wired** from this REF path.
  It requires `T_BUFFER_ANY` on the scan ancestor and the
  completion-time sweep pattern used by
  `handleAggAncestorComplete`. Deferred to when Test 5 in
  `testCteNdbApiOuterJoin.cpp` exercises it.

Debug trace in the handler now prints innerJoin / aggFeed / corr so
the three modes are distinguishable in traces.

## Files touched

| File | Change |
|---|---|
| `storage/ndb/include/kernel/signaldata/CteLookup.hpp` | Add `Uint32 correlation` to `CteLookupRef`; SignalLength 3→4 |
| `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp` | `sendCteLookupRef` signature gains `Uint32 correlation` |
| `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` | 14 call sites pass `req.correlation`; helper writes the field |
| `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` | `execCTE_LOOKUP_REF` comment/debug trace updated; behavior unchanged |

## Deferred to a later phase

- Outer-join CTE_LOOKUP inside a CTE subtree (`T_AGGREGATE_LEAF` path):
  needs `T_BUFFER_ANY` on the scan ancestor and NULL-row injection via
  `sendJoinAggNullRow`. Will land when Phase 4's Test 5 needs it.

## Canary

`testCteNdbApiFilter::testCteLookupFilterLeftJoin` (Test 10) returns
5 rows. Confirmed passing with this change.

## Build / run (user runs)

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd testCteNdbApiFilter
./runtime_output_directory/testCteNdbApiFilter -c <cs> -m 3306 -v
```
