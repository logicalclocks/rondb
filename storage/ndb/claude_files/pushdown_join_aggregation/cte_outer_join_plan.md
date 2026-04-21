# CTE outer-join support — overview + phase index

## Context

The RONDB-1050 branch has CTE materialization working, CTE_LOOKUP
plumbed as an inner-join child, and SCAN_NEXTREQ flow control wired
up for main-SELECT CTEs. The final missing feature: **CTE operations
cannot participate in outer joins**.

Today `execCTE_LOOKUP_REF` silently discards parent rows on
`GROUP_NOT_FOUND`, per the explicit comment at
`DbspjMain.cpp:6431`:

> For left outer joins, we would send a NULL row to API (not yet
> implemented — CTE lookups are currently inner join only).

An aspirational test already exists in-tree and fails today:
`testCteNdbApiFilter.cpp::testCteLookupFilterLeftJoin` (Test 10)
asserts 5 main rows when 2 parents have filter-rejected CTE lookups.
That test is the Phase 1 canary.

The NDB API side is already outer-join-aware — `lookupCte` / `scanCte`
extend from regular lookup/scan, and `NdbQueryOptions::setMatchType`
(`MatchAll` default, `MatchNonNull` = inner) maps to the wire flag
`DABits::NI_INNER_JOIN` → DBSPJ `TreeNode::T_INNER_JOIN`. The CTE
builders in DBSPJ just don't honor that bit yet.

## In scope (shipped)

1. CTE_LOOKUP as outer-join child — main SELECT (API auto-fills NULL
   via `NdbResultStream::prepareResultSet`). Phase 1.
2. scanCte as LEFT-side parent of outer join — driving regular
   `readTuple`/`scanTable` children. Phase 2 (uncovered one
   pre-existing `cte_scan_build` bug along the way).
3. NDB-API test binary `testCteNdbApiOuterJoin.cpp`, integrated in
   the `ndb_push_agg` MTR suite. Phase 4.

## Out of scope / dropped

- **CTE_SCAN as outer-join child** (Phase 3) — dropped. The SQL shape
  is a cross-join with NULL-fill-if-empty, uncommon in practice, and
  requires substantial new DBSPJ machinery (`cte_scan_parent_row` +
  per-parent tracking + match-bit sweep). See
  `cte_outer_join_phase_3.md`.
- **Agg-feed NULL injection** for CTE_LOOKUP returning
  GROUP_NOT_FOUND inside an enclosing CTE's aggregation subtree —
  deferred; see `next_steps.md`.
- `MatchNullOnly` (anti-join), `MatchFirst`.

## Phase index

| Phase | Doc | Scope | Status |
|---|---|---|---|
| 1 | `cte_outer_join_phase_1.md` | CTE_LOOKUP outer-join child (API-direct path) | Shipped |
| 2 | `cte_outer_join_phase_2.md` | scanCte as outer-join LEFT-side parent | Shipped (plus pre-existing T_ONE_SHOT bug fix) |
| 3 | `cte_outer_join_phase_3.md` | CTE_SCAN as outer-join child | Dropped |
| 4 | `cte_outer_join_phase_4.md` | `testCteNdbApiOuterJoin.cpp` consolidated tests | Shipped |

## Key files

- `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` — `TreeNode::T_INNER_JOIN`
  (already exists), `CteLookupData` / `CteScanData` structs.
- `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` —
  `cte_lookup_build`, `cte_lookup_send`, `execCTE_LOOKUP_CONF/REF`,
  `sendJoinAggNullRow`, `g_CteScanOpInfo`.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` —
  `execCTE_LOOKUP_REQ` (echo `correlation` in REF if signal extended).
- `storage/ndb/include/kernel/signaldata/CteLookup.hpp` —
  likely extend `CteLookupRef::SignalLength` with `correlation`.
- `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` (new),
  `storage/ndb/block_unit_test/CMakeLists.txt` (register).

## Reused machinery

- `sendJoinAggNullRow` (`DbspjMain.cpp:8529+`) — existing outer-join
  agg-feed NULL injection.
- `T_BUFFER_MATCH` + `RowPtr::m_matched` — regular outer-join match
  tracking pattern.
- `T_INNER_JOIN` bit / `NI_INNER_JOIN` wire flag — already defined.

## Verification (per phase)

User runs builds/tests; agent hands off commands:

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd
make -j$(sysctl -n hw.ncpu) testCteNdbApi testCteNdbApiFilter \
     testCteNdbApiOuterJoin testJoinAggNdbApi
./runtime_output_directory/testCteNdbApi           -c <cs> -m 3306 -v
./runtime_output_directory/testCteNdbApiFilter     -c <cs> -m 3306 -v
./runtime_output_directory/testCteNdbApiOuterJoin  -c <cs> -m 3306 -v
```

Phase 1 canary: Test 10 in `testCteNdbApiFilter` returns 5 rows.
