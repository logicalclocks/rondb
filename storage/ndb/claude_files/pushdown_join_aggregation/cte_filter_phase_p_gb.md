# CTE Filter Phase P-GB — verify parent-table GROUP BY over CTE agg leaf

## Status: DONE (2026-04-27, no kernel changes required)

Tests 13–14 in `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` (INNER
JOIN + LEFT JOIN, both `GROUP BY c.c_region` over a `sums` CTE agg leaf)
pass on first run. **Phase P-GB was incidentally fixed by Phase E.1K's
writer-side rework** — DBSPJ now writes a uniform 2-word
`[tableId][schemaVersion]` (real-table) or `[encodeWord0][encodeWord1]`
(CTE virt-col) prefix for every linked entry, and the reader
(`JoinAggInterpreter::initGBTypes`) already discriminates the two via
the marker bit (`CteLinkedAttr::isCteMarker`).

The deliverable is the two new MTR tests, which exercise the
previously-uncovered shape and lock in regression protection.

The remainder of the plan below is preserved as historical context.

---

## Context

Earlier branch state (Phase B.1 era, `cte_filter_phase_b.md`) flagged a
buffer-format mismatch: DBLQH `buildCteLinkedBuffer` was said to copy
parent linked projections into Step 1 of its linked-attr buffer in raw
LQHKEYREQ subroutine format (`[AttrHeader][data]`), with no 2-word
`[tableId][schemaVersion]` prefix, while Steps 2/3 (CTE GB keys + agg
results) emitted entries WITH the prefix. Reader
`JoinAggInterpreter::initGBTypes` walked the buffer assuming uniform
prefixed format → SEGV on any GROUP BY of a parent-table column over a
CTE agg leaf.

That description pre-dates Phase E.1K (`cte_filter_phase_e1k.md`).
Reading the current code, the mismatch no longer exists:

- `Dbspj::cte_lookup_send` at `DbspjMain.cpp:6353` calls
  `expand(..., true /* addTableMeta */)` when building the
  CTE_LOOKUP_REQ AttrInfo with parent linked columns.
- `expand` at `DbspjMain.cpp:13963-13985` dispatches `P_ATTRINFO`:
  - real-table parent → `appendAttrinfoWithTableMeta` →
    `[tableId][schemaVersion][AttrHeader][data]`.
  - CTE virt-col parent → `appendCteAttrinfoWithVirtMeta` →
    `[encodeWord0 (marker+typeId+maxBytes)][encodeWord1 (csNumber)]
    [AttrHeader][data]`.
- DBLQH `buildCteLinkedBuffer` at `DblqhMain.cpp:18958-18979` copies the
  subroutine output verbatim into Step 1, prefix and all.
- Reader `JoinAggInterpreter::initGBTypes` at
  `JoinAggInterpreter.cpp:1928-1992` walks 2 prefix words then
  AttrHeader+data, and discriminates real-table-vs-CTE via the marker
  bit (`CteLinkedAttr::isCteMarker`). Both branches fully implemented.

So the writer and reader speak a consistent `[word0][word1][AttrHeader]
[data]` format. The parent-table-column GB shape **should** work
end-to-end. **No existing test exercises it**, which is why the branch
state still listed the phase as open.

## Plan: verify-first, fix only if needed

Treat Phase P-GB as a verification phase. Add a focused MTR test that
exercises the suspected-broken shape; the test outcome dictates the rest
of the work.

### Step 1 — Add MTR coverage in `ronsql_cte_basic.test`

Two new test blocks, both grouping on a parent-table column over a CTE
agg leaf:

**Test 13 (INNER JOIN, parent-table GB):**
```sql
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_amt) AS t
  FROM cte_orders GROUP BY o_custkey)
SELECT c.c_region, SUM(sums.t)
FROM cte_customer AS c JOIN sums ON sums.k = c.c_id
GROUP BY c.c_region;
```
- `c.c_region` is a real-table parent column → main GB emits
  `GroupByLinked(0, c_region_col)` with the parent-linked descriptor
  routed through Step 1 of the linked-attr buffer.
- Sums leaf delivers via Steps 2/3.

**Test 14 (LEFT JOIN variant):**
```sql
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_amt) AS t
  FROM cte_orders GROUP BY o_custkey)
SELECT c.c_region, SUM(sums.t)
FROM cte_customer AS c LEFT JOIN sums ON sums.k = c.c_id
GROUP BY c.c_region;
```
- Forces the agg-feed NULL-injection path (Phase 5 — `47d81b43903`) for
  Dave (c_id=400, no matching `sums` row). Customers grouped by region
  (1: Alice/Bob; 2: Charlie/Dave): region 2 should aggregate Charlie's
  orders plus a NULL contribution from Dave.

Both tests use the existing `cte_orders` / `cte_customer` fixture in the
file — no schema additions.

### Step 2 — Run and dispatch on outcome

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
./mtr --record --suite=ronsql ronsql_cte_basic
```

Three outcomes:

**(a) Tests pass.** Phase P-GB was incidentally fixed by Phase E.1K's
writer-side rework. Recorded `.result` is the deliverable. Update phase
docs: mark `Phase P-GB DONE — verified by Tests 13-14`, no kernel
changes required.

**(b) Tests fail with a kernel-side error or SEGV.** Investigate:
- First, dump the buffer at `cteLookupAggFeed`'s call to
  `processRecWithLinkedAttrs` (`DEB_CTE` traces) and confirm Step 1
  entries actually have the prefix in the agg-feed-with-linked-parents
  path.
- If Step 1 is unprefixed: writer-side fix in `cte_lookup_send` or
  `expand` so `appendAttrinfoWithTableMeta` runs on the agg-feed path.
  Most likely candidate: a `T_ATTRINFO_CONSTRUCTED` flag check that
  bypasses the addTableMeta branch.
- If Step 1 is prefixed but the reader still mishandles it: bug in
  `initGBTypes` real-table branch (e.g. the AttrHeader at `p[2]`
  doesn't carry the linked attrId the reader expects).
- Fix surgically; rerun Step 1 tests; ship.

**(c) Tests fail with a RonSQL-side error.** RonSQL has a guard that
rejects this shape. Lift the guard if its precondition is now
satisfied; rerun.

### Step 3 — Document

Once tests are green, regardless of (a) or (b)/(c) path:
- Update `ronsql_cte_plan.md` status block: drop "Phase P-GB" from open
  follow-ups, add to the "Current status" list with the verification
  approach noted.
- Update memory `project_cte_branch_state.md`: drop Phase P-GB from open
  follow-ups.
- Commit with subject `RONDB-1050: Verify Phase P-GB — parent-table GB
  over CTE agg leaf` (or fix-style subject if (b)/(c) required code
  changes).

## Files

- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — append Tests 13,
  14.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- (Conditional) `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` or
  `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp` if (b).
- (Conditional) `storage/ndb/src/ronsql/RonSQLPreparer.cpp` if (c).

## Verification

- `./mtr --suite=ronsql ronsql_cte_basic` — Tests 13–14 must pass and
  Tests 1–12 must continue to pass.
- `./mtr --suite=ronsql` — full suite, no regressions.
- `./mtr --suite=ronsql ronsql_cte_scan` — Phase E tests still green.

## Why verify-first

1. The bug description pre-dates Phase E.1K. Reading the current code,
   the format consistency the bug claimed broken appears to be present.
   Verifying takes one MTR run; rewriting the buffer logic without
   verifying risks fixing a non-bug.
2. Even if the format were inconsistent, the right fix would be
   writer-side (centralize at the source) — which is exactly what
   Phase E.1K already did for both real-table and CTE-virt-col entries.
3. The deliverable in (a) is just as valuable as in (b): an MTR test
   covering parent-table GROUP BY over a CTE agg leaf that previously
   had no coverage. If green from the start, that's a passing free
   test; if it surfaces a real bug, we've caught it with the test in
   hand.
