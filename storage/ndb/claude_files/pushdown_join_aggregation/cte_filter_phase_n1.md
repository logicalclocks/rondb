# Phase N.1 — Make `scanCte` work as parent of ordered-index children

## Status

**Plan, not yet implemented.**  Phase N.1 is the first sub-phase of
the wrap-up phase scoped in `cte_filter_phase_n.md`.  Originally
narrow ("fix Test 4 string-CTE re-aggregation"); widened after
triage to its real scope: support `scanCte` as the parent of an
ordered-index child where the index bound is derived from CTE result
columns.

## The principle

`scanCte` already works as a root for direct projection and for
some child operations, notably PK lookup children.  The remaining
gap exposed here is the ordered-index child shape:

```sql
FROM cte AS s
JOIN real_table AS t ON t.indexed_col = s.result_col
```

where the planner chooses `scanIndex` for `t` and the ordered-index
bound is a linked value from the CTE parent.  Today that shape can
produce silent wrong answers or NDB-API build errors.  This phase
closes that gap without changing the join order to avoid it.

**Explicit non-goals:**

- An AST rewrite that swaps the join order to avoid the unsupported
  shape.  Rewrites of that form mask the capability gap and lock the
  planner into permanent contortions.
- `scanTable` child support.  A table-scan child with a CTE-dependent
  join predicate needs linked filter execution, not ordered-index
  bound construction, and belongs in a later phase.

## Failing query

The motivating MTR case is `ronsql_minmax_string` Test 4:

```sql
WITH s AS (SELECT grp, MIN(v) AS min_v, MAX(v) AS max_v
           FROM str_minmax
           GROUP BY grp)
SELECT MIN(s.min_v), MAX(s.max_v)
FROM s
JOIN str_minmax AS t ON t.grp = s.grp;
```

`str_minmax` has `PRIMARY KEY USING HASH (id)` and
`INDEX idx_grp (grp)`.  The join column `t.grp` is on a non-unique
secondary index, not the PK.  Expected `('', 'echo')`; actual
`(NULL, NULL)`.

Additional triage narrows the failure:

- Direct CTE re-aggregation without a child works:
  `FROM s` returns `('', 'echo')`.
- CTE re-aggregation through a helper table with a PK on `grp` works:
  `FROM s JOIN str_groups AS g ON g.grp = s.grp` returns
  `('', 'echo')`.
- The failing ordered-index child still fails with a single parent
  row (`WHERE s.grp = 10`) and with `WHERE s.grp IN (10, 20)`, so the
  current evidence does not point to multi-range parameter advancement
  as the primary cause.
- Replacing string MIN/MAX with a numeric CTE aggregate such as
  `SUM(s.sum_id)` through the same ordered-index child returns `NULL`,
  so the failure is not string-specific.
- `COUNT(s.min_v)` and `COUNT(s.max_v)` through the failing shape
  return `NULL`, and grouped variants can return no rows.  The first
  diagnostic step must establish whether the child produces zero rows,
  whether linked CTE values are missing at the aggregation leaf, or
  both.

## Symptom matrix (before N.1)

Reference helper: `str_groups (grp INT NOT NULL,
PRIMARY KEY USING HASH (grp))` (PK on `grp`, single row per grp).

| Variant | Outer SELECT shape | Planner picks | RDRS result |
|---|---|---|---|
| no-join | `... FROM s` | `CTE_SCAN` only | `('', 'echo')` correct |
| PK helper | `... FROM s JOIN str_groups g ON g.grp = s.grp` | `CTE_SCAN` + `PK_LOOKUP` | `('', 'echo')` correct |
| Test 4 (CTE-only outer agg) | `MIN(s.min_v), MAX(s.max_v) FROM s JOIN str_minmax t ON t.grp = s.grp` | `CTE_SCAN` + `INDEX_SCAN` | `(NULL, NULL)` silent wrong |
| F (mixed projection) | `s.grp, s.min_v, s.max_v, t.id FROM s JOIN str_unique_grp t ...` | `CTE_SCAN` + `INDEX_SCAN` | build error: *Failed to create child operation* |
| G (numeric outer agg) | `MIN(s.c), MAX(s.c) FROM s_num JOIN ...` | `CTE_SCAN` + `INDEX_SCAN` | build error |
| COUNT(*) | `COUNT(*) FROM s JOIN ... ON t.grp = s.grp` | `CTE_SCAN` + `INDEX_SCAN` | build error |

The shape that builds (Test 4) is silent because the final aggregate
sees no non-NULL linked values and finalises as `MIN`/`MAX` over an
empty multiset.  Earlier kernel logs showed per-fragment receivers
with `m_processed_rows = 0`, but N.1 must reproduce this in a focused
block test before deciding whether the child scan produces no rows or
linked CTE values fail to reach the aggregation leaf.

Both behaviours are symptoms of the same unsupported ordered-index
child shape, but the exact failure points may differ.  Treat the
build errors and silent NULLs as related evidence, not as proven
identical code paths.

## Architecture today

### Kernel-supported shapes (what already works)

| Parent | Child | Where exercised |
|---|---|---|
| `scanTable` / `scanIndex` real-table | `lookupCte` | Phase A-D2 + tests 8-16 in `testCteNdbApi.cpp` |
| `scanTable` real-table | `readTuple` real-table (PK linked) | every join test ever |
| `scanCte` | `readTuple` real-table (PK linked) | testCteNdbApi.cpp Test 16; testCteNdbApiOuterJoin.cpp Test 6 |
| `scanCte` | `lookupCte` (chained CTE) | Phase E.2 |
| `scanCte` | **`scanIndex` real-table (linked bound)** | **broken — N.1 target** |
| `scanCte` | `scanTable` real-table | out of N.1 scope; needs linked filter execution |
| `scanCte` | `readTuple` real-table via UNIQUE_LOOKUP (non-PK unique idx) | verify as adjacent regression |

### Why the working shapes work

`scanCte` parent + `readTuple` PK-lookup child works because:
- the kernel's `LQHKEYREQ` path receives the linked PK key bytes
  via DBSPJ `Dbspj::expand` walking a `P_PARENT(1) + P_COL(virtPkColIdx)`
  pattern out of the CTE row buffer.
- `Dbspj::appendColToSection` reads the column raw value from
  `targetRow.m_row_data` using `row.m_header->m_offset[col]`,
  which `cte_scan_build` populated correctly for CTE row deliveries.
- No table-meta prefix is needed for the key bytes (PK lookup uses
  raw bytes; ATTRINFO with table-meta is a separate path covered
  by E.1K via `appendCteAttrinfoWithVirtMeta`).

### Why the broken shapes break

The exact root cause is not yet proven.  Two symptoms are known:

1. Some `scanCte` parent + ordered-index child variants fail during
   NDB-API operation creation with *Failed to create child operation*.
   This must be reproduced at block-test level and traced to the
   exact failing condition.  Do not assume broad API rejection of
   CTE-sourced linked operands: current code inspection shows
   `linkedValue(scanCteOp, attr)` resolves against the CTE virtual
   table, and `NdbLinkedOperandImpl::bindOperand()` checks column type
   metadata plus `linkWithParent()`, not "real table only".

2. Other variants build but silently produce NULL or no rows.  The
   working helper-PK query proves linked CTE aggregate values can be
   read correctly when the child is a PK lookup.  The failing
   ordered-index query therefore needs diagnostics around the
   `scanIndex` child: are no child rows produced, are parent CTE
   linked values unavailable at the aggregation leaf, or are ordered
   index bounds malformed?

The build-error variants and silent-NULL variants may have the same
underlying cause, but that is not yet established.  N.1 must start
with diagnostics that separate row production from linked-value
delivery.

### What is *already* in place from Phase E.1K

Phase E.1K (`cte_filter_phase_e1k.md`) addressed the *projection*
side of CTE-virt-col linked attrs:

- `Dbspj::appendCteAttrinfoWithVirtMeta` writes the inline-encoded
  CTE marker (`0x80000000 | typeId<<16 | maxBytes` etc.) for
  ATTRINFO bytes delivered to a child.
- `Dbspj::expand` and `Dbspj::appendFromParent` correctly take the
  CTE branch when `treeNodePtr.p->m_primaryTableId == 0`.
- `JoinAggInterpreter::initGBTypes` decodes the marker.
- F.4-K.1 / K.2 / K.2b substitute string payload bytes for
  `accumulators[i].value` at the DBLQH delivery sites.

What E.1K did *not* validate — and what N.1 needs to validate or fix
— is the **ordered-index bound path**.  Current NDB-API serialisation
for a linked `scanIndex` bound emits:

- the bound type as data; then
- `QueryPattern::attrInfo(linkedColIx)`.

That is intentional.  Ordered-index bounds require
`BoundType + AttributeHeader + payload`; `scanFrag_fixupBound()`
renumbers the AttributeHeader attribute id to the ordered-index key
position.  Replacing this with `P_COL` would strip the
AttributeHeader and break scanIndex bounds.  If N.1 needs to adjust
DBSPJ expansion, it must preserve the existing AH-bearing
`P_ATTRINFO` bound format.

## What N.1 must change

The work is at three layers, in order of dependency.

### Layer 1 — NDB API (`NdbQueryBuilder.cpp`)

First reproduce the build-error variants at NDB-API block-test level
and identify the exact rejecting condition.  If `qb->scanIndex(idx,
op.table, &bound, &opts)` rejects a linked bound whose source is a
`scanCte` op def, fix that specific condition only.

Do not add broad "CTE parent bound" flags until proven necessary.
The current serialised pattern for linked ordered-index bounds
already contains `P_PARENT` plus `P_ATTRINFO` through the existing
linked operand machinery.  A new `QN_ScanIndexParameters` bit is only
justified if diagnostics prove DBSPJ cannot distinguish the case from
the existing serialised pattern.

Adjacent verification:

- `qb->readTuple(idx, op.table, ...)` for UNIQUE_LOOKUP children of
  CTE parents should be checked as a regression guard, but it is not
  the primary N.1 target.
- `qb->scanTable` is out of N.1 scope; it needs linked filters, not
  ordered-index bound construction.

### Layer 2 — DBSPJ (`DbspjMain.cpp`)

Validate the existing expansion path before changing it.  For linked
ordered-index bounds, the current `m_keyPattern` should expand from
the parent row using the AH-bearing `P_ATTRINFO` format, and
`scanFrag_fixupBound()` should then rewrite the AttributeHeader ids
to ordered-index key positions.

If the diagnostics show that expansion from a CTE parent row is wrong,
fix the expansion or row-access bug while preserving:

- `BoundType + AttributeHeader + payload` in the bound section.
- `P_ATTRINFO` for linked scanIndex bound values.
- `scanFrag_fixupBound()` as the place that maps table column ids to
  ordered-index key positions.

Do not convert scanIndex bound construction to `P_COL`; that format
is suitable for raw key bytes in PK lookup paths, not ordered-index
bounds.

### Layer 3 — DBLQH / DBTUP

No changes expected.  Once DBSPJ delivers the correct bound bytes
in `LQHKEYREQ` / `SCAN_FRAGREQ`, DBLQH executes the index scan
identically regardless of the byte source.  The agg-feed path
(linked CTE columns reaching the outer aggregator) is wired by
F.4-K.3 already; the new shape inherits that.  Verify under N.1
test coverage.

### Layer 4 — RonSQL emit (`RonSQLPreparer.cpp`)

The current `emit_child_ops` INDEX_SCAN arm at lines 7896-7944
already emits `qb->scanIndex(idx, op.table, &bound, &opts)` with
linked keys.  Once Layer 1 lets the API accept the CTE-sourced
linked operand, this code path Just Works.

Two things to verify (likely no changes):

- `linked_projections` from CTE-virt-cols flowing through the
  scanIndex child to the outer aggregator's leaf — this is the
  case F/G/COUNT exercise.  Phase F.4-K.3 wired
  `JoinAggInterpreter::kOpLoadCol` for linked string aggregate
  columns.  Confirm the path holds when the leaf is an index scan
  rather than a PK lookup.
- Outer-aggregator anchor when the leaf is a `scanIndex` child of
  `scanCte`.  The fallback at `RonSQLPreparer.cpp:4406-4416`
  already handles a NULL leaf table by anchoring on the CTE
  virtual table; this case is the inverse (real-table leaf with
  CTE parent).  Should already work via the
  `linked_projections[]` wiring.

### Layer 5 — Outer aggregator linked-projection from CTE ancestor through scanIndex leaf

The OUTER aggregator runs on the deepest leaf.  When the leaf is
`scanIndex t` and the program needs `s.min_v`, `s.max_v`
(strings) and `s.grp` (INT) — all CTE-virt-cols — these arrive as
linked projections from the grandparent (CTE_SCAN root → INDEX_SCAN
child → OUTER aggregator on the child).

The existing linked-projection plumbing in
`emit_child_ops` (lines 7867-7886) adds
`addLinkedProjection(linkedValue(...))` per
`linked_projs[j]`.  When `source_op_idx` references the CTE_SCAN
root, the linked value's binding goes through Layer 1's path.  Add
the necessary acceptance there.

The receiver
(`JoinAggInterpreter::kOpLoadCol` linked-attr arm) already handles
strings post-F.4-K.3.  Numeric handling is older and unaffected.

## Step plan

### Step N.1.1 — Failing block-test in NDB-API

Before touching production code, add diagnostic block tests to
`storage/ndb/block_unit_test/testCteNdbApi.cpp` (or a dedicated
new `testCteNdbApiCteScanRoot.cpp`) reproducing the broken shape
end-to-end at the NDB-API level:

- A scalar CTE `s` with `MIN/MAX(varchar) GROUP BY grp`.
- Outer scan: `qb->scanCte(...)`.
- Outer child: `qb->scanIndex(idx_grp_on_real_t, real_t, &bound, ...)`
  with `bound` linked to `s.grp`.
- Diagnostics that separate child row production from linked CTE
  value delivery:
  `COUNT(t.id)`, `SUM(t.id)`, `COUNT(s.grp)`, `SUM(s.grp)`,
  and string `MIN(s.min_v)` / `MAX(s.max_v)`.
- A single-parent-row variant equivalent to `WHERE s.grp = 10`.
- A multi-parent-row variant equivalent to `WHERE s.grp IN (10,20)`.
- Numeric CTE linked values as well as string CTE linked values.
- INNER join only at first; LEFT_OUTER and ANTI variants explicitly
  skipped (separate sub-phases if needed).

These tests are the gating fixtures.  Their first purpose is to
answer: does the scanIndex child produce no rows, are CTE linked
values missing at the aggregation leaf, or both?

### Step N.1.2 — API: reproduce and fix exact scanIndex build errors

`storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`.  Edits centred on
the operand-binding path triggered from `scanIndex` /
`NdbQueryIndexBound` construction, but only after Step N.1.1 has
identified the precise rejection.  Fix the smallest failing condition
rather than adding a broad CTE-parent mode.

Do not introduce a new `m_isCteParentBound` flag unless the block
test proves the existing `P_PARENT` / `P_ATTRINFO` serialisation is
insufficient.

Verify by re-running the Step N.1.1 block tests.  If the build errors
are gone but NULL/no-row results remain, continue to Step N.1.3.  If
all diagnostic variants pass after the API fix, keep Step N.1.3 as
code inspection plus regression validation only.

### Step N.1.3 — DBSPJ: validate or fix scanIndex bound expansion

`storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`.  In
`scanFrag_build` / `scanFrag_parent_row`, validate that linked
ordered-index bounds expand from a `scanCte` parent row using
`BoundType + P_ATTRINFO`, and that `scanFrag_fixupBound()` sees valid
AttributeHeaders to renumber.

Care points:
- Multi-column ordered-index bounds: one bound entry per indexed
  column, in index-bound order.
- `hasNull` semantics: a NULL CTE-virt-col bound key should bail
  out the same way it does for real-table parents (the existing
  `if (hasNull)` arm in `scanFrag_parent_row`).
- `T_PRUNE_PATTERN` interaction: not exercised by current CTE
  tests; verify whether prune patterns are constructible from CTE
  virt-cols — likely deferred / rejected at Layer 1.
- `scanCopyAttrinfo` must not be changed as part of this phase.
  Its stored parameter length is also used to locate the `0x0721`
  pushdown interpreter program; the earlier experimental change did
  not fix the issue and risks unrelated regressions.

After this step, the Step N.1.1 block test produces correct rows
for INNER join shapes.

### Step N.1.4 — Validate OUTER aggregator linked-projection path

Confirm Layer 5 works: OUTER aggregator's program loads
CTE-virt-col linked attrs through the scanIndex leaf's row buffer.
Existing F.4-K.3 path should suffice; add a multi-slot string
test variant (Test 4 shape) to N.1.1.

If a gap surfaces here (e.g. linked-string load through scanIndex
leaf differs from through readTuple leaf), patch
`JoinAggInterpreter::kOpLoadCol` accordingly.

### Step N.1.5 — RonSQL emit verification

`storage/ndb/src/ronsql/RonSQLPreparer.cpp`.  Existing
`emit_child_ops` should produce the correct calls already.
Confirm under MTR.  No expected edits except possibly the generic
"Failed to create child operation" message becoming dead code for
the now-supported shapes — leave it as a backstop.

### Step N.1.6 — RonSQL MTR coverage

Augment `mysql-test/suite/ronsql/t/ronsql_minmax_string.test`:

- **Test 4** stays as-is, now passing.
- **Test 4-mixed-projection** — outer SELECT references both `s.*`
  and `t.*` columns (the F variant the user ran, e.g.
  `SELECT s.grp, s.min_v, t.id FROM s JOIN t ...`).
- **Test 4-numeric-outer-agg** — outer aggregate is numeric
  (`MIN(s.c)`); covers the G variant and proves the gap fix isn't
  string-specific.
- **Test 4-count-star** — `COUNT(*) FROM s JOIN t ON ...`; covers
  the COUNT variant.
- **Test 4-single-parent** — same ordered-index child with
  `WHERE s.grp = 10`; prevents misdiagnosing the issue as only
  multi-range parameter advancement.
- **Test 4-chained** — a chained CTE that re-aggregates an earlier
  CTE through a non-PK join.

Add matching block-test variants in `testCteNdbApi.cpp` to keep
coverage at the NDB-API level (per the
`feedback_phase_to_kernel_tests.md` rule).

### Step N.1.7 — Coverage extension to outer-join cases

LEFT_OUTER / ANTI / SEMI variants where the CTE is the LEFT side
need separate handling because the existing CTE-as-outer-join-child
path was deliberately dropped at the kernel level
(`cte_outer_join_phase_3.md`).  N.1 covers `scanCte` as the LEFT
*parent* of an outer join with a non-PK real-table child.

Likely smaller than N.1.3 since the agg-feed NULL injection path
already exists for the working `readTuple`-child case (Phase
outer-join 1 / 5).  Verify in a separate sub-phase if scope
expands; otherwise fold into Step N.1.6.

### Step N.1.8 — Memory + catalogue

When N.1 lands:

- Update `memory/project_i6_varchar_minmax_state.md` — drop the
  CTE-root + index-scan-child gated-shape note.
- Update `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
  catalogue: mark this phase doc shipped; mark I.25 string CTE
  re-aggregation closed.
- Update `cte_filter_phase_n.md` (Phase N umbrella): mark N.1
  closed.

Test 5 (`WHERE min_v < 'beta'`) remains a separate sub-phase
(string-literal parse + MTR quote-safe compare); not closed here.

## Tests (block + MTR)

Block-test additions in `storage/ndb/block_unit_test/testCteNdbApi.cpp`
or new `testCteNdbApiCteScanRoot.cpp`:

| Test | Shape | Asserts |
|---|---|---|
| 1 | scanCte + scanIndex child + `COUNT(t.id)`, `SUM(t.id)` | child row production is correct |
| 2 | scanCte + scanIndex child + `COUNT(s.grp)`, `SUM(s.grp)` | linked CTE numeric values reach the leaf |
| 3 | scanCte + scanIndex child + string `MIN(s.min_v)`, `MAX(s.max_v)` | linked CTE string values reach the leaf |
| 4 | scanCte + scanIndex child + outer GB on CTE col | grouped agg correct |
| 5 | scanCte + scanIndex child + single-parent `WHERE s.grp = 10` | single range works |
| 6 | scanCte + readTuple via UNIQUE_LOOKUP (non-PK unique idx) child + outer agg | should already work; regression guard |
| 7 | Multi-fragment / multi-node redistribute on the new shape | result independent of partition layout |
| 8 | Multi-column ordered-index bound from CTE columns | full bound construction |

MTR additions in `ronsql_minmax_string.test` per Step N.1.6.

Regression matrix:

```
./mtr --suite=ronsql ronsql_minmax_string
./mtr --suite=ronsql
./mtr --suite=ndb_push_agg
testCteNdbApi -c <cs> -m <mp>
testCteNdbApiOuterJoin -c <cs> -m <mp>
testCteNdbApiFilter -c <cs> -m <mp>
testJoinAggNdbApi -c <cs> -m <mp>
testVarcharMinMax -c <cs> -m <mp>
```

## Risks

1. **Ordered-index bound format.**  Accidentally converting linked
   scanIndex bounds from `P_ATTRINFO` to `P_COL` would strip the
   AttributeHeader needed by `scanFrag_fixupBound()`.  This would
   risk regressions in ordinary ordered-index scans.
2. **Multi-column ordered-index bounds.**  Existing CTE_LOOKUP path
   covers multi-key joins; the new scanIndex-bound path needs to do
   the same for one bound entry per index column.
3. **Charset / type widening on the bound column.**  Bound bytes
   are raw values; charset doesn't matter for INT virt-PK cols.
   For non-INT virt-PK joins (rarer), confirm Phase D2's widening
   logic at the API operand-binding layer.
4. **Phase outer-join 3 boundary.**  N.1 covers `scanCte` as
   parent (any join type that's not on the LEFT-side disallow
   list).  Do not let N.1 reopen `CTE_SCAN as outer-join *child*`.
5. **`hasNull` bailout** on CTE-virt-col bound.  The existing
   `scanFrag_parent_row` `hasNull` path emits null injection for
   outer-join leaves and bails for inner.  Should work
   transparently with CTE bound bytes; verify with a NULL grp test.
6. **No new wire format.**  AGG_RESULT / AGG_CHAR_RESULT unchanged.
   E.1K's CTE-marker encoding unchanged.  All N.1 work is at the
   bound-key construction layer, which uses raw bytes (no marker).

## Critical files

- `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`,
  `NdbQueryBuilderImpl.hpp` — operand binding + serialisation
  (Steps N.1.2).
- `storage/ndb/src/ndbapi/NdbQueryOperationDef*.{cpp,hpp}` —
  possible index-scan op-def metadata only if diagnostics prove a new
  marker is required.
- `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` —
  `scanFrag_build` key pattern emission (Step N.1.3),
  `scanFrag_parent_row` (no edit expected).
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp` —
  re-validation only; no expected change (Step N.1.4).
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` —
  re-validation only; no expected edit (Step N.1.5).
- `storage/ndb/block_unit_test/testCteNdbApi.cpp` (or new
  `testCteNdbApiCteScanRoot.cpp`) — gating fixture + coverage
  (Steps N.1.1, N.1.6).
- `mysql-test/suite/ronsql/t/ronsql_minmax_string.test` — MTR
  coverage extension (Step N.1.6).
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md` —
  catalogue update (Step N.1.8).

## Cleanly-rejected shapes (post-N.1)

- **`CTE_SCAN` as outer-join *child*** — kernel-level reject
  preserved (`cte_outer_join_phase_3.md`).
- **DECIMAL > 18 (signed) / > 19 (unsigned) in MIN/MAX** —
  Phase I.22 guard preserved.
- **Recursive CTEs / UNION-in-CTE / DISTINCT aggregation** —
  unchanged.
- **`SUM(string)` and other non-MIN/MAX string aggregates** —
  Phase I.6 S.4 guard preserved.

## Out of scope for N.1

- Test 5 (`WHERE min_v < 'beta'`).  String-literal parser +
  MTR-quote-safe-compare track is orthogonal; ride a separate
  sub-phase (N.1.2 or Phase N follow-up).
- `scanCte` parent + `scanTable` child with a non-indexed join
  predicate.  That needs linked filter execution and is not fixed by
  ordered-index bound support.
- Comparison matrix across all types (signed / unsigned / float /
  DECIMAL / date / time) — broader I.25 catalogue, not the
  capability-gap closure N.1 targets.
- Performance tuning of the new shape (cost-based decisions
  between rewriting vs. executing the new path).  N.1 makes the
  shape work; cost decisions remain heuristic.

## Deliverables

- Failing block test landed first as the gating fixture.
- API + DBSPJ commits per Steps N.1.2-N.1.4.
- Block + MTR coverage per Step N.1.6.
- Outer-join coverage per Step N.1.7 (or explicit deferral).
- Phase doc + catalogue + memory updates per Step N.1.8.

## Open questions for execution

1. Is any new tree-node bit needed at all, or is the existing
   `P_PARENT` / `P_ATTRINFO` linked-bound serialisation already
   sufficient once the CTE parent row is handled correctly?
2. Does the build-error variant fail in NDB-API operand binding,
   parent linkage, index-bound serialisation, or later DBSPJ build?
   Step N.1.1 must answer this before production changes.
3. Do the silent-NULL variants produce zero child rows, or do they
   produce child rows with missing linked CTE values at the outer
   aggregation leaf?
4. Do any RDRS-specific paths need to know about the new shape,
   or is the change kernel + NDB-API only?  Verify during N.1.6
   MTR coverage.
