# Phase N.1 — Make `scanCte` a first-class join root with arbitrary children

## Status

**Plan, not yet implemented.**  Phase N.1 is the first sub-phase of
the wrap-up phase scoped in `cte_filter_phase_n.md`.  Originally
narrow ("fix Test 4 string-CTE re-aggregation"); widened after
triage to its real scope: complete the architectural symmetry that
makes `scanCte` behave like any other join root.

## The principle

`scanTable`, `scanIndex`, and `readTuple` already function as
join roots that accept *any* child operation type
(`readTuple`, `lookupCte`, `scanIndex`, `scanCte` chained, ...).
`scanCte` should have the same property: a query that selects
`FROM cte JOIN <anything>` should pick whatever access method is
optimal for the child without the engine caring that the parent
happens to be a CTE.  Today that symmetry is incomplete and the
gap surfaces as either silent wrong answers or NDB-API build
errors.  This phase closes the gap.

**Explicit non-goal:** an AST rewrite that swaps the join order to
avoid the unsupported shape is rejected as a fix.  Rewrites of that
form mask the capability gap and lock the planner into permanent
contortions.  The fix is to support the shape, not to avoid it.

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

The single shape that builds (Test 4) is silent because its outer
SELECT references no `t.*` columns, so the API operand-binding
that fails for F/G/COUNT does not fire — but the OUTER aggregator's
per-fragment receivers report `m_processed_rows = 0` for every
fragment (kernel logs).  Result: `MIN`/`MAX` over an empty multiset
= `(NULL, NULL)`.

Both behaviours are the same underlying gap: `scanCte` parent +
non-PK real-table child is unimplemented.  The build errors are an
honest "rejected at API"; the silent NULL is just a flavour where
the rejection slips through and runtime quietly produces nothing.

## Architecture today

### Kernel-supported shapes (what already works)

| Parent | Child | Where exercised |
|---|---|---|
| `scanTable` / `scanIndex` real-table | `lookupCte` | Phase A-D2 + tests 8-16 in `testCteNdbApi.cpp` |
| `scanTable` real-table | `readTuple` real-table (PK linked) | every join test ever |
| `scanCte` | `readTuple` real-table (PK linked) | testCteNdbApi.cpp Test 16; testCteNdbApiOuterJoin.cpp Test 6 |
| `scanCte` | `lookupCte` (chained CTE) | Phase E.2 |
| `scanCte` | **`scanIndex` real-table (linked bound)** | **broken — N.1A target** |
| `scanCte` | **`scanTable` real-table** | **broken — N.1A target** |
| `scanCte` | **`readTuple` real-table via UNIQUE_LOOKUP (non-PK unique idx)** | **untested — verify under N.1A** |

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

`scanCte` parent + `scanIndex` child fails for two reasons (one at
build, one at runtime):

1. **API operand binding** (`NdbQueryBuilder.cpp`).  When
   `linkedValue(scanCteOp, virtCol)` is supplied as a bound key to
   `qb->scanIndex(...)`, operand binding rejects the linked operand
   because the binding logic is wired only for real-table parents'
   tuple payloads.  The exact failure point is in
   `bindOperand` triggered from `scanIndex` operand attachment;
   the symptom is `qb->scanIndex` returning `nullptr`, surfacing
   as the generic *Failed to create child operation* at
   `RonSQLPreparer.cpp:8021`.

2. **DBSPJ bound-key construction**
   (`DbspjMain.cpp::scanFrag_build` /
   `scanFrag_parent_row`).  Even if the API allowed the operand,
   DBSPJ's `m_keyPattern` for `scanIndex` is constructed assuming a
   real-table parent's tuple descriptor.  The pattern that should
   reach `appendColToSection` for a CTE-virt-col bound is not
   emitted by `scanFrag_build` for CTE parents.

For the silent-NULL case (Test 4), the API path differs: the outer
SELECT touches no `t.*` columns, so no linked projection from `t`
is bound through the API operand path that rejects.  But the OUTER
aggregator's program still expects rows from the join leaf, which
is `scanIndex t` — and `scanIndex t` produces zero rows because
the bound construction never wires up the CTE-virt-col path.  The
outer aggregator finalises over an empty input.

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

What E.1K did *not* cover — and what N.1 needs to add — is the
**bound-key path**.  That path uses `appendColToSection` (no
table-meta prefix), so it does *not* reuse the inline-marker
infrastructure, but it *does* need the kernel to know which
column-offset to read out of a CTE row buffer.  `cte_scan_build`
already populates `m_offset[]` for CTE virt-cols (this is what makes
the working `readTuple` PK-linked case succeed); the missing piece
is plumbing the same offset through into the scanIndex bound.

## What N.1 must change

The work is at three layers, in order of dependency.

### Layer 1 — NDB API (`NdbQueryBuilder.cpp`)

`qb->scanIndex(idx, op.table, &bound, &opts)` must accept linked
bound operands whose source is a `scanCte` op def.  Specific
changes (line numbers from current tip; verify at implementation):

- `NdbQueryOperationDefImpl::bindOperand` (or its index-scan
  variant): allow the linked operand's parent to be of kind
  `scanCte` / `lookupCte`.  Today it's narrowly typed for
  real-table parents.
- The implicit parent that an `NdbLinkedOperand` points at must
  resolve through `setParent` correctly when source is CTE.  Phase
  E.1 and E.1K already exercise this for the `readTuple` PK path;
  extend the same plumbing to the index-scan bound path.
- A new `m_isCteParentBound` bit (or repurposed
  `m_isCteEmbedded`) on `NdbQueryIndexScanOperationDefImpl` so
  serialisation can flag the case for DBSPJ.
- `NdbQueryBuilderImpl::serialize` emits the right
  `QN_ScanIndexParameters` tree-node bits when the bound has any
  CTE-sourced linked operand.

Mirror the same in `qb->readTuple(idx, op.table, ...)` for
`UNIQUE_LOOKUP` children of CTE parents.  And in `qb->scanTable`
for the no-bound table-scan case.

The build-error path becomes a clean success — the operand binds,
the op def is non-NULL, `RonSQLPreparer.cpp:8021` no longer fires
for these shapes.

### Layer 2 — DBSPJ (`DbspjMain.cpp`)

`scanFrag_build` (around line 10208 today) parses the new
tree-node bits and constructs `m_keyPattern` for the CTE-bound
case:

- For each linked-bound column, emit a pattern token of the form
  `P_PARENT(levels) + P_COL(virtColIdx)` that, when expanded by
  `expand` at `scanFrag_parent_row`, reads the column raw value
  from the CTE parent row via `appendColToSection`.
- The CTE parent's `m_offset[]` is already correct
  (`cte_scan_build` populates it), so the existing
  `appendColToSection` reads the right bytes without a CTE-marker
  prefix (bound keys are raw bytes, not ATTRINFO).
- For multi-column virt-PK CTEs, emit one P_PARENT/P_COL pair per
  bound column, in virt-PK order.

`scanFrag_parent_row` (line 11103) needs no new logic — it already
calls `expand(keyPtrI, pattern, rowRef, hasNull)`.  With the
correct pattern, `expand` walks to the CTE parent and
`appendColToSection` reads the bound bytes.  The `hasNull` path is
unchanged.

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

Before touching any production code, add a *failing* block test to
`storage/ndb/block_unit_test/testCteNdbApi.cpp` (or a dedicated
new `testCteNdbApiCteScanRoot.cpp`) reproducing the broken shape
end-to-end at the NDB-API level:

- A scalar CTE `s` with `MIN/MAX(varchar) GROUP BY grp`.
- Outer scan: `qb->scanCte(...)`.
- Outer child: `qb->scanIndex(idx_grp_on_real_t, real_t, &bound, ...)`
  with `bound` linked to `s.grp`.
- OUTER aggregator on the child leaf, reading linked CTE-virt-cols
  (mix numeric `s.grp` + string `s.min_v`).
- Multiple variants: INNER join only at first; LEFT_OUTER and ANTI
  variants explicitly skipped (separate sub-phases if needed).

This test fails today and is the gating fixture.  Land it first as
"reproduces N.1 gap" (commented `#if 0` or with an explicit
`expectedToFail` annotation), unfence as the layers below land.

### Step N.1.2 — API: accept CTE-sourced linked operand on scanIndex bound

`storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`.  Edits centred on
the operand-binding path triggered from `scanIndex` /
`NdbQueryIndexBound` construction.  Mirror the existing
`readTuple`-with-CTE-parent acceptance.  Add the
`m_isCteParentBound` flag on the index-scan op def for serialisation.

Make the same change for `qb->readTuple` UNIQUE_LOOKUP variant
and `qb->scanTable` (no-bound) for CTE parents.

Verify by re-running the Step N.1.1 block test: now the build no
longer fails, but the query still returns wrong results (zero rows
or NULLs) because DBSPJ doesn't yet construct the bound correctly.

### Step N.1.3 — DBSPJ: scanFrag_build emits CTE-aware key pattern

`storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`.  In
`scanFrag_build`, when parsing `QN_ScanIndexParameters` that has
the CTE-bound flag, walk linked-key columns and emit per-column
pattern tokens that resolve through `appendColToSection` from the
CTE parent row.

Care points:
- Multi-column virt-PK: one pair per column, in virt-PK order.
- `hasNull` semantics: a NULL CTE-virt-col bound key should bail
  out the same way it does for real-table parents (the existing
  `if (hasNull)` arm in `scanFrag_parent_row`).
- `T_PRUNE_PATTERN` interaction: not exercised by current CTE
  tests; verify whether prune patterns are constructible from CTE
  virt-cols — likely deferred / rejected at Layer 1.

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
- **Test 4-table-scan-child** — variant where `t` has *no* useful
  index for the join column; planner picks `TABLE_SCAN` child
  rather than `INDEX_SCAN`.  Same enablement should cover this
  shape.
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
| 1 | scanCte + scanIndex child + outer scalar agg (numeric) | rows produced, agg correct |
| 2 | scanCte + scanIndex child + outer scalar agg (string) | string MIN/MAX correct |
| 3 | scanCte + scanIndex child + outer GB on CTE col | grouped agg correct |
| 4 | scanCte + scanTable child (no useful index) + outer agg | rows produced, agg correct |
| 5 | scanCte + readTuple via UNIQUE_LOOKUP (non-PK unique idx) child + outer agg | should already work; regression guard |
| 6 | Multi-fragment / multi-node redistribute on the new shape | result independent of partition layout |
| 7 | Multi-column virt-PK CTE + scanIndex child binding all virt-PK columns | full bound construction |

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

1. **Multi-column virt-PK CTEs.**  Existing CTE_LOOKUP path covers
   multi-key joins; the new scanIndex-bound path needs to do the
   same.  N.1.3 must emit one pattern pair per virt-PK column.
2. **Charset / type widening on the bound column.**  Bound bytes
   are raw values; charset doesn't matter for INT virt-PK cols.
   For non-INT virt-PK joins (rarer), confirm Phase D2's widening
   logic at the API operand-binding layer.
3. **Phase outer-join 3 boundary.**  N.1 covers `scanCte` as
   parent (any join type that's not on the LEFT-side disallow
   list).  Do not let N.1 reopen `CTE_SCAN as outer-join *child*`.
4. **`hasNull` bailout** on CTE-virt-col bound.  The existing
   `scanFrag_parent_row` `hasNull` path emits null injection for
   outer-join leaves and bails for inner.  Should work
   transparently with CTE bound bytes; verify with a NULL grp test.
5. **No new wire format.**  AGG_RESULT / AGG_CHAR_RESULT unchanged.
   E.1K's CTE-marker encoding unchanged.  All N.1 work is at the
   bound-key construction layer, which uses raw bytes (no marker).

## Critical files

- `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`,
  `NdbQueryBuilderImpl.hpp` — operand binding + serialisation
  (Steps N.1.2).
- `storage/ndb/src/ndbapi/NdbQueryOperationDef*.{cpp,hpp}` — flag
  on index-scan op def.
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

1. Are there tree-node bits available on `QN_ScanIndexParameters`
   for the CTE-bound flag, or does it need a new bit allocated and
   versioned?  (Likely a free bit; check `Sections.hpp` /
   `QueryTree.hpp` at implementation time.)
2. Does the existing `m_isCteEmbedded` flag suffice, or is a new
   `m_hasCteParentBound` needed?  Decide during N.1.2.
3. Should `qb->scanTable` (no-bound table-scan child of CTE
   parent) be enabled in the same set of changes as `scanIndex`,
   or split as a follow-up?  Recommend same set — it's mostly the
   acceptance change at Layer 1; no key pattern at Layer 2.
4. Do any RDRS-specific paths need to know about the new shape,
   or is the change kernel + NDB-API only?  Verify during N.1.6
   MTR coverage.
