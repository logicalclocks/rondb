# Phase I.24 - Emit from resolved column descriptors

## Status

**Shipped.**  The descriptor migration ran in 21 commits between
`855ec891694` (plan) and `7e0f33b7890` (legacy-array removal), with
`cf188857098` introducing the descriptor type itself.  Each emit
surface (CTE metadata helpers, filter emit, aggregation loads,
pass-through routing, CASE condition emit, single-table
aggregation, embedded filter loads, linked projections, subquery
aggregation, aggregate validation, CTE source emit, column
ownership, WHERE classification, result metadata, CTE metadata,
index bound metadata) was migrated incrementally to the
`ResolvedColumnRef` descriptor contract before the validation
commit `e8a2e00686a` and the legacy arrays
(`column_attrId_map` / `column_map` / `column_table_idx`) were
removed.

I.11's kernel-topology coverage commit `3ca4f897af3` landed on top
of the migrated emit and exercised the new descriptor path
end-to-end.

The original plan body follows.

Phase context (preserved): I.23 fixed scoped name resolution for
CTE bodies and the main SELECT, but most emit paths still consumed
the legacy resolver outputs:

- `QueryScope::column_attrId_map[col_idx]`;
- `QueryScope::column_map[col_idx]`;
- `QueryScope::column_table_idx[col_idx]`.

Those arrays were populated by the scoped resolver, so the
behaviour was correct.  However, they were a lossy emit
interface: a CTE result column, a stored-table column, an alias-only
HAVING reference, and an unresolved sentinel were all represented by
a mix of array entries and implicit conventions.  I.24 made the
resolved column descriptor the authoritative emit contract.

## Problem

Phase I.23 made the resolver scope-aware, but emit still reinterprets
global `col_idx` through parallel arrays.  This keeps several old
failure modes possible:

- emit code can accidentally use a `col_idx` that belongs to another
  scope;
- CTE output references have to rediscover output names or trust that
  `column_attrId_map[col_idx]` contains a CTE output ordinal;
- stored columns and CTE result columns are distinguished indirectly by
  checking the owning `JoinOp` type and whether `column_map[col_idx]` is
  NULL;
- unsupported alias-only references rely on sentinel values and special
  skips;
- virtual-table construction and aggregation emit can diverge if they
  interpret the old arrays differently.

The goal is not to add new SQL shapes.  The goal is to replace the emit
contract with explicit metadata that was produced by scoped resolution.

## Target model

Add an authoritative per-scope descriptor array, indexed by `col_idx`
for compatibility with parser and aggregation compiler references:

```cpp
struct ResolvedColumnRef {
  enum class Kind {
    Unresolved,
    StoredColumn,
    CteResultColumn,
    AliasOnly
  };

  Kind kind;
  Uint32 source_scope_id;
  Uint32 join_op_idx;

  // StoredColumn
  NdbAttrId attr_id;
  const NdbDictionary::Column* dict_column;

  // CteResultColumn
  Uint32 cte_def_idx;
  Uint32 cte_result_idx;
  const Outputs* cte_output;
  QueryScope* cte_scope;

  // Common derived metadata
  NdbDictionary::Column::Type type;
  Uint32 length;
  const void* charset;
  Int32 scale;
  Int32 precision;
  bool nullable;
};
```

Names and exact field layout can change during implementation.  The
important invariant is that emit code should not have to rediscover
whether a column reference is stored-table or CTE-backed by probing
parallel arrays.

The old arrays remain during the early I.24 conversion commits, but they
are temporary compatibility outputs derived from `ResolvedColumnRef`.
The end state of I.24 is to remove them from `QueryScope` after every
consumer has moved to descriptors or to a descriptor-derived API.

## Implementation plan

### Step 1 - Add descriptors without changing emit behaviour

Extend `QueryScope` with:

- `ResolvedColumnRef* resolved_columns`;
- optional small helper methods such as `resolved_col(col_idx)`;
- helper predicates for `is_stored_column`, `is_cte_result_column`,
  and `is_alias_only`.

Populate `resolved_columns` in the same scoped resolver paths that now
populate `column_attrId_map`, `column_map`, and `column_table_idx`.

After populating descriptors, derive the legacy arrays from them:

- stored column: `column_attrId_map = attr_id`,
  `column_map = dict_column`, `column_table_idx = join_op_idx`;
- CTE result: `column_attrId_map = cte_result_idx`,
  `column_map = NULL`, `column_table_idx = join_op_idx`;
- unresolved / alias-only: sentinel values matching current behaviour.

This step must be behaviour-preserving.

### Step 2 - Convert CTE type and virtual-table helpers

Convert helpers that only need metadata and do not emit kernel programs:

- `resolve_cte_output_columns_for_scope()`;
- `resolve_chained_column_type()`;
- `build_cte_virtual_tables()`;
- any CTE-output source-type checks.

These are the safest first users because they already model the stored
column vs CTE output distinction explicitly in comments and control
flow.  After this step, virtual-table type derivation should consume
`ResolvedColumnRef` rather than re-walking CTE outputs from
`column_attrId_map`.

### Step 3 - Convert filter emit

Convert filter paths to descriptor helpers:

- `apply_filter_cmp()`;
- `apply_filter_like()`;
- `emit_cte_lookup_filter()`;
- embedded filter expression helpers used for cross-table WHERE and
  CASE conditions.

The conversion should make invalid emit states fail with clear
prepare-time errors, not `ndbrequire()` or generic "failed writing
program" errors.

### Step 4 - Convert aggregation emit

Convert `programAggregator_join()` and related helpers:

- group-by column emission;
- linked projection construction;
- leaf vs parent column loads;
- CTE leaf linked-buffer position calculation;
- GREATEST / LEAST pair-op nullability and typed-load support;
- CASE condition column resolution.

This step should remove duplicated checks such as:

```cpp
op.type == CTE_LOOKUP || op.type == CTE_SCAN
```

from call sites where the descriptor kind already tells us the source
kind.  Some checks will still remain at the boundary where NDB API op
type controls the physical emit instruction.

### Step 5 - Convert pass-through drain and result routing

Convert projection-only result routing in `execute_passthrough_drain()`
to descriptors.  This is important because pass-through currently has
its own CTE-column attr registration assumptions and can diverge from
aggregation emit.

### Step 6 - Tighten assertions and retire old direct dependencies

Once all emit users consume descriptors:

- keep legacy arrays only if still required by old helper APIs;
- otherwise remove them or restrict them to compatibility shims;
- replace user-triggerable `ndbrequire()` checks with clear resolver /
  planner errors;
- add debug assertions that legacy arrays, if still present, match the
  descriptor-derived values.

### Step 7 - Convert ResultPrinter input metadata

`ResultPrinter` still takes `column_map` for aggregate result formatting.
Move it to descriptor-based metadata before deleting the compatibility
arrays.

Acceptable implementation choices:

- pass `ResolvedColumnRef*` plus the column count and let
  `ResultPrinter` derive source column metadata from each descriptor; or
- build a small output/type descriptor array for the printer and keep
  `ResultPrinter` independent of `QueryScope`.

The second option has a cleaner ownership boundary, but the first option
is likely smaller.  Either way, `ResultPrinter` must no longer require
`QueryScope::column_map`.

### Step 8 - Convert CTE output metadata back-fill

`resolve_cte_output_columns_for_scope()` still back-fills
`scope.column_map[col_idx]` for CTE result columns.  Replace that with
descriptor-only metadata:

- update `ResolvedColumnRef::dict_column` for source-backed CTE COLUMN
  outputs;
- update it for MIN/MAX aggregate outputs where the source type is
  preserved;
- leave SUM/COUNT and unsupported aggregate output metadata explicit
  rather than represented by a NULL `column_map` sentinel.

After this step, CTE virtual-table construction and result formatting
must read descriptor metadata, not `column_map`.

### Step 9 - Convert remaining index/scan metadata users

Move remaining scan/index emit helpers off `column_map`.

Known examples:

- index-bound constant encoding that still calls
  `encode_constant(..., column_map[col_idx])`;
- any scan-config helper that uses `column_map` only to find the source
  dictionary column.

These should read `ResolvedColumnRef::dict_column` after confirming the
reference is a stored-table column.

### Step 10 - Update comments and helper API contracts

Remove stale comments and helper signatures that describe
`column_attrId_map`, `column_map`, or `column_table_idx` as the emit
contract.

This step should make it clear that:

- descriptors are authoritative;
- any remaining legacy array use is a temporary compatibility shim;
- helper APIs either accept descriptors directly or accept a narrow
  descriptor-derived structure.

### Step 11 - Add temporary consistency checks

Before deleting the legacy arrays, add debug/runtime consistency checks
that compare each legacy array entry with the descriptor-derived value.
This should be a small, behaviour-preserving subphase.

Checks should cover:

- stored column descriptor: attr id, dictionary column pointer, op index;
- CTE result descriptor: result index and op index;
- alias/unresolved descriptor: sentinel values.

Run the full `ronsql` suite with these checks in place.  If this passes,
the next subphase can remove the arrays with much lower risk.

### Step 12 - Remove legacy arrays from QueryScope

Final removal subphase:

- delete `QueryScope::column_attrId_map`;
- delete `QueryScope::column_map`;
- delete `QueryScope::column_table_idx`;
- remove allocation and derivation in `load_single_table()` and
  `resolve_columns_for_scope()`;
- remove any compatibility assertions added in Step 11 that only exist
  to compare arrays;
- update remaining comments and declarations.

After this step, any attempt to add new emit logic through the old arrays
will fail at compile time.

## Test plan

I.24 should not rely on new functionality to prove itself.  It should
rerun and preserve existing coverage:

- `ronsql.ronsql_cte_name_resolution`;
- `ronsql.ronsql_cte_chained`;
- `ronsql.ronsql_cte_basic`;
- `ronsql.ronsql_cte_case`;
- `ronsql.ronsql_cte_greatest_least*`;
- `ronsql.ronsql_join_agg`;
- `ronsql.ronsql_subquery_agg_ext`;
- full `ronsql` suite before commit.

Add small regression tests only where conversion exposes missing
coverage:

1. A CTE result column used in GROUP BY and aggregate load in the same
   query.
2. A stored table column and CTE result column with the same short name,
   both emitted through qualified references.
3. A HAVING-only aggregate not in SELECT.
4. A SELECT-list subquery aggregate alias in HAVING.

If those cases are already covered by existing tests, prefer comments in
the phase notes over duplicate SQL.

## Rollout rules

- Do not remove the legacy arrays until Steps 7-11 are complete and
  green.
- Do not change parser `col_idx` allocation in this phase.
- Keep each conversion step separately reviewable.
- Every I.24 subphase commit must pass the full `ronsql` MTR suite.
  Targeted tests are useful while developing, but they are not enough
  for commit because the descriptor contract feeds broad emit paths.
  The commit message should mention that the full `ronsql` suite was
  green.
- If an emit path is hard to convert cleanly, leave it on the legacy
  arrays only temporarily and add an explicit TODO in the I.24 notes
  identifying the remaining dependency before Step 12.
- Preserve existing user-visible error messages unless the old message
  was misleading or generic.

## Out of scope

- New SQL support for I.11 shapes.
- Planner changes for CTE_SCAN / CTE_LOOKUP fallback.
- Parser alias model changes.
- Removing all `col_idx` usage from the aggregation compiler.
- Arbitrary expression result descriptors beyond the column references
  already needed by emit.
