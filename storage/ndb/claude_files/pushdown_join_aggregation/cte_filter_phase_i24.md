# Phase I.24 - Emit from resolved column descriptors

## Status

**Planned.**  This phase follows Phase I.23.  I.23 fixed scoped name
resolution for CTE bodies and the main SELECT, but most emit paths still
consume the legacy resolver outputs:

- `QueryScope::column_attrId_map[col_idx]`;
- `QueryScope::column_map[col_idx]`;
- `QueryScope::column_table_idx[col_idx]`.

Those arrays are currently populated by the scoped resolver, so the
behaviour is correct again.  However, they are still a lossy emit
interface: a CTE result column, a stored-table column, an alias-only
HAVING reference, and an unresolved sentinel are all represented by a
mix of array entries and implicit conventions.  I.24 should make the
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

The old arrays should remain during I.24, but become compatibility
outputs derived from `ResolvedColumnRef`.

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

- Do not remove the legacy arrays in the first commit.
- Do not change parser `col_idx` allocation in this phase.
- Keep each conversion step separately reviewable.
- Every I.24 subphase commit must pass the full `ronsql` MTR suite.
  Targeted tests are useful while developing, but they are not enough
  for commit because the descriptor contract feeds broad emit paths.
  The commit message should mention that the full `ronsql` suite was
  green.
- If an emit path is hard to convert cleanly, leave it on the legacy
  arrays but add an explicit TODO in the I.24 notes identifying the
  remaining dependency.
- Preserve existing user-visible error messages unless the old message
  was misleading or generic.

## Out of scope

- New SQL support for I.11 shapes.
- Planner changes for CTE_SCAN / CTE_LOOKUP fallback.
- Parser alias model changes.
- Removing all `col_idx` usage from the aggregation compiler.
- Arbitrary expression result descriptors beyond the column references
  already needed by emit.
