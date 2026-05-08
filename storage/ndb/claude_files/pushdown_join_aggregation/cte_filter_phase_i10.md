# Phase I.10 — Scalar `MIN(col)` / `MAX(col)` CTE via DESC/ASC index + `maxRows=1`

## Status

**Shipped** in `2c5f5813f46` ("RONDB-1050: implement Phase I.10
MIN/MAX index CTE").  RonSQL detects scalar `MIN` / `MAX` CTE bodies
over a direct NOT NULL indexed column, emits an ordered index scan with
`maxRows=1`, and falls back to the baseline materialisation path for
unsupported shapes.

## Kernel reference

`testCteNdbApi.cpp` Test 19 (`testMaxValWithDescScanIndex`,
~line 5226): scalar `MAX(val)` aggregate CTE materialised through a
descending ordered index scan with `setMaxRows(1)`, exiting after
the first row per fragment.

```
WITH cte0 AS (SELECT MAX(val) AS max_val FROM cte_src)
SELECT * FROM cte0;
```

Conceptual tree shape:

```
Node 0: CTE 0 subtree start
Node 1: aggregate scan (scanIndex over cte_src on idx_cte_src_val,
        DESC ordering, maxRows=1, MAX(val) attached as agg-leaf)
Node 2: scanCte(0) — main SELECT root
```

The aggregate scan is one conceptual node from the planning view —
the `scanIndex` + `readTuple` self-join shape inside
`beginCteSubtree…endCteSubtree` is the materialisation contract
(emitted unchanged by the existing single-op CTE-body branch); the
operation count `queryDef->getNoOfOperations()` reports more nodes
because the subtree start markers are counted, but those are
plumbing.

The optimisation is purely in how the body's source-table scan is
shaped:

1. `qb->scanIndex(idx, srcTab, /*bound=*/nullptr, opts)` — full
   scan via an ordered index on the aggregated column.
2. `opts.setOrdering(ScanOrdering_descending)` for `MAX`,
   `ScanOrdering_ascending` for `MIN`.
3. `opts.setMaxRows(1)` — kernel closes the scan after delivering
   one row per fragment, so the cost is O(fragments) rather than
   O(N).

Each fragment delivers its single extreme row to the aggregator;
the materialised CTE merges them into one global result, which the
main `scanCte` returns to the API as a single row.

## What's missing on the RonSQL side

Phase I.9 already gave RonSQL the single-op CTE-body
`INDEX_SCAN` materialisation branch.  The two missing pieces:

1. **Detector.**  `select_cte_body_scan_config` picks the index
   based on WHERE bounds.  The MIN/MAX case has no WHERE — the
   indexed column is consumed by the aggregator instead — so a
   second helper (or an extension to the same one) needs to spot
   the shape and pick the index whose leading column matches the
   aggregated column.

2. **Emit knobs.**  The new `INDEX_SCAN` CTE-body branch builds
   `NdbQueryIndexBound` and applies a residual filter; it doesn't
   yet call `setOrdering` or `setMaxRows`.  Both knobs sit on
   `NdbQueryOptions`, and the existing emit branch already builds
   a `NdbQueryOptions rootOpts` — extending it is mechanical once
   the planner has tagged the scope with the chosen direction.

## Scope — Test 19 only

Land just the kernel-tested shape:

- CTE body has exactly one output and that output is an
  `AGGREGATE` whose function is `T_MIN` or `T_MAX`.
- The aggregate's argument is a direct column load
  (`Expr::isLoad()` true, no nested expression).
- The aggregate argument resolves to the single source table through
  `scope.column_map[agg_col_idx]`.  The detector must use the resolved
  `NdbDictionary::Column*`, not raw parser text from `m_columns`, when
  matching the aggregate argument to the index leading column.
- The aggregate argument column is `NOT NULL`.  MySQL `MIN` / `MAX`
  ignore NULL values, while a full ordered index scan with
  `maxRows=1` can stop on the first index entry before a later
  non-NULL candidate is seen.  Nullable columns therefore stay on the
  baseline scan path until RonSQL can add an explicit `IS NOT NULL`
  composition.
- The aggregate source type is already supported by the existing
  `MIN` / `MAX` CTE type-derivation path.  I.10 must not make string
  MIN/MAX appear supported ahead of F.2, and it must respect the
  DECIMAL limitations documented by F.1 / I.22.
- The body has no `GROUP BY` (scalar — already enforced by
  Phase I.17's relaxation; otherwise this isn't a single-row
  result and `setMaxRows(1)` would lose data).
- The body has no `WHERE`.  With WHERE, the kernel must keep
  scanning past rejected rows until it finds `maxRows`
  filter-passing rows, so the optimisation degrades; correctness
  is still fine but the speed win is gone.  Defer that
  composition to a follow-up phase if a use case appears.
- The source table has an online ordered index whose leading
  column matches the aggregate's argument.  No special
  preference for `PRIMARY` or named index — any matching
  ordered index works.
- The body has exactly one source op (`cp.num_ops == 1`,
  `cp.ops[0].type` is `TABLE_SCAN`).  Multi-op CTE bodies
  (joins inside the body) are out of scope.

Anything that doesn't match falls through to Phase I.9 / Phase
H baseline.

## Implementation

Three localised edits.

### 1. Detector: `select_cte_body_minmax_index`

New helper, called from `build_cte_scopes` immediately after
`select_cte_body_scan_config` (so I.9's index loading is
already done — we can reuse `scope.body_indexes`).

Pseudocode:

```cpp
void RonSQLPreparer::select_cte_body_minmax_index(
    QueryScope& scope, const CteDefinition* cte)
{
  JoinPlan& plan = scope.join_plan;
  if (plan.num_ops != 1) return;
  if (plan.ops[0].type != JoinOp::TABLE_SCAN &&
      plan.ops[0].type != JoinOp::INDEX_SCAN) return;
  if (cte->stmt->where_expression != NULL) return;
  if (cte->stmt->groupby_columns != NULL) return;

  // Exactly one output, must be MIN/MAX over a direct column load.
  const Outputs* o = cte->stmt->outputs;
  if (o == NULL || o->next != NULL) return;
  if (o->type != Outputs::Type::AGGREGATE) return;
  TokenKind fun = o->aggregate.fun;
  if (fun != T_MIN && fun != T_MAX) return;
  AggregationAPICompiler::Expr* arg = o->aggregate.arg;
  if (arg == NULL || !arg->isLoad()) return;
  Uint32 agg_col_idx = arg->getLoadIdx();

  // Resolve the aggregate argument through the prepared CTE scope.
  // Do not use raw parser text from m_columns here: aliases and
  // name-resolution details have already been normalised into
  // column_map / column_table_idx.
  const NdbDictionary::Table* tab = plan.ops[0].table;
  if (tab == NULL) return;
  if (scope.column_map == NULL || scope.column_table_idx == NULL) return;
  if (scope.column_table_idx[agg_col_idx] != 0) return;
  const NdbDictionary::Column* agg_col = scope.column_map[agg_col_idx];
  if (agg_col == NULL) return;
  if (agg_col->getNullable()) return;
  if (!minmax_index_source_type_supported(agg_col)) return;
  const char* agg_col_name = agg_col->getName();

  // Find an ordered index whose leading column matches.
  // scope.body_indexes already populated by
  // select_cte_body_scan_config; if I.9 hadn't loaded indexes
  // (no WHERE, so the helper bailed early), populate now.
  if (scope.body_indexes.size() == 0) {
    /* mirror I.9's listIndexes loop into scope.body_indexes */
  }
  const NdbDictionary::Index* chosen = NULL;
  for (Uint32 i = 0; i < scope.body_indexes.size(); i++) {
    const NdbDictionary::Index* idx = scope.body_indexes[i];
    if (idx->getNoOfColumns() == 0) continue;
    const NdbDictionary::Column* idx_col = idx->getColumn(0);
    if (idx_col == NULL) continue;
    if (strcmp(idx_col->getName(), agg_col_name) == 0) {
      chosen = idx;
      break;
    }
  }
  if (chosen == NULL) return;

  // Tag the scope so the emit branch sets ordering + maxRows.
  plan.ops[0].type = JoinOp::INDEX_SCAN;
  plan.ops[0].index = chosen;
  scope.body_minmax_kind = (fun == T_MAX)
      ? QueryScope::MinMaxKind::MAX_DESC
      : QueryScope::MinMaxKind::MIN_ASC;
}
```

Note: `select_cte_body_scan_config` (I.9) returns early when
`where_ce == NULL`, leaving `scope.body_indexes` empty.  The
detector must either:

- Move the `listIndexes` block of I.9 into a small helper
  callable from both, OR
- Re-load indexes inline.

Refactoring is cleaner — one shared helper
`load_table_indexes(tab, scope.body_indexes)`.

Add a small detector guard:

```cpp
bool minmax_index_source_type_supported(const NdbDictionary::Column* col)
```

This should mirror the currently supported CTE `MIN` / `MAX`
type-derivation rules:

- signed and unsigned integer widths are supported;
- `FLOAT` / `DOUBLE` are supported;
- scale-positive DECIMAL follows F.1's `DOUBLE` widening;
- scale-zero DECIMAL is supported only when I.22's 64-bit range guard
  accepts the declaration;
- CHAR / VARCHAR / LONGVARCHAR and other non-numeric types are not
  supported in I.10.

This helper is only a detector guard.  The normal aggregate compiler
and virtual-table type derivation remain the authoritative type checks.

### 2. `QueryScope` extension

```cpp
enum class MinMaxKind : uint8_t { NONE = 0, MIN_ASC, MAX_DESC };
MinMaxKind body_minmax_kind = MinMaxKind::NONE;
```

(Ship as a small enum on `QueryScope`; reuse the same pattern
elsewhere if a similar tag is needed later.)

### 3. Emit branch tweak

Inside the existing `cp.num_ops == 1 && cp.ops[0].type ==
JoinOp::INDEX_SCAN` branch added in Phase I.9:

```cpp
if (cs.body_minmax_kind != QueryScope::MinMaxKind::NONE) {
  // Skip bound construction — full DESC/ASC scan via the index.
  rootOpts.setOrdering(
      cs.body_minmax_kind == QueryScope::MinMaxKind::MAX_DESC
        ? NdbQueryOptions::ScanOrdering_descending
        : NdbQueryOptions::ScanOrdering_ascending);
  rootOpts.setMaxRows(1);
  cteOpDefs[0] = qb->scanIndex(idx, srcTab, /*bound=*/nullptr,
                                &rootOpts);
} else {
  /* existing I.9 bound + residual filter logic */
}
```

The `setMatchType(MatchNonNull)` + aggregator-on-leaf tail
identical to I.9 / TABLE_SCAN paths.

### 4. Result projection / scalar emit

Phase I.17 already handles scalar-aggregate CTE main-side
delivery (`scanCte` over the materialised CTE; kernel ships the
single accumulator entry per node and DBTC merges into one
result row).  No changes there — the only thing different is
how the body's source scan is shaped.

## Test plan

`mysql-test/suite/ronsql/t/ronsql_cte_minmax_index.test`:

| Test | Shape | Notes |
|------|-------|-------|
| 1 | `WITH t AS (SELECT MAX(val) AS m FROM tab) SELECT m FROM t` | core Test 19 shape; `val` must be `NOT NULL` |
| 2 | Same with `MIN(val)` instead of `MAX` | ASC ordering path; `val` must be `NOT NULL` |
| 3 | `MAX(val)` over a column **without** an ordered index | falls through, still correct |
| 4 | `MAX(val) ... GROUP BY g` | rejected by detector — Phase I.9 path |
| 5 | `MAX(expr)` where `expr` is an arithmetic expression, not a direct column | rejected by detector — Phase I.9 path |
| 6 | Scalar `MAX(val)` cross-joined to a scalar `MIN(val2)` | composes with Phase I.17's two-CTE cross-join |
| 7 | `MAX(nullable_val)` with an ordered index and at least one NULL plus one non-NULL value | detector must not use maxRows=1 path; result still matches MySQL |
| 8 | `MAX(name)` over indexed VARCHAR | detector must not use I.10 path; reject or fall back according to existing F.2 status |
| 9 | `MAX(dec_big)` over unsafe scale-zero DECIMAL from I.22 | clear prepare-time rejection, not I.10 tagging |

For all green tests, verify result correctness against mysql.

Also verify plan shape for the positive optimisation tests.  A pure
MySQL-vs-RonSQL result diff is not enough because a failed detector can
fall back to the baseline table scan and still return the right value.
Add one of:

- an `EXPLAIN` / RonSQL plan-output assertion that the CTE body root is
  `INDEX_SCAN` and includes the I.10 `MIN_ASC` / `MAX_DESC` tag;
- a debug/trace line emitted only when `body_minmax_kind != NONE`;
- a focused test hook/counter that records the chosen CTE-body root
  shape.

The final MTR should prove both result correctness and that Tests 1 /
2 actually selected `scanIndex + ordering + maxRows=1`.

## Risks

1. **`maxRows` semantics across fragments.**  `setMaxRows(1)` is
   per-fragment, so the kernel returns one candidate row per
   fragment to the aggregator, and the merge picks the global
   extreme.  Verified by Test 19 — no extra work needed.
2. **Aggregator output type.**  `MIN`/`MAX` widening is already
   handled by Phase I.6 F.1 (DECIMAL → BIGINT/Bigunsigned/DOUBLE)
   and the existing integer-type passthrough.  Phase I.10 doesn't
   change types.
3. **Nullable indexed columns.**  Deferred.  The I.10 detector should
   require `NOT NULL` aggregate arguments.  Nullable MIN/MAX over an
   ordered index needs an explicit `IS NOT NULL` composition before
   `maxRows=1` is safe.
4. **WHERE composition.**  Deferred — see Scope.  If a use case
   appears, the next phase wires the I.9 bound construction +
   the I.10 ordering/maxRows knobs together.  The kernel handles
   it; only RonSQL needs to drop the "no WHERE" gate.
5. **Selectivity heuristics.**  None.  The detector picks the
   first matching index; there's no penalty if multiple ordered
   indexes exist with the same leading column (rare).

## Recommendation

Ship I.10 as a single commit on top of I.9.  Total surface:

- One new helper (`select_cte_body_minmax_index`).
- One new `QueryScope` field (`body_minmax_kind`).
- One small refactor (extract `load_table_indexes` helper from
  `select_cte_body_scan_config` so the detector can share it).
- One fork in the I.9 INDEX_SCAN emit branch.
- One MTR file with result checks, plan-shape checks for the positive
  optimisation cases, and guardrail cases for nullable columns,
  unsupported string MIN/MAX, and unsafe DECIMAL precision.

Estimated diff: ~150 LOC.  No kernel changes, no NDB-API
changes — pure RonSQL.  Test 19 already validates the kernel
side.
