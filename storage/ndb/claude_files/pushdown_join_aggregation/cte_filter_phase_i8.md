# Phase I.8 — RonSQL: Test 17 shape (readTuple root + CTE_LOOKUP child, no aggregation)

## Why I.8 turned out bigger than the index doc said

The Phase I index framed I.8 as a planner-coverage check.  It's not.
The plan and emit sides already produce the right shape (the
readTuple-root gate at `RonSQLPreparer.cpp:5612` rejects only
`INDEX_SCAN` / `TABLE_SCAN` children, not `CTE_LOOKUP`; the I.16a
key-count guard fires correctly on the CTE_LOOKUP child).

The actual gap is RonSQL's "aggregate-only" assumption: any join
query without aggregation hits `"This query has no aggregate
expression …"` at `RonSQLPreparer.cpp:549` unless the narrow Phase
E.3 carve-out applies (`from_is_cte && !has_joins`).  The kernel +
NDB API have always supported joins without aggregation —
testCteNdbApi.cpp Test 17 builds exactly this shape — but the
RonSQL client side was never wired for it.

## Scope — Test 17 only

Land just the shape that testCteNdbApi.cpp Test 17 already
validates kernel-side:

```
WITH cte0 AS (SELECT grp, SUM(val) AS total
              FROM cte_src GROUP BY grp)
SELECT cte_src.pk, cte_src.grp, cte_src.val,
       cte0.grp, cte0.total
FROM cte_src
JOIN cte0 ON cte0.grp = cte_src.grp
WHERE cte_src.pk = <const>;
```

Concretely:
- root = real-table `readTuple` (full PK cover) — already supported
- child = `CTE_LOOKUP` with linkedValue from root — already supported
- INNER JOIN — already supported by existing emit
- no aggregation in outer SELECT — **the new bit**
- projection-only outputs (plain column refs) — same as Phase E.3

Out of scope for I.8:
- Real-table root via SCAN (no PK cover) — defer until a kernel
  test pins down the per-row delivery shape we expect.
- `FROM cte JOIN real_table` — direction-flipped Test 17.
  Defer to its own follow-up so changes stay reviewable.
- ORDER BY / LIMIT / DISTINCT / HAVING — keep current rejections.
- Real-table-only no-aggregate queries — that's
  `ronsql_join_phase7.md` territory, independent of this work.

When we hit a new kernel-tested shape, that's its own follow-up
phase — same cadence as I.16a → I.16b/c.

## Implementation

Only three edits.

### 1. Front-end gate (RonSQLPreparer.cpp:543-554)

Today:
```cpp
bool projection_only_cte_scan =
    (from_is_cte && all_column_outputs && !has_groupby &&
     !has_having && !has_orderby && !has_limit && !has_joins);
```

For I.8: also accept the case where `from_is_real_table` and the
joins list contains exactly one entry that resolves to a CTE
(matches `cte_list`).  Conservative: a single CTE join, no chained
joins yet.

```cpp
bool single_cte_join_to_real_root =
    (!from_is_cte) &&
    has_joins &&
    /* exactly one join, target is a CTE name */;
bool projection_only =
    all_column_outputs && !has_groupby && !has_having &&
    !has_orderby && !has_limit &&
    (projection_only_cte_scan || single_cte_join_to_real_root);
```

Keep the existing reject path for everything else.

### 2. emit_child_ops invocation (RonSQLPreparer.cpp:5204)

Drop the `if (m_is_aggregate_query)` guard so children are
emitted in the no-aggregate path too.  emit_child_ops's
aggregator-attach blocks are already gated on
`singleAgg != NULL` / `leafAggs[i] != NULL`, so they no-op
naturally when the caller passes nullptr.

```cpp
emit_root_op(qb, m_main_scope, opDefs, main_singleAgg, cteVirtualTables);
if (m_main_scope.join_plan.num_ops > 1) {
  emit_child_ops(qb, m_main_scope, opDefs,
                 m_is_aggregate_query ? &singleAgg : nullptr,
                 m_is_aggregate_query ? leafAggs : nullptr,
                 cteVirtualTables);
}
```

### 3. execute_passthrough_drain — multi-op getValue

Today it hardcodes one root op + virt-table column lookups.
Generalize to walk per-output and route to the right op via
`m_main_scope.column_table_idx`:

```cpp
Uint32 numCteSubtreeOps = query->getNoOfOperations()
                       - m_main_scope.join_plan.num_ops;

for each output:
  Uint32 plan_op_idx = column_table_idx[col_idx];
  NdbQueryOperation* op =
      query->getQueryOperation(numCteSubtreeOps + plan_op_idx);
  const JoinOp& jop = m_main_scope.join_plan.ops[plan_op_idx];

  if (jop.type == CTE_LOOKUP || jop.type == CTE_SCAN) {
    Uint32 cte_col_idx = column_attrId_map[col_idx];
    const NdbDictionary::Column* vcol =
        cteVirtualTables[plan_op_idx]->getColumn(cte_col_idx);
    attrs[i] = op->getValue(vcol);
  } else {
    // Real-table op
    const NdbDictionary::Column* col =
        m_main_scope.column_map[col_idx];
    attrs[i] = op->getValue(col);
  }
```

Helper signature changes to take `cteVirtualTables` instead of
the single `root_virt`.  Update the lone caller at
`RonSQLPreparer.cpp:5232`.

## Test plan

`mysql-test/suite/ronsql/t/ronsql_cte_pk_join.test` (already drafted
upstream of this phase).  Five cases:

1. `WHERE pk_real = const`, INNER JOIN to single-PK CTE — basic
   Test 17 shape.
2. Same as 1 + WHERE inside the CTE body (composes with Phase B.2b).
3. `pk_real = const` finds no row — MatchNonNull on root.
4. Join column has no CTE match — INNER JOIN drops the row.
5. Mixed projection (real-table + aggregate-output columns) —
   exercises multi-op getValue dispatch.

## Risks

1. **`column_map` NULL for CTE refs.**  The existing E.3 path never
   calls `column_map[col_idx]` because all outputs are CTE refs.
   Multi-op path will hit it for real-table refs.  Verify
   `load_join` populates `column_map` non-NULL for real-table
   columns; spot-check the AST→scope wiring.
2. **`numCteSubtreeOps` derivation.**  Assumes CTE subtree ops are
   appended in full before main ops with nothing interleaving.
   True today (loop at line ~5046 runs first); verify nothing
   else lands in between for the specific Test 17 shape.

## Follow-ups (not in this phase)

- `FROM cte JOIN real_table` direction.
- Multiple-table joins involving multiple CTEs.
- ORDER BY / LIMIT / non-trivial WHERE on the no-aggregate path.

Each one ships as its own commit when a kernel test or concrete
use case pins down the expected shape.
