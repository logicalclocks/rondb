# CTE filter Phase J — LEFT JOIN to INNER promotion when WHERE rejects NULL

## Status

**Plan only.**  No code yet.

## Context

Phase H.3 Test 21 surfaced a RonSQL/MySQL divergence on this shape:

```sql
WITH sums AS (SELECT k, SUM(v) AS t FROM tab GROUP BY k)
SELECT sums.k, SUM(sums.t)
FROM real_tab AS c LEFT JOIN sums ON sums.k = c.id
WHERE sums.t > 50
GROUP BY sums.k;
```

For a parent row with no CTE match, the API/DBSPJ NULL-injects the
unmatched child columns to `(NULL, NULL)`.  MySQL evaluates
`NULL > 50` as NULL (not TRUE) and drops the row in WHERE.
RonSQL keeps the NULL row in the main aggregator.

Root cause: RonSQL pushes the WHERE down to `CTE_LOOKUP` server-side
via `emit_cte_lookup_filter`.  Unmatched parents never trigger
CTE_LOOKUP, so the filter doesn't see the NULL row.  After
NULL-injection the API delivers it to the main aggregator, which has
no equivalent post-join filter.

The same gap applies to non-CTE LEFT JOIN children with a WHERE
filter on a child column — though the user-visible blast radius today
is dominated by CTE_LOOKUP because that's the path Phase C/D2 wired
WHERE pushdown for.  The fix presented here is general, not
CTE-specific.

## Goal

Adopt the standard MySQL optimizer rule: a `LEFT OUTER JOIN` of `A`
and `B` can be reduced to `INNER JOIN` when the WHERE clause contains
a **null-rejecting** predicate over `B`.  After promotion, RonSQL
emits `MatchNonNull` on the child instead of `MatchAll` (the default),
and DBSPJ never NULL-injects unmatched parent rows — semantically
identical to MySQL's behaviour.

A predicate is null-rejecting if it evaluates to FALSE/NULL whenever
any of its column references on the RHS is NULL.  All RonSQL
pushdown WHERE conjuncts today are null-rejecting:

- Single-column equality / inequality / range comparisons against a
  constant or another column (`= != < <= > >=`) — `NULL OP val`
  evaluates to NULL.
- Top-level AND of any number of such conjuncts.
- Cross-table comparisons in the cross_table_where_filters path —
  also null-rejecting in their column references.

There is no `IS NULL` / `IS NOT NULL` / OR-with-IS-NULL in the
pushdown grammar today.  When such constructs are added, the
promotion check has to gate on null-rejection more carefully — see
"What we're not doing" at the end.

## Design

The fix is a single pass in `RonSQLPreparer::prepare()` (and in
`build_cte_scopes` for CTE bodies that contain LEFT JOINs), inserted
**after** `classify_where_by_table` populates
`scope.join_where_ce[t]` and the cross-table filter list, and
**before** any code consumes `match_type`.

```cpp
// LEFT-to-INNER promotion: a non-empty single-table WHERE conjunct
// on a LEFT_OUTER child rejects NULL by construction (all current
// pushdown grammar is null-rejecting), so the LEFT JOIN is
// equivalent to INNER JOIN.  Promote here so emit uses
// MatchNonNull, which avoids DBSPJ NULL-injection that would
// otherwise leak past the pushed-down filter.
for (Uint32 t = 1; t < scope.join_plan.num_ops; t++)
{
  JoinOp& op = scope.join_plan.ops[t];
  if (op.match_type == JoinOp::LEFT_OUTER &&
      scope.join_where_ce[t] != NULL)
  {
    op.match_type = JoinOp::INNER;
  }
}

// Same predicate applies to cross-table filters that reference the
// LEFT_OUTER child's column.  ctf.child_table_idx is the deeper of
// the two tables (the LHS planning layer); promote that op.
for (Uint32 i = 0; i < scope.cross_table_where_filters.size(); i++)
{
  const CrossTableFilter& ctf = scope.cross_table_where_filters[i];
  JoinOp& op = scope.join_plan.ops[ctf.child_table_idx];
  if (op.match_type == JoinOp::LEFT_OUTER)
  {
    op.match_type = JoinOp::INNER;
  }
}
```

That's the entire change.  The promotion has to run on every scope
where WHERE classification ran (main scope + each CTE body scope).

### Where it fires in `prepare()` and `build_cte_scopes`

- `RonSQLPreparer::prepare()` — after the
  `classify_where_by_table(m_main_scope, ...)` call at
  `RonSQLPreparer.cpp:1198` and after the
  `cross_table_where_filters` push-back at line 1338.  Insert
  before the linked-projection / aggregator-construction blocks
  that follow (none of them read `match_type` for INNER vs LEFT
  decisions yet — verified via grep: `match_type` is only consumed
  at `emit_child_ops` line 5877 and the CTE_SCAN-as-outer-join
  rejection at `validate_cte_execution_shapes` line 2956).
- `RonSQLPreparer::build_cte_scopes` — symmetric.  The CTE body's
  classify call is at line 2862; promote each CTE scope's ops the
  same way.

### Why it's safe

1. The promotion is a strict refinement of LEFT JOIN: every row that
   INNER JOIN produces is also produced by LEFT JOIN.  Promotion only
   eliminates rows that would have been dropped by WHERE anyway
   (because their child columns are NULL and the WHERE rejects NULL).
2. RonSQL's pushdown grammar today contains no null-tolerant
   predicates (no `IS NULL`, no `OR col IS NULL`, no
   `COALESCE(col, ...) > val`).  Every `join_where_ce[t]` and every
   `cross_table_where_filters[i].ce` is null-rejecting in its
   column references, so the promotion is unconditionally correct.
3. `validate_cte_execution_shapes` (Phase G) rejects CTE_SCAN as
   outer-join child *before* the promotion would even need to run on
   such a shape — the planner only ever produces CTE_LOOKUP for
   CTE children today.
4. After promotion, `emit_child_ops` already takes the
   `setMatchType(MatchNonNull)` branch (`RonSQLPreparer.cpp:5888`).
   No other code path needs adjustment.

### Why not a post-join filter on the main aggregator instead

A post-join filter would also work and is more general (no NULL
semantics to reason about), but it's strictly more expensive: every
matched row pays the filter cost twice (once at the kernel, once at
the API/aggregator), and the kernel push-down would lose the
ability to suppress the row early.  Promotion to INNER avoids both
costs and matches the standard MySQL optimizer behaviour.

## Test plan

Reinstate the dropped Test 21 from `ronsql_cte_basic.test`, plus
two additional shapes:

| Test | Shape | Notes |
|---|---|---|
| 1 | `LEFT JOIN sums ON ... WHERE sums.t > 50` | the H.3 Test 21 shape; promotion to INNER |
| 2 | `LEFT JOIN sums ON ... WHERE sums.k != 100` | promotion when WHERE is on the GB key (not aggregate output) |
| 3 | Two LEFT JOINs (`LEFT JOIN cte1 LEFT JOIN cte2`) with WHERE on cte1 only | only cte1 promoted; cte2 stays LEFT |

The third test is important — it verifies the promotion is per-op,
not whole-plan.

After ship, the H.3 follow-up note in `ronsql_cte_basic.test`
(currently saying "deferred — see project memory") gets removed and
the test is renumbered or kept as Phase J Test 1.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — promotion pass after
  `classify_where_by_table` in `prepare()` and `build_cte_scopes`.
  Add a small helper `promote_left_to_inner_for_where(QueryScope&)`
  to keep both call sites consistent.
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — restore the
  deferred Test 21 and add Tests 2, 3 from above.  Update the
  comment block.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- This doc.
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md` —
  add `cte_filter_phase_j.md` to the index.
- `storage/ndb/claude_files/pushdown_join_aggregation/ronsql_cte_plan.md`
  — note Phase J landing.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql                # full suite — no regressions
./mtr --suite=ndb_push_agg          # block tests — no regressions
```

Specifically verify:
- Existing LEFT JOIN tests (basic Test 10, basic Test 14, scan
  Phase E.2 Test 8 if it exercises LEFT JOIN) still produce the
  same results — those have no WHERE on the RHS, so no promotion
  fires.
- The dropped H.3 Test 21 now gives the same 3-row result as MySQL.

## What we're not doing

- **`IS NULL` / `IS NOT NULL` in WHERE.**  Not in the pushdown
  grammar today.  When added, the promotion check needs to skip
  conjuncts where the WHERE explicitly preserves NULLs (`col IS
  NULL`) and only fire when at least one conjunct rejects NULL.
- **Multi-level LEFT JOIN chains with WHERE on a deeper node.**
  Standard MySQL semantics also promote the intermediate LEFT JOINs
  in the chain (because if the deepest child must match, every
  ancestor in the LEFT chain must also match).  This phase only
  promotes the directly-WHERE'd op.  A follow-up phase can add the
  cascade if it surfaces; for now the user gets the benefit on the
  common single-level case.
- **WHERE conjuncts that span multiple tables via OR.**  Already
  rejected by `classify_where_by_table` for cases that don't fit
  the `cross_table_where_filters` shape.  No new constraint here.

## Risks

1. **Existing LEFT JOIN test results.**  Any LEFT JOIN test that
   has a WHERE on the RHS *and* depends on NULL-injected rows being
   delivered will diverge.  Such a test would already be diverging
   from MySQL today, so a recorded `.result` mismatch is more likely
   to be a pre-existing latent issue than a regression.  Re-record
   any divergence and audit before accepting.
2. **Cross-table filter promotion picks `child_table_idx`.**  The
   classification at `RonSQLPreparer.cpp:1330-1333` defines
   `child_t = max(tables_seen[0], tables_seen[1])`.  In a LEFT JOIN
   chain, this is the deeper op — exactly the one whose
   `match_type` we want to promote.  Confirm by spot-check during
   implementation that no cross-table filter routes a WHERE on a
   parent column to the child slot.
3. **Promotion ordering vs other planner passes.**  Promotion must
   land *after* `classify_where_by_table` and *before*
   `emit_child_ops`.  No other promote-style transforms exist
   today, so insertion order is unambiguous.
