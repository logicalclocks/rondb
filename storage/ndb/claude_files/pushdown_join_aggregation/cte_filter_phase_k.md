# CTE filter Phase K — ANTI_JOIN promotion for `WHERE col IS NULL` on LEFT JOIN RHS

## Status

**Plan only.**  No code yet.

## Context

Phase I.1 closed `IS NULL` / `IS NOT NULL` for INNER JOIN but
defensively rejected `WHERE col IS NULL` on a LEFT JOIN's RHS
column.  Reason: the kernel filter pushdown rejects matched rows
(CTE columns are non-NULL on the matched path), and the API can't
distinguish "rejected match" from "no match" — so all parents end
up NULL-injected and the result mixes rejected matches with
genuine unmatched rows.

The user-visible idiom — "find unmatched parents" via
`LEFT JOIN ... WHERE rhs.col IS NULL` — still falls into a clean
`require_prm` rejection today.  Phase K closes it.

## Goal

Detect the canonical LEFT-anti-join idiom and emit it as an
**anti-join** (NDB API `MatchNullOnly`) instead of pushing the
WHERE down to CTE_LOOKUP.  `MatchNullOnly` instructs DBSPJ to
deliver only the NULL-padded unmatched rows — exactly the user's
intent — with no kernel filter involvement.

The infrastructure is already in place:

- `JoinOp::MatchType` enum has `ANTI_JOIN`
  (`QueryPlanner.hpp:45`).
- `emit_child_ops` already maps `JoinOp::ANTI_JOIN` →
  `setMatchType(MatchNullOnly)` (`RonSQLPreparer.cpp:5988-5989`).
- `EXPLAIN` already prints `[ANTI]` for the case
  (`RonSQLPreparer.cpp:7942`).

What's missing is the **detection pass**: nothing currently sets
`match_type = ANTI_JOIN`.

## Design

### Detection pass

Add to `RonSQLPreparer::promote_left_to_inner_for_where` (or a
companion pass right after it).  For each `LEFT_OUTER` op `t`:

1. If `is_null_rejecting(scope.join_where_ce[t])` is true:
   already promoted to `INNER` by Phase J — no change.
2. Else if `is_anti_join_promotable(scope, t,
   scope.join_where_ce[t])` is true: set
   `op.match_type = JoinOp::ANTI_JOIN` and clear
   `scope.join_where_ce[t] = NULL` (the `MatchNullOnly` itself
   implements the filter; emit_cte_lookup_filter must not run on
   this op).
3. Else: leave as `LEFT_OUTER`.  emit_cte_lookup_filter will hit
   the I.1 defensive reject if any IS NULL conjunct remains.

### `is_anti_join_promotable` helper

The promotion is safe only when **every conjunct is `IS NULL` on a
CTE column that's provably non-NULL on the matched path**:

- **CTE GB key (`Outputs::Type::COLUMN`):** never NULL by
  construction — rows with NULL GB key don't aggregate into a
  group at all.
- **SUM / COUNT aggregate output (`Outputs::Type::AGGREGATE` with
  `fun ∈ {T_SUM, T_COUNT}`):** never NULL on a non-empty group
  (the only path that produces a CTE row).
- **MIN / MAX aggregate output:** **NOT safe.**  `AggResItem.is_null`
  is set to true when every source value in the group is NULL,
  yielding a legitimately-NULL aggregate output for a *matched*
  group.  `IS NULL` should match such groups in MySQL semantics,
  but `MatchNullOnly` would skip them (they're matched).  Phase K
  leaves MIN/MAX IS NULL on LEFT JOIN under the existing I.1
  defensive reject.

```cpp
static bool
is_anti_join_promotable(const QueryScope& scope, Uint32 op_idx,
                        const ConditionalExpression* ce)
{
  if (ce == NULL) return false;
  if (ce->op == T_AND) {
    return is_anti_join_promotable(scope, op_idx, ce->args.left) &&
           is_anti_join_promotable(scope, op_idx, ce->args.right);
  }
  if (ce->op != T_IS || !ce->is.null) return false;

  const ConditionalExpression* col_side = ce->is.arg;
  if (col_side == NULL || col_side->op != T_IDENTIFIER) return false;
  Uint32 col_idx = col_side->col_idx;
  if (scope.column_table_idx[col_idx] != op_idx) return false;

  // Walk the CTE outputs to the referenced column and check it's
  // a GB-key column or a SUM/COUNT aggregate.
  const JoinOp& cte_op = scope.join_plan.ops[op_idx];
  if (cte_op.cte_def == NULL) return false;  // not a CTE — be safe
  Uint32 cte_col_idx = (Uint32)scope.column_attrId_map[col_idx];
  Uint32 walk = 0;
  const Outputs* o = cte_op.cte_def->stmt->outputs;
  while (o != NULL && walk < cte_col_idx) { o = o->next; walk++; }
  if (o == NULL) return false;

  if (o->type == Outputs::Type::COLUMN) return true;
  if (o->type == Outputs::Type::AGGREGATE) {
    TokenKind fun = o->aggregate.fun;
    return fun == T_SUM || fun == T_COUNT;
  }
  return false;
}
```

### Why we clear `join_where_ce[t]`

`emit_cte_lookup_filter` runs per child op when
`scope.join_where_ce[i] != NULL` (`RonSQLPreparer.cpp:5973`).  For
ANTI_JOIN the WHERE is implicit in `MatchNullOnly`; pushing the
IS NULL conjunct down would do nothing useful (CTE_LOOKUP doesn't
even fire for unmatched parents) and would still incur the
DBTUP-side filter program emit cost.  Clearing the conjunct keeps
the emit path clean.

### Cross-table filters

`promote_left_to_inner_for_where` also iterates
`cross_table_where_filters`.  Cross-table filters reference two
tables; if one of those is a CTE on the LEFT_OUTER side and the
condition is `cte.col IS NULL`, in principle the same promotion
could apply.  But cross-table filters today carry comparison
operators only (= != < <= > >=, see `classify_where_by_table`
line 1294-1298) — no `T_IS`.  So this case can't arise with the
current pushdown grammar.  Leave the cross-table loop unchanged.

## Test plan

Reinstate Test 28 in `ronsql_cte_basic.test` — the H.3 dropped /
I.1 deferred test — plus three companions:

| # | Shape | Expected |
|---|---|---|
| 28 | `LEFT JOIN sums ... WHERE sums.t IS NULL` | `c_id=400, NULL` (unmatched only) |
| 29 | `LEFT JOIN sums ... WHERE sums.k IS NULL` | same — IS NULL on GB key, also unmatched only |
| 30 | `LEFT JOIN sums ... WHERE sums.t IS NULL AND sums.k IS NULL` | same — AND of two IS NULL on same op, both promotable |
| 31 | `LEFT JOIN bounds ... WHERE bounds.mn IS NULL` (bounds = MIN aggregate) | **expect rejection** — MIN can be NULL on matched group, ANTI_JOIN would be incorrect; covered by I.1 defensive reject |

Test 31 should hit the existing `require_prm` from I.1 with the
"WHERE col IS NULL on a LEFT JOIN's RHS column is not yet
supported" message.  MTR can capture that with `--error 0,N` or
the `--replace_regex` patterns used elsewhere — adapt to whatever
ronsql_compare.inc supports for error queries.  If error capture
isn't wired, drop Test 31 from MTR and document the case in the
plan doc only.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`:
  - Add `is_anti_join_promotable` static helper near
    `is_null_rejecting` and `promote_left_to_inner_for_where`.
  - Extend `promote_left_to_inner_for_where` with the second
    promotion branch.
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — restore
  Test 28 and add Tests 29 / 30 (and 31 if reachable).
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
  — add `cte_filter_phase_k.md` to the index.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_i1.md`
  — update "Limitations" section: GB-key / SUM / COUNT cases
  closed by Phase K; MIN/MAX IS NULL still rejected.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql                # full suite — no regressions
./mtr --suite=ndb_push_agg          # block tests — no regressions
```

Spot-check `EXPLAIN` output (if RonSQL exposes it) for the new
shapes — should show `[ANTI]` for the promoted ops, confirming the
right promotion fired.

## Risks

1. **`MatchNullOnly` semantics for chained CTE_LOOKUP children.**
   The kernel side's `MatchNullOnly` delivers only NULL-padded
   unmatched rows.  For a chained CTE_LOOKUP under another
   CTE_LOOKUP the topology is more nuanced.  Phase K's detection
   pass triggers per-op based on `match_type == LEFT_OUTER` — same
   shape as Phase J.  No additional check needed; if the kernel
   doesn't support ANTI_JOIN inside a CTE-feeding tree, we'd see
   it at runtime and add a planner-time rejection then.  Standard
   `LEFT JOIN sums ... WHERE sums.col IS NULL` shapes don't hit
   this and pass straight through.
2. **GB key vs aggregate distinction in the helper.**  The walker
   relies on `Outputs::Type::COLUMN` for GB and
   `Outputs::Type::AGGREGATE` for aggregates.  CTE bodies in
   RonSQL today only emit those two output kinds (verified via
   the existing emit_cte_lookup_filter walker at
   RonSQLPreparer.cpp:5814-5876).  A future kind would need an
   explicit case here; default-`return false` is the safe
   fallback.
3. **Phase J + Phase K ordering.**  Phase J's `is_null_rejecting`
   returns false for `IS NULL` conjuncts, so Phase J never
   promotes a LEFT_OUTER with an IS-NULL WHERE.  Phase K runs
   after (or as a second branch in the same pass) and picks up
   the LEFT_OUTERs Phase J skipped.  Order is correct by
   construction.
4. **Multiple LEFT JOINs, IS NULL on only one.**  E.g.
   `... LEFT JOIN a ... LEFT JOIN b ... WHERE a.t IS NULL`.
   Phase K promotes only `a` to ANTI_JOIN; `b` stays
   LEFT_OUTER.  WHERE conjunct on `a` is per-op via
   `join_where_ce[a_idx]` — no cross-op leak.

## What we're not doing

- **MIN/MAX IS NULL on LEFT JOIN.**  Stays under I.1's defensive
  reject.  A future phase could lift it by combining ANTI_JOIN
  with a post-join filter that re-checks the MIN/MAX value, but
  that's strictly more complex than this phase's scope.
- **`IS NULL OR <other>` conjuncts.**  Not in the pushdown grammar;
  rejected at classify time.  Covered by Phase I.2 if/when that
  lands.
- **`IS NULL` on a non-CTE-driven LEFT JOIN (e.g. a real-table
  LEFT JOIN with no CTE involvement).**  The helper checks
  `cte_op.cte_def != NULL` — non-CTE LEFT JOINs are out of scope.
  Real-table anti-join is a separate planner concern; track if a
  user hits the existing I.1 reject on that shape.
