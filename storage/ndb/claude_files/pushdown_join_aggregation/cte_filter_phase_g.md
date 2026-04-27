# CTE Filter Phase G — defensive guard for CTE_SCAN-as-outer-join-child

## Context

Phase G in the overarching plan (`ronsql_cte_plan.md`) is a single-shape
reject-cleanly guard:

> **CTE_SCAN as outer-join child.** If any outer SELECT join maps a
> CTE-named child under LEFT JOIN and the planner selects CTE_SCAN for
> that child (not CTE_LOOKUP), throw "CTE_SCAN as outer-join child not
> yet supported by NDB."

Reading the planner today (`QueryPlanner.cpp:160`), the shape is
**unreachable from SQL**: child CTE references always become
`JoinOp::CTE_LOOKUP`, never `CTE_SCAN`. `CTE_SCAN` is only emitted for
the root op (line 109).

That means the guard is purely defensive — a tripwire for any future
planner change that might select CTE_SCAN for a non-root op. Still
worth landing because:
- It's a few lines of code.
- It documents the unsupported shape inline.
- It would catch a regression long before it reaches DBSPJ.

## Plan

### Step 1 — Add the validator

Add `validate_cte_execution_shapes()` to `RonSQLPreparer`. Walks the
main scope's `JoinPlan` and every per-CTE `JoinPlan`, rejecting any
non-root op with `type == JoinOp::CTE_SCAN` and `match_type ==
JoinOp::LEFT_OUTER`. Throw `RonSQLPermanentError` with a clear
message.

### Step 2 — Wire the call

Call `validate_cte_execution_shapes()` from `prepare()` after `load()`
returns — that's when both `m_main_scope.join_plan` and every
`m_cte_scopes[c]->join_plan` are populated. Only call when CTEs exist
(`m_has_ctes`), so the cost on non-CTE queries is zero.

### Step 3 — Documentation

- Update `ronsql_cte_plan.md`: mark Phase G done, note the guard is
  defensive (no test because the shape is unreachable from SQL today).
- Update memory `project_cte_branch_state.md`: drop Phase G from open
  follow-ups.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — add the validator
  function and one call site in `prepare()`.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` — declare the function.

## What we're not doing

- **No MTR test for the guard.** The shape is currently unreachable
  from SQL — the planner never emits CTE_SCAN as a non-root op. We'd
  have to fabricate a planner state to trigger the guard. The
  defensive value is in the tripwire itself, not in test coverage.
  When/if the planner gains the ability to emit CTE_SCAN for a child,
  Phase G's guard can grow a real test alongside that work.
- **No positive test for `LEFT JOIN cte` inside a CTE body** (the
  ex-6a shape mentioned in the original Phase G plan). That's
  end-to-end coverage of an already-shipped capability and should
  land as part of Phase H (test consolidation), not the guard work.

## Verification

- `./mtr --suite=ronsql` — full suite, no regressions. The guard adds
  one cheap loop-walk per query with CTEs; no functional change for
  any shape currently produced by the planner.
