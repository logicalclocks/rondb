# CTE Filter Phase E.3 — projection-only main SELECT over CTE_SCAN root

## Context

After Phase E.1 (`scanCte` as main-query root with aggregation),
Phase E.2 (chained CTEs), and Phase E.1K (kernel virt-col linked-attr
support), the only remaining gap in the CTE feature surface is
projection-only main SELECTs over a CTE — e.g.

```sql
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_amt) AS t
  FROM cte_orders GROUP BY o_custkey)
SELECT k, t FROM sums;
```

Today RonSQL rejects this at `RonSQLPreparer.cpp:449-457` with
`"Not an aggregate query."` because every code path from `prepare()`
through `execute_join()` and `ResultPrinter::print_result()` assumes
an outer `NdbAggregator`. The kernel + NDB API already support
`scanCte` without `setAggregation()` (`testCteNdbApi.cpp` Test 8) —
the gap is purely in RonSQL's client-side execution and result
delivery.

The broader "lift the aggregation requirement for any query shape"
work is tracked as Phase 7 (steps 45a-d) in `ronsql_join_phase7.md`
and is independent. This phase covers the narrowly-scoped CTE case
only: pass-through row delivery from a `scanCte` root, no GROUP BY,
no aggregates, no joined children.

## Approach

Add an `m_is_aggregate_query` flag set during `compile()`, and route
projection-only CTE_SCAN-root queries through a new pass-through
result-delivery path. Keep all existing aggregating queries on the
existing path with no behavioral change.

### Step 1 — Flag the rejection

In the existing rejection block at `RonSQLPreparer.cpp:449-457`:
1. Set `m_is_aggregate_query = (has_aggregate_outputs ||
   has_having_aggregates || has_subquery_agg_outputs)`.
2. If `!m_is_aggregate_query`, *also* check whether the query is the
   narrowly-supported shape: main plan has exactly one op, that op
   is `JoinOp::CTE_SCAN`, no GROUP BY, no HAVING, no ORDER BY, no
   LIMIT (LIMIT/ORDER-BY land in Phase H or step 45d). If yes, allow.
   Otherwise, throw the existing `"Not an aggregate query."` error
   with an updated message pointing at Phase 45 for general support.

The existing aggregator construction (`AggregationAPICompiler` in the
parser, line 8246) keeps running unconditionally — removing it would
ripple through dozens of unguarded `m_main_scope.agg->...`
dereferences elsewhere. Instead, just leave `m_main_scope.agg`
allocated but unused for projection-only queries.

### Step 2 — Skip aggregator wiring in `execute_join()`

In `execute_join()` (around lines 4464-4667), gate the aggregator-only
work on `m_is_aggregate_query`:
- `programAggregator_join()` and `singleAgg.Finalize()` — skipped.
- `emit_root_op` and `emit_child_ops` are *already* tolerant of
  `singleAgg=NULL` for the CTE_SCAN-root path; pass NULL when
  `!m_is_aggregate_query`.
- The CTE-subtree per-CTE loop (CTE materialization) keeps its own
  aggregator path — CTE bodies still aggregate; only the outer
  SELECT is projection-only.

### Step 3 — Pass-through result delivery

Add a new private method `RonSQLPreparer::execute_passthrough_drain()`
that runs after `qb->prepare()` and `m_trans->execute(NoCommit)`:
1. Resolve the root `NdbQueryOperation*` from the prepared query.
2. For each output in `m_context.ast_root.outputs` (all `COLUMN` type
   for projection-only): call `op->getValue(col_name)` to get an
   `NdbRecAttr*`. Store in an arena-allocated array.
3. Loop `query->nextResult(true)` until `NextResult_scanComplete`. On
   each `NextResult_gotRow`, hand the `NdbRecAttr` array off to a new
   `ResultPrinter::print_passthrough_row(...)` method.

Replace the existing `m_resultprinter->print_result(resultAgg, ...)`
call with a dispatch: `m_is_aggregate_query` → existing path,
otherwise → pass-through drain.

### Step 4 — `ResultPrinter::print_passthrough_row`

Add small new methods to `ResultPrinter`:

```cpp
void print_passthrough_header(std::ostream& out);
void print_passthrough_row(const NdbRecAttr* const* attrs,
                           Uint32 num_cols,
                           std::ostream& out);
void print_passthrough_finish(std::ostream& out);
```

Reuses the existing `m_json_output / m_tsv_output / m_quote /
m_null_representation` configuration. Header emission (column-name
row in TEXT mode, `[` in JSON mode). Per-row formatting from
`NdbRecAttr` (NULL via `isNULL()`, value via the appropriate
typed accessor + charset). Finish closes JSON `]` if needed.

### Step 5 — Tests

Add to `mysql-test/suite/ronsql/t/ronsql_cte_scan.test`:

**Test 10 (E.3): projection-only over a CTE_SCAN root**
```sql
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_amt) AS t
  FROM cte_orders GROUP BY o_custkey)
SELECT k, t FROM sums;
```

**Test 11 (E.3): projection-only with WHERE filter on CTE output**
```sql
WITH sums AS (
  SELECT o_custkey AS k, SUM(o_amt) AS t
  FROM cte_orders GROUP BY o_custkey)
SELECT k, t FROM sums WHERE t > 100;
```

Use `--sorted_result` (no ORDER BY support yet).

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — rejection gate,
  conditional aggregator path in `execute_join`, new
  `execute_passthrough_drain()` helper.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` — `m_is_aggregate_query`
  flag, helper declaration.
- `storage/ndb/src/ronsql/ResultPrinter.cpp` /
  `storage/ndb/src/ronsql/ResultPrinter.hpp` —
  `print_passthrough_row` / `_header` / `_finish`.
- `mysql-test/suite/ronsql/t/ronsql_cte_scan.test` — Tests 10, 11.
- `mysql-test/suite/ronsql/r/ronsql_cte_scan.result` — re-record.

## What we're not doing

- **General non-aggregate support** (real-table scans, joins,
  subqueries) — Phase 7 / step 45 in `ronsql_join_phase7.md`, much
  larger scope.
- **ORDER BY / LIMIT in the projection-only path** — both require
  buffering or kernel-side support; revisit in Phase H or with step
  45d.
- **HAVING in projection-only** — projection-only by definition has
  no HAVING (no aggregates to filter on).
- **Removing `AggregationAPICompiler` allocation for non-agg queries**
  — leaves it allocated-but-unused to avoid touching ~10 unguarded
  dereference sites; pure scoping decision.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
./mtr --record --suite=ronsql ronsql_cte_scan
./mtr --suite=ronsql                   # full suite — no regressions
./mtr --suite=ronsql ronsql_cte_basic  # parent-table GB still green
```

The pass-through path is new code; existing aggregating queries take
the unchanged path. Risk is contained to:
- Result-delivery formatter (new but small).
- Conditional gate in `execute_join` (one branch).

If Tests 10/11 pass and existing tests stay green, ship as Phase E.3.
