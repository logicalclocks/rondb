# Phase I.17 — scalar aggregate CTEs without GROUP BY

## Status

**Shipped.**  Five commits on `RONDB-1050-cte-filter`:

| Commit | Scope |
|--------|-------|
| `a2e75657e47` | I.17a — RonSQL relaxation: CTE bodies without GROUP BY accepted as long as every output is an aggregate.  Mixed non-aggregate columns without GROUP BY still rejected with a clear error |
| `cfaf99441d3` | I.17 MTR — `ronsql_cte_scalar.test` Tests 1-5: scalar MAX populated, scalar MAX / COUNT empty, multi-aggregate populated, and a negative test for non-aggregate columns without GROUP BY |
| `3aa426b3fb9` | Kernel — `JoinAggInterpreter::Init` pre-zeroes COUNT slots in scalar mode so empty input emits 0 (not NULL) for COUNT.  `cteScanEmitResults` initially relaxed the `processed_rows() > 0` precondition |
| `b4c337a7ac6` | Kernel — interim single-emitter rule via `m_cte_node_list[0]`.  Replaced by I.17e |
| `d0b9d6e9dd2` | I.17e kernel — cross-node scalar redistribute.  Owner = DBTC's data node (via `refToNode(state->m_senderRef)`).  `JoinAggInterpreter::mergeOneGroup` dispatches `keyLen == 0` to a new `mergeScalarAccumulators`.  `Dblqh::sendScalarRedistributeReq` packages `m_agg_results` into a `JoinAggRedistributeReq` with `keyLen = 0`.  `cteScanShouldEmitScalar` simplifies to `refToNode(state->m_senderRef) == getOwnNodeId()`.  Plan + what-shipped in `cte_filter_phase_i17_redistribute.md` |

After this work, `ronsql_cte_scalar.test` Tests 1-4 produce empty
`== Diff ==` blocks and Test 5 emits the new "non-aggregate
output columns and must contain GROUP BY" reject.  No regressions
in the existing grouped-CTE path.

### Phase I.17g + h + cross-join shipped (later commits)

The watermark / cross-CTE-join shape from the original plan now
works end-to-end:

| Commit | Scope |
|--------|-------|
| `53a99cb7edc` | Parser — top-level GREATEST / LEAST in `nonaliased_output` (wrapped in implicit MAX); optional FROM clause via `from_clause` non-terminal; `synthesize_from_for_scalar_ctes()` walks qualified column refs and synthesises a comma cross-join AST when the parser produces NULL `root_table` |
| `7fdb7caba65` | Re-add comma cross-join grammar rule and switch `emit_child_ops`'s scalar-CTE-cross-join child case to the working pattern from `testCteNdbApi.cpp` Test 20: `lookupCte()` with a single dummy `constValue((Int64)0)` key + `setParent(rootOp)`.  The kernel ignores the key for scalar CTEs (`n_gb_cols == 0`) and returns the materialised `m_agg_results` directly |
| `8609cad17f4` | `build_cte_virtual_tables`: scalar CTE virt tables now mark the first output column as PK (PK count == 1) so they match Test 20's `(result BIGINT PRIMARY KEY)` shape.  Without this, `lookupCte()` rejects the dummy-key array on a 0-PK virt table |

`ronsql_cte_scalar.test` Tests 6-9 cover:
- 6: comma cross-join, top-level GREATEST → `biggest=100`
- 7: comma cross-join, top-level LEAST → `smallest=10`
- 8: no-FROM auto-synthesised cross-join (RonSQL-only; recorded directly)
- 9: grouped CTE qualifier in no-FROM SELECT — rejected

Working watermark form on the user's actual query:

```sql
WITH max_update AS (SELECT MAX(update_dt) AS latest_update
                    FROM hopsworks_online_feature_store),
     max_insert AS (SELECT MAX(insert_dt) AS latest_insert
                    FROM hopsworks_online_feature_store)
SELECT GREATEST(max_update.latest_update,
                max_insert.latest_insert) AS watermark
FROM max_update, max_insert;
```

…or, equivalently, with no explicit FROM:

```sql
WITH max_update AS (...), max_insert AS (...)
SELECT GREATEST(max_update.latest_update,
                max_insert.latest_insert) AS watermark;
```

## Problem

RonSQL currently requires every CTE body to contain `GROUP BY`.  That
rejects valid scalar aggregate CTEs such as:

```sql
WITH max_update AS (
  SELECT MAX(update_dt) AS latest_update
  FROM feature_store)
SELECT latest_update
FROM max_update;
```

MySQL semantics: an aggregate query without `GROUP BY` returns one row
for the whole input, even if the input is empty.  Aggregate values then
follow normal aggregate rules (`COUNT(*) = 0`, `SUM` / `MIN` / `MAX`
return `NULL` on empty input).

This limitation blocks watermark-style queries:

```sql
WITH max_update AS (
    SELECT MAX(update_dt) AS latest_update
    FROM hopsworks_online_feature_store
),
max_insert AS (
    SELECT MAX(insert_dt) AS latest_insert
    FROM hopsworks_online_feature_store
)
SELECT GREATEST(latest_update, latest_insert) AS watermark
FROM max_update, max_insert;
```

It also blocks Phase I.5 tests that need scalar CTE inputs to
`GREATEST` / `LEAST`.

## Desired Behaviour

Support CTE bodies that have aggregate outputs and no `GROUP BY`.

The materialized CTE should behave as a one-row virtual table:

- zero key columns;
- one output column per CTE select-list item;
- exactly one result row after CTE materialization;
- aggregate values follow MySQL scalar aggregate semantics.

## Phase I.17a — clear parser / planner classification

Replace the blanket "CTE must contain GROUP BY" rejection with shape
classification:

1. CTE body has `GROUP BY`:
   existing grouped CTE path.
2. CTE body has aggregate outputs and no `GROUP BY`:
   scalar aggregate CTE path.
3. CTE body has neither aggregate outputs nor `GROUP BY`:
   reject for now, unless a separate pass-through CTE body phase
   explicitly supports it.

The scalar aggregate CTE path should reject mixed non-aggregate column
outputs without `GROUP BY`, matching MySQL's `ONLY_FULL_GROUP_BY`
expectations for RonSQL's supported subset.

## Phase I.17b — keyless virtual CTE table

Extend virtual table construction for scalar aggregate CTEs:

- no columns marked as primary key;
- `COUNT` output type remains unsigned 64-bit;
- `SUM` / `MIN` / `MAX` output type derivation follows the existing
  grouped CTE rules;
- attrIds remain synthetic 0..N-1 as in the grouped path.

Audit all uses of `getNoOfPrimaryKeys()` and key-count assumptions for
CTE virtual tables.  A keyless virtual table must not accidentally flow
into `lookupCte()`.

## Phase I.17c — execution and result delivery

Use `scanCte()` for scalar aggregate CTE consumption.

For a scalar CTE as the main root:

```sql
WITH s AS (SELECT MAX(v) AS m FROM t)
SELECT m FROM s;
```

the root operation should be `CTE_SCAN`.

For joins of scalar CTEs, either:

- support the existing join syntax once cross-join support exists, or
- initially require an explicit supported join form if RonSQL still
  lacks comma / cross join parsing.

The materialization side must ensure one row is emitted for the scalar
aggregate, including the empty-input case.

## Phase I.17d — tests

Add RonSQL MTR coverage:

1. Single scalar CTE:

```sql
WITH s AS (SELECT MAX(v) AS m FROM t)
SELECT m FROM s;
```

2. Scalar CTE with `COUNT(*)` over empty input:

```sql
WITH s AS (SELECT COUNT(*) AS n FROM t WHERE false)
SELECT n FROM s;
```

3. Scalar CTE with `MAX` over empty input:

```sql
WITH s AS (SELECT MAX(v) AS m FROM t WHERE false)
SELECT m FROM s;
```

4. Watermark shape once scalar CTE joins are supported:

```sql
WITH max_update AS (SELECT MAX(update_dt) AS latest_update FROM t),
     max_insert AS (SELECT MAX(insert_dt) AS latest_insert FROM t)
SELECT GREATEST(latest_update, latest_insert) AS watermark
FROM max_update, max_insert;
```

5. A Phase I.5 integration test using `GREATEST` / `LEAST` over scalar
   CTE outputs.

## Open Questions

- Does the current kernel CTE aggregation path emit a final row for an
  aggregate with zero group-by columns, or does it depend on at least
  one group key?  If it depends on a key, add a synthetic singleton key
  internally but do not expose it in the virtual table.
- Should keyless scalar CTEs ever be addressable by `lookupCte()`?  The
  initial answer should be no; use `scanCte()`.
- Should non-aggregate, no-`GROUP BY` CTE bodies be included?  Defer
  unless needed.  This phase is for scalar aggregate CTEs.

## Completion Criteria

- RonSQL accepts scalar aggregate CTE bodies without `GROUP BY`.
- The scalar CTE is exposed as a one-row `CTE_SCAN` result.
- Empty-input aggregate semantics match MySQL.
- Existing grouped CTE tests continue to pass.
- The watermark query shape can be tested once cross/scalar CTE joins
  are also accepted.
