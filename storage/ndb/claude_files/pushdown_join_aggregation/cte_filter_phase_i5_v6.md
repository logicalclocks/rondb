# Phase I.5 v6 — CTE linked-vs-linked GREATEST / LEAST tests

**Shipped.**  This phase captures the CTE linked-vs-linked runtime
coverage that was intentionally removed from Phase I.5 v2a.  No
production-code changes were needed: v2b's `Greatest2` / `Least2`
SVM ops handle CTE-leaf operands uniformly through the aggregation
register machine, so v6 is purely a runtime test addition.

The v6 query swaps the FROM root: `pairs` (the multi-column GROUP BY
CTE) becomes the CTE_SCAN root and `cte_customer` is the join child.
That avoids the unrelated "Failed to create child operation" path
hit when joining INTO a multi-column-GROUP-BY CTE on only one
column — that limitation is captured in the new `cte_filter_phase_i16.md`
phase.

**What shipped:**

- `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v6.test` —
  four cases: `SUM(GREATEST(pairs.k, pairs.amt))` linked-vs-linked
  with `pairs` as CTE_SCAN root, `LEAST` mirror, n=3 chain mixing
  CTE columns and a parent physical column, and a CTE COLUMN +
  CTE AGGREGATE (`COUNT(*)`) variant.
- New phases I.16 / I.17 captured separately for the unrelated
  planner gaps surfaced while writing the test.

## Motivation

Phase I.5 v2a supports column-vs-column CASE atoms and therefore
two-column `GREATEST` / `LEAST` lowering.  The physical-table variants
fit the current SPJ shapes, but the natural CTE test:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(GREATEST(pairs.k, pairs.amt)) AS gmax
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id
GROUP BY c.c_id;
```

currently places `pairs` as the aggregation leaf.  That makes
`pairs.k` and `pairs.amt` inline CTE-leaf columns, which v2a rejects
until the typed linked register-load work in Phase I.5 v5 is available.

Attempts to force `pairs` to be linked by adding a later physical child
operation reached query operation construction but failed with:

```text
Failed to create child operation.
NDB Success 0, No error, MySQL 0: No error
```

So v2a should not carry this as a runtime test.

## Test To Add

Add a passing MTR test after v5/v6 support exists:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(GREATEST(pairs.k, pairs.amt)) AS gmax
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id
GROUP BY c.c_id;
```

Expected result for the current `ronsql_cte_greatest_least_v2a.test`
fixture:

```text
c_id    gmax
100     200
200     400
300     300
```

Also add the `LEAST(pairs.k, pairs.amt)` variant if the same support
lands in a general way.

## Acceptance Criteria

- The test must pass without relying on an unsupported physical child
  operation after `CTE_LOOKUP`.
- CTE column projections used by `GREATEST` / `LEAST` must be evaluated
  with the same semantics as MySQL.
- The test should cover the linked-vs-linked CTE projection case, not
  only physical parent-table columns.
