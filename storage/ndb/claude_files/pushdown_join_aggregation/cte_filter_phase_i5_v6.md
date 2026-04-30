# Phase I.5 v6 — CTE linked-vs-linked GREATEST / LEAST tests

**Planned.**  This phase captures the CTE linked-vs-linked runtime
coverage that was intentionally removed from Phase I.5 v2a.

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
