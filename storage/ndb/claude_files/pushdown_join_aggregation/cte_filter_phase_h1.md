# CTE filter Phase H.1 — filter operator + conjunct matrix

## Status

**Plan only.**  No code yet.  Independent of H.2 / H.3.

## Context

Today's CTE filter MTR coverage exercises a narrow operator subset:
`>` and `=` dominate (`ronsql_cte_basic.test` Test 9 / 11 / 12,
`ronsql_cte_scan.test` Test 2 / 3).  `emit_cte_lookup_filter` accepts
six operators (`= != < <= > >=`) and any number of top-level AND
conjuncts; testCteNdbApiFilter has Tests 6 / 7 / 8 covering this
matrix at the NDB-API level but the SQL surface isn't exercised.

H.1 closes the gap with test-only changes — appended to the two
existing CTE test files.  No production code touched.

## Tests to add

### `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` (CTE_LOOKUP path)

Append after current Test 14, using existing tables (`cte_purchase`,
`cte_customer`).

- **Test 15 — `!=` on CTE_LOOKUP key.**
  ```sql
  WITH purchase_agg AS (
    SELECT custkey, SUM(amt) AS tot
    FROM cte_purchase GROUP BY custkey)
  SELECT pa.custkey, pa.tot
  FROM cte_customer c JOIN purchase_agg pa ON c.c_id = pa.custkey
  WHERE pa.custkey != 100;
  ```
- **Test 16 — `<=` and `>=` on agg output.**
  Two queries, one each, mirroring Test 12's shape.
- **Test 17 — multi-conjunct AND on CTE output.**
  ```sql
  ... WHERE pa.custkey >= 100 AND pa.tot < 200
  ```
- **Test 18 — all-reject filter.**
  ```sql
  ... WHERE pa.tot > 1000000
  ```
  Verify zero result rows (no crash, just empty result set).

### `mysql-test/suite/ronsql/t/ronsql_cte_scan.test` (CTE_SCAN path)

Append after current Test 11, using existing `cte_orders` /
`cte_customer`.

- **Test 12 — `!=` and `<=` on CTE_SCAN agg output.**
- **Test 13 — multi-conjunct AND on CTE_SCAN root.**
  ```sql
  WITH sums AS (...)
  SELECT k, SUM(t) FROM sums WHERE k > 100 AND t < 200 GROUP BY k;
  ```
- **Test 14 — all-reject filter on CTE_SCAN.**

## Pattern

Every test follows the existing block:

```
--echo
--echo
--echo === Test N (H.1): description ===

let $QUERY=
<sql>;|
--source suite/ronsql/include/ronsql_compare.inc

--echo == Expected result ==
--sorted_result
<sql>|
```

`--sorted_result` because no ORDER BY is supported.  `ronsql_compare.inc`
runs both the RDRS path and the MySQL reference and diffs them — same
verification pattern as the existing tests.

## Files

- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — append Tests
  15-18.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- `mysql-test/suite/ronsql/t/ronsql_cte_scan.test` — append Tests
  12-14.
- `mysql-test/suite/ronsql/r/ronsql_cte_scan.result` — re-record.
- This doc.

## Verification

```
cd debug_build && ./mtr --record --suite=ronsql ronsql_cte_basic
                  ./mtr --record --suite=ronsql ronsql_cte_scan
                  ./mtr --suite=ronsql            # no regressions
```

## What we're not doing

- **OR conjuncts / col-vs-col / IS NULL** — `emit_cte_lookup_filter`
  rejects these by design; lifting requires filter-grammar work,
  separate phase.
- **MIN aggregate output filter** — covered indirectly via D2 today;
  add coverage if H.3's edge cases miss it.
