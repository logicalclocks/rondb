# CTE filter Phase H.3 — reachable edge cases

## Status

**Plan only.**  No code yet.  Independent of H.1 / H.2.

## Context

H.1 covers the filter operator/conjunct matrix and H.2 covers
multi-batch.  Remaining reachable gaps from the
testCteNdbApi*/testCteNdbApiFilter triage:

- **Multi-CTE filter isolation** — testCteNdbApiFilter Test 11
  variants.  RonSQL Test 5 in `ronsql_cte_basic.test` exercises
  two independent CTEs but neither has a per-CTE WHERE; we don't
  know that filter on one doesn't bleed into the other's path.
- **CHAR/VARCHAR GB key with WHERE** — testCteNdbApiFilter Test
  22 analogue.  RonSQL Test 7 has a VARCHAR GB column but no
  filter on it.
- **LEFT JOIN + WHERE on CTE output** — testCteNdbApiOuterJoin
  shapes.  RonSQL Tests 10 / 14 have LEFT JOIN but no surviving
  WHERE filter on the CTE side.
- **Empty CTE / single-row CTE** — boundary delivery.

H.3 ships these as test-only additions to
`ronsql_cte_basic.test`.

## Tests to add

Append after current Test 14 (or after H.1's Test 18 if H.1 lands
first; pick numbering at commit time).

- **Multi-CTE filter isolation.**
  Two independent CTEs, each with WHERE on its own agg output.
  Verify both filters apply only within their own CTE — e.g. one
  rejects half the groups, the other rejects a different half,
  and the SELECT pulls from both:
  ```sql
  WITH agg_a AS (SELECT k1, SUM(v) AS sa FROM tab1 GROUP BY k1),
       agg_b AS (SELECT k2, SUM(v) AS sb FROM tab2 GROUP BY k2)
  SELECT (SELECT SUM(sa) FROM agg_a WHERE sa > 50) AS total_a,
         (SELECT SUM(sb) FROM agg_b WHERE sb > 50) AS total_b
  FROM dual;
  ```
  (Adjust SELECT shape to whatever the existing planner already
  accepts for two-CTE main queries — Test 5 in basic is the
  template.)

- **VARCHAR GB key + WHERE filter on it.**
  Extends Test 7 (VARCHAR GB):
  ```sql
  WITH name_agg AS (SELECT c_name, SUM(amt) AS tot
                    FROM cte_purchase JOIN cte_customer ...
                    GROUP BY c_name)
  SELECT c_name, tot FROM name_agg WHERE c_name = 'Alice';
  ```

- **LEFT JOIN + WHERE on CTE side surviving the OUTER.**
  Extends Test 10 / 14 — add a WHERE on the CTE's agg output
  that the LEFT JOIN preserves on matched rows but discards the
  unmatched-NULL rows.  Mainly verifies the filter doesn't
  short-circuit before the NULL-injection.

- **Empty CTE.**
  Use a WHERE in the CTE body that matches no rows
  (`WHERE custkey = 999999`).  Verify the outer SELECT returns
  zero rows for `SUM(t)`-style queries (single NULL row for
  scalar agg, zero rows for grouped).  Mirrors `testJoinAgg`'s
  empty-input behaviour.

- **Single-row CTE.**
  WHERE in CTE body that matches exactly one input row.
  Verify the outer SELECT returns the one group as expected.

## Pattern

Same `ronsql_compare.inc` block as H.1.  Each test gets:

```
--echo
--echo
--echo === Test N (H.3): description ===
let $QUERY=...|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
<sql>|
```

For empty-CTE / scalar-CTE cases, double-check the MySQL
reference output — MySQL's empty-aggregate semantics
(`SUM` over 0 rows = NULL, `COUNT` = 0) are what the diff
should reflect.

## Files

- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — append the
  five new tests.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- This doc.

## Verification

```
cd debug_build && ./mtr --record --suite=ronsql ronsql_cte_basic
                  ./mtr --suite=ronsql            # no regressions
```

## What we're not doing

- **CTE_SCAN-as-outer-join-child** — Phase G defensively rejects
  this; H.3 doesn't try to test it from SQL.  If the rejection
  is hit, add a `--error` test in a future phase.
- **Multi-CTE coordination across batch boundaries** — that
  combination is H.2 × this; one large multi-CTE test is enough
  if H.2 ships before H.3 (otherwise covered separately).
