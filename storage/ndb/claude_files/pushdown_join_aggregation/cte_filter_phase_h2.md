# CTE filter Phase H.2 — multi-batch / SCAN_NEXTREQ pacing

## Status

**Plan only.**  No code yet.  Independent of H.1 / H.3.

## Context

`testCteNdbApiFilter.cpp` Tests 13 (256+ rows), 14 (600 rows), 18
(chained CTE multi-batch) stress SCAN_NEXTREQ pacing across batch
boundaries — the kernel's batch limit (256 rows by default) means a
CTE with > 256 groups requires the API to issue a continuation
SCAN_NEXTREQ.  Existing MTR tests use ≤ 7 rows and never trigger
this path, leaving the CTE multi-batch correctness silent against
regressions.

H.2 ships a new test file with deliberately large CTE scans.  Test
data only — no production code changes.

## Tests

New file: `mysql-test/suite/ronsql/t/ronsql_cte_multi_batch.test`.

### Schema and data

```sql
CREATE TABLE cte_big (
  pk INT NOT NULL,
  grp INT NOT NULL,
  val INT NOT NULL,
  PRIMARY KEY USING HASH (pk)
) ENGINE=NDB;
CREATE INDEX idx_grp ON cte_big (grp);
```

Populate ~1500 rows yielding ~300 groups (rows-per-group = 5).
Generate via `--disable_query_log` + a single multi-row INSERT
built by the test, e.g. INSERT VALUES (1,1,10), (2,1,20), … in
chunks of ~100 per INSERT statement to avoid lex/parse overhead.
Target setup time: < 2 s.

### Tests

- **Test 1 — CTE_SCAN root over 300+ groups.**
  ```sql
  WITH sums AS (SELECT grp AS k, SUM(val) AS t
                FROM cte_big GROUP BY grp)
  SELECT k, SUM(t) FROM sums GROUP BY k;
  ```
  Verify all 300 groups returned (count, sum-of-sums).

- **Test 2 — CTE_SCAN with WHERE rejecting half.**
  ```sql
  ... SELECT k, SUM(t) FROM sums WHERE t > <median> GROUP BY k;
  ```
  Verify ~150 groups returned, batch boundary lands inside the
  filtered subset.

- **Test 3 — chained CTE, both > 256 groups.**
  ```sql
  WITH a AS (SELECT grp AS k, SUM(val) AS s
             FROM cte_big GROUP BY grp),
       b AS (SELECT k, MAX(s) AS m
             FROM a GROUP BY k)
  SELECT k, SUM(m) FROM b GROUP BY k;
  ```
  Mirrors testCteNdbApiFilter Test 18.

- **Test 4 — CTE_LOOKUP under multi-batch parent scan.**
  Parent table with > 256 rows joined to a CTE keyed by `grp`,
  forcing CTE_LOOKUP to fire many times across a multi-batch
  parent scan.  Schema: `cte_parent (id INT PK, grp INT)` with
  300 rows.
  ```sql
  WITH g AS (SELECT grp AS k, COUNT(*) AS c
             FROM cte_big GROUP BY grp)
  SELECT p.id, g.c FROM cte_parent p JOIN g ON p.grp = g.k;
  ```

## Pattern

Same `ronsql_compare.inc` skeleton as the other ronsql tests, with
the standard suppression preamble:

```
--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
```

(The `suppress_ronsql_cli=yes` flag is set in the existing
`ronsql_cte_*` tests because `ronsql_cli` doesn't refresh
dictionary cache for join queries — same applies here.)

`--sorted_result` per query.  Use small-width INTs to keep INSERT
data dense in the .test file.

## Files

- `mysql-test/suite/ronsql/t/ronsql_cte_multi_batch.test` — new.
- `mysql-test/suite/ronsql/r/ronsql_cte_multi_batch.result` —
  record on first run.
- This doc.

## Verification

```
cd debug_build && ./mtr --record --suite=ronsql ronsql_cte_multi_batch
                  ./mtr --suite=ronsql            # no regressions
```

Measure runtime; if > 30 s, fold into a slow-suite tag instead of
the default `ronsql` suite, or shrink to ~300 rows × 60 groups
(still triggers SCAN_NEXTREQ at default 256 batch).

## What we're not doing

- **`setBatchSize` exposure** — no SQL surface; can't be exercised
  from MTR.  Block tests stay authoritative for that.
- **Multi-fragment / multi-data-node behaviour** — MTR cluster
  config is fixed; this is single-node coverage of the
  protocol-pacing path.  Multi-fragment regressions surface in the
  block tests.
- **Memory pressure / eviction tests** — MAX_AGG_RESULT_BATCH_BYTES
  pressure is exercised by `testJoinAgg` and not in scope here.
