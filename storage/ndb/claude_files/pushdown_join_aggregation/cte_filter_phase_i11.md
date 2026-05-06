# Phase I.11 — RonSQL coverage of testCteNdbApi.cpp Tests 12–16

## Status

**Planned.**  Test-driven phase.  The original I.11 catalogue entry
mapped to testCteNdbApi.cpp Test 20 (cross-join of two scalar CTEs),
but Test 20's shape was already shipped end-to-end by Phase I.17
(`8609cad17f4`).  This phase repurposes I.11 to cover the kernel
shapes still without a RonSQL phase mapping in the same Test number
range:

| Kernel test | Title                                              |
|-------------|----------------------------------------------------|
| Test 12     | `lookupCte` as CTE materialisation root + child   |
| Test 13     | `lookupCte` as main-query internal node           |
| Test 14     | `lookupCte` as CTE materialisation internal       |
| Test 15     | `scanCte` as main-query agg leaf                  |
| Test 16     | `scanCte` as CTE materialisation root non-leaf   |

Test 21 (`GREATEST(MAX(val), MIN(val))` via CASE in aggregation) is
already covered by earlier work and is not included here.

## Approach

I.11 is **test-first**:

1. Write one MTR file `mysql-test/suite/ronsql/t/ronsql_cte_kernel_t12_t16.test`
   with one SQL shape per kernel test (the shapes are documented in
   `testCteNdbApi.cpp` directly above each test function).
2. Run the file.  Some shapes are expected to pass already through
   prior phases (E.1 / E.2 / I.7 / I.8); others may fall through to
   error paths or rejection messages.
3. For each shape that does **not** match MySQL today, diagnose the
   gap and either:
   - fix it inside I.11 if the fix is small and localised, or
   - extract the gap into a follow-up phase
     (`cte_filter_phase_i11_<n>.md`) and ship I.11 with that test
     marked `--error` until the follow-up lands.
4. For each shape that already works, the MTR coverage alone is the
   I.11 deliverable for that line.

The plan deliberately does **not** predict which shapes will work
versus which need code; the running tree has shifted enough that
running the tests is faster than reasoning about it.

## Per-test SQL shapes

Each section gives the SQL trigger, the kernel-side expected tree,
and the expected runtime result derived from the testCteNdbApi.cpp
fixture (`cte_src`: `(pk, grp, val)` with rows
`(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)`).

The MTR fixture should mirror this exactly:

```sql
CREATE TABLE cte_src (
  pk  INT NOT NULL,
  grp INT NOT NULL,
  val INT NOT NULL,
  PRIMARY KEY (pk),
  INDEX idx_grp (grp)
) ENGINE=NDB;

INSERT INTO cte_src VALUES
  (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50);
```

### Test 12 — `lookupCte` as CTE materialisation root + child

Kernel-side, the CTE 1 body root is a `lookupCte(0, key=constInt(1))`
followed by a real-table `readTuple` agg leaf.  In SQL terms, the CTE
body looks up a single `cte0` group by constant key, then joins it to
`cte_src` to compute its own aggregate.

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM cte_src GROUP BY grp),
     cte1 AS (SELECT s.grp, SUM(s.val) AS total
                FROM cte0
                JOIN cte_src AS s ON s.pk = cte0.grp
               WHERE cte0.grp = 1
               GROUP BY s.grp)
SELECT grp, total FROM cte1;
```

Expected result:
```
grp  total
1    10
```

What this exercises that prior phases did not:
- A multi-op CTE body where the **root** of the body is a CTE op
  (CTE_LOOKUP), not a real-table scan.
- The constant-keyed `lookupCte` materialisation pattern that the
  kernel build-loop walk-up (added in T12 kernel work) supports.

Likely RonSQL gap: today CTE body planning prefers a real-table root
unless explicitly chained through a CTE_SCAN.  Whether the planner
will pick CTE_LOOKUP as the body root with a constant key bound from
WHERE is the test question.  If it doesn't, the fall-back (real-table
scan + CTE_LOOKUP child) still produces the right rows, so the SQL
result will likely match MySQL even if the chosen plan shape isn't
identical to Test 12.  Strict match-MySQL test passes either way.

### Test 13 — `lookupCte` as main-query internal node

Three-level main query, no aggregation.  CTE 0 is a normal grouped
CTE; the main query is `real_table → cte0 → real_table`:

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM cte_src GROUP BY grp)
SELECT m.pk, m.grp, cte0.grp AS c_grp, cte0.total, s2.pk AS s2_pk, s2.val
  FROM cte_src AS m
  JOIN cte0 ON cte0.grp = m.grp
  JOIN cte_src AS s2 ON s2.pk = cte0.grp
ORDER BY m.pk;
```

Expected result (5 rows, one per `cte_src` row):
```
pk  grp  c_grp  total  s2_pk  val
1   1    1      30     1      10
2   1    1      30     1      10
3   2    2      70     2      20
4   2    2      70     2      20
5   3    3      50     3      30
```

What this exercises that prior phases did not:
- Non-aggregating multi-join with a CTE in the **middle** of the
  chain (parent and child both real tables).
- Phase I.8 accepted `FROM <real> JOIN <cte> ON ...` projection-only
  with **exactly one** AST join entry where the target resolves to a
  CTE.  Test 13 has two joins (`m JOIN cte0`, `JOIN s2 ON ...`).

Likely RonSQL gap: I.8's single-CTE-join gate at
`RonSQLPreparer.cpp:543-554` rejects two-join shapes outright with
the "Not an aggregate query" / similar permanent error.  Concrete
fix is to relax the gate to accept **any** projection-only join
chain that contains at least one CTE join, then verify
`emit_child_ops` and `execute_passthrough_drain` already handle the
multi-CTE-or-CTE-in-middle shape (the multi-op pre-registration loop
in `execute_passthrough_drain` was already generalised in I.8).

### Test 14 — `lookupCte` as CTE materialisation internal

CTE 1 body has three ops: real-table scan, CTE_LOOKUP into cte0,
real-table readTuple.  All inside the CTE 1 body.

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM cte_src GROUP BY grp),
     cte1 AS (SELECT s2.grp, SUM(s2.val) AS total
                FROM cte_src AS s1
                JOIN cte0 ON cte0.grp = s1.grp
                JOIN cte_src AS s2 ON s2.pk = cte0.grp
               GROUP BY s2.grp)
SELECT grp, total FROM cte1
ORDER BY grp;
```

Expected result:
```
grp  total
1    20
2    30
```

(Rows feeding cte1Agg: `(grp=1,val=10), (grp=1,val=10),
(grp=2,val=20), (grp=2,val=20), (grp=3,val=30)` per the kernel test
trace.  Note: the test's narrative table at line 4173 differs from
the actual fixture; trust the fixture-derived computation.)

What this exercises that prior phases did not:
- A **multi-op CTE body** (`num_ops > 1`) where the body's middle
  op is a CTE_LOOKUP.
- The single-op CTE-body emit branches added in Phase I.9
  (TABLE_SCAN, INDEX_SCAN) and the chained-CTE-body branch added
  in E.2 (CTE_SCAN root) only handle `cp.num_ops == 1`.  This
  shape needs the multi-op CTE-body path.

Likely RonSQL gap: the multi-op CTE-body emit path at
`RonSQLPreparer.cpp:4681` (the `else` arm noted in the E.2 plan)
already exists, but whether it composes with a CTE_LOOKUP child
inside the CTE body — and whether `build_cte_virtual_tables` /
`emit_child_ops` correctly attach the cte0 virtual table to the
cte1-body's child — needs verification.  Re-runs of E.2 / I.16's
nested cases give partial confidence but don't cover this exact
shape.

### Test 15 — `scanCte` as main-query agg leaf

Main query aggregates over a CTE_SCAN root with no GROUP BY:

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM cte_src GROUP BY grp)
SELECT COUNT(*) AS cnt, SUM(cte0.total) AS s
  FROM cte0;
```

Expected result:
```
cnt  s
3    150
```

(150 = 30 + 70 + 50 across the three cte0 groups.)

This shape is already covered by Phase E.1 Test 5
(`SELECT SUM(t) AS gt FROM sums` — main-query aggregation over
CTE_SCAN root, `agg_leaf_idx == 0`).  I.11's MTR is regression
coverage, not new functionality.

### Test 16 — `scanCte` as CTE materialisation root non-leaf

CTE 1 body uses `scanCte(cte0)` as its root, joined to a real
table that becomes the agg leaf.

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM cte_src GROUP BY grp),
     cte1 AS (SELECT s.grp, SUM(s.val) AS total
                FROM cte0
                JOIN cte_src AS s ON s.pk = cte0.grp
               GROUP BY s.grp)
SELECT grp, total FROM cte1
ORDER BY grp;
```

Expected result:
```
grp  total
1    30
2    30
```

(Rows feeding cte1Agg: `(grp=1,val=10), (grp=1,val=20),
(grp=2,val=30)` per the kernel test trace.)

What this exercises that prior phases did not:
- Phase E.2 covered the **single-op** chained-CTE body
  (`cp.num_ops == 1 && cp.ops[0].type == JoinOp::CTE_SCAN`,
  `RonSQLPreparer.cpp:4634+`).  Test 16's CTE 1 body has
  **two ops**: a CTE_SCAN root + a real-table readTuple child.
- The kernel side suppresses `T_USER_PROJECTION` on the CTE_SCAN
  root inside the CTE materialisation subtree so its rows return
  to DBSPJ rather than being shipped to the API.  RonSQL's emit
  has to produce a CTE_SCAN root inside a multi-op CTE body and
  let `build_cte_virtual_tables` plus `emit_child_ops` wire the
  child correctly.

Likely RonSQL gap: same multi-op-CTE-body emit gap as Test 14, but
with a CTE_SCAN root instead of a CTE_LOOKUP middle node.  E.2's
plan explicitly noted the single-op carve-out; whether the
multi-op `else` arm at `RonSQLPreparer.cpp:4681` already handles
CTE_SCAN-as-body-root correctly is the test question.

## MTR file layout

```
mysql-test/suite/ronsql/t/ronsql_cte_kernel_t12_t16.test
mysql-test/suite/ronsql/r/ronsql_cte_kernel_t12_t16.result
```

Use the existing `suite/ronsql/include/ronsql_compare.inc` harness
and the standard fixtures pattern:

```
--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings

--let $suppress_ronsql_cli=yes
--let $strict_diff=yes

--disable_query_log
--disable_warnings
CREATE TABLE cte_src (...) ENGINE=NDB;
INSERT INTO cte_src VALUES (...);
--enable_warnings
--enable_query_log

DELIMITER |;

# --- Test 12 ---
let $QUERY=...;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
...|

# --- Test 13, 14, 15, 16 — same pattern ---

DELIMITER ;|

--disable_query_log
DROP TABLE cte_src;
--enable_query_log
```

ORDER BY in the MTR queries (where used above) keeps the result
stable across NDB fragment iteration order without needing
`--sorted_result` for every block.

## Procedure

1. Write the MTR file with all five shapes.
2. Run `./mtr --suite=ronsql ronsql_cte_kernel_t12_t16` (no
   `--record` first — see what passes / fails as-is).
3. For each failing shape:
   - Capture the actual error (RonSQL permanent error, mismatched
     result, or assertion).
   - Decide between fix-in-I.11 (small localised change confined to
     gates / dispatch in `RonSQLPreparer.cpp`) and split-to-follow-up
     (anything touching planner internals, kernel signal flow, or
     more than ~50 LOC of RonSQL code).
   - Document the decision inline in this plan (append to a
     "Findings" section once tests run).
4. Once all green or all deferred to follow-ups, run
   `./mtr --record --suite=ronsql ronsql_cte_kernel_t12_t16` and
   commit.
5. Run the broader regression set:
   ```bash
   ./mtr --suite=ronsql
   ./mtr --suite=ndb_push_agg
   ```

## Non-goals

- No new kernel work.  All five shapes are kernel-validated in
  testCteNdbApi.cpp; any I.11 fix is RonSQL-only.
- No NDB-API changes.
- No general planner restructuring.  Multi-op CTE-body shapes that
  need significant planner work are split to follow-ups.

## Completion criteria

- MTR file `ronsql_cte_kernel_t12_t16.test` exists and exercises
  all five shapes.
- Every shape either passes (matches MySQL) or is `--error`-gated
  with a follow-up phase reference recorded in this plan.
- Catalogue (`cte_filter_phase_i.md`) updated: I.11 entry
  repointed to "testCteNdbApi.cpp Tests 12–16" and marked
  shipped, with any deferred-shape follow-ups linked.
- I.16's catalogue entry left unchanged.

## Implementation checklist

1. Write the MTR `.test` file with the five SQL shapes and the
   shared fixture.
2. Run unrecorded:
   ```bash
   cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
   cd debug_build/mysql-test
   ./mtr --suite=ronsql ronsql_cte_kernel_t12_t16
   ```
3. Triage failures: append a "Findings" subsection to this plan
   noting which shapes pass / fail / are deferred to follow-ups.
4. For each fix-in-I.11 shape, make the smallest possible RonSQL
   change to make the SQL match MySQL, tested against the
   recorded MTR baseline.
5. Re-run with `--record` to capture the result file.
6. Run the broader suites:
   ```bash
   ./mtr --suite=ronsql
   ./mtr --suite=ndb_push_agg
   ```
7. Update `cte_filter_phase_i.md` (the catalogue): repoint the
   I.11 entry to Tests 12–16 and mark shipped.
8. Update
   `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
   with an I.11 entry below I.10's.
9. Commit per the per-phase cadence used on this branch.
