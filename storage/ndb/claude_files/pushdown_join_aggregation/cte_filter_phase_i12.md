# Phase I.12 — RonSQL coverage of testCteNdbApiOuterJoin.cpp

## Status

**Planned.**  Test-driven phase.  The original I.12 catalogue entry
mapped to "CTE_SCAN as a LEFT JOIN inner side" (the cross-join LEFT
JOIN shape over an unkeyed `scanCte` child).  That shape was dropped
at the kernel level in `cte_outer_join_phase_3.md` and is guarded by
the Phase G defensive reject at `RonSQLPreparer.cpp:3801-3805`.  The
guard stays.

While the originally-targeted shape was being dropped, the rest of
the outer-join work shipped:

| cte_outer_join phase | Scope                                              |
|----------------------|----------------------------------------------------|
| Phase 1              | `scanTable + lookupCte` LEFT JOIN child            |
| Phase 2              | `scanCte` LEFT-side parent (INNER + LEFT children) |
| Phase 4              | Consolidated kernel test coverage                  |
| Phase 5              | CTE-subtree agg-feed NULL injection                |
| Phase E.1K           | `scanCte` parent + main aggregator on real leaf   |

testCteNdbApiOuterJoin.cpp Tests 1, 2, 3, 5, and 6 exercise these
kernel shapes.  None of them have RonSQL MTR coverage today
(`mysql-test/suite/ronsql/t/` has `ronsql_left_join.test` and
`ronsql_chained_left_join.test` for non-CTE LEFT joins, plus
`ronsql_cte_partial_key.test` which uses LEFT JOIN only as a
rejection probe — no positive CTE outer-join coverage).

I.12 repoints to **RonSQL coverage of the shipped outer-join shapes**,
mirroring the test-first structure used in Phase I.11.

## Approach

I.12 is **test-first**:

1. Write one MTR file
   `mysql-test/suite/ronsql/t/ronsql_cte_outer_join.test` with one
   SQL shape per kernel-shipped outer-join Test (1, 2, 3, 5, 6).
2. Run the file.  Some shapes likely pass already through prior
   phases (E.1 / E.2 / I.7 / I.8 / I.11).  Others may hit Phase G's
   defensive reject (incorrectly — for shapes that should be
   accepted), the "Not an aggregate query" gate, or a planner gap
   that produced the wrong `JoinOp::match_type` on the CTE child.
3. For each shape that does **not** match MySQL today, diagnose the
   gap and either fix it inside I.12 (small localised change) or
   extract the gap into a follow-up phase
   (`cte_filter_phase_i12_<n>.md`) and ship I.12 with that test
   marked `--error` until the follow-up lands.
4. For each shape that already works, MTR coverage alone is the
   I.12 deliverable for that line.

The plan deliberately does **not** predict pass/fail status — running
the tests is faster than reasoning about it through Phase G's reject,
the post-I.23 scoped resolver, the I.24 descriptor model, and the
non-aggregate gate that I.11 partially relaxed.

The Phase G reject for `CTE_SCAN as outer-join child` stays.  It
guards a kernel shape that was deliberately dropped; preserving the
reject keeps the planner-regression tripwire intact.

Important distinction (carried over from I.11): a positive MTR
comparison exercises the RonSQL path for the kernel shape.  A
`--error` test is only a documented RonSQL gap with a stable
rejection; it does **not** count as kernel shape coverage.

## Predicted state

The non-aggregate gate at `RonSQLPreparer.cpp:540-636` (Phase
E.3 + I.8 + I.11) admits two narrow projection-only shapes:

- `from_is_cte && !has_joins` — Phase E.3, projection-only over a
  single CTE.
- `!from_is_cte && all joins are INNER_JOIN && has_joins` — I.8 +
  I.11, projection-only `real-table-root INNER chain`, with
  every CTE join forced through `cte_key_coverage` to be
  `ExactOrdered` or `ExactPermuted`.

Anything else with no aggregation in the outer SELECT is rejected
with `Not an aggregate query.`.  Aggregate queries take a
different path that does not depend on this gate.

Cross-referencing the five Tests:

| Test | Outer SELECT | Gate verdict (predicted)           |
|------|--------------|------------------------------------|
| 1    | non-aggregate, `FROM cte JOIN real`         | rejected (`from_is_cte && has_joins`)  |
| 2    | non-aggregate, `FROM cte LEFT JOIN real`    | rejected (same)                        |
| 3    | non-aggregate, `FROM real LEFT JOIN cte`    | rejected (`LEFT_OUTER` — gate forces INNER) |
| 5    | aggregate (`COUNT`, `SUM`), `FROM real LEFT JOIN cte` | aggregate path, gate not hit  |
| 6    | aggregate (`SUM`, `GROUP BY cte.grp`), `FROM cte JOIN real` | aggregate path, gate not hit |

Tests 5 and 6 are therefore the **expected-to-pass** cases.
Tests 1, 2, 3 are the **expected-to-fail** cases, all blocked by
the same gate.  Plan starts with Tests 5 and 6 to lock in the
positive baseline, then digs into the gate extension and any
emit-side gaps Tests 1/2/3 require.

## Per-test SQL shapes

The kernel tests use this fixture
(`testCteNdbApiOuterJoin.cpp:30-130` area):

- `oj_cte_src(pk INT PK, grp INT, val INT)` — same data as
  `cte_src` in earlier phases:
  `(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)`.  CTE 0 is
  `SELECT grp, SUM(val) AS total FROM oj_cte_src GROUP BY grp` →
  `{(1,30),(2,70),(3,50)}`.
- `oj_rhs(id INT PK, label VARCHAR)` — three rows
  `(1,"one"), (3,"three"), (5,"five")`.

The MTR fixture should mirror this.  Use ENGINE=NDB, set widths
that round-trip through the existing diff harness, and keep the
table names short:

```sql
CREATE TABLE oj_cte_src (
  pk  INT NOT NULL,
  grp INT NOT NULL,
  val INT NOT NULL,
  PRIMARY KEY (pk),
  INDEX idx_grp (grp)
) ENGINE=NDB;

CREATE TABLE oj_rhs (
  id    INT NOT NULL,
  label VARCHAR(16) NOT NULL,
  PRIMARY KEY (id)
) ENGINE=NDB;

INSERT INTO oj_cte_src VALUES
  (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50);

INSERT INTO oj_rhs VALUES
  (1,'one'),(3,'three'),(5,'five');
```

## Tests expected to pass (verify first)

These two shapes are aggregate queries.  They take the
aggregate-query path and bypass the non-aggregate gate that blocks
Tests 1-3.  If they pass, they confirm that the kernel-side
outer-join + linked-attr machinery works end-to-end through RonSQL,
and that any failures in Tests 1-3 are RonSQL-only gating issues
(not deeper kernel or NDB-API gaps).  Run these first.

### Test 5 — Scalar main aggregation over `LEFT JOIN <cte>`  (Phase 5)

Kernel: `oj_rhs LEFT JOIN cte0 ON cte0.grp = oj_rhs.id` with main
aggregator on the parent path.  `COUNT(*)` is built via a
register-loaded constant 1 so it survives the NULL-injected row;
`SUM(cte0.total)` ignores the NULL row's contribution.

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM oj_cte_src GROUP BY grp)
SELECT COUNT(*) AS cnt, SUM(cte0.total) AS s
  FROM oj_rhs
  LEFT JOIN cte0 ON cte0.grp = oj_rhs.id;
```

Expected result (1 row):
```
cnt  s
3    80
```

(80 = 30 + 50; id=5 had no CTE match so cte0.total is NULL on that
row; SQL's SUM ignores NULL but COUNT(\*) still counts the parent
row.)

Why this is expected to pass: aggregate query, so the
`Not an aggregate query` gate is bypassed.  The kernel agg-feed
NULL-injection shipped in Phase 5 (`47d81b43903`); the RonSQL emit
path for `setAggregation` on a CTE_LOOKUP child under LEFT JOIN
exists.  The unknown is whether `emit_child_ops` correctly omits
`setMatchType` (or sets `MatchAll`) when
`JoinOp::match_type == LEFT_OUTER` for a CTE_LOOKUP child.

### Test 6 — `scanCte` parent + main aggregator on real leaf  (E.1K)

Kernel: `scanCte(0) INNER JOIN readTuple(oj_rhs, id = cte.grp)` with
the main aggregator on the real-table leaf, grouping by `cte.grp`
(linked CTE virt-column) and summing `cte.total` (also linked).
This drives DBSPJ's `appendFromParent` (Path 2) into the main
aggregator's linked-attr buffer.

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM oj_cte_src GROUP BY grp)
SELECT cte0.grp AS g, SUM(cte0.total) AS s
  FROM cte0
  JOIN oj_rhs ON oj_rhs.id = cte0.grp
 GROUP BY cte0.grp;
```

Expected result (2 rows; grp=2 dropped by INNER):
```
g  s
1  30
3  50
```

Why this is expected to pass: aggregate query with `GROUP BY`,
again bypassing the non-aggregate gate.  Phase E.1 ships
`scanCte` as a main-query root with main aggregation, and Phase
E.1K shipped the inline-type encoding for linked CTE virt-columns.
The unknown is whether the planner's `agg_leaf_idx` lands on the
real-table child (correct) versus the CTE_SCAN root (incorrect),
and whether `GroupByLinked` / `LoadLinkedColumn` emit on a
CTE-virt-column reference inside the aggregator program.

## Tests expected to fail (deeper analysis)

These three shapes are projection-only and route through the
non-aggregate gate at `RonSQLPreparer.cpp:540-636`.  All three are
predicted to fail with `Not an aggregate query.` for the reasons
spelled out in the per-test analysis below; the gate-extension fix
is shared.

### Common failure mechanism — non-aggregate gate

Today's gate (`RonSQLPreparer.cpp:553-626`) accepts:

1. `projection_only_cte_scan` — `from_is_cte && !has_joins`
   (Phase E.3), and
2. `cte_lookup_join_chain_to_real_root` —
   `!from_is_cte && has_joins`, every join must be `INNER_JOIN`,
   every CTE join must be a complete-key `CTE_LOOKUP`
   (`ExactOrdered` or `ExactPermuted` per `cte_key_coverage`).

Tests 1, 2 violate (1) and (2) because they have `from_is_cte` and
joins.  Test 3 violates (2) because the join type is `LEFT_OUTER`.

The proposed I.12 gate extension splits into two independent
relaxations (each minimally scoped):

**A. Accept `from_is_cte && has_joins`** — extend the I.8/I.11
   chain logic to also start from a CTE root.  The chain walker
   already tracks `visible_aliases[]` — seed it with the CTE root's
   alias and reuse the rest of the loop unchanged.  The CTE root
   becomes the parent of the first join, so the planner already
   handles it as a `CTE_SCAN` main-query root with real-table
   children (the Phase E.1 shape).  Required additions: when the
   root is a CTE, the chain may also contain `CTE_LOOKUP` joins to
   later CTEs; the existing complete-key check via
   `cte_key_coverage` continues to apply.

**B. Accept `LEFT_OUTER` joins in the chain** — drop the strict
   `INNER_JOIN` check at line 575.  The remaining safety nets are:
   - Phase G's defensive reject for `CTE_SCAN` as outer-join child
     stays at `RonSQLPreparer.cpp:3801-3805`.  Any chain that the
     planner classifies as `CTE_SCAN` under LEFT JOIN child fires
     this guard, keeping the dropped Phase 3 shape rejected.
   - Phase J's LEFT-to-INNER promotion at
     `RonSQLPreparer.cpp:1552` only fires when WHERE rejects NULL
     on the RHS.  The Test 1/2/3 queries have no WHERE, so Phase J
     does not promote — verify by inspection in the I.12 work.
   - Phase K's ANTI_JOIN promotion fires on
     `WHERE col IS NULL` on LEFT JOIN RHS.  The I.12 queries do
     not exercise this; no interaction expected.

Both relaxations should be applied together because Test 1
requires (A) only, Test 2 requires (A) and (B), and Test 3
requires (B) only.  Splitting them lets the I.12 fix accept
exactly the new shapes without admitting unrelated combinations.

### Test 1 — `scanCte` INNER JOIN `readTuple`  (Phase 2 baseline)

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM oj_cte_src GROUP BY grp)
SELECT cte0.grp AS g, cte0.total AS t, oj_rhs.label AS lbl
  FROM cte0
  JOIN oj_rhs ON oj_rhs.id = cte0.grp;
```

Expected result (2 rows; grp=2 has no oj_rhs match and is dropped
by the INNER):
```
g  t   lbl
1  30  one
3  50  five
```

Why it fails today: outer SELECT is projection-only and has both
`from_is_cte == true` and `has_joins == true`.  Neither
`projection_only_cte_scan` nor `cte_lookup_join_chain_to_real_root`
admits the combination.  Hits `Not an aggregate query.`.

Fix: relaxation **A** above.  No emit-side change expected — a
CTE_SCAN main-query root with a real-table PK-key child is already
emitted by Phase E.1's path.  Verify
`execute_passthrough_drain` (rewritten in I.8 for multi-op) handles
a CTE_SCAN root with one real-table child correctly: pre-register
all virt-table columns on the CTE op, all real-table columns on the
child op, in attrId order.

### Test 2 — `scanCte` LEFT JOIN `readTuple`  (Phase 2)

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM oj_cte_src GROUP BY grp)
SELECT cte0.grp AS g, cte0.total AS t, oj_rhs.label AS lbl
  FROM cte0
  LEFT JOIN oj_rhs ON oj_rhs.id = cte0.grp;
```

Expected result (3 rows):
```
g  t   lbl
1  30  one
2  70  NULL
3  50  five
```

Why it fails today: same as Test 1 (`from_is_cte + has_joins`)
plus the `LEFT_OUTER` join type that violates the strict-INNER
check.

Fix: relaxations **A** and **B** combined.  Two emit-side
considerations:

- The planner sets `op.match_type = LEFT_OUTER` for the
  real-table child of a CTE_SCAN parent.  `emit_child_ops` then
  needs to either omit `setMatchType` (default `MatchAll` =
  LEFT JOIN) or call `setMatchType(MatchAll)` explicitly for this
  case.  Inspect the existing `setMatchType` plumbing for the
  CTE_LOOKUP-under-LEFT-JOIN path used in Test 5; the same
  convention applies to a real-table child under LEFT JOIN.
- Phase G's reject does **not** trigger here.  The CTE_SCAN is the
  PARENT (root), not the child — Phase G only rejects CTE_SCAN as
  an outer-join CHILD.

### Test 3 — `scanTable` LEFT JOIN `lookupCte`  (Phase 1)

```sql
WITH cte0 AS (SELECT grp, SUM(val) AS total
                FROM oj_cte_src GROUP BY grp)
SELECT oj_rhs.id AS rid, oj_rhs.label AS lbl,
       cte0.grp AS g, cte0.total AS t
  FROM oj_rhs
  LEFT JOIN cte0 ON cte0.grp = oj_rhs.id;
```

Expected result (3 rows):
```
rid  lbl    g     t
1    one    1     30
3    three  3     50
5    five   NULL  NULL
```

Why it fails today: outer SELECT is projection-only,
`!from_is_cte && has_joins`.  The chain walker hits the strict
`join->join_type != JoinClause::INNER_JOIN` check at line 575 and
rejects.

Fix: relaxation **B**.  The chain walker stays exactly as I.11
left it except for the join-type check; complete-key
`cte_key_coverage` still applies (so wrong-column LEFT JOINs are
still rejected by the `WrongColumns` path).

Emit-side: `emit_child_ops` needs to set the right match type on
the CTE_LOOKUP child when `op.match_type == LEFT_OUTER`.  Phase 5
(`47d81b43903`) shipped the kernel side; the equivalent emit
plumbing for an aggregate query with a CTE_LOOKUP under LEFT JOIN
is what Test 5 exercises, so by the time Test 5 passes the same
emit path here should work.

## MTR file layout

```
mysql-test/suite/ronsql/t/ronsql_cte_outer_join.test
mysql-test/suite/ronsql/r/ronsql_cte_outer_join.result
```

Use the existing `suite/ronsql/include/ronsql_compare.inc` harness
and the standard fixtures pattern from
`ronsql_cte_kernel_t12_t16.test`:

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
CREATE TABLE oj_cte_src (...) ENGINE=NDB;
CREATE TABLE oj_rhs     (...) ENGINE=NDB;
INSERT INTO oj_cte_src VALUES (...);
INSERT INTO oj_rhs     VALUES (...);
--enable_warnings
--enable_query_log

DELIMITER |;

# --- Test 1 ---
let $QUERY=...;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
...|

# --- Test 2, 3, 5, 6 — same pattern ---

DELIMITER ;|

--disable_query_log
DROP TABLE oj_cte_src;
DROP TABLE oj_rhs;
--enable_query_log
```

Do not add `ORDER BY` to projection-only tests in this phase
(reasoning carried from I.11): the non-aggregate gate rejects
ORDER BY, and the compare include sorts result rows by default.
The sorted-result echo block prints the expected MySQL output for
the diff.

## Test 4 (multi-batch) and beyond

Test 4 in testCteNdbApiOuterJoin.cpp is Test 3 with `setBatchSize(1)`.
RonSQL has no SQL surface for batch-size selection (catalogue I.13).
Multi-batch behaviour for CTE main-SELECTs already has dedicated
coverage in `ronsql_cte_multi_batch.test`; combining LEFT JOIN with
small-batch is reasonable to add **after** Test 3 passes, but only
as a regression for batch-driven NULL-row delivery — extracting it
into a separate test is fine, or note it in this plan as deferred to
the existing multi-batch suite.

## Procedure

Order: lock in the expected-to-pass shapes first, then handle the
expected-to-fail group together (single shared gate-extension fix).

1. Write a first cut of the MTR file containing only Tests 5 and 6
   (the expected-to-pass shapes).  Run unrecorded:
   ```bash
   cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
   cd debug_build/mysql-test
   ./mtr --suite=ronsql ronsql_cte_outer_join
   ```
   - If both pass: positive baseline locked.  Move to step 2.
   - If either fails: capture the actual error, decide whether the
     gap is a small localised RonSQL fix (e.g., wrong
     `setMatchType` on the CTE_LOOKUP child for Test 5; missing
     `GroupByLinked` / `LoadLinkedColumn` for Test 6) or a deeper
     issue.  Fix small; defer big.  Append findings.

2. Add Tests 1, 2, 3 to the MTR file.  Run unrecorded.  These are
   predicted to fail today on `Not an aggregate query.`.  Confirm
   the predicted failure mode for each — anything else is news and
   should be investigated before the gate extension.

3. Apply the gate extension (relaxations A and B from the deeper
   analysis).  Re-run unrecorded.

4. Triage any remaining failures (most likely emit-side
   `setMatchType` plumbing or `execute_passthrough_drain` gaps for
   the CTE-root variants).  Fix small or split to follow-up
   (`cte_filter_phase_i12_<n>.md`); record the decision in
   Findings.

5. Once all five shapes pass (or are `--error`-gated with a linked
   follow-up), run with `--record` and commit:
   ```bash
   ./mtr --record --suite=ronsql ronsql_cte_outer_join
   ```

6. Run the broader regression set:
   ```bash
   ./mtr --suite=ronsql
   ./mtr --suite=ndb_push_agg
   ```
   Confirm zero regressions before committing.

## Non-goals

- No new kernel work.  Tests 1, 2, 3, 5, 6 are kernel-validated; any
  I.12 fix is RonSQL-only.
- No NDB-API changes.
- No revival of the dropped Phase 3 shape (CTE_SCAN as outer-join
  child).  The Phase G reject stays; do not add positive coverage
  for that shape.
- No general planner restructuring of `match_type` propagation.
  Anything bigger than minor `setMatchType` plumbing is split to a
  follow-up.

## Completion criteria

- MTR file `ronsql_cte_outer_join.test` exists and exercises all
  five shapes (Tests 1, 2, 3, 5, 6).
- Every shape either passes (matches MySQL) as positive coverage,
  or is `--error`-gated with a follow-up phase reference recorded
  in this plan.  The final I.12 status must explicitly separate
  these two categories.
- Phase G's defensive reject for CTE_SCAN-as-outer-join-child
  remains in place at `RonSQLPreparer.cpp:3801-3805`.
- Catalogue (`cte_filter_phase_i.md`) updated: I.12 entry repointed
  to "testCteNdbApiOuterJoin.cpp Tests 1/2/3/5/6" and marked
  shipped, with any deferred-shape follow-ups linked.
- CLAUDE.md updated with an I.12 entry below I.11's.

## Implementation checklist

1. Write the MTR `.test` file containing **only Tests 5 and 6** plus
   the shared fixture.  Run unrecorded:
   ```bash
   cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
   cd debug_build/mysql-test
   ./mtr --suite=ronsql ronsql_cte_outer_join
   ```
   Append a **Findings** subsection to this plan noting whether
   Tests 5 and 6 pass as predicted, plus any small fix-in-I.12
   work needed (likely candidates: `setMatchType` plumbing for the
   CTE_LOOKUP child under LEFT JOIN — Test 5; `GroupByLinked` /
   `LoadLinkedColumn` for the linked-CTE-virt-column GROUP BY —
   Test 6).
2. Add Tests 1, 2, 3 to the MTR file.  Run unrecorded.  Confirm
   each fails with `Not an aggregate query.`.  Append findings.
3. Apply the gate extension at `RonSQLPreparer.cpp:540-636`:
   - **Relaxation A.**  Allow `from_is_cte && has_joins` by
     extending the I.8/I.11 chain logic to seed
     `visible_aliases[]` with the CTE root's alias.
   - **Relaxation B.**  Drop the strict `INNER_JOIN` check at
     `RonSQLPreparer.cpp:575`.  Keep the `cte_key_coverage`
     complete-key check; keep Phase G's defensive reject; keep
     Phase J / Phase K untouched.
4. Re-run unrecorded.  Triage any remaining failures into
   fix-in-I.12 vs split-to-follow-up
   (`cte_filter_phase_i12_<n>.md`).  Likely emit-side candidates:
   - `emit_child_ops` `setMatchType` plumbing when
     `op.match_type == LEFT_OUTER` for both real-table and
     CTE_LOOKUP children.
   - `execute_passthrough_drain` walking a multi-op shape with a
     CTE_SCAN root and a real-table child (or vice versa) under
     LEFT JOIN.
5. Re-run with `--record` to capture the result file.
6. Run the broader suites:
   ```bash
   ./mtr --suite=ronsql
   ./mtr --suite=ndb_push_agg
   ```
   Confirm zero regressions.
7. Update `cte_filter_phase_i.md`: repoint the I.12 entry to
   "RonSQL coverage of testCteNdbApiOuterJoin.cpp Tests 1/2/3/5/6"
   and mark shipped.
8. Update `CLAUDE.md` with an I.12 entry below I.11's.
9. Commit per the per-phase cadence used on this branch.
