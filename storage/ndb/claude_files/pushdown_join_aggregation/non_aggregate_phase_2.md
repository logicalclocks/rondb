# Non-Aggregate Pushdown — Phase 2 Detailed Plan

**Status: IMPLEMENTED (August 2026) — pending user build + MTR --record.**

Implementation notes: landed exactly as planned below — W1 renamed the
gate flag to `projection_only_join_chain` and dropped the
`query_has_cte` conjunct; W2 added both planner pre-checks (identity
check guarded on `childOp.table != NULL && parent.table != NULL` so
every CTE-involving link is skipped); W3 is
`body_passthrough_snowflake.inc` (sn-1..15 + sn-P1..P3, family-local
`snow1` for the unique child, EXPLAIN greps on sn-2/7/12) ×5 suites +
`findings/passthrough_snowflake.md`.  sn-4/5 are the first pure
LookupQuery-protocol pass-through queries (lookup root, lookup
children, no CTE — legal for non-aggregate defs; the Phase 0 guards
are aggregate-only).

**sn-15 probe outcome (first record run): CONFIRMED wrong results.**
RonSQL delivered the LEFT JOIN's NULL-extended rows (the orders whose
`o_clerk` IS NULL) that MySQL's INNER join below eliminates — exactly
the predicted nest-metadata gap.  Per the stated disposition, the
parse gate now tracks which visible aliases are "under LEFT"
(LEFT-joined or reached through one) and rejects an INNER join whose
parent is under-LEFT with a targeted permanent error; sn-15 became a
rejection-assert with a NEXT-PHASE marker (emit
`setFirstInnerJoin`/`setUpperJoin` per `ha_ndbcluster_push.cc`
`build_query`, then re-enable the compare).  LEFT-under-LEFT and
LEFT-under-INNER remain allowed (semantics match without nest
metadata).  The **aggregate join path emits the same MatchTypes with
no nest metadata and is not covered by this gate** — the same shape
under COUNT/SUM is a suspected latent wrong-results bug, tracked as a
separate probe in the findings file.
**Parent:** `non_aggregate_pushdown_plan.md` (Phase 2 overview).
**Predecessors:** `non_aggregate_phase_0.md` (residual filters, printer
types, lookup-root crash hardening), `non_aggregate_phase_1.md`
(single-table non-aggregate queries).

## Goal

Snowflake-schema pushed joins over **real tables only** — the first
non-aggregate joins with no CTE anywhere:

- **2a** Root (table scan / index scan / PK lookup) + PK or
  unique-index lookup children following foreign keys, INNER joins —
  the NDB API's fully green path (`LookupQuery` / `SingleScanQuery`,
  no batch-repeat semantics).
- **2b** Ordered-index-scan children (linked bounds from parent
  columns) — the query becomes `MultiScanQuery`
  (`RT_MULTI_SCAN` + `RT_REPEAT_SCAN_RESULT`).
- **2c** LEFT OUTER joins in the chain, on both lookup and scan
  children.

Projection-only, per-table WHERE conjuncts allowed everywhere (they
route to per-op filters/bounds via the existing machinery); cross-table
residuals (`a.x > b.y`) stay rejected; ORDER BY / LIMIT stay rejected.

## Why this phase is small

Everything below the parse gate already handles real-only chains:

- `QueryPlanner` classifies real children as `PK_LOOKUP` /
  `UNIQUE_LOOKUP` / `INDEX_SCAN` and is battle-tested by the aggregate
  join suite (`ronsql_dbt3*`, `body_joins`, `body_main_root_index`).
- `emit_root_op` (Phase 0-hardened: residual filters, the
  lookup-root-vs-aggregation guard is aggregate-only so pass-through
  readTuple roots are allowed) and `emit_child_ops` (linked
  `readTuple`, linked-bound `scanIndex`, per-child `NdbScanFilter`,
  `MatchAll` for LEFT) need no changes.
- `execute_passthrough_drain` is multi-op (I.8), substitutes NULL via
  per-op `isRowNULL()` (I.12), and guards empty-projection real-table
  ops with a dummy getValue.  With zero CTE subtree ops its CTE
  bookkeeping degenerates to no-ops.
- `classify_where_by_table` routes per-table conjuncts; leftover
  cross-table filters already hit the clean `compile()` rejection for
  non-aggregate queries.

Phase 2 is therefore: one conjunct removed from the parse gate, two new
planner pre-checks that turn runtime NDB 48xx errors into clean
prepare-time errors, and a test family that exercises the cross
product of shapes.

## Work items

### W1 — parse gate: drop the CTE requirement

The shape-B walk in `parse()` already validates exactly what
`QueryPlanner` needs (INNER/LEFT_OUTER only, ON conditions bind the
joined alias's columns to an already-visible alias, CTE operands
complete-key).  Its final `&& query_has_cte` conjunct is the only
reason a pure real-table chain rejects.  Remove it (and the now-unused
`query_has_cte` tracking), rename the flag to
`projection_only_join_chain`, and update the block comment + the
rejection error text.  CTE-involving chains keep their exact current
envelope — the walk's CTE coverage checks are untouched.

### W2 — planner pre-checks (clean errors instead of NDB 48xx)

Both in `QueryPlanner::plan`, and deliberately **not** gated on
aggregate-vs-passthrough — aggregate queries hit the same API rules
and currently surface them as runtime NDB errors; getting a clean
prepare-time message is an improvement for both (called out as a
behavior change).

1. **Linked-column identity + BLOB rejection.**  The NDB API's
   linked-operand rule (`NdbQueryBuilder.cpp` `bindOperand`): child key
   column and parent column must match in type, precision, scale,
   length and charset — no implicit conversion — and BLOB/TEXT can
   never be linked.  After child classification, for real-child /
   real-parent links, compare the dict columns and throw
   `RonSQLPermanentError` with a message naming both columns.  CTE
   operands are skipped (virtual-table typing is handled by the CTE
   machinery).  Also turns a previously-latent "unknown parent column"
   emit-time failure into a clean planner error.
2. **Internal-node budget.**  The NDB API caps a query def at 32
   internal tree nodes, and a unique-index lookup expands to 2 (index +
   base table); the planner's own `MAX_SPJ_TREE_NODES` check counts it
   as 1.  Count `num_ops + #UNIQUE_LOOKUP` at the end of `plan()` and
   reject over-budget plans with a message explaining the 2× rule.
   CTE subtree nodes stay accounted by the API's own prepare-time cap
   (the planner cannot know body sizes).

### W3 — MTR family

`body_passthrough_snowflake.inc` + wrapper
`ronsql_cte_dd_passthrough_snowflake.test` ×5 topology suites, strict
diff vs MySQL, sorted compare.  A family-local `snow1` table provides
the unique-index child (the shared schema has no unique secondary
index).  Cases (sn-N):

| # | Case | Shape |
|---|---|---|
| 1 | 2-level PK chain (`orders → customer`) | scan root + PK child |
| 2 | 3-level PK chain (`orders → customer → nation`) with EXPLAIN greps | chain |
| 3 | 4-level PK chain (`… → region`) + per-table WHERE on root and mid | chain + filters |
| 4 | readTuple root (`o_orderkey = 77`) + PK chain | lookup-rooted pass-through |
| 5 | readTuple root + residual (Phase 0a lookup filter) + PK child | lookup + filter |
| 6 | scan-config INDEX_SCAN root (`idx_c_nationkey`) + PK child | index-scan root |
| 7 | INDEX_SCAN child (`customer → orders` via `idx_o_custkey`) | scan-scan (2b) |
| 8 | 3-level scan-scan chain (`nation → customer → orders`) | MultiScanQuery |
| 9 | full `customer JOIN orders` (300 → 1500 rows) | batch-stress scan-scan |
| 10 | LEFT JOIN on a nullable key (`o_clerk`) → NULL-extended rows | 2c lookup |
| 11 | same join INNER → NULL keys drop rows | NULL-key semantics |
| 12 | UNIQUE_LOOKUP child (local `snow1.s_uk`) | unique child (2 internal nodes) |
| 13 | projection touching only the root | child empty-projection guard |
| 14 | LEFT child under an INNER chain (always-matched) | mixed chain |
| 15 | INNER below a LEFT child (nullable link) | mixed-nest probe |
| P1 | rejection: type-mismatch join (`r_regionkey` TINYINT = `c_custkey` INT) | W2 check |
| P2 | rejection: join column with no usable index (`l_quantity`) | existing planner error |
| P3 | rejection: cross-table WHERE residual on a pass-through join | existing compile() error |

sn-15 is explicitly a semantics probe: RonSQL emits no outer-join nest
options (`setFirstInnerJoin` / `setUpperJoin`), matching what the
aggregate chains already ship; the strict diff against MySQL decides
whether INNER-below-LEFT matches without them.  If recording shows a
diff, the case converts to a NEXT-PHASE marker and the shape gets a
gate restriction — evidence first.

### W4 — docs

Status updates in this file, the parent plan, and the directory
CLAUDE.md; findings file `findings/passthrough_snowflake.md` carrying
the deferred probes.

## Deliberately out of scope (tracked)

- **Nest options for mixed INNER/LEFT chains** — follow
  `ha_ndbcluster_push.cc:2589-2625` if sn-15-style probes ever diff.
- **Child-local constant bounds** emitted as filters instead of bounds
  — the known optimizer gap shared with the aggregate path
  (`next_steps.md`, join-root follow-ups item 2).
- **Genuine multi-batch scan-scan verification at scale** — the shared
  schema tops out at `customer JOIN orders` (sn-9); a
  `load_ronsql_large`-based case in the `ronsql_large` suite is the
  named follow-up for forcing many SCAN_NEXTREQ round trips through
  the `RT_REPEAT_SCAN_RESULT` protocol.
- **Semi/anti-join revival** (`MatchFirst`/`MatchNullOnly`, dormant
  since phase7 Step 40) — ha_ndbcluster deliberately forbids
  semi-joined index scans; out of the non-aggregate roadmap for now.
- **BLOB-join rejection MTR** — the shared schema has no BLOB/TEXT
  columns; the planner check is code-covered only.
- Star fan-out (two children on one parent) is **Phase 3**, not here —
  the gate walk admits it structurally, but coverage and verdicts
  belong to that phase.

## Risks / notes

- **W2 is a behavior change for aggregate queries too**: join-column
  type mismatches and over-budget plans now fail at prepare time with
  clean permanent errors instead of runtime NDB errors (which the
  retry loop could previously burn attempts on).  All green suites use
  identically-typed join columns, so no baseline should move.
- **Scan-scan correctness across batches** rests on the API's
  `nextResult()` presenting each joined row exactly once (the MySQL
  handler consumes the same protocol); sn-9 is the in-suite stress,
  the large-data case the real proof.
- Committed reads, streaming drain, result-volume notes: unchanged
  from Phase 1.

## Verification (user-run)

- Build, then `./mtr --record --suite=ronsql_cte
  ronsql_cte_dd_passthrough_snowflake`, then the 4 siblings.
- Regression: full `ronsql_cte` suite + `ronsql` suite (W2 touches the
  planner used by every join query).
- `.explain_ronsql` spot check on sn-2's shape (join plan tree with
  `[ROOT] TABLE_SCAN` + two `[INNER] PK_LOOKUP` lines).
