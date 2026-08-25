# Non-Aggregate Pushdown — Phase 5 Detailed Plan

**Status: SHIPPED (August 2026) — all cases green on first record ×5
topologies, including the three novel-topology probes (gc-7/8/9): SPJ
accepts different-parent CTE branches, real children below CTE_LOOKUPs,
and CTE-rooted real chains without any fix.  Zero code changes end to
end — the first phase to ship as pure coverage.**

**Parent:** `non_aggregate_pushdown_plan.md` (Phase 5 overview).
**Predecessors:** Phases 0-4 + nest-semantics Parts A/B.

## Goal

The final roadmap phase: grouped (GROUP BY) aggregating CTEs joined
anywhere into the projection-only snowflake/star trees of Phases 2-3 —
the general form of what I.8/I.11/I.12 pioneered on fixed shapes:

```sql
WITH per_cust AS (SELECT o_custkey AS k, COUNT(*) AS cnt
                  FROM orders GROUP BY o_custkey)
SELECT c.c_custkey, n.n_name, per_cust.cnt
FROM customer AS c
JOIN nation AS n ON n.n_nationkey = c.c_nationkey   -- real arm
JOIN per_cust   ON per_cust.k = c.c_custkey         -- grouped CTE arm
WHERE c.c_custkey < 10;
```

## How the research reshaped the parent plan's sketch

The parent sketch expected "removing the remaining CTE-specific
asymmetries".  The audit found **none left** — Phase 5 is a pure
coverage phase, with zero code changes expected up front:

1. **Gate**: the `projection_only_join_chain` walk (I.8 heritage,
   generalized in Phases 2-4) already accepts grouped CTE operands via
   the `cte_key_coverage` `ExactOrdered`/`ExactPermuted` check —
   grouped complete-key CTE_LOOKUPs need no gate change at all.  The
   rejection message already covers them ("complete-key CTE lookups"),
   so **no baseline churn** this phase.
2. **Planner**: sibling re-chaining (`tree_parent_op_idx` loop) is
   type-general — keyed CTE_LOOKUPs sharing a parent re-chain exactly
   like the aggregate path.  The Phase 2 linked-column identity
   pre-check is guarded `childOp.table != NULL && parent.table !=
   NULL`, so CTE-linked ops skip it cleanly.  Mixed-parent ON
   conditions (a multi-key CTE keyed off two different aliases) are
   already rejected with a clear planner error ("All ON conditions in
   a single JOIN must reference the same parent table") — supporting
   per-key parents is feasible later (NdbQueryBuilder's `linkedValue`
   accepts any ancestor) but out of scope.
3. **Emit**: the generic child-op path pins `tree_parent_op_idx` when
   it differs from the key source (RonSQLPreparer.cpp:8935), covering
   chained keyed CTE siblings; the CTE_LOOKUP filter arm, drain
   registration, and printer metadata plumb are all op-generic.
4. **The D5 rejection does not apply here.**  The
   `linked_source_is_leaf_ancestor` guard (cte_fix_plan.md H6) rejects
   *aggregation* linked projections from non-ancestor siblings.  A
   pass-through query has no aggregator and no linked projections —
   only join keys, each linked from its direct parent — so the
   CTE-branch + real-branch star topology that D5 made famous is legal
   on this path (gc-3 covers it).

## Novel-topology probes (first-ever on either path)

Three shapes have never been exercised anywhere; they land as compare
cases and the first `--record` decides whether they work:

- **gc-7 — two grouped CTEs keyed on *different* parents** (one on the
  root, one on a lookup child).  The planner's re-chaining only
  linearizes same-parent siblings; different-parent CTE_LOOKUPs stay
  in separate branches, and SPJ's acceptance of that topology is
  unproven.
- **gc-8 — a real PK lookup *below* a CTE_LOOKUP**, keyed on the CTE's
  output column (the pass-through twin of testCteNdbApi Test 13's
  "lookupCte as internal node" kernel topology).
- **gc-9 — CTE_SCAN root with a two-level real chain below**
  (extends I.12's single-child `scanCte JOIN readTuple` shape).

If a probe fails, the established rhythm applies: user reports the
failure, we fix (or gate-reject with a clear error) and re-record.

## Work items

### W1 — MTR family (the whole phase)

`body_passthrough_groupby_cte.inc` + one-line wrappers ×5 suites
(gc-N), on the shared TPC-H-lite schema plus a local 2-column table
`gckey` for multi-key CTE joins (the shared schema has no real table
carrying both halves of a 2-key group):

| # | Case |
|---|---|
| gc-1 | root real scan + grouped CTE arm (the I.8 basic shape in the data-driven suite) |
| gc-2 | snowflake chain with the CTE keyed on a lookup child (depth 2) |
| gc-3 | star: real branch (nation) + CTE branch on the same root — the D5 fan-out topology, legal here because pass-through has no cross-branch aggregation reads |
| gc-4 | CTE keyed on an INDEX_SCAN child's column (CTE under a scan child) |
| gc-5 | LEFT JOIN grouped CTE with NULL groups (body WHERE restricts groups; drain `isRowNULL` substitution prints NULLs) |
| gc-6 | two grouped CTEs on the same parent — keyed sibling re-chaining (`tree_parent_op_idx`); second CTE grouped over the *nullable* `o_clerk` |
| gc-7 | **probe**: two grouped CTEs on different parents (separate branches) |
| gc-8 | **probe**: real PK lookup below a CTE_LOOKUP, keyed on the CTE output |
| gc-9 | **probe**: CTE_SCAN root + two-level real chain below, WHERE on the CTE output at the root |
| gc-10 | multi-key grouped CTE (`GROUP BY o_custkey, o_clerk`) joined complete-key in *permuted* order via `gckey` (I.20 reorder on the pass-through path) |
| gc-11 | WHERE mix: root PK bound + CTE-output conjunct (jump-table CTE_LOOKUP filter) |
| gc-12 | DATE + DECIMAL grouped outputs through the pass-through printer (`is_date` decode, scale-2 DECIMAL) |
| gc-13 | VARCHAR MIN/MAX grouped outputs through the pass-through printer (F.2/F.3 string machinery on the row path) |
| gc-P1 | rejection: partial-key join to the 2-key CTE (generic gate message — the aggregate path's I.16 rewrite never runs because the gate rejects at parse) |
| gc-P2 | rejection: multi-key CTE keyed off two different aliases (clean planner error) |
| gc-P3 | rejection: non-aggregating (projection-only) CTE body (`analyze_ctes` stands) |

### W2 — docs + findings

`findings/passthrough_groupby_cte.md` carries the probe outcomes and
the kept restrictions; CLAUDE.md index entry; parent-plan Phase 5
notes.

## Kept restrictions (pinned, not lifted)

- ~~Partial-key CTE joins (gc-P1)~~ — **LIFTED by
  `non_aggregate_phase_6.md`** (August 2026): the I.16b/c root-rewrite
  now runs pre-gate for non-aggregate queries (gc-14..16); residual
  partial shapes throw the clean I.16a-mirror message (gc-P1a/b/c).
- ~~Multi-key CTEs keyed off multiple aliases (gc-P2)~~ — **LIFTED by
  `non_aggregate_phase_6.md`**: per-key parent sources in QueryPlanner
  + emit, both paths (gc-17..20); sibling-branch key sources stay
  rejected with a clean planner error (gc-P2b).
- Non-aggregating CTE bodies (gc-P3) — CTE bodies must aggregate;
  lifting this is a separate kernel-facing feature.
- `CTE_SCAN` as a keyed join child — inexpressible through the gate
  (CTE operands must be complete-key CTE_LOOKUPs); Phase G's defensive
  reject stands for planner-produced shapes.

## Verification (user-run)

- Rebuild ronsql (no kernel change), then from `debug_build/mysql-test`:
  `./mtr --record --suite=ronsql_cte ronsql_cte_dd_passthrough_groupby_cte`
  + the same for the 4 topology siblings; then a plain full regression
  of the `ronsql_cte*` + `ronsql` suites.
- Watch the three probes (gc-7/8/9) on first record — they are the
  phase's discovery surface.  gc-5's NULL groups and gc-12/13's
  typed outputs are the printer's risk spots.
