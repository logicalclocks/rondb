# Phase 6 — pass-through CTE-join envelope: partial-key rewrite + per-key parents

**Status: SHIPPED (2026-08-25, recorded green ×5 topology suites +
regression pass).**  Lifts the two "kept restrictions" from
`non_aggregate_phase_5.md`: partial-key CTE joins on the pass-through
path (gc-P1) and multi-key CTEs keyed off multiple aliases (gc-P2).
Two independent changes, RonSQL-only — kernel and NdbQueryBuilder
untouched.

## 6a — partial-key CTE joins via the pre-gate I.16b/c rewrite (gc-P1)

Root cause: the I.16b/c root rewrite (promote a partial-key INNER
multi-key-CTE join to CTE_SCAN root, demote the original root to a
keyed child) lived at the top of `load_join()`, but the projection-only
parse gate runs earlier in `parse()` and rejected `Partial` coverage
before the rewrite could fire.  Key insight: the **post-rewrite tree
needs no gate relaxation** — a CTE_SCAN root with a real-table child
chain is the shipped gc-9 shape, and demoted real-table operands get no
coverage check in the gate walk.

As implemented:

- The rewrite block is extracted verbatim into
  `RonSQLPreparer::maybe_rewrite_partial_key_cte_root()` (pure AST +
  `m_amalloc`; `cte_key_coverage` is AST-only, already called at parse
  time by the gate).  One addition: the demoted-root JoinClause copy
  clears `hint_kind`/`hint_indexes` — on the aggregate path
  `reject_index_hints_on_joins` had already run (root hints were
  silently dropped); on the new pre-gate call it runs later and would
  otherwise newly trip on the copy.
- `parse()` calls the helper inside `if (!m_is_aggregate_query)`,
  before the gate's `from_is_cte` computation.  The `load_join()` call
  stays — aggregate-path timing is byte-identical, and for rewritten
  pass-through queries the second call no-ops via the `root_is_cte`
  bail.  (`ast_root.table` keeps the pre-rewrite FROM name, as the
  shipped aggregate rewrite always did.)
- The gate's CTE arm now throws targeted errors for post-rewrite
  residual states instead of the generic "Not an aggregate query"
  text: `Partial` → the I.16a-mirror message (covers LEFT JOIN on the
  partial CTE join, non-root parent alias, a second partial CTE,
  CTE-on-CTE roots), `WrongColumns` → the I.20-mirror message.  The
  generic rejection text is untouched (baked into many baselines).
- Planner/emit/drain/EXPLAIN: zero changes.

## 6b — per-key parent sources (gc-P2)

Root cause: `QueryPlanner::plan` resolved ONE parent per JOIN from the
first ON condition and rejected any later condition naming a different
alias.  NdbQueryBuilder's `linkWithParent` already accepts any
*ancestor* per bound key, resolving the operation's effective parent
to the deepest key source (unrelated branches fail with
`QRY_MULTIPLE_PARENTS`); per-key `P_PARENT` patterns are standard in
DBSPJ, and `ndb_opt/join_pushdown.inc` ("Test multiparent pushed
joins") is the kernel-path precedent.

As implemented:

- `JoinOp` gains `key_parent_op_idx[MAX_JOIN_KEY_COLS]` (per-key key
  source, the `RangeBound::parent_op_idx` idiom); `parent_op_idx` is
  re-documented as "effective parent = deepest key source".
- `QueryPlanner::plan` resolves every ON condition's parent alias,
  drops the same-parent rejection, computes the effective parent as
  the deepest referenced op and requires every other source on its
  ancestor chain (walking `parent_op_idx`; tree_parent ==
  parent for every op built at that point — the sibling-CTE re-chain
  only deepens chains).  Violations throw the clean "single ancestor
  chain" error, firing before the API's `QRY_MULTIPLE_PARENTS` would.
  The Phase 2 linked-column identity pre-check runs per key against
  the key's own source table (CTE sources skipped as before).
- `emit_child_ops` links each key from
  `opDefs[op.key_parent_op_idx[k]]` (both the plain `keys[]` build and
  the INDEX_SCAN bounds-path equality prefix); the `setParent` pin is
  unchanged (a tree-parent pin is always at-or-below every key source,
  so the builder's grandparent replacement never clobbers it).  The
  CTE_LOOKUP arm rides the shared `keys[]` array unchanged.
- EXPLAIN's per-op `Key:` line prints each key's own source alias —
  byte-identical for single-parent plans.
- The lift lands in the shared planner, so the **aggregate path
  inherits it** — deliberate; gc-20 pins that side.

## MTR (`body_passthrough_groupby_cte.inc` ×5 topology suites)

gc-14 = the exact former gc-P1 probe as a compare, with EXPLAIN greps
pinning `[ROOT] CTE_SCAN CTE:pt` + `PK_LOOKUP gckey AS g`; gc-15 3-key
CTE with 1-of-3 keys; gc-16 partial CTE + extra real join below the
demoted root.  gc-17 multi-alias 2-key CTE (data-rich: the gckey
(g_a, g_b) pairs hit real groups) with a `Key: k = cu.c_custkey,
cl = g.g_b` grep; gc-18 the exact former gc-P2 probe (empty result by
mod-arithmetic — proof in the findings file); gc-19 LEFT JOIN
multi-alias with a missing group (NULL-extended row); gc-20 aggregate
twin (SUM over the multi-alias CTE join).  New pinned rejections:
gc-P1a LEFT-on-partial, gc-P1b non-root partial, gc-P1c wrong-column
full-count, gc-P2b sibling-branch key sources.  (The aggregate twin
lives in this family rather than extending `ronsql_cte_partial_key`'s
fixture — no cross-suite re-record needed.)

## Verification (user-run)

- Build `ronsql_cli` + `rdrs2` (RonSQL-only).
- `./mtr --record --suite=ronsql_cte
  ronsql_cte_dd_passthrough_groupby_cte` + the same in
  `ronsql_cte_ng1r3/ng2r2/ng2r3/ng4r2`.
- Regression: full `ronsql` + `ronsql_cte` suites (the planner change
  is byte-identical for single-parent plans; the rewrite change is
  gated to non-aggregate queries + the aggregate-path call site is
  unchanged in timing), plus `ndb_push_agg`.

## Deferred (documented, pinned where noted)

- Multi-alias **partial**-key CTE joins — the rewrite still requires
  the original root as the partial join's single parent (gc-P1b).
- Sibling-branch key sources (gc-P2b) — would need artificial
  dependency pinning à la `ha_ndbcluster_push`.
- LEFT JOIN on the partial-key CTE join itself (gc-P1a) — semantics
  (the swap would lose unmatched rows), both paths.
- CTE-on-CTE root rewrite; non-root CTE_SCAN children (permanent API
  hold); gc-P3 non-aggregating CTE bodies (kernel-facing).
