# Join-Query Root Index Scan (fs_batch TABLE_SCAN fix)

**Status: Changes 1-4 SHIPPED — build green, MTR family recorded and
green across all 5 topology suites.  Change 5 (multi-op CTE bodies)
still pending as a follow-up commit.**

Recording surfaced one test-shape issue (not a code regression): the
original mri-9 used a mixed-type col-vs-col conjunct (INT c_custkey vs
TINYINT c_nationkey); the interpreter's attr-vs-attr compare requires
identical column types, so the residual filter failed with retryable
"Failed to apply filter." — identically on the pre-change scanTable
path.  mri-9 now uses the all-INT acct table with the col-vs-col
conjunct first (so the generator hardening is the deciding factor);
the mixed-type gap is recorded as a NEXT-PHASE probe in the .inc.

Implementation notes beyond the plan below:

- `select_cte_body_scan_config` was renamed `select_root_scan_config`;
  the `num_ops == 1` gate moved to the CTE call site in
  `build_cte_scopes`.  The main-scope call sits at the end of
  `load_join()`, guarded by `root_pk_equality_covered()` (bypassed under
  FORCE INDEX so the hint can win over the PK-lookup branches — the new
  emit branch is checked before them).
- The shared bound/residual emit is `emit_index_scan_root()`; the I.9
  CTE-body branch now calls it too (single implementation).
- **Generator hardening found during implementation:**
  `build_scan_config_candidates` accepted any `col <op> <anything>`
  conjunct as a bound without checking the right side; a col-vs-col
  conjunct (`WHERE t.a > t.b`) or unfolded expression would reach
  `encode_constant` at emit and throw "Bug in ...".  Latent on the
  single-table path too.  Now only right sides of kind
  T_INT / T_FLOAT / T_STRING / I_MYSQL_TIME / I_SUBQUERY are
  bound-eligible; everything else routes to the residual filter
  (`apply_filter_cmp` supports col-vs-col via the two-column
  `NdbScanFilter::cmp`).
- `NdbQueryOptions` needed a forward declaration in RonSQLPreparer.hpp
  (NdbQueryBuilder.hpp is a src-side header not pulled in via
  NdbApi.hpp).
- Tests: new family `suite/ronsql_cte/include/body_main_root_index.inc`
  (cases mri-1..10 + probe mri-P1) sourced by
  `ronsql_cte_dd_main_root_index.test` in ronsql_cte + the 4 topology
  siblings.  Includes a dedicated `acct` table with an ORDERED primary
  key (shared schema is hash-PK-only) for the fs_batch PRIMARY-range
  shape.  Baselines need `./mtr --record` per suite.

## Trigger

`.explain_ronsql fs_batch` shows the main-query root as
`[ROOT] TABLE_SCAN customer AS c` even though the query has
`WHERE c.c_custkey >= {KEY} AND c.c_custkey < {KEY2}` — a double-bounded
range on the leading (and only) column of the PRIMARY ordered index. The
root scans all 150k customer rows (SF=1) per request with an interpreted
filter, when a ~100-row PRIMARY index range scan would do. Ironically the
CTE body of the same query *does* get `INDEX_SCAN orders using
idx_orders_custkey` — Phase I.9 covered CTE bodies but not the main query.

## Root cause

Three-link chain, all in `RonSQLPreparer.cpp`:

1. `plan_index_and_filter()` — the original index-selection machinery
   (candidates, goodness scoring, `m_scan_config`) — is called **only when
   `!is_join_query()`** (line ~184). It feeds the single-table
   `NdbIndexScanOperation` execution path. Any query with a JOIN (including
   every CTE join) skips it entirely.
2. `load_join()` never discovers the root table's ordered indexes
   (`m_indexes` is populated only by `load_single_table()`).
3. `emit_root_op()`'s real-table branch (line ~6912) recognises only
   **PK equality** cover:
   - full-PK equalities + no scan child → `readTuple` (PK lookup root);
   - full-PK equalities + scan child → `scanIndex` on the PRIMARY ordered
     index with an equality bound (line ~6990);
   - **everything else → `scanTable` + NdbScanFilter** (line ~7010).

   Range predicates (`>=` / `<`) are never equalities, so
   `collect_pk_equalities` yields `pk_covered = false` and the root becomes
   a filtered table scan. Equality on a *secondary*-indexed column (e.g.
   `WHERE c_nationkey = 7`) falls through the same way — it is not a PK
   equality and no secondary-index path exists for the root.

## Affected query shapes

Any join query (CTE or real-table joins) whose root-table WHERE conjuncts
could bind an ordered index:

| Benchmark | Root WHERE | Usable index (unused today) |
|-----------|-----------|------------------------------|
| fs_batch, fs_freshness | `c_custkey >= K AND c_custkey < K2` | PRIMARY (customer) |
| fs_supplier | `s_nationkey = K` | idx_supplier_nationkey |
| fs_nation, fs_topk | `c_nationkey = K` | idx_customer_nationkey |
| tpch_q11 (CTE rewrite) | `s_nationkey = 7` | idx_supplier_nationkey |

Not affected: fs_point (CTE_SCAN root), tpch_q2 (`p_size` has no index —
table scan is unavoidable), offline_fs_* (no root WHERE). The same gap
also applies to **multi-op CTE bodies** (they emit through `emit_root_op`
too, line ~6253, and `select_cte_body_scan_config` gates on
`num_ops == 1`) — see Change 5.

## Existing infrastructure (all reusable)

- **Shared candidate generator** `build_scan_config_candidates(indexes,
  toplevel_conditions, out_candidates, hint)` (line ~2294): pushes a
  TABLE_SCAN fallback candidate plus one candidate per ordered index any
  `col <op> const` conjunct can bind (T_EQUALS / T_GE / T_GT / T_LE /
  T_LT; composite leading-column coverage; only the last bound column may
  be a range). Column matching uses the **bare** column name
  (`m_columns[col_idx]`) — alias qualifiers live in the parallel
  `m_column_qualifiers` array, so `c.c_custkey` matches index column
  `c_custkey` unchanged. Safe here because the conjuncts passed in are
  exactly those classified to op 0.
- **Phase I.9 selector** `select_cte_body_scan_config(scope, where_ce,
  hint)` (line ~2697): loads per-scope indexes, AND-flattens the WHERE
  into `scope.body_toplevel_conditions`, runs the generator, picks the
  best candidate into `scope.body_scan_config`, and flips
  `plan.ops[0].type = INDEX_SCAN` + `ops[0].index`. Gated on
  `plan.num_ops == 1` (CTE-body single-op only).
- **Phase I.9 emit block** (line ~6121-6221 in the CTE-body INDEX_SCAN
  branch): builds `NdbQueryIndexBound(lowKeys, lowIncl, highKeys,
  highIncl)` chains from `body_toplevel_conditions` +
  `body_scan_config->condition_handling_map`, routes residual conjuncts
  (map == -1) into an NdbScanFilter. Includes the D9 DATE bound-encoding
  handling via `encode_constant`.
- **QueryScope already carries the per-scope fields for the main scope
  too** — `body_indexes` / `body_toplevel_conditions` /
  `body_scan_config_candidates` / `body_scan_config` are documented as
  "NULL for the main scope" (RonSQLPreparer.hpp ~246). The fix simply
  starts using them for `m_main_scope`.
- **Root `scanIndex` through `execute_join` already ships** (the
  PK-equality-with-scan-child branch, line ~6990), so NdbQueryBuilder /
  DBSPJ / receiver handling of an index-scan root in a join-agg query is
  already exercised. Index-scan root row counting also already respects
  the `m_aggNodes` suppression rule (scan-side rule at
  DbspjMain.cpp:13173, cf. the suppressed-nextreq fix) — **zero kernel or
  NDB API work is expected.**

## Fix design

### Change 1 — plan-time selection for the main-scope root

Generalise `select_cte_body_scan_config` into a scope-agnostic
`select_root_scan_config(QueryScope& scope, ConditionalExpression*
where_ce, const TableRef* hint)`:

- Drop the `plan.num_ops != 1` gate from the shared core; keep the
  remaining gates (`ops[0].type == TABLE_SCAN`, real table, ndb + dict
  available, `where_ce != NULL`).
- Call it for the main scope at the **end of `load_join()`** (after
  `classify_where_by_table`, `promote_left_to_inner_for_where`,
  `assign_cross_table_index_bounds`, and the subquery-filter merge, so
  `join_where_ce[0]` is final):

  ```
  if (m_main_scope.join_plan.ops[0].type == JoinOp::TABLE_SCAN &&
      !root_pk_equality_covered(m_main_scope))   // see ordering note
    select_root_scan_config(m_main_scope,
                            m_main_scope.join_where_ce[0],
                            m_context.ast_root.root_table);
  ```

- **Ordering with the existing PK-equality branches:** the selector must
  not run when the root PK is fully equality-covered — those queries keep
  today's `readTuple` / PK-equality-`scanIndex` emit paths untouched.
  Replicate the `collect_pk_equalities` check at plan time (cheap) as the
  `root_pk_equality_covered` guard, so `ops[0].type` stays TABLE_SCAN for
  them and EXPLAIN doesn't lie about what emit chooses.
- The existing CTE-body call site keeps its `num_ops == 1` gate (either
  at the call site in `build_cte_scopes` or via a thin wrapper) so CTE
  behaviour is unchanged by this change (Change 5 relaxes it separately).
- Note the WHERE input difference: CTE bodies pass the body's whole WHERE;
  the main scope passes only `join_where_ce[0]` (root-classified
  conjuncts) — cross-table and child-table conjuncts are already routed
  elsewhere and must not be offered as root bounds.

### Change 2 — emit: INDEX_SCAN branch in `emit_root_op`

Factor the I.9 bound-building + residual-filter block (lines ~6121-6221)
into a shared helper, e.g.

```
const NdbQueryOperationDef*
emit_index_scan_root(NdbQueryBuilder* qb, QueryScope& scope,
                     const NdbDictionary::Table* tab,
                     const NdbDictionary::Index* idx,
                     NdbQueryOptions& opts);
```

reading `scope.body_toplevel_conditions` + `scope.body_scan_config`.
Then in `emit_root_op`'s real-table section, after the two `pk_covered`
branches and before the `scanTable` fallback:

```
if (plan.ops[0].type == JoinOp::INDEX_SCAN &&
    scope.body_scan_config != NULL &&
    scope.body_scan_config->index == plan.ops[0].index) {
  opDefs[0] = emit_index_scan_root(qb, scope, root_table,
                                   plan.ops[0].index, rootOpts);
  return;
}
```

The I.9 CTE-body branch switches to the same helper (single
implementation of bound building — the D9 DATE encoding fix and the
half-open-column truncation invariant live in exactly one place).
The `scanTable` fallback branch is unreachable for the new case (early
return), so no double-filtering: bound conjuncts become index bounds,
residual conjuncts (`condition_handling_map == -1`) become the
interpreted filter, and `join_where_ce[0]` is **not** applied wholesale.

Behavioural notes:
- Like today's `scanTable` root branch, the new branch never attaches
  `singleAgg` — for real-table-rooted join queries the aggregation leaf
  is always a child op.
- Subquery placeholders substitute in place into the shared CE nodes
  before `execute_join()` runs, and `body_toplevel_conditions` stores
  pointers to those same nodes — `encode_constant` at emit time sees the
  substituted constants. Same semantics as the existing single-table
  `m_scan_config` path.

### Change 3 — EXPLAIN: print root bounds

The join-plan printer (line ~11424) already prints `INDEX_SCAN` +
`Index: name(cols)` for any op with `op.index` set, so the op line fixes
itself. Add a root `Bounds:` line rendered from
`body_scan_config->condition_handling_map` (constant bounds, e.g.
`c_custkey >= <const> AND c_custkey < <const>`), and a `Filter: N residual
conjunct(s)` note — child ops already print their parent-linked bounds;
the root today prints nothing about condition routing.

### Change 4 — index hints on join-query roots

Today a `FORCE/USE/IGNORE INDEX` hint on the **root of a join query** is
accepted by the parser (`reject_index_hints_on_joins` only rejects joined
tables) but silently ignored, because the only hint consumer is the
never-called-for-joins `plan_index_and_filter`. Passing
`m_context.ast_root.root_table` as the hint in Change 1 makes the W3
"root scans honor hints" contract hold for join queries too. Behaviour
change to call out: a previously-ignored `FORCE INDEX` naming an unusable
index will now throw the standard FORCE error — desired, but needs test
coverage and a line in the commit message.

### Change 5 (sequenced follow-up commit) — multi-op CTE bodies

With `emit_root_op` handling INDEX_SCAN roots generically, relaxing the
CTE call-site gate from `num_ops == 1` to "ops[0] is a real-table
TABLE_SCAN" gives multi-op CTE bodies (e.g. a CTE over
`lineitem JOIN orders WHERE l_shipdate >= ...`) the same treatment for
free. Keep it a separate commit so main-query and CTE-body regressions
bisect independently.

## Risks / audit list for implementation

- **Downstream consumers of `ops[0].type`:** grep the main-scope uses of
  `JoinOp::TABLE_SCAN` / `ops[0].type` outside the selector + emit (e.g.
  planner validation from I.11, linked-projection setup, filter emission
  loop at ~8243 which starts at child ops). CTE_SCAN/CTE_LOOKUP dispatch
  in `emit_root_op` happens before the real-table section, unaffected.
- **`m_scan_config == NULL` gates:** the EXPLAIN join-plan block prints
  under `m_scan_config == NULL && num_ops > 1` — main-scope selection
  writes `body_scan_config`, not `m_scan_config`, so that gate is
  untouched; verify no other code treats `m_scan_config != NULL` as "is
  single-table query".
- **Goodness heuristic** may pick a different index than MySQL's
  optimizer would; identical to the risk already accepted for
  single-table and CTE-body paths (and hint-overridable after Change 4).
- **Multi-batch coverage:** index-bounded roots examine far fewer rows,
  so keep the existing `ronsql_large` multi-batch regression on a
  no-root-WHERE shape (don't accidentally shrink the batch-protocol net
  from the suppressed-nextreq fix).
- Update the `QueryScope` comment (hpp ~241-249) that claims `body_*`
  is "NULL for the main scope".

## Verification plan

1. Build (user runs); no kernel targets — mysqld/RDRS-side RonSQL only
   (`RonSQLPreparer.cpp/.hpp` + librdrs relink).
2. New MTR `ronsql_root_index_scan.test` (main suite, plus reuse
   `ronsql_explain.inc`):
   - fs_batch shape: PK range on root + CTE join → EXPLAIN shows
     `[ROOT] INDEX_SCAN ... using PRIMARY` + bounds; results identical to
     MySQL via `ronsql_compare.inc`.
   - Secondary-index equality root (fs_nation shape) → INDEX_SCAN using
     the nationkey index.
   - Residual routing: root WHERE with one bindable range + one
     non-indexable conjunct (e.g. `c_acctbal > 0`) → bounds + filter,
     correct results.
   - Regressions: no-WHERE root stays TABLE_SCAN; full-PK-equality root
     stays PK_LOOKUP/readTuple path; LEFT JOIN on the CTE unchanged.
   - Hints: `FORCE INDEX (PRIMARY)` on a join root now honored;
     `FORCE INDEX` unusable → error; `IGNORE INDEX (PRIMARY)` → falls
     back to TABLE_SCAN.
3. Full regression net: `./mtr --suite=ronsql_cte` + topology siblings
   (ng1r3/ng2r2/ng2r3/ng4r2), `ronsql` suite, `ronsql_large`.
4. Benchmark before/after: `.bench_ronsql fs_batch 4 100` and
   `.explain_ronsql fs_batch` — expect the root scan to drop from ~150k
   rows examined to ~KeySpan rows; compare against `.bench_sql fs_batch`
   (MySQL uses the PK range already, so the RonSQL-vs-MySQL gap should
   close by roughly the customer-scan cost).

## Files touched (expected)

- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` — selector rename/signature,
  QueryScope comment.
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — `select_root_scan_config`,
  `load_join()` call site, `emit_index_scan_root` helper, `emit_root_op`
  branch, EXPLAIN root-bounds print.
- `mysql-test/suite/...` — `ronsql_root_index_scan.test/.result` (+ hint
  cases), possibly an `ronsql_cte` sibling case re-enable if any
  `# NEXT-PHASE` marker matches this shape.
