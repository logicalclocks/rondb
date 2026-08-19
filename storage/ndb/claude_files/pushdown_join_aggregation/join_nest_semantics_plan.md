# Join Nest Semantics — Verification + Fix Plan

**Status: Part A COMPLETE — verdict: CONFIRMED WRONG on the aggregate
path (ns-1 recorded COUNT(*) = 30 vs MySQL's 10; both the lookup-miss
and NULL-key sub-cases counted, settling both open cells with one
number).  Part B is therefore a CORRECTNESS fix for shipped aggregate
functionality, not just a quality improvement.  Parts B/C not yet
implemented; ns-1..4 NEXT-PHASE-disabled pending B.**

Part A addendum — reconciliation with the evidence table below: Test 2
of `testMultiOuterJoinAggNdbApi` is green with the same tree shape but
its aggregate LEAF is a LEFT node *below* the INNER, whereas ns-1's
INNER node *is* the leaf.  The kernel NULL-injection's INNER-awareness
evidently depends on the leaf position — worth verifying in the kernel
when Part B lands (Part B makes the flat-SQL case moot, but the
injection behavior still matters for future genuine nests).
**Trigger:** the Phase 2 sn-15 probe (`non_aggregate_phase_2.md`):
`orders LEFT JOIN customer ON c_custkey = o_clerk JOIN nation ON
n_nationkey = cl.c_nationkey` returned the LEFT JOIN's NULL-extended
rows that MySQL eliminates, and the aggregate join path was suspected
of the same defect.

## The analysis — the problem is associativity, not (primarily) nest metadata

Research for this plan reframed the sn-15 finding:

1. **SQL joins are left-associative.**  `o LEFT JOIN cl ON X JOIN n ON
   n.ref(cl)` means `(o LEFT JOIN cl) INNER JOIN n` — and because the
   INNER join's condition is a null-rejecting equality on `cl`'s
   columns, the whole expression is equivalent to
   `o INNER cl INNER n`.  MySQL's optimizer performs exactly this
   LEFT→INNER simplification (`simplify_joins`).
2. **RonSQL's emitted tree says something else.**  Making `n` a
   `MatchNonNull` child of `cl` expresses `o LEFT (cl INNER n)` — a
   *different query* (one only writable in SQL with a parenthesized
   join nest, which RonSQL's grammar does not have).  For that query,
   delivering the NULL-extended row when the `{cl, n}` nest is empty is
   *correct*.  DBSPJ answered the wrong question correctly.
3. **Nest metadata is for genuine nests.**  ha_ndbcluster's
   `build_query` comment (`ha_ndbcluster_push.cc:2589-2632`) states the
   purpose of `setFirstInnerJoin`/`setUpperJoin`: cases "where there
   are no linkedValues determining which inner_ and upper_nest a table
   is a member of", e.g. `t1 LEFT JOIN (t2 INNER t3)` where t3's
   condition doesn't reference t2 — and "the API will just ignore such
   redundant nest dependencies" otherwise.  Since RonSQL cannot parse
   parenthesized join expressions, **every INNER-below-LEFT arising
   from RonSQL's flat chains is promotable to INNER**, and nest
   metadata emission is not required to fix anything shipping today.

### Evidence table

| Observation | Path | Sub-case | Result |
|---|---|---|---|
| sn-15 first record (Phase 2) | pass-through row delivery | LEFT link on a **NULL key** (`o_clerk IS NULL`); all non-NULL keys matched | **WRONG** — NULL-extended rows delivered that `(o LEFT cl) INNER n` eliminates |
| `testMultiOuterJoinAggNdbApi` Test 2 (`scan → LEFT lkp → INNER lkp → LEFT lkp`) | aggregate (kernel NULL-injection) | **lookup miss** on the LEFT (region with no store; non-NULL key) — data genuinely exercises it, expectations follow MySQL's left-associative semantics, options identical to RonSQL's emission (MatchAll + MatchNonNull, no nest info) | **GREEN** — the kernel's aggregate NULL-injection appears INNER-below-aware for misses |
| `testMultiOuterJoinAggNdbApi` Test 7 (`scan → LEFT scan → INNER lkp → LEFT lkp`) | aggregate | scan intermediate variant | GREEN |
| — untested — | aggregate | LEFT link on a **NULL key** | **open** (Part A closes this) |
| — untested — | pass-through row delivery | **lookup miss** on the LEFT | **open** (sn-15's data had no dangling non-NULL keys; Part A closes this) |

So the earlier "aggregate path suspected latent bug" claim is narrower
than first recorded: the miss case is proven green at block level; only
the NULL-key aggregate case and the miss-case row path are open cells.

## Part A — verification tests (run before any fix)

New family `body_nest_semantics.inc` +
`ronsql_cte_dd_nest_semantics.test` ×5 suites.  Aggregate queries skip
the parse gate, so the aggregate probes are runnable **today**; the
first `--record` run is the verification.  A family-local probe table
separates the two sub-cases:

```sql
CREATE TABLE nestp (
  p_id INT NOT NULL,
  ref  INT NULL,      -- three row classes:
  PRIMARY KEY (p_id)  --   matching refs   (1..N  -> existing c_custkey)
) ENGINE=NDB;         --   dangling refs   (9001..: no such customer)
                      --   NULL refs
```

| # | Case | Closes |
|---|---|---|
| ns-1 | `SELECT COUNT(*) FROM nestp AS t LEFT JOIN customer AS cl ON cl.c_custkey = t.ref JOIN nation AS n ON n.n_nationkey = cl.c_nationkey` — full mix | headline aggregate verdict |
| ns-2 | same, `WHERE t.p_id` sliced to NULL-ref rows only | **aggregate × NULL-key** (the open cell) |
| ns-3 | same, sliced to dangling-ref rows only | aggregate × miss (expected green per Test 2) |
| ns-4 | ns-1 with `GROUP BY` on a root column | grouped variant |
| ns-5 | ns-1 with `COUNT(n.n_nationkey)` | COUNT(col) NULL semantics |
| ns-6 | control: same chain all-INNER | baseline |
| ns-7 | control: `LEFT ... LEFT` chain (no INNER below) | already-supported semantics unchanged |

Interpretation of the record run: any diff pinpoints the exact broken
sub-case; all-green clears the aggregate path entirely and leaves only
the pass-through row path (already gate-rejected, so nothing shipping
is wrong).  Either way the family stays as the regression net for
Part B, with any diffing case converted to a `# NEXT-PHASE` marker
until Part B lands (the established probe workflow).

The pass-through twins (sn-15's query and a dangling-ref variant)
cannot run yet — the Phase 2 gate rejects them — and get enabled as
compares in Part B.

## Part B — the fix: join-condition LEFT→INNER promotion

The correct, complete fix for every flat-chain shape, on both paths:

**Rule.**  If a join J (any type) has an ON condition referencing an
alias B that was LEFT-joined (directly or transitively under-LEFT),
and J is INNER — or J itself gets promoted by this rule — then B's
LEFT join promotes to INNER.  Iterate to a fixpoint (a promotion can
cascade up the referenced chain: `A LEFT B LEFT C` + `INNER D ON
D.ref(C)` promotes C, whose ON references B, promoting B).
Justification: RonSQL join conditions are equality-only, hence
null-rejecting on the referenced alias — the textbook `simplify_joins`
condition.  LEFT joins referenced only by later LEFT joins stay LEFT
(ns-7 semantics are correct today).

**Placement.**  An AST pre-pass over `ast_root.joins` in `parse()`,
*before* the non-aggregate gate — so it applies to aggregate queries,
pass-through queries, and the EXPLAIN parse-tree print alike (mutating
`join->join_type`, exactly like the Phase J WHERE-based promotion at
`RonSQLPreparer.cpp:~1600` with its null-rejecting helper at `:1861`,
and the I.16 `load_join` rewrite precedent).

**Consequences.**

- sn-15's query becomes `o INNER cl INNER n` — correct results and a
  cheaper plan (no NULL-extension work).  Re-enable it as a compare.
- The Phase 2 gate's INNER-below-LEFT targeted rejection becomes
  unreachable for equality conditions; keep it as a defensive
  dead-man's check (future non-equality join conditions).
- Any latent aggregate divergence in these shapes disappears by
  construction — post-promotion, flat SQL can never emit an
  INNER-below-LEFT tree.
- Part A's family flips to an all-green regression net; add the
  pass-through twins; MTR EXPLAIN check that the parse-tree print
  shows the promoted INNER (mirroring MySQL EXPLAIN's simplified
  form).

**Non-goals of the promotion**: WHERE-based promotion (Phase J owns
it); semi/anti interactions (none exist in the accepted envelope).

## Part C — nest-metadata emission (deferred design)

Needed only when RonSQL can express a genuine join nest — i.e. when
the grammar gains parenthesized join expressions
(`A LEFT JOIN (B JOIN C ON ...) ON ...`), or an INNER join whose
condition references only an alias *above* the LEFT (which the flat
walk maps to fan-out, not a nest).  Design sketch for that day:

- `QueryPlanner` computes per-op nest indices: each LEFT child starts
  a nest (`first_inner = self`, `upper = parent's first_inner`); INNER
  children inherit the parent's nest.
- `emit_child_ops` emits, on outer-joined ops only (per
  `ha_ndbcluster_push.cc:2589-2632`): `setFirstInnerJoin(op)` when the
  table is not first in its nest, else `setUpperJoin(first_upper_op)`.
  The API validates ancestor-or-sibling placement
  (`QRY_NEST_NOT_SUPPORTED`, 4829); redundant info is ignored.
- The kernel row path is mature (ha_ndbcluster exercises it daily);
  the aggregate NULL-injection path is Test-2-green without nest info
  — a block-level check that redundant nest info stays a no-op for
  aggregation belongs to that future work.

Until then, Part C is documentation, not a work item.

## Sequencing

1. **A** — land `body_nest_semantics.inc` (aggregate probes + controls),
   record once ×5 suites; findings updated with the verdict per
   sub-case.
2. **B** — promotion pre-pass + gate check demoted to defensive +
   sn-15/pass-through twins enabled + family re-recorded green.
3. **C** — deferred until grammar-level nested joins exist.

## Verification (user-run)

- Step A: build not needed (test-only) — `./mtr --record
  --suite=ronsql_cte ronsql_cte_dd_nest_semantics` + siblings; report
  any diffs (they ARE the findings).
- Step B: rebuild ronsql; re-record the family +
  `ronsql_cte_dd_passthrough_snowflake` (sn-15 flips from rejection to
  compare); full `ronsql_cte` + `ronsql` regression (the pre-pass
  touches every join query's AST).
