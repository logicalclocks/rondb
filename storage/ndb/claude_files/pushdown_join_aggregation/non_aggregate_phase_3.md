# Non-Aggregate Pushdown — Phase 3 Detailed Plan

**Status: IMPLEMENTED (August 2026) — pending user build + MTR
--record.**

Implementation notes: landed as planned.  W1's sweep found no chain
assumptions (fan-out flows through gate walk, planner alias-based
parent resolution, per-op emit and the topology-agnostic drain
unchanged); the alias-uniqueness check lives at the top of the planner
child loop (covers root + all prior ops, aggregate queries included).
W2's `<- parent` annotation is on the non-root join-plan print lines.
W3 is `body_passthrough_star.inc` (sr-1..10 + sr-P1, local
star_e/star_m/star_v tables for the ordered-PK-prefix bushy cases)
×5 suites + `findings/passthrough_star.md`.
**Parent:** `non_aggregate_pushdown_plan.md` (Phase 3 overview).
**Predecessors:** Phases 0-2 (`non_aggregate_phase_{0,1,2}.md`) and the
nest-semantics work (`join_nest_semantics_plan.md`, Parts A+B shipped).

## Goal

Star-schema fan-out for projection-only queries: multiple children
linked to the **same parent**, typically on a shared key:

- **3a** Lookup-children fan-out — N `readTuple` (PK/unique) children
  on one parent, e.g. two dimension lookups from one fact row
  (including self-join stars: two `customer` lookups from one `orders`
  row via different key columns).
- **3b** Scan-children fan-out (bushy) — the common-key case where
  children bind only a PK *prefix* (`(entity_id, seq)` keyed on
  `entity_id`), classifying as `INDEX_SCAN` children of the same
  parent.  This is the bushy `NI_REPEAT_SCAN_RESULT` case: DBSPJ
  produces the cross product of the sibling scans per parent row —
  exactly MySQL's semantics for the same flat SQL, worth stating in
  the tests because the result *sizes* surprise people.

## Why this is mostly a test phase

Fan-out is already structurally admitted end to end:

- The parse-gate walk lets every join reference **any**
  already-visible alias (and the Part B promotion + under-LEFT
  tracking are per-join-parent, so they compose with fan-out).
- `QueryPlanner` resolves each join's parent by alias against all
  prior ops; two children naming the same parent both get
  `parent_op_idx` pointing at it.  Sibling re-chaining exists only for
  CTE_LOOKUPs.
- `emit_child_ops` emits per-op linked keys/filters with no chain
  assumption; the NDB API accepts N children per parent for lookups
  and scans alike (`testStarJoinAggNdbApi` proves the lookup star at
  API level; bushy scans are what `QueryTree.hpp`'s repeat protocol
  was written for).
- The pass-through drain is per-op (getValues, `isRowNULL`,
  empty-projection guards) — topology-agnostic.
- The aggregate star needed the RONDB-1044 multi-leaf machinery; the
  non-aggregate star needs none of it (`agg_leaf_idx` stays the
  harmless last-op default when no aggregator is attached — confirmed
  as part of W1).

Two genuine gaps surfaced while planning, both now in scope:

1. **No alias-uniqueness validation exists** (grep-verified).  A star
   query duplicating an alias (`JOIN customer AS cu ... JOIN customer
   AS cu ...`) would silently bind conditions to the first match —
   MySQL rejects this as ERROR 1066 "Not unique table/alias".  Star
   shapes make collisions likely; add the check.
2. **EXPLAIN cannot show topology.**  The join-plan print lists ops in
   order with no parent reference — a star and a chain print
   identically.  No recorded baseline contains the print
   (grep-verified), so annotating child lines is baseline-safe.

## Work items

### W1 — verification sweep + alias-uniqueness validation

- Sweep (expected no code change, assert-by-reading): gate walk,
  `classify_where_by_table`, `emit_child_ops`, drain op-index mapping
  and `agg_leaf_idx` deadness for `leafAggs == nullptr` — no
  chain-shaped assumption survives for real-table fan-out.
- New validation in `QueryPlanner::plan` (covers aggregate queries
  too): reject a child whose alias equals the root alias or any prior
  op's alias, with a clean permanent error mirroring MySQL's
  "Not unique table/alias".  Behavior change for any query that
  previously bound ambiguously — silently-wrong becomes a clean error.

### W2 — EXPLAIN topology annotation

Append the parent reference to non-root lines of the join-plan print:

```
├─ 1: [INNER] PK_LOOKUP customer AS cu  <- o
├─ 2: [LEFT JOIN] PK_LOOKUP customer AS c2  <- o
```

(child ops only; `parent = ops[parent_op_idx].alias`).  Baseline-safe
per the grep; the star family's EXPLAIN greps pin it.

### W3 — MTR family

`body_passthrough_star.inc` + wrapper
`ronsql_cte_dd_passthrough_star.test` ×5 suites.  Shared schema serves
the lookup stars (including the self-join star via `o_custkey` +
`o_clerk`); family-local tables provide the ordered-PK-prefix bushy
case (shared-schema PKs are `USING HASH`, so no PK-prefix ordered
index exists there):

```sql
CREATE TABLE star_e (e_id INT PK, e_name CHAR(8));            -- 50 rows
CREATE TABLE star_m (m_eid INT, m_seq INT, m_val INT,
                     PRIMARY KEY (m_eid, m_seq));             -- 4 per parent
CREATE TABLE star_v (v_eid INT, v_seq INT, v_tag INT,
                     PRIMARY KEY (v_eid, v_seq));             -- 3 per parent,
                                                              -- some parents empty
```

| # | Case | Shape |
|---|---|---|
| sr-1 | self-join lookup star: `orders` → `customer AS cu` (o_custkey) + `customer AS c2` (o_clerk), both INNER | 3a; NULL clerks drop rows |
| sr-2 | same with LEFT on the clerk branch | 3a + NULL extension per branch |
| sr-3 | star grafted onto a snowflake: `orders` → {`customer` → `nation`} + `customer AS c2` | tree topology |
| sr-4 | mixed star: `customer` → `nation` (PK lookup) + `orders` (INDEX_SCAN via `idx_o_custkey`) | lookup + scan branches |
| sr-5 | bushy scan star: `star_e` → `star_m` + `star_v` (both PK-prefix INDEX_SCAN) | 3b cross product per parent |
| sr-6 | sr-5 + per-branch WHERE (filter on `m_val`, filter on `v_tag`) | branch-local filters |
| sr-7 | bushy stress: full `star_e` scan, 4×3 cross per parent (~600 rows) | repeat-protocol stress |
| sr-8 | LEFT scan branch: `star_v` LEFT with parents that have no `star_v` rows | outer scan child NULL extension |
| sr-9 | projection touching only the root, both branches unprojected | empty-projection guards ×2 |
| sr-10 | EXPLAIN greps on sr-1's shape: both children annotated `<- o` | W2 pin |
| sr-P1 | rejection: duplicate alias (`JOIN customer AS cu` twice) | W1 pin |

All strict-diffed vs MySQL, sorted compare.  Cross-product semantics
called out in sr-5/7 comments.

### W4 — docs

Status updates here, parent plan, CLAUDE.md; findings file
`findings/passthrough_star.md` (deferred probes below).

## Deliberately out of scope (tracked)

- **Aggregate fan-out** — RONDB-1044's multi-leaf territory, separate.
- **Genuine multi-batch bushy verification at scale** — sr-7 is the
  in-suite stress; the `load_ronsql_large` case remains the named
  follow-up (shared with Phase 2's scan-scan item).
- ~~**Constant bounds on scan branches**~~ — **DONE (August 2026,
  child_bounds feature)**: sr-6's branch predicates now emit as
  bounds (fatal EXPLAIN greps); see `next_steps.md` items 2+3 and
  `body_child_bounds.inc`.
- Sibling scan branches execute sequentially in DBSPJ (legacy
  protocol) — a perf note, not correctness.

## Risks / notes

- **W1's alias check is a behavior change** for aggregate queries with
  duplicate aliases (previously silent first-match binding — i.e.
  potentially wrong results — now a clean error).  No existing suite
  uses duplicate aliases (they'd have been flagged as ambiguous
  binds); call it out in the commit message.
- **Cross-product sizes**: sr-7 deliberately bounded (~600 rows).
- The repeat-protocol correctness across batches rests on the same
  API guarantee as Phase 2b; sr-7 plus the large-data follow-up are
  the evidence.

## Verification (user-run)

- Build (ronsql only), then `./mtr --record --suite=ronsql_cte
  ronsql_cte_dd_passthrough_star` + 4 siblings.
- Regression: full `ronsql_cte` + `ronsql` (W1 touches every join
  query's planner; W2 touches EXPLAIN output that no baseline
  records — the greps in existing families are non-fatal).
- `.explain_ronsql` spot check on a star shape showing the `<- parent`
  annotations.
