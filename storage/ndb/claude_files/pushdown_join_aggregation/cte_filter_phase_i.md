# CTE filter Phase I — RonSQL feature gaps vs. NDB-API block tests

## Status

**Catalogue only.**  This phase enumerates capabilities exercised by
`storage/ndb/block_unit_test/testCteNdbApi*.cpp` that RonSQL today
cannot reach from SQL.  Each item is described at the level of "what
it is, where it's tested at the NDB-API layer, what would need to
change in RonSQL".  Detailed implementation planning is deferred —
each item below is a candidate for its own per-phase doc when
prioritised.

## Why this catalogue

Phase H ships MTR coverage for everything RonSQL **does** support.
That work surfaces a parallel question: which CTE/aggregation
capabilities does the NDB-API + DBSPJ + DBLQH stack already provide
that RonSQL hasn't exposed yet?  This doc is the running answer.
Use it as the entry point when picking the next RonSQL feature
phase.

## Items

Items are grouped by area.  Effort signals: **S** = test-only or
small RonSQL-side change; **M** = parser/planner work; **L** =
multi-layer (RonSQL + maybe NDB-API tweaks).

### Filter grammar on CTE outputs

#### I.1 — `IS NULL` / `IS NOT NULL` on CTE column or agg output  (S/M)

`emit_cte_lookup_filter` (`RonSQLPreparer.cpp:5606-5612`) accepts
only the six binary comparisons (`= != < <= > >=`).  The kernel's
jump-table interpreter handles null semantics through the inline
branch family.  Adding `T_IS NULL` would extend the conjunct
walker and emit a single `branch_linked_inline_eq` against a
NULL-tagged constant (or a dedicated `branch_linked_*_isnull`
opcode if the encoding doesn't already support it).

NDB-API analogue: testCteNdbApiFilter uses null-mask checks in its
filter programs but no dedicated test exercises `IS NULL` from
the SQL surface today.

#### I.2 — OR conjuncts at top level on CTE output (M)

**Shipped** — see `cte_filter_phase_i2.md`.  Top-level DNF accepted:
`D_1 OR D_2 OR ... OR D_n` where each `D_i` is one atom or an
AND-conjunction of atoms.  Single-disjunct emission is byte-equivalent
to the pre-I.2 AND-only path.  Non-DNF nesting (OR inside AND) and
NOT outside `IS NOT NULL` are rejected with clean errors.

#### I.3 — Column-vs-column comparisons on CTE output (M)

**Shipped (Bigint-only)** — see `cte_filter_phase_i3.md`.  Two CTE
columns from the same lookup, both `Bigint` (signed 64-bit), via
READ_LINKED_TO_MEM + READ_INT64_MEM_TO_REG twice + BRANCH_<inv>_REG_REG.
Bigunsigned, mixed-width, float, decimal, string, and parent-vs-CTE
comparisons remain rejected — they need either kernel work
(unsigned reg-cmp opcode) or a different code path.

### Aggregation expression surface

#### I.4 — CASE expressions in aggregation over CTE columns (M)

testCteNdbApi Test 21 exercises `GREATEST(MAX, MIN)` via CASE in
the aggregation program.  RonSQL has CASE plumbing
(`AggregationAPICompiler::CaseExpr`) but coverage of CASE inside
aggregates referencing CTE virt columns is incomplete; verify and
add MTR if reachable, or wire up the missing parser/preparer
plumbing.

#### I.5 — `GREATEST` / `LEAST` and similar n-ary functions (M)

No parser support today.  testCteNdbApi Test 21 builds these via
CASE chains — RonSQL would do the same once I.4 is solid.

### Aggregator output types

#### I.6 — MIN/MAX over CHAR / VARCHAR / DECIMAL  (L)

Already split into a dedicated plan:
`cte_filter_minmax_strings_plan.md` (sub-phases F.1 = DECIMAL
RonSQL-only, F.2 = kernel agg over strings, F.3 = RonSQL CTE
filter on string MIN/MAX).  Listed here for cross-reference.

### Plan shapes RonSQL doesn't emit

#### I.7 — `lookupCte` as the main-query root (M)

testCteNdbApi Test 11 (constant key) and Test 13 (internal
non-leaf) build `lookupCte` as the root.  RonSQL's `emit_root_op`
(`RonSQLPreparer.cpp:5098-5141`) only emits `scanCte` for CTE
roots; lookup-by-constant would correspond to
`SELECT * FROM cte WHERE pk = const` shapes.  Today RonSQL would
plan that as `scanCte + filter`, missing the optimisation.

#### I.8 — `readTuple` main root + `lookupCte` child (M)

testCteNdbApi Test 17.  Pattern is "PK lookup on real table joined
to CTE by foreign key".  RonSQL today emits `readTuple` as root
correctly, but the planner's child dispatch may not pick
`lookupCte` for a CTE join under a PK-rooted parent.  Verify by
running the existing PK-join shape with a CTE child and check
which child op the planner emits.

#### I.9 — `scanIndex` inside CTE materialization subtree (M)

testCteNdbApi Test 18.  CTE bodies in RonSQL today use scan/lookup
on the source table or `scanCte` for chained CTEs; the body's root
goes through the same dispatch as the main query, but real-world
CTE bodies often want an index-bounded scan when the body has a
WHERE on an indexed column.  Verify whether the body's
`emit_root_op` recursion picks `scanIndex` and add MTR if so;
otherwise this is a planner gap.

#### I.10 — Scalar CTE + `MAX(val)` via DESC index + `LIMIT 1` (L)

testCteNdbApi Test 19.  Optimisation: a `MAX` over an indexed
column can be rewritten as `ORDER BY col DESC LIMIT 1`, avoiding
the hash-aggregation pass entirely.  RonSQL has neither ORDER BY
pushdown nor LIMIT pushdown to CTE bodies.  Significant work;
unblock requires the LIMIT/ORDER-BY phase first (cf. Phase 7 /
step 45d in `ronsql_join_phase7.md`).

#### I.11 — Cross-join of two scalar CTEs (`FROM a, b`) (M)

testCteNdbApi Test 20.  RonSQL's parser doesn't accept the
comma-join syntax (`ronsql_join.md` line 76: "CROSS JOIN — inner
join only for now").  Adding `,`-join is parser-only if the
planner can already cross-join via INNER JOIN with no
condition.  Check the planner's behaviour with
`a INNER JOIN b ON 1=1` first; if that works, MTR coverage alone
might suffice.  If the planner rejects, this is a planner phase.

#### I.12 — CTE_SCAN as a LEFT JOIN inner side (L)

Phase G shipped a defensive reject for this.  The kernel side
isn't ready
(`testCteNdbApiOuterJoin.cpp` Phase 5 covers some agg-feed
NULL injection but not the full CTE_SCAN-as-outer-join-child
shape).  Both kernel + RonSQL work needed.

### Batch / pacing controls

#### I.13 — `setBatchSize` from SQL (S/M)

testCteNdbApiFilter Test 15.  No SQL surface today.  Pure
performance/tuning; could expose via a SET statement or a
session variable but value is low until users hit batch-size
issues from SQL.

#### I.14 — Early close of a CTE scan (S)

testCteNdbApiFilter Test 16.  RonSQL drains all rows.  Could be
exposed via LIMIT once that lands; until then, mostly an internal
optimisation hook (`query->close()` mid-drain when LIMIT N is
satisfied).  Bundle with LIMIT phase.

### Result delivery

#### I.15 — Multi-fragment / multi-data-node CTE behaviour (M)

`m_fragsPerWorker` interactions with CTE_SCAN: testCteNdbApi has
fragment-level coverage; MTR cluster config is single-node so this
is hard to exercise.  Capture as a known coverage gap; rely on
block tests.

## Recommended next-pick heuristic

When picking the next post-Phase-H feature work, sort the items
above by:
1. **User-visible impact** — does anyone hit the rejection in real
   queries?  I.1, I.2, I.4 are the most user-visible (filter
   grammar and CASE).
2. **Effort/value ratio** — small RonSQL-only items first (I.1,
   I.13).
3. **Dependency chain** — I.10 / I.14 want LIMIT/ORDER-BY to land
   first; I.12 wants kernel work first.

When a candidate is chosen, write its detailed per-phase doc as
`cte_filter_phase_<id>.md` (or under a fresh letter if it's
substantial), then update this catalogue's status for the entry
to "in progress" / "shipped".

## Relationship to other docs

- Phase H (`cte_filter_phase_h{1,2,3}.md`) covers MTR coverage of
  things RonSQL **does** support today.
- This doc (Phase I) lists things RonSQL **doesn't** support that
  the NDB-API stack already does.
- `cte_filter_minmax_strings_plan.md` (I.6 / Phase F) is the only
  catalogue entry already broken into sub-phase plans.
- `ronsql_join_phase7.md` step 45 covers general non-aggregate
  query support — overlaps with I.10 / I.14.
