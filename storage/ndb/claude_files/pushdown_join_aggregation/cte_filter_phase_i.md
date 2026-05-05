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

**Shipped** — see `cte_filter_phase_i4.md` (commit `4828effed0e`).
CASE conditions can reference a CTE COLUMN projection — both on a
non-leaf CTE op (BRANCH_MEM_OP_ARG via the CTE body's source-table
descriptor) and on the leaf CTE op (BRANCH_MEM_OP_ARG_INLINE_TYPE
via the CteLinkedAttr inline metadata).  Atoms remain `=` / `!=`
combined under AND / OR — same restriction the embedded condition
applied to non-CTE columns before.  Kernel side adds a real
`KeyReqStruct` on the CTE agg-feed path so the embedded interpreter
actually runs (previously short-circuited by `m_null_local_columns`)
and wires `BRANCH_MEM_OP_ARG_INLINE_TYPE` into `interpreterNextLab`
+ JoinAggInterpreter validation.  Deferred: CTE AGGREGATE outputs
in the condition (rejected cleanly with a clear message);
register-based CASE conditions (Test 21 shape — needs separate
codegen path); inequality CASE conditions (pre-existing
non-CTE-or-CTE-agnostic limit).

#### I.5 — `GREATEST` / `LEAST` and similar n-ary functions (M)

**v1 shipped** — see `cte_filter_phase_i5.md`.  Two-argument
GREATEST/LEAST with one column + one integer constant, lowered at
parse time to a single `CaseExpr`; column always normalised to the
LHS of the comparison; nullable column operands rejected cleanly
(MySQL NULL-propagation would need multi-arm CASE — deferred).  Also
lifts the embedded-condition `=` / `!=`-only restriction so any
inequality (`<` / `<=` / `>` / `>=`) can appear inside an embedded
CASE atom.  Bigint-only.

**v2a shipped** — see `cte_filter_phase_i5_v2.md`.  Column-vs-column
atoms in embedded CASE conditions (Bigint-only on each side via
`READ_*-into-register` + `BRANCH_*_REG_REG`).  Distinct-column
`GREATEST` / `LEAST` and multi-column CASE conditions (`WHEN col_a
> col_b`) now work.  Per-atom self-contained loads also fix a
latent v1 bug.

**v2b shipped** — see `cte_filter_phase_i5_v2b.md`.  N-ary
GREATEST / LEAST via the SVM extension (new `Greatest2` / `Least2`
ops shaped like arithmetic ops).  Phase M
(`cte_filter_phase_m.md`) later rewrote pair-op kernel emission from
`BranchReg + Mov` to an embedded normal-interpreter program (using
`READ_AGG_REG_TO_REG + BRANCH_(GE|LE)_REG_REG`) plus
`Mov(dest, src)`; SVM model unchanged.  v2b also replaces v1's
two-arg CaseExpr-based lowering — n=2 now flows through the same
SVM path as n>2.  Integer-typed operands (Tinyint through Bigint,
signed + unsigned).

**v4 shipped, refined by v7** — see `cte_filter_phase_i5_v4.md` and
`cte_filter_phase_i5_v7.md`.  v4 lifted the parser-time
nullable-column rejection and added per-pair-op NULL detection
(`BRANCH_REG_EQ_NULL`).  v7 changed the NULL outcome from a row-stop
(`AGG_EMBEDDED_INTERP_STOP_PROGRAM`) to expression-local
`SetRegNull(dest)` — so a NULL in one `GREATEST` / `LEAST` only
makes that expression's aggregate skip this row; unrelated
`COUNT(*)` / `SUM` outputs still update.

**v6 shipped** — see `cte_filter_phase_i5_v6.md`.  CTE linked-vs-linked
GREATEST / LEAST runtime coverage; test-only (no code change needed
since v2b's pair-op SVM path already handles CTE-leaf operands).
Surfaced two unrelated planner gaps now captured as I.16 (partial-key
joins to multi-key CTEs) and I.17 (scalar aggregate CTEs without
GROUP BY).

**v5 shipped** — see `cte_filter_phase_i5_v5.md`.  New
`READ_LINKED_COLUMN_TO_REG` kernel opcode; v2a's col-vs-col
linked-side emission migrated from a two-word
`READ_LINKED_TO_MEM + READ_*_MEM_TO_REG_CONST` sequence (silently
zero-extended signed sub-Bigint linked operands) to one-word typed
loading with correct sign extension.  All 10 NDB integer types
accepted on the linked side; pair-op SVM path unaffected.  Known
follow-up: leaf-side `READ_ATTR_INTO_REG` has the same
zero-extension issue for signed sub-Bigint leaf columns — queued
as a separate phase.

Follow-ups: v3 (Float / Decimal / VARCHAR — converges with I.6),
typed `READ_ATTR_TYPED_TO_REG` for the leaf-side gap.

### Aggregator output types

#### I.6 — MIN/MAX over CHAR / VARCHAR / DECIMAL  (L)

Already split into a dedicated plan:
`cte_filter_minmax_strings_plan.md` (sub-phases F.1 = DECIMAL
RonSQL-only, F.2 = kernel agg over strings, F.3 = RonSQL CTE
filter on string MIN/MAX).  Listed here for cross-reference.

### Plan shapes RonSQL doesn't emit

#### I.7 — `lookupCte` as the main-query root (M) — shipped

Shipped via commits `adaf4c9e130` (initial RonSQL emit + test file),
`0c72daaf55c` (typed `constValue` dispatch — synthetic virt-table
columns can't go through the byte-buffer overload), and
`a71f192cf04` (populate `m_attrSize / m_orgAttrSize / m_arraySize`
in `build_cte_virtual_tables` so the lookup-op receiver buffer
sizes correctly).

`emit_root_op`'s CTE_SCAN root branch now walks the WHERE for
equality predicates on every virt-table PK column and, if all PK
columns are bound and there are no scan children, emits
`qb->lookupCte(cteId, numCols, virtTab, const_keys, opts)`
(matching `testCteNdbApi.cpp` Test 11).  Partial-key WHERE / no
WHERE / non-equality predicates fall back to the existing scanCte
+ inline-type filter path.

MTR coverage: `mysql-test/suite/ronsql/t/ronsql_cte_root_lookup.test`
(single-PK lookupCte, multi-PK composite, partial-key fallback,
no-WHERE regression).

#### I.8 — `readTuple` main root + `lookupCte` child, no aggregation (M) — shipped

The original blurb framed this as a planner coverage check, but the
plan/emit sides already produced the right shape.  The actual gap
was RonSQL's "aggregate query only" assumption: any join query
without aggregation in the outer SELECT hit the "Not an aggregate
query" rejection unless the narrow Phase E.3 single-CTE projection
carve-out matched.

Phase doc: `cte_filter_phase_i8.md`.  Three RonSQLPreparer.cpp
edits:
- Front-end gate accepts `FROM <real_table> JOIN <cte> ON ...`
  projection-only when there's exactly one AST join entry whose
  target name resolves to a CTE.
- `emit_child_ops` invocation no longer gated on
  `m_is_aggregate_query` — the aggregator-attach blocks inside it
  are already null-guarded so passing nullptr no-ops naturally.
- `execute_passthrough_drain` rewritten for multi-op:
  pre-registers all virt-table columns on each CTE op in attrId
  order (lookupCte/scanCte hardcode the kernel emit to send all
  numResultCols cols 0..N-1), then per-output dispatches getValue
  on the right `NdbQueryOperation` via `column_table_idx`.  TSV
  header emission deferred until first row arrives so empty
  results match mysql client's no-output behaviour.

MTR: `mysql-test/suite/ronsql/t/ronsql_cte_pk_join.test` (basic
Test 17 shape, body-WHERE composition, PK-no-match empty result).

Out of scope (each its own follow-up): `FROM cte JOIN real_table`
direction, multi-CTE joins in projection-only queries, ORDER BY /
LIMIT in no-aggregate.

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

#### I.16 — Partial-key joins to multi-key CTEs (M/L)

Detailed plan: `cte_filter_phase_i16.md`.

Today any joined CTE child is planned as `CTE_LOOKUP`.  That is only
valid when the join predicates bind the complete virtual CTE primary
key, which is derived from the CTE's `GROUP BY` columns.  A query such
as:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(GREATEST(pairs.k, pairs.amt))
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id
GROUP BY c.c_id;
```

tries to create a `CTE_LOOKUP` with only `k`, while the CTE key is
`(k, amt)`.  `lookupCte()` returns `NULL`, and RonSQL reports the
opaque "Failed to create child operation" error.

The planner should detect this before emission.  Short term, reject
partial-key CTE_LOOKUP with a clear RonSQL error.  Full support means
planning this as a CTE scan plus join predicate evaluation, most likely
by driving the query from `CTE_SCAN` when possible, or by adding support
for non-root CTE_SCAN children with linked predicate evaluation.

#### I.17 — Scalar aggregate CTEs without GROUP BY (M/L)

Detailed plan: `cte_filter_phase_i17.md`.

RonSQL currently rejects CTE bodies that do not contain `GROUP BY`,
even when the body is a valid scalar aggregate that should produce one
row:

```sql
WITH max_update AS (
  SELECT MAX(update_dt) AS latest_update
  FROM feature_store)
SELECT latest_update FROM max_update;
```

This blocks SQL shapes such as scalar CTE cross-joins and watermark
queries:

```sql
WITH max_update AS (SELECT MAX(update_dt) AS latest_update FROM t),
     max_insert AS (SELECT MAX(insert_dt) AS latest_insert FROM t)
SELECT GREATEST(latest_update, latest_insert)
FROM max_update, max_insert;
```

The implementation needs a representation for a one-row CTE result
with no key columns, clear `scanCte` / `lookupCte` planning rules for
that keyless virtual table, and tests covering both single scalar CTEs
and joins of two scalar CTEs.  This should be handled before relying
on watermark-style CTE tests in I.5 or later phases.

#### I.18 — Typed leaf-column register loads for embedded col-vs-col (M) — kernel + NDB-API shipped

Detailed plan: `cte_filter_phase_i18.md` (kernel + NDB-API shipped
across `0786adb0f19` → `2784d7746e5`).  Outstanding: RonSQL MTR
coverage for negative leaf, float operands, and `Bigunsigned >
INT64_MAX` — queued as a follow-up.

Phase I.5 v5 adds a typed linked-column register load for parent /
linked attributes, but leaf columns in embedded col-vs-col conditions
still use the normal interpreter's existing `READ_ATTR_INTO_REG`.
That path zero-extends signed sub-Bigint leaf values when expanding
the raw attribute word into an `Int64` register value.  A condition
such as:

```sql
SUM(CASE WHEN c.c_tinyint > o.o_tinyint THEN o.o_int ELSE 0 END)
```

can therefore compare a correctly sign-extended linked value against a
zero-extended leaf `TINYINT` value.  Example: linked `5` compared to
leaf `-20` can become `5 > 236`, returning false.

This phase should add a typed leaf-column register-load path for the
normal embedded interpreter, or equivalent typed register metadata, so
signed `TINYINT` / `SMALLINT` / `MEDIUMINT` / `INT` leaf operands are
sign-extended before `BRANCH_*_REG_REG`.

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
