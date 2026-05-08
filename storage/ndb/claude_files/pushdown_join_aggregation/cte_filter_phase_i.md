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

#### I.11 — RonSQL coverage of testCteNdbApi Tests 12-16 (M) — shipped

The original I.11 catalogue entry mapped to Test 20, but the scalar
CTE cross-join shape shipped in Phase I.17.  I.11 is now repointed to
the kernel CTE topology shapes still missing SQL-level RonSQL coverage:

- Test 12: `lookupCte` as CTE materialisation root + child;
- Test 13: `lookupCte` as main-query internal node;
- Test 14: `lookupCte` as CTE materialisation internal node;
- Test 15: `scanCte` as main-query aggregate leaf;
- Test 16: `scanCte` as CTE materialisation root non-leaf.

See `cte_filter_phase_i11.md`.  All five shapes are positive
MySQL-vs-RonSQL coverage in `ronsql_cte_kernel_t12_t16`.  The phase
also fixed a `QueryPlanner` child-op initialisation gap where a
physical child could be misclassified as a CTE during scoped
resolution.

#### I.12 — RonSQL coverage of testCteNdbApiOuterJoin.cpp Tests 1/2/3/5/6 (M) — shipped

Detailed plan: `cte_filter_phase_i12.md`.

The original I.12 catalogue entry mapped to the cross-join LEFT JOIN
shape over an unkeyed `scanCte` child.  That shape was dropped at the
kernel level in `cte_outer_join_phase_3.md`; Phase G's defensive
reject at `RonSQLPreparer.cpp:3801-3805` keeps it firmly rejected.

I.12 repointed to RonSQL coverage of the outer-join shapes that
**did** ship (cte_outer_join Phase 1 / 2 / 5 / E.1K), exercising
testCteNdbApiOuterJoin.cpp Tests 1 (`scanCte INNER JOIN readTuple`),
2 (`scanCte LEFT JOIN readTuple`), 3 (`scanTable LEFT JOIN
lookupCte`), 5 (scalar main aggregation over `LEFT JOIN cte` with
NULL-injection), and 6 (`scanCte` parent + main aggregator on real
leaf with linked CTE GROUP BY).

Three RonSQL changes shipped:
1. Gate extension at `RonSQLPreparer.cpp:540-636` accepting CTE-root
   projection-only chains (relaxation A) and `LEFT_OUTER_JOIN` in the
   chain (relaxation B), with the existing `cte_key_coverage`
   complete-key check preserved.
2. `Char` / `Varchar` / `Longvarchar` arms added to
   `ResultPrinter::print_passthrough_value` so projection-only
   queries can return real-table string columns alongside CTE
   numeric columns.
3. `execute_passthrough_drain` LEFT JOIN NULL-row plumbing — per-row
   substitution via parallel `output_ops[]` + `effective_attrs[]`
   arrays, using `NdbQueryOperation::isRowNULL()`.

MTR coverage: `mysql-test/suite/ronsql/t/ronsql_cte_outer_join.test`
(Tests 1/2/3/5/6).

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

#### I.19 — CASE literal normalisation and INT boundary fixes (S/M)

Detailed plan: `cte_filter_phase_i19.md`.

Review of the Phase I.5 v3b float-literal CASE work found three
follow-ups in the embedded CASE col-vs-const path:

- CASE atoms should be simplified as whole comparisons before side
  resolution, matching the normal WHERE path.  Today v3b only
  simplifies the RHS literal, so `o.o_double < -50.5` works but the
  symmetric `100.0 < o.o_double` shape can still fail before the
  normaliser swaps it into column-vs-literal form.
- `encode_constant()` has signed INT bounds off by one
  (`-2147483647..2147483648` instead of
  `-2147483648..2147483647`), which can reject valid `INT_MIN` and
  accept invalid `2147483648`.
- The v3b MTR labels imply leaf FLOAT negative-literal coverage, but
  the query uses DOUBLE.  Add explicit leaf FLOAT, leaf DOUBLE,
  literal-on-left, and INT boundary tests.

#### I.20 — CTE lookup key coverage and rewrite validation (M)

Detailed plan: `cte_filter_phase_i20.md`.

Review of Phase I.16 found that the partial-key guard and rewrite use
join-key count as a proxy for virtual CTE key coverage.  That is not
strong enough: full-count joins can still bind the wrong CTE output
columns or bind the right key columns in the wrong order relative to
the CTE `GROUP BY`-derived virtual PK.

This phase should add a shared helper that derives the ordered virtual
CTE PK columns and classifies a join as exact ordered, reorderable,
partial, or wrong-column.  `CTE_LOOKUP` emission should accept only
valid key coverage, reordering keys to virtual-PK order if feasible or
rejecting clearly.  The I.16 root rewrite should use the same helper
and should also verify that the matched partial-key CTE join references
the original root alias before demoting the original root under the CTE.

#### I.21 — Scalar CTE semantic guardrails after I.17 (M)

Detailed plan: `cte_filter_phase_i21.md`.

Review of the shipped Phase I.17 scalar CTE work found that the
first-output-as-PK workaround needed for scalar CTE child lookup can
leak into normal SQL semantics.  Root scalar CTE queries with `WHERE`
predicates can be incorrectly considered full-PK lookups even though
scalar lookup keys are dummy values and should not decide predicate
truth.  The same structural key can also make nullable aggregate
outputs such as `MAX()` over empty input look non-nullable in virtual
table metadata.

I.21 should keep the scalar dummy-key mechanism local to the scalar
cross-join implementation: root lookup optimisation should remain
limited to grouped CTEs with real `GROUP BY`-derived keys, scalar
aggregate output nullability must stay user-visible, and top-level
`GREATEST` / `LEAST` implicit aggregate lowering should be constrained
to proven scalar-CTE expressions so ordinary row-wise table queries
cannot silently become aggregate queries.

#### I.22 — DECIMAL MIN/MAX 64-bit range guard (S/M)

Detailed plan: `cte_filter_phase_i22.md`.

Review of Phase I.6 F.1 found that RonSQL now accepts all
scale-zero DECIMAL `MIN` / `MAX` outputs by widening the CTE virtual
type to `BIGINT` / `BIGUNSIGNED`, but the kernel conversion path still
uses `decimal2longlong()` / `decimal2ulonglong()`.  Declarations such
as signed `DECIMAL(19,0)` or unsigned `DECIMAL(20,0)` can contain
valid MySQL values outside the 64-bit target range and can therefore
fail at execution with a DBTUP decimal conversion overflow.

I.22 should add a prepare-time guard for scale-zero DECIMAL `MIN` /
`MAX`: accept signed precision up to 18 and unsigned precision up to
19, reject wider declarations clearly, and add MTR coverage for both
safe boundaries and rejected unsafe declarations.  Full arbitrary
precision DECIMAL preservation is explicitly out of scope.

#### I.23 — Scoped CTE and stored-table name resolution (M) — shipped

Detailed plan: `cte_filter_phase_i23.md`.

The aborted I.11 implementation attempt showed that RonSQL's current
CTE name handling is still too global.  Internal aliases and columns
from one CTE body can leak into later scopes, while later planning and
virtual-table construction sometimes lack the CTE index/result-column
metadata needed to resolve chained CTE outputs safely.

I.23 should build name resolution one CTE at a time, in SQL visibility
order.  While resolving a CTE body, only stored tables plus previously
published CTE result interfaces are visible.  After the CTE is handled,
only its CTE name and exposed result columns are published to later
CTEs and to the main SELECT; none of its internal stored-table aliases
or columns are visible outside the CTE body.

Unqualified names should be accepted only when unique in the current
scope across both stored tables and visible CTE outputs.  Duplicate
names require qualification as `stored_alias.column` or
`cte_name.result_column`.  Resolution must produce stable metadata for
both stored-table columns and CTE result columns: stored table/join op
index for physical columns, and CTE index plus result-column index for
CTE outputs.  I.11 should not be resumed until this resolver model is
in place.

I.23 shipped in `ffae6a9627a`.  It added scoped per-SELECT column
collection, resolved CTE bodies in declaration order, preserved the
legacy emit maps as compatibility output, fixed HAVING-only aggregate
resolution, and added `ronsql_cte_name_resolution` MTR coverage.

#### I.24 — Emit from resolved column descriptors (M) — shipped

Detailed plan: `cte_filter_phase_i24.md`.

I.23 made name resolution scope-aware, but emit still consumed the old
parallel arrays: `column_attrId_map`, `column_map`, and
`column_table_idx`.  Those arrays were populated from the scoped
resolver, but they remained a lossy interface: CTE result columns,
stored-table columns, alias-only HAVING references, and unresolved
sentinels were distinguished by implicit conventions.

I.24 introduced `ResolvedColumnRef` descriptors on each `QueryScope`
and made them the authoritative emit contract.  Migration ran in 21
commits between `855ec891694` (plan) and `7e0f33b7890` (legacy-array
removal), with `cf188857098` introducing the descriptor type.  Each
emit surface was migrated incrementally — CTE metadata helpers,
filter emit, aggregation loads, pass-through routing, CASE condition
emit, single-table aggregation, embedded filter loads, linked
projections, subquery aggregation, aggregate validation, CTE source
emit, column ownership, WHERE classification, result metadata, CTE
metadata, and index-bound metadata — followed by validation in
`e8a2e00686a` and the legacy-array removal.  I.11's kernel-topology
coverage commit `3ca4f897af3` landed on top of the migrated emit and
exercised the new descriptor path end-to-end.

#### I.25 — Deferred string CTE query failures from F.4 coverage (M)

Detailed plan: `cte_filter_phase_i25.md`.

Phase I.6 F.4 string MIN/MAX testing exposed two extra failures that
should not be solved by weakening the final coverage.  First, a CTE
string MIN/MAX output re-aggregated through a join back to the same
table on a non-unique ordered index (`s JOIN str_minmax ON
str_minmax.grp = s.grp`) produced `NULL, NULL` after the index was
added, while the narrower primary-key helper-table variant worked.
This needs a focused fix for the CTE-root to ordered-index child
topology or linked-projection positioning in that topology.

Second, the intended predicate `WHERE min_v < 'beta'` exposed two
separate issues: the MTR compare helper passes queries through a
single-quoted `mysql -e '...'` shell command, so SQL single quotes
break the MySQL baseline invocation; replacing the literal with
double quotes avoids the shell problem but RonSQL currently rejects
double-quoted string literals.  I.25 should make the compare path
safe for single-quoted SQL literals, clarify/support the parser
semantics for string literals, and restore the literal predicate
regression.  The attempted workaround `WHERE min_v < max_v` is also
currently rejected because CTE_LOOKUP filter col-vs-col comparisons
are limited to signed BIGINT; string col-vs-col predicate support is
therefore included in I.25 as a separate regression.  I.25 should
cover this as a general CTE output comparison matrix rather than a
single string case: col-vs-col, const-vs-col, and col-vs-const across
signed/unsigned integers, floats, DECIMAL widening, strings, date/time
types where exposed, and NULL semantics, with clear prepare-time
rejections for families that remain unsupported.

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
