# RonSQL Non-Aggregate Query Pushdown Plan

**Status: PLAN — not yet implemented.**

## Goal

Support queries **without** aggregation pushdown, in five steps:

1. **Single-table queries** with conditions: PK lookups, ordered index
   scans, full table scans.
2. **Snowflake-schema join queries** (following foreign keys): first all
   child accesses are PK lookups, then also index scans inside the
   snowflake.
3. **Star schemas** where all tables share a common key (fan-out:
   multiple children linked to the same parent).
4. Extend snowflake/star queries with **CTEs doing aggregation that
   generates a single row** (scalar CTEs).
5. Extend with **CTEs that use GROUP BY**.

Guiding principle: **the first aim is to support only what the NDB API
already supports.** Verified conclusion of the research pass: Steps 1-3
need **no kernel work and no NDB API work at all** — the NdbQueryBuilder
/ DBSPJ envelope already covers every shape (see the legality table
below), and RonSQL's own planner, emit and drain machinery downstream of
the parse gate is already general enough to handle most of it. Steps 4-5
reuse the shipped CTE kernel machinery (scalar CTEs from Phase I.17,
GROUP BY CTE_LOOKUP from the CTE branch). The project is almost entirely
a RonSQL front-end project: parse-gate relaxation, small planner/emit
extensions, a non-aggregate single-table drain, printer type coverage,
and a large test surface.

## Relationship to existing plans

- `ronsql_orderby_limit_plan.md` — its Phase 4 ("single-table
  non-aggregate SELECT") is **subsumed by Step 1 here** minus the ORDER
  BY/LIMIT parts; its Phases 2-3 (passthrough LIMIT / buffered ORDER BY)
  apply on top of every shape this plan adds. ORDER BY / LIMIT stays out
  of scope here; the two plans compose. When Step 1 lands, that plan's
  Phase 4 reduces to its 4b (`SF_OrderBy` index-order top-N).
- `ronsql_join_phase7.md` Step 45 — the original "remove the
  aggregate-only gate" sketch (45a-45e). This plan replaces it with a
  shape-by-shape rollout. (Note: Step 45 says the gate is in `load()`;
  it is actually in `parse()` today.)
- `star_schema_plan.md` / `star_schema_implementation.md` — the
  *aggregating* star-schema work (multi-leaf fan-out aggregation). Step 3
  here is the non-aggregating sibling: it needs none of the multi-leaf
  aggregation-state machinery, only the fan-out join topology, which the
  API and DBSPJ already provide.
- `cte_filter_phase_e3/i8/i11/i12.md` — the three passthrough shapes the
  gate accepts today; Steps 2-5 generalize them.

## Current state (verified against the code, August 2026)

### The single admission gate

`RonSQLPreparer::parse()` — `RonSQLPreparer.cpp:566-707`:

- `:566-567` — `m_is_aggregate_query = (has_aggregate_outputs ||
  has_having_aggregates || has_subquery_agg_outputs)`.
- `:568-707` — the non-aggregate gate body. Two accepted shapes:
  - **Shape A (E.3)** `:612-614` — projection-only `CTE_SCAN` root:
    `FROM <cte>`, all-COLUMN outputs, no GROUP BY / HAVING / ORDER BY /
    LIMIT / joins.
  - **Shape B (I.8/I.11/I.12)** `:631-696` — all-COLUMN outputs +
    joins: strictly left-deep INNER / LEFT_OUTER chain, each ON
    referencing an already-visible alias, and every CTE join operand
    bound by a **complete-key** `CTE_LOOKUP`
    (`cte_key_coverage ∈ {ExactOrdered, ExactPermuted}`, `:683-689`).
  - `:694-695` — `cte_lookup_join_chain &&= query_has_cte`: **at least
    one CTE required.** This single line is why a pure real-table join
    can never pass.
- `:697-706` — everything else throws
  `RonSQLPermanentError("Not an aggregate query.")`.

So `SELECT a, b FROM t WHERE pk = 1` dies here, before `load()`, before
`plan_index_and_filter()`, before any dictionary access.

### Downstream machinery is already (mostly) general

- **QueryPlanner** (`QueryPlanner.cpp:72-314`) is aggregate-agnostic.
  Children classify as `PK_LOOKUP` / `UNIQUE_LOOKUP` (via
  `findUniqueIndex`) / `INDEX_SCAN` (via `findOrderedIndex`) / clean
  reject (`:242-277`). Parent resolution is alias-based against **all**
  previously planned ops (`:199-214`), so fan-out (two children naming
  the same parent) already builds structurally. Sibling `CTE_LOOKUP`s
  are re-chained via `tree_parent_op_idx` (`:293-310`). Caps:
  `MAX_SPJ_TREE_NODES = 32`, `MAX_JOIN_KEY_COLS = 8`
  (`QueryPlanner.hpp:35,38`). `agg_leaf_idx = num_ops - 1`
  unconditionally (`:313`) — harmless when no aggregator is attached.
- **`emit_root_op`** (`RonSQLPreparer.cpp:6834-7192`) already emits:
  scalar-CTE `lookupCte` with dummy key, full-key `lookupCte`,
  `scanCte` + filter, `INDEX_SCAN` root via `emit_index_scan_root`
  (`:6726-6825`, shared bounds + residual filter), **real-table
  `readTuple` root** when PK-equality-covered and no scan child
  (`:7127-7145`), PRIMARY-ordered-index equality scan when PK-covered
  *with* a scan child (`:7147-7177` — the NDB API forbids lookup-root +
  scan-child), and `scanTable` + `NdbScanFilter` fallback
  (`:7179-7191`).
- **`emit_child_ops`** (`:8374-8678`) already emits linked real-table
  `readTuple` (PK `:8497`, unique `:8500`), linked-bound `scanIndex`
  (`:8504-8547` incl. extended constant bounds), `lookupCte` incl. the
  `ScalarDummy` cross-join pattern (`:8580-8630`), match types
  (`:8384-8399`), and per-child `NdbScanFilter` / CTE jump-table filters
  (`:8423-8448`). Aggregator attach is already null-guarded
  (`:8455-8493`) — the passthrough path passes `nullptr`.
- **`execute_passthrough_drain`** (`:6463-6708`) is multi-op (I.8),
  registers every CTE virt column in attrId order (`:6530-6551`),
  handles empty-projection ops with a dummy `getValue` (`:6615-6632` —
  the `QRY_EMPTY_PROJECTION` 4826 fix), substitutes NULL for LEFT JOIN
  unmatched rows via per-op `isRowNULL()` (`:6679-6686`), and defers the
  TSV header so empty results match mysql-client baselines.
- **Single-table path** (`execute()` `:5730-5905`) does **not** use
  NdbQueryBuilder: plain `getNdbScanOperation` /
  `getNdbIndexScanOperation`, `readTuples(LM_CommittedRead)`, bounds
  from `m_scan_config` (`condition_handling_map`), `NdbScanFilter`
  residuals — then unconditionally `setAggregationCode(&aggregator)` +
  `DoAggregation()`. The planning half (`plan_index_and_filter()`
  `:2305-2344`, gated `!is_join_query()` at `:184-185`) is 100%
  reusable; only the delivery half is aggregate-only.

### NDB API / DBSPJ legality envelope (what "already supported" means)

| Shape | Legal? | Enforced at |
|---|---|---|
| `readTuple` root + `readTuple` children (any depth/fan-out) | Yes | — |
| `scanTable`/`scanIndex` root + `readTuple` children (star/bushy fan-out) | Yes | — |
| scan root + `scanIndex` children with linked bounds (incl. multiple scan children) | Yes | sets `NI_REPEAT_SCAN_RESULT` (`NdbQueryBuilder.cpp:3643-3646`) |
| `readTuple` root + any scan child | **No** | `NdbQueryBuilder.cpp:1473-1480` → QRY_WRONG_OPERATION_TYPE (4820) |
| `scanTable` as non-root | **No** | `:1432-1435` → QRY_UNKNOWN_PARENT (4807); scan children must be `scanIndex` |
| sorted child scan | **No** | `:1487-1491` → QRY_MULTIPLE_SCAN_SORTED (4824) |
| diamond / multi-parent DAG | **No** | `:2422-2449` → QRY_MULTIPLE_PARENTS (4806); strict trees only |
| > 32 tree nodes (unique-index lookup counts as **2**) | **No** | `:2369-2370` (4812); kernel `DbspjMain.cpp:1956-1959` (20017) |
| leaf op with empty projection | **No** | `NdbQueryOperation.cpp:6162-6172` (4826) |

Other hard rules that shape the plan:

- **Linked operand type identity** (`NdbQueryBuilder.cpp:2067-2091`):
  child key column and parent column must match in type, precision,
  scale, length and charset — **no implicit conversion**; BLOB/TEXT
  rejected. Same rule ha_ndbcluster enforces via `eq_def()`
  (`ha_ndbcluster_push.cc:1905-1917`).
- **Scan-scan batch semantics**: every non-root scan sets
  `NI_REPEAT_SCAN_RESULT`; DBSPJ replays ancestor rows across batches to
  produce the cross product incrementally (`QueryTree.hpp:121-131`,
  `DbspjMain.cpp:3957-4080`). The API's `nextResult()` presents each
  joined row exactly once — the MySQL handler consumes this same
  protocol with no client-side dedup — but a multi-batch MTR case must
  lock this in for our drain.
- **Read semantics**: SPJ requests are hard-wired to committed read, no
  locks (`DbspjMain.cpp:10946-10952`); no repeatable read across
  batches.
- **Interpreted filters** attach per-op via
  `NdbQueryOptions::setInterpretedCode` on root or children, lookup or
  scan (`PI_ATTR_INTERPRET`). ha_ndbcluster policy caps lookup-op
  filters at 64 words (`ha_ndbcluster.cc:15778-15788`) because the
  program rides in every LQHKEYREQ — mirror this.
- **Pattern-source tests**: `testSpj.cpp:814-1030` (negative-test
  bible), `testStarJoinAggNdbApi.cpp` (scan root + N `readTuple`
  children on one parent), `testJoinAggScanScan.cpp` (linked-bound
  `scanIndex` children, composite bounds, root filters),
  `testCteNdbApi.cpp` Tests 8/11/17/22,
  `mysql-test/suite/ndb_opt/join_pushdown.inc` (the 6000-line SQL-level
  definition of "supported" through the MySQL handler).

---

## Phase 0 — pre-existing defects to fix first

These bite the very first shapes we ship; both are small and
independent. **Detailed plan: `non_aggregate_phase_0.md` —
IMPLEMENTED (August 2026), pending user build + MTR --record** (MTR
families `body_root_pk_residual.inc` rpr-1..12/P1 and
`body_passthrough_types.inc` pt-1..6/P1/P2 ×5 topology suites).

**0a. `emit_root_op` drops root residual conjuncts under PK-equality
cover.** Branches at `RonSQLPreparer.cpp:7127-7145` (real `readTuple`
root), `:7147-7177` (PK-covered + scan child) and `:6960-7044`
(full-key `lookupCte` root) build keys from PK equalities but never
apply the remaining `where_ce` conjuncts — `WHERE pk = 1 AND c > 5`
silently drops `c > 5`. Today these branches are reachable only in
aggregate join/CTE queries (a live wrong-results bug there too); Step 1
would put them on the hottest path. Fix: attach the residual as
interpreted code (lookup ops: ≤ 64 words, else fall back to the scan
branch, which is always correct), mirroring the residual handling in
`emit_index_scan_root` (`:6795-6819`). MTR: aggregate + passthrough
cases with residuals on PK-covered roots.

**0b. Passthrough printer type coverage + metadata.**
`print_passthrough_value` (`ResultPrinter.cpp:1149-1250`) rejects DATE /
DATETIME2 / TIME2 / TIMESTAMP2 / YEAR / DECIMAL / BIT / binary at
`:1243-1248`, and the passthrough ctor (`:165-189`) carries no
`ColumnMetadata` at all (`m_column_metadata = NULL`), so the temporal /
decimal-scale handling the aggregate path has
(`RonSQLPreparer.cpp:5190-5216`) never reaches it. Plumb an optional
`ColumnMetadata` argument into the passthrough ctor (built from
`resolved_columns[].dict_column`, same as the aggregate path) and add
the missing type arms (reuse the aggregate path's temporal unpack
helpers, incl. the D17 date machinery and `ronsql_utc_sec_to_TIME`).
Without this, realistic snowflake queries (dates everywhere in TPC-H /
feature-store schemas) fail at print time. BLOB/TEXT stay rejected
(also unsupported as linked values).

---

## Phase 1 — single-table non-aggregate queries

**Detailed plan: `non_aggregate_phase_1.md` — IMPLEMENTED (August
2026), pending user build + MTR --record** (work items W1-W6, v1
residual-on-PK policy, MTR family `body_passthrough_single_table.inc`
st-1..14/P1..P4 ×5 topology suites).

Target shapes (`all_column_outputs`, no joins, no CTEs, no GROUP BY /
HAVING; ORDER BY / LIMIT still rejected, per the interlock):

- **1a** `SELECT a, b FROM t WHERE pk = 1 [AND residual]` — PK lookup.
- **1b** `SELECT a, b FROM t WHERE indexed >= x [AND ...]` — ordered
  index scan with bounds + residual filter.
- **1c** `SELECT a, b FROM t [WHERE residual]` — full table scan +
  filter.

**Execution mechanism — decision: reuse the plain-NDB-API path, not
NdbQueryBuilder.** Rationale: it is what ha_ndbcluster uses for
non-pushed access (DBTC→DBLQH, no SPJ hop, no QueryTree serialization);
the whole planning half (`plan_index_and_filter`, `ScanConfig`,
`condition_handling_map`, `apply_filter_top_level`, the `setBound`
inversion mapping at `:5848-5862`) is already there; and it keeps
`SF_OrderBy` / `SF_MultiRange` available for the ORDER BY/LIMIT plan's
Phase 4b. The alternative (single-op NdbQueryBuilder query reusing
`emit_root_op` + `execute_passthrough_drain`) would save a drain loop
but pays the SPJ hop on every single-table query — wrong default for
the simplest and most latency-sensitive shape. The
`plan_index_and_filter` vs `select_root_scan_config` duplication is a
known seam; unifying them is optional cleanup, not a dependency.

Work items:

1. **Gate**: in the `:568-707` block, accept single-table
   projection-only real-table queries (no joins, `!from_is_cte`,
   all-COLUMN outputs, no GROUP BY/HAVING/ORDER BY/LIMIT). Keep the
   clean error for everything else.
2. **PK-lookup detection**: `plan_index_and_filter()` today only picks
   scan configs. Add a pre-step that walks `m_toplevel_conditions` for
   a full PK equality cover (reuse the `collect_pk_equalities` logic
   from `:7106`); record it as a new `ScanConfig` variant
   (`pk_lookup = true`, residual conjuncts in the handling map).
3. **Execution**: a non-aggregate branch in `execute()`'s single-table
   path (`:5771` onward), skipping `NdbAggregator` construction
   entirely:
   - PK lookup: `startTransaction` with the PK as hint (fragment
     locality), `getNdbOperation(table)` → `readTuple(LM_CommittedRead)`
     → `equal()` per PK column → `getValue()` per projected column →
     residual conjuncts as interpreted code on the op (verify the plain
     `NdbOperation::setInterpretedCode` read path; if unavailable or
     > 64 words, fall back to the 1b/1c scan emission — always
     correct). A filtered-out row must print as an empty result, not an
     error (map NoDataFound / interpreter-reject accordingly).
   - Scans: existing table/index-scan setup minus
     `setAggregationCode`/`DoAggregation`, plus `getValue()` per
     projected column, `m_trans->execute(NoCommit)`, `nextResult()`
     drain feeding `print_passthrough_row` (deferred TSV header, same
     convention as the join drain).
   - Output resolution via `resolved_columns` (`load_single_table`
     already populates `ResolvedColumnRef` with `join_op_idx = 0`).
4. **EXPLAIN**: print the chosen access (PK_LOOKUP / INDEX_SCAN with
   bounds / TABLE_SCAN) + CONDITIONS, mirroring the join-root EXPLAIN
   print.
5. **MTR**: new `ronsql_nonagg_single_table` family (see the test
   strategy section): PK hit / PK miss / PK + residual accepted and
   rejected rows, half-open and double bounds, residual-only, no-WHERE
   full scan, every projected column type incl. temporals + DECIMAL
   (Phase 0b), NULL columns, empty results (no header), FORCE/USE/IGNORE
   INDEX on the root, EXPLAIN. Row order is nondeterministic without
   ORDER BY — rely on `ronsql_compare.inc` sorted comparison.

---

## Phase 2 — snowflake-schema pushed joins (real tables only)

Target: left-deep-or-tree FK chains `root → child → grandchild …`,
`SELECT` of bare columns from any of the tables.

**2a. All child accesses are PK (or unique-index) lookups, INNER
joins.** The NDB API's fully green path (`LookupQuery` /
`SingleScanQuery`, no batch-repeat semantics, no version gates).

- **Gate**: this is the core relaxation — drop the `query_has_cte`
  requirement (`:694-695`) and restructure the shape-B walk so it
  admits pure real-table chains. Rather than growing more booleans,
  restructure the walk to mirror `QueryPlanner`'s own requirements
  (each ON binds child columns to one already-visible parent alias) so
  gate and planner cannot drift apart; keep per-shape clean errors.
- **Planner/emit**: nothing new — `PK_LOOKUP`/`UNIQUE_LOOKUP`
  classification, linked `readTuple` emission, per-child
  `NdbScanFilter`, root selection (`select_root_scan_config` +
  `root_pk_equality_covered` + Phase 0a's fixed readTuple root) all
  exist. Root may be `readTuple` (all children lookups), `scanIndex`,
  or `scanTable`.
- **New pre-checks for clean errors** (today these would surface as raw
  NDB 48xx codes at execute):
  - linked-column type identity (the `:2067-2091` rule) checked in
    `QueryPlanner` against dict columns → clean "join columns must have
    identical declarations" error;
  - node budget: count unique-index lookups as **2** internal nodes
    against `MAX_SPJ_TREE_NODES` (the current planner counts them
    as 1);
  - BLOB/TEXT join columns rejected.
- **Kept restrictions** (clean errors, revisit later): cross-table
  WHERE residuals (`a.x > b.y`) stay rejected for non-aggregate queries
  (`:5130-5137` requires the aggregate sentinel machinery); DISTINCT /
  expressions in the projection / `SELECT *` remain unsupported.

**2b. Index-scan children in the snowflake.** `INDEX_SCAN` children
(linked bounds from parent columns) join the accepted set — the query
becomes `MultiScanQuery` (`RT_MULTI_SCAN` + `RT_REPEAT_SCAN_RESULT`).

- Emission exists (`:8504-8547`). The work is gate acceptance, the
  planner pre-checks above, and **verification that the passthrough
  drain is correct across scan-scan batch boundaries** — a dedicated
  MTR case with a child rowset large enough to force multiple
  SCAN_NEXTREQ round-trips (compare against MySQL on the same data).
- Child-scan constraints to encode: no ordering on child scans (we have
  no ORDER BY anyway), child batch-size rule
  (`NdbQueryOperation.cpp:7059-7073`) left at defaults.
- Known optimizer gap carried over from `next_steps.md` ("join-root
  index scan follow-ups"): child-local **constant** bounds (e.g.
  `o_orderdate >= X` on index `(o_custkey, o_orderdate)`) are emitted
  as filters, not bounds. Same gap as aggregate joins; not a blocker —
  track there, don't fix here.

**2c. LEFT OUTER in the snowflake.** The drain's `isRowNULL()`
substitution (I.12) and `MatchAll` emission already exist; shape-B
already admits LEFT_OUTER on CTE chains. Extend to real-table lookup
children (cheap) and scan children — the latter needs the
`NdbQueryBuilder::outerJoinedScanSupported(ndb)` probe (`:939-947`)
surfaced as a clean error, and outer-join nest options
(`setFirstInnerJoin` / `setUpperJoin`) once mixed INNER/LEFT chains
are accepted; follow `ha_ndbcluster_push.cc:2589-2625` as the
reference. Semi/anti-join (the dormant `MatchFirst` / `MatchNullOnly`
infra from phase7 Step 40) stays out of scope; note ha_ndbcluster
deliberately forbids semi-joined index scans (`:1584-1621`).

MTR (`ronsql_nonagg_snowflake` family): 2/3/4-level chains; root
readTuple + lookup chain; root index-scan + lookup chain; scan child
mid-chain; per-table WHERE on root/mid/leaf; LEFT JOIN NULL rows incl.
NULL join keys; unique-index child; type-mismatch join rejection;
32-node and 8-key-col cap rejections; multi-batch scan-scan case;
projection touching only a subset of tables (empty-projection guard);
EXPLAIN for each.

---

## Phase 3 — star schemas (fan-out on a common key)

Target (the `star_schema_plan.md` schema, minus aggregation):

```sql
SELECT e.name, m.value, ev.event_type
FROM entity e
JOIN measurements m ON m.entity_id = e.entity_id
JOIN events ev      ON ev.entity_id = e.entity_id
WHERE ...
```

**3a. Lookup-children fan-out.** All children bind their full PK (or a
unique index) from the shared parent key — N `readTuple` children on
one parent. API-legal and proven (`testStarJoinAggNdbApi.cpp` builds
exactly this tree). Planner already resolves both children to the same
parent; verify no chain-shaped assumption survives in `emit_child_ops`
ordering, `classify_where_by_table`, and the drain's op-index mapping.
`agg_leaf_idx` (always last op) must be confirmed dead when
`leafAggs == nullptr` — for non-aggregate queries there is no
single-leaf constraint at all.

**3b. Scan-children fan-out (bushy).** The common-key star typically
binds only a PK *prefix* (`(entity_id, ts)` keyed on `entity_id`) — so
children classify as `INDEX_SCAN` on the PK ordered index, and the tree
is the bushy case `NI_REPEAT_SCAN_RESULT` was designed for
(`QueryTree.hpp:121-131`). Semantically this is exactly MySQL's cross
product per parent row (`m × ev` per entity) — correct, and worth a
comment in the tests because the result *sizes* surprise people. DBSPJ
executes sibling scan branches sequentially by legacy protocol
(`DbspjMain.cpp:2944-3000`) — a performance note, not a correctness
issue. Constant per-child bounds on `ts` hit the 2b optimizer gap
(filter, not bound) — acceptable v1.

MTR (`ronsql_nonagg_star` family): 2- and 3-leaf lookup stars; 2-leaf
scan star with cross-product verification vs MySQL; mixed lookup+scan
star; star grafted onto a snowflake chain (tree shape); LEFT JOIN on
one branch; WHERE on individual branches; multi-batch bushy case;
EXPLAIN showing the fan-out.

---

## Phase 4 — scalar aggregate CTEs in non-aggregate main queries

Target: snowflake/star projection queries joined with single-row
aggregating CTEs (Phase I.17 machinery):

```sql
WITH stats AS (SELECT MAX(o_orderdate) AS max_d FROM orders)
SELECT c.c_name, c.c_acctbal
FROM customer c JOIN stats
WHERE c.c_last_order = stats.max_d;   -- or filter vs stats.max_d
```

The kernel and API sides are shipped (scalar redistribute I.17e,
dummy-key `lookupCte` child emission `:8580-8629`, scalar-CTE-root
`lookupCte` + filter `:6881-6931`). The work is front-end admission and
condition routing:

- **4a. Gate**: in the shape-B walk, accept
  `cte_key_coverage == ScalarDummy` join operands (today only
  Exact* pass, `:683-689`) when the CTE is scalar; keep the I.21
  guardrails (dummy-key workaround stays local to child lookups; no
  keyless root `lookupCte` for WHERE-carrying root scalar CTEs).
- **4b. Condition routing**: predicates comparing a real-table column
  against a scalar CTE output must land as jump-table filters on the
  `CTE_LOOKUP` op (Phase I.3 col-vs-col + I.5 machinery — verify which
  comparisons `classify_where_by_table` currently routes vs rejects for
  non-aggregate queries, and extend the accept-list). Cross-table
  real-vs-real residuals stay rejected as in Phase 2.
- **4c. Drain/printer**: scalar CTE outputs arrive like any CTE virt
  column (all registered in attrId order); nullable scalar outputs
  (COUNT-over-empty → NULL, I.21) must print correctly through the
  passthrough printer.
- MTR (`ronsql_nonagg_cte_scalar` family): scalar CTE joined to
  single-table main query; into a snowflake chain; into a star branch;
  WHERE vs scalar output (both directions, incl. NULL scalar); two
  scalar CTEs (cross-join dummy-key pattern, Test 20 shape); LEFT JOIN
  scalar CTE; EXPLAIN.

---

## Phase 5 — GROUP BY CTEs in non-aggregate main queries

Target: the general form of what I.8/I.11/I.12 pioneered — grouped
CTEs (`CTE_LOOKUP` on the GROUP BY key) joined anywhere into the
snowflake/star trees of Phases 2-3:

```sql
WITH per_cust AS (SELECT o_custkey, COUNT(*) AS cnt, SUM(o_total) AS sp
                  FROM orders GROUP BY o_custkey)
SELECT c.c_name, n.n_name, per_cust.cnt, per_cust.sp
FROM customer c
JOIN nation n    ON n.n_nationkey = c.c_nationkey     -- real snowflake arm
JOIN per_cust    ON per_cust.o_custkey = c.c_custkey  -- grouped CTE arm
WHERE ...;
```

After Phases 2-3 the gate walk already admits mixed trees; Phase 5 is
mostly removing the remaining CTE-specific asymmetries and testing the
cross product of shapes:

- **5a**: complete-key `CTE_LOOKUP` children mixed freely with
  real-table lookup/scan children, incl. as star branches and below
  scan children; multiple distinct CTEs in one query (sibling
  re-chaining via `tree_parent_op_idx` exists); CTE-rooted trees with
  real snowflake arms below (generalizing I.12 beyond its fixed
  shapes).
- **5b**: LEFT OUTER on the CTE arm (I.12's NULL substitution covers
  the drain; the agg-feed NULL injection machinery is irrelevant
  without a main aggregator).
- **Kept restrictions**: partial-key CTE joins stay rejected
  (`scanCte` takes no key array — same envelope as I.16; the I.16b/c
  root-rewrite can be evaluated for non-aggregate queries as a
  follow-up); `CTE_SCAN` as keyed join child (`:8643-8645`) and as
  LEFT_OUTER child (`:4096-4114`) stay rejected; **non-aggregating
  (projection-only) CTE bodies stay rejected** (`analyze_ctes`
  `:3925-3938` stands — CTE bodies must aggregate; lifting that is a
  separate kernel-facing feature, not part of this plan).
- MTR (`ronsql_nonagg_cte_groupby` family): the shape above ×
  variations (CTE arm first vs last, two CTEs, CTE under a scan child,
  LEFT JOIN CTE arm with NULL groups, WHERE on CTE outputs, string /
  temporal CTE outputs through the passthrough printer), plus
  regression reruns of the existing I.8/I.11/I.12 tests against the
  restructured gate.

---

## Test strategy (all phases)

- New data-driven families in the `ronsql_cte` suite + its four
  topology siblings (`ng1r3/ng2r2/ng2r3/ng4r2`), following
  `cte_test_authoring_guide.md` conventions (`ronsql_compare.inc`
  RonSQL-vs-MySQL comparison, AS-aliases, `ronsql_explain.inc`).
  Non-aggregate results have nondeterministic row order — every case
  relies on sorted comparison (no ORDER BY support yet).
- One `.inc` family per phase as listed above; each lands with its
  phase, ×5 topologies. Multi-node topologies are load-bearing for 2b/3b
  (scan-scan batching differs with fragment count).
- Negative tests are first-class: every kept restriction and every new
  planner pre-check gets a rejection case asserting the clean error
  text, so API 48xx codes never reach users.
- No new kernel/API block tests expected (`testSpj`, `spj_sanity_test`,
  `testStarJoinAggNdbApi`, `testJoinAggScanScan`, `testCteNdbApi`
  already pin the API envelope); if a drain bug surfaces in 2b/3b, add
  a non-aggregate multi-batch case to `testCteNdbApi`-style block tests
  then.
- Benchmarks: after Phase 1 + the ORDER BY/LIMIT plan's Phases 2-4,
  `fs_history` unlocks; after Phase 5, projection-only `fs_topk`'s
  original form unlocks (see that plan's Phase 5). Consider a
  `nonagg_*` `.bench_ronsql` entry comparing a snowflake projection
  query vs MySQL to quantify the SPJ win on row-reducing joins.

## Risks / notes

- **Result volume**: non-aggregate queries can return arbitrarily many
  rows. The drain streams (no buffering), so RonSQL itself is safe, but
  RDRS/rondb-cli response handling should be sanity-checked with a
  million-row result before calling Phase 2 done. LIMIT (other plan,
  Phase 2) is the real mitigation.
- **Scan-scan repeat protocol** is the one place where "the API already
  supports it" needs proof at our layer — the multi-batch MTR cases in
  2b/3b are mandatory, not nice-to-have.
- **Committed-read semantics**: no locks, re-reads between batches;
  same as every existing RonSQL query, but non-aggregate results make
  torn reads *visible* as rows rather than folded into aggregates.
  Document, don't fix.
- **Gate restructuring regression risk**: shapes A/B carry shipped MTR
  coverage (I.8/I.11/I.12, E.3, outer-join tests) — rerun the full
  `ronsql` + `ronsql_cte` suites at every phase; the restructured walk
  must keep their acceptance and their error texts.
- **Phase 0a is a behavior change** for aggregate queries too (silently
  wrong results become correct); call it out in the commit message.
- Phase ordering is strict only where stated (0 → 1 → 2a → {2b, 2c, 3a}
  → 3b → 4 → 5); 2b/2c/3a are independent of each other.

## Verification (user-run)

- Per phase: `./mtr --suite=ronsql ronsql_basic` +
  `./mtr --suite=ronsql_cte <new family>` ×5 topology suites, plus the
  existing passthrough regressions (`ronsql_cte_outer_join`,
  `ronsql_cte_scan`, `ronsql_cte_scalar`).
- `.explain_ronsql` on representative shapes showing PK_LOOKUP /
  INDEX_SCAN / TABLE_SCAN roots, linked children and fan-out.
- 2b/3b: the multi-batch comparison cases against MySQL on the
  data-rich suite schema.
