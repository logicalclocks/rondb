# Non-Aggregate Pushdown — Phase 1 Detailed Plan

**Status: IMPLEMENTED (August 2026) — pending user build + MTR --record.**

Implementation deltas vs the plan below:

- Landed as a single change rather than the two-commit split (the
  scan-arm and lookup-arm shared too much scaffolding to be worth
  separating).
- `detect_pk_lookup` implements the v1 policy in its simplest
  equivalent form: it returns false on ANY conjunct that is not a
  consumable `pk_col = const` equality (non-equality, non-PK column,
  duplicate, partial cover) — so "residual exists" and "not fully
  covered" collapse into one fallback path.  `m_pk_lookup_const` is
  arena-allocated per PK column count instead of a fixed-size member
  array.
- `register_passthrough_getvalues` is a shared helper (works on
  `NdbOperation*`; `NdbScanOperation` derives from it), enforcing the
  outputs-order == attrs-order contract the Phase 0b printer metadata
  lookup relies on.
- MTR family: `body_passthrough_single_table.inc` (st-1..14 +
  st-P1..P4) with EXPLAIN greps on st-1 (PK lookup line), st-8 (index
  scan + filter), st-11 (table scan); wrappers
  `ronsql_cte_dd_passthrough_single_table.test` ×5 suites; findings
  file `findings/passthrough_single_table.md` (incl. the WHERE
  IN-subquery NEXT-PHASE probe).
- The gate error text now names single-table projections and points at
  `non_aggregate_pushdown_plan.md` instead of `ronsql_join_phase7.md`.
**Parent:** `non_aggregate_pushdown_plan.md` (Phase 1 overview).
**Predecessor:** `non_aggregate_phase_0.md` (shipped: residual filters,
pass-through type coverage, lookup-root crash hardening).

## Goal

Single-table non-aggregate queries — the first shape where RonSQL
accepts a query with **no aggregation and no CTE**:

- **1a** `SELECT a, b FROM t WHERE pk = 1` — PK lookup (single-row).
- **1b** `SELECT a, b FROM t WHERE indexed >= x [AND ...]` — ordered
  index scan with bounds + residual filter.
- **1c** `SELECT a, b FROM t [WHERE residual]` — full table scan +
  filter.

Projection-only (`all_column_outputs`), no joins, no CTEs, no GROUP
BY / HAVING.  ORDER BY / LIMIT were rejected at the time — they layered
on top via `ronsql_orderby_limit_plan.md` (Phase 2 streaming LIMIT,
Phase 3 buffered sort, Phase 4b `SF_OrderBy` index-order streaming —
all shipped / implemented since).

## Execution mechanism (decided in the parent plan, confirmed here)

**The plain NDB API path, not NdbQueryBuilder**: RecAttr-style
`getNdbOperation` / `getNdbScanOperation` / `getNdbIndexScanOperation`,
exactly what the single-table *aggregate* path uses today
(DBTC→DBLQH, no SPJ hop, no QueryTree serialization).  The planning
half is fully reusable: `plan_index_and_filter()`
(`RonSQLPreparer.cpp:2305`, gated `!is_join_query()` at `:184-185`),
`ScanConfig` + `condition_handling_map`, `apply_filter_top_level`, and
the bound-type inversion mapping (`:5848-5862`).  Only the delivery
half is new.

**v1 policy for residuals on PK lookups.**  The RecAttr-style
`NdbOperation` read has no interpreted-code facility — `OO_INTERPRETED`
on `readTuple` exists only in the NdbRecord API
(`NdbTransaction.hpp:751`), and RonSQL's single-table path (and the
Phase 0b printer) is RecAttr end-to-end.  So in v1:

- Full PK equality cover **and no residual conjuncts** → PK lookup.
- Full PK cover **with** residuals → the normal scan-config path
  (bounds + filter — always correct; on a hash-PK table this is a
  table scan, the same envelope the aggregate path has today).
- Named follow-up (not v1): an NdbRecord-based `readTuple` with
  `OO_INTERPRETED` (+ `OO_GETVALUE` extra gets, or an NdbRecord row
  decode in the printer) to carry residuals on lookups — or routing
  the shape through NdbQueryBuilder where Phase 0 already does
  readTuple + filter, at the cost of the SPJ hop.

This differs from the join path deliberately: there, Phase 0 attaches
lookup filters via `NdbQueryOptions` because the pushed-query API
supports it; here the plain API doesn't, and correctness must not
depend on it.

## Work items

### W1 — parse-gate shape C

In the `parse()` non-aggregate gate (the `:568-707` block), add a third
accepted shape alongside E.3 (shape A) and the CTE join chains
(shape B):

```cpp
bool projection_only_single_table =
    (has_from && !from_is_cte && !has_joins && all_column_outputs &&
     !has_groupby && !has_having && !has_orderby && !has_limit &&
     cte_list == NULL);
```

Notes:

- `has_from` guards the Phase I.17h optional-FROM path (a FROM-less
  all-COLUMN query cannot resolve; let it keep its current clean
  rejection).
- `cte_list == NULL` keeps CTE-involving pass-through on shapes A/B —
  a `WITH x AS (...)` prefix on a single-table query stays rejected
  rather than silently ignoring the CTE.
- Update the `:697-706` rejection text: it currently names only
  "aggregate queries and projection-only SELECTs over supported CTE
  shapes"; add plain single-table projection to the supported list,
  and keep ORDER BY / LIMIT rejections pointing at their plan.
- `m_is_aggregate_query` stays false; the Phase 0b passthrough
  `ResultPrinter` construction (with `ColumnMetadata`) already handles
  this in `compile()`.

### W2 — PK-lookup detection in the planner

`plan_index_and_filter()` today only selects scan configs.  Add a
detection pre-step after `collect_toplevel_conditions`, **guarded
`!m_is_aggregate_query`** (single-table aggregation runs through
`NdbScanOperation` + `SO_AGGREGATION`; a plain `readTuple` has no
aggregator path — the single-table twin of the Phase 0 lookup-root
rule):

- Walk `m_toplevel_conditions` for `col = const` conjuncts covering
  every PK column (the same matching rule as
  `collect_pk_equalities`, but over the already-flattened conjunct
  array so consumed conjuncts are tracked by index).
- If fully covered **and every conjunct was consumed** (no residual):
  set new members `m_pk_lookup = true` and
  `m_pk_lookup_const[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY]`, and skip
  scan-config candidate selection.
- Otherwise: proceed exactly as today (candidates → `m_scan_config`).
  Duplicate/contradictory PK equalities simply fail full-consumption
  and take the scan path — correct by construction.

Composite PKs (e.g. `lineitem (l_orderkey, l_linenumber)`) fall out of
the same walk; a partial PK equality is just a normal condition for
the candidate generator.

### W3 — non-aggregate execution path

Dispatch in `execute()` right after the `is_join_query()` branch
(`:5771-5775`): `if (!m_is_aggregate_query) {
execute_single_table_passthrough(); cleanup_trans(); return; }` — the
aggregate path below it stays untouched (today it would deref a NULL
agg program; the branch makes that unreachable).

`execute_single_table_passthrough()`:

1. **Output registration** (shared by both arms): walk
   `m_context.ast_root.outputs` in order; each is
   `Outputs::Type::COLUMN` (gate-guaranteed; `ndbrequire`), resolve
   via `m_main_scope.resolved_columns[o->column.col_idx]`
   (`load_single_table` populates these with `join_op_idx = 0`),
   require `Kind::StoredColumn`, and `op->getValue(dict_column)` into
   `attrs[i]`.  Output order == attrs order is the
   `print_passthrough_row` contract (Phase 0b metadata lookup keys on
   the same outputs walk).
2. **PK-lookup arm** (`m_pk_lookup`):
   `m_trans->getNdbOperation(table)` →
   `readTuple(NdbOperation::LM_CommittedRead)` → per PK column
   `equal(pk_name, rv.val)` with `encode_constant(m_pk_lookup_const[k],
   pk_col)` → getValues → `m_trans->execute(NdbTransaction::Commit)`.
   - Row found: JSON frame open + row + close, or TSV header + row
     (conventions below).
   - No row: the operation fails with classification
     `NdbError::NoDataFound` (626) — treat as an empty result, not an
     error: TSV prints nothing (deferred header), JSON prints an empty
     frame.  Any other error goes through
     `handle_ronsql_exception` as usual.  Verify the exact
     classification check during implementation (match on
     `classification == NoDataFound`, not the literal code).
3. **Scan arm** (everything else): the existing table-scan /
   index-scan setup from the aggregate path (`:5795-5897`) minus
   `setAggregationCode` + `DoAggregation`: `readTuples
   (LM_CommittedRead)`, bounds from `condition_handling_map` with the
   inverted `setBound` mapping, `end_of_bound(0)`,
   `NdbScanFilter` + `apply_filter_top_level` when any
   `map[i] == -1`, then getValues, `execute(NoCommit)`, and a drain:
   `while (scan_op->nextResult(true) == 0) print row;` (1 = done,
   -1 = error → retryable, mirroring the join drain's error text).
4. **Output framing** — copy `execute_passthrough_drain`'s
   conventions exactly (`:6641-6707`): JSON opens `[` up front and
   always closes; TSV defers the header until the first row so empty
   results produce no output, matching the mysql-client baseline that
   `ronsql_compare.inc` diffs against.  All value printing goes
   through the Phase 0b `print_passthrough_row` /
   `print_passthrough_value` (temporals, DECIMAL, zero-TIMESTAMP rule
   included for free).

### W4 — shared scan-setup helper (recommended, behavior-preserving)

The bounds + filter setup (`:5819-5897`) is subtle (inverted bound
mapping) and would otherwise exist twice.  Factor
`open_single_table_scan_op()` returning the configured
`NdbScanOperation*` (filter applied, no aggregation attached), used by
both the aggregate path (which then adds `setAggregationCode` +
`DoAggregation`) and the new drain.  Pure refactor; the aggregate
MTR suite (`ronsql_basic` etc.) is the regression net.

### W5 — EXPLAIN

Extend the single-table branch of `print()`:

- `m_pk_lookup` → an access line naming the PK lookup and the bound
  key values' columns.
- Otherwise the existing scan-config print (index + bound/filter
  conditions) applies as-is.
- No aggregation section for non-aggregate queries.

Unlike the join path's emit-time refinements, the single-table access
choice lives in the planner state, so EXPLAIN can tell the whole truth
here — worth stating in the EXPLAIN output tests.

### W6 — MTR family

`body_passthrough_single_table.inc` + wrapper
`ronsql_cte_dd_passthrough_single_table.test` ×5 topology suites (the
data-rich harness; `ronsql_compare.inc` strict diff, sorted compare
since there is no ORDER BY).  Cases (st-N):

| # | Case | Arm |
|---|---|---|
| 1 | PK hit, mixed-type projection (INT, DATE, DECIMAL, CHAR from `orders`) | lookup |
| 2 | PK miss → empty output (no TSV header) | lookup |
| 3 | Composite PK full equality (`lineitem` 2-col PK) | lookup |
| 4 | PK equality + accepting residual | scan fallback (v1 policy) |
| 5 | PK equality + rejecting residual → empty | scan fallback |
| 6 | Partial composite PK (`l_orderkey = X` only) | index/scan |
| 7 | Secondary-index equality (`idx_o_custkey`) | index scan |
| 8 | Half-open range (`idx_o_orderdate`) + residual filter | index scan |
| 9 | Double-bounded range | index scan |
| 10 | Composite index prefix (`idx_o_status_date`, QUERY_FILE for the CHAR literal) | index scan |
| 11 | Residual-only WHERE, no usable index | table scan |
| 12 | No WHERE (full projection of `nation`, 25 rows) | table scan |
| 13 | Temporal projection sweep (`evlog`: YEAR, DATETIME(6), TIME(3), TIMESTAMP(6)) | any |
| 14 | NULL values (`o_clerk`) | any |
| P1 | rejection: ORDER BY on single-table projection (message points at the ORDER BY/LIMIT plan) | — |
| P2 | rejection: LIMIT | — |
| P3 | rejection: expression in the projection (`SELECT o_orderkey + 1 ...`) | — |
| P4 | rejection: `WITH x AS (...)` prefix on a single-table projection | — |

EXPLAIN checks (`ronsql_explain.inc`, greps non-fatal at authoring):
st-1 shows the PK-lookup access line; st-8 shows index + bounds +
filter; st-11 shows table scan + filter.

## Commit sequencing

1. **W1 + W3-scan + W4 + W6 subset** — gate + scan-arm execution
   (every accepted query runs through the scan config, including
   PK-covered ones) + the refactor + cases st-4..14/P1..P4.  Green and
   useful on its own.
2. **W2 + W3-lookup + W5 + remaining W6** — the PK-lookup fast path,
   EXPLAIN, cases st-1..3.

## Interlocks

- `is_join_query()` routing, the aggregate single-table path, and
  everything join/CTE are untouched (W4 refactor excepted).
- `ronsql_orderby_limit_plan.md`: its Phase 4 became "add ORDER BY /
  LIMIT to this path" (Phase 2 streaming limit / Phase 3 buffered sort
  / 4b `SF_OrderBy` index-order top-N — all delivered).  `fs_history`
  unlocked with Phase 3 (buffered); `fs_latest` is the 4b index-order
  benchmark.  Phase 1 alone did not flip any CLI benchmark flags.
- Parent-plan Phase 2 (snowflake joins) is independent — it relaxes
  the same gate for join shapes and runs on NdbQueryBuilder.

## Risks / notes

- **EXPLAIN-only mode**: `plan_index_and_filter` returns early when
  `m_conf.ndb == NULL`; the PK detection needs the dictionary too —
  skip it there, matching the existing partial-EXPLAIN behavior.
- **NoDataFound mapping**: the one place a "failed" NDB call is a
  normal result.  Get the classification check right and lock it in
  with st-2 (a wrong mapping shows up as a spurious error line in the
  recorded output).
- **Streaming only** — no buffering, no memory cap needed; result
  volume is the client's concern until LIMIT lands.
- **Committed reads**, same as every RonSQL query; a scan sees no
  snapshot.  Document, don't fix.
- The v1 PK+residual scan fallback on hash-PK tables reads the whole
  table for one row — same suboptimality class as the Phase 0
  aggregate no-CTE gap, resolved by the named NdbRecord
  `OO_INTERPRETED` follow-up rather than v1 scope creep.

## Verification (user-run)

- Build, then `./mtr --record --suite=ronsql_cte
  ronsql_cte_dd_passthrough_single_table` + the 4 siblings.
- Regression: `./mtr --suite=ronsql ronsql_basic` (W4 refactor
  touches the aggregate scan setup) and the full `ronsql_cte` suite.
- `.explain_ronsql` spot checks: PK-lookup line on st-1's shape,
  bounds + filter on st-8's.
