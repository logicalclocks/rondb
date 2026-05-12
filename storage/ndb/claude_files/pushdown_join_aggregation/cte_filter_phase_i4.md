# Phase I.4 — CASE with a CTE column in the condition

## Status

**Shipped** in commit `4828effed0e` ("RONDB-1050: support CASE
conditions on CTE columns").

## Background

`SUM(CASE WHEN <cond> THEN <expr> ELSE <expr> END)` already worked
in RonSQL for non-CTE shapes (parser → `AggregationAPICompiler::CaseExpr`
→ `programAggregator(...)` SVMInstrType::EmbeddedInterp dispatch
→ `generate_embedded_condition`).  Two code-paths in
`programAggregator_join` already handled CTE virt columns (Phase
E.2 / E.3 / D2):

- Load in THEN / ELSE arms — `RonSQLPreparer.cpp`'s join-aggregator
  Load handler.
- GroupBy column from a CTE.

But CASE *conditions* on CTE columns crashed in two distinct places:

1. **Preparer:** `generate_embedded_condition` read
   `m_main_scope.column_map[col_side->col_idx]`, which is `NULL`
   for CTE virt columns (`build_cte_virtual_tables` only populates
   the virt-table descriptor; the per-column-idx column_map slot is
   not used for CTE outputs).  Calling `getLength()` on NULL
   crashed before any kernel signal was sent.

2. **Kernel:** even with the preparer crash worked around, the
   downstream aggregator on a CTE_LOOKUP / CTE_SCAN agg-feed row
   never ran the embedded interpreter.  `Dblqh::cteLookupAggFeed`
   (and the two CTE_SCAN agg-feed sites) called
   `processRecWithLinkedAttrs(nullptr, nullptr, linkedBuf, ...)`,
   which triggered `m_null_local_columns = true`, which made
   `kOpEmbeddedInterp` in `JoinAggInterpreter::ProcessRec` short-circuit
   the embedded program — silently advancing `exec_pos` past it and
   always taking the THEN arm.  Result: the condition was never
   evaluated; every row took THEN.

This blocks the shape:

```sql
WITH sums AS (SELECT k, SUM(amt) AS t FROM orders GROUP BY k)
SELECT c.c_id, SUM(CASE WHEN sums.k = 100 THEN sums.t ELSE 0 END)
FROM cte_customer c JOIN sums ON sums.k = c.c_id
GROUP BY c.c_id;
```

even though every individual ingredient (CTE LOOKUP child agg,
linked-projection of the CTE column for the condition load,
embedded BRANCH_MEM_OP_ARG comparing it to a constant, THEN/ELSE
arms each loading a CTE column) is independently supported.

## What shipped

### Kernel (DBLQH)

`Dblqh::cteLookupAggFeed` and the two CTE_SCAN agg-feed sites in
`Dblqh::cteScanAggFeed` now construct a real `Dbtup::KeyReqStruct
aggReq(c_tup)` and pass `(c_tup, &aggReq, linkedBuf, ...)` instead
of nullptr/nullptr.  This gives `processRecWithLinkedAttrs` the
DBTUP block reference it needs to dispatch the embedded interpreter
on a CTE agg-feed row, and prevents `m_null_local_columns` from
short-circuiting the embedded program.  `aggReq.no_exec_instructions`
is reset before each call (and before each retry-after-eviction).

### Kernel (DBTUP)

`Dbtup::interpreterNextLab` now dispatches
`BRANCH_MEM_OP_ARG_INLINE_TYPE` (handler already existed; one more
case in the switch).  This is what the preparer emits when the
CASE condition references a CTE leaf column whose linked-attr
buffer entry uses CteLinkedAttr inline-type encoding.

### Kernel (JoinAggInterpreter)

`validateEmbeddedProgram`'s whitelist now accepts
`BRANCH_MEM_OP_ARG_INLINE_TYPE` so embedded programs that reference
CTE leaf columns pass validation.

### Preparer (RonSQLPreparer)

`generate_embedded_condition` gains a `QueryScope&` and
`NdbDictionary::Table* const* cteVirtualTables` parameter, replacing
the implicit `m_main_scope` read.  Two new helpers
(`require_cte_case_condition_column_output` and
`resolve_case_condition_column`) gate and resolve the column
descriptor for CTE references.  Three condition-column shapes are
now handled:

1. Parent-table column (existing path) — `BRANCH_MEM_OP_ARG` with
   parent-table tableId+schemaVersion.
2. **CTE COLUMN projection on a non-leaf CTE op** — resolves
   `(parentTable, attr_id)` from the CTE body's source op (via
   `m_cte_scopes[cte_def_idx]`), then `BRANCH_MEM_OP_ARG`.
3. **CTE COLUMN projection on the leaf CTE op** — uses the new
   `BRANCH_MEM_OP_ARG_INLINE_TYPE` opcode because the agg-feed
   linked-buffer entry carries inline type metadata
   (`CteLinkedAttr`).  The READ_LINKED_TO_MEM position is computed
   as `num_linked_projs + cte_col_idx` so it lands in the CTE's
   per-row slot.

Two caller updates (`programAggregator` passes `&m_main_scope,
nullptr`; `programAggregator_join` passes `&scope, cteVirtualTables`).
Header signature updated.

### Test coverage

New `mysql-test/suite/ronsql/t/ronsql_cte_case.test` with 6 cases:

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `SUM(CASE WHEN c.c_region = 1 THEN sums.t ELSE 0 END)` | Parent-table condition + CTE THEN arm |
| 2 | `SUM(CASE WHEN sums.k = 100 THEN sums.t ELSE 0 END)` | CTE-column condition (the I.4 fix) |
| 3 | `SUM(CASE WHEN sums.k = 100 OR sums.k = 200 THEN sums.t ELSE 0 END)` | Multi-atom OR over CTE column |
| 4 | `SUM(CASE WHEN sums.k != 100 AND sums.k != 200 THEN sums.t ELSE 0 END)` | Multi-atom AND over CTE column |
| 5 | `SUM(CASE WHEN c.c_region = 1 THEN o.o_amt ELSE 0 END)` (non-CTE) | Regression: existing path unchanged |
| 6 | `WHEN sums.t = 100` (CTE aggregate output) | **Rejected cleanly** — deferred to follow-up |

Tests 1-5 use `ronsql_compare.inc`.  Test 6 uses `--error 1`.

## Cleanly-rejected shapes

- CASE conditions referencing a CTE **AGGREGATE** output
  (`WHEN sums.t = 100` where `t = SUM(...)`).  The preparer
  rejects this in `require_cte_case_condition_column_output` with
  the message "CASE condition referencing a CTE aggregate output is
  not yet supported; reference a CTE column projection instead, or
  move the predicate into the CTE body's WHERE clause."  Lifting
  this needs the inline-type opcode to encode the synthesized
  aggregate result type at the AttrHeader-less linked-buffer slot;
  meaningfully bigger work and deferred.
- Inequality CASE conditions (`WHEN sums.t > 100`).  Pre-existing
  rejection (`ndbrequire(atom->op == T_EQUALS || T_NOT_EQUALS)` in
  `generate_embedded_condition`); not changed by this phase.
- Register-based CASE conditions (`WHEN sums.gross > sums.net` —
  exercised by `testCteNdbApi` Test 21 via `BranchRegGe + Mov`).
  RonSQL doesn't emit that pattern even for non-CTE; needs a new
  codegen path paired with the I.5 GREATEST surface and I.11
  cross-join.
- Cross-CTE column conditions — same cross-table limitation as
  Phase I.3.

## Files

- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` — three CTE
  agg-feed sites pass real KeyReqStruct.
- `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` —
  interpreterNextLab dispatches BRANCH_MEM_OP_ARG_INLINE_TYPE.
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp` —
  validateEmbeddedProgram whitelist additions.
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` —
  generate_embedded_condition rewrite + helpers.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` — signature.
- `mysql-test/suite/ronsql/t/ronsql_cte_case.test` — new file.
- `mysql-test/suite/ronsql/r/ronsql_cte_case.result` — recorded.

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --suite=ronsql ronsql_cte_case
./mtr --suite=ronsql                       # full suite — no regressions
./mtr --suite=ndb_push_agg                 # block tests — no regressions
```

## What we're not doing

- Inequality conditions in CASE (RonSQL `ndbrequire`-rejects today
  even for non-CTE).
- Register-based CASE conditions (Test 21 shape — needs RonSQL
  parser/codegen for `WHEN reg-expr OP reg-expr` plus
  `BranchReg* + Mov` emit).
- CASE conditions on CTE aggregate outputs.
- The `GREATEST` / `LEAST` SQL surface (Phase I.5).
