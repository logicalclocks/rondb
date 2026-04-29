# Phase I.4 — CASE with a CTE column in the condition

## Status

Plan-only.  Verification + targeted fix.

## Background

`SUM(CASE WHEN <cond> THEN <expr> ELSE <expr> END)` already works
in RonSQL for non-CTE shapes (parser → `AggregationAPICompiler::CaseExpr`
→ `programAggregator(...)` SVMInstrType::EmbeddedInterp dispatch
→ `generate_embedded_condition`).  Two code-paths in
`programAggregator_join` already learned how to handle CTE virt
columns (Phase E.2 / E.3 / D2):

- Load in THEN / ELSE arms — `RonSQLPreparer.cpp:7841-7867`.
- GroupBy column from a CTE — `RonSQLPreparer.cpp:7686-7698`.

But the **CASE condition** compilation
(`generate_embedded_condition`, `RonSQLPreparer.cpp:7994-` end-of-file)
still goes:

```cpp
const NdbDictionary::Column* col =
    m_main_scope.column_map[col_side->col_idx];
Uint32 byte_len = col->getLength();
```

For a CTE virt column, `m_main_scope.column_map[col_idx]` is `NULL`
(see comments at `RonSQLPreparer.cpp:7855` and the
`build_cte_virtual_tables` notes).  Calling `getLength()` on NULL
would crash before any kernel signal is sent.  This blocks the
shape:

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

## Scope

Lift the gap in `generate_embedded_condition` so the CASE condition
can reference a column from a CTE_LOOKUP / CTE_SCAN parent.  The
condition shape stays equality-only (`= / != / OR / AND` flat — same
restriction the existing code applies to non-CTE columns at
`ndbrequire(atom->op == T_EQUALS || atom->op == T_NOT_EQUALS)`).

Also add MTR coverage for the **already-working** shapes that have
no test today:

- CASE in main aggregate over a CTE-leaf agg-feed (THEN arm =
  CTE virt col).
- CASE in main aggregate where the condition references a parent
  *real* table column and THEN/ELSE references a CTE virt column.

Out of scope:

- Inequality CASE conditions (`WHEN sums.t > 100`) — currently
  `ndbrequire`-rejected for non-CTE too; would need a new dispatch.
- Register-based CASE conditions (`WHEN sums.gross > sums.net`) —
  exercised by `testCteNdbApi` Test 21 via `BranchRegGe + Mov`, but
  RonSQL's CASE doesn't emit that pattern even for non-CTE.  Defers
  to a future phase paired with the Test 21 SQL surface (which also
  needs I.5 GREATEST and I.11 cross-join).
- CASE with cross-CTE columns (`WHEN cte_a.x = cte_b.y`) — same
  cross-table limitation as Phase I.3 column-vs-column.

## Code change

`storage/ndb/src/ronsql/RonSQLPreparer.cpp`:

1. **Signature change** for `generate_embedded_condition`:
   ```cpp
   void generate_embedded_condition(NdbAggregator* aggregator,
                                    QueryScope& scope,
                                    ConditionalExpression* ce,
                                    Uint32 then_arm_raw_size,
                                    NdbDictionary::Table* const* cteVirtualTables);
   ```
   Replaces the implicit `m_main_scope` usage with an explicit
   `scope` parameter (currently the function reads `m_main_scope`
   even when called from a CTE-body program — stale-context bug
   waiting to surface).  `cteVirtualTables` is `nullptr` when
   called from the non-join `programAggregator` path (no CTE
   possible there).

2. **Body change**: where the function reads
   `scope.column_map[col_side->col_idx]` (twice — the size-precompute
   loop and the per-atom emit loop), check whether
   `scope.column_table_idx[col_idx]` points at a CTE op.  If yes,
   resolve the virt-column descriptor from
   `cteVirtualTables[cte_op_idx]->getColumn(cte_col_idx)` (where
   `cte_col_idx = scope.column_attrId_map[col_idx]`).  Identical
   pattern to `programAggregator_join`'s Load handler at
   line 7855-7867.

3. **Caller updates**:
   - `programAggregator` (line 7430): pass `&m_main_scope`,
     `nullptr` for cteVirtualTables.
   - `programAggregator_join` (line 7949): pass `&scope` and the
     existing `cteVirtualTables` parameter.

4. **Header update**: `RonSQLPreparer.hpp` — adjust
   `generate_embedded_condition` declaration accordingly.

The READ_LINKED_TO_MEM emit at line 8069-8072 already handles the
linked-projection lookup correctly because it uses
`find_or_add_linked_proj` which is type-agnostic.  The only NULL
crash sites are the two `column_map` reads — that's the entire
fix.

## Test plan

New file `mysql-test/suite/ronsql/t/ronsql_cte_case.test`, fixture
identical to `ronsql_cte_basic` (cte_orders / cte_customer).

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `SUM(CASE WHEN c.c_region = 1 THEN sums.t ELSE 0 END)` | Parent-table condition + CTE THEN arm — works today, baseline |
| 2 | `SUM(CASE WHEN sums.k = 100 THEN sums.t ELSE 0 END)` | CTE-column condition (the I.4 fix) |
| 3 | `SUM(CASE WHEN sums.k = 100 OR sums.k = 200 THEN sums.t ELSE 0 END)` | Multi-atom OR over CTE column |
| 4 | `SUM(CASE WHEN sums.k != 100 AND sums.k != 200 THEN sums.t ELSE 0 END)` | Multi-atom AND over CTE column |
| 5 | regression: `SUM(CASE WHEN c.c_region = 1 THEN o_amt ELSE 0 END)` (non-CTE) | Existing path unchanged |

All five tests use `ronsql_compare.inc`.  No rejection tests for I.4
specifically — the structural rejections (CASE with `>` etc.) are
pre-existing limits, not changed by this phase.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — signature + body of
  `generate_embedded_condition`; two caller updates.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp` — declaration.
- `mysql-test/suite/ronsql/t/ronsql_cte_case.test` — new file.
- `mysql-test/suite/ronsql/r/ronsql_cte_case.result` — recorded.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_i.md`
  — flip I.4 to "shipped".

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_case
./mtr --suite=ronsql                       # full suite — no regressions
./mtr --suite=ndb_push_agg                 # block tests — no regressions
```

## Commit cadence

Single commit:

| Commit | Contents | Approx LoC |
|---|---|---|
| 1 | generate_embedded_condition signature + CTE virt-col resolve + 2 caller updates + MTR tests + status flip | ~120-180 |

## Risks

1. **Stale `m_main_scope` reads.**  The current function reads
   `m_main_scope` directly, which is the *outer* scope — wrong when
   called from a CTE-body's aggregator program (chained CTE).  The
   plan replaces this with an explicit `scope` parameter; a follow-on
   benefit beyond the I.4 surface but the same single edit.  Verify
   no other path mutates `m_main_scope` between the join-emit start
   and `generate_embedded_condition` call.
2. **Linked-projection slot reuse.**  `find_or_add_linked_proj` is
   already idempotent — calling it for the same (op, col) in CASE
   condition vs THEN-arm load returns the same `lp_pos`.  No
   double-emit risk.
3. **Constant-side type lookup.**  The CASE condition compares
   against a constant whose byte-length must match the column
   descriptor.  For a CTE virt column resolved via
   `cteVirtualTables[op]->getColumn(idx)`, `byte_len = vt_col->getLength()`
   matches what the buffer entry's data-section actually holds (the
   AttrHeader-prefixed data — same path as the WHERE filter
   inline-opcode emit).
4. **Single-table EmbeddedInterp regression.**  The non-join path
   (`programAggregator`) doesn't carry `cteVirtualTables`; we pass
   `nullptr`, and `column_map[col_idx]` is non-NULL for real-table
   columns there.  No behaviour change.

## What we're not doing

- Inequality conditions in CASE (RonSQL `ndbrequire`-rejects today
  even for non-CTE).
- Register-based CASE conditions (Test 21 shape — needs RonSQL
  parser/codegen for `WHEN reg-expr OP reg-expr` plus
  `BranchReg* + Mov` emit).
- The `GREATEST` / `LEAST` SQL surface (Phase I.5).
