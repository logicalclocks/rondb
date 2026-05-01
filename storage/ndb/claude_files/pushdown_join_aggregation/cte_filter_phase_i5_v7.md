# Phase I.5 v7 — expression-local NULL propagation for GREATEST / LEAST

**Shipped.**  This phase replaces the Phase I.5 v4 row-stop NULL
propagation model so a NULL operand in one `GREATEST` / `LEAST`
expression no longer stops the entire row's aggregation program —
unrelated `COUNT(*)` / `SUM` / `MIN` / `MAX` outputs still update on
the same row.

## What shipped

- **New aggregation opcode** `kOpSetRegNull(reg)` defined in
  `NdbAggregationCommon.hpp`.  Marks `m_registers[reg].is_null = true`
  and preserves the register's value type.  Handlers added in
  `AggInterpreter.cpp`, `JoinAggInterpreter.cpp`, and
  `PushdownInterpreter.cpp`.
- **Public NDB API surface** `NdbAggregator::SetRegNull(reg_id)` in
  `NdbAggregator.{hpp,cpp}` to emit the new opcode into an
  aggregation program.
- **Pair-op embedded program (nullable path) extended to 3 outputs:**
  - output `0` → fall through to `Mov(dest, src)`, then `Skip(1)`
    skips `SetRegNull`.
  - output `1` → land on `Skip(1)`, preserving dest and skipping
    `SetRegNull`.
  - output `2` → land on `SetRegNull(dest)`, making only the
    current expression's value NULL for this row.
  Embedded body grew from 14 to 14 words at PC layout but the third
  exit was repurposed: PC 11–13 now write `2` (instead of
  `AGG_EMBEDDED_INTERP_STOP_PROGRAM`).
- **Pair-op tail in `RonSQLPreparer::emit_pair_op_embedded`:**
  nullable path now emits `EmbeddedInterp(14) + body + Mov(dest, src)
  + Skip(1) + SetRegNull(dest)` — total 18 kernel words.
  Non-nullable fast path unchanged at 11 words.
- **`AggregationAPICompiler::raw_word_size`** updated to count 18
  for nullable pair-ops (was 16 in v4); 11 for non-nullable.
- **CASE-arm sizing regression test** added in
  `ronsql_cte_greatest_least_v4.test` (Test 15) to verify
  `raw_word_size` matches the emitted nullable pair-op body inside
  CASE arms.
- **Mixed aggregate tests** added (Tests 11–14, 16) to cover
  shapes that the v4 row-stop model produced wrong results for:
  - `COUNT(*) + SUM(GREATEST(nullable, K))`
  - `SUM(unrelated) + COUNT(GREATEST(nullable))`
  - two independent nullable pair-op aggregates
  - join `COUNT(*) + nullable linked-parent GREATEST`
  - CTE projection + join mixed aggregate.

## v4 vs v7 difference, in one line

v4: NULL operand → `AGG_EMBEDDED_INTERP_STOP_PROGRAM` →
`exec_pos = m_prog_len` → entire row dropped from every aggregate.

v7: NULL operand → `SetRegNull(dest)` → only this expression's
register becomes NULL → only the immediately-following aggregate
update skips this row.

## Problem

Phase I.5 v4 made nullable operands work for simple aggregate shapes by
emitting this pair-op embedded-program tail:

```text
BRANCH_REG_EQ_NULL(r1) -> null_exit
BRANCH_REG_EQ_NULL(r2) -> null_exit
...
null_exit:
  WRITE_INTERPRETER_OUTPUT(AGG_EMBEDDED_INTERP_STOP_PROGRAM)
```

The aggregation interpreters treat `AGG_EMBEDDED_INTERP_STOP_PROGRAM`
as:

```text
exec_pos = m_prog_len
```

That is correct for row filters, where the row should not update any
aggregate state.  It is not correct for expression-level NULL
propagation.  For example:

```sql
SELECT COUNT(*), SUM(GREATEST(nullable_col, 5))
FROM t;
```

MySQL counts every row in `COUNT(*)`, while `SUM(GREATEST(...))` skips
only rows where `GREATEST(...)` is NULL.  The v4 model can stop the
program before later aggregate instructions run, so unrelated aggregate
outputs can be undercounted or left stale.

## Design Rule

Keep two distinct embedded-interpreter outcomes:

1. Row-filter outcome:
   `AGG_EMBEDDED_INTERP_STOP_PROGRAM` remains valid only for WHERE /
   cross-table filter lowering where the whole row must be rejected.

2. Expression outcome:
   `GREATEST` / `LEAST` NULL propagation must affect only the current
   expression value or the current aggregate update.  It must not stop
   the rest of the aggregation program for the row.

## Implementation

Add expression-local NULL support to the aggregation register machine.

### New aggregation opcode

Add a small aggregation opcode that marks an aggregation register as
NULL:

```text
kOpSetRegNull(reg)
```

or an equivalent name following local naming conventions.

Semantics:

- set `m_registers[reg].is_null = true`,
- set the type to a stable integer type if the current register type is
  undefined; otherwise preserve the existing type where practical,
- do not update any aggregate result directly,
- continue with the next aggregation instruction.

This opcode is expression-local.  Aggregate operators already skip NULL
input registers for `SUM`, `MIN`, `MAX`, and `COUNT(expr)` semantics, so
the following aggregate instruction should naturally ignore the value.

### Pair-op lowering

Change `emit_pair_op_embedded()` for the nullable path from:

```text
EmbeddedInterp(14) -> STOP_PROGRAM on NULL
Mov(dest, src)
Aggregate(...)
```

to:

```text
EmbeddedInterp(14) -> output 2 on NULL, output 1 to keep dest,
                      output 0 to run Mov(dest, src)
Mov(dest, src)
Skip(1)
SetRegNull(dest)
Aggregate(...)
```

```text
EmbeddedInterp(...)      returns:
  0  -> execute Mov(dest, src), then Skip(1) over SetRegNull
  1  -> land on Skip(1), keeping dest and skipping SetRegNull
  2  -> land on SetRegNull(dest)
```

The important property is that NULL produces a NULL input register for
the current expression, not `exec_pos = m_prog_len`.

### Non-null fast path

Keep the Phase I.5 v4 fast path:

- if both operands are statically non-nullable, emit the 9-word
  compare-only body,
- no NULL branches,
- no `SetRegNull`.

### Raw word sizing

Update `AggregationAPICompiler::raw_word_size()` to account for the new
nullable pair-op expansion:

- non-null fast path remains the existing 9-word embedded body + `Mov`,
- nullable path is 18 words total:
  `EmbeddedInterp` header + 14 embedded words + `Mov` + `Skip` +
  `SetRegNull`.

`raw_word_size()` and `emit_pair_op_embedded()` must consume the same
`m_pair_op_needs_null_check` decision so CASE-arm skip sizes remain
exact.

## Tests

Add MTR coverage that would fail under v4's whole-row stop behavior.

### Single-table mixed aggregates

```sql
SELECT COUNT(*) AS rows_seen,
       SUM(GREATEST(n_a, 30)) AS g
FROM v4_nullable;
```

Expected: `COUNT(*)` includes every row, while `SUM` skips only NULL
`GREATEST` rows.

Also add:

```sql
SELECT SUM(n_id) AS id_sum,
       COUNT(GREATEST(n_a, n_b)) AS non_null_greatest
FROM v4_nullable;
```

### Multiple aggregate outputs

```sql
SELECT SUM(GREATEST(n_a, 30)) AS g,
       SUM(LEAST(n_b, 30)) AS l
FROM v4_nullable;
```

Each expression must skip independently.  A NULL in `n_a` must not
prevent the `LEAST(n_b, 30)` aggregate from updating when `n_b` is
non-NULL, and vice versa.

### Join aggregation

```sql
SELECT c.c_id,
       COUNT(*) AS rows_seen,
       SUM(GREATEST(c.c_floor, o.o_amt)) AS g
FROM v4_customer AS c
JOIN v4_orders AS o ON o.o_custkey = c.c_id
GROUP BY c.c_id;
```

For a group where `c.c_floor` is NULL, `COUNT(*)` must still count the
joined rows while `SUM(GREATEST(...))` is NULL.

### CASE arm sizing regression

Add a CASE expression where a nullable pair-op appears in one arm and a
second aggregate appears after the CASE-generated bytecode:

```sql
SELECT SUM(CASE WHEN n_id < 4 THEN GREATEST(n_a, 30) ELSE 0 END) AS g,
       COUNT(*) AS rows_seen
FROM v4_nullable;
```

This verifies `raw_word_size()` matches the emitted nullable pair-op
body inside CASE arms.

## Acceptance Criteria

- `AGG_EMBEDDED_INTERP_STOP_PROGRAM` is used only for row-filter
  lowering, not for `GREATEST` / `LEAST` expression NULL propagation.
- A NULL operand in `GREATEST` / `LEAST` affects only that expression's
  aggregate update.
- Mixed aggregate queries match MySQL:
  `COUNT(*)` and unrelated `SUM` / `MIN` / `MAX` outputs still update
  on rows where another `GREATEST` / `LEAST` expression is NULL.
- Existing Phase I.5 v4 tests still pass.
- Non-nullable v2b fast-path tests still use the shorter embedded body.
- `raw_word_size()` remains exact for CASE arms containing nullable
  pair-ops.
