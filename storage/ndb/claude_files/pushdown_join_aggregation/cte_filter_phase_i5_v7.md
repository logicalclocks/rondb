# Phase I.5 v7 — expression-local NULL propagation for GREATEST / LEAST

**Planned.**  This phase fixes the Phase I.5 v4 NULL propagation model
so a NULL operand in one `GREATEST` / `LEAST` expression does not stop
the entire row's aggregation program.

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

## Preferred Implementation

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
EmbeddedInterp(N) -> output 2 on NULL, output 1 to keep dest,
                     output 0 to run Mov(dest, src)
Skip(1) or equivalent control around SetRegNull
SetRegNull(dest)
Mov(dest, src)
Aggregate(...)
```

One concrete shape:

```text
EmbeddedInterp(nullable_pair_op)
  output 0: src wins
  output 1: dest wins
  output 2: expression is NULL

Skip/branch in aggregation bytecode:
  if output 2, execute SetRegNull(dest) and skip Mov
  if output 1, skip SetRegNull and Mov
  if output 0, skip SetRegNull and execute Mov
```

If the current aggregation bytecode cannot branch on the embedded
output except by using the returned skip count, encode the three exits
as skip counts over a short sequence:

```text
EmbeddedInterp(...)      returns:
  0  -> execute Mov(dest, src)
  1  -> skip Mov, keep dest
  2  -> skip Mov and execute SetRegNull(dest) via reordered sequence
```

Choose the concrete layout that keeps the fewest new opcodes while
remaining readable and verifier-friendly.

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
- nullable path includes the embedded body plus any `Skip` /
  `SetRegNull` / `Mov` words used by the chosen layout.

`raw_word_size()` and `emit_pair_op_embedded()` must consume the same
`m_pair_op_needs_null_check` decision so CASE-arm skip sizes remain
exact.

## Alternative If Register NULL Opcode Is Too Invasive

If adding `SetRegNull` is more work than expected, add an aggregation
opcode that skips only the current expression's aggregate update:

```text
kOpSkipCurrentAggUpdate
```

or emit an existing `Skip()` sequence around the one aggregate
instruction that consumes the nullable pair-op result.

This is less general than setting the expression register to NULL and
is harder for nested expressions, so prefer `SetRegNull`.

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
