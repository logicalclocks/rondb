# Phase I.5 v3 — float / double operands in col-vs-col CASE

## Status

**v3a shipped.**  Three commits on `RONDB-1050-cte-filter`:

| Commit | Scope |
|--------|-------|
| `32b86c5052d` | Kernel — `handleReadLinkedColumnToReg` `NDB_TYPE_FLOAT` / `NDB_TYPE_DOUBLE` arms (early-return paths that tag `REG_TYPE_DOUBLE` and store the 8-byte double bit pattern; FLOAT widens to double up front) |
| `c44f0fe928b` | RonSQL — `can_load_integer_reg` renamed to `can_load_typed_reg`; `Float` and `Double` added to the whitelist; rejection message updated to reflect that only VARCHAR / DECIMAL remain deferred |
| `ee27b22dea0` + recorded result | MTR — `ronsql_cte_greatest_least_v5.test` Tests 14-17 (linked DOUBLE vs leaf DOUBLE, linked FLOAT vs leaf BIGINT, linked DOUBLE vs linked BIGINT, leaf FLOAT vs linked BIGINT).  All four `== Diff ==` blocks empty: RonSQL output matches MySQL's reference |

**v3b deferred.**  Col-vs-const float literal stays unshipped —
the RonSQL col-vs-const branch still uses `BRANCH_ATTR_OP_ARG` /
`BRANCH_MEM_OP_ARG` which lack float compare semantics, and the
Option A vs Option B trade-off (lower the literal to a register
via `LOAD_DOUBLE_CONST`, or extend the `_ARG` opcodes with float
compare) deserves a separate plan.

## Background

After Phases I.5 v2a (col-vs-col atoms in embedded CASE) and v5
(typed linked-column register loads with sign extension across the
10 NDB integer widths), the col-vs-col pipeline looks like this:

```text
LHS (leaf  )  → READ_ATTR_INTO_REG  → R1 (typed)
RHS (linked)  → READ_LINKED_COLUMN_TO_REG → R2 (typed)
BRANCH_*_REG_REG R1, R2 → branch
```

All three opcodes are now type-aware after I.18:
- `handleReadAttrIntoReg` handles every NDB integer width plus
  Float / Double / Bigint / Bigunsigned (descriptor-driven via
  `sint*korr` / `uint*korr` — see commit `bab40b92982`).
- `handleReadLinkedColumnToReg` handles every NDB **integer** width
  (10 cases), and tags `REG_TYPE_INT` / `REG_TYPE_UINT`.  Float
  and Double currently fall through to `default → -ZNO_INSTRUCTION_ERROR`
  (`DbtupExecQuery.cpp:7659`).
- `BRANCH_*_REG_REG` family accepts mixed int / float operands
  via `compareTypedRegs` (I.18 batch 1, commit `ed93f65716a`):
  if either operand is float, both promote to double for the
  comparison.

So the kernel can already do float-vs-float, float-vs-int, and
int-vs-float compares correctly **once both sides are loaded as
typed registers**.  The remaining work splits into two tiny
pieces:

1. **Kernel** — extend `handleReadLinkedColumnToReg` with two
   new cases: `NDB_TYPE_FLOAT` (read 4 bytes, cast to double,
   mark `REG_TYPE_DOUBLE`) and `NDB_TYPE_DOUBLE` (memcpy 8 bytes,
   mark `REG_TYPE_DOUBLE`).
2. **RonSQL** — relax `can_load_integer_reg` to also accept
   `Float` and `Double` columns.  Rename to
   `can_load_typed_reg` for clarity.  Update the rejection
   message to exclude float from the deferred list.

That's the whole feature.  No new opcodes; no new validators;
no aggregator-program changes.

## Scope split

### v3a (this phase) — col-vs-col over Float / Double

Lands the two pieces above plus an MTR test extending
`ronsql_cte_greatest_least_v5.test` with float-leaf and float-linked
coverage.

Out of scope for v3a:
- col-vs-const with float literal — needs an emit path that loads
  the constant via `LOAD_DOUBLE_CONST` and compares with
  `BRANCH_*_REG_REG` (today col-vs-const uses `BRANCH_ATTR_OP_ARG`
  / `BRANCH_MEM_OP_ARG` which compare against the column's native
  representation — these don't have float comparison support).
  Defer to v3b.
- VARCHAR / DECIMAL — defer to I.6.

### v3b (separate phase, deferred) — col-vs-const float literal

Picks up where v3a leaves off: a float column compared against a
numeric literal in CASE.  Two emit options:
- **Option A (preferred)** — convert col-vs-const-float to
  col-vs-col-float by emitting `LOAD_DOUBLE_CONST` for the literal
  into a register, then reusing v3a's path.  Lands on top of
  v3a's plumbing.
- **Option B** — extend `BRANCH_ATTR_OP_ARG` / `BRANCH_MEM_OP_ARG`
  with float-aware compare.  More kernel work, less RonSQL work.

v3b stays a separate plan because Option A vs B is a real
trade-off and the test surface is different (literal parsing,
NaN handling, denormals).

## v3a code changes

### Kernel

`storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp`,
`handleReadLinkedColumnToReg` (around line 7623, after the
existing `NDB_TYPE_BIGUNSIGNED` arm):

```cpp
case NDB_TYPE_FLOAT: {
  float fval;
  memcpy(&fval, data, 4);
  double dval = static_cast<double>(fval);
  ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
  memcpy(ctx.TregMemBuffer + ctx.theRegister + 2, &dval, 8);
  return INTERP_CONTINUE;
}
case NDB_TYPE_DOUBLE: {
  ctx.TregMemBuffer[ctx.theRegister] = Interpreter::REG_TYPE_DOUBLE;
  memcpy(ctx.TregMemBuffer + ctx.theRegister + 2, data, 8);
  return INTERP_CONTINUE;
}
```

The early-return shape is necessary because the existing flow
through the bottom of the function (after the integer switch)
writes the integer slot conditionally on `is_unsigned`.  Adding
two early returns keeps the integer path untouched.

### RonSQL

`storage/ndb/src/ronsql/RonSQLPreparer.cpp:8467-8495` — extend
the lambda + rename for clarity:

```cpp
std::function<bool(const NdbDictionary::Column*)> can_load_typed_reg =
    [](const NdbDictionary::Column* c)
{
  switch (c->getType()) {
  case NdbDictionary::Column::Tinyint:
  case NdbDictionary::Column::Tinyunsigned:
  case NdbDictionary::Column::Smallint:
  case NdbDictionary::Column::Smallunsigned:
  case NdbDictionary::Column::Mediumint:
  case NdbDictionary::Column::Mediumunsigned:
  case NdbDictionary::Column::Int:
  case NdbDictionary::Column::Unsigned:
  case NdbDictionary::Column::Bigint:
  case NdbDictionary::Column::Bigunsigned:
  case NdbDictionary::Column::Float:
  case NdbDictionary::Column::Double:
    return true;
  default:
    return false;
  }
};
require_prm(can_load_typed_reg(info.lhs.col) &&
            can_load_typed_reg(info.rhs.col),
            "Column-vs-column CASE / GREATEST / LEAST currently "
            "supports integer + float / double types only "
            "(VARCHAR / DECIMAL deferred to I.6).");
```

Other call sites of `can_load_integer_reg` (if any — grep first)
keep the integer-only check by inlining the integer-only switch
locally or by adding a sibling `can_load_integer_only_reg`.

### MTR

Extend `ronsql_cte_greatest_least_v5.test` (the natural home —
already covers v2a / v5 / I.18 leaf):

- Add a Float and a Double column to v5_customer (linked side):
  `c_float FLOAT NOT NULL`, `c_double DOUBLE NOT NULL`.
- Add a Float and a Double column to v5_orders (leaf side):
  `o_float FLOAT NOT NULL`, `o_double DOUBLE NOT NULL`.
- Insert representative values: positive, negative, fractional,
  edge values (very large + very small in magnitude, but skip
  NaN / Inf for v3a — defer to v3b's NaN-handling discussion).
- New tests:

| Test | Shape | Notes |
|------|-------|-------|
| 14 | linked DOUBLE vs leaf DOUBLE col-vs-col | basic float compare both sides |
| 15 | linked FLOAT vs leaf BIGINT | mixed int/float, kernel promotes to double |
| 16 | linked DOUBLE > c_threshold (BIGINT) — leaf-on-right, mixed | mirror of 15 |
| 17 | leaf FLOAT < c_threshold (BIGINT) | leaf-side float read via READ_ATTR_INTO_REG |

Confirms RonSQL output matches MySQL's reference for all four.

## Risks

1. **Float compare semantics** — `compareTypedRegs` falls back to
   `double` comparison.  IEEE-754 NaN comparisons should follow
   the kernel's existing semantics (NaN unordered).  v3a fixture
   deliberately skips NaN; document that v3b will need to decide
   whether to track MySQL's NaN handling.
2. **`can_load_integer_reg` callers** — if any other call site
   relies on the integer-only meaning (e.g. an arithmetic atom
   that doesn't go through the typed compare path), those need
   to keep the integer-only check.  Grep before renaming.
3. **No regression on v5 / I.18 tests** — Tests 1-13 must keep
   passing.  The MTR re-record will reveal any drift.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli ndbmtd
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_greatest_least_v5
./mtr --suite=ronsql                  # full ronsql suite — no regressions
./mtr --suite=ndb_push_agg            # block tests — no regressions
```

Each batch ships as one commit:
1. Kernel `handleReadLinkedColumnToReg` Float / Double arms.
2. RonSQL `can_load_typed_reg` rename + extension.
3. MTR Tests 14-17 + recorded result.

Three commits total, plus the plan-doc commit and a doc-update
commit if `cte_filter_phase_i.md` / `CLAUDE.md` need touching
(the RonSQL feature gap catalogue mentions float as deferred).
