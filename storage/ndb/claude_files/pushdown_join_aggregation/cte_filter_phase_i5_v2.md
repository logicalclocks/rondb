# Phase I.5 v2 — Register-based CASE codegen

## Status

**v2a shipped.**  Column-vs-column atoms in embedded CASE conditions
land via the `READ_*-into-register` + `BRANCH_*_REG_REG` family,
unlocking distinct-column `GREATEST` / `LEAST` and multi-column
`CASE WHEN col_a > col_b …` shapes (Bigint-only on each side).
Per-atom self-contained loads also fix a latent v1 bug where atoms
with different LHS columns aliased on `heap[0]`.

**v2b shipped** — n-ary GREATEST/LEAST via the SVM extension; the
dedicated plan + what-shipped is in `cte_filter_phase_i5_v2b.md`.
The sketch later in this doc is preserved for design history but
the v2b plan superseded it (in particular, v2b models pair-max /
pair-min as a single value-producing SVM instruction shaped like an
arithmetic op rather than introducing six new `BranchReg*` SVM types
and a third operand on `Instr`).  v2b also replaced v1's two-arg
CaseExpr-based lowering — n=2 now flows through the same SVM path
as n>2, and the v1-specific `m_greatest_least_conditions` /
`is_greatest_least_condition` machinery was removed.

The CTE linked-vs-linked runtime test that was intentionally removed
from v2a's MTR file is captured separately in
`cte_filter_phase_i5_v6.md`.  Sub-Bigint integer support for
linked-column register loads is captured in
`cte_filter_phase_i5_v5.md`.

## Goals

Three RonSQL-side capabilities, all unlocked by the same kernel
register-branch machinery (`BRANCH_*_REG_REG` + `READ_ATTR_INTO_REG`
/ `READ_LINKED_TO_MEM` + `Mov`):

1. **GREATEST / LEAST over two distinct columns** — e.g.
   `GREATEST(t.a, t.b)`, today rejected by `lower_greatest_least`'s
   "same-column" guard.
2. **Multi-column CASE conditions** in aggregation — e.g.
   `SUM(CASE WHEN sums.gross > sums.net THEN sums.gross ELSE 0 END)`.
   Today rejected because `generate_embedded_condition` emits
   column-vs-constant atoms only.  This is the deferred
   "register-based CASE conditions" item from `cte_filter_phase_i4.md`
   "Cleanly-rejected shapes" and the `testCteNdbApi` Test 21 shape.
3. **GREATEST / LEAST with three or more arguments** — e.g.
   `GREATEST(a, b, c, d)`.  Today rejected by v1's grammar.

(1) and (2) share the same code path.  (3) needs SVM-level
extensions to the AggregationAPICompiler.  Scope split:

- **v2a** = (1) + (2) — extends the embedded-CASE atom emit path.
- **v2b** = (3) — adds new SVM instructions and an n-ary GREATEST/
  LEAST `Expr` op.

Each sub-phase ships as its own commit.  v2a is independently
useful and lower-risk; v2b builds on it.

NULL propagation for nullable column operands is **out of scope**
for v2 — that's I.5 v4 (`cte_filter_phase_i5.md` §"I.5 v4").

## Why this is achievable now

Investigation confirmed that the kernel-side machinery is fully
present and already validated:

- **DBTUP**: `s_cte_filter_handlers`
  (`storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp:8844-8862`)
  dispatches `BRANCH_REG_EQ_NULL`, `BRANCH_REG_NE_NULL`,
  `BRANCH_EQ_REG_REG` … `BRANCH_GE_REG_REG`.  The aggregation-
  program dispatch table `s_agg_interp_handlers` (line ~8997)
  also covers `READ_ATTR_INTO_REG`, `LOAD_CONST*`, and the same
  `BRANCH_*_REG_REG` family.
- **JoinAggInterpreter**: `validateEmbeddedProgram`
  (`JoinAggInterpreter.cpp:540-631`) already whitelists
  `READ_ATTR_INTO_REG`, `LOAD_CONST_NULL/16/32/64`, all
  `BRANCH_REG_*_NULL` and `BRANCH_*_REG_REG` opcodes, plus the
  `BRANCH_MEM_OP_ARG*` family.  No whitelist surgery needed.
- **NdbAggregator API**: `LoadLinkedColumn`, `LoadColumn`,
  `LoadInt64`, `LoadUint64`, `BranchRegGe`/`Le`/`Lt`/`Gt`/`Eq`/`Ne`,
  `Mov` are all public (`NdbAggregator.hpp:273-311`).  Test 21 in
  `testCteNdbApi.cpp:5693-5711` uses exactly these.
- **NdbInterpretedCode API**: `read_attr`, `read_int64_to_reg_const`,
  `read_int64_to_reg_reg`, `move_reg`, `branch_eq/ne/lt/le/gt/ge`
  (reg-vs-reg), `branch_*_const` are public
  (`NdbInterpretedCode.hpp:441-792`).

So v2 is a pure RonSQL-side codegen extension.  No kernel changes
needed.

## v2a — Column-vs-column embedded-CASE atoms (shipped)

### What shipped

**Kernel** (`storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp`):

- `validateEmbeddedProgram` whitelist gains
  `Interpreter::READ_INT64_MEM_TO_REG`.  This was the only kernel
  change required — DBTUP's `interpreterNextLab` switch and the
  `s_cte_filter_handlers` / `s_agg_interp_handlers` dispatch tables
  already covered the register-based opcode family.

**RonSQL preparer** (`storage/ndb/src/ronsql/RonSQLPreparer.cpp`):

- `generate_embedded_condition` rewritten to per-atom
  self-contained loads.  Each linked-column atom emits its own
  `READ_LINKED_TO_MEM` rather than relying on a shared preamble
  word, which removes the prior implicit assumption that all atoms
  in a CASE share the same LHS column (latent v1 bug fix).
- New col-vs-col emit shape: `READ_*-into-register` for both sides
  + `BRANCH_*_REG_REG`.  Static R1 / R2 register usage; safe across
  atoms because the embedded interpreter resets its register file
  on each `kOpEmbeddedInterp` invocation.
- Per-atom resolution factored into `resolve_col_side` (also
  promoted to `std::function` rather than `auto` lambdas to keep
  the function readable from a cold review).
- Per-atom shape captured in `AtomInfo { lhs, rhs, is_col_vs_col,
  word_count }` and reused verbatim in the emit pass.
- Branch instruction position computed per atom shape: last word
  for col-vs-col; `pos+1` for linked col-vs-const (after
  `READ_LINKED_TO_MEM`); `pos` for leaf-table col-vs-const.
- Two separate cond-direction maps:
  - `cond_for_arg_family` keeps the existing `_OP_ARG`
    inverted-inequality mapping.
  - `reg_branch_opcode` uses direct mapping (the `BRANCH_*_REG_REG`
    family does *not* have the inverted-inequality quirk — verified
    in `DbtupExecQuery.cpp:6000-6069`).
- `lower_greatest_least` lifts the same-column guard.  Distinct
  columns produce a col-vs-col CASE with both sides as
  `T_IDENTIFIER` cond_expr nodes.  Same-column `GREATEST(x, x)` /
  `LEAST(x, x)` still collapses to `x` directly.

**Restrictions kept in v2a:**

- Bigint-only on each side — sub-Bigint integer linked columns
  defer to v5 (`cte_filter_phase_i5_v5.md`) which adds typed
  signed/unsigned `READ_*_MEM_TO_REG` opcodes.
- `InlineLinked` (CTE-leaf inline-typed columns) on either side of
  a col-vs-col atom rejected — would need an inline-typed
  register-load opcode; defer with v5.
- Nullable column operands still rejected via the v1
  `is_greatest_least_condition` post-resolution check (extended to
  cover both LHS and RHS in v2a).
- CTE aggregate outputs in CASE conditions remain rejected (same
  defer as I.4).

**Test coverage**:
`mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v2a.test` —
seven cases covering distinct-column GREATEST/LEAST, multi-column
CASE conditions (`>`, `<=`, `=`), same-column regression, and v1
col-vs-const regression.  The CTE linked-vs-linked runtime case was
intentionally moved to v6 (`cte_filter_phase_i5_v6.md`) because
RonSQL's pass-through CTE constraint plus the
CTE-aggregate-output-in-CASE rejection made the natural shape
unreachable; v6 either lifts those constraints or finds an
alternative coverage path.

### Original v2a design (pre-implementation)

### Surface

Lift the column-vs-constant restriction in the embedded-CASE atom
emit path, enabling:

```
SUM(CASE WHEN sums.gross > sums.net THEN sums.gross ELSE 0 END)
SUM(CASE WHEN c.a >= c.b OR c.a < c.c THEN c.a ELSE 0 END)
SUM(GREATEST(c.a, c.b))   -- via the existing v1 lowering
```

with all six comparison ops (`=`, `!=`, `<`, `<=`, `>`, `>=`).

### Where the change lives

`storage/ndb/src/ronsql/RonSQLPreparer.cpp`, function
`generate_embedded_condition` (line ~8049).  Today each atom is
either `BRANCH_ATTR_OP_ARG`, `BRANCH_MEM_OP_ARG`, or
`BRANCH_MEM_OP_ARG_INLINE_TYPE` (column-vs-constant).  v2a adds a
fourth branch shape: register-vs-register, used when
`atom->args.right->op == T_IDENTIFIER`.

### Per-atom emission for col-vs-col

Pseudocode for one atom `lhs CMP rhs` where both sides are columns:

```
// Pick two registers; v2a allocates R1, R2 statically (the embedded
// CASE block has no other register users).  REGS = 8.
R1 := first free reg
R2 := first free reg above R1

// Load lhs:
//  - leaf-table column: READ_ATTR_INTO_REG(R1, attr_id_lhs)
//  - linked column:     READ_LINKED_TO_MEM(linked_pos_lhs)
//                       READ_<TYPE>_MEM_TO_REG_CONST(R1, /*offset=*/4)
// Load rhs symmetrically into R2.

// Branch:
//   For OR:  BRANCH_*_REG_REG(R1, R2, second_exit) on direct cmp
//   For AND: BRANCH_*_REG_REG(R1, R2, second_exit) on inverted cmp
// (The kernel BRANCH_*_REG_REG opcodes are *not* inverted like the
//  embedded BRANCH_MEM_OP_ARG family — verify by reading
//  DbtupExecQuery.cpp handleBranchGtRegReg etc. and update the
//  direct/invert mappings accordingly.  CLAUDE.md "NdbInterpretedCode:
//  Inverted Inequality Branches" notes the inversion only for the
//  branch_col_*/branch_attr family — the reg-vs-reg form is direct.)
```

Linked columns must use the memory-to-register opcode matching the
source column type.  `READ_LINKED_TO_MEM` stages an
`AttrHeader + data` blob at heap offset 0; the data starts at byte
offset 4.  For v2a, support the integer family by choosing:

| NDB column type | Embedded load from linked heap |
|-----------------|--------------------------------|
| `Tinyint`, `Tinyunsigned` | 1-byte heap load, sign- or zero-extended to register |
| `Smallint`, `Smallunsigned` | 2-byte heap load, sign- or zero-extended to register |
| `Int`, `Unsigned` | 4-byte heap load, sign- or zero-extended to register |
| `Bigint`, `Bigunsigned` | 8-byte heap load into register |

All loads arrive in interpreter registers as the VM's integer
register representation and are compared as BIGINT-like values.
During implementation, read `Interpreter.hpp` and
`DbtupExecQuery.cpp` to confirm the signed/unsigned variants that
exist.  If only unsigned heap-load helpers exist for sub-64-bit
types, add the missing signed-extension path in the embedded
interpreter or an equivalent RonSQL-side emit sequence.  `INT` and
the other integer widths are in scope for v2a; do not silently
zero-extend negative signed values.

Leaf-table columns can continue to use `READ_ATTR_INTO_REG`, which
already performs the column-type-aware conversion into a register.

### Word-size accounting

Each col-vs-col atom emits:

- 1 program word per side for a leaf column (`READ_ATTR_INTO_REG`).
- 2 program words per side for a linked column
  (`READ_LINKED_TO_MEM` + typed `READ_*_MEM_TO_REG_CONST`).
- 1 program word for `BRANCH_*_REG_REG`.

Total: 3, 4, or 5 program words per col-vs-col atom depending on
whether neither, one, or both operands are linked columns.

The `total_branch_words` accumulator in
`generate_embedded_condition` (line ~8128) extends with a
col-vs-col branch.  The READ_LINKED_TO_MEM ledger
(`has_linked_col`, `linked_position` at line ~8085) extends to
track multiple linked positions, since each column operand may
require its own linked-buffer slot.

### Mixed atoms in one CASE

A CASE condition with a mix of col-vs-const and col-vs-col atoms
under AND/OR (e.g.
`WHEN c.a > 5 AND c.b < c.c`) should work — emit each atom using
the appropriate shape, sharing `R1`/`R2` if both atoms are col-vs-
col (each atom re-loads its operands).  No SVM-level state shared
between atoms; the embedded interpreter resets reg state each
invocation.

### Lift the same-column guard in `lower_greatest_least`

```cpp
if (a_is_col && b_is_col)
{
  require_prm(a->getLoadIdx() == b->getLoadIdx(),
              "GREATEST/LEAST with two distinct column operands is "
              "not yet supported (deferred to I.5 v2 — needs "
              "register-based CASE codegen).");
  return a;
}
```

becomes

```cpp
if (a_is_col && b_is_col && a->getLoadIdx() == b->getLoadIdx())
{
  return a;  // GREATEST(x, x) ≡ x
}
// Distinct columns: lowers to a CaseExpr with a col-vs-col cond.
// generate_embedded_condition handles the col-vs-col atom via the
// new register-based emit path.
```

The cond construction (line ~9089-9116 in v1) needs to allow
`rhs` to be a `T_IDENTIFIER` rather than always `T_INT`:

```cpp
if (b_is_col) {
  rhs->op = T_IDENTIFIER;
  rhs->col_idx = b->getLoadIdx();
} else {
  rhs->op = T_INT;
  rhs->constant_integer = ...;
}
```

The "column on LHS" normalisation in v1 still applies; for
col-vs-col both sides are columns, no swap needed.

The current v1 helper uses `condition_col_expr` /
`condition_const_expr` and asserts the remaining non-same-column
case is `const, col`.  v2a must rewrite that shape handling, not
just patch `rhs` construction:

- `col, const`: keep v1's normalized column-vs-constant condition.
- `const, col`: keep v1's swapped condition and adjusted comparison
  operator.
- `col, col same idx`: fold to the column expression.
- `col, col distinct idx`: build a `T_IDENTIFIER CMP T_IDENTIFIER`
  condition with the original operand order and the normal
  `GREATEST`/`LEAST` comparison (`>=` or `<=`).

### Nullability check (carry-over from v1)

`is_greatest_least_condition(ce)` still gates nullability.  v2a
extends the check to **both** column operands when both are
columns; either nullable rejects.

### Test plan

Extend `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least.test`
or add a new file `ronsql_cte_greatest_least_v2.test`.  Cases:

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `SUM(GREATEST(c.c_region, o.o_amt))` | distinct-column GREATEST |
| 2 | `SUM(LEAST(c.c_region, o.o_amt))` | distinct-column LEAST |
| 3 | `SUM(CASE WHEN c.c_region > o.o_amt THEN o.o_amt ELSE 0 END)` | multi-column CASE > |
| 4 | `SUM(CASE WHEN c.c_region <= o.o_amt THEN o.o_amt ELSE 0 END)` | multi-column CASE <= |
| 5 | `SUM(CASE WHEN c.a = c.b THEN ... END)` | multi-column CASE = |
| 6 | `WHEN c.a > o.x AND c.b < o.y` (both col-vs-col, AND) | mixed AND of col-vs-col |
| 7 | `WHEN c.a > 5 AND c.b < o.y` (col-vs-const + col-vs-col) | mixed atom shapes |
| 8 | `SUM(GREATEST(sums.k, sums.k))` (degenerate same-col) | regression: same-col still works |
| 9 | regression: rerun a v1 col-vs-const case | byte-equivalent emit |

Tests 1-7 use `ronsql_compare.inc`.  Test 8 confirms the
degenerate path was preserved.  Test 9 is sanity.

### Deliverables (v2a)

- `RonSQLPreparer.cpp` — col-vs-col atom emit in
  `generate_embedded_condition`; lift same-column guard in
  `lower_greatest_least`.
- New MTR file or extension to existing.
- This doc flips v2a to "shipped".

## v2b — N-ary GREATEST / LEAST

### Surface

```
SUM(GREATEST(a, b, c))
SUM(LEAST(a, b, c, d))
SUM(GREATEST(c.x, 100, c.y, 200))
```

Arbitrary number of operands, mix of columns and constants, all
Bigint.

### Why a single CaseExpr lowering doesn't extend

`GREATEST(a, b, c)` would require nesting `CaseExpr` as a
comparison operand:

```
CASE WHEN <pair-max(a,b)> >= c THEN <pair-max(a,b)> ELSE c END
```

`<pair-max(a,b)>` is itself a CaseExpr, but the
`generate_embedded_condition` atom emit path expects a plain
column or constant on each side — even after v2a.  Extending the
embedded path to evaluate arbitrary CaseExpr trees inside an
embedded program is meaningfully bigger than v2a.

The cleaner alternative: emit n-ary GREATEST/LEAST as a
**register-based aggregation-program chain**, identical in shape to
`testCteNdbApi.cpp` Test 21.  This bypasses CaseExpr entirely and
uses the existing kOpLoadCol / kOpBranchReg* / kOpMov agg-program
opcodes.

### SVM extension

`storage/ndb/src/ronsql/AggregationAPICompiler.{hpp,cpp}`:

1. **Add `ExprOp::Greatest` and `ExprOp::Least`**
   (`AggregationAPICompiler.hpp:65-71`).  Each carries a *list* of
   operand `Expr*`s (today `Expr` has only `left`/`right`).

   Two design options:
   - **Option A**: extend `Expr` with `Expr** operands; Uint32
     num_operands;` — more flexible but a bigger struct change.
   - **Option B**: lower n-ary GREATEST/LEAST to a left-associative
     binary chain at the parser level: `Greatest2(Greatest2(a,b), c)`.
     Each `Greatest2` is a binary `Expr` with `left`/`right`.

   Option B keeps the `Expr` struct unchanged.  The compile-time
   register pressure is the same — the binary tree compiles into
   the same `Load+BranchReg+Mov` chain.  Choose B unless a strong
   reason to prefer A surfaces during implementation.

2. **Add new SVMInstrTypes** for register-based comparisons:
   ```
   BranchRegGe, BranchRegLe, BranchRegLt, BranchRegGt,
   BranchRegEq, BranchRegNe
   ```
   in `FORALL_INSTRUCTIONS(X)` or as separate enum entries (the
   former plays nicely with existing instr-emit machinery; the
   latter doesn't pollute the instruction macro fan-out).

3. **Extend `Instr` struct** to carry a third operand if needed:
   today `Instr {SVMInstrType type; Uint32 dest; Uint32 src;}`.
   A `BranchRegGe` instruction needs three values: `reg_a`,
   `reg_b`, `skip_count`.  Pack as `dest=reg_a`, `src=reg_b`,
   add a `Uint32 extra` field for `skip_count`.  Most existing
   instrs leave `extra=0`.  Footprint hit: 4 bytes per Instr.

4. **Implement `compile(Expr*, Uint32* reg)` for `Greatest`/`Least`**
   in `AggregationAPICompiler.cpp` (around line ~700, where binary
   ops are compiled today).  Steps:
   ```
   compile(left, &left_reg)        // left_reg holds left's value
   compile(right, &right_reg)      // right_reg holds right's value
   Allocate result register.  Reuse left_reg if left's last use.
   Emit: BranchRegGe(left_reg, right_reg, skip=1)   // for GREATEST
   Emit: Mov(left_reg, right_reg)
   *reg = left_reg
   ```
   For `Least`: emit `BranchRegLe`.  Inverted-branch quirk does
   *not* apply (verify in DbtupExecQuery.cpp handlers).

5. **`pushInstr` overloads** to write the three-operand reg-branch
   word into the agg program.  Encoding follows
   `NdbAggregator::BranchRegGe` (`NdbAggregator.cpp` —
   look for the existing producer).  Wire each new SVMInstrType
   to its kernel opcode in the `pushInstr(SVMInstrType, …)`
   dispatch switch.

6. **Update all SVM bookkeeping paths** for branch instructions:
   - `svm_execute()` must understand branch instructions as
     control-flow operations that read both operand registers but do
     not define a register value.
   - `dead_code_elimination()` must always preserve branch
     instructions and mark both operand registers live before the
     branch.  Branches are not dead even if their operands are not
     used after the branch.
   - The DCE rebuild path must preserve the third operand
     (`Instr.extra` / skip count), not call a two-operand
     `pushInstr` overload that drops it.
   - Program printers / debug dumps must display branch instructions
     and their skip count.
   - Any `svm_execute()` assertions that reconstruct symbolic
     expressions from straight-line arithmetic must account for the
     `Mov` after the branch being conditional at runtime.

### Parser changes

Lift the v1 grammar's two-arg restriction.  Add an
`arith_expr_list` non-terminal accepting 2+ operands; collect into
a small linked list (struct member to add to the `%union`):

```
arith_expr_list:
  arith_expr T_COMMA arith_expr             { mk_pair(...) }
| arith_expr_list T_COMMA arith_expr        { append(...) }

arith_expr:
  ...
| T_GREATEST T_LEFT arith_expr_list T_RIGHT
    { $$ = context->lower_greatest_least_nary($3.head, true); }
| T_LEAST T_LEFT arith_expr_list T_RIGHT
    { $$ = context->lower_greatest_least_nary($3.head, false); }
```

The two-arg rule from v1 collapses into the n-ary rule (n=2 is a
list of length 2).

`lower_greatest_least_nary` walks the operand list and builds a
left-associative `Greatest2`/`Least2` `Expr` tree.  Each leaf
operand still must be `Load` or `LoadConstantInteger` (operand-
shape restriction stays); arithmetic operands are deferred (they'd
need a separate code path — out of scope for v2b).

### Aggregator-vs-CaseExpr interaction

`Greatest`/`Least` Expr nodes appear at any position the SVM
compiler accepts an Expr (e.g., as the argument to `SUM`, `MIN`,
`MAX`).  Since the lowering produces a single result register,
`SUM(GREATEST(a, b, c))` becomes:

```
program:
  Load a → r1, Load b → r2
  BranchRegGe r1, r2, +1
  Mov r1, r2                     // r1 = max(a, b)
  Load c → r2
  BranchRegGe r1, r2, +1
  Mov r1, r2                     // r1 = max(a, b, c)
  Sum agg_index, r1
```

No CaseExpr involvement; no embedded interpreter.  Pure
agg-program execution.

### Coexistence with v1's CaseExpr lowering

v2b's parser rule replaces v1's two-arg rule.  v1's lowering helper
(`Context::lower_greatest_least`) also collapses into the new n-ary
helper.  The v1 nullable-column rejection must be reimplemented for
the new `Greatest`/`Least` Expr path, since the current v1 guard is
attached to the CaseExpr condition and `generate_embedded_condition`
will no longer run for v2b.

Before compiling a `Greatest`/`Least` Expr, validate every column
operand after normal RonSQL column resolution:

- physical leaf columns via `scope.column_map[col_idx]`;
- linked parent columns via the same descriptor lookup used by
  `LoadLinkedColumn`;
- CTE virtual columns via the virtual-table / CTE-body resolution
  helpers from the v1 CASE-condition path.

Reject if any operand column is nullable with the existing
`"GREATEST/LEAST on nullable column operands is not yet supported"`
message.  This remains a v2 restriction; MySQL-correct NULL
propagation is still v4.

For `n == 2`, the natural emit is two `Load`s + one `BranchReg` +
one `Mov` — strictly fewer program words than v1's CaseExpr-based
emit.  This means **v2b changes the byte-level emit for v1
cases**.  Decision point: is the change byte-equivalent enough that
existing recorded results can stay?  Most likely **no** — recorded
results are query-output-equivalent (same numeric answers) but the
internal program differs.  Recorded results stay valid (output
unchanged), only internal traces would differ.

### Test plan

Extend or add a new file (likely
`ronsql_cte_greatest_least_v2b.test`).  Cases:

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `SUM(GREATEST(a, b, c))` | n=3, all columns |
| 2 | `SUM(GREATEST(a, b, c, d))` | n=4 |
| 3 | `SUM(GREATEST(col, 100, 200))` | mixed col + const |
| 4 | `SUM(LEAST(a, b, c))` | LEAST n=3 |
| 5 | `SUM(GREATEST(a, GREATEST(b, c)))` | nested GREATEST (parser exposure) |
| 6 | regression: v1 two-arg case still produces correct value | output-equivalent |
| 7 | regression: v2a col-vs-col CASE in mixed expr still works | composition |

Tests 1-5 use `ronsql_compare.inc`.  6-7 are sanity.

### Deliverables (v2b)

- `AggregationAPICompiler.{hpp,cpp}` — new ExprOps, new
  SVMInstrTypes, three-operand `Instr.extra`, compile() + pushInstr
  cases.
- `RonSQLPreparer.{hpp,cpp}` — new `lower_greatest_least_nary`,
  `arith_expr_list` plumbing.
- `RonSQLParser.y` — n-ary grammar.
- New MTR test file.
- This doc flips v2b to "shipped".

## What we're not doing in v2

- **NULL propagation** for nullable column operands — that's v4.
  v2 keeps the v1 rejection in place (extended to col-vs-col
  atoms).
- **Wider operand types** (Float / Decimal / VARCHAR) — that's v3
  / I.6.  v2 stays Bigint-only.
- **Arithmetic operands inside GREATEST/LEAST** — `GREATEST(a + b,
  c * 2)`.  Out of scope; would need to thread arbitrary Expr
  trees through the new register chain.  Defer to a future v5
  if real demand surfaces.
- **CASE conditions referencing CTE aggregate output** — same
  defer as I.4.

## Risks

1. **Inverted-branch quirk on register-based opcodes.**  The
   embedded `BRANCH_*_OP_ARG` family inverts inequality
   conditions; the register family
   (`BRANCH_*_REG_REG` / `branchRegGe` etc.) does *not* per
   `NdbScanFilter.cpp:560` and matching kernel handlers.  Verify
   during v2a implementation by reading the DBTUP handlers
   (`handleBranchGeRegReg` etc.) and adjust direct/invert mappings
   if the assumption is wrong.

2. **Linked-buffer position accounting.**  When both LHS and RHS
   are linked columns from the same op, the embedded program
   needs two linked buffer slots (or two READ_LINKED_TO_MEM calls
   with different positions).  Confirm `find_or_add_linked_proj`
   handles repeat lookups idempotently and returns stable
   positions.

3. **Three-operand SVM Instr struct change (v2b).**  Adding
   `Uint32 extra` to `Instr` is a 4-byte-per-instr footprint hit
   in `m_program`.  Most programs are short (10-100 instrs), so
   the absolute hit is negligible — but cross-check that no
   existing producer relies on `sizeof(Instr) == 12`.

4. **Register pressure in long n-ary chains.**  REGS = 8.  An
   `n=8` GREATEST won't run out (each operand uses 2 regs but
   the chain reuses the result reg as one operand on each
   step), but a deeply-nested query like
   `SUM(GREATEST(a, b)) + SUM(GREATEST(c, d))` plus other agg
   expressions could.  The agg-compiler's existing
   register-pressure heuristic
   (`estimated_cost_of_recalculating`) handles this — verify it
   sees the new ExprOps via `est_regs`.

5. **Partial-failure cleanup.**  If v2b's parser changes go in
   without v2b's SVM compile() handling, queries with n>2
   GREATEST/LEAST will reach the SVM with an unrecognised op and
   abort.  Land both halves in one commit, or guard the parser
   to keep emitting v1-style CaseExpr until the SVM half is
   ready.

## Verification

Per sub-phase:

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_greatest_least_v2[a|b]
./mtr --suite=ronsql                     # full suite — no regressions
./mtr --suite=ndb_push_agg               # block tests — no regressions
```

Each sub-phase commits separately and updates this doc's status
section to flip v2a → shipped, then v2b → shipped.
