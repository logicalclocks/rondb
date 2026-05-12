# Phase I.5 v2b — N-ary GREATEST / LEAST via SVM extension

## Status

**Shipped (commit `959d45a20ce`); pair-op kernel emission rewritten
in Phase M (commit `a28ed6d817f`).**  v2b's grammar accepts n-ary
GREATEST / LEAST for any n ≥ 2; lowering folds the operand list
left-associative into a chain of `Greatest2` / `Least2` SVM ops.
After Phase M, each pair-op expands at programAggregator time to a
9-word embedded normal-interpreter program followed by `Mov(dest,
src)`:

```text
EmbeddedInterp(9)
  READ_AGG_REG_TO_REG(dest → reg1)
  READ_AGG_REG_TO_REG(src  → reg2)
  BRANCH_(GE|LE)_REG_REG(reg2, reg1, +4)   // skip past "set output=0"
  LoadConst16(reg3, 0); WriteInterpreterOutput(reg3); ExitOK
  LoadConst16(reg3, 1); WriteInterpreterOutput(reg3); ExitOK
Mov(dest, src)                              // skipped iff output == 1
```

The embedded program returns 0 when `r[dest]` already holds the
max/min and 1 otherwise; the aggregation interpreter consumes that
scalar to decide whether the trailing `Mov` runs.  All comparison and
branching live in the normal interpreter — the aggregation interpreter
no longer has its own `BranchReg*` opcodes (see
`cte_filter_phase_m.md`).

v2b also **replaces** v1's two-arg CaseExpr-based lowering — n=2 now
goes through the same SVM pair-op path as n>2, simplifying the
implementation and removing the v1-specific
`m_greatest_least_conditions` / `is_greatest_least_condition`
machinery.

**What shipped:**

- **AggregationAPICompiler** (`AggregationAPICompiler.{hpp,cpp}`):
  new `FORALL_PAIR_OPS` macro covering `Greatest2` / `Least2`;
  hooked into `FORALL_INSTRUCTIONS`, the `ExprOp` enum, the
  `DEFINE_ARITH_FUNC` factory macro, the `OP_CASE` translation
  switch in `pushInstr(ExprOp, …)`, the `OPERATOR_CASE` cases in
  `svm_execute` / `dead_code_elimination` / `print(Instr*)`, and a
  `relstr_*` entry per pair-op.  Constant folding extended to
  evaluate `Greatest2` / `Least2` of two `LoadConstantInt`
  operands at program-construction time.  `print(Expr*)` adds
  GREATEST(...)/LEAST(...) cases.  `raw_word_size` returns the
  per-pair-op kernel-word count (post-Phase-M: the 9-word embedded
  program plus a `Mov`).  Public helper `owns_expr(Expr*)` lets
  RonSQLPreparer identify the owning compiler when validating
  cross-scope pair-op operand nullability.
- **Parser** (`RonSQLParser.y`): replaces v1's two-arg
  `T_GREATEST T_LEFT arith_expr T_COMMA arith_expr T_RIGHT` rule
  with `T_GREATEST T_LEFT arith_expr_list T_RIGHT` (and the LEAST
  analogue).  New `arith_expr_list` non-terminal collects 2+
  operands via `mk_arg_list` / `append_arg_list`.  New `%union`
  member `arith_expr_list`.
- **RonSQLPreparer** (`RonSQLPreparer.{hpp,cpp}`):
  `Context::lower_greatest_least_nary` validates operand shape
  (Load or LoadConstantInt; ≥1 column required), pushes column
  Loads to `m_greatest_least_pair_loads` for post-resolution
  validation, then folds the list left-associative into
  `Greatest2` / `Least2` calls.  Same-operand short-circuit at
  parse time avoids emitting `Greatest2(x, x)` pair-ops.
  `validate_greatest_least_pair_loads` runs at the start of
  `compile()` after column resolution; rejects nullable column
  operands and non-integer types (Tinyint .. Bigint signed +
  unsigned accepted; Float / Decimal / VARCHAR deferred to
  v3 / I.6).  `programAggregator` and `programAggregator_join`
  gain `Greatest2` / `Least2` cases that emit a 9-word embedded
  normal-interpreter program (`READ_AGG_REG_TO_REG ×2 +
  BRANCH_(GE|LE)_REG_REG + LoadConst16/WriteInterpreterOutput/ExitOK ×2`)
  followed by `Mov(dest, src)`; see the Status section above for the
  full shape.  The v1-specific `m_greatest_least_conditions` array,
  `is_greatest_least_condition` helper, and the two call sites in
  `generate_embedded_condition` are removed (dead code under
  v2b's lowering).
- **Tests**:
  - `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v2b.test`
    — 11 cases (n=3 GREATEST/LEAST, n=4, mixed col+const,
    interleaved, explicit nesting, v1 regression, same-column
    degenerate, CTE-scope chain, nullable rejection, pure-constant
    rejection).
  - `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least.test`
    (v1 regression file): retired Tests 8 (n>2 rejection) and 9
    (distinct-column rejection) — both shapes are now accepted by
    v2b.  Test 10 (nullable rejection) preserved; the rejection
    now flows through `validate_greatest_least_pair_loads` instead
    of the v1 CE tracker.

**Limitation resolved by v4 / v7.**  The original v2b implementation
rejected nullable column operands at compile time and skipped the
parser-time nullable check on CTE columns (because
`scope.column_map[col_idx] == NULL` until
`build_cte_virtual_tables` runs at execute time).  v4
(`cte_filter_phase_i5_v4.md`) lifted the nullable rejection.  v7
(`cte_filter_phase_i5_v7.md`) refined v4's NULL outcome from a
row-stop (`AGG_EMBEDDED_INTERP_STOP_PROGRAM`, which dropped
unrelated aggregates from the same row) to expression-local
`SetRegNull(dest)` — so the result is MySQL-correct even when the
query mixes a nullable `GREATEST` / `LEAST` with `COUNT(*)` or
unrelated `SUM` outputs.  The CTE-column skip in
`validate_greatest_least_pair_loads` remains as a "type-check skip"
only — type validation for CTE columns is still deferred until the
virt-table descriptors exist at execute time, but nullability is no
longer a parser-time concern.

**Why pair-op as a single SVM instruction.**  The earlier v2b sketch
in `cte_filter_phase_i5_v2.md` proposed six new `BranchReg*` SVM
types plus a third operand on `Instr` to carry the skip count.  This
plan refined that to a single value-producing SVM instruction per
pair-op (`Greatest2` / `Least2`) shaped exactly like an arithmetic op
— `Instr` stays at `{type, dest, src}`, the SVM treats the pair-op
as if it deterministically writes `max(r[dest], r[src])` to
`r[dest]`, and the kernel-level expansion is encapsulated inside
`programAggregator`.  This shape held up across Phase M's emission
rewrite: the SVM model didn't change at all when the underlying
emission moved from `BranchReg + Mov` to embedded interpreter +
`Mov`.  The existing arithmetic-op machinery (constant folding,
register allocation, DCE) still covers pair-ops for free.

This doc is a refinement of the v2b sketch in
`cte_filter_phase_i5_v2.md`.  The key design simplification vs. that
sketch: rather than introducing six new `BranchReg*` SVM instruction
types and a third operand field on `Instr`, model the pair-max /
pair-min as **one** value-producing SVM instruction each
(`Greatest2`, `Least2`), shaped exactly like the existing arithmetic
ops.  The fact that a pair-op expands to a `BranchRegGe + Mov` (or
`BranchRegLe + Mov`) sequence at the kernel level is encapsulated in
`programAggregator` — the SVM stays clean.

## Goal

```sql
SELECT SUM(GREATEST(a, b, c)) FROM t;
SELECT SUM(GREATEST(a, b, c, d)) FROM t;
SELECT SUM(LEAST(a, b, c, d)) FROM t;
SELECT SUM(GREATEST(t.x, 100, t.y, 200)) FROM t;
SELECT MAX(GREATEST(o.amt, c.threshold)) FROM ... -- v2a + v2b
```

Bigint-only on each operand (same operand-shape restriction as v1
and v2a).  Mix of leaf columns, linked parent/CTE columns, and
integer constants is fine.  Nullable column operands remain
rejected — full NULL propagation is v4.

## Why a CaseExpr-based lowering doesn't extend

v1 lowers `GREATEST(a, b)` to `CASE WHEN a >= b THEN a ELSE b END`.
For `n = 3` the natural extension is

```
CASE WHEN (CASE WHEN a >= b THEN a ELSE b END) >= c
     THEN (CASE WHEN a >= b THEN a ELSE b END)
     ELSE c
END
```

— a `CaseExpr` whose **comparison side** is itself a `CaseExpr`.
The embedded-condition emit path (`generate_embedded_condition` in
`RonSQLPreparer.cpp`) accepts column / linked-column / constant
operands on each side of an atom, but not nested `CaseExpr` trees.
Lifting that is a deep change to the embedded interpreter's
expression model and is out of scope.

The clean alternative is the shape `testCteNdbApi.cpp` Test 21 uses:
expand `GREATEST(a, b)` directly to a `BranchRegGe + Mov` pair on
the aggregation program (no embedded CASE involved).  Chained
left-associatively, this handles arbitrary `n`:

```
GREATEST(a, b, c)        =  pair-max( pair-max(a, b), c )
GREATEST(a, b, c, d)     =  pair-max( pair-max( pair-max(a,b), c ), d )
```

Each `pair-max` is a single-result expression that the SVM compiler
treats just like an arithmetic op — two operand registers in, one
result register out.

## Where this is achievable now

Kernel side is already there:

- `s_agg_interp_handlers` (DBTUP `DbtupExecQuery.cpp:8997`) covers
  `READ_ATTR_INTO_REG`, `LOAD_CONST*`, `BRANCH_*_REG_REG`, `MOV`.
- `JoinAggInterpreter::validateEmbeddedProgram`
  (`JoinAggInterpreter.cpp:540-631`) already whitelists the
  `BRANCH_*_REG_REG` family — but note that this whitelist is for
  **embedded** programs.  v2b emits into the **aggregation**
  program, which uses `s_agg_interp_handlers` directly.  No
  whitelist surgery for v2b.  Verify during implementation by
  tracing one program through the kernel.
- `NdbAggregator::BranchRegLt/Le/Gt/Ge/Eq/Ne` and `Mov` are public
  (`NdbAggregator.hpp:307-313`).  Test 21
  (`testCteNdbApi.cpp:5707-5709`) drives them end-to-end.
- The SVM compiler already has a precedent for emitting
  `BranchReg*` calls directly to `NdbAggregator` from outside the
  SVM — the cross-table-filter path in `RonSQLPreparer.cpp:4834-4855`
  bypasses the SVM and writes to the aggregator imperatively.  v2b
  goes the other way (puts pair-ops *into* the SVM) so the SVM's
  register allocator and dead-code-eliminator handle them
  uniformly.

So v2b is a pure RonSQL-side extension — no kernel changes.

## Design

### Layer 1 — Parser

`RonSQLParser.y`:

```
arith_expr_list
    : arith_expr T_COMMA arith_expr
        { $$ = context->mk_arg_list($1, $3); }
    | arith_expr_list T_COMMA arith_expr
        { $$ = context->append_arg_list($1, $3); }
    ;

arith_expr
    : ...
    | T_GREATEST T_LEFT arith_expr_list T_RIGHT
        { $$ = context->lower_greatest_least_nary($3, /*is_greatest*/true); }
    | T_LEAST    T_LEFT arith_expr_list T_RIGHT
        { $$ = context->lower_greatest_least_nary($3, /*is_greatest*/false); }
    ;
```

The two-argument rules from v1 collapse into the n-ary rule (the
list is the smallest at length 2; a single-arg `GREATEST(x)` is
rejected by the grammar — `arith_expr_list` requires `,`).

`RonSQLPreparer.hpp`: add a small struct (no `auto` anywhere — see
`feedback_no_auto.md`):

```cpp
struct ArithExprList
{
  AggregationAPICompiler::Expr* head;   // first operand
  ArithExprList* next;                  // singly-linked
};
```

allocated from the arena.  Helpers `mk_arg_list(a, b)` and
`append_arg_list(list, x)` assemble the list in source order.

`Context::lower_greatest_least_nary(ArithExprList* args,
bool is_greatest)`:

1. Walk the list left-to-right, folding each subsequent operand into
   a left-associative binary chain.  For `GREATEST(a, b, c, d)`:
   ```
   acc = pair_max(a, b)
   acc = pair_max(acc, c)
   acc = pair_max(acc, d)
   return acc
   ```
2. Each `pair_max` / `pair_min` call goes through the new
   `AggregationAPICompiler::Greatest2(Expr*, Expr*)` /
   `Least2(Expr*, Expr*)` factory below.
3. Operand validation reuses v1's `is_greatest_least_condition`
   tracker so the post-resolution nullable-column rejection covers
   every operand of every fold.  Track the synthesized `Expr*` in a
   parallel array (`m_greatest_least_pair_exprs` or extend the
   existing tracker to take a tagged kind) — pair-ops aren't tied
   to a `ConditionalExpression` the way v1's CASE was, so the v1
   data structure needs a kind tag or a sibling tracker.

### Layer 2 — SVM (AggregationAPICompiler)

`AggregationAPICompiler.hpp`:

1. Extend `FORALL_INSTRUCTIONS` so `Greatest2` and `Least2` are
   first-class SVM instruction types — the existing macro fan-out
   then wires `pushInstr(SVMInstrType, …)` and `dead_code_elimination`
   correctly:
   ```cpp
   #define FORALL_PAIR_OPS(X) \
     X(Greatest2) \
     X(Least2)
   ```
   And include `FORALL_PAIR_OPS(X)` in `FORALL_INSTRUCTIONS(X)`.

2. Extend `enum class ExprOp` with `Greatest2`, `Least2`.  Each is
   a binary op (left + right Expr operands) — exactly the same
   shape as `Add` / `Mul`.  No new fields on `Expr`.

3. Public factories:
   ```cpp
   Expr* Greatest2(Expr* x, Expr* y);
   Expr* Least2(Expr* x, Expr* y);
   ```
   Both delegate to `public_arithmetic_expression_helper(ExprOp::Greatest2,
   x, y)` (or `Least2`) — reusing the existing reference-counting,
   `est_regs` propagation, and operand-canonicalisation infrastructure.

4. **`Instr` struct unchanged.**  A pair-op is a `{type, dest, src}`
   instruction just like `Add`.  `dest` holds the result-and-left-operand
   register; `src` holds the right-operand register.  No third operand,
   no skip count in the SVM — both are added at translation time.

5. **`svm_execute`**: handle `Greatest2` / `Least2` exactly like
   the arithmetic ops (`OPERATOR_CASE` macro path).  No new code —
   the `FORALL_INSTRUCTIONS(X)` extension picks them up.

6. **`compile(Expr*, …)` (`AggregationAPICompiler.cpp:580`)**:
   pair-ops fall through to the existing arithmetic code path —
   `expr->left == expr->right` is collapsed; otherwise both
   operands are loaded into registers, both locked, the destination
   may be moved to preserve `expr->left` for later reuse, and
   `pushInstr(expr->op, dest, src, …)` emits the SVM instruction.
   Adding `Greatest2`/`Least2` to `FORALL_INSTRUCTIONS` handles
   it for free.

7. **`dead_code_elimination`**: pair-ops are arithmetic-shaped
   (both operands needed before, dest defined after) — picked up
   by the `OPERATOR_CASE` macro.  No special case needed.

8. **`pushInstr(ExprOp, …)` translation table** at
   `AggregationAPICompiler.cpp:855-872`: extend the
   `OP_CASE(Name)` macro coverage so `ExprOp::Greatest2 →
   SVMInstrType::Greatest2` (and Least2) lookups succeed.  Done
   via `FORALL_PAIR_OPS(OP_CASE)` alongside
   `FORALL_ARITHMETIC_OPS(OP_CASE)`.

9. **Printer** (`print(Expr*)`, `print(Instr*)`): add cases.
   Useful for `--explain`.

### Layer 3 — Aggregator emission (programAggregator)

`RonSQLPreparer.cpp:7370-7440`: extend the SVMInstrType switch with
two new cases.

```cpp
case AggregationAPICompiler::SVMInstrType::Greatest2:
  // result = max(reg[dest], reg[src])
  // BranchRegGe(dest, src, /*skip*/1) — jump over the Mov when
  //   dest >= src, leaving the larger value already in dest.
  // Mov(dest, src)                   — else copy src into dest.
  programAggregator_do_or_fail(aggregator->BranchRegGe(dest, src, 1));
  programAggregator_do_or_fail(aggregator->Mov(dest, src));
  break;
case AggregationAPICompiler::SVMInstrType::Least2:
  programAggregator_do_or_fail(aggregator->BranchRegLe(dest, src, 1));
  programAggregator_do_or_fail(aggregator->Mov(dest, src));
  break;
```

The CTE-scope aggregator emit path at `RonSQLPreparer.cpp:7894+`
mirrors the main-scope path — extend it identically.  Same pair of
cases.

That's the whole emission story.  Each pair-op expands to **two**
NdbAggregator calls (`BranchRegGe + Mov` or `BranchRegLe + Mov`).

### Layer 4 — Coexistence with v1 / v2a

v1's two-argument grammar rule for `GREATEST` / `LEAST` is replaced
by the new n-ary rule.  v1's lowering helper
`Context::lower_greatest_least(a, b, is_greatest)` is replaced by
`lower_greatest_least_nary(args, is_greatest)`; v1's
two-arg-in-CaseExpr lowering goes away entirely for the n=2 case as
well — even `GREATEST(col, 100)` now emits as a `Greatest2` SVM op
rather than a `CaseExpr`.

This is a **byte-level change** to v1's emit shape.  Externally
visible behaviour (query results) is unchanged.  Recorded MTR
results stay valid (output equivalence), but if any internal
`--explain` output is recorded, refresh those baselines.

The col-vs-col CASE-condition path that v2a unlocked stays in
place — `WHEN col_a > col_b THEN …` still uses the embedded path
because the user wrote a CASE explicitly.  v2b does not redirect
that path.

### Edge cases

1. **`GREATEST(a)`** — rejected by grammar (`arith_expr_list`
   requires at least two operands).
2. **`GREATEST(a, a)`** — collapses to `a` directly in
   `lower_greatest_least_nary` (carry-over from v1's degenerate
   path).  Spelled `Greatest2(a, a)` reaches the SVM, the
   `expr->left == expr->right` branch in `compile(Expr*)` causes
   the same register to be loaded twice and `BranchRegGe(dest,
   dest, 1) + Mov(dest, dest)` to be emitted; correct but
   unnecessary.  Collapse at parse time to skip both runtime cost
   and compiler complexity.
3. **`GREATEST(GREATEST(a, b), c)`** — written explicitly by the
   user; lowered into the same chain as `GREATEST(a, b, c)`.  The
   parser invokes `lower_greatest_least_nary` once per inner
   GREATEST, so the result is the same chain regardless of how
   the user spelled it.
4. **`GREATEST(a, 5)`** — v1's `col, const` shape.  Now emits as
   `Greatest2(Load(a), LoadConstantInteger(5))`, with two SVM
   loads and a pair-op.  Output unchanged.
5. **`GREATEST(NULL_LITERAL, x)`** — RonSQL has no `NULL` literal
   surface today; no change for v2b.
6. **`GREATEST(c1, c2)` where c1 is nullable** — rejected
   post-resolution by the carry-over of v1's nullable-column
   guard.  Reject message stays
   `"GREATEST/LEAST on nullable column operands is not yet supported"`.
7. **CTE virtual / linked column operands** — same column kinds
   as v2a's col-vs-col CASE path.  `compile(Expr*)` reaches the
   existing `Load`/`LoadLinkedColumn`-driven SVM machinery, which
   already handles parent-table / CTE-virtual-table linked columns
   for arithmetic ops.  No new code; verify with a CTE-scope test.

### Restrictions kept in v2b

- **Bigint-only** on each operand.  Sub-Bigint linked columns
  defer to v5 (`cte_filter_phase_i5_v5.md`).  Float / Decimal /
  VARCHAR defer to v3 / I.6.
- **Nullable column operands rejected** (deferred to v4).
- **Arithmetic operands inside GREATEST/LEAST**
  (e.g. `GREATEST(a + b, c)`) — accepted iff the SVM compiler
  already handles the inner `Add` / `Mul` etc. shape, which it
  does.  No new restriction.
- **CTE aggregate output as an operand** — accepted iff the
  existing CTE-aggregate-projection path supports it for
  arithmetic ops, which it does.  Verify during implementation.

## Test plan

`mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v2b.test`,
modelled on the v2a test file.  Use the `ronsql_compare.inc`
harness for output equivalence.

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `SUM(GREATEST(a, b, c))` over single table | n=3 baseline |
| 2 | `SUM(GREATEST(a, b, c, d))` | n=4 |
| 3 | `SUM(LEAST(a, b, c))` | LEAST n=3 |
| 4 | `SUM(GREATEST(col, 100, 200))` | mixed col + const |
| 5 | `SUM(GREATEST(t.a, t.b, 50, t.c))` | const interleaved |
| 6 | `SUM(GREATEST(GREATEST(a, b), c))` | explicit nesting same as flat |
| 7 | regression: v1 two-arg `GREATEST(col, 100)` | output equivalence after re-emit |
| 8 | regression: v2a distinct-col CASE in same query as a Greatest2 | composition |
| 9 | CTE-scope: `WITH c AS (SELECT k, MAX(v) FROM t GROUP BY k) SELECT k, GREATEST(MAX(v), 100, 50) FROM c GROUP BY k` | CTE-body pair-ops via linked / virt columns |
| 10 | `GREATEST(c1, c2, c3)` where c2 nullable → expect rejection with v1 nullable message | guard carry-over |
| 11 | join + linked column: `GREATEST(parent.x, child.y, 100)` over a CTE_LOOKUP join | cross-row register loads |

Tests 1–6, 8, 9, 11 use `ronsql_compare.inc`.  Test 7 confirms v1
output is unchanged.  Test 10 uses RonSQL's error-expecting harness.

If any test surfaces a corner case where v2b produces wrong output
relative to v1's CaseExpr emit (e.g. mismatched signed/unsigned
register comparison), capture in the doc and either fix or restrict
v2b further before shipping.

## Risks

1. **`Mov` after a `BranchRegGe` is conditional at runtime**
   but unconditional in the SVM model.  The SVM treats
   `Greatest2(dest, src)` as if it always executed
   `dest := max(r[dest], r[src])`.  At runtime, the kernel either
   skips the `Mov` (when `dest >= src`) or executes it.  In both
   cases the **resulting value** in `dest` is `max(r[dest], r[src])`.
   The SVM symbolic model is correct as long as that matches.
   Verify by reading `s_agg_interp_handlers` for `BranchRegGe` and
   the agg-program execution model in DBTUP.

2. **Dead-code elimination on chained pair-ops.**  If a register
   holds a pair-op result and is never used after, will DCE remove
   the `BranchRegGe + Mov` sequence cleanly?  The SVM-level DCE
   sees one `Greatest2` instruction and removes it (or keeps it).
   The expansion in `programAggregator` runs *after* DCE, so DCE
   sees the simple two-operand instruction and does the right
   thing.  Cross-check by reading `dead_code_elimination` and
   confirming the macro fan-out covers the new types.

3. **Register allocator and `est_regs`.**  The existing
   `est_regs` field on `Expr` propagates via the arithmetic-op
   constructor.  Pair-ops should pick up the same propagation by
   reusing `public_arithmetic_expression_helper`.  Verify with a
   query like `SUM(GREATEST(a, b, c)) + SUM(GREATEST(d, e, f))`
   that exercises register pressure.  REGS = 8 leaves comfortable
   headroom for n up to ~15 with two parallel chains.

4. **`raw_word_size` accounting** for embedded CASE blocks
   (`AggregationAPICompiler.cpp:1185`) computes the agg-program
   word count between two SVM positions.  Pair-ops emit two kernel
   words (`BranchRegGe + Mov` or `BranchRegLe + Mov`) per SVM
   instruction.  Update `raw_word_size`'s switch to return 2 for
   `Greatest2` / `Least2`, otherwise embedded-CASE blocks that
   contain a pair-op will compute the wrong skip distance.  This
   is the load-bearing accounting bug to watch for.

5. **`pair-op result is used twice` in a chain.**  In a chain like
   `GREATEST(a, b, c, d)` the intermediate result is used exactly
   once (immediately as the left operand of the next pair-op), so
   `usage = 1` for the inner expressions.  Test #6
   (`GREATEST(GREATEST(a, b), c)` written explicitly) is the same
   shape.  No reuse.  But a query like
   `SUM(GREATEST(a, b)) + AVG(GREATEST(a, b))` shares the inner
   `Greatest2(a, b)` Expr and exercises the
   `expr->usage > expr->program_usage` save-copy path in
   `compile(Expr*)`.  Should work via the existing arithmetic
   machinery; confirm with a test if not already covered.

6. **Constant-only operands.**  `GREATEST(100, 200, 50)`
   constant-folds to `200` at parse time?  Today the
   `AggregationAPICompiler` does not constant-fold arithmetic — it
   compiles `Add(LoadConstantInteger(1), LoadConstantInteger(2))`
   into `LoadConstantInteger(1) + LoadConstantInteger(2) +
   Add(reg, reg)`.  Pair-ops follow the same convention; v2b does
   not introduce constant folding.  Still correct; mildly
   suboptimal.  Out of scope.

## Deliverables

- `storage/ndb/src/ronsql/AggregationAPICompiler.hpp`,
  `.cpp` — `Greatest2` / `Least2` factories, `FORALL_PAIR_OPS`
  macro extension, `raw_word_size` cases, printer cases.
- `storage/ndb/src/ronsql/RonSQLPreparer.hpp`,
  `.cpp` — `lower_greatest_least_nary`, `mk_arg_list`,
  `append_arg_list`, struct `ArithExprList`, two new
  `programAggregator` cases (main and CTE scopes), nullable-operand
  tracker extension.
- `storage/ndb/src/ronsql/RonSQLParser.y` — `arith_expr_list`
  non-terminal, n-ary GREATEST/LEAST production rules; remove v1's
  two-arg rules.
- `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v2b.test` —
  new test file.
- `mysql-test/suite/ronsql/r/ronsql_cte_greatest_least_v2b.result`
  — recorded on first run.
- `cte_filter_phase_i5_v2.md`, `cte_filter_phase_i5.md`,
  `cte_filter_phase_i.md`, `CLAUDE.md` index — flip v2b to
  "shipped" and update the catalogue.
- This doc — flip Status to "shipped" with a "What shipped"
  section in the v2a / v2 style.
- Memory `project_cte_branch_state.md` — drop v2b from the open
  follow-ups list once shipped.

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ronsql_cli rdrs2

cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_greatest_least_v2b
./mtr --suite=ronsql                # full suite — no regressions
./mtr --suite=ndb_push_agg          # block tests — no regressions
```

Single commit (`RONDB-1050: Phase I.5 v2b — n-ary GREATEST / LEAST
via SVM extension`).  No staged sub-commits — the parser, SVM, and
emit changes must land together to keep the build coherent (per
risk #5 in `cte_filter_phase_i5_v2.md`).
