# Phase I.5 v4 — NULL propagation for GREATEST / LEAST

## Status

**Shipped.**  Supersedes the v4 sketch in `cte_filter_phase_i5.md`
(multi-arm CASE detour / v2-paired register short-circuit) — neither
was needed: Phase M (`cte_filter_phase_m.md`) gave us the embedded
normal-interpreter primitives, and v4 is a focused extension of v2b's
pair-op program (9 → 14 words) plus removal of the parser-time
nullable rejection.

**What shipped:**

- **`RonSQLPreparer.cpp`** — extracted the pair-op kernel emission
  into `RonSQLPreparer::emit_pair_op_embedded(aggregator, dest, src,
  is_greatest)`.  Both `programAggregator` and
  `programAggregator_join` switch cases for `Greatest2` / `Least2`
  collapse to one helper call.  Helper emits a 14-word embedded
  program (NULL-test on each operand → STOP_PROGRAM tail; otherwise
  `BRANCH_(GE|LE)_REG_REG` to write 0 or 1) followed by `Mov(dest,
  src)`.
- **Branch register encoding gotcha (documented in code).**  The
  encoder's `Reg1` lands in bits 9..11 and `Reg2` in bits 6..8, but
  the handler reads `theRegister = getReg1 << 2` from bits 6..8 and
  `TrightRegister = getReg2 << 2` from bits 9..11.  So an encoded
  `Branch(BRANCH_GE_REG_REG, Reg1=2, Reg2=1)` evaluates `r[1] >=
  r[2]` at runtime — the register names are swapped between encoder
  and handler.  The new helper's comment captures this so future
  reviewers don't waste a couple of hours like I did.
- **`validate_greatest_least_pair_loads`** — dropped the nullable
  rejection.  The CTE-column `column_map==NULL` early-return is now
  documented as a "type-check skip"; nullability is no longer the
  reason it exists.
- **`AggregationAPICompiler.cpp` `raw_word_size`** — pair-op
  per-instruction kernel-word count bumped from 11 to 16 (1
  EmbeddedInterp header + 14-word body + 1 trailing Mov).
- **`ronsql_cte_greatest_least.test` Test 10** — converted from
  `--error 1` rejection to a normal `ronsql_compare.inc` pass case
  with a `BIGINT NULL` fixture; serves as the v1 → v4 regression
  marker.
- **New `ronsql_cte_greatest_least_v4.test`** — ten cases covering
  nullable col+const, n=3 chain, n + multiple constants,
  `MAX(GREATEST(...))`, `COUNT(GREATEST(...))`, explicit nesting, a
  NOT NULL regression, join + linked nullable parent column, and
  CTE-scope chained pair-op against a nullable virtual column.

**Per-row semantics shipped:** the first pair-op that sees a NULL
operand writes `AGG_EMBEDDED_INTERP_STOP_PROGRAM` to the embedded
output; the aggregation interpreter abandons the rest of the row's
program.  Outer SUM / MIN / MAX / COUNT therefore never accumulate
that row, matching MySQL's `GREATEST(NULL, …) = NULL` under
aggregation.

**Caveat removed.**  The v2b doc's "validate skips type/nullable
check for CTE columns because column_map==NULL at compile time" is
no longer load-bearing for nullable correctness — v4 handles NULL at
runtime regardless of whether the parser-time check fired.  The skip
still applies for the type check (CTE virt-table descriptors are
built at execute time), but a nullable CTE column operand now flows
through correctly.

## Static-NULL-elision fast path (follow-up shipped)

After the initial v4 commit, a fast path was added so that pair-ops
whose operands are statically known to be non-nullable emit the v2b
9-word body (no NULL test), saving five program words per pair-op
plus the cost of running the two `BRANCH_REG_EQ_NULL` instructions
at runtime.  The decision is per-pair-op: nested chains where some
operands are nullable and others not still pay the cost only on the
nullable pair-ops.

**Mechanism:**

1. **`AggregationAPICompiler` parallel arrays.**  New
   `m_pair_op_program_exprs[i]` records the `Greatest2` / `Least2`
   `Expr*` for each pair-op program slot, NULL otherwise.
   Populated in `compile(Expr*, …)` immediately after the pair-op
   `pushInstr`.  `m_pair_op_needs_null_check[i]` records the
   per-slot bool decision; populated lazily by RonSQLPreparer.
   Both arrays grow alongside `m_program` (kept in lock-step at the
   four `m_program.push` sites and through DCE rebuild).
2. **`RonSQLPreparer::compute_pair_op_needs_null_check(scope, expr)`**
   recursively walks `Greatest2` / `Least2` operand trees.
   Returns `false` only when every leaf is a `LoadConstantInt` or a
   `Load(col_idx)` where
   `scope.column_map[col_idx]->getNullable() == false`.  CTE virt
   columns (`column_map[col_idx] == NULL` until execute time)
   conservatively return `true`.  Other arithmetic ops also
   conservatively return `true`.
3. **`prepare_pair_op_null_check_cache(scope)`** fills the per-slot
   bool array.  Called at the top of `programAggregator` and
   `programAggregator_join` before any `raw_word_size` query or
   pair-op emission consumes it.  Idempotent.
4. **`emit_pair_op_embedded(...)`** gains a `bool needs_null_check`
   parameter — emits either the 14-word body (NULL-aware) or the
   9-word body (fast).  Trailing `Mov(dest, src)` is unchanged in
   both branches.
5. **`raw_word_size`** consults `m_pair_op_needs_null_check[i]` per
   slot and counts 16 / 11 accordingly.  Defaults conservatively to
   16 if the cache hasn't been populated yet (so pre-cache callers
   don't underflow embedded-CASE skip distances).

**Coverage.**  No new MTR test — existing tests already exercise
both paths:

- `ronsql_cte_greatest_least.test` / `ronsql_cte_greatest_least_v2b.test`
  use NOT NULL columns → fast path (9-word body, count 11).
- `ronsql_cte_greatest_least_v4.test` and the new Test 10 in v2b use
  nullable columns → slow path (14-word body, count 16).

The recorded baselines for both paths are unchanged after the
optimisation lands; only the kernel program shape varies per
pair-op.

## Goal

Lift the v1 / v2b "nullable column operand" rejection so

```sql
GREATEST(nullable_col, K)
LEAST(a, nullable_b, c)
SUM(GREATEST(nullable, K))
```

return MySQL-correct results — i.e. the per-row GREATEST / LEAST is
NULL whenever any operand is NULL on that row, and outer aggregations
(`SUM` / `MIN` / `MAX` / `COUNT`) skip those NULL rows the same way
they skip any other NULL input.

## Why this is achievable now

After v2b + Phase M, every primitive needed for NULL propagation is
already in place:

- **NULL-aware register bridging.**  `READ_AGG_REG_TO_REG`
  (`DbtupExecQuery.cpp:7073` `handleReadAggRegToReg`) already
  copies the source aggregation register's NULL flag into the
  normal-interpreter register's `TregMemBuffer[Reg]` slot as
  `NULL_INDICATOR` when `src.is_null` is set.  No kernel change
  needed there.
- **NULL-test branch in the normal interpreter.**
  `BRANCH_REG_EQ_NULL` (`DbtupExecQuery.cpp:5918`
  `handleBranchRegEqNull`) branches when the interpreter register's
  buffer slot equals `NULL_INDICATOR`.  Already whitelisted in
  `JoinAggInterpreter::validateEmbeddedProgram` (line 557).
- **"Stop aggregation program for this row" outcome.**  Phase M
  added `AGG_EMBEDDED_INTERP_STOP_PROGRAM`
  (`NdbAggregationCommon.hpp:74`, value `0xFFFF`).  The
  agg-interpreter recognises that magic skip-offset and abandons
  the rest of the program for the current row
  (`AggInterpreter.cpp:1841`,
  `JoinAggInterpreter.cpp:1477`).  RonSQL already uses it from the
  cross-table embedded filter path
  (`RonSQLPreparer.cpp:7800`).
- **Pair-op SVM model unchanged.**  v2b's `Greatest2` / `Least2`
  shape stays as-is; only the embedded program emitted at
  `programAggregator` time grows from 9 words to 14.

## Per-row semantics

For one pair-op `Greatest2(dest, src)` (the second operand is the
register holding `src`'s value):

```
if (r[dest] is NULL) || (r[src] is NULL):
    output := AGG_EMBEDDED_INTERP_STOP_PROGRAM
else if (r[src] >= r[dest]):
    output := 1                 # src wins, run the trailing Mov
else:
    output := 0                 # dest wins, skip the trailing Mov
```

For `Least2`, swap `>=` for `<=`.  For chains
`Greatest2(Greatest2(a, b), c)`, the first pair-op that sees a NULL
operand stops the entire row — downstream pair-ops never run, and the
outer SUM / MIN / MAX / COUNT skips the row.  This is exactly MySQL
semantics.

Today RonSQL only allows GREATEST / LEAST inside an aggregation
function (SUM / MIN / MAX / COUNT) — there is no "GREATEST as a
projection" surface that would need a per-row NULL value to flow
downstream.  v4 therefore does not need a way to write NULL into an
aggregation register; "stop this row's program" is sufficient.

## Embedded program — new shape

v2b's current 9-word program (Phase M) is:

```text
0  READ_AGG_REG_TO_REG(dest → reg1)
1  READ_AGG_REG_TO_REG(src  → reg2)
2  BRANCH_(GE|LE)_REG_REG(reg2, reg1, +4)
3  LoadConst16(reg3, 0)
4  WriteInterpreterOutput(reg3, 0)
5  ExitOK
6  LoadConst16(reg3, 1)             ← +4 land here
7  WriteInterpreterOutput(reg3, 0)
8  ExitOK
EmbeddedInterp(9) header is in the agg program; the 9 words above
form the embedded program body.
```

v4 grows this to 14 words:

```text
0   READ_AGG_REG_TO_REG(dest → reg1)
1   READ_AGG_REG_TO_REG(src  → reg2)
2   BRANCH_REG_EQ_NULL(reg1, +9)             ← jump to NULL branch
3   BRANCH_REG_EQ_NULL(reg2, +8)             ← jump to NULL branch
4   BRANCH_(GE|LE)_REG_REG(reg2, reg1, +4)   ← src wins → output=1
5   LoadConst16(reg3, 0)
6   WriteInterpreterOutput(reg3, 0)
7   ExitOK
8   LoadConst16(reg3, 1)             ← compare-true land here
9   WriteInterpreterOutput(reg3, 0)
10  ExitOK
11  LoadConst16(reg3, AGG_EMBEDDED_INTERP_STOP_PROGRAM)  ← NULL land here
12  WriteInterpreterOutput(reg3, 0)
13  ExitOK
```

Skip offsets are kernel-PC offsets relative to the instruction *after*
the branch, which is the convention the existing v2b emission already
uses.  Concretely, with PC starting at 0 for the first word of the
embedded body:

- BRANCH_REG_EQ_NULL at PC=2 wants to land at PC=11
  (`LoadConst16(reg3, STOP_PROGRAM)`); offset = 11 - 3 = **8**.
  (Phase M's existing emission encodes the offset in the instruction's
  upper bits; v4 uses the same encoder.)
- BRANCH_REG_EQ_NULL at PC=3 wants to land at PC=11; offset = 11 - 4
  = **7**.
- BRANCH_(GE|LE)_REG_REG at PC=4 wants to land at PC=8
  (`LoadConst16(reg3, 1)`); offset = 8 - 5 = **3**.

These three offsets are the only emit-time numbers that change vs.
v2b.  All other words are unchanged.

The only constant we need to encode that the SVM compiler doesn't
already use is `AGG_EMBEDDED_INTERP_STOP_PROGRAM == 0xFFFF`.
`LoadConst16` is the right opcode (16-bit immediate).  No kernel
change.

## RonSQL preparer changes

1. **Drop the parser-time nullable rejection.**
   `validate_greatest_least_pair_loads` in `RonSQLPreparer.cpp` (added
   by v2b) currently rejects nullable column operands with
   ```
   "GREATEST/LEAST on nullable column operands is not yet supported
    because MySQL NULL propagation would require multi-arm CASE
    lowering."
   ```
   Remove this rejection.  Keep the integer-type check (Tinyint ..
   Bigint signed + unsigned).  The CTE-column nullability skip
   (`column_map[col_idx] == NULL` early-out) becomes a no-op of its
   own accord — it was put there to bypass the nullable check the
   helper used to enforce.

2. **Update the pair-op emission in `programAggregator` and
   `programAggregator_join`.**  Both currently emit the 9-word
   program; replace each with the 14-word program above.  The
   `EmbeddedInterp(9)` header word becomes `EmbeddedInterp(14)` (the
   value 14 in the lower bits is the embedded-body length).

3. **`raw_word_size`** in `AggregationAPICompiler.cpp` — Phase M
   left this returning the per-pair-op kernel-word count (the embedded
   header + 9-word body + Mov = 11 words).  v4 raises that to
   1 + 14 + 1 = 16 words.  Update the constant.  The function is the
   load-bearing accounting hook for embedded-CASE skip distances,
   so this must move in lock-step with the new emission.

4. **No SVM changes.**  `Greatest2` / `Least2` SVM types, the
   compile path, DCE, and constant folding are all untouched — the
   change is below the SVM, in the kernel-emit translation.

## Tests

New file `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v4.test`
mirroring the v2b shape, plus a couple of cases borrowed from the v1
nullable rejection that should now pass.

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `SELECT SUM(GREATEST(n_val, 5)) FROM cte_nullable;` over a fixture mixing NULL and non-NULL `n_val` | NULL rows skipped; non-NULL rows give MySQL-correct max |
| 2 | `SELECT SUM(LEAST(n_val, 100)) ...` | mirror |
| 3 | `SELECT SUM(GREATEST(n_a, n_b, n_c)) FROM cte_nullable_3` over a fixture where each col is independently NULL on different rows | chain stops on first NULL operand each row |
| 4 | `SELECT SUM(GREATEST(n_val, 5, 100)) ...` | nullable column + ≥2 constants; output = NULL when the column is NULL, else max |
| 5 | `SELECT MAX(GREATEST(n_a, n_b)) ...` | outer MAX skips NULL rows |
| 6 | `SELECT COUNT(GREATEST(n_a, n_b)) ...` | COUNT(NULL)=0 — count drops on NULL rows |
| 7 | `SELECT SUM(GREATEST(GREATEST(n_a, n_b), n_c)) ...` | nested GREATEST same as flat |
| 8 | regression: rerun a v2b NOT-NULL case (e.g. `SUM(GREATEST(c.c_region, c.c_floor, c.c_ceil))` from `ronsql_cte_greatest_least_v2b.test` Test 1) | output identical to v2b |
| 9 | join + linked column with nullable parent column | NULL propagation across linked-attr buffer |
| 10 | CTE-scope: `WITH s AS (SELECT k, MAX(v) t FROM ... GROUP BY k) SELECT SUM(GREATEST(s.t, 5, n_val)) ...` where `n_val` nullable | CTE scope's pair-op emission also gets the NULL handling |

Use `--source suite/ronsql/include/ronsql_compare.inc` to compare
RonSQL output against MySQL.

Update `ronsql_cte_greatest_least.test` Test 10 (still expects
nullable rejection): convert from `--error 1` to a normal compare so
it now exercises the v4 success path.

## Out of scope (still)

- **GREATEST as a projection** (i.e. `SELECT GREATEST(a, b) FROM ...`
  without an outer aggregate) — RonSQL doesn't expose this surface
  today.  When it does, we'd need a way to write NULL into a CTE
  virtual column or pass-through register.  Tracked alongside any
  future projection-only RonSQL work.
- **`COALESCE(...)`** as a NULL-coalescing companion — separate
  feature; not part of v4.
- **`IS NULL` / `IS NOT NULL` on a GREATEST result** — currently no
  user surface in RonSQL; add when needed.
- **Mixed integer + float / decimal NULL operands** — folds into v3 /
  I.6 work.  v4 stays integer-only on each operand.

## Risks

1. **`LoadConst16(reg, 0xFFFF)` upper-bits clash.**  Confirm
   `LoadConst16` packs the 16-bit constant cleanly into the instruction
   word and that `0xFFFF` does not overlap with the opcode bits.  If
   the encoding can't represent `0xFFFF`, fall back to
   `LoadConst32(reg, 0xFFFF)` (1 extra word).
2. **PC vs. exec-relative offsets.**  v2b's existing
   `BRANCH_(GE|LE)_REG_REG(2, 1, +4)` computed via `(4 << 16)` is the
   reference for offset encoding.  Mirror that exactly for the two
   new `BRANCH_REG_EQ_NULL` instructions; do not invent a new
   convention.
3. **Validator whitelist.**  `BRANCH_REG_EQ_NULL` is already in
   `JoinAggInterpreter::validateEmbeddedProgram` (line 557) and the
   AggInterpreter equivalent.  Sanity-check both before shipping.
4. **`raw_word_size` synchronisation.**  v2b shipped with one count;
   v4 changes it.  If anything else assumes the v2b count it will
   miscompute embedded-CASE skip distances.  Grep for hard-coded `11`
   or other pair-op constants before commit.
5. **Embedded-program `LoadConst16` register reuse with the existing
   `reg3`.**  Today's program uses `reg3` for output; v4 keeps the
   same convention.  No new register needed.
6. **Multi-batch CTE delivery.**  When pair-ops feed into a CTE
   aggregator and the CTE leaf re-runs across batches, the embedded
   program runs per-row, so NULL handling Just Works regardless of
   batching.  No specific flow-control consideration.

## Sequencing within the I.5 family

- **v3** (Float / Decimal / VARCHAR operands) is independent of v4 —
  can land in either order.
- **v5** (typed sub-Bigint linked-column register loads) only
  affects v2a's embedded-CASE path; orthogonal to v4.
- **v6** (CTE linked-vs-linked runtime tests) is orthogonal too —
  pair-op kernel emission is the same shape regardless of
  linked-vs-linked vs. column-vs-column.

So v4 can ship next without blocking or being blocked.

## Deliverables

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — drop nullable
  rejection in `validate_greatest_least_pair_loads`; rewrite
  `Greatest2` / `Least2` cases in `programAggregator` and
  `programAggregator_join` to emit the 14-word body with the new
  NULL branches.
- `storage/ndb/src/ronsql/AggregationAPICompiler.cpp` — bump
  `raw_word_size`'s pair-op return value (9 → 14 inside the embedded
  body, total per-pair-op kernel words 11 → 16).
- `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v4.test` —
  new file; ten cases above.
- `mysql-test/suite/ronsql/r/ronsql_cte_greatest_least_v4.result` —
  recorded on first run.
- `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least.test` —
  Test 10 converted from rejection to passing case.
- This doc — flip Status to "Shipped" with a "What shipped" section
  in the v2b style.
- Catalogue updates: `cte_filter_phase_i.md`, `cte_filter_phase_i5.md`,
  `cte_filter_phase_i5_v2.md` (note v4 supersedes the embedded
  CTE-column nullability skip), `CLAUDE.md` index.
- Memory `project_cte_branch_state.md`: drop v4 from the open
  follow-ups list once shipped; rephrase the v2b CTE-column-nullability
  caveat as "fixed in v4".

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ronsql_cli rdrs2

cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_greatest_least_v4
./mtr --record --suite=ronsql ronsql_cte_greatest_least
./mtr --suite=ronsql                 # full suite — no regressions
./mtr --suite=ndb_push_agg           # block tests — no regressions
```

Single commit (`RONDB-1050: Phase I.5 v4 — NULL propagation for
GREATEST / LEAST`).  No staged sub-commits — drop the rejection,
extend the embedded program, and update the test in lock-step.
