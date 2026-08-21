# Phase 5E — division, modulo, and GENERIC arithmetic; RonSQL as the test platform

**Status: 5E-1 DONE & VERIFIED (2026-08-21 — NO regen; bridge_tests
116/116, rondb_jit_ronsql_canary green incl. the decimal must-JITs
Q7/Q8 and the 4060 pipeline canaries; see the CORRECTION in the
record below — the data node optimizer already types most generic
arith, the live 5E-1 win is DECIMAL arithmetic).
5E-2 DONE & VERIFIED (2026-08-21 — regen clean on both arches,
bridge_tests 123/123, RonSQL canary Q9-Q13 green incl. div-by-zero
NULL semantics and the INT64_MIN/-1 overflow under 4060, Test 27
repointed and green, census green, full ndb_push_agg sweep passed).
5E-3 not started.**

## 5E-2 implementation record (2026-08-21)

Landed as planned. Four stencils: `op_div_int_checked` (divisor 0 →
row_fallback per op_div_f64's pattern; dividend==INT64_MIN &&
divisor==-1 guarded BEFORE the hardware divide — x86-64 idiv traps —
and routed to HOLE_DIV_OVF_TGT via op->d), `op_mod_int` (same zero
fallback; INT64_MIN % -1 → 0 via the guard; no overflow hole — a
remainder's magnitude is always < |divisor|), `op_div_u64` /
`op_mod_u64` (plain udiv/urem + the zero fallback). New DIV_*/MOD_*
fold-hole trios (sha-recipe magics, shared signed/unsigned like the
ADD family) + the HOLE_DIV_OVF_TGT symbol; op_div_int_checked rides
the `_checked` KEEP_ALL naming rule, the other three are STRIP_TAIL
single-tail stencils. jit1's op_has_overflow_target gains
OP_DIV_INT_CHECKED.

Bridge: one case for kOpDivInt / kOpDivIntBigint (the optimizer's
typed rewrite — both lower identically) / kOpMod, off the tracker's
proof with the 5C-4 unsigned rules. DIV's result track is U64 iff
either operand is (kernel is_unsigned = a|b); MOD's follows the
DIVIDEND only (kernel a_unsigned) — an NNC dividend with a u64
divisor computes on OP_MOD_U64 but lands I64 (T63e pins it). f64 /
STR / UNKNOWN operands and u64-mixed-signed-variable → UNSUPPORTED
(double trunc-DIV and fmod stay durable negatives). The new kinds
join the nb-pass read/write tables and every name table.

Test 27 (testJoinAggNdbApi) repointed from kOpMod to kOpSetRegNull —
SUM(0) with a dead SetRegNull(1), the bridge's PERMANENTLY
unsupported opcode, so the canary never needs repointing again.

Tests: bridge 123/123 (T63a-g: signed DIV with d-fixup, typed
kOpDivIntBigint, signed MOD without one, u64 DIV via NNC divisor,
the MOD dividend-signedness rule, f64 reject, mixed-variable
reject). RonSQL canary Q9-Q13: DIV/% must-JIT, negative operands
(truncation + dividend-sign, MIN/MAX-discriminated), unsigned with
constant divisors, division-by-zero rows as correctness-only
NULL-semantics probes (per-row fallback — no 4060), and INT64_MIN
DIV -1 erroring from the JIT's overflow exit UNDER 4060 on both
engines. Census int_div/int_mod comments updated (still 0 =
never-pushed by the SQL planner).

Regen war story: the first arm64 regen failed with an unrecognised
relocation to `next` mid-body — clang TAIL-DUPLICATED the exit for
the divisor==0 if/else, which STRIP_TAIL cannot strip. Fixed by
making all four bodies branchless (safe-divisor-1 divide + csel of
the result; MOD additionally uses the exact identity x % -1 == 0 to
kill the trapping input without a branch). Lesson for future
stencils: a second path that ends in its own store + TAIL_NEXT WILL
get duplicated — keep one exit reachable by straight line.

## 5E-1 implementation record (2026-08-21)

**Correction (caught in review by Mikael): the data node OPTIMIZES
generic ops to typed BEFORE compilation.** The pipeline in
`PushdownInterpreterFactory::Create` is Init → OptimizeProgram →
dbtup_jit_compile_agg, and `OptimizeProgramBuffer` rewrites
kOpPlus/kOpMinus/kOpMul to the kOp*Bigint/kOp*Double forms in place
whenever both operand types are statically inferable (all integer
widths — incl. BIGUNSIGNED — infer BIGINT; FLOAT/DOUBLE infer
DOUBLE; kOpDivInt similarly becomes kOpDivIntBigint; kOpSum/Min/Max
become typed). So RonSQL's BIGINT and DOUBLE arithmetic already
reached the bridge TYPED and already compiled — the plan's original
"RonSQL arithmetic falls back wholesale" claim was wrong.

What genuinely stays GENERIC at the bridge (from the optimizer's
own code): arithmetic with a DECIMAL operand (the optimizer types
DECIMAL loads UNDEFINED), mixed BIGINT/DOUBLE operands, kOpDiv
always, kOpMod always, and kOpDivInt with non-BIGINT operands. The
5E-2/5E-3 slices are unaffected (they target exactly the never-
rewritten opcodes).

The new bridge case therefore has one LIVE lowering win — **DECIMAL
arithmetic** (`SUM(price * qty)`, the TPC-H Q9 pattern): the bridge's
5G decimal loads type registers I64/U64/F64 by scale, richer than
the optimizer's UNDEFINED, so the generic case lowers scale>0 pairs
into F64 arith and scale-0 pairs into checked signed arith. Bridge
case as implemented: both-F64 → OP_ADD/MINUS/MUL_F64; known integer
tracks → the 5C-4 classifier verbatim; UNKNOWN / STR / mixed
int-double / u64-with-signed-variable → UNSUPPORTED (deliberately
not TYPE_MISMATCH — the kernel handles those shapes, the lowering
doesn't). All emitted ops ride the existing checked_arith_ops
overflow-target fixup. The case is also the robustness backstop for
any producer path that reaches the bridge untyped.

RonSQL platform: `RONSQL_CLI` found by mysql-test-run.pl
(my_find_bin, NOT_REQUIRED — tests skip without it). New canary
`rondb_jit_ronsql_canary`: every query runs through mysqld first
(server-side compute — the planner never pushes these shapes) as the
ground-truth differential, then through ronsql_cli under 4060 with
compile-counter deltas. Q1-Q5 are optimizer+bridge PIPELINE
regression canaries (int chains, double chains incl. the all-double
generic '/', u64+NNC, narrow INT, grouped via TEXT_NOHEADER | sort)
— nothing ever ran RonSQL under 4060 before; Q6 is the mixed
int/double NEGATIVE control (fallback counter, no 4060); Q7/Q8 are
the NEW-lowering must-JITs (DECIMAL(9,2) multiply → F64 track,
DECIMAL(9,0) add → checked signed track). Bridge tests 116/116
(T62a-f: i64 chain, f64 chain incl. '/', u64+const, mixed reject,
u64+signed-variable reject, UNKNOWN-after-embedded reject; T62g/h:
the LIVE decimal paths at both scales).

## The finding that reshaped this phase

5E was parked as "int div/mod, zero SQL demand — API/RonSQL only".
Scoping it against RonSQL (the platform that actually emits these
opcodes) surfaced something much bigger: **RonSQL emits the GENERIC
arithmetic opcodes** — `NdbAggregator::Add/Minus/Mul/Div` put
`kOpPlus`(1)/`kOpMinus`(2)/`kOpMul`(3)/`kOpDiv`(4) on the wire — and
the bridge only lowers the TYPED `kOp*Bigint`/`kOp*Double` families
the mysqld planner emits. So today **every RonSQL query with ANY
arithmetic falls back wholesale**, not just the div/mod shapes. The
generic-lowering gap dwarfs div/mod in value and comes first.

The SQL planner never pushes `/`, `DIV`, or `%` at all
(`is_pushable_arithmetic_expr` accepts only +, −, ×), which is why
the census probes read 0 ("not pushed") — mysqld cannot drive this
phase, hence RonSQL as the testing platform. Conveniently, mysqld
computing the same query server-side is a free ground-truth
differential for every RonSQL canary.

## Kernel audit (equivalence targets, InterpreterCommonOp.hpp)

- **Generic `RegPlusReg`/`RegMinusReg`/`RegMulReg`**: over two BIGINT
  registers the body IS the typed `Reg*Bigint` kernel (same
  signed/unsigned dance the 5C-4 classifier already models); over
  doubles it is the F64 stencil semantics (double op + isfinite →
  overflow). Mixed int/double converts the int inline — a conversion
  the JIT does not model → those stay unsupported.
- **`RegDivIntBigint` / `RegDivReg(is_div_int=true)` over BIGINT**
  (`kOpDivIntBigint`, `kOpDivInt`): sign-aware magnitude division =
  C truncating division. Divisor 0 → result register NULL (the SQL
  NULL). Overflow = LLONG_MIN / −1 (and mixed-signedness result
  mismatches, unreachable in uniform tracks) → ZAGG_MATH_OVERFLOW.
  Result is_unsigned = a|b.
- **`RegModReg`** (`kOpMod`): integer path = C `%` (sign follows the
  DIVIDEND; result is_unsigned = dividend's only). Divisor 0 → NULL.
  LLONG_MIN % −1 = 0 (must be an explicit guard in the stencil —
  x86-64 idiv TRAPS on that input; arm64 does not). Double path =
  fmod — out of scope (below).
- **`RegDivReg(is_div_int=false)`** (generic `kOpDiv`): converts
  BIGINT operands to double with a ±2^53 magnitude guard (violation →
  ZAGG_MATH_OVERFLOW), then double-divides: divisor 0 → NULL result,
  non-finite → overflow. All-double operands are ALREADY lowered
  (OP_DIV_F64 since 5C-2, incl. the divisor-0 per-row fallback).

The JIT has no register-NULL state, so every "result becomes NULL"
edge (divisor 0) takes the PER-ROW fallback — the exact pattern
op_div_f64 already uses (set `row_fallback`, store a harmless value,
continue; the interpreter re-runs the row with exact semantics).
Div-by-zero rows therefore CANNOT sit under 4060 — canaries probe
them as correctness-only queries.

## RonSQL as the testing platform

- `ronsql_cli` (src/ronsql, built into runtime_output_directory/bin)
  takes `--connect-string`, `-D <db>`, `-e <query>`, prints TEXT when
  not a tty — MTR `--exec` ready. Add a `RONSQL_CLI` env var in
  mysql-test-run.pl (my_find_bin, NOT_REQUIRED — the testDeadlock
  pattern).
- Canary recipe: create + fill tables via mysqld; read
  `ndbinfo.jit` counters via mysqld; arm 4060 via $NDB_MGM; run the
  aggregation via `--exec $RONSQL_CLI`; assert counter deltas; and
  run the SAME query through mysqld (which computes it server-side —
  never pushed) as the results differential.
- RonSQL programs run through the same DBTUP bridge/JIT, so
  ndbinfo.jit and 4060 observe them exactly like planner programs.
- Synergy already banked: RonSQL's `LoadColumn` emits declared column
  types, so 5H's narrow-int admission applies to RonSQL loads too.

## Slices

### 5E-1 — generic arithmetic lowering + RonSQL test platform (NO regen)

Bridge only: `BR_kOpPlus`/`BR_kOpMinus`/`BR_kOpMul` join the existing
classifiers —
- both operands proven F64 → OP_ADD_F64 / OP_MINUS_F64 / OP_MUL_F64
  (the kOpDiv-all-double precedent in the same switch arm);
- integer tracks → the 5C-4 signed/unsigned classifier verbatim →
  checked signed / u64 stencils;
- mixed int/f64, STR, or anything else → UNSUPPORTED (a missing
  conversion, not a type bug — mirrors the kOpDiv comment).

Plus the RonSQL platform: RONSQL_CLI in mysql-test-run.pl and canary
`rondb_jit_ronsql_canary` — arithmetic shapes over BIGINT, DOUBLE,
and narrow columns, 4060-armed with counter deltas + mysqld
differentials. Bridge tests: generic ops on each track + mixed
rejects.

### 5E-2 — integer DIV / MOD (regen: 4 hot stencils)

New OpKinds (append after OP_MINMAX_STR_NDB=56):
- `OP_DIV_INT_CHECKED` (57): signed divide; divisor 0 →
  row_fallback-and-continue; dividend==LLONG_MIN && divisor==−1 →
  HK_OVERFLOW_TAKE (operand d). Explicit guards BEFORE the hardware
  divide (x86 idiv trap).
- `OP_MOD_INT` (58): signed remainder; divisor 0 → row_fallback;
  LLONG_MIN % −1 → 0 via the same explicit guard. No overflow hole.
- `OP_DIV_U64` (59) / `OP_MOD_U64` (60): unsigned; divisor 0 →
  row_fallback. No overflow hole.

Bridge: `kOpDivInt` + `kOpDivIntBigint` (same lowering; result track
U64 if either operand U64 else I64 — kernel is_unsigned = a|b) and
`kOpMod` (result track = DIVIDEND's — kernel uses a_unsigned only);
NNC constants compose per the 5C-4 rules (u64 ⊕ NNC reduces to plain
u64 div/mod; i64 ⊕ NNC stays signed). f64-typed operands for
DIV-INT/MOD (double fmod / trunc-div) stay UNSUPPORTED — RonSQL's
demand is integer; document as a durable negative.

Repoint Test 27 (testJoinAggNdbApi unsupported-fallback canary) from
`kOpMod` to `kOpSetRegNull` — the bridge's documented permanently-
unsupported opcode (its durability note already anticipated this).

RonSQL canary extends: `DIV`/`%` shapes on both tracks (4060 +
differential), negative operands (truncation + sign-of-dividend),
div-by-zero rows correctness-only (per-row fallback — no 4060), and
the LLONG_MIN/−1 overflow error.

### 5E-3 — generic '/' over integer operands (regen: 1 cold-call stencil)

`OP_DIV_CONV_F64` (61): one cold call `ndb_jit_h_div_conv(s, dst,
src, packed_types)` implementing `RegDivReg(is_div_int=false)` for
any int-involved operand combination: per-operand ±2^53 conversion
guards, divisor-0 → NULL, non-finite → overflow — EVERY edge path
sets row_fallback (the interpreter re-run reproduces the exact NULL
result or ZAGG_MATH_OVERFLOW); the hot path writes the f64 quotient
bits to dst. Tracker: operands may be I64/U64/NNC/F64 in any mix;
result F64. Division is rare per-program — a cold call is the right
cost/complexity point (the conversion + three guards would bloat a
hot stencil anyway).

RonSQL canary extends: `SUM(a / b)` over integer columns 4060-armed,
a >2^53-magnitude row and a zero-divisor row as correctness-only
probes.

## Census updates (ride with the slices)

`int_div` / `int_mod` / `generic_div_int` keep expectation 0 but the
comments flip from "5E territory" to "the SQL planner does not push
division — the lowering is exercised by rondb_jit_ronsql_canary".

## Non-goals

- Mixed int/double GENERIC +/−/× (inline conversion) — no demand
  seen; would need conversion ops or a cold call.
- `kOpMod` / `kOpDivInt` over doubles (fmod, trunc-double-DIV).
- `kOpSetRegNull` — stays the permanent unsupported-op canary.

## Verification pattern

bridge_tests batteries per slice; regen for 5E-2/5E-3;
`rondb_jit_ronsql_canary` (4060 + counters + mysqld differentials);
census comment updates; full `ndb_push_agg` sweep with `--force`.
