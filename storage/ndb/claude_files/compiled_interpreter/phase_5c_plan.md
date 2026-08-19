# Phase 5C — DOUBLE family + unsigned BIGINT (and why the lattice shrank)

**Status: PLANNED (2026-08-19). Code: not started.**

## Headline finding: the full type-state lattice is NOT needed

The original Phase 5 design (`phase_5_implementation.md` §3) called for
a forward-dataflow lattice with join meets inside the JIT. That design
predates a structural fact we established during 5B:
**`PushdownInterpreter::OptimizeProgramBuffer` already type-specializes
the outer bytecode on BOTH compile paths** (in
`PushdownInterpreterFactory::Create` for standalone/scan, and in
`DblqhProxy` before the join-agg compile). The bridge never sees
generic `kOpSum`/`kOpMin`/`kOpPlus` for typed tracks — it sees
`kOp*Bigint` / `kOp*Double`, and loads carry explicit type fields.

What the bridge does need is a **linear register-type tracker**:
`reg_type[8] ∈ {i64, f64, unknown}`, set by loads (from their explicit
type fields) and constants, consumed to validate each typed op's
operands (defense-in-depth against optimizer bugs → reject, fall back),
and **invalidated at embedded blocks** — mirroring the optimizer's own
convention (it skips embedded blocks entirely: `exec_pos += emb_len`;
the planner re-loads outer registers after embedded blocks). No join
meets: no current emitter produces cross-branch register flows in the
outer stream. The full lattice is deferred until an emitter does.

## Key semantics audited (2026-08-19)

- **`SumDouble`** (`AggInterpreterBase.cpp:1108`): first-row-initialize
  (same as MIN/MAX → needs the 5B `value_initialized` mask!) and an
  `isfinite` check on every accumulate — non-finite ⇒
  `ZAGG_MATH_OVERFLOW`. So the JIT double-SUM needs BOTH the init mask
  and an overflow branch (d-hole → `OP_OVERFLOW_EXIT`, like checked
  BIGINT SUM).
- **`RegDivDouble`** (`InterpreterCommonOp.hpp:1089`): divisor == 0.0
  ⇒ the RESULT REGISTER becomes NULL (SQL semantics), and downstream
  kernels skip null inputs. The JIT has no register null-tracking
  until 5D — solved WITHOUT 5D by a new terminator:
  **`OP_ROW_FALLBACK_EXIT`** (sets `JitState::row_fallback = 1` and
  returns). The div stencil branches there on divisor == 0; the glue
  discards the row and the interpreter re-runs it — exact
  null-register semantics per row. Non-finite results branch to
  `OP_OVERFLOW_EXIT` (⇒ ZAGG_MATH_OVERFLOW), matching the kernel.
  This terminator also gives every FUTURE hot stencil a per-row escape
  hatch (the cold-call helpers already have one via the field).
- **`kOpLoadConst`** carries BIGINT / BIGUNSIGNED / DOUBLE (2 raw value
  words). Double constants need NO new stencil: bits are bits —
  `OP_LOAD_CONST_INT`'s imm64 hole carries the double's bit pattern
  (bridge_tests T9's double-const reject flips to accept).
- **Storage decision**: f64 values live bit-cast in the existing
  `regs_i64` / `acc_i64` arrays. Copy-in already bit-copies the
  `AggResItem` value union, so it is type-agnostic as-is. Writeback
  needs a new **`value_double[BC_MAX_ACCS]`** mask (the
  `value_unsigned` pattern): double-agg stencils mark it; the glue sets
  `type = NDB_TYPE_DOUBLE` + `val_double` for marked results.

## Slices

### 5C-1 — bridge register-type tracker (no regen)

`reg_type[8]` in the outer translate walk: set by `kOpLoadCol` /
`kOpLoadConst` type fields and `kOpMov`; embedded blocks invalidate all
(5A's READ_ATTR / LOAD_CONST64 write registers invisible to the outer
tracker — same convention as the optimizer). Typed ops verify operand
types and reject on mismatch (JIT_BRIDGE_NON_BIGINT reason reused or a
new TYPE_MISMATCH). Pure hardening + the foundation 5C-2/3 hang off;
behavior-neutral for all currently admitted programs (i64-only).

### 5C-2 — the DOUBLE family (regen, ~9 stencils)

- `OP_LOAD_COL_NDB_F64` (cold-call, new helper `ndb_jit_h_load_col_f64`):
  DOUBLE columns load their bits; FLOAT columns promote to double
  (mirroring `handleReadAttrIntoReg`'s FLOAT case); anything else ⇒
  `row_fallback`. NULL ⇒ `row_fallback` (as in 5A).
- Double constants → existing `OP_LOAD_CONST_INT` (bit pattern).
- `OP_ADD_F64` / `OP_MINUS_F64` / `OP_MUL_F64`: fadd/fsub/fmul +
  isfinite check → `OP_OVERFLOW_EXIT` (audit kOpPlusDouble et al.
  kernels in-slice to confirm the isfinite ⇒ overflow pattern).
- `OP_DIV_F64`: divisor == 0 → `OP_ROW_FALLBACK_EXIT`; non-finite →
  `OP_OVERFLOW_EXIT`.
- `OP_SUM_F64`: `value_initialized` + isfinite → overflow branch;
  marks `value_updated` + `value_double`.
- `OP_MIN_F64` / `OP_MAX_F64`: 5B's shape with double compares (plain
  `<`/`>`, matching the kernel; NaN can't reach them — loads/arith
  guarantee finite).
- New `OP_ROW_FALLBACK_EXIT` terminator stencil (shared).
- JitState: append `value_double[BC_MAX_ACCS]`; glue writeback branch.
- Bridge: `kOp{Sum,Min,Max,Plus,Minus,Mul,Div}Double` + double-typed
  loads/consts, validated by the 5C-1 tracker.
- In-slice audits: AVG's pushed decomposition (the ndb_push_agg AVG
  tests — what ops does `AVG(val * 2)` actually emit?); DECIMAL
  constants (the planner claims to push them — as doubles?); the
  double arithmetic kernels' exact isfinite/null behavior.
- Canaries: double-column SUM/MIN/MAX grouped under 4060 + counter
  delta; a div-by-zero row (⇒ per-row fallback ⇒ SQL NULL semantics)
  differential; AVG queries flipping from fallback to JIT; the
  existing ta1/ta2 AVG tests in the full sweep as the regression net.

### 5C-3 — unsigned BIGINT (regen, ~4 stencils)

Outer aggregation over BIGUNSIGNED columns: unsigned MIN/MAX (u64
compares), checked unsigned SUM (carry check), unsigned load admission
(type field BIGUNSIGNED → a load variant that stores the bits and —
design detail in-slice — distinguishes itself from the signed
contract; the embedded READ_ATTR keeps its BIGUNSIGNED per-row
fallback). Writeback via the existing `value_unsigned` mask. Embedded
unsigned comparisons stay on the interpreter (rare; needs unsigned
REG_REG stencils — defer until fallback data demands it).

## Non-goals (explicit)

- Register null-tracking (5D) — `OP_ROW_FALLBACK_EXIT` covers 5C's
  needs per row.
- Embedded DOUBLE conditions (CASE WHEN double_col …): READ_ATTR's
  FLOAT/DOUBLE per-row fallback stays; revisit with 5D or when the
  fallback counters show demand.
- DECIMAL as a first-class type; FLOAT beyond promote-on-load.
- The join-meet lattice — only when an emitter produces cross-branch
  register flows.

## Verification pattern (per slice)

bridge_tests lowering/reject cases; coldcall execution cases (incl.
bit-pattern round-trips and the div-by-zero fallback exit); MTR canary
with 4060 + `ndbinfo.jit` counter delta; **full `ndb_push_agg` sweep
with `--force` to completion** — the AVG/double tests silently start
JITting and are the real net.
