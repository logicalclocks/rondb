# RONDB-1056 Phase 5.1 fix — signed-overflow parity with the interpreter

**Status: planning; checked stencils chosen.** Correctness defect: the JIT silently
wraps on signed overflow while `JoinAggInterpreter` returns
`ZAGG_MATH_OVERFLOW` (= 1860) and aborts the aggregation. Given identical input
rows the two paths can produce different outcomes (wrap vs. error), which
violates the JIT-must-be-equivalent-to-interpreter contract. The chosen fix is
overflow-checked arithmetic stencils; the fallback/SQL-var guardrail is not the
product path.

Branch: `RONDB-1056-compiled-interpreter`.

## The mismatch

Four arithmetic stencils currently wrap silently:

| Stencil | NDB op | Interp helper | Interp behavior on overflow |
|---|---|---|---|
| `op_add_int_int`   | `kOpPlus`       | `RegPlusReg`  | returns -1 → `ZAGG_MATH_OVERFLOW` |
| `op_minus_int_int` | `kOpMinus`      | `RegMinusReg` | returns -1 → `ZAGG_MATH_OVERFLOW` |
| `op_mul_int_int`   | `kOpMul`        | `RegMulReg`   | returns -1 → `ZAGG_MATH_OVERFLOW` |
| `op_sum_bigint`    | `kOpSumBigint`  | `SumBigint` (`JoinAggInterpreter.cpp:245`) | returns -1 → `ZAGG_MATH_OVERFLOW` |

The interpreter raises an error; the JIT produces an arbitrary
wrapped value. For sums of int64 columns at the upper end of the
type's range this divergence is reachable in practice.

## The choice

Three paths were considered, each with different cost / coverage tradeoffs:

**A. Wide admission rejection (safe-but-aggressive)**

Reject any program containing an arithmetic op (`PLUS`, `MINUS`,
`MUL`, `SUM_BIGINT`). Effectively kills the JIT for every
SUM-bearing query, including the 100% safe cases (small sums of
small values). Wrong tradeoff — the JIT loses its primary
purpose.

**B. Document mismatch + accept divergence (cheap-but-unsafe)**

Add a known-divergence note to plan.md / phase_4_implementation
docs. Keep the JIT as-is for SUM-bearing queries. Provide a
session variable to force-disable JIT for queries where overflow
behavior matters. Tests stay.

Acceptable as a temporary stance, but each phase that depends on
arithmetic correctness (Phase 5.x SUM-with-filters, Phase 6
GROUP BY) inherits the divergence risk.

**C. Overflow-checked arithmetic stencils (correct-but-expensive)**

Build replacement stencils that detect overflow inline using
hardware flags:

- arm64: `ADDS`/`SUBS`/`SMULH` set NZCV; tail-call to overflow
  handler if V (signed overflow) set.
- x86_64: `add` / `sub` / `imul` set OF; `JO` to overflow handler.

The handler sets a flag in JitState (e.g.,
`uint32_t row_overflowed`) and tail-calls to a function-return
sequence. `dbtup_jit_invoke` checks the flag after the JIT runs;
if set, return `ZAGG_MATH_OVERFLOW` upward (matching the
interpreter's exit code).

This is the correct fix. It costs four new stencils + one new
JitState field + one new HOLE_BRANCH_OVERFLOW pattern + audit
updates + extractor regen. Estimated 5-7 days. Comparable in
scope to Phase 5.0.

**Decision (2026-06-06): choose overflow-checked stencils.** This is Option B
from the latest two-option discussion, and Path C in the older trilemma below.
Do not add a user-visible "overflow safe mode" flag as the product path. The
JIT should either match the interpreter directly or not claim support for the
operation.

## Recommended path

**Stage 1 (now) — checked-stencil spike and skeleton**:
- Add the overflow result field to `JitState` and wire the
  `dbtup_jit_invoke` post-call check to return `ZAGG_MATH_OVERFLOW`.
- Add the shared overflow-exit stencil and prove the extractor can produce a
  patchable overflow branch target on x86_64 and arm64.
- Add focused host tests that force an overflow path and a non-overflow path.

**Stage 2 — full checked arithmetic coverage**:
- New JitState field: `uint32_t row_overflowed`. Zeroed per row
  by `dbtup_jit_invoke`'s memset (no extra code).
- New stencils, each with one extra branch hole pointing to a
  shared function-return-with-overflow tail:
  - `op_add_int_int_checked`
  - `op_minus_int_int_checked`
  - `op_mul_int_int_checked`
  - `op_sum_bigint_checked`
- New stencil for the overflow tail (`op_overflow_exit`): sets
  `s->row_overflowed = 1` and returns. Reachable only from the
  four checked stencils via HK_BRANCH_TAKE patches.
- Bridge always emits the `_checked` variant (no flag, no
  user-visible toggle). The unchecked variants stay in the
  source tree only for `proto_microbench` benchmarking — they
  measure the overflow-check overhead vs. the wrapping
  baseline.
- `dbtup_jit_invoke` after `entry_fn(&s)`:
  ```cpp
  if (s.row_overflowed != 0) return ZAGG_MATH_OVERFLOW;
  ```
- audit_magics gains the new HOLE_OVERFLOW_TAKE entries (one
  per checked stencil).
- Stencil regen.
-- Add an overflow canary whose expected output shows the JIT path producing
  `ZAGG_MATH_OVERFLOW` directly.

## Rejected change set — fallback guardrail

This was the previous Stage-1 proposal. It is recorded for context only and is
not the chosen implementation path.

### Files it would have touched

| File | Change |
|---|---|
| `storage/ndb/src/kernel/blocks/dbtup/jit/ndb_jit_bridge.c` | Reject `PLUS/MINUS/MUL/SUM_BIGINT` when `safe_mode_overflow_strict` flag is set in admission ctx |
| `storage/ndb/src/kernel/blocks/dbtup/DbtupJitGlue.hpp` | Add `bool overflow_safe_mode;` to `dbtup_jit_call_ctx` |
| `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp` | Read the THD-level (or block-level) flag, populate ctx field at JIT setup time |
| `sql/sys_vars.cc` | Register `ndb_join_pushdown_jit_aggregate_overflow_safe` SESSION variable |
| `storage/ndb/handler/ha_ndbcluster.cc` | Plumb the SQL variable down through the SETUP_REQ payload |
| `storage/ndb/test/jit_proto/bridge_tests.c` | T17 — admission rejects arithmetic when flag set |
| `mysql-test/suite/ndb_push_agg/t/rondb_jit_overflow_canary.test` | New canary: flag OFF → JIT wraps (record value); flag ON → interp errors |
| `storage/ndb/claude_files/compiled_interpreter/plan.md` | Note the known divergence and link to this plan + Stage 2 |

### Reproducer shape

```sql
-- Two BIGINT NOT NULL rows whose sum overflows int64.
CREATE TABLE t1 (pk BIGINT NOT NULL, c1 BIGINT NOT NULL,
                 PRIMARY KEY(pk)) ENGINE=NDB;
INSERT INTO t1 VALUES (1, 9223372036854775806),  -- INT64_MAX - 1
                      (2, 2);                     -- adds to (INT64_MAX + 1)

SET ndb_join_pushdown_aggregate=ON;

-- Flag OFF (default): JIT wraps. Result depends on the wrap.
SET ndb_join_pushdown_jit_aggregate_overflow_safe=OFF;
SELECT SUM(c1) FROM t1;        -- expected: -INT64_MAX (wraps)

-- Flag ON: bridge rejects, interp catches overflow.
SET ndb_join_pushdown_jit_aggregate_overflow_safe=ON;
--error ER_DATA_OUT_OF_RANGE
SELECT SUM(c1) FROM t1;        -- expected: ZAGG_MATH_OVERFLOW → SQL error
```

The canary records BOTH behaviors as the test's expected output.
The diff between the two is the documented divergence.

## Concrete change set — checked stencils

### New stencils (4 + 1 tail)

```c
/* overflow-tail: shared landing pad for all _checked variants */
STENCIL op_overflow_exit(JitState *s) {
  s->row_overflowed = 1u;
  return;                        /* function-return terminator */
}

/* Each checked variant carries an HK_OVERFLOW_TAKE hole that
 * patches to op_overflow_exit's address. */
STENCIL op_add_int_int_checked(JitState *s) {
  HOLE_STORE_REG(ADD_DST, s,
      HOLE_LOAD_REG(ADD_LHS, s) + HOLE_LOAD_REG(ADD_RHS, s));
  if (__builtin_add_overflow_signed(...)) {
    [[clang::musttail]] return HOLE_OVERFLOW_TAKE(s);
  }
  TAIL_NEXT(s);
}
```

(`__builtin_add_overflow` may not lower to the desired
hardware-flag pattern under copy-and-patch; the actual
implementation likely needs inline asm to ensure the flag
sequence — to be confirmed during the Stage 2 spike.)

### Hole inventory

- `HK_OVERFLOW_TAKE` — branch target hole for the four checked
  stencils' overflow path. Patches to `op_overflow_exit`'s
  emitted Op index.

### audit_magics

Add HK_OVERFLOW_TAKE recognition. Each checked stencil must have
exactly one such hole.

### JitState

```c
uint32_t row_overflowed;       /* set by op_overflow_exit; read
                                * by dbtup_jit_invoke. uint32 for
                                * single-instruction store. */
```

### dbtup_jit_invoke

```cpp
entry_fn(&s);
if (s.row_overflowed != 0) {
  /* Match the interpreter's exit path. */
  return ZAGG_MATH_OVERFLOW;
}
/* ... existing accumulator-mask gated writeback ... */
```

### Bridge

Always emit `_checked` variants. The unchecked stencils remain
in `stencils_src.c` strictly for `proto_microbench`'s
benchmark-only programs.

## Verification

| Test | Expected |
|---|---|
| `proto_microbench` arithmetic suite | new "overflow-checked" variant column added; speedup stays ≥1.5x with checked stencils |
| `rondb_jit_overflow_canary` | JIT path runs, returns error 1860 — same as interp |
| Phase 4 / 5 canaries | unchanged outputs |
| New unit test `overflow_stencil_tests` (microbench-style) | proves: checked stencils detect INT64_MAX+1 overflow; non-overflow paths produce identical results to unchecked variants byte-for-byte |

## Effort + risk

| Work | Effort | Risk |
|---|---|---|
| overflow-exit skeleton + one checked op spike | ~1-2 days | medium; confirms branch lowering and extractor support |
| full checked add/sub/mul/sum coverage | ~5-7 days | medium; spike needed to confirm flag-detect lowering on both arches; copy-and-patch + branch from arithmetic is novel for this codebase |

**Rollback**: revert the new stencils + hole kind +
JitState field as a single commit. The bridge falls back to
emitting the unchecked variants again. Stencil regen reverts
automatically.

## Sequencing

| 1 | Spike: confirm flag-detect lowering on arm64 (ADDS/V flag) and x86_64 (add/JO). Land overflow_exit stencil + JitState field. |
| 2 | Implement the 4 _checked stencils (one stencil per session). Run extractor; confirm bytes contain expected BR/JO instructions. |
| 3 | audit_magics: HK_OVERFLOW_TAKE recognition + per-checked-stencil hole count check. |
| 4 | Bridge: switch emission to _checked variants. dbtup_jit_invoke: ZAGG_MATH_OVERFLOW return. |
| 5 | Verification: rondb_jit_overflow_canary updated to expect error 1860 from JIT path. Add overflow_stencil_tests microbench. Re-run full canary set. |
| 6 | Cleanup docs and benchmarking split: unchecked variants remain benchmark-only, checked variants are the bridge default. |
| 7 | Buffer for arch-specific debugging (likely arm64 imm hole interactions). |

## Out of scope

- **Unsigned arithmetic overflow**: separate code path in
  `SumBigint` for `is_unsigned`. JIT currently treats all
  bigints as signed. When unsigned support lands, mirror the
  interp's overflow-detection logic.
- **Float / double overflow**: not currently JIT'd.
- **Division-by-zero**: separate concern (currently no `op_div`
  stencil).
- **Decimal type arithmetic**: NDB_TYPE_DECIMAL has its own path
  in the interpreter; not JIT'd.
