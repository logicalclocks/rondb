# RONDB-1056 Phase 4.7 — addressing-mode fold + narrow LoadConst variants

**Status: shipped.** Phase 4.7 lands two complementary aarch64
optimisations:

1. **imm12 fold for LDR/STR.** Replace the
   `movz wN, #idx; ldr/str xT, [x20, xN, lsl #3]` two-instruction
   register-file access pattern with a single
   `ldr/str xT, [x20, #(idx*8)]` whose imm12 field is patched at
   JIT compile time. Eliminates one MOVZ per memory access.
2. **Narrow LoadConst variants.** Add four bridge-routed
   stencils — `op_load_const_uint16/int16/uint32/int32` — so the
   bridge picks the smallest-fitting stencil per literal range.
   Most NDB query literals are small non-negative integers and
   route to the 8 B `uint16` form (vs the 20 B int64 form
   today).

x86_64 stencils unchanged (their 32-bit relocation-driven
operand sites are already optimal). Branch:
`RONDB-1056-compiled-interpreter`.

## Outcome

All Phase 4.7 verification gates clear on aarch64 (macOS prod_build):

| Gate | Result |
|---|---|
| `regen-stencils` audit (39 magics: 3 wide + 4 narrow + 32 fold) | PASS |
| `proto_smoke` arena/JIT smoke | PASS |
| `admission_tests` 12/12 | PASS |
| `bridge_tests` 10/10 | PASS |
| `coldcall_tests` 5/5 | PASS |
| `proto_microbench` 30-op JIT == interp aggregate | 12,517,131 |
| `proto_microbench` forked-program JIT == interp | 7,250,995 |
| Phase 1 microbench VERDICT (all 3 thresholds) | PASS |
| `drift_check.sh` (committed headers == regen output) | PASS |
| `rondb_jit_canary` MTR | PASS (253 ms) |

## Headline numbers

**Per-stencil byte savings** (post-strip, aarch64):

| Stencil | Phase 4.6 (shipped) | Phase 4.7 | Δ |
|---|---:|---:|---:|
| `op_mov_int_int`        | 16 |  **8** | −50 % |
| `op_load_col_int`       | 20 | **12** | −40 % |
| `op_load_const_int`     | 24 | **20** | −17 % |
| `op_add_int_int`        | 28 | **16** | −43 % |
| `op_minus_int_int`      | 28 | **16** | −43 % |
| `op_mul_int_int`        | 28 | **16** | −43 % |
| `op_sum_bigint`         | 28 | **20** | −29 % |
| `op_branch_*_int_int`   | 32 | **24** | −25 % (× 6) |
| `op_load_col_ndb`       | 28 | **28** | (unchanged — helper-arg) |
| **Existing-set total**  | 408 | **296** | **−27 %** |

Plus 4 new LoadConst variants, bridge-routed by literal range:

| New stencil | Size | Range |
|---|---:|---|
| `op_load_const_uint16`  |  **8** | `[0, 65535]` (common case) |
| `op_load_const_int16`   | 12 | `[-32768, -1]` |
| `op_load_const_uint32`  | 12 | `[0, 2³²-1]` |
| `op_load_const_int32`   | 16 | negative int32 (the gap) |

**Cumulative trajectory** (existing 14-stencil set):

| Phase | Bytes |
|---|---:|
| 4 baseline      | 1112 |
| 4.5 narrow      |  748 |
| 4.6 inline asm  |  408 |
| **4.7 fold**    | **296** |

That's **−73 % off the Phase 4 baseline** for the existing set,
and a further saving on small-literal LoadConst (60 % on
positive uint16 values vs the int64 fallback).

**End-to-end microbench impact** (30-op program, 100k rows,
aarch64 prod_build; same program as in Phase 4.6 — no LoadConst
in this synthetic mix):

| Metric | Phase 4.6 | Phase 4.7 |
|---|---:|---:|
| Emitted bytes | 684 | **436** |
| JIT ns/row (median) | 10.1 | **~8** |
| Speedup vs interp | 2.13× | **2.52×** |
| Break-even rows | 355 | **316** |

The microbench doesn't go through `ndb_jit_bridge` (it builds
Programs directly), so the new LoadConst dispatch isn't
exercised here. Real-world queries route small literals to the
new variants and shrink further.

## What changed (commits)

| Day | Work | Commit |
|---|---|---|
| 0 | Investigation spike (6 ★ items + 2 uint variants) | `0185e7cf8f9` |
| 1 | imm12 fold infrastructure (magics + helpers + extractor + audit + patcher) | `4337cc0af5e` |
| 2 | Migrate 13 stencils to fold pattern | `157c1ae877f` |
| 3 | Narrow LoadConst variants (uint16, int16) + bridge dispatch | `1969ec854f6` |
| 4 | Wider LoadConst variants (uint32, int32) + 2-slot chain detection | `d224f4b71f2` |
| 5 | This results doc + Phase 4.7 → shipped in plan.md | (this) |

**Modified core**:

- `hole_kinds.h` — 34 new 12-bit fold magics (sha256-derived,
  collision-checked) + `kHoleFoldMagicTable[]` + new fold
  symbol-table entries. `HoleMagicEntry` gained a `chain_len`
  field. 4 new narrow magic entries (LCx16/LCx32_VAL).
- `stencils_src.c` — three helper families (`aarch64_load_reg_`/
  `_acc_`/`_col_` + their store counterparts) using inline asm
  with `"n"` constraints, the destination base pointer matching
  the JitState array. `aarch64_hole32_` for the 2-slot W-form
  chain. New macros: `HOLE_LOAD_REG`/`STORE_REG`/
  `LOAD_ACC`/`STORE_ACC`/`LOAD_COL`/`HOLE_32` (aarch64) +
  parallel x86_64 fallbacks.
- 13 existing stencils flipped from `regs_i64[HOLE_NARROW(...)]`
  to `HOLE_LOAD_REG`/`STORE_REG`. 4 new LoadConst stencils.
- `extract_stencils.c` — new pass-4 LDR/STR imm12 fold detector
  (mask `0xFFC00000` matching `0xF9400000` LDR / `0xF9000000`
  STR; imm12 at bits 21..10). Pass-2 chain detector relaxed to
  sf-agnostic (mask `0x7F800000`) + `chain_len`-aware match
  (`0xF` for X-form 64-bit chain; `0x3` for W-form 32-bit
  chain).
- `audit_magics.c` — new `count_fold_matches_arm64` +
  `kFoldMagicToStencil[]` table (with `expected_count` per
  entry — `SUM_SLOT_FOLD = 2` for the load+store accumulator
  pattern). Chain detector mirrors the new sf-agnostic +
  `chain_len`-aware logic.
- `jit1.c` — `width=1` patcher branch: writes bits 21..10
  (`mask 0xFFFu << 10`, value `<< 10`) of the LDR/STR
  instruction. `width=2` narrow MOVZ and `width=4` wide chain
  paths unchanged.
- `bytecode1.h` — 4 new OpKinds: `OP_LOAD_CONST_UINT16=17`,
  `OP_LOAD_CONST_INT16=18`, `OP_LOAD_CONST_UINT32=19`,
  `OP_LOAD_CONST_INT32=20`. Append-only invariant preserved.
- `ndb_jit_bridge.c` — `BR_kOpLoadConst` dispatches to the
  smallest-fitting variant by inspecting the constant value at
  admission time.
- `MAX_STENCILS` bumped 16 → 24 in extractor + audit (we have
  20 stencils now).

**Re-generated**: both `stencils_arm64.h` and `stencils_x86_64.h`
(the latter only because the new 4 LoadConst stencils are
emitted there too — existing x86 stencils byte-identical apart
from `op_sum_bigint` being a hole-order shuffle from the source
restructuring, no functional change).

## Encoding details

### imm12 fold — LDR/STR (immediate, X-form)

```c
__attribute__((always_inline))
static inline int64_t aarch64_load_reg_(uint32_t magic_byte_off,
                                         const JitState *state) {
  int64_t v;
  __asm__ volatile (
    "ldr %[out], [%[base], %[off]]"
    : [out] "=r" (v)
    : [base] "r"  (state),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
  );
  return v;
}
```

Three base-pointer variants:
- `aarch64_load_reg_` — base = state pointer (regs_i64 at
  offset 0 of JitState).
- `aarch64_load_acc_` — base = `state->acc_i64` (clang emits
  `add x_base, x20, #64` once per stencil).
- `aarch64_load_col_` — base = `state->row_cols_i64` (a pointer
  member; clang loads it via `ldr x_base, [x20, #96]`).

Plus matching `_store_*` variants for writes.

### Patcher (`jit1.c`)

```c
case HK_OP_A: case HK_OP_B: case HK_OP_C: case HK_OP_IMM: {
  int64_t v = hole_value_from_op(hole->kind, op);
#if defined(__aarch64__)
  if (hole->width == 1) {
    /* Phase 4.7 imm12 fold (bits 21..10). */
    rmw_insn_word(patch,
                  (uint32_t)0xFFFu << 10,
                  ((uint32_t)(int32_t)v & 0xFFFu) << 10);
  } else if (hole->width == 2) {
    patch_operand(patch, 0, v);          /* narrow MOVZ */
  } else {
    uint8_t slot = slot_counter[hole->kind]++;
    patch_operand(patch, slot, v);        /* wide chain */
  }
#else
  ...
#endif
  break;
}
```

### sf-agnostic 2-slot chain detection

The Phase 4.7 32-bit LoadConst chains use **W-form** MOVZ+MOVK
(encoding prefix `0x52800000` / `0x72800000`). The original
4-slot wide chain uses **X-form** (`0xD2800000` / `0xF2800000`).
Mask `0x7F800000` ignores the sf bit so both forms register;
the `chain_len` field per magic decides whether the detector
matches at `slot_seen == 0xF` (4 slots) or `slot_seen == 0x3`
(2 slots).

### Bridge dispatch (`ndb_jit_bridge.c`)

```c
OpKind kind;
if      (value >= 0   && value <= 0xFFFFLL)        kind = OP_LOAD_CONST_UINT16;
else if (value >= -32768 && value <= -1)           kind = OP_LOAD_CONST_INT16;
else if (value >= 0   && value <= 0xFFFFFFFFLL)    kind = OP_LOAD_CONST_UINT32;
else if ((int64_t)(int32_t)value == value)        kind = OP_LOAD_CONST_INT32;
else                                              kind = OP_LOAD_CONST_INT;
```

Smallest-fitting first so the common case (small positive
literal → 8 B `UINT16`) exits early.

## What didn't change

- **Engine patching for existing kinds** — `width=4` wide chain
  and `width=2` narrow MOVZ paths byte-compatible with Phase
  4.6.
- **op_load_col_ndb** — kept on narrow-MOVZ for LCN_COL/LCN_DST
  because they're scalar arguments to a `bl` helper call, not
  array indices. The fold doesn't apply.
- **x86_64 stencils** — relocation-driven operand sites already
  use a 32-bit immediate per hole; no compaction available.
  The 4 new LoadConst stencils land on x86_64 with the same
  `extern uint64_t HOLE_*` pattern as the rest.
- **Bridge / admission for non-LoadConst opcodes** — same as
  Phase 4.

## What we learned

1. **The narrow-MOVZ pattern was a rest stop, not the destination.**
   Phase 4.5 saved 12 B per register-index operand by collapsing
   a 16-byte wide chain to a 4-byte MOVZ. Phase 4.7 saves
   another 4 B per operand by folding the MOVZ entirely into
   the LDR/STR instruction. The narrow magic table for register
   indices effectively goes away — only the LCN_*/LCx16_VAL
   narrow holes remain.

2. **A single fold magic family covers regs_i64, acc_i64, and
   row_cols_i64.** The instruction pattern is identical
   (`ldr/str xT, [x_base, #imm12]`); only the base register
   computation differs. clang's CSE handles repeated base
   computations within a stencil naturally.

3. **`SUM_SLOT_FOLD` appears 2× in `op_sum_bigint`.** The
   accumulator-update pattern reads and writes the same slot,
   so the fold magic is patched into both the LDR's and the
   STR's imm12 — both with the same `op->a` value. The audit's
   `expected_count` field per kFoldMagicToStencil entry handles
   this elegantly.

4. **2-slot W-form chains required sf-agnostic mask
   relaxation.** This is the same fix family as Phase 4.5
   needed for narrow MOVZ — the `volatile uint32_t` /
   `volatile uint64_t` pattern lowers to W-form when the type
   fits in a w-register, X-form otherwise. Mask `0x7F800000`
   accepts both.

5. **Cumulative gain compounds.** Phase 4.5 (narrow): −33%.
   Phase 4.6 (inline asm): −47%. Phase 4.7 (fold): −27% on
   existing stencils + new variants for small literals. Each
   pass shaved a different cause of bloat: chain length →
   spill/reload → indexed addressing → variant dispatch.

6. **Stale-build hazard on `make ndbmtd` after stencil regen.**
   Discovered during Day 4 verification: the canary MTR failed
   with SIGBUS until a clean rebuild propagated the new
   stencils into ndbmtd. The CMake dependency graph doesn't
   pull stencils_arm64.h through to all consumers. A clean
   rebuild after `regen-stencils` is the safe workflow.

## Out of scope (for Phase 5+)

- **Fold for op_load_col_ndb.** The `bl` argument-passing path
  is fundamentally different from array indexing. Could be
  done via a register-pre-load pattern but the savings are
  small (4 B per stencil) compared to Phase 5 work.
- **`op_load_const_double` / float arithmetic.** Phase 5
  typed-register work.
- **Cold-call branch family + embedded normal-interp
  fallback.** Phase 5.

## References

- Phase 4.7 implementation plan: `phase_4_7_implementation.md`.
- Phase 4.6 results doc: `phase_4_6_inline_asm.md`.
- Phase 4.5 results doc: `phase_4_5_narrow_holes.md`.
- AArch64 ARM ARM, section C6.2.119 (LDR immediate, unsigned
  offset) — for the imm12 encoding details.
- Stencils header (regenerated baseline):
  `storage/ndb/src/kernel/blocks/dbtup/jit/stencils_arm64.h`.
- Engine patcher (`width == 1` branch):
  `storage/ndb/src/kernel/blocks/dbtup/jit/jit1.c`.
- Bridge dispatch (`BR_kOpLoadConst`):
  `storage/ndb/src/kernel/blocks/dbtup/jit/ndb_jit_bridge.c`.
