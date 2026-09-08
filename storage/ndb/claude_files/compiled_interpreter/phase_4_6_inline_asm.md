# RONDB-1056 Phase 4.6 — inline-asm immediate constraint (eliminate spill/reload)

**Status: shipped.** Phase 4.6 replaces the Phase 4.5
`volatile uint{16,64}_t v = magic; return v;` codegen pattern
with `__asm__ volatile` carrying the `"n"` immediate constraint.
The MOVZ chain bytes themselves are byte-identical to Phase 4.5;
what disappears is the volatile-forced spill/reload around each
hole and the `sub sp / add sp` stack-frame pair that exists only
to back the spill slot. Aarch64-only; x86_64 stencils unchanged.
Branch: `RONDB-1056-compiled-interpreter`.

## Outcome

All Phase 4.6 verification gates clear on aarch64 (macOS prod_build):

| Gate | Result |
|---|---|
| `regen-stencils` audit (1 wide LCI_VAL + 30 narrow magics) | PASS |
| `proto_smoke` arena/JIT smoke | PASS |
| `admission_tests` 12/12 | PASS |
| `bridge_tests` 10/10 | PASS |
| `coldcall_tests` 5/5 | PASS |
| `proto_microbench` 30-op JIT == interp aggregate | 12,517,131 |
| `proto_microbench` forked-program JIT == interp | 7,250,995 |
| **Phase 1 microbench VERDICT (all 3 thresholds)** | **PASS** |
| `drift_check.sh` (committed headers == regen output) | PASS |
| `rondb_jit_canary` MTR | PASS (242 ms) |

The Phase 1 microbench's three thresholds (speedup ≥ 1.50×,
warm compile < 15 µs, break-even < 5000 rows) **all pass for
the first time** since the project began. Phase 4.5 hit only the
warm-compile gate; Phase 4.6 clears all three.

## Headline numbers

**Per-stencil byte savings** (post-strip, aarch64):

| Stencil | Phase 4 | Phase 4.5 | Phase 4.6 | Δ vs P4.5 |
|---|---:|---:|---:|---:|
| `op_load_const_int`     |  60 | 44 | 24 | −45 % |
| `op_load_col_int`       |  68 | 44 | 20 | −55 % |
| `op_mov_int_int`        |  64 | 40 | 16 | −60 % |
| `op_add_int_int`        |  96 | 60 | 28 | −53 % |
| `op_minus_int_int`      |  96 | 60 | 28 | −53 % |
| `op_mul_int_int`        |  96 | 60 | 28 | −53 % |
| `op_sum_bigint`         |  76 | 52 | 28 | −46 % |
| `op_branch_lt_int_int`  |  80 | 56 | 32 | −43 % |
| `op_branch_le_int_int`  |  80 | 56 | 32 | −43 % |
| `op_branch_eq_int_int`  |  80 | 56 | 32 | −43 % |
| `op_branch_gt_int_int`  |  80 | 56 | 32 | −43 % |
| `op_branch_ge_int_int`  |  80 | 56 | 32 | −43 % |
| `op_branch_ne_int_int`  |  80 | 56 | 32 | −43 % |
| `op_load_col_ndb`       |  76 | 52 | 28 | −46 % |
| **Stencil-set total**   |**1112**|**748**|**392**|**−47 %**|

`op_skip` and `op_exit` are unchanged (8 B each, no holes).

**Cumulative vs Phase 4 baseline**: 1112 → 392 = **−720 bytes
(−65 %)** across the stencil set. Roughly 3× smaller code.

**End-to-end microbench impact** (30-op program, 100k rows,
aarch64 prod_build):

| Metric | Phase 4 | Phase 4.5 | Phase 4.6 |
|---|---:|---:|---:|
| Emitted bytes | 2164 | 1412 | **684** |
| Interpreter ns/row | 21.8 | 21.5 | 21.5 |
| JIT ns/row (median) | 22.5 | 24.8 | **10.1** |
| **Speedup vs interp** | 0.97× | 0.85× | **2.13×** |
| Cold first-compile | 19.8 µs | 5.2 µs | ~5 µs (variance) |
| Warm-median compile | 4.5 µs | 4.5 µs | 4.0 µs |
| **Break-even rows** | 1 B | 1 B | **355** |

The per-row dispatch number is the unexpected win: Phase 4.5's
24.8 ns/row fell to 10.1 ns/row. The mechanism is **icache
density** — Phase 4.5 stencils carried a 4-instruction
spill/reload + 2-instruction prologue/epilogue around each hole,
all of which executed at row dispatch time. Removing those 6
extra instructions per row (across the typical 2-3 holes per op
× 30 ops = ~90 fewer instructions per row) is what cut row time
in half.

## What changed (commits)

| Day | Work | Commit |
|---|---|---|
| 0 | Investigation spike + plan resolution notes | `3dd9c3f739c` |
| 1 | Inline-asm helpers + regenerated stencils | `005e9f0be70` |
| 2 | Verification (no source changes — just tests) | — |
| 4 | This results doc + Phase 4.6 → shipped in `plan.md` | (this) |

Day 3 was originally scoped for "investigate any stencil whose
codegen surprised the spike." Nothing surprised; skipped.

**Modified core**: a single function pair in `stencils_src.c`.
Two helpers — `aarch64_hole_(uint64_t)` and
`aarch64_hole_narrow_(uint32_t)` — swap their `volatile` body
for `__asm__ volatile` with a `"n"` immediate constraint. Every
stencil that calls `HOLE()` / `HOLE_NARROW()` inherits the new
codegen automatically. Zero stencil bodies touched.

**Re-generated**: `stencils_arm64.h`. `stencils_x86_64.h`
unchanged (verified by `regen-stencils` and `drift_check.sh`).

## Encoding details

### Wide hole (64-bit, single occurrence: `MAGIC_LCI_VAL`)

```c
__attribute__((always_inline))
static inline uint64_t aarch64_hole_(uint64_t magic) {
  uint64_t v;
  __asm__ volatile (
    "movz %[out], %[a]\n\t"
    "movk %[out], %[b], lsl #16\n\t"
    "movk %[out], %[c], lsl #32\n\t"
    "movk %[out], %[d], lsl #48"
    : [out] "=r" (v)
    : [a] "n" ( magic        & 0xFFFFu),
      [b] "n" ((magic >> 16) & 0xFFFFu),
      [c] "n" ((magic >> 32) & 0xFFFFu),
      [d] "n" ((magic >> 48) & 0xFFFFu)
  );
  return v;
}
```

Emits four contiguous instructions (MOVZ + 3×MOVK) targeting
the same Rd. The chain is structurally guaranteed contiguous
because it's a single asm block. Bytes byte-identical to Phase
4.5's wide-chain emission.

### Narrow hole (16-bit, used 30× across the stencil set)

```c
__attribute__((always_inline))
static inline uint64_t aarch64_hole_narrow_(uint32_t magic) {
  uint64_t v;
  __asm__ volatile (
    "movz %w[out], %[m]"
    : [out] "=r" (v)
    : [m]   "n"  (magic & 0xFFFFu)
  );
  return v;
}
```

Emits a single 32-bit MOVZ (`0x52800000` encoding family) into a
W-register. The X-register's upper 32 bits are zero by AArch64
spec. Bytes byte-identical to Phase 4.5's narrow MOVZ emission.

### Two adjustments resolved during the Day 0 spike

1. **Magic must be masked `& 0xFFFFu` in the constraint
   expression.** Without it, clang's asm formatter prints the
   value as a signed integer when the high 16-bit bit is set
   (e.g., `0xfc24` → `-988`) and rejects it with
   *"immediate must be an integer in range \[0, 65535\]"*. The
   wide chain already does this masking per slice; the narrow
   helper now matches.

2. **Narrow helper returns `uint64_t`, not `uint16_t`.** Returning
   `uint16_t` made clang emit a redundant `and rN, rN, #0xffff`
   after each MOVZ to "honor" the 16-bit return type, even
   though the W-form MOVZ already zero-fills the upper bits.
   Returning `uint64_t` lets clang trust the zero-extension.
   The narrow parameter type also widened to `uint32_t` so the
   masking expression compiles cleanly.

The plan in `phase_4_6_implementation.md` captures these as
permanent design notes for future stencil work.

## What didn't change

- **Engine patcher** — `width=2` narrow path and `slot_counter`
  wide path are byte-compatible with Phase 4.5 holes. The
  patcher branches on `hole->width` and writes the same imm16
  field of the same instruction encoding.
- **Extractor** — pass-2 wide-chain detector and pass-3 narrow
  walk both find the same instruction sequences. Hole offsets
  shift (because the surrounding bytes shrank), but
  Hole-relative-to-stencil offsets are still emitted correctly.
- **Audit** — `count_chain_matches_arm64` and
  `count_narrow_matches_arm64` both unchanged. Both detect 1×
  per declaring stencil + 0× elsewhere as before.
- **`hole_kinds.h`, bridge, admission, `JitBridgeReason`** —
  invisible to all of them.
- **x86_64 stencils** — codegen produces the same `mov reg32,
  imm32` operand sites it always did. The `extern uint64_t
  HOLE_*` relocation pattern doesn't have a spill/reload to
  remove.

## What we learned

1. **`volatile` was a sledgehammer for "don't constant-fold
   this."** The C standard's volatile semantics demand a memory
   round-trip; clang has no choice but to emit STR/LDR. Inline
   asm with `volatile` on the asm statement gives us exactly the
   "side effect, don't reorder" guarantee we wanted, without the
   memory round-trip. Lesson: in copy-and-patch JIT codegen,
   reach for inline asm before `volatile`.

2. **The cold-compile bottleneck is emit-byte-bound.** Phase 4.5
   cut emit bytes by 35 % and cold compile dropped 3.4×. Phase
   4.6 cut another 50 % off bytes; cold compile floor is now
   variance-bound (page-fault and JIT-write toggles dominate the
   ~5 µs floor on macOS).

3. **The per-row dispatch bottleneck was *also* emit-byte-bound,
   indirectly.** We had assumed Phase 4 / 4.5's per-row time was
   limited by the actual operations (loads, stores, branches).
   It wasn't — the spill/reload + prologue/epilogue around each
   hole executed at *runtime* on every row. Removing those 6
   instructions per hole × ~75 hole-bearing instructions in the
   30-op test program bought us back the 2× speedup.

4. **The `"n"` constraint with explicit masking is the right
   primitive.** Cleaner than `extern uint64_t HOLE_*` (which
   only works because x86_64 has rip-relative `mov reg, imm32`
   with relocations), cleaner than `volatile`, and arch-portable
   in principle. Future Phase 5 stencils should adopt this
   pattern from the start.

## Out of scope (for Phase 5+)

- **Per-stencil-family addressing-mode folding.** Now that the
  stencils are dense, the AArch64 indexed addressing modes
  (`ldr Xt, [Xn, Wm, sxtw #3]`) become tractable as a Phase-5
  optimisation. Not Phase 4.6's job.
- **x86_64 inline asm.** Not needed — already optimal there.
- **Embedded normal-interp branches and the cold-call branch
  pattern.** Phase 5.

## References

- Phase 4.6 implementation plan: `phase_4_6_implementation.md`.
- Phase 4.5 results doc: `phase_4_5_narrow_holes.md`.
- Phase 4 results doc: `phase_4_setup_integration.md`.
- Stencils header (regenerated baseline):
  `storage/ndb/src/kernel/blocks/dbtup/jit/stencils_arm64.h`.
- Helper definitions:
  `storage/ndb/src/kernel/blocks/dbtup/jit/stencils_src.c`
  (lines 110-160).
