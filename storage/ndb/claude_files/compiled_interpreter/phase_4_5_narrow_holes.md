# RONDB-1056 Phase 4.5 — narrow-hole encoding (aarch64)

**Status: shipped.** Phase 4.5 collapses the wide 4-instruction
movz/movk chain used for register-index and column-id holes on
aarch64 down to a single MOVZ instruction (16-bit immediate),
saving 12 bytes per such hole. Targeted at aarch64 only — x86_64
keeps its existing `mov reg32, imm32` pattern. Branch:
`RONDB-1056-compiled-interpreter`.

## Outcome

All Phase 4.5 verification gates clear on aarch64 (macOS prod_build):

| Gate | Result |
|---|---|
| `regen-stencils` audit (1 wide LCI_VAL + 30 narrow magics) | PASS |
| `proto_smoke` arena/JIT smoke | PASS |
| `admission_tests` 12/12 | PASS |
| `bridge_tests` 10/10 | PASS |
| `coldcall_tests` 5/5 | PASS |
| `proto_microbench` 30-op JIT == interp aggregate | 12,517,131 |
| `proto_microbench` forked-program JIT == interp | 7,250,995 |
| `drift_check.sh` (committed headers == regen output) | PASS |
| `rondb_jit_canary` MTR | PASS |

x86_64 stencil bytes unchanged — the narrow encoding is a
codegen-level no-op outside aarch64, since `volatile uint16_t`
already lowers to `mov r32, imm16` there.

## What shipped

**aarch64 byte savings (cumulative, across 14 stencils):**

| Stencil | Before | After | Saved |
|---|---|---|---|
| `op_load_const_int` | 60 | 44 | 16 |
| `op_load_col_int` | 68 | 44 | 24 |
| `op_mov_int_int` | 64 | 40 | 24 |
| `op_add_int_int` | 96 | 60 | 36 |
| `op_minus_int_int` | 96 | 60 | 36 |
| `op_mul_int_int` | 96 | 60 | 36 |
| `op_sum_bigint` | 76 | 52 | 24 |
| `op_branch_lt_int_int` | 80 | 56 | 24 |
| `op_branch_le_int_int` | 80 | 56 | 24 |
| `op_branch_eq_int_int` | 80 | 56 | 24 |
| `op_branch_gt_int_int` | 80 | 56 | 24 |
| `op_branch_ge_int_int` | 80 | 56 | 24 |
| `op_branch_ne_int_int` | 80 | 56 | 24 |
| `op_load_col_ndb` | 76 | 52 | 24 |
| | | **total** | **−364 bytes** |

`op_skip` and `op_exit` have no operand holes and are unchanged
(8 bytes each).

**End-to-end microbench impact** (30-op program, 100k rows,
aarch64, prod_build):

| Metric | Pre-Phase-4.5 | Post-Phase-4.5 | Δ |
|---|---|---|---|
| Emitted bytes | 2164 | 1412 | −752 (−35 %) |
| Cold first-compile | 17.5 µs | 5.2 µs | −3.4× |
| Warm-median compile | 4.5 µs | 4.5 µs | — (within noise) |
| 30-op JIT == interp aggregate | 12,517,131 | 12,517,131 | match |
| Forked-program JIT == interp | 7,250,995 | 7,250,995 | match |

The cold compile gain is the headline — `emit + patch` dropped
from ~16-17 µs to ~4 µs because there are 30/35 fewer bytes per
stencil to memcpy and 3× fewer instructions to patch per narrow
hole. The warm-median compile budget is dominated by `arena_seal`
and inter-thread JIT-write toggles on macOS, neither of which
narrow-encoding affects, so the warm number sits flat.

## What changed (commits)

| Day | Work | Commit |
|---|---|---|
| 1 | Narrow magics + `HOLE_NARROW` macro + 3 smoke stencils | `35ab92b0386` |
| 2 | Extractor pass-3 narrow detection + audit narrow scan | `53f8c35d614` |
| 3 | `width=2` patcher branch in `jit1.c` | `0fa29127723` |
| 4 | Narrow encoding for the remaining 10 stencils | `90854d9a63c` |
| 5 | This results doc + Phase 4.5 → shipped in `plan.md` | (this) |

**Modified core**:

- `stencils_src.c` — `HOLE_NARROW(name)` macro defined as an
  always-inline `volatile uint16_t v = magic; return v;` helper
  (aarch64) or as an alias to `HOLE(name)` (x86_64). 25 holes
  across 11 stencils flipped from `HOLE(...)` to
  `HOLE_NARROW(...)`; 5 already converted on Day 1.
- `hole_kinds.h` — 30 new `MAGIC_*_NARROW` 16-bit magics
  (sha256-derived from
  `RONDB-1056-Phase4_5-narrow-magic-v1|<name>`, truncated to
  16 bits, collision-checked across all 30 entries during the
  Day 0 spike). New `kHoleNarrowMagicTable[]` + `HoleNarrowMagicEntry`
  struct. Wide `kHoleMagicTable[]` shrank from 28 entries to 1
  (`MAGIC_LCI_VAL` is the only remaining wide hole).
- `jit1.c` — operand-patch path branches on `hole->width`.
  `width == 2` paths force `slot = 0` and skip
  `slot_counter[kind]++` so wide chains and narrow MOVZ for the
  same `HK_OP_A/B/C` kind can coexist within one stencil. The
  patch-instruction byte sequence is unchanged: `patch_operand`
  with `slot = 0` writes the bottom 16 bits of the operand value
  into the imm16 field, which is exactly what a narrow MOVZ
  encodes.
- `extract_stencils.c` — pass-3 narrow walk runs after the
  existing wide-chain detector. For each MOVZ with `hw == 0` and
  no immediately-following MOVK on the same Rd, looks up imm16 in
  `kHoleNarrowMagicTable[]`. Matches emit a `width=2` Hole at the
  4-byte instruction's offset.
- `audit_magics.c` — parallel `kNarrowMagicToStencil[]` mapping
  + `count_narrow_matches_arm64` helper. Narrow audit runs
  aarch64-only (16-bit literal collisions on x86 are too frequent
  to be a useful check). The wide map shrank by 25 entries.

**Re-generated**: `stencils_arm64.h`. `stencils_x86_64.h`
unchanged (verified by `regen-stencils` and `drift_check.sh`).

## Encoding details

**Narrow MOVZ pattern (aarch64).** Given:

```c
__attribute__((always_inline))
static inline uint16_t aarch64_hole_narrow_(uint16_t magic) {
  volatile uint16_t v = magic;
  return v;
}
```

clang lowers `aarch64_hole_narrow_(MAGIC_X_NARROW)` to a single
**32-bit** MOVZ instruction:

```
movz w<Rd>, #imm16          ; 0x52800000 | (imm16 << 5) | Rd
```

`volatile uint16_t` is happiest in a w-register, hence the 32-bit
form (encoding prefix `0x52800000`) rather than the 64-bit form
(`0xD2800000`) used for wide chains. Both the extractor's pass-3
walker and the audit's narrow scanner mask the sf bit out
(`mask 0x7F800000`, prefix `0x52800000`) so they accept either
form. The patcher doesn't care about sf — it just rewrites bits
20..5 of the instruction word.

**Why width=2.** The Hole carries `width = 2` to mark a narrow
encoding. The instruction is still 4 bytes wide; `width=2` means
"the 16-bit immediate field carries the operand value, no chain
slots, no slot-counter accounting." Without this discriminator the
patcher would have to infer narrow vs. wide from heuristics, and a
future stencil that mixed both encodings for the same operand kind
would mis-count slots.

**Patcher invariant.** For `width == 2` holes, `patch_operand` is
always called with `slot = 0`. The bottom 16 bits of the operand
are written to the imm16 field. The slot counter for that kind is
**not** incremented — wide chains using the same kind continue to
get slot = 0..3 in byte-order, and narrow holes within the same
stencil are independent.

## What didn't change

- **x86_64 stencils** — codegen produces the same `mov reg32,
  imm32` operand sites it always did. The 32-bit immediate
  trivially carries 16-bit operand values; no win to chase.
- **Wide chain detector (extractor pass-2)** — still 64-bit-only
  (mask `0xFF800000`, prefix `0xD2800000` for MOVZ /
  `0xF2800000` for MOVK). Wide chains accumulate >32-bit values,
  so the 32-bit MOVZ path doesn't apply.
- **`HK_OP_IMM`** — `op_load_const_int`'s int64 immediate stays
  wide (4-instruction movz/movk chain). It's the only remaining
  wide hole in the entire stencil set.
- **Bridge / admission walk / `JitBridgeReason`** — entirely
  invisible to them. The bridge produces the same Op fields; the
  encoding is a JIT-engine implementation detail.

## What we learned

1. **`volatile uint16_t` is enough.** No inline-asm tricks, no
   pragma fences. clang's volatile-store/load pattern reliably
   lowers to a single MOVZ on aarch64, even at `-O3` with all the
   inlining that copy-and-patch needs.
2. **sf-agnostic decoding is non-obvious but cheap.** The audit
   originally only saw 64-bit MOVZ (`0xD2800000`), missed the
   actual 32-bit form (`0x52800000`), and reported 0 matches for
   every narrow magic. One-character mask change (`0xFF` →
   `0x7F`) fixed both the audit and the extractor.
3. **The patcher needed almost no change.** The existing
   `patch_operand(site, slot=0, value)` already writes the bottom
   16 bits to imm16 — exactly what narrow MOVZ wants. The
   `width=2` branch is defensive (so wide and narrow can coexist
   within a future stencil), not load-bearing for the smoke set.
4. **Cold compile is dominated by emit+patch byte volume**, not
   per-instruction work. Cutting 35 % of the bytes cut 70 % of
   the cold compile time.

## Out of scope (for Phase 5+)

- **Per-stencil-family addressing-mode folding.** Now that the
  load/store + MOVZ sequences are no longer drowned in a 16-byte
  movz/movk preamble, the AArch64 indexed addressing modes
  (`ldr Xt, [Xn, Wm, sxtw #3]`) become tractable as a Phase-5
  optimisation. Not Phase 4.5's job.
- **x86_64 narrow path.** No-op as discussed above.
- **Embedded normal-interp branches and the cold-call branch
  pattern.** Phase 5.

## References

- Phase 4.5 implementation plan: `phase_4_5_implementation.md`.
- Phase 4 results doc: `phase_4_setup_integration.md`.
- Stencils header (regenerated baseline):
  `storage/ndb/src/kernel/blocks/dbtup/jit/stencils_arm64.h`.
- Engine patcher (`width == 2` branch):
  `storage/ndb/src/kernel/blocks/dbtup/jit/jit1.c`.
