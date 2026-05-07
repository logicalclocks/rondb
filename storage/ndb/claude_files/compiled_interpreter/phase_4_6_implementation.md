# RONDB-1056 Phase 4.6 — inline-asm immediate constraint (eliminate spill/reload)

**Status: planning.** Phase 4.6 replaces the
`volatile uint16_t v = magic; return v;` codegen pattern with
inline assembly carrying an `"n"` immediate constraint. This
eliminates the STRH/LDRH spill-reload that `volatile` is forced
to emit, and consequently the `sub sp / add sp` stack-frame
prologue/epilogue that exists only to back the spill slot.

Aarch64-only, like Phase 4.5. x86_64 keeps the
`extern uint64_t HOLE_*` relocation-driven pattern unchanged.

Branch: `RONDB-1056-compiled-interpreter`.

## Headline math (predicted)

| Stencil | Phase 4 | Phase 4.5 (shipped) | Phase 4.6 (predicted) |
|---|---:|---:|---:|
| op_load_const_int | 60 B | 44 B | ~20-24 B |
| op_load_col_int   | 68 B | 44 B | ~20-24 B |
| op_mov_int_int    | 64 B | 40 B | ~16-20 B |
| op_add_int_int    | 96 B | 60 B | ~20-24 B |
| op_branch_*       | 80 B | 56 B | ~20-24 B |
| op_load_col_ndb   | 76 B | 52 B | ~24-28 B |

**Source of the saving** (per stencil):

- 4–6 bytes from the `sub sp, sp, #16 / add sp, sp, #16` pair
  going away once no spill slot is needed.
- 4 bytes per narrow hole (the `strh w<x>, [sp+N] / ldrh w<x>,
  [sp+N]` pair).
- 4 bytes per wide hole (the `str x<x>, [sp+N] / ldr x<x>, [sp+N]`
  pair after the 4-instruction movz/movk chain).

Cumulative across the 14 stencils:
- Phase 4.5 saved 364 bytes by collapsing chains.
- Phase 4.6 should save another ~300-400 bytes by removing the
  spill/reload round-trip.
- Combined: roughly 5× smaller than the Phase 4 baseline for the
  arithmetic / branch / mov stencils.

The big secondary win is **cold first-compile time**, which is
emit-byte-bound. Phase 4.5 dropped that 17.5 µs → 5.2 µs (−3.4×).
Phase 4.6 should drop it again to roughly 3-4 µs.

## ★ Items to investigate (before coding starts)

Same posture as Phase 4.5 — concrete experiments first, then
implementation. The spike fits in ~½ day.

> **Day 0 spike resolutions (2026-05-07).** All six items
> resolved via `/tmp/phase46_spike.c` compiled with the pinned
> upstream clang 20.1.8 and disassembled. Two adjustments
> required vs the initial design:
>
> 1. The narrow `"n"` constraint **must mask the magic to
>    `& 0xFFFFu`** in the constraint expression — otherwise
>    clang's asm formatter prints the value as a signed int and
>    rejects magics with the high 16-bit bit set (e.g.,
>    `0xfc24` was being printed as `-988`). The wide chain
>    already does this masking per slice. The narrow helper
>    parameter type changes from `uint16_t` to `uint32_t` so the
>    masking expression compiles cleanly.
> 2. The narrow helper **must return `uint64_t`, not `uint16_t`**
>    — otherwise clang emits a redundant
>    `and rN, rN, #0xffff` after each MOVZ to "honor" the
>    16-bit return type. With `uint64_t` return, clang trusts
>    the MOVZ zero-extension and drops the AND. (MOVZ in the
>    W-form encoding `0x52800000` zero-fills the upper 32 bits
>    of the X-register; subsequent uses index into a 64-bit
>    array slot using the X-register, which is exactly what we
>    want.)
>
> Headline numbers from the spike (post-strip):
>
> | Spike stencil | Post-strip | Phase 4.5 | Saving |
> |---|---:|---:|---:|
> | s1_load_const  | 24 B | 44 B | −45 % |
> | s2_mov         | 16 B | 40 B | −60 % |
> | s3_add         | 28 B | 60 B | −53 % |
> | s4_branch_lt   | 28 B | 56 B | −50 % |
> | s5_load_col_ndb| 28 B | 52 B | −46 % |
>
> Predicted full stencil-set saving: ~50 % off Phase 4.5 bytes.

### ★ Investigate I1 — `"n"` constraint accepts our magic constants

The `"n"` constraint is *integer immediate, must fit machine
operand*. For AArch64 MOVZ that's 16 bits unsigned. We pass
- the bare `MAGIC_*_NARROW` (already `uint16_t`) for narrow holes,
- four shifted slices `magic & 0xFFFF`, `(magic >> 16) & 0xFFFF`,
  `(magic >> 32) & 0xFFFF`, `(magic >> 48) & 0xFFFF` for wide.

Each slice is provably ≤ `0xFFFF`. clang should accept all of
them. **Spike:** compile a single helper for each pattern
isolated in a tiny .c, `objdump -d`, confirm:
- the bytes for the 4-instruction chain are byte-identical to the
  current `volatile uint64_t` pattern (because they encode the
  same MOVZ + 3×MOVK sequence), and
- the bytes for the narrow MOVZ are byte-identical to the
  current `volatile uint16_t` pattern.

If clang complains about a slice ≥ `0xFFFF` (it shouldn't —
masking is explicit in our source), fall back to `"i"` (less
strict immediate constraint) and re-verify.

### ★ Investigate I2 — clang keeps the chain contiguous

The extractor's pass-2 detector requires the 4 instructions of a
wide chain to be contiguous and target the same Rd. With inline
asm this is structurally guaranteed *within one asm block*:
clang treats the asm string as a single instruction-emission unit
that doesn't get reordered or interleaved.

But the asm block's *placement relative to other asm blocks* and
to the surrounding C code is up to clang's scheduler. The
question: can clang interleave instructions from two different
asm-volatile blocks?

`asm volatile` documented behavior: clang treats each as having
unspecified side effects and a memory clobber-equivalent for
ordering. Adjacent asm-volatile blocks shouldn't interleave (each
is a single statement at the IR level). **Spike:** compile
`op_add_int_int` (3 wide holes worth of asm — but Phase 4.5
already made these narrow, so really 3 narrow asm blocks +
some surrounding C); verify each MOVZ stays in its own 4-byte
instruction with no interleaving.

For Phase 4.6's actual implementation we have 1 wide hole left
in the entire codebase (`HK_OP_IMM` for `op_load_const_int`'s
int64 immediate) plus 30 narrow holes — the contiguity question
matters mostly for that one wide chain.

### ★ Investigate I3 — prologue/epilogue actually drops

Today's stencils start with `sub sp, sp, #16` and end with
`add sp, sp, #16`. After Phase 4.5 those bytes are still there —
because the volatile reload still spills `op->c` (the branch-
target argument? no, narrow operands). In any case we observe
them in the regenerated headers.

After Phase 4.6 there should be no volatile-driven spill in any
hot stencil, so no stack frame needed. **Spike:** compile
`op_mov_int_int` (the simplest two-narrow-hole stencil) with
inline asm; confirm `sub sp` and `add sp` are absent from the
disassembly.

If a stencil still spills for non-hole reasons (e.g.,
`op_load_col_ndb` calling a helper might require frame setup for
the call instruction itself), accept that and document it as a
local exception.

### ★ Investigate I4 — `[[clang::musttail]]` interaction

Branch stencils end with a `[[clang::musttail]] return
HOLE_TGT(s);`. The musttail attribute requires the call to be in
tail position with no callee-save reload between it and the
return. Inline asm before the musttail call is fine in principle
— it produces a value used in the comparison, then the comparison
result drives the conditional tail call. **Spike:** compile
`op_branch_lt_int_int` with inline-asm holes; confirm the
disassembly still ends with a clean conditional `b.lt`-or-
fallthrough pattern, no stack manipulation between asm and
musttail.

### ★ Investigate I5 — what about `op_load_col_ndb`?

This stencil has a non-tail `bl ndb_jit_h_load_col` call. AArch64
calling conventions require an x29/x30 frame for non-leaf
functions. The current implementation uses `preserve_none` which
spills x30 (link register) at the call site. The current bytes
reflect that: a stack frame of some size, the `bl`, and a
restore.

Inline asm for the two narrow holes (LCN_COL, LCN_DST) won't
make the frame go away — the `bl` itself requires it. But it
will eliminate the four `strh`/`ldrh` instructions tied to the
volatile pattern, saving 16 bytes even if the frame stays.
**Spike:** compile `op_load_col_ndb`; expect ~24 B savings, not
the full 32 B that frame-removal would give.

### ★ Investigate I6 — debug builds

Drift check uses prod_build (no debug info, optimised). Debug
builds use `-O0` and may emit different bytes. We don't ship
debug stencils — the regen pipeline pins clang version and uses
release-mode flags. Verify by running `regen-stencils` in both
debug_build and prod_build and confirming identical output. If
they differ, the regen pipeline already protects us (debug builds
consume committed prod-flagged headers).

## 1. Scope

**In scope.**
- Replace the AArch64 `aarch64_hole_(uint64_t)` and
  `aarch64_hole_narrow_(uint16_t)` helpers in `stencils_src.c`
  with inline-asm versions using `"n"` immediate constraints.
- Verify byte-level equivalence of the *MOVZ chain itself* (the
  spill/reload bytes go away, but the chain bytes are identical).
- Roll the new helpers through all 14 hot stencils + the 2
  terminator stencils (op_skip / op_exit have no holes — no
  change needed).
- regen-stencils PASS for all magics (1 wide LCI_VAL + 30 narrow).
- All Phase 4 tests + microbench differential + drift check +
  rondb_jit_canary MTR PASS.

**Out of scope.**
- x86_64 — already optimal (relocation-driven, no volatile).
- Engine patcher — no changes; the chain bytes and narrow MOVZ
  bytes are byte-identical to Phase 4.5, only their surroundings
  shrink.
- Extractor / audit — unchanged for the same reason.
- Bridge / admission — invisible.
- Per-stencil-family addressing-mode folding (Phase 5+).
- Wider lattice / type-state work (Phase 5).

## 2. File layout

Single primary change: `stencils_src.c` (the AArch64 helper
section). Everything else (hole_kinds.h, audit_magics.c,
extract_stencils.c, jit1.c) is unchanged.

```
storage/ndb/src/kernel/blocks/dbtup/jit/
└── stencils_src.c              ← only file that changes
```

## 3. Stencil source pattern

**Today (Phase 4.5):**

```c
__attribute__((always_inline))
static inline uint64_t aarch64_hole_(uint64_t magic) {
  volatile uint64_t v = magic;
  return v;                    /* forces STR/LDR via stack slot */
}

__attribute__((always_inline))
static inline uint16_t aarch64_hole_narrow_(uint16_t magic) {
  volatile uint16_t v = magic;
  return v;                    /* forces STRH/LDRH via stack slot */
}
```

**Phase 4.6** (post-spike form):

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

__attribute__((always_inline))
static inline uint64_t aarch64_hole_narrow_(uint32_t magic) {
  /* Parameter is uint32_t (not uint16_t) so the `& 0xFFFFu` mask
   * in the constraint expression compiles without sign-formatting
   * trouble. Return is uint64_t so clang trusts the MOVZ zero-
   * extension and skips emitting `and rN, rN, #0xffff`. The
   * caller's `HOLE_NARROW(name)` macro feeds the magic constant
   * directly. */
  uint64_t v;
  __asm__ volatile (
    "movz %w[out], %[m]"
    : [out] "=r" (v)
    : [m]   "n"  (magic & 0xFFFFu)
  );
  return v;
}
```

Why this form:

- `__asm__ volatile` keeps the side-effect pinned to its lexical
  position — clang won't CSE two HOLE() calls of the same magic.
- `"=r"` lets clang allocate any free register for the output
  (no spill required to honor the constraint).
- `"n"` forces the operand to be a compile-time integer immediate
  that clang substitutes textually into the asm string.
- `%w[out]` selects the W-register form for the narrow path
  (32-bit MOVZ, encoding prefix `0x52800000`); `%[out]` defaults
  to the X form for the wide chain (`0xD2800000`). Both forms are
  already accepted by the extractor and audit (sf-agnostic mask).

The macro layer in `stencils_src.c` does *not* change — `HOLE()`
and `HOLE_NARROW()` keep their existing signatures and call the
helpers below them. All 14 stencils are touched zero times.

## 4. Magic generation

No new magics needed — the 16-bit narrow magics from Phase 4.5
and the 64-bit wide magic for `MAGIC_LCI_VAL` carry over byte-
identical. `hole_kinds.h` is unchanged.

## 5. Extractor / audit

Unchanged. The extractor's pass-2 chain detector still finds the
4-instruction MOVZ+3×MOVK sequence emitted by the wide asm block
(structurally guaranteed contiguous within one asm). Pass-3
narrow detector still finds the standalone MOVZ. Audit verifies
the same magic counts.

The only observable change at extraction time: stencil **byte
counts** drop because the surrounding spill/reload and prologue
bytes are gone. The Hole entries themselves keep their offsets
relative to the chain/MOVZ — but the chain may now sit at offset
0 (no `sub sp` ahead of it) instead of offset 4.

## 6. Engine patcher

Unchanged. `width=2` narrow holes still patch via `slot=0` write
to bits 5..20 of the MOVZ instruction; `width=4` wide chains
still get four `slot=0..3` writes through `slot_counter[kind]`.
The instruction encoding is byte-identical to Phase 4.5 — only
the surroundings change.

## 7. Bridge / admission

No changes. The bridge produces the same Op fields; the JIT-
engine encoding is invisible to it. `JitBridgeReason` unchanged.

## 8. Stencils to update

None at the source level. The change is entirely in the helper
definitions; every stencil that calls `HOLE()` / `HOLE_NARROW()`
inherits the new codegen automatically.

| Stencil | Holes | Source change |
|---|---|---|
| op_load_const_int | LCI_DST narrow + LCI_VAL wide | none |
| op_load_col_int   | LRC_DST, LRC_COL narrow      | none |
| op_mov_int_int    | MV_DST, MV_SRC narrow        | none |
| op_add/minus/mul  | DST, A, B narrow             | none |
| op_sum_bigint     | SLOT, SRC narrow             | none |
| op_branch_*       | A, B narrow + TGT branch     | none |
| op_load_col_ndb   | COL, DST narrow + cold-call  | none |
| op_skip / op_exit | —                            | none |

## 9. Day breakdown

**Day 0 (~½ day) — investigation spike.** Resolve ★ I1 through
★ I6 with concrete experiments. Output: a one-page note
recording the chosen approach for each. If clang doesn't accept
the `"n"` constraint or the chain breaks contiguity, *stop here*
and revisit — the rest of the plan depends on this.

**Day 1 (~3h)** — implement the two helpers. Run regen-stencils;
compare the regenerated `stencils_arm64.h` against the Phase 4.5
baseline; verify only the *surroundings* of the MOVZ chains
changed (chain bytes byte-identical, spill/reload gone, prologue
gone where applicable). Hand-disassemble `op_mov_int_int` and
`op_load_const_int` to validate.

**Day 2 (~2h)** — run all Phase 4 unit tests, microbench
differential, drift check baseline, `rondb_jit_canary` MTR.
Record numbers.

**Day 3 (~3h)** — investigate any stencil whose codegen shape
surprised the spike (e.g., op_load_col_ndb keeping its frame
because of the `bl`). Document local exceptions in the helper or
in `phase_4_6_inline_asm.md`.

**Day 4 (~2h)** — write `phase_4_6_inline_asm.md` results doc,
flip Phase 4.6 to shipped in `plan.md`. Mirror the
`phase_4_5_narrow_holes.md` structure.

**Total: 2-3 days** (excluding the Day 0 spike). Smaller than
Phase 4.5 because there's only one source-level change, no new
magic table, no extractor/audit/engine work.

## 10. Test approach

- **Day 0 smoke test** (per spike): hand-disassemble two
  representative stencils. Verify chain bytes match, spill/reload
  is absent, prologue/epilogue is absent.
- **Extractor regression**: pass-2 wide-chain and pass-3 narrow
  detection both still find their magics. regen-stencils PASS.
- **Engine regression**: existing `width=2` narrow path and
  `slot_counter`-based wide path continue to work. All Phase 4
  unit tests PASS unchanged.
- **Microbench correctness**: aggregate matches interpreter on
  the existing 30-op program AND the forked program. Speedup
  *should* improve due to shrunken emit cost.
- **MTR canary**: `rondb_jit_canary` PASSes with all 4 queries.
- **Drift check**: regenerated `stencils_arm64.h` is the new
  baseline.

## 11. Verification checklist

- [ ] All ★ Investigate items resolved with documented decisions.
- [ ] Smoke test: clang-emitted bytes for `op_mov_int_int`'s two
      narrow holes = 2 standalone MOVZ instructions, no
      surrounding spill/reload, no prologue/epilogue.
- [ ] All Phase 4 unit-test binaries (`bridge_tests`,
      `admission_tests`, `coldcall_tests`, `extractor-tests`,
      `proto_microbench`) PASS unchanged.
- [ ] Magic-byte audit PASS for all magics (1 wide + 30 narrow).
- [ ] aarch64 stencil byte count drops measurably (target: 30-op
      microbench program drops from 1412 B to ~700-900 B,
      another ~35-50% off the Phase 4.5 baseline).
- [ ] aarch64 cold-compile time drops (target: ~3-4 µs, down
      from 5.2 µs).
- [ ] `rondb_jit_canary` MTR test PASSes.
- [ ] Drift check accepts the new baseline.
- [ ] x86_64 stencil bytes unchanged (Phase 4.6 is aarch64-only).

## 12. Out of scope (explicit reminder)

- x86_64 inline asm — the `extern uint64_t HOLE_*` pattern
  already produces optimal bytes there.
- New opcodes — Phase 5 territory.
- Wider lattice / type-state / embedded interpreter — Phase 5.
- Cold-call branch pattern — Phase 5.

## 13. Risks / things that may surprise us

1. **clang patch-version sensitivity.** Inline asm with
   `"n"` constraint behavior is generally stable, but our pinned
   20.1.8 might happen to do something subtly different from
   another 20.1.x. The drift check + audit catches this — same
   class of risk as the magic-byte chain itself.

2. **Wide chain interleaving.** If clang ever decides to split a
   single asm block across two locations (it shouldn't —
   single-statement inline asm is atomic at the IR level), pass-2
   would fail to detect the chain. Mitigation: write the wide
   asm as a single multi-line string (which we already do).

3. **`bl` stencils keep their frame.** `op_load_col_ndb` may not
   shrink as much as the others because the AArch64 ABI requires
   x29/x30 setup around the helper call. Document and accept.

4. **Tail-call interaction.** `[[clang::musttail]]` is strict
   about the call being last with no other code in between. Our
   inline asm is *before* the musttail (it produces operand
   values), not between asm and call, so this should be fine —
   but worth verifying on each branch stencil.

5. **Debug builds drifting.** Optimisation level affects how
   freely clang treats inline asm. The regen pipeline pins
   release flags, so this matters only if a contributor tries to
   `regen-stencils` from a debug-flagged build. The CMake driver
   already enforces release codegen flags.

6. **What if cold-compile time *increases*?** Small possibility:
   if removing the spill changes which registers are live across
   the chain, clang might insert different other-instruction
   choices that emit similar bytes. Net effect on bytes should
   still be a win (no spill is strictly fewer bytes), but worth
   measuring.

## 14. What we learn from Phase 4.6

If this works as predicted:

- **Stencil bytes are governed by the source-level idiom**, not
  by anything intrinsic to copy-and-patch JIT. We've now seen
  three idiom changes (Phase 1 64-bit chain → Phase 4.5 16-bit
  chain → Phase 4.6 inline asm) drop the same stencils from 60 B
  to 20 B. Future Phase 5 stencils should adopt inline asm from
  the start.

- **The `"n"` constraint is the right primitive** for embedding
  patched immediates in copy-and-patch stencils. Cleaner than
  `extern uint64_t HOLE_*` (which only works because x86_64 has
  rip-relative `mov reg, imm32` with relocations), cleaner than
  `volatile`, and arch-portable in principle (an x86_64
  equivalent would emit `mov reg, $imm32` directly).

- **The cold-compile bottleneck is emit-byte-bound**, not per-
  instruction-bound. Cumulatively across Phase 4.5 + Phase 4.6 we
  expect ~5× shrinkage in cold-compile time, dominated by less
  memcpy and fewer patches.

If the spike turns up something unexpected:

- We learn that clang's optimiser doesn't honor inline asm
  immediate constraints the way we expect, and we revisit the
  pattern. The fallback is the current Phase 4.5 status quo —
  perfectly acceptable.
