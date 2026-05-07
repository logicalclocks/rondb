# RONDB-1056 Phase 4.5 — narrow-hole optimization (aarch64 stencil compaction)

This document is the implementation plan for Phase 4.5, an
optimization phase that lands **before** Phase 5's mass stencil
authoring. The aarch64 movz+3×movk chain pattern, used uniformly
in Phases 2-4 for every operand hole, allocates **16 bytes per
hole** to materialize a 64-bit magic constant. Most operand holes
carry 8-bit values (register indices, accumulator slots, small
column ids); 12 of those 16 bytes are wasted per hole.

Phase 5 wants to write ~75 new stencils. With ~3 holes per
stencil and ~12 wasted bytes each, that's ~2.7 KB of unnecessary
JIT bytes across the stencil set on aarch64. More importantly,
it's ~225 holes that would have to be re-reviewed if we
optimize after Phase 5 ships. **Cheaper to compress the pattern
first.**

The optimization is aarch64-only — x86_64 already uses inline
32-bit immediates per relocation and produces minimum-size
encodings.

## ★ Items still to investigate (before coding starts)

These need empirical or design work before Phase 4.5 day-1
implementation can begin. Each is a directive for the planning
spike that precedes the implementation work.

### ★ Investigate I1 — clang codegen for narrow `volatile` reads

**Question:** does clang lower `volatile uint16_t v = magic; return v;`
to a single `movz Rd, #magic` on aarch64, or does it emit
something else (literal-pool `ldr`, masking + zero-extension,
etc.)?

**Why it matters:** the entire optimization rests on this. If
clang emits a literal-pool `ldr` instead of `movz`, the
extractor's narrow-pattern detection has nothing to recognize.

**How to verify:** compile a tiny `.c` with just the macro
expansion using `brew llvm@20` clang, disassemble with
`llvm-objdump -d`, confirm the byte sequence is exactly
`<imm16-bytes> <opcode-byte> 0xd2`.

**Scenarios to check:**
- `volatile uint16_t v = MAGIC; return v;` (the proposed pattern)
- `volatile uint32_t v = MAGIC; return v;` (32-bit fallback)
- Same patterns wrapped in `__attribute__((always_inline))` —
  what we'd actually use
- Same patterns at `-O2` (production) and `-O0` (debug) — both
  must work

**Output:** documented codegen for each pattern. If `volatile
uint16_t` works, proceed with 16-bit. If not, fall back to 32-bit
(2-instruction movz+movk chain, still 8 bytes shorter than the
4-instruction full chain).

### ★ Investigate I2 — magic-collision space at 16-bit width

**Question:** with ~30 narrow magics packed into 65,536 possible
16-bit values, what's the realistic collision rate, and what's
the right detection strategy?

**Why it matters:** the audit-step's correctness assumption is
that each magic is unique. Birthday-bound for 30 magics in 65k
space gives ~0.7% collision probability — small but nonzero. If
two narrow magics collide, the extractor mis-attributes one to
the other and the engine patches the wrong field.

**Trade-offs:**
- **16-bit magic, single movz**: 4 bytes per hole. 0.7%
  collision risk, requires deterministic regeneration if it
  hits.
- **32-bit magic, movz+movk**: 8 bytes per hole. Collision-free
  in practice (30 magics in 4 billion values).
- **64-bit magic** (current): 16 bytes per hole. Collision-free.

**How to decide:** simulate magic-collision rates with the
project's SHA-256-derived generator across 30, 50, 100 narrow
holes. If 16-bit collision-free generation requires more than 5
regen attempts, fall back to 32-bit.

**Output:** chosen width (16 or 32 bits) with documented
collision-rate measurement. The plan below tentatively assumes
16-bit; flip if I2 says otherwise.

### ★ Investigate I3 — narrow-hole detection vs existing chain detection

**Question:** does the extractor's existing chain detector
incorrectly match a narrow `movz` as the start of a
movz+3×movk chain that never completes? If so, do we need a
second pass / classification rule?

**Why it matters:** the extractor walks every aarch64
instruction. The chain detector's logic: see movz with magic →
start tracking → expect 3 movks → match if reg's accumulated
value equals a known magic. If a narrow movz matches a 16-bit
magic's *low slot*, the chain detector might incorrectly start
tracking, then fail to complete (no movks follow), and silently
drop the narrow hole.

**Solution sketches** (pick one in the planning spike):
1. **Disjoint magic spaces**: narrow magics never appear as the
   low 16 bits of any wide magic. Enforced via collision-audit
   extension.
2. **Two-pass walk**: first pass identifies all complete wide
   chains and marks their instructions; second pass treats the
   remaining stand-alone movz instructions as narrow-hole
   candidates.
3. **Per-instruction context check**: when a movz is seen, look
   at the next instruction. If it's a movk targeting the same
   register, treat as a chain start; else as narrow.

**Output:** chosen detection strategy. Plan below tentatively
assumes (2) two-pass walk — cleanest separation, no constraints
on magic generation.

### ★ Investigate I4 — Hole struct layout

**Question:** add a `width=2` semantic to existing `HK_OP_*`
kinds, OR add new kinds like `HK_OP_A_NARROW`, `HK_OP_B_NARROW`
etc.?

**Why it matters:** affects engine patcher dispatch + extractor
output format + audit table.

**Options:**
1. **Reuse kind, vary width**: `Hole{kind=HK_OP_A, width=2}` for
   narrow, `Hole{kind=HK_OP_A, width=4}` for full. Engine
   patcher's HK_OP_A case branches on width. Backward-compatible:
   existing wide aarch64 holes (4-Hole-per-chain, width=4) keep
   their shape.
2. **New kinds**: `HK_OP_A_NARROW`, etc. Engine patcher gains
   new cases, no width-branching. Extractor's audit table picks
   up new kind names.

**Trade-off:** (1) is less code change but the patcher needs to
look at width in addition to kind — small added complexity in a
hot path. (2) is more boilerplate but cleaner switch dispatch.

**Output:** chosen layout. Plan below tentatively assumes (1)
reuse kind + vary width. Flip if I4 says otherwise.

### ★ Investigate I5 — x86_64 narrow path worth doing?

**Question:** on x86_64, we currently use `mov reg32, imm32`
(5 bytes) for register-index holes. We could switch to
`mov reg8, imm8` (3 bytes) for narrow holes. Is the 2-byte
saving × ~225 holes = ~450 bytes of x86_64 stencil savings
worth the implementation effort?

**Why it matters:** scope decision. Phase 4.5 is aarch64-driven;
x86_64 is a tangential add-on.

**Trade-offs:**
- `mov reg8, imm8` requires the register to be one of `al/cl/...`
  rather than `eax/ecx/...`. Constrains register allocation.
- Existing x86_64 stencils have been carefully verified against
  Phase 1's hand-extracted bytes. Changing the source pattern
  invalidates that baseline.
- 450 bytes is small relative to total x86_64 emitted (623 bytes
  for the 30-op program — proportional savings would be ~70 bytes
  for the same program).

**Output:** yes or no, with justification. Plan below tentatively
assumes **no** — Phase 4.5 stays aarch64-only. x86_64 keeps the
existing `mov reg32, imm32` pattern.

### ★ Investigate I6 — narrow hole admissibility per opcode

**Question:** which existing operand holes can safely become
narrow? Specifically:
- `HK_OP_A` (register / acc slot / col_id): max value = 255
  (Phase 4 col_id cap). Fits in 8 bits.
- `HK_OP_B`: same constraints.
- `HK_OP_C`: register or branch-target pc. Max pc = BC_MAX_OPS-1
  = 63. Fits in 8 bits.
- `HK_OP_IMM`: signed int64 (e.g., load_const value). Genuinely
  full-width — keeps current chain encoding.

**Why it matters:** narrow optimization applies to A/B/C only.
IMM keeps the full chain.

**Open detail:** `HK_BRANCH_TAKE` / `HK_BRANCH_FALL` are PC-rel
displacements, not operand-holes. They use a different mechanism
(linker relocation on branch instructions, not magic-byte
chains) and are unaffected by Phase 4.5.

**Output:** confirmed narrow-eligible kinds: HK_OP_A, HK_OP_B,
HK_OP_C. HK_OP_IMM keeps the wide chain. Phase 4.5's stencil
source updates only affect A/B/C holes.

## 1. Scope

**In:**
- New narrow-encoding for HK_OP_A, HK_OP_B, HK_OP_C on aarch64.
  HK_OP_IMM stays wide.
- `HOLE_NARROW(name)` macro in `stencils_src.c` for narrow
  holes; existing `HOLE(name)` keeps the wide chain for
  backward-compat (used by HK_OP_IMM).
- Extractor recognizes narrow patterns alongside the existing
  chain pattern (per ★ I3 strategy).
- Engine patcher writes the narrower field for narrow holes
  (per ★ I4 layout).
- All 13 existing stencils' source updated to use `HOLE_NARROW`
  for register / slot / col_id holes.
- Generated `stencils_arm64.h` regenerated (smaller). Drift
  check + audit refreshed.

**Out (deferred to Phase 5+):**
- x86_64 narrow path (per ★ I5).
- Addressing-mode folding (`ldr Xd, [x20, #imm12]` baking the
  index into the data instruction). Per-stencil-family work;
  better with Phase 5 data on which families dominate hot
  paths.
- Multi-stencil constant-folding optimizations.

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── hole_kinds.h                (Hole struct gains width=2 semantics
│                                or new kinds — per ★ I4; new narrow
│                                magic table — per ★ I2)
├── stencils_src.c              (HOLE_NARROW macro added; ~30 hole
│                                sites updated to use it across the
│                                13 existing stencils)
├── stencils_x86_64.h           (unchanged — x86_64 not in scope)
├── stencils_arm64.h            (regenerated, smaller bytes)
└── extract_stencils/
    ├── extract_stencils.c      (narrow-pattern detector — per ★ I3)
    └── audit_magics.c          (narrow magic table support)
```

No new files; all changes are extensions to existing components.

## 3. Stencil source pattern (after ★ I1 / ★ I2 resolved)

Tentative — final form depends on investigation results.

**aarch64 narrow path** (assuming I2 resolves to 16-bit):

```c
#if defined(__aarch64__)
__attribute__((always_inline))
static inline uint16_t aarch64_hole_narrow_(uint16_t magic) {
  volatile uint16_t v = magic;
  return v;
}
#  define HOLE_NARROW(name) aarch64_hole_narrow_(MAGIC_##name##_NARROW)
#endif
```

**aarch64 wide path** (unchanged from Phase 4):

```c
__attribute__((always_inline))
static inline uint64_t aarch64_hole_(uint64_t magic) {
  volatile uint64_t v = magic;
  return v;
}
#define HOLE(name) aarch64_hole_(MAGIC_##name)
```

**Stencil example** (`op_load_const_int` after Phase 4.5):

```c
DECLARE_HOLE(LCI_DST);     /* narrow — reg index */
DECLARE_HOLE(LCI_VAL);     /* wide — int64 immediate */
STENCIL op_load_const_int(JitState *s) {
  s->regs_i64[HOLE_NARROW(LCI_DST)] = (int64_t)HOLE(LCI_VAL);
  TAIL_NEXT(s);
}
```

Stencil bytes shift; magic table grows with the new narrow
magics. Wide MAGIC_LCI_VAL stays.

## 4. Magic generation

For 16-bit narrow magics:

```python
import hashlib
salt = "RONDB-1056-Phase4_5-narrow-magic-v1"
for name in narrow_hole_names:  # ~30 entries: LCI_DST_NARROW, etc.
    h = hashlib.sha256((salt + "|" + name).encode()).digest()
    val = int.from_bytes(h[0:2], 'little') & 0xFFFF
    # ★ I2: check uniqueness; regenerate with bumped salt if collision
```

For 32-bit fallback (per ★ I2): use 4 bytes from the SHA-256
output, same uniqueness check.

Magics live in `hole_kinds.h` alongside the existing wide
magics. The narrow magic table for the audit (`kHoleMagicTable`
or a sibling table) gets new entries.

## 5. Extractor narrow-pattern detection (per ★ I3)

Tentative two-pass walk:

**Pass 1** — existing chain detector. Walks instructions,
accumulates per-Rd 64-bit values via movz/movk. On match against
the wide `kHoleMagicTable`, emits 4 Hole entries (one per slot).
**Marks the 4 instructions as consumed.**

**Pass 2** — narrow detector. Walks instructions, skips any
marked-consumed by Pass 1. For each unmarked movz, decodes its
imm16, looks up in the narrow magic table. On match, emits 1
Hole entry with `kind=HK_OP_<X>`, `width=2`.

Unmarked movz instructions that don't match any magic are
ordinary code (e.g., from clang's stack manipulation) — left
alone.

## 6. Engine patcher (per ★ I4)

Tentative reuse-kind-vary-width approach:

```c
case HK_OP_A:
case HK_OP_B:
case HK_OP_C: {
  int64_t v = hole_value_from_op(hole->kind, op);
#if defined(__aarch64__)
  if (hole->width == 2) {
    /* Narrow: single movz, 16 bits of imm in the instruction */
    patch_imm16_at(patch, /*slot=*/0, (uint16_t)v);
  } else {
    /* Wide: 4-Hole-per-chain via slot_counter */
    ... existing logic ...
  }
#else  /* x86_64 */
  ... existing logic ...
#endif
  break;
}
```

The width field on Hole becomes load-bearing for aarch64. Phase
4 already declares it; Phase 4.5 makes it semantically meaningful
beyond "4 == always".

## 7. Bridge / admission

No changes. The bridge produces the same Op fields; the
JIT-engine-side encoding is invisible to it. Admission walk
unchanged. `JitBridgeReason` unchanged.

## 8. Stencils to update

All 13 existing stencils have at least one register-index hole
that becomes narrow:

| Stencil | Narrow holes | Wide holes |
|---|---|---|
| op_load_const_int | LCI_DST | LCI_VAL |
| op_load_col_int | LRC_DST, LRC_COL | — |
| op_mov_int_int | MV_DST, MV_SRC | — |
| op_add_int_int | ADD_DST, ADD_A, ADD_B | — |
| op_minus_int_int | MINUS_DST, MINUS_A, MINUS_B | — |
| op_mul_int_int | MUL_DST, MUL_A, MUL_B | — |
| op_sum_bigint | SUM_SLOT, SUM_SRC | — |
| op_branch_lt_int_int | BLT_A, BLT_B | — (TGT is reloc) |
| op_branch_le_int_int | BLE_A, BLE_B | — |
| op_branch_eq_int_int | BEQ_A, BEQ_B | — |
| op_branch_gt_int_int | BGT_A, BGT_B | — |
| op_branch_ge_int_int | BGE_A, BGE_B | — |
| op_branch_ne_int_int | BNE_A, BNE_B | — |
| op_load_col_ndb | LCN_COL, LCN_DST | — |
| op_skip / op_exit | — | — |

Total: ~28 new narrow-magic entries.

## 9. Day breakdown

**Day 0 (~½ day) — investigation spike**: resolve ★ I1 through ★
I6 with concrete experiments. Output: a one-page note recording
the chosen approach for each.

**Day 1 (~4h)**: implement narrow magic table + Hole struct
extension + HOLE_NARROW macro. Update `op_load_const_int`,
`op_load_col_int`, `op_mov_int_int` as the smoke-test set. Verify
clang produces the expected single-movz bytes via `objdump`.

**Day 2 (~4h)**: extend extractor's narrow-pattern detector.
Process `op_load_const_int`'s aarch64 .o; verify the extracted
header has 1 narrow Hole + 4 wide chain Holes (LCI_VAL stays).
Update audit_magics.c to handle narrow magics (1× per declaring
stencil expectation).

**Day 3 (~4h)**: extend engine patcher for narrow holes. Run
microbench on aarch64; confirm the 30-op program still matches
interpreter aggregate. Drift check.

**Day 4 (~3h)**: roll narrow encoding through the remaining 10
stencils. regen-stencils, audit pass for all magics (wide +
narrow). All Phase 4 unit tests + MTR canary still pass.

**Day 5 (~2h)**: results doc `phase_4_5_narrow_holes.md` + mark
shipped in plan.md.

**Total: 3-4 days** (excluding investigation spike). The spike is
load-bearing — without it, the implementation can't start.

## 10. Test approach

- **Smoke test on Day 1**: hand-disassemble `op_load_const_int`'s
  aarch64 bytes; confirm a single 4-byte `movz` instruction
  appears for LCI_DST instead of the 16-byte chain.
- **Extractor regression**: existing chain-detection still works
  for wide holes (LCI_VAL et al.).
- **Engine regression**: existing 4-Hole-per-chain patcher path
  still works.
- **Audit regression**: all wide magics still found 1× in
  declaring stencil; narrow magics added with same invariant.
- **Microbench correctness**: aggregate matches interpreter on
  the existing 30-op program AND the forked program. Speedup
  may improve due to smaller emit cost.
- **MTR canary**: `rondb_jit_canary` still PASSes with all 4
  queries.
- **Drift check**: regenerated `stencils_arm64.h` is the new
  baseline.

## 11. Verification checklist

- [ ] All ★ Investigate items resolved with documented
      decisions.
- [ ] Smoke test: clang-emitted bytes for `op_load_const_int`'s
      LCI_DST hole = single 4-byte movz on aarch64.
- [ ] All Phase 4 unit-test binaries (`bridge_tests`,
      `admission_tests`, `coldcall_tests`, `extractor-tests`,
      `proto_microbench`) PASS unchanged.
- [ ] Magic-byte audit PASS for all magics (wide existing +
      narrow new).
- [ ] aarch64 stencil byte count drops measurably (target:
      30-op program drops from 2164 bytes to ~1700-1800).
- [ ] aarch64 microbench compile-warm time drops (informational).
- [ ] `rondb_jit_canary` MTR test PASSes.
- [ ] Drift check accepts the new baseline.
- [ ] x86_64 stencil bytes unchanged (Phase 4.5 is aarch64-only
      per ★ I5).

## 12. Out of scope (explicit reminder)

- Addressing-mode folding (per-stencil-family, Phase 5+).
- x86_64 narrow path (per ★ I5).
- Wider lattice work (Phase 5).
- Cold-call branch pattern (Phase 5).
- Generator (Phase 5; per Q3 (b) decision).

## 13. Risks / things that may surprise us

1. **clang's narrow `volatile` codegen surprises us.** Per ★ I1:
   if clang emits an `ldr` from a literal pool instead of `movz`,
   the entire detection strategy breaks. Mitigation: 32-bit
   fallback (★ I2). If 32-bit ALSO fails to produce movz+movk,
   abandon Phase 4.5 and accept the 16-byte chain cost.

2. **Magic collision at 16-bit width.** Per ★ I2: ~0.7%
   collision rate may force regeneration. Mitigation: salt
   variation, multiple regen attempts, fall back to 32-bit if
   needed.

3. **Narrow detector accidentally matches non-magic movz
   instructions.** Per ★ I3: if Pass 2 matches an unrelated
   movz that happens to load a 16-bit value matching one of the
   narrow magics. Mitigation: high-entropy magics make this
   vanishingly unlikely; collision audit extension catches it.

4. **The 13-stencil rewrite drag.** Each updated stencil's bytes
   change; each needs re-review. Mitigation: this is the work
   Phase 5 would have demanded anyway, just done before Phase 5
   instead of after.

5. **Hole struct breakage downstream.** If width-semantics
   changes break some currently-tolerant consumer (e.g., audit
   tool, future tooling), the regression surfaces in the
   audit/test PASS counts. Mitigation: standard regen + audit
   discipline.

## 14. What we learn from Phase 4.5

- Clang's actual narrow-`volatile` codegen on aarch64 — informs
  whether other narrow patterns are viable for Phase 5+.
- Magic-collision behavior at 16-bit width — useful data point
  for any future shrinkage.
- The lower bound on aarch64 stencil size at the
  current architectural level. If addressing-mode folding
  (Phase 5+) lands later, we can compare savings.
- Whether the extractor's two-pass walk (★ I3) generalises
  cleanly to other narrow patterns (e.g., Phase 5's cold-call
  branches' inline operands).
