# RONDB-1056 Phase 4.7 — addressing-mode folding + narrow LoadConst variants

**Status: planning.** Phase 4.7 lands two independent but
complementary optimisations on aarch64:

1. **Addressing-mode folding.** Replace the
   `movz wN, #idx_magic; ldr/str xT, [x20, xN, lsl #3]` two-instr
   pattern with a single `ldr/str xT, [x20, #(idx_magic*8)]` that
   encodes the register index directly into the LDR/STR imm12
   field. Eliminates one MOVZ per memory access — typically 1-3
   instructions per stencil.
2. **Narrow `LoadConst` variants.** Add four bridge-routed
   variants:
   - `op_load_const_uint16` (1 MOVZ + STR, **8 B**) for
     `[0, 65535]`
   - `op_load_const_int16` (MOVZ + SXTH + STR, **12 B**) for
     `[-32768, -1]` (covers the negative-int16 gap that uint16
     can't reach)
   - `op_load_const_uint32` (MOVZ + MOVK + STR, **12 B**) for
     `[0, 4294967295]`
   - `op_load_const_int32` (MOVZ + MOVK + SXTW + STR, **16 B**)
     for negatives `[INT32_MIN, -32769]`
   The existing `op_load_const_int` (now effectively the
   `int64` fallback at **20 B**) handles values that don't fit
   any of the above. Most NDB query literals are small
   non-negative integers and will route to the **8 B uint16**
   stencil — the common-case win.

Aarch64-only; x86_64 stencils unchanged. Branch:
`RONDB-1056-compiled-interpreter`.

## Headline math (predicted)

**Stencil sizes after both optimisations:**

| Stencil | Phase 4.6 (shipped) | Phase 4.7 (predicted) |
|---|---:|---:|
| op_mov_int_int        | 16 B | **8 B**  (LDR + STR, no MOVZ) |
| op_load_col_int       | 20 B | **12 B** |
| op_add/minus/mul      | 28 B | **16 B** |
| op_sum_bigint         | 28 B | **16 B** |
| op_branch_*           | 32 B | **20 B** |
| op_load_const_int (i64) | 24 B | **20 B** (still has wide-chain) |
| op_load_const_int32 (new)  | — | **16 B** |
| op_load_const_uint32 (new) | — | **12 B** |
| op_load_const_int16 (new)  | — | **12 B** |
| op_load_const_uint16 (new) | — | **8 B**  |
| op_load_col_ndb       | 28 B | **20 B** |

**Cumulative trajectory** (stencil-set bytes excluding the new
LoadConst variants):

| Phase | Bytes |
|---|---:|
| 4 baseline      | 1112 |
| 4.5 narrow      |  748 |
| 4.6 inline asm  |  392 |
| **4.7 imm12 fold** | **~240** |

Roughly another ~40 % off Phase 4.6, and ~80 % off Phase 4
baseline.

The imm12-fold win is **per-row** (icache density continues to
improve), not just compile-time. Predicted JIT per-row dispatch:
10.1 ns → ~7-8 ns on the 30-op program. The new LoadConst
variants are bridge-routing wins for queries with small integer
constants (very common — most NDB query literals fit in 32 bits,
many in 16).

## ★ Items to investigate (before coding starts)

Same posture as Phase 4.5 / 4.6 — concrete experiments first.
The spike fits in ~½ day.

> **Day 0 spike resolutions (2026-05-07).** All six items
> resolved via `/tmp/phase47_spike.c` compiled with the pinned
> upstream clang 20.1.8 + a magic-collision Python pass.
>
> **Findings & adjustments to the design:**
>
> 1. **`"n"` works for LDR/STR offset** (I1). Spike emitted
>    `ldr x8, [x20, #0x5090]` and `str x8, [x20, #0x29b8]` with
>    the byte-offset values exactly as written in the C source.
>    The encoded imm12 = byte_offset / 8 sits at bits 21..10.
>    LDR mask `0xFFC00000` → `0xF9400000`; STR mask →
>    `0xF9000000`.
>
> 2. **12-bit magic space is workable** (I2). 32 candidate
>    fold magics generated from
>    `sha256("RONDB-1056-Phase4_7-fold-magic-v1|" + name +
>    optional_counter)[0:12-bit]` produced exactly 1 collision
>    (`MINUS_B_FOLD` ↔ `ADD_A_FOLD` at `0x767`). Salt-rotation
>    via `#counter` suffix on collision resolved it in 1
>    retry. Final 32-magic set documented inline.
>
> 3. **Fold + narrow detection no-conflict** (I3). A fold-only
>    stencil emits zero MOVZ instructions; the narrow audit
>    finds 0 matches. No false positives.
>
> 4. **Bridge sees the constant** (I4). `ndb_jit_bridge.c:183`
>    reads `int64_t value` directly from the bytecode stream
>    inside `BR_kOpLoadConst` before calling `emit_op` — adding
>    the variant dispatch is ~5 lines.
>
> 5. **2-slot chain emits W-form (32-bit) MOVZ+MOVK** (I5),
>    encoding `0x52800000 / 0x72800000`, NOT the X-form
>    `0xD2/0xF2` that the existing pass-2 chain detector
>    targets. Same situation as Phase 4.5's narrow MOVZ. Pass-2
>    needs sf-agnostic mask relaxation (mask `0x7F800000` → match
>    `0x52800000` for MOVZ / `0x72800000` for MOVK), plus a
>    `chain_len` field per magic so the detector emits Hole
>    entries when the right number of slots are seen (2 for
>    int32 chain, 4 for the int64 wide chain).
>
> 6. **Width=1 patcher math is straightforward** (I6). Mask
>    `0xFFFu << 10` (= `0x3FFC00`), shift the operand value
>    left by 10. Verified by hand against the AArch64 ARM ARM.
>
> **Sign-extension surprise** (not in the original ★ list).
> `(int64_t)(int16_t)v` and `(int64_t)(int32_t)v` casts emit a
> `sxth x8, w8` / `sxtw x8, w8` instruction (4 bytes each), so
> the LCI16/LCI32 stencils are 4 bytes larger than the
> optimistic estimate. Updated predictions:
> - `op_load_const_int16`: **12 B** post-strip (was 8 B in the
>   plan)
> - `op_load_const_int32`: **16 B** post-strip (was 12 B)
>
> The `sxth` / `sxtw` is unavoidable for correct sign-extension
> across the int16 / int32 ranges (MOVZ is unsigned-only).
> Still much smaller than the int64 form's 20 B.
>
> **Spike disassembly** (representative):
>
> | Spike stencil | Pre-strip | Post-strip | Phase 4.6 | Saving |
> |---|---:|---:|---:|---:|
> | s1_mov_fold (mov + imm12)         | 12 B |  8 B | 16 B | −50 % |
> | s2_add_fold (add + imm12 ×3)      | 20 B | 16 B | 28 B | −43 % |
> | s3_lci16 (movz + sxth + str)      | 16 B | 12 B | n/a (new) | — |
> | s4_lci32 (movz + movk + sxtw + str) | 20 B | 16 B | n/a (new) | — |
> | s5_lcu16 (movz + str)             | 12 B |  8 B | n/a (new) | — |
> | s6_lcu32 (movz + movk + str)      | 16 B | 12 B | n/a (new) | — |
>
> The unsigned variants drop the `sxth` / `sxtw` because the
> W-form MOVZ (and MOVZ+MOVK) already zero-fills the upper bits
> of the X-register — exactly what an unsigned constant needs.
> Saves 4 B per LoadConst stencil for non-negative values, the
> common case in NDB query literals.

### ★ Investigate I1 — clang accepts `"n"` constraint for LDR/STR imm offset

Today's narrow MOVZ uses inline asm:
```c
__asm__ volatile ("movz %w[out], %[m]" : [out] "=r"(v) : [m] "n"(magic & 0xFFFFu));
```

For LDR/STR fold we need:
```c
__asm__ volatile (
  "ldr %[out], [%[base], %[off]]"
  : [out] "=r"(v)
  : [base] "r"(state),
    [off]  "n"((MAGIC * 8) & 0x7FF8u)
);
```

The assembler resolves `%[off]` as a 12-bit unsigned offset
(scaled by 8 for X-form). **Spike:** compile a minimal helper,
disassemble, confirm:
- The bytes are a single `ldr` instruction (no `movz` ahead of
  it).
- The instruction's imm12 field at bits 21..10 contains the
  expected magic (encoded as `magic`, since the assembler
  divides by 8 for the scaled form).
- The instruction's encoding family matches the LDR (immediate,
  unsigned offset, X-form): `0xF9400000` (LDR) or `0xF9000000`
  (STR) with the appropriate masks.

If clang refuses the `"n"` constraint for an LDR offset, fall
back to embedding the magic via `"i"` or hand-encoding the
instruction as a `.word` directive in the asm string.

### ★ Investigate I2 — imm12 collision space at 12 bits

The narrow magics are 16-bit. The fold magics will be 12-bit
(after the assembler-side /8 scaling). 4096 distinct values
versus 65536. With ~30 fold magics needed across the stencil
set, expected birthday-style collision among magics is small
(~4 % chance with random 12-bit values), and false positives
from non-hole LDR/STR instructions in stencils are bounded by
the number of non-hole LDR/STR (very few — mostly the spilled
helper-call frame in `op_load_col_ndb`).

**Spike:** generate 30 candidate 12-bit magics from
`sha256("RONDB-1056-Phase4_7-fold-magic-v1|" + name)[0:12-bit]`,
check for collisions, regenerate any colliders. Document the
salt + procedure (mirrors Phase 4.5's narrow magic generation).

### ★ Investigate I3 — does fold detection fight current narrow detection?

Pass-3 today walks every MOVZ and matches imm16 against the
narrow table. After Phase 4.7, register-index MOVZ instructions
disappear — they're replaced by LDR/STR imm12 patterns. The
narrow table effectively shrinks to just the wide HK_OP_IMM
chain. **Spike:** confirm that a stencil with no HK_OP_A/B/C
narrow MOVZ doesn't trigger any false-positive narrow audit
hits. (Other LDR/STR instructions could carry imm12 patterns
that resemble narrow magics? Unlikely since the narrow magic
table values are 16-bit and won't match any 12-bit field, but
worth verifying.)

### ★ Investigate I4 — do `op_load_const_int16/32` wire cleanly through the bridge?

NDB's aggregation interpreter has `kOpLoadConstBigint` carrying
a 64-bit constant. The bridge today routes all of these to
`op_load_const_int`. Phase 4.7 wants the bridge to inspect the
constant value:

| Constant range | Routes to | Stencil size |
|---|---|---|
| `-2^15 ≤ v ≤ 2^16-1` (fits in MOVZ imm16) | `op_load_const_int16` | 8 B |
| `-2^31 ≤ v ≤ 2^32-1` (fits in MOVZ + MOVK) | `op_load_const_int32` | 12 B |
| else | `op_load_const_int` (today's variant) | 20 B |

**Spike:** verify `JitBridge::admit_op` sees the constant value
when it processes a kOpLoadConstBigint — yes, the value is in
the bytecode word stream right after the opcode. The dispatch
is a simple constant-range check.

For aarch64 MOVZ the imm16 must be unsigned ≥ 0; negative values
need to use MOVN (move-not). To keep the stencil set small,
we admit only constants that fit unsigned MOVZ (0..65535) into
the int16 stencil; negatives fall through to int32 or int64.

### ★ Investigate I5 — extractor pass for 2-instruction MOVZ+MOVK chain

The existing pass-2 chain detector requires `slot_seen == 0xF`
(all four 16-bit slots written). For `op_load_const_int32`'s
2-slot chain we need to relax that check.

**Options**:

- **(a)** Extend pass-2 to accept partial chains: emit Hole
  entries when `slot_seen` matches the expected pattern for the
  declared chain length (`0x3` for 2-slot, `0xF` for 4-slot).
  Requires the magic table to declare chain length per entry.
- **(b)** Add a new pass-2.5 that detects 2-slot MOVZ+MOVK
  targeting the same Rd matching declared 2-slot magics.

(a) is simpler. **Spike:** prototype the pass-2 relaxation;
verify wide-chain detection still works for HK_OP_IMM, and that
2-slot chain detection picks up the int32 magic in
`op_load_const_int32`.

### ★ Investigate I6 — patcher path for imm12 fold

The patcher today handles `width=2` (narrow MOVZ, 16-bit imm at
bits 5..20 of one instruction) and `width=4` (wide chain,
4×16-bit imms via `slot_counter`). Phase 4.7 needs a third
path:

- **`width=1`** — imm12 in LDR/STR (12-bit imm at bits 10..21
  of one instruction). The patcher writes `op->kind` value into
  bits 10..21 (with any sign-bit handling — register indices
  are unsigned 0..255, no concern in practice).

**Spike:** sketch the patcher branch:
```c
if (hole->width == 1) {
  /* LDR/STR imm12 fold. */
  uint16_t imm12 = (uint16_t)v & 0x0FFFu;
  rmw_insn_word(patch, 0xFFFu << 10, (uint32_t)imm12 << 10);
}
```

Verify the mask + shift positions match the AArch64 LDR/STR
unsigned-offset encoding.

## 1. Scope

**In scope.**
- New `HOLE_FOLD_LDR(name)` / `HOLE_FOLD_STR(name)` source
  macros that emit imm12-folded LDR/STR with a patched register
  index. Replace the `regs_i64[HOLE_NARROW(...)]` array-index
  pattern in all hot stencils.
- New `HOLE_32(name)` helper for 2-slot MOVZ+MOVK chain (used by
  `op_load_const_int32`).
- New `op_load_const_int16` and `op_load_const_int32` stencils.
- Bridge dispatch: route `kOpLoadConstBigint` to the smallest-
  fitting stencil variant.
- Extractor pass-2 relaxation to detect 2-slot chains.
- Extractor pass-4 to detect LDR/STR imm12 holes.
- Audit table extension for the new fold + 2-slot magics.
- Patcher `width=1` branch in `jit1.c`.
- 14 existing stencils flipped from `regs_i64[narrow_idx]` to
  the new fold pattern.
- 2 new stencils added.
- regen-stencils PASS for all magics (1 wide + 30 narrow + ~30
  fold + ~2 32-slot).

**Out of scope.**
- x86_64 — already optimal.
- Bridge dispatch for non-int constants (uint, float) — Phase 5.
- Other addressing-mode opportunities (immediate-shifted ADD,
  MOV-to-zero, etc.) — Phase 5.
- 8-bit / typed register variants — Phase 5.
- Cold-call branch family — Phase 5.

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
├── stencils_src.c              ← 2 new helpers, 14+2 stencils touched
├── hole_kinds.h                ← ~30 new fold magics, ~2 32-slot magics
├── jit1.c                      ← width=1 patcher branch
├── bytecode1.h                 ← 2 new OpKind values (LCI16, LCI32)
└── extract_stencils/
    ├── extract_stencils.c      ← pass-2 relaxation, pass-4 LDR/STR walk
    └── audit_magics.c          ← fold table + 32-slot table, audit passes

storage/ndb/src/kernel/blocks/dbtup/jit/
└── bridge.c (or wherever kOpLoadConstBigint dispatches)
                                 ← constant-range check + variant routing
```

## 3. Source patterns

**Imm12-folded LDR/STR** (replaces the
`regs_i64[HOLE_NARROW(idx)]` array-subscript pattern):

```c
/* Reads an i64 from regs_i64[idx] where idx is patched at JIT
 * compile time. Emits a single ldr instruction; bytes 10..21
 * carry the patched imm12 (= idx). */
__attribute__((always_inline))
static inline int64_t aarch64_load_reg_(uint32_t magic_byte_off,
                                         const JitState *s) {
  int64_t v;
  __asm__ volatile (
    "ldr %[out], [%[base], %[off]]"
    : [out] "=r" (v)
    : [base] "r"  (s),
      [off]  "n"  (magic_byte_off & 0x7FF8u)
  );
  return v;
}

#  define HOLE_LOAD_REG(name, state)  \
      aarch64_load_reg_(MAGIC_##name##_FOLD * 8, (state))
```

Analogous `HOLE_STORE_REG(name, state, value)` for stores. The
caller writes:
```c
STENCIL op_mov_int_int(JitState *s) {
  HOLE_STORE_REG(MV_DST, s, HOLE_LOAD_REG(MV_SRC, s));
  TAIL_NEXT(s);
}
```

**32-bit constant chain** (used by `op_load_const_int32`):

```c
__attribute__((always_inline))
static inline uint64_t aarch64_hole32_(uint32_t magic) {
  uint64_t v;
  __asm__ volatile (
    "movz %w[out], %[a]\n\t"
    "movk %w[out], %[b], lsl #16"
    : [out] "=r"(v)
    : [a] "n"( magic        & 0xFFFFu),
      [b] "n"((magic >> 16) & 0xFFFFu)
  );
  return v;
}

#  define HOLE_32(name)  aarch64_hole32_(MAGIC_##name##_32)
```

**Two new stencils**:

```c
DECLARE_NARROW_HOLE(LCI16_DST);
DECLARE_NARROW_HOLE(LCI16_VAL);
STENCIL op_load_const_int16(JitState *s) {
  HOLE_STORE_REG(LCI16_DST, s,
                 (int64_t)(int16_t)HOLE_NARROW(LCI16_VAL));
  TAIL_NEXT(s);
}

DECLARE_NARROW_HOLE(LCI32_DST);
DECLARE_HOLE32(LCI32_VAL);
STENCIL op_load_const_int32(JitState *s) {
  HOLE_STORE_REG(LCI32_DST, s,
                 (int64_t)(int32_t)HOLE_32(LCI32_VAL));
  TAIL_NEXT(s);
}
```

Sign-extension via `(int16_t)` / `(int32_t)` cast pre-extends the
operand value to int64 before the store. clang lowers this to a
`sxth` / `sxtw` instruction (or folds it into the LDR/STR
addressing — TBD in spike).

## 4. Magic generation

**Fold magics** (per-hole, ~30 total): 12-bit values from
`sha256("RONDB-1056-Phase4_7-fold-magic-v1|" + name)[0:12-bit]`.
Stored as `magic_byte_off = magic_idx * 8` so the source
expression can be `(magic_byte_off & 0x7FF8u)` and the assembler
encodes `magic_byte_off / 8 = magic_idx` into bits 21..10.

**32-slot magics** (`MAGIC_LCI32_VAL_32`): 32-bit values from
the same salt, both 16-bit halves non-zero so clang emits a full
2-instruction MOVZ+MOVK chain.

`hole_kinds.h` gains:
- `MAGIC_*_FOLD` constants (~30 of them)
- `kHoleFoldMagicTable[]` parallel to `kHoleNarrowMagicTable`
- `MAGIC_LCI32_VAL_32` (or similar)
- `kHole32MagicTable[]` (just one entry initially)

## 5. Extractor / audit changes

**Pass-2 relaxation** (for 2-slot int32 chains): the chain
detector emits Hole entries when `slot_seen` matches the
declared length. The wide-chain table gets a `chain_len` field
(4 for the existing wide entries, 2 for the new 32-slot entry).

**Pass-4 (new): LDR/STR imm12 fold detection.** After pass-3,
walk every 4-byte instruction; for any LDR or STR (immediate,
unsigned offset, X-form) check imm12 against
`kHoleFoldMagicTable`. Emit Hole entries with `width=1` for
matches.

The encoding masks:
- LDR Xt, [Xn, #imm12]: `(insn & 0xFFC003E0) == 0xF9400000`
  (Rt at bits 4..0, Rn at bits 9..5, imm12 at bits 21..10)
- STR Xt, [Xn, #imm12]: `(insn & 0xFFC003E0) == 0xF9000000`

**Audit changes:**
- New `count_fold_matches_arm64(bytes, n_bytes, magic)` that
  walks LDR/STR instructions and counts imm12 == magic.
- New `kFoldMagicToStencil[]` mapping (parallel to existing
  narrow / wide tables).
- Pass continues to require 1× per declaring stencil + 0×
  elsewhere, same invariant as Phase 4.5/4.6.

## 6. Engine patcher (jit1.c)

New `width=1` branch in the operand-patch path:

```c
case HK_OP_A:
case HK_OP_B:
case HK_OP_C: {
  int64_t v = hole_value_from_op(hole->kind, op);
  if (hole->width == 1) {
    /* Phase 4.7: LDR/STR imm12 fold. The register index is
     * written to bits 21..10 (the imm12 field). */
    rmw_insn_word(patch, 0xFFFu << 10,
                  ((uint32_t)v & 0xFFFu) << 10);
  } else if (hole->width == 2) {
    /* Phase 4.5 narrow MOVZ — slot=0, no slot_counter bump. */
    patch_operand(patch, 0, v);
  } else {
    /* Wide chain. */
    uint8_t slot = slot_counter[hole->kind]++;
    patch_operand(patch, slot, v);
  }
  break;
}
```

For HK_OP_IMM with chain length 2 (op_load_const_int32), the
existing slot_counter machinery works unchanged — slot 0..1
write 16-bit slices to bits 5..20 of consecutive MOVZ/MOVK.

## 7. Bridge / admission

`ndb_jit_bridge.c` for `BR_kOpLoadConst` inspects the constant
value and picks the smallest-fitting variant. Smallest-first
order so each fast path exits early:

```c
case BR_kOpLoadConst: {
  /* ... existing parsing of reg_index + value ... */
  OpKind kind;
  if (0 <= value && value <= 0xFFFFLL) {
    kind = OP_LOAD_CONST_UINT16;          /*  8 B */
  } else if (-32768 <= value && value <= -1) {
    kind = OP_LOAD_CONST_INT16;           /* 12 B (negative int16 only) */
  } else if (0 <= value && value <= 0xFFFFFFFFLL) {
    kind = OP_LOAD_CONST_UINT32;          /* 12 B */
  } else if ((int64_t)(int32_t)value == value) {
    kind = OP_LOAD_CONST_INT32;           /* 16 B (negative int32) */
  } else {
    kind = OP_LOAD_CONST_INT;             /* 20 B (full int64) */
  }
  if (!emit_op(out_prog, kind, reg_index, 0, 0, value)) { ... }
  break;
}
```

Note the second arm checks `-32768 ≤ v ≤ -1` (not the full
int16 range), since values in `[0, 32767]` are already handled
by the cheaper UINT16 path above. Same logic for INT32 — only
negatives hit it after the UINT32 branch covers `[0, 2³²−1]`.

The four new OpKind values land at the next free slots in
`bytecode1.h`. Existing `OP_LOAD_CONST_INT` keeps its number
(append-only invariant).

## 8. Stencils to update / add

**Update** (14 existing stencils, all switch from
`regs_i64[HOLE_NARROW(idx)]` to `HOLE_LOAD_REG/STORE_REG`):

`op_load_const_int`, `op_load_col_int`, `op_mov_int_int`,
`op_add_int_int`, `op_minus_int_int`, `op_mul_int_int`,
`op_sum_bigint`, `op_branch_lt/le/eq/gt/ge/ne_int_int`,
`op_load_col_ndb`.

**Add** (4 new stencils):

`op_load_const_uint16` (8 B), `op_load_const_int16` (12 B),
`op_load_const_uint32` (12 B), `op_load_const_int32` (16 B).
The existing `op_load_const_int` becomes the int64 fallback at
20 B (unchanged from Phase 4.6).

## 9. Day breakdown

**Day 0 (~½ day) — investigation spike.** Resolve ★ I1-I6 with
a minimal `/tmp/phase47_spike.c`. Verify the LDR-fold codegen,
12-bit collision space, pass-2 relaxation feasibility, and
bridge dispatch. Output: spike findings noted inline in this
plan.

**Day 1 (~4h) — imm12 fold infrastructure.**
- Generate 30 fold magics; commit to `hole_kinds.h`.
- Add `HOLE_LOAD_REG/STORE_REG` helpers to `stencils_src.c`.
- Extend extractor with pass-4; extend audit.
- Add `width=1` patcher branch.

**Day 2 (~4h) — flip stencils to fold.** Convert all 14
existing stencils from narrow-MOVZ register-index pattern to the
fold pattern. regen-stencils, audit PASS, run all unit tests +
microbench differential. Cumulative byte total should drop from
~390 B to ~240 B.

**Day 3 (~4h) — narrow LoadConst variants (uint16, int16) +
bridge dispatch.**
- Add `op_load_const_uint16` (no sxth) and
  `op_load_const_int16` (with sxth) stencils.
- Add OP_LOAD_CONST_UINT16, OP_LOAD_CONST_INT16 to bytecode1.h,
  kOpkindMap.
- Bridge dispatch: smallest-fitting variant per range table
  (uint16 first, then int16 for `[-32768, -1]`).
- Bridge unit test for the dispatch logic (asserts each value
  routes to the expected variant).
- regen-stencils PASS, microbench validation.

**Day 4 (~4h) — wider LoadConst variants (uint32, int32) +
2-slot chain detection.**
- Add `HOLE_32` helper.
- Extend pass-2 chain detector with sf-agnostic mask
  (`0x7F800000`) AND a `chain_len` field per magic, so 2-slot
  W-form chains are detected alongside 4-slot X-form chains.
- Add the 32-slot magics + audit entries.
- Add `op_load_const_uint32` (no sxtw) and
  `op_load_const_int32` (with sxtw) stencils.
- Add OP_LOAD_CONST_UINT32, OP_LOAD_CONST_INT32 to bytecode1.h,
  kOpkindMap. Bridge dispatch routes to them.
- regen-stencils PASS, microbench validation.

**Day 5 (~3h) — drift check, MTR, `phase_4_7_addr_mode.md`
results doc, plan.md flip to shipped.**

**Total: 5-6 days** (excluding the Day 0 spike).

## 10. Test approach

- Day 0 smoke: hand-disassemble a fold-pattern stencil; confirm
  the LDR/STR carries the expected imm12.
- Existing wide / narrow audit invariants continue to hold (1×
  per stencil, 0× elsewhere); fold + 32-slot get the same
  invariant.
- `proto_microbench` differential aggregates match interp on
  the 30-op program AND the forked program.
- New variant routing: bridge unit test that a kOpLoadConstBigint
  with constant=42 routes to LCI16, with constant=100000 routes
  to LCI32, with constant=10^12 routes to LCI64.
- `rondb_jit_canary` MTR PASSes (the canary's queries use small
  integer constants — should auto-route to LCI16 via the new
  bridge dispatch and exercise the new path end-to-end).

## 11. Verification checklist

- [ ] All ★ Investigate items resolved with documented decisions.
- [ ] Smoke test: clang-emitted bytes for op_mov_int_int = 2
      instructions (LDR + STR), 8 bytes total.
- [ ] Audit PASS for all magics (1 wide LCI_VAL, 30 narrow,
      ~30 fold, 1 32-slot).
- [ ] All Phase 4 unit-test binaries PASS unchanged.
- [ ] Bridge dispatch unit test: small constants route to LCI16,
      medium to LCI32, large to LCI64.
- [ ] aarch64 stencil byte count drops from ~390 B to ~240 B.
- [ ] aarch64 microbench JIT per-row drops from 10 ns to ~7-8 ns.
- [ ] `rondb_jit_canary` MTR PASS.
- [ ] Drift check accepts the new baseline.
- [ ] x86_64 stencil bytes unchanged.

## 12. Out of scope (explicit reminder)

- x86_64 imm12 fold — different addressing modes, not symmetric.
  x86_64 already encodes the operand into a `mov reg, [base +
  disp32]` form for free, no fold needed.
- `op_load_const_uint16/32` (unsigned constant variants) —
  Phase 5 typed-register work.
- `op_load_const_double` and floating-point arithmetic —
  Phase 5.
- Cold-call branch pattern — Phase 5.
- Embedded normal-interp fallback — Phase 5.

## 13. Risks / things that may surprise us

1. **clang refuses `"n"` for LDR offset.** Mitigation: hand-
   encode the LDR via `.word` with bit-fiddled imm12. Ugly but
   works. Detected at Day 0.

2. **Sign-extension for int16/int32 constants requires extra
   instructions.** clang's `(int16_t)` / `(int32_t)` cast
   typically lowers to one `sxth` / `sxtw` — 4 bytes per stencil.
   If it lowers to two instructions, the size advantage shrinks.
   Detected at Day 0 spike for the LCI16/LCI32 stencils.

3. **12-bit magic collision** — only 4096 distinct values; with
   30 magics the birthday collision odds are ~10 %. The salted
   sha256 plus a regeneration loop (drop colliders, retry) keeps
   this low. The audit catches any collision at regen time.

4. **2-slot pass-2 detection breaks 4-slot detection.** Mitigation:
   the chain length is declared per magic, so the detector
   matches against the *expected* length per match. A 4-slot
   chain that only filled 2 slots can't match a 2-slot magic
   because the magic's value (full 32-bit) is computed from
   slots 0+1 only — and that's what gets emitted by the 2-slot
   asm block.

5. **Bridge dispatch performance.** Adding a value-range check
   per kOpLoadConstBigint is one extra branch per LoadConst at
   admission time. Admission already runs O(n) over the program;
   one branch is negligible.

6. **`op_load_const_int16` performance regression on uniform
   workloads.** If a query happens to hit only int16 constants,
   the JIT now picks the 8-byte stencil — but if the bridge ever
   misroutes a value that exceeds int16 range, it'd silently
   truncate. Mitigation: assert in admission that the value
   actually fits the chosen variant; admission failure routes to
   the next-larger variant. Same risk class as today's
   bytecode-bridge admit checks.

## 14. What we learn from Phase 4.7

If both optimisations land as predicted:

- **The narrow-MOVZ infrastructure was a stepping stone.** With
  the imm12 fold in place, register-index narrow MOVZ
  effectively disappears from the stencil set (only HK_OP_IMM in
  op_load_const_int still uses the wide chain). The narrow magic
  table shrinks to almost nothing — but that's a feature: each
  optimisation pass folded one more piece of the operand into
  the surrounding instruction.

- **Bridge-time variant selection is the right knob for type-
  driven specialisation.** Phase 5's typed-register work will
  generalise this — e.g., one stencil per (DST type, A type,
  B type) combination of an arithmetic op. Phase 4.7's
  int16/int32 split is a small precursor.

- **The 4.7 stencil set is roughly icache-resident** at 240 B
  for the hot subset. A 30-op program emits ~400 B, well within
  L1 icache (typically 32-128 KB on aarch64). At this point
  per-row dispatch is bound by *data*-cache (the regs_i64 loads/
  stores), not icache.

## References

- Phase 4.6 results doc: `phase_4_6_inline_asm.md`.
- Phase 4.5 results doc: `phase_4_5_narrow_holes.md`.
- AArch64 ARM ARM, section C6.2.119 (LDR immediate, unsigned
  offset) — for the imm12 encoding details.
- Plan.md §10 (Phase 4 admission walk) — for the bridge
  dispatch model.
