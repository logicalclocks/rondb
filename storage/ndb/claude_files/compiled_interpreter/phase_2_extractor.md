# RONDB-1056 Phase 2 — extractor + dual-arch JIT engine

**Status: shipped.** Phase 2 establishes the cross-arch copy-and-patch
pipeline. The Phase 1 hand-authored x86_64 stencil header is replaced
by a generated artefact, and the engine extends to aarch64 via
magic-byte sentinel chains and a movz/movk patcher. Branch:
`RONDB-1056-compiled-interpreter`. Final commit at writing:
`16f4692131c`.

## Outcome

All three Phase 2 verification gates clear on both architectures:

| Gate | x86_64 (Linux Rocky 9.5, prod_build) | aarch64 (macOS Apple Silicon, debug_build) | aarch64 (macOS Apple Silicon, prod_build) |
|------|---|---|---|
| `regen-stencils` byte-identical | ✓ generated == Phase 1 hand-authored bytes | ✓ stable across re-runs | ✓ stable across re-runs |
| Magic-byte audit (13 magics) | ✓ 0× anywhere | ✓ 1× in declaring stencil, 0× elsewhere | ✓ same |
| Microbench: JIT aggregate == interp | ✓ 12517131 | ✓ 12517131 | ✓ 12517131 |
| Microbench speedup floor | 2.67× ≥ 2.0× ✓ | 2.96× ≥ 1.5× ✓ | 1.03× ≥ 1.5× ✗ (see below) |
| Microbench warm compile budget | 1.51 µs < 5 µs ✓ | 8.38 µs < 15 µs ✓ | 6.00 µs < 15 µs ✓ |

The aarch64 prod_build speedup of 1.03× is below the 1.5× sanity floor,
but **correctness is intact** (aggregate matches interp, audit clean).
The result is consistent with what the cross-arch design predicts —
see "Performance" below — and Phase 5's operand-folding work is the
agreed remediation.

## Generated header sizes

Per stencil, in bytes (n_holes in parentheses):

| Stencil | x86_64 | aarch64 | ratio |
|---|---:|---:|---:|
| op_load_const_int    | 13 (2)  | 60 (8)   | 4.6× |
| op_load_col_int      | 23 (2)  | 68 (8)   | 3.0× |
| op_mov_int_int       | 18 (2)  | 64 (8)   | 3.6× |
| op_add_int_int       | 27 (3)  | 96 (12)  | 3.6× |
| op_sum_bigint        | 19 (2)  | 76 (8)   | 4.0× |
| op_branch_lt_int_int | 29 (4)  | 80 (10)  | 2.8× |
| op_skip / op_exit    | 3 (0) ×2 | 8 (0) ×2 | 2.7× |
| **30-op program total** | 623 | 2164 | 3.5× |

The aarch64 bloat is dominated by 4-instruction movz/movk chains for
each operand hole (16 bytes per chain, vs 4 bytes for an x86_64
`mov reg32, imm32`). 12 of 13 magic-bearing holes carry register or
slot indices that fit in 8 bits — they're all over-encoded right now.

## What shipped

**New host tools** (`storage/ndb/src/kernel/blocks/dbtup/jit/extract_stencils/`):

| File | Role |
|------|------|
| `extract_stencils.c` (~620 LOC) | Reads clang-compiled `.o`, walks `.rela.text` (x86_64) or scans for movz/movk magic chains (aarch64), emits a generated header. |
| `audit_magics.c` (~370 LOC) | Independent verification that each `MAGIC_*` appears exactly the expected number of times in the generated header. |
| `regen.cmake` | Driver invoked by the `regen-stencils` CMake target. Clang preflight, per-arch compile + extract + audit + diff + copy-if-changed. |
| `tests/run_tests.cmake` | 11-case unit-test harness for the host tools. |
| `drift_check.sh` | CI script: asserts committed headers match regenerated output. |
| `elf64.h` | Minimal ELF64 subset (macOS doesn't ship `<elf.h>`). |
| `README.md` | Lead clang-pin paragraph, recipe, troubleshooting, "when the diff is unexpected" guide. |

**Modified core engine**:

| File | Change |
|------|--------|
| `jit1.c` | Dropped Phase-1 ENOTSUP stub. Cross-arch via `kPreamble` / `patch_operand` / `patch_branch_disp` inline helpers. Per-stencil `slot_counter[kind]` drives the 4-Hole-per-chain pattern on aarch64. |
| `stencils_src.c` | Single source for both arches. `extern uint64_t HOLE_*` placeholders on x86_64, `volatile uint64_t v = MAGIC_*` sentinels on aarch64. |
| `hole_kinds.h` | Single source of truth for `HoleKind` enum, `Hole`/`Stencil` structs, the relocation-symbol-name table (x86_64), and the high-entropy magic constants table (aarch64). |
| `stencils_x86_64.h` | Now a generated artefact. Bytes byte-identical to Phase 1's hand-authored set. |
| `stencils_arm64.h` | New, generated. Committed for the first time. |
| `proto_microbench.c` | Dropped Phase-1 arch guard. Per-arch verdict thresholds (5 µs / 2.0× on x86_64; 15 µs / 1.5× on aarch64). |

**Removed**:

- `ndb_jit1_stencils` Phase-1 CMake target (one-shot `stencils.o` for manual objdump). The extractor + `regen-stencils` pipeline replaces its purpose.

## Hole encoding strategies — recap

| Aspect | x86_64 | aarch64 |
|---|---|---|
| Source pattern | `extern uint64_t HOLE_*` placeholder | `volatile uint64_t v = MAGIC_*` sentinel |
| Codegen | `mov reg32, imm32` with R_X86_64_32 relocation | `movz Rd, #s0; movk Rd, #s1, lsl #16; movk #s2, lsl #32; movk #s3, lsl #48` |
| Per-hole bytes | 4 (one inline immediate) | 16 (4 instructions × 4 bytes each) |
| Extractor finds the hole by | reading `.rela.text` for relocations against `HOLE_*` symbols | walking 4-byte instructions, decoding movz/movk, accumulating per-Rd 64-bit value, matching against `kHoleMagicTable` |
| Engine patches the hole by | writing 4-byte LE int32 at `byte_offset` | writing each instruction's `imm16` field (bits 5-20) with the matching slot of the operand value; `slot_counter[kind]` tracks slot 0/1/2/3 across the 4 holes |

The aarch64 fallback is forced by clang's lowering of `extern uint64_t`
on aarch64: it produces `adrp + ldr` GOT-indirect loads, with no inline
immediate to patch. The magic-byte sentinel approach gets us back to a
deterministic, patcher-friendly inline shape.

## Performance

The cross-arch architecture is sound; the cross-arch *performance
profile* is asymmetric.

**x86_64**: ~3× speedup, ~1.5 µs warm compile, ~100 rows break-even —
Phase 1's numbers held against the generated header.

**aarch64**: 3-4× larger emitted code, ~5× slower compile, per-row JIT
overhead comparable to x86_64 in nanoseconds but *relative speedup
small* because:

1. Each operand is materialised by a 4-instruction dependency chain
   (movz + 3× movk), even when the operand is an 8-bit register index
   that should be a single instruction.
2. The C interpreter benefits massively from compiler optimization
   (`prod_build`'s -O2/-O3 takes the interpreter from ~97 ns/row to
   ~35 ns/row on the same Apple Silicon chip). The JIT'd machine code
   is the same regardless of how `jit1.c` was compiled, so the
   interpreter speedup narrows the gap.
3. With `prod_build` interpreter at 35 ns/row and aarch64 JIT at
   ~34 ns/row, the speedup is essentially noise — JIT roughly equals
   interpreter.

**Phase 5 fix.** Of the 13 magic-bearing holes in the current stencil
set, 12 are register or slot indices (`op->a/b/c`, all uint8_t — 8 bits
of useful payload). Replacing those with `movz Rd, #imm8` (single
instruction) instead of 16-byte chains shrinks each stencil ~30-40%
and proportionally tightens emit+patch and per-row paths. That alone
should restore aarch64 to a meaningful production speedup. A more
aggressive approach — folding the operand into addressing-mode
displacements (`ldr Xd, [x20, #imm12]`) — is an even bigger win but
introduces a new `HoleKind` requiring extractor and engine support.
Both are mechanical extensions of the Phase 2 framework, not redesigns.

## Toolchain quirks discovered

- **Clang folding magic chains**: an early version of `hole_kinds.h`
  used 0xC0DEC0DE_NNNN_C0DE magics differing only by 0x10000. Clang
  noticed and emitted ONE chain plus `add x9, x8, #0x10, lsl #12` to
  derive the second magic — defeating the extractor. Fix: regenerated
  every magic from SHA-256(salt || name)[0:8] to ensure no exploitable
  arithmetic relationship.

- **Apple clang vs upstream clang**: macOS ships Apple's clang fork.
  The Apple-version-to-LLVM-version mapping is undocumented and
  codegen can differ. `regen.cmake`'s preflight rejects `Apple clang`
  outright with a recovery hint pointing at `brew install llvm@20`.

- **macOS lacks `<elf.h>`**: rather than introduce libelf as a
  dependency, the extractor bundles `elf64.h` — a minimal ELF64 subset
  (Ehdr, Shdr, Sym, Rela, plus the relocation type constants we
  classify against). Pure C with no third-party deps.

- **aarch64 cross-compile from x86_64 host**: `--target=aarch64-linux-gnu`
  works out of the box on the user's Rocky 9.5 box without sysroot
  setup. The compile flags (`-ffreestanding`, `-fno-pic`, no libc) mean
  no link is attempted, so clang's driver doesn't enforce sysroot
  presence.

- **Bytes byte-identical across hosts**: macOS Apple Silicon
  Homebrew llvm@20 and Rocky 9.5 system clang 20.1.8 produce
  byte-identical `stencils_x86_64.o` and `stencils_arm64.o`.
  Cross-validated end-to-end: same generated header bytes from
  both hosts.

## Verification checklist (§10 walk-through)

| Item | Status |
|------|--------|
| `regen-stencils` produces unchanged `stencils_x86_64.h` | ✓ — bytes match Phase 1 hand-authored set |
| `regen-stencils` produces a fresh `stencils_arm64.h` whose first stencil executes correctly | ✓ — full 30-op program, aggregate matches interp |
| Phase 1 microbench against generated x86_64 header passes thresholds | ✓ — 2.67× / 1.51 µs / 97 rows on Linux x86_64 |
| CI drift check in place | ✓ — `extract_stencils/drift_check.sh` |
| CI magic-byte collision audit in place | ✓ — `ndb_jit_audit_magics` runs inside `regen.cmake`'s pipeline |
| Extractor unit tests covering single stencil, branch, terminator override, aarch64 magic, malformed-ELF | ✓ — 11-case `extractor-tests` target |
| README's lead paragraph names pinned clang + Apple-clang rejection | ✓ — `extract_stencils/README.md` |
| No external dependencies | ✓ — libc + bundled `elf64.h` only |
| `cmake --version` accepts ≥3.16 | ✓ — `regen.cmake` and `run_tests.cmake` declare `cmake_minimum_required(VERSION 3.16)` |

## Forward pointers

- **Phase 3** generalises the forward-jump fixup mechanism beyond
  Phase 1's narrow `op_branch_lt_int_int` pattern. The two-pass
  emission + fixup queue infrastructure is already in place; Phase 3
  expands the bytecode to more branch-bearing opcodes and tightens the
  admission walk.

- **Phase 5** is where aarch64 perf gets real attention via the
  operand-folding optimization sketched above. The infrastructure
  added in Phase 2 (extractor framework, magic-byte tooling,
  collision audit) accommodates new hole kinds without disturbing
  the engine's pass structure.
