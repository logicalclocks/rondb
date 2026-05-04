# Phase 2 — Implementation Plan (RONDB-1056)

Status: ready to implement. Branch: `RONDB-1056-compiled-interpreter`.
Companion to `plan.md` §8 and `phase_1_microbench.md`.

## 1. Scope

Replace Phase 1's hand-extracted stencil bytes with an automated
pipeline. After this phase, editing `stencils_src.c` and running
`cmake --build . --target regen-stencils` reproduces
`stencils_x86_64.h` and `stencils_arm64.h` from clean.

Three things land:

- A pure-C ELF reader (`extract_stencils.c`) that walks `.text`
  byte ranges + `.rela.text` relocations and emits the per-arch
  stencil header.
- CMake glue: `regen-stencils` custom target with a clang-version
  preflight check.
- aarch64 stencil source rewrite — Phase 1 confirmed that the
  `extern uint64_t HOLE_*` placeholder pattern compiles to
  GOT-indirect addressing on aarch64 (unusable for inline patching).
  Phase 2 introduces a magic-byte sentinel pattern in `movz/movk`
  chains that the extractor recognises.

Exit: `regen-stencils` produces byte-identical `stencils_x86_64.h`
to the Phase 1 hand-authored set; the Phase 1 microbench rebuilt
against the generated header passes (same speedup, same
correctness). aarch64 stencils are generated and a smoke test on
aarch64 hardware (or under qemu-user) executes one successfully.

## 2. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
  ... (existing Phase 0/1 files)
  stencils_src.c            # rewritten — see §5
  stencils_x86_64.h         # GENERATED — must match Phase 1 byte-for-byte
  stencils_arm64.h          # GENERATED — new in this phase
  CMakeLists.txt            # gains regen-stencils target

storage/ndb/src/kernel/blocks/dbtup/jit/extract_stencils/
  extract_stencils.c        # NEW: the extractor, pure C, ~400-500 LOC
  elf64.h                   # NEW: subset of <elf.h> we need (cross-platform
                            # — macOS doesn't ship <elf.h>, this avoids the
                            # platform dependency)
  hole_kinds.h              # NEW: shared HoleKind enum + magic constants;
                            # included by stencils_src.c, the extractor,
                            # and jit1.c
  CMakeLists.txt            # NEW: builds extract_stencils as a host tool
  README.md                 # NEW: extraction recipe + clang-pin guidance

storage/ndb/test/jit_proto/
  ... (existing Phase 0/1 files)
  ... (no new files; the existing microbench and interp test pick up the
       generated headers automatically)
```

## 3. The extractor — `extract_stencils.c`

### 3.1 Inputs

```
$ extract_stencils <input.o> <arch> <output.h>
```

- `<input.o>` — path to the ELF `.o` clang produced from
  `stencils_src.c`.
- `<arch>` — `x86_64` or `arm64`. Determines:
  - which relocation type table to use,
  - which trailing tail-call instruction to strip (5-byte `e9 ...` on
    x86_64, 4-byte `b ...` on aarch64),
  - which magic-byte hole kinds are valid for this arch.
- `<output.h>` — path to write the generated header.

The tool is invoked twice during a `regen-stencils` run, once per
arch.

### 3.2 What it reads from the ELF

1. **Header.** Verifies `EI_CLASS == ELFCLASS64`, `EI_DATA ==
   ELFDATA2LSB`, `e_machine == EM_X86_64` or `EM_AARCH64` matches
   `<arch>`.
2. **Section headers.** Locates `.text`, `.rela.text`, `.symtab`,
   `.strtab`. Errors out with a clear message if any is missing or
   has an unexpected type (no `libelf` dependency means we do this
   lookup explicitly).
3. **Symbol table.** Iterates symbols, picks ones with
   `STT_FUNC`-typed visibility and a name starting with `op_`. These
   are the stencils, in symbol-table order.
4. **For each `op_*` symbol:**
   - Records `(st_value, st_size)` — the byte range in `.text`.
   - Walks `.rela.text` for relocations whose offset falls in
     `[st_value, st_value + st_size)`.
   - Classifies each relocation per §3.3.
   - Identifies the trailing tail-call relocation (the one whose
     offset is at the very end of the symbol's range, against `next`
     or another tail-call target — see §3.4); strips the preceding
     `jmp` instruction's bytes from the captured byte range.
   - Records remaining relocation-driven holes as `(byte_offset
     within stripped range, hole_kind, reloc_type, addend)`.
   - For each hole kind declared as "sub-word immediate" in the arch
     table (aarch64 only), runs `memmem` on the stripped byte range
     against the kind's curated magic constant; records each match
     as `(byte_offset, hole_kind, width)`. See §3.5.
5. **Symbol-name → hole-kind table.** Hard-coded in the extractor
   (compiled in from `hole_kinds.h`). Each `HOLE_*` symbol name maps
   to a `HoleKind` enum value; a relocation against that name
   produces a hole entry of that kind.

### 3.3 Relocation classification (x86_64)

| Reloc type | Target | Hole kind |
|---|---|---|
| `R_X86_64_32` | `HOLE_*` symbol | `HK_OP_*` (per name table) |
| `R_X86_64_32S` | `HOLE_*` symbol | `HK_OP_IMM` |
| `R_X86_64_PLT32` | `next` | trailing tail-call (strip the preceding 5 bytes) |
| `R_X86_64_PLT32` | `HOLE_BLT_TGT` | `HK_BRANCH_TAKE` |
| `R_X86_64_PC32` | (any) | `HK_BRANCH_FALL` (the JGE in branch stencil) |

The `HK_BRANCH_FALL` recognition is by relocation **target name**
(against `next` from a JGE site) AND position within the stencil
(non-trailing). The extractor walks the relocations in offset
order; the *last* reloc against `next` is the tail; all others
against `next` are JGE-style fall-throughs.

### 3.4 Trailing-tail strip

For x86_64: the trailing 5-byte `e9 ?? ?? ?? ??` ends every
"ordinary" stencil. The extractor:

1. Identifies the last relocation in the symbol's range.
2. If it's `R_X86_64_PLT32` against `next`, the relocation lives at
   `byte_offset = symbol_end - 4` (the 4-byte rel32 displacement,
   preceded by the 1-byte `0xe9` opcode).
3. Strips the preceding 5 bytes. Captured byte range becomes
   `[st_value, st_value + st_size - 5)`.
4. Asserts the stripped opcode byte is `0xe9` (sanity check —
   anything else means our model of clang's tail-call lowering is
   wrong; halt with a diagnostic).

For aarch64: the trailing tail is a 4-byte `B` instruction
(`R_AARCH64_CALL26` against `next`). Strip the trailing 4 bytes;
assert the upper 6 bits are `000101` (the `B` opcode prefix).

The branch stencil and the row terminators (`op_skip`, `op_exit`)
are different shapes:

- **Branch stencil** keeps both its `JGE rel32` to `next` and its
  `JMP rel32` to `HOLE_BLT_TGT` — neither is stripped. The
  extractor recognises this stencil by name (`op_branch_*`) and
  skips the strip step.
- **`op_skip` and `op_exit`** are emitted by the *engine*, not the
  extractor, with hand-written bytes (see Phase 1's preamble +
  terminator scheme). The extractor either ignores these symbols
  entirely (preferred) or extracts them as zero-hole stencils that
  the engine ignores in favour of its hand-authored bytes. We'll
  go with "ignore by name match" — see §3.6.

### 3.5 Magic-byte hole detection (aarch64-only in this phase)

The aarch64 stencils use `volatile` sentinel constants in `movz/movk`
chains (per `plan.md` §8 magic-byte fallback). The extractor:

1. Reads the curated magic-constant table from `hole_kinds.h` —
   one 64-bit constant per declared aarch64 hole kind.
2. For each magic constant, scans the stencil's stripped byte range
   for a 4-instruction `movz/movk/movk/movk` sequence that
   materialises the constant.
3. Records each match as a magic-byte hole: `(byte_offset_of_first_instruction,
   kind, width=8)`. The patcher (engine) at runtime replaces all 4
   instructions with new movz/movk chain materialising the actual
   operand value.

The CI collision audit (§7) asserts each magic constant appears the
expected number of times within its declaring stencil and zero
elsewhere in any stencil on either arch. Failure halts CI and
demands a new magic constant.

### 3.6 Output format

The header is emitted in a deterministic order so byte-identical
re-runs are achievable:

```c
/* GENERATED FILE. Do not edit.
 * Source: stencils_src.c
 * Toolchain: clang version 20.1.8
 * Extracted: <absolute UTC timestamp ISO 8601>  -- via env, not host clock
 * Extractor: extract_stencils.c at git-sha <SHA>
 */

#ifndef NDB_JIT_STENCILS_X86_64_H
#define NDB_JIT_STENCILS_X86_64_H
#include "bytecode1.h"
#include "hole_kinds.h"

static const uint8_t bytes_op_load_const_int[] = { 0xb8, ... };
static const Hole    holes_op_load_const_int[] = { ... };

... (one block per stencil, sorted by OpKind enum value) ...

static const Stencil g_stencils[OP_KIND_MAX + 1] = {
  [OP_LOAD_CONST_INT] = STENCIL_(op_load_const_int),
  ...
  [OP_SKIP]           = STENCIL_NOHOLES(op_skip),
  [OP_EXIT]           = STENCIL_NOHOLES(op_exit),
};

#endif
```

The `op_skip` and `op_exit` bytes are written **by the extractor**
to be the engine-required `41 5c c3` (pop r12; ret) on x86_64, not
extracted from the source's bare `c3 ret`. This is one of two
arch-specific quirks the extractor handles by hard-coded knowledge:

1. `op_skip` / `op_exit` byte values per arch (terminator overrides).
2. Trailing-tail strip per arch (see §3.4).

The extractor records these as `static const uint8_t bytes_op_*[]
= { ... };` literals same as any other stencil; `g_stencils[OP_SKIP]
= STENCIL_NOHOLES(op_skip)`. The engine doesn't need special-case
logic — the bytes already encode the preamble-restoration sequence.

### 3.7 Implementation details (LOC budget)

| Component | LOC estimate |
|---|---|
| ELF header / section parsing | ~80 |
| Symbol table iteration | ~60 |
| Per-arch relocation classification | ~80 |
| Trailing-tail detection + strip | ~40 |
| Magic-byte scan (aarch64) | ~60 |
| Hole sort + dedup + canonicalisation | ~40 |
| Header emission (printf-style) | ~80 |
| CLI + error reporting | ~40 |
| `elf64.h` minimal subset | ~50 |
| **Total** | **~530** |

Within the plan §8's 400-500 LOC ballpark. No external deps; only
libc + a 50-LOC `elf64.h` subset (so it builds on macOS hosts that
don't ship `<elf.h>`).

## 4. CMake `regen-stencils` target

### 4.1 Top-level structure

```cmake
# In jit/CMakeLists.txt — additive to what already exists.

# Build the extractor as a host tool. Pure C, no NDB deps.
ADD_EXECUTABLE(ndb_jit_extract_stencils
  extract_stencils/extract_stencils.c
)
SET_TARGET_PROPERTIES(ndb_jit_extract_stencils PROPERTIES
  C_STANDARD 11
  C_STANDARD_REQUIRED YES
)
TARGET_INCLUDE_DIRECTORIES(ndb_jit_extract_stencils PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/extract_stencils
)

# Pinned clang version. Updated as a deliberate three-place change
# (this var, plan.md §2, jit/extract_stencils/README.md).
SET(NDB_JIT_CLANG_VERSION "20.1.8")

OPTION(RONDB_REGEN_STENCILS
       "Regenerate the JIT stencil headers from stencils_src.c"
       OFF)

# The regen target is always defined but only built when the user
# explicitly requests it. The default build consumes the
# checked-in headers without re-running the extractor.

ADD_CUSTOM_TARGET(regen-stencils
  COMMAND ${CMAKE_COMMAND} -E echo
          "[RONDB-1056] regenerating stencil headers with clang ${NDB_JIT_CLANG_VERSION}"
  COMMAND ${CMAKE_COMMAND} -P
          "${CMAKE_CURRENT_SOURCE_DIR}/extract_stencils/regen.cmake"
          --
          "${NDB_JIT_CLANG}"
          "${NDB_JIT_CLANG_VERSION}"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_src.c"
          "${CMAKE_CURRENT_BINARY_DIR}/stencils_x86_64_new.h"
          "${CMAKE_CURRENT_BINARY_DIR}/stencils_arm64_new.h"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_x86_64.h"
          "${CMAKE_CURRENT_SOURCE_DIR}/stencils_arm64.h"
          "$<TARGET_FILE:ndb_jit_extract_stencils>"
  DEPENDS ndb_jit_extract_stencils stencils_src.c
  COMMENT "Regenerate stencil headers with the pinned clang"
  VERBATIM
)
```

### 4.2 `regen.cmake` script

A separate CMake script invoked by `regen-stencils` so the logic
isn't tangled in the parent CMakeLists.txt. It:

1. Asserts `clang --version` first line starts with `clang version`
   (rejects Apple clang).
2. Asserts the version string equals `${NDB_JIT_CLANG_VERSION}`
   (rejects any other patch level).
3. For x86_64:
   - Compiles `stencils_src.c` to `stencils_x86_64.o` with the
     project flags (`-O2 -fno-asynchronous-unwind-tables
     -ffreestanding -fno-stack-protector -fno-pic -std=c11
     --target=x86_64-pc-linux-gnu`).
   - Runs `ndb_jit_extract_stencils` to produce
     `stencils_x86_64_new.h`.
   - Diffs against the committed `stencils_x86_64.h`. If unchanged,
     deletes the `_new.h`. If changed, copies `_new.h` over the
     committed version and prints a "REGENERATED" message; emits a
     reminder to commit.
4. Same for aarch64 with `--target=aarch64-linux-gnu`.

The `_new.h` indirection means a successful rerun on identical
source produces no diff, and a failed rerun (clang version
mismatch, extraction error) leaves the committed headers untouched.

### 4.3 Default build behaviour

Normal `cmake --build` does **not** run the extractor. The
checked-in `stencils_x86_64.h` and `stencils_arm64.h` are consumed
by `ndb_jit1`. This is the same as Phase 1.

The Phase 1 `ndb_jit1_stencils` custom target (which compiles
stencils_src.c to a `.o` artefact for manual objdump) is removed
in this phase — the extractor replaces its purpose.

## 5. aarch64 stencil source rewrite

Phase 1 confirmed `extern uint64_t HOLE_*` produces GOT-indirect
addressing on aarch64. The aarch64 source path needs different
hole materialisation: sentinel 64-bit constants encoded inline as
`movz/movk/movk/movk` chains.

### 5.1 The pattern

```c
#if defined(__aarch64__)
/* Materialise a 64-bit sentinel as four 16-bit immediates baked
 * into a movz+3×movk chain. clang lowers `volatile` literals to
 * exactly this shape. The extractor finds the chain by scanning
 * for the 64-bit constant assembled out of the four 16-bit
 * pieces of the chain's `imm16` fields. */
#define HOLE64(magic_const) ({ \
    volatile uint64_t _h = (uint64_t)(magic_const); \
    _h; })
#endif
```

### 5.2 Per-stencil arch dispatch in `stencils_src.c`

The single source file emits both archs by `#if defined(__x86_64__)
... #else ...` per stencil:

```c
STENCIL op_load_const_int(JitState *s) {
#if defined(__x86_64__)
  s->regs_i64[(uint64_t)&HOLE_LCI_DST] = (int64_t)(uint64_t)&HOLE_LCI_VAL;
#else  /* __aarch64__ */
  s->regs_i64[HOLE64(MAGIC_LCI_DST)] = (int64_t)HOLE64(MAGIC_LCI_VAL);
#endif
  TAIL_NEXT(s);
}
```

The MAGIC_* constants are defined in `hole_kinds.h` and chosen
per-hole-kind from a curated high-entropy table (see plan.md §8).

### 5.3 Curated magic constant table

In `hole_kinds.h`:

```c
/* High-entropy 64-bit sentinels for aarch64 hole materialisation.
 * Each kind gets a unique value that's unlikely to collide with
 * any legitimate immediate in our generated code. The CI
 * collision audit asserts uniqueness across all stencils on all
 * archs.
 *
 * Add new entries at the END (append-only) so the existing
 * stencil bytes don't shift when a new hole kind appears. */
#define MAGIC_LCI_DST   0xC0DEC0DE0001C0DEull
#define MAGIC_LCI_VAL   0xC0DEC0DE0002C0DEull
... (one per declared hole kind) ...
```

The 0xC0DEC0DE prefix is a recognisable banner; the trailing
`0xNNNNC0DE` is the per-kind serial. This pattern is high-entropy
relative to typical x86/aarch64 instruction encodings — a
collision would require a 64-bit literal in a stencil to start
with `0xC0DEC0DE`, which is vanishingly unlikely for our handler
shapes.

### 5.4 What changes for x86_64

Nothing material. The `extern uint64_t HOLE_*` placeholders are
kept on x86_64 because they work there and the resulting bytes
match the Phase 1 hand-extracted set (which is the byte-identical
target).

### 5.5 What the extractor does differently per arch

- **x86_64**: walks `.rela.text`, all holes are relocation-driven.
  No magic-byte search.
- **aarch64**: relocation-driven holes for full-width 64-bit data
  (none in the current stencil set, but reserved). Magic-byte
  search for sub-word holes that came from `volatile uint64_t`
  literals — this is most of the aarch64 holes.

The arch-specific code paths in the extractor sit behind a small
dispatch table; the core ELF parse and per-stencil iteration are
shared.

## 6. README — `jit/extract_stencils/README.md`

Lead paragraph (the one that catches the eye):

> **Pinned compiler: upstream LLVM clang 20.1.8.** Apple clang is
> NOT acceptable. macOS contributors install via
> `brew install llvm@20` and use `/opt/homebrew/opt/llvm@20/bin/clang`
> (or run inside the project's Linux dev container). Linux
> contributors on Rocky 9.5 / RHEL 9 / Debian 13 / Ubuntu 24.10 get
> 20.1.8 from the default repos via `dnf install clang` /
> `apt install clang`.

Plus:
- The extraction recipe (`cmake --build . --target regen-stencils
  -DRONDB_REGEN_STENCILS=ON`).
- Troubleshooting: missing `--target=aarch64-linux-gnu` sysroot
  (most common); `/usr/bin/clang` rejected on macOS; differing
  patch level rejected.
- "If the diff is unexpected": how to read the regenerated header
  vs. the committed one, and which kind of change usually means
  clang did codegen differently (instruction selection, register
  allocation, prologue shape).
- A pointer to plan.md §2 / §16 risk #3 for the rationale.

## 7. CI checks

### 7.1 Drift check

A CI job runs:

```bash
cmake -DWITH_NDB=1 -DWITH_NDB_TEST=1 -DRONDB_REGEN_STENCILS=ON ..
cmake --build . --target regen-stencils
git diff --exit-code -- 'storage/ndb/src/kernel/blocks/dbtup/jit/stencils_*.h'
```

Failure = the checked-in headers have drifted from what the pinned
clang produces. Fix is either (a) commit the regenerated headers,
or (b) figure out why clang's output changed and address that.

### 7.2 Magic-byte collision audit

A CI job (or a build-time check inside `regen.cmake`):

```bash
for stencil_bytes in stencils_x86_64.h stencils_arm64.h ; do
    for magic in MAGIC_LCI_DST MAGIC_LCI_VAL ... ; do
        # Assert: each declared magic-byte hole-kind appears exactly
        # the expected number of times within its declaring
        # stencil's byte range, and zero elsewhere in any stencil
        # on either arch.
        ...
    done
done
```

Implementation: a small Python or C helper that parses the
generated header (which is regular structured C) and walks the
byte arrays.

### 7.3 Phase 1 microbench against generated headers

The Phase 1 microbench is part of CI and built against whatever
`stencils_x86_64.h` is checked in. After `regen-stencils`, the
microbench should still PASS its three thresholds. Falling below
the warm-compile or speedup thresholds after a regen is a regression
worth investigating — the most likely cause is clang inserting
extra instructions in a stencil (e.g., a redundant register move).

### 7.4 Extractor unit tests

Hand-crafted minimal `.o` inputs in `extract_stencils/tests/`,
each exercising a specific code path:

- Single stencil, single hole, x86_64 → expected bytes + holes table.
- Branch stencil with two displacements.
- `op_skip` / `op_exit` recognition.
- aarch64 ADRP/ADD/B sequences (when present).
- aarch64 magic-byte detection on hand-written `movz/movk` chain.
- Misshapen ELF (truncated section, missing `.rela.text`, unknown
  relocation type) → clean error message, non-zero exit.

These run independent of the full pipeline and pin down regression
sources when the extractor changes.

## 8. Step-by-step task breakdown

**Day 1, AM (~3h):**
- Stand up the extractor skeleton: `extract_stencils.c` with ELF
  header parse, section enumeration, symbol-table iteration over
  `op_*` symbols. Print symbol names + sizes from a known `.o`.
- `elf64.h` minimal subset (cross-platform — works on macOS host
  without `<elf.h>`).
- `hole_kinds.h` with the curated magic constants.

**Day 1, PM (~4h):**
- Add `.rela.text` parsing for x86_64.
- Add hole-kind classification by symbol name (lookup table).
- Add trailing-tail strip + sanity check.
- Add header emission (printf-style; deterministic order).
- Run on Phase 1's `stencils_src.c`, diff output against the
  Phase-1 hand-authored `stencils_x86_64.h`. Iterate until
  byte-identical.

**Day 2, AM (~4h):**
- Rewrite `stencils_src.c` for the dual-arch pattern (§5).
- Write `regen.cmake` script with version preflight.
- Add `regen-stencils` CMake target; verify it runs end-to-end
  for x86_64 and produces an unchanged header.

**Day 2, PM (~3h):**
- Add aarch64 to the extractor: ELF header check, page-pair
  relocation handling (none in our current stencils, but the
  parser tolerates them so future stencils don't surprise it),
  magic-byte scan for sub-word holes.
- Cross-compile aarch64 stencils on Linux x86_64 host
  (`--target=aarch64-linux-gnu`), generate `stencils_arm64.h`.
- Smoke test: build the Phase 1 microbench for aarch64 (under
  qemu-aarch64 if no native hardware available) and run a single
  iteration to confirm bytes execute correctly. macOS aarch64
  hardware works too; Phase 1's microbench was already
  arch-agnostic at the C level.

**Day 3, AM (~3h):**
- Write README with pinned-clang lead paragraph + extraction
  recipe + troubleshooting.
- Add the magic-byte collision audit to CI (or to `regen.cmake`).
- Add the drift check job spec.

**Day 3, PM (~3h):**
- Extractor unit tests (§7.4). Each test is a small `.o` blob
  committed in `extract_stencils/tests/`, plus a CMake target
  that runs the extractor on each and diffs against expected
  output.
- Cleanup: remove the Phase 1 `ndb_jit1_stencils` custom target
  (no longer needed).

**Day 4, AM (~3h):**
- Iteration on whatever surprised us. Most likely candidates:
  - aarch64 page-pair relocations the extractor didn't expect.
  - A magic-byte collision in some stencil that requires picking
    a new constant.
  - Generated header byte-mismatch with Phase 1's bytes (an
    instruction-selection difference between Apple clang 17
    cross-compile and the pinned 20.1.8).

**Day 4, PM + Day 5 (~6h, contingency):**
- Tune until the drift check + magic-byte audit + microbench all
  pass on both arches.
- Write `phase_2_extractor.md` results doc.

Total: ~4-5 days as plan.md predicts. Day 4 PM and Day 5 are the
contingency slot for the inevitable byte-mismatch debug.

## 9. `phase_2_extractor.md` template

Written at the very end. Sections:

- Outcome (drift check pass, magic-byte audit pass, microbench
  pass on both archs).
- Generated headers' byte sizes per arch; notable codegen
  differences from Phase 1's hand-authored x86_64 set if any.
- aarch64 stencil bytes — first time we have these; record any
  surprises.
- Toolchain quirks discovered (likely: aarch64 cross-compile
  sysroot setup, magic-constant collisions).
- Removed: Phase 1 `ndb_jit1_stencils` custom target.
- Forward-pointers to Phase 3 (forward-jump fixups generalised
  beyond Phase 1's narrow pattern).

## 10. Verification checklist

Before declaring Phase 2 done:

- [ ] `cmake --build . --target regen-stencils` produces unchanged
      `stencils_x86_64.h` (byte-identical to the Phase 1
      hand-authored set, modulo header timestamp/comment).
- [ ] `cmake --build . --target regen-stencils` produces a fresh
      `stencils_arm64.h` whose first stencil executes correctly
      under a smoke test on aarch64.
- [ ] Phase 1 microbench rebuilt against the generated x86_64
      header passes all three thresholds (same as Phase 1's PASS).
- [ ] CI drift check is in place and fires on any unexpected
      regen output.
- [ ] CI magic-byte collision audit is in place and passes.
- [ ] Extractor unit tests cover at least: single stencil,
      branch stencil, terminator override, aarch64 magic-byte hole,
      misshapen-ELF error path.
- [ ] README's lead paragraph names the pinned clang version
      and mentions the Apple-clang rejection.
- [ ] No external dependencies introduced (only libc + the
      hand-written `elf64.h` subset).
- [ ] `cmake --version` rejects pre-3.18 (script needed for
      `cmake -P`).

## 11. Out of scope for Phase 2 (explicit reminder)

Don't drift into these:

- Stencil set expansion (Phase 5).
- Type prop / picker (Phase 5).
- DBTUP integration (Phase 4).
- Backward-branch admission walk (Phase 3).
- Persistent on-disk cache of compiled programs (post-v1 per
  plan §17).
- aarch64 *perf* measurement (Phase 5; we only need correctness
  on aarch64 in Phase 2).
- Optimisation of jit1.c emit-phase memcpy (deferred per Phase 1
  doc's compile-breakdown analysis).

## 12. Risks / things that may surprise us

1. **Apple clang 17 (Phase 1 baseline) and pinned clang 20.1.8
   produce different x86_64 bytes for `stencils_src.c`.** If so,
   the regen-vs-Phase-1 diff is non-empty and we have to update
   the committed `stencils_x86_64.h`. The Phase 1 microbench
   re-run against the new bytes confirms they're still
   correct. Mitigation: this is expected behaviour, just commit
   and move on; the version pin is the contract.
2. **aarch64 cross-compile sysroot.** On a Linux x86_64 host,
   `clang --target=aarch64-linux-gnu` may need
   `-fuse-ld=lld` or `-isysroot /path/to/aarch64-sysroot` to find
   headers. We don't actually need to *link* — we only emit a
   `.o` — so the sysroot dependency should be minimal. Mitigation:
   document the exact flag set in the README, or vendor a small
   sysroot if it turns out to be brittle.
3. **Magic-byte collision in the curated table.** The CI audit
   catches this loudly. Mitigation: append-only table; on
   collision, add a new entry rather than reusing.
4. **`op_skip` / `op_exit` byte override drift.** If the extractor
   changes the terminator override bytes (e.g., we update the
   preamble scheme), all stencils' relative offsets shift. This
   is a deliberate invariant: the override bytes are hard-coded
   in the extractor and any change is a deliberate code change
   touching the engine, the extractor, and the regenerated
   headers in one commit.
5. **macOS host running the extractor on a `.o` cross-compiled
   to ELF.** macOS doesn't ship `<elf.h>` so we provide the
   minimal subset. The extractor binary itself is built by the
   project's regular C compiler (any host), reads ELF byte-by-
   byte; no platform-specific behaviour. Should just work.
6. **clang 20.1.8 not available on the contributor's distro.**
   Most current distros ship it; older ones (RHEL 8, Debian 12)
   may need a side-by-side install. The README documents this.
   In CI we control the image, so just pin it there.
7. **Existing Phase 1 `stencils_x86_64.h` becomes "generated"
   and its hand-written comment blocks need to be preserved or
   regenerated.** Phase 2's extractor should emit per-stencil
   comments matching what Phase 1 has (instruction disassembly,
   hole table) — they're not part of the byte stream but they
   help future debugging. Mitigation: the extractor invokes
   `objdump` to produce the comment text, OR the comments are
   emitted from a fixed template using only data the extractor
   already has. Latter is preferred — fewer external deps.

## 13. What we learn from Phase 2

If Phase 2 closes cleanly:

- The extraction pipeline is reproducible — any contributor can
  add a new opcode by editing `stencils_src.c` and running
  `regen-stencils`. No more hand-extraction.
- aarch64 stencils exist as a real artefact, with a documented
  hole-encoding strategy that survives clang's GOT-indirect
  default behaviour on the platform.
- The clang version pin is enforced by the build itself, not
  just by docs — drift is impossible on a CI'd build.

If Phase 2 hits a wall — most likely the byte-diff reconciliation
between Apple-clang-17 Phase 1 bytes and pinned-clang-20.1.8 Phase
2 bytes — the engine logic isn't affected, only the byte arrays.
The rough shape of work in Phase 3+ is unchanged.
