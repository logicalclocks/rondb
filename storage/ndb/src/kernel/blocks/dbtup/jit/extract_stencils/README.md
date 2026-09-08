# RONDB-1056 — JIT stencil extractor

> **Pinned compiler: upstream LLVM clang 20.1.8.** Apple clang is **not**
> acceptable. macOS contributors install via `brew install llvm@20` and
> point `NDB_JIT_CLANG` at `/opt/homebrew/opt/llvm@20/bin/clang` (or run
> inside the project's Linux dev container). Linux contributors on Rocky
> 9.5 / RHEL 9 / Debian 13 / Ubuntu 24.10 get 20.1.8 from the default
> repos via `dnf install clang` / `apt install clang`.

## What this is

This directory contains the host tools that turn `../stencils_src.c`
into the per-arch byte + hole tables consumed by `../jit1.c`.

| File | Role |
|------|------|
| `extract_stencils.c` | Reads a clang-compiled `.o`, walks `.rela.text` (x86_64) or scans for movz/movk magic chains (aarch64), emits a generated header. |
| `audit_magics.c`     | Independent verification that every declared `MAGIC_*` appears exactly the expected number of times in a generated header (1× in its declaring stencil on aarch64, 0× elsewhere). |
| `regen.cmake`        | Driver invoked by the `regen-stencils` CMake target. Does clang preflight, compiles `stencils_src.c` for both arches, runs the extractor + audit, copies regenerated headers into the source tree only on actual byte change. |
| `elf64.h`            | Minimal ELF64 header subset (`<elf.h>` doesn't ship on macOS). |

Default `cmake --build` does **not** run the extractor. The committed
`stencils_x86_64.h` and `stencils_arm64.h` (one level up) are linked
directly into `ndb_jit1`. The `regen-stencils` target is a developer
action, run when stencil source or compiler version changes.

## Extraction recipe

From your CMake build directory:

```bash
# Override NDB_JIT_CLANG if your system clang isn't 20.1.8.
cmake -DNDB_JIT_CLANG=/opt/homebrew/opt/llvm@20/bin/clang ..
cmake --build . --target regen-stencils
```

Successful run prints, per arch, either:

```
regen.cmake: [x86_64] up-to-date (no change to stencils_x86_64.h)
```

or:

```
regen.cmake: [x86_64] REGENERATED stencils_x86_64.h — remember to `git add` + commit it.
```

If the latter, `git diff` the regenerated header to see what shifted,
then commit. The drift-check CI job (Phase 2 §7.1) asserts the committed
headers are byte-identical to what the pinned clang produces.

## Troubleshooting

**`Apple clang ... is NOT acceptable`** — the macOS system clang is
Apple's fork; the Apple-version-to-LLVM-version mapping is undocumented
and codegen can differ. Install upstream LLVM and override
`NDB_JIT_CLANG`:

```bash
brew install llvm@20
cmake -DNDB_JIT_CLANG=/opt/homebrew/opt/llvm@20/bin/clang ..
```

**`Required: clang version 20.1.8 ... got 20.1.7`** — patch-level
mismatch. Either upgrade clang, or roll the pin (see §2 of `plan.md`
for the rationale on why we pin). Rolling the pin requires three
synchronised edits:

  - `jit/CMakeLists.txt` — `NDB_JIT_CLANG_VERSION`
  - `compiled_interpreter/plan.md` — §2 clang pin
  - `jit/extract_stencils/README.md` — this file's lead paragraph

After rolling the pin, expect every stencil's bytes to shift; commit
the regenerated headers with the pin bump.

**`unable to find target sysroot ... aarch64-linux-gnu`** — the
aarch64 cross-compile typically Just Works with `--target=aarch64-linux-gnu`
under brew llvm@20, but on minimal Linux installs you may need:

```bash
# Fedora/RHEL/Rocky
sudo dnf install gcc-aarch64-linux-gnu

# Debian/Ubuntu
sudo apt install gcc-aarch64-linux-gnu
```

These ship the headers + crtN that clang's `--target=` integration
expects. Our compile flags (`-ffreestanding`, `-fno-pic`, no libc) mean
we don't actually link against anything, but clang's driver still
checks for sysroot bits.

**`magic-byte audit FAILED`** — clang produced a chain shape the
extractor or audit didn't expect. Most likely cause: a new opcode was
added but its `MAGIC_*` declaration wasn't registered in
`audit_magics.c`'s `kMagicToStencil[]`. Less likely: clang folded two
chains together into one (e.g., neighbouring magics differ by a small
arithmetic constant — fix is to regenerate the affected `MAGIC_*` with
a fresh high-entropy SHA-256-derived value).

## When the diff is unexpected

After `regen-stencils`, if `git diff` on the generated headers shows
changes you didn't ask for:

  - **Only nop padding changed** — irrelevant. The engine reads each
    stencil's bytes individually via `bytes_op_*[]`, not as a
    contiguous blob. Commit and move on.

  - **Hole offsets shifted by 1-2 bytes** — clang chose a different
    instruction-selection path. Verify the engine's patcher still sees
    the right shape at each hole offset (`mov reg32, imm32` on x86_64;
    a complete movz/movk chain on aarch64). The audit catches missing
    chains; an extractor regression would surface as wrong relocation
    types in the header.

  - **A whole stencil grew by 4-16 bytes** — clang inserted something.
    Common causes: extra register move from prologue pressure, branch
    alignment, retpoline. Run the Phase 1 microbench against the
    regenerated header — if speedup falls below 2.5×, investigate
    before merging.

  - **All stencils' bytes shifted by 16+ bytes** — clang's prologue or
    register-allocation changed. Often happens across major clang
    versions. Verify the engine's preamble bytes still bridge
    correctly (see `jit1.c kPreambleArm64[]` once Day 4 lands the
    aarch64 preamble).

## Background

  - `compiled_interpreter/plan.md` §2 — rationale for pinning clang
    (preserve_none + magic-byte chains both depend on stable codegen).
  - `compiled_interpreter/plan.md` §16 risk #3 — what happens when
    the pin drifts and we don't notice.
  - `compiled_interpreter/phase_2_implementation.md` — the Phase 2
    design doc this directory implements.
