# Compiled Interpreter — Phased Implementation Plan (RONDB-1056)

Status: planning. Code: not started. Branch: `RONDB-1056-compiled-interpreter`.

## 1. Goal

JIT-compile NDB aggregation-interpreter programs (and the embedded
normal-interpreter blocks they reference) into native machine code
using **copy-and-patch** compilation. Supported targets are **Linux
x86_64**, **Linux ARM64**, and **macOS ARM64 (Apple Silicon)** — the
last for development only. **macOS x86_64 is explicitly unsupported**:
those builds keep using the existing interpreter unconditionally. No
stencils, no arena, no JIT entry point are compiled into the macOS
x86_64 build. The interpreter remains the source of truth on every
platform, so this is a no-op there.

Primary trigger paths:

- `JOIN_AGG_SETUP_REQ` in `DblqhProxy::execJOIN_AGG_SETUP_REQ`
  (`DblqhProxy.cpp:2336-2435`) — aggregation programs always.
- `SCAN_FRAGREQ` for selected scan-filter cases — later phase.

Out of scope for v1:

- Persistent compiled-program cache (Rondis precompile case).
- Native lowering of variable-length string opcodes (`STRING_SEARCH`,
  `BINARY_SEARCH`, `QSORT`, `COMPRESS_NUM_ARRAY`). These do **not**
  trigger whole-program fallback. Each has a dedicated **cold-call
  stencil** (one per opcode) whose body is a short prologue that
  marshals arguments from the JIT register file and tail-calls the
  existing C++ helper via an `extern "C"` shim. The hot path (single
  argument load + `call`/`bl` + tail to `next`) is small; the cost
  lives in the C++ helper, exactly where it lives today in the
  interpreter. See §10.3 for the explicit shim inventory.
- Backward jumps (programs are forward-only by construction).
- Subroutine `CALL`/`RETURN` opcodes — fall back to interpreter.
- Windows (RonDB does not ship on Windows; WSL2 uses the Linux build).
- macOS x86_64: feature is compiled out entirely; the macOS x86_64
  build path keeps using the interpreter unchanged. Apple no longer
  ships Intel Macs and we do not have hardware to validate stencils on
  that target.

## 2. Constraints (decided)

| Constraint | Decision |
|---|---|
| Languages | Pure **C11** for JIT engine, stencils, extractor tool. C++ interop only at thin shim boundary inside DBTUP. |
| Compile budget | Linux: ~0.5-1 µs per program (warm arena). macOS ARM64: not optimized, correctness only. macOS x86_64: not built — interpreter only. |
| Compile technique | **Copy-and-patch JIT** (Xu & Kjolstad 2021; CPython 3.13 precedent). |
| Stencil compiler | **Upstream LLVM clang 19.1.7** is the **pinned** version. This is the single most important constant in the project: any deviation produces different stencil bytes and `regen-stencils` will diff against the checked-in headers. The pin is recorded in three places that must stay in sync — `jit/CMakeLists.txt` (`set(NDB_JIT_CLANG_VERSION 19.1.7)`), the CI job that runs `regen-stencils`, and `jit/README.md`. **Apple clang (the Xcode-shipped compiler on macOS) is explicitly NOT acceptable for stencil regeneration**, even when its reported version "looks close" — Apple's version numbers don't map to upstream LLVM (Apple clang 15 ≈ upstream 16, Apple clang 16 ≈ upstream 17/18, Apple clang 17 ≈ upstream 19, etc.) and Apple's tree carries Apple-specific patches that drift codegen on the byte level. macOS developers who need to regenerate stencils must install upstream LLVM via Homebrew (`brew install llvm@19` then `/opt/homebrew/opt/llvm@19/bin/clang`) or run `regen-stencils` inside the same Linux container CI uses; system `/usr/bin/clang` is rejected by the version check. Required only for stencil regeneration; normal RonDB builds (including macOS Apple-clang builds) use gcc / Apple clang as today and consume the checked-in headers without regenerating. Minimum-version rationale: 19.1.x for stable `[[clang::preserve_none]]` on **both** x86_64 (since upstream clang 15) and ARM64 (since upstream clang 18, with bug fixes through 19); we pin a specific point release rather than ">= 19" so two contributors on different 19.1.x patch levels do not produce drifting bytes. |
| Stencil source format | Hand-written C functions in `stencils_src.c` with `__attribute__((preserve_none))` + `[[clang::musttail]]` and sentinel-constant placeholders. |
| Generated artefacts | Per-arch C headers (`stencils_x86_64.h`, `stencils_arm64.h`) **checked into git** (CPython model). |
| Extractor tool | Pure C, ~400-500 LOC, hand-rolled minimal ELF parser, no `libelf`/`binutils` dependency. |
| Specializations | Multiple stencils per opcode (e.g., 5-6 for `ADD_REG_REG` covering int/uint/double × NULL-handling). Picker driven by forward dataflow over Phase I.18 typed registers. |
| Branches | Forward-only **by enforcement, not assumption**: every program is walked once at the entry of `ndb_jit_compile` and any branch with `target ≤ source` triggers whole-program fallback to the interpreter before any emission begins. See §9.1 for the pre-pass definition. |
| Cold opcodes | Call out to existing C++ handlers via `extern "C"` shims. |
| W^X handling | Linux: **dual-mapping** via `memfd_create` + `mmap` twice — one `PROT_READ\|PROT_WRITE` mapping for emission, a second `PROT_READ\|PROT_EXEC` mapping for execution, both backed by the same memfd. Avoids RWX pages, which are refused by hardened kernels (SELinux `deny_execmem`, some container runtimes such as gVisor, certain GKE configs). macOS: `MAP_JIT` + per-thread JIT-write toggle, correctness only. |
| Fallback | **Per-program, decided once at SETUP time.** If `ndb_jit_compile` fails (compile error, unsupported opcode mix, OOM in arena), the proxy publishes a `NULL` JIT entry to all LDM workers and every row of *that* program runs through `AggInterpreter::ProcessRec`'s interpreter dispatch loop. There is no per-row fallback: a JIT'd program is always JIT'd, an interpreted program is always interpreted, for the lifetime of that `JOIN_AGG_SETUP_REQ`. Different programs in the same query may independently land on either side. The interpreter remains the source of truth on every platform; failures are silent (logged, rate-limited, but not surfaced to the client). |

## 3. Architecture in one picture

```
                    JOIN_AGG_SETUP_REQ                  per-row
                          │                                │
                          ▼                                ▼
              ┌──────────────────────┐         ┌──────────────────────┐
              │ DblqhProxy::execJOIN │         │ AggInterp::ProcessRec│
              │ _AGG_SETUP_REQ       │         │  (DBTUP worker, C++) │
              │  (compile once       │         │  reads shared        │
              │   per data node)     │         │  NdbJitProg*         │
              └──────────┬───────────┘         └──────────┬───────────┘
                         │ ndb_jit_compile()              │ entry_fn(state, row)
                         │ + fan out NdbJitProg* to       │ (RX-only access,
                         │   all LDM workers              │  no further sync)
                         ▼                                ▼
              ┌──────────────────────────────────────────────────────┐
              │             JIT engine — pure C, jit/                 │
              │   ┌──────────────┐   ┌──────────────┐                 │
              │   │ type-prop    │ → │ stencil      │                 │
              │   │ pass         │   │ picker       │                 │
              │   └──────────────┘   └──────┬───────┘                 │
              │                             │                          │
              │                             ▼                          │
              │   ┌──────────────────────────────────────────────┐   │
              │   │ copy-and-patch: memcpy stencils + patch holes │   │
              │   │ + monotonic forward-branch fixup              │   │
              │   └──────────────────────────────────────────────┘   │
              │                             │                          │
              │                             ▼                          │
              │   ┌──────────────────────────────────────────────┐   │
              │   │ jit arena: mmap'd executable page, icache flush│   │
              │   └──────────────────────────────────────────────┘   │
              └──────────────────────────────────────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────────────┐
              │ stencils_x86_64.h / stencils_arm64.h (generated, in git)│
              │ extracted from stencils_src.c (clang -O2, preserve_none)│
              └──────────────────────────────────────────────────────┘
                                     ▲
                                     │ regenerate via build target
                          ┌──────────┴───────────┐
                          │ extract_stencils.c   │
                          │  (build-time tool, C)│
                          └──────────────────────┘
```

## 4. File / directory layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
  jit.h                       # public extern "C" API, used by C++ AggInterp
  jit.c                       # engine: bytecode → compiled blob
  jit_internal.h              # private types
  jit_arena.c                 # mmap arena, executable-memory helpers
  jit_typeprop.c              # forward dataflow over typed registers
  jit_stencil_picker.c        # opcode + types → stencil id
  jit_emit_x86_64.c           # arch-specific patching for x86_64
  jit_emit_arm64.c            # arch-specific patching for ARM64
  stencils_src.c              # hand-written stencils, compiled with clang
  stencils_x86_64.h           # GENERATED header (checked in)
  stencils_arm64.h            # GENERATED header (checked in)
  helpers.cpp                 # C++ shims for cold-opcode calls
  CMakeLists.txt
  README.md                   # how to regenerate stencils

storage/ndb/src/kernel/blocks/dbtup/jit/extract_stencils/
  extract_stencils.c          # build-time tool, pure C
  elf64.h                     # subset of <elf.h> we use
  CMakeLists.txt
  README.md

storage/ndb/test/jit_proto/   # standalone microbenchmark, removed before merge
  proto.c
  CMakeLists.txt

storage/ndb/claude_files/compiled_interpreter/
  plan.md                     # this document
  phase_0_arena.md            # per-phase docs as they happen
  phase_1_microbench.md
  ...
```

## 5. Phase index

| Phase | Title | Decision gate? | Effort |
|-------|-------|---|---|
| 0 | Foundation: arena + smoke harness | no | 2-3 d |
| 1 | Hand-extracted-stencil microbenchmark | **YES** | 3-5 d |
| 2 | Extractor tool + clang build pipeline | no | 4-5 d |
| 3 | Forward-jump fixups + branch handling | no | 2-3 d |
| 4 | DBTUP thin-slice integration | **YES** | 4-5 d |
| 5 | Hot-opcode lowering, full set + embedded normal-interp branches | no | 6-8 d |
| 6 | Cross-branch always-JIT test integration | no | 2-3 d |
| 7 | `SCAN_FRAGREQ` scan-filter path | no | 4-5 d |
| 8 | Production readiness: perf counters, error paths, defaults | no | 3-4 d |

Total: ~30-40 engineer-days. Decision gates after Phase 1 and Phase 4
let us back out before sinking the bulk of the labour into stencil
specialization.

## 6. Phase 0 — Foundation: arena + smoke harness

**Goal.** Get a writable+executable memory page going on Linux x86_64,
Linux ARM64, and macOS ARM64 (Apple Silicon), with icache flushing and
a smoke test that runs hand-written native code. macOS x86_64 is not a
target — the JIT subsystem is `#ifdef`'d out of that build.

**Deliverables.**

- `jit_arena.c` + `jit_arena.h`:
  - `ndb_jit_arena_create(size_t)` / `ndb_jit_arena_destroy`
  - `ndb_jit_arena_alloc(arena, size, align)` returns writable pointer
    (into the RW mapping; corresponding RX address obtained via
    `ndb_jit_arena_exec_addr(arena, rw_ptr)`)
  - `ndb_jit_arena_seal(arena, rw_ptr, size)` publishes the bytes (msync
    if needed, then flushes icache for the RX mirror) and returns the
    callable RX pointer. Emission writes through the RW mapping; the
    returned function pointer is in the RX mapping.
- Linux: **dual-mapping** strategy. `memfd_create("ndb_jit", MFD_CLOEXEC)`
  to obtain an anonymous fd; `ftruncate` to arena size; `mmap` the fd
  twice — once `PROT_READ|PROT_WRITE` (emit side), once
  `PROT_READ|PROT_EXEC` (exec side). Same physical pages, different
  protections. No RWX page ever exists, so hardened kernels and
  restrictive container sandboxes accept the mappings. Fallback path
  (only if `memfd_create` is unavailable, e.g. very old kernels): a
  W^X two-mapping scheme via a tmpfs file, with a configure-time check
  and a clear error if neither works.
- macOS: `mmap(... MAP_JIT ...)` + `pthread_jit_write_protect_np` toggle.
  Single mapping, page-level write-protect flips per-thread; correctness
  only, perf not optimised on macOS.
- ARM64 cache flush: `__builtin___clear_cache(begin, end)` against the
  **RX mapping range** (cleaning data side from the RW alias is handled
  by the kernel's coherency on the same memfd; flush the I-side at the
  RX address). Phase 0 measures the actual flush cost on real ARM64
  hardware (Graviton/Ampere/Apple) and records it in `phase_0_arena.md`
  so the compile budget in §2 can be revised if needed.
- Standalone smoke test that allocates an arena, copies a hand-coded
  3-instruction return-42 function, jumps to it, asserts result.
- Compiled into `storage/ndb/test/jit_proto/` as the first test target.

**Test approach.** Standalone binary, runs in CI manually or by hand,
no DBTUP dependency. Smoke test verifies: arena create → write through
RW mapping → seal → execute via RX mapping → expected return. Adds to
CTest. Fails clearly on each platform. A second variant of the smoke
test runs under a deliberately RWX-hostile environment — at minimum
SELinux enforcing with `deny_execmem`, and a gVisor-style sandbox if
available — to confirm the dual-mapping path works where the legacy
RWX approach would not. Also asserts that `mprotect(rw_ptr,
PROT_READ|PROT_WRITE|PROT_EXEC)` would fail on the test host (sanity
check that we are *actually* in a hardened environment, not silently
running under a permissive kernel).

**Exit criterion.** Smoke test passes on Linux x86_64, Linux ARM64,
and macOS Apple Silicon, including under the hardened-kernel variant
on Linux. macOS x86_64 is **out of scope**: on that build the JIT
subsystem is compiled out, the interpreter runs as today, and a
build-time check confirms no JIT symbols are linked. ARM64 icache-flush
cost is measured and recorded; if it exceeds the §2 compile budget,
the budget is revised in this phase rather than discovered at Phase 1's
decision gate.

**Test infrastructure note.** Phase 0 is the only one fully testable
without any stencils. Use it to debug platform-specific quirks before
piling on the JIT logic.

## 7. Phase 1 — Hand-extracted-stencil microbenchmark

**Goal.** Validate that copy-and-patch produces a measurable speedup
over the existing interpreter on a representative aggregation program,
*before* committing to the extractor tooling. This is the major
decision gate.

**Deliverables.**

- `stencils_src.c` with 6-8 hand-written stencils:
  - `op_load_const_int`
  - `op_load_col_int`
  - `op_mov_int_int`
  - `op_add_int_int`
  - `op_sum_bigint`
  - `op_branch_lt_int_int` (forward jump only)
  - `op_skip` (forward jump to row-end)
  - `op_exit`
- Compiled with `clang -O2 -fno-asynchronous-unwind-tables
  -ffreestanding`. Stencil functions tagged
  `__attribute__((preserve_none))` and end with
  `[[clang::musttail]] return next(state)`.
- For Phase 1, **manually** run `objdump -d stencils.o`, copy the bytes
  into a hand-authored `stencils_x86_64.h` and `stencils_arm64.h`. No
  extractor yet.
- A self-contained C program in `storage/ndb/test/jit_proto/` that:
  1. Builds an interpreter program shaped like a real aggregation
     (~30 ops: LoadCol → BranchLt skip → Plus → Sum, in a loop body).
  2. Runs it interpreted by linking against a stripped-down copy of
     `AggInterpreter::ProcessRec`'s dispatch loop.
  3. Compiles it via copy-and-patch using the hand-extracted stencils.
  4. Measures wall-clock ns/row for both, plus compile time, over
     100k synthetic rows.
- Output: a table with interpreted ns/row, JIT'd ns/row, compile time,
  and break-even row count.

**Test approach.** The benchmark *is* the test in this phase.
Correctness check: both runs must produce bit-identical aggregation
results. Asserted in the binary.

**Exit criterion (DECISION GATE).** Continue if:
- Compile time on Linux x86_64 is **< 5 µs** for the 30-op program.
- JIT'd code is **at least 2x faster** than interpreted on the
  per-row hot path.
- Break-even row count is **< 5,000**.

If any of these fail, stop and reassess. Likely outcomes if it fails:
- Stencil overhead too high → revisit specialization strategy.
- Compile too slow → drop preserve_none, use `__regparm` style hand-tuned
  ABI instead.
- Speedup too small → the dispatch overhead wasn't where the time was;
  pivot to optimizing handlers themselves.

**Effort.** 3-5 days. Most of the time goes into debugging the
hand-extracted stencils until they run correctly.

**Phase 1 doc.** `phase_1_microbench.md` records the exact stencils,
the bytes, the benchmark numbers, and the go/no-go verdict.

## 8. Phase 2 — Extractor tool + clang build pipeline

**Goal.** Replace Phase 1's hand-extracted stencil bytes with an
automated pipeline. After this phase, editing `stencils_src.c` and
running `make regen-stencils` reproduces the headers from clean.

**Deliverables.**

- **Hole-discovery policy (decided).** Two distinct mechanisms, used
  for two disjoint classes of holes — never both for the same hole:
  - **Relocation-driven (preferred, default).** Any hole that the
    compiler can express as a relocatable reference uses an `extern`
    placeholder symbol (`extern uint64_t HOLE_REG_DST;` etc.) so that
    clang emits a real entry in `.rela.text`. The extractor reads the
    relocation type, byte offset, and addend directly from the section.
    This covers every full-width hole — pointer-sized data references,
    32-bit `R_X86_64_PC32` / `R_X86_64_PLT32` immediates, ARM64 page-
    pair relocations (`R_AARCH64_ADR_PREL_PG_HI21` +
    `_LO12_NC`), and tail-call targets (`R_X86_64_PLT32` to `next`
    /`R_AARCH64_CALL26`). The relocation table is authoritative.
    No byte-pattern search is involved.
  - **Magic-byte-driven (narrow fallback).** Only used for sub-word
    immediates that the compiler cannot express as a relocation —
    8-bit displacements on x86_64, ARM64 12-bit `add/sub` immediates,
    and the like. For these, the stencil source uses a `volatile`
    sentinel literal of the appropriate width drawn from a curated
    table of magic constants (one per hole-kind, all chosen to be
    high-entropy bit patterns unlikely to collide with legitimate
    instruction encodings — e.g., `0xA7` for 8-bit, `0xCAF` for
    12-bit). The extractor locates these by linear scan within the
    stencil byte range. The set of magic constants is defined once in
    `jit_internal.h` and shared between `stencils_src.c` and the
    extractor.
  - **CI collision check.** A CI step disassembles every generated
    stencil and asserts that:
    1. Each declared magic-byte hole appears exactly the expected
       number of times within its stencil's byte range.
    2. No magic constant appears anywhere in any stencil *outside* its
       declared hole sites — i.e., no false positives anywhere in the
       generated header, across all stencils on all archs.
    3. No two hole-kinds share the same magic constant.
    A failure here halts the build and demands a new magic constant.
- `extract_stencils.c` — pure C, hand-rolled minimal ELF reader:
  - Parses the ELF64 header and section headers from `<elf.h>`.
  - Walks the symbol table to enumerate stencils (recognised by name
    prefix `op_`) and to resolve `HOLE_*` placeholder symbols to
    hole-kind IDs via a name-to-ID table compiled in.
  - For each stencil:
    - Captures the byte range from `.text`.
    - Walks `.rela.text` relocations within the range. **All
      relocations whose target is a `HOLE_*` symbol become
      relocation-driven hole entries** — the extractor never treats
      them as tail-calls or external calls.
    - Identifies the trailing tail-call relocation (R_X86_64_PLT32 to
      `next` on x86_64; R_AARCH64_CALL26 on ARM64) and **strips the
      preceding `jmp`** (5 bytes on x86_64, 4 bytes on ARM64).
    - For the narrow set of hole-kinds declared as sub-word immediate
      (looked up from a static table indexed by hole-kind), runs a
      linear `memmem` of the stencil's byte range against that
      hole-kind's magic constant; records each hit. For all other
      hole-kinds the magic-byte search is **not** run, eliminating
      collision risk for full-width holes by construction.
    - Records each relocation-driven hole as
      `(byte_offset, kind, reloc_type, addend)`.
    - Records each magic-byte hole as `(byte_offset, kind, width)`.
    - Records each non-hole relocation (i.e., calls to extern C
      helpers) as `(byte_offset, target_kind, target_index)`.
  - Emits a C header containing:
    - One `static const uint8_t bytes_<stencil>[]` per stencil.
    - One `static const NdbJitHole holes_<stencil>[]` — a tagged union
      of `RELOC_HOLE { reloc_type, addend, byte_offset, kind }` and
      `MAGIC_HOLE { width, byte_offset, kind }` so the emitter can
      patch each correctly without re-deriving the mechanism.
    - One `static const NdbJitReloc relocs_<stencil>[]` for non-hole
      external calls.
    - A top-level `static const NdbJitStencil g_stencils[]` table
      indexing all of the above by stencil ID.

- CMake glue:
  - `add_custom_command` runs the extractor at configure time only when
    `stencils_src.c` changes — but the developer must opt in via a
    `-DRONDB_REGEN_STENCILS=ON` flag, otherwise the checked-in headers
    are used as-is.
  - A `regen-stencils` target invokes clang explicitly (independent of
    the project's CC), runs the extractor, writes the headers.
  - Cross-arch generation: when `regen-stencils` runs on x86_64,
    `clang --target=aarch64-linux-gnu` is used to also produce the
    ARM64 stencils.

- README.md in `jit/extract_stencils/`:
  - **Pinned compiler: upstream LLVM clang 19.1.7** (NOT Apple
    clang). The README leads with this in its first paragraph — same
    constant as §2 and as the `NDB_JIT_CLANG_VERSION` variable in
    `jit/CMakeLists.txt`. If a different version is used, the diff
    against the checked-in headers will fail in CI.
  - macOS-specific guidance: install via `brew install llvm@19`,
    invoke as `/opt/homebrew/opt/llvm@19/bin/clang` (Apple Silicon)
    or `/usr/local/opt/llvm@19/bin/clang` (Intel). Do **not** use
    `/usr/bin/clang` (Apple clang). Alternatively, regenerate inside
    the project's Linux dev container, which already ships the pinned
    upstream clang.
  - The `regen-stencils` target runs a two-line preflight check:
    1. The `clang --version` output starts with `clang version` (rules
       out Apple clang, whose first line is `Apple clang version
       N.N.N`).
    2. The trailing version string equals `19.1.7`.
    Failing either prints an error pointing back to this README.
  - Exact clang flags, troubleshooting (most common: missing
    `--target=aarch64-linux-gnu` sysroot for cross-compile).
  - "Run `cmake --build . --target regen-stencils` after editing
    `stencils_src.c`."

**Test approach.**

- Diff-based regression: extractor output compared byte-for-byte
  against Phase 1's hand-authored headers for the same `stencils_src.c`.
- Integration test: rebuilds the Phase 1 microbenchmark using
  generated headers and asserts identical results and similar
  performance.
- CI lint: a CI job regenerates stencils using the pinned upstream
  LLVM clang version (**19.1.7**, per §2) and fails if checked-in
  headers drift. The CI job's first step asserts both:
  - The first line of `clang --version` begins with `clang version`
    (rejects Apple clang, whose first line begins with
    `Apple clang version` — Apple's numbering is not interchangeable
    with upstream's).
  - The version string on that line equals `19.1.7`.
  Failing either exits non-zero before any stencils are produced —
  preventing a CI infra upgrade from silently bumping the pin or from
  swapping in Apple clang on a macOS-hosted runner.
- **Magic-byte collision audit (CI).** After regen, iterate every
  generated `bytes_<stencil>[]` and assert: (a) each declared
  magic-byte hole-kind appears exactly N times within its declaring
  stencil's byte range — where N is recorded in `stencils_src.c` by
  hole-site count; (b) the magic constant for hole-kind K never
  appears outside its declared hole sites in *any* stencil on
  *either* arch; (c) the curated magic-constant table has no two
  hole-kinds sharing a value. Any failure halts CI with the offending
  stencil + offset, and the fix is to choose a new magic constant
  (entries in the table are append-only so prior stencils are
  unaffected).
- Hand-crafted minimal-`.o` unit tests for the extractor, exercising
  each ARM64 relocation type and each hole-kind in isolation.

**Exit criterion.**

- Extractor handles the Phase 1 stencil set on both arches with no
  hand-editing.
- Generated headers produce a microbenchmark identical to Phase 1's.
- `regen-stencils` works on a Linux x86_64 dev box with cross-compilation
  to ARM64 stencils.

**Effort.** 4-5 days. ELF parsing is mechanical but the relocation
type interpretation needs care, especially on ARM64
(`R_AARCH64_ADR_PREL_PG_HI21` + `R_AARCH64_ADD_ABS_LO12_NC` for
absolute addressing of helper-function holes).

## 9. Phase 3 — Forward-jump fixups + branch handling

**Goal.** Add proper forward-branch handling to the JIT engine, both
for branches between stencils (e.g., `BRANCH_LT_REG_REG` skipping over
`Sum`) and for the `Skip` opcode (skip-to-row-end), and **explicitly
enforce the forward-only invariant** with a single bytecode pre-pass at
the top of `ndb_jit_compile`.

### 9.1 Forward-only pre-pass (program admission check)

Before any stencil emission, `ndb_jit_compile` performs one linear walk
of the program with the sole job of admitting or rejecting the program
as JIT-compilable. This pass produces no output beyond a yes/no verdict
and the reason; if rejected, the function returns `NULL`,
`ndb_jit_last_error` is set, and the caller falls back to the
interpreter.

```
admission_walk(bytecode, n_words):
    pc = 0
    while pc < n_words:
        op = decode_op(bytecode[pc])
        len = op_word_length(op, bytecode[pc])

        if op is BRANCH-class (agg or embedded normal-interp):
            target = decode_branch_target(bytecode, pc, op)
            if target <= pc:                            # backward or self
                reject("backward branch at pc=%u → %u", pc, target)
            if target > n_words:                        # out of range
                reject("branch target out of range at pc=%u → %u", pc, target)

        if op is CALL / RETURN / has-no-stencil-at-all:
            reject("unsupported opcode 0x%x at pc=%u", op, pc)
            # NOTE: opcodes with a *cold-call* stencil (see §10.3 —
            # STRING_SEARCH, BINARY_SEARCH, QSORT, COMPRESS_NUM_ARRAY,
            # div-null-fixup, etc.) are *not* rejected here; they have
            # a stencil that calls out to the existing C++ helper.

        if op opens an embedded normal-interpreter block:
            walk_embedded_block(bytecode + offset, length)
                # same rules apply recursively: every BRANCH_* must be
                # forward; CALL/RETURN inside the embedded block also
                # rejects the *whole* program.

        pc += len
    accept
```

The walk is intentionally cheap (one pass, no allocation, no state
beyond a 64-bit error code) so that even rejected programs cost only a
few hundred nanoseconds before the interpreter takes over. It runs
**unconditionally**, not as a Phase 3 add-on — the same source file
holds the function from Phase 1 onwards, but the embedded-block clause
becomes live in Phase 5 when those opcodes start appearing.

**Why an explicit pre-pass instead of "fail during emission":**

- Single point of truth: §2's "forward-only by enforcement" is grounded
  in one function in `jit.c`, citable and testable.
- Cheap rejection: a malformed program never touches the arena, so
  there is nothing to roll back if we discover the problem mid-emit.
- Clear diagnostics: the error string identifies the offending PC and
  target, which makes the fallback log message useful.
- Embedded normal-interp blocks are screened in the same place as
  aggregation branches; the encoder of those blocks (`NdbInterpreter`
  / scan-filter builders in MySQL) cannot be relied on to produce
  forward-only programs — historically they emit backward jumps for
  any loop construct, and we cannot retrofit a forward-only invariant
  into the on-the-wire program format. Instead we *detect and reject*
  here.

**Where the "forward-only by guarantee" claim used to come from.**
Aggregation programs as produced by RonSQL's planner today happen to
be forward-only because their grammar does not generate loops, but
this is an emergent property of the planner, not a contract enforced
at the encoder. Embedded normal-interpreter blocks have no such
property. The pre-pass is therefore **the** enforcement point; the
plan does not depend on any upstream invariant.

### 9.2 Fixup table and emission

**Deliverables.**

- In `jit.c`, a small fixup data structure:
  ```c
  typedef struct {
      uint32_t source_label;   // bytecode word offset of branch target
      uint32_t emit_offset;    // byte offset in compiled blob to patch
      uint8_t  width;          // 1, 4, or arch-specific (e.g., ARM64 19-bit)
      uint8_t  arch_kind;      // x86_64 rel8 / rel32, ARM64 B.cond / B
  } NdbJitFixup;
  ```
- A monotonic resolver: maintain a flat array of `NdbJitFixup`,
  appended to as branches are emitted. When the emitter advances past
  bytecode word offset `L`, drain all fixups with `source_label == L`
  by computing `target - source - width` and writing it to the patch
  site.
- Type-state at branch joins: when a fixup is queued, snapshot the
  per-register typed-state vector. When the target label is reached,
  intersect with the fall-through state; demote disagreeing registers
  to `REG_TYPE_UNKNOWN` for code emitted after the join. **Ordering
  is fixed:** at every bytecode word offset `L` the emitter first
  drains all fixups with `source_label == L` (computing displacements
  *and* meeting their snapshotted type-states into the current
  type-state vector), then queries the stencil picker for `L`'s
  opcode against the post-meet type-state, then emits the opcode's
  bytes — never the other order. This is what keeps emission single-
  pass: the type-state visible to the picker at any opcode site is
  already the join of every reaching predecessor (fall-through +
  every queued forward branch targeting `L`), so no second pass to
  fix up choices made under stale assumptions is needed.
- Encoding helpers in `jit_emit_x86_64.c` and `jit_emit_arm64.c`:
  - x86_64: prefer `Jcc rel8` (2 bytes) when displacement < 128 known,
    else `Jcc rel32` (6 bytes). Heuristic chosen at fixup time using
    the maximum stencil-output stride.
  - ARM64: `B.cond` always fits (±1 MB).
- Microbenchmark extended with a forked program containing two
  forward branches.

**Test approach.**

- Microbenchmark adds an aggregation program with conditional skip
  and verifies that:
  - The compiled program reaches the same final aggregate as
    interpreted.
- **Pre-pass unit tests** (table-driven, in C). One row per case,
  asserting verdict + error string substring:
  - Pure forward branches: accept.
  - Self-branch (`target == pc`): reject ("backward branch").
  - Backward branch from anywhere in the program: reject.
  - Branch target past `n_words`: reject ("out of range").
  - Hand-built embedded-normal-interp block with a backward
    `BRANCH_*`: reject (whole program).
  - `CALL` / `RETURN` opcode (agg or embedded): reject
    ("unsupported opcode").
  - **Cold-call opcodes accepted, not rejected.** A program containing
    `STRING_SEARCH`, `BINARY_SEARCH`, `QSORT`, or `COMPRESS_NUM_ARRAY`
    must be admitted. The compiled blob lowers each as a cold-call
    stencil per §10.3, not as a fallback trigger. A negative test
    asserts these do not appear in the rejected-opcode list at any
    point in the project's lifetime.
  - Same program twice — first JIT'd, then with one branch flipped to
    backward — asserts the second admission rejects without touching
    the arena (arena byte-count delta == 0).
- Unit tests on the fixup table itself (table-driven, in C).
- Differential test: every program in the Phase 3 microbenchmark is
  also fed to the interpreter; results must match. Programs that the
  pre-pass rejects must still reach the same answer via interpreter.

**Exit criterion.** Forked-control-flow program runs correctly through
the JIT and matches interpreted output bit-for-bit.

**Effort.** 2-3 days.

## 10. Phase 4 — DBTUP thin-slice integration

**Goal.** Wire the JIT into the proxy-level `JOIN_AGG_SETUP_REQ`
handler so that each aggregation program is compiled **once per data
node** at setup time, then shared read-only with every LDM worker
(and any other thread that runs LDM code) on that node. This is the
second major decision gate.

### 10.1 Compile scope: per-node, not per-LDM

**Decision.** A single `NdbJit` engine instance lives in `DblqhProxy`,
not in each per-LDM `Dbtup` block. `JOIN_AGG_SETUP_REQ` arrives at the
proxy, the proxy runs `ndb_jit_compile` once, and the resulting
`NdbJitProg*` is fanned out to every LDM worker as part of the same
setup signal flow that already distributes the rest of the SETUP
state. Workers store the pointer in their `AggInterpreter` and call
it as `entry_fn(state, row)` per row, with no further coordination.

**Why per-node is the correct scope.**

- **Different data nodes may run on different hardware.** A cluster
  can mix x86_64 and ARM64 data nodes, so cross-node sharing of
  compiled blobs is impossible — each data node compiles its own copy
  for its own arch. This is automatic because data nodes are
  separate processes; nothing extra is needed.
- **Within a node, all LDM threads run identical code on identical
  hardware.** Once compiled, the blob is byte-for-byte usable by every
  worker. The cost ratio between proxy-level and per-LDM compilation
  on a 24-LDM node is exactly 24× compile work and ~24× arena bytes
  per program — non-trivial when a complex query produces many
  programs.
- **Compiled blobs are read-only at execution time.** After
  `ndb_jit_arena_seal` has flipped the bytes from RW to RX (Linux
  dual-mapping) or toggled `pthread_jit_write_protect_np` (macOS),
  the code pages are not written again for the lifetime of the
  program. Concurrent reads from N LDM threads need no mutex, no
  refcount on the hot path, and no per-call memory barrier.

**Cross-thread visibility.** The compile thread (proxy) writes
through the RW mapping and flushes the icache for the RX mapping
range (`__builtin___clear_cache` on ARM64, no-op on x86_64). Before
publishing the `NdbJitProg*` to LDM workers, the proxy issues a
release-store; workers acquire-load when consuming the SETUP signal.
On ARM64 specifically, the proxy issues
`membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE)` after the
icache flush so each LDM CPU executes the necessary `ISB` before
running the new code (per the Linux JIT-coherency contract); on
x86_64 the existing `IRET`/`SYSCALL` on signal entry to the worker
is sufficient. Phase 4 measures this barrier cost — it is amortised
over all rows of all LDMs, so it does not threaten the per-program
compile budget, but it is real and is recorded in `phase_4_*.md`.

**Lifetime.** The `NdbJitProg*` is owned by the proxy's `NdbJit`
arena. Workers hold a borrowed reference, never `release`. The proxy
calls `ndb_jit_release` exactly once when `JOIN_AGG_RELEASE_REQ`
completes across all workers (the same fan-in path that already
tears down per-program state). If a worker were ever to be torn down
ahead of the proxy's release — currently impossible by NDB block
lifecycle, but worth asserting — the proxy detects it via the
existing release-confirm protocol and refuses to free.

**Tradeoffs the proxy-level scheme accepts.** The proxy must serialise
compiles for the same program (it already serialises SETUP work, so
this is free), and the arena's allocator becomes a single-writer
many-reader structure rather than thread-private. The arena writer is
the proxy thread only, so no lock is needed there either; readers
never touch the allocator's bookkeeping, only the published RX bytes.
The remaining cost is the cross-thread publication barrier described
above, which is paid once per program, not once per row.

### 10.2 Deliverables

- Public C API in `jit.h`:
  ```c
  typedef struct NdbJit       NdbJit;
  typedef struct NdbJitProg   NdbJitProg;
  typedef int (*NdbJitEntry)(void* interp_state, void* row);

  NdbJit*     ndb_jit_create(size_t arena_bytes);
  void        ndb_jit_destroy(NdbJit*);
  NdbJitProg* ndb_jit_compile(NdbJit*, const uint32_t* bytecode, size_t n_words,
                              const NdbJitColumnDescr* descr, size_t n_descr);
  void        ndb_jit_publish(NdbJit*, NdbJitProg*);  /* release-store + sync_core */
  void        ndb_jit_release(NdbJit*, NdbJitProg*);
  NdbJitEntry ndb_jit_entry  (NdbJitProg*);
  const char* ndb_jit_last_error(NdbJit*);
  ```
  Contract for `ndb_jit_compile`: the function first runs the §9.1
  forward-only admission walk over `bytecode[0..n_words)`, including
  every embedded normal-interpreter block reachable from it. If the
  walk rejects (backward branch, out-of-range target, `CALL`/`RETURN`,
  any opcode without a stencil), the function returns `NULL` with
  `ndb_jit_last_error` set to a one-line diagnostic, **before any
  arena allocation**. Only an accepted program proceeds to type-prop
  and emission. This is the single enforcement point referenced by §2.
  Contract for `ndb_jit_publish`: must be called by the producer
  (proxy) thread before the `NdbJitProg*` is observable by any
  consumer thread; performs the release-store and arch-appropriate
  cross-core code-coherency barrier (`membarrier ... SYNC_CORE` on
  Linux ARM64, no-op on x86_64). After publish, any thread holding the
  pointer may call `ndb_jit_entry` and invoke the returned function.
- C++ shim layer (`helpers.cpp`):
  - **One** `NdbJit*` per `DblqhProxy` instance, lifetime tied to
    proxy block construction. No `NdbJit*` lives in worker `Dbtup`
    instances.
  - `extern "C"` wrappers for the full cold-opcode helper inventory
    listed in §10.3. Each wrapper is a thin shim around an existing
    DBTUP helper — no new logic, just a stable C ABI for the cold-call
    stencil to target.
- `DblqhProxy::execJOIN_AGG_SETUP_REQ`:
  - After existing validation, attempt JIT compile via the proxy's
    `NdbJit*`. On success, call `ndb_jit_publish` and stash the
    `NdbJitProg*` in the per-program SETUP record that fans out to
    workers. On failure, record the diagnostic and fan out a SETUP
    record with a `NULL` JIT entry; **this single decision binds the
    program to the interpreter for its entire lifetime — there is no
    per-row re-evaluation downstream.** The fan-out is identical in
    both cases except for the `NdbJitEntry` field; downstream signal
    flow does not branch on JIT/interp status.
- `AggInterpreter::Init()` (per-worker):
  - Reads the `NdbJitProg*` and `NdbJitEntry` from its SETUP record
    once; this is an acquire-load (the matching release-store happened
    in the proxy's `ndb_jit_publish`). The result is cached in the
    per-program `AggInterpreter` state for the rest of the program's
    life, so `ProcessRec` does not re-check on every row.
- `AggInterpreter::ProcessRec` (per-worker):
  - **Per-program dispatch, not per-row.** A single branch on the
    cached `entry_fn` chooses JIT vs. interpreter the first time
    `ProcessRec` runs for the program; from there each row takes
    exactly one path with no further check. Equivalent shape:
    ```c
    /* once per program, not once per row */
    if (state->entry_fn != NULL) {
        for (row : rows) state->entry_fn(state, row);   /* JIT path  */
    } else {
        for (row : rows) interpret(state, row);         /* interp path */
    }
    ```
    Implementation may keep the per-row `if` if the branch predictor
    handles it perfectly (it will — `entry_fn` is constant for the
    program), but the *semantic* commitment is per-program and is
    asserted by a unit test that flips the entry pointer mid-program
    and confirms the change is **not** picked up.
- A new debug macro `DEBUG_JIT` (alongside `DEBUG_AGG`).
- `DUMP` state command **2370** to log JIT cache stats and force
  fallback (mirrors the existing 2359/2360 pattern in DBLQH). Stats
  are reported at proxy granularity (one node, one number per
  counter), reflecting the per-node compile scope.

### 10.3 Cold-call stencil and shim inventory

**Distinction.** Three categories of opcode behaviour at compile time:

| Category | Behaviour | Example |
|---|---|---|
| Hot-lowered | Full native body emitted in stencil bytes | `Plus`, `LoadCol`, `Sum`, `BRANCH_LT_REG_REG` |
| Cold-call | Stencil emits a small prologue that calls an `extern "C"` helper, then tails to `next` | `STRING_SEARCH`, `BINARY_SEARCH`, `QSORT`, `COMPRESS_NUM_ARRAY`, `Div`-by-zero NULL fixup, accumulator-slot resolution if not inlined |
| Whole-program fallback | Admission walk in §9.1 rejects the program; interpreter runs everything | `CALL` / `RETURN`, backward branches, opcodes with no stencil at all |

Cold-call opcodes are **not** fallback triggers. The program still
goes through the JIT; only the body of that one opcode runs as a C++
helper call. This preserves the surrounding hot-path inlining and
keeps the proxy-level compile-once-share-many model intact.

**Shim inventory (initial set, §10 deliverable).** Each entry is one
`extern "C"` function in `helpers.cpp` plus one stencil entry in
`stencils_src.c`. The C++ helpers wrapped here already exist in DBTUP;
the shim does no new work, only stabilises the C ABI:

| Stencil id | Shim symbol | Wrapped helper | Notes |
|---|---|---|---|
| `op_string_search` | `ndb_jit_h_string_search` | existing `STRING_SEARCH` handler in `AggInterpreter.cpp` | char-set + locale handling stays in C++ |
| `op_binary_search` | `ndb_jit_h_binary_search` | existing `BINARY_SEARCH` handler | per-row, takes JIT register pointers |
| `op_qsort` | `ndb_jit_h_qsort` | existing `QSORT` handler | works on aggregation register file in place |
| `op_compress_num_array` | `ndb_jit_h_compress_num_array` | existing `COMPRESS_NUM_ARRAY` handler | bigint/double array compaction |
| `op_div_null_fixup` | `ndb_jit_h_div_null_fixup` | existing div-by-zero NULL helper | only invoked from the divide-family stencils when the zero-check trips |
| `op_accumulator_lookup` | `ndb_jit_h_accumulator_slot` | existing accumulator slot resolver | omitted if Phase 5 inlines slot resolution; left in the inventory as a fallback |

**Cold-stencil shape.** Each cold-call stencil is mechanically:

```c
/* pseudo-C — actual stencils in stencils_src.c */
__attribute__((preserve_none))
static void op_string_search(NdbJitState* state) {
    extern void ndb_jit_h_string_search(NdbJitState*);
    ndb_jit_h_string_search(state);                  /* call, not tail-call */
    [[clang::musttail]] return next(state);          /* tail to dispatch */
}
```

The non-tail `call` is deliberate: the C++ helper has its own stack
frame and may invoke ndbrequire / signal-block primitives that expect
a normal frame above them. Returning from the helper to a register-
ABI tail-call to `next` is correct and matches what the interpreter
loop already does for these opcodes.

**Coverage assertion.** The CI canary in §11 (Phase 5) treats the
cold-call stencils as covered: a program containing `STRING_SEARCH`
must compile cleanly. Only the three whole-program-fallback triggers
in the table above cause `ndb_jit_compile` to return `NULL`.

**Why this is in §10, not §11.** The shim mechanism has to exist
before Phase 5 can use it for the wider opcode set, and Phase 4 needs
at least one cold-call helper (the `Div`-by-zero NULL fixup) to land
real aggregation programs. Phase 5 then *populates* the inventory; it
does not *introduce* the mechanism.

**Test approach.**

- This branch only: a focused MTR test or `block_unit_test` target
  that builds a small aggregation query with the existing
  `testJoinAgg` framework, runs it twice — once with JIT forced on
  (`DUMP 2370 mode=force-jit`) and once with JIT forced off
  (`DUMP 2370 mode=force-interp`) — and asserts result equality.
- Reuse `bench_q12_dbtc` for a real query measurement.
- Coverage matrix in this branch is intentionally narrow because
  Phase 6 will integrate the wider always-JIT test program from the
  other branch.

**Exit criterion (DECISION GATE).**

- All `testJoinAgg`, `testJoinAggSpj`, and `testJoinAggNdbApi` tests
  pass with JIT on for programs in coverage; passthrough to
  interpreter for programs out of coverage.
- `bench_q12_dbtc` shows speedup over interpreted baseline (target:
  ≥ 1.3x end-to-end query, given query time has many other components).
- No new MTR failures.

If failing here: most likely cause is the proxy-to-worker boundary —
either lifetime of `NdbJit` / `NdbJitProg*` (proxy releasing while a
worker still holds it), shim-call conventions across the C/C++ edge,
or missing the `SYNC_CORE` barrier on ARM64 (workers running stale
icache contents). Fix in this phase before continuing to Phase 5;
the issues compound as more stencils land.

**Effort.** 4-5 days.

## 11. Phase 5 — Hot-opcode lowering, full set + embedded normal-interp branches

**Goal.** Bring stencil coverage up to the full hot subset of the
aggregation interpreter and the embedded normal-interpreter branches.

**Deliverables.**

- Full stencil set in `stencils_src.c`, with specializations driven
  by Phase I.18 typed registers. Estimated ~75 stencils, broken down:

  | Family | Specializations | Count |
  |---|---|---|
  | `LoadConst*` (int/uint/double) | typed by destination | 4 |
  | `LoadCol*` (int/uint/double) | typed by source column | 6 |
  | `Mov` | typed src→dst (4 type pairs) | 4 |
  | `Plus`, `Minus`, `Mul` | int-int, uint-uint, mixed-promoted, double-double, plus nullable variants | 5 × 6 = 30 |
  | `Div`, `DivInt`, `Mod` | as above + zero-check trap | 6 × 4 = 24 |
  | `Sum`, `Max`, `Min`, `Count` | bigint, double, int-promoted | 3 × 4 = 12 |
  | `Skip`, `SetRegNull`, `Exit` | unspecialized | 3 |
  | `BRANCH_*_REG_REG` (embedded normal-interp) | per-comparator × 3 types | ~18 |
  | `BRANCH_REG_EQ_NULL`, `BRANCH_REG_NE_NULL` | unspecialized | 2 |

- **Authoring strategy: hand-written first, codegen if it slows down.**
  Phase 5 starts by hand-writing the stencils — most are small (a few
  C statements each), the bytes need scrutiny one at a time anyway,
  and with ~75 entries the volume is borderline manageable. *But*
  the structure is regular: most rows in the table above are
  `{op, lhs_type, rhs_type, dst_type, nullable_mode}` mechanical
  transforms over a small fixed body. If the hand-writing approach
  starts to slow Phase 5 down — measured by either calendar time
  blowing past the 6–8 day estimate, or a single bug-class
  (e.g., NULL handling, integer-promotion, write-back encoding)
  recurring across multiple stencils — switch to a small generator
  that emits `stencils_src.c` from a table. Concrete switch-over
  signal: ≥ 2 days into Phase 5 and < 30% of the stencil set passes
  its unit tests, **or** the same fix has been applied to ≥ 4
  stencils. The generator is the better long-term shape regardless,
  because it lets a single-line policy change (calling convention,
  NULL-handling, register pressure tweak) rewrite every affected
  stencil at once, but its setup cost (~½–1 day) only pays back if
  hand-writing is in fact getting bogged down.
  - Generator form: a small Python or m4 driver (~150 LOC) reading
    a table of `{family, type-tuple, op}` rows and templating the C
    body. Tracked as `stencils_gen.py` + a `stencils_table.txt` next
    to `stencils_src.c`; the generated `stencils_src.c` is a build
    output (not checked in), but the *generated headers*
    (`stencils_x86_64.h` / `stencils_arm64.h`) remain checked in
    as before — the existing CI drift check covers the result of
    the codegen → clang → extractor pipeline as a single artefact.
  - Backout: if the generator is started and turns out to be the
    wrong fit (e.g., one stencil family resists templating), keep
    the generator for the regular families and write the holdouts
    by hand. Both forms produce the same `stencils_src.c`-shaped
    input to the extractor, so they coexist without infrastructure
    work.

- Stencil picker in `jit_stencil_picker.c`:
  - Forward dataflow over typed registers (Phase I.18 lattice).
  - At each opcode site, compute current type-state and look up the
    matching stencil ID. Fall back to most-conservative "nullable
    unknown" stencil if types disagree at a join.
- Embedded normal-interpreter blocks (`kOpEmbeddedInterp`):
  - Lowered in place using the same stencils. The aggregation register
    file is shared with the embedded program (per Phase M), so no
    boundary translation.
  - Skip-offset semantics from the embedded program's
    `WRITE_INTERPRETER_OUTPUT` are honoured by emitting a forward
    branch in the compiled blob.
- Coverage assertion in `jit.c`: any opcode encountered without a
  matching stencil triggers fallback to interpreter. CI canary
  asserts the canonical hot set is fully covered.

**Test approach.**

- All Phase 4 tests still pass.
- Direct comparison: for each `cte_filter_phase_*` MTR test, run with
  JIT on and off; results must match.
- Microbenchmark extended with each stencil family.

**Exit criterion.**

- 100% of the canonical hot opcode set has at least one stencil.
- All existing aggregation tests pass with JIT enabled.
- `bench_q9_dbtc` and `bench_q12_dbtc` measurable speedup.

**Effort.** 6-8 days. Most of the effort is writing 75 small stencils
correctly and verifying each one. The infrastructure from Phases 1-4
should not need significant changes. If the codegen escape hatch
above is taken, add ~½–1 day for generator setup but expect to
recover the cost via faster fix-once-apply-everywhere edits — net
effort estimate is unchanged.

## 12. Phase 6 — Cross-branch always-JIT test integration

**Goal.** Use the interpreter test program being built on the user's
parallel branch as a heavy-duty conformance harness for the JIT, by
flipping a switch that forces every interpreter program to be
JIT-compiled.

**Deliverables.**

- A new flag on the test harness (mirrored from the parallel branch's
  interface): `--force-jit` or equivalent. When set:
  - Every aggregation program goes through the JIT.
  - Every interpreter program (including normal-interp WHERE filters
    encountered through that test framework) is JIT'd if covered.
  - On any uncovered opcode, the test fails loud rather than silently
    falling back. This is the inverse of the production policy and is
    correct for test mode.
- Branch merge logistics: rebase or merge the parallel branch's
  test-program work into this branch. Coordinate so the CLI flag set
  is consistent.
- Update `CLAUDE.md` for the JIT directory with the test command.

**Test approach.**

- Run the full interpreter test suite from the parallel branch with
  `--force-jit`. Every program must produce identical output.
- Anything failing in this mode is a JIT bug, not a test bug, and is
  fixed before Phase 7.

**Exit criterion.** Full interpreter test suite passes with
`--force-jit`. Performance not yet a goal — correctness only.

**Effort.** 2-3 days, mostly merge mechanics + bug-fixing surfaced by
heavy testing.

## 13. Phase 7 — `SCAN_FRAGREQ` scan-filter path

**Goal.** Extend JIT compilation to scan filters — the WHERE-clause
interpreter programs that run in `SCAN_FRAGREQ` for large scans.

**Deliverables.**

- A second entry point: `ndb_jit_compile_scan_filter()` invoked from
  the scan-filter setup path. Most of the engine is shared with the
  aggregation entry point because the embedded normal-interpreter
  blocks lowered in Phase 5 already cover the same opcodes.
- Heuristic for *when* to compile scan filters (per `plan.md` §6 in
  the original investigation):
  - If `SCAN_FRAGREQ` carries a "hot scan" hint, compile.
  - Otherwise, run interpreted for the first N rows; if the scan
    continues, compile in the background of the same scan and switch
    over. (Deferred — start with always-compile-if-large-program and
    revisit.)
- Extended fallback story: scan-filter programs that use `CALL`/
  `RETURN` or any uncovered opcode fall back to interpreter,
  identically to the aggregation path. **The fallback decision is
  per-program at scan-setup time**, matching §2's commitment — a
  scan that begins interpreted runs every row of that scan
  interpreted (the only exception is the deferred compile-then-switch
  optimisation listed two bullets above, which is explicitly
  post-v1 and would require its own §2 amendment if and when
  revived).

**Test approach.**

- All CTE filter MTR tests run with and without JIT; results match.
- A new perf scenario: large CTE scan where the WHERE clause is the
  hot path. Measure end-to-end query time.

**Exit criterion.** Scan-filter JIT enabled by default for large
scans, no regressions in any CTE test suite.

**Effort.** 4-5 days.

## 14. Phase 8 — Production readiness

**Goal.** Polish JIT for default-on production use.

**Deliverables.**

- **Perf counters.** New NDBINFO table or extend an existing one:
  `ndb_jit_compiles`, `ndb_jit_compile_ns_total`, `ndb_jit_runs`,
  `ndb_jit_fallbacks_total`, `ndb_jit_arena_bytes_used`.
- **Error reporting.** `ndb_jit_last_error()` strings logged via
  `g_eventLogger->info` when fallback occurs in production
  (rate-limited).
- **Default behaviour.** Config parameter `JoinAggCompiledInterpreter`
  with values `OFF` / `AUTO` / `ON`. Default `AUTO`: compile programs
  >K opcodes covering supported opcodes only.
- **Arena lifecycle.** **One arena per data node**, owned by the
  `DblqhProxy` (matches the per-node compile scope decided in §10.1),
  sized from a config parameter, sub-allocated for compiled programs.
  All LDM workers read RX bytes from this single arena; the proxy is
  the sole writer. On OOM, release LRU programs; if can't, fall back
  to interpreter. Counter reporting (next bullet) is naturally
  per-node because there is only one arena per node.
- **`DUMP` commands** (debug builds only): toggles for force-on /
  force-off / log-stats.
- **Crash diagnosis.** JIT'd code has no symbols, so a SIGSEGV inside
  a compiled blob lands at a bare PC with no backtrace, no source
  line, and no stencil identity — `gdb`'s `bt` shows the helper-call
  frame above (if any) and then `??`. To make these survivable in
  production:
  - Each `NdbJitProg*` retains, alongside the RX pointer, a **debug
    sidecar** kept on the writable side of the arena (or in a
    parallel `malloc` block — it is never executed): the original
    bytecode words, the stencil-ID array used to lower it, the
    `(byte_offset → bytecode_pc)` map produced during emission, and
    the post-meet type-state snapshot at each opcode site. The
    sidecar is small (tens of bytes per opcode) and is referenced
    by the `NdbJitProg*` itself, so a single pointer is enough to
    recover everything.
  - A signal handler / `ndbrequire`-on-SIGSEGV path, registered on
    block startup, walks the live JIT arenas to identify which blob
    the faulting PC belongs to (RX pointer ranges are sorted in the
    proxy's program list — O(log N) search) and dumps:
    - the bytecode word offset of the opcode being executed (looked
      up via the byte_offset map);
    - the stencil ID at that site;
    - the per-register type-state at that site;
    - the surrounding 16 bytes of the RX blob in hex;
    - the full bytecode of the program;
    - the proxy + worker block instance numbers.
    Output goes to the cluster error log via the existing
    `g_eventLogger->fatal` path, prefixed `JIT-CRASH:` for grep.
  - A `DUMP` command (debug builds) prints the sidecar for any live
    program by id, so the same data is reachable without needing a
    crash to inspect a suspect program.
  - The sidecar is **not** consulted on the per-row hot path —
    purely for post-mortem and `DUMP`. A unit test asserts
    `entry_fn` does not load any sidecar field.
  - `perf` integration (perf-map files á la CPython's `Tools/jit/`)
    is **out of scope for v1**: it would require emitting a JIT-info
    file per process, which conflicts with the multi-tenant LDM
    model. The sidecar covers the in-process diagnostic need; `perf`
    can be revisited if external profiling pressure justifies it.
- **Documentation** updated:
  - `storage/ndb/CLAUDE.md`: new section pointing at this directory.
  - `storage/ndb/claude_files/compiled_interpreter/CLAUDE.md`: index
    of phase docs.
  - User-facing docs (separate task; not blocking).

**Test approach.**

- All previous tests still green.
- Long-running stress test that flips JIT on/off mid-flight and
  verifies no leaks, no use-after-free in arena, no signal-thread
  delays beyond budget.

**Exit criterion.** JIT shipping enabled in production builds with
`AUTO` default. No regressions in any test suite. Verified speedup on
TPC-H benchmark queries.

**Effort.** 3-4 days.

## 15. Test strategy

Tests come in three layers, applied at the phase where they make sense:

1. **Standalone microbenchmark** (`storage/ndb/test/jit_proto/`).
   Phases 0-3 live entirely here. No DBTUP, no signals. Fast iteration,
   easy to debug under gdb. Removed before merging the integration
   commits, but kept on the branch for reference until Phase 6.
2. **In-tree DBTUP integration tests** (`block_unit_test/testJoinAgg`,
   `testJoinAggSpj`, etc.). Phases 4-5 use these. Programs covered by
   the JIT must produce identical output to interpreted; programs not
   covered must fall back cleanly.
3. **Cross-branch conformance harness.** Phase 6 picks up the
   interpreter test program from the parallel branch and runs it with
   `--force-jit`. This is the heavy correctness layer — it forces
   every code path through the JIT and surfaces bugs that the focused
   in-tree tests miss.

CI hook for stencil regeneration: a job that runs `regen-stencils`
with the **pinned upstream LLVM clang `19.1.7`** (§2) and fails if
the checked-in headers drift. The job's first step asserts the
`clang --version` first line starts with `clang version` (rejects
Apple clang outright) and that the version string equals `19.1.7`,
so neither a CI image refresh that brings in a newer upstream clang
nor a macOS-hosted runner using Apple clang can silently produce new
bytes. Catches the case where someone edits `stencils_src.c` without
regenerating, where two devs use different upstream clang patch
levels, or where a macOS dev unwittingly uses `/usr/bin/clang`.

## 16. Risks

1. **Compile time amortization on small row counts.** Programs that
   run on <K rows may lose to the interpreter. Mitigation: heuristic
   in §6 of the investigation, fallback path, and the `AUTO` config
   default.
2. **Specialization explosion.** If type-prop gives up too often, we
   fall through to nullable-unknown stencils and lose the win.
   Mitigation: instrument specialization-hit-rate as an NDBINFO
   counter; if <80%, revisit lattice.
3. **Stencil drift across clang versions.** Different clang versions
   (and even different patch levels of the same minor) produce
   different bytes. Apple clang adds a second drift axis — Apple's
   version numbers do not map to upstream LLVM, so `Apple clang 17`
   and `clang version 17` produce different codegen, and any macOS
   dev who runs `regen-stencils` with `/usr/bin/clang` would silently
   corrupt the headers. Mitigation: pin **upstream LLVM clang 19.1.7**
   as named in §2 and explicitly reject Apple clang; record the pin
   in three coupled places (§2, `jit/CMakeLists.txt`'s
   `NDB_JIT_CLANG_VERSION`, `jit/README.md`); CI's `regen-stencils`
   job rejects any compiler whose `--version` first line does not
   start with `clang version` *and* whose trailing version string is
   not exactly `19.1.7`, before any bytes are produced. macOS devs
   regenerating stencils install upstream clang via `brew install
   llvm@19` (or use the Linux dev container). Regenerate-and-fail-CI
   policy if the headers drift. Bumping the pin (e.g., to clang 20)
   is a deliberate code change that touches all three places and
   regenerates the headers in one commit.
4. **macOS dev experience.** macOS ARM64 perf is non-critical, but it
   must not be so slow it blocks dev iteration. Mitigation: the simple
   `MAP_JIT` toggle is fast enough; if it isn't, add a `--no-jit` dev
   flag. macOS x86_64 is **not a build target** for this feature —
   that path keeps using the interpreter, so there is no dev-experience
   risk on Intel Macs.
5. **C++/C lifetime mishaps at the boundary.** `NdbJit*` lifetime
   crossing the proxy-to-worker boundary is the classic shape of bug:
   the proxy is sole writer to the arena while N LDM workers
   concurrently read the published RX bytes. Mitigation: arena tied
   to `DblqhProxy` block construction (single owner); workers hold
   borrowed references, never `release`; release-store on
   `ndb_jit_publish` paired with acquire-load on worker SETUP, plus
   `membarrier ... SYNC_CORE` after the icache flush so each LDM CPU
   issues the required `ISB` on ARM64 before executing the new code;
   `ndb_jit_release` is reachable only from the proxy after a fan-in
   acknowledgement from all workers.
6. **ARM64 relocation handling in extractor.** `R_AARCH64_ADR_PREL_PG_HI21`
   + `_LO12_NC` page-pair addressing is the most error-prone bit of
   the extractor. Mitigation: dedicated extractor unit tests on
   hand-crafted minimal `.o` inputs.
7. **Coverage drift on opcode additions.** Adding a new agg opcode
   without a stencil silently routes through fallback. Mitigation: CI
   assertion that every opcode in the canonical hot set has a stencil
   ID registered.
8. **Hardened-kernel deployment refusing RWX pages.** SELinux
   `deny_execmem`, gVisor, and some container runtimes silently reject
   `PROT_READ|PROT_WRITE|PROT_EXEC` mappings, which would cause JIT to
   fail at every node start in those environments. Mitigation: dual-
   mapping arena (`memfd_create` + two `mmap`s) implemented in Phase 0,
   not deferred to production-readiness. Phase 0 smoke test exercises
   the dual-mapping path under a deliberately RWX-hostile sandbox so the
   policy is validated before any stencils exist.
9. **Magic-byte collision in sub-word hole detection.** A
   sufficiently unlucky magic constant could appear inside a legitimate
   instruction encoding, causing the extractor to mis-identify a hole
   site and corrupt the stencil. Mitigation: scope the magic-byte
   mechanism to the narrow set of sub-word-immediate hole-kinds only;
   every full-width hole is relocation-driven and immune. A CI audit
   (Phase 2) disassembles every generated stencil and asserts each
   declared magic constant appears exactly the expected number of
   times in its declaring stencil and zero times anywhere else; any
   collision halts CI and forces a new constant.
10. **Backward branches sneaking past the JIT.** A program containing a
    backward `BRANCH_*` (especially in an embedded normal-interpreter
    block, since those opcodes have no upstream forward-only invariant)
    would corrupt the fixup table and produce broken code. Mitigation:
    explicit admission walk in `ndb_jit_compile` (§9.1) that rejects
    the whole program before any emission. Pre-pass unit tests cover
    self-branches, backward branches, out-of-range targets, and
    backward branches inside embedded blocks. The interpreter remains
    capable of running every rejected program, so the failure mode is
    a missed JIT opportunity, not incorrect results.

## 17. Open questions deferred past Phase 0

- Cold-opcode call mechanism: `mov rax, imm64; call rax` (12 bytes)
  vs. trampoline table (8 bytes plus one cache line shared). Decide
  during Phase 5 with measurement.
- Cache-line alignment policy for compiled blobs. Decide during Phase 4
  based on smallest-program size distribution.
- Whether to JIT primary-key-read interpreter programs (LQHKEYREQ
  path). Default no for v1; revisit only if benchmarks show the case.
- Persistent compiled-program cache (Rondis precompile use case).
  Explicitly post-v1.
- Cross-program inlining: lower an embedded-normal-interp block once
  per call site vs. once globally as a callable native function.
  Phase 5 default is once-per-call-site (inline). Revisit if hot
  patterns repeat across many programs.
- Stencil authoring: hand-written vs. table-driven codegen. Phase 5
  starts hand-written; the trigger to switch is documented in §11
  (≥ 2 days in with < 30% passing, **or** the same fix landing in ≥ 4
  stencils). Worth promoting from "if it slows down" to default
  practice if Phase 5 actually hits the trigger — once the generator
  exists, future opcode additions almost certainly want to flow
  through it.

## 18. References

- Xu & Kjolstad, "Copy-and-Patch Compilation," OOPSLA 2021.
- CPython 3.13 JIT: PEP 744; `Tools/jit/` in CPython source.
- Phase I.18 typed registers: `../pushdown_join_aggregation/cte_filter_phase_i18.md`.
- Phase M BranchReg removal: `../pushdown_join_aggregation/cte_filter_phase_m.md`.
- Aggregation interpreter dispatch:
  `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp:1226-1867`.
- Normal interpreter dispatch:
  `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp:9844-10334`.
- DBLQH JOIN_AGG_SETUP_REQ entry:
  `storage/ndb/src/kernel/blocks/dblqh/DblqhProxy.cpp:2336-2435`.
- Portability headers: `storage/ndb/include/portlib/NdbMem.h`,
  `storage/ndb/include/portlib/mt-asm.h`.
- Vendoring precedent for build-time tools: `extra/lz4`, `extra/zstd`.
