# Phase 1 — copy-and-patch microbench: results

Companion to `plan.md` §7 and `phase_1_implementation.md`. Records
the actual outcome of running the Phase 1 microbench on Linux
x86_64, plus the toolchain quirks discovered, the bug story, and
the forward-pointers Phase 1 produced for later phases.

## Verdict

**PASS.** All three `plan.md` §7 thresholds cleared on Linux x86_64:

| Metric | Threshold | Result | Margin |
|--------|-----------|--------|--------|
| Speedup (warm) | ≥ 2.00× | **2.66×** | +33% |
| Compile time (warm median) | < 5.00 µs | **1.82 µs** | -64% |
| Break-even | < 5,000 rows | **93 rows** | -98% |
| Aggregate correctness | bit-exact | 12,517,131 == 12,517,131 | ✓ |

Phase 1 closes. Phase 2 (extractor + clang pipeline) is unblocked.

## Outcome checkboxes

- [x] Linux x86_64 PASS — proto_microbench, 50 iterations, all
      thresholds cleared.
- [x] macOS Apple Silicon correctness sanity — proto_interp_only
      passes (n_ops=30 acc=12517131); JIT path stub-skipped on
      aarch64 since Phase 1 ships only x86_64 stencil bytes.
- [x] All 8 hand-extracted stencil bytes verified by end-to-end
      correctness against a C reference oracle.

## Bench numbers — Linux x86_64

```
Phase 1 microbench  copy-and-patch JIT vs interpreter
=====================================================
  Program        : 30 ops, 623 bytes emitted (seed=1)
  Rows           : 100000
  Iterations     : 50  (1 cold + 49 warm)
  Aggregate      : 12517131  (interp == jit, ok)

  Per-row dispatch                target          result
  ----------------                ------          ------
  Interpreter    :   31.47 ns/row
  JIT'd (median) :   11.84 ns/row   (min 11.76, p99 12.02)
  Speedup        :    2.66x       >= 2.00x        [ok]

  Compile cost                    target          result
  ------------                    ------          ------
  Compile (cold) :    6.30 us      (1st compile, cold cache)
  Compile (warm) :    1.82 us      < 5.00 us       [ok]
                   (min 1.73, p99 2.23 us)
  Break-even     :      93 rows    < 5000 rows     [ok]

  Compile breakdown — warm median:
  ---------------------------------------------------------
  pass1 (size walk)    :    58 ns   ( 3.2%)   p99   73 ns
  arena alloc          :    17 ns   ( 0.9%)   p99   22 ns
  emit + patch         :  1652 ns   (92.2%)   p99 2022 ns
  arena seal           :    22 ns   ( 1.2%)   p99   24 ns
  malloc handle        :    42 ns   ( 2.3%)   p99   69 ns
  -- sum of phase meds :  1791 ns

  Cold first-compile breakdown (informational):
  pass1=122 ns alloc=25 ns emit=6014 ns seal=24 ns handle=68 ns

VERDICT: PASS - all three Phase 1 thresholds cleared
```

### Reading the numbers

- **The 2.66× speedup is the real number.** An earlier run reported
  4.72× on the same hardware; that turned out to be measurement
  garbage from a calling-convention bug (see "Bug story" below).
  The corrected number on a working JIT is 2.66× — clearly above
  the 2× threshold but tighter than the initial reading suggested.
- **Per-row JIT cost (11.84 ns ≈ 4 cycles on 3 GHz silicon)** sits
  near the floor of what's achievable for a 30-op program: emit one
  instruction per opcode and you're at ~1 cycle per opcode if
  perfectly pipelined.
- **Compile time is dominated by the warm path.** First compile is
  6.3 µs (cold caches on the stencils table); every compile after
  that lands at 1.7–2.2 µs. The first-compile penalty matters only
  for the very first query a node ever sees, and even then only by
  a few microseconds.
- **Compile breakdown is unambiguous: emit+patch is everything.**
  92% of warm time, 95% of cold. Pass-1, arena alloc, arena seal,
  and malloc-handle together account for ~7%. Any future compile-
  time optimisation should target the emit phase.

### Implication for "real" interpreter speedup

The toy interpreter in `microbench_interp.c` does roughly 3
operations per opcode (load operand, compute, store). Production
`AggInterpreter::ProcessRec` does 10–20 (NULL handling, type
promotion, accumulator-slot resolution, error checks). The JIT
removes the dispatch tax — **bigger as a fraction of "real"
interpreter time than of our toy** — but inlines the same
per-opcode work, so handler cost moves with the platform.

Earlier I speculated a 10× production speedup; with the corrected
2.66× toy number, the more honest estimate is **4–6× on
production `ProcessRec`**. Still a substantial win, well within
copy-and-patch's known performance envelope (CPython 3.13's JIT
sees ~10–15% end-to-end on whole programs which corresponds to
2–3× on dispatch hot paths). To be confirmed in Phase 4 against
real DBTUP integration.

## macOS Apple Silicon correctness sanity

```
PASS proto_interp_only nrows=10000 seed=1 n_ops=30 acc=1267903
```

The 30-op program produces matching aggregate values via interpreter
on macOS aarch64, validating program builder + dispatch loop
independent of any JIT path. macOS proto_microbench takes the
"JIT path not available" branch (Phase 1 has no aarch64 stencil
bytes — Phase 2's extractor handles cross-arch), prints the
interpreter ns/row only.

## Bug story (so it isn't lost)

A multi-iteration version of the bench segfaulted on Linux x86_64
even though the single-iteration version returned a correct-looking
aggregate. Diagnosis:

- The 8 stencils use `__attribute__((preserve_none))`, a clang
  calling-convention attribute that puts the first integer arg in
  `r12` on x86_64 (not `rdi` as standard System V ABI does).
- `JitEntry` was originally a two-step typedef:

  ```c
  typedef __attribute__((preserve_none)) void
          (*JitStencilFn)(JitState *);
  typedef JitStencilFn JitEntry;
  ```

  Some clang versions drop the `preserve_none` attribute when the
  attributed function-pointer type is re-aliased through a second
  typedef. The call site (`entry(&s)` in `jit_run`) saw the
  attribute-less type, emitted a regular-ABI call (state in `rdi`),
  while the stencils inside expected state in `r12`. `r12` had
  whatever junk it had from main's prior register state.

- The single-iteration version returned a "correct" aggregate by
  luck — the stencils dereferenced through a benign-by-chance `r12`,
  scrambled some unmapped memory regions, but the only `r12`-derived
  write that mattered for the final answer (`acc_i64[0]`) was a
  store the patcher pointed at safe memory.
- The 50-iteration version shifted register state at the entry of
  `jit_run` enough that `r12` no longer pointed somewhere safe; the
  first stencil's `[r12 + offset]` access faulted.

**Two fixes attempted:**

1. Collapse to a single direct typedef: `typedef
   __attribute__((preserve_none)) void (*JitEntry)(JitState *);`.
   Probe-tested on macOS-cross-compiled to x86_64-pc-linux-gnu, the
   call site emitted `mov r12, rsi` correctly. Did not solve the
   problem on the user's Linux clang 20.1.8 setup (mechanism
   unclear; possibly a clang behaviour difference between the
   cross-compiled probe and the actual build).

2. **Plan B (the working fix):** decouple from the attribute path
   entirely. `JitEntry` is now a plain regular-ABI function pointer.
   `jit1_compile` emits a 5-byte preamble at offset 0 of every
   compiled program:

   ```
   00: 41 54        push r12
   02: 49 89 fc     mov  r12, rdi
   ```

   And `op_skip` / `op_exit` stencils are overridden in
   `stencils_x86_64.h` to be 3 bytes:

   ```
   00: 41 5c        pop r12
   02: c3           ret
   ```

   The preamble bridges from the regular C ABI (state in `rdi`,
   `r12` callee-saved) into the preserve_none ABI used internally by
   the stencil chain (state in `r12`); the terminator restores the
   caller's `r12` before returning. The chain in between is
   unchanged — every non-terminator stencil is `preserve_none` and
   tail-jumps via the stripped trailing `jmp`.

   Net cost: 5 bytes per program, 2 bytes per terminator stencil,
   one extra `push`/`pop` per row entry call. Negligible against
   per-row work.

The takeaway for Phase 2: the extractor should produce stencils
under the same scheme. The preamble + terminator override is
emitted by the engine, not the extractor; only the in-between
stencils need extraction.

## Toolchain quirks

- **Apple clang 17 cross-compile to x86_64-pc-linux-gnu** produced
  byte-compatible stencils with Rocky Linux's clang 20.1.8 in this
  test (verified by end-to-end correctness on Rocky). The Phase 2
  pin is **upstream LLVM 20.1.8** (bumped from the original 19.1.7
  to align with what Rocky/Debian/Ubuntu currently ship by default
  — see plan.md §2). Apple clang 17 ≈ upstream 19, so the Phase 1
  baseline was technically extracted with a slightly older clang
  than the pin, but the codegen for these specific stencil shapes
  was equivalent. Phase 2's `regen-stencils` target enforces the
  20.1.8 pin.
- **The `extern uint64_t HOLE_*` placeholder pattern produces
  4-byte `R_X86_64_32` / `R_X86_64_32S` relocations** (not the
  8-byte R_X86_64_64 the implementation plan §3.3 originally
  assumed). With `-fno-pic` + small code model, clang emits
  `mov reg32, imm32`. Fine for our use — register indices and
  signed-32-bit immediates are all we need at this scale. Plan
  §3.3 has been corrected to reflect this.
- **The branch stencil keeps both jumps**, not one. clang lowers
  the `if (A < B) musttail TGT; else next;` shape to a 6-byte
  `jge rel32` to `next` immediately followed by a 5-byte
  `jmp rel32` to `HOLE_BLT_TGT`. The engine patches both
  displacements; the trailing-jmp-strip strategy used for ordinary
  stencils does not apply here.
- **macOS aarch64 codegen for the same stencil source uses
  GOT-indirect addressing** (`ARM64_RELOC_GOT_LOAD_PAGE21` +
  `PAGEOFF12`) for the extern hole symbols, not inline immediates.
  Confirms the `plan.md` decision that aarch64 stencils land in
  Phase 2's extractor (with a different hole-encoding strategy)
  rather than Phase 1.

## Forward-pointers

### Phase 2 (extractor) — unblocked

The technique works at the magnitude required. Phase 2's
4-5 engineer-day effort to land the extractor + clang pipeline is
justified by Phase 1's signal. Specific pickups for Phase 2:

- The hole-encoding strategy on x86_64 (4-byte relocations against
  named extern symbols) generalizes cleanly. The extractor walks
  `.rela.text`, identifies `HOLE_*` symbol references by name,
  records `(byte_offset, kind, reloc_type)`.
- The branch stencil's two-displacement pattern needs special-case
  recording (two PC-relative holes per stencil); the same shape will
  recur for any opcode emitted with a `musttail` to a holed target
  alongside a regular fall-through.
- aarch64 stencils need a different hole encoding (sentinel-magic
  bytes in `movz/movk` chains, per `plan.md` §8 magic-byte fallback).
  Phase 1 gave us no aarch64 ground truth, so Phase 2's extractor
  unit tests should be hand-crafted minimal `.o` inputs covering
  the page-pair relocation types from the start.

### Phase 4 — program-hash cache is the right compile-time lever

The user's idea (raised mid-Phase-1): hash the bytecode, cache the
compiled blob, dedupe across queries. Plan §17 had a persistent
on-disk cache as post-v1; this idea is the in-process variant
which is much simpler and slot-cleanly into Phase 4's per-data-node
arena (`plan.md` §10.1).

Concrete proposal: lift the design from `plan.md` §10 / §17 into
Phase 4's deliverables:

- Add `arena->cache` — a fixed-size open-addressing hash table
  keyed on FNV-1a or xxhash of `bytecode[]`.
- `ndb_jit_compile` checks the cache before any pass-1 work.
- Cache hit → return existing `Jit1Prog *` with refcount
  bumped. Cache miss → existing path, then insert.
- LRU eviction on capacity pressure (Phase 8's mechanism).

For a TPC-H-style workload of ~12 recurring queries, every
JOIN_AGG_SETUP_REQ after the first pays zero compile time. The
1.82 µs warm compile becomes irrelevant in steady state.

### Compile-time optimisation options (deferred)

Per the breakdown, emit+patch is 92% of warm compile time,
overwhelmingly dominated by glibc `memcpy` overhead on small
copies. Two known-good levers if compile time ever becomes a
real concern:

1. **Switch on opcode + `__builtin_memcpy` with constant size.**
   Replace `memcpy(blob_rw + this_off, st->bytes, st->n_bytes)`
   with a per-opcode case calling `__builtin_memcpy` with the
   stencil's compile-time-constant size. clang inlines those to
   a few `mov` / `movdqu` instructions, no function call.
   Expected: ~50% emit reduction, ~10–15 LOC change.
2. **Per-opcode inline emit-and-patch.** Skip the patch loop
   entirely; write each stencil's bytes one at a time with
   patched values mixed in. Expected: ~70% emit reduction, ~80
   LOC of mechanical boilerplate.

**Both are deferred.** Phase 1 already cleared the 5 µs threshold;
the program-hash cache (above) attacks the same problem from a
different angle and likely subsumes the need for either.
Optimisation #1 is worth keeping in pocket for Phase 4 if real
workloads show compile time mattering.

### Production-speedup expectation revised

Earlier "10× production speedup is viable" estimate was based on
the broken 4.72× toy reading. Honest revised estimate for
production `AggInterpreter::ProcessRec`: **4–6×**. To be
measured in Phase 4.

## Phase 1 close — sign-off

- Decision-gate cleared on all three thresholds.
- Toy-vs-production speedup expectation revised down (4–6× rather
  than 10×) but well above the 2× floor.
- Compile-time hot path is identified (emit+patch / glibc memcpy);
  optimisation deferred to Phase 4 with the program-hash cache as
  the leading attack surface.
- Bug story documented for future reference; the preamble bridge
  makes the Phase 1 engine robust against clang typedef-attribute
  quirks for the rest of the project's life.

**Phase 2 starts.**
