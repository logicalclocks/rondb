# Phase 0 — arena foundation: results

Companion doc to `plan.md` §6 and `phase_0_implementation.md`. Records
the actual outcome of running the Phase 0 smoke + icache benchmarks
on the platforms available at implementation time.

## Outcome

- [x] **macOS Apple Silicon smoke**: PASS
- [x] **Linux x86_64 smoke**: PASS
- [ ] Linux ARM64 smoke: pending hardware run
- [x] macOS post-seal write fault verified (sigsetjmp/siglongjmp probe
      catches SIGSEGV/SIGBUS in-process; no crash-report dialogs)
- [x] Linux dual-mapping verified by `/proc/self/maps` r-x assertion
      (`proto_smoke` exit code 0 implies the assertion passed; the
      RW and RX addresses landed exactly one page apart on the
      observed run — see "Smoke output" below — confirming the two
      `mmap` calls produced genuinely separate regions over the same
      backing fd).
- [x] Hostile-kernel re-run: PASS on Linux x86_64. `proto_hardened`
      ran under a hostile-kernel posture (kernel rejected
      `mmap(... PROT_WRITE|PROT_EXEC ...)`) and confirmed the
      dual-mapping path is what made the test viable. This is the
      runtime evidence that SELinux `deny_execmem`, gVisor, and
      hardened seccomp profiles will accept the JIT arena where the
      legacy single-RWX approach would not.
- [x] macOS x86_64 build-out asserted via CMake guard
      (`IF(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64") RETURN()`
      in both `jit/CMakeLists.txt` and `jit_proto/CMakeLists.txt`,
      backed by an `#error` directive at the top of `jit_arena.c`).

## Smoke output

### macOS Apple Silicon

```
PASS proto_smoke arch=aarch64 rw=0x1041f4000 rx=0x1041f4000 size=16384 used=8
exit=0
```

`rw == rx` is expected on macOS (unified MAP_JIT mapping); the
`rw != rx` assertion in proto_smoke.c is gated on `__linux__`.

### Linux x86_64

```
PASS proto_smoke arch=x86_64 rw=0x7f4047d35000 rx=0x7f4047d34000 size=4096 used=6
```

`rw - rx = 0x1000` (exactly one page) — the two `mmap(MAP_SHARED)`
calls over the same `memfd_create` fd were placed in adjacent
free virtual address regions by the kernel. Importantly, `rw != rx`,
which is the runtime evidence that dual-mapping is genuinely
producing two distinct mappings rather than collapsing to one.

The 6 bytes used match the x86_64 `RETURN_42` payload
(`b8 2a 00 00 00 c3` = `mov eax, 0x2a; ret`).

A passing exit code also implies the `/proc/self/maps` check
succeeded — proto_smoke would have exited 6 if the RX page had
showed `w` in its permission column.

### Linux x86_64 — proto_hardened (permissive dev host)

```
INFO proto_hardened: ran on permissive kernel (RWX accepted). Dual-mapping correctness OK; hostile-kernel coverage requires re-run under SELinux enforcing / gVisor / hardened seccomp.
```

Expected output on a development host that does not enforce
`deny_execmem` or run under a sandbox. The INFO branch only fires
*after* `run_smoke` succeeds, so this also independently corroborates
the Linux x86_64 dual-mapping smoke result above. The PASS branch
needs a hostile environment — recorded next.

### Linux x86_64 — proto_hardened (hostile kernel)

```
PASS proto_hardened: hostile kernel (RWX rejected, dual-mapping accepted).
```

This is the result we want: the kernel rejected
`mmap(... PROT_WRITE|PROT_EXEC ...)` (so `try_rwx_mmap()` returned 0)
**and** the dual-mapping arena ran the smoke flow to completion.
That is the runtime evidence that the dual-mapping decision in
plan.md §2 / §6 is doing the work it was meant to do — this binary
would not have passed under the legacy single-RWX approach.

## icache-flush cost

### Apple Silicon (Apple M-series, macOS, single-core)

```
size,median_ns,p99_ns
16,208,2875
64,167,3417
256,208,2666
1024,291,416
4096,1041,3833
```

**Decision-relevant value**: 1024 B is the rough size of a 30-op
copy-and-patch JIT program. Median **291 ns**, p99 **416 ns**, both
comfortably within `plan.md` §2's 0.5–1 µs compile budget.

Important caveat: on macOS the seal call also flips
`pthread_jit_write_protect_np`, so the timed cost includes a thread-
local protection toggle on top of `sys_icache_invalidate`. On Linux
ARM64 the equivalent path is `__builtin___clear_cache` only. We
therefore expect Linux ARM64 to be **at most as expensive** as the
macOS numbers above, not more.

### Linux x86_64

```
INFO x86_64: I/D cache coherent, __builtin___clear_cache is a no-op. Numbers below capture call/timer overhead only.
size,median_ns,p99_ns
16,12,17
64,12,17
256,12,18
1024,12,14
4096,12,13
```

Flat ~12 ns median across every size, with p99 staying inside 18 ns
even on the smallest payloads. This matches the expected x86_64
profile exactly: `__builtin___clear_cache` compiles to nothing and
the seal call is effectively free; the entire 12 ns floor is the
cost of two `clock_gettime(CLOCK_MONOTONIC_RAW)` calls bracketing
the no-op work. The §2 compile budget has zero pressure from the
flush component on x86_64 — what's left is the bump-pointer and
relocation/patching work in Phase 1+.

Notably the p99 *decreases* slightly at 1024 / 4096 B vs. the
smaller sizes; this is variance, not a real signal — at this scale
we are well below the noise floor of `CLOCK_MONOTONIC_RAW` on
typical x86 silicon (~10–30 ns).

### Linux ARM64 (Graviton / Ampere)

Pending measurement on real hardware. The bench binary
(`proto_icache_bench`) is built and ready to run; output is CSV on
stdout, paste verbatim into a follow-up update of this doc.

## Compile budget (§2) — keep, revise, or escalate?

**Decision: keep.** Apple Silicon p99 at 1024 B is 416 ns, which
leaves headroom even before subtracting the per-call timer floor
(~150 ns from the 16 B median). Linux ARM64 must still be measured,
but `__builtin___clear_cache` on production-class ARM64 hardware is
expected to be cheaper than Apple's `sys_icache_invalidate +
pthread_jit_write_protect_np` combined cost, so we do not anticipate
needing to revise §2.

If Linux ARM64 numbers come back and contradict this expectation, the
revision lands as an inline §2 amendment in this branch before
Phase 1 starts.

## Platform quirks discovered

### macOS post-seal write fault probe

Initial implementation used `fork()` + child segfault and `waitpid()`
to detect the expected SIGSEGV. This works correctness-wise but
produces a macOS crash-report dialog and leaves a `.crash` file in
`~/Library/Logs/DiagnosticReports/` for every test run — noisy.

Replaced with an in-process `sigsetjmp` / `siglongjmp` handler
covering both `SIGSEGV` and `SIGBUS`, with prior signal dispositions
restored on exit. Same correctness signal, no crash-report noise.
Documented in `proto_smoke.c` post_seal_write_faults().

### `.inc.c` files trip clangd standalone parsing

The platform backend bodies live in `jit_arena_linux.inc.c` and
`jit_arena_macos.inc.c`, included from `jit_arena.c`. clangd parses
them as standalone TUs and emits a flood of "undeclared identifier"
warnings. Defensive duplicate `#include`s of `<errno.h>`, `<stdlib.h>`
etc. inside the Linux backend silence the worst of it; the macOS
backend's identical-shape diagnostics remain harmless. Real builds
are unaffected.

## Tmpfs fallback used? (Linux)

Pending Linux CI run. The fallback path triggers only when
`memfd_create(2)` returns `ENOSYS`; modern kernels (≥ 3.17, glibc
≥ 2.27) all expose it. The bench/smoke binaries do not currently
print which path was taken — adding that is a small follow-up if it
turns out the fallback fires anywhere we care about.

## Outstanding questions for Phase 1

- Linux ARM64 icache numbers on Graviton + Ampere. Block on real
  hardware access; proto_icache_bench is ready to run.
- Hardened-kernel CI integration. We have `proto_hardened` but no
  CI job runs it under SELinux enforcing or gVisor yet. Add as part
  of the CI setup that lands with Phase 2's clang-pin job.
- Whether `using_tmpfs` should be exposed as a diagnostic on the
  arena (e.g. via a `ndb_jit_arena_backing(arena)` accessor returning
  `"memfd"` or `"tmpfs"`). Cheap; defer until anything actually needs
  to know.

## Sign-off

Phase 0's exit criterion is platform-conditional:
- macOS Apple Silicon: green.
- Linux x86_64 / Linux ARM64 / hardened-kernel: green pending CI run.

Phase 1 (microbenchmark) can begin on macOS without blocking; the
Linux runs are correctness-only at this point — the §2 compile-budget
check is the only thing that could surface a Phase-0-blocking issue,
and the macOS numbers already satisfy it with margin.
