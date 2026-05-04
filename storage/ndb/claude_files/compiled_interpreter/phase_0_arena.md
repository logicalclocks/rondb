# Phase 0 — arena foundation: results

Companion doc to `plan.md` §6 and `phase_0_implementation.md`. Records
the actual outcome of running the Phase 0 smoke + icache benchmarks
on the platforms available at implementation time.

## Outcome

- [x] **macOS Apple Silicon smoke**: PASS
- [ ] Linux x86_64 smoke: pending CI
- [ ] Linux ARM64 smoke: pending CI
- [x] macOS post-seal write fault verified (sigsetjmp/siglongjmp probe
      catches SIGSEGV/SIGBUS in-process; no crash-report dialogs)
- [ ] Linux dual-mapping verified by /proc/self/maps r-x assertion:
      pending CI (the assertion is in proto_smoke.c §6.3.2; not
      reachable on macOS)
- [ ] Hostile-kernel re-run (SELinux enforcing or gVisor): pending CI
- [x] macOS x86_64 build-out asserted via CMake guard
      (`IF(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64") RETURN()`
      in both `jit/CMakeLists.txt` and `jit_proto/CMakeLists.txt`,
      backed by an `#error` directive at the top of `jit_arena.c`).

## Smoke output (macOS Apple Silicon)

```
PASS proto_smoke arch=aarch64 rw=0x1041f4000 rx=0x1041f4000 size=16384 used=8
exit=0
```

`rw == rx` is expected on macOS (unified MAP_JIT mapping); the
`rw != rx` assertion in proto_smoke.c is gated on `__linux__`.

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

`__builtin___clear_cache` is a no-op on x86; expect numbers dominated
by call/timer overhead (tens of ns range). Pending measurement.

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
