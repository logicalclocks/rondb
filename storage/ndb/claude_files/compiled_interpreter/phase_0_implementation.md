# Phase 0 — Implementation Plan (RONDB-1056)

Status: ready to implement. Branch: `RONDB-1056-compiled-interpreter`.

Companion doc to `plan.md` §6. Scope is exactly what `plan.md` calls
out: arena + smoke harness, no stencils, no bytecode, no DBTUP wiring.

## 1. File layout

```
storage/ndb/src/kernel/blocks/dbtup/jit/
  jit_arena.h            # private (Phase 0) header, public for smoke test
  jit_arena.c            # platform-dispatched arena impl
  jit_arena_linux.inc.c  # included from jit_arena.c when __linux__
  jit_arena_macos.inc.c  # included from jit_arena.c when __APPLE__
  CMakeLists.txt         # builds nothing on macOS x86_64 (whole dir #if'd out)

storage/ndb/test/jit_proto/
  proto_smoke.c          # standalone smoke test, compiled as a CTest target
  proto_hardened.c       # hardened-kernel variant (Linux only)
  proto_icache_bench.c   # ARM64 icache-flush microbench
  CMakeLists.txt

storage/ndb/claude_files/compiled_interpreter/
  phase_0_arena.md       # written at end of Phase 0; see §11 for template
```

`jit.h` and the rest of the engine listed in `plan.md` §4 do not yet
exist. They land in Phase 1+.

## 2. Public arena API (`jit_arena.h`)

```c
/* SPDX-License-Identifier: GPL-2.0
 * Phase 0 substrate for RONDB-1056 copy-and-patch JIT.
 * Pure C11, no NDB dependencies.
 */
#ifndef NDB_JIT_ARENA_H
#define NDB_JIT_ARENA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NdbJitArena NdbJitArena;

/* Create an arena of `size` bytes (rounded up to page size).
 * Returns NULL on failure; errno set per mmap/memfd_create. */
NdbJitArena* ndb_jit_arena_create(size_t size);

/* Tear down the arena. Safe to call with NULL. */
void ndb_jit_arena_destroy(NdbJitArena*);

/* Reserve `bytes` of arena space at `align` alignment.
 * Returns a writable pointer into the RW mapping (Linux) or the
 * write-enabled view of the unified mapping (macOS).
 * Returns NULL on OOM (no growth in Phase 0). */
void* ndb_jit_arena_alloc(NdbJitArena*, size_t bytes, size_t align);

/* Publish `bytes` bytes starting at `rw_ptr`:
 *   - Linux:  msync if needed, flush icache for the corresponding RX
 *             range, return the RX pointer (different address from rw_ptr,
 *             same physical pages).
 *   - macOS:  flip pthread_jit_write_protect_np to true, flush icache,
 *             return rw_ptr (single-mapping; addresses are equal).
 * Caller must not write through `rw_ptr` again after seal. */
const void* ndb_jit_arena_seal(NdbJitArena*, void* rw_ptr, size_t bytes);

/* Translate an RW pointer to its RX alias without a flush.
 * Useful for emitter passes that need to compute a callable address
 * before sealing (e.g. forward-branch fixup target inside the same
 * unsealed allocation). On macOS, returns the same address. */
const void* ndb_jit_arena_exec_addr(const NdbJitArena*, const void* rw_ptr);

/* Diagnostics — used by smoke test asserts and by the Phase 8
 * crash-diagnosis path later. */
size_t ndb_jit_arena_size(const NdbJitArena*);
size_t ndb_jit_arena_used(const NdbJitArena*);

#ifdef __cplusplus
}
#endif
#endif /* NDB_JIT_ARENA_H */
```

Notes on the API shape:

- `seal` is **one-shot per allocation**. No re-sealing in Phase 0.
- `alloc` is bump-pointer; no free, no LRU. Phase 8 adds those.
- Single arena per process for Phase 0. Multi-arena lifetime questions
  arrive with the per-DBLQH-proxy work in Phase 4.

## 3. Internal struct (`jit_arena.c`)

```c
struct NdbJitArena {
    size_t   size;          /* mmap'd extent, page-aligned */
    size_t   used;          /* bump-pointer offset into the arena */
    uint8_t* rw_base;       /* writable mapping start */
    uint8_t* rx_base;       /* executable mapping start (== rw_base on macOS) */
    int      memfd;         /* Linux: memfd, or -1 if tmpfs fallback used */
    int      using_tmpfs;   /* Linux: 1 if memfd_create unavailable */
#ifdef __APPLE__
    int      jit_write_enabled;  /* mirrors pthread_jit_write_protect_np state */
#endif
};
```

`exec_addr(rw_ptr)` is `rx_base + (rw_ptr - rw_base)`. On macOS the
delta is zero and the call is a no-op cast.

## 4. Linux backend (`jit_arena_linux.inc.c`)

### 4.1 Create

```
1. page = sysconf(_SC_PAGESIZE);  size = round_up(size, page);
2. fd = memfd_create("ndb_jit", MFD_CLOEXEC);
   if (fd < 0 && errno == ENOSYS) goto tmpfs_fallback;
3. ftruncate(fd, size);
4. rw = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
5. rx = mmap(NULL, size, PROT_READ|PROT_EXEC,  MAP_SHARED, fd, 0);
6. if either mmap failed: cleanup (munmap successful one, close fd), return NULL.
7. arena->memfd = fd; arena->rw_base = rw; arena->rx_base = rx; arena->using_tmpfs = 0;
8. return arena.

tmpfs_fallback:
1. char path[] = "/dev/shm/ndb_jit_XXXXXX"; fd = mkstemp(path); unlink(path);
2. ftruncate(fd, size);
3. same two-mmap dance as above with MAP_SHARED.
4. arena->using_tmpfs = 1; arena->memfd = fd;
```

`/dev/shm` is tmpfs on every Linux RonDB targets ship to. If `/dev/shm`
is absent we fall through to a hard error rather than `/tmp` (which may
be `noexec`). `using_tmpfs` is recorded in `phase_0_arena.md`.

### 4.2 Alloc

Pure bump pointer:

```
off = round_up(arena->used, align);
if (off + bytes > arena->size) return NULL;
arena->used = off + bytes;
return arena->rw_base + off;
```

### 4.3 Seal

```
off = (uint8_t*)rw_ptr - arena->rw_base;
__builtin___clear_cache((char*)(arena->rx_base + off),
                        (char*)(arena->rx_base + off + bytes));
return arena->rx_base + off;
```

x86_64: `__builtin___clear_cache` is documented as a no-op (the gcc
builtin is implemented as nothing on x86) which is correct — x86 has
coherent I/D caches.

ARM64: emits the necessary `IC IVAU` / `DSB ISH` / `ISB` sequence over
the range. Note we flush the **RX** range, not the RW range — the I-side
must be invalidated at the address from which it will be fetched.

We do **not** issue `membarrier(MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE)`
in Phase 0. That barrier is needed only when another thread will execute
the code; the smoke test runs sealed bytes on the same thread that
sealed them, where the kernel's return-from-syscall path already issues
the `ISB` we need. Phase 4 introduces the cross-thread barrier in
`ndb_jit_publish`.

### 4.4 Destroy

```
if (arena->rw_base) munmap(arena->rw_base, arena->size);
if (arena->rx_base) munmap(arena->rx_base, arena->size);
if (arena->memfd != -1) close(arena->memfd);
free(arena);
```

## 5. macOS backend (`jit_arena_macos.inc.c`)

### 5.1 Create

```
1. rw = mmap(NULL, size,
             PROT_READ|PROT_WRITE|PROT_EXEC,
             MAP_PRIVATE|MAP_ANON|MAP_JIT,
             -1, 0);
   if (rw == MAP_FAILED) return NULL;
2. arena->rw_base = arena->rx_base = rw;  /* unified mapping */
3. arena->memfd = -1;
4. arena->jit_write_enabled = 1;          /* MAP_JIT starts writable for the calling thread */
5. pthread_jit_write_protect_np(0);       /* explicit; matches state */
6. return arena.
```

### 5.2 Alloc

Same bump-pointer logic as Linux. If `arena->jit_write_enabled` is
false, we toggle it back on first (defensive — should not happen given
seal is one-shot per allocation, but cheap).

### 5.3 Seal

```
sys_icache_invalidate(arena->rx_base + off, bytes);   /* declared in <libkern/OSCacheControl.h> */
pthread_jit_write_protect_np(1);
arena->jit_write_enabled = 0;
return arena->rw_base + off;   /* same address as RX */
```

`sys_icache_invalidate` is the canonical macOS API and Apple's recommended
choice over `__builtin___clear_cache` on Darwin — equivalent effect, but
using the documented surface.

If the next `alloc` call needs to write more bytes after a seal, the
arena toggles `pthread_jit_write_protect_np(0)` on the way in. (We do
not need this in the Phase 0 smoke test, which only calls alloc once,
but the bump allocator should not break the pattern.)

### 5.4 macOS x86_64 build exclusion

Top of `jit_arena.c`:

```c
#if defined(__APPLE__) && defined(__x86_64__)
#error "RONDB-1056 JIT is not built on macOS x86_64. \
        Remove the jit/ directory from this target's CMake config."
#endif
```

Backup belt + suspenders: `CMakeLists.txt` for the `jit/` directory
short-circuits when `APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64"`.
Build-link assertion in CTest: a no-op test that links a translation
unit referencing `ndb_jit_arena_create` should *fail to link* on macOS
x86_64. (See §8.5.)

## 6. Smoke test (`storage/ndb/test/jit_proto/proto_smoke.c`)

Goal: end-to-end roundtrip, ~80 lines of C.

### 6.1 Per-arch instruction bytes

```c
#if defined(__x86_64__)
/* mov eax, 42 ; ret */
static const uint8_t RETURN_42[] = { 0xb8, 0x2a, 0x00, 0x00, 0x00, 0xc3 };
#elif defined(__aarch64__)
/* mov w0, #42 ; ret  (little-endian, 4 bytes per insn) */
static const uint8_t RETURN_42[] = { 0x40, 0x05, 0x80, 0x52,
                                     0xc0, 0x03, 0x5f, 0xd6 };
#else
#  error "unsupported test arch"
#endif

typedef int (*fn_t)(void);
```

Both sequences compute the integer 42 in the platform's int-return
register and return. Assembled by hand and verified against
`objdump -d` of a trivial `int f(void){return 42;}` compiled at `-O0`.
The bytes are immortalised in the source file as a comment so a future
reader can re-verify without an objdump round-trip.

### 6.2 Test body

```c
int main(void) {
    NdbJitArena* a = ndb_jit_arena_create(4096);
    assert(a);

    uint8_t* rw = ndb_jit_arena_alloc(a, sizeof(RETURN_42), 16);
    assert(rw);

    memcpy(rw, RETURN_42, sizeof(RETURN_42));

    fn_t fn = (fn_t)(uintptr_t)ndb_jit_arena_seal(a, rw, sizeof(RETURN_42));
    assert(fn);

    int got = fn();
    if (got != 42) {
        fprintf(stderr, "FAIL: got %d, expected 42\n", got);
        return 1;
    }

    ndb_jit_arena_destroy(a);
    fprintf(stdout, "PASS proto_smoke arch=%s\n",
#if defined(__x86_64__)
            "x86_64"
#else
            "arm64"
#endif
    );
    return 0;
}
```

### 6.3 Asserts beyond "got == 42"

These run in the same binary, after the basic call:

1. **RW/RX address relationship** (Linux only): assert `rx_addr != rw_addr`.
   Sanity check that we are actually using dual-mapping, not silently
   falling back to single-RWX.
2. **RX page protection** (Linux only): parse `/proc/self/maps`, find
   the line covering the RX address, assert the permission column
   contains `r-x` and **not** `rwx`. This is the runtime evidence that
   the W^X invariant holds, regardless of the host kernel's policy.
3. **Cross-mapping write-after-seal rejection**: in a child process
   (so the SIGSEGV doesn't kill the test), attempt
   `((uint8_t*)rw_ptr_post_seal)[0] = 0xCC;`. Linux: writing through
   `rw_base` is still legal (RW mapping is unchanged by seal); this
   subassert is macOS-only, where post-seal writes through the unified
   mapping should fault. The Linux variant instead asserts the RX
   pointer faults on write.
4. **Re-execute after seal**: call `fn()` 1000 times; result must be
   42 every time. Catches transient icache-coherency bugs.

### 6.4 Build-link assertion (macOS x86_64)

A degenerate translation unit referencing `ndb_jit_arena_create`
should fail to link. Implemented as a CTest entry whose expected
result is `WILL_FAIL TRUE` when `APPLE AND x86_64`. Confirms §5.4's
build-out.

## 7. icache-flush microbench (`proto_icache_bench.c`)

ARM64 only; on x86_64 the test compiles to a no-op that just prints
"x86: I/D cache coherent, flush is free."

Loop:

```
for (size = 16, 64, 256, 1024, 4096; ...; size *= 4) {
    for (rep = 0; rep < 10000; ++rep) {
        rw = arena_alloc(... size ...);
        memcpy(rw, dummy_bytes, size);
        t0 = rdtsc_or_clock_gettime();
        rx = arena_seal(rw, size);          /* the part we time */
        t1 = ...;
        accumulate(size, t1 - t0);
        /* in Phase 0 we don't reuse the arena; new arena per iter */
    }
}
report median + p99 per size.
```

Output goes to stdout in a CSV format; `phase_0_arena.md` records the
numbers verbatim. If the p99 at size=1024 (representative of a 30-op
JIT'd program) exceeds **3 µs** on Graviton-class hardware we flag it
to the §2 compile budget for revision before Phase 1 starts.

The benchmark uses `clock_gettime(CLOCK_MONOTONIC_RAW)` for measurement;
no rdtsc on ARM64.

## 8. CMake integration

### 8.1 `storage/ndb/src/kernel/blocks/dbtup/jit/CMakeLists.txt`

```cmake
# RONDB-1056 Phase 0: arena substrate.
if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  message(STATUS "RONDB-1056: JIT excluded on macOS x86_64")
  return()
endif()

add_library(ndb_jit_arena STATIC
  jit_arena.c
)
target_include_directories(ndb_jit_arena PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(ndb_jit_arena PRIVATE c_std_11)
# Phase 0 has zero dependencies on the rest of NDB.
```

### 8.2 `storage/ndb/test/jit_proto/CMakeLists.txt`

```cmake
if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
  return()
endif()

add_executable(proto_smoke proto_smoke.c)
target_link_libraries(proto_smoke PRIVATE ndb_jit_arena)
add_test(NAME jit_proto_smoke COMMAND proto_smoke)

if(NOT APPLE)
  add_executable(proto_hardened proto_hardened.c)
  target_link_libraries(proto_hardened PRIVATE ndb_jit_arena)
  add_test(NAME jit_proto_hardened COMMAND proto_hardened)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
  add_executable(proto_icache_bench proto_icache_bench.c)
  target_link_libraries(proto_icache_bench PRIVATE ndb_jit_arena)
  # Not added to add_test — manual perf run, output captured to phase_0_arena.md.
endif()
```

### 8.3 Plumb into the parent `dbtup/CMakeLists.txt`

`add_subdirectory(jit)` guarded by the same macOS x86_64 check.

### 8.4 Plumb into top-level test discovery

`storage/ndb/test/CMakeLists.txt` (or wherever `jit_proto` will be a
sibling of `block_unit_test`): `add_subdirectory(jit_proto)`. Verify
`make test` picks up `jit_proto_smoke`.

### 8.5 macOS x86_64 link-check

A separate CTest entry that compiles a 5-line `void f(void) { (void)ndb_jit_arena_create; }`
and expects link failure on macOS x86_64, link success elsewhere. Realised
with `WILL_FAIL TRUE` and a per-platform conditional.

## 9. Hardened-kernel test variant (`proto_hardened.c`)

Linux only. Goals:

1. Run the same flow as `proto_smoke.c`.
2. Additionally assert that the legacy single-RWX path **would fail**
   on this host. This is what tells us we are actually exercising the
   dual-mapping value proposition.

Implementation:

```c
#include <sys/mman.h>
/* ... */

static int try_rwx_mmap(void) {
    void* p = mmap(NULL, 4096,
                   PROT_READ|PROT_WRITE|PROT_EXEC,
                   MAP_PRIVATE|MAP_ANON, -1, 0);
    if (p == MAP_FAILED) return 0;        /* hostile kernel: good */
    munmap(p, 4096);
    return 1;                              /* permissive kernel */
}

int main(void) {
    int rwx_works = try_rwx_mmap();
    /* run the smoke test path */
    int smoke = run_smoke();
    if (smoke != 0) return 2;

    /* The interesting bit: on a hostile kernel, this binary proves
       dual-mapping passes where RWX would not. On a permissive
       kernel, this binary still proves dual-mapping is correct, but
       does not exercise the hostile-config rejection path — we report
       that loudly so the operator can rerun under SELinux/gVisor. */
    if (rwx_works) {
        fprintf(stdout, "INFO proto_hardened ran on permissive kernel "
                        "(RWX accepted). Dual-mapping correctness OK; "
                        "hostile-kernel coverage requires re-run under "
                        "SELinux enforcing or gVisor.\n");
    } else {
        fprintf(stdout, "PASS proto_hardened on hostile kernel "
                        "(RWX rejected, dual-mapping accepted).\n");
    }
    return 0;
}
```

`phase_0_arena.md` records the manual recipe for running under SELinux
enforcing (`docker run --security-opt label=type:container_t ...` or
similar) and under gVisor (`runsc` runtime). At least one such run
must be performed before Phase 0 closes.

## 10. icache-flush measurement procedure

1. Build `proto_icache_bench` on each available ARM64 target:
   - Apple M-series laptop (developer convenience).
   - Graviton (representative Linux ARM64 production target). Run on
     a `c7g` or `r7g` instance.
   - Ampere if accessible (Oracle Cloud).
2. Run with the system otherwise idle, taskset to one core.
3. Capture stdout to `phase_0_arena.md` verbatim.
4. Compute median and p99 per size.
5. Compare p99-at-1024-bytes against the §2 compile budget (~0.5–1 µs).
   If exceeded, file an inline §2 amendment in this branch before
   declaring Phase 0 done.

## 11. `phase_0_arena.md` doc template

To be written at the very end of Phase 0.

```
# Phase 0 — arena foundation: results

## Outcome
- [ ] Linux x86_64 smoke: PASS / FAIL
- [ ] Linux ARM64 smoke: PASS / FAIL
- [ ] macOS Apple Silicon smoke: PASS / FAIL
- [ ] Linux dual-mapping verified by /proc/self/maps r-x assertion
- [ ] macOS post-seal write fault verified
- [ ] Hostile-kernel re-run: SELinux=PASS / gVisor=PASS (record which)
- [ ] macOS x86_64 link-check: build-out asserted

## icache-flush cost (ARM64)
| Size  | Median (ns) | p99 (ns) | Hardware |
|-------|-------------|----------|----------|
| 16    |             |          |          |
| 64    |             |          |          |
| 256   |             |          |          |
| 1024  |             |          |          |  <- compare to §2 budget
| 4096  |             |          |          |

## Compile budget (§2) — keep, revise, or escalate?
Decision: ___

## Platform quirks discovered
- ...

## Tmpfs fallback used? (Linux)
- Host kernel: ...
- memfd_create available: yes/no

## Outstanding questions for Phase 1
- ...
```

## 12. Step-by-step task breakdown

**Day 1, AM (~3 h):**
- Create `jit/` directory, write `jit_arena.h` (§2).
- Write `jit_arena.c` skeleton with stubs and dispatch, plus the
  internal struct.
- Write `jit_arena_linux.inc.c` create/destroy with `memfd_create` only
  (no tmpfs fallback yet).

**Day 1, PM (~4 h):**
- Add Linux alloc + seal (§4.2, §4.3).
- Write `proto_smoke.c` for x86_64 only.
- Wire CMake (§8.1, §8.2, §8.3, §8.4).
- Run on a Linux x86_64 dev box. Iterate until green.

**Day 2, AM (~4 h):**
- Add ARM64 instruction bytes to `proto_smoke.c`. Test on Linux ARM64
  (Graviton or local ARM container). Iterate until green.
- Add tmpfs fallback (§4.1) and exercise on a docker container without
  memfd (`--security-opt seccomp=...` to remove `memfd_create`).

**Day 2, PM (~3 h):**
- Write `jit_arena_macos.inc.c` (§5).
- Run `proto_smoke` on macOS Apple Silicon. Iterate until green.
- Add the §6.3 extra asserts.
- Add macOS x86_64 build-exclusion (§5.4) and the link-check (§8.5).

**Day 3, AM (~3 h):**
- Write `proto_hardened.c` (§9). Run on a default Linux dev box (will
  print INFO). Then run inside a docker container with `--security-opt
  no-new-privileges` and a hardened seccomp profile that blocks
  `mmap(... PROT_EXEC|PROT_WRITE ...)`. Confirm PASS path.
- If access available, run under gVisor `runsc`.

**Day 3, PM (~3 h):**
- Write `proto_icache_bench.c` (§7). Run on Graviton + Apple M.
- Fill in `phase_0_arena.md` (§11).
- Self-review against the verification checklist (§13).
- Open PR.

Total: ~2.5 working days of focused work. The 3-day slot in `plan.md`
accounts for platform-context-switch and SELinux/gVisor environment
debugging.

## 13. Verification checklist

Before declaring Phase 0 done:

- [ ] `proto_smoke` PASSes on Linux x86_64.
- [ ] `proto_smoke` PASSes on Linux ARM64.
- [ ] `proto_smoke` PASSes on macOS Apple Silicon.
- [ ] `proto_hardened` PASSes on at least one hostile-kernel host
      (record which: SELinux enforcing / gVisor / seccomp profile).
- [ ] `proto_icache_bench` numbers recorded for ≥ 1 ARM64 host.
- [ ] §2 compile-budget decision recorded in `phase_0_arena.md`
      (keep / revise / escalate).
- [ ] macOS x86_64 build excludes the `jit/` directory entirely
      (verified by attempting to link a TU referencing
      `ndb_jit_arena_create` and observing the link failure).
- [ ] No `PROT_READ|PROT_WRITE|PROT_EXEC` flag combination appears in
      `jit_arena_linux.inc.c` (grep assertion in CI).
- [ ] No new external dependencies introduced (only libc + libdl;
      no libelf, no binutils).
- [ ] `make test` picks up `jit_proto_smoke` and `jit_proto_hardened`.
- [ ] Code reviewed; commit message follows the `RONDB-1056:` prefix
      convention.

## 14. Out of scope for Phase 0 (explicit reminder)

Don't drift into these. They belong to later phases:

- Anything reading `stencils_src.c` or extracted bytes.
- Anything resembling a `jit.h` engine API.
- Type propagation, register state, opcodes, bytecode.
- DBTUP block wiring, signal handling, `JOIN_AGG_SETUP_REQ`.
- LRU eviction, multiple programs in one arena.
- Cross-thread `membarrier` / publication barriers (Phase 4).
- Stencil regen, clang dependency, extractor tool (Phase 2).
- `phase_0_arena.md` finalisation before code is green on all 3
  platforms.

## 15. Risks / things that may surprise us in Phase 0

1. **`memfd_create` rejected by older container runtimes.** Fallback
   to tmpfs handles this; the test itself should print which path it
   took so we know what to expect in CI.
2. **macOS `MAP_JIT` requires entitlements** in some sandboxed
   contexts. Standalone CTest binaries are not sandboxed, so we are
   fine in the smoke test, but worth noting.
3. **ARM64 `__builtin___clear_cache` cost on real hardware**
   exceeding the §2 budget. The bench in §7 is the canary; if it
   trips, we revise §2 in this phase rather than Phase 1.
4. **`/proc/self/maps` permission column varies** across kernels
   (some show `r-xp`, some `r-xs`). The check in §6.3.2 only asserts
   `r-x` substring + absence of `w` to be portable.
5. **macOS post-seal write fault** is the only way we exercise the
   `pthread_jit_write_protect_np(1)` toggle. If it doesn't fault,
   the toggle is broken on the host's macOS version — flag and
   investigate, do not paper over.
