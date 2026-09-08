/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.

 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * RONDB-1056 Phase 0 — JIT arena smoke test.
 *
 * Standalone C program. Allocates an arena, copies a hand-coded
 * "return 42" function into it, seals, calls it, asserts the result.
 * Plus the four extra checks documented in
 * claude_files/compiled_interpreter/phase_0_implementation.md §6.3:
 *   1. RW != RX (Linux only — verifies dual-mapping is actually in use)
 *   2. /proc/self/maps shows r-x without w on the RX page (Linux only)
 *   3. Re-execute 1000x to catch transient I-cache coherency bugs
 *   4. Post-seal write through rw_ptr on macOS faults; via fork()/SIGSEGV
 *
 * Exit codes:
 *   0  success
 *   1  return value mismatch
 *   2  arena create failed
 *   3  alloc failed
 *   4  seal failed
 *   5  RW == RX (dual-mapping not actually in use on Linux)
 *   6  /proc/self/maps check failed (Linux)
 *   7  re-execute mismatch
 *   8  post-seal write didn't fault on macOS
 */

#include "jit_arena.h"

#include <assert.h>
#include <inttypes.h>
#include <setjmp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Per-arch instruction bytes for "return 42".                        */
/*                                                                    */
/* These were assembled by hand and verified against `objdump -d` of  */
/* a trivial `int f(void) { return 42; }` compiled with -O0. The      */
/* exact disassembly is recorded in the comment above each blob so a  */
/* future reader can re-verify without re-running objdump.            */
/* ------------------------------------------------------------------ */

#if defined(__x86_64__)
/* objdump -d (Intel syntax):
 *     b8 2a 00 00 00     mov    eax,0x2a
 *     c3                 ret
 */
static const uint8_t RETURN_42[] = {0xb8, 0x2a, 0x00, 0x00, 0x00, 0xc3};
#define ARCH_NAME "x86_64"

#elif defined(__aarch64__)
/* objdump -d (little-endian instruction halfwords):
 *     52800540    mov w0, #0x2a
 *     d65f03c0    ret
 *
 * Encoded as bytes (little-endian per 32-bit instruction):
 *     0x40 0x05 0x80 0x52      -> mov w0, #0x2a
 *     0xc0 0x03 0x5f 0xd6      -> ret
 */
static const uint8_t RETURN_42[] = {0x40, 0x05, 0x80, 0x52,
                                    0xc0, 0x03, 0x5f, 0xd6};
#define ARCH_NAME "aarch64"

#else
#error "RONDB-1056 proto_smoke: unsupported test arch"
#endif

typedef int (*fn_t)(void);

/* ------------------------------------------------------------------ */
/* Linux-specific dual-mapping verification.                          */
/* ------------------------------------------------------------------ */

#if defined(__linux__)
/* Read /proc/self/maps and confirm the line covering `addr` shows
 * `r-x` permissions (no `w`). This is the runtime evidence that the
 * RX mapping really is non-writable, regardless of the host kernel's
 * SELinux / seccomp / sandbox configuration.
 *
 * Returns 1 on success, 0 if not found, -1 on error.
 */
static int check_rx_perms(const void *addr) {
  FILE *f = fopen("/proc/self/maps", "r");
  if (!f) return -1;

  uintptr_t target = (uintptr_t)addr;
  char line[512];
  int result = 0;

  while (fgets(line, sizeof(line), f)) {
    uintptr_t lo = 0, hi = 0;
    char perms[8] = {0};
    if (sscanf(line, "%" SCNxPTR "-%" SCNxPTR " %7s",
               &lo, &hi, perms) != 3) {
      continue;
    }
    if (target < lo || target >= hi) continue;

    /* perms is e.g. "r-xp" or "rwxp". We want r and x but not w. */
    int has_r = (strchr(perms, 'r') != NULL);
    int has_w = (strchr(perms, 'w') != NULL);
    int has_x = (strchr(perms, 'x') != NULL);
    if (has_r && has_x && !has_w) {
      result = 1;
    } else {
      fprintf(stderr, "FAIL RX page perms = '%s' (want r-x, got w too)\n",
              perms);
      result = 0;
    }
    break;
  }

  fclose(f);
  return result;
}
#endif

/* ------------------------------------------------------------------ */
/* macOS-specific post-seal write-fault check.                        */
/*                                                                    */
/* On macOS the unified MAP_JIT mapping should fault on write after   */
/* pthread_jit_write_protect_np(1). We exercise this in a child       */
/* process so SIGSEGV doesn't kill the test binary.                   */
/* ------------------------------------------------------------------ */

#if defined(__APPLE__)
/* In-process write-fault probe using sigsetjmp/siglongjmp.
 *
 * Why not fork: a forked child segfaulting (the expected outcome)
 * causes macOS to spawn a crash-report dialog and write a .crash
 * file under ~/Library/Logs/DiagnosticReports/, which is noise we
 * do not want from a passing test. Catching SIGSEGV/SIGBUS in the
 * same process and longjmp-ing out keeps the test silent on success.
 *
 * The handlers are installed only for the duration of the probe;
 * they are restored to the prior disposition on exit. */

static sigjmp_buf g_post_seal_jmpbuf;
static volatile sig_atomic_t g_caught_signo = 0;

__attribute__((noreturn))
static void post_seal_handler(int signo) {
  g_caught_signo = signo;
  siglongjmp(g_post_seal_jmpbuf, 1);
}

static int post_seal_write_faults(void *post_seal_ptr) {
  struct sigaction old_segv, old_bus;
  struct sigaction new_act;
  memset(&new_act, 0, sizeof(new_act));
  new_act.sa_handler = post_seal_handler;
  /* No SA_RESTART; we want the handler to interrupt and longjmp. */
  sigemptyset(&new_act.sa_mask);
  if (sigaction(SIGSEGV, &new_act, &old_segv) != 0) {
    perror("sigaction SIGSEGV");
    return 0;
  }
  if (sigaction(SIGBUS, &new_act, &old_bus) != 0) {
    perror("sigaction SIGBUS");
    sigaction(SIGSEGV, &old_segv, NULL);
    return 0;
  }

  int faulted = 0;
  g_caught_signo = 0;
  if (sigsetjmp(g_post_seal_jmpbuf, 1) == 0) {
    /* First pass: attempt the write. Should fault, handler longjmps
     * us back here with the saved signal number in g_caught_signo. */
    volatile uint8_t *p = (volatile uint8_t *)post_seal_ptr;
    *p = 0xCC;
    /* If we reach here without faulting, the test fails. */
    fprintf(stderr,
            "FAIL post-seal write succeeded — "
            "MAP_JIT write protection didn't engage\n");
    faulted = 0;
  } else {
    /* Second pass: arrived via siglongjmp. */
    if (g_caught_signo == SIGSEGV || g_caught_signo == SIGBUS) {
      faulted = 1;
    } else {
      fprintf(stderr,
              "FAIL post-seal caught unexpected signal %d\n",
              (int)g_caught_signo);
      faulted = 0;
    }
  }

  sigaction(SIGSEGV, &old_segv, NULL);
  sigaction(SIGBUS, &old_bus, NULL);
  return faulted;
}
#endif

/* ------------------------------------------------------------------ */
/* Test body.                                                         */
/* ------------------------------------------------------------------ */

int main(void) {
  NdbJitArena *arena = ndb_jit_arena_create(4096);
  if (!arena) {
    perror("ndb_jit_arena_create");
    return 2;
  }

  uint8_t *rw = (uint8_t *)ndb_jit_arena_alloc(arena, sizeof(RETURN_42), 16);
  if (!rw) {
    fprintf(stderr, "FAIL ndb_jit_arena_alloc returned NULL\n");
    ndb_jit_arena_destroy(arena);
    return 3;
  }

  memcpy(rw, RETURN_42, sizeof(RETURN_42));

  const void *rx_void = ndb_jit_arena_seal(arena, rw, sizeof(RETURN_42));
  if (!rx_void) {
    fprintf(stderr, "FAIL ndb_jit_arena_seal returned NULL\n");
    ndb_jit_arena_destroy(arena);
    return 4;
  }

  /* Cast through uintptr_t to satisfy strict aliasing / pedantic
   * function-pointer-from-void-pointer warnings on some toolchains. */
  fn_t fn = (fn_t)(uintptr_t)rx_void;

  /* (a) Basic call. */
  int got = fn();
  if (got != 42) {
    fprintf(stderr, "FAIL got=%d want=42\n", got);
    ndb_jit_arena_destroy(arena);
    return 1;
  }

  /* (b) RW != RX — Linux only. On macOS the unified MAP_JIT mapping
   * shares a single address, so this assertion would falsely fail. */
#if defined(__linux__)
  if ((const void *)rw == rx_void) {
    fprintf(stderr,
            "FAIL rw == rx on Linux — dual-mapping not actually in use "
            "(rw=%p rx=%p)\n",
            (void *)rw, rx_void);
    ndb_jit_arena_destroy(arena);
    return 5;
  }
#endif

  /* (c) /proc/self/maps shows r-x without w — Linux only. */
#if defined(__linux__)
  int rx_ok = check_rx_perms(rx_void);
  if (rx_ok != 1) {
    fprintf(stderr, "FAIL /proc/self/maps RX-perms check rc=%d\n", rx_ok);
    ndb_jit_arena_destroy(arena);
    return 6;
  }
#endif

  /* (d) 1000 re-calls — catch transient I-cache coherency bugs. */
  for (int i = 0; i < 1000; ++i) {
    int v = fn();
    if (v != 42) {
      fprintf(stderr, "FAIL re-call iter=%d got=%d\n", i, v);
      ndb_jit_arena_destroy(arena);
      return 7;
    }
  }

  /* (e) Post-seal write fault — macOS only. */
#if defined(__APPLE__)
  if (!post_seal_write_faults(rw)) {
    ndb_jit_arena_destroy(arena);
    return 8;
  }
#endif

  printf("PASS proto_smoke arch=%s rw=%p rx=%p size=%zu used=%zu\n",
         ARCH_NAME, (void *)rw, rx_void,
         ndb_jit_arena_size(arena),
         ndb_jit_arena_used(arena));

  ndb_jit_arena_destroy(arena);
  return 0;
}
