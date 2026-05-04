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
 * RONDB-1056 Phase 0 — hardened-kernel variant smoke test.
 *
 * Linux only. Probes whether the host kernel rejects PROT_WRITE
 * combined with PROT_EXEC, then runs the same arena lifecycle as
 * proto_smoke. Two outcomes:
 *
 *   PASS  RWX rejected by kernel + dual-mapping accepted
 *         (ideal: we are running under SELinux enforcing /
 *         deny_execmem / gVisor / a hardened seccomp profile, and
 *         the dual-mapping path is what makes the JIT work here).
 *
 *   INFO  RWX accepted by kernel + dual-mapping accepted
 *         (correctness OK, but the host is permissive — re-run under
 *         a hostile sandbox to validate the deployment posture).
 *
 *   FAIL  Anything else.
 *
 * Exit codes:
 *   0 PASS or INFO
 *   1 dual-mapping smoke failure
 *   2 unexpected: arena succeeded but RX page is writable
 */

#include "jit_arena.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#if defined(__x86_64__)
static const uint8_t RETURN_42[] = {0xb8, 0x2a, 0x00, 0x00, 0x00, 0xc3};
#elif defined(__aarch64__)
static const uint8_t RETURN_42[] = {0x40, 0x05, 0x80, 0x52,
                                    0xc0, 0x03, 0x5f, 0xd6};
#else
#error "RONDB-1056 proto_hardened: unsupported arch"
#endif

typedef int (*fn_t)(void);

/* Try a one-page PROT_READ|PROT_WRITE|PROT_EXEC mapping; munmap and
 * return 1 on success, 0 if the kernel rejects it. This is the
 * "hostile kernel" probe — if RWX is rejected we know the dual-
 * mapping path is what's making the JIT viable on this host. */
static int try_rwx_mmap(void) {
  void *p = mmap(NULL, 4096,
                 PROT_READ | PROT_WRITE | PROT_EXEC,
                 MAP_PRIVATE | MAP_ANON, -1, 0);
  if (p == MAP_FAILED) return 0;
  munmap(p, 4096);
  return 1;
}

/* Verify via /proc/self/maps that `addr` falls in a page whose
 * permissions are r-x (no w). Returns 1 on success. */
static int rx_page_is_not_writable(const void *addr) {
  FILE *f = fopen("/proc/self/maps", "r");
  if (!f) return 0;
  uintptr_t target = (uintptr_t)addr;
  char line[512];
  int ok = 0;
  while (fgets(line, sizeof(line), f)) {
    uintptr_t lo = 0, hi = 0;
    char perms[8] = {0};
    if (sscanf(line, "%" SCNxPTR "-%" SCNxPTR " %7s",
               &lo, &hi, perms) != 3) {
      continue;
    }
    if (target < lo || target >= hi) continue;
    int has_w = (strchr(perms, 'w') != NULL);
    int has_x = (strchr(perms, 'x') != NULL);
    ok = has_x && !has_w;
    break;
  }
  fclose(f);
  return ok;
}

static int run_smoke(void) {
  NdbJitArena *arena = ndb_jit_arena_create(4096);
  if (!arena) return 0;
  uint8_t *rw = (uint8_t *)ndb_jit_arena_alloc(arena, sizeof(RETURN_42), 16);
  if (!rw) { ndb_jit_arena_destroy(arena); return 0; }
  memcpy(rw, RETURN_42, sizeof(RETURN_42));
  const void *rx = ndb_jit_arena_seal(arena, rw, sizeof(RETURN_42));
  if (!rx) { ndb_jit_arena_destroy(arena); return 0; }
  if (!rx_page_is_not_writable(rx)) {
    fprintf(stderr,
            "FAIL proto_hardened: RX page is writable — dual-mapping "
            "didn't actually flip the permissions\n");
    ndb_jit_arena_destroy(arena);
    return -1;
  }
  fn_t fn = (fn_t)(uintptr_t)rx;
  int ok = (fn() == 42);
  ndb_jit_arena_destroy(arena);
  return ok ? 1 : 0;
}

int main(void) {
  int rwx_works = try_rwx_mmap();
  int smoke = run_smoke();
  if (smoke == 0) {
    fprintf(stderr, "FAIL proto_hardened: dual-mapping smoke failed\n");
    return 1;
  }
  if (smoke < 0) return 2;

  if (rwx_works) {
    printf("INFO proto_hardened: ran on permissive kernel "
           "(RWX accepted). Dual-mapping correctness OK; "
           "hostile-kernel coverage requires re-run under SELinux "
           "enforcing / gVisor / hardened seccomp.\n");
  } else {
    printf("PASS proto_hardened: hostile kernel "
           "(RWX rejected, dual-mapping accepted).\n");
  }
  return 0;
}
