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
 * RONDB-1056 Phase 0 — JIT arena, platform-dispatched implementation.
 *
 * The platform-specific bodies live in jit_arena_linux.inc.c and
 * jit_arena_macos.inc.c; this file pulls in the right one based on
 * preprocessor checks. The internal struct layout is shared.
 */

#if defined(__APPLE__) && defined(__x86_64__)
#error \
    "RONDB-1056 JIT is not built on macOS x86_64. " \
    "The CMake config should exclude this directory; " \
    "if you see this error the build system is misconfigured."
#endif

#include "jit_arena.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Internal struct shared across platform backends.                   */
/* ------------------------------------------------------------------ */

struct NdbJitArena {
  size_t size;            /* page-rounded mmap extent */
  size_t used;            /* bump-pointer offset */
  uint8_t *rw_base;       /* writable mapping start */
  uint8_t *rx_base;       /* executable mapping start (== rw_base on macOS) */
  int memfd;              /* Linux: backing fd, or -1 if unused */
  int using_tmpfs;        /* Linux: 1 if memfd_create unavailable */
#ifdef __APPLE__
  int jit_write_enabled;  /* tracks pthread_jit_write_protect_np state */
#endif
};

/* ------------------------------------------------------------------ */
/* Platform-independent helpers.                                      */
/* ------------------------------------------------------------------ */

static size_t round_up_to(size_t n, size_t mult) {
  return (n + mult - 1u) & ~(mult - 1u);
}

static int is_power_of_two(size_t n) {
  return n > 0 && (n & (n - 1u)) == 0;
}

static size_t arena_page_size(void) {
  long ps = sysconf(_SC_PAGESIZE);
  return (ps > 0) ? (size_t)ps : 4096u;
}

/* ------------------------------------------------------------------ */
/* Diagnostics — same on all platforms.                               */
/* ------------------------------------------------------------------ */

size_t ndb_jit_arena_size(const NdbJitArena *arena) {
  return arena ? arena->size : 0;
}

size_t ndb_jit_arena_used(const NdbJitArena *arena) {
  return arena ? arena->used : 0;
}

const void *ndb_jit_arena_exec_addr(const NdbJitArena *arena,
                                    const void *rw_ptr) {
  if (!arena || !rw_ptr) return NULL;
  const uint8_t *p = (const uint8_t *)rw_ptr;
  if (p < arena->rw_base || p >= arena->rw_base + arena->size) return NULL;
  return arena->rx_base + (size_t)(p - arena->rw_base);
}

void ndb_jit_arena_prepare_write(NdbJitArena *arena) {
  if (!arena) return;
#ifdef __APPLE__
  /* MAP_JIT write protection is per-thread; re-enable on the calling
   * thread (the one about to emit). Reuses the macOS backend helper so
   * the jit_write_enabled bookkeeping stays in one place. */
  extern void ndb_jit_arena_macos_enable_write(NdbJitArena *);
  ndb_jit_arena_macos_enable_write(arena);
#else
  /* Linux: the RW mapping is always writable; nothing to do. */
  (void)arena;
#endif
}

/* ------------------------------------------------------------------ */
/* Bump-pointer alloc — same on all platforms.                        */
/* ------------------------------------------------------------------ */

void *ndb_jit_arena_alloc(NdbJitArena *arena, size_t bytes, size_t align) {
  if (!arena || bytes == 0) return NULL;
  if (align <= 1) align = 1;
  else if (!is_power_of_two(align)) return NULL;

#ifdef __APPLE__
  /* On macOS the alloc surface must be writable.
   * pthread_jit_write_protect_np is PER-THREAD; the
   * arena->jit_write_enabled flag is global and only tracks whether
   * SOME thread has toggled the protect, not whether the calling
   * thread has. If alloc runs on a different thread from create
   * (Phase 4: DblqhProxy constructor on init thread, jit1_compile
   * on the receiver thread), the calling thread's protect is the
   * macOS default (ON) and writes through the unified MAP_JIT
   * mapping fault with SIGBUS. Always re-enable on the calling
   * thread; the helper is idempotent and cheap. */
  extern void ndb_jit_arena_macos_enable_write(NdbJitArena *);
  ndb_jit_arena_macos_enable_write(arena);
#endif

  size_t off = round_up_to(arena->used, align);
  if (off > arena->size) return NULL;          /* overflow guard */
  if (bytes > arena->size - off) return NULL;  /* OOM */
  arena->used = off + bytes;
  return arena->rw_base + off;
}

/* ------------------------------------------------------------------ */
/* Platform-specific bodies.                                          */
/* ------------------------------------------------------------------ */

#if defined(__linux__)
#include "jit_arena_linux.inc.c"
#elif defined(__APPLE__)
#include "jit_arena_macos.inc.c"
#else
#error "RONDB-1056 JIT arena: unsupported platform"
#endif
