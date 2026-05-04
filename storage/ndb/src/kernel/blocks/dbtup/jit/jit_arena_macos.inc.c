/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license terms as jit_arena.c.)
 */

/*
 * RONDB-1056 Phase 0 — macOS backend for the JIT arena.
 *
 * Strategy: single MAP_JIT mapping with per-thread write protection
 * toggled via pthread_jit_write_protect_np(). This is the Apple-
 * recommended path on Apple Silicon. Unified mapping means the RW
 * and RX views of the arena are at the same address.
 *
 * Lifecycle:
 *   create -> mmap MAP_JIT, pthread_jit_write_protect_np(0), arena
 *             starts in writable state.
 *   alloc  -> if currently protected (post-seal), toggle back to
 *             writable. Bump pointer.
 *   seal   -> sys_icache_invalidate over the range, toggle to
 *             protected. Same address returned (single mapping).
 *
 * macOS x86_64 is excluded by the #error in jit_arena.c.
 *
 * This file is compiled only by being #included from jit_arena.c on
 * macOS; it is not a standalone translation unit.
 */

#include <errno.h>
#include <libkern/OSCacheControl.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/mman.h>

NdbJitArena *ndb_jit_arena_create(size_t size) {
  if (size == 0) {
    errno = EINVAL;
    return NULL;
  }
  size_t page = arena_page_size();
  size = round_up_to(size, page);

  NdbJitArena *arena = (NdbJitArena *)calloc(1, sizeof(*arena));
  if (!arena) {
    errno = ENOMEM;
    return NULL;
  }
  arena->size = size;
  arena->memfd = -1;          /* unused on macOS */
  arena->using_tmpfs = 0;

  void *p = mmap(NULL, size,
                 PROT_READ | PROT_WRITE | PROT_EXEC,
                 MAP_PRIVATE | MAP_ANON | MAP_JIT,
                 -1, 0);
  if (p == MAP_FAILED) {
    int saved = errno;
    free(arena);
    errno = saved;
    return NULL;
  }

  arena->rw_base = (uint8_t *)p;
  arena->rx_base = (uint8_t *)p;          /* unified mapping */

  /* MAP_JIT pages start writable for the calling thread. We mirror
   * the toggle state explicitly so alloc/seal stay in sync. */
  pthread_jit_write_protect_np(0);
  arena->jit_write_enabled = 1;

  return arena;
}

void ndb_jit_arena_destroy(NdbJitArena *arena) {
  if (!arena) return;
  /* Make sure subsequent code on this thread isn't left in
   * jit-write-disabled state when the arena goes away. */
  if (!arena->jit_write_enabled) {
    pthread_jit_write_protect_np(0);
  }
  if (arena->rw_base) munmap(arena->rw_base, arena->size);
  free(arena);
}

/* Called from ndb_jit_arena_alloc in jit_arena.c when the arena was
 * left in jit-write-protected state by the previous seal. */
void ndb_jit_arena_macos_enable_write(NdbJitArena *arena) {
  pthread_jit_write_protect_np(0);
  arena->jit_write_enabled = 1;
}

const void *ndb_jit_arena_seal(NdbJitArena *arena,
                               void *rw_ptr,
                               size_t bytes) {
  if (!arena || !rw_ptr || bytes == 0) return NULL;
  const uint8_t *p = (const uint8_t *)rw_ptr;
  if (p < arena->rw_base || p >= arena->rw_base + arena->size) return NULL;
  size_t off = (size_t)(p - arena->rw_base);
  if (bytes > arena->size - off) return NULL;

  uint8_t *rx = arena->rx_base + off;        /* same as rw on macOS */

  /* I-cache invalidation. Apple's sys_icache_invalidate is the
   * canonical surface on Darwin and is preferred over
   * __builtin___clear_cache here. */
  sys_icache_invalidate(rx, bytes);

  /* Flip the per-thread JIT write protection so subsequent attempts
   * to write through the unified mapping fault. The address remains
   * executable. */
  pthread_jit_write_protect_np(1);
  arena->jit_write_enabled = 0;

  return rx;
}
