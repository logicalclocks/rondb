/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license terms as jit_arena.c.)
 */

/*
 * RONDB-1056 Phase 0 — Linux backend for the JIT arena.
 *
 * Strategy: dual mapping over a memfd_create(2) backing fd.
 *   1. memfd_create -> fd (anonymous, in kernel memory)
 *   2. ftruncate fd to the arena size
 *   3. mmap fd PROT_READ|PROT_WRITE -> rw_base
 *   4. mmap fd PROT_READ|PROT_EXEC  -> rx_base
 *
 * Because both mappings refer to the same physical pages, writes
 * through rw_base become visible at rx_base after the necessary
 * I-cache invalidation (issued by ndb_jit_arena_seal). No page is
 * ever simultaneously writable and executable, so SELinux
 * deny_execmem and similar policies accept the arena.
 *
 * Fallback: on systems where memfd_create(2) is unavailable
 * (errno == ENOSYS at runtime, or no glibc wrapper at compile time),
 * we fall back to a tmpfs file under /dev/shm. The two-mapping dance
 * is identical from there.
 *
 * This file is compiled only by being #included from jit_arena.c on
 * Linux; it is not a standalone translation unit.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

/* glibc 2.27+ exposes memfd_create. Older systems need the syscall.
 * We always go via syscall() to keep the build portable across the
 * Linux distros RonDB ships on. */
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif

static int linux_memfd_create(const char *name, unsigned int flags) {
#ifdef SYS_memfd_create
  return (int)syscall(SYS_memfd_create, name, flags);
#else
  (void)name;
  (void)flags;
  errno = ENOSYS;
  return -1;
#endif
}

static int open_tmpfs_backing(size_t size) {
  /* /dev/shm is tmpfs on every Linux RonDB target; /tmp may be
   * noexec on some hardened distros so we don't fall through there.
   * mkstemp + immediate unlink keeps the file unnamed in the FS. */
  char path[] = "/dev/shm/ndb_jit_XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0) return -1;
  if (unlink(path) != 0) {
    int saved = errno;
    close(fd);
    errno = saved;
    return -1;
  }
  if (ftruncate(fd, (off_t)size) != 0) {
    int saved = errno;
    close(fd);
    errno = saved;
    return -1;
  }
  return fd;
}

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
  arena->memfd = -1;
  arena->using_tmpfs = 0;

  /* Try memfd_create first. */
  int fd = linux_memfd_create("ndb_jit", MFD_CLOEXEC);
  if (fd < 0) {
    /* memfd unavailable -> tmpfs fallback. */
    fd = open_tmpfs_backing(size);
    if (fd < 0) {
      int saved = errno;
      free(arena);
      errno = saved;
      return NULL;
    }
    arena->using_tmpfs = 1;
  } else {
    if (ftruncate(fd, (off_t)size) != 0) {
      int saved = errno;
      close(fd);
      free(arena);
      errno = saved;
      return NULL;
    }
  }
  arena->memfd = fd;

  /* RW mapping for emission. */
  void *rw = mmap(NULL, size, PROT_READ | PROT_WRITE,
                  MAP_SHARED, fd, 0);
  if (rw == MAP_FAILED) {
    int saved = errno;
    close(fd);
    free(arena);
    errno = saved;
    return NULL;
  }

  /* RX mapping for execution. Same fd, same offset, same physical
   * pages — different virtual address with different protection. */
  void *rx = mmap(NULL, size, PROT_READ | PROT_EXEC,
                  MAP_SHARED, fd, 0);
  if (rx == MAP_FAILED) {
    int saved = errno;
    munmap(rw, size);
    close(fd);
    free(arena);
    errno = saved;
    return NULL;
  }

  arena->rw_base = (uint8_t *)rw;
  arena->rx_base = (uint8_t *)rx;
  return arena;
}

void ndb_jit_arena_destroy(NdbJitArena *arena) {
  if (!arena) return;
  if (arena->rw_base) munmap(arena->rw_base, arena->size);
  if (arena->rx_base) munmap(arena->rx_base, arena->size);
  if (arena->memfd >= 0) close(arena->memfd);
  free(arena);
}

const void *ndb_jit_arena_seal(NdbJitArena *arena,
                               void *rw_ptr,
                               size_t bytes) {
  if (!arena || !rw_ptr || bytes == 0) return NULL;
  const uint8_t *p = (const uint8_t *)rw_ptr;
  if (p < arena->rw_base || p >= arena->rw_base + arena->size) return NULL;
  size_t off = (size_t)(p - arena->rw_base);
  if (bytes > arena->size - off) return NULL;

  /* Flush the I-cache for the RX alias of [rw_ptr, rw_ptr+bytes).
   *
   * On x86_64 __builtin___clear_cache is a no-op (the I and D caches
   * are coherent through the memory hierarchy).
   *
   * On aarch64 it emits the IC IVAU / DSB ISH / ISB sequence over the
   * range. Single-thread Phase 0 does not need a cross-core barrier
   * — the kernel return-from-syscall path issues the necessary ISB
   * on the executing thread before we run the new bytes. Cross-thread
   * publication (membarrier SYNC_CORE) lands in Phase 4 alongside the
   * proxy-to-LDM sharing model. */
  uint8_t *rx_lo = arena->rx_base + off;
  uint8_t *rx_hi = rx_lo + bytes;
  __builtin___clear_cache((char *)rx_lo, (char *)rx_hi);

  return rx_lo;
}
