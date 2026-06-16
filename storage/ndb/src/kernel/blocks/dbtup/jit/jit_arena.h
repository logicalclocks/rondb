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
 * RONDB-1056 Phase 0 — JIT arena substrate.
 *
 * Pure C11. No NDB / kernel dependencies. The arena owns one or two
 * memory mappings used by the copy-and-patch JIT (Phase 1+) for
 * emitting native code:
 *
 *   - Linux: dual mapping over a memfd_create(2) (or tmpfs) backing
 *     fd. One mapping is PROT_READ|PROT_WRITE for emission, the other
 *     is PROT_READ|PROT_EXEC for execution. No PROT_WRITE|PROT_EXEC
 *     page is ever produced, so SELinux deny_execmem and similar
 *     hardened-kernel policies accept the arena.
 *   - macOS: single MAP_JIT mapping with per-thread write protection
 *     toggled via pthread_jit_write_protect_np(). Single address
 *     range; alloc returns the writable view, seal returns the
 *     executable view (same address).
 *   - macOS x86_64: not supported. The translation unit refuses to
 *     compile (the caller is expected to exclude the directory from
 *     the build via CMake; the #error is belt-and-suspenders).
 *
 * Phase 0 is single-threaded by construction. Cross-thread publication
 * barriers (membarrier SYNC_CORE etc.) arrive in Phase 4 alongside the
 * proxy-to-LDM sharing model.
 */

#ifndef NDB_JIT_ARENA_H
#define NDB_JIT_ARENA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NdbJitArena NdbJitArena;

/**
 * Create an arena of @p size bytes. The size is rounded up to a
 * multiple of the system page size. The arena starts empty
 * (used == 0).
 *
 * @return  pointer to the arena on success; NULL on failure
 *          (errno is set per mmap(2) / memfd_create(2)).
 */
NdbJitArena *ndb_jit_arena_create(size_t size);

/**
 * Tear down the arena. Safe to call with NULL (no-op).
 */
void ndb_jit_arena_destroy(NdbJitArena *arena);

/**
 * Reserve @p bytes of arena space at @p align byte alignment
 * (must be a power of two; values <= 1 are treated as 1).
 *
 * @return writable pointer into the arena, or NULL on OOM.
 *         The pointer is into the RW mapping on Linux, and into the
 *         (currently writable) MAP_JIT mapping on macOS.
 *         Phase 0 has no growth and no free; OOM is permanent for
 *         the lifetime of the arena.
 */
void *ndb_jit_arena_alloc(NdbJitArena *arena, size_t bytes, size_t align);

/**
 * Publish @p bytes bytes of code starting at @p rw_ptr.
 *
 *   - Linux: flushes the I-cache for the corresponding RX address
 *     range and returns the RX pointer (different address from
 *     rw_ptr, same backing pages).
 *   - macOS: flips pthread_jit_write_protect_np() to the protected
 *     state, flushes the I-cache, returns rw_ptr (single mapping).
 *
 * The caller must not write through @p rw_ptr again after seal.
 * Reading is always allowed.
 *
 * Phase 0 contract: seal is one-shot per allocation.
 *
 * @return callable pointer on success; NULL only if @p rw_ptr is
 *         not within this arena's RW range (caller bug).
 */
const void *ndb_jit_arena_seal(NdbJitArena *arena,
                               void *rw_ptr,
                               size_t bytes);

/**
 * Translate a writable pointer into the arena to its executable
 * alias, without flushing or sealing. Useful for emitter passes
 * that compute callable addresses for forward-branch fixup before
 * seal.
 *
 *   - Linux: returns rx_base + (rw_ptr - rw_base).
 *   - macOS: returns rw_ptr unchanged (unified mapping).
 *
 * Returns NULL if @p rw_ptr is not within this arena.
 */
const void *ndb_jit_arena_exec_addr(const NdbJitArena *arena,
                                    const void *rw_ptr);

/**
 * Re-enable writes to the arena's RW view on the *calling thread*.
 *
 *   - macOS: pthread_jit_write_protect_np(0) — MAP_JIT write protection
 *     is per-thread, so a thread that did not create/seal the arena
 *     (or that wrote through it before a seal flipped protection on)
 *     must call this before emitting. Idempotent and cheap.
 *   - Linux: no-op (the RW mapping is always writable).
 *
 * ndb_jit_arena_alloc already does this internally; this entry point
 * exists for a slot allocator that re-emits into a *recycled* slot
 * without going through alloc's bump pointer. Safe to call with NULL.
 */
void ndb_jit_arena_prepare_write(NdbJitArena *arena);

/**
 * Total size of the arena in bytes (page-rounded value passed
 * to ndb_jit_arena_create).
 */
size_t ndb_jit_arena_size(const NdbJitArena *arena);

/**
 * Bytes currently allocated (bump-pointer offset).
 */
size_t ndb_jit_arena_used(const NdbJitArena *arena);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_ARENA_H */
