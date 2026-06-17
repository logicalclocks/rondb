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
 * RONDB-1056 Phase 8 — compiled-program reuse cache.
 *
 * Identical bytecode compiles once and is shared. Two scans with the
 * same pushed WHERE filter, or a prepared statement re-EXECUTEd many
 * times, reuse one compiled blob instead of recompiling and re-spending
 * code memory. Reuse is sound because a compiled blob is a pure function
 * of its bytecode: stencils are static, cold-call helpers resolve by
 * global name, and runtime operands (e.g. OP_PARAM bind values) are read
 * per-row from a param buffer — never baked into the code — so two
 * EXECUTEs with different bind values share one blob.
 *
 * Structure (the design Mikael asked for):
 *   - A hash table keyed on the EXACT bytecode words (length + bytes;
 *     hash + memcmp, so a hash collision never causes false reuse).
 *   - Sharded into NJP_N_SHARDS independent sections, each a small
 *     bucket array protected by its own mutex (striped locking): a
 *     lookup/insert locks only one shard.
 *   - Entries are refcounted. acquire() bumps the count (compiling on
 *     miss); release() drops it and, at zero, evicts the entry and frees
 *     its compiled product. A `pinned` entry is retained at refcount 0
 *     for future reuse (the RonSQL PREPARE hint) and reclaimed only at
 *     cache teardown (or a future memory-pressure sweep).
 *
 * The cache is deliberately decoupled from the code-memory manager and
 * from NDB: the caller supplies a compile callback (run on miss, e.g.
 * bridge-translate + jit1_compile into a codemem slot) and a destroy
 * callback (run on evict, e.g. free the codemem slot). The cache only
 * owns the hash, the key copies, and the refcounts.
 *
 * Pure C11 + pthreads. No NDB / arena / jit1 dependency.
 */

#ifndef NDB_JIT_PROGCACHE_H
#define NDB_JIT_PROGCACHE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NdbJitProgCache NdbJitProgCache;

/* Opaque handle to one cached compiled program. Returned by acquire,
 * surrendered by release. Valid while its refcount > 0 (and, if pinned,
 * until cache teardown). */
typedef struct NjpEntry NjpEntry;

/* The caller's compiled product carried by the cache. `entry_fn` is the
 * callable the caller will run (opaque here); `user` is any extra
 * payload the caller needs at destroy time (e.g. a pointer to the
 * code-memory slot handle so it can be freed). */
typedef struct {
  void *entry_fn;
  void *user;
} NdbJitProgItem;

/* Compile callback — invoked on a cache miss, under the shard lock, to
 * produce *out for @p key[0..key_len). Return 0 on success, -1 to refuse
 * (acquire() then returns NULL and the caller falls back). */
typedef int (*NjpCompileFn)(void *cb_ctx, const uint8_t *key,
                            uint32_t key_len, NdbJitProgItem *out);

/* Destroy callback — invoked when an entry is evicted (refcount hit 0
 * and not pinned) or at cache teardown, to release the product's
 * resources (e.g. free the code-memory slot, free `user`). */
typedef void (*NjpDestroyFn)(void *cb_ctx, NdbJitProgItem *item);

/**
 * Create a cache using @p compile / @p destroy with the opaque
 * @p cb_ctx threaded into both. Returns NULL on allocation / mutex-init
 * failure.
 */
NdbJitProgCache *ndb_jit_progcache_create(NjpCompileFn compile,
                                          NjpDestroyFn destroy,
                                          void *cb_ctx);

/**
 * Destroy the cache: calls @p destroy for every remaining entry
 * (pinned or not), frees all bookkeeping. The caller must guarantee no
 * cached program is still executing. Safe with NULL.
 */
void ndb_jit_progcache_destroy(NdbJitProgCache *cache);

/**
 * Acquire the compiled program for @p key[0..key_len). On a hit, bumps
 * the refcount and returns the shared entry. On a miss, runs the
 * compile callback; on success inserts a new entry (refcount 1) and
 * returns it, on refuse returns NULL. If @p pinned is non-zero the entry
 * is marked pinned (sticky — survives refcount 0). Fills @p out_item
 * (entry_fn / user) when non-NULL.
 *
 * Returns the entry handle (pass to release), or NULL on compile-refuse
 * or allocation failure.
 */
NjpEntry *ndb_jit_progcache_acquire(NdbJitProgCache *cache,
                                    const uint8_t *key, uint32_t key_len,
                                    int pinned, NdbJitProgItem *out_item);

/**
 * Release a handle from acquire(). Drops the refcount; at zero, a
 * non-pinned entry is evicted (destroy callback runs) and @p handle
 * becomes invalid. Safe with NULL. Do not use @p handle after a release
 * that may have evicted it.
 */
void ndb_jit_progcache_release(NdbJitProgCache *cache, NjpEntry *handle);

/* ---- Diagnostics (feed NDBINFO counters in a later slice) -------- */

/* Number of entries currently in the cache (live + pinned-idle). */
unsigned ndb_jit_progcache_live_count(NdbJitProgCache *cache);

/* Total successful compiles (cache misses that produced an entry). */
uint64_t ndb_jit_progcache_compile_count(NdbJitProgCache *cache);

/* Total cache hits (reuses that avoided a compile). */
uint64_t ndb_jit_progcache_hit_count(NdbJitProgCache *cache);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_PROGCACHE_H */
