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
 *     miss); release() drops it. At zero a `pinned` entry (the RonSQL
 *     PREPARE hint) is retained until cache teardown; an unpinned entry
 *     is RETAINED on the shard's idle LRU list up to an idle budget
 *     (ndb_jit_progcache_set_idle_limit, default NJP_IDLE_LIMIT_DEFAULT
 *     entries per cache, spread over the shards), the least recently
 *     released being evicted first. Without this (the Phase 8 design,
 *     evict at refcount 0) every one-shot program recompiled on each
 *     use: a pushed-join child scan is one SCAN_FRAGREQ per parent row
 *     per fragment, so tpch_q13 compiled ~50k identical programs per
 *     request (3.5% of the request) with only the concurrently live
 *     fragment instances hitting. Retention turns that into one compile
 *     per distinct program. Keys are exact bytecode, so a retained
 *     entry can never be stale.
 *   - Code-memory pressure: a compile callback that fails for lack of
 *     code memory returns NJP_COMPILE_NOMEM; acquire() then evicts every
 *     idle entry (all shards, outside its own shard lock) and retries
 *     the compile once, so retained programs never starve a live one.
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
 * produce *out for @p key[0..key_len). Return NJP_COMPILE_OK on success,
 * NJP_COMPILE_REFUSE to refuse permanently (acquire() returns NULL and
 * the caller falls back), or NJP_COMPILE_NOMEM when the only problem is
 * code memory (acquire() sweeps the idle entries and retries once). The
 * callback must not call back into the cache (its shard lock is held). */
#define NJP_COMPILE_OK      0
#define NJP_COMPILE_REFUSE (-1)
#define NJP_COMPILE_NOMEM  (-2)
typedef int (*NjpCompileFn)(void *cb_ctx, const uint8_t *key,
                            uint32_t key_len, NdbJitProgItem *out);

/* Default idle budget per cache: unpinned entries retained at refcount 0
 * (spread over the shards). Compiled blobs are at most
 * NDB_JIT_CODEMEM_MAX_BLOB (8 KB) and typically a few hundred bytes, so
 * 1024 retained blobs stay well inside the 16 MB default code memory;
 * the NOMEM sweep covers the rest. 0 restores evict-at-refcount-0. */
#define NJP_IDLE_LIMIT_DEFAULT 1024u

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
 * non-pinned entry moves to the idle LRU (or is evicted at once when the
 * idle budget is 0 / exceeded — destroy callback runs). @p handle must
 * not be used after release: it may be evicted at any later time.
 * Safe with NULL.
 */
void ndb_jit_progcache_release(NdbJitProgCache *cache, NjpEntry *handle);

/**
 * Like acquire(), and additionally reports why NULL came back in
 * @p out_rc (when non-NULL): NJP_COMPILE_REFUSE (callback refused, or
 * bookkeeping allocation failed) or NJP_COMPILE_NOMEM (code memory still
 * full after the idle sweep + retry). 0 on success.
 */
NjpEntry *ndb_jit_progcache_acquire_ex(NdbJitProgCache *cache,
                                       const uint8_t *key, uint32_t key_len,
                                       int pinned, NdbJitProgItem *out_item,
                                       int *out_rc);

/**
 * Set the idle budget: at most @p max_idle unpinned refcount-0 entries
 * are retained per cache (split evenly over the shards, at least one
 * per shard unless @p max_idle is 0). Trims immediately. Thread-safe.
 */
void ndb_jit_progcache_set_idle_limit(NdbJitProgCache *cache,
                                      unsigned max_idle);

/**
 * Evict idle (unpinned, refcount 0) entries until at most @p keep remain
 * per cache (spread over the shards; 0 = evict all idle). Returns the
 * number evicted. Never touches live or pinned entries. Thread-safe;
 * must not be called from a compile callback.
 */
unsigned ndb_jit_progcache_evict_idle(NdbJitProgCache *cache, unsigned keep);

/* ---- Diagnostics (feed NDBINFO counters in a later slice) -------- */

/* Number of entries currently in the cache (live + idle + pinned-idle). */
unsigned ndb_jit_progcache_live_count(NdbJitProgCache *cache);
/* Number of unpinned refcount-0 entries currently retained. */
unsigned ndb_jit_progcache_idle_count(NdbJitProgCache *cache);

/* Total successful compiles (cache misses that produced an entry). */
uint64_t ndb_jit_progcache_compile_count(NdbJitProgCache *cache);

/* Total cache hits (reuses that avoided a compile). */
uint64_t ndb_jit_progcache_hit_count(NdbJitProgCache *cache);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_PROGCACHE_H */
