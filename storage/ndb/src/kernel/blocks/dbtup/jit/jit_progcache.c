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
 * RONDB-1056 Phase 8 — compiled-program reuse cache (impl).
 * See jit_progcache.h for the design rationale.
 */

#include "jit_progcache.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Layout.                                                            */
/* ------------------------------------------------------------------ */

#define NJP_SHARD_BITS 4u
#define NJP_N_SHARDS   (1u << NJP_SHARD_BITS)   /* 16 striped sections */
#define NJP_N_BUCKETS  64u                      /* buckets per shard   */

struct NjpEntry {
  struct NjpEntry *next;     /* bucket chain */
  uint64_t         hash;     /* full key hash (shard + bucket derive from it) */
  uint8_t         *key;      /* copy of the bytecode bytes */
  uint32_t         key_len;
  uint32_t         refcount;
  uint8_t          pinned;   /* retained at refcount 0 for reuse */
  NdbJitProgItem   item;     /* caller's compiled product */
};

typedef struct {
  pthread_mutex_t mtx;
  NjpEntry       *buckets[NJP_N_BUCKETS];
  uint32_t        live;      /* entries in this shard */
  uint64_t        compiles;  /* successful misses */
  uint64_t        hits;      /* reuses */
} NjpShard;

struct NdbJitProgCache {
  NjpShard     shards[NJP_N_SHARDS];
  NjpCompileFn compile;
  NjpDestroyFn destroy;
  void        *cb_ctx;
};

/* ------------------------------------------------------------------ */
/* Hashing — FNV-1a 64. Shard = low bits, bucket = next bits.         */
/* ------------------------------------------------------------------ */

static uint64_t njp_hash(const uint8_t *p, uint32_t n) {
  uint64_t h = 1469598103934665603ull;
  for (uint32_t i = 0; i < n; ++i) {
    h ^= (uint64_t)p[i];
    h *= 1099511628211ull;
  }
  return h;
}

static NjpShard *shard_of(NdbJitProgCache *cache, uint64_t hash) {
  return &cache->shards[hash & (NJP_N_SHARDS - 1u)];
}

static uint32_t bucket_of(uint64_t hash) {
  return (uint32_t)(hash >> NJP_SHARD_BITS) & (NJP_N_BUCKETS - 1u);
}

/* ------------------------------------------------------------------ */
/* Lifecycle.                                                         */
/* ------------------------------------------------------------------ */

NdbJitProgCache *ndb_jit_progcache_create(NjpCompileFn compile,
                                          NjpDestroyFn destroy,
                                          void *cb_ctx) {
  if (compile == NULL || destroy == NULL) return NULL;
  NdbJitProgCache *cache = (NdbJitProgCache *)calloc(1, sizeof(*cache));
  if (cache == NULL) return NULL;
  cache->compile = compile;
  cache->destroy = destroy;
  cache->cb_ctx = cb_ctx;
  for (unsigned i = 0; i < NJP_N_SHARDS; ++i) {
    if (pthread_mutex_init(&cache->shards[i].mtx, NULL) != 0) {
      for (unsigned j = 0; j < i; ++j) pthread_mutex_destroy(&cache->shards[j].mtx);
      free(cache);
      return NULL;
    }
  }
  return cache;
}

void ndb_jit_progcache_destroy(NdbJitProgCache *cache) {
  if (cache == NULL) return;
  for (unsigned i = 0; i < NJP_N_SHARDS; ++i) {
    NjpShard *sh = &cache->shards[i];
    for (unsigned b = 0; b < NJP_N_BUCKETS; ++b) {
      NjpEntry *e = sh->buckets[b];
      while (e != NULL) {
        NjpEntry *next = e->next;
        cache->destroy(cache->cb_ctx, &e->item);
        free(e->key);
        free(e);
        e = next;
      }
      sh->buckets[b] = NULL;
    }
    pthread_mutex_destroy(&sh->mtx);
  }
  free(cache);
}

/* ------------------------------------------------------------------ */
/* Acquire / release.                                                 */
/* ------------------------------------------------------------------ */

NjpEntry *ndb_jit_progcache_acquire(NdbJitProgCache *cache,
                                    const uint8_t *key, uint32_t key_len,
                                    int pinned, NdbJitProgItem *out_item) {
  if (cache == NULL || key == NULL || key_len == 0) return NULL;
  uint64_t hash = njp_hash(key, key_len);
  NjpShard *sh = shard_of(cache, hash);
  uint32_t b = bucket_of(hash);

  pthread_mutex_lock(&sh->mtx);

  for (NjpEntry *e = sh->buckets[b]; e != NULL; e = e->next) {
    if (e->hash == hash && e->key_len == key_len &&
        memcmp(e->key, key, key_len) == 0) {
      e->refcount++;
      if (pinned) e->pinned = 1;
      sh->hits++;
      if (out_item != NULL) *out_item = e->item;
      pthread_mutex_unlock(&sh->mtx);
      return e;
    }
  }

  /* Miss: compile under the shard lock so two concurrent acquirers of
   * the same program can't both compile + insert. Compile is ~µs and
   * off the per-row path; it only blocks this 1/NJP_N_SHARDS section. */
  NdbJitProgItem item;
  memset(&item, 0, sizeof(item));
  if (cache->compile(cache->cb_ctx, key, key_len, &item) != 0) {
    pthread_mutex_unlock(&sh->mtx);
    return NULL;   /* compile refused -> caller falls back */
  }

  NjpEntry *e = (NjpEntry *)malloc(sizeof(*e));
  uint8_t *key_copy = (uint8_t *)malloc(key_len);
  if (e == NULL || key_copy == NULL) {
    free(e);
    free(key_copy);
    cache->destroy(cache->cb_ctx, &item);   /* don't leak the just-compiled product */
    pthread_mutex_unlock(&sh->mtx);
    return NULL;
  }
  memcpy(key_copy, key, key_len);
  e->next = sh->buckets[b];
  e->hash = hash;
  e->key = key_copy;
  e->key_len = key_len;
  e->refcount = 1;
  e->pinned = pinned ? 1 : 0;
  e->item = item;
  sh->buckets[b] = e;
  sh->live++;
  sh->compiles++;
  if (out_item != NULL) *out_item = item;
  pthread_mutex_unlock(&sh->mtx);
  return e;
}

void ndb_jit_progcache_release(NdbJitProgCache *cache, NjpEntry *handle) {
  if (cache == NULL || handle == NULL) return;
  NjpShard *sh = shard_of(cache, handle->hash);
  uint32_t b = bucket_of(handle->hash);

  pthread_mutex_lock(&sh->mtx);
  if (handle->refcount > 0) handle->refcount--;
  if (handle->refcount == 0 && !handle->pinned) {
    /* Unlink from its bucket chain, then destroy outside the structure. */
    NjpEntry **pp = &sh->buckets[b];
    while (*pp != NULL && *pp != handle) pp = &(*pp)->next;
    if (*pp == handle) {
      *pp = handle->next;
      sh->live--;
      cache->destroy(cache->cb_ctx, &handle->item);
      free(handle->key);
      free(handle);
    }
  }
  pthread_mutex_unlock(&sh->mtx);
}

/* ------------------------------------------------------------------ */
/* Diagnostics.                                                       */
/* ------------------------------------------------------------------ */

unsigned ndb_jit_progcache_live_count(NdbJitProgCache *cache) {
  if (cache == NULL) return 0;
  unsigned total = 0;
  for (unsigned i = 0; i < NJP_N_SHARDS; ++i) {
    pthread_mutex_lock(&cache->shards[i].mtx);
    total += cache->shards[i].live;
    pthread_mutex_unlock(&cache->shards[i].mtx);
  }
  return total;
}

uint64_t ndb_jit_progcache_compile_count(NdbJitProgCache *cache) {
  if (cache == NULL) return 0;
  uint64_t total = 0;
  for (unsigned i = 0; i < NJP_N_SHARDS; ++i) {
    pthread_mutex_lock(&cache->shards[i].mtx);
    total += cache->shards[i].compiles;
    pthread_mutex_unlock(&cache->shards[i].mtx);
  }
  return total;
}

uint64_t ndb_jit_progcache_hit_count(NdbJitProgCache *cache) {
  if (cache == NULL) return 0;
  uint64_t total = 0;
  for (unsigned i = 0; i < NJP_N_SHARDS; ++i) {
    pthread_mutex_lock(&cache->shards[i].mtx);
    total += cache->shards[i].hits;
    pthread_mutex_unlock(&cache->shards[i].mtx);
  }
  return total;
}
