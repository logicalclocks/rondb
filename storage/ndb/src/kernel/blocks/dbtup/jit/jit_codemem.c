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
 * RONDB-1056 Phase 8 — node-global JIT code-memory manager (impl).
 * See jit_codemem.h for the design rationale.
 */

#include "jit_codemem.h"
#include "jit_arena.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Tunables.                                                          */
/* ------------------------------------------------------------------ */

/* Size classes. Smallest first; class_for_bytes returns the smallest
 * that holds the request. Top class == NDB_JIT_CODEMEM_MAX_BLOB. */
static const uint32_t kClassBytes[] = {256u, 512u, 1024u, 2048u, 4096u, 8192u};
#define NJC_N_CLASSES ((int)(sizeof(kClassBytes) / sizeof(kClassBytes[0])))

/* Target bytes reserved per slab. A multiple of every class size, so a
 * slab carves into a whole number of slots with no waste and rounds to
 * the page on every supported platform (4 KB Linux, 16 KB macOS). */
#define NJC_SLAB_TARGET (64u * 1024u)

/* Default node-global reservation ceiling. Bounds total executable
 * memory across all slabs; once hit, alloc fails and callers fall back
 * to the interpreter rather than growing without limit. */
#define NJC_DEFAULT_CAP_BYTES (16u * 1024u * 1024u)

/* ------------------------------------------------------------------ */
/* Internal structures.                                               */
/* ------------------------------------------------------------------ */

typedef struct NjcSlot {
  void           *rw;         /* writable slot address */
  const void     *rx;         /* executable alias (precomputed at carve) */
  struct NjcSlot *next_free;  /* free-list link (heap, never in code mem) */
  struct NjcSlab *slab;       /* owning slab */
  uint8_t         in_use;     /* 1 while handed out */
} NjcSlot;

typedef struct NjcSlab {
  NdbJitArena    *arena;      /* W^X dual mapping backing this slab */
  NjcSlot        *slots;      /* n_slots descriptors (heap) */
  uint32_t        n_slots;
  uint8_t         class_idx;  /* index into kClassBytes */
  struct NjcSlab *next;       /* next slab in the same size class */
} NjcSlab;

typedef struct {
  pthread_mutex_t mtx;        /* per-class lock (striped) */
  uint32_t        slot_bytes; /* == kClassBytes[idx] */
  NjcSlot        *free_head;  /* intrusive free list head */
  NjcSlab        *slabs;      /* slab list for this class */
  uint32_t        live;       /* in-use slot count (under mtx) */
} NjcClass;

struct NdbJitCodeMem {
  NjcClass        classes[NJC_N_CLASSES];
  pthread_mutex_t cap_mtx;    /* guards reserved_bytes + slab growth */
  size_t          cap_bytes;
  size_t          reserved_bytes;
};

/* ------------------------------------------------------------------ */
/* Helpers.                                                           */
/* ------------------------------------------------------------------ */

static int class_for_bytes(size_t bytes) {
  for (int i = 0; i < NJC_N_CLASSES; ++i) {
    if (bytes <= kClassBytes[i]) return i;
  }
  return -1;
}

/* Grow size class @p idx by one slab. Caller holds classes[idx].mtx.
 * Returns 0 on success (free list now non-empty), -1 on OOM. */
static int grow_class(NdbJitCodeMem *mem, int idx) {
  NjcClass *cls = &mem->classes[idx];
  uint32_t slot_bytes = cls->slot_bytes;
  uint32_t n_slots = NJC_SLAB_TARGET / slot_bytes;
  if (n_slots == 0) n_slots = 1;
  size_t need = (size_t)n_slots * slot_bytes;

  /* Reserve against the node-global cap, then map. */
  pthread_mutex_lock(&mem->cap_mtx);
  if (mem->reserved_bytes + need > mem->cap_bytes) {
    pthread_mutex_unlock(&mem->cap_mtx);
    return -1;
  }
  NdbJitArena *arena = ndb_jit_arena_create(need);
  if (arena == NULL) {
    pthread_mutex_unlock(&mem->cap_mtx);
    return -1;
  }
  size_t actual = ndb_jit_arena_size(arena);
  mem->reserved_bytes += actual;
  pthread_mutex_unlock(&mem->cap_mtx);

  /* Carve the whole usable region in one bump; slots are offsets into
   * it. exec_addr gives each slot's executable alias up front. */
  void *base = ndb_jit_arena_alloc(arena, need, 16);
  NjcSlab *slab = (NjcSlab *)calloc(1, sizeof(*slab));
  NjcSlot *slots = (NjcSlot *)calloc(n_slots, sizeof(*slots));
  if (base == NULL || slab == NULL || slots == NULL) {
    free(slots);
    free(slab);
    ndb_jit_arena_destroy(arena);
    pthread_mutex_lock(&mem->cap_mtx);
    mem->reserved_bytes -= actual;
    pthread_mutex_unlock(&mem->cap_mtx);
    return -1;
  }

  for (uint32_t i = 0; i < n_slots; ++i) {
    uint8_t *slot_rw = (uint8_t *)base + (size_t)i * slot_bytes;
    slots[i].rw = slot_rw;
    slots[i].rx = ndb_jit_arena_exec_addr(arena, slot_rw);
    slots[i].slab = slab;
    slots[i].in_use = 0;
    /* push onto the class free list */
    slots[i].next_free = cls->free_head;
    cls->free_head = &slots[i];
  }

  slab->arena = arena;
  slab->slots = slots;
  slab->n_slots = n_slots;
  slab->class_idx = (uint8_t)idx;
  slab->next = cls->slabs;
  cls->slabs = slab;
  return 0;
}

/* ------------------------------------------------------------------ */
/* Lifecycle.                                                         */
/* ------------------------------------------------------------------ */

NdbJitCodeMem *ndb_jit_codemem_create(size_t cap_bytes) {
  NdbJitCodeMem *mem = (NdbJitCodeMem *)calloc(1, sizeof(*mem));
  if (mem == NULL) return NULL;
  mem->cap_bytes = (cap_bytes != 0) ? cap_bytes : NJC_DEFAULT_CAP_BYTES;
  mem->reserved_bytes = 0;

  if (pthread_mutex_init(&mem->cap_mtx, NULL) != 0) {
    free(mem);
    return NULL;
  }
  for (int i = 0; i < NJC_N_CLASSES; ++i) {
    if (pthread_mutex_init(&mem->classes[i].mtx, NULL) != 0) {
      for (int j = 0; j < i; ++j) pthread_mutex_destroy(&mem->classes[j].mtx);
      pthread_mutex_destroy(&mem->cap_mtx);
      free(mem);
      return NULL;
    }
    mem->classes[i].slot_bytes = kClassBytes[i];
    mem->classes[i].free_head = NULL;
    mem->classes[i].slabs = NULL;
    mem->classes[i].live = 0;
  }
  return mem;
}

void ndb_jit_codemem_destroy(NdbJitCodeMem *mem) {
  if (mem == NULL) return;
  for (int i = 0; i < NJC_N_CLASSES; ++i) {
    NjcSlab *slab = mem->classes[i].slabs;
    while (slab != NULL) {
      NjcSlab *next = slab->next;
      ndb_jit_arena_destroy(slab->arena);
      free(slab->slots);
      free(slab);
      slab = next;
    }
    pthread_mutex_destroy(&mem->classes[i].mtx);
  }
  pthread_mutex_destroy(&mem->cap_mtx);
  free(mem);
}

/* ------------------------------------------------------------------ */
/* Node-global singleton.                                             */
/* ------------------------------------------------------------------ */

static pthread_once_t g_global_once = PTHREAD_ONCE_INIT;
static NdbJitCodeMem *g_global = NULL;

static void global_init(void) { g_global = ndb_jit_codemem_create(0); }

NdbJitCodeMem *ndb_jit_codemem_global(void) {
  pthread_once(&g_global_once, global_init);
  return g_global;
}

/* ------------------------------------------------------------------ */
/* Alloc / seal / free.                                               */
/* ------------------------------------------------------------------ */

int ndb_jit_codemem_alloc(NdbJitCodeMem *mem, size_t bytes,
                          NdbJitCodeSlot *out) {
  if (mem == NULL || out == NULL || bytes == 0) return -1;
  int idx = class_for_bytes(bytes);
  if (idx < 0) return -1;   /* larger than the top class -> no JIT */
  NjcClass *cls = &mem->classes[idx];

  pthread_mutex_lock(&cls->mtx);
  if (cls->free_head == NULL) {
    if (grow_class(mem, idx) != 0) {
      pthread_mutex_unlock(&cls->mtx);
      return -1;            /* OOM */
    }
  }
  NjcSlot *slot = cls->free_head;
  cls->free_head = slot->next_free;
  slot->next_free = NULL;
  slot->in_use = 1;
  cls->live++;
  NdbJitArena *arena = slot->slab->arena;
  pthread_mutex_unlock(&cls->mtx);

  /* The emitting thread must be write-enabled for this slot's backing
   * region (macOS no-op elsewhere). Done outside the lock — it toggles
   * only the calling thread's protection. */
  ndb_jit_arena_prepare_write(arena);

  out->rw = slot->rw;
  out->rx = NULL;
  out->capacity = cls->slot_bytes;
  out->length = 0;
  out->_slot = slot;
  return 0;
}

int ndb_jit_codemem_seal(NdbJitCodeMem *mem, NdbJitCodeSlot *out) {
  if (mem == NULL || out == NULL || out->_slot == NULL || out->rw == NULL) {
    return -1;
  }
  if (out->length == 0 || out->length > out->capacity) return -1;
  NjcSlot *slot = (NjcSlot *)out->_slot;
  const void *rx = ndb_jit_arena_seal(slot->slab->arena, out->rw, out->length);
  if (rx == NULL) return -1;
  out->rx = rx;   /* == slot->rx */
  return 0;
}

void ndb_jit_codemem_free(NdbJitCodeMem *mem, NdbJitCodeSlot *out) {
  if (mem == NULL || out == NULL || out->_slot == NULL) return;
  NjcSlot *slot = (NjcSlot *)out->_slot;
  NjcClass *cls = &mem->classes[slot->slab->class_idx];

  pthread_mutex_lock(&cls->mtx);
  if (slot->in_use) {
    slot->in_use = 0;
    slot->next_free = cls->free_head;
    cls->free_head = slot;
    cls->live--;
  }
  pthread_mutex_unlock(&cls->mtx);

  out->_slot = NULL;
  out->rw = NULL;
  out->rx = NULL;
  out->length = 0;
  out->capacity = 0;
}

/* ------------------------------------------------------------------ */
/* Diagnostics.                                                       */
/* ------------------------------------------------------------------ */

size_t ndb_jit_codemem_reserved_bytes(NdbJitCodeMem *mem) {
  if (mem == NULL) return 0;
  pthread_mutex_lock(&mem->cap_mtx);
  size_t r = mem->reserved_bytes;
  pthread_mutex_unlock(&mem->cap_mtx);
  return r;
}

size_t ndb_jit_codemem_inuse_bytes(NdbJitCodeMem *mem) {
  if (mem == NULL) return 0;
  size_t total = 0;
  for (int i = 0; i < NJC_N_CLASSES; ++i) {
    pthread_mutex_lock(&mem->classes[i].mtx);
    total += (size_t)mem->classes[i].live * mem->classes[i].slot_bytes;
    pthread_mutex_unlock(&mem->classes[i].mtx);
  }
  return total;
}

unsigned ndb_jit_codemem_live_slots(NdbJitCodeMem *mem) {
  if (mem == NULL) return 0;
  unsigned total = 0;
  for (int i = 0; i < NJC_N_CLASSES; ++i) {
    pthread_mutex_lock(&mem->classes[i].mtx);
    total += mem->classes[i].live;
    pthread_mutex_unlock(&mem->classes[i].mtx);
  }
  return total;
}
