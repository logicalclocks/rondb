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
 * RONDB-1056 Phase 8 — node-global JIT code-memory manager.
 *
 * Replaces the Phase 0 monotonic bump arena (which never freed, so a
 * long-running node with many distinct prepared statements eventually
 * exhausted its 1 MB and silently stopped JITting). This is a
 * free-capable slot allocator over the same W^X executable-memory
 * substrate (jit_arena.{c,inc.c}: Linux dual RW/RX mappings, macOS
 * MAP_JIT + per-thread write protection).
 *
 * Structure (plan.md §14):
 *   - A small fixed set of size classes (256 B .. 8 KB). A compiled
 *     blob goes into the smallest class that holds it; blobs larger
 *     than the top class don't JIT (alloc fails, caller falls back).
 *   - Each size class owns one or more *slabs* — a slab is one W^X
 *     arena split into fixed-size slots. Free slots are tracked by an
 *     intrusive singly-linked free list whose links live in normal
 *     heap (NjcSlot descriptors), NOT in the executable memory, so
 *     free() never has to re-enable macOS JIT writes.
 *   - Each size class has its OWN mutex (striped). Allocation and free
 *     for different classes never contend. A separate small cap mutex
 *     guards slab growth against a node-global reserved-byte ceiling.
 *   - The per-row EXECUTION path takes no lock here at all: a worker
 *     only ever calls an already-published RX pointer.
 *
 * On OOM (cap reached / mmap failure) alloc returns -1; the caller
 * publishes a NULL JIT entry and runs that program on the interpreter.
 * Reclaiming freed slots is automatic (free-list reuse); evicting
 * still-live-but-idle cached programs is a higher layer (jit_progcache)
 * and out of scope for the allocator.
 *
 * Pure C11. No NDB / kernel dependencies beyond the arena substrate.
 */

#ifndef NDB_JIT_CODEMEM_H
#define NDB_JIT_CODEMEM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NdbJitCodeMem NdbJitCodeMem;

/* Handle to one reserved executable slot. Returned by alloc, completed
 * by seal, surrendered by free. The caller treats rw/rx/capacity as
 * read-only and sets `length` to the number of bytes it emitted before
 * calling seal. `_slot` is opaque allocator bookkeeping. */
typedef struct {
  void       *rw;        /* write emitted code here (writable view) */
  const void *rx;        /* executable alias; set at alloc, CALLABLE after seal */
  uint32_t    capacity;  /* usable bytes in the slot (size-class size) */
  uint32_t    length;    /* bytes the caller emitted; set before seal */
  void       *_slot;     /* opaque: owning NjcSlot* */
} NdbJitCodeSlot;

/* Largest blob the manager can place. Requests above this fail alloc
 * (the program stays on the interpreter). */
#define NDB_JIT_CODEMEM_MAX_BLOB 8192u

/**
 * Create a manager whose slabs may reserve at most @p cap_bytes of
 * executable memory in total (page-rounded per slab). Pass 0 for the
 * built-in default. Returns NULL only on a mutex-init failure; slab
 * mmap is lazy (first alloc of a class).
 */
NdbJitCodeMem *ndb_jit_codemem_create(size_t cap_bytes);

/**
 * Destroy a manager: unmaps every slab and frees all bookkeeping.
 * The caller must guarantee no compiled program from this manager is
 * still live (executing or referenced). Safe with NULL.
 */
void ndb_jit_codemem_destroy(NdbJitCodeMem *mem);

/**
 * The node-global singleton, lazily created with the default cap on
 * first call. Thread-safe. Never destroyed (lives for the node's
 * lifetime). Returns NULL only if the one-time init failed.
 */
NdbJitCodeMem *ndb_jit_codemem_global(void);

/**
 * Reserve a slot able to hold @p bytes of code. On success returns 0
 * and fills @p out (rw, capacity, _slot, length=0, and rx = the slot's
 * executable alias — known immediately for branch fixups, but only
 * CALLABLE after seal); the calling thread is left write-enabled for
 * the slot's backing region (macOS) so the caller can emit immediately.
 *
 * Returns -1 if @p bytes exceeds NDB_JIT_CODEMEM_MAX_BLOB, or on OOM
 * (reserved-byte cap reached, or a new slab's mmap failed). On -1 the
 * caller falls back to the interpreter for that program.
 */
int ndb_jit_codemem_alloc(NdbJitCodeMem *mem, size_t bytes,
                          NdbJitCodeSlot *out);

/**
 * Publish @p out->length bytes emitted at @p out->rw: flush the
 * I-cache for the slot range and (macOS) flip the calling thread's
 * JIT write protection back on. Sets @p out->rx to the callable
 * pointer. Returns 0, or -1 on a bad/empty handle.
 */
int ndb_jit_codemem_seal(NdbJitCodeMem *mem, NdbJitCodeSlot *out);

/**
 * Return the slot to its size-class free list for reuse. Touches no
 * executable memory. Clears @p out. Safe with a zeroed/already-freed
 * handle (no-op).
 */
void ndb_jit_codemem_free(NdbJitCodeMem *mem, NdbJitCodeSlot *out);

/* ---- Diagnostics (feed NDBINFO counters in a later slice) -------- */

/* Total executable bytes mmap'd across all slabs (page-rounded). */
size_t ndb_jit_codemem_reserved_bytes(NdbJitCodeMem *mem);

/* Sum of the capacities of currently-allocated (live) slots. */
size_t ndb_jit_codemem_inuse_bytes(NdbJitCodeMem *mem);

/* Number of currently-allocated (live) slots. */
unsigned ndb_jit_codemem_live_slots(NdbJitCodeMem *mem);

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_CODEMEM_H */
