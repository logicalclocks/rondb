# Phase 8 — JIT code-memory manager + compiled-program cache

**Status: Slices 1a (code-memory manager) + 1b (program reuse cache)
implemented, host-testable, inert in the kernel (2026-06-17).** The
remaining slices (2 jit1 wiring, 3 DBTUP/agg wiring, 4 RonSQL PREPARE) are
designed below but not yet built.

This is Phase 8 item #1 from `plan.md` §14 — *the* ship blocker. Before
this, JIT code memory was a per-block monotonic bump arena
(`jit/jit_arena.c`, `ndb_jit_arena_alloc` never frees): per-DBTUP 1 MB
(`DbtupGen.cpp:~269`) + per-`DblqhProxy` 1 MB. A long-running node with many
distinct prepared statements exhausted it and silently stopped JITting
forever (OOM → `jit1_compile` NULL → interpreter fallback, permanently).

## Architecture — two node-global layers

Decided with Mikael (2026-06-17): **node-global** (not per-block), with
**striped locking** and a **reuse cache**, so identical programs (e.g. the
same prepared statement re-executed) compile once and share one blob.

### Layer 1 — code-memory manager (`jit_codemem.{h,c}`)  ← Slice 1a, DONE

Free-capable slot allocator over the existing W^X substrate (kept verbatim:
Linux dual RW/RX `memfd` mappings, macOS `MAP_JIT` + per-thread write
protect). A slab is one `NdbJitArena`; the manager carves it into fixed
slots.

- **Size classes** `{256, 512, 1024, 2048, 4096, 8192}` B (`kClassBytes`).
  `alloc(bytes)` picks the smallest class that fits; `bytes >
  NDB_JIT_CODEMEM_MAX_BLOB (8192)` → `-1` (program stays on the
  interpreter). Realistic filter/agg blobs are far under 8 KB.
- **Striped locks:** each size class is an independent free list + its own
  `pthread_mutex_t`. Different classes never contend. A separate small
  `cap_mtx` guards slab growth against the node-global reserved-byte ceiling
  (`NJC_DEFAULT_CAP_BYTES = 16 MB`, overridable via `create(cap_bytes)`).
- **Free lists live in heap, not in code memory.** Each slot has an
  `NjcSlot` descriptor (`rw`, precomputed `rx`, `next_free`, `slab`,
  `in_use`) in normal `malloc` memory. `free()` only relinks descriptors —
  it never writes into executable memory, so it needs no macOS write-toggle
  and is a plain mutex-protected list push.
- **Slabs:** `NJC_SLAB_TARGET = 64 KB` per slab (a multiple of every class
  size and ≥ one page on 4 KB/16 KB platforms, so no waste, clean page
  rounding). `n_slots = 64 KB / slot_bytes`. Grown lazily on first
  exhaustion of a class; capped globally.
- **Execution is lock-free:** a worker only ever calls an already-sealed
  `rx` pointer; no `jit_codemem` lock is on the per-row path. Locks are
  taken only on alloc / free / seal / diagnostics (per-program, not
  per-row).
- **OOM** (`cap` reached or `mmap` fails): `alloc` returns `-1` → caller
  publishes a NULL entry → interpreter fallback. Reclaiming *freed* slots is
  automatic (free-list reuse); evicting *live-but-idle* cached programs is
  Layer 2's job.
- **API:** `create/destroy`, `global()` (lazy `pthread_once` singleton,
  node-lived, never destroyed), `alloc(bytes,&slot)`, `seal(&slot)` (sets
  `rx`), `free(&slot)`, and diagnostics `reserved_bytes` / `inuse_bytes` /
  `live_slots` (feed NDBINFO counters in a later Phase 8 item).
- **Substrate addition:** `ndb_jit_arena_prepare_write(arena)` — macOS
  re-enables the calling thread's JIT write (recycled-slot emit doesn't go
  through `alloc`'s bump); Linux no-op. `alloc` calls it before returning a
  slot.

**macOS note (dev only; production is Linux):** `MAP_JIT` write protection
is per-thread. `alloc` enables write on the calling thread; `seal` flips it
back; both touch `arena->jit_write_enabled` (a shared int) — a benign race
under concurrent compile that affects only the `destroy`-time re-enable
decision, and slabs live until node shutdown. On Linux the field is unused
and RW is always writable, so concurrent compile of distinct slots is fully
safe; per-row execution is lock-free on both.

**Tests (`test/jit_proto/codemem_tests.c`, host):** class selection,
alloc+emit+seal+execute round-trip (arch "return N" stub), distinct-slots-
distinct-code, slot reuse after free (no new slab — `reserved_bytes`
unchanged, recycled `rw` from the freed set), reserved/in-use/live
accounting, the cap OOM path (`-1` past cap, succeeds again after a free),
`> MAX_BLOB` / zero rejection, seal validation, global singleton.

### Layer 2 — compiled-program cache (`jit_progcache.{h,c}`)  ← Slice 1b, DONE

Sharded refcounted hash so identical bytecode reuses one compiled blob.

- **Key = exact bytecode words** (length + content; `hash` + `memcmp`, so a
  hash collision never causes false reuse). Reuse is sound because the
  compiled blob is a pure function of the bytecode: stencils are static,
  helpers resolve by global name, and runtime operands (OP_PARAM bind
  values) are read per-row via `param_buf` — *not* baked into the code. So
  two `EXECUTE`s with different bind values share one blob.
- **Sharded hash, striped mutexes:** `NJP_N_SHARDS = 16` independent
  sections, each a `NJP_N_BUCKETS = 64` bucket array + its own mutex
  (FNV-1a hash; shard = low bits, bucket = next bits). A lookup/insert locks
  only one shard. On miss, compile under the shard lock (≈µs, off the
  per-row path; serializes only that 1/16 section and guarantees no
  duplicate compile of the same program) → insert.
- **Refcount + create/destroy API:** `acquire(key, len, pinned, &item) →
  handle` (hit: refcount++, return shared entry; miss: run compile_cb +
  insert, refcount = 1; refuse: return NULL → caller falls back).
  `release(handle)`: refcount−−; at 0, a non-pinned entry is evicted and its
  destroy_cb runs. **Decoupled from `codemem` and NDB via callbacks** —
  `NjpCompileFn` (caller does bridge-translate + `jit1_compile` into a
  codemem slot) and `NjpDestroyFn` (caller frees the slot); the cache owns
  only the hash, key copies, and refcounts.
- **`pinned` / reusable hint:** a pinned entry is **retained at refcount 0**
  (not evicted) for future reuse, and reclaimed only at cache teardown (or a
  future memory-pressure sweep) — the **RonSQL PREPARE** use case
  (`acquire` pinned at prepare; the prepared statement also holds a ref
  across every `EXECUTE`). A one-off scan acquires unpinned and frees its
  slot when its last reference releases. `acquire` upgrades an entry to
  pinned (sticky); never downgrades.
- **Diagnostics:** `live_count` / `compile_count` (misses) / `hit_count`
  (reuses) — feed NDBINFO "compiles"/"reuses" later.
- Tested on the host (`test/jit_proto/progcache_tests.c`): mock-compiler
  cases (miss-compiles-once/hit-reuses, refcount eviction, distinct keys,
  exact-key no-false-reuse, pinned-survives-refcount-0, compile-refuse,
  diagnostics, destroy-frees-live) **plus a capstone integration test on the
  real `codemem`** — same bytecode → one slot, the stub executes, and
  release-to-zero frees the slot (asserted via `codemem` `live_slots`).

## Staging

- **1a — `jit_codemem` + tests.** ✅ implemented, inert in kernel.
- **1b — `jit_progcache` + tests.** ✅ implemented, inert in kernel.
- **2 — `jit1_compile`** gains a `codemem`-backed allocation path
  (`Jit1Prog` records the `NdbJitCodeSlot` handle) + `jit1_free(prog)`;
  callers stop leaking the `Jit1Prog*`.
- **3 — DBTUP / agg** switch their compiles to `progcache` acquire/release
  and wire the program-death sites: scan filter `storedProc`
  (`DbtupExecQuery.cpp:1106` set / `DbtupStoredProcDef.cpp:188` reset), agg
  `setJitEntry` on `AggInterpreter` / `JoinAggInterpreter` /
  `DblqhProxy::m_leaf_programs[i]`. Drop the per-block `m_jit_arena`s.
- **4 — RonSQL PREPARE** pinned acquire at prepare / release at deallocate.

## Cross-thread publication (orthogonal, pre-existing)

aarch64 cross-core I-cache coherence (membarrier `SYNC_CORE`) is a separate
concern flagged in `jit_arena.c`'s seal comment; it is unchanged by this
work (the join-agg path already compiles on one thread and executes on LDM
threads). Not part of the allocator slice; revisit when validating on
aarch64 production hardware (production target is x86_64 Linux where I/D
caches are coherent).

## Files

- `src/kernel/blocks/dbtup/jit/jit_codemem.{h,c}` — the manager (Slice 1a).
- `src/kernel/blocks/dbtup/jit/jit_progcache.{h,c}` — the reuse cache (Slice 1b).
- `src/kernel/blocks/dbtup/jit/jit_arena.{h,c}` — `+ndb_jit_arena_prepare_write`.
- `src/kernel/blocks/dbtup/jit/CMakeLists.txt` — `jit_codemem.c` into
  `ndb_jit_arena`; new `ndb_jit_progcache` lib; `Threads::Threads` linked.
- `test/jit_proto/codemem_tests.c`, `progcache_tests.c` + `CMakeLists.txt` —
  host unit tests.
