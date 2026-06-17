/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

/*
 * RONDB-1056 Phase 8 — compiled-program reuse cache unit tests.
 *
 * Two harnesses:
 *   1. A MOCK compiler (no real code) exercising the cache contract:
 *      miss-compiles-once / hit-reuses, refcount eviction, distinct
 *      keys, exact-key (no false reuse), pinned-survives-refcount-0,
 *      compile-refuse, diagnostics, destroy-frees-live.
 *   2. A capstone INTEGRATION test wiring the cache to the real
 *      jit_codemem manager: the compile callback emits a "return N"
 *      stub into a codemem slot; eviction frees the slot. Proves
 *      reuse maps to one slot and release-to-zero reclaims it.
 */

#include "jit_codemem.h"
#include "jit_progcache.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int n_pass = 0;
static int n_fail = 0;

static void check(int cond, const char *name) {
  if (cond) {
    printf("  PASS  %s\n", name);
    n_pass++;
  } else {
    printf("  FAIL  %s\n", name);
    n_fail++;
  }
}

/* ------------------------------------------------------------------ */
/* Mock compiler.                                                     */
/* ------------------------------------------------------------------ */

typedef struct {
  int compiles;
  int destroys;
  int refuse;     /* when set, compile callback refuses */
} Mock;

static int mock_compile(void *ctx, const uint8_t *key, uint32_t key_len,
                        NdbJitProgItem *out) {
  Mock *m = (Mock *)ctx;
  if (m->refuse) return -1;
  m->compiles++;
  /* A fake "compiled product": a heap block tagged with the key len. */
  int *blk = (int *)malloc(sizeof(int));
  *blk = (int)key_len;
  (void)key;
  out->entry_fn = blk;
  out->user = NULL;
  return 0;
}

static void mock_destroy(void *ctx, NdbJitProgItem *item) {
  Mock *m = (Mock *)ctx;
  m->destroys++;
  free(item->entry_fn);
}

/* ------------------------------------------------------------------ */
/* Mock-compiler tests.                                               */
/* ------------------------------------------------------------------ */

static void test_miss_then_hit(void) {
  Mock m = {0, 0, 0};
  NdbJitProgCache *c = ndb_jit_progcache_create(mock_compile, mock_destroy, &m);
  const uint8_t key[] = {1, 2, 3, 4};

  NdbJitProgItem i1, i2;
  NjpEntry *h1 = ndb_jit_progcache_acquire(c, key, sizeof(key), 0, &i1);
  NjpEntry *h2 = ndb_jit_progcache_acquire(c, key, sizeof(key), 0, &i2);
  check(h1 != NULL && h2 == h1, "same key returns the same entry");
  check(m.compiles == 1, "miss compiles once; hit does not recompile");
  check(ndb_jit_progcache_hit_count(c) == 1, "hit counted");
  check(i1.entry_fn == i2.entry_fn && i1.entry_fn != NULL,
        "hit yields the same compiled product");
  check(ndb_jit_progcache_live_count(c) == 1, "one live entry");

  ndb_jit_progcache_release(c, h1);
  check(m.destroys == 0, "release with refcount remaining does not evict");
  ndb_jit_progcache_release(c, h2);
  check(m.destroys == 1, "release to refcount 0 evicts (destroy called)");
  check(ndb_jit_progcache_live_count(c) == 0, "no live entries after eviction");

  ndb_jit_progcache_destroy(c);
}

static void test_distinct_keys(void) {
  Mock m = {0, 0, 0};
  NdbJitProgCache *c = ndb_jit_progcache_create(mock_compile, mock_destroy, &m);
  const uint8_t a[] = {1, 2, 3, 4};
  const uint8_t b[] = {9, 9, 9, 9};

  NdbJitProgItem ia, ib;
  NjpEntry *ha = ndb_jit_progcache_acquire(c, a, sizeof(a), 0, &ia);
  NjpEntry *hb = ndb_jit_progcache_acquire(c, b, sizeof(b), 0, &ib);
  check(ha != hb && ia.entry_fn != ib.entry_fn, "distinct keys -> distinct entries");
  check(m.compiles == 2, "two compiles for two keys");
  check(ndb_jit_progcache_live_count(c) == 2, "two live entries");

  ndb_jit_progcache_release(c, ha);
  ndb_jit_progcache_release(c, hb);
  check(m.destroys == 2, "both evicted");
  ndb_jit_progcache_destroy(c);
}

static void test_exact_key_no_false_reuse(void) {
  Mock m = {0, 0, 0};
  NdbJitProgCache *c = ndb_jit_progcache_create(mock_compile, mock_destroy, &m);
  /* Same length, last byte differs — must not alias. */
  const uint8_t a[] = {1, 2, 3, 4};
  const uint8_t a2[] = {1, 2, 3, 5};

  NjpEntry *ha = ndb_jit_progcache_acquire(c, a, sizeof(a), 0, NULL);
  NjpEntry *ha2 = ndb_jit_progcache_acquire(c, a2, sizeof(a2), 0, NULL);
  check(ha != ha2 && m.compiles == 2,
        "keys differing by one byte do not falsely reuse");

  ndb_jit_progcache_release(c, ha);
  ndb_jit_progcache_release(c, ha2);
  ndb_jit_progcache_destroy(c);
}

static void test_pinned_survives_zero(void) {
  Mock m = {0, 0, 0};
  NdbJitProgCache *c = ndb_jit_progcache_create(mock_compile, mock_destroy, &m);
  const uint8_t key[] = {7, 7, 7};

  NjpEntry *h = ndb_jit_progcache_acquire(c, key, sizeof(key), /*pinned=*/1, NULL);
  ndb_jit_progcache_release(c, h);
  check(m.destroys == 0 && ndb_jit_progcache_live_count(c) == 1,
        "pinned entry is retained at refcount 0");

  NjpEntry *h2 = ndb_jit_progcache_acquire(c, key, sizeof(key), 0, NULL);
  check(h2 == h && m.compiles == 1, "pinned entry is reused (no recompile)");
  ndb_jit_progcache_release(c, h2);
  check(m.destroys == 0, "pinned entry still retained after release");

  ndb_jit_progcache_destroy(c);
  check(m.destroys == 1, "cache teardown frees the pinned entry");
}

static void test_compile_refuse(void) {
  Mock m = {0, 0, 1 /*refuse*/};
  NdbJitProgCache *c = ndb_jit_progcache_create(mock_compile, mock_destroy, &m);
  const uint8_t key[] = {3, 1, 4};

  NjpEntry *h = ndb_jit_progcache_acquire(c, key, sizeof(key), 0, NULL);
  check(h == NULL, "compile-refuse -> acquire returns NULL");
  check(ndb_jit_progcache_live_count(c) == 0 && m.destroys == 0,
        "refuse leaves nothing cached and destroys nothing");
  ndb_jit_progcache_destroy(c);
}

static void test_destroy_frees_live(void) {
  Mock m = {0, 0, 0};
  NdbJitProgCache *c = ndb_jit_progcache_create(mock_compile, mock_destroy, &m);
  const uint8_t a[] = {1}, b[] = {2}, d[] = {3};
  /* Acquire and DON'T release — still-live at teardown. */
  (void)ndb_jit_progcache_acquire(c, a, sizeof(a), 0, NULL);
  (void)ndb_jit_progcache_acquire(c, b, sizeof(b), 0, NULL);
  (void)ndb_jit_progcache_acquire(c, d, sizeof(d), 0, NULL);
  check(ndb_jit_progcache_live_count(c) == 3, "three live entries before teardown");
  ndb_jit_progcache_destroy(c);
  check(m.destroys == 3, "teardown frees every remaining entry");
}

/* ------------------------------------------------------------------ */
/* Integration: real jit_codemem as the compiled-program backing.     */
/* ------------------------------------------------------------------ */

typedef struct {
  NdbJitCodeMem *mem;
  int compiles;
  int destroys;
} CodeCtx;

#if defined(__x86_64__)
static size_t make_ret(uint8_t imm, uint8_t *buf) {
  buf[0] = 0xb8; buf[1] = imm; buf[2] = 0; buf[3] = 0; buf[4] = 0; buf[5] = 0xc3;
  return 6;
}
#elif defined(__aarch64__)
static size_t make_ret(uint8_t imm, uint8_t *buf) {
  uint32_t movz = 0x52800000u | ((uint32_t)imm << 5);
  buf[0] = (uint8_t)(movz & 0xff);
  buf[1] = (uint8_t)((movz >> 8) & 0xff);
  buf[2] = (uint8_t)((movz >> 16) & 0xff);
  buf[3] = (uint8_t)((movz >> 24) & 0xff);
  buf[4] = 0xc0; buf[5] = 0x03; buf[6] = 0x5f; buf[7] = 0xd6;
  return 8;
}
#else
#error "RONDB-1056 progcache_tests: unsupported test arch"
#endif

typedef int (*ret_fn_t)(void);

/* Compile a "return key[0]" stub into a codemem slot. user carries the
 * slot handle so destroy can free it. */
static int code_compile(void *ctx, const uint8_t *key, uint32_t key_len,
                        NdbJitProgItem *out) {
  CodeCtx *cc = (CodeCtx *)ctx;
  (void)key_len;
  NdbJitCodeSlot *slot = (NdbJitCodeSlot *)malloc(sizeof(*slot));
  if (slot == NULL) return -1;
  if (ndb_jit_codemem_alloc(cc->mem, 64, slot) != 0) { free(slot); return -1; }
  slot->length = (uint32_t)make_ret(key[0], (uint8_t *)slot->rw);
  if (ndb_jit_codemem_seal(cc->mem, slot) != 0) {
    ndb_jit_codemem_free(cc->mem, slot);
    free(slot);
    return -1;
  }
  cc->compiles++;
  out->entry_fn = (void *)(uintptr_t)slot->rx;
  out->user = slot;
  return 0;
}

static void code_destroy(void *ctx, NdbJitProgItem *item) {
  CodeCtx *cc = (CodeCtx *)ctx;
  cc->destroys++;
  ndb_jit_codemem_free(cc->mem, (NdbJitCodeSlot *)item->user);
  free(item->user);
}

static void test_codemem_integration(void) {
  CodeCtx cc = {ndb_jit_codemem_create(0), 0, 0};
  NdbJitProgCache *c = ndb_jit_progcache_create(code_compile, code_destroy, &cc);
  const uint8_t key[] = {55, 0, 1, 2};   /* key[0]=55 -> stub returns 55 */

  NdbJitProgItem i1, i2;
  NjpEntry *h1 = ndb_jit_progcache_acquire(c, key, sizeof(key), 0, &i1);
  NjpEntry *h2 = ndb_jit_progcache_acquire(c, key, sizeof(key), 0, &i2);
  check(h1 != NULL && h1 == h2, "integration: same bytecode shares one entry");
  check(cc.compiles == 1, "integration: compiled once");
  check(ndb_jit_codemem_live_slots(cc.mem) == 1,
        "integration: reuse maps to a single codemem slot");
  if (h1 != NULL) {
    ret_fn_t fn = (ret_fn_t)(uintptr_t)i1.entry_fn;
    check(fn() == 55, "integration: cached slot executes the real stub");
  }

  ndb_jit_progcache_release(c, h1);
  check(ndb_jit_codemem_live_slots(cc.mem) == 1, "integration: slot held while refcount>0");
  ndb_jit_progcache_release(c, h2);
  check(cc.destroys == 1 && ndb_jit_codemem_live_slots(cc.mem) == 0,
        "integration: release-to-zero frees the codemem slot");

  ndb_jit_progcache_destroy(c);
  ndb_jit_codemem_destroy(cc.mem);
}

int main(void) {
  printf("RONDB-1056 jit_progcache unit tests\n");
  test_miss_then_hit();
  test_distinct_keys();
  test_exact_key_no_false_reuse();
  test_pinned_survives_zero();
  test_compile_refuse();
  test_destroy_frees_live();
  test_codemem_integration();
  printf("\n%d passed, %d failed\n", n_pass, n_fail);
  return n_fail == 0 ? 0 : 1;
}
