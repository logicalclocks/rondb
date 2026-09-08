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
 * RONDB-1056 Phase 8 — node-global JIT code-memory manager unit tests.
 *
 * Exercises jit_codemem.c directly (no NDB / DBTUP / jit1): size-class
 * selection, alloc/seal/execute round-trip, slot reuse after free,
 * reserved/in-use accounting, the reserved-byte cap (OOM -> -1), and
 * over-max / zero-byte rejection. Runs straight off libndb_jit_arena.a.
 *
 * The execute checks emit a tiny architecture-specific "return N" stub
 * into a slot, seal it, and call it — proving a sealed slot's rx is a
 * live callable distinct from any other slot.
 */

#include "jit_codemem.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Harness.                                                           */
/* ------------------------------------------------------------------ */

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
/* Architecture-specific "return imm" stub builder.                   */
/* ------------------------------------------------------------------ */

typedef int (*ret_fn_t)(void);

#if defined(__x86_64__)
/*   b8 NN 00 00 00   mov eax, NN
 *   c3               ret                                    */
static size_t make_ret(uint8_t imm, uint8_t *buf) {
  buf[0] = 0xb8;
  buf[1] = imm;
  buf[2] = 0x00;
  buf[3] = 0x00;
  buf[4] = 0x00;
  buf[5] = 0xc3;
  return 6;
}
#elif defined(__aarch64__)
/*   52800000 | (imm<<5)   mov w0, #imm   (MOVZ, imm in [0,255])
 *   d65f03c0              ret                                 */
static size_t make_ret(uint8_t imm, uint8_t *buf) {
  uint32_t movz = 0x52800000u | ((uint32_t)imm << 5);
  buf[0] = (uint8_t)(movz & 0xff);
  buf[1] = (uint8_t)((movz >> 8) & 0xff);
  buf[2] = (uint8_t)((movz >> 16) & 0xff);
  buf[3] = (uint8_t)((movz >> 24) & 0xff);
  buf[4] = 0xc0;
  buf[5] = 0x03;
  buf[6] = 0x5f;
  buf[7] = 0xd6;
  return 8;
}
#else
#error "RONDB-1056 codemem_tests: unsupported test arch"
#endif

/* Emit "return imm" into a freshly-allocated slot of `mem`, seal it,
 * and return the slot handle (out). Returns 0 on success. */
static int compile_ret(NdbJitCodeMem *mem, uint8_t imm, NdbJitCodeSlot *out) {
  if (ndb_jit_codemem_alloc(mem, 64, out) != 0) return -1;
  out->length = (uint32_t)make_ret(imm, (uint8_t *)out->rw);
  return ndb_jit_codemem_seal(mem, out);
}

/* ------------------------------------------------------------------ */
/* Tests.                                                             */
/* ------------------------------------------------------------------ */

static void test_create_destroy(void) {
  NdbJitCodeMem *mem = ndb_jit_codemem_create(0);
  check(mem != NULL, "create returns a manager");
  check(ndb_jit_codemem_reserved_bytes(mem) == 0, "fresh: reserved == 0");
  check(ndb_jit_codemem_inuse_bytes(mem) == 0, "fresh: in-use == 0");
  check(ndb_jit_codemem_live_slots(mem) == 0, "fresh: live == 0");
  ndb_jit_codemem_destroy(mem);
  ndb_jit_codemem_destroy(NULL); /* must be a no-op, not a crash */
}

static void test_class_selection(void) {
  NdbJitCodeMem *mem = ndb_jit_codemem_create(0);
  struct { size_t req; uint32_t cap; } cases[] = {
    {1, 256}, {256, 256}, {257, 512}, {512, 512},
    {1000, 1024}, {2048, 2048}, {4097, 8192}, {8192, 8192},
  };
  int ok = 1;
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    NdbJitCodeSlot s;
    if (ndb_jit_codemem_alloc(mem, cases[i].req, &s) != 0) { ok = 0; break; }
    if (s.capacity != cases[i].cap) { ok = 0; ndb_jit_codemem_free(mem, &s); break; }
    if (s.rw == NULL) { ok = 0; ndb_jit_codemem_free(mem, &s); break; }
    ndb_jit_codemem_free(mem, &s);
  }
  check(ok, "alloc picks the smallest size class that fits");

  NdbJitCodeSlot big;
  check(ndb_jit_codemem_alloc(mem, NDB_JIT_CODEMEM_MAX_BLOB + 1, &big) == -1,
        "alloc rejects > MAX_BLOB");
  NdbJitCodeSlot zero;
  check(ndb_jit_codemem_alloc(mem, 0, &zero) == -1, "alloc rejects 0 bytes");
  ndb_jit_codemem_destroy(mem);
}

static void test_seal_execute(void) {
  NdbJitCodeMem *mem = ndb_jit_codemem_create(0);
  NdbJitCodeSlot s;
  int rc = compile_ret(mem, 42, &s);
  check(rc == 0 && s.rx != NULL, "alloc+emit+seal succeeds, rx set");
  if (rc == 0) {
    ret_fn_t fn = (ret_fn_t)(uintptr_t)s.rx;
    check(fn() == 42, "sealed slot executes (returns 42)");
  }
  ndb_jit_codemem_free(mem, &s);
  check(s._slot == NULL && s.rw == NULL, "free clears the handle");
  ndb_jit_codemem_destroy(mem);
}

static void test_distinct_then_reuse(void) {
  NdbJitCodeMem *mem = ndb_jit_codemem_create(0);

  NdbJitCodeSlot a, b;
  int rc = (compile_ret(mem, 7, &a) == 0) && (compile_ret(mem, 9, &b) == 0);
  check(rc, "two slots compile");
  void *rw_a = a.rw, *rw_b = b.rw;
  if (rc) {
    ret_fn_t fa = (ret_fn_t)(uintptr_t)a.rx;
    ret_fn_t fb = (ret_fn_t)(uintptr_t)b.rx;
    check(rw_a != rw_b, "distinct slots have distinct addresses");
    check(fa() == 7 && fb() == 9, "distinct slots run distinct code");
  }
  check(ndb_jit_codemem_live_slots(mem) == 2, "live == 2 before free");

  size_t reserved_before = ndb_jit_codemem_reserved_bytes(mem);
  ndb_jit_codemem_free(mem, &a);
  ndb_jit_codemem_free(mem, &b);
  check(ndb_jit_codemem_live_slots(mem) == 0, "live == 0 after free");

  /* Re-alloc the same class: must recycle the freed slots, not grow. */
  NdbJitCodeSlot c, d;
  check(compile_ret(mem, 11, &c) == 0 && compile_ret(mem, 13, &d) == 0,
        "re-alloc after free succeeds");
  check(ndb_jit_codemem_reserved_bytes(mem) == reserved_before,
        "re-alloc reuses freed slots (no new slab)");
  int recycled = (c.rw == rw_a || c.rw == rw_b) && (d.rw == rw_a || d.rw == rw_b);
  check(recycled, "recycled slots come from the freed set");
  ndb_jit_codemem_free(mem, &c);
  ndb_jit_codemem_free(mem, &d);
  ndb_jit_codemem_destroy(mem);
}

static void test_accounting(void) {
  NdbJitCodeMem *mem = ndb_jit_codemem_create(0);
  NdbJitCodeSlot s[3];
  for (int i = 0; i < 3; ++i) {
    (void)ndb_jit_codemem_alloc(mem, 200, &s[i]); /* class 256 */
  }
  check(ndb_jit_codemem_live_slots(mem) == 3, "accounting: live == 3");
  check(ndb_jit_codemem_inuse_bytes(mem) == 3u * 256u,
        "accounting: in-use == 3 * 256");
  ndb_jit_codemem_free(mem, &s[1]);
  check(ndb_jit_codemem_live_slots(mem) == 2, "accounting: live == 2 after one free");
  check(ndb_jit_codemem_inuse_bytes(mem) == 2u * 256u,
        "accounting: in-use == 2 * 256 after one free");
  ndb_jit_codemem_free(mem, &s[0]);
  ndb_jit_codemem_free(mem, &s[2]);
  ndb_jit_codemem_destroy(mem);
}

static void test_oom_cap(void) {
  /* Cap at exactly one 64 KB slab. Class 8192 -> 8 slots per slab, so
   * the 9th alloc must force a second slab and hit the cap. */
  NdbJitCodeMem *mem = ndb_jit_codemem_create(64u * 1024u);
  NdbJitCodeSlot s[8];
  int all_ok = 1;
  for (int i = 0; i < 8; ++i) {
    if (ndb_jit_codemem_alloc(mem, 5000, &s[i]) != 0) all_ok = 0;
  }
  check(all_ok, "OOM: first slab's 8 slots all allocate");

  NdbJitCodeSlot over;
  check(ndb_jit_codemem_alloc(mem, 5000, &over) == -1,
        "OOM: alloc past the cap returns -1");

  /* Freeing one makes room again without growing. */
  ndb_jit_codemem_free(mem, &s[0]);
  NdbJitCodeSlot again;
  check(ndb_jit_codemem_alloc(mem, 5000, &again) == 0,
        "OOM: alloc succeeds again after a free");
  ndb_jit_codemem_free(mem, &again);
  for (int i = 1; i < 8; ++i) ndb_jit_codemem_free(mem, &s[i]);
  ndb_jit_codemem_destroy(mem);
}

static void test_seal_validation(void) {
  NdbJitCodeMem *mem = ndb_jit_codemem_create(0);
  NdbJitCodeSlot s;
  check(ndb_jit_codemem_alloc(mem, 64, &s) == 0, "validation: alloc ok");
  s.length = 0;
  check(ndb_jit_codemem_seal(mem, &s) == -1, "seal rejects length == 0");
  s.length = s.capacity + 1;
  check(ndb_jit_codemem_seal(mem, &s) == -1, "seal rejects length > capacity");
  ndb_jit_codemem_free(mem, &s);
  ndb_jit_codemem_destroy(mem);
}

static void test_global_singleton(void) {
  NdbJitCodeMem *a = ndb_jit_codemem_global();
  NdbJitCodeMem *b = ndb_jit_codemem_global();
  check(a != NULL && a == b, "global() returns a stable singleton");
  /* Use it once to prove it's functional; never destroyed (node-lived). */
  NdbJitCodeSlot s;
  int rc = compile_ret(a, 99, &s);
  check(rc == 0, "global manager compiles");
  if (rc == 0) {
    ret_fn_t fn = (ret_fn_t)(uintptr_t)s.rx;
    check(fn() == 99, "global manager's slot executes");
    ndb_jit_codemem_free(a, &s);
  }
}

int main(void) {
  printf("RONDB-1056 jit_codemem unit tests\n");
  test_create_destroy();
  test_class_selection();
  test_seal_execute();
  test_distinct_then_reuse();
  test_accounting();
  test_oom_cap();
  test_seal_validation();
  test_global_singleton();
  printf("\n%d passed, %d failed\n", n_pass, n_fail);
  return n_fail == 0 ? 0 : 1;
}
