/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 2 — copy-and-patch JIT engine, cross-arch.
 *
 * Two-pass compile:
 *   Pass 1: walk the bytecode, accumulate the per-pc byte offset
 *           where each opcode's stencil will live in the emitted
 *           blob. Reserve arena bytes.
 *   Pass 2: for each opcode:
 *             - memcpy the stencil's bytes
 *             - patch each hole with op->{a,b,c,imm} or PC-rel disp
 *             - if branch: queue a forward fixup for the taken disp
 *             - drain any forward fixups whose source pc has now
 *               been emitted (the immediate fall-through to the
 *               next stencil is patched directly without going
 *               through the queue)
 *
 * Phase 2 supports both x86_64 (relocation-driven 32-bit operand
 * holes; `mov reg32, imm32` patch sites) and aarch64 (magic-byte
 * holes; movz/movk/movk/movk chains where each of the 4 instructions
 * carries one imm16 slice). The generated headers
 * (stencils_x86_64.h, stencils_arm64.h) contain the per-arch byte
 * arrays + Hole tables.
 *
 * The engine is arch-uniform at the high level — same Pass 1 / Pass 2
 * structure, same fixup queue. Arch differences are encapsulated in
 * three inline helpers: kPreamble[] bytes, patch_operand(), and
 * patch_branch_disp().
 */

#include "jit1.h"

#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(__x86_64__)
#  include "stencils_x86_64.h"
#elif defined(__aarch64__)
#  include "stencils_arm64.h"
#else
#  error "RONDB-1056 jit1: unsupported architecture"
#endif

/* ------------------------------------------------------------------ */
/* Per-arch preamble.                                                 */
/*                                                                    */
/* The preamble bridges the public JitEntry calling convention        */
/* (regular C ABI) into the preserve_none convention used inside the  */
/* stencil chain. It saves whatever caller-saved registers the        */
/* preserve_none ABI repurposes, then loads the state pointer into    */
/* the preserve_none "first-int-arg" register.                        */
/*                                                                    */
/* The matching teardown is in op_skip / op_exit, whose stencils are  */
/* overridden by the extractor with the engine-required terminator    */
/* bytes (kX86Terminator / kArm64Terminator in extract_stencils.c).   */
/* ------------------------------------------------------------------ */

#if defined(__x86_64__)
/*   00: 41 54           push r12          ; save caller's r12
 *   02: 48 83 ec 08     sub  rsp, 8       ; stencil-entry stack parity
 *   06: 49 89 fc        mov  r12, rdi     ; r12 = state
 *
 * A regular SysV C call enters with rsp%16 == 8. The first push would
 * make stencil entry rsp%16 == 0, but clang-generated cold-call
 * stencils assume ordinary function-entry parity and push once before
 * calling C helpers. Keep stencil entry at rsp%16 == 8 so those helper
 * calls are aligned. op_skip/op_exit undo this with add rsp,8; pop r12;
 * ret. */
static const uint8_t kPreamble[] = {
  0x41, 0x54,
  0x48, 0x83, 0xec, 0x08,
  0x49, 0x89, 0xfc
};

#elif defined(__aarch64__)
/*   00: a9bf7bf4     stp  x20, x30, [sp, #-16]!  ; save x20 + LR
 *   04: aa0003f4     mov  x20, x0               ; x20 = state */
static const uint8_t kPreamble[] = {
  0xf4, 0x7b, 0xbf, 0xa9,
  0xf4, 0x03, 0x00, 0xaa,
};
#endif

#define PREAMBLE_SIZE ((uint32_t)sizeof(kPreamble))

/* ------------------------------------------------------------------ */
/* Internal types.                                                    */
/* ------------------------------------------------------------------ */

/* A queued forward branch — the patch site is emitted but the
 * target hasn't been emitted yet. Resolved when the emitter's pc
 * advances to `target_pc` and we know the target's byte offset. */
typedef struct {
  uint16_t target_pc;        /* bytecode pc of the branch target */
  uint32_t patch_off;        /* byte offset in the arena blob where the
                                branch instruction (x86_64: rel32 disp;
                                aarch64: full b imm26 word) starts */
} Fixup;

/* One forward branch fixup per bytecode op is the practical maximum. */
#define J1_MAX_FIXUPS  BC_MAX_OPS

struct Jit1Prog {
  const uint8_t *rx_entry;      /* RX-mapping pointer to start of compiled blob */
  size_t         emitted;       /* total bytes emitted */
  NdbJitCodeMem *mem;           /* manager the slot came from (for jit1_free) */
  NdbJitCodeSlot slot;          /* code-memory slot backing the blob */

  /* RONDB-1056 Phase 8 — crash-diagnosis sidecar. Captured at compile,
   * read ONLY by jit1_describe_pc (a SIGSEGV handler / DUMP), never on the
   * per-row execution path. Best-effort: op_kind/pc_off may be NULL if the
   * small allocations failed (the program still runs). */
  uint16_t       n_ops;         /* opcode count (0 when sidecar absent) */
  uint8_t       *op_kind;       /* [n_ops] OpKind per bytecode pc, or NULL */
  uint32_t      *pc_off;        /* [n_ops+1] byte offset per pc; [n_ops]=total */
  struct Jit1Prog *reg_next;    /* node-global live-program registry (intrusive) */
  struct Jit1Prog *reg_prev;
};

/* ------------------------------------------------------------------ */
/* Live-program registry (for crash diagnosis).                       */
/*                                                                    */
/* An intrusive doubly-linked list of every live Jit1Prog. Mutated    */
/* under a mutex by compile/free; traversed WITHOUT the lock by        */
/* jit1_describe_pc, which runs in a SIGSEGV handler where taking a    */
/* lock is unsafe. The traversal reads only immutable post-seal fields */
/* (rx range, op_kind/pc_off); racing a concurrent free during a crash */
/* is accepted (rare, and the alternative — a lock in a signal handler */
/* — is worse).                                                       */
/* ------------------------------------------------------------------ */

static pthread_mutex_t g_jit1_reg_mtx = PTHREAD_MUTEX_INITIALIZER;
static Jit1Prog *g_jit1_reg_head = NULL;

static void jit1_registry_add(Jit1Prog *p) {
  pthread_mutex_lock(&g_jit1_reg_mtx);
  p->reg_prev = NULL;
  p->reg_next = g_jit1_reg_head;
  if (g_jit1_reg_head != NULL) g_jit1_reg_head->reg_prev = p;
  g_jit1_reg_head = p;
  pthread_mutex_unlock(&g_jit1_reg_mtx);
}

static void jit1_registry_remove(Jit1Prog *p) {
  pthread_mutex_lock(&g_jit1_reg_mtx);
  if (p->reg_prev != NULL) {
    p->reg_prev->reg_next = p->reg_next;
  } else if (g_jit1_reg_head == p) {
    g_jit1_reg_head = p->reg_next;
  }
  if (p->reg_next != NULL) p->reg_next->reg_prev = p->reg_prev;
  p->reg_next = NULL;
  p->reg_prev = NULL;
  pthread_mutex_unlock(&g_jit1_reg_mtx);
}

/* ------------------------------------------------------------------ */
/* Pure helpers — no arena, no state.                                 */
/* ------------------------------------------------------------------ */

/* Read the operand value to bake into a hole, given the kind and
 * the current Op. Returns the i64 value; arch helpers narrow as
 * needed. */
static inline int64_t hole_value_from_op(uint8_t kind, const Op *op) {
  switch (kind) {
    case HK_OP_A:   return (int64_t)op->a;
    case HK_OP_B:   return (int64_t)op->b;
    case HK_OP_C:   return (int64_t)op->c;
    case HK_OP_IMM: return op->imm;
    default:        return 0;   /* branch holes handled separately */
  }
}

static inline int op_has_overflow_target(uint8_t kind) {
  switch (kind) {
    case OP_ADD_INT_INT_CHECKED:
    case OP_MINUS_INT_INT_CHECKED:
    case OP_MUL_INT_INT_CHECKED:
    case OP_SUM_BIGINT_CHECKED:
    /* Phase 5C-2: every f64 op with a non-finite check carries the
     * shared HOLE_F64_OVF_TGT, patched from op->d like the checked
     * int arithmetic. (MIN/MAX_F64 have no arithmetic — no target.) */
    case OP_ADD_F64:
    case OP_MINUS_F64:
    case OP_MUL_F64:
    case OP_DIV_F64:
    case OP_SUM_F64:
    /* Phase 5C-3: u64 SUM carry check → HOLE_SUM_OVF_TGT via op->d. */
    case OP_SUM_U64_CHECKED:
    /* Phase 5C-4: unsigned checked arithmetic — per-family OVF
     * targets, patched from op->d. */
    case OP_ADD_U64_CHECKED:
    case OP_MINUS_U64_CHECKED:
    case OP_MUL_U64_CHECKED:
    /* Phase 5E-2: signed integer division's INT64_MIN / -1 overflow →
     * HOLE_DIV_OVF_TGT via op->d. MOD and the u64 pair carry no
     * overflow hole (remainders can't overflow; u64 divide can't). */
    case OP_DIV_INT_CHECKED:
      return 1;
    default:
      return 0;
  }
}

/* ------------------------------------------------------------------ */
/* Per-arch hole-patching helpers.                                    */
/*                                                                    */
/* On x86_64 each operand hole is independent — patch a 32-bit LE     */
/* value at the hole's byte offset. `slot` is unused.                 */
/*                                                                    */
/* On aarch64 each operand hole is one of four movz/movk imm16        */
/* slots. `slot` ∈ {0,1,2,3} selects which 16 bits of the value get   */
/* emitted at this site. The engine increments `slot_counter[kind]`   */
/* per hole within a stencil so the four entries with the same kind   */
/* receive slot=0,1,2,3 in byte-offset order (which matches clang's   */
/* movz-then-3×movk emission order within a chain).                   */
/* ------------------------------------------------------------------ */

#if defined(__x86_64__)

/* Write a 4-byte little-endian value at `dst`. */
static inline void put_u32_le(uint8_t *dst, uint32_t v) {
  dst[0] = (uint8_t)(v       & 0xff);
  dst[1] = (uint8_t)((v >> 8)  & 0xff);
  dst[2] = (uint8_t)((v >> 16) & 0xff);
  dst[3] = (uint8_t)((v >> 24) & 0xff);
}

static inline void put_u64_le(uint8_t *dst, uint64_t v) {
  put_u32_le(dst, (uint32_t)(v & 0xffffffffu));
  put_u32_le(dst + 4, (uint32_t)(v >> 32));
}

static inline void patch_operand(uint8_t *site, uint8_t slot, int64_t value) {
  (void)slot;
  /* Phase 1+2 x86_64 stencils use 32-bit operand sites: `mov reg32,
   * imm32` (zero-extended for register-index holes), `mov [...], imm32`
   * (sign-extended for HK_OP_IMM in op_load_const_int's qword store).
   * We narrow to int32 — out-of-range immediates would surprise the
   * engine but are out of scope for the Phase 1 microbench's value
   * domain. */
  put_u32_le(site, (uint32_t)(int32_t)value);
}

/* x86_64 PC-relative-32 is relative to the END of the rel32 field
 * (i.e., next instruction). disp = target_off - patch_site_off - 4. */
static inline void patch_branch_disp(uint8_t *site, int32_t byte_disp) {
  put_u32_le(site, (uint32_t)(byte_disp - 4));
}

#elif defined(__aarch64__)

/* Read/modify/write 4 LE bytes at `site` using the (mask, value)
 * idiom common to imm16 / imm26 patching. */
static inline void rmw_insn_word(uint8_t *site, uint32_t mask_clear, uint32_t bits_set) {
  uint32_t w = (uint32_t)site[0]            |
               ((uint32_t)site[1] << 8)     |
               ((uint32_t)site[2] << 16)    |
               ((uint32_t)site[3] << 24);
  w = (w & ~mask_clear) | (bits_set & mask_clear);
  site[0] = (uint8_t)(w & 0xff);
  site[1] = (uint8_t)((w >> 8)  & 0xff);
  site[2] = (uint8_t)((w >> 16) & 0xff);
  site[3] = (uint8_t)((w >> 24) & 0xff);
}

/* aarch64 movz/movk imm16 lives at instruction bits 5..20.
 * `slot` selects which 16-bit slice of `value` to bake in:
 *   slot=0 → bits 0..15, slot=1 → bits 16..31, ... */
static inline void patch_operand(uint8_t *site, uint8_t slot, int64_t value) {
  uint16_t imm16 = (uint16_t)((uint64_t)value >> ((uint32_t)slot * 16u));
  rmw_insn_word(site,
                (uint32_t)0xffff << 5,
                (uint32_t)imm16  << 5);
}

/* aarch64 b imm26 displacement is in 4-byte units, signed, relative
 * to the start of the b instruction (PC of that instruction). */
static inline void patch_branch_disp(uint8_t *site, int32_t byte_disp) {
  /* Arithmetic shift — preserve sign. The mask + final cast handle
   * out-of-range values harmlessly: a 26-bit truncation flips a
   * forward branch into a backward one, but our Phase 1+2 forward-
   * only programs are well under ±128 MB, so this is purely
   * defensive. */
  int32_t disp_words = byte_disp >> 2;
  rmw_insn_word(site,
                (uint32_t)0x03ffffff,
                (uint32_t)disp_words);
}

#endif

/* ------------------------------------------------------------------ */
/* Helper registry (Phase 4 RONDB-1056).                              */
/*                                                                    */
/* Cold-call stencils (HK_COLDCALL holes) reference extern helpers    */
/* by symbol name; the engine resolves them at compile time through  */
/* this static table. Registration is one-shot at engine init from   */
/* the C++ glue layer (DbtupJitGlue::dbtup_jit_register_helpers).    */
/*                                                                    */
/* Single-threaded compile per RonSQL §10.1, but the table is        */
/* register-once and never modified after init, so concurrent         */
/* lookups by future multi-threaded compile would be safe-by-       */
/* construction without locks. */
/* ------------------------------------------------------------------ */

#define J1_MAX_HELPERS 16

typedef struct {
  const char  *name;
  JitHelperFn  fn;
} JitHelperEntry;

static JitHelperEntry g_helpers[J1_MAX_HELPERS];
static int g_n_helpers = 0;

int jit1_register_helper(const char *name, JitHelperFn fn) {
  if (name == NULL || fn == NULL) {
    errno = EINVAL;
    return -1;
  }
  /* Idempotent re-register: same name + same fn is a no-op. Same
   * name + different fn is rejected — that's a bug worth catching. */
  for (int i = 0; i < g_n_helpers; ++i) {
    if (strcmp(g_helpers[i].name, name) == 0) {
      if (g_helpers[i].fn == fn) return 0;
      errno = EEXIST;
      return -1;
    }
  }
  if (g_n_helpers >= J1_MAX_HELPERS) {
    errno = ENOSPC;
    return -1;
  }
  g_helpers[g_n_helpers].name = name;
  g_helpers[g_n_helpers].fn   = fn;
  g_n_helpers++;
  return 0;
}

JitHelperFn jit1_lookup_helper(const char *name) {
  if (name == NULL) return NULL;
  for (int i = 0; i < g_n_helpers; ++i) {
    if (strcmp(g_helpers[i].name, name) == 0) return g_helpers[i].fn;
  }
  return NULL;
}

/* ------------------------------------------------------------------ */
/* Per-phase timing helpers.                                          */
/* ------------------------------------------------------------------ */

#if defined(CLOCK_MONOTONIC_RAW)
#define J1_CLOCK CLOCK_MONOTONIC_RAW
#else
#define J1_CLOCK CLOCK_MONOTONIC
#endif

static inline uint64_t j1_now_ns(void) {
  struct timespec ts;
  clock_gettime(J1_CLOCK, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ------------------------------------------------------------------ */
/* Admission walk.                                                    */
/*                                                                    */
/* A single forward linear pass over prog->ops, executed before any   */
/* arena allocation or per-phase timing. Produces a yes/no verdict +  */
/* enough context to log the rejection reason. Cheap rejection: a     */
/* malformed program never touches the arena.                         */
/*                                                                    */
/* Single-threaded compile per RonSQL §10.1, but the sidecar is       */
/* _Thread_local for defense in depth against future use cases.       */
/* ------------------------------------------------------------------ */

static _Thread_local Jit1AdmitError g_last_admit = {
  .reason = JIT_ADMIT_OK,
};

const Jit1AdmitError *jit1_last_admit_error(void) {
  return &g_last_admit;
}

/* Branch-class opcodes have an HK_BRANCH_TAKE hole and an op->c
 * forward target. The predicate lives in bytecode1.h so the
 * interpreter, builder, and JIT engine all consult the same source
 * of truth. */

static Jit1AdmitReason admit_program(const Program *prog) {
  if (prog->n_ops == 0) {
    g_last_admit = (Jit1AdmitError){ .reason = JIT_ADMIT_EMPTY_PROG };
    return JIT_ADMIT_EMPTY_PROG;
  }
  if (prog->n_ops > BC_MAX_OPS) {
    g_last_admit = (Jit1AdmitError){ .reason = JIT_ADMIT_PROG_TOO_LARGE };
    return JIT_ADMIT_PROG_TOO_LARGE;
  }

  for (uint16_t pc = 0; pc < prog->n_ops; ++pc) {
    const Op *op   = &prog->ops[pc];
    uint8_t  kind  = op->kind;

    if (kind == 0 || kind > OP_KIND_MAX) {
      g_last_admit = (Jit1AdmitError){
        .reason         = JIT_ADMIT_INVALID_KIND,
        .offending_pc   = pc,
        .offending_kind = kind,
      };
      return JIT_ADMIT_INVALID_KIND;
    }
    if (g_stencils[kind].n_bytes == 0) {
      g_last_admit = (Jit1AdmitError){
        .reason         = JIT_ADMIT_UNSUPPORTED_OP,
        .offending_pc   = pc,
        .offending_kind = kind,
      };
      return JIT_ADMIT_UNSUPPORTED_OP;
    }
    if (bc_op_is_branch(kind)) {
      if (op->c <= pc) {
        g_last_admit = (Jit1AdmitError){
          .reason           = JIT_ADMIT_BACKWARD_BRANCH,
          .offending_pc     = pc,
          .offending_target = op->c,
          .offending_kind   = kind,
        };
        return JIT_ADMIT_BACKWARD_BRANCH;
      }
      if (op->c >= prog->n_ops) {
        g_last_admit = (Jit1AdmitError){
          .reason           = JIT_ADMIT_BRANCH_OOR,
          .offending_pc     = pc,
          .offending_target = op->c,
          .offending_kind   = kind,
        };
        return JIT_ADMIT_BRANCH_OOR;
      }
    }
    if (op_has_overflow_target(kind)) {
      if (op->d <= pc) {
        g_last_admit = (Jit1AdmitError){
          .reason           = JIT_ADMIT_BACKWARD_BRANCH,
          .offending_pc     = pc,
          .offending_target = op->d,
          .offending_kind   = kind,
        };
        return JIT_ADMIT_BACKWARD_BRANCH;
      }
      if (op->d >= prog->n_ops) {
        g_last_admit = (Jit1AdmitError){
          .reason           = JIT_ADMIT_BRANCH_OOR,
          .offending_pc     = pc,
          .offending_target = op->d,
          .offending_kind   = kind,
        };
        return JIT_ADMIT_BRANCH_OOR;
      }
    }
  }

  g_last_admit = (Jit1AdmitError){ .reason = JIT_ADMIT_OK };
  return JIT_ADMIT_OK;
}

/* ------------------------------------------------------------------ */
/* Compile — entry point.                                             */
/* ------------------------------------------------------------------ */

Jit1Prog *jit1_compile(NdbJitCodeMem *mem,
                       const Program *prog,
                       Jit1Timing *out_timing) {
  if (out_timing) memset(out_timing, 0, sizeof(*out_timing));

  if (!mem || !prog) {
    /* Null inputs aren't an admission rubric mismatch; leave the
     * sidecar untouched so a prior reason isn't overwritten by a
     * less informative result. */
    errno = EINVAL;
    return NULL;
  }

  /* Admission walk runs before any allocation. Rejected programs
   * cost only the walk's work and never touch the arena. */
  if (admit_program(prog) != JIT_ADMIT_OK) {
    errno = EINVAL;
    return NULL;
  }

  uint64_t t_entry = out_timing ? j1_now_ns() : 0;
  uint64_t t_pass1_end = 0, t_alloc_end = 0, t_emit_end = 0,
           t_seal_end = 0, t_handle_end = 0;

  /* ---------------------------------------------------------------- */
  /* Pass 1: walk bytecode, compute per-pc byte offsets, total size.  */
  /*                                                                  */
  /* Layout of the emitted blob:                                      */
  /*   [PREAMBLE_SIZE bytes — bridges C ABI -> preserve_none ABI]     */
  /*   [stencil for pc=0]                                             */
  /*   [stencil for pc=1]                                             */
  /*   ...                                                            */
  /*                                                                  */
  /* pc_byte_off[pc] is the byte offset where pc's stencil starts —   */
  /* everything is shifted by PREAMBLE_SIZE relative to "stencils     */
  /* alone". Branch displacements are relative differences so they    */
  /* remain correct under the shift.                                  */
  /* ---------------------------------------------------------------- */

  uint32_t pc_byte_off[BC_MAX_OPS + 1];   /* +1 for "end of program" */
  uint32_t total = PREAMBLE_SIZE;
  for (uint16_t pc = 0; pc < prog->n_ops; ++pc) {
    pc_byte_off[pc] = total;
    /* admit_program() guarantees kind ∈ [1, OP_KIND_MAX] and
     * non-empty stencil; the asserts catch a broken upstream
     * invariant in debug builds and are no-ops in release. */
    uint8_t kind = prog->ops[pc].kind;
    assert(kind > 0 && kind <= OP_KIND_MAX);
    const Stencil *st = &g_stencils[kind];
    assert(st->n_bytes > 0);
    total += st->n_bytes;
  }
  pc_byte_off[prog->n_ops] = total;

  if (out_timing) t_pass1_end = j1_now_ns();

  /* ---------------------------------------------------------------- */
  /* Reserve a code-memory slot for the blob bytes. The slot carries   */
  /* both its writable base (emit here) and its executable alias       */
  /* (callable after seal); the Jit1Prog handle is malloc'd separately */
  /* and records the slot so jit1_free() can return it to the manager. */
  /* ---------------------------------------------------------------- */

  NdbJitCodeSlot slot;
  if (ndb_jit_codemem_alloc(mem, total, &slot) != 0) {
    errno = ENOMEM;   /* cap reached, or blob > largest size class */
    return NULL;
  }
  uint8_t *blob_rw = (uint8_t *)slot.rw;

  if (out_timing) t_alloc_end = j1_now_ns();

  /* Emit the entry preamble. */
  memcpy(blob_rw, kPreamble, PREAMBLE_SIZE);

  /* ---------------------------------------------------------------- */
  /* Pass 2: emit each stencil, patch holes, queue + drain fixups.   */
  /* ---------------------------------------------------------------- */

  Fixup fixups[J1_MAX_FIXUPS];
  uint8_t n_fixups = 0;

  /* Per-kind slot counter — incremented per operand hole within a
   * stencil. On aarch64 this drives the four-instruction movz/movk
   * chain's slot selection; on x86_64 it stays at 0 and is unused. */
  uint8_t slot_counter[HK_KIND_MAX + 1];

  for (uint16_t pc = 0; pc < prog->n_ops; ++pc) {
    const Op *op = &prog->ops[pc];
    const Stencil *st = &g_stencils[op->kind];
    uint32_t this_off = pc_byte_off[pc];
    uint32_t next_off = pc_byte_off[pc + 1];

    /* memcpy stencil bytes. */
    memcpy(blob_rw + this_off, st->bytes, st->n_bytes);

    /* Reset the slot counter for this stencil. */
    memset(slot_counter, 0, sizeof(slot_counter));

    /* Patch each hole. */
    for (uint8_t h = 0; h < st->n_holes; ++h) {
      const Hole *hole = &st->holes[h];
      uint8_t *patch = blob_rw + this_off + hole->byte_offset;

      switch (hole->kind) {
        case HK_OP_A:
        case HK_OP_B:
        case HK_OP_C:
        case HK_OP_IMM: {
          int64_t v = hole_value_from_op(hole->kind, op);
          /* width discriminator (aarch64 only — x86_64 always
           * has width=4 from the existing relocation path):
           *   width=1: Phase 4.7 LDR/STR imm12 fold. Operand
           *            value goes into bits 21..10 of one
           *            LDR/STR instruction. Slot counter
           *            untouched.
           *   width=2: Phase 4.5 narrow MOVZ. slot=0, slot
           *            counter untouched.
           *   width=4: wide chain. slot=0..3 via slot_counter. */
#if defined(__aarch64__)
          if (hole->width == 1) {
            /* Phase 4.7 imm12 fold. */
            rmw_insn_word(patch,
                          (uint32_t)0xFFFu << 10,
                          ((uint32_t)(int32_t)v & 0xFFFu) << 10);
          } else if (hole->width == 2) {
            patch_operand(patch, 0, v);
          } else {
            uint8_t slot = slot_counter[hole->kind]++;
            patch_operand(patch, slot, v);
          }
#else  /* x86_64 */
          (void)0; /* width is always 4 on x86_64; the
                    * patch_operand call below handles all
                    * Phase 4-era operand holes uniformly. */
          if (hole->width == 2) {
            patch_operand(patch, 0, v);
          } else {
            uint8_t slot = slot_counter[hole->kind]++;
            patch_operand(patch, slot, v);
          }
#endif
          break;
        }
        case HK_BRANCH_FALL: {
          /* Fall-through goes to the immediately-following stencil. */
          uint32_t patch_site_off = this_off + hole->byte_offset;
          int32_t  byte_disp = (int32_t)next_off - (int32_t)patch_site_off;
          patch_branch_disp(patch, byte_disp);
          break;
        }
        case HK_COLDCALL: {
          /* Phase 4 RONDB-1056: call to an extern helper. On x86_64
           * the extractor expands helper calls to an absolute
           * movabs/call sequence. On aarch64, the helper lives outside
           * the JIT arena, so the PC-relative displacement must be
           * computed using the patch site's eventual RX address, not
           * its RW address. */
          if (hole->helper_name == NULL) {
            errno = EINVAL;   /* extractor bug — HK_COLDCALL without name */
            goto fail;
          }
          JitHelperFn helper = jit1_lookup_helper(hole->helper_name);
          if (helper == NULL) {
            errno = ENOENT;   /* helper not registered before compile */
            goto fail;
          }
#if defined(__x86_64__)
          if (hole->width == 8) {
            /* x86_64 cold-call stencils use:
             *   movabs rax, imm64
             *   call   rax
             *
             * Linux can map the RX code memory far away from the main
             * executable, so a rel32 call is not generally safe. */
            put_u64_le(patch, (uint64_t)(uintptr_t)helper);
          } else {
            errno = EINVAL;
            goto fail;
          }
#else
          uint32_t patch_site_off = this_off + hole->byte_offset;
          /* The patch site's executable address is the slot's rx base
           * plus the same offset (the slot's rw and rx views are
           * offset-aliased), known before seal. */
          const void *rx_site = (const uint8_t *)slot.rx + patch_site_off;
          int64_t byte_disp = (int64_t)(intptr_t)helper -
                               (int64_t)(uintptr_t)rx_site;
          patch_branch_disp(patch, (int32_t)byte_disp);
#endif
          break;
        }
        case HK_BRANCH_TAKE: {
          /* Taken goes to op->c bytecode pc — forward by construction.
           * The target hasn't been emitted yet, so queue a fixup.
           * admit_program() already proved op->c > pc and
           * op->c < prog->n_ops; assert defends in debug builds. */
          assert(op->c > pc && op->c < prog->n_ops);
          if (n_fixups >= J1_MAX_FIXUPS) {
            errno = ENOSPC;
            goto fail;
          }
          fixups[n_fixups++] = (Fixup){
            .target_pc = op->c,
            .patch_off = this_off + hole->byte_offset,
          };
          /* Leave the patch site bytes as the stencil emitted; the
           * displacement field will be cleared + filled by
           * patch_branch_disp during the drain. */
          break;
        }
        case HK_OVERFLOW_TAKE: {
          assert(op->d > pc && op->d < prog->n_ops);
          if (n_fixups >= J1_MAX_FIXUPS) {
            errno = ENOSPC;
            goto fail;
          }
          fixups[n_fixups++] = (Fixup){
            .target_pc = op->d,
            .patch_off = this_off + hole->byte_offset,
          };
          break;
        }
        default:
          errno = EINVAL;
          goto fail;
      }
    }

    /* Drain any fixups targeting this newly-emitted pc. The branch
     * stencil's HK_BRANCH_TAKE queues its fixup *before* this drain
     * runs (above), but its target_pc is op->c > pc, so it won't drain
     * here. Drains happen when we reach pc == target_pc later. */
    for (uint8_t f = 0; f < n_fixups;) {
      if (fixups[f].target_pc == pc) {
        uint32_t patch_site_off = fixups[f].patch_off;
        int32_t  byte_disp = (int32_t)this_off - (int32_t)patch_site_off;
        patch_branch_disp(blob_rw + patch_site_off, byte_disp);
        /* Remove by swap-with-last. */
        fixups[f] = fixups[--n_fixups];
      } else {
        ++f;
      }
    }
  }

  if (n_fixups != 0) {
    errno = EINVAL;     /* a branch target was never reached */
    goto fail;
  }

  if (out_timing) t_emit_end = j1_now_ns();

  /* ---------------------------------------------------------------- */
  /* Seal the slot and obtain the RX-mapping (callable) pointer.      */
  /* ---------------------------------------------------------------- */

  slot.length = total;
  if (ndb_jit_codemem_seal(mem, &slot) != 0) {
    errno = EFAULT;
    goto fail;
  }
  const void *rx = slot.rx;

  if (out_timing) t_seal_end = j1_now_ns();

  Jit1Prog *handle = (Jit1Prog *)calloc(1, sizeof(*handle));
  if (!handle) {
    errno = ENOMEM;
    goto fail;
  }
  handle->rx_entry = (const uint8_t *)rx;
  handle->emitted  = total;
  handle->mem      = mem;
  handle->slot     = slot;

  /* Capture the crash-diagnosis sidecar (per-opcode kind + byte offset)
   * and add the program to the live registry so a faulting PC can be
   * mapped back to a bytecode opcode. Best-effort: if either small
   * allocation fails the program still runs, just without opcode detail. */
  handle->n_ops    = 0;
  handle->op_kind  = NULL;
  handle->pc_off   = NULL;
  handle->reg_next = NULL;
  handle->reg_prev = NULL;
  {
    uint16_t n = prog->n_ops;
    uint8_t  *ok = (uint8_t *)malloc(n ? n : 1);
    uint32_t *po = (uint32_t *)malloc((size_t)(n + 1) * sizeof(uint32_t));
    if (ok != NULL && po != NULL) {
      for (uint16_t i = 0; i < n; ++i) ok[i] = prog->ops[i].kind;
      for (uint16_t i = 0; i <= n; ++i) po[i] = pc_byte_off[i];
      handle->n_ops   = n;
      handle->op_kind = ok;
      handle->pc_off  = po;
    } else {
      free(ok);
      free(po);
    }
  }
  jit1_registry_add(handle);

  if (out_timing) {
    t_handle_end = j1_now_ns();
    out_timing->pass1_ns  = t_pass1_end  - t_entry;
    out_timing->alloc_ns  = t_alloc_end  - t_pass1_end;
    out_timing->emit_ns   = t_emit_end   - t_alloc_end;
    out_timing->seal_ns   = t_seal_end   - t_emit_end;
    out_timing->handle_ns = t_handle_end - t_seal_end;
    out_timing->total_ns  = t_handle_end - t_entry;
  }

  return handle;

fail:
  /* Any failure after the slot was reserved returns it to the manager
   * so a compile error never leaks code memory. errno is preserved. */
  {
    int saved = errno;
    ndb_jit_codemem_free(mem, &slot);
    errno = saved;
  }
  return NULL;
}

JitEntry jit1_entry(const Jit1Prog *prog) {
  if (!prog) return NULL;
  /* The compiled blob's first byte is the entry point. Cast through
   * uintptr_t to satisfy strict-aliasing / function-pointer-from-
   * data-pointer pedantic warnings. */
  return (JitEntry)(uintptr_t)prog->rx_entry;
}

size_t jit1_emitted_size(const Jit1Prog *prog) {
  return prog ? prog->emitted : 0;
}

void jit1_free(Jit1Prog *prog) {
  if (prog == NULL) return;
  /* Remove from the crash-diagnosis registry before freeing anything, so
   * a concurrent jit1_describe_pc never observes a half-freed program. */
  jit1_registry_remove(prog);
  free(prog->op_kind);
  free(prog->pc_off);
  /* Return the code-memory slot to its manager, then free the handle.
   * After this the entry pointer is dangling — the slot may be reused. */
  ndb_jit_codemem_free(prog->mem, &prog->slot);
  free(prog);
}

int jit1_describe_pc(const void *pc, char *buf, size_t buflen) {
  if (buf == NULL || buflen == 0) return 0;
  const uint8_t *p = (const uint8_t *)pc;

  /* Lock-free traversal — see the registry comment above. We read only
   * post-seal-immutable fields, so a SIGSEGV handler can call this safely. */
  unsigned idx = 0;
  for (const Jit1Prog *prog = g_jit1_reg_head; prog != NULL;
       prog = prog->reg_next, ++idx) {
    const uint8_t *lo = prog->rx_entry;
    const uint8_t *hi = lo + prog->emitted;
    if (p < lo || p >= hi) continue;

    size_t off = (size_t)(p - lo);
    if (prog->pc_off != NULL && prog->op_kind != NULL && prog->n_ops > 0) {
      if (off < prog->pc_off[0]) {
        snprintf(buf, buflen,
                 "JIT-CRASH: pc=%p in JIT blob #%u [%p..%p) byte_off=%zu "
                 "(entry preamble) n_ops=%u",
                 pc, idx, (const void *)lo, (const void *)hi, off,
                 (unsigned)prog->n_ops);
        return 1;
      }
      for (uint16_t i = 0; i < prog->n_ops; ++i) {
        if (off >= prog->pc_off[i] && off < prog->pc_off[i + 1]) {
          snprintf(buf, buflen,
                   "JIT-CRASH: pc=%p in JIT blob #%u [%p..%p) byte_off=%zu "
                   "bytecode_pc=%u op_kind=%u (op bytes [%u..%u)) n_ops=%u",
                   pc, idx, (const void *)lo, (const void *)hi, off,
                   (unsigned)i, (unsigned)prog->op_kind[i],
                   (unsigned)prog->pc_off[i], (unsigned)prog->pc_off[i + 1],
                   (unsigned)prog->n_ops);
          return 1;
        }
      }
    }
    snprintf(buf, buflen,
             "JIT-CRASH: pc=%p in JIT blob #%u [%p..%p) byte_off=%zu "
             "(no opcode sidecar)",
             pc, idx, (const void *)lo, (const void *)hi, off);
    return 1;
  }
  return 0; /* pc is not inside any live JIT blob */
}

void jit1_registry_dump(Jit1DumpEmitFn emit, void *arg) {
  if (emit == NULL) return;
  char line[512];
  unsigned idx = 0;
  pthread_mutex_lock(&g_jit1_reg_mtx);
  for (const Jit1Prog *prog = g_jit1_reg_head; prog != NULL;
       prog = prog->reg_next, ++idx) {
    const uint8_t *lo = prog->rx_entry;
    /* Compact per-blob op-kind list. The full bytecode lives with the
     * program's owner (progcache key / stored procedure), not here —
     * the kinds are enough to see what shape a blob is. Kind values are
     * one byte (<= 3 digits) plus a comma, so 8 spare bytes always fit
     * the next element or the ",..." truncation marker. */
    char kinds[192];
    size_t used = 0;
    kinds[0] = '\0';
    if (prog->op_kind != NULL) {
      for (uint16_t i = 0; i < prog->n_ops; ++i) {
        if (sizeof(kinds) - used < 8) {
          snprintf(kinds + used, sizeof(kinds) - used, ",...");
          break;
        }
        used += (size_t)snprintf(kinds + used, sizeof(kinds) - used,
                                 "%s%u", i ? "," : "",
                                 (unsigned)prog->op_kind[i]);
      }
    }
    snprintf(line, sizeof(line),
             "JIT-DUMP: blob #%u [%p..%p) bytes=%zu n_ops=%u kinds=[%s]",
             idx, (const void *)lo, (const void *)(lo + prog->emitted),
             prog->emitted, (unsigned)prog->n_ops,
             prog->op_kind != NULL ? kinds : "no sidecar");
    emit(arg, line);
  }
  snprintf(line, sizeof(line), "JIT-DUMP: %u live JIT program(s)", idx);
  emit(arg, line);
  pthread_mutex_unlock(&g_jit1_reg_mtx);
}
