/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 1 — copy-and-patch JIT engine.
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
 * Phase 1 only runs on x86_64. Other archs return NULL with errno
 * set; jit_arena.h's macOS-x86_64 #error guarantees we never reach
 * this on the unsupported platform.
 */

#include "jit1.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if !defined(__x86_64__)
/* Phase 1 is x86_64 only. The aarch64 stencils need a different
 * hole-encoding strategy because extern symbol references compile
 * to GOT-indirect on AArch64, not inline immediates. Phase 2 lands
 * the cross-arch extractor.
 *
 * We still compile the file on other architectures (so the engine
 * API symbols exist and the interpreter-only test target builds)
 * but jit1_compile always returns NULL with errno=ENOTSUP and the
 * other entry points return safe defaults. */

struct Jit1Prog { int unused; };

Jit1Prog *jit1_compile(NdbJitArena *arena, const Program *prog) {
  (void)arena;
  (void)prog;
  errno = ENOTSUP;
  return NULL;
}

JitEntry jit1_entry(const Jit1Prog *prog) {
  (void)prog;
  return NULL;
}

size_t jit1_emitted_size(const Jit1Prog *prog) {
  (void)prog;
  return 0;
}

#else  /* __x86_64__ */

#include "stencils_x86_64.h"

/* ------------------------------------------------------------------ */
/* Internal types.                                                    */
/* ------------------------------------------------------------------ */

/* A queued forward branch — the patch site is emitted but the
 * target hasn't been emitted yet. Resolved when the emitter's pc
 * advances to `target_pc` and we know the target's byte offset. */
typedef struct {
  uint16_t target_pc;        /* bytecode pc of the branch target */
  uint32_t patch_off;        /* byte offset in the arena blob where the
                                4-byte rel32 displacement starts */
} Fixup;

/* Phase 1 hard limits. The 30-op program has 1 branch; we size
 * generously. */
#define J1_MAX_FIXUPS  16

struct Jit1Prog {
  const uint8_t *rx_entry;      /* RX-mapping pointer to start of compiled blob */
  size_t         emitted;       /* total bytes emitted */
};

/* ------------------------------------------------------------------ */
/* Pure helpers — no arena, no state.                                 */
/* ------------------------------------------------------------------ */

/* Write a 4-byte little-endian value at `dst`. */
static inline void put_u32_le(uint8_t *dst, uint32_t v) {
  dst[0] = (uint8_t)(v       & 0xff);
  dst[1] = (uint8_t)((v >> 8)  & 0xff);
  dst[2] = (uint8_t)((v >> 16) & 0xff);
  dst[3] = (uint8_t)((v >> 24) & 0xff);
}

/* Read the operand value to bake into a hole, given the kind and
 * the current Op. Returns the i32 value; the caller widths it. */
static inline int32_t hole_value_from_op(uint8_t kind, const Op *op) {
  switch (kind) {
    case HK_OP_A:   return (int32_t)op->a;
    case HK_OP_B:   return (int32_t)op->b;
    case HK_OP_C:   return (int32_t)op->c;
    case HK_OP_IMM: return (int32_t)op->imm;
    default:        return 0;   /* branch holes handled separately */
  }
}

/* ------------------------------------------------------------------ */
/* Compile — entry point.                                             */
/* ------------------------------------------------------------------ */

Jit1Prog *jit1_compile(NdbJitArena *arena, const Program *prog) {
  if (!arena || !prog || prog->n_ops == 0 || prog->n_ops > BC_MAX_OPS) {
    errno = EINVAL;
    return NULL;
  }

  /* ---------------------------------------------------------------- */
  /* Pass 1: walk bytecode, compute per-pc byte offsets, total size.  */
  /* ---------------------------------------------------------------- */

  uint32_t pc_byte_off[BC_MAX_OPS + 1];   /* +1 for "end of program" */
  uint32_t total = 0;
  for (uint16_t pc = 0; pc < prog->n_ops; ++pc) {
    pc_byte_off[pc] = total;
    uint8_t kind = prog->ops[pc].kind;
    if (kind == 0 || kind > OP_KIND_MAX) {
      errno = EINVAL;
      return NULL;
    }
    const Stencil *st = &g_stencils[kind];
    if (st->n_bytes == 0) {
      errno = EINVAL;     /* missing stencil */
      return NULL;
    }
    total += st->n_bytes;
  }
  pc_byte_off[prog->n_ops] = total;

  /* ---------------------------------------------------------------- */
  /* Allocate space in the arena: blob bytes + the Jit1Prog handle.  */
  /* The handle is allocated separately (via malloc) because it must  */
  /* outlive the arena's RW/RX lifecycle in a way that's independent  */
  /* of seal — only the *bytes* live in the arena.                    */
  /* ---------------------------------------------------------------- */

  uint8_t *blob_rw = (uint8_t *)ndb_jit_arena_alloc(arena, total, 16);
  if (!blob_rw) {
    errno = ENOMEM;
    return NULL;
  }

  /* ---------------------------------------------------------------- */
  /* Pass 2: emit each stencil, patch holes, queue + drain fixups.   */
  /* ---------------------------------------------------------------- */

  Fixup fixups[J1_MAX_FIXUPS];
  uint8_t n_fixups = 0;

  for (uint16_t pc = 0; pc < prog->n_ops; ++pc) {
    const Op *op = &prog->ops[pc];
    const Stencil *st = &g_stencils[op->kind];
    uint32_t this_off = pc_byte_off[pc];
    uint32_t next_off = pc_byte_off[pc + 1];

    /* memcpy stencil bytes. */
    memcpy(blob_rw + this_off, st->bytes, st->n_bytes);

    /* Patch each hole. */
    for (uint8_t h = 0; h < st->n_holes; ++h) {
      const Hole *hole = &st->holes[h];
      uint8_t *patch = blob_rw + this_off + hole->byte_offset;

      switch (hole->kind) {
        case HK_OP_A:
        case HK_OP_B:
        case HK_OP_C:
        case HK_OP_IMM: {
          int32_t v = hole_value_from_op(hole->kind, op);
          /* All Phase 1 operand holes are 4-byte. */
          put_u32_le(patch, (uint32_t)v);
          break;
        }
        case HK_BRANCH_FALL: {
          /* Fall-through goes to the immediately-following stencil.
           * Displacement = next_pc_offset - patch_site - 4
           * (PC-relative-32 references "next instruction"). */
          uint32_t patch_site_off = this_off + hole->byte_offset;
          int32_t  disp = (int32_t)next_off - (int32_t)patch_site_off - 4;
          put_u32_le(patch, (uint32_t)disp);
          break;
        }
        case HK_BRANCH_TAKE: {
          /* Taken goes to op->c bytecode pc — forward by construction.
           * The target hasn't been emitted yet, so queue a fixup. */
          if (op->c <= pc || op->c >= prog->n_ops) {
            errno = EINVAL;   /* backward / out-of-range branch */
            return NULL;
          }
          if (n_fixups >= J1_MAX_FIXUPS) {
            errno = ENOSPC;
            return NULL;
          }
          fixups[n_fixups++] = (Fixup){
            .target_pc = op->c,
            .patch_off = this_off + hole->byte_offset,
          };
          /* Leave the patch site as zeros for now; resolved below. */
          break;
        }
        default:
          errno = EINVAL;
          return NULL;
      }
    }

    /* Drain any fixups targeting this newly-emitted pc. The branch
     * stencil emits its taken-fixup *before* this drain runs (above),
     * but its target_pc is op->c > pc, so it won't drain here. Drains
     * happen when we reach pc == target_pc later. */
    for (uint8_t f = 0; f < n_fixups;) {
      if (fixups[f].target_pc == pc) {
        uint32_t patch_site_off = fixups[f].patch_off;
        int32_t  disp = (int32_t)this_off - (int32_t)patch_site_off - 4;
        put_u32_le(blob_rw + patch_site_off, (uint32_t)disp);
        /* Remove by swap-with-last. */
        fixups[f] = fixups[--n_fixups];
      } else {
        ++f;
      }
    }
  }

  if (n_fixups != 0) {
    errno = EINVAL;     /* a branch target was never reached */
    return NULL;
  }

  /* ---------------------------------------------------------------- */
  /* Seal the arena range and obtain the RX-mapping pointer.          */
  /* ---------------------------------------------------------------- */

  const void *rx = ndb_jit_arena_seal(arena, blob_rw, total);
  if (!rx) {
    errno = EFAULT;
    return NULL;
  }

  Jit1Prog *handle = (Jit1Prog *)calloc(1, sizeof(*handle));
  if (!handle) {
    errno = ENOMEM;
    return NULL;
  }
  handle->rx_entry = (const uint8_t *)rx;
  handle->emitted  = total;
  return handle;
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

#endif /* __x86_64__ */
