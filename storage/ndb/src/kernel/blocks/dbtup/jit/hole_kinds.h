/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 2 — single source of truth for hole kinds.
 *
 * Pure C11. Included by stencils_src.c (where the kinds are
 * referenced via extern HOLE_* / MAGIC_* placeholders), the
 * generated stencils_*.h headers (where holes are recorded by
 * kind), the extractor (where symbol names map to kinds), and
 * jit1.c (where the engine patches each hole).
 *
 * Add new hole kinds at the END (append-only) so existing
 * stencil bytes don't shift when a new opcode arrives.
 */

#ifndef NDB_JIT_HOLE_KINDS_H
#define NDB_JIT_HOLE_KINDS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/* HoleKind — what value gets baked into the patch site at JIT time. */
/* ------------------------------------------------------------------ */

typedef enum {
  HK_OP_A           = 1,   /* op->a (register / accumulator slot) */
  HK_OP_B           = 2,   /* op->b (register / column index) */
  HK_OP_C           = 3,   /* op->c (register) */
  HK_OP_IMM         = 4,   /* op->imm (signed; fits 32 bits sign-extended) */
  HK_BRANCH_FALL    = 5,   /* PC-rel disp from patch site to next stencil */
  HK_BRANCH_TAKE    = 6,   /* PC-rel disp from patch site to op.c bytecode pc */
  HK_KIND_MAX       = HK_BRANCH_TAKE,
} HoleKind;

/* ------------------------------------------------------------------ */
/* Hole, Stencil — record types in the generated headers.             */
/* ------------------------------------------------------------------ */

typedef struct {
  uint16_t byte_offset;   /* into the stencil's bytes_*[] array */
  uint8_t  kind;          /* HoleKind */
  uint8_t  width;         /* 4 (x86_64 32-bit) or 8 (aarch64 64-bit chain) */
} Hole;

typedef struct {
  const uint8_t *bytes;
  uint16_t       n_bytes;
  const Hole    *holes;
  uint8_t        n_holes;
} Stencil;

/* ------------------------------------------------------------------ */
/* Symbol-name -> HoleKind table for relocation-driven holes (x86_64).*/
/*                                                                    */
/* The extractor matches the relocation's target symbol against this  */
/* table to decide which kind a relocation-driven hole is. The list   */
/* must mirror the `extern uint64_t HOLE_*` / `extern void *HOLE_*`  */
/* declarations in stencils_src.c.                                    */
/* ------------------------------------------------------------------ */

#if !defined(NDB_JIT_HOLE_KINDS_NO_TABLE)
typedef struct {
  const char *name;
  uint8_t     kind;
} HoleSymbolEntry;

static const HoleSymbolEntry kHoleSymbolTable[] = {
  /* op_load_const_int */
  { "HOLE_LCI_DST",  HK_OP_A   },
  { "HOLE_LCI_VAL",  HK_OP_IMM },
  /* op_load_col_int */
  { "HOLE_LRC_DST",  HK_OP_A   },
  { "HOLE_LRC_COL",  HK_OP_B   },
  /* op_mov_int_int */
  { "HOLE_MV_DST",   HK_OP_A   },
  { "HOLE_MV_SRC",   HK_OP_B   },
  /* op_add_int_int */
  { "HOLE_ADD_DST",  HK_OP_A   },
  { "HOLE_ADD_A",    HK_OP_B   },
  { "HOLE_ADD_B",    HK_OP_C   },
  /* op_sum_bigint */
  { "HOLE_SUM_SLOT", HK_OP_A   },
  { "HOLE_SUM_SRC",  HK_OP_B   },
  /* op_branch_lt_int_int */
  { "HOLE_BLT_A",    HK_OP_A          },
  { "HOLE_BLT_B",    HK_OP_B          },
  { "HOLE_BLT_TGT",  HK_BRANCH_TAKE   },
};
static const size_t kHoleSymbolTableLen =
    sizeof(kHoleSymbolTable) / sizeof(kHoleSymbolTable[0]);
#endif /* NDB_JIT_HOLE_KINDS_NO_TABLE */

/* ------------------------------------------------------------------ */
/* Curated magic-byte constants for aarch64 sub-word holes.           */
/*                                                                    */
/* Each kind that appears as a `volatile uint64_t` literal in the     */
/* aarch64 path of stencils_src.c gets one 64-bit sentinel here.      */
/* clang lowers `volatile` literals to a movz/movk/movk/movk chain    */
/* materialising the constant; the extractor scans the stencil's     */
/* byte range for the assembled 64-bit value and records each match  */
/* as a magic-byte hole.                                              */
/*                                                                    */
/* These are HIGH-ENTROPY values, deterministically generated from    */
/* SHA-256(salt || name)[0:8]. The reasons:                           */
/*                                                                    */
/*   1. clang folds across magic constants when they differ by a      */
/*      simple arithmetic (e.g., previous version used                */
/*      0xC0DEC0DE_NNNN_C0DE — clang noticed neighbouring kinds       */
/*      differed by 0x10000 and emitted ONE movz/movk chain plus an   */
/*      `add x9, x8, #0x10, lsl #12`, hiding the second magic from   */
/*      the extractor entirely). High-entropy values defeat this.    */
/*                                                                    */
/*   2. Random byte distribution means false positives in stencil     */
/*      bytes are vanishingly unlikely. The CI collision audit        */
/*      (see phase_2_implementation.md §7.2) asserts each magic       */
/*      appears the expected number of times within its declaring     */
/*      stencil and zero elsewhere.                                   */
/*                                                                    */
/* APPEND-ONLY: never reuse a value or renumber an existing one —     */
/* that would change historical stencil bytes. To add a new hole      */
/* kind, append a new line; the extractor's table picks it up         */
/* automatically.                                                     */
/* ------------------------------------------------------------------ */

#define MAGIC_LCI_DST     0x504a8c2cd757d72bull
#define MAGIC_LCI_VAL     0x223975d389209953ull
#define MAGIC_LRC_DST     0xcb07ae05fcf0cf58ull
#define MAGIC_LRC_COL     0x0a7785535b0dae38ull
#define MAGIC_MV_DST      0x25470740206cc41bull
#define MAGIC_MV_SRC      0x5790d9b5c360b5a4ull
#define MAGIC_ADD_DST     0xc7a90e5900373d62ull
#define MAGIC_ADD_A       0xdffb058746e5c4c4ull
#define MAGIC_ADD_B       0x0699c1e457d7617bull
#define MAGIC_SUM_SLOT    0x807a71779a2095a5ull
#define MAGIC_SUM_SRC     0xf8922a3d54402fd8ull
#define MAGIC_BLT_A       0xd79f74ebb8bb0e4dull
#define MAGIC_BLT_B       0x6595bd297abcb657ull
/* MAGIC_BLT_TGT is not a magic-byte hole on aarch64 either —
 * branch targets use B/CALL26 relocations on aarch64. */

/* For the magic-byte scan, the extractor needs a (magic_value,
 * hole_kind) table. We use the same entries as the symbol table
 * above, mapped to magic constants. This is for aarch64 only. */
#if !defined(NDB_JIT_HOLE_KINDS_NO_TABLE)
typedef struct {
  uint64_t magic;
  uint8_t  kind;
  const char *name;     /* informational, used in diagnostics */
} HoleMagicEntry;

static const HoleMagicEntry kHoleMagicTable[] = {
  { MAGIC_LCI_DST,  HK_OP_A,        "MAGIC_LCI_DST"  },
  { MAGIC_LCI_VAL,  HK_OP_IMM,      "MAGIC_LCI_VAL"  },
  { MAGIC_LRC_DST,  HK_OP_A,        "MAGIC_LRC_DST"  },
  { MAGIC_LRC_COL,  HK_OP_B,        "MAGIC_LRC_COL"  },
  { MAGIC_MV_DST,   HK_OP_A,        "MAGIC_MV_DST"   },
  { MAGIC_MV_SRC,   HK_OP_B,        "MAGIC_MV_SRC"   },
  { MAGIC_ADD_DST,  HK_OP_A,        "MAGIC_ADD_DST"  },
  { MAGIC_ADD_A,    HK_OP_B,        "MAGIC_ADD_A"    },
  { MAGIC_ADD_B,    HK_OP_C,        "MAGIC_ADD_B"    },
  { MAGIC_SUM_SLOT, HK_OP_A,        "MAGIC_SUM_SLOT" },
  { MAGIC_SUM_SRC,  HK_OP_B,        "MAGIC_SUM_SRC"  },
  { MAGIC_BLT_A,    HK_OP_A,        "MAGIC_BLT_A"    },
  { MAGIC_BLT_B,    HK_OP_B,        "MAGIC_BLT_B"    },
};
static const size_t kHoleMagicTableLen =
    sizeof(kHoleMagicTable) / sizeof(kHoleMagicTable[0]);
#endif /* NDB_JIT_HOLE_KINDS_NO_TABLE */

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_HOLE_KINDS_H */
