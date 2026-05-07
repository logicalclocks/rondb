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
  /* Phase 4 RONDB-1056: HK_COLDCALL — call site for an extern C++
   * helper (e.g., ndb_jit_h_load_col). Engine resolves the
   * helper's address at compile time via the helper registry
   * (jit1_lookup_helper) and patches the PC-rel displacement.
   * The Hole's helper_name field carries the symbol name to
   * resolve. */
  HK_COLDCALL       = 7,
  HK_KIND_MAX       = HK_COLDCALL,
} HoleKind;

/* ------------------------------------------------------------------ */
/* Hole, Stencil — record types in the generated headers.             */
/* ------------------------------------------------------------------ */

typedef struct {
  uint16_t    byte_offset;  /* into the stencil's bytes_*[] array */
  uint8_t     kind;         /* HoleKind */
  uint8_t     width;        /* 4 (x86_64 32-bit) or 8 (aarch64 64-bit chain) */
  /* Helper symbol name — meaningful only when kind == HK_COLDCALL.
   * NULL for every other hole kind. The engine resolves this
   * through jit1_lookup_helper at compile time and patches the
   * call site with the helper's PC-relative displacement. */
  const char *helper_name;
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
  /* Phase 4 op_load_col_ndb — cold-call shape. The two operand
   * holes are inline mov-imm32 patches; HOLE_HELPER_LOAD_COL is
   * the cold-call PLT32 / CALL26 patch site (kind=HK_COLDCALL,
   * helper_name="ndb_jit_h_load_col"). The extractor records the
   * helper symbol name from the relocation; this table only
   * lists the operand holes here. */
  { "HOLE_LCN_COL",  HK_OP_C          },   /* col_id (16-bit, fits in op->c) */
  { "HOLE_LCN_DST",  HK_OP_A          },   /* dst register slot */
  /* Phase 3 branch-comparison siblings — same hole shape as
   * BLT, one entry per opcode for operand A, B, and target. */
  { "HOLE_BLE_A",    HK_OP_A          },
  { "HOLE_BLE_B",    HK_OP_B          },
  { "HOLE_BLE_TGT",  HK_BRANCH_TAKE   },
  { "HOLE_BEQ_A",    HK_OP_A          },
  { "HOLE_BEQ_B",    HK_OP_B          },
  { "HOLE_BEQ_TGT",  HK_BRANCH_TAKE   },
  { "HOLE_BGT_A",    HK_OP_A          },
  { "HOLE_BGT_B",    HK_OP_B          },
  { "HOLE_BGT_TGT",  HK_BRANCH_TAKE   },
  { "HOLE_BGE_A",    HK_OP_A          },
  { "HOLE_BGE_B",    HK_OP_B          },
  { "HOLE_BGE_TGT",  HK_BRANCH_TAKE   },
  { "HOLE_BNE_A",    HK_OP_A          },
  { "HOLE_BNE_B",    HK_OP_B          },
  { "HOLE_BNE_TGT",  HK_BRANCH_TAKE   },
  /* Phase 4 hot-arithmetic siblings of OP_ADD_INT_INT — same
   * (a=dst, b=lhs, c=rhs) operand layout. */
  { "HOLE_MINUS_DST", HK_OP_A         },
  { "HOLE_MINUS_A",   HK_OP_B         },
  { "HOLE_MINUS_B",   HK_OP_C         },
  { "HOLE_MUL_DST",   HK_OP_A         },
  { "HOLE_MUL_A",     HK_OP_B         },
  { "HOLE_MUL_B",     HK_OP_C         },
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

/* Phase 3 branch-comparison siblings. Generated via
 *   sha256("RONDB-1056-Phase3-magic-v1|" + name)[0:8]
 * interpreted as little-endian uint64. All four 16-bit slots of
 * each magic are nonzero, so clang emits a full 4-instruction
 * movz+3×movk chain rather than a shorter form. */
#define MAGIC_BLE_A       0xce20d1d961ff7bfdull
#define MAGIC_BLE_B       0xf05552ec3a63703dull
#define MAGIC_BEQ_A       0xefb196d6735077c0ull
#define MAGIC_BEQ_B       0xf3e097cab9f3f700ull
#define MAGIC_BGT_A       0xeec8b413284d098eull
#define MAGIC_BGT_B       0x4780763aac046a98ull
#define MAGIC_BGE_A       0x7a8707912973aac3ull
#define MAGIC_BGE_B       0xf9ea82077d8aae88ull
#define MAGIC_BNE_A       0x6914f2c388141a47ull
#define MAGIC_BNE_B       0xe3406ccf859c6a83ull
/* MAGIC_B<XX>_TGT — same as BLT_TGT, no aarch64 magic; targets
 * resolve via R_AARCH64_CALL26 / JUMP26 relocations. */

/* Phase 4 hot-arithmetic siblings (Minus, Mul). Generated via
 *   sha256("RONDB-1056-Phase4-magic-v1|" + name)[0:8]
 * interpreted as little-endian uint64. All four 16-bit slots of
 * each magic are nonzero, so clang emits a full movz+3×movk chain. */
#define MAGIC_MINUS_DST   0xd8b283b66c231d0cull
#define MAGIC_MINUS_A     0xab82e19e8b86abd7ull
#define MAGIC_MINUS_B     0xfbf6f681133e7678ull
#define MAGIC_MUL_DST     0xf25c1230a00880f1ull
#define MAGIC_MUL_A       0xb1b16f0f977988afull
#define MAGIC_MUL_B       0xcdb2fcdafc003d6bull

/* Phase 4 cold-call op_load_col_ndb — operand magics. The
 * cold-call patch site itself uses HK_COLDCALL with the helper
 * symbol name; no magic is needed for it. */
#define MAGIC_LCN_COL     0xffdc6affeafb85ccull
#define MAGIC_LCN_DST     0x398f52beccc0b5c5ull

/* ------------------------------------------------------------------ */
/* Phase 4.5 narrow operand magics (aarch64-only).                    */
/*                                                                    */
/* For HK_OP_A/B/C holes carrying ≤8-bit values (register indices,    */
/* slot indices, col_id ≤ 255), we use a single movz Rd, #imm16       */
/* instruction (4 bytes) instead of the 16-byte movz+3×movk chain.    */
/* Saves 12 bytes per narrow hole on aarch64. HK_OP_IMM stays wide    */
/* (carries int64). x86_64 is unaffected — keeps `mov reg32, imm32`. */
/*                                                                    */
/* Generated via                                                      */
/*   sha256("RONDB-1056-Phase4_5-narrow-magic-v1|" + name)[0:2]      */
/* interpreted as little-endian uint16. Verified collision-free       */
/* across all 30 narrow-eligible holes during the I2 spike.           */
/*                                                                    */
/* APPEND-ONLY: never reuse a value or rename. New entries land at    */
/* the bottom; the v1 salt regenerates deterministically.             */
/* ------------------------------------------------------------------ */
#define MAGIC_LCI_DST_NARROW      0xfc24u
#define MAGIC_LRC_DST_NARROW      0x0c6du
#define MAGIC_LRC_COL_NARROW      0xa616u
#define MAGIC_MV_DST_NARROW       0xd0ecu
#define MAGIC_MV_SRC_NARROW       0xbbaeu
#define MAGIC_ADD_DST_NARROW      0x12ebu
#define MAGIC_ADD_A_NARROW        0x7bd9u
#define MAGIC_ADD_B_NARROW        0x95d5u
#define MAGIC_MINUS_DST_NARROW    0x70dfu
#define MAGIC_MINUS_A_NARROW      0x2c65u
#define MAGIC_MINUS_B_NARROW      0x129bu
#define MAGIC_MUL_DST_NARROW      0xdac5u
#define MAGIC_MUL_A_NARROW        0xc4bdu
#define MAGIC_MUL_B_NARROW        0x71bcu
#define MAGIC_SUM_SLOT_NARROW     0x0d96u
#define MAGIC_SUM_SRC_NARROW      0xf31fu
#define MAGIC_BLT_A_NARROW        0xf437u
#define MAGIC_BLT_B_NARROW        0x6d7au
#define MAGIC_BLE_A_NARROW        0x1dc5u
#define MAGIC_BLE_B_NARROW        0xe486u
#define MAGIC_BEQ_A_NARROW        0x74aeu
#define MAGIC_BEQ_B_NARROW        0x8e83u
#define MAGIC_BGT_A_NARROW        0x1a92u
#define MAGIC_BGT_B_NARROW        0xabddu
#define MAGIC_BGE_A_NARROW        0x16e9u
#define MAGIC_BGE_B_NARROW        0xab1eu
#define MAGIC_BNE_A_NARROW        0x8e91u
#define MAGIC_BNE_B_NARROW        0xa048u
#define MAGIC_LCN_COL_NARROW      0x08f7u
#define MAGIC_LCN_DST_NARROW      0x1f53u

/* ------------------------------------------------------------------ */
/* Phase 4.7 imm12-fold magics (aarch64-only).                        */
/*                                                                    */
/* These get encoded into the imm12 field (bits 21..10) of an LDR or  */
/* STR (immediate, unsigned offset, X-form). Source-level macros pass */
/* the magic as a byte offset (= magic_idx * 8) and the assembler     */
/* divides by 8 to encode imm12 = magic_idx. The extractor's pass-4   */
/* walk and the audit's fold scan both match against imm12 directly.  */
/*                                                                    */
/* Generated via                                                      */
/*   sha256("RONDB-1056-Phase4_7-fold-magic-v1|" + name +             */
/*          optional_#counter)[0:12-bit]                              */
/* Counter is rotated on collision (max 1 retry observed in spike).   */
/*                                                                    */
/* APPEND-ONLY: never reuse a value or rename. New entries land at    */
/* the bottom; the v1 salt regenerates deterministically.             */
/* ------------------------------------------------------------------ */
#define MAGIC_MV_DST_FOLD        0xce9u
#define MAGIC_MV_SRC_FOLD        0x858u
#define MAGIC_LCI_DST_FOLD       0x156u
#define MAGIC_LRC_DST_FOLD       0x99au
#define MAGIC_LRC_COL_FOLD       0xeedu
#define MAGIC_ADD_DST_FOLD       0x974u
#define MAGIC_ADD_A_FOLD         0x767u
#define MAGIC_ADD_B_FOLD         0x522u
#define MAGIC_MINUS_DST_FOLD     0x078u
#define MAGIC_MINUS_A_FOLD       0x2d1u
#define MAGIC_MINUS_B_FOLD       0x029u  /* salt-rotated, retry=1 */
#define MAGIC_MUL_DST_FOLD       0x208u
#define MAGIC_MUL_A_FOLD         0xf35u
#define MAGIC_MUL_B_FOLD         0x5c6u
#define MAGIC_SUM_SLOT_FOLD      0x08cu
#define MAGIC_SUM_SRC_FOLD       0x6f0u
#define MAGIC_BLT_A_FOLD         0x707u
#define MAGIC_BLT_B_FOLD         0x4f2u
#define MAGIC_BLE_A_FOLD         0xa92u
#define MAGIC_BLE_B_FOLD         0xe68u
#define MAGIC_BEQ_A_FOLD         0xc4bu
#define MAGIC_BEQ_B_FOLD         0xfbdu
#define MAGIC_BGT_A_FOLD         0x4bbu
#define MAGIC_BGT_B_FOLD         0x242u
#define MAGIC_BGE_A_FOLD         0x494u
#define MAGIC_BGE_B_FOLD         0x86fu
#define MAGIC_BNE_A_FOLD         0x1efu
#define MAGIC_BNE_B_FOLD         0xe2eu
#define MAGIC_LCN_COL_FOLD       0x336u
#define MAGIC_LCN_DST_FOLD       0xa9bu
#define MAGIC_LCI16_DST_FOLD     0x37eu
#define MAGIC_LCU16_DST_FOLD     0xa6bu
#define MAGIC_LCI32_DST_FOLD     0x938u
#define MAGIC_LCU32_DST_FOLD     0x064u  /* salt-rotated, retry=1 */

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
  /* Phase 4.5 Day 4: only HK_OP_IMM (load_const_int's int64 immediate)
   * still uses the wide 4-instruction movz/movk chain. All register-
   * index and column-id holes migrated to single-MOVZ narrow encoding;
   * see kHoleNarrowMagicTable below. */
  { MAGIC_LCI_VAL,  HK_OP_IMM,      "MAGIC_LCI_VAL"  },
};
static const size_t kHoleMagicTableLen =
    sizeof(kHoleMagicTable) / sizeof(kHoleMagicTable[0]);

/* ------------------------------------------------------------------ */
/* Phase 4.5 narrow-magic table — single-MOVZ holes on aarch64.       */
/*                                                                    */
/* The extractor's pass-3 narrow walk and the audit's narrow scan     */
/* both consult this table. Magic values are 16-bit; the kind /       */
/* width interpretation is the same as for kHoleMagicTable except     */
/* width is implicitly 2 (single MOVZ instruction, 4 bytes per hole). */
/*                                                                    */
/* Append-only: new entries land at the bottom as more stencils       */
/* migrate from the wide chain pattern to single-MOVZ.                */
/* ------------------------------------------------------------------ */

typedef struct {
  uint16_t    magic;
  uint8_t     kind;
  const char *name;        /* informational, used in diagnostics */
} HoleNarrowMagicEntry;

static const HoleNarrowMagicEntry kHoleNarrowMagicTable[] = {
  /* Phase 4.7: 28 of the original 30 narrow magics migrated to
   * imm12 fold (kHoleFoldMagicTable below). Only op_load_col_ndb's
   * cold-call helper-argument holes (LCN_COL, LCN_DST) remain on
   * the narrow-MOVZ path — they're scalar arguments to bl, not
   * array indices, so fold doesn't apply. */
  { MAGIC_LCN_COL_NARROW,   HK_OP_C,  "MAGIC_LCN_COL_NARROW"   },
  { MAGIC_LCN_DST_NARROW,   HK_OP_A,  "MAGIC_LCN_DST_NARROW"   },
};
static const size_t kHoleNarrowMagicTableLen =
    sizeof(kHoleNarrowMagicTable) / sizeof(kHoleNarrowMagicTable[0]);

/* ------------------------------------------------------------------ */
/* Phase 4.7 imm12-fold table — LDR/STR fold holes on aarch64.        */
/*                                                                    */
/* The extractor's pass-4 fold walk and the audit's fold scan both    */
/* consult this table. Magic values are 12-bit; the kind is the same  */
/* as kHoleMagicTable (HK_OP_A/B/C/IMM); width is implicitly 1 (the   */
/* operand value occupies the imm12 field of one LDR/STR instruction, */
/* 4 bytes per hole). Patcher writes bits 21..10.                     */
/*                                                                    */
/* All 34 fold magics are pre-declared here (Day 1) but the actual    */
/* kFoldMagicToStencil[] mapping in audit_magics.c starts empty and   */
/* is populated as stencils migrate. Entries here without a stencil   */
/* mapping are inert — the audit's pass loop skips them.              */
/* ------------------------------------------------------------------ */

typedef struct {
  uint16_t    magic;
  uint8_t     kind;
  const char *name;
} HoleFoldMagicEntry;

static const HoleFoldMagicEntry kHoleFoldMagicTable[] = {
  { MAGIC_MV_DST_FOLD,        HK_OP_A,  "MAGIC_MV_DST_FOLD"        },
  { MAGIC_MV_SRC_FOLD,        HK_OP_B,  "MAGIC_MV_SRC_FOLD"        },
  { MAGIC_LCI_DST_FOLD,       HK_OP_A,  "MAGIC_LCI_DST_FOLD"       },
  { MAGIC_LRC_DST_FOLD,       HK_OP_A,  "MAGIC_LRC_DST_FOLD"       },
  { MAGIC_LRC_COL_FOLD,       HK_OP_B,  "MAGIC_LRC_COL_FOLD"       },
  { MAGIC_ADD_DST_FOLD,       HK_OP_A,  "MAGIC_ADD_DST_FOLD"       },
  { MAGIC_ADD_A_FOLD,         HK_OP_B,  "MAGIC_ADD_A_FOLD"         },
  { MAGIC_ADD_B_FOLD,         HK_OP_C,  "MAGIC_ADD_B_FOLD"         },
  { MAGIC_MINUS_DST_FOLD,     HK_OP_A,  "MAGIC_MINUS_DST_FOLD"     },
  { MAGIC_MINUS_A_FOLD,       HK_OP_B,  "MAGIC_MINUS_A_FOLD"       },
  { MAGIC_MINUS_B_FOLD,       HK_OP_C,  "MAGIC_MINUS_B_FOLD"       },
  { MAGIC_MUL_DST_FOLD,       HK_OP_A,  "MAGIC_MUL_DST_FOLD"       },
  { MAGIC_MUL_A_FOLD,         HK_OP_B,  "MAGIC_MUL_A_FOLD"         },
  { MAGIC_MUL_B_FOLD,         HK_OP_C,  "MAGIC_MUL_B_FOLD"         },
  { MAGIC_SUM_SLOT_FOLD,      HK_OP_A,  "MAGIC_SUM_SLOT_FOLD"      },
  { MAGIC_SUM_SRC_FOLD,       HK_OP_B,  "MAGIC_SUM_SRC_FOLD"       },
  { MAGIC_BLT_A_FOLD,         HK_OP_A,  "MAGIC_BLT_A_FOLD"         },
  { MAGIC_BLT_B_FOLD,         HK_OP_B,  "MAGIC_BLT_B_FOLD"         },
  { MAGIC_BLE_A_FOLD,         HK_OP_A,  "MAGIC_BLE_A_FOLD"         },
  { MAGIC_BLE_B_FOLD,         HK_OP_B,  "MAGIC_BLE_B_FOLD"         },
  { MAGIC_BEQ_A_FOLD,         HK_OP_A,  "MAGIC_BEQ_A_FOLD"         },
  { MAGIC_BEQ_B_FOLD,         HK_OP_B,  "MAGIC_BEQ_B_FOLD"         },
  { MAGIC_BGT_A_FOLD,         HK_OP_A,  "MAGIC_BGT_A_FOLD"         },
  { MAGIC_BGT_B_FOLD,         HK_OP_B,  "MAGIC_BGT_B_FOLD"         },
  { MAGIC_BGE_A_FOLD,         HK_OP_A,  "MAGIC_BGE_A_FOLD"         },
  { MAGIC_BGE_B_FOLD,         HK_OP_B,  "MAGIC_BGE_B_FOLD"         },
  { MAGIC_BNE_A_FOLD,         HK_OP_A,  "MAGIC_BNE_A_FOLD"         },
  { MAGIC_BNE_B_FOLD,         HK_OP_B,  "MAGIC_BNE_B_FOLD"         },
  { MAGIC_LCN_COL_FOLD,       HK_OP_C,  "MAGIC_LCN_COL_FOLD"       },
  { MAGIC_LCN_DST_FOLD,       HK_OP_A,  "MAGIC_LCN_DST_FOLD"       },
  { MAGIC_LCI16_DST_FOLD,     HK_OP_A,  "MAGIC_LCI16_DST_FOLD"     },
  { MAGIC_LCU16_DST_FOLD,     HK_OP_A,  "MAGIC_LCU16_DST_FOLD"     },
  { MAGIC_LCI32_DST_FOLD,     HK_OP_A,  "MAGIC_LCI32_DST_FOLD"     },
  { MAGIC_LCU32_DST_FOLD,     HK_OP_A,  "MAGIC_LCU32_DST_FOLD"     },
};
static const size_t kHoleFoldMagicTableLen =
    sizeof(kHoleFoldMagicTable) / sizeof(kHoleFoldMagicTable[0]);
#endif /* NDB_JIT_HOLE_KINDS_NO_TABLE */

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_HOLE_KINDS_H */
