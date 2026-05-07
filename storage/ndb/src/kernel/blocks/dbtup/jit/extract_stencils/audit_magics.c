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
 * RONDB-1056 Phase 2 — magic-byte collision audit.
 *
 * Verifies that each magic constant declared in hole_kinds.h appears
 * exactly the expected number of times across the generated stencil
 * headers:
 *
 *   - aarch64: each magic must appear as exactly ONE complete
 *     movz/movk chain in its declaring stencil, and ZERO chains in
 *     every other stencil.
 *
 *   - x86_64: each magic must NOT appear as an 8-byte little-endian
 *     literal in any stencil's bytes. (x86_64 codegen uses relocation
 *     placeholders, not the magic constants — but high-entropy
 *     magics could in principle coincide with raw instruction bytes.
 *     We assert they don't.)
 *
 * Two failure modes this catches:
 *
 *   1. Clang folds across two chains (one magic appears 0x when we
 *      expect 1x). Symptom: an aarch64 hole is missing in the
 *      generated header → engine can't patch the operand → wrong
 *      codegen. The extractor itself can't see this — it processes
 *      whatever movz/movk chains clang emits — so the audit needs
 *      external corroboration.
 *
 *   2. A magic byte sequence accidentally coincides with raw
 *      instruction bytes. Vanishingly unlikely with high-entropy
 *      random magics, but worth asserting.
 *
 * Usage:
 *   audit_magics <header_path> <arch>
 *     arch: x86_64 | arm64
 *
 * Returns 0 on PASS, 1 on any violation. Prints per-match diagnostic
 * lines plus a summary.
 */

#include "../hole_kinds.h"   /* MAGIC_* constants + kHoleMagicTable */

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Diagnostics.                                                       */
/* ------------------------------------------------------------------ */

static const char *g_argv0 = "audit_magics";

__attribute__((format(printf, 1, 2)))
__attribute__((noreturn))
static void die(const char *fmt, ...) {
  va_list ap;
  fprintf(stderr, "%s: ", g_argv0);
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(2);
}

/* ------------------------------------------------------------------ */
/* Magic-name -> declaring-stencil map.                               */
/*                                                                    */
/* Stays local to this audit tool. hole_kinds.h knows about magics    */
/* and their kinds; the audit additionally cares about which stencil  */
/* each magic is supposed to live inside.                             */
/* ------------------------------------------------------------------ */

static const struct {
  const char *magic_name;
  const char *stencil_name;
} kMagicToStencil[] = {
  /* Phase 4.5 Day 4: only HK_OP_IMM still uses the wide chain.
   * All register-index and column-id operand magics migrated to
   * kNarrowMagicToStencil[] below. */
  { "MAGIC_LCI_VAL",  "op_load_const_int"     },
};
static const size_t kMagicToStencilLen =
    sizeof(kMagicToStencil) / sizeof(kMagicToStencil[0]);

/* Phase 4.5: narrow magics declared by stencils. Each appears as a
 * single-MOVZ (hw=0) instruction in its declaring stencil and zero
 * times in any other stencil. */
static const struct {
  const char *magic_name;
  const char *stencil_name;
} kNarrowMagicToStencil[] = {
  /* Day 1 smoke set. */
  { "MAGIC_LCI_DST_NARROW",   "op_load_const_int"     },
  { "MAGIC_LRC_DST_NARROW",   "op_load_col_int"       },
  { "MAGIC_LRC_COL_NARROW",   "op_load_col_int"       },
  { "MAGIC_MV_DST_NARROW",    "op_mov_int_int"        },
  { "MAGIC_MV_SRC_NARROW",    "op_mov_int_int"        },
  /* Day 4: hot arithmetic. */
  { "MAGIC_ADD_DST_NARROW",   "op_add_int_int"        },
  { "MAGIC_ADD_A_NARROW",     "op_add_int_int"        },
  { "MAGIC_ADD_B_NARROW",     "op_add_int_int"        },
  { "MAGIC_MINUS_DST_NARROW", "op_minus_int_int"      },
  { "MAGIC_MINUS_A_NARROW",   "op_minus_int_int"      },
  { "MAGIC_MINUS_B_NARROW",   "op_minus_int_int"      },
  { "MAGIC_MUL_DST_NARROW",   "op_mul_int_int"        },
  { "MAGIC_MUL_A_NARROW",     "op_mul_int_int"        },
  { "MAGIC_MUL_B_NARROW",     "op_mul_int_int"        },
  { "MAGIC_SUM_SLOT_NARROW",  "op_sum_bigint"         },
  { "MAGIC_SUM_SRC_NARROW",   "op_sum_bigint"         },
  /* Day 4: branch-comparison siblings. */
  { "MAGIC_BLT_A_NARROW",     "op_branch_lt_int_int"  },
  { "MAGIC_BLT_B_NARROW",     "op_branch_lt_int_int"  },
  { "MAGIC_BLE_A_NARROW",     "op_branch_le_int_int"  },
  { "MAGIC_BLE_B_NARROW",     "op_branch_le_int_int"  },
  { "MAGIC_BEQ_A_NARROW",     "op_branch_eq_int_int"  },
  { "MAGIC_BEQ_B_NARROW",     "op_branch_eq_int_int"  },
  { "MAGIC_BGT_A_NARROW",     "op_branch_gt_int_int"  },
  { "MAGIC_BGT_B_NARROW",     "op_branch_gt_int_int"  },
  { "MAGIC_BGE_A_NARROW",     "op_branch_ge_int_int"  },
  { "MAGIC_BGE_B_NARROW",     "op_branch_ge_int_int"  },
  { "MAGIC_BNE_A_NARROW",     "op_branch_ne_int_int"  },
  { "MAGIC_BNE_B_NARROW",     "op_branch_ne_int_int"  },
  /* Day 4: cold-call op_load_col_ndb. */
  { "MAGIC_LCN_COL_NARROW",   "op_load_col_ndb"       },
  { "MAGIC_LCN_DST_NARROW",   "op_load_col_ndb"       },
};
static const size_t kNarrowMagicToStencilLen =
    sizeof(kNarrowMagicToStencil) / sizeof(kNarrowMagicToStencil[0]);

static const char *expected_stencil_for(const char *magic_name) {
  for (size_t i = 0; i < kMagicToStencilLen; ++i) {
    if (strcmp(kMagicToStencil[i].magic_name, magic_name) == 0) {
      return kMagicToStencil[i].stencil_name;
    }
  }
  die("magic %s missing from kMagicToStencil[] — add an entry "
      "before declaring a new MAGIC_* in hole_kinds.h", magic_name);
}

static const char *expected_stencil_for_narrow(const char *magic_name) {
  for (size_t i = 0; i < kNarrowMagicToStencilLen; ++i) {
    if (strcmp(kNarrowMagicToStencil[i].magic_name, magic_name) == 0) {
      return kNarrowMagicToStencil[i].stencil_name;
    }
  }
  die("narrow magic %s missing from kNarrowMagicToStencil[] — add "
      "an entry before declaring a new MAGIC_*_NARROW in hole_kinds.h",
      magic_name);
}

/* Phase 4.7: fold magics declared by stencils. Each appears as a
 * single LDR/STR (immediate, X-form) imm12 at bits 21..10 in its
 * declaring stencil and zero times in any other stencil.
 *
 * Day 1 lands the infrastructure with this table EMPTY — fold
 * magics are pre-declared in hole_kinds.h but not yet referenced
 * by any stencil. Day 2 starts populating this table as stencils
 * migrate to the fold pattern.
 *
 * Note: unlike kNarrowMagicToStencil, this table is allowed to be
 * smaller than the underlying kHoleFoldMagicTable[]. Magics in the
 * latter without an entry here are inert — the audit's fold pass
 * skips them (no expectation = no check). */
static const struct {
  const char *magic_name;
  const char *stencil_name;
} kFoldMagicToStencil[] = {
  /* (none yet — Day 2+ migration adds entries) */
  { NULL, NULL }    /* sentinel; the length-1 array form keeps C happy */
};
static const size_t kFoldMagicToStencilLen =
    /* count actual entries (excluding the {NULL,NULL} sentinel). */
    (sizeof(kFoldMagicToStencil) / sizeof(kFoldMagicToStencil[0])) - 1;

static const char *expected_stencil_for_fold(const char *magic_name) {
  for (size_t i = 0; i < kFoldMagicToStencilLen; ++i) {
    if (strcmp(kFoldMagicToStencil[i].magic_name, magic_name) == 0) {
      return kFoldMagicToStencil[i].stencil_name;
    }
  }
  /* Returning NULL signals "no stencil currently declares this
   * fold magic" — caller treats as "expected 0× everywhere". */
  return NULL;
}

/* ------------------------------------------------------------------ */
/* Header parsing.                                                    */
/*                                                                    */
/* Linear scan for `static const uint8_t bytes_<name>[] = {`, capture */
/* the identifier, then read hex bytes (`0xXX`) until `}`.            */
/* ------------------------------------------------------------------ */

#define MAX_STENCILS         16
#define MAX_STENCIL_BYTES  2048
#define MAX_NAME_LEN         64

typedef struct {
  char    name[MAX_NAME_LEN];
  uint8_t bytes[MAX_STENCIL_BYTES];
  size_t  n_bytes;
} ParsedStencil;

static char *slurp_file(const char *path, size_t *out_size) {
  FILE *f = fopen(path, "rb");
  if (!f) die("cannot open %s: %s", path, strerror(errno));
  if (fseek(f, 0, SEEK_END) != 0) die("fseek %s: %s", path, strerror(errno));
  long sz = ftell(f);
  if (sz < 0) die("ftell %s: %s", path, strerror(errno));
  if (fseek(f, 0, SEEK_SET) != 0) die("fseek %s: %s", path, strerror(errno));

  char *buf = (char *)malloc((size_t)sz + 1);
  if (!buf) die("oom slurping %s (%ld bytes)", path, sz);
  size_t got = fread(buf, 1, (size_t)sz, f);
  if (got != (size_t)sz) die("short read on %s: %zu of %ld", path, got, sz);
  buf[sz] = '\0';
  fclose(f);

  *out_size = (size_t)sz;
  return buf;
}

static size_t parse_header(const char *path,
                            ParsedStencil *out,
                            size_t cap) {
  size_t buf_sz = 0;
  char *buf = slurp_file(path, &buf_sz);
  const char *s   = buf;
  const char *end = buf + buf_sz;

  static const char marker[] = "static const uint8_t bytes_";
  const size_t marker_len = sizeof(marker) - 1;

  size_t n = 0;
  while (s < end) {
    const char *m = strstr(s, marker);
    if (!m) break;
    s = m + marker_len;

    if (n >= cap) die("header has more than %zu stencils — "
                      "raise MAX_STENCILS", cap);

    /* Identifier: [A-Za-z0-9_]+ until '['. */
    char *name = out[n].name;
    size_t name_len = 0;
    while (s < end &&
           (isalnum((unsigned char)*s) || *s == '_') &&
           name_len + 1 < MAX_NAME_LEN) {
      name[name_len++] = *s++;
    }
    name[name_len] = '\0';
    if (name_len == 0) die("malformed header: empty identifier "
                            "after `bytes_`");

    /* Skip whitespace + `[]` + `=` until `{`. */
    while (s < end && *s != '{') s++;
    if (s >= end) die("malformed header: no `{` after bytes_%s", name);
    s++;

    /* Read hex bytes until matching `}` (no nesting expected). */
    size_t n_bytes = 0;
    while (s < end && *s != '}') {
      /* Skip non-hex chars looking for `0x`. */
      if (s + 1 < end && s[0] == '0' && s[1] == 'x') {
        s += 2;
        if (s + 2 > end) die("malformed header: truncated hex byte "
                              "in bytes_%s", name);
        char h[3] = { s[0], s[1], '\0' };
        if (!isxdigit((unsigned char)h[0]) ||
            !isxdigit((unsigned char)h[1])) {
          die("malformed header: non-hex chars `%s` in bytes_%s",
              h, name);
        }
        if (n_bytes >= MAX_STENCIL_BYTES) {
          die("bytes_%s exceeds %d-byte buffer — raise "
              "MAX_STENCIL_BYTES", name, MAX_STENCIL_BYTES);
        }
        out[n].bytes[n_bytes++] = (uint8_t)strtoul(h, NULL, 16);
        s += 2;
      } else {
        s++;
      }
    }
    if (s >= end) die("malformed header: no `}` closing bytes_%s", name);
    out[n].n_bytes = n_bytes;
    s++;   /* past '}' */
    n++;
  }

  free(buf);
  return n;
}

/* ------------------------------------------------------------------ */
/* aarch64: count complete movz/movk chains matching `magic`.         */
/*                                                                    */
/* Walks every 4-byte instruction in the stencil's bytes. Decodes     */
/* movz / movk (sf=1 forms). Maintains per-Rd accumulated 64-bit      */
/* value plus a slot-seen bitmask. When a register's value equals     */
/* `magic` AND all four slots have been written, count one chain.    */
/*                                                                    */
/* Same logic the extractor uses to detect chains; reproducing it     */
/* here independently is the audit's value — if the extractor has    */
/* a bug that swallows a chain, the audit's separate run can flag    */
/* it (assuming this independent implementation also has the bug,    */
/* obviously, but at least one degree of separation).                 */
/* ------------------------------------------------------------------ */

static int count_chain_matches_arm64(const uint8_t *bytes,
                                      size_t n_bytes,
                                      uint64_t magic) {
  int count = 0;
  uint64_t reg_value[32];
  uint8_t  reg_slot_seen[32];
  for (int i = 0; i < 32; ++i) {
    reg_value[i]     = 0;
    reg_slot_seen[i] = 0;
  }

  for (size_t off = 0; off + 4 <= n_bytes; off += 4) {
    uint32_t insn = (uint32_t)bytes[off]            |
                    ((uint32_t)bytes[off + 1] << 8) |
                    ((uint32_t)bytes[off + 2] << 16)|
                    ((uint32_t)bytes[off + 3] << 24);

    int is_movz = (insn & 0xFF800000) == 0xD2800000;
    int is_movk = (insn & 0xFF800000) == 0xF2800000;
    int is_movn = (insn & 0xFF800000) == 0x92800000;

    if (is_movn) {
      uint32_t Rd = insn & 0x1F;
      reg_slot_seen[Rd] = 0;
      reg_value[Rd]     = 0;
      continue;
    }
    if (!is_movz && !is_movk) continue;

    uint32_t hw    = (insn >> 21) & 0x3;
    uint32_t imm16 = (insn >> 5)  & 0xFFFF;
    uint32_t Rd    = insn & 0x1F;

    if (is_movz) {
      reg_value[Rd]     = (uint64_t)imm16 << (hw * 16);
      reg_slot_seen[Rd] = (uint8_t)(1u << hw);
    } else {
      uint64_t mask = ~((uint64_t)0xFFFFu << (hw * 16));
      reg_value[Rd] = (reg_value[Rd] & mask) |
                      ((uint64_t)imm16 << (hw * 16));
      reg_slot_seen[Rd] |= (uint8_t)(1u << hw);
    }

    if (reg_slot_seen[Rd] == 0xF && reg_value[Rd] == magic) {
      count++;
      reg_slot_seen[Rd] = 0;
      reg_value[Rd]     = 0;
    }
  }
  return count;
}

/* ------------------------------------------------------------------ */
/* aarch64: count single-MOVZ instructions encoding `magic` in slot 0.*/
/*                                                                    */
/* Phase 4.5 narrow holes: the codegen pattern collapses a 64-bit     */
/* 4-instruction movz/movk chain into a single MOVZ (hw=0) carrying   */
/* a 16-bit immediate. The audit must verify that each declared       */
/* narrow magic appears as exactly one such MOVZ in its declaring     */
/* stencil and zero times in every other stencil.                     */
/*                                                                    */
/* We deliberately ignore movz instructions that are part of a        */
/* longer chain — those are caught by count_chain_matches_arm64.      */
/* A narrow MOVZ is identified as: hw==0 AND no immediately following */
/* MOVK targeting the same Rd. (In practice: the volatile-store/load  */
/* pattern emits a single MOVZ then a STRH/LDRH, so there is no       */
/* trailing MOVK on Rd.)                                              */
/*                                                                    */
/* sf-agnostic: clang lowers `volatile uint16_t v = magic; return v;` */
/* to a 32-bit MOVZ (0x52800000) since uint16_t fits in a w-reg —    */
/* NOT the 64-bit MOVZ (0xD2800000) used for the wide chain pattern.  */
/* Mask 0x7F800000 / 0x52800000 accepts both forms; mask              */
/* 0x7F800000 / 0x72800000 likewise for the trailing-MOVK guard.      */
/* ------------------------------------------------------------------ */

static int count_narrow_matches_arm64(const uint8_t *bytes,
                                       size_t n_bytes,
                                       uint16_t magic) {
  int count = 0;
  for (size_t off = 0; off + 4 <= n_bytes; off += 4) {
    uint32_t insn = (uint32_t)bytes[off]            |
                    ((uint32_t)bytes[off + 1] << 8) |
                    ((uint32_t)bytes[off + 2] << 16)|
                    ((uint32_t)bytes[off + 3] << 24);

    int is_movz = (insn & 0x7F800000) == 0x52800000;
    if (!is_movz) continue;

    uint32_t hw    = (insn >> 21) & 0x3;
    uint32_t imm16 = (insn >> 5)  & 0xFFFF;
    uint32_t Rd    = insn & 0x1F;

    if (hw != 0)             continue;
    if (imm16 != magic)      continue;

    /* Reject if the next instruction is a MOVK targeting the same Rd
     * — that would mean we're looking at the head of a wide chain,
     * not a standalone narrow MOVZ. */
    if (off + 8 <= n_bytes) {
      uint32_t next = (uint32_t)bytes[off + 4]            |
                      ((uint32_t)bytes[off + 5] << 8)     |
                      ((uint32_t)bytes[off + 6] << 16)    |
                      ((uint32_t)bytes[off + 7] << 24);
      int next_is_movk = (next & 0x7F800000) == 0x72800000;
      uint32_t next_Rd = next & 0x1F;
      if (next_is_movk && next_Rd == Rd) continue;
    }

    count++;
  }
  return count;
}

/* ------------------------------------------------------------------ */
/* aarch64: count LDR/STR (immediate, X-form) imm12 matches.          */
/*                                                                    */
/* Phase 4.7 fold holes: the codegen pattern collapses a              */
/* `movz wN, #idx; ldr/str xT, [base, xN, lsl #3]` 2-instruction      */
/* sequence into a single `ldr/str xT, [base, #(idx*8)]`. The audit   */
/* verifies each declared fold magic appears as exactly one such      */
/* LDR/STR imm12 in its declaring stencil and zero times elsewhere.   */
/*                                                                    */
/*   LDR Xt, [Xn, #imm12]: mask 0xFFC00000 -> match 0xF9400000        */
/*   STR Xt, [Xn, #imm12]: mask 0xFFC00000 -> match 0xF9000000        */
/* ------------------------------------------------------------------ */

static int count_fold_matches_arm64(const uint8_t *bytes,
                                     size_t n_bytes,
                                     uint16_t magic) {
  int count = 0;
  for (size_t off = 0; off + 4 <= n_bytes; off += 4) {
    uint32_t insn = (uint32_t)bytes[off]            |
                    ((uint32_t)bytes[off + 1] << 8) |
                    ((uint32_t)bytes[off + 2] << 16)|
                    ((uint32_t)bytes[off + 3] << 24);

    int is_ldr_imm = (insn & 0xFFC00000) == 0xF9400000;
    int is_str_imm = (insn & 0xFFC00000) == 0xF9000000;
    if (!is_ldr_imm && !is_str_imm) continue;

    uint16_t imm12 = (uint16_t)((insn >> 10) & 0xFFFu);
    if (imm12 == magic) count++;
  }
  return count;
}

/* ------------------------------------------------------------------ */
/* x86_64: count 8-byte little-endian literal occurrences of `magic`. */
/*                                                                    */
/* Slides an 8-byte window across the stencil's bytes. With high-     */
/* entropy magics, expected count is 0 across all stencils — a hit    */
/* would mean a freak coincidence between random magic bits and       */
/* actual instruction bytes, indistinguishable from a magic-bearing   */
/* literal at runtime.                                                */
/* ------------------------------------------------------------------ */

static int count_literal_matches_x86(const uint8_t *bytes,
                                      size_t n_bytes,
                                      uint64_t magic) {
  int count = 0;
  if (n_bytes < 8) return 0;
  for (size_t off = 0; off + 8 <= n_bytes; ++off) {
    uint64_t window = 0;
    for (int i = 0; i < 8; ++i) {
      window |= ((uint64_t)bytes[off + i]) << (i * 8);
    }
    if (window == magic) count++;
  }
  return count;
}

/* Narrow magics have no x86_64 audit: they are an aarch64-only
 * codegen pattern, and a 16-bit literal scan on x86 has too high a
 * collision rate (~n_bytes / 65536) to be a useful check. */

/* ------------------------------------------------------------------ */
/* main.                                                              */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
  g_argv0 = argv[0];
  if (argc != 3) {
    fprintf(stderr,
            "usage: %s <header_path> <arch>\n"
            "  arch: x86_64 | arm64\n",
            argv[0]);
    return 2;
  }
  const char *header_path = argv[1];
  const char *arch        = argv[2];

  int is_arm64;
  if      (strcmp(arch, "x86_64") == 0) is_arm64 = 0;
  else if (strcmp(arch, "arm64")  == 0) is_arm64 = 1;
  else die("unknown arch '%s' (must be x86_64 or arm64)", arch);

  ParsedStencil stencils[MAX_STENCILS];
  size_t n_stencils = parse_header(header_path, stencils, MAX_STENCILS);
  fprintf(stderr,
          "%s: parsed %zu stencils from %s (arch=%s)\n",
          g_argv0, n_stencils, header_path, arch);

  /* Cross-check: every magic in the table has a stencil mapping. */
  for (size_t k = 0; k < kHoleMagicTableLen; ++k) {
    (void)expected_stencil_for(kHoleMagicTable[k].name);  /* dies on miss */
  }
  for (size_t k = 0; k < kHoleNarrowMagicTableLen; ++k) {
    (void)expected_stencil_for_narrow(kHoleNarrowMagicTable[k].name);
  }

  /* Cross-check: every stencil declared as a target by either map
   * actually appears in the parsed header. */
  for (size_t i = 0; i < kMagicToStencilLen; ++i) {
    int found = 0;
    for (size_t j = 0; j < n_stencils; ++j) {
      if (strcmp(stencils[j].name, kMagicToStencil[i].stencil_name) == 0) {
        found = 1;
        break;
      }
    }
    if (!found && is_arm64) {
      die("expected stencil %s (declared by magic %s) not present in header",
          kMagicToStencil[i].stencil_name, kMagicToStencil[i].magic_name);
    }
  }
  for (size_t i = 0; i < kNarrowMagicToStencilLen; ++i) {
    int found = 0;
    for (size_t j = 0; j < n_stencils; ++j) {
      if (strcmp(stencils[j].name,
                 kNarrowMagicToStencil[i].stencil_name) == 0) {
        found = 1;
        break;
      }
    }
    if (!found && is_arm64) {
      die("expected stencil %s (declared by narrow magic %s) not "
          "present in header",
          kNarrowMagicToStencil[i].stencil_name,
          kNarrowMagicToStencil[i].magic_name);
    }
  }

  int errors = 0;
  for (size_t k = 0; k < kHoleMagicTableLen; ++k) {
    const HoleMagicEntry *m = &kHoleMagicTable[k];
    const char *expected_in = expected_stencil_for(m->name);

    for (size_t i = 0; i < n_stencils; ++i) {
      int count;
      if (is_arm64) {
        count = count_chain_matches_arm64(stencils[i].bytes,
                                          stencils[i].n_bytes,
                                          m->magic);
      } else {
        count = count_literal_matches_x86(stencils[i].bytes,
                                          stencils[i].n_bytes,
                                          m->magic);
      }

      int expected =
          (is_arm64 && strcmp(stencils[i].name, expected_in) == 0) ? 1 : 0;

      if (count != expected) {
        fprintf(stderr,
                "  VIOLATION: %s in %s: expected %d, found %d\n",
                m->name, stencils[i].name, expected, count);
        errors++;
      } else if (count > 0) {
        fprintf(stderr,
                "  ok: %s found %dx in %s (expected)\n",
                m->name, count, stencils[i].name);
      }
    }
  }

  /* Phase 4.5 narrow magics: aarch64-only check. Each declared
   * narrow magic must appear as a single MOVZ (hw=0) in its
   * declaring stencil and zero times in any other stencil. */
  if (is_arm64) {
    for (size_t k = 0; k < kHoleNarrowMagicTableLen; ++k) {
      const HoleNarrowMagicEntry *m = &kHoleNarrowMagicTable[k];
      const char *expected_in = expected_stencil_for_narrow(m->name);

      for (size_t i = 0; i < n_stencils; ++i) {
        int count = count_narrow_matches_arm64(stencils[i].bytes,
                                                stencils[i].n_bytes,
                                                m->magic);
        int expected =
            (strcmp(stencils[i].name, expected_in) == 0) ? 1 : 0;

        if (count != expected) {
          fprintf(stderr,
                  "  VIOLATION: %s in %s: expected %d, found %d "
                  "(narrow MOVZ)\n",
                  m->name, stencils[i].name, expected, count);
          errors++;
        } else if (count > 0) {
          fprintf(stderr,
                  "  ok: %s found %dx in %s (narrow MOVZ, expected)\n",
                  m->name, count, stencils[i].name);
        }
      }
    }
  }

  /* Phase 4.7 fold magics: aarch64-only check. Each declared
   * fold magic must appear as exactly one LDR/STR (immediate,
   * X-form) imm12 in its declaring stencil and zero times in any
   * other stencil. Fold magics WITHOUT a kFoldMagicToStencil[]
   * entry are inert (no expectation, no check) — the table fills
   * in as Day 2+ migrates stencils. */
  if (is_arm64) {
    for (size_t k = 0; k < kHoleFoldMagicTableLen; ++k) {
      const HoleFoldMagicEntry *m = &kHoleFoldMagicTable[k];
      const char *expected_in = expected_stencil_for_fold(m->name);
      if (expected_in == NULL) continue;   /* no stencil yet — skip */

      for (size_t i = 0; i < n_stencils; ++i) {
        int count = count_fold_matches_arm64(stencils[i].bytes,
                                              stencils[i].n_bytes,
                                              m->magic);
        int expected =
            (strcmp(stencils[i].name, expected_in) == 0) ? 1 : 0;

        if (count != expected) {
          fprintf(stderr,
                  "  VIOLATION: %s in %s: expected %d, found %d "
                  "(fold imm12)\n",
                  m->name, stencils[i].name, expected, count);
          errors++;
        } else if (count > 0) {
          fprintf(stderr,
                  "  ok: %s found %dx in %s (fold imm12, expected)\n",
                  m->name, count, stencils[i].name);
        }
      }
    }
  }

  if (errors > 0) {
    fprintf(stderr, "%s: FAIL — %d violation(s)\n", g_argv0, errors);
    return 1;
  }
  fprintf(stderr, "%s: PASS\n", g_argv0);
  return 0;
}
