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
 * RONDB-1056 Phase 2 — stencil extractor.
 *
 * Reads an ELF64 .o produced by clang from stencils_src.c, walks
 * the symbol table for op_* entries, and emits a C header with
 * per-stencil byte arrays + Hole tables.
 *
 * Pure C11. No libelf, no binutils, no third-party deps. Cross-
 * platform: builds and runs on macOS hosts as well as Linux.
 *
 * Usage:
 *   extract_stencils <input.o> <arch> <output.h>
 *
 *   <arch>   x86_64 | arm64
 *   <input>  path to clang-compiled stencils.o
 *   <output> path to write the generated header
 */

#include "../hole_kinds.h"   /* HoleKind enum, Hole/Stencil structs, name + magic tables */
#include "elf64.h"

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Diagnostics.                                                       */
/* ------------------------------------------------------------------ */

static const char *g_argv0 = "extract_stencils";

__attribute__((format(printf, 1, 2)))
__attribute__((noreturn))
static void die(const char *fmt, ...) {
  va_list ap;
  fprintf(stderr, "%s: ", g_argv0);
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(1);
}

/* ------------------------------------------------------------------ */
/* Slurp the input file into memory.                                  */
/* ------------------------------------------------------------------ */

typedef struct {
  uint8_t *data;
  size_t   size;
} Blob;

static Blob slurp(const char *path) {
  FILE *f = fopen(path, "rb");
  if (!f) die("cannot open %s: %s", path, strerror(errno));
  if (fseek(f, 0, SEEK_END) != 0) die("fseek %s: %s", path, strerror(errno));
  long sz = ftell(f);
  if (sz < 0) die("ftell %s: %s", path, strerror(errno));
  if (fseek(f, 0, SEEK_SET) != 0) die("fseek %s: %s", path, strerror(errno));

  uint8_t *data = (uint8_t *)malloc((size_t)sz);
  if (!data) die("oom slurping %s (%ld bytes)", path, sz);
  size_t got = fread(data, 1, (size_t)sz, f);
  if (got != (size_t)sz) die("short read on %s: %zu of %ld", path, got, sz);
  fclose(f);

  Blob b = { .data = data, .size = (size_t)sz };
  return b;
}

/* ------------------------------------------------------------------ */
/* ELF parsing helpers — bounds-checked accessors.                    */
/* ------------------------------------------------------------------ */

static const void *blob_at(const Blob *b, size_t off, size_t len) {
  if (off > b->size || len > b->size - off) {
    die("ELF parse: range [%zu, %zu) out of bounds (file size %zu)",
        off, off + len, b->size);
  }
  return b->data + off;
}

static const Elf64_Ehdr *parse_ehdr(const Blob *b, uint16_t want_machine) {
  if (b->size < sizeof(Elf64_Ehdr)) die("ELF too small (%zu bytes)", b->size);
  const Elf64_Ehdr *e = (const Elf64_Ehdr *)b->data;

  if (e->e_ident[EI_MAG0] != ELFMAG0 || e->e_ident[EI_MAG1] != ELFMAG1 ||
      e->e_ident[EI_MAG2] != ELFMAG2 || e->e_ident[EI_MAG3] != ELFMAG3) {
    die("not an ELF file (bad magic)");
  }
  if (e->e_ident[EI_CLASS] != ELFCLASS64) die("not ELF64");
  if (e->e_ident[EI_DATA]  != ELFDATA2LSB) die("not little-endian");
  if (e->e_type != ET_REL) die("not a relocatable .o (e_type=%u)", e->e_type);
  if (e->e_machine != want_machine) {
    die("e_machine=%u, expected %u for the requested arch",
        e->e_machine, want_machine);
  }
  if (e->e_shentsize != sizeof(Elf64_Shdr)) {
    die("unexpected section-header size %u", e->e_shentsize);
  }
  return e;
}

/* ------------------------------------------------------------------ */
/* Section-table walk: return pointer to the array of Shdrs, plus the */
/* string-table for section names.                                    */
/* ------------------------------------------------------------------ */

typedef struct {
  const Elf64_Shdr *shdrs;     /* contiguous array of e_shnum entries */
  size_t            n_shdrs;
  const char       *shstr;     /* section-name string table */
  size_t            shstr_len;
} SectionTable;

static SectionTable parse_sections(const Blob *b, const Elf64_Ehdr *e) {
  SectionTable st;
  st.n_shdrs = e->e_shnum;
  st.shdrs   = (const Elf64_Shdr *)blob_at(
      b, e->e_shoff, st.n_shdrs * sizeof(Elf64_Shdr));

  if (e->e_shstrndx >= st.n_shdrs) die("e_shstrndx out of range");
  const Elf64_Shdr *shstr_hdr = &st.shdrs[e->e_shstrndx];
  st.shstr     = (const char *)blob_at(b, shstr_hdr->sh_offset, shstr_hdr->sh_size);
  st.shstr_len = shstr_hdr->sh_size;
  return st;
}

static const char *shdr_name(const SectionTable *st, const Elf64_Shdr *s) {
  if (s->sh_name >= st->shstr_len) die("section name offset out of range");
  return st->shstr + s->sh_name;
}

static const Elf64_Shdr *find_section(const SectionTable *st, const char *name) {
  for (size_t i = 0; i < st->n_shdrs; ++i) {
    if (strcmp(shdr_name(st, &st->shdrs[i]), name) == 0) {
      return &st->shdrs[i];
    }
  }
  return NULL;
}

/* ------------------------------------------------------------------ */
/* Symbol table walk.                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
  const Elf64_Sym *syms;
  size_t           n_syms;
  const char      *strtab;
  size_t           strtab_len;
  uint16_t         text_shndx;
} SymbolTable;

static SymbolTable parse_symbols(const Blob *b, const SectionTable *st) {
  const Elf64_Shdr *symtab_hdr = find_section(st, ".symtab");
  if (!symtab_hdr) die("missing .symtab");
  if (symtab_hdr->sh_type != SHT_SYMTAB) die(".symtab has wrong type");
  if (symtab_hdr->sh_entsize != sizeof(Elf64_Sym)) {
    die(".symtab entry size %lu != %zu",
        (unsigned long)symtab_hdr->sh_entsize, sizeof(Elf64_Sym));
  }

  /* The strtab linked from .symtab is what we use for symbol names. */
  if (symtab_hdr->sh_link >= st->n_shdrs) die("symtab sh_link out of range");
  const Elf64_Shdr *strtab_hdr = &st->shdrs[symtab_hdr->sh_link];

  SymbolTable s;
  s.syms       = (const Elf64_Sym *)blob_at(
      b, symtab_hdr->sh_offset, symtab_hdr->sh_size);
  s.n_syms     = symtab_hdr->sh_size / sizeof(Elf64_Sym);
  s.strtab     = (const char *)blob_at(
      b, strtab_hdr->sh_offset, strtab_hdr->sh_size);
  s.strtab_len = strtab_hdr->sh_size;

  /* Locate .text's section index — needed to filter symbols defined
   * in .text from forward declarations / extern references. */
  const Elf64_Shdr *text_hdr = find_section(st, ".text");
  if (!text_hdr) die("missing .text");
  s.text_shndx = (uint16_t)(text_hdr - st->shdrs);

  return s;
}

static const char *sym_name(const SymbolTable *s, const Elf64_Sym *sym) {
  if (sym->st_name >= s->strtab_len) die("symbol name offset out of range");
  return s->strtab + sym->st_name;
}

/* ------------------------------------------------------------------ */
/* .rela.text + .text accessors.                                      */
/* ------------------------------------------------------------------ */

typedef struct {
  const Elf64_Rela *relas;
  size_t            n_relas;
  const uint8_t    *text_bytes;
  size_t            text_size;
} TextRelas;

static TextRelas parse_text_relas(const Blob *b, const SectionTable *st) {
  TextRelas tr;
  const Elf64_Shdr *text_hdr = find_section(st, ".text");
  if (!text_hdr) die("missing .text");
  tr.text_bytes = (const uint8_t *)blob_at(b, text_hdr->sh_offset, text_hdr->sh_size);
  tr.text_size  = text_hdr->sh_size;

  const Elf64_Shdr *rela_hdr = find_section(st, ".rela.text");
  if (!rela_hdr) die("missing .rela.text");
  if (rela_hdr->sh_type != SHT_RELA) die(".rela.text has wrong type");
  if (rela_hdr->sh_entsize != sizeof(Elf64_Rela)) {
    die(".rela.text entry size %lu != %zu",
        (unsigned long)rela_hdr->sh_entsize, sizeof(Elf64_Rela));
  }
  tr.relas   = (const Elf64_Rela *)blob_at(
      b, rela_hdr->sh_offset, rela_hdr->sh_size);
  tr.n_relas = rela_hdr->sh_size / sizeof(Elf64_Rela);
  return tr;
}

/* ------------------------------------------------------------------ */
/* Hole-kind lookup by relocation target symbol name.                 */
/* ------------------------------------------------------------------ */

static int starts_with(const char *s, const char *prefix) {
  return strncmp(s, prefix, strlen(prefix)) == 0;
}

/* Returns HK_* on match, 0 if the name isn't in the HOLE_* table. */
static uint8_t lookup_hole_kind(const char *symbol_name) {
  for (size_t i = 0; i < kHoleSymbolTableLen; ++i) {
    if (strcmp(kHoleSymbolTable[i].name, symbol_name) == 0) {
      return kHoleSymbolTable[i].kind;
    }
  }
  return 0;   /* not a HOLE_* symbol */
}

/* ------------------------------------------------------------------ */
/* Per-stencil extraction.                                            */
/* ------------------------------------------------------------------ */

/* Maximum holes per stencil — generous for Phase 1's set (the
 * branch stencil has 4); pump up later if a future stencil needs more. */
#define MAX_HOLES_PER_STENCIL 16

typedef struct {
  Hole holes[MAX_HOLES_PER_STENCIL];
  uint8_t n_holes;
} HoleVec;

typedef struct {
  const char *name;
  const uint8_t *bytes;     /* pointer into the input ELF's .text */
  uint16_t       n_bytes;   /* after trailing-jmp strip / terminator override */
  HoleVec        holes;
} ExtractedStencil;

/* Decide how to handle a given stencil's trailing instruction(s):
 *   STRIP_TAIL     — ordinary stencil, strip the trailing 5-byte jmp to next.
 *   KEEP_ALL       — branch stencil, keep both the jge and the jmp.
 *   TERMINATOR     — op_skip / op_exit, override bytes with engine-required
 *                    pop-r12-then-ret sequence (3 bytes on x86_64). */
typedef enum {
  TAIL_STRIP_TAIL,
  TAIL_KEEP_ALL,
  TAIL_TERMINATOR,
} TailPolicy;

static TailPolicy classify_tail(const char *name) {
  if (strcmp(name, "op_skip") == 0) return TAIL_TERMINATOR;
  if (strcmp(name, "op_exit") == 0) return TAIL_TERMINATOR;
  if (starts_with(name, "op_branch_")) return TAIL_KEEP_ALL;
  return TAIL_STRIP_TAIL;
}

/* x86_64 hand-coded terminator: pop r12; ret. Engine-required —
 * matches the preamble (push r12; mov r12, rdi) emitted at offset 0
 * of every compiled program. See jit1.c kPreamble[]. */
static const uint8_t kX86Terminator[]  = { 0x41, 0x5c, 0xc3 };

/* aarch64 hand-coded terminator: ldp x20, x30, [sp], #16 ; ret.
 *
 * Engine-required — must match the aarch64 preamble (stp x20, x30,
 * [sp, #-16]! ; mov x20, x0) emitted at offset 0 of every compiled
 * program. The preamble lives in jit1.c, added in Phase 2 Day 4
 * when the engine is wired to the generated stencils — until then
 * these bytes are extractor-only and don't get executed.
 *
 * Encoding cross-check (little-endian, MSB byte last):
 *   0xA8C17BF4 = ldp x20, x30, [sp], #16   (post-index)
 *   0xD65F03C0 = ret  (= ret x30) */
static const uint8_t kArm64Terminator[] = {
  0xF4, 0x7B, 0xC1, 0xA8,   /* ldp x20, x30, [sp], #16 */
  0xC0, 0x03, 0x5F, 0xD6,   /* ret */
};

/* Trailing-jmp width by arch — informational; the strip code derives
 * the same value from the relocation offset. Kept here as documentation
 * of the model. */
__attribute__((unused))
static const size_t kX86TrailJmpSize    = 5;   /* e9 ?? ?? ?? ?? */
__attribute__((unused))
static const size_t kArm64TrailJmpSize  = 4;   /* one b imm26 instruction */

static void add_hole(HoleVec *v, uint16_t off, uint8_t kind, uint8_t width) {
  if (v->n_holes >= MAX_HOLES_PER_STENCIL) {
    die("too many holes in one stencil (raise MAX_HOLES_PER_STENCIL)");
  }
  v->holes[v->n_holes++] = (Hole){
    .byte_offset = off, .kind = kind, .width = width,
  };
}

/* Sort holes by byte_offset for deterministic output. */
static int hole_cmp(const void *a, const void *b) {
  const Hole *ha = (const Hole *)a;
  const Hole *hb = (const Hole *)b;
  return (int)ha->byte_offset - (int)hb->byte_offset;
}

/* Extract one op_* symbol from the ELF: capture byte range, walk
 * relocations, classify, strip / override per TailPolicy. */
static ExtractedStencil extract_one_x86(
    const SymbolTable *symbols,
    const TextRelas *tr,
    const Elf64_Sym *sym,
    const char *name)
{
  ExtractedStencil out = {0};
  out.name    = name;
  out.bytes   = tr->text_bytes + sym->st_value;
  out.n_bytes = (uint16_t)sym->st_size;

  TailPolicy policy = classify_tail(name);

  if (policy == TAIL_TERMINATOR) {
    /* Override bytes entirely. Hard-coded engine-required terminator. */
    out.bytes   = kX86Terminator;
    out.n_bytes = (uint16_t)sizeof(kX86Terminator);
    return out;
  }

  /* Walk all relocations whose r_offset falls in this symbol's range. */
  uint64_t lo = sym->st_value;
  uint64_t hi = sym->st_value + sym->st_size;

  /* For STRIP_TAIL, find the trailing-jmp relocation: it's the relocation
   * with the highest r_offset that targets `next`. We strip it and
   * everything from (its offset - 1) onward (the e9 opcode byte plus the
   * 4-byte rel32 displacement = 5 bytes). */
  size_t   trail_idx = SIZE_MAX;
  uint64_t trail_off = 0;

  if (policy == TAIL_STRIP_TAIL) {
    for (size_t i = 0; i < tr->n_relas; ++i) {
      const Elf64_Rela *r = &tr->relas[i];
      if (r->r_offset < lo || r->r_offset >= hi) continue;
      uint32_t type = (uint32_t)ELF64_R_TYPE(r->r_info);
      if (type != R_X86_64_PLT32) continue;
      const Elf64_Sym *target = &symbols->syms[ELF64_R_SYM(r->r_info)];
      const char *tname = sym_name(symbols, target);
      if (strcmp(tname, "next") != 0) continue;
      if (trail_idx == SIZE_MAX || r->r_offset > trail_off) {
        trail_idx = i;
        trail_off = r->r_offset;
      }
    }
    if (trail_idx == SIZE_MAX) {
      die("%s: TAIL_STRIP_TAIL but no trailing PLT32 reloc against `next`",
          name);
    }
    /* Sanity: the byte at (trail_off - 1) should be 0xe9 (jmp rel32). */
    uint64_t opcode_off = trail_off - 1;
    if (tr->text_bytes[opcode_off] != 0xe9) {
      die("%s: expected 0xe9 (jmp rel32) at trailing tail, got 0x%02x",
          name, tr->text_bytes[opcode_off]);
    }
    /* Strip: new size = (opcode_off - lo). */
    out.n_bytes = (uint16_t)(opcode_off - lo);
  }
  /* TAIL_KEEP_ALL: leave out.n_bytes alone; both the jge and the jmp
   * are part of the captured bytes. */

  /* Now classify the remaining (non-stripped) relocations. */
  for (size_t i = 0; i < tr->n_relas; ++i) {
    if (i == trail_idx) continue;     /* already consumed by strip */
    const Elf64_Rela *r = &tr->relas[i];
    if (r->r_offset < lo) continue;
    if (policy == TAIL_STRIP_TAIL) {
      if (r->r_offset >= lo + out.n_bytes) continue;  /* in the stripped part */
    } else {
      if (r->r_offset >= hi) continue;
    }

    uint32_t type = (uint32_t)ELF64_R_TYPE(r->r_info);
    const Elf64_Sym *target = &symbols->syms[ELF64_R_SYM(r->r_info)];
    const char *tname = sym_name(symbols, target);

    uint16_t local_off = (uint16_t)(r->r_offset - lo);

    /* Branch stencil: a PLT32 relocation against `next` is the
     * fall-through fixup. A PLT32 against any HOLE_*_TGT symbol is
     * the taken-branch fixup — its kind comes from the symbol table
     * (HK_BRANCH_TAKE) rather than being hardcoded to BLT, so new
     * branch opcodes light up with no extractor change required. */
    if (policy == TAIL_KEEP_ALL) {
      if (type == R_X86_64_PLT32 && strcmp(tname, "next") == 0) {
        add_hole(&out.holes, local_off, HK_BRANCH_FALL, 4);
        continue;
      }
      uint8_t lookup_kind = lookup_hole_kind(tname);
      if (type == R_X86_64_PLT32 && lookup_kind == HK_BRANCH_TAKE) {
        add_hole(&out.holes, local_off, HK_BRANCH_TAKE, 4);
        continue;
      }
    }

    /* Operand holes: relocation against one of the HOLE_* symbols
     * declared in stencils_src.c. */
    uint8_t kind = lookup_hole_kind(tname);
    if (kind != 0) {
      if (type != R_X86_64_32 && type != R_X86_64_32S) {
        die("%s: hole %s has unexpected reloc type %u (want R_X86_64_32 or 32S)",
            name, tname, type);
      }
      add_hole(&out.holes, local_off, kind, 4);
      continue;
    }

    /* Anything else is unexpected. */
    die("%s: unrecognised relocation: type=%u target=%s offset=%llu",
        name, type, tname, (unsigned long long)r->r_offset);
  }

  /* Sort holes deterministically. */
  qsort(out.holes.holes, out.holes.n_holes, sizeof(Hole), hole_cmp);
  return out;
}

/* ------------------------------------------------------------------ */
/* aarch64 per-stencil extraction.                                    */
/*                                                                    */
/* Two-pass walk:                                                     */
/*                                                                    */
/*   Pass 1 — relocation classification.                              */
/*     • TAIL_STRIP_TAIL: find the trailing R_AARCH64_CALL26/JUMP26   */
/*       reloc against `next`; strip its 4-byte b instruction.        */
/*     • TAIL_KEEP_ALL (branch stencil): keep both unconditional      */
/*       branches; classify the one against `next` as HK_BRANCH_FALL  */
/*       and the one against `HOLE_BLT_TGT` as HK_BRANCH_TAKE.        */
/*     • TAIL_TERMINATOR: replace bytes wholesale (handled before     */
/*       the walk — see top of function).                             */
/*                                                                    */
/*   Pass 2 — magic-byte chain scan for operand holes.                */
/*     Walk every 4-byte instruction in the trimmed stencil bytes.    */
/*     Decode movz / movk and accumulate per-Rd 64-bit values with    */
/*     per-slot byte offsets. When a register's accumulated value     */
/*     equals one of the magics in kHoleMagicTable, emit 4 Hole       */
/*     entries — one per imm16 slot (slots 0/16/32/48), each pointing */
/*     at the start of its movz/movk instruction, all sharing the     */
/*     same kind. The engine sees four holes of the same kind and     */
/*     patches the imm16 fields slot-by-slot.                         */
/*                                                                    */
/*     movn-based chains are NOT detected: clang only chooses movn    */
/*     when more than half the bits are 1, and our magics are random  */
/*     enough to avoid that. If a future magic happens to trigger     */
/*     movn lowering, the collision audit (Phase 2 Day 4) will flag   */
/*     it as zero matches; remedy is to regenerate the magic.         */
/* ------------------------------------------------------------------ */

static ExtractedStencil extract_one_arm64(
    const SymbolTable *symbols,
    const TextRelas *tr,
    const Elf64_Sym *sym,
    const char *name)
{
  ExtractedStencil out = {0};
  out.name    = name;
  out.bytes   = tr->text_bytes + sym->st_value;
  out.n_bytes = (uint16_t)sym->st_size;

  TailPolicy policy = classify_tail(name);

  if (policy == TAIL_TERMINATOR) {
    out.bytes   = kArm64Terminator;
    out.n_bytes = (uint16_t)sizeof(kArm64Terminator);
    return out;
  }

  uint64_t lo = sym->st_value;
  uint64_t hi = sym->st_value + sym->st_size;

  size_t   trail_idx = SIZE_MAX;
  uint64_t trail_off = 0;

  if (policy == TAIL_STRIP_TAIL) {
    for (size_t i = 0; i < tr->n_relas; ++i) {
      const Elf64_Rela *r = &tr->relas[i];
      if (r->r_offset < lo || r->r_offset >= hi) continue;
      uint32_t type = (uint32_t)ELF64_R_TYPE(r->r_info);
      if (type != R_AARCH64_CALL26 && type != R_AARCH64_JUMP26) continue;
      const Elf64_Sym *target = &symbols->syms[ELF64_R_SYM(r->r_info)];
      if (strcmp(sym_name(symbols, target), "next") != 0) continue;
      if (trail_idx == SIZE_MAX || r->r_offset > trail_off) {
        trail_idx = i;
        trail_off = r->r_offset;
      }
    }
    if (trail_idx == SIZE_MAX) {
      die("%s: TAIL_STRIP_TAIL but no trailing CALL26/JUMP26 reloc against `next`",
          name);
    }
    if (trail_off + 4 > hi) {
      die("%s: trailing branch reloc at offset %llu does not fit (sym size=%llu)",
          name, (unsigned long long)trail_off,
          (unsigned long long)sym->st_size);
    }
    /* Sanity: top 6 bits of the high byte should be 000101 (b imm26) —
     * stored little-endian, the high byte is at trail_off+3 and must
     * be in 0x14..0x17. */
    uint8_t high_byte = tr->text_bytes[trail_off + 3];
    if ((high_byte & 0xFC) != 0x14) {
      die("%s: expected b imm26 (top byte 0x14-0x17) at trailing tail, got 0x%02x",
          name, high_byte);
    }
    out.n_bytes = (uint16_t)(trail_off - lo);
  }

  /* Pass 1: classify relocations within the trimmed byte range. */
  for (size_t i = 0; i < tr->n_relas; ++i) {
    if (i == trail_idx) continue;     /* already consumed by strip */
    const Elf64_Rela *r = &tr->relas[i];
    if (r->r_offset < lo) continue;
    if (r->r_offset >= lo + out.n_bytes) continue;

    uint32_t type = (uint32_t)ELF64_R_TYPE(r->r_info);
    const Elf64_Sym *target = &symbols->syms[ELF64_R_SYM(r->r_info)];
    const char *tname = sym_name(symbols, target);
    uint16_t local_off = (uint16_t)(r->r_offset - lo);

    if (policy == TAIL_KEEP_ALL) {
      if ((type == R_AARCH64_CALL26 || type == R_AARCH64_JUMP26) &&
          strcmp(tname, "next") == 0) {
        add_hole(&out.holes, local_off, HK_BRANCH_FALL, 4);
        continue;
      }
      uint8_t lookup_kind = lookup_hole_kind(tname);
      if ((type == R_AARCH64_CALL26 || type == R_AARCH64_JUMP26) &&
          lookup_kind == HK_BRANCH_TAKE) {
        add_hole(&out.holes, local_off, HK_BRANCH_TAKE, 4);
        continue;
      }
    }

    /* On aarch64 operand holes use magic-byte chains, NOT relocations.
     * Anything reaching this point is unexpected — diagnose. */
    die("%s: unrecognised aarch64 relocation: type=%u target=%s offset=%llu",
        name, type, tname, (unsigned long long)r->r_offset);
  }

  /* Pass 2: scan stencil bytes for magic-byte movz/movk chains.
   *
   * reg_value[Rd]    — current accumulated 64-bit value for register Rd.
   * reg_slot_off[Rd] — byte offset (within the stencil) of the
   *                    movz/movk instruction that wrote each slot.
   * reg_slot_seen[Rd]— bitmask of slots written (0xF = all four). */
  uint64_t reg_value[32];
  uint16_t reg_slot_off[32][4];
  uint8_t  reg_slot_seen[32];
  for (int i = 0; i < 32; ++i) {
    reg_value[i]     = 0;
    reg_slot_seen[i] = 0;
    for (int s = 0; s < 4; ++s) reg_slot_off[i][s] = 0;
  }

  for (uint16_t off = 0; off + 4 <= out.n_bytes; off += 4) {
    const uint8_t *p = out.bytes + off;
    uint32_t insn = (uint32_t)p[0]        |
                    ((uint32_t)p[1] << 8) |
                    ((uint32_t)p[2] << 16)|
                    ((uint32_t)p[3] << 24);

    /* Decode movz / movk / movn (64-bit form, sf=1).
     *   movz: 1 10 100101 hw imm16 Rd  → bits 31..23 == 0b110100101
     *   movk: 1 11 100101 hw imm16 Rd  → bits 31..23 == 0b111100101
     *   movn: 1 00 100101 hw imm16 Rd  → bits 31..23 == 0b100100101 */
    int is_movz = (insn & 0xFF800000) == 0xD2800000;
    int is_movk = (insn & 0xFF800000) == 0xF2800000;
    int is_movn = (insn & 0xFF800000) == 0x92800000;

    if (is_movn) {
      /* movn-based chain: we lack movz/movk slot offsets to patch
       * the slots that movn implicitly fills with 1s. Reset the
       * register's bookkeeping so we don't mistakenly match a
       * subsequent movk-only sequence. */
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
      /* movz starts a fresh chain on Rd: zero everything else,
       * record this slot's byte offset. */
      reg_value[Rd]      = (uint64_t)imm16 << (hw * 16);
      reg_slot_seen[Rd]  = (uint8_t)(1u << hw);
      for (int s = 0; s < 4; ++s) reg_slot_off[Rd][s] = 0;
      reg_slot_off[Rd][hw] = off;
    } else { /* movk */
      uint64_t mask = ~((uint64_t)0xFFFFu << (hw * 16));
      reg_value[Rd] = (reg_value[Rd] & mask) |
                      ((uint64_t)imm16 << (hw * 16));
      reg_slot_seen[Rd] |= (uint8_t)(1u << hw);
      reg_slot_off[Rd][hw] = off;
    }

    if (reg_slot_seen[Rd] == 0xF) {
      /* All four slots seen — try to match against the magic table. */
      for (size_t k = 0; k < kHoleMagicTableLen; ++k) {
        if (kHoleMagicTable[k].magic == reg_value[Rd]) {
          uint8_t kind = kHoleMagicTable[k].kind;
          for (int s = 0; s < 4; ++s) {
            add_hole(&out.holes, reg_slot_off[Rd][s], kind, 4);
          }
          /* Reset to avoid double-matching if the same chain
           * recurs via additional movks later. */
          reg_slot_seen[Rd] = 0;
          reg_value[Rd]     = 0;
          break;
        }
      }
    }
  }

  qsort(out.holes.holes, out.holes.n_holes, sizeof(Hole), hole_cmp);
  return out;
}

/* ------------------------------------------------------------------ */
/* Header emission.                                                   */
/* ------------------------------------------------------------------ */

/* Bytecode-1 OpKind values (mirror of bytecode1.h, locally so the
 * extractor doesn't need to pull in that header). */
typedef struct {
  const char *name;       /* matches the op_* symbol */
  const char *opkind_id;  /* matches the OpKind enum identifier */
} OpkindMap;

static const OpkindMap kOpkindMap[] = {
  { "op_load_const_int",      "OP_LOAD_CONST_INT"     },
  { "op_load_col_int",        "OP_LOAD_COL_INT"       },
  { "op_mov_int_int",         "OP_MOV_INT_INT"        },
  { "op_add_int_int",         "OP_ADD_INT_INT"        },
  { "op_sum_bigint",          "OP_SUM_BIGINT"         },
  { "op_branch_lt_int_int",   "OP_BRANCH_LT_INT_INT"  },
  { "op_branch_le_int_int",   "OP_BRANCH_LE_INT_INT"  },
  { "op_branch_eq_int_int",   "OP_BRANCH_EQ_INT_INT"  },
  { "op_branch_gt_int_int",   "OP_BRANCH_GT_INT_INT"  },
  { "op_branch_ge_int_int",   "OP_BRANCH_GE_INT_INT"  },
  { "op_branch_ne_int_int",   "OP_BRANCH_NE_INT_INT"  },
  { "op_skip",                "OP_SKIP"               },
  { "op_exit",                "OP_EXIT"               },
  { "op_minus_int_int",       "OP_MINUS_INT_INT"      },
  { "op_mul_int_int",         "OP_MUL_INT_INT"        },
};
static const size_t kOpkindMapLen = sizeof(kOpkindMap) / sizeof(kOpkindMap[0]);

static const char *opkind_id_for(const char *name) {
  for (size_t i = 0; i < kOpkindMapLen; ++i) {
    if (strcmp(kOpkindMap[i].name, name) == 0) return kOpkindMap[i].opkind_id;
  }
  return NULL;
}

static const char *kind_name(uint8_t kind) {
  switch (kind) {
    case HK_OP_A:        return "HK_OP_A";
    case HK_OP_B:        return "HK_OP_B";
    case HK_OP_C:        return "HK_OP_C";
    case HK_OP_IMM:      return "HK_OP_IMM";
    case HK_BRANCH_FALL: return "HK_BRANCH_FALL";
    case HK_BRANCH_TAKE: return "HK_BRANCH_TAKE";
    default:             return "?";
  }
}

static void emit_header(FILE *out,
                        const char *arch_predef,
                        const char *guard_macro,
                        const ExtractedStencil *stencils,
                        size_t n_stencils) {
  fprintf(out,
          "/* GENERATED FILE. Do not edit.\n"
          " * Source: stencils_src.c\n"
          " * Toolchain: clang version 20.1.8 (pinned)\n"
          " * Extractor: extract_stencils.c (RONDB-1056 Phase 2)\n"
          " *\n"
          " * Re-generate via:\n"
          " *   cmake --build . --target regen-stencils -DRONDB_REGEN_STENCILS=ON\n"
          " */\n\n");
  fprintf(out, "#ifndef %s\n", guard_macro);
  fprintf(out, "#define %s\n\n", guard_macro);
  fprintf(out,
          "#if !defined(%s)\n"
          "#error \"%s is only valid on %s build targets.\"\n"
          "#endif\n\n",
          arch_predef, guard_macro, arch_predef);
  fprintf(out,
          "#include <stddef.h>\n"
          "#include <stdint.h>\n\n"
          "#include \"bytecode1.h\"\n"
          "#include \"hole_kinds.h\"\n\n");

  /* Per-stencil byte and hole arrays, in input order. */
  for (size_t i = 0; i < n_stencils; ++i) {
    const ExtractedStencil *s = &stencils[i];
    fprintf(out, "/* %s — %u bytes, %u holes */\n",
            s->name, (unsigned)s->n_bytes, (unsigned)s->holes.n_holes);
    fprintf(out, "static const uint8_t bytes_%s[] = {\n  ", s->name);
    for (uint16_t j = 0; j < s->n_bytes; ++j) {
      fprintf(out, "0x%02x,%s",
              s->bytes[j],
              ((j + 1) % 12 == 0) ? "\n  " : " ");
    }
    fprintf(out, "\n};\n");
    if (s->holes.n_holes > 0) {
      fprintf(out, "static const Hole holes_%s[] = {\n", s->name);
      for (uint8_t h = 0; h < s->holes.n_holes; ++h) {
        const Hole *hh = &s->holes.holes[h];
        fprintf(out,
                "  { .byte_offset = %u, .kind = %s, .width = %u },\n",
                hh->byte_offset, kind_name(hh->kind), hh->width);
      }
      fprintf(out, "};\n");
    }
    fprintf(out, "\n");
  }

  /* Top-level g_stencils[] table. */
  fprintf(out,
          "#define STENCIL_(name) \\\n"
          "  { .bytes = bytes_##name, \\\n"
          "    .n_bytes = (uint16_t)sizeof(bytes_##name), \\\n"
          "    .holes = holes_##name, \\\n"
          "    .n_holes = (uint8_t)(sizeof(holes_##name) / sizeof(holes_##name[0])) }\n\n");
  fprintf(out,
          "#define STENCIL_NOHOLES(name) \\\n"
          "  { .bytes = bytes_##name, \\\n"
          "    .n_bytes = (uint16_t)sizeof(bytes_##name), \\\n"
          "    .holes = NULL, \\\n"
          "    .n_holes = 0 }\n\n");
  fprintf(out, "static const Stencil g_stencils[OP_KIND_MAX + 1] = {\n");
  for (size_t i = 0; i < n_stencils; ++i) {
    const char *opkind = opkind_id_for(stencils[i].name);
    if (!opkind) die("no OpKind for %s", stencils[i].name);
    if (stencils[i].holes.n_holes > 0) {
      fprintf(out, "  [%s] = STENCIL_(%s),\n", opkind, stencils[i].name);
    } else {
      fprintf(out, "  [%s] = STENCIL_NOHOLES(%s),\n", opkind, stencils[i].name);
    }
  }
  fprintf(out, "};\n\n");

  fprintf(out, "#endif /* %s */\n", guard_macro);
}

/* ------------------------------------------------------------------ */
/* main.                                                              */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
  g_argv0 = argv[0];
  if (argc != 4) {
    fprintf(stderr,
            "usage: %s <input.o> <arch> <output.h>\n"
            "  arch: x86_64 | arm64\n",
            argv[0]);
    return 2;
  }
  const char *input  = argv[1];
  const char *arch   = argv[2];
  const char *output = argv[3];

  uint16_t want_machine;
  if      (strcmp(arch, "x86_64") == 0) want_machine = EM_X86_64;
  else if (strcmp(arch, "arm64")  == 0) want_machine = EM_AARCH64;
  else die("unknown arch '%s' (must be x86_64 or arm64)", arch);

  Blob blob = slurp(input);
  const Elf64_Ehdr *ehdr = parse_ehdr(&blob, want_machine);
  SectionTable sections = parse_sections(&blob, ehdr);
  SymbolTable  symbols  = parse_symbols(&blob, &sections);
  TextRelas    relas    = parse_text_relas(&blob, &sections);

  /* Walk symbols in order, extract each op_*. */
  ExtractedStencil stencils[16];
  size_t n_stencils = 0;
  for (size_t i = 0; i < symbols.n_syms; ++i) {
    const Elf64_Sym *sym = &symbols.syms[i];
    if (ELF64_ST_TYPE(sym->st_info) != STT_FUNC) continue;
    if (sym->st_shndx != symbols.text_shndx) continue;
    const char *name = sym_name(&symbols, sym);
    if (!starts_with(name, "op_")) continue;
    if (n_stencils >= sizeof(stencils) / sizeof(stencils[0])) {
      die("too many op_* stencils — bump the local stencils[] array");
    }
    if (want_machine == EM_X86_64) {
      stencils[n_stencils++] = extract_one_x86(&symbols, &relas, sym, name);
    } else {
      stencils[n_stencils++] = extract_one_arm64(&symbols, &relas, sym, name);
    }
  }

  /* Emit. */
  const char *arch_predef = (want_machine == EM_X86_64)
      ? "__x86_64__" : "__aarch64__";
  const char *guard_macro = (want_machine == EM_X86_64)
      ? "NDB_JIT_STENCILS_X86_64_H" : "NDB_JIT_STENCILS_ARM64_H";

  FILE *out = fopen(output, "w");
  if (!out) die("cannot open %s for writing: %s", output, strerror(errno));
  emit_header(out, arch_predef, guard_macro, stencils, n_stencils);
  fclose(out);

  fprintf(stderr, "%s: wrote %s (%zu stencils)\n",
          g_argv0, output, n_stencils);

  free(blob.data);
  return 0;
}
