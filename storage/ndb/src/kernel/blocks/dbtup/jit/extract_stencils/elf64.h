/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * (Same license as bytecode1.h.)
 */

/*
 * RONDB-1056 Phase 2 — minimal ELF64 subset for the stencil
 * extractor.
 *
 * macOS doesn't ship <elf.h>; rather than make the extractor
 * Linux-only or pull in libelf as a dependency, we provide just
 * enough of the ELF64 spec to read the .o files clang produces:
 *
 *   - ELF header (Elf64_Ehdr)
 *   - Section header (Elf64_Shdr)
 *   - Symbol (Elf64_Sym)
 *   - Relocation with addend (Elf64_Rela)
 *
 * Plus the relocation type and symbol-binding constants we
 * actually classify against.
 *
 * Reference: System V Application Binary Interface, AMD64
 * Architecture Processor Supplement, Draft Version 0.99.6, plus
 * the AArch64 ELF supplement.
 */

#ifndef NDB_JIT_ELF64_H
#define NDB_JIT_ELF64_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/* Type aliases (match the names <elf.h> uses).                       */
/* ------------------------------------------------------------------ */

typedef uint64_t Elf64_Addr;
typedef uint64_t Elf64_Off;
typedef uint16_t Elf64_Half;
typedef uint32_t Elf64_Word;
typedef int32_t  Elf64_Sword;
typedef uint64_t Elf64_Xword;
typedef int64_t  Elf64_Sxword;

/* ------------------------------------------------------------------ */
/* ELF identification.                                                */
/* ------------------------------------------------------------------ */

#define EI_NIDENT 16
#define EI_MAG0      0
#define EI_MAG1      1
#define EI_MAG2      2
#define EI_MAG3      3
#define EI_CLASS     4
#define EI_DATA      5
#define EI_VERSION   6

#define ELFMAG0    0x7f
#define ELFMAG1    'E'
#define ELFMAG2    'L'
#define ELFMAG3    'F'

#define ELFCLASS64    2
#define ELFDATA2LSB   1

/* e_type. */
#define ET_REL        1   /* relocatable */

/* e_machine. */
#define EM_X86_64    62
#define EM_AARCH64  183

/* ------------------------------------------------------------------ */
/* ELF header.                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
  unsigned char e_ident[EI_NIDENT];
  Elf64_Half    e_type;
  Elf64_Half    e_machine;
  Elf64_Word    e_version;
  Elf64_Addr    e_entry;
  Elf64_Off     e_phoff;
  Elf64_Off     e_shoff;
  Elf64_Word    e_flags;
  Elf64_Half    e_ehsize;
  Elf64_Half    e_phentsize;
  Elf64_Half    e_phnum;
  Elf64_Half    e_shentsize;
  Elf64_Half    e_shnum;
  Elf64_Half    e_shstrndx;
} Elf64_Ehdr;

/* ------------------------------------------------------------------ */
/* Section header.                                                    */
/* ------------------------------------------------------------------ */

/* sh_type. */
#define SHT_NULL      0
#define SHT_PROGBITS  1
#define SHT_SYMTAB    2
#define SHT_STRTAB    3
#define SHT_RELA      4
#define SHT_NOBITS    8

typedef struct {
  Elf64_Word    sh_name;
  Elf64_Word    sh_type;
  Elf64_Xword   sh_flags;
  Elf64_Addr    sh_addr;
  Elf64_Off     sh_offset;
  Elf64_Xword   sh_size;
  Elf64_Word    sh_link;
  Elf64_Word    sh_info;
  Elf64_Xword   sh_addralign;
  Elf64_Xword   sh_entsize;
} Elf64_Shdr;

/* ------------------------------------------------------------------ */
/* Symbol.                                                            */
/* ------------------------------------------------------------------ */

/* st_info bindings (high nibble) and types (low nibble). */
#define ELF64_ST_BIND(info) ((info) >> 4)
#define ELF64_ST_TYPE(info) ((info) & 0xf)

#define STB_LOCAL   0
#define STB_GLOBAL  1
#define STB_WEAK    2

#define STT_NOTYPE  0
#define STT_OBJECT  1
#define STT_FUNC    2
#define STT_SECTION 3

typedef struct {
  Elf64_Word    st_name;
  unsigned char st_info;
  unsigned char st_other;
  Elf64_Half    st_shndx;
  Elf64_Addr    st_value;
  Elf64_Xword   st_size;
} Elf64_Sym;

/* ------------------------------------------------------------------ */
/* Relocation with addend.                                            */
/* ------------------------------------------------------------------ */

#define ELF64_R_SYM(info)  ((info) >> 32)
#define ELF64_R_TYPE(info) ((info) & 0xffffffff)

typedef struct {
  Elf64_Addr    r_offset;
  Elf64_Xword   r_info;
  Elf64_Sxword  r_addend;
} Elf64_Rela;

/* ------------------------------------------------------------------ */
/* x86_64 relocation types we care about.                             */
/* ------------------------------------------------------------------ */

#define R_X86_64_NONE        0
#define R_X86_64_64          1   /* 8-byte absolute */
#define R_X86_64_PC32        2   /* 4-byte PC-relative */
#define R_X86_64_PLT32       4   /* 4-byte PC-relative to PLT entry */
#define R_X86_64_GOTPCREL    9
#define R_X86_64_32         10   /* 4-byte unsigned absolute */
#define R_X86_64_32S        11   /* 4-byte signed absolute */

/* ------------------------------------------------------------------ */
/* AArch64 relocation types we care about.                            */
/* ------------------------------------------------------------------ */

#define R_AARCH64_NONE                  0
#define R_AARCH64_ABS64               257  /* 64-bit absolute */
#define R_AARCH64_ABS32               258
#define R_AARCH64_ADR_PREL_PG_HI21    275  /* page-pair high 21 bits */
#define R_AARCH64_ADD_ABS_LO12_NC     277  /* page-pair low 12 bits */
#define R_AARCH64_LDST64_ABS_LO12_NC  286
#define R_AARCH64_CALL26              283  /* unconditional branch */
#define R_AARCH64_JUMP26              282
#define R_AARCH64_ADR_GOT_PAGE        311  /* GOT page-pair high 21 bits */
#define R_AARCH64_LD64_GOT_LO12_NC    312  /* GOT page-pair low 12 bits */

#ifdef __cplusplus
}
#endif

#endif /* NDB_JIT_ELF64_H */
