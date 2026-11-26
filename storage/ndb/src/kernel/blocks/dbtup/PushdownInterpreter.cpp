/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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

#include "PushdownInterpreter.hpp"
#include "AggInterpreter.hpp"
#include "VecSearchInterpreter.hpp"
#include "ndbd_malloc.hpp"
#include "../record_types.hpp"
#include "NdbAggregationCommon.hpp"
#include "util/require.h"
#include <Interpreter.hpp>

/* Phase 8 RONDB-1056: standalone agg compiles go through the reuse cache
 * (DbtupJitGlue), which owns the bridge-translate + jit1_compile path. */
#include "DbtupJitGlue.hpp"

void PushdownInterpreter::Destruct(PushdownInterpreter* ptr) {
  if (ptr == nullptr) {
    return;
  }
  ptr->~PushdownInterpreter();
  lc_ndbd_pool_free(ptr);
}

namespace {

/* No SUM / MAX / MIN of the aggregate slot seen yet. */
constexpr Uint32 kNoAggSlotWriter = ~0U;

/* The type of a register, or of a slot, on two paths. */
inline Uint32 joinRegType(Uint32 a, Uint32 b) {
  return (a == b) ? a : NDB_TYPE_UNDEFINED;
}

/*
 * The register types follow the jumps of the program.  kOpSkip jumps
 * forward over its count of words.  The rows leaving kOpEmbeddedInterp
 * continue after the embedded program, skipping the number of words the
 * embedded program wrote to interpreter output 0 (ProcessRec of
 * AggInterpreter and JoinAggInterpreter).  Neither falls through to the
 * next word.  A join point keeps the register types of the rows that
 * jump to the words first..last, where last is ~0U when they may land on
 * any later word.  A word reached on several paths gets the register
 * types the paths agree on.
 */
struct AggJoinPoints {
  static constexpr Uint32 kMaxPoints = 16;
  struct Point {
    Uint32 first;
    Uint32 last;
    Uint32 reg_types[kRegTotal];
  };
  Point m_points[kMaxPoints];
  Uint32 m_count = 0;

  void add(Uint32 first, Uint32 last, const Uint32* reg_types) {
    Point* p = nullptr;
    for (Uint32 i = 0; i < m_count && p == nullptr; i++) {
      if (m_points[i].first == first && m_points[i].last == last) {
        p = &m_points[i];
      }
    }
    if (p == nullptr && m_count == kMaxPoints) {
      /* No room: the last point covers this jump too. */
      p = &m_points[kMaxPoints - 1];
      if (first < p->first) {
        p->first = first;
      }
      p->last = ~0U;
    }
    if (p == nullptr) {
      p = &m_points[m_count++];
      p->first = first;
      p->last = last;
      for (Uint32 r = 0; r < kRegTotal; r++) {
        p->reg_types[r] = reg_types[r];
      }
      return;
    }
    for (Uint32 r = 0; r < kRegTotal; r++) {
      p->reg_types[r] = joinRegType(p->reg_types[r], reg_types[r]);
    }
  }

  /* Set reg_types to the types at the word pos: those of the path from
   * the word before, when `reached`, joined with those of every jump
   * that may land on pos.  They are unknown at a word no path reaches. */
  void enter(Uint32 pos, Uint32* reg_types, bool reached) {
    for (Uint32 i = 0; i < m_count;) {
      Point& p = m_points[i];
      if (p.first <= pos && pos <= p.last) {
        for (Uint32 r = 0; r < kRegTotal; r++) {
          reg_types[r] = reached ? joinRegType(reg_types[r], p.reg_types[r])
                                 : p.reg_types[r];
        }
        reached = true;
      }
      if (p.last <= pos) {
        p = m_points[--m_count];  // No later word to land on
      } else {
        i++;
      }
    }
    if (!reached) {
      for (Uint32 r = 0; r < kRegTotal; r++) {
        reg_types[r] = NDB_TYPE_UNDEFINED;
      }
    }
  }
};

/* Words of a bitmap with a bit for each word of a program. */
constexpr Uint32 kProgramBitmapWords = (MAX_AGG_PROGRAM_WORD_SIZE + 31) / 32;

inline bool testProgramBit(const Uint32* bitmap, Uint32 bit) {
  return ((bitmap[bit >> 5] >> (bit & 31)) & 1) != 0;
}

inline void setProgramBit(Uint32* bitmap, Uint32 bit) {
  bitmap[bit >> 5] |= 1U << (bit & 31);
}

/* A WRITE_INTERPRETER_OUTPUT to interpreter output 0. */
inline bool isOutput0Write(Uint32 instr) {
  return Interpreter::getOpCode(instr) ==
             Interpreter::WRITE_INTERPRETER_OUTPUT &&
         (instr >> 16) == 0;
}

/* The registers (bit r for register r) that an embedded instruction
 * other than LOAD_CONST16 may write: the one in bits 6-8 for the loads
 * below, none for a branch, an exit or an output write, all of them for
 * any other instruction. */
Uint32 embeddedRegWrites(Uint32 instr, bool is_branch) {
  if (is_branch) {
    return 0;
  }
  switch (Interpreter::getOpCode(instr)) {
    case Interpreter::LOAD_CONST_NULL:
    case Interpreter::LOAD_CONST32:
    case Interpreter::LOAD_CONST64:
    case Interpreter::LOAD_DOUBLE_CONST:
    case Interpreter::READ_ATTR_INTO_REG:
    case Interpreter::READ_AGG_REG_TO_REG:
      return 1U << Interpreter::getReg1(instr);
    case Interpreter::EXIT_OK:
    case Interpreter::EXIT_OK_LAST:
    case Interpreter::EXIT_REFUSE:
    case Interpreter::WRITE_INTERPRETER_OUTPUT:
      return 0;
    default:
      return ~0U;
  }
}

/* Set bit skip of skips for the output-0 write of register reg, which
 * holds values[reg] when bit reg of known is set.  A skip of n_skips or
 * more ends the row (embeddedSkipTarget), as
 * AGG_EMBEDDED_INTERP_STOP_PROGRAM does: no word to land on.  Returns
 * false when the register may hold another value. */
bool addEmbeddedSkip(Uint32 reg, Uint32 known, const Uint32* values,
                     Uint32* skips, Uint32 n_skips) {
  if (((known >> reg) & 1) == 0) {
    return false;
  }
  const Uint32 skip = values[reg];
  if (skip != AGG_EMBEDDED_INTERP_STOP_PROGRAM && skip < n_skips) {
    setProgramBit(skips, skip);
  }
  return true;
}

/*
 * The skips the embedded program emb[0..emb_len) may write to
 * interpreter output 0: bit s of skips (kProgramBitmapWords words) for
 * each skip s below n_skips, the words after the embedded program.  Every
 * emitter (CASE and GREATEST / LEAST of RonSQL, CASE of
 * ha_ndbcluster_push_agg, RonSQL's guarded COUNT(*) of a subquery)
 * writes output 0 from a register that a LOAD_CONST16 set.  The walk
 * keeps the value a LOAD_CONST16 gave each register on the straight line
 * since the last word a branch lands on.  A write takes the line's value
 * when the line falls through to it, and the value at each branch to it
 * (the guarded COUNT(*) branches to its write).  Return false, the skips
 * being unknown, when the written register may hold another value, or a
 * branch lands inside an instruction: its operand words would then run
 * as instructions, which the walk does not see.
 */
bool embeddedSkips(const Uint32* emb, Uint32 emb_len, Uint32* skips,
                   Uint32 n_skips) {
  memset(skips, 0, kProgramBitmapWords * sizeof(Uint32));
  if (emb_len > MAX_AGG_PROGRAM_WORD_SIZE ||
      n_skips > MAX_AGG_PROGRAM_WORD_SIZE) {
    return false;
  }
  Uint32 starts[kProgramBitmapWords];
  Uint32 targets[kProgramBitmapWords];
  memset(starts, 0, sizeof(starts));
  memset(targets, 0, sizeof(targets));
  Uint32 values[8] = {0};  // Registers 0-7 (Interpreter::getReg1)
  Uint32 known = 0;
  bool falls_through = true;
  Uint32 pc = 0;
  while (pc < emb_len) {
    const Uint32 instr = emb[pc];
    Interpreter::InstructionPreProcessing processing;
    const Uint32* next = Interpreter::getInstructionPreProcessingInfo(
        const_cast<Uint32*>(&emb[pc]), processing);
    if (next == nullptr) {
      return false;
    }
    setProgramBit(starts, pc);
    if (falls_through && isOutput0Write(instr) &&
        !addEmbeddedSkip(Interpreter::getReg1(instr), known, values, skips,
                         n_skips)) {
      return false;
    }
    if (testProgramBit(targets, pc)) {
      known = 0;  // Paths join here
    }
    const Uint32 op = Interpreter::getOpCode(instr);
    const bool is_branch =
        processing == Interpreter::LABEL_ADDRESS_REPLACEMENT;
    if (op == Interpreter::LOAD_CONST16) {
      values[Interpreter::getReg1(instr)] = instr >> 16;
      known |= 1U << Interpreter::getReg1(instr);
    } else {
      known &= ~embeddedRegWrites(instr, is_branch);
    }
    if (is_branch) {
      /* Forward offset from the branch word, as validateEmbeddedProgram. */
      const Uint32 target = pc + ((instr >> 16) & 0x7FFF);
      if ((instr >> 31) != 0 || target >= emb_len) {
        return false;
      }
      setProgramBit(targets, target);
      if (isOutput0Write(emb[target]) &&
          !addEmbeddedSkip(Interpreter::getReg1(emb[target]), known, values,
                           skips, n_skips)) {
        return false;
      }
    }
    falls_through = op != Interpreter::EXIT_OK &&
                    op != Interpreter::EXIT_OK_LAST &&
                    op != Interpreter::EXIT_REFUSE &&
                    op != Interpreter::BRANCH;
    pc = Uint32(next - emb);
  }
  for (Uint32 i = 0; i < kProgramBitmapWords; i++) {
    if ((targets[i] & ~starts[i]) != 0) {
      return false;
    }
  }
  return true;
}

/*
 * A CASE updates its aggregate slot from every arm (each later arm
 * through NdbAggregator::RepeatAgg), and one arm runs per row.  The typed
 * SUM / MAX / MIN kernels never convert the slot: SumDouble adds to
 * val_double also of a BIGINT slot, SumBigint adds to the bits of a
 * DOUBLE slot.  So a slot is typed only when every SUM / MAX / MIN of it
 * reads a register of one proven type.  The first walk collects that
 * type of each slot in slot_types, the second walk returns it.
 */
Uint32 aggSlotType(Uint32* slot_types, Uint32 value, Uint32 reg_type,
                   bool rewrite) {
  const Uint32 agg_index = value & 0xFFFF;
  if (agg_index >= MAX_AGG_N_RESULTS) {
    return NDB_TYPE_UNDEFINED;
  }
  if (rewrite) {
    return slot_types[agg_index];
  }
  slot_types[agg_index] = (slot_types[agg_index] == kNoAggSlotWriter)
                              ? reg_type
                              : joinRegType(slot_types[agg_index], reg_type);
  return NDB_TYPE_UNDEFINED;
}

/*
 * One walk of OptimizeProgramBuffer over the program.  The first walk
 * (rewrite == false) only fills slot_types, the second rewrites the
 * program.  Both see the same register types.
 */
void optimizeAggProgramWalk(Uint32* prog, Uint32 prog_len, Uint32 start_pos,
                            Uint32* slot_types, bool rewrite) {
  using DataType = Uint32;
  DataType reg_types[kRegTotal];
  for (Uint32 i = 0; i < kRegTotal; i++) {
    reg_types[i] = NDB_TYPE_UNDEFINED;
  }
  AggJoinPoints joins;
  bool reached = true;

  Uint32 exec_pos = start_pos;
  while (exec_pos < prog_len) {
    joins.enter(exec_pos, reg_types, reached);
    reached = true;
    const Uint32 op_pos = exec_pos;
    Uint32 value = prog[exec_pos];
    Uint8 op = (value & 0xFC000000) >> 26;
    Uint32 reg_index, reg_index2;
    DataType type;
    Uint8 new_op = op;

    switch (op) {
      case kOpLoadCol:
        type = AggInterpreterBase::decodeLoadColType(value);
        reg_index = (value & 0x000F0000) >> 16;
        if (type == NDB_TYPE_FLOAT || type == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (type == NDB_TYPE_DECIMAL ||
                   type == NDB_TYPE_DECIMALUNSIGNED) {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
          exec_pos++;
        } else if (type == NDB_TYPE_CHAR ||
                   type == NDB_TYPE_VARCHAR ||
                   type == NDB_TYPE_LONGVARCHAR) {
          // Phase I.6 (F.2-K.4): keep strings out of the BIGINT
          // track — the type-specialised kOpMaxBigint / kOpMinBigint
          // variants only handle numeric AggResItem values.  Leaving
          // the type as UNDEFINED means kOpMax / kOpMin stay generic
          // and dispatch on m_registers[reg].type at runtime to
          // route strings into minMaxString.
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        } else {
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        }
        break;

      case kOpLoadConst:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        if (type == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else {
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        }
        exec_pos += 2;
        break;

      case kOpMov:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        reg_types[reg_index] = reg_types[reg_index2];
        break;

      // Arithmetic ops: optimize to Bigint variant only when BOTH
      // inputs are BIGINT.  Do NOT optimize to Double variants —
      // the generic ops (RegPlusReg etc.) handle mixed BIGINT/DOUBLE
      // operands with proper conversion and overflow checks, while
      // the Double variants read .val_double from both registers
      // unconditionally, which is wrong when one register holds BIGINT.
      case kOpPlus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE &&
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          new_op = kOpPlusDouble;
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
                   reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
                   reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpPlusBigint;
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        } else {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        }
        break;

      case kOpMinus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE &&
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          new_op = kOpMinusDouble;
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
                   reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
                   reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpMinusBigint;
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        } else {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        }
        break;

      case kOpMul:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE &&
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          new_op = kOpMulDouble;
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
                   reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
                   reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpMulBigint;
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        } else {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        }
        break;

      case kOpDiv:
        reg_types[(value >> 12) & 0x0F] = NDB_TYPE_DOUBLE;
        break;

      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
            reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpDivIntBigint;
        }
        reg_types[reg_index] = NDB_TYPE_BIGINT;
        break;

      case kOpMod:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
            reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_UNDEFINED ||
                   reg_types[reg_index2] == NDB_TYPE_UNDEFINED) {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
        } else {
          reg_types[reg_index] = NDB_TYPE_BIGINT;
        }
        break;

      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        type = aggSlotType(slot_types, value, reg_types[reg_index], rewrite);
        if (type == NDB_TYPE_DOUBLE) new_op = kOpSumDouble;
        else if (type == NDB_TYPE_BIGINT) new_op = kOpSumBigint;
        break;

      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        type = aggSlotType(slot_types, value, reg_types[reg_index], rewrite);
        if (type == NDB_TYPE_DOUBLE) new_op = kOpMaxDouble;
        else if (type == NDB_TYPE_BIGINT) new_op = kOpMaxBigint;
        break;

      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        type = aggSlotType(slot_types, value, reg_types[reg_index], rewrite);
        if (type == NDB_TYPE_DOUBLE) new_op = kOpMinDouble;
        else if (type == NDB_TYPE_BIGINT) new_op = kOpMinBigint;
        break;

      case kOpCount:
        break;

      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        const Uint32 emb_end = exec_pos + 1 + emb_len;
        /* A NULL-extended row whose condition JoinAggInterpreter cannot
         * evaluate continues right after the embedded program. */
        joins.add(emb_end, emb_end, reg_types);
        Uint32 skips[kProgramBitmapWords];
        if (emb_end > prog_len ||
            !embeddedSkips(&prog[exec_pos + 1], emb_len, skips,
                           prog_len - emb_end)) {
          joins.add(emb_end, ~0U, reg_types);
        } else {
          for (Uint32 skip = 0; skip < prog_len - emb_end; skip++) {
            if (testProgramBit(skips, skip)) {
              joins.add(emb_end + skip, emb_end + skip, reg_types);
            }
          }
        }
        reached = false;
        exec_pos += emb_len;
        break;
      }

      case kOpSkip:
        joins.add(exec_pos + 1 + (value & 0xFFFF),
                  exec_pos + 1 + (value & 0xFFFF), reg_types);
        reached = false;
        break;

      case kOpSetRegNull:
        break;

      default:
        break;
    }
    if (rewrite && new_op != op) {
      prog[op_pos] = (new_op << 26) | (value & 0x03FFFFFF);
    }
    exec_pos++;
  }
}

/*
 * The kinds of value a register may hold on the paths that reach a word,
 * for checkAggSlotKinds: a string (CHAR, VARCHAR or LONGVARCHAR), a value
 * of another type, or nothing the row loaded.  Register r has the
 * kRegKindBits bits at r * kRegKindBits of a Uint32, so the kinds of two
 * paths join with |, and 0 is a word no row reaches.
 */
constexpr Uint32 kRegKindString = 1;
constexpr Uint32 kRegKindOther = 2;
constexpr Uint32 kRegKindNotLoaded = 4;
constexpr Uint32 kRegKindValue = kRegKindString | kRegKindOther;
constexpr Uint32 kRegKindBits = 4;
static_assert(kRegTotal * kRegKindBits <= 32,
              "the kinds of all registers fit in a word");

inline Uint32 regKind(Uint32 kinds, Uint32 reg) {
  return (kinds >> (reg * kRegKindBits)) & ((1U << kRegKindBits) - 1);
}

inline Uint32 setRegKind(Uint32 kinds, Uint32 reg, Uint32 kind) {
  const Uint32 shift = reg * kRegKindBits;
  return (kinds & ~(((1U << kRegKindBits) - 1) << shift)) | (kind << shift);
}

/* kOpLoadCol, kOpLoadConst, kOpSetRegNull and the aggregates name their
 * register in bits 16-19. */
inline bool aggOpUsesReg16(Uint32 op) {
  return op == kOpLoadCol || op == kOpLoadConst || op == kOpSetRegNull ||
         aggIsSlotOp(op) || op == kOpAvg;
}

/* The arithmetic instructions and kOpMov name theirs in bits 12-15 and
 * 8-11, as executeStandardOpcode decodes them. */
inline bool aggOpUsesRegs12And8(Uint32 op) {
  return (op >= kOpPlus && op <= kOpMod) ||
         (op >= kOpPlusBigint && op <= kOpDivIntBigint) || op == kOpMov;
}

/* The aggregate an instruction that updates a slot computes, one bit
 * each, for checkAggSlotKinds: the typed variants compute the aggregate
 * of their generic form. */
constexpr Uint8 kSlotFamilySum = 1;
constexpr Uint8 kSlotFamilyAvg = 2;
constexpr Uint8 kSlotFamilyMax = 4;
constexpr Uint8 kSlotFamilyMin = 8;
constexpr Uint8 kSlotFamilyCount = 16;

inline Uint8 aggSlotFamily(Uint32 op) {
  switch (op) {
    case kOpSum:
    case kOpSumBigint:
    case kOpSumDouble:
      return kSlotFamilySum;
    case kOpAvg:
      return kSlotFamilyAvg;
    case kOpMax:
    case kOpMaxBigint:
    case kOpMaxDouble:
      return kSlotFamilyMax;
    case kOpMin:
    case kOpMinBigint:
    case kOpMinDouble:
      return kSlotFamilyMin;
    case kOpCount:
      return kSlotFamilyCount;
    default:
      return 0;
  }
}

}  // namespace

void
PushdownInterpreter::OptimizeProgramBuffer(Uint32* prog, Uint32 prog_len,
                                           Uint32 start_pos) {
  Uint32 slot_types[MAX_AGG_N_RESULTS];
  for (Uint32 i = 0; i < MAX_AGG_N_RESULTS; i++) {
    slot_types[i] = kNoAggSlotWriter;
  }
  optimizeAggProgramWalk(prog, prog_len, start_pos, slot_types, false);
  optimizeAggProgramWalk(prog, prog_len, start_pos, slot_types, true);
}

/*
 * checkAggSlotKinds — see PushdownInterpreter.hpp.  The walk follows the
 * jumps of the program as OptimizeProgramBuffer does, but keeps every
 * jump to every word (jumps_to), so a CASE with many arms costs no
 * precision.  At runtime an aggregate, kOpMov or an arithmetic
 * instruction fails the row when it reads a register the row did not
 * load (m_loaded_regs), so only the loaded kinds of a register count.
 */
const char*
PushdownInterpreter::checkAggSlotKinds(const Uint32* prog, Uint32 prog_len,
                                       Uint32 start_pos) {
  if (prog_len > MAX_AGG_PROGRAM_WORD_SIZE) {
    return "longer than MAX_AGG_PROGRAM_WORD_SIZE words";
  }
  /* The register kinds of the rows that jump to each word, and of those
   * that may land on any later word (an embedded program whose skips
   * are unknown). */
  Uint32 jumps_to[MAX_AGG_PROGRAM_WORD_SIZE];
  memset(jumps_to, 0, prog_len * sizeof(Uint32));
  Uint32 jumps_anywhere = 0;
  /* kRegKindString and kRegKindOther of what the writers of each slot
   * put there. */
  Uint8 slot_kinds[MAX_AGG_N_RESULTS];
  memset(slot_kinds, 0, sizeof(slot_kinds));
  /* aggSlotFamily of the instructions that update each slot */
  Uint8 slot_families[MAX_AGG_N_RESULTS];
  memset(slot_families, 0, sizeof(slot_families));
  Uint32 skips[kProgramBitmapWords];

  /* A row starts with no register loaded. */
  Uint32 kinds = 0;
  for (Uint32 r = 0; r < kRegTotal; r++) {
    kinds = setRegKind(kinds, r, kRegKindNotLoaded);
  }
  bool falls_through = true;
  for (Uint32 pos = start_pos; pos < prog_len;) {
    const Uint32 word = prog[pos];
    const Uint32 words = aggInstructionWords(word);
    if (words > prog_len - pos) {
      return "operand words past the end";
    }
    kinds = (falls_through ? kinds : 0) | jumps_to[pos] | jumps_anywhere;
    falls_through = true;
    if (kinds == 0) {
      pos += words;  // No row reaches this instruction
      continue;
    }
    const Uint32 op = aggProgramOp(word);
    /* Register fields: bits 16-19 of the loads and aggregates, bits
     * 12-15 (written) and 8-11 of the two-register instructions.
     * checkProgram has checked them and the slot field; as they index
     * kinds and slot_kinds here, they are checked again. */
    const Uint32 reg = (word >> 16) & 0xF;
    const Uint32 reg_a = (word >> 12) & 0xF;
    const Uint32 reg_b = (word >> 8) & 0xF;
    const Uint32 slot = word & 0xFFFF;
    if ((aggOpUsesReg16(op) && reg >= kRegTotal) ||
        (aggOpUsesRegs12And8(op) &&
         (reg_a >= kRegTotal || reg_b >= kRegTotal))) {
      return "register out of range";
    }
    if ((aggIsSlotOp(op) || op == kOpAvg) && slot >= MAX_AGG_N_RESULTS) {
      return "aggregate slot out of range";
    }
    switch (op) {
      case kOpLoadCol: {
        const Uint32 type = aggLoadColType(word);
        const bool is_string = type == NDB_TYPE_CHAR ||
                               type == NDB_TYPE_VARCHAR ||
                               type == NDB_TYPE_LONGVARCHAR;
        kinds = setRegKind(kinds, reg,
                           is_string ? kRegKindString : kRegKindOther);
        break;
      }
      case kOpLoadConst:
        kinds = setRegKind(kinds, reg, kRegKindOther);
        break;
      case kOpSetRegNull:
        /* The register keeps its type: that of an earlier row when this
         * row did not load it, which may be any. */
        if ((regKind(kinds, reg) & kRegKindNotLoaded) != 0) {
          kinds = setRegKind(kinds, reg, kRegKindValue);
        }
        break;
      case kOpMov:
        kinds = setRegKind(kinds, reg_a,
                           regKind(kinds, reg_b) & kRegKindValue);
        break;
      case kOpPlus:
      case kOpMinus:
      case kOpMul:
      case kOpDiv:
      case kOpDivInt:
      case kOpMod:
      case kOpPlusBigint:
      case kOpPlusDouble:
      case kOpMinusBigint:
      case kOpMinusDouble:
      case kOpMulBigint:
      case kOpMulDouble:
      case kOpDivDouble:
      case kOpDivIntBigint:
        /* A string register holds the type and 0, its value is in
         * m_register_string_data.  The arithmetic took it for a number,
         * and the generic forms (RegPlusReg ...) assert that a register
         * that is not a DOUBLE is a BIGINT. */
        if (((regKind(kinds, reg_a) | regKind(kinds, reg_b)) &
             kRegKindString) != 0) {
          return "arithmetic on a register that may hold a string";
        }
        /* The result is a BIGINT or a DOUBLE (InterpreterCommonOp.hpp). */
        kinds = setRegKind(kinds, reg_a, kRegKindOther);
        break;
      case kOpMax:
      case kOpMin:
        /* executeStandardOpcode: minMaxString for a string register,
         * Max or Min for any other. */
        slot_kinds[slot] = static_cast<Uint8>(
            slot_kinds[slot] | (regKind(kinds, reg) & kRegKindValue));
        slot_families[slot] =
            static_cast<Uint8>(slot_families[slot] | aggSlotFamily(op));
        break;
      case kOpSum:
      case kOpSumBigint:
      case kOpSumDouble:
      case kOpMaxBigint:
      case kOpMaxDouble:
      case kOpMinBigint:
      case kOpMinDouble:
      case kOpAvg:
        /* Only the generic kOpMax / kOpMin take a string.  Sum stored
         * the string register in the slot and asserted on the next row
         * that the slot is a number, the typed aggregates read its 0. */
        if ((regKind(kinds, reg) & kRegKindString) != 0) {
          return "a numeric aggregate of a register that may hold a "
                 "string";
        }
        [[fallthrough]];
      case kOpCount:
        /* A number in the slot, whatever the register holds. */
        if ((regKind(kinds, reg) & kRegKindValue) != 0) {
          slot_kinds[slot] =
              static_cast<Uint8>(slot_kinds[slot] | kRegKindOther);
        }
        slot_families[slot] =
            static_cast<Uint8>(slot_families[slot] | aggSlotFamily(op));
        break;
      case kOpSkip: {
        const Uint32 target = pos + 1 + (word & 0xFFFF);
        if (target < prog_len) {
          jumps_to[target] |= kinds;
        }
        falls_through = false;
        break;
      }
      case kOpEmbeddedInterp: {
        /* As OptimizeProgramBuffer: a NULL-extended row whose condition
         * JoinAggInterpreter cannot evaluate continues right after the
         * embedded program, the others skip what it wrote. */
        const Uint32 emb_end = pos + words;
        if (emb_end < prog_len) {
          jumps_to[emb_end] |= kinds;
        }
        if (embeddedSkips(&prog[pos + 1], words - 1, skips,
                          prog_len - emb_end)) {
          for (Uint32 skip = 0; skip < prog_len - emb_end; skip++) {
            if (testProgramBit(skips, skip)) {
              jumps_to[emb_end + skip] |= kinds;
            }
          }
        } else {
          jumps_anywhere |= kinds;
        }
        falls_through = false;
        break;
      }
      default:
        break;
    }
    pos += words;
  }

  for (Uint32 slot = 0; slot < MAX_AGG_N_RESULTS; slot++) {
    if (slot_kinds[slot] == kRegKindValue) {
      return "a string MIN / MAX and another aggregate update one slot";
    }
    /* One aggregate per slot: the merges take the slot's one operation
     * (extractAggOps), and Count asserts that its slot holds an
     * unsigned BIGINT, which a SUM or a MAX of the slot need not leave
     * there.  The arms of a CASE repeat one aggregate
     * (NdbAggregator::RepeatAgg). */
    const Uint8 families = slot_families[slot];
    if ((families & (families - 1)) != 0) {
      return "two different aggregates update one slot";
    }
  }
  return nullptr;
}

PushdownType
PushdownInterpreterFactory::DetectType(const Uint32* prog, Uint32 prog_len) {
  require(prog != nullptr);
  require(prog_len >= 4);

  /* Word 0: magic (upper 16) | prog_len (lower 16) */
  Uint32 w0 = prog[0];
  require(((w0 & 0xFFFF0000) >> 16) == 0x0721);

  /* Word 3: bit 31 set => vector search */
  Uint32 w3 = prog[3];
  if (w3 & 0x80000000) {
    return PushdownType::VECTOR_SEARCH;
  }
  return PushdownType::AGGREGATION;
}

/*
 * Init and OptimizeProgram a new aggregation interpreter.  On failure
 * *out_of_memory tells why: Init fails without its buffer block when
 * query memory is short and with it, or with program_rejected() set,
 * when it rejects the program, and a
 * program of a newer version leaves Init successful but the interpreter
 * not inited, which OptimizeProgram rejects.  fail_init fails Init as
 * when query memory is short.  The caller destroys the interpreter on
 * failure; nothing was processed, so the destructor only frees what Init
 * allocated (see ~AggInterpreterBase, case (c)).
 */
static bool initAggInterpreter(AggInterpreter* agg, const Uint32* prog,
                               bool fail_init, bool* out_of_memory) {
  const bool init_ok = !fail_init && agg->Init(prog);
  if (init_ok && agg->OptimizeProgram()) {
    return true;
  }
  *out_of_memory =
      !init_ok && !agg->has_buf_block() && !agg->program_rejected();
  return false;
}

PushdownCreateResult
PushdownInterpreterFactory::Create(const Uint32* prog, Uint32 prog_len,
                                   Int64 table_id, Int64 frag_id,
                                   Uint32 thread_id, bool fail_init,
                                   bool* out_of_memory) {
  PushdownCreateResult result = {nullptr, nullptr};
  *out_of_memory = false;
  PushdownType type = DetectType(prog, prog_len);

  /* An aggregation interpreter keeps its buffers in m_buf_block and its
   * groups in chunk pages, so it needs only its own size.  A vector
   * search interpreter stores its program in the rest of a
   * MEM_CHUNK_SIZE page (VecSearchInterpreter::Init). */
  const size_t obj_size = (type == PushdownType::AGGREGATION)
                              ? sizeof(AggInterpreter)
                              : MEM_CHUNK_SIZE;
  void* obj_ptr = lc_ndbd_pool_malloc(obj_size, RG_QUERY_MEMORY,
                                      thread_id, false);
  if (obj_ptr == nullptr) {
    g_eventLogger->error("Alloc mem for pushdown interpreter failed");
    *out_of_memory = true;
    return result;
  }

  if (type == PushdownType::AGGREGATION) {
    AggInterpreter* agg = new(obj_ptr) AggInterpreter(prog_len, table_id,
                                                      frag_id, thread_id);
    if (!initAggInterpreter(agg, prog, fail_init, out_of_memory)) {
      PushdownInterpreter::Destruct(agg);
      return result;
    }
    result.agg = agg;
    /* Phase 8 Slice 3c: JIT-compile the per-row aggregation. The
     * GROUP BY gate lift (Phase 8 #5) compiles grouped programs too —
     * the compiled region starts at agg_prog_start_pos, past the
     * GROUP BY metadata, so grouping never appears in the blob (the
     * group prologue in ProcessRec resolves the row's accumulator
     * slots before dispatch). Acquire from the node-global agg reuse
     * cache (identical instruction streams share one blob — sound
     * even across different GROUP BY column sets); the handle is
     * released in ~AggInterpreterBase. */
    {
      const Uint32 *agg_prog = result.agg->agg_program();
      const Uint32 bc_off = result.agg->agg_prog_start_pos();
      if (bc_off < prog_len) {
        void *handle = nullptr;
        void *entry = dbtup_jit_compile_agg(agg_prog + bc_off,
                                            prog_len - bc_off, &handle,
                                            result.agg->prog_reusable(),
                                            /*n_visible_results=*/
                                            NDB_JIT_NO_AVG_SLOTS);
        if (entry != nullptr) {
          result.agg->setJitEntry(reinterpret_cast<JitEntry>(entry));
          result.agg->setJitCacheHandle(handle);
        }
      }
    }
  } else {
    VecSearchInterpreter* vs = new(obj_ptr) VecSearchInterpreter(
        prog_len, table_id, frag_id, thread_id);
    /* VecSearchInterpreter::Init fails only when it cannot allocate; the
     * destructor frees what it did allocate. */
    if (fail_init || !vs->Init(prog)) {
      *out_of_memory = true;
      PushdownInterpreter::Destruct(vs);
      return result;
    }
    result.vs = vs;
  }

  return result;
}

AggInterpreter*
PushdownInterpreterFactory::CreateAggForRead(const Uint32* prog,
                                             Uint32 prog_len,
                                             Int64 table_id, Int64 frag_id,
                                             Uint32 thread_id, bool fail_init,
                                             bool* out_of_memory) {
  *out_of_memory = false;
  if (prog == nullptr || prog_len < 8 ||
      DetectType(prog, prog_len) != PushdownType::AGGREGATION) {
    return nullptr;
  }
  void* obj_ptr = lc_ndbd_pool_malloc(sizeof(AggInterpreter),
                                      RG_QUERY_MEMORY, thread_id, false);
  if (obj_ptr == nullptr) {
    g_eventLogger->error("Alloc mem for aggregation on a key read failed");
    *out_of_memory = true;
    return nullptr;
  }
  AggInterpreter* agg = new(obj_ptr) AggInterpreter(prog_len, table_id,
                                                    frag_id, thread_id);
  if (!initAggInterpreter(agg, prog, fail_init, out_of_memory)) {
    PushdownInterpreter::Destruct(agg);
    return nullptr;
  }
  if (agg->prog_reusable()) {
    const Uint32 *agg_prog = agg->agg_program();
    const Uint32 bc_off = agg->agg_prog_start_pos();
    if (bc_off < prog_len) {
      void *handle = nullptr;
      void *entry = dbtup_jit_compile_agg(agg_prog + bc_off,
                                          prog_len - bc_off, &handle,
                                          /*reusable=*/true,
                                          /*n_visible_results=*/
                                          NDB_JIT_NO_AVG_SLOTS);
      if (entry != nullptr) {
        agg->setJitEntry(reinterpret_cast<JitEntry>(entry));
        agg->setJitCacheHandle(handle);
      }
    }
  }
  return agg;
}
