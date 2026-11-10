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

/* True if a branch of the embedded program emb lands on emb[target]. */
bool embeddedBranchTo(const Uint32* emb, Uint32 target) {
  Uint32 pc = 0;
  while (pc < target) {
    Interpreter::InstructionPreProcessing processing;
    const Uint32* next = Interpreter::getInstructionPreProcessingInfo(
        const_cast<Uint32*>(&emb[pc]), processing);
    if (next == nullptr) {
      return true;
    }
    /* Forward offset from the branch word, as validateEmbeddedProgram. */
    if (processing == Interpreter::LABEL_ADDRESS_REPLACEMENT &&
        pc + ((emb[pc] >> 16) & 0x7FFF) == target) {
      return true;
    }
    pc = Uint32(next - emb);
  }
  return false;
}

/*
 * Add a join point for each skip the embedded program emb[0..emb_len)
 * writes to interpreter output 0; emb_end is the word after it.  Every
 * emitter (CASE and GREATEST / LEAST of RonSQL, CASE of
 * ha_ndbcluster_push_agg) writes the skip with a LOAD_CONST16 directly
 * followed by WRITE_INTERPRETER_OUTPUT.  Return false, the skips being
 * unknown, when output 0 is written in another way or a branch lands on
 * the write, so that the register may hold another value.
 * AGG_EMBEDDED_INTERP_STOP_PROGRAM ends the row: it is no jump.
 */
bool addEmbeddedJoinPoints(AggJoinPoints& joins, const Uint32* emb,
                           Uint32 emb_len, Uint32 emb_end,
                           const Uint32* reg_types) {
  Uint32 pc = 0;
  Uint32 prev_pc = ~0U;
  while (pc < emb_len) {
    const Uint32 instr = emb[pc];
    Interpreter::InstructionPreProcessing processing;
    const Uint32* next = Interpreter::getInstructionPreProcessingInfo(
        const_cast<Uint32*>(&emb[pc]), processing);
    if (next == nullptr) {
      return false;
    }
    if (Interpreter::getOpCode(instr) ==
            Interpreter::WRITE_INTERPRETER_OUTPUT &&
        (instr >> 16) == 0) {
      if (prev_pc == ~0U ||
          Interpreter::getOpCode(emb[prev_pc]) != Interpreter::LOAD_CONST16 ||
          Interpreter::getReg1(emb[prev_pc]) != Interpreter::getReg1(instr) ||
          embeddedBranchTo(emb, pc)) {
        return false;
      }
      const Uint32 skip = emb[prev_pc] >> 16;
      if (skip != AGG_EMBEDDED_INTERP_STOP_PROGRAM) {
        joins.add(emb_end + skip, emb_end + skip, reg_types);
      }
    }
    prev_pc = pc;
    pc = Uint32(next - emb);
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
        if (emb_end > prog_len ||
            !addEmbeddedJoinPoints(joins, &prog[exec_pos + 1], emb_len,
                                   emb_end, reg_types)) {
          joins.add(emb_end, ~0U, reg_types);
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

PushdownCreateResult
PushdownInterpreterFactory::Create(const Uint32* prog, Uint32 prog_len,
                                   Int64 table_id, Int64 frag_id,
                                   Uint32 thread_id) {
  PushdownCreateResult result = {nullptr, nullptr};
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
    return result;
  }

  if (type == PushdownType::AGGREGATION) {
    result.agg = new(obj_ptr) AggInterpreter(prog_len, table_id, frag_id,
                                             thread_id);
    require(result.agg->Init(prog));
    require(result.agg->OptimizeProgram());
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
    result.vs = new(obj_ptr) VecSearchInterpreter(prog_len, table_id, frag_id,
                                                  thread_id);
    require(result.vs->Init(prog));
  }

  return result;
}

AggInterpreter*
PushdownInterpreterFactory::CreateAggForRead(const Uint32* prog,
                                             Uint32 prog_len,
                                             Int64 table_id, Int64 frag_id,
                                             Uint32 thread_id,
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
  if (!agg->Init(prog) || !agg->OptimizeProgram()) {
    *out_of_memory = !agg->has_buf_block();
    /* Nothing was processed: the destructor only frees what Init
     * allocated (see ~AggInterpreterBase, case (c)). */
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
