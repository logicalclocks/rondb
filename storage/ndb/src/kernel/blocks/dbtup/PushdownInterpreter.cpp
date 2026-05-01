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

void PushdownInterpreter::Destruct(PushdownInterpreter* ptr) {
  if (ptr == nullptr) {
    return;
  }
  ptr->~PushdownInterpreter();
  lc_ndbd_pool_free(ptr);
}

void
PushdownInterpreter::OptimizeProgramBuffer(Uint32* prog, Uint32 prog_len,
                                           Uint32 start_pos) {
  using DataType = Uint32;
  DataType reg_types[kRegTotal];
  for (Uint32 i = 0; i < kRegTotal; i++) {
    reg_types[i] = NDB_TYPE_UNDEFINED;
  }

  Uint32 exec_pos = start_pos;
  while (exec_pos < prog_len) {
    Uint32 value = prog[exec_pos];
    Uint8 op = (value & 0xFC000000) >> 26;
    Uint32 reg_index, reg_index2;
    DataType type;
    Uint8 new_op = op;

    switch (op) {
      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        if (type == NDB_TYPE_FLOAT || type == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (type == NDB_TYPE_DECIMAL ||
                   type == NDB_TYPE_DECIMALUNSIGNED) {
          reg_types[reg_index] = NDB_TYPE_UNDEFINED;
          exec_pos++;
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
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
                   reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
                   reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpPlusBigint;
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
                   reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
                   reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpMinusBigint;
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_DOUBLE ||
                   reg_types[reg_index2] == NDB_TYPE_DOUBLE) {
          reg_types[reg_index] = NDB_TYPE_DOUBLE;
        } else if (reg_types[reg_index] == NDB_TYPE_BIGINT &&
                   reg_types[reg_index2] == NDB_TYPE_BIGINT) {
          new_op = kOpMulBigint;
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
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
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) new_op = kOpSumDouble;
        else if (reg_types[reg_index] == NDB_TYPE_BIGINT) new_op = kOpSumBigint;
        if (new_op != op)
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) new_op = kOpMaxDouble;
        else if (reg_types[reg_index] == NDB_TYPE_BIGINT) new_op = kOpMaxBigint;
        if (new_op != op)
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        if (reg_types[reg_index] == NDB_TYPE_DOUBLE) new_op = kOpMinDouble;
        else if (reg_types[reg_index] == NDB_TYPE_BIGINT) new_op = kOpMinBigint;
        if (new_op != op)
          prog[exec_pos] = (new_op << 26) | (value & 0x03FFFFFF);
        break;

      case kOpCount:
        break;

      case kOpEmbeddedInterp:
      {
        Uint32 emb_len = value & 0xFFFF;
        exec_pos += emb_len;
        break;
      }

      case kOpSkip:
        break;

      default:
        break;
    }
    exec_pos++;
  }
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

  void* page_ptr = lc_ndbd_pool_malloc(MEM_CHUNK_SIZE, RG_QUERY_MEMORY,
                                       thread_id, false);
  if (page_ptr == nullptr) {
    g_eventLogger->error("Alloc mem for pushdown interpreter failed");
    return result;
  }

  if (type == PushdownType::AGGREGATION) {
    result.agg = new(page_ptr) AggInterpreter(prog_len, table_id, frag_id,
                                              thread_id);
    require(result.agg->Init(prog));
    require(result.agg->OptimizeProgram());
  } else {
    result.vs = new(page_ptr) VecSearchInterpreter(prog_len, table_id, frag_id,
                                                   thread_id);
    require(result.vs->Init(prog));
  }

  return result;
}
