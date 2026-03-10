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
  } else {
    result.vs = new(page_ptr) VecSearchInterpreter(prog_len, table_id, frag_id,
                                                   thread_id);
    require(result.vs->Init(prog));
  }

  return result;
}
