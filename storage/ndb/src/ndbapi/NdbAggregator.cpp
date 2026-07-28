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

#include "NdbAggregator.hpp"
#include "AttributeHeader.hpp"
#include "../../src/ndbapi/NdbDictionaryImpl.hpp"
#include <simsimd/simsimd.h>
#include <NdbSqlUtil.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_NDBAGGREGATOR 1
//#define DEBUG_JOIN_AGG_API 1
#endif

#ifdef DEBUG_JOIN_AGG_API
#define DEB_JOIN_AGG_API(...) do {      \
  fprintf(stderr, __VA_ARGS__);         \
  fflush(stderr);                       \
} while (0)
#else
#define DEB_JOIN_AGG_API(...) do { } while (0)
#endif

#ifdef DEBUG_NDBAGGREGATOR
#define DEB_TRACE() do { \
  printf("NdbAggregator.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
#define DEB(...) do { \
  printf(__VA_ARGS__); \
  fflush(stdout); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#define DEB(...) do { } while (0)
#endif

#define PROGRAM_HEADER_SIZE 8
#define RESULT_HEADER_SIZE 3
#define RESULT_ITEM_HEADER_SIZE 1

// Encode a column type into a kOpLoadCol instruction word's type field.
// The type is 6 bits: the low 5 bits sit at instruction bits 21-25 (the
// historical position), and the most-significant 6th bit sits at bit 20
// (previously unused).  Every type that existed before DATETIME2 (32) /
// TIMESTAMP2 (33) is <= 31, so its bit 20 is 0 and the word is byte-identical
// to the old 5-bit encoding — backward compatible.  Mirrors the kernel's
// AggInterpreterBase::decodeLoadColType.
static inline Uint32 encodeLoadColType(Uint32 type) {
  return ((type & 0x1F) << 21) | (((type >> 5) & 0x1) << 20);
}

bool
GBHashEntryCmp::operator()(const GBHashEntry &n1,
                           const GBHashEntry &n2) const {
  if (ctx == nullptr || ctx->n_cols == 0 || ctx->all_binary_cmp) {
    /* Binary comparison: safe when all group-by columns are
       non-charset-aware, since binary identity equals semantic identity. */
    Uint32 len = n1.len < n2.len ? n1.len : n2.len;
    int ret = memcmp(n1.ptr, n2.ptr, len);
    if (ret == 0) {
      return n1.len < n2.len;
    }
    return ret < 0;
  }

  const char *p1 = n1.ptr;
  const char *p2 = n2.ptr;
  [[maybe_unused]] const char *end1 = n1.ptr + n1.len;
  [[maybe_unused]] const char *end2 = n2.ptr + n2.len;

  for (Uint32 i = 0; i < ctx->n_cols; i++) {
    assert(p1 + sizeof(Uint32) <= end1);
    assert(p2 + sizeof(Uint32) <= end2);
    const AttributeHeader ah1(*(const Uint32 *)p1);
    const AttributeHeader ah2(*(const Uint32 *)p2);

    bool null1 = ah1.isNULL();
    bool null2 = ah2.isNULL();
    if (null1 && null2) {
      p1 += sizeof(Uint32);
      p2 += sizeof(Uint32);
      continue;
    }
    if (null1) return true;   /* NULL < non-NULL */
    if (null2) return false;

    const char *data1 = p1 + sizeof(Uint32);
    const char *data2 = p2 + sizeof(Uint32);
    Uint32 byteSize1 = ah1.getByteSize();
    Uint32 byteSize2 = ah2.getByteSize();

    int ret = NdbSqlUtil::getType(ctx->col_meta[i].typeId).m_cmp(
                ctx->col_meta[i].cs, data1, byteSize1, data2, byteSize2);
    if (ret != 0) {
      return ret < 0;
    }

    /* Advance past header + word-aligned data */
    p1 += sizeof(Uint32) + ah1.getDataSize() * sizeof(Uint32);
    p2 += sizeof(Uint32) + ah2.getDataSize() * sizeof(Uint32);
  }
  return false;  /* All columns equal */
}

NdbAggregator::NdbAggregator(const NdbDictionary::Table* table) :
  table_impl_(nullptr), n_gb_cols_(0), n_agg_results_(0),
  agg_results_(nullptr), gb_map_(nullptr),
  finalized_(false), finished_(false),
  curr_prog_pos_(PROGRAM_HEADER_SIZE),
  instructions_length_(PROGRAM_HEADER_SIZE),
  result_record_fetched_(false),
  result_size_est_(RESULT_HEADER_SIZE * sizeof(Uint32) +
               RESULT_ITEM_HEADER_SIZE * sizeof(Uint32)),
  disk_columns_(false),
  uses_wide_type_(false),
  vec_top_n_(0), vec_result_(nullptr),
  userAttrs_(nullptr), n_userAttrs_(0),
  results_prepared_(false), results_left_(0),
  type_(kAggregation) {
    if (table != nullptr) {
      table_impl_ = & NdbTableImpl::getImpl(*table);
    }
    memset(agg_ops_, kOpUnknown, MAX_AGGREGATION_OP_SIZE * 4);
    vec_result_final_.clear();
    memset(gb_columns_, 0, sizeof(gb_columns_));
    memset(reg_columns_, 0, sizeof(reg_columns_));
    memset(agg_columns_, 0, sizeof(agg_columns_));
    memset(reg_types_, NDB_TYPE_UNDEFINED, sizeof(reg_types_));
}

NdbAggregator::~NdbAggregator() {
  // Phase I.6 (F.2-K.5d): release any string MIN/MAX val_ptr buffers
  // owned by this aggregator before tearing down the slot arrays.
  if (agg_results_ != nullptr) {
    freeStringSlots(agg_results_, n_agg_results_);
  }
  delete[] agg_results_;
  if (gb_map_) {
    for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
      AggResItem* slots = reinterpret_cast<AggResItem*>(iter->second.ptr);
      freeStringSlots(slots, n_agg_results_);
      delete[] iter->first.ptr;
    }
    delete gb_map_;
  }
  if (vec_result_) {
    while (!vec_result_->empty()) {
      delete vec_result_->top();
      vec_result_->pop();
    }
    delete vec_result_;
  }
  for (Uint32 i = 0; i < vec_result_final_.size(); i++) {
    delete vec_result_final_[i];
  }
}

void NdbAggregator::initForResults(const Uint32 *programBuffer,
                                   Uint32 programLen,
                                   const NdbDictionary::Column *const *gbColumns,
                                   Uint32 nGbColumns,
                                   const NdbDictionary::Column *const *aggColumns,
                                   Uint32 nAggColumns) {
  assert(programLen >= PROGRAM_HEADER_SIZE);
  // Word 1: (n_gb_cols << 16) | n_agg_results
  n_gb_cols_ = programBuffer[1] >> 16;
  n_agg_results_ = programBuffer[1] & 0xFFFF;

  // Allocate gb_map if GROUP BY is used
  if (n_gb_cols_ > 0 && gb_map_ == nullptr) {
    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>();
  }

  // Allocate agg_results for non-GROUP-BY case
  if (n_gb_cols_ == 0 && agg_results_ == nullptr && n_agg_results_ > 0) {
    agg_results_ = new AggResItem[n_agg_results_];
    for (Uint32 i = 0; i < n_agg_results_; i++) {
      agg_results_[i].type = NDB_TYPE_UNDEFINED;
      agg_results_[i].value.val_int64 = 0;
      agg_results_[i].is_unsigned = false;
      agg_results_[i].is_null = true;
    }
  }

  // Store GROUP BY column definitions for FetchGroupbyColumn().
  memset(gb_columns_, 0, sizeof(gb_columns_));
  if (gbColumns != nullptr) {
    for (Uint32 i = 0; i < nGbColumns && i < MAX_AGG_N_GROUPBY_COLS; i++) {
      gb_columns_[i] = gbColumns[i];
    }
  }
  memset(reg_columns_, 0, sizeof(reg_columns_));
  memset(agg_columns_, 0, sizeof(agg_columns_));
  memset(reg_types_, NDB_TYPE_UNDEFINED, sizeof(reg_types_));
  if (aggColumns != nullptr) {
    for (Uint32 i = 0; i < nAggColumns && i < MAX_AGG_N_RESULTS; i++) {
      agg_columns_[i] = aggColumns[i];
    }
  }

  // Parse instructions to extract agg_ops for COUNT null→0 fixup.
  // Instructions start at PROGRAM_HEADER_SIZE + n_gb_cols_.
  Uint32 instrStart = PROGRAM_HEADER_SIZE + n_gb_cols_;
  for (Uint32 i = instrStart; i < programLen; i++) {
    Uint32 op = (programBuffer[i] >> 26) & 0x3F;
    if (op == kOpSum || op == kOpMax || op == kOpMin || op == kOpCount ||
        op == kOpSumBigint || op == kOpSumDouble ||
        op == kOpMaxBigint || op == kOpMaxDouble ||
        op == kOpMinBigint || op == kOpMinDouble) {
      Uint32 agg_id = programBuffer[i] & 0xFFFF;
      if (agg_id < MAX_AGG_N_RESULTS) {
        agg_ops_[agg_id] = (InterpreterOp)op;
      }
    }
  }

  finalized_ = true;
}

void NdbAggregator::resolveStringSlots(AggResItem* slots, Uint32 n_slots,
                                        const char* appended_region) {
  const char* p = appended_region;
  for (Uint32 i = 0; i < n_slots; i++) {
    Uint32 t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) && !slots[i].is_null) {
      const Uint32 byte_size = *reinterpret_cast<const Uint32*>(p);
      p += sizeof(Uint32);
      const Uint32 prefix = (t == NDB_TYPE_CHAR) ? 0
                          : (t == NDB_TYPE_VARCHAR) ? 1 : 2;
      // Mirror the kernel layout so data_str() can decode the same
      // way: [Uint16 payload_len][Uint16 capacity][prefix+payload],
      // total alloc rounded to a multiple of 16 (min 16).
      Uint32 alloc_size = (4 + byte_size + 15) & ~15U;
      if (alloc_size < 16) alloc_size = 16;
      char* dst = new char[alloc_size];
      Uint16* hdr = reinterpret_cast<Uint16*>(dst);
      hdr[0] = static_cast<Uint16>(byte_size - prefix);  // payload_len
      hdr[1] = static_cast<Uint16>(alloc_size - 4);      // capacity
      memcpy(dst + 4, p, byte_size);
      slots[i].value.val_ptr = dst;
      // Advance past the Uint32-padded payload.
      p += (byte_size + 3) & ~3U;
    }
  }
}

void NdbAggregator::freeStringSlots(AggResItem* slots, Uint32 n_slots) {
  for (Uint32 i = 0; i < n_slots; i++) {
    Uint32 t = slots[i].type;
    if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
         t == NDB_TYPE_LONGVARCHAR) &&
        slots[i].value.val_ptr != nullptr) {
      delete[] static_cast<char*>(slots[i].value.val_ptr);
    }
  }
}

bool NdbAggregator::isStringType(Uint32 type) const {
  return type == NDB_TYPE_CHAR ||
         type == NDB_TYPE_VARCHAR ||
         type == NDB_TYPE_LONGVARCHAR;
}

bool NdbAggregator::isTemporalType(Uint32 type) const {
  return type == NDB_TYPE_DATE ||
         type == NDB_TYPE_YEAR ||
         type == NDB_TYPE_DATETIME2 ||
         type == NDB_TYPE_TIME2 ||
         type == NDB_TYPE_TIMESTAMP2;
}

void NdbAggregator::clearStringSlot(AggResItem *slot) const {
  if (slot != nullptr && isStringType(slot->type) &&
      slot->value.val_ptr != nullptr) {
    delete[] static_cast<char*>(slot->value.val_ptr);
    slot->value.val_ptr = nullptr;
  }
}

void NdbAggregator::assignStringSlot(AggResItem *dst,
                                     const AggResItem *src) const {
  clearStringSlot(dst);
  *dst = *src;
}

int NdbAggregator::compareStringSlots(const AggResItem *lhs,
                                      const AggResItem *rhs,
                                      Uint32 agg_id) const {
  const NdbDictionary::Column *col =
      (agg_id < MAX_AGG_N_RESULTS) ? agg_columns_[agg_id] : nullptr;
  assert(col != nullptr);
  const char *lhs_buf = static_cast<const char*>(lhs->value.val_ptr);
  const char *rhs_buf = static_cast<const char*>(rhs->value.val_ptr);
  const Uint16 lhs_payload_len =
      *reinterpret_cast<const Uint16*>(lhs_buf);
  const Uint16 rhs_payload_len =
      *reinterpret_cast<const Uint16*>(rhs_buf);
  const Uint32 prefix = (lhs->type == NDB_TYPE_CHAR) ? 0 :
                        (lhs->type == NDB_TYPE_VARCHAR) ? 1 : 2;
  const Uint32 lhs_len = prefix + lhs_payload_len;
  const Uint32 rhs_len = prefix + rhs_payload_len;
  const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(lhs->type);
  return (*sqlType.m_cmp)(col->getCharset(),
                          lhs_buf + 4, lhs_len,
                          rhs_buf + 4, rhs_len);
}

void NdbAggregator::mergeStringSlot(AggResItem *dst,
                                    const AggResItem *src,
                                    Uint32 agg_id) {
  if (src->type == NDB_TYPE_UNDEFINED || src->is_null) {
    return;
  }
  if (dst->type == NDB_TYPE_UNDEFINED || dst->is_null) {
    assignStringSlot(dst, src);
    return;
  }
  const int cmp = compareStringSlots(src, dst, agg_id);
  const Uint32 op = agg_ops_[agg_id];
  const bool isMax =
      op == kOpMax || op == kOpMaxBigint || op == kOpMaxDouble;
  const bool replace = isMax ? (cmp > 0) : (cmp < 0);
  if (replace) {
    assignStringSlot(dst, src);
  }
}

Int32 NdbAggregator::ProcessRes(char* buf) {
#ifdef DEBUG_NDBAGGREGATOR
  {
    int dynamic_dispatch = simsimd_uses_dynamic_dispatch();
    simsimd_metric_kind_t kind = simsimd_metric_l2sq_k;
    simsimd_datatype_t datatype = simsimd_datatype_f32_k;

    simsimd_kernel_punned_t result = 0;
    simsimd_capability_t c = simsimd_cap_serial_k;
    simsimd_capability_t supported = simsimd_capabilities();
    simsimd_capability_t allowed = simsimd_cap_any_k;
    simsimd_find_kernel_punned(kind, datatype, supported, allowed, &result, &c);

    fprintf(stderr, "use_dynamic_dispatch: %d, capabilities: %d, choose: [%p, %d]\n",
            dynamic_dispatch, supported, result, c);
  }
#endif
  DEB_TRACE();
  if (buf != nullptr) {
    DEB_TRACE();
  }
  DEB_TRACE();
  // PA related
  // Aggregation result
  assert(buf != nullptr);
  Uint32 parse_pos = 0;
  const Uint32* data_buf = (const Uint32*)buf;

  // Phase I.6 (F.2-K.5d): peek the marker word.  AGG_RESULT means
  // raw AggResItem arrays (today's format).  AGG_CHAR_RESULT means
  // each per-group AggResItem array is followed by an appended
  // string-payload region; resolve those into local val_ptr buffers
  // before the merge code reads slot values.
  AttributeHeader marker_ah(data_buf[parse_pos++]);
  const Uint32 marker_id = marker_ah.getAttributeId();
  assert(marker_id == AttributeHeader::AGG_RESULT ||
         marker_id == AttributeHeader::AGG_CHAR_RESULT);
  const bool wire_has_strings =
      (marker_id == AttributeHeader::AGG_CHAR_RESULT);

#ifdef DEBUG_JOIN_AGG_API
  DEB_JOIN_AGG_API("[AGG_API] ProcessRes begin: marker=0x%x "
                   "wire_has_strings=%u first_words=0x%x 0x%x 0x%x 0x%x\n",
                   marker_id, wire_has_strings ? 1 : 0,
                   data_buf[0], data_buf[1], data_buf[2], data_buf[3]);
#endif

  Uint32 n_gb_cols = data_buf[parse_pos] >> 16;
  Uint32 n_agg_results = data_buf[parse_pos++] & 0xFFFF;
  assert(n_gb_cols == n_gb_cols_);
  assert(n_agg_results == n_agg_results_);
  Uint32 n_res_items = data_buf[parse_pos++];

  AggResItem* agg_res_ptr = nullptr;
  DEB_TRACE();
  if (n_gb_cols) {
    DEB_TRACE();
    char* agg_rec = nullptr;
    // const AttributeHeader* header = nullptr;
    for (Uint32 rowNo = 0; rowNo < n_res_items; rowNo++) {
      DEB_TRACE();
      bool need_merge = false;
      Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
      Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;

      GBHashEntry entry{const_cast<char*>(
          reinterpret_cast<const char*>(&data_buf[parse_pos])),
                  gb_cols_len};
      auto iter = gb_map_->find(entry);
#ifdef DEBUG_JOIN_AGG_API
      const Uint32 key_words = (gb_cols_len + 3) >> 2;
      const Uint32 key0 = key_words > 0 ? data_buf[parse_pos] : 0;
      const Uint32 key1 = key_words > 1 ? data_buf[parse_pos + 1] : 0;
      const AggResItem* wire_res = reinterpret_cast<const AggResItem*>(
          &data_buf[parse_pos + (gb_cols_len >> 2)]);
      const AggResItem& agg0 = wire_res[0];

      DEB_JOIN_AGG_API("[AGG_API] ProcessRes row: rowNo=%u parse_pos=%u "
                       "gb_cols_len=%u agg_res_len=%u key_words=%u "
                       "key[0]=0x%x key[1]=0x%x action=%s "
                       "agg0_type=%u agg0_unsigned=%u agg0_null=%u "
                       "agg0_i64=%lld agg0_u64=%llu gb_map_size=%zu\n",
                       rowNo, parse_pos, gb_cols_len, agg_res_len,
                       key_words, key0, key1,
                       iter != gb_map_->end() ? "merge" : "insert",
                       agg0.type, agg0.is_unsigned, agg0.is_null,
                       (long long)agg0.value.val_int64,
                       (unsigned long long)agg0.value.val_uint64,
                       gb_map_->size());
#endif
      if (iter != gb_map_->end()) {
        // header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
        agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
        // fprintf(stderr, "[PA DEBUG] Found GBHashEntry, id: %u, byte_size: %u, "
        //     "data_size: %u, is_null: %u\n",
        //     header->getAttributeId(), header->getByteSize(),
        //     header->getDataSize(), header->isNULL());
        need_merge = true;
#ifdef DEBUG_JOIN_AGG_API
        DEB_JOIN_AGG_API("[AGG_API] ProcessRes merging: rowNo=%u "
                         "key[0]=0x%x key[1]=0x%x agg_ptr=%p\n",
                         rowNo, key0, key1, static_cast<void*>(agg_res_ptr));
#endif
      } else {
        // For AGG_CHAR_RESULT, agg_res_len includes both the
        // AggResItem array and the appended string-payload region.
        assert(wire_has_strings ||
               n_agg_results * sizeof(AggResItem) == agg_res_len);
        agg_rec = new char[gb_cols_len + agg_res_len];
        memcpy(agg_rec, reinterpret_cast<const char*>(&data_buf[parse_pos]),
            gb_cols_len + agg_res_len);
        const Uint32 agg_array_len = n_agg_results * sizeof(AggResItem);
        GBHashEntry new_entry{agg_rec, gb_cols_len};
        GBHashEntry new_aggs{agg_rec + gb_cols_len, agg_array_len};

        // Phase I.6 (F.2-K.5d): replace each string slot's wire
        // val_ptr (zero) with a freshly-allocated local buffer
        // mirroring the kernel layout.  The appended region within
        // agg_rec is read once and then unused — the live winner
        // bytes live in the per-slot allocation.
        if (wire_has_strings) {
          AggResItem* slots = reinterpret_cast<AggResItem*>(new_aggs.ptr);
          const char* appended = new_aggs.ptr + agg_array_len;
          resolveStringSlots(slots, n_agg_results, appended);
        }

        // RONDB-831: COUNT() over zero rows should result in 0, not NULL.
        // Therefore, replace NULLs/UNDEFINED with 0 for all COUNT results.
        for (Uint32 i = 0; i < n_agg_results_; i++) {
          if (agg_ops_[i] == kOpCount) {
            AggResItem* item = reinterpret_cast<AggResItem*>(new_aggs.ptr) + i;
            if (item->is_null || item->type == NDB_TYPE_UNDEFINED) {
              item->type = NDB_TYPE_BIGINT;
              item->is_unsigned = 1;
              item->is_null = false;
              item->value.val_uint64 = 0;
            }
          }
        }

        gb_map_->insert(std::pair<GBHashEntry, GBHashEntry>(
              new_entry, new_aggs));
        agg_res_ptr = reinterpret_cast<AggResItem*>(new_aggs.ptr);
#ifdef DEBUG_JOIN_AGG_API
        DEB_JOIN_AGG_API("[AGG_API] ProcessRes inserted: rowNo=%u "
                         "key[0]=0x%x key[1]=0x%x gb_map_size=%zu "
                         "agg_ptr=%p\n",
                         rowNo, key0, key1, gb_map_->size(),
                         static_cast<void*>(agg_res_ptr));
#endif
      }
      DEB_TRACE();

      // For AGG_CHAR_RESULT, agg_res_len includes the appended
      // string-payload region; the array is still n_agg_results × 16
      // bytes at the start.
      assert(wire_has_strings ||
             agg_res_len == n_agg_results * sizeof(AggResItem));
      const AggResItem* res = reinterpret_cast<const AggResItem*>(
                           &data_buf[parse_pos + (gb_cols_len >> 2)]);
      AggResItem local_res[MAX_AGG_N_RESULTS];
      if (need_merge && wire_has_strings) {
        memcpy(local_res, res, n_agg_results * sizeof(AggResItem));
        const Uint32 agg_array_len = n_agg_results * sizeof(AggResItem);
        const char* appended =
            reinterpret_cast<const char*>(res) + agg_array_len;
        resolveStringSlots(local_res, n_agg_results, appended);
        res = local_res;
      }
      if (need_merge) {
        DEB_TRACE();
        for (Uint32 i = 0; i < n_agg_results; i++) {
          DEB_TRACE();
          // Handle NDB_TYPE_UNDEFINED and NULL cases before merging.
          // Mirrors kernel mergeAccumulators() logic.
          if (res[i].type == NDB_TYPE_UNDEFINED) {
            continue;
          }
          if (isStringType(res[i].type)) {
            mergeStringSlot(&agg_res_ptr[i], &res[i], i);
            continue;
          }
          if (agg_res_ptr[i].type == NDB_TYPE_UNDEFINED) {
            agg_res_ptr[i] = res[i];
            continue;
          }
          if (res[i].is_null) {
            DEB_TRACE();
            continue;
          }
          if (agg_res_ptr[i].is_null) {
            DEB_TRACE();
            agg_res_ptr[i] = res[i];
            continue;
          }
          // Both sides are non-null with real types — check consistency.
          assert(((res[i].type == NDB_TYPE_BIGINT &&
                  res[i].is_unsigned == agg_res_ptr[i].is_unsigned) ||
                  res[i].type == NDB_TYPE_DOUBLE) &&
                  res[i].type == agg_res_ptr[i].type);
          {
            DEB_TRACE();
            agg_res_ptr[i].type = res[i].type;
            agg_res_ptr[i].is_unsigned = res[i].is_unsigned;
            switch (agg_ops_[i]) {
              case kOpSum:
                DEB_TRACE();
                if (res[i].type == NDB_TYPE_BIGINT) {
                  if (res[i].is_unsigned) {
                    agg_res_ptr[i].value.val_uint64 += res[i].value.val_uint64;
                  } else {
                    agg_res_ptr[i].value.val_int64 += res[i].value.val_int64;
                  }
                } else {
                  assert(res[i].type == NDB_TYPE_DOUBLE);
                  agg_res_ptr[i].value.val_double += res[i].value.val_double;
                }
                break;
              case kOpCount:
                DEB_TRACE();
                assert(res[i].type == NDB_TYPE_BIGINT);
                assert(res[i].is_unsigned == 1);
                agg_res_ptr[i].value.val_int64 += res[i].value.val_int64;
                break;
              case kOpMax:
                DEB_TRACE();
                if (res[i].type == NDB_TYPE_BIGINT) {
                  if (res[i].is_unsigned) {
                    agg_res_ptr[i].value.val_uint64 =
                      agg_res_ptr[i].value.val_uint64 >= res[i].value.val_uint64 ?
                      agg_res_ptr[i].value.val_uint64 : res[i].value.val_uint64;
                  } else {
                    agg_res_ptr[i].value.val_int64 =
                      agg_res_ptr[i].value.val_int64 >= res[i].value.val_int64 ?
                      agg_res_ptr[i].value.val_int64 : res[i].value.val_int64;
                  }
                } else {
                  assert(res[i].type == NDB_TYPE_DOUBLE);
                  agg_res_ptr[i].value.val_double =
                    agg_res_ptr[i].value.val_double >= res[i].value.val_double ?
                    agg_res_ptr[i].value.val_double : res[i].value.val_double;
                }
                break;
              case kOpMin:
                DEB_TRACE();
                if (res[i].type == NDB_TYPE_BIGINT) {
                  if (res[i].is_unsigned) {
                    agg_res_ptr[i].value.val_uint64 =
                      agg_res_ptr[i].value.val_uint64 <= res[i].value.val_uint64 ?
                      agg_res_ptr[i].value.val_uint64 : res[i].value.val_uint64;
                  } else {
                    agg_res_ptr[i].value.val_int64 =
                      agg_res_ptr[i].value.val_int64 <= res[i].value.val_int64 ?
                      agg_res_ptr[i].value.val_int64 : res[i].value.val_int64;
                  }
                } else {
                  assert(res[i].type == NDB_TYPE_DOUBLE);
                  agg_res_ptr[i].value.val_double =
                    agg_res_ptr[i].value.val_double <= res[i].value.val_double ?
                    agg_res_ptr[i].value.val_double : res[i].value.val_double;
                }
                break;
              default:
                DEB_TRACE();
                assert(0);
                break;
            }
          }
        }
        if (wire_has_strings) {
          for (Uint32 i = 0; i < n_agg_results; i++) {
            Uint32 t = local_res[i].type;
            if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
                 t == NDB_TYPE_LONGVARCHAR) &&
                local_res[i].value.val_ptr != nullptr &&
                local_res[i].value.val_ptr !=
                    agg_res_ptr[i].value.val_ptr) {
              delete[] static_cast<char*>(local_res[i].value.val_ptr);
            }
          }
        }
      }
#if defined(PA_CHECK) && !defined(NDEBUG)
      {
        DEB_TRACE();
        /*
         * PA related
         * Validation
         */
        Uint32 pos = parse_pos;
        for (Uint32 i = 0; i < n_gb_cols_; i++) {
          DEB("[i: %d, n_gb_cols_: %u, pos: %u\n", i, n_gb_cols_, pos);
          AttributeHeader ah(data_buf[pos]);
          DEB("[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
              "res_len: %u, value: %p]\n",
              ah.getAttributeId(), ah.getByteSize(),
              ah.getDataSize(), gb_cols_len, agg_res_len,
              agg_res_ptr);
          assert(ah.getDataPtr() != &data_buf[pos]);
          static_assert(sizeof(AttributeHeader) % sizeof(Int32) == 0,
              "AttributeHeader size must be divisible by Int32 size");
          pos += sizeof(AttributeHeader) / sizeof(Int32) + ah.getDataSize();
          DEB_TRACE();
          if (i == gb_cols_len - 1) {
            DEB_TRACE();
            assert(pos == gb_cols_len);
          }
          DEB_TRACE();
        }
      }
#endif // PA_CHECK && !NDEBUG
      parse_pos += ((gb_cols_len + agg_res_len) >> 2);
#ifdef DEBUG_JOIN_AGG_API
      DEB_JOIN_AGG_API("[AGG_API] ProcessRes row done: rowNo=%u "
                       "next_parse_pos=%u key[0]=0x%x key[1]=0x%x\n",
                       rowNo, parse_pos, key0, key1);
#endif
    }
  } else {
    DEB_TRACE();
    Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
    Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
    assert(gb_cols_len == 0);
    // Get rid of warning in release-binary
    (void)gb_cols_len;
    // For AGG_CHAR_RESULT, agg_res_len includes the appended
    // string-payload region after the AggResItem array.
    assert(wire_has_strings ||
           agg_res_len == n_agg_results_ * sizeof(AggResItem));
    assert(agg_results_ != nullptr);
    AggResItem* agg_res_ptr = agg_results_;
    const AggResItem* res = reinterpret_cast<const AggResItem*>(
                         &data_buf[parse_pos/* + (gb_cols_len >> 2)*/]);

    // Phase I.6 (F.2-K.5d): for AGG_CHAR_RESULT, deep-copy res to a
    // local buffer and fix up each string slot's val_ptr (zero on
    // the wire) to point to a freshly-allocated local buffer.  The
    // first-contribution merge step (`agg_res_ptr[i] = res[i]` when
    // agg_res_ptr is_null/UNDEFINED) then transfers ownership of the
    // val_ptr to agg_results_; any val_ptrs that were not transferred
    // are released after the loop.  Multi-source string MIN/MAX
    // merge (when both sides are non-null) is K.5d-2.
    AggResItem local_res[MAX_AGG_N_RESULTS];
    if (wire_has_strings) {
      memcpy(local_res, res, n_agg_results_ * sizeof(AggResItem));
      const Uint32 array_bytes = n_agg_results_ * sizeof(AggResItem);
      const char* appended =
          reinterpret_cast<const char*>(res) + array_bytes;
      resolveStringSlots(local_res, n_agg_results_, appended);
      res = local_res;
    }

    for (Uint32 i = 0; i < n_agg_results; i++) {
      DEB_TRACE();
      // Phase I.6: allow CHAR / VARCHAR / Longvarchar through the
      // per-slot type check and merge them with charset-aware compare.
      const bool is_string_type =
          (res[i].type == NDB_TYPE_CHAR ||
           res[i].type == NDB_TYPE_VARCHAR ||
           res[i].type == NDB_TYPE_LONGVARCHAR);
      assert((((res[i].type == NDB_TYPE_BIGINT &&
              (res[i].is_unsigned == agg_res_ptr[i].is_unsigned ||
               agg_res_ptr[i].is_null)) ||
              res[i].type == NDB_TYPE_DOUBLE ||
              is_string_type) &&
              res[i].type == agg_res_ptr[i].type) ||
              agg_res_ptr[i].type == NDB_TYPE_UNDEFINED ||
              (res[i].type == NDB_TYPE_UNDEFINED &&
               n_gb_cols == 0));
      if (is_string_type) {
        mergeStringSlot(&agg_res_ptr[i], &res[i], i);
      } else if (res[i].is_null) {
        DEB_TRACE();
      } else if (agg_res_ptr[i].is_null) {
        DEB_TRACE();
        agg_res_ptr[i] = res[i];
      } else {
        DEB_TRACE();
        agg_res_ptr[i].type = res[i].type;
        agg_res_ptr[i].is_unsigned = res[i].is_unsigned;
        switch (agg_ops_[i]) {
          case kOpSum:
            DEB_TRACE();
            if (res[i].type == NDB_TYPE_BIGINT) {
              if (res[i].is_unsigned) {
                agg_res_ptr[i].value.val_uint64 += res[i].value.val_uint64;
              } else {
                agg_res_ptr[i].value.val_int64 += res[i].value.val_int64;
              }
            } else {
              assert(res[i].type == NDB_TYPE_DOUBLE);
              agg_res_ptr[i].value.val_double += res[i].value.val_double;
            }
            break;
          case kOpCount:
            DEB_TRACE();
            assert(res[i].type == NDB_TYPE_BIGINT);
            assert(res[i].is_unsigned == 1);
            agg_res_ptr[i].value.val_int64 += res[i].value.val_int64;
            break;
          case kOpMax:
            DEB_TRACE();
            if (res[i].type == NDB_TYPE_BIGINT) {
              if (res[i].is_unsigned) {
                agg_res_ptr[i].value.val_uint64 =
                  agg_res_ptr[i].value.val_uint64 >= res[i].value.val_uint64 ?
                  agg_res_ptr[i].value.val_uint64 : res[i].value.val_uint64;
              } else {
                agg_res_ptr[i].value.val_int64 =
                  agg_res_ptr[i].value.val_int64 >= res[i].value.val_int64 ?
                  agg_res_ptr[i].value.val_int64 : res[i].value.val_int64;
              }
            } else {
              assert(res[i].type == NDB_TYPE_DOUBLE);
              agg_res_ptr[i].value.val_double =
                agg_res_ptr[i].value.val_double >= res[i].value.val_double ?
                agg_res_ptr[i].value.val_double : res[i].value.val_double;
            }
            break;
          case kOpMin:
            DEB_TRACE();
            if (res[i].type == NDB_TYPE_BIGINT) {
              if (res[i].is_unsigned) {
                agg_res_ptr[i].value.val_uint64 =
                  agg_res_ptr[i].value.val_uint64 <= res[i].value.val_uint64 ?
                  agg_res_ptr[i].value.val_uint64 : res[i].value.val_uint64;
              } else {
                agg_res_ptr[i].value.val_int64 =
                  agg_res_ptr[i].value.val_int64 <= res[i].value.val_int64 ?
                  agg_res_ptr[i].value.val_int64 : res[i].value.val_int64;
              }
            } else {
              assert(res[i].type == NDB_TYPE_DOUBLE);
              agg_res_ptr[i].value.val_double =
                agg_res_ptr[i].value.val_double <= res[i].value.val_double ?
                agg_res_ptr[i].value.val_double : res[i].value.val_double;
            }
            break;
          default:
            DEB_TRACE();
            assert(0);
            break;
        }
      }
    }
    // Phase I.6 (F.2-K.5d): release any string val_ptr buffers in
    // local_res that were not transferred into agg_results_.  A
    // first-contribution slot has its val_ptr handed off via
    // `agg_res_ptr[i] = res[i]`; for those, the pointers compare
    // equal and we keep them.
    if (wire_has_strings) {
      for (Uint32 i = 0; i < n_agg_results; i++) {
        Uint32 t = local_res[i].type;
        if ((t == NDB_TYPE_CHAR || t == NDB_TYPE_VARCHAR ||
             t == NDB_TYPE_LONGVARCHAR) &&
            local_res[i].value.val_ptr != nullptr &&
            local_res[i].value.val_ptr !=
                agg_res_ptr[i].value.val_ptr) {
          delete[] static_cast<char*>(local_res[i].value.val_ptr);
        }
      }
    }
    DEB_TRACE();
    parse_pos += ((/*gb_cols_len + */agg_res_len) >> 2);
  }
  DEB_TRACE();
#ifdef DEBUG_JOIN_AGG_API
  DEB_JOIN_AGG_API("[AGG_API] ProcessRes end: final_parse_pos=%u "
                   "n_res_items=%u n_gb_cols=%u n_agg_results=%u\n",
                   parse_pos, n_res_items, n_gb_cols, n_agg_results);
#endif
  return parse_pos;
}

bool NdbAggregator::TypeSupported(NdbDictionary::Column::Type type) {
  switch(type) {
    case NdbDictionary::Column::Tinyint:
    case NdbDictionary::Column::Tinyunsigned:
    case NdbDictionary::Column::Smallint:
    case NdbDictionary::Column::Smallunsigned:
    case NdbDictionary::Column::Mediumint:
    case NdbDictionary::Column::Mediumunsigned:
    case NdbDictionary::Column::Int:
    case NdbDictionary::Column::Unsigned:
    case NdbDictionary::Column::Bigint:
    case NdbDictionary::Column::Bigunsigned:
    case NdbDictionary::Column::Float:
    case NdbDictionary::Column::Double:
    case NdbDictionary::Column::Decimal:
    case NdbDictionary::Column::Decimalunsigned:
    // Phase I.6 (F.2 + F.3): kernel-side MIN/MAX over CHAR / VARCHAR /
    // Longvarchar is wired via the AggResItem.value.val_ptr per-(group,
    // slot) state and the AGG_CHAR_RESULT wire format.  LoadColumn /
    // Max / Min on these types are accepted; LoadColumn-then-Sum is
    // still a kernel-side error (no Sum-over-string semantics).
    case NdbDictionary::Column::Char:
    case NdbDictionary::Column::Varchar:
    case NdbDictionary::Column::Longvarchar:
    // D17 + temporal extension: MIN/MAX over DATE / YEAR / DATETIME2 /
    // TIME2.  The kernel reads each column's native packed value as an
    // unsigned integer and returns a Bigunsigned result (see
    // AggInterpreterBase); RonSQL decodes it for display.  SUM/AVG over
    // these is rejected separately (see Sum()).  TIMESTAMP2's epoch ordering
    // is absolute, so MIN/MAX is exact; RonSQL applies the session timezone
    // (UTC) at display.
    case NdbDictionary::Column::Date:
    case NdbDictionary::Column::Year:
    case NdbDictionary::Column::Datetime2:
    case NdbDictionary::Column::Time2:
    case NdbDictionary::Column::Timestamp2:
      return true;
    default:
      return false;
  }
}

bool NdbAggregator::LoadColumn(const char* name, Uint32 reg_id) {
  if (name == nullptr) {
    SetError(kErrInvalidColumnName);
    return false;
  }
  const NdbDictionary::Column* col = table_impl_->getColumn(name);
  if (col == nullptr) {
    SetError(kErrInvalidColumnName);
    return false;
  }
  NdbDictionary::Column::Type type = col->getType();
  if (!TypeSupported(type)) {
    SetError(kErrUnSupportedColumn);
    return false;
  }
  if (reg_id >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }
  if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

  Int32 col_id = col->getAttrId();
  assert((col_id & 0xFFFFFF00) == 0);
  buffer_[curr_prog_pos_++] =
    (kOpLoadCol) << 26 |
    encodeLoadColType(type) |
    (reg_id & 0x0F) << 16 |
    col_id;
  reg_columns_[reg_id] = col;
  reg_types_[reg_id] = type;
  // Wide (6-bit) column type → kOpLoadCol sets bit 20; the scan-send path
  // gates emission on ndbd_support_agg_wide_type.
  if (((Uint32)type) > 0x1F) uses_wide_type_ = true;

  /*
   * For decimal, use 1 more byte to take precision/scale
   * info.
   */
  if (type == NdbDictionary::Column::Decimal ||
      type == NdbDictionary::Column::Decimalunsigned) {
    assert((col->getPrecision() & 0xFFFFFF00) == 0);
    assert((col->getScale() & 0xFFFFFF00) == 0);
    Int32 decimal_info = col->getPrecision() << 16 |
                           col->getScale();
    int4store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              decimal_info);
    curr_prog_pos_++;
  }

  return true;
}

bool NdbAggregator::LoadColumn(Int32 col_id, Uint32 reg_id) {
  const NdbDictionary::Column* col = table_impl_->getColumn(col_id);
  if (col == nullptr) {
    SetError(kErrInvalidColumnId);
    return false;
  }
  NdbDictionary::Column::Type type = col->getType();
  if (!TypeSupported(type)) {
    SetError(kErrUnSupportedColumn);
    return false;
  }
  if (reg_id >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }
  if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

  assert((col_id & 0xFFFFFF00) == 0);
  buffer_[curr_prog_pos_++] =
    (kOpLoadCol) << 26 |
    encodeLoadColType(type) |
    (reg_id & 0x0F) << 16 |
    col_id;
  reg_columns_[reg_id] = col;
  reg_types_[reg_id] = type;
  // Wide (6-bit) column type → kOpLoadCol sets bit 20; the scan-send path
  // gates emission on ndbd_support_agg_wide_type.
  if (((Uint32)type) > 0x1F) uses_wide_type_ = true;
  /*
   * For decimal, use 1 more byte to take precision/scale
   * info.
   */
  if (type == NdbDictionary::Column::Decimal ||
      type == NdbDictionary::Column::Decimalunsigned) {
    assert((col->getPrecision() & 0xFFFFFF00) == 0);
    assert((col->getScale() & 0xFFFFFF00) == 0);
    Int32 decimal_info = col->getPrecision() << 16 |
                           col->getScale();
    int4store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              decimal_info);
    curr_prog_pos_++;
  }

  return true;
}

bool NdbAggregator::LoadLinkedColumn(Uint32 position, Uint32 reg_id,
                                     const NdbDictionary::Column *col) {
  if (col == nullptr) {
    SetError(kErrInvalidColumnId);
    return false;
  }
  NdbDictionary::Column::Type type = col->getType();
  if (!TypeSupported(type)) {
    SetError(kErrUnSupportedColumn);
    return false;
  }
  if (reg_id >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }

  Uint32 col_id = AGG_LINKED_COL_FLAG | position;
  buffer_[curr_prog_pos_++] =
    (kOpLoadCol) << 26 |
    encodeLoadColType(type) |
    (reg_id & 0x0F) << 16 |
    col_id;
  reg_columns_[reg_id] = col;
  reg_types_[reg_id] = type;
  // Wide (6-bit) column type → kOpLoadCol sets bit 20; the scan-send path
  // gates emission on ndbd_support_agg_wide_type.
  if (((Uint32)type) > 0x1F) uses_wide_type_ = true;

  if (type == NdbDictionary::Column::Decimal ||
      type == NdbDictionary::Column::Decimalunsigned) {
    Int32 decimal_info = col->getPrecision() << 16 |
                           col->getScale();
    int4store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              decimal_info);
    curr_prog_pos_++;
  }

  return true;
}

bool NdbAggregator::LoadUint64(Uint64 value, Uint32 reg_id) {
  buffer_[curr_prog_pos_++] =
    (kOpLoadConst) << 26 |
    (NDB_TYPE_BIGUNSIGNED & 0x1F) << 21 |
    (reg_id & 0x0F) << 16 |
    0;
  int8store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              value);
  curr_prog_pos_ += 2;
  reg_columns_[reg_id] = nullptr;
  reg_types_[reg_id] = NDB_TYPE_BIGUNSIGNED;
  return true;
}

bool NdbAggregator::LoadInt64(Int64 value, Uint32 reg_id) {
  buffer_[curr_prog_pos_++] =
    (kOpLoadConst) << 26 |
    (NDB_TYPE_BIGINT & 0x1F) << 21 |
    (reg_id & 0x0F) << 16 |
    0;
  int8store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              value);
  curr_prog_pos_ += 2;
  reg_columns_[reg_id] = nullptr;
  reg_types_[reg_id] = NDB_TYPE_BIGINT;
  return true;
}

bool NdbAggregator::LoadDouble(double value, Uint32 reg_id) {
  buffer_[curr_prog_pos_++] =
    (kOpLoadConst) << 26 |
    (NDB_TYPE_DOUBLE & 0x1F) << 21 |
    (reg_id & 0x0F) << 16 |
    0;
  float8store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              value);
  curr_prog_pos_ += 2;
  reg_columns_[reg_id] = nullptr;
  reg_types_[reg_id] = NDB_TYPE_DOUBLE;
  return true;
}

bool NdbAggregator::CheckRegs(Uint32 reg_1, Uint32 reg_2) {
  if (reg_1 >= kRegTotal || reg_2 >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }

  return true;
}

bool NdbAggregator::Mov(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMov) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;
  reg_columns_[reg_1] = reg_columns_[reg_2];
  reg_types_[reg_1] = reg_types_[reg_2];

  return true;
}

bool NdbAggregator::Add(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpPlus) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Minus(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMinus) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Mul(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMul) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Div(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpDiv) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::DivInt(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpDivInt) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Mod(Uint32 reg_1, Uint32 reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMod) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::CheckAggAndReg(Uint32 agg_id, Uint32 reg_id) {
  if (agg_id >= MAX_AGGREGATION_OP_SIZE) {
    SetError(kErrInvalidAggNo);
  }

  if (agg_ops_[agg_id] != kOpUnknown) {
    SetError(kErrAggNoUsed);
    return false;
  }
  if (reg_id >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }
  return true;
}

bool NdbAggregator::Sum(Uint32 agg_id, Uint32 reg_id) {
  if (!CheckAggAndReg(agg_id, reg_id)) {
    return false;
  }
  if (isStringType(reg_types_[reg_id])) {
    SetError(kErrUnsupportedStringOperation);
    return false;
  }
  // D17 + temporal: SUM/AVG over DATE/YEAR/DATETIME/TIME is meaningless —
  // only MIN/MAX/COUNT.
  if (isTemporalType(reg_types_[reg_id])) {
    SetError(kErrUnsupportedTemporalOperation);
    return false;
  }

  buffer_[curr_prog_pos_++] =
    (kOpSum) << 26 |
    (reg_id & 0x0F) << 16 |
    agg_id;

  agg_ops_[agg_id] = kOpSum;
  agg_columns_[agg_id] = reg_columns_[reg_id];
  n_agg_results_++;

  return true;
}

bool NdbAggregator::Max(Uint32 agg_id, Uint32 reg_id) {
  if (!CheckAggAndReg(agg_id, reg_id)) {
    return false;
  }

  buffer_[curr_prog_pos_++] =
    (kOpMax) << 26 |
    (reg_id & 0x0F) << 16 |
    agg_id;

  agg_ops_[agg_id] = kOpMax;
  agg_columns_[agg_id] = reg_columns_[reg_id];
  n_agg_results_++;

  return true;
}

bool NdbAggregator::Min(Uint32 agg_id, Uint32 reg_id) {
  if (!CheckAggAndReg(agg_id, reg_id)) {
    return false;
  }

  buffer_[curr_prog_pos_++] =
    (kOpMin) << 26 |
    (reg_id & 0x0F) << 16 |
    agg_id;

  agg_ops_[agg_id] = kOpMin;
  agg_columns_[agg_id] = reg_columns_[reg_id];
  n_agg_results_++;

  return true;
}

bool NdbAggregator::Count(Uint32 agg_id, Uint32 reg_id) {
  if (!CheckAggAndReg(agg_id, reg_id)) {
    return false;
  }

  buffer_[curr_prog_pos_++] =
    (kOpCount) << 26 |
    (reg_id & 0x0F) << 16 |
    agg_id;

  agg_ops_[agg_id] = kOpCount;
  agg_columns_[agg_id] = reg_columns_[reg_id];
  n_agg_results_++;

  return true;
}

bool NdbAggregator::GroupBy(const char* name) {
  if (name == nullptr) {
    SetError(kErrInvalidColumnName);
    return false;
  }
  const NdbDictionary::Column* col = table_impl_->getColumn(name);
  if (col == nullptr) {
    SetError(kErrInvalidColumnName);
    return false;
  }
  NdbDictionary::Column::Type type = col->getType();
  if (type == NdbDictionary::Column::Blob ||
      type == NdbDictionary::Column::Text) {
    SetError(kErrUnSupportedColumn);
    return false;
  }
  Int32 col_id = col->getAttrId();
  buffer_[curr_prog_pos_++] =
      (col_id << 16) | (col->getType() & AGG_GB_COL_TYPE_MASK);

  result_size_est_ += (sizeof(AttributeHeader) + ((col->getSizeInBytes() + 3) & (~3)));

  gb_col_ids_[n_gb_cols_] = col_id;
  gb_columns_[n_gb_cols_] = col;
  n_gb_cols_++;

  if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

  return true;
}

bool NdbAggregator::GroupBy(Int32 col_id) {
  if (col_id & AGG_LINKED_COL_FLAG) {
    SetError(kErrInvalidColumnId);
    return false;
  }
  const Int32 raw_col_id = col_id & ~AGG_LINKED_COL_FLAG;

  const NdbDictionary::Column* col = table_impl_->getColumn(raw_col_id);
  if (col == nullptr) {
    SetError(kErrInvalidColumnId);
    return false;
  }
  if (col != nullptr) {
    NdbDictionary::Column::Type type = col->getType();
    if (type == NdbDictionary::Column::Blob ||
        type == NdbDictionary::Column::Text) {
      SetError(kErrUnSupportedColumn);
      return false;
    }
  }

  buffer_[curr_prog_pos_++] = raw_col_id << 16;

  result_size_est_ +=
      (sizeof(AttributeHeader) + ((col->getSizeInBytes() + 3) & (~3)));
  if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

  gb_columns_[n_gb_cols_] = col;
  n_gb_cols_++;

  return true;
}

bool NdbAggregator::GroupByLinked(Uint32 position,
                                  const NdbDictionary::Column *parentCol) {
  if (parentCol == nullptr) {
    SetError(kErrInvalidColumnId);
    return false;
  }
  Uint32 col_id = position | AGG_LINKED_COL_FLAG;
  buffer_[curr_prog_pos_++] =
      (col_id << 16) | (parentCol->getType() & AGG_GB_COL_TYPE_MASK);

  result_size_est_ += (sizeof(AttributeHeader) +
                        ((parentCol->getSizeInBytes() + 3) & (~3)));
  if (parentCol->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

  gb_columns_[n_gb_cols_] = parentCol;
  n_gb_cols_++;
  return true;
}

bool NdbAggregator::EmbeddedInterp(Uint32 embedded_length) {
  buffer_[curr_prog_pos_++] =
      (kOpEmbeddedInterp << 26) | (embedded_length & 0xFFFF);
  return true;
}

bool NdbAggregator::EmitEmbeddedWord(Uint32 word) {
  buffer_[curr_prog_pos_++] = word;
  return true;
}

bool NdbAggregator::Skip(Uint32 skip_count) {
  buffer_[curr_prog_pos_++] = (kOpSkip << 26) | (skip_count & 0xFFFF);
  return true;
}

bool NdbAggregator::SetRegNull(Uint32 reg_id) {
  if (reg_id >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }
  buffer_[curr_prog_pos_++] =
      (kOpSetRegNull << 26) | ((reg_id & 0x0F) << 16);
  return true;
}

bool NdbAggregator::RepeatAgg(Uint32 agg_id, Uint32 reg_id) {
  if (agg_id >= MAX_AGGREGATION_OP_SIZE) {
    SetError(kErrInvalidAggNo);
    return false;
  }
  if (agg_ops_[agg_id] == kOpUnknown) {
    SetError(kErrAggNoUsed);
    return false;
  }
  if (reg_id >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }
  buffer_[curr_prog_pos_++] =
      (agg_ops_[agg_id]) << 26 | (reg_id & 0x0F) << 16 | agg_id;
  return true;
}

bool NdbAggregator::Finalize() {
  if (curr_prog_pos_ == PROGRAM_HEADER_SIZE) {
    SetError(kErrEmptyProgram);
    return false;
  }
  if (finalized_) {
    SetError(kErrAlreadyFinalized);
    return false;
  }
  instructions_length_ = curr_prog_pos_;
  if (instructions_length_ >= MAX_AGG_PROGRAM_WORD_SIZE) {
    SetError(kErrTooBigProgram);
    return false;
  }

  buffer_[0] = (0x0721) << 16 | curr_prog_pos_;
  buffer_[1] = n_gb_cols_ << 16 | n_agg_results_;
  buffer_[2] = PUSHDOWN_AGGREGATION_VERSION;

  // Initialize the next 5 reserved Uint32 elements to 0
  buffer_[3] = 0;
  buffer_[4] = 0;
  buffer_[5] = 0;
  buffer_[6] = 0;
  buffer_[7] = 0;

  if (n_gb_cols_) {
    if (n_gb_cols_ >= MAX_AGG_N_GROUPBY_COLS) {
      SetError(kErrTooManyGroupbyCols);
      return false;
    }
    gb_cmp_ctx_.n_cols = n_gb_cols_;
    bool all_binary = true;
    for (Uint32 i = 0; i < n_gb_cols_; i++) {
      const NdbDictionary::Column* col = gb_columns_[i];
      if (col == nullptr) {
        // Linked column without metadata — skip charset check,
        // kernel handles collation for linked columns.
        continue;
      }
      gb_cmp_ctx_.col_meta[i].typeId = (Uint32)col->getType();
      gb_cmp_ctx_.col_meta[i].cs = col->getCharset();
      if (col->getCharset() != nullptr) {
        all_binary = false;
      }
    }
    gb_cmp_ctx_.all_binary_cmp = all_binary;
    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>(
                      GBHashEntryCmp(&gb_cmp_ctx_));
  }
  if (n_agg_results_ == 0) {
    SetError(kErrEmptyAggResult);
    return false;
  } else if (n_agg_results_ >= MAX_AGG_N_RESULTS) {
    SetError(kErrTooManyAggResult);
    return false;
  } else {
    agg_results_ = new AggResItem[n_agg_results_];
    Uint32 i = 0;
    while (i < n_agg_results_) {
      if (agg_ops_[i] == kOpCount) {
        agg_results_[i].type = NDB_TYPE_BIGINT;
        agg_results_[i].is_unsigned = 1;
        agg_results_[i].is_null = false;
        agg_results_[i].value.val_uint64 = 0;
      } else {
        agg_results_[i].type = NDB_TYPE_UNDEFINED;
        agg_results_[i].is_unsigned = false;
        agg_results_[i].is_null = true;
        agg_results_[i].value.val_int64 = 0;
      }
      i++;
    }
  }

  result_size_est_ += sizeof(AggResItem) * n_agg_results_;

  if (result_size_est_ >= MAX_AGG_RESULT_BATCH_BYTES - 128) {
    SetError(kErrTooBigResult);
    /*
     * PA related
     * No need to release memory here.
     * Destruction will do it.
     * if (gb_map_) {
     *   delete gb_map_;
     * }
     * if (agg_results_) {
     *   delete agg_results_;
     * }
     */
    return false;
  }
  finalized_ = true;
  return true;
}

void NdbAggregator::PrepareResults() {
  if (n_gb_cols_) {
    iter_ = gb_map_->begin();
  } else if (agg_results_ != nullptr) {
    // RONDB-831 (scalar analog): COUNT() over zero rows must read 0, not
    // NULL.  The GROUP BY path applies this fixup at group-insert time
    // (see the "RONDB-831" block in iterate()); the scalar (no-GROUP-BY)
    // result record is simply the pre-initialised agg_results_ array,
    // whose every slot starts is_null=true.  When the kernel sends no
    // scalar group at all — e.g. a main scalar aggregation reading an
    // EMPTY CTE_SCAN that materialised to zero rows (D16) — that NULL
    // state survives to FetchResultRecord.  SUM / MIN / MAX correctly
    // surface NULL on empty input, but COUNT must surface 0.
    for (Uint32 i = 0; i < n_agg_results_; i++) {
      if (agg_ops_[i] == kOpCount &&
          (agg_results_[i].is_null ||
           agg_results_[i].type == NDB_TYPE_UNDEFINED)) {
        agg_results_[i].type = NDB_TYPE_BIGINT;
        agg_results_[i].is_unsigned = 1;
        agg_results_[i].is_null = false;
        agg_results_[i].value.val_uint64 = 0;
      }
    }
  }
  finished_ = true;
}

NdbAggregator::ResultRecord NdbAggregator::FetchResultRecord() {
  assert(finished_);
  if (!finished_) {
    return ResultRecord(nullptr, {nullptr, 0}, {nullptr, 0}, true);
  }

  if (n_gb_cols_) {
    if (iter_ != gb_map_->end()) {
      NdbAggregator::ResultRecord rec(this, iter_->first, iter_->second, false);
      iter_++;
      return rec;
    }
  } else {
    if (!result_record_fetched_) {
      result_record_fetched_ = true;
      return ResultRecord(this, {nullptr, 0},
          {reinterpret_cast<char*>(agg_results_),
           static_cast<Uint32>(n_agg_results_ * sizeof(AggResItem))},
                          false);
    }
  }
  return ResultRecord(nullptr, {nullptr, 0}, {nullptr, 0}, true);
}

NdbAggregator::Column NdbAggregator::ResultRecord::FetchGroupbyColumn() {
  if (aggregator_->n_gb_cols() == 0) {
    return Column(0, NdbDictionary::Column::Undefined,
                  0, true, nullptr, true);
  }
  if (curr_group_pos_ == group_records_.len) {
    return Column(0, NdbDictionary::Column::Undefined,
                  0, true, nullptr, true);
  }
  assert(curr_group_pos_ < group_records_.len);
  AttributeHeader header(
      *reinterpret_cast<Uint32*>(group_records_.ptr + curr_group_pos_));
  curr_group_pos_ += sizeof(AttributeHeader);

  Uint32 id = header.getAttributeId();
  Uint32 byte_size = header.getByteSize();

  /* Determine column type.  gb_columns_[] has the authoritative column
     definition for each GROUP BY column — for local columns it points
     into the leaf table's dictionary, for linked columns it points into
     the ancestor table's dictionary (set via GroupByLinked / initForResults).
     Fall back to leaf table lookup only when gb_columns_ is not set. */
  Uint32 gb_idx = curr_gb_col_index_++;
  const NdbDictionary::Column *gb_col =
      (gb_idx < MAX_AGG_N_GROUPBY_COLS)
          ? aggregator_->gb_columns_[gb_idx] : nullptr;
  NdbDictionary::Column::Type type;
  if (gb_col != nullptr) {
    type = gb_col->getType();
  } else {
    const NdbDictionary::Column *col =
        aggregator_->table_impl()->getColumn(id);
    type = (col != nullptr) ? col->getType()
                            : NdbDictionary::Column::Undefined;
  }

  bool is_null = header.isNULL();
  Uint32 word_size = header.getDataSize() * sizeof(Int32);
  char* ptr = is_null ? nullptr : group_records_.ptr + curr_group_pos_;
  if (is_null) {
    assert(byte_size == 0 && ptr == nullptr);
  }

  Column column(id, type, byte_size, is_null, ptr, false);
  curr_group_pos_ += word_size;
  return column;
}

NdbAggregator::Result NdbAggregator::ResultRecord::FetchAggregationResult() {
  if (curr_result_pos_ == result_records_.len) {
    return Result(nullptr, true);
  }
  assert(curr_result_pos_ < result_records_.len);
  Result result(
      reinterpret_cast<AggResItem*>(result_records_.ptr + curr_result_pos_),
      false);
  curr_result_pos_ += sizeof(AggResItem);

  return result;
}

#define sint3korr(A)  ((Int32) ((((Uint8) (A)[2]) & 128) ? \
                                  (((Uint32) 255L << 24) | \
                                  (((Uint32) (Uint8) (A)[2]) << 16) |\
                                  (((Uint32) (Uint8) (A)[1]) << 8) | \
                                   ((Uint32) (Uint8) (A)[0])) : \
                                 (((Uint32) (Uint8) (A)[2]) << 16) |\
                                 (((Uint32) (Uint8) (A)[1]) << 8) | \
                                  ((Uint32) (Uint8) (A)[0])))

#define uint3korr(A)  (Uint32) (((Uint32) ((Uint8) (A)[0])) +\
                                  (((Uint32) ((Uint8) (A)[1])) << 8) +\
                                  (((Uint32) ((Uint8) (A)[2])) << 16))

Int32 NdbAggregator::Column::data_medium() {
	return sint3korr(ptr_);
}
Uint32 NdbAggregator::Column::data_umedium() {
	return uint3korr(ptr_);
}
bool NdbAggregator::VectorSearch(const char* name,
                                 const float* vec, Uint32 dims,
                                 Uint32 top_n) {
  buffer_[1] = n_gb_cols_ << 16 | n_agg_results_;
  buffer_[2] = PUSHDOWN_AGGREGATION_VERSION;

  buffer_[3] = 0x80000000;

  Uint32 type = 0;
  Uint32 metric = 0;
  buffer_[4] = ((type & 0xFF) << 24) | ((metric & 0xFF) << 16) | (dims & 0xFFFF);

  if (name == nullptr) {
    SetError(kErrInvalidColumnName);
    return false;
  }
  const NdbDictionary::Column* col = table_impl_->getColumn(name);
  if (col == nullptr) {
    SetError(kErrInvalidColumnName);
    return false;
  }
  NdbDictionary::Column::Type col_type = col->getType();
  if (col_type != NDB_TYPE_LONGVARBINARY) {
    SetError(kErrUnSupportedColumn);
    return false;
  }

  // if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
  //   disk_columns_ = true;
  // }

  Int32 col_id = col->getAttrId();
  assert((col_id & 0xFFFF0000) == 0);
  Uint32 vec_size_in_bytes = dims * sizeof(float);
  buffer_[5] = ((col_id & 0xFFFF) << 16) | (top_n & 0xFFFF);
  buffer_[6] = vec_size_in_bytes;
  curr_prog_pos_ = 7;

  if (curr_prog_pos_ + dims >= MAX_VEC_SEARCH_PROGRAM_WORD_SIZE) {
    SetError(kErrTooBigProgram);
    return false;
  }

  memcpy(&buffer_[curr_prog_pos_], vec, dims * sizeof(float));
  curr_prog_pos_ += dims;

  // 0x0721: magic number identifying this as a pushdown aggregation program
  buffer_[0] = (0x0721) << 16 | curr_prog_pos_;

  instructions_length_ = curr_prog_pos_;
  vec_top_n_ = top_n;
  if (vec_result_) {
    while (!vec_result_->empty()) {
      VectorSearchResult* ptr = vec_result_->top();
      vec_result_->pop();
      delete ptr;
    }
    delete vec_result_;
    vec_result_ = nullptr;
  }
  vec_result_ = new std::priority_queue<VectorSearchResult*,
    std::vector<VectorSearchResult*>,
    ByDistance>;
  type_ = kVectorSearch;
  finalized_ = true;
  return true;
}

bool NdbAggregator::VecProcessRes(NdbRecAttr** userAttrs, Uint32 n_userAttrs,
                                  NdbRecAttr* vecDistanceAttr) {
  // fprintf(stderr, "  Receive a result from 1 fragment, "
  //         "pk: %d, distance: %lf, n_userAttrs: %u\n",
  //     (userAttrs[0])->int32_value(), vecDistanceAttr->double_value(),
  //     n_userAttrs);

  VectorSearchResult* result = new VectorSearchResult(
                                   vecDistanceAttr->double_value(),
                                   n_userAttrs, userAttrs);
  vec_result_->push(result);
  if (vec_result_->size() > vec_top_n_) {
    delete vec_result_->top();
    vec_result_->pop();
  }
  return true;
}

bool NdbAggregator::VecPrepareResults(NdbRecAttr** userAttrs, Uint32 n_userAttrs) {
  while (!vec_result_->empty()) {
    VectorSearchResult* result = vec_result_->top();
    vec_result_final_.push_back(result);
    vec_result_->pop();
  }
  userAttrs_ = userAttrs;
  n_userAttrs_ = n_userAttrs;
  results_prepared_ = true;
  results_left_ = vec_result_final_.size();
  return true;
}

bool NdbAggregator::VecFetchNextResult() {
  if (results_prepared_ && results_left_ > 0) {
    VectorSearchResult* result = vec_result_final_[results_left_ - 1];
    for (Uint32 i = 0; i < n_userAttrs_; i++) {
      userAttrs_[i] =  result->attrs_[i];
    }
    results_left_--;
    return true;
  } else {
    return false;
  }
}
