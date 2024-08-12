/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#define PROGRAM_HEADER_SIZE 2
#define RESULT_HEADER_SIZE 3
#define RESULT_ITEM_HEADER_SIZE 1

NdbAggregator::NdbAggregator(const NdbDictionary::Table* table) :
  table_impl_(nullptr), n_gb_cols_(0), n_agg_results_(0),
  agg_results_(nullptr), gb_map_(nullptr),
  finalized_(false), finished_(false),
  curr_prog_pos_(PROGRAM_HEADER_SIZE),
  instructions_length_(PROGRAM_HEADER_SIZE),
  result_record_fetched_(false),
  result_size_est_(RESULT_HEADER_SIZE * sizeof(Uint32) +
               RESULT_ITEM_HEADER_SIZE * sizeof(Uint32)),
  disk_columns_(false) {
    if (table != nullptr) {
      table_impl_ = & NdbTableImpl::getImpl(*table);
    }
    memset(agg_ops_, kOpUnknown, MAX_AGGREGATION_OP_SIZE * 4);
}

NdbAggregator::~NdbAggregator() {
  delete[] agg_results_;
  if (gb_map_) {
    for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
      delete[] iter->first.ptr;
    }
    delete gb_map_;
  }
}

Int32 NdbAggregator::ProcessRes(char* buf) {
  if (buf != nullptr) {
  }
  // Moz
  // Aggregation result
  assert(buf != nullptr);
  Uint32 parse_pos = 0;
  const Uint32* data_buf = (const Uint32*)buf;

  Uint32 n_gb_cols = data_buf[parse_pos] >> 16;
  Uint32 n_agg_results = data_buf[parse_pos++] & 0xFFFF;
  assert(n_gb_cols == n_gb_cols_);
  assert(n_agg_results == n_agg_results_);
  Uint32 n_res_items = data_buf[parse_pos++];
  //fprintf(stderr, "Moz-ProcessRes, GB cols: %u, AGG results: %u, RES items: %u\n",
  //    n_gb_cols, n_agg_results, n_res_items);

  AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols) {
    char* agg_rec = nullptr;
    // const AttributeHeader* header = nullptr;
    for (Uint32 i = 0; i < n_res_items; i++) {
      bool need_merge = false;
      Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
      Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;

      GBHashEntry entry{const_cast<char*>(
          reinterpret_cast<const char*>(&data_buf[parse_pos])),
                  gb_cols_len};
      auto iter = gb_map_->find(entry);
      if (iter != gb_map_->end()) {
        // header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
        agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
        // fprintf(stderr, "Moz, Found GBHashEntry, id: %u, byte_size: %u, "
        //     "data_size: %u, is_null: %u\n",
        //     header->getAttributeId(), header->getByteSize(),
        //     header->getDataSize(), header->isNULL());
        need_merge = true;
      } else {
        assert(n_agg_results * sizeof(AggResItem) == agg_res_len);
        agg_rec = new char[gb_cols_len + agg_res_len];
        memcpy(agg_rec, reinterpret_cast<const char*>(&data_buf[parse_pos]),
            gb_cols_len + agg_res_len);
        GBHashEntry new_entry{agg_rec, gb_cols_len};

        gb_map_->insert(std::make_pair<GBHashEntry, GBHashEntry>(
              std::move(new_entry), std::move(
                GBHashEntry{agg_rec + gb_cols_len,
                agg_res_len})));
        agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + agg_res_len);
      }

      assert(agg_res_len == n_agg_results * sizeof(AggResItem));
      const AggResItem* res = reinterpret_cast<const AggResItem*>(
                           &data_buf[parse_pos + (gb_cols_len >> 2)]);
      if (need_merge) {
        for (Uint32 i = 0; i < n_agg_results; i++) {
          assert(((res[i].type == NDB_TYPE_BIGINT &&
                  (res[i].is_unsigned == agg_res_ptr[i].is_unsigned ||
                   agg_res_ptr[i].is_null)) ||
                  res[i].type == NDB_TYPE_DOUBLE) &&
                  res[i].type == agg_res_ptr[i].type);
          if (res[i].is_null) {
          } else if (agg_res_ptr[i].is_null) {
            agg_res_ptr[i] = res[i];
          } else {
            agg_res_ptr[i].type = res[i].type;
            agg_res_ptr[i].is_unsigned = res[i].is_unsigned;
            switch (agg_ops_[i]) {
              case kOpSum:
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
                assert(res[i].type == NDB_TYPE_BIGINT);
                assert(res[i].is_unsigned == 1);
                agg_res_ptr[i].value.val_int64 += res[i].value.val_int64;
                break;
              case kOpMax:
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
                assert(0);
                break;
            }
          }
        }
      }
#if defined(MOZ_AGG_CHECK) && !defined(NDEBUG)
      {
        /*
         * Moz
         * Validation
         */
        Uint32 pos = parse_pos;
        for (Uint32 i = 0; i < n_gb_cols_; i++) {
          AttributeHeader ah(data_buf[pos]);
          /*
             fprintf(stderr,
             "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
             "res_len: %u, value: %p]\n",
             ah.getAttributeId(), ah.getByteSize(),
             ah.getDataSize(), gb_cols_len, agg_res_len,
             agg_res_ptr);
             */
          assert(ah.getDataPtr() != &data_buf[pos]);
          pos += sizeof(AttributeHeader) + ah.getDataSize() * sizeof(Int32);
          if (i == gb_cols_len - 1) {
            assert(pos == gb_cols_len);
          }
        }
      }
#endif // MOZ_AGG_CHECK && !NDEBUG
      parse_pos += ((gb_cols_len + agg_res_len) >> 2);
    }
  } else {
    Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
    Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
    assert(gb_cols_len == 0);
    // Get rid of warning in release-binary
    (void)gb_cols_len;
    assert(agg_res_len == n_agg_results_ * sizeof(AggResItem));
    assert(agg_results_ != nullptr);
    AggResItem* agg_res_ptr = agg_results_;
    const AggResItem* res = reinterpret_cast<const AggResItem*>(
                         &data_buf[parse_pos/* + (gb_cols_len >> 2)*/]);
    for (Uint32 i = 0; i < n_agg_results; i++) {
      assert((((res[i].type == NDB_TYPE_BIGINT &&
              (res[i].is_unsigned == agg_res_ptr[i].is_unsigned ||
               agg_res_ptr[i].is_null)) ||
              res[i].type == NDB_TYPE_DOUBLE) &&
              res[i].type == agg_res_ptr[i].type) ||
              agg_res_ptr[i].type == NDB_TYPE_UNDEFINED ||
              (res[i].type == NDB_TYPE_UNDEFINED &&
               n_gb_cols == 0));
      if (res[i].is_null) {
      } else if (agg_res_ptr[i].is_null) {
        agg_res_ptr[i] = res[i];
      } else {
        agg_res_ptr[i].type = res[i].type;
        agg_res_ptr[i].is_unsigned = res[i].is_unsigned;
        switch (agg_ops_[i]) {
          case kOpSum:
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
            assert(res[i].type == NDB_TYPE_BIGINT);
            assert(res[i].is_unsigned == 1);
            agg_res_ptr[i].value.val_int64 += res[i].value.val_int64;
            break;
          case kOpMax:
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
            assert(0);
            break;
        }
      }
    }
    parse_pos += ((/*gb_cols_len + */agg_res_len) >> 2);
  }
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
    (type & 0x1F) << 21 |
    (reg_id & 0x0F) << 16 |
    col_id;

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
    (type & 0x1F) << 21 |
    (reg_id & 0x0F) << 16 |
    col_id;
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

bool NdbAggregator::LoadUint64(Uint64 value, Uint32 reg_id) {
  buffer_[curr_prog_pos_++] =
    (kOpLoadConst) << 26 |
    (NDB_TYPE_BIGUNSIGNED & 0x1F) << 21 |
    (reg_id & 0x0F) << 16 |
    0;
  int8store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              value);
  curr_prog_pos_ += 2;
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

  buffer_[curr_prog_pos_++] =
    (kOpSum) << 26 |
    (reg_id & 0x0F) << 16 |
    agg_id;

  agg_ops_[agg_id] = kOpSum;
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
  Int32 col_id = col->getAttrId();
  buffer_[curr_prog_pos_++] = col_id << 16;

  result_size_est_ += (sizeof(AttributeHeader) + ((col->getSizeInBytes() + 3) & (~3)));
  // fprintf(stderr, "Group by %s, getSizeInBytes: %u,getArrayType: %u, getSize: %u, getLength: %u, est: %u\n",
  //     name, col->getSizeInBytes(), col->getArrayType(), col->getSize(), col->getLength(),
  //     result_size_est_);

  n_gb_cols_++;

  if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

  return true;
}

bool NdbAggregator::GroupBy(Int32 col_id) {
  const NdbDictionary::Column* col = table_impl_->getColumn(col_id);
  if (col == nullptr) {
    SetError(kErrInvalidColumnId);
    return false;
  }
  buffer_[curr_prog_pos_++] = col_id << 16;

  result_size_est_ += (sizeof(AttributeHeader) + ((col->getSizeInBytes() + 3) & (~3)));

  n_gb_cols_++;

  if (col->getStorageType() == NDB_STORAGETYPE_DISK) {
    disk_columns_ = true;
  }

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

  if (n_gb_cols_) {
    if (n_gb_cols_ >= MAX_AGG_N_GROUPBY_COLS) {
      SetError(kErrTooManyGroupbyCols);
      return false;
    }
    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>;
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
      agg_results_[i].type = NDB_TYPE_UNDEFINED;
      agg_results_[i].is_unsigned = false;
      agg_results_[i].is_null = true;
      agg_results_[i].value.val_int64 = 0;
      i++;
    }
  }

  result_size_est_ += sizeof(AggResItem) * n_agg_results_;

  if (result_size_est_ >= MAX_AGG_RESULT_BATCH_BYTES - 128) {
    SetError(kErrTooBigResult);
    /*
     * Moz
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
  const NdbDictionary::Column* col =
                      aggregator_->table_impl()->getColumn(id);
  NdbDictionary::Column::Type type = col->getType();
  bool is_null = header.isNULL();
  Uint32 byte_size = header.getByteSize();
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
