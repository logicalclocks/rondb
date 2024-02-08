/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
 *
 * Author: Zhao Song
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
  result_size_est_(RESULT_HEADER_SIZE * sizeof(uint32_t) +
               RESULT_ITEM_HEADER_SIZE * sizeof(uint32_t)),
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

int32_t NdbAggregator::ProcessRes(char* buf) {
  if (buf != nullptr) {
  }
  // Moz
  // Aggregation result
  assert(buf != nullptr);
  uint32_t parse_pos = 0;
  const uint32_t* data_buf = (const uint32_t*)buf;

  uint32_t n_gb_cols = data_buf[parse_pos] >> 16;
  uint32_t n_agg_results = data_buf[parse_pos++] & 0xFFFF;
  assert(n_gb_cols == n_gb_cols_);
  assert(n_agg_results == n_agg_results_);
  uint32_t n_res_items = data_buf[parse_pos++];
  //fprintf(stderr, "Moz-ProcessRes, GB cols: %u, AGG results: %u, RES items: %u\n",
  //    n_gb_cols, n_agg_results, n_res_items);

  AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols) {
    char* agg_rec = nullptr;
    // const AttributeHeader* header = nullptr;
    for (uint32_t i = 0; i < n_res_items; i++) {
      bool need_merge = false;
      uint32_t gb_cols_len = data_buf[parse_pos] >> 16;
      uint32_t agg_res_len = data_buf[parse_pos++] & 0xFFFF;

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
        for (uint32_t i = 0; i < n_agg_results; i++) {
          assert(((res[i].type == NDB_TYPE_BIGINT &&
                  res[i].is_unsigned == agg_res_ptr[i].is_unsigned) ||
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
        uint32_t pos = parse_pos;
        for (uint32_t i = 0; i < n_gb_cols_; i++) {
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
          pos += sizeof(AttributeHeader) + ah.getDataSize() * sizeof(int32_t);
          if (i == gb_cols_len - 1) {
            assert(pos == gb_cols_len);
          }
        }
      }
#endif // MOZ_AGG_CHECK && !NDEBUG
      parse_pos += ((gb_cols_len + agg_res_len) >> 2);
    }
  } else {
    uint32_t gb_cols_len = data_buf[parse_pos] >> 16;
    uint32_t agg_res_len = data_buf[parse_pos++] & 0xFFFF;
    assert(gb_cols_len == 0);
    // Get rid of warning in release-binary
    (void)gb_cols_len;
    assert(agg_res_len == n_agg_results_ * sizeof(AggResItem));
    assert(agg_results_ != nullptr);
    AggResItem* agg_res_ptr = agg_results_;
    const AggResItem* res = reinterpret_cast<const AggResItem*>(
                         &data_buf[parse_pos/* + (gb_cols_len >> 2)*/]);
    for (uint32_t i = 0; i < n_agg_results; i++) {
      assert((((res[i].type == NDB_TYPE_BIGINT &&
              res[i].is_unsigned == agg_res_ptr[i].is_unsigned) ||
              res[i].type == NDB_TYPE_DOUBLE) &&
              res[i].type == agg_res_ptr[i].type) ||
              agg_res_ptr[i].type == NDB_TYPE_UNDEFINED);
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

bool NdbAggregator::LoadColumn(const char* name, uint32_t reg_id) {
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

  int32_t col_id = col->getAttrId();
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
    int32_t decimal_info = col->getPrecision() << 16 |
                           col->getScale();
    int4store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              decimal_info);
    curr_prog_pos_++;
  }

  return true;
}

bool NdbAggregator::LoadColumn(int32_t col_id, uint32_t reg_id) {
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
    int32_t decimal_info = col->getPrecision() << 16 |
                           col->getScale();
    int4store(reinterpret_cast<char*>(&buffer_[curr_prog_pos_]),
              decimal_info);
    curr_prog_pos_++;
  }

  return true;
}

bool NdbAggregator::LoadUint64(uint64_t value, uint32_t reg_id) {
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

bool NdbAggregator::LoadInt64(int64_t value, uint32_t reg_id) {
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

bool NdbAggregator::LoadDouble(double value, uint32_t reg_id) {
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

bool NdbAggregator::CheckRegs(uint32_t reg_1, uint32_t reg_2) {
  if (reg_1 >= kRegTotal || reg_2 >= kRegTotal) {
    SetError(kErrInvalidRegNo);
    return false;
  }

  return true;
}

bool NdbAggregator::Mov(uint32_t reg_1, uint32_t reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMov) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Add(uint32_t reg_1, uint32_t reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpPlus) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Minus(uint32_t reg_1, uint32_t reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMinus) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Mul(uint32_t reg_1, uint32_t reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMul) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Div(uint32_t reg_1, uint32_t reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpDiv) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::Mod(uint32_t reg_1, uint32_t reg_2) {
  if (!CheckRegs(reg_1, reg_2)) {
    return false;
  }
  buffer_[curr_prog_pos_++] =
    (kOpMod) << 26 |
    (reg_1 & 0x0F) << 12 |
    (reg_2 & 0x0F) << 8;

  return true;
}

bool NdbAggregator::CheckAggAndReg(uint32_t agg_id, uint32_t reg_id) {
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

bool NdbAggregator::Sum(uint32_t agg_id, uint32_t reg_id) {
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

bool NdbAggregator::Max(uint32_t agg_id, uint32_t reg_id) {
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

bool NdbAggregator::Min(uint32_t agg_id, uint32_t reg_id) {
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

bool NdbAggregator::Count(uint32_t agg_id, uint32_t reg_id) {
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
  int32_t col_id = col->getAttrId();
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

bool NdbAggregator::GroupBy(int32_t col_id) {
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
    uint32_t i = 0;
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
     * Distruction will do it.
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
           static_cast<uint32_t>(n_agg_results_ * sizeof(AggResItem))},
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
      *reinterpret_cast<uint32_t*>(group_records_.ptr + curr_group_pos_));
  curr_group_pos_ += sizeof(AttributeHeader);

  uint32_t id = header.getAttributeId();
  const NdbDictionary::Column* col =
                      aggregator_->table_impl()->getColumn(id);
  NdbDictionary::Column::Type type = col->getType();
  bool is_null = header.isNULL();
  uint32_t byte_size = header.getByteSize();
  uint32_t word_size = header.getDataSize() * sizeof(int32_t);
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

#define sint3korr(A)  ((int32_t) ((((uint8_t) (A)[2]) & 128) ? \
                                  (((uint32_t) 255L << 24) | \
                                  (((uint32_t) (uint8_t) (A)[2]) << 16) |\
                                  (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                   ((uint32_t) (uint8_t) (A)[0])) : \
                                 (((uint32_t) (uint8_t) (A)[2]) << 16) |\
                                 (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                  ((uint32_t) (uint8_t) (A)[0])))

#define uint3korr(A)  (uint32_t) (((uint32_t) ((uint8_t) (A)[0])) +\
                                  (((uint32_t) ((uint8_t) (A)[1])) << 8) +\
                                  (((uint32_t) ((uint8_t) (A)[2])) << 16))

int32_t NdbAggregator::Column::data_medium() {
	return sint3korr(ptr_);
}
uint32_t NdbAggregator::Column::data_umedium() {
	return uint3korr(ptr_);
}
