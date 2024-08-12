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

#include <cstdint>
#include <cstring>
#include <utility>

#define DBTUP_C
#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "AggInterpreter.hpp"
#include "InterpreterCommonOp.hpp"
#include "decimal.h"
#include "Dbtup.hpp"

Uint32 AggInterpreter::g_buf_len_ = READ_BUF_WORD_SIZE;
Uint32 AggInterpreter::g_result_header_size_ = 3 * sizeof(Uint32);
Uint32 AggInterpreter::g_result_header_size_per_group_ = sizeof(Uint32);

bool AggInterpreter::Init() {
  if (inited_) {
    return true;
  }

  Uint32 value = 0;

  /*
   * 1. Double check the magic num and  total length of program.
   */
  value = prog_[cur_pos_++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == prog_len_);

  /*
   * 2. Get num of columns for group by and num of aggregation results;
   */
  value = prog_[cur_pos_++];
  n_gb_cols_ = (value >> 16) & 0xFFFF;
  n_agg_results_ = value & 0xFFFF;

  /*
   * 3. Get all the group by columns id.
   */
  if (n_gb_cols_) {
#ifdef MOZ_AGG_MALLOC
    assert(n_gb_cols_ <= MAX_AGG_N_GROUPBY_COLS);
    gb_cols_ = gb_cols_buf_;
#else
    gb_cols_ = new Uint32[n_gb_cols_];
#endif // MOZ_AGG_MALLOC

    Uint32 i = 0;
    while (i < n_gb_cols_ && cur_pos_ < prog_len_) {
      gb_cols_[i++] = prog_[cur_pos_++];
    }
#ifdef MOZ_AGG_MALLOC
    gb_map_ = &gb_map_buf_;
#else
    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>;
#endif // MOZ_AGG_MALLOC
  }

  /*
   * 4. Reset all aggregation results
   */
  if (n_agg_results_) {
#ifdef MOZ_AGG_MALLOC
    assert(n_agg_results_ <= MAX_AGG_N_RESULTS);
    agg_results_ = agg_results_buf_;
#else
    agg_results_ = new AggResItem[n_agg_results_];
#endif // MOZ_AGG_MALLOC
    Uint32 i = 0;
    while (i < n_agg_results_) {
      agg_results_[i].type = NDB_TYPE_UNDEFINED;
      agg_results_[i].value.val_int64 = 0;
      agg_results_[i].is_unsigned = false;
      agg_results_[i].is_null = true;
      i++;
    }
  }

  inited_ = true;
  agg_prog_start_pos_ = cur_pos_;
  memset(registers_, 0, sizeof(registers_));

  return true;
}

static bool TypeSupported(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:

    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:
      return true;
    default:
      return false;
  }
  return false;
}

static bool IsUnsigned(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:
    case NDB_TYPE_DECIMALUNSIGNED:
      return true;
    default:
      return false;
  }
  return false;
}

static DataType AlignedType(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

      return NDB_TYPE_BIGINT;
    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return NDB_TYPE_DOUBLE;
    /*
     * TODO (Zhao)
     * Moz
     * Temporary solultion
     * Currently regard Decimal as a undefined,
     * then decide it as BIGINT/BIGUNSIGNED/DOUBLE in LoadColumn
     * dynamically.
     */
    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:
      return NDB_TYPE_UNDEFINED;
    default:
      assert(0);
  }
  return NDB_TYPE_UNDEFINED;
}

static void PrintValue(const AggResItem* res, char* log_buf) {
  if (res->type == NDB_TYPE_BIGINT) {
    if (res->is_unsigned) {
      sprintf(log_buf + strlen(log_buf), "[%llu, %d, %d, %d]",
          res->value.val_uint64, res->type, res->is_unsigned, res->is_null);
    } else {
      sprintf(log_buf + strlen(log_buf), "[%lld, %d, %d, %d]",
          res->value.val_int64, res->type, res->is_unsigned, res->is_null);
    }
  } else {
    assert(res->type == NDB_TYPE_DOUBLE);
    sprintf(log_buf + strlen(log_buf), "[%lf, %d, %d, %d]",
        res->value.val_double, res->type, res->is_unsigned, res->is_null);
  }
  g_eventLogger->info("%s", log_buf);
}

static Int32 Sum(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Moz, Sum() init AggRes to ");
      PrintValue(res, log_buf);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = res->value.val_int64;
    Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (res->is_unsigned || val1 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
          // overflows;
          return -1;
        } else {
          res_unsigned = true;
        }
      } else {
        if ((Uint64)val0 > (Uint64)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (res->is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
            // overflows;
            return -1;
          } else {
            res_unsigned = true;
          }
        } else {
          if ((Uint64)val1 > (Uint64)(LLONG_MAX)) {
            res_unsigned = true;
          }
        }
      } else {
        if (val0 >= 0 && val1 >= 0) {
          res_unsigned = true;
        } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
          // overflow
          return -1;
        }
      }
    }

    // Check if res_val is overflow
    bool unsigned_flag = false;
    if (res_type == NDB_TYPE_BIGINT) {
      unsigned_flag = (a.is_unsigned | res->is_unsigned);
    } else {
      assert(res_type == NDB_TYPE_DOUBLE);
      unsigned_flag = (a.is_unsigned & res->is_unsigned);
    }
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (res->type == NDB_TYPE_DOUBLE) ?
                     res->value.val_double :
                     ((res->is_unsigned == true) ?
                       static_cast<double>(res->value.val_uint64) :
                       static_cast<double>(res->value.val_int64));
    double res_val = val0 + val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
    res->is_unsigned = false;
  }

  res->type = res_type;
  res->is_null = false;

  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Moz, Sum(), update AggRes to ");
    PrintValue(res, log_buf);
  }
  return 0;
}

static Int32 Max(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    // Agg result first initialized
    *res = a;
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Moz, Max(), init AggRes to ");
      PrintValue(res, log_buf);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 > res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 > res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
        res->value.val_uint64 = a.value.val_uint64;
      } else {
        res->value.val_uint64 = a.value.val_uint64 >
                static_cast<Uint64>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<Uint64>(res->value.val_int64);
      }
      res->is_unsigned = true;
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64) >
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double > res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Moz, Max(), update AggRes to ");
    PrintValue(res, log_buf);
  }

  return 0;
}

static Int32 Min(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED || res->is_null) {
    // Agg result first initialized
    *res = a;
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Moz, Min(), init AggRes to ");
      PrintValue(res, log_buf);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 < res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 < res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = a.value.val_uint64 <
                static_cast<Uint64>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<Uint64>(res->value.val_int64);
        res->is_unsigned = true;
      }
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
        res->value.val_int64 = a.value.val_int64;
        res->is_unsigned = false;
      } else {
        res->value.val_uint64 = static_cast<Uint64>(a.value.val_int64) <
                                res->value.val_uint64 ?
                                static_cast<Uint64>(a.value.val_int64) :
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double < res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Moz, Min(), update AggRes to ");
    PrintValue(res, log_buf);
  }

  return 0;
}

static Int32 Count(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    res->type = NDB_TYPE_BIGINT;
    res->value.val_uint64 = 0;
    res->is_unsigned = true;
    res->is_null = false;
    if (print) {
      char log_buf[128];
      sprintf(log_buf, "Moz, Count(), init AggRes to ");
      PrintValue(res, log_buf);
    }
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  assert(res->type == NDB_TYPE_BIGINT &&
      res->is_null == false && res->is_unsigned == true);
  res->value.val_uint64 += 1;

  if (print) {
    char log_buf[128];
    sprintf(log_buf, "Moz, Count(), update AggRes to ");
    PrintValue(res, log_buf);
  }

  return 0;
}

/*
 * Success: RETURN 0
 * Failure: RETURN 1860+ by aggregation interpreter
 *          Others returned by readAttributes
 */
Int32 AggInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct) {
  // assert(inited_);
  // assert(req_struct->read_length == 0);
  if (!inited_ || req_struct->read_length != 0) {
    g_eventLogger->debug("AggInterpreter::ProcessRec error, inited: %d, read_length: %u",
            inited_, req_struct->read_length);
    return ZAGG_OTHER_ERROR;
  }

  AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols_) {
    char* agg_rec = nullptr;

    AttributeHeader* header = nullptr;
    buf_pos_ = 0;
    for (Uint32 i = 0; i < n_gb_cols_; i++) {
      int ret = block_tup->readAttributes(req_struct, &(gb_cols_[i]), 1,
                    buf_ + buf_pos_, g_buf_len_ - buf_pos_);
      // assert(ret >= 0);
      if (ret < 0) {
        g_eventLogger->debug("read group by column error: %d", ret);
        return -ret;
      }
      header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
      buf_pos_ += (1 + header->getDataSize());
    }

    Uint32 len_in_char = buf_pos_ * sizeof(Uint32);
    GBHashEntry entry{reinterpret_cast<char*>(buf_), len_in_char};
    auto iter = gb_map_->find(entry);
    if (iter != gb_map_->end()) {
      header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
      agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
      if (print_) {
        g_eventLogger->debug("Moz, Found GBHashEntry, id: %u, byte_size: %u, "
            "data_size: %u, is_null: %u",
            header->getAttributeId(), header->getByteSize(),
            header->getDataSize(), header->isNULL());
      }
    } else {
      /*
       * update req_struct->read_length here, which will update the
       * Dblqh::ScanRecord::m_curr_batch_size_bytes later in the
       * Dblqh::scanTupkeyConfLab, even we don't use that variable
       * to decide whether reaches batch limitation. Only increase
       * Dblqh::ScanRecord::m_curr_batch_size_bytes when new group
       * item is inserted into gb_map_.
       * For aggregation,
       * we use Dblqh::ScanRecord::m_agg_curr_batch_size_bytes to
       * indicate batch limitation
       */
      req_struct->read_length = (len_in_char +
                       n_agg_results_ * sizeof(AggResItem)) / sizeof(Int32);

      // we use result_size_ to decide whether need to send some aggregation
      // results to API.
      result_size_ += len_in_char +
                       n_agg_results_ * sizeof(AggResItem);
#ifdef MOZ_AGG_MALLOC
      agg_rec = MemAlloc(len_in_char +
                          n_agg_results_ * sizeof(AggResItem));
#else
      agg_rec = new char[len_in_char +
                        n_agg_results_ * sizeof(AggResItem)];
#endif // MOZ_AGG_MALLOC
      memset(agg_rec, 0, len_in_char +
                        n_agg_results_ * sizeof(AggResItem));
      memcpy(agg_rec, reinterpret_cast<char*>(buf_), len_in_char);
      GBHashEntry new_entry{agg_rec, len_in_char};

      gb_map_->insert(std::make_pair<GBHashEntry, GBHashEntry>(
                      std::move(new_entry), std::move(
            GBHashEntry{agg_rec + len_in_char,
            static_cast<Uint32>(n_agg_results_ * sizeof(AggResItem))})));
      n_groups_ = gb_map_->size();
      agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + len_in_char);

      for (uint32_t i = 0; i < n_agg_results_; i++) {
        agg_res_ptr[i].type = NDB_TYPE_UNDEFINED;
        agg_res_ptr[i].value.val_int64 = 0;
        agg_res_ptr[i].is_unsigned = false;
        agg_res_ptr[i].is_null = true;
        assert(agg_res_ptr[i].type == agg_results_[i].type);
        assert(agg_res_ptr[i].value.val_int64 == agg_results_[i].value.val_int64);
        assert(agg_res_ptr[i].is_unsigned == agg_results_[i].is_unsigned);
        assert(agg_res_ptr[i].is_null == agg_results_[i].is_null);
      }
    }
  } else {
    agg_res_ptr = agg_results_;
  }

  Uint32 col_index;

  Uint32 value;
  DataType type;
  bool is_unsigned;
  Uint32 reg_index;

  Uint32 reg_index2;

  Uint32 agg_index;

  const Uint32* attrDescriptor = nullptr;

  Int32 decimal_info = 0;
  Int32 precision = 0;
  Int32 scale = 0;
  decimal_t decimal;
  decimal.buf = decimal_buf_;
  Int32 dec_ret = E_DEC_OK;
  Uint8* dec_buf_ptr = nullptr;
  longlong dec_val_ll = 0;
  ulonglong dec_val_ull = 0;

  Uint32 exec_pos = agg_prog_start_pos_;
  while (exec_pos < prog_len_) {
    value = prog_[exec_pos++];
    Uint8 op = (value & 0xFC000000) >> 26;
    int ret = 0;
    buf_pos_ = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpPlus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegPlusReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[PLUS], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMinus:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegMinusReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MINUS], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMul:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegMulReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MUL], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpDiv:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegDivReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index], false);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[DIV], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpDivInt:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegDivReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index], true);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[DIVINT], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMod:
        reg_index = (value >> 12) & 0x0F;
        reg_index2 = (value >> 8) & 0x0F;

        ret = RegModReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[MOD], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        col_index = (value & 0x0000FFFF) << 16;

        ret = block_tup->readAttributes(req_struct, &(col_index), 1,
                  buf_ + buf_pos_, g_buf_len_ - buf_pos_);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("read column error: %d", ret);
          return -ret;
        }
        header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
        attrDescriptor = req_struct->tablePtrP->tabDescriptor +
          (((col_index) >> 16) * ZAD_SIZE);
        assert(header->getAttributeId() == (col_index >> 16));

        assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
        // assert(TypeSupported(type));
        if (!TypeSupported(type)) {
          g_eventLogger->debug("Unsupported column type: %u", type);
          return ZAGG_COL_TYPE_UNSUPPORTED;
        }

        ResetRegister(&registers_[reg_index]);
        registers_[reg_index].type = AlignedType(type);
        registers_[reg_index].is_unsigned = is_unsigned;
        registers_[reg_index].is_null = header->isNULL();
        if (registers_[reg_index].is_null) {
          // Column has a null value
          // g_eventLogger->info("Moz-Intp: Load NULL, type: %u",
          //     registers_[reg_index].type);
          registers_[reg_index].value.val_int64 = 0;
          break;
        }
        switch (type) {
          case NDB_TYPE_TINYINT:
            registers_[reg_index].value.val_int64 =
                *reinterpret_cast<Int8*>(&buf_[buf_pos_ + 1]);
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_TINYINT %ld",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_SMALLINT:
            registers_[reg_index].value.val_int64 =
                sint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_SMALLINT %ld",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_MEDIUMINT:
            registers_[reg_index].value.val_int64 =
                sint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_MEDIUM %ld",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_INT:
            registers_[reg_index].value.val_int64 =
                sint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_INT %ld",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_BIGINT %ld",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_TINYUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                *reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_TINYUNSIGNED %lu",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_SMALLUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_SMALLUNSIGNED %lu",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_MEDIUMUNSIGNED %lu",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_UNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_UNSIGNED %lu",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_BIGUNSIGNED %lu",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_FLOAT:
            registers_[reg_index].value.val_double =
                floatget(reinterpret_cast<unsigned char*>(&buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_FLOAT %lf",
            //     registers_[reg_index].value.val_double);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &buf_[buf_pos_ + 1]));
            // g_eventLogger->info("Moz-Intp: Load NDB_TYPE_DOUBLE %lf",
            //     registers_[reg_index].value.val_double);
            break;
          case NDB_TYPE_DECIMAL:
            decimal_info =
                sint4korr(reinterpret_cast<char*>(&prog_[exec_pos]));
            precision = decimal_info >> 16;
            scale = decimal_info & 0xFFFF;
            exec_pos += 1;
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                AttributeDescriptor::getSizeInBytes(attrDescriptor[0]));
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            decimal.len = AttributeDescriptor::getSizeInBytes(attrDescriptor[0]);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&buf_[buf_pos_ + 1]),
                      &decimal, precision, scale);
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_PARSE_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_PARSE_ERROR;
              }
            }
            /*
             * Moz
             * convery from decimal to supported type dynamically.
             */
            if (scale != 0) {
              dec_ret = decimal2double(&decimal, &(registers_[reg_index].value.val_double));
              registers_[reg_index].type = NDB_TYPE_DOUBLE;
            } else {
              dec_ret = decimal2longlong(&decimal, &dec_val_ll);
              registers_[reg_index].value.val_int64 = dec_val_ll;
              registers_[reg_index].type = NDB_TYPE_BIGINT;
            }
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
            assert(registers_[reg_index].is_unsigned == false);
            // if (frag_id_ == 0) {
            //   if (scale != 0) {
            //     g_eventLogger->info("Moz-Intp: Load NDB_TYPE_DECIMAL[double] %lf",
            //         registers_[reg_index].value.val_double);
            //   } else {
            //     g_eventLogger->info("Moz-Intp: Load NDB_TYPE_DECIMAL[int64] %ld",
            //         registers_[reg_index].value.val_int64);
            //   }
            // }
          break;
        case NDB_TYPE_DECIMALUNSIGNED:
            decimal_info =
                sint4korr(reinterpret_cast<char*>(&prog_[exec_pos]));
            precision = decimal_info >> 16;
            scale = decimal_info & 0xFFFF;
            exec_pos += 1;
            assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
                AttributeDescriptor::getSizeInBytes(attrDescriptor[0]));
            // memset(decimal.buf, 0, sizeof(Int32) * DECIMAL_BUFF_LENGTH);
            decimal.len = AttributeDescriptor::getSizeInBytes(attrDescriptor[0]);
            dec_ret = bin2decimal(reinterpret_cast<const uchar*>(&buf_[buf_pos_ + 1]),
                      &decimal, precision, scale);
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while parsing decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_PARSE_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_PARSE_ERROR;
              }
            }
            /*
             * Moz
             * convery from decimal to supported type dynamically.
             */
            if (scale != 0) {
              dec_ret = decimal2double(&decimal, &(registers_[reg_index].value.val_double));
              registers_[reg_index].type = NDB_TYPE_DOUBLE;
              assert(registers_[reg_index].is_unsigned == false);
            } else {
              dec_ret = decimal2ulonglong(&decimal, &dec_val_ull);
              registers_[reg_index].value.val_uint64 = dec_val_ull;
              registers_[reg_index].type = NDB_TYPE_BIGUNSIGNED;
              registers_[reg_index].is_unsigned = true;
            }
            // assert(dec_ret == E_DEC_OK);
            if (dec_ret != E_DEC_OK) {
              dec_buf_ptr = reinterpret_cast<Uint8*>(&buf_[buf_pos_ + 1]);
              char log_buf[128];
              sprintf(log_buf, "Error while converting decimal: ");
              for (Uint32 i = 0;
                  i < AttributeDescriptor::getSizeInBytes(attrDescriptor[0]); i++) {
                sprintf(log_buf + strlen(log_buf), "%x ", *(dec_buf_ptr + i));
              }
              g_eventLogger->debug("%s", log_buf);
              if (dec_ret == E_DEC_OVERFLOW) {
                return ZAGG_DECIMAL_CONV_OVERFLOW;
              } else {
                return ZAGG_DECIMAL_CONV_ERROR;
              }
            }
            // if (frag_id_ == 0) {
            //   if (scale != 0) {
            //     g_eventLogger->info("Moz-Intp: Load NDB_TYPE_DECIMALUNSIGNED[double] %lf",
            //         registers_[reg_index].value.val_double);
            //   } else {
            //     g_eventLogger->info("Moz-Intp: Load NDB_TYPE_DECIMALUNSIGNED[uin64] %lu",
            //         registers_[reg_index].value.val_uint64);
            //   }
            // }
          break;

          default:
            // assert(0);
            return ZAGG_LOAD_COL_WRONG_TYPE;
        }
        break;
      case kOpLoadConst:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        assert(type == NDB_TYPE_BIGINT || type == NDB_TYPE_BIGUNSIGNED ||
               type == NDB_TYPE_DOUBLE);
        ResetRegister(&registers_[reg_index]);
        registers_[reg_index].type = AlignedType(type);
        registers_[reg_index].is_unsigned = IsUnsigned(type);
        registers_[reg_index].is_null = false;
        switch (type) {
          case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&prog_[exec_pos]));
            // g_eventLogger->info("Moz-Intp: LoadConst[%u] NDB_TYPE_BIGINT %ld",
            //     reg_index, registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&prog_[exec_pos]));
            // g_eventLogger->info("Moz-Intp: LoadConst[%u] "
            //                 "NDB_TYPE_BIGUNSIGNED %lu",
            //     reg_index, registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &prog_[exec_pos]));
            // g_eventLogger->info("Moz-Intp: LoadConst[%u] NDB_TYPE_DOUBLE %lf",
            //     reg_index, registers_[reg_index].value.val_double);
            break;
          default:
            // assert(0);
            return ZAGG_LOAD_CONST_WRONG_TYPE;
        }
        exec_pos += 2;
        break;
      case kOpMov:
        reg_index = (value >> 12 ) & 0x0F;
        reg_index2 = (value >> 8 ) & 0x0F;

        registers_[reg_index] = registers_[reg_index2];
        break;
      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Sum(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        // assert(ret >= 0);
        if (ret < 0) {
          g_eventLogger->debug("Overflow[SUM], value is out of range");
          return ZAGG_MATH_OVERFLOW;
        }
        break;
      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Max(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        // assert(ret >= 0);
        break;
      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Min(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        // assert(ret >= 0);
        break;
      case kOpCount:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Count(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        // assert(ret >= 0);
        break;

      default:
        // assert(0);
        return ZAGG_WRONG_OPERATION;
    }
  }
  processed_rows_++;
  return 0;
}

void AggInterpreter::Print() {
  // if (!print_) {
  //   return;
  // }
  char log_buf[1024];
  if (n_gb_cols_) {
    if (gb_map_) {
      sprintf(log_buf, "Group by columns: [");
      for (Uint32 i = 0; i < n_gb_cols_; i++) {
        if (i != n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u ", gb_cols_[i] >> 16);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u", gb_cols_[i] >> 16);
        }
      }
      sprintf(log_buf + strlen(log_buf), "]");
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';

      g_eventLogger->info("Num of groups: %lu, Aggregation results:",
                          gb_map_->size());
      for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
        int pos = 0;
        sprintf(log_buf, "(");
        for (Uint32 i = 0; i < n_gb_cols_; i++) {
          if (i != n_gb_cols_ - 1) {
            sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter->first.ptr + pos);
          } else {
            sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter->first.ptr + pos);
          }
        }

        AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
        for (Uint32 i = 0; i < n_agg_results_; i++) {
          sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
                  item[i].is_unsigned, item[i].is_null);
          if (item[i].is_null) {
            sprintf(log_buf + strlen(log_buf), "[NULL]");
          } else {
            switch (item[i].type) {
              case NDB_TYPE_BIGINT:
                sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
                break;
              case NDB_TYPE_DOUBLE:
                sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
                break;
              default:
                assert(0);
            }
          }
        }
        g_eventLogger->info("%s", log_buf);
      }
    }
  } else {
    AggResItem* item = agg_results_;
    log_buf[0] = '\0';
    for (Uint32 i = 0; i < n_agg_results_; i++) {
      sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
              item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        sprintf(log_buf + strlen(log_buf), "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    g_eventLogger->info("%s", log_buf);
  }
}

// NOTICE: Need to define agg_ops[] before using this func.
void AggInterpreter::MergePrint(const AggInterpreter* in1,
                                   const AggInterpreter* in2) {
  assert(in1 != nullptr && in2 != nullptr);
  assert(in1->n_agg_results_ == in2->n_agg_results_);
  auto iter1 = in1->gb_map_->begin();
  auto iter2 = in2->gb_map_->begin();
  char log_buf[1024];
  log_buf[0] = '\0';

  while (iter1 != in1->gb_map_->end() && iter2 != in2->gb_map_->end()) {
    Uint32 len1 = iter1->first.len;
    Uint32 len2 = iter2->first.len;
#ifdef NDEBUG
    (void)len2;
#endif // NDEBUG
    assert(len1 == len2);

    int ret = memcmp(iter1->first.ptr, iter2->first.ptr, len1);
    if (ret < 0) {
      int pos = 0;
      sprintf(log_buf + strlen(log_buf), "(");
      for (Uint32 i = 0; i < in1->n_gb_cols_; i++) {
        if (i != in1->n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter1->first.ptr + pos);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter1->first.ptr + pos);
        }
      }
      AggResItem* item = reinterpret_cast<AggResItem*>(iter1->second.ptr);
      for (Uint32 i = 0; i < in1->n_agg_results_; i++) {
        sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
            item[i].is_unsigned, item[i].is_null);
        if (item[i].is_null) {
          sprintf(log_buf + strlen(log_buf), "[NULL]");
        } else {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';
      iter1++;
    } else if (ret > 0) {
      int pos = 0;
      sprintf(log_buf + strlen(log_buf), "(");
      for (Uint32 i = 0; i < in2->n_gb_cols_; i++) {
        if (i != in2->n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter2->first.ptr + pos);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter2->first.ptr + pos);
        }
      }
      AggResItem* item = reinterpret_cast<AggResItem*>(iter2->second.ptr);
      for (Uint32 i = 0; i < in2->n_agg_results_; i++) {
        sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
            item[i].is_unsigned, item[i].is_null);
        if (item[i].is_null) {
          sprintf(log_buf + strlen(log_buf), "[NULL]");
        } else {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';
      iter2++;
    } else {
      int pos = 0;
      sprintf(log_buf + strlen(log_buf), "(");
      for (Uint32 i = 0; i < in1->n_gb_cols_; i++) {
        if (i != in1->n_gb_cols_ - 1) {
          sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter1->first.ptr + pos);
        } else {
          sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter1->first.ptr + pos);
        }
      }
      AggResItem* item1 = reinterpret_cast<AggResItem*>(iter1->second.ptr);
      AggResItem* item2 = reinterpret_cast<AggResItem*>(iter2->second.ptr);
      AggResItem result;
      // NOTICE: Need to define agg_ops[] first.
      Uint32 agg_ops[32];
      for (Uint32 i = 0; i < in1->n_agg_results_; i++) {
        assert(((item1[i].type == NDB_TYPE_BIGINT &&
                item1[i].is_unsigned == item2[i].is_unsigned) ||
                item1[i].type == NDB_TYPE_DOUBLE) &&
                item1[i].type == item2[i].type);
        if (item1[i].is_null) {
          result = item2[i];
        } else if (item2[i].is_null) {
          result = item1[i];
        } else {
          result.type = item1[i].type;
          result.is_unsigned = item1[i].is_unsigned;
          switch (agg_ops[i]) {
            case kOpSum:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 = (item1[i].value.val_uint64 +
                                                 item2[i].value.val_uint64);
                } else {
                  result.value.val_int64 = (item1[i].value.val_int64 +
                                                 item2[i].value.val_int64);
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double = (item1[i].value.val_double +
                                               item2[i].value.val_double);
              }
              break;
            case kOpCount:
              assert(item1[i].type == NDB_TYPE_BIGINT);
              assert(item1[i].is_unsigned == 1);
              result.value.val_int64 = (item1[i].value.val_int64 +
                                             item2[i].value.val_int64);
              break;
            case kOpMax:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 =
                    item1[i].value.val_uint64 >= item2[i].value.val_uint64 ?
                    item1[i].value.val_uint64 : item2[i].value.val_uint64;
                } else {
                  result.value.val_int64 =
                    item1[i].value.val_int64 >= item2[i].value.val_int64 ?
                    item1[i].value.val_int64 : item2[i].value.val_int64;
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double =
                  item1[i].value.val_double >= item2[i].value.val_double ?
                  item1[i].value.val_double : item2[i].value.val_double;
              }
              break;
            case kOpMin:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 =
                    item1[i].value.val_uint64 <= item2[i].value.val_uint64 ?
                    item1[i].value.val_uint64 : item2[i].value.val_uint64;
                } else {
                  result.value.val_int64 =
                    item1[i].value.val_int64 <= item2[i].value.val_int64 ?
                    item1[i].value.val_int64 : item2[i].value.val_int64;
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double =
                  item1[i].value.val_double <= item2[i].value.val_double ?
                  item1[i].value.val_double : item2[i].value.val_double;
              }
              break;
            default:
              assert(0);
              break;
          }
        }
        sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", result.type,
            result.is_unsigned, result.is_null);
        if (result.is_null) {
          sprintf(log_buf + strlen(log_buf), "[NULL]");
        } else {
          switch (result.type) {
            case NDB_TYPE_BIGINT:
              sprintf(log_buf + strlen(log_buf), "[%15lld]", result.value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              sprintf(log_buf + strlen(log_buf), "[%31.16f]", result.value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      g_eventLogger->info("%s", log_buf);
      log_buf[0] = '\0';
      iter1++;
      iter2++;
    }
  }
  while (iter1 != in1->gb_map_->end()) {
    int pos = 0;
    sprintf(log_buf + strlen(log_buf), "(");
    for (Uint32 i = 0; i < in1->n_gb_cols_; i++) {
      if (i != in1->n_gb_cols_ - 1) {
        sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter1->first.ptr + pos);
      } else {
        sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter1->first.ptr + pos);
      }
    }
    AggResItem* item = reinterpret_cast<AggResItem*>(iter1->second.ptr);
    for (Uint32 i = 0; i < in1->n_agg_results_; i++) {
      sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
          item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        sprintf(log_buf + strlen(log_buf), "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    g_eventLogger->info("%s", log_buf);
    log_buf[0] = '\0';
  }
  while (iter2 != in2->gb_map_->end()) {
    int pos = 0;
    sprintf(log_buf + strlen(log_buf), "(");
    for (Uint32 i = 0; i < in2->n_gb_cols_; i++) {
      if (i != in2->n_gb_cols_ - 1) {
        sprintf(log_buf + strlen(log_buf), "%u: %p, ", i, iter2->first.ptr + pos);
      } else {
        sprintf(log_buf + strlen(log_buf), "%u: %p): ", i, iter2->first.ptr + pos);
      }
    }
    AggResItem* item = reinterpret_cast<AggResItem*>(iter2->second.ptr);
    for (Uint32 i = 0; i < in2->n_agg_results_; i++) {
      sprintf(log_buf + strlen(log_buf), "(%u, %u, %u)", item[i].type,
          item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        sprintf(log_buf + strlen(log_buf), "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            sprintf(log_buf + strlen(log_buf), "[%15lld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            sprintf(log_buf + strlen(log_buf), "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    g_eventLogger->info("%s", log_buf);
    log_buf[0] = '\0';
    iter2++;
  }
}


Uint32 AggInterpreter::PrepareAggResIfNeeded(Signal* signal, bool force) {
  // Limitation
  Uint32 total_size = result_size_ +
                  (gb_map_ ?
                   gb_map_->size() * g_result_header_size_per_group_ : 0) +
                  g_result_header_size_;
  if (!force && (gb_map_ == nullptr ||
        total_size < DEF_AGG_RESULT_BATCH_BYTES)) {
    return 0;
  }
  if (force &&
      (n_gb_cols_ != 0 && (gb_map_ == nullptr || gb_map_->size() == 0))) {
    assert(result_size_ == 0);
    return 0;
  }
  Uint32* data_buf = (&signal->theData[25]);
  Uint32 pos = 0;
  assert(n_gb_cols_ < 0xFFFF);
  assert(n_agg_results_ < 0xFFFF);

  if (n_gb_cols_) {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = n_gb_cols_ << 16 | n_agg_results_;
    data_buf[pos++] = gb_map_->size();
    for (auto iter = gb_map_->begin(); iter != gb_map_->end();) {
      assert(iter->first.len % 4 == 0 && iter->first.len < 0xFFFF);
      assert(iter->second.len % 4 == 0 && iter->second.len < 0xFFFF);
      data_buf[pos++] = iter->first.len << 16 | iter->second.len;
      assert(iter->first.ptr + (iter->first.len + iter->second.len) ==
          iter->second.ptr + iter->second.len);
      MEMCOPY_NO_WORDS(&data_buf[pos], iter->first.ptr,
          (iter->first.len + iter->second.len) >> 2);
      pos += ((iter->first.len + iter->second.len) >> 2);
#ifndef MOZ_AGG_MALLOC
      delete[] iter->first.ptr;
#endif // !MOZ_AGG_MALLOC
      gb_map_->erase(iter++);
      result_size_ = 0;
    }
#ifdef MOZ_AGG_MALLOC
    alloc_len_ = 0;
#endif // MOZ_AGG_MALLOC
    assert(gb_map_->empty());
  } else {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = n_gb_cols_ << 16 | n_agg_results_;
    data_buf[pos++] = 0;
    data_buf[pos++] = 0 << 16 | (n_agg_results_ * sizeof(AggResItem));
    assert(gb_map_ == nullptr);
    MEMCOPY_NO_WORDS(&data_buf[pos], agg_results_,
        (n_agg_results_ * sizeof(AggResItem)) >> 2);
    pos += ((n_agg_results_ * sizeof(AggResItem)) >> 2);
  }

#if defined(MOZ_AGG_CHECK) && !defined(NDEBUG)
  /*
   * Moz
   * Validation
   */
  Uint32 data_len = pos;
  Uint32 parse_pos = 0;

  while (parse_pos < data_len) {
    AttributeHeader agg_checker_ah(data_buf[parse_pos++]);
    assert(agg_checker_ah.getAttributeId() == AttributeHeader::AGG_RESULT &&
           agg_checker_ah.getByteSize() == 0x0721);
    Uint32 n_gb_cols = data_buf[parse_pos] >> 16;
    Uint32 n_agg_results = data_buf[parse_pos++] & 0xFFFF;
    Uint32 n_res_items = data_buf[parse_pos++];
    // g_eventLogger->info("Moz, GB cols: %u, AGG results: %u, RES items: %u",
    //         n_gb_cols, n_agg_results, n_res_items);

    if (n_gb_cols) {
      // char log_buf[128];
      for (Uint32 i = 0; i < n_res_items; i++) {
        Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
        Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
        // remove compile warnings
        (void)gb_cols_len;
        (void)agg_res_len;
        for (Uint32 j = 0; j < n_gb_cols; j++) {
          AttributeHeader ah(data_buf[parse_pos++]);
          // sprintf(log_buf,
          //     "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
          //     "res_len: %u, value: ",
          //     ah.getAttributeId(), ah.getByteSize(),
          //     ah.getDataSize(), gb_cols_len, agg_res_len);
          assert(ah.getDataPtr() != &data_buf[parse_pos]);
          // char* ptr = (char*)(&data_buf[parse_pos]);
          // for (Uint32 i = 0; i < ah.getByteSize(); i++) {
          //   sprintf(log_buf + strlen(log_buf), " %x", ptr[i]);
          // }
          parse_pos += ah.getDataSize();
          // sprintf(log_buf + strlen(log_buf), "]");
        }
        for (Uint32 i = 0; i < n_agg_results; i++) {
          // AggResItem* ptr = (AggResItem*)(&data_buf[parse_pos]);
          // sprintf(log_buf + strlen(log_buf), "(type: %u, is_unsigned: %u, is_null: %u, value: ",
          //         ptr->type, ptr->is_unsigned, ptr->is_null);
          // switch (ptr->type) {
          //   case NDB_TYPE_BIGINT:
          //     sprintf(log_buf + strlen(log_buf), "%15ld", ptr->value.val_int64);
          //     break;
          //   case NDB_TYPE_DOUBLE:
          //     sprintf(log_buf + strlen(log_buf), "%31.16f", ptr->value.val_double);
          //     break;
          //   default:
          //     assert(0);
          // }
          // sprintf(log_buf + strlen(log_buf), ")");
          parse_pos += (sizeof(AggResItem) >> 2);
        }
        // g_eventLogger->info("%s", log_buf);
      }
    } else {
      assert(n_gb_cols == 0);
      assert(n_agg_results == n_agg_results_);
      assert(n_res_items == 0);
      Uint32 gb_cols_len = data_buf[parse_pos] >> 16;
      Uint32 agg_res_len = data_buf[parse_pos++] & 0xFFFF;
      assert(gb_cols_len == 0);
      assert(agg_res_len == n_agg_results_ * sizeof(AggResItem));
      parse_pos += (agg_res_len >> 2);
    }
  }
  assert(parse_pos == data_len);
#endif // MOZ_AGG_CHECK && !NDEBUG
  return pos;
}

Uint32 AggInterpreter::NumOfResRecords(bool last_time) {
  /*
   * Moz
   * NumOfResRecords is called after PrepareAggResIfNeeded
   * to see if there's no result left in the interpreter.
   * we use this return value to stop Dblqh::scanTupkeyRefLab
   * to send scanfragconf to TC wrongly
   * see [MOZ-COMMENT] there.
   */

  if (!last_time) {
    /*
     * if it's not the last time PrepareAggResIfNeeded,
     * here we can return the real value.
     * NOTICE:
     * always return 1 even if gb_map_ is empty().
     * In this situation: pushdown aggregation with filter and
     * group by. 99% rows has been filtered out which means
     * gb_map_ has big chance to stay empty. In order to stop
     * Dblqh::scanTupkeyRefLab send scanfragconf before aggregation
     * scan finishes. here return 1 to stop that.
     */
    if (gb_map_) {
      return (gb_map_->empty() ? 1 : gb_map_->size());
    } else {
      /*
       * In non-groupby mode, before we send the result to API
       * at the last time. we always return 1.
       * NOTICE:
       * In non-groupby mode, we still need to stop scanTupkeyRefLab
       * send scanfragconf wrongly.
       */
      return 1;
    }
  } else {
    /*
     * This is the last time we call PrepareAggResIfNeeded, so the
     * aggregation is going to finish.
     * We assert all results have been sent and return 0 here.
     */
    if (gb_map_) {
      assert(gb_map_->empty());
    }
    return 0;
  }
}

#ifdef MOZ_AGG_MALLOC
char* AggInterpreter::MemAlloc(Uint32 len) {
  if (alloc_len_ + len >= MAX_AGG_RESULT_BATCH_BYTES) {
    return nullptr;
  } else {
    char* ptr = &(mem_buf_[alloc_len_]);
    alloc_len_ += len;
    return ptr;
  }
}

void AggInterpreter::Destruct(AggInterpreter* ptr) {
  if (ptr == nullptr) {
    return;
  }
  /*
  Ndbd_mem_manager* _mm = ptr->mm();
  Uint32 _page_ref = ptr->page_ref();
  _mm->release_page(RT_DBTUP_PAGE, _page_ref);
  */
  lc_ndbd_pool_free(ptr);
}
#endif // MOZ_AGG_MALLOC
