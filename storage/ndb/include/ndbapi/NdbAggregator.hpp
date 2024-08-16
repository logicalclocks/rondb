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

#ifndef NDBAGGREGATOR_H_
#define NDBAGGREGATOR_H_

#include "NdbDictionary.hpp"
#include "NdbAggregationCommon.hpp"
#include <map>

class NdbTableImpl;

#define MAX_PROGRAM_SIZE 4096
#define MAX_AGGREGATION_OP_SIZE 256

typedef struct AggregationError {
  Uint32 errno_;
  const char* err_msg_;
} AggregationError;

enum NdbAggregatorError {
  kErrUnSupportedColumn = 0,
  kErrInvalidColumnName,
  kErrInvalidColumnId,
  kErrInvalidRegNo,
  kErrInvalidAggNo,
  kErrAggNoUsed,
  kErrEmptyProgram,
  kErrAlreadyFinalized,
  kErrTooBigResult,
  kErrTooBigProgram,
  kErrTooManyGroupbyCols,
  kErrEmptyAggResult,
  kErrTooManyAggResult,
  kErrMaxErrno
};

static AggregationError g_errors_[] = {
  {kErrUnSupportedColumn, "Column type hasn't been supported"},
  {kErrInvalidColumnName, "Column name is invalid"},
  {kErrInvalidColumnId, "Column id is invalid"},
  {kErrInvalidRegNo, "Register id is invalid"},
  {kErrInvalidAggNo, "Aggregation id is invalid"},
  {kErrAggNoUsed, "Aggregation id is already used"},
  {kErrEmptyProgram, "Empty program"},
  {kErrAlreadyFinalized, "Already finalized"},
  {kErrTooBigResult, "Single aggregation result could be larger than 8K bytes"},
  {kErrTooBigProgram, "Aggregation program should be less than 4K bytes"},
  {kErrTooManyGroupbyCols, "Number of group by columns should be less than 128"},
  {kErrEmptyAggResult, "Empty aggregation"},
  {kErrTooManyAggResult, "Number of aggregation results should be less than 256"},
  {kErrMaxErrno, ""}
};

class NdbAggregator {
 public:
  class Column {
   public:
    Column(Uint32 id, NdbDictionary::Column::Type type, Uint32 byte_size,
           bool is_null, char* ptr, bool end) :
      id_(id), type_(type), byte_size_(byte_size), is_null_(is_null),
      ptr_(ptr), end_(end) {
      }
    Uint32 id() {
      return id_;
    }
    NdbDictionary::Column::Type type() {
      return type_;
    }
    Uint32 byte_size() {
      return byte_size_;
    }
    Uint32 is_null() {
      return is_null_;
    }
    const char* ptr() {
      return ptr_;
    }
    bool end() {
      return end_;
    }

    Int8 data_int8() {
      return *(Int8*)(ptr_);
    }
    Uint8 data_uint8() {
      return *(Uint8*)(ptr_);
    }
    Int16 data_int16() {
      return *(Int16*)(ptr_);
    }
    Uint16 data_uint16() {
      return *(Uint16*)(ptr_);
    }
    Int32 data_medium();
    Uint32 data_umedium();
    Int32 data_int32() {
      return *(Int32*)(ptr_);
    }
    Uint32 data_uint32() {
      return *(Uint32*)(ptr_);
    }
    Int64 data_int64() {
      return *(Int64*)(ptr_);
    }
    Uint64 data_uint64() {
      return *(Uint64*)(ptr_);
    }
    float data_float() {
      float val;
      memcpy(&val,ptr_,sizeof(val));
      return val;
    }
    float data_double() {
      double val;
      memcpy(&val,ptr_,sizeof(val));
      return val;
    }
    const char* data() {
      return ptr_;
    }

   private:
    Uint32 id_;
    NdbDictionary::Column::Type type_;
    Uint32 byte_size_;
    bool is_null_;
    char* ptr_;
    bool end_;
  };

  class Result {
   public:
    Result(AggResItem* item, bool end) :
      type_(NdbDictionary::Column::Type::Undefined),
      is_null_(true), is_unsigned_(false) {
      data_.val_int64 = 0;
      if (!end) {
        type_ = static_cast<NdbDictionary::Column::Type>(item->type);
        is_null_ = item->is_null;
        data_ = item->value;
        is_unsigned_ = item->is_unsigned;
        if (type_ == NdbDictionary::Column::Bigint && is_unsigned_) {
          type_ = NdbDictionary::Column::Bigunsigned;
        }
      }
      end_ = end;
    }

    NdbDictionary::Column::Type type() {
      return type_;
    }
    bool is_null() {
      return is_null_;
    }
    bool end() {
      return end_;
    }

    Int64 data_int64() {
      return data_.val_int64;
    }
    Uint64 data_uint64() {
      return data_.val_uint64;
    }
    double data_double() {
      return data_.val_double;
    }

   private:
    NdbDictionary::Column::Type type_;
    bool is_null_;
    bool is_unsigned_;
    DataValue data_;
    bool end_;
  };

  class ResultRecord {
   public:
    ResultRecord(const NdbAggregator* aggregator,
        const GBHashEntry group, const GBHashEntry result, bool end) :
      aggregator_(aggregator), group_records_(group), curr_group_pos_(0),
      result_records_(result), curr_result_pos_(0), end_(end) {
    }
    bool end() {
      return end_;
    }
    Column FetchGroupbyColumn();
    Result FetchAggregationResult();
   private:
    const NdbAggregator* aggregator_;
    GBHashEntry group_records_;
    Uint32 curr_group_pos_;
    GBHashEntry result_records_;
    Uint32 curr_result_pos_;
    bool end_;
  };

  NdbAggregator(const NdbDictionary::Table* table);
  ~NdbAggregator();
  const Uint32* buffer() const {
    return &buffer_[0];
  }
  Uint32 instructions_length() const {
    return instructions_length_;
  }
  Uint32 n_gb_cols() const {
    return n_gb_cols_;
  }
  bool finalized() const {
    return finalized_;
  }
  bool finished() const {
    return finished_;
  }
  const AggregationError& GetError() {
    return error_;
  }
  const NdbTableImpl* table_impl() const {
    return table_impl_;
  }
  bool disk_columns() const {
    return disk_columns_;
  }

  Int32 ProcessRes(char* buf);

  bool LoadColumn(const char* name, Uint32 reg_id);
  bool LoadColumn(Int32 col_id, Uint32 reg_id);
  bool LoadUint64(Uint64 value, Uint32 reg_id);
  bool LoadInt64(Int64 value, Uint32 reg_id);
  bool LoadDouble(double value, Uint32 reg_id);
  bool Mov(Uint32 reg_1, Uint32 reg_2);
  bool Add(Uint32 reg_1, Uint32 reg_2);
  bool Minus(Uint32 reg_1, Uint32 reg_2);
  bool Mul(Uint32 reg_1, Uint32 reg_2);
  bool Div(Uint32 reg_1, Uint32 reg_2);
  bool Mod(Uint32 reg_1, Uint32 reg_2);

  bool Sum(Uint32 agg_id, Uint32 reg_id);
  bool Max(Uint32 agg_id, Uint32 reg_id);
  bool Min(Uint32 agg_id, Uint32 reg_id);
  bool Count(Uint32 agg_id, Uint32 reg_id);

  bool GroupBy(const char* name);
  bool GroupBy(Int32 col_id);

  bool Finalize();

  void PrepareResults();
  ResultRecord FetchResultRecord();

  const std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map() {
    return gb_map_;
  }

 private:
  bool TypeSupported(NdbDictionary::Column::Type type);
  const NdbTableImpl* table_impl_;
  Uint32 buffer_[MAX_AGG_PROGRAM_WORD_SIZE];

  Uint32 n_gb_cols_;
  Uint32 n_agg_results_;
  AggResItem* agg_results_;
  Uint32 agg_ops_[MAX_AGG_N_RESULTS];
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_;

  bool finalized_;
  bool finished_;
  Uint32 curr_prog_pos_;
  Uint32 instructions_length_;
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>::iterator iter_;

  AggregationError error_;
  void SetError(Uint32 err_no) {
    error_ = g_errors_[err_no];
  }
  bool CheckRegs(Uint32 reg_1, Uint32 reg_2);
  bool CheckAggAndReg(Uint32 agg_id, Uint32 reg_id);
  bool result_record_fetched_;
  Uint32 result_size_est_;
  bool disk_columns_;
};
#endif  // NDBAGGREGATOR_H_
