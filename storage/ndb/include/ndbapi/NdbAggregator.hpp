/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
 *
 * Author: Zhao Song
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
  uint32_t errno_;
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
    Column(uint32_t id, NdbDictionary::Column::Type type, uint32_t byte_size,
           bool is_null, char* ptr, bool end) :
      id_(id), type_(type), byte_size_(byte_size), is_null_(is_null),
      ptr_(ptr), end_(end) {
      }
    uint32_t id() {
      return id_;
    }
    NdbDictionary::Column::Type type() {
      return type_;
    }
    uint32_t byte_size() {
      return byte_size_;
    }
    uint32_t is_null() {
      return is_null_;
    }
    const char* ptr() {
      return ptr_;
    }
    bool end() {
      return end_;
    }

    int8_t data_int8() {
      return *(int8_t*)(ptr_);
    }
    uint8_t data_uint8() {
      return *(uint8_t*)(ptr_);
    }
    int16_t data_int16() {
      return *(int16_t*)(ptr_);
    }
    uint16_t data_uint16() {
      return *(uint16_t*)(ptr_);
    }
		int32_t data_medium();
		uint32_t data_umedium();
    int32_t data_int32() {
      return *(int32_t*)(ptr_);
    }
    uint32_t data_uint32() {
      return *(uint32_t*)(ptr_);
    }
    int64_t data_int64() {
      return *(int64_t*)(ptr_);
    }
    uint64_t data_uint64() {
      return *(uint64_t*)(ptr_);
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
    uint32_t id_;
    NdbDictionary::Column::Type type_;
    uint32_t byte_size_;
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

    int64_t data_int64() {
      return data_.val_int64;
    }
    uint64_t data_uint64() {
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
    uint32_t curr_group_pos_;
    GBHashEntry result_records_;
    uint32_t curr_result_pos_;
    bool end_;
  };

  NdbAggregator(const NdbDictionary::Table* table);
  ~NdbAggregator();
  const uint32_t* buffer() const {
    return &buffer_[0];
  }
  uint32_t instructions_length() const {
    return instructions_length_;
  }
  uint32_t n_gb_cols() const {
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

  int32_t ProcessRes(char* buf);

  bool LoadColumn(const char* name, uint32_t reg_id);
  bool LoadColumn(int32_t col_id, uint32_t reg_id);
  bool LoadUint64(uint64_t value, uint32_t reg_id);
  bool LoadInt64(int64_t value, uint32_t reg_id);
  bool LoadDouble(double value, uint32_t reg_id);
  bool Add(uint32_t reg_1, uint32_t reg_2);
  bool Minus(uint32_t reg_1, uint32_t reg_2);
  bool Mul(uint32_t reg_1, uint32_t reg_2);
  bool Div(uint32_t reg_1, uint32_t reg_2);
  bool Mod(uint32_t reg_1, uint32_t reg_2);

  bool Sum(uint32_t agg_id, uint32_t reg_id);
  bool Max(uint32_t agg_id, uint32_t reg_id);
  bool Min(uint32_t agg_id, uint32_t reg_id);
  bool Count(uint32_t agg_id, uint32_t reg_id);

  bool GroupBy(const char* name);
  bool GroupBy(int32_t col_id);

  bool Finalize();

  void PrepareResults();
  ResultRecord FetchResultRecord();

  const std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map() {
    return gb_map_;
  }

 private:
  bool TypeSupported(NdbDictionary::Column::Type type);
  const NdbTableImpl* table_impl_;
  uint32_t buffer_[MAX_AGG_PROGRAM_WORD_SIZE];

  uint32_t n_gb_cols_;
  uint32_t n_agg_results_;
  AggResItem* agg_results_;
  uint32_t agg_ops_[MAX_AGG_N_RESULTS];
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_;

  bool finalized_;
  bool finished_;
  uint32_t curr_prog_pos_;
  uint32_t instructions_length_;
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>::iterator iter_;

  AggregationError error_;
  void SetError(uint32_t errno) {
    error_ = g_errors_[errno];
  }
  bool CheckRegs(uint32_t reg_1, uint32_t reg_2);
  bool CheckAggAndReg(uint32_t agg_id, uint32_t reg_id);
  bool result_record_fetched_;
  uint32_t result_size_est_;
  bool disk_columns_;
};
#endif  // NDBAGGREGATOR_H_
