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
#include "NdbRecAttr.hpp"
#include <map>
#include <queue>

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
  kErrUnsupportedStringOperation,
  kErrUnsupportedTemporalOperation,
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
  {kErrUnsupportedStringOperation, "String columns are only supported for MIN/MAX"},
  {kErrUnsupportedTemporalOperation, "Temporal (DATE/YEAR/DATETIME/TIME) columns are only supported for MIN/MAX/COUNT"},
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
    double data_double() {
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

    // Phase I.6 (F.2-K.5d-3): payload of a string MIN/MAX result.
    // Decodes the local val_ptr buffer set up by NdbAggregator's
    // resolveStringSlots: `[Uint16 payload_len][Uint16 capacity]
    // [prefix_bytes + payload]`.  Returns a pointer to the payload
    // bytes (past the wire-format prefix) and writes the payload
    // length to *payload_len.  Valid only when type() is one of
    // Char / Varchar / Longvarchar AND is_null() is false.
    const char* data_str(Uint32* payload_len) {
      const char* buf = static_cast<const char*>(data_.val_ptr);
      const Uint16 plen = *reinterpret_cast<const Uint16*>(buf);
      const Uint32 prefix =
          (type_ == NdbDictionary::Column::Char)        ? 0
        : (type_ == NdbDictionary::Column::Varchar)     ? 1
        : 2;  // Longvarchar
      if (payload_len != nullptr) {
        *payload_len = plen;
      }
      return buf + 4 + prefix;
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
      curr_gb_col_index_(0),
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
    Uint32 curr_gb_col_index_;
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
  // True if any LoadColumn encoded a column type > 31 (DATETIME2 /
  // TIMESTAMP2), which sets bit 20 in the kOpLoadCol instruction.  Such a
  // program must only be sent to data nodes that decode the 6-bit type field
  // (ndbd_support_agg_wide_type); the scan-send path checks this.
  bool uses_wide_type() const {
    return uses_wide_type_;
  }

  /**
   * Single-row CTE projection mode (cte_single_row_kernel_plan.md):
   * the program declares every projected column as a GROUP BY column
   * and carries ZERO aggregate slots — the materialized "group" IS the
   * row.  Finalize() then accepts n_agg_results == 0 (normally
   * kErrEmptyAggResult).  Only meaningful for a program passed to
   * NdbQueryBuilder::defineCte() together with the CTE_SINGLE_ROW
   * flag; the kernel stores the row as a key-only group record.
   */
  void SetSingleRowMode() {
    single_row_mode_ = true;
  }
  bool single_row_mode() const {
    return single_row_mode_;
  }

  /**
   * Initialize this aggregator for receiving results, given a program buffer.
   * Reads the program header to set n_gb_cols, n_agg_results, and allocates
   * the gb_map if needed. Must be called before ProcessRes() when the
   * aggregator was not built with GroupBy/Sum/etc. calls.
   *
   * @param gbColumns  Optional array of NdbDictionary::Column pointers for
   *                   each GROUP BY column (local and linked).  Provides full
   *                   column metadata (type, precision, charset, etc.) to
   *                   FetchGroupbyColumn().  May be nullptr for backward
   *                   compatibility (falls back to leaf table lookup).
   * @param nGbColumns Number of entries in gbColumns array.
   */
  void initForResults(const Uint32 *programBuffer, Uint32 programLen,
                      const NdbDictionary::Column *const *gbColumns = nullptr,
                      Uint32 nGbColumns = 0,
                      const NdbDictionary::Column *const *aggColumns = nullptr,
                      Uint32 nAggColumns = 0);

  const NdbDictionary::Column *const *gb_columns() const {
    return gb_columns_;
  }
  const NdbDictionary::Column *const *agg_columns() const {
    return agg_columns_;
  }

  Int32 ProcessRes(char* buf);

  bool LoadColumn(const char* name, Uint32 reg_id);
  bool LoadColumn(Int32 col_id, Uint32 reg_id);
  bool LoadLinkedColumn(Uint32 position, Uint32 reg_id,
                        const NdbDictionary::Column *col);
  bool LoadUint64(Uint64 value, Uint32 reg_id);
  bool LoadInt64(Int64 value, Uint32 reg_id);
  bool LoadDouble(double value, Uint32 reg_id);
  bool Mov(Uint32 reg_1, Uint32 reg_2);
  bool Add(Uint32 reg_1, Uint32 reg_2);
  bool Minus(Uint32 reg_1, Uint32 reg_2);
  bool Mul(Uint32 reg_1, Uint32 reg_2);
  bool Div(Uint32 reg_1, Uint32 reg_2);
  bool DivInt(Uint32 reg_1, Uint32 reg_2);
  bool Mod(Uint32 reg_1, Uint32 reg_2);

  bool Sum(Uint32 agg_id, Uint32 reg_id);
  bool Max(Uint32 agg_id, Uint32 reg_id);
  bool Min(Uint32 agg_id, Uint32 reg_id);
  bool Count(Uint32 agg_id, Uint32 reg_id);
  /* AVG(x): one visible DOUBLE result slot; the kernel carries a
   * hidden SUM/COUNT pair through merge + CTE redistribute and divides
   * on the owner at CTE_READY (count == 0 => NULL).  CTE aggregators
   * only in v1 — see cte_avg_plan.md. */
  bool Avg(Uint32 agg_id, Uint32 reg_id);
  /* ORDER BY / LIMIT trailer for CTE aggregation programs: the owner
   * node selects the top-Limit(n) groups under the OrderBy spec after
   * the CTE redistribute (cte_orderby_limit_plan.md).  Declarative —
   * no per-row cost. */
  bool OrderBy(Uint32 idx, bool is_agg_result, bool descending);
  bool Limit(Uint32 n);

  bool GroupBy(const char* name);
  bool GroupBy(Int32 col_id);
  bool GroupByLinked(Uint32 position, const NdbDictionary::Column *parentCol);

  // Embedded interpreter support for CASE expressions
  bool EmbeddedInterp(Uint32 embedded_length);
  bool EmitEmbeddedWord(Uint32 word);
  bool Skip(Uint32 skip_count);
  bool SetRegNull(Uint32 reg_id);
  bool RepeatAgg(Uint32 agg_id, Uint32 reg_id);

  bool Finalize();

  void PrepareResults();
  ResultRecord FetchResultRecord();

  const std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map() {
    return gb_map_;
  }

  class VectorSearchResult {
   #define MAX_N 20
   public:
    VectorSearchResult(double distance, int n_columns, NdbRecAttr** src)
      : distance_(distance), n_columns_(n_columns) {
      for (int i = 0; i < MAX_N; i++) {
        attrs_[i] = nullptr;
        if (i < n_columns) {
          attrs_[i] = (src[i])->clone();
        }
      }
    }
    ~VectorSearchResult() {
      for (int i = 0; i < MAX_N; i++) {
        if (attrs_[i] != nullptr) {
          delete attrs_[i];
          attrs_[i] = nullptr;
        }
      }
    }
    double distance_;
    int n_columns_;
    NdbRecAttr* attrs_[MAX_N];
  };

  struct ByDistance {
    bool operator()(const VectorSearchResult* lhs, const VectorSearchResult* rhs) {
      return lhs->distance_ < rhs->distance_ ? true : false;
    }
  };


  bool VectorSearch(const char* name, const float* vec, Uint32 dims,
                    Uint32 top_n);
  bool VecProcessRes(NdbRecAttr** userAttrs, Uint32 n_userAttrs,
                     NdbRecAttr* vecDistanceAttr);
  bool VecPrepareResults(NdbRecAttr** userAttrs, Uint32 n_userAttrs);
  bool VecFetchNextResult();

  enum Type {
    kAggregation = 0,
    kVectorSearch
  };
  Type type() {
    return type_;
  }

 private:
  bool TypeSupported(NdbDictionary::Column::Type type);
  bool isStringType(Uint32 type) const;
  bool isTemporalType(Uint32 type) const;
  void clearStringSlot(AggResItem *slot) const;
  void assignStringSlot(AggResItem *dst, const AggResItem *src) const;
  int compareStringSlots(const AggResItem *lhs,
                         const AggResItem *rhs,
                         Uint32 agg_id) const;
  void mergeStringSlot(AggResItem *dst,
                       const AggResItem *src,
                       Uint32 agg_id);
  const NdbTableImpl* table_impl_;
  Uint32 buffer_[MAX_VEC_SEARCH_PROGRAM_WORD_SIZE];

  /* Column definitions for each GROUP BY column.  For local columns this
   * points into the child table's dictionary entry; for linked columns
   * (from a parent/grandparent) it points into that ancestor table's
   * dictionary entry.  Set during GroupBy()/GroupByLinked() on the build
   * side and transferred through NdbQueryOptions to the result-side
   * aggregator via initForResults(). */
  const NdbDictionary::Column *gb_columns_[MAX_AGG_N_GROUPBY_COLS];
  const NdbDictionary::Column *reg_columns_[kRegTotal];
  const NdbDictionary::Column *agg_columns_[MAX_AGG_N_RESULTS];
  Uint32 reg_types_[kRegTotal];

  Uint32 n_gb_cols_;
  Uint32 gb_col_ids_[MAX_AGG_N_GROUPBY_COLS];
  GBCmpContext gb_cmp_ctx_;
  Uint32 n_agg_results_;
  AggResItem* agg_results_;
  Uint32 agg_ops_[MAX_AGG_N_RESULTS];
  std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>* gb_map_;

  // Phase I.6 (F.2-K.5d): for AGG_CHAR_RESULT wire results, walk the
  // appended string-payload region and replace each string slot's
  // val_ptr (zero on the wire) with a freshly-allocated local buffer
  // mirroring the kernel layout `[Uint16 payload_len][Uint16
  // capacity][prefix+payload]`.  The buffer is owned by the
  // aggregator and freed by freeStringSlots when the slot's group
  // is released (destructor).
  static void resolveStringSlots(AggResItem* slots, Uint32 n_slots,
                                  const char* appended_region);

  // Phase I.6 (F.2-K.5d): release string val_ptr buffers attached to
  // a slot array (scalar agg_results_ or one group's AggResItem
  // array within an agg_rec block).  Cheap no-op for non-string
  // slots and null string slots.
  static void freeStringSlots(AggResItem* slots, Uint32 n_slots);

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
  bool uses_wide_type_;   // any LoadColumn type > 31 (DATETIME2/TIMESTAMP2)
  bool single_row_mode_;  // single-row CTE projection program: GROUP BY
                          // columns only, zero aggregate slots allowed

  // Vector Search
  Uint32 vec_top_n_;
  std::priority_queue<VectorSearchResult*,
    std::vector<VectorSearchResult*>,
    ByDistance>* vec_result_;

  std::vector<VectorSearchResult*> vec_result_final_;
  NdbRecAttr** userAttrs_;
  Uint32 n_userAttrs_;
  bool results_prepared_;
  Uint32 results_left_;
  Type type_;
};
#endif  // NDBAGGREGATOR_H_
