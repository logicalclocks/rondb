/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef STORAGE_NDB_SRC_RONSQL_RESULTPRINTER_HPP
#define STORAGE_NDB_SRC_RONSQL_RESULTPRINTER_HPP 1

#include "NdbAggregator.hpp"
#include <NdbSqlUtil.hpp>

#include "ArenaMalloc.hpp"
#include "DynamicArray.hpp"
#include "LexString.hpp"
#include "RonSQLCommon.hpp"

class NdbRecAttr;

/*
 * One pass-through value copied out of its NdbRecAttr into the statement's
 * arena.  The client-side ORDER BY keeps every row this way (the
 * NdbRecAttr buffers are overwritten by the next fetch) instead of
 * cloning each NdbRecAttr on the heap, one or two allocations per value.
 * It answers the part of the NdbRecAttr interface that the pass-through
 * printer, the sort and the API-side aggregation read, with the same
 * results, so their code reads either type.  The value bytes follow the
 * object, 8-byte aligned and zero-padded to at least 8 bytes.
 */
class PassthroughValue
{
public:
  // A copy of src in amalloc.  Throws as NdbRecAttr::clone() fails when
  // src holds no value (isNULL() == -1).
  static const PassthroughValue* copy(const NdbRecAttr* src,
                                      ArenaMalloc* amalloc);
  const NdbDictionary::Column* getColumn() const { return m_column; }
  NdbDictionary::Column::Type getType() const { return m_column->getType(); }
  int isNULL() const { return m_size == 0 ? 1 : 0; }
  Uint32 get_size_in_bytes() const { return m_size; }
  char* aRef() const
  {
    return const_cast<char*>(reinterpret_cast<const char*>(this + 1));
  }
  Int8 int8_value() const { return get<Int8>(); }
  Uint8 u_8_value() const { return get<Uint8>(); }
  short short_value() const { return get<short>(); }
  Uint16 u_short_value() const { return get<Uint16>(); }
  Int32 medium_value() const;
  Uint32 u_medium_value() const;
  Int32 int32_value() const { return get<Int32>(); }
  Uint32 u_32_value() const { return get<Uint32>(); }
  Int64 int64_value() const { return get<Int64>(); }
  Uint64 u_64_value() const { return get<Uint64>(); }
  float float_value() const { return get<float>(); }
  double double_value() const { return get<double>(); }
private:
  template <typename T> T get() const
  {
    T v;
    memcpy(&v, aRef(), sizeof(v));
    return v;
  }
  const NdbDictionary::Column* m_column;
  Uint32 m_size;
};
static_assert(sizeof(PassthroughValue) % 8 == 0,
              "PassthroughValue data must stay 8-byte aligned");

class ResultPrinter
{
public:
  // D17 + temporal extension: a MIN/MAX over a temporal column is computed
  // in the kernel as an unsigned integer (the column's native packed bytes)
  // and returned as Bigunsigned.  This tells the printer which temporal
  // decode to apply to that Bigunsigned value.  NONE = not temporal.
  enum class TemporalDisplay : int {
    NONE = 0,
    DATE,        // 3-byte little-endian uint3korr → YYYY-MM-DD
    YEAR,        // 1 byte → YYYY (v+1900, 0→0000)
    DATETIME2,   // big-endian 5+flen bytes → YYYY-MM-DD HH:MM:SS[.frac]
    TIME2,       // big-endian 3+flen bytes → [-]HH:MM:SS[.frac]
    TIMESTAMP2   // big-endian 4+flen bytes (UTC epoch) → YYYY-MM-DD HH:MM:SS[.frac]
  };
  struct ColumnMetadata {
    CHARSET_INFO* charset;
    int precision;
    int scale;
    int avg_scale;  // AVG fractional digits; -1 selects compact double output
    bool float_display;  // FLOAT value, even when a CTE carries it as DOUBLE
    bool has_metadata;
    TemporalDisplay temporal;
    int temporal_fsp;   // fractional-seconds precision (DATETIME2 / TIME2)
  };

private:

  // Configuration provided to constructor
  ArenaMalloc* m_amalloc;
  struct SelectStatement* m_query;
  DynamicArray<LexCString>* m_column_names;
  const ColumnMetadata* m_column_metadata;
  RonSQLExecParams::OutputFormat m_output_format;
  std::basic_ostream<char>* m_err;

  // Program
  struct Cmd
  {
    enum class Type
    {
      STORE_GROUP_BY_COLUMN,
      END_OF_GROUP_BY_COLUMNS,
      STORE_AGGREGATE,
      END_OF_AGGREGATES,
      PRINT_GROUP_BY_COLUMN,
      PRINT_AGGREGATE,
      PRINT_AVG,
      PRINT_STR,
      PRINT_STR_JSON,
    };
    Type type;
    union
    {
      struct
      {
        Uint32 reg_g;
        Uint32 group_by_idx; // Only used for assertions
      } store_group_by_column;
      struct
      {
        Uint32 reg_a;
        Uint32 agg_index; // Only used for assertions
      } store_aggregate;
      struct
      {
        Uint32 reg_g;
        CHARSET_INFO* charset;
        int precision;
        int scale;
        bool float_display;
      } print_group_by_column;
      struct
      {
        Uint32 reg_a;
        CHARSET_INFO* charset;
        int scale;   // source DECIMAL scale for MIN/MAX (0 = none/compact)
        bool float_display;
        TemporalDisplay temporal; // D17+: temporal decode for the result
        int temporal_fsp;         // fractional-seconds precision (DATETIME2/TIME2)
      } print_aggregate;
      struct
      {
        Uint32 reg_a_sum;
        Uint32 reg_a_count;
        int scale;
      } print_avg;
      struct
      {
        LexString content;
      } print_str;
    };
  };
  typedef DynamicArray<Cmd> Program;
  Program m_program;

  DynamicArray<uint> m_groupby_cols;
  DynamicArray<Outputs*> m_outputs;
  DynamicArray<uint> m_col_idx_groupby_map;
  bool m_json_output;
  bool m_utf8_output;
  bool m_tsv_output;
  bool m_tsv_headers;
  const char* m_quote;
  LexString m_null_representation;
  // RONDB-1135: AVRO(column) outputs of a pass-through query.  When set,
  // m_avro_handles has one entry per output position: the decoder's
  // handle for an AVRO() output, NULL for any other output.
  RonSQLAvroDecoder* m_avro_decoder = NULL;
  const void* const* m_avro_handles = NULL;
  // Program state
  NdbAggregator::Column* m_regs_g;
  NdbAggregator::Result* m_regs_a;
  Uint32 m_print_start_idx;
  Uint32 m_num_groupby_cols;
  Uint32 m_num_aggregates;

  // ORDER BY support
  struct OrderbySpec {
    enum class Kind { GROUPBY_COL, AGGREGATE };
    Kind kind;
    union {
      Uint32 groupby_idx;    // GROUPBY_COL: index into StoredRow.cols
      Uint32 agg_result_idx; // AGGREGATE: index into StoredRow.results
    };
    bool ascending;
    CHARSET_INFO* charset; // for string types, NULL otherwise
  };
  struct StoredRow {
    NdbAggregator::Column* cols;    // array of m_num_groupby_cols Column objects
    NdbAggregator::Result* results; // array of m_num_aggregates Result objects
  };
  DynamicArray<OrderbySpec> m_orderby_specs;
  bool m_has_orderby;
  // compare_rows' scratch for length-prefixed VARCHAR keys: two arena
  // buffers of m_cmp_buf_len bytes, grown when a longer key comes.
  char* m_cmp_buf_a = NULL;
  char* m_cmp_buf_b = NULL;
  Uint32 m_cmp_buf_len = 0;

  void validate_orderby_columns();
  void compile();
  void optimize();
  void print_record(NdbAggregator::ResultRecord& record,
                    std::ostream& out);
  void run_program(Uint32 from, Uint32 to,
                   NdbAggregator::ResultRecord* record,
                   std::ostream* out);
  StoredRow store_record(NdbAggregator::ResultRecord& record);
  void print_stored_record(StoredRow& row, std::ostream& out);
  int compare_rows(StoredRow& a, StoredRow& b);
  void print_result_ordered(NdbAggregator* aggregator,
                            std::basic_ostream<char>* out_stream);
  void print_float_or_double(std::ostream& out, double value,
                              bool float_display = false);
  void print_avg_result(std::ostream& out,
                        NdbAggregator::Result sum,
                        NdbAggregator::Result count,
                        int scale);
  int avg_arg_scale(const Outputs* out) const;
  void print_aggregate_result(std::ostream& out,
                              NdbAggregator::Result result,
                              CHARSET_INFO* charset,
                              int scale,
                              bool float_display,
                              TemporalDisplay temporal,
                              int temporal_fsp);
  bool aggregate_uses_float_display(const Outputs* out) const;
  CHARSET_INFO* aggregate_arg_charset(const Outputs* out) const;
  int aggregate_arg_scale(const Outputs* out) const;
  int aggregate_arg_precision(const Outputs* out) const;
  // Returns the temporal decode for a MIN/MAX aggregate's source column
  // (NONE if not temporal); sets fsp to the source column's fractional
  // precision for DATETIME2 / TIME2.
  TemporalDisplay aggregate_arg_temporal(const Outputs* out,
                                         int& fsp) const;
  bool evaluate_having(const ConditionalExpression* expr);
  double evaluate_having_value(const ConditionalExpression* expr);
  void scan_having_max_agg(const ConditionalExpression* expr,
                           Uint32& max_idx);
public:
  ResultPrinter(ArenaMalloc* amalloc,
                struct SelectStatement* query,
                DynamicArray<LexCString>* column_names,
                const ColumnMetadata* column_metadata,
                RonSQLExecParams::OutputFormat output_format,
                std::basic_ostream<char>* err);
  // Phase E.3 pass-through constructor for projection-only main SELECTs
  // over a CTE_SCAN root.  Skips compile()/optimize() (which require
  // every COLUMN output to be in GROUP BY); only sets up the output-format
  // helpers used by print_passthrough_*.  Phase 0b: takes the same
  // col_idx-indexed ColumnMetadata as the aggregate constructor so
  // temporal / DECIMAL outputs format correctly.
  ResultPrinter(ArenaMalloc* amalloc,
                struct SelectStatement* query,
                DynamicArray<LexCString>* column_names,
                const ColumnMetadata* column_metadata,
                RonSQLExecParams::OutputFormat output_format,
                std::basic_ostream<char>* err,
                bool /*passthrough_marker*/);
  void print_result(NdbAggregator* aggregator,
                    std::basic_ostream<char>* out_stream);
  // COUNT(*) fragment-stats shortcut
  // (ronsql_count_star_shortcut_plan.md W1): print the scalar result
  // without an NdbAggregator — every output is COUNT(*), so each
  // prints the same summed per-fragment ROW_COUNT.  Format fidelity
  // mirrors the compiled print program (JSON record {"name":value,..}
  // inside [...]; TSV header + row; LIMIT 0 suppression).
  void print_count_star_result(Uint64 count,
                               std::basic_ostream<char>* out_stream);
  // The pass-through printers read a row as an array of NdbRecAttr (as
  // delivered) or of PassthroughValue (as buffered for a client-side
  // ORDER BY); both are instantiated in ResultPrinter.cpp.  A NULL entry
  // is SQL NULL.
  template <class Attr>
  void print_passthrough_header(const Attr* const* attrs,
                                Uint32 num_cols,
                                std::basic_ostream<char>* out_stream);
  template <class Attr>
  void print_passthrough_row(const Attr* const* attrs,
                             Uint32 num_cols,
                             bool is_first_row,
                             std::basic_ostream<char>* out_stream);
  void print_passthrough_finish(std::basic_ostream<char>* out_stream);
  // RONDB-1135: print the AVRO() outputs of the pass-through rows through
  // `decoder`; `handles` is indexed by output position (see m_avro_handles).
  void set_avro_decoder(RonSQLAvroDecoder* decoder,
                        const void* const* handles);
  void explain(std::basic_ostream<char>* out_stream);
private:
  void setup_output_format();
  // Phase 0b: `meta` (may be NULL) carries the resolved source column's
  // metadata — needed to decode CTE MIN/MAX temporal outputs, which
  // arrive as Bigunsigned virt columns holding the packed value.
  template <class Attr>
  void print_passthrough_value(std::ostream& out,
                               const Attr* attr,
                               const ColumnMetadata* meta);
  // One pass-through output value: an AVRO() output when output position
  // `pos` has a decoder handle, otherwise print_passthrough_value.
  template <class Attr>
  void print_passthrough_output(std::ostream& out,
                                const Attr* attr,
                                const struct Outputs* o,
                                Uint32 pos);
  template <class Attr>
  void print_avro_value(std::ostream& out,
                        const Attr* attr,
                        const void* avro_handle);
  // col_idx-indexed metadata lookup for one pass-through output; NULL
  // when no metadata is available or the output is not a plain column.
  const ColumnMetadata* passthrough_column_metadata(
      const struct Outputs* o) const;
};

// Instantiated in ResultPrinter.cpp.
extern template void ResultPrinter::print_passthrough_header<NdbRecAttr>(
    const NdbRecAttr* const*, Uint32, std::basic_ostream<char>*);
extern template void ResultPrinter::print_passthrough_header<PassthroughValue>(
    const PassthroughValue* const*, Uint32, std::basic_ostream<char>*);
extern template void ResultPrinter::print_passthrough_row<NdbRecAttr>(
    const NdbRecAttr* const*, Uint32, bool, std::basic_ostream<char>*);
extern template void ResultPrinter::print_passthrough_row<PassthroughValue>(
    const PassthroughValue* const*, Uint32, bool, std::basic_ostream<char>*);

#endif
