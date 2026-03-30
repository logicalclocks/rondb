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

class ResultPrinter
{
private:

  // Configuration provided to constructor
  ArenaMalloc* m_amalloc;
  struct SelectStatement* m_query;
  DynamicArray<LexCString>* m_column_names;
  const NdbDictionary::Column** m_column_map;
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
      } print_group_by_column;
      struct
      {
        Uint32 reg_a;
      } print_aggregate;
      struct
      {
        Uint32 reg_a_sum;
        Uint32 reg_a_count;
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
  void print_float_or_double(std::ostream& out, double value);
  bool evaluate_having(const ConditionalExpression* expr);
  double evaluate_having_value(const ConditionalExpression* expr);
  void scan_having_max_agg(const ConditionalExpression* expr,
                           Uint32& max_idx);
public:
  ResultPrinter(ArenaMalloc* amalloc,
                struct SelectStatement* query,
                DynamicArray<LexCString>* column_names,
                const NdbDictionary::Column** column_map,
                RonSQLExecParams::OutputFormat output_format,
                std::basic_ostream<char>* err);
  void print_result(NdbAggregator* aggregator,
                    std::basic_ostream<char>* out_stream);
  void explain(std::basic_ostream<char>* out_stream);
};

#endif
