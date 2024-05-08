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

#ifndef ResultPrinter_hpp_included
#define ResultPrinter_hpp_included 1

#include "NdbAggregator.hpp"

#include "ArenaAllocator.hpp"
#include "DynamicArray.hpp"
#include "LexString.hpp"
#include "RonSQLCommon.hpp"

class ResultPrinter
{
private:

  // Configuration provided to constructor
  ArenaAllocator* m_aalloc;
  struct SelectStatement* m_query;
  DynamicArray<LexCString>* m_column_names;
  ExecutionParameters::QueryOutputFormat m_output_format;
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
        uint reg_g;
        uint group_by_idx; // Only used for assertions
      } store_group_by_column;
      struct
      {
        uint reg_a;
        uint agg_index; // Only used for assertions
      } store_aggregate;
      struct
      {
        uint reg_g;
      } print_group_by_column;
      struct
      {
        uint reg_a;
      } print_aggregate;
      struct
      {
        uint reg_a_sum;
        uint reg_a_count;
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
  // Program state
  NdbAggregator::Column* m_regs_g;
  NdbAggregator::Result* m_regs_a;

  void compile();
  void optimize();
  void print_record(NdbAggregator::ResultRecord& record,
                    std::ostream& out);
public:
  ResultPrinter(ArenaAllocator* aalloc,
                struct SelectStatement* query,
                DynamicArray<LexCString>* column_names,
                ExecutionParameters::QueryOutputFormat output_format,
                std::basic_ostream<char>* err);
  void print_result(NdbAggregator* aggregator,
                    std::basic_ostream<char>* query_output_stream);
  void explain(std::basic_ostream<char>* explain_output_stream);
};

#endif
