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

#ifndef RonSQLCommon_hpp_included
#define RonSQLCommon_hpp_included 1

#include "Ndb.hpp"
#include "NdbOperation.hpp"

#include "ArenaAllocator.hpp"
#include "LexString.hpp"

struct ExecutionParameters
{
  char* sql_buffer = NULL;
  size_t sql_len = 0;
  ArenaAllocator* aalloc;
  Ndb* ndb = NULL;
  enum class ExecutionMode
  {
    ALLOW_BOTH_QUERY_AND_EXPLAIN, // Explain if EXPLAIN keyword is present in
                                  // SQL code, otherwise query.
    ALLOW_QUERY_ONLY,             // Throw an exception if EXPLAIN keyword is
                                  // present in SQL code, otherwise query.
    ALLOW_EXPLAIN_ONLY,           // Explain if EXPLAIN keyword is present in
                                  // SQL code, otherwise throw an exception.
    QUERY_OVERRIDE,               // Query regardless of whether EXPLAIN keyword
                                  // is present in SQL code.
    EXPLAIN_OVERRIDE,             // Explain regardless of whether EXPLAIN
                                  // keyword is present in SQL code.
  };
  ExecutionMode mode = ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN;
  std::basic_ostream<char>* query_output_stream = NULL;
  enum class QueryOutputFormat
  {
    JSON_UTF8,  // Output a JSON representation of the result set. Characters
                // with code point U+0080 and above are encoded as UTF-8.
    JSON_ASCII, // Output a JSON representation of the result set. Characters
                // with code point U+0080 and above are encoded using \u escape
                // sequences, meaning the output stream will only contain ASCII
                // characters 0x00 to 0x7f.
    TSV,        // Mimic mysql tab-separated output with headers
    TSV_DATA,   // Mimic mysql tab-separated output without headers
  };
  QueryOutputFormat query_output_format = QueryOutputFormat::JSON_UTF8;
  std::basic_ostream<char>* explain_output_stream = NULL;
  enum class ExplainOutputFormat
  {
    TEXT,
    JSON_UTF8, // See comment above
  };
  ExplainOutputFormat explain_output_format = ExplainOutputFormat::TEXT;
  std::basic_ostream<char>* err_output_stream = NULL;
};

// Forward declaration used in struct Outputs below
class AggregationAPICompiler_Expr;

// structs for parse tree

struct Outputs
{
  enum class Type
  {
    COLUMN,
    AGGREGATE,
    AVG,
  };
  Type type;
  LexString output_name;
  union
  {
    struct
    {
      uint col_idx;
    } column;
    struct
    {
      int fun;
      AggregationAPICompiler_Expr* arg;
      uint agg_index;
    } aggregate;
    struct
    {
      AggregationAPICompiler_Expr* arg;
      uint agg_index_sum;
      uint agg_index_count;
    } avg;
  };
  struct Outputs* next;
};

struct ConditionalExpression
{
  int op;
  union
  {
    struct
    {
      struct ConditionalExpression* left;
      struct ConditionalExpression* right;
    } args;
    uint col_idx;
    long int constant_integer;
    struct
    {
      struct ConditionalExpression* arg;
      bool null;
    } is;
    struct
    {
      struct ConditionalExpression* arg;
      int interval_type;
    } interval;
    struct
    {
      int interval_type;
      struct ConditionalExpression* arg;
    } extract;
    LexString string;
  };
};

struct GroupbyColumns
{
  uint col_idx;
  struct GroupbyColumns* next;
};

struct OrderbyColumns
{
  uint col_idx;
  bool ascending;
  struct OrderbyColumns* next;
};

struct SelectStatement
{
  bool do_explain = false;
  Outputs* outputs = NULL;
  LexCString table = LexCString{ NULL, 0};
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
};

#endif
