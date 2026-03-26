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

#ifndef STORAGE_NDB_SRC_RONSQL_RONSQLCOMMON_HPP
#define STORAGE_NDB_SRC_RONSQL_RONSQLCOMMON_HPP 1

#include "Ndb.hpp"
#include "NdbOperation.hpp"
#include "mysql_time.h"
#include "storage/ndb/plugin/ndb_require.h"
using ndbcluster::ndbrequire;

#include "ArenaMalloc.hpp"
#include "LexString.hpp"

#define ARRAY_LEN(x) (sizeof(x) / sizeof(x[0]))

typedef Int32 TokenKind; /* The type of the T_* values used to indicate token
                          * type. These values are also used in the parse tree.
                          */

struct RonSQLExecParams
{
  char* sql_buffer = NULL;
  size_t sql_len = 0;
  ArenaMalloc* amalloc = NULL;
  Ndb* ndb = NULL;
  enum class ExplainMode
  {
             // SELECT causes: | EXPLAIN SELECT causes: | Ndb connection required:
    ALLOW,   // SELECT         | EXPLAIN SELECT         | Yes
    FORBID,  // SELECT         | Exception              | Yes
    REQUIRE, // Exception      | EXPLAIN SELECT         | No
    REMOVE,  // SELECT         | SELECT                 | Yes
    FORCE,   // EXPLAIN SELECT | EXPLAIN SELECT         | No
  };
  ExplainMode explain_mode = ExplainMode::ALLOW;
  std::basic_ostream<char>* out_stream = NULL;
  enum class OutputFormat
  {
    JSON,          // Output a JSON representation of the result set or EXPLAIN
                   // output. Characters with code point U+00a0 and above are
                   // encoded as UTF-8.
    JSON_ASCII,    // Output a JSON representation of the result set or EXPLAIN
                   // output. Characters with code point U+00a0 and above are
                   // encoded using \u escape sequences, meaning the output stream
                   // will only contain ASCII characters 0x0a, 0x20 -- 0x7e.
    TEXT,          // For query output, mimic mysql tab-separated output with
                   // headers. For EXPLAIN output, use a plain text format.
    TEXT_NOHEADER, // Same as TEXT, except suppress the header line for query
                   // output.
  };
  OutputFormat output_format = OutputFormat::JSON;
  std::basic_ostream<char>* err_stream = NULL;
  const char* operation_id = NULL; // Only used with RDRS
  bool* do_explain = NULL; // If not NULL, use this to inform the caller whether
                           // we EXPLAIN. This is needed by RDRS to determine
                           // content type.
  static const Uint32 ARENA_MALLOC_PAGE_SIZE = 2048;
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
    SUBQUERY_AGG,
  };
  Type type;
  LexString output_name;
  union
  {
    struct
    {
      Uint32 col_idx;
    } column;
    struct
    {
      TokenKind fun;
      AggregationAPICompiler_Expr* arg;
      Uint32 agg_index;
    } aggregate;
    struct
    {
      AggregationAPICompiler_Expr* arg;
      Uint32 agg_index_sum;
      Uint32 agg_index_count;
    } avg;
    struct
    {
      struct SelectStatement* stmt;  // parsed inner SELECT
      Uint32 agg_index;             // assigned during compilation
    } subquery_agg;
  };
  struct Outputs* next;
};

struct SelectStatement;  // forward declaration for subquery support

struct ConditionalExpression
{
  TokenKind op;
  union
  {
    struct
    {
      struct ConditionalExpression* left;
      struct ConditionalExpression* right;
    } args;
    Uint32 col_idx;
    Int64 constant_integer;
    struct
    {
      double dbl;
      LexString ls;
    } constant_float;
    struct
    {
      struct ConditionalExpression* arg;
      bool null;
    } is;
    struct
    {
      struct ConditionalExpression* arg;
      TokenKind interval_type;
    } interval;
    struct
    {
      TokenKind interval_type;
      struct ConditionalExpression* arg;
    } extract;
    LexString string;
    MYSQL_TIME mysql_time;
    struct
    {
      Uint32 agg_index;   // register in m_regs_a (SUM, COUNT, MIN, MAX, or AVG sum)
      Uint32 agg_index2;  // only for AVG: the count register
    } having_agg;
    struct
    {
      SelectStatement *stmt;  // op == T_EXISTS or I_SUBQUERY
    } subquery;
    struct
    {
      struct ConditionalExpression *expr;
      SelectStatement *stmt;  // op == I_IN_SUBQUERY
    } in_subquery;
    struct
    {
      TokenKind cmp_op;                        // Original comparison operator
      struct ConditionalExpression* cmp_expr;   // Outer column being compared
      struct ConditionalExpression* key_expr;   // Outer correlation key column
      SelectStatement* stmt;                    // Rewritten inner query
    } corr_scalar;
  };
};

struct GroupbyColumns
{
  Uint32 col_idx;
  struct GroupbyColumns* next;
};

struct OrderbyColumns
{
  Uint32 col_idx;
  bool ascending;
  struct OrderbyColumns* next;
};

struct TableRef
{
  LexCString database;     /* database name, or {NULL, 0} if unqualified */
  LexCString name;         /* table name */
  LexCString alias;        /* alias, or same as name if no alias */
};

struct JoinCondition
{
  LexCString child_table;  /* child table alias/name */
  LexCString child_column; /* child column name */
  LexCString parent_table; /* parent table alias/name */
  LexCString parent_column;/* parent column name */
  struct JoinCondition *next;  /* for multi-column ON conditions */
};

struct JoinClause
{
  enum JoinType { INNER_JOIN, LEFT_OUTER_JOIN };
  JoinType join_type;
  TableRef table;
  JoinCondition *conditions;  /* linked list of ON conditions */
  struct JoinClause *next;
};

struct SelectStatement
{
  bool do_explain = false;
  Outputs* outputs = NULL;
  LexCString table = LexCString{NULL, 0};
  TableRef *root_table = NULL;
  JoinClause *joins = NULL;
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct ConditionalExpression* having_expression = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
  Int64 limit = -1; // -1 means no limit
  Int32 sentinel_agg_slot = -1; // Hidden COUNT slot for cross-table filter
                                // semantics: groups where this is 0 had no
                                // rows pass the filter and must be suppressed.
  char* sql_begin = NULL;  // Start of inner query SQL (points into original buffer)
  char* sql_end = NULL;    // End of inner query SQL
};

struct SubqueryResult {
  bool is_null = true;
  Int64 int_val = 0;
  double float_val = 0.0;
  bool is_float = false;
};

struct CorrelatedPair {
  SubqueryResult key;
  SubqueryResult val;
};

/* RonSQL uses 4 types of exceptions:
 * - RonSQLRetryableError indicates that the RonSQL query might be worth
 *   retrying.
 * - RonSQLPermanentError indicates that the RonSQL query is not worth retrying.
 * - RonSQLMaybeStaleSchema is used internally to communicate errors from
 *   operations that depend on the schema. This will cause a schema unload and
 *   reload. Then, if the schema version was stale, it will be rethrown as a
 *   RetryableError, otherwise as a PermanentError. In particular, this is used
 *   for operations that can fail due to stale schema without causing any Ndb
 *   errors, such as RonSQL type checking.
 * - std::runtime_error is used internally to indicate errors where it's unknown
 *   whether they should be retried. This will cause an investigation of Ndb
 *   error codes. Then, it will be rethrown as a RetryableError or a
 *   PermanentError.
 */
class RonSQLRetryableError : public std::runtime_error {
  using std::runtime_error::runtime_error;
};
class RonSQLPermanentError : public std::runtime_error {
  using std::runtime_error::runtime_error;
};
class RonSQLMaybeStaleSchema : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

#endif
