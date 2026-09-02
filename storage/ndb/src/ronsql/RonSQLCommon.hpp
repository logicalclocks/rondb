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

class RdrsSchemaCache;  // Forward declaration — optional, for index list caching

/*
 * Per-request phase timing statistics.  All *_us values are microseconds
 * from a monotonic clock.  A phase that did not run for the given query
 * shape stays 0.  On internal retries (ronsql_op), values reflect the LAST
 * attempt; attempts counts how many attempts ran (1 = no retry).  Capture
 * sites are compiled in only under RONSQL_PHASE_STATS (RonSQLPerf.hpp,
 * default on) and additionally require this sink to be non-NULL on
 * RonSQLExecParams::phase_stats.
 *
 * Phase meanings per execution path:
 * - join/CTE aggregate path: ndbprep = NdbQueryBuilder::prepare;
 *   send = trans->execute; firstbatch = wait for the first result row
 *   (data-node execution incl. CTE materialization); drain = remaining
 *   rows; print = ResultPrinter.
 * - pass-through (non-aggregate) path: send = trans->execute;
 *   firstbatch = first nextResult; drain = remaining rows; print stays 0
 *   (rows are printed inside the drain loop).
 * - single-table path: ndbprep = scan-op definition (bounds, filter,
 *   aggregation code); firstbatch = DoAggregation (the NDB API fuses
 *   send + execute + drain); send/drain stay 0.
 * - single-table pass-through scan arm: like the pass-through path
 *   (ndbprep = scan-op definition + getValue wiring; send / firstbatch /
 *   drain split; rows printed inside the drain, print stays 0).
 * - single-table pass-through PK-lookup arm: ndbprep = lookup-op
 *   definition; firstbatch = execute(Commit) (the NDB API fuses send +
 *   read); send/drain stay 0; rows is 0 or 1.
 */
struct RonSQLPhaseStats
{
  Uint64 parse_us = 0;      // lex + parse + ORDER BY alias resolution
  Uint64 analyze_us = 0;    // CTE / subquery / SELECT-subquery analysis
  Uint64 load_us = 0;       // NDB dictionary access (schema load)
  Uint64 plan_us = 0;       // CTE shape validation + index/filter planning
  Uint64 compile_us = 0;    // agg-program compile + linked projections
  Uint64 prepare_us = 0;    // RonSQLPreparer constructor total
  Uint64 subquery_us = 0;   // execute_subqueries + result substitution
  Uint64 ndbprep_us = 0;    // NdbQueryBuilder::prepare / scan-op definition
  Uint64 send_us = 0;       // transaction execute (send + first wait)
  Uint64 firstbatch_us = 0; // wait for the first result row / DoAggregation
  Uint64 drain_us = 0;      // drain of the remaining result rows
  Uint64 print_us = 0;      // result formatting into the output stream
  Uint64 execute_us = 0;    // RonSQLPreparer::execute total
  Uint64 rows_drained = 0;  // rows pulled from the NDB API (aggregate
                            // records on the agg path, result rows on the
                            // pass-through path; 0 on the fused
                            // single-table path)
  Uint32 attempts = 0;      // ronsql_op attempts (1 = no retry)
};

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
  RdrsSchemaCache* schema_cache = nullptr;  // Optional: avoids dict->listIndexes()
  RonSQLPhaseStats* phase_stats = nullptr;  // Optional: per-phase timing sink
                                            // (captured only when non-NULL and
                                            // RONSQL_PHASE_STATS is compiled in)
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
      bool implicit_scalar_pair_op;
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
  enum class Kind { TABLE_COLUMN, OUTPUT_REF };
  Kind kind;
  union {
    Uint32 col_idx;      // TABLE_COLUMN: index into m_columns
    Uint32 output_idx;   // OUTPUT_REF: index into SELECT outputs list
  };
  bool ascending;
  struct OrderbyColumns* next;
};

/* A single index name in a FORCE/USE/IGNORE INDEX hint list. */
struct IndexHintList
{
  LexCString index_name;
  struct IndexHintList* next;
};

struct TableRef
{
  LexCString database;     /* database name, or {NULL, 0} if unqualified */
  LexCString name;         /* table name */
  LexCString alias;        /* alias, or same as name if no alias */
  /* MySQL-style index hint attached to this table reference.  Honored only
   * for root-table scans (main-query root and CTE-body root); a hint on a
   * joined table is rejected at prepare time.  USE with an empty
   * hint_indexes list means "use no index" (force a table scan). */
  /* Enumerator names are prefixed to avoid clashing with NONE / FORCE /
   * USE / IGNORE preprocessor macros pulled in by MySQL/NDB headers. */
  enum class HintKind : Uint8 { HINT_NONE = 0, HINT_FORCE, HINT_USE, HINT_IGNORE };
  HintKind hint_kind = HintKind::HINT_NONE;
  IndexHintList* hint_indexes = NULL;
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

struct CteDefinition;
class AggregationAPICompiler;

struct SelectStatement
{
  bool do_explain = false;
  CteDefinition* cte_list = NULL;  // Linked list of CTE definitions (WITH clause)
  Outputs* outputs = NULL;
  LexCString table = LexCString{NULL, 0};
  TableRef *root_table = NULL;
  JoinClause *joins = NULL;
  struct ConditionalExpression* where_expression = NULL;
  struct GroupbyColumns* groupby_columns = NULL;
  struct ConditionalExpression* having_expression = NULL;
  struct OrderbyColumns* orderby_columns = NULL;
  Int64 limit = -1; // -1 means no limit
  Int64 frags_per_worker = 0; // FRAGS_PER_WORKER hint: root fragments bundled
                              // per SPJ worker for aggregate pushed queries.
                              // 0 = unset; the NDB API normalizes and clamps.
  Int32 sentinel_agg_slot = -1; // Hidden COUNT slot for cross-table filter
                                // semantics: groups where this is 0 had no
                                // rows pass the filter and must be suppressed.
  // Single-row key-lookup CTE body (cte_single_row_kernel_plan.md).
  // Meaningful only on a CTE body statement: a non-aggregating
  // single-table body whose WHERE binds the full primary key by
  // equality with constants (enforced at plan time in
  // enforce_single_row_cte_body).  Emitted with the CTE_SINGLE_ROW
  // kernel mode: every projected column a GROUP BY column, zero
  // aggregate slots, subset-key CTE_LOOKUP consumers.  Set by
  // detect_single_row_ctes() during parse(); no AST rewrite.
  bool is_single_row_cte = false;
  char* sql_begin = NULL;  // Start of inner query SQL (points into original buffer)
  char* sql_end = NULL;    // End of inner query SQL

  // Aggregator program compiled while parsing this SELECT body. Populated
  // by the cte_def / subquery grammar actions via the value returned from
  // Context::leave_subquery(). The main query's aggregator lives on
  // QueryScope::agg and is not stored here.
  AggregationAPICompiler* agg = NULL;
};

struct CteDefinition
{
  LexCString name;              // CTE alias (e.g., "purchase_agg")
  SelectStatement* stmt;        // The CTE's SELECT statement
  struct CteDefinition* next;   // Linked list
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
