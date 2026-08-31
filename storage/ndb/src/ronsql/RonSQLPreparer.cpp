// vim: set fileencoding=utf-8 : -*- coding: utf-8 -*-

/*
   Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.

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

#include "AggregationAPICompiler.hpp"
#include "NdbDictionaryImpl.hpp"
#include "NdbQueryBuilder.hpp"
#include "NdbQueryOperation.hpp"
#include "QueryPlanner.hpp"
#include "RonSQLParser.y.hpp"
#include "RonSQLLexer.l.hpp"
#include "RonSQLPreparer.hpp"
#include <iostream>
#include <sstream>
#include <cstdlib>
#include <cerrno>
#include <climits>
#include <algorithm>
#include <vector>
#include <map>
#include <set>
#include <string>
#include <functional>
#include <string>
#include "define_formatter.hpp"
#include "my_time.h"
#include "mysql_time.h"
#include "my_inttypes.h"
#include <my_base.h>
#include "decimal.h"
#include <decimal_utils.hpp>
#include <kernel/Interpreter.hpp>
#include <kernel/signaldata/QueryTree.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_RONSQLPREPARER 1
// Independent of the very verbose DEBUG_RONSQLPREPARER DEB_TRACE(): a printf-style
// trace (stdout -> RDRS log) for the projection-only pass-through drain, used to
// chase the D3 hang (nextResult loop over a CTE subtree + CTE_LOOKUP child).
// Keep disabled: the [drain] lines print to ronsql_cli stdout and break
// ronsql_compare diffs (e.g. ronsql_minmax_string).
//#define DEBUG_RONSQL_DRAIN 1
#endif

#ifdef DEBUG_RONSQL_DRAIN
#define DEB_DRAIN(arglist) do { printf arglist ; fflush(stdout); } while (0)
#else
#define DEB_DRAIN(arglist) do { } while (0)
#endif

#ifdef DEBUG_RONSQLPREPARER
#define DEB_TRACE() do { \
  printf("RonSQLPreparer.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
std::ostream& operator<<(std::ostream& os, const raw_value& rv) {
  os << "(len=" << rv.len << ", val=";
  if (rv.val == NULL) {
    os << "NULL";
  } else {
    const unsigned char* bytes = static_cast<const unsigned char*>(rv.val);
    for (Uint32 i = 0; i < rv.len; i++) {
      char hex[4];
      snprintf(hex, sizeof(hex), "%s%02x", (i > 0 ? " " : ""), bytes[i]);
      os << hex;
    }
  }
  os << ")";
  return os;
}
template<typename T>
inline T dbg_print(int line, const char* expr, T val) {
  std::cout << "RonSQLPreparer.cpp:" << line << " " << expr << " -> " << val << std::endl;
  return val;
}
#define DBG(expr) dbg_print(__LINE__, #expr, (expr))
#define DBGV(expr) do { \
  dbg_print(__LINE__, #expr, "<void>"); \
  (expr); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#define DBG(expr) (expr)
#define DBGV(expr) (expr)
#endif

using std::endl;

#include "RonSQLPerf.hpp"
#include "RdrsSchemaCache.hpp"

#define feature_not_implemented(description) \
  throw RonSQLPermanentError("RonSQL feature not implemented: " description)

static const char* interval_type_name(TokenKind interval_type);

DEFINE_FORMATTER(quoted_identifier, LexString, {
  os.put('`');
  for (Uint32 i = 0; i < value.len; i++)
  {
    char ch = value.str[i];
    if (ch == '`')
      os.write("``", 2);
    else
      os.put(ch);
  }
  os.put('`');
})

DEFINE_FORMATTER(quoted_identifier, char*, {
  const char* iter = value;
  os.put('`');
  while (*iter != '\0')
  {
    if (*iter == '`')
      os.write("``", 2);
    else
      os.put(*iter);
    iter++;
  }
  os.put('`');
})

// Make sure that the number of registers in AggregationAPICompiler.hpp matches
// that in ../../include/ndbapi/NdbAggregationCommon.hpp
static_assert(REGS == kRegTotal);

RonSQLPreparer::RonSQLPreparer(RonSQLExecParams conf):
  m_conf(conf),
  m_amalloc(conf.amalloc),
  m_context(*this),
  m_columns(conf.amalloc),
  m_column_qualifiers(conf.amalloc),
  m_col_is_inner(conf.amalloc),
  m_col_is_alias(conf.amalloc),
  m_main_scope(conf.amalloc),
  m_indexes(conf.amalloc),
  m_toplevel_conditions(conf.amalloc),
  m_greatest_least_pair_loads(conf.amalloc),
  m_scan_config_candidates(conf.amalloc),
  m_select_subquery_leaves(conf.amalloc),
  m_merged_leaves(conf.amalloc),
  m_subquery_infos(conf.amalloc),
  m_cte_scopes(conf.amalloc)
{
  ndbrequire(m_status == Status::BEGIN);
  try {
    PERF_TS(t_prep_start);
    configure();
    PERF_TS(t_parse_start);
    STAT_TS(m_conf.phase_stats, s_parse_start);
    parse();
    resolve_orderby_aliases();
    PERF_TS(t_parse_end);
    PERF_LOG("  parse", t_parse_start, t_parse_end);
    STAT_TS(m_conf.phase_stats, s_parse_end);
    STAT_SET(m_conf.phase_stats, parse_us, s_parse_start, s_parse_end);
    analyze_ctes();
    analyze_subqueries();
    analyze_select_subqueries();
    PERF_TS(t_load_start);
    STAT_TS(m_conf.phase_stats, s_load_start);
    STAT_SET(m_conf.phase_stats, analyze_us, s_parse_end, s_load_start);
    load();
    PERF_TS(t_load_end);
    PERF_LOG("  load (dict)", t_load_start, t_load_end);
    STAT_TS(m_conf.phase_stats, s_load_end);
    STAT_SET(m_conf.phase_stats, load_us, s_load_start, s_load_end);
    if (m_has_ctes)
      validate_cte_execution_shapes();
    if (!is_join_query())
      plan_index_and_filter();
    PERF_TS(t_compile_start);
    STAT_TS(m_conf.phase_stats, s_compile_start);
    STAT_SET(m_conf.phase_stats, plan_us, s_load_end, s_compile_start);
    compile();
    if (is_join_query())
      build_agg_linked_projections();
    if (m_cte_scopes.size() > 0)
      build_cte_linked_projections();
    PERF_TS(t_compile_end);
    PERF_LOG("  compile", t_compile_start, t_compile_end);
    STAT_TS(m_conf.phase_stats, s_compile_end);
    STAT_SET(m_conf.phase_stats, compile_us, s_compile_start, s_compile_end);
    determine_explain();
    PERF_LOG("  prepare total", t_prep_start, t_compile_end);
    m_status = Status::PREPARED;
  }
  catch (...) {
    m_status = Status::FAILED;
    handle_ronsql_exception(std::current_exception());
  }
}

RonSQLPreparer::RonSQLPreparer(RonSQLExecParams conf, ParseOnly):
  m_conf(conf),
  m_parse_only(true),
  m_amalloc(conf.amalloc),
  m_context(*this),
  m_columns(conf.amalloc),
  m_column_qualifiers(conf.amalloc),
  m_col_is_inner(conf.amalloc),
  m_col_is_alias(conf.amalloc),
  m_main_scope(conf.amalloc),
  m_indexes(conf.amalloc),
  m_toplevel_conditions(conf.amalloc),
  m_greatest_least_pair_loads(conf.amalloc),
  m_scan_config_candidates(conf.amalloc),
  m_select_subquery_leaves(conf.amalloc),
  m_merged_leaves(conf.amalloc),
  m_subquery_infos(conf.amalloc),
  m_cte_scopes(conf.amalloc)
{
  ndbrequire(m_status == Status::BEGIN);
  try {
    configure();
    parse();
    m_status = Status::PARSED;
  }
  catch (...) {
    m_status = Status::FAILED;
    handle_ronsql_exception(std::current_exception());
  }
}

LexCString
RonSQLPreparer::get_table_name()
{
  ndbrequire(m_status == Status::PARSED || m_status == Status::PREPARED);
  return m_context.ast_root.table;
}

const DynamicArray<LexCString>&
RonSQLPreparer::get_referenced_columns()
{
  ndbrequire(m_status == Status::PARSED || m_status == Status::PREPARED);
  return m_columns;
}

// require or fail without retry
static inline void
require_prm(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw RonSQLPermanentError(msg);
}

// require or fail without retry, claiming it's a bug
#define require_bug(x, msg) require_prm(x, msg " Please report a bug.")

// require or investigate schema version
static inline void
require_sch(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw RonSQLMaybeStaleSchema(msg);
}

// require or investigate ndb error
static inline void
require_run(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw std::runtime_error(msg);
}

// require or fail with retry
static inline void
require_tmp(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw RonSQLRetryableError(msg);
}

/*
 * Walk a ConditionalExpression subtree and determine which table its
 * columns reference.  Returns:
 *   >= 0 : all columns belong to that table index
 *     -1 : no column references (constant-only subtree)
 *     -2 : columns span multiple tables (cross-table condition)
 */
Int32
RonSQLPreparer::classify_ce_table_resolved(
    const QueryScope& scope,
    struct ConditionalExpression* ce) const
{
  if (ce == NULL)
    return -1;
  switch (ce->op)
  {
  case T_IDENTIFIER:
  {
    ndbrequire(scope.resolved_columns != NULL);
    const QueryScope::ResolvedColumnRef& ref =
        scope.resolved_columns[ce->col_idx];
    if (ref.kind == QueryScope::ResolvedColumnRef::Kind::Unresolved ||
        ref.kind == QueryScope::ResolvedColumnRef::Kind::AliasOnly)
      return -1;
    return (Int32)ref.join_op_idx;
  }
  case T_OR:
  case T_AND:
  case T_EQUALS:
  case T_NOT_EQUALS:
  case T_GE:
  case T_GT:
  case T_LE:
  case T_LT:
  case T_PLUS:
  case T_MINUS:
  case T_MULTIPLY:
  case T_SLASH:
  case T_DIV:
  case T_MODULO:
  case T_BITWISE_OR:
  case T_BITWISE_AND:
  case T_BITSHIFT_LEFT:
  case T_BITSHIFT_RIGHT:
  case T_BITWISE_XOR:
  case T_DATE_ADD:
  case T_DATE_SUB:
  {
    Int32 left_t = classify_ce_table_resolved(scope, ce->args.left);
    Int32 right_t = classify_ce_table_resolved(scope, ce->args.right);
    if (left_t == -1) return right_t;
    if (right_t == -1) return left_t;
    if (left_t == right_t) return left_t;
    return -2;  // cross-table
  }
  case T_NOT:
  case T_EXCLAMATION:
    return classify_ce_table_resolved(scope, ce->args.left);
  case T_IS:
    return classify_ce_table_resolved(scope, ce->is.arg);
  case T_INTERVAL:
    return classify_ce_table_resolved(scope, ce->interval.arg);
  case T_EXTRACT:
    return classify_ce_table_resolved(scope, ce->extract.arg);
  case T_EXISTS:
  case I_IN_SUBQUERY:
  case I_SUBQUERY:
    return -1;  // Subqueries are treated as constants
  default:
    // Constants, strings, etc. — no column reference
    return -1;
  }
}

static Uint32 filter_expr_reg_depth(ConditionalExpression* ce);
static const Uint32 MAX_WHERE_CONJUNCTS = 64;

// Defined later in this file; forward-declared for the Phase i26
// real-vs-CTE filter routing in classify_where_by_table.
static Uint32 find_or_add_linked_proj(JoinPlan& plan, Uint32 op_idx,
                                      const char* col_name);
static bool linked_source_is_leaf_ancestor(const JoinPlan& plan,
                                           Uint32 leaf_idx,
                                           Uint32 target_op_idx);

/*
 * Flatten nested AND nodes into an array of conjuncts.
 */
static void
flatten_and_conjuncts(struct ConditionalExpression* ce,
                      ConditionalExpression** conjuncts,
                      Uint32* count)
{
  if (ce == NULL) return;
  if (ce->op == T_AND)
  {
    flatten_and_conjuncts(ce->args.left, conjuncts, count);
    flatten_and_conjuncts(ce->args.right, conjuncts, count);
    return;
  }
  if (*count >= MAX_WHERE_CONJUNCTS)
  {
    throw RonSQLPermanentError(
        "WHERE clause has too many top-level AND conditions.");
  }
  conjuncts[*count] = ce;
  (*count)++;
}

/*
 * Flatten nested OR nodes into an array of disjuncts.  Mirrors
 * flatten_and_conjuncts.  An expression with no top-level T_OR yields
 * one disjunct (the whole expression).  Used by Phase I.2's CTE_LOOKUP
 * filter DNF emit; CNF/DNF rewriting is not performed here, so callers
 * that need DNF must reject non-DNF nesting separately.
 */
static void
flatten_or_disjuncts(struct ConditionalExpression* ce,
                     ConditionalExpression** disjuncts,
                     Uint32* count)
{
  if (ce == NULL) return;
  if (ce->op == T_OR)
  {
    flatten_or_disjuncts(ce->args.left, disjuncts, count);
    flatten_or_disjuncts(ce->args.right, disjuncts, count);
    return;
  }
  if (*count >= MAX_WHERE_CONJUNCTS)
  {
    throw RonSQLPermanentError(
        "WHERE clause has too many top-level OR disjuncts.");
  }
  disjuncts[*count] = ce;
  (*count)++;
}

/*
 * Phase I.2: detect non-DNF shapes in a CTE_LOOKUP WHERE conjunct.
 * After flatten_or_disjuncts, each disjunct should contain only T_AND
 * nodes plus comparison/IS atoms — any remaining T_OR (i.e. an OR
 * nested inside an AND) means the expression isn't in DNF and we
 * reject cleanly rather than silently mis-evaluating.
 */
static bool
contains_or_below_top_level(struct ConditionalExpression* ce)
{
  if (ce == NULL) return false;
  if (ce->op == T_OR) return true;
  if (ce->op == T_AND)
  {
    return contains_or_below_top_level(ce->args.left) ||
           contains_or_below_top_level(ce->args.right);
  }
  return false;
}

void
RonSQLPreparer::configure()
{
  // Validate m_conf
#ifdef VM_TRACE
  ndbrequire(m_conf.sql_buffer != NULL);
  ndbrequire(m_conf.sql_len > 0);
  ndbrequire(m_conf.amalloc != NULL);
  RonSQLExecParams::ExplainMode mode = m_conf.explain_mode;
  bool may_query =
    (mode == RonSQLExecParams::ExplainMode::ALLOW ||
     mode == RonSQLExecParams::ExplainMode::FORBID ||
     mode == RonSQLExecParams::ExplainMode::REMOVE);
  bool may_explain =
    (mode == RonSQLExecParams::ExplainMode::ALLOW ||
     mode == RonSQLExecParams::ExplainMode::REQUIRE ||
     mode == RonSQLExecParams::ExplainMode::FORCE);
  ndbrequire(may_query || may_explain);
  ndbrequire(m_conf.out_stream != NULL);
  ndbrequire(m_conf.output_format == RonSQLExecParams::OutputFormat::JSON ||
             m_conf.output_format == RonSQLExecParams::OutputFormat::JSON_ASCII ||
             m_conf.output_format == RonSQLExecParams::OutputFormat::TEXT ||
             m_conf.output_format == RonSQLExecParams::OutputFormat::TEXT_NOHEADER);
  if (may_query && !m_parse_only)
  {
    ndbrequire(m_conf.ndb != NULL);
  }
  ndbrequire(m_conf.err_stream != NULL);
#endif
  /*
   * Both `yy_scan_string' and `yy_scan_bytes' create and scan a copy of the
   * input. This may be desirable, since `yylex()' modifies the contents of the
   * buffer it is scanning. In order to avoid copying, we use `yy_scan_buffer'.
   * It requires the last two bytes of the buffer to be NUL. These last two
   * bytes are not scanned.
   * See https://ftp.gnu.org/old-gnu/Manuals/flex-2.5.4/html_node/flex_12.html
   */
  char* sql_buffer = m_conf.sql_buffer;
  size_t sql_len = m_conf.sql_len;
  // SQL buffer must be double NUL-terminated
  ndbrequire(sql_len >= 2 &&
             sql_buffer[sql_len-1] == '\0' &&
             sql_buffer[sql_len-2] == '\0');
  rsqlp_lex_init_extra(&m_context, &m_scanner);
  // The non-const sql_buffer is only used to initialize the flex scanner. The
  // flex scanner shouldn't modify it either, but only because we have removed
  // the buffer-modifying code from the generated output (see build_lexer.sh).
  // For this reason, the lexer still declares the buffer as non-const.
  m_buf = rsqlp__scan_buffer(DBG(sql_buffer), sql_len, m_scanner);
  // We don't want the NUL bytes that flex requires.
  size_t our_buffer_len = sql_len - 2;
  m_sql = { static_cast<const char*>(sql_buffer), our_buffer_len };
}

void
RonSQLPreparer::parse()
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  int parse_result = rsqlp_parse(m_scanner); /* datatype to match declaration
                                              * int yyparse (yyscan_t scanner)
                                              * in RonDBSQLParser.y.cpp, which
                                              * is generated by bison in
                                              * build_parser.sh
                                              */
  if (parse_result == 0)
  {
    ndbrequire(m_context.m_err_state == ErrState::NONE);
    /* We have already provided columns and expressions to the
     * AggregationAPICompiler. E.g. in `SELECT Max(col1 + col2)`, m_main_scope.agg already
     * knows about `col1`, `col2` and `col1 + col2`. Here, we let m_main_scope.agg know about
     * the aggregate expressions themselves, e.g. `Max(col1 + col2)`, making sure
     * they are provided in the correct order.
     */
    Outputs* outputs = m_context.ast_root.outputs;
    bool has_aggregate_outputs = false;
    bool has_subquery_agg_outputs = false;
    while (outputs != NULL)
    {
      switch (outputs->type)
      {
      case Outputs::Type::COLUMN:
        break;
      case Outputs::Type::AGGREGATE:
      {
        has_aggregate_outputs = true;
        ndbrequire(m_main_scope.agg != NULL);
        TokenKind fun = outputs->aggregate.fun;
        AggregationAPICompiler::Expr* expr = outputs->aggregate.arg;
        switch (fun)
        {
        case T_COUNT:
          outputs->aggregate.agg_index = m_main_scope.agg->Count(expr);
          break;
        case T_MAX:
          outputs->aggregate.agg_index = m_main_scope.agg->Max(expr);
          break;
        case T_MIN:
          outputs->aggregate.agg_index = m_main_scope.agg->Min(expr);
          break;
        case T_SUM:
          outputs->aggregate.agg_index = m_main_scope.agg->Sum(expr);
          break;
        default:
          abort();
        }
        break;
      }
      case Outputs::Type::AVG:
        has_aggregate_outputs = true;
        outputs->avg.agg_index_sum = m_main_scope.agg->Sum(outputs->avg.arg);
        outputs->avg.agg_index_count = m_main_scope.agg->Count(outputs->avg.arg);
        break;
      case Outputs::Type::SUBQUERY_AGG:
        // Handled later in analyze_select_subqueries() and compile().
        // Don't set has_aggregate_outputs (avoids m_main_scope.agg != NULL assert).
        has_subquery_agg_outputs = true;
        break;
      default:
        abort();
      }
      outputs = outputs->next;
    }
    bool has_having_aggregates = (m_main_scope.agg != NULL &&
                                  m_context.ast_root.having_expression != NULL);
    if (m_main_scope.agg == NULL)
    {
      ndbrequire(!has_aggregate_outputs);
    }
    else
    {
      ndbrequire(has_aggregate_outputs || has_having_aggregates);
      ndbrequire(m_main_scope.agg->getStatus() == AggregationAPICompiler::Status::PROGRAMMING);
    }
    // Part B (join_nest_semantics_plan.md): LEFT->INNER promotion must
    // run before the non-aggregate gate (whose INNER-below-LEFT check
    // is thereby demoted to a defensive dead-man's check) and applies
    // to aggregate queries too — the emitted tree for an unpromoted
    // chain (MatchNonNull child of a MatchAll node, no nest metadata)
    // expresses `t LEFT (b INNER c)` instead of SQL's left-associative
    // `(t LEFT b) INNER c`, with verified wrong results on both the
    // aggregate path (ns-1: COUNT(*) 30 vs 10) and the pass-through
    // row path (sn-15).
    promote_left_joins();
    m_is_aggregate_query = (has_aggregate_outputs || has_having_aggregates ||
                            has_subquery_agg_outputs);
    // Single-row key-lookup CTE bodies (cte_single_row_kernel_plan.md):
    // mark candidate CTEs BEFORE the projection-only gate and the
    // partial-key root rewrite so cte_key_coverage can classify their
    // consumers as SingleRowSubset.  Runs on aggregate main queries
    // too (the gate is pass-through-only, but analyze_ctes()'s
    // aggregate requirement applies to every path).
    detect_single_row_ctes();
    if (!m_is_aggregate_query)
    {
      // Phase E.3: allow the narrowly-supported projection-only main
      // SELECT over a CTE_SCAN root (e.g. SELECT k, t FROM cte [WHERE ...]).
      // The kernel + NDB API already support scanCte without
      // setAggregation(); the gap was purely RonSQL's client-side
      // delivery path.  All other non-aggregating shapes still hit the
      // existing rejection — Phase 7 / step 45 in ronsql_join_phase7.md
      // tracks general non-aggregate support.
      //
      // Plan-shape decisions (CTE_SCAN root, GROUP BY, HAVING, ORDER BY,
      // LIMIT) are checked in execute_join() / passthrough_drain when
      // the JoinPlan is finalised — analyze_ctes() runs before this
      // point but the join_plan is only populated by load_join().  We
      // do the cheap parse-time guards here (no aggregates already
      // implies no HAVING aggregates) and require the FROM root to be
      // a CTE alias; remaining shape rejection is deferred.
      //
      // Phase 6 (non_aggregate_phase_6.md, gc-P1 lift): run the
      // I.16b/c partial-key-CTE root rewrite BEFORE the gate walk, so
      // a partial-key INNER join to a multi-key CTE becomes the
      // shipped CTE_SCAN-root shape instead of failing the
      // complete-key coverage check below.  The load_join() call
      // stays (aggregate-path timing unchanged); this earlier
      // invocation makes that one a no-op via its root_is_cte bail.
      maybe_rewrite_partial_key_cte_root();
      bool from_is_cte = false;
      for (const CteDefinition* cte = m_context.ast_root.cte_list;
           cte != NULL; cte = cte->next) {
        if (m_context.ast_root.table.str != NULL &&
            cte->name.len == m_context.ast_root.table.len &&
            strncmp(cte->name.str, m_context.ast_root.table.str,
                    m_context.ast_root.table.len) == 0) {
          from_is_cte = true;
          break;
        }
      }
      bool has_groupby   = (m_context.ast_root.groupby_columns != NULL);
      bool has_having    = (m_context.ast_root.having_expression != NULL);
      bool has_joins     = (m_context.ast_root.joins != NULL);
      // Phases 2+3 (ronsql_orderby_limit_plan.md): LIMIT and ORDER BY
      // are accepted on every projection-only shape.  LIMIT alone
      // streams with an early scan close; ORDER BY buffers the rows for
      // a client-side sort in the drains (LIMIT then applies post-sort
      // via partial_sort) — or, on the single-table path, streams in
      // index order when an ordered index serves the ORDER BY
      // (Phase 4b, ScanConfig::index_order).
      // All projection-only outputs must be plain COLUMN refs.
      bool all_column_outputs = true;
      for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
           o = o->next) {
        if (o->type != Outputs::Type::COLUMN) {
          all_column_outputs = false;
          break;
        }
      }
      // Phase E.3 (already shipped): FROM <cte>, projection-only,
      // no joins.
      bool projection_only_cte_scan =
          (from_is_cte && all_column_outputs && !has_groupby &&
           !has_having && !has_joins);
      // Phase 1 (non_aggregate_phase_1.md, W1): projection-only
      // single-table queries over a real table — the first shape with
      // no aggregation and no CTE.  Requires a FROM table (the Phase
      // I.17h optional-FROM path cannot resolve COLUMN outputs) and no
      // CTE definitions at all (a WITH prefix on a single-table
      // projection stays rejected rather than silently ignoring the
      // CTE).
      bool has_from = (m_context.ast_root.table.str != NULL &&
                       m_context.ast_root.table.len > 0);
      bool projection_only_single_table =
          (has_from && !from_is_cte && !has_joins && all_column_outputs &&
           !has_groupby && !has_having &&
           m_context.ast_root.cte_list == NULL);
      // Projection-only join chains (Phase I.8/I.11/I.12 for
      // CTE-involving shapes; Phase 2 of non_aggregate_pushdown_plan.md
      // extends the same walk to pure real-table snowflake chains by
      // dropping the previous at-least-one-CTE requirement).  Accepted:
      // strictly left-deep INNER / LEFT_OUTER chains whose ON
      // conditions bind the joined alias's columns to an
      // already-visible alias, with every CTE operand a complete-key
      // CTE_LOOKUP (ExactOrdered or ExactPermuted via
      // cte_key_coverage); no GROUP BY / HAVING / aggregate outputs
      // (ORDER BY buffers + sorts client-side, LIMIT streams or applies
      // post-sort — Phases 2+3).  Phase G's defensive reject for
      // CTE_SCAN-as-outer-join-child still fires later in
      // validate_cte_execution_shapes() for any planner-produced shape
      // outside this conservative class.
      bool projection_only_join_chain = false;
      if (all_column_outputs && !has_groupby &&
          !has_having && has_joins) {
        projection_only_join_chain = true;
        LexCString visible_aliases[MAX_SPJ_TREE_NODES];
        // Defensive dead-man's check (join_nest_semantics_plan.md): an
        // alias is "under LEFT" when it was LEFT-joined itself or
        // reached through one.  promote_left_joins() runs before this
        // gate and rewrites every INNER-below-LEFT flat chain to
        // all-INNER (equality join conditions are null-rejecting), so
        // this check firing means a promotion bug or a future
        // non-equality join condition — without promotion the emitted
        // tree (no nest metadata) would deliver NULL-extended rows the
        // INNER join must eliminate (verified wrong results, sn-15).
        bool alias_under_left[MAX_SPJ_TREE_NODES];
        Uint32 num_visible_aliases = 0;
        alias_under_left[num_visible_aliases] = false;
        visible_aliases[num_visible_aliases++] =
            m_context.ast_root.root_table->alias;
        for (const JoinClause* join = m_context.ast_root.joins;
             join != NULL; join = join->next) {
          // Phase 4 (non_aggregate_phase_4.md, W1): a conditionless
          // join clause is acceptable only as the comma cross-join of
          // a no-GROUP-BY CTE (the grammar's one conditionless
          // production).  A SCALAR CTE always produces exactly one
          // row (1-row multiplier, coverage ScalarDummy); a
          // SINGLE-ROW CTE (cte_single_row_kernel_plan.md) produces
          // at most one — the comma join is a zero-key existence
          // probe (coverage SingleRowSubset) with exact MySQL
          // empty-CTE semantics.  Real-table and grouped-CTE comma
          // joins stay rejected.
          const CteDefinition* join_cte =
              find_cte_definition(join->table.name);
          bool scalar_cte_cross_join =
              (join->conditions == NULL && join_cte != NULL &&
               join_cte->stmt->groupby_columns == NULL &&
               join->join_type == JoinClause::INNER_JOIN);
          if (num_visible_aliases >= MAX_SPJ_TREE_NODES ||
              (join->join_type != JoinClause::INNER_JOIN &&
               join->join_type != JoinClause::LEFT_OUTER_JOIN) ||
              (join->conditions == NULL && !scalar_cte_cross_join)) {
            projection_only_join_chain = false;
            break;
          }
          const LexCString& join_alias = join->table.alias;
          bool any_parent_under_left = false;
          for (const JoinCondition* jc = join->conditions;
               jc != NULL; jc = jc->next) {
            if (!(jc->child_table == join_alias)) {
              projection_only_join_chain = false;
              break;
            }
            bool parent_seen = false;
            for (Uint32 a = 0; a < num_visible_aliases; a++) {
              if (jc->parent_table == visible_aliases[a]) {
                parent_seen = true;
                if (alias_under_left[a]) any_parent_under_left = true;
                break;
              }
            }
            if (!parent_seen) {
              projection_only_join_chain = false;
              break;
            }
          }
          if (!projection_only_join_chain) break;
          if (join->join_type == JoinClause::INNER_JOIN &&
              any_parent_under_left) {
            ndbrequire(m_conf.err_stream != NULL);
            std::basic_ostream<char>& err = *m_conf.err_stream;
            err << "INNER JOIN below a LEFT JOIN is not supported in"
                   " projection-only queries: RonSQL does not yet emit"
                   " outer-join nest metadata, and NDB would deliver"
                   " NULL-extended rows the INNER join must eliminate.\n";
            throw RonSQLPermanentError(
                "INNER JOIN below LEFT JOIN not supported.");
          }
          if (join_cte != NULL) {
            LexCString child_names[MAX_JOIN_KEY_COLS];
            Uint32 num_keys = 0;
            for (const JoinCondition* jc = join->conditions;
                 jc != NULL; jc = jc->next) {
              if (num_keys >= MAX_JOIN_KEY_COLS) {
                projection_only_join_chain = false;
                break;
              }
              child_names[num_keys++] = jc->child_column;
            }
            if (!projection_only_join_chain) break;
            CteKeyCoverageResult coverage;
            // Phase 4: ScalarDummy (scalar CTE, no keys) joins the
            // accepted set — its producer restricts it to exactly the
            // conditionless-scalar case admitted above.
            if (!cte_key_coverage(join_cte, child_names, num_keys,
                                  coverage)) {
              projection_only_join_chain = false;
              break;
            }
            // Phase 6 (non_aggregate_phase_6.md): the pre-gate
            // I.16b/c rewrite above already promoted the first
            // eligible partial-key INNER CTE join to root, so a
            // Partial state reaching this walk is a residual shape
            // the rewrite cannot serve (LEFT JOIN on the partial CTE
            // join itself, a non-root parent alias, a second partial
            // CTE, or a CTE-on-CTE root).  Throw the emit-side I.16a
            // wording instead of the generic gate text; same for
            // WrongColumns (the I.20 message).
            if (coverage.state == CteKeyCoverage::Partial) {
              ndbrequire(m_conf.err_stream != NULL);
              std::basic_ostream<char>& err = *m_conf.err_stream;
              err << "Partial CTE lookup key not supported.  The virtual"
                     " CTE primary key matches the CTE body's GROUP BY"
                     " column list and the join must bind every key"
                     " column.  Workaround: place the multi-key CTE on"
                     " the joined root and join the smaller table to it"
                     " (an INNER join from the root parent is rewritten"
                     " that way automatically).\n";
              throw RonSQLPermanentError(
                  "Partial CTE lookup key not supported.");
            }
            if (coverage.state == CteKeyCoverage::WrongColumns) {
              ndbrequire(m_conf.err_stream != NULL);
              std::basic_ostream<char>& err = *m_conf.err_stream;
              if (join_cte->stmt->is_single_row_cte) {
                err << "CTE lookup key references a column that is not"
                       " an output of the single-row CTE.\n";
              } else {
                err << "CTE lookup key references a CTE output column"
                       " that is not part of the virtual primary key."
                       "  The virtual CTE primary key matches the CTE"
                       " body's GROUP BY column list.\n";
              }
              throw RonSQLPermanentError(
                  "CTE lookup key references a non-key CTE column.");
            }
            if (coverage.state != CteKeyCoverage::ExactOrdered &&
                coverage.state != CteKeyCoverage::ExactPermuted &&
                coverage.state != CteKeyCoverage::ScalarDummy &&
                coverage.state != CteKeyCoverage::SingleRowSubset) {
              projection_only_join_chain = false;
              break;
            }
          }
          alias_under_left[num_visible_aliases] =
              (join->join_type == JoinClause::LEFT_OUTER_JOIN) ||
              any_parent_under_left;
          visible_aliases[num_visible_aliases++] = join_alias;
        }
      }
      if (!projection_only_cte_scan && !projection_only_join_chain &&
          !projection_only_single_table) {
        ndbrequire(m_conf.err_stream != NULL);
        std::basic_ostream<char>& err = *m_conf.err_stream;
        err << "This query has no aggregate expression, so it is not an aggregate query.\n"
               "Currently, RonSQL only supports aggregate queries and projection-only\n"
               "SELECTs: single-table, left-deep join chains over real tables and/or\n"
               "complete-key CTE lookups (INNER / LEFT JOIN), comma cross-joins of\n"
               "scalar CTEs, and CTE_SCAN roots — all without GROUP BY /\n"
               "expressions (ORDER BY and LIMIT are supported).  See\n"
               "non_aggregate_pushdown_plan.md for the broader roadmap.\n";
        throw RonSQLPermanentError("Not an aggregate query.");
      }
    }

    // Mirror the main-query aggregate-registration loop for each CTE body.
    // The parser builds each AGGREGATE output's arg Expr in the CTE's own
    // AggregationAPICompiler (stashed on cte->stmt->agg by leave_subquery),
    // but the Sum/Count/Min/Max call that binds the aggregate to the
    // program only happens here. Without it, compile() would reject the
    // CTE's program because the arg Exprs have usage=0.
    for (CteDefinition* cte = m_context.ast_root.cte_list;
         cte != NULL; cte = cte->next) {
      if (cte->stmt->agg == NULL) continue;
      for (Outputs* co = cte->stmt->outputs; co != NULL; co = co->next) {
        switch (co->type) {
        case Outputs::Type::COLUMN:
          break;
        case Outputs::Type::AGGREGATE: {
          AggregationAPICompiler::Expr* expr = co->aggregate.arg;
          switch (co->aggregate.fun) {
          case T_COUNT:
            co->aggregate.agg_index = cte->stmt->agg->Count(expr); break;
          case T_MAX:
            co->aggregate.agg_index = cte->stmt->agg->Max(expr); break;
          case T_MIN:
            co->aggregate.agg_index = cte->stmt->agg->Min(expr); break;
          case T_SUM:
            co->aggregate.agg_index = cte->stmt->agg->Sum(expr); break;
          default:
            abort();
          }
          break;
        }
        case Outputs::Type::AVG:
          co->avg.agg_index_sum = cte->stmt->agg->Sum(co->avg.arg);
          co->avg.agg_index_count = cte->stmt->agg->Count(co->avg.arg);
          break;
        case Outputs::Type::SUBQUERY_AGG:
          // Disallowed in CTE bodies (would need nested subquery orchestration).
          throw RonSQLPermanentError(
              "Subquery aggregation inside CTE body not yet supported.");
        default:
          abort();
        }
      }
    }
    return;
  }
  // The rest is error handling.
  if (parse_result == 2)
  {
    /*
     * Bison parser reports OOM. Generally, this can happen in three situations:
     * 1) Stack depth would exceed YYINITDEPTH but bison doesn't know how to
     *    expand the stack. Since RSQLP_LTYPE_IS_TRIVIAL and
     *    RSQLP_STYPE_IS_TRIVIAL are defined in RonSQLParser.y, this case does
     *    not apply to us.
     * 2) Stack depth would exceed YYMAXDEPTH.
     * 3) The allocator used by the parser returns NULL, indicating OOM. Since
     *    the allocation function we use will never return NULL but rather throw
     *    an exception on OOM, this case does not apply to us.
     * Therefore, we know that if we end up here, we are in case 2).
     */
    throw RonSQLPermanentError("Parser stack exceeded its maximum depth.");
  }
  ndbrequire(parse_result == 1);
  ndbrequire(m_context.m_err_state != ErrState::NONE);
  ndbrequire(m_sql.str <= m_context.m_err_pos);
  size_t err_pos = m_context.m_err_pos - m_sql.str;
  size_t err_stop = err_pos + m_context.m_err_len;
  ndbrequire(err_pos <= m_sql.len);
  ndbrequire(err_stop <= m_sql.len + 1); // "Unexpected end of input" marks the
                                         // character directly after the end.
  const char* msg = NULL;
  bool print_statement = true;
  switch (m_context.m_err_state)
  {
  case ErrState::LEX_NUL:
    err << "Input contains null byte at position " << err_pos << ".\n";
    print_statement = false;
    msg = "Unexpected null byte.";
    break;
  case ErrState::LEX_U_ILLEGAL_BYTE:
    msg = "Bytes 0xf8-0xff are illegal in UTF-8.";
    break;
  case ErrState::LEX_U_OVERLONG:
    msg = "Overlong UTF-8 encoding.";
    break;
  case ErrState::LEX_U_TOOHIGH:
    msg = "Unicode code points above U+10FFFF are invalid.";
    break;
  case ErrState::LEX_U_SURROGATE:
    msg = "Unicode code points U+D800 -- U+DFFF are invalid, as they correspond to UTF-16 surrogate pairs.";
    break;
  case ErrState::LEX_NONBMP_IDENTIFIER:
    msg = "Unicode code points above U+FFFF are not allowed in MySQL identifiers.";
    break;
  case ErrState::LEX_UNIMPLEMENTED_KEYWORD:
    msg = "Unimplemented keyword. If this was intended as an identifier, use backtick quotation.";
    break;
  case ErrState::LEX_TOO_LONG_IDENTIFIER:
    /*
     * MySQL will happily truncate an identifier that is too long, but does not
     * check that truncation happens at character boundaries. For identifiers
     * containing multi-byte UTF-8 sequences, such truncation can result in an
     * identifier with a name that is illegal UTF-8. We cannot allow such
     * identifiers since the REST server may need to return legal UTF-8. We also
     * cannot truncate in a "better" way than MySQL since we promise to either
     * produce a result equivalent with that produced by MySQL, or fail.
     * Therefore we have to fail, at least in some cases. We could check whether
     * truncation would result in legal UTF-8, but it is simpler both from
     * the implementer's and user's perspective to disallow all identifiers that
     * are too long.
     *
     * Note that MySQL allows for 256-byte aliases, but we restrict them to 64
     * bytes. It is simpler that way, as we allow only identifiers, not strings,
     * as aliases.
     */
    msg = "This identifier is too long. The limit is 64 bytes encoded as UTF-8.";
    break;
  case ErrState::LEX_INCOMPLETE_ESCAPE_SEQUENCE_IN_SINGLE_QUOTED_STRING:
    msg = "Incomplete escape sequence in single-quoted string";
    break;
  case ErrState::LEX_UNEXPECTED_EOI_IN_SINGLE_QUOTED_STRING:
    msg = "Unexpected end of input inside single-quoted string";
    break;
  case ErrState::LEX_ILLEGAL_TOKEN:
    msg = "Illegal token";
    break;
  case ErrState::LEX_UNEXPECTED_EOI_IN_QUOTED_IDENTIFIER:
    msg = "Unexpected end of input inside quoted identifier";
    break;
  case ErrState::LEX_U_ENC_ERR:
    msg = "Invalid UTF-8 encoding.";
    break;
  case ErrState::LEX_LITERAL_INTEGER_TOO_BIG:
    msg = "Literal integer too big.";
    break;
  case ErrState::LEX_LITERAL_FLOAT_INVALID:
    msg = "Invalid literal float.";
    break;
  case ErrState::TOO_LONG_UNALIASED_OUTPUT:
    msg = "Unaliased select expression too long. Use `AS` to add an alias no more than 64 bytes long.";
    break;
  case ErrState::INVALID_FRAGS_PER_WORKER:
    msg = "FRAGS_PER_WORKER must be a positive integer.";
    break;
  case ErrState::PARSER_ERROR:
    if (m_sql.len == 0)
    {
      err << "Syntax error in SQL statement: Empty input" << endl;
      print_statement = false;
    }
    else if (err_pos == m_sql.len)
    {
      msg = "Unexpected end of input";
    }
    else
    {
      msg = "Unexpected at this point";
    }
    break;
  default:
    abort();
  }
  if (print_statement)
  {
    /*
     * Explain the syntax error by showing the message followed by a print of
     * the SQL statement with the problematic section underlined with carets.
     */
    err << "Syntax error in SQL statement: " << msg << endl;
    size_t line_started_at = 0;
    for (size_t pos = 0; pos <= m_sql.len; pos++)
    {
      if (line_started_at == pos && pos < m_sql.len)
      {
        err << "> ";
      }
      char c = m_sql.str[pos];
      bool is_eol = c == '\n';
      if (pos == m_sql.len)
      {
        if (m_sql.str[pos-1] != '\n')
        {
          err << '\n';
          is_eol = true;
        }
        if (m_sql.str[pos-1] == '\n' && pos < err_stop)
        {
          err << "> \n";
          is_eol = true;
        }
      }
      else if ( c != '\r')
      {
        err << c;
      }
      if (is_eol &&
         err_pos <= pos &&
         line_started_at <= err_stop)
      {
        err << "! ";
        size_t err_marker_pos = line_started_at;
        // We use has_width to find the number of code points in the string
        // before and inside the error. This is a quite crude approximation of
        // the number of graphemes[†]. Thus, the error marker will be misaligned
        // whenever the number of graphemes do not match the number of code
        // points, e.g. when the string contains combining, zero-width or
        // control characters that are often used with emojis or with diacritics
        // that are unusual or NFD/NDKD normalized. This approximation is used
        // for the sake of simplicity and stability, as correctness is less
        // important in this case.
        // [†] https://unicode.org/glossary/#grapheme
        while (err_marker_pos < err_pos)
        {
          if (has_width(err_marker_pos))
          {
            err << " ";
          }
          err_marker_pos++;
        }
        while (err_marker_pos < err_stop &&
              (pos == err_pos
               ? err_marker_pos <= pos
               : err_marker_pos < pos))
        {
          if (has_width(err_marker_pos))
          {
            err << "^";
          }
          err_marker_pos++;
        }
        err << endl;
      }
      if (is_eol)
      {
        line_started_at = pos + 1;
      }
    }
  }
  throw RonSQLPermanentError("Syntax error.");
}

void
RonSQLPreparer::resolve_orderby_aliases()
{
  OrderbyColumns* ob = m_context.ast_root.orderby_columns;
  while (ob != NULL)
  {
    if (ob->kind == OrderbyColumns::Kind::TABLE_COLUMN)
    {
      Uint32 col_idx = ob->col_idx;
      // Only unqualified names can be aliases
      if (m_column_qualifiers.size() > col_idx &&
          m_column_qualifiers[col_idx].c_str() != NULL)
      {
        ob = ob->next;
        continue;
      }
      const char* name = m_columns[col_idx].c_str();
      // Walk SELECT outputs looking for a matching alias
      Uint32 output_pos = 0;
      Outputs* out = m_context.ast_root.outputs;
      while (out != NULL)
      {
        if (out->output_name.len > 0 && out->output_name.str != NULL &&
            strlen(name) == out->output_name.len &&
            strncmp(name, out->output_name.str,
                    out->output_name.len) == 0)
        {
          // Match found — convert to OUTPUT_REF
          ob->kind = OrderbyColumns::Kind::OUTPUT_REF;
          ob->output_idx = output_pos;
          // Mark this col_idx so load_join/load_single_table skips it —
          // UNLESS the matched output is a plain COLUMN spelled the
          // same way (e.g. `SELECT k ... ORDER BY k`, both bare): then
          // the ORDER BY name and the output share ONE registry entry,
          // which must still resolve as a real column for the output
          // itself.  The scoped resolver treats an alias-marked entry
          // as AliasOnly BEFORE attempting column resolution, so
          // marking a shared entry poisons the output's resolution
          // ("output is not a table or CTE column" in the pass-through
          // drain; found by po-1 on first record).  The single-table
          // resolver was already robust — it tries the real column
          // first and falls back to AliasOnly only on lookup failure.
          bool shares_entry_with_column_output =
              (out->type == Outputs::Type::COLUMN &&
               out->column.col_idx == col_idx);
          if (!shares_entry_with_column_output)
          {
            while (m_col_is_alias.size() <= col_idx)
              m_col_is_alias.push(false);
            m_col_is_alias[col_idx] = true;
          }
          break;
        }
        out = out->next;
        output_pos++;
      }
    }
    ob = ob->next;
  }
}

/*
 * Two resolved column references denote the same underlying column when
 * they landed on the same join-plan op and the same stored attribute
 * (or the same CTE result column).  Used to unify differently-spelled
 * references (bare vs qualified) after scoped resolution.
 */
bool
RonSQLPreparer::same_resolved_column(const QueryScope::ResolvedColumnRef& a,
                                     const QueryScope::ResolvedColumnRef& b)
{
  if (a.kind != b.kind || a.join_op_idx != b.join_op_idx)
    return false;
  if (a.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn)
    return a.attr_id == b.attr_id;
  if (a.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn)
    return a.cte_def_idx == b.cte_def_idx &&
           a.cte_result_idx == b.cte_result_idx;
  return false;
}

/*
 * The parser registers column references keyed on the (qualifier, name)
 * pair, so the same column reached through different spellings — bare
 * `c_nationkey` vs qualified `cu.c_nationkey` — carries different
 * col_idx values.  ResultPrinter::validate_orderby_columns matches
 * ORDER BY targets against GROUP BY by raw col_idx equality, so without
 * canonicalization a legal query like
 *   ... GROUP BY cu.c_nationkey ORDER BY c_nationkey
 * is wrongly rejected as "ORDER BY column not in GROUP BY clause".
 * Rewrite each TABLE_COLUMN ORDER BY col_idx to the GROUP BY entry's
 * col_idx when both resolve to the same underlying column.  Runs after
 * scoped resolution (which has already rejected ambiguous unqualified
 * names, so a resolved spelling is unique) and before ResultPrinter
 * construction.  Aliases were already converted to OUTPUT_REF by
 * resolve_orderby_aliases and are not affected.
 */
void
RonSQLPreparer::canonicalize_orderby_columns()
{
  OrderbyColumns* ob = m_context.ast_root.orderby_columns;
  GroupbyColumns* gb_head = m_context.ast_root.groupby_columns;
  QueryScope::ResolvedColumnRef* resolved = m_main_scope.resolved_columns;
  if (ob == NULL || gb_head == NULL || resolved == NULL)
    return;
  Uint32 num_cols = m_columns.size();
  for (; ob != NULL; ob = ob->next)
  {
    if (ob->kind != OrderbyColumns::Kind::TABLE_COLUMN)
      continue;
    Uint32 ob_idx = ob->col_idx;
    if (ob_idx >= num_cols)
      continue;
    bool literal_match = false;
    for (GroupbyColumns* gb = gb_head; gb != NULL; gb = gb->next)
    {
      if (gb->col_idx == ob_idx)
      {
        literal_match = true;
        break;
      }
    }
    if (literal_match)
      continue;
    const QueryScope::ResolvedColumnRef& obr = resolved[ob_idx];
    if (obr.kind != QueryScope::ResolvedColumnRef::Kind::StoredColumn &&
        obr.kind != QueryScope::ResolvedColumnRef::Kind::CteResultColumn)
      continue;
    for (GroupbyColumns* gb = gb_head; gb != NULL; gb = gb->next)
    {
      if (gb->col_idx >= num_cols)
        continue;
      if (same_resolved_column(obr, resolved[gb->col_idx]))
      {
        ob->col_idx = gb->col_idx;
        break;
      }
    }
  }
}

/*
 * Return false if the position is a UTF-8 continuation byte and part of a
 * prefix of a correct UTF-8 multi-byte sequence, otherwise true.
 */
bool
RonSQLPreparer::has_width(size_t pos)
{
  const char* s = m_sql.str;
  char c = s[pos];
  if ((c & 0xc0) != 0x80) return true;
  if (pos < 1) return true;
  c = s[pos - 1];
  if ((c & 0xe0) == 0xc0) return false;
  if ((c & 0xf0) == 0xe0) return false;
  if ((c & 0xf8) == 0xf0) return false;
  if ((c & 0xc0) != 0x80) return true;
  if (pos < 2) return true;
  c = s[pos - 2];
  if ((c & 0xf0) == 0xe0) return false;
  if ((c & 0xf8) == 0xf0) return false;
  if ((c & 0xc0) != 0x80) return true;
  if (pos < 3) return true;
  c = s[pos - 3];
  if ((c & 0xf8) == 0xf0) return false;
  return true;
}

void
RonSQLPreparer::load()
{
  DEB_TRACE();
  // W1: an index hint on a joined table is rejected up front (schema is not
  // needed for this check); only the main-query root scan honors a hint.
  reject_index_hints_on_joins(m_context.ast_root.joins);
  /*
   * During parsing, strings that were claimed to be column names were inserted
   * into m_columns. The element indexes in m_columns, usually called col_idx,
   * have already been used to construct Load instructions in m_main_scope.agg,
   * as well as the parse tree in ast_root. Now that parsing is done and we
   * know the table name, we look up the column descriptors in the schema and
   * populate m_main_scope.resolved_columns. The aggregation program can keep
   * col_idx references; emit resolves them through descriptors before talking
   * to NdbAggregator.
   */
  // Populate m_dict, m_main_scope.table and resolved column descriptors when
  // m_conf.ndb is available. If m_conf.ndb is not available, we'll still be
  // able to do a partial EXPLAIN SELECT, so no need to fail yet.
  Ndb* ndb = m_conf.ndb;
  if (ndb == NULL) return;

  // Populate m_dict
  m_dict = ndb->getDictionary();

  // Transform EXISTS subqueries into IN subqueries (may set m_has_subqueries)
  decorrelate_exists();

  // Transform correlated scalar subqueries (may set m_has_subqueries)
  decorrelate_scalar();

  // Phase I.17h: optional FROM clause at top-level SELECT.  When the
  // parser produced a NULL root_table, synthesise one from the
  // qualified column references in the SELECT — every distinct
  // qualifier that matches a scalar (no-GROUP-BY) CTE becomes part
  // of the synthetic FROM, with the first matched qualifier as root
  // and the rest as comma cross-joins.  Reject if no qualifier
  // matches a scalar CTE.
  synthesize_from_for_scalar_ctes();

  if (is_join_query()) {
    load_join();
  } else {
    load_single_table();
  }

  // Plan each CTE body into its own QueryScope. Main-query load runs first
  // so m_dict is populated; each CTE's planner call resolves CTE references
  // in its body against the CTEs declared before it (topological order).
  if (m_has_ctes) {
    build_cte_scopes();
    resolve_cte_output_columns();
  }
}

bool
RonSQLPreparer::is_join_query() const
{
  if (m_context.ast_root.joins != NULL) return true;
  // FROM <cte_name> with no joins: route through load_join so
  // QueryPlanner produces a CTE_SCAN root op and execute_join
  // drives result delivery.
  for (const CteDefinition* cte = m_context.ast_root.cte_list;
       cte != NULL; cte = cte->next) {
    if (strcmp(cte->name.c_str(),
               m_context.ast_root.table.c_str()) == 0) {
      return true;
    }
  }
  return false;
}

void
RonSQLPreparer::load_single_table()
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  // Populate m_main_scope.table
  m_main_scope.table = DBG(m_dict->getTable(DBG(m_context.ast_root.table.c_str())));
  require_prm(m_main_scope.table != NULL,
              "Failed to get table. Note that RonSQL only supports tables"
              " with ENGINE=NDB.");
  // Populate m_indexes — use schema cache if available to skip listIndexes()
  ndbrequire(m_dict != NULL);
  ndbrequire(m_main_scope.table != NULL);
  const char* db = m_conf.ndb ? m_conf.ndb->getDatabaseName() : nullptr;
  const auto* cached_indexes = (m_conf.schema_cache && db)
      ? m_conf.schema_cache->getIndexes(m_dict, m_main_scope.table, db,
                                         m_main_scope.table->getName())
      : nullptr;

  NdbDictionary::Dictionary::List index_list;
  if (cached_indexes == nullptr) {
    require_sch(m_dict->listIndexes(index_list, *m_main_scope.table) == 0,
                "Failed to list indexes.");
  }

  // Iterate: either cached index names or the raw list
  Uint32 idx_count = cached_indexes ? cached_indexes->size() : index_list.count;
  bool err_failed_to_get_index = false;
  bool err_object_status_not_retrieved = false;
  bool err_table_verid_mismatch = false;
  for (Uint32 i = 0; i < idx_count; i++) {
    const char* idx_name;
    NdbDictionary::Object::Type idx_type;
    NdbDictionary::Object::State idx_state;
    if (cached_indexes) {
      const auto& ci = (*cached_indexes)[i];
      idx_name = ci.name.c_str();
      idx_type = ci.type;
      idx_state = ci.state;
    } else {
      NdbDictionary::Dictionary::List::Element& elem = index_list.elements[i];
      idx_name = elem.name;
      idx_type = (NdbDictionary::Object::Type)elem.type;
      idx_state = elem.state;
    }
    if (idx_state != NdbDictionary::Object::StateOnline) {
      DEB_TRACE();
      continue;
    }
    if (idx_type == NdbDictionary::Object::UniqueHashIndex) {
      DEB_TRACE();
      continue;
    }
    require_bug(idx_type == NdbDictionary::Object::OrderedIndex,
                "Unexpected index type.");
    const NdbDictionary::Index* index = m_dict->getIndex(idx_name, *m_main_scope.table);
    if (index == NULL) {
      err_failed_to_get_index = true;
      DEB_TRACE();
      continue;
    }
    if (DBG(index->getObjectStatus()) !=
        NdbDictionary::Object::Status::Retrieved) {
      DEB_TRACE();
      err_object_status_not_retrieved = true;
    }
    if(DBG(index->getTableId()) != DBG(m_main_scope.table->getObjectId()) ||
       DBG(index->getTableVersion()) != DBG(m_main_scope.table->getObjectVersion())) {
      DEB_TRACE();
      err_table_verid_mismatch = true;
    }
    m_indexes.push(index);
  }
  require_sch(DBG(m_main_scope.table->getObjectStatus()) ==
              NdbDictionary::Object::Status::Retrieved,
              "Schema cache for table not up to date.");
  if (err_failed_to_get_index) {
    DEB_TRACE();
    throw RonSQLMaybeStaleSchema("Failed to get index.");
  }
  if (err_object_status_not_retrieved) {
    DEB_TRACE();
    throw RonSQLMaybeStaleSchema("Schema cache for index not up to date.");
  }
  if (err_table_verid_mismatch) {
    DEB_TRACE();
    throw RonSQLMaybeStaleSchema("Index's table id/version did not match"
                                 " table's object id/version.");
  }
  // Populate resolved_columns for the single-table path. Join queries use
  // resolve_columns_for_scope().
  QueryScope::ResolvedColumnRef* resolved =
      m_amalloc->alloc_exc<QueryScope::ResolvedColumnRef>(m_columns.size());
  for (Uint32 col_idx = 0; col_idx < m_columns.size(); col_idx++) {
    new (&resolved[col_idx]) QueryScope::ResolvedColumnRef();

    if (m_col_is_inner.size() > col_idx && m_col_is_inner[col_idx]) {
      continue;
    }
    const char* col_name = DBG(m_columns[DBG(col_idx)].c_str());
    const NdbDictionary::Column* col = m_main_scope.table->getColumn(col_name);
    if (col == NULL) {
      // An ORDER BY alias that shares col_idx with a real column would
      // have been found above.  A pure alias (no matching table column)
      // is harmless — skip it with a sentinel.
      if (m_col_is_alias.size() > col_idx && m_col_is_alias[col_idx]) {
        resolved[col_idx].kind =
            QueryScope::ResolvedColumnRef::Kind::AliasOnly;
        continue;
      }
      err << "Failed to get column " << quoted_identifier(col_name) << "."
          << endl << "Note that column names are case sensitive." << endl;
      DEB_TRACE();
      throw RonSQLMaybeStaleSchema("Could not find column (column names are"
                                   " case sensitive).");
    }
    resolved[col_idx].kind =
        QueryScope::ResolvedColumnRef::Kind::StoredColumn;
    resolved[col_idx].join_op_idx = 0;
    resolved[col_idx].attr_id = DBG(col->getAttrId());
    resolved[col_idx].dict_column = col;
  }
  m_main_scope.resolved_columns = resolved;
}

void
RonSQLPreparer::synthesize_from_for_scalar_ctes()
{
  if (m_context.ast_root.root_table != NULL) return;  // FROM given

  std::basic_ostream<char>& err = *m_conf.err_stream;

  // Walk every qualified column reference recorded by the parser and
  // pick the unique qualifiers that match a scalar (no-GROUP-BY) CTE
  // declared at the outer level.  Qualifiers that don't match any
  // outer-CTE name are left alone — they're typically subquery-local
  // and resolved inside their own scope.
  DynamicArray<LexCString> matched_qualifiers(m_amalloc);
  for (Uint32 i = 0; i < m_column_qualifiers.size(); i++) {
    const LexCString& q = m_column_qualifiers[i];
    if (q.c_str() == NULL) continue;
    bool already = false;
    for (Uint32 j = 0; j < matched_qualifiers.size(); j++) {
      if (strcmp(matched_qualifiers[j].c_str(), q.c_str()) == 0) {
        already = true;
        break;
      }
    }
    if (already) continue;
    const CteDefinition* match = NULL;
    for (const CteDefinition* cte = m_context.ast_root.cte_list;
         cte != NULL; cte = cte->next) {
      if (strcmp(cte->name.c_str(), q.c_str()) == 0) {
        match = cte;
        break;
      }
    }
    if (match == NULL) continue;  // Not an outer-CTE qualifier; skip.
    if (match->stmt->groupby_columns != NULL ||
        match->stmt->is_single_row_cte) {
      err << "Column qualifier '" << q.c_str()
          << "' refers to a "
          << (match->stmt->is_single_row_cte ? "single-row" : "grouped")
          << " CTE.  SELECT without an explicit FROM clause supports "
             "only scalar (aggregate-only) CTEs; add an explicit "
             "comma cross join instead (FROM t, "
          << q.c_str() << ")." << std::endl;
      throw RonSQLPermanentError(
          "Non-scalar CTE referenced with no FROM clause.");
    }
    matched_qualifiers.push(q);
  }

  if (matched_qualifiers.size() == 0) {
    err << "SELECT without FROM clause requires at least one "
           "qualified column reference (e.g. cte_name.col) to a "
           "scalar CTE." << std::endl;
    throw RonSQLPermanentError(
        "SELECT without FROM and no scalar-CTE qualifier.");
  }

  // First matched qualifier becomes the synthetic root_table; the
  // rest become comma cross-join clauses (no ON conditions).  Each
  // synthetic JoinClause / TableRef is allocated from the request
  // arena so its lifetime matches the rest of the AST.  At
  // emit_child_ops time, scalar CTE cross-join children take the
  // dummy-key + setParent path patterned on testCteNdbApi.cpp
  // Test 20.
  TableRef* root = m_amalloc->alloc_exc<TableRef>(1);
  root->database = LexCString{NULL, 0};
  root->name = matched_qualifiers[0];
  root->alias = matched_qualifiers[0];
  m_context.ast_root.root_table = root;
  m_context.ast_root.table = matched_qualifiers[0];

  JoinClause* head = NULL;
  JoinClause* tail = NULL;
  for (Uint32 i = 1; i < matched_qualifiers.size(); i++) {
    JoinClause* jc = m_amalloc->alloc_exc<JoinClause>(1);
    jc->join_type = JoinClause::INNER_JOIN;
    jc->table.database = LexCString{NULL, 0};
    jc->table.name = matched_qualifiers[i];
    jc->table.alias = matched_qualifiers[i];
    jc->conditions = NULL;
    jc->next = NULL;
    if (head == NULL) {
      head = tail = jc;
    } else {
      tail->next = jc;
      tail = jc;
    }
  }
  m_context.ast_root.joins = head;
}

const CteDefinition*
RonSQLPreparer::find_cte_definition(const LexCString& name) const
{
  for (const CteDefinition* c = m_context.ast_root.cte_list;
       c != NULL; c = c->next) {
    if (strcmp(c->name.c_str(), name.c_str()) == 0) return c;
  }
  return NULL;
}

bool
RonSQLPreparer::cte_key_coverage(
    const CteDefinition* cte,
    const LexCString* bound_cte_side_names,
    Uint32 num_keys,
    CteKeyCoverageResult& out) const
{
  out.state = CteKeyCoverage::WrongColumns;
  out.num_keys = num_keys;
  out.num_pk_cols = 0;
  out.first_wrong_key = 0;
  out.first_missing_pk = 0;
  for (Uint32 i = 0; i < MAX_JOIN_KEY_COLS; i++) {
    out.pk_index_for_key[i] = -1;
    out.pk_covered[i] = false;
  }
  if (cte == NULL || cte->stmt == NULL) return false;

  /* Single-row CTE (cte_single_row_kernel_plan.md): keys may bind ANY
   * SUBSET of the outputs, including none (comma cross join =
   * existence probe).  pk_index_for_key[k] carries the OUTPUT
   * POSITION each key binds, in the caller's key order — passed to
   * NdbQueryOptions::setCteKeyColumns at emit.  An unknown name is
   * still WrongColumns. */
  if (cte->stmt->is_single_row_cte) {
    bool wrong = false;
    for (Uint32 k = 0; k < num_keys; k++) {
      int pos = -1;
      Uint32 oi = 0;
      for (const Outputs* o = cte->stmt->outputs; o != NULL;
           o = o->next, oi++) {
        if (bound_cte_side_names[k] ==
            o->output_name.to_LexCString(m_amalloc)) {
          pos = (int)oi;
          break;
        }
      }
      out.pk_index_for_key[k] = pos;
      if (pos < 0) {
        if (!wrong) out.first_wrong_key = k;
        wrong = true;
      }
    }
    out.state = wrong ? CteKeyCoverage::WrongColumns
                      : CteKeyCoverage::SingleRowSubset;
    return true;
  }

  LexCString pk_names[MAX_JOIN_KEY_COLS];
  for (const GroupbyColumns* gb = cte->stmt->groupby_columns;
       gb != NULL; gb = gb->next) {
    if (out.num_pk_cols >= MAX_JOIN_KEY_COLS) return false;
    const Outputs* match = NULL;
    for (const Outputs* o = cte->stmt->outputs; o != NULL; o = o->next) {
      if (o->type == Outputs::Type::COLUMN &&
          o->column.col_idx == gb->col_idx) {
        match = o;
        break;
      }
    }
    if (match == NULL) return false;
    pk_names[out.num_pk_cols] =
        match->output_name.to_LexCString(m_amalloc);
    out.num_pk_cols++;
  }

  if (out.num_pk_cols == 0 && num_keys == 0) {
    out.state = CteKeyCoverage::ScalarDummy;
    return true;
  }

  bool wrong_column = false;
  for (Uint32 k = 0; k < num_keys; k++) {
    int pk_idx = -1;
    for (Uint32 p = 0; p < out.num_pk_cols; p++) {
      if (bound_cte_side_names[k] == pk_names[p]) {
        pk_idx = (int)p;
        break;
      }
    }
    out.pk_index_for_key[k] = pk_idx;
    if (pk_idx < 0) {
      if (!wrong_column) out.first_wrong_key = k;
      wrong_column = true;
    } else {
      out.pk_covered[pk_idx] = true;
    }
  }
  if (wrong_column) {
    out.state = CteKeyCoverage::WrongColumns;
    return true;
  }

  bool all_pk_covered = (num_keys == out.num_pk_cols);
  for (Uint32 p = 0; p < out.num_pk_cols; p++) {
    if (!out.pk_covered[p]) {
      all_pk_covered = false;
      out.first_missing_pk = p;
      break;
    }
  }
  if (!all_pk_covered) {
    out.state = CteKeyCoverage::Partial;
    return true;
  }

  bool ordered = true;
  for (Uint32 k = 0; k < num_keys; k++) {
    if (out.pk_index_for_key[k] != (int)k) {
      ordered = false;
      break;
    }
  }
  out.state = ordered ? CteKeyCoverage::ExactOrdered
                      : CteKeyCoverage::ExactPermuted;
  return true;
}

bool
RonSQLPreparer::cte_key_coverage(
    const CteDefinition* cte,
    const char* const* bound_cte_side_names,
    Uint32 num_keys,
    CteKeyCoverageResult& out) const
{
  LexCString names[MAX_JOIN_KEY_COLS];
  for (Uint32 i = 0; i < num_keys; i++) {
    names[i] = LexCString(bound_cte_side_names[i],
                          strlen(bound_cte_side_names[i]));
  }
  return cte_key_coverage(cte, names, num_keys, out);
}

bool
RonSQLPreparer::join_conditions_reference_only_parent(
    const JoinCondition* conditions,
    const LexCString& parent_alias) const
{
  for (const JoinCondition* jc = conditions; jc != NULL; jc = jc->next) {
    if (!(jc->parent_table == parent_alias)) return false;
  }
  return true;
}

void
RonSQLPreparer::reorder_cte_join_conditions_to_pk_order(
    JoinClause* join,
    const CteKeyCoverageResult& r)
{
  JoinCondition* by_pk[MAX_JOIN_KEY_COLS];
  for (Uint32 i = 0; i < MAX_JOIN_KEY_COLS; i++) by_pk[i] = NULL;

  JoinCondition* jc = join->conditions;
  for (Uint32 k = 0; k < r.num_keys; k++) {
    ndbrequire(jc != NULL);
    int pk_idx = r.pk_index_for_key[k];
    ndbrequire(pk_idx >= 0 && (Uint32)pk_idx < r.num_pk_cols);
    by_pk[pk_idx] = jc;
    jc = jc->next;
  }

  JoinCondition* head = NULL;
  JoinCondition* tail = NULL;
  for (Uint32 p = 0; p < r.num_pk_cols; p++) {
    ndbrequire(by_pk[p] != NULL);
    if (head == NULL) {
      head = tail = by_pk[p];
    } else {
      tail->next = by_pk[p];
      tail = by_pk[p];
    }
  }
  tail->next = NULL;
  join->conditions = head;
}

void
RonSQLPreparer::normalize_cte_join_key_order()
{
  for (JoinClause* join = m_context.ast_root.joins;
       join != NULL; join = join->next) {
    const CteDefinition* child_cte = find_cte_definition(join->table.name);
    if (child_cte == NULL || join->conditions == NULL) continue;

    LexCString child_names[MAX_JOIN_KEY_COLS];
    Uint32 num_keys = 0;
    for (JoinCondition* jc = join->conditions;
         jc != NULL; jc = jc->next) {
      if (num_keys >= MAX_JOIN_KEY_COLS) break;
      child_names[num_keys++] = jc->child_column;
    }
    CteKeyCoverageResult coverage;
    if (!cte_key_coverage(child_cte, child_names, num_keys, coverage))
      continue;
    if (coverage.state == CteKeyCoverage::ExactPermuted) {
      reorder_cte_join_conditions_to_pk_order(join, coverage);
    }
  }
}

/*
 * Phase I.16b / I.16c: detect a partial-key INNER join to a multi-key
 * CTE somewhere in the join chain and rewrite the AST so the CTE
 * becomes the joined root.  After the swap the planner produces a
 * CTE_SCAN root with the original parent as a child operation via its
 * existing PK / unique / index lookup logic; the remaining joins keep
 * their alias-based parent references and resolve unchanged.
 *
 * Applies when:
 *   - the original root is a real table (not a CTE)
 *   - the multikey-CTE-join is INNER (LEFT_OUTER would change
 *     semantics under the swap — bail to I.16a's clean reject)
 *   - the CTE body has GROUP BY with N columns and the join binds
 *     fewer than N column-pairs (would otherwise hit I.16a)
 *
 * Other joins in the chain may be INNER or LEFT_OUTER and any count.
 * Non-root CTE_SCAN child shapes are out of scope — scanCte() in the
 * NDB API doesn't take a key array, so RonSQL follows what the API
 * supports.
 *
 * Called from two sites (non_aggregate_phase_6.md): load_join() (the
 * original aggregate-path timing, byte-identical behavior) and, for
 * non-aggregate queries, parse() BEFORE the projection-only gate — the
 * gate's complete-key CTE coverage check then evaluates the
 * post-rewrite tree (a CTE_SCAN root + real chain, the shipped gc-9
 * shape needing no gate relaxation).  The second invocation is a
 * natural no-op: post-rewrite the root IS the CTE, so the root_is_cte
 * bail fires.  Pure AST + m_amalloc work, safe at parse time
 * (cte_key_coverage is AST-only; the gate already calls it at parse
 * time).
 */
void
RonSQLPreparer::maybe_rewrite_partial_key_cte_root()
{
  if (m_context.ast_root.root_table != NULL &&
      m_context.ast_root.joins != NULL)
  {
    // Bail if the original root is itself a CTE.
    bool root_is_cte = false;
    for (const CteDefinition* c = m_context.ast_root.cte_list;
         c != NULL; c = c->next) {
      if (strcmp(c->name.c_str(),
                 m_context.ast_root.root_table->name.c_str()) == 0) {
        root_is_cte = true;
        break;
      }
    }
    if (!root_is_cte) {
      LexCString original_root_alias = m_context.ast_root.root_table->alias;
      // Walk the joins list looking for a partial-key INNER join to a
      // multi-key CTE.  Track the previous JoinClause so we can splice
      // the matched one out.
      JoinClause* prev = NULL;
      JoinClause* cur = m_context.ast_root.joins;
      JoinClause* match = NULL;
      JoinClause* match_prev = NULL;
      while (cur != NULL) {
        if (cur->join_type == JoinClause::INNER_JOIN) {
          const CteDefinition* child_cte =
              find_cte_definition(cur->table.name);
          if (child_cte != NULL) {
            LexCString child_names[MAX_JOIN_KEY_COLS];
            Uint32 join_cols = 0;
            for (const JoinCondition* jc = cur->conditions;
                 jc != NULL && join_cols < MAX_JOIN_KEY_COLS;
                 jc = jc->next) {
              child_names[join_cols++] = jc->child_column;
            }
            CteKeyCoverageResult coverage;
            if (cte_key_coverage(child_cte, child_names, join_cols,
                                 coverage) &&
                coverage.state == CteKeyCoverage::Partial &&
                join_conditions_reference_only_parent(
                    cur->conditions, original_root_alias)) {
              match = cur;
              match_prev = prev;
              break;
            }
          }
        }
        prev = cur;
        cur = cur->next;
      }
      if (match != NULL) {
        // Splice match out of the joins list.
        if (match_prev == NULL) {
          m_context.ast_root.joins = match->next;
        } else {
          match_prev->next = match->next;
        }
        match->next = NULL;
        // Build a new JoinClause carrying the original root as a
        // child of the CTE: same conditions as match, flipped.
        JoinClause* new_jc = m_amalloc->alloc_exc<JoinClause>(1);
        new_jc->join_type = JoinClause::INNER_JOIN;
        new_jc->table = *m_context.ast_root.root_table;
        // A root index hint does not survive the demotion: on the
        // aggregate path reject_index_hints_on_joins already ran (the
        // hint was silently dropped — nothing reads join-clause hints
        // after it); on the pre-gate parse() call the reject runs
        // later and would otherwise newly trip on the demoted copy.
        // Clearing keeps both paths at the established behavior.
        new_jc->table.hint_kind = TableRef::HintKind::HINT_NONE;
        new_jc->table.hint_indexes = NULL;
        new_jc->conditions = match->conditions;
        for (JoinCondition* jc = new_jc->conditions;
             jc != NULL; jc = jc->next) {
          LexCString tmp_table = jc->child_table;
          jc->child_table = jc->parent_table;
          jc->parent_table = tmp_table;
          LexCString tmp_col = jc->child_column;
          jc->child_column = jc->parent_column;
          jc->parent_column = tmp_col;
        }
        // Insert new_jc at the head of the joins list (so the
        // original root resolves before any later join referencing
        // it as a parent alias).
        new_jc->next = m_context.ast_root.joins;
        m_context.ast_root.joins = new_jc;
        // Promote the matched CTE TableRef to root.  TableRef in
        // the AST root is a pointer; copy the value in.
        *m_context.ast_root.root_table = match->table;
        // Note: ast_root.table (the FROM name LexString set once by
        // the parser) deliberately keeps the pre-rewrite name — the
        // shipped aggregate-path rewrite always left it stale, and
        // its consumers (get_table_name(), ParseOnly) read the
        // original FROM name.
      }
    }
  }
}

/* Single-row key-lookup CTE bodies (cte_single_row_kernel_plan.md).
 *
 * A CTE body with no aggregate functions and no GROUP BY is accepted
 * when it is a single-row key lookup: a single-table SELECT of plain
 * columns whose WHERE binds every primary key column of the source
 * table by equality with a constant.  That guarantee is checked at
 * plan time in enforce_single_row_cte_body() — the dictionary is not
 * loaded yet here — but candidacy must be marked at parse time,
 * before the projection-only gate evaluates CTE key coverage.
 *
 * NO AST rewrite: the CTE is emitted with the kernel's CTE_SINGLE_ROW
 * mode — every projected column becomes a GROUP BY column with zero
 * aggregate slots (the materialized "group" IS the row), and
 * consumers bind ANY SUBSET of the outputs via subset-key CTE_LOOKUPs
 * (including none: the comma cross join is a pure existence probe
 * with exact MySQL empty-CTE semantics).
 *
 * Candidacy is pure AST; non-candidates fall through to the existing
 * analyze_ctes() rejections unchanged (gc-P3 / cs-probe-5 baselines
 * stay byte-identical).  Duplicate output columns are excluded: two
 * outputs sharing one source column would need distinct virtual
 * columns backed by the same stored key entry (virt-PK aliasing,
 * deferred).
 */
void
RonSQLPreparer::detect_single_row_ctes()
{
  for (CteDefinition* cte = m_context.ast_root.cte_list;
       cte != NULL; cte = cte->next)
  {
    SelectStatement* stmt = cte->stmt;
    if (stmt->groupby_columns != NULL) continue;
    if (stmt->agg != NULL) continue;  // body parsed aggregate/arith exprs
    if (stmt->having_expression != NULL) continue;
    if (stmt->joins != NULL) continue;
    if (stmt->where_expression == NULL) continue;
    if (stmt->root_table == NULL) continue;
    // Chained CTE bodies (FROM an earlier CTE) stay on the
    // aggregating envelope.
    if (find_cte_definition(stmt->root_table->name) != NULL) continue;
    bool all_plain_columns = (stmt->outputs != NULL);
    for (const Outputs* o = stmt->outputs;
         o != NULL && all_plain_columns; o = o->next)
    {
      if (o->type != Outputs::Type::COLUMN)
        all_plain_columns = false;
    }
    if (!all_plain_columns) continue;
    bool has_duplicate = false;
    for (const Outputs* o = stmt->outputs;
         o != NULL && !has_duplicate; o = o->next)
      for (const Outputs* p = o->next; p != NULL; p = p->next)
        if (p->column.col_idx == o->column.col_idx)
        {
          has_duplicate = true;
          break;
        }
    if (has_duplicate) continue;
    // Subset keys are capped by the wire limit; more outputs than
    // that is fine (consumers just can't bind them all at once), but
    // the emit-side key count is checked per consumer join anyway
    // (MAX_JOIN_KEY_COLS <= QN_CteLookupNode::MaxKeyPositions).
    stmt->is_single_row_cte = true;
  }
}

/*
 * Phase i26: register linked projections for the ancestor real-table
 * columns referenced by CTE-op jump-table filters
 * (JoinOp::needs_parent_linked_attrs, set by classify_where_by_table's
 * routing).  Deliberately NOT done at classification time: the
 * aggregation program's GroupByLinked positions are a sequential
 * counter over the GROUP BY list, which requires GROUP BY linked
 * projections to occupy the first slots of plan.linked_projs — this
 * pass therefore runs after load_join's GB block and dedups via
 * find_or_add_linked_proj.  Walks every identifier in the routed
 * filter CE; StoredColumn refs on other ops are ancestors by the
 * routing gate's construction.
 */
void
RonSQLPreparer::register_cte_filter_linked_projs(QueryScope& scope)
{
  if (scope.resolved_columns == NULL) return;
  JoinPlan& plan = scope.join_plan;
  for (Uint32 i = 1; i < plan.num_ops; i++) {
    if (!plan.ops[i].needs_parent_linked_attrs) continue;
    ConditionalExpression* ce = scope.join_where_ce[i];
    if (ce == NULL) continue;
    std::function<void(ConditionalExpression*)> walk =
        [&](ConditionalExpression* e) {
      if (e == NULL) return;
      if (e->op == T_IDENTIFIER) {
        const QueryScope::ResolvedColumnRef& r =
            scope.resolved_columns[e->col_idx];
        if (r.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn &&
            r.join_op_idx != i) {
          find_or_add_linked_proj(plan, r.join_op_idx,
                                  m_columns[e->col_idx].c_str());
        }
        return;
      }
      switch (e->op) {
      case T_AND: case T_OR:
      case T_EQUALS: case T_NOT_EQUALS:
      case T_LT: case T_LE: case T_GT: case T_GE:
        walk(e->args.left);
        walk(e->args.right);
        return;
      case T_IS:
        walk(e->is.arg);
        return;
      default:
        return;  // constants and anything else carry no column refs
      }
    };
    walk(ce);
  }
}

void
RonSQLPreparer::load_join()
{
  std::basic_ostream<char>& err = *m_conf.err_stream;

  // No-op for non-aggregate queries — parse() already ran the rewrite
  // ahead of the projection-only gate; for aggregate queries this is
  // the original (byte-identical) timing.
  maybe_rewrite_partial_key_cte_root();
  normalize_cte_join_key_order();

  // Build the join plan via QueryPlanner
  const char* db = m_conf.ndb ? m_conf.ndb->getDatabaseName() : nullptr;
  QueryPlanner::plan(
      m_context.ast_root.root_table,
      m_context.ast_root.joins,
      m_dict,
      err,
      m_main_scope.join_plan,
      m_conf.schema_cache,
      db,
      m_context.ast_root.cte_list);

  // For multi-leaf subquery pushdown, map merged leaves to plan op indices.
  // Each merged leaf corresponds to a JoinClause that was appended to the
  // join list, and QueryPlanner assigned it an operation index.
  if (m_has_select_subqueries) {
    // The injected joins were appended after any pre-existing joins.
    // With no pre-existing joins (outer query is single-table + subqueries),
    // the first injected join is op index 1, second is 2, etc.
    Uint32 first_injected_op = m_main_scope.join_plan.num_ops - m_merged_leaves.size();
    for (Uint32 i = 0; i < m_merged_leaves.size(); i++) {
      m_merged_leaves[i].plan_op_idx = first_injected_op + i;
      m_main_scope.join_plan.agg_leaf_indices[i] = first_injected_op + i;
    }
    m_main_scope.join_plan.num_agg_leaves = m_merged_leaves.size();
  }

  // Set m_main_scope.table to root table (used by existing code paths)
  m_main_scope.table = m_main_scope.join_plan.ops[0].table;

  resolve_columns_for_scope(m_main_scope, m_context.ast_root, true);

  // Classify WHERE conditions by table for per-table filter pushdown
  classify_where_by_table(m_main_scope,
                          m_context.ast_root.where_expression);

  // Promote LEFT OUTER children to INNER when WHERE rejects NULL on
  // their column — see cte_filter_phase_j.md.
  promote_left_to_inner_for_where(m_main_scope);

  // Convert cross-table WHERE filters to index range bounds where possible
  assign_child_index_bounds(m_main_scope);

  // Apply inner subquery filters to the corresponding leaf operations
  if (m_has_select_subqueries) {
    for (Uint32 i = 0; i < m_merged_leaves.size(); i++) {
      MergedLeaf &ml = m_merged_leaves[i];
      SelectSubqueryLeaf &base = m_select_subquery_leaves[ml.first_subquery_idx];
      if (base.inner_filter != NULL) {
        Uint32 op_idx = ml.plan_op_idx;
        if (m_main_scope.join_where_ce[op_idx] == NULL) {
          m_main_scope.join_where_ce[op_idx] = base.inner_filter;
        } else {
          ConditionalExpression* combined =
              m_amalloc->alloc_exc<ConditionalExpression>(1);
          combined->op = T_AND;
          combined->args.left = m_main_scope.join_where_ce[op_idx];
          combined->args.right = base.inner_filter;
          m_main_scope.join_where_ce[op_idx] = combined;
        }
      }
    }
  }

  // Build linked projections for GROUP BY columns on non-leaf tables.
  // INVARIANT: this block must stay the FIRST producer of
  // plan.linked_projs — the aggregation program's GroupByLinked
  // positions are emitted as a sequential counter over the GROUP BY
  // list, so GB projections must occupy slots 0..n-1.  Every other
  // producer (build_agg_linked_projections' loads / cross-table /
  // CASE columns, register_cte_filter_linked_projs) runs later and
  // dedups via find_or_add_linked_proj.
  require_run(m_main_scope.resolved_columns != NULL,
              "GROUP BY linked projection setup: missing resolved columns.");
  Uint32 leaf_idx = m_main_scope.join_plan.agg_leaf_idx;
  struct GroupbyColumns* groupby = m_context.ast_root.groupby_columns;
  while (groupby != NULL)
  {
    Uint32 col_idx = groupby->col_idx;
    const QueryScope::ResolvedColumnRef& col_ref =
        m_main_scope.resolved_columns[col_idx];
    if (col_ref.join_op_idx != leaf_idx)
    {
      require_prm(m_main_scope.join_plan.num_linked_projs < MAX_LINKED_PROJS,
                  "Too many linked projections.");
      JoinPlan::LinkedProj& lp =
          m_main_scope.join_plan.linked_projs[m_main_scope.join_plan.num_linked_projs];
      lp.source_op_idx = col_ref.join_op_idx;
      lp.column_name = m_columns[col_idx].c_str();
      m_main_scope.join_plan.num_linked_projs++;
    }
    groupby = groupby->next;
  }

  // Phase i26: register linked projections for ancestor real-table
  // columns referenced by CTE-op filters (needs_parent_linked_attrs,
  // set by classify_where_by_table).  Must run AFTER the GB block
  // above so GB projections keep slots 0..n-1 (find_or_add dedups
  // against them); emit_cte_lookup_filter later resolves the same
  // (op, name) pairs find-only.
  register_cte_filter_linked_projs(m_main_scope);

  // Root scan-config selection (join_root_index_scan_plan.md): consider
  // an ordered-index scan for the main-query root when the
  // root-classified WHERE conjuncts (join_where_ce[0], final at this
  // point) can serve as index bounds — e.g. a range on the PK ordered
  // index or an equality on a secondary-indexed column, shapes
  // emit_root_op's PK-equality branches cannot serve.  Skipped when the
  // root PK is fully equality-covered (readTuple / PK-equality index
  // scan are better) unless a FORCE INDEX hint demands an index scan.
  // This also makes root index hints on join queries effective — they
  // were previously parsed but silently ignored because
  // plan_index_and_filter never runs for join queries.
  {
    const TableRef* root_ref = m_context.ast_root.root_table;
    const TableRef::HintKind root_hint =
        (root_ref != NULL) ? root_ref->hint_kind
                           : TableRef::HintKind::HINT_NONE;
    JoinPlan& jp = m_main_scope.join_plan;
    if (jp.ops[0].type == JoinOp::TABLE_SCAN &&
        jp.ops[0].table != NULL &&
        (root_hint == TableRef::HintKind::HINT_FORCE ||
         !root_pk_equality_covered(m_main_scope)))
    {
      select_root_scan_config(m_main_scope,
                              m_main_scope.join_where_ce[0],
                              root_ref);
    }
  }
}

void
RonSQLPreparer::classify_where_by_table(QueryScope& scope,
                                         ConditionalExpression* where_ce)
{
  for (Uint32 t = 0; t < MAX_SPJ_TREE_NODES; t++)
    scope.join_where_ce[t] = NULL;

  if (where_ce == NULL) return;

  // Flatten top-level AND conjuncts
  ConditionalExpression* conjuncts[MAX_WHERE_CONJUNCTS];
  Uint32 num_conjuncts = 0;
  flatten_and_conjuncts(where_ce, conjuncts, &num_conjuncts);

  // Classify each conjunct by table
  for (Uint32 i = 0; i < num_conjuncts; i++)
  {
    Int32 table_idx = classify_ce_table_resolved(scope, conjuncts[i]);

    if (table_idx == -2)
    {
      // Cross-table condition.  For simple comparisons (col OP col) between
      // two tables, collect as a cross-table filter for BranchReg handling
      // in the aggregation program.  Complex cross-table expressions
      // (OR branches, arithmetic involving multiple tables) are rejected.
      ConditionalExpression* ce = conjuncts[i];

      // Flatten OR into atoms (single comparison = 1 atom).
      ConditionalExpression* or_atoms[32];
      Uint32 num_atoms = 0;
      std::function<void(ConditionalExpression*)> flatten_or =
          [&](ConditionalExpression* node) {
        if (node->op == T_OR) {
          flatten_or(node->args.left);
          flatten_or(node->args.right);
        } else if (num_atoms < 32) {
          or_atoms[num_atoms++] = node;
        }
      };
      flatten_or(ce);

      // Validate each atom: must be a comparison where each side
      // references at most one table, expression depth within limits,
      // and the union of all referenced tables is at most 2 distinct tables.
      Int32 tables_seen[2] = {-1, -1};
      Uint32 num_tables = 0;
      bool valid = true;
      for (Uint32 a = 0; a < num_atoms && valid; a++) {
        ConditionalExpression* atom = or_atoms[a];
        bool is_cmp =
            (atom->op == T_EQUALS || atom->op == T_NOT_EQUALS ||
             atom->op == T_LT || atom->op == T_LE ||
             atom->op == T_GT || atom->op == T_GE) &&
            atom->args.left != NULL && atom->args.right != NULL;
        if (!is_cmp) { valid = false; break; }

        Int32 lt = classify_ce_table_resolved(scope, atom->args.left);
        Int32 rt = classify_ce_table_resolved(scope, atom->args.right);
        if (lt == -2 || rt == -2) { valid = false; break; }
        if (filter_expr_reg_depth(atom->args.left) > 2 ||
            filter_expr_reg_depth(atom->args.right) > 2) { valid = false; break; }

        // Track distinct tables across all atoms
        Int32 sides[2] = {lt, rt};
        for (int s = 0; s < 2; s++) {
          if (sides[s] == -1) continue;  // constant
          bool found = false;
          for (Uint32 t = 0; t < num_tables; t++) {
            if (tables_seen[t] == sides[s]) { found = true; break; }
          }
          if (!found) {
            if (num_tables >= 2) { valid = false; break; }
            tables_seen[num_tables++] = sides[s];
          }
        }
      }

      // Must involve exactly 2 distinct tables overall
      if (!valid || num_tables < 2) {
        throw RonSQLPermanentError(
            "WHERE condition references columns from multiple tables in a "
            "single comparison or OR branch. Each OR branch must be a simple "
            "comparison between the same two tables.");
      }

      Uint32 child_t = ((Uint32)tables_seen[0] > (Uint32)tables_seen[1]) ?
                        (Uint32)tables_seen[0] : (Uint32)tables_seen[1];
      Uint32 parent_t = ((Uint32)tables_seen[0] > (Uint32)tables_seen[1]) ?
                         (Uint32)tables_seen[1] : (Uint32)tables_seen[0];

      /*
       * Phase i26 (cte_filter_phase_i26.md): a conjunct comparing this
       * CTE_LOOKUP op's outputs against a TREE-ANCESTOR real-table
       * column (`t.col > s.m`, the watermark shape) routes to the CTE
       * op's jump-table filter instead of the cross-table machinery.
       * The ancestor columns ride the linked-attr buffer as linked
       * parent projections: DBSPJ expands them per parent row, DBLQH
       * prepends them ahead of the CTE outputs, and the filter reads
       * them by projection index.  v1 gate:
       *  - the higher op is a CTE_LOOKUP and the other op is a tree
       *    ancestor of it (sibling branches stay cross-table);
       *  - every atom side is a bare identifier or constant (already
       *    validated as simple comparisons above; arithmetic falls
       *    back to the cross-table path);
       *  - projection bookkeeping is safe: no multi-leaf aggregation,
       *    and any single-leaf aggregator sits on this CTE op — the
       *    plan's linked_projs list is shared, so an aggregator on a
       *    DIFFERENT op would get the filter's projections attached
       *    and its buffer positions would shift.
       */
      {
        JoinPlan& plan = scope.join_plan;
        bool routable =
            (&scope == &m_main_scope &&  // CTE-body scopes deferred
             scope.resolved_columns != NULL &&
             child_t < plan.num_ops && parent_t < plan.num_ops &&
             plan.ops[child_t].type == JoinOp::CTE_LOOKUP &&
             child_t != parent_t &&
             linked_source_is_leaf_ancestor(plan, child_t, parent_t) &&
             plan.num_agg_leaves == 0 &&
             (scope.agg == NULL || plan.agg_leaf_idx == child_t));
        if (routable) {
          for (Uint32 a = 0; a < num_atoms && routable; a++) {
            ConditionalExpression* atom = or_atoms[a];
            ConditionalExpression* atom_sides[2] = {atom->args.left,
                                                    atom->args.right};
            for (int s2 = 0; s2 < 2 && routable; s2++) {
              ConditionalExpression* e = atom_sides[s2];
              if (e->op == T_IDENTIFIER || e->op == T_INT ||
                  e->op == T_FLOAT || e->op == T_STRING ||
                  e->op == T_NULL) {
                continue;
              }
              routable = false;
            }
          }
        }
        if (routable) {
          // Only mark + route here.  The ancestor columns' linked
          // projections are registered LATER, by
          // register_cte_filter_linked_projs after load_join's
          // GROUP BY linked-projection block: the aggregation
          // program's GroupByLinked positions are a sequential
          // counter over the GROUP BY list, which requires GB
          // projections to occupy the FIRST slots of
          // plan.linked_projs — registering filter columns here
          // (before the GB pass) shifted them and the target
          // aggregator read the wrong buffer entries (the sc-17
          // VARCHAR GB-key crash on first record).
          plan.ops[child_t].needs_parent_linked_attrs = true;
          if (scope.join_where_ce[child_t] == NULL) {
            scope.join_where_ce[child_t] = ce;
          } else {
            ConditionalExpression* combined =
                m_amalloc->alloc_exc<ConditionalExpression>(1);
            combined->op = T_AND;
            combined->args.left = scope.join_where_ce[child_t];
            combined->args.right = ce;
            scope.join_where_ce[child_t] = combined;
          }
          continue;
        }
      }

      CrossTableFilter ctf;
      ctf.ce = ce;
      ctf.child_table_idx = child_t;
      ctf.parent_table_idx = parent_t;
      scope.cross_table_where_filters.push(ctf);
      continue;
    }

    // Constant-only conditions: assign to root table
    if (table_idx == -1)
      table_idx = 0;

    // Accumulate conditions for this table
    if (scope.join_where_ce[table_idx] == NULL)
    {
      scope.join_where_ce[table_idx] = conjuncts[i];
    }
    else
    {
      // Build AND(existing, new_conjunct)
      ConditionalExpression* combined =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
      combined->op = T_AND;
      combined->args.left = scope.join_where_ce[table_idx];
      combined->args.right = conjuncts[i];
      scope.join_where_ce[table_idx] = combined;
    }
  }
}

// Predicate is null-rejecting if it evaluates to FALSE/NULL whenever
// any of its column references is NULL.  Used by Phase J's LEFT-to-
// INNER promotion: a LEFT JOIN with a null-rejecting WHERE conjunct
// over the RHS is equivalent to INNER JOIN.  See cte_filter_phase_j.md
// and cte_filter_phase_i1.md.
static bool
is_null_rejecting(const ConditionalExpression* ce)
{
  if (ce == NULL) return false;
  switch (ce->op) {
  case T_EQUALS: case T_NOT_EQUALS:
  case T_LT: case T_LE: case T_GT: case T_GE:
    return true;
  case T_IS:
    // IS NOT NULL rejects NULL; IS NULL preserves NULL.
    return !ce->is.null;
  case T_AND:
    // AND is null-rejecting if at least one branch is — the whole
    // AND evaluates to FALSE/NULL when that branch does.
    return is_null_rejecting(ce->args.left) ||
           is_null_rejecting(ce->args.right);
  case T_OR:
    // OR is null-rejecting only if every branch rejects NULL.
    return is_null_rejecting(ce->args.left) &&
           is_null_rejecting(ce->args.right);
  default:
    return false;
  }
}

// Phase K: detect the canonical LEFT-anti-join idiom — `LEFT JOIN
// cte ON ... WHERE cte.col IS NULL` — where every WHERE conjunct is
// IS NULL on a CTE column that's provably non-NULL on the matched
// path (GB key, SUM, COUNT).  For such cases ANTI_JOIN is
// semantically equivalent to MySQL's LEFT JOIN + WHERE filter.
// emit_child_ops emits setMatchType(MatchNullOnly) which
// translates to NI_ANTI_JOIN in the QueryTree; DBSPJ's
// cte_lookup_send picks up the flag and tells DBLQH to suppress
// the agg feed on matched rows (kernel work landed alongside this
// helper).  Unmatched parents continue to NULL-inject via the
// existing JOIN_AGG_NULL_ROW_REQ path.
//
// MIN/MAX outputs are NOT promotable: AggResItem.is_null
// legitimately fires when every source value is NULL, so a matched
// group can have a NULL MIN/MAX.  ANTI_JOIN would skip those
// matched-but-NULL groups, breaking semantics.  Such cases stay
// under Phase I.1's defensive reject.
//
// See cte_filter_phase_k.md.
bool
RonSQLPreparer::is_anti_join_promotable(const QueryScope& scope,
                                         Uint32 op_idx,
                                         const ConditionalExpression* ce)
{
  if (ce == NULL) return false;
  if (ce->op == T_AND) {
    return is_anti_join_promotable(scope, op_idx, ce->args.left) &&
           is_anti_join_promotable(scope, op_idx, ce->args.right);
  }
  if (ce->op != T_IS || !ce->is.null) return false;

  const ConditionalExpression* col_side = ce->is.arg;
  if (col_side == NULL || col_side->op != T_IDENTIFIER) return false;
  Uint32 col_idx = col_side->col_idx;
  if (scope.resolved_columns == NULL) return false;
  const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[col_idx];
  if (ref.kind != QueryScope::ResolvedColumnRef::Kind::CteResultColumn)
    return false;
  if (ref.join_op_idx != op_idx) return false;

  // Walk the CTE outputs to the referenced column and check it's a
  // GB-key (Outputs::Type::COLUMN) or a SUM/COUNT aggregate.
  const JoinOp& cte_op = scope.join_plan.ops[op_idx];
  if (cte_op.cte_def == NULL) return false;
  if (ref.cte_def_idx != cte_op.cte_def_idx) return false;
  const Outputs* o = ref.cte_output;
  if (o == NULL) return false;

  if (o->type == Outputs::Type::COLUMN) return true;
  if (o->type == Outputs::Type::AGGREGATE) {
    TokenKind fun = o->aggregate.fun;
    return fun == T_SUM || fun == T_COUNT;
  }
  return false;
}

// Standard MySQL optimizer rule: a LEFT OUTER JOIN of A and B can be
// reduced to INNER JOIN when the WHERE clause has a null-rejecting
// predicate over B.  See cte_filter_phase_j.md.  Phase K extends
// this with ANTI_JOIN promotion for the LEFT-anti-join idiom.
void
RonSQLPreparer::promote_left_to_inner_for_where(QueryScope& scope)
{
  for (Uint32 t = 1; t < scope.join_plan.num_ops; t++)
  {
    JoinOp& op = scope.join_plan.ops[t];
    if (op.match_type != JoinOp::LEFT_OUTER) continue;

    if (is_null_rejecting(scope.join_where_ce[t]))
    {
      op.match_type = JoinOp::INNER;
    }
    else if (is_anti_join_promotable(scope, t, scope.join_where_ce[t]))
    {
      // Phase K: promote to ANTI_JOIN and clear the WHERE — the
      // MatchNullOnly emitted by emit_child_ops, plus the kernel
      // CTE_LOOKUP_ANTI_JOIN_FLAG handling in DBLQH, implements the
      // filter without pushing IS NULL down.
      op.match_type = JoinOp::ANTI_JOIN;
      scope.join_where_ce[t] = NULL;
    }
  }
  for (Uint32 i = 0; i < scope.cross_table_where_filters.size(); i++)
  {
    const CrossTableFilter& ctf = scope.cross_table_where_filters[i];
    if (ctf.child_table_idx >= scope.join_plan.num_ops) continue;
    JoinOp& op = scope.join_plan.ops[ctf.child_table_idx];
    if (op.match_type == JoinOp::LEFT_OUTER &&
        is_null_rejecting(ctf.ce))
    {
      op.match_type = JoinOp::INNER;
    }
  }
}

TokenKind
RonSQLPreparer::cross_table_bound_op(QueryScope& scope, Uint32 op_idx,
                                     CrossTableFilter& ctf,
                                     const char* idx_col_name,
                                     const char** out_child_col,
                                     const char** out_parent_col,
                                     Uint32* out_parent_op)
{
  if (ctf.ce == NULL) return (TokenKind)0;  // Already consumed
  if (ctf.child_table_idx != op_idx && ctf.parent_table_idx != op_idx)
    return (TokenKind)0;  // Not for this operation
  ConditionalExpression* cf = ctf.ce;
  // Only simple comparisons can serve as bounds; guard before touching
  // col_idx (an OR-shaped cross-table filter carries comparison nodes,
  // not identifiers, in args.left/right).
  if (cf->op != T_EQUALS && cf->op != T_GE && cf->op != T_GT &&
      cf->op != T_LE && cf->op != T_LT)
    return (TokenKind)0;
  if (cf->args.left == NULL || cf->args.left->op != T_IDENTIFIER ||
      cf->args.right == NULL || cf->args.right->op != T_IDENTIFIER)
    return (TokenKind)0;
  Uint32 left_cidx = cf->args.left->col_idx;
  Uint32 right_cidx = cf->args.right->col_idx;
  require_run(scope.resolved_columns != NULL,
              "Cross-table index bound setup: missing resolved "
              "columns.");
  const QueryScope::ResolvedColumnRef& left_ref =
      scope.resolved_columns[left_cidx];
  const QueryScope::ResolvedColumnRef& right_ref =
      scope.resolved_columns[right_cidx];
  require_prm(
      left_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn &&
      right_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
      "Cross-table index bound setup requires stored-table columns.");

  bool left_is_child = (left_ref.join_op_idx == op_idx);
  Uint32 child_cidx = left_is_child ? left_cidx : right_cidx;
  const char* child_col_name = m_columns[child_cidx].c_str();

  if (strcmp(idx_col_name, child_col_name) != 0)
    return (TokenKind)0;  // Not this index column

  // Match found. Normalize operator to child_col OP parent_col.
  Uint32 parent_cidx = left_is_child ? right_cidx : left_cidx;
  TokenKind filter_op = cf->op;
  if (!left_is_child)
  {
    switch (filter_op) {
    case T_LT: filter_op = T_GT; break;
    case T_LE: filter_op = T_GE; break;
    case T_GT: filter_op = T_LT; break;
    case T_GE: filter_op = T_LE; break;
    default: break;
    }
  }
  // Nullable high-only guard (findings/nullable_bounds.md; the precise
  // sibling of child_const_bound_op's v1 NOT-NULL-only rule): NULL
  // sorts below every value in an NDB ordered index, so a HIGH bound
  // with no low side would include the child's NULL entries while SQL
  // comparison is UNKNOWN for NULL.  A cross-table high bound never
  // gets a pairing low (the walk consumes one filter per column), and
  // the NdbQueryBuilder emit cannot express a NULL-excluding low bound
  // operand — so a normalized T_LT / T_LE on a nullable child column
  // must stay a filter.  Equality pairs and low bounds (T_GT / T_GE)
  // exclude NULLs naturally and stay allowed.
  if (filter_op == T_LT || filter_op == T_LE) {
    const NdbDictionary::Table* child_table =
        scope.join_plan.ops[op_idx].table;
    const NdbDictionary::Column* child_col =
        (child_table != NULL) ? child_table->getColumn(child_col_name)
                              : NULL;
    if (child_col == NULL || child_col->getNullable())
      return (TokenKind)0;
  }
  *out_child_col = child_col_name;
  *out_parent_col = m_columns[parent_cidx].c_str();
  *out_parent_op = scope.resolved_columns[parent_cidx].join_op_idx;
  return filter_op;
}

TokenKind
RonSQLPreparer::child_const_bound_op(QueryScope& scope, Uint32 op_idx,
                                     ConditionalExpression* ce,
                                     const char* idx_col_name,
                                     const NdbDictionary::Table* table)
{
  TokenKind op = ce->op;
  if (op != T_EQUALS && op != T_GE && op != T_GT &&
      op != T_LE && op != T_LT)
    return (TokenKind)0;
  ConditionalExpression* ident = ce->args.left;
  if (ident == NULL || ident->op != T_IDENTIFIER)
    return (TokenKind)0;
  // Same constant-eligibility set as build_scan_config_candidates —
  // encode_constant can serve these at emit; anything else stays a
  // residual filter.  (I_SUBQUERY placeholders are substituted with
  // constants before any emit path runs.)
  ConditionalExpression* cconst = ce->args.right;
  if (cconst == NULL ||
      (cconst->op != T_INT && cconst->op != T_FLOAT &&
       cconst->op != T_STRING && cconst->op != I_MYSQL_TIME &&
       cconst->op != I_SUBQUERY))
    return (TokenKind)0;
  if (scope.resolved_columns == NULL) return (TokenKind)0;
  const QueryScope::ResolvedColumnRef& ref =
      scope.resolved_columns[ident->col_idx];
  if (ref.kind != QueryScope::ResolvedColumnRef::Kind::StoredColumn ||
      ref.join_op_idx != op_idx)
    return (TokenKind)0;
  if (strcmp(m_columns[ident->col_idx].c_str(), idx_col_name) != 0)
    return (TokenKind)0;
  // v1 nullability guard: NULL sorts below every value in NDB ordered
  // indexes, so an index bound on a nullable column would include /
  // exclude NULL rows differently from SQL comparison semantics
  // (col <= X is UNKNOWN for NULL, but NULL < X in index order).
  // NOT NULL columns only; nullable columns keep the filter.
  const NdbDictionary::Column* tab_col = table->getColumn(idx_col_name);
  if (tab_col == NULL || tab_col->getNullable())
    return (TokenKind)0;
  return op;
}

Uint32
RonSQLPreparer::score_child_index_bounds(QueryScope& scope, Uint32 op_idx,
                                         const NdbDictionary::Index* idx,
                                         ConditionalExpression** local_conjuncts,
                                         Uint32 num_local,
                                         Uint32 num_key_cols,
                                         const NdbDictionary::Table* table)
{
  Uint32 score = 0;
  Uint32 num_idx_cols = (Uint32)idx->getNoOfColumns();
  for (Uint32 col_pos = num_key_cols; col_pos < num_idx_cols; col_pos++)
  {
    const NdbDictionary::Column* idx_col = idx->getColumn(col_pos);
    if (idx_col == NULL) break;
    const char* idx_col_name = idx_col->getName();
    bool has_eq = false, has_low = false, has_high = false;
    auto count_op = [&](TokenKind t) {
      if (t == T_EQUALS) has_eq = true;
      else if (t == T_GT || t == T_GE) has_low = true;
      else if (t == T_LT || t == T_LE) has_high = true;
    };
    for (Uint32 f = 0;
         !has_eq && f < scope.cross_table_where_filters.size();
         f++)
    {
      const char* cc; const char* pc; Uint32 po;
      count_op(cross_table_bound_op(scope, op_idx,
                                    scope.cross_table_where_filters[f],
                                    idx_col_name, &cc, &pc, &po));
    }
    for (Uint32 ci = 0; !has_eq && ci < num_local; ci++)
    {
      if (local_conjuncts[ci] == NULL) continue;
      count_op(child_const_bound_op(scope, op_idx, local_conjuncts[ci],
                                    idx_col_name, table));
    }
    if (has_eq) { score += 3; continue; }
    if (has_low) score += 1;
    if (has_high) score += 1;
    break;  // A range (or nothing) ends the prefix walk
  }
  return score;
}

void
RonSQLPreparer::assign_child_index_bounds(QueryScope& scope)
{
  // For each non-root INDEX_SCAN operation, walk the index columns
  // after the join-key prefix and bind consecutive columns from (a)
  // cross-table WHERE filters (parent-linked range bounds) and (b)
  // child-local CONSTANT conjuncts (child_bounds feature,
  // next_steps.md item 2).  Matches become index bounds — bounds
  // prune reads, filters only prune transfer; everything unmatched
  // stays a filter (cross-table: embedded-interpreter aggregation
  // predicate; child-local: the op's NdbScanFilter via
  // join_where_ce).
  //
  // Index bounds must follow prefix order: once a column has a
  // non-equality bound, later columns cannot be bounded.
  const char* database =
      m_conf.ndb ? m_conf.ndb->getDatabaseName() : nullptr;
  for (Uint32 op_idx = 1; op_idx < scope.join_plan.num_ops; op_idx++)
  {
    JoinOp& op = scope.join_plan.ops[op_idx];
    if (op.type != JoinOp::INDEX_SCAN || op.index == NULL ||
        op.table == NULL)
      continue;

    // Flatten this op's child-local conjuncts once; consumed entries
    // are NULLed and the remainder rebuilt into join_where_ce below.
    ConditionalExpression* local_conjuncts[MAX_WHERE_CONJUNCTS];
    Uint32 num_local = 0;
    if (scope.join_where_ce[op_idx] != NULL)
    {
      ConditionalExpression* simplified =
          simplify_ce(scope.join_where_ce[op_idx], -1);
      flatten_and_conjuncts(simplified, local_conjuncts, &num_local);
    }

    // Index re-selection (next_steps.md item 3): findOrderedIndex took
    // the FIRST join-key-prefix match; another candidate can bind
    // strictly more columns as bounds (e.g. idx(a) vs idx(a, b) with a
    // `b >= const` conjunct).  Ties keep the planner's choice, so
    // every existing plan is stable.
    if (m_dict != NULL)
    {
      std::vector<const NdbDictionary::Index*> candidates;
      QueryPlanner::collectOrderedIndexCandidates(
          m_dict, op.table, op.child_key_col_names, op.num_key_cols,
          m_conf.schema_cache, database, candidates);
      const NdbDictionary::Index* best = op.index;
      Uint32 best_score = score_child_index_bounds(
          scope, op_idx, op.index, local_conjuncts, num_local,
          op.num_key_cols, op.table);
      for (Uint32 c = 0; c < candidates.size(); c++)
      {
        const NdbDictionary::Index* cand = candidates[c];
        if (strcmp(cand->getName(), op.index->getName()) == 0)
          continue;
        Uint32 s = score_child_index_bounds(
            scope, op_idx, cand, local_conjuncts, num_local,
            op.num_key_cols, op.table);
        if (s > best_score)
        {
          best_score = s;
          best = cand;
        }
      }
      if (best != op.index)
      {
        op.index = best;
        // Re-normalize the key arrays to the new index's column order
        // (QueryPlanner::plan normalized for its original choice).
        for (Uint32 pos = 0; pos < op.num_key_cols; pos++)
        {
          const char* want = best->getColumn(pos)->getName();
          bool found = false;
          for (Uint32 k = pos; k < op.num_key_cols; k++)
          {
            if (strcmp(op.child_key_col_names[k], want) == 0)
            {
              if (k != pos)
              {
                const char* tc = op.child_key_col_names[pos];
                op.child_key_col_names[pos] = op.child_key_col_names[k];
                op.child_key_col_names[k] = tc;
                const char* tp = op.parent_key_col_names[pos];
                op.parent_key_col_names[pos] = op.parent_key_col_names[k];
                op.parent_key_col_names[k] = tp;
                Uint32 ti = op.key_parent_op_idx[pos];
                op.key_parent_op_idx[pos] = op.key_parent_op_idx[k];
                op.key_parent_op_idx[k] = ti;
              }
              found = true;
              break;
            }
          }
          require_bug(found, "Re-selected child index lost a join key.");
        }
      }
    }

    const NdbDictionary::Index* idx = op.index;
    Uint32 num_idx_cols = (Uint32)idx->getNoOfColumns();
    bool later_blocked = false;

    // Walk index columns starting after the join key prefix
    for (Uint32 col_pos = op.num_key_cols;
         col_pos < num_idx_cols && !later_blocked;
         col_pos++)
    {
      const NdbDictionary::Column* idx_col = idx->getColumn(col_pos);
      if (idx_col == NULL) break;
      const char* idx_col_name = idx_col->getName();

      // (a) A cross-table filter matching this index column + operation
      bool matched = false;
      for (Uint32 f = 0; f < scope.cross_table_where_filters.size(); f++)
      {
        CrossTableFilter& ctf = scope.cross_table_where_filters[f];
        const char* child_col_name;
        const char* parent_col_name;
        Uint32 parent_op;
        TokenKind filter_op =
            cross_table_bound_op(scope, op_idx, ctf, idx_col_name,
                                 &child_col_name, &parent_col_name,
                                 &parent_op);
        if (filter_op == (TokenKind)0) continue;
        bool assigned = false;

        if (filter_op == T_GT || filter_op == T_GE || filter_op == T_EQUALS)
        {
          if (op.num_low_bounds < MAX_JOIN_KEY_COLS)
          {
            JoinOp::RangeBound& lb = op.low_bounds[op.num_low_bounds++];
            lb.child_col_name = child_col_name;
            lb.parent_col_name = parent_col_name;
            lb.parent_op_idx = parent_op;
            lb.inclusive = (filter_op == T_GE || filter_op == T_EQUALS);
            lb.const_cond = NULL;
            assigned = true;
          }
        }
        if (filter_op == T_LT || filter_op == T_LE || filter_op == T_EQUALS)
        {
          if (op.num_high_bounds < MAX_JOIN_KEY_COLS)
          {
            JoinOp::RangeBound& hb = op.high_bounds[op.num_high_bounds++];
            hb.child_col_name = child_col_name;
            hb.parent_col_name = parent_col_name;
            hb.parent_op_idx = parent_op;
            hb.inclusive = (filter_op == T_LE || filter_op == T_EQUALS);
            hb.const_cond = NULL;
            assigned = true;
          }
        }

        if (assigned)
        {
          ctf.ce = NULL;  // Consumed
          matched = true;
          // Non-equality blocks later columns
          if (filter_op != T_EQUALS)
            later_blocked = true;
          break;  // One filter per index column
        }
      }

      // (b) Child-local constant conjuncts on this index column.  Like
      // the root generator, a column can take one lower AND one upper
      // bound (closed range) or a single EQ (both sides at once, and
      // the walk may continue to the next column).
      if (!matched)
      {
        bool lbound_set = false;
        bool ubound_set = false;
        bool range_seen = false;
        for (Uint32 ci = 0;
             ci < num_local && !(lbound_set && ubound_set);
             ci++)
        {
          if (local_conjuncts[ci] == NULL) continue;  // Consumed
          TokenKind cop = child_const_bound_op(scope, op_idx,
                                               local_conjuncts[ci],
                                               idx_col_name, op.table);
          if (cop == (TokenKind)0) continue;
          ConditionalExpression* cond = local_conjuncts[ci];
          const char* child_col_name =
              m_columns[cond->args.left->col_idx].c_str();
          bool wants_low = (cop == T_GT || cop == T_GE || cop == T_EQUALS);
          bool wants_high = (cop == T_LT || cop == T_LE || cop == T_EQUALS);
          if ((wants_low && lbound_set) || (wants_high && ubound_set))
            continue;  // ndbapi would refuse a second bound on one side
          bool assigned = false;

          if (wants_low && op.num_low_bounds < MAX_JOIN_KEY_COLS)
          {
            JoinOp::RangeBound& lb = op.low_bounds[op.num_low_bounds++];
            lb.child_col_name = child_col_name;
            lb.parent_col_name = NULL;
            lb.parent_op_idx = 0;
            lb.inclusive = (cop == T_GE || cop == T_EQUALS);
            lb.const_cond = cond;
            lbound_set = true;
            assigned = true;
          }
          if (wants_high && op.num_high_bounds < MAX_JOIN_KEY_COLS)
          {
            JoinOp::RangeBound& hb = op.high_bounds[op.num_high_bounds++];
            hb.child_col_name = child_col_name;
            hb.parent_col_name = NULL;
            hb.parent_op_idx = 0;
            hb.inclusive = (cop == T_LE || cop == T_EQUALS);
            hb.const_cond = cond;
            ubound_set = true;
            assigned = true;
          }

          if (assigned)
          {
            local_conjuncts[ci] = NULL;  // Consumed
            matched = true;
            if (cop != T_EQUALS)
              range_seen = true;
            else
              break;  // EQ binds the column completely
          }
        }
        if (range_seen)
          later_blocked = true;
      }

      // If nothing matched this column, stop — prefix must be contiguous
      if (!matched)
        break;
    }

    // Rebuild the op's residual filter from the unconsumed local
    // conjuncts (the ctf.ce = NULL consumption already handles the
    // cross-table side).
    if (num_local > 0)
    {
      ConditionalExpression* rebuilt = NULL;
      for (Uint32 ci = 0; ci < num_local; ci++)
      {
        if (local_conjuncts[ci] == NULL) continue;
        if (rebuilt == NULL)
        {
          rebuilt = local_conjuncts[ci];
        }
        else
        {
          ConditionalExpression* combined =
              m_amalloc->alloc_exc<ConditionalExpression>(1);
          combined->op = T_AND;
          combined->args.left = rebuilt;
          combined->args.right = local_conjuncts[ci];
          rebuilt = combined;
        }
      }
      scope.join_where_ce[op_idx] = rebuilt;
    }
  }
}

static Uint32
find_or_add_linked_proj(JoinPlan& plan, Uint32 op_idx, const char* col_name)
{
  for (Uint32 j = 0; j < plan.num_linked_projs; j++)
  {
    if (plan.linked_projs[j].source_op_idx == op_idx &&
        strcmp(plan.linked_projs[j].column_name, col_name) == 0)
      return j;
  }
  require_prm(plan.num_linked_projs < MAX_LINKED_PROJS,
              "Too many linked projections.");
  JoinPlan::LinkedProj& lp = plan.linked_projs[plan.num_linked_projs];
  lp.source_op_idx = op_idx;
  lp.column_name = col_name;
  return plan.num_linked_projs++;
}

void
RonSQLPreparer::build_agg_linked_projections()
{
  if (m_has_select_subqueries && m_main_scope.join_plan.num_agg_leaves > 0) {
    require_run(m_main_scope.resolved_columns != NULL,
                "Linked projection setup: missing resolved columns.");
    // Multi-leaf: build linked projections for GROUP BY columns from root.
    // All leaves share the same GROUP BY columns via linked projection.
    struct GroupbyColumns* groupby = m_context.ast_root.groupby_columns;
    while (groupby != NULL) {
      Uint32 col_idx = groupby->col_idx;
      const QueryScope::ResolvedColumnRef& col_ref =
          m_main_scope.resolved_columns[col_idx];
      // GROUP BY column is from the root — needs linked projection to leaves
      bool is_on_any_leaf = false;
      for (Uint32 ml = 0; ml < m_merged_leaves.size(); ml++) {
        if (col_ref.join_op_idx == m_merged_leaves[ml].plan_op_idx) {
          is_on_any_leaf = true;
          break;
        }
      }
      if (!is_on_any_leaf) {
        find_or_add_linked_proj(m_main_scope.join_plan,
                                col_ref.join_op_idx,
                                m_columns[col_idx].c_str());
      }
      groupby = groupby->next;
    }
    return;
  }

  if (m_main_scope.agg == NULL)
    return;
  require_run(m_main_scope.resolved_columns != NULL,
              "Linked projection setup: missing resolved columns.");
  Uint32 leaf_idx = m_main_scope.join_plan.agg_leaf_idx;
  DynamicArray<AggregationAPICompiler::Instr>& program = m_main_scope.agg->m_program;
  for (Uint32 i = 0; i < program.size(); i++)
  {
    if (program[i].type == AggregationAPICompiler::SVMInstrType::Load)
    {
      Uint32 col_idx = program[i].src;
      const QueryScope::ResolvedColumnRef& col_ref =
          m_main_scope.resolved_columns[col_idx];
      if (col_ref.join_op_idx != leaf_idx)
      {
        find_or_add_linked_proj(m_main_scope.join_plan,
                                col_ref.join_op_idx,
                                m_columns[col_idx].c_str());
      }
    }
  }

  // Add linked projections for cross-table WHERE filter columns that are
  // not on the leaf table.  Walk expression trees to find all column refs
  // (supports arithmetic expressions like col + 1 > other_col * 2).
  std::function<void(ConditionalExpression*)> register_linked_projs =
      [&](ConditionalExpression* ce) {
    if (ce == NULL) return;
    if (ce->op == T_IDENTIFIER) {
      Uint32 cidx = ce->col_idx;
      const QueryScope::ResolvedColumnRef& ref =
          m_main_scope.resolved_columns[cidx];
      if (ref.join_op_idx != leaf_idx) {
        find_or_add_linked_proj(m_main_scope.join_plan,
                                ref.join_op_idx,
                                m_columns[cidx].c_str());
      }
      return;
    }
    // Constants use different union members — don't access args
    if (ce->op == T_INT || ce->op == T_FLOAT ||
        ce->op == T_STRING || ce->op == T_NULL)
      return;
    // Binary ops (T_PLUS, T_MINUS, T_MULTIPLY, comparisons, etc.)
    if (ce->args.left != NULL) register_linked_projs(ce->args.left);
    if (ce->args.right != NULL) register_linked_projs(ce->args.right);
  };
  for (Uint32 f = 0; f < m_main_scope.cross_table_where_filters.size(); f++)
  {
    CrossTableFilter& ctf = m_main_scope.cross_table_where_filters[f];
    if (ctf.ce == NULL) continue;  // Consumed (converted to index bound)
    register_linked_projs(ctf.ce->args.left);
    register_linked_projs(ctf.ce->args.right);
  }

  // Add linked projections for CASE condition columns on parent tables.
  // Walk the aggregation program looking for CASE instructions whose
  // condition column is not on the leaf table.
  if (m_main_scope.agg != NULL) {
    for (Uint32 c = 0; c < m_main_scope.agg->m_cases.size(); c++) {
      auto& ci = m_main_scope.agg->m_cases[c];
      if (ci.condition == NULL) continue;
      // Flatten AND/OR to find all atom conditions
      DynamicArray<ConditionalExpression*> atoms(m_amalloc);
      ConditionalExpression* ce = ci.condition;
      if (ce->op == T_AND || ce->op == T_OR) {
        TokenKind flatten_op = ce->op;
        while (ce->op == flatten_op) {
          atoms.push(ce->args.right);
          ce = ce->args.left;
        }
        atoms.push(ce);
      } else {
        atoms.push(ce);
      }
      for (Uint32 a = 0; a < atoms.size(); a++) {
        ConditionalExpression* atom = atoms[a];
        if (atom->args.left != NULL && atom->args.left->op == T_IDENTIFIER) {
          Uint32 col_idx = atom->args.left->col_idx;
          const QueryScope::ResolvedColumnRef& col_ref =
              m_main_scope.resolved_columns[col_idx];
          if (col_ref.join_op_idx != leaf_idx) {
            find_or_add_linked_proj(m_main_scope.join_plan,
                                    col_ref.join_op_idx,
                                    m_columns[col_idx].c_str());
          }
        }
      }
    }
  }
}

// Register linked projections for each CTE body's aggregator program —
// the CTE-scope analogue of build_agg_linked_projections (main scope) and
// the GB pre-registration block in analyze_columns (line ~1147).
//
// A CTE body aggregator can reference parent-table columns (from tables
// above its own agg leaf) in its GROUP BY, Load, and cross-table WHERE
// filters. Those parent columns must be registered as linked projections
// in scope.join_plan.linked_projs so the NDB API includes them in the
// leaf op's inbound linked buffer. Without this, GroupByLinked/LoadLinkedColumn
// walks into undefined buffer positions and DBLQH crashes.
void
RonSQLPreparer::build_cte_linked_projections()
{
  for (Uint32 c = 0; c < m_cte_scopes.size(); c++) {
    QueryScope& scope = *m_cte_scopes[c];
    if (scope.agg == NULL) continue;
    require_run(scope.resolved_columns != NULL,
                "CTE linked projection setup: missing resolved columns.");
    // Locate the CTE definition to access its GroupbyColumns list.
    const CteDefinition* cte = NULL;
    Uint32 ci = 0;
    for (CteDefinition* it = m_context.ast_root.cte_list; it != NULL;
         it = it->next, ci++) {
      if (ci == c) { cte = it; break; }
    }
    if (cte == NULL) continue;

    const Uint32 leaf_idx = scope.join_plan.agg_leaf_idx;

    // GB parent columns
    for (GroupbyColumns* g = cte->stmt->groupby_columns; g != NULL;
         g = g->next) {
      Uint32 col_idx = g->col_idx;
      const QueryScope::ResolvedColumnRef& col_ref =
          scope.resolved_columns[col_idx];
      if (col_ref.join_op_idx != leaf_idx) {
        find_or_add_linked_proj(scope.join_plan,
                                col_ref.join_op_idx,
                                m_columns[col_idx].c_str());
      }
    }

    // Load parent columns from the CTE body's aggregate program
    DynamicArray<AggregationAPICompiler::Instr>& program =
        scope.agg->m_program;
    for (Uint32 i = 0; i < program.size(); i++) {
      if (program[i].type != AggregationAPICompiler::SVMInstrType::Load)
        continue;
      Uint32 col_idx = program[i].src;
      const QueryScope::ResolvedColumnRef& col_ref =
          scope.resolved_columns[col_idx];
      if (col_ref.join_op_idx != leaf_idx) {
        find_or_add_linked_proj(scope.join_plan,
                                col_ref.join_op_idx,
                                m_columns[col_idx].c_str());
      }
    }

    // Cross-table WHERE filter columns on non-leaf tables
    std::function<void(ConditionalExpression*)> register_from_ce =
        [&](ConditionalExpression* ce) {
      if (ce == NULL) return;
      if (ce->op == T_IDENTIFIER) {
        Uint32 cidx = ce->col_idx;
        const QueryScope::ResolvedColumnRef& ref =
            scope.resolved_columns[cidx];
        if (ref.join_op_idx != leaf_idx) {
          find_or_add_linked_proj(scope.join_plan,
                                  ref.join_op_idx,
                                  m_columns[cidx].c_str());
        }
        return;
      }
      if (ce->op == T_INT || ce->op == T_FLOAT ||
          ce->op == T_STRING || ce->op == T_NULL)
        return;
      if (ce->args.left != NULL) register_from_ce(ce->args.left);
      if (ce->args.right != NULL) register_from_ce(ce->args.right);
    };
    for (Uint32 f = 0; f < scope.cross_table_where_filters.size(); f++) {
      CrossTableFilter& ctf = scope.cross_table_where_filters[f];
      if (ctf.ce == NULL) continue;
      register_from_ce(ctf.ce->args.left);
      register_from_ce(ctf.ce->args.right);
    }
  }
}

void
RonSQLPreparer::plan_index_and_filter()
{
  /*
   * The scan can be performed in two ways:
   * A) A table scan, which will scan all rows in the table. A table scan can
   *    apply a filter to return the appropriate rows.
   * B) An index scan, which can limit the scan to one or several ranges in an
   *    ordered index, and apply a filter to each.
   * Given a ConditionalExpression, we attempt to find an ordered index such
   * that the ConditionalExpression can be split into index range(s) and a
   * filter. Failing that, we fall back on a table scan, where the entire
   * ConditionalExpression becomes the filter. The attempt to find a usable
   * ordered index is done in two steps:
   * 1) Based only on the SQL query, we generate a set of candidate index scan
   *    configurations, each of which defines a column, a set of ranges and a
   *    (reduced) ConditionalExpression.
   * 2) We search the ndb dictionary for an ordered index on any of the columns
   *    identified among the candidates, and try to choose the best index.
   */
  ConditionalExpression* ce = simplify_ce(m_context.ast_root.where_expression,
                                          -1 /* no max depth */);
  collect_toplevel_conditions(ce);
  if (m_conf.ndb == NULL) {
    // No connection, so we can't discover indexes.
    return;
  }
  // Phase 1 W2: a non-aggregate WHERE fully consumed as PK equalities
  // executes as a single-row PK lookup — no scan config needed.
  if (detect_pk_lookup()) {
    return;
  }
  // Phase 4b (ronsql_orderby_limit_plan.md): on the single-table
  // pass-through path an ORDER BY can be served by an ordered index
  // (SF_OrderBy streaming) instead of the Phase 3 buffered sort.  The
  // generator's FORCE INDEX satisfiability throws are deferred past
  // the ORDER BY pass so a forced index that serves only the ORDER BY
  // still counts as usable.
  const bool passthrough_orderby =
      (!m_is_aggregate_query &&
       m_context.ast_root.orderby_columns != NULL);
  // Add scan config candidates, including both index scans and table scan. This
  // will guarantee that we get at least one candidate.
  generate_scan_config_candidates(/*defer_force_check=*/passthrough_orderby);
  if (passthrough_orderby) {
    add_orderby_scan_config_candidates();
  }
  // Choose a scan config candidate
  Uint32 chosen_candidate = 0;
  for (Uint32 i = 1; i < m_scan_config_candidates.size(); i++) {
    if (m_scan_config_candidates[i].goodness >
        m_scan_config_candidates[chosen_candidate].goodness)
    {
      chosen_candidate = i;
    }
  }
  m_scan_config = &m_scan_config_candidates[chosen_candidate];
  if (passthrough_orderby) {
    validate_force_hint_after_orderby();
  }
}

void
RonSQLPreparer::collect_toplevel_conditions(ConditionalExpression* ce)
{
  if (ce == NULL) {
    return;
  }
  if (ce->op == T_AND) {
    collect_toplevel_conditions(ce->args.left);
    collect_toplevel_conditions(ce->args.right);
  } else {
    m_toplevel_conditions.push(ce);
  }
}

void
RonSQLPreparer::generate_scan_config_candidates(bool defer_force_check)
{
  // allow_nullable_high_bound = true: open_single_table_scan_op appends
  // the NULL-excluding low bound (setBound BoundLT NULL) for nullable
  // high-only columns, so the plan keeps the index.
  build_scan_config_candidates(m_indexes,
                               m_toplevel_conditions,
                               m_scan_config_candidates,
                               m_context.ast_root.root_table,
                               defer_force_check,
                               m_main_scope.table,
                               /*allow_nullable_high_bound=*/true);
}

// Interpreted programs on lookup operations ride inside every
// LQHKEYREQ, so cap them like ha_ndbcluster caps pushed conditions on
// lookups (64 words); getWordsUsed() overcounts by 2 words per label,
// which only makes the cap more conservative.  Shared by the join-root
// PK lookup (emit_root_op) and the single-table PK+residual lookup
// (detect_pk_lookup).
static const Uint32 LOOKUP_FILTER_MAX_WORDS = 64;

bool
RonSQLPreparer::detect_pk_lookup()
{
  // Aggregate queries never take the lookup: single-table aggregation
  // runs through NdbScanOperation + SO_AGGREGATION; a plain readTuple
  // has no aggregator path (the single-table twin of the Phase 0
  // lookup-root rule).
  if (m_is_aggregate_query) return false;
  const NdbDictionary::Table* tab = m_main_scope.table;
  if (tab == NULL) return false;
  const int nkeys = tab->getNoOfPrimaryKeys();
  if (nkeys <= 0) return false;
  const Uint32 num_conds = m_toplevel_conditions.size();
  if (num_conds == 0) return false;  // no WHERE — full scan
  ConditionalExpression** pk_const =
      m_amalloc->alloc_exc<ConditionalExpression*>(nkeys);
  for (int k = 0; k < nkeys; k++) pk_const[k] = NULL;
  int* cond_map = m_amalloc->alloc_exc<int>(num_conds);
  Uint32 num_residual = 0;
  /*
   * PK+residual policy (non_aggregate_pk_residual_lookup.md): each
   * conjunct either binds a not-yet-bound PK column as `pk_col = const`
   * (cond_map[i] = PK ordinal) or stays a residual filter conjunct
   * (cond_map[i] = -1) carried by the lookup's OO_INTERPRETED program.
   * Duplicate and contradictory PK equalities become residuals too —
   * the program re-checks them against the fetched row, so
   * `pk = 77 AND pk = 78` correctly yields an empty result.  The
   * lookup is taken only when every PK column is bound and, if
   * residuals exist, a trial build proves the program fits the lookup
   * word cap using only emit-supported types; otherwise the
   * scan-config path takes over (always correct, possibly a full
   * table scan on a hash-PK table).
   */
  for (Uint32 i = 0; i < num_conds; i++) {
    ConditionalExpression* ce = m_toplevel_conditions[i];
    cond_map[i] = -1;
    if (ce->op != T_EQUALS) { num_residual++; continue; }
    ConditionalExpression* col_side = NULL;
    ConditionalExpression* const_side = NULL;
    if (ce->args.left->op == T_IDENTIFIER &&
        ce->args.right->op != T_IDENTIFIER) {
      col_side = ce->args.left;
      const_side = ce->args.right;
    } else if (ce->args.right->op == T_IDENTIFIER &&
               ce->args.left->op != T_IDENTIFIER) {
      col_side = ce->args.right;
      const_side = ce->args.left;
    } else {
      num_residual++;  // col-vs-col or const-vs-const equality
      continue;
    }
    const char* col_name = m_columns[col_side->col_idx].c_str();
    bool matched = false;
    for (int k = 0; k < nkeys; k++) {
      const char* pk_name = tab->getPrimaryKey(k);
      if (pk_name != NULL && strcmp(pk_name, col_name) == 0 &&
          pk_const[k] == NULL) {
        pk_const[k] = const_side;
        cond_map[i] = k;
        matched = true;
        break;
      }
    }
    if (!matched) num_residual++;  // non-PK column or duplicate equality
  }
  for (int k = 0; k < nkeys; k++) {
    if (pk_const[k] == NULL) return false;  // partial PK cover
  }
  if (num_residual > 0) {
    /*
     * Trial build of the residual program, discarded afterwards (the
     * execute arm rebuilds it, mirroring the scan arm's per-execute
     * NdbScanFilter build).  Failures fall back to the scan-config
     * path rather than throwing: plan_index_and_filter runs at
     * prepare time — including for EXPLAIN — and the scan path throws
     * the same errors at execute time, keeping error timing and
     * classification identical to the pre-lookup behavior for
     * unsupported residual types (e.g. constants encode_constant
     * cannot encode).
     */
    try {
      NdbInterpretedCode trial_code(tab);
      {
        NdbScanFilter trial_filter(&trial_code);
        DBGV(trial_filter.setSqlCmpSemantics());
        if (DBG(trial_filter.begin(NdbScanFilter::AND)) < 0) return false;
        for (Uint32 i = 0; i < num_conds; i++) {
          if (cond_map[i] == -1) {
            apply_filter(&trial_filter, m_main_scope,
                         m_toplevel_conditions[i]);
          }
        }
        if (DBG(trial_filter.end()) < 0) return false;
      }
      if (DBG(trial_code.finalise()) != 0) return false;
      if (trial_code.getWordsUsed() > LOOKUP_FILTER_MAX_WORDS)
        return false;
    } catch (RonSQLPermanentError&) {
      return false;
    } catch (RonSQLMaybeStaleSchema&) {
      return false;
    } catch (RonSQLRetryableError&) {
      return false;
    }
  }
  m_pk_lookup = true;
  m_pk_lookup_const = pk_const;
  m_pk_lookup_cond_map = cond_map;
  m_pk_lookup_has_residual = (num_residual > 0);
  return true;
}

void
RonSQLPreparer::build_scan_config_candidates(
    DynamicArray<const NdbDictionary::Index*>& indexes,
    DynamicArray<ConditionalExpression*>& toplevel_conditions,
    DynamicArray<ScanConfig>& out_candidates,
    const TableRef* hint,
    bool defer_force_check,
    const NdbDictionary::Table* table,
    bool allow_nullable_high_bound)
{
  const Uint32 num_conds = toplevel_conditions.size();
  const TableRef::HintKind hint_kind =
    (hint != NULL) ? hint->hint_kind : TableRef::HintKind::HINT_NONE;
  {
    // Add a scan config candidate that represents table scan. This
    // guarantees we always have at least one candidate to fall back on.
    int *condition_handling_map =
      m_amalloc->alloc_exc<int>(num_conds == 0 ? 1 : num_conds);
    for (Uint32 i = 0; i < num_conds; i++) {
      condition_handling_map[i] = -1;
    }
    out_candidates.push(ScanConfig { NULL, condition_handling_map, 0 });
  }
  // With no WHERE clause there is no usable index bound, so the table scan
  // is the only candidate.  A FORCE hint then cannot be satisfied (unless
  // the caller defers the check for a later ORDER BY index pass).
  if (num_conds == 0) {
    if (hint_kind == TableRef::HintKind::HINT_FORCE && !defer_force_check) {
      throw RonSQLPermanentError(
        "FORCE INDEX cannot be used: the query has no WHERE condition that "
        "can serve as an index bound.");
    }
    return;
  }
  for(Uint32 i = 0; i < indexes.size(); i++) {
    const NdbDictionary::Index* index = indexes[i];
    // Apply the index hint: IGNORE skips named indexes; USE / FORCE consider
    // only named indexes (USE with an empty list considers none -> table scan).
    if (hint_kind != TableRef::HintKind::HINT_NONE) {
      const bool named = index_named_in_hint(index, hint);
      if (hint_kind == TableRef::HintKind::HINT_IGNORE && named) continue;
      if ((hint_kind == TableRef::HintKind::HINT_USE ||
           hint_kind == TableRef::HintKind::HINT_FORCE) && !named) continue;
    }
    int *condition_handling_map = m_amalloc->alloc_exc<int>(num_conds);
    for (Uint32 j = 0; j < num_conds; j++) {
      condition_handling_map[j] = -1;
    }
    int goodness = 0;
    Uint32 col_count = index->getNoOfColumns();
    require_bug(col_count > 0, "Index appears to have no columns.");
    bool later_columns_blocked = false;
    for(Uint32 col_idx = 0;
        col_idx < col_count && !later_columns_blocked;
        col_idx++) {
      const NdbDictionary::Column* column = index->getColumn(col_idx);
      require_bug(column != NULL, "Index column object is NULL.");
      // todo Can getAttrId be used to match against the parent table?
      const char* column_name = column->getName();
      bool lbound_set = false, ubound_set = false;
      // Conjuncts consumed for THIS column (at most a low and a high),
      // so the nullable high-only guard below can revert them.
      Uint32 consumed_this_col[2];
      Uint32 num_consumed_this_col = 0;
      for(Uint32 cond_idx = 0;
          cond_idx < num_conds && !(lbound_set && ubound_set);
          cond_idx++) {
        ConditionalExpression* ce = toplevel_conditions[cond_idx];
        if (condition_handling_map[cond_idx] != -1) {
          // Already used as bound for an earlier index column
          continue;
        }
        TokenKind op = ce->op;
        if (op != T_EQUALS &&
            op != T_GE &&
            op != T_GT &&
            op != T_LE &&
            op != T_LT) {
          // Condition unfit to serve as bound
          continue;
        }
        ConditionalExpression* condition_identifier = ce->args.left;
        if (condition_identifier == NULL ||
            condition_identifier->op != T_IDENTIFIER) {
          // Not a "column <op> value" shape; leave it as a residual filter.
          continue;
        }
        ConditionalExpression* condition_constant = ce->args.right;
        if (condition_constant == NULL ||
            (condition_constant->op != T_INT &&
             condition_constant->op != T_FLOAT &&
             condition_constant->op != T_STRING &&
             condition_constant->op != I_MYSQL_TIME &&
             condition_constant->op != I_SUBQUERY)) {
          // The right side is not a constant the bound encoder can
          // serve — e.g. column-vs-column or an expression simplify_ce
          // couldn't fold.  encode_constant would throw at emit time,
          // so leave the conjunct as a residual filter.  (I_SUBQUERY
          // placeholders are substituted with constants before any
          // emit path runs, so they are bound-eligible.)
          continue;
        }
        const char* condition_col_name =
          m_columns[condition_identifier->col_idx].c_str();
        if (strcmp(column_name, condition_col_name) == 0) {
          bool wants_lbound = op == T_EQUALS || op == T_GE || op == T_GT;
          bool wants_ubound = op == T_EQUALS || op == T_LE || op == T_LT;
          if ((wants_lbound && lbound_set) || (wants_ubound && ubound_set)) {
            // ndbapi would refuse
            continue;
          }
          lbound_set = lbound_set || wants_lbound;
          ubound_set = ubound_set || wants_ubound;
          later_columns_blocked = op != T_EQUALS;
          condition_handling_map[cond_idx] = (int)col_idx;
          ndbrequire(num_consumed_this_col < 2);
          consumed_this_col[num_consumed_this_col++] = cond_idx;
        } else {
          later_columns_blocked = true;
        }
      }
      // Nullable high-only guard (findings/nullable_bounds.md): a bound
      // with no low side starts the DBTUX scan at the index head, which
      // INCLUDES the NULL entries (NULL sorts below every value), while
      // SQL comparison is UNKNOWN for NULL — consuming `col <= X` on a
      // nullable column as a bare high bound returns NULL rows SQL
      // excludes.  When the caller's emit path cannot append a
      // NULL-excluding low bound, revert this column's conjuncts to
      // residual filters; the column is then unbounded, ending the
      // contiguous bound prefix (later_columns_blocked is already set
      // by the range).  With no table descriptor, treat the column as
      // nullable (conservative).
      if (ubound_set && !lbound_set && !allow_nullable_high_bound) {
        const NdbDictionary::Column* tab_col =
            (table != NULL) ? table->getColumn(column_name) : NULL;
        if (tab_col == NULL || tab_col->getNullable()) {
          for (Uint32 u = 0; u < num_consumed_this_col; u++) {
            condition_handling_map[consumed_this_col[u]] = -1;
          }
          ubound_set = false;
        }
      }
      if (lbound_set || ubound_set) {
        // goodness is a crude estimate of how performant the scan configuration
        // will be. Each bound added to the configuration adds 100000 for
        // equality, 1000 for a double-bounded range and 100 for a half-open
        // range. A 10% bonus is added for data types other than VARCHAR.
        // There's also a 1-point bonus for PRIMARY index.
        int points = 100;
        if (column->getType() != NdbDictionary::Column::Type::Varchar &&
            column->getType() != NdbDictionary::Column::Type::Longvarchar) {
          points+=10;
        }
        if (lbound_set && ubound_set) points *= 10;
        if (!later_columns_blocked) points *= 100;
        goodness += points;
      }
    }
    if (goodness) {
      if (strcmp(index->getName(), "PRIMARY") == 0) {
        // If the index can be used, then add a 1-point bonus for the PRIMARY
        // index.
        goodness++;
      }
      out_candidates.push(ScanConfig { index,
                                       condition_handling_map,
                                       goodness });
    }
  }

  // FORCE INDEX must end up choosing one of the named indexes.  A usable
  // FORCE-named index yields goodness > 0 and therefore beats the goodness-0
  // table-scan candidate during selection, so it is enough here to verify
  // that at least one named index produced a usable candidate.  (Deferred
  // for the pass-through ORDER BY path: validate_force_hint_after_orderby.)
  if (hint_kind == TableRef::HintKind::HINT_FORCE && !defer_force_check) {
    bool forced_usable = false;
    for (Uint32 i = 0; i < out_candidates.size(); i++) {
      if (out_candidates[i].index != NULL &&
          index_named_in_hint(out_candidates[i].index, hint)) {
        forced_usable = true;
        break;
      }
    }
    if (!forced_usable) {
      bool present = false;
      for (Uint32 i = 0; i < indexes.size(); i++) {
        if (index_named_in_hint(indexes[i], hint)) { present = true; break; }
      }
      const char* nm = (hint->hint_indexes != NULL)
                         ? hint->hint_indexes->index_name.c_str() : "";
      std::ostringstream msg;
      if (!present) {
        msg << "FORCE INDEX names '" << nm << "', which is not an available "
               "ordered index on the scanned table.";
      } else {
        msg << "FORCE INDEX '" << nm << "' cannot be used: no WHERE condition "
               "matches its leading column.";
      }
      throw RonSQLPermanentError(msg.str().c_str());
    }
  }
}

// Case-insensitive ASCII compare of two NUL-terminated strings.
static bool
ci_equal(const char* a, const char* b)
{
  while (*a != '\0' && *b != '\0') {
    char ca = *a, cb = *b;
    if (ca >= 'a' && ca <= 'z') ca = char(ca - 32);
    if (cb >= 'a' && cb <= 'z') cb = char(cb - 32);
    if (ca != cb) return false;
    a++; b++;
  }
  return *a == *b;
}

bool
RonSQLPreparer::index_named_in_hint(const NdbDictionary::Index* index,
                                    const TableRef* hint)
{
  if (index == NULL || hint == NULL) return false;
  const char* iname = index->getName();
  if (iname == NULL) return false;
  for (const IndexHintList* h = hint->hint_indexes; h != NULL; h = h->next) {
    if (ci_equal(iname, h->index_name.c_str())) return true;
  }
  return false;
}

/*
 * Phase 4b (ronsql_orderby_limit_plan.md) helpers: ORDER BY served by
 * ordered-index scan order on the single-table pass-through path.
 */

// The stored column an ORDER BY entry denotes on the single-table path:
// a TABLE_COLUMN resolves through the main scope; an OUTPUT_REF (alias)
// goes through the SELECT output it names, which the pass-through gate
// guarantees is a plain COLUMN.  NULL when either does not resolve to a
// stored column (the caller then falls back to the buffered sort).
const NdbDictionary::Column*
RonSQLPreparer::orderby_stored_column(
    const SelectStatement& ast_root,
    const QueryScope::ResolvedColumnRef* resolved,
    const OrderbyColumns* ob)
{
  if (resolved == NULL) return NULL;
  Uint32 col_idx;
  if (ob->kind == OrderbyColumns::Kind::OUTPUT_REF) {
    const Outputs* o = ast_root.outputs;
    for (Uint32 i = 0; o != NULL && i < ob->output_idx; i++) o = o->next;
    if (o == NULL || o->type != Outputs::Type::COLUMN) return NULL;
    col_idx = o->column.col_idx;
  } else {
    col_idx = ob->col_idx;
  }
  const QueryScope::ResolvedColumnRef& R = resolved[col_idx];
  if (R.kind != QueryScope::ResolvedColumnRef::Kind::StoredColumn)
    return NULL;
  return R.dict_column;
}

bool
RonSQLPreparer::index_serves_orderby(const NdbDictionary::Index* index,
                                     const int* condition_handling_map,
                                     bool& descending) const
{
  const OrderbyColumns* first = m_context.ast_root.orderby_columns;
  if (index == NULL || first == NULL) return false;
  const bool desc = !first->ascending;
  const Uint32 ncols = index->getNoOfColumns();
  const Uint32 num_conds = m_toplevel_conditions.size();
  // An index column carrying an equality bound is constant across the
  // scanned range, so it may be skipped when matching the ORDER BY list
  // (WHERE a = 5 ORDER BY b on index (a, b) is in b order).
  auto eq_bound = [&](Uint32 pos) -> bool {
    if (condition_handling_map == NULL) return false;
    for (Uint32 i = 0; i < num_conds; i++) {
      if (condition_handling_map[i] == (int)pos &&
          m_toplevel_conditions[i]->op == T_EQUALS)
        return true;
    }
    return false;
  };
  Uint32 pos = 0;
  for (const OrderbyColumns* ob = first; ob != NULL; ob = ob->next) {
    if (ob->ascending == desc) return false;  // mixed directions
    const NdbDictionary::Column* col = orderby_stored_column(
        m_context.ast_root, m_main_scope.resolved_columns, ob);
    if (col == NULL) return false;
    const char* want = col->getName();
    while (pos < ncols &&
           strcmp(index->getColumn(pos)->getName(), want) != 0 &&
           eq_bound(pos))
      pos++;
    if (pos >= ncols || strcmp(index->getColumn(pos)->getName(), want) != 0)
      return false;
    pos++;
  }
  descending = desc;
  return true;
}

void
RonSQLPreparer::add_orderby_scan_config_candidates()
{
  const TableRef* hint = m_context.ast_root.root_table;
  const TableRef::HintKind hint_kind =
      (hint != NULL) ? hint->hint_kind : TableRef::HintKind::HINT_NONE;
  const Uint32 num_conds = m_toplevel_conditions.size();
  for (Uint32 i = 0; i < m_indexes.size(); i++) {
    const NdbDictionary::Index* index = m_indexes[i];
    // Same hint semantics as build_scan_config_candidates: IGNORE skips
    // named indexes; USE / FORCE consider only named ones.
    if (hint_kind != TableRef::HintKind::HINT_NONE) {
      const bool named = index_named_in_hint(index, hint);
      if (hint_kind == TableRef::HintKind::HINT_IGNORE && named) continue;
      if ((hint_kind == TableRef::HintKind::HINT_USE ||
           hint_kind == TableRef::HintKind::HINT_FORCE) && !named) continue;
    }
    bool desc = false;
    ScanConfig* existing = NULL;
    for (Uint32 c = 0; c < m_scan_config_candidates.size(); c++) {
      if (m_scan_config_candidates[c].index == index) {
        existing = &m_scan_config_candidates[c];
        break;
      }
    }
    if (existing != NULL) {
      // A bound-based candidate: its equality bounds may let the ORDER
      // BY skip leading index columns.  The bonus only breaks ties
      // between equally good bound candidates — a better bound
      // elsewhere still wins and the query sorts client-side.
      if (index_serves_orderby(index, existing->condition_handling_map,
                               desc)) {
        existing->index_order = true;
        existing->index_order_desc = desc;
        existing->goodness += ORDERBY_INDEX_BONUS;
      }
      continue;
    }
    // No usable bound on this index: it still beats the table scan when
    // it delivers the ORDER BY order (full index scan in key order,
    // every conjunct a residual filter, early close under LIMIT).
    if (!index_serves_orderby(index, NULL, desc)) continue;
    int* condition_handling_map =
        m_amalloc->alloc_exc<int>(num_conds == 0 ? 1 : num_conds);
    for (Uint32 j = 0; j < num_conds; j++) condition_handling_map[j] = -1;
    ScanConfig sc;
    sc.index = index;
    sc.condition_handling_map = condition_handling_map;
    sc.goodness = ORDERBY_INDEX_BONUS;
    sc.index_order = true;
    sc.index_order_desc = desc;
    m_scan_config_candidates.push(sc);
  }
}

void
RonSQLPreparer::validate_force_hint_after_orderby() const
{
  const TableRef* hint = m_context.ast_root.root_table;
  if (hint == NULL || hint->hint_kind != TableRef::HintKind::HINT_FORCE)
    return;
  for (Uint32 c = 0; c < m_scan_config_candidates.size(); c++) {
    const ScanConfig& sc = m_scan_config_candidates[c];
    if (sc.index != NULL && index_named_in_hint(sc.index, hint)) return;
  }
  bool present = false;
  for (Uint32 i = 0; i < m_indexes.size(); i++) {
    if (index_named_in_hint(m_indexes[i], hint)) { present = true; break; }
  }
  const char* nm = (hint->hint_indexes != NULL)
                     ? hint->hint_indexes->index_name.c_str() : "";
  std::ostringstream msg;
  if (!present) {
    msg << "FORCE INDEX names '" << nm << "', which is not an available "
           "ordered index on the scanned table.";
  } else if (m_toplevel_conditions.size() == 0) {
    msg << "FORCE INDEX '" << nm << "' cannot be used: the query has no "
           "WHERE condition that can serve as an index bound, and the "
           "ORDER BY is not a prefix of the index.";
  } else {
    msg << "FORCE INDEX '" << nm << "' cannot be used: no WHERE condition "
           "matches its leading column, and the ORDER BY is not a prefix "
           "of the index.";
  }
  throw RonSQLPermanentError(msg.str().c_str());
}

void
RonSQLPreparer::reject_index_hints_on_joins(const JoinClause* joins) const
{
  for (const JoinClause* jc = joins; jc != NULL; jc = jc->next) {
    if (jc->table.hint_kind != TableRef::HintKind::HINT_NONE) {
      throw RonSQLPermanentError(
        "Index hints (FORCE/USE/IGNORE INDEX) are only supported on the "
        "root table of a query or CTE body, not on a joined table.");
    }
  }
}

// Phase I.10: turn a scalar MIN/MAX CTE body into an ordered
// index scan with maxRows=1.  Detects a single-op CTE body whose
// only output is a direct-column MIN(col) / MAX(col) over a NOT
// NULL column that is the leading column of an ordered index, and
// no WHERE / GROUP BY.  When matched, rewrites the planner's first
// op to INDEX_SCAN on that index and records the scan ordering
// (MAX => descending, MIN => ascending) in `scope.body_minmax_kind`
// so the emit step issues `scanIndex(idx, …, setMaxRows(1))` and
// reads just the extreme row per fragment.
//
// Quietly returns (leaving the body as a plain scan + aggregation)
// for any shape it doesn't handle: multi-op body, a WHERE or
// GROUP BY, a non-MIN/MAX or non-direct-column aggregate, a
// nullable / unsupported-type source column, or no ordered index
// whose leading column is the aggregated column.
void
RonSQLPreparer::select_cte_body_minmax_index(QueryScope& scope,
                                              const CteDefinition* cte)
{
  JoinPlan& plan = scope.join_plan;
  if (cte == NULL || cte->stmt == NULL) return;
  if (plan.num_ops != 1) return;
  if (plan.ops[0].type != JoinOp::TABLE_SCAN &&
      plan.ops[0].type != JoinOp::INDEX_SCAN) return;
  if (cte->stmt->where_expression != NULL) return;
  if (cte->stmt->groupby_columns != NULL) return;

  const Outputs* output = cte->stmt->outputs;
  if (output == NULL || output->next != NULL) return;
  if (output->type != Outputs::Type::AGGREGATE) return;
  TokenKind fun = output->aggregate.fun;
  if (fun != T_MIN && fun != T_MAX) return;

  AggregationAPICompiler::Expr* arg = output->aggregate.arg;
  if (arg == NULL || !arg->isLoad()) return;
  Uint32 agg_col_idx = arg->getLoadIdx();

  const NdbDictionary::Table* tab = plan.ops[0].table;
  if (tab == NULL) return;
  if (scope.resolved_columns == NULL) return;
  const QueryScope::ResolvedColumnRef& agg_ref =
      scope.resolved_columns[agg_col_idx];
  if (agg_ref.kind != QueryScope::ResolvedColumnRef::Kind::StoredColumn)
    return;
  if (agg_ref.join_op_idx != 0) return;

  const NdbDictionary::Column* agg_col = agg_ref.dict_column;
  if (agg_col == NULL) return;
  if (agg_col->getNullable()) return;
  if (!minmax_index_source_type_supported(agg_col)) return;

  if (!load_cte_body_indexes(scope, tab)) return;
  if (scope.body_indexes.size() == 0) return;

  const NdbDictionary::Index* chosen = NULL;
  const char* agg_col_name = agg_col->getName();
  for (Uint32 i = 0; i < scope.body_indexes.size(); i++) {
    const NdbDictionary::Index* index = scope.body_indexes[i];
    if (index == NULL || index->getNoOfColumns() == 0) continue;
    const NdbDictionary::Column* index_col = index->getColumn(0);
    if (index_col == NULL) continue;
    if (strcmp(index_col->getName(), agg_col_name) == 0) {
      chosen = index;
      break;
    }
  }
  if (chosen == NULL) return;

  plan.ops[0].type = JoinOp::INDEX_SCAN;
  plan.ops[0].index = chosen;
  scope.body_minmax_kind = (fun == T_MAX)
      ? QueryScope::MinMaxKind::MAX_DESC
      : QueryScope::MinMaxKind::MIN_ASC;
}

bool
RonSQLPreparer::decimal_minmax_fits_64bit(
    NdbDictionary::Column::Type type,
    Int32 precision,
    Int32 scale)
{
  if (type == NdbDictionary::Column::Decimal)
  {
    return scale > 0 || precision <= 18;
  }
  if (type == NdbDictionary::Column::Decimalunsigned)
  {
    return scale > 0 || precision <= 19;
  }
  return true;
}

bool
RonSQLPreparer::minmax_index_source_type_supported(
    const NdbDictionary::Column* col)
{
  if (col == NULL) return false;
  switch (col->getType()) {
  case NdbDictionary::Column::Tinyint:
  case NdbDictionary::Column::Smallint:
  case NdbDictionary::Column::Mediumint:
  case NdbDictionary::Column::Int:
  case NdbDictionary::Column::Bigint:
  case NdbDictionary::Column::Tinyunsigned:
  case NdbDictionary::Column::Smallunsigned:
  case NdbDictionary::Column::Mediumunsigned:
  case NdbDictionary::Column::Unsigned:
  case NdbDictionary::Column::Bigunsigned:
  case NdbDictionary::Column::Float:
  case NdbDictionary::Column::Double:
    return true;
  case NdbDictionary::Column::Decimal:
  case NdbDictionary::Column::Decimalunsigned:
    return decimal_minmax_fits_64bit(col->getType(),
                                     col->getPrecision(),
                                     col->getScale());
  default:
    return false;
  }
}

bool
RonSQLPreparer::load_cte_body_indexes(QueryScope& scope,
                                       const NdbDictionary::Table* tab)
{
  if (scope.body_indexes.size() > 0) return true;
  if (tab == NULL) return false;
  if (m_conf.ndb == NULL || m_dict == NULL) return false;

  const char* db = m_conf.ndb->getDatabaseName();
  const std::vector<RdrsSchemaCache::CachedIndex>* cached = NULL;
  if (m_conf.schema_cache != NULL && db != NULL) {
    cached = m_conf.schema_cache->getIndexes(m_dict, tab, db,
                                             tab->getName());
  }

  NdbDictionary::Dictionary::List index_list;
  if (cached == NULL) {
    if (m_dict->listIndexes(index_list, *tab) != 0) {
      return false;
    }
  }

  Uint32 idx_count = cached ? cached->size() : index_list.count;
  for (Uint32 i = 0; i < idx_count; i++) {
    const char* idx_name;
    NdbDictionary::Object::Type idx_type;
    NdbDictionary::Object::State idx_state;
    if (cached != NULL) {
      const RdrsSchemaCache::CachedIndex& ci = (*cached)[i];
      idx_name = ci.name.c_str();
      idx_type = ci.type;
      idx_state = ci.state;
    } else {
      NdbDictionary::Dictionary::List::Element& elem = index_list.elements[i];
      idx_name = elem.name;
      idx_type = (NdbDictionary::Object::Type)elem.type;
      idx_state = elem.state;
    }
    if (idx_state != NdbDictionary::Object::StateOnline) continue;
    if (idx_type != NdbDictionary::Object::OrderedIndex) continue;
    const NdbDictionary::Index* index = m_dict->getIndex(idx_name, *tab);
    if (index == NULL) continue;
    if (index->getObjectStatus() !=
        NdbDictionary::Object::Status::Retrieved) {
      continue;
    }
    scope.body_indexes.push(index);
  }
  return true;
}

// Phase I.9 (+ composite bounds) / join_root_index_scan_plan.md: pick
// an ordered index for a scope's root scan, mirroring the single-table
// scan-config selection at `plan_index_and_filter` but writing to
// per-scope state instead of `m_indexes` / `m_toplevel_conditions`
// / `m_scan_config*`.
//
// On entry, `scope` has already been planned + column-resolved +
// where-classified.  If ops[0] is a real-table TABLE_SCAN with
// available ordered indexes, this function:
//   1. Loads the root-table indexes into `scope.body_indexes`.
//   2. Walks `where_ce` for top-level AND conjuncts and
//      stashes them in `scope.body_toplevel_conditions`.
//   3. Generates candidate scan configs via the shared
//      `build_scan_config_candidates` (one TABLE_SCAN candidate
//      plus one per index any conjunct can serve).  A single
//      candidate can bind several leading index columns: e.g.
//      `WHERE a = 1 AND b > 2` on a (a,b) index yields a
//      two-column bound.
//   4. Picks the highest-scoring candidate.
//   5. If the chosen candidate uses a real index, rewrites
//      `cp.ops[0].type = INDEX_SCAN` and `cp.ops[0].index = idx`
//      so the emit branch recognises it.
//
// Caller contract for `where_ce`: CTE bodies (single-op gate at the
// call site) pass the body's whole WHERE; the main scope of a join
// query passes only the root-classified conjuncts
// (`join_where_ce[0]`) — conjuncts belonging to child ops or
// cross-table filters must not be offered as root bounds.
//
// Bound-vs-residual routing for the emit step lives on
// `scope.body_scan_config->condition_handling_map[i]`:
// `-1` => apply conjunct as InterpretedCode filter; otherwise the
// value is the index column number the conjunct bounds.  Only the
// last bound column may be a half-open / range comparison; all
// earlier bound columns are equalities (enforced by
// `later_columns_blocked`), which keeps the single NdbQueryIndexBound
// inclusivity flag per side correct.
//
// Quietly returns without rewriting anything for shapes the
// optimiser doesn't yet handle (chained CTE / CTE_SCAN root,
// no usable index, no Ndb connection at prepare time) — except
// under a FORCE INDEX hint, where the "no WHERE" and "no ordered
// index" early-outs fall through to the shared generator so it
// throws the same FORCE errors as the single-table path.
void
RonSQLPreparer::select_root_scan_config(QueryScope& scope,
                                        ConditionalExpression* where_ce,
                                        const TableRef* hint)
{
  JoinPlan& plan = scope.join_plan;
  const TableRef::HintKind hint_kind =
    (hint != NULL) ? hint->hint_kind : TableRef::HintKind::HINT_NONE;
  const bool hint_force = (hint_kind == TableRef::HintKind::HINT_FORCE);
  if (plan.ops[0].type != JoinOp::TABLE_SCAN) return;
  const NdbDictionary::Table* tab = plan.ops[0].table;
  if (tab == NULL) return;
  if (m_conf.ndb == NULL || m_dict == NULL) return;
  if (where_ce == NULL && !hint_force) return;

  // 1. Load this scope's root-table indexes — same listIndexes +
  //    schema-cache flow as load_single_table, just writing to
  //    scope.body_indexes.
  if (!load_cte_body_indexes(scope, tab)) return;
  if (scope.body_indexes.size() == 0 && !hint_force) return;

  // 2. Flatten WHERE to top-level AND conjuncts.  Mirror
  //    `collect_toplevel_conditions` but write to scope state.
  //    (With FORCE INDEX and no WHERE the conjunct list stays empty
  //    and the generator throws the standard FORCE error.)
  if (where_ce != NULL) {
    ConditionalExpression* simplified = simplify_ce(where_ce, -1);
    // Inline AND-flatten — the existing helper writes to
    // m_toplevel_conditions; the per-scope shape uses local logic.
    // Stack of nodes to expand; cap on top-level conjuncts is the
    // same as the rest of RonSQL's WHERE flattening.
    ConditionalExpression* stack[MAX_WHERE_CONJUNCTS];
    Uint32 stack_top = 0;
    stack[stack_top++] = simplified;
    while (stack_top > 0) {
      ConditionalExpression* node = stack[--stack_top];
      if (node == NULL) continue;
      if (node->op == T_AND) {
        if (stack_top + 2 > MAX_WHERE_CONJUNCTS) return;
        stack[stack_top++] = node->args.right;
        stack[stack_top++] = node->args.left;
      } else {
        scope.body_toplevel_conditions.push(node);
      }
    }
  }

  // 3. Generate candidate scan configs via the shared generator.  It
  //    pushes a TABLE_SCAN candidate (goodness=0) first so it's always
  //    available as the fallback, then one per usable ordered index
  //    scored with the same heuristic as the main scope.
  // allow_nullable_high_bound = false: this path's bounds emit through
  // NdbQueryIndexBound / emit_index_scan_root, which cannot express a
  // NULL-excluding low bound operand — a nullable high-only conjunct
  // must stay a residual filter.
  build_scan_config_candidates(scope.body_indexes,
                               scope.body_toplevel_conditions,
                               scope.body_scan_config_candidates,
                               hint,
                               /*defer_force_check=*/false,
                               scope.table,
                               /*allow_nullable_high_bound=*/false);

  // 4. Pick highest-scoring candidate.
  Uint32 chosen = 0;
  for (Uint32 i = 1; i < scope.body_scan_config_candidates.size(); i++) {
    if (scope.body_scan_config_candidates[i].goodness >
        scope.body_scan_config_candidates[chosen].goodness) {
      chosen = i;
    }
  }
  scope.body_scan_config = &scope.body_scan_config_candidates[chosen];

  // 5. If the winner is a real index, rewrite the planner's first
  //    op so the emit dispatch sees JoinOp::INDEX_SCAN.  (Leaving
  //    cp.ops[0].type as TABLE_SCAN is the correct fallback when
  //    no useful index was found — body_scan_config->index stays
  //    NULL and the existing single-op TABLE_SCAN emit still runs.)
  if (scope.body_scan_config->index != NULL) {
    plan.ops[0].type = JoinOp::INDEX_SCAN;
    plan.ops[0].index = scope.body_scan_config->index;
  }
}

// True when every primary-key column of the scope's root table has an
// equality against a constant among the root-classified WHERE conjuncts.
// emit_root_op serves those shapes with readTuple (no scan child) or an
// equality-bound PRIMARY index scan (scan child), both better than a
// scan-config index scan — so select_root_scan_config is skipped for
// them (unless FORCE INDEX overrides).
bool
RonSQLPreparer::root_pk_equality_covered(QueryScope& scope)
{
  if (scope.join_where_ce[0] == NULL) return false;
  const NdbDictionary::Table* tab = scope.join_plan.ops[0].table;
  if (tab == NULL) return false;
  int nkeys = tab->getNoOfPrimaryKeys();
  if (nkeys <= 0) return false;
  ConditionalExpression* w = simplify_ce(scope.join_where_ce[0], -1);
  ConditionalExpression* pk_const[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
  for (int k = 0; k < nkeys; k++) pk_const[k] = NULL;
  collect_pk_equalities(w, tab, pk_const);
  for (int k = 0; k < nkeys; k++) {
    if (pk_const[k] == NULL) return false;
  }
  return true;
}

/*
 * Walk a CE tree and throw if T_EXISTS is found.
 * Used after decorrelation to catch EXISTS inside OR (unsupported).
 */
static void
check_no_nested_exists(struct ConditionalExpression* ce)
{
  if (ce == NULL) return;
  if (ce->op == T_EXISTS)
    throw RonSQLPermanentError(
        "EXISTS subquery inside OR is not supported. "
        "EXISTS must be a top-level AND conjunct in WHERE.");
  switch (ce->op)
  {
  case T_IS:
    check_no_nested_exists(ce->is.arg);
    return;
  case T_INTERVAL:
    check_no_nested_exists(ce->interval.arg);
    return;
  case T_EXTRACT:
    check_no_nested_exists(ce->extract.arg);
    return;
  case T_IDENTIFIER:
  case T_INT:
  case T_FLOAT:
  case T_STRING:
  case I_MYSQL_TIME:
  case T_NULL:
  case I_SUBQUERY:
  case I_IN_SUBQUERY:
  case I_CORR_SCALAR:
    return;
  default:
    check_no_nested_exists(ce->args.left);
    check_no_nested_exists(ce->args.right);
    return;
  }
}

/*
 * Walk a CTE-body WHERE tree and throw a permanent error if any
 * comparison predicate compares two columns (column-vs-column).
 *
 * In a CTE body a col-vs-col WHERE predicate is not yet supported and
 * misbehaves two ways depending on whether the left column is indexed:
 *   - on an INDEX_SCAN body it is mis-classified as an index bound and
 *     encode_constant() then fails on the column RHS, thrown as the
 *     retryable RonSQLMaybeStaleSchema so RDRS retries it 10x (D12);
 *   - on a TABLE_SCAN body it is emitted as an NdbScanFilter attr-vs-attr
 *     program that hangs the data node (D4).
 * Reject it cleanly and permanently here so RonSQL fails fast instead of
 * retrying or hanging.  Column-vs-constant predicates are unaffected.
 * Supporting col-vs-col in a CTE-body filter is tracked for a later phase.
 * Main-query col-vs-col does not flow through build_cte_scopes and is
 * unaffected by this check.
 */
static void
check_no_cte_body_col_vs_col(struct ConditionalExpression* ce)
{
  if (ce == NULL) return;
  switch (ce->op)
  {
  case T_EQUALS:
  case T_NOT_EQUALS:
  case T_LT:
  case T_LE:
  case T_GT:
  case T_GE:
    if (ce->args.left != NULL && ce->args.right != NULL &&
        ce->args.left->op == T_IDENTIFIER &&
        ce->args.right->op == T_IDENTIFIER)
    {
      throw RonSQLPermanentError(
          "Column-vs-column comparison in a CTE body WHERE clause is not "
          "yet supported. Compare a column to a constant instead.");
    }
    // A comparison's operands are scalar expressions, not boolean
    // connectives, so there is no nested comparison to recurse into.
    return;
  case T_IS:
    check_no_cte_body_col_vs_col(ce->is.arg);
    return;
  case T_INTERVAL:
    check_no_cte_body_col_vs_col(ce->interval.arg);
    return;
  case T_EXTRACT:
    check_no_cte_body_col_vs_col(ce->extract.arg);
    return;
  case T_IDENTIFIER:
  case T_INT:
  case T_FLOAT:
  case T_STRING:
  case I_MYSQL_TIME:
  case T_NULL:
  case I_SUBQUERY:
  case I_IN_SUBQUERY:
  case I_CORR_SCALAR:
    return;
  default:
    check_no_cte_body_col_vs_col(ce->args.left);
    check_no_cte_body_col_vs_col(ce->args.right);
    return;
  }
}

void
RonSQLPreparer::decorrelate_exists()
{
  ConditionalExpression* where_ce = m_context.ast_root.where_expression;
  if (where_ce == NULL) return;

  // Flatten outer WHERE into AND conjuncts
  ConditionalExpression* conjuncts[MAX_WHERE_CONJUNCTS];
  Uint32 num_conjuncts = 0;
  flatten_and_conjuncts(where_ce, conjuncts, &num_conjuncts);

  // Check if any conjunct is T_EXISTS or NOT EXISTS (T_NOT/T_EXCLAMATION wrapping T_EXISTS)
  bool has_exists = false;
  for (Uint32 i = 0; i < num_conjuncts; i++)
  {
    if (conjuncts[i]->op == T_EXISTS)
    {
      has_exists = true;
      break;
    }
    if ((conjuncts[i]->op == T_NOT || conjuncts[i]->op == T_EXCLAMATION) &&
        conjuncts[i]->args.left != NULL &&
        conjuncts[i]->args.left->op == T_EXISTS)
    {
      has_exists = true;
      break;
    }
  }
  if (!has_exists) return;

  SelectStatement& ast = m_context.ast_root;

  // Kept conjuncts (non-EXISTS, plus IN-subquery nodes generated below)
  ConditionalExpression* kept[MAX_WHERE_CONJUNCTS];
  Uint32 num_kept = 0;

  for (Uint32 i = 0; i < num_conjuncts; i++)
  {
    // Extract EXISTS node (possibly wrapped in T_NOT/T_EXCLAMATION)
    bool is_not_exists = false;
    ConditionalExpression* exists_node = NULL;

    if (conjuncts[i]->op == T_EXISTS)
    {
      exists_node = conjuncts[i];
    }
    else if ((conjuncts[i]->op == T_NOT || conjuncts[i]->op == T_EXCLAMATION) &&
             conjuncts[i]->args.left != NULL &&
             conjuncts[i]->args.left->op == T_EXISTS)
    {
      exists_node = conjuncts[i]->args.left;
      is_not_exists = true;
    }
    else
    {
      require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                  "Too many WHERE conjuncts after decorrelation.");
      kept[num_kept++] = conjuncts[i];
      continue;
    }

    // Process EXISTS/NOT EXISTS conjunct — transform into IN subquery
    SelectStatement* inner_stmt = exists_node->subquery.stmt;
    require_prm(inner_stmt != NULL,
                "EXISTS subquery has no inner statement.");
    require_prm(inner_stmt->joins == NULL,
                "EXISTS subquery with inner joins not supported.");
    require_prm(inner_stmt->table.c_str() != NULL,
                "EXISTS subquery has no inner table.");

    // Look up inner table in dictionary
    const NdbDictionary::Table* inner_table =
        m_dict->getTable(inner_stmt->table.c_str());
    require_prm(inner_table != NULL,
                "EXISTS subquery references unknown table.");

    // Flatten inner WHERE
    ConditionalExpression* inner_conjuncts[MAX_WHERE_CONJUNCTS];
    Uint32 num_inner = 0;
    flatten_and_conjuncts(inner_stmt->where_expression,
                          inner_conjuncts, &num_inner);

    require_prm(num_inner > 0,
                "EXISTS subquery must have a WHERE clause with "
                "correlation predicates.");

    // Classify inner conjuncts: find correlation predicate and
    // collect non-correlation predicates
    Uint32 outer_col_idx = UINT32_MAX;
    const char* inner_col_name = NULL;
    Uint32 num_correlation = 0;
    Uint32 num_non_corr = 0;

    for (Uint32 j = 0; j < num_inner; j++)
    {
      ConditionalExpression* ic = inner_conjuncts[j];

      // Check for correlation predicate: T_EQUALS between two T_IDENTIFIERs
      if (ic->op == T_EQUALS &&
          ic->args.left != NULL && ic->args.left->op == T_IDENTIFIER &&
          ic->args.right != NULL && ic->args.right->op == T_IDENTIFIER)
      {
        Uint32 left_idx = ic->args.left->col_idx;
        Uint32 right_idx = ic->args.right->col_idx;
        const char* left_name = m_columns[left_idx].c_str();
        const char* right_name = m_columns[right_idx].c_str();

        // Check qualifier first, then dictionary to determine inner/outer
        LexCString left_qual = m_column_qualifiers[left_idx];
        LexCString right_qual = m_column_qualifiers[right_idx];

        bool left_is_inner;
        bool right_is_inner;

        LexCString inner_alias = inner_stmt->root_table != NULL
            ? inner_stmt->root_table->alias
            : inner_stmt->table;

        if (left_qual.c_str() != NULL)
          left_is_inner = (left_qual == inner_alias);
        else
          left_is_inner = (inner_table->getColumn(left_name) != NULL);

        if (right_qual.c_str() != NULL)
          right_is_inner = (right_qual == inner_alias);
        else
          right_is_inner = (inner_table->getColumn(right_name) != NULL);

        if (left_is_inner != right_is_inner)
        {
          // Correlation predicate: one inner, one outer
          require_prm(num_correlation == 0,
                      "EXISTS subquery with multiple correlation "
                      "predicates not supported (use single-column "
                      "correlation).");
          outer_col_idx = left_is_inner ? right_idx : left_idx;
          inner_col_name = left_is_inner ? left_name : right_name;
          num_correlation++;
          continue;
        }
      }

      // Non-correlation predicate
      require_prm(num_non_corr < MAX_WHERE_CONJUNCTS,
                  "Too many non-correlation predicates in EXISTS.");
      num_non_corr++;
    }

    require_prm(num_correlation == 1,
                "EXISTS subquery must have exactly one correlation "
                "predicate (inner.col = outer.col).");

    // Build the inner SQL string for the IN subquery.
    // Format: SELECT <inner_col> FROM <table> [WHERE ...] GROUP BY <inner_col>
    //
    // For the WHERE clause, we extract the original text of each
    // non-correlation predicate from the source buffer. Since individual
    // CE nodes don't carry position info, we extract text spans from the
    // original inner query and remove the correlation predicate.
    //
    // Simplified approach: extract the whole inner WHERE text from
    // sql_begin..sql_end and use it minus the correlation predicate.
    // For robustness, we build the SQL using the inner table name and
    // inner column name, then splice in non-correlation predicate text.

    const char* inner_table_name = inner_stmt->table.c_str();
    LexCString inner_alias = inner_stmt->root_table != NULL
        ? inner_stmt->root_table->alias
        : inner_stmt->table;
    bool has_alias = (inner_stmt->root_table != NULL &&
                      !(inner_alias == inner_stmt->table));

    // Build inner SQL string
    std::ostringstream sql;
    sql << "SELECT MIN(" << inner_col_name << ")"
        << " FROM " << inner_table_name;
    if (has_alias)
      sql << " AS " << inner_alias.c_str();

    if (num_non_corr > 0)
    {
      // We need to serialize non-correlation predicates back to SQL.
      // Extract the original inner SQL text and remove the correlation part.
      // Strategy: use the original SQL between sql_begin and sql_end,
      // which is the complete inner SELECT. We need just the WHERE part
      // without the correlation predicate.
      //
      // Alternative: serialize simple predicates (col op const).
      // For now, extract the original WHERE clause text and strip the
      // correlation predicate textually. This is fragile but works for
      // simple cases like "l_orderkey = o_id AND l_quantity > 20".
      //
      // More robust: find WHERE keyword in inner SQL, then for each AND
      // conjunct, check if it contains both the inner and outer column
      // names of the correlation. Skip that one, keep the rest.

      ndbrequire(inner_stmt->sql_begin != NULL &&
                 inner_stmt->sql_end != NULL);
      std::string inner_sql(inner_stmt->sql_begin,
                            inner_stmt->sql_end - inner_stmt->sql_begin);

      // Find WHERE keyword (case-insensitive)
      size_t where_pos = std::string::npos;
      for (size_t p = 0; p + 5 <= inner_sql.size(); p++)
      {
        if ((inner_sql[p] == 'W' || inner_sql[p] == 'w') &&
            (inner_sql[p+1] == 'H' || inner_sql[p+1] == 'h') &&
            (inner_sql[p+2] == 'E' || inner_sql[p+2] == 'e') &&
            (inner_sql[p+3] == 'R' || inner_sql[p+3] == 'r') &&
            (inner_sql[p+4] == 'E' || inner_sql[p+4] == 'e') &&
            (p + 5 == inner_sql.size() || inner_sql[p+5] == ' '))
        {
          where_pos = p;
          break;
        }
      }

      if (where_pos != std::string::npos)
      {
        // Extract the WHERE clause text (everything after "WHERE ")
        std::string where_text = inner_sql.substr(where_pos + 6);

        // Strip GROUP BY / ORDER BY / LIMIT if present
        for (const char* kw : {"GROUP BY", "ORDER BY", "LIMIT",
                                "group by", "order by", "limit"})
        {
          size_t kw_pos = where_text.find(kw);
          if (kw_pos != std::string::npos)
            where_text = where_text.substr(0, kw_pos);
        }

        // Trim trailing whitespace
        while (!where_text.empty() &&
               (where_text.back() == ' ' || where_text.back() == '\n' ||
                where_text.back() == '\r' || where_text.back() == '\t'))
          where_text.pop_back();

        // Now split by AND and remove the correlation predicate.
        // The correlation predicate contains both the outer column name
        // and the inner column name connected by '='.
        const char* outer_col_name = m_columns[outer_col_idx].c_str();
        std::vector<std::string> and_parts;
        size_t pos = 0;
        while (pos < where_text.size())
        {
          // Find next AND (case-insensitive)
          size_t and_pos = std::string::npos;
          for (size_t p = pos; p + 3 <= where_text.size(); p++)
          {
            if ((where_text[p] == 'A' || where_text[p] == 'a') &&
                (where_text[p+1] == 'N' || where_text[p+1] == 'n') &&
                (where_text[p+2] == 'D' || where_text[p+2] == 'd') &&
                (p == 0 || where_text[p-1] == ' ') &&
                (p + 3 >= where_text.size() || where_text[p+3] == ' '))
            {
              and_pos = p;
              break;
            }
          }
          std::string part;
          if (and_pos == std::string::npos)
          {
            part = where_text.substr(pos);
            pos = where_text.size();
          }
          else
          {
            part = where_text.substr(pos, and_pos - pos);
            pos = and_pos + 3; // skip "AND"
          }
          // Trim
          while (!part.empty() && part.front() == ' ') part.erase(0, 1);
          while (!part.empty() && part.back() == ' ') part.pop_back();
          if (part.empty()) continue;

          // Check if this part is the correlation predicate
          bool is_correlation = false;
          if (part.find(outer_col_name) != std::string::npos &&
              part.find(inner_col_name) != std::string::npos &&
              part.find('=') != std::string::npos)
          {
            is_correlation = true;
          }
          if (!is_correlation)
            and_parts.push_back(part);
        }

        if (!and_parts.empty())
        {
          sql << " WHERE ";
          for (size_t p = 0; p < and_parts.size(); p++)
          {
            if (p > 0) sql << " AND ";
            sql << and_parts[p];
          }
        }
      }
    }

    sql << " GROUP BY " << inner_col_name;

    std::string sql_str = sql.str();
    size_t sql_len = sql_str.size();
    char* sql_buf = m_amalloc->alloc_exc<char>(sql_len + 1);
    memcpy(sql_buf, sql_str.c_str(), sql_len);
    sql_buf[sql_len] = '\0';

    // Create a new inner SelectStatement for the IN subquery
    SelectStatement* new_inner = m_amalloc->alloc_exc<SelectStatement>(1);
    new (new_inner) SelectStatement();
    new_inner->table = inner_stmt->table;
    new_inner->root_table = inner_stmt->root_table;
    new_inner->sql_begin = sql_buf;
    new_inner->sql_end = sql_buf + sql_len;

    // Build the outer column reference CE node (LHS of IN)
    // The outer column was marked as inner during subquery parsing
    // (m_subquery_depth > 0 at parse time). Clear the flag since it's
    // actually an outer reference used in the WHERE clause.
    if (outer_col_idx < m_col_is_inner.size())
      m_col_is_inner[outer_col_idx] = false;

    ConditionalExpression* outer_ref =
        m_amalloc->alloc_exc<ConditionalExpression>(1);
    outer_ref->op = T_IDENTIFIER;
    outer_ref->col_idx = outer_col_idx;

    // Transform the T_EXISTS CE node into I_IN_SUBQUERY in-place
    exists_node->op = I_IN_SUBQUERY;
    exists_node->in_subquery.expr = outer_ref;
    exists_node->in_subquery.stmt = new_inner;

    // Register in m_subquery_infos (since analyze_subqueries already ran)
    SubqueryInfo info;
    info.ce_node = exists_node;
    info.inner_stmt = new_inner;
    info.is_in_subquery = true;
    info.in_expr = outer_ref;
    info.in_values = new (m_amalloc->alloc_exc<DynamicArray<SubqueryResult>>(1))
      DynamicArray<SubqueryResult>(m_amalloc);
    m_subquery_infos.push(info);
    m_has_subqueries = true;

    // Keep the transformed node in the WHERE:
    // - EXISTS: keep exists_node (now I_IN_SUBQUERY)
    // - NOT EXISTS: keep conjuncts[i] (T_NOT wrapper, child is now I_IN_SUBQUERY)
    require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                "Too many WHERE conjuncts after decorrelation.");
    kept[num_kept++] = is_not_exists ? conjuncts[i] : exists_node;
  }

  // Rebuild outer WHERE from kept conjuncts
  ConditionalExpression* rebuilt = NULL;
  for (Uint32 i = 0; i < num_kept; i++)
  {
    if (rebuilt == NULL)
    {
      rebuilt = kept[i];
    }
    else
    {
      ConditionalExpression* combined =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
      combined->op = T_AND;
      combined->args.left = rebuilt;
      combined->args.right = kept[i];
      rebuilt = combined;
    }
  }
  ast.where_expression = rebuilt;

  // Check no nested EXISTS remain (e.g., inside OR)
  check_no_nested_exists(ast.where_expression);
}

static TokenKind
flip_cmp_op(TokenKind op)
{
  switch (op)
  {
  case T_GT: return T_LT;
  case T_LT: return T_GT;
  case T_GE: return T_LE;
  case T_LE: return T_GE;
  default: return op;  // T_EQUALS, T_NOT_EQUALS are symmetric
  }
}

static bool
is_comparison_op(TokenKind op)
{
  return op == T_EQUALS || op == T_NOT_EQUALS ||
         op == T_GT || op == T_GE || op == T_LT || op == T_LE;
}

static void
parse_subquery_value(const std::string& str, SubqueryResult& val)
{
  if (str == "NULL")
  {
    val.is_null = true;
    return;
  }
  char* endptr = NULL;
  errno = 0;
  long long ll = strtoll(str.c_str(), &endptr, 10);
  if (endptr != NULL && *endptr == '\0' && errno == 0)
  {
    val.is_null = false;
    val.is_float = false;
    val.int_val = (Int64)ll;
    return;
  }
  errno = 0;
  double dbl = strtod(str.c_str(), &endptr);
  if (endptr != NULL && *endptr == '\0' && errno == 0)
  {
    val.is_null = false;
    val.is_float = true;
    val.float_val = dbl;
    return;
  }
  throw RonSQLPermanentError(
      "Correlated scalar subquery returned a non-numeric value.");
}

void
RonSQLPreparer::decorrelate_scalar()
{
  ConditionalExpression* where_ce = m_context.ast_root.where_expression;
  if (where_ce == NULL) return;

  // Flatten outer WHERE into AND conjuncts
  ConditionalExpression* conjuncts[MAX_WHERE_CONJUNCTS];
  Uint32 num_conjuncts = 0;
  flatten_and_conjuncts(where_ce, conjuncts, &num_conjuncts);

  // Check if any conjunct is a comparison with I_SUBQUERY on one side
  bool has_corr_scalar = false;
  for (Uint32 i = 0; i < num_conjuncts; i++)
  {
    if (!is_comparison_op(conjuncts[i]->op)) continue;
    ConditionalExpression* left = conjuncts[i]->args.left;
    ConditionalExpression* right = conjuncts[i]->args.right;
    if ((left != NULL && left->op == I_SUBQUERY) ||
        (right != NULL && right->op == I_SUBQUERY))
    {
      // Check if the inner query has a correlated WHERE
      SelectStatement* inner_stmt = (left != NULL && left->op == I_SUBQUERY)
          ? left->subquery.stmt : right->subquery.stmt;
      if (inner_stmt == NULL || inner_stmt->joins != NULL) continue;
      if (inner_stmt->table.c_str() == NULL) continue;
      if (inner_stmt->where_expression == NULL) continue;

      const NdbDictionary::Table* inner_table =
          m_dict->getTable(inner_stmt->table.c_str());
      if (inner_table == NULL) continue;

      // Flatten inner WHERE
      ConditionalExpression* inner_conjuncts[MAX_WHERE_CONJUNCTS];
      Uint32 num_inner = 0;
      flatten_and_conjuncts(inner_stmt->where_expression,
                            inner_conjuncts, &num_inner);

      // Look for a correlation predicate
      bool found_correlation = false;
      for (Uint32 j = 0; j < num_inner; j++)
      {
        ConditionalExpression* ic = inner_conjuncts[j];
        if (ic->op == T_EQUALS &&
            ic->args.left != NULL && ic->args.left->op == T_IDENTIFIER &&
            ic->args.right != NULL && ic->args.right->op == T_IDENTIFIER)
        {
          Uint32 left_idx = ic->args.left->col_idx;
          Uint32 right_idx = ic->args.right->col_idx;
          const char* left_name = m_columns[left_idx].c_str();
          const char* right_name = m_columns[right_idx].c_str();

          LexCString left_qual = m_column_qualifiers[left_idx];
          LexCString right_qual = m_column_qualifiers[right_idx];

          LexCString inner_alias = inner_stmt->root_table != NULL
              ? inner_stmt->root_table->alias
              : inner_stmt->table;

          bool left_is_inner;
          bool right_is_inner;

          if (left_qual.c_str() != NULL)
            left_is_inner = (left_qual == inner_alias);
          else
            left_is_inner = (inner_table->getColumn(left_name) != NULL);

          if (right_qual.c_str() != NULL)
            right_is_inner = (right_qual == inner_alias);
          else
            right_is_inner = (inner_table->getColumn(right_name) != NULL);

          if (left_is_inner != right_is_inner)
          {
            found_correlation = true;
            break;
          }
        }
      }
      if (found_correlation)
      {
        has_corr_scalar = true;
        break;
      }
    }
  }
  if (!has_corr_scalar) return;

  SelectStatement& ast = m_context.ast_root;

  ConditionalExpression* kept[MAX_WHERE_CONJUNCTS];
  Uint32 num_kept = 0;

  for (Uint32 i = 0; i < num_conjuncts; i++)
  {
    if (!is_comparison_op(conjuncts[i]->op))
    {
      require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                  "Too many WHERE conjuncts after decorrelation.");
      kept[num_kept++] = conjuncts[i];
      continue;
    }

    ConditionalExpression* left = conjuncts[i]->args.left;
    ConditionalExpression* right = conjuncts[i]->args.right;

    // Determine which side is the subquery
    bool subq_on_left = (left != NULL && left->op == I_SUBQUERY);
    bool subq_on_right = (right != NULL && right->op == I_SUBQUERY);

    if (!subq_on_left && !subq_on_right)
    {
      require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                  "Too many WHERE conjuncts after decorrelation.");
      kept[num_kept++] = conjuncts[i];
      continue;
    }

    ConditionalExpression* subq_child = subq_on_left ? left : right;
    ConditionalExpression* cmp_expr = subq_on_left ? right : left;
    TokenKind cmp_op = conjuncts[i]->op;
    // If subquery is on the left, flip: (SELECT ...) < col  means col > (SELECT ...)
    if (subq_on_left)
      cmp_op = flip_cmp_op(cmp_op);

    SelectStatement* inner_stmt = subq_child->subquery.stmt;
    if (inner_stmt == NULL || inner_stmt->joins != NULL ||
        inner_stmt->table.c_str() == NULL ||
        inner_stmt->where_expression == NULL)
    {
      // Not decorrelatable, keep as-is
      require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                  "Too many WHERE conjuncts after decorrelation.");
      kept[num_kept++] = conjuncts[i];
      continue;
    }

    const NdbDictionary::Table* inner_table =
        m_dict->getTable(inner_stmt->table.c_str());
    if (inner_table == NULL)
    {
      require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                  "Too many WHERE conjuncts after decorrelation.");
      kept[num_kept++] = conjuncts[i];
      continue;
    }

    // Flatten inner WHERE
    ConditionalExpression* inner_conjuncts[MAX_WHERE_CONJUNCTS];
    Uint32 num_inner = 0;
    flatten_and_conjuncts(inner_stmt->where_expression,
                          inner_conjuncts, &num_inner);

    // Classify inner conjuncts: find correlation predicate
    Uint32 outer_col_idx = UINT32_MAX;
    const char* inner_col_name = NULL;
    Uint32 num_correlation = 0;
    Uint32 num_non_corr = 0;

    for (Uint32 j = 0; j < num_inner; j++)
    {
      ConditionalExpression* ic = inner_conjuncts[j];

      if (ic->op == T_EQUALS &&
          ic->args.left != NULL && ic->args.left->op == T_IDENTIFIER &&
          ic->args.right != NULL && ic->args.right->op == T_IDENTIFIER)
      {
        Uint32 l_idx = ic->args.left->col_idx;
        Uint32 r_idx = ic->args.right->col_idx;
        const char* l_name = m_columns[l_idx].c_str();
        const char* r_name = m_columns[r_idx].c_str();

        LexCString l_qual = m_column_qualifiers[l_idx];
        LexCString r_qual = m_column_qualifiers[r_idx];

        LexCString inner_alias = inner_stmt->root_table != NULL
            ? inner_stmt->root_table->alias
            : inner_stmt->table;

        bool l_is_inner;
        bool r_is_inner;

        if (l_qual.c_str() != NULL)
          l_is_inner = (l_qual == inner_alias);
        else
          l_is_inner = (inner_table->getColumn(l_name) != NULL);

        if (r_qual.c_str() != NULL)
          r_is_inner = (r_qual == inner_alias);
        else
          r_is_inner = (inner_table->getColumn(r_name) != NULL);

        if (l_is_inner != r_is_inner)
        {
          require_prm(num_correlation == 0,
                      "Correlated scalar subquery with multiple "
                      "correlation predicates not supported.");
          outer_col_idx = l_is_inner ? r_idx : l_idx;
          inner_col_name = l_is_inner ? l_name : r_name;
          num_correlation++;
          continue;
        }
      }

      require_prm(num_non_corr < MAX_WHERE_CONJUNCTS,
                  "Too many non-correlation predicates.");
      num_non_corr++;
    }

    if (num_correlation != 1)
    {
      // No correlation found — leave as uncorrelated I_SUBQUERY
      require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                  "Too many WHERE conjuncts after decorrelation.");
      kept[num_kept++] = conjuncts[i];
      continue;
    }

    // Extract aggregate expression from original inner SQL
    ndbrequire(inner_stmt->sql_begin != NULL &&
               inner_stmt->sql_end != NULL);
    std::string inner_sql(inner_stmt->sql_begin,
                          inner_stmt->sql_end - inner_stmt->sql_begin);

    // Find the text between "SELECT " and " FROM"
    size_t select_pos = std::string::npos;
    for (size_t p = 0; p + 6 <= inner_sql.size(); p++)
    {
      if ((inner_sql[p] == 'S' || inner_sql[p] == 's') &&
          (inner_sql[p+1] == 'E' || inner_sql[p+1] == 'e') &&
          (inner_sql[p+2] == 'L' || inner_sql[p+2] == 'l') &&
          (inner_sql[p+3] == 'E' || inner_sql[p+3] == 'e') &&
          (inner_sql[p+4] == 'C' || inner_sql[p+4] == 'c') &&
          (inner_sql[p+5] == 'T' || inner_sql[p+5] == 't') &&
          (p + 6 == inner_sql.size() || inner_sql[p+6] == ' '))
      {
        select_pos = p;
        break;
      }
    }
    require_prm(select_pos != std::string::npos,
                "Could not find SELECT in inner subquery.");

    size_t from_pos = std::string::npos;
    for (size_t p = select_pos + 7; p + 4 <= inner_sql.size(); p++)
    {
      if ((inner_sql[p] == 'F' || inner_sql[p] == 'f') &&
          (inner_sql[p+1] == 'R' || inner_sql[p+1] == 'r') &&
          (inner_sql[p+2] == 'O' || inner_sql[p+2] == 'o') &&
          (inner_sql[p+3] == 'M' || inner_sql[p+3] == 'm') &&
          (p == 0 || inner_sql[p-1] == ' ') &&
          (p + 4 == inner_sql.size() || inner_sql[p+4] == ' '))
      {
        from_pos = p;
        break;
      }
    }
    require_prm(from_pos != std::string::npos,
                "Could not find FROM in inner subquery.");

    std::string agg_expr = inner_sql.substr(select_pos + 7,
                                             from_pos - select_pos - 7);
    // Trim
    while (!agg_expr.empty() && agg_expr.front() == ' ')
      agg_expr.erase(0, 1);
    while (!agg_expr.empty() && agg_expr.back() == ' ')
      agg_expr.pop_back();

    const char* inner_table_name = inner_stmt->table.c_str();
    LexCString inner_alias = inner_stmt->root_table != NULL
        ? inner_stmt->root_table->alias
        : inner_stmt->table;
    bool has_alias = (inner_stmt->root_table != NULL &&
                      !(inner_alias == inner_stmt->table));

    // Build rewritten inner SQL
    std::ostringstream sql;
    sql << "SELECT " << inner_col_name << ", " << agg_expr
        << " FROM " << inner_table_name;
    if (has_alias)
      sql << " AS " << inner_alias.c_str();

    // Handle non-correlation WHERE predicates
    if (num_non_corr > 0)
    {
      // Extract original WHERE text and strip correlation predicate
      size_t where_pos = std::string::npos;
      for (size_t p = 0; p + 5 <= inner_sql.size(); p++)
      {
        if ((inner_sql[p] == 'W' || inner_sql[p] == 'w') &&
            (inner_sql[p+1] == 'H' || inner_sql[p+1] == 'h') &&
            (inner_sql[p+2] == 'E' || inner_sql[p+2] == 'e') &&
            (inner_sql[p+3] == 'R' || inner_sql[p+3] == 'r') &&
            (inner_sql[p+4] == 'E' || inner_sql[p+4] == 'e') &&
            (p + 5 == inner_sql.size() || inner_sql[p+5] == ' '))
        {
          where_pos = p;
          break;
        }
      }

      if (where_pos != std::string::npos)
      {
        std::string where_text = inner_sql.substr(where_pos + 6);

        // Strip GROUP BY / ORDER BY / LIMIT
        for (const char* kw : {"GROUP BY", "ORDER BY", "LIMIT",
                                "group by", "order by", "limit"})
        {
          size_t kw_pos = where_text.find(kw);
          if (kw_pos != std::string::npos)
            where_text = where_text.substr(0, kw_pos);
        }

        while (!where_text.empty() &&
               (where_text.back() == ' ' || where_text.back() == '\n' ||
                where_text.back() == '\r' || where_text.back() == '\t'))
          where_text.pop_back();

        const char* outer_col_name = m_columns[outer_col_idx].c_str();
        std::vector<std::string> and_parts;
        size_t pos = 0;
        while (pos < where_text.size())
        {
          size_t and_pos = std::string::npos;
          for (size_t p = pos; p + 3 <= where_text.size(); p++)
          {
            if ((where_text[p] == 'A' || where_text[p] == 'a') &&
                (where_text[p+1] == 'N' || where_text[p+1] == 'n') &&
                (where_text[p+2] == 'D' || where_text[p+2] == 'd') &&
                (p == 0 || where_text[p-1] == ' ') &&
                (p + 3 >= where_text.size() || where_text[p+3] == ' '))
            {
              and_pos = p;
              break;
            }
          }
          std::string part;
          if (and_pos == std::string::npos)
          {
            part = where_text.substr(pos);
            pos = where_text.size();
          }
          else
          {
            part = where_text.substr(pos, and_pos - pos);
            pos = and_pos + 3;
          }
          while (!part.empty() && part.front() == ' ') part.erase(0, 1);
          while (!part.empty() && part.back() == ' ') part.pop_back();
          if (part.empty()) continue;

          bool is_correlation = false;
          if (part.find(outer_col_name) != std::string::npos &&
              part.find(inner_col_name) != std::string::npos &&
              part.find('=') != std::string::npos)
          {
            is_correlation = true;
          }
          if (!is_correlation)
            and_parts.push_back(part);
        }

        if (!and_parts.empty())
        {
          sql << " WHERE ";
          for (size_t p = 0; p < and_parts.size(); p++)
          {
            if (p > 0) sql << " AND ";
            sql << and_parts[p];
          }
        }
      }
    }

    sql << " GROUP BY " << inner_col_name;

    std::string sql_str = sql.str();
    size_t sql_len = sql_str.size();
    char* sql_buf = m_amalloc->alloc_exc<char>(sql_len + 1);
    memcpy(sql_buf, sql_str.c_str(), sql_len);
    sql_buf[sql_len] = '\0';

    // Create new inner SelectStatement
    SelectStatement* new_inner = m_amalloc->alloc_exc<SelectStatement>(1);
    new (new_inner) SelectStatement();
    new_inner->table = inner_stmt->table;
    new_inner->root_table = inner_stmt->root_table;
    new_inner->sql_begin = sql_buf;
    new_inner->sql_end = sql_buf + sql_len;

    // Clear m_col_is_inner for the outer column
    if (outer_col_idx < m_col_is_inner.size())
      m_col_is_inner[outer_col_idx] = false;

    // Build the outer correlation key reference
    ConditionalExpression* key_expr =
        m_amalloc->alloc_exc<ConditionalExpression>(1);
    key_expr->op = T_IDENTIFIER;
    key_expr->col_idx = outer_col_idx;

    // Find and update existing SubqueryInfo for this I_SUBQUERY node
    bool found_info = false;
    for (Uint32 si = 0; si < m_subquery_infos.size(); si++)
    {
      if (m_subquery_infos[si].ce_node == subq_child)
      {
        m_subquery_infos[si].ce_node = conjuncts[i];
        m_subquery_infos[si].inner_stmt = new_inner;
        m_subquery_infos[si].is_corr_scalar = true;
        m_subquery_infos[si].corr_values =
          new (m_amalloc->alloc_exc<DynamicArray<CorrelatedPair>>(1))
            DynamicArray<CorrelatedPair>(m_amalloc);
        found_info = true;
        break;
      }
    }

    if (!found_info)
    {
      // Register new SubqueryInfo
      SubqueryInfo info;
      info.ce_node = conjuncts[i];
      info.inner_stmt = new_inner;
      info.is_corr_scalar = true;
      info.corr_values =
        new (m_amalloc->alloc_exc<DynamicArray<CorrelatedPair>>(1))
          DynamicArray<CorrelatedPair>(m_amalloc);
      m_subquery_infos.push(info);
      m_has_subqueries = true;
    }

    // Transform comparison node in-place to I_CORR_SCALAR
    conjuncts[i]->op = I_CORR_SCALAR;
    conjuncts[i]->corr_scalar.cmp_op = cmp_op;
    conjuncts[i]->corr_scalar.cmp_expr = cmp_expr;
    conjuncts[i]->corr_scalar.key_expr = key_expr;
    conjuncts[i]->corr_scalar.stmt = new_inner;

    require_prm(num_kept < MAX_WHERE_CONJUNCTS,
                "Too many WHERE conjuncts after decorrelation.");
    kept[num_kept++] = conjuncts[i];
  }

  // Rebuild outer WHERE from kept conjuncts
  ConditionalExpression* rebuilt = NULL;
  for (Uint32 i = 0; i < num_kept; i++)
  {
    if (rebuilt == NULL)
    {
      rebuilt = kept[i];
    }
    else
    {
      ConditionalExpression* combined =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
      combined->op = T_AND;
      combined->args.left = rebuilt;
      combined->args.right = kept[i];
      rebuilt = combined;
    }
  }
  ast.where_expression = rebuilt;
}

void
RonSQLPreparer::reject_ignored_orderby_limit(const SelectStatement* stmt,
                                             const char* what,
                                             const char* name)
{
  if (stmt == NULL)
    return;
  const bool has_orderby = (stmt->orderby_columns != NULL);
  const bool has_limit = (stmt->limit >= 0);
  if (!has_orderby && !has_limit)
    return;
  std::basic_ostream<char>& err = *m_conf.err_stream;
  err << (has_orderby && has_limit ? "ORDER BY and LIMIT"
          : has_orderby ? "ORDER BY"
                        : "LIMIT")
      << " inside " << what;
  if (name != NULL)
    err << " '" << name << "'";
  err << " is not supported. It is only supported at the main SELECT"
         " level; accepting it here would silently ignore it, changing"
         " results compared to MySQL." << std::endl;
  throw RonSQLPermanentError(
      "ORDER BY / LIMIT in a CTE body or subquery is not supported.");
}

void
RonSQLPreparer::analyze_ctes()
{
  CteDefinition* cte = m_context.ast_root.cte_list;
  if (cte == NULL)
    return;

  m_has_ctes = true;
  std::basic_ostream<char>& err = *m_conf.err_stream;

  for (; cte != NULL; cte = cte->next)
  {
    /* Phase 0 of ronsql_orderby_limit_plan.md: the grammar parses
     * ORDER BY / LIMIT on CTE bodies but nothing ever applied them —
     * the body ran unordered and UNLIMITED, silently diverging from
     * MySQL.  Reject until body-level support ships. */
    reject_ignored_orderby_limit(cte->stmt, "CTE body", cte->name.c_str());

    /* Phase I.17: a CTE without GROUP BY is valid as long as every
     * output column is an aggregate (scalar aggregate CTE — one
     * synthetic group, exactly one materialized result row).  CTEs
     * with non-aggregate output columns still require GROUP BY. */
    bool has_groupby = (cte->stmt->groupby_columns != NULL);
    bool has_agg = false;
    bool has_non_agg_column = false;
    for (const Outputs* o = cte->stmt->outputs; o != NULL; o = o->next)
    {
      if (o->type == Outputs::Type::AGGREGATE ||
          o->type == Outputs::Type::AVG)
      {
        has_agg = true;
      }
      else
      {
        has_non_agg_column = true;
      }
    }
    /* Single-row key-lookup CTE bodies (cte_single_row_kernel_plan.md)
     * are exempt from the aggregate requirement: the kernel's
     * CTE_SINGLE_ROW mode materializes the (at most one) row natively.
     * The <= 1-row guarantee (full PK equality with constants) is
     * enforced at plan time in enforce_single_row_cte_body(). */
    if (!cte->stmt->is_single_row_cte)
    {
      if (!has_agg)
      {
        err << "CTE '" << cte->name.c_str()
            << "' must contain at least one aggregate function." << std::endl;
        throw RonSQLPermanentError("CTE without aggregate function.");
      }
      if (!has_groupby && has_non_agg_column)
      {
        err << "CTE '" << cte->name.c_str()
            << "' has non-aggregate output columns and must contain GROUP BY."
            << std::endl;
        throw RonSQLPermanentError(
            "CTE has non-aggregate columns without GROUP BY.");
      }
    }

    /* Validate: CTE must also have a FROM clause (enforced by parser) */
    require_prm(cte->stmt->root_table != NULL,
                "CTE has no FROM clause.");

    /* Validate: CTE name does not conflict with another CTE */
    for (const CteDefinition* other = m_context.ast_root.cte_list;
         other != cte; other = other->next)
    {
      if (strcmp(other->name.c_str(), cte->name.c_str()) == 0)
      {
        err << "Duplicate CTE name '" << cte->name.c_str() << "'."
            << std::endl;
        throw RonSQLPermanentError("Duplicate CTE name.");
      }
    }
  }
}

/* Single-row CTE bodies: the parse-time candidacy
 * (detect_single_row_ctes) is only sound when the body's WHERE
 * guarantees <= 1 row.  Enforce that here, where the dictionary is
 * loaded: every primary key column of the body's source table must be
 * bound by equality with a plain constant.  The bound side must be
 * constant — an expression containing another column (e.g.
 * pk = other_col + 1) can hold on several rows.  The accepted
 * constant set matches child_const_bound_op; I_SUBQUERY placeholders
 * are substituted with literal constants before any emit path runs
 * (a NULL substitute matches no row, which is the correct empty CTE).
 * Residual conjuncts beside the PK equalities are fine (they can only
 * reduce the row count) and stay on the body's normal bound/filter
 * path. */
void
RonSQLPreparer::enforce_single_row_cte_body(const CteDefinition* cte,
                                            QueryScope& scope)
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  const NdbDictionary::Table* tab =
      (scope.join_plan.num_ops > 0) ? scope.join_plan.ops[0].table : NULL;
  int nkeys = (tab != NULL) ? tab->getNoOfPrimaryKeys() : 0;
  bool covered = (tab != NULL && nkeys > 0 &&
                  scope.join_where_ce[0] != NULL);
  if (covered)
  {
    ConditionalExpression* w = simplify_ce(scope.join_where_ce[0], -1);
    ConditionalExpression* pk_const[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
    for (int k = 0; k < nkeys; k++) pk_const[k] = NULL;
    collect_pk_equalities(w, tab, pk_const);
    for (int k = 0; k < nkeys && covered; k++)
    {
      ConditionalExpression* c = pk_const[k];
      if (c == NULL ||
          (c->op != T_INT && c->op != T_FLOAT && c->op != T_STRING &&
           c->op != I_MYSQL_TIME && c->op != I_SUBQUERY))
        covered = false;
    }
  }
  if (!covered)
  {
    err << "CTE '" << cte->name.c_str()
        << "' has no aggregate functions and no GROUP BY, so it is"
           " only supported as a single-row key lookup: its WHERE"
           " clause must bind every primary key column of table '"
        << ((tab != NULL) ? tab->getName() : "?")
        << "' by equality with a constant.  Otherwise add GROUP BY"
           " and aggregate functions to the CTE body." << std::endl;
    throw RonSQLPermanentError(
        "Non-aggregating CTE body is not a single-row key lookup.");
  }
}

// Plan each CTE body into a per-scope JoinPlan and stash the resulting
// QueryScope in m_cte_scopes. Visibility is topological: CTE N sees
// CTEs 0..N-1. Achieved by temporarily truncating the cte_list at the
// predecessor's `next` pointer during the planner call, then restoring.
void
RonSQLPreparer::build_cte_scopes()
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  const char* db = m_conf.ndb ? m_conf.ndb->getDatabaseName() : nullptr;

  CteDefinition* prev = NULL;
  for (CteDefinition* cte = m_context.ast_root.cte_list;
       cte != NULL;
       prev = cte, cte = cte->next)
  {
    // Truncate list so findCte sees only predecessors of `cte`.
    CteDefinition* visible_head = m_context.ast_root.cte_list;
    CteDefinition* saved_next = NULL;
    if (prev == NULL) {
      visible_head = NULL;
    } else {
      saved_next = prev->next;
      prev->next = NULL;
    }

    QueryScope* scope = m_amalloc->alloc_exc<QueryScope>(1);
    new (scope) QueryScope(m_amalloc);
    QueryPlanner::plan(cte->stmt->root_table, cte->stmt->joins, m_dict,
                       err, scope->join_plan, m_conf.schema_cache, db,
                       visible_head);
    scope->table = scope->join_plan.ops[0].table;
    scope->agg = cte->stmt->agg;
    resolve_columns_for_cte_scope(*scope, *cte->stmt);
    classify_where_by_table(*scope, cte->stmt->where_expression);
    // Col-vs-col in a CTE-body WHERE is not yet supported and otherwise
    // either retries 10x (indexed left column, D12) or hangs the data node
    // (TABLE_SCAN body, D4).  Reject it permanently before scan-config
    // selection and emit.  See check_no_cte_body_col_vs_col.
    check_no_cte_body_col_vs_col(cte->stmt->where_expression);
    // Single-row CTE bodies: the parse-time candidacy is only sound
    // when the body WHERE binds the full primary key by equality
    // (<= 1 row).
    if (cte->stmt->is_single_row_cte)
      enforce_single_row_cte_body(cte, *scope);
    promote_left_to_inner_for_where(*scope);
    // child_bounds: constant bounds on multi-op CTE-body child index
    // scans (no-op for single-op bodies — the loop starts at op 1).
    assign_child_index_bounds(*scope);
    // W1: an index hint on a joined table inside a CTE body is rejected;
    // only the body's root scan honors FORCE/USE/IGNORE INDEX.
    reject_index_hints_on_joins(cte->stmt->joins);
    // Phase I.9: try to convert this CTE body's root scan into an
    // ordered index scan when the body's WHERE has bounds on an
    // indexed column.  Operates only on single-op real-table bodies
    // (the whole body WHERE belongs to the root op then); chained
    // CTEs (CTE_SCAN root) and multi-table bodies fall through
    // unchanged.  The root table's index hint (if any) constrains
    // index selection.
    if (scope->join_plan.num_ops == 1)
      select_root_scan_config(*scope, cte->stmt->where_expression,
                              cte->stmt->root_table);
    // Phase I.10: scalar MIN/MAX over a NOT NULL indexed column can
    // materialise through a full ordered index scan with maxRows=1.
    select_cte_body_minmax_index(*scope, cte);
    m_cte_scopes.push(scope);

    if (prev != NULL) {
      prev->next = saved_next;
    }
  }
}

// Fill in source NdbDictionary::Column* metadata for each CTE-output
// descriptor in `scope`. The CTE's virtual columns don't exist until
// execute-time virtual table construction, but build_cte_scopes()
// populates per-CTE scopes so we can walk back from a CTE output to its
// body-source column. The charset/precision/scale on that real column is
// what ResultPrinter and build_cte_virtual_tables need for source-backed
// CTE outputs. Non-COLUMN outputs (COUNT / SUM) remain without
// dict_column metadata because their result type is synthesized.
void
RonSQLPreparer::resolve_cte_output_columns_for_scope(QueryScope& scope)
{
  if (scope.resolved_columns == NULL) return;
  for (Uint32 col_idx = 0; col_idx < m_columns.size(); col_idx++) {
    QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[col_idx];
    if (ref.kind != QueryScope::ResolvedColumnRef::Kind::CteResultColumn)
      continue;
    if (ref.dict_column != NULL) continue;
    if (ref.cte_def_idx >= m_cte_scopes.size()) continue;
    QueryScope* cs = m_cte_scopes[ref.cte_def_idx];
    if (cs == NULL || cs->resolved_columns == NULL) continue;

    const Outputs* o = ref.cte_output;
    if (o == NULL) continue;

    if (o->type == Outputs::Type::COLUMN) {
      Uint32 src_col_idx = o->column.col_idx;
      const QueryScope::ResolvedColumnRef& src_ref =
          cs->resolved_columns[src_col_idx];
      if (src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn)
        ref.dict_column = src_ref.dict_column;
    } else if (o->type == Outputs::Type::AGGREGATE) {
      // MIN/MAX preserve the source type; SUM preserves the source SCALE
      // (SUM(DECIMAL(M,s)) is scale s, widened to DOUBLE in the kernel —
      // D1).  Plumb the source column's metadata for all three so the
      // result printer can format a DECIMAL-derived result with the source
      // scale (D15/D1); for int/float sources this carries scale 0, which
      // leaves the compact formatting unchanged.  COUNT has no source
      // column and stays unplumbed.
      TokenKind fun = o->aggregate.fun;
      if (fun != T_MIN && fun != T_MAX && fun != T_SUM) continue;
      AggregationAPICompiler::Expr* arg = o->aggregate.arg;
      if (arg == NULL || !arg->isLoad()) continue;
      Uint32 src_col_idx = arg->getLoadIdx();
      const QueryScope::ResolvedColumnRef& src_ref =
          cs->resolved_columns[src_col_idx];
      if (src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn)
        ref.dict_column = src_ref.dict_column;
    }
  }
}

void
RonSQLPreparer::resolve_cte_output_columns()
{
  // Iterate CTEs in declaration order so a chained CTE's body sees its
  // predecessors' already-resolved descriptors. The main scope is resolved
  // last because it can reference any CTE.
  for (Uint32 c = 0; c < m_cte_scopes.size(); c++) {
    if (m_cte_scopes[c] != NULL) {
      resolve_cte_output_columns_for_scope(*m_cte_scopes[c]);
    }
  }
  resolve_cte_output_columns_for_scope(m_main_scope);
}

// Reject CTE shapes that aren't supported end-to-end.  Defensive
// tripwire — today only flags CTE_SCAN-as-outer-join-child, a shape
// the planner doesn't currently produce (CTE children always become
// CTE_LOOKUP — see QueryPlanner.cpp:160).  A planner regression that
// started selecting CTE_SCAN for a non-root op under LEFT JOIN would
// otherwise reach DBSPJ and surface as a runtime crash; this guard
// turns it into a clean RonSQLPermanentError at prepare time.  See
// cte_filter_phase_g.md.
void
RonSQLPreparer::validate_cte_execution_shapes()
{
  auto check_plan = [](const JoinPlan& plan) {
    for (Uint32 i = 1; i < plan.num_ops; i++) {
      const JoinOp& op = plan.ops[i];
      if (op.type == JoinOp::CTE_SCAN &&
          op.match_type == JoinOp::LEFT_OUTER) {
        throw RonSQLPermanentError(
            "CTE_SCAN as outer-join child is not supported by NDB.");
      }
    }
  };
  check_plan(m_main_scope.join_plan);
  for (Uint32 c = 0; c < m_cte_scopes.size(); c++) {
    if (m_cte_scopes[c] != NULL) {
      check_plan(m_cte_scopes[c]->join_plan);
    }
  }
}

void
RonSQLPreparer::mark_scope_column_ref(bool* refs, Uint32 col_idx) const
{
  if (col_idx < m_columns.size())
    refs[col_idx] = true;
}

void
RonSQLPreparer::mark_scope_column_refs_ce(
    bool* refs,
    const ConditionalExpression* ce) const
{
  if (ce == NULL)
    return;
  switch (ce->op)
  {
  case T_IDENTIFIER:
    mark_scope_column_ref(refs, ce->col_idx);
    return;
  case T_IS:
    mark_scope_column_refs_ce(refs, ce->is.arg);
    return;
  case T_INTERVAL:
    mark_scope_column_refs_ce(refs, ce->interval.arg);
    return;
  case T_EXTRACT:
    mark_scope_column_refs_ce(refs, ce->extract.arg);
    return;
  case T_EXISTS:
  case I_SUBQUERY:
    return;
  case I_IN_SUBQUERY:
    mark_scope_column_refs_ce(refs, ce->in_subquery.expr);
    return;
  case I_CORR_SCALAR:
    mark_scope_column_refs_ce(refs, ce->corr_scalar.cmp_expr);
    mark_scope_column_refs_ce(refs, ce->corr_scalar.key_expr);
    return;
  case T_INT:
  case T_FLOAT:
  case T_STRING:
  case I_MYSQL_TIME:
  case T_SUM:
  case T_MIN:
  case T_MAX:
  case T_COUNT:
  case T_AVG:
  case T_NULL:
    return;
  default:
    mark_scope_column_refs_ce(refs, ce->args.left);
    mark_scope_column_refs_ce(refs, ce->args.right);
    return;
  }
}

void
RonSQLPreparer::mark_scope_column_refs_expr(
    bool* refs,
    const AggregationAPICompiler::Expr* expr) const
{
  if (expr == NULL)
    return;
  if (expr->isLoad())
  {
    mark_scope_column_ref(refs, expr->getLoadIdx());
    return;
  }
  if (expr->isLoadConstantInt())
    return;
  if (expr->isCase())
    mark_scope_column_refs_ce(refs, expr->getCaseCondition());
  mark_scope_column_refs_expr(refs, expr->getLeft());
  mark_scope_column_refs_expr(refs, expr->getRight());
}

bool*
RonSQLPreparer::collect_scope_column_refs(const SelectStatement& stmt)
{
  bool* refs = m_amalloc->alloc_exc<bool>(m_columns.size());
  for (Uint32 i = 0; i < m_columns.size(); i++)
    refs[i] = false;

  for (const Outputs* o = stmt.outputs; o != NULL; o = o->next)
  {
    switch (o->type)
    {
    case Outputs::Type::COLUMN:
      mark_scope_column_ref(refs, o->column.col_idx);
      break;
    case Outputs::Type::AGGREGATE:
      mark_scope_column_refs_expr(refs, o->aggregate.arg);
      break;
    case Outputs::Type::AVG:
      mark_scope_column_refs_expr(refs, o->avg.arg);
      break;
    case Outputs::Type::SUBQUERY_AGG:
      break;
    }
  }
  for (const GroupbyColumns* gb = stmt.groupby_columns; gb != NULL;
       gb = gb->next)
    mark_scope_column_ref(refs, gb->col_idx);
  for (const OrderbyColumns* ob = stmt.orderby_columns; ob != NULL;
       ob = ob->next)
  {
    if (ob->kind == OrderbyColumns::Kind::TABLE_COLUMN)
      mark_scope_column_ref(refs, ob->col_idx);
  }
  mark_scope_column_refs_ce(refs, stmt.where_expression);
  mark_scope_column_refs_ce(refs, stmt.having_expression);
  if (stmt.agg != NULL)
  {
    stmt.agg->for_each_expr([&](const AggregationAPICompiler::Expr* expr) {
      mark_scope_column_refs_expr(refs, expr);
    });
  }
  if (&stmt == &m_context.ast_root && m_main_scope.agg != NULL)
  {
    m_main_scope.agg->for_each_expr(
        [&](const AggregationAPICompiler::Expr* expr) {
      mark_scope_column_refs_expr(refs, expr);
    });
  }
  return refs;
}

// Populate resolved descriptors for column references that belong to `stmt`.
// The parser keeps a single global column namespace (m_columns /
// m_column_qualifiers), so I.23 resolves only the col_idx values reachable
// from this SELECT body's AST. This prevents aliases and columns from one CTE
// body from leaking into later CTE bodies or the main SELECT.
void
RonSQLPreparer::resolve_columns_for_scope(QueryScope& scope,
                                          const SelectStatement& stmt,
                                          bool main_scope)
{
  Uint32 num_cols = m_columns.size();
  QueryScope::ResolvedColumnRef* resolved =
      m_amalloc->alloc_exc<QueryScope::ResolvedColumnRef>(num_cols);
  bool* refs = collect_scope_column_refs(stmt);
  std::basic_ostream<char>& err = *m_conf.err_stream;

  if (main_scope && m_has_select_subqueries)
  {
    for (Uint32 i = 0; i < m_select_subquery_leaves.size(); i++)
    {
      const SelectSubqueryLeaf& leaf = m_select_subquery_leaves[i];
      if (!leaf.is_count_star)
        mark_scope_column_ref(refs, leaf.inner_agg_col_idx);
      mark_scope_column_refs_ce(refs, leaf.inner_filter);
      mark_scope_column_refs_ce(refs, leaf.cross_table_filter);
      for (Uint32 c = 0; c < m_columns.size(); c++)
      {
        if (m_column_qualifiers[c].str != NULL &&
            m_column_qualifiers[c].len == leaf.outer_join_table.len &&
            strncmp(m_column_qualifiers[c].str, leaf.outer_join_table.str,
                    leaf.outer_join_table.len) == 0 &&
            m_columns[c].len == leaf.outer_join_col.len &&
            strncmp(m_columns[c].str, leaf.outer_join_col.str,
                    leaf.outer_join_col.len) == 0)
        {
          mark_scope_column_ref(refs, c);
        }
      }
    }
  }

  JoinPlan& plan = scope.join_plan;

  for (Uint32 col_idx = 0; col_idx < num_cols; col_idx++) {
    new (&resolved[col_idx]) QueryScope::ResolvedColumnRef();

    if (!refs[col_idx]) {
      continue;
    }

    const char* col_name = m_columns[col_idx].c_str();
    const char* qualifier = m_column_qualifiers[col_idx].c_str();

    if (main_scope &&
        m_col_is_alias.size() > col_idx && m_col_is_alias[col_idx]) {
      resolved[col_idx].kind = QueryScope::ResolvedColumnRef::Kind::AliasOnly;
      continue;
    }
    if (main_scope && m_has_select_subqueries &&
        m_col_is_inner.size() > col_idx && m_col_is_inner[col_idx] &&
        qualifier == NULL) {
      bool is_subquery_agg_alias = false;
      for (const Outputs* o = stmt.outputs; o != NULL; o = o->next) {
        if (o->type == Outputs::Type::SUBQUERY_AGG &&
            o->output_name.len == strlen(col_name) &&
            strncmp(o->output_name.str, col_name, o->output_name.len) == 0) {
          is_subquery_agg_alias = true;
          break;
        }
      }
      if (is_subquery_agg_alias) {
        resolved[col_idx].kind = QueryScope::ResolvedColumnRef::Kind::AliasOnly;
        continue;
      }
    }

    if (qualifier != NULL) {
      bool found_qualifier = false;
      for (Uint32 t = 0; t < plan.num_ops; t++) {
        if (strcmp(plan.ops[t].alias.c_str(), qualifier) != 0) continue;
        found_qualifier = true;
        JoinOp& op = plan.ops[t];
        if (op.type == JoinOp::CTE_LOOKUP || op.type == JoinOp::CTE_SCAN) {
          const CteDefinition* cte = op.cte_def;
          require_prm(cte != NULL, "CTE op has no CTE definition.");
          Uint32 cte_col_idx = 0;
          for (const Outputs* o = cte->stmt->outputs; o;
               o = o->next, cte_col_idx++) {
            if (o->output_name.len == strlen(col_name) &&
                strncmp(o->output_name.str, col_name, o->output_name.len) == 0) {
              resolved[col_idx].kind =
                  QueryScope::ResolvedColumnRef::Kind::CteResultColumn;
              resolved[col_idx].join_op_idx = t;
              resolved[col_idx].attr_id = (NdbAttrId)cte_col_idx;
              resolved[col_idx].cte_def_idx = op.cte_def_idx;
              resolved[col_idx].cte_result_idx = cte_col_idx;
              resolved[col_idx].cte_output = o;
              break;
            }
          }
        } else if (op.table != NULL) {
          const NdbDictionary::Column* col = op.table->getColumn(col_name);
          if (col != NULL) {
            resolved[col_idx].kind =
                QueryScope::ResolvedColumnRef::Kind::StoredColumn;
            resolved[col_idx].join_op_idx = t;
            resolved[col_idx].attr_id = col->getAttrId();
            resolved[col_idx].dict_column = col;
          }
        }
        break;
      }
      if (resolved[col_idx].kind ==
          QueryScope::ResolvedColumnRef::Kind::Unresolved) {
        if (found_qualifier) {
          err << "Column '" << qualifier << "." << col_name
              << "' not found in table or CTE '" << qualifier << "'."
              << endl;
          throw RonSQLPermanentError("Column not found.");
        }
        err << "Unknown table or CTE alias '" << qualifier
            << "' in column '" << qualifier << "." << col_name << "'."
            << endl;
        throw RonSQLPermanentError("Unknown table alias.");
      }
    } else {
      Uint32 match_count = 0;
      Uint32 match_table = 0;
      const NdbDictionary::Column* match_col = NULL;
      NdbAttrId match_attr_id = -1;
      const Outputs* match_cte_output = NULL;
      Uint32 match_cte_def_idx = 0;
      Uint32 match_cte_result_idx = 0;
      bool match_is_cte = false;
      for (Uint32 t = 0; t < plan.num_ops; t++) {
        JoinOp& op = plan.ops[t];
        if (op.type == JoinOp::CTE_LOOKUP || op.type == JoinOp::CTE_SCAN) {
          const CteDefinition* cte = op.cte_def;
          require_prm(cte != NULL, "CTE op has no CTE definition.");
          Uint32 cte_col_idx = 0;
          for (const Outputs* o = cte->stmt->outputs; o;
               o = o->next, cte_col_idx++) {
            if (o->output_name.len == strlen(col_name) &&
                strncmp(o->output_name.str, col_name, o->output_name.len) == 0) {
              match_count++;
              match_table = t;
              match_col = NULL;
              match_attr_id = (NdbAttrId)cte_col_idx;
              match_cte_output = o;
              match_cte_def_idx = op.cte_def_idx;
              match_cte_result_idx = cte_col_idx;
              match_is_cte = true;
              break;
            }
          }
        } else if (op.table != NULL) {
          const NdbDictionary::Column* col = op.table->getColumn(col_name);
          if (col != NULL) {
            match_count++;
            match_table = t;
            match_col = col;
            match_attr_id = col->getAttrId();
            match_cte_output = NULL;
            match_cte_def_idx = 0;
            match_cte_result_idx = 0;
            match_is_cte = false;
          }
        }
      }
      if (match_count == 1) {
        if (match_is_cte) {
          resolved[col_idx].kind =
              QueryScope::ResolvedColumnRef::Kind::CteResultColumn;
          resolved[col_idx].cte_def_idx = match_cte_def_idx;
          resolved[col_idx].cte_result_idx = match_cte_result_idx;
          resolved[col_idx].cte_output = match_cte_output;
        } else {
          resolved[col_idx].kind =
              QueryScope::ResolvedColumnRef::Kind::StoredColumn;
          resolved[col_idx].dict_column = match_col;
        }
        resolved[col_idx].join_op_idx = match_table;
        resolved[col_idx].attr_id = match_attr_id;
      } else if (match_count == 0) {
        err << "Column '" << col_name << "' not found in any visible "
            << "table or CTE." << endl;
        throw RonSQLPermanentError("Column not found.");
      } else {
        err << "Ambiguous column '" << col_name
            << "' found in multiple tables or CTEs. Use 'table.column' "
            << "syntax." << endl;
        throw RonSQLPermanentError("Ambiguous column.");
      }
    }
  }

  scope.resolved_columns = resolved;
}

void
RonSQLPreparer::resolve_columns_for_cte_scope(QueryScope& scope,
                                              const SelectStatement& stmt)
{
  resolve_columns_for_scope(scope, stmt, false);
}

void
RonSQLPreparer::analyze_subqueries()
{
  analyze_subqueries_ce(m_context.ast_root.where_expression);
  analyze_subqueries_ce(m_context.ast_root.having_expression);
}

void
RonSQLPreparer::analyze_subqueries_ce(ConditionalExpression* ce)
{
  if (ce == NULL) return;
  switch (ce->op)
  {
  case I_SUBQUERY:
  {
    /* Phase 0 of ronsql_orderby_limit_plan.md: subquery-body ORDER BY /
     * LIMIT was parsed but never applied (and the decorrelation
     * rewrites additionally strip it textually) — reject instead of
     * silently changing results.  Checked here on the ORIGINAL parsed
     * statement, before any rewrite replaces it. */
    reject_ignored_orderby_limit(ce->subquery.stmt, "a scalar subquery",
                                 NULL);
    SubqueryInfo info;
    info.ce_node = ce;
    info.inner_stmt = ce->subquery.stmt;
    m_subquery_infos.push(info);
    m_has_subqueries = true;
    return;
  }
  case T_EXISTS:
    // Transformed into I_IN_SUBQUERY in decorrelate_exists() during load().
    // SubqueryInfo is registered there, not here.  The transform builds a
    // fresh inner statement from rewritten text, so the ORDER BY / LIMIT
    // rejection must inspect the original statement here.
    reject_ignored_orderby_limit(ce->subquery.stmt, "an EXISTS subquery",
                                 NULL);
    return;
  case T_NOT:
  case T_EXCLAMATION:
    // NOT EXISTS handled in decorrelate_exists() — T_NOT wraps I_IN_SUBQUERY.
    analyze_subqueries_ce(ce->args.left);
    return;
  case I_IN_SUBQUERY:
  {
    reject_ignored_orderby_limit(ce->in_subquery.stmt, "an IN subquery",
                                 NULL);
    SubqueryInfo info;
    info.ce_node = ce;
    info.inner_stmt = ce->in_subquery.stmt;
    info.is_in_subquery = true;
    info.in_expr = ce->in_subquery.expr;
    info.in_values = new (m_amalloc->alloc_exc<DynamicArray<SubqueryResult>>(1))
      DynamicArray<SubqueryResult>(m_amalloc);
    m_subquery_infos.push(info);
    m_has_subqueries = true;
    return;
  }
  case I_CORR_SCALAR:
    // Registered in decorrelate_scalar(). No further action.
    return;
  case T_IS:
    analyze_subqueries_ce(ce->is.arg);
    return;
  case T_INTERVAL:
    analyze_subqueries_ce(ce->interval.arg);
    return;
  case T_EXTRACT:
    analyze_subqueries_ce(ce->extract.arg);
    return;
  case T_IDENTIFIER:
  case T_INT:
  case T_FLOAT:
  case T_STRING:
  case I_MYSQL_TIME:
  case T_SUM:
  case T_MIN:
  case T_MAX:
  case T_COUNT:
  case T_AVG:
  case T_NULL:
    return;
  default:
    // Binary/unary operators: recurse into both sides
    analyze_subqueries_ce(ce->args.left);
    analyze_subqueries_ce(ce->args.right);
    return;
  }
}

/*
 * analyze_select_subqueries()
 *
 * Walk the SELECT output list looking for SUBQUERY_AGG entries (correlated
 * scalar subqueries with aggregation).  Validate each inner query is a
 * single-table aggregate with a simple equi-join correlation to the outer
 * table.  If all valid and the outer query has GROUP BY or is implicit
 * aggregation, set m_has_select_subqueries = true and populate the leaf
 * descriptors.  Then merge same-table subqueries and rewrite into joins.
 */
void
RonSQLPreparer::analyze_select_subqueries()
{
  SelectStatement &ast = m_context.ast_root;
  Uint32 output_idx = 0;
  bool has_any = false;

  for (Outputs *out = ast.outputs; out != NULL; out = out->next, output_idx++) {
    if (out->type == Outputs::Type::SUBQUERY_AGG) {
      has_any = true;
    }
  }

  if (!has_any) return;

  // For now, reject if the outer query already has GROUP BY — the
  // semantics of GROUP BY + correlated subquery are ambiguous (the
  // subquery references a column not in GROUP BY).
  // Supported: no GROUP BY → per-row correlated subquery results,
  // pushed as join with auto-injected GROUP BY on correlation key.
  require_prm(ast.groupby_columns == NULL,
              "SELECT-list subqueries with GROUP BY are not yet supported. "
              "Use the query without GROUP BY for per-row results.");

  // Validate each subquery
  output_idx = 0;
  for (Outputs *out = ast.outputs; out != NULL; out = out->next, output_idx++) {
    if (out->type != Outputs::Type::SUBQUERY_AGG) continue;

    SelectStatement *inner = out->subquery_agg.stmt;
    require_prm(inner != NULL,
                "SELECT-list subquery has NULL inner statement.");

    // Must be single-table (no joins)
    require_prm(inner->joins == NULL,
                "SELECT-list subquery with JOIN is not supported for pushdown.");

    // Must have no GROUP BY or HAVING in inner query
    require_prm(inner->groupby_columns == NULL,
                "SELECT-list subquery must not have GROUP BY.");
    require_prm(inner->having_expression == NULL,
                "SELECT-list subquery must not have HAVING.");

    // Phase 0 of ronsql_orderby_limit_plan.md: parsed but never applied.
    reject_ignored_orderby_limit(inner, "a SELECT-list subquery", NULL);

    // Inner SELECT must have exactly one aggregate output
    Outputs *inner_out = inner->outputs;
    require_prm(inner_out != NULL,
                "SELECT-list subquery has empty output list.");
    require_prm(inner_out->next == NULL,
                "SELECT-list subquery must have exactly one output.");
    require_prm(inner_out->type == Outputs::Type::AGGREGATE,
                "SELECT-list subquery output must be an aggregate function.");

    TokenKind agg_fun = inner_out->aggregate.fun;
    require_prm(agg_fun == T_SUM || agg_fun == T_COUNT ||
                agg_fun == T_MIN || agg_fun == T_MAX,
                "SELECT-list subquery aggregate must be SUM, COUNT, MIN, or MAX.");

    // Inner WHERE must contain exactly one equi-join correlation predicate
    // of the form: inner_table.col = outer_table.col
    ConditionalExpression *inner_where = inner->where_expression;
    require_prm(inner_where != NULL,
                "SELECT-list subquery must have a WHERE clause with correlation.");

    // Extract the correlation predicate from the inner WHERE.
    // The WHERE can be a single equality (correlation only) or
    // T_AND(correlation, additional_filter).
    // Flatten AND conjuncts and find the one equality with one inner
    // and one outer column reference.
    ConditionalExpression *correlation_eq = NULL;
    ConditionalExpression *inner_filter = NULL;

    if (inner_where->op == T_EQUALS) {
      // Simple case: single correlation predicate, no additional filter
      correlation_eq = inner_where;
    } else if (inner_where->op == T_AND) {
      // Flatten AND and find correlation predicate
      // We support: correlation AND filter (any nesting depth)
      ConditionalExpression *conjuncts[32];
      Uint32 num_conjuncts = 0;
      std::function<void(ConditionalExpression*)> flatten =
          [&](ConditionalExpression* ce) {
        if (ce->op == T_AND) {
          flatten(ce->args.left);
          flatten(ce->args.right);
        } else if (num_conjuncts < 32) {
          conjuncts[num_conjuncts++] = ce;
        }
      };
      flatten(inner_where);

      // Find the correlation predicate (equality between inner and outer col)
      LexCString inner_table_name = inner->root_table->alias;
      for (Uint32 ci = 0; ci < num_conjuncts; ci++) {
        if (conjuncts[ci]->op != T_EQUALS) continue;
        ConditionalExpression *l = conjuncts[ci]->args.left;
        ConditionalExpression *r = conjuncts[ci]->args.right;
        if (l == NULL || r == NULL) continue;
        if (l->op != T_IDENTIFIER || r->op != T_IDENTIFIER) continue;
        // Check if one side is inner table, other is outer
        LexCString lq = m_column_qualifiers[l->col_idx];
        LexCString rq = m_column_qualifiers[r->col_idx];
        bool l_inner = (lq.str != NULL && lq.len == inner_table_name.len &&
                        strncmp(lq.str, inner_table_name.str, lq.len) == 0);
        bool r_inner = (rq.str != NULL && rq.len == inner_table_name.len &&
                        strncmp(rq.str, inner_table_name.str, rq.len) == 0);
        if (l_inner != r_inner) {
          correlation_eq = conjuncts[ci];
          // Build remaining filter from other conjuncts
          for (Uint32 fi = 0; fi < num_conjuncts; fi++) {
            if (fi == ci) continue;
            if (inner_filter == NULL) {
              inner_filter = conjuncts[fi];
            } else {
              ConditionalExpression *combined =
                  m_amalloc->alloc_exc<ConditionalExpression>(1);
              combined->op = T_AND;
              combined->args.left = inner_filter;
              combined->args.right = conjuncts[fi];
              inner_filter = combined;
            }
          }
          break;
        }
      }
    }

    require_prm(correlation_eq != NULL,
                "SELECT-list subquery WHERE must contain a correlation "
                "predicate (inner.col = outer.col).");

    // Both sides must be column references (T_IDENTIFIER with col_idx)
    ConditionalExpression *lhs = correlation_eq->args.left;
    ConditionalExpression *rhs = correlation_eq->args.right;
    require_prm(lhs != NULL && rhs != NULL,
                "Correlation predicate must have two sides.");
    require_prm(lhs->op == T_IDENTIFIER && rhs->op == T_IDENTIFIER,
                "Correlation predicate must compare two columns.");

    // Determine which side is inner vs outer by matching table qualifier
    // against the inner table name
    LexCString inner_table = inner->root_table->alias;
    Uint32 lhs_col_idx = lhs->col_idx;
    Uint32 rhs_col_idx = rhs->col_idx;
    LexCString lhs_qualifier = m_column_qualifiers[lhs_col_idx];
    LexCString rhs_qualifier = m_column_qualifiers[rhs_col_idx];

    bool lhs_is_inner =
        (lhs_qualifier.str != NULL && inner_table.str != NULL &&
         lhs_qualifier.len == inner_table.len &&
         strncmp(lhs_qualifier.str, inner_table.str, inner_table.len) == 0);
    bool rhs_is_inner =
        (rhs_qualifier.str != NULL && inner_table.str != NULL &&
         rhs_qualifier.len == inner_table.len &&
         strncmp(rhs_qualifier.str, inner_table.str, inner_table.len) == 0);

    require_prm(lhs_is_inner != rhs_is_inner,
                "Correlation predicate must reference one inner and one "
                "outer column.");

    LexCString inner_col, outer_col, outer_table;
    Uint32 outer_col_idx;
    if (lhs_is_inner) {
      inner_col = m_columns[lhs_col_idx];
      outer_col = m_columns[rhs_col_idx];
      outer_table = rhs_qualifier;
      outer_col_idx = rhs_col_idx;
    } else {
      inner_col = m_columns[rhs_col_idx];
      outer_col = m_columns[lhs_col_idx];
      outer_table = lhs_qualifier;
      outer_col_idx = lhs_col_idx;
    }
    // Clear m_col_is_inner for the outer correlation column — it was
    // registered as "inner" because parsing occurred inside a subquery,
    // but it references the outer table and must be resolved by load_join().
    if (outer_col_idx < m_col_is_inner.size())
      m_col_is_inner[outer_col_idx] = false;

    // Extract the inner aggregate column's col_idx from the expression tree.
    // For simple SUM(col), the arg is a Load expression with idx = col_idx.
    // For COUNT(*), the arg is a LoadImmediate(1) — no column reference needed.
    AggregationAPICompiler_Expr *agg_arg = inner_out->aggregate.arg;
    bool is_count_star = (agg_fun == T_COUNT && agg_arg != NULL &&
                          !agg_arg->isLoad());
    require_prm(agg_arg != NULL && (agg_arg->isLoad() || is_count_star),
                "SELECT-list subquery aggregate argument must be a simple "
                "column reference (or COUNT(*)).");
    Uint32 inner_agg_col_idx = is_count_star ? 0 : agg_arg->getLoadIdx();

    SelectSubqueryLeaf leaf;
    leaf.inner_stmt = inner;
    leaf.output_node = out;
    leaf.output_idx = output_idx;
    leaf.inner_table_name = inner->root_table->name;
    leaf.inner_table_alias = inner_table;
    leaf.inner_join_col = inner_col;
    leaf.outer_join_col = outer_col;
    leaf.outer_join_table = outer_table;
    leaf.agg_fun = agg_fun;
    leaf.inner_agg_col = is_count_star ? LexCString{"*", 1}
                                       : m_columns[inner_agg_col_idx];
    leaf.inner_agg_col_idx = inner_agg_col_idx;
    leaf.combined_agg_slot = 0;
    leaf.merged_leaf_idx = 0;
    leaf.use_inner_join = false;  // LEFT OUTER: unmatched entities get NULL
    leaf.is_count_star = is_count_star;
    // Separate inner-only predicates from cross-table predicates.
    // Inner-only → pushed as NdbScanFilter on child scan.
    // Cross-table → converted to conditional aggregation via BranchReg.
    ConditionalExpression *inner_only_filter = NULL;
    ConditionalExpression *cross_table_filter_ce = NULL;

    if (inner_filter != NULL) {
      // Flatten the inner_filter and classify each conjunct
      ConditionalExpression *filter_conjuncts[32];
      Uint32 num_filter_conjuncts = 0;
      std::function<void(ConditionalExpression*)> flatten_filter =
          [&](ConditionalExpression* ce) {
        if (ce->op == T_AND) {
          flatten_filter(ce->args.left);
          flatten_filter(ce->args.right);
        } else if (num_filter_conjuncts < 32) {
          filter_conjuncts[num_filter_conjuncts++] = ce;
        }
      };
      flatten_filter(inner_filter);

      LexCString itbl = inner->root_table->alias;
      for (Uint32 fi = 0; fi < num_filter_conjuncts; fi++) {
        // Check if this conjunct references only inner table columns
        bool has_outer_ref = false;
        std::function<void(ConditionalExpression*)> check_refs =
            [&](ConditionalExpression* ce) {
          if (ce == NULL) return;
          if (ce->op == T_IDENTIFIER) {
            Uint32 cidx = ce->col_idx;
            if (cidx < m_column_qualifiers.size() &&
                m_column_qualifiers[cidx].str != NULL) {
              LexCString q = m_column_qualifiers[cidx];
              if (!(q.len == itbl.len &&
                    strncmp(q.str, itbl.str, q.len) == 0)) {
                has_outer_ref = true;
              }
            }
            return;
          }
          if (ce->op == T_IS) { check_refs(ce->is.arg); return; }
          if (ce->op == T_INT || ce->op == T_FLOAT ||
              ce->op == T_STRING || ce->op == T_NULL) return;
          check_refs(ce->args.left);
          check_refs(ce->args.right);
        };
        check_refs(filter_conjuncts[fi]);

        ConditionalExpression **target =
            has_outer_ref ? &cross_table_filter_ce : &inner_only_filter;
        if (*target == NULL) {
          *target = filter_conjuncts[fi];
        } else {
          ConditionalExpression *combined =
              m_amalloc->alloc_exc<ConditionalExpression>(1);
          combined->op = T_AND;
          combined->args.left = *target;
          combined->args.right = filter_conjuncts[fi];
          *target = combined;
        }
      }
    }

    leaf.inner_filter = inner_only_filter;
    leaf.cross_table_filter = cross_table_filter_ce;

    m_select_subquery_leaves.push(leaf);
  }

  if (m_select_subquery_leaves.size() == 0) return;

  m_has_select_subqueries = true;

  // Mark HAVING identifier columns that match output aliases as "inner"
  // so load_join() skips them.  They'll be resolved in compile().
  if (ast.having_expression != NULL) {
    // Collect output alias names from SUBQUERY_AGG outputs
    std::set<std::string> alias_names;
    for (Outputs *out = ast.outputs; out != NULL; out = out->next) {
      if (out->type == Outputs::Type::SUBQUERY_AGG &&
          out->output_name.str != NULL && out->output_name.len > 0) {
        alias_names.insert(std::string(out->output_name.str,
                                       out->output_name.len));
      }
    }
    // Walk columns and mark any that match an alias name
    for (Uint32 c = 0; c < m_columns.size(); c++) {
      if (m_column_qualifiers[c].str == NULL &&  // unqualified
          alias_names.count(std::string(m_columns[c].str,
                                        m_columns[c].len)) > 0) {
        // Ensure m_col_is_inner is large enough
        while (m_col_is_inner.size() <= c)
          m_col_is_inner.push(false);
        m_col_is_inner[c] = true;
      }
    }
  }

  // Merge subqueries on same table+join into single leaves
  merge_same_table_subqueries();

  // Inject join clauses for each merged leaf
  rewrite_select_subqueries_as_joins();
}

/*
 * merge_same_table_subqueries()
 *
 * Group SelectSubqueryLeaf entries by (inner_table_name, inner_join_col,
 * outer_join_col).  Subqueries in the same group share a single leaf node
 * with a combined aggregation program.
 */
void
RonSQLPreparer::merge_same_table_subqueries()
{
  Uint32 n = m_select_subquery_leaves.size();
  bool assigned[MAX_SQL_SUBQUERIES] = {};
  require_prm(n <= MAX_SQL_SUBQUERIES, "Too many SELECT-list subqueries.");

  Uint32 agg_slot = 0;
  for (Uint32 i = 0; i < n; i++) {
    if (assigned[i]) continue;
    SelectSubqueryLeaf &base = m_select_subquery_leaves[i];

    MergedLeaf ml;
    ml.first_subquery_idx = i;
    ml.num_aggs = 1;
    ml.plan_op_idx = 0;
    Uint32 ml_idx = m_merged_leaves.size();

    base.merged_leaf_idx = ml_idx;
    base.combined_agg_slot = agg_slot++;
    assigned[i] = true;

    for (Uint32 j = i + 1; j < n; j++) {
      if (assigned[j]) continue;
      SelectSubqueryLeaf &other = m_select_subquery_leaves[j];
      if (base.inner_table_name.len == other.inner_table_name.len &&
          strncmp(base.inner_table_name.str, other.inner_table_name.str,
                  base.inner_table_name.len) == 0 &&
          base.inner_join_col.len == other.inner_join_col.len &&
          strncmp(base.inner_join_col.str, other.inner_join_col.str,
                  base.inner_join_col.len) == 0 &&
          base.outer_join_col.len == other.outer_join_col.len &&
          strncmp(base.outer_join_col.str, other.outer_join_col.str,
                  base.outer_join_col.len) == 0) {
        other.merged_leaf_idx = ml_idx;
        other.combined_agg_slot = agg_slot++;
        ml.num_aggs++;
        assigned[j] = true;

        // Remap column qualifiers from other's alias to base's alias
        // so that load_join() resolves them against the single joined table.
        // Also update inner_table_alias so cross-table filter classification
        // in execute_join() matches the remapped qualifiers.
        LexCString other_alias = other.inner_table_alias;
        LexCString base_alias = base.inner_table_alias;
        other.inner_table_alias = base_alias;
        for (Uint32 c = 0; c < m_column_qualifiers.size(); c++) {
          if (m_column_qualifiers[c].str != NULL &&
              m_column_qualifiers[c].len == other_alias.len &&
              strncmp(m_column_qualifiers[c].str, other_alias.str,
                      other_alias.len) == 0) {
            m_column_qualifiers[c] = base_alias;
          }
        }
      }
    }

    m_merged_leaves.push(ml);
  }
}

/*
 * rewrite_select_subqueries_as_joins()
 *
 * For each merged leaf, inject a JoinClause into the outer query's join list.
 * This makes the inner table visible to load() and QueryPlanner::plan().
 */
void
RonSQLPreparer::rewrite_select_subqueries_as_joins()
{
  SelectStatement &ast = m_context.ast_root;

  for (Uint32 i = 0; i < m_merged_leaves.size(); i++) {
    MergedLeaf &ml = m_merged_leaves[i];
    SelectSubqueryLeaf &base = m_select_subquery_leaves[ml.first_subquery_idx];

    JoinCondition *cond = m_amalloc->alloc_exc<JoinCondition>(1);
    cond->child_table = base.inner_table_alias;
    cond->child_column = base.inner_join_col;
    cond->parent_table = base.outer_join_table;
    cond->parent_column = base.outer_join_col;
    cond->next = NULL;

    JoinClause *jc = m_amalloc->alloc_exc<JoinClause>(1);
    jc->join_type = base.use_inner_join
        ? JoinClause::INNER_JOIN : JoinClause::LEFT_OUTER_JOIN;
    jc->table = *base.inner_stmt->root_table;
    jc->conditions = cond;
    jc->next = NULL;

    // Append to end of join list
    if (ast.joins == NULL) {
      ast.joins = jc;
    } else {
      JoinClause *last = ast.joins;
      while (last->next != NULL) last = last->next;
      last->next = jc;
    }
  }

  // Auto-inject GROUP BY on the outer correlation key if there's no
  // explicit GROUP BY.  This gives per-parent-row aggregation semantics.
  if (ast.groupby_columns == NULL) {
    // Use the outer correlation key from the first subquery leaf.
    // All leaves correlate to the outer table — use the first one's key.
    SelectSubqueryLeaf &first = m_select_subquery_leaves[0];

    // Find col_idx for the outer correlation column.
    // It's already in m_columns from the subquery parsing.
    Uint32 outer_col_idx = UINT32_MAX;
    for (Uint32 c = 0; c < m_columns.size(); c++) {
      if (m_column_qualifiers[c].str != NULL &&
          m_column_qualifiers[c].len == first.outer_join_table.len &&
          strncmp(m_column_qualifiers[c].str, first.outer_join_table.str,
                  first.outer_join_table.len) == 0 &&
          m_columns[c].len == first.outer_join_col.len &&
          strncmp(m_columns[c].str, first.outer_join_col.str,
                  first.outer_join_col.len) == 0) {
        outer_col_idx = c;
        break;
      }
    }
    require_prm(outer_col_idx != UINT32_MAX,
                "Could not find outer correlation column for GROUP BY injection.");

    GroupbyColumns *gb = m_amalloc->alloc_exc<GroupbyColumns>(1);
    gb->col_idx = outer_col_idx;
    gb->next = NULL;
    ast.groupby_columns = gb;

    // Also add all other COLUMN outputs to GROUP BY so the ResultPrinter
    // doesn't reject them as ungrouped.  Since we GROUP BY the correlation
    // key (typically PK), other outer columns are functionally dependent.
    for (Outputs *out = ast.outputs; out != NULL; out = out->next) {
      if (out->type == Outputs::Type::COLUMN &&
          out->column.col_idx != outer_col_idx) {
        GroupbyColumns *extra = m_amalloc->alloc_exc<GroupbyColumns>(1);
        extra->col_idx = out->column.col_idx;
        extra->next = NULL;
        // Append to end of GROUP BY list
        GroupbyColumns *last = ast.groupby_columns;
        while (last->next != NULL) last = last->next;
        last->next = extra;
      }
    }
  }

  // Clear m_col_is_inner for columns from the rewritten subquery tables.
  // These columns were registered as "inner" during subquery parsing but
  // are now part of the join and need to be resolved by load_join().
  for (Uint32 i = 0; i < m_select_subquery_leaves.size(); i++) {
    SelectSubqueryLeaf &leaf = m_select_subquery_leaves[i];
    LexCString inner_alias = leaf.inner_table_alias;
    for (Uint32 c = 0; c < m_columns.size(); c++) {
      if (c < m_col_is_inner.size() && m_col_is_inner[c] &&
          c < m_column_qualifiers.size() &&
          m_column_qualifiers[c].str != NULL &&
          m_column_qualifiers[c].len == inner_alias.len &&
          strncmp(m_column_qualifiers[c].str, inner_alias.str,
                  inner_alias.len) == 0) {
        m_col_is_inner[c] = false;
      }
    }
  }
}

void
RonSQLPreparer::compile()
{
  // For multi-leaf subquery pushdown: rewrite SUBQUERY_AGG outputs to
  // AGGREGATE outputs with the correct combined_agg_slot.  This makes
  // the ResultPrinter treat them as regular aggregate results.
  if (m_has_select_subqueries) {
    for (Uint32 i = 0; i < m_select_subquery_leaves.size(); i++) {
      SelectSubqueryLeaf &sl = m_select_subquery_leaves[i];
      Outputs *out = sl.output_node;
      ndbrequire(out->type == Outputs::Type::SUBQUERY_AGG);
      out->type = Outputs::Type::AGGREGATE;
      out->aggregate.fun = sl.agg_fun;
      out->aggregate.arg = NULL;  // not used for result fetching
      out->aggregate.agg_index = sl.combined_agg_slot;
      out->aggregate.implicit_scalar_pair_op = false;
    }
  }

  // Resolve output aliases in HAVING expression for subquery aggregation.
  // Walk the HAVING tree and map identifier nodes to their agg_index
  // based on output alias names.
  if (m_has_select_subqueries &&
      m_context.ast_root.having_expression != NULL) {
    // Build alias → agg_index map from rewritten outputs
    std::map<std::string, Uint32> alias_map;
    for (Outputs *out = m_context.ast_root.outputs; out; out = out->next) {
      if (out->type == Outputs::Type::AGGREGATE &&
          out->output_name.str != NULL && out->output_name.len > 0) {
        std::string name(out->output_name.str, out->output_name.len);
        alias_map[name] = out->aggregate.agg_index;
      }
    }
    // Walk HAVING expression and resolve identifiers
    std::function<void(ConditionalExpression*)> resolve_having_aliases =
        [&](ConditionalExpression* ce) {
      if (ce == NULL) return;
      if (ce->op == T_IDENTIFIER) {
        Uint32 col_idx = ce->col_idx;
        if (col_idx < m_columns.size()) {
          std::string name(m_columns[col_idx].str, m_columns[col_idx].len);
          auto it = alias_map.find(name);
          if (it != alias_map.end()) {
            ce->having_agg.agg_index = it->second;
            // Keep op as T_IDENTIFIER — evaluate_having_value handles it
          }
        }
      }
      if (ce->op == T_IS) {
        resolve_having_aliases(ce->is.arg);
      } else if (ce->op == T_AND || ce->op == T_OR ||
                 ce->op == T_EQUALS || ce->op == T_NOT_EQUALS ||
                 ce->op == T_GT || ce->op == T_GE ||
                 ce->op == T_LT || ce->op == T_LE ||
                 ce->op == T_PLUS || ce->op == T_MINUS ||
                 ce->op == T_MULTIPLY || ce->op == T_SLASH) {
        resolve_having_aliases(ce->args.left);
        resolve_having_aliases(ce->args.right);
      } else if (ce->op == T_NOT || ce->op == T_EXCLAMATION) {
        resolve_having_aliases(ce->args.left);
      }
    };
    resolve_having_aliases(m_context.ast_root.having_expression);
  }

  // Phase I.5 v2b: reject nullable column operands of GREATEST /
  // LEAST cleanly.  Runs after column resolution but before the SVM
  // compile pass, so any rejection happens before kernel emission.
  validate_greatest_least_pair_loads();
  validate_implicit_scalar_pair_ops();

  // Compile aggregation program if applicable
  if (m_main_scope.agg != NULL) {
    if (m_main_scope.agg->compile()) {
      ndbrequire(m_main_scope.agg->getStatus() == AggregationAPICompiler::Status::COMPILED);
    } else {
      ndbrequire(m_main_scope.agg->getStatus() == AggregationAPICompiler::Status::FAILED);
      throw RonSQLPermanentError("Failed to compile aggregation program.");
    }
  }

  // Compile each CTE body's aggregator. These were captured during parsing
  // (Context::leave_subquery returned the inner AggregationAPICompiler),
  // stashed on SelectStatement::agg, and copied into QueryScope::agg by
  // build_cte_scopes. They start in PROGRAMMING state and must be compiled
  // before NdbAggregator::Finalize will accept them.
  for (Uint32 c = 0; c < m_cte_scopes.size(); c++) {
    AggregationAPICompiler* cte_agg = m_cte_scopes[c]->agg;
    if (cte_agg == NULL) continue;
    if (cte_agg->compile()) {
      ndbrequire(cte_agg->getStatus() == AggregationAPICompiler::Status::COMPILED);
    } else {
      ndbrequire(cte_agg->getStatus() == AggregationAPICompiler::Status::FAILED);
      throw RonSQLPermanentError(
          "Failed to compile CTE aggregation program.");
    }
  }

  // Cross-table WHERE filters require aggregation (BranchReg is in the
  // aggregation program).  Non-aggregate queries with cross-table WHERE
  // are not yet supported.
  {
    // Check for remaining (non-consumed) cross-table WHERE filters.
    // Consumed filters (ce == NULL) were converted to index bounds.
    bool has_remaining = false;
    for (Uint32 f = 0; f < m_main_scope.cross_table_where_filters.size(); f++) {
      if (m_main_scope.cross_table_where_filters[f].ce != NULL) {
        has_remaining = true;
        break;
      }
    }
    if (has_remaining) {
      if (m_main_scope.agg == NULL) {
        throw RonSQLPermanentError(
            "Cross-table WHERE conditions (e.g., a.x > b.y) are only "
            "supported in queries with aggregation (GROUP BY / aggregate "
            "functions).  Split into separate conditions per table, or "
            "add aggregation.");
      }
      // Pre-compute sentinel slot for ResultPrinter (set before construction).
      // The actual sentinel instructions are emitted later in execute_join().
      if (m_main_scope.agg != NULL && !m_has_select_subqueries) {
        Uint32 sentinel_slot = 0;
        DynamicArray<AggregationAPICompiler::Instr>& prog = m_main_scope.agg->m_program;
        for (Uint32 pi = 0; pi < prog.size(); pi++) {
          auto t = prog[pi].type;
          if (t == AggregationAPICompiler::SVMInstrType::Sum ||
              t == AggregationAPICompiler::SVMInstrType::Count ||
              t == AggregationAPICompiler::SVMInstrType::Min ||
              t == AggregationAPICompiler::SVMInstrType::Max ||
              t == AggregationAPICompiler::SVMInstrType::AggRepeat) {
            if (prog[pi].dest >= sentinel_slot)
              sentinel_slot = prog[pi].dest + 1;
          }
        }
        m_context.ast_root.sentinel_agg_slot = (Int32)sentinel_slot;
      }
    }
  }

  // Unify ORDER BY spellings with GROUP BY spellings (bare vs qualified
  // references to the same column carry different col_idx values) so
  // ResultPrinter's col_idx-equality GROUP BY membership check accepts
  // e.g. GROUP BY cu.c_nationkey ORDER BY c_nationkey.
  canonicalize_orderby_columns();

  // Compile post-processing/printer program. CTE queries use the main
  // aggregator on a physical-table leaf (CTE-at-leaf is rejected in
  // execute_join), so ResultPrinter's normal construction applies.
  // Phase E.3: projection-only CTE_SCAN-root queries skip the
  // GROUP-BY-validating compile() path and use the pass-through
  // formatter helpers instead — Phase 0b gives them the same column
  // metadata so temporal / DECIMAL outputs format correctly.
  if (m_is_aggregate_query) {
    m_resultprinter = new (m_amalloc->alloc_exc<ResultPrinter>(1))
      ResultPrinter(m_amalloc,
                    &m_context.ast_root,
                    &m_columns,
                    build_result_column_metadata(),
                    m_conf.output_format,
                    m_conf.err_stream);
  } else {
    m_resultprinter = new (m_amalloc->alloc_exc<ResultPrinter>(1))
      ResultPrinter(m_amalloc,
                    &m_context.ast_root,
                    &m_columns,
                    build_result_column_metadata(),
                    m_conf.output_format,
                    m_conf.err_stream,
                    /*passthrough_marker=*/true);
  }
}

ResultPrinter::ColumnMetadata*
RonSQLPreparer::build_result_column_metadata()
{
  ResultPrinter::ColumnMetadata* column_metadata =
      m_amalloc->alloc_exc<ResultPrinter::ColumnMetadata>(m_columns.size());
  for (Uint32 col_idx = 0; col_idx < m_columns.size(); col_idx++) {
    column_metadata[col_idx].charset = NULL;
    column_metadata[col_idx].precision = 0;
    column_metadata[col_idx].scale = 0;
    column_metadata[col_idx].has_metadata = false;
    column_metadata[col_idx].temporal =
        ResultPrinter::TemporalDisplay::NONE;
    column_metadata[col_idx].temporal_fsp = 0;
    if (m_main_scope.resolved_columns == NULL) continue;
    const QueryScope::ResolvedColumnRef& ref =
        m_main_scope.resolved_columns[col_idx];
    const NdbDictionary::Column* col = ref.dict_column;
    if (col == NULL) continue;
    column_metadata[col_idx].charset = col->getCharset();
    column_metadata[col_idx].precision = col->getPrecision();
    column_metadata[col_idx].scale = col->getScale();
    column_metadata[col_idx].has_metadata = true;
    // D17 + temporal: resolve_cte_output_columns_for_scope plumbs
    // ref.dict_column back to the original source column even through a CTE
    // MIN/MAX, so for MIN(temporal)/MAX(temporal) this is the temporal
    // column itself.  Tag it so the printer decodes the Bigunsigned packed
    // value back to its text form.
    switch (col->getType()) {
    case NdbDictionary::Column::Date:
      column_metadata[col_idx].temporal =
          ResultPrinter::TemporalDisplay::DATE;
      break;
    case NdbDictionary::Column::Year:
      column_metadata[col_idx].temporal =
          ResultPrinter::TemporalDisplay::YEAR;
      break;
    case NdbDictionary::Column::Datetime2:
      column_metadata[col_idx].temporal =
          ResultPrinter::TemporalDisplay::DATETIME2;
      column_metadata[col_idx].temporal_fsp = col->getPrecision();
      break;
    case NdbDictionary::Column::Time2:
      column_metadata[col_idx].temporal =
          ResultPrinter::TemporalDisplay::TIME2;
      column_metadata[col_idx].temporal_fsp = col->getPrecision();
      break;
    case NdbDictionary::Column::Timestamp2:
      column_metadata[col_idx].temporal =
          ResultPrinter::TemporalDisplay::TIMESTAMP2;
      column_metadata[col_idx].temporal_fsp = col->getPrecision();
      break;
    default:
      break;
    }
  }
  return column_metadata;
}

void
RonSQLPreparer::determine_explain()
{
  // Read whether the parsed query is EXPLAIN SELECT
  bool do_explain = m_context.ast_root.do_explain;
  switch (m_conf.explain_mode)
  {
  case RonSQLExecParams::ExplainMode::ALLOW:
    break;
  case RonSQLExecParams::ExplainMode::FORBID:
    require_prm(!do_explain, "Tried to EXPLAIN with explain mode set to FORBID.");
    break;
  case RonSQLExecParams::ExplainMode::REQUIRE:
    require_prm(do_explain, "Tried to query with explain mode set to REQUIRE.");
    break;
  case RonSQLExecParams::ExplainMode::REMOVE:
    // Execute as if EXPLAIN was not specified in the query, even if it was.
    do_explain = false;
    break;
  case RonSQLExecParams::ExplainMode::FORCE:
    // Execute as if EXPLAIN was specified in the query, even if it wasn't.
    do_explain = true;
    break;
  default:
    abort();
  }
  // Write to m_do_explain which will be picked up by RonSQLPreparer::execute()
  m_do_explain = do_explain;
  if (m_conf.do_explain != NULL) {
    // Write to the caller-provided pointer. This is used by RDRS to determine
    // content type.
    *m_conf.do_explain = do_explain;
  }
}

void
RonSQLPreparer::execute_subqueries()
{
  for (Uint32 i = 0; i < m_subquery_infos.size(); i++)
  {
    SubqueryInfo& info = m_subquery_infos[i];
    SelectStatement* inner = info.inner_stmt;
    ndbrequire(inner->sql_begin != NULL && inner->sql_end != NULL);
    ndbrequire(inner->sql_end > inner->sql_begin);

    // Extract inner SQL from the original buffer
    size_t inner_len = inner->sql_end - inner->sql_begin;

    // Build a standalone SQL buffer with semicolon and two NUL terminators
    // (flex requirement)
    size_t buf_len = inner_len + 1 /* ; */ + 2 /* NUL NUL */;
    char* buf = new char[buf_len];
    memcpy(buf, inner->sql_begin, inner_len);
    buf[inner_len] = ';';
    buf[inner_len + 1] = '\0';
    buf[inner_len + 2] = '\0';

    std::ostringstream oss;
    std::ostringstream ess;

    try
    {
      // Create arena allocator for inner preparer
      ArenaMalloc inner_amalloc(RonSQLExecParams::ARENA_MALLOC_PAGE_SIZE);

      // Set up inner execution parameters
      RonSQLExecParams inner_params;
      inner_params.sql_buffer = buf;
      inner_params.sql_len = buf_len;
      inner_params.amalloc = &inner_amalloc;
      inner_params.ndb = m_conf.ndb;
      inner_params.output_format = RonSQLExecParams::OutputFormat::TEXT_NOHEADER;
      inner_params.out_stream = &oss;
      inner_params.err_stream = &ess;
      inner_params.explain_mode = RonSQLExecParams::ExplainMode::FORBID;

      // Create and execute inner preparer
      {
        RonSQLPreparer inner_preparer(inner_params);
        inner_preparer.execute();
      }

      // Parse the TEXT_NOHEADER output
      std::string result_str = oss.str();
      // Trim trailing newline
      while (!result_str.empty() &&
             (result_str.back() == '\n' || result_str.back() == '\r'))
        result_str.pop_back();

      if (info.is_corr_scalar)
      {
        // Correlated scalar: parse multi-line output with two tab-separated columns
        std::istringstream iss(result_str);
        std::string line;
        while (std::getline(iss, line))
        {
          while (!line.empty() && line.back() == '\r') line.pop_back();
          if (line.empty()) continue;
          size_t tab_pos = line.find('\t');
          require_prm(tab_pos != std::string::npos,
                      "Correlated scalar subquery did not return two columns.");
          std::string key_str = line.substr(0, tab_pos);
          std::string val_str = line.substr(tab_pos + 1);
          CorrelatedPair pair;
          parse_subquery_value(key_str, pair.key);
          parse_subquery_value(val_str, pair.val);
          info.corr_values->push(pair);
        }
        if (info.corr_values->size() > 1000)
          throw RonSQLPermanentError(
              "Correlated scalar subquery returned more than 1000 rows.");
      }
      else if (info.is_in_subquery)
      {
        // IN-subquery: parse multi-line output, one value per line
        std::istringstream iss(result_str);
        std::string line;
        while (std::getline(iss, line))
        {
          while (!line.empty() && line.back() == '\r') line.pop_back();
          if (line.empty()) continue;
          SubqueryResult val;
          if (line == "NULL")
          {
            val.is_null = true;
          }
          else
          {
            char* endptr = NULL;
            errno = 0;
            long long ll = strtoll(line.c_str(), &endptr, 10);
            if (endptr != NULL && *endptr == '\0' && errno == 0)
            {
              val.is_null = false;
              val.is_float = false;
              val.int_val = (Int64)ll;
            }
            else
            {
              errno = 0;
              double dbl = strtod(line.c_str(), &endptr);
              if (endptr != NULL && *endptr == '\0' && errno == 0)
              {
                val.is_null = false;
                val.is_float = true;
                val.float_val = dbl;
              }
              else
              {
                throw RonSQLPermanentError(
                    "IN-subquery returned a non-numeric result.");
              }
            }
          }
          info.in_values->push(val);
        }
        if (info.in_values->size() > 1000)
        {
          throw RonSQLPermanentError(
              "IN-subquery returned more than 1000 values.");
        }
      }
      else if (result_str == "NULL" || result_str.empty())
      {
        info.result.is_null = true;
      }
      else
      {
        // Scalar subquery: single-value parsing
        char* endptr = NULL;
        errno = 0;
        long long ll = strtoll(result_str.c_str(), &endptr, 10);
        if (endptr != NULL && *endptr == '\0' && errno == 0)
        {
          info.result.is_null = false;
          info.result.is_float = false;
          info.result.int_val = (Int64)ll;
        }
        else
        {
          // Try float
          errno = 0;
          double dbl = strtod(result_str.c_str(), &endptr);
          if (endptr != NULL && *endptr == '\0' && errno == 0)
          {
            info.result.is_null = false;
            info.result.is_float = true;
            info.result.float_val = dbl;
          }
          else
          {
            throw RonSQLPermanentError(
                "Subquery returned a non-numeric result.");
          }
        }
      }
    }
    catch (const RonSQLPermanentError&)
    {
      delete[] buf;
      throw;
    }
    catch (const RonSQLRetryableError&)
    {
      delete[] buf;
      throw;
    }
    catch (const std::bad_alloc&)
    {
      // Out of memory: free this scope's buffer and let the OOM propagate
      // so the whole query fails cleanly as out-of-memory (handled by
      // handle_ronsql_exception) rather than being relabelled as a generic
      // subquery execution failure below.
      delete[] buf;
      throw;
    }
    catch (const std::exception& e)
    {
      delete[] buf;
      std::string msg = "Subquery execution failed: ";
      msg += e.what();
      throw RonSQLPermanentError(msg);
    }
    catch (...)
    {
      delete[] buf;
      throw;
    }
    delete[] buf;
  }
}

void
RonSQLPreparer::substitute_subquery_results()
{
  substitute_subquery_results_ce(&m_context.ast_root.where_expression);
  substitute_subquery_results_ce(&m_context.ast_root.having_expression);
}

void
RonSQLPreparer::substitute_subquery_results_ce(ConditionalExpression** ce_ptr)
{
  ConditionalExpression* ce = *ce_ptr;
  if (ce == NULL) return;

  if (ce->op == I_CORR_SCALAR)
  {
    for (Uint32 i = 0; i < m_subquery_infos.size(); i++)
    {
      if (m_subquery_infos[i].ce_node != ce) continue;
      SubqueryInfo& info = m_subquery_infos[i];
      TokenKind cmp_op = ce->corr_scalar.cmp_op;
      ConditionalExpression* cmp_expr = ce->corr_scalar.cmp_expr;
      ConditionalExpression* key_expr = ce->corr_scalar.key_expr;

      if (info.corr_values->size() == 0)
      {
        // Empty: always false
        ce->op = T_NOT_EQUALS;
        ce->args.left = cmp_expr;
        ce->args.right = cmp_expr;
        return;
      }

      // Build OR-chain: (key=k1 AND col OP v1) OR (key=k2 AND col OP v2) ...
      ConditionalExpression* result = NULL;
      for (Uint32 j = 0; j < info.corr_values->size(); j++)
      {
        CorrelatedPair& pair = (*info.corr_values)[j];
        if (pair.key.is_null || pair.val.is_null) continue;

        // Build: key_expr = key_const
        ConditionalExpression* key_const =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        if (pair.key.is_float)
        {
          key_const->op = T_FLOAT;
          key_const->constant_float.dbl = pair.key.float_val;
          key_const->constant_float.ls = LexString{NULL, 0};
        }
        else
        {
          key_const->op = T_INT;
          key_const->constant_integer = pair.key.int_val;
        }

        ConditionalExpression* key_eq =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        key_eq->op = T_EQUALS;
        key_eq->args.left = key_expr;
        key_eq->args.right = key_const;

        // Build: cmp_expr <cmp_op> val_const
        ConditionalExpression* val_const =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        if (pair.val.is_float)
        {
          val_const->op = T_FLOAT;
          val_const->constant_float.dbl = pair.val.float_val;
          val_const->constant_float.ls = LexString{NULL, 0};
        }
        else
        {
          val_const->op = T_INT;
          val_const->constant_integer = pair.val.int_val;
        }

        ConditionalExpression* cmp_node =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        cmp_node->op = cmp_op;
        cmp_node->args.left = cmp_expr;
        cmp_node->args.right = val_const;

        // Build: key_eq AND cmp_node
        ConditionalExpression* and_node =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        and_node->op = T_AND;
        and_node->args.left = key_eq;
        and_node->args.right = cmp_node;

        // Accumulate into OR-chain
        if (result == NULL)
        {
          result = and_node;
        }
        else
        {
          ConditionalExpression* or_node =
            m_amalloc->alloc_exc<ConditionalExpression>(1);
          or_node->op = T_OR;
          or_node->args.left = result;
          or_node->args.right = and_node;
          result = or_node;
        }
      }

      if (result == NULL)
      {
        // All null: always false
        ce->op = T_NOT_EQUALS;
        ce->args.left = cmp_expr;
        ce->args.right = cmp_expr;
      }
      else
      {
        *ce = *result;
      }
      return;
    }
    ndbrequire(false);
    return;
  }

  if (ce->op == I_IN_SUBQUERY)
  {
    for (Uint32 i = 0; i < m_subquery_infos.size(); i++)
    {
      if (m_subquery_infos[i].ce_node == ce)
      {
        DynamicArray<SubqueryResult>* values = m_subquery_infos[i].in_values;
        ConditionalExpression* in_expr = m_subquery_infos[i].in_expr;

        if (values->size() == 0)
        {
          // Empty result set: always false via col != col
          ce->op = T_NOT_EQUALS;
          ce->args.left = in_expr;
          ce->args.right = in_expr;
          return;
        }

        // Build OR-chain: expr = v1 OR expr = v2 OR ...
        ConditionalExpression* result = NULL;
        for (Uint32 j = 0; j < values->size(); j++)
        {
          SubqueryResult& val = (*values)[j];
          if (val.is_null) continue;  // NULL = x is never true

          ConditionalExpression* const_node =
            m_amalloc->alloc_exc<ConditionalExpression>(1);
          if (val.is_float)
          {
            const_node->op = T_FLOAT;
            const_node->constant_float.dbl = val.float_val;
            const_node->constant_float.ls = LexString{NULL, 0};
          }
          else
          {
            const_node->op = T_INT;
            const_node->constant_integer = val.int_val;
          }

          ConditionalExpression* eq =
            m_amalloc->alloc_exc<ConditionalExpression>(1);
          eq->op = T_EQUALS;
          eq->args.left = in_expr;
          eq->args.right = const_node;

          if (result == NULL)
          {
            result = eq;
          }
          else
          {
            ConditionalExpression* or_node =
              m_amalloc->alloc_exc<ConditionalExpression>(1);
            or_node->op = T_OR;
            or_node->args.left = result;
            or_node->args.right = eq;
            result = or_node;
          }
        }

        if (result == NULL)
        {
          // All values were NULL: always false via col != col
          ce->op = T_NOT_EQUALS;
          ce->args.left = in_expr;
          ce->args.right = in_expr;
        }
        else
        {
          *ce = *result;
        }
        return;
      }
    }
    ndbrequire(false);
    return;
  }

  if (ce->op == I_SUBQUERY)
  {
    // Find matching SubqueryInfo
    for (Uint32 i = 0; i < m_subquery_infos.size(); i++)
    {
      if (m_subquery_infos[i].ce_node == ce)
      {
        SubqueryResult& result = m_subquery_infos[i].result;
        if (result.is_null)
        {
          ce->op = T_NULL;
        }
        else if (result.is_float)
        {
          ce->op = T_FLOAT;
          ce->constant_float.dbl = result.float_val;
          ce->constant_float.ls = LexString{NULL, 0};
        }
        else
        {
          ce->op = T_INT;
          ce->constant_integer = result.int_val;
        }
        return;
      }
    }
    // Should not happen — every I_SUBQUERY node should have a SubqueryInfo
    ndbrequire(false);
    return;
  }

  switch (ce->op)
  {
  case T_IS:
    substitute_subquery_results_ce(&ce->is.arg);
    return;
  case T_INTERVAL:
    substitute_subquery_results_ce(&ce->interval.arg);
    return;
  case T_EXTRACT:
    substitute_subquery_results_ce(&ce->extract.arg);
    return;
  case T_IDENTIFIER:
  case T_INT:
  case T_FLOAT:
  case T_STRING:
  case I_MYSQL_TIME:
  case T_SUM:
  case T_MIN:
  case T_MAX:
  case T_COUNT:
  case T_AVG:
  case T_NULL:
    return;
  default:
    // Binary/unary operators: recurse into both sides
    substitute_subquery_results_ce(&ce->args.left);
    substitute_subquery_results_ce(&ce->args.right);
    return;
  }
}

void
RonSQLPreparer::execute()
{
  DEB_TRACE();
  require_prm(m_status != Status::FAILED,
              "Attempting RonSQLPreparer::execute while in failed state.");
  DEB_TRACE();
  ndbrequire(m_status == Status::PREPARED);
  DEB_TRACE();
  Ndb* ndb = m_conf.ndb;
  DEB_TRACE();
  try {
    if (m_do_explain) {
      DEB_TRACE();
      switch (m_conf.output_format) {
      case RonSQLExecParams::OutputFormat::TEXT:
        [[fallthrough]];
      case RonSQLExecParams::OutputFormat::TEXT_NOHEADER:
        print();
        break;
      case RonSQLExecParams::OutputFormat::JSON:
        feature_not_implemented("JSON format for EXPLAIN output");
        break;
      case RonSQLExecParams::OutputFormat::JSON_ASCII:
        feature_not_implemented("JSON_ASCII format for EXPLAIN output");
        break;
      default:
        DEB_TRACE();
        abort();
      }

      DEB_TRACE();
      return;
    }
    DEB_TRACE();
    require_prm(ndb != NULL, "Cannot query without ndb object.");

    if (m_has_subqueries) {
      STAT_TS(m_conf.phase_stats, s_subq_start);
      execute_subqueries();
      substitute_subquery_results();
      STAT_TS(m_conf.phase_stats, s_subq_end);
      STAT_SET(m_conf.phase_stats, subquery_us, s_subq_start, s_subq_end);
    }

    if (is_join_query()) {
      execute_join();
      cleanup_trans();
      return;
    }

    if (!m_is_aggregate_query) {
      // Phase 1 (non_aggregate_phase_1.md): projection-only
      // single-table queries — plain-API pass-through execution.
      execute_single_table_passthrough();
      cleanup_trans();
      return;
    }

    ndbrequire(m_trans == NULL);
    m_trans = DBG(ndb->startTransaction());
    require_run(m_trans != NULL, "Failed to start transaction.");
    // Since ndb exists, m_main_scope.table should have been initialized in load()
    ndbrequire(m_main_scope.table != NULL);
    NdbAggregator aggregator(m_main_scope.table);
    DBGV(programAggregator(&aggregator));
    require_prm(aggregator.Finalize(), "Failed to finalize aggregator.");
    STAT_TS(m_conf.phase_stats, s_scandef_start);
    NdbScanOperation* scanOp = open_single_table_scan_op();
    DEB_TRACE();
    require_run(DBG(scanOp->setAggregationCode(&aggregator)) >= 0,
                "Failed to set aggregation code.");
    STAT_TS(m_conf.phase_stats, s_agg_start);
    STAT_SET(m_conf.phase_stats, ndbprep_us, s_scandef_start, s_agg_start);
    require_run(DBG(scanOp->DoAggregation()) >= 0,
                "Failed to execute scan aggregation.");
    STAT_TS(m_conf.phase_stats, s_agg_end);
    STAT_SET(m_conf.phase_stats, firstbatch_us, s_agg_start, s_agg_end);
    DEB_TRACE();

    // Print results
    STAT_TS(m_conf.phase_stats, s_print_start);
    m_resultprinter->print_result(&aggregator, m_conf.out_stream);
    STAT_TS(m_conf.phase_stats, s_print_end);
    STAT_SET(m_conf.phase_stats, print_us, s_print_start, s_print_end);
    DEB_TRACE();

    cleanup_trans();
  }
  catch (...) {
    handle_ronsql_exception(std::current_exception());
  }
}

NdbScanOperation*
RonSQLPreparer::open_single_table_scan_op()
{
  ScanConfig& sc = *m_scan_config;
  const NdbDictionary::Index* index = sc.index;
  bool has_filter = false;
  for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
    if (sc.condition_handling_map[i] == -1) {
      has_filter = true;
    }
  }

  if (index == NULL) {
    // Prepare full table scan
    DEB_TRACE();
    NdbScanOperation* myScanOp = DBG(m_trans->getNdbScanOperation(DBG(m_main_scope.table)));
    require_sch(myScanOp != NULL, "Failed to get scan operation.");
    require_prm(DBG(myScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead)) == 0,
                "Failed to initialize scan operation.");
    DEB_TRACE();
    if (has_filter)
    {
      DEB_TRACE();
      NdbScanFilter filter(myScanOp);
      DBGV(filter.setSqlCmpSemantics());
      DEB_TRACE();
      apply_filter_top_level(&filter);
      DEB_TRACE();
    }
    DEB_TRACE();
    return myScanOp;
  }

  DEB_TRACE();
  // Prepare index scan
  require_sch(DBG(index->getTableId()) == DBG(m_main_scope.table->getObjectId()) &&
              DBG(index->getTableVersion()) ==
              DBG(m_main_scope.table->getObjectVersion()),
              "Table id/version mismatch");
  NdbIndexScanOperation *myIndexScanOp =
    DBG(m_trans->getNdbIndexScanOperation(DBG(index)));
  require_run(myIndexScanOp != NULL,
              "Failed getting index scan operation.");
  Uint32 scanFlags = 0;
  if (sc.index_order) {
    // Phase 4b (ronsql_orderby_limit_plan.md): the index delivers the
    // pass-through ORDER BY order.  SF_OrderBy makes the NDB API
    // merge-sort the per-fragment ordered scans into one globally
    // ordered stream (the RecAttr API auto-adds the index key columns
    // to the read set and forces full fragment parallelism);
    // SF_Descending reverses the index order incl. NULL placement
    // (NULLs first ASC / last DESC, matching MySQL).  Under
    // ORDER BY indexed_col LIMIT n this is MySQL's top-N-via-index
    // plan: the drain stops at the limit and closes the scan early,
    // having fetched roughly one batch per fragment.  Aggregate
    // single-table scans never set index_order — the aggregator
    // consumes every row, so the serialised merge would be pure cost.
    scanFlags |= NdbScanOperation::SF_OrderBy;
    if (sc.index_order_desc) scanFlags |= NdbScanOperation::SF_Descending;
  }
  require_run(DBG(myIndexScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead,
                                            DBG(scanFlags))) == 0,
              "Failed to initialize index scan operation.");
  bool any_bound = false;
  // Per-index-column bound sides, for the nullable high-only fix below.
  static const Uint32 MAX_BOUND_COLS = 32;
  bool col_has_low[MAX_BOUND_COLS];
  bool col_has_high[MAX_BOUND_COLS];
  for (Uint32 c = 0; c < MAX_BOUND_COLS; c++) {
    col_has_low[c] = false;
    col_has_high[c] = false;
  }
  int max_bound_col = -1;
  for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++)
  {
    int index_col_idx = DBG(sc.condition_handling_map[i]);
    if (index_col_idx == -1) {
      // This condition could not be configured for the index scan. It will
      // be applied as part of the filter instead.
      continue;
    }
    any_bound = true;
    ConditionalExpression* ce = m_toplevel_conditions[i];
    Uint32 condition_col_idx = DBG(ce->args.left->col_idx);
    ConditionalExpression* condition_constant = ce->args.right;
    TokenKind op = DBG(ce->op);
    NdbIndexScanOperation::BoundType bt;
    switch (op) {
    case T_EQUALS: bt = NdbIndexScanOperation::BoundType::BoundEQ; break;
    /* This mapping might seem surprising.
     * - In RonSQL, we have normalized the conditional expressions to have
     *   the column name on the left and the constant on the right. Thus,
     *   T_GE means column value >= constant, or in other words, the
     *   constant is a lower bound.
     * - In ndbapi, BoundLE is documented to mean non-strict "lower bound".
     */
    case T_GE:     bt = NdbIndexScanOperation::BoundType::BoundLE; break;
    case T_GT:     bt = NdbIndexScanOperation::BoundType::BoundLT; break;
    case T_LE:     bt = NdbIndexScanOperation::BoundType::BoundGE; break;
    case T_LT:     bt = NdbIndexScanOperation::BoundType::BoundGT; break;
    default: abort();
    }
    const char* colName = m_columns[condition_col_idx].c_str();
    require_run(m_main_scope.resolved_columns != NULL,
                "Index scan bound: missing resolved columns.");
    const QueryScope::ResolvedColumnRef& condition_ref =
        m_main_scope.resolved_columns[condition_col_idx];
    require_prm(
        condition_ref.kind ==
        QueryScope::ResolvedColumnRef::Kind::StoredColumn,
        "Index scan bound requires a stored-table column.");
    require_prm(condition_ref.dict_column != NULL,
                "Index scan bound column descriptor missing.");
    raw_value rv = encode_constant(condition_constant,
                                   condition_ref.dict_column);
    require_run(DBG(myIndexScanOp->setBound(DBG(colName),
                                            DBG(bt),
                                            DBG(rv).val)) == 0,
                "Failed to set bound for index scan.");
    if (index_col_idx >= 0 && index_col_idx < (int)MAX_BOUND_COLS) {
      if (op == T_EQUALS || op == T_GE || op == T_GT)
        col_has_low[index_col_idx] = true;
      if (op == T_EQUALS || op == T_LE || op == T_LT)
        col_has_high[index_col_idx] = true;
      if (index_col_idx > max_bound_col) max_bound_col = index_col_idx;
    }
    DEB_TRACE();
  }
  /*
   * NULL-excluding low bound (findings/nullable_bounds.md): NULL sorts
   * below every value in an NDB ordered index, so a HIGH-only bound on
   * a NULLABLE column starts the scan at the index head and would
   * return NULL rows that SQL comparison semantics exclude (col <= X
   * is UNKNOWN for NULL).  Emit the mysqld range-optimizer idiom for
   * such columns: setBound(col, BoundLT, NULL) — a strict low bound
   * whose value is NULL, i.e. "col > NULL" — which keeps the conjunct
   * a bound (plan unchanged) while excluding the NULL entries.  A
   * high-only column is always the LAST bounded column (a range blocks
   * later columns), so the strict low bound lands on the last low key
   * part, as the old setBound API requires; emitting after the loop
   * keeps earlier equality lows ahead of it.
   */
  for (int c = 0; c <= max_bound_col; c++) {
    if (!col_has_high[c] || col_has_low[c]) continue;
    const NdbDictionary::Column* idx_col = index->getColumn(c);
    require_run(idx_col != NULL, "Index column missing for bound.");
    const NdbDictionary::Column* tab_col =
        m_main_scope.table->getColumn(idx_col->getName());
    if (tab_col == NULL || tab_col->getNullable()) {
      require_run(DBG(myIndexScanOp->setBound(
                      DBG(idx_col->getName()),
                      NdbIndexScanOperation::BoundType::BoundLT,
                      /*aValue=*/NULL)) == 0,
                  "Failed to set NULL-excluding low bound.");
    }
  }
  DEB_TRACE();
  // Close the (single) range only when a bound was added: end_of_bound
  // fails with 4259 "Invalid set of range scan bounds" when no setBound
  // preceded it, and a Phase 4b ORDER BY-driven candidate may carry no
  // bounds at all (full index scan in key order, conjuncts as filters).
  // todo Is this necessary after removing the multirange flag?
  if (any_bound) {
    require_run(DBG(myIndexScanOp->end_of_bound(0)) == 0,
                "Failed to set end of bound.");
  }
  if (has_filter)
  {
    DEB_TRACE();
    NdbScanFilter filter(myIndexScanOp);
    DBGV(filter.setSqlCmpSemantics());
    apply_filter_top_level(&filter);
  }
  DEB_TRACE();
  return myIndexScanOp;
}

void
RonSQLPreparer::register_passthrough_getvalues(NdbOperation* op,
                                               const NdbRecAttr** attrs,
                                               Uint32 num_cols)
{
  require_run(m_main_scope.resolved_columns != NULL,
              "Single-table pass-through: missing resolved columns.");
  Uint32 i = 0;
  for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
       o = o->next, i++) {
    ndbrequire(o->type == Outputs::Type::COLUMN);
    const QueryScope::ResolvedColumnRef& col_ref =
        m_main_scope.resolved_columns[o->column.col_idx];
    require_prm(col_ref.kind ==
                    QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                "Single-table pass-through: output is not a stored column.");
    require_run(col_ref.dict_column != NULL,
                "Single-table pass-through: column descriptor missing.");
    attrs[i] = op->getValue(col_ref.dict_column);
    require_run(attrs[i] != NULL,
                "Single-table pass-through: getValue() failed.");
  }
  ndbrequire(i == num_cols);
}

void
RonSQLPreparer::build_passthrough_getvalue_specs(
    NdbOperation::GetValueSpec* gets, Uint32 num_cols)
{
  /*
   * The NdbRecord-lookup twin of register_passthrough_getvalues: the
   * identical outputs walk and checks, filling OO_GETVALUE specs
   * instead of calling getValue() (an NdbRecord operation cannot take
   * RecAttr reads directly).  The walk preserves the outputs-order ==
   * attrs-order contract the printer metadata lookup relies on.  The
   * spec array is arena-allocated (alloc_exc runs no constructors), so
   * every field is set explicitly.
   */
  require_run(m_main_scope.resolved_columns != NULL,
              "Single-table pass-through: missing resolved columns.");
  Uint32 i = 0;
  for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
       o = o->next, i++) {
    ndbrequire(o->type == Outputs::Type::COLUMN);
    const QueryScope::ResolvedColumnRef& col_ref =
        m_main_scope.resolved_columns[o->column.col_idx];
    require_prm(col_ref.kind ==
                    QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                "Single-table pass-through: output is not a stored column.");
    require_run(col_ref.dict_column != NULL,
                "Single-table pass-through: column descriptor missing.");
    gets[i].column = col_ref.dict_column;
    gets[i].appStorage = NULL;
    gets[i].recAttr = NULL;
    gets[i].m_startPos = 0;
    gets[i].m_size = 0;
  }
  ndbrequire(i == num_cols);
}

/*
 * Phase 3 (ronsql_orderby_limit_plan.md): client-side ORDER BY for
 * projection-only shapes.  Rows arrive via per-row NdbRecAttr buffers
 * that are overwritten each fetch, so the drains buffer every row as an
 * array of NdbRecAttr clones (heap; ~PassthroughSortBuffer deletes them
 * on every exit path incl. exceptions), sort with NdbSqlUtil
 * comparators, then print through the unchanged NdbRecAttr-based
 * pass-through printer.  A NULL entry in a stored row denotes SQL NULL
 * from an op-level LEFT JOIN NULL-extended row.  Stored-row layout:
 * slots 0..num_cols-1 are the SELECT outputs (printed); ORDER BY-only
 * columns occupy extra slots after them (sorted on, never printed).
 */
namespace {

struct PassthroughSortKey {
  Uint32 slot;                        // index into the stored row array
  bool ascending;
  const NdbDictionary::Column* col;   // type + charset for the comparator
};

struct PassthroughSortBuffer {
  DynamicArray<NdbRecAttr**> rows;
  Uint32 num_stored;
  Uint64 bytes = 0;
  PassthroughSortBuffer(ArenaMalloc* am, Uint32 n)
    : rows(am), num_stored(n) {}
  ~PassthroughSortBuffer() {
    for (Uint32 r = 0; r < rows.size(); r++) {
      NdbRecAttr** row = rows[r];
      for (Uint32 c = 0; c < num_stored; c++) delete row[c];
    }
  }
};

/* Generous fixed caps: the buffered result is unaggregated and
 * unbounded, and LIMIT cannot reduce the buffering (top-N still has to
 * see every row).  Exceeding either cap is a clean permanent error. */
static const Uint32 PASSTHROUGH_SORT_MAX_ROWS = 1000000;
static const Uint64 PASSTHROUGH_SORT_MAX_BYTES = 256 * 1024 * 1024;

/* NULLs sort first in ASC (MySQL semantics) — same rules as the
 * aggregate path's ResultPrinter::compare_rows GROUP BY-column arm. */
static int
compare_passthrough_rows(NdbRecAttr* const* a, NdbRecAttr* const* b,
                         const PassthroughSortKey* keys, Uint32 nkeys)
{
  for (Uint32 i = 0; i < nkeys; i++) {
    const PassthroughSortKey& k = keys[i];
    const NdbRecAttr* va = a[k.slot];
    const NdbRecAttr* vb = b[k.slot];
    bool a_null = (va == NULL || va->isNULL());
    bool b_null = (vb == NULL || vb->isNULL());
    if (a_null && b_null) continue;
    if (a_null) return k.ascending ? -1 : 1;
    if (b_null) return k.ascending ? 1 : -1;
    const NdbSqlUtil::Type& sqlType =
        NdbSqlUtil::getType(static_cast<Uint32>(k.col->getType()));
    int cmp = (*sqlType.m_cmp)(k.col->getCharset(),
                               va->aRef(), va->get_size_in_bytes(),
                               vb->aRef(), vb->get_size_in_bytes());
    if (cmp != 0) return k.ascending ? cmp : -cmp;
  }
  return 0;
}

/* Clone one delivered row into the buffer.  `ops` routes the op-level
 * LEFT JOIN NULL-row marker (isRowNULL) per slot; NULL for the
 * single-table path.  The row array is pushed before cloning so a
 * mid-row exception still frees the clones made so far. */
static void
buffer_passthrough_row(PassthroughSortBuffer& buf, ArenaMalloc* amalloc,
                       const NdbRecAttr* const* attrs,
                       NdbQueryOperation* const* ops,
                       std::basic_ostream<char>* err_stream)
{
  if (buf.rows.size() >= PASSTHROUGH_SORT_MAX_ROWS ||
      buf.bytes > PASSTHROUGH_SORT_MAX_BYTES) {
    *err_stream
        << "ORDER BY on a projection-only query buffers the full result "
           "for a client-side sort; this result exceeded the sort buffer "
           "cap (" << PASSTHROUGH_SORT_MAX_ROWS << " rows / "
        << (PASSTHROUGH_SORT_MAX_BYTES >> 20) << " MB).  Tighten the "
           "WHERE clause or use an aggregate query.\n";
    throw RonSQLPermanentError("Pass-through ORDER BY result too large.");
  }
  Uint32 n = buf.num_stored;
  NdbRecAttr** row = amalloc->alloc_exc<NdbRecAttr*>(n);
  for (Uint32 c = 0; c < n; c++) row[c] = NULL;
  buf.rows.push(row);
  for (Uint32 c = 0; c < n; c++) {
    const NdbRecAttr* src = attrs[c];
    if (src == NULL || (ops != NULL && ops[c] != NULL && ops[c]->isRowNULL()))
      continue;  // stored as SQL NULL
    row[c] = src->clone();
    require_run(row[c] != NULL,
                "Pass-through sort: failed to clone a row value.");
    buf.bytes += row[c]->get_size_in_bytes();
  }
}

/* Sort the buffered rows and print the first min(limit, n).  The TSV
 * header stays deferred until the first printed row (LIMIT 0 or an
 * empty result prints nothing); JSON's '[' was emitted by the caller
 * before draining and `header_emitted` reflects that. */
static void
sort_and_print_passthrough_rows(PassthroughSortBuffer& buf,
                                const PassthroughSortKey* keys, Uint32 nkeys,
                                Uint32 num_cols, Int64 limit,
                                ArenaMalloc* amalloc, ResultPrinter* printer,
                                bool& header_emitted,
                                std::basic_ostream<char>* out)
{
  Uint32 n = buf.rows.size();
  if (n == 0) return;
  // Copy to a plain array for std::sort (DynamicArray lacks
  // random-access iterators), mirroring print_result_ordered.
  NdbRecAttr*** arr = amalloc->alloc_exc<NdbRecAttr**>(n);
  for (Uint32 i = 0; i < n; i++) arr[i] = buf.rows[i];
  auto less = [keys, nkeys](NdbRecAttr** ra, NdbRecAttr** rb) {
    return compare_passthrough_rows(ra, rb, keys, nkeys) < 0;
  };
  Uint32 print_count = n;
  if (limit >= 0 && (Int64)n > limit) print_count = (Uint32)limit;
  if (print_count < n)
    std::partial_sort(arr, arr + print_count, arr + n, less);
  else
    std::sort(arr, arr + n, less);
  for (Uint32 i = 0; i < print_count; i++) {
    const NdbRecAttr* const* row =
        const_cast<const NdbRecAttr* const*>(arr[i]);
    if (!header_emitted) {
      printer->print_passthrough_header(row, num_cols, out);
      header_emitted = true;
    }
    printer->print_passthrough_row(row, num_cols, /*is_first_row=*/(i == 0),
                                   out);
  }
}

}  // namespace

void
RonSQLPreparer::execute_single_table_passthrough()
{
  /*
   * Phase 1 (non_aggregate_phase_1.md, W3): projection-only
   * single-table execution.  PK-lookup arm when the WHERE was fully
   * consumed as PK equalities (detect_pk_lookup); otherwise the shared
   * scan setup + a streaming NdbRecAttr drain.  Output framing mirrors
   * execute_passthrough_drain: JSON always frames '[' ... ']' (an
   * empty array is the correct empty representation); TSV defers the
   * header until the first row so empty results produce no output,
   * matching the mysql client baseline ronsql_compare.inc diffs
   * against.
   */
  Ndb* ndb = m_conf.ndb;
  ndbrequire(m_trans == NULL);
  m_trans = DBG(ndb->startTransaction());
  require_run(m_trans != NULL, "Failed to start transaction.");
  ndbrequire(m_main_scope.table != NULL);

  Uint32 num_cols = 0;
  for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
       o = o->next) {
    num_cols++;
  }
  ndbrequire(num_cols > 0);
  // Phase 3 (ronsql_orderby_limit_plan.md): ORDER BY on the
  // pass-through path — count entries up front so `attrs` has room for
  // ORDER BY-only extra slots on the scan arm.  The PK-lookup arm
  // returns at most one row, so sorting is a no-op there.
  Uint32 n_orderby = 0;
  for (const OrderbyColumns* ob = m_context.ast_root.orderby_columns;
       ob != NULL; ob = ob->next)
    n_orderby++;
  // Phase 4b: when the planner picked an ordered index that delivers
  // the ORDER BY order (ScanConfig::index_order), the scan runs with
  // SF_OrderBy and rows arrive already globally sorted — stream them
  // through the Phase 2 LIMIT cutoff instead of buffering for the
  // Phase 3 sort.  (m_scan_config is NULL on the PK-lookup arm, which
  // returns at most one row.)
  const bool index_order_streaming =
      (!m_pk_lookup && m_scan_config != NULL && m_scan_config->index_order);
  const bool sorting = (n_orderby > 0) && !index_order_streaming;
  const NdbRecAttr** attrs =
      m_amalloc->alloc_exc<const NdbRecAttr*>(num_cols + n_orderby);

  const bool is_json =
      (m_conf.output_format == RonSQLExecParams::OutputFormat::JSON ||
       m_conf.output_format == RonSQLExecParams::OutputFormat::JSON_ASCII);
  bool header_emitted = false;

  STAT_TS(m_conf.phase_stats, s_scandef_start);

  if (m_pk_lookup) {
    /*
     * Single-row primary key lookup.  Two definition arms share the
     * execute + print tail below: the RecAttr readTuple when the WHERE
     * was fully consumed as keys, and an NdbRecord readTuple carrying
     * residual conjuncts as an OO_INTERPRETED filter program (the
     * RecAttr-style operation has no interpreted-code facility —
     * OO_INTERPRETED is NdbRecord-only).  The interpreted-code object
     * and the arena rows must outlive execute(), hence block scope.
     */
    const NdbOperation* exec_op = NULL;
    NdbInterpretedCode residual_code(m_main_scope.table);
    const int nkeys = m_main_scope.table->getNoOfPrimaryKeys();
    if (!m_pk_lookup_has_residual) {
      NdbOperation* op = DBG(m_trans->getNdbOperation(m_main_scope.table));
      require_sch(op != NULL, "Failed to get lookup operation.");
      require_run(DBG(op->readTuple(NdbOperation::LM_CommittedRead)) == 0,
                  "Failed to initialize lookup operation.");
      for (int k = 0; k < nkeys; k++) {
        const char* pk_name = m_main_scope.table->getPrimaryKey(k);
        const NdbDictionary::Column* pk_col =
            m_main_scope.table->getColumn(pk_name);
        ndbrequire(pk_col != NULL);
        raw_value rv = encode_constant(m_pk_lookup_const[k], pk_col);
        require_run(DBG(op->equal(pk_name,
                                  static_cast<const char*>(rv.val))) == 0,
                    "Failed to set primary key value for lookup.");
      }
      register_passthrough_getvalues(op, attrs, num_cols);
      exec_op = op;
    } else {
      const NdbRecord* rec = m_main_scope.table->getDefaultRecord();
      require_sch(rec != NULL, "Failed to get default record for lookup.");
      const Uint32 rowlen = NdbDictionary::getRecordRowLength(rec);
      require_run(rowlen > 0, "Empty default record for lookup.");
      char* key_row = m_amalloc->alloc_exc<char>(rowlen);
      memset(key_row, 0, rowlen);
      /*
       * The result record/row are pure scaffolding: the all-zero mask
       * requests no NdbRecord result columns, and the projected
       * columns instead arrive as OO_GETVALUE extra reads — plain
       * NdbRecAttr results, so the pass-through printer keeps its
       * RecAttr path unchanged.
       */
      char* result_row = m_amalloc->alloc_exc<char>(rowlen);
      memset(result_row, 0, rowlen);
      const Uint32 mask_len =
          ((Uint32)m_main_scope.table->getNoOfColumns() + 7) / 8;
      unsigned char* zero_mask = m_amalloc->alloc_exc<unsigned char>(mask_len);
      memset(zero_mask, 0, mask_len);
      for (int k = 0; k < nkeys; k++) {
        const char* pk_name = m_main_scope.table->getPrimaryKey(k);
        const NdbDictionary::Column* pk_col =
            m_main_scope.table->getColumn(pk_name);
        ndbrequire(pk_col != NULL);
        // encode_constant produces the column's NdbRecord byte format
        // for every supported PK type (little-endian ints at column
        // width, space-padded CHAR, length-prefixed VARCHAR, packed
        // temporals), so the bytes go into the key row verbatim.
        raw_value rv = encode_constant(m_pk_lookup_const[k], pk_col);
        char* dst = NdbDictionary::getValuePtr(rec, key_row,
                                               pk_col->getAttrId());
        require_run(dst != NULL, "Failed to locate key column in record.");
        memcpy(dst, rv.val, rv.len);
      }
      // Residual filter program, rebuilt per execute like the scan
      // arm's NdbScanFilter; the detection-time trial build proved it
      // fits LOOKUP_FILTER_MAX_WORDS with emit-supported types.
      {
        NdbScanFilter filter(&residual_code);
        DBGV(filter.setSqlCmpSemantics());
        require_run(DBG(filter.begin(NdbScanFilter::AND)) >= 0,
                    "Failed to apply lookup filter.");
        Uint32 num_residual = 0;
        for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
          if (m_pk_lookup_cond_map[i] == -1) {
            apply_filter(&filter, m_main_scope, m_toplevel_conditions[i]);
            num_residual++;
          }
        }
        ndbrequire(num_residual > 0);
        require_run(DBG(filter.end()) >= 0, "Failed to apply lookup filter.");
      }
      require_run(DBG(residual_code.finalise()) == 0,
                  "Failed to finalise lookup filter.");
      NdbOperation::GetValueSpec* gets =
          m_amalloc->alloc_exc<NdbOperation::GetValueSpec>(num_cols);
      build_passthrough_getvalue_specs(gets, num_cols);
      NdbOperation::OperationOptions opts;
      memset(&opts, 0, sizeof(opts));
      opts.optionsPresent = NdbOperation::OperationOptions::OO_INTERPRETED |
                            NdbOperation::OperationOptions::OO_GETVALUE;
      opts.interpretedCode = &residual_code;
      opts.extraGetValues = gets;
      opts.numExtraGetValues = num_cols;
      exec_op = DBG(m_trans->readTuple(rec, key_row, rec, result_row,
                                       NdbOperation::LM_CommittedRead,
                                       zero_mask, &opts, sizeof(opts)));
      require_sch(exec_op != NULL, "Failed to define lookup operation.");
      for (Uint32 i = 0; i < num_cols; i++) {
        // OO_GETVALUE RecAttrs are populated at definition time.
        attrs[i] = gets[i].recAttr;
        require_run(attrs[i] != NULL,
                    "Single-table pass-through: OO_GETVALUE failed.");
      }
    }
    // Phase stats: ndbprep = lookup-op definition; the NDB API fuses
    // send + read for a committed lookup, reported as firstbatch (send
    // and drain stay 0, like the fused single-table aggregate path).
    STAT_TS(m_conf.phase_stats, s_pk_defined);
    STAT_SET(m_conf.phase_stats, ndbprep_us, s_scandef_start, s_pk_defined);
    bool have_row = true;
    if (DBG(m_trans->execute(NdbTransaction::Commit)) != 0 ||
        exec_op->getNdbError().code != 0) {
      // A missing row surfaces as NoDataFound — the one place a failed
      // NDB call is a normal, empty result.  On the residual arm this
      // also covers a fetched-but-rejected row: NdbScanFilter's reject
      // path is interpret_exit_nok() with the default error code 626,
      // whose classification is NoDataFound — indistinguishable from
      // row-absent by design, and both mean an empty result here.
      // (The NdbRecord readTuple defaults to AO_IgnoreError, so
      // execute() may return 0 with only the op error set — hence the
      // dual check above.)  Anything else is a real error, classified
      // by handle_ronsql_exception.
      const NdbError& op_err = exec_op->getNdbError();
      const NdbError& trans_err = m_trans->getNdbError();
      if (op_err.classification == NdbError::NoDataFound ||
          trans_err.classification == NdbError::NoDataFound) {
        have_row = false;
      } else {
        require_run(false, "Failed to execute lookup.");
      }
    }
    // Phase 2 (ronsql_orderby_limit_plan.md): LIMIT 0 prints nothing
    // (any LIMIT >= 1 cannot constrain a single-row lookup further).
    if (m_context.ast_root.limit == 0) have_row = false;
    STAT_TS(m_conf.phase_stats, s_pk_done);
    STAT_SET(m_conf.phase_stats, firstbatch_us, s_pk_defined, s_pk_done);
    STAT_COUNT(m_conf.phase_stats, rows_drained, have_row ? 1 : 0);
    if (is_json) {
      m_resultprinter->print_passthrough_header(attrs, num_cols,
                                                m_conf.out_stream);
      header_emitted = true;
    }
    if (have_row) {
      if (!header_emitted) {
        m_resultprinter->print_passthrough_header(attrs, num_cols,
                                                  m_conf.out_stream);
        header_emitted = true;
      }
      m_resultprinter->print_passthrough_row(attrs, num_cols,
                                             /*is_first_row=*/true,
                                             m_conf.out_stream);
    }
    if (header_emitted) {
      m_resultprinter->print_passthrough_finish(m_conf.out_stream);
    }
    return;
  }

  // Scan arm: shared setup, then a streaming drain.
  NdbScanOperation* scanOp = open_single_table_scan_op();
  register_passthrough_getvalues(scanOp, attrs, num_cols);
  // Phase 3: resolve ORDER BY sort keys BEFORE execute.  A sort column
  // already in the SELECT reuses its output slot; others get an extra
  // getValue in a slot after the outputs (fetched for the comparator,
  // never printed).
  Uint32 num_stored = num_cols;
  PassthroughSortKey* sort_keys = NULL;
  Uint32 n_sort_keys = 0;
  if (sorting) {
    sort_keys = m_amalloc->alloc_exc<PassthroughSortKey>(n_orderby);
    for (const OrderbyColumns* ob = m_context.ast_root.orderby_columns;
         ob != NULL; ob = ob->next) {
      Uint32 slot;
      if (ob->kind == OrderbyColumns::Kind::OUTPUT_REF) {
        require_prm(ob->output_idx < num_cols,
                    "ORDER BY alias resolves outside the SELECT outputs.");
        slot = ob->output_idx;
      } else {
        const QueryScope::ResolvedColumnRef& R =
            m_main_scope.resolved_columns[ob->col_idx];
        require_prm(R.kind ==
                        QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                    "ORDER BY column is not a stored-table column.");
        bool found = false;
        Uint32 oi = 0;
        slot = 0;
        for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
             o = o->next, oi++) {
          if (same_resolved_column(
                  R, m_main_scope.resolved_columns[o->column.col_idx])) {
            slot = oi;
            found = true;
            break;
          }
        }
        if (!found) {
          require_run(R.dict_column != NULL,
                      "ORDER BY column descriptor missing.");
          slot = num_stored++;
          attrs[slot] = scanOp->getValue(R.dict_column);
          require_run(attrs[slot] != NULL,
                      "ORDER BY column getValue() failed.");
        }
      }
      const NdbDictionary::Column* kcol = attrs[slot]->getColumn();
      require_prm(NdbSqlUtil::getType(
                      static_cast<Uint32>(kcol->getType())).m_cmp != NULL,
                  "ORDER BY is not supported on this column type.");
      sort_keys[n_sort_keys].slot = slot;
      sort_keys[n_sort_keys].ascending = ob->ascending;
      sort_keys[n_sort_keys].col = kcol;
      n_sort_keys++;
    }
  }
  PassthroughSortBuffer sort_buf(m_amalloc, num_stored);
  STAT_TS(m_conf.phase_stats, s_exec_start);
  STAT_SET(m_conf.phase_stats, ndbprep_us, s_scandef_start, s_exec_start);
  require_run(DBG(m_trans->execute(NdbTransaction::NoCommit)) == 0,
              "Failed to execute transaction (single-table pass-through).");
  STAT_TS(m_conf.phase_stats, s_exec_sent);
  STAT_SET(m_conf.phase_stats, send_us, s_exec_start, s_exec_sent);

  if (is_json) {
    m_resultprinter->print_passthrough_header(attrs, num_cols,
                                              m_conf.out_stream);
    header_emitted = true;
  }
  // Phase 2 (ronsql_orderby_limit_plan.md): LIMIT without ORDER BY —
  // stream rows and stop at the limit, then close the scan early
  // instead of draining the remaining batches.  limit == -1 means no
  // LIMIT.  With LIMIT 0 the loop never runs, so the deferred TSV
  // header is never printed (JSON keeps its framing).  Phase 4b's
  // index-order streaming takes the same path: the SF_OrderBy merge
  // delivers rows in ORDER BY order, so the cutoff is the top-N.
  const Int64 limit = m_context.ast_root.limit;
  Uint32 row_count = 0;
  int rc = 1;  // pre-set "scan complete" for the LIMIT-0 no-loop case
  bool limit_reached = (limit == 0);
  while (!limit_reached) {
    rc = DBG(scanOp->nextResult(true));
    if (row_count == 0) {
      // First nextResult covers data-node execution up to the first
      // result batch (captured whether or not a row arrived).
      STAT_TS(m_conf.phase_stats, s_first_row);
      STAT_SET(m_conf.phase_stats, firstbatch_us, s_exec_sent, s_first_row);
    }
    if (rc != 0) break;
    if (sorting) {
      // Phase 3: buffer the row for the post-drain sort.  The streaming
      // LIMIT cutoff below must not fire — top-N needs every row.
      buffer_passthrough_row(sort_buf, m_amalloc, attrs, NULL,
                             m_conf.err_stream);
      row_count++;
      continue;
    }
    if (!header_emitted) {
      m_resultprinter->print_passthrough_header(attrs, num_cols,
                                                m_conf.out_stream);
      header_emitted = true;
    }
    m_resultprinter->print_passthrough_row(attrs, num_cols,
                                           /*is_first_row=*/(row_count == 0),
                                           m_conf.out_stream);
    row_count++;
    if (limit >= 0 && (Int64)row_count >= limit) limit_reached = true;
  }
  STAT_TS(m_conf.phase_stats, s_drain_done);
#ifdef RONSQL_PHASE_STATS
  if (m_conf.phase_stats != nullptr) {
    // Rows are printed inside the drain loop on this path, so print_us
    // stays 0 and drain_us includes the formatting cost (same
    // convention as execute_passthrough_drain).
    m_conf.phase_stats->drain_us =
        (s_drain_done - s_exec_sent) - m_conf.phase_stats->firstbatch_us;
    m_conf.phase_stats->rows_drained = row_count;
  }
#endif
  if (limit_reached) {
    // Early close (Phase 2): release the still-open scan instead of
    // draining its remaining batches.
    scanOp->close();
  } else {
    require_run(rc == 1, "Single-table pass-through scan failed.");
  }
  if (sorting) {
    // Phase 3: sort the buffered rows and print the first
    // min(limit, n).  The deferred TSV header is emitted here on the
    // first printed row.
    sort_and_print_passthrough_rows(sort_buf, sort_keys, n_sort_keys,
                                    num_cols, limit, m_amalloc,
                                    m_resultprinter, header_emitted,
                                    m_conf.out_stream);
  }
  if (header_emitted) {
    m_resultprinter->print_passthrough_finish(m_conf.out_stream);
  }
}

void
RonSQLPreparer::cleanup_trans() {
  if (m_trans) {
    Ndb* ndb = m_conf.ndb;
    ndbrequire(ndb);
    DBGV(ndb->closeTransaction(m_trans));
    m_trans = NULL;
  }
}

void
RonSQLPreparer::promote_left_joins()
{
  JoinClause* joins = m_context.ast_root.joins;
  if (joins == NULL) return;
  JoinClause* list[MAX_SPJ_TREE_NODES];
  Uint32 num_joins = 0;
  for (JoinClause* j = joins;
       j != NULL && num_joins < MAX_SPJ_TREE_NODES; j = j->next) {
    list[num_joins++] = j;
  }
  // Aliases referenced by the ON conditions of effectively-INNER joins
  // processed so far (backward walk).  Capacity bounds: a query with
  // more joins or more conditions per join than fits here is rejected
  // by the planner's own caps before execution, so silently skipping
  // the overflow is safe (a missed promotion then at worst hits the
  // gate's defensive INNER-below-LEFT rejection).
  static const Uint32 MAX_INNER_REFS =
      MAX_SPJ_TREE_NODES * MAX_JOIN_KEY_COLS;
  LexCString inner_refs[MAX_INNER_REFS];
  Uint32 num_inner_refs = 0;
  for (Uint32 i = num_joins; i-- > 0; ) {
    JoinClause* j = list[i];
    if (j->join_type == JoinClause::LEFT_OUTER_JOIN) {
      const LexCString& alias = j->table.alias;
      for (Uint32 r = 0; r < num_inner_refs; r++) {
        if (inner_refs[r] == alias) {
          j->join_type = JoinClause::INNER_JOIN;
          break;
        }
      }
    }
    if (j->join_type == JoinClause::INNER_JOIN) {
      for (const JoinCondition* jc = j->conditions;
           jc != NULL; jc = jc->next) {
        if (num_inner_refs < MAX_INNER_REFS) {
          inner_refs[num_inner_refs++] = jc->parent_table;
        }
      }
    }
  }
}

void
RonSQLPreparer::collect_pk_equalities(
    struct ConditionalExpression* ce,
    const NdbDictionary::Table* table,
    struct ConditionalExpression* pk_const[],
    struct ConditionalExpression* pk_eq_ce[])
{
  if (ce == NULL) return;
  if (ce->op == T_AND)
  {
    collect_pk_equalities(ce->args.left, table, pk_const, pk_eq_ce);
    collect_pk_equalities(ce->args.right, table, pk_const, pk_eq_ce);
    return;
  }
  if (ce->op != T_EQUALS) return;

  ConditionalExpression* col_side = NULL;
  ConditionalExpression* const_side = NULL;
  if (ce->args.left->op == T_IDENTIFIER && ce->args.right->op != T_IDENTIFIER)
  {
    col_side = ce->args.left;
    const_side = ce->args.right;
  }
  else if (ce->args.right->op == T_IDENTIFIER && ce->args.left->op != T_IDENTIFIER)
  {
    col_side = ce->args.right;
    const_side = ce->args.left;
  }
  else return;

  const char* col_name = m_columns[col_side->col_idx].c_str();
  int nkeys = table->getNoOfPrimaryKeys();
  for (int k = 0; k < nkeys; k++)
  {
    const char* pk_name = table->getPrimaryKey(k);
    if (pk_name != NULL && strcmp(pk_name, col_name) == 0)
    {
      pk_const[k] = const_side;
      if (pk_eq_ce != NULL) pk_eq_ce[k] = ce;
      break;
    }
  }
}

// Phase 0a helper: AND-flatten a simplified WHERE tree into its
// top-level conjuncts.  Sets *overflow instead of throwing so callers
// can fall back to the whole-tree filter path, which never flattens.
static void
flatten_and_conjuncts(ConditionalExpression* ce,
                      ConditionalExpression* out[],
                      Uint32* num,
                      bool* overflow)
{
  if (ce == NULL || *overflow) return;
  if (ce->op == T_AND)
  {
    flatten_and_conjuncts(ce->args.left, out, num, overflow);
    flatten_and_conjuncts(ce->args.right, out, num, overflow);
    return;
  }
  if (*num >= MAX_WHERE_CONJUNCTS)
  {
    *overflow = true;
    return;
  }
  out[(*num)++] = ce;
}

bool
RonSQLPreparer::build_root_residual(
    struct ConditionalExpression* where_ce,
    struct ConditionalExpression* const consumed[],
    int nkeys,
    struct ConditionalExpression** residual_out)
{
  *residual_out = NULL;
  ConditionalExpression* conjuncts[MAX_WHERE_CONJUNCTS];
  Uint32 num = 0;
  bool overflow = false;
  flatten_and_conjuncts(where_ce, conjuncts, &num, &overflow);
  if (overflow) return false;
  ConditionalExpression* residual = NULL;
  for (Uint32 i = 0; i < num; i++)
  {
    bool used = false;
    for (int k = 0; k < nkeys; k++)
    {
      if (consumed[k] == conjuncts[i]) { used = true; break; }
    }
    if (used) continue;
    if (residual == NULL)
    {
      residual = conjuncts[i];
    }
    else
    {
      ConditionalExpression* combined =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
      combined->op = T_AND;
      combined->args.left = residual;
      combined->args.right = conjuncts[i];
      residual = combined;
    }
  }
  *residual_out = residual;
  return true;
}

void
RonSQLPreparer::execute_join()
{
  Ndb* ndb = m_conf.ndb;
  ndbrequire(m_trans == NULL);
  m_trans = ndb->startTransaction();
  require_run(m_trans != NULL, "Failed to start transaction.");

  JoinPlan& plan = m_main_scope.join_plan;

  // Virtual tables for CTE children of the main plan. Built up-front so
  // the agg leaf's NdbAggregator can be constructed against the right
  // schema even when the leaf is a CTE_LOOKUP (no physical table).
  NdbDictionary::Table* cteVirtualTables[MAX_SPJ_TREE_NODES] = {};
  build_cte_virtual_tables(plan, cteVirtualTables);

  // Build aggregator(s).
  // Multi-leaf: one NdbAggregator per merged leaf, each with its own program.
  // Single-leaf: one NdbAggregator on plan.agg_leaf_idx (existing path).
  NdbAggregator* leafAggs[MAX_SPJ_TREE_NODES] = {};
  const NdbDictionary::Table* agg_leaf_table =
      plan.ops[plan.agg_leaf_idx].table;
  if (agg_leaf_table == NULL) {
    // CTE_LOOKUP / CTE_SCAN leaf: use the per-CTE virtual table whose
    // columns mirror the CTE's output schema.
    agg_leaf_table = cteVirtualTables[plan.agg_leaf_idx];
    require_run(agg_leaf_table != NULL,
                "Main aggregator leaf is a CTE op but its virtual table "
                "was not built.");
  }
  NdbAggregator singleAgg(agg_leaf_table);

  if (m_has_select_subqueries && plan.num_agg_leaves > 0) {
    require_run(m_main_scope.resolved_columns != NULL,
                "Subquery aggregation emit: missing resolved columns.");
    // Build per-leaf aggregators for subquery pushdown
    for (Uint32 ml = 0; ml < m_merged_leaves.size(); ml++) {
      MergedLeaf &merged = m_merged_leaves[ml];
      Uint32 op_idx = merged.plan_op_idx;
      const NdbDictionary::Table* leaf_table = plan.ops[op_idx].table;
      NdbAggregator* agg = new NdbAggregator(leaf_table);

      // Program GROUP BY columns (all from root → GroupByLinked)
      Uint32 linked_proj_pos = 0;
      struct GroupbyColumns* groupby = m_context.ast_root.groupby_columns;
      while (groupby != NULL) {
        Uint32 col_idx = groupby->col_idx;
        const QueryScope::ResolvedColumnRef& col_ref =
            m_main_scope.resolved_columns[col_idx];
        require_prm(
            col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
            "Subquery GROUP BY column is not a stored-table column.");
        if (col_ref.join_op_idx == op_idx) {
          require_prm(agg->GroupBy(col_ref.attr_id),
                      "Failed to program GroupBy on leaf.");
        } else {
          require_prm(col_ref.dict_column != NULL,
                      "Subquery linked GROUP BY column descriptor missing.");
          require_prm(agg->GroupByLinked(linked_proj_pos,
                                         col_ref.dict_column),
                      "Failed to program GroupByLinked on leaf.");
          linked_proj_pos++;
        }
        groupby = groupby->next;
      }

      // Program aggregate functions for all subqueries merged into this leaf.
      // Slot indices are leaf-local (0-based); the kernel applies acc_offset.
      Uint32 leaf_local_slot = 0;
      for (Uint32 si = 0; si < m_select_subquery_leaves.size(); si++) {
        SelectSubqueryLeaf &sl = m_select_subquery_leaves[si];
        if (sl.merged_leaf_idx != ml) continue;

        Uint32 col_idx = sl.inner_agg_col_idx;
        const QueryScope::ResolvedColumnRef& agg_ref =
            m_main_scope.resolved_columns[col_idx];
        require_prm(
            agg_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
            "Subquery aggregate input is not a stored-table column.");
        NdbAttrId attr_id = agg_ref.attr_id;
        Uint32 reg = 0;  // use register 0 for loads
        Uint32 agg_slot = leaf_local_slot++;

        // If a cross-table filter exists, run the condition through the
        // embedded normal interpreter.  A failed predicate skips this
        // subquery's aggregate update; comparison and branch semantics remain
        // owned by the normal interpreter.
        //
        // Without a cross-table filter, just: LoadColumn + Agg.
        if (sl.cross_table_filter != NULL) {
          ConditionalExpression *cf = sl.cross_table_filter;
          require_prm(cf->args.left != NULL && cf->args.right != NULL &&
                      cf->args.left->op == T_IDENTIFIER &&
                      cf->args.right->op == T_IDENTIFIER,
                      "Cross-table filter must be a simple column comparison.");
          require_prm(cf->op == T_LT || cf->op == T_LE ||
                      cf->op == T_GT || cf->op == T_GE ||
                      cf->op == T_EQUALS || cf->op == T_NOT_EQUALS,
                      "Cross-table filter must use <, <=, >, >=, = or !=.");
          Uint32 aggregate_update_raw_size = sl.is_count_star ? 4 : 2;
          generate_embedded_condition(
              agg, m_main_scope, cf, 0, /*cteVirtualTables=*/NULL,
              /*use_custom_outputs=*/true,
              /*first_exit_output=*/aggregate_update_raw_size,
              /*second_exit_output=*/0,
              /*agg_leaf_idx_override=*/op_idx);
        }

        if (sl.is_count_star) {
          require_prm(agg->LoadUint64(1, reg),
                      "Failed to load immediate for COUNT(*).");
        } else {
          require_prm(agg->LoadColumn(attr_id, reg),
                      "Failed to load column for subquery aggregate.");
        }

        switch (sl.agg_fun) {
        case T_SUM:
          require_prm(agg->Sum(agg_slot, reg),
                      "Failed to program SUM.");
          break;
        case T_COUNT:
          require_prm(agg->Count(agg_slot, reg),
                      "Failed to program COUNT.");
          break;
        case T_MIN:
          require_prm(agg->Min(agg_slot, reg),
                      "Failed to program MIN.");
          break;
        case T_MAX:
          require_prm(agg->Max(agg_slot, reg),
                      "Failed to program MAX.");
          break;
        default:
          require_prm(false, "Unsupported aggregate function in subquery.");
        }
      }

      // No sentinel for multi-leaf subquery path — subquery results are
      // consumed per-row by the subquery handler, not through ResultPrinter.
      // Sentinel filtering only applies to explicit JOIN + GROUP BY queries
      // (handled in programAggregator_join).

      require_prm(agg->Finalize(), "Failed to finalize leaf aggregator.");
      leafAggs[op_idx] = agg;
    }
  } else if (m_is_aggregate_query) {
    // Single-leaf path (existing)
    programAggregator_join(m_main_scope, m_context.ast_root, &singleAgg,
                           cteVirtualTables);
    require_prm(singleAgg.Finalize(), "Failed to finalize aggregator.");
  }
  // Phase E.3: when !m_is_aggregate_query, singleAgg stays unprogrammed
  // and unfinalized.  emit_root_op will receive NULL and skip
  // setAggregation() on the scanCte root; result delivery uses
  // execute_passthrough_drain instead of NdbAggregator.

  // Build NdbQueryBuilder tree
  NdbQueryBuilder* qb = NdbQueryBuilder::create();
  ndbrequire(qb != NULL);

  // Emit CTE subtrees first, in declaration order, so the main query's
  // CTE_LOOKUP / CTE_SCAN references resolve.
  NdbAggregator** cteAggs = NULL;
  // Per-CTE child virt tables.  Storage spans the whole execute_join body
  // because qb's serialized scanCte ops retain raw NdbDictionary::Table*
  // pointers into these virt tables — they must outlive qb->prepare()
  // and the result-drain phase.  A 2D array (per CTE × per op) gives each
  // iteration its own cteChildVT row while keeping the underlying
  // NdbDictionary::Table objects alive until the end-of-execute cleanup.
  NdbDictionary::Table** cteChildVTAll = NULL;
  if (m_cte_scopes.size() > 0) {
    cteAggs = m_amalloc->alloc_exc<NdbAggregator*>(m_cte_scopes.size());
    cteChildVTAll = m_amalloc->alloc_exc<NdbDictionary::Table*>(
        m_cte_scopes.size() * MAX_SPJ_TREE_NODES);
    for (Uint32 i = 0; i < m_cte_scopes.size() * MAX_SPJ_TREE_NODES; i++) {
      cteChildVTAll[i] = NULL;
    }
    CteDefinition* cte = m_context.ast_root.cte_list;
    for (Uint32 c = 0; c < m_cte_scopes.size();
         c++, cte = cte->next) {
      QueryScope& cs = *m_cte_scopes[c];
      JoinPlan& cp = cs.join_plan;

      // Build virtual tables for this CTE body's CTE children up-front
      // (mirrors the main-query path).  Required so a chained CTE body
      // whose agg leaf is a CTE op can anchor cteAgg on the predecessor's
      // virt table; programAggregator_join also uses these to resolve
      // column metadata for inter-CTE GROUP BY / linked-load references.
      NdbDictionary::Table** cteChildVT =
          &cteChildVTAll[c * MAX_SPJ_TREE_NODES];
      build_cte_virtual_tables(cp, cteChildVT);

      const NdbDictionary::Table* cte_leaf_table =
          cp.ops[cp.agg_leaf_idx].table;
      if (cte_leaf_table == NULL) {
        // Chained CTE body whose aggregation leaf is a CTE op (the leaf
        // has no physical NDB table).  Anchor on the per-CTE virt table
        // — same trick as the main-query agg-leaf-table fallback above.
        cte_leaf_table = cteChildVT[cp.agg_leaf_idx];
        require_run(cte_leaf_table != NULL,
                    "CTE aggregation leaf is a CTE op but its virtual "
                    "table was not built.");
      }
      NdbAggregator* cteAgg = new NdbAggregator(cte_leaf_table);
      cteAggs[c] = cteAgg;

      if (cte->stmt->is_single_row_cte) {
        // Single-row projection program
        // (cte_single_row_kernel_plan.md): every output is a GROUP BY
        // column and there are ZERO aggregate slots — the kernel
        // stores the (at most one) row as a key-only group record.
        // Bodies are single-table by candidacy, so every output
        // resolves to a stored column of the source table.
        cteAgg->SetSingleRowMode();
        for (const Outputs* o = cte->stmt->outputs; o != NULL;
             o = o->next) {
          const QueryScope::ResolvedColumnRef& ref =
              cs.resolved_columns[o->column.col_idx];
          require_prm(ref.kind ==
                          QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                      "Single-row CTE output did not resolve to a "
                      "stored column.");
          require_prm(cteAgg->GroupBy((Int32)ref.attr_id),
                      "Failed to program the single-row CTE projection "
                      "(BLOB/TEXT columns cannot be projected).");
        }
      } else {
        programAggregator_join(cs, *cte->stmt, cteAgg, cteChildVT);
      }
      require_prm(cteAgg->Finalize(), "Failed to finalize CTE aggregator.");

      qb->beginCteSubtree(c);
      const NdbQueryOperationDef* cteOpDefs[MAX_SPJ_TREE_NODES];
      if (cp.num_ops == 1 && cp.ops[0].type == JoinOp::TABLE_SCAN) {
        // Single-table CTE body: attach the aggregator directly on the
        // root scan.  The scan is then a T_CTE_SCAN node with no
        // T_AGGREGATE_LEAF child below it, so DBSPJ sets JoinAggFlag on
        // the SCAN_FRAGREQ and DBLQH/DBTUP feed each scanned row into
        // the CTE hash table in place (handleJoinAggRow) — no per-row
        // PK round trip through DBSPJ and no second read of the row.
        // The aggregation program itself travels via defineCte(); the
        // aggregator on the op only marks it NI_AGGREGATE_LEAF.
        const NdbDictionary::Table* srcTab = cp.ops[0].table;
        require_run(srcTab != NULL, "CTE body root has no physical table.");

        // Attach the CTE body WHERE filter (pre-GROUP BY) to the root
        // scan when present. build_cte_scopes classifies the CTE body's
        // WHERE into cs.join_where_ce; emit_root_op handles this for
        // multi-op bodies, but the single-table path must do it inline.
        // The WHERE interpreter runs before the aggregation feed, so
        // only passing rows aggregate.
        NdbQueryOptions rootOpts;
        NdbInterpretedCode rootCode(srcTab);
        if (cs.join_where_ce[0] != NULL) {
          ConditionalExpression* root_ce =
              simplify_ce(cs.join_where_ce[0], -1);
          NdbScanFilter filter(&rootCode);
          filter.setSqlCmpSemantics();
          filter.begin(NdbScanFilter::AND);
          apply_filter(&filter, cs, root_ce);
          filter.end();
          rootCode.finalise();
          rootOpts.setInterpretedCode(rootCode);
        }
        require_run(rootOpts.setAggregation(*cteAgg) == 0,
                    "Failed to attach aggregator to CTE body scan root.");
        cteOpDefs[0] = qb->scanTable(srcTab, &rootOpts);
        require_run(cteOpDefs[0] != NULL,
                    "Failed to create CTE body scan root.");
      } else if (cp.num_ops == 1 && cp.ops[0].type == JoinOp::INDEX_SCAN) {
        // Phase I.9: single-table CTE body via ordered index.  Same
        // in-place aggregation feed as the TABLE_SCAN branch, but use
        // scanIndex(idx, srcTab, &bound) for the root and route bound
        // conjuncts to NdbQueryIndexBound, residual conjuncts to the
        // InterpretedCode filter.  Bound vs residual routing comes from
        // select_root_scan_config's per-scope condition_handling_map,
        // and the bound/filter emit itself is the shared
        // emit_index_scan_root (also used for main-query roots, see
        // join_root_index_scan_plan.md).
        const NdbDictionary::Table* srcTab = cp.ops[0].table;
        const NdbDictionary::Index* idx = cp.ops[0].index;
        require_run(srcTab != NULL,
                    "CTE body INDEX_SCAN root has no physical table.");
        require_run(idx != NULL,
                    "CTE body INDEX_SCAN root has no index.");

        NdbQueryOptions rootOpts;
        if (cs.body_minmax_kind != QueryScope::MinMaxKind::NONE) {
          // Phase I.10 scalar MIN/MAX: ordered scan + maxRows=1.  Keep
          // the readTuple(linked_pk) self-join here — the early close
          // relies on scan rows reaching DBSPJ for the maxRows
          // accounting, which an in-place aggregation scan (0 reported
          // rows) would defeat.
          if (cs.body_minmax_kind == QueryScope::MinMaxKind::MAX_DESC) {
            rootOpts.setOrdering(NdbQueryOptions::ScanOrdering_descending);
          } else {
            rootOpts.setOrdering(NdbQueryOptions::ScanOrdering_ascending);
          }
          rootOpts.setMaxRows(1);
          cteOpDefs[0] = qb->scanIndex(idx, srcTab, NULL, &rootOpts);
          require_run(cteOpDefs[0] != NULL,
                      "Failed to create CTE body index-scan root.");

          const NdbQueryOperand* keys[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
          int nkeys = srcTab->getNoOfPrimaryKeys();
          for (int k = 0; k < nkeys; k++) {
            const char* pk_name = srcTab->getPrimaryKey(k);
            keys[k] = qb->linkedValue(cteOpDefs[0], pk_name);
            require_run(keys[k] != NULL,
                        "Failed to create CTE body self-join linked key.");
          }
          keys[nkeys] = nullptr;
          NdbQueryOptions leafOpts;
          leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
          require_run(leafOpts.setAggregation(*cteAgg) == 0,
                      "Failed to attach aggregator to CTE body leaf.");
          cteOpDefs[1] = qb->readTuple(srcTab, keys, &leafOpts);
          require_run(cteOpDefs[1] != NULL,
                      "Failed to create CTE body self-join leaf.");
        } else {
          // Aggregator directly on the index-scan root — same in-place
          // feed as the TABLE_SCAN branch; bounds and residual filter
          // come from emit_index_scan_root.
          require_run(rootOpts.setAggregation(*cteAgg) == 0,
                      "Failed to attach aggregator to CTE body scan root.");
          cteOpDefs[0] = emit_index_scan_root(qb, cs, srcTab, idx, rootOpts);
        }
      } else if (cp.num_ops == 1 && cp.ops[0].type == JoinOp::CTE_SCAN) {
        // Chained CTE body whose root is a CTE_SCAN reading a
        // predecessor CTE.  emit_root_op already handles CTE_SCAN
        // (sets up scanCte + main aggregator + WHERE filter) — reuse
        // it with cteAgg in the singleAgg slot so the body's aggregator
        // attaches on the scanCte root.
        emit_root_op(qb, cs, cteOpDefs, cteAgg, cteChildVT);
      } else {
        // Multi-op body.  Pass cteAgg to emit_root_op so it attaches
        // on the CTE_SCAN root when cp.agg_leaf_idx == 0 (chained CTE
        // with joined children).  emit_child_ops covers all other
        // agg-leaf positions.
        emit_root_op(qb, cs, cteOpDefs, cteAgg, cteChildVT);
        emit_child_ops(qb, cs, cteOpDefs, cteAgg, nullptr, cteChildVT);
      }
      qb->endCteSubtree();
      // For chained CTE bodies whose root is CTE_SCAN, cs.table is NULL.
      // defineCte's srcTab argument expects a table descriptor that
      // matches the CTE's output schema — use the predecessor's virt
      // table from cteChildVT[0].
      const NdbDictionary::Table* defineSrcTab = cs.table;
      if (cp.ops[0].type == JoinOp::CTE_SCAN) {
        defineSrcTab = cteChildVT[0];
        require_run(defineSrcTab != NULL,
                    "defineCte for chained CTE: predecessor virtual "
                    "table missing.");
      }
      // Compute dependency bitmask: bit i set for each predecessor CTE
      // referenced by this body.  DBTC turns this into phase numbers so
      // chained CTEs materialize in the right order; without it,
      // execCTE_SCAN_REQ for a not-yet-ready predecessor returns
      // ZCTE_LOOKUP_STATE_NOT_READY (1264).
      Uint64 cteDepMask = 0;
      for (Uint32 i = 0; i < cp.num_ops; i++) {
        const JoinOp& cop = cp.ops[i];
        if (cop.type == JoinOp::CTE_LOOKUP || cop.type == JoinOp::CTE_SCAN) {
          require_run(cop.cte_def_idx < 64,
                      "CTE depMask: cte_def_idx out of range.");
          cteDepMask |= (Uint64(1) << cop.cte_def_idx);
        }
      }
      require_run(qb->defineCte(c, defineSrcTab, *cteAgg, cteDepMask,
                                 cte->stmt->is_single_row_cte
                                     ? QN_CteSubtreeNode::CTE_SINGLE_ROW
                                     : 0) == 0,
                  "Failed to defineCte.");
      // Don't delete cteChildVT entries here: qb's serialized scanCte
      // op retains raw NdbDictionary::Table* pointers into them.
      // Deletion happens in the end-of-execute cleanup alongside main
      // cteVirtualTables.
    }
  }

  const NdbQueryOperationDef* opDefs[MAX_SPJ_TREE_NODES];

  NdbAggregator* main_singleAgg =
      m_is_aggregate_query ? &singleAgg : nullptr;
  emit_root_op(qb, m_main_scope, opDefs, main_singleAgg, cteVirtualTables);

  // Phase E.3 (single-CTE projection): num_ops==1 and no children
  // to emit.  Phase I.8: real-table root + CTE_LOOKUP child
  // (testCteNdbApi.cpp Test 17 shape) — the join_plan has the
  // child op, and emit_child_ops's aggregator-attach blocks are
  // each gated on singleAgg/leafAggs being non-NULL, so passing
  // nullptr in the no-aggregate path naturally skips them.
  if (m_main_scope.join_plan.num_ops > 1) {
    emit_child_ops(qb, m_main_scope, opDefs,
                   m_is_aggregate_query ? &singleAgg : nullptr,
                   m_is_aggregate_query ? leafAggs : nullptr,
                   cteVirtualTables);
  }

  // Virtual tables stay alive until the end of this function: the
  // NdbAggregator caches virt-table NdbDictionary::Column pointers (see
  // gb_columns_ in NdbAggregator::GroupByLinked) and ResultPrinter reads
  // them via column.type() during print_result. Deleting here would leave
  // dangling pointers.

  // Prepare and execute
  PERF_TS(t_qb_prepare);
  STAT_TS(m_conf.phase_stats, s_qb_prepare);
  const NdbQueryDef* queryDef = qb->prepare(ndb);
  require_run(queryDef != NULL, "Failed to prepare query.");
  PERF_TS(t_qb_prepared);
  PERF_LOG("  qb->prepare", t_qb_prepare, t_qb_prepared);
  STAT_TS(m_conf.phase_stats, s_qb_prepared);
  STAT_SET(m_conf.phase_stats, ndbprep_us, s_qb_prepare, s_qb_prepared);

  NdbQuery* query = m_trans->createQuery(queryDef);
  require_run(query != NULL, "Failed to create query.");

  if (!m_is_aggregate_query) {
    // Phase E.3 / I.8 pass-through path: wire getValue() per output
    // BEFORE m_trans->execute() (NdbQueryOperation::getValue must be
    // called between createQuery and execute, mirroring
    // testCteNdbApi.cpp Tests 8 / 17), then drain rows after execute.
    // Helper does both halves around the trans->execute() the
    // existing aggregating path runs separately below.  The whole
    // cteVirtualTables array is forwarded so the helper can route
    // each output to the correct op (CTE child or real-table parent).
    execute_passthrough_drain(query, cteVirtualTables);
    PERF_TS(t_drain_done);
    PERF_LOG("  pass-through drain", t_qb_prepare, t_drain_done);
    PERF_LOG("  execute total", t_qb_prepare, t_drain_done);
  } else {
    PERF_TS(t_exec_start);
    STAT_TS(m_conf.phase_stats, s_exec_start);
    require_run(m_trans->execute(NdbTransaction::NoCommit) == 0,
                "Failed to execute transaction.");
    PERF_TS(t_exec_sent);
    PERF_LOG("  trans->execute", t_exec_start, t_exec_sent);
    STAT_TS(m_conf.phase_stats, s_exec_sent);
    STAT_SET(m_conf.phase_stats, send_us, s_exec_start, s_exec_sent);

    // Consume all rows.  The first nextResult covers data-node execution
    // up to the first result batch (incl. CTE materialization for CTE
    // queries); the remaining drain is result transfer + local unpack.
    Uint64 n_rows = 0;
    NdbQuery::NextResultOutcome rc = query->nextResult(true);
    STAT_TS(m_conf.phase_stats, s_first_row);
    STAT_SET(m_conf.phase_stats, firstbatch_us, s_exec_sent, s_first_row);
    while (rc == NdbQuery::NextResult_gotRow) {
      n_rows++;
      rc = query->nextResult(true);
    }
    PERF_TS(t_drain_done);
    PERF_LOG("  drain results", t_exec_sent, t_drain_done);
    STAT_TS(m_conf.phase_stats, s_drain_done);
    STAT_SET(m_conf.phase_stats, drain_us, s_first_row, s_drain_done);
    STAT_COUNT(m_conf.phase_stats, rows_drained, n_rows);
    (void)n_rows;
    if (rc == NdbQuery::NextResult_error)
    {
      const NdbError& err = query->getNdbError();
      std::basic_ostream<char>& errout = *m_conf.err_stream;
      errout << "Join query failed: " << err.message
             << " (code " << err.code << ")" << std::endl;
      const NdbError::Classification classification = err.classification;
      query->close();
      queryDef->destroy();
      qb->destroy();
      // Schema errors (e.g. WrongSchemaVersion 20021 from DBSPJ after a
      // concurrent DDL) must invalidate the cached dictionary before retry,
      // otherwise every retry resends the same stale schema version.  Route
      // through RonSQLMaybeStaleSchema so handle_ronsql_exception calls
      // unload_schema(); the next attempt's RonSQLPreparer load() then
      // fetches the fresh schema.
      if (classification == NdbError::SchemaError) {
        throw RonSQLMaybeStaleSchema("Join query execution failed.");
      }
      throw RonSQLRetryableError("Join query execution failed.");
    }

    // Collect and print aggregation results
    PERF_TS(t_result_start);
    STAT_TS(m_conf.phase_stats, s_result_start);
    NdbAggregator* resultAgg = query->getAggregator();
    ndbrequire(resultAgg != NULL);
    m_resultprinter->print_result(resultAgg, m_conf.out_stream);
    PERF_TS(t_result_done);
    PERF_LOG("  print results", t_result_start, t_result_done);
    PERF_LOG("  execute total", t_qb_prepare, t_result_done);
    STAT_TS(m_conf.phase_stats, s_result_done);
    STAT_SET(m_conf.phase_stats, print_us, s_result_start, s_result_done);
  }

  // Cleanup
  query->close();
  queryDef->destroy();
  qb->destroy();

  // Free per-leaf aggregators
  for (Uint32 i = 0; i < MAX_SPJ_TREE_NODES; i++) {
    if (leafAggs[i] != NULL) {
      delete leafAggs[i];
      leafAggs[i] = NULL;
    }
  }

  // Free per-CTE aggregators
  if (cteAggs != NULL) {
    for (Uint32 c = 0; c < m_cte_scopes.size(); c++) {
      delete cteAggs[c];
      cteAggs[c] = NULL;
    }
  }

  // Free per-CTE virtual tables last. Aggregators and ResultPrinter have
  // already been drained, so the cached virt-table column pointers they
  // used are no longer needed.
  for (Uint32 i = 0; i < MAX_SPJ_TREE_NODES; i++) {
    delete cteVirtualTables[i];
    cteVirtualTables[i] = NULL;
  }
  // Same for per-CTE-body child virt tables held alive across qb's lifetime
  // (chained CTE scanCte ops retain raw pointers into these).
  if (cteChildVTAll != NULL) {
    for (Uint32 i = 0;
         i < m_cte_scopes.size() * MAX_SPJ_TREE_NODES; i++) {
      delete cteChildVTAll[i];
      cteChildVTAll[i] = NULL;
    }
  }
}

// Pass-through row delivery for projection-only main SELECTs.
// Originally Phase E.3 (single CTE_SCAN root); Phase I.8 generalized
// to multi-op shapes (real-table root + CTE_LOOKUP child, etc.).
// Each output column is routed to its owning operation via
// m_main_scope.resolved_columns: CTE refs use the pre-registered
// cteAttrsByCol entry, real-table refs use the stored column descriptor.
//
// Mirrors testCteNdbApi.cpp Tests 8 (single CTE_SCAN root) and 17
// (readTuple root + CTE_LOOKUP child) — both call getValue() on
// the right NdbQueryOperation per projection column.
void
RonSQLPreparer::execute_passthrough_drain(NdbQuery* query,
                                           NdbDictionary::Table**
                                               cteVirtualTables)
{
  ndbrequire(query != NULL);
  ndbrequire(m_resultprinter != NULL);

  // CTE subtree ops are appended to the NdbQuery before main-query
  // ops (the qb->beginCteSubtree...endCteSubtree blocks run first
  // in execute_join, then emit_root_op + emit_child_ops).  Main-query
  // ops occupy the trailing num_main_ops positions; their JoinPlan
  // index 0..num_main_ops-1 maps to query op index
  // numCteSubtreeOps..numCteSubtreeOps+num_main_ops-1.
  Uint32 numTotalOps = query->getNoOfOperations();
  Uint32 numMainOps = m_main_scope.join_plan.num_ops;
  require_run(numTotalOps >= numMainOps,
              "Pass-through drain: query has fewer operations than the "
              "main JoinPlan reports.");
  Uint32 numCteSubtreeOps = numTotalOps - numMainOps;

  Uint32 num_cols = 0;
  for (const Outputs* o = m_context.ast_root.outputs; o != NULL; o = o->next)
    num_cols++;
  require_run(num_cols > 0, "Pass-through drain: no outputs to deliver.");
  // Phase 3 (ronsql_orderby_limit_plan.md): ORDER BY-only columns
  // occupy extra slots after the outputs, so all per-slot arrays get
  // room for them up front.
  Uint32 n_orderby = 0;
  for (const OrderbyColumns* ob = m_context.ast_root.orderby_columns;
       ob != NULL; ob = ob->next)
    n_orderby++;
  NdbRecAttr** attrs =
      m_amalloc->alloc_exc<NdbRecAttr*>(num_cols + n_orderby);
  // Phase I.12: parallel array tracking the NdbQueryOperation that
  // produced each output column.  Needed at row-delivery time so we
  // can call op->isRowNULL() — the LEFT JOIN NULL-row marker is on
  // the operation, not on individual NdbRecAttrs.
  NdbQueryOperation** output_ops =
      m_amalloc->alloc_exc<NdbQueryOperation*>(num_cols + n_orderby);
  // Per-row effective_attrs[]: attrs[] with NULL substituted for
  // every column whose owning op reports isRowNULL().  Allocated
  // once, refilled per row.
  NdbRecAttr** effective_attrs =
      m_amalloc->alloc_exc<NdbRecAttr*>(num_cols);
  require_run(m_main_scope.resolved_columns != NULL,
              "Pass-through drain: missing resolved columns.");

  // Register NdbRecAttrs per main op.  Two constraints from the NDB
  // API receiver layer:
  //   (a) NdbReceiver::handle_rec_attrs walks the m_firstRecAttr list
  //       sequentially and matches each entry's attrId against the
  //       incoming row's attrId stream — entries must be registered
  //       in the same order the kernel emits.
  //   (b) For CTE ops, lookupCte/scanCte hardcode the kernel emit to
  //       send ALL numResultCols virt-table columns (attrIds 0..N-1)
  //       per row, regardless of which the SELECT projects.  So we
  //       must register every virt-table column up front in attrId
  //       order, otherwise packed_rowsize undersizes the buffer
  //       (NdbReceiverBuffer::allocRow assertion) AND the row's
  //       attrIds won't line up with our NdbRecAttrs (handle_rec_attrs
  //       abort).  testCteNdbApi.cpp Test 17 demonstrates the same
  //       requirement — it always reads BOTH grp and total off the
  //       CTE child, in declaration order.
  // For real-table ops, getValue populates the kernel's per-op
  // AttrInfo program in registration order, so the kernel emits in
  // the same order — registering only the SELECT'd columns (in any
  // order) is fine.
  //
  // Per-CTE-op map of cte_col_idx → NdbRecAttr*; we look up the right
  // entry per output afterwards.
  NdbRecAttr*** cteAttrsByCol =
      m_amalloc->alloc_exc<NdbRecAttr**>(numMainOps);
  for (Uint32 dop = 0; dop < numMainOps; dop++) cteAttrsByCol[dop] = NULL;

  for (Uint32 dop = 0; dop < numMainOps; dop++) {
    const JoinOp& djop = m_main_scope.join_plan.ops[dop];
    if (djop.type != JoinOp::CTE_LOOKUP &&
        djop.type != JoinOp::CTE_SCAN) continue;
    require_run(cteVirtualTables != NULL && cteVirtualTables[dop] != NULL,
                "Pass-through drain: missing CTE virt-table for op.");
    NdbQueryOperation* dopOp =
        query->getQueryOperation(numCteSubtreeOps + dop);
    require_run(dopOp != NULL,
                "Pass-through drain: failed to resolve CTE-op handle.");
    Uint32 vtNcols = (Uint32)cteVirtualTables[dop]->getNoOfColumns();
    cteAttrsByCol[dop] = m_amalloc->alloc_exc<NdbRecAttr*>(vtNcols);
    for (Uint32 cte_col = 0; cte_col < vtNcols; cte_col++) {
      const NdbDictionary::Column* vcol =
          cteVirtualTables[dop]->getColumn(cte_col);
      ndbrequire(vcol != NULL);
      NdbRecAttr* ra = dopOp->getValue(vcol);
      require_run(ra != NULL,
                  "Pass-through drain: CTE column getValue() failed.");
      cteAttrsByCol[dop][cte_col] = ra;
    }
  }

  // Real-table outputs — register in SELECT declaration order.  Also
  // build attrs[] for both real-table and CTE outputs (CTE entries
  // resolve via cteAttrsByCol).
  // Track which main ops have at least one projected column.  An op with an
  // EMPTY projection (no getValue) breaks the NDB API two ways: a root op
  // hangs in nextResult (the D3/H1 case), and a real-table CHILD op is
  // rejected with NDB error 4826 "Query has operation with empty projection"
  // (e.g. `cte JOIN real` selecting only CTE columns — the real table is used
  // for the join but never projected).  CTE_LOOKUP/CTE_SCAN ops always get a
  // getValue from the cteAttrsByCol loop above, so only real-table ops need
  // the guard applied after this loop.
  bool* op_has_value = m_amalloc->alloc_exc<bool>(numMainOps);
  for (Uint32 dop = 0; dop < numMainOps; dop++) op_has_value[dop] = false;
  Uint32 i = 0;
  for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
       o = o->next, i++) {
    ndbrequire(o->type == Outputs::Type::COLUMN);
    Uint32 col_idx = o->column.col_idx;
    const QueryScope::ResolvedColumnRef& col_ref =
        m_main_scope.resolved_columns[col_idx];
    require_prm(
        col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn ||
        col_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
        "Pass-through drain: output is not a table or CTE column.");
    Uint32 plan_op_idx = col_ref.join_op_idx;
    require_run(plan_op_idx < numMainOps,
                "Pass-through drain: column resolves to op outside "
                "the main JoinPlan.");
    op_has_value[plan_op_idx] = true;
    NdbQueryOperation* op =
        query->getQueryOperation(numCteSubtreeOps + plan_op_idx);
    require_run(op != NULL,
                "Pass-through drain: failed to resolve "
                "NdbQueryOperation for output column.");

    if (col_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn) {
      Uint32 cte_col_idx = col_ref.cte_result_idx;
      ndbrequire(cteAttrsByCol[plan_op_idx] != NULL);
      attrs[i] = cteAttrsByCol[plan_op_idx][cte_col_idx];
      require_run(attrs[i] != NULL,
                  "Pass-through drain: CTE NdbRecAttr lookup failed.");
    } else {
      const NdbDictionary::Column* col = col_ref.dict_column;
      require_run(col != NULL,
                  "Pass-through drain: real-table column descriptor "
                  "missing.");
      attrs[i] = op->getValue(col);
      require_run(attrs[i] != NULL,
                  "Pass-through drain: real-table getValue() failed.");
    }
    output_ops[i] = op;
  }

  // Phase 3 (ronsql_orderby_limit_plan.md): resolve ORDER BY sort keys
  // BEFORE execute.  A sort column already in the SELECT reuses its
  // output slot; an ORDER BY-only real-table column gets an extra
  // getValue on its op, and an ORDER BY-only CTE column reuses the
  // always-fetched virt-table registration (the kernel emits every
  // virt-table column regardless of the projection).
  const bool sorting = (n_orderby > 0);
  Uint32 num_stored = num_cols;
  PassthroughSortKey* sort_keys = NULL;
  Uint32 n_sort_keys = 0;
  if (sorting) {
    sort_keys = m_amalloc->alloc_exc<PassthroughSortKey>(n_orderby);
    for (const OrderbyColumns* ob = m_context.ast_root.orderby_columns;
         ob != NULL; ob = ob->next) {
      Uint32 slot;
      if (ob->kind == OrderbyColumns::Kind::OUTPUT_REF) {
        require_prm(ob->output_idx < num_cols,
                    "ORDER BY alias resolves outside the SELECT outputs.");
        slot = ob->output_idx;
      } else {
        const QueryScope::ResolvedColumnRef& R =
            m_main_scope.resolved_columns[ob->col_idx];
        require_prm(
            R.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn ||
            R.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
            "ORDER BY target is not a table or CTE column.");
        bool found = false;
        Uint32 oi = 0;
        slot = 0;
        for (const Outputs* o = m_context.ast_root.outputs; o != NULL;
             o = o->next, oi++) {
          if (same_resolved_column(
                  R, m_main_scope.resolved_columns[o->column.col_idx])) {
            slot = oi;
            found = true;
            break;
          }
        }
        if (!found) {
          Uint32 plan_op_idx = R.join_op_idx;
          require_run(plan_op_idx < numMainOps,
                      "ORDER BY column resolves to an op outside the "
                      "main JoinPlan.");
          NdbQueryOperation* op =
              query->getQueryOperation(numCteSubtreeOps + plan_op_idx);
          require_run(op != NULL,
                      "Failed to resolve the op for an ORDER BY column.");
          slot = num_stored++;
          if (R.kind ==
              QueryScope::ResolvedColumnRef::Kind::CteResultColumn) {
            ndbrequire(cteAttrsByCol[plan_op_idx] != NULL);
            attrs[slot] = cteAttrsByCol[plan_op_idx][R.cte_result_idx];
            require_run(attrs[slot] != NULL,
                        "ORDER BY CTE column NdbRecAttr lookup failed.");
          } else {
            require_run(R.dict_column != NULL,
                        "ORDER BY column descriptor missing.");
            attrs[slot] = op->getValue(R.dict_column);
            require_run(attrs[slot] != NULL,
                        "ORDER BY column getValue() failed.");
            op_has_value[plan_op_idx] = true;
          }
          output_ops[slot] = op;
        }
      }
      const NdbDictionary::Column* kcol = attrs[slot]->getColumn();
      require_prm(NdbSqlUtil::getType(
                      static_cast<Uint32>(kcol->getType())).m_cmp != NULL,
                  "ORDER BY is not supported on this column type.");
      sort_keys[n_sort_keys].slot = slot;
      sort_keys[n_sort_keys].ascending = ob->ascending;
      sort_keys[n_sort_keys].col = kcol;
      n_sort_keys++;
    }
  }
  PassthroughSortBuffer sort_buf(m_amalloc, num_stored);

  // D3/H1 + empty-projection fix: the NDB API only drives (and sizes the
  // receive buffer for) a query operation that has at least one getValue().
  // A real-table main op with an EMPTY projection either hangs (root op — the
  // projected columns all come from a CTE child, D3/H1) or is rejected with
  // NDB error 4826 (a real-table child used only for the join, e.g. cte JOIN
  // real selecting only CTE columns).  For every real-table main op that no
  // output reads, register a throwaway getValue on its first column (mirrors
  // testCteNdbApi.cpp Test 17's mainQueryOp->getValue("grp")).  CTE ops are
  // skipped — cteAttrsByCol already registered all their virtual columns.
  for (Uint32 dop = 0; dop < numMainOps; dop++) {
    if (op_has_value[dop]) continue;
    const JoinOp& gjop = m_main_scope.join_plan.ops[dop];
    if (gjop.type == JoinOp::CTE_LOOKUP || gjop.type == JoinOp::CTE_SCAN)
      continue;
    const NdbDictionary::Table* gtab = gjop.table;
    if (gtab == NULL || gtab->getNoOfColumns() == 0) continue;
    NdbQueryOperation* gop = query->getQueryOperation(numCteSubtreeOps + dop);
    require_run(gop != NULL,
                "Pass-through drain: failed to resolve op for the dummy "
                "empty-projection read.");
    require_run(gop->getValue(gtab->getColumn(0)) != NULL,
                "Pass-through drain: dummy getValue() on an empty-projection "
                "op failed.");
    DEB_DRAIN(("[drain] registered dummy getValue on main op %u (col '%s') — "
               "no SELECT column reads it\n",
               dop, gtab->getColumn(0)->getName()));
  }

  DEB_DRAIN(("[drain] before execute(NoCommit): numTotalOps=%u numMainOps=%u "
             "numCteSubtreeOps=%u num_cols=%u\n",
             numTotalOps, numMainOps, numCteSubtreeOps, num_cols));
  STAT_TS(m_conf.phase_stats, s_exec_start);
  require_run(m_trans->execute(NdbTransaction::NoCommit) == 0,
              "Failed to execute transaction (pass-through).");
  STAT_TS(m_conf.phase_stats, s_exec_sent);
  STAT_SET(m_conf.phase_stats, send_us, s_exec_start, s_exec_sent);
  DEB_DRAIN(("[drain] execute(NoCommit) returned OK; entering drain\n"));

  // For JSON output we always want the framing '[' ... ']' (an empty
  // array is the correct empty representation).  For TSV we defer
  // the header line until at least one row arrives so empty results
  // produce no output, matching the mysql client baseline that
  // ronsql_compare.inc diffs against.
  bool header_emitted = false;
  bool is_json =
      (m_conf.output_format == RonSQLExecParams::OutputFormat::JSON ||
       m_conf.output_format == RonSQLExecParams::OutputFormat::JSON_ASCII);
  if (is_json) {
    m_resultprinter->print_passthrough_header(
        const_cast<const NdbRecAttr* const*>(attrs), num_cols,
        m_conf.out_stream);
    header_emitted = true;
  }

  Uint32 row_count = 0;
  // Phase 2 (ronsql_orderby_limit_plan.md): LIMIT without ORDER BY —
  // stop fetching once `limit` rows are printed and return; the
  // caller's query->close() then closes the still-open scan early
  // (I.14 kernel/API early close, block-tested).  limit == -1 means no
  // LIMIT.  With LIMIT 0 the loop never runs, so the deferred TSV
  // header is never printed — matching the mysql client's empty-result
  // output (JSON keeps its '[' ... ']' framing).
  const Int64 limit = m_context.ast_root.limit;
  // rc pre-set for the post-loop error check when LIMIT 0 skips the loop.
  NdbQuery::NextResultOutcome rc = NdbQuery::NextResult_scanComplete;
  // D3-hang instrumentation: log BEFORE each nextResult(true) and the rc it
  // returns.  If the consumer hangs, the last line will be "calling nextResult,
  // row_count=N" with NO following "returned rc=..." — i.e. it blocked in
  // nextResult after N rows instead of returning scanComplete.
  // Phase 3: with ORDER BY every row is fetched and buffered (the LIMIT
  // is applied post-sort) — except LIMIT 0, which prints nothing either
  // way and skips the drain entirely.
  while ((sorting && limit != 0) ||
         limit < 0 || (Int64)row_count < limit) {
    DEB_DRAIN(("[drain] calling nextResult(true), row_count=%u\n", row_count));
    rc = query->nextResult(true);
    if (row_count == 0) {
      // First nextResult covers data-node execution up to the first result
      // batch (captured whether or not a row arrived).
      STAT_TS(m_conf.phase_stats, s_first_row);
      STAT_SET(m_conf.phase_stats, firstbatch_us, s_exec_sent, s_first_row);
    }
    DEB_DRAIN(("[drain] nextResult returned rc=%d "
               "(gotRow=%d scanComplete=%d error=%d bufferEmpty=%d)\n",
               (int)rc, (int)NdbQuery::NextResult_gotRow,
               (int)NdbQuery::NextResult_scanComplete,
               (int)NdbQuery::NextResult_error,
               (int)NdbQuery::NextResult_bufferEmpty));
    if (rc != NdbQuery::NextResult_gotRow) break;
    if (sorting) {
      // Phase 3: buffer the row (clones; op-level LEFT JOIN NULL rows
      // stored as SQL NULL) for the post-drain sort.
      buffer_passthrough_row(sort_buf, m_amalloc,
                             const_cast<const NdbRecAttr* const*>(attrs),
                             output_ops, m_conf.err_stream);
      row_count++;
      continue;
    }
    if (!header_emitted) {
      m_resultprinter->print_passthrough_header(
          const_cast<const NdbRecAttr* const*>(attrs), num_cols,
          m_conf.out_stream);
      header_emitted = true;
    }
    // Phase I.12: substitute NULL for every column whose op reports
    // isRowNULL() (LEFT JOIN unmatched-row marker).  print_passthrough_value
    // already treats a NULL NdbRecAttr* as the NULL representation.
    for (Uint32 ci = 0; ci < num_cols; ci++) {
      effective_attrs[ci] =
          (output_ops[ci] != NULL && output_ops[ci]->isRowNULL())
              ? NULL : attrs[ci];
    }
    m_resultprinter->print_passthrough_row(
        const_cast<const NdbRecAttr* const*>(effective_attrs), num_cols,
        /*is_first_row=*/(row_count == 0),
        m_conf.out_stream);
    row_count++;
  }
  DEB_DRAIN(("[drain] loop exited: rc=%d total_rows=%u\n", (int)rc, row_count));
  STAT_TS(m_conf.phase_stats, s_drain_done);
#ifdef RONSQL_PHASE_STATS
  if (m_conf.phase_stats != nullptr) {
    // Rows are printed inside the drain loop on this path, so print_us
    // stays 0 and drain_us includes the formatting cost.
    m_conf.phase_stats->drain_us =
        (s_drain_done - s_exec_sent) - m_conf.phase_stats->firstbatch_us;
    m_conf.phase_stats->rows_drained = row_count;
  }
#endif
  if (rc == NdbQuery::NextResult_error) {
    const NdbError& err = query->getNdbError();
    std::basic_ostream<char>& errout = *m_conf.err_stream;
    errout << "Pass-through query failed: " << err.message
           << " (code " << err.code << ")" << std::endl;
    throw RonSQLRetryableError("Pass-through drain failed.");
  }

  if (sorting) {
    // Phase 3: sort the buffered rows and print the first
    // min(limit, n).  The deferred TSV header is emitted here on the
    // first printed row.
    sort_and_print_passthrough_rows(sort_buf, sort_keys, n_sort_keys,
                                    num_cols, limit, m_amalloc,
                                    m_resultprinter, header_emitted,
                                    m_conf.out_stream);
  }

  // Only finish if we actually opened a frame (JSON always; TSV
  // never needs a finish since print_passthrough_finish is a no-op
  // for TSV).
  if (header_emitted) {
    m_resultprinter->print_passthrough_finish(m_conf.out_stream);
  }
}

// Shared emit for a scan-config-selected index-scan root (Phase I.9
// CTE bodies + join_root_index_scan_plan.md main-query roots).
//
// Builds the NdbQueryIndexBound key arrays from the scope's flattened
// conjuncts (`body_toplevel_conditions`) and the chosen candidate's
// `condition_handling_map`.  Two independent pointer chains terminated
// by nullptr — one for low bounds, one for high.
// select_root_scan_config guarantees consecutive leading-column
// coverage starting at column 0 (no gaps): equality-bound columns sit
// at the head of both chains; an optional half-open range can extend
// exactly one further column on either side.  lowIncl / highIncl
// reflect the inclusivity of the LAST entry on each chain
// (NdbQueryIndexBound uses one bool per side, applied uniformly).
// Residual conjuncts (map == -1) go through an InterpretedCode filter
// on rootOpts.
const NdbQueryOperationDef*
RonSQLPreparer::emit_index_scan_root(NdbQueryBuilder* qb,
                                     QueryScope& scope,
                                     const NdbDictionary::Table* tab,
                                     const NdbDictionary::Index* idx,
                                     NdbQueryOptions& rootOpts)
{
  require_run(scope.body_scan_config != NULL &&
              scope.body_scan_config->index == idx,
              "Index-scan root missing scan-config metadata.");

  Uint32 idx_col_count = idx->getNoOfColumns();
  const NdbQueryOperand* lowKeys[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
  const NdbQueryOperand* highKeys[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
  Uint32 lowFill = 0, highFill = 0;
  bool lowIncl = true, highIncl = true;
  for (Uint32 k = 0; k < idx_col_count; k++) {
    const NdbDictionary::Column* idx_col = idx->getColumn(k);
    ndbrequire(idx_col != NULL);
    const NdbDictionary::Column* tab_col =
        tab->getColumn(idx_col->getName());
    require_run(tab_col != NULL,
                "Index-scan root: index column missing on table.");

    const NdbQueryOperand* low_op = NULL;
    const NdbQueryOperand* high_op = NULL;
    bool k_low_incl = true, k_high_incl = true;
    for (Uint32 ci = 0; ci < scope.body_toplevel_conditions.size();
         ci++) {
      if ((Uint32)scope.body_scan_config->condition_handling_map[ci]
          != k) continue;
      ConditionalExpression* ce = scope.body_toplevel_conditions[ci];
      ConditionalExpression* right_const = ce->args.right;
      raw_value rv = encode_constant(right_const, tab_col);
      const NdbQueryOperand* operand = qb->constValue(rv.val, rv.len);
      require_run(operand != NULL,
                  "Failed to create const value for index-scan root bound.");
      TokenKind op = ce->op;
      if (op == T_EQUALS || op == T_GE || op == T_GT) {
        low_op = operand;
        if (op == T_GT) k_low_incl = false;
      }
      if (op == T_EQUALS || op == T_LE || op == T_LT) {
        high_op = operand;
        if (op == T_LT) k_high_incl = false;
      }
    }
    if (low_op == NULL && high_op == NULL) break;
    if (low_op != NULL) {
      lowKeys[lowFill++] = low_op;
      lowIncl = k_low_incl;
    }
    if (high_op != NULL) {
      highKeys[highFill++] = high_op;
      highIncl = k_high_incl;
    }
    // A half-open last column truncates further coverage on
    // both sides — select_root_scan_config already
    // enforces this via later_columns_blocked, but make the
    // emit-side invariant explicit too.
    if (low_op == NULL || high_op == NULL) break;
  }
  lowKeys[lowFill] = nullptr;
  highKeys[highFill] = nullptr;

  // Residual conjuncts (cmh[i] == -1) go through the
  // InterpretedCode filter, mirroring the TABLE_SCAN branch.
  NdbInterpretedCode rootCode(tab);
  bool has_residual = false;
  ConditionalExpression* residual_root = NULL;
  for (Uint32 ci = 0; ci < scope.body_toplevel_conditions.size(); ci++) {
    if (scope.body_scan_config->condition_handling_map[ci] != -1)
      continue;
    ConditionalExpression* ce = scope.body_toplevel_conditions[ci];
    if (residual_root == NULL) {
      residual_root = ce;
    } else {
      ConditionalExpression* combined =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
      combined->op = T_AND;
      combined->args.left = residual_root;
      combined->args.right = ce;
      residual_root = combined;
    }
    has_residual = true;
  }
  if (has_residual) {
    NdbScanFilter filter(&rootCode);
    filter.setSqlCmpSemantics();
    filter.begin(NdbScanFilter::AND);
    apply_filter(&filter, scope, residual_root);
    filter.end();
    rootCode.finalise();
    rootOpts.setInterpretedCode(rootCode);
  }

  NdbQueryIndexBound bound(lowKeys, lowIncl, highKeys, highIncl);
  const NdbQueryOperationDef* def = qb->scanIndex(idx, tab, &bound, &rootOpts);
  require_run(def != NULL, "Failed to create index-scan root.");
  return def;
}

const NdbQueryOperationDef*
RonSQLPreparer::emit_pk_equality_index_scan_root(
    NdbQueryBuilder* qb, QueryScope& scope,
    const NdbDictionary::Table* root_table,
    ConditionalExpression* const pk_const[], int nkeys,
    ConditionalExpression* residual,
    NdbQueryOptions& rootOpts)
{
  const char* pk_col_names[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
  for (int k = 0; k < nkeys; k++)
    pk_col_names[k] = root_table->getPrimaryKey(k);
  const NdbDictionary::Index* pk_ordered_idx =
      QueryPlanner::findOrderedIndex(
          m_dict, root_table, pk_col_names, (Uint32)nkeys);
  if (pk_ordered_idx == NULL)
    return NULL;
  const NdbQueryOperand* pk_keys[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
  for (int k = 0; k < nkeys; k++)
  {
    const NdbDictionary::Column* pk_col =
        root_table->getColumn(pk_col_names[k]);
    ndbrequire(pk_col != NULL);
    raw_value rv = encode_constant(pk_const[k], pk_col);
    pk_keys[k] = qb->constValue(rv.val, rv.len);
    require_run(pk_keys[k] != NULL,
                "Failed to create const value for PK index scan.");
  }
  pk_keys[nkeys] = nullptr;
  if (residual != NULL)
  {
    // Phase 0a: residual conjuncts (not consumed as equality bounds) go
    // through an InterpretedCode filter, mirroring emit_index_scan_root.
    NdbInterpretedCode code(root_table);
    NdbScanFilter filter(&code);
    filter.setSqlCmpSemantics();
    filter.begin(NdbScanFilter::AND);
    apply_filter(&filter, scope, residual);
    filter.end();
    code.finalise();
    require_run(rootOpts.setInterpretedCode(code) == 0,
                "Failed to set interpreted code on root PK index scan.");
  }
  NdbQueryIndexBound bound(pk_keys);
  const NdbQueryOperationDef* def =
      qb->scanIndex(pk_ordered_idx, root_table, &bound, &rootOpts);
  require_run(def != NULL, "Failed to create root PK index scan.");
  return def;
}

// Emit the root scan/lookup/index-scan for the scope's plan. Chooses PK
// lookup when WHERE fully covers the PK and no child is a scan; ordered
// index scan with equality bounds when PK-covered with a scan child;
// scan-config-selected index scan with WHERE bounds when
// select_root_scan_config flipped ops[0] to INDEX_SCAN;
// table scan with WHERE filter otherwise.
void
RonSQLPreparer::emit_root_op(NdbQueryBuilder* qb, QueryScope& scope,
                              const NdbQueryOperationDef** opDefs,
                              NdbAggregator* singleAgg,
                              NdbDictionary::Table** cteVirtualTables)
{
  JoinPlan& plan = scope.join_plan;
  NdbQueryOptions rootOpts;

  // Statement-level FRAGS_PER_WORKER hint: bundle N root fragments per
  // SPJ worker for aggregate pushed queries.  Applied to the main-query
  // root options only (rootOpts is shared with emit_index_scan_root);
  // CTE-body emission is deliberately untouched — the value is
  // query-global, carried in SCAN_TABREQ.  The NDB API normalizes
  // (power of two, cap 8), clamps to the per-node fragment count, and
  // ignores it for non-aggregate queries, ordered/pruned scans and
  // scanCte-containing queries (see frags_per_worker_plan.md).
  if (m_context.ast_root.frags_per_worker > 0)
  {
    rootOpts.setFragsPerWorker((Uint32)m_context.ast_root.frags_per_worker);
  }

  // Dispatch CTE_SCAN root before the real-table logic. Reuses the
  // CTE_LOOKUP filter helper (same opcode family — verified by
  // testCteNdbApiFilter::testCteScanFilterRoot) and attaches the main
  // aggregator on the root when the agg leaf IS the root.
  if (plan.ops[0].type == JoinOp::CTE_SCAN)
  {
    require_run(cteVirtualTables != NULL && cteVirtualTables[0] != NULL,
                "CTE_SCAN root requires a virtual table.");
    Uint32 numResultCols = 0;
    for (const Outputs* o = plan.ops[0].cte_def->stmt->outputs;
         o != NULL; o = o->next) numResultCols++;

    // Phase I.7: when the WHERE clause supplies equality predicates
    // on every virt-table PK column (i.e. every GROUP BY column of
    // the CTE body), convert the scan to a single-row lookupCte
    // root.  Mirrors the real-table readTuple optimisation just
    // below.  testCteNdbApi.cpp Test 11 demonstrates the working
    // NDB API setup: `qb->lookupCte(cteId, numCols, virtTab,
    // const_keys, opts)` with no parent.
    bool root_pk_covered = false;
    int root_nkeys = 0;
    ConditionalExpression* root_pk_const[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
    ConditionalExpression* root_pk_eq[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
    ConditionalExpression* root_residual = NULL;
    bool root_has_scan_child = false;
    // Single-row CTEs (cte_single_row_kernel_plan.md) also have no
    // GROUP BY but are NOT scalar: as a FROM root they take the
    // scanCte path below (WHERE as a jump-table filter).  The scalar
    // dummy-key lookupCte arm and the full-virt-PK lookupCte arm are
    // both wrong for them (no positions declared; the kernel's
    // single-row probe rejects position-less keys).
    bool root_cte_is_single_row =
        (plan.ops[0].cte_def != NULL &&
         plan.ops[0].cte_def->stmt->is_single_row_cte);
    bool root_cte_is_scalar =
        (plan.ops[0].cte_def != NULL &&
         plan.ops[0].cte_def->stmt->groupby_columns == NULL &&
         !root_cte_is_single_row);
    if (root_cte_is_scalar && scope.join_where_ce[0] != NULL)
    {
      NdbInterpretedCode code(cteVirtualTables[0]);
      emit_cte_lookup_filter(code, scope, /*op_idx=*/0,
                             cteVirtualTables[0],
                             scope.join_where_ce[0]);
      require_run(rootOpts.setInterpretedCode(code) == 0,
                  "Failed to set interpreted code on scalar CTE root.");
      if (singleAgg != NULL && plan.agg_leaf_idx == 0 &&
          plan.num_agg_leaves == 0 && scope.agg != NULL)
      {
        require_run(rootOpts.setAggregation(*singleAgg) == 0,
                    "Failed to set aggregation on scalar CTE root.");
      }
      // D8: dummy key must match the scalar virt PK column type (its first
      // output column), else lookupCte rejects it for a non-Bigint PK (e.g.
      // a Double from a scale>0 DECIMAL / FLOAT / DOUBLE aggregate).  Value
      // is ignored by the kernel for scalar CTEs.
      const NdbDictionary::Column* root_pkc =
          cteVirtualTables[0]->getColumn(0);
      require_run(root_pkc != NULL,
                  "Scalar CTE root virtual table has no primary-key column.");
      const NdbQueryOperand* scalar_keys[2];
      switch (root_pkc->getType()) {
      case NdbDictionary::Column::Bigint:
        scalar_keys[0] = qb->constValue((Int64)0);
        break;
      case NdbDictionary::Column::Bigunsigned:
        scalar_keys[0] = qb->constValue((Uint64)0);
        break;
      case NdbDictionary::Column::Double:
        scalar_keys[0] = qb->constValue((double)0);
        break;
      default: {
        const Uint32 rpksz = root_pkc->getSizeInBytes();
        Uint8* rzero = m_amalloc->alloc_exc<Uint8>(rpksz == 0 ? 1 : rpksz);
        memset(rzero, 0, rpksz);
        scalar_keys[0] = qb->constValue(static_cast<const void*>(rzero), rpksz);
        break;
      }
      }
      require_run(scalar_keys[0] != NULL,
                  "Failed to create dummy scalar CTE root lookup key.");
      scalar_keys[1] = nullptr;
      opDefs[0] = qb->lookupCte(plan.ops[0].cte_def_idx, numResultCols,
                                cteVirtualTables[0], scalar_keys,
                                &rootOpts);
      require_run(opDefs[0] != NULL,
                  "Failed to create scalar lookupCte root.");
      return;
    }
    if (scope.join_where_ce[0] != NULL && !root_cte_is_scalar &&
        !root_cte_is_single_row)
    {
      ConditionalExpression* w = simplify_ce(scope.join_where_ce[0], -1);
      root_nkeys = cteVirtualTables[0]->getNoOfPrimaryKeys();
      for (int k = 0; k < root_nkeys; k++)
      {
        root_pk_const[k] = NULL;
        root_pk_eq[k] = NULL;
      }
      if (root_nkeys > 0)
      {
        collect_pk_equalities(w, cteVirtualTables[0], root_pk_const,
                              root_pk_eq);
        root_pk_covered = true;
        for (int k = 0; k < root_nkeys; k++)
        {
          if (root_pk_const[k] == NULL) { root_pk_covered = false; break; }
        }
      }
      // Phase 0a: conjuncts not consumed as virt-PK equalities must be
      // applied as a CTE_LOOKUP filter — they were silently dropped
      // before.  On flatten overflow fall back to scanCte below, which
      // applies the whole WHERE.
      if (root_pk_covered &&
          !build_root_residual(w, root_pk_eq, root_nkeys, &root_residual))
      {
        root_pk_covered = false;
      }
      if (root_pk_covered)
      {
        for (Uint32 ci = 1; ci < plan.num_ops; ci++)
        {
          if (plan.ops[ci].type == JoinOp::INDEX_SCAN ||
              plan.ops[ci].type == JoinOp::TABLE_SCAN)
          {
            root_has_scan_child = true;
            break;
          }
        }
      }
    }

    if (root_pk_covered && !root_has_scan_child)
    {
      const NdbQueryOperand* lookup_keys[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
      for (int k = 0; k < root_nkeys; k++)
      {
        const char* pk_name = cteVirtualTables[0]->getPrimaryKey(k);
        const NdbDictionary::Column* pk_col =
            cteVirtualTables[0]->getColumn(pk_name);
        ndbrequire(pk_col != NULL);
        const ConditionalExpression* ce_const = root_pk_const[k];
        // Synthetic virt-table columns have m_attrSize == 0
        // (memory: project_synthetic_virt_table_gotcha.md), so
        // the (void*, len) constValue overload fails column
        // validation at lookupCte().  Dispatch to the typed
        // constValue overloads instead — they create operands
        // independent of column metadata.
        switch (pk_col->getType()) {
        case NdbDictionary::Column::Tinyint:
        case NdbDictionary::Column::Smallint:
        case NdbDictionary::Column::Mediumint:
        case NdbDictionary::Column::Int:
          require_prm(ce_const->op == T_INT,
                      "Expected integer constant for CTE root lookup key.");
          lookup_keys[k] = qb->constValue((Int32) ce_const->constant_integer);
          break;
        case NdbDictionary::Column::Bigint:
          require_prm(ce_const->op == T_INT,
                      "Expected integer constant for CTE root lookup key.");
          lookup_keys[k] = qb->constValue((Int64) ce_const->constant_integer);
          break;
        case NdbDictionary::Column::Tinyunsigned:
        case NdbDictionary::Column::Smallunsigned:
        case NdbDictionary::Column::Mediumunsigned:
        case NdbDictionary::Column::Unsigned:
          require_prm(ce_const->op == T_INT,
                      "Expected integer constant for CTE root lookup key.");
          lookup_keys[k] = qb->constValue((Uint32) ce_const->constant_integer);
          break;
        case NdbDictionary::Column::Bigunsigned:
          require_prm(ce_const->op == T_INT,
                      "Expected integer constant for CTE root lookup key.");
          lookup_keys[k] = qb->constValue((Uint64) ce_const->constant_integer);
          break;
        case NdbDictionary::Column::Float:
        case NdbDictionary::Column::Double:
          if (ce_const->op == T_FLOAT) {
            lookup_keys[k] = qb->constValue(ce_const->constant_float.dbl);
          } else if (ce_const->op == T_INT) {
            lookup_keys[k] =
                qb->constValue((double) ce_const->constant_integer);
          } else {
            throw RonSQLPermanentError(
                "Expected numeric constant for CTE root lookup key.");
          }
          break;
        default:
          // CHAR / VARCHAR / DECIMAL etc.: fall through to the
          // byte-buffer variant.  May still fail validation on
          // synthetic virt-table columns; users hitting this can
          // fall back to scanCte by removing the equality from
          // WHERE or expressing the constant as a different type.
          {
            raw_value rv = encode_constant(
                const_cast<ConditionalExpression*>(ce_const), pk_col);
            lookup_keys[k] = qb->constValue(rv.val, rv.len);
          }
          break;
        }
        require_run(lookup_keys[k] != NULL,
                    "Failed to create const value for CTE root lookup.");
      }
      lookup_keys[root_nkeys] = nullptr;
      if (root_residual != NULL)
      {
        // Phase 0a: residual conjuncts ride the same CTE_LOOKUP
        // jump-table filter the scalar and scanCte branches already
        // use; emit_cte_lookup_filter finalises the program and throws
        // a clean error on unsupported atoms (which were silently
        // dropped before).
        NdbInterpretedCode residual_code(cteVirtualTables[0]);
        emit_cte_lookup_filter(residual_code, scope, /*op_idx=*/0,
                               cteVirtualTables[0], root_residual);
        require_run(rootOpts.setInterpretedCode(residual_code) == 0,
                    "Failed to set interpreted code on CTE_LOOKUP root.");
      }
      if (singleAgg != NULL && plan.agg_leaf_idx == 0 &&
          plan.num_agg_leaves == 0 && scope.agg != NULL)
      {
        require_run(rootOpts.setAggregation(*singleAgg) == 0,
                    "Failed to set aggregation on CTE_LOOKUP root.");
      }
      opDefs[0] = qb->lookupCte(plan.ops[0].cte_def_idx, numResultCols,
                                 cteVirtualTables[0], lookup_keys,
                                 &rootOpts);
      require_run(opDefs[0] != NULL,
                  "Failed to create lookupCte root.");
      return;
    }

    NdbInterpretedCode code(cteVirtualTables[0]);
    if (scope.join_where_ce[0] != NULL)
    {
      // emit_cte_lookup_filter already finalises the program
      // (line ~5406 in this file).  Don't double-finalise.
      emit_cte_lookup_filter(code, scope, /*op_idx=*/0,
                             cteVirtualTables[0],
                             scope.join_where_ce[0]);
      require_run(rootOpts.setInterpretedCode(code) == 0,
                  "Failed to set interpreted code on CTE_SCAN root.");
    }
    // scope.agg is the per-scope aggregator (main scope OR per-CTE
    // body scope), so this gate fires correctly whether emit_root_op
    // is called for the main query (Phase E.1) or for a chained CTE
    // body (Phase E.2).
    if (singleAgg != NULL && plan.agg_leaf_idx == 0 &&
        plan.num_agg_leaves == 0 && scope.agg != NULL)
    {
      require_run(rootOpts.setAggregation(*singleAgg) == 0,
                  "Failed to set aggregation on CTE_SCAN root.");
    }
    opDefs[0] = qb->scanCte(plan.ops[0].cte_def_idx, numResultCols,
                            cteVirtualTables[0], &rootOpts);
    require_run(opDefs[0] != NULL, "Failed to create scanCte root.");
    return;
  }

  const NdbDictionary::Table* root_table = plan.ops[0].table;

  // Scan-config-selected index-scan root (join_root_index_scan_plan.md):
  // select_root_scan_config flipped ops[0] to INDEX_SCAN because the
  // root WHERE conjuncts bind an ordered index — range or
  // secondary-index predicates the PK-equality branches below cannot
  // serve.  Those branches keep priority by construction: the selector
  // skips fully-PK-equality-covered roots unless FORCE INDEX overrides.
  // Bounds come from condition_handling_map; residual conjuncts go to
  // the interpreted filter inside the helper, so join_where_ce[0] must
  // NOT also be applied wholesale (no scanTable fallthrough).
  if (plan.ops[0].type == JoinOp::INDEX_SCAN &&
      scope.body_scan_config != NULL &&
      scope.body_scan_config->index == plan.ops[0].index)
  {
    opDefs[0] = emit_index_scan_root(qb, scope, root_table,
                                     plan.ops[0].index, rootOpts);
    return;
  }

  ConditionalExpression* where_ce = NULL;
  bool pk_covered = false;
  bool has_scan_child = false;
  int nkeys = 0;
  ConditionalExpression* pk_const[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
  ConditionalExpression* pk_eq_ce[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
  ConditionalExpression* root_residual = NULL;

  if (scope.join_where_ce[0] != NULL)
  {
    where_ce = simplify_ce(scope.join_where_ce[0], -1);

    nkeys = root_table->getNoOfPrimaryKeys();
    for (int k = 0; k < nkeys; k++)
    {
      pk_const[k] = NULL;
      pk_eq_ce[k] = NULL;
    }
    collect_pk_equalities(where_ce, root_table, pk_const, pk_eq_ce);
    pk_covered = true;
    for (int k = 0; k < nkeys; k++)
    {
      if (pk_const[k] == NULL) { pk_covered = false; break; }
    }

    // Phase 0a: conjuncts not consumed as PK equalities must be applied
    // as an interpreted filter — they were silently dropped before.  On
    // flatten overflow skip the PK-equality optimization; the table-scan
    // fallback applies the whole WHERE.
    if (pk_covered &&
        !build_root_residual(where_ce, pk_eq_ce, nkeys, &root_residual))
    {
      pk_covered = false;
    }

    if (pk_covered)
    {
      for (Uint32 ci = 1; ci < plan.num_ops; ci++)
      {
        if (plan.ops[ci].type == JoinOp::INDEX_SCAN ||
            plan.ops[ci].type == JoinOp::TABLE_SCAN)
        {
          has_scan_child = true;
          break;
        }
      }
    }
  }

  // A readTuple root makes this a lookup-rooted (TCKEYREQ) query when
  // no CTE subtree precedes it, and DBTC's JOIN_AGG_SETUP only exists
  // on the scan path — an aggregate LookupQuery reaches DBSPJ with an
  // empty m_aggNodes and fails lookup_send's ndbrequire (node failure;
  // also rejected at prepare time by NdbQueryDefImpl since Phase 0a).
  // Emit the readTuple root only when nothing aggregates here
  // (pass-through main query), or when this is the main scope of a
  // CTE-containing query — the CTE materialisation scan makes the
  // compound query scan-rooted (the proven fpw-6 shape).  CTE-body
  // scopes always aggregate and may themselves be op[0], so they
  // always take the scan fallbacks.
  bool lookup_root_supported = (singleAgg == NULL && scope.agg == NULL);
  if (!lookup_root_supported && &scope == &m_main_scope)
  {
    for (Uint32 ci = 0; ci < plan.num_ops; ci++)
    {
      if (plan.ops[ci].type == JoinOp::CTE_LOOKUP ||
          plan.ops[ci].type == JoinOp::CTE_SCAN)
      {
        lookup_root_supported = true;
        break;
      }
    }
  }

  if (pk_covered && !has_scan_child && lookup_root_supported)
  {
    // An over-cap residual falls through to the ordered-PK-index scan /
    // table-scan branches below (scan programs travel in SCAN_FRAGREQ
    // sections instead of LQHKEYREQ; cap rationale at the shared
    // LOOKUP_FILTER_MAX_WORDS definition).
    NdbInterpretedCode residual_code(root_table);
    bool residual_fits = true;
    if (root_residual != NULL)
    {
      NdbScanFilter filter(&residual_code);
      filter.setSqlCmpSemantics();
      filter.begin(NdbScanFilter::AND);
      apply_filter(&filter, scope, root_residual);
      filter.end();
      residual_code.finalise();
      residual_fits =
          (residual_code.getWordsUsed() <= LOOKUP_FILTER_MAX_WORDS);
    }
    if (residual_fits)
    {
      const NdbQueryOperand* pk_keys[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY + 1];
      for (int k = 0; k < nkeys; k++)
      {
        const char* pk_name = root_table->getPrimaryKey(k);
        const NdbDictionary::Column* pk_col =
            root_table->getColumn(pk_name);
        ndbrequire(pk_col != NULL);
        raw_value rv = encode_constant(pk_const[k], pk_col);
        pk_keys[k] = qb->constValue(rv.val, rv.len);
        require_run(pk_keys[k] != NULL,
                    "Failed to create const value for PK lookup.");
      }
      pk_keys[nkeys] = nullptr;
      if (root_residual != NULL)
      {
        require_run(rootOpts.setInterpretedCode(residual_code) == 0,
                    "Failed to set interpreted code on root PK lookup.");
      }
      opDefs[0] = qb->readTuple(root_table, pk_keys, &rootOpts);
      require_run(opDefs[0] != NULL, "Failed to create root PK lookup.");
      return;
    }
  }

  if (pk_covered)
  {
    // Reached with a scan child (a readTuple root cannot have scan
    // children — NDB API restriction) or with a residual filter too
    // large for a lookup op.
    const NdbQueryOperationDef* def =
        emit_pk_equality_index_scan_root(qb, scope, root_table,
                                         pk_const, nkeys, root_residual,
                                         rootOpts);
    if (def != NULL)
    {
      opDefs[0] = def;
      return;
    }
    // No ordered index on PK — fall through to table scan with filter
  }

  NdbInterpretedCode code(root_table);
  if (where_ce != NULL)
  {
    NdbScanFilter filter(&code);
    filter.setSqlCmpSemantics();
    filter.begin(NdbScanFilter::AND);
    apply_filter(&filter, scope, where_ce);
    filter.end();
    code.finalise();
    rootOpts.setInterpretedCode(code);
  }
  opDefs[0] = qb->scanTable(root_table, &rootOpts);
  require_run(opDefs[0] != NULL, "Failed to create root scan.");
}

// Recursively resolve the (Type, length, charset) tuple for a column
// reference, walking through chained CTE output descriptors. Mirrors the
// aggregate widening rules in build_cte_virtual_tables so chained CTE layers
// (e.g. b's MAX(a.s) where a.s = SUM(real.col)) yield consistent types.
bool
RonSQLPreparer::resolve_chained_column_type(
    QueryScope& scope, Uint32 col_idx,
    NdbDictionary::Column::Type& out_type,
    Uint32& out_length,
    const void*& out_cs,
    Int32& out_scale,
    Int32& out_precision)
{
  out_scale = 0;
  out_precision = 0;
  if (scope.resolved_columns == NULL)
    return false;
  const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[col_idx];
  if (ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn) {
    const NdbDictionary::Column* src = ref.dict_column;
    if (src == NULL) return false;
    out_type = src->getType();
    out_length = src->getLength();
    out_cs = src->getCharset();
    out_scale = src->getScale();
    out_precision = src->getPrecision();
    return true;
  }
  if (ref.kind != QueryScope::ResolvedColumnRef::Kind::CteResultColumn)
    return false;
  if (ref.cte_def_idx >= m_cte_scopes.size()) return false;
  QueryScope* cs = m_cte_scopes[ref.cte_def_idx];
  if (cs == NULL) return false;

  const Outputs* o = ref.cte_output;
  if (o == NULL) return false;

  if (o->type == Outputs::Type::COLUMN) {
    return resolve_chained_column_type(*cs, o->column.col_idx,
                                        out_type, out_length, out_cs,
                                        out_scale, out_precision);
  }
  if (o->type == Outputs::Type::AGGREGATE) {
    TokenKind fun = o->aggregate.fun;
    if (fun == T_COUNT) {
      out_type = NdbDictionary::Column::Bigunsigned;
      out_length = 1;
      out_cs = NULL;
      out_scale = 0;
      out_precision = 0;
      return true;
    }
    AggregationAPICompiler::Expr* arg = o->aggregate.arg;
    if (arg == NULL || !arg->isLoad()) return false;
    NdbDictionary::Column::Type arg_type;
    Uint32 arg_length;
    const void* arg_cs;
    Int32 arg_scale = 0;
    Int32 arg_precision = 0;
    if (!resolve_chained_column_type(*cs, arg->getLoadIdx(),
                                      arg_type, arg_length, arg_cs,
                                      arg_scale, arg_precision))
      return false;
    if (fun == T_SUM) {
      switch (arg_type) {
        case NdbDictionary::Column::Tinyint:
        case NdbDictionary::Column::Smallint:
        case NdbDictionary::Column::Mediumint:
        case NdbDictionary::Column::Int:
        case NdbDictionary::Column::Bigint:
          out_type = NdbDictionary::Column::Bigint;
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        case NdbDictionary::Column::Tinyunsigned:
        case NdbDictionary::Column::Smallunsigned:
        case NdbDictionary::Column::Mediumunsigned:
        case NdbDictionary::Column::Unsigned:
        case NdbDictionary::Column::Bigunsigned:
          out_type = NdbDictionary::Column::Bigunsigned;
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        case NdbDictionary::Column::Float:
        case NdbDictionary::Column::Double:
          out_type = NdbDictionary::Column::Double;
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        case NdbDictionary::Column::Decimal:
        case NdbDictionary::Column::Decimalunsigned:
          // D1: mirror the MIN/MAX DECIMAL widening below (kernel widens
          // DECIMAL -> BIGINT (scale==0) / DOUBLE (scale>0) on load).
          require_prm(
              decimal_minmax_fits_64bit(arg_type, arg_precision, arg_scale),
              "SUM over scale-zero DECIMAL wider than the 64-bit integer "
              "range is not yet supported.");
          if (arg_scale == 0) {
            out_type = (arg_type == NdbDictionary::Column::Decimalunsigned)
                       ? NdbDictionary::Column::Bigunsigned
                       : NdbDictionary::Column::Bigint;
          } else {
            out_type = NdbDictionary::Column::Double;
          }
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        default:
          return false;
      }
    }
    if (fun == T_MIN || fun == T_MAX) {
      // Same widening as build_cte_virtual_tables MIN/MAX branch.
      switch (arg_type) {
        case NdbDictionary::Column::Tinyint:
        case NdbDictionary::Column::Smallint:
        case NdbDictionary::Column::Mediumint:
        case NdbDictionary::Column::Int:
        case NdbDictionary::Column::Bigint:
          out_type = NdbDictionary::Column::Bigint;
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        case NdbDictionary::Column::Tinyunsigned:
        case NdbDictionary::Column::Smallunsigned:
        case NdbDictionary::Column::Mediumunsigned:
        case NdbDictionary::Column::Unsigned:
        case NdbDictionary::Column::Bigunsigned:
          out_type = NdbDictionary::Column::Bigunsigned;
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        case NdbDictionary::Column::Float:
        case NdbDictionary::Column::Double:
          out_type = NdbDictionary::Column::Double;
          out_length = 1; out_cs = NULL;
          out_scale = 0; out_precision = 0; return true;
        case NdbDictionary::Column::Decimal:
        case NdbDictionary::Column::Decimalunsigned:
          // Phase I.6 F.1: kernel `AggInterpreter::AlignedType`
          // widens DECIMAL → BIGINT (scale==0) or DOUBLE (scale>0)
          // before MIN/MAX runs.  Mirror the same widening here so
          // RonSQL's virt-table column type matches what the kernel
          // emits, allowing the inline-type CTE filter opcode to
          // accept the result.
          require_prm(
              decimal_minmax_fits_64bit(arg_type, arg_precision, arg_scale),
              "MIN/MAX over scale-zero DECIMAL wider than the 64-bit "
              "integer range is not yet supported.  Full DECIMAL "
              "precision preservation requires a wider aggregate-result "
              "representation.");
          if (arg_scale == 0) {
            out_type = (arg_type == NdbDictionary::Column::Decimalunsigned)
                       ? NdbDictionary::Column::Bigunsigned
                       : NdbDictionary::Column::Bigint;
          } else {
            out_type = NdbDictionary::Column::Double;
          }
          out_length = 1;
          out_cs = NULL;
          out_scale = 0;
          out_precision = 0;
          return true;
        case NdbDictionary::Column::Char:
        case NdbDictionary::Column::Varchar:
        case NdbDictionary::Column::Longvarchar:
          // Phase I.6 F.2: kernel-side MIN/MAX over CHAR / VARCHAR /
          // Longvarchar is wired (per-(group, slot) val_ptr +
          // AGG_CHAR_RESULT wire marker).  Mirror the
          // build_cte_virtual_tables decision here so chained CTE
          // layers see the same type, length, and charset.
          out_type = arg_type;
          out_length = arg_length;
          out_cs = arg_cs;
          out_scale = arg_scale;
          out_precision = arg_precision;
          return true;
        case NdbDictionary::Column::Date:
        case NdbDictionary::Column::Year:
        case NdbDictionary::Column::Datetime2:
        case NdbDictionary::Column::Time2:
        case NdbDictionary::Column::Timestamp2:
          // D17 + temporal: MIN/MAX over a temporal column widens to
          // Bigunsigned (8-byte packed value) on the wire — mirror
          // build_cte_virtual_tables so chained CTE layers see the same
          // type.  (Temporal display is only recovered at the top level for
          // a single-CTE MIN/MAX today.)
          out_type = NdbDictionary::Column::Bigunsigned;
          out_length = 1;
          out_cs = NULL;
          out_scale = 0;
          out_precision = 0;
          return true;
        default:
          // Other non-numeric source types: best-effort passthrough
          // (see build_cte_virtual_tables for the same caveat).
          out_type = arg_type;
          out_length = arg_length;
          out_cs = arg_cs;
          out_scale = arg_scale;
          out_precision = arg_precision;
          return true;
      }
    }
  }
  return false;
}

// Allocate per-CTE virtual NdbDictionary::Table objects. Each virt table's
// columns mirror the referenced CTE's output list: GROUP BY columns as PK
// (same names/types as their source columns, supporting linked-key binding
// via the parent's join operand), aggregate columns as non-PK (types
// derived from the aggregate function + source column). Column order
// matches cte->stmt->outputs so attrIds align with cte_col_idx from
// load_join's name→attrId resolution. out[] is indexed by op-index of the
// referencing CTE_LOOKUP or CTE_SCAN; non-CTE ops leave NULL.
void
RonSQLPreparer::build_cte_virtual_tables(const JoinPlan& plan,
                                          NdbDictionary::Table** out)
{
  for (Uint32 i = 0; i < plan.num_ops; i++) {
    const JoinOp& op = plan.ops[i];
    if (op.type != JoinOp::CTE_LOOKUP && op.type != JoinOp::CTE_SCAN)
      continue;

    const CteDefinition* cte = op.cte_def;
    ndbrequire(cte != NULL);
    ndbrequire(op.cte_def_idx < m_cte_scopes.size());
    QueryScope* cte_scope = m_cte_scopes[op.cte_def_idx];
    ndbrequire(cte_scope != NULL);

    char vtname[64];
    snprintf(vtname, sizeof(vtname), "__cte_%u", op.cte_def_idx);
    NdbDictionary::Table* vt = new NdbDictionary::Table(vtname);

    for (const Outputs* o = cte->stmt->outputs; o != NULL; o = o->next) {
      NdbDictionary::Column vcol;
      const char* vcol_name =
          o->output_name.to_LexCString(m_amalloc).c_str();
      vcol.setName(vcol_name);

      // GROUP BY columns become PK of the virt table and keep their
      // source type; everything else is non-PK.
      //
      // Phase I.17: scalar CTEs (no GROUP BY) have no natural key,
      // but lookupCte() requires a non-zero PK count to be a valid
      // join child — see testCteNdbApi.cpp Test 20's
      // `cte_virtual_scalar (result BIGINT PRIMARY KEY)` shape.
      // For a scalar CTE we mark the FIRST output column as PK so
      // the virt table has structural PK count == 1, matching
      // Test 20's single-PK / single-dummy-key contract.  The
      // kernel ignores the key at scalar lookup (n_gb_cols == 0 →
      // returns m_agg_results directly).
      // Single-row CTEs (cte_single_row_kernel_plan.md) also have
      // groupby_columns == NULL but are NOT scalar: kernel-side EVERY
      // output is a GROUP BY (key) column of the key-only group
      // record.  Mark them all PK (subset-key lookupCte skips the
      // PK-count validation via setCteKeyColumns) and all nullable
      // (source columns may be NULL, and LEFT-join NULL-fill writes
      // NULLs regardless).
      bool cte_is_single_row = cte->stmt->is_single_row_cte;
      bool cte_is_scalar =
          (cte->stmt->groupby_columns == NULL) && !cte_is_single_row;
      bool is_groupby = false;
      if (cte_is_single_row) {
        is_groupby = true;
      } else if (cte_is_scalar) {
        is_groupby = (o == cte->stmt->outputs);  // first output only
      } else if (o->type == Outputs::Type::COLUMN) {
        for (const GroupbyColumns* gb = cte->stmt->groupby_columns;
             gb != NULL; gb = gb->next) {
          if (gb->col_idx == o->column.col_idx) { is_groupby = true; break; }
        }
      }

      // Derive type. Simple cases only: plain column refs, COUNT(*),
      // and SUM/MIN/MAX over a direct column load (possibly chained
      // through a predecessor CTE — resolve_chained_column_type walks
      // the chain).  More complex expressions raise a clear error.
      NdbDictionary::Column::Type derived_type =
          NdbDictionary::Column::Bigint;  // fallback for COUNT
      Uint32 derived_length = 1;
      const void* derived_cs = NULL;
      // D15: display scale + precision carried onto the virt column so the
      // result printer formats DECIMAL-derived MIN/MAX (widened to DOUBLE) with
      // the source scale, matching MySQL (e.g. 20055.00) — but only when the
      // DECIMAL is within DOUBLE's exact range (precision <= 15); the printer
      // gates on precision.  0 for non-scaled outputs.
      Int32 derived_scale = 0;
      Int32 derived_precision = 0;
      bool have_derived = false;

      if (o->type == Outputs::Type::COLUMN) {
        Uint32 col_idx = o->column.col_idx;
        NdbDictionary::Column::Type rt;
        Uint32 rlen = 1;
        const void* rcs = NULL;
        Int32 rscale = 0;
        Int32 rprecision = 0;
        require_prm(
            resolve_chained_column_type(*cte_scope, col_idx,
                                         rt, rlen, rcs, rscale, rprecision),
            "CTE output column has no resolved source type.");
        derived_type = rt;
        derived_length = rlen;
        derived_cs = rcs;
        derived_scale = rscale;
        derived_precision = rprecision;
        have_derived = true;
      } else if (o->type == Outputs::Type::AGGREGATE) {
        TokenKind fun = o->aggregate.fun;
        AggregationAPICompiler::Expr* arg = o->aggregate.arg;
        if (fun == T_COUNT) {
          derived_type = NdbDictionary::Column::Bigunsigned;
          derived_length = 1;
          have_derived = true;
        } else if (arg != NULL && arg->isLoad()) {
          Uint32 src_col_idx = arg->getLoadIdx();
          NdbDictionary::Column::Type st;
          Uint32 src_length = 1;
          const void* src_cs = NULL;
          Int32 src_scale = 0;
          Int32 src_precision = 0;
          require_prm(
              resolve_chained_column_type(*cte_scope, src_col_idx,
                                           st, src_length, src_cs,
                                           src_scale, src_precision),
              "CTE aggregate references unresolved source column.");
          if (fun == T_SUM) {
            switch (st) {
            case NdbDictionary::Column::Tinyint:
            case NdbDictionary::Column::Smallint:
            case NdbDictionary::Column::Mediumint:
            case NdbDictionary::Column::Int:
            case NdbDictionary::Column::Bigint:
              derived_type = NdbDictionary::Column::Bigint;
              break;
            case NdbDictionary::Column::Tinyunsigned:
            case NdbDictionary::Column::Smallunsigned:
            case NdbDictionary::Column::Mediumunsigned:
            case NdbDictionary::Column::Unsigned:
            case NdbDictionary::Column::Bigunsigned:
              derived_type = NdbDictionary::Column::Bigunsigned;
              break;
            case NdbDictionary::Column::Float:
            case NdbDictionary::Column::Double:
              derived_type = NdbDictionary::Column::Double;
              break;
            case NdbDictionary::Column::Decimal:
            case NdbDictionary::Column::Decimalunsigned:
              // D1: the kernel widens DECIMAL -> BIGINT (scale==0) or DOUBLE
              // (scale>0) on load via AggInterpreter::AlignedType, so SUM
              // accumulates the widened value.  Mirror the MIN/MAX DECIMAL
              // widening (below) so the virt-table column type matches the
              // wire format the kernel emits.  scale>0 SUM is therefore a
              // DOUBLE sum (best-effort, like DECIMAL MIN/MAX — no exact
              // DECIMAL accumulation in the kernel); the source scale +
              // precision are carried for fixed-scale display (D15), gated on
              // precision <= 15 in the result printer.
              require_prm(
                  decimal_minmax_fits_64bit(st, src_precision, src_scale),
                  "SUM over scale-zero DECIMAL wider than the 64-bit integer "
                  "range is not yet supported.");
              if (src_scale == 0) {
                derived_type = (st == NdbDictionary::Column::Decimalunsigned)
                               ? NdbDictionary::Column::Bigunsigned
                               : NdbDictionary::Column::Bigint;
              } else {
                derived_type = NdbDictionary::Column::Double;
                derived_scale = src_scale;
                derived_precision = src_precision;
              }
              break;
            default:
              throw RonSQLPermanentError(
                  "SUM over this column type in CTE not yet supported.");
            }
            derived_length = 1;
            have_derived = true;
          } else if (fun == T_MIN || fun == T_MAX) {
            // Numeric MIN/MAX results are written into the CTE
            // linked-attr buffer as 8-byte AggResItem.value (Uint64),
            // identical to SUM/COUNT — see Dblqh::cteLookupEmitResult
            // (DblqhMain.cpp ~19268: AttributeHeader::init(..., 8);
            // memcpy(..., 8) unconditionally for every aggregate slot).
            // Widen the virt-table column type to match the wire size
            // so the inline-type filter opcode and any client-side size
            // checks see consistent metadata. Aggregator loads
            // (LoadLinkedColumn etc.) read the buffer header length, so
            // the widening doesn't disturb existing aggregate consumers.
            switch (st) {
            case NdbDictionary::Column::Tinyint:
            case NdbDictionary::Column::Smallint:
            case NdbDictionary::Column::Mediumint:
            case NdbDictionary::Column::Int:
            case NdbDictionary::Column::Bigint:
              derived_type = NdbDictionary::Column::Bigint;
              derived_length = 1;
              break;
            case NdbDictionary::Column::Tinyunsigned:
            case NdbDictionary::Column::Smallunsigned:
            case NdbDictionary::Column::Mediumunsigned:
            case NdbDictionary::Column::Unsigned:
            case NdbDictionary::Column::Bigunsigned:
              derived_type = NdbDictionary::Column::Bigunsigned;
              derived_length = 1;
              break;
            case NdbDictionary::Column::Float:
            case NdbDictionary::Column::Double:
              derived_type = NdbDictionary::Column::Double;
              derived_length = 1;
              break;
            case NdbDictionary::Column::Decimal:
            case NdbDictionary::Column::Decimalunsigned:
              // Phase I.6 F.1: kernel widens DECIMAL → BIGINT
              // (scale==0) or DOUBLE (scale>0) via AlignedType
              // before MIN/MAX runs.  Mirror the widening here so
              // the virt-table column type matches the wire format
              // the kernel actually emits, unblocking the
              // inline-type CTE filter opcode for DECIMAL MIN/MAX
              // outputs.  User-visible result type follows the
              // widening (no DECIMAL precision preservation —
              // already lossy in the kernel today).
              require_prm(
                  decimal_minmax_fits_64bit(st, src_precision, src_scale),
                  "MIN/MAX over scale-zero DECIMAL wider than the 64-bit "
                  "integer range is not yet supported.  Full DECIMAL "
                  "precision preservation requires a wider "
                  "aggregate-result representation.");
              if (src_scale == 0) {
                derived_type = (st == NdbDictionary::Column::Decimalunsigned)
                               ? NdbDictionary::Column::Bigunsigned
                               : NdbDictionary::Column::Bigint;
              } else {
                derived_type = NdbDictionary::Column::Double;
                // D15: keep the source scale + precision for display so the
                // DOUBLE result prints with fixed scale (e.g. 20055.00) like
                // MySQL — gated on precision <= 15 in the printer.
                derived_scale = src_scale;
                derived_precision = src_precision;
              }
              derived_length = 1;
              break;
            case NdbDictionary::Column::Char:
            case NdbDictionary::Column::Varchar:
            case NdbDictionary::Column::Longvarchar:
              // Phase I.6 F.2: kernel computes per-(group, slot)
              // string MIN/MAX via AggResItem.value.val_ptr (see
              // cte_filter_phase_i6_varchar.md, K.4) and ships
              // the result via the AGG_CHAR_RESULT wire marker
              // (K.5).  Preserve the source column's type, length,
              // and charset so the virt-table descriptor matches
              // exactly what the kernel produces.  F.3-R.2 will
              // also accept these virt-types in the inline-type
              // filter opcode.
              derived_type = st;
              derived_length = src_length;
              derived_cs = src_cs;
              break;
            case NdbDictionary::Column::Date:
            case NdbDictionary::Column::Year:
            case NdbDictionary::Column::Datetime2:
            case NdbDictionary::Column::Time2:
            case NdbDictionary::Column::Timestamp2:
              // D17 + temporal: the kernel reads the temporal column's native
              // packed value as an unsigned integer and emits an 8-byte
              // Bigunsigned MIN/MAX result (see AggInterpreterBase).  The
              // virt-table column type MUST be Bigunsigned (8 bytes) to
              // match the wire format, not the source temporal type — re-
              // aggregation and the inline-type filter opcode rely on
              // consistent metadata.  The temporal *display* is recovered at
              // the top level via ColumnMetadata::temporal
              // (resolve_cte_output_columns_for_scope plumbs the source
              // temporal column back through the CTE MIN/MAX).
              derived_type = NdbDictionary::Column::Bigunsigned;
              derived_length = 1;
              break;
            default:
              // Other non-numeric source types (Time / Timestamp / Bit /
              // Binary / Blob / Text / etc.): best-effort passthrough —
              // kernel-side TypeSupported on these is not yet implemented,
              // so MIN/MAX over them is not actually supported end-to-end.
              // Kept here for the rare case that a passthrough aggregator
              // path consumes a virt column without going through MIN/MAX
              // execution.
              derived_type = st;
              derived_length = src_length;
              derived_cs = src_cs;
              break;
            }
            have_derived = true;
          } else {
            throw RonSQLPermanentError(
                "Unsupported aggregate function in CTE output.");
          }
        } else {
          throw RonSQLPermanentError(
              "CTE aggregate over complex expression not yet supported.");
        }
      } else if (o->type == Outputs::Type::AVG) {
        throw RonSQLPermanentError("AVG in CTE output not yet supported.");
      } else {
        throw RonSQLPermanentError(
            "Unsupported CTE output kind.");
      }
      ndbrequire(have_derived);

      vcol.setType(derived_type);
      vcol.setLength(derived_length);
      if (derived_cs != NULL) {
        vcol.setCharset(
            static_cast<CHARSET_INFO*>(const_cast<void*>(derived_cs)));
      }
      // D15: carry the display scale + precision (DECIMAL source) onto the virt
      // column so column metadata / the result printer can format with fixed
      // scale, gated on precision <= 15.  Only set when meaningful.
      if (derived_scale > 0) {
        vcol.setScale(derived_scale);
      }
      if (derived_precision > 0) {
        vcol.setPrecision(derived_precision);
      }
      vcol.setPrimaryKey(is_groupby);
      vcol.setNullable((cte_is_scalar || cte_is_single_row)
                           ? true
                           : !is_groupby);
      vt->addColumn(vcol);
    }
    // NdbDictionary::Table aggregate counts (getNoOfPrimaryKeys etc.) are
    // not auto-updated as columns are added. For in-memory virtual tables
    // built by this helper, we must call aggregate() explicitly before
    // handing the table to lookupCte — otherwise the PK count stays 0 and
    // lookupCte's key-count check fails with QRY_TOO_MANY_KEY_VALUES.
    NdbError vtErr;
    require_run(vt->aggregate(vtErr) == 0,
                "Failed to aggregate CTE virtual table metadata.");
    // Assign synthetic attrIds.  NdbDictionary::Table::addColumn leaves
    // m_attrId=-1 on each column (only the dictionary's table-create
    // path sets it).  Most use sites resolve columns by index via
    // getColumn(idx), but the Phase E.3 pass-through drain calls
    // NdbQueryOperation::getValue(column*) on the scanCte root, which
    // copies column.m_attrId into the resulting NdbRecAttr.theAttrId.
    // The kernel's CTE_SCAN result delivery emits attrIds 0..numCols-1
    // (see prepareAttrInfo's QN_CTE_SCAN case in NdbQueryOperation.cpp);
    // without this assignment, NdbReceiver::handle_rec_attrs aborts
    // when matching incoming attrId=0,1,... against recAttr.attrId=-1.
    {
      Uint32 ncol = (Uint32)vt->getNoOfColumns();
      for (Uint32 cidx = 0; cidx < ncol; cidx++) {
        NdbDictionary::Column* mut_col = vt->getColumn((int)cidx);
        ndbrequire(mut_col != NULL);
        NdbColumnImpl& mut_impl = NdbColumnImpl::getImpl(*mut_col);
        mut_impl.m_attrId = (int)cidx;

        // setType() does not populate m_attrSize / m_arraySize;
        // those are normally set when the dictionary parses a
        // table descriptor from the kernel.  For synthetic virt
        // tables they stay at zero, so getSizeInBytes() returns 0.
        // The Phase E.3 scanCte pass-through has tolerated this
        // because NdbReceiver sizes scan-op buffers via
        // calculate_batch_size (default sizes, column-independent).
        // The Phase I.7 lookupCte path goes through packed_rowsize
        // → getColumn()->getSizeInBytes() instead, and a 0 there
        // makes NdbReceiverBuffer::allocRow's assertion fire when
        // the actual row arrives.  Populate the attr-size fields
        // for the numeric types build_cte_virtual_tables can emit.
        switch (mut_col->getType()) {
        case NdbDictionary::Column::Tinyint:
        case NdbDictionary::Column::Tinyunsigned:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 1;
          break;
        case NdbDictionary::Column::Smallint:
        case NdbDictionary::Column::Smallunsigned:
          mut_impl.m_attrSize = 2;
          mut_impl.m_orgAttrSize = 4;
          mut_impl.m_arraySize = 1;
          break;
        case NdbDictionary::Column::Mediumint:
        case NdbDictionary::Column::Mediumunsigned:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 3;
          break;
        case NdbDictionary::Column::Int:
        case NdbDictionary::Column::Unsigned:
        case NdbDictionary::Column::Float:
          mut_impl.m_attrSize = 4;
          mut_impl.m_orgAttrSize = 5;
          mut_impl.m_arraySize = 1;
          break;
        case NdbDictionary::Column::Bigint:
        case NdbDictionary::Column::Bigunsigned:
        case NdbDictionary::Column::Double:
          mut_impl.m_attrSize = 8;
          mut_impl.m_orgAttrSize = 6;
          mut_impl.m_arraySize = 1;
          break;
        case NdbDictionary::Column::Char:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = mut_col->getLength();
          break;
        case NdbDictionary::Column::Varchar:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 1 + mut_col->getLength();
          break;
        case NdbDictionary::Column::Longvarchar:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 2 + mut_col->getLength();
          break;
        // Fixed-width temporals (single-row CTE bodies project source
        // columns verbatim, so these appear as virt columns on the
        // lookupCte path — e.g. a DATE watermark).
        case NdbDictionary::Column::Year:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 1;
          break;
        case NdbDictionary::Column::Date:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 3;
          break;
        case NdbDictionary::Column::Time:
          mut_impl.m_attrSize = 1;
          mut_impl.m_orgAttrSize = 3;
          mut_impl.m_arraySize = 3;
          break;
        case NdbDictionary::Column::Datetime:
          mut_impl.m_attrSize = 8;
          mut_impl.m_orgAttrSize = 6;
          mut_impl.m_arraySize = 1;
          break;
        case NdbDictionary::Column::Timestamp:
          mut_impl.m_attrSize = 4;
          mut_impl.m_orgAttrSize = 5;
          mut_impl.m_arraySize = 1;
          break;
        default:
          // DECIMAL, fractional-second temporals (Datetime2 / Time2 /
          // Timestamp2) etc.: existing scanCte path tolerated 0; the
          // lookupCte path with such virt column types is not
          // exercised yet by RonSQL.
          break;
        }
      }
    }
    out[i] = vt;
  }
}

// Compile a main-query WHERE filter on a CTE_LOOKUP child into an
// NdbInterpretedCode program of branch_linked_* instructions.  The
// filter evaluates against DBLQH's linked-attr buffer for the CTE
// lookup: position 0..N-1 matches the virtual table's columns in the
// order added by build_cte_virtual_tables.
//
// Phase I.2: top-level DNF accepted — `D_1 OR D_2 OR ... OR D_n`,
// where each disjunct D_i is a single atom or a conjunction of
// atoms (`A AND B AND ...`).  Atoms remain column-vs-constant
// comparisons (= != < <= > >=) or `IS NULL` / `IS NOT NULL` on a
// CTE output column.  Single-disjunct emission is byte-equivalent
// to the original AND-only path.
//
// Codegen pattern (n disjuncts, m_i atoms each):
//
//   for i = 0..n-1:
//     fail_i = (i < n-1) ? alloc_label() : REJECT
//     emit each atom A_ij as "branch fail_i if NOT predicate"
//     if i < n-1:
//        branch_label ACCEPT       ; this disjunct matched
//        def_label fail_i
//   def_label ACCEPT
//   interpret_exit_ok
//   def_label REJECT
//   interpret_exit_nok
//
// Non-DNF nesting (e.g. `(a OR b) AND c`), NOT outside an IS NOT NULL
// atom, column-vs-column, and cross-table comparisons are still
// rejected with a clean error — kernel side already supports them
// via the jump-table interpreter (see cte_filter_plan.md), but the
// RonSQL surface currently doesn't normalise them.
void
RonSQLPreparer::emit_cte_lookup_filter(NdbInterpretedCode& code,
                                       QueryScope& scope,
                                       Uint32 op_idx,
                                       NdbDictionary::Table* virtTab,
                                       ConditionalExpression* where_ce)
{
  require_run(virtTab != NULL,
              "CTE_LOOKUP filter requires a virtual table descriptor.");

  ConditionalExpression* simplified = simplify_ce(where_ce, -1);
  ConditionalExpression* disjuncts[MAX_WHERE_CONJUNCTS];
  Uint32 num_disjuncts = 0;
  flatten_or_disjuncts(simplified, disjuncts, &num_disjuncts);
  require_prm(num_disjuncts > 0,
              "CTE_LOOKUP filter: no disjuncts after simplification.");

  // Defensive cap on disjunct count keeps program size sane and label
  // allocation predictable.  Far smaller than the kernel's per-program
  // instruction limit; user-facing queries have at most a handful.
  static const Uint32 MAX_DNF_DISJUNCTS = 16;
  require_prm(num_disjuncts <= MAX_DNF_DISJUNCTS,
              "CTE_LOOKUP filter: too many top-level OR disjuncts.");

  /*
   * Phase i26 (W3): when linked parent projections attach to this op,
   * Dblqh::buildCteLinkedBuffer PREPENDS them ahead of the CTE
   * outputs, so every CTE-side buffer position is offset by
   * num_linked_projs.  Projections attach for (a) a single-leaf
   * aggregator sitting on this op with linked projections — the
   * latent base-offset bug: filters on the CTE's own outputs compared
   * the wrong buffer entries when GROUP BY linked projections were
   * present — and (b) the needs_parent_linked_attrs filter
   * projections (real-vs-CTE atoms).  Root CTE ops keep base 0
   * naturally: a root has no ancestors, so no projections can attach.
   */
  Uint32 linked_base = 0;
  {
    const JoinPlan& jplan = scope.join_plan;
    const JoinOp& jop = jplan.ops[op_idx];
    bool has_linked_attach = jop.needs_parent_linked_attrs;
    if (scope.agg != NULL && jplan.num_agg_leaves == 0 &&
        jplan.agg_leaf_idx == op_idx && jplan.num_linked_projs > 0) {
      has_linked_attach = true;
    }
    if (has_linked_attach) {
      linked_base = jplan.num_linked_projs;
    }
  }

  // Find-only linked-projection position for an ancestor column
  // (classify_where_by_table registered them via
  // find_or_add_linked_proj; positions equal projection indices).
  auto find_proj_pos = [&](Uint32 src_op_idx,
                           const char* col_name) -> Uint32 {
    const JoinPlan& jplan = scope.join_plan;
    for (Uint32 j = 0; j < jplan.num_linked_projs; j++) {
      if (jplan.linked_projs[j].source_op_idx == src_op_idx &&
          strcmp(jplan.linked_projs[j].column_name, col_name) == 0) {
        return j;
      }
    }
    require_prm(false,
                "CTE_LOOKUP filter: ancestor column has no registered "
                "linked projection.  Please report a bug.");
    return 0;  // unreachable
  };

  // Type envelope for typed register loads (READ_LINKED_COLUMN_TO_REG
  // kernel arms: the 10 integer widths + Float + Double).
  auto is_typed_reg_loadable = [](NdbDictionary::Column::Type t) -> bool {
    switch (t) {
    case NdbDictionary::Column::Tinyint:
    case NdbDictionary::Column::Tinyunsigned:
    case NdbDictionary::Column::Smallint:
    case NdbDictionary::Column::Smallunsigned:
    case NdbDictionary::Column::Mediumint:
    case NdbDictionary::Column::Mediumunsigned:
    case NdbDictionary::Column::Int:
    case NdbDictionary::Column::Unsigned:
    case NdbDictionary::Column::Bigint:
    case NdbDictionary::Column::Bigunsigned:
    case NdbDictionary::Column::Float:
    case NdbDictionary::Column::Double:
      return true;
    default:
      return false;
    }
  };

  const Uint32 REJECT = 0;
  const Uint32 ACCEPT = 1;
  Uint32 next_label = 2;  // FAIL_i labels start here

  // Per-atom emit — extracted from the original AND-only loop body so
  // that DNF emission can re-target the fail label per disjunct.
  // Captures the surrounding scope/virtTab/op_idx; called repeatedly
  // for every atom in every disjunct.  Returns nothing; throws on
  // structural rejections (column-vs-column, NULL operands, etc.) via
  // require_prm.
  auto emit_atom = [&](ConditionalExpression* atom,
                       Uint32 fail_label) -> void {
    require_prm(atom != NULL, "CTE_LOOKUP filter: NULL atom.");

    // T_IS: `col IS NULL` / `col IS NOT NULL` — Phase I.1.  Emit a
    // single null-flag branch on the linked-attr buffer entry.  No
    // type info needed; the AttributeHeader's NULL flag is the only
    // thing examined.
    if (atom->op == T_IS) {
      ConditionalExpression* col_side = atom->is.arg;
      require_prm(col_side != NULL && col_side->op == T_IDENTIFIER,
                  "CTE_LOOKUP filter: IS NULL operand must be a "
                  "column reference.");
      Uint32 col_idx = col_side->col_idx;
      require_run(scope.resolved_columns != NULL,
                  "CTE_LOOKUP filter: missing resolved columns.");
      const QueryScope::ResolvedColumnRef& col_ref =
          scope.resolved_columns[col_idx];
      require_prm(
          col_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn &&
          col_ref.join_op_idx == op_idx,
                  "CTE_LOOKUP filter: IS conjunct references a column "
                  "not on this CTE.");

      // IS NULL on a LEFT_OUTER op's RHS column collides with the
      // LEFT JOIN's NULL-injection: the kernel filter rejects matched
      // rows (CTE columns are non-NULL on the matched path), and the
      // API can't distinguish "rejected match" from "no match" — so
      // all parents end up NULL-injected and the result mixes
      // rejected matches with unmatched rows.  Reject cleanly until
      // post-join filtering lands; IS NOT NULL is fine because
      // Phase J already promoted the LEFT JOIN to INNER for it.
      if (atom->is.null) {
        require_prm(scope.join_plan.ops[op_idx].match_type !=
                    JoinOp::LEFT_OUTER,
                    "WHERE col IS NULL on a LEFT JOIN's RHS column is "
                    "not yet supported — kernel filter pushdown "
                    "collides with LEFT-JOIN NULL-injection of "
                    "unmatched rows.");
      }

      Uint32 cte_col_idx = col_ref.cte_result_idx;
      // W3: CTE outputs sit after any prepended linked projections.
      Uint32 position = linked_base + cte_col_idx;
      int rc = atom->is.null
          ? code.branch_linked_isnotnull(position, fail_label)
          : code.branch_linked_isnull(position, fail_label);
      require_prm(rc == 0,
                  "CTE_LOOKUP filter: failed to emit IS-null branch.");
      return;
    }

    bool is_cmp = (atom->op == T_EQUALS || atom->op == T_NOT_EQUALS ||
                   atom->op == T_LT || atom->op == T_LE ||
                   atom->op == T_GT || atom->op == T_GE);
    require_prm(is_cmp,
                "CTE_LOOKUP filter supports only simple comparisons "
                "(=, !=, <, <=, >, >=), IS NULL / IS NOT NULL, and "
                "DNF combinations of these — column expressions and "
                "non-DNF nesting will be added in a later phase.");

    ConditionalExpression* left = atom->args.left;
    ConditionalExpression* right = atom->args.right;
    require_prm(left != NULL && right != NULL,
                "CTE_LOOKUP filter: comparison has NULL operand.");

    // Phase I.3 → i26 (W2 typed upgrade + W6 parent sides):
    // column-vs-column via typed register loads.  Each side is either
    // an output of THIS CTE (buffer position = linked_base +
    // cte_result_idx, type from the virt-table column) or a
    // TREE-ANCESTOR real-table column riding the linked projections
    // (buffer position = projection index, type from the dictionary
    // column).  READ_LINKED_COLUMN_TO_REG decodes with correct sign
    // extension for all 10 integer widths plus Float / Double, and
    // I.18's typed registers make the reg-vs-reg branches compare
    // correctly across mixed signedness and int-vs-double
    // (compareTypedRegs).  DECIMAL / string / temporal operands are
    // rejected.  This flips both rpr-P1 (CTE-own-output col-vs-col
    // beyond Bigint) and sc-P1 (real-vs-scalar-CTE watermarks).
    if (left->op == T_IDENTIFIER && right->op == T_IDENTIFIER) {
      require_run(scope.resolved_columns != NULL,
                  "CTE_LOOKUP filter: missing resolved columns.");
      // Resolve one side to (buffer position, NDB type).
      auto resolve_side =
          [&](ConditionalExpression* side, Uint32& out_pos,
              NdbDictionary::Column::Type& out_type) -> void {
        Uint32 cidx = side->col_idx;
        const QueryScope::ResolvedColumnRef& ref =
            scope.resolved_columns[cidx];
        if (ref.kind ==
                QueryScope::ResolvedColumnRef::Kind::CteResultColumn &&
            ref.join_op_idx == op_idx) {
          Uint32 cte_idx = ref.cte_result_idx;
          const NdbDictionary::Column* vc =
              virtTab->getColumn((int)cte_idx);
          require_prm(vc != NULL,
                      "CTE_LOOKUP filter col-vs-col: virt-table missing "
                      "column descriptor.");
          out_pos = linked_base + cte_idx;
          out_type = vc->getType();
          return;
        }
        if (ref.kind ==
                QueryScope::ResolvedColumnRef::Kind::StoredColumn &&
            ref.join_op_idx != op_idx &&
            linked_source_is_leaf_ancestor(scope.join_plan, op_idx,
                                           ref.join_op_idx)) {
          require_prm(ref.dict_column != NULL,
                      "CTE_LOOKUP filter col-vs-col: ancestor column "
                      "descriptor missing.");
          out_pos = find_proj_pos(ref.join_op_idx,
                                  m_columns[cidx].c_str());
          out_type = ref.dict_column->getType();
          return;
        }
        require_prm(false,
                    "CTE_LOOKUP filter col-vs-col: column must be an "
                    "output of this CTE or a tree-ancestor real-table "
                    "column.  Cross-CTE and sibling-branch comparisons "
                    "not yet supported.");
      };
      Uint32 pos_l = 0, pos_r = 0;
      NdbDictionary::Column::Type type_l = NdbDictionary::Column::Bigint;
      NdbDictionary::Column::Type type_r = NdbDictionary::Column::Bigint;
      resolve_side(left, pos_l, type_l);
      resolve_side(right, pos_r, type_r);
      require_prm(is_typed_reg_loadable(type_l) &&
                  is_typed_reg_loadable(type_r),
                  "CTE_LOOKUP filter col-vs-col: only integer, FLOAT "
                  "and DOUBLE operands are supported — DECIMAL, string "
                  "and temporal comparisons need a cast or a "
                  "constant-vs-column form.");

      // UNKNOWN rejects the row/disjunct: guard both operands' NULL
      // flags before the typed loads (a NULL register would raise
      // ZREGISTER_INIT_ERROR instead of rejecting).
      require_prm(code.branch_linked_isnull(pos_l, fail_label) == 0,
                  "CTE_LOOKUP filter col-vs-col: failed to emit left "
                  "NULL guard.");
      require_prm(code.branch_linked_isnull(pos_r, fail_label) == 0,
                  "CTE_LOOKUP filter col-vs-col: failed to emit right "
                  "NULL guard.");

      const Uint32 R1 = 1, R2 = 2;
      require_prm(code.read_linked_column_to_reg(R1, pos_l,
                                                 (Uint32)type_l) == 0,
                  "CTE_LOOKUP filter col-vs-col: typed load (left) "
                  "failed.");
      require_prm(code.read_linked_column_to_reg(R2, pos_r,
                                                 (Uint32)type_r) == 0,
                  "CTE_LOOKUP filter col-vs-col: typed load (right) "
                  "failed.");

      int rc = -1;
      switch (atom->op) {
      case T_EQUALS:     rc = code.branch_ne(R1, R2, fail_label); break;
      case T_NOT_EQUALS: rc = code.branch_eq(R1, R2, fail_label); break;
      case T_LT:         rc = code.branch_ge(R1, R2, fail_label); break;
      case T_LE:         rc = code.branch_gt(R1, R2, fail_label); break;
      case T_GT:         rc = code.branch_le(R1, R2, fail_label); break;
      case T_GE:         rc = code.branch_lt(R1, R2, fail_label); break;
      default:
        require_prm(false,
                    "CTE_LOOKUP filter col-vs-col: unsupported "
                    "operator.");
      }
      require_prm(rc == 0,
                  "CTE_LOOKUP filter col-vs-col: failed to emit "
                  "reg-vs-reg branch.");
      return;
    }

    // Identify which side is the CTE column and which is the constant.
    auto is_const = [](ConditionalExpression* e) {
      return e->op == T_INT || e->op == T_FLOAT ||
             e->op == T_STRING || e->op == T_NULL;
    };
    ConditionalExpression* col_side = NULL;
    ConditionalExpression* const_side = NULL;
    bool swapped = false;
    if (left->op == T_IDENTIFIER && is_const(right)) {
      col_side = left;
      const_side = right;
    } else if (right->op == T_IDENTIFIER && is_const(left)) {
      col_side = right;
      const_side = left;
      swapped = true;
    } else {
      require_prm(false,
                  "CTE_LOOKUP filter supports only column-vs-constant "
                  "and same-CTE column-vs-column comparisons; "
                  "expressions on either side will be added in a "
                  "later phase.");
    }

    Uint32 col_idx = col_side->col_idx;
    require_run(scope.resolved_columns != NULL,
                "CTE_LOOKUP filter: missing resolved columns.");
    const QueryScope::ResolvedColumnRef& col_ref =
        scope.resolved_columns[col_idx];

    // i26 (W6): ancestor real-table column vs constant, inside a
    // conjunct classify_where_by_table routed to this CTE op.  The
    // buffer entry is a real-table [tableId][schemaVersion]-prefixed
    // linked value at the projection index — exactly what the
    // branch_linked_mem_* family resolves; the constant encodes
    // against the real column descriptor.
    if (col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn &&
        col_ref.join_op_idx != op_idx &&
        linked_source_is_leaf_ancestor(scope.join_plan, op_idx,
                                       col_ref.join_op_idx)) {
      require_prm(col_ref.dict_column != NULL,
                  "CTE_LOOKUP filter: ancestor column descriptor "
                  "missing.");
      Uint32 anc_position = find_proj_pos(col_ref.join_op_idx,
                                          m_columns[col_idx].c_str());
      const NdbDictionary::Table* anc_table =
          scope.join_plan.ops[col_ref.join_op_idx].table;
      require_prm(anc_table != NULL,
                  "CTE_LOOKUP filter: ancestor op has no physical "
                  "table.");
      Uint32 anc_attrId = (Uint32)col_ref.dict_column->getColumnNo();
      raw_value anc_rv = encode_constant(const_side, col_ref.dict_column);
      require_prm(code.branch_linked_isnull(anc_position, fail_label) == 0,
                  "CTE_LOOKUP filter: failed to emit ancestor NULL "
                  "guard.");
      TokenKind anc_op = atom->op;
      if (swapped) {
        switch (anc_op) {
        case T_LT: anc_op = T_GT; break;
        case T_LE: anc_op = T_GE; break;
        case T_GT: anc_op = T_LT; break;
        case T_GE: anc_op = T_LE; break;
        default: break;
        }
      }
      int anc_rc = -1;
      switch (anc_op) {
      case T_EQUALS:
        anc_rc = code.branch_linked_mem_ne(anc_position, anc_table,
                                           anc_attrId, anc_rv.val,
                                           anc_rv.len, fail_label);
        break;
      case T_NOT_EQUALS:
        anc_rc = code.branch_linked_mem_eq(anc_position, anc_table,
                                           anc_attrId, anc_rv.val,
                                           anc_rv.len, fail_label);
        break;
      case T_LT:
        anc_rc = code.branch_linked_mem_le(anc_position, anc_table,
                                           anc_attrId, anc_rv.val,
                                           anc_rv.len, fail_label);
        break;
      case T_LE:
        anc_rc = code.branch_linked_mem_lt(anc_position, anc_table,
                                           anc_attrId, anc_rv.val,
                                           anc_rv.len, fail_label);
        break;
      case T_GT:
        anc_rc = code.branch_linked_mem_ge(anc_position, anc_table,
                                           anc_attrId, anc_rv.val,
                                           anc_rv.len, fail_label);
        break;
      case T_GE:
        anc_rc = code.branch_linked_mem_gt(anc_position, anc_table,
                                           anc_attrId, anc_rv.val,
                                           anc_rv.len, fail_label);
        break;
      default:
        require_prm(false,
                    "Unsupported CTE_LOOKUP filter operator "
                    "(ancestor).");
      }
      require_prm(anc_rc == 0,
                  "CTE_LOOKUP filter: failed to emit ancestor branch.");
      return;
    }

    require_prm(
        col_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn &&
        col_ref.join_op_idx == op_idx,
                "CTE_LOOKUP filter conjunct references a column not on "
                "this CTE. classify_where_by_table should have routed it "
                "elsewhere.");

    // cte_col_idx is the 0-based CTE output index (see load_join CTE
    // branch). The linked-buffer position used by handleReadLinkedToMem
    // matches the addColumn order — GB keys first, then aggregate
    // results — which is also the order of CTE outputs (offset by
    // linked_base when linked projections are prepended, W3).
    Uint32 cte_col_idx = col_ref.cte_result_idx;
    Uint32 position = linked_base + cte_col_idx;

    // The compare path depends on whether the CTE output is a direct
    // column projection or an aggregate result:
    //   - COLUMN: linked-buffer slot stores the source-column-typed
    //     value, so we can reference the source real column's
    //     descriptor via the existing branch_linked_mem_* family
    //     (server resolves type via tablerec[tableId]).
    //   - AGGREGATE / SUM | COUNT: result type is synthesized
    //     (Bigint / Bigunsigned / Double — always 8 bytes in the
    //     buffer slot; see Dblqh::buildCteLinkedBuffer Step 3) and
    //     no real registered NDB column matches.  Use the new
    //     branch_linked_inline_* family which encodes type/length/
    //     charset inline in the program.
    //   - AGGREGATE / MIN | MAX: virt-table type follows source type
    //     (e.g. Int = 4 bytes) but the buffer slot is 8 bytes, so
    //     descriptor wouldn't match the data.  Reject for now.
    //   - AGGREGATE / AVG and DECIMAL aggregates: out of scope.
    const JoinOp& cte_op = scope.join_plan.ops[op_idx];
    require_run(cte_op.cte_def != NULL,
                "CTE_LOOKUP filter: op has no CTE definition.");
    require_run(col_ref.cte_def_idx == cte_op.cte_def_idx,
                "CTE_LOOKUP filter: descriptor CTE index mismatch.");
    require_run(col_ref.cte_def_idx < m_cte_scopes.size(),
                "CTE_LOOKUP filter: cte_def_idx out of range.");
    QueryScope* cs = m_cte_scopes[col_ref.cte_def_idx];
    require_run(cs != NULL,
                "CTE_LOOKUP filter: missing CTE body scope.");

    const Outputs* o = col_ref.cte_output;
    require_prm(o != NULL, "CTE_LOOKUP filter: output index out of range.");

    bool use_inline_path = false;
    const NdbDictionary::Table* src_table = NULL;
    Uint32 attrId = 0;

    // Use the synthetic virt-table column for vt-context (charset etc.
    // for encode_constant) and for the inline-path source-column.
    const NdbDictionary::Column* vtcol =
        virtTab->getColumn((int)cte_col_idx);
    require_prm(vtcol != NULL,
                "CTE_LOOKUP filter: virt table has no column at "
                "cte_col_idx.");

    if (o->type == Outputs::Type::COLUMN) {
      // Source-real-column path.
      Uint32 src_col_idx = o->column.col_idx;
      require_run(cs->resolved_columns != NULL,
                  "CTE_LOOKUP filter: CTE body missing resolved columns.");
      const QueryScope::ResolvedColumnRef& src_ref =
          cs->resolved_columns[src_col_idx];
      require_prm(
          src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
          "CTE_LOOKUP filter: CTE body source is not a stored-table "
          "column.");
      const NdbDictionary::Column* src_col = src_ref.dict_column;
      require_prm(src_col != NULL,
                  "CTE_LOOKUP filter: CTE body source column not "
                  "resolved.");
      Uint32 src_op_idx = src_ref.join_op_idx;
      require_prm(src_op_idx < cs->join_plan.num_ops,
                  "CTE_LOOKUP filter: CTE body source op idx out of "
                  "range.");
      src_table = cs->join_plan.ops[src_op_idx].table;
      require_prm(src_table != NULL,
                  "CTE_LOOKUP filter: CTE body source op has no "
                  "physical table.");
      attrId = (Uint32)src_col->getColumnNo();
    } else if (o->type == Outputs::Type::AGGREGATE) {
      TokenKind fun = o->aggregate.fun;
      if (fun == T_MIN || fun == T_MAX) {
        // Numeric MIN/MAX has been widened to Bigint/Bigunsigned/
        // Double by build_cte_virtual_tables — those share the
        // 8-byte AggResItem.value wire format with SUM/COUNT and
        // dispatch through the same inline-type opcode.
        // Phase I.6 F.3-R.2: CHAR / VARCHAR / Longvarchar MIN/MAX
        // results are now produced by the kernel via the
        // AGG_CHAR_RESULT wire path — accept them too.  The inline
        // opcode emit below derives the correct columnSize and
        // csNumber from the virt-table column for these types.
        NdbDictionary::Column::Type vt = vtcol->getType();
        require_prm(vt == NdbDictionary::Column::Bigint ||
                    vt == NdbDictionary::Column::Bigunsigned ||
                    vt == NdbDictionary::Column::Double ||
                    vt == NdbDictionary::Column::Char ||
                    vt == NdbDictionary::Column::Varchar ||
                    vt == NdbDictionary::Column::Longvarchar,
                    "CTE_LOOKUP filter on MIN/MAX of this column type "
                    "not yet supported.  Numeric (BIGINT / DOUBLE) and "
                    "CHAR / VARCHAR / Longvarchar are accepted; DECIMAL "
                    "MIN/MAX outputs go through the BIGINT/DOUBLE "
                    "widening path (Phase I.6 F.1).");
      } else {
        require_prm(fun == T_SUM || fun == T_COUNT,
                    "CTE_LOOKUP filter on aggregate output: only SUM, "
                    "COUNT, and numeric MIN/MAX are supported.  AVG "
                    "and DECIMAL aggregates need DECIMAL precision/scale "
                    "encoding in the inline opcode — deferred to "
                    "follow-up work.");
      }
      use_inline_path = true;
    } else {
      require_prm(false,
                  "CTE_LOOKUP filter: unsupported CTE output kind.");
    }

    raw_value rv = encode_constant(const_side, vtcol);

    require_prm(code.branch_linked_isnull(position, fail_label) == 0,
                "CTE_LOOKUP filter: failed to emit comparison NULL "
                "guard.");

    // Canonicalise the operator so we can think of it as "col OP const".
    TokenKind eff_op = atom->op;
    if (swapped) {
      switch (eff_op) {
      case T_LT: eff_op = T_GT; break;
      case T_LE: eff_op = T_GE; break;
      case T_GT: eff_op = T_LT; break;
      case T_GE: eff_op = T_LE; break;
      default: break;  // EQ / NE unchanged
      }
    }

    // Emit the INVERTED branch — we want to jump to REJECT when the
    // predicate is FALSE. Inequality methods on NdbInterpretedCode are
    // themselves documented as inverted (branch_linked_mem_le branches
    // when col >= val, etc — see the header comment and CLAUDE.md).
    // The two inversions net out to "pick the method that matches the
    // SQL operator that should still REJECT".
    int rc = -1;
    if (use_inline_path) {
      // SUM / COUNT and numeric MIN/MAX results are written into the
      // linked buffer as fixed 8-byte values regardless of source
      // type (Dblqh::buildCteLinkedBuffer Step 3 — AggResItem.value:
      // Uint64).  Phase I.6 F.3-R.2: CHAR / VARCHAR / Longvarchar
      // MIN/MAX results carry the source column's declared length
      // and charset on the wire; derive columnSize and csNumber
      // from the virt-table column (set up by
      // build_cte_virtual_tables / F.3-R.1).  Phase D's
      // branch_linked_inline_* opcode family already handles
      // variable column size + charset.
      const Uint32 inline_typeId = (Uint32)vtcol->getType();
      const NdbDictionary::Column::Type vt = vtcol->getType();
      Uint32 inline_columnSize = 8;
      Uint32 inline_csNumber = 0;
      if (vt == NdbDictionary::Column::Char ||
          vt == NdbDictionary::Column::Varchar ||
          vt == NdbDictionary::Column::Longvarchar) {
        inline_columnSize = (Uint32)vtcol->getLength();
        // getCharsetNumber dereferences the impl charset; use the
        // public accessor so RonSQLPreparer.cpp doesn't need the
        // full CHARSET_INFO definition.
        inline_csNumber = (Uint32)vtcol->getCharsetNumber();
      }
      switch (eff_op) {
      case T_EQUALS:
        rc = code.branch_linked_inline_ne(position, inline_typeId,
                                          inline_columnSize, inline_csNumber,
                                          rv.val, rv.len, fail_label);
        break;
      case T_NOT_EQUALS:
        rc = code.branch_linked_inline_eq(position, inline_typeId,
                                          inline_columnSize, inline_csNumber,
                                          rv.val, rv.len, fail_label);
        break;
      case T_LT:
        rc = code.branch_linked_inline_le(position, inline_typeId,
                                          inline_columnSize, inline_csNumber,
                                          rv.val, rv.len, fail_label);
        break;
      case T_LE:
        rc = code.branch_linked_inline_lt(position, inline_typeId,
                                          inline_columnSize, inline_csNumber,
                                          rv.val, rv.len, fail_label);
        break;
      case T_GT:
        rc = code.branch_linked_inline_ge(position, inline_typeId,
                                          inline_columnSize, inline_csNumber,
                                          rv.val, rv.len, fail_label);
        break;
      case T_GE:
        rc = code.branch_linked_inline_gt(position, inline_typeId,
                                          inline_columnSize, inline_csNumber,
                                          rv.val, rv.len, fail_label);
        break;
      default:
        require_prm(false,
                    "Unsupported CTE_LOOKUP filter operator (inline).");
      }
      require_prm(rc == 0,
                  "CTE_LOOKUP filter: failed to emit inline branch.");
      return;
    }
    switch (eff_op) {
    case T_EQUALS:
      rc = code.branch_linked_mem_ne(position, src_table, attrId,
                                     rv.val, rv.len, fail_label);
      break;
    case T_NOT_EQUALS:
      rc = code.branch_linked_mem_eq(position, src_table, attrId,
                                     rv.val, rv.len, fail_label);
      break;
    case T_LT:
      // keep: col < val; reject: col >= val; method that branches on
      // col >= val is branch_linked_mem_le.
      rc = code.branch_linked_mem_le(position, src_table, attrId,
                                     rv.val, rv.len, fail_label);
      break;
    case T_LE:
      // keep: col <= val; reject: col > val; branches on col > val is
      // branch_linked_mem_lt.
      rc = code.branch_linked_mem_lt(position, src_table, attrId,
                                     rv.val, rv.len, fail_label);
      break;
    case T_GT:
      // keep: col > val; reject: col <= val; branches on col <= val is
      // branch_linked_mem_ge.
      rc = code.branch_linked_mem_ge(position, src_table, attrId,
                                     rv.val, rv.len, fail_label);
      break;
    case T_GE:
      // keep: col >= val; reject: col < val; branches on col < val is
      // branch_linked_mem_gt.
      rc = code.branch_linked_mem_gt(position, src_table, attrId,
                                     rv.val, rv.len, fail_label);
      break;
    default:
      require_prm(false, "Unsupported CTE_LOOKUP filter operator.");
    }
    require_prm(rc == 0, "CTE_LOOKUP filter: failed to emit branch.");
  };  // end emit_atom lambda

  // Phase I.2: drive the per-disjunct emit.  For n=1 (no top-level OR)
  // this collapses to the original AND-only path: no FAIL_i labels are
  // allocated, no ACCEPT branch is emitted, and atom branches target
  // REJECT directly — byte-equivalent to pre-I.2 emission.
  for (Uint32 di = 0; di < num_disjuncts; di++) {
    ConditionalExpression* d = disjuncts[di];
    require_prm(d != NULL, "CTE_LOOKUP filter: NULL disjunct.");
    require_prm(!contains_or_below_top_level(d),
                "CTE_LOOKUP filter: only top-level OR / DNF is supported. "
                "Convert '(A OR B) AND C' to DNF or split into UNION.");

    ConditionalExpression* conjuncts[MAX_WHERE_CONJUNCTS];
    Uint32 num_conjuncts = 0;
    flatten_and_conjuncts(d, conjuncts, &num_conjuncts);
    require_prm(num_conjuncts > 0,
                "CTE_LOOKUP filter: empty disjunct after simplification.");

    const bool is_last = (di + 1 == num_disjuncts);
    const Uint32 fail_label = is_last ? REJECT : next_label++;

    for (Uint32 c = 0; c < num_conjuncts; c++) {
      emit_atom(conjuncts[c], fail_label);
    }

    if (!is_last) {
      // All atoms in this disjunct passed → skip the remaining
      // disjuncts and accept.  The next disjunct's atoms are reached
      // by falling through fail_label.
      require_prm(code.branch_label(ACCEPT) == 0,
                  "CTE_LOOKUP filter: failed to emit ACCEPT branch.");
      require_prm(code.def_label(fail_label) == 0,
                  "CTE_LOOKUP filter: failed to define disjunct fail "
                  "label.");
    }
    // On the last disjunct, falling through reaches the ACCEPT label
    // defined just below — no extra branch needed.
  }

  if (num_disjuncts > 1) {
    // ACCEPT is only reached via branch_label from a passing
    // non-last disjunct; for n=1 there is no branch to it and we
    // skip the def_label to keep the program byte-equivalent to the
    // pre-I.2 emission.
    require_prm(code.def_label(ACCEPT) == 0,
                "CTE_LOOKUP filter: def_label(ACCEPT) failed.");
  }
  require_prm(code.interpret_exit_ok() == 0,
              "CTE_LOOKUP filter: interpret_exit_ok failed.");
  require_prm(code.def_label(REJECT) == 0,
              "CTE_LOOKUP filter: def_label(REJECT) failed.");
  require_prm(code.interpret_exit_nok() == 0,
              "CTE_LOOKUP filter: interpret_exit_nok failed.");
  require_prm(code.finalise() == 0,
              "CTE_LOOKUP filter: finalise failed.");
}

// True iff `target_op_idx` is the aggregation leaf itself or one of its
// tree-ancestors (walking tree_parent_op_idx up to the root).  A linked
// projection / interpreted param attached to the agg leaf can only reference
// an ancestor operation: the NDB API serializes it by walking up the parent
// chain (NdbQueryBuilder.cpp appendLinkedOperand), and a non-ancestor (sibling)
// source walks off the root and aborts the server (assert in debug, null deref
// in release).  Used to reject fan-out aggregation across a non-ancestor
// sibling branch cleanly instead of crashing.
static bool
linked_source_is_leaf_ancestor(const JoinPlan& plan, Uint32 leaf_idx,
                               Uint32 target_op_idx)
{
  Uint32 cur = leaf_idx;
  for (Uint32 guard = 0; guard <= plan.num_ops; guard++) {
    if (cur == target_op_idx) return true;
    if (plan.ops[cur].is_root) return false;       // reached root, not found
    Uint32 next = plan.ops[cur].tree_parent_op_idx;
    if (next == cur) return false;                 // self-loop safety
    cur = next;
  }
  return false;                                    // malformed chain — reject
}

// Emit every non-root op in scope.join_plan: linked keys from the parent,
// optional WHERE filter, optional aggregator attachment (multi-leaf if
// leafAggs[i] is non-null, else single-leaf at plan.agg_leaf_idx), and
// dispatch on op.type to readTuple / scanIndex / lookupCte.
void
RonSQLPreparer::emit_child_ops(NdbQueryBuilder* qb, QueryScope& scope,
                                const NdbQueryOperationDef** opDefs,
                                NdbAggregator* singleAgg,
                                NdbAggregator** leafAggs,
                                NdbDictionary::Table** cteVirtualTables)
{
  JoinPlan& plan = scope.join_plan;
  for (Uint32 i = 1; i < plan.num_ops; i++) {
    JoinOp& op = plan.ops[i];
    NdbQueryOptions opts;
    switch (op.match_type) {
    case JoinOp::SEMI_JOIN:
      opts.setMatchType(NdbQueryOptions::MatchNonNull);
      opts.setMatchType(NdbQueryOptions::MatchFirst);
      break;
    case JoinOp::ANTI_JOIN:
      opts.setMatchType(NdbQueryOptions::MatchNullOnly);
      break;
    case JoinOp::LEFT_OUTER:
      // MatchAll is the default (outer join) — no setMatchType needed
      break;
    case JoinOp::INNER:
    default:
      opts.setMatchType(NdbQueryOptions::MatchNonNull);
      break;
    }

    // When the tree parent differs from the key-source parent (chained
    // CTE_LOOKUPs — see QueryPlanner tree_parent_op_idx assignment),
    // explicitly pin the tree parent. The implicit parent from linkedValue
    // would otherwise point at the key source, yielding a sibling-CTE
    // topology that the SPJ protocol rejects.
    if (op.tree_parent_op_idx != op.parent_op_idx) {
      require_run(opts.setParent(opDefs[op.tree_parent_op_idx]) == 0,
                  "Failed to set tree parent override.");
    }

    // Per-key parent sources (non_aggregate_phase_6.md): each key links
    // from its own key-source op.  NdbQueryBuilder's linkWithParent
    // resolves the operation's effective parent to the deepest source —
    // by construction equal to op.parent_op_idx (validated in
    // QueryPlanner::plan) — and a tree-parent pin (chained CTEs) is
    // always at-or-below every key source, so the pin above is never
    // clobbered by the builder's grandparent replacement.
    const NdbQueryOperand* keys[MAX_JOIN_KEY_COLS + 1];
    for (Uint32 k = 0; k < op.num_key_cols; k++) {
      keys[k] = qb->linkedValue(opDefs[op.key_parent_op_idx[k]],
                                 op.parent_key_col_names[k]);
      require_run(keys[k] != NULL, "Failed to create linked value.");
    }
    keys[op.num_key_cols] = nullptr;

    NdbInterpretedCode child_code_storage(op.table);
    NdbInterpretedCode cte_filter_code_storage(
        (op.type == JoinOp::CTE_LOOKUP && cteVirtualTables != NULL)
        ? cteVirtualTables[i] : NULL);
    if (op.type != JoinOp::CTE_LOOKUP && scope.join_where_ce[i] != NULL)
    {
      ConditionalExpression* child_ce = simplify_ce(scope.join_where_ce[i],
                                                    -1);
      NdbScanFilter filter(&child_code_storage);
      filter.setSqlCmpSemantics();
      filter.begin(NdbScanFilter::AND);
      apply_filter(&filter, scope, child_ce);
      filter.end();
      child_code_storage.finalise();
      opts.setInterpretedCode(child_code_storage);
    }
    else if (op.type == JoinOp::CTE_LOOKUP && scope.join_where_ce[i] != NULL)
    {
      // Main-query WHERE conjuncts on this CTE's output columns push down
      // to the CTE_LOOKUP's interpreted-code filter. See
      // cte_filter_plan.md Phase A for the server-side jump-table
      // interpreter that evaluates branch_linked_mem_* instructions
      // against DBLQH's linked-attr buffer.
      require_run(cteVirtualTables != NULL && cteVirtualTables[i] != NULL,
                  "CTE_LOOKUP filter requires a virtual table; missing "
                  "in emit context.");
      emit_cte_lookup_filter(cte_filter_code_storage, scope, i,
                             cteVirtualTables[i], scope.join_where_ce[i]);
      opts.setInterpretedCode(cte_filter_code_storage);
    }

    // Attach aggregator. Multi-leaf path (leafAggs[i]) is for merged
    // select-list subqueries and never fires for CTE ops. Single-leaf
    // path fires when this op is plan.agg_leaf_idx — works for both
    // physical-table leaves and CTE leaves (A0), the latter using the
    // per-CTE virtual table for schema.
    if (plan.num_agg_leaves > 0 && leafAggs != NULL &&
        leafAggs[i] != NULL) {
      require_run(opts.setAggregation(*leafAggs[i]) == 0,
                  "Failed to set aggregation on leaf.");
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
        require_prm(
            linked_source_is_leaf_ancestor(plan, i,
                                           plan.linked_projs[j].source_op_idx),
            "Aggregation references a column from a join branch that is not an "
            "ancestor of the aggregation leaf. Fan-out aggregation across a "
            "non-ancestor sibling (e.g. a real-table child alongside a CTE "
            "lookup under the same parent) is not yet supported.");
        NdbLinkedOperand* lv = qb->linkedValue(
            opDefs[plan.linked_projs[j].source_op_idx],
            plan.linked_projs[j].column_name);
        require_run(lv != NULL, "Failed to create linked projection.");
        require_run(opts.addLinkedProjection(lv) == 0,
                    "Failed to add linked projection.");
      }
    } else if (i == plan.agg_leaf_idx && plan.num_agg_leaves == 0 &&
               singleAgg != NULL) {
      require_run(opts.setAggregation(*singleAgg) == 0,
                  "Failed to set aggregation.");
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
        require_prm(
            linked_source_is_leaf_ancestor(plan, i,
                                           plan.linked_projs[j].source_op_idx),
            "Aggregation references a column from a join branch that is not an "
            "ancestor of the aggregation leaf. Fan-out aggregation across a "
            "non-ancestor sibling (e.g. a real-table child alongside a CTE "
            "lookup under the same parent) is not yet supported.");
        NdbLinkedOperand* lv = qb->linkedValue(
            opDefs[plan.linked_projs[j].source_op_idx],
            plan.linked_projs[j].column_name);
        require_run(lv != NULL, "Failed to create linked projection.");
        require_run(opts.addLinkedProjection(lv) == 0,
                    "Failed to add linked projection.");
      }
    } else if (op.type == JoinOp::CTE_LOOKUP &&
               op.needs_parent_linked_attrs) {
      // i26 (W5): filtered CTE op with ancestor-column atoms but no
      // aggregator here — attach the plan's linked projections so
      // DBLQH prepends them for the jump-table filter (the API
      // serializes addLinkedProjection for CTE_LOOKUP nodes
      // unconditionally; classify_where_by_table guaranteed any
      // single-leaf aggregator sits on THIS op, which the branch
      // above already handled).
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
        require_prm(
            linked_source_is_leaf_ancestor(plan, i,
                                           plan.linked_projs[j].source_op_idx),
            "CTE filter references a column from a join branch that is "
            "not an ancestor of the CTE lookup.");
        NdbLinkedOperand* lv = qb->linkedValue(
            opDefs[plan.linked_projs[j].source_op_idx],
            plan.linked_projs[j].column_name);
        require_run(lv != NULL, "Failed to create linked projection.");
        require_run(opts.addLinkedProjection(lv) == 0,
                    "Failed to add linked projection.");
      }
    }

    switch (op.type) {
    case JoinOp::PK_LOOKUP:
      opDefs[i] = qb->readTuple(op.table, keys, &opts);
      break;
    case JoinOp::UNIQUE_LOOKUP:
      opDefs[i] = qb->readTuple(op.index, op.table, keys, &opts);
      break;
    case JoinOp::INDEX_SCAN:
    {
      if (op.num_low_bounds == 0 && op.num_high_bounds == 0)
      {
        NdbQueryIndexBound bound(keys);
        opDefs[i] = qb->scanIndex(op.index, op.table, &bound, &opts);
      }
      else
      {
        const NdbQueryOperand* lowKeys[MAX_JOIN_KEY_COLS * 2 + 1];
        const NdbQueryOperand* highKeys[MAX_JOIN_KEY_COLS * 2 + 1];

        for (Uint32 k = 0; k < op.num_key_cols; k++) {
          lowKeys[k] = highKeys[k] = qb->linkedValue(
              opDefs[op.key_parent_op_idx[k]], op.parent_key_col_names[k]);
          require_run(lowKeys[k] != NULL, "Failed to create linked value.");
        }

        // child_bounds: a RangeBound with const_cond carries a
        // constant (child-local conjunct) instead of a parent-linked
        // value.  Operands are memoized by conjunct so an EQ pushed to
        // both sides reuses ONE operand — appendBoundPattern then
        // collapses the identical low/high pair to BoundEQ.
        const ConditionalExpression* cb_conds[MAX_JOIN_KEY_COLS * 2];
        const NdbQueryOperand* cb_operands[MAX_JOIN_KEY_COLS * 2];
        Uint32 num_cb_operands = 0;
        auto bound_operand =
            [&](const JoinOp::RangeBound& rb) -> const NdbQueryOperand* {
          if (rb.const_cond == NULL) {
            return qb->linkedValue(opDefs[rb.parent_op_idx],
                                   rb.parent_col_name);
          }
          for (Uint32 m = 0; m < num_cb_operands; m++) {
            if (cb_conds[m] == rb.const_cond) return cb_operands[m];
          }
          const NdbDictionary::Column* bcol =
              op.table->getColumn(rb.child_col_name);
          require_run(bcol != NULL, "Unknown bound column.");
          raw_value rv = encode_constant(rb.const_cond->args.right, bcol);
          const NdbQueryOperand* cv = qb->constValue(rv.val, rv.len);
          ndbrequire(num_cb_operands < MAX_JOIN_KEY_COLS * 2);
          cb_conds[num_cb_operands] = rb.const_cond;
          cb_operands[num_cb_operands] = cv;
          num_cb_operands++;
          return cv;
        };

        bool lowIncl = true;
        Uint32 lowIdx = op.num_key_cols;
        for (Uint32 b = 0; b < op.num_low_bounds; b++) {
          lowKeys[lowIdx] = bound_operand(op.low_bounds[b]);
          require_run(lowKeys[lowIdx] != NULL,
                      "Failed to create lower bound operand.");
          lowIncl = op.low_bounds[b].inclusive;
          lowIdx++;
        }
        lowKeys[lowIdx] = nullptr;

        bool highIncl = true;
        Uint32 highIdx = op.num_key_cols;
        for (Uint32 b = 0; b < op.num_high_bounds; b++) {
          highKeys[highIdx] = bound_operand(op.high_bounds[b]);
          require_run(highKeys[highIdx] != NULL,
                      "Failed to create upper bound operand.");
          highIncl = op.high_bounds[b].inclusive;
          highIdx++;
        }
        highKeys[highIdx] = nullptr;

        NdbQueryIndexBound bound(lowKeys, lowIncl, highKeys, highIncl);
        opDefs[i] = qb->scanIndex(op.index, op.table, &bound, &opts);
      }
      break;
    }
    case JoinOp::CTE_LOOKUP:
    {
      // Phase I.20: the virtual CTE primary key is the CTE body's
      // GROUP BY column list, in GROUP BY order.  Validate both
      // column identity and order; count alone is not enough.
      CteKeyCoverageResult coverage;
      require_prm(cte_key_coverage(op.cte_def,
                                   op.child_key_col_names,
                                   op.num_key_cols,
                                   coverage),
                  "Could not derive CTE virtual primary key columns.");
      require_prm(coverage.state != CteKeyCoverage::WrongColumns,
                  "CTE lookup key references a CTE output column that "
                  "is not part of the virtual primary key.  The virtual "
                  "CTE primary key matches the CTE body's GROUP BY "
                  "column list.");
      require_prm(coverage.state != CteKeyCoverage::Partial,
                  "Partial CTE lookup key not supported.  The "
                  "virtual CTE primary key matches the CTE body's "
                  "GROUP BY column list and the join must bind "
                  "every key column.  Workaround: place the "
                  "multi-key CTE on the joined root and join the "
                  "smaller table to it.");
      require_prm(coverage.state != CteKeyCoverage::ExactPermuted,
                  "CTE lookup keys were not ordered by the virtual "
                  "primary key.  Please report a bug.");
      Uint32 numResultCols = 0;
      for (const Outputs* o = op.cte_def->stmt->outputs; o; o = o->next)
        numResultCols++;
      // Phase I.17: scalar CTE cross-join child.  When both the join
      // key count and the virtual PK count are zero, this is a
      // cross-join over a scalar (no-GROUP-BY) CTE.  Per the
      // testCteNdbApi.cpp Test 20 pattern, lookupCte requires a
      // non-empty key array; the kernel ignores the key for scalar
      // CTEs and returns the materialised m_agg_results directly.
      // setParent establishes the cross-join dependency since there
      // is no linkedValue connecting the parent to the child.
      const NdbQueryOperand* effective_keys[2];
      const NdbQueryOperand** keys_to_use = keys;
      if (coverage.state == CteKeyCoverage::ScalarDummy) {
        // Phase 4 (W2): parent on tree_parent_op_idx, not
        // parent_op_idx — identical for every 2-CTE shape, but with
        // three or more comma-joined scalar CTEs the planner chains
        // the sibling CTE_LOOKUPs via tree_parent_op_idx (SPJ rejects
        // un-chained sibling CTEs) and setParent(root) would clobber
        // that chain.
        require_run(opts.setParent(opDefs[op.tree_parent_op_idx]) == 0,
                    "Failed to set parent for scalar CTE cross-join.");
        // D8: the scalar CTE's virtual PK is its FIRST output column (I.21),
        // whose type is the derived aggregate type — Bigint, Bigunsigned,
        // Double (e.g. MIN/MAX over a scale>0 DECIMAL or FLOAT/DOUBLE widens to
        // Double), or a string.  lookupCte type-checks the key operand against
        // that PK column, so a fixed Int64 zero fails for a non-Bigint PK
        // (returns NULL with no NDB error -> "Failed to create child
        // operation").  The kernel ignores the key value for a scalar CTE, so
        // only the type/size must match — build a zero of the PK column type.
        const NdbDictionary::Column* pkc = cteVirtualTables[i]->getColumn(0);
        require_run(pkc != NULL,
                    "Scalar CTE virtual table has no primary-key column.");
        switch (pkc->getType()) {
        case NdbDictionary::Column::Bigint:
          effective_keys[0] = qb->constValue((Int64)0);
          break;
        case NdbDictionary::Column::Bigunsigned:
          effective_keys[0] = qb->constValue((Uint64)0);
          break;
        case NdbDictionary::Column::Double:
          effective_keys[0] = qb->constValue((double)0);
          break;
        default: {
          // String (Char/Varchar/Longvarchar) or any other PK type: a
          // zero buffer sized to the PK column (value ignored by the kernel).
          const Uint32 pksz = pkc->getSizeInBytes();
          Uint8* zero = m_amalloc->alloc_exc<Uint8>(pksz == 0 ? 1 : pksz);
          memset(zero, 0, pksz);
          effective_keys[0] =
              qb->constValue(static_cast<const void*>(zero), pksz);
          break;
        }
        }
        require_run(effective_keys[0] != NULL,
                    "Failed to create dummy scalar-CTE lookup key.");
        effective_keys[1] = nullptr;
        keys_to_use = effective_keys;
      }
      if (coverage.state == CteKeyCoverage::SingleRowSubset) {
        // Single-row CTE (cte_single_row_kernel_plan.md): the keys
        // bind ANY SUBSET of the outputs.  Declare the bound output
        // positions so lookupCte binds each key operand to its
        // projected column and the kernel compares at those stamped
        // positions.  Zero keys = comma cross join = pure existence
        // probe: no key operands at all (no dummy key), parent
        // established via setParent on the tree parent (the
        // ScalarDummy sibling-chaining rule).
        Uint32 positions[MAX_JOIN_KEY_COLS];
        for (Uint32 k = 0; k < op.num_key_cols; k++) {
          require_run(coverage.pk_index_for_key[k] >= 0,
                      "Single-row CTE key did not resolve to an "
                      "output column.");
          positions[k] = (Uint32)coverage.pk_index_for_key[k];
        }
        require_run(opts.setCteKeyColumns(positions,
                                          op.num_key_cols) == 0,
                    "Failed to declare single-row CTE key columns.");
        if (op.num_key_cols == 0) {
          require_run(opts.setParent(opDefs[op.tree_parent_op_idx]) == 0,
                      "Failed to set parent for single-row CTE "
                      "cross-join.");
          effective_keys[0] = nullptr;
          keys_to_use = effective_keys;
        }
      }
      opDefs[i] = qb->lookupCte(
          op.cte_def_idx, numResultCols,
          cteVirtualTables[i],
          keys_to_use, &opts);
      break;
    }
    case JoinOp::CTE_SCAN:
    {
      // scanCte takes (cteId, numCols, virtTab, options) — no keys[].
      // If the planner picked CTE_SCAN as a non-root child WITH linked
      // keys, that's a shape scanCte doesn't support — reject cleanly
      // rather than emit incorrect code.  The parent->child join must
      // express its dependency via aggregator linked-loads instead.
      require_run(op.num_key_cols == 0,
                  "CTE_SCAN as join child with linked keys is not "
                  "supported (use CTE_LOOKUP for keyed access).");
      require_run(cteVirtualTables != NULL && cteVirtualTables[i] != NULL,
                  "CTE_SCAN as join child requires a virtual table.");
      Uint32 numResultCols = 0;
      for (const Outputs* o = op.cte_def->stmt->outputs; o; o = o->next)
        numResultCols++;
      opDefs[i] = qb->scanCte(op.cte_def_idx, numResultCols,
                               cteVirtualTables[i], &opts);
      break;
    }
    default:
      abort();
    }
    if (opDefs[i] == NULL) {
      // NdbQueryBuilder reports QRY_UNRELATED_INDEX when the index's
      // recorded m_table_id / m_table_version disagree with the table's
      // current ObjectId / ObjectVersion — a stale-index condition that
      // arises after a concurrent DROP INDEX / CREATE INDEX on a joined
      // child table, when the dict cache holds a stale index against a
      // freshly reloaded table.  The builder classifies it as
      // ApplicationError (not SchemaError), so the generic SRE handler
      // would treat it as permanent and skip retry.  Route through
      // RonSQLMaybeStaleSchema so handle_ronsql_exception calls
      // unload_schema(); if the schema actually changed, the next retry's
      // RonSQLPreparer.load() picks up consistent table + index objects.
      const NdbError& qb_err = qb->getNdbError();
      if (qb_err.code == 4809 /* QRY_UNRELATED_INDEX */) {
        throw RonSQLMaybeStaleSchema("Failed to create child operation"
                                     " (index/table version mismatch).");
      }
    }
    require_run(opDefs[i] != NULL, "Failed to create child operation.");
  }
}

std::ostream& operator<<(std::ostream& os, const NdbError& err) {
  const char* err_status_msg =
    ndberror_status_message((ndberror_status)err.status);
  const char* err_class_msg =
    ndberror_classification_message((ndberror_classification)
                                    err.classification);
  os << "NDB " << err_status_msg << " " << err.code << ", " << err_class_msg;
  if (err.mysql_code != -1) {
    os << ", MySQL " << err.mysql_code;
  }
  if (err.message != NULL) {
    os << ": " << err.message;
  }
  if (err.details != NULL) {
    os << " (" << err.details << ")";
  }
  return os;
}

void
RonSQLPreparer::handle_ronsql_exception(std::exception_ptr eptr) {
  std::basic_ostream<char>& err = *m_conf.err_stream;
  try {
    std::rethrow_exception(eptr);
  }
  catch (const RonSQLRetryableError& e) {
    // This exception type inherits from std::runtime_error, but we don't want
    // it caught below.
    DEB_TRACE(); err << "Error handling: RRE\n";
    cleanup_trans();
    throw;
  }
  catch (const RonSQLPermanentError& e) {
    // This exception type inherits from std::runtime_error, but we don't want
    // it caught below.
    DEB_TRACE(); err << "Error handling: RPE\n";
    cleanup_trans();
    throw;
  }
  catch (const RonSQLMaybeStaleSchema& e) {
    // Attempt to unload then reload the schema and detect whether the version
    // changed. If so, rethrow as RonSQLRetryableError, otherwise as
    // RonSQLPermanentError.
    cleanup_trans();
    err << "Error handling: RMS";
    if (unload_schema()) {
      DEB_TRACE(); err << "->RRE\n";
      throw RonSQLRetryableError(e.what());
    }
    DEB_TRACE(); err << "->RPE\n";
    throw RonSQLPermanentError(e.what());
  }
  catch (const std::bad_alloc&)
  {
    // Out of memory while processing the query (ArenaMalloc::alloc_exc /
    // realloc_exc, or any other std::bad_alloc on this query's stack).
    // Roll back — cleanup_trans() releases the NDB transaction, and the
    // caller frees this query's arena once the failure propagates — then
    // fail the whole query with a clean permanent error.  Without this arm
    // std::bad_alloc would fall to the catch(...) below and abort().  Note
    // it must precede the std::runtime_error arm in intent: std::bad_alloc
    // is NOT a std::runtime_error, so it would otherwise reach catch(...).
    DEB_TRACE(); err << "Error handling: OOM->RPE\n";
    cleanup_trans();
    throw RonSQLPermanentError("Out of memory while processing the query.");
  }
  catch (const std::runtime_error& e)
  {
    Ndb* ndb = m_conf.ndb;
    err << "Error handling: SRE";
    // Fetch error
    NdbError ndb_err;
    if (m_trans != NULL) {
      DEB_TRACE(); err << ",te";
      ndbrequire(ndb != NULL);
      ndb_err = m_trans->getNdbError();
    } else if (ndb != NULL) {
      DEB_TRACE(); err << ",oe";
      ndb_err = DBG(ndb->getNdbError());
    } else {
      DEB_TRACE(); err << ",nn\n";
      cleanup_trans();
      throw RonSQLPermanentError("No NDB object");
    }
    cleanup_trans();
    bool temporary = false;
    // Decide whether to unload and whether the error is temporary
    if (ndb_err.classification == NdbError::Classification::SchemaError) {
      DEB_TRACE(); err << ",rl";
      // Try to reload the schema. If changes were detected then we treat the
      // error as temporary.
      temporary = unload_schema();
    } else if (ndb_err.status == NdbError::Status::TemporaryError ||
               ndb_err.mysql_code == HA_ERR_LOCK_WAIT_TIMEOUT) {
      DEB_TRACE(); err << ",nu";
      // Treat error as temporary, no unloading of schema.
      temporary = true;
    }
    // Now that the ndb error is described on err stream, we'll rethrow the
    // exception with a new error type.
    if (temporary) {
      DEB_TRACE();
      err << "->RRE\n" << ndb_err << '\n';
      throw RonSQLRetryableError(e.what());
    }
    DEB_TRACE();
    err << "->RPE\n" << ndb_err << '\n';
    throw RonSQLPermanentError(e.what());
  }
  catch (...) {
    // All exceptions thrown should be instances of runtime_error.
    DEB_TRACE();
    abort();
  }
}

/* Unload table and indexes, then detect schema changes.
   Return true if any schema change was detected.
 */
bool
RonSQLPreparer::unload_schema() {
  DEB_TRACE();
  Ndb* ndb = m_conf.ndb;
  if (ndb == NULL) {
    DEB_TRACE();
    return false;
  }
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const char* db = ndb->getDatabaseName();

  struct IndexName {
    std::string index;
    std::string table;
  };
  std::vector<std::string> table_names;
  std::vector<IndexName> index_names;
  auto add_table_name = [&table_names](const char* name) {
    if (name == NULL || name[0] == '\0') return;
    std::string s(name);
    bool seen =
      std::find(table_names.begin(), table_names.end(), s) !=
      table_names.end();
    if (!seen) {
      table_names.push_back(std::move(s));
    }
  };
  auto add_table = [&add_table_name](const NdbDictionary::Table* table) {
    if (table != NULL) add_table_name(table->getName());
  };
  auto add_index = [&index_names](const NdbDictionary::Index* index,
    const char* table_name) {
    if (index == NULL || table_name == NULL || table_name[0] == '\0')
      return;
    std::string idx(index->getName());
    std::string tab(table_name);
    for (const IndexName& seen : index_names) {
      if (seen.index == idx && seen.table == tab) return;
    }
    index_names.push_back({std::move(idx), std::move(tab)});
  };
  auto add_scope = [&add_table, &add_index](const QueryScope& scope) {
    add_table(scope.table);
    for (Uint32 i = 0; i < scope.body_indexes.size(); i++) {
      const char* table_name = NULL;
      if (scope.table != NULL) table_name = scope.table->getName();
      add_index(scope.body_indexes[i], table_name);
    }
    for (Uint32 i = 0; i < scope.join_plan.num_ops; i++) {
      const JoinOp& op = scope.join_plan.ops[i];
      add_table(op.table);
      const char* table_name = op.table != NULL ? op.table->getName() : NULL;
      add_index(op.index, table_name);
    }
  };
  add_scope(m_main_scope);
  for (Uint32 i = 0; i < m_indexes.size(); i++) {
    const char* table_name = NULL;
    if (m_main_scope.table != NULL)
      table_name = m_main_scope.table->getName();
    add_index(m_indexes[i], table_name);
  }
  for (Uint32 i = 0; i < m_cte_scopes.size(); i++) {
    if (m_cte_scopes[i] != NULL) add_scope(*m_cte_scopes[i]);
  }
  auto invalidate_collected_schema = [&]() {
    for (const IndexName& name : index_names) {
      dict->invalidateIndex(name.index.c_str(), name.table.c_str());
    }
    for (const std::string& name : table_names) {
      if (m_conf.schema_cache != NULL && db != NULL) m_conf.schema_cache->invalidate(db, name);
      dict->invalidateTable(name.c_str());
    }
  };
#define RETURN_UNLOAD_SCHEMA(value) \
  do {                              \
    invalidate_collected_schema();  \
    return (value);                 \
  } while (0)

  /*
   * Join-aware version walk (the tracked defect in
   * findings/root_pk_residual.md): the single-table comparison below
   * reads only m_main_scope.table + m_indexes, and m_indexes is
   * populated only by load_single_table — so on the join path
   * old_indexes_count was always 0 and any online ordered index on the
   * root made the reload's count check report "schema changed",
   * turning EVERY join-path RonSQLMaybeStaleSchema into a retryable
   * error (10 doomed RDRS attempts / 3 CLI attempts per query).
   * Instead, snapshot the (id, version) of every dictionary object the
   * query actually HOLDS — the same enumeration the invalidation
   * lambdas above walk: each scope's root table, every join-plan op's
   * table and index, every body_indexes entry, across the main scope
   * and all CTE scopes (CTE ops have NULL tables and synthetic virtual
   * tables never appear here) — then invalidate everything, reload by
   * name, and compare.
   *
   * Scope note: only HELD objects are compared, so a newly ADDED index
   * on a join table is not detected.  That is fine: the join-path "no
   * suitable index" errors are RonSQLPermanentError (never
   * RonSQLMaybeStaleSchema), so an added-index retry was never
   * reachable here.  This branch also covers CTE-root queries, whose
   * child ops' real tables the old NULL-root short-circuit ignored.
   */
  if (is_join_query()) {
    typedef std::pair<int, int> Idver;
    struct OldTable { std::string name; Idver idver; };
    struct OldIndex { std::string index; std::string table; Idver idver; };
    std::vector<OldTable> old_tables;
    std::vector<OldIndex> old_indexes;
    bool changed = false;
    auto snap_table = [&](const NdbDictionary::Table* t) {
      if (t == NULL) return;
      std::string n(DBG(t->getName()));
      for (const OldTable& o : old_tables) {
        if (o.name == n) return;
      }
      if (t->getObjectStatus() !=
          NdbDictionary::Object::Status::Retrieved) {
        changed = true;
      }
      old_tables.push_back({ std::move(n),
                             { DBG(t->getObjectId()),
                               DBG(t->getObjectVersion()) } });
    };
    auto snap_index = [&](const NdbDictionary::Index* ix,
                          const NdbDictionary::Table* t) {
      if (ix == NULL || t == NULL) return;
      std::string in(DBG(ix->getName()));
      std::string tn(t->getName());
      for (const OldIndex& o : old_indexes) {
        if (o.index == in && o.table == tn) return;
      }
      if (ix->getObjectStatus() !=
          NdbDictionary::Object::Status::Retrieved) {
        changed = true;
      }
      // Cross-check: the index's recorded owning-table id/version must
      // match the table object we hold (the single-table
      // table_idver_mismatch check, per index here).
      const Idver owning = { (int)DBG(ix->getTableId()),
                             (int)DBG(ix->getTableVersion()) };
      const Idver held = { t->getObjectId(), t->getObjectVersion() };
      if (owning != held) changed = true;
      old_indexes.push_back({ std::move(in), std::move(tn),
                              { DBG(ix->getObjectId()),
                                DBG(ix->getObjectVersion()) } });
    };
    auto snap_scope = [&](const QueryScope& scope) {
      snap_table(scope.table);
      for (Uint32 i = 0; i < scope.body_indexes.size(); i++) {
        snap_index(scope.body_indexes[i], scope.table);
      }
      for (Uint32 i = 0; i < scope.join_plan.num_ops; i++) {
        const JoinOp& op = scope.join_plan.ops[i];
        snap_table(op.table);
        snap_index(op.index, op.table);
      }
    };
    snap_scope(m_main_scope);
    for (Uint32 i = 0; i < m_cte_scopes.size(); i++) {
      if (m_cte_scopes[i] != NULL) snap_scope(*m_cte_scopes[i]);
    }

    // Invalidate EVERYTHING before reloading: the single-table path
    // below invalidates only the root pre-reload, but here a child
    // table's getTable() would otherwise return the stale cached
    // object.  The RETURN macro invalidates again afterwards —
    // idempotent, and it preserves the leave-everything-invalidated
    // contract for the retry's fresh preparer.
    invalidate_collected_schema();

    for (const OldTable& ot : old_tables) {
      if (changed) break;
      const NdbDictionary::Table* nt =
          DBG(dict->getTable(ot.name.c_str()));
      if (nt == NULL) { changed = true; break; }
      const Idver nv = { nt->getObjectId(), nt->getObjectVersion() };
      if (nv != ot.idver) changed = true;
    }
    for (const OldIndex& oi : old_indexes) {
      if (changed) break;
      const NdbDictionary::Table* nt = dict->getTable(oi.table.c_str());
      if (nt == NULL) { changed = true; break; }
      const NdbDictionary::Index* ni =
          DBG(dict->getIndex(oi.index.c_str(), *nt));
      if (ni == NULL) { changed = true; break; }
      const Idver nv = { ni->getObjectId(), ni->getObjectVersion() };
      if (nv != oi.idver) changed = true;
    }
    DEB_TRACE();
    RETURN_UNLOAD_SCHEMA(changed);
  }

  // Non-join queries with no root table (defensive — load_single_table
  // either sets it or throws): treat as "no schema change detected" so
  // the caller falls back to a permanent error.
  if (m_main_scope.table == NULL) {
    DEB_TRACE();
    RETURN_UNLOAD_SCHEMA(false);
  }
  // Save table object ID and version
  typedef std::pair<int, int> Idver;
  const Idver old_table_idver = { DBG(m_main_scope.table->getObjectId()),
                                  DBG(m_main_scope.table->getObjectVersion()) };
  // Unload indexes, saving their object IDs and versions in a sorted list
  bool table_idver_mismatch = false;
  const Uint32 old_indexes_count = DBG(m_indexes.size());
  Idver* old_indexes_idver = m_amalloc->alloc_exc<Idver>(old_indexes_count);
  bool some_old_indexes_not_retrieved = false;
  for (Uint32 i = 0; i < old_indexes_count; i++) {
    DEB_TRACE();
    const NdbDictionary::Index* old_index = m_indexes[i];
    ndbrequire(old_index != NULL);
    old_indexes_idver[i] = { DBG(old_index->getObjectId()),
                             DBG(old_index->getObjectVersion()) };
    some_old_indexes_not_retrieved = some_old_indexes_not_retrieved ||
      (old_index->getObjectStatus() !=
       NdbDictionary::Object::Status::Retrieved);
    const Idver table_idver_according_to_old_index = {
      DBG(old_index->getTableId()),
      DBG(old_index->getTableVersion()) };
    table_idver_mismatch = table_idver_mismatch ||
      (old_table_idver != table_idver_according_to_old_index);
    dict->invalidateIndex(old_index);
    m_indexes[i] = NULL;
  }
  std::sort(old_indexes_idver, old_indexes_idver + old_indexes_count);
  // Unload table
  dict->invalidateTable(m_main_scope.table);
  m_main_scope.table = NULL;
  if (table_idver_mismatch) {
    // We don't need to reload table or indexes, since we have already
    // determined an inconsistency. This inconsistency should go away next time
    // we load metadata, and this is checked in RonSQLPreparer::load().
    DEB_TRACE();
    RETURN_UNLOAD_SCHEMA(true);
  }
  // Reload table
  const NdbDictionary::Table* new_table =
    DBG(dict->getTable(DBG(m_context.ast_root.table.c_str())));
  if (new_table == NULL) {
    // We don't need to reload indexes, since we have already determined a
    // schema change.
    DEB_TRACE();
    RETURN_UNLOAD_SCHEMA(true);
  }
  require_prm(DBG(new_table->getObjectStatus()) ==
              NdbDictionary::Object::Status::Retrieved,
              "Reloaded table not in Retrieved status.");
  const Idver new_table_idver = { DBG(new_table->getObjectId()),
                                  DBG(new_table->getObjectVersion()) };
  if (new_table_idver != old_table_idver) {
    // We don't need to reload indexes, since we have already determined a
    // schema change.
    DEB_TRACE();
    RETURN_UNLOAD_SCHEMA(true);
  }
  // Reload indexes
  // Match logic in load(): Load online ordered indexes
  NdbDictionary::Dictionary::List index_list;
  require_prm(dict->listIndexes(index_list, *new_table) == 0,
              "Failed to list indexes while reloading schema.");
  Uint32 new_indexes_count = 0;
  Idver* new_indexes_idver = m_amalloc->alloc_exc<Idver>(old_indexes_count);
  for (Uint32 i = 0; i < index_list.count; i++) {
    NdbDictionary::Dictionary::List::Element& list_element =
      index_list.elements[i];
    if (DBG(list_element.state) != NdbDictionary::Object::StateOnline ||
        DBG(list_element.type) == NdbDictionary::Object::UniqueHashIndex) {
      DEB_TRACE();
      continue;
    }
    require_bug(list_element.type == NdbDictionary::Object::OrderedIndex,
                "Unexpected index type.");
    const NdbDictionary::Index* new_index = dict->getIndex(list_element.name,
                                                           *new_table);
    require_prm(new_index != NULL,
                "Failed to get index while reloading schema.");
    require_prm(DBG(new_index->getObjectStatus()) ==
                NdbDictionary::Object::Status::Retrieved,
                "Reloaded index not in Retrieved status.");
    if (new_indexes_count >= old_indexes_count) {
      // Number of indexes changed, so a schema change must have occurred.
      DEB_TRACE();
      RETURN_UNLOAD_SCHEMA(true);
    }
    new_indexes_idver[new_indexes_count++] = {
      DBG(new_index->getObjectId()),
      DBG(new_index->getObjectVersion()) };
    const Idver table_idver_according_to_index = {
      DBG(new_index->getTableId()),
      DBG(new_index->getTableVersion()) };
    require_prm(new_table_idver == table_idver_according_to_index,
                "Index's table id/version did not match table's object"
                " id/version, while reloading schema.");
  }
  if (new_indexes_count != old_indexes_count) {
    // Number of indexes changed, so a schema change must have occurred.
    DEB_TRACE();
    RETURN_UNLOAD_SCHEMA(true);
  }
  std::sort(new_indexes_idver, new_indexes_idver + new_indexes_count);
  for (Uint32 i = 0; i < new_indexes_count; i++) {
    if (new_indexes_idver[i] != old_indexes_idver[i]) {
      // Object ID or version changed for this index, so a schema change
      // must have occurred.
      DEB_TRACE();
      RETURN_UNLOAD_SCHEMA(true);
    }
  }
  DEB_TRACE();
  RETURN_UNLOAD_SCHEMA(some_old_indexes_not_retrieved);
#undef RETURN_UNLOAD_SCHEMA
}

constexpr const char* filter_fail = "Failed to apply filter.";

void
RonSQLPreparer::apply_filter_top_level(NdbScanFilter* filter)
{
  /* ndbapi filter has unary AND and OR operators, i.e. they take an arbitrary
   * number of operands. In a number of places, it is required to have at least
   * one "group", i.e. containing AND or OR expression, active. We wrap our
   * array of top-level conditions in an AND group. Technically this is
   * unnecessary in the special case of exactly one condition of type T_AND or
   * T_OR.
   */
  require_tmp(DBG(filter->begin(NdbScanFilter::AND)) >= 0, filter_fail);
  bool has_filter = false;
  for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
    if (m_scan_config->condition_handling_map[i] == -1) {
      has_filter = true;
      apply_filter(filter, m_main_scope, m_toplevel_conditions[i]);
    }
  }
  ndbrequire(has_filter);
  require_sch(DBG(filter->end()) >= 0, filter_fail);
}

void
RonSQLPreparer::apply_filter(NdbScanFilter* filter, QueryScope& scope,
                             struct ConditionalExpression* ce)
{
  ndbrequire(ce != NULL);
  switch (ce->op)
  {
  case T_OR:
    require_tmp(DBG(filter->begin(NdbScanFilter::OR)) >= 0, filter_fail);
    apply_filter(filter, scope, ce->args.left);
    apply_filter(filter, scope, ce->args.right);
    require_sch(DBG(filter->end()) >= 0, filter_fail);
    break;
  case T_XOR:
    abort(); // This should have been "simplified" away
  case T_AND:
    require_tmp(DBG(filter->begin(NdbScanFilter::AND)) >= 0, filter_fail);
    apply_filter(filter, scope, ce->args.left);
    apply_filter(filter, scope, ce->args.right);
    require_sch(DBG(filter->end()) >= 0, filter_fail);
    break;
  case T_NOT:
    require_tmp(DBG(filter->begin(NdbScanFilter::NAND)) >= 0, filter_fail);
    apply_filter(filter, scope, ce->args.left);
    require_sch(DBG(filter->end()) >= 0, filter_fail);
    break;
  case T_EQUALS:
    apply_filter_cmp(filter, scope, NdbScanFilter::COND_EQ,
                     ce->args.left, ce->args.right);
    break;
  case T_GE:
    apply_filter_cmp(filter, scope, NdbScanFilter::COND_GE,
                     ce->args.left, ce->args.right);
    break;
  case T_GT:
    apply_filter_cmp(filter, scope, NdbScanFilter::COND_GT,
                     ce->args.left, ce->args.right);
    break;
  case T_LE:
    apply_filter_cmp(filter, scope, NdbScanFilter::COND_LE,
                     ce->args.left, ce->args.right);
    break;
  case T_LT:
    apply_filter_cmp(filter, scope, NdbScanFilter::COND_LT,
                     ce->args.left, ce->args.right);
    break;
  case T_NOT_EQUALS:
    apply_filter_cmp(filter, scope, NdbScanFilter::COND_NE,
                     ce->args.left, ce->args.right);
    break;
  case T_LIKE:
    apply_filter_like(filter, scope, NdbScanFilter::COND_LIKE,
                      ce->args.left, ce->args.right);
    break;
  case T_IS:
    // D10: `col IS NULL` / `col IS NOT NULL` in a (CTE-body or top-level)
    // WHERE.  NdbScanFilter::isnull / isnotnull lower this directly; the
    // same predicate on a main-query CTE_LOOKUP output is handled
    // separately via branch_linked_isnull (Phase I.1).
    apply_filter_isnull(filter, scope, ce);
    break;
  default:
    throw RonSQLPermanentError("Non-boolean term in WHERE condition");
  }
}

void
RonSQLPreparer::apply_filter_cmp(NdbScanFilter* filter,
                                 QueryScope& scope,
                                 NdbScanFilter::BinaryCondition cond,
                                 struct ConditionalExpression* left,
                                 struct ConditionalExpression* right)
{
  if (left->op != T_IDENTIFIER) {
    throw RonSQLPermanentError("For comparison operators, at least one of the"
                               " operands must be a column name");
  }
  require_run(scope.resolved_columns != NULL,
              "WHERE filter comparison: missing resolved columns.");
  const QueryScope::ResolvedColumnRef& left_ref =
      scope.resolved_columns[left->col_idx];
  require_prm(left_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
              "WHERE filter comparison requires a stored-table column.");
  if (right->op == T_IDENTIFIER) {
    const QueryScope::ResolvedColumnRef& right_ref =
        scope.resolved_columns[right->col_idx];
    require_prm(
        right_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
        "WHERE filter column-vs-column comparison requires stored-table "
        "columns.");
    require_sch(DBG(filter->cmp(DBG(cond),
                                DBG(left_ref.attr_id),
                                DBG(right_ref.attr_id))) >= 0,
                filter_fail);
    return;
  }
  require_prm(left_ref.dict_column != NULL,
              "WHERE filter comparison has no source column descriptor.");
  raw_value rv = encode_constant(right, left_ref.dict_column);
  require_sch(DBG(filter->cmp(DBG(cond),
                              DBG(left_ref.attr_id),
                              DBG(rv).val,
                              rv.len)) >= 0,
              filter_fail);
}

void
RonSQLPreparer::apply_filter_like(NdbScanFilter* filter,
                                   QueryScope& scope,
                                   NdbScanFilter::BinaryCondition cond,
                                   struct ConditionalExpression* left,
                                   struct ConditionalExpression* right)
{
  if (left->op != T_IDENTIFIER) {
    throw RonSQLPermanentError("LIKE requires a column name on the left side");
  }
  if (right->op != T_STRING) {
    throw RonSQLPermanentError("LIKE requires a string pattern on the right side");
  }
  require_run(scope.resolved_columns != NULL,
              "WHERE LIKE filter: missing resolved columns.");
  const QueryScope::ResolvedColumnRef& left_ref =
      scope.resolved_columns[left->col_idx];
  require_prm(left_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
              "WHERE LIKE filter requires a stored-table column.");
  require_sch(DBG(filter->cmp(cond,
                               left_ref.attr_id,
                               right->string.str,
                               right->string.len)) >= 0,
              filter_fail);
}

void
RonSQLPreparer::apply_filter_isnull(NdbScanFilter* filter,
                                    QueryScope& scope,
                                    struct ConditionalExpression* ce)
{
  // ce->op == T_IS; ce->is.null is true for `IS NULL`, false for
  // `IS NOT NULL`; ce->is.arg is the operand (must be a column).
  struct ConditionalExpression* arg = ce->is.arg;
  if (arg == NULL || arg->op != T_IDENTIFIER) {
    throw RonSQLPermanentError("IS NULL / IS NOT NULL requires a column name "
                               "as its operand");
  }
  require_run(scope.resolved_columns != NULL,
              "WHERE IS NULL filter: missing resolved columns.");
  const QueryScope::ResolvedColumnRef& ref =
      scope.resolved_columns[arg->col_idx];
  require_prm(ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
              "WHERE IS NULL / IS NOT NULL requires a stored-table column.");
  if (ce->is.null) {
    require_sch(DBG(filter->isnull(ref.attr_id)) >= 0, filter_fail);
  } else {
    require_sch(DBG(filter->isnotnull(ref.attr_id)) >= 0, filter_fail);
  }
}

void
rondb_str_to_mysql_time(MYSQL_TIME *mt, LexString str) {
  my_time_flags_t flags = 0; // todo
  MYSQL_TIME_STATUS status;
  bool err = str_to_datetime(str.str, str.len, mt, flags, &status);
  if (unlikely(err)) {
    throw RonSQLMaybeStaleSchema("Failed to interpret string literal as a date"
                                 " or timestamp");
  }
  if (status.warnings) {
    throw RonSQLMaybeStaleSchema("String literal interpreted as date or"
                                 " timestamp with warnings");
  }
  if (status.m_deprecation.m_kind !=
      MYSQL_TIME_STATUS::DEPRECATION::DEPR_KIND::DP_NONE) {
    throw RonSQLMaybeStaleSchema("String literal interpreted as date or"
                                 " timestamp with weird delimiters or spaces");
  }
  // todo test nanosecond rounding, see status->nanoseconds
}

raw_value
RonSQLPreparer::encode_constant(struct ConditionalExpression *ce,
                                const NdbDictionary::Column* col) {
  static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
                "RonSQLPreparer::encode_constant assumes little endian architecture.");
  TokenKind op = ce->op;
  Int64 min = 0, max = 0, bytes = 0;
  int maxlen = 0, lenbytes = 0;
  Uint32 binlen = 0;
  enum_mysql_timestamp_type timetype = MYSQL_TIMESTAMP_NONE;
  int tk = 0;
  const int INT = 1, FLOAT = 2, DECIMAL = 3, STR = 4, TIME = 5;
  NdbDictionary::Column::Type type = col->getType();
  switch (type) {
  case NdbDictionary::Column::Type::Tinyint:
    tk = INT; min = -128; max = 127; bytes = 1; break;
  case NdbDictionary::Column::Type::Tinyunsigned:
    tk = INT; min = 0; max = 255; bytes = 1; break;
  case NdbDictionary::Column::Type::Smallint:
    tk = INT; min = -32768; max = 32767; bytes = 2; break;
  case NdbDictionary::Column::Type::Smallunsigned:
    tk = INT; min = 0; max = 65535; bytes = 2; break;
  case NdbDictionary::Column::Type::Mediumint:
    tk = INT; min = -8388608; max = 8388607; bytes = 3; break;
  case NdbDictionary::Column::Type::Mediumunsigned:
    tk = INT; min = 0; max = 16777215; bytes = 3; break;
  case NdbDictionary::Column::Type::Int:
    tk = INT; min = INT32_MIN; max = INT32_MAX; bytes = 4; break;
  case NdbDictionary::Column::Type::Unsigned:
    tk = INT; min = 0; max = 4294967295LL; bytes = 4; break;
  case NdbDictionary::Column::Type::Bigint:
    tk = INT; min = INT64_MIN; max = INT64_MAX; bytes = 8; break;
  case NdbDictionary::Column::Type::Bigunsigned:
    // Values greater than INT64_MAX can be represented by Bigunsigned but not
    // by ConditionalExpression::integer_constant, so we restrict it to the
    // narrower range.
    tk = INT; min = 0; max = INT64_MAX; bytes = 8; break;
  case NdbDictionary::Column::Type::Float:
    tk = FLOAT; bytes = 4; break;
  case NdbDictionary::Column::Type::Double:
    tk = FLOAT; bytes = 8; break;
  case NdbDictionary::Column::Type::Decimal:
    [[fallthrough]];
  case NdbDictionary::Column::Type::Decimalunsigned:
    tk = DECIMAL; break;
  case NdbDictionary::Column::Type::Char:
    tk = STR; maxlen = col->getLength(); lenbytes = 0; break;
  case NdbDictionary::Column::Type::Varchar:
    tk = STR; maxlen = 255; lenbytes = 1; break;
  case NdbDictionary::Column::Type::Longvarchar:
    tk = STR; maxlen = 65535; lenbytes = 2; break;
  case NdbDictionary::Column::Type::Date:
    // NDB DATE is 3 bytes (uint3korr); my_date_to_binary writes exactly 3
    // (int3store, same packing as NDB pack_date).  binlen must be 3 — the
    // length flows to qb->constValue(rv.val, rv.len) for CTE-body index
    // bounds (D9); a 4-byte operand made scanIndex reject the 3-byte DATE
    // index column ("Failed to create CTE body index-scan root").  The
    // main-query path uses setBound(col, bt, val) which derives the length
    // from the column, so it tolerated the old over-long value.
    tk = TIME; binlen = 3; timetype = MYSQL_TIMESTAMP_DATE; break;
  case NdbDictionary::Column::Type::Datetime2:
    tk = TIME; binlen = 8; timetype = MYSQL_TIMESTAMP_DATETIME; break;
  case NdbDictionary::Column::Type::Timestamp2:
    tk = TIME; binlen = 7; timetype = MYSQL_TIMESTAMP_DATETIME; break;
  default:
    throw RonSQLMaybeStaleSchema("Unsupported column type in comparison"
                                 " condition. Supported types are integer"
                                 " types, CHAR, VARCHAR, DATE, DATETIME and"
                                 " TIMESTAMP.");
  }
  if (op == T_INT && tk == INT) {
    if (ce->constant_integer < min || ce->constant_integer > max) {
      // Permanent, not RonSQLMaybeStaleSchema: the literal lies outside
      // the column type's domain — a property of the query text against
      // the declared schema, not a transient condition.  Classifying it
      // retryable burned 10 doomed attempts per query (and the join-path
      // unload_schema comparison misreports "schema changed" whenever
      // the root table has an ordered index, so the RMS disambiguation
      // never converged — see non_aggregate_phase_0.md).
      throw RonSQLPermanentError("Integer type column compared to an integer"
                                 " literal out of range.");
    }
    Int64* val = m_amalloc->alloc_exc<Int64>(1);
    *val = ce->constant_integer;
    return raw_value{ val, static_cast<Uint32>(bytes) };
  }
  if (tk == INT) {
    throw RonSQLMaybeStaleSchema("Integer type column compared to an"
                                 " incompatible value. Only integer literals"
                                 " are supported.");
  }
  if (tk == FLOAT) {
    if (op == T_INT && bytes == 4) {
      float* val = m_amalloc->alloc_exc<float>(1);
      *val = float(ce->constant_integer);
      return raw_value{ val, static_cast<Uint32>(bytes) };
    } else if (op == T_INT && bytes == 8) {
      double* val = m_amalloc->alloc_exc<double>(1);
      *val = double(ce->constant_integer);
      return raw_value{ val, static_cast<Uint32>(bytes) };
    } else if (op == T_FLOAT && bytes == 4) {
      float* val = m_amalloc->alloc_exc<float>(1);
      *val = float(ce->constant_float.dbl);
      return raw_value{ val, static_cast<Uint32>(bytes) };
    } else if (op == T_FLOAT && bytes == 8) {
      double* val = m_amalloc->alloc_exc<double>(1);
      *val = ce->constant_float.dbl;
      return raw_value{ val, static_cast<Uint32>(bytes) };
    } else {
      throw RonSQLMaybeStaleSchema("Floating point type column compared to an"
                                   " incompatible value. Only integer and float"
                                   " literals are supported.");
    }
  }
  if (tk == DECIMAL) {
    int prec = col->getPrecision();
    int scale = col->getScale();
    size_t bin_len = decimal_bin_size(prec, scale);
    void* bin = m_amalloc->alloc_bytes(bin_len, 4);
    int err;
    if (op == T_INT) {
      decimal_t dec;
      decimal_digit_t digits[9];
      dec.len = 9;
      dec.buf = digits;
      if (type == NdbDictionary::Column::Type::Decimalunsigned &&
          ce->constant_integer < 0) {
        // Permanent for the same reason as the integer out-of-range case.
        throw RonSQLPermanentError("Decimal type column compared to an"
                                   " integer literal out of range.");
      }
      longlong2decimal(ce->constant_integer, &dec);
      err = decimal2bin(&dec, (unsigned char *)bin, prec, scale);
    } else if (op == T_FLOAT) {
      if (type == NdbDictionary::Column::Type::Decimalunsigned &&
          ce->constant_float.dbl < 0) {
        // Permanent for the same reason as the integer out-of-range case.
        throw RonSQLPermanentError("Decimal type column compared to a"
                                   " float literal out of range.");
      }
      LexString ls = ce->constant_float.ls;
      char dbl_buf[400];
      if (ls.str == NULL) {
        /* A subquery-substituted float constant carries only .dbl —
         * every substitution site sets ls = {NULL, 0} since there is
         * no source text to point at.  decimal_str2bin on the NULL
         * pointer crashed the whole process when a scalar subquery
         * result was compared against a DECIMAL column
         * (ronsql_cte_subquery Test 7).  Render at the COLUMN's scale:
         * the subquery text was formatted at that scale by the inner
         * query, and rounding the double back at the same scale
         * recovers the exact original value (DECIMAL digits within
         * double's exact range).  A round-trip-exact %.17g rendering
         * instead surfaces the double's binary error as extra digits
         * and fails decimal_str2bin with E_DEC_TRUNCATED.  The buffer
         * covers %.f of any double (up to ~309 integer digits). */
        int n = snprintf(dbl_buf, sizeof(dbl_buf), "%.*f", scale,
                         ce->constant_float.dbl);
        require_prm(n > 0 && n < (int)sizeof(dbl_buf),
                    "Failed to render substituted float constant.");
        ls = LexString{ dbl_buf, (size_t)n };
      }
      err = decimal_str2bin(ls.str, ls.len, prec, scale, bin, bin_len);
    } else {
      throw RonSQLMaybeStaleSchema("Floating point type column compared to an"
                                   " incompatible value. Only integer and float"
                                   " literals are supported.");
    }
    if (unlikely(err != E_DEC_OK)) {
      switch (err) {
      case E_DEC_BAD_PREC: [[fallthrough]];
      case E_DEC_BAD_SCALE: [[fallthrough]];
      case E_DEC_OOM: [[fallthrough]];
      case E_DEC_BAD_NUM:
        throw RonSQLPermanentError("Failed converting float literal to DECIMAL");
      case E_DEC_OVERFLOW: [[fallthrough]];
      case E_DEC_TRUNCATED:
        throw RonSQLMaybeStaleSchema("Failed converting float literal to DECIMAL");
      default:
        abort();
      }
    }
    return raw_value{ bin, bin_len };
  }
  if (op == T_INT) {
    throw RonSQLMaybeStaleSchema("Integer literal compared to an incompatible"
                                 " column. Only integer and float type columns"
                                 " are supported.");
  }
  if (op == T_STRING && tk == STR) {
    if (lenbytes == 0) {
      // CHAR: fixed-length, right-padded with spaces, no length prefix
      Uint32 col_len = col->getLength();
      if (ce->string.len > static_cast<size_t>(col_len)) {
        throw RonSQLMaybeStaleSchema("CHAR column compared to a string literal"
                                     " that is too long.");
      }
      Uint8* val = m_amalloc->alloc_exc<Uint8>(col_len);
      memcpy(val, ce->string.str, ce->string.len);
      memset(val + ce->string.len, ' ', col_len - ce->string.len);
      return raw_value{ val, col_len };
    }
    if (ce->string.len > static_cast<size_t>(maxlen)) {
      throw RonSQLMaybeStaleSchema("VARCHAR column compared to a string literal"
                                   " that is too long. Note that if the column"
                                   " length is less than 256, then the length"
                                   " of the string literal must also be less"
                                   " than 256.");
    }
    Uint8* val = m_amalloc->alloc_exc<Uint8>(lenbytes + ce->string.len);
    memcpy(val, &ce->string.len, lenbytes);
    memcpy(val + lenbytes, ce->string.str, ce->string.len);
    return raw_value{ val, static_cast<Uint32>(lenbytes + ce->string.len) };
  }
  if (tk == STR) {
    throw RonSQLMaybeStaleSchema("CHAR/VARCHAR column compared to an"
                                 " incompatible value. Only string literals"
                                 " are supported.");
  }
  if (tk == TIME) {
    MYSQL_TIME mt;
    if (op == T_STRING) {
      rondb_str_to_mysql_time(&mt, ce->string);
    } else if (op == I_MYSQL_TIME) {
      mt = ce->mysql_time;
    } else {
      throw RonSQLMaybeStaleSchema("DATE/DATETIME/TIMESTAMP column compared to"
                                   " an incompatible value. Only string"
                                   " literals and calls to DATE_ADD and"
                                   " DATE_SUB are supported.");
    }
    uchar* bindate = m_amalloc->alloc_exc<uchar>(binlen);
    int precision = col->getPrecision();
    int warnings = 0;
    if (unlikely(mt.time_type != timetype)) {
      throw RonSQLMaybeStaleSchema("DATE/DATETIME/TIMESTAMP column compared to"
                                   " a valid date/time constant of the wrong"
                                   " type. Note that presence/absence of the"
                                   " time part must match.");
    }
    switch (type) {
    case NdbDictionary::Column::Type::Date: {
      my_date_to_binary(&mt, bindate);
      break;
    }
    case NdbDictionary::Column::Type::Datetime2: {
      my_datetime_adjust_frac(&mt, precision, &warnings, true);
      if (unlikely(warnings != 0)) {
        // Actually won't ever happen when truncate argument is set
        throw RonSQLMaybeStaleSchema("Perhaps an invalid DATETIME constant."
                                     " Please report a bug.");
      }
      longlong numericDateTime = TIME_to_longlong_datetime_packed(mt);
      my_datetime_packed_to_binary(numericDateTime, bindate, precision);
      break;
    }
    case NdbDictionary::Column::Type::Timestamp2: {
      // todo See ../../rest-server2/server/src/db_operations/pk/common.cpp for
      // timezone issues with Timestamp2. Currently this acts as if the server
      // is always in UTC.
      time_t epoch = 0;
      errno = 0;
      struct tm time_info;
      time_info.tm_year = mt.year - 1900;  // tm_year is years since 1900
      time_info.tm_mon = mt.month - 1;     // tm_mon is 0-based
      time_info.tm_mday = mt.day;
      time_info.tm_hour = mt.hour;
      time_info.tm_min = mt.minute;
      time_info.tm_sec = mt.second;
      time_info.tm_isdst = -1; // Daylight saving t
      epoch = timegm(&time_info);
      if (unlikely(epoch <= 0 || epoch > 2147483647)) {
        throw RonSQLMaybeStaleSchema("TIMESTAMP column compared to a constant"
                                     " out of range (valid range is '1970-01-01"
                                     " 00:00:01' UTC to '2038-01-19 03:14:07'"
                                     " UTC)");
      }
      my_datetime_adjust_frac(&mt, precision, &warnings, true);
      if (unlikely(warnings != 0)) {
        // Actually won't ever happen when truncate argument is set
        throw RonSQLMaybeStaleSchema("Perhaps an invalid TIMESTAMP constant."
                                     " Please report a bug.");
      }
      // On Mac timeval.tv_usec is Int32 and on linux it is Int64.
      // Inorder to be compatible we cast l_time.second_part to Int32
      // This will not create problems as only six digit nanoseconds
      // are stored in Timestamp2
      my_timeval myTV{epoch, (Int32)mt.second_part};
      my_timestamp_to_binary(&myTV, bindate, precision);
      break;
    }
    default:
      abort();
    }
    return raw_value{bindate, binlen};
  }
  throw RonSQLPermanentError("Bug in RonSQLPreparer::encode_constant");
}

struct ConditionalExpression*
RonSQLPreparer::simplify_ce(struct ConditionalExpression* ce, int maxdepth)
{
  if (maxdepth == 0 || ce == NULL) {
    return ce;
  }
  TokenKind op = ce->op;
  switch(ce->op)
  {
  case T_EQUALS:
    [[fallthrough]];
  case T_GE:
    [[fallthrough]];
  case T_GT:
    [[fallthrough]];
  case T_LE:
    [[fallthrough]];
  case T_LT:
    [[fallthrough]];
  case T_NOT_EQUALS:
    {
      struct ConditionalExpression* left =
        simplify_ce(ce->args.left, maxdepth - 1);
      struct ConditionalExpression* right =
        simplify_ce(ce->args.right, maxdepth - 1);
      if (left->op != T_IDENTIFIER && right->op == T_IDENTIFIER ) {
        if      (op == T_GE) op = T_LE;
        else if (op == T_GT) op = T_LT;
        else if (op == T_LE) op = T_GE;
        else if (op == T_LT) op = T_GT;
        ConditionalExpression* tmp = left; left = right; right = tmp;
      }
      // D11: GREATEST(...)/LEAST(...) in a comparison -> boolean rewrite.
      if (left->op == T_GREATEST || left->op == T_LEAST ||
          right->op == T_GREATEST || right->op == T_LEAST) {
        ConditionalExpression* rw =
            rewrite_minmax_comparison(op, left, right, maxdepth - 1);
        return simplify_ce(rw, maxdepth - 1);
      }
      if (op == ce->op && left == ce->args.left && right == ce->args.right) {
        return ce;
      }
      ConditionalExpression* ret =
        m_amalloc->alloc_exc<ConditionalExpression>(1);
      ret->op = op;
      ret->args.left = left;
      ret->args.right = right;
      return ret;
    }
  case T_DATE_ADD:
    [[fallthrough]];
  case T_DATE_SUB:
    {
      bool neg = ce->op == T_DATE_SUB;
      ConditionalExpression* date_ce = simplify_ce(ce->args.left, maxdepth - 1);
      ConditionalExpression* interval_ce = ce->args.right;
      MYSQL_TIME ltime;
      if (date_ce->op == T_STRING) {
        rondb_str_to_mysql_time(&ltime, date_ce->string);
      } else if (date_ce->op == I_MYSQL_TIME) {
        ltime = date_ce->mysql_time;
      } else {
        throw RonSQLPermanentError("The first argument to DATE_ADD and DATE_SUB"
                                   " must be a string literal or another call"
                                   " to DATE_ADD or DATE_SUB");
      }
      if (interval_ce->op != T_INTERVAL) {
        throw RonSQLPermanentError("The second argument to DATE_ADD and"
                                   " DATE_SUB must be an INTERVAL literal");
      }
      ConditionalExpression* amount_ce = simplify_ce(interval_ce->interval.arg,
                                                     maxdepth - 1);
      Int64 constant_integer;
      switch (amount_ce->op) {
      case T_INT:
        constant_integer = amount_ce->constant_integer;
        break;
      case T_STRING:
        {
          const char* string_literal =
            amount_ce->string.to_LexCString(m_amalloc).c_str();
          char* endptr;
          constant_integer = strtoll(string_literal, &endptr, 10);
          if (*endptr != '\0') {
            throw RonSQLPermanentError("Failed to convert INTERVAL string"
                                       " literal amount to an integer");
          }
        }
        break;
      default:
        throw RonSQLPermanentError("INTERVAL literal requires an amount in the"
                                   " form of an integer literal or a string"
                                   " literal representing an integer");
      }
      // Member variables in Interval are unsigned long int or
      // unsigned long long int
      if (constant_integer < 0) {
        neg = !neg;
        constant_integer = -constant_integer;
      }
      unsigned long long int amount = constant_integer;
      Interval interval = {0, 0, 0, 0, 0, 0, 0, neg};
      enum interval_type interval_type;
      switch(interval_ce->interval.interval_type) {
      case T_MICROSECOND:
        interval.second_part = amount;
        interval_type = INTERVAL_MICROSECOND;
        break;
      case T_SECOND:
        interval.second = amount;
        interval_type = INTERVAL_SECOND;
        break;
      case T_MINUTE:
        interval.minute = amount;
        interval_type = INTERVAL_MINUTE;
        break;
      case T_HOUR:
        interval.hour = amount;
        interval_type = INTERVAL_HOUR;
        break;
      case T_DAY:
        interval.day = amount;
        interval_type = INTERVAL_DAY;
        break;
      case T_WEEK:
        interval.day = amount * 7;
        interval_type = INTERVAL_WEEK;
        break;
      case T_MONTH:
        interval.month = amount;
        interval_type = INTERVAL_MONTH;
        break;
      case T_QUARTER:
        interval.month = amount * 3;
        interval_type = INTERVAL_QUARTER;
        break;
      case T_YEAR:
        interval.year = amount;
        interval_type = INTERVAL_YEAR;
        break;
      default:
        throw RonSQLPermanentError("INTERVAL literals support only interval"
                                   " types MICROSECOND, SECOND, MINUTE, HOUR,"
                                   " DAY, WEEK, MONTH, QUARTER, and YEAR");
      }
      int warnings = 0;
      bool err = date_add_interval(&ltime,
                                   interval_type,
                                   interval,
                                   &warnings);
      if (err || warnings) {
        throw RonSQLPermanentError("DATE_ADD or DATE_SUB failed");
      }
      ConditionalExpression* ret =
        m_amalloc->alloc_exc<ConditionalExpression>(1);
      ret->op = I_MYSQL_TIME;
      ret->mysql_time = ltime;
      return ret;
    }
  case T_XOR:
    {
      // Conjunctive Normal Form expansion of XOR:
      // x XOR y ->
      // (x OR   y) AND  (NOT  x    OR   NOT y)
      //    r[1]    r[0]  r[3]      r[2] r[4]
      ConditionalExpression* x = simplify_ce(ce->args.left, maxdepth - 1);
      ConditionalExpression* y = simplify_ce(ce->args.right, maxdepth - 1);
      ConditionalExpression* r = m_amalloc->alloc_exc<ConditionalExpression>(5);
      r[4].op = T_NOT;
      r[4].args.left = y;
      r[3].op = T_NOT;
      r[3].args.left = x;
      r[2].args.left = simplify_ce(&r[3], 1);
      r[2].op = T_OR;
      r[2].args.right = simplify_ce(&r[4], 1);
      r[1].args.left = x;
      r[1].op = T_OR;
      r[1].args.right = y;
      r[0].args.left = &r[1];
      r[0].op = T_AND;
      r[0].args.right = &r[2];
      return &r[0];
    }
  case T_NOT:
    {
      ConditionalExpression* l = simplify_ce(ce->args.left, maxdepth - 1);
      if (l->op == T_NOT) {
        // Simplify NOT NOT x -> x
        return simplify_ce(l->args.left, maxdepth - 1);
      }
      if (l->op == T_OR) {
        // Simplify towards conjunctive normal form:
        // NOT (x OR y) ->
        // (NOT  x) AND (NOT  y)
        //  r[1]    r[0] r[2]
        ConditionalExpression* x = simplify_ce(l->args.left, maxdepth - 1);
        ConditionalExpression* y = simplify_ce(l->args.right, maxdepth - 1);
        ConditionalExpression* r =
          m_amalloc->alloc_exc<ConditionalExpression>(3);
        r[2].op = T_NOT;
        r[2].args.left = y;
        r[1].op = T_NOT;
        r[1].args.left = x;
        r[0].args.left = simplify_ce(&r[1], 1);
        r[0].op = T_AND;
        r[0].args.right = simplify_ce(&r[2], 1);
        return &r[0];
      }
      if (l != ce->args.left) {
        ConditionalExpression* r =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        r->op = T_NOT;
        r->args.left = l;
        return r;
      }
      return ce;
    }
  case T_AND:
    [[fallthrough]];
  case T_OR:
    {
      ConditionalExpression* x = simplify_ce(ce->args.left, maxdepth - 1);
      ConditionalExpression* y = simplify_ce(ce->args.right, maxdepth - 1);
      if (x == ce->args.left && y == ce->args.right) {
        return ce;
      }
      ConditionalExpression* r = m_amalloc->alloc_exc<ConditionalExpression>(1);
      r->op = ce->op;
      r->args.left = x;
      r->args.right = y;
      return r;
    }
  case T_MINUS:
    {
      if (ce->args.left != NULL) {
        return ce;
      }
      ConditionalExpression* n = simplify_ce(ce->args.right, maxdepth - 1);
      if (n->op == T_INT || n->op == T_FLOAT) {
        ConditionalExpression* r =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
        r->op = n->op;
        if (n->op == T_INT) {
          r->constant_integer = -n->constant_integer;
        } else {
          r->constant_float.dbl = -n->constant_float.dbl;
          assert(n->constant_float.ls.len > 0);
          if (n->constant_float.ls.str[0] == '-') {
            r->constant_float.ls.str = &n->constant_float.ls.str[1];
            r->constant_float.ls.len = n->constant_float.ls.len - 1;
          } else {
            r->constant_float.ls = (LexString{"-", 1})
              .concat(n->constant_float.ls, m_amalloc);
          }
        }
        return r;
      }
      return ce;
    }
  default:
    // No simplification to do
    return ce;
  }
}

// D11 helper: evaluate a constant-integer comparison arm at prepare time.
static bool ronsql_eval_int_cmp(TokenKind op, Int64 a, Int64 b) {
  switch (op) {
    case T_GT: return a >  b;
    case T_GE: return a >= b;
    case T_LT: return a <  b;
    case T_LE: return a <= b;
    default:   return false;  // = / != are rejected before reaching here
  }
}

// D11: lower a `GREATEST(...) <cmp> const` / `LEAST(...) <cmp> const`
// comparison (the min/max node may be on either side) into a boolean
// OR/AND of per-argument column-vs-constant comparisons.  Semantics:
//   GREATEST(a..) >  c  <=>  a > c OR  b > c ...     (OR direction)
//   GREATEST(a..) <  c  <=>  a < c AND b < c ...     (AND direction)
//   LEAST(a..)    <  c  <=>  a < c OR  b < c ...     (OR direction)
//   LEAST(a..)    >  c  <=>  a > c AND b > c ...     (AND direction)
// (>= / <= analogous.)  GREATEST/LEAST is NULL when any argument is NULL,
// so the OR direction additionally requires every column argument to be
// non-NULL; the AND direction is already NULL-rejecting through its per-arg
// comparisons.  Constant arguments are folded; = / != and non-constant
// comparands are rejected (the latter would otherwise yield an unsupported
// column-vs-column CTE-body filter).
//
// Node allocation uses ArenaMalloc::alloc_exc; on out-of-memory it throws
// std::bad_alloc, which RonSQL's exception handler turns into a clean
// permanent "out of memory" failure of the whole query.  Semantic rejections
// use the file's normal RonSQLPermanentError path.
struct ConditionalExpression*
RonSQLPreparer::rewrite_minmax_comparison(TokenKind cmp_op,
                                          struct ConditionalExpression* left,
                                          struct ConditionalExpression* right,
                                          int maxdepth)
{
  struct ConditionalExpression* mm;
  struct ConditionalExpression* comparand;
  TokenKind op;  // normalised so the relation reads `mm <op> comparand`
  if (left->op == T_GREATEST || left->op == T_LEAST) {
    mm = left; comparand = right; op = cmp_op;
  } else {
    mm = right; comparand = left;
    switch (cmp_op) {            // `c <cmp_op> mm`  ==  `mm <op> c`
      case T_GT: op = T_LT; break;
      case T_GE: op = T_LE; break;
      case T_LT: op = T_GT; break;
      case T_LE: op = T_GE; break;
      default:   op = cmp_op; break;
    }
  }
  const bool is_greatest = (mm->op == T_GREATEST);

  if (op == T_EQUALS || op == T_NOT_EQUALS) {
    throw RonSQLPermanentError(
        "GREATEST/LEAST with = or != in a WHERE clause is not supported; "
        "use a range comparison (<, <=, >, >=).");
  }
  const bool comparand_is_int = (comparand->op == T_INT);
  if (comparand->op != T_INT && comparand->op != T_FLOAT &&
      comparand->op != T_STRING) {
    throw RonSQLPermanentError(
        "GREATEST/LEAST in a WHERE clause must be compared to a constant "
        "value.");
  }

  const bool or_dir = is_greatest ? (op == T_GT || op == T_GE)
                                   : (op == T_LT || op == T_LE);

  struct ConditionalExpression* combined = NULL;  // OR/AND of column arms
  struct ConditionalExpression* guards = NULL;    // AND of `col IS NOT NULL`
  bool forced_true = false;                       // OR forced true by a const
  Uint32 n_col_args = 0;

  for (struct ConditionalExpression* node = mm->args.left; node != NULL;
       node = node->args.right) {
    struct ConditionalExpression* elem = simplify_ce(node->args.left, maxdepth);
    if (elem->op == T_INT && comparand_is_int) {
      const bool arm = ronsql_eval_int_cmp(op, elem->constant_integer,
                                           comparand->constant_integer);
      if (or_dir) {
        if (arm) forced_true = true;     // false arm: drop
      } else if (!arm) {
        throw RonSQLPermanentError(
            "GREATEST/LEAST WHERE predicate is a constant contradiction; "
            "not supported.");
      }                                  // true AND-arm: drop
      continue;
    }
    if (elem->op != T_IDENTIFIER) {
      throw RonSQLPermanentError(
          "GREATEST/LEAST in a WHERE clause supports only column and integer "
          "constant arguments.");
    }
    n_col_args++;
    struct ConditionalExpression* cmp = m_amalloc->alloc_exc<ConditionalExpression>(1);
    cmp->op = op;
    cmp->args.left = elem;
    cmp->args.right = comparand;
    if (combined == NULL) {
      combined = cmp;
    } else {
      struct ConditionalExpression* c = m_amalloc->alloc_exc<ConditionalExpression>(1);
      c->op = or_dir ? T_OR : T_AND;
      c->args.left = combined;
      c->args.right = cmp;
      combined = c;
    }
    if (or_dir) {
      struct ConditionalExpression* g = m_amalloc->alloc_exc<ConditionalExpression>(1);
      g->op = T_IS;
      g->is.arg = elem;
      g->is.null = false;  // IS NOT NULL
      if (guards == NULL) {
        guards = g;
      } else {
        struct ConditionalExpression* a = m_amalloc->alloc_exc<ConditionalExpression>(1);
        a->op = T_AND;
        a->args.left = guards;
        a->args.right = g;
        guards = a;
      }
    }
  }

  if (n_col_args == 0) {
    throw RonSQLPermanentError(
        "GREATEST/LEAST in a WHERE clause requires at least one column "
        "argument.");
  }
  if (!or_dir) {
    return combined;  // AND of per-arg comparisons (NULL-safe as-is)
  }
  struct ConditionalExpression* core = forced_true ? NULL : combined;
  if (core == NULL) {
    return guards;    // always-true comparison: only the non-NULL guards remain
  }
  struct ConditionalExpression* root = m_amalloc->alloc_exc<ConditionalExpression>(1);
  root->op = T_AND;
  root->args.left = guards;
  root->args.right = core;
  return root;
}

#define programAggregator_do_or_fail(CALL) \
  require_prm(CALL, "Failed writing aggregation program. Please report a bug.")
void
RonSQLPreparer::programAggregator(NdbAggregator* aggregator)
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  SelectStatement& ast_root = m_context.ast_root;
  // Phase I.5 v4 fast path: precompute per-pair-op nullability so
  // raw_word_size and emit_pair_op_embedded agree on the body size.
  prepare_pair_op_null_check_cache(m_main_scope);
  // Program groupby columns
  require_run(m_main_scope.resolved_columns != NULL,
              "Aggregation emit: missing resolved columns.");
  struct GroupbyColumns* groupby = ast_root.groupby_columns;
  while (groupby != NULL)
  {
    const QueryScope::ResolvedColumnRef& col_ref =
        m_main_scope.resolved_columns[groupby->col_idx];
    require_prm(col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                "Single-table GROUP BY column is not a stored-table column.");
    programAggregator_do_or_fail
      (aggregator->GroupBy(col_ref.attr_id));
    groupby = groupby->next;
  }
  // Program aggregations
  assert(m_main_scope.agg != NULL); // Ensured in RonSQLPreparer::load
  DynamicArray<AggregationAPICompiler::Instr>& program = m_main_scope.agg->m_program;
  for (Uint32 i=0; i<program.size(); i++)
  {
    AggregationAPICompiler::Instr* instr = &program[i];
    Uint32 dest = instr->dest;
    Uint32 src = instr->src;
    switch (instr->type)
    {
    case AggregationAPICompiler::SVMInstrType::Load:
    {
      const QueryScope::ResolvedColumnRef& src_ref =
          m_main_scope.resolved_columns[src];
      require_prm(src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                  "Single-table aggregation load is not a stored-table column.");
      NdbAttrId col_id = src_ref.attr_id;
      if (!aggregator->LoadColumn(col_id, dest))
      {
        err << "Failed writing aggregation program "
               "when attempting to load column "
            << quoted_identifier(m_columns[src]) << endl;
        throw RonSQLMaybeStaleSchema("Failed writing aggregation program");
      }
      break;
    }
    case AggregationAPICompiler::SVMInstrType::LoadConstantInteger:
      programAggregator_do_or_fail
        (aggregator->LoadInt64(m_main_scope.agg->m_constants[src].int_64, dest));
      break;
    case AggregationAPICompiler::SVMInstrType::Mov:
      programAggregator_do_or_fail(aggregator->Mov(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Add:
      programAggregator_do_or_fail(aggregator->Add(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Minus:
      programAggregator_do_or_fail(aggregator->Minus(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Mul:
      programAggregator_do_or_fail(aggregator->Mul(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Div:
      programAggregator_do_or_fail(aggregator->Div(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::DivInt:
      programAggregator_do_or_fail(aggregator->DivInt(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Rem:
      programAggregator_do_or_fail(aggregator->Mod(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Sum:
      programAggregator_do_or_fail(aggregator->Sum(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Min:
      programAggregator_do_or_fail(aggregator->Min(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Max:
      programAggregator_do_or_fail(aggregator->Max(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Count:
      programAggregator_do_or_fail(aggregator->Count(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Greatest2:
    case AggregationAPICompiler::SVMInstrType::Least2:
      emit_pair_op_embedded(
          aggregator, dest, src,
          instr->type == AggregationAPICompiler::SVMInstrType::Greatest2,
          m_main_scope.agg->m_pair_op_needs_null_check[i]);
      break;
    case AggregationAPICompiler::SVMInstrType::AggRepeat:
      programAggregator_do_or_fail(aggregator->RepeatAgg(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::EmbeddedInterp:
    {
      auto& ci = m_main_scope.agg->m_cases[dest];
      Uint32 then_raw = m_main_scope.agg->raw_word_size(ci.then_start, ci.skip_pos);
      Uint32 skip_raw = 1;
      Uint32 then_arm_total = then_raw + skip_raw;
      generate_embedded_condition(aggregator, m_main_scope, ci.condition,
                                  then_arm_total, /*cteVirtualTables=*/nullptr);
      break;
    }
    case AggregationAPICompiler::SVMInstrType::Skip:
    {
      for (Uint32 c = 0; c < m_main_scope.agg->m_cases.size(); c++)
      {
        if (m_main_scope.agg->m_cases[c].skip_pos == i)
        {
          auto& ci = m_main_scope.agg->m_cases[c];
          Uint32 else_raw = m_main_scope.agg->raw_word_size(ci.else_start, ci.else_end);
          programAggregator_do_or_fail(aggregator->Skip(else_raw));
          break;
        }
      }
      break;
    }
    default:
      // Unknown instruction
      abort();
    }
  }
}

/*
 * Compute the minimum number of registers needed to evaluate a filter
 * expression.  Used to reject expressions too complex for the 2-register
 * per-side scheme (reg + tmp_reg).
 *   Leaf (column/constant): 1
 *   Binary op: max(left, right) if different, left+1 if equal
 */
static Uint32
filter_expr_reg_depth(ConditionalExpression* ce)
{
  switch (ce->op) {
  case T_IDENTIFIER:
  case T_INT:
  case T_FLOAT:
    return 1;
  case T_PLUS:
  case T_MINUS:
  case T_MULTIPLY:
  {
    Uint32 ld = filter_expr_reg_depth(ce->args.left);
    Uint32 rd = filter_expr_reg_depth(ce->args.right);
    return (ld == rd) ? ld + 1 : (ld > rd ? ld : rd);
  }
  default:
    return 99;  // unsupported → will be caught later
  }
}

/*
 * Count the number of NdbAggregator program words needed to compile
 * Count normal-interpreter words needed to evaluate a cross-table filter
 * expression into a register.  Supports identifiers, integer constants,
 * and the arithmetic shapes accepted by cross_table_where_filters.
 */
Uint32
RonSQLPreparer::embedded_filter_expr_word_count(QueryScope& scope,
                                                ConditionalExpression* ce,
                                                Uint32 leaf_idx)
{
  switch (ce->op) {
  case T_IDENTIFIER:
  {
    Uint32 cidx = ce->col_idx;
    require_run(scope.resolved_columns != NULL,
                "Cross-table WHERE filter: missing resolved columns.");
    const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[cidx];
    require_prm(ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                "Cross-table WHERE filter expression requires a "
                "stored-table column.");
    require_prm(ref.dict_column != NULL,
                "Cross-table WHERE filter: unresolved column.");
    // One typed load word (leaf READ_ATTR_INTO_REG / linked
    // READ_LINKED_COLUMN_TO_REG) plus a NULL-guard word for nullable
    // columns — UNKNOWN must reject the atom instead of erroring the
    // query (findings/nullable_bounds.md, the nb-8 "code 1872"
    // finding: the reg-reg branches return ZREGISTER_INIT_ERROR on a
    // NULL register).
    return ref.dict_column->getNullable() ? 2 : 1;
  }
  case T_INT:
    return 3;  // LOAD_CONST64 + 2 value words
  case T_PLUS:
  case T_MINUS:
  case T_MULTIPLY:
    return embedded_filter_expr_word_count(scope, ce->args.left, leaf_idx) +
           embedded_filter_expr_word_count(scope, ce->args.right, leaf_idx) +
           1;
  default:
    require_prm(false,
        "Unsupported expression in cross-table WHERE filter. "
        "Only columns, integer constants, and +/-/* are supported.");
    return 0;
  }
}

void
RonSQLPreparer::emit_embedded_filter_expr(NdbAggregator* agg,
                                          QueryScope& scope,
                                          ConditionalExpression* ce,
                                          Uint32 leaf_idx,
                                          Uint32 reg,
                                          Uint32 tmp_reg,
                                          Uint32& cur_pos,
                                          Uint32 null_fail_target)
{
  switch (ce->op) {
  case T_IDENTIFIER:
  {
    Uint32 cidx = ce->col_idx;
    require_run(scope.resolved_columns != NULL,
                "Cross-table WHERE filter: missing resolved columns.");
    const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[cidx];
    require_prm(ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                "Cross-table WHERE filter expression requires a "
                "stored-table column.");
    const NdbDictionary::Column* col = ref.dict_column;
    require_prm(col != NULL,
                "Cross-table WHERE filter: unresolved column.");
    if (ref.join_op_idx == leaf_idx) {
      // READ_ATTR_INTO_REG: native-typed load, sets the register's
      // NULL_INDICATOR for NULL values.
      programAggregator_do_or_fail(agg->EmitEmbeddedWord(
          Interpreter::Read((Uint32)ref.attr_id, reg)));
      cur_pos++;
    } else {
      Uint32 lp = find_or_add_linked_proj(
          scope.join_plan, ref.join_op_idx,
          m_columns[cidx].c_str());
      // Phase I.5 v5 typed one-word load, replacing the pre-v5
      // READ_LINKED_TO_MEM + READ_*_MEM_TO_REG_CONST pair which read
      // raw bytes: no NULL flag (a NULL linked value produced a
      // garbage register) and zero-extension of signed sub-Bigint
      // operands.  Integer family only (the arithmetic ops below are
      // integer ops).
      switch (col->getType()) {
      case NdbDictionary::Column::Tinyint:
      case NdbDictionary::Column::Tinyunsigned:
      case NdbDictionary::Column::Smallint:
      case NdbDictionary::Column::Smallunsigned:
      case NdbDictionary::Column::Mediumint:
      case NdbDictionary::Column::Mediumunsigned:
      case NdbDictionary::Column::Int:
      case NdbDictionary::Column::Unsigned:
      case NdbDictionary::Column::Bigint:
      case NdbDictionary::Column::Bigunsigned:
        break;
      default:
        require_prm(false,
                    "Unsupported linked column type in cross-table "
                    "WHERE filter (integer columns only).");
      }
      programAggregator_do_or_fail(agg->EmitEmbeddedWord(
          Interpreter::ReadLinkedColumnIntoReg(reg, lp,
                                               (Uint32)col->getType())));
      cur_pos++;
    }
    if (col->getNullable()) {
      // UNKNOWN rejects the atom (findings/nullable_bounds.md, the
      // nb-8 "code 1872" finding): on NULL, jump to the atom's fail
      // destination — the reg-reg compare branches return
      // ZREGISTER_INIT_ERROR on a NULL register otherwise, failing
      // the whole query instead of filtering the row.
      Uint32 guard_offset = null_fail_target - cur_pos;
      programAggregator_do_or_fail(agg->EmitEmbeddedWord(
          Interpreter::Branch(Interpreter::BRANCH_REG_EQ_NULL,
                              /*Reg1=*/0, /*Reg2=*/reg) |
          (guard_offset << 16)));
      cur_pos++;
    }
    break;
  }
  case T_INT:
  {
    Int64 v = ce->constant_integer;
    Uint32 lo = 0;
    Uint32 hi = 0;
    memcpy(&lo, &v, 4);
    memcpy(&hi, ((char*)&v) + 4, 4);
    programAggregator_do_or_fail(
        agg->EmitEmbeddedWord(Interpreter::LoadConst64(reg)));
    programAggregator_do_or_fail(agg->EmitEmbeddedWord(lo));
    programAggregator_do_or_fail(agg->EmitEmbeddedWord(hi));
    cur_pos += 3;
    break;
  }
  case T_PLUS:
  case T_MINUS:
  case T_MULTIPLY:
  {
    // Nullable inner columns guard right after their own load (an
    // Add/Sub/Mul on a NULL register would error just like a
    // compare), so the whole expression is UNKNOWN when any operand
    // is NULL — matching SQL.
    emit_embedded_filter_expr(agg, scope, ce->args.left, leaf_idx,
                              reg, tmp_reg, cur_pos, null_fail_target);
    emit_embedded_filter_expr(agg, scope, ce->args.right, leaf_idx,
                              tmp_reg, reg, cur_pos, null_fail_target);
    switch (ce->op) {
    case T_PLUS:
      programAggregator_do_or_fail(
          agg->EmitEmbeddedWord(Interpreter::Add(reg, reg, tmp_reg)));
      break;
    case T_MINUS:
      programAggregator_do_or_fail(
          agg->EmitEmbeddedWord(Interpreter::Sub(reg, reg, tmp_reg)));
      break;
    case T_MULTIPLY:
      programAggregator_do_or_fail(
          agg->EmitEmbeddedWord(Interpreter::Mul(reg, reg, tmp_reg)));
      break;
    default:
      break;
    }
    cur_pos++;
    break;
  }
  default:
    require_prm(false,
        "Unsupported expression in cross-table WHERE filter.");
  }
}

void
RonSQLPreparer::generate_embedded_filter_condition(NdbAggregator* aggregator,
                                                   QueryScope& scope,
                                                   ConditionalExpression* ce,
                                                   Uint32 true_output,
                                                   Uint32 false_output,
                                                   Uint32 leaf_idx)
{
  DynamicArray<ConditionalExpression*> atoms(m_amalloc);
  bool is_and = false;
  if (ce->op == T_AND || ce->op == T_OR) {
    is_and = (ce->op == T_AND);
    TokenKind flatten_op = ce->op;
    ConditionalExpression* node = ce;
    while (node->op == flatten_op) {
      atoms.push(node->args.right);
      node = node->args.left;
    }
    atoms.push(node);
  } else {
    atoms.push(ce);
  }

  Uint32 atom_words[32];
  require_prm(atoms.size() <= 32,
              "Cross-table WHERE filter has too many OR/AND atoms.");
  Uint32 total_atom_words = 0;
  for (Uint32 a = 0; a < atoms.size(); a++) {
    ConditionalExpression* atom = atoms[a];
    require_prm(atom->op == T_EQUALS || atom->op == T_NOT_EQUALS ||
                atom->op == T_LT || atom->op == T_LE ||
                atom->op == T_GT || atom->op == T_GE,
                "Cross-table WHERE filter atom: only =, !=, <, <=, >, >= "
                "supported.");
    atom_words[a] =
        embedded_filter_expr_word_count(scope, atom->args.left, leaf_idx) +
        embedded_filter_expr_word_count(scope, atom->args.right, leaf_idx) +
        1;
    total_atom_words += atom_words[a];
  }

  Uint32 emb_len = total_atom_words + 6;
  Uint32 second_exit_label = emb_len - 3;
  programAggregator_do_or_fail(aggregator->EmbeddedInterp(emb_len));

  std::function<Uint32(TokenKind)> reg_branch_opcode =
      [&](TokenKind op) -> Uint32 {
    if (is_and) {
      switch (op) {
      case T_EQUALS:     return Interpreter::BRANCH_NE_REG_REG;
      case T_NOT_EQUALS: return Interpreter::BRANCH_EQ_REG_REG;
      case T_LT:         return Interpreter::BRANCH_GE_REG_REG;
      case T_LE:         return Interpreter::BRANCH_GT_REG_REG;
      case T_GT:         return Interpreter::BRANCH_LE_REG_REG;
      case T_GE:         return Interpreter::BRANCH_LT_REG_REG;
      default: abort();
      }
    }
    switch (op) {
    case T_EQUALS:     return Interpreter::BRANCH_EQ_REG_REG;
    case T_NOT_EQUALS: return Interpreter::BRANCH_NE_REG_REG;
    case T_LT:         return Interpreter::BRANCH_LT_REG_REG;
    case T_LE:         return Interpreter::BRANCH_LE_REG_REG;
    case T_GT:         return Interpreter::BRANCH_GT_REG_REG;
    case T_GE:         return Interpreter::BRANCH_GE_REG_REG;
    default: abort();
    }
  };

  Uint32 pos = 0;
  for (Uint32 a = 0; a < atoms.size(); a++) {
    ConditionalExpression* atom = atoms[a];
    const Uint32 R1 = 1;
    const Uint32 R2 = 2;
    const Uint32 R3 = 3;
    // NULL-guard fail destination for this atom's nullable operands
    // (UNKNOWN rejects the atom): under AND a failed atom fails the
    // whole conjunct — jump to the false exit; under OR it just fails
    // this disjunct — jump past this atom's compare (for the last
    // atom that falls into the first exit, which is OR's false exit).
    Uint32 atom_fail = is_and ? second_exit_label : (pos + atom_words[a]);
    Uint32 cur = pos;
    emit_embedded_filter_expr(aggregator, scope, atom->args.left, leaf_idx,
                              R1, R3, cur, atom_fail);
    emit_embedded_filter_expr(aggregator, scope, atom->args.right, leaf_idx,
                              R2, R3, cur, atom_fail);
    Uint32 branch_instr_pos = pos + atom_words[a] - 1;
    ndbrequire(cur == branch_instr_pos);
    Uint32 branch_offset = second_exit_label - branch_instr_pos;
    Uint32 br_op = reg_branch_opcode(atom->op);
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::Branch(br_op, /*Reg1=*/R2, /*Reg2=*/R1) |
        (branch_offset << 16)));
    pos += atom_words[a];
  }

  Uint32 first_exit_output = is_and ? true_output : false_output;
  Uint32 second_exit_output = is_and ? false_output : true_output;

  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::LoadConst16(2, first_exit_output)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::WriteInterpreterOutput(2, 0)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::ExitOK()));

  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::LoadConst16(2, second_exit_output)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::WriteInterpreterOutput(2, 0)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::ExitOK()));
}

void
RonSQLPreparer::programAggregator_join(QueryScope& scope,
                                        SelectStatement& ast_root,
                                        NdbAggregator* aggregator,
                                        NdbDictionary::Table* const*
                                            cteVirtualTables)
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  // Phase I.5 v4 fast path: precompute per-pair-op nullability so
  // raw_word_size and emit_pair_op_embedded agree on the body size.
  prepare_pair_op_null_check_cache(scope);
  Uint32 leaf_idx = scope.join_plan.agg_leaf_idx;
  // When the aggregation leaf is a CTE op, the CTE's per-group data
  // (GROUP BY key + aggregate results, in cte->stmt->outputs order)
  // reaches the main aggregator through the linked-attr buffer that
  // cteLookupAggFeed builds in DBLQH. We therefore emit LoadLinkedColumn
  // at linked-position == cte_col_idx instead of a row-fetch LoadColumn.
  const bool leafIsCte =
      (scope.join_plan.ops[leaf_idx].type == JoinOp::CTE_LOOKUP ||
       scope.join_plan.ops[leaf_idx].type == JoinOp::CTE_SCAN);
  const NdbDictionary::Table* cteLeafVirtTab =
      (leafIsCte && cteVirtualTables != NULL)
      ? cteVirtualTables[leaf_idx]
      : NULL;

  // Program groupby columns.
  //
  // For a CTE agg-feed leaf, the DBLQH linked-attr buffer layout is:
  //   [parent linked projections, N entries] [CTE GB keys] [CTE agg results]
  // i.e. parent projections occupy positions 0..N-1, and the CTE's virt-table
  // outputs begin at position N = scope.join_plan.num_linked_projs.
  // For a non-CTE leaf, CTE output positions are unused; parent projections
  // still occupy positions 0..N-1 and leaf-local cols are addressed via
  // GroupBy(attrId).
  require_run(scope.resolved_columns != NULL,
              "Aggregation emit: missing resolved columns.");
  const Uint32 cte_base_pos = leafIsCte ? scope.join_plan.num_linked_projs : 0;
  Uint32 linked_proj_pos = 0;
  struct GroupbyColumns* groupby = ast_root.groupby_columns;
  while (groupby != NULL)
  {
    Uint32 col_idx = groupby->col_idx;
    const QueryScope::ResolvedColumnRef& col_ref =
        scope.resolved_columns[col_idx];
    require_prm(
        col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn ||
        col_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
        "GROUP BY column is not resolved to a table or CTE column.");
    if (col_ref.join_op_idx == leaf_idx)
    {
      if (leafIsCte && cteLeafVirtTab != NULL) {
        // CTE leaf: GB col is a CTE output. Offset by parent linked
        // projections because cteLookupAggFeed puts CTE outputs after them.
        require_prm(
            col_ref.kind ==
            QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
            "CTE leaf GROUP BY column is not a CTE result column.");
        const Uint32 cte_col_idx = col_ref.cte_result_idx;
        const NdbDictionary::Column* vtcol =
            cteLeafVirtTab->getColumn((int)cte_col_idx);
        require_prm(vtcol != NULL,
                    "CTE leaf GroupBy: virt table has no column at cte_col_idx.");
        programAggregator_do_or_fail(
            aggregator->GroupByLinked(cte_base_pos + cte_col_idx, vtcol));
      } else {
        require_prm(
            col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
            "Leaf GROUP BY column is not a stored-table column.");
        programAggregator_do_or_fail
          (aggregator->GroupBy(col_ref.attr_id));
      }
    }
    else
    {
      const Uint32 src_op_idx = col_ref.join_op_idx;
      if (col_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn &&
          cteVirtualTables != NULL &&
          cteVirtualTables[src_op_idx] != NULL)
      {
        // Non-leaf CTE GB column: resolve the column descriptor from the
        // non-leaf CTE's virtual table.
        const Uint32 cte_col_idx = col_ref.cte_result_idx;
        const NdbDictionary::Column* vtcol =
            cteVirtualTables[src_op_idx]->getColumn((int)cte_col_idx);
        require_prm(vtcol != NULL,
                    "Non-leaf CTE GroupBy: virt table has no column at"
                    " cte_col_idx.");
        programAggregator_do_or_fail(
            aggregator->GroupByLinked(linked_proj_pos, vtcol));
      }
      else
      {
        require_prm(
            col_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
            "Linked GROUP BY column is not a stored-table column.");
        require_prm(col_ref.dict_column != NULL,
                    "Linked GROUP BY column descriptor is missing.");
        programAggregator_do_or_fail(
            aggregator->GroupByLinked(linked_proj_pos,
                                      col_ref.dict_column));
      }
      linked_proj_pos++;
    }
    groupby = groupby->next;
  }

  // Emit cross-table WHERE filters through the embedded normal interpreter.
  // Each failed filter stops this row's aggregation program before any
  // aggregate update.  Branch/comparison semantics stay in the normal
  // interpreter; the aggregation interpreter only consumes the stop result.
  // A hidden sentinel COUNT accumulator is appended so that groups where
  // no rows passed the filter can be suppressed at result time.
  bool has_embedded_filter = false;
  if (scope.cross_table_where_filters.size() > 0)
  {
    // Check if any remaining (non-consumed) cross-table filters exist
    for (Uint32 f = 0; f < scope.cross_table_where_filters.size(); f++) {
      CrossTableFilter& ctf = scope.cross_table_where_filters[f];
      if (ctf.ce != NULL)
        has_embedded_filter = true;
    }

    for (Uint32 f = 0; f < scope.cross_table_where_filters.size(); f++)
    {
      CrossTableFilter& ctf = scope.cross_table_where_filters[f];
      if (ctf.ce == NULL) continue;  // Consumed (converted to index bound)

      generate_embedded_filter_condition(
          aggregator, scope, ctf.ce,
          /*true_output=*/0,
          /*false_output=*/AGG_EMBEDDED_INTERP_STOP_PROGRAM,
          leaf_idx);
    }
  }

  // Program aggregations — same as single-table path
  assert(scope.agg != NULL);
  DynamicArray<AggregationAPICompiler::Instr>& program = scope.agg->m_program;
  for (Uint32 i = 0; i < program.size(); i++)
  {
    AggregationAPICompiler::Instr* instr = &program[i];
    Uint32 dest = instr->dest;
    Uint32 src = instr->src;
    switch (instr->type)
    {
    case AggregationAPICompiler::SVMInstrType::Load:
    {
      const QueryScope::ResolvedColumnRef& src_ref =
          scope.resolved_columns[src];
      require_prm(
          src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn ||
          src_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
          "Aggregation load source is not resolved to a table or CTE column.");
      if (src_ref.join_op_idx != leaf_idx)
      {
        const Uint32 src_op_idx = src_ref.join_op_idx;
        Uint32 lp_pos = find_or_add_linked_proj(
            scope.join_plan, src_op_idx,
            m_columns[src].c_str());
        if (src_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn &&
            cteVirtualTables != NULL &&
            cteVirtualTables[src_op_idx] != NULL)
        {
          // Non-leaf CTE: resolve the column from the CTE's virtual table.
          const Uint32 cte_col_idx = src_ref.cte_result_idx;
          const NdbDictionary::Column* vtcol =
              cteVirtualTables[src_op_idx]->getColumn((int)cte_col_idx);
          require_prm(vtcol != NULL,
                      "Non-leaf CTE load: virt table has no column at"
                      " cte_col_idx.");
          programAggregator_do_or_fail(
              aggregator->LoadLinkedColumn(lp_pos, dest, vtcol));
        }
        else
        {
          require_prm(
              src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
              "Linked aggregation load is not a stored-table column.");
          require_prm(src_ref.dict_column != NULL,
                      "Linked aggregation load column descriptor is missing.");
          programAggregator_do_or_fail(
              aggregator->LoadLinkedColumn(lp_pos, dest,
                                           src_ref.dict_column));
        }
      }
      else if (leafIsCte && cteLeafVirtTab != NULL)
      {
        // CTE leaf: agg-feed data arrives via the linked-attr buffer after
        // any parent linked projections. Final position is
        // num_linked_projs + cte_col_idx (see cte_base_pos in the GB loop).
        require_prm(
            src_ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
            "CTE leaf aggregation load is not a CTE result column.");
        const Uint32 cte_col_idx = src_ref.cte_result_idx;
        const NdbDictionary::Column* vtcol =
            cteLeafVirtTab->getColumn((int)cte_col_idx);
        require_prm(vtcol != NULL,
                    "CTE leaf load: virt table has no column at cte_col_idx.");
        programAggregator_do_or_fail(
            aggregator->LoadLinkedColumn(cte_base_pos + cte_col_idx,
                                          dest, vtcol));
      }
      else
      {
        require_prm(
            src_ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
            "Leaf aggregation load is not a stored-table column.");
        NdbAttrId col_id = src_ref.attr_id;
        if (!aggregator->LoadColumn(col_id, dest))
        {
          err << "Failed writing aggregation program "
                 "when attempting to load column "
              << quoted_identifier(m_columns[src]) << endl;
          throw RonSQLMaybeStaleSchema(
              "Failed writing aggregation program");
        }
      }
      break;
    }
    case AggregationAPICompiler::SVMInstrType::LoadConstantInteger:
      programAggregator_do_or_fail
        (aggregator->LoadInt64(scope.agg->m_constants[src].int_64, dest));
      break;
    case AggregationAPICompiler::SVMInstrType::Mov:
      programAggregator_do_or_fail(aggregator->Mov(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Add:
      programAggregator_do_or_fail(aggregator->Add(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Minus:
      programAggregator_do_or_fail(aggregator->Minus(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Mul:
      programAggregator_do_or_fail(aggregator->Mul(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Div:
      programAggregator_do_or_fail(aggregator->Div(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::DivInt:
      programAggregator_do_or_fail(aggregator->DivInt(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Rem:
      programAggregator_do_or_fail(aggregator->Mod(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Sum:
      programAggregator_do_or_fail(aggregator->Sum(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Min:
      programAggregator_do_or_fail(aggregator->Min(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Max:
      programAggregator_do_or_fail(aggregator->Max(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Count:
      programAggregator_do_or_fail(aggregator->Count(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::Greatest2:
    case AggregationAPICompiler::SVMInstrType::Least2:
      emit_pair_op_embedded(
          aggregator, dest, src,
          instr->type == AggregationAPICompiler::SVMInstrType::Greatest2,
          scope.agg->m_pair_op_needs_null_check[i]);
      break;
    case AggregationAPICompiler::SVMInstrType::AggRepeat:
      programAggregator_do_or_fail(aggregator->RepeatAgg(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::EmbeddedInterp:
    {
      auto& ci = scope.agg->m_cases[dest];
      Uint32 then_raw = scope.agg->raw_word_size(ci.then_start, ci.skip_pos);
      Uint32 skip_raw = 1;
      Uint32 then_arm_total = then_raw + skip_raw;
      generate_embedded_condition(aggregator, scope, ci.condition,
                                  then_arm_total, cteVirtualTables);
      break;
    }
    case AggregationAPICompiler::SVMInstrType::Skip:
    {
      for (Uint32 c = 0; c < scope.agg->m_cases.size(); c++)
      {
        if (scope.agg->m_cases[c].skip_pos == i)
        {
          auto& ci = scope.agg->m_cases[c];
          Uint32 else_raw = scope.agg->raw_word_size(ci.else_start, ci.else_end);
          programAggregator_do_or_fail(aggregator->Skip(else_raw));
          break;
        }
      }
      break;
    }
    default:
      abort();
    }
  }

  // Emit hidden sentinel COUNT: counts rows that passed cross-table filters.
  // Groups where sentinel is 0 are suppressed at result time.
  if (has_embedded_filter) {
    // Sentinel slot = next slot after all user-visible aggregate slots
    Uint32 sentinel_slot = 0;
    for (Uint32 i = 0; i < program.size(); i++) {
      auto t = program[i].type;
      if (t == AggregationAPICompiler::SVMInstrType::Sum ||
          t == AggregationAPICompiler::SVMInstrType::Count ||
          t == AggregationAPICompiler::SVMInstrType::Min ||
          t == AggregationAPICompiler::SVMInstrType::Max ||
          t == AggregationAPICompiler::SVMInstrType::AggRepeat) {
        if (program[i].dest >= sentinel_slot)
          sentinel_slot = program[i].dest + 1;
      }
    }
    Uint32 reg = 0;
    programAggregator_do_or_fail(aggregator->LoadUint64(1, reg));
    programAggregator_do_or_fail(aggregator->Count(sentinel_slot, reg));
    // sentinel_agg_slot already set in compile() before ResultPrinter creation
  }
}

void
RonSQLPreparer::require_cte_case_condition_column_output(QueryScope& scope,
                                                         Uint32 op_idx,
                                                         Uint32 cidx)
{
  require_run(scope.resolved_columns != NULL,
              "CASE over CTE: missing resolved columns.");
  const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[cidx];
  require_prm(
      ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn &&
      ref.join_op_idx == op_idx,
      "CASE over CTE: column is not resolved to the requested CTE op.");
  const JoinOp& cte_op = scope.join_plan.ops[op_idx];
  require_run(cte_op.cte_def != NULL,
              "CASE over CTE: op has no CTE definition.");
  require_run(ref.cte_def_idx == cte_op.cte_def_idx,
              "CASE over CTE: descriptor CTE index mismatch.");
  const Outputs* o = ref.cte_output;
  require_prm(o != NULL,
              "CASE over CTE: output index out of range.");
  require_prm(o->type == Outputs::Type::COLUMN,
              "CASE condition referencing a CTE aggregate output "
              "is not yet supported; reference a CTE column projection "
              "instead, or move the predicate into the CTE body's "
              "WHERE clause.");
}

const NdbDictionary::Column*
RonSQLPreparer::resolve_case_condition_column(
    QueryScope& scope,
    ConditionalExpression* col_side,
    NdbDictionary::Table* const* cteVirtualTables)
{
  Uint32 cidx = col_side->col_idx;
  require_run(scope.resolved_columns != NULL,
              "CASE condition: missing resolved columns.");
  const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[cidx];
  if (ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn) {
    Uint32 op_idx = ref.join_op_idx;
    if (cteVirtualTables != NULL && op_idx < scope.join_plan.num_ops &&
        cteVirtualTables[op_idx] != NULL) {
      const NdbDictionary::Column* vtcol =
          cteVirtualTables[op_idx]->getColumn((int)ref.cte_result_idx);
      require_prm(vtcol != NULL,
                  "CASE over CTE: virt-table missing column at "
                  "cte_col_idx.");
      return vtcol;
    }
    return NULL;
  }
  require_prm(ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn,
              "CASE condition: column is not a stored-table column.");
  return ref.dict_column;
}

// Phase I.5 v7 — emit one Greatest2 / Least2 pair-op.  The embedded
// program shape depends on `needs_null_check`:
//
// `needs_null_check == true` (14-word embedded body, 18 total agg words):
//   0   READ_AGG_REG_TO_REG(dest → r1)
//   1   READ_AGG_REG_TO_REG(src  → r2)
//   2   BRANCH_REG_EQ_NULL(r1) | (9 << 16)        → land at PC 11 if r1 == NULL
//   3   BRANCH_REG_EQ_NULL(r2) | (8 << 16)        → land at PC 11 if r2 == NULL
//   4   BRANCH_(GE|LE)_REG_REG(R2=2, R1=1) | (4 << 16)  cmp picks output
//   5   LoadConst16(r3, 0)                         output=0 → run trailing Mov
//   6   WriteInterpreterOutput(r3, 0)
//   7   ExitOK
//   8   LoadConst16(r3, 1)             ← cmp lands here; output=1 → skip Mov
//   9   WriteInterpreterOutput(r3, 0)
//   10  ExitOK
//   11  LoadConst16(r3, 2)             ← NULL lands here; output=2 → SetRegNull
//   12  WriteInterpreterOutput(r3, 0)
//   13  ExitOK
// After the embedded body:
//   Mov(dest, src)
//   Skip(1)
//   SetRegNull(dest)
//
// output=0 lands on Mov, then Skip(1) skips SetRegNull.
// output=1 lands on Skip(1), preserving dest and skipping SetRegNull.
// output=2 lands on SetRegNull(dest), making only this expression NULL.
//
// `needs_null_check == false` (9-word body — Phase I.5 v4 fast path):
//   0   READ_AGG_REG_TO_REG(dest → r1)
//   1   READ_AGG_REG_TO_REG(src  → r2)
//   2   BRANCH_(GE|LE)_REG_REG(R2=2, R1=1) | (4 << 16)
//   3   LoadConst16(r3, 0)
//   4   WriteInterpreterOutput(r3, 0)
//   5   ExitOK
//   6   LoadConst16(r3, 1)
//   7   WriteInterpreterOutput(r3, 0)
//   8   ExitOK
// (then Mov(dest, src) at +9)
//
// Branch register slots: handler reads getReg1 from bits 6..8 (encoded
// as Reg2) and getReg2 from bits 9..11 (encoded as Reg1).  For
// `r1 op r2` the encoded form is therefore Branch(op, Reg1=r2,
// Reg2=r1).  The handler then evaluates `r1 GE r2` for GREATEST
// (branch when dest already holds the max → skip Mov), or `r1 LE r2`
// for LEAST.
void
RonSQLPreparer::emit_pair_op_embedded(NdbAggregator* aggregator,
                                       Uint32 dest,
                                       Uint32 src,
                                       bool is_greatest,
                                       bool needs_null_check)
{
  Uint32 cmp = is_greatest ? Interpreter::BRANCH_GE_REG_REG
                           : Interpreter::BRANCH_LE_REG_REG;
  if (needs_null_check)
  {
    programAggregator_do_or_fail(aggregator->EmbeddedInterp(14));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ReadAggRegIntoReg(dest, 1)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ReadAggRegIntoReg(src, 2)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::Branch(Interpreter::BRANCH_REG_EQ_NULL,
                            /*Reg1=*/0, /*Reg2=*/1) | (9 << 16)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::Branch(Interpreter::BRANCH_REG_EQ_NULL,
                            /*Reg1=*/0, /*Reg2=*/2) | (8 << 16)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::Branch(cmp,
                            /*Reg1=*/2, /*Reg2=*/1) | (4 << 16)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::LoadConst16(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::WriteInterpreterOutput(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ExitOK()));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::LoadConst16(3, 1)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::WriteInterpreterOutput(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ExitOK()));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::LoadConst16(3, 2)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::WriteInterpreterOutput(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ExitOK()));
  }
  else
  {
    programAggregator_do_or_fail(aggregator->EmbeddedInterp(9));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ReadAggRegIntoReg(dest, 1)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ReadAggRegIntoReg(src, 2)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::Branch(cmp,
                            /*Reg1=*/2, /*Reg2=*/1) | (4 << 16)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::LoadConst16(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::WriteInterpreterOutput(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ExitOK()));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::LoadConst16(3, 1)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::WriteInterpreterOutput(3, 0)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::ExitOK()));
  }
  programAggregator_do_or_fail(aggregator->Mov(dest, src));
  if (needs_null_check)
  {
    programAggregator_do_or_fail(aggregator->Skip(1));
    programAggregator_do_or_fail(aggregator->SetRegNull(dest));
  }
}

// Phase I.5 v4 fast path — recursively walk a Greatest2 / Least2
// Expr tree to determine whether any leaf Load reaches a nullable
// column.  Returns true (conservative) when a leaf can't be resolved
// to a non-nullable column descriptor.
bool
RonSQLPreparer::compute_pair_op_needs_null_check(
    const QueryScope& scope,
    AggregationAPICompiler::Expr* expr) const
{
  if (expr == NULL) return true;
  if (expr->isLoadConstantInt()) return false;
  if (expr->isLoad())
  {
    if (scope.resolved_columns == NULL) return true;
    Uint32 col_idx = expr->getLoadIdx();
    const QueryScope::ResolvedColumnRef& ref = scope.resolved_columns[col_idx];
    if (ref.kind != QueryScope::ResolvedColumnRef::Kind::StoredColumn)
      return true;  // CTE virt — descriptor not yet built here.
    if (ref.dict_column == NULL) return true;
    return ref.dict_column->getNullable();
  }
  if (expr->isGreatest2() || expr->isLeast2())
  {
    return compute_pair_op_needs_null_check(scope, expr->getLeft())
        || compute_pair_op_needs_null_check(scope, expr->getRight());
  }
  // Other arithmetic ops (Add / Mul / etc.) — pair-ops aren't nested
  // inside arithmetic in the lowering today, but if that ever
  // changes we conservatively assume the operand could be NULL.
  return true;
}

// Phase I.5 v4 fast path — populate scope.agg->m_pair_op_needs_null_check
// once per scope before any raw_word_size or pair-op emission
// consumes it.  Idempotent: re-running is a no-op once the array is
// the right size.
void
RonSQLPreparer::prepare_pair_op_null_check_cache(QueryScope& scope)
{
  if (scope.agg == NULL) return;
  AggregationAPICompiler* agg = scope.agg;
  if (agg->m_pair_op_needs_null_check.size() == agg->m_program.size())
    return;
  agg->m_pair_op_needs_null_check.truncate();
  for (Uint32 i = 0; i < agg->m_program.size(); i++)
  {
    AggregationAPICompiler::SVMInstrType t = agg->m_program[i].type;
    if (t == AggregationAPICompiler::SVMInstrType::Greatest2 ||
        t == AggregationAPICompiler::SVMInstrType::Least2)
    {
      AggregationAPICompiler::Expr* expr = agg->m_pair_op_program_exprs[i];
      agg->m_pair_op_needs_null_check.push(
          compute_pair_op_needs_null_check(scope, expr));
    }
    else
    {
      agg->m_pair_op_needs_null_check.push(false);
    }
  }
}

void
RonSQLPreparer::validate_implicit_scalar_pair_op_expr(
    AggregationAPICompiler::Expr* expr) const
{
  require_prm(expr != NULL,
              "Top-level GREATEST/LEAST has an invalid operand.");
  if (expr->isLoadConstantInt())
  {
    return;
  }
  if (expr->isLoad())
  {
    Uint32 col_idx = expr->getLoadIdx();
    require_prm(col_idx < m_column_qualifiers.size(),
                "Top-level GREATEST/LEAST references an unknown column.");
    const LexCString& qualifier = m_column_qualifiers[col_idx];
    require_prm(is_scalar_cte_qualifier(qualifier),
                "Top-level GREATEST/LEAST is only supported for scalar "
                "CTE output columns.  Use an explicit aggregate for "
                "ordinary table columns.");
    return;
  }
  if (expr->isGreatest2() || expr->isLeast2())
  {
    validate_implicit_scalar_pair_op_expr(expr->getLeft());
    validate_implicit_scalar_pair_op_expr(expr->getRight());
    return;
  }
  require_prm(false,
              "Top-level GREATEST/LEAST operand must be a scalar CTE "
              "column or integer constant.");
}

bool
RonSQLPreparer::is_scalar_cte_qualifier(const LexCString& qualifier) const
{
  if (qualifier.str == NULL) return false;
  for (const CteDefinition* cte = m_context.ast_root.cte_list;
       cte != NULL; cte = cte->next)
  {
    if (cte->name.len == qualifier.len &&
        strncmp(cte->name.str, qualifier.str, qualifier.len) == 0)
    {
      /* Single-row CTEs also have no GROUP BY but are not scalar —
       * the implicit top-level GREATEST/LEAST MAX-wrapping (I.17/I.21)
       * stays scalar-only: over an EMPTY single-row CTE it would turn
       * the empty result into a NULL row. */
      return cte->stmt->groupby_columns == NULL &&
             !cte->stmt->is_single_row_cte;
    }
  }
  return false;
}

void
RonSQLPreparer::validate_implicit_scalar_pair_ops()
{
  for (const Outputs* out = m_context.ast_root.outputs;
       out != NULL; out = out->next)
  {
    if (out->type != Outputs::Type::AGGREGATE ||
        !out->aggregate.implicit_scalar_pair_op)
    {
      continue;
    }
    validate_implicit_scalar_pair_op_expr(out->aggregate.arg);
  }
}

void
RonSQLPreparer::validate_greatest_least_pair_loads()
{
  for (Uint32 i = 0; i < m_greatest_least_pair_loads.size(); i++)
  {
    AggregationAPICompiler::Expr* load_expr = m_greatest_least_pair_loads[i];
    Uint32 col_idx = load_expr->getLoadIdx();
    // The Expr* belongs to either the main scope's compiler or one of
    // the CTE scopes' compilers; same col_idx may appear in multiple
    // scopes' resolved column descriptors (e.g. when a CTE references a parent
    // column).  Find the scope that owns the Expr and validate
    // against that scope's descriptor.
    QueryScope* owning_scope = NULL;
    if (m_main_scope.agg != NULL && m_main_scope.agg->owns_expr(load_expr))
    {
      owning_scope = &m_main_scope;
    }
    else
    {
      for (Uint32 c = 0; c < m_cte_scopes.size(); c++)
      {
        QueryScope* cs = m_cte_scopes[c];
        if (cs->agg != NULL && cs->agg->owns_expr(load_expr))
        {
          owning_scope = cs;
          break;
        }
      }
    }
    require_run(owning_scope != NULL,
                "GREATEST/LEAST pair-op operand: failed to locate "
                "owning scope.  Please report a bug.");
    require_run(owning_scope->resolved_columns != NULL,
                "GREATEST/LEAST pair-op operand: scope resolved columns "
                "not initialised.  Please report a bug.");
    const QueryScope::ResolvedColumnRef& ref =
        owning_scope->resolved_columns[col_idx];
    if (ref.kind != QueryScope::ResolvedColumnRef::Kind::StoredColumn)
    {
      // CTE COLUMN / AGGREGATE projection: the virtual-table column
      // descriptor isn't built until build_cte_virtual_tables runs at
      // execute time.  Skip the type check here.  v7 propagates NULL
      // at runtime via the pair-op embedded program (BRANCH_REG_EQ_NULL
      // to SetRegNull), so nullability is no longer a parser-time
      // concern even when the descriptor was available.
      continue;
    }
    const NdbDictionary::Column* col = ref.dict_column;
    require_run(col != NULL,
                "GREATEST/LEAST pair-op operand: stored column descriptor "
                "missing.  Please report a bug.");
    // Phase I.5 v7: nullable column operands are supported.  The
    // pair-op embedded program detects NULL via BRANCH_REG_EQ_NULL
    // and lands on SetRegNull(dest), so only the current expression
    // becomes NULL.  The following SUM/MIN/MAX/COUNT(expr) skips that
    // input, while unrelated aggregate outputs still update.
    // Integer-only restriction.  Pair-ops emit LoadColumn into a
    // register and embedded-interpreter compare / Mov.  The kernel's kOpLoadCol
    // widens every integer type (Tinyint .. Bigint, signed +
    // unsigned) to NDB_TYPE_BIGINT in the register via AlignedType
    // (AggInterpreter.cpp:411), so all integer widths compare
    // correctly through the embedded compare.  Float / Decimal / VARCHAR /
    // string operands are out of scope (deferred to I.5 v3 / I.6).
    NdbDictionary::Column::Type t = col->getType();
    require_prm(t == NdbDictionary::Column::Tinyint ||
                t == NdbDictionary::Column::Tinyunsigned ||
                t == NdbDictionary::Column::Smallint ||
                t == NdbDictionary::Column::Smallunsigned ||
                t == NdbDictionary::Column::Mediumint ||
                t == NdbDictionary::Column::Mediumunsigned ||
                t == NdbDictionary::Column::Int ||
                t == NdbDictionary::Column::Unsigned ||
                t == NdbDictionary::Column::Bigint ||
                t == NdbDictionary::Column::Bigunsigned,
                "GREATEST/LEAST column operand must be an integer "
                "type.  Float / Decimal / VARCHAR operands are not "
                "yet supported (deferred to I.5 v3 / I.6).");
  }
}

void
RonSQLPreparer::generate_embedded_condition(
    NdbAggregator* aggregator,
    QueryScope& scope,
    ConditionalExpression* ce,
    Uint32 then_arm_raw_size,
    NdbDictionary::Table* const* cteVirtualTables,
    bool use_custom_outputs,
    Uint32 first_exit_output,
    Uint32 second_exit_output,
    Uint32 agg_leaf_idx_override)
{
  // Two patterns supported:
  //   OR: (col = 'X' OR col = 'Y') → branch to THEN on match, fall-through ELSE
  //   AND: (col <> 'X' AND col <> 'Y') → branch to ELSE on inverted match,
  //                                       fall-through THEN
  //   Single atom: handled as OR with one atom
  //
  // Each atom is one of:
  //   col-vs-const : LHS is a column, RHS is a literal — uses
  //                  BRANCH_ATTR_OP_ARG / BRANCH_MEM_OP_ARG /
  //                  BRANCH_MEM_OP_ARG_INLINE_TYPE.
  //   col-vs-col   : LHS and RHS are both columns — uses
  //                  READ_*-into-register loads + BRANCH_*_REG_REG.
  //                  Bigint-only in v2a; the kernel's only signed
  //                  memory-to-register opcode is READ_INT64_MEM_TO_REG.
  //
  // Each atom emits a SELF-CONTAINED sequence so multiple atoms can
  // reference different LHS columns without aliasing on heap[0].
  DynamicArray<ConditionalExpression*> atoms(m_amalloc);
  bool is_and = false;

  if (ce->op == T_AND || ce->op == T_OR)
  {
    is_and = (ce->op == T_AND);
    TokenKind flatten_op = ce->op;
    ConditionalExpression* node = ce;
    while (node->op == flatten_op)
    {
      atoms.push(node->args.right);
      node = node->args.left;
    }
    atoms.push(node);
  }
  else
  {
    atoms.push(ce);
  }
  for (Uint32 a = 0; a < atoms.size(); a++)
  {
    atoms[a] = simplify_ce(atoms[a], -1);
  }
  require_run(scope.resolved_columns != NULL,
              "CASE condition: missing resolved columns.");

  // Per-atom resolution info, populated in the pre-pass and reused
  // verbatim in the emit pass.
  Uint32 leaf_idx = scope.join_plan.agg_leaf_idx;
  if (agg_leaf_idx_override != 0xFFFFFFFF) {
    leaf_idx = agg_leaf_idx_override;
  }

  enum class SideKind {
    // col-vs-const RHS literal — only valid for `info[].rhs`.
    Constant,
    // Leaf-table column on the agg leaf op (uses READ_ATTR_INTO_REG
    // for the register path, or BRANCH_ATTR_OP_ARG for col-vs-const).
    LeafTable,
    // Parent-table column from a non-leaf op, fed via the linked-attr
    // buffer (READ_LINKED_TO_MEM).  Type info comes from the parent
    // table descriptor (BRANCH_MEM_OP_ARG path), or is taken as INT64
    // for the register path.
    LinkedParent,
    // CTE COLUMN projection on a non-leaf CTE op — same opcode family
    // as LinkedParent but the parent-table descriptor is resolved via
    // the CTE body's source op.
    LinkedCteCol,
    // CTE leaf column (the agg leaf is itself a CTE op): uses inline
    // type metadata via BRANCH_MEM_OP_ARG_INLINE_TYPE for the
    // col-vs-const path.  v2a does *not* support col-vs-col over an
    // inline-linked side (would need an inline-typed register-load
    // opcode); reject cleanly.
    InlineLinked
  };

  struct SideInfo {
    SideKind kind;
    Uint32 col_idx;                       // RonSQL column idx for the side
    Uint32 linked_position;                // valid when kind != Leaf*
    NdbAttrId attr_id;                     // valid for Leaf* / LinkedParent / LinkedCteCol
    const NdbDictionary::Column* col;      // resolved column descriptor
    const NdbDictionary::Table* parent_table;  // for LinkedParent / LinkedCteCol BRANCH_MEM_OP_ARG
    // Constant operand only:
    raw_value rv;
    Uint32 byte_len;
    Uint32 data_words;
  };

  struct AtomInfo {
    SideInfo lhs;
    SideInfo rhs;
    bool is_col_vs_col = false;
    /* Phase I.5 v3b: float column LHS with numeric-literal RHS.
     * Emits register-based compare with LOAD_DOUBLE_CONST for the
     * RHS instead of the integer-only BRANCH_*_OP_ARG path. */
    bool is_float_const = false;
    /* For is_float_const: the constant pre-converted to double. */
    double float_const_val = 0.0;
    Uint32 word_count = 0;
  };

  std::function<void(ConditionalExpression*, SideInfo&)> resolve_col_side =
      [&](ConditionalExpression* col_side, SideInfo& out)
      {
        ndbrequire(col_side->op == T_IDENTIFIER);
        Uint32 cidx = col_side->col_idx;
        out.col_idx = cidx;
        out.linked_position = 0;
        out.attr_id = 0;
        out.col = NULL;
        out.parent_table = NULL;
        const QueryScope::ResolvedColumnRef& ref =
            scope.resolved_columns[cidx];
        require_prm(
            ref.kind == QueryScope::ResolvedColumnRef::Kind::StoredColumn ||
            ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn,
            "CASE condition: column is not resolved to a table or CTE "
            "column.");
        Uint32 op_idx = ref.join_op_idx;
        bool op_is_cte =
            ref.kind == QueryScope::ResolvedColumnRef::Kind::CteResultColumn;
        if (op_is_cte) {
          require_cte_case_condition_column_output(scope, op_idx, cidx);
        }
        if (op_idx != leaf_idx) {
          // Linked column (parent-table or non-leaf CTE column).
          out.linked_position = find_or_add_linked_proj(
              scope.join_plan, op_idx, m_columns[cidx].c_str());
          if (op_is_cte) {
            const JoinOp& cte_op = scope.join_plan.ops[op_idx];
            require_run(cte_op.cte_def != NULL,
                        "CASE over CTE: op has no CTE definition.");
            require_run(cte_op.cte_def_idx < m_cte_scopes.size(),
                        "CASE over CTE: cte_def_idx out of range.");
            QueryScope* cs = m_cte_scopes[cte_op.cte_def_idx];
            require_run(cs != NULL,
                        "CASE over CTE: missing CTE body scope.");
            const Outputs* o = ref.cte_output;
            require_prm(o != NULL,
                        "CASE over CTE: output index out of range.");
            require_prm(o->type == Outputs::Type::COLUMN,
                        "CASE condition referencing a CTE aggregate "
                        "output is not yet supported — would need an "
                        "inline-type branch opcode.  Reference a CTE "
                        "column projection instead, or move the "
                        "predicate into the CTE body's WHERE clause.");
            Uint32 src_col_idx = o->column.col_idx;
            require_run(cs->resolved_columns != NULL,
                        "CASE over CTE: CTE body missing resolved columns.");
            const QueryScope::ResolvedColumnRef& src_ref =
                cs->resolved_columns[src_col_idx];
            require_prm(
                src_ref.kind ==
                QueryScope::ResolvedColumnRef::Kind::StoredColumn,
                "CASE over CTE: source column is not a stored-table "
                "column.");
            const NdbDictionary::Column* src_col = src_ref.dict_column;
            require_prm(src_col != NULL,
                        "CASE over CTE: source column not resolved.");
            Uint32 src_op_idx = src_ref.join_op_idx;
            require_prm(src_op_idx < cs->join_plan.num_ops,
                        "CASE over CTE: source op out of range.");
            out.parent_table = cs->join_plan.ops[src_op_idx].table;
            require_prm(out.parent_table != NULL,
                        "CASE over CTE: source op has no physical table.");
            out.col = src_col;
            out.attr_id = (NdbAttrId)src_col->getColumnNo();
            out.kind = SideKind::LinkedCteCol;
          } else {
            out.col = ref.dict_column;
            require_prm(out.col != NULL,
                        "CASE condition: unresolved column descriptor.");
            out.attr_id = ref.attr_id;
            out.parent_table = scope.join_plan.ops[op_idx].table;
            require_prm(out.parent_table != NULL,
                        "CASE condition: linked parent op has no "
                        "physical table.");
            out.kind = SideKind::LinkedParent;
          }
        } else if (op_is_cte) {
          // CTE leaf column: inline-type encoding.
          out.linked_position = scope.join_plan.num_linked_projs +
              ref.cte_result_idx;
          out.col = resolve_case_condition_column(
              scope, col_side, cteVirtualTables);
          require_prm(out.col != NULL,
                      "CASE over CTE leaf: unresolved virtual column.");
          out.kind = SideKind::InlineLinked;
        } else {
          // Plain leaf-table column on the agg leaf op.
          out.col = ref.dict_column;
          require_prm(out.col != NULL,
                      "CASE condition: unresolved column descriptor.");
          out.attr_id = ref.attr_id;
          out.kind = SideKind::LeafTable;
        }
      };

  // Per-atom resolution (pre-pass).
  DynamicArray<AtomInfo> infos(m_amalloc);
  for (Uint32 a = 0; a < atoms.size(); a++) infos.push(AtomInfo());
  Uint32 total_atom_words = 0;
  for (Uint32 a = 0; a < atoms.size(); a++)
  {
    ConditionalExpression* atom = atoms[a];
    require_prm(atom->op == T_EQUALS || atom->op == T_NOT_EQUALS ||
                atom->op == T_LT || atom->op == T_LE ||
                atom->op == T_GT || atom->op == T_GE,
                "CASE condition atom: only =, !=, <, <=, >, >= "
                "supported.");
    require_prm(atom->args.left != NULL &&
                atom->args.left->op == T_IDENTIFIER,
                "CASE condition atom: left side must resolve to a "
                "column after simplification.");
    AtomInfo& info = infos[a];

    resolve_col_side(atom->args.left, info.lhs);

    info.is_col_vs_col = (atom->args.right->op == T_IDENTIFIER);
    if (info.is_col_vs_col)
    {
      resolve_col_side(atom->args.right, info.rhs);
      // Register-based path supports every NDB integer width.  Leaf
      // operands use READ_ATTR_INTO_REG (DBTUP's type-aware loader);
      // linked operands use READ_LINKED_COLUMN_TO_REG (Phase I.5 v5)
      // which decodes by NDB type with correct sign extension.
      std::function<bool(const NdbDictionary::Column*)> can_load_typed_reg =
          [](const NdbDictionary::Column* c)
      {
        switch (c->getType()) {
        case NdbDictionary::Column::Tinyint:
        case NdbDictionary::Column::Tinyunsigned:
        case NdbDictionary::Column::Smallint:
        case NdbDictionary::Column::Smallunsigned:
        case NdbDictionary::Column::Mediumint:
        case NdbDictionary::Column::Mediumunsigned:
        case NdbDictionary::Column::Int:
        case NdbDictionary::Column::Unsigned:
        case NdbDictionary::Column::Bigint:
        case NdbDictionary::Column::Bigunsigned:
        case NdbDictionary::Column::Float:
        case NdbDictionary::Column::Double:
          return true;
        default:
          return false;
        }
      };
      require_prm(info.lhs.kind != SideKind::InlineLinked &&
                  info.rhs.kind != SideKind::InlineLinked,
                  "CASE condition with two CTE-leaf columns is not yet "
                  "supported (deferred to I.5 v5 — needs an inline-typed "
                  "register-load opcode).");
      require_prm(can_load_typed_reg(info.lhs.col) &&
                  can_load_typed_reg(info.rhs.col),
                  "Column-vs-column CASE / GREATEST / LEAST currently "
                  "supports integer + float / double types only "
                  "(VARCHAR / DECIMAL deferred to I.6).");
      // Word counts (Phase I.5 v5: linked side is now one word —
      // READ_LINKED_COLUMN_TO_REG):
      //   LeafTable side       → 1 word (READ_ATTR_INTO_REG)
      //   LinkedParent / Cte   → 1 word (READ_LINKED_COLUMN_TO_REG)
      // + 1 word for the BRANCH_*_REG_REG.
      info.word_count = 1u + 1u + 1u;
    }
    else
    {
      info.rhs.kind = SideKind::Constant;
      // Phase I.5 v3b: detect float column LHS with numeric literal
      // RHS — emit register-based compare via LOAD_DOUBLE_CONST
      // instead of the integer-only BRANCH_*_OP_ARG family.
      ConditionalExpression* rhs_simplified = atom->args.right;
      bool lhs_is_float =
          info.lhs.col->getType() == NdbDictionary::Column::Float ||
          info.lhs.col->getType() == NdbDictionary::Column::Double;
      info.is_float_const = lhs_is_float &&
          (rhs_simplified->op == T_INT ||
           rhs_simplified->op == T_FLOAT);
      if (info.is_float_const) {
        require_prm(info.lhs.kind != SideKind::InlineLinked,
                    "Float / double CTE-leaf column compared to a "
                    "literal in CASE is not yet supported (would need "
                    "an inline-typed double-load opcode).");
        if (rhs_simplified->op == T_INT) {
          info.float_const_val =
              static_cast<double>(rhs_simplified->constant_integer);
        } else {
          info.float_const_val = rhs_simplified->constant_float.dbl;
        }
        // 1 word: read LHS into R1
        // 3 words: LOAD_DOUBLE_CONST(R2) + 2 data words
        // 1 word: BRANCH_*_REG_REG R2, R1
        info.word_count = 1u + 3u + 1u;
      } else {
        // col-vs-const path (v1): resolve the constant against the LHS
        // column descriptor.  Use the simplified node so negative
        // literals fold into T_INT / T_FLOAT here too.
        info.rhs.rv = encode_constant(rhs_simplified, info.lhs.col);
        info.rhs.byte_len = info.rhs.rv.len;
        info.rhs.data_words = (info.rhs.byte_len + 3) / 4;
        // BRANCH_ATTR_OP_ARG       : 2 words + data
        // BRANCH_MEM_OP_ARG        : 4 words + data, plus 1 for READ_LINKED_TO_MEM
        // BRANCH_MEM_OP_ARG_INLINE : 3 words + data, plus 1 for READ_LINKED_TO_MEM
        switch (info.lhs.kind) {
          case SideKind::LeafTable:
            info.word_count = 2 + info.rhs.data_words;
            break;
          case SideKind::LinkedParent:
          case SideKind::LinkedCteCol:
            info.word_count = 1 + 4 + info.rhs.data_words;
            break;
          case SideKind::InlineLinked:
            info.word_count = 1 + 3 + info.rhs.data_words;
            break;
          case SideKind::Constant:
            abort();  // unreachable — LHS is always a column
        }
      }
    }
    total_atom_words += info.word_count;
  }

  Uint32 emb_len = total_atom_words + 6;

  // For OR:  branches → second_exit (THEN), fall-through → first_exit (ELSE)
  // For AND: branches → second_exit (ELSE), fall-through → first_exit (THEN)
  Uint32 second_exit_label = emb_len - 3;
  if (!use_custom_outputs) {
    first_exit_output = is_and ? 0 : then_arm_raw_size;
    second_exit_output = is_and ? then_arm_raw_size : 0;
  }

  programAggregator_do_or_fail(aggregator->EmbeddedInterp(emb_len));

  Uint32 pos = 0;
  for (Uint32 a = 0; a < atoms.size(); a++)
  {
    ConditionalExpression* atom = atoms[a];
    AtomInfo& info = infos[a];

    // Branch instruction position within the atom: for col-vs-col
    // (and v3b float-const) it is the last word (after both register
    // loads); for col-vs-const linked / inline-linked it sits right
    // after the READ_LINKED_TO_MEM staging word (at pos+1); for
    // col-vs-const leaf it is the first word.
    Uint32 branch_instr_pos;
    if (info.is_col_vs_col || info.is_float_const) {
      branch_instr_pos = pos + info.word_count - 1;
    } else if (info.lhs.kind == SideKind::LeafTable) {
      branch_instr_pos = pos;
    } else {
      branch_instr_pos = pos + 1;
    }
    Uint32 branch_offset = second_exit_label - branch_instr_pos;

    // For OR: branch to THEN when atom holds (direct cond).
    // For AND: invert the atom condition so we branch to ELSE on miss.
    //
    // The embedded BRANCH_*_OP_ARG family inverts inequality
    // conditions (kernel `Interpreter::LT/LE/GT/GE` branch on the
    // *opposite* relation between col and const — see
    // DbtupExecQuery.cpp handleBranchMemOpArg and CLAUDE.md
    // "NdbInterpretedCode: Inverted Inequality Branches").  The
    // BRANCH_*_REG_REG family does *not* invert (handleBranchLtRegReg
    // branches when Tleft0 < Tright0, matching the name).  Maintain
    // two mappings:
    //   _arg  family direct: T_EQ→EQ T_NEQ→NE T_LT→GT T_LE→GE T_GT→LT T_GE→LE
    //   _arg  family invert: T_EQ→NE T_NEQ→EQ T_LT→LE T_LE→LT T_GT→GE T_GE→GT
    //   _reg  family direct: T_EQ→EQ T_NEQ→NE T_LT→LT T_LE→LE T_GT→GT T_GE→GE
    //   _reg  family invert: T_EQ→NE T_NEQ→EQ T_LT→GE T_LE→GT T_GT→LE T_GE→LT
    std::function<Uint32(TokenKind)> cond_for_arg_family =
        [&](TokenKind op) -> Uint32 {
      if (is_and) {
        switch (op) {
          case T_EQUALS:     return Interpreter::NE;
          case T_NOT_EQUALS: return Interpreter::EQ;
          case T_LT:         return Interpreter::LE;
          case T_LE:         return Interpreter::LT;
          case T_GT:         return Interpreter::GE;
          case T_GE:         return Interpreter::GT;
          default: abort();
        }
      }
      switch (op) {
        case T_EQUALS:     return Interpreter::EQ;
        case T_NOT_EQUALS: return Interpreter::NE;
        case T_LT:         return Interpreter::GT;
        case T_LE:         return Interpreter::GE;
        case T_GT:         return Interpreter::LT;
        case T_GE:         return Interpreter::LE;
        default: abort();
      }
    };
    std::function<Uint32(TokenKind)> reg_branch_opcode =
        [&](TokenKind op) -> Uint32 {
      if (is_and) {
        switch (op) {
          case T_EQUALS:     return Interpreter::BRANCH_NE_REG_REG;
          case T_NOT_EQUALS: return Interpreter::BRANCH_EQ_REG_REG;
          case T_LT:         return Interpreter::BRANCH_GE_REG_REG;
          case T_LE:         return Interpreter::BRANCH_GT_REG_REG;
          case T_GT:         return Interpreter::BRANCH_LE_REG_REG;
          case T_GE:         return Interpreter::BRANCH_LT_REG_REG;
          default: abort();
        }
      }
      switch (op) {
        case T_EQUALS:     return Interpreter::BRANCH_EQ_REG_REG;
        case T_NOT_EQUALS: return Interpreter::BRANCH_NE_REG_REG;
        case T_LT:         return Interpreter::BRANCH_LT_REG_REG;
        case T_LE:         return Interpreter::BRANCH_LE_REG_REG;
        case T_GT:         return Interpreter::BRANCH_GT_REG_REG;
        case T_GE:         return Interpreter::BRANCH_GE_REG_REG;
        default: abort();
      }
    };

    if (info.is_col_vs_col || info.is_float_const)
    {
      // Register-based path.  Two static registers: R1 (LHS), R2 (RHS).
      // The embedded interpreter has its own register file and is
      // re-initialised per kOpEmbeddedInterp call, so register reuse
      // across atoms is safe (each atom is self-contained).
      const Uint32 R1 = 1;
      const Uint32 R2 = 2;
      std::function<void(const SideInfo&, Uint32)> emit_load_into_reg =
          [&](const SideInfo& s, Uint32 reg)
          {
            if (s.kind == SideKind::LeafTable) {
              // READ_ATTR_INTO_REG handles the column's native type
              // (sign extension done by DBTUP's existing path).
              programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
                  Interpreter::Read((Uint32)s.attr_id, reg)));
            } else {
              // Phase I.5 v5: one-word type-aware load from the
              // linked-attr buffer.  Replaces the previous
              // READ_LINKED_TO_MEM + READ_*_MEM_TO_REG_CONST(reg, 4)
              // sequence (which zero-extended signed sub-Bigint
              // operands).  The kernel handler decodes by NDB type
              // and writes a sign- or zero-extended value plus the
              // register's NULL_INDICATOR.
              programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
                  Interpreter::ReadLinkedColumnIntoReg(
                      reg, s.linked_position,
                      (Uint32)s.col->getType())));
            }
          };
      emit_load_into_reg(info.lhs, R1);
      if (info.is_float_const) {
        // Phase I.5 v3b: load the literal as a double immediate.
        // LOAD_DOUBLE_CONST occupies one opcode word followed by
        // two data words (low, high) carrying the IEEE-754 bit
        // pattern, matching LOAD_CONST64's encoding.
        union { double d; Uint32 w[2]; } pun;
        pun.d = info.float_const_val;
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::LoadDoubleConst(R2)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(pun.w[0]));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(pun.w[1]));
      } else {
        emit_load_into_reg(info.rhs, R2);
      }

      // Encoding: Branch(opcode, RegRvalue=R2, RegLvalue=R1) per
      // NdbInterpretedCode::branch_lt et al.  The branch label offset
      // sits in the high bits like the other BRANCH_* opcodes.
      Uint32 br_op = reg_branch_opcode(atom->op);
      programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
          Interpreter::Branch(br_op, /*Reg1=*/R2, /*Reg2=*/R1) |
          (branch_offset << 16)));
    }
    else
    {
      Uint32 cond = cond_for_arg_family(atom->op);
      const SideInfo& lhs = info.lhs;

      if (lhs.kind == SideKind::InlineLinked) {
        // CTE leaf column.  Stage column then compare with
        // BRANCH_MEM_OP_ARG_INLINE_TYPE.
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::READ_LINKED_TO_MEM | (lhs.linked_position << 16)));
        Uint32 csNumber = 0;
        if (lhs.col->getCharset() != NULL)
          csNumber = (Uint32)lhs.col->getCharsetNumber();
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE |
            (Interpreter::NULL_CMP_EQUAL << 6) | (cond << 12) |
            ((branch_offset - 0) << 16)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::BranchMem_2((Uint32)lhs.col->getType(),
                                     info.rhs.byte_len)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            (info.rhs.byte_len << 16) | csNumber));
      } else if (lhs.kind == SideKind::LinkedParent ||
                 lhs.kind == SideKind::LinkedCteCol) {
        // Stage the linked column into heap[0] then compare with
        // BRANCH_MEM_OP_ARG.
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::READ_LINKED_TO_MEM | (lhs.linked_position << 16)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::BranchMem(
                static_cast<Interpreter::BinaryCondition>(cond),
                Interpreter::NULL_CMP_EQUAL) |
            ((branch_offset - 0) << 16)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::BranchMem_2(lhs.attr_id, info.rhs.byte_len)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            lhs.parent_table->getTableId()));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            lhs.parent_table->getObjectVersion()));
      } else {
        // LeafTable: BRANCH_ATTR_OP_ARG (reads via readAttributes).
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::BranchCol(
                static_cast<Interpreter::BinaryCondition>(cond),
                Interpreter::NULL_CMP_EQUAL) |
            (branch_offset << 16)));
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
            Interpreter::BranchCol_2(lhs.attr_id, info.rhs.byte_len)));
      }

      // Emit constant data words (after the branch instruction).
      Uint32 padded_len = info.rhs.data_words * 4;
      Uint8* buf = m_amalloc->alloc_exc<Uint8>(padded_len);
      memcpy(buf, info.rhs.rv.val, info.rhs.rv.len);
      if (padded_len > info.rhs.rv.len)
        memset(buf + info.rhs.rv.len, 0, padded_len - info.rhs.rv.len);
      for (Uint32 w = 0; w < info.rhs.data_words; w++)
      {
        Uint32 word;
        memcpy(&word, buf + w * 4, 4);
        programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(word));
      }
    }

    pos += info.word_count;
  }

  // First exit (fall-through)
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::LoadConst16(2, first_exit_output)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::WriteInterpreterOutput(2, 0)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::ExitOK()));

  // Second exit (branched to)
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::LoadConst16(2, second_exit_output)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::WriteInterpreterOutput(2, 0)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::ExitOK()));
}
#undef programAggregator_do_or_fail

void
RonSQLPreparer::print()
{
  std::basic_ostream<char>& out = *m_conf.out_stream;

  if (m_context.ast_root.frags_per_worker > 0) {
    out << "FRAGS_PER_WORKER = " << m_context.ast_root.frags_per_worker
        << " (requested; the NDB API normalizes to a power of two <= 8 and"
           " may clamp or ignore it, see frags_per_worker_plan.md)\n\n";
  }

  // Print CTE definitions
  if (m_has_ctes) {
    out << "CTE definitions:\n";
    Uint32 cte_idx = 0;
    for (const CteDefinition* cte = m_context.ast_root.cte_list;
         cte != NULL; cte = cte->next, cte_idx++)
    {
      out << "  CTE[" << cte_idx << "] " << cte->name.c_str()
          << " — source: " << cte->stmt->table.c_str();
      if (cte->stmt->joins != NULL)
        out << " (with joins)";
      if (cte->stmt->is_single_row_cte)
        out << " [single-row key lookup body]";
      out << "\n    Outputs: ";
      Uint32 oc = 0;
      for (const Outputs *o = cte->stmt->outputs; o; o = o->next, oc++) {
        if (oc > 0) out << ", ";
        out << std::string(o->output_name.str, o->output_name.len);
      }
      out << "\n    GROUP BY: ";
      Uint32 gc = 0;
      for (const GroupbyColumns *g = cte->stmt->groupby_columns; g; g = g->next, gc++) {
        if (gc > 0) out << ", ";
        out << "col_" << g->col_idx;
      }
      out << '\n';
      if (cte_idx < m_cte_scopes.size()) {
        QueryScope* cte_scope = m_cte_scopes[cte_idx];
        if (cte_scope != NULL && cte_scope->join_plan.num_ops > 0) {
          const JoinOp& root_op = cte_scope->join_plan.ops[0];
          out << "    Body root: ";
          switch (root_op.type) {
          case JoinOp::TABLE_SCAN:     out << "TABLE_SCAN";    break;
          case JoinOp::INDEX_SCAN:     out << "INDEX_SCAN";    break;
          case JoinOp::PK_LOOKUP:      out << "PK_LOOKUP";     break;
          case JoinOp::UNIQUE_LOOKUP:  out << "UNIQUE_LOOKUP"; break;
          case JoinOp::CTE_LOOKUP:     out << "CTE_LOOKUP";    break;
          case JoinOp::CTE_SCAN:       out << "CTE_SCAN";      break;
          }
          if (root_op.index != NULL) {
            out << " using " << root_op.index->getName();
          }
          if (cte_scope->body_minmax_kind ==
              QueryScope::MinMaxKind::MIN_ASC) {
            out << " [I.10 MIN_ASC maxRows=1]";
          } else if (cte_scope->body_minmax_kind ==
                     QueryScope::MinMaxKind::MAX_DESC) {
            out << " [I.10 MAX_DESC maxRows=1]";
          }
          out << '\n';
        }
      }
    }
    out << '\n';
  }

  // Print join plan at the top for multi-table queries
  if (m_conf.ndb != NULL && m_scan_config == NULL && m_main_scope.join_plan.num_ops > 1) {
    const JoinPlan& jp = m_main_scope.join_plan;
    out << "Join plan (" << jp.num_ops << " operations):\n";
    for (Uint32 i = 0; i < jp.num_ops; i++) {
      const JoinOp& op = jp.ops[i];
      bool is_last = (i + 1 == jp.num_ops);
      out << (is_last ? "╰─ " : "├─ ") << i << ": ";
      if (op.is_root) {
        out << "[ROOT] ";
      } else {
        switch (op.match_type) {
        case JoinOp::INNER:      out << "[INNER] ";      break;
        case JoinOp::LEFT_OUTER: out << "[LEFT JOIN] ";   break;
        case JoinOp::SEMI_JOIN:  out << "[SEMI] ";        break;
        case JoinOp::ANTI_JOIN:  out << "[ANTI] ";        break;
        }
      }
      switch (op.type) {
      case JoinOp::TABLE_SCAN:     out << "TABLE_SCAN ";    break;
      case JoinOp::INDEX_SCAN:     out << "INDEX_SCAN ";    break;
      case JoinOp::PK_LOOKUP:      out << "PK_LOOKUP ";     break;
      case JoinOp::UNIQUE_LOOKUP:  out << "UNIQUE_LOOKUP "; break;
      case JoinOp::CTE_LOOKUP:     out << "CTE_LOOKUP ";    break;
      case JoinOp::CTE_SCAN:       out << "CTE_SCAN ";      break;
      }
      if (op.type == JoinOp::CTE_LOOKUP || op.type == JoinOp::CTE_SCAN) {
        out << "CTE:" << op.cte_def->name.c_str();
      } else {
        out << op.table->getName();
      }
      if (op.alias.len > 0) out << " AS " << op.alias.c_str();
      // Phase 3 (W2): show the topology — a star and a chain used to
      // print identically.  No recorded baseline contains this print
      // (it goes to ronsql_explain.inc's $EXPLAIN_FILE), so the
      // annotation is baseline-safe.
      if (!op.is_root) {
        out << "  <- " << jp.ops[op.parent_op_idx].alias.c_str();
      }
      out << '\n';
      const char *indent = is_last ? "   " : "│  ";
      if ((op.type == JoinOp::CTE_LOOKUP || op.type == JoinOp::CTE_SCAN) &&
          op.cte_def != NULL) {
        out << indent << "  CTE outputs: ";
        Uint32 oc = 0;
        for (const Outputs *o = op.cte_def->stmt->outputs; o; o = o->next) {
          if (oc > 0) out << ", ";
          out << std::string(o->output_name.str, o->output_name.len);
          oc++;
        }
        out << '\n';
      }
      if (op.index != NULL) {
        out << indent << "  Index: " << op.index->getName() << "(";
        for (Uint32 c = 0; c < op.index->getNoOfColumns(); c++) {
          if (c > 0) out << ", ";
          out << op.index->getColumn(c)->getName();
        }
        out << ")\n";
      }
      // Root scan-config conditions (join_root_index_scan_plan.md):
      // show bound-vs-filter routing for a scan-config-selected
      // index-scan root, in the same style as the single-table
      // CONDITIONS block.
      if (op.is_root && op.index != NULL &&
          m_main_scope.body_scan_config != NULL &&
          m_main_scope.body_scan_config->index == op.index) {
        const ScanConfig& sc = *m_main_scope.body_scan_config;
        Uint32 cond_cnt = m_main_scope.body_toplevel_conditions.size();
        Uint32 filter_cnt = 0;
        for (Uint32 ci = 0; ci < cond_cnt; ci++) {
          if (sc.condition_handling_map[ci] == -1) filter_cnt++;
        }
        Uint32 bound_cnt = cond_cnt - filter_cnt;
        out << indent << "  CONDITIONS (" << bound_cnt << " bound"
            << (bound_cnt == 1 ? "" : "s");
        if (filter_cnt > 0) {
          out << " and " << filter_cnt << " filter"
              << (filter_cnt == 1 ? "" : "s");
        }
        out << "):\n";
        LexString outer = LexString{indent, strlen(indent)}
                              .concat(LexString{"  ", 2}, m_amalloc);
        for (Uint32 ci = 0; ci < cond_cnt; ci++) {
          bool cond_last = (ci + 1 == cond_cnt);
          out << outer << (cond_last ? "╰─ " : "├─ ");
          int handling = sc.condition_handling_map[ci];
          Uint32 labellen;
          if (handling == -1) {
            out << "FILTER: ";
            labellen = 11;
          } else {
            out << "INDEX[" << handling << "]: ";
            labellen = 13;
            if (handling > 9) {
              labellen++;
            }
          }
          // Continuation prefix: op indent + tree continuation +
          // spaces aligning under the label (same widths as the
          // single-table CONDITIONS block).
          LexString cont = outer.concat(
              cond_last ? LexString{"              ", labellen}
                        : LexString{"│             ", labellen + 2},
              m_amalloc);
          print(m_main_scope.body_toplevel_conditions[ci], cont);
        }
      }
      if (!op.is_root && op.num_key_cols > 0) {
        out << indent << "  Key: ";
        for (Uint32 k = 0; k < op.num_key_cols; k++) {
          if (k > 0) out << ", ";
          // Per-key parent sources: print each key's own source alias
          // (byte-identical to the old parent_op_idx print for every
          // single-parent plan).
          out << op.child_key_col_names[k] << " = "
              << jp.ops[op.key_parent_op_idx[k]].alias.c_str()
              << "." << op.parent_key_col_names[k];
        }
        out << '\n';
      }
      if (op.num_low_bounds > 0 || op.num_high_bounds > 0) {
        // child_bounds: a const_cond bound prints its constant value;
        // parent-linked bounds print alias.column as before.
        auto print_bound_source = [&](const JoinOp::RangeBound& rb) {
          if (rb.const_cond == NULL) {
            out << jp.ops[rb.parent_op_idx].alias.c_str()
                << "." << rb.parent_col_name;
            return;
          }
          const ConditionalExpression* c = rb.const_cond->args.right;
          switch (c->op) {
          case T_INT:
            out << c->constant_integer;
            break;
          case T_FLOAT:
            out << c->constant_float.dbl;
            break;
          case T_STRING:
            out << '\'';
            out.write(c->string.str, c->string.len);
            out << '\'';
            break;
          default:
            out << "<const>";
            break;
          }
        };
        out << indent << "  Bounds:";
        for (Uint32 b = 0; b < op.num_low_bounds; b++) {
          out << " " << op.low_bounds[b].child_col_name
              << (op.low_bounds[b].inclusive ? " >= " : " > ");
          print_bound_source(op.low_bounds[b]);
        }
        for (Uint32 b = 0; b < op.num_high_bounds; b++) {
          out << " " << op.high_bounds[b].child_col_name
              << (op.high_bounds[b].inclusive ? " <= " : " < ");
          print_bound_source(op.high_bounds[b]);
        }
        out << '\n';
      }
      // child_bounds: make the residual visible next to the bounds so
      // an EXPLAIN grep can pin bound-vs-filter routing per op (real
      // tables only — CTE ops route WHERE to the jump-table filter).
      if (!op.is_root && op.table != NULL &&
          m_main_scope.join_where_ce[i] != NULL) {
        out << indent << "  Residual filter: yes\n";
      }
      if (jp.num_agg_leaves > 0) {
        for (Uint32 a = 0; a < jp.num_agg_leaves; a++) {
          if (jp.agg_leaf_indices[a] == i) {
            out << indent << "  ** Aggregation leaf **\n";
            break;
          }
        }
      } else if (jp.agg_leaf_idx == i && !op.is_root) {
        out << indent << "  ** Aggregation leaf **\n";
      }
    }
    out << '\n';
  }

  // Print query parse tree
  SelectStatement& ast_root = m_context.ast_root;
  out << "Query parse tree:\n"
      << "SELECT\n";
  Outputs* outputs = ast_root.outputs;
  Uint32 out_count = 0;
  while (outputs != NULL)
  {
    out << "  Out_" << out_count << ":"
        << quoted_identifier(outputs->output_name)
        << "\n   = ";
    switch (outputs->type)
    {
    case Outputs::Type::AGGREGATE:
      {
        Uint32 pr;
        switch (outputs->aggregate.fun)
        {
        case T_COUNT:
          pr = m_main_scope.agg->Count(outputs->aggregate.arg);
          break;
        case T_MAX:
          pr = m_main_scope.agg->Max(outputs->aggregate.arg);
          break;
        case T_MIN:
          pr = m_main_scope.agg->Min(outputs->aggregate.arg);
          break;
        case T_SUM:
          pr = m_main_scope.agg->Sum(outputs->aggregate.arg);
          break;
        default:
          // Unknown aggregate function
          abort();
        }
        out << "A" << pr << ":";
        m_main_scope.agg->print_aggregate(pr);
        out << '\n';
      }
      break;
    case Outputs::Type::AVG:
      {
        Uint32 pr;
        out << "CLIENT-SIDE CALCULATION: ";
        pr = m_main_scope.agg->Sum(outputs->avg.arg);
        out << "A" << pr << ":";
        m_main_scope.agg->print_aggregate(pr);
        out << " / ";
        pr = m_main_scope.agg->Count(outputs->avg.arg);
        out << "A" << pr << ":";
        m_main_scope.agg->print_aggregate(pr);
        out << '\n';
      }
      break;
    case Outputs::Type::COLUMN:
      {
        Uint32 col_idx = outputs->column.col_idx;
        out << "C" << col_idx << ":"
            << quoted_identifier(column_idx_to_name(col_idx)) << '\n';
      }
      break;
    default:
      abort();
    }
    out_count++;
    outputs = outputs->next;
  }
  out << "FROM " << ast_root.table.c_str() << '\n';
  struct ConditionalExpression* where = ast_root.where_expression;
  if (where != NULL)
  {
    out << "WHERE\n  ";
    print(where, LexString{"  ", 2});
  }
  struct GroupbyColumns* groupby = ast_root.groupby_columns;
  if (groupby != NULL)
  {
    out << "GROUP BY\n";
    while (groupby != NULL)
    {
      Uint32 col_idx = groupby->col_idx;
      out << "  C" << col_idx << ":"
          << quoted_identifier(column_idx_to_name(col_idx)) << '\n';
      groupby = groupby->next;
    }
  }
  struct OrderbyColumns* orderby = ast_root.orderby_columns;
  if (orderby != NULL)
  {
    out << "ORDER BY\n";
    while (orderby != NULL)
    {
      bool ascending = orderby->ascending;
      if (orderby->kind == OrderbyColumns::Kind::OUTPUT_REF)
      {
        // The union holds output_idx here, not col_idx: an alias target
        // resolved to a SELECT output.  Print it like the SELECT section
        // (Out_N:`alias`) instead of misreading it as a column index.
        Uint32 output_idx = orderby->output_idx;
        out << "  Out_" << output_idx;
        Outputs* ob_output = ast_root.outputs;
        for (Uint32 i = 0; ob_output != NULL && i < output_idx; i++)
          ob_output = ob_output->next;
        if (ob_output != NULL)
          out << ":" << quoted_identifier(ob_output->output_name);
        out << (ascending ? " ASC" : " DESC") << '\n';
      }
      else
      {
        Uint32 col_idx = orderby->col_idx;
        out << "  C" << col_idx << ":" <<
          quoted_identifier(column_idx_to_name(col_idx)) <<
          (ascending ? " ASC" : " DESC") << '\n';
      }
      orderby = orderby->next;
    }
  }
  if (ast_root.limit >= 0)
  {
    out << "LIMIT " << ast_root.limit << '\n';
  }

  out << '\n';

  // Print aggregation program
  if (m_main_scope.agg != NULL)
  {
    m_main_scope.agg->print_program();
  }
  else
  {
    out << "No aggregation program.\n";
  }

  out << '\n';

  // Print scan information
  if (m_conf.ndb == NULL) {
    out << "No NDB connection, so no index scan analysis.\n";
  } else if (m_pk_lookup) {
    // Phase 1 W5: unlike the join path's emit-time refinements, the
    // single-table access choice lives in planner state, so EXPLAIN
    // can tell the whole truth here.
    Uint32 cond_cnt = m_toplevel_conditions.size();
    if (!m_pk_lookup_has_residual) {
      out << "Execute as primary key lookup.\n"
          << "KEYS (" << cond_cnt << "):\n";
      for (Uint32 i = 0; i < cond_cnt; i++) {
        out << (i+1 == cond_cnt ? "╰─ " : "├─ ");
        print(m_toplevel_conditions[i],
              i + 1 == cond_cnt
              ? LexString{"   ", 3}
              : LexString{"│  ", 5});
      }
    } else {
      // PK+residual lookup: per-conjunct KEY[pk ordinal] / FILTER
      // labels in the index-scan CONDITIONS idiom, driven by
      // m_pk_lookup_cond_map.
      Uint32 filter_cnt = 0;
      for (Uint32 i = 0; i < cond_cnt; i++) {
        if (m_pk_lookup_cond_map[i] == -1) {
          filter_cnt++;
        }
      }
      Uint32 key_cnt = cond_cnt - filter_cnt;
      out << "Execute as primary key lookup.\n"
          << "CONDITIONS (" << key_cnt << " key"
          << (key_cnt == 1 ? "" : "s")
          << " and " << filter_cnt << " filter"
          << (filter_cnt == 1 ? "" : "s") << "):\n";
      for (Uint32 i = 0; i < cond_cnt; i++) {
        out << (i+1 == cond_cnt ? "╰─ " : "├─ ");
        int handling = m_pk_lookup_cond_map[i];
        Uint32 prefixlen;
        if (handling == -1) {
          out << "FILTER: ";
          prefixlen = 11;
        } else {
          out << "KEY[" << handling << "]: ";
          prefixlen = 11;
          if (handling > 9) {
            prefixlen++;
          }
        }
        print(m_toplevel_conditions[i],
              i + 1 == cond_cnt
              ? LexString{"              ", prefixlen}
              : LexString{"│             ", prefixlen + 2});
      }
    }
  } else if (m_scan_config == NULL) {
    // Join plan already printed at the top
  } else {
    ScanConfig& sc = *m_scan_config;
    if (sc.index == NULL) {
      out << "Execute as table scan.\n";
      Uint32 cond_cnt = m_toplevel_conditions.size();
      out << (cond_cnt ?"FILTERS:\n" : "No filters.\n");
      for (Uint32 i = 0; i < cond_cnt; i++) {
        out << (i+1 == cond_cnt ? "╰─ " : "├─ ");
        print(m_toplevel_conditions[i],
              i + 1 == cond_cnt
              ? LexString{"   ", 3}
              : LexString{"│  ", 5});
      }
    } else {
      out << "Execute as index scan.\n"
          << "Index: " << quoted_identifier(sc.index->getName()) << "(";
      for (Uint32 i = 0; i < sc.index->getNoOfColumns(); i++) {
        if (i > 0) out << ", ";
        out << quoted_identifier(sc.index->getColumn(i)->getName());
      }
      out << ")\nWith goodness " << sc.goodness << " it's the best of "
          << m_scan_config_candidates.size() << " options.\n";
      Uint32 cond_cnt = m_toplevel_conditions.size();
      Uint32 filter_cnt = 0;
      for (Uint32 i = 0; i < cond_cnt; i++) {
        if (sc.condition_handling_map[i] == -1) {
          filter_cnt++;
        }
      }
      Uint32 bound_cnt = cond_cnt - filter_cnt;
      // Phase 4b: an ORDER BY-driven candidate may carry no bounds at
      // all (full index scan in key order, conjuncts as filters).
      ndbrequire(bound_cnt > 0 || sc.index_order);
      if (bound_cnt == 0) {
        out << "Chosen for ORDER BY index order (no bounds).\n"
            << (cond_cnt ? "FILTERS:\n" : "No filters.\n");
        for (Uint32 i = 0; i < cond_cnt; i++) {
          out << (i+1 == cond_cnt ? "╰─ " : "├─ ");
          print(m_toplevel_conditions[i],
                i + 1 == cond_cnt
                ? LexString{"   ", 3}
                : LexString{"│  ", 5});
        }
      } else {
        out << "CONDITIONS (" << bound_cnt << " bound"
            << (bound_cnt == 1 ? "" : "s");
        if (filter_cnt > 0) {
          out << " and " << filter_cnt << " filter"
              << (filter_cnt == 1 ? "" : "s");
        }
        out << "):\n";
        for (Uint32 i = 0; i < cond_cnt; i++) {
          out << (i+1 == cond_cnt ? "╰─ " : "├─ ");
          int handling = sc.condition_handling_map[i];
          Uint32 prefixlen;
          if (handling == -1) {
            out << "FILTER: ";
            prefixlen = 11;
          } else {
            out << "INDEX[" << handling << "]: ";
            prefixlen = 13;
            if (handling > 9) {
              prefixlen++;
            }
          }
          print(m_toplevel_conditions[i],
                i + 1 == cond_cnt
                ? LexString{"              ", prefixlen}
                : LexString{"│             ", prefixlen + 2});
        }
      }
    }
  }
  // Phase 4b (ronsql_orderby_limit_plan.md): pass-through ORDER BY
  // strategy — index order (SF_OrderBy streaming) vs the Phase 3
  // buffered client-side sort.  Aggregate queries sort in the
  // ResultPrinter and report that below.
  if (!m_is_aggregate_query && ast_root.orderby_columns != NULL &&
      m_conf.ndb != NULL) {
    if (m_pk_lookup) {
      out << "ORDER BY: single-row primary key lookup, no sort needed.\n";
    } else if (m_scan_config != NULL && m_scan_config->index_order) {
      out << "ORDER BY: index order (SF_OrderBy"
          << (m_scan_config->index_order_desc ? " | SF_Descending" : "")
          << " merge of the fragment scans, streamed, no client-side "
             "sort).\n";
    } else {
      out << "ORDER BY: client-side sort (rows buffered, then sorted; "
             "LIMIT applied after the sort).\n";
    }
  }

  out << '\n';

  // Print post-processing information
  ndbrequire(m_resultprinter != NULL);
  ndbrequire(m_conf.out_stream != NULL);
  m_resultprinter->explain(m_conf.out_stream);

  out << '\n';
}

void
RonSQLPreparer::print(struct ConditionalExpression* ce,
                      LexString prefix)
{
  std::basic_ostream<char>& out = *m_conf.out_stream;
  const char* opstr = NULL;
  bool prefix_op = false;
  switch (ce->op)
  {
  case T_IDENTIFIER:
    out << quoted_identifier(column_idx_to_name(ce->col_idx)) << '\n';
    return;
  case T_STRING:
    {
      out << "STRING: ";
      for (Uint32 i = 0; i < ce->string.len; i++)
      {
        char c = ce->string.str[i];
        if ( 0x21 <= c && c <= 0x7E && c != '<' && c != '>')
        {
          out << c;
        }
        else
        {
          static const char* hex = "0123456789ABCDEF";
          out << "<" << hex[(c >> 4) & 0xF] << hex[c & 0xF] << ">";
        }
      }
      out << '\n';
      return;
    }
  case T_INT:
    out << ce->constant_integer << '\n';
    return;
  case T_FLOAT:
    {
      LexString ls = ce->constant_float.ls;
      if (ls.str == NULL) {
        // Subquery-substituted constant — no source text; print the value.
        out << ce->constant_float.dbl << '\n';
        return;
      }
      while (ls.len > 0 && ls.str[ls.len-1]=='0') ls.len--;
      if (ls.len > 0 && ls.str[ls.len-1]=='.') ls.len--;
      out << ls << '\n';
      return;
    }
  case T_OR:
    opstr = "OR";
    break;
  case T_XOR:
    opstr = "XOR";
    break;
  case T_AND:
    opstr = "AND";
    break;
  case T_NOT:
    opstr = "NOT";
    prefix_op = true;
    break;
  case T_EQUALS:
    opstr = "=";
    break;
  case T_GE:
    opstr = ">=";
    break;
  case T_GT:
    opstr = ">";
    break;
  case T_LE:
    opstr = "<=";
    break;
  case T_LT:
    opstr = "<";
    break;
  case T_NOT_EQUALS:
    opstr = "!=";
    break;
  case T_IS:
    {
      out << "IS\n"
          << prefix << "├─ ";
      LexString prefix_arg = prefix.concat(LexString{"│  ", 5}, m_amalloc);
      print(ce->is.arg, prefix_arg);
      out << prefix << "╰─ "
          << (ce->is.null ? "NULL\n" : "NOT NULL\n");
      return;
    }
  case T_BITWISE_OR:
    opstr = "BITWISE-OR (|)";
    break;
  case T_BITWISE_AND:
    opstr = "&";
    break;
  case T_BITSHIFT_LEFT:
    opstr = "<<";
    break;
  case T_BITSHIFT_RIGHT:
    opstr = ">>";
    break;
  case T_PLUS:
    opstr = "+";
    break;
  case T_MINUS:
    if (ce->args.left == NULL)
    {
      out << "NEGATION\n"
          << prefix << "╰─ ";
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_amalloc);
      print(ce->args.right, prefix_arg);
      return;
    }
    opstr = "-";
    break;
  case T_MULTIPLY:
    opstr = "*";
    break;
  case T_SLASH:
    opstr = "/";
    break;
  case T_DIV:
    opstr = "DIV";
    break;
  case T_MODULO:
    opstr = "%";
    break;
  case T_BITWISE_XOR:
    opstr = "^";
    break;
  case T_EXCLAMATION:
    opstr = "!";
    prefix_op = true;
    break;
  case T_INTERVAL:
    {
      out << "INTERVAL\n"
          << prefix << "├─ ";
      LexString prefix_arg = prefix.concat(LexString{"│  ", 5}, m_amalloc);
      print(ce->interval.arg, prefix_arg);
      out << prefix << "╰─ " <<
        interval_type_name(ce->interval.interval_type) << '\n';
      return;
    }
  case T_DATE_ADD:
    opstr = "DATE_ADD";
    break;
  case T_DATE_SUB:
    opstr = "DATE_SUB";
    break;
  case T_EXTRACT:
    {
    out << "EXTRACT\n"
        << prefix << "├─ "
        << interval_type_name(ce->extract.interval_type) << '\n'
        << prefix << "╰─ ";
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_amalloc);
      print(ce->extract.arg, prefix_arg);
      return;
    }
  case I_MYSQL_TIME:
    {
      char to[MAX_DATE_STRING_REP_LENGTH];
      my_TIME_to_str(ce->mysql_time, to, 6);
      int len = strlen(to);
      while (len > 0 && to[len-1]=='0') len--;
      if (len > 0 && to[len-1]=='.') len--;
      to[len]='\0';
      out << "DATETIME: " << to << '\n';
      return;
    }
  default:
    // Unknown operator
    abort();
  }
  if (prefix_op)
  {
    out << opstr << '\n'
        << prefix << "╰─ ";
    LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_amalloc);
    print(ce->args.left, prefix_arg);
  }
  else
  {
    out << opstr << '\n'
        << prefix << "├─ ";
    LexString prefix_left = prefix.concat(LexString{"│  ", 5}, m_amalloc);
    print(ce->args.left, prefix_left);
    out << prefix << "╰─ ";
    LexString prefix_right = prefix.concat(LexString{"   ", 3}, m_amalloc);
    print(ce->args.right, prefix_right);
  }
}

static const char* interval_type_name(TokenKind interval_type)
{
  switch (interval_type)
  {
  case T_MICROSECOND: return "MICROSECOND";
  case T_SECOND: return "SECOND";
  case T_MINUTE: return "MINUTE";
  case T_HOUR: return "HOUR";
  case T_DAY: return "DAY";
  case T_WEEK: return "WEEK";
  case T_MONTH: return "MONTH";
  case T_QUARTER: return "QUARTER";
  case T_YEAR: return "YEAR";
  case T_SECOND_MICROSECOND: return "SECOND_MICROSECOND";
  case T_MINUTE_MICROSECOND: return "MINUTE_MICROSECOND";
  case T_MINUTE_SECOND: return "MINUTE_SECOND";
  case T_HOUR_MICROSECOND: return "HOUR_MICROSECOND";
  case T_HOUR_SECOND: return "HOUR_SECOND";
  case T_HOUR_MINUTE: return "HOUR_MINUTE";
  case T_DAY_MICROSECOND: return "DAY_MICROSECOND";
  case T_DAY_SECOND: return "DAY_SECOND";
  case T_DAY_MINUTE: return "DAY_MINUTE";
  case T_DAY_HOUR: return "DAY_HOUR";
  case T_YEAR_MONTH: return "YEAR_MONTH";
  default: abort();
  }
}

Uint32
RonSQLPreparer::Context::column_name_to_idx(LexCString col_name)
{
  DynamicArray<LexCString>& columns = m_parser.m_columns;
  DynamicArray<LexCString>& qualifiers = m_parser.m_column_qualifiers;
  DynamicArray<bool>& is_inner = m_parser.m_col_is_inner;
  bool in_subquery = (m_subquery_depth > 0);
  Uint32 sz = columns.size();
  for (Uint32 i=0; i < sz; i++)
  {
    if (columns[i] == col_name && qualifiers[i].c_str() == NULL)
    {
      return i;
    }
  }
  columns.push(col_name);
  qualifiers.push(LexCString{NULL, 0});
  is_inner.push(in_subquery);
  return sz;
}

Uint32
RonSQLPreparer::Context::qualified_column_name_to_idx(
    LexCString table_qualifier, LexCString col_name)
{
  DynamicArray<LexCString>& columns = m_parser.m_columns;
  DynamicArray<LexCString>& qualifiers = m_parser.m_column_qualifiers;
  DynamicArray<bool>& is_inner = m_parser.m_col_is_inner;
  bool in_subquery = (m_subquery_depth > 0);
  Uint32 sz = columns.size();
  for (Uint32 i = 0; i < sz; i++)
  {
    if (columns[i] == col_name && qualifiers[i] == table_qualifier)
    {
      return i;
    }
  }
  columns.push(col_name);
  qualifiers.push(table_qualifier);
  is_inner.push(in_subquery);
  return sz;
}

LexCString
RonSQLPreparer::column_idx_to_name(Uint32 col_idx)
{
  ndbrequire(col_idx < m_columns.size());
  return m_columns[col_idx];
}

RonSQLPreparer::~RonSQLPreparer()
{
  rsqlp__delete_buffer(m_buf, m_scanner);
  rsqlp_lex_destroy(m_scanner);
}

void
RonSQLPreparer::Context::set_err_state(ErrState state,
                                       char* err_pos,
                                       size_t err_len)
{
  if (m_err_state == ErrState::NONE)
  {
    m_err_state = state;
    m_err_pos = err_pos;
    m_err_len = err_len;
  }
  else
  {
    /*
     * We want to save the error with the left-most position or, if two errors
     * have the same position, the shorter (more low-level) error. However,
     * above we actually save the error detected first. Presumably, that's the
     * same thing, but here we check it.
     */
    ndbrequire((m_err_pos < err_pos) ||
               (m_err_pos == err_pos && m_err_len <= err_len));
  }
}

AggregationAPICompiler*
RonSQLPreparer::Context::get_agg()
{
  if (m_subquery_depth > 0)
  {
    if (m_inner_agg == NULL)
    {
      RonSQLPreparer* _this = &m_parser;
      std::function<const char*(uint)> column_idx_to_name =
        [_this](Uint32 idx) -> const char*
        {
          return _this->column_idx_to_name(idx).c_str();
        };
      m_inner_agg = new (get_allocator()->alloc_exc<AggregationAPICompiler>(1))
        AggregationAPICompiler(column_idx_to_name,
                               *m_parser.m_conf.out_stream,
                               *m_parser.m_conf.err_stream,
                               m_parser.m_amalloc);
    }
    return m_inner_agg;
  }
  if (m_parser.m_main_scope.agg)
  {
    return m_parser.m_main_scope.agg;
  }
  RonSQLPreparer* _this = &m_parser;
  std::function<const char*(uint)> column_idx_to_name =
    [_this](Uint32 idx) -> const char*
    {
      return _this->column_idx_to_name(idx).c_str();
    };

  /*
   * The aggregator uses the same arena allocator as the RonSQLPreparer object
   * because they are both working in the prepare phase. After loading and
   * compilation, a new object will be crafted that holds the information
   * necessary for execution and post-processing.
   */
  m_parser.m_main_scope.agg = new (get_allocator()->alloc_exc<AggregationAPICompiler>(1))
    AggregationAPICompiler(column_idx_to_name,
                           *m_parser.m_conf.out_stream,
                           *m_parser.m_conf.err_stream,
                           m_parser.m_amalloc);
  return m_parser.m_main_scope.agg;
}

void
RonSQLPreparer::Context::enter_subquery()
{
  m_subquery_depth++;
  m_inner_agg = NULL;
}

AggregationAPICompiler*
RonSQLPreparer::Context::leave_subquery()
{
  ndbrequire(m_subquery_depth > 0);
  m_subquery_depth--;
  AggregationAPICompiler* out = m_inner_agg;
  m_inner_agg = NULL;
  return out;
}

ArenaMalloc*
RonSQLPreparer::Context::get_allocator()
{
  return m_parser.m_amalloc;
}

// Phase I.5 v2b — n-ary GREATEST / LEAST.
//
// Lowering at parse time: the operand list is folded left-associative
// into a chain of pair-ops on the SVM:
//   GREATEST(a, b)        =  Greatest2(a, b)
//   GREATEST(a, b, c)     =  Greatest2(Greatest2(a, b), c)
//   GREATEST(a, b, c, d)  =  Greatest2(Greatest2(Greatest2(a, b), c), d)
// Each `Greatest2` / `Least2` SVM instruction expands at programAggregator
// time to an embedded normal-interpreter comparison plus a conditional Mov
// on the kernel aggregation program.
//
// Scope:
// - Each operand must be a column ref (Load) or an integer constant
//   (LoadConstantInteger).  Arithmetic / nested expressions other
//   than another GREATEST / LEAST are not yet accepted at this
//   layer (the SVM compiler can chain pair-ops with arithmetic ops,
//   but the parser-level validation here is conservative).
// - At least one column operand is required across the whole list.
// - Two-or-more constants are accepted only as long as one operand
//   is a column; pure-constant GREATEST(1, 2, 3) is folded by the
//   AggregationAPICompiler's constant-folding path before reaching
//   the SVM.
// - Same-column degenerate `GREATEST(x, x, ..., x)` collapses to
//   `x` directly via the SVM's expression de-duplication
//   (Greatest2(x, x) returns the existing Load expression).
// - Nullable column operands rejected post-resolution via
//   `validate_greatest_least_pair_loads`.
ArithExprList*
RonSQLPreparer::Context::mk_arg_list(
    AggregationAPICompiler::Expr* a,
    AggregationAPICompiler::Expr* b)
{
  ArenaMalloc* amalloc = get_allocator();
  ArithExprList* tail = amalloc->alloc_exc<ArithExprList>(1);
  tail->head = b;
  tail->next = NULL;
  ArithExprList* list = amalloc->alloc_exc<ArithExprList>(1);
  list->head = a;
  list->next = tail;
  return list;
}

ArithExprList*
RonSQLPreparer::Context::append_arg_list(
    ArithExprList* list,
    AggregationAPICompiler::Expr* x)
{
  ArenaMalloc* amalloc = get_allocator();
  ArithExprList* node = amalloc->alloc_exc<ArithExprList>(1);
  node->head = x;
  node->next = NULL;
  ArithExprList* cur = list;
  while (cur->next != NULL) cur = cur->next;
  cur->next = node;
  return list;
}

AggregationAPICompiler::Expr*
RonSQLPreparer::Context::lower_greatest_least_nary(
    ArithExprList* args,
    bool is_greatest)
{
  require_prm(args != NULL && args->next != NULL,
              "GREATEST/LEAST requires at least two arguments.");

  // Pass 1: validate operand shape and collect column-Load operands
  // for the post-resolution nullable check.  Operands may be a
  // Load (column ref), LoadConstantInt, or another Greatest2 /
  // Least2 (so that user-written nested GREATEST / LEAST works
  // identically to a flat list); constants are always non-nullable
  // and pair-op operands are recursively walked to surface their
  // leaf Loads.
  bool any_col = false;
  std::function<void(AggregationAPICompiler::Expr*)> collect_loads =
      [&](AggregationAPICompiler::Expr* e) -> void
  {
    require_prm(e != NULL,
                "GREATEST/LEAST: NULL operand.");
    if (e->isLoad())
    {
      any_col = true;
      m_parser.m_greatest_least_pair_loads.push(e);
      return;
    }
    if (e->isLoadConstantInt())
    {
      return;
    }
    if (e->isGreatest2() || e->isLeast2())
    {
      collect_loads(e->getLeft());
      collect_loads(e->getRight());
      return;
    }
    require_prm(false,
                "GREATEST/LEAST operand must be a column reference, "
                "an integer constant, or a nested GREATEST / LEAST "
                "— arithmetic and other expression shapes are not "
                "yet supported.");
  };
  for (ArithExprList* it = args; it != NULL; it = it->next)
  {
    collect_loads(it->head);
  }
  require_prm(any_col,
              "GREATEST/LEAST requires at least one column operand "
              "— pure-constant forms can be folded by hand.");

  // Pass 2: left-associative fold.  Same-operand degenerate
  // `GREATEST(x, x, ...)` collapses to `x` directly here so the SVM
  // never sees a `Greatest2(x, x)` pair-op (which would emit a
  // wasteful embedded compare + Mov against the same register).
  AggregationAPICompiler* agg = get_agg();
  ArithExprList* it = args;
  AggregationAPICompiler::Expr* acc = it->head;
  it = it->next;
  while (it != NULL)
  {
    if (acc == it->head)
    {
      it = it->next;
      continue;
    }
    if (is_greatest)
      acc = agg->Greatest2(acc, it->head);
    else
      acc = agg->Least2(acc, it->head);
    it = it->next;
  }
  return acc;
}
