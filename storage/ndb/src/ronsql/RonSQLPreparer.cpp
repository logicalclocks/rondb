// vim: set fileencoding=utf-8 : -*- coding: utf-8 -*-

/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

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

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_RONSQLPREPARER 1
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
  m_indexes(conf.amalloc),
  m_toplevel_conditions(conf.amalloc),
  m_scan_config_candidates(conf.amalloc),
  m_select_subquery_leaves(conf.amalloc),
  m_merged_leaves(conf.amalloc),
  m_subquery_infos(conf.amalloc)
{
  ndbrequire(m_status == Status::BEGIN);
  try {
    configure();
    parse();
    analyze_subqueries();
    analyze_select_subqueries();
    load();
    if (m_context.ast_root.joins == NULL)
      plan_index_and_filter();
    compile();
    if (m_context.ast_root.joins != NULL)
      build_agg_linked_projections();
    determine_explain();
    m_status = Status::PREPARED;
  }
  catch (...) {
    m_status = Status::FAILED;
    handle_ronsql_exception(std::current_exception());
  }
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
static Int32
classify_ce_table(struct ConditionalExpression* ce, Uint32* col_table_idx)
{
  if (ce == NULL)
    return -1;
  switch (ce->op)
  {
  case T_IDENTIFIER:
    return (Int32)col_table_idx[ce->col_idx];
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
    Int32 left_t = classify_ce_table(ce->args.left, col_table_idx);
    Int32 right_t = classify_ce_table(ce->args.right, col_table_idx);
    if (left_t == -1) return right_t;
    if (right_t == -1) return left_t;
    if (left_t == right_t) return left_t;
    return -2;  // cross-table
  }
  case T_NOT:
  case T_EXCLAMATION:
    return classify_ce_table(ce->args.left, col_table_idx);
  case T_IS:
    return classify_ce_table(ce->is.arg, col_table_idx);
  case T_INTERVAL:
    return classify_ce_table(ce->interval.arg, col_table_idx);
  case T_EXTRACT:
    return classify_ce_table(ce->extract.arg, col_table_idx);
  case T_EXISTS:
  case I_IN_SUBQUERY:
  case I_SUBQUERY:
    return -1;  // Subqueries are treated as constants
  default:
    // Constants, strings, etc. — no column reference
    return -1;
  }
}

static const Uint32 MAX_WHERE_CONJUNCTS = 64;

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
  if (may_query)
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
     * AggregationAPICompiler. E.g. in `SELECT Max(col1 + col2)`, m_agg already
     * knows about `col1`, `col2` and `col1 + col2`. Here, we let m_agg know about
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
        ndbrequire(m_agg != NULL);
        TokenKind fun = outputs->aggregate.fun;
        AggregationAPICompiler::Expr* expr = outputs->aggregate.arg;
        switch (fun)
        {
        case T_COUNT:
          outputs->aggregate.agg_index = m_agg->Count(expr);
          break;
        case T_MAX:
          outputs->aggregate.agg_index = m_agg->Max(expr);
          break;
        case T_MIN:
          outputs->aggregate.agg_index = m_agg->Min(expr);
          break;
        case T_SUM:
          outputs->aggregate.agg_index = m_agg->Sum(expr);
          break;
        default:
          abort();
        }
        break;
      }
      case Outputs::Type::AVG:
        has_aggregate_outputs = true;
        outputs->avg.agg_index_sum = m_agg->Sum(outputs->avg.arg);
        outputs->avg.agg_index_count = m_agg->Count(outputs->avg.arg);
        break;
      case Outputs::Type::SUBQUERY_AGG:
        // Handled later in analyze_select_subqueries() and compile().
        // Don't set has_aggregate_outputs (avoids m_agg != NULL assert).
        has_subquery_agg_outputs = true;
        break;
      default:
        abort();
      }
      outputs = outputs->next;
    }
    bool has_having_aggregates = (m_agg != NULL &&
                                  m_context.ast_root.having_expression != NULL);
    if (m_agg == NULL)
    {
      ndbrequire(!has_aggregate_outputs);
    }
    else
    {
      ndbrequire(has_aggregate_outputs || has_having_aggregates);
      ndbrequire(m_agg->getStatus() == AggregationAPICompiler::Status::PROGRAMMING);
    }
    if (!has_aggregate_outputs && !has_having_aggregates &&
        !has_subquery_agg_outputs)
    {
      ndbrequire(m_conf.err_stream != NULL);
      std::basic_ostream<char>& err = *m_conf.err_stream;
      err << "This query has no aggregate expression, so it is not an aggregate query.\n"
             "Currently, RonSQL only supports aggregate queries.\n";
      throw RonSQLPermanentError("Not an aggregate query.");
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
  /*
   * During parsing, strings that were claimed to be column names were inserted
   * into m_columns. The element indexes in m_columns, usually called col_idx,
   * have already been used to construct Load instructions in m_agg, as well as
   * the parse tree in ast_root. Now that parsing is done and we know the table
   * name, we look up the column attrIds in the schema and check that the table
   * and columns exist. RonSQLPreparer keeps the col_idx around and relies on
   * m_column_attrId_map to map col_idx to column attrId, e.g. when programming
   * the aggregator. This also means we don't need to change anything in m_agg;
   * instead, RonSQLPreparer::programAggregator will read the program from m_agg
   * and map col_idx to attrId before speaking to NdbAggregator.
   */
  // Populate m_dict, m_table, m_column_attrId_map and m_column_map, on the
  // condition that m_conf.ndb is available. If m_conf.ndb is not available,
  // we'll still be able to do a (partial) EXPLAIN SELECT, so no need to fail
  // yet.
  Ndb* ndb = m_conf.ndb;
  if (ndb == NULL) return;

  // Populate m_dict
  m_dict = ndb->getDictionary();

  // Transform EXISTS subqueries into IN subqueries (may set m_has_subqueries)
  decorrelate_exists();

  // Transform correlated scalar subqueries (may set m_has_subqueries)
  decorrelate_scalar();

  bool is_join = (m_context.ast_root.joins != NULL);

  if (is_join) {
    load_join();
  } else {
    load_single_table();
  }
}

void
RonSQLPreparer::load_single_table()
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  // Populate m_table
  m_table = DBG(m_dict->getTable(DBG(m_context.ast_root.table.c_str())));
  require_prm(m_table != NULL,
              "Failed to get table. Note that RonSQL only supports tables"
              " with ENGINE=NDB.");
  // Populate m_indexes
  NdbDictionary::Dictionary::List index_list;
  ndbrequire(m_dict != NULL);
  ndbrequire(m_table != NULL);
  require_sch(m_dict->listIndexes(index_list, *m_table) == 0,
              "Failed to list indexes.");
  bool err_failed_to_get_index = false;
  bool err_object_status_not_retrieved = false;
  bool err_table_verid_mismatch = false;
  for(Uint32 i = 0; i < index_list.count; i++) {
    NdbDictionary::Dictionary::List::Element& list_element =
      index_list.elements[i];
    if (list_element.state != NdbDictionary::Object::StateOnline) {
      DEB_TRACE();
      continue;
    }
    if (list_element.type == NdbDictionary::Object::UniqueHashIndex) {
      DEB_TRACE();
      continue;
    }
    require_bug(list_element.type == NdbDictionary::Object::OrderedIndex,
                "Unexpected index type.");
    const NdbDictionary::Index* index = m_dict->getIndex(list_element.name,
                                                         *m_table);
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
    if(DBG(index->getTableId()) != DBG(m_table->getObjectId()) ||
       DBG(index->getTableVersion()) != DBG(m_table->getObjectVersion())) {
      DEB_TRACE();
      err_table_verid_mismatch = true;
    }
    m_indexes.push(index);
  }
  require_sch(DBG(m_table->getObjectStatus()) ==
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
  // Populate m_column_attrId_map and m_column_map
  NdbAttrId* col_id_map = m_amalloc->alloc_exc<NdbAttrId>(m_columns.size());
  const NdbDictionary::Column** col_map =
      m_amalloc->alloc_exc<const NdbDictionary::Column*>(m_columns.size());
  for (Uint32 col_idx = 0; col_idx < m_columns.size(); col_idx++) {
    if (m_col_is_inner.size() > col_idx && m_col_is_inner[col_idx]) {
      col_id_map[col_idx] = -1;
      col_map[col_idx] = NULL;
      continue;
    }
    const char* col_name = DBG(m_columns[DBG(col_idx)].c_str());
    const NdbDictionary::Column* col = m_table->getColumn(col_name);
    if (col == NULL) {
      err << "Failed to get column " << quoted_identifier(col_name) << "."
          << endl << "Note that column names are case sensitive." << endl;
      DEB_TRACE();
      throw RonSQLMaybeStaleSchema("Could not find column (column names are"
                                   " case sensitive).");
    }
    col_id_map[col_idx] = DBG(col->getAttrId());
    col_map[col_idx] = col;
  }
  m_column_attrId_map = col_id_map;
  m_column_map = col_map;
}

void
RonSQLPreparer::load_join()
{
  std::basic_ostream<char>& err = *m_conf.err_stream;

  // Build the join plan via QueryPlanner
  QueryPlanner::plan(
      m_context.ast_root.root_table,
      m_context.ast_root.joins,
      m_dict,
      err,
      m_join_plan);

  // For multi-leaf subquery pushdown, map merged leaves to plan op indices.
  // Each merged leaf corresponds to a JoinClause that was appended to the
  // join list, and QueryPlanner assigned it an operation index.
  if (m_has_select_subqueries) {
    // The injected joins were appended after any pre-existing joins.
    // With no pre-existing joins (outer query is single-table + subqueries),
    // the first injected join is op index 1, second is 2, etc.
    Uint32 first_injected_op = m_join_plan.num_ops - m_merged_leaves.size();
    for (Uint32 i = 0; i < m_merged_leaves.size(); i++) {
      m_merged_leaves[i].plan_op_idx = first_injected_op + i;
      m_join_plan.agg_leaf_indices[i] = first_injected_op + i;
    }
    m_join_plan.num_agg_leaves = m_merged_leaves.size();
  }

  // Set m_table to root table (used by existing code paths)
  m_table = m_join_plan.ops[0].table;

  // Resolve columns: match each column to its table
  Uint32 num_cols = m_columns.size();
  NdbAttrId* col_id_map = m_amalloc->alloc_exc<NdbAttrId>(num_cols);
  const NdbDictionary::Column** col_map =
      m_amalloc->alloc_exc<const NdbDictionary::Column*>(num_cols);
  Uint32* col_table_idx =
      m_amalloc->alloc_exc<Uint32>(num_cols);

  for (Uint32 col_idx = 0; col_idx < num_cols; col_idx++)
  {
    if (m_col_is_inner.size() > col_idx && m_col_is_inner[col_idx]) {
      col_id_map[col_idx] = -1;
      col_map[col_idx] = NULL;
      col_table_idx[col_idx] = 0;
      continue;
    }
    const char* col_name = m_columns[col_idx].c_str();
    const char* qualifier = m_column_qualifiers[col_idx].c_str();

    if (qualifier != NULL)
    {
      // Qualified column: find the table by alias
      bool found = false;
      for (Uint32 t = 0; t < m_join_plan.num_ops; t++)
      {
        if (strcmp(m_join_plan.ops[t].alias.c_str(), qualifier) == 0)
        {
          const NdbDictionary::Column* col =
              m_join_plan.ops[t].table->getColumn(col_name);
          if (col == NULL)
          {
            err << "Column '" << qualifier << "." << col_name
                << "' not found in table '"
                << m_join_plan.ops[t].table->getName() << "'." << endl;
            throw RonSQLMaybeStaleSchema("Column not found.");
          }
          col_id_map[col_idx] = col->getAttrId();
          col_map[col_idx] = col;
          col_table_idx[col_idx] = t;
          found = true;
          break;
        }
      }
      if (!found)
      {
        err << "Unknown table alias '" << qualifier << "' in column '"
            << qualifier << "." << col_name << "'." << endl;
        throw RonSQLPermanentError("Unknown table alias.");
      }
    }
    else
    {
      // Unqualified column: search all tables
      Uint32 match_count = 0;
      Uint32 match_table = 0;
      const NdbDictionary::Column* match_col = NULL;
      for (Uint32 t = 0; t < m_join_plan.num_ops; t++)
      {
        const NdbDictionary::Column* col =
            m_join_plan.ops[t].table->getColumn(col_name);
        if (col != NULL)
        {
          match_count++;
          match_table = t;
          match_col = col;
        }
      }
      if (match_count == 0)
      {
        err << "Column '" << col_name << "' not found in any joined table."
            << endl;
        throw RonSQLMaybeStaleSchema("Column not found.");
      }
      if (match_count > 1)
      {
        err << "Ambiguous column '" << col_name
            << "' found in multiple tables. Use 'table.column' syntax."
            << endl;
        throw RonSQLPermanentError("Ambiguous column.");
      }
      col_id_map[col_idx] = match_col->getAttrId();
      col_map[col_idx] = match_col;
      col_table_idx[col_idx] = match_table;
    }
  }

  m_column_attrId_map = col_id_map;
  m_column_map = col_map;
  m_column_table_idx = col_table_idx;

  // Classify WHERE conditions by table for per-table filter pushdown
  classify_where_by_table();

  // Apply inner subquery filters to the corresponding leaf operations
  if (m_has_select_subqueries) {
    for (Uint32 i = 0; i < m_merged_leaves.size(); i++) {
      MergedLeaf &ml = m_merged_leaves[i];
      SelectSubqueryLeaf &base = m_select_subquery_leaves[ml.first_subquery_idx];
      if (base.inner_filter != NULL) {
        Uint32 op_idx = ml.plan_op_idx;
        if (m_join_where_ce[op_idx] == NULL) {
          m_join_where_ce[op_idx] = base.inner_filter;
        } else {
          ConditionalExpression* combined =
              m_amalloc->alloc_exc<ConditionalExpression>(1);
          combined->op = T_AND;
          combined->args.left = m_join_where_ce[op_idx];
          combined->args.right = base.inner_filter;
          m_join_where_ce[op_idx] = combined;
        }
      }
    }
  }

  // Build linked projections for GROUP BY columns on non-leaf tables
  Uint32 leaf_idx = m_join_plan.agg_leaf_idx;
  struct GroupbyColumns* groupby = m_context.ast_root.groupby_columns;
  while (groupby != NULL)
  {
    Uint32 col_idx = groupby->col_idx;
    if (col_table_idx[col_idx] != leaf_idx)
    {
      require_prm(m_join_plan.num_linked_projs < MAX_LINKED_PROJS,
                  "Too many linked projections.");
      JoinPlan::LinkedProj& lp =
          m_join_plan.linked_projs[m_join_plan.num_linked_projs];
      lp.source_op_idx = col_table_idx[col_idx];
      lp.column_name = m_columns[col_idx].c_str();
      m_join_plan.num_linked_projs++;
    }
    groupby = groupby->next;
  }
}

void
RonSQLPreparer::classify_where_by_table()
{
  for (Uint32 t = 0; t < MAX_SPJ_TREE_NODES; t++)
    m_join_where_ce[t] = NULL;

  ConditionalExpression* where_ce = m_context.ast_root.where_expression;
  if (where_ce == NULL) return;

  // Flatten top-level AND conjuncts
  ConditionalExpression* conjuncts[MAX_WHERE_CONJUNCTS];
  Uint32 num_conjuncts = 0;
  flatten_and_conjuncts(where_ce, conjuncts, &num_conjuncts);

  // Classify each conjunct by table
  for (Uint32 i = 0; i < num_conjuncts; i++)
  {
    Int32 table_idx = classify_ce_table(conjuncts[i], m_column_table_idx);

    if (table_idx == -2)
    {
      throw RonSQLPermanentError(
          "WHERE condition references columns from multiple tables in a "
          "single comparison or OR branch. Split into separate AND "
          "conditions, each referencing only one table.");
    }

    // Constant-only conditions: assign to root table
    if (table_idx == -1)
      table_idx = 0;

    // Accumulate conditions for this table
    if (m_join_where_ce[table_idx] == NULL)
    {
      m_join_where_ce[table_idx] = conjuncts[i];
    }
    else
    {
      // Build AND(existing, new_conjunct)
      ConditionalExpression* combined =
          m_amalloc->alloc_exc<ConditionalExpression>(1);
      combined->op = T_AND;
      combined->args.left = m_join_where_ce[table_idx];
      combined->args.right = conjuncts[i];
      m_join_where_ce[table_idx] = combined;
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
  if (m_has_select_subqueries && m_join_plan.num_agg_leaves > 0) {
    // Multi-leaf: build linked projections for GROUP BY columns from root.
    // All leaves share the same GROUP BY columns via linked projection.
    struct GroupbyColumns* groupby = m_context.ast_root.groupby_columns;
    while (groupby != NULL) {
      Uint32 col_idx = groupby->col_idx;
      // GROUP BY column is from the root — needs linked projection to leaves
      bool is_on_any_leaf = false;
      for (Uint32 ml = 0; ml < m_merged_leaves.size(); ml++) {
        if (m_column_table_idx[col_idx] == m_merged_leaves[ml].plan_op_idx) {
          is_on_any_leaf = true;
          break;
        }
      }
      if (!is_on_any_leaf) {
        find_or_add_linked_proj(m_join_plan,
                                m_column_table_idx[col_idx],
                                m_columns[col_idx].c_str());
      }
      groupby = groupby->next;
    }
    return;
  }

  if (m_agg == NULL)
    return;
  Uint32 leaf_idx = m_join_plan.agg_leaf_idx;
  DynamicArray<AggregationAPICompiler::Instr>& program = m_agg->m_program;
  for (Uint32 i = 0; i < program.size(); i++)
  {
    if (program[i].type == AggregationAPICompiler::SVMInstrType::Load)
    {
      Uint32 col_idx = program[i].src;
      if (m_column_table_idx[col_idx] != leaf_idx)
      {
        find_or_add_linked_proj(m_join_plan,
                                m_column_table_idx[col_idx],
                                m_columns[col_idx].c_str());
      }
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
  // Add scan config candidates, including both index scans and table scan. This
  // will guarantee that we get at least one candidate.
  generate_scan_config_candidates();
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
RonSQLPreparer::generate_scan_config_candidates()
{
  {
    // Add a scan config candidate that represents table scan
    int *condition_handling_map =
      m_amalloc->alloc_exc<int>(m_toplevel_conditions.size());
    for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
      condition_handling_map[i] = -1;
    }
    m_scan_config_candidates.push(ScanConfig { NULL,
                                               condition_handling_map,
                                               0 });
  }
  if (m_toplevel_conditions.size() == 0) {
    // No WHERE clause
    return;
  }
  for(Uint32 i = 0; i < m_indexes.size(); i++) {
    const NdbDictionary::Index* index = m_indexes[i];
    int *condition_handling_map =
      m_amalloc->alloc_exc<int>(m_toplevel_conditions.size());
    for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
      condition_handling_map[i] = -1;
    }
    int goodness = 0;
    unsigned col_count = index->getNoOfColumns();
    require_bug(col_count > 0, "Index appears to have no columns.");
    bool later_columns_blocked = false;
    for(unsigned col_idx = 0;
        col_idx < col_count && !later_columns_blocked;
        col_idx++) {
      const NdbDictionary::Column* column = index->getColumn(col_idx);
      require_bug(column != NULL, "Index column object is NULL.");
      // todo Can getAttrId be used to match against the parent table?
      const char* column_name = column->getName();
      bool lbound_set = false, ubound_set = false;
      for(Uint32 cond_idx = 0;
          cond_idx < m_toplevel_conditions.size() &&
            !(lbound_set && ubound_set);
          cond_idx++) {
        ConditionalExpression* ce = m_toplevel_conditions[cond_idx];
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
        ndbrequire(condition_identifier->op == T_IDENTIFIER);
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
          condition_handling_map[cond_idx] = col_idx;
        } else {
          later_columns_blocked = true;
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
      m_scan_config_candidates.push(ScanConfig { index,
                                                 condition_handling_map,
                                                 goodness });
    }
  }
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
    SubqueryInfo info;
    info.ce_node = ce;
    info.inner_stmt = ce->subquery.stmt;
    m_subquery_infos.push(info);
    m_has_subqueries = true;
    return;
  }
  case T_EXISTS:
    // Transformed into I_IN_SUBQUERY in decorrelate_exists() during load().
    // SubqueryInfo is registered there, not here.
    return;
  case T_NOT:
  case T_EXCLAMATION:
    // NOT EXISTS handled in decorrelate_exists() — T_NOT wraps I_IN_SUBQUERY.
    analyze_subqueries_ce(ce->args.left);
    return;
  case I_IN_SUBQUERY:
  {
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
    if (lhs_is_inner) {
      inner_col = m_columns[lhs_col_idx];
      outer_col = m_columns[rhs_col_idx];
      outer_table = rhs_qualifier;
    } else {
      inner_col = m_columns[rhs_col_idx];
      outer_col = m_columns[lhs_col_idx];
      outer_table = lhs_qualifier;
    }

    // Extract the inner aggregate column's col_idx from the expression tree.
    // For simple SUM(col), the arg is a Load expression with idx = col_idx.
    AggregationAPICompiler_Expr *agg_arg = inner_out->aggregate.arg;
    require_prm(agg_arg != NULL && agg_arg->isLoad(),
                "SELECT-list subquery aggregate argument must be a simple "
                "column reference.");
    Uint32 inner_agg_col_idx = agg_arg->getLoadIdx();

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
    leaf.inner_agg_col = m_columns[inner_agg_col_idx];
    leaf.inner_agg_col_idx = inner_agg_col_idx;
    leaf.combined_agg_slot = 0;
    leaf.merged_leaf_idx = 0;
    leaf.use_inner_join = false;  // LEFT OUTER: unmatched entities get NULL
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

  // Compile aggregation program if applicable
  if (m_agg != NULL) {
    if (m_agg->compile()) {
      ndbrequire(m_agg->getStatus() == AggregationAPICompiler::Status::COMPILED);
    } else {
      ndbrequire(m_agg->getStatus() == AggregationAPICompiler::Status::FAILED);
      throw RonSQLPermanentError("Failed to compile aggregation program.");
    }
  }
  // Compile post-processing/printer program
  m_resultprinter = new (m_amalloc->alloc_exc<ResultPrinter>(1))
    ResultPrinter(m_amalloc,
                  &m_context.ast_root,
                  &m_columns,
                  m_column_map,
                  m_conf.output_format,
                  m_conf.err_stream);
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
      execute_subqueries();
      substitute_subquery_results();
    }

    if (m_context.ast_root.joins != NULL) {
      execute_join();
      cleanup_trans();
      return;
    }

    ndbrequire(m_trans == NULL);
    m_trans = DBG(ndb->startTransaction());
    require_run(m_trans != NULL, "Failed to start transaction.");
    // Since ndb exists, m_table should have been initialized in load()
    ndbrequire(m_table != NULL);
    NdbAggregator aggregator(m_table);
    DBGV(programAggregator(&aggregator));
    require_prm(aggregator.Finalize(), "Failed to finalize aggregator.");
    ScanConfig& sc = *m_scan_config;
    const NdbDictionary::Index* index = sc.index;
    bool has_filter = false;
    for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
      if (sc.condition_handling_map[i] == -1) {
        has_filter = true;
      }
    }
    // End of general preparation

    if(index == NULL) {
      // Prepare and execute full table scan
      DEB_TRACE();
      NdbScanOperation* myScanOp = DBG(m_trans->getNdbScanOperation(DBG(m_table)));
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
      require_run(DBG(myScanOp->setAggregationCode(&aggregator)) >= 0,
                  "Failed to set aggregation code.");
      DEB_TRACE();
      require_run(DBG(myScanOp->DoAggregation()) >= 0,
                  "Failed to execute table scan aggregation.");
      DEB_TRACE();
    } else {
      DEB_TRACE();
      // Prepare and execute index scan
      require_sch(DBG(index->getTableId()) == DBG(m_table->getObjectId()) &&
                  DBG(index->getTableVersion()) ==
                  DBG(m_table->getObjectVersion()),
                  "Table id/version mismatch");
      NdbIndexScanOperation *myIndexScanOp =
        DBG(m_trans->getNdbIndexScanOperation(DBG(index)));
      require_run(myIndexScanOp != NULL,
                  "Failed getting index scan operation.");
      Uint32 scanFlags = 0;
      // todo Decide whether NdbScanOperation::SF_OrderBy is good for performance
      require_run(DBG(myIndexScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead,
                                                DBG(scanFlags))) == 0,
                  "Failed to initialize index scan operation.");
      for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++)
      {
        int index_col_idx = DBG(sc.condition_handling_map[i]);
        if (index_col_idx == -1) {
          // This condition could not be configured for the index scan. It will
          // be applied as part of the filter instead.
          continue;
        }
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
        raw_value rv = encode_constant(condition_constant,
                                       m_column_map[condition_col_idx]);
        require_run(DBG(myIndexScanOp->setBound(DBG(colName),
                                                DBG(bt),
                                                DBG(rv).val)) == 0,
                    "Failed to set bound for index scan.");
        DEB_TRACE();
      }
      DEB_TRACE();
      // todo Is this necessary after removing the multirange flag?
      require_run(DBG(myIndexScanOp->end_of_bound(0)) == 0,
                  "Failed to set end of bound.");
      if (has_filter)
      {
        DEB_TRACE();
        NdbScanFilter filter(myIndexScanOp);
        DBGV(filter.setSqlCmpSemantics());
        apply_filter_top_level(&filter);
      }
      DEB_TRACE();
      require_run(DBG(myIndexScanOp->setAggregationCode(&aggregator)) >= 0,
                  "Failed to set aggregation code.");
      require_run(DBG(myIndexScanOp->DoAggregation()) >= 0,
                  "Failed to execute index scan aggregation.");
    }
    DEB_TRACE();

    // Print results
    m_resultprinter->print_result(&aggregator, m_conf.out_stream);
    DEB_TRACE();

    cleanup_trans();
  }
  catch (...) {
    handle_ronsql_exception(std::current_exception());
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
RonSQLPreparer::collect_pk_equalities(
    struct ConditionalExpression* ce,
    const NdbDictionary::Table* table,
    struct ConditionalExpression* pk_const[])
{
  if (ce == NULL) return;
  if (ce->op == T_AND)
  {
    collect_pk_equalities(ce->args.left, table, pk_const);
    collect_pk_equalities(ce->args.right, table, pk_const);
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
      break;
    }
  }
}

void
RonSQLPreparer::execute_join()
{
  Ndb* ndb = m_conf.ndb;
  ndbrequire(m_trans == NULL);
  m_trans = ndb->startTransaction();
  require_run(m_trans != NULL, "Failed to start transaction.");

  JoinPlan& plan = m_join_plan;

  // Build aggregator(s).
  // Multi-leaf: one NdbAggregator per merged leaf, each with its own program.
  // Single-leaf: one NdbAggregator on plan.agg_leaf_idx (existing path).
  NdbAggregator* leafAggs[MAX_SPJ_TREE_NODES] = {};
  NdbAggregator singleAgg(plan.ops[plan.agg_leaf_idx].table);

  if (m_has_select_subqueries && plan.num_agg_leaves > 0) {
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
        if (m_column_table_idx[col_idx] == op_idx) {
          require_prm(agg->GroupBy(m_column_attrId_map[col_idx]),
                      "Failed to program GroupBy on leaf.");
        } else {
          require_prm(agg->GroupByLinked(linked_proj_pos,
                                         m_column_map[col_idx]),
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
        NdbAttrId attr_id = m_column_attrId_map[col_idx];
        Uint32 reg = 0;  // use register 0 for loads
        Uint32 agg_slot = leaf_local_slot++;

        // If cross-table filter exists, build conditional aggregation:
        //   LoadLinkedColumn(linked_pos, reg_outer, outer_col)
        //   LoadColumn(inner_attr_id, reg_inner)
        //   BranchRegXx(reg_inner, reg_outer, 2) — skip Load+Agg
        //   LoadColumn(agg_col, reg)
        //   Sum/Count/etc(slot, reg)
        //
        // Without cross-table filter, just: LoadColumn + Agg
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

          // Determine which side is inner, which is outer
          Uint32 left_cidx = cf->args.left->col_idx;
          Uint32 right_cidx = cf->args.right->col_idx;
          LexCString itbl = sl.inner_table_alias;
          bool left_is_inner =
              (m_column_qualifiers[left_cidx].str != NULL &&
               m_column_qualifiers[left_cidx].len == itbl.len &&
               strncmp(m_column_qualifiers[left_cidx].str, itbl.str,
                       itbl.len) == 0);

          Uint32 inner_cidx = left_is_inner ? left_cidx : right_cidx;
          Uint32 outer_cidx = left_is_inner ? right_cidx : left_cidx;

          // Add linked projection for the outer column
          Uint32 outer_lp_pos = find_or_add_linked_proj(
              m_join_plan, m_column_table_idx[outer_cidx],
              m_columns[outer_cidx].c_str());

          // Load outer column into register 2 via linked projection
          Uint32 reg_outer = 2;
          require_prm(agg->LoadLinkedColumn(outer_lp_pos, reg_outer,
                                            m_column_map[outer_cidx]),
                      "Failed to load linked outer column for cross-table filter.");

          // Load inner filter column into register 1
          Uint32 reg_inner = 1;
          NdbAttrId inner_filter_attr = m_column_attrId_map[inner_cidx];
          require_prm(agg->LoadColumn(inner_filter_attr, reg_inner),
                      "Failed to load inner column for cross-table filter.");

          // Determine branch instruction: we want to SKIP aggregation
          // when the filter does NOT match.
          // Original filter: inner_col OP outer_col (when left_is_inner)
          // We skip when the NEGATION is true.
          // e.g., filter is "l.qty > o.min_qty" → skip when l.qty <= o.min_qty
          //        → BranchRegLe(reg_inner, reg_outer, skip_2)
          TokenKind filter_op = cf->op;
          if (!left_is_inner) {
            // Flip: if right is inner, swap operand order
            // "o.min_qty < l.qty" is same as "l.qty > o.min_qty"
            switch (filter_op) {
            case T_LT: filter_op = T_GT; break;
            case T_LE: filter_op = T_GE; break;
            case T_GT: filter_op = T_LT; break;
            case T_GE: filter_op = T_LE; break;
            default: break;
            }
          }
          // Now filter_op is from inner's perspective: inner_col OP outer_col
          // We skip when NOT(inner_col OP outer_col):
          //   NOT(a > b) = a <= b → BranchRegLe
          //   NOT(a >= b) = a < b → BranchRegLt
          //   NOT(a < b) = a >= b → BranchRegGe
          //   NOT(a <= b) = a > b → BranchRegGt
          //   NOT(a = b) = a != b → BranchRegNe
          //   NOT(a != b) = a = b → BranchRegEq
          Uint32 skip_count = 2;  // skip LoadColumn + Agg instruction
          switch (filter_op) {
          case T_GT:
            require_prm(agg->BranchRegLe(reg_inner, reg_outer, skip_count),
                        "Failed to program BranchRegLe.");
            break;
          case T_GE:
            require_prm(agg->BranchRegLt(reg_inner, reg_outer, skip_count),
                        "Failed to program BranchRegLt.");
            break;
          case T_LT:
            require_prm(agg->BranchRegGe(reg_inner, reg_outer, skip_count),
                        "Failed to program BranchRegGe.");
            break;
          case T_LE:
            require_prm(agg->BranchRegGt(reg_inner, reg_outer, skip_count),
                        "Failed to program BranchRegGt.");
            break;
          case T_EQUALS:
            require_prm(agg->BranchRegNe(reg_inner, reg_outer, skip_count),
                        "Failed to program BranchRegNe.");
            break;
          case T_NOT_EQUALS:
            require_prm(agg->BranchRegEq(reg_inner, reg_outer, skip_count),
                        "Failed to program BranchRegEq.");
            break;
          default:
            require_prm(false,
                "Unsupported cross-table filter operator.");
          }
        }

        require_prm(agg->LoadColumn(attr_id, reg),
                    "Failed to load column for subquery aggregate.");

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

      require_prm(agg->Finalize(), "Failed to finalize leaf aggregator.");
      leafAggs[op_idx] = agg;
    }
  } else {
    // Single-leaf path (existing)
    programAggregator_join(&singleAgg);
    require_prm(singleAgg.Finalize(), "Failed to finalize aggregator.");
  }

  // Build NdbQueryBuilder tree
  NdbQueryBuilder* qb = NdbQueryBuilder::create();
  ndbrequire(qb != NULL);

  const NdbQueryOperationDef* opDefs[MAX_SPJ_TREE_NODES];

  // Root operation: PK lookup if WHERE fully covers PK, else TABLE_SCAN.
  // When children have scan ops, NDB doesn't support lookup root, so use
  // an ordered index scan on PK with equality bounds instead.
  {
    NdbQueryOptions rootOpts;
    const NdbDictionary::Table* root_table = plan.ops[0].table;
    ConditionalExpression* where_ce = NULL;
    bool pk_covered = false;
    bool has_scan_child = false;
    int nkeys = 0;
    ConditionalExpression* pk_const[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];

    if (m_join_where_ce[0] != NULL)
    {
      where_ce = simplify_ce(m_join_where_ce[0], -1);

      // Check if WHERE fully covers the root PK with equality constants
      nkeys = root_table->getNoOfPrimaryKeys();
      for (int k = 0; k < nkeys; k++)
        pk_const[k] = NULL;
      collect_pk_equalities(where_ce, root_table, pk_const);
      pk_covered = true;
      for (int k = 0; k < nkeys; k++)
      {
        if (pk_const[k] == NULL) { pk_covered = false; break; }
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

    if (pk_covered && !has_scan_child)
    {
      // All children are lookups — use readTuple PK lookup for root
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
      opDefs[0] = qb->readTuple(root_table, pk_keys, &rootOpts);
      require_run(opDefs[0] != NULL, "Failed to create root PK lookup.");
    }
    else if (pk_covered && has_scan_child)
    {
      // Children have scans — NDB doesn't support lookup root with scan
      // children. Use ordered index scan on PK with equality bounds.
      const char* pk_col_names[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
      for (int k = 0; k < nkeys; k++)
        pk_col_names[k] = root_table->getPrimaryKey(k);
      const NdbDictionary::Index* pk_ordered_idx =
          QueryPlanner::findOrderedIndex(
              m_dict, root_table, pk_col_names, (Uint32)nkeys);
      if (pk_ordered_idx != NULL)
      {
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
        NdbQueryIndexBound bound(pk_keys);
        opDefs[0] = qb->scanIndex(pk_ordered_idx, root_table,
                                  &bound, &rootOpts);
        require_run(opDefs[0] != NULL,
                    "Failed to create root PK index scan.");
      }
      else
      {
        // No ordered index on PK — fall back to table scan with filter
        pk_covered = false;
      }
    }

    if (!pk_covered)
    {
      NdbInterpretedCode code(root_table);
      if (where_ce != NULL)
      {
        NdbScanFilter filter(&code);
        filter.setSqlCmpSemantics();
        filter.begin(NdbScanFilter::AND);
        apply_filter(&filter, where_ce);
        filter.end();
        code.finalise();
        rootOpts.setInterpretedCode(code);
      }
      opDefs[0] = qb->scanTable(root_table, &rootOpts);
      require_run(opDefs[0] != NULL, "Failed to create root scan.");
    }
  }

  // Child operations
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

    // Build linked key from parent
    const NdbQueryOperand* keys[MAX_JOIN_KEY_COLS + 1];
    for (Uint32 k = 0; k < op.num_key_cols; k++) {
      keys[k] = qb->linkedValue(opDefs[op.parent_op_idx],
                                 op.parent_key_col_names[k]);
      require_run(keys[k] != NULL, "Failed to create linked value.");
    }
    keys[op.num_key_cols] = nullptr;

    // Attach WHERE filter for this child table (if any)
    NdbInterpretedCode child_code(op.table);
    if (m_join_where_ce[i] != NULL)
    {
      ConditionalExpression* child_ce = simplify_ce(m_join_where_ce[i], -1);
      NdbScanFilter filter(&child_code);
      filter.setSqlCmpSemantics();
      filter.begin(NdbScanFilter::AND);
      apply_filter(&filter, child_ce);
      filter.end();
      child_code.finalise();
      opts.setInterpretedCode(child_code);
    }

    // Attach aggregation to leaf(s)
    if (m_has_select_subqueries && plan.num_agg_leaves > 0 &&
        leafAggs[i] != NULL) {
      // Multi-leaf: attach this leaf's aggregator
      require_run(opts.setAggregation(*leafAggs[i]) == 0,
                  "Failed to set aggregation on leaf.");
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
        NdbLinkedOperand* lv = qb->linkedValue(
            opDefs[plan.linked_projs[j].source_op_idx],
            plan.linked_projs[j].column_name);
        require_run(lv != NULL, "Failed to create linked projection.");
        require_run(opts.addLinkedProjection(lv) == 0,
                    "Failed to add linked projection.");
      }
    } else if (i == plan.agg_leaf_idx && plan.num_agg_leaves == 0) {
      // Single-leaf: existing path
      require_run(opts.setAggregation(singleAgg) == 0,
                  "Failed to set aggregation.");
      for (Uint32 j = 0; j < plan.num_linked_projs; j++) {
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
      NdbQueryIndexBound bound(keys);
      opDefs[i] = qb->scanIndex(op.index, op.table, &bound, &opts);
      break;
    }
    default:
      abort();
    }
    require_run(opDefs[i] != NULL, "Failed to create child operation.");
  }

  // Prepare and execute
  const NdbQueryDef* queryDef = qb->prepare(ndb);
  require_run(queryDef != NULL, "Failed to prepare query.");

  NdbQuery* query = m_trans->createQuery(queryDef);
  require_run(query != NULL, "Failed to create query.");

  require_run(m_trans->execute(NdbTransaction::NoCommit) == 0,
              "Failed to execute transaction.");

  // Consume all rows
  NdbQuery::NextResultOutcome rc;
  while ((rc = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (rc == NdbQuery::NextResult_error)
  {
    const NdbError& err = query->getNdbError();
    std::basic_ostream<char>& errout = *m_conf.err_stream;
    errout << "Join query failed: " << err.message
           << " (code " << err.code << ")" << std::endl;
    query->close();
    queryDef->destroy();
    qb->destroy();
    throw RonSQLRetryableError("Join query execution failed.");
  }

  // Collect and print aggregation results
  NdbAggregator* resultAgg = query->getAggregator();
  ndbrequire(resultAgg != NULL);
  m_resultprinter->print_result(resultAgg, m_conf.out_stream);

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
  ndbrequire(m_table != NULL);
  // Save table object ID and version
  typedef std::pair<int, int> Idver;
  const Idver old_table_idver = { DBG(m_table->getObjectId()),
                                  DBG(m_table->getObjectVersion()) };
  // Unload indexes, saving their object IDs and versions in a sorted list
  bool table_idver_mismatch = false;
  const Uint32 old_indexes_count = DBG(m_indexes.size());
  Idver* old_indexes_idver = m_amalloc->alloc_exc<Idver>(old_indexes_count);
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
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
  dict->invalidateTable(m_table);
  m_table = NULL;
  if (table_idver_mismatch) {
    // We don't need to reload table or indexes, since we have already
    // determined an inconsistency. This inconsistency should go away next time
    // we load metadata, and this is checked in RonSQLPreparer::load().
    DEB_TRACE();
    return true;
  }
  // Reload table
  const NdbDictionary::Table* new_table =
    DBG(dict->getTable(DBG(m_context.ast_root.table.c_str())));
  if (new_table == NULL) {
    // We don't need to reload indexes, since we have already determined a
    // schema change.
    DEB_TRACE();
    return true;
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
    return true;
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
      return true;
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
    return true;
  }
  std::sort(new_indexes_idver, new_indexes_idver + new_indexes_count);
  for (Uint32 i = 0; i < new_indexes_count; i++) {
    if (new_indexes_idver[i] != old_indexes_idver[i]) {
      // Object ID or version changed for this index, so a schema change
      // must have occurred.
      DEB_TRACE();
      return true;
    }
  }
  DEB_TRACE();
  return some_old_indexes_not_retrieved;
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
      apply_filter(filter, m_toplevel_conditions[i]);
    }
  }
  ndbrequire(has_filter);
  require_sch(DBG(filter->end()) >= 0, filter_fail);
}

void
RonSQLPreparer::apply_filter(NdbScanFilter* filter,
                             struct ConditionalExpression* ce)
{
  ndbrequire(ce != NULL);
  switch (ce->op)
  {
  case T_OR:
    require_tmp(DBG(filter->begin(NdbScanFilter::OR)) >= 0, filter_fail);
    apply_filter(filter, ce->args.left);
    apply_filter(filter, ce->args.right);
    require_sch(DBG(filter->end()) >= 0, filter_fail);
    break;
  case T_XOR:
    abort(); // This should have been "simplified" away
  case T_AND:
    require_tmp(DBG(filter->begin(NdbScanFilter::AND)) >= 0, filter_fail);
    apply_filter(filter, ce->args.left);
    apply_filter(filter, ce->args.right);
    require_sch(DBG(filter->end()) >= 0, filter_fail);
    break;
  case T_NOT:
    require_tmp(DBG(filter->begin(NdbScanFilter::NAND)) >= 0, filter_fail);
    apply_filter(filter, ce->args.left);
    require_sch(DBG(filter->end()) >= 0, filter_fail);
    break;
  case T_EQUALS:
    apply_filter_cmp(filter, NdbScanFilter::COND_EQ, ce->args.left, ce->args.right);
    break;
  case T_GE:
    apply_filter_cmp(filter, NdbScanFilter::COND_GE, ce->args.left, ce->args.right);
    break;
  case T_GT:
    apply_filter_cmp(filter, NdbScanFilter::COND_GT, ce->args.left, ce->args.right);
    break;
  case T_LE:
    apply_filter_cmp(filter, NdbScanFilter::COND_LE, ce->args.left, ce->args.right);
    break;
  case T_LT:
    apply_filter_cmp(filter, NdbScanFilter::COND_LT, ce->args.left, ce->args.right);
    break;
  case T_NOT_EQUALS:
    apply_filter_cmp(filter, NdbScanFilter::COND_NE, ce->args.left, ce->args.right);
    break;
  case T_LIKE:
    apply_filter_like(filter, NdbScanFilter::COND_LIKE,
                      ce->args.left, ce->args.right);
    break;
  default:
    throw RonSQLPermanentError("Non-boolean term in WHERE condition");
  }
}

void
RonSQLPreparer::apply_filter_cmp(NdbScanFilter* filter,
                                 NdbScanFilter::BinaryCondition cond,
                                 struct ConditionalExpression* left,
                                 struct ConditionalExpression* right)
{
  if (left->op != T_IDENTIFIER) {
    throw RonSQLPermanentError("For comparison operators, at least one of the"
                               " operands must be a column name");
  }
  if (right->op == T_IDENTIFIER) {
    ndbrequire(m_column_attrId_map != NULL);
    require_sch(DBG(filter->cmp(DBG(cond),
                                DBG(m_column_attrId_map[left->col_idx]),
                                DBG(m_column_attrId_map[right->col_idx]))) >= 0,
                filter_fail);
    return;
  }
  ndbrequire(m_column_attrId_map != NULL);
  ndbrequire(m_column_map != NULL);
  raw_value rv = encode_constant(right, m_column_map[left->col_idx]);
  require_sch(DBG(filter->cmp(DBG(cond),
                              DBG(m_column_attrId_map[left->col_idx]),
                              DBG(rv).val,
                              rv.len)) >= 0,
              filter_fail);
}

void
RonSQLPreparer::apply_filter_like(NdbScanFilter* filter,
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
  ndbrequire(m_column_attrId_map != NULL);
  require_sch(DBG(filter->cmp(cond,
                               m_column_attrId_map[left->col_idx],
                               right->string.str,
                               right->string.len)) >= 0,
              filter_fail);
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
    tk = INT; min = -2147483647LL; max = 2147483648LL; bytes = 4; break;
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
    tk = TIME; binlen = 4; timetype = MYSQL_TIMESTAMP_DATE; break;
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
      throw RonSQLMaybeStaleSchema("Integer type column compared to an integer"
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
        throw RonSQLMaybeStaleSchema("Decimal type column compared to an"
                                     " integer literal out of range.");
      }
      longlong2decimal(ce->constant_integer, &dec);
      err = decimal2bin(&dec, (unsigned char *)bin, prec, scale);
    } else if (op == T_FLOAT) {
      if (type == NdbDictionary::Column::Type::Decimalunsigned &&
          ce->constant_float.dbl < 0) {
        throw RonSQLMaybeStaleSchema("Decimal type column compared to a"
                                     " float literal out of range.");
      }
      LexString ls = ce->constant_float.ls;
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

#define programAggregator_do_or_fail(CALL) \
  require_prm(CALL, "Failed writing aggregation program. Please report a bug.")
void
RonSQLPreparer::programAggregator(NdbAggregator* aggregator)
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  SelectStatement& ast_root = m_context.ast_root;
  // Program groupby columns
  assert(m_column_attrId_map != NULL); // Ensured in RonSQLPreparer::load
  struct GroupbyColumns* groupby = ast_root.groupby_columns;
  while (groupby != NULL)
  {
    programAggregator_do_or_fail
      (aggregator->GroupBy(m_column_attrId_map[groupby->col_idx]));
    groupby = groupby->next;
  }
  // Program aggregations
  assert(m_agg != NULL); // Ensured in RonSQLPreparer::load
  DynamicArray<AggregationAPICompiler::Instr>& program = m_agg->m_program;
  for (Uint32 i=0; i<program.size(); i++)
  {
    AggregationAPICompiler::Instr* instr = &program[i];
    Uint32 dest = instr->dest;
    Uint32 src = instr->src;
    switch (instr->type)
    {
    case AggregationAPICompiler::SVMInstrType::Load:
    {
      NdbAttrId col_id = m_column_attrId_map[src];
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
        (aggregator->LoadInt64(m_agg->m_constants[src].int_64, dest));
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
    case AggregationAPICompiler::SVMInstrType::AggRepeat:
      programAggregator_do_or_fail(aggregator->RepeatAgg(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::EmbeddedInterp:
    {
      auto& ci = m_agg->m_cases[dest];
      Uint32 then_raw = m_agg->raw_word_size(ci.then_start, ci.skip_pos);
      Uint32 skip_raw = 1;
      Uint32 then_arm_total = then_raw + skip_raw;
      generate_embedded_condition(aggregator, ci.condition, then_arm_total);
      break;
    }
    case AggregationAPICompiler::SVMInstrType::Skip:
    {
      for (Uint32 c = 0; c < m_agg->m_cases.size(); c++)
      {
        if (m_agg->m_cases[c].skip_pos == i)
        {
          auto& ci = m_agg->m_cases[c];
          Uint32 else_raw = m_agg->raw_word_size(ci.else_start, ci.else_end);
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

void
RonSQLPreparer::programAggregator_join(NdbAggregator* aggregator)
{
  std::basic_ostream<char>& err = *m_conf.err_stream;
  SelectStatement& ast_root = m_context.ast_root;
  Uint32 leaf_idx = m_join_plan.agg_leaf_idx;

  // Program groupby columns — use GroupByLinked for parent columns
  assert(m_column_attrId_map != NULL);
  Uint32 linked_proj_pos = 0;
  struct GroupbyColumns* groupby = ast_root.groupby_columns;
  while (groupby != NULL)
  {
    Uint32 col_idx = groupby->col_idx;
    if (m_column_table_idx[col_idx] == leaf_idx)
    {
      programAggregator_do_or_fail
        (aggregator->GroupBy(m_column_attrId_map[col_idx]));
    }
    else
    {
      programAggregator_do_or_fail
        (aggregator->GroupByLinked(linked_proj_pos, m_column_map[col_idx]));
      linked_proj_pos++;
    }
    groupby = groupby->next;
  }

  // Program aggregations — same as single-table path
  assert(m_agg != NULL);
  DynamicArray<AggregationAPICompiler::Instr>& program = m_agg->m_program;
  for (Uint32 i = 0; i < program.size(); i++)
  {
    AggregationAPICompiler::Instr* instr = &program[i];
    Uint32 dest = instr->dest;
    Uint32 src = instr->src;
    switch (instr->type)
    {
    case AggregationAPICompiler::SVMInstrType::Load:
    {
      if (m_column_table_idx[src] != leaf_idx)
      {
        Uint32 lp_pos = find_or_add_linked_proj(
            m_join_plan, m_column_table_idx[src],
            m_columns[src].c_str());
        programAggregator_do_or_fail
          (aggregator->LoadLinkedColumn(lp_pos, dest, m_column_map[src]));
      }
      else
      {
        NdbAttrId col_id = m_column_attrId_map[src];
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
        (aggregator->LoadInt64(m_agg->m_constants[src].int_64, dest));
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
    case AggregationAPICompiler::SVMInstrType::AggRepeat:
      programAggregator_do_or_fail(aggregator->RepeatAgg(dest, src));
      break;
    case AggregationAPICompiler::SVMInstrType::EmbeddedInterp:
    {
      auto& ci = m_agg->m_cases[dest];
      Uint32 then_raw = m_agg->raw_word_size(ci.then_start, ci.skip_pos);
      Uint32 skip_raw = 1;
      Uint32 then_arm_total = then_raw + skip_raw;
      generate_embedded_condition(aggregator, ci.condition, then_arm_total);
      break;
    }
    case AggregationAPICompiler::SVMInstrType::Skip:
    {
      for (Uint32 c = 0; c < m_agg->m_cases.size(); c++)
      {
        if (m_agg->m_cases[c].skip_pos == i)
        {
          auto& ci = m_agg->m_cases[c];
          Uint32 else_raw = m_agg->raw_word_size(ci.else_start, ci.else_end);
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
}

void
RonSQLPreparer::generate_embedded_condition(
    NdbAggregator* aggregator,
    ConditionalExpression* ce,
    Uint32 then_arm_raw_size)
{
  // Two patterns supported:
  //   OR: (col = 'X' OR col = 'Y') → branch to THEN on match, fall-through ELSE
  //   AND: (col <> 'X' AND col <> 'Y') → branch to ELSE on inverted match,
  //                                       fall-through THEN
  //   Single atom: handled as OR with one atom
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

  // Compute embedded word sizes.
  // The embedded interpreter runs interpreterNextLab() which reads columns via
  // readAttributes() from the local tuple. For join queries, only columns on
  // the aggregation leaf table are accessible — columns from parent tables
  // arrive as linked attributes in the signal buffer, not readable by attrId.
  Uint32 total_branch_words = 0;
  for (Uint32 a = 0; a < atoms.size(); a++)
  {
    ConditionalExpression* atom = atoms[a];
    ndbrequire(atom->op == T_EQUALS || atom->op == T_NOT_EQUALS);
    ConditionalExpression* col_side = atom->args.left;
    ndbrequire(col_side->op == T_IDENTIFIER);
    if (m_column_table_idx != NULL)
    {
      Uint32 leaf_idx = m_join_plan.agg_leaf_idx;
      require_prm(m_column_table_idx[col_side->col_idx] == leaf_idx,
                  "CASE condition column must be on the aggregation leaf table. "
                  "Columns from parent tables in CASE conditions are not yet "
                  "supported.");
    }
    const NdbDictionary::Column* col = m_column_map[col_side->col_idx];
    Uint32 byte_len = col->getLength();
    Uint32 data_words = (byte_len + 3) / 4;
    total_branch_words += 2 + data_words;
  }
  Uint32 emb_len = total_branch_words + 6;

  // For OR:  branches → second_exit (THEN), fall-through → first_exit (ELSE)
  // For AND: branches → second_exit (ELSE), fall-through → first_exit (THEN)
  Uint32 second_exit_label = emb_len - 3;
  Uint32 first_exit_skip_offset =
      is_and ? 0 : then_arm_raw_size;
  Uint32 second_exit_skip_offset =
      is_and ? then_arm_raw_size : 0;

  programAggregator_do_or_fail(aggregator->EmbeddedInterp(emb_len));

  Uint32 pos = 0;
  for (Uint32 a = 0; a < atoms.size(); a++)
  {
    ConditionalExpression* atom = atoms[a];
    ConditionalExpression* col_side = atom->args.left;
    ConditionalExpression* val_side = atom->args.right;
    const NdbDictionary::Column* col = m_column_map[col_side->col_idx];
    NdbAttrId attr_id = m_column_attrId_map[col_side->col_idx];
    Uint32 byte_len = col->getLength();
    Uint32 data_words = (byte_len + 3) / 4;

    Uint32 branch_offset = second_exit_label - pos;

    // For OR with EQ atoms: BranchCol(EQ) → THEN on match
    // For AND with NE atoms: invert NE to EQ, BranchCol(EQ) → ELSE on match
    Interpreter::BinaryCondition cond;
    if (is_and)
    {
      // AND: invert the atom condition for the branch
      cond = (atom->op == T_NOT_EQUALS) ? Interpreter::EQ : Interpreter::NE;
    }
    else
    {
      // OR: branch on the atom condition directly
      cond = (atom->op == T_EQUALS) ? Interpreter::EQ : Interpreter::NE;
    }

    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::BranchCol(cond, Interpreter::NULL_CMP_EQUAL) |
        (branch_offset << 16)));
    programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
        Interpreter::BranchCol_2(attr_id, byte_len)));

    raw_value rv = encode_constant(val_side, col);
    Uint32 padded_len = data_words * 4;
    Uint8* buf = m_amalloc->alloc_exc<Uint8>(padded_len);
    memcpy(buf, rv.val, rv.len);
    if (padded_len > rv.len)
      memset(buf + rv.len, 0, padded_len - rv.len);
    for (Uint32 w = 0; w < data_words; w++)
    {
      Uint32 word;
      memcpy(&word, buf + w * 4, 4);
      programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(word));
    }
    pos += 2 + data_words;
  }

  // First exit (fall-through)
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::LoadConst16(2, first_exit_skip_offset)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::WriteInterpreterOutput(2, 0)));
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::ExitOK()));

  // Second exit (branched to)
  programAggregator_do_or_fail(aggregator->EmitEmbeddedWord(
      Interpreter::LoadConst16(2, second_exit_skip_offset)));
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
          pr = m_agg->Count(outputs->aggregate.arg);
          break;
        case T_MAX:
          pr = m_agg->Max(outputs->aggregate.arg);
          break;
        case T_MIN:
          pr = m_agg->Min(outputs->aggregate.arg);
          break;
        case T_SUM:
          pr = m_agg->Sum(outputs->aggregate.arg);
          break;
        default:
          // Unknown aggregate function
          abort();
        }
        out << "A" << pr << ":";
        m_agg->print_aggregate(pr);
        out << '\n';
      }
      break;
    case Outputs::Type::AVG:
      {
        Uint32 pr;
        out << "CLIENT-SIDE CALCULATION: ";
        pr = m_agg->Sum(outputs->avg.arg);
        out << "A" << pr << ":";
        m_agg->print_aggregate(pr);
        out << " / ";
        pr = m_agg->Count(outputs->avg.arg);
        out << "A" << pr << ":";
        m_agg->print_aggregate(pr);
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
      Uint32 col_idx = orderby->col_idx;
      bool ascending = orderby->ascending;
      out << "  C" << col_idx << ":" <<
        quoted_identifier(column_idx_to_name(col_idx)) <<
        (ascending ? " ASC" : " DESC") << '\n';
      orderby = orderby->next;
    }
  }
  if (ast_root.limit >= 0)
  {
    out << "LIMIT " << ast_root.limit << '\n';
  }

  out << '\n';

  // Print aggregation program
  if (m_agg != NULL)
  {
    m_agg->print_program();
  }
  else
  {
    out << "No aggregation program.\n";
  }

  out << '\n';

  // Print scan information
  if (m_conf.ndb == NULL) {
    out << "No NDB connection, so no index scan analysis.\n";
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
      ndbrequire(cond_cnt > 0);
      Uint32 filter_cnt = 0;
      for (Uint32 i = 0; i < cond_cnt; i++) {
        if (sc.condition_handling_map[i] == -1) {
          filter_cnt++;
        }
      }
      Uint32 bound_cnt = cond_cnt - filter_cnt;
      ndbrequire(bound_cnt > 0);
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
  if (m_parser.m_agg)
  {
    return m_parser.m_agg;
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
  m_parser.m_agg = new (get_allocator()->alloc_exc<AggregationAPICompiler>(1))
    AggregationAPICompiler(column_idx_to_name,
                           *m_parser.m_conf.out_stream,
                           *m_parser.m_conf.err_stream,
                           m_parser.m_amalloc);
  return m_parser.m_agg;
}

void
RonSQLPreparer::Context::enter_subquery()
{
  m_subquery_depth++;
  m_inner_agg = NULL;
}

void
RonSQLPreparer::Context::leave_subquery()
{
  ndbrequire(m_subquery_depth > 0);
  m_subquery_depth--;
  m_inner_agg = NULL;
}

ArenaMalloc*
RonSQLPreparer::Context::get_allocator()
{
  return m_parser.m_amalloc;
}
