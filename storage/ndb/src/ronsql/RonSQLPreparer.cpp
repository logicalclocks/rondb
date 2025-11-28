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

#include <assert.h>
#include "AggregationAPICompiler.hpp"
#include "RonSQLParser.y.hpp"
#include "RonSQLLexer.l.hpp"
#include "RonSQLPreparer.hpp"
#include <iostream>
#include "define_formatter.hpp"
#include "my_time.h"
#include "mysql_time.h"
#include "my_inttypes.h"
#include <my_base.h>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_RONSQLPREPARER 1
#endif

#ifdef DEBUG_RONSQLPREPARER
#define DEB_TRACE() do { \
  printf("RonSQLPreparer.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#endif

using std::endl;
using std::runtime_error;

#define feature_not_implemented(description) \
  throw runtime_error("RonSQL feature not implemented: " description)

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
  m_toplevel_conditions(conf.amalloc),
  m_scan_config_candidates(conf.amalloc)
{
  assert(m_status == Status::BEGIN);
  try
  {
    configure();
    parse();
    load();
    plan_index_and_filter();
    compile();
    determine_explain();
    m_status = Status::PREPARED;
  }
  catch (...)
  {
    m_status = Status::FAILED;
    throw;
  }
}

static inline void
soft_assert(bool condition, const char* msg)
{
  if (likely(condition)) return;
  throw runtime_error(msg);
}

void
RonSQLPreparer::configure()
{
  // Validate m_conf
#ifdef VM_TRACE
  assert(m_conf.sql_buffer != NULL);
  assert(m_conf.sql_len > 0);
  assert(m_conf.amalloc != NULL);
  RonSQLExecParams::ExplainMode mode = m_conf.explain_mode;
  bool may_query =
    (mode == RonSQLExecParams::ExplainMode::ALLOW ||
     mode == RonSQLExecParams::ExplainMode::FORBID ||
     mode == RonSQLExecParams::ExplainMode::REMOVE);
  bool may_explain =
    (mode == RonSQLExecParams::ExplainMode::ALLOW ||
     mode == RonSQLExecParams::ExplainMode::REQUIRE ||
     mode == RonSQLExecParams::ExplainMode::FORCE);
  assert(may_query || may_explain);
  assert(m_conf.out_stream != NULL);
  assert(m_conf.output_format == RonSQLExecParams::OutputFormat::JSON ||
         m_conf.output_format == RonSQLExecParams::OutputFormat::JSON_ASCII ||
         m_conf.output_format == RonSQLExecParams::OutputFormat::TEXT ||
         m_conf.output_format == RonSQLExecParams::OutputFormat::TEXT_NOHEADER);
  if (may_query)
  {
    assert(m_conf.ndb != NULL);
  }
  assert(m_conf.err_stream != NULL);
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
  assert(sql_len >= 2 &&
         sql_buffer[sql_len-1] == '\0' &&
         sql_buffer[sql_len-2] == '\0');
  rsqlp_lex_init_extra(&m_context, &m_scanner);
  // The non-const sql_buffer is only used to initialize the flex scanner. The
  // flex scanner shouldn't modify it either, but only because we have removed
  // the buffer-modifying code from the generated output (see build_lexer.sh).
  // For this reason, the lexer still declares the buffer as non-const.
  m_buf = rsqlp__scan_buffer(sql_buffer, sql_len, m_scanner);
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
    assert(m_context.m_err_state == ErrState::NONE);
    /* We have already provided columns and expressions to the
     * AggregationAPICompiler. E.g. in `SELECT Max(col1 + col2)`, m_agg already
     * knows about `col1`, `col2` and `col1 + col2`. Here, we let m_agg know about
     * the aggregate expressions themselves, e.g. `Max(col1 + col2)`, making sure
     * they are provided in the correct order.
     */
    Outputs* outputs = m_context.ast_root.outputs;
    bool has_aggregate_outputs = false;
    while (outputs != NULL)
    {
      switch (outputs->type)
      {
      case Outputs::Type::COLUMN:
        break;
      case Outputs::Type::AGGREGATE:
      {
        has_aggregate_outputs = true;
        assert(m_agg != NULL);
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
      default:
        abort();
      }
      outputs = outputs->next;
    }
    if (m_agg == NULL)
    {
      assert(!has_aggregate_outputs);
    }
    else
    {
      assert(has_aggregate_outputs);
      assert(m_agg->getStatus() == AggregationAPICompiler::Status::PROGRAMMING);
    }
    if (!has_aggregate_outputs)
    {
      assert(m_conf.err_stream != NULL);
      std::basic_ostream<char>& err = *m_conf.err_stream;
      err << "This query has no aggregate expression, so it is not an aggregate query.\n"
             "Currently, RonSQL only supports aggregate queries.\n";
      throw runtime_error("Not an aggregate query.");
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
     *    the allocation functio we use will never return NULL but rather throw
     *    an exception on OOM, so this case does not apply to us.
     * Therefore, we know that if we end up here, we are in case 2).
     */
    throw runtime_error("Parser stack exceeded its maximum depth.");
  }
  assert(parse_result == 1);
  assert(m_context.m_err_state != ErrState::NONE);
  assert(m_sql.str <= m_context.m_err_pos);
  size_t err_pos = m_context.m_err_pos - m_sql.str;
  size_t err_stop = err_pos + m_context.m_err_len;
  assert(err_pos <= m_sql.len);
  assert(err_stop <= m_sql.len + 1); // "Unexpected end of input" marks the
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
  throw runtime_error("Syntax error.");
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
  std::basic_ostream<char>& err = *m_conf.err_stream;
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
  if (ndb != NULL)
  {
    m_dict = ndb->getDictionary();
    m_table = m_dict->getTable(m_context.ast_root.table.c_str());
    soft_assert(m_table != NULL,
                "Failed to get table. Note that RonSQL only supports tables with"
                " ENGINE=NDB.");
    NdbAttrId* col_id_map = m_amalloc->alloc_exc<NdbAttrId>(m_columns.size());
    const NdbDictionary::Column** col_map =
        m_amalloc->alloc_exc<const NdbDictionary::Column*>(m_columns.size());
    for (Uint32 col_idx = 0; col_idx < m_columns.size(); col_idx++)
    {
      const char* col_name = m_columns[col_idx].c_str();
      const NdbDictionary::Column* col = m_table->getColumn(col_name);
      if (col == NULL)
      {
        err << "Failed to get column " << quoted_identifier(col_name) << "."
            << endl << "Note that column names are case sensitive." << endl;
        // It's possible that the schema is stale. Since we haven't attempted
        // any ndb operation yet, we have no error code from NDB and no way to
        // tell whether this error is due to stale schema or a permanent error
        // in the query. We unload the schema just in case it is stale, then
        // throw a separate exception type so that the caller can decide whether
        // to retry.
        unload_schema();
        throw ColumnNotFoundError();
      }
      col_id_map[col_idx] = col->getAttrId();
      col_map[col_idx] = col;
    }
    m_column_attrId_map = col_id_map;
    m_column_map = col_map;
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
  NdbDictionary::Dictionary::List index_list;
  assert(m_dict != NULL);
  assert(m_table != NULL);
  soft_assert(m_dict->listIndexes(index_list, *m_table) == 0,
              "Failed to list indexes.");
  for(Uint32 i = 0; i < index_list.count; i++) {
    NdbDictionary::Dictionary::List::Element& index_obj =
      index_list.elements[i];
    if (index_obj.state != NdbDictionary::Object::StateOnline) {
      // listIndexes() returns indexes in all states while this function is
      // only interested in indexes that are online and usable. Filtering out
      // indexes in other states is particularly important when metadata is
      // being restored as they may be in StateBuilding indicating that all
      // metadata related to the table hasn't been restored yet.
      continue;
    }
    if (index_obj.type == NdbDictionary::Object::UniqueHashIndex) {
      // We are not interested in hash indexes
      continue;
    }
    soft_assert(index_obj.type == NdbDictionary::Object::OrderedIndex,
                "Unexpected index type. Please report a bug.");
    // todo getIndex or getIndexGlobal?
    const NdbDictionary::Index* index = m_dict->getIndex(index_obj.name,
                                                         *m_table);
    soft_assert(index != NULL, "Failed to get index.");
    int *condition_handling_map =
      m_amalloc->alloc_exc<int>(m_toplevel_conditions.size());
    for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
      condition_handling_map[i] = -1;
    }
    int goodness = 0;
    unsigned col_count = index->getNoOfColumns();
    soft_assert(col_count > 0,
                "Index appears to have no columns. Please report a bug.");
    bool later_columns_blocked = false;
    for(unsigned col_idx = 0;
        col_idx < col_count && !later_columns_blocked;
        col_idx++)
    {
      const NdbDictionary::Column* column = index->getColumn(col_idx);
      soft_assert(column != NULL,
                  "Index column object is NULL. Please report a bug.");
      // todo Can getAttrId be used to match against the parent table?
      const char* column_name = column->getName();
      bool lbound_set = false, ubound_set = false;
      for(Uint32 cond_idx = 0;
          cond_idx < m_toplevel_conditions.size() &&
            !(lbound_set && ubound_set);
          cond_idx++)
      {
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
        assert(condition_identifier->op == T_IDENTIFIER);
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
        // will be. Each bound added to the configuration adds 10000 for
        // equality, 100 for a double-bounded range and 10 for a half-open
        // range. A 10% bonus is added for data types other than VARCHAR.
        int points = 10;
        if (column->getType() != NdbDictionary::Column::Type::Varchar) points++;
        if (lbound_set && ubound_set) points *= 10;
        if (!later_columns_blocked) points *= 100;
        goodness += points;
      }
    }
    if (goodness) {
      m_scan_config_candidates.push(ScanConfig { index,
                                                 condition_handling_map,
                                                 goodness });
    }
  }
}

void
RonSQLPreparer::compile()
{
  // Compile aggregation program if applicable
  if (m_agg != NULL)
  {
    if (m_agg->compile())
    {
      assert(m_agg->getStatus() == AggregationAPICompiler::Status::COMPILED);
    }
    else
    {
      assert(m_agg->getStatus() == AggregationAPICompiler::Status::FAILED);
      throw runtime_error("Failed to compile aggregation program.");
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
    soft_assert(!do_explain, "Tried to EXPLAIN with explain mode set to FORBID.");
    break;
  case RonSQLExecParams::ExplainMode::REQUIRE:
    soft_assert(do_explain, "Tried to query with explain mode set to REQUIRE.");
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
RonSQLPreparer::execute()
{
  DEB_TRACE();
  soft_assert(m_status != Status::FAILED,
              "Attempting RonSQLPreparer::execute while in failed state.");
  DEB_TRACE();
  assert(m_status == Status::PREPARED);
  DEB_TRACE();
  Ndb* ndb = m_conf.ndb;
  NdbTransaction* myTrans = NULL;
  DEB_TRACE();
  try
  {
    if (m_do_explain)
    {
      switch (m_conf.output_format)
      {
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
    soft_assert(ndb != NULL, "Cannot query without ndb object.");
    myTrans = ndb->startTransaction();
    soft_assert(myTrans != NULL, "Failed to start transaction.");
    // Since ndb exists, m_table should have been initialized in load()
    assert(m_table != NULL);
    NdbAggregator aggregator(m_table);
    programAggregator(&aggregator);
    soft_assert(aggregator.Finalize(), "Failed to finalize aggregator.");
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
      DEB_TRACE();
      // Prepare and execute full table scan
      DEB_TRACE();
      NdbScanOperation* myScanOp = myTrans->getNdbScanOperation(m_table);
      soft_assert(myScanOp != NULL, "Failed to get scan operation.");
      soft_assert(myScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead) == 0,
                  "Failed to initialize scan operation.");
      DEB_TRACE();
      if (has_filter)
      {
        DEB_TRACE();
        NdbScanFilter filter(myScanOp);
        DEB_TRACE();
        apply_filter_top_level(&filter);
        DEB_TRACE();
      }
      DEB_TRACE();
      soft_assert(myScanOp->setAggregationCode(&aggregator) >= 0,
                  "Failed to set aggregation code.");
      DEB_TRACE();
      soft_assert(myScanOp->DoAggregation() >= 0,
                  "Failed to execute aggregation.");
      DEB_TRACE();
    } else {
      DEB_TRACE();
      // Prepare and execute index scan
      NdbIndexScanOperation *myIndexScanOp =
        myTrans->getNdbIndexScanOperation(index);
      Uint32 scanFlags = 0;
      // todo Decide whether NdbScanOperation::SF_OrderBy is good for performance
      soft_assert(myIndexScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead,
                                            scanFlags) == 0,
                  "Failed to initialize index scan operation.");
      for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++)
      {
        int index_col_idx = sc.condition_handling_map[i];
        if (index_col_idx == -1) {
          // This condition could not be configured for the index scan. It will
          // be applied as part of the filter instead.
          continue;
        }
        ConditionalExpression* ce = m_toplevel_conditions[i];
        Uint32 condition_col_idx = ce->args.left->col_idx;
        ConditionalExpression* condition_constant = ce->args.right;
        TokenKind op = ce->op;
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
        soft_assert(myIndexScanOp->setBound(colName, bt, rv.val) == 0,
                    "Failed to set bound for index scan.");
      }
      // todo Is this necessary after removing the multirange flag?
      soft_assert(myIndexScanOp->end_of_bound(0) == 0,
                  "Failed to set end of bound.");
      if (has_filter)
      {
        NdbScanFilter filter(myIndexScanOp);
        apply_filter_top_level(&filter);
      }
      soft_assert(myIndexScanOp->setAggregationCode(&aggregator) >= 0,
                  "Failed to set aggregation code.");
      soft_assert(myIndexScanOp->DoAggregation() >= 0,
                  "Failed to execute aggregation.");
    }
    DEB_TRACE();

    // Print results
    m_resultprinter->print_result(&aggregator, m_conf.out_stream);
    DEB_TRACE();

    ndb->closeTransaction(myTrans);
    DEB_TRACE();
  }
  catch (const std::exception& e)
  {
    std::basic_ostream<char>& err = *m_conf.err_stream;

    // Fetch error
    DEB_TRACE();
    NdbError ndb_err;
    if (myTrans != NULL)
    {
      ndb_err = myTrans->getNdbError();
      ndb->closeTransaction(myTrans);
    }
    else if (ndb != NULL)
    {
      ndb_err = ndb->getNdbError();
    }
    else
    {
      DEB_TRACE();
      throw;
    }

    // Decide whether to unload and whether the error is temporary
    if (/*Invalid schema object version*/
        (ndb_err.mysql_code == HA_ERR_TABLE_DEF_CHANGED &&
         ndb_err.code == 241)) {
      unload_schema();
      err << "Retry after unload is possible" << endl;
      throw TemporaryError();
    }
    if (/*Table is being dropped*/
               (ndb_err.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                ndb_err.code == 283)) {
      unload_schema();
    }
    if (/*Table not defined in transaction coordinator*/
               (ndb_err.mysql_code == HA_ERR_TABLE_DEF_CHANGED &&
                ndb_err.code == 284)) {
      unload_schema();
      err << "Retry after unload is possible" << endl;
      throw TemporaryError();
    }
    if (/*No such table existed*/
               (ndb_err.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                ndb_err.code == 709)) {
      unload_schema();
    }
    if (/*No such table existed*/
               (ndb_err.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                ndb_err.code == 723)) {
      unload_schema();
    }
    if (/*Table is being dropped*/
               (ndb_err.mysql_code == HA_ERR_NO_SUCH_TABLE &&
                ndb_err.code == 1226)) {
      unload_schema();
    }

    switch (ndb_err.status)
    {
    case NdbError::Status::Success:
      assert(ndb_err.code == 0);
      // Rethrow since error not from ndb
      DEB_TRACE();
      throw;
    case NdbError::Status::TemporaryError:
      err << "NDB Temporary error: " << ndb_err.code << " " << ndb_err.message
          << endl
          << "Caught exception, probably caused by the temporary error above: "
          << e.what() << endl;
      DEB_TRACE();
      throw TemporaryError();
    case NdbError::Status::PermanentError:
      err << "NDB Permanent error " << ndb_err.code << ": " << ndb_err.message
          << endl;
      // Now that the ndb error is described on err stream, we'll rethrow the
      // original exception.
      DEB_TRACE();
      throw;
    case NdbError::Status::UnknownResult:
      err << "NDB Unknown result: " << ndb_err.code << ": " << ndb_err.message
          << endl;
      // Now that the ndb error is described on err stream, we'll rethrow the
      // original exception.
      DEB_TRACE();
      throw;
    }
    // Unreachable
    DEB_TRACE();
    abort();
  }
  catch (...)
  {
    // All exceptions thrown should be instances of runtime_error.
    DEB_TRACE();
    abort();
  }
}

void
RonSQLPreparer::unload_schema() {
  Ndb* ndb = m_conf.ndb;
  const char* table = m_context.ast_root.table.c_str();
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  NdbDictionary::Dictionary::List indexes;
  dict->listIndexes(indexes, table);
  for (unsigned i = 0; i < indexes.count; i++) {
    dict->invalidateIndex(indexes.elements[i].name, table);
  }
  dict->invalidateTable(table);
}

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
  soft_assert(filter->begin(NdbScanFilter::AND) >= 0,
              "Failed to apply filter.");
  bool has_filter = false;
  for (Uint32 i = 0; i < m_toplevel_conditions.size(); i++) {
    if (m_scan_config->condition_handling_map[i] == -1) {
      has_filter = true;
      soft_assert(apply_filter(filter, m_toplevel_conditions[i]),
                  "Failed to apply filter.");
    }
  }
  assert(has_filter);
  soft_assert(filter->end() >= 0, "Failed to apply filter.");
}

bool
RonSQLPreparer::apply_filter(NdbScanFilter* filter,
                             struct ConditionalExpression* ce)
{
  assert (ce != NULL);
  switch (ce->op)
  {
  case T_OR:
    return (filter->begin(NdbScanFilter::OR) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            apply_filter(filter, ce->args.right) &&
            filter->end() >= 0);
  case T_XOR:
    abort(); // This should have been "simplified" away
  case T_AND:
    return (filter->begin(NdbScanFilter::AND) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            apply_filter(filter, ce->args.right) &&
            filter->end() >= 0);
  case T_NOT:
    return (filter->begin(NdbScanFilter::NAND) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            filter->end() >= 0);
  case T_EQUALS:
    return apply_filter_cmp(filter, NdbScanFilter::COND_EQ, ce->args.left, ce->args.right);
  case T_GE:
    return apply_filter_cmp(filter, NdbScanFilter::COND_GE, ce->args.left, ce->args.right);
  case T_GT:
    return apply_filter_cmp(filter, NdbScanFilter::COND_GT, ce->args.left, ce->args.right);
  case T_LE:
    return apply_filter_cmp(filter, NdbScanFilter::COND_LE, ce->args.left, ce->args.right);
  case T_LT:
    return apply_filter_cmp(filter, NdbScanFilter::COND_LT, ce->args.left, ce->args.right);
  case T_NOT_EQUALS:
    return apply_filter_cmp(filter, NdbScanFilter::COND_NE, ce->args.left, ce->args.right);
  default:
    throw runtime_error("Non-boolean term in WHERE condition");
  }
}

bool
RonSQLPreparer::apply_filter_cmp(NdbScanFilter* filter,
                                   NdbScanFilter::BinaryCondition cond,
                                   struct ConditionalExpression* left,
                                   struct ConditionalExpression* right)
{
  if (left->op != T_IDENTIFIER) {
    throw runtime_error("For comparison operators, at least one of the operands must be a column name");
  }
  if (right->op == T_IDENTIFIER)
  {
    assert(m_column_attrId_map != NULL);
    // todo This only works in simple expressions. For full correctness, the
    // condition needs to be translated from 3-valued logic to 2-valued logic.
    return (filter->begin(NdbScanFilter::AND) >= 0 &&
            filter->isnotnull(m_column_attrId_map[left->col_idx]) >=0 &&
            filter->isnotnull(m_column_attrId_map[right->col_idx]) >=0 &&
            filter->cmp(cond,
                        m_column_attrId_map[left->col_idx],
                        m_column_attrId_map[right->col_idx]) >= 0 &&
            filter->end() >= 0);
  }
  assert(m_column_attrId_map != NULL);
  assert(m_column_map != NULL);
  raw_value rv = encode_constant(right, m_column_map[left->col_idx]);
  return (filter->begin(NdbScanFilter::AND) >= 0 &&
          filter->isnotnull(m_column_attrId_map[left->col_idx]) >=0 &&
          filter->cmp(cond,
                      m_column_attrId_map[left->col_idx],
                      rv.val, rv.len) >= 0 &&
          filter->end() >= 0);
}

void
rondb_str_to_mysql_time(MYSQL_TIME *mt, LexString str) {
  my_time_flags_t flags = 0; // todo
  MYSQL_TIME_STATUS status;
  bool err = str_to_datetime(str.str, str.len, mt, flags, &status);
  if (unlikely(err)) {
    throw runtime_error("Failed to interpret string literal as a date or"
                        " timestamp");
  }
  if (status.warnings) {
    throw runtime_error("String literal interpreted as date or timestamp with"
                        " warnings");
  }
  if (status.m_deprecation.m_kind !=
      MYSQL_TIME_STATUS::DEPRECATION::DEPR_KIND::DP_NONE) {
    throw runtime_error("String literal interpreted as date or timestamp with"
                        " weird delimiters or spaces");
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
  const int INT = 1, STR = 2, TIME = 3;
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
    throw runtime_error("Unsupported column type in comparison condition."
                        " Supported types are integer types, VARCHAR, DATE,"
                        " DATETIME and TIMESTAMP.");
  }
  if (op == T_INT && tk == INT) {
    if (ce->constant_integer < min || ce->constant_integer > max) {
      throw runtime_error("Integer type column compared to an integer literal"
                          " out of range.");
    }
    Int64* val = m_amalloc->alloc_exc<Int64>(1);
    *val = ce->constant_integer;
    return raw_value{ val, static_cast<Uint32>(bytes) };
  }
  if (tk == INT) {
    throw runtime_error("Integer type column compared to an incompatible value."
                        " Only integer literals are supported.");
  }
  if (op == T_INT) {
    throw runtime_error("Non-integer type column compared to an integer"
                        " literal.");
  }
  if (op == T_STRING && tk == STR) {
    if (ce->string.len > static_cast<size_t>(maxlen)) {
      throw runtime_error("VARCHAR column compared to a string literal that is"
                          " too long. Note that if the column length is less"
                          " than 256, then the length of the string literal"
                          " must also be less than 256.");
    }
    Uint8* val = m_amalloc->alloc_exc<Uint8>(lenbytes + ce->string.len);
    memcpy(val, &ce->string.len, lenbytes);
    memcpy(val + lenbytes, ce->string.str, ce->string.len);
    return raw_value{ val, static_cast<Uint32>(lenbytes + ce->string.len) };
  }
  if (tk == STR) {
    throw runtime_error("VARCHAR column compared to an incompatible value. Only"
                        " string literals are supported.");
  }
  if (tk == TIME) {
    MYSQL_TIME mt;
    if (op == T_STRING) {
      rondb_str_to_mysql_time(&mt, ce->string);
    } else if (op == I_MYSQL_TIME) {
      mt = ce->mysql_time;
    } else {
      throw runtime_error("DATE/DATETIME/TIMESTAMP column compared to an"
                          " incompatible value. Only string literals and calls"
                          " to DATE_ADD and DATE_SUB are supported.");
    }
    uchar* bindate = m_amalloc->alloc_exc<uchar>(binlen);
    int precision = col->getPrecision();
    int warnings = 0;
    if (unlikely(mt.time_type != timetype)) {
      throw runtime_error("DATE/DATETIME/TIMESTAMP column compared to a valid"
                          " date/time constant of the wrong type. Note that"
                          " presence/absence of the time part must match.");
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
        throw runtime_error("Perhaps an invalid DATETIME constant. Please"
                            " report a bug.");
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
        throw runtime_error("TIMESTAMP column compared to a constant out of"
                            " range (valid range is '1970-01-01 00:00:01' UTC"
                            " to '2038-01-19 03:14:07' UTC)");
      }
      my_datetime_adjust_frac(&mt, precision, &warnings, true);
      if (unlikely(warnings != 0)) {
        // Actually won't ever happen when truncate argument is set
        throw runtime_error("Perhaps an invalid TIMESTAMP constant. Please"
                            " report a bug.");
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
  throw runtime_error("Bug in RonSQLPreparer::encode_constant");
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
      ConditionalExpression* ret = m_amalloc->alloc_exc<ConditionalExpression>(1);
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
        throw runtime_error("The first argument to DATE_ADD and DATE_SUB must be a string literal or another call to DATE_ADD or DATE_SUB");
      }
      if (interval_ce->op != T_INTERVAL)
      {
        throw runtime_error("The second argument to DATE_ADD and DATE_SUB must be an INTERVAL literal");
      }
      ConditionalExpression* amount_ce = interval_ce->interval.arg;
      Int64 constant_integer;
      switch (amount_ce->op) {
      case T_INT:
        constant_integer = amount_ce->constant_integer;
        break;
      case T_STRING:
        {
          const char* string_literal = amount_ce->string.to_LexCString(m_amalloc).c_str();
          char* endptr;
          constant_integer = strtoll(string_literal, &endptr, 10);
          if (*endptr != '\0') {
            throw runtime_error("Failed to convert INTERVAL string literal amount to an integer");
          }
        }
        break;
      default:
        throw runtime_error("INTERVAL literal requires an amount in the form of an integer literal or a string literal representing an integer");
      }
      // Member variables in Interval are unsigned long int or unsigned long long int
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
        throw runtime_error("INTERVAL literals support only interval types MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, and YEAR");
      }
      int warnings = 0;
      bool err = date_add_interval(&ltime,
                                   interval_type,
                                   interval,
                                   &warnings);
      if (err || warnings) {
        throw runtime_error("DATE_ADD or DATE_SUB failed");
      }
      ConditionalExpression* ret = m_amalloc->alloc_exc<ConditionalExpression>(1);
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
        ConditionalExpression* r = m_amalloc->alloc_exc<ConditionalExpression>(1);
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
  default:
    // No simplification to do
    return ce;
  }
}

#define programAggregator_do_or_fail(CALL) \
  do { \
    if (!(CALL)) \
    { \
      err << "Failed writing aggregation program at " #CALL << endl; \
      throw runtime_error("Failed writing aggregation program"); \
    } \
  } while (0)
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
        throw runtime_error("Failed writing aggregation program");
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
    default:
      // Unknown instruction
      abort();
    }
  }
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
    out << "WHERE\n";
    print(where, LexString{NULL, 0});
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
    } else {
      out << "Execute as index scan.\n"
          << "Index: " << quoted_identifier(sc.index->getName()) << "(";
      for (Uint32 i = 0; i < sc.index->getNoOfColumns(); i++) {
        if (i > 0) out << ", ";
        out << sc.index->getColumn(i)->getName();
      }
      out << ")\nWith goodness " << sc.goodness << " it's the best of "
          << m_scan_config_candidates.size() << " options.\n";
    }
    Uint32 cond_cnt = m_toplevel_conditions.size();
    if (cond_cnt) {
      out << "There are " << cond_cnt << " top-level conditions:\n";
    } else {
      out << "There are no top-level conditions.\n";
    }
    for (Uint32 i = 0; i < cond_cnt; i++) {
      out << "- ";
      int handling = sc.condition_handling_map[i];
      Uint32 prefixlen;
      if (handling == -1) {
        out << "FILTER: ";
        prefixlen = 10;
      } else {
        out << "INDEX[" << handling << "]: ";
        prefixlen = 12;
        if (handling > 9) {
          prefixlen++;
        }
      }
      print(m_toplevel_conditions[i], LexString{"             ", prefixlen});
    }
  }

  out << '\n';

  // Print post-processing information
  assert(m_resultprinter != NULL);
  assert(m_conf.out_stream != NULL);
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
          << prefix << "+- ";
      LexString prefix_arg = prefix.concat(LexString{"|  ", 3}, m_amalloc);
      print(ce->is.arg, prefix_arg);
      out << prefix << "\\- "
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
          << prefix << "\\- ";
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
          << prefix << "+- ";
      LexString prefix_arg = prefix.concat(LexString{"|  ", 3}, m_amalloc);
      print(ce->interval.arg, prefix_arg);
      out << prefix << "\\- " <<
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
        << prefix << "+- "
        << interval_type_name(ce->extract.interval_type) << '\n'
        << prefix << "\\- ";
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_amalloc);
      print(ce->extract.arg, prefix_arg);
      return;
    }
  default:
    // Unknown operator
    abort();
  }
  if (prefix_op)
  {
    out << opstr << '\n'
        << prefix << "\\- ";
    LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_amalloc);
    print(ce->args.left, prefix_arg);
  }
  else
  {
    out << opstr << '\n'
        << prefix << "+- ";
    LexString prefix_left = prefix.concat(LexString{"|  ", 3}, m_amalloc);
    print(ce->args.left, prefix_left);
    out << prefix << "\\- ";
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
  Uint32 sz = columns.size();
  for (Uint32 i=0; i < sz; i++)
  {
    if (columns[i] == col_name)
    {
      return i;
    }
  }
  columns.push(col_name);
  return sz;
}

LexCString
RonSQLPreparer::column_idx_to_name(Uint32 col_idx)
{
  assert(col_idx < m_columns.size());
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
     * same thing, but here we assert so.
     */
    assert((m_err_pos < err_pos) ||
           (m_err_pos == err_pos &&
            m_err_len <= err_len));
  }
}

AggregationAPICompiler*
RonSQLPreparer::Context::get_agg()
{
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

ArenaMalloc*
RonSQLPreparer::Context::get_allocator()
{
  return m_parser.m_amalloc;
}
