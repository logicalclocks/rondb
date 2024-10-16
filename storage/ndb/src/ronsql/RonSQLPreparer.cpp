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
  m_index_scan_config_candidates(conf.amalloc)
{
  assert(m_status == Status::BEGIN);
  try
  {
    configure();
    parse();
    load();
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
  if (!condition)
  {
    throw runtime_error(msg);
  }
}

void
RonSQLPreparer::configure()
{
  // Validate m_conf
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
  // Populate m_dict, m_table and m_column_attrId_map, on the condition that
  // m_conf.ndb is available. If m_conf.ndb is not available, we'll still be
  // able to do a (partial) EXPLAIN SELECT, so no need to fail yet.
  Ndb* ndb = m_conf.ndb;
  if (ndb != NULL)
  {
    m_dict = ndb->getDictionary();
    m_table = m_dict->getTable(m_context.ast_root.table.c_str());
    soft_assert(m_table != NULL,
                "Failed to get table. Note that RonSQL only supports tables with"
                " ENGINE=NDB.");
    NdbAttrId* col_id_map = m_amalloc->alloc_exc<NdbAttrId>(m_columns.size());
    for (Uint32 col_idx = 0; col_idx < m_columns.size(); col_idx++)
    {
      const char* col_name = m_columns[col_idx].c_str();
      const NdbDictionary::Column* col = m_table->getColumn(col_name);
      if (col == NULL)
      {
        err << "Failed to get column " << quoted_identifier(col_name) << "."
            << endl << "Note that column names are case sensitive." << endl;
        throw runtime_error("Failed to get column.");
      }
      col_id_map[col_idx] = col->getAttrId();
    }
    m_column_attrId_map = col_id_map;
  }

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
  generate_index_scan_config_candidates();
  choose_index_scan_config();
  if (!m_do_index_scan)
  {
    // Fallback to table scan
    m_do_table_scan = true;
    m_table_scan_filter = m_context.ast_root.where_expression;
  }
}

void
RonSQLPreparer::generate_index_scan_config_candidates()
{
  struct ConditionalExpression* ce = m_context.ast_root.where_expression;
  if (ce == NULL)
  {
    // No WHERE clause
    return;
  }
  // Hard coded case to only detect structure
  // WHERE [column_1] >= [int_1] AND [column_1] < [int_2] AND [some_condition]
  // todo Generalize. Perhaps a good ambition for the first step is to flatten the top AND tree, iterate through all columns and make an attempt for each.
  assert(ce != NULL);
  if (
      ce->op == T_AND &&
        ce->args.left->op == T_AND &&
          ce->args.left->args.left->op == T_GE &&
            ce->args.left->args.left->args.left->op == T_IDENTIFIER &&
            ce->args.left->args.left->args.right->op == T_INT &&
          ce->args.left->args.right->op == T_LT &&
            ce->args.left->args.right->args.left->op == T_IDENTIFIER &&
            ce->args.left->args.right->args.right->op == T_INT &&
      ce->args.left->args.left->args.left->col_idx ==
      ce->args.left->args.right->args.left->col_idx)
  {
    Uint32 col_idx = ce->args.left->args.left->args.left->col_idx;
    Int64 low_bound = ce->args.left->args.left->args.right->constant_integer;
    Int64 high_bound = ce->args.left->args.right->args.right->constant_integer;
    IndexScanConfig isc;
    isc.col_idx = col_idx;
    isc.ranges = m_amalloc->alloc_exc<IndexScanConfig::Range>(1);
    isc.ranges[0].ltype = IndexScanConfig::Range::Type::INCLUSIVE;
    isc.ranges[0].lvalue = low_bound;
    isc.ranges[0].htype = IndexScanConfig::Range::Type::EXCLUSIVE;
    isc.ranges[0].hvalue = high_bound;
    isc.range_count = 1;
    isc.filter = ce->args.right;
    m_index_scan_config_candidates.push(isc);
  }
}

void
RonSQLPreparer::choose_index_scan_config()
{
  // todo: Currently, we just pick the first match. We'll have to optimize index choice.
  if (m_index_scan_config_candidates.size() == 0)
  {
    // No candidate configurations, so no matter what indexes are found we won't
    // use them.
    return;
  }
  if (m_conf.ndb == NULL)
  {
    // No connection, so we can't discover indexes.
    return;
  }
  // todo find and iterate through indexes.
  NdbDictionary::Dictionary::List index_list;
  assert(m_dict != NULL);
  assert(m_table != NULL);
  soft_assert(m_dict->listIndexes(index_list, *m_table) == 0,
              "Failed to list indexes.");
  for(Uint32 i = 0; i < index_list.count; i++)
  {
    NdbDictionary::Dictionary::List::Element& index_obj = index_list.elements[i];
    if (index_obj.state != NdbDictionary::Object::StateOnline) {
      // listIndexes() returns indexes in all states while this function is
      // only interested in indexes that are online and usable. Filtering out
      // indexes in other states is particularly important when metadata is
      // being restored as they may be in StateBuilding indicating that all
      // metadata related to the table hasn't been restored yet.
      continue;
    }
    switch (index_obj.type) {
      case NdbDictionary::Object::OrderedIndex:
        {
          // todo getIndex or getIndexGlobal?
          const NdbDictionary::Index* index = m_dict->getIndex(index_obj.name, *m_table);
          soft_assert(index != NULL, "Failed to get index.");
          // We are only interested in the first index column in an ordered index,
          // since that is the column the index is first/mainly ordered on.
          assert(index->getNoOfColumns() > 0);
          const NdbDictionary::Column* first_index_column = index->getColumn(0);
          assert(first_index_column != NULL);
          const char* first_index_column_name = first_index_column->getName();
          // Try to match the index column name against the name of the table
          // column used in the config candidates
          for (Uint32 j = 0; j < m_index_scan_config_candidates.size(); j++)
          {
            IndexScanConfig& candidate = m_index_scan_config_candidates[j];
            const char* candidate_column_name = m_columns[candidate.col_idx].c_str();
            if (strcmp(first_index_column_name, candidate_column_name) == 0)
            {
              // Found a match
              m_do_index_scan = true;
              m_index_scan_config = &candidate;
              m_index_scan_index = index;
              return;
            }
          }
        }
        break;
      case NdbDictionary::Object::UniqueHashIndex:
        // We are not interested in hash indexes
        continue;
      default:
        // Unexpected object type
        abort();
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
  soft_assert(m_status != Status::FAILED,
              "Attempting RonSQLPreparer::execute while in failed state.");
  assert(m_status == Status::PREPARED);
  Ndb* ndb = m_conf.ndb;
  NdbTransaction* myTrans = NULL;
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
        abort();
      }
      return;
    }
    soft_assert(ndb != NULL, "Cannot query without ndb object.");
    myTrans = ndb->startTransaction();
    soft_assert(myTrans != NULL, "Failed to start transaction.");
    // Since ndb exists, m_table should have been initialized in load()
    assert(m_table != NULL);
    NdbAggregator aggregator(m_table);
    programAggregator(&aggregator);
    soft_assert(aggregator.Finalize(), "Failed to finalize aggregator.");
    // End of general preparation

    assert(m_do_table_scan || m_do_index_scan);

    if(m_do_table_scan)
    {
      // Prepare and execute full table scan
      assert(!m_do_index_scan &&
             m_index_scan_config == NULL &&
             m_index_scan_index == NULL);
      NdbScanOperation* myScanOp = myTrans->getNdbScanOperation(m_table);
      soft_assert(myScanOp != NULL, "Failed to get scan operation.");
      soft_assert(myScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead) == 0,
                  "Failed to initialize scan operation.");
      if (m_table_scan_filter != NULL)
      {
        NdbScanFilter filter(myScanOp);
        apply_filter_top_level(&filter, m_table_scan_filter);
      }
      soft_assert(myScanOp->setAggregationCode(&aggregator) >= 0,
                  "Failed to set aggregation code.");
      soft_assert(myScanOp->DoAggregation() >= 0,
                  "Failed to execute aggregation.");
    }
    if(m_do_index_scan)
    {
      // Prepare and execute index scan
      assert(m_index_scan_config != NULL &&
             m_index_scan_index != NULL &&
             !m_do_table_scan &&
             m_table_scan_filter == NULL);
      NdbIndexScanOperation *myIndexScanOp =
        myTrans->getNdbIndexScanOperation(m_index_scan_index);
      Uint32 scanFlags = NdbScanOperation::SF_MultiRange;
      // todo Decide whether NdbScanOperation::SF_OrderBy is good for performance
      // todo Perhaps apply NdbScanOperation::SF_MultiRange only when necessary
      soft_assert(myIndexScanOp->readTuples(NdbOperation::LockMode::LM_CommittedRead, scanFlags) == 0,
                  "Failed to initialize index scan operation.");
      IndexScanConfig& isc = *m_index_scan_config;
      const char* col_name = m_columns[isc.col_idx].c_str();
      for (Uint32 i = 0; i < isc.range_count; i++)
      {
        const IndexScanConfig::Range& range = isc.ranges[i];
        if (range.ltype == IndexScanConfig::Range::Type::INCLUSIVE &&
            range.htype == IndexScanConfig::Range::Type::INCLUSIVE &&
            range.lvalue == range.hvalue)
        {
          // Equality
          soft_assert(myIndexScanOp->setBound(col_name,
                                              NdbIndexScanOperation::BoundType::BoundEQ,
                                              &range.lvalue) == 0,
                      "Failed to set lower bound.");
          soft_assert(myIndexScanOp->end_of_bound(i) == 0,
                      "Failed to set end of bound.");
          continue;
        }
        static const int BoundNone = -1; // datatype matches lboundt and hboundt
        static_assert(NdbIndexScanOperation::BoundType::BoundEQ != BoundNone);
        static_assert(NdbIndexScanOperation::BoundType::BoundLE != BoundNone);
        static_assert(NdbIndexScanOperation::BoundType::BoundLT != BoundNone);
        static_assert(NdbIndexScanOperation::BoundType::BoundGT != BoundNone);
        static_assert(NdbIndexScanOperation::BoundType::BoundGE != BoundNone);
        int lboundt; // datatype matches NdbIndexScanOperation::setBound
        int hboundt; // datatype matches NdbIndexScanOperation::setBound
        switch(range.ltype)
        {
        case IndexScanConfig::Range::Type::NONE:
          lboundt = BoundNone;
          break;
        case IndexScanConfig::Range::Type::INCLUSIVE:
          lboundt = NdbIndexScanOperation::BoundLE;
          break;
        case IndexScanConfig::Range::Type::EXCLUSIVE:
          lboundt = NdbIndexScanOperation::BoundLT;
          break;
        default:
          abort();
        }
        switch(range.htype)
        {
        case IndexScanConfig::Range::Type::NONE:
          hboundt = BoundNone;
          break;
        case IndexScanConfig::Range::Type::EXCLUSIVE:
          hboundt = NdbIndexScanOperation::BoundGT;
          break;
        case IndexScanConfig::Range::Type::INCLUSIVE:
          hboundt = NdbIndexScanOperation::BoundGE;
          break;
        default:
          abort();
        }
        assert(lboundt != BoundNone ||
               hboundt != BoundNone);
        if (lboundt != BoundNone && hboundt != BoundNone)
        {
          assert(range.lvalue < range.hvalue);
        }
        if (lboundt != BoundNone)
        {
          soft_assert(myIndexScanOp->setBound(col_name,
                                              lboundt,
                                              &range.lvalue) == 0,
                      "Failed to set lower bound.");
        }
        if (hboundt != BoundNone)
        {
          soft_assert(myIndexScanOp->setBound(col_name,
                                              hboundt,
                                              &range.hvalue) == 0,
                      "Failed to set upper bound.");
        }
        soft_assert(myIndexScanOp->end_of_bound(i) == 0,
                    "Failed to set end of bound.");
      }
      if (isc.filter != NULL)
      {
        NdbScanFilter filter(myIndexScanOp);
        apply_filter_top_level(&filter, isc.filter);
      }
      soft_assert(myIndexScanOp->setAggregationCode(&aggregator) >= 0,
                  "Failed to set aggregation code.");
      soft_assert(myIndexScanOp->DoAggregation() >= 0,
                  "Failed to execute aggregation.");
    }

    // Print results
    m_resultprinter->print_result(&aggregator, m_conf.out_stream);

    ndb->closeTransaction(myTrans);
  }
  catch (const std::exception& e)
  {
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
      throw;
    }
    std::basic_ostream<char>& err = *m_conf.err_stream;
    switch (ndb_err.status)
    {
    case NdbError::Status::Success:
      assert(ndb_err.code == 0);
      // Rethrow since error not from ndb
      throw;
    case NdbError::Status::TemporaryError:
      err << "NDB Temporary error: " << ndb_err.code << " " << ndb_err.message
          << endl
          << "Caught exception, probably caused by the temporary error above: "
          << e.what() << endl;
      throw TemporaryError();
    case NdbError::Status::PermanentError:
      err << "NDB Permanent error " << ndb_err.code << ": " << ndb_err.message
          << endl;
      // Now that the ndb error is described on err stream, we'll rethrow the
      // original exception.
      throw;
    case NdbError::Status::UnknownResult:
      err << "NDB Unknown result: " << ndb_err.code << ": " << ndb_err.message
          << endl;
      // Now that the ndb error is described on err stream, we'll rethrow the
      // original exception.
      throw;
    }
    // Unreachable
    abort();
  }
  catch (...)
  {
    // All exceptions thrown should be instances of runtime_error.
    abort();
  }
}

void
RonSQLPreparer::apply_filter_top_level(NdbScanFilter* filter,
                                       struct ConditionalExpression* ce)
{
  assert(ce != NULL);
  /* ndbapi filter has unary AND and OR operators, i.e. they take an arbitrary
   * number of operands. In a number of places, it is required to have at least
   * one "group", i.e. containing AND or OR expression, active. Unless the
   * top-level expression is an AND or OR operation, we wrap it in an AND group
   * with a single argument.
   */
  bool success = false;
  if (ce->op == T_AND || ce->op == T_OR)
  {
    success = apply_filter(filter, ce);
  }
  else
  {
    success = (filter->begin(NdbScanFilter::AND) >= 0 &&
               apply_filter(filter, ce) &&
               filter->end() >= 0);
  }
  if (!success)
  {
    throw runtime_error("Failed to apply filter.");
  }
}

bool
RonSQLPreparer::apply_filter(NdbScanFilter* filter,
                             struct ConditionalExpression* ce)
{
  assert (ce != NULL);
  switch (ce->op)
  {
  case T_IDENTIFIER:
    feature_not_implemented("Column name in WHERE clause outside of comparison");
  case T_STRING:
    feature_not_implemented("String constant in WHERE clause");
  case T_INT:
    feature_not_implemented("Integer constant in WHERE clause outside of comparison");
  case T_OR:
    return (filter->begin(NdbScanFilter::OR) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            apply_filter(filter, ce->args.right) &&
            filter->end() >= 0);
  case T_XOR:
    feature_not_implemented("XOR in WHERE clause");
  case T_AND:
    return (filter->begin(NdbScanFilter::AND) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            apply_filter(filter, ce->args.right) &&
            filter->end() >= 0);
  case T_NOT:
    feature_not_implemented("NOT in WHERE clause");
    /*
    return (apply_filter(filter, ce->args.left) &&
            // todo no idea if this is correct
            filter->isfalse() >= 0);
    */
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
  case T_IS:
    feature_not_implemented("IS in WHERE clause");
  case T_BITWISE_OR:
    feature_not_implemented("| in WHERE clause");
  case T_BITWISE_AND:
    feature_not_implemented("&& in WHERE clause");
  case T_BITSHIFT_LEFT:
    feature_not_implemented("<< in WHERE clause");
  case T_BITSHIFT_RIGHT:
    feature_not_implemented(">> in WHERE clause");
  case T_PLUS:
    feature_not_implemented("+ (addition) in WHERE clause");
  case T_MINUS:
    feature_not_implemented("- (subtraction) in WHERE clause");
  case T_MULTIPLY:
    feature_not_implemented("* in WHERE clause");
  case T_SLASH:
    feature_not_implemented("/ in WHERE clause");
  case T_DIV:
    feature_not_implemented("DIV in WHERE clause");
  case T_MODULO:
    feature_not_implemented("% in WHERE clause");
  case T_BITWISE_XOR:
    feature_not_implemented("^ in WHERE clause");
  case T_EXCLAMATION:
    feature_not_implemented("! in WHERE clause");
  case T_INTERVAL:
    feature_not_implemented("INTERVAL in WHERE clause");
  case T_DATE_ADD:
    feature_not_implemented("DATE_ADD in WHERE clause");
  case T_DATE_SUB:
    feature_not_implemented("DATE_SUB in WHERE clause");
  case T_EXTRACT:
    feature_not_implemented("EXTRACT in WHERE clause");
  default:
    // Unknown operator
    abort();
  }
}

bool
RonSQLPreparer::apply_filter_cmp(NdbScanFilter* filter,
                                   NdbScanFilter::BinaryCondition cond,
                                   struct ConditionalExpression* left,
                                   struct ConditionalExpression* right)
{
  if (left->op == T_IDENTIFIER &&
           right->op == T_IDENTIFIER)
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
  if (left->op == T_IDENTIFIER)
  {
    assert(m_column_attrId_map != NULL);
    raw_value rv = eval_const_expr(right);
    return (filter->begin(NdbScanFilter::AND) >= 0 &&
            filter->isnotnull(m_column_attrId_map[left->col_idx]) >=0 &&
            filter->cmp(cond,
                        m_column_attrId_map[left->col_idx],
                        rv.val, rv.len) >= 0 &&
            filter->end() >= 0);
  }
  throw runtime_error("Failed to apply filter.");
}

bool rv_bool(raw_value rv)
{
  for (Uint32 i = 0; i < rv.len; i++)
    if (((const Uint8*)rv.val)[i] != 0)
      return true;
  return false;
}

raw_value bool_rv(bool b)
{
  static const struct raw_value rv_true8 = {(const void *)"\001", 1};
  static const struct raw_value rv_false8 = {(const void *)"\000", 1};
  return b ? rv_true8 : rv_false8;
}

// todo, perhaps eval_const_expr is better made part of a filter simplification
// stage, where data types can be matched etc. Or maybe it should even be made
// part of apply_filter.
raw_value
RonSQLPreparer::eval_const_expr(ConditionalExpression* ce)
{
  raw_value ret;
  switch(ce->op)
  {
  case T_IDENTIFIER:
    throw runtime_error("Expected constant expression"); // todo track code location
  case T_STRING:
    feature_not_implemented("String constants in WHERE clause");
  case T_INT:
    {
      Int64* val = m_amalloc->alloc_exc<Int64>(1);
      *val = ce->constant_integer;
      ret.val = val;
      ret.len = sizeof(*val);
      return ret;
    }
  case T_OR:
    return bool_rv(rv_bool(eval_const_expr(ce->args.left)) ||
                   rv_bool(eval_const_expr(ce->args.right)));
  case T_XOR:
    {
      bool left = rv_bool(eval_const_expr(ce->args.left));
      bool right = rv_bool(eval_const_expr(ce->args.right));
      return bool_rv(left != right);
    }
  case T_AND:
    return bool_rv(rv_bool(eval_const_expr(ce->args.left)) &&
                   rv_bool(eval_const_expr(ce->args.right)));
  case T_NOT:
    return bool_rv(!rv_bool(eval_const_expr(ce->args.left)));
  case T_EQUALS:
    feature_not_implemented("Constant evaluation of equals (=) expression");
  case T_GE:
    feature_not_implemented("Constant evaluation of greater or equal (>=) expression");
  case T_GT:
    feature_not_implemented("Constant evaluation of greater than (>) expression");
  case T_LE:
    feature_not_implemented("Constant evaluation of less or equal (<=) expression");
  case T_LT:
    feature_not_implemented("Constant evaluation of less than (<) expression");
  case T_NOT_EQUALS:
    feature_not_implemented("Constant evaluation of not equals (<> or !=) expression");
  case T_IS:
    feature_not_implemented("Constant evaluation of IS expression");
  case T_BITWISE_OR:
    feature_not_implemented("Constant evaluation of bitwise or (|) expression");
  case T_BITWISE_AND:
    feature_not_implemented("Constant evaluation of bitwise and (&) expression");
  case T_BITSHIFT_LEFT:
    feature_not_implemented("Constant evaluation of bitshift left (<<) expression");
  case T_BITSHIFT_RIGHT:
    feature_not_implemented("Constant evaluation of bitshift right (>>) expression");
  case T_PLUS:
    feature_not_implemented("Constant evaluation of addition (+) expression");
  case T_MINUS:
    feature_not_implemented("Constant evaluation of subtraction (-) expression");
  case T_MULTIPLY:
    feature_not_implemented("Constant evaluation of multiplication (*) expression");
  case T_SLASH:
    feature_not_implemented("Constant evaluation of division (/) expression");
  case T_DIV:
    feature_not_implemented("Constant evaluation of DIV expression");
  case T_MODULO:
    feature_not_implemented("Constant evaluation of modulo (%) expression");
  case T_BITWISE_XOR:
    feature_not_implemented("Constant evaluation of bitwise xor (^) expression");
  case T_EXCLAMATION:
    feature_not_implemented("Constant evaluation of negation (!) expression");
  case T_INTERVAL:
    feature_not_implemented("Constant evaluation of INTERVAL expression");
  case T_DATE_ADD:
    feature_not_implemented("Constant evaluation of DATE_ADD expression");
  case T_DATE_SUB:
    {
      ConditionalExpression* date_ce = ce->args.left;
      ConditionalExpression* interval_ce = ce->args.right;
      if (date_ce->op != T_STRING)
      {
        throw runtime_error("DATE_SUB expected date string as first argument");
      }
      if (interval_ce->op != T_INTERVAL)
      {
        throw runtime_error("DATE_SUB expected interval as second argument");
      }
      LexString datestr = date_ce->string;
      if (interval_ce->interval.interval_type != T_DAY)
      {
        // todo support all kinds of intervals.
        throw runtime_error("DATE_SUB only supports DAY intervals");
      }
      Int64 days;
      ConditionalExpression* days_ce = interval_ce->interval.arg;
      if (days_ce->op == T_INT)
      {
        days = days_ce->constant_integer;
      }
      else if (days_ce->op == T_STRING)
      {
        LexString ls = days_ce->string;
        static const size_t maxchars = 15;
        if (ls.len > maxchars)
        {
          throw runtime_error("Too many characters in interval string");
        }
        char buf[maxchars+1];
        memcpy(buf, ls.str, ls.len);
        buf[ls.len] = '\0';
        for (Uint32 i = 0; i < ls.len; i++)
        {
          if (buf[i] < '0' || buf[i] > '9')
          {
            throw runtime_error("Non-digit character in interval string");
          }
        }
        days = atol(buf); // todo or atoll?
      }
      else
      {
        throw runtime_error("DATE_SUB only supports constant integers and strings for specifying days");
      }
      if (days < 0)
      {
        // todo support negative intervals.
        throw runtime_error("DATE_SUB only supports positive days in interval");
      }
      unsigned long int udays = days; // datatype matches Interval.day
      MYSQL_TIME ltime;
      my_time_flags_t flags = 0; // todo
      MYSQL_TIME_STATUS status;
      bool err = str_to_datetime(datestr.str, datestr.len, &ltime,
                                 flags, &status);
      if (err)
      {
        throw runtime_error("DATE_SUB failed to parse date string");
      }
      bool neg = true;
      Interval interval = {0, 0, udays, 0, 0, 0, 0, neg};
      err = date_add_interval(&ltime,
                              interval_type::INTERVAL_DAY,
                              interval,
                              NULL);
      if (err)
      {
        throw runtime_error("DATE_SUB failed");
      }
      Uint32* date = m_amalloc->alloc_exc<Uint32>(1);
      // todo find and use MySQL conversion function
      *date = ltime.year << 9 | ltime.month << 5 | ltime.day;
      //ulonglong* timeull = m_amalloc->alloc_exc<ulonglong>(1);
      //*timeull = TIME_to_ulonglong_datetime(ltime);
      //std::cerr << "==========DBG: In RonSQLPreparer::eval_const_expr DATE_SUB: *timeull=" << *timeull << endl;
      //return raw_value{ timeull, sizeof(*timeull)};
      return raw_value{ date, sizeof(*date)};
    }
  case T_EXTRACT:
    feature_not_implemented("Constant evaluation of EXTRACT expression");
  default:
    // Unknown operator
    abort();
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
      assert(m_column_attrId_map != NULL);
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
  if (m_conf.ndb == NULL)
  {
    if (m_index_scan_config_candidates.size() == 0)
    {
      out << "Execute as table scan.\n"
          << "(There is no NDB connection,"
          << " but this can be determined based on the"
          << (m_context.ast_root.where_expression == NULL ? " absense of a" : "")
          << " WHERE clause.)\n";
    }
    else
    {
      out << "Cannot determine whether this would be a table or index scan.\n";
      if (m_index_scan_config_candidates.size() == 1)
      {
        out << "(There is 1 candidate plan for index scan, but without an NDB"
               " connection it cannot be determined whether it is"
               " applicable.)\n";
      }
      else
      {
        out << "(There are " << m_index_scan_config_candidates.size()
            << " candidate plans for index scan, but without an NDB connection"
               " it cannot be determined whether any of them are"
               " applicable.)\n";
      }
    }
  }
  else if (m_do_table_scan)
  {
    assert(!m_do_index_scan &&
           m_index_scan_config == NULL &&
           m_index_scan_index == NULL);
    out << "Execute as table scan "
        << (m_table_scan_filter == NULL
            ? "without any filter.\n"
            : "with a filter.\n");
    if (m_index_scan_config_candidates.size() == 0)
    {
      out << "(No candidate plans for index scan.)\n";
    }
    else
    {
      if (m_index_scan_config_candidates.size() == 1)
      {
        out << "(There is 1 candidate plan for index scan, but it could not be"
               " applied.)\n";
      }
      else
      {
        out << "(There are " << m_index_scan_config_candidates.size()
            << " candidate plans for index scan, none of which could be"
               " applied.)\n";
      }
    }
  }
  else
  {
    assert(m_do_index_scan &&
           m_index_scan_config != NULL &&
           m_index_scan_index != NULL &&
           !m_do_table_scan &&
           m_table_scan_filter == NULL);
    LexCString col_name = m_columns[m_index_scan_config->col_idx];
    out << "Execute as index scan.\n"
        << "Indexed column: "
        << quoted_identifier(col_name) << '\n'
        << "Index name: " << quoted_identifier(m_index_scan_index->getName())
        << '\n'
        << "Ranges:\n";
    for (Uint32 i = 0; i < m_index_scan_config->range_count; i++)
    {
      out << "- ";
      print(m_index_scan_config->ranges[i], col_name.c_str());
      out << '\n';
    }
    out << "Filter:\n";
    print(m_index_scan_config->filter, LexString{NULL, 0});
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

void
RonSQLPreparer::print(struct IndexScanConfig::Range& range, const char* col_name)
{
  std::basic_ostream<char>& out = *m_conf.out_stream;
  if (range.ltype == IndexScanConfig::Range::Type::INCLUSIVE &&
      range.htype == IndexScanConfig::Range::Type::INCLUSIVE &&
      range.lvalue == range.hvalue)
  {
    // Equality
    out << col_name << " = " << range.lvalue;
    return;
  }
  const char* lboundt;
  const char* hboundt;
  switch(range.ltype)
  {
  case IndexScanConfig::Range::Type::NONE:
    lboundt = NULL;
    break;
  case IndexScanConfig::Range::Type::INCLUSIVE:
    lboundt = " <= ";
    break;
  case IndexScanConfig::Range::Type::EXCLUSIVE:
    lboundt = " < ";
    break;
  default:
    abort();
  }
  switch(range.htype)
  {
  case IndexScanConfig::Range::Type::NONE:
    hboundt = NULL;
    break;
  case IndexScanConfig::Range::Type::EXCLUSIVE:
    hboundt = " < ";
    break;
  case IndexScanConfig::Range::Type::INCLUSIVE:
    hboundt = " <= ";
    break;
  default:
    abort();
  }
  assert(lboundt != NULL ||
         hboundt != NULL);
  if (lboundt != NULL && hboundt != NULL)
  {
    assert(range.lvalue < range.hvalue);
  }
  if (lboundt != NULL)
  {
    out << range.lvalue << lboundt;
  }
  out << col_name;
  if (hboundt != NULL)
  {
    out << hboundt << range.hvalue;
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
