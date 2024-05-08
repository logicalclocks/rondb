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

#define not_implemented() not_implemented_helper(__FILE__, __LINE__)
#define not_implemented_helper(file, line) \
  throw runtime_error(file ":" #line ": Not implemented")

static const char* interval_type_name(int interval_type);

DEFINE_FORMATTER(quoted_identifier, LexString, {
  os.put('`');
  for (uint i = 0; i < value.len; i++)
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

RonSQLPreparer::RonSQLPreparer(ExecutionParameters conf):
  m_conf(conf),
  m_aalloc(conf.aalloc),
  m_context(*this),
  m_columns(conf.aalloc),
  m_index_scan_config_candidates(conf.aalloc)
{
  assert(m_status == Status::BEGIN);
  try
  {
    configure();
    parse();
    load();
    compile();
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
  assert(m_conf.aalloc != NULL);
  ExecutionParameters::ExecutionMode mode = m_conf.mode;
  bool may_query =
    (mode == ExecutionParameters::ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN ||
     mode == ExecutionParameters::ExecutionMode::ALLOW_QUERY_ONLY ||
     mode == ExecutionParameters::ExecutionMode::QUERY_OVERRIDE);
  bool may_explain =
    (mode == ExecutionParameters::ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN ||
     mode == ExecutionParameters::ExecutionMode::ALLOW_EXPLAIN_ONLY ||
     mode == ExecutionParameters::ExecutionMode::EXPLAIN_OVERRIDE);
  assert(may_query || may_explain);
  if (may_query)
  {
    assert(m_conf.ndb != NULL);
    assert(m_conf.query_output_stream != NULL);
    assert(m_conf.query_output_format == ExecutionParameters::QueryOutputFormat::JSON_UTF8 ||
           m_conf.query_output_format == ExecutionParameters::QueryOutputFormat::JSON_ASCII ||
           m_conf.query_output_format == ExecutionParameters::QueryOutputFormat::TSV ||
           m_conf.query_output_format == ExecutionParameters::QueryOutputFormat::TSV_DATA);
  }
  if (may_explain)
  {
    assert(m_conf.explain_output_stream != NULL);
    assert(m_conf.explain_output_format == ExecutionParameters::ExplainOutputFormat::TEXT ||
           m_conf.explain_output_format == ExecutionParameters::ExplainOutputFormat::JSON_UTF8);
  }
  assert(m_conf.err_output_stream != NULL);

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
  uint our_buffer_len = sql_len - 2;
  m_sql = { static_cast<const char*>(sql_buffer), our_buffer_len };
}

void
RonSQLPreparer::parse()
{
  std::basic_ostream<char>& err = *m_conf.err_output_stream;
  int parse_result = rsqlp_parse(m_scanner);
  if (parse_result == 0)
  {
    assert(m_context.m_err_state == ErrState::NONE);
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
     *    our ArenaAllocator does not return NULL but rather throws an exception
     *    on OOM, this case does not apply to us.
     * Therefore, we know that if we end up here, we are in case 2).
     */
    throw runtime_error("Parser stack exceeded its maximum depth.");
  }
  assert(parse_result == 1);
  assert(m_context.m_err_state != ErrState::NONE);
  assert(m_sql.str <= m_context.m_err_pos);
  uint err_pos = m_context.m_err_pos - m_sql.str;
  uint err_stop = err_pos + m_context.m_err_len;
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
    uint line_started_at = 0;
    for (uint pos = 0; pos <= m_sql.len; pos++)
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
        uint err_marker_pos = line_started_at;
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
RonSQLPreparer::has_width(uint pos)
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
  /* The parser has already provided columns and expressions to the
   * AggregationAPICompiler. E.g. in `SELECT Max(col1 + col2)`, m_agg already
   * knows about `col1`, `col2` and `col1 + col2`. Here, we let m_agg know about
   * the aggregate expressions themselves, e.g. `Max(col1 + col2)`, making sure
   * they are provided in the correct order.
   * todo maybe this part is better placed in parse()
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
      int fun = outputs->aggregate.fun;
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
    assert(m_conf.err_output_stream != NULL);
    std::basic_ostream<char>& err = *m_conf.err_output_stream;
    err << "This query has no aggregate expression, so it is not an aggregate query.\n"
           "Currently, RonSQL only supports aggregate queries.\n";
    throw runtime_error("Not an aggregate query.");
  }

  std::basic_ostream<char>& err = *m_conf.err_output_stream;
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
    // todo Explain that only ENGINE=NDB tables are supported
    soft_assert(m_table != NULL,
                "Failed to get table. Note that RonSQL only supports tables with"
                " ENGINE=ndbcluster.");
    int32_t* col_id_map = m_aalloc->alloc<int32_t>(m_columns.size());
    for (uint col_idx = 0; col_idx < m_columns.size(); col_idx++)
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
    uint col_idx = ce->args.left->args.left->args.left->col_idx;
    long low_bound = ce->args.left->args.left->args.right->constant_integer;
    long high_bound = ce->args.left->args.right->args.right->constant_integer;
    IndexScanConfig isc;
    isc.col_idx = col_idx;
    isc.ranges = m_aalloc->alloc<IndexScanConfig::Range>(1);
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
  for(uint i = 0; i < index_list.count; i++)
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
          for (uint j = 0; j < m_index_scan_config_candidates.size(); j++)
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
  m_resultprinter = new (m_aalloc->alloc<ResultPrinter>(1))
    ResultPrinter(m_aalloc,
                  &m_context.ast_root,
                  &m_columns,
                  m_conf.query_output_format,
                  m_conf.err_output_stream);
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
    bool do_explain = m_context.ast_root.do_explain;
    switch (m_conf.mode)
    {
    case ExecutionParameters::ExecutionMode::ALLOW_BOTH_QUERY_AND_EXPLAIN:
      break;
    case ExecutionParameters::ExecutionMode::ALLOW_QUERY_ONLY:
      soft_assert(!do_explain, "Execution mode does not allow EXPLAIN.");
      break;
    case ExecutionParameters::ExecutionMode::ALLOW_EXPLAIN_ONLY:
      soft_assert(do_explain, "Execution mode does not allow query, only EXPLAIN.");
      break;
    case ExecutionParameters::ExecutionMode::QUERY_OVERRIDE:
      do_explain = false;
      break;
    case ExecutionParameters::ExecutionMode::EXPLAIN_OVERRIDE:
      do_explain = true;
      break;
    default:
      throw runtime_error("Invalid execution mode.");
    }
    if (do_explain)
    {
      switch (m_conf.explain_output_format)
      {
      case ExecutionParameters::ExplainOutputFormat::TEXT:
        print();
        break;
      case ExecutionParameters::ExplainOutputFormat::JSON_UTF8:
        not_implemented();
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
      for (uint i = 0; i < isc.range_count; i++)
      {
        const IndexScanConfig::Range& range = isc.ranges[i];
        if (range.ltype == IndexScanConfig::Range::Type::INCLUSIVE &&
            range.htype == IndexScanConfig::Range::Type::INCLUSIVE &&
            range.lvalue == range.hvalue)
        {
          // Equality
          soft_assert(myIndexScanOp->setBound(col_name,
                                              NdbIndexScanOperation::BoundEQ,
                                              &range.lvalue) == 0,
                      "Failed to set lower bound.");
          soft_assert(myIndexScanOp->end_of_bound(i) == 0,
                      "Failed to set end of bound.");
          continue;
        }
        static const int BoundNone = -1;
        static_assert(NdbIndexScanOperation::BoundEQ != BoundNone);
        static_assert(NdbIndexScanOperation::BoundLE != BoundNone);
        static_assert(NdbIndexScanOperation::BoundLT != BoundNone);
        static_assert(NdbIndexScanOperation::BoundGT != BoundNone);
        static_assert(NdbIndexScanOperation::BoundGE != BoundNone);
        int lboundt;
        int hboundt;
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
    m_resultprinter->print_result(&aggregator, m_conf.query_output_stream);

    ndb->closeTransaction(myTrans);
  }
  catch (const std::exception& e)
  {
    NdbError ndb_err = ndb->getNdbError();
    std::basic_ostream<char>& err = *m_conf.err_output_stream;
    if (myTrans != NULL)
    {
      ndb->closeTransaction(myTrans);
    }
    switch (ndb_err.status)
    {
    case NdbError::Status::Success:
      assert(ndb_err.code == 0); // todo improve error handling if this fails
      // Rethrow since error not from ndb
      throw;
    case NdbError::Status::TemporaryError:
      err << "NDB temporary error: " << ndb_err.code << " " << ndb_err.message
          << endl;
      err << "Caught exception, probably caused by the temporary error above: "
          << e.what() << endl;
      throw TemporaryError();
    case NdbError::Status::PermanentError:
      err << "NDB permanent error: " << ndb_err.code << " " << ndb_err.message
          << endl;
      // Now that the ndb error is described on err stream, we'll rethrow the
      // original exception.
      throw;
    case NdbError::Status::UnknownResult:
      err << "NDB unknown result: " << ndb_err.code << " " << ndb_err.message
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
    not_implemented();
  case T_STRING:
    not_implemented();
  case T_INT:
    not_implemented();
  case T_OR:
    return (filter->begin(NdbScanFilter::OR) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            apply_filter(filter, ce->args.right) &&
            filter->end() >= 0);
  case T_XOR:
    not_implemented();
  case T_AND:
    return (filter->begin(NdbScanFilter::AND) >= 0 &&
            apply_filter(filter, ce->args.left) &&
            apply_filter(filter, ce->args.right) &&
            filter->end() >= 0);
  case T_NOT:
    return (apply_filter(filter, ce->args.left) &&
            // todo no idea if this is correct
            filter->isfalse() >= 0);
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
    not_implemented();
  case T_BITWISE_OR:
    not_implemented();
  case T_BITWISE_AND:
    not_implemented();
  case T_BITSHIFT_LEFT:
    not_implemented();
  case T_BITSHIFT_RIGHT:
    not_implemented();
  case T_PLUS:
    not_implemented();
  case T_MINUS:
    not_implemented();
  case T_MULTIPLY:
    not_implemented();
  case T_DIVIDE:
    not_implemented();
  case T_MODULO:
    not_implemented();
  case T_BITWISE_XOR:
    not_implemented();
  case T_EXCLAMATION:
    not_implemented();
  case T_INTERVAL:
    not_implemented();
  case T_DATE_ADD:
    not_implemented();
  case T_DATE_SUB:
    not_implemented();
  case T_EXTRACT:
    not_implemented();
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
    return (filter->cmp(cond,
                        m_column_attrId_map[left->col_idx],
                        m_column_attrId_map[right->col_idx]) >= 0);
  }
  if (left->op == T_IDENTIFIER)
  {
    assert(m_column_attrId_map != NULL);
    raw_value rv = eval_const_expr(right);
    return (filter->cmp(cond,
                        m_column_attrId_map[left->col_idx],
                        rv.val, rv.len) >= 0);
  }
  throw runtime_error("Failed to apply filter.");
}

bool rv_bool(raw_value rv)
{
  for (uint i = 0; i < rv.len; i++)
    if (((const uint8_t*)rv.val)[i] != 0)
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
    not_implemented();
  case T_INT:
    {
      long int* val = m_aalloc->alloc<long int>(1);
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
    not_implemented();
  case T_GE:
    not_implemented();
  case T_GT:
    not_implemented();
  case T_LE:
    not_implemented();
  case T_LT:
    not_implemented();
  case T_NOT_EQUALS:
    not_implemented();
  case T_IS:
    not_implemented();
  case T_BITWISE_OR:
    not_implemented();
  case T_BITWISE_AND:
    not_implemented();
  case T_BITSHIFT_LEFT:
    not_implemented();
  case T_BITSHIFT_RIGHT:
    not_implemented();
  case T_PLUS:
    not_implemented();
  case T_MINUS:
    not_implemented();
  case T_MULTIPLY:
    not_implemented();
  case T_DIVIDE:
    not_implemented();
  case T_MODULO:
    not_implemented();
  case T_BITWISE_XOR:
    not_implemented();
  case T_EXCLAMATION:
    not_implemented();
  case T_INTERVAL:
    not_implemented();
  case T_DATE_ADD:
    not_implemented();
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
        throw runtime_error("DATE_SUB only supports DAY intervals");
      }
      long int days;
      ConditionalExpression* days_ce = interval_ce->interval.arg;
      if (days_ce->op == T_INT)
      {
        days = days_ce->constant_integer;
      }
      else if (days_ce->op == T_STRING)
      {
        LexString ls = days_ce->string;
        static const int maxchars = 15;
        if (ls.len > maxchars)
        {
          throw runtime_error("Too many characters in interval string");
        }
        char buf[maxchars+1];
        memcpy(buf, ls.str, ls.len);
        buf[ls.len] = '\0';
        for (unsigned int i = 0; i < ls.len; i++)
        {
          if (buf[i] < '0' || buf[i] > '9')
          {
            throw runtime_error("Non-digit character in interval string");
          }
        }
        days = atol(buf);
      }
      else
      {
        throw runtime_error("DATE_SUB only supports constant integers and strings for specifying days");
      }
      if (days < 0)
      {
        throw runtime_error("DATE_SUB only supports positive days in interval");
      }
      unsigned long int udays = days;
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
      Uint32* date = m_aalloc->alloc<Uint32>(1);
      // todo find and use MySQL conversion function
      *date = ltime.year << 9 | ltime.month << 5 | ltime.day;
      //ulonglong* timeull = m_aalloc->alloc<ulonglong>(1);
      //*timeull = TIME_to_ulonglong_datetime(ltime);
      //std::cerr << "==========DBG: In RonSQLPreparer::eval_const_expr DATE_SUB: *timeull=" << *timeull << endl;
      //return raw_value{ timeull, sizeof(*timeull)};
      return raw_value{ date, sizeof(*date)};
    }
  case T_EXTRACT:
    not_implemented();
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
  std::basic_ostream<char>& err = *m_conf.err_output_stream;
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
  for (uint i=0; i<program.size(); i++)
  {
    AggregationAPICompiler::Instr* instr = &program[i];
    uint dest = instr->dest;
    uint src = instr->src;
    switch (instr->type)
    {
    case AggregationAPICompiler::SVMInstrType::Load:
    {
      assert(m_column_attrId_map != NULL);
      int32_t col_id = m_column_attrId_map[src];
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
        (aggregator->LoadInt64(m_agg->m_constants[src].long_int, dest));
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
  std::basic_ostream<char>& out = *m_conf.explain_output_stream;

  // Print query parse tree
  SelectStatement& ast_root = m_context.ast_root;
  out << "Query parse tree:\n"
      << "SELECT\n";
  Outputs* outputs = ast_root.outputs;
  int out_count = 0;
  while (outputs != NULL)
  {
    out << "  Out_" << out_count << ":"
        << quoted_identifier(outputs->output_name)
        << "\n   = ";
    switch (outputs->type)
    {
    case Outputs::Type::AGGREGATE:
      {
        int pr;
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
        int pr;
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
        int col_idx = outputs->column.col_idx;
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
      uint col_idx = groupby->col_idx;
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
      uint col_idx = orderby->col_idx;
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
           m_index_scan_index == NULL &&
           m_table_scan_filter != NULL);
    out << "Execute as table scan.\n";
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
    for (uint i = 0; i < m_index_scan_config->range_count; i++)
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
  assert(m_conf.explain_output_stream != NULL);
  m_resultprinter->explain(m_conf.explain_output_stream);

  out << '\n';
}

void
RonSQLPreparer::print(struct ConditionalExpression* ce,
                        LexString prefix)
{
  std::basic_ostream<char>& out = *m_conf.explain_output_stream;
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
      for (uint i = 0; i < ce->string.len; i++)
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
      LexString prefix_arg = prefix.concat(LexString{"|  ", 3}, m_aalloc);
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
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_aalloc);
      print(ce->args.right, prefix_arg);
      return;
    }
    opstr = "-";
    break;
  case T_MULTIPLY:
    opstr = "*";
    break;
  case T_DIVIDE:
    opstr = "/";
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
      LexString prefix_arg = prefix.concat(LexString{"|  ", 3}, m_aalloc);
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
      LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_aalloc);
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
    LexString prefix_arg = prefix.concat(LexString{"   ", 3}, m_aalloc);
    print(ce->args.left, prefix_arg);
  }
  else
  {
    out << opstr << '\n'
        << prefix << "+- ";
    LexString prefix_left = prefix.concat(LexString{"|  ", 3}, m_aalloc);
    print(ce->args.left, prefix_left);
    out << prefix << "\\- ";
    LexString prefix_right = prefix.concat(LexString{"   ", 3}, m_aalloc);
    print(ce->args.right, prefix_right);
  }
}

static const char* interval_type_name(int interval_type)
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
  std::basic_ostream<char>& out = *m_conf.explain_output_stream;
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

uint
RonSQLPreparer::Context::column_name_to_idx(LexCString col_name)
{
  DynamicArray<LexCString>& columns = m_parser.m_columns;
  uint sz = columns.size();
  for (uint i=0; i < sz; i++)
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
RonSQLPreparer::column_idx_to_name(uint col_idx)
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
                                  uint err_len)
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
    [_this](uint idx) -> const char*
    {
      return _this->column_idx_to_name(idx).c_str();
    };

  /*
   * The aggregator uses the same arena allocator as the RonSQLPreparer object
   * because they are both working in the prepare phase. After loading and
   * compilation, a new object will be crafted that holds the information
   * necessary for execution and post-processing.
   */
  m_parser.m_agg = new (get_allocator()->alloc<AggregationAPICompiler>(1))
    AggregationAPICompiler(column_idx_to_name,
                           *m_parser.m_conf.explain_output_stream,
                           *m_parser.m_conf.err_output_stream,
                           m_parser.m_aalloc);
  return m_parser.m_agg;
}

ArenaAllocator*
RonSQLPreparer::Context::get_allocator()
{
  return m_parser.m_aalloc;
}
