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

#ifndef STORAGE_NDB_SRC_RONSQL_RONSQLPREPARER_HPP
#define STORAGE_NDB_SRC_RONSQL_RONSQLPREPARER_HPP 1

#include <cstddef>
#include <cstdint>

#include <NdbApi.hpp>

#include "AggregationAPICompiler.hpp"
#include "ArenaAllocator.hpp"
#include "DynamicArray.hpp"
#include "LexString.hpp"
#include "ResultPrinter.hpp"
#include "RonSQLCommon.hpp"

// Definitions from RonSQLLexer.l.hpp that are needed here. We can't include
// the whole file because it would create a circular dependency.
typedef void* yyscan_t;
typedef struct yy_buffer_state *YY_BUFFER_STATE;
struct yy_buffer_state;

struct LexLocation
{
  char* begin = NULL;
  char* end = NULL;
};

struct raw_value
{
  const void* val = NULL;
  Uint32 len = 0;
};

/*
  NdbAPI is not consistent wrt to the datatype used for attrId. For example,
    int NdbDictionary::Column::getAttrId() const
  returns a signed attrId while
    inline const NdbColumnImpl *NdbTableImpl::getColumn(unsigned attrId) const
  requires an unsigned attrId. There are also
    int NdbScanFilter::cmp(BinaryCondition cond, int ColId, const void *val,
                           Uint32 len)
    int NdbScanFilter::isnotnull(int AttrId)
  that require a signed attrId and
    bool NdbAggregator::GroupBy(Int32 col_id)
    bool NdbAggregator::LoadColumn(Int32 col_id, Uint32 reg_id)
  that require Int32. Here we use a typedef to label, but not solve, this
  particular mess. This allows us to handle negative return values indicating
  failure.
 */
// todo Change to Uint32 and handle int return values by checking non-negative and then convert
typedef Int32 NdbAttrId;

class RonSQLPreparer
{
public:
  enum class ErrState
  {
    NONE,
    LEX_NUL,
    LEX_U_ILLEGAL_BYTE,
    LEX_U_ENC_ERR,
    LEX_U_OVERLONG,
    LEX_U_TOOHIGH,
    LEX_U_SURROGATE,
    LEX_NONBMP_IDENTIFIER,
    LEX_UNIMPLEMENTED_KEYWORD,
    LEX_TOO_LONG_IDENTIFIER,
    LEX_INCOMPLETE_ESCAPE_SEQUENCE_IN_SINGLE_QUOTED_STRING,
    LEX_UNEXPECTED_EOI_IN_SINGLE_QUOTED_STRING,
    LEX_ILLEGAL_TOKEN,
    LEX_UNEXPECTED_EOI_IN_QUOTED_IDENTIFIER,
    LEX_LITERAL_INTEGER_TOO_BIG,
    TOO_LONG_UNALIASED_OUTPUT,
    PARSER_ERROR,
  };
  class TemporaryError : public std::exception {};
  /*
   * The context class is used to expose parser internals to flex and bison code
   * without making them public.
   */
  class Context
  {
    friend class RonSQLPreparer;
  private:
    RonSQLPreparer& m_parser;
    ErrState m_err_state = ErrState::NONE;
    const char* m_err_pos = NULL;
    size_t m_err_len = 0;
  public:
    Context(RonSQLPreparer& parser):
      m_parser(parser)
    {}
    void set_err_state(ErrState state, char* err_pos, size_t err_len);
    AggregationAPICompiler* get_agg();
    ArenaAllocator* get_allocator();
    Uint32 column_name_to_idx(LexCString);
    SelectStatement ast_root;
  };
private:
  // m_conf is a value rather than a pointer to prevent the caller from altering
  // it during the lifetime of RonSQLPreparer.
  RonSQLExecParams m_conf;
  enum class Status
  {
    BEGIN,
    PREPARED,
    FAILED,
  };
  Status m_status = Status::BEGIN;
  LexString m_sql = {NULL, 0};
  ArenaAllocator* m_aalloc;
  Context m_context;
  DynamicArray<LexCString> m_columns;
  NdbAttrId* m_column_attrId_map = NULL;
  const NdbDictionary::Dictionary* m_dict = NULL;
  const NdbDictionary::Table* m_table = NULL;
  yyscan_t m_scanner;
  YY_BUFFER_STATE m_buf;
  bool m_do_explain = false;

  // Index scan
  bool m_do_index_scan = false;
  class IndexScanConfig
  {
  public:
    Uint32 col_idx;
    struct Range
    {
      enum class Type { NONE, INCLUSIVE, EXCLUSIVE };
      Type ltype;
      Int64 lvalue;
      Type htype;
      Int64 hvalue;
    };
    Range* ranges;
    Uint32 range_count;
    ConditionalExpression* filter;
  };
  DynamicArray<IndexScanConfig> m_index_scan_config_candidates;
  IndexScanConfig* m_index_scan_config = NULL;
  const NdbDictionary::Index* m_index_scan_index = NULL;

  // Table scan
  bool m_do_table_scan = false;
  ConditionalExpression* m_table_scan_filter = NULL;

  AggregationAPICompiler* m_agg = NULL;
  ResultPrinter* m_resultprinter = NULL;
  LexCString column_idx_to_name(uint);
  void (*m_print_json_string)(std::basic_ostream<char>& out, const char* str) = NULL;

  // Functions used in preparation phase
public:
  RonSQLPreparer(RonSQLExecParams conf);
private:
  void configure();
  void parse();
  bool has_width(size_t pos);
  void load();
  void generate_index_scan_config_candidates();
  void choose_index_scan_config();
  void compile();
  void determine_explain();

  // Functions used in execution phase
public:
  void execute(); // todo make sure we can execute several times, do not mutate. Make this a separate object that takes a preparer as const input (This todo from review 2024-08-22 with MR)
private:
  void apply_filter_top_level(NdbScanFilter* filter,
                              struct ConditionalExpression* ce);
  bool apply_filter(NdbScanFilter* filter, struct ConditionalExpression* ce);
  bool apply_filter_cmp(NdbScanFilter* filter,
                        NdbScanFilter::BinaryCondition cond,
                        struct ConditionalExpression* left,
                        struct ConditionalExpression* right);
  raw_value eval_const_expr(struct ConditionalExpression* ce);
  void programAggregator(NdbAggregator* aggregator);
  void print_result_json(NdbAggregator* aggregator);
  void print();
  void print(struct ConditionalExpression* ce,
             LexString prefix);
  void print(struct IndexScanConfig::Range& range, const char* col_name);

public:
  ~RonSQLPreparer();
};

#endif
