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
#include "ArenaMalloc.hpp"
#include "DynamicArray.hpp"
#include "LexString.hpp"
#include "QueryPlanner.hpp"
#include "ResultPrinter.hpp"
#include "RonSQLCommon.hpp"

// Definitions from RonSQLLexer.l.hpp that are needed here. We can't include
// the whole file because it would create a circular dependency.
typedef void* yyscan_t;
typedef struct yy_buffer_state *YY_BUFFER_STATE;
struct yy_buffer_state;

// NdbQueryBuilder.hpp is an internal (src-side) header, not pulled in via
// <NdbApi.hpp>, so forward-declare the handful of types we reference from
// this header. Full definitions are included in RonSQLPreparer.cpp.
class NdbQueryBuilder;
class NdbQueryOperationDef;

struct LexLocation
{
  char* begin = NULL;
  char* end = NULL;
};

struct raw_value
{
  const void* val = NULL;
  size_t len = 0;
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
    LEX_LITERAL_FLOAT_INVALID,
    TOO_LONG_UNALIASED_OUTPUT,
    PARSER_ERROR,
  };
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
    int m_subquery_depth = 0;
    AggregationAPICompiler* m_inner_agg = NULL;
  public:
    Context(RonSQLPreparer& parser):
      m_parser(parser)
    {}
    void set_err_state(ErrState state, char* err_pos, size_t err_len);
    AggregationAPICompiler* get_agg();
    ArenaMalloc* get_allocator();
    Uint32 column_name_to_idx(LexCString);
    Uint32 qualified_column_name_to_idx(LexCString table, LexCString column);
    void enter_subquery();
    // Returns the AggregationAPICompiler that was active inside the
    // subquery/CTE body just exited (or NULL if none was created because
    // the body had no aggregate expressions). Callers save the pointer on
    // the corresponding SelectStatement before the next subquery begins.
    AggregationAPICompiler* leave_subquery();
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
  ArenaMalloc* m_amalloc;
  Context m_context;
  DynamicArray<LexCString> m_columns;
  DynamicArray<LexCString> m_column_qualifiers; /* table qualifier per col_idx */
  DynamicArray<bool> m_col_is_inner; /* true for columns from inner subqueries */
  DynamicArray<bool> m_col_is_alias; /* true for ORDER BY alias references */
  const NdbDictionary::Dictionary* m_dict = NULL;

  // Cross-table WHERE filters (e.g., WHERE l.price > o.min_price).
  // These reference columns from two different tables and cannot be
  // pushed as scan filters.  For aggregation queries, they are compiled
  // into BranchReg conditional aggregation instructions.
  struct CrossTableFilter {
    ConditionalExpression* ce;
    Uint32 child_table_idx;   // table index of the "inner" side
    Uint32 parent_table_idx;  // table index of the "outer" side
  };

  // QueryScope groups the per-join-plan state that the planner, filter
  // compiler and NdbQueryBuilder emit path all read. One instance per
  // query body — the outer SELECT uses m_main_scope; CTE bodies carry
  // their own scopes so they can be planned and emitted independently.
  struct QueryScope {
    JoinPlan join_plan;
    ConditionalExpression* join_where_ce[MAX_SPJ_TREE_NODES];
    DynamicArray<CrossTableFilter> cross_table_where_filters;
    NdbAttrId* column_attrId_map = NULL;
    const NdbDictionary::Column** column_map = NULL;
    Uint32* column_table_idx = NULL;
    const NdbDictionary::Table* table = NULL;
    AggregationAPICompiler* agg = NULL;

    QueryScope(ArenaMalloc* amalloc) : cross_table_where_filters(amalloc) {}
  };
  QueryScope m_main_scope;

  DynamicArray<const NdbDictionary::Index*> m_indexes;
  NdbTransaction* m_trans = NULL;
  yyscan_t m_scanner;
  YY_BUFFER_STATE m_buf;
  bool m_do_explain = false;

  // Index/table scan config
  class ScanConfig
  {
  public:
    // If index == NULL, do a table scan.
    const NdbDictionary::Index* index = NULL;
    // condition_handling_map[i] is -1 if m_toplevel_conditions[i] should be
    // included in the filter, or the column number in the index if it should be
    // applied as a bound.
    int* condition_handling_map = NULL;
    // An estimate of how performant the scan configuration will be.
    int goodness = 0;
  };
  DynamicArray<ConditionalExpression*> m_toplevel_conditions;
  DynamicArray<ScanConfig> m_scan_config_candidates;
  ScanConfig* m_scan_config = NULL;

  // SELECT-list subquery aggregation (multi-leaf pushdown)
  struct SelectSubqueryLeaf {
    SelectStatement* inner_stmt;       // parsed inner SELECT
    Outputs* output_node;              // the SUBQUERY_AGG output
    Uint32 output_idx;                 // position in output list
    LexCString inner_table_name;       // inner table name
    LexCString inner_table_alias;      // inner table alias
    LexCString inner_join_col;         // inner side of correlation
    LexCString outer_join_col;         // outer correlation column name
    LexCString outer_join_table;       // outer correlation table qualifier
    TokenKind agg_fun;                 // T_SUM, T_COUNT, T_MIN, T_MAX
    LexCString inner_agg_col;          // aggregated column name
    Uint32 inner_agg_col_idx;          // col_idx of aggregated column (in m_columns)
    Uint32 combined_agg_slot;          // slot in combined result
    Uint32 merged_leaf_idx;            // index into m_merged_leaves
    bool use_inner_join;               // true=INNER, false=LEFT OUTER
    bool is_count_star;                // COUNT(*) — no inner column needed
    ConditionalExpression *inner_filter;       // inner-only WHERE filter
    ConditionalExpression *cross_table_filter; // filter referencing outer columns
  };
  struct MergedLeaf {
    Uint32 first_subquery_idx;         // first SelectSubqueryLeaf in this group
    Uint32 num_aggs;                   // number of aggregates in this leaf
    Uint32 plan_op_idx;                // index in JoinPlan::ops after rewriting
  };
  DynamicArray<SelectSubqueryLeaf> m_select_subquery_leaves;
  DynamicArray<MergedLeaf> m_merged_leaves;
  bool m_has_select_subqueries = false;

  // Subquery orchestration
  struct SubqueryInfo {
    ConditionalExpression* ce_node;  // The SubqueryExpr node in outer AST
    SelectStatement* inner_stmt;     // For sql_begin/sql_end access
    SubqueryResult result;           // Populated during execution (scalar)
    bool is_in_subquery = false;     // true for I_IN_SUBQUERY
    ConditionalExpression* in_expr = NULL; // LHS expression (col IN (...))
    DynamicArray<SubqueryResult>* in_values = NULL; // Multi-value results
    bool is_corr_scalar = false;     // true for I_CORR_SCALAR
    DynamicArray<CorrelatedPair>* corr_values = NULL; // (key,val) pairs
  };
  DynamicArray<SubqueryInfo> m_subquery_infos;
  bool m_has_subqueries = false;
  bool m_has_ctes = false;

  // One QueryScope per CTE in ast_root.cte_list, in declaration order.
  // Pointers because QueryScope holds a DynamicArray — non-trivially-copyable.
  // Allocated from m_amalloc (arena), so no explicit delete is needed.
  DynamicArray<QueryScope*> m_cte_scopes;

  ResultPrinter* m_resultprinter = NULL;
  LexCString column_idx_to_name(uint);
  void (*m_print_json_string)(std::basic_ostream<char>& out, const char* str) = NULL;

  // Functions used in preparation phase
public:
  RonSQLPreparer(RonSQLExecParams conf);
private:
  void configure();
  void parse();
  void resolve_orderby_aliases();
  bool has_width(size_t pos);
  void load();
  void load_single_table();
  void load_join();
  void classify_where_by_table();
  void assign_cross_table_index_bounds();
  void plan_index_and_filter();
  void collect_toplevel_conditions(ConditionalExpression* ce);
  void generate_scan_config_candidates();
  void analyze_ctes();
  void build_cte_scopes();
  void resolve_columns_for_cte_scope(QueryScope& scope);
  void analyze_subqueries();
  void analyze_subqueries_ce(ConditionalExpression* ce);
  void analyze_select_subqueries();
  void merge_same_table_subqueries();
  void rewrite_select_subqueries_as_joins();
  void decorrelate_exists();
  void decorrelate_scalar();
  void compile();
  void build_agg_linked_projections();
  void determine_explain();
  bool unload_schema();
  void handle_ronsql_exception(std::exception_ptr eptr);

  // Functions used in execution phase
public:
  void execute(); // todo make sure we can execute several times, do not mutate. Make this a separate object that takes a preparer as const input (This todo from review 2024-08-22 with MR)
private:
  void cleanup_trans();
  void execute_subqueries();
  void substitute_subquery_results();
  void substitute_subquery_results_ce(ConditionalExpression** ce_ptr);
  void execute_join();
  void emit_root_op(NdbQueryBuilder* qb, QueryScope& scope,
                    const NdbQueryOperationDef** opDefs);
  void build_cte_virtual_tables(const JoinPlan& plan,
                                NdbDictionary::Table** out);
  void emit_child_ops(NdbQueryBuilder* qb, QueryScope& scope,
                      const NdbQueryOperationDef** opDefs,
                      NdbAggregator* singleAgg,
                      NdbAggregator** leafAggs,
                      NdbDictionary::Table** cteVirtualTables);
  void collect_pk_equalities(struct ConditionalExpression* ce,
                             const NdbDictionary::Table* table,
                             struct ConditionalExpression* pk_const[]);
  void apply_filter_top_level(NdbScanFilter* filter);
  void apply_filter(NdbScanFilter* filter, QueryScope& scope,
                    struct ConditionalExpression* ce);
  void apply_filter_cmp(NdbScanFilter* filter, QueryScope& scope,
                        NdbScanFilter::BinaryCondition cond,
                        struct ConditionalExpression* left,
                        struct ConditionalExpression* right);
  void apply_filter_like(NdbScanFilter* filter, QueryScope& scope,
                         NdbScanFilter::BinaryCondition cond,
                         struct ConditionalExpression* left,
                         struct ConditionalExpression* right);
  raw_value encode_constant(struct ConditionalExpression *ce,
                            const NdbDictionary::Column* col);
  struct ConditionalExpression* simplify_ce(struct ConditionalExpression* ce,
                                            int maxdepth);
  void programAggregator(NdbAggregator* aggregator);
  void programAggregator_join(NdbAggregator* aggregator);
  Uint32 filter_expr_word_count(struct ConditionalExpression* ce);
  void emit_filter_expr(NdbAggregator* agg, QueryScope& scope,
                        struct ConditionalExpression* ce,
                        Uint32 leaf_idx, Uint32 reg, Uint32 tmp_reg);
  void generate_embedded_condition(NdbAggregator* aggregator,
                                   struct ConditionalExpression* ce,
                                   Uint32 then_arm_raw_size);
  void print_result_json(NdbAggregator* aggregator);
  void print();
  void print(struct ConditionalExpression* ce,
             LexString prefix);

public:
  ~RonSQLPreparer();
};

#endif
