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
class NdbQueryOptions;

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

// Phase I.5 v2b: linked list of operands for an n-ary GREATEST / LEAST.
// Built bottom-up by the parser via mk_arg_list / append_arg_list.
struct ArithExprList
{
  AggregationAPICompiler::Expr* head;
  ArithExprList* next;
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
    INVALID_FRAGS_PER_WORKER,
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
    // Phase I.5 v2b — n-ary GREATEST / LEAST.  Operands are folded
    // left-associative into a chain of Greatest2 / Least2 SVM ops.
    // Each operand must be a Load (column ref) or LoadConstantInteger.
    // At least one column operand required.  Nullable column operands
    // rejected post-resolution via m_greatest_least_pair_loads.
    AggregationAPICompiler::Expr* lower_greatest_least_nary(
        struct ArithExprList* args,
        bool is_greatest);
    // Build a two-element list (the smallest the n-ary grammar
    // accepts).
    struct ArithExprList* mk_arg_list(
        AggregationAPICompiler::Expr* a,
        AggregationAPICompiler::Expr* b);
    // Append one operand at the end of an existing list, in source
    // order.  Returns the same list head.
    struct ArithExprList* append_arg_list(
        struct ArithExprList* list,
        AggregationAPICompiler::Expr* x);
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
    PARSED,
    PREPARED,
    FAILED,
  };
  Status m_status = Status::BEGIN;
  bool m_parse_only = false;
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
  // into embedded normal-interpreter predicates before aggregate updates.
  struct CrossTableFilter {
    ConditionalExpression* ce;
    Uint32 child_table_idx;   // table index of the "inner" side
    Uint32 parent_table_idx;  // table index of the "outer" side
  };

  // Forward decl so QueryScope can carry a chosen scan config pointer.
  // Phase I.9: CTE bodies need per-scope scan-config state; the existing
  // m_scan_config_candidates / m_scan_config / m_indexes fields belong
  // to the main query and would corrupt across CTEs if reused.
  class ScanConfig;

  // QueryScope groups the per-join-plan state that the planner, filter
  // compiler and NdbQueryBuilder emit path all read. One instance per
  // query body — the outer SELECT uses m_main_scope; CTE bodies carry
  // their own scopes so they can be planned and emitted independently.
  struct QueryScope {
    struct ResolvedColumnRef {
      enum class Kind : uint8_t {
        Unresolved = 0,
        StoredColumn,
        CteResultColumn,
        AliasOnly
      };

      Kind kind = Kind::Unresolved;
      Uint32 join_op_idx = 0;

      // StoredColumn
      NdbAttrId attr_id = -1;
      const NdbDictionary::Column* dict_column = NULL;

      // CteResultColumn
      Uint32 cte_def_idx = 0;
      Uint32 cte_result_idx = 0;
      const Outputs* cte_output = NULL;
    };

    enum class MinMaxKind : uint8_t {
      NONE = 0,
      MIN_ASC,
      MAX_DESC
    };

    JoinPlan join_plan;
    ConditionalExpression* join_where_ce[MAX_SPJ_TREE_NODES];
    DynamicArray<CrossTableFilter> cross_table_where_filters;

    ResolvedColumnRef* resolved_columns = NULL;
    const NdbDictionary::Table* table = NULL;
    AggregationAPICompiler* agg = NULL;

    // Phase I.9: per-scope scan-config state.  `body_indexes`,
    // `body_toplevel_conditions`, and `body_scan_config_candidates`
    // mirror the single-table members `m_indexes`,
    // `m_toplevel_conditions`, and `m_scan_config_candidates`, but
    // scoped per query body so multi-CTE queries don't trample each
    // other.  Used by CTE bodies (Phase I.9) and, since
    // join_root_index_scan_plan.md, by the main scope of join queries
    // (the single-table `m_*` fields still serve non-join queries).
    // `body_scan_config` is the chosen candidate (NULL when the
    // selector didn't run or found no useful index, in which case the
    // scope falls back to the existing TABLE_SCAN emit).
    DynamicArray<const NdbDictionary::Index*> body_indexes;
    DynamicArray<ConditionalExpression*> body_toplevel_conditions;
    DynamicArray<ScanConfig> body_scan_config_candidates;
    ScanConfig* body_scan_config = NULL;
    MinMaxKind body_minmax_kind = MinMaxKind::NONE;

    QueryScope(ArenaMalloc* amalloc)
      : join_plan(),
        join_where_ce(),
        cross_table_where_filters(amalloc),
        body_indexes(amalloc),
        body_toplevel_conditions(amalloc),
        body_scan_config_candidates(amalloc) {}
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
    // Phase 4b (ronsql_orderby_limit_plan.md): the ordered index of this
    // candidate delivers the query's ORDER BY order directly (ORDER BY
    // list == the index columns after any leading equality-bound
    // columns, uniform direction), so the pass-through scan runs with
    // SF_OrderBy [| SF_Descending] — the NDB API merge-sorts the
    // per-fragment ordered scans — and streams rows in global index
    // order with the Phase 2 LIMIT cutoff instead of buffering for the
    // Phase 3 client-side sort.  Only ever set on the single-table
    // pass-through path (index == NULL candidates never qualify).
    bool index_order = false;
    bool index_order_desc = false;
  };
  enum class CteKeyCoverage {
    ExactOrdered,
    ExactPermuted,
    Partial,
    WrongColumns,
    ScalarDummy
  };
  struct CteKeyCoverageResult {
    CteKeyCoverage state = CteKeyCoverage::WrongColumns;
    int pk_index_for_key[MAX_JOIN_KEY_COLS];
    bool pk_covered[MAX_JOIN_KEY_COLS];
    Uint32 num_keys = 0;
    Uint32 num_pk_cols = 0;
    Uint32 first_wrong_key = 0;
    Uint32 first_missing_pk = 0;
  };
  DynamicArray<ConditionalExpression*> m_toplevel_conditions;
  // Phase I.5 v2b: column-Load Expr nodes that appeared as direct
  // operands of an n-ary GREATEST / LEAST.  Validated at compile()
  // time against the appropriate scope's resolved descriptors.
  DynamicArray<AggregationAPICompiler::Expr*> m_greatest_least_pair_loads;
  DynamicArray<ScanConfig> m_scan_config_candidates;
  ScanConfig* m_scan_config = NULL;
  // Phase 1 (non_aggregate_phase_1.md, W2): single-row PK lookup for
  // non-aggregate single-table queries whose WHERE covers the full
  // primary key with equalities.  When set, m_scan_config stays NULL
  // and m_pk_lookup_const holds one constant per PK column (in PK
  // order, arena-allocated).  Aggregate queries never set this —
  // single-table aggregation needs the scan protocol (SO_AGGREGATION);
  // a plain readTuple has no aggregator path (the single-table twin of
  // the Phase 0 lookup-root rule).
  // PK+residual follow-up (non_aggregate_pk_residual_lookup.md):
  // residual conjuncts no longer force the scan fallback — they ride
  // the lookup as an NdbRecord OO_INTERPRETED filter program.
  // m_pk_lookup_cond_map maps each m_toplevel_conditions entry to the
  // PK ordinal it binds, or -1 for a residual filter conjunct (same
  // idiom as ScanConfig::condition_handling_map, so EXPLAIN prints in
  // the same format).  The scan fallback remains for residuals whose
  // program exceeds the lookup word cap or uses types apply_filter /
  // encode_constant cannot emit (trial build at detection time).
  bool m_pk_lookup = false;
  bool m_pk_lookup_has_residual = false;
  struct ConditionalExpression** m_pk_lookup_const = NULL;
  int* m_pk_lookup_cond_map = NULL;

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
  // True for aggregating queries (the only ones RonSQL fully supports).
  // Set to false in parse() for the narrow projection-only-over-CTE_SCAN
  // shape that Phase E.3 enables — drives the pass-through delivery
  // path in execute_join() and skips ResultPrinter::compile() (which
  // requires every SELECT-list column to appear in GROUP BY).
  bool m_is_aggregate_query = true;

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
  /*
   * Parse-only construction: lexes and parses the SQL but never touches NDB
   * (conf.ndb may be NULL). Exposes the referenced table and columns via the
   * getters below so the REST layer can authorize the query before full
   * preparation. execute() cannot be called on a parse-only instance.
   */
  struct ParseOnly {};
  RonSQLPreparer(RonSQLExecParams conf, ParseOnly);
  LexCString get_table_name();
  const DynamicArray<LexCString>& get_referenced_columns();
private:
  void configure();
  void parse();
  void resolve_orderby_aliases();
  void canonicalize_orderby_columns();
  static bool same_resolved_column(const QueryScope::ResolvedColumnRef& a,
                                   const QueryScope::ResolvedColumnRef& b);
  bool has_width(size_t pos);
  void load();
  void load_single_table();
  void load_join();
  // Phase I.16b/c partial-key-CTE root rewrite, extracted from
  // load_join() (non_aggregate_phase_6.md): also called from parse()
  // ahead of the projection-only gate for non-aggregate queries, so
  // the gate's coverage check evaluates the post-rewrite tree.
  // Idempotent — a second call bails on the promoted CTE root.
  void maybe_rewrite_partial_key_cte_root();
  /* Phase I.17h: synthesise a FROM clause from qualified column refs
   * to scalar CTEs when the parser produced a NULL root_table.  No-op
   * when an explicit FROM was given. */
  void synthesize_from_for_scalar_ctes();
  const CteDefinition* find_cte_definition(const LexCString& name) const;
  bool cte_key_coverage(const CteDefinition* cte,
                        const LexCString* bound_cte_side_names,
                        Uint32 num_keys,
                        CteKeyCoverageResult& out) const;
  bool cte_key_coverage(const CteDefinition* cte,
                        const char* const* bound_cte_side_names,
                        Uint32 num_keys,
                        CteKeyCoverageResult& out) const;
  bool join_conditions_reference_only_parent(
      const JoinCondition* conditions,
      const LexCString& parent_alias) const;
  void reorder_cte_join_conditions_to_pk_order(
      JoinClause* join,
      const CteKeyCoverageResult& r);
  void normalize_cte_join_key_order();
  Int32 classify_ce_table_resolved(const QueryScope& scope,
                                   ConditionalExpression* ce) const;
  void classify_where_by_table(QueryScope& scope,
                                ConditionalExpression* where_ce);
  // Phase i26: register linked projections for ancestor columns in
  // routed CTE-op filters.  Runs after load_join's GROUP BY
  // linked-projection block (GB projections must keep slots 0..n-1 —
  // GroupByLinked positions are a sequential counter over the GROUP BY
  // list); dedups via find_or_add_linked_proj.
  void register_cte_filter_linked_projs(QueryScope& scope);
  void promote_left_to_inner_for_where(QueryScope& scope);
  static bool is_anti_join_promotable(const QueryScope& scope,
                                       Uint32 op_idx,
                                       const ConditionalExpression* ce);
  // Child-op index bounds (child_bounds feature, next_steps.md items
  // 2+3; formerly assign_cross_table_index_bounds): per non-root
  // INDEX_SCAN op, walk index columns after the join-key prefix and
  // bind consecutive columns from cross-table WHERE filters
  // (parent-linked bounds) and child-local constant conjuncts
  // (RangeBound::const_cond); consumed local conjuncts leave
  // join_where_ce[op].  Re-selects the op's index when another
  // join-key-prefix candidate binds strictly more columns.  Called
  // for the main scope (load_join) and each CTE-body scope
  // (build_cte_scopes); no-op for single-op plans.
  void assign_child_index_bounds(QueryScope& scope);
  // The normalized (child-side) comparison op if `ctf` provides a
  // bound on column idx_col_name of op op_idx; (TokenKind)0 otherwise.
  // Outputs child/parent column names + the parent op index.
  TokenKind cross_table_bound_op(QueryScope& scope, Uint32 op_idx,
                                 CrossTableFilter& ctf,
                                 const char* idx_col_name,
                                 const char** out_child_col,
                                 const char** out_parent_col,
                                 Uint32* out_parent_op);
  // The comparison op if `ce` is a bound-eligible child-local constant
  // conjunct (IDENT op CONST, identifier = column idx_col_name of op
  // op_idx, constant in the encode_constant-servable set, column NOT
  // NULL — the v1 nullability guard); (TokenKind)0 otherwise.
  TokenKind child_const_bound_op(QueryScope& scope, Uint32 op_idx,
                                 ConditionalExpression* ce,
                                 const char* idx_col_name,
                                 const NdbDictionary::Table* table);
  // Dry-run of the assign walk for index re-selection scoring:
  // EQ-bound column = +3 (walk continues), range-bound = +1 (walk
  // stops), unmatched column stops.
  Uint32 score_child_index_bounds(QueryScope& scope, Uint32 op_idx,
                                  const NdbDictionary::Index* idx,
                                  ConditionalExpression** local_conjuncts,
                                  Uint32 num_local,
                                  Uint32 num_key_cols,
                                  const NdbDictionary::Table* table);
  void plan_index_and_filter();
  void collect_toplevel_conditions(ConditionalExpression* ce);
  // Phase 1 W2 + PK+residual follow-up: returns true (and sets
  // m_pk_lookup, m_pk_lookup_const, m_pk_lookup_cond_map,
  // m_pk_lookup_has_residual) when the top-level WHERE conjuncts cover
  // the full primary key with equalities.  Conjuncts not consumed as
  // keys become residual filters carried by the lookup's interpreted
  // program; a trial build gates on the lookup word cap and on
  // emit-supported types, falling back to the scan-config path
  // (always correct) when the program cannot be carried.
  bool detect_pk_lookup();
  // `defer_force_check` (Phase 4b): skip build_scan_config_candidates'
  // FORCE INDEX satisfiability throws so the ORDER BY index pass can
  // still qualify the forced index; plan_index_and_filter then runs
  // validate_force_hint_after_orderby().
  void generate_scan_config_candidates(bool defer_force_check = false);
  // Phase 4b (ronsql_orderby_limit_plan.md): ORDER BY-driven index-order
  // candidates for single-table pass-through queries.  Resolves the ORDER
  // BY list to stored columns + a uniform direction, then for every
  // ordered index admitted by the table's index hint: an existing
  // bound-based candidate whose index serves the order is tagged
  // index_order and gets ORDERBY_INDEX_BONUS goodness (a tie-breaker
  // below any bound's value — WHERE-selected indexes keep priority); an
  // index with no bound candidate that serves the order is pushed as a
  // bonus-only candidate (all conjuncts residual filters) so it beats
  // the goodness-0 table scan.  No-op when any ORDER BY target is not a
  // stored column, directions mix, or no index matches (the query then
  // takes the Phase 3 buffered sort).
  void add_orderby_scan_config_candidates();
  // True when `index` delivers the ORDER BY order for candidate bounds
  // `condition_handling_map` (NULL = no bounds): walking the ORDER BY
  // list against the index columns, a non-matching index column may be
  // skipped only if it carries an equality bound (constant within the
  // scanned range).  `descending` receives the uniform direction.
  bool index_serves_orderby(const NdbDictionary::Index* index,
                            const int* condition_handling_map,
                            bool& descending) const;
  // The stored column an ORDER BY entry denotes on the single-table
  // path (TABLE_COLUMN via the main scope; OUTPUT_REF via the SELECT
  // output it names, a plain COLUMN under the pass-through gate); NULL
  // when it does not resolve to a stored column.
  static const NdbDictionary::Column* orderby_stored_column(
      const SelectStatement& ast_root,
      const QueryScope::ResolvedColumnRef* resolved,
      const OrderbyColumns* ob);
  // Deferred FORCE INDEX satisfiability check for the pass-through ORDER
  // BY path (see defer_force_check); throws RonSQLPermanentError with
  // the same messages as build_scan_config_candidates plus the
  // ORDER BY-aware no-WHERE variant.
  void validate_force_hint_after_orderby() const;
  static const int ORDERBY_INDEX_BONUS = 10;
  // Phase 1 W4: the single-table scan setup shared by the aggregate
  // path and the pass-through drain — table or index scan per
  // m_scan_config, bounds from condition_handling_map (with the
  // documented inverted BoundType mapping), residual conjuncts applied
  // as an NdbScanFilter.  Returns the configured operation; the caller
  // attaches aggregation or getValue()s and executes.
  NdbScanOperation* open_single_table_scan_op();
  // Phase 1 W3: projection-only single-table execution — PK-lookup arm
  // (NoDataFound = empty result) or scan drain arm, both feeding the
  // pass-through printer.
  void execute_single_table_passthrough();
  void register_passthrough_getvalues(NdbOperation* op,
                                      const NdbRecAttr** attrs,
                                      Uint32 num_cols);
  // PK+residual follow-up: the NdbRecord-lookup twin of
  // register_passthrough_getvalues — same outputs walk and checks, but
  // fills OO_GETVALUE GetValueSpec entries (an NdbRecord operation
  // cannot take RecAttr getValue() calls; the specs' recAttr results
  // are the same NdbRecAttr* the printer consumes).
  void build_passthrough_getvalue_specs(NdbOperation::GetValueSpec* gets,
                                        Uint32 num_cols);
  // Shared scan-config candidate generator used by both the
  // single-table path (`generate_scan_config_candidates`) and the
  // per-scope path (`select_root_scan_config`).  Pushes one TABLE_SCAN candidate
  // (goodness 0) plus one INDEX_SCAN candidate per ordered index that
  // any top-level conjunct can serve as a (possibly multi-column)
  // bound.  Bound-vs-residual routing for each conjunct is recorded in
  // the candidate's `condition_handling_map`.  Candidate selection
  // (highest goodness) is left to the caller.
  //
  // `hint` is the table reference whose optional MySQL-style index hint
  // (FORCE/USE/IGNORE INDEX) constrains which indexes are considered.  NULL
  // (or a hint of kind NONE) means "no hint".  A FORCE hint that names no
  // usable index throws RonSQLPermanentError.
  //
  // `table` is the scanned table (nullability authority for the guard
  // below); `allow_nullable_high_bound` (findings/nullable_bounds.md):
  // NULL sorts below every value in an NDB ordered index, so a HIGH-only
  // bound on a NULLABLE column would scan the NULL entries at the index
  // head while SQL comparison is UNKNOWN for NULL — wrong results.
  // Callers whose emit can append a NULL-excluding low bound
  // (single-table: setBound(col, BoundLT, NULL) = "col > NULL", the
  // mysqld range-optimizer idiom) pass true and keep the bound; the
  // NdbQueryBuilder-emitted roots cannot express a NULL bound operand
  // and pass false — the conjunct then stays a residual filter.
  void build_scan_config_candidates(
      DynamicArray<const NdbDictionary::Index*>& indexes,
      DynamicArray<ConditionalExpression*>& toplevel_conditions,
      DynamicArray<ScanConfig>& out_candidates,
      const TableRef* hint,
      bool defer_force_check = false,
      const NdbDictionary::Table* table = NULL,
      bool allow_nullable_high_bound = false);
  // True if `index` is named in the table ref's index-hint list (case
  // insensitive).  Used to apply FORCE/USE/IGNORE INDEX.
  static bool index_named_in_hint(const NdbDictionary::Index* index,
                                  const TableRef* hint);
  // Reject (throw) a FORCE/USE/IGNORE INDEX hint on any joined table — index
  // hints are only honored on root-table scans.  Safe to call with NULL.
  void reject_index_hints_on_joins(const JoinClause* joins) const;
  // Per-scope scan-config selection pipeline (Phase I.9 for CTE bodies;
  // join_root_index_scan_plan.md for the main-query root of join
  // queries).  Loads the root table's indexes into the scope, walks
  // `where_ce` for top-level AND conjuncts, scores candidate index
  // plans, and (if a usable ordered index is found) flips the planner's
  // first JoinOp from TABLE_SCAN to INDEX_SCAN.  Bound vs residual
  // filter routing lives in
  // `scope.body_scan_config->condition_handling_map`.
  //
  // Callers gate the scope shape: CTE bodies call it only for
  // single-op bodies (passing the body's whole WHERE); the main scope
  // calls it for any real-table TABLE_SCAN root (passing only the
  // root-classified conjuncts, `join_where_ce[0]`).  A FORCE INDEX
  // hint bypasses the "no WHERE" early-out so it can throw the same
  // errors as the single-table path.
  void select_root_scan_config(QueryScope& scope,
                               ConditionalExpression* where_ce,
                               const TableRef* hint);
  // True when every primary-key column of scope's root table has an
  // equality against a constant among the root-classified WHERE
  // conjuncts (join_where_ce[0]) — the shapes emit_root_op serves
  // better with readTuple / an equality-bound PK index scan.  Used to
  // keep root scan-config selection out of their way.
  bool root_pk_equality_covered(QueryScope& scope);
  bool load_cte_body_indexes(QueryScope& scope,
                             const NdbDictionary::Table* tab);
  static bool decimal_minmax_fits_64bit(
      NdbDictionary::Column::Type type,
      Int32 precision,
      Int32 scale);
  static bool minmax_index_source_type_supported(
      const NdbDictionary::Column* col);
  void select_cte_body_minmax_index(QueryScope& scope,
                                     const CteDefinition* cte);
  void analyze_ctes();
  void build_cte_scopes();
  bool* collect_scope_column_refs(const SelectStatement& stmt);
  void mark_scope_column_ref(bool* refs, Uint32 col_idx) const;
  void mark_scope_column_refs_ce(bool* refs,
                                 const ConditionalExpression* ce) const;
  void mark_scope_column_refs_expr(
      bool* refs,
      const AggregationAPICompiler::Expr* expr) const;
  void resolve_columns_for_scope(QueryScope& scope,
                                 const SelectStatement& stmt,
                                 bool main_scope);
  void resolve_columns_for_cte_scope(QueryScope& scope,
                                     const SelectStatement& stmt);
  void resolve_cte_output_columns();
  void resolve_cte_output_columns_for_scope(QueryScope& scope);
  /**
   * Reject CTE shapes the kernel/SPJ doesn't currently support.
   * Defensive tripwire — see cte_filter_phase_g.md.  Today only
   * blocks CTE_SCAN-as-outer-join-child (which the planner doesn't
   * emit, so this never fires from SQL — but a planner regression
   * would surface here as a clean error instead of a runtime crash).
   */
  void validate_cte_execution_shapes();
  /**
   * Resolve the (NdbDictionary::Column::Type, length, charset) tuple
   * for a column reference in `scope`, walking through chained CTE
   * output descriptors.  Mirrors the aggregate widening rules in
   * build_cte_virtual_tables so derived types stay consistent across
   * CTE chain layers.
   * Returns true on success.  Caller raises a clear error on false.
   */
  bool resolve_chained_column_type(QueryScope& scope, Uint32 col_idx,
                                    NdbDictionary::Column::Type& out_type,
                                    Uint32& out_length,
                                    const void*& out_cs,
                                    Int32& out_scale,
                                    Int32& out_precision);
  void analyze_subqueries();
  void analyze_subqueries_ce(ConditionalExpression* ce);
  void analyze_select_subqueries();
  // ORDER BY / LIMIT are parsed on every SELECT body but only the main
  // SELECT's are ever applied (ResultPrinter).  Reject them everywhere
  // else at prepare time instead of silently ignoring them (wrong
  // results vs MySQL) — see ronsql_orderby_limit_plan.md Phase 0.
  void reject_ignored_orderby_limit(const SelectStatement* stmt,
                                    const char* what,
                                    const char* name);
  void merge_same_table_subqueries();
  void rewrite_select_subqueries_as_joins();
  void decorrelate_exists();
  void decorrelate_scalar();
  void compile();
  void build_agg_linked_projections();
  void build_cte_linked_projections();
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
  // Pass-through row delivery for projection-only main SELECTs.
  // Originally Phase E.3 (single CTE_SCAN root); generalized in
  // Phase I.8 to multi-op shapes.  Each output column is routed through
  // resolved descriptors; CTE refs use the virt-table descriptor and
  // real-table refs use the stored dictionary column descriptor.
  // Caller passes the prepared NdbQuery* and the per-op
  // cteVirtualTables array (NULL entries for non-CTE ops).
  void execute_passthrough_drain(class NdbQuery* query,
                                 NdbDictionary::Table** cteVirtualTables);
  // Returns true iff the query routes through the multi-op join path:
  // either AST joins are present, or the FROM root names a CTE alias
  // (which forces QueryPlanner to produce a CTE_SCAN root op and the
  // emit/execute path to use the multi-op infrastructure).
  bool is_join_query() const;
  void emit_root_op(NdbQueryBuilder* qb, QueryScope& scope,
                    const NdbQueryOperationDef** opDefs,
                    NdbAggregator* singleAgg = nullptr,
                    NdbDictionary::Table** cteVirtualTables = nullptr);
  // Shared emit for a scan-config-selected index-scan root (Phase I.9
  // CTE bodies + main-query roots).  Builds the NdbQueryIndexBound
  // low/high chains from the scope's flattened conjuncts +
  // condition_handling_map, routes residual conjuncts (map == -1) into
  // an InterpretedCode filter on rootOpts, and returns the scanIndex
  // operation def.
  const NdbQueryOperationDef* emit_index_scan_root(
      NdbQueryBuilder* qb, QueryScope& scope,
      const NdbDictionary::Table* tab,
      const NdbDictionary::Index* idx,
      NdbQueryOptions& rootOpts);
  void build_cte_virtual_tables(const JoinPlan& plan,
                                NdbDictionary::Table** out);
  void emit_child_ops(NdbQueryBuilder* qb, QueryScope& scope,
                      const NdbQueryOperationDef** opDefs,
                      NdbAggregator* singleAgg,
                      NdbAggregator** leafAggs,
                      NdbDictionary::Table** cteVirtualTables);
  void emit_cte_lookup_filter(NdbInterpretedCode& code,
                              QueryScope& scope,
                              Uint32 op_idx,
                              NdbDictionary::Table* virtTab,
                              struct ConditionalExpression* where_ce);
  // Part B of join_nest_semantics_plan.md: join-condition LEFT->INNER
  // promotion over the flat AST join list, run in parse() before the
  // non-aggregate gate and before everything that consumes join types
  // (planner match types, EXPLAIN parse tree).  An effectively-INNER
  // join whose ON references a LEFT-joined alias eliminates that
  // alias's NULL-extended rows (join conditions are null-rejecting
  // equalities), making the LEFT equivalent to INNER — MySQL's
  // simplify_joins rewrite.  One backward pass is exact: ON conditions
  // only reference earlier aliases.
  void promote_left_joins();
  void collect_pk_equalities(struct ConditionalExpression* ce,
                             const NdbDictionary::Table* table,
                             struct ConditionalExpression* pk_const[],
                             struct ConditionalExpression* pk_eq_ce[] = NULL);
  // Phase 0a (non_aggregate_phase_0.md): AND-flatten the simplified root
  // WHERE, drop every conjunct consumed as a PK equality (pointer
  // identity with consumed[]), and recombine the rest with T_AND.
  // Returns false when the WHERE has more top-level conjuncts than the
  // flatten cap — the caller must then skip the PK-equality optimization
  // and use the full-filter scan fallback (always correct).  On success
  // sets *residual_out (NULL when every conjunct was consumed).  Both
  // walks must run on the same simplify_ce output for pointer identity
  // to hold.
  bool build_root_residual(struct ConditionalExpression* where_ce,
                           struct ConditionalExpression* const consumed[],
                           int nkeys,
                           struct ConditionalExpression** residual_out);
  // Ordered-index equality scan over the full PK — used when the WHERE
  // fully covers the PK by equality but a readTuple root is unavailable
  // (a scan child exists, or the residual filter exceeds the lookup-op
  // program cap).  Attaches the residual conjuncts as an InterpretedCode
  // filter.  Returns NULL when the table has no ordered index on the PK
  // columns; the caller then falls back to a table scan with the full
  // WHERE filter.
  const NdbQueryOperationDef* emit_pk_equality_index_scan_root(
      NdbQueryBuilder* qb, QueryScope& scope,
      const NdbDictionary::Table* root_table,
      struct ConditionalExpression* const pk_const[], int nkeys,
      struct ConditionalExpression* residual,
      NdbQueryOptions& rootOpts);
  // Phase 0b: col_idx-indexed ColumnMetadata (charset / precision /
  // scale / temporal tag per referenced column), built from the main
  // scope's resolved dict columns.  Shared by the aggregate and
  // pass-through ResultPrinter constructions.
  ResultPrinter::ColumnMetadata* build_result_column_metadata();
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
  void apply_filter_isnull(NdbScanFilter* filter, QueryScope& scope,
                           struct ConditionalExpression* ce);
  raw_value encode_constant(struct ConditionalExpression *ce,
                            const NdbDictionary::Column* col);
  struct ConditionalExpression* simplify_ce(struct ConditionalExpression* ce,
                                            int maxdepth);
  // D11: lower a `GREATEST(...) <cmp> const` / `LEAST(...) <cmp> const`
  // comparison (either operand may be the min/max node) into a boolean
  // OR/AND of per-argument column-vs-constant comparisons (with IS NOT NULL
  // guards on the OR direction).  Called from simplify_ce.
  struct ConditionalExpression* rewrite_minmax_comparison(
      TokenKind cmp_op,
      struct ConditionalExpression* left,
      struct ConditionalExpression* right,
      int maxdepth);
  void programAggregator(NdbAggregator* aggregator);
  void programAggregator_join(QueryScope& scope, SelectStatement& stmt,
                              NdbAggregator* aggregator,
                              NdbDictionary::Table* const* cteVirtualTables
                                  = NULL);
  Uint32 embedded_filter_expr_word_count(QueryScope& scope,
                                         struct ConditionalExpression* ce,
                                         Uint32 leaf_idx);
  // `cur_pos` tracks the word position inside the embedded program
  // (incremented per emitted word); `null_fail_target` is the position
  // a nullable operand's NULL guard jumps to (UNKNOWN rejects the
  // atom — findings/nullable_bounds.md, the nb-8 "code 1872" finding).
  void emit_embedded_filter_expr(NdbAggregator* agg, QueryScope& scope,
                                 struct ConditionalExpression* ce,
                                 Uint32 leaf_idx, Uint32 reg, Uint32 tmp_reg,
                                 Uint32& cur_pos, Uint32 null_fail_target);
  void generate_embedded_filter_condition(NdbAggregator* aggregator,
                                          QueryScope& scope,
                                          struct ConditionalExpression* ce,
                                          Uint32 true_output,
                                          Uint32 false_output,
                                          Uint32 leaf_idx);
  void generate_embedded_condition(NdbAggregator* aggregator,
                                   QueryScope& scope,
                                   struct ConditionalExpression* ce,
                                   Uint32 then_arm_raw_size,
                                   NdbDictionary::Table* const*
                                       cteVirtualTables,
                                   bool use_custom_outputs = false,
                                   Uint32 first_exit_output = 0,
                                   Uint32 second_exit_output = 0,
                                   Uint32 agg_leaf_idx_override = 0xFFFFFFFF);
  const NdbDictionary::Column* resolve_case_condition_column(
      QueryScope& scope,
      struct ConditionalExpression* col_side,
      NdbDictionary::Table* const* cteVirtualTables);
  // Phase I.5 v2b: walk m_greatest_least_pair_loads and reject any
  // unsupported column operand.  Run at compile() time, after column
  // resolution has populated each scope's descriptors.
  void validate_greatest_least_pair_loads();
  // Phase I.21: top-level GREATEST / LEAST is implemented as an
  // implicit MAX over a scalar expression and is only valid for scalar
  // CTE outputs.  Ordinary table columns need a real SELECT-level
  // expression evaluator, not this aggregate wrapper.
  void validate_implicit_scalar_pair_ops();
  bool is_scalar_cte_qualifier(const LexCString& qualifier) const;
  void validate_implicit_scalar_pair_op_expr(
      AggregationAPICompiler::Expr* expr) const;
  // Phase I.5 v4: emit the kernel program for one Greatest2 / Least2
  // pair-op.  Expands to either a 14-word embedded normal-interpreter
  // program (NULL-test on each operand -> SetRegNull, otherwise
  // BRANCH_(GE|LE)_REG_REG to choose output 0/1) or a 9-word body
  // (no NULL test).  The nullable path appends Mov + Skip +
  // SetRegNull; the non-null path appends only Mov.  The embedded
  // program output selects the expression-local path without stopping
  // unrelated aggregate updates.
  void emit_pair_op_embedded(NdbAggregator* aggregator,
                             Uint32 dest,
                             Uint32 src,
                             bool is_greatest,
                             bool needs_null_check);
  // Phase I.5 v4 fast path: walk a Greatest2 / Least2 Expr tree and
  // return true iff any leaf Load reaches a nullable column or an
  // unresolved CTE virtual column (where nullability isn't known at
  // compile time).  Used by `prepare_pair_op_null_check_cache` to
  // populate the AggregationAPICompiler's per-program-index decision
  // cache.
  bool compute_pair_op_needs_null_check(
      const QueryScope& scope,
      AggregationAPICompiler::Expr* expr) const;
  // Fill scope.agg->m_pair_op_needs_null_check (one entry per
  // m_program slot; only meaningful at pair-op slots) before any
  // raw_word_size or pair-op emission consumes it.  Idempotent.
  void prepare_pair_op_null_check_cache(QueryScope& scope);
  void require_cte_case_condition_column_output(QueryScope& scope,
                                                Uint32 op_idx,
                                                Uint32 cidx);
  void print_result_json(NdbAggregator* aggregator);
  void print();
  void print(struct ConditionalExpression* ce,
             LexString prefix);

public:
  ~RonSQLPreparer();
};

#endif
