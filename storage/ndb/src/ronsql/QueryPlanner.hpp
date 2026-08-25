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

#ifndef STORAGE_NDB_SRC_RONSQL_QUERYPLANNER_HPP
#define STORAGE_NDB_SRC_RONSQL_QUERYPLANNER_HPP 1

#include <NdbApi.hpp>
#include <vector>
#include "RonSQLCommon.hpp"
#include "LexString.hpp"

// Max tree nodes in pushed SPJ query (matches NDB_SPJ_MAX_TREE_NODES).
// After subquery merge, each distinct table = 1 tree node.
static const Uint32 MAX_SPJ_TREE_NODES = 32;
// Max subqueries before merge (SQL-level limit, can be >> tree nodes).
static const Uint32 MAX_SQL_SUBQUERIES = 128;
static const Uint32 MAX_JOIN_KEY_COLS = 8;
static const Uint32 MAX_LINKED_PROJS = 16;

struct JoinOp
{
  enum Type { TABLE_SCAN, INDEX_SCAN, PK_LOOKUP, UNIQUE_LOOKUP, CTE_LOOKUP,
              CTE_SCAN };
  enum MatchType { INNER, LEFT_OUTER, SEMI_JOIN, ANTI_JOIN };
  Type type;
  MatchType match_type;
  const NdbDictionary::Table *table;  // NULL for CTE_LOOKUP
  const NdbDictionary::Index *index;
  LexCString alias;
  // Effective parent = the deepest key-source op.  With per-key parent
  // sources (non_aggregate_phase_6.md, gc-P2 lift) the ON conditions of
  // one JOIN may reference different ancestor aliases; parent_op_idx is
  // then the deepest referenced op (every other key source must lie on
  // its ancestor chain — validated in plan()), matching exactly how
  // NdbQueryBuilder's linkWithParent resolves the operation's parent.
  // In the common single-parent case it equals every
  // key_parent_op_idx[k].
  Uint32 parent_op_idx;
  // Tree parent in the SPJ query tree. Normally equals parent_op_idx (the
  // op whose column provides this op's join key — also used as the key-
  // source for linkedValue). They diverge when CTE_LOOKUP siblings are
  // chained in the main query: the key source stays on the original join
  // parent (e.g. the real-table root), while the tree parent is set to the
  // previous CTE_LOOKUP so the main aggregator (on the deepest CTE_LOOKUP)
  // can read all other CTE outputs as ancestor-linked projections. See
  // testCrossJoinTwoScalarCtes / testGreatestViaCaseAgg for the canonical
  // chained-CTE-LOOKUP topology.
  Uint32 tree_parent_op_idx;
  bool is_root;
  const char *child_key_col_names[MAX_JOIN_KEY_COLS];
  const char *parent_key_col_names[MAX_JOIN_KEY_COLS];
  // Per-key key-source op (the op whose column provides key k's value;
  // emit passes it to linkedValue).  Generalizes keys the way
  // RangeBound::parent_op_idx already generalizes bounds.
  Uint32 key_parent_op_idx[MAX_JOIN_KEY_COLS];
  Uint32 num_key_cols;

  // Range bounds on index columns after the join-key prefix — from
  // cross-table WHERE conjuncts (parent-linked values) or child-local
  // constant conjuncts (child_bounds feature, next_steps.md item 2).
  struct RangeBound {
    const char *child_col_name;    // child index column
    const char *parent_col_name;   // parent column providing the bound value
    Uint32 parent_op_idx;          // parent operation index
    bool inclusive;                 // true = <=/>= , false = </>
    // Non-NULL => constant bound: the whole conjunct (IDENT op CONST);
    // emit encodes args.right via encode_constant and the parent_*
    // fields are unused.  NULL => parent-linked bound as before.
    ConditionalExpression *const_cond;
  };
  RangeBound low_bounds[MAX_JOIN_KEY_COLS];
  Uint32 num_low_bounds;
  RangeBound high_bounds[MAX_JOIN_KEY_COLS];
  Uint32 num_high_bounds;

  // CTE-specific fields (valid when type == CTE_LOOKUP)
  CteDefinition *cte_def;  // Pointer to CTE definition AST node
  Uint32 cte_def_idx;      // Index of CTE in cte_list (0-based)

  // Phase i26 (cte_filter_phase_i26.md): this CTE_LOOKUP op's filter
  // references ancestor real-table columns (e.g. `t.col > s.m`), so
  // emit must attach the plan's linked projections even when no
  // aggregator sits on the op — DBLQH prepends them to the CTE outputs
  // in the linked-attr buffer and the filter reads them by projection
  // index.  Set by classify_where_by_table.
  bool needs_parent_linked_attrs;
};

struct JoinPlan
{
  JoinOp ops[MAX_SPJ_TREE_NODES];
  Uint32 num_ops;
  Uint32 agg_leaf_idx;           // single-leaf mode (num_agg_leaves == 0)

  // Multi-leaf aggregation (for SELECT-list subquery pushdown)
  Uint32 agg_leaf_indices[MAX_SPJ_TREE_NODES];
  Uint32 num_agg_leaves;         // 0 = single-leaf mode

  struct LinkedProj {
    Uint32 source_op_idx;
    const char *column_name;
  };
  LinkedProj linked_projs[MAX_LINKED_PROJS];
  Uint32 num_linked_projs;
};

class QueryPlanner
{
public:
  static void plan(
      const TableRef *root_table,
      const JoinClause *joins,
      const NdbDictionary::Dictionary *dict,
      std::basic_ostream<char> &err,
      JoinPlan &plan,
      RdrsSchemaCache *cache = nullptr,
      const char *database = nullptr,
      const CteDefinition *cte_list = nullptr);

  static const NdbDictionary::Index *
  findOrderedIndex(const NdbDictionary::Dictionary *dict,
                   const NdbDictionary::Table *table,
                   const char *col_names[], Uint32 num_cols,
                   RdrsSchemaCache *cache = nullptr,
                   const char *database = nullptr);

  // All online ordered indexes whose first num_cols columns contain the
  // given columns (the findOrderedIndex predicate, every match instead
  // of the first).  Used by the child-bounds index re-selection
  // (next_steps.md item 3): a later candidate can beat the planner's
  // first match when it additionally serves child-local constant or
  // cross-table bounds.
  static void
  collectOrderedIndexCandidates(const NdbDictionary::Dictionary *dict,
                                const NdbDictionary::Table *table,
                                const char *col_names[], Uint32 num_cols,
                                RdrsSchemaCache *cache,
                                const char *database,
                                std::vector<const NdbDictionary::Index*> &out);

private:
  static bool isPrimaryKey(const NdbDictionary::Table *table,
                           const char *col_names[], Uint32 num_cols);

  static const NdbDictionary::Index *
  findUniqueIndex(const NdbDictionary::Dictionary *dict,
                  const NdbDictionary::Table *table,
                  const char *col_names[], Uint32 num_cols,
                  RdrsSchemaCache *cache = nullptr,
                  const char *database = nullptr);
};

#endif
