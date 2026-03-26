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
  enum Type { TABLE_SCAN, INDEX_SCAN, PK_LOOKUP, UNIQUE_LOOKUP };
  enum MatchType { INNER, LEFT_OUTER, SEMI_JOIN, ANTI_JOIN };
  Type type;
  MatchType match_type;
  const NdbDictionary::Table *table;
  const NdbDictionary::Index *index;
  LexCString alias;
  Uint32 parent_op_idx;
  bool is_root;
  const char *child_key_col_names[MAX_JOIN_KEY_COLS];
  const char *parent_key_col_names[MAX_JOIN_KEY_COLS];
  Uint32 num_key_cols;

  // Range bounds from cross-table WHERE on index columns (after join keys).
  // These extend the index scan bounds beyond the equality join keys.
  struct RangeBound {
    const char *child_col_name;    // child index column
    const char *parent_col_name;   // parent column providing the bound value
    Uint32 parent_op_idx;          // parent operation index
    bool inclusive;                 // true = <=/>= , false = </>
  };
  RangeBound low_bounds[MAX_JOIN_KEY_COLS];
  Uint32 num_low_bounds;
  RangeBound high_bounds[MAX_JOIN_KEY_COLS];
  Uint32 num_high_bounds;
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
      JoinPlan &plan);

  static const NdbDictionary::Index *
  findOrderedIndex(const NdbDictionary::Dictionary *dict,
                   const NdbDictionary::Table *table,
                   const char *col_names[], Uint32 num_cols);

private:
  static bool isPrimaryKey(const NdbDictionary::Table *table,
                           const char *col_names[], Uint32 num_cols);

  static const NdbDictionary::Index *
  findUniqueIndex(const NdbDictionary::Dictionary *dict,
                  const NdbDictionary::Table *table,
                  const char *col_names[], Uint32 num_cols);
};

#endif
