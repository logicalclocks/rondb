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

static const Uint32 MAX_JOIN_TABLES = 16;
static const Uint32 MAX_JOIN_KEY_COLS = 8;
static const Uint32 MAX_LINKED_PROJS = 16;

struct JoinOp
{
  enum Type { TABLE_SCAN, INDEX_SCAN, PK_LOOKUP, UNIQUE_LOOKUP };
  enum MatchType { INNER, SEMI_JOIN, ANTI_JOIN };
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
};

struct JoinPlan
{
  JoinOp ops[MAX_JOIN_TABLES];
  Uint32 num_ops;
  Uint32 agg_leaf_idx;

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
