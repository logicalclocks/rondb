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

#include "QueryPlanner.hpp"
#include <cstring>
#include <iostream>
#include <sstream>

void
QueryPlanner::plan(
    const TableRef *root_table,
    const JoinClause *joins,
    const NdbDictionary::Dictionary *dict,
    std::basic_ostream<char> &err,
    JoinPlan &out)
{
  out.num_ops = 0;
  out.num_linked_projs = 0;

  /*
   * Root operation: always TABLE_SCAN for now.
   * PK lookup root is deferred to a later step.
   */
  const char *root_name = root_table->name.c_str();
  const NdbDictionary::Table *root_ndb_table = dict->getTable(root_name);
  if (root_ndb_table == NULL)
  {
    err << "Table '" << root_name << "' not found." << std::endl;
    throw RonSQLPermanentError("Table not found.");
  }

  JoinOp &rootOp = out.ops[0];
  rootOp.type = JoinOp::TABLE_SCAN;
  rootOp.table = root_ndb_table;
  rootOp.index = NULL;
  rootOp.alias = root_table->alias;
  rootOp.parent_op_idx = 0;
  rootOp.is_root = true;
  rootOp.num_key_cols = 0;
  out.num_ops = 1;

  /*
   * Child operations: iterate JoinClause linked list.
   */
  for (const JoinClause *jc = joins; jc != NULL; jc = jc->next)
  {
    if (out.num_ops >= MAX_JOIN_TABLES)
    {
      err << "Too many joined tables (max " << MAX_JOIN_TABLES << ")."
          << std::endl;
      throw RonSQLPermanentError("Too many joined tables.");
    }

    /* Look up child table */
    const char *child_table_name = jc->table.name.c_str();
    const NdbDictionary::Table *child_ndb_table =
        dict->getTable(child_table_name);
    if (child_ndb_table == NULL)
    {
      err << "Table '" << child_table_name << "' not found." << std::endl;
      throw RonSQLPermanentError("Table not found.");
    }

    /* Find parent operation by matching condition.parent_table against aliases */
    const char *parent_alias = jc->condition.parent_table.c_str();
    Uint32 parent_idx = 0;
    bool found_parent = false;
    for (Uint32 p = 0; p < out.num_ops; p++)
    {
      if (strcmp(out.ops[p].alias.c_str(), parent_alias) == 0)
      {
        parent_idx = p;
        found_parent = true;
        break;
      }
    }
    if (!found_parent)
    {
      err << "Join condition references unknown table '" << parent_alias
          << "'. Tables must be joined in order, each referencing a "
          << "previously defined table." << std::endl;
      throw RonSQLPermanentError("Join references unknown table.");
    }

    /* Determine join type for the child */
    const char *child_key_col = jc->condition.child_column.c_str();
    const char *parent_key_col = jc->condition.parent_column.c_str();

    JoinOp &childOp = out.ops[out.num_ops];
    childOp.table = child_ndb_table;
    childOp.alias = jc->table.alias;
    childOp.parent_op_idx = parent_idx;
    childOp.is_root = false;
    childOp.child_key_col_names[0] = child_key_col;
    childOp.parent_key_col_names[0] = parent_key_col;
    childOp.num_key_cols = 1;

    if (isPrimaryKey(child_ndb_table, child_key_col))
    {
      childOp.type = JoinOp::PK_LOOKUP;
      childOp.index = NULL;
    }
    else
    {
      const NdbDictionary::Index *unique_idx =
          findUniqueIndex(dict, child_ndb_table, child_key_col);
      if (unique_idx != NULL)
      {
        childOp.type = JoinOp::UNIQUE_LOOKUP;
        childOp.index = unique_idx;
      }
      else
      {
        const NdbDictionary::Index *ordered_idx =
            findOrderedIndex(dict, child_ndb_table, child_key_col);
        if (ordered_idx != NULL)
        {
          childOp.type = JoinOp::INDEX_SCAN;
          childOp.index = ordered_idx;
        }
        else
        {
          err << "Cannot push join: no suitable index on '"
              << child_key_col << "' for table '" << child_table_name
              << "'. Create a primary key, unique index, or ordered "
              << "index on the join column." << std::endl;
          throw RonSQLPermanentError(
              "No suitable index for join column.");
        }
      }
    }

    out.num_ops++;
  }

  /* Leaf = last operation */
  out.agg_leaf_idx = out.num_ops - 1;
}

bool
QueryPlanner::isPrimaryKey(const NdbDictionary::Table *table,
                           const char *col_name)
{
  int nkeys = table->getNoOfPrimaryKeys();
  if (nkeys != 1) return false;
  const char *pk_name = table->getPrimaryKey(0);
  return (pk_name != NULL && strcmp(pk_name, col_name) == 0);
}

const NdbDictionary::Index *
QueryPlanner::findUniqueIndex(const NdbDictionary::Dictionary *dict,
                              const NdbDictionary::Table *table,
                              const char *col_name)
{
  NdbDictionary::Dictionary::List index_list;
  if (dict->listIndexes(index_list, *table) != 0) return NULL;

  for (Uint32 i = 0; i < index_list.count; i++)
  {
    NdbDictionary::Dictionary::List::Element &elem = index_list.elements[i];
    if (elem.type != NdbDictionary::Object::UniqueHashIndex) continue;
    if (elem.state != NdbDictionary::Object::StateOnline) continue;

    const NdbDictionary::Index *idx = dict->getIndex(elem.name, *table);
    if (idx == NULL) continue;
    if (idx->getNoOfColumns() != 1) continue;

    const NdbDictionary::Column *idx_col = idx->getColumn(0);
    if (idx_col == NULL) continue;

    const char *idx_col_name = idx_col->getName();
    if (idx_col_name != NULL && strcmp(idx_col_name, col_name) == 0)
    {
      return idx;
    }
  }
  return NULL;
}

const NdbDictionary::Index *
QueryPlanner::findOrderedIndex(const NdbDictionary::Dictionary *dict,
                               const NdbDictionary::Table *table,
                               const char *col_name)
{
  NdbDictionary::Dictionary::List index_list;
  if (dict->listIndexes(index_list, *table) != 0) return NULL;

  for (Uint32 i = 0; i < index_list.count; i++)
  {
    NdbDictionary::Dictionary::List::Element &elem = index_list.elements[i];
    if (elem.type != NdbDictionary::Object::OrderedIndex) continue;
    if (elem.state != NdbDictionary::Object::StateOnline) continue;

    const NdbDictionary::Index *idx = dict->getIndex(elem.name, *table);
    if (idx == NULL) continue;
    if (idx->getNoOfColumns() < 1) continue;

    const NdbDictionary::Column *idx_col = idx->getColumn(0);
    if (idx_col == NULL) continue;

    const char *idx_col_name = idx_col->getName();
    if (idx_col_name != NULL && strcmp(idx_col_name, col_name) == 0)
    {
      return idx;
    }
  }
  return NULL;
}
