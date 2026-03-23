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
  out.num_agg_leaves = 0;
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
  rootOp.match_type = JoinOp::INNER;
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

    /* Collect all key columns from the ON condition list */
    JoinOp &childOp = out.ops[out.num_ops];
    childOp.table = child_ndb_table;
    childOp.alias = jc->table.alias;
    childOp.is_root = false;
    childOp.match_type = (jc->join_type == JoinClause::LEFT_OUTER_JOIN)
        ? JoinOp::LEFT_OUTER : JoinOp::INNER;

    Uint32 num_keys = 0;
    Uint32 parent_idx = 0;
    bool found_parent = false;
    for (const JoinCondition *cond = jc->conditions;
         cond != NULL;
         cond = cond->next)
    {
      if (num_keys >= MAX_JOIN_KEY_COLS)
      {
        err << "Too many join key columns (max " << MAX_JOIN_KEY_COLS
            << ")." << std::endl;
        throw RonSQLPermanentError("Too many join key columns.");
      }

      /* Find parent operation from first condition, verify consistent */
      const char *parent_alias = cond->parent_table.c_str();
      if (num_keys == 0)
      {
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
      }
      else
      {
        /* All conditions must reference the same parent */
        if (strcmp(out.ops[parent_idx].alias.c_str(), parent_alias) != 0)
        {
          err << "All ON conditions in a single JOIN must reference the "
              << "same parent table." << std::endl;
          throw RonSQLPermanentError(
              "Mixed parent tables in ON conditions.");
        }
      }

      childOp.child_key_col_names[num_keys] = cond->child_column.c_str();
      childOp.parent_key_col_names[num_keys] = cond->parent_column.c_str();
      num_keys++;
    }

    childOp.parent_op_idx = parent_idx;
    childOp.num_key_cols = num_keys;

    /* Determine join type for the child */
    if (isPrimaryKey(child_ndb_table, childOp.child_key_col_names, num_keys))
    {
      childOp.type = JoinOp::PK_LOOKUP;
      childOp.index = NULL;
    }
    else
    {
      const NdbDictionary::Index *unique_idx =
          findUniqueIndex(dict, child_ndb_table,
                          childOp.child_key_col_names, num_keys);
      if (unique_idx != NULL)
      {
        childOp.type = JoinOp::UNIQUE_LOOKUP;
        childOp.index = unique_idx;
      }
      else
      {
        const NdbDictionary::Index *ordered_idx =
            findOrderedIndex(dict, child_ndb_table,
                             childOp.child_key_col_names, num_keys);
        if (ordered_idx != NULL)
        {
          childOp.type = JoinOp::INDEX_SCAN;
          childOp.index = ordered_idx;
        }
        else
        {
          err << "Cannot push join: no suitable index on join columns"
              << " for table '" << child_table_name
              << "'. Create a primary key, unique index, or ordered "
              << "index on the join columns." << std::endl;
          throw RonSQLPermanentError(
              "No suitable index for join columns.");
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
                           const char *col_names[], Uint32 num_cols)
{
  int nkeys = table->getNoOfPrimaryKeys();
  if ((Uint32)nkeys != num_cols) return false;
  for (Uint32 k = 0; k < num_cols; k++)
  {
    bool found = false;
    for (int p = 0; p < nkeys; p++)
    {
      const char *pk_name = table->getPrimaryKey(p);
      if (pk_name != NULL && strcmp(pk_name, col_names[k]) == 0)
      {
        found = true;
        break;
      }
    }
    if (!found) return false;
  }
  return true;
}

const NdbDictionary::Index *
QueryPlanner::findUniqueIndex(const NdbDictionary::Dictionary *dict,
                              const NdbDictionary::Table *table,
                              const char *col_names[], Uint32 num_cols)
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
    if ((Uint32)idx->getNoOfColumns() != num_cols) continue;

    bool all_match = true;
    for (Uint32 c = 0; c < num_cols; c++)
    {
      bool found = false;
      for (Uint32 j = 0; j < (Uint32)idx->getNoOfColumns(); j++)
      {
        const NdbDictionary::Column *idx_col = idx->getColumn(j);
        if (idx_col != NULL &&
            strcmp(idx_col->getName(), col_names[c]) == 0)
        {
          found = true;
          break;
        }
      }
      if (!found) { all_match = false; break; }
    }
    if (all_match) return idx;
  }
  return NULL;
}

const NdbDictionary::Index *
QueryPlanner::findOrderedIndex(const NdbDictionary::Dictionary *dict,
                               const NdbDictionary::Table *table,
                               const char *col_names[], Uint32 num_cols)
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
    if ((Uint32)idx->getNoOfColumns() < num_cols) continue;

    /* Check that all join columns appear as a prefix of the index */
    bool all_match = true;
    for (Uint32 c = 0; c < num_cols; c++)
    {
      bool found = false;
      for (Uint32 j = 0; j < num_cols && j < (Uint32)idx->getNoOfColumns(); j++)
      {
        const NdbDictionary::Column *idx_col = idx->getColumn(j);
        if (idx_col != NULL &&
            strcmp(idx_col->getName(), col_names[c]) == 0)
        {
          found = true;
          break;
        }
      }
      if (!found) { all_match = false; break; }
    }
    if (all_match) return idx;
  }
  return NULL;
}
