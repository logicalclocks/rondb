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
#include "RdrsSchemaCache.hpp"
#include <cstring>
#include <iostream>
#include <sstream>

/**
 * Helper: get the index list for a table, using the schema cache if available.
 * Returns the cached index vector, or populates fallback_list via dict->listIndexes()
 * and returns nullptr (caller must use fallback_list).
 */
static const std::vector<RdrsSchemaCache::CachedIndex>*
getIndexList(const NdbDictionary::Dictionary *dict,
             const NdbDictionary::Table *table,
             RdrsSchemaCache *cache,
             const char *database,
             NdbDictionary::Dictionary::List &fallback_list) {
  if (cache != nullptr && database != nullptr) {
    return cache->getIndexes(dict, table, database, table->getName());
  }
  if (dict->listIndexes(fallback_list, *table) != 0) {
    return nullptr;  // error
  }
  return nullptr;  // signal: use fallback_list
}

/**
 * Find a CTE definition by table name. Returns NULL if not a CTE.
 * If found, sets out_idx to the 0-based index of the CTE in the list.
 */
static const CteDefinition*
findCte(const CteDefinition *cte_list, const char *name, Uint32 &out_idx)
{
  Uint32 idx = 0;
  for (const CteDefinition *cte = cte_list; cte != NULL; cte = cte->next, idx++)
  {
    if (strcmp(cte->name.c_str(), name) == 0)
    {
      out_idx = idx;
      return cte;
    }
  }
  out_idx = 0;
  return NULL;
}

void
QueryPlanner::plan(
    const TableRef *root_table,
    const JoinClause *joins,
    const NdbDictionary::Dictionary *dict,
    std::basic_ostream<char> &err,
    JoinPlan &out,
    RdrsSchemaCache *cache,
    const char *database,
    const CteDefinition *cte_list)
{
  out.num_ops = 0;
  out.num_agg_leaves = 0;
  out.num_linked_projs = 0;

  /*
   * Root operation: CTE_SCAN if the root name refers to a CTE visible in
   * this scope, otherwise TABLE_SCAN on the physical table. PK lookup
   * root is deferred to a later step.
   */
  const char *root_name = root_table->name.c_str();
  Uint32 root_cte_idx = 0;
  const CteDefinition *root_cte_match =
      findCte(cte_list, root_name, root_cte_idx);

  JoinOp &rootOp = out.ops[0];
  rootOp.alias = root_table->alias;
  rootOp.index = NULL;
  rootOp.parent_op_idx = 0;
  rootOp.tree_parent_op_idx = 0;
  rootOp.is_root = true;
  rootOp.match_type = JoinOp::INNER;
  rootOp.num_key_cols = 0;
  rootOp.num_low_bounds = 0;
  rootOp.num_high_bounds = 0;

  if (root_cte_match != NULL)
  {
    rootOp.type = JoinOp::CTE_SCAN;
    rootOp.table = NULL;
    rootOp.cte_def = const_cast<CteDefinition*>(root_cte_match);
    rootOp.cte_def_idx = root_cte_idx;
  }
  else
  {
    const NdbDictionary::Table *root_ndb_table = dict->getTable(root_name);
    if (root_ndb_table == NULL)
    {
      err << "Table '" << root_name << "' not found." << std::endl;
      throw RonSQLPermanentError("Table not found.");
    }
    rootOp.type = JoinOp::TABLE_SCAN;
    rootOp.table = root_ndb_table;
    rootOp.cte_def = NULL;
    rootOp.cte_def_idx = 0;
  }
  out.num_ops = 1;

  /*
   * Child operations: iterate JoinClause linked list.
   */
  for (const JoinClause *jc = joins; jc != NULL; jc = jc->next)
  {
    if (out.num_ops >= MAX_SPJ_TREE_NODES)
    {
      err << "Too many joined tables (max " << MAX_SPJ_TREE_NODES << ")."
          << std::endl;
      throw RonSQLPermanentError("Too many joined tables.");
    }

    /* Look up child table — check CTEs first, then NDB dictionary */
    const char *child_table_name = jc->table.name.c_str();
    Uint32 cte_idx = 0;
    const CteDefinition *cte_match = findCte(cte_list, child_table_name,
                                             cte_idx);

    JoinOp &childOp = out.ops[out.num_ops];
    childOp.alias = jc->table.alias;
    childOp.is_root = false;
    childOp.match_type = (jc->join_type == JoinClause::LEFT_OUTER_JOIN)
        ? JoinOp::LEFT_OUTER : JoinOp::INNER;
    childOp.num_low_bounds = 0;
    childOp.num_high_bounds = 0;
    childOp.type = JoinOp::TABLE_SCAN;
    childOp.table = NULL;
    childOp.index = NULL;
    childOp.cte_def = NULL;
    childOp.cte_def_idx = 0;

    if (cte_match != NULL)
    {
      /* CTE reference — no NDB table, use virtual schema */
      childOp.type = JoinOp::CTE_LOOKUP;
      childOp.table = NULL;
      childOp.index = NULL;
      childOp.cte_def = const_cast<CteDefinition*>(cte_match);
      childOp.cte_def_idx = cte_idx;
    }
    else
    {
      const NdbDictionary::Table *child_ndb_table =
          dict->getTable(child_table_name);
      if (child_ndb_table == NULL)
      {
        err << "Table '" << child_table_name << "' not found." << std::endl;
        throw RonSQLPermanentError("Table not found.");
      }
      childOp.table = child_ndb_table;
    }

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
    childOp.tree_parent_op_idx = parent_idx;
    childOp.num_key_cols = num_keys;

    /* CTE_LOOKUP type and index are already set — skip index determination */
    if (childOp.type == JoinOp::CTE_LOOKUP)
    {
      /* CTE lookups use hash table, no NDB index needed */
    }
    else if (isPrimaryKey(childOp.table, childOp.child_key_col_names, num_keys))
    {
      childOp.type = JoinOp::PK_LOOKUP;
      childOp.index = NULL;
    }
    else
    {
      const NdbDictionary::Index *unique_idx =
          findUniqueIndex(dict, childOp.table,
                          childOp.child_key_col_names, num_keys,
                          cache, database);
      if (unique_idx != NULL)
      {
        childOp.type = JoinOp::UNIQUE_LOOKUP;
        childOp.index = unique_idx;
      }
      else
      {
        const NdbDictionary::Index *ordered_idx =
            findOrderedIndex(dict, childOp.table,
                             childOp.child_key_col_names, num_keys,
                             cache, database);
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

    /* Phase 2 (non_aggregate_phase_2.md, W2): pre-check the NDB API's
     * linked-operand rule for real-table links — the child key column
     * and the parent column must be identically declared (type,
     * precision, scale, length, charset; no implicit conversion), and
     * BLOB/TEXT can never be linked.  Failing here gives a clean
     * permanent error instead of a runtime NDB error from
     * NdbQueryBuilder's linkedValue/bindOperand.  CTE operands are
     * skipped — virtual-table typing is handled by the CTE machinery.
     */
    if (childOp.table != NULL && out.ops[parent_idx].table != NULL)
    {
      for (Uint32 k = 0; k < num_keys; k++)
      {
        const NdbDictionary::Column *child_col =
            childOp.table->getColumn(childOp.child_key_col_names[k]);
        const NdbDictionary::Column *parent_col =
            out.ops[parent_idx].table->getColumn(
                childOp.parent_key_col_names[k]);
        if (child_col == NULL || parent_col == NULL)
        {
          err << "Join condition references unknown column '"
              << (child_col == NULL ? childOp.child_key_col_names[k]
                                    : childOp.parent_key_col_names[k])
              << "'." << std::endl;
          throw RonSQLPermanentError("Unknown join column.");
        }
        if (child_col->getType() == NdbDictionary::Column::Blob ||
            child_col->getType() == NdbDictionary::Column::Text ||
            parent_col->getType() == NdbDictionary::Column::Blob ||
            parent_col->getType() == NdbDictionary::Column::Text)
        {
          err << "BLOB/TEXT columns cannot be used as join columns."
              << std::endl;
          throw RonSQLPermanentError("BLOB/TEXT join column.");
        }
        if (child_col->getType() != parent_col->getType() ||
            child_col->getPrecision() != parent_col->getPrecision() ||
            child_col->getScale() != parent_col->getScale() ||
            child_col->getLength() != parent_col->getLength() ||
            child_col->getCharset() != parent_col->getCharset())
        {
          err << "Join columns '" << childOp.parent_key_col_names[k]
              << "' and '" << childOp.child_key_col_names[k]
              << "' must have identical type, precision, scale, length"
              << " and character set: NDB pushed joins do not convert"
              << " linked values." << std::endl;
          throw RonSQLPermanentError("Join column type mismatch.");
        }
      }
    }

    out.num_ops++;
  }

  /* Chain sibling CTE_LOOKUPs in declaration order. The SPJ tree requires
   * linked-projection sources to be ancestors of the op that consumes them;
   * two CTE_LOOKUPs sharing a common non-CTE parent are siblings and
   * cannot reference each other. Re-parenting the later CTE_LOOKUP under
   * the earlier one (as its tree parent, leaving parent_op_idx / key
   * source unchanged) forms a chain: the common ancestor remains reachable
   * via linkedValue, and the deepest CTE_LOOKUP sees all earlier CTE
   * outputs as ancestor-linked projections. Materialization still runs in
   * parallel (defineCte depMask unaffected). */
  for (Uint32 i = 1; i < out.num_ops; i++)
  {
    if (out.ops[i].type != JoinOp::CTE_LOOKUP) continue;
    Uint32 latest_cte_idx = out.num_ops; /* sentinel */
    for (Uint32 j = i; j-- > 1; )
    {
      if (out.ops[j].type == JoinOp::CTE_LOOKUP &&
          out.ops[j].parent_op_idx == out.ops[i].parent_op_idx)
      {
        latest_cte_idx = j;
        break;
      }
    }
    if (latest_cte_idx < out.num_ops)
    {
      out.ops[i].tree_parent_op_idx = latest_cte_idx;
    }
  }

  /* Phase 2 (non_aggregate_phase_2.md, W2): NDB API internal-node
   * budget — a unique-index lookup expands to 2 internal tree nodes
   * (index table + base table), which the num_ops bound above counts
   * as 1.  CTE subtree nodes are accounted by the API's own
   * prepare-time cap (the planner cannot know body sizes). */
  {
    Uint32 internal_nodes = out.num_ops;
    for (Uint32 i = 0; i < out.num_ops; i++)
    {
      if (out.ops[i].type == JoinOp::UNIQUE_LOOKUP)
        internal_nodes++;
    }
    if (internal_nodes > MAX_SPJ_TREE_NODES)
    {
      err << "Pushed join too large: " << internal_nodes
          << " internal operations exceed the NDB limit of "
          << MAX_SPJ_TREE_NODES
          << " (a unique-index lookup counts as 2)." << std::endl;
      throw RonSQLPermanentError("Pushed join too large.");
    }
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
                              const char *col_names[], Uint32 num_cols,
                              RdrsSchemaCache *cache,
                              const char *database)
{
  NdbDictionary::Dictionary::List fallback_list;
  const auto* cached = getIndexList(dict, table, cache, database, fallback_list);

  if (cached != nullptr) {
    // Use cached index names
    for (const auto& ci : *cached) {
      if (ci.type != NdbDictionary::Object::UniqueHashIndex) continue;
      if (ci.state != NdbDictionary::Object::StateOnline) continue;
      const NdbDictionary::Index *idx = dict->getIndex(ci.name.c_str(), *table);
      if (idx == NULL) continue;
      if ((Uint32)idx->getNoOfColumns() != num_cols) continue;
      bool all_match = true;
      for (Uint32 c = 0; c < num_cols; c++) {
        bool found = false;
        for (Uint32 j = 0; j < (Uint32)idx->getNoOfColumns(); j++) {
          const NdbDictionary::Column *idx_col = idx->getColumn(j);
          if (idx_col != NULL && strcmp(idx_col->getName(), col_names[c]) == 0) {
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

  // Fallback: use dict->listIndexes() result
  for (Uint32 i = 0; i < fallback_list.count; i++)
  {
    NdbDictionary::Dictionary::List::Element &elem = fallback_list.elements[i];
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
                               const char *col_names[], Uint32 num_cols,
                               RdrsSchemaCache *cache,
                               const char *database)
{
  NdbDictionary::Dictionary::List fallback_list;
  const auto* cached = getIndexList(dict, table, cache, database, fallback_list);

  // Lambda to check one index by name
  auto checkIndex = [&](const char* idx_name) -> const NdbDictionary::Index* {
    const NdbDictionary::Index *idx = dict->getIndex(idx_name, *table);
    if (idx == NULL) return NULL;
    if ((Uint32)idx->getNoOfColumns() < num_cols) return NULL;
    for (Uint32 c = 0; c < num_cols; c++) {
      bool found = false;
      for (Uint32 j = 0; j < num_cols && j < (Uint32)idx->getNoOfColumns(); j++) {
        const NdbDictionary::Column *idx_col = idx->getColumn(j);
        if (idx_col != NULL && strcmp(idx_col->getName(), col_names[c]) == 0) {
          found = true;
          break;
        }
      }
      if (!found) return NULL;
    }
    return idx;
  };

  if (cached != nullptr) {
    for (const auto& ci : *cached) {
      if (ci.type != NdbDictionary::Object::OrderedIndex) continue;
      if (ci.state != NdbDictionary::Object::StateOnline) continue;
      const NdbDictionary::Index *idx = checkIndex(ci.name.c_str());
      if (idx != NULL) return idx;
    }
    return NULL;
  }

  // Fallback
  for (Uint32 i = 0; i < fallback_list.count; i++) {
    NdbDictionary::Dictionary::List::Element &elem = fallback_list.elements[i];
    if (elem.type != NdbDictionary::Object::OrderedIndex) continue;
    if (elem.state != NdbDictionary::Object::StateOnline) continue;
    const NdbDictionary::Index *idx = checkIndex(elem.name);
    if (idx != NULL) return idx;
  }
  return NULL;
}
