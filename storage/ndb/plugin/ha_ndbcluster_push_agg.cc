/*
   Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/**
 * @file ha_ndbcluster_push_agg.cc
 *
 * Aggregation pushdown for NDB pushed joins — implementation.
 *
 * Contains all logic for detecting pushable aggregation queries,
 * building NdbAggregator programs, modifying the AccessPath tree,
 * and fetching aggregate results.
 */

#include "storage/ndb/plugin/ha_ndbcluster_push_agg.h"

#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "storage/ndb/include/ndbapi/NdbAggregator.hpp"
#include "storage/ndb/plugin/ha_ndbcluster.h"
#include "storage/ndb/plugin/ha_ndbcluster_push.h"

/**
 * Check if the aggregate functions and GROUP BY in a JOIN are pushable.
 *
 * Validates:
 * - Query has aggregate functions (COUNT/SUM/MIN/MAX, no DISTINCT)
 * - No ROLLUP
 * - GROUP BY columns are simple field references from pushed tables
 * - Aggregate arguments are field references from pushed tables
 *
 * @return true if aggregation is pushable
 */
static bool ndb_can_push_aggregation(const JOIN *join) {
  // Query must have aggregate functions.
  if (join->sum_funcs == nullptr || join->sum_funcs[0] == nullptr) {
    return false;
  }

  // No ROLLUP.
  if (join->query_block->olap != UNSPECIFIED_OLAP_TYPE) {
    return false;
  }

  // All aggregate functions must be pushable types.
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    switch ((*func)->sum_func()) {
      case Item_sum::COUNT_FUNC:
        // COUNT(*) or COUNT(expr) — always pushable.
        break;
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        // Must have exactly one argument that is a field reference
        // from a pushed table.
        if ((*func)->argument_count() != 1) return false;
        Item *arg = (*func)->arguments()[0];
        if (arg->type() != Item::FIELD_ITEM) return false;
        const auto *field_item = down_cast<const Item_field *>(arg);
        if (field_item->field->table->file->member_of_pushed_join() ==
            nullptr) {
          return false;
        }
        break;
      }
      default:
        // Unsupported: COUNT_DISTINCT, SUM_DISTINCT, AVG, GROUP_CONCAT, etc.
        return false;
    }
  }

  // GROUP BY columns must all be simple field references from pushed tables.
  for (ORDER *group = join->group_list.order; group != nullptr;
       group = group->next) {
    Item *item = *(group->item);
    if (item->type() != Item::FIELD_ITEM) return false;
    const auto *field_item = down_cast<const Item_field *>(item);
    if (field_item->field->table->file->member_of_pushed_join() == nullptr) {
      return false;
    }
  }

  return true;
}

/**
 * Build an NdbAggregator program from the MySQL query plan.
 *
 * Phase 3 scope: builds and validates the program but does not attach it
 * to the pushed join for execution. The NdbAggregator is constructed on the
 * stack and discarded — this validates that the translation from MySQL's
 * aggregate representation to NdbAggregator instructions is correct.
 *
 * Assumes a 2-table join where GROUP BY is on the root table and
 * aggregated columns are on the leaf table.
 *
 * @param join           MySQL JOIN containing aggregation info
 * @param root_ndb_tab   NDB table for the root (parent) table
 * @param leaf_ndb_tab   NDB table for the leaf (child) table
 * @return true if the NdbAggregator program was built successfully
 */
static bool ndb_build_aggregation_program(
    const JOIN *join, const NdbDictionary::Table *root_ndb_tab,
    const NdbDictionary::Table *leaf_ndb_tab) {
  NdbAggregator agg(leaf_ndb_tab);

  // Map GROUP BY columns. For Phase 3 scope, all GROUP BY columns are
  // from the root (parent) table, so they use linked projection.
  Uint32 linked_pos = 0;
  for (ORDER *group = join->group_list.order; group != nullptr;
       group = group->next) {
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    const int col_id = field_item->field->field_index();
    const NdbDictionary::Column *parent_col =
        root_ndb_tab->getColumn(col_id);
    if (parent_col == nullptr ||
        !agg.GroupByLinked(linked_pos++, parent_col)) {
      return false;
    }
  }

  // Map aggregate functions.
  Uint32 agg_id = 0;
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    switch ((*func)->sum_func()) {
      case Item_sum::COUNT_FUNC: {
        // COUNT(*): load constant 1, then count.
        if (!agg.LoadUint64(1, kReg1) || !agg.Count(agg_id, kReg1)) {
          return false;
        }
        break;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        // SUM/MIN/MAX(leaf_column): load column, then aggregate.
        const auto *field_item =
            down_cast<const Item_field *>((*func)->arguments()[0]);
        const int col_id = field_item->field->field_index();

        if (!agg.LoadColumn(col_id, kReg1)) return false;
        switch ((*func)->sum_func()) {
          case Item_sum::SUM_FUNC:
            if (!agg.Sum(agg_id, kReg1)) return false;
            break;
          case Item_sum::MIN_FUNC:
            if (!agg.Min(agg_id, kReg1)) return false;
            break;
          case Item_sum::MAX_FUNC:
            if (!agg.Max(agg_id, kReg1)) return false;
            break;
          default:
            break;
        }
        break;
      }
      default:
        return false;
    }
    agg_id++;
  }

  if (!agg.Finalize()) return false;

  DBUG_PRINT("info",
             ("ndb_push_aggregation: built NdbAggregator program "
              "with %u GROUP BY columns and %u aggregates",
              linked_pos, agg_id));
  return true;
}

void ndb_push_aggregation(THD *, const JOIN *join,
                          const ndb_pushed_builder_ctx &builder) {
  // All tables in the builder must be part of a pushed join.
  // If any table is not pushed, MySQL still needs raw rows for joining.
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab == nullptr || tab->file->member_of_pushed_join() == nullptr) {
      return;
    }
  }

  if (!ndb_can_push_aggregation(join)) return;

  // Extract NDB table pointers (requires friend access to ha_ndbcluster).
  const auto *root_handler =
      down_cast<const ha_ndbcluster *>(builder.m_tables[0].get_table()->file);
  const uint leaf_idx = builder.m_table_count - 1;
  const auto *leaf_handler = down_cast<const ha_ndbcluster *>(
      builder.m_tables[leaf_idx].get_table()->file);

  // Build the NdbAggregator program to validate the translation.
  // Phase 3: program is built but not used for execution.
  ndb_build_aggregation_program(join, root_handler->m_table,
                                leaf_handler->m_table);
}
