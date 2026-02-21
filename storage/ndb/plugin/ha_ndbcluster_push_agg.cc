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

  DBUG_PRINT("info", ("ndb_push_aggregation: aggregation is pushable"));

  // Phase 2: Detection succeeded.
  // Phase 3+ will build the NdbAggregator program and modify AccessPath.
}
