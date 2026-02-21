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
#include "storage/ndb/src/ndbapi/NdbQueryOperation.hpp"

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

bool ndb_push_aggregation(THD *, const JOIN *join,
                          const ndb_pushed_builder_ctx &builder) {
  // All tables in the builder must be part of a pushed join.
  // If any table is not pushed, MySQL still needs raw rows for joining.
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab == nullptr || tab->file->member_of_pushed_join() == nullptr) {
      return false;
    }
  }

  if (!ndb_can_push_aggregation(join)) return false;

  // Extract NDB table pointers (requires friend access to ha_ndbcluster).
  const auto *root_handler =
      down_cast<const ha_ndbcluster *>(builder.m_tables[0].get_table()->file);
  const uint leaf_idx = builder.m_table_count - 1;
  const auto *leaf_handler = down_cast<const ha_ndbcluster *>(
      builder.m_tables[leaf_idx].get_table()->file);

  // Build the NdbAggregator program from the MySQL query plan.
  if (!ndb_build_aggregation_program(join, root_handler->m_table,
                                     leaf_handler->m_table)) {
    return false;
  }

  DBUG_PRINT("info", ("ndb_push_aggregation: aggregation pushed successfully"));
  return true;
}

/**
 * Store a GROUP BY column value from the NdbAggregator result into
 * the corresponding MySQL Field.
 *
 * Handles integer, float/double, char/varchar, and date/time types.
 * The NDB attribute data format matches MySQL's record buffer format
 * for date/time types (both originate from the MySQL→NDB storage path),
 * so those use direct memcpy to the field position.
 *
 * @return 0 on success, error code on unsupported type
 */
static int store_group_column(NdbAggregator::Column &col, Field *field) {
  if (col.is_null()) {
    field->set_null();
    return 0;
  }
  field->set_notnull();
  switch (col.type()) {
    // Integer types — use typed accessors.
    case NdbDictionary::Column::Tinyint:
      field->store(col.data_int8(), false);
      break;
    case NdbDictionary::Column::Tinyunsigned:
      field->store(col.data_uint8(), true);
      break;
    case NdbDictionary::Column::Smallint:
      field->store(col.data_int16(), false);
      break;
    case NdbDictionary::Column::Smallunsigned:
      field->store(col.data_uint16(), true);
      break;
    case NdbDictionary::Column::Mediumint:
      field->store(col.data_medium(), false);
      break;
    case NdbDictionary::Column::Mediumunsigned:
      field->store(col.data_umedium(), true);
      break;
    case NdbDictionary::Column::Int:
      field->store(col.data_int32(), false);
      break;
    case NdbDictionary::Column::Unsigned:
      field->store(col.data_uint32(), true);
      break;
    case NdbDictionary::Column::Bigint:
      field->store(col.data_int64(), false);
      break;
    case NdbDictionary::Column::Bigunsigned:
      field->store(col.data_uint64(), true);
      break;

    // Floating-point types.
    case NdbDictionary::Column::Float:
      field->store(static_cast<double>(col.data_float()));
      break;
    case NdbDictionary::Column::Double:
      field->store(col.data_double());
      break;

    // Fixed-length string/binary — raw character data, no length prefix.
    case NdbDictionary::Column::Char:
    case NdbDictionary::Column::Binary:
      field->store(col.data(), col.byte_size(), field->charset());
      break;

    // Short variable-length — 1-byte length prefix.
    case NdbDictionary::Column::Varchar:
    case NdbDictionary::Column::Varbinary: {
      const uchar *data = reinterpret_cast<const uchar *>(col.data());
      const uint len = data[0];
      field->store(reinterpret_cast<const char *>(data + 1), len,
                   field->charset());
      break;
    }

    // Long variable-length — 2-byte little-endian length prefix.
    case NdbDictionary::Column::Longvarchar:
    case NdbDictionary::Column::Longvarbinary: {
      const uchar *data = reinterpret_cast<const uchar *>(col.data());
      const uint len = data[0] | (static_cast<uint>(data[1]) << 8);
      field->store(reinterpret_cast<const char *>(data + 2), len,
                   field->charset());
      break;
    }

    // Date/time types — NDB wire format matches MySQL record format.
    case NdbDictionary::Column::Date:
    case NdbDictionary::Column::Time:
    case NdbDictionary::Column::Time2:
    case NdbDictionary::Column::Datetime:
    case NdbDictionary::Column::Datetime2:
    case NdbDictionary::Column::Timestamp:
    case NdbDictionary::Column::Timestamp2:
    case NdbDictionary::Column::Year:
      memcpy(field->field_ptr(), col.data(), col.byte_size());
      break;

    default:
      return HA_ERR_UNSUPPORTED;
  }
  return 0;
}

/**
 * Drain the pushed scan to completion and prepare aggregate results.
 *
 * Calls nextResult() until the scan completes, then calls PrepareResults()
 * on the NdbAggregator to finalize the group-by hash map for iteration.
 *
 * @return 0 on success, error code on failure
 */
static int ndb_init_aggregate_results(NdbQueryOperation *pushed_op,
                                      bool force_send,
                                      NdbAggregator *agg) {
  // Drain the scan — aggregate results accumulate in the NdbAggregator
  // as TRANSID_AI data arrives during nextResult() calls.
  NdbQuery::NextResultOutcome result;
  while ((result = pushed_op->nextResult(true, force_send)) ==
         NdbQuery::NextResult_gotRow) {
  }
  if (unlikely(result != NdbQuery::NextResult_scanComplete)) {
    return HA_ERR_INTERNAL_ERROR;
  }

  agg->PrepareResults();
  return 0;
}

/**
 * Fetch the next aggregate result row from the NdbAggregator.
 *
 * Populates the MySQL row buffer with GROUP BY column values and
 * sets Item_sum pushed values for aggregate results.
 *
 * @return 0 (NextResult_gotRow) for row found,
 *         NextResult_scanComplete when done, or error code
 */
static int ndb_fetch_next_aggregate_row(NdbAggregator *agg,
                                        const JOIN *join) {
  NdbAggregator::ResultRecord rec = agg->FetchResultRecord();
  if (rec.end()) {
    return NdbQuery::NextResult_scanComplete;
  }

  // Populate GROUP BY columns in MySQL's row buffer.
  for (ORDER *group = join->group_list.order; group != nullptr;
       group = group->next) {
    NdbAggregator::Column col = rec.FetchGroupbyColumn();
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    const int err = store_group_column(col, field_item->field);
    if (err) return err;
  }

  // Set pushed values on Item_sum aggregate functions.
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    NdbAggregator::Result res = rec.FetchAggregationResult();
    if (res.is_null()) {
      (*func)->set_pushed_null();
    } else {
      switch (res.type()) {
        case NdbDictionary::Column::Bigint:
          (*func)->set_pushed_value_int(res.data_int64());
          break;
        case NdbDictionary::Column::Bigunsigned:
          (*func)->set_pushed_value_int(
              static_cast<int64_t>(res.data_uint64()));
          break;
        case NdbDictionary::Column::Double:
          (*func)->set_pushed_value_double(res.data_double());
          break;
        default:
          return HA_ERR_INTERNAL_ERROR;
      }
    }
  }

  return NdbQuery::NextResult_gotRow;
}

int ndb_fetch_pushed_aggregate(ha_ndbcluster *handler) {
  NdbAggregator *agg = handler->m_active_query->getAggregator();
  if (agg == nullptr) return HA_ERR_INTERNAL_ERROR;

  if (!handler->m_agg_results_initialized) {
    const int err = ndb_init_aggregate_results(
        handler->m_pushed_operation, handler->m_thd_ndb->m_force_send, agg);
    if (err) return err;
    handler->m_agg_results_initialized = true;
  }
  return ndb_fetch_next_aggregate_row(agg, handler->m_pushed_agg_join);
}
