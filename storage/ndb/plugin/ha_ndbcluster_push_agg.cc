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
 * building NdbAggregator programs, rebuilding the NdbQueryDef with
 * aggregation attached, and fetching aggregate results.
 */

#include "storage/ndb/plugin/ha_ndbcluster_push_agg.h"

#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "storage/ndb/include/ndbapi/NdbAggregator.hpp"
#include "storage/ndb/plugin/ha_ndbcluster.h"
#include "storage/ndb/plugin/ha_ndbcluster_push.h"
#include "storage/ndb/src/ndbapi/NdbQueryBuilder.hpp"
#include "storage/ndb/src/ndbapi/NdbQueryOperation.hpp"

/**
 * When aggregation is pushed, walk down through NESTED_LOOP_JOINs whose
 * inner side is a pushed-join child and return the outermost 'outer' path
 * that is not such a join — i.e., the root table scan.
 */
AccessPath *strip_pushed_child_nljs(AccessPath *path) {
  while (path->type == AccessPath::NESTED_LOOP_JOIN) {
    const TABLE *inner_table =
        GetBasicTable(path->nested_loop_join().inner);
    if (inner_table != nullptr &&
        inner_table->file->member_of_pushed_join() != nullptr &&
        inner_table->file->member_of_pushed_join() != inner_table) {
      path = path->nested_loop_join().outer;
    } else {
      break;
    }
  }
  return path;
}

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
        // COUNT(*) is always pushable.
        // COUNT(column) where column is nullable cannot be pushed yet
        // because the data node counts all rows without NULL checking.
        if ((*func)->argument_count() == 1) {
          Item *arg = (*func)->arguments()[0];
          if (arg->type() == Item::FIELD_ITEM && arg->is_nullable()) {
            return false;
          }
        }
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
  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
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
 * Find the table index in the tables array for a given MySQL TABLE*.
 * Returns the matching index, or table_count if not found.
 */
static uint find_table_index(const pushed_table *tables, uint table_count,
                             const TABLE *mysql_table) {
  for (uint i = 0; i < table_count; i++) {
    if (tables[i].get_table() == mysql_table) return i;
  }
  return table_count;
}

/**
 * Build an NdbAggregator program from the MySQL query plan.
 *
 * Supports multi-table GROUP BY: columns from the leaf table use GroupBy(),
 * columns from parent tables use GroupByLinked() with linked projections.
 * Aggregate arguments (SUM/MIN/MAX) must be from the leaf table.
 *
 * @param join           MySQL JOIN containing aggregation info
 * @param tables         The pushed table array
 * @param table_count    Number of tables in the array
 * @param leaf_tab_no    Table index of the leaf table
 * @param ndb_tables     Array of NDB table pointers indexed by table position
 * @return heap-allocated NdbAggregator on success, nullptr on failure
 */
static NdbAggregator *ndb_build_aggregation_program(
    const JOIN *join, const pushed_table *tables, uint table_count,
    uint leaf_tab_no, const NdbDictionary::Table *const *ndb_tables) {
  const NdbDictionary::Table *leaf_ndb_tab = ndb_tables[leaf_tab_no];
  NdbAggregator *agg = new NdbAggregator(leaf_ndb_tab);

  // Map GROUP BY columns. Columns from parent tables use GroupByLinked(),
  // columns from the leaf table use GroupBy().
  Uint32 linked_pos = 0;
  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
       group = group->next) {
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    const int col_id = field_item->field->field_index();
    const uint tab_idx =
        find_table_index(tables, table_count, field_item->field->table);
    if (tab_idx == leaf_tab_no) {
      // Leaf table column — direct GROUP BY.
      if (!agg->GroupBy(col_id)) {
        delete agg;
        return nullptr;
      }
    } else {
      // Parent table column — linked GROUP BY.
      const NdbDictionary::Column *parent_col =
          ndb_tables[tab_idx]->getColumn(col_id);
      if (parent_col == nullptr ||
          !agg->GroupByLinked(linked_pos++, parent_col)) {
        delete agg;
        return nullptr;
      }
    }
  }

  // Map aggregate functions.
  Uint32 agg_id = 0;
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    switch ((*func)->sum_func()) {
      case Item_sum::COUNT_FUNC: {
        // COUNT(*): load constant 1, then count.
        if (!agg->LoadUint64(1, kReg1) || !agg->Count(agg_id, kReg1)) {
          delete agg;
          return nullptr;
        }
        break;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        // SUM/MIN/MAX(column): load column, then aggregate.
        // The column must be from the leaf table, since LoadColumn()
        // operates on the leaf table's column namespace.
        const auto *field_item =
            down_cast<const Item_field *>((*func)->arguments()[0]);
        const uint agg_tab_idx =
            find_table_index(tables, table_count, field_item->field->table);
        if (agg_tab_idx != leaf_tab_no) {
          // Aggregate source column not on leaf table — can't push.
          DBUG_PRINT("info",
                     ("ndb_push_aggregation: aggregate source on table %u, "
                      "leaf is %u — cannot push",
                      agg_tab_idx, leaf_tab_no));
          delete agg;
          return nullptr;
        }
        const int col_id = field_item->field->field_index();

        if (!agg->LoadColumn(col_id, kReg1)) {
          delete agg;
          return nullptr;
        }
        switch ((*func)->sum_func()) {
          case Item_sum::SUM_FUNC:
            if (!agg->Sum(agg_id, kReg1)) {
              delete agg;
              return nullptr;
            }
            break;
          case Item_sum::MIN_FUNC:
            if (!agg->Min(agg_id, kReg1)) {
              delete agg;
              return nullptr;
            }
            break;
          case Item_sum::MAX_FUNC:
            if (!agg->Max(agg_id, kReg1)) {
              delete agg;
              return nullptr;
            }
            break;
          default:
            break;
        }
        break;
      }
      default:
        delete agg;
        return nullptr;
    }
    agg_id++;
  }

  if (!agg->Finalize()) {
    delete agg;
    return nullptr;
  }

  DBUG_PRINT("info",
             ("ndb_push_aggregation: built NdbAggregator program "
              "with %u linked GROUP BY columns and %u aggregates",
              linked_pos, agg_id));
  return agg;
}

/**
 * Apply aggregation options to the leaf table during build_query().
 *
 * Called from build_query() when m_aggregator is set. For the leaf table
 * (last in join scope), calls setAggregation() on its NdbQueryOptions and
 * adds linked projections for GROUP BY columns from parent tables.
 *
 * This function is a friend of ndb_pushed_builder_ctx.
 */
void ndb_apply_aggregation_options(ndb_pushed_builder_ctx &builder,
                                   uint tab_no, NdbQueryOptions *options) {
  // Only apply to the leaf table (last in join scope).
  uint leaf_tab_no = 0;
  for (uint t = 0; t < builder.m_table_count; t++) {
    if (builder.m_join_scope.contain(t)) leaf_tab_no = t;
  }
  if (tab_no != leaf_tab_no) return;

  options->setAggregation(*builder.m_aggregator);

  // Add linked projections for GROUP BY columns from parent tables.
  const JOIN *join = builder.m_join;
  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
       group = group->next) {
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    const uint tab_idx = find_table_index(
        builder.m_tables, builder.m_table_count, field_item->field->table);

    if (tab_idx != leaf_tab_no) {
      // Parent column — create linked projection.
      const NdbQueryOperationDef *parent_op = builder.m_tables[tab_idx].m_op;
      const NdbLinkedOperand *linked = builder.m_builder->linkedValue(
          parent_op, field_item->original_field_name());
      options->addLinkedProjection(linked);
    }
  }
}

bool ndb_push_aggregation(THD *, const JOIN *join,
                          ndb_pushed_builder_ctx &builder) {
  // All tables in the builder must be part of a pushed join.
  // If any table is not pushed, MySQL still needs raw rows for joining.
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab == nullptr || tab->file->member_of_pushed_join() == nullptr) {
      return false;
    }
  }

  if (!ndb_can_push_aggregation(join)) {
    return false;
  }

  // Find the leaf table (last in join scope).
  uint leaf_tab_no = 0;
  for (uint t = 0; t < builder.m_table_count; t++) {
    if (builder.m_join_scope.contain(t)) leaf_tab_no = t;
  }

  // Extract NDB table pointers (friend access to ha_ndbcluster::m_table).
  const NdbDictionary::Table *ndb_tables[MAX_TABLES];
  for (uint i = 0; i < builder.m_table_count; i++) {
    const auto *h = down_cast<const ha_ndbcluster *>(
        builder.m_tables[i].get_table()->file);
    ndb_tables[i] = h->m_table;
  }

  // Build the NdbAggregator program on the heap.
  NdbAggregator *agg = ndb_build_aggregation_program(
      join, builder.m_tables, builder.m_table_count, leaf_tab_no,
      ndb_tables);
  if (agg == nullptr) return false;

  // Store the aggregator on the builder for build_query() to use.
  builder.m_aggregator = agg;

  // Force chain topology for the aggregation rebuild.
  // The optimizer may create flat trees (e.g., root with 2 direct children)
  // when join keys are transitive. In aggregate mode, "orphan" leaf nodes
  // (non-aggregate leaves with no children) cause DBSPJ issues because they
  // have no projection and no aggregate role.  Force each non-root table's
  // parent to be the immediately preceding table in the join scope, creating
  // a chain: root → t1 → t2 → ... → leaf.  The linked key operands still
  // reference the original table (possibly a grandparent), and DBSPJ handles
  // grandparent references correctly via level counting.
  // Must also recompute m_ancestors to match the new parents.
  {
    const uint root_no = builder.m_join_root->get_table_no();
    uint prev_tab_no = root_no;
    for (uint t = root_no + 1; t < builder.m_table_count; t++) {
      if (builder.m_join_scope.contain(t)) {
        builder.m_tables[t].m_parent = prev_tab_no;
        // Recompute ancestors: parent's ancestors + parent itself
        builder.m_tables[t].m_ancestors =
            builder.m_tables[prev_tab_no].m_ancestors;
        builder.m_tables[t].m_ancestors.add(prev_tab_no);
        prev_tab_no = t;
      }
    }
  }

  // Rebuild the query with aggregation attached. build_query() will call
  // ndb_apply_aggregation_options() for the leaf table, which calls
  // setAggregation() and addLinkedProjection() on its NdbQueryOptions.
  const int error = builder.build_query();
  if (unlikely(error)) {
    delete agg;
    builder.m_aggregator = nullptr;
    return false;
  }

  const NdbQueryDef *const query_def =
      builder.m_builder->prepare(get_thd_ndb(builder.m_thd)->ndb);
  if (unlikely(query_def == nullptr)) {
    delete agg;
    builder.m_aggregator = nullptr;
    return false;
  }

  // Verify the query tree forms a chain (root → ... → leaf).
  // Aggregation pushdown requires each node to have at most 1 child.
  // Flat trees (e.g., root with 2 children) have sibling branches that
  // produce leaf nodes with no projection and no aggregate role, which
  // DBSPJ cannot handle in aggregate mode.
  for (Uint32 i = 0; i < query_def->getNoOfOperations(); i++) {
    const NdbQueryOperationDef *op = query_def->getQueryOperation(i);
    if (op->getNoOfChildOperations() > 1) {
      DBUG_PRINT("info",
                 ("ndb_push_aggregation: op %u has %u children, "
                  "not a chain — cannot push",
                  i, op->getNoOfChildOperations()));
      query_def->destroy();
      delete agg;
      builder.m_aggregator = nullptr;
      return false;
    }
  }

  // Create a new ndb_pushed_join with the aggregation-enabled query def.
  const ndb_pushed_join *new_pushed_join =
      new ndb_pushed_join(builder, query_def);
  if (unlikely(new_pushed_join == nullptr)) {
    delete agg;
    builder.m_aggregator = nullptr;
    return false;
  }

  // Replace the old pushed join on all handlers.
  const ndb_pushed_join *old_pushed_join = nullptr;
  for (uint i = 0; i < new_pushed_join->get_operation_count(); i++) {
    const TABLE *const tab = new_pushed_join->get_table(i);
    ha_ndbcluster *child = down_cast<ha_ndbcluster *>(tab->file);
    if (old_pushed_join == nullptr) {
      old_pushed_join = child->m_pushed_join_member;
    }
    child->m_pushed_join_member = new_pushed_join;
    child->m_pushed_join_operation = i;
  }
  delete old_pushed_join;

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
                                      bool force_send) {
  // Drain the scan — aggregate results accumulate in the NdbAggregator
  // as TRANSID_AI data arrives during nextResult() calls.
  NdbQuery::NextResultOutcome result;
  while ((result = pushed_op->nextResult(true, force_send)) ==
         NdbQuery::NextResult_gotRow) {
  }
  if (unlikely(result != NdbQuery::NextResult_scanComplete)) {
    return HA_ERR_INTERNAL_ERROR;
  }

  // PrepareResults() is already called by NdbQueryImpl::processAggResults()
  // when the scan reaches EndOfData.  Do NOT call it again here — a second
  // call would reset the result iterator and lose accumulated groups.
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
  // Temporarily mark all columns writable — the aggregate pushdown path
  // writes GROUP BY columns that may not be in the normal write_set.
  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
       group = group->next) {
    NdbAggregator::Column col = rec.FetchGroupbyColumn();
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    Field *field = field_item->field;
    my_bitmap_map *old_map =
        dbug_tmp_use_all_columns(field->table, field->table->write_set);
    const int err = store_group_column(col, field);
    dbug_tmp_restore_column_map(field->table->write_set, old_map);
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
  if (agg == nullptr) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (!handler->m_agg_results_initialized) {
    const int err = ndb_init_aggregate_results(
        handler->m_pushed_operation, handler->m_thd_ndb->m_force_send);
    if (err) return err;
    handler->m_agg_results_initialized = true;
  }
  return ndb_fetch_next_aggregate_row(agg, handler->m_pushed_agg_join);
}
