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
 * Aggregation pushdown for NDB pushed joins and single-table queries.
 *
 * Contains all logic for detecting pushable aggregation queries,
 * building NdbAggregator programs, rebuilding the NdbQueryDef with
 * aggregation attached, and fetching aggregate results.
 */

#include "storage/ndb/plugin/ha_ndbcluster_push_agg.h"

#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_sum.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "storage/ndb/include/ndbapi/NdbAggregator.hpp"
#include "storage/ndb/include/ndbapi/NdbScanOperation.hpp"
#include "storage/ndb/plugin/ha_ndbcluster.h"

// Interpreter.hpp uses NONE as an enum value; ndb_opts.h defines it as a macro.
#ifdef NONE
#undef NONE
#endif
#include <kernel/Interpreter.hpp>
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
 * Check if a CASE expression is a pushable searched CASE with a single
 * WHEN/THEN/ELSE where the WHEN is a simple integer comparison and the
 * THEN/ELSE values are integer constants or field references.
 *
 * @param arg         The candidate CASE item
 * @param leaf_table  If non-null, require fields to belong to this table.
 *                    If null, require fields from a pushed-join table.
 * @return true if the CASE is pushable
 */
static bool is_pushable_case_expr(const Item *arg, const TABLE *leaf_table) {
  if (arg->type() != Item::FUNC_ITEM) return false;
  const auto *func = down_cast<const Item_func *>(arg);
  if (func->functype() != Item_func::CASE_FUNC) return false;
  const auto *case_item = down_cast<const Item_func_case *>(arg);

  // Must be searched CASE (no expression between CASE and WHEN).
  if (case_item->get_first_expr_num() >= 0) return false;

  // Must have an ELSE clause.
  if (case_item->get_else_expr_num() < 0) return false;

  // Compute ncases: argument_count minus optional first_expr/else entries.
  int ncases = static_cast<int>(case_item->argument_count());
  if (case_item->get_else_expr_num() >= 0) ncases--;
  // Require exactly one WHEN/THEN pair (ncases == 2).
  if (ncases != 2) return false;

  Item **args = case_item->arguments();
  const int else_idx = case_item->get_else_expr_num();

  // Validate WHEN condition: must be a simple comparison.
  Item *when_item = args[0];
  if (when_item->type() != Item::FUNC_ITEM) return false;
  const auto *when_func = down_cast<const Item_func *>(when_item);
  switch (when_func->functype()) {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GT_FUNC:
    case Item_func::GE_FUNC:
      break;
    default:
      return false;
  }
  if (when_func->argument_count() != 2) return false;
  Item *cmp_left = when_func->arguments()[0];
  Item *cmp_right = when_func->arguments()[1];

  // One side must be an integer field, the other an integer constant.
  const Item_field *field_item = nullptr;
  if (cmp_left->type() == Item::FIELD_ITEM &&
      cmp_right->type() == Item::INT_ITEM) {
    field_item = down_cast<const Item_field *>(cmp_left);
  } else if (cmp_left->type() == Item::INT_ITEM &&
             cmp_right->type() == Item::FIELD_ITEM) {
    field_item = down_cast<const Item_field *>(cmp_right);
  } else {
    return false;
  }
  if (!is_integer_type(field_item->field->type())) return false;

  // Validate field table membership.
  if (leaf_table != nullptr) {
    if (field_item->field->table != leaf_table) return false;
  } else {
    if (field_item->field->table->file->member_of_pushed_join() == nullptr)
      return false;
  }

  // Validate THEN and ELSE: must be integer constant or integer field.
  auto check_value_item = [&](Item *item) -> bool {
    if (item->type() == Item::INT_ITEM) return true;
    if (item->type() == Item::FIELD_ITEM) {
      const auto *fi = down_cast<const Item_field *>(item);
      if (!is_integer_type(fi->field->type())) return false;
      if (leaf_table != nullptr) {
        return fi->field->table == leaf_table;
      } else {
        return fi->field->table->file->member_of_pushed_join() != nullptr;
      }
    }
    return false;
  };

  if (!check_value_item(args[1])) return false;       // THEN
  if (!check_value_item(args[else_idx])) return false;  // ELSE

  return true;
}

/**
 * Map MySQL comparison functype to NDB interpreter BRANCH_XX_REG_REG opcode.
 * REG_REG opcodes are NOT inverted (unlike the NdbInterpretedCode API).
 */
static Uint32 get_branch_opcode(Item_func::Functype ft) {
  switch (ft) {
    case Item_func::EQ_FUNC:
      return Interpreter::BRANCH_EQ_REG_REG;
    case Item_func::NE_FUNC:
      return Interpreter::BRANCH_NE_REG_REG;
    case Item_func::LT_FUNC:
      return Interpreter::BRANCH_LT_REG_REG;
    case Item_func::LE_FUNC:
      return Interpreter::BRANCH_LE_REG_REG;
    case Item_func::GT_FUNC:
      return Interpreter::BRANCH_GT_REG_REG;
    case Item_func::GE_FUNC:
      return Interpreter::BRANCH_GE_REG_REG;
    default:
      return 0;
  }
}

/**
 * Emit an NdbAggregator program for a CASE aggregate expression.
 *
 * Builds an embedded interpreter program for the WHEN condition,
 * followed by aggregation arms for the THEN and ELSE paths.
 *
 * Pattern: SUM/MIN/MAX/COUNT(CASE WHEN col OP const THEN val ELSE val END)
 *
 * The embedded interpreter evaluates the condition and writes a skip offset
 * to interpreter output[0].  The aggregation program reads that offset to
 * select the THEN or ELSE arm.
 *
 * @param case_item  The CASE expression (already validated by is_pushable_case_expr)
 * @param ndb_table  The NDB table for column attribute lookups
 * @param agg        The NdbAggregator being built
 * @param agg_type   The aggregate function type (SUM, MIN, MAX, COUNT)
 * @param agg_id     The aggregate slot ID
 * @return true on success, false on failure
 */
static bool emit_case_aggregation(const Item_func_case *case_item,
                                  const NdbDictionary::Table *ndb_table,
                                  NdbAggregator *agg,
                                  Item_sum::Sumfunctype agg_type,
                                  Uint32 agg_id) {
  Item **args = case_item->arguments();
  const int else_idx = case_item->get_else_expr_num();

  // Extract WHEN comparison components.
  const auto *when_func = down_cast<const Item_func *>(args[0]);
  Item *cmp_left = when_func->arguments()[0];
  Item *cmp_right = when_func->arguments()[1];

  // Identify which comparison arg is the field and which is the constant.
  const Item_field *cmp_field;
  Item_int *cmp_const;
  bool field_on_left;
  if (cmp_left->type() == Item::FIELD_ITEM) {
    cmp_field = down_cast<const Item_field *>(cmp_left);
    cmp_const = down_cast<Item_int *>(cmp_right);
    field_on_left = true;
  } else {
    cmp_field = down_cast<const Item_field *>(cmp_right);
    cmp_const = down_cast<Item_int *>(cmp_left);
    field_on_left = false;
  }

  // Get NDB attribute ID for the comparison field.
  const NdbDictionary::Column *ndb_col =
      ndb_table->getColumn(cmp_field->field->field_index());
  if (ndb_col == nullptr) return false;
  const Uint32 attr_id = ndb_col->getAttrId();

  const longlong const_val = cmp_const->val_int();
  const bool use_const16 = (const_val >= 0 && const_val <= 65535);
  const Uint32 const_words = use_const16 ? 1 : 3;

  // Compute THEN/ELSE arm sizes.
  Item *then_item = args[1];
  Item *else_item = args[else_idx];
  const Uint32 then_load_words =
      (then_item->type() == Item::INT_ITEM) ? 3 : 1;  // LoadInt64 or LoadColumn
  const Uint32 else_load_words =
      (else_item->type() == Item::INT_ITEM) ? 3 : 1;
  const Uint32 then_arm_words = then_load_words + 1;  // load + agg op
  const Uint32 else_arm_words = else_load_words + 1;  // load + RepeatAgg
  const Uint32 else_skip_offset = then_arm_words + 1;  // THEN arm + Skip

  // Embedded interpreter layout:
  //   [0]              READ_ATTR_INTO_REG reg0, attr_id
  //   [1..const_words] LOAD_CONST16/64 reg1, const_val
  //   [1+const_words]  BRANCH_XX reg0/reg1, offset=4  → THEN
  //   ELSE path:
  //   [2+const_words]  LOAD_CONST16 reg2, else_skip_offset
  //   [3+const_words]  WRITE_INTERPRETER_OUTPUT reg2, 0
  //   [4+const_words]  EXIT_OK
  //   THEN path:
  //   [5+const_words]  LOAD_CONST16 reg2, 0
  //   [6+const_words]  WRITE_INTERPRETER_OUTPUT reg2, 0
  //   [7+const_words]  EXIT_OK
  const Uint32 emb_len = 8 + const_words;

  // Emit the embedded interpreter block.
  if (!agg->EmbeddedInterp(emb_len)) return false;

  // READ_ATTR_INTO_REG reg0, attr_id
  if (!agg->EmitEmbeddedWord(Interpreter::Read(attr_id, 0))) return false;

  // LOAD_CONST16/64 reg1, const_val
  if (use_const16) {
    if (!agg->EmitEmbeddedWord(
            Interpreter::LoadConst16(1, static_cast<Uint32>(const_val))))
      return false;
  } else {
    if (!agg->EmitEmbeddedWord(Interpreter::LoadConst64(1))) return false;
    // Emit the 64-bit constant value as 2 words.
    Int64 val64 = static_cast<Int64>(const_val);
    Uint32 data[2];
    memcpy(data, &val64, sizeof(Int64));
    if (!agg->EmitEmbeddedWord(data[0])) return false;
    if (!agg->EmitEmbeddedWord(data[1])) return false;
  }

  // BRANCH_XX_REG_REG with offset=4 (always 4, see layout above).
  // If field is on the left: left_reg=reg0(col), right_reg=reg1(const).
  // If field is on the right: left_reg=reg1(const), right_reg=reg0(col).
  const Uint32 branch_opcode = get_branch_opcode(when_func->functype());
  const Uint32 left_reg = field_on_left ? 0 : 1;
  const Uint32 right_reg = field_on_left ? 1 : 0;
  if (!agg->EmitEmbeddedWord(branch_opcode | (left_reg << 6) |
                             (right_reg << 9) | (4 << 16)))
    return false;

  // ELSE path: skip_offset = else_skip_offset (skip past THEN arm + Skip).
  if (!agg->EmitEmbeddedWord(
          Interpreter::LoadConst16(2, else_skip_offset)))
    return false;
  if (!agg->EmitEmbeddedWord(Interpreter::WriteInterpreterOutput(2, 0)))
    return false;
  if (!agg->EmitEmbeddedWord(Interpreter::ExitOK())) return false;

  // THEN path: skip_offset = 0 (land at start of agg arms).
  if (!agg->EmitEmbeddedWord(Interpreter::LoadConst16(2, 0))) return false;
  if (!agg->EmitEmbeddedWord(Interpreter::WriteInterpreterOutput(2, 0)))
    return false;
  if (!agg->EmitEmbeddedWord(Interpreter::ExitOK())) return false;

  // Emit aggregation arms.
  // THEN arm: load value, aggregate, skip past ELSE.
  auto emit_load = [&](Item *item) -> bool {
    if (item->type() == Item::INT_ITEM) {
      return agg->LoadInt64(item->val_int(), kReg1);
    } else {
      const auto *fi = down_cast<const Item_field *>(item);
      const NdbDictionary::Column *col =
          ndb_table->getColumn(fi->field->field_index());
      if (col == nullptr) return false;
      return agg->LoadColumn(col->getAttrId(), kReg1);
    }
  };

  if (!emit_load(then_item)) return false;
  switch (agg_type) {
    case Item_sum::SUM_FUNC:
      if (!agg->Sum(agg_id, kReg1)) return false;
      break;
    case Item_sum::MIN_FUNC:
      if (!agg->Min(agg_id, kReg1)) return false;
      break;
    case Item_sum::MAX_FUNC:
      if (!agg->Max(agg_id, kReg1)) return false;
      break;
    case Item_sum::COUNT_FUNC:
      if (!agg->Count(agg_id, kReg1)) return false;
      break;
    default:
      return false;
  }
  if (!agg->Skip(else_arm_words)) return false;

  // ELSE arm: load value, repeat aggregate.
  if (!emit_load(else_item)) return false;
  if (!agg->RepeatAgg(agg_id, kReg1)) return false;

  return true;
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
        // COUNT(*) and COUNT(column) are both pushable.
        // COUNT(column) correctly skips NULLs — the data node's Count()
        // function (AggInterpreter.cpp) checks a.is_null and returns
        // without incrementing.
        // COUNT(CASE ...) is pushable if the CASE is a valid pattern.
        if ((*func)->argument_count() == 1) {
          Item *arg = (*func)->arguments()[0];
          if (arg->type() == Item::FIELD_ITEM) {
            const auto *field_item = down_cast<const Item_field *>(arg);
            if (field_item->field->table->file->member_of_pushed_join() ==
                nullptr) {
              return false;
            }
          } else if (arg->type() == Item::FUNC_ITEM) {
            if (!is_pushable_case_expr(arg, nullptr)) return false;
          }
        }
        break;
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        // Must have exactly one argument: a field reference from a pushed
        // table, or a pushable CASE expression.
        if ((*func)->argument_count() != 1) return false;
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          const auto *field_item = down_cast<const Item_field *>(arg);
          if (field_item->field->table->file->member_of_pushed_join() ==
              nullptr) {
            return false;
          }
        } else if (!is_pushable_case_expr(arg, nullptr)) {
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
        // COUNT(*) has args[0] as Item_int(0); COUNT(column) has Item_field.
        // COUNT(CASE ...) has args[0] as Item_func_case.
        Item *count_arg = (*func)->arguments()[0];
        if (count_arg->type() == Item::FIELD_ITEM) {
          // COUNT(column): load the column so Count() can skip NULLs.
          const auto *field_item =
              down_cast<const Item_field *>(count_arg);
          const uint count_tab_idx =
              find_table_index(tables, table_count, field_item->field->table);
          if (count_tab_idx != leaf_tab_no) {
            DBUG_PRINT("info",
                       ("ndb_push_aggregation: COUNT(column) source on "
                        "table %u, leaf is %u — cannot push",
                        count_tab_idx, leaf_tab_no));
            delete agg;
            return nullptr;
          }
          const int col_id = field_item->field->field_index();
          if (!agg->LoadColumn(col_id, kReg1) ||
              !agg->Count(agg_id, kReg1)) {
            delete agg;
            return nullptr;
          }
        } else if (count_arg->type() == Item::FUNC_ITEM) {
          const auto *case_item =
              down_cast<const Item_func_case *>(count_arg);
          if (!emit_case_aggregation(case_item, leaf_ndb_tab, agg,
                                     (*func)->sum_func(), agg_id)) {
            delete agg;
            return nullptr;
          }
        } else {
          // COUNT(*): load constant 1, count always increments.
          if (!agg->LoadUint64(1, kReg1) || !agg->Count(agg_id, kReg1)) {
            delete agg;
            return nullptr;
          }
        }
        break;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          // SUM/MIN/MAX(column): load column, then aggregate.
          // The column must be from the leaf table, since LoadColumn()
          // operates on the leaf table's column namespace.
          const auto *field_item = down_cast<const Item_field *>(arg);
          const uint agg_tab_idx =
              find_table_index(tables, table_count, field_item->field->table);
          if (agg_tab_idx != leaf_tab_no) {
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
        } else if (arg->type() == Item::FUNC_ITEM) {
          const auto *case_item = down_cast<const Item_func_case *>(arg);
          if (!emit_case_aggregation(case_item, leaf_ndb_tab, agg,
                                     (*func)->sum_func(), agg_id)) {
            delete agg;
            return nullptr;
          }
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

  // Reject aggregation with outer/anti/semi joins.
  // Aggregation runs in DBLQH on the leaf table via handleJoinAggRow.
  // For outer joins, when the inner-side table has no match, DBSPJ produces
  // a NULL-extended row at the coordinator level that DBLQH never sees.
  // This causes incorrect COUNT(*) and missing groups.
  {
    const pushed_table &root = builder.m_tables[0];
    for (uint i = 1; i < builder.m_table_count; i++) {
      if (!builder.m_join_scope.contain(i)) continue;
      const pushed_table &tab = builder.m_tables[i];
      if (tab.isOuterJoined(root) || tab.isSemiJoined(root) ||
          tab.isAntiJoined(root)) {
        DBUG_PRINT("info",
                   ("ndb_push_aggregation: table %u has outer/semi/anti join "
                    "— cannot push aggregation",
                    i));
        return false;
      }
    }
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
  return ndb_fetch_next_aggregate_row(agg, handler->m_agg_join);
}

/**
 * Check if a single-table query's aggregate functions and GROUP BY
 * columns are pushable.
 *
 * Validates:
 * - Query has aggregate functions (COUNT/SUM/MIN/MAX, no DISTINCT)
 * - No ROLLUP
 * - GROUP BY columns are simple field references from the single table
 * - Aggregate arguments are field references from the single table
 *
 * @param join        MySQL JOIN with aggregation info
 * @param table       The single MySQL TABLE involved
 * @return true if aggregation is pushable
 */
static bool ndb_can_push_single_table_aggregation(const JOIN *join,
                                                   const TABLE *table) {
  if (join->sum_funcs == nullptr || join->sum_funcs[0] == nullptr) {
    return false;
  }

  if (join->query_block->olap != UNSPECIFIED_OLAP_TYPE) {
    return false;
  }

  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    switch ((*func)->sum_func()) {
      case Item_sum::COUNT_FUNC:
        if ((*func)->argument_count() == 1) {
          Item *arg = (*func)->arguments()[0];
          if (arg->type() == Item::FIELD_ITEM) {
            const auto *field_item = down_cast<const Item_field *>(arg);
            if (field_item->field->table != table) {
              return false;
            }
          } else if (arg->type() == Item::FUNC_ITEM) {
            if (!is_pushable_case_expr(arg, table)) return false;
          }
        }
        break;
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        if ((*func)->argument_count() != 1) return false;
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          const auto *field_item = down_cast<const Item_field *>(arg);
          if (field_item->field->table != table) {
            return false;
          }
        } else if (!is_pushable_case_expr(arg, table)) {
          return false;
        }
        break;
      }
      default:
        return false;
    }
  }

  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
       group = group->next) {
    Item *item = *(group->item);
    if (item->type() != Item::FIELD_ITEM) return false;
    const auto *field_item = down_cast<const Item_field *>(item);
    if (field_item->field->table != table) {
      return false;
    }
  }

  return true;
}

/**
 * Build an NdbAggregator program for a single-table aggregate query.
 *
 * All GROUP BY columns and aggregate arguments are on the same table,
 * so no linked projections are needed.
 *
 * @param join      MySQL JOIN with aggregation info
 * @param ndb_table The NDB table definition
 * @return heap-allocated NdbAggregator on success, nullptr on failure
 */
static NdbAggregator *ndb_build_stm_aggregation_program(
    const JOIN *join, const NdbDictionary::Table *ndb_table) {
  NdbAggregator *agg = new NdbAggregator(ndb_table);

  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
       group = group->next) {
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    const int col_id = field_item->field->field_index();
    if (!agg->GroupBy(col_id)) {
      delete agg;
      return nullptr;
    }
  }

  Uint32 agg_id = 0;
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    switch ((*func)->sum_func()) {
      case Item_sum::COUNT_FUNC: {
        Item *count_arg = (*func)->arguments()[0];
        if (count_arg->type() == Item::FIELD_ITEM) {
          const auto *field_item =
              down_cast<const Item_field *>(count_arg);
          const int col_id = field_item->field->field_index();
          if (!agg->LoadColumn(col_id, kReg1) ||
              !agg->Count(agg_id, kReg1)) {
            delete agg;
            return nullptr;
          }
        } else if (count_arg->type() == Item::FUNC_ITEM) {
          const auto *case_item =
              down_cast<const Item_func_case *>(count_arg);
          if (!emit_case_aggregation(case_item, ndb_table, agg,
                                     (*func)->sum_func(), agg_id)) {
            delete agg;
            return nullptr;
          }
        } else {
          if (!agg->LoadUint64(1, kReg1) || !agg->Count(agg_id, kReg1)) {
            delete agg;
            return nullptr;
          }
        }
        break;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          const auto *field_item = down_cast<const Item_field *>(arg);
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
        } else if (arg->type() == Item::FUNC_ITEM) {
          const auto *case_item = down_cast<const Item_func_case *>(arg);
          if (!emit_case_aggregation(case_item, ndb_table, agg,
                                     (*func)->sum_func(), agg_id)) {
            delete agg;
            return nullptr;
          }
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
             ("ndb_build_stm_aggregation_program: built program "
              "with %u aggregates",
              agg_id));
  return agg;
}

bool ndb_push_single_table_aggregation(THD *, const JOIN *join,
                                       const ndb_pushed_builder_ctx &builder) {
  if (builder.m_table_count != 1) {
    return false;
  }

  const TABLE *table = builder.m_tables[0].get_table();
  if (table == nullptr) {
    return false;
  }

  if (!ndb_can_push_single_table_aggregation(join, table)) {
    return false;
  }

  const auto *h = down_cast<const ha_ndbcluster *>(table->file);
  const NdbDictionary::Table *ndb_table = h->m_table;
  if (ndb_table == nullptr) {
    return false;
  }

  NdbAggregator *agg = ndb_build_stm_aggregation_program(join, ndb_table);
  if (agg == nullptr) {
    return false;
  }

  auto *handler = down_cast<ha_ndbcluster *>(table->file);
  handler->m_stm_aggregator = agg;

  DBUG_PRINT("info",
             ("ndb_push_single_table_aggregation: aggregation pushed "
              "on table %s",
              table->s->table_name.str));
  return true;
}

int ndb_start_stm_aggregate_scan(ha_ndbcluster *handler,
                                 NdbScanOperation *op) {
  // Aggregation code was attached via ScanOptions::SO_AGGREGATION
  // before scanTable(). DoAggregation() executes the scan, drains
  // all fragments, and merges per-fragment results API-side.
  if (op->DoAggregation() != 0) {
    return ndb_to_mysql_error(&op->getNdbError());
  }
  const int res = ndb_fetch_next_aggregate_row(handler->m_stm_aggregator,
                                               handler->m_agg_join);
  if (res == NdbQuery::NextResult_scanComplete) {
    return HA_ERR_END_OF_FILE;
  }
  return res;
}

int ndb_fetch_stm_aggregate(ha_ndbcluster *handler) {
  const int res = ndb_fetch_next_aggregate_row(handler->m_stm_aggregator,
                                               handler->m_agg_join);
  if (res == NdbQuery::NextResult_scanComplete) {
    return HA_ERR_END_OF_FILE;
  }
  return res;
}
