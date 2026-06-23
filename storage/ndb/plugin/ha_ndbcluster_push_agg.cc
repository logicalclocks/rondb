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

// AggBuildContext is defined in ha_ndbcluster_push.h

// Forward declaration — defined further down, but needed by emit_expr().
static uint find_table_index(const pushed_table *tables, uint table_count,
                             const TABLE *mysql_table);

/**
 * Unwrap transparent AccessPath nodes (FILTER, etc.) to find the
 * underlying basic table path.  GetBasicTable() only handles leaf
 * path types — this helper looks through wrapper nodes first.
 */
static const TABLE *get_inner_table(const AccessPath *path) {
  while (path != nullptr) {
    if (path->type == AccessPath::FILTER) {
      path = path->filter().child;
    } else {
      return GetBasicTable(path);
    }
  }
  return nullptr;
}

/**
 * When aggregation is pushed, walk down through NESTED_LOOP_JOINs whose
 * inner side is a pushed-join child and return the outermost 'outer' path
 * that is not such a join — i.e., the root table scan.
 */
AccessPath *strip_pushed_child_nljs(AccessPath *path) {
  while (path->type == AccessPath::NESTED_LOOP_JOIN) {
    const TABLE *inner_table =
        get_inner_table(path->nested_loop_join().inner);
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
 * Check if a field belongs to the correct table for pushdown.
 */
static bool check_field_table(const Item_field *fi, const TABLE *leaf_table) {
  if (leaf_table != nullptr) {
    return fi->field->table == leaf_table;
  } else {
    return fi->field->table->file->member_of_pushed_join() != nullptr;
  }
}

/**
 * Check if a WHEN comparison is pushable.
 * Supports integer field vs integer constant (all 6 operators) and
 * CHAR field vs string constant (EQ/NE only).
 */
static bool is_pushable_comparison(Item *when_item,
                                   const TABLE *leaf_table) {
  if (when_item->type() != Item::FUNC_ITEM) return false;
  const auto *when_func = down_cast<const Item_func *>(when_item);
  const auto functype = when_func->functype();
  switch (functype) {
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

  // Integer field vs integer constant.
  const Item_field *field_item = nullptr;
  if (cmp_left->type() == Item::FIELD_ITEM &&
      cmp_right->type() == Item::INT_ITEM) {
    field_item = down_cast<const Item_field *>(cmp_left);
  } else if (cmp_left->type() == Item::INT_ITEM &&
             cmp_right->type() == Item::FIELD_ITEM) {
    field_item = down_cast<const Item_field *>(cmp_right);
  }
  if (field_item != nullptr && is_integer_type(field_item->field->type())) {
    return check_field_table(field_item, leaf_table);
  }

  // CHAR field vs string constant (EQ/NE only).
  if (functype != Item_func::EQ_FUNC && functype != Item_func::NE_FUNC)
    return false;
  field_item = nullptr;
  if (cmp_left->type() == Item::FIELD_ITEM &&
      cmp_right->type() == Item::STRING_ITEM) {
    field_item = down_cast<const Item_field *>(cmp_left);
  } else if (cmp_left->type() == Item::STRING_ITEM &&
             cmp_right->type() == Item::FIELD_ITEM) {
    field_item = down_cast<const Item_field *>(cmp_right);
  }
  if (field_item == nullptr) return false;
  // Only fixed-length CHAR (MYSQL_TYPE_STRING). VARCHAR/BLOB not supported.
  if (field_item->field->type() != MYSQL_TYPE_STRING) return false;
  return check_field_table(field_item, leaf_table);
}

/**
 * Check if an arithmetic expression is pushable for aggregation.
 * Accepts: numeric fields, integer/real/decimal constants, and
 * arithmetic operators (+, -, *) applied recursively.
 *
 * @param item        The expression to check
 * @param leaf_table  Table membership constraint (see check_field_table)
 * @param depth       Recursion depth (max 8 due to register limit)
 */
static bool is_pushable_arithmetic_expr(const Item *item,
                                        const TABLE *leaf_table,
                                        int depth = 0) {
  if (depth > 7) return false;
  if (item->type() == Item::INT_ITEM) return true;
  if (item->type() == Item::REAL_ITEM) return true;
  if (item->type() == Item::DECIMAL_ITEM) return true;
  if (item->type() == Item::FIELD_ITEM) {
    const auto *fi = down_cast<const Item_field *>(item);
    if (!is_numeric_type(fi->field->type())) return false;
    return check_field_table(fi, leaf_table);
  }
  if (item->type() == Item::FUNC_ITEM) {
    const auto *func = down_cast<const Item_func *>(item);
    switch (func->functype()) {
      case Item_func::PLUS_FUNC:
      case Item_func::MINUS_FUNC:
      case Item_func::MUL_FUNC:
        break;
      default:
        return false;
    }
    if (func->argument_count() != 2) return false;
    return is_pushable_arithmetic_expr(func->arguments()[0], leaf_table,
                                       depth + 1) &&
           is_pushable_arithmetic_expr(func->arguments()[1], leaf_table,
                                       depth + 1);
  }
  return false;
}

/**
 * Check if a value item (THEN/ELSE) is pushable: integer constant,
 * integer field, or arithmetic expression.
 */
static bool is_pushable_value_item(Item *item, const TABLE *leaf_table) {
  if (item->type() == Item::INT_ITEM) return true;
  if (item->type() == Item::FIELD_ITEM) {
    const auto *fi = down_cast<const Item_field *>(item);
    if (!is_numeric_type(fi->field->type())) return false;
    return check_field_table(fi, leaf_table);
  }
  if (item->type() == Item::FUNC_ITEM) {
    return is_pushable_arithmetic_expr(item, leaf_table);
  }
  return false;
}

/**
 * Check if a CASE expression is pushable for aggregation.
 *
 * Supports:
 * - Searched CASE: CASE WHEN cond1 THEN v1 [WHEN cond2 THEN v2 ...] ELSE ve END
 * - Simple CASE:   CASE col WHEN val1 THEN v1 [WHEN val2 THEN v2 ...] ELSE ve END
 * - Integer comparisons with all 6 operators (searched) or EQ (simple)
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

  // Must have an ELSE clause.
  if (case_item->get_else_expr_num() < 0) return false;

  // Compute ncases: total WHEN/THEN items (excluding first_expr and else).
  int ncases = static_cast<int>(case_item->argument_count());
  if (case_item->get_first_expr_num() >= 0) ncases--;
  if (case_item->get_else_expr_num() >= 0) ncases--;
  // Must have at least one WHEN/THEN pair and be even.
  if (ncases < 2 || (ncases % 2) != 0) return false;

  Item **args = case_item->arguments();
  const int else_idx = case_item->get_else_expr_num();
  const int first_expr_num = case_item->get_first_expr_num();

  if (first_expr_num >= 0) {
    // Simple CASE: CASE col WHEN val1 THEN v1 ...
    // Search expression must be an integer or CHAR field.
    Item *search_expr = args[first_expr_num];
    if (search_expr->type() != Item::FIELD_ITEM) return false;
    const auto *search_field = down_cast<const Item_field *>(search_expr);
    const bool is_int_search = is_integer_type(search_field->field->type());
    const bool is_char_search =
        (search_field->field->type() == MYSQL_TYPE_STRING);
    if (!is_int_search && !is_char_search) return false;
    if (!check_field_table(search_field, leaf_table)) return false;

    // Each WHEN value must match the search type, each THEN must be pushable.
    for (int i = 0; i < ncases; i += 2) {
      if (is_int_search) {
        if (args[i]->type() != Item::INT_ITEM) return false;
      } else {
        if (args[i]->type() != Item::STRING_ITEM) return false;
      }
      if (!is_pushable_value_item(args[i + 1], leaf_table)) return false;
    }
  } else {
    // Searched CASE: CASE WHEN cond1 THEN v1 ...
    for (int i = 0; i < ncases; i += 2) {
      if (!is_pushable_comparison(args[i], leaf_table)) return false;
      if (!is_pushable_value_item(args[i + 1], leaf_table)) return false;
    }
  }

  // Validate ELSE value.
  if (!is_pushable_value_item(args[else_idx], leaf_table)) return false;

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
 * Count agg program words needed to load/compute an expression.
 * Handles constants, fields, and arithmetic expression trees.
 * DECIMAL columns require an extra word for precision/scale info.
 */
static Uint32 count_expr_words(Item *item,
                               const NdbDictionary::Table *ndb_table) {
  switch (item->type()) {
    case Item::INT_ITEM:
    case Item::REAL_ITEM:
    case Item::DECIMAL_ITEM:
      return 3;  // LoadInt64 or LoadDouble
    case Item::FIELD_ITEM: {
      const auto *fi = down_cast<const Item_field *>(item);
      const NdbDictionary::Column *col =
          ndb_table->getColumn(fi->field->field_index());
      if (col != nullptr &&
          (col->getType() == NdbDictionary::Column::Decimal ||
           col->getType() == NdbDictionary::Column::Decimalunsigned)) {
        return 2;  // LoadColumn + decimal info word
      }
      return 1;  // LoadColumn
    }
    case Item::FUNC_ITEM: {
      const auto *func = down_cast<const Item_func *>(item);
      return count_expr_words(func->arguments()[0], ndb_table) +
             count_expr_words(func->arguments()[1], ndb_table) +
             1;  // +1 for arith op
    }
    default:
      return 0;
  }
}

/**
 * Emit an expression (constant, field, or arithmetic tree) into the
 * NdbAggregator program, putting the result in target_reg.
 *
 * For constants in arithmetic context: uses LoadDouble when the parent
 * expression involves DECIMAL/REAL types, LoadInt64 for pure integer.
 *
 * When ctx is non-null, fields from parent tables use LoadLinkedColumn()
 * with a linked projection position from the AggBuildContext.
 *
 * @param item          The expression to emit
 * @param ndb_table     NDB table for column lookups (leaf table)
 * @param agg           The NdbAggregator being built
 * @param target_reg    Register to store the result
 * @param next_free_reg Next available register (incremented by recursive calls)
 * @param use_double    If true, load integer constants as double (for DECIMAL context)
 * @param ctx           Build context for linked column resolution (may be nullptr)
 */
static bool emit_expr(Item *item, const NdbDictionary::Table *ndb_table,
                      NdbAggregator *agg, Uint32 target_reg,
                      Uint32 *next_free_reg, bool use_double,
                      AggBuildContext *ctx = nullptr) {
  switch (item->type()) {
    case Item::INT_ITEM:
      if (use_double)
        return agg->LoadDouble(item->val_real(), target_reg);
      else
        return agg->LoadInt64(item->val_int(), target_reg);
    case Item::REAL_ITEM:
    case Item::DECIMAL_ITEM:
      return agg->LoadDouble(item->val_real(), target_reg);
    case Item::FIELD_ITEM: {
      const auto *fi = down_cast<const Item_field *>(item);
      if (ctx != nullptr) {
        const uint tab_idx = find_table_index(ctx->tables, ctx->table_count,
                                              fi->field->table);
        if (tab_idx >= ctx->table_count) return false;
        if (tab_idx != ctx->leaf_tab_no) {
          // Parent-table column: use LoadLinkedColumn.
          const int col_id = fi->field->field_index();
          const Uint32 pos = ctx->get_or_add_linked(tab_idx, col_id);
          if (pos == ~Uint32(0)) return false;
          const NdbDictionary::Column *parent_col =
              ctx->ndb_tables[tab_idx]->getColumn(col_id);
          if (parent_col == nullptr) return false;
          return agg->LoadLinkedColumn(pos, target_reg, parent_col);
        }
      }
      const NdbDictionary::Column *col =
          ndb_table->getColumn(fi->field->field_index());
      if (col == nullptr) return false;
      return agg->LoadColumn(col->getAttrId(), target_reg);
    }
    case Item::FUNC_ITEM: {
      const auto *func = down_cast<const Item_func *>(item);
      // Determine if children should use double loading.
      bool child_use_double =
          use_double || func->result_type() == DECIMAL_RESULT ||
          func->result_type() == REAL_RESULT;

      // Emit left operand into target_reg.
      if (!emit_expr(func->arguments()[0], ndb_table, agg, target_reg,
                     next_free_reg, child_use_double, ctx))
        return false;

      // Emit right operand into next available register.
      Uint32 right_reg = (*next_free_reg)++;
      if (right_reg > kReg8) return false;  // Out of registers.
      if (!emit_expr(func->arguments()[1], ndb_table, agg, right_reg,
                     next_free_reg, child_use_double, ctx))
        return false;

      // Emit arithmetic operation.
      switch (func->functype()) {
        case Item_func::PLUS_FUNC:
          return agg->Add(target_reg, right_reg);
        case Item_func::MINUS_FUNC:
          return agg->Minus(target_reg, right_reg);
        case Item_func::MUL_FUNC:
          return agg->Mul(target_reg, right_reg);
        default:
          return false;
      }
    }
    default:
      return false;
  }
}

/**
 * Emit a value expression into the NdbAggregator, result in kReg1.
 * Handles integer constants, fields, and arithmetic trees.
 * When ctx is non-null, parent-table fields use LoadLinkedColumn().
 */
static bool emit_value_load(Item *item,
                            const NdbDictionary::Table *ndb_table,
                            NdbAggregator *agg,
                            AggBuildContext *ctx = nullptr) {
  Uint32 next_free = kReg2;
  return emit_expr(item, ndb_table, agg, kReg1, &next_free, false, ctx);
}

/**
 * Count agg program words for a value expression.
 */
static Uint32 count_value_load_words(Item *item,
                                     const NdbDictionary::Table *ndb_table) {
  return count_expr_words(item, ndb_table);
}

/**
 * Emit the primary aggregate operation for a CASE arm.
 */
static bool emit_agg_op(NdbAggregator *agg, Item_sum::Sumfunctype agg_type,
                         Uint32 agg_id) {
  switch (agg_type) {
    case Item_sum::SUM_FUNC:
      return agg->Sum(agg_id, kReg1);
    case Item_sum::MIN_FUNC:
      return agg->Min(agg_id, kReg1);
    case Item_sum::MAX_FUNC:
      return agg->Max(agg_id, kReg1);
    case Item_sum::COUNT_FUNC:
      return agg->Count(agg_id, kReg1);
    default:
      return false;
  }
}

/**
 * Compute the number of embedded interpreter words for one integer
 * comparison: READ_ATTR + LOAD_CONST + BRANCH.
 */
static Uint32 int_comparison_words(longlong const_val) {
  const bool use_const16 = (const_val >= 0 && const_val <= 65535);
  // READ_ATTR(1) + LOAD_CONST16(1) or LOAD_CONST64(3) + BRANCH(1)
  return 1 + (use_const16 ? 1 : 3) + 1;
}

/**
 * Emit one integer comparison + branch in the embedded interpreter.
 * Returns the position after the BRANCH instruction.
 */
static bool emit_int_comparison_branch(
    const Item_func *when_func, const NdbDictionary::Table *ndb_table,
    NdbAggregator *agg, Uint32 branch_offset) {
  Item *cmp_left = when_func->arguments()[0];
  Item *cmp_right = when_func->arguments()[1];

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

  const NdbDictionary::Column *ndb_col =
      ndb_table->getColumn(cmp_field->field->field_index());
  if (ndb_col == nullptr) return false;
  const Uint32 attr_id = ndb_col->getAttrId();

  // READ_ATTR_INTO_REG reg0, attr_id
  if (!agg->EmitEmbeddedWord(Interpreter::Read(attr_id, 0))) return false;

  // LOAD_CONST16/64 reg1, const_val
  const longlong const_val = cmp_const->val_int();
  const bool use_const16 = (const_val >= 0 && const_val <= 65535);
  if (use_const16) {
    if (!agg->EmitEmbeddedWord(
            Interpreter::LoadConst16(1, static_cast<Uint32>(const_val))))
      return false;
  } else {
    if (!agg->EmitEmbeddedWord(Interpreter::LoadConst64(1))) return false;
    Int64 val64 = static_cast<Int64>(const_val);
    Uint32 data[2];
    memcpy(data, &val64, sizeof(Int64));
    if (!agg->EmitEmbeddedWord(data[0])) return false;
    if (!agg->EmitEmbeddedWord(data[1])) return false;
  }

  // BRANCH_XX_REG_REG
  const Uint32 branch_opcode = get_branch_opcode(when_func->functype());
  const Uint32 left_reg = field_on_left ? 0 : 1;
  const Uint32 right_reg = field_on_left ? 1 : 0;
  if (!agg->EmitEmbeddedWord(branch_opcode | (left_reg << 6) |
                             (right_reg << 9) |
                             (branch_offset << 16)))
    return false;

  return true;
}

/**
 * Emit one simple-CASE value comparison + branch in the embedded interpreter.
 * The search field has already been loaded into reg0.
 */
static bool emit_simple_case_branch(Item *when_val, NdbAggregator *agg,
                                    Uint32 branch_offset) {
  const longlong val = when_val->val_int();
  const bool use_const16 = (val >= 0 && val <= 65535);
  if (use_const16) {
    if (!agg->EmitEmbeddedWord(
            Interpreter::LoadConst16(1, static_cast<Uint32>(val))))
      return false;
  } else {
    if (!agg->EmitEmbeddedWord(Interpreter::LoadConst64(1))) return false;
    Int64 val64 = static_cast<Int64>(val);
    Uint32 data[2];
    memcpy(data, &val64, sizeof(Int64));
    if (!agg->EmitEmbeddedWord(data[0])) return false;
    if (!agg->EmitEmbeddedWord(data[1])) return false;
  }

  // BRANCH_EQ_REG_REG reg0(search), reg1(val)
  if (!agg->EmitEmbeddedWord(Interpreter::BRANCH_EQ_REG_REG | (0 << 6) |
                             (1 << 9) | (branch_offset << 16)))
    return false;

  return true;
}

/**
 * Compute embedded words for one simple-CASE comparison: LOAD_CONST + BRANCH.
 * For integer values. For string values, use string_branch_words().
 */
static Uint32 simple_case_comparison_words(longlong val) {
  const bool use_const16 = (val >= 0 && val <= 65535);
  return (use_const16 ? 1 : 3) + 1;  // LOAD_CONST + BRANCH
}

/**
 * Compute embedded words for a BRANCH_ATTR_OP_ARG string comparison.
 * Total: 2 (opcode + attr/len) + ((col_byte_len + 3) >> 2) data words.
 */
static Uint32 string_branch_words(Uint32 col_byte_len) {
  return 2 + ((col_byte_len + 3) >> 2);
}

/**
 * Emit a BRANCH_ATTR_OP_ARG string comparison + branch.
 *
 * @param attr_id        NDB attribute ID for the CHAR column
 * @param col_byte_len   Column byte length from NdbDictionary::Column::getLength()
 * @param str_val        The constant string to compare against
 * @param str_len        Length of str_val in bytes
 * @param cond           Interpreter::EQ or Interpreter::NE
 * @param branch_offset  Branch target offset from this instruction
 * @param agg            The NdbAggregator to emit into
 */
static bool emit_string_branch(Uint32 attr_id, Uint32 col_byte_len,
                                const char *str_val, uint str_len,
                                Interpreter::BinaryCondition cond,
                                Uint32 branch_offset, NdbAggregator *agg) {
  // Word 0: BranchCol encoding with branch offset.
  if (!agg->EmitEmbeddedWord(
          Interpreter::BranchCol(cond, Interpreter::NULL_CMP_EQUAL) |
          (branch_offset << 16)))
    return false;

  // Word 1: (attr_id << 16) | col_byte_len.
  if (!agg->EmitEmbeddedWord(
          Interpreter::BranchCol_2(attr_id, col_byte_len)))
    return false;

  // Words 2+: space-padded string data, 4-byte aligned.
  const Uint32 data_words = (col_byte_len + 3) >> 2;
  char buf[256];  // Max CHAR column is 255 bytes.
  if (col_byte_len > sizeof(buf)) return false;
  memset(buf, ' ', col_byte_len);
  memcpy(buf, str_val, str_len < col_byte_len ? str_len : col_byte_len);
  // Zero-pad the last partial word if needed.
  Uint32 total_bytes = data_words * 4;
  if (total_bytes > col_byte_len) {
    memset(buf + col_byte_len, 0, total_bytes - col_byte_len);
  }
  const Uint32 *words = reinterpret_cast<const Uint32 *>(buf);
  for (Uint32 w = 0; w < data_words; w++) {
    if (!agg->EmitEmbeddedWord(words[w])) return false;
  }

  return true;
}

/**
 * Check if a searched CASE WHEN condition is a string comparison.
 */
static bool is_string_comparison(Item *when_item) {
  if (when_item->type() != Item::FUNC_ITEM) return false;
  const auto *when_func = down_cast<const Item_func *>(when_item);
  Item *cmp_left = when_func->arguments()[0];
  Item *cmp_right = when_func->arguments()[1];
  if (cmp_left->type() == Item::FIELD_ITEM &&
      cmp_right->type() == Item::STRING_ITEM) {
    return down_cast<const Item_field *>(cmp_left)->field->type() ==
           MYSQL_TYPE_STRING;
  }
  if (cmp_left->type() == Item::STRING_ITEM &&
      cmp_right->type() == Item::FIELD_ITEM) {
    return down_cast<const Item_field *>(cmp_right)->field->type() ==
           MYSQL_TYPE_STRING;
  }
  return false;
}

/**
 * Compute embedded words for one searched-CASE WHEN condition.
 * Dispatches between integer comparison and string comparison.
 */
static Uint32 searched_comparison_words(Item *when_item,
                                        const NdbDictionary::Table *ndb_table) {
  if (is_string_comparison(when_item)) {
    const auto *when_func = down_cast<const Item_func *>(when_item);
    Item *cmp_left = when_func->arguments()[0];
    Item *cmp_right = when_func->arguments()[1];
    const Item_field *field_item;
    if (cmp_left->type() == Item::FIELD_ITEM)
      field_item = down_cast<const Item_field *>(cmp_left);
    else
      field_item = down_cast<const Item_field *>(cmp_right);
    const NdbDictionary::Column *ndb_col =
        ndb_table->getColumn(field_item->field->field_index());
    return string_branch_words(ndb_col->getLength());
  }
  // Integer comparison.
  const auto *when_func = down_cast<const Item_func *>(when_item);
  Item *cmp_left = when_func->arguments()[0];
  Item *cmp_right = when_func->arguments()[1];
  Item_int *cmp_const;
  if (cmp_left->type() == Item::INT_ITEM)
    cmp_const = down_cast<Item_int *>(cmp_left);
  else
    cmp_const = down_cast<Item_int *>(cmp_right);
  return int_comparison_words(cmp_const->val_int());
}

/**
 * Emit one searched-CASE WHEN condition + branch.
 * Dispatches between integer and string comparisons.
 */
static bool emit_searched_comparison_branch(
    Item *when_item, const NdbDictionary::Table *ndb_table,
    NdbAggregator *agg, Uint32 branch_offset) {
  if (is_string_comparison(when_item)) {
    const auto *when_func = down_cast<const Item_func *>(when_item);
    Item *cmp_left = when_func->arguments()[0];
    Item *cmp_right = when_func->arguments()[1];
    const Item_field *field_item;
    Item *str_item;
    if (cmp_left->type() == Item::FIELD_ITEM) {
      field_item = down_cast<const Item_field *>(cmp_left);
      str_item = cmp_right;
    } else {
      field_item = down_cast<const Item_field *>(cmp_right);
      str_item = cmp_left;
    }
    const NdbDictionary::Column *ndb_col =
        ndb_table->getColumn(field_item->field->field_index());
    if (ndb_col == nullptr) return false;

    // Map MySQL functype to Interpreter::BinaryCondition.
    Interpreter::BinaryCondition cond =
        (when_func->functype() == Item_func::EQ_FUNC) ? Interpreter::EQ
                                                       : Interpreter::NE;

    // Get string value.
    String tmp;
    String *str = str_item->val_str(&tmp);
    if (str == nullptr) return false;

    return emit_string_branch(ndb_col->getAttrId(), ndb_col->getLength(),
                              str->ptr(), str->length(), cond, branch_offset,
                              agg);
  }

  // Integer comparison.
  return emit_int_comparison_branch(
      down_cast<const Item_func *>(when_item), ndb_table, agg, branch_offset);
}

/**
 * Compute embedded words for one simple-CASE comparison.
 * For string search: uses BRANCH_ATTR_OP_ARG.
 * For integer search: uses LOAD_CONST + BRANCH_EQ_REG_REG.
 */
static Uint32 simple_case_cond_words(Item *when_val, bool is_string_search,
                                     const NdbDictionary::Table *ndb_table,
                                     Item *search_expr) {
  if (is_string_search) {
    const auto *search_field = down_cast<const Item_field *>(search_expr);
    const NdbDictionary::Column *ndb_col =
        ndb_table->getColumn(search_field->field->field_index());
    return string_branch_words(ndb_col->getLength());
  }
  return simple_case_comparison_words(when_val->val_int());
}

/**
 * Emit one simple-CASE value comparison + branch.
 * For string search: uses BRANCH_ATTR_OP_ARG EQ.
 * For integer search: uses LOAD_CONST + BRANCH_EQ_REG_REG.
 */
static bool emit_simple_case_cond_branch(Item *when_val,
                                         bool is_string_search,
                                         Item *search_expr,
                                         const NdbDictionary::Table *ndb_table,
                                         NdbAggregator *agg,
                                         Uint32 branch_offset) {
  if (is_string_search) {
    const auto *search_field = down_cast<const Item_field *>(search_expr);
    const NdbDictionary::Column *ndb_col =
        ndb_table->getColumn(search_field->field->field_index());
    if (ndb_col == nullptr) return false;
    String tmp;
    String *str = when_val->val_str(&tmp);
    if (str == nullptr) return false;
    return emit_string_branch(ndb_col->getAttrId(), ndb_col->getLength(),
                              str->ptr(), str->length(), Interpreter::EQ,
                              branch_offset, agg);
  }
  return emit_simple_case_branch(when_val, agg, branch_offset);
}

/**
 * Emit an NdbAggregator program for a CASE aggregate expression.
 *
 * Supports searched CASE with multiple WHEN/THEN pairs and simple CASE.
 * Builds an embedded interpreter program for the conditions, followed by
 * aggregation arms for each THEN value and the ELSE value.
 *
 * The embedded interpreter evaluates conditions and writes a skip offset
 * to interpreter output[0].  The aggregation program reads that offset to
 * select the correct arm.
 *
 * @param case_item  The CASE expression (already validated)
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
                                  Uint32 agg_id,
                                  AggBuildContext *ctx = nullptr) {
  Item **args = case_item->arguments();
  const int else_idx = case_item->get_else_expr_num();
  const int first_expr_num = case_item->get_first_expr_num();
  const bool is_simple_case = (first_expr_num >= 0);

  // Compute ncases (WHEN/THEN item count, excluding first_expr and else).
  int ncases = static_cast<int>(case_item->argument_count());
  if (first_expr_num >= 0) ncases--;
  if (else_idx >= 0) ncases--;
  const int n_pairs = ncases / 2;

  // --- Phase 1: Compute sizes ---

  // Per-condition embedded interpreter words (excluding the output blocks).
  // For searched CASE: READ_ATTR + LOAD_CONST + BRANCH per condition.
  // For simple CASE: LOAD_CONST + BRANCH per condition (READ_ATTR once at top).
  static constexpr Uint32 MAX_CASE_PAIRS = 32;
  Uint32 cond_words[MAX_CASE_PAIRS];
  if (n_pairs > (int)MAX_CASE_PAIRS) return false;

  // For simple CASE with string search, BRANCH_ATTR_OP_ARG includes the
  // attribute read, so no separate READ_ATTR needed. For integer search,
  // READ_ATTR is emitted once at the top.
  const bool is_string_search =
      is_simple_case &&
      (down_cast<const Item_field *>(args[first_expr_num])
           ->field->type() == MYSQL_TYPE_STRING);

  Uint32 total_cond_words = 0;
  if (is_simple_case) {
    if (!is_string_search) {
      total_cond_words += 1;  // READ_ATTR for integer search (once)
    }
    for (int i = 0; i < n_pairs; i++) {
      cond_words[i] = simple_case_cond_words(args[i * 2], is_string_search,
                                             ndb_table, args[first_expr_num]);
      total_cond_words += cond_words[i];
    }
  } else {
    for (int i = 0; i < n_pairs; i++) {
      cond_words[i] = searched_comparison_words(args[i * 2], ndb_table);
      total_cond_words += cond_words[i];
    }
  }

  // Output blocks: 3 words each (LOAD_CONST16 + WRITE_INTERP_OUTPUT + EXIT_OK).
  // One block for ELSE (fall-through) + one block per THEN label.
  const Uint32 output_block_words = 3;
  const Uint32 total_output_words =
      output_block_words * (static_cast<Uint32>(n_pairs) + 1);

  const Uint32 emb_len = total_cond_words + total_output_words;

  // Per-arm agg words: load + agg_op (first arm) or load + RepeatAgg (rest).
  // Each arm except the last also has a Skip(1 word).
  Uint32 arm_words[MAX_CASE_PAIRS + 1];  // n_pairs THEN arms + 1 ELSE arm
  for (int i = 0; i < n_pairs; i++) {
    arm_words[i] = count_value_load_words(args[i * 2 + 1], ndb_table) + 1;  // load + op
  }
  arm_words[n_pairs] =
      count_value_load_words(args[else_idx], ndb_table) + 1;  // ELSE: load + RepeatAgg

  // Compute skip offsets for each arm's output block in the embedded interp.
  // skip_offset = number of agg words to skip from the start of the agg arms
  // to reach this arm.
  // ARM_0 (THEN_0): skip_offset = 0
  // ARM_1 (THEN_1): skip_offset = arm_words[0] + 1(Skip)
  // ARM_2 (THEN_2): skip_offset = arm_words[0]+1 + arm_words[1]+1
  // ...
  // ARM_N (ELSE):   skip_offset = sum of (arm_words[i]+1) for i in 0..N-1
  Uint32 skip_offsets[MAX_CASE_PAIRS + 1];
  skip_offsets[0] = 0;
  for (int i = 1; i <= n_pairs; i++) {
    skip_offsets[i] = skip_offsets[i - 1] + arm_words[i - 1] + 1;  // +1 for Skip
  }

  // Each arm's Skip count = remaining words after this arm.
  // ARM_i Skip = sum of (arm_words[j] + 1) for j in (i+1..n_pairs-1) + arm_words[n_pairs]
  // (Last THEN arm skips past ELSE arm; ELSE arm has no Skip.)

  // --- Phase 2: Emit embedded interpreter ---

  if (!agg->EmbeddedInterp(emb_len)) return false;

  // For simple CASE with integer search: load search field into reg0 once.
  // (String search uses BRANCH_ATTR_OP_ARG which reads the attr internally.)
  if (is_simple_case && !is_string_search) {
    Item *search_expr = args[first_expr_num];
    const auto *search_field = down_cast<const Item_field *>(search_expr);
    const NdbDictionary::Column *ndb_col =
        ndb_table->getColumn(search_field->field->field_index());
    if (ndb_col == nullptr) return false;
    if (!agg->EmitEmbeddedWord(Interpreter::Read(ndb_col->getAttrId(), 0)))
      return false;
  }

  // Emit conditions with branches to THEN labels.
  //
  // Layout:
  //   [conditions...]
  //   [ELSE output block]     <- fall-through after all conditions
  //   [THEN_0 output block]
  //   ...
  //   [THEN_{N-1} output block]
  //
  // For BRANCH_ATTR_OP_ARG (string), the branch offset is from the first
  // word of the instruction (not the last). For BRANCH_XX_REG_REG (integer),
  // the offset is also from the BRANCH instruction word itself.
  // Both use "offset from the instruction word that contains the offset field".

  Uint32 cond_pos = (is_simple_case && !is_string_search) ? 1 : 0;
  for (int i = 0; i < n_pairs; i++) {
    // For BRANCH_ATTR_OP_ARG: offset is from the first word of instruction.
    // For BRANCH_XX_REG_REG: offset is from the BRANCH word (last of cond).
    // BRANCH_ATTR_OP_ARG stores offset in word 0 bits[31:16].
    // BRANCH_XX_REG_REG stores offset in the single instruction word bits[31:16].
    // Both: branch target = instruction_pos + offset.
    // For int: instruction_pos = cond_pos + cond_words[i] - 1 (BRANCH is last)
    // For string: instruction_pos = cond_pos (BRANCH_ATTR_OP_ARG is first word)
    bool is_str_cond;
    if (is_simple_case)
      is_str_cond = is_string_search;
    else
      is_str_cond = is_string_comparison(args[i * 2]);

    Uint32 branch_instr_pos;
    if (is_str_cond) {
      branch_instr_pos = cond_pos;  // BRANCH_ATTR_OP_ARG is at start
    } else {
      branch_instr_pos = cond_pos + cond_words[i] - 1;  // BRANCH is at end
    }

    Uint32 then_label_pos =
        total_cond_words + output_block_words * (1 + static_cast<Uint32>(i));
    Uint32 branch_offset = then_label_pos - branch_instr_pos;

    if (is_simple_case) {
      if (!emit_simple_case_cond_branch(args[i * 2], is_string_search,
                                        args[first_expr_num], ndb_table, agg,
                                        branch_offset))
        return false;
    } else {
      if (!emit_searched_comparison_branch(args[i * 2], ndb_table, agg,
                                           branch_offset))
        return false;
    }
    cond_pos += cond_words[i];
  }

  // ELSE output block (fall-through when no condition matched).
  if (!agg->EmitEmbeddedWord(
          Interpreter::LoadConst16(2, skip_offsets[n_pairs])))
    return false;
  if (!agg->EmitEmbeddedWord(Interpreter::WriteInterpreterOutput(2, 0)))
    return false;
  if (!agg->EmitEmbeddedWord(Interpreter::ExitOK())) return false;

  // THEN output blocks.
  for (int i = 0; i < n_pairs; i++) {
    if (!agg->EmitEmbeddedWord(
            Interpreter::LoadConst16(2, skip_offsets[i])))
      return false;
    if (!agg->EmitEmbeddedWord(Interpreter::WriteInterpreterOutput(2, 0)))
      return false;
    if (!agg->EmitEmbeddedWord(Interpreter::ExitOK())) return false;
  }

  // --- Phase 3: Emit aggregation arms ---

  // Total remaining words after each arm (for Skip count).
  // After ARM_i, remaining = sum of (arm_words[j]+1) for j in (i+1..n_pairs-1)
  //                         + arm_words[n_pairs]
  // ELSE arm (ARM_{n_pairs}) has no Skip.

  for (int i = 0; i < n_pairs; i++) {
    // THEN arm i.
    if (!emit_value_load(args[i * 2 + 1], ndb_table, agg, ctx)) return false;
    if (i == 0) {
      if (!emit_agg_op(agg, agg_type, agg_id)) return false;
    } else {
      if (!agg->RepeatAgg(agg_id, kReg1)) return false;
    }
    // Skip past remaining arms.
    Uint32 remaining = skip_offsets[n_pairs] - skip_offsets[i + 1];
    if (!agg->Skip(remaining + arm_words[n_pairs])) return false;
  }

  // ELSE arm.
  if (!emit_value_load(args[else_idx], ndb_table, agg, ctx)) return false;
  if (!agg->RepeatAgg(agg_id, kReg1)) return false;

  return true;
}

/**
 * True for temporal field types (DATE / DATETIME / TIMESTAMP / TIME / YEAR,
 * incl. the internal NEWDATE / *2 storage variants).
 *
 * MIN/MAX over a temporal column IS computed correctly by the data node (the
 * kernel returns the column's native packed value as a Bigunsigned), but the
 * handler's aggregate-result consumption path does not yet decode that packed
 * value back into a temporal MySQL Field.  Pushing it would store the raw
 * packed integer (e.g. 1036335 for 2024-01-15) → "Incorrect date value".  So
 * the handler declines to push temporal MIN/MAX/SUM and lets the server
 * compute them; RonSQL has the decode and still pushes.  (COUNT over a
 * temporal column and GROUP BY a temporal column are unaffected — they don't
 * route a temporal value through the aggregate-result path.)
 */
static bool is_temporal_field_type(enum_field_types t) {
  switch (t) {
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_YEAR:
      return true;
    default:
      return false;
  }
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
      case Item_sum::AVG_FUNC:
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        // Must have exactly one argument: a field reference from a pushed
        // table, a pushable CASE expression, or a pushable arithmetic expr.
        if ((*func)->argument_count() != 1) return false;
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          const auto *field_item = down_cast<const Item_field *>(arg);
          if (field_item->field->table->file->member_of_pushed_join() ==
              nullptr) {
            return false;
          }
          // Temporal SUM/MIN/MAX: the handler can't decode the kernel's packed
          // result into a temporal Field — let the server compute it.
          if (is_temporal_field_type(field_item->field->type())) {
            return false;
          }
        } else if (arg->type() == Item::FUNC_ITEM) {
          if (!is_pushable_case_expr(arg, nullptr) &&
              !is_pushable_arithmetic_expr(arg, nullptr)) {
            return false;
          }
        } else {
          return false;
        }
        break;
      }
      default:
        // Unsupported: COUNT_DISTINCT, SUM_DISTINCT, AVG_DISTINCT, etc.
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
 * Emit a column load (local or linked) for a simple field reference in
 * an aggregate argument.  For leaf-table columns uses LoadColumn(),
 * for parent-table columns uses LoadLinkedColumn() via the context.
 */
static bool emit_field_load(const Item_field *field_item,
                            NdbAggregator *agg, Uint32 reg_id,
                            AggBuildContext &ctx) {
  const uint tab_idx = find_table_index(ctx.tables, ctx.table_count,
                                        field_item->field->table);
  if (tab_idx >= ctx.table_count) return false;
  const int col_id = field_item->field->field_index();
  if (tab_idx != ctx.leaf_tab_no) {
    // Parent-table column: use LoadLinkedColumn.
    const Uint32 pos = ctx.get_or_add_linked(tab_idx, col_id);
    if (pos == ~Uint32(0)) return false;
    const NdbDictionary::Column *parent_col =
        ctx.ndb_tables[tab_idx]->getColumn(col_id);
    if (parent_col == nullptr) return false;
    return agg->LoadLinkedColumn(pos, reg_id, parent_col);
  }
  // Leaf table column: use LoadColumn.
  return agg->LoadColumn(col_id, reg_id);
}

/**
 * Build an NdbAggregator program from the MySQL query plan.
 *
 * Supports multi-table GROUP BY and aggregate arguments: columns from the
 * leaf table use GroupBy()/LoadColumn(), columns from parent tables use
 * GroupByLinked()/LoadLinkedColumn() with linked projections.
 *
 * Cross-table arithmetic expressions (e.g. SUM(t1.a + t2.b)) are supported:
 * each field reference resolves to the correct table via the AggBuildContext.
 *
 * @param join           MySQL JOIN containing aggregation info
 * @param tables         The pushed table array
 * @param table_count    Number of tables in the array
 * @param leaf_tab_no    Table index of the leaf table
 * @param ndb_tables     Array of NDB table pointers indexed by table position
 * @param ctx            Build context; populated with linked column entries
 * @return heap-allocated NdbAggregator on success, nullptr on failure
 */
static NdbAggregator *ndb_build_aggregation_program(
    const JOIN *join, const pushed_table *tables, uint table_count,
    uint leaf_tab_no, const NdbDictionary::Table *const *ndb_tables,
    AggBuildContext &ctx) {
  const NdbDictionary::Table *leaf_ndb_tab = ndb_tables[leaf_tab_no];
  NdbAggregator *agg = new NdbAggregator(leaf_ndb_tab);

  // Initialize context.
  ctx.tables = tables;
  ctx.table_count = table_count;
  ctx.leaf_tab_no = leaf_tab_no;
  ctx.ndb_tables = ndb_tables;
  ctx.n_linked_cols = 0;
  ctx.next_linked_pos = 0;

  // Map GROUP BY columns. Columns from parent tables use GroupByLinked(),
  // columns from the leaf table use GroupBy().
  for (ORDER *group = join->query_block->group_list.first; group != nullptr;
       group = group->next) {
    const auto *field_item = down_cast<const Item_field *>(*(group->item));
    const int col_id = field_item->field->field_index();
    const uint tab_idx =
        find_table_index(tables, table_count, field_item->field->table);
    if (tab_idx >= table_count) {
      // GROUP BY column references a table not in the builder scope.
      delete agg;
      return nullptr;
    }
    if (tab_idx == leaf_tab_no) {
      // Leaf table column — direct GROUP BY.
      if (!agg->GroupBy(col_id)) {
        delete agg;
        return nullptr;
      }
    } else {
      // Parent table column — linked GROUP BY.
      // Register in context to assign a linked position.
      const Uint32 pos = ctx.get_or_add_linked(tab_idx, col_id);
      if (pos == ~Uint32(0)) {
        delete agg;
        return nullptr;
      }
      const NdbDictionary::Column *parent_col =
          ndb_tables[tab_idx]->getColumn(col_id);
      if (parent_col == nullptr ||
          !agg->GroupByLinked(pos, parent_col)) {
        delete agg;
        return nullptr;
      }
    }
  }

  // Map aggregate functions.  AVG uses two agg slots (SUM + COUNT),
  // so agg_id advances by 2 for AVG and by 1 for other functions.
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
          if (!emit_field_load(field_item, agg, kReg1, ctx) ||
              !agg->Count(agg_id, kReg1)) {
            delete agg;
            return nullptr;
          }
        } else if (count_arg->type() == Item::FUNC_ITEM) {
          const auto *case_item =
              down_cast<const Item_func_case *>(count_arg);
          if (!emit_case_aggregation(case_item, leaf_ndb_tab, agg,
                                     (*func)->sum_func(), agg_id, &ctx)) {
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
        agg_id++;
        break;
      }
      case Item_sum::AVG_FUNC: {
        // AVG(x) decomposes into SUM(x) at agg_id and COUNT(x) at agg_id+1.
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          const auto *field_item = down_cast<const Item_field *>(arg);
          if (!emit_field_load(field_item, agg, kReg1, ctx) ||
              !agg->Sum(agg_id, kReg1) ||
              !agg->Count(agg_id + 1, kReg1)) {
            delete agg;
            return nullptr;
          }
        } else if (arg->type() == Item::FUNC_ITEM) {
          const auto *func_item = down_cast<const Item_func *>(arg);
          if (func_item->functype() == Item_func::CASE_FUNC) {
            const auto *case_item =
                down_cast<const Item_func_case *>(arg);
            if (!emit_case_aggregation(case_item, leaf_ndb_tab, agg,
                                       Item_sum::SUM_FUNC, agg_id, &ctx) ||
                !emit_case_aggregation(case_item, leaf_ndb_tab, agg,
                                       Item_sum::COUNT_FUNC, agg_id + 1, &ctx)) {
              delete agg;
              return nullptr;
            }
          } else {
            if (!emit_value_load(arg, leaf_ndb_tab, agg, &ctx) ||
                !agg->Sum(agg_id, kReg1) ||
                !agg->Count(agg_id + 1, kReg1)) {
              delete agg;
              return nullptr;
            }
          }
        }
        agg_id += 2;
        break;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          // SUM/MIN/MAX(column): load column (local or linked), then aggregate.
          const auto *field_item = down_cast<const Item_field *>(arg);
          if (!emit_field_load(field_item, agg, kReg1, ctx)) {
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
          const auto *func_item = down_cast<const Item_func *>(arg);
          if (func_item->functype() == Item_func::CASE_FUNC) {
            const auto *case_item =
                down_cast<const Item_func_case *>(arg);
            if (!emit_case_aggregation(case_item, leaf_ndb_tab, agg,
                                       (*func)->sum_func(), agg_id, &ctx)) {
              delete agg;
              return nullptr;
            }
          } else {
            // Arithmetic expression: emit expr then agg op.
            if (!emit_value_load(arg, leaf_ndb_tab, agg, &ctx) ||
                !emit_agg_op(agg, (*func)->sum_func(), agg_id)) {
              delete agg;
              return nullptr;
            }
          }
        }
        agg_id++;
        break;
      }
      default:
        delete agg;
        return nullptr;
    }
  }

  if (!agg->Finalize()) {
    delete agg;
    return nullptr;
  }

  DBUG_PRINT("info",
             ("ndb_push_aggregation: built NdbAggregator program "
              "with %u linked columns (%u total) and %u aggregates",
              ctx.n_linked_cols, ctx.next_linked_pos, agg_id));
  return agg;
}

/**
 * Apply aggregation options to the leaf table during build_query().
 *
 * Called from build_query() when m_aggregator is set. For the leaf table
 * (last in join scope), calls setAggregation() on its NdbQueryOptions and
 * adds linked projections for all parent-table columns used in GROUP BY
 * and aggregate expressions (tracked by the AggBuildContext).
 *
 * Linked projections are emitted in position order (0, 1, 2, ...) matching
 * the linked_pos assigned during program building.
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

  // Add linked projections for all parent-table columns tracked in the
  // AggBuildContext, in position order.  This covers both GROUP BY and
  // aggregate expression columns.
  const AggBuildContext &ctx = builder.m_agg_build_ctx;
  for (Uint32 pos = 0; pos < ctx.next_linked_pos; pos++) {
    // Find the entry with this position.
    for (uint i = 0; i < ctx.n_linked_cols; i++) {
      if (ctx.linked_cols[i].linked_pos == pos) {
        const uint tab_idx = ctx.linked_cols[i].tab_idx;
        const NdbDictionary::Column *ndb_col = ctx.linked_cols[i].ndb_col;
        const NdbQueryOperationDef *parent_op =
            builder.m_tables[tab_idx].m_op;
        const NdbLinkedOperand *linked = builder.m_builder->linkedValue(
            parent_op, ndb_col->getName());
        options->addLinkedProjection(linked);
        break;
      }
    }
  }
}

bool ndb_has_unpushable_filter_for_aggregate(const AccessPath *path) {
  while (path != nullptr) {
    switch (path->type) {
      case AccessPath::FILTER: {
        // A FILTER whose condition references only the child table's columns
        // is pushable to NDB (handled by prep_cond_push / build_cond_push)
        // and will be eliminated by fixup_pushed_access_paths.
        // Multi-table or subquery conditions cannot be pushed and block
        // aggregation pushdown.
        const AccessPath *child = path->filter().child;
        const TABLE *table = GetBasicTable(child);
        if (table != nullptr && path->filter().condition != nullptr) {
          const table_map used = path->filter().condition->used_tables();
          const table_map this_table = table->pos_in_table_list->map();
          if ((used & ~this_table) == 0) {
            // Condition only references this table — pushable. Skip filter.
            path = child;
            break;
          }
        }
        // Non-pushable or multi-table filter — blocks aggregation.
        return true;
      }
      case AccessPath::TEMPTABLE_AGGREGATE:
        path = path->temptable_aggregate().subquery_path;
        break;
      case AccessPath::AGGREGATE:
        path = path->aggregate().child;
        break;
      case AccessPath::SORT:
        path = path->sort().child;
        break;
      case AccessPath::NESTED_LOOP_JOIN:
        if (ndb_has_unpushable_filter_for_aggregate(
                path->nested_loop_join().inner)) {
          return true;
        }
        path = path->nested_loop_join().outer;
        break;
      case AccessPath::TABLE_SCAN:
      case AccessPath::INDEX_SCAN:
      case AccessPath::REF:
      case AccessPath::EQ_REF:
      case AccessPath::PUSHED_JOIN_REF:
      case AccessPath::INDEX_RANGE_SCAN:
        return false;  // Reached leaf — no blocking filter
      default:
        return false;
    }
  }
  return false;
}

bool ndb_push_aggregation(THD *, const JOIN *join,
                          ndb_pushed_builder_ctx &builder,
                          bool allow_outer_join) {
  // All tables in the builder must be part of the same pushed join.
  // If any table is not pushed, or belongs to a different pushed join,
  // MySQL still needs raw rows for joining.
  const TABLE *root_table = nullptr;
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab == nullptr || tab->file->member_of_pushed_join() == nullptr) {
      return false;
    }
    if (root_table == nullptr) {
      root_table = tab->file->member_of_pushed_join();
    } else if (tab->file->member_of_pushed_join() != root_table) {
      return false;  // Different pushed joins — can't aggregate across them.
    }
  }

  if (!ndb_can_push_aggregation(join)) {
    return false;
  }

  // Reject aggregation with semi/anti joins (not yet supported).
  // Outer joins are supported when allow_outer_join is true:
  // DBSPJ tracks matched/unmatched parents and injects null-extended rows
  // into the aggregation engine via JOIN_AGG_NULL_ROW_REQ/CONF.
  {
    const pushed_table &root = builder.m_tables[0];
    for (uint i = 1; i < builder.m_table_count; i++) {
      if (!builder.m_join_scope.contain(i)) continue;
      const pushed_table &tab = builder.m_tables[i];
      if (tab.isSemiJoined(root) || tab.isAntiJoined(root)) {
        DBUG_PRINT("info",
                   ("ndb_push_aggregation: table %u has semi/anti join "
                    "— cannot push aggregation",
                    i));
        return false;
      }
      if (!allow_outer_join && tab.isOuterJoined(root)) {
        DBUG_PRINT("info",
                   ("ndb_push_aggregation: table %u has outer join "
                    "— ndb_join_pushdown_aggregate_outer_join is OFF",
                    i));
        return false;
      }
    }
  }

  // Reject if any table has a non-pushable remainder condition.
  // Such conditions must be evaluated by MySQL on individual rows BEFORE
  // aggregation, which is impossible when NDB returns aggregated results.
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab == nullptr) continue;
    const auto *h = down_cast<const ha_ndbcluster *>(tab->file);
    if (h->m_cond.m_remainder_cond != nullptr) {
      return false;
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
      ndb_tables, builder.m_agg_build_ctx);
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
  // AVG uses two agg slots (SUM + COUNT), so fetch two results for it.
  for (Item_sum **func = join->sum_funcs; *func != nullptr; func++) {
    if ((*func)->sum_func() == Item_sum::AVG_FUNC) {
      // AVG: fetch SUM and COUNT results.  Pass the raw values to
      // Item_sum_avg so that reset_field() can store them with the
      // correct precision (f_scale=0 for INT columns would truncate
      // a pre-computed fractional average).
      NdbAggregator::Result sum_res = rec.FetchAggregationResult();
      NdbAggregator::Result cnt_res = rec.FetchAggregationResult();
      const uint64_t count = cnt_res.is_null() ? 0 : cnt_res.data_uint64();
      if (count == 0 || sum_res.is_null()) {
        (*func)->set_pushed_null();
      } else {
        switch (sum_res.type()) {
          case NdbDictionary::Column::Bigint:
            (*func)->set_pushed_avg(sum_res.data_int64(), count);
            break;
          case NdbDictionary::Column::Bigunsigned:
            (*func)->set_pushed_avg(
                static_cast<int64_t>(sum_res.data_uint64()), count);
            break;
          case NdbDictionary::Column::Double:
            (*func)->set_pushed_avg_double(sum_res.data_double(), count);
            break;
          default:
            return HA_ERR_INTERNAL_ERROR;
        }
      }
    } else {
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
          case NdbDictionary::Column::Char:
          case NdbDictionary::Column::Varchar:
          case NdbDictionary::Column::Longvarchar: {
            Uint32 len = 0;
            const char *ptr = res.data_str(&len);
            (*func)->set_pushed_value_string(ptr, len,
                                             (*func)->collation.collation);
            break;
          }
          default:
            return HA_ERR_INTERNAL_ERROR;
        }
      }
    }
  }

  return NdbQuery::NextResult_gotRow;
}

void ndb_clear_pushed_agg_state(ndb_pushed_builder_ctx &builder) {
  for (uint i = 0; i < builder.m_table_count; i++) {
    const TABLE *tab = builder.m_tables[i].get_table();
    if (tab == nullptr) continue;
    auto *h = dynamic_cast<ha_ndbcluster *>(tab->file);
    if (h == nullptr) continue;
    h->m_pushed_agg_mode = false;
    h->m_agg_join = nullptr;
  }
}

int ndb_fetch_pushed_aggregate(ha_ndbcluster *handler) {
  if (handler->m_active_query == nullptr) {
    // Safety: should not reach here on a child handler (m_pushed_agg_mode
    // should only be set on the root).  Return end-of-file to avoid crash.
    assert(false);
    return HA_ERR_END_OF_FILE;
  }
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
      case Item_sum::AVG_FUNC:
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
          // Temporal SUM/MIN/MAX: the handler can't decode the kernel's packed
          // result into a temporal Field — let the server compute it.
          if (is_temporal_field_type(field_item->field->type())) {
            return false;
          }
        } else if (arg->type() == Item::FUNC_ITEM) {
          if (!is_pushable_case_expr(arg, table) &&
              !is_pushable_arithmetic_expr(arg, table)) {
            return false;
          }
        } else {
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
        agg_id++;
        break;
      }
      case Item_sum::AVG_FUNC: {
        // AVG(x) decomposes into SUM(x) at agg_id and COUNT(x) at agg_id+1.
        Item *arg = (*func)->arguments()[0];
        if (arg->type() == Item::FIELD_ITEM) {
          const auto *field_item = down_cast<const Item_field *>(arg);
          const int col_id = field_item->field->field_index();
          if (!agg->LoadColumn(col_id, kReg1) ||
              !agg->Sum(agg_id, kReg1) ||
              !agg->Count(agg_id + 1, kReg1)) {
            delete agg;
            return nullptr;
          }
        } else if (arg->type() == Item::FUNC_ITEM) {
          const auto *func_item = down_cast<const Item_func *>(arg);
          if (func_item->functype() == Item_func::CASE_FUNC) {
            const auto *case_item =
                down_cast<const Item_func_case *>(arg);
            if (!emit_case_aggregation(case_item, ndb_table, agg,
                                       Item_sum::SUM_FUNC, agg_id) ||
                !emit_case_aggregation(case_item, ndb_table, agg,
                                       Item_sum::COUNT_FUNC, agg_id + 1)) {
              delete agg;
              return nullptr;
            }
          } else {
            if (!emit_value_load(arg, ndb_table, agg) ||
                !agg->Sum(agg_id, kReg1) ||
                !agg->Count(agg_id + 1, kReg1)) {
              delete agg;
              return nullptr;
            }
          }
        }
        agg_id += 2;
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
          const auto *func_item = down_cast<const Item_func *>(arg);
          if (func_item->functype() == Item_func::CASE_FUNC) {
            const auto *case_item =
                down_cast<const Item_func_case *>(arg);
            if (!emit_case_aggregation(case_item, ndb_table, agg,
                                       (*func)->sum_func(), agg_id)) {
              delete agg;
              return nullptr;
            }
          } else {
            // Arithmetic expression: emit expr then agg op.
            if (!emit_value_load(arg, ndb_table, agg) ||
                !emit_agg_op(agg, (*func)->sum_func(), agg_id)) {
              delete agg;
              return nullptr;
            }
          }
        }
        agg_id++;
        break;
      }
      default:
        delete agg;
        return nullptr;
    }
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

  // STM aggregation requires a scan (full table or index scan).
  // Single-row access methods (PK/unique key lookup) are not scans
  // and the STM aggregation path would never execute.
  const enum_access_type jt = builder.m_tables[0].get_access_type();
  if (jt == AT_PRIMARY_KEY || jt == AT_UNIQUE_KEY || jt == AT_OTHER) {
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
