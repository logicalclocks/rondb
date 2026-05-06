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

#ifndef STORAGE_NDB_SRC_RONSQL_AGGREGATIONAPICOMPILER_HPP
#define STORAGE_NDB_SRC_RONSQL_AGGREGATIONAPICOMPILER_HPP 1

#include <stdlib.h>
#include <stdexcept>
#include <functional>
#include "LexString.hpp"
#include "ArenaMalloc.hpp"
#include "DynamicArray.hpp"
#include "RonSQLCommon.hpp"
// todo order and remove superfluous includes

// todo Increase to 16 if possible
#define REGS 8

#define FORALL_ARITHMETIC_OPS(X) \
  X(Add) \
  X(Minus) \
  X(Mul) \
  X(Div) \
  X(DivInt) \
  X(Rem)
// Pair-ops are value-producing binary ops shaped like arithmetic ops at the
// SVM level. Each one emits an embedded normal-interpreter comparison that
// imports aggregation registers and conditionally skips a Mov. Used to lower
// n-ary GREATEST / LEAST.
#define FORALL_PAIR_OPS(X) \
  X(Greatest2) \
  X(Least2)
#define FORALL_AGGS(X) \
  X(Sum) \
  X(Min) \
  X(Max) \
  X(Count)
#define FORALL_INSTRUCTIONS(X) \
  X(Load) \
  X(LoadConstantInteger) \
  X(Mov) \
  FORALL_ARITHMETIC_OPS(X) \
  FORALL_PAIR_OPS(X) \
  FORALL_AGGS(X)

// This class is called AggregationAPICompiler::Expr everywhere except in
// RonSQLCommon.hpp. It needs to be defined in the top level since nested
// classes cannot be forward-declared.
class AggregationAPICompiler_Expr
{
#define ARITHMETIC_ENUM(Name) Name,
  enum class ExprOp
  {
    Load,
    LoadConstantInt,
    FORALL_ARITHMETIC_OPS(ARITHMETIC_ENUM)
    FORALL_PAIR_OPS(ARITHMETIC_ENUM)
    Case,
  };
#undef ARITHMETIC_ENUM
  using Expr = AggregationAPICompiler_Expr;
  friend class AggregationAPICompiler;
public:
  bool isLoad() const { return op == ExprOp::Load; }
  bool isLoadConstantInt() const { return op == ExprOp::LoadConstantInt; }
  bool isGreatest2() const { return op == ExprOp::Greatest2; }
  bool isLeast2() const { return op == ExprOp::Least2; }
  bool isCase() const { return op == ExprOp::Case; }
  Uint32 getLoadIdx() const { return idx; }
  Uint32 getConstantIdx() const { return idx; }
  AggregationAPICompiler_Expr* getLeft() const { return left; }
  AggregationAPICompiler_Expr* getRight() const { return right; }
  ConditionalExpression* getCaseCondition() const { return case_condition; }
private:
  ExprOp op; // Binary operation or Load
  Expr* left = NULL; // Left argument to binary operation
  Expr* right = NULL; // Right argument to binary operation
  Uint32 idx = 0; // Column number for load operation, or index in constant list
                  // for loadconstant operations
  Int32 usage = 0; // Reference count from Expr and AggExpr.
                   // Only used for asserts.
  Uint32 est_regs = 0; // Estimated number of registers necessary to calculate
                       // the expression.
  bool eval_left_first = false; // True if we should evaluate left before
                                // right.
  // The following values belong to compiler (below). They are placed in this
  // struct for convenience.
  Int32 program_usage = 0; // Reference count in program, including uses so far
                           // in calculation but excluding uses in
                           // re-calculation. Only used for asserts.
  bool has_been_compiled = false; // Only used to determine program_usage.
  ConditionalExpression* case_condition = NULL; // Only for Case op
};

class AggregationAPICompiler
{
public:
  AggregationAPICompiler(std::function<const char*(uint)> column_idx_to_name,
                         std::basic_ostream<char>& out,
                         std::basic_ostream<char>& err,
                         ArenaMalloc* amalloc);
  enum class Status
  {
    PROGRAMMING, // High-level API available only in this state
    COMPILING,
    COMPILED,
    FAILED,
  };
  Status getStatus();
  // Phase I.5 v2b: lets RonSQLPreparer identify which compiler owns a
  // given Expr* (for cross-scope tracking of GREATEST / LEAST
  // operands).
  bool owns_expr(class AggregationAPICompiler_Expr* e);
  void for_each_expr(
      std::function<void(const AggregationAPICompiler_Expr*)> fn) const;
private:
  std::basic_ostream<char>& m_out;
  std::basic_ostream<char>& m_err;
  Status m_status = Status::PROGRAMMING;
  ArenaMalloc* m_amalloc;

  // High-level API:
public:
  using Expr = AggregationAPICompiler_Expr;
  using ExprOp = AggregationAPICompiler_Expr::ExprOp;
  union Constant
  {
    Int64 int_64;
  };
private:
  std::function<const char*(uint)> m_column_idx_to_name;
  DynamicArray<Expr> m_exprs;
  Expr* new_expr(ExprOp op, Expr* left, Expr* right, Uint32 idx);
#define AGG_ENUM(Name) Name,
  enum class AggType
  {
    FORALL_AGGS(AGG_ENUM)
  };
#undef AGG_ENUM
  struct AggExpr
  {
    AggType agg_type;
    Expr* expr = NULL;
  };
  DynamicArray<AggExpr> m_aggs;
  Uint32 new_agg(AggType agg_type, Expr* expr);
public:
  DynamicArray<Constant> m_constants;
  // Load operations
  Expr* Load(Uint32 col_idx);
  Expr* ConstantInteger(Int64 int_64);
  // Arithmetic and aggregation operations could easily have been defined using
  // templates, but we prefer doing it without templates and with better
  // argument names.
  // Arithmetic operations
#define DEFINE_ARITH_FUNC(OP) \
  Expr* OP(Expr* expr_x, Expr* expr_y) \
  { \
    return public_arithmetic_expression_helper(ExprOp::OP, \
                                               expr_x, \
                                               expr_y); \
  }
  FORALL_ARITHMETIC_OPS(DEFINE_ARITH_FUNC)
  FORALL_PAIR_OPS(DEFINE_ARITH_FUNC)
#undef DEFINE_ARITH_FUNC
  // Aggregation operations
#define DEFINE_AGG_FUNC(OP) \
  Uint32 OP(Expr* expr) \
  { \
    return public_aggregate_function_helper(AggType::OP, expr); \
  }
  FORALL_AGGS(DEFINE_AGG_FUNC)
#undef DEFINE_AGG_FUNC
private:
  Expr* public_arithmetic_expression_helper(ExprOp op, Expr* x, Expr* y);
  Uint32 public_aggregate_function_helper(AggType agg_type, Expr* x);

  // Symbolic Virtual Machine:
private:
  Expr* r[REGS];
  void svm_init();
public:
#define INSTR_ENUM(Name) Name,
  enum class SVMInstrType
  {
    FORALL_INSTRUCTIONS(INSTR_ENUM)
    EmbeddedInterp,    // dest=case_index, src=0
    Skip,              // dest=0, src=0 (placeholder)
    AggRepeat,         // dest=agg_index, src=reg (CASE ELSE arm duplicate)
  };
#undef INSTR_ENUM
  struct Instr
  {
    SVMInstrType type;
    Uint32 dest;
    Uint32 src;
  };
private:
  void svm_execute(Instr* instr, bool is_first_compilation);
  void svm_use(Uint32 reg, bool is_first_compilation);

  // Case expression support:
public:
  struct CaseInfo
  {
    ConditionalExpression* condition;
    Uint32 then_start;   // program index: first THEN arm instruction
    Uint32 skip_pos;     // program index: Skip instruction
    Uint32 else_start;   // program index: first ELSE arm instruction
    Uint32 else_end;     // program index: one past last ELSE arm instruction
  };
  DynamicArray<CaseInfo> m_cases;
  Expr* CaseExpr(ConditionalExpression* condition,
                 Expr* then_expr, Expr* else_expr);
  Uint32 raw_word_size(Uint32 start, Uint32 end);

  // Aggregation Compiler:
public:
  DynamicArray<Instr> m_program;
  // Phase I.5 v4 fast path: parallel to m_program.  Slot i holds the
  // Greatest2 / Least2 Expr* for instruction i if that instruction is
  // a pair-op, NULL otherwise.  Used by RonSQLPreparer to compute
  // per-pair-op nullability and elide the BRANCH_REG_EQ_NULL words
  // when both operands are statically non-nullable.
  DynamicArray<Expr*> m_pair_op_program_exprs;
  // Per-program-index needs_null_check decision (filled by
  // RonSQLPreparer at the start of programAggregator before any
  // raw_word_size queries fire).  true = emit the nullable 18-word
  // expansion, false = emit the 9-word embedded body + Mov fast path.
  // Only meaningful at indices where m_program[i] is a pair-op.
  DynamicArray<bool> m_pair_op_needs_null_check;
private:
  Uint32 m_locked[REGS];
public:
  bool compile();
private:
  bool compile(AggExpr* agg, Uint32 idx);
  bool compile(Expr* expr, Uint32* reg);
  bool seize_register(Uint32* reg, Uint32 max_cost);
  Uint32 estimated_cost_of_recalculating(Expr* expr, Uint32 without_using_reg);
  void pushInstr(SVMInstrType type,
                 Uint32 dest,
                 Uint32 src,
                 bool is_first_compilation);
  void pushInstr(AggType type, Uint32 dest, Uint32 src, bool is_first_compilation);
  void pushInstr(ExprOp op, Uint32 dest, Uint32 src, bool is_first_compilation);
  void dead_code_elimination();

  // Aggregation Program Printer
public:
  void print_aggregates();
  void print_aggregate(Uint32 idx);
  void print(Expr* expr);
  void print_program();
  void print(Instr* instr);

}; // End of class AggregationAPICompiler

#endif
