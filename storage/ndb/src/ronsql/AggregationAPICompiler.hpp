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
#include "ArenaAllocator.hpp"
#include "DynamicArray.hpp"
#include "RonSQLCommon.hpp"
// todo order and remove superfluous includes
using std::string;

// todo Increase to 16 if possible
#define REGS 8

#define FORALL_ARITHMETIC_OPS(X) \
  X(Add) \
  X(Minus) \
  X(Mul) \
  X(Div) \
  X(DivInt) \
  X(Rem)
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
  };
#undef ARITHMETIC_ENUM
  using Expr = AggregationAPICompiler_Expr;
  friend class AggregationAPICompiler;
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
};

class AggregationAPICompiler
{
public:
  AggregationAPICompiler(std::function<const char*(uint)> column_idx_to_name,
                         std::basic_ostream<char>& out,
                         std::basic_ostream<char>& err,
                         ArenaAllocator* aalloc);
  enum class Status
  {
    PROGRAMMING, // High-level API available only in this state
    COMPILING,
    COMPILED,
    FAILED,
  };
  Status getStatus();
private:
  std::basic_ostream<char>& m_out;
  std::basic_ostream<char>& m_err;
  Status m_status = Status::PROGRAMMING;
  ArenaAllocator* m_aalloc;

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

  // Aggregation Compiler:
public:
  DynamicArray<Instr> m_program;
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
