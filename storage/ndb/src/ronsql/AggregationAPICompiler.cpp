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

#include <iostream>
#include "define_formatter.hpp"
#include <cstring>
#include "AggregationAPICompiler.hpp"
using std::endl;
using std::max;

AggregationAPICompiler::AggregationAPICompiler
    (std::function<const char*(uint)> column_idx_to_name,
     std::basic_ostream<char>& out,
     std::basic_ostream<char>& err,
     ArenaMalloc* amalloc):
  m_out(out),
  m_err(err),
  m_amalloc(amalloc),
  m_column_idx_to_name(column_idx_to_name),
  m_exprs(amalloc),
  m_aggs(amalloc),
  m_constants(amalloc),
  m_cases(amalloc),
  m_program(amalloc)
{}

AggregationAPICompiler::Status
AggregationAPICompiler::getStatus()
{
  return m_status;
}

bool
AggregationAPICompiler::owns_expr(AggregationAPICompiler_Expr* e)
{
  return m_exprs.has_item(e);
}

#define require_status(name) ndbrequire(m_status == Status::name)

/*
 * Start of High-level API
 *
 * This is the high-level API used to construct a pushdown aggregation
 * program.
 */

// Detecting signed integer overlow for the biggest datatype is not trivial.
bool
int64_add_overflow(Int64 x, Int64 y)
{
  bool x_neg = x < 0;
  bool y_neg = y < 0;
  if (x_neg != y_neg)
    return false;
  // Signed addition overflow is undefined behaviour, so we must detect it
  // before performing a calculation that could trigger it. We use unsigned
  // addition to calculate a result under modular arithmetic with no undefined
  // behaviour, then use this result to detect overflow.
  Int64 result = Int64(Uint64(x) + Uint64(y));
  bool result_neg = result < 0;
  return x_neg != result_neg;
}
bool
int64_sub_overflow(Int64 x, Int64 y)
{
  if (y == INT64_MIN)
  {
    // y cannot be negated
    if (x == INT64_MAX)
    {
      // x cannot be incremented, but at this code path we know the values for
      // both x and y.
      return true;
    }
    // Both x and y can be incremented. Doing so will preserve the difference
    // and guarantee that y can be negated.
    x++; y++;
  }
  // y can be negated.
  return int64_add_overflow(x, -y);
}
bool
int64_mul_overflow(Int64 x, Int64 y)
{
    if (x > 0 && y > 0 && x > INT64_MAX / y) return true;
    if (x < 0 && y > 0 && x < INT64_MIN / y) return true;
    if (x > 0 && y < 0 && y < INT64_MIN / x) return true;
    if (x < 0 && y < 0 && x < INT64_MAX / y) return true;
    return false;
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::new_expr(ExprOp op,
                                 Expr* left,
                                 Expr* right,
                                 Uint32 idx)
{
  if (m_status == Status::FAILED)
  {
    return NULL;
  }
  ndbrequire(m_status == Status::PROGRAMMING ||
             m_status == Status::COMPILING ||
             m_status == Status::COMPILED);
  ndbrequire(left == NULL || m_exprs.has_item(left));
  ndbrequire(right == NULL || m_exprs.has_item(right));
  Expr e;
  e.op = op;
  e.left = left;
  e.right = right;
  e.idx = idx;
  if (op == ExprOp::Load ||
      op == ExprOp::LoadConstantInt)
  {
    ndbrequire(left == NULL);
    ndbrequire(right == NULL);
    e.est_regs = 1;
  }
  else
  {
    ndbrequire(left != NULL);
    ndbrequire(right != NULL);
    ndbrequire(idx == 0);
    // Estimate the numbers of registers necessary to calculate the
    // expression, and use that to determine the order of evaluation.
    // We cannot afford to calculate the exact number of registers needed
    // since that is context-dependent and takes exponential time.
    if (left == right)
    {
      e.est_regs = left->est_regs;
      e.eval_left_first = true;
    }
    else if (left->est_regs >= right->est_regs)
    {
      e.est_regs = max(left->est_regs, right->est_regs + 1);
      e.eval_left_first = true;
    }
    else
    {
      e.est_regs = max(left->est_regs + 1, right->est_regs);
      e.eval_left_first = false;
    }
  }
  // Integer constant folding
  if (left != NULL &&
      left->op == ExprOp::LoadConstantInt &&
      right != NULL &&
      right->op == ExprOp::LoadConstantInt &&
      op != ExprOp::Div)
  {
    Int64 arg1 = m_constants[left->idx].int_64;
    Int64 arg2 = m_constants[right->idx].int_64;
    Int64 result = 0;
    switch (op)
    {
    case ExprOp::Greatest2:
      result = (arg1 >= arg2) ? arg1 : arg2;
      break;
    case ExprOp::Least2:
      result = (arg1 <= arg2) ? arg1 : arg2;
      break;
    case ExprOp::Add:
      if (int64_add_overflow(arg1, arg2)) {
        m_err << "Overflow when attempting to fold constant expression (" << arg1 << " + " << arg2 << ").\n";
        throw RonSQLPermanentError("Overflow in integer constant folding.");
      }
      result = arg1 + arg2;
      break;
    case ExprOp::Minus:
      if (int64_sub_overflow(arg1, arg2)) {
        m_err << "Overflow when attempting to fold constant expression (" << arg1 << " - " << arg2 << ").\n";
        throw RonSQLPermanentError("Overflow in integer constant folding.");
      }
      result = arg1 - arg2;
      break;
    case ExprOp::Mul:
      if (int64_mul_overflow(arg1, arg2)) {
        m_err << "Overflow when attempting to fold constant expression (" << arg1 << " * " << arg2 << ").\n";
        throw RonSQLPermanentError("Overflow in integer constant folding.");
      }
      result = arg1 * arg2;
      break;
    case ExprOp::DivInt:
      if (arg2 == 0) {
        m_err << "Divide by zero when attempting to fold constant expression (" << arg1 << " DIV " << arg2 << ").\n";
        throw RonSQLPermanentError("Divide by zero in integer constant folding.");
      }
      result = arg1 / arg2;
      break;
    case ExprOp::Rem:
      if (arg2 == 0) {
        m_err << "Divide by zero when attempting to fold constant expression (" << arg1 << " % " << arg2 << ").\n";
        throw RonSQLPermanentError("Divide by zero in integer constant folding.");
      }
      result = arg1 % arg2;
      break;
    default:
      // Unknown operation
      abort();
    }
    m_constants.push({result});
    return new_expr(ExprOp::LoadConstantInt, 0, 0, m_constants.size() - 1);
  }
  // Deduplication
  for (Uint32 i=0; i<m_exprs.size(); i++)
  {
    Expr* other = &m_exprs[i];
    if (e.op == other->op &&
       e.left == other->left &&
       e.right == other->right &&
       e.idx == other->idx)
    {
      return other;
    }
  }
  // Since new expressions are only to be created during programming, the
  // above deduplication should always succeed during compilation.
  require_status(PROGRAMMING);
  if (left)
  {
    left->usage++;
  }
  if (right)
  {
    right->usage++;
  }
  m_exprs.push(e);
  return &m_exprs.last_item();
}

Uint32
AggregationAPICompiler::new_agg(AggregationAPICompiler::AggType agg_type,
                                AggregationAPICompiler::Expr* expr)
{
  if (m_status == Status::FAILED)
  {
    return -1; // todo can't return -1 due to return type.
  }
  ndbrequire(m_exprs.has_item(expr));
  ndbrequire(m_status == Status::PROGRAMMING ||
             m_status == Status::COMPILING ||
             m_status == Status::COMPILED);
  AggExpr agg;
  agg.agg_type = agg_type;
  agg.expr = expr;
  // Deduplication
  for (Uint32 i=0; i<m_aggs.size(); i++)
  {
    AggExpr* other = &m_aggs[i];
    if (agg.agg_type == other->agg_type &&
        agg.expr == other->expr)
    {
      return i;
    }
  }
  // Since new aggregates are only to be created during programming, the above
  // deduplication should always succeed during compilation.
  require_status(PROGRAMMING);
  expr->usage++;
  m_aggs.push(agg);
  return m_aggs.size() - 1;
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::Load(Uint32 col_idx)
{
  if (m_status == Status::FAILED)
  {
    return NULL;
  }
  require_status(PROGRAMMING);
  return new_expr(ExprOp::Load, 0, 0, col_idx);
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::ConstantInteger(Int64 int_64)
{
  for (Uint32 idx = 0; idx < m_constants.size(); idx++)
  {
    if (m_constants[idx].int_64 == int_64)
    {
      return new_expr(ExprOp::LoadConstantInt, 0, 0, idx);
    }
  }
  m_constants.push({int_64});
  return new_expr(ExprOp::LoadConstantInt, 0, 0, m_constants.size() - 1);
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::CaseExpr(ConditionalExpression* condition,
                                  Expr* then_expr,
                                  Expr* else_expr)
{
  if (m_status == Status::FAILED) return NULL;
  require_status(PROGRAMMING);
  ndbrequire(then_expr != NULL);
  ndbrequire(else_expr != NULL);
  ndbrequire(m_exprs.has_item(then_expr));
  ndbrequire(m_exprs.has_item(else_expr));
  Expr e;
  e.op = ExprOp::Case;
  e.left = then_expr;
  e.right = else_expr;
  e.case_condition = condition;
  e.idx = 0;
  e.est_regs = 1;
  e.eval_left_first = true;
  then_expr->usage++;
  else_expr->usage++;
  m_exprs.push(e);
  return &m_exprs.last_item();
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::public_arithmetic_expression_helper(ExprOp op,
                                                            Expr* x,
                                                            Expr* y)
{
  if (m_status == Status::FAILED)
  {
    return NULL;
  }
  require_status(PROGRAMMING);
  return new_expr(op, x, y, 0);
}

Uint32
AggregationAPICompiler::public_aggregate_function_helper(AggType agg_type,
                                                         Expr* x)
{
  ndbrequire(m_status != Status::FAILED);
  return new_agg(agg_type, x);
}

/*
 * End of High-level API
 */

/*
 * Start of Symbolic Virtual Machine
 *
 * This VM mirrors the pushdown aggregation VM exactly, except it makes
 * "symbolic" computations, i.e. register values are expressions. This way, we
 * can categorically prove the correctness of the program rather than just
 * test example instances. The SVM is used by the compiler to help guide the
 * compilation and to prove correctness of the produced program.
 */

#define require_reg(REG) ndbrequire((REG) < REGS)

void
AggregationAPICompiler::svm_init()
{
  for (Uint32 i=0; i<REGS; i++)
  {
    r[i] = NULL;
  }
}

#define OPERATOR_CASE(Name) \
  case SVMInstrType::Name: \
    require_reg(dest); require_reg(src); \
    svm_use(dest, is_first_compilation); \
    svm_use(src, is_first_compilation); \
    r[dest]=new_expr(ExprOp::Name, r[dest], r[src], 0); \
    break;
#define AGG_CASE(Name) \
  case SVMInstrType::Name: \
    ndbrequire(dest < m_aggs.size()); \
    require_reg(src); \
    svm_use(src, is_first_compilation); \
    if (m_aggs[dest].expr->op != ExprOp::Case) \
      ndbrequire(m_aggs[dest].expr == r[src]); \
    break;
void
AggregationAPICompiler::svm_execute(AggregationAPICompiler::Instr* instr,
                                    bool is_first_compilation)
{
  SVMInstrType type = instr->type;
  Uint32 dest = instr->dest;
  Uint32 src = instr->src;
  switch (type)
  {
  case SVMInstrType::Load:
    require_reg(dest);
    r[dest]=new_expr(ExprOp::Load, NULL, NULL, src);
    break;
  case SVMInstrType::LoadConstantInteger:
    require_reg(dest);
    r[dest]=new_expr(ExprOp::LoadConstantInt, NULL, NULL, src);
    break;
  case SVMInstrType::Mov:
    require_reg(dest); require_reg(src);
    r[dest]=r[src];
    break;
  FORALL_ARITHMETIC_OPS(OPERATOR_CASE)
  FORALL_PAIR_OPS(OPERATOR_CASE)
  FORALL_AGGS(AGG_CASE)
  case SVMInstrType::EmbeddedInterp:
    break;
  case SVMInstrType::Skip:
    break;
  case SVMInstrType::AggRepeat:
    ndbrequire(dest < m_aggs.size());
    require_reg(src);
    svm_use(src, is_first_compilation);
    break;
  default:
    // Unknown instruction
    abort();
  }
}
# undef OPERATOR_CASE
# undef AGG_CASE

// svm_use communicates to the compiler when a value is used in a calculation
void
AggregationAPICompiler::svm_use(Uint32 reg, bool is_first_compilation)
{
  Expr* value = r[reg];
  ndbrequire(value != NULL);
  if (is_first_compilation)
  {
    ndbrequire(value->usage - value->program_usage > 0);
    value->program_usage++;
  }
}

/*
 * End of Symbolic Virtual Machine
 */

/*
 * Start of Aggregation Compiler
 *
 * The compiler translates the symbolic expressions received via the High-level
 * API to a low-level program that can be executed by the pushdown aggregation
 * VM on data nodes.
 */

#define AGG_CASE(Name) \
    case SVMInstrType::Name: \
      if (m_program[i].dest == next_aggregate) \
        next_aggregate++; \
      else \
        ndbrequire(m_program[i].dest == next_aggregate - 1); \
      break;
bool
AggregationAPICompiler::compile()
{
  if (m_status == Status::FAILED)
  {
    return false;
  }
  require_status(PROGRAMMING);
  m_status = Status::COMPILING;
  svm_init();
  for (Uint32 i=0; i<REGS; i++)
  {
    m_locked[i] = 0;
  }
#ifdef VM_TRACE
  for (Uint32 i=0; i<m_exprs.size(); i++)
  {
    Expr* e = &m_exprs[i];
    ndbrequire(0 < e->usage || e->op == ExprOp::LoadConstantInt);
    ndbrequire(e->program_usage == 0);
    ndbrequire(e->has_been_compiled == false);
  }
#endif
  for (Uint32 i=0; i<m_aggs.size(); i++)
  {
    bool res = compile(&m_aggs[i], i);
    if (!res)
    {
      m_err << "Failed to compile aggregation " << i << endl;
      m_status = Status::FAILED;
      return false;
    }
  }
  for (Uint32 i=0; i<m_exprs.size(); i++)
  {
    if (m_exprs[i].op == ExprOp::Case) continue;
    ndbrequire(m_exprs[i].usage == m_exprs[i].program_usage);
  }
  dead_code_elimination();
  m_status = Status::COMPILED;
  // Check correctness
  svm_init();
  Uint32 next_aggregate = 0;
  for (Uint32 i=0; i<m_program.size(); i++)
  {
    svm_execute(&m_program[i], false);
    switch (m_program[i].type)
    {
    FORALL_AGGS(AGG_CASE)
    case SVMInstrType::AggRepeat:
      ndbrequire(m_program[i].dest == next_aggregate - 1);
      break;
    default:
      void(); // Do nothing
    }
  }
  ndbrequire(next_aggregate == m_aggs.size());
  return true;
}
#undef AGG_CASE

bool
AggregationAPICompiler::compile(AggExpr* agg, Uint32 idx)
{
  require_status(COMPILING);
  if (agg->expr->op == ExprOp::Case)
  {
    Expr* case_expr = agg->expr;
    bool is_first = !case_expr->has_been_compiled;
    case_expr->has_been_compiled = true;

    CaseInfo ci;
    ci.condition = case_expr->case_condition;
    Uint32 case_idx = m_cases.size();
    m_cases.push(ci);

    // EmbeddedInterp placeholder
    Instr emb_instr;
    emb_instr.type = SVMInstrType::EmbeddedInterp;
    emb_instr.dest = case_idx;
    emb_instr.src = 0;
    m_program.push(emb_instr);

    // THEN arm
    m_cases[case_idx].then_start = m_program.size();
    Uint32 then_reg;
    if (!compile(case_expr->left, &then_reg)) return false;
    pushInstr(agg->agg_type, idx, then_reg, is_first);

    // Skip placeholder
    m_cases[case_idx].skip_pos = m_program.size();
    Instr skip_instr;
    skip_instr.type = SVMInstrType::Skip;
    skip_instr.dest = 0;
    skip_instr.src = 0;
    m_program.push(skip_instr);

    // ELSE arm
    m_cases[case_idx].else_start = m_program.size();
    Uint32 else_reg;
    if (!compile(case_expr->right, &else_reg)) return false;
    pushInstr(SVMInstrType::AggRepeat, idx, else_reg, true);
    m_cases[case_idx].else_end = m_program.size();

    // Values loaded inside CASE arms are conditional — they may not have
    // executed at runtime depending on the branch taken. Clear SVM register
    // tracking to prevent subsequent code from reusing them.
    svm_init();

    return true;
  }
  Uint32 reg;
  if (!compile(agg->expr, &reg))
  {
    return false;
  }
  pushInstr(agg->agg_type, idx, reg, true);
  return true;
}

// This function is the most central and brittle part in the compiler. Test
// changes thoroughly!
bool
AggregationAPICompiler::compile(Expr* expr, Uint32* reg)
{
  require_status(COMPILING);
  // If the value already exists in a register then use that.
  for (Uint32 i=0; i<REGS; i++)
  {
    if (r[i] == expr)
    {
      *reg = i;
      return true;
    }
  }
  bool is_first_compilation = !expr->has_been_compiled;
  expr->has_been_compiled = true;
  // Load operations are straight-forward since they only have one register
  // argument.
  if (expr->op == ExprOp::Load)
  {
    if (!seize_register(reg, UINT32_MAX))
    {
      return false;
    }
    require_reg(*reg);
    pushInstr(SVMInstrType::Load, *reg, expr->idx, is_first_compilation);
    return true;
  }
  if (expr->op == ExprOp::LoadConstantInt)
  {
    if (!seize_register(reg, UINT32_MAX))
    {
      return false;
    }
    require_reg(*reg);
    pushInstr(SVMInstrType::LoadConstantInteger, *reg, expr->idx, is_first_compilation);
    return true;
  }
  // The rest of the logic is about arithmetic operations and optimization.
  Uint32 dest=0, src=0;
  if (expr->left == expr->right)
  {
    if (!compile(expr->left, &dest))
    {
      return false;
    }
    require_reg(dest);
    src = dest;
    m_locked[dest]++;
    m_locked[src]++; // Yes, this will lock the same register twice.
  }
  else if (expr->eval_left_first)
  {
    if (!compile(expr->left, &dest))
    {
      return false;
    }
    require_reg(dest);
    m_locked[dest]++;
    if (!compile(expr->right, &src))
    {
      return false;
    }
    require_reg(src);
    m_locked[src]++;
  }
  else
  {
    if (!compile(expr->right, &src))
    {
      return false;
    }
    require_reg(src);
    m_locked[src]++;
    if (!compile(expr->left, &dest))
    {
      return false;
    }
    require_reg(dest);
    m_locked[dest]++;
  }
  // At this point, dest and src are both registers containing the correct
  // values, and both are locked. If they are the same register, it's locked
  // twice.
  ndbrequire(r[dest] == expr->left); ndbrequire(r[src] == expr->right);
  ndbrequire(m_locked[dest]); ndbrequire(m_locked[src]);
  if (dest == src)
  {
    ndbrequire(m_locked[dest] >= 2);
  }
  if (expr->left->usage - expr->left->program_usage > (dest == src ? 2 : 1))
  {
    // Destination holds a value that we'll need later.
    // Before writing to destination, try to save a copy.
    bool copy_already_exists = false;
    for (Uint32 i=0; i<REGS; i++)
    {
      if (i != dest && r[i] == expr->left)
      {
        copy_already_exists = true;
        break;
      }
    }
    if (!copy_already_exists)
    {
      Uint32 new_reg;
      if (seize_register(&new_reg,
                        estimated_cost_of_recalculating(expr->left, dest)))
      {
        require_reg(new_reg);
        ndbrequire(r[dest] == expr->left);
        pushInstr(SVMInstrType::Mov, new_reg, dest, is_first_compilation);
        ndbrequire(r[new_reg] == expr->left);
        ndbrequire(r[dest] == expr->left);
      }
    }
  }
  if (m_locked[dest] > (dest == src ? 2 : 1))
  {
    // Destination register is not writable after removing our locks, so we
    // need to select another destination register.
    Uint32 new_dest = 0;
    bool copy_already_exists = false;
    for (Uint32 i=0; i<REGS; i++)
    {
      if (r[i] == expr->left && m_locked[i] == 0)
      {
        new_dest = i;
        copy_already_exists = true;
        break;
      }
    }
    if (!copy_already_exists)
    {
      if (!seize_register(&new_dest, UINT32_MAX))
      {
        return false;
      }
    }
    require_reg(new_dest);
    ndbrequire(m_locked[new_dest] == 0);
    if (r[new_dest] != expr->left)
    {
      pushInstr(SVMInstrType::Mov, new_dest, dest, is_first_compilation);
    }
    m_locked[new_dest]++;
    m_locked[dest]--;
    dest = new_dest;
  }
  ndbrequire(r[dest] == expr->left);
  ndbrequire(m_locked[dest] == (dest == src ? 2 : 1));
  m_locked[dest]--;
  ndbrequire(r[src] == expr->right);
  ndbrequire(m_locked[src] >= 1);
  m_locked[src]--;
  pushInstr(expr->op, dest, src, is_first_compilation);
  ndbrequire(r[dest] == expr);
  *reg = dest;
  return true;
}

/*
  Choose a register suitable for writing a value. The register is chosen to
  minimize the expected cost of recalculating the value it holds. Return false
  if no register could be found that is unlocked and has an estimated cost of
  recalculation no larger than max_cost.
 */
bool
AggregationAPICompiler::seize_register(Uint32* reg, Uint32 max_cost)
{
  require_status(COMPILING);
  Uint32 cost[REGS];
  Uint32 min_cost = UINT32_MAX;
  Uint32 ret = 0;
  for (Uint32 i=0; i<REGS; i++)
  {
    if (m_locked[i])
    {
      cost[i] = UINT32_MAX;
    }
    else if (r[i] == NULL)
    {
      cost[i] = 0;
    }
    else if (r[i]->usage == r[i]->program_usage)
    {
      cost[i] = 0;
    }
    else
    {
      cost[i] = estimated_cost_of_recalculating(r[i], i);
    }
    if (cost[i] < min_cost)
    {
      min_cost = cost[i];
      ret = i;
    }
  }
  if (!m_locked[ret] && cost[ret] <= max_cost)
  {
    require_reg(ret);
    *reg = ret;
    return true;
  }
  m_err << "No suitable registers." << endl;
  return false;
}

/*
  Estimate the number of instructions needed to calculate expr, given the
  current registers except for `without_using_reg'. This estimate does not
  account for reusing expressions, so real_cost <= estimated_cost.
 */
uint
AggregationAPICompiler::estimated_cost_of_recalculating(Expr* expr,
                                                        Uint32 without_using_reg)
{
  if (expr == NULL)
  {
    return 0;
  }
  for (Uint32 i=0; i<REGS; i++)
  {
    if (i == without_using_reg)
    {
      continue;
    }
    if (r[i] == expr)
    {
      return 0;
    }
  }
  if (expr->op == ExprOp::Load)
  {
    return 1;
  }
  return 1 +
    estimated_cost_of_recalculating(expr->left, without_using_reg) +
    estimated_cost_of_recalculating(expr->right, without_using_reg);
}

void
AggregationAPICompiler::pushInstr(SVMInstrType type,
                                  Uint32 dest,
                                  Uint32 src,
                                  bool is_first_compilation)
{
  require_status(COMPILING);
  Instr instr;
  instr.type = type;
  instr.dest = dest;
  instr.src = src;
  m_program.push(instr);
  svm_execute(&m_program.last_item(), is_first_compilation);
}

#define AGG_CASE(Name) \
      case AggType::Name: instr = SVMInstrType::Name; break;
void
AggregationAPICompiler::pushInstr(AggType type,
                                  Uint32 dest,
                                  Uint32 src,
                                  bool is_first_compilation)
{
  require_status(COMPILING);
  SVMInstrType instr;
  switch (type)
  {
    FORALL_AGGS(AGG_CASE)
    default:
      // Unknown aggregation type
      abort();
  }
  pushInstr(instr, dest, src, is_first_compilation);
}
#undef AGG_CASE

#define OP_CASE(Name) case ExprOp::Name: instr = SVMInstrType::Name; break;
void
AggregationAPICompiler::pushInstr(ExprOp op,
                                  Uint32 dest,
                                  Uint32 src,
                                  bool is_first_compilation)
{
  require_status(COMPILING);
  SVMInstrType instr;
  switch (op)
  {
    FORALL_ARITHMETIC_OPS(OP_CASE)
    FORALL_PAIR_OPS(OP_CASE)
    default:
      // Unknown operation
      abort();
  }
  pushInstr(instr, dest, src, is_first_compilation);
}
#undef OP_CASE

#define OPERATOR_CASE(Name) \
    case SVMInstrType::Name: \
      require_reg(dest); require_reg(src); \
      this_instr_is_useful = reg_needed[dest]; \
      if (this_instr_is_useful) \
      { \
        reg_needed[dest] = true; \
        reg_needed[src] = true; \
      } \
      break;
#define AGG_CASE(Name) \
    case SVMInstrType::Name: \
      ndbrequire(dest < m_aggs.size()); \
      require_reg(src); \
      this_instr_is_useful = true; \
      reg_needed[src] = true; \
      break;
void
AggregationAPICompiler::dead_code_elimination()
{
  if (m_program.size() == 0) return;
  // We identify dead code by traversing the program in reverse while keeping
  // track of what registers will be used later. At end of program, where we
  // begin traversing, no registers will be used later.
  bool reg_needed[REGS];
  for (Uint32 i=0; i<REGS; i++)
  {
    reg_needed[i] = false;
  }
  bool* instr_useful = m_amalloc->alloc_exc<bool>(m_program.size());
  for (Uint32 i=0; i<m_program.size(); i++)
  {
    instr_useful[i] = false;
  }
  bool dead_code_found = false;
  for (Uint32 mark = m_program.size(); mark > 0; mark--)
  {
    Uint32 idx = mark - 1;
    Instr* instr = &m_program[idx];
    bool this_instr_is_useful;
    SVMInstrType type = instr->type;
    Uint32 dest = instr->dest;
    Uint32 src = instr->src;
    // reg_needed specifies what registers are needed *after* this
    // instruction. For this instruction type, use reg_needed to determine
    // whether the instructon does useful work, then adjust reg_needed to
    // specify what registers are needed *before* this instruction.
    switch (type)
    {
    case SVMInstrType::Load:
    case SVMInstrType::LoadConstantInteger:
      require_reg(dest);
      this_instr_is_useful = reg_needed[dest];
      if (this_instr_is_useful)
      {
        reg_needed[dest] = false;
      }
      break;
    case SVMInstrType::Mov:
      require_reg(dest); require_reg(src);
      this_instr_is_useful = reg_needed[dest];
      if (this_instr_is_useful)
      {
        reg_needed[dest] = false;
        reg_needed[src] = true;
      }
      break;
    FORALL_ARITHMETIC_OPS(OPERATOR_CASE)
    FORALL_PAIR_OPS(OPERATOR_CASE)
    FORALL_AGGS(AGG_CASE)
    case SVMInstrType::AggRepeat:
      ndbrequire(dest < m_aggs.size());
      require_reg(src);
      this_instr_is_useful = true;
      reg_needed[src] = true;
      break;
    case SVMInstrType::EmbeddedInterp:
    case SVMInstrType::Skip:
      this_instr_is_useful = true;
      break;
    default:
      // Unknown instruction
      abort();
    }
    if (this_instr_is_useful)
    {
      instr_useful[idx] = true;
    }
    else
    {
      // We believe the compiler will not generate useless instructions of any
      // other type than Mov.
      ndbrequire(type == SVMInstrType::Mov);
      dead_code_found = true;
    }
  }
  if (dead_code_found)
  {
    DynamicArray<Instr> old_program = m_program;
    m_program.truncate();
    svm_init();
    for (Uint32 i=0; i<old_program.size(); i++)
    {
      if (instr_useful[i])
      {
        Instr instr = old_program[i];
        pushInstr(instr.type, instr.dest, instr.src, false);
      }
    }
  }
}
#undef OPERATOR_CASE
#undef AGG_CASE

/*
 * End of Aggregation Compiler
 */

/*
 * Start of Aggregation Program Printer
 *
 * This can be called after compilation to print both the high-level and
 * low-level program.
 */

void
AggregationAPICompiler::print_aggregates()
{
  require_status(COMPILED);
  m_out << "Aggregations:\n";
  for (Uint32 i=0; i<m_aggs.size(); i++)
  {
    m_out << 'A' << i << '=';
    print_aggregate(i);
    m_out << '\n';
  }
}

#define AGG_CASE(Name) \
  case AggType::Name: \
    m_out << #Name "("; \
    print(m_aggs[idx].expr); \
    m_out << ')'; \
    break;
void
AggregationAPICompiler::print_aggregate(Uint32 idx)
{
  switch (m_aggs[idx].agg_type)
  {
    FORALL_AGGS(AGG_CASE)
  default:
    // Unknown aggregation
      abort();
  }
}
#undef AGG_CASE

void
AggregationAPICompiler::print_program()
{
  if (m_program.size() == 0)
  {
    m_out << "No aggregation program.\n\n";
    return;
  }
  svm_init();
  m_out << "Aggregation program (" << m_program.size() << " instructions):\n"
        << "Instr. DEST SRC DESCRIPTION\n";
  for (Uint32 i=0; i<m_program.size(); i++)
  {
    print(&m_program[i]);
    svm_execute(&m_program[i], false);
  }
}

DEFINE_FORMATTER(quoted_identifier, char*, {
  const char* iter = value;
  os.put('`');
  while (*iter != '\0')
  {
    if (*iter == '`')
      os.write("``", 2);
    else
      os.put(*iter);
    iter++;
  }
  os.put('`');
})

void
AggregationAPICompiler::print(Expr* expr)
{
  if (expr == NULL)
  {
    m_out << "<EMPTY>";
    return;
  }
  if (expr->op == AggregationAPICompiler::ExprOp::Load)
  {
    m_out << quoted_identifier(m_column_idx_to_name(expr->idx));
    return;
  }
  if (expr->op == AggregationAPICompiler::ExprOp::LoadConstantInt)
  {
    m_out << m_constants[expr->idx].int_64;
    return;
  }
  if (expr->op == AggregationAPICompiler::ExprOp::Greatest2)
  {
    m_out << "GREATEST(";
    print(expr->left);
    m_out << ", ";
    print(expr->right);
    m_out << ')';
    return;
  }
  if (expr->op == AggregationAPICompiler::ExprOp::Least2)
  {
    m_out << "LEAST(";
    print(expr->left);
    m_out << ", ";
    print(expr->right);
    m_out << ')';
    return;
  }
  m_out << '(';
  print(expr->left);
  switch (expr->op)
  {
  case ExprOp::Add: m_out << " + "; break;
  case ExprOp::Minus: m_out << " - "; break;
  case ExprOp::Mul: m_out << " * "; break;
  case ExprOp::Div: m_out << " / "; break;
  case ExprOp::DivInt: m_out << " DIV "; break;
  case ExprOp::Rem: m_out << " % "; break;
  default:
    // Unknown operation
    abort();
  }
  print(expr->right);
  m_out << ')';
}

DEFINE_FORMATTER(s6, char*, {
  os << value;
  for (Uint32 i = strlen(value); i < 6; i++) os << ' ';
})
DEFINE_FORMATTER(d2, uint, {
  if (value < 10) os << '0';
  os << value;
})

#define OPERATOR_CASE(Name) \
  case SVMInstrType::Name: \
    require_reg(dest); require_reg(src); \
    m_out << s6(#Name) << " r" << d2(dest) << "  r" << d2(src) << " r" << \
      d2(dest) << ":"; \
    print(r[dest]); \
    m_out << ' ' << relstr_##Name << "= r" << d2(src) << ':'; \
    print(r[src]); \
    break;
#define AGG_CASE(Name) \
  case SVMInstrType::Name: \
    ndbrequire(dest < m_aggs.size()); \
    require_reg(src); \
    m_out << s6(#Name) << " A" << d2(dest) << "  r" << d2(src) << " A" << \
      d2(dest) << ":" << ucasestr_##Name << " <- r" << d2(src) << ':'; \
    print(r[src]); \
    break;
void
AggregationAPICompiler::print(Instr* instr)
{
  Uint32 dest = instr->dest;
  Uint32 src = instr->src;
  static const char* relstr_Add = "+";
  static const char* relstr_Minus = "-";
  static const char* relstr_Mul = "*";
  static const char* relstr_Div = "/";
  static const char* relstr_DivInt = "DIV";
  static const char* relstr_Rem = "%";
  static const char* relstr_Greatest2 = "GREATEST";
  static const char* relstr_Least2 = "LEAST";
  static const char* ucasestr_Sum = "SUM";
  static const char* ucasestr_Min = "MIN";
  static const char* ucasestr_Max = "MAX";
  static const char* ucasestr_Count = "COUNT";
  switch (instr->type)
  {
  case SVMInstrType::Load:
    require_reg(dest);
    m_out << "Load   r" << d2(dest) << "  C" << d2(src) << " r" << d2(dest) <<
      " = C" << d2(src) << ':' << quoted_identifier(m_column_idx_to_name(src));
    break;
  case SVMInstrType::LoadConstantInteger:
    require_reg(dest);
    m_out << "LoadI  r" << d2(dest) << "  I" << d2(src) << " r" << d2(dest) <<
      " = I" << d2(src) << ':' << m_constants[src].int_64;
    break;
  case SVMInstrType::Mov:
    require_reg(dest); require_reg(src);
    m_out << "Mov    r" << d2(dest) << "  r" << d2(src) << " r" << d2(dest) <<
      " = r" << d2(src) << ':';
    print(r[src]);
    break;
  FORALL_ARITHMETIC_OPS(OPERATOR_CASE)
  FORALL_PAIR_OPS(OPERATOR_CASE)
  FORALL_AGGS(AGG_CASE)
  case SVMInstrType::AggRepeat:
    ndbrequire(dest < m_aggs.size());
    require_reg(src);
    m_out << "AggRep A" << d2(dest) << "  r" << d2(src);
    break;
  case SVMInstrType::EmbeddedInterp:
    m_out << "EmbInt C" << d2(dest) << "  ---";
    break;
  case SVMInstrType::Skip:
    m_out << "Skip   ---  " << d2(src);
    break;
  default:
    // Unknown instruction
    abort();
  }
  m_out << '\n';
}
#undef OPERATOR_CASE
#undef AGG_CASE

/*
 * End of Aggregation Program Printer
 */

Uint32
AggregationAPICompiler::raw_word_size(Uint32 start, Uint32 end)
{
  Uint32 count = 0;
  for (Uint32 i = start; i < end; i++)
  {
    SVMInstrType t = m_program[i].type;
    if (t == SVMInstrType::LoadConstantInteger)
      count += 3;
    else if (t == SVMInstrType::Greatest2 || t == SVMInstrType::Least2)
      // Pair-op expands to EmbeddedInterp(9-word program) + Mov.
      count += 11;
    else
      count += 1;
  }
  return count;
}
