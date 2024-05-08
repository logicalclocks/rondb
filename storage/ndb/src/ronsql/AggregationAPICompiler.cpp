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
#include <assert.h>
#include <cstring>
#include "AggregationAPICompiler.hpp"
#define UINT_MAX ((uint)0xffffffff)
using std::endl;
using std::max;

AggregationAPICompiler::AggregationAPICompiler
    (std::function<const char*(uint)> column_idx_to_name,
     std::basic_ostream<char>& out,
     std::basic_ostream<char>& err,
     ArenaAllocator* aalloc):
  m_out(out),
  m_err(err),
  m_aalloc(aalloc),
  m_column_idx_to_name(column_idx_to_name),
  m_exprs(aalloc),
  m_aggs(aalloc),
  m_constants(aalloc),
  m_program(aalloc)
{}

AggregationAPICompiler::Status
AggregationAPICompiler::getStatus()
{
  return m_status;
}

#define assert_status(name) assert(m_status == Status::name)

/*
 * Start of High-level API
 *
 * This is the high-level API used to construct a pushdown aggregation
 * program.
 */

AggregationAPICompiler::Expr*
AggregationAPICompiler::new_expr(ExprOp op,
                                 Expr* left,
                                 Expr* right,
                                 uint idx)
{
  if (m_status == Status::FAILED)
  {
    return NULL;
  }
  assert(m_status == Status::PROGRAMMING ||
         m_status == Status::COMPILING ||
         m_status == Status::COMPILED);
  assert(left == NULL || m_exprs.has_item(left));
  assert(right == NULL || m_exprs.has_item(right));
  Expr e;
  e.op = op;
  e.left = left;
  e.right = right;
  e.idx = idx;
  if (op == ExprOp::Load ||
      op == ExprOp::LoadConstantInt)
  {
    assert(left == NULL);
    assert(right == NULL);
    e.est_regs = 1;
  }
  else
  {
    assert(left != NULL);
    assert(right != NULL);
    assert(idx == 0);
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
  // Constant folding
  if (left != NULL &&
      left->op == ExprOp::LoadConstantInt &&
      right != NULL &&
      right->op == ExprOp::LoadConstantInt)
  {
    long int arg1 = m_constants[left->idx].long_int;
    long int arg2 = m_constants[right->idx].long_int;
    long int result = 0;
    switch (op)
    {
    case ExprOp::Add:
      result = arg1 + arg2;
      break;
    case ExprOp::Minus:
      result = arg1 - arg2;
      break;
    case ExprOp::Mul:
      result = arg1 * arg2;
      break;
    case ExprOp::Div:
      result = arg1 / arg2;
      break;
    case ExprOp::Rem:
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
  for (uint i=0; i<m_exprs.size(); i++)
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
  assert_status(PROGRAMMING);
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

int
AggregationAPICompiler::new_agg(AggregationAPICompiler::AggType agg_type,
                                AggregationAPICompiler::Expr* expr)
{
  if (m_status == Status::FAILED)
  {
    return -1;
  }
  assert(m_exprs.has_item(expr));
  assert(m_status == Status::PROGRAMMING ||
         m_status == Status::COMPILING ||
          m_status == Status::COMPILED);
  AggExpr agg;
  agg.agg_type = agg_type;
  agg.expr = expr;
  // Deduplication
  for (uint i=0; i<m_aggs.size(); i++)
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
  assert_status(PROGRAMMING);
  expr->usage++;
  m_aggs.push(agg);
  return m_aggs.size() - 1;
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::Load(uint col_idx)
{
  if (m_status == Status::FAILED)
  {
    return NULL;
  }
  assert_status(PROGRAMMING);
  return new_expr(ExprOp::Load, 0, 0, col_idx);
}

AggregationAPICompiler::Expr*
AggregationAPICompiler::ConstantInteger(long int long_int)
{
  for (uint idx = 0; idx < m_constants.size(); idx++)
  {
    if (m_constants[idx].long_int == long_int)
    {
      return new_expr(ExprOp::LoadConstantInt, 0, 0, idx);
    }
  }
  m_constants.push({long_int});
  return new_expr(ExprOp::LoadConstantInt, 0, 0, m_constants.size() - 1);
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
  assert_status(PROGRAMMING);
  return new_expr(op, x, y, 0);
}

int
AggregationAPICompiler::public_aggregate_function_helper(AggType agg_type,
                                                         Expr* x)
{
  if (m_status == Status::FAILED)
  {
    return -1;
  }
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

#define assert_reg(REG) assert((REG) < REGS)

void
AggregationAPICompiler::svm_init()
{
  for (uint i=0; i<REGS; i++)
  {
    r[i] = NULL;
  }
}

#define OPERATOR_CASE(Name) \
  case SVMInstrType::Name: \
    assert_reg(dest); assert_reg(src); \
    svm_use(dest, is_first_compilation); \
    svm_use(src, is_first_compilation); \
    r[dest]=new_expr(ExprOp::Name, r[dest], r[src], 0); \
    break;
#define AGG_CASE(Name) \
  case SVMInstrType::Name: \
    assert(dest < m_aggs.size()); \
    assert_reg(src); \
    svm_use(src, is_first_compilation); \
    assert(m_aggs[dest].expr == r[src]); \
    break;
void
AggregationAPICompiler::svm_execute(AggregationAPICompiler::Instr* instr,
                                    bool is_first_compilation)
{
  SVMInstrType type = instr->type;
  uint dest = instr->dest;
  uint src = instr->src;
  switch (type)
  {
  case SVMInstrType::Load:
    assert_reg(dest);
    r[dest]=new_expr(ExprOp::Load, NULL, NULL, src);
    break;
  case SVMInstrType::LoadConstantInteger:
    assert_reg(dest);
    r[dest]=new_expr(ExprOp::LoadConstantInt, NULL, NULL, src);
    break;
  case SVMInstrType::Mov:
    assert_reg(dest); assert_reg(src);
    r[dest]=r[src];
    break;
  FORALL_ARITHMETIC_OPS(OPERATOR_CASE)
  FORALL_AGGS(AGG_CASE)
  default:
    // Unknown instruction
    abort();
  }
}
# undef OPERATOR_CASE
# undef AGG_CASE

// svm_use communicates to the compiler when a value is used in a calculation
void
AggregationAPICompiler::svm_use(uint reg, bool is_first_compilation)
{
  Expr* value = r[reg];
  assert(value != NULL);
  if (is_first_compilation)
  {
    assert(value->usage - value->program_usage > 0);
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
      assert(m_program[i].dest == next_aggregate); \
      next_aggregate++; \
      break;
bool
AggregationAPICompiler::compile()
{
  if (m_status == Status::FAILED)
  {
    return false;
  }
  assert_status(PROGRAMMING);
  m_status = Status::COMPILING;
  svm_init();
  for (uint i=0; i<REGS; i++)
  {
    m_locked[i] = 0;
  }
  for (uint i=0; i<m_exprs.size(); i++)
  {
    Expr* e = &m_exprs[i];
    assert(0 < e->usage || e->op == ExprOp::LoadConstantInt);
    assert(e->program_usage == 0);
    assert(e->has_been_compiled == false);
  }
  for (uint i=0; i<m_aggs.size(); i++)
  {
    bool res = compile(&m_aggs[i], i);
    if (!res)
    {
      m_err << "Failed to compile aggregation " << i << endl;
      m_status = Status::FAILED;
      return false;
    }
  }
  for (uint i=0; i<m_exprs.size(); i++)
  {
    assert(m_exprs[i].usage == m_exprs[i].program_usage);
  }
  dead_code_elimination();
  m_status = Status::COMPILED;
  // Assert correctness
  svm_init();
  uint next_aggregate = 0;
  for (uint i=0; i<m_program.size(); i++)
  {
    svm_execute(&m_program[i], false);
    switch (m_program[i].type)
    {
    FORALL_AGGS(AGG_CASE)
    default:
      void(); // Do nothing
    }
  }
  assert(next_aggregate == m_aggs.size());
  return true;
}
#undef AGG_CASE

bool
AggregationAPICompiler::compile(AggExpr* agg, int idx)
{
  assert_status(COMPILING);
  uint reg;
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
AggregationAPICompiler::compile(Expr* expr, uint* reg)
{
  assert_status(COMPILING);
  // If the value already exists in a register then use that.
  for (int i=0; i<REGS; i++)
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
    if (!seize_register(reg, UINT_MAX))
    {
      return false;
    }
    assert_reg(*reg);
    pushInstr(SVMInstrType::Load, *reg, expr->idx, is_first_compilation);
    return true;
  }
  if (expr->op == ExprOp::LoadConstantInt)
  {
    if (!seize_register(reg, UINT_MAX))
    {
      return false;
    }
    assert_reg(*reg);
    pushInstr(SVMInstrType::LoadConstantInteger, *reg, expr->idx, is_first_compilation);
    return true;
  }
  // The rest of the logic is about arithmetic operations and optimization.
  uint dest=0, src=0;
  if (expr->left == expr->right)
  {
    if (!compile(expr->left, &dest))
    {
      return false;
    }
    assert_reg(dest);
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
    assert_reg(dest);
    m_locked[dest]++;
    if (!compile(expr->right, &src))
    {
      return false;
    }
    assert_reg(src);
    m_locked[src]++;
  }
  else
  {
    if (!compile(expr->right, &src))
    {
      return false;
    }
    assert_reg(src);
    m_locked[src]++;
    if (!compile(expr->left, &dest))
    {
      return false;
    }
    assert_reg(dest);
    m_locked[dest]++;
  }
  // At this point, dest and src are both registers containing the correct
  // values, and both are locked. If they are the same register, it's locked
  // twice.
  assert(r[dest] == expr->left); assert(r[src] == expr->right);
  assert(m_locked[dest]); assert(m_locked[src]);
  if (dest == src)
  {
    assert(m_locked[dest] >= 2);
  }
  if (expr->left->usage - expr->left->program_usage > (dest == src ? 2 : 1))
  {
    // Destination holds a value that we'll need later.
    // Before writing to destination, try to save a copy.
    bool copy_already_exists = false;
    for (uint i=0; i<REGS; i++)
    {
      if (i != dest && r[i] == expr->left)
      {
        copy_already_exists = true;
        break;
      }
    }
    if (!copy_already_exists)
    {
      uint new_reg;
      if (seize_register(&new_reg,
                        estimated_cost_of_recalculating(expr->left, dest)))
      {
        assert_reg(new_reg);
        assert(r[dest] == expr->left);
        pushInstr(SVMInstrType::Mov, new_reg, dest, is_first_compilation);
        assert(r[new_reg] == expr->left);
        assert(r[dest] == expr->left);
      }
    }
  }
  if (m_locked[dest] > (dest == src ? 2 : 1))
  {
    // Destination register is not writable after removing our locks, so we
    // need to select another destination register.
    uint new_dest;
    bool copy_already_exists = false;
    for (uint i=0; i<REGS; i++)
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
      if (!seize_register(&new_dest, UINT_MAX))
      {
        return false;
      }
    }
    assert_reg(new_dest);
    assert(m_locked[new_dest] == 0);
    if (r[new_dest] != expr->left)
    {
      pushInstr(SVMInstrType::Mov, new_dest, dest, is_first_compilation);
    }
    m_locked[new_dest]++;
    m_locked[dest]--;
    dest = new_dest;
  }
  assert(r[dest] == expr->left);
  assert(m_locked[dest] == (dest == src ? 2 : 1));
  m_locked[dest]--;
  assert(r[src] == expr->right);
  assert(m_locked[src] >= 1);
  m_locked[src]--;
  pushInstr(expr->op, dest, src, is_first_compilation);
  assert(r[dest] == expr);
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
AggregationAPICompiler::seize_register(uint* reg, uint max_cost)
{
  assert_status(COMPILING);
  uint cost[REGS];
  uint min_cost = UINT_MAX;
  uint ret = 0;
  for (uint i=0; i<REGS; i++)
  {
    if (m_locked[i])
    {
      cost[i] = UINT_MAX;
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
    assert_reg(ret);
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
                                                        uint without_using_reg)
{
  if (expr == NULL)
  {
    return 0;
  }
  for (uint i=0; i<REGS; i++)
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
                                  uint dest,
                                  uint src,
                                  bool is_first_compilation)
{
  assert_status(COMPILING);
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
                                  uint dest,
                                  uint src,
                                  bool is_first_compilation)
{
  assert_status(COMPILING);
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
                                  uint dest,
                                  uint src,
                                  bool is_first_compilation)
{
  assert_status(COMPILING);
  SVMInstrType instr;
  switch (op)
  {
    FORALL_ARITHMETIC_OPS(OP_CASE)
    default:
      // Unknown operation
      abort();
  }
  pushInstr(instr, dest, src, is_first_compilation);
}
#undef OP_CASE

#define OPERATOR_CASE(Name) \
    case SVMInstrType::Name: \
      assert_reg(dest); assert_reg(src); \
      this_instr_is_useful = reg_needed[dest]; \
      if (this_instr_is_useful) \
      { \
        reg_needed[dest] = true; \
        reg_needed[src] = true; \
      } \
      break;
#define AGG_CASE(Name) \
    case SVMInstrType::Name: \
      assert(dest < m_aggs.size()); \
      assert_reg(src); \
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
  for (uint i=0; i<REGS; i++)
  {
    reg_needed[i] = false;
  }
  bool* instr_useful = m_aalloc->alloc<bool>(m_program.size());
  for (uint i=0; i<m_program.size(); i++)
  {
    instr_useful[i] = false;
  }
  bool dead_code_found = false;
  for (uint mark = m_program.size(); mark > 0; mark--)
  {
    uint idx = mark - 1;
    Instr* instr = &m_program[idx];
    bool this_instr_is_useful;
    SVMInstrType type = instr->type;
    uint dest = instr->dest;
    uint src = instr->src;
    // reg_needed specifies what registers are needed *after* this
    // instruction. For this instruction type, use reg_needed to determine
    // whether the instructon does useful work, then adjust reg_needed to
    // specify what registers are needed *before* this instruction.
    switch (type)
    {
    case SVMInstrType::Load:
    case SVMInstrType::LoadConstantInteger:
      assert_reg(dest);
      this_instr_is_useful = reg_needed[dest];
      if (this_instr_is_useful)
      {
        reg_needed[dest] = false;
      }
      break;
    case SVMInstrType::Mov:
      assert_reg(dest); assert_reg(src);
      this_instr_is_useful = reg_needed[dest];
      if (this_instr_is_useful)
      {
        reg_needed[dest] = false;
        reg_needed[src] = true;
      }
      break;
    FORALL_ARITHMETIC_OPS(OPERATOR_CASE)
    FORALL_AGGS(AGG_CASE)
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
      assert(type == SVMInstrType::Mov);
      dead_code_found = true;
    }
  }
  if (dead_code_found)
  {
    DynamicArray<Instr> old_program = m_program;
    m_program.truncate();
    svm_init();
    for (uint i=0; i<old_program.size(); i++)
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
  assert_status(COMPILED);
  m_out << "Aggregations:\n";
  for (uint i=0; i<m_aggs.size(); i++)
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
AggregationAPICompiler::print_aggregate(int idx)
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
  for (uint i=0; i<m_program.size(); i++)
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
    m_out << m_constants[expr->idx].long_int;
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
  case ExprOp::Rem: m_out << " %% "; break;
  default:
    // Unknown operation
    abort();
  }
  print(expr->right);
  m_out << ')';
}

DEFINE_FORMATTER(s5, char*, {
  os << value;
  for (int i = strlen(value); i < 5; i++) os << ' ';
})
DEFINE_FORMATTER(d2, uint, {
  if (value < 10) os << '0';
  os << value;
})

#define OPERATOR_CASE(Name) \
  case SVMInstrType::Name: \
    assert_reg(dest); assert_reg(src); \
    m_out << s5(#Name) << "  r" << d2(dest) << "  r" << d2(src) << " r" << \
      d2(dest) << ":"; \
    print(r[dest]); \
    m_out << ' ' << relstr_##Name << "= r" << d2(src) << ':'; \
    print(r[src]); \
    break;
#define AGG_CASE(Name) \
  case SVMInstrType::Name: \
    assert(dest < m_aggs.size()); \
    assert_reg(src); \
    m_out << s5(#Name) << "  A" << d2(dest) << "  r" << d2(src) << " A" << \
      d2(dest) << ":" << ucasestr_##Name << " <- r" << d2(src) << ':'; \
    print(r[src]); \
    break;
void
AggregationAPICompiler::print(Instr* instr)
{
  uint dest = instr->dest;
  uint src = instr->src;
  static const char* relstr_Add = "+";
  static const char* relstr_Minus = "-";
  static const char* relstr_Mul = "*";
  static const char* relstr_Div = "/";
  static const char* relstr_Rem = "%";
  static const char* ucasestr_Sum = "SUM";
  static const char* ucasestr_Min = "MIN";
  static const char* ucasestr_Max = "MAX";
  static const char* ucasestr_Count = "COUNT";
  switch (instr->type)
  {
  case SVMInstrType::Load:
    assert_reg(dest);
    m_out << "Load   r" << d2(dest) << "  C" << d2(src) << " r" << d2(dest) <<
      " = C" << d2(src) << ':' << quoted_identifier(m_column_idx_to_name(src));
    break;
  case SVMInstrType::LoadConstantInteger:
    assert_reg(dest);
    m_out << "LoadI  r" << d2(dest) << "  I" << d2(src) << " r" << d2(dest) <<
      " = I" << d2(src) << ':' << m_constants[src].long_int;
    break;
  case SVMInstrType::Mov:
    assert_reg(dest); assert_reg(src);
    m_out << "Mov    r" << d2(dest) << "  r" << d2(src) << " r" << d2(dest) <<
      " = r" << d2(src) << ':';
    print(r[src]);
    break;
  FORALL_ARITHMETIC_OPS(OPERATOR_CASE)
  FORALL_AGGS(AGG_CASE)
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
