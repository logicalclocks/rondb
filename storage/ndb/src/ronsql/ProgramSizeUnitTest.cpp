/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

/*
 * T-16, F-22: the word counts RonSQL keeps next to its emitters against
 * the programs the emitters write, run without a cluster.  A CASE skips
 * its arms by counted words (AggregationAPICompiler::raw_word_size), and
 * an embedded interpreter program is preceded by its counted length
 * (generate_embedded_condition's per-atom word_count, the pair-op sizes,
 * embedded_filter_expr_word_count); a count that differs from what is
 * emitted makes a skip land inside an instruction (D-1, F-3).
 *
 * A statement is prepared for EXPLAIN without a connection, its columns
 * are resolved against an NdbDictionary::Table built here (as
 * load_single_table does from the dictionary), and the single-table
 * emitter (programAggregator) writes the program into an NdbAggregator
 * over that table.  The program is then walked with the instruction
 * lengths the data nodes use (aggInstructionWords, and
 * Interpreter::getInstructionPreProcessingInfo inside an embedded
 * program):
 *  - every SVM instruction other than a CASE condition emits
 *    raw_word_size(i, i + 1) words;
 *  - an embedded program's instructions take exactly its counted length,
 *    and its branches land on its instructions;
 *  - every skip (a Skip, or an embedded program's output) lands on an
 *    instruction or the end of the program;
 *  - a CASE condition skips 0 words to its THEN arm and the THEN arm's
 *    words to its ELSE arm, and the THEN arm's Skip lands after the ELSE
 *    arm.
 * Not covered: the join emitter (programAggregator_join) and the
 * multi-leaf subquery path, which need a join plan.
 */

#include "RonSQLPreparer.hpp"
#include "RonSQLParser.y.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <new>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <kernel/Interpreter.hpp>
#include <ndb_init.h>
#include "mysql/strings/m_ctype.h"

// The private members of RonSQLPreparer this test calls (a friend).
class RonSQLPreparerTestAccess
{
public:
  typedef RonSQLPreparer::QueryScope::ResolvedColumnRef ColumnRef;

  /*
   * Resolve the statement's columns against table, as load_single_table()
   * does from the dictionary.  Every column is on join operation leaf_idx,
   * the one named linked_column (if any) on operation 0, the parent of a
   * cross-table filter.
   */
  static void resolve_columns(RonSQLPreparer& preparer,
                              const NdbDictionary::Table* table,
                              Uint32 leaf_idx,
                              const char* linked_column)
  {
    const Uint32 count = preparer.m_columns.size();
    ColumnRef* resolved = preparer.m_amalloc->alloc_exc<ColumnRef>(count);
    for (Uint32 i = 0; i < count; i++)
    {
      new (&resolved[i]) ColumnRef();
      const char* name = preparer.m_columns[i].c_str();
      const NdbDictionary::Column* col = table->getColumn(name);
      if (col == NULL)
        continue;
      resolved[i].kind = ColumnRef::Kind::StoredColumn;
      resolved[i].join_op_idx =
          (linked_column != NULL && strcmp(name, linked_column) == 0)
              ? 0
              : leaf_idx;
      resolved[i].attr_id = col->getColumnNo();
      resolved[i].dict_column = col;
    }
    preparer.m_main_scope.resolved_columns = resolved;
    preparer.m_main_scope.table = table;
  }

  // The index of column name in the statement, or the column count.
  static Uint32 column_index(RonSQLPreparer& preparer, const char* name)
  {
    for (Uint32 i = 0; i < preparer.m_columns.size(); i++)
    {
      if (strcmp(preparer.m_columns[i].c_str(), name) == 0)
        return i;
    }
    return preparer.m_columns.size();
  }

  static AggregationAPICompiler* compiler(RonSQLPreparer& preparer)
  {
    return preparer.m_main_scope.agg;
  }

  static void set_compiler(RonSQLPreparer& preparer,
                           AggregationAPICompiler* compiler)
  {
    preparer.m_main_scope.agg = compiler;
  }

  static ConditionalExpression* where(RonSQLPreparer& preparer)
  {
    return preparer.m_context.ast_root.where_expression;
  }

  static void program_aggregator(RonSQLPreparer& preparer,
                                 NdbAggregator* aggregator)
  {
    preparer.programAggregator(aggregator);
  }

  static void filter(RonSQLPreparer& preparer, NdbAggregator* aggregator,
                     ConditionalExpression* ce, Uint32 true_output,
                     Uint32 false_output, Uint32 leaf_idx)
  {
    preparer.generate_embedded_filter_condition(
        aggregator, preparer.m_main_scope, ce, true_output, false_output,
        leaf_idx);
  }
};

typedef AggregationAPICompiler::SVMInstrType SVM;
typedef AggregationAPICompiler::Expr Expr;
typedef NdbDictionary::Column C;

// NdbAggregator's PROGRAM_HEADER_SIZE: the header words before the GROUP
// BY columns and the instructions.
static const Uint32 kProgramHeaderWords = 8;

static int g_failures = 0;

// What the checks ran on, so that a test that checks nothing fails.
struct Seen
{
  Uint32 cases;
  Uint32 decimal_loads;
  Uint32 pair_ops;
  Uint32 nullable_pair_ops;
  Uint32 filters;
};
static Seen g_seen = {0, 0, 0, 0, 0};

#define CHECK(condition)                                               \
  do {                                                                 \
    if (!(condition)) {                                                \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);     \
      g_failures++;                                                    \
    }                                                                  \
  } while (0)

static void
fail(const std::string& program, const std::string& what)
{
  fprintf(stderr, "FAIL %s: %s\n", program.c_str(), what.c_str());
  g_failures++;
}

static std::string
num(Uint32 n)
{
  return std::to_string(n);
}

/*
 * A statement prepared for EXPLAIN without a cluster connection, as
 * ronsql_oom_test prepares one.
 */
struct Statement
{
  ArenaMalloc arena;
  std::string sql;
  std::ostringstream out;
  std::ostringstream err;
  RonSQLExecParams params;

  explicit Statement(const char* text)
    : arena(RonSQLExecParams::ARENA_MALLOC_PAGE_SIZE),
      sql(text)
  {
    sql.append(2, '\0');
    params.sql_buffer = &sql[0];
    params.sql_len = sql.size();
    params.amalloc = &arena;
    params.ndb = NULL;
    params.explain_mode = RonSQLExecParams::ExplainMode::FORCE;
    params.output_format = RonSQLExecParams::OutputFormat::TEXT;
    params.out_stream = &out;
    params.err_stream = &err;
  }
};

/*
 * Walk the embedded interpreter program words[0, len) and collect the
 * skips it writes to interpreter output 0, which every RonSQL emitter
 * writes with a LOAD_CONST16 directly followed by
 * WRITE_INTERPRETER_OUTPUT (the data nodes find them with embeddedSkips,
 * PushdownInterpreter.cpp).  False when its instructions do not take
 * exactly len words or a branch lands inside an instruction.
 */
static bool
walk_embedded(const std::string& name, const Uint32* words, Uint32 len,
              std::set<Uint32>& skips)
{
  // The walker reads the word after a branch with data for its length:
  // the padding keeps a block that is shorter than counted in bounds.
  std::vector<Uint32> emb(words, words + len);
  emb.resize(len + 4, 0);
  std::set<Uint32> starts;
  std::vector<Uint32> branches;
  Uint32 pc = 0;
  Uint32 prev = UINT32_MAX;
  while (pc < len)
  {
    starts.insert(pc);
    Interpreter::InstructionPreProcessing processing;
    const Uint32* next =
        Interpreter::getInstructionPreProcessingInfo(&emb[pc], processing);
    if (next == NULL)
    {
      fail(name, "embedded word " + num(pc) + " is no instruction");
      return false;
    }
    const Uint32 instr = emb[pc];
    if (processing == Interpreter::LABEL_ADDRESS_REPLACEMENT)
      branches.push_back(pc + ((instr >> 16) & 0x7FFF));
    if (Interpreter::getOpCode(instr) ==
            Interpreter::WRITE_INTERPRETER_OUTPUT &&
        (instr >> 16) == 0)
    {
      if (prev == UINT32_MAX ||
          Interpreter::getOpCode(emb[prev]) != Interpreter::LOAD_CONST16 ||
          Interpreter::getReg1(emb[prev]) != Interpreter::getReg1(instr))
      {
        fail(name, "embedded word " + num(pc) +
                   " writes output 0 from no LOAD_CONST16");
        return false;
      }
      skips.insert(emb[prev] >> 16);
    }
    prev = pc;
    pc = static_cast<Uint32>(next - emb.data());
  }
  if (pc != len)
  {
    fail(name, "the embedded instructions take " + num(pc) +
               " words, not the " + num(len) + " counted");
    return false;
  }
  bool ok = true;
  for (size_t k = 0; k < branches.size(); k++)
  {
    if (branches[k] >= len || starts.count(branches[k]) == 0)
    {
      fail(name, "an embedded branch lands on word " + num(branches[k]) +
                 ", which starts no instruction");
      ok = false;
    }
  }
  return ok;
}

// The NdbAggregator instructions programAggregator emits for SVM
// instruction i: a pair-op is an embedded program and a Mov, and a Skip
// and a SetRegNull when an operand may be NULL.
static Uint32
emitted_instructions(AggregationAPICompiler& compiler, Uint32 i)
{
  const SVM type = compiler.m_program[i].type;
  if (type == SVM::Greatest2 || type == SVM::Least2)
  {
    const bool nullable = i < compiler.m_pair_op_needs_null_check.size() &&
                          compiler.m_pair_op_needs_null_check[i];
    return nullable ? 4 : 2;
  }
  return 1;
}

/*
 * Check the program the single-table emitter wrote for compiler into
 * aggregator (finalized) against the compiler's word counts.
 */
static void
check_program(const std::string& name, AggregationAPICompiler& compiler,
              const NdbAggregator& aggregator)
{
  const Uint32 end = aggregator.instructions_length();
  const Uint32* prog = aggregator.buffer();
  const Uint32 n = compiler.m_program.size();
  // The first word of each SVM instruction's emitted instructions.
  std::vector<Uint32> svm_start(n + 1, 0);
  // For a CASE condition: the word after its embedded program, and the
  // skips that program writes.
  std::vector<Uint32> block_end(n, 0);
  std::vector<std::set<Uint32>> block_skips(n);
  std::set<Uint32> starts;
  std::vector<Uint32> targets;
  Uint32 pos = kProgramHeaderWords + aggregator.n_gb_cols();
  for (Uint32 i = 0; i < n; i++)
  {
    svm_start[i] = pos;
    const Uint32 count = emitted_instructions(compiler, i);
    Uint32 words = 0;
    for (Uint32 k = 0; k < count; k++)
    {
      if (pos >= end)
      {
        fail(name, "the program ends inside SVM instruction " + num(i));
        return;
      }
      const Uint32 word = prog[pos];
      const Uint32 len = aggInstructionWords(word);
      const Uint32 op = aggProgramOp(word);
      if (pos + len > end)
      {
        fail(name, "the instruction at word " + num(pos) +
                   " runs past the end of the program");
        return;
      }
      starts.insert(pos);
      if (op == kOpEmbeddedInterp)
      {
        std::set<Uint32> skips;
        if (!walk_embedded(name, &prog[pos + 1], len - 1, skips))
          return;
        for (std::set<Uint32>::const_iterator it = skips.begin();
             it != skips.end(); ++it)
          targets.push_back(pos + len + *it);
        block_end[i] = pos + len;
        block_skips[i] = skips;
      }
      else if (op == kOpSkip)
      {
        targets.push_back(pos + 1 + (word & 0xFFFF));
      }
      else if (op == kOpLoadCol && len == 2)
      {
        g_seen.decimal_loads++;
      }
      pos += len;
      words += len;
    }
    const SVM type = compiler.m_program[i].type;
    if (type == SVM::EmbeddedInterp)
      continue;  // Its length is the embedded program's, checked above.
    if (type == SVM::Greatest2 || type == SVM::Least2)
    {
      g_seen.pair_ops++;
      if (count == 4)
        g_seen.nullable_pair_ops++;
    }
    const Uint32 counted = compiler.raw_word_size(i, i + 1);
    if (counted != words)
    {
      fail(name, "SVM instruction " + num(i) + ": raw_word_size " +
                 num(counted) + ", emitted " + num(words));
    }
  }
  svm_start[n] = pos;
  if (pos != end)
  {
    fail(name, num(end - pos) + " words after the last instruction");
    return;
  }
  starts.insert(end);
  for (size_t k = 0; k < targets.size(); k++)
  {
    if (starts.count(targets[k]) == 0)
      fail(name, "a skip lands on word " + num(targets[k]) +
                 ", inside an instruction");
  }
  for (Uint32 i = 0; i < n; i++)
  {
    const AggregationAPICompiler::Instr& instr = compiler.m_program[i];
    if (instr.type != SVM::EmbeddedInterp)
      continue;
    if (instr.dest >= compiler.m_cases.size())
    {
      fail(name, "SVM instruction " + num(i) + " names no CASE");
      continue;
    }
    const AggregationAPICompiler::CaseInfo& ci = compiler.m_cases[instr.dest];
    if (ci.then_start > n || ci.skip_pos >= n || ci.else_start > n ||
        ci.else_end > n)
    {
      fail(name, "CASE " + num(instr.dest) + " has an arm outside the "
                 "program");
      continue;
    }
    g_seen.cases++;
    if (svm_start[ci.then_start] != block_end[i])
      fail(name, "the THEN arm of CASE " + num(instr.dest) +
                 " does not follow its condition");
    std::set<Uint32> expected;
    expected.insert(0);
    expected.insert(svm_start[ci.else_start] - block_end[i]);
    if (block_skips[i] != expected)
      fail(name, "the condition of CASE " + num(instr.dest) +
                 " does not skip 0 and " +
                 num(svm_start[ci.else_start] - block_end[i]) + " words");
    const Uint32 skip_word = prog[svm_start[ci.skip_pos]];
    if (aggProgramOp(skip_word) != kOpSkip ||
        svm_start[ci.skip_pos] + 1 + (skip_word & 0xFFFF) !=
            svm_start[ci.else_end])
      fail(name, "the Skip of CASE " + num(instr.dest) +
                 " does not land after its ELSE arm");
  }
}

static void
add_column(NdbDictionary::Table& table, const char* name, C::Type type,
           bool nullable)
{
  NdbDictionary::Column col(name);
  col.setType(type);
  if (type == C::Decimal)
  {
    col.setPrecision(10);
    col.setScale(2);
  }
  if (type == C::Varchar)
  {
    col.setLength(10);
    col.setCharset(&my_charset_latin1);
  }
  col.setNullable(nullable);
  CHECK(table.addColumn(col) == 0);
}

/*
 * CASE programs, compiled by the parser as for execution: a CASE
 * condition is an embedded program, the THEN arm ends with a Skip over
 * the ELSE arm, whose aggregate is a RepeatAgg.
 */
static const char* const case_queries[] = {
  // D-1, F-3: a DECIMAL load (two words) in the THEN arm, and in the
  // ELSE arm.
  "SELECT SUM(CASE WHEN a > 1 THEN d ELSE b END) FROM t;",
  "SELECT SUM(CASE WHEN a > 1 THEN b ELSE d END), COUNT(*) FROM t;",
  // A DOUBLE column against a constant (a 6-word atom), OR, integer
  // constants (3 words) and arithmetic in the arms.
  "SELECT MAX(CASE WHEN c >= 2.5 OR a = 3 THEN a + 2 ELSE a * b END) "
  "FROM t;",
  // Column against column (5 words) and a VARCHAR constant under AND;
  // AVG is a SUM and a COUNT of the CASE (F-1); a GROUP BY word.
  "SELECT a, AVG(CASE WHEN a <> b AND s = 'abc' THEN d ELSE 0 END) "
  "FROM t GROUP BY a;",
  // DATETIME constants (5 bytes, two data words) and a nullable column.
  // Aliased: an unaliased output may be at most 64 bytes long.
  "SELECT MIN(CASE WHEN dt >= '2026-01-01 00:00:00' THEN d ELSE d END) "
  "AS m1, "
  "SUM(CASE WHEN dt < '2026-01-01 00:00:00' AND n > 0 THEN b ELSE 1 END) "
  "AS s1 FROM t;",
};

static void
check_case_program(const NdbDictionary::Table& table, const char* sql)
{
  Statement statement(sql);
  try
  {
    RonSQLPreparer preparer(statement.params);
    RonSQLPreparerTestAccess::resolve_columns(preparer, &table, 0, NULL);
    AggregationAPICompiler* compiler =
        RonSQLPreparerTestAccess::compiler(preparer);
    if (compiler == NULL)
    {
      fail(sql, "no aggregation program");
      return;
    }
    NdbAggregator aggregator(&table);
    RonSQLPreparerTestAccess::program_aggregator(preparer, &aggregator);
    if (!aggregator.Finalize())
    {
      fail(sql, std::string("Finalize: ") + aggregator.GetError().err_msg_);
      return;
    }
    check_program(sql, *compiler, aggregator);
  }
  catch (const std::exception& e)
  {
    fail(sql, std::string(e.what()) + "\n" + statement.err.str());
  }
}

static const char*
no_column_name(uint)
{
  return "?";
}

/*
 * GREATEST and LEAST, which the parser compiles only with resolved
 * columns (validate_greatest_least_pair_loads), so the program is built
 * here: an embedded program of 9 words, or of 14 with a Skip and a
 * SetRegNull when an operand may be NULL.
 */
static void
check_pair_op_program(const NdbDictionary::Table& table, bool nested)
{
  const std::string name = nested
      ? "SUM(LEAST(a, LEAST(n, 5))), MAX(GREATEST(b, a))"
      : "SUM(GREATEST(a, b))";
  Statement statement("SELECT COUNT(a), COUNT(b), COUNT(n) FROM t;");
  try
  {
    RonSQLPreparer preparer(statement.params);
    RonSQLPreparerTestAccess::resolve_columns(preparer, &table, 0, NULL);
    const Uint32 a = RonSQLPreparerTestAccess::column_index(preparer, "a");
    const Uint32 b = RonSQLPreparerTestAccess::column_index(preparer, "b");
    const Uint32 n = RonSQLPreparerTestAccess::column_index(preparer, "n");
    const Uint32 count =
        RonSQLPreparerTestAccess::column_index(preparer, "no such column");
    if (a == count || b == count || n == count)
    {
      fail(name, "the statement lacks a column");
      return;
    }
    AggregationAPICompiler* compiler =
        new (statement.arena.alloc_exc<AggregationAPICompiler>(1))
            AggregationAPICompiler(no_column_name, statement.out,
                                   statement.err, &statement.arena);
    if (nested)
    {
      Expr* inner = compiler->Least2(compiler->Load(n),
                                     compiler->ConstantInteger(5));
      compiler->Sum(compiler->Least2(compiler->Load(a), inner));
      compiler->Max(compiler->Greatest2(compiler->Load(b),
                                        compiler->Load(a)));
    }
    else
    {
      compiler->Sum(compiler->Greatest2(compiler->Load(a),
                                        compiler->Load(b)));
    }
    if (!compiler->compile())
    {
      fail(name, "the compiler failed");
      return;
    }
    RonSQLPreparerTestAccess::set_compiler(preparer, compiler);
    NdbAggregator aggregator(&table);
    RonSQLPreparerTestAccess::program_aggregator(preparer, &aggregator);
    if (!aggregator.Finalize())
    {
      fail(name, std::string("Finalize: ") + aggregator.GetError().err_msg_);
      return;
    }
    check_program(name, *compiler, aggregator);
  }
  catch (const std::exception& e)
  {
    fail(name, std::string(e.what()) + "\n" + statement.err.str());
  }
}

/*
 * A cross-table WHERE filter (generate_embedded_filter_condition) on the
 * aggregated table, join operation 1, whose column b is its parent's
 * (operation 0): the embedded program's length is
 * embedded_filter_expr_word_count's sum over the atoms plus 6 words of
 * exits, and outputs 0 (row passes) and 1 (row rejected).  The program is
 * the filter and one COUNT.
 */
static void
check_filter(const NdbDictionary::Table& table, const char* sql,
             Uint32 expected_len)
{
  Statement statement(sql);
  try
  {
    RonSQLPreparer preparer(statement.params);
    RonSQLPreparerTestAccess::resolve_columns(preparer, &table, 1, "b");
    ConditionalExpression* where = RonSQLPreparerTestAccess::where(preparer);
    if (where == NULL)
    {
      fail(sql, "no WHERE");
      return;
    }
    NdbAggregator aggregator(&table);
    RonSQLPreparerTestAccess::filter(preparer, &aggregator, where, 0, 1, 1);
    if (!aggregator.Count(0, 0) || !aggregator.Finalize())
    {
      fail(sql, std::string("Count / Finalize: ") +
                aggregator.GetError().err_msg_);
      return;
    }
    const Uint32* prog = aggregator.buffer();
    const Uint32 end = aggregator.instructions_length();
    const Uint32 header = prog[kProgramHeaderWords];
    if (aggProgramOp(header) != kOpEmbeddedInterp)
    {
      fail(sql, "the program does not start with the filter");
      return;
    }
    const Uint32 len = header & 0xFFFF;
    if (len != expected_len)
      fail(sql, "the filter is counted as " + num(len) + " words, not " +
                num(expected_len));
    // The filter's words, then the COUNT.
    if (kProgramHeaderWords + 1 + len + 1 != end)
    {
      fail(sql, "the filter is counted as " + num(len) + " words and " +
                "emitted as " + num(end - kProgramHeaderWords - 2));
      return;
    }
    std::set<Uint32> skips;
    if (!walk_embedded(sql, &prog[kProgramHeaderWords + 1], len, skips))
      return;
    std::set<Uint32> expected;
    expected.insert(0);
    expected.insert(1);
    if (skips != expected)
      fail(sql, "the filter does not output 0 and 1");
    g_seen.filters++;
  }
  catch (const std::exception& e)
  {
    fail(sql, std::string(e.what()) + "\n" + statement.err.str());
  }
}

int
main()
{
  if (ndb_init() != 0)
  {
    printf("FAIL: ndb_init\n");
    return 1;
  }
  {
    NdbDictionary::Table table("t");
    add_column(table, "a", C::Int, false);
    add_column(table, "b", C::Int, false);
    add_column(table, "n", C::Int, true);
    add_column(table, "c", C::Double, false);
    add_column(table, "d", C::Decimal, false);
    add_column(table, "s", C::Varchar, false);
    add_column(table, "dt", C::Datetime2, false);

    for (size_t q = 0; q < sizeof(case_queries) / sizeof(case_queries[0]);
         q++)
      check_case_program(table, case_queries[q]);
    check_pair_op_program(table, false);
    check_pair_op_program(table, true);
    /*
     * a + 1 > b: 1 + 3 + 1 words, b 1, the branch 1: 7.  n * 2 < a: the
     * nullable n 2 (its NULL guard), 3 + 1, a 1, the branch 1: 8.  With
     * the exits: 21.
     */
    check_filter(table, "SELECT COUNT(*) FROM t WHERE a + 1 > b AND n * 2 < a;",
                 21);
    // a = b: 3 words.  n > 3: 2 + 3 + 1 = 6.  With the exits: 15.
    check_filter(table, "SELECT COUNT(*) FROM t WHERE a = b OR n > 3;", 15);
  }
  // The checks ran on what they are meant to check.
  CHECK(g_seen.cases >= 6);
  CHECK(g_seen.decimal_loads >= 4);
  CHECK(g_seen.pair_ops >= 4);
  CHECK(g_seen.nullable_pair_ops >= 2);
  CHECK(g_seen.filters == 2);
  int result = 0;
  if (g_failures != 0)
  {
    printf("FAIL: %d checks failed\n", g_failures);
    result = 1;
  }
  else
  {
    printf("OK\n");
  }
  ndb_end(0);
  return result;
}
