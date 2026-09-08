/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

/*
 * testJoinAggJit — JIT (RONDB-1056 compiled interpreter) canaries for
 *                  pushdown join aggregation, via the NdbQueryBuilder API.
 *
 * Split out of testJoinAggNdbApi (its former Tests 25-30) so the parent
 * binary carries no JIT dependency: it runs under CompiledInterpreter=OFF
 * (suite ndb_push_agg), while this binary REQUIRES the JIT and lives in
 * suite ndb_push_agg_jit (CompiledInterpreter=ON). Tests 1,2,3,5,6 arm
 * ERROR_INSERT 4060 (any JIT fallback aborts the data node), so a green
 * run proves the program actually compiled and executed natively.
 * Test 4 is the opposite: the clean unsupported-program reject path,
 * deliberately with NO error inserts (a 33-aggregate program, one past
 * the JIT's BC_MAX_ACCS accumulator capacity).
 *
 * Test map (former testJoinAggNdbApi number in parens):
 *   Test 1 (25): JIT must compile SUM of a local attribute
 *   Test 2 (26): all-rejected SUM preserves the NULL result
 *   Test 3 (27): linked-NULL filter path (READ_LINKED_TO_MEM +
 *                BRANCH_LINKED_NE_NULL), tables jit3_*
 *   Test 4 (28): over-capacity shape (33 aggregates) falls back cleanly
 *   Test 5 (29): SUM of a column with id > 255, tables jit5_*
 *   Test 6 (30): embedded CASE non-zero skip_offset, tables jit6_*
 *
 * Schema for Tests 1,2,4 (created via MySQL, same as testJoinAggNdbApi):
 *   jagg_parent(id INT PK, grp INT)
 *   jagg_child(parent_id INT PK, amount BIGINT)
 *
 * Usage: testJoinAggJit -c <connect_string> -m <mysql_port> [-v|--verbose]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include <NdbAggregator.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryOperation.hpp"

#include <NdbRestarter.hpp>
#include <mysql.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

/* Verbose output control */
static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *PARENT_TABLE = "jagg_parent";
static const char *CHILD_TABLE = "jagg_child";
static const char *TEST_DB = "test_db";

static const char *T3_PARENT = "jit3_parent";
static const char *T3_CHILD  = "jit3_child";
static const char *T5_PARENT = "jit5_parent";
static const char *T5_CHILD  = "jit5_child";
static const char *T6_PARENT = "jit6_parent";
static const char *T6_CHILD  = "jit6_child";
static const char *T7_ROOT   = "jit7_root";
static const char *T7_LEAF_A = "jit7_leaf_a";
static const char *T7_LEAF_B = "jit7_leaf_b";

/* ------------------------------------------------------------------ */
/* MySQL helpers                                                       */
/* ------------------------------------------------------------------ */

static int
sqlExec(MYSQL *conn, const char *query)
{
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL failed: %s\n  query: %s\n",
            mysql_error(conn), query);
    return -1;
  }
  return 0;
}

static MYSQL *
connectMysql(int mysqlPort)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) {
    fprintf(stderr, "mysql_init failed\n");
    return nullptr;
  }
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         nullptr, mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

/* ------------------------------------------------------------------ */
/* Table setup via MySQL                                               */
/* ------------------------------------------------------------------ */

static int
createTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_parent");

  if (sqlExec(conn,
        "CREATE TABLE jagg_parent ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", PARENT_TABLE);

  if (sqlExec(conn,
        "CREATE TABLE jagg_child ("
        "  parent_id INT NOT NULL PRIMARY KEY,"
        "  amount BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", CHILD_TABLE);

  return 0;
}

static int
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_parent");
  V("Dropped test tables\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Data insertion via MySQL                                            */
/* ------------------------------------------------------------------ */

static int
insertTestData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO jagg_parent VALUES "
        "(1,1),(2,1),(3,2),(4,2),(5,3)") != 0) return -1;
  V("Inserted 5 parent rows\n");

  if (sqlExec(conn,
        "INSERT INTO jagg_child VALUES "
        "(1,100),(2,200),(3,300),(4,400),(5,500)") != 0) return -1;
  V("Inserted 5 child rows\n");

  return 0;
}

/* Scalar verification (no GROUP BY): query returns 1 row with N columns */
static int
verifyScalarWithMysql(MYSQL *conn, const char *testName, const char *query,
                      const std::vector<Int64> &expected)
{
  V("  MySQL verify: %s\n", query);
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "MySQL verify failed: %s\n  query: %s\n",
            mysql_error(conn), query);
    return -1;
  }

  MYSQL_RES *result = mysql_store_result(conn);
  if (result == nullptr) {
    fprintf(stderr, "mysql_store_result failed: %s\n", mysql_error(conn));
    return -1;
  }

  MYSQL_ROW row = mysql_fetch_row(result);
  if (row == nullptr) {
    fprintf(stderr, "  %s: MySQL returned no rows\n", testName);
    mysql_free_result(result);
    return -1;
  }

  unsigned int numFields = mysql_num_fields(result);
  if (numFields != expected.size()) {
    fprintf(stderr, "  %s: MySQL returned %u columns, expected %zu\n",
            testName, numFields, expected.size());
    mysql_free_result(result);
    return -1;
  }

  for (unsigned int i = 0; i < numFields; i++) {
    Int64 actual = atoll(row[i]);
    if (actual != expected[i]) {
      fprintf(stderr, "  %s: MySQL column %u: expected %lld, got %lld\n",
              testName, i, (long long)expected[i], (long long)actual);
      mysql_free_result(result);
      return -1;
    }
  }
  mysql_free_result(result);
  V("  MySQL verification OK\n");
  return 0;
}

static Uint32
encEmbeddedOp(Uint32 op, Uint32 lower)
{
  return (op & 0x3Fu) | (((op >> 6) & 0x1u) << 15) | lower;
}

static Uint32
encEmbeddedBranchAttrNull(Uint32 op, Uint32 branchLength)
{
  return encEmbeddedOp(op, (branchLength & 0x7FFFu) << 16);
}

static Uint32
encEmbeddedAttrId(Uint32 attrId)
{
  return (attrId & 0xFFFFu) << 16;
}

/* Embedded normal-interpreter opcodes for the linked-NULL path (mirror
 * of Interpreter.hpp; see bridge_tests.c enc_emb_* helpers). */
#define EMB_OP_EXIT_OK              18
#define EMB_OP_EXIT_REFUSE          19
#define EMB_OP_LOAD_CONST16          4
#define EMB_OP_READ_LINKED_TO_MEM   39
#define EMB_OP_BRANCH_LINKED_EQ_NULL 41
#define EMB_OP_BRANCH_LINKED_NE_NULL 42

/* EXIT_REFUSE carries a client error code in bits 31..16. Codes 626,
 * 899, and 6000-6999 mean "filter this row out" (NdbInterpretedCode
 * convention); other codes abort the aggregation. */
static Uint32
encEmbeddedExitRefuse(Uint32 errorCode)
{
  return encEmbeddedOp(EMB_OP_EXIT_REFUSE, (errorCode & 0xFFFFu) << 16);
}

/* LOAD_CONST16 reg, val — load a 16-bit constant into an interpreter
 * register (val in bits 31..16, reg in bits 8..6). Used to stage the
 * accept-path skip_offset for WRITE_INTERPRETER_OUTPUT. */
static Uint32
encEmbeddedLoadConst16(Uint32 reg, Uint32 val)
{
  return EMB_OP_LOAD_CONST16 | ((reg & 0x7u) << 6) | ((val & 0xFFFFu) << 16);
}

/* WRITE_INTERPRETER_OUTPUT reg, outIdx — write a register to interpreter
 * output slot outIdx. Mirrors Interpreter::WriteInterpreterOutput:
 * opcode = LOAD_CONST_MEM(59) | (1<<15) (decodes to 123), reg in
 * bits 8..6, outIdx in bits 31..16. Slot 0 is the row-disposition
 * skip_offset that selects which aggregation instruction runs next. */
static Uint32
encEmbeddedWriteOutput(Uint32 reg, Uint32 outIdx)
{
  return 59u | (1u << 15) | ((reg & 0x7u) << 6) | ((outIdx & 0xFFFFu) << 16);
}

/* READ_LINKED_TO_MEM — 1-word instruction; position in bits 23..16. */
static Uint32
encEmbeddedReadLinkedToMem(Uint32 position)
{
  return encEmbeddedOp(EMB_OP_READ_LINKED_TO_MEM, (position & 0xFFu) << 16);
}

/* BRANCH_LINKED_EQ_NULL / NE_NULL — 1-word instruction; no operand word
 * (position is implicit from the preceding READ_LINKED_TO_MEM). Forward
 * branch with branchLength in bits 30..16. */
static Uint32
encEmbeddedBranchLinkedNull(Uint32 op, Uint32 branchLength)
{
  return encEmbeddedOp(op, (branchLength & 0x7FFFu) << 16);
}

static int
testJitMustCompileSum(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter,
                      Int64 expectedSum)
{
  const char *testName = "Test 1: JIT must compile SUM local attr";
  printf("%s ... ", testName);
  fflush(stdout);

  if (verifyScalarWithMysql(conn, testName,
        "SELECT SUM(amount) FROM jagg_parent "
        "JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id",
        {expectedSum}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(PARENT_TABLE);
  dict->invalidateTable(CHILD_TABLE);
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4060) != 0) {
    printf("FAILED (insertErrorInAllNodes(4060))\n");
    return -1;
  }
  bool mustCompileSet = true;
  auto clearMustCompile = [&]() {
    if (mustCompileSet) {
      restarter.insertErrorInAllNodes(0);
      mustCompileSet = false;
      V("  ERROR_INSERT cleared\n");
    }
  };
  V("\n  ERROR_INSERT 4060 set (JIT fallback is fatal)\n");

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setMatchType(NdbQueryOptions::MatchNonNull);
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    clearMustCompile();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    clearMustCompile();
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  clearMustCompile();

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 actualSum = sumRes.data_int64();

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  printf("OK (sum=%lld, JIT required)\n", (long long)expectedSum);
  return 0;
}

static int
testJitAllRejectedSumNull(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter)
{
  printf("Test 2: JIT all-rejected SUM returns NULL ... ");
  fflush(stdout);

  if (mysql_query(conn,
        "SELECT SUM(jagg_child.amount) FROM jagg_parent "
        "JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id "
        "WHERE jagg_child.amount IS NULL") != 0) {
    printf("FAILED (MySQL verification: %s)\n", mysql_error(conn));
    return -1;
  }
  {
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row = result == nullptr ? nullptr : mysql_fetch_row(result);
    const bool mysqlNull = (row != nullptr && row[0] == nullptr);
    if (result != nullptr) mysql_free_result(result);
    if (!mysqlNull) {
      printf("FAILED (MySQL verification: expected NULL)\n");
      return -1;
    }
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(PARENT_TABLE);
  dict->invalidateTable(CHILD_TABLE);
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  const NdbDictionary::Column *amountCol = childTab->getColumn("amount");
  if (amountCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  const Uint32 amountAttrId = amountCol->getColumnNo();
  NdbAggregator agg(childTab);
  if (!agg.EmbeddedInterp(3) ||
      !agg.EmitEmbeddedWord(encEmbeddedBranchAttrNull(25, 2)) ||
      !agg.EmitEmbeddedWord(encEmbeddedAttrId(amountAttrId)) ||
      !agg.EmitEmbeddedWord(encEmbeddedOp(EMB_OP_EXIT_REFUSE, 0)) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4060) != 0) {
    printf("FAILED (insertErrorInAllNodes(4060))\n");
    return -1;
  }
  bool mustCompileSet = true;
  auto clearMustCompile = [&]() {
    if (mustCompileSet) {
      restarter.insertErrorInAllNodes(0);
      mustCompileSet = false;
      V("  ERROR_INSERT cleared\n");
    }
  };

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setMatchType(NdbQueryOptions::MatchNonNull);
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    clearMustCompile();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    clearMustCompile();
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  clearMustCompile();

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  const bool isNull = sumRes.is_null();
  const Int64 actualSum = isNull ? 0 : sumRes.data_int64();

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (!isNull) {
    printf("FAILED (expected SUM NULL, got %lld)\n",
           (long long)actualSum);
    return -1;
  }

  printf("OK (sum=NULL, JIT required)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3 helpers: dedicated tables with a nullable parent column      */
/* ------------------------------------------------------------------ */

static int
createT3Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit3_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jit3_parent");

  if (sqlExec(conn,
        "CREATE TABLE jit3_parent ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  marker BIGINT NULL"
        ") ENGINE=NDB") != 0) return -1;
  if (sqlExec(conn,
        "CREATE TABLE jit3_child ("
        "  parent_id INT NOT NULL PRIMARY KEY,"
        "  amount BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  return 0;
}

static int
insertT3Data(MYSQL *conn)
{
  /* marker NULL for parents 2,4; non-NULL for 1,3,5. */
  if (sqlExec(conn,
        "INSERT INTO jit3_parent VALUES "
        "(1,10),(2,NULL),(3,30),(4,NULL),(5,50)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO jit3_child VALUES "
        "(1,100),(2,200),(3,300),(4,400),(5,500)") != 0) return -1;
  return 0;
}

static int
dropT3Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit3_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jit3_parent");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: JIT canary for the Phase 5.1a linked-NULL filter path.      */
/* parent scan projects a nullable linked column (marker); the child    */
/* leaf aggregation reads it via READ_LINKED_TO_MEM and rejects rows    */
/* where marker IS NULL via BRANCH_LINKED_EQ_NULL, then SUM(amount).    */
/* Non-NULL markers are parents 1,3,5 -> SUM = 100+300+500 = 900.       */
/* ERROR_INSERT 4060 forces JIT (fatal on fallback), so a green run     */
/* proves the linked-NULL program compiled and executed through JIT.    */
/* ------------------------------------------------------------------ */
static int
testJitLinkedNullSum(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter)
{
  const char *testName = "Test 3: JIT linked NULL filter";
  printf("%s ... ", testName);
  fflush(stdout);

  if (verifyScalarWithMysql(conn, testName,
        "SELECT SUM(c.amount) FROM jit3_parent p "
        "JOIN jit3_child c ON c.parent_id = p.id "
        "WHERE p.marker IS NOT NULL",
        {900}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T3_PARENT);
  dict->invalidateTable(T3_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(T3_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(T3_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Embedded block (emb_len=6) following the full row-disposition model:
   * a per-row decision that explicitly skips (EXIT_REFUSE) or uses
   * (WRITE_INTERPRETER_OUTPUT skip_offset 0 + EXIT_OK) the row.
   *   0: READ_LINKED_TO_MEM position 0   (loads projected marker)
   *   1: BRANCH_LINKED_NE_NULL +2        (marker NOT NULL -> accept @3)
   *   2: EXIT_REFUSE 626                 (marker IS NULL -> skip row)
   *   3: LOAD_CONST16 r2, 0              (accept: skip_offset = 0)
   *   4: WRITE_INTERPRETER_OUTPUT r2, 0  (slot 0 = run next agg instr)
   *   5: EXIT_OK                         (accept -> outer LoadColumn+Sum)
   * Non-NULL markers sum amount (100+300+500=900); NULL markers skip.
   * This bytecode runs identically on the interpreter and the JIT. */
  NdbAggregator agg(childTab);
  if (!agg.EmbeddedInterp(6) ||
      !agg.EmitEmbeddedWord(encEmbeddedReadLinkedToMem(0)) ||
      !agg.EmitEmbeddedWord(
          encEmbeddedBranchLinkedNull(EMB_OP_BRANCH_LINKED_NE_NULL, 2)) ||
      !agg.EmitEmbeddedWord(encEmbeddedExitRefuse(626)) ||
      !agg.EmitEmbeddedWord(encEmbeddedLoadConst16(2, 0)) ||
      !agg.EmitEmbeddedWord(encEmbeddedWriteOutput(2, 0)) ||
      !agg.EmitEmbeddedWord(encEmbeddedOp(EMB_OP_EXIT_OK, 0)) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4060) != 0) {
    printf("FAILED (insertErrorInAllNodes(4060))\n");
    return -1;
  }
  bool mustCompileSet = true;
  auto clearMustCompile = [&]() {
    if (mustCompileSet) {
      restarter.insertErrorInAllNodes(0);
      mustCompileSet = false;
      V("  ERROR_INSERT cleared\n");
    }
  };
  V("\n  ERROR_INSERT 4060 set (JIT fallback is fatal)\n");

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setMatchType(NdbQueryOptions::MatchNonNull);

  /* Project parent.marker down to the child leaf at linked position 0,
   * matching READ_LINKED_TO_MEM position 0 above. */
  const NdbLinkedOperand *markerLink = qb->linkedValue(parentOp, "marker");
  if (markerLink == nullptr) {
    printf("FAILED (linkedValue marker: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  opts.addLinkedProjection(markerLink);
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    clearMustCompile();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    clearMustCompile();
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  clearMustCompile();

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  const bool isNull = sumRes.is_null();
  const Int64 actualSum = isNull ? 0 : sumRes.data_int64();

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (isNull || actualSum != 900) {
    printf("FAILED (expected SUM 900, got %s)\n",
           isNull ? "NULL" : std::to_string((long long)actualSum).c_str());
    return -1;
  }

  printf("OK (sum=900, JIT required)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: Unsupported-program fallback canary                        */
/*                                                                     */
/* Purpose: lock in clean interpreter fallback for a program that is a */
/* JIT candidate by shape (child leaf aggregation, no GROUP BY) but    */
/* contains an opcode the bridge does not lower.                       */
/*                                                                     */
/* The program is rejected at JOIN_AGG_SETUP_REQ, m_jit_entry stays    */
/* nullptr, and JoinAggInterpreter::ProcessRec runs the normal         */
/* interpreter loop. This test proves that reject path produces the    */
/* correct result and never errors the query.                          */
/*                                                                     */
/* The shape: 33 SUM(0) aggregates (agg indices 0..32). Index 32 is    */
/* one past the JIT's accumulator capacity (BC_MAX_ACCS = 32), so the  */
/* bridge rejects with REG_OUT_OF_RANGE while the interpreter          */
/* (MAX_AGG_N_RESULTS = 256) computes all 33 results fine. Durable     */
/* until BC_MAX_ACCS is raised to >= 33 — then add more aggregates.    */
/* Historically MAX(amount) (until 5B), SUM(amount % amount) (until    */
/* 5E-2 lowered kOpMod), then a dead kOpSetRegNull (until ronsql_jit   */
/* slice 2 lowered it to the per-row fallback for GREATEST/LEAST).     */
/* Deliberately runs with NO error inserts: 4060 (fallback fatal) and  */
/* 5120 (setup-compile fatal) would both abort precisely the path we   */
/* want to exercise. A developer can confirm the reject reason         */
/* manually with a one-off 5120 run; that fatal variant is             */
/* intentionally kept out of MTR. Because it needs no error inserts,   */
/* this test also runs for real in production builds.                  */
/* ------------------------------------------------------------------ */
static int
testJitUnsupportedFallback(Ndb *ndb, MYSQL *conn, Int64 expectedSum)
{
  const char *testName = "Test 4: Unsupported JIT shape falls back cleanly";
  printf("%s ... ", testName);
  fflush(stdout);

  if (verifyScalarWithMysql(conn, testName,
        "SELECT SUM(0) FROM jagg_parent "
        "JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id",
        {expectedSum}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(PARENT_TABLE);
  dict->invalidateTable(CHILD_TABLE);
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* 33 SUM(0) aggregates: agg index 32 is one past the JIT's
   * accumulator capacity (BC_MAX_ACCS = 32), so the bridge rejects the
   * program at setup and the query runs on the interpreter (whose
   * MAX_AGG_N_RESULTS = 256 handles all 33). */
  const Uint32 T4_N_AGGS = 33;
  NdbAggregator agg(childTab);
  bool progOk = agg.LoadInt64(0, 0);
  for (Uint32 i = 0; progOk && i < T4_N_AGGS; i++) {
    progOk = agg.Sum(i, 0);
  }
  if (!progOk || !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setMatchType(NdbQueryOptions::MatchNonNull);
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Int64 actualSum = 0;
  for (Uint32 i = 0; i < T4_N_AGGS; i++) {
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    if (sumRes.end() || sumRes.is_null() ||
        sumRes.data_int64() != expectedSum) {
      printf("FAILED (agg %u: expected %lld, got %s)\n", (unsigned)i,
             (long long)expectedSum,
             sumRes.end() ? "end-of-record" :
             sumRes.is_null() ? "NULL" :
             std::to_string((long long)sumRes.data_int64()).c_str());
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    actualSum = sumRes.data_int64();
  }

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  printf("OK (sum=%lld via interpreter fallback)\n", (long long)actualSum);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: operand-width boundary — JIT must compile SUM of a column  */
/* whose id is past the old 255 cap (RONDB-1056).                      */
/*                                                                     */
/* Builds a child table wide enough that the aggregated column sits at */
/* a column id > 255 (NDB allows up to MAX_ATTRIBUTES_IN_TABLE=4096).  */
/* With ERROR_INSERT 4060 (fallback fatal), the test passes only if    */
/* the high-id LoadCol actually compiled through JIT — proving the     */
/* bridge admits col_id up to 4095 and the engine encodes the full     */
/* 16-bit value. Before the widening fix the bridge rejected col_id    */
/* 260, fell back, and 4060 would have made the run fatal. The bridge  */
/* full-range boundary (255/256/4095 accept, 4096 reject) is covered   */
/* at the unit level in bridge_tests.c; this is the end-to-end proof.  */
/* ------------------------------------------------------------------ */
static const int   T5_NUM_COLS = 260;  /* c1..c260; c260 -> column id 260 */

static int
createT5Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit5_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jit5_parent");

  if (sqlExec(conn,
        "CREATE TABLE jit5_parent ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  /* Wide child: parent_id PK + c1..c260, all BIGINT NOT NULL, declared
   * in order so c260 gets column id 260. */
  std::string create = "CREATE TABLE jit5_child ("
                       "parent_id INT NOT NULL PRIMARY KEY";
  for (int i = 1; i <= T5_NUM_COLS; i++) {
    create += ", c";
    create += std::to_string(i);
    create += " BIGINT NOT NULL";
  }
  create += ") ENGINE=NDB";
  if (sqlExec(conn, create.c_str()) != 0) return -1;
  V("Created Test 5 tables (%d child columns)\n", T5_NUM_COLS);
  return 0;
}

static int
insertT5Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO jit5_parent VALUES "
        "(1,1),(2,1),(3,2),(4,2),(5,3)") != 0) return -1;

  /* 5 child rows. Only the last column (c260) carries the aggregated
   * values 100..500 (sum=1500); c1..c259 are 0 padding whose only job
   * is to push c260's column id past 255. */
  static const long long lastColVals[5] = {100, 200, 300, 400, 500};
  std::string ins = "INSERT INTO jit5_child VALUES ";
  for (int r = 0; r < 5; r++) {
    if (r != 0) ins += ",";
    ins += "(";
    ins += std::to_string(r + 1);            /* parent_id */
    for (int i = 1; i <= T5_NUM_COLS; i++) {
      ins += ",";
      ins += (i == T5_NUM_COLS) ? std::to_string(lastColVals[r]) : "0";
    }
    ins += ")";
  }
  if (sqlExec(conn, ins.c_str()) != 0) return -1;
  V("Inserted 5 Test 5 child rows\n");
  return 0;
}

static int
dropT5Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit5_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jit5_parent");
  V("Dropped Test 5 tables\n");
  return 0;
}

static int
testJitWideColumn(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter,
                  Int64 expectedSum)
{
  const char *testName = "Test 5: JIT compiles SUM of column id > 255";
  printf("%s ... ", testName);
  fflush(stdout);

  if (verifyScalarWithMysql(conn, testName,
        "SELECT SUM(c260) FROM jit5_parent "
        "JOIN jit5_child ON jit5_child.parent_id = jit5_parent.id",
        {expectedSum}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T5_PARENT);
  dict->invalidateTable(T5_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(T5_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(T5_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Self-check: confirm the aggregated column really sits past the old
   * 255 cap, so this test cannot silently stop exercising the widening. */
  const NdbDictionary::Column *targetCol = childTab->getColumn("c260");
  if (targetCol == nullptr) {
    printf("FAILED (column c260 lookup)\n");
    return -1;
  }
  int targetColNo = targetCol->getColumnNo();
  if (targetColNo <= 255) {
    printf("FAILED (c260 column id %d, expected > 255)\n", targetColNo);
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.LoadColumn("c260", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4060) != 0) {
    printf("FAILED (insertErrorInAllNodes(4060))\n");
    return -1;
  }
  bool mustCompileSet = true;
  auto clearMustCompile = [&]() {
    if (mustCompileSet) {
      restarter.insertErrorInAllNodes(0);
      mustCompileSet = false;
      V("  ERROR_INSERT cleared\n");
    }
  };
  V("\n  ERROR_INSERT 4060 set (JIT fallback is fatal); c260 col_id=%d\n",
    targetColNo);

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setMatchType(NdbQueryOptions::MatchNonNull);
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    clearMustCompile();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    clearMustCompile();
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  clearMustCompile();

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 actualSum = sumRes.data_int64();

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  printf("OK (sum=%lld, col_id=%d, JIT required)\n",
         (long long)expectedSum, targetColNo);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: JIT CASE non-zero skip_offset canary                       */
/*                                                                     */
/* The bridge lowers WRITE_INTERPRETER_OUTPUT slot 0 with a non-zero   */
/* skip_offset to OP_JUMP. This test proves the full NDB API path      */
/* compiles and executes that jump: accepted rows jump over a dummy    */
/* SUM and land on the real SUM. ERROR_INSERT 4060 makes fallback      */
/* fatal, so a green run proves the CASE skip path was JIT-compiled.   */
/* ------------------------------------------------------------------ */

static int
createT6Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit6_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jit6_parent");

  if (sqlExec(conn,
        "CREATE TABLE jit6_parent ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  marker BIGINT NULL"
        ") ENGINE=NDB") != 0) return -1;
  if (sqlExec(conn,
        "CREATE TABLE jit6_child ("
        "  parent_id INT NOT NULL PRIMARY KEY,"
        "  dummy BIGINT NOT NULL,"
        "  amount BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  return 0;
}

static int
insertT6Data(MYSQL *conn)
{
  /* marker NULL for parents 2,4; non-NULL for 1,3,5. */
  if (sqlExec(conn,
        "INSERT INTO jit6_parent VALUES "
        "(1,10),(2,NULL),(3,30),(4,NULL),(5,50)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO jit6_child VALUES "
        "(1,10000,100),(2,20000,200),(3,30000,300),"
        "(4,40000,400),(5,50000,500)") != 0) return -1;
  return 0;
}

static int
dropT6Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit6_child");
  sqlExec(conn, "DROP TABLE IF EXISTS jit6_parent");
  return 0;
}

static int
testJitCaseSkipOffset(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter)
{
  const char *testName = "Test 6: JIT CASE skip offset";
  printf("%s ... ", testName);
  fflush(stdout);

  if (verifyScalarWithMysql(conn, testName,
        "SELECT SUM(c.amount) FROM jit6_parent p "
        "JOIN jit6_child c ON c.parent_id = p.id "
        "WHERE p.marker IS NOT NULL",
        {900}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T6_PARENT);
  dict->invalidateTable(T6_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(T6_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(T6_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Embedded block (emb_len=6):
   *   0: READ_LINKED_TO_MEM position 0   (parent.marker)
   *   1: BRANCH_LINKED_NE_NULL +2        (non-NULL -> accept @3)
   *   2: EXIT_REFUSE 626                 (NULL -> skip row)
   *   3: LOAD_CONST16 r2, 2              (skip dummy LoadCol+SUM)
   *   4: WRITE_INTERPRETER_OUTPUT r2, 0  (slot 0 = CASE skip_offset)
   *   5: EXIT_OK
   * Outer program:
   *   LoadColumn(dummy);  Sum(agg0,dummy);
   *   LoadColumn(amount); Sum(agg1,amount);
   *
   * Correct JIT behavior leaves agg0 NULL and sets agg1=900. If the
   * non-zero skip_offset is ignored, agg0 would be updated with the large
   * dummy values instead. */
  NdbAggregator agg(childTab);
  if (!agg.EmbeddedInterp(6) ||
      !agg.EmitEmbeddedWord(encEmbeddedReadLinkedToMem(0)) ||
      !agg.EmitEmbeddedWord(
          encEmbeddedBranchLinkedNull(EMB_OP_BRANCH_LINKED_NE_NULL, 2)) ||
      !agg.EmitEmbeddedWord(encEmbeddedExitRefuse(626)) ||
      !agg.EmitEmbeddedWord(encEmbeddedLoadConst16(2, 2)) ||
      !agg.EmitEmbeddedWord(encEmbeddedWriteOutput(2, 0)) ||
      !agg.EmitEmbeddedWord(encEmbeddedOp(EMB_OP_EXIT_OK, 0)) ||
      !agg.LoadColumn("dummy", 0) ||
      !agg.Sum(0, 0) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(1, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4060) != 0) {
    printf("FAILED (insertErrorInAllNodes(4060))\n");
    return -1;
  }
  bool mustCompileSet = true;
  auto clearMustCompile = [&]() {
    if (mustCompileSet) {
      restarter.insertErrorInAllNodes(0);
      mustCompileSet = false;
      V("  ERROR_INSERT cleared\n");
    }
  };
  V("\n  ERROR_INSERT 4060 set (JIT fallback is fatal)\n");

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setMatchType(NdbQueryOptions::MatchNonNull);

  const NdbLinkedOperand *markerLink = qb->linkedValue(parentOp, "marker");
  if (markerLink == nullptr) {
    printf("FAILED (linkedValue marker: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  opts.addLinkedProjection(markerLink);
  opts.setAggregation(agg);

  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    clearMustCompile();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    clearMustCompile();
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  clearMustCompile();

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result dummyRes = rec.FetchAggregationResult();
  NdbAggregator::Result amountRes = rec.FetchAggregationResult();
  const bool dummyNull = dummyRes.is_null();
  const bool amountNull = amountRes.is_null();
  const Int64 dummySum = dummyNull ? 0 : dummyRes.data_int64();
  const Int64 amountSum = amountNull ? 0 : amountRes.data_int64();

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (!dummyNull) {
    printf("FAILED (dummy SUM expected NULL, got %lld)\n",
           (long long)dummySum);
    return -1;
  }
  if (amountNull || amountSum != 900) {
    printf("FAILED (amount SUM expected 900, got %s)\n",
           amountNull ? "NULL" : std::to_string((long long)amountSum).c_str());
    return -1;
  }

  printf("OK (dummy=NULL, amount=900, JIT required)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 7: JIT star 2-leaf SUM (Phase 6-3 multi-leaf)                  */
/*                                                                     */
/* Two aggregation leaves on one root scan — a star. Before 6-3 the    */
/* compile gate (m_num_leaves == 1) skipped multi-leaf setups          */
/* entirely and 4060 could never be armed near a star shape. Now      */
/* every leaf compiles independently and the per-row leaf switch      */
/* installs the current leaf's entry + accumulator count; leaf B's    */
/* accumulator sits at m_acc_offset 1, so a green run under 4060      */
/* proves BOTH leaves executed native code against the correct        */
/* accumulator slices (running leaf 0's code for leaf B rows, or the  */
/* combined count, would corrupt/overrun the slice and fail the sum   */
/* checks).                                                            */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT SUM(a.val_a), SUM(b.val_b)                                 */
/*   FROM jit7_root r                                                  */
/*   JOIN jit7_leaf_a a ON a.root_id = r.id                            */
/*   JOIN jit7_leaf_b b ON b.root_id = r.id                            */
/* ------------------------------------------------------------------ */

static int
createT7Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit7_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS jit7_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS jit7_root");

  if (sqlExec(conn,
        "CREATE TABLE jit7_root ("
        "  id INT NOT NULL PRIMARY KEY"
        ") ENGINE=NDB") != 0) return -1;
  if (sqlExec(conn,
        "CREATE TABLE jit7_leaf_a ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_a BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  if (sqlExec(conn,
        "CREATE TABLE jit7_leaf_b ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  return 0;
}

static int
insertT7Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO jit7_root VALUES (1),(2),(3),(4),(5)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO jit7_leaf_a VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO jit7_leaf_b VALUES "
        "(1,1),(2,2),(3,3),(4,4),(5,5)") != 0) return -1;
  return 0;
}

static int
dropT7Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jit7_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS jit7_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS jit7_root");
  return 0;
}

static int
testJitStarTwoLeafSum(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter,
                      Int64 expectedSumA, Int64 expectedSumB)
{
  const char *testName = "Test 7: JIT star 2-leaf SUM";
  printf("%s ... ", testName);
  fflush(stdout);

  if (verifyScalarWithMysql(conn, testName,
        "SELECT SUM(a.val_a), SUM(b.val_b) "
        "FROM jit7_root r "
        "JOIN jit7_leaf_a a ON a.root_id = r.id "
        "JOIN jit7_leaf_b b ON b.root_id = r.id",
        {expectedSumA, expectedSumB}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T7_ROOT);
  dict->invalidateTable(T7_LEAF_A);
  dict->invalidateTable(T7_LEAF_B);
  const NdbDictionary::Table *rootTab = dict->getTable(T7_ROOT);
  const NdbDictionary::Table *leafATab = dict->getTable(T7_LEAF_A);
  const NdbDictionary::Table *leafBTab = dict->getTable(T7_LEAF_B);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* One aggregation program per leaf; leaf B's accumulator lands at
   * m_acc_offset 1 in the combined layout. */
  NdbAggregator aggA(leafATab);
  if (!aggA.LoadColumn("val_a", 0) ||
      !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA program: %s)\n", aggA.GetError().err_msg_);
    return -1;
  }
  NdbAggregator aggB(leafBTab);
  if (!aggB.LoadColumn("val_b", 0) ||
      !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB program: %s)\n", aggB.GetError().err_msg_);
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4060) != 0) {
    printf("FAILED (insertErrorInAllNodes(4060))\n");
    return -1;
  }
  bool mustCompileSet = true;
  auto clearMustCompile = [&]() {
    if (mustCompileSet) {
      restarter.insertErrorInAllNodes(0);
      mustCompileSet = false;
      V("  ERROR_INSERT cleared\n");
    }
  };
  V("\n  ERROR_INSERT 4060 set (JIT fallback is fatal, both leaves)\n");

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *joinKeyA[] = {
    qb->linkedValue(rootOp, "id"),
    nullptr
  };
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  const NdbQueryLookupOperationDef *leafAOp =
      qb->readTuple(leafATab, joinKeyA, &optsA);
  if (leafAOp == nullptr) {
    printf("FAILED (readTuple leaf_a: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *joinKeyB[] = {
    qb->linkedValue(rootOp, "id"),
    nullptr
  };
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  const NdbQueryLookupOperationDef *leafBOp =
      qb->readTuple(leafBTab, joinKeyB, &optsB);
  if (leafBOp == nullptr) {
    printf("FAILED (readTuple leaf_b: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    clearMustCompile();
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    clearMustCompile();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    clearMustCompile();
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  clearMustCompile();

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result sumARes = rec.FetchAggregationResult();
  Int64 actualSumA = sumARes.data_int64();
  NdbAggregator::Result sumBRes = rec.FetchAggregationResult();
  Int64 actualSumB = sumBRes.data_int64();

  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected single record for non-GROUP-BY, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualSumA != expectedSumA || actualSumB != expectedSumB) {
    printf("FAILED (expected (%lld, %lld), got (%lld, %lld))\n",
           (long long)expectedSumA, (long long)expectedSumB,
           (long long)actualSumA, (long long)actualSumB);
    return -1;
  }

  printf("OK (sum_a=%lld, sum_b=%lld, 2 leaves, JIT required)\n",
         (long long)actualSumA, (long long)actualSumB);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static int onlyTest = 0;   /* 0 = run all, N = run only test N */
static int skipTest = 0;   /* 0 = skip none, N = skip test N */

#if !defined(VM_TRACE) && !defined(ERROR_INSERT)
/* Tests that require ERROR_INSERT support in the data node.
 * In production builds (no VM_TRACE / ERROR_INSERT), insertErrorInAllNodes
 * is a no-op, so these tests cannot exercise the code path they target.
 * We print the same OK line a successful debug run would produce so MTR
 * .result files stay authoritative — a real FAIL in debug still diffs.
 * Note: this is a FAKE OK. The test body does not run in production.
 * (Test 4 is absent on purpose: it uses no error inserts and runs for
 * real everywhere.)
 */
static const char *
fakeOkLineForErrorInsertTest(int testNum)
{
  switch (testNum) {
    case 1: return "Test 1: JIT must compile SUM local attr ... OK (sum=1500, JIT required)";
    case 2: return "Test 2: JIT all-rejected SUM returns NULL ... OK (sum=NULL, JIT required)";
    case 3: return "Test 3: JIT linked NULL filter ... OK (sum=900, JIT required)";
    case 5: return "Test 5: JIT compiles SUM of column id > 255 ... OK (sum=1500, col_id=260, JIT required)";
    case 6: return "Test 6: JIT CASE skip offset ... OK (dummy=NULL, amount=900, JIT required)";
    case 7: return "Test 7: JIT star 2-leaf SUM ... OK (sum_a=150, sum_b=15, 2 leaves, JIT required)";
    default: return nullptr;
  }
}
#endif

static bool
shouldRun(int testNum)
{
  if (onlyTest != 0 && testNum != onlyTest) return false;
  if (skipTest != 0 && testNum == skipTest) return false;
#if !defined(VM_TRACE) && !defined(ERROR_INSERT)
  const char *fake = fakeOkLineForErrorInsertTest(testNum);
  if (fake != nullptr) {
    printf("%s\n", fake);
    return false;
  }
#endif
  return true;
}

static void
usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s -c <connect_string> -m <mysql_port> [-v|--verbose] [-h|--help]\n"
          "\n"
          "Options:\n"
          "  -c  NDB management server connect string (default: localhost:1186)\n"
          "  -m  MySQL server port (default: 3306)\n"
          "  --only <N>     Run only test N\n"
          "  --skip <N>     Skip test N\n"
          "  -v, --verbose  Show detailed progress output\n"
          "  -h, --help     Show this help\n",
          prog);
}

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;

  /* Parse arguments */
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "--only") == 0 && i + 1 < argc) {
      onlyTest = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--skip") == 0 && i + 1 < argc) {
      skipTest = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-h") == 0 ||
               strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }
  }

  printf("=== testJoinAggJit ===\n");
  printf("Connect string: %s\n", connectString);
  printf("MySQL port: %d\n\n", mysqlPort);

  ndb_init();

  int exitCode = 0;

  {
    /* Scoping block: all NDB objects must be destroyed before ndb_end() */
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(30, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm %s: %s\n",
              connectString, clusterConn.get_latest_error_msg());
      exitCode = 1;
    }
    else if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30s\n");
      exitCode = 1;
    }
    else {
      /* Connect to MySQL (without database — it may not exist yet) */
      MYSQL *conn = connectMysql(mysqlPort);
      if (conn == nullptr) {
        exitCode = 1;
      } else {
        /* Ensure database exists */
        char createDb[128];
        snprintf(createDb, sizeof(createDb),
                 "CREATE DATABASE IF NOT EXISTS %s", TEST_DB);
        sqlExec(conn, createDb);

        char useDb[128];
        snprintf(useDb, sizeof(useDb), "USE %s", TEST_DB);
        sqlExec(conn, useDb);

        Ndb ndb(&clusterConn, TEST_DB);
        if (ndb.init() != 0) {
          fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
          exitCode = 1;
        } else {
          /* Test 1: JIT must compile local-attribute SUM */
          if (shouldRun(1)) {
            NdbRestarter restarter(connectString);
            if (createTestTables(conn) == 0 && insertTestData(conn) == 0) {
              if (testJitMustCompileSum(&ndb, conn, restarter, 1500) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTestTables(conn);
          }

          /* Test 2: JIT all-rejected SUM preserves NULL result */
          if (shouldRun(2)) {
            NdbRestarter restarter(connectString);
            if (createTestTables(conn) == 0 && insertTestData(conn) == 0) {
              if (testJitAllRejectedSumNull(&ndb, conn, restarter) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTestTables(conn);
          }

          /* Test 3: JIT canary for the linked-NULL filter path */
          if (shouldRun(3)) {
            NdbRestarter restarter(connectString);
            if (createT3Tables(conn) == 0 && insertT3Data(conn) == 0) {
              if (testJitLinkedNullSum(&ndb, conn, restarter) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropT3Tables(conn);
          }

          /* Test 4: unsupported-program fallback (33 aggregates — one
           * past BC_MAX_ACCS = 32). No error inserts: this exercises
           * the clean reject->interpreter path, which 4060/4062 would
           * abort. Reuses the shared jagg tables. */
          if (shouldRun(4)) {
            if (createTestTables(conn) == 0 && insertTestData(conn) == 0) {
              if (testJitUnsupportedFallback(&ndb, conn, 0) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTestTables(conn);
          }

          /* Test 5: operand-width boundary — JIT must compile SUM of a
           * column whose id is > 255 (proves the bridge admits col_id up to
           * 4095, matching NDB's MAX_ATTRIBUTES_IN_TABLE). */
          if (shouldRun(5)) {
            NdbRestarter restarter(connectString);
            if (createT5Tables(conn) == 0 && insertT5Data(conn) == 0) {
              if (testJitWideColumn(&ndb, conn, restarter, 1500) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropT5Tables(conn);
          }

          /* Test 6: embedded CASE non-zero skip_offset — JIT must jump
           * over the dummy aggregate and land on the real aggregate. */
          if (shouldRun(6)) {
            NdbRestarter restarter(connectString);
            if (createT6Tables(conn) == 0 && insertT6Data(conn) == 0) {
              if (testJitCaseSkipOffset(&ndb, conn, restarter) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropT6Tables(conn);
          }

          /* Test 7: star 2-leaf SUM — Phase 6-3 multi-leaf JIT. */
          if (shouldRun(7)) {
            NdbRestarter restarter(connectString);
            if (createT7Tables(conn) == 0 && insertT7Data(conn) == 0) {
              if (testJitStarTwoLeafSum(&ndb, conn, restarter, 150, 15) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropT7Tables(conn);
          }
        }
        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n%s\n", exitCode == 0 ? "All tests PASSED" : "Some tests FAILED");
  return exitCode;
}
