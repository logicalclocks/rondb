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
 * testJoinAggNdbApi — Integration test for pushdown join aggregation
 *                     using the NdbQueryBuilder API.
 *
 * Tests the complete NDB API path for pushed join queries with aggregation:
 *   NdbQueryBuilder → NdbQueryDef → NdbQuery → getAggregator()
 *
 * Schema (created via MySQL):
 *   jagg_parent(id INT PK, grp INT)
 *   jagg_child(parent_id INT PK, amount BIGINT)
 *
 * Usage: testJoinAggNdbApi -c <connect_string> -m <mysql_port> [-v|--verbose]
 *
 * ========================================================================
 * Wire format reference (SCAN_TABREQ AttrInfo for Test 3)
 * ========================================================================
 *
 * Test 3 query: SELECT jagg_parent.grp, COUNT(*), SUM(jagg_child.amount)
 *               FROM jagg_parent JOIN jagg_child
 *               ON jagg_child.parent_id = jagg_parent.id
 *               GROUP BY jagg_parent.grp
 *
 * The SCAN_TABREQ has three long signal sections:
 *   Section 0: Worker receiver IDs (1 word per worker)
 *   Section 1: AttrInfo = QueryTree nodes + QueryParameters
 *   Section 2: [boundsLen, bounds..., aggReceiverId, aggProgram...]
 *
 * --- Section 1: QueryTree (37 words) ---
 *
 * QueryTree header:
 *   [0]  0x00100002  nodeCount=2, treeLength=16 words
 *
 * Node 0: QN_ScanFragNode (root table scan on jagg_parent):
 *   [1]  0x00060004  type=QN_SCAN_FRAG(4), nodeLength=6
 *   [2]  0x00002010  requestInfo: NI_LINKED_ATTR | NI_AGGREGATE
 *   [3]  tableId     (jagg_parent)
 *   [4]  tableVersion
 *   [5]  0x00000002  NI_LINKED_ATTR Uint16Sequence: size=2,
 *                      col[0]=parent.id(0)
 *   [6]  0xbabe0001  col[1]=parent.grp(1), pad=0xBABE
 *        -> Parent projects [id, grp] for child operations.
 *
 * Node 1: QN_LookupNode (child PK lookup on jagg_child):
 *   [7]  0x00090001  type=QN_LOOKUP(1), nodeLength=9
 *   [8]  0x00006083  requestInfo: NI_HAS_PARENT | NI_KEY_LINKED |
 *                      NI_ATTR_LINKED | NI_AGGREGATE | NI_AGGREGATE_LEAF
 *   [9]  tableId     (jagg_child)
 *   [10] tableVersion
 *   Part1 (NI_HAS_PARENT):
 *   [11] 0x00000001  Uint16Sequence: size=1, parent=Node 0
 *   Part2 (NI_KEY_LINKED):
 *   [12] 0x00000001  key pattern length=1
 *   [13] 0x00020000  P_COL | spjRef 0 -> parent.spjProjection[0] = parent.id
 *   Part3 (NI_ATTR_LINKED):
 *   [14] 0x00010000  attr-linked pattern length=1
 *   [15] 0x00070001  P_ATTRINFO | spjRef 1 -> parent.spjProjection[1] = parent.grp
 *        -> Provides parent.grp as linked column for GROUP BY.
 *
 * Params for Node 0: QN_ScanFragParameters:
 *   [16] 0x000c0004  type=QN_SCAN_FRAG(4), paramLength=12
 *   [17] requestInfo: PI_ATTR_LIST | SFP_PARALLEL
 *   [18] resultData  (API receiver ID)
 *   [19] batch_size_rows
 *   [20] batch_size_bytes
 *   [21-23] unused/reserved
 *   PI_ATTR_LIST:
 *   [24] 0x00000003  3 AttributeHeaders
 *   [25] attrId=0 (parent.id)
 *   [26] attrId=1 (parent.grp)
 *   [27] attrId=0xffe8 (FRAGMENT pseudo-column)
 *
 * Params for Node 1: QN_LookupParameters:
 *   [28] 0x00090001  type=QN_LOOKUP(1), paramLength=9
 *   [29] requestInfo: PI_ATTR_LIST | PI_ATTR_INTERPRET
 *   [30] resultData  (API receiver ID)
 *   PI_ATTR_INTERPRET (minimal ExitOK — creates 5-word interpreter
 *     header for linked subroutine framing):
 *   [31] 0x00000001  program_len=1, subroutine_len=0
 *   [32] 0x00000012  Interpreter::ExitOK (opcode 18)
 *   PI_ATTR_LIST:
 *   [33] 0x00000003  3 AttributeHeaders
 *   [34] attrId=0 (child.parent_id)
 *   [35] attrId=1 (child.amount)
 *   [36] attrId=0xffe8 (FRAGMENT pseudo-column)
 *
 * --- Section 2: Bounds + Aggregation ---
 *
 *   [0]  boundsLen=0 (full table scan, no bounds)
 *   [1]  aggReceiverId (object map ID for aggregation results)
 *   [2..13] aggProgram (12 words):
 *     Header (8 words):
 *       [0] magic=0x0721, programLength=12
 *       [1] n_gb_cols=1, n_agg_results=2
 *       [2] version=PUSHDOWN_AGGREGATION_VERSION(2)
 *       [3-7] reserved (0)
 *     GroupBy columns (1 word):
 *       [8] GroupBy(col 0 | AGG_LINKED_COL_FLAG)
 *           -> GROUP BY linked column at position 0 (= parent.grp)
 *     Instructions (3 words):
 *       [9]  kOpLoadCol: type=BIGINT, reg=0, colId=1 (child.amount)
 *       [10] kOpCount:   agg[0]=COUNT(*), reg=0
 *       [11] kOpSum:     agg[1]=SUM(reg0), reg=0
 *
 * --- Linked column position mapping ---
 *
 * The aggregation program references parent columns by a 0-based position
 * index (with AGG_LINKED_COL_FLAG set), NOT by table attrId.  The position
 * maps to the order of P_ATTRINFO entries in the NI_ATTR_LINKED pattern
 * on the child node (Part3 above):
 *
 *   NI_ATTR_LINKED pattern          Linked buffer at runtime     Agg program ref
 *   ─────────────────────────────  ────────────────────────     ───────────────
 *   P_ATTRINFO|spjRef 1 [word 15]  position 0: parent.grp     GroupBy(0|LINKED)
 *
 * At runtime, DBSPJ expands each P_ATTRINFO(spjRef) by copying the
 * AttributeHeader + data for parent spjProjection[spjRef] into the linked
 * buffer, in pattern order.  The AggInterpreter then walks the linked
 * buffer, skipping entries until it reaches the requested position index.
 *
 * If multiple parent columns were needed (e.g. GROUP BY grp, other_col):
 *   NI_ATTR_LINKED would have two P_ATTRINFO entries,
 *   producing positions 0 and 1 in the linked buffer,
 *   referenced as GroupBy(0|LINKED) and GroupBy(1|LINKED).
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include <NdbAggregator.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryOperation.hpp"

#include <mysql.h>

#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

/* Verbose output control */
static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *PARENT_TABLE = "jagg_parent";
static const char *CHILD_TABLE = "jagg_child";
static const char *T4_REGION = "t4_region";
static const char *T4_ORDER = "t4_order";
static const char *T4_LINE = "t4_line";
static const char *TEST_DB = "test_db";

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

/* ------------------------------------------------------------------ */
/* MySQL verification query                                            */
/* ------------------------------------------------------------------ */

static int
verifyWithMysql(MYSQL *conn, const char *testName, const char *query,
                const std::map<Int32, Int64> &expected)
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

  std::map<Int32, Int64> mysqlResults;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result)) != nullptr) {
    Int32 grp = atoi(row[0]);
    Int64 val = atoll(row[1]);
    mysqlResults[grp] = val;
  }
  mysql_free_result(result);

  if (mysqlResults.size() != expected.size()) {
    fprintf(stderr, "  %s: MySQL returned %zu groups, expected %zu\n",
            testName, mysqlResults.size(), expected.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = mysqlResults.find(e.first);
    if (it == mysqlResults.end()) {
      fprintf(stderr, "  %s: MySQL missing group %d\n", testName, e.first);
      return -1;
    }
    if (it->second != e.second) {
      fprintf(stderr, "  %s: MySQL group %d: expected %lld, got %lld\n",
              testName, e.first, (long long)e.second, (long long)it->second);
      return -1;
    }
  }
  V("  MySQL verification OK\n");
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

/* Multi-aggregate GROUP BY verification: query returns (grp, val1, val2) */
static int
verifyMultiAggWithMysql(MYSQL *conn, const char *testName, const char *query,
                        const std::map<Int32, std::pair<Int64, Int64>> &expected)
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

  std::map<Int32, std::pair<Int64, Int64>> mysqlResults;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result)) != nullptr) {
    Int32 grp = atoi(row[0]);
    Int64 val1 = atoll(row[1]);
    Int64 val2 = atoll(row[2]);
    mysqlResults[grp] = {val1, val2};
  }
  mysql_free_result(result);

  if (mysqlResults.size() != expected.size()) {
    fprintf(stderr, "  %s: MySQL returned %zu groups, expected %zu\n",
            testName, mysqlResults.size(), expected.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = mysqlResults.find(e.first);
    if (it == mysqlResults.end()) {
      fprintf(stderr, "  %s: MySQL missing group %d\n", testName, e.first);
      return -1;
    }
    if (it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      fprintf(stderr, "  %s: MySQL group %d mismatch\n", testName, e.first);
      return -1;
    }
  }
  V("  MySQL verification OK\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 1: SUM(amount) GROUP BY grp                                    */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT grp, SUM(amount)                                           */
/*   FROM jagg_parent JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id */
/*   GROUP BY grp                                                      */
/* ------------------------------------------------------------------ */

static int
testSumGroupBy(Ndb *ndb, MYSQL *conn,
               const std::map<Int32, Int64> &expected)
{
  printf("Test 1: SUM(amount) GROUP BY grp ... ");
  fflush(stdout);

  /* First verify expected results via MySQL */
  if (verifyWithMysql(conn, "Test 1",
        "SELECT grp, SUM(amount) FROM jagg_parent "
        "JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id "
        "GROUP BY grp ORDER BY grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(PARENT_TABLE);
  dict->invalidateTable(CHILD_TABLE);
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Build aggregation program:
   *   GROUP BY grp (parent column → AGG_LINKED_COL_FLAG)
   *   SUM(amount)  (child column)
   */
  NdbAggregator agg(childTab);
  if (!agg.GroupBy(0 | AGG_LINKED_COL_FLAG) ||  /* linked projection pos 0 = grp */
      !agg.LoadColumn("amount", 0) ||   /* reg 0 */
      !agg.Sum(0, 0) ||                 /* agg[0] = SUM(reg 0) */
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Build pushed join query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Root: scan parent table */
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Join key: child.parent_id = parent.id */
  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"),
    nullptr
  };

  /* Child options: attach aggregation + linked projection for grp */
  NdbQueryOptions opts;
  opts.setAggregation(agg);

  const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  opts.addLinkedProjection(grpLink);

  /* Child: lookup child_t by parent_id */
  const NdbQueryLookupOperationDef *childOp =
      qb->readTuple(childTab, joinKey, &opts);
  if (childOp == nullptr) {
    printf("FAILED (readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Prepare query definition */
  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  V("\n  Query prepared: %u operations, isScan=%d\n",
    queryDef->getNoOfOperations(), queryDef->isScanQuery());

  /* Execute query */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction: %s)\n", ndb->getNdbError().message);
    queryDef->destroy();
    return -1;
  }

  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Set up projections — SPJ needs at least one column per operation */
  NdbQueryOperation *parentQueryOp = query->getQueryOperation((Uint32)0);
  parentQueryOp->getValue("id");
  parentQueryOp->getValue("grp");
  NdbQueryOperation *childQueryOp = query->getQueryOperation((Uint32)1);
  childQueryOp->getValue("parent_id");
  childQueryOp->getValue("amount");

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume all scan batches */
  NdbQuery::NextResultOutcome outcome;
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n", query->getNdbError().code, query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  V("  Scan consumed %u rows\n", rowCount);

  /* Retrieve aggregated results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  std::map<Int32, Int64> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = sumVal;
    V("  grp=%d SUM(amount)=%lld\n", grpVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Verify results */
  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("FAILED (missing group %d)\n", e.first);
      return -1;
    }
    if (it->second != e.second) {
      printf("FAILED (group %d: expected SUM=%lld, got %lld)\n",
             e.first, (long long)e.second, (long long)it->second);
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: COUNT(*), SUM(amount) — no GROUP BY                         */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT COUNT(*), SUM(amount)                                      */
/*   FROM jagg_parent JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id */
/* ------------------------------------------------------------------ */

static int
testCountSum(Ndb *ndb, MYSQL *conn, Int64 expectedCount, Int64 expectedSum)
{
  printf("Test 2: COUNT(*), SUM(amount) ... ");
  fflush(stdout);

  /* First verify expected results via MySQL */
  if (verifyScalarWithMysql(conn, "Test 2",
        "SELECT COUNT(*), SUM(amount) FROM jagg_parent "
        "JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id",
        {expectedCount, expectedSum}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Build aggregation: COUNT(*), SUM(amount) — no GROUP BY */
  NdbAggregator agg(childTab);
  if (!agg.LoadColumn("amount", 0) ||
      !agg.Count(0, 0) ||  /* agg[0] = COUNT */
      !agg.Sum(1, 0) ||    /* agg[1] = SUM */
      !agg.Finalize()) {
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

  /* Set up projections — SPJ needs at least one column per operation */
  NdbQueryOperation *parentQueryOp = query->getQueryOperation((Uint32)0);
  parentQueryOp->getValue("id");
  NdbQueryOperation *childQueryOp = query->getQueryOperation((Uint32)1);
  childQueryOp->getValue("parent_id");
  childQueryOp->getValue("amount");

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan */
  NdbQuery::NextResultOutcome outcome2;
  while ((outcome2 = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome2 == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n", query->getNdbError().code, query->getNdbError().message);
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

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 actualCount = countRes.data_int64();

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 actualSum = sumRes.data_int64();

  V("\n  COUNT(*)=%lld SUM(amount)=%lld\n",
    (long long)actualCount, (long long)actualSum);

  /* Verify no more records */
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

  if (actualCount != expectedCount) {
    printf("FAILED (COUNT: expected %lld, got %lld)\n",
           (long long)expectedCount, (long long)actualCount);
    return -1;
  }
  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  printf("OK (count=%lld, sum=%lld)\n",
         (long long)actualCount, (long long)actualSum);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: Multiple groups with SUM and COUNT                          */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT grp, COUNT(*), SUM(amount)                                 */
/*   FROM jagg_parent JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id */
/*   GROUP BY grp                                                      */
/* ------------------------------------------------------------------ */

static int
testMultiAggGroupBy(Ndb *ndb, MYSQL *conn,
                    const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 3: COUNT(*), SUM(amount) GROUP BY grp ... ");
  fflush(stdout);

  /* First verify expected results via MySQL */
  if (verifyMultiAggWithMysql(conn, "Test 3",
        "SELECT grp, COUNT(*), SUM(amount) FROM jagg_parent "
        "JOIN jagg_child ON jagg_child.parent_id = jagg_parent.id "
        "GROUP BY grp ORDER BY grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Build aggregation: GROUP BY grp, COUNT(*), SUM(amount) */
  NdbAggregator agg(childTab);
  if (!agg.GroupBy(0 | AGG_LINKED_COL_FLAG) ||  /* linked projection pos 0 = grp */
      !agg.LoadColumn("amount", 0) ||
      !agg.Count(0, 0) ||  /* agg[0] = COUNT */
      !agg.Sum(1, 0) ||    /* agg[1] = SUM */
      !agg.Finalize()) {
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
  opts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
  opts.addLinkedProjection(grpLink);

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

  /* Set up projections — SPJ needs at least one column per operation */
  NdbQueryOperation *parentQueryOp = query->getQueryOperation((Uint32)0);
  parentQueryOp->getValue("id");
  parentQueryOp->getValue("grp");
  NdbQueryOperation *childQueryOp = query->getQueryOperation((Uint32)1);
  childQueryOp->getValue("parent_id");
  childQueryOp->getValue("amount");

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome3;
  while ((outcome3 = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome3 == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n", query->getNdbError().code, query->getNdbError().message);
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

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = {countVal, sumVal};
    V("\n  grp=%d COUNT=%lld SUM=%lld", grpVal,
      (long long)countVal, (long long)sumVal);
  }
  V("\n");

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("FAILED (missing group %d)\n", e.first);
      return -1;
    }
    if (it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group %d: expected COUNT=%lld SUM=%lld, "
             "got COUNT=%lld SUM=%lld)\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: 3-Way Join with Multi-Level Linked Attributes              */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.area, o.discount, SUM(l.amount), COUNT(*)                */
/*   FROM t4_region r                                                  */
/*   JOIN t4_order o ON o.region_id = r.id                             */
/*   JOIN t4_line l ON l.order_id = o.region_id                        */
/*   WHERE o.priority >= r.area                                        */
/*   GROUP BY r.area, o.discount                                       */
/* ------------------------------------------------------------------ */

static int
createTest4Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t4_line");
  sqlExec(conn, "DROP TABLE IF EXISTS t4_order");
  sqlExec(conn, "DROP TABLE IF EXISTS t4_region");

  if (sqlExec(conn,
        "CREATE TABLE t4_region ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  area INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t4_order ("
        "  region_id INT NOT NULL PRIMARY KEY,"
        "  priority INT NOT NULL,"
        "  discount BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t4_line ("
        "  order_id INT NOT NULL PRIMARY KEY,"
        "  amount BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 4 tables\n");
  return 0;
}

static int
insertTest4Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t4_region VALUES "
        "(1,1),(2,2),(3,3),(4,4),(5,5)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t4_order VALUES "
        "(1,3,5),(2,1,10),(3,5,15),(4,2,20),(5,6,25)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t4_line VALUES "
        "(1,100),(2,200),(3,300),(4,400),(5,500)") != 0) return -1;

  V("Inserted test 4 data\n");
  return 0;
}

static int
dropTest4Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t4_line");
  sqlExec(conn, "DROP TABLE IF EXISTS t4_order");
  sqlExec(conn, "DROP TABLE IF EXISTS t4_region");
  V("Dropped test 4 tables\n");
  return 0;
}

static int
testThreeWayJoin(Ndb *ndb, MYSQL *conn)
{
  printf("Test 4: 3-way join with linked param filter ... ");
  fflush(stdout);

  /* Verify with MySQL first */
  {
    const char *query =
      "SELECT r.area, o.discount, SUM(l.amount), COUNT(*) "
      "FROM t4_region r "
      "JOIN t4_order o ON o.region_id = r.id "
      "JOIN t4_line l ON l.order_id = o.region_id "
      "WHERE o.priority >= r.area "
      "GROUP BY r.area, o.discount "
      "ORDER BY r.area";
    V("  MySQL verify: %s\n", query);
    if (mysql_query(conn, query) != 0) {
      printf("FAILED (MySQL verify: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    if (result == nullptr) {
      printf("FAILED (mysql_store_result: %s)\n", mysql_error(conn));
      return -1;
    }
    Uint32 mysqlRows = 0;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      Int32 area = atoi(row[0]);
      Int64 disc = atoll(row[1]);
      Int64 sum = atoll(row[2]);
      Int64 cnt = atoll(row[3]);
      V("  MySQL: area=%d disc=%lld sum=%lld cnt=%lld\n",
        area, (long long)disc, (long long)sum, (long long)cnt);
      mysqlRows++;
    }
    mysql_free_result(result);
    if (mysqlRows != 3) {
      printf("FAILED (MySQL returned %u groups, expected 3)\n", mysqlRows);
      return -1;
    }
    V("  MySQL verification OK\n");
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T4_REGION);
  dict->invalidateTable(T4_ORDER);
  dict->invalidateTable(T4_LINE);
  const NdbDictionary::Table *regionTab = dict->getTable(T4_REGION);
  const NdbDictionary::Table *orderTab = dict->getTable(T4_ORDER);
  const NdbDictionary::Table *lineTab = dict->getTable(T4_LINE);
  if (regionTab == nullptr || orderTab == nullptr || lineTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Look up parent columns needed for aggregation and filter */
  const NdbDictionary::Column *areaCol = regionTab->getColumn("area");
  const NdbDictionary::Column *discountCol = orderTab->getColumn("discount");
  const NdbDictionary::Column *priCol = orderTab->getColumn("priority");
  if (areaCol == nullptr || discountCol == nullptr || priCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  /* Build aggregation program for t4_line (the leaf):
   *   GROUP BY linked position 0 (= region.area, grandparent)
   *   GROUP BY linked position 1 (= order.discount, parent)
   *   SUM(amount)  -> agg[0]
   *   COUNT(*)     -> agg[1]
   */
  NdbAggregator agg(lineTab);
  if (!agg.GroupByLinked(0, areaCol) ||
      !agg.GroupByLinked(1, discountCol) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Count(1, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Build interpreted code for t4_order filter: priority >= area.
   * branch_col_le_param is INVERTED: branches when col >= param. */
  Uint32 priorityAttrId = priCol->getColumnNo();

  NdbInterpretedCode code(orderTab);
  code.branch_col_le_param(priorityAttrId, 0, 0);  /* branch to 0 when pri >= area */
  code.interpret_exit_nok();                         /* fall-through: reject */
  code.def_label(0);
  code.interpret_exit_ok();
  if (code.finalise() != 0) {
    printf("FAILED (NdbInterpretedCode finalise: %s)\n",
           code.getNdbError().message);
    return -1;
  }

  /* Build 3-node pushed join query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Node 0: scan t4_region */
  const NdbQueryTableScanOperationDef *regionOp = qb->scanTable(regionTab);
  if (regionOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup t4_order (region_id = region.id) with filter */
  const NdbQueryOperand *orderJoinKey[] = {
    qb->linkedValue(regionOp, "id"),
    nullptr
  };

  NdbQueryOptions orderOpts;
  const NdbQueryOperand *filterParams[] = {
    qb->linkedValue(regionOp, "area"),
    nullptr
  };
  orderOpts.setInterpretedCode(code);
  orderOpts.setParameters(filterParams);

  const NdbQueryLookupOperationDef *orderOp =
      qb->readTuple(orderTab, orderJoinKey, &orderOpts);
  if (orderOp == nullptr) {
    printf("FAILED (readTuple order: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup t4_line (order_id = order.region_id) with aggregation */
  const NdbQueryOperand *lineJoinKey[] = {
    qb->linkedValue(orderOp, "region_id"),
    nullptr
  };

  NdbQueryOptions lineOpts;
  lineOpts.setAggregation(agg);

  const NdbLinkedOperand *areaLink = qb->linkedValue(regionOp, "area");
  const NdbLinkedOperand *discLink = qb->linkedValue(orderOp, "discount");
  if (areaLink == nullptr || discLink == nullptr) {
    printf("FAILED (linkedValue for projection: %s)\n",
           qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  lineOpts.addLinkedProjection(areaLink);
  lineOpts.addLinkedProjection(discLink);

  const NdbQueryLookupOperationDef *lineOp =
      qb->readTuple(lineTab, lineJoinKey, &lineOpts);
  if (lineOp == nullptr) {
    printf("FAILED (readTuple line: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Prepare query definition */
  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  V("\n  Query prepared: %u operations, isScan=%d\n",
    queryDef->getNoOfOperations(), queryDef->isScanQuery());

  /* Execute query */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction: %s)\n", ndb->getNdbError().message);
    queryDef->destroy();
    return -1;
  }

  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Set up projections for scan root (needed for nextResult iteration).
   *
   * IMPORTANT: Do NOT call getValue() on intermediate aggregate nodes
   * (order) or the aggregate leaf (line).  When FLUSH_AI is suppressed
   * for intermediate aggregate nodes in DBSPJ, PI_ATTR_LIST columns are
   * prepended to NI_LINKED_ATTR columns in the TRANSID_AI.  This shifts
   * the m_offset[] indices so P_ATTRINFO(spjIdx) reads the wrong column.
   * Omitting getValue() avoids PI_ATTR_LIST entirely, keeping the row
   * data aligned with SPJ projection indices.
   */
  NdbQueryOperation *regionQueryOp = query->getQueryOperation((Uint32)0);
  regionQueryOp->getValue("id");
  regionQueryOp->getValue("area");

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume all scan batches */
  NdbQuery::NextResultOutcome outcome;
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: error %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  V("  Scan consumed %u rows\n", rowCount);

  /* Retrieve aggregated results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  struct ResultGroup {
    Int64 discount;
    Int64 sum_amount;
    Int64 count;
  };
  std::map<Int32, ResultGroup> actual;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column areaCol = rec.FetchGroupbyColumn();
    Int32 areaVal = areaCol.data_int32();

    NdbAggregator::Column discCol = rec.FetchGroupbyColumn();
    Int64 discVal = discCol.data_int64();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    NdbAggregator::Result cntRes = rec.FetchAggregationResult();
    Int64 cntVal = cntRes.data_int64();

    actual[areaVal] = {discVal, sumVal, cntVal};
    V("  area=%d disc=%lld SUM=%lld COUNT=%lld\n",
      areaVal, (long long)discVal, (long long)sumVal, (long long)cntVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Expected:
   *   area=1, disc=5:  SUM=100, COUNT=1
   *   area=3, disc=15: SUM=300, COUNT=1
   *   area=5, disc=25: SUM=500, COUNT=1
   */
  struct Expected {
    Int32 area;
    Int64 discount;
    Int64 sum_amount;
    Int64 count;
  };
  Expected exp[] = {
    {1, 5, 100, 1},
    {3, 15, 300, 1},
    {5, 25, 500, 1}
  };
  Uint32 numExpected = sizeof(exp) / sizeof(exp[0]);

  if (actual.size() != numExpected) {
    printf("FAILED (expected %u groups, got %zu)\n",
           numExpected, actual.size());
    return -1;
  }

  for (Uint32 i = 0; i < numExpected; i++) {
    auto it = actual.find(exp[i].area);
    if (it == actual.end()) {
      printf("FAILED (missing area group %d)\n", exp[i].area);
      return -1;
    }
    if (it->second.discount != exp[i].discount ||
        it->second.sum_amount != exp[i].sum_amount ||
        it->second.count != exp[i].count) {
      printf("FAILED (area=%d: expected disc=%lld sum=%lld cnt=%lld, "
             "got disc=%lld sum=%lld cnt=%lld)\n",
             exp[i].area,
             (long long)exp[i].discount,
             (long long)exp[i].sum_amount,
             (long long)exp[i].count,
             (long long)it->second.discount,
             (long long)it->second.sum_amount,
             (long long)it->second.count);
      return -1;
    }
  }

  printf("OK (3 groups)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static void
usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s -c <connect_string> -m <mysql_port> [-v|--verbose] [-h|--help]\n"
          "\n"
          "Options:\n"
          "  -c  NDB management server connect string (default: localhost:1186)\n"
          "  -m  MySQL server port (default: 3306)\n"
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

  printf("=== testJoinAggNdbApi ===\n");
  printf("Connect string: %s\n", connectString);
  printf("MySQL port: %d\n\n", mysqlPort);

  ndb_init();

  int exitCode = 0;

  {
    /* Scoping block: all NDB objects must be destroyed before ndb_end() */
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm: %s\n", connectString);
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
        }
        else if (createTestTables(conn) != 0 ||
                 insertTestData(conn) != 0) {
          exitCode = 1;
        }
        else {
          /*
           * Test data:
           *   parent: (1,1), (2,1), (3,2), (4,2), (5,3)
           *   child:  (1,100), (2,200), (3,300), (4,400), (5,500)
           *
           * Join result (parent_id = id):
           *   (id=1, grp=1, amount=100)
           *   (id=2, grp=1, amount=200)
           *   (id=3, grp=2, amount=300)
           *   (id=4, grp=2, amount=400)
           *   (id=5, grp=3, amount=500)
           *
           * GROUP BY grp:
           *   grp=1: SUM=300, COUNT=2
           *   grp=2: SUM=700, COUNT=2
           *   grp=3: SUM=500, COUNT=1
           *
           * Totals: COUNT=5, SUM=1500
           */

          /* Test 1: SUM(amount) GROUP BY grp */
          {
            std::map<Int32, Int64> expected = {
              {1, 300}, {2, 700}, {3, 500}
            };
            if (testSumGroupBy(&ndb, conn, expected) != 0) {
              exitCode = 1;
            }
          }

          /* Test 2: COUNT(*), SUM(amount) — no GROUP BY */
          if (testCountSum(&ndb, conn, 5, 1500) != 0) {
            exitCode = 1;
          }

          /* Test 3: COUNT(*), SUM(amount) GROUP BY grp */
          {
            std::map<Int32, std::pair<Int64, Int64>> expected = {
              {1, {2, 300}}, {2, {2, 700}}, {3, {1, 500}}
            };
            if (testMultiAggGroupBy(&ndb, conn, expected) != 0) {
              exitCode = 1;
            }
          }
        }
        dropTestTables(conn);

        /* Test 4: 3-way join with linked param filter */
        if (createTest4Tables(conn) == 0 &&
            insertTest4Data(conn) == 0) {
          if (testThreeWayJoin(&ndb, conn) != 0) {
            exitCode = 1;
          }
        } else {
          exitCode = 1;
        }
        dropTest4Tables(conn);
        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n%s\n", exitCode == 0 ? "All tests PASSED" : "Some tests FAILED");
  return exitCode;
}
