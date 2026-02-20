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
 * --- Section 0: Worker Receiver IDs (1 word) ---
 *
 *   worker[0] receiverId (single-worker scan, parallelism=1)
 *
 * --- Section 1: QueryTree + Params (37 words) ---
 *
 * QueryTree header:
 *   [0]  0x00100002  nodeCount=2, treeLength=16 words
 *
 * Node 0: QN_ScanFragNode (root table scan on jagg_parent):
 *   [1]  0x00060004  type=QN_SCAN_FRAG(4), nodeLength=6
 *   [2]  0x00002010  requestInfo: NI_LINKED_ATTR | NI_AGGREGATE
 *   [3]  0x0000001d  tableId=29 (jagg_parent)
 *   [4]  0x00000001  tableVersion=1
 *   Part4 (NI_LINKED_ATTR Uint16Sequence, 2 cols for children):
 *   [5]  0x00000002  size=2, col[0]=id(0)
 *   [6]  0xbabe0001  col[1]=grp(1), pad=0xBABE
 *        -> Projects [id, grp] for child operations.
 *
 * Node 1: QN_LookupNode (child PK lookup on jagg_child):
 *   [7]  0x00090001  type=QN_LOOKUP(1), nodeLength=9
 *   [8]  0x00006083  requestInfo: NI_HAS_PARENT | NI_KEY_LINKED |
 *                      NI_ATTR_LINKED | NI_AGGREGATE | NI_AGGREGATE_LEAF
 *   [9]  0x0000001f  tableId=31 (jagg_child)
 *   [10] 0x00000001  tableVersion=1
 *   Part1 (NI_HAS_PARENT):
 *   [11] 0x00000001  Uint16Sequence: size=1, parent=Node 0
 *   Part2 (NI_KEY_LINKED, key: child.parent_id = parent.id):
 *   [12] 0x00000001  key pattern length=1
 *   [13] 0x00020000  P_COL(0x2) col=0 -> parent.spjProj[0] = parent.id
 *   Part3 (NI_ATTR_LINKED, linked col for GROUP BY):
 *   [14] 0x00010000  attr-linked pattern length=1
 *   [15] 0x00070001  P_ATTRINFO(0x7) col=1 -> parent.spjProj[1] = grp
 *        -> Provides parent.grp as linked column for GROUP BY.
 *
 * Params for Node 0: QN_ScanFragParameters:
 *   [16] 0x000c0004  type=QN_SCAN_FRAG(4), paramLength=12
 *   [17] 0x00020001  requestInfo: PI_ATTR_LIST | SFP_PARALLEL
 *   [18] resultData  (API receiver ID)
 *   [19] batch_size_rows
 *   [20] batch_size_bytes
 *   [21-23] unused/reserved
 *   PI_ATTR_LIST:
 *   [24] 0x00000003  3 AttributeHeaders
 *   [25] 0x00000000  attrId=0 (parent.id)
 *   [26] 0x00010000  attrId=1 (parent.grp)
 *   [27] 0xffe80000  attrId=0xFFE8 (CORR_FACTOR64 pseudo-column)
 *
 * Params for Node 1: QN_LookupParameters:
 *   [28] 0x00090001  type=QN_LOOKUP(1), paramLength=9
 *   [29] 0x00000009  requestInfo: PI_ATTR_LIST | PI_ATTR_INTERPRET
 *   [30] resultData  (API receiver ID)
 *   PI_ATTR_INTERPRET (minimal ExitOK — creates interpreter
 *     header for linked subroutine framing):
 *   [31] 0x00000001  program_len=1
 *   [32] 0x00000012  EXIT_OK (opcode 18)
 *   PI_ATTR_LIST:
 *   [33] 0x00000003  3 AttributeHeaders
 *   [34] 0x00000000  attrId=0 (child.parent_id)
 *   [35] 0x00010000  attrId=1 (child.amount)
 *   [36] 0xffe80000  attrId=0xFFE8 (CORR_FACTOR64 pseudo-column)
 *
 * --- Section 2: Bounds + Aggregation (14 words) ---
 *
 *   [0]  boundsLen=0 (full table scan, no bounds)
 *   [1]  aggReceiverId=0x18 (object map ID for aggregation results)
 *   [2..13] aggProgram (12 words):
 *     Header (8 words):
 *       [0] 0x0721000c  magic=0x0721, totalLen=12
 *       [1] 0x00010002  n_gb_cols=1, n_agg_results=2
 *       [2] 0x00000002  version=PUSHDOWN_AGGREGATION_VERSION(2)
 *       [3-7] 0x00000000  reserved
 *     GroupBy column descriptors (1 word):
 *       [8] 0x80000000  GB col 0: (0x8000|pos 0)=LINKED parent.grp
 *     Instructions (3 words):
 *       [9]  0x1d200001  kOpLoadCol: colId=1 (child.amount), reg=0
 *       [10] 0x34000000  kOpCount:   agg[0]=COUNT(*)
 *       [11] 0x28000001  kOpSum:     agg[1]=SUM(reg0)
 *
 * --- Linked column position mapping ---
 *
 * The aggregation program references parent columns by a 0-based position
 * index (with AGG_LINKED_COL_FLAG=0x8000 set), NOT by table attrId.  The
 * position maps to the order of P_ATTRINFO entries in the NI_ATTR_LINKED
 * pattern on the child node (Part3 above):
 *
 *   NI_ATTR_LINKED pattern          Linked buffer at runtime     Agg program ref
 *   ─────────────────────────────  ────────────────────────     ───────────────
 *   P_ATTRINFO|spjRef 1 [word 15]  position 0: parent.grp     GroupBy(0|LINKED)
 *
 * At runtime, DBSPJ expands each P_ATTRINFO(spjRef) by prepending the
 * source (tableId, tableVersion), then copying the AttributeHeader + data
 * for parent spjProjection[spjRef] into the linked buffer, in pattern
 * order.  The AggInterpreter walks the linked buffer, skipping the
 * (tableId, tableVersion) prefix and data entries until it reaches
 * the requested position index.
 *
 * Each linked buffer entry at runtime:
 *   [tableId] [tableVersion] [AH(attrId, dataSize)] [data...]
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
static const char *T4_REGION = "t4_region";
static const char *T4_ORDER = "t4_order";
static const char *T4_LINE = "t4_line";
static const char *TEST_DB = "test_db";

static const char *T5_DEPT      = "t5_dept";
static const char *T5_EMP       = "t5_emp";
static const char *T6_CATEGORY  = "t6_category";
static const char *T6_PRODUCT   = "t6_product";
static const char *T7_COUNTRY   = "t7_country";
static const char *T7_CITY      = "t7_city";
static const char *T7_STORE     = "t7_store";
static const char *T7_SALE      = "t7_sale";
static const char *T8_ORDER     = "t8_order";
static const char *T8_ITEM      = "t8_item";
static const char *T10_PARENT   = "t10_parent";
static const char *T10_CHILD    = "t10_child";
static const char *T12_PARENT   = "t12_parent";
static const char *T12_CHILD    = "t12_child";

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
/*                                                                     */
/* ================================================================== */
/* Wire format reference (SCAN_TABREQ for Test 4)                     */
/* ================================================================== */
/*                                                                     */
/* --- Section 0: Worker Receiver IDs (1 word) ---                     */
/*                                                                     */
/*   worker[0] receiverId (single-worker scan, parallelism=1)          */
/*                                                                     */
/* --- Section 1: QueryTree + Params (54 words) ---                    */
/*                                                                     */
/* QueryTree header:                                                   */
/*   [0]  0x001d0003  nodeCount=3, treeLength=29 words                 */
/*                                                                     */
/* Node 0: QN_ScanFragNode (root table scan on t4_region):             */
/*   [1]  0x00060004  type=QN_SCAN_FRAG(4), nodeLength=6               */
/*   [2]  0x00002010  requestInfo: NI_LINKED_ATTR | NI_AGGREGATE       */
/*   [3]  tableId     (t4_region)                                      */
/*   [4]  tableVersion                                                 */
/*   Part4 (NI_LINKED_ATTR Uint16Sequence, 2 cols for children):       */
/*   [5]  0x00000002  size=2, col[0]=id(0)                             */
/*   [6]  0xbabe0001  col[1]=area(1), pad=0xBABE                       */
/*        -> Projects [id, area] for child operations.                 */
/*                                                                     */
/* Node 1: QN_LookupNode (child PK lookup on t4_order):                */
/*   [7]  0x000b0001  type=QN_LOOKUP(1), nodeLength=11                 */
/*   [8]  0x00002093  requestInfo: NI_HAS_PARENT | NI_KEY_LINKED |     */
/*                      NI_LINKED_ATTR | NI_ATTR_LINKED | NI_AGGREGATE */
/*   [9]  tableId     (t4_order)                                       */
/*   [10] tableVersion                                                 */
/*   Part1 (NI_HAS_PARENT):                                            */
/*   [11] 0x00000001  Uint16Sequence: size=1, parent=Node 0            */
/*   Part2 (NI_KEY_LINKED, key: o.region_id = r.id):                   */
/*   [12] 0x00000001  key pattern length=1                             */
/*   [13] 0x00020000  P_COL(0x2) col=0 -> parent.spjProj[0] = r.id    */
/*   Part3 (NI_ATTR_LINKED, filter param for WHERE o.priority>=r.area):*/
/*   [14] 0x00010000  attr-linked pattern length=1                     */
/*   [15] 0x00070001  P_ATTRINFO(0x7) col=1 -> parent.spjProj[1]=area */
/*        -> Provides r.area as linked param 0 for the filter below.   */
/*   Part4 (NI_LINKED_ATTR Uint16Sequence, 2 cols for children):       */
/*   [16] 0x00000002  size=2, col[0]=region_id(0)                      */
/*   [17] 0xbabe0002  col[1]=discount(2), pad=0xBABE                   */
/*        -> Projects [region_id, discount] for Node 2 operations.     */
/*                                                                     */
/* Node 2: QN_LookupNode (grandchild PK lookup on t4_line, agg leaf): */
/*   [18] 0x000b0001  type=QN_LOOKUP(1), nodeLength=11                 */
/*   [19] 0x00006083  requestInfo: NI_HAS_PARENT | NI_KEY_LINKED |     */
/*                      NI_ATTR_LINKED | NI_AGGREGATE | NI_AGG_LEAF    */
/*   [20] tableId     (t4_line)                                        */
/*   [21] tableVersion                                                 */
/*   Part1 (NI_HAS_PARENT):                                            */
/*   [22] 0x00010001  Uint16Sequence: size=1, parent=Node 1            */
/*   Part2 (NI_KEY_LINKED, key: l.order_id = o.region_id):             */
/*   [23] 0x00000001  key pattern length=1                             */
/*   [24] 0x00020000  P_COL(0x2) col=0 -> parent.spjProj[0]=region_id */
/*   Part3 (NI_ATTR_LINKED, linked cols for GROUP BY):                 */
/*   [25] 0x00030000  attr-linked pattern length=3                     */
/*   [26] 0x00050001  P_PARENT(0x5) levels=1 -> go up Node 1->Node 0  */
/*   [27] 0x00070001  P_ATTRINFO(0x7) col=1 -> grandparent col 1=area */
/*   [28] 0x00070001  P_ATTRINFO(0x7) col=1 -> parent col 1=discount  */
/*        -> Linked positions: pos 0=r.area (grandparent),             */
/*           pos 1=o.discount (parent).                                */
/*        Note: P_PARENT(levels=1) shifts the "current ancestor" to    */
/*        Node 0 (grandparent), so the first P_ATTRINFO reads from    */
/*        Node 0's spjProjection. The second P_ATTRINFO (no P_PARENT   */
/*        prefix) reads from the direct parent Node 1's projection.    */
/*                                                                     */
/* Params for Node 0: QN_ScanFragParameters:                           */
/*   [29] 0x000c0004  type=QN_SCAN_FRAG(4), paramLength=12            */
/*   [30] requestInfo: PI_ATTR_LIST | SFP_PARALLEL                     */
/*   [31] resultData  (API receiver ID)                                */
/*   [32] batch_size_rows                                              */
/*   [33] batch_size_bytes                                             */
/*   [34-36] unused/reserved                                           */
/*   PI_ATTR_LIST:                                                     */
/*   [37] 0x00000003  3 AttributeHeaders                               */
/*   [38] attrId=0 (r.id)                                              */
/*   [39] attrId=1 (r.area)                                            */
/*   [40] attrId=0xFFE8 (CORR_FACTOR64 pseudo-column)                  */
/*                                                                     */
/* Params for Node 1: QN_LookupParameters (+ filter):                  */
/*   [41] 0x00080001  type=QN_LOOKUP(1), paramLength=8                 */
/*   [42] requestInfo: PI_ATTR_INTERPRET                               */
/*   [43] resultData  (API receiver ID)                                */
/*   PI_ATTR_INTERPRET (filter for WHERE o.priority >= r.area):        */
/*   [44] 0x00000004  program_len=4                                    */
/*   [45] 0x0003301a  BRANCH_COL_LE_PARAM(cond=LE, nulls=CMP_EQUAL)   */
/*                      -> branch target=3 (EXIT_OK)                   */
/*   [46] 0x00010000  attrId=1 (o.priority), paramNo=0 (linked r.area)*/
/*   [47] 0x02720013  EXIT_REFUSE(errorCode=626) — reject row          */
/*   [48] 0x00000012  EXIT_OK — accept row                             */
/*        Note: NDB inequality branches are INVERTED —                 */
/*        branch_col_le_param branches when col >= param.              */
/*        So: if priority >= area → EXIT_OK; else → EXIT_REFUSE.       */
/*                                                                     */
/* Params for Node 2: QN_LookupParameters (agg leaf):                  */
/*   [49] 0x00050001  type=QN_LOOKUP(1), paramLength=5                 */
/*   [50] requestInfo: PI_ATTR_INTERPRET                               */
/*   [51] resultData  (API receiver ID)                                */
/*   PI_ATTR_INTERPRET (trivial program):                              */
/*   [52] 0x00000001  program_len=1                                    */
/*   [53] 0x00000012  EXIT_OK — accept all rows (agg is in Section 2)  */
/*                                                                     */
/* --- Section 2: Bounds + Aggregation (15 words) ---                  */
/*                                                                     */
/*   [0]  boundsLen=0 (full table scan, no index bounds)               */
/*   [1]  aggReceiverId (object map ID for aggregation results)        */
/*   [2..14] aggProgram (13 words):                                    */
/*     Header (8 words):                                               */
/*       [0] magic=0x0721, totalLen=13                                 */
/*       [1] n_gb_cols=2, n_agg_results=2                              */
/*       [2] version=PUSHDOWN_AGGREGATION_VERSION(2)                   */
/*       [3-7] reserved (0)                                            */
/*     GroupBy column descriptors (2 words):                           */
/*       [8] 0x80000007  GB col 0: (0x8000|pos 0)=LINKED r.area,      */
/*                        type=7 (INT)                                 */
/*       [9] 0x80010009  GB col 1: (0x8000|pos 1)=LINKED o.discount,  */
/*                        type=9 (BIGINT)                              */
/*     Instructions (3 words):                                         */
/*       [10] kOpLoadCol: type=BIGINT, reg=0, colId=1 (l.amount)      */
/*       [11] kOpSum:     agg[0]=SUM(reg0)                             */
/*       [12] kOpCount:   agg[1]=COUNT(*)                              */
/*                                                                     */
/* --- Data flow summary ---                                           */
/*                                                                     */
/*   Node 0: SCAN t4_region                                            */
/*     reads: {id, area, CORR_FACTOR64}                                */
/*     provides to children: col 0 (id), col 1 (area)                 */
/*       |                                                             */
/*   Node 1: LOOKUP t4_order — key: region_id = r.id                  */
/*     filter: priority >= r.area (linked param 0)                     */
/*     provides to children: col 0 (region_id), col 1 (discount)      */
/*       |                                                             */
/*   Node 2: LOOKUP t4_line — key: order_id = o.region_id             */
/*     AGGREGATE LEAF:                                                 */
/*       linked[0] = r.area  (via P_PARENT(1) + P_ATTRINFO(1))        */
/*       linked[1] = o.discount (via P_ATTRINFO(1))                   */
/*       GROUP BY linked[0] (INT), linked[1] (BIGINT)                 */
/*       SUM(l.amount), COUNT(*)                                       */
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
/* Test 5: MIN/MAX/SUM/COUNT with NULL salary column                   */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.dept_name, MIN(e.salary), MAX(e.salary),                 */
/*          SUM(e.salary), COUNT(e.salary)                             */
/*   FROM t5_dept d JOIN t5_emp e ON e.dept_id = d.id                  */
/*   GROUP BY d.dept_name                                              */
/* ------------------------------------------------------------------ */

static int
createTest5Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t5_emp");
  sqlExec(conn, "DROP TABLE IF EXISTS t5_dept");

  if (sqlExec(conn,
        "CREATE TABLE t5_dept ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  dept_name INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t5_emp ("
        "  dept_id INT NOT NULL PRIMARY KEY,"
        "  salary BIGINT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created t5_dept, t5_emp\n");
  return 0;
}

static int
insertTest5Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t5_dept VALUES "
        "(1,10),(2,10),(3,20),(4,20),(5,20)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t5_emp VALUES "
        "(1,5000),(2,NULL),(3,NULL),(4,NULL),(5,NULL)") != 0) return -1;

  V("Inserted t5_dept/t5_emp data\n");
  return 0;
}

static int
dropTest5Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t5_emp");
  sqlExec(conn, "DROP TABLE IF EXISTS t5_dept");
  V("Dropped t5_dept, t5_emp\n");
  return 0;
}

static int
testMinMaxWithNull(Ndb *ndb, MYSQL *conn)
{
  printf("Test 5: MIN/MAX/SUM/COUNT with NULL values ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T5_DEPT);
  dict->invalidateTable(T5_EMP);
  const NdbDictionary::Table *deptTab = dict->getTable(T5_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(T5_EMP);
  if (deptTab == nullptr || empTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *deptNameCol = deptTab->getColumn("dept_name");
  if (deptNameCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  NdbAggregator agg(empTab);
  if (!agg.GroupByLinked(0, deptNameCol) ||
      !agg.LoadColumn("salary", 0) ||
      !agg.Min(0, 0) ||
      !agg.Max(1, 0) ||
      !agg.Sum(2, 0) ||
      !agg.Count(3, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *deptOp = qb->scanTable(deptTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(deptOp, "id"), nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  const NdbLinkedOperand *deptNameLink = qb->linkedValue(deptOp, "dept_name");
  opts.addLinkedProjection(deptNameLink);

  const NdbQueryLookupOperationDef *empOp =
      qb->readTuple(empTab, joinKey, &opts);
  if (empOp == nullptr) {
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
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  struct NullVal { bool null_flag; Int64 value; };
  struct AggRow5 { NullVal min_v, max_v, sum_v; Int64 count; };
  std::map<Int32, AggRow5> actual;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 deptName = grpCol.data_int32();

    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxRes = rec.FetchAggregationResult();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result cntRes = rec.FetchAggregationResult();

    AggRow5 r;
    r.min_v = {minRes.is_null(), minRes.is_null() ? 0 : minRes.data_int64()};
    r.max_v = {maxRes.is_null(), maxRes.is_null() ? 0 : maxRes.data_int64()};
    r.sum_v = {sumRes.is_null(), sumRes.is_null() ? 0 : sumRes.data_int64()};
    r.count = cntRes.data_int64();
    actual[deptName] = r;

    if (r.min_v.null_flag) {
      V("  dept_name=%d MIN=NULL MAX=NULL SUM=NULL COUNT=%lld\n",
        deptName, (long long)r.count);
    } else {
      V("  dept_name=%d MIN=%lld MAX=%lld SUM=%lld COUNT=%lld\n",
        deptName, (long long)r.min_v.value, (long long)r.max_v.value,
        (long long)r.sum_v.value, (long long)r.count);
    }
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != 2) {
    printf("FAILED (expected 2 groups, got %zu)\n", actual.size());
    return -1;
  }

  /* Group 10: salary {5000, NULL} → MIN=5000, MAX=5000, SUM=5000, COUNT=1 */
  {
    auto it = actual.find(10);
    if (it == actual.end()) {
      printf("FAILED (missing group 10)\n");
      return -1;
    }
    const AggRow5 &r = it->second;
    if (r.min_v.null_flag || r.min_v.value != 5000 ||
        r.max_v.null_flag || r.max_v.value != 5000 ||
        r.sum_v.null_flag || r.sum_v.value != 5000 ||
        r.count != 1) {
      printf("FAILED (group 10 values wrong)\n");
      return -1;
    }
  }

  /* Group 20: all NULL → MIN=NULL, MAX=NULL, SUM=NULL, COUNT=0 */
  {
    auto it = actual.find(20);
    if (it == actual.end()) {
      printf("FAILED (missing group 20)\n");
      return -1;
    }
    const AggRow5 &r = it->second;
    if (!r.min_v.null_flag || !r.max_v.null_flag ||
        !r.sum_v.null_flag || r.count != 0) {
      printf("FAILED (group 20: expected all NULL with COUNT=0)\n");
      return -1;
    }
  }

  /* Cross-check NDB results against MySQL */
  {
    if (mysql_query(conn,
          "SELECT d.dept_name, MIN(e.salary), MAX(e.salary), "
          "SUM(e.salary), COUNT(e.salary) "
          "FROM t5_dept d JOIN t5_emp e ON e.dept_id = d.id "
          "GROUP BY d.dept_name ORDER BY d.dept_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      Int32 dn = atoi(row[0]);
      auto it = actual.find(dn);
      if (it == actual.end()) {
        printf("FAILED (MySQL cross-check: NDB missing group %d)\n", dn);
        mysql_free_result(result);
        return -1;
      }
      const AggRow5 &r = it->second;
      bool sqlMinNull = (row[1] == nullptr);
      bool sqlMaxNull = (row[2] == nullptr);
      bool sqlSumNull = (row[3] == nullptr);
      Int64 sqlCount = atoll(row[4]);
      if (r.min_v.null_flag != sqlMinNull ||
          r.max_v.null_flag != sqlMaxNull ||
          r.sum_v.null_flag != sqlSumNull ||
          r.count != sqlCount) {
        printf("FAILED (MySQL cross-check: group %d mismatch)\n", dn);
        mysql_free_result(result);
        return -1;
      }
      if (!sqlMinNull && r.min_v.value != atoll(row[1])) {
        printf("FAILED (MySQL cross-check: group %d MIN mismatch)\n", dn);
        mysql_free_result(result);
        return -1;
      }
      if (!sqlMaxNull && r.max_v.value != atoll(row[2])) {
        printf("FAILED (MySQL cross-check: group %d MAX mismatch)\n", dn);
        mysql_free_result(result);
        return -1;
      }
      if (!sqlSumNull && r.sum_v.value != atoll(row[3])) {
        printf("FAILED (MySQL cross-check: group %d SUM mismatch)\n", dn);
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (2 groups, NULL handling verified)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: CHAR GROUP BY with ordered index scan                       */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT c.cat_name, SUM(p.price)                                   */
/*   FROM t6_category c                                                */
/*   JOIN t6_product p ON p.category_id = c.id                         */
/*   WHERE c.cat_name >= 'A' AND c.cat_name <= 'Z'                    */
/*   GROUP BY c.cat_name                                               */
/* ------------------------------------------------------------------ */

static int
createTest6Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t6_product");
  sqlExec(conn, "DROP TABLE IF EXISTS t6_category");

  if (sqlExec(conn,
        "CREATE TABLE t6_category ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  cat_name CHAR(20) NOT NULL,"
        "  INDEX idx_cat_name (cat_name)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t6_product ("
        "  category_id INT NOT NULL PRIMARY KEY,"
        "  price BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created t6_category, t6_product + idx_cat_name\n");
  return 0;
}

static int
insertTest6Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t6_category VALUES "
        "(1,'Electronics'),(2,'Electronics'),"
        "(3,'Books'),(4,'Books'),(5,'Toys')") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t6_product VALUES "
        "(1,500),(2,300),(3,50),(4,75),(5,200)") != 0) return -1;

  V("Inserted t6_category/t6_product data\n");
  return 0;
}

static int
dropTest6Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t6_product");
  sqlExec(conn, "DROP TABLE IF EXISTS t6_category");
  V("Dropped t6_category, t6_product\n");
  return 0;
}

static int
testCharGroupByWithIndex(Ndb *ndb, MYSQL *conn)
{
  printf("Test 6: CHAR GROUP BY with index scan ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->removeCachedIndex("idx_cat_name", T6_CATEGORY);
  dict->removeCachedTable(T6_CATEGORY);
  dict->removeCachedTable(T6_PRODUCT);
  const NdbDictionary::Table *catTab = dict->getTable(T6_CATEGORY);
  const NdbDictionary::Table *prodTab = dict->getTable(T6_PRODUCT);
  const NdbDictionary::Index *catIdx =
      dict->getIndex("idx_cat_name", T6_CATEGORY);
  if (catTab == nullptr || prodTab == nullptr || catIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }
  V("\n  catTab: id=%d ver=%d  prodTab: id=%d ver=%d\n",
    catTab->getObjectId(), catTab->getObjectVersion(),
    prodTab->getObjectId(), prodTab->getObjectVersion());
  V("  catIdx: id=%d ver=%d\n",
    catIdx->getObjectId(), catIdx->getObjectVersion());

  const NdbDictionary::Column *catNameCol = catTab->getColumn("cat_name");
  if (catNameCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  NdbAggregator agg(prodTab);
  if (!agg.GroupByLinked(0, catNameCol) ||
      !agg.LoadColumn("price", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Warm-up: simple index scan to force NDB API schema version sync
   * with data nodes before building the pushed query. */
  {
    NdbTransaction *warmup = ndb->startTransaction();
    if (warmup != nullptr) {
      NdbIndexScanOperation *sop =
          warmup->getNdbIndexScanOperation(catIdx, catTab);
      if (sop != nullptr) {
        sop->readTuples(NdbOperation::LM_CommittedRead);
        if (warmup->execute(NdbTransaction::NoCommit) == 0) {
          sop->nextResult(true);
        }
      }
      warmup->close();
    }
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Index bound: cat_name >= 'A' AND cat_name <= 'Z'
   * Use constValue(const char*) which auto-pads to column byte size */
  const NdbQueryOperand *lowKey[] = {
    qb->constValue("A"), nullptr
  };
  const NdbQueryOperand *highKey[] = {
    qb->constValue("Z"), nullptr
  };
  NdbQueryIndexBound bound(lowKey, true, highKey, true);

  const NdbQueryIndexScanOperationDef *catOp =
      qb->scanIndex(catIdx, catTab, &bound);
  if (catOp == nullptr) {
    printf("FAILED (scanIndex: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(catOp, "id"), nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  const NdbLinkedOperand *catNameLink = qb->linkedValue(catOp, "cat_name");
  opts.addLinkedProjection(catNameLink);

  const NdbQueryLookupOperationDef *prodOp =
      qb->readTuple(prodTab, joinKey, &opts);
  if (prodOp == nullptr) {
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
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  std::map<std::string, Int64> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    int nameLen = (int)nameCol.byte_size();
    const char *namePtr = nameCol.data();
    /* Trim trailing spaces and find effective string length */
    int effLen = (int)strnlen(namePtr, nameLen);
    while (effLen > 0 && namePtr[effLen - 1] == ' ')
      effLen--;
    std::string name(namePtr, effLen);

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[name] = sumVal;
    V("  cat_name='%s' SUM(price)=%lld\n", name.c_str(), (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Expected: Books=125, Electronics=800, Toys=200 */
  std::map<std::string, Int64> expected = {
    {"Books", 125}, {"Electronics", 800}, {"Toys", 200}
  };

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("FAILED (missing group '%s')\n", e.first.c_str());
      return -1;
    }
    if (it->second != e.second) {
      printf("FAILED (group '%s': expected SUM=%lld, got %lld)\n",
             e.first.c_str(), (long long)e.second, (long long)it->second);
      return -1;
    }
  }

  /* Cross-check NDB results against MySQL */
  {
    if (mysql_query(conn,
          "SELECT c.cat_name, SUM(p.price) "
          "FROM t6_category c "
          "JOIN t6_product p ON p.category_id = c.id "
          "WHERE c.cat_name >= 'A' AND c.cat_name <= 'Z' "
          "GROUP BY c.cat_name ORDER BY c.cat_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      Int64 sqlSum = atoll(row[1]);
      auto it = actual.find(name);
      if (it == actual.end() || it->second != sqlSum) {
        printf("FAILED (MySQL cross-check: group '%s' mismatch)\n",
               name.c_str());
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (3 groups, CHAR GROUP BY with index scan)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 7: 4-way join with composite key on leaf                       */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT co.name, ci.region, st.size, SUM(sa.amount)                */
/*   FROM t7_country co                                                */
/*   JOIN t7_city ci ON ci.country_id = co.id                          */
/*   JOIN t7_store st ON st.city_id = ci.country_id                    */
/*   JOIN t7_sale sa ON sa.store_id = st.city_id                       */
/*                  AND sa.item_id = ci.region                         */
/*   GROUP BY co.name, ci.region, st.size                              */
/* ------------------------------------------------------------------ */

static int
createTest7Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t7_sale");
  sqlExec(conn, "DROP TABLE IF EXISTS t7_store");
  sqlExec(conn, "DROP TABLE IF EXISTS t7_city");
  sqlExec(conn, "DROP TABLE IF EXISTS t7_country");

  if (sqlExec(conn,
        "CREATE TABLE t7_country ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  name INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t7_city ("
        "  country_id INT NOT NULL PRIMARY KEY,"
        "  region INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t7_store ("
        "  city_id INT NOT NULL PRIMARY KEY,"
        "  size INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t7_sale ("
        "  store_id INT NOT NULL,"
        "  item_id INT NOT NULL,"
        "  amount BIGINT NOT NULL,"
        "  PRIMARY KEY (store_id, item_id)"
        ") ENGINE=NDB") != 0) return -1;

  V("Created t7_country, t7_city, t7_store, t7_sale\n");
  return 0;
}

static int
insertTest7Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t7_country VALUES (1,100),(2,200)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t7_city VALUES (1,10),(2,20)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t7_store VALUES (1,5),(2,8)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t7_sale VALUES (1,10,1000),(2,20,2000)") != 0) return -1;

  V("Inserted t7_country/t7_city/t7_store/t7_sale data\n");
  return 0;
}

static int
dropTest7Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t7_sale");
  sqlExec(conn, "DROP TABLE IF EXISTS t7_store");
  sqlExec(conn, "DROP TABLE IF EXISTS t7_city");
  sqlExec(conn, "DROP TABLE IF EXISTS t7_country");
  V("Dropped t7_country, t7_city, t7_store, t7_sale\n");
  return 0;
}

static int
testFourWayCompositeKey(Ndb *ndb, MYSQL *conn)
{
  printf("Test 7: 4-way join with composite key ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T7_COUNTRY);
  dict->invalidateTable(T7_CITY);
  dict->invalidateTable(T7_STORE);
  dict->invalidateTable(T7_SALE);
  const NdbDictionary::Table *countryTab = dict->getTable(T7_COUNTRY);
  const NdbDictionary::Table *cityTab = dict->getTable(T7_CITY);
  const NdbDictionary::Table *storeTab = dict->getTable(T7_STORE);
  const NdbDictionary::Table *saleTab = dict->getTable(T7_SALE);
  if (countryTab == nullptr || cityTab == nullptr ||
      storeTab == nullptr || saleTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *nameCol = countryTab->getColumn("name");
  const NdbDictionary::Column *regionCol = cityTab->getColumn("region");
  const NdbDictionary::Column *sizeCol = storeTab->getColumn("size");
  if (nameCol == nullptr || regionCol == nullptr || sizeCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  /* Aggregation on sale (leaf): GROUP BY name, region, size; SUM(amount) */
  NdbAggregator agg(saleTab);
  if (!agg.GroupByLinked(0, nameCol) ||
      !agg.GroupByLinked(1, regionCol) ||
      !agg.GroupByLinked(2, sizeCol) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Node 0: scan t7_country */
  const NdbQueryTableScanOperationDef *countryOp = qb->scanTable(countryTab);
  if (countryOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup t7_city (city.country_id = country.id) */
  const NdbQueryOperand *cityJoinKey[] = {
    qb->linkedValue(countryOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *cityOp =
      qb->readTuple(cityTab, cityJoinKey);
  if (cityOp == nullptr) {
    printf("FAILED (readTuple city: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup t7_store (store.city_id = city.country_id) */
  const NdbQueryOperand *storeJoinKey[] = {
    qb->linkedValue(cityOp, "country_id"), nullptr
  };
  const NdbQueryLookupOperationDef *storeOp =
      qb->readTuple(storeTab, storeJoinKey);
  if (storeOp == nullptr) {
    printf("FAILED (readTuple store: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup t7_sale (composite key: store_id, item_id)
   * sale.store_id = store.city_id, sale.item_id = city.region */
  const NdbQueryOperand *saleJoinKey[] = {
    qb->linkedValue(storeOp, "city_id"),
    qb->linkedValue(cityOp, "region"),
    nullptr
  };

  NdbQueryOptions saleOpts;
  saleOpts.setAggregation(agg);

  const NdbLinkedOperand *nameLink = qb->linkedValue(countryOp, "name");
  const NdbLinkedOperand *regionLink = qb->linkedValue(cityOp, "region");
  const NdbLinkedOperand *sizeLink = qb->linkedValue(storeOp, "size");
  if (nameLink == nullptr || regionLink == nullptr || sizeLink == nullptr) {
    printf("FAILED (linkedValue for projection: %s)\n",
           qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  saleOpts.addLinkedProjection(nameLink);
  saleOpts.addLinkedProjection(regionLink);
  saleOpts.addLinkedProjection(sizeLink);

  const NdbQueryLookupOperationDef *saleOp =
      qb->readTuple(saleTab, saleJoinKey, &saleOpts);
  if (saleOp == nullptr) {
    printf("FAILED (readTuple sale: %s)\n", qb->getNdbError().message);
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

  V("\n  Query prepared: %u operations\n", queryDef->getNoOfOperations());

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
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  struct ResultRow7 { Int32 region; Int32 size; Int64 sum_amount; };
  std::map<Int32, ResultRow7> actual;  /* keyed on name */

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column nameGrp = rec.FetchGroupbyColumn();
    Int32 nameVal = nameGrp.data_int32();

    NdbAggregator::Column regionGrp = rec.FetchGroupbyColumn();
    Int32 regionVal = regionGrp.data_int32();

    NdbAggregator::Column sizeGrp = rec.FetchGroupbyColumn();
    Int32 sizeVal = sizeGrp.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[nameVal] = {regionVal, sizeVal, sumVal};
    V("  name=%d region=%d size=%d SUM=%lld\n",
      nameVal, regionVal, sizeVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Expected: (100,10,5,1000), (200,20,8,2000) */
  if (actual.size() != 2) {
    printf("FAILED (expected 2 groups, got %zu)\n", actual.size());
    return -1;
  }

  struct Exp7 { Int32 name; Int32 region; Int32 size; Int64 sum; };
  Exp7 exp[] = {{100, 10, 5, 1000}, {200, 20, 8, 2000}};
  for (int i = 0; i < 2; i++) {
    auto it = actual.find(exp[i].name);
    if (it == actual.end()) {
      printf("FAILED (missing name=%d)\n", exp[i].name);
      return -1;
    }
    if (it->second.region != exp[i].region ||
        it->second.size != exp[i].size ||
        it->second.sum_amount != exp[i].sum) {
      printf("FAILED (name=%d: expected region=%d size=%d sum=%lld, "
             "got region=%d size=%d sum=%lld)\n",
             exp[i].name, exp[i].region, exp[i].size, (long long)exp[i].sum,
             it->second.region, it->second.size,
             (long long)it->second.sum_amount);
      return -1;
    }
  }

  /* Cross-check NDB results against MySQL */
  {
    if (mysql_query(conn,
          "SELECT co.name, ci.region, st.size, SUM(sa.amount) "
          "FROM t7_country co "
          "JOIN t7_city ci ON ci.country_id = co.id "
          "JOIN t7_store st ON st.city_id = ci.country_id "
          "JOIN t7_sale sa ON sa.store_id = st.city_id "
          "AND sa.item_id = ci.region "
          "GROUP BY co.name, ci.region, st.size "
          "ORDER BY co.name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      Int32 name = atoi(row[0]);
      Int32 region = atoi(row[1]);
      Int32 size = atoi(row[2]);
      Int64 sum = atoll(row[3]);
      auto it = actual.find(name);
      if (it == actual.end()) {
        printf("FAILED (MySQL cross-check: NDB missing name=%d)\n", name);
        mysql_free_result(result);
        return -1;
      }
      if (it->second.region != region || it->second.size != size ||
          it->second.sum_amount != sum) {
        printf("FAILED (MySQL cross-check: name=%d mismatch)\n", name);
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (2 groups, 4-way join with composite key)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 8: Arithmetic expression (Mul + Minus + LoadLinkedColumn)       */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT o.grp, SUM(i.qty * o.unit_price - i.discount)              */
/*   FROM t8_order o JOIN t8_item i ON i.order_id = o.id               */
/*   GROUP BY o.grp                                                    */
/* ------------------------------------------------------------------ */

static int
createTest8Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t8_item");
  sqlExec(conn, "DROP TABLE IF EXISTS t8_order");

  if (sqlExec(conn,
        "CREATE TABLE t8_order ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL,"
        "  unit_price BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t8_item ("
        "  order_id INT NOT NULL PRIMARY KEY,"
        "  qty BIGINT NOT NULL,"
        "  discount BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created t8_order, t8_item\n");
  return 0;
}

static int
insertTest8Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t8_order VALUES "
        "(1,1,100),(2,1,200),(3,2,300),(4,2,50)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t8_item VALUES "
        "(1,10,50),(2,5,25),(3,3,100),(4,8,30)") != 0) return -1;

  V("Inserted t8_order/t8_item data\n");
  return 0;
}

static int
dropTest8Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t8_item");
  sqlExec(conn, "DROP TABLE IF EXISTS t8_order");
  V("Dropped t8_order, t8_item\n");
  return 0;
}

static int
testArithmeticExpression(Ndb *ndb, MYSQL *conn)
{
  printf("Test 8: Arithmetic expression (Mul + Minus) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T8_ORDER);
  dict->invalidateTable(T8_ITEM);
  const NdbDictionary::Table *orderTab = dict->getTable(T8_ORDER);
  const NdbDictionary::Table *itemTab = dict->getTable(T8_ITEM);
  if (orderTab == nullptr || itemTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = orderTab->getColumn("grp");
  const NdbDictionary::Column *unitPriceCol = orderTab->getColumn("unit_price");
  if (grpCol == nullptr || unitPriceCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  /* Agg program:
   *   GroupByLinked(0) = order.grp
   *   reg0 = item.qty (local)
   *   reg1 = order.unit_price (linked pos 1)
   *   reg0 = qty * unit_price
   *   reg2 = item.discount (local)
   *   reg0 = reg0 - discount
   *   agg[0] = SUM(reg0)
   */
  NdbAggregator agg(itemTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadColumn("qty", 0) ||
      !agg.LoadLinkedColumn(1, 1, unitPriceCol) ||
      !agg.Mul(0, 1) ||
      !agg.LoadColumn("discount", 2) ||
      !agg.Minus(0, 2) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *orderOp = qb->scanTable(orderTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(orderOp, "id"), nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(orderOp, "grp");
  const NdbLinkedOperand *priceLink = qb->linkedValue(orderOp, "unit_price");
  opts.addLinkedProjection(grpLink);
  opts.addLinkedProjection(priceLink);

  const NdbQueryLookupOperationDef *itemOp =
      qb->readTuple(itemTab, joinKey, &opts);
  if (itemOp == nullptr) {
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
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  std::map<Int32, Int64> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grp = rec.FetchGroupbyColumn();
    Int32 grpVal = grp.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = sumVal;
    V("\n  grp=%d SUM(qty*price-disc)=%lld", grpVal, (long long)sumVal);
  }
  V("\n");

  query->close();
  trans->close();
  queryDef->destroy();

  std::map<Int32, Int64> expected = {{1, 1925}, {2, 1170}};
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
      printf("FAILED (group %d: expected %lld, got %lld)\n",
             e.first, (long long)e.second, (long long)it->second);
      return -1;
    }
  }

  /* Cross-check NDB results against MySQL */
  if (verifyWithMysql(conn, "Test 8",
        "SELECT o.grp, SUM(i.qty * o.unit_price - i.discount) "
        "FROM t8_order o JOIN t8_item i ON i.order_id = o.id "
        "GROUP BY o.grp ORDER BY o.grp", actual) != 0) {
    printf("FAILED (MySQL cross-check)\n");
    return -1;
  }

  printf("OK (2 groups, arithmetic expression)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 9: Empty result (filter rejects all child rows)                */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT grp, SUM(amount)                                           */
/*   FROM jagg_parent JOIN jagg_child                                  */
/*   ON jagg_child.parent_id = jagg_parent.id                          */
/*   WHERE jagg_child.amount < 0                                       */
/*   GROUP BY grp                                                      */
/*   (returns empty set)                                               */
/* ------------------------------------------------------------------ */

static int
testEmptyResult(Ndb *ndb, MYSQL *conn)
{
  printf("Test 9: Empty result (filter rejects all) ... ");
  fflush(stdout);

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *parentTab = dict->getTable(PARENT_TABLE);
  const NdbDictionary::Table *childTab = dict->getTable(CHILD_TABLE);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  const NdbDictionary::Column *grpCol = parentTab->getColumn("grp");
  if (grpCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Interpreted code that unconditionally rejects all rows */
  NdbInterpretedCode rejectAll(childTab);
  rejectAll.interpret_exit_nok();
  if (rejectAll.finalise() != 0) {
    printf("FAILED (NdbInterpretedCode finalise)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"), nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  opts.setInterpretedCode(rejectAll);
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

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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
  if (!rec.end()) {
    printf("FAILED (expected 0 groups, but FetchResultRecord returned data)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Cross-check: MySQL with WHERE 0=1 should also return 0 rows */
  {
    if (mysql_query(conn,
          "SELECT grp, SUM(amount) "
          "FROM jagg_parent JOIN jagg_child "
          "ON jagg_child.parent_id = jagg_parent.id "
          "WHERE 0 = 1 "
          "GROUP BY grp") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    Uint32 rowCount = (Uint32)mysql_num_rows(result);
    mysql_free_result(result);
    if (rowCount != 0) {
      printf("FAILED (MySQL cross-check: expected 0 rows, got %u)\n", rowCount);
      return -1;
    }
    V("  MySQL cross-check OK (0 rows)\n");
  }

  printf("OK (0 groups, empty result)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 10: High cardinality GROUP BY (20 groups, 1 row each)          */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT p.grp, SUM(c.val), MIN(c.val), MAX(c.val)                 */
/*   FROM t10_parent p JOIN t10_child c ON c.parent_id = p.id          */
/*   GROUP BY p.grp                                                    */
/* ------------------------------------------------------------------ */

static int
createTest10Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t10_child");
  sqlExec(conn, "DROP TABLE IF EXISTS t10_parent");

  if (sqlExec(conn,
        "CREATE TABLE t10_parent ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t10_child ("
        "  parent_id INT NOT NULL PRIMARY KEY,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created t10_parent, t10_child\n");
  return 0;
}

static int
insertTest10Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t10_parent VALUES "
        "(1,1),(2,2),(3,3),(4,4),(5,5),"
        "(6,6),(7,7),(8,8),(9,9),(10,10),"
        "(11,11),(12,12),(13,13),(14,14),(15,15),"
        "(16,16),(17,17),(18,18),(19,19),(20,20)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t10_child VALUES "
        "(1,100),(2,200),(3,300),(4,400),(5,500),"
        "(6,600),(7,700),(8,800),(9,900),(10,1000),"
        "(11,1100),(12,1200),(13,1300),(14,1400),(15,1500),"
        "(16,1600),(17,1700),(18,1800),(19,1900),(20,2000)") != 0) return -1;

  V("Inserted t10_parent/t10_child data (20 rows)\n");
  return 0;
}

static int
dropTest10Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t10_child");
  sqlExec(conn, "DROP TABLE IF EXISTS t10_parent");
  V("Dropped t10_parent, t10_child\n");
  return 0;
}

static int
testHighCardinalityGroupBy(Ndb *ndb, MYSQL *conn)
{
  printf("Test 10: High cardinality GROUP BY (20 groups) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T10_PARENT);
  dict->invalidateTable(T10_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(T10_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(T10_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = parentTab->getColumn("grp");
  if (grpCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadColumn("val", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Min(1, 0) ||
      !agg.Max(2, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"), nullptr
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

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  struct AggRow10 { Int64 sum; Int64 min; Int64 max; };
  std::map<Int32, AggRow10> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grp = rec.FetchGroupbyColumn();
    Int32 grpVal = grp.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxRes = rec.FetchAggregationResult();

    actual[grpVal] = {sumRes.data_int64(), minRes.data_int64(),
                      maxRes.data_int64()};
    V("  grp=%d SUM=%lld MIN=%lld MAX=%lld\n", grpVal,
      (long long)sumRes.data_int64(), (long long)minRes.data_int64(),
      (long long)maxRes.data_int64());
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != 20) {
    printf("FAILED (expected 20 groups, got %zu)\n", actual.size());
    return -1;
  }

  /* Each group N has exactly 1 row with val = N*100 → SUM=MIN=MAX */
  for (Int32 n = 1; n <= 20; n++) {
    auto it = actual.find(n);
    if (it == actual.end()) {
      printf("FAILED (missing group %d)\n", n);
      return -1;
    }
    Int64 expected = (Int64)n * 100;
    if (it->second.sum != expected || it->second.min != expected ||
        it->second.max != expected) {
      printf("FAILED (group %d: expected SUM=MIN=MAX=%lld, "
             "got SUM=%lld MIN=%lld MAX=%lld)\n",
             n, (long long)expected,
             (long long)it->second.sum, (long long)it->second.min,
             (long long)it->second.max);
      return -1;
    }
  }

  /* Cross-check NDB results against MySQL */
  {
    if (mysql_query(conn,
          "SELECT p.grp, SUM(c.val), MIN(c.val), MAX(c.val) "
          "FROM t10_parent p JOIN t10_child c ON c.parent_id = p.id "
          "GROUP BY p.grp ORDER BY p.grp") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    Uint32 mysqlRows = 0;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      Int32 grp = atoi(row[0]);
      Int64 sum = atoll(row[1]);
      Int64 min = atoll(row[2]);
      Int64 max = atoll(row[3]);
      auto it = actual.find(grp);
      if (it == actual.end()) {
        printf("FAILED (MySQL cross-check: NDB missing grp=%d)\n", grp);
        mysql_free_result(result);
        return -1;
      }
      if (it->second.sum != sum || it->second.min != min ||
          it->second.max != max) {
        printf("FAILED (MySQL cross-check: grp=%d mismatch)\n", grp);
        mysql_free_result(result);
        return -1;
      }
      mysqlRows++;
    }
    mysql_free_result(result);
    if (mysqlRows != actual.size()) {
      printf("FAILED (MySQL cross-check: %u rows vs %zu NDB groups)\n",
             mysqlRows, actual.size());
      return -1;
    }
    V("  MySQL cross-check OK\n");
  }

  printf("OK (20 groups, SUM=MIN=MAX verified)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 11: Global aggregation on 3-way join (no GROUP BY)             */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT COUNT(*), SUM(l.amount), MIN(l.amount), MAX(l.amount)      */
/*   FROM t4_region r                                                  */
/*   JOIN t4_order o ON o.region_id = r.id                             */
/*   JOIN t4_line l ON l.order_id = o.region_id                        */
/*   WHERE o.priority >= r.area                                        */
/* ------------------------------------------------------------------ */

static int
testGlobalAggThreeWay(Ndb *ndb, MYSQL *conn)
{
  printf("Test 11: Global aggregation on 3-way join ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *regionTab = dict->getTable(T4_REGION);
  const NdbDictionary::Table *orderTab = dict->getTable(T4_ORDER);
  const NdbDictionary::Table *lineTab = dict->getTable(T4_LINE);
  if (regionTab == nullptr || orderTab == nullptr || lineTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *priCol = orderTab->getColumn("priority");
  if (priCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  /* Aggregation: COUNT, SUM, MIN, MAX on line.amount — no GROUP BY */
  NdbAggregator agg(lineTab);
  if (!agg.LoadColumn("amount", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Min(2, 0) ||
      !agg.Max(3, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Interpreted code for t4_order: priority >= area.
   * branch_col_le_param is INVERTED: branches when col >= param. */
  Uint32 priorityAttrId = priCol->getColumnNo();
  NdbInterpretedCode code(orderTab);
  code.branch_col_le_param(priorityAttrId, 0, 0);
  code.interpret_exit_nok();
  code.def_label(0);
  code.interpret_exit_ok();
  if (code.finalise() != 0) {
    printf("FAILED (NdbInterpretedCode finalise)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Node 0: scan t4_region */
  const NdbQueryTableScanOperationDef *regionOp = qb->scanTable(regionTab);

  /* Node 1: lookup t4_order with filter */
  const NdbQueryOperand *orderJoinKey[] = {
    qb->linkedValue(regionOp, "id"), nullptr
  };
  NdbQueryOptions orderOpts;
  const NdbQueryOperand *filterParams[] = {
    qb->linkedValue(regionOp, "area"), nullptr
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

  /* Node 2: lookup t4_line with aggregation — NO linked projections */
  const NdbQueryOperand *lineJoinKey[] = {
    qb->linkedValue(orderOp, "region_id"), nullptr
  };
  NdbQueryOptions lineOpts;
  lineOpts.setAggregation(agg);

  const NdbQueryLookupOperationDef *lineOp =
      qb->readTuple(lineTab, lineJoinKey, &lineOpts);
  if (lineOp == nullptr) {
    printf("FAILED (readTuple line: %s)\n", qb->getNdbError().message);
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
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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
    printf("FAILED (no result record for global aggregation)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result cntRes = rec.FetchAggregationResult();
  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  NdbAggregator::Result minRes = rec.FetchAggregationResult();
  NdbAggregator::Result maxRes = rec.FetchAggregationResult();

  Int64 cnt = cntRes.data_int64();
  Int64 sum = sumRes.data_int64();
  Int64 minV = minRes.data_int64();
  Int64 maxV = maxRes.data_int64();

  V("\n  COUNT=%lld SUM=%lld MIN=%lld MAX=%lld\n",
    (long long)cnt, (long long)sum, (long long)minV, (long long)maxV);

  /* Verify no more records */
  NdbAggregator::ResultRecord rec2 = resultAgg->FetchResultRecord();
  if (!rec2.end()) {
    printf("FAILED (expected 1 record, got more)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (cnt != 3 || sum != 900 || minV != 100 || maxV != 500) {
    printf("FAILED (expected COUNT=3 SUM=900 MIN=100 MAX=500, "
           "got COUNT=%lld SUM=%lld MIN=%lld MAX=%lld)\n",
           (long long)cnt, (long long)sum, (long long)minV, (long long)maxV);
    return -1;
  }

  /* Cross-check NDB results against MySQL */
  if (verifyScalarWithMysql(conn, "Test 11",
        "SELECT COUNT(*), SUM(l.amount), MIN(l.amount), MAX(l.amount) "
        "FROM t4_region r "
        "JOIN t4_order o ON o.region_id = r.id "
        "JOIN t4_line l ON l.order_id = o.region_id "
        "WHERE o.priority >= r.area",
        {cnt, sum, minV, maxV}) != 0) {
    printf("FAILED (MySQL cross-check)\n");
    return -1;
  }

  printf("OK (COUNT=3 SUM=900 MIN=100 MAX=500)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 12: All-NULL aggregation column                                */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT p.grp, SUM(c.val), MIN(c.val), MAX(c.val), COUNT(c.val)   */
/*   FROM t12_parent p JOIN t12_child c ON c.parent_id = p.id          */
/*   GROUP BY p.grp                                                    */
/* ------------------------------------------------------------------ */

static int
createTest12Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t12_child");
  sqlExec(conn, "DROP TABLE IF EXISTS t12_parent");

  if (sqlExec(conn,
        "CREATE TABLE t12_parent ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE t12_child ("
        "  parent_id INT NOT NULL PRIMARY KEY,"
        "  val BIGINT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created t12_parent, t12_child\n");
  return 0;
}

static int
insertTest12Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO t12_parent VALUES "
        "(1,1),(2,1),(3,2),(4,2)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO t12_child VALUES "
        "(1,NULL),(2,NULL),(3,100),(4,200)") != 0) return -1;

  V("Inserted t12_parent/t12_child data\n");
  return 0;
}

static int
dropTest12Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS t12_child");
  sqlExec(conn, "DROP TABLE IF EXISTS t12_parent");
  V("Dropped t12_parent, t12_child\n");
  return 0;
}

static int
testAllNullAggColumn(Ndb *ndb, MYSQL *conn)
{
  printf("Test 12: All-NULL aggregation column ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(T12_PARENT);
  dict->invalidateTable(T12_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(T12_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(T12_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = parentTab->getColumn("grp");
  if (grpCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadColumn("val", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Min(1, 0) ||
      !agg.Max(2, 0) ||
      !agg.Count(3, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "id"), nullptr
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

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  struct NullVal { bool null_flag; Int64 value; };
  struct AggRow12 { NullVal sum_v, min_v, max_v; Int64 count; };
  std::map<Int32, AggRow12> actual;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grp = rec.FetchGroupbyColumn();
    Int32 grpVal = grp.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxRes = rec.FetchAggregationResult();
    NdbAggregator::Result cntRes = rec.FetchAggregationResult();

    AggRow12 r;
    r.sum_v = {sumRes.is_null(), sumRes.is_null() ? 0 : sumRes.data_int64()};
    r.min_v = {minRes.is_null(), minRes.is_null() ? 0 : minRes.data_int64()};
    r.max_v = {maxRes.is_null(), maxRes.is_null() ? 0 : maxRes.data_int64()};
    r.count = cntRes.data_int64();
    actual[grpVal] = r;

    if (r.sum_v.null_flag) {
      V("  grp=%d SUM=NULL MIN=NULL MAX=NULL COUNT=%lld\n",
        grpVal, (long long)r.count);
    } else {
      V("  grp=%d SUM=%lld MIN=%lld MAX=%lld COUNT=%lld\n",
        grpVal, (long long)r.sum_v.value, (long long)r.min_v.value,
        (long long)r.max_v.value, (long long)r.count);
    }
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != 2) {
    printf("FAILED (expected 2 groups, got %zu)\n", actual.size());
    return -1;
  }

  /* Group 1: val = {NULL, NULL} → all NULL, COUNT=0 */
  {
    auto it = actual.find(1);
    if (it == actual.end()) {
      printf("FAILED (missing group 1)\n");
      return -1;
    }
    const AggRow12 &r = it->second;
    if (!r.sum_v.null_flag || !r.min_v.null_flag ||
        !r.max_v.null_flag || r.count != 0) {
      printf("FAILED (group 1: expected all NULL with COUNT=0)\n");
      return -1;
    }
  }

  /* Group 2: val = {100, 200} → SUM=300, MIN=100, MAX=200, COUNT=2 */
  {
    auto it = actual.find(2);
    if (it == actual.end()) {
      printf("FAILED (missing group 2)\n");
      return -1;
    }
    const AggRow12 &r = it->second;
    if (r.sum_v.null_flag || r.sum_v.value != 300 ||
        r.min_v.null_flag || r.min_v.value != 100 ||
        r.max_v.null_flag || r.max_v.value != 200 ||
        r.count != 2) {
      printf("FAILED (group 2: expected SUM=300 MIN=100 MAX=200 COUNT=2)\n");
      return -1;
    }
  }

  /* Cross-check NDB results against MySQL */
  {
    if (mysql_query(conn,
          "SELECT p.grp, SUM(c.val), MIN(c.val), MAX(c.val), COUNT(c.val) "
          "FROM t12_parent p JOIN t12_child c ON c.parent_id = p.id "
          "GROUP BY p.grp ORDER BY p.grp") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      Int32 grp = atoi(row[0]);
      auto it = actual.find(grp);
      if (it == actual.end()) {
        printf("FAILED (MySQL cross-check: NDB missing grp=%d)\n", grp);
        mysql_free_result(result);
        return -1;
      }
      const AggRow12 &r = it->second;
      bool sqlSumNull = (row[1] == nullptr);
      bool sqlMinNull = (row[2] == nullptr);
      bool sqlMaxNull = (row[3] == nullptr);
      Int64 sqlCount = atoll(row[4]);
      if (r.sum_v.null_flag != sqlSumNull ||
          r.min_v.null_flag != sqlMinNull ||
          r.max_v.null_flag != sqlMaxNull ||
          r.count != sqlCount) {
        printf("FAILED (MySQL cross-check: grp=%d null/count mismatch)\n", grp);
        mysql_free_result(result);
        return -1;
      }
      if (!sqlSumNull && r.sum_v.value != atoll(row[1])) {
        printf("FAILED (MySQL cross-check: grp=%d SUM mismatch)\n", grp);
        mysql_free_result(result);
        return -1;
      }
      if (!sqlMinNull && r.min_v.value != atoll(row[2])) {
        printf("FAILED (MySQL cross-check: grp=%d MIN mismatch)\n", grp);
        mysql_free_result(result);
        return -1;
      }
      if (!sqlMaxNull && r.max_v.value != atoll(row[3])) {
        printf("FAILED (MySQL cross-check: grp=%d MAX mismatch)\n", grp);
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (2 groups, all-NULL vs non-NULL verified)\n");
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

          /* Test 9: Empty result (filter rejects all child rows) */
          if (testEmptyResult(&ndb, conn) != 0) {
            exitCode = 1;
          }
        }
        dropTestTables(conn);

        /* Test 4: 3-way join with linked param filter */
        if (createTest4Tables(conn) == 0 &&
            insertTest4Data(conn) == 0) {
          if (testThreeWayJoin(&ndb, conn) != 0) {
            exitCode = 1;
          }

          /* Test 11: Global aggregation on 3-way join (reuses t4 tables) */
          if (testGlobalAggThreeWay(&ndb, conn) != 0) {
            exitCode = 1;
          }
        } else {
          exitCode = 1;
        }
        dropTest4Tables(conn);

        /* Test 5: MIN/MAX/SUM/COUNT with NULL values */
        if (createTest5Tables(conn) == 0 && insertTest5Data(conn) == 0) {
          if (testMinMaxWithNull(&ndb, conn) != 0) exitCode = 1;
        } else {
          exitCode = 1;
        }
        dropTest5Tables(conn);

        /* Test 6: CHAR GROUP BY with ordered index scan */
        if (createTest6Tables(conn) == 0 && insertTest6Data(conn) == 0) {
          if (testCharGroupByWithIndex(&ndb, conn) != 0) exitCode = 1;
        } else {
          exitCode = 1;
        }
        dropTest6Tables(conn);

        /* Test 7: 4-way join with composite key */
        if (createTest7Tables(conn) == 0 && insertTest7Data(conn) == 0) {
          if (testFourWayCompositeKey(&ndb, conn) != 0) exitCode = 1;
        } else {
          exitCode = 1;
        }
        dropTest7Tables(conn);

        /* Test 8: Arithmetic expression (Mul + Minus) */
        if (createTest8Tables(conn) == 0 && insertTest8Data(conn) == 0) {
          if (testArithmeticExpression(&ndb, conn) != 0) exitCode = 1;
        } else {
          exitCode = 1;
        }
        dropTest8Tables(conn);

        /* Test 10: High cardinality GROUP BY (20 groups) */
        if (createTest10Tables(conn) == 0 && insertTest10Data(conn) == 0) {
          if (testHighCardinalityGroupBy(&ndb, conn) != 0) exitCode = 1;
        } else {
          exitCode = 1;
        }
        dropTest10Tables(conn);

        /* Test 12: All-NULL aggregation column */
        if (createTest12Tables(conn) == 0 && insertTest12Data(conn) == 0) {
          if (testAllNullAggColumn(&ndb, conn) != 0) exitCode = 1;
        } else {
          exitCode = 1;
        }
        dropTest12Tables(conn);

        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n%s\n", exitCode == 0 ? "All tests PASSED" : "Some tests FAILED");
  return exitCode;
}
