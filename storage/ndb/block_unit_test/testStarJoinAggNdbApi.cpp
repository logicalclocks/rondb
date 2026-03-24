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
 * testStarJoinAggNdbApi — Integration test for multi-leaf star schema
 *                          aggregation using the NdbQueryBuilder API.
 *
 * Tests fan-out join topologies where a root table joins to multiple
 * child tables, each with its own aggregation program.
 *
 * Schema (created via MySQL):
 *   star_root(id INT PK, grp INT)
 *   star_leaf_a(root_id INT PK, val_a BIGINT)
 *   star_leaf_b(root_id INT PK, val_b BIGINT)
 *   star_leaf_c(root_id INT PK, val_c BIGINT)
 *
 * Usage: testStarJoinAggNdbApi -c <connect_string> -m <mysql_port> [-v]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include <NdbAggregator.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryBuilderImpl.hpp"
#include "NdbQueryOperation.hpp"
#include "NdbQueryOperationImpl.hpp"

#include <NdbRestarter.hpp>
#include <mysql.h>

#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <tuple>
#include <vector>

/* Verbose output control */
static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Debug: dump serialized QueryTree                                    */
/* ------------------------------------------------------------------ */

static void
dumpQueryTree(const NdbQueryDef *queryDef)
{
  if (!verbose) return;
  const NdbQueryDefImpl &impl = queryDef->getImpl();
  const Uint32Buffer &ser = impl.getSerialized();
  const Uint32 sz = ser.getSize();
  const Uint32 *buf = ser.addr();
  printf("  QueryTree (%u words):\n", sz);
  for (Uint32 i = 0; i < sz; i++) {
    printf("    [%2u] 0x%08x", i, buf[i]);
    if (i == 0) {
      Uint32 cnt = buf[i] >> 16;
      Uint32 len = buf[i] & 0xFFFF;
      printf("  (nodeCount=%u, treeLen=%u)", cnt, len);
    } else {
      /* Decode requestInfo bits for NI_AGGREGATE / NI_AGGREGATE_LEAF */
      /* NI_AGGREGATE = 0x2000, NI_AGGREGATE_LEAF = 0x4000 */
      if ((buf[i] & 0x6000) != 0) {
        printf("  (");
        if (buf[i] & 0x2000) printf("NI_AGGREGATE");
        if (buf[i] & 0x4000) printf("|NI_AGGREGATE_LEAF");
        printf(")");
      }
    }
    printf("\n");
  }
  printf("  numAggLeaves=%u\n", impl.getNumAggregateLeaves());
  for (Uint32 i = 0; i < impl.getNumAggregateLeaves(); i++) {
    printf("  aggLeafOpNo[%u]=%u\n", i, impl.getAggregateLeafOpNo(i));
  }
}

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *ROOT_TABLE   = "star_root";
static const char *LEAF_A_TABLE = "star_leaf_a";
static const char *LEAF_B_TABLE = "star_leaf_b";
static const char *LEAF_C_TABLE = "star_leaf_c";

/* Time-series tables (Tests 6-8): composite PK (entity_id, ts) */
static const char *TS_ENTITY   = "star_ts_entity";
static const char *TS_MEASURES = "star_ts_measures";
static const char *TS_EVENTS   = "star_ts_events";

static const char *TEST_DB      = "test_db";

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
  sqlExec(conn, "DROP TABLE IF EXISTS star_leaf_c");
  sqlExec(conn, "DROP TABLE IF EXISTS star_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS star_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS star_root");

  if (sqlExec(conn,
        "CREATE TABLE star_root ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", ROOT_TABLE);

  if (sqlExec(conn,
        "CREATE TABLE star_leaf_a ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_a BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", LEAF_A_TABLE);

  if (sqlExec(conn,
        "CREATE TABLE star_leaf_b ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", LEAF_B_TABLE);

  if (sqlExec(conn,
        "CREATE TABLE star_leaf_c ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_c BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", LEAF_C_TABLE);

  return 0;
}

static int
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS star_leaf_c");
  sqlExec(conn, "DROP TABLE IF EXISTS star_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS star_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS star_root");
  V("Dropped test tables\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Data insertion via MySQL                                            */
/* ------------------------------------------------------------------ */

static int
insertTestData(MYSQL *conn)
{
  /*
   * Root: 6 rows with 3 groups
   *   id=1 grp=1, id=2 grp=1, id=3 grp=2, id=4 grp=2, id=5 grp=3, id=6 grp=3
   *
   * Leaf A: val_a per root_id
   *   (1, 100), (2, 200), (3, 300), (4, 400), (5, 500), (6, 600)
   *
   * Leaf B: val_b per root_id
   *   (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)
   *
   * Leaf C: val_c per root_id
   *   (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)
   *
   * Expected 2-leaf (A+B) GROUP BY grp:
   *   grp=1: SUM(val_a)=300, SUM(val_b)=30
   *   grp=2: SUM(val_a)=700, SUM(val_b)=70
   *   grp=3: SUM(val_a)=1100, SUM(val_b)=110
   *
   * Expected 3-leaf (A+B+C) GROUP BY grp:
   *   grp=1: SUM(val_a)=300, COUNT(val_b)=2, MAX(val_c)=2
   *   grp=2: SUM(val_a)=700, COUNT(val_b)=2, MAX(val_c)=4
   *   grp=3: SUM(val_a)=1100, COUNT(val_b)=2, MAX(val_c)=6
   */
  if (sqlExec(conn,
        "INSERT INTO star_root VALUES "
        "(1,1),(2,1),(3,2),(4,2),(5,3),(6,3)") != 0) return -1;
  V("Inserted 6 root rows\n");

  if (sqlExec(conn,
        "INSERT INTO star_leaf_a VALUES "
        "(1,100),(2,200),(3,300),(4,400),(5,500),(6,600)") != 0) return -1;
  V("Inserted 6 leaf_a rows\n");

  if (sqlExec(conn,
        "INSERT INTO star_leaf_b VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)") != 0) return -1;
  V("Inserted 6 leaf_b rows\n");

  if (sqlExec(conn,
        "INSERT INTO star_leaf_c VALUES "
        "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6)") != 0) return -1;
  V("Inserted 6 leaf_c rows\n");

  return 0;
}

/* ------------------------------------------------------------------ */
/* MySQL verification helpers                                          */
/* ------------------------------------------------------------------ */

/* Verify query returning (grp, val1, val2) — 2-aggregate GROUP BY */
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
      fprintf(stderr, "  %s: MySQL group %d: expected (%lld, %lld), "
              "got (%lld, %lld)\n", testName, e.first,
              (long long)e.second.first, (long long)e.second.second,
              (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }
  V("  MySQL verification OK\n");
  return 0;
}

/* Verify query returning (grp, val1, val2, val3) — 3-aggregate GROUP BY */
static int
verifyTripleAggWithMysql(
    MYSQL *conn, const char *testName, const char *query,
    const std::map<Int32, std::tuple<Int64, Int64, Int64>> &expected)
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

  std::map<Int32, std::tuple<Int64, Int64, Int64>> mysqlResults;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result)) != nullptr) {
    Int32 grp = atoi(row[0]);
    Int64 val1 = atoll(row[1]);
    Int64 val2 = atoll(row[2]);
    Int64 val3 = atoll(row[3]);
    mysqlResults[grp] = {val1, val2, val3};
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
      fprintf(stderr, "  %s: MySQL group %d mismatch\n", testName, e.first);
      return -1;
    }
  }
  V("  MySQL verification OK\n");
  return 0;
}

/* Verify scalar query returning N columns (no GROUP BY) */
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

/* ------------------------------------------------------------------ */
/* Test 1: 2-leaf SUM(val_a) + SUM(val_b) GROUP BY grp                */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.grp, SUM(a.val_a), SUM(b.val_b)                         */
/*   FROM star_root r                                                  */
/*   JOIN star_leaf_a a ON a.root_id = r.id                            */
/*   JOIN star_leaf_b b ON b.root_id = r.id                            */
/*   GROUP BY r.grp                                                    */
/* ------------------------------------------------------------------ */

static int
test2LeafSumGroupBy(Ndb *ndb, MYSQL *conn,
                    const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 1: 2-leaf SUM(val_a) + SUM(val_b) GROUP BY grp ... ");
  fflush(stdout);

  if (verifyMultiAggWithMysql(conn, "Test 1",
        "SELECT r.grp, SUM(a.val_a), SUM(b.val_b) "
        "FROM star_root r "
        "JOIN star_leaf_a a ON a.root_id = r.id "
        "JOIN star_leaf_b b ON b.root_id = r.id "
        "GROUP BY r.grp ORDER BY r.grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(ROOT_TABLE);
  dict->invalidateTable(LEAF_A_TABLE);
  dict->invalidateTable(LEAF_B_TABLE);
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *leafBTab = dict->getTable(LEAF_B_TABLE);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Build aggregation programs for each leaf */

  /* Leaf A: GROUP BY grp (linked pos 0), SUM(val_a) */
  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) ||
      !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA program: %s)\n", aggA.GetError().err_msg_);
    return -1;
  }
  if (verbose) {
    printf("  aggA program (%u words):", aggA.instructions_length());
    for (Uint32 i = 0; i < aggA.instructions_length(); i++)
      printf(" %08x", aggA.buffer()[i]);
    printf("\n");
  }

  /* Leaf B: GROUP BY grp (linked pos 0), SUM(val_b) */
  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) ||
      !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB program: %s)\n", aggB.GetError().err_msg_);
    return -1;
  }
  if (verbose) {
    printf("  aggB program (%u words):", aggB.instructions_length());
    for (Uint32 i = 0; i < aggB.instructions_length(); i++)
      printf(" %08x", aggB.buffer()[i]);
    printf("\n");
  }

  /* Build pushed join query with 2 aggregate leaves */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Root: scan root table */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Linked value for GROUP BY grp */
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Leaf A: lookup by root_id = root.id */
  const NdbQueryOperand *joinKeyA[] = {
    qb->linkedValue(rootOp, "id"),
    nullptr
  };
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(grpLink);

  const NdbQueryLookupOperationDef *leafAOp =
      qb->readTuple(leafATab, joinKeyA, &optsA);
  if (leafAOp == nullptr) {
    printf("FAILED (readTuple leaf_a: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Leaf B: lookup by root_id = root.id */
  const NdbQueryOperand *joinKeyB[] = {
    qb->linkedValue(rootOp, "id"),
    nullptr
  };
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);

  /* Leaf B needs the same linked projection for GROUP BY */
  const NdbLinkedOperand *grpLinkB = qb->linkedValue(rootOp, "grp");
  if (grpLinkB == nullptr) {
    printf("FAILED (linkedValue grp for B: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  optsB.addLinkedProjection(grpLinkB);

  const NdbQueryLookupOperationDef *leafBOp =
      qb->readTuple(leafBTab, joinKeyB, &optsB);
  if (leafBOp == nullptr) {
    printf("FAILED (readTuple leaf_b: %s)\n", qb->getNdbError().message);
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
  dumpQueryTree(queryDef);

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

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result sumA = rec.FetchAggregationResult();
    Int64 valA = sumA.data_int64();

    NdbAggregator::Result sumB = rec.FetchAggregationResult();
    Int64 valB = sumB.data_int64();

    actual[grpVal] = {valA, valB};
    V("  grp=%d SUM(val_a)=%lld SUM(val_b)=%lld\n",
      grpVal, (long long)valA, (long long)valB);
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
    if (it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group %d: expected (%lld, %lld), got (%lld, %lld))\n",
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
/* Test 2: 2-leaf SUM(val_a) + SUM(val_b) — no GROUP BY               */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT SUM(a.val_a), SUM(b.val_b)                                 */
/*   FROM star_root r                                                  */
/*   JOIN star_leaf_a a ON a.root_id = r.id                            */
/*   JOIN star_leaf_b b ON b.root_id = r.id                            */
/* ------------------------------------------------------------------ */

static int
test2LeafSumNoGroupBy(Ndb *ndb, MYSQL *conn,
                      Int64 expectedSumA, Int64 expectedSumB)
{
  printf("Test 2: 2-leaf SUM(val_a) + SUM(val_b) no GROUP BY ... ");
  fflush(stdout);

  if (verifyScalarWithMysql(conn, "Test 2",
        "SELECT SUM(a.val_a), SUM(b.val_b) "
        "FROM star_root r "
        "JOIN star_leaf_a a ON a.root_id = r.id "
        "JOIN star_leaf_b b ON b.root_id = r.id",
        {expectedSumA, expectedSumB}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *leafBTab = dict->getTable(LEAF_B_TABLE);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Leaf A: SUM(val_a) — no GROUP BY */
  NdbAggregator aggA(leafATab);
  if (!aggA.LoadColumn("val_a", 0) ||
      !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA program: %s)\n", aggA.GetError().err_msg_);
    return -1;
  }

  /* Leaf B: SUM(val_b) — no GROUP BY */
  NdbAggregator aggB(leafBTab);
  if (!aggB.LoadColumn("val_b", 0) ||
      !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB program: %s)\n", aggB.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  const NdbQueryOperand *joinKeyA[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  qb->readTuple(leafATab, joinKeyA, &optsA);

  const NdbQueryOperand *joinKeyB[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  qb->readTuple(leafBTab, joinKeyB, &optsB);

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

  NdbAggregator::Result resA = rec.FetchAggregationResult();
  Int64 actualSumA = resA.data_int64();

  NdbAggregator::Result resB = rec.FetchAggregationResult();
  Int64 actualSumB = resB.data_int64();

  V("  SUM(val_a)=%lld SUM(val_b)=%lld\n",
    (long long)actualSumA, (long long)actualSumB);

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualSumA != expectedSumA || actualSumB != expectedSumB) {
    printf("FAILED (expected (%lld, %lld), got (%lld, %lld))\n",
           (long long)expectedSumA, (long long)expectedSumB,
           (long long)actualSumA, (long long)actualSumB);
    return -1;
  }

  printf("OK\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: 3-leaf SUM(val_a) + COUNT(val_b) + MAX(val_c) GROUP BY grp */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.grp, SUM(a.val_a), COUNT(b.val_b), MAX(c.val_c)         */
/*   FROM star_root r                                                  */
/*   JOIN star_leaf_a a ON a.root_id = r.id                            */
/*   JOIN star_leaf_b b ON b.root_id = r.id                            */
/*   JOIN star_leaf_c c ON c.root_id = r.id                            */
/*   GROUP BY r.grp                                                    */
/* ------------------------------------------------------------------ */

static int
test3LeafMixedAgg(
    Ndb *ndb, MYSQL *conn,
    const std::map<Int32, std::tuple<Int64, Int64, Int64>> &expected)
{
  printf("Test 3: 3-leaf SUM + COUNT + MAX GROUP BY grp ... ");
  fflush(stdout);

  if (verifyTripleAggWithMysql(conn, "Test 3",
        "SELECT r.grp, SUM(a.val_a), COUNT(b.val_b), MAX(c.val_c) "
        "FROM star_root r "
        "JOIN star_leaf_a a ON a.root_id = r.id "
        "JOIN star_leaf_b b ON b.root_id = r.id "
        "JOIN star_leaf_c c ON c.root_id = r.id "
        "GROUP BY r.grp ORDER BY r.grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(ROOT_TABLE);
  dict->invalidateTable(LEAF_A_TABLE);
  dict->invalidateTable(LEAF_B_TABLE);
  dict->invalidateTable(LEAF_C_TABLE);
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *leafBTab = dict->getTable(LEAF_B_TABLE);
  const NdbDictionary::Table *leafCTab = dict->getTable(LEAF_C_TABLE);
  if (rootTab == nullptr || leafATab == nullptr ||
      leafBTab == nullptr || leafCTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Leaf A: GROUP BY grp (linked pos 0), SUM(val_a) */
  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) ||
      !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA: %s)\n", aggA.GetError().err_msg_);
    return -1;
  }

  /* Leaf B: GROUP BY grp (linked pos 0), COUNT(val_b) */
  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) ||
      !aggB.Count(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB: %s)\n", aggB.GetError().err_msg_);
    return -1;
  }

  /* Leaf C: GROUP BY grp (linked pos 0), MAX(val_c) */
  NdbAggregator aggC(leafCTab);
  if (!aggC.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggC.LoadColumn("val_c", 0) ||
      !aggC.Max(0, 0) ||
      !aggC.Finalize()) {
    printf("FAILED (aggC: %s)\n", aggC.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Leaf A */
  const NdbQueryOperand *joinKeyA[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  const NdbLinkedOperand *grpLinkA = qb->linkedValue(rootOp, "grp");
  optsA.addLinkedProjection(grpLinkA);
  const NdbQueryLookupOperationDef *leafAOp =
      qb->readTuple(leafATab, joinKeyA, &optsA);
  if (leafAOp == nullptr) {
    printf("FAILED (readTuple leaf_a: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Leaf B */
  const NdbQueryOperand *joinKeyB[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  const NdbLinkedOperand *grpLinkB = qb->linkedValue(rootOp, "grp");
  optsB.addLinkedProjection(grpLinkB);
  const NdbQueryLookupOperationDef *leafBOp =
      qb->readTuple(leafBTab, joinKeyB, &optsB);
  if (leafBOp == nullptr) {
    printf("FAILED (readTuple leaf_b: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Leaf C */
  const NdbQueryOperand *joinKeyC[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsC;
  optsC.setMatchType(NdbQueryOptions::MatchNonNull);
  optsC.setAggregation(aggC);
  const NdbLinkedOperand *grpLinkC = qb->linkedValue(rootOp, "grp");
  optsC.addLinkedProjection(grpLinkC);
  const NdbQueryLookupOperationDef *leafCOp =
      qb->readTuple(leafCTab, joinKeyC, &optsC);
  if (leafCOp == nullptr) {
    printf("FAILED (readTuple leaf_c: %s)\n", qb->getNdbError().message);
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

  std::map<Int32, std::tuple<Int64, Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result resA = rec.FetchAggregationResult();
    Int64 valA = resA.data_int64();

    NdbAggregator::Result resB = rec.FetchAggregationResult();
    Int64 valB = resB.data_int64();

    NdbAggregator::Result resC = rec.FetchAggregationResult();
    Int64 valC = resC.data_int64();

    actual[grpVal] = {valA, valB, valC};
    V("  grp=%d SUM(val_a)=%lld COUNT(val_b)=%lld MAX(val_c)=%lld\n",
      grpVal, (long long)valA, (long long)valB, (long long)valC);
  }

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
    if (it->second != e.second) {
      printf("FAILED (group %d: expected (%lld, %lld, %lld), "
             "got (%lld, %lld, %lld))\n", e.first,
             (long long)std::get<0>(e.second),
             (long long)std::get<1>(e.second),
             (long long)std::get<2>(e.second),
             (long long)std::get<0>(it->second),
             (long long)std::get<1>(it->second),
             (long long)std::get<2>(it->second));
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: 2-leaf COUNT(*) + SUM(val_b) GROUP BY grp                   */
/*         Tests COUNT (not SUM) on first leaf                         */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.grp, COUNT(*), SUM(b.val_b)                              */
/*   FROM star_root r                                                  */
/*   JOIN star_leaf_a a ON a.root_id = r.id                            */
/*   JOIN star_leaf_b b ON b.root_id = r.id                            */
/*   GROUP BY r.grp                                                    */
/* ------------------------------------------------------------------ */

static int
test2LeafCountSum(Ndb *ndb, MYSQL *conn,
                  const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 4: 2-leaf COUNT(*) + SUM(val_b) GROUP BY grp ... ");
  fflush(stdout);

  if (verifyMultiAggWithMysql(conn, "Test 4",
        "SELECT r.grp, COUNT(*), SUM(b.val_b) "
        "FROM star_root r "
        "JOIN star_leaf_a a ON a.root_id = r.id "
        "JOIN star_leaf_b b ON b.root_id = r.id "
        "GROUP BY r.grp ORDER BY r.grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *leafBTab = dict->getTable(LEAF_B_TABLE);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Leaf A: GROUP BY grp, COUNT(*) */
  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) ||
      !aggA.Count(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA: %s)\n", aggA.GetError().err_msg_);
    return -1;
  }

  /* Leaf B: GROUP BY grp, SUM(val_b) */
  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) ||
      !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB: %s)\n", aggB.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  /* Leaf A */
  const NdbQueryOperand *joinKeyA[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafATab, joinKeyA, &optsA);

  /* Leaf B */
  const NdbQueryOperand *joinKeyB[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  optsB.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafBTab, joinKeyB, &optsB);

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
    V("  grp=%d COUNT(*)=%lld SUM(val_b)=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

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
    if (it->second != e.second) {
      printf("FAILED (group %d: expected (%lld, %lld), got (%lld, %lld))\n",
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
/* Test 5: 2-leaf MIN(val_a) + MAX(val_b) GROUP BY grp                */
/*         Tests MIN and MAX across leaves                             */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.grp, MIN(a.val_a), MAX(b.val_b)                         */
/*   FROM star_root r                                                  */
/*   JOIN star_leaf_a a ON a.root_id = r.id                            */
/*   JOIN star_leaf_b b ON b.root_id = r.id                            */
/*   GROUP BY r.grp                                                    */
/* ------------------------------------------------------------------ */

static int
test2LeafMinMax(Ndb *ndb, MYSQL *conn,
                const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 5: 2-leaf MIN(val_a) + MAX(val_b) GROUP BY grp ... ");
  fflush(stdout);

  if (verifyMultiAggWithMysql(conn, "Test 5",
        "SELECT r.grp, MIN(a.val_a), MAX(b.val_b) "
        "FROM star_root r "
        "JOIN star_leaf_a a ON a.root_id = r.id "
        "JOIN star_leaf_b b ON b.root_id = r.id "
        "GROUP BY r.grp ORDER BY r.grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *leafBTab = dict->getTable(LEAF_B_TABLE);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  /* Leaf A: GROUP BY grp, MIN(val_a) */
  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) ||
      !aggA.Min(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA: %s)\n", aggA.GetError().err_msg_);
    return -1;
  }

  /* Leaf B: GROUP BY grp, MAX(val_b) */
  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) ||
      !aggB.Max(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB: %s)\n", aggB.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  const NdbQueryOperand *joinKeyA[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafATab, joinKeyA, &optsA);

  const NdbQueryOperand *joinKeyB[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  optsB.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafBTab, joinKeyB, &optsB);

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

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    Int64 minVal = minRes.data_int64();

    NdbAggregator::Result maxRes = rec.FetchAggregationResult();
    Int64 maxVal = maxRes.data_int64();

    actual[grpVal] = {minVal, maxVal};
    V("  grp=%d MIN(val_a)=%lld MAX(val_b)=%lld\n",
      grpVal, (long long)minVal, (long long)maxVal);
  }

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
    if (it->second != e.second) {
      printf("FAILED (group %d: expected (%lld, %lld), got (%lld, %lld))\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ================================================================== */
/* Time-series star schema tests (Tests 6-8)                           */
/*                                                                     */
/* Tables use composite PK (entity_id, ts) so each entity has multiple */
/* rows per leaf table — requiring scan-scan joins via ordered index.   */
/* ================================================================== */

static int
createTsTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS star_ts_events");
  sqlExec(conn, "DROP TABLE IF EXISTS star_ts_measures");
  sqlExec(conn, "DROP TABLE IF EXISTS star_ts_entity");

  if (sqlExec(conn,
        "CREATE TABLE star_ts_entity ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", TS_ENTITY);

  if (sqlExec(conn,
        "CREATE TABLE star_ts_measures ("
        "  entity_id INT NOT NULL,"
        "  ts INT NOT NULL,"
        "  val BIGINT NOT NULL,"
        "  PRIMARY KEY(entity_id, ts),"
        "  INDEX idx_measures_eid (entity_id)"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", TS_MEASURES);

  if (sqlExec(conn,
        "CREATE TABLE star_ts_events ("
        "  entity_id INT NOT NULL,"
        "  ts INT NOT NULL,"
        "  event_type INT NOT NULL,"
        "  PRIMARY KEY(entity_id, ts),"
        "  INDEX idx_events_eid (entity_id)"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", TS_EVENTS);

  return 0;
}

static int
dropTsTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS star_ts_events");
  sqlExec(conn, "DROP TABLE IF EXISTS star_ts_measures");
  sqlExec(conn, "DROP TABLE IF EXISTS star_ts_entity");
  V("Dropped time-series tables\n");
  return 0;
}

static int
insertTsData(MYSQL *conn)
{
  /*
   * Entity: 4 entities in 2 groups
   *   (1, grp=1), (2, grp=1), (3, grp=2), (4, grp=2)
   *
   * Measures: 3 rows per entity (ts=1,2,3)
   *   entity 1: val=10,20,30   (sum=60)
   *   entity 2: val=40,50,60   (sum=150)
   *   entity 3: val=70,80,90   (sum=240)
   *   entity 4: val=100,110,120 (sum=330)
   *
   * Events: 2 rows per entity (ts=1,2)
   *   entity 1: type=1,2   (count=2, max=2)
   *   entity 2: type=3,4   (count=2, max=4)
   *   entity 3: type=5,6   (count=2, max=6)
   *   entity 4: type=7,8   (count=2, max=8)
   *
   * GROUP BY grp:
   *   grp=1 (entities 1,2):
   *     SUM(val)=60+150=210, COUNT(event_type)=2+2=4, MAX(event_type)=4
   *   grp=2 (entities 3,4):
   *     SUM(val)=240+330=570, COUNT(event_type)=2+2=4, MAX(event_type)=8
   *
   * Totals: SUM(val)=780, COUNT(event_type)=8
   */
  if (sqlExec(conn,
        "INSERT INTO star_ts_entity VALUES "
        "(1,1),(2,1),(3,2),(4,2)") != 0) return -1;
  V("Inserted 4 entity rows\n");

  if (sqlExec(conn,
        "INSERT INTO star_ts_measures VALUES "
        "(1,1,10),(1,2,20),(1,3,30),"
        "(2,1,40),(2,2,50),(2,3,60),"
        "(3,1,70),(3,2,80),(3,3,90),"
        "(4,1,100),(4,2,110),(4,3,120)") != 0) return -1;
  V("Inserted 12 measures rows\n");

  if (sqlExec(conn,
        "INSERT INTO star_ts_events VALUES "
        "(1,1,1),(1,2,2),"
        "(2,1,3),(2,2,4),"
        "(3,1,5),(3,2,6),"
        "(4,1,7),(4,2,8)") != 0) return -1;
  V("Inserted 8 events rows\n");

  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: Scan-scan 2-leaf SUM(val) + COUNT(event_type) GROUP BY grp  */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT e.grp, SUM(m.val), COUNT(ev.event_type)                    */
/*   FROM star_ts_entity e                                             */
/*   JOIN star_ts_measures m ON m.entity_id = e.id                     */
/*   JOIN star_ts_events ev ON ev.entity_id = e.id                     */
/*   GROUP BY e.grp                                                    */
/* ------------------------------------------------------------------ */

static int
testTsSumCountGroupBy(Ndb *ndb, MYSQL *conn,
                      const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 6: TS scan-scan 2-leaf SUM(val) + COUNT(event_type) "
         "GROUP BY grp ... ");
  fflush(stdout);

  if (verifyMultiAggWithMysql(conn, "Test 6",
        "SELECT grp, SUM(sum_val), SUM(cnt_evt) FROM ("
        "  SELECT e.id, e.grp, "
        "    (SELECT SUM(m.val) FROM star_ts_measures m "
        "     WHERE m.entity_id = e.id) AS sum_val, "
        "    (SELECT COUNT(ev.event_type) FROM star_ts_events ev "
        "     WHERE ev.entity_id = e.id) AS cnt_evt "
        "  FROM star_ts_entity e"
        ") t GROUP BY grp ORDER BY grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(TS_ENTITY);
  dict->invalidateTable(TS_MEASURES);
  dict->invalidateTable(TS_EVENTS);
  const NdbDictionary::Table *entityTab = dict->getTable(TS_ENTITY);
  const NdbDictionary::Table *measTab = dict->getTable(TS_MEASURES);
  const NdbDictionary::Table *evtTab = dict->getTable(TS_EVENTS);
  if (entityTab == nullptr || measTab == nullptr || evtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Index *measIdx =
      dict->getIndex("idx_measures_eid", TS_MEASURES);
  const NdbDictionary::Index *evtIdx =
      dict->getIndex("idx_events_eid", TS_EVENTS);
  if (measIdx == nullptr || evtIdx == nullptr) {
    printf("FAILED (index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Leaf measures: GROUP BY grp (linked pos 0), SUM(val) */
  NdbAggregator aggM(measTab);
  if (!aggM.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggM.LoadColumn("val", 0) ||
      !aggM.Sum(0, 0) ||
      !aggM.Finalize()) {
    printf("FAILED (aggM: %s)\n", aggM.GetError().err_msg_);
    return -1;
  }

  /* Leaf events: GROUP BY grp (linked pos 0), COUNT(event_type) */
  NdbAggregator aggE(evtTab);
  if (!aggE.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggE.LoadColumn("event_type", 0) ||
      !aggE.Count(0, 0) ||
      !aggE.Finalize()) {
    printf("FAILED (aggE: %s)\n", aggE.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Root: scan entity table */
  const NdbQueryTableScanOperationDef *entityOp = qb->scanTable(entityTab);
  if (entityOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Measures: scanIndex bound = entity.id */
  const NdbQueryOperand *measBound[] = {
    qb->linkedValue(entityOp, "id"), nullptr
  };
  NdbQueryIndexBound measIxBound(measBound);

  NdbQueryOptions measOpts;
  measOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  measOpts.setAggregation(aggM);
  const NdbLinkedOperand *grpLinkM = qb->linkedValue(entityOp, "grp");
  if (grpLinkM == nullptr) {
    printf("FAILED (linkedValue grp for M: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  measOpts.addLinkedProjection(grpLinkM);

  const NdbQueryIndexScanOperationDef *measOp =
      qb->scanIndex(measIdx, measTab, &measIxBound, &measOpts);
  if (measOp == nullptr) {
    printf("FAILED (scanIndex measures: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Events: scanIndex bound = entity.id */
  const NdbQueryOperand *evtBound[] = {
    qb->linkedValue(entityOp, "id"), nullptr
  };
  NdbQueryIndexBound evtIxBound(evtBound);

  NdbQueryOptions evtOpts;
  evtOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  evtOpts.setAggregation(aggE);
  const NdbLinkedOperand *grpLinkE = qb->linkedValue(entityOp, "grp");
  if (grpLinkE == nullptr) {
    printf("FAILED (linkedValue grp for E: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  evtOpts.addLinkedProjection(grpLinkE);

  const NdbQueryIndexScanOperationDef *evtOp =
      qb->scanIndex(evtIdx, evtTab, &evtIxBound, &evtOpts);
  if (evtOp == nullptr) {
    printf("FAILED (scanIndex events: %s)\n", qb->getNdbError().message);
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

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();

    actual[grpVal] = {sumVal, countVal};
    V("  grp=%d SUM(val)=%lld COUNT(event_type)=%lld\n",
      grpVal, (long long)sumVal, (long long)countVal);
  }

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
    if (it->second != e.second) {
      printf("FAILED (group %d: expected (%lld, %lld), got (%lld, %lld))\n",
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
/* Test 7: Scan-scan 2-leaf SUM(val) + COUNT(event_type) — no GROUP BY */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT SUM(m.val), COUNT(ev.event_type)                           */
/*   FROM star_ts_entity e                                             */
/*   JOIN star_ts_measures m ON m.entity_id = e.id                     */
/*   JOIN star_ts_events ev ON ev.entity_id = e.id                     */
/* ------------------------------------------------------------------ */

static int
testTsSumCountNoGroupBy(Ndb *ndb, MYSQL *conn,
                        Int64 expectedSum, Int64 expectedCount)
{
  printf("Test 7: TS scan-scan 2-leaf SUM(val) + COUNT(event_type) "
         "no GROUP BY ... ");
  fflush(stdout);

  if (verifyScalarWithMysql(conn, "Test 7",
        "SELECT "
        "  (SELECT SUM(m.val) FROM star_ts_measures m "
        "   JOIN star_ts_entity e ON m.entity_id = e.id), "
        "  (SELECT COUNT(ev.event_type) FROM star_ts_events ev "
        "   JOIN star_ts_entity e ON ev.entity_id = e.id)",
        {expectedSum, expectedCount}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *entityTab = dict->getTable(TS_ENTITY);
  const NdbDictionary::Table *measTab = dict->getTable(TS_MEASURES);
  const NdbDictionary::Table *evtTab = dict->getTable(TS_EVENTS);
  if (entityTab == nullptr || measTab == nullptr || evtTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  const NdbDictionary::Index *measIdx =
      dict->getIndex("idx_measures_eid", TS_MEASURES);
  const NdbDictionary::Index *evtIdx =
      dict->getIndex("idx_events_eid", TS_EVENTS);
  if (measIdx == nullptr || evtIdx == nullptr) {
    printf("FAILED (index lookup)\n");
    return -1;
  }

  /* Leaf measures: SUM(val) — no GROUP BY */
  NdbAggregator aggM(measTab);
  if (!aggM.LoadColumn("val", 0) ||
      !aggM.Sum(0, 0) ||
      !aggM.Finalize()) {
    printf("FAILED (aggM: %s)\n", aggM.GetError().err_msg_);
    return -1;
  }

  /* Leaf events: COUNT(event_type) — no GROUP BY */
  NdbAggregator aggE(evtTab);
  if (!aggE.LoadColumn("event_type", 0) ||
      !aggE.Count(0, 0) ||
      !aggE.Finalize()) {
    printf("FAILED (aggE: %s)\n", aggE.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *entityOp = qb->scanTable(entityTab);

  const NdbQueryOperand *measBound[] = {
    qb->linkedValue(entityOp, "id"), nullptr
  };
  NdbQueryIndexBound measIxBound(measBound);
  NdbQueryOptions measOpts;
  measOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  measOpts.setAggregation(aggM);
  qb->scanIndex(measIdx, measTab, &measIxBound, &measOpts);

  const NdbQueryOperand *evtBound[] = {
    qb->linkedValue(entityOp, "id"), nullptr
  };
  NdbQueryIndexBound evtIxBound(evtBound);
  NdbQueryOptions evtOpts;
  evtOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  evtOpts.setAggregation(aggE);
  qb->scanIndex(evtIdx, evtTab, &evtIxBound, &evtOpts);

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

  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 actualSum = sumRes.data_int64();

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 actualCount = countRes.data_int64();

  V("  SUM(val)=%lld COUNT(event_type)=%lld\n",
    (long long)actualSum, (long long)actualCount);

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualSum != expectedSum || actualCount != expectedCount) {
    printf("FAILED (expected (%lld, %lld), got (%lld, %lld))\n",
           (long long)expectedSum, (long long)expectedCount,
           (long long)actualSum, (long long)actualCount);
    return -1;
  }

  printf("OK\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 8: Scan-scan 2-leaf SUM(val) + MAX(event_type) GROUP BY grp    */
/*         Exercises MAX across fan-out with multiple rows per entity   */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT e.grp, SUM(m.val), MAX(ev.event_type)                      */
/*   FROM star_ts_entity e                                             */
/*   JOIN star_ts_measures m ON m.entity_id = e.id                     */
/*   JOIN star_ts_events ev ON ev.entity_id = e.id                     */
/*   GROUP BY e.grp                                                    */
/* ------------------------------------------------------------------ */

static int
testTsSumMaxGroupBy(Ndb *ndb, MYSQL *conn,
                    const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  printf("Test 8: TS scan-scan 2-leaf SUM(val) + MAX(event_type) "
         "GROUP BY grp ... ");
  fflush(stdout);

  if (verifyMultiAggWithMysql(conn, "Test 8",
        "SELECT grp, SUM(sum_val), MAX(max_evt) FROM ("
        "  SELECT e.id, e.grp, "
        "    (SELECT SUM(m.val) FROM star_ts_measures m "
        "     WHERE m.entity_id = e.id) AS sum_val, "
        "    (SELECT MAX(ev.event_type) FROM star_ts_events ev "
        "     WHERE ev.entity_id = e.id) AS max_evt "
        "  FROM star_ts_entity e"
        ") t GROUP BY grp ORDER BY grp", expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *entityTab = dict->getTable(TS_ENTITY);
  const NdbDictionary::Table *measTab = dict->getTable(TS_MEASURES);
  const NdbDictionary::Table *evtTab = dict->getTable(TS_EVENTS);
  if (entityTab == nullptr || measTab == nullptr || evtTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  const NdbDictionary::Index *measIdx =
      dict->getIndex("idx_measures_eid", TS_MEASURES);
  const NdbDictionary::Index *evtIdx =
      dict->getIndex("idx_events_eid", TS_EVENTS);
  if (measIdx == nullptr || evtIdx == nullptr) {
    printf("FAILED (index lookup)\n");
    return -1;
  }

  /* Leaf measures: GROUP BY grp (linked pos 0), SUM(val) */
  NdbAggregator aggM(measTab);
  if (!aggM.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggM.LoadColumn("val", 0) ||
      !aggM.Sum(0, 0) ||
      !aggM.Finalize()) {
    printf("FAILED (aggM: %s)\n", aggM.GetError().err_msg_);
    return -1;
  }

  /* Leaf events: GROUP BY grp (linked pos 0), MAX(event_type) */
  NdbAggregator aggE(evtTab);
  if (!aggE.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggE.LoadColumn("event_type", 0) ||
      !aggE.Max(0, 0) ||
      !aggE.Finalize()) {
    printf("FAILED (aggE: %s)\n", aggE.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *entityOp = qb->scanTable(entityTab);

  const NdbQueryOperand *measBound[] = {
    qb->linkedValue(entityOp, "id"), nullptr
  };
  NdbQueryIndexBound measIxBound(measBound);
  NdbQueryOptions measOpts;
  measOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  measOpts.setAggregation(aggM);
  measOpts.addLinkedProjection(qb->linkedValue(entityOp, "grp"));
  qb->scanIndex(measIdx, measTab, &measIxBound, &measOpts);

  const NdbQueryOperand *evtBound[] = {
    qb->linkedValue(entityOp, "id"), nullptr
  };
  NdbQueryIndexBound evtIxBound(evtBound);
  NdbQueryOptions evtOpts;
  evtOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  evtOpts.setAggregation(aggE);
  evtOpts.addLinkedProjection(qb->linkedValue(entityOp, "grp"));
  qb->scanIndex(evtIdx, evtTab, &evtIxBound, &evtOpts);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
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

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpCol.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    NdbAggregator::Result maxRes = rec.FetchAggregationResult();
    Int64 maxVal = maxRes.data_int64();

    actual[grpVal] = {sumVal, maxVal};
    V("  grp=%d SUM(val)=%lld MAX(event_type)=%lld\n",
      grpVal, (long long)sumVal, (long long)maxVal);
  }

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
    if (it->second != e.second) {
      printf("FAILED (group %d: expected (%lld, %lld), got (%lld, %lld))\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (%zu groups)\n", actual.size());
  return 0;
}

/* ================================================================== */
/* Multi-fragment tests (Test 9)                                       */
/* Tables with PARTITION_BALANCE=FOR_RP_BY_LDM_X_2 for many fragments  */
/* ================================================================== */

static const char *MF_ROOT   = "mf_root";
static const char *MF_LEAF_A = "mf_leaf_a";
static const char *MF_LEAF_B = "mf_leaf_b";

static int
createMfTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS mf_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS mf_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS mf_root");

  if (sqlExec(conn,
        "CREATE TABLE mf_root ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB "
        "COMMENT='NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_2'") != 0)
    return -1;

  if (sqlExec(conn,
        "CREATE TABLE mf_leaf_a ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_a BIGINT NOT NULL"
        ") ENGINE=NDB "
        "COMMENT='NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_2'") != 0)
    return -1;

  if (sqlExec(conn,
        "CREATE TABLE mf_leaf_b ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_b BIGINT NOT NULL"
        ") ENGINE=NDB "
        "COMMENT='NDB_TABLE=PARTITION_BALANCE=FOR_RP_BY_LDM_X_2'") != 0)
    return -1;

  V("Created mf_root, mf_leaf_a, mf_leaf_b (FOR_RP_BY_LDM_X_2)\n");
  return 0;
}

static int
insertMfData(MYSQL *conn)
{
  /*
   * 500 roots: id=1..500, grp = ((id-1) % 10) + 1  (10 groups, 50 each)
   * 500 leaf_a: root_id=id, val_a = id * 10
   * 500 leaf_b: root_id=id, val_b = id
   *
   * Group g: ids g, g+10, g+20, ..., g+490  (50 rows)
   * SUM(val_a) = 10 * (50*g + 10*(0+1+...+49)) = 500*g + 122500
   * SUM(val_b) = 50*g + 10*(0+1+...+49) = 50*g + 12250
   */
  char buf[65536];
  int pos = snprintf(buf, sizeof(buf), "INSERT INTO mf_root VALUES ");
  for (int i = 1; i <= 500; i++) {
    if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
    int grp = ((i - 1) % 10) + 1;
    pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d)", i, grp);
  }
  if (sqlExec(conn, buf) != 0) return -1;

  pos = snprintf(buf, sizeof(buf), "INSERT INTO mf_leaf_a VALUES ");
  for (int i = 1; i <= 500; i++) {
    if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
    pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d)", i, i * 10);
  }
  if (sqlExec(conn, buf) != 0) return -1;

  pos = snprintf(buf, sizeof(buf), "INSERT INTO mf_leaf_b VALUES ");
  for (int i = 1; i <= 500; i++) {
    if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
    pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d)", i, i);
  }
  if (sqlExec(conn, buf) != 0) return -1;

  V("Inserted 500 mf_root + 500 mf_leaf_a + 500 mf_leaf_b rows\n");
  return 0;
}

static int
dropMfTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS mf_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS mf_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS mf_root");
  return 0;
}

static int
testMultiFragStar(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 9: Multi-fragment 2-leaf SUM(val_a) + SUM(val_b) "
         "GROUP BY grp (500 rows, 10 groups) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(MF_ROOT);
  dict->invalidateTable(MF_LEAF_A);
  dict->invalidateTable(MF_LEAF_B);
  const NdbDictionary::Table *rootTab = dict->getTable(MF_ROOT);
  const NdbDictionary::Table *leafATab = dict->getTable(MF_LEAF_A);
  const NdbDictionary::Table *leafBTab = dict->getTable(MF_LEAF_B);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) || !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA)\n"); return -1;
  }

  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) || !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB)\n"); return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  const NdbQueryOperand *keyA[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafATab, keyA, &optsA);

  const NdbQueryOperand *keyB[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  optsB.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafBTab, keyB, &optsB);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy(); return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy(); return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator null)\n");
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;
    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    NdbAggregator::Result resA = rec.FetchAggregationResult();
    NdbAggregator::Result resB = rec.FetchAggregationResult();
    actual[grpCol.data_int32()] = {resA.data_int64(), resB.data_int64()};
  }

  query->close(); trans->close(); queryDef->destroy();

  /* Verify: 10 groups, each with expected SUM values */
  if (actual.size() != 10) {
    printf("FAILED (expected 10 groups, got %zu)\n", actual.size());
    return -1;
  }
  for (int g = 1; g <= 10; g++) {
    Int64 exp_a = 500LL * g + 122500LL;
    Int64 exp_b = 50LL * g + 12250LL;
    auto it = actual.find(g);
    if (it == actual.end()) {
      printf("FAILED (missing group %d)\n", g); return -1;
    }
    if (it->second.first != exp_a || it->second.second != exp_b) {
      printf("FAILED (group %d: expected (%lld,%lld), got (%lld,%lld))\n",
             g, (long long)exp_a, (long long)exp_b,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (10 groups, multi-fragment verified)\n");
  return 0;
}

/* ================================================================== */
/* Edge case tests (Tests 10-11)                                       */
/* ================================================================== */

static int
testEmptyLeafTable(Ndb *ndb, MYSQL *conn)
{
  printf("Test 10: Empty leaf table (root has rows, leaf_b empty) ... ");
  fflush(stdout);

  /* Use the basic tables but truncate leaf_b */
  sqlExec(conn, "TRUNCATE TABLE star_leaf_b");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(ROOT_TABLE);
  dict->invalidateTable(LEAF_A_TABLE);
  dict->invalidateTable(LEAF_B_TABLE);
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *leafBTab = dict->getTable(LEAF_B_TABLE);
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup)\n"); return -1;
  }

  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) || !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA)\n"); return -1;
  }

  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) || !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB)\n"); return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  const NdbQueryOperand *keyA[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafATab, keyA, &optsA);

  const NdbQueryOperand *keyB[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  optsB.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafBTab, keyB, &optsB);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy(); return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy(); return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator null)\n");
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  /* Multi-leaf fan-out: leaves are independent.  Leaf A matches all root
   * rows, leaf B matches none.  Groups are created by leaf A; leaf B's
   * accumulator slots stay at initial state (NULL for SUM).
   * Expected: 3 groups with leaf A's SUM and leaf B's SUM = NULL (0).
   */
  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;
    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    NdbAggregator::Result resA = rec.FetchAggregationResult();
    NdbAggregator::Result resB = rec.FetchAggregationResult();
    Int64 valB = resB.is_null() ? 0 : resB.data_int64();
    actual[grpCol.data_int32()] = {resA.data_int64(), valB};
  }

  query->close(); trans->close(); queryDef->destroy();

  /* leaf_a SUM per group from original data: grp1=300, grp2=700, grp3=1100
   * leaf_b SUM = 0 (empty table, no contributions) */
  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {1, {300, 0}}, {2, {700, 0}}, {3, {1100, 0}}
  };
  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }
  for (auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() || it->second != e.second) {
      printf("FAILED (group %d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (3 groups, empty leaf_b produces NULL/0)\n");
  return 0;
}

static int
testSingleRowResult(Ndb *ndb, MYSQL *conn)
{
  printf("Test 11: Single root row, single group ... ");
  fflush(stdout);

  sqlExec(conn, "DROP TABLE IF EXISTS sr_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS sr_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS sr_root");
  sqlExec(conn, "CREATE TABLE sr_root (id INT PRIMARY KEY, grp INT) ENGINE=NDB");
  sqlExec(conn, "CREATE TABLE sr_leaf_a (root_id INT PRIMARY KEY, val_a BIGINT) ENGINE=NDB");
  sqlExec(conn, "CREATE TABLE sr_leaf_b (root_id INT PRIMARY KEY, val_b BIGINT) ENGINE=NDB");
  sqlExec(conn, "INSERT INTO sr_root VALUES (1, 1)");
  sqlExec(conn, "INSERT INTO sr_leaf_a VALUES (1, 42)");
  sqlExec(conn, "INSERT INTO sr_leaf_b VALUES (1, 99)");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("sr_root");
  dict->invalidateTable("sr_leaf_a");
  dict->invalidateTable("sr_leaf_b");
  const NdbDictionary::Table *rootTab = dict->getTable("sr_root");
  const NdbDictionary::Table *leafATab = dict->getTable("sr_leaf_a");
  const NdbDictionary::Table *leafBTab = dict->getTable("sr_leaf_b");
  if (rootTab == nullptr || leafATab == nullptr || leafBTab == nullptr) {
    printf("FAILED (table lookup)\n"); return -1;
  }

  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) || !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA)\n"); return -1;
  }

  NdbAggregator aggB(leafBTab);
  if (!aggB.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggB.LoadColumn("val_b", 0) || !aggB.Sum(0, 0) ||
      !aggB.Finalize()) {
    printf("FAILED (aggB)\n"); return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  const NdbQueryOperand *keyA[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafATab, keyA, &optsA);

  const NdbQueryOperand *keyB[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsB;
  optsB.setMatchType(NdbQueryOptions::MatchNonNull);
  optsB.setAggregation(aggB);
  optsB.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafBTab, keyB, &optsB);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy(); return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy(); return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result)\n");
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
  NdbAggregator::Result resA = rec.FetchAggregationResult();
  NdbAggregator::Result resB = rec.FetchAggregationResult();
  Int32 grp = grpCol.data_int32();
  Int64 valA = resA.data_int64();
  Int64 valB = resB.data_int64();

  query->close(); trans->close(); queryDef->destroy();
  sqlExec(conn, "DROP TABLE IF EXISTS sr_leaf_b");
  sqlExec(conn, "DROP TABLE IF EXISTS sr_leaf_a");
  sqlExec(conn, "DROP TABLE IF EXISTS sr_root");

  if (grp != 1 || valA != 42 || valB != 99) {
    printf("FAILED (expected grp=1 sum_a=42 sum_b=99, "
           "got grp=%d sum_a=%lld sum_b=%lld)\n",
           grp, (long long)valA, (long long)valB);
    return -1;
  }

  printf("OK (1 group, single row)\n");
  return 0;
}

/* ================================================================== */
/* Mixed topology test (Test 12): one PK lookup + one index scan       */
/* ================================================================== */

static int
testMixedTopology(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 12: Mixed topology — PK lookup leaf + index scan leaf ... ");
  fflush(stdout);

  /* star_root + star_leaf_a (PK lookup) + star_ts_measures (index scan) */
  /* Root ids 1-4 overlap between both leaf table sets */

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(ROOT_TABLE);
  dict->invalidateTable(LEAF_A_TABLE);
  dict->invalidateTable(TS_MEASURES);
  const NdbDictionary::Table *rootTab = dict->getTable(ROOT_TABLE);
  const NdbDictionary::Table *leafATab = dict->getTable(LEAF_A_TABLE);
  const NdbDictionary::Table *measTab = dict->getTable(TS_MEASURES);
  if (rootTab == nullptr || leafATab == nullptr || measTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Index *measIdx =
      dict->getIndex("idx_measures_eid", TS_MEASURES);
  if (measIdx == nullptr) {
    printf("FAILED (index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Leaf A (PK lookup): SUM(val_a) GROUP BY grp */
  NdbAggregator aggA(leafATab);
  if (!aggA.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggA.LoadColumn("val_a", 0) || !aggA.Sum(0, 0) ||
      !aggA.Finalize()) {
    printf("FAILED (aggA)\n"); return -1;
  }

  /* Leaf M (index scan): COUNT(val) GROUP BY grp */
  NdbAggregator aggM(measTab);
  if (!aggM.GroupBy(0 | AGG_LINKED_COL_FLAG) ||
      !aggM.LoadColumn("val", 0) || !aggM.Count(0, 0) ||
      !aggM.Finalize()) {
    printf("FAILED (aggM)\n"); return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);

  /* Leaf A: PK lookup */
  const NdbQueryOperand *keyA[] = {qb->linkedValue(rootOp, "id"), nullptr};
  NdbQueryOptions optsA;
  optsA.setMatchType(NdbQueryOptions::MatchNonNull);
  optsA.setAggregation(aggA);
  optsA.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->readTuple(leafATab, keyA, &optsA);

  /* Leaf M: index scan */
  const NdbQueryOperand *measBound[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  NdbQueryIndexBound measIxBound(measBound);
  NdbQueryOptions optsM;
  optsM.setMatchType(NdbQueryOptions::MatchNonNull);
  optsM.setAggregation(aggM);
  optsM.addLinkedProjection(qb->linkedValue(rootOp, "grp"));
  qb->scanIndex(measIdx, measTab, &measIxBound, &optsM);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy(); return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy(); return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %d: %s)\n",
           query->getNdbError().code, query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator null)\n");
    query->close(); trans->close(); queryDef->destroy(); return -1;
  }

  /*
   * star_root has ids 1-6 with grp 1,1,2,2,3,3
   * star_leaf_a has val_a: 100,200,300,400,500,600 — matches all 6
   * star_ts_measures has entity_id 1-4 only (3 rows each)
   *
   * Multi-leaf fan-out: leaves independent.  All 6 root rows produce
   * leaf_a results.  Only ids 1-4 produce measures results.
   *
   *   grp=1 (ids 1,2): SUM(val_a)=300, COUNT(measures)=6 (3+3)
   *   grp=2 (ids 3,4): SUM(val_a)=700, COUNT(measures)=6 (3+3)
   *   grp=3 (ids 5,6): SUM(val_a)=1100, COUNT(measures)=0 (no matches)
   */
  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;
    NdbAggregator::Column grpCol = rec.FetchGroupbyColumn();
    NdbAggregator::Result resA = rec.FetchAggregationResult();
    NdbAggregator::Result resM = rec.FetchAggregationResult();
    Int64 valM = resM.is_null() ? 0 : resM.data_int64();
    actual[grpCol.data_int32()] = {resA.data_int64(), valM};
  }

  query->close(); trans->close(); queryDef->destroy();

  if (actual.size() != 3) {
    printf("FAILED (expected 3 groups, got %zu)\n", actual.size());
    for (auto &p : actual)
      V("  grp=%d sum_a=%lld cnt_m=%lld\n", p.first,
        (long long)p.second.first, (long long)p.second.second);
    return -1;
  }

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {1, {300, 6}}, {2, {700, 6}}, {3, {1100, 0}}
  };
  for (auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() || it->second != e.second) {
      printf("FAILED (group %d: expected (%lld,%lld) got (%lld,%lld))\n",
             e.first, (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (3 groups, mixed PK-lookup + index-scan)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static int onlyTest = 0;
static int skipTest = 0;

static bool
shouldRun(int testNum)
{
  if (onlyTest != 0) return testNum == onlyTest;
  if (skipTest != 0) return testNum != skipTest;
  return true;
}

static void
usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s -c <connect_string> -m <mysql_port> [-v|--verbose] [-h]\n"
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

  printf("=== testStarJoinAggNdbApi ===\n");
  printf("Connect string: %s\n", connectString);
  printf("MySQL port: %d\n\n", mysqlPort);

  ndb_init();

  int exitCode = 0;

  {
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
      MYSQL *conn = connectMysql(mysqlPort);
      if (conn == nullptr) {
        exitCode = 1;
      } else {
        char createDb[128];
        snprintf(createDb, sizeof(createDb),
                 "CREATE DATABASE IF NOT EXISTS %s", TEST_DB);
        sqlExec(conn, createDb);

        char useDb[128];
        snprintf(useDb, sizeof(useDb), "USE %s", TEST_DB);
        sqlExec(conn, useDb);

        Ndb ndb(&clusterConn, TEST_DB);
        if (ndb.init() != 0) {
          fprintf(stderr, "Ndb::init failed: %s\n",
                  ndb.getNdbError().message);
          exitCode = 1;
        }
        else if (createTestTables(conn) != 0 ||
                 insertTestData(conn) != 0) {
          exitCode = 1;
        }
        else {
          /*
           * Test data:
           *   root: (1,1),(2,1),(3,2),(4,2),(5,3),(6,3)
           *   leaf_a: (1,100),(2,200),(3,300),(4,400),(5,500),(6,600)
           *   leaf_b: (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)
           *   leaf_c: (1,1),(2,2),(3,3),(4,4),(5,5),(6,6)
           *
           * GROUP BY grp:
           *   grp=1: rows id=1,2 → sum_a=300, sum_b=30, count=2,
           *          min_a=100, max_b=20, max_c=2
           *   grp=2: rows id=3,4 → sum_a=700, sum_b=70, count=2,
           *          min_a=300, max_b=40, max_c=4
           *   grp=3: rows id=5,6 → sum_a=1100, sum_b=110, count=2,
           *          min_a=500, max_b=60, max_c=6
           *
           * Totals: sum_a=2100, sum_b=210
           */

          /* Test 1: 2-leaf SUM + SUM GROUP BY grp */
          if (shouldRun(1)) {
            std::map<Int32, std::pair<Int64, Int64>> expected = {
              {1, {300, 30}}, {2, {700, 70}}, {3, {1100, 110}}
            };
            if (test2LeafSumGroupBy(&ndb, conn, expected) != 0) {
              exitCode = 1;
            }
          }

          /* Test 2: 2-leaf SUM + SUM no GROUP BY */
          if (shouldRun(2)) {
            if (test2LeafSumNoGroupBy(&ndb, conn, 2100, 210) != 0) {
              exitCode = 1;
            }
          }

          /* Test 3: 3-leaf SUM + COUNT + MAX GROUP BY grp */
          if (shouldRun(3)) {
            std::map<Int32, std::tuple<Int64, Int64, Int64>> expected = {
              {1, {300, 2, 2}}, {2, {700, 2, 4}}, {3, {1100, 2, 6}}
            };
            if (test3LeafMixedAgg(&ndb, conn, expected) != 0) {
              exitCode = 1;
            }
          }

          /* Test 4: 2-leaf COUNT + SUM GROUP BY grp */
          if (shouldRun(4)) {
            std::map<Int32, std::pair<Int64, Int64>> expected = {
              {1, {2, 30}}, {2, {2, 70}}, {3, {2, 110}}
            };
            if (test2LeafCountSum(&ndb, conn, expected) != 0) {
              exitCode = 1;
            }
          }

          /* Test 5: 2-leaf MIN + MAX GROUP BY grp */
          if (shouldRun(5)) {
            std::map<Int32, std::pair<Int64, Int64>> expected = {
              {1, {100, 20}}, {2, {300, 40}}, {3, {500, 60}}
            };
            if (test2LeafMinMax(&ndb, conn, expected) != 0) {
              exitCode = 1;
            }
          }
        }

        dropTestTables(conn);

        /*
         * Time-series tests (6-8): composite PK (entity_id, ts)
         * with scan-scan joins via ordered index.
         *
         * Data:
         *   entity: (1,grp=1),(2,grp=1),(3,grp=2),(4,grp=2)
         *   measures: 3 rows per entity, val=10..120
         *   events: 2 rows per entity, event_type=1..8
         *
         * GROUP BY grp:
         *   grp=1: SUM(val)=210, COUNT(event_type)=4, MAX(event_type)=4
         *   grp=2: SUM(val)=570, COUNT(event_type)=4, MAX(event_type)=8
         *
         * Totals: SUM(val)=780, COUNT(event_type)=8
         */
        if (shouldRun(6) || shouldRun(7) || shouldRun(8)) {
          if (createTsTables(conn) == 0 && insertTsData(conn) == 0) {

            /* Test 6: TS 2-leaf SUM + COUNT GROUP BY grp */
            if (shouldRun(6)) {
              std::map<Int32, std::pair<Int64, Int64>> expected = {
                {1, {210, 4}}, {2, {570, 4}}
              };
              if (testTsSumCountGroupBy(&ndb, conn, expected) != 0) {
                exitCode = 1;
              }
            }

            /* Test 7: TS 2-leaf SUM + COUNT no GROUP BY */
            if (shouldRun(7)) {
              if (testTsSumCountNoGroupBy(&ndb, conn, 780, 8) != 0) {
                exitCode = 1;
              }
            }

            /* Test 8: TS 2-leaf SUM + MAX GROUP BY grp */
            if (shouldRun(8)) {
              std::map<Int32, std::pair<Int64, Int64>> expected = {
                {1, {210, 4}}, {2, {570, 8}}
              };
              if (testTsSumMaxGroupBy(&ndb, conn, expected) != 0) {
                exitCode = 1;
              }
            }

          } else {
            exitCode = 1;
          }
          dropTsTables(conn);
        }

        /* Test 9: Multi-fragment 2-leaf */
        if (shouldRun(9)) {
          if (createMfTables(conn) == 0 && insertMfData(conn) == 0) {
            if (testMultiFragStar(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropMfTables(conn);
        }

        /* Test 10: Empty leaf table */
        if (shouldRun(10)) {
          if (createTestTables(conn) == 0 && insertTestData(conn) == 0) {
            if (testEmptyLeafTable(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropTestTables(conn);
        }

        /* Test 11: Single row result */
        if (shouldRun(11)) {
          if (testSingleRowResult(&ndb, conn) != 0) exitCode = 1;
        }

        /* Test 12: Mixed topology (PK lookup + index scan) */
        if (shouldRun(12)) {
          /* Needs both star_root/leaf_a tables and ts_measures */
          if (createTestTables(conn) == 0 && insertTestData(conn) == 0 &&
              createTsTables(conn) == 0 && insertTsData(conn) == 0) {
            if (testMixedTopology(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropTsTables(conn);
          dropTestTables(conn);
        }

        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n%s\n", exitCode == 0 ? "All tests PASSED" : "Some tests FAILED");
  return exitCode;
}
