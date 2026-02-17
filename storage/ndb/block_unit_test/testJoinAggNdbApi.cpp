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
        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n%s\n", exitCode == 0 ? "All tests PASSED" : "Some tests FAILED");
  return exitCode;
}
