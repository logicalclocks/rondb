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
 * testOuterJoinAggNdbApi — Integration tests for outer join aggregation
 *                          using the NdbQueryBuilder API.
 *
 * Tests LEFT OUTER JOIN aggregation pushdown through the NDB API:
 *   NdbQueryBuilder → NdbQueryDef → NdbQuery → getAggregator()
 *
 * Outer join = default MatchAll (no setMatchType call).
 * Parent rows without child matches contribute COUNT but SUM(val)=0
 * (NULL child val treated as 0 by the aggregation engine).
 *
 * Tests 1-4:   scan-lookup outer join (COUNT(c.pk) semantics)
 * Tests 5-8:   scan-scan outer join (COUNT(c.pk) semantics)
 * Tests 9-12:  COUNT(*) via LoadUint64(1) — counts all rows incl. unmatched
 *
 * Usage: testOuterJoinAggNdbApi -c <connect_string> -m <mysql_port> [-v]
 *        [--only N] [--skip N]
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

static int onlyTest = 0;
static int skipTest = 0;

static bool shouldRun(int testNum) {
  if (onlyTest > 0 && testNum != onlyTest) return false;
  if (skipTest > 0 && testNum == skipTest) return false;
  return true;
}

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *TEST_DB = "test_db";

/* Lookup tables (Tests 1-4) */
static const char *OJ_PARENT = "oj_parent";
static const char *OJ_CHILD  = "oj_child";

/* Scan-scan tables (Tests 5-8) */
static const char *OJ_PARENT_SS = "oj_parent_ss";
static const char *OJ_CHILD_SS  = "oj_child_ss";
static const char *OJ_CHILD_SS_IDX = "ix_jc";

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
/* Scalar verification: query returns 1 row with N columns             */
/* ------------------------------------------------------------------ */

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
    Int64 actual = (row[i] != nullptr) ? atoll(row[i]) : 0;
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

/* GROUP BY verification: query returns (grp, val1, val2) */
static int
verifyGroupByWithMysql(MYSQL *conn, const char *testName, const char *query,
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
    Int64 val2 = (row[2] != nullptr) ? atoll(row[2]) : 0;
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
      fprintf(stderr, "  %s: MySQL group %d: expected (%lld,%lld), got (%lld,%lld)\n",
              testName, e.first,
              (long long)e.second.first, (long long)e.second.second,
              (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }
  V("  MySQL verification OK\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Lookup table setup (Tests 1-4)                                      */
/* ------------------------------------------------------------------ */

static int
createLookupTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj_child");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_parent");

  if (sqlExec(conn,
        "CREATE TABLE oj_parent ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ_PARENT);

  if (sqlExec(conn,
        "CREATE TABLE oj_child ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ_CHILD);

  return 0;
}

static int
dropLookupTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj_child");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_parent");
  V("Dropped lookup tables\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Scan-scan table setup (Tests 5-8)                                   */
/* ------------------------------------------------------------------ */

static int
createScanScanTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj_child_ss");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_parent_ss");

  if (sqlExec(conn,
        "CREATE TABLE oj_parent_ss ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  join_col INT NOT NULL,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ_PARENT_SS);

  if (sqlExec(conn,
        "CREATE TABLE oj_child_ss ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  join_col INT NOT NULL,"
        "  val BIGINT NOT NULL,"
        "  INDEX ix_jc (join_col)"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ_CHILD_SS);

  return 0;
}

static int
dropScanScanTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj_child_ss");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_parent_ss");
  V("Dropped scan-scan tables\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Data insertion helpers                                               */
/* ------------------------------------------------------------------ */

/* Partial match: 10 parent, 5 child (pk 1-5 match, 6-10 no match)
 * parent: pk=1..10, grp = (pk-1)/3 + 1
 *   grp 1: pk 1,2,3
 *   grp 2: pk 4,5,6
 *   grp 3: pk 7,8,9
 *   grp 4: pk 10
 * child:  pk=1..5, val = pk*10
 *
 * Outer join result (10 rows):
 *   pk=1..5 have child val, pk=6..10 have NULL child
 * COUNT(*) = 10, SUM(COALESCE(val,0)) = 10+20+30+40+50 = 150
 */
static int
insertPartialMatchLookup(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj_parent VALUES "
        "(1,1),(2,1),(3,1),(4,2),(5,2),(6,2),(7,3),(8,3),(9,3),(10,4)") != 0)
    return -1;
  V("Inserted 10 parent rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj_child VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50)") != 0) return -1;
  V("Inserted 5 child rows\n");

  return 0;
}

static int
insertAllMatchLookup(MYSQL *conn)
{
  /* Clear and re-insert: 5 parent, 5 child — all match */
  sqlExec(conn, "DELETE FROM oj_child");
  sqlExec(conn, "DELETE FROM oj_parent");

  if (sqlExec(conn,
        "INSERT INTO oj_parent VALUES "
        "(1,1),(2,1),(3,2),(4,2),(5,3)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj_child VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50)") != 0) return -1;
  V("Inserted all-match data (5 parent, 5 child)\n");
  return 0;
}

static int
insertNoMatchLookup(MYSQL *conn)
{
  /* Clear and re-insert: 5 parent, child pks 100-104 (no match) */
  sqlExec(conn, "DELETE FROM oj_child");
  sqlExec(conn, "DELETE FROM oj_parent");

  if (sqlExec(conn,
        "INSERT INTO oj_parent VALUES "
        "(1,1),(2,1),(3,2),(4,2),(5,3)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj_child VALUES "
        "(100,10),(101,20),(102,30),(103,40),(104,50)") != 0) return -1;
  V("Inserted no-match data (5 parent, 5 child at different pks)\n");
  return 0;
}

/* Scan-scan partial match:
 * parent_ss: 10 rows, pk=1..10, join_col=pk, grp same as lookup
 * child_ss:  5 rows, pk=1..5, join_col=pk, val=pk*10
 */
static int
insertPartialMatchScanScan(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj_parent_ss VALUES "
        "(1,1,1),(2,2,1),(3,3,1),(4,4,2),(5,5,2),"
        "(6,6,2),(7,7,3),(8,8,3),(9,9,3),(10,10,4)") != 0) return -1;
  V("Inserted 10 parent_ss rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj_child_ss VALUES "
        "(1,1,10),(2,2,20),(3,3,30),(4,4,40),(5,5,50)") != 0) return -1;
  V("Inserted 5 child_ss rows\n");

  return 0;
}

static int
insertNoMatchScanScan(MYSQL *conn)
{
  /* 10 parent, child join_col values don't match any parent */
  sqlExec(conn, "DELETE FROM oj_child_ss");
  sqlExec(conn, "DELETE FROM oj_parent_ss");

  if (sqlExec(conn,
        "INSERT INTO oj_parent_ss VALUES "
        "(1,1,1),(2,2,1),(3,3,1),(4,4,2),(5,5,2),"
        "(6,6,2),(7,7,3),(8,8,3),(9,9,3),(10,10,4)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj_child_ss VALUES "
        "(100,100,10),(101,101,20),(102,102,30),(103,103,40),(104,104,50)") != 0)
    return -1;
  V("Inserted no-match scan-scan data\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Helper: run a scalar outer join aggregation query (scan-lookup)      */
/*                                                                     */
/* NDB agg equivalent: SELECT COUNT(c.pk), SUM(c.val)                  */
/*   FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk              */
/* COUNT counts matched children; SUM over matched child values.       */
/* ------------------------------------------------------------------ */

static int
runScalarOuterJoinLookup(Ndb *ndb, const char *testName,
                         Int64 expectedCount, Int64 expectedSum)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ_PARENT);
  dict->invalidateTable(OJ_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(OJ_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(OJ_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Aggregation: COUNT(*), SUM(val) — no GROUP BY */
  NdbAggregator agg(childTab);
  if (!agg.LoadColumn("val", 0) ||   /* reg 0 = val */
      !agg.Count(0, 0) ||            /* agg[0] = COUNT */
      !agg.Sum(1, 0) ||              /* agg[1] = SUM(val) */
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "pk"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  /* No setMatchType(MatchNonNull) → outer join (default MatchAll) */

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

  V("\n  %s: COUNT=%lld SUM=%lld\n",
    testName, (long long)actualCount, (long long)actualSum);

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

  return 0;
}

/* ------------------------------------------------------------------ */
/* Helper: run a GROUP BY outer join aggregation query (scan-lookup)    */
/*                                                                     */
/* NDB agg equivalent: SELECT p.grp, COUNT(c.pk), SUM(c.val)          */
/*   FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk             */
/*   GROUP BY p.grp                                                    */
/* Groups exist for all parents; COUNT/SUM only over matched children. */
/* ------------------------------------------------------------------ */

static int
runGroupByOuterJoinLookup(Ndb *ndb, const char *testName [[maybe_unused]],
                          const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ_PARENT);
  dict->invalidateTable(OJ_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(OJ_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(OJ_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Aggregation: GROUP BY grp (linked), COUNT(*), SUM(val) */
  NdbAggregator agg(childTab);
  const NdbDictionary::Column *grpCol = parentTab->getColumn("grp");
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadColumn("val", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "pk"),
    nullptr
  };

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
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

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpValCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpValCol.data_int32();

    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = {countVal, sumVal};
    V("  grp=%d COUNT=%lld SUM=%lld\n",
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

  return 0;
}

/* ------------------------------------------------------------------ */
/* Helper: run a scalar outer join aggregation query (scan-scan)        */
/*                                                                     */
/* NDB agg equivalent: SELECT COUNT(c.pk), SUM(c.val)                  */
/*   FROM oj_parent_ss p LEFT JOIN oj_child_ss c                       */
/*     ON c.join_col = p.join_col                                      */
/* ------------------------------------------------------------------ */

static int
runScalarOuterJoinScanScan(Ndb *ndb, const char *testName,
                           Int64 expectedCount, Int64 expectedSum,
                           bool useInlineMatch)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ_PARENT_SS);
  dict->invalidateTable(OJ_CHILD_SS);
  dict->invalidateIndex(OJ_CHILD_SS_IDX, OJ_CHILD_SS);
  const NdbDictionary::Table *parentTab = dict->getTable(OJ_PARENT_SS);
  const NdbDictionary::Table *childTab = dict->getTable(OJ_CHILD_SS);
  const NdbDictionary::Index *childIdx =
      dict->getIndex(OJ_CHILD_SS_IDX, OJ_CHILD_SS);
  if (parentTab == nullptr || childTab == nullptr || childIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.LoadColumn("val", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(parentOp, "join_col"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  if (useInlineMatch) {
    opts.setInlineMatch(true);
  }

  const NdbQueryIndexScanOperationDef *childOp =
      qb->scanIndex(childIdx, childTab, &bound, &opts);
  if (childOp == nullptr) {
    printf("FAILED (scanIndex: %s)\n", qb->getNdbError().message);
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

  V("\n  %s: COUNT=%lld SUM=%lld (inline=%d)\n",
    testName, (long long)actualCount, (long long)actualSum, useInlineMatch);

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

  return 0;
}

/* ------------------------------------------------------------------ */
/* Helper: GROUP BY outer join scan-scan                                */
/* ------------------------------------------------------------------ */

static int
runGroupByOuterJoinScanScan(Ndb *ndb, const char *testName [[maybe_unused]],
                            const std::map<Int32, std::pair<Int64, Int64>> &expected)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ_PARENT_SS);
  dict->invalidateTable(OJ_CHILD_SS);
  dict->invalidateIndex(OJ_CHILD_SS_IDX, OJ_CHILD_SS);
  const NdbDictionary::Table *parentTab = dict->getTable(OJ_PARENT_SS);
  const NdbDictionary::Table *childTab = dict->getTable(OJ_CHILD_SS);
  const NdbDictionary::Index *childIdx =
      dict->getIndex(OJ_CHILD_SS_IDX, OJ_CHILD_SS);
  if (parentTab == nullptr || childTab == nullptr || childIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = parentTab->getColumn("grp");

  NdbAggregator agg(childTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadColumn("val", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(parentOp, "join_col"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(parentOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  opts.addLinkedProjection(grpLink);

  const NdbQueryIndexScanOperationDef *childOp =
      qb->scanIndex(childIdx, childTab, &bound, &opts);
  if (childOp == nullptr) {
    printf("FAILED (scanIndex: %s)\n", qb->getNdbError().message);
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

  std::map<Int32, std::pair<Int64, Int64>> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grpValCol = rec.FetchGroupbyColumn();
    Int32 grpVal = grpValCol.data_int32();

    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[grpVal] = {countVal, sumVal};
    V("  grp=%d COUNT=%lld SUM=%lld\n",
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

  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 1: Scan-lookup partial match COUNT/SUM                         */
/*                                                                     */
/* 10 parent rows, 5 child rows (pk 1-5 match).                       */
/* Aggregation COUNT counts matched child rows, not all outer-join     */
/* extended rows. SUM only over matched children.                      */
/* Expected: COUNT=5, SUM=150                                          */
/* ------------------------------------------------------------------ */

static int
testPartialMatchLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 1: Scan-lookup partial match COUNT/SUM ... ");
  fflush(stdout);

  if (verifyScalarWithMysql(conn, "Test 1",
        "SELECT COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk",
        {5, 150}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runScalarOuterJoinLookup(ndb, "Test 1", 5, 150) != 0) return -1;

  printf("OK (count=5, sum=150)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: Scan-lookup all match COUNT/SUM                             */
/*                                                                     */
/* 5 parent, 5 child — all match. Expected: COUNT=5, SUM=150          */
/* ------------------------------------------------------------------ */

static int
testAllMatchLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 2: Scan-lookup all match COUNT/SUM ... ");
  fflush(stdout);

  if (insertAllMatchLookup(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 2",
        "SELECT COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk",
        {5, 150}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runScalarOuterJoinLookup(ndb, "Test 2", 5, 150) != 0) return -1;

  printf("OK (count=5, sum=150)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: Scan-lookup no match COUNT/SUM                              */
/*                                                                     */
/* 5 parent, child pks 100-104 (no match).                            */
/* No children match → COUNT=0, SUM=0.                                */
/* ------------------------------------------------------------------ */

static int
testNoMatchLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 3: Scan-lookup no match COUNT/SUM ... ");
  fflush(stdout);

  if (insertNoMatchLookup(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 3",
        "SELECT COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk",
        {0, 0}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runScalarOuterJoinLookup(ndb, "Test 3", 0, 0) != 0) return -1;

  printf("OK (count=0, sum=0)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: Scan-lookup partial match GROUP BY grp                      */
/*                                                                     */
/* Re-insert partial match data (10 parent, 5 child).                  */
/* COUNT counts matched children only (COUNT(c.pk) semantics).         */
/* grp 1: pk 1,2,3 → 3 match child, COUNT=3, SUM=10+20+30=60         */
/* grp 2: pk 4,5,6 → 2 match child, COUNT=2, SUM=40+50=90            */
/* grp 3: pk 7,8,9 → 0 match child, COUNT=0, SUM=0                   */
/* grp 4: pk 10    → 0 match child, COUNT=0, SUM=0                    */
/* ------------------------------------------------------------------ */

static int
testGroupByLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 4: Scan-lookup partial match GROUP BY grp ... ");
  fflush(stdout);

  /* Re-insert partial match data */
  sqlExec(conn, "DELETE FROM oj_child");
  sqlExec(conn, "DELETE FROM oj_parent");
  if (insertPartialMatchLookup(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {1, {3, 60}},   /* grp 1: COUNT=3, SUM=10+20+30 */
    {2, {2, 90}},   /* grp 2: COUNT=2, SUM=40+50 */
    {3, {0, 0}},    /* grp 3: COUNT=0, SUM=0 (no match) */
    {4, {0, 0}}     /* grp 4: COUNT=0, SUM=0 (no match) */
  };

  if (verifyGroupByWithMysql(conn, "Test 4",
        "SELECT p.grp, COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk "
        "GROUP BY p.grp ORDER BY p.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runGroupByOuterJoinLookup(ndb, "Test 4", expected) != 0) return -1;

  printf("OK (4 groups)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: Scan-scan partial match COUNT/SUM (bitmask protocol)        */
/*                                                                     */
/* 10 parent, 5 child (join_col 1-5 match).                           */
/* COUNT counts matched children only. Expected: COUNT=5, SUM=150     */
/* ------------------------------------------------------------------ */

static int
testPartialMatchScanScan(Ndb *ndb, MYSQL *conn)
{
  printf("Test 5: Scan-scan partial match COUNT/SUM (bitmask) ... ");
  fflush(stdout);

  if (verifyScalarWithMysql(conn, "Test 5",
        "SELECT COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent_ss p LEFT JOIN oj_child_ss c "
        "ON c.join_col = p.join_col",
        {5, 150}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runScalarOuterJoinScanScan(ndb, "Test 5", 5, 150,
                                  false /*bitmask*/) != 0) return -1;

  printf("OK (count=5, sum=150)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: Scan-scan partial match COUNT/SUM (inline match)            */
/*                                                                     */
/* Same data as Test 5 but uses setInlineMatch(true).                  */
/* Expected: COUNT=5, SUM=150                                          */
/* ------------------------------------------------------------------ */

static int
testPartialMatchScanScanInline(Ndb *ndb, MYSQL *conn)
{
  printf("Test 6: Scan-scan partial match COUNT/SUM (inline match) ... ");
  fflush(stdout);

  if (verifyScalarWithMysql(conn, "Test 6",
        "SELECT COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent_ss p LEFT JOIN oj_child_ss c "
        "ON c.join_col = p.join_col",
        {5, 150}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runScalarOuterJoinScanScan(ndb, "Test 6", 5, 150,
                                  true /*inline match*/) != 0) return -1;

  printf("OK (count=5, sum=150)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 7: Scan-scan no match COUNT/SUM                                */
/*                                                                     */
/* 10 parent, child join_col values all different.                     */
/* No children match → COUNT=0, SUM=0.                                */
/* ------------------------------------------------------------------ */

static int
testNoMatchScanScan(Ndb *ndb, MYSQL *conn)
{
  printf("Test 7: Scan-scan no match COUNT/SUM ... ");
  fflush(stdout);

  if (insertNoMatchScanScan(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 7",
        "SELECT COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent_ss p LEFT JOIN oj_child_ss c "
        "ON c.join_col = p.join_col",
        {0, 0}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runScalarOuterJoinScanScan(ndb, "Test 7", 0, 0,
                                  false /*bitmask*/) != 0) return -1;

  printf("OK (count=0, sum=0)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 8: Scan-scan partial match GROUP BY grp                        */
/*                                                                     */
/* Re-insert partial match data. COUNT counts matched children only.   */
/* grp 1: join_col 1,2,3 → 3 match, COUNT=3, SUM=60                  */
/* grp 2: join_col 4,5,6 → 2 match, COUNT=2, SUM=90                  */
/* grp 3: join_col 7,8,9 → 0 match, COUNT=0, SUM=0                   */
/* grp 4: join_col 10    → 0 match, COUNT=0, SUM=0                    */
/* ------------------------------------------------------------------ */

static int
testGroupByScanScan(Ndb *ndb, MYSQL *conn)
{
  printf("Test 8: Scan-scan partial match GROUP BY grp ... ");
  fflush(stdout);

  /* Re-insert partial match data */
  sqlExec(conn, "DELETE FROM oj_child_ss");
  sqlExec(conn, "DELETE FROM oj_parent_ss");
  if (insertPartialMatchScanScan(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {1, {3, 60}},
    {2, {2, 90}},
    {3, {0, 0}},
    {4, {0, 0}}
  };

  if (verifyGroupByWithMysql(conn, "Test 8",
        "SELECT p.grp, COUNT(c.pk), COALESCE(SUM(c.val),0) "
        "FROM oj_parent_ss p LEFT JOIN oj_child_ss c "
        "ON c.join_col = p.join_col "
        "GROUP BY p.grp ORDER BY p.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runGroupByOuterJoinScanScan(ndb, "Test 8", expected) != 0) return -1;

  printf("OK (4 groups)\n");
  return 0;
}

/* ================================================================== */
/* COUNT(*) tests (Tests 9-12)                                         */
/*                                                                     */
/* COUNT(*) in SQL counts ALL rows including null-extended outer join   */
/* rows. To achieve this in the NDB aggregation engine, use            */
/* LoadUint64(1, reg) instead of LoadColumn — a constant is never NULL */
/* so Count always increments, matching SQL COUNT(*) semantics.        */
/* ================================================================== */

/* ------------------------------------------------------------------ */
/* Helper: COUNT(*) scalar outer join scan-lookup                       */
/*                                                                     */
/* Agg program: LoadUint64(1) for COUNT(*), LoadColumn(val) for SUM.   */
/* COUNT(*) counts all rows including null-extended unmatched parents.  */
/* ------------------------------------------------------------------ */

static int
runCountStarOuterJoinLookup(Ndb *ndb, const char *testName,
                            Int64 expectedCount, Int64 expectedSum)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ_PARENT);
  dict->invalidateTable(OJ_CHILD);
  const NdbDictionary::Table *parentTab = dict->getTable(OJ_PARENT);
  const NdbDictionary::Table *childTab = dict->getTable(OJ_CHILD);
  if (parentTab == nullptr || childTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Aggregation: COUNT(*) via LoadUint64, SUM(val) via LoadColumn */
  NdbAggregator agg(childTab);
  if (!agg.LoadUint64(1, 0) ||       /* reg 0 = constant 1 (never NULL) */
      !agg.LoadColumn("val", 1) ||   /* reg 1 = val (NULL for unmatched) */
      !agg.Count(0, 0) ||            /* agg[0] = COUNT(*) */
      !agg.Sum(1, 1) ||              /* agg[1] = SUM(val) */
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *joinKey[] = {
    qb->linkedValue(parentOp, "pk"),
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

  V("\n  %s: COUNT(*)=%lld SUM=%lld\n",
    testName, (long long)actualCount, (long long)actualSum);

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualCount != expectedCount) {
    printf("FAILED (COUNT(*): expected %lld, got %lld)\n",
           (long long)expectedCount, (long long)actualCount);
    return -1;
  }
  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Helper: COUNT(*) scalar outer join scan-scan                         */
/* ------------------------------------------------------------------ */

static int
runCountStarOuterJoinScanScan(Ndb *ndb, const char *testName,
                              Int64 expectedCount, Int64 expectedSum,
                              bool useInlineMatch)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ_PARENT_SS);
  dict->invalidateTable(OJ_CHILD_SS);
  dict->invalidateIndex(OJ_CHILD_SS_IDX, OJ_CHILD_SS);
  const NdbDictionary::Table *parentTab = dict->getTable(OJ_PARENT_SS);
  const NdbDictionary::Table *childTab = dict->getTable(OJ_CHILD_SS);
  const NdbDictionary::Index *childIdx =
      dict->getIndex(OJ_CHILD_SS_IDX, OJ_CHILD_SS);
  if (parentTab == nullptr || childTab == nullptr || childIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator agg(childTab);
  if (!agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("val", 1) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 1) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  const NdbQueryTableScanOperationDef *parentOp = qb->scanTable(parentTab);
  if (parentOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(parentOp, "join_col"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions opts;
  opts.setAggregation(agg);
  if (useInlineMatch) {
    opts.setInlineMatch(true);
  }

  const NdbQueryIndexScanOperationDef *childOp =
      qb->scanIndex(childIdx, childTab, &bound, &opts);
  if (childOp == nullptr) {
    printf("FAILED (scanIndex: %s)\n", qb->getNdbError().message);
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

  V("\n  %s: COUNT(*)=%lld SUM=%lld (inline=%d)\n",
    testName, (long long)actualCount, (long long)actualSum, useInlineMatch);

  query->close();
  trans->close();
  queryDef->destroy();

  if (actualCount != expectedCount) {
    printf("FAILED (COUNT(*): expected %lld, got %lld)\n",
           (long long)expectedCount, (long long)actualCount);
    return -1;
  }
  if (actualSum != expectedSum) {
    printf("FAILED (SUM: expected %lld, got %lld)\n",
           (long long)expectedSum, (long long)actualSum);
    return -1;
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 9: Scan-lookup partial match COUNT(*)/SUM                      */
/*                                                                     */
/* Uses LoadUint64(1) for COUNT(*) — always non-NULL, so unmatched     */
/* parent rows also increment COUNT.                                   */
/* 10 parent, 5 child match → COUNT(*)=10, SUM=150.                   */
/* ------------------------------------------------------------------ */

static int
testCountStarPartialMatchLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 9: Scan-lookup COUNT(*) partial match ... ");
  fflush(stdout);

  /* Re-insert partial match data */
  sqlExec(conn, "DELETE FROM oj_child");
  sqlExec(conn, "DELETE FROM oj_parent");
  if (insertPartialMatchLookup(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 9",
        "SELECT COUNT(*), COALESCE(SUM(c.val),0) "
        "FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk",
        {10, 150}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runCountStarOuterJoinLookup(ndb, "Test 9", 10, 150) != 0) return -1;

  printf("OK (count=10, sum=150)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 10: Scan-lookup no match COUNT(*)                              */
/*                                                                     */
/* 5 parent, no child match → COUNT(*)=5, SUM=0.                      */
/* ------------------------------------------------------------------ */

static int
testCountStarNoMatchLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 10: Scan-lookup COUNT(*) no match ... ");
  fflush(stdout);

  if (insertNoMatchLookup(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 10",
        "SELECT COUNT(*), COALESCE(SUM(c.val),0) "
        "FROM oj_parent p LEFT JOIN oj_child c ON c.pk = p.pk",
        {5, 0}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runCountStarOuterJoinLookup(ndb, "Test 10", 5, 0) != 0) return -1;

  printf("OK (count=5, sum=0)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 11: Scan-scan partial match COUNT(*) (bitmask)                 */
/*                                                                     */
/* 10 parent, 5 child match → COUNT(*)=10, SUM=150.                   */
/* ------------------------------------------------------------------ */

static int
testCountStarPartialMatchScanScan(Ndb *ndb, MYSQL *conn)
{
  printf("Test 11: Scan-scan COUNT(*) partial match (bitmask) ... ");
  fflush(stdout);

  /* Re-insert partial match data */
  sqlExec(conn, "DELETE FROM oj_child_ss");
  sqlExec(conn, "DELETE FROM oj_parent_ss");
  if (insertPartialMatchScanScan(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 11",
        "SELECT COUNT(*), COALESCE(SUM(c.val),0) "
        "FROM oj_parent_ss p LEFT JOIN oj_child_ss c "
        "ON c.join_col = p.join_col",
        {10, 150}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runCountStarOuterJoinScanScan(ndb, "Test 11", 10, 150,
                                     false /*bitmask*/) != 0) return -1;

  printf("OK (count=10, sum=150)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 12: Scan-scan no match COUNT(*)                                */
/*                                                                     */
/* 10 parent, no child match → COUNT(*)=10, SUM=0.                    */
/* ------------------------------------------------------------------ */

static int
testCountStarNoMatchScanScan(Ndb *ndb, MYSQL *conn)
{
  printf("Test 12: Scan-scan COUNT(*) no match ... ");
  fflush(stdout);

  if (insertNoMatchScanScan(conn) != 0) {
    printf("FAILED (data setup)\n");
    return -1;
  }

  if (verifyScalarWithMysql(conn, "Test 12",
        "SELECT COUNT(*), COALESCE(SUM(c.val),0) "
        "FROM oj_parent_ss p LEFT JOIN oj_child_ss c "
        "ON c.join_col = p.join_col",
        {10, 0}) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  if (runCountStarOuterJoinScanScan(ndb, "Test 12", 10, 0,
                                     false /*bitmask*/) != 0) return -1;

  printf("OK (count=10, sum=0)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Usage and main                                                      */
/* ------------------------------------------------------------------ */

static void
usage(const char *prog)
{
  fprintf(stderr,
    "Usage: %s [-c connect_string] [-m mysql_port] [-v|--verbose]\n"
    "       [--only N] [--skip N]\n", prog);
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

  printf("=== testOuterJoinAggNdbApi ===\n");
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
          fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
          exitCode = 1;
        }
        else {
          /* Tests 1-4: Scan-lookup outer join */
          if (createLookupTables(conn) == 0 &&
              insertPartialMatchLookup(conn) == 0) {

            if (shouldRun(1)) {
              if (testPartialMatchLookup(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(2)) {
              if (testAllMatchLookup(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(3)) {
              if (testNoMatchLookup(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(4)) {
              if (testGroupByLookup(&ndb, conn) != 0) exitCode = 1;
            }

            /* Tests 9-10: COUNT(*) scan-lookup (reuse lookup tables) */
            if (shouldRun(9)) {
              if (testCountStarPartialMatchLookup(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(10)) {
              if (testCountStarNoMatchLookup(&ndb, conn) != 0) exitCode = 1;
            }
          } else {
            exitCode = 1;
          }
          dropLookupTables(conn);

          /* Tests 5-8: Scan-scan outer join */
          if (createScanScanTables(conn) == 0 &&
              insertPartialMatchScanScan(conn) == 0) {

            if (shouldRun(5)) {
              if (testPartialMatchScanScan(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(6)) {
              if (testPartialMatchScanScanInline(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(7)) {
              if (testNoMatchScanScan(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(8)) {
              if (testGroupByScanScan(&ndb, conn) != 0) exitCode = 1;
            }

            /* Tests 11-12: COUNT(*) scan-scan (reuse scan-scan tables) */
            if (shouldRun(11)) {
              if (testCountStarPartialMatchScanScan(&ndb, conn) != 0) exitCode = 1;
            }
            if (shouldRun(12)) {
              if (testCountStarNoMatchScanScan(&ndb, conn) != 0) exitCode = 1;
            }
          } else {
            exitCode = 1;
          }
          dropScanScanTables(conn);
        }

        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n=== %s ===\n", exitCode == 0 ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
  return exitCode;
}
