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
 * testMultiOuterJoinAggNdbApi — Integration tests for chained (3-way, 4-way)
 *                                outer join aggregation using the NdbQueryBuilder API.
 *
 * Tests multi-level LEFT OUTER JOIN aggregation pushdown through the NDB API:
 *   NdbQueryBuilder → NdbQueryDef → NdbQuery → getAggregator()
 *
 * These tests expose DBSPJ gaps in null row propagation for chained outer joins.
 * When intermediate (non-leaf) outer join nodes get no match, they must propagate
 * null rows to their children for correct aggregation. Currently only 2-way outer
 * joins work correctly (see testOuterJoinAggNdbApi).
 *
 * Test 1: 3-way outer join (scan → LEFT lookup → LEFT lookup)
 *         GROUP BY dept.name, COUNT(*), SUM(task.hours)
 *
 * Test 2: 4-way mixed join (scan → LEFT lookup → INNER lookup → LEFT lookup)
 *         GROUP BY region.area, COUNT(*), SUM(review.rating)
 *
 * Usage: testMultiOuterJoinAggNdbApi -c <connect_string> -m <mysql_port> [-v]
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

/* Test 1: 3-way outer join tables */
static const char *OJ3_DEPT = "oj3_dept";
static const char *OJ3_EMP  = "oj3_emp";
static const char *OJ3_TASK = "oj3_task";

/* Test 2: 4-way mixed join tables */
static const char *OJ4_REGION  = "oj4_region";
static const char *OJ4_STORE   = "oj4_store";
static const char *OJ4_PRODUCT = "oj4_product";
static const char *OJ4_REVIEW  = "oj4_review";

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
/* GROUP BY verification: query returns (grp, val1, val2) rows         */
/*                                                                     */
/* val2 can be NULL (for SUM with no matches) — treated as 0.         */
/* ------------------------------------------------------------------ */

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
    V("  MySQL: grp=%d val1=%lld val2=%lld\n",
      grp, (long long)val1, (long long)val2);
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

/* ================================================================== */
/* Test 1: 3-Way Outer Join (scan → LEFT lookup → LEFT lookup)         */
/*                                                                     */
/* Schema:                                                             */
/*   oj3_dept (id INT PK, name INT)                                    */
/*   oj3_emp  (dept_id INT PK, salary BIGINT)                         */
/*   oj3_task (emp_id INT PK, hours BIGINT)                           */
/*                                                                     */
/* Data:                                                               */
/*   dept: (1,10),(2,20),(3,30),(4,40),(5,50) — 5 depts                */
/*   emp:  (1,1000),(2,2000),(3,3000) — 3 emps for depts 1,2,3        */
/*   task: (1,8),(2,12) — 2 tasks for emps 1,2                        */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.name, COUNT(*), COALESCE(SUM(t.hours),0)                 */
/*   FROM oj3_dept d                                                   */
/*   LEFT JOIN oj3_emp e ON e.dept_id = d.id                           */
/*   LEFT JOIN oj3_task t ON t.emp_id = e.dept_id                      */
/*   GROUP BY d.name ORDER BY d.name                                   */
/*                                                                     */
/* Expected results:                                                   */
/*   name=10: dept 1 → emp 1 → task 1 (hours=8)  → COUNT=1, SUM=8    */
/*   name=20: dept 2 → emp 2 → task 2 (hours=12) → COUNT=1, SUM=12   */
/*   name=30: dept 3 → emp 3 → no task           → COUNT=1, SUM=0    */
/*   name=40: dept 4 → no emp → no task           → COUNT=1, SUM=0   */
/*   name=50: dept 5 → no emp → no task           → COUNT=1, SUM=0   */
/*                                                                     */
/* Known limitation: depts 4,5 will likely be missing from pushdown    */
/* results because intermediate null rows (emp not found) are not      */
/* propagated to the leaf node (task) for aggregation.                 */
/* ================================================================== */

static int
createTest1Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj3_task");
  sqlExec(conn, "DROP TABLE IF EXISTS oj3_emp");
  sqlExec(conn, "DROP TABLE IF EXISTS oj3_dept");

  if (sqlExec(conn,
        "CREATE TABLE oj3_dept ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  name INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ3_DEPT);

  if (sqlExec(conn,
        "CREATE TABLE oj3_emp ("
        "  dept_id INT NOT NULL PRIMARY KEY,"
        "  salary BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ3_EMP);

  if (sqlExec(conn,
        "CREATE TABLE oj3_task ("
        "  emp_id INT NOT NULL PRIMARY KEY,"
        "  hours BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ3_TASK);

  return 0;
}

static int
insertTest1Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj3_dept VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50)") != 0) return -1;
  V("Inserted 5 dept rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj3_emp VALUES "
        "(1,1000),(2,2000),(3,3000)") != 0) return -1;
  V("Inserted 3 emp rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj3_task VALUES "
        "(1,8),(2,12)") != 0) return -1;
  V("Inserted 2 task rows\n");

  return 0;
}

static int
dropTest1Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj3_task");
  sqlExec(conn, "DROP TABLE IF EXISTS oj3_emp");
  sqlExec(conn, "DROP TABLE IF EXISTS oj3_dept");
  V("Dropped test 1 tables\n");
  return 0;
}

static int
testThreeWayOuterJoin(Ndb *ndb, MYSQL *conn)
{
  printf("Test 1: 3-way outer join GROUP BY (scan→LEFT→LEFT) ... ");
  fflush(stdout);

  /* Expected: 5 groups, one per dept */
  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {1, 8}},    /* dept 1: emp 1, task(8) */
    {20, {1, 12}},   /* dept 2: emp 2, task(12) */
    {30, {1, 0}},    /* dept 3: emp 3, no task → COUNT=1, SUM=0 */
    {40, {1, 0}},    /* dept 4: no emp → COUNT=1, SUM=0 */
    {50, {1, 0}}     /* dept 5: no emp → COUNT=1, SUM=0 */
  };

  /* Verify with MySQL first */
  if (verifyGroupByWithMysql(conn, "Test 1",
        "SELECT d.name, COUNT(*), COALESCE(SUM(t.hours),0) "
        "FROM oj3_dept d "
        "LEFT JOIN oj3_emp e ON e.dept_id = d.id "
        "LEFT JOIN oj3_task t ON t.emp_id = e.dept_id "
        "GROUP BY d.name ORDER BY d.name",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ3_DEPT);
  dict->invalidateTable(OJ3_EMP);
  dict->invalidateTable(OJ3_TASK);
  const NdbDictionary::Table *deptTab = dict->getTable(OJ3_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(OJ3_EMP);
  const NdbDictionary::Table *taskTab = dict->getTable(OJ3_TASK);
  if (deptTab == nullptr || empTab == nullptr || taskTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *nameCol = deptTab->getColumn("name");
  if (nameCol == nullptr) {
    printf("FAILED (column lookup: name)\n");
    return -1;
  }

  /* Aggregation program on the leaf (task):
   *   GROUP BY linked position 0 (= dept.name)
   *   LoadUint64(1) for COUNT(*)
   *   LoadColumn(hours) for SUM(hours)
   */
  NdbAggregator agg(taskTab);
  if (!agg.GroupByLinked(0, nameCol) ||
      !agg.LoadUint64(1, 0) ||          /* reg 0 = constant 1 for COUNT(*) */
      !agg.LoadColumn("hours", 1) ||    /* reg 1 = hours */
      !agg.Count(0, 0) ||              /* agg[0] = COUNT(*) */
      !agg.Sum(1, 1) ||                /* agg[1] = SUM(hours) */
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Node 0: scan oj3_dept */
  const NdbQueryTableScanOperationDef *deptOp = qb->scanTable(deptTab);
  if (deptOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj3_emp (LEFT JOIN — default MatchAll) */
  const NdbQueryOperand *empKey[] = {
    qb->linkedValue(deptOp, "id"),
    nullptr
  };
  /* No setMatchType → MatchAll (outer join) */
  const NdbQueryLookupOperationDef *empOp =
      qb->readTuple(empTab, empKey, nullptr);
  if (empOp == nullptr) {
    printf("FAILED (readTuple emp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj3_task (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *taskKey[] = {
    qb->linkedValue(empOp, "dept_id"),
    nullptr
  };

  NdbQueryOptions taskOpts;
  taskOpts.setAggregation(agg);
  const NdbLinkedOperand *nameLink = qb->linkedValue(deptOp, "name");
  if (nameLink == nullptr) {
    printf("FAILED (linkedValue name: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  taskOpts.addLinkedProjection(nameLink);

  const NdbQueryLookupOperationDef *taskOp =
      qb->readTuple(taskTab, taskKey, &taskOpts);
  if (taskOp == nullptr) {
    printf("FAILED (readTuple task: %s)\n", qb->getNdbError().message);
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
    V("  NDB: name=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Compare results — report KNOWN LIMITATION instead of FAILED for
   * expected failures due to chained outer join gaps */
  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu — "
           "intermediate null rows not propagated)\n",
           expected.size(), actual.size());
    /* Print what we got */
    for (const auto &a : actual) {
      printf("  got: name=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    /* Print what's missing */
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: name=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("KNOWN LIMITATION (missing group name=%d — "
             "intermediate null rows not propagated)\n", e.first);
      return -1;
    }
    if (it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group name=%d: expected COUNT=%lld SUM=%lld, "
             "got COUNT=%lld SUM=%lld)\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (5 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 2: 4-Way Mixed Join                                            */
/*   scan → LEFT lookup → INNER lookup → LEFT lookup                   */
/*                                                                     */
/* Schema:                                                             */
/*   oj4_region  (id INT PK, area INT)                                 */
/*   oj4_store   (region_id INT PK, capacity INT)                     */
/*   oj4_product (store_id INT PK, price BIGINT)                      */
/*   oj4_review  (product_id INT PK, rating BIGINT)                   */
/*                                                                     */
/* Data:                                                               */
/*   region:  (1,100),(2,200),(3,300),(4,400),(5,500) — 5 regions      */
/*   store:   (1,50),(2,80),(3,120),(4,200) — 4 stores, region 5 none  */
/*   product: (1,1000),(2,2000),(3,3000) — 3 products, store 4 none   */
/*   review:  (1,5),(2,3) — 2 reviews, product 3 none                 */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.area, COUNT(*), COALESCE(SUM(v.rating),0)                */
/*   FROM oj4_region r                                                 */
/*   LEFT JOIN oj4_store s ON s.region_id = r.id                       */
/*   JOIN oj4_product p ON p.store_id = s.region_id                    */
/*   LEFT JOIN oj4_review v ON v.product_id = p.store_id               */
/*   GROUP BY r.area ORDER BY r.area                                   */
/*                                                                     */
/* Expected results:                                                   */
/*   area=100: region 1→store 1→product 1→review(5) → COUNT=1, SUM=5  */
/*   area=200: region 2→store 2→product 2→review(3) → COUNT=1, SUM=3  */
/*   area=300: region 3→store 3→product 3→no review → COUNT=1, SUM=0  */
/*   area=400: region 4→store 4→no product→(INNER filters) → COUNT=1, SUM=0 */
/*   area=500: region 5→no store→(LEFT null)→(INNER filters) → COUNT=1, SUM=0 */
/*                                                                     */
/* Note: The INNER JOIN at node 2 filters out null-extended store rows  */
/* from node 1. So region 5 (no store) produces a LEFT null at node 1, */
/* but the INNER at node 2 should filter it. MySQL COUNT(*) for regions */
/* 4 and 5 is actually 1 each because of the LEFT JOIN semantics at    */
/* the outermost level... Let's verify with MySQL to get exact results. */
/* ================================================================== */

static int
createTest2Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_review");
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_product");
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_store");
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_region");

  if (sqlExec(conn,
        "CREATE TABLE oj4_region ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  area INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ4_REGION);

  if (sqlExec(conn,
        "CREATE TABLE oj4_store ("
        "  region_id INT NOT NULL PRIMARY KEY,"
        "  capacity INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ4_STORE);

  if (sqlExec(conn,
        "CREATE TABLE oj4_product ("
        "  store_id INT NOT NULL PRIMARY KEY,"
        "  price BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ4_PRODUCT);

  if (sqlExec(conn,
        "CREATE TABLE oj4_review ("
        "  product_id INT NOT NULL PRIMARY KEY,"
        "  rating BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ4_REVIEW);

  return 0;
}

static int
insertTest2Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj4_region VALUES "
        "(1,100),(2,200),(3,300),(4,400),(5,500)") != 0) return -1;
  V("Inserted 5 region rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj4_store VALUES "
        "(1,50),(2,80),(3,120),(4,200)") != 0) return -1;
  V("Inserted 4 store rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj4_product VALUES "
        "(1,1000),(2,2000),(3,3000)") != 0) return -1;
  V("Inserted 3 product rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj4_review VALUES "
        "(1,5),(2,3)") != 0) return -1;
  V("Inserted 2 review rows\n");

  return 0;
}

static int
dropTest2Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_review");
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_product");
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_store");
  sqlExec(conn, "DROP TABLE IF EXISTS oj4_region");
  V("Dropped test 2 tables\n");
  return 0;
}

static int
testFourWayMixedJoin(Ndb *ndb, MYSQL *conn)
{
  printf("Test 2: 4-way mixed join GROUP BY (scan→LEFT→INNER→LEFT) ... ");
  fflush(stdout);

  /* First, determine expected results from MySQL.
   *
   * The query is:
   *   SELECT r.area, COUNT(*), COALESCE(SUM(v.rating),0)
   *   FROM oj4_region r
   *   LEFT JOIN oj4_store s ON s.region_id = r.id
   *   JOIN oj4_product p ON p.store_id = s.region_id
   *   LEFT JOIN oj4_review v ON v.product_id = p.store_id
   *   GROUP BY r.area ORDER BY r.area
   *
   * MySQL semantics: LEFT JOIN store, then INNER JOIN product, then LEFT JOIN review.
   * - region 5: no store → LEFT null → INNER product fails → row filtered by INNER
   *   BUT LEFT JOIN semantics at outer level: region 5 row is NOT preserved because
   *   the INNER JOIN is nested inside the LEFT JOIN scope.
   *
   * Actually in standard SQL, the join order matters:
   *   r LEFT JOIN s ... JOIN p ... LEFT JOIN v ...
   * is parsed as:
   *   ((r LEFT JOIN s ON ...) JOIN p ON ...) LEFT JOIN v ON ...
   *
   * So: r LEFT JOIN s gives region 5 with NULL store. Then INNER JOIN p
   * on s.region_id (which is NULL) → no match → region 5 filtered out.
   * Region 4: store 4 exists, product store_id=4 → no match → filtered out.
   * Region 3: store 3, product store_id=3 → match, review product_id=3 → no match → (3,NULL)
   * Region 2: store 2, product store_id=2 → match, review product_id=2 → rating=3
   * Region 1: store 1, product store_id=1 → match, review product_id=1 → rating=5
   *
   * Expected: 3 groups: area=100(1,5), area=200(1,3), area=300(1,0)
   */

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {100, {1, 5}},    /* region 1 → store 1 → product 1 → review(5) */
    {200, {1, 3}},    /* region 2 → store 2 → product 2 → review(3) */
    {300, {1, 0}}     /* region 3 → store 3 → product 3 → no review */
  };

  /* Verify with MySQL first */
  if (verifyGroupByWithMysql(conn, "Test 2",
        "SELECT r.area, COUNT(*), COALESCE(SUM(v.rating),0) "
        "FROM oj4_region r "
        "LEFT JOIN oj4_store s ON s.region_id = r.id "
        "JOIN oj4_product p ON p.store_id = s.region_id "
        "LEFT JOIN oj4_review v ON v.product_id = p.store_id "
        "GROUP BY r.area ORDER BY r.area",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ4_REGION);
  dict->invalidateTable(OJ4_STORE);
  dict->invalidateTable(OJ4_PRODUCT);
  dict->invalidateTable(OJ4_REVIEW);
  const NdbDictionary::Table *regionTab = dict->getTable(OJ4_REGION);
  const NdbDictionary::Table *storeTab = dict->getTable(OJ4_STORE);
  const NdbDictionary::Table *productTab = dict->getTable(OJ4_PRODUCT);
  const NdbDictionary::Table *reviewTab = dict->getTable(OJ4_REVIEW);
  if (regionTab == nullptr || storeTab == nullptr ||
      productTab == nullptr || reviewTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *areaCol = regionTab->getColumn("area");
  if (areaCol == nullptr) {
    printf("FAILED (column lookup: area)\n");
    return -1;
  }

  /* Aggregation program on the leaf (review):
   *   GROUP BY linked position 0 (= region.area)
   *   LoadUint64(1) for COUNT(*)
   *   LoadColumn(rating) for SUM(rating)
   */
  NdbAggregator agg(reviewTab);
  if (!agg.GroupByLinked(0, areaCol) ||
      !agg.LoadUint64(1, 0) ||            /* reg 0 = constant 1 for COUNT(*) */
      !agg.LoadColumn("rating", 1) ||     /* reg 1 = rating */
      !agg.Count(0, 0) ||                /* agg[0] = COUNT(*) */
      !agg.Sum(1, 1) ||                  /* agg[1] = SUM(rating) */
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (NdbQueryBuilder::create)\n");
    return -1;
  }

  /* Node 0: scan oj4_region */
  const NdbQueryTableScanOperationDef *regionOp = qb->scanTable(regionTab);
  if (regionOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj4_store (LEFT JOIN — default MatchAll) */
  const NdbQueryOperand *storeKey[] = {
    qb->linkedValue(regionOp, "id"),
    nullptr
  };
  /* No setMatchType → MatchAll (outer join) */
  const NdbQueryLookupOperationDef *storeOp =
      qb->readTuple(storeTab, storeKey, nullptr);
  if (storeOp == nullptr) {
    printf("FAILED (readTuple store: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj4_product (INNER JOIN — MatchNonNull) */
  const NdbQueryOperand *productKey[] = {
    qb->linkedValue(storeOp, "region_id"),
    nullptr
  };
  NdbQueryOptions productOpts;
  productOpts.setMatchType(NdbQueryOptions::MatchNonNull);

  const NdbQueryLookupOperationDef *productOp =
      qb->readTuple(productTab, productKey, &productOpts);
  if (productOp == nullptr) {
    printf("FAILED (readTuple product: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj4_review (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *reviewKey[] = {
    qb->linkedValue(productOp, "store_id"),
    nullptr
  };

  NdbQueryOptions reviewOpts;
  reviewOpts.setAggregation(agg);
  const NdbLinkedOperand *areaLink = qb->linkedValue(regionOp, "area");
  if (areaLink == nullptr) {
    printf("FAILED (linkedValue area: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  reviewOpts.addLinkedProjection(areaLink);

  const NdbQueryLookupOperationDef *reviewOp =
      qb->readTuple(reviewTab, reviewKey, &reviewOpts);
  if (reviewOp == nullptr) {
    printf("FAILED (readTuple review: %s)\n", qb->getNdbError().message);
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
    V("  NDB: area=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Compare results — report KNOWN LIMITATION for expected failures */
  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu — "
           "chained outer+inner join aggregation gap)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: area=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: area=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end()) {
      printf("KNOWN LIMITATION (missing group area=%d)\n", e.first);
      return -1;
    }
    if (it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group area=%d: expected COUNT=%lld SUM=%lld, "
             "got COUNT=%lld SUM=%lld)\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (3 groups)\n");
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

  printf("=== testMultiOuterJoinAggNdbApi ===\n");
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
          /* Test 1: 3-way outer join */
          if (shouldRun(1)) {
            if (createTest1Tables(conn) == 0 &&
                insertTest1Data(conn) == 0) {
              if (testThreeWayOuterJoin(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest1Tables(conn);
          }

          /* Test 2: 4-way mixed join */
          if (shouldRun(2)) {
            if (createTest2Tables(conn) == 0 &&
                insertTest2Data(conn) == 0) {
              if (testFourWayMixedJoin(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest2Tables(conn);
          }
        }

        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  printf("\n=== %s ===\n", exitCode == 0 ? "ALL TESTS PASSED" : "SOME TESTS FAILED");
  return exitCode;
}
