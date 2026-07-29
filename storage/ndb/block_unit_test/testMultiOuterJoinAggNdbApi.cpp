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
 * Test 1: 3-way outer join (scan -> LEFT lookup -> LEFT lookup)
 *         GROUP BY dept.name, COUNT(*), SUM(task.hours)
 *
 * Test 2: 4-way mixed join (scan -> LEFT lookup -> INNER lookup -> LEFT lookup)
 *         GROUP BY region.area, COUNT(*), SUM(review.rating)
 *
 * Test 3: 5-way deep all-lookup (scan -> LEFT lkp -> LEFT lkp -> LEFT lkp -> LEFT lkp)
 *         GROUP BY grp, COUNT(*), SUM(hours)
 *
 * Test 4: scan-scan-lookup (scan -> LEFT scan -> LEFT lookup)
 *         GROUP BY grp, COUNT(*), SUM(hours)
 *
 * Test 5: lookup then scan leaf (scan -> LEFT lookup -> LEFT scan)
 *         GROUP BY grp, COUNT(*), SUM(hours)
 *
 * Test 6: two scan intermediates (scan -> LEFT scan -> LEFT scan -> LEFT lookup)
 *         GROUP BY grp, COUNT(*), SUM(hours)
 *
 * Test 7: scan intermediate + inner blocker (scan -> LEFT scan -> INNER lkp -> LEFT lkp)
 *         GROUP BY grp, COUNT(*), SUM(hours)
 *
 * Test 8: GROUP BY intermediate column (scan -> LEFT lookup -> LEFT lookup)
 *         GROUP BY mid.category, COUNT(*), SUM(leaf.value)
 *
 * Test 9: GROUP BY root + intermediate (scan -> LEFT lookup -> LEFT lookup)
 *         GROUP BY root.grp, mid.category, COUNT(*), SUM(leaf.value)
 *
 * Test 10: GROUP BY deep intermediate (scan -> LEFT lkp -> LEFT lkp -> LEFT lkp)
 *          GROUP BY b.b_cat, COUNT(*), SUM(leaf.score)
 *
 * Test 11: Sibling branch + aggregation (scan -> LEFT lkp + LEFT lkp -> LEFT lkp)
 *          GROUP BY items.category, COUNT(*), SUM(shipment.cost)
 *
 * Test 12: Sibling branch + GROUP BY root (scan -> LEFT lkp + LEFT lkp -> LEFT lkp)
 *          GROUP BY orders.priority, COUNT(*), SUM(shipment.cost)
 *
 * Test 13: 3-scan deferred null row (scan -> LEFT scan -> LEFT scan)
 *          GROUP BY grp, COUNT(*), SUM(hours)
 *          Tests deferred null row injection for scan aggregate leaves.
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

/* Test 3: 5-way deep all-lookup tables */
static const char *OJ5_ROOT = "oj5_root";
static const char *OJ5_A    = "oj5_a";
static const char *OJ5_B    = "oj5_b";
static const char *OJ5_C    = "oj5_c";
static const char *OJ5_D    = "oj5_d";

/* Test 4: scan-scan-lookup tables */
static const char *OJ6_ROOT     = "oj6_root";
static const char *OJ6_MID      = "oj6_mid";
static const char *OJ6_MID_IDX  = "ix_oj6_root";
static const char *OJ6_LEAF     = "oj6_leaf";

/* Test 5: lookup then scan leaf tables */
static const char *OJ7_ROOT     = "oj7_root";
static const char *OJ7_MID      = "oj7_mid";
static const char *OJ7_LEAF     = "oj7_leaf";
static const char *OJ7_LEAF_IDX = "ix_oj7_mid";

/* Test 6: two scan intermediates tables */
static const char *OJ8_ROOT     = "oj8_root";
static const char *OJ8_A        = "oj8_a";
static const char *OJ8_A_IDX    = "ix_oj8_root";
static const char *OJ8_B        = "oj8_b";
static const char *OJ8_B_IDX    = "ix_oj8_a";
static const char *OJ8_LEAF     = "oj8_leaf";

/* Test 7: scan intermediate + inner blocker tables */
static const char *OJ9_ROOT      = "oj9_root";
static const char *OJ9_MID       = "oj9_mid";
static const char *OJ9_MID_IDX   = "ix_oj9_root";
static const char *OJ9_INNER     = "oj9_inner";
static const char *OJ9_LEAF      = "oj9_leaf";

/* Test 8: GROUP BY intermediate column (3-way) */
static const char *OJ10_ROOT = "oj10_root";
static const char *OJ10_MID  = "oj10_mid";
static const char *OJ10_LEAF = "oj10_leaf";

/* Test 10: GROUP BY intermediate in 4-way chain */
static const char *OJ12_ROOT = "oj12_root";
static const char *OJ12_A    = "oj12_a";
static const char *OJ12_B    = "oj12_b";
static const char *OJ12_LEAF = "oj12_leaf";

/* Tests 11-12: sibling outer join branch tables */
static const char *OJ13_ORDERS   = "oj13_orders";
static const char *OJ13_DETAILS  = "oj13_details";
static const char *OJ13_ITEMS    = "oj13_items";
static const char *OJ13_SHIPMENT = "oj13_shipment";

/* Tests 13-14: 3-scan deferred null row tables */
static const char *OJ14_ROOT     = "oj14_root";
static const char *OJ14_MID      = "oj14_mid";
static const char *OJ14_MID_IDX  = "ix_oj14_root";
static const char *OJ14_LEAF     = "oj14_leaf";
static const char *OJ14_LEAF_IDX = "ix_oj14_mid";

/* Sentinel for NULL group-by values */
static const Int32 NULL_GROUP = INT_MIN;

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

/* ================================================================== */
/* Test 3: 5-Way Deep All-Lookup                                       */
/*   scan -> LEFT lkp -> LEFT lkp -> LEFT lkp -> LEFT lkp [leaf]       */
/*                                                                     */
/* Schema:                                                             */
/*   oj5_root (id INT PK, grp INT)                                     */
/*   oj5_a    (root_id INT PK, val_a BIGINT)                           */
/*   oj5_b    (a_id INT PK, val_b BIGINT)                              */
/*   oj5_c    (b_id INT PK, val_c BIGINT)                              */
/*   oj5_d    (c_id INT PK, hours BIGINT)                              */
/*                                                                     */
/* Data: 6 roots, 4 in A, 3 in B, 2 in C, 1 in D                      */
/*   root: (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)                    */
/*   a:    (1,100),(2,200),(3,300),(4,400)                              */
/*   b:    (1,10),(2,20),(3,30)                                         */
/*   c:    (1,1),(2,2)                                                  */
/*   d:    (1,100)                                                      */
/*                                                                     */
/* Expected: 6 groups, all should be present via Phase 2 propagation   */
/*   grp=10: root1->a1->b1->c1->d1(100) -> COUNT=1, SUM=100           */
/*   grp=20: root2->a2->b2->c2->no d    -> COUNT=1, SUM=0             */
/*   grp=30: root3->a3->b3->no c        -> COUNT=1, SUM=0             */
/*   grp=40: root4->a4->no b            -> COUNT=1, SUM=0             */
/*   grp=50: root5->no a                -> COUNT=1, SUM=0             */
/*   grp=60: root6->no a                -> COUNT=1, SUM=0             */
/* ================================================================== */

static int
createTest3Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_d");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_c");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_b");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_a");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_root");

  if (sqlExec(conn,
        "CREATE TABLE oj5_root ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj5_a ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  val_a BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj5_b ("
        "  a_id INT NOT NULL PRIMARY KEY,"
        "  val_b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj5_c ("
        "  b_id INT NOT NULL PRIMARY KEY,"
        "  val_c BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj5_d ("
        "  c_id INT NOT NULL PRIMARY KEY,"
        "  hours BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 3 tables\n");
  return 0;
}

static int
insertTest3Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj5_root VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50),(6,60)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj5_a VALUES "
        "(1,100),(2,200),(3,300),(4,400)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj5_b VALUES "
        "(1,10),(2,20),(3,30)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj5_c VALUES "
        "(1,1),(2,2)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj5_d VALUES (1,100)") != 0) return -1;
  V("Inserted test 3 data\n");
  return 0;
}

static int
dropTest3Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_d");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_c");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_b");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_a");
  sqlExec(conn, "DROP TABLE IF EXISTS oj5_root");
  V("Dropped test 3 tables\n");
  return 0;
}

static int
testFiveWayDeepLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 3: 5-way deep lookup GROUP BY "
         "(scan->LEFT->LEFT->LEFT->LEFT) ... ");
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {1, 100}},   /* root1->a1->b1->c1->d1(100) */
    {20, {1, 0}},     /* root2->a2->b2->c2->no d */
    {30, {1, 0}},     /* root3->a3->b3->no c */
    {40, {1, 0}},     /* root4->a4->no b */
    {50, {1, 0}},     /* root5->no a */
    {60, {1, 0}}      /* root6->no a */
  };

  if (verifyGroupByWithMysql(conn, "Test 3",
        "SELECT r.grp, COUNT(*), COALESCE(SUM(d.hours),0) "
        "FROM oj5_root r "
        "LEFT JOIN oj5_a a ON a.root_id = r.id "
        "LEFT JOIN oj5_b b ON b.a_id = a.root_id "
        "LEFT JOIN oj5_c c ON c.b_id = b.a_id "
        "LEFT JOIN oj5_d d ON d.c_id = c.b_id "
        "GROUP BY r.grp ORDER BY r.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ5_ROOT);
  dict->invalidateTable(OJ5_A);
  dict->invalidateTable(OJ5_B);
  dict->invalidateTable(OJ5_C);
  dict->invalidateTable(OJ5_D);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ5_ROOT);
  const NdbDictionary::Table *aTab = dict->getTable(OJ5_A);
  const NdbDictionary::Table *bTab = dict->getTable(OJ5_B);
  const NdbDictionary::Table *cTab = dict->getTable(OJ5_C);
  const NdbDictionary::Table *dTab = dict->getTable(OJ5_D);
  if (rootTab == nullptr || aTab == nullptr || bTab == nullptr ||
      cTab == nullptr || dTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");

  /* Aggregation on leaf (oj5_d) */
  NdbAggregator agg(dTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("hours", 1) ||
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

  /* Node 0: scan oj5_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj5_a (LEFT JOIN) */
  const NdbQueryOperand *aKey[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *aOp =
      qb->readTuple(aTab, aKey, nullptr);
  if (aOp == nullptr) {
    printf("FAILED (readTuple a: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj5_b (LEFT JOIN) */
  const NdbQueryOperand *bKey[] = {
    qb->linkedValue(aOp, "root_id"), nullptr
  };
  const NdbQueryLookupOperationDef *bOp =
      qb->readTuple(bTab, bKey, nullptr);
  if (bOp == nullptr) {
    printf("FAILED (readTuple b: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj5_c (LEFT JOIN) */
  const NdbQueryOperand *cKey[] = {
    qb->linkedValue(bOp, "a_id"), nullptr
  };
  const NdbQueryLookupOperationDef *cOp =
      qb->readTuple(cTab, cKey, nullptr);
  if (cOp == nullptr) {
    printf("FAILED (readTuple c: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 4: lookup oj5_d (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *dKey[] = {
    qb->linkedValue(cOp, "b_id"), nullptr
  };

  NdbQueryOptions dOpts;
  dOpts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  dOpts.addLinkedProjection(grpLink);

  const NdbQueryLookupOperationDef *dOp =
      qb->readTuple(dTab, dKey, &dOpts);
  if (dOp == nullptr) {
    printf("FAILED (readTuple d: %s)\n", qb->getNdbError().message);
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
    V("  NDB: grp=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: grp=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: grp=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group grp=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (6 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 4: Scan-Scan-Lookup (scan -> LEFT scan -> LEFT lookup [leaf])   */
/*                                                                     */
/* Schema:                                                             */
/*   oj6_root (pk INT PK, grp INT, join_col INT)                       */
/*   oj6_mid  (pk INT PK, root_id INT, val INT, INDEX ix_oj6_root(root_id)) */
/*   oj6_leaf (mid_pk INT PK, hours BIGINT)                            */
/*                                                                     */
/* Data:                                                               */
/*   root: (1,10,1),(2,20,2),(3,30,3),(4,40,4)                         */
/*   mid:  (1,1,100),(2,1,200),(3,2,300) -- root 1 has 2, root 2 has 1 */
/*   leaf: (1,10),(2,20)                  -- mid 1,2 have leaf          */
/*                                                                     */
/* Expected: Phase 4 NOT implemented for scan intermediates            */
/*   grp=10: root1->mid(1,2)->leaf(10,20)  -> COUNT=2, SUM=30         */
/*   grp=20: root2->mid(3)->leaf(none)     -> COUNT=1, SUM=0          */
/*   grp=30: root3->no mid                 -> COUNT=1, SUM=0          */
/*   grp=40: root4->no mid                 -> COUNT=1, SUM=0          */
/* ================================================================== */

static int
createTest4Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj6_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj6_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj6_root");

  if (sqlExec(conn,
        "CREATE TABLE oj6_root ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL,"
        "  join_col INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj6_mid ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  root_id INT NOT NULL,"
        "  val INT NOT NULL,"
        "  INDEX ix_oj6_root (root_id)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj6_leaf ("
        "  mid_pk INT NOT NULL PRIMARY KEY,"
        "  hours BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 4 tables\n");
  return 0;
}

static int
insertTest4Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj6_root VALUES "
        "(1,10,1),(2,20,2),(3,30,3),(4,40,4)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj6_mid VALUES "
        "(1,1,100),(2,1,200),(3,2,300)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj6_leaf VALUES "
        "(1,10),(2,20)") != 0) return -1;
  V("Inserted test 4 data\n");
  return 0;
}

static int
dropTest4Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj6_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj6_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj6_root");
  V("Dropped test 4 tables\n");
  return 0;
}

static int
testScanScanLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 4: scan-scan-lookup GROUP BY "
         "(scan->LEFT scan->LEFT lkp) ... ");
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {2, 30}},    /* root1: mid(1,2)->leaf(10,20) */
    {20, {1, 0}},     /* root2: mid(3)->no leaf */
    {30, {1, 0}},     /* root3: no mid */
    {40, {1, 0}}      /* root4: no mid */
  };

  if (verifyGroupByWithMysql(conn, "Test 4",
        "SELECT r.grp, COUNT(*), COALESCE(SUM(l.hours),0) "
        "FROM oj6_root r "
        "LEFT JOIN oj6_mid m ON m.root_id = r.join_col "
        "LEFT JOIN oj6_leaf l ON l.mid_pk = m.pk "
        "GROUP BY r.grp ORDER BY r.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ6_ROOT);
  dict->invalidateTable(OJ6_MID);
  dict->invalidateTable(OJ6_LEAF);
  dict->invalidateIndex(OJ6_MID_IDX, OJ6_MID);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ6_ROOT);
  const NdbDictionary::Table *midTab = dict->getTable(OJ6_MID);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ6_LEAF);
  const NdbDictionary::Index *midIdx = dict->getIndex(OJ6_MID_IDX, OJ6_MID);
  if (rootTab == nullptr || midTab == nullptr || leafTab == nullptr ||
      midIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");

  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("hours", 1) ||
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

  /* Node 0: scan oj6_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: scanIndex oj6_mid (LEFT JOIN - scan child) */
  const NdbQueryOperand *midBound[] = {
    qb->linkedValue(rootOp, "join_col"), nullptr
  };
  NdbQueryIndexBound midBoundObj(midBound);

  /* No setMatchType -> MatchAll (outer join) */
  const NdbQueryIndexScanOperationDef *midOp =
      qb->scanIndex(midIdx, midTab, &midBoundObj, nullptr);
  if (midOp == nullptr) {
    printf("FAILED (scanIndex mid: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj6_leaf (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(midOp, "pk"), nullptr
  };

  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(grpLink);

  const NdbQueryLookupOperationDef *leafOp =
      qb->readTuple(leafTab, leafKey, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (readTuple leaf: %s)\n", qb->getNdbError().message);
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
    V("  NDB: grp=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu - "
           "Phase 4 scan intermediate not implemented)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: grp=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: grp=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group grp=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (4 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 5: Lookup then Scan Leaf                                       */
/*   scan -> LEFT lookup -> LEFT scan [leaf]                            */
/*                                                                     */
/* Schema:                                                             */
/*   oj7_root (id INT PK, grp INT)                                     */
/*   oj7_mid  (root_id INT PK, mid_val INT)                            */
/*   oj7_leaf (pk INT PK, mid_id INT, hours BIGINT,                    */
/*             INDEX ix_oj7_mid(mid_id))                               */
/*                                                                     */
/* Data:                                                               */
/*   root: (1,10),(2,20),(3,30),(4,40)                                  */
/*   mid:  (1,100),(2,200)              -- root 3,4 have no mid        */
/*   leaf: (1,1,10),(2,1,20),(3,2,30)   -- mid 1 has 2 leaves          */
/*                                                                     */
/* Expected: Phase 2 handles lookup intermediate,                      */
/*           existing scanFrag_complete handles scan leaf               */
/*   grp=10: root1->mid1->leaf(10,20)  -> COUNT=2, SUM=30             */
/*   grp=20: root2->mid2->leaf(30)     -> COUNT=1, SUM=30             */
/*   grp=30: root3->no mid             -> COUNT=1, SUM=0              */
/*   grp=40: root4->no mid             -> COUNT=1, SUM=0              */
/* ================================================================== */

static int
createTest5Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj7_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj7_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj7_root");

  if (sqlExec(conn,
        "CREATE TABLE oj7_root ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj7_mid ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  mid_val INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj7_leaf ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  mid_id INT NOT NULL,"
        "  hours BIGINT NOT NULL,"
        "  INDEX ix_oj7_mid (mid_id)"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 5 tables\n");
  return 0;
}

static int
insertTest5Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj7_root VALUES "
        "(1,10),(2,20),(3,30),(4,40)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj7_mid VALUES "
        "(1,100),(2,200)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj7_leaf VALUES "
        "(1,1,10),(2,1,20),(3,2,30)") != 0) return -1;
  V("Inserted test 5 data\n");
  return 0;
}

static int
dropTest5Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj7_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj7_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj7_root");
  V("Dropped test 5 tables\n");
  return 0;
}

static int
testLookupThenScanLeaf(Ndb *ndb, MYSQL *conn)
{
  printf("Test 5: lookup then scan leaf GROUP BY "
         "(scan->LEFT lkp->LEFT scan) ... ");
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {2, 30}},    /* root1->mid1->leaf(10,20) */
    {20, {1, 30}},    /* root2->mid2->leaf(30) */
    {30, {1, 0}},     /* root3->no mid */
    {40, {1, 0}}      /* root4->no mid */
  };

  if (verifyGroupByWithMysql(conn, "Test 5",
        "SELECT r.grp, COUNT(*), COALESCE(SUM(l.hours),0) "
        "FROM oj7_root r "
        "LEFT JOIN oj7_mid m ON m.root_id = r.id "
        "LEFT JOIN oj7_leaf l ON l.mid_id = m.root_id "
        "GROUP BY r.grp ORDER BY r.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ7_ROOT);
  dict->invalidateTable(OJ7_MID);
  dict->invalidateTable(OJ7_LEAF);
  dict->invalidateIndex(OJ7_LEAF_IDX, OJ7_LEAF);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ7_ROOT);
  const NdbDictionary::Table *midTab = dict->getTable(OJ7_MID);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ7_LEAF);
  const NdbDictionary::Index *leafIdx = dict->getIndex(OJ7_LEAF_IDX, OJ7_LEAF);
  if (rootTab == nullptr || midTab == nullptr || leafTab == nullptr ||
      leafIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");

  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("hours", 1) ||
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

  /* Node 0: scan oj7_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj7_mid (LEFT JOIN) */
  const NdbQueryOperand *midKey[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *midOp =
      qb->readTuple(midTab, midKey, nullptr);
  if (midOp == nullptr) {
    printf("FAILED (readTuple mid: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: scanIndex oj7_leaf (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *leafBound[] = {
    qb->linkedValue(midOp, "root_id"), nullptr
  };
  NdbQueryIndexBound leafBoundObj(leafBound);

  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(grpLink);

  const NdbQueryIndexScanOperationDef *leafOp =
      qb->scanIndex(leafIdx, leafTab, &leafBoundObj, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (scanIndex leaf: %s)\n", qb->getNdbError().message);
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
    V("  NDB: grp=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: grp=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: grp=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group grp=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (4 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 6: Two Scan Intermediates                                      */
/*   scan -> LEFT scan -> LEFT scan -> LEFT lookup [leaf]               */
/*                                                                     */
/* Schema:                                                             */
/*   oj8_root (pk INT PK, grp INT, join_col INT)                       */
/*   oj8_a    (pk INT PK, root_id INT, a_val INT,                      */
/*             INDEX ix_oj8_root(root_id))                             */
/*   oj8_b    (pk INT PK, a_id INT, b_val INT,                        */
/*             INDEX ix_oj8_a(a_id))                                   */
/*   oj8_leaf (b_pk INT PK, hours BIGINT)                              */
/*                                                                     */
/* Data:                                                               */
/*   root: (1,10,1),(2,20,2),(3,30,3)                                  */
/*   a:    (1,1,100),(2,1,200)         -- root 1 has 2 rows            */
/*   b:    (1,1,10),(2,2,20)           -- a rows 1,2 each have 1 b row */
/*   leaf: (1,5)                       -- only b row 1 has a leaf      */
/*                                                                     */
/* Expected: KNOWN LIMITATION (Phase 4 not implemented)                */
/*   grp=10: root1->a(1,2)->b(1,2)->leaf(5,none) -> COUNT=2, SUM=5    */
/*   grp=20: root2->no a                         -> COUNT=1, SUM=0    */
/*   grp=30: root3->no a                         -> COUNT=1, SUM=0    */
/* ================================================================== */

static int
createTest6Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_b");
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_a");
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_root");

  if (sqlExec(conn,
        "CREATE TABLE oj8_root ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL,"
        "  join_col INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj8_a ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  root_id INT NOT NULL,"
        "  a_val INT NOT NULL,"
        "  INDEX ix_oj8_root (root_id)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj8_b ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  a_id INT NOT NULL,"
        "  b_val INT NOT NULL,"
        "  INDEX ix_oj8_a (a_id)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj8_leaf ("
        "  b_pk INT NOT NULL PRIMARY KEY,"
        "  hours BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 6 tables\n");
  return 0;
}

static int
insertTest6Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj8_root VALUES "
        "(1,10,1),(2,20,2),(3,30,3)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj8_a VALUES "
        "(1,1,100),(2,1,200)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj8_b VALUES "
        "(1,1,10),(2,2,20)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj8_leaf VALUES (1,5)") != 0) return -1;
  V("Inserted test 6 data\n");
  return 0;
}

static int
dropTest6Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_b");
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_a");
  sqlExec(conn, "DROP TABLE IF EXISTS oj8_root");
  V("Dropped test 6 tables\n");
  return 0;
}

static int
testTwoScanIntermediates(Ndb *ndb, MYSQL *conn)
{
  printf("Test 6: two scan intermediates GROUP BY "
         "(scan->LEFT scan->LEFT scan->LEFT lkp) ... ");
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {2, 5}},     /* root1->a(1,2)->b(1,2)->leaf(5,none) */
    {20, {1, 0}},     /* root2->no a */
    {30, {1, 0}}      /* root3->no a */
  };

  if (verifyGroupByWithMysql(conn, "Test 6",
        "SELECT r.grp, COUNT(*), COALESCE(SUM(l.hours),0) "
        "FROM oj8_root r "
        "LEFT JOIN oj8_a a ON a.root_id = r.join_col "
        "LEFT JOIN oj8_b b ON b.a_id = a.pk "
        "LEFT JOIN oj8_leaf l ON l.b_pk = b.pk "
        "GROUP BY r.grp ORDER BY r.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ8_ROOT);
  dict->invalidateTable(OJ8_A);
  dict->invalidateTable(OJ8_B);
  dict->invalidateTable(OJ8_LEAF);
  dict->invalidateIndex(OJ8_A_IDX, OJ8_A);
  dict->invalidateIndex(OJ8_B_IDX, OJ8_B);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ8_ROOT);
  const NdbDictionary::Table *aTab = dict->getTable(OJ8_A);
  const NdbDictionary::Table *bTab = dict->getTable(OJ8_B);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ8_LEAF);
  const NdbDictionary::Index *aIdx = dict->getIndex(OJ8_A_IDX, OJ8_A);
  const NdbDictionary::Index *bIdx = dict->getIndex(OJ8_B_IDX, OJ8_B);
  if (rootTab == nullptr || aTab == nullptr || bTab == nullptr ||
      leafTab == nullptr || aIdx == nullptr || bIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");

  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("hours", 1) ||
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

  /* Node 0: scan oj8_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: scanIndex oj8_a (LEFT JOIN) */
  const NdbQueryOperand *aBound[] = {
    qb->linkedValue(rootOp, "join_col"), nullptr
  };
  NdbQueryIndexBound aBoundObj(aBound);

  const NdbQueryIndexScanOperationDef *aOp =
      qb->scanIndex(aIdx, aTab, &aBoundObj, nullptr);
  if (aOp == nullptr) {
    printf("FAILED (scanIndex a: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: scanIndex oj8_b (LEFT JOIN) */
  const NdbQueryOperand *bBound[] = {
    qb->linkedValue(aOp, "pk"), nullptr
  };
  NdbQueryIndexBound bBoundObj(bBound);

  const NdbQueryIndexScanOperationDef *bOp =
      qb->scanIndex(bIdx, bTab, &bBoundObj, nullptr);
  if (bOp == nullptr) {
    printf("FAILED (scanIndex b: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj8_leaf (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(bOp, "pk"), nullptr
  };

  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(grpLink);

  const NdbQueryLookupOperationDef *leafOp =
      qb->readTuple(leafTab, leafKey, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (readTuple leaf: %s)\n", qb->getNdbError().message);
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
    V("  NDB: grp=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu - "
           "Phase 4 scan intermediate not implemented)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: grp=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: grp=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group grp=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (3 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 7: Scan Intermediate + Inner Blocker                           */
/*   scan -> LEFT scan -> INNER lookup -> LEFT lookup [leaf]            */
/*                                                                     */
/* Schema:                                                             */
/*   oj9_root  (pk INT PK, grp INT, join_col INT)                      */
/*   oj9_mid   (pk INT PK, root_id INT, mid_val INT,                   */
/*              INDEX ix_oj9_root(root_id))                            */
/*   oj9_inner (mid_pk INT PK, inner_val INT)                          */
/*   oj9_leaf  (inner_pk INT PK, hours BIGINT)                         */
/*                                                                     */
/* Data:                                                               */
/*   root:  (1,10,1),(2,20,2),(3,30,3)                                 */
/*   mid:   (1,1,100),(2,1,200),(3,2,300) -- root 1 has 2, root 2 has 1*/
/*   inner: (1,10),(3,30)       -- mid 1,3 have inner; mid 2 does not  */
/*   leaf:  (1,5)               -- only inner row 1 has leaf           */
/*                                                                     */
/* The INNER JOIN at node 2 blocks null propagation from scan           */
/* intermediate. When root 3 has no mid matches, the LEFT JOIN null     */
/* at node 1 is blocked by INNER at node 2 -> no contribution.         */
/* When mid 2 has no inner match -> filtered by INNER.                  */
/*                                                                     */
/* Expected:                                                            */
/*   grp=10: root1->mid(1,2)->inner(1,none)->leaf(5,filtered)          */
/*           mid 1->inner 1->leaf(5): COUNT=1                          */
/*           mid 2->no inner->filtered by INNER                        */
/*           -> COUNT=1, SUM=5                                         */
/*   grp=20: root2->mid(3)->inner(3)->leaf(none) -> COUNT=1, SUM=0    */
/*   grp=30: root3->no mid->LEFT null->INNER blocks -> filtered        */
/*                                                                     */
/* Verify with MySQL first to confirm exact expected results.           */
/* ================================================================== */

static int
createTest7Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_inner");
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_root");

  if (sqlExec(conn,
        "CREATE TABLE oj9_root ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL,"
        "  join_col INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj9_mid ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  root_id INT NOT NULL,"
        "  mid_val INT NOT NULL,"
        "  INDEX ix_oj9_root (root_id)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj9_inner ("
        "  mid_pk INT NOT NULL PRIMARY KEY,"
        "  inner_val INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj9_leaf ("
        "  inner_pk INT NOT NULL PRIMARY KEY,"
        "  hours BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 7 tables\n");
  return 0;
}

static int
insertTest7Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj9_root VALUES "
        "(1,10,1),(2,20,2),(3,30,3)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj9_mid VALUES "
        "(1,1,100),(2,1,200),(3,2,300)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj9_inner VALUES "
        "(1,10),(3,30)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj9_leaf VALUES (1,5)") != 0) return -1;
  V("Inserted test 7 data\n");
  return 0;
}

static int
dropTest7Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_inner");
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj9_root");
  V("Dropped test 7 tables\n");
  return 0;
}

static int
testScanIntermediateInnerBlocker(Ndb *ndb, MYSQL *conn)
{
  printf("Test 7: scan intermediate + inner blocker GROUP BY "
         "(scan->LEFT scan->INNER lkp->LEFT lkp) ... ");
  fflush(stdout);

  /* Determine expected results from MySQL first.
   * SQL:
   *   SELECT r.grp, COUNT(*), COALESCE(SUM(l.hours),0)
   *   FROM oj9_root r
   *   LEFT JOIN oj9_mid m ON m.root_id = r.join_col
   *   JOIN oj9_inner i ON i.mid_pk = m.pk
   *   LEFT JOIN oj9_leaf l ON l.inner_pk = i.mid_pk
   *   GROUP BY r.grp ORDER BY r.grp
   *
   * Trace:
   * root 1 (grp=10,join_col=1):
   *   mid 1 (root_id=1): inner 1 exists -> leaf 1 (hours=5) -> (10,1,5)
   *   mid 2 (root_id=1): inner 2 missing -> INNER filters -> gone
   * root 2 (grp=20,join_col=2):
   *   mid 3 (root_id=2): inner 3 exists -> leaf 3 missing -> (20,1,0)
   * root 3 (grp=30,join_col=3):
   *   no mid -> LEFT null -> INNER on null fails ->
   *   Standard SQL: ((r LEFT JOIN m) JOIN i) LEFT JOIN l
   *   r LEFT JOIN m gives null mid for root 3, INNER JOIN i on null -> no match
   *   root 3 disappears (not preserved by outer LEFT JOIN level)
   *
   * Expected: 2 groups
   */

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {1, 5}},     /* root1: mid1->inner1->leaf(5) */
    {20, {1, 0}}      /* root2: mid3->inner3->no leaf */
  };

  if (verifyGroupByWithMysql(conn, "Test 7",
        "SELECT r.grp, COUNT(*), COALESCE(SUM(l.hours),0) "
        "FROM oj9_root r "
        "LEFT JOIN oj9_mid m ON m.root_id = r.join_col "
        "JOIN oj9_inner i ON i.mid_pk = m.pk "
        "LEFT JOIN oj9_leaf l ON l.inner_pk = i.mid_pk "
        "GROUP BY r.grp ORDER BY r.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ9_ROOT);
  dict->invalidateTable(OJ9_MID);
  dict->invalidateTable(OJ9_INNER);
  dict->invalidateTable(OJ9_LEAF);
  dict->invalidateIndex(OJ9_MID_IDX, OJ9_MID);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ9_ROOT);
  const NdbDictionary::Table *midTab = dict->getTable(OJ9_MID);
  const NdbDictionary::Table *innerTab = dict->getTable(OJ9_INNER);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ9_LEAF);
  const NdbDictionary::Index *midIdx = dict->getIndex(OJ9_MID_IDX, OJ9_MID);
  if (rootTab == nullptr || midTab == nullptr || innerTab == nullptr ||
      leafTab == nullptr || midIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");

  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("hours", 1) ||
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

  /* Node 0: scan oj9_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: scanIndex oj9_mid (LEFT JOIN - scan child) */
  const NdbQueryOperand *midBound[] = {
    qb->linkedValue(rootOp, "join_col"), nullptr
  };
  NdbQueryIndexBound midBoundObj(midBound);

  const NdbQueryIndexScanOperationDef *midOp =
      qb->scanIndex(midIdx, midTab, &midBoundObj, nullptr);
  if (midOp == nullptr) {
    printf("FAILED (scanIndex mid: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj9_inner (INNER JOIN - MatchNonNull) */
  const NdbQueryOperand *innerKey[] = {
    qb->linkedValue(midOp, "pk"), nullptr
  };
  NdbQueryOptions innerOpts;
  innerOpts.setMatchType(NdbQueryOptions::MatchNonNull);

  const NdbQueryLookupOperationDef *innerOp =
      qb->readTuple(innerTab, innerKey, &innerOpts);
  if (innerOp == nullptr) {
    printf("FAILED (readTuple inner: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj9_leaf (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(innerOp, "mid_pk"), nullptr
  };

  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(grpLink);

  const NdbQueryLookupOperationDef *leafOp =
      qb->readTuple(leafTab, leafKey, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (readTuple leaf: %s)\n", qb->getNdbError().message);
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
    V("  NDB: grp=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("KNOWN LIMITATION (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: grp=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: grp=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("KNOWN LIMITATION (group grp=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (2 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 8: GROUP BY intermediate column (3-way LEFT JOIN)              */
/*                                                                     */
/* Schema:                                                             */
/*   oj10_root (id INT PK, grp INT)                                    */
/*   oj10_mid  (root_id INT PK, category INT)                         */
/*   oj10_leaf (mid_id INT PK, value BIGINT)                          */
/*                                                                     */
/* Data:                                                               */
/*   root: (1,10),(2,20),(3,30),(4,40)                                 */
/*   mid:  (1,100),(2,200)  — only roots 1,2 have mid match            */
/*   leaf: (1,5),(2,15)     — only mids 1,2 have leaf match            */
/*                                                                     */
/* SQL: SELECT m.category, COUNT(*), COALESCE(SUM(l.value),0)          */
/*      FROM oj10_root r LEFT JOIN oj10_mid m ON m.root_id = r.id      */
/*      LEFT JOIN oj10_leaf l ON l.mid_id = m.root_id                  */
/*      GROUP BY m.category                                            */
/*                                                                     */
/* Expected:                                                           */
/*   category=100: root 1 → mid(100) → leaf(5)  → COUNT=1, SUM=5     */
/*   category=200: root 2 → mid(200) → leaf(15) → COUNT=1, SUM=15    */
/*   category=NULL: roots 3,4 → no mid → COUNT=2, SUM=0              */
/* ================================================================== */

static int
createTest8Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj10_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj10_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj10_root");

  if (sqlExec(conn,
        "CREATE TABLE oj10_root ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ10_ROOT);

  if (sqlExec(conn,
        "CREATE TABLE oj10_mid ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  category INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ10_MID);

  if (sqlExec(conn,
        "CREATE TABLE oj10_leaf ("
        "  mid_id INT NOT NULL PRIMARY KEY,"
        "  value BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ10_LEAF);

  return 0;
}

static int
insertTest8Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj10_root VALUES "
        "(1,10),(2,20),(3,30),(4,40)") != 0) return -1;
  V("Inserted 4 root rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj10_mid VALUES "
        "(1,100),(2,200)") != 0) return -1;
  V("Inserted 2 mid rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj10_leaf VALUES "
        "(1,5),(2,15)") != 0) return -1;
  V("Inserted 2 leaf rows\n");

  return 0;
}

static int
dropTest8Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj10_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj10_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj10_root");
  V("Dropped test 8 tables\n");
  return 0;
}

static int
testGroupByIntermediate(Ndb *ndb, MYSQL *conn)
{
  printf("Test 8: GROUP BY intermediate column (scan→LEFT→LEFT) ... ");
  fflush(stdout);

  /* Expected: 3 groups (category=100, 200, NULL) */
  /* Use NULL_GROUP sentinel for the NULL group */
  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {100, {1, 5}},           /* root 1 → mid(100) → leaf(5) */
    {200, {1, 15}},          /* root 2 → mid(200) → leaf(15) */
    {NULL_GROUP, {2, 0}}     /* roots 3,4 → no mid → COUNT=2, SUM=0 */
  };

  /* Verify with MySQL */
  if (verifyGroupByWithMysql(conn, "Test 8",
        "SELECT COALESCE(m.category, -2147483648), COUNT(*), "
        "COALESCE(SUM(l.value),0) "
        "FROM oj10_root r "
        "LEFT JOIN oj10_mid m ON m.root_id = r.id "
        "LEFT JOIN oj10_leaf l ON l.mid_id = m.root_id "
        "GROUP BY m.category ORDER BY m.category",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ10_ROOT);
  dict->invalidateTable(OJ10_MID);
  dict->invalidateTable(OJ10_LEAF);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ10_ROOT);
  const NdbDictionary::Table *midTab = dict->getTable(OJ10_MID);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ10_LEAF);
  if (rootTab == nullptr || midTab == nullptr || leafTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *categoryCol = midTab->getColumn("category");
  if (categoryCol == nullptr) {
    printf("FAILED (column lookup: category)\n");
    return -1;
  }

  /* Aggregation: GROUP BY mid.category, COUNT(*), SUM(leaf.value) */
  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, categoryCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("value", 1) ||
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

  /* Node 0: scan oj10_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj10_mid (LEFT JOIN) */
  const NdbQueryOperand *midKey[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *midOp =
      qb->readTuple(midTab, midKey, nullptr);
  if (midOp == nullptr) {
    printf("FAILED (readTuple mid: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj10_leaf (LEFT JOIN, aggregate leaf)
   * Linked projection: mid.category (from intermediate, NOT root!) */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(midOp, "root_id"), nullptr
  };
  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *catLink = qb->linkedValue(midOp, "category");
  if (catLink == nullptr) {
    printf("FAILED (linkedValue category: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(catLink);

  const NdbQueryLookupOperationDef *leafOp =
      qb->readTuple(leafTab, leafKey, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (readTuple leaf: %s)\n", qb->getNdbError().message);
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
    Int32 grpVal;
    if (grpValCol.is_null()) {
      grpVal = NULL_GROUP;
    } else {
      grpVal = grpValCol.data_int32();
    }
    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();
    actual[grpVal] = {countVal, sumVal};
    V("  NDB: category=%d COUNT=%lld SUM=%lld%s\n",
      grpVal, (long long)countVal, (long long)sumVal,
      grpVal == NULL_GROUP ? " (NULL)" : "");
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: category=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: category=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group category=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (3 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 9: GROUP BY root + intermediate columns (3-way LEFT JOIN)      */
/*                                                                     */
/* Same schema as Test 8 but GROUP BY both root.grp AND mid.category.  */
/* This tests that linked projections from BOTH root (via P_PARENT)    */
/* and intermediate (direct P_ATTRINFO) work correctly in null         */
/* propagation.                                                        */
/*                                                                     */
/* SQL: SELECT r.grp, m.category, COUNT(*), COALESCE(SUM(l.value),0)   */
/*      FROM oj10_root r LEFT JOIN oj10_mid m ON m.root_id = r.id      */
/*      LEFT JOIN oj10_leaf l ON l.mid_id = m.root_id                  */
/*      GROUP BY r.grp, m.category                                     */
/*                                                                     */
/* Expected (grp*1000 + COALESCE(category,0) as composite key):        */
/*   (10,100): root 1 → mid(100) → leaf(5)  → COUNT=1, SUM=5         */
/*   (20,200): root 2 → mid(200) → leaf(15) → COUNT=1, SUM=15        */
/*   (30,NULL): root 3 → no mid → COUNT=1, SUM=0                     */
/*   (40,NULL): root 4 → no mid → COUNT=1, SUM=0                     */
/* ================================================================== */

static int
testGroupByRootAndIntermediate(Ndb *ndb, MYSQL *conn)
{
  printf("Test 9: GROUP BY root+intermediate (scan→LEFT→LEFT) ... ");
  fflush(stdout);

  /* Composite key: grp*1000 + COALESCE(category,0) */
  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10*1000+100, {1, 5}},    /* (grp=10,cat=100): leaf(5) */
    {20*1000+200, {1, 15}},   /* (grp=20,cat=200): leaf(15) */
    {30*1000+0, {1, 0}},      /* (grp=30,cat=NULL): no mid */
    {40*1000+0, {1, 0}}       /* (grp=40,cat=NULL): no mid */
  };

  /* Verify with MySQL */
  if (verifyGroupByWithMysql(conn, "Test 9",
        "SELECT r.grp*1000 + COALESCE(m.category,0), COUNT(*), "
        "COALESCE(SUM(l.value),0) "
        "FROM oj10_root r "
        "LEFT JOIN oj10_mid m ON m.root_id = r.id "
        "LEFT JOIN oj10_leaf l ON l.mid_id = m.root_id "
        "GROUP BY r.grp, m.category ORDER BY r.grp, m.category",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ10_ROOT);
  dict->invalidateTable(OJ10_MID);
  dict->invalidateTable(OJ10_LEAF);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ10_ROOT);
  const NdbDictionary::Table *midTab = dict->getTable(OJ10_MID);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ10_LEAF);
  if (rootTab == nullptr || midTab == nullptr || leafTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");
  const NdbDictionary::Column *categoryCol = midTab->getColumn("category");
  if (grpCol == nullptr || categoryCol == nullptr) {
    printf("FAILED (column lookup)\n");
    return -1;
  }

  /* Aggregation: GROUP BY root.grp, mid.category, COUNT(*), SUM(leaf.value) */
  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, grpCol) ||       /* linked pos 0 = root.grp */
      !agg.GroupByLinked(1, categoryCol) ||   /* linked pos 1 = mid.category */
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("value", 1) ||
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

  /* Node 0: scan oj10_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj10_mid (LEFT JOIN) */
  const NdbQueryOperand *midKey[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *midOp =
      qb->readTuple(midTab, midKey, nullptr);
  if (midOp == nullptr) {
    printf("FAILED (readTuple mid: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj10_leaf (LEFT JOIN, aggregate leaf)
   * Linked projections: root.grp (pos 0) + mid.category (pos 1) */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(midOp, "root_id"), nullptr
  };
  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);

  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(grpLink);

  const NdbLinkedOperand *catLink = qb->linkedValue(midOp, "category");
  if (catLink == nullptr) {
    printf("FAILED (linkedValue category: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(catLink);

  const NdbQueryLookupOperationDef *leafOp =
      qb->readTuple(leafTab, leafKey, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (readTuple leaf: %s)\n", qb->getNdbError().message);
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
    NdbAggregator::Column catValCol = rec.FetchGroupbyColumn();
    Int32 catVal = catValCol.is_null() ? 0 : catValCol.data_int32();
    /* Composite key: grp*1000 + category */
    Int32 compositeKey = grpVal * 1000 + catVal;

    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();
    actual[compositeKey] = {countVal, sumVal};
    V("  NDB: grp=%d category=%d COUNT=%lld SUM=%lld\n",
      grpVal, catVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: key=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (composite key=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (4 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 10: GROUP BY intermediate in 4-way chain                       */
/*   scan → LEFT lookup → LEFT lookup → LEFT lookup [leaf]             */
/*   GROUP BY second intermediate (Node 2) column                      */
/*                                                                     */
/* Schema:                                                             */
/*   oj12_root (id INT PK, grp INT)                                    */
/*   oj12_a    (root_id INT PK, a_val INT)                             */
/*   oj12_b    (a_id INT PK, b_cat INT)                                */
/*   oj12_leaf (b_id INT PK, score BIGINT)                             */
/*                                                                     */
/* Data:                                                               */
/*   root: (1,10),(2,20),(3,30)                                        */
/*   a:    (1,100),(2,200)      — roots 1,2 match                      */
/*   b:    (1,500)              — only a=1 matches                     */
/*   leaf: (1,7)                — only b=1 matches                     */
/*                                                                     */
/* SQL: SELECT b.b_cat, COUNT(*), COALESCE(SUM(l.score),0)             */
/*      FROM oj12_root r LEFT JOIN oj12_a a ON a.root_id = r.id        */
/*      LEFT JOIN oj12_b b ON b.a_id = a.root_id                       */
/*      LEFT JOIN oj12_leaf l ON l.b_id = b.a_id                       */
/*      GROUP BY b.b_cat                                               */
/*                                                                     */
/* Expected:                                                           */
/*   b_cat=500:  root 1→a(100)→b(500)→leaf(7) → COUNT=1, SUM=7       */
/*   b_cat=NULL: root 2→a(200)→no b;                                  */
/*               root 3→no a → COUNT=2, SUM=0                         */
/* ================================================================== */

static int
createTest10Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_b");
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_a");
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_root");

  if (sqlExec(conn,
        "CREATE TABLE oj12_root ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ12_ROOT);

  if (sqlExec(conn,
        "CREATE TABLE oj12_a ("
        "  root_id INT NOT NULL PRIMARY KEY,"
        "  a_val INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ12_A);

  if (sqlExec(conn,
        "CREATE TABLE oj12_b ("
        "  a_id INT NOT NULL PRIMARY KEY,"
        "  b_cat INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ12_B);

  if (sqlExec(conn,
        "CREATE TABLE oj12_leaf ("
        "  b_id INT NOT NULL PRIMARY KEY,"
        "  score BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ12_LEAF);

  return 0;
}

static int
insertTest10Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj12_root VALUES "
        "(1,10),(2,20),(3,30)") != 0) return -1;
  V("Inserted 3 root rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj12_a VALUES "
        "(1,100),(2,200)") != 0) return -1;
  V("Inserted 2 a rows\n");

  if (sqlExec(conn,
        "INSERT INTO oj12_b VALUES (1,500)") != 0) return -1;
  V("Inserted 1 b row\n");

  if (sqlExec(conn,
        "INSERT INTO oj12_leaf VALUES (1,7)") != 0) return -1;
  V("Inserted 1 leaf row\n");

  return 0;
}

static int
dropTest10Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_b");
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_a");
  sqlExec(conn, "DROP TABLE IF EXISTS oj12_root");
  V("Dropped test 10 tables\n");
  return 0;
}

static int
testGroupByDeepIntermediate(Ndb *ndb, MYSQL *conn)
{
  printf("Test 10: GROUP BY deep intermediate (scan→LEFT→LEFT→LEFT) ... ");
  fflush(stdout);

  /* Expected: 2 groups (b_cat=500, NULL) */
  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {500, {1, 7}},           /* root 1 → a → b(500) → leaf(7) */
    {NULL_GROUP, {2, 0}}     /* roots 2,3 → b unmatched → COUNT=2 */
  };

  /* Verify with MySQL */
  if (verifyGroupByWithMysql(conn, "Test 10",
        "SELECT COALESCE(b.b_cat, -2147483648), COUNT(*), "
        "COALESCE(SUM(l.score),0) "
        "FROM oj12_root r "
        "LEFT JOIN oj12_a a ON a.root_id = r.id "
        "LEFT JOIN oj12_b b ON b.a_id = a.root_id "
        "LEFT JOIN oj12_leaf l ON l.b_id = b.a_id "
        "GROUP BY b.b_cat ORDER BY b.b_cat",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ12_ROOT);
  dict->invalidateTable(OJ12_A);
  dict->invalidateTable(OJ12_B);
  dict->invalidateTable(OJ12_LEAF);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ12_ROOT);
  const NdbDictionary::Table *aTab = dict->getTable(OJ12_A);
  const NdbDictionary::Table *bTab = dict->getTable(OJ12_B);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ12_LEAF);
  if (rootTab == nullptr || aTab == nullptr ||
      bTab == nullptr || leafTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *bCatCol = bTab->getColumn("b_cat");
  if (bCatCol == nullptr) {
    printf("FAILED (column lookup: b_cat)\n");
    return -1;
  }

  /* Aggregation: GROUP BY b.b_cat, COUNT(*), SUM(leaf.score) */
  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, bCatCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("score", 1) ||
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

  /* Node 0: scan oj12_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj12_a (LEFT JOIN) */
  const NdbQueryOperand *aKey[] = {
    qb->linkedValue(rootOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *aOp =
      qb->readTuple(aTab, aKey, nullptr);
  if (aOp == nullptr) {
    printf("FAILED (readTuple a: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj12_b (LEFT JOIN) */
  const NdbQueryOperand *bKey[] = {
    qb->linkedValue(aOp, "root_id"), nullptr
  };
  const NdbQueryLookupOperationDef *bOp =
      qb->readTuple(bTab, bKey, nullptr);
  if (bOp == nullptr) {
    printf("FAILED (readTuple b: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj12_leaf (LEFT JOIN, aggregate leaf)
   * Linked projection: b.b_cat (from Node 2, P_PARENT(1) P_ATTRINFO) */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(bOp, "a_id"), nullptr
  };
  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *bCatLink = qb->linkedValue(bOp, "b_cat");
  if (bCatLink == nullptr) {
    printf("FAILED (linkedValue b_cat: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(bCatLink);

  const NdbQueryLookupOperationDef *leafOp =
      qb->readTuple(leafTab, leafKey, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (readTuple leaf: %s)\n", qb->getNdbError().message);
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
    Int32 grpVal;
    if (grpValCol.is_null()) {
      grpVal = NULL_GROUP;
    } else {
      grpVal = grpValCol.data_int32();
    }
    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();
    actual[grpVal] = {countVal, sumVal};
    V("  NDB: b_cat=%d COUNT=%lld SUM=%lld%s\n",
      grpVal, (long long)countVal, (long long)sumVal,
      grpVal == NULL_GROUP ? " (NULL)" : "");
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: b_cat=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group b_cat=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (2 groups)\n");
  return 0;
}

/* ================================================================== */
/* Tests 11-12: Sibling outer join branches                            */
/*                                                                     */
/* Query tree topology (branching, not linear):                        */
/*   oj13_orders (scan, root)                                          */
/*     ├── oj13_details (lookup, LEFT JOIN) — sibling, NOT on agg path */
/*     └── oj13_items (lookup, LEFT JOIN, T_AGGREGATE_ANCESTOR)        */
/*           └── oj13_shipment (lookup, LEFT JOIN, T_AGGREGATE_LEAF)   */
/*                                                                     */
/* Verifies that sibling branches don't interfere with aggregation.    */
/* T_AGGREGATE_ANCESTOR is only set on the path root→items→shipment.   */
/* The details node is a sibling that participates in the query but    */
/* does NOT affect aggregation null propagation.                       */
/* ================================================================== */

static int
createTest11Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_shipment");
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_items");
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_details");
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_orders");

  if (sqlExec(conn,
        "CREATE TABLE oj13_orders ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  priority INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ13_ORDERS);

  if (sqlExec(conn,
        "CREATE TABLE oj13_details ("
        "  order_id INT NOT NULL PRIMARY KEY,"
        "  note INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ13_DETAILS);

  if (sqlExec(conn,
        "CREATE TABLE oj13_items ("
        "  order_id INT NOT NULL PRIMARY KEY,"
        "  category INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ13_ITEMS);

  if (sqlExec(conn,
        "CREATE TABLE oj13_shipment ("
        "  item_id INT NOT NULL PRIMARY KEY,"
        "  cost BIGINT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", OJ13_SHIPMENT);

  return 0;
}

static int
insertTest11Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj13_orders VALUES "
        "(1,10),(2,20),(3,30),(4,40)") != 0) return -1;
  V("Inserted 4 order rows\n");

  /* details: orders 2,4 have no detail (sibling no-match) */
  if (sqlExec(conn,
        "INSERT INTO oj13_details VALUES "
        "(1,100),(3,300)") != 0) return -1;
  V("Inserted 2 detail rows\n");

  /* items: orders 3,4 have no item (agg path no-match) */
  if (sqlExec(conn,
        "INSERT INTO oj13_items VALUES "
        "(1,500),(2,600)") != 0) return -1;
  V("Inserted 2 item rows\n");

  /* shipment: item 2 has no shipment (leaf no-match) */
  if (sqlExec(conn,
        "INSERT INTO oj13_shipment VALUES (1,25)") != 0) return -1;
  V("Inserted 1 shipment row\n");

  return 0;
}

static int
dropTest11Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_shipment");
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_items");
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_details");
  sqlExec(conn, "DROP TABLE IF EXISTS oj13_orders");
  V("Dropped test 11 tables\n");
  return 0;
}

/* ================================================================== */
/* Test 11: Sibling branch + aggregation on one branch                 */
/*                                                                     */
/* SQL: SELECT COALESCE(i.category, -2147483648), COUNT(*),            */
/*             COALESCE(SUM(s.cost), 0)                                */
/*      FROM oj13_orders o                                             */
/*      LEFT JOIN oj13_details d ON d.order_id = o.id                  */
/*      LEFT JOIN oj13_items i ON i.order_id = o.id                    */
/*      LEFT JOIN oj13_shipment s ON s.item_id = i.order_id            */
/*      GROUP BY i.category                                            */
/*                                                                     */
/* Expected:                                                           */
/*   cat=500:  order 1 → item(500) → shipment(25) → COUNT=1, SUM=25  */
/*   cat=600:  order 2 → item(600) → no shipment  → COUNT=1, SUM=0   */
/*   cat=NULL: orders 3,4 → no item               → COUNT=2, SUM=0   */
/* ================================================================== */

static int
testSiblingBranch(Ndb *ndb, MYSQL *conn)
{
  printf("Test 11: Sibling branch + aggregation (scan→LEFT+LEFT→LEFT) ... ");
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {500, {1, 25}},          /* order 1 → item(500) → shipment(25) */
    {600, {1, 0}},           /* order 2 → item(600) → no shipment */
    {NULL_GROUP, {2, 0}}     /* orders 3,4 → no item */
  };

  /* Verify with MySQL */
  if (verifyGroupByWithMysql(conn, "Test 11",
        "SELECT COALESCE(i.category, -2147483648), COUNT(*), "
        "COALESCE(SUM(s.cost),0) "
        "FROM oj13_orders o "
        "LEFT JOIN oj13_details d ON d.order_id = o.id "
        "LEFT JOIN oj13_items i ON i.order_id = o.id "
        "LEFT JOIN oj13_shipment s ON s.item_id = i.order_id "
        "GROUP BY i.category ORDER BY i.category",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ13_ORDERS);
  dict->invalidateTable(OJ13_DETAILS);
  dict->invalidateTable(OJ13_ITEMS);
  dict->invalidateTable(OJ13_SHIPMENT);
  const NdbDictionary::Table *ordersTab = dict->getTable(OJ13_ORDERS);
  const NdbDictionary::Table *detailsTab = dict->getTable(OJ13_DETAILS);
  const NdbDictionary::Table *itemsTab = dict->getTable(OJ13_ITEMS);
  const NdbDictionary::Table *shipTab = dict->getTable(OJ13_SHIPMENT);
  if (ordersTab == nullptr || detailsTab == nullptr ||
      itemsTab == nullptr || shipTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *categoryCol = itemsTab->getColumn("category");
  if (categoryCol == nullptr) {
    printf("FAILED (column lookup: category)\n");
    return -1;
  }

  /* Aggregation: GROUP BY items.category, COUNT(*), SUM(shipment.cost) */
  NdbAggregator agg(shipTab);
  if (!agg.GroupByLinked(0, categoryCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("cost", 1) ||
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

  /* Node 0: scan oj13_orders */
  const NdbQueryTableScanOperationDef *ordersOp = qb->scanTable(ordersTab);
  if (ordersOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj13_details (LEFT JOIN — sibling, NOT on agg path) */
  const NdbQueryOperand *detKey[] = {
    qb->linkedValue(ordersOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *detOp =
      qb->readTuple(detailsTab, detKey, nullptr);
  if (detOp == nullptr) {
    printf("FAILED (readTuple details: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj13_items (LEFT JOIN — on agg path) */
  const NdbQueryOperand *itemKey[] = {
    qb->linkedValue(ordersOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *itemOp =
      qb->readTuple(itemsTab, itemKey, nullptr);
  if (itemOp == nullptr) {
    printf("FAILED (readTuple items: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj13_shipment (LEFT JOIN, aggregate leaf)
   * Linked projection: items.category (from sibling's sibling) */
  const NdbQueryOperand *shipKey[] = {
    qb->linkedValue(itemOp, "order_id"), nullptr
  };
  NdbQueryOptions shipOpts;
  shipOpts.setAggregation(agg);
  const NdbLinkedOperand *catLink = qb->linkedValue(itemOp, "category");
  if (catLink == nullptr) {
    printf("FAILED (linkedValue category: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  shipOpts.addLinkedProjection(catLink);

  const NdbQueryLookupOperationDef *shipOp =
      qb->readTuple(shipTab, shipKey, &shipOpts);
  if (shipOp == nullptr) {
    printf("FAILED (readTuple shipment: %s)\n", qb->getNdbError().message);
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
    Int32 grpVal;
    if (grpValCol.is_null()) {
      grpVal = NULL_GROUP;
    } else {
      grpVal = grpValCol.data_int32();
    }
    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    Int64 countVal = countRes.data_int64();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();
    actual[grpVal] = {countVal, sumVal};
    V("  NDB: category=%d COUNT=%lld SUM=%lld%s\n",
      grpVal, (long long)countVal, (long long)sumVal,
      grpVal == NULL_GROUP ? " (NULL)" : "");
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: category=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group category=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (3 groups)\n");
  return 0;
}

/* ================================================================== */
/* Test 12: Sibling branch + GROUP BY root column                      */
/*                                                                     */
/* Same schema/data as Test 11 but GROUP BY orders.priority (root).    */
/* Verifies linked projections from root work with sibling branches.   */
/*                                                                     */
/* SQL: SELECT o.priority, COUNT(*), COALESCE(SUM(s.cost), 0)          */
/*      FROM oj13_orders o                                             */
/*      LEFT JOIN oj13_details d ON d.order_id = o.id                  */
/*      LEFT JOIN oj13_items i ON i.order_id = o.id                    */
/*      LEFT JOIN oj13_shipment s ON s.item_id = i.order_id            */
/*      GROUP BY o.priority                                            */
/*                                                                     */
/* Expected:                                                           */
/*   priority=10: order 1 → shipment(25) → COUNT=1, SUM=25            */
/*   priority=20: order 2 → no shipment  → COUNT=1, SUM=0             */
/*   priority=30: order 3 → no item      → COUNT=1, SUM=0             */
/*   priority=40: order 4 → no item      → COUNT=1, SUM=0             */
/* ================================================================== */

static int
testSiblingBranchGroupByRoot(Ndb *ndb, MYSQL *conn)
{
  printf("Test 12: Sibling branch + GROUP BY root (scan→LEFT+LEFT→LEFT) ... ");
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {1, 25}},    /* order 1 → item → shipment(25) */
    {20, {1, 0}},     /* order 2 → item → no shipment */
    {30, {1, 0}},     /* order 3 → no item */
    {40, {1, 0}}      /* order 4 → no item */
  };

  /* Verify with MySQL */
  if (verifyGroupByWithMysql(conn, "Test 12",
        "SELECT o.priority, COUNT(*), COALESCE(SUM(s.cost),0) "
        "FROM oj13_orders o "
        "LEFT JOIN oj13_details d ON d.order_id = o.id "
        "LEFT JOIN oj13_items i ON i.order_id = o.id "
        "LEFT JOIN oj13_shipment s ON s.item_id = i.order_id "
        "GROUP BY o.priority ORDER BY o.priority",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  /* Build NDB pushdown query */
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ13_ORDERS);
  dict->invalidateTable(OJ13_DETAILS);
  dict->invalidateTable(OJ13_ITEMS);
  dict->invalidateTable(OJ13_SHIPMENT);
  const NdbDictionary::Table *ordersTab = dict->getTable(OJ13_ORDERS);
  const NdbDictionary::Table *detailsTab = dict->getTable(OJ13_DETAILS);
  const NdbDictionary::Table *itemsTab = dict->getTable(OJ13_ITEMS);
  const NdbDictionary::Table *shipTab = dict->getTable(OJ13_SHIPMENT);
  if (ordersTab == nullptr || detailsTab == nullptr ||
      itemsTab == nullptr || shipTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *priorityCol = ordersTab->getColumn("priority");
  if (priorityCol == nullptr) {
    printf("FAILED (column lookup: priority)\n");
    return -1;
  }

  /* Aggregation: GROUP BY orders.priority, COUNT(*), SUM(shipment.cost) */
  NdbAggregator agg(shipTab);
  if (!agg.GroupByLinked(0, priorityCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("cost", 1) ||
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

  /* Node 0: scan oj13_orders */
  const NdbQueryTableScanOperationDef *ordersOp = qb->scanTable(ordersTab);
  if (ordersOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup oj13_details (LEFT JOIN — sibling) */
  const NdbQueryOperand *detKey[] = {
    qb->linkedValue(ordersOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *detOp =
      qb->readTuple(detailsTab, detKey, nullptr);
  if (detOp == nullptr) {
    printf("FAILED (readTuple details: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup oj13_items (LEFT JOIN — agg path) */
  const NdbQueryOperand *itemKey[] = {
    qb->linkedValue(ordersOp, "id"), nullptr
  };
  const NdbQueryLookupOperationDef *itemOp =
      qb->readTuple(itemsTab, itemKey, nullptr);
  if (itemOp == nullptr) {
    printf("FAILED (readTuple items: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup oj13_shipment (LEFT JOIN, aggregate leaf)
   * Linked projection: orders.priority (from root, P_PARENT path) */
  const NdbQueryOperand *shipKey[] = {
    qb->linkedValue(itemOp, "order_id"), nullptr
  };
  NdbQueryOptions shipOpts;
  shipOpts.setAggregation(agg);
  const NdbLinkedOperand *prioLink = qb->linkedValue(ordersOp, "priority");
  if (prioLink == nullptr) {
    printf("FAILED (linkedValue priority: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  shipOpts.addLinkedProjection(prioLink);

  const NdbQueryLookupOperationDef *shipOp =
      qb->readTuple(shipTab, shipKey, &shipOpts);
  if (shipOp == nullptr) {
    printf("FAILED (readTuple shipment: %s)\n", qb->getNdbError().message);
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
    V("  NDB: priority=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: priority=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group priority=%d mismatch)\n", e.first);
      return -1;
    }
  }

  printf("OK (4 groups)\n");
  return 0;
}

/* ================================================================== */
/* Tests 13-14: 3-Scan Deferred Null Row                               */
/*   scan -> LEFT scanIndex -> LEFT scanIndex [leaf]                    */
/*                                                                     */
/* Schema:                                                             */
/*   oj14_root (pk INT PK, grp INT)                                    */
/*   oj14_mid  (pk INT PK, root_id INT, mid_val INT,                   */
/*              INDEX ix_oj14_root(root_id))                            */
/*   oj14_leaf (pk INT PK, mid_pk INT, hours BIGINT,                   */
/*              INDEX ix_oj14_mid(mid_pk))                              */
/*                                                                     */
/* Data:                                                               */
/*   root: (1,10),(2,20),(3,30),(4,40)                                  */
/*   mid:  (1,1,100),(2,1,200),(3,2,300)  -- root 1 has 2, root 2 has 1*/
/*   leaf: (1,1,5),(2,1,10),(3,3,15)      -- mid 1 has 2, mid 3 has 1  */
/*                                                                     */
/* Expected (correct SQL):                                             */
/*   grp=10: r1->m(1,2)->l: m1->l(5,10), m2->no l -> COUNT=3, SUM=15  */
/*   grp=20: r2->m(3)->l(15)                       -> COUNT=1, SUM=15  */
/*   grp=30: r3->no mid                            -> COUNT=1, SUM=0   */
/*   grp=40: r4->no mid                            -> COUNT=1, SUM=0   */
/*                                                                     */
/* Without the deferred null row fix, roots 3 and 4 (which have no mid */
/* match) would crash the data node or produce wrong results: the      */
/* intermediate scan's handleAggAncestorComplete sends null rows to     */
/* the scan leaf before its SCAN_FRAGCONFs arrive, corrupting the      */
/* completed_tree_nodes tracking.                                      */
/* ================================================================== */

static int
createTest13Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj14_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj14_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj14_root");

  if (sqlExec(conn,
        "CREATE TABLE oj14_root ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj14_mid ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  root_id INT NOT NULL,"
        "  mid_val INT NOT NULL,"
        "  INDEX ix_oj14_root (root_id)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE oj14_leaf ("
        "  pk INT NOT NULL PRIMARY KEY,"
        "  mid_pk INT NOT NULL,"
        "  hours BIGINT NOT NULL,"
        "  INDEX ix_oj14_mid (mid_pk)"
        ") ENGINE=NDB") != 0) return -1;

  V("Created test 13/14 tables\n");
  return 0;
}

static int
insertTest13Data(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO oj14_root VALUES "
        "(1,10),(2,20),(3,30),(4,40)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj14_mid VALUES "
        "(1,1,100),(2,1,200),(3,2,300)") != 0) return -1;
  if (sqlExec(conn,
        "INSERT INTO oj14_leaf VALUES "
        "(1,1,5),(2,1,10),(3,3,15)") != 0) return -1;
  V("Inserted test 13/14 data\n");
  return 0;
}

static int
dropTest13Tables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj14_leaf");
  sqlExec(conn, "DROP TABLE IF EXISTS oj14_mid");
  sqlExec(conn, "DROP TABLE IF EXISTS oj14_root");
  V("Dropped test 13/14 tables\n");
  return 0;
}

/**
 * Helper: build and run 3-scan deferred null row test.
 */
static int
testThreeScanDeferred(Ndb *ndb, MYSQL *conn)
{
  const char *testLabel = "Test 13: 3-scan deferred null row";
  printf("%s (scan->LEFT scan->LEFT scan) ... ", testLabel);
  fflush(stdout);

  std::map<Int32, std::pair<Int64, Int64>> expected = {
    {10, {3, 15}},   /* r1->m(1,2)->l: m1->l(5,10), m2->no l */
    {20, {1, 15}},   /* r2->m(3)->l(15) */
    {30, {1, 0}},    /* r3->no mid -> null row */
    {40, {1, 0}}     /* r4->no mid -> null row */
  };

  if (verifyGroupByWithMysql(conn, testLabel,
        "SELECT r.grp, COUNT(*), COALESCE(SUM(l.hours),0) "
        "FROM oj14_root r "
        "LEFT JOIN oj14_mid m ON m.root_id = r.pk "
        "LEFT JOIN oj14_leaf l ON l.mid_pk = m.pk "
        "GROUP BY r.grp ORDER BY r.grp",
        expected) != 0) {
    printf("FAILED (MySQL verification)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(OJ14_ROOT);
  dict->invalidateTable(OJ14_MID);
  dict->invalidateTable(OJ14_LEAF);
  dict->invalidateIndex(OJ14_MID_IDX, OJ14_MID);
  dict->invalidateIndex(OJ14_LEAF_IDX, OJ14_LEAF);
  const NdbDictionary::Table *rootTab = dict->getTable(OJ14_ROOT);
  const NdbDictionary::Table *midTab = dict->getTable(OJ14_MID);
  const NdbDictionary::Table *leafTab = dict->getTable(OJ14_LEAF);
  const NdbDictionary::Index *midIdx = dict->getIndex(OJ14_MID_IDX, OJ14_MID);
  const NdbDictionary::Index *leafIdx = dict->getIndex(OJ14_LEAF_IDX, OJ14_LEAF);
  if (rootTab == nullptr || midTab == nullptr || leafTab == nullptr ||
      midIdx == nullptr || leafIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = rootTab->getColumn("grp");

  NdbAggregator agg(leafTab);
  if (!agg.GroupByLinked(0, grpCol) ||
      !agg.LoadUint64(1, 0) ||
      !agg.LoadColumn("hours", 1) ||
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

  /* Node 0: scan oj14_root */
  const NdbQueryTableScanOperationDef *rootOp = qb->scanTable(rootTab);
  if (rootOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: scanIndex oj14_mid (LEFT JOIN — default MatchAll) */
  const NdbQueryOperand *midBound[] = {
    qb->linkedValue(rootOp, "pk"), nullptr
  };
  NdbQueryIndexBound midBoundObj(midBound);

  const NdbQueryIndexScanOperationDef *midOp =
      qb->scanIndex(midIdx, midTab, &midBoundObj, nullptr);
  if (midOp == nullptr) {
    printf("FAILED (scanIndex mid: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: scanIndex oj14_leaf (LEFT JOIN, aggregate leaf) */
  const NdbQueryOperand *leafBound[] = {
    qb->linkedValue(midOp, "pk"), nullptr
  };
  NdbQueryIndexBound leafBoundObj(leafBound);

  NdbQueryOptions leafOpts;
  leafOpts.setAggregation(agg);
  const NdbLinkedOperand *grpLink = qb->linkedValue(rootOp, "grp");
  if (grpLink == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  leafOpts.addLinkedProjection(grpLink);

  const NdbQueryIndexScanOperationDef *leafOp =
      qb->scanIndex(leafIdx, leafTab, &leafBoundObj, &leafOpts);
  if (leafOp == nullptr) {
    printf("FAILED (scanIndex leaf: %s)\n", qb->getNdbError().message);
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
    V("  NDB: grp=%d COUNT=%lld SUM=%lld\n",
      grpVal, (long long)countVal, (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    for (const auto &a : actual) {
      printf("  got: grp=%d COUNT=%lld SUM=%lld\n",
             a.first, (long long)a.second.first, (long long)a.second.second);
    }
    for (const auto &e : expected) {
      if (actual.find(e.first) == actual.end()) {
        printf("  missing: grp=%d COUNT=%lld SUM=%lld\n",
               e.first, (long long)e.second.first, (long long)e.second.second);
      }
    }
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() ||
        it->second.first != e.second.first ||
        it->second.second != e.second.second) {
      printf("FAILED (group grp=%d: expected COUNT=%lld SUM=%lld, "
             "got COUNT=%lld SUM=%lld)\n",
             e.first,
             (long long)e.second.first, (long long)e.second.second,
             (long long)it->second.first, (long long)it->second.second);
      return -1;
    }
  }

  printf("OK (4 groups)\n");
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

          /* Test 3: 5-way deep all-lookup */
          if (shouldRun(3)) {
            if (createTest3Tables(conn) == 0 &&
                insertTest3Data(conn) == 0) {
              if (testFiveWayDeepLookup(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest3Tables(conn);
          }

          /* Test 4: scan-scan-lookup */
          if (shouldRun(4)) {
            if (createTest4Tables(conn) == 0 &&
                insertTest4Data(conn) == 0) {
              if (testScanScanLookup(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest4Tables(conn);
          }

          /* Test 5: lookup then scan leaf */
          if (shouldRun(5)) {
            if (createTest5Tables(conn) == 0 &&
                insertTest5Data(conn) == 0) {
              if (testLookupThenScanLeaf(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest5Tables(conn);
          }

          /* Test 6: two scan intermediates */
          if (shouldRun(6)) {
            if (createTest6Tables(conn) == 0 &&
                insertTest6Data(conn) == 0) {
              if (testTwoScanIntermediates(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest6Tables(conn);
          }

          /* Test 7: scan intermediate + inner blocker */
          if (shouldRun(7)) {
            if (createTest7Tables(conn) == 0 &&
                insertTest7Data(conn) == 0) {
              if (testScanIntermediateInnerBlocker(&ndb, conn) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest7Tables(conn);
          }

          /* Tests 8-9: GROUP BY intermediate (shared tables) */
          if (shouldRun(8) || shouldRun(9)) {
            if (createTest8Tables(conn) == 0 &&
                insertTest8Data(conn) == 0) {
              /* Test 8: GROUP BY intermediate column */
              if (shouldRun(8)) {
                if (testGroupByIntermediate(&ndb, conn) != 0) exitCode = 1;
              }
              /* Test 9: GROUP BY root + intermediate columns */
              if (shouldRun(9)) {
                if (testGroupByRootAndIntermediate(&ndb, conn) != 0)
                  exitCode = 1;
              }
            } else {
              exitCode = 1;
            }
            dropTest8Tables(conn);
          }

          /* Test 10: GROUP BY deep intermediate (4-way chain) */
          if (shouldRun(10)) {
            if (createTest10Tables(conn) == 0 &&
                insertTest10Data(conn) == 0) {
              if (testGroupByDeepIntermediate(&ndb, conn) != 0) exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropTest10Tables(conn);
          }

          /* Tests 11-12: sibling outer join branches (shared tables) */
          if (shouldRun(11) || shouldRun(12)) {
            if (createTest11Tables(conn) == 0 &&
                insertTest11Data(conn) == 0) {
              /* Test 11: sibling branch + GROUP BY intermediate */
              if (shouldRun(11)) {
                if (testSiblingBranch(&ndb, conn) != 0) exitCode = 1;
              }
              /* Test 12: sibling branch + GROUP BY root */
              if (shouldRun(12)) {
                if (testSiblingBranchGroupByRoot(&ndb, conn) != 0)
                  exitCode = 1;
              }
            } else {
              exitCode = 1;
            }
            dropTest11Tables(conn);
          }

          /* Tests 13-14: 3-scan deferred null row (shared tables) */
          if (shouldRun(13) || shouldRun(14)) {
            if (createTest13Tables(conn) == 0 &&
                insertTest13Data(conn) == 0) {
              /* Test 13: 3-scan deferred null row */
              if (shouldRun(13)) {
                if (testThreeScanDeferred(&ndb, conn) != 0)
                  exitCode = 1;
              }
            } else {
              exitCode = 1;
            }
            dropTest13Tables(conn);
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
