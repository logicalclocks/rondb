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
 * testJoinAggScanScan — Functional tests for pushdown join aggregation
 *                        with SCAN-SCAN joins (child scanIndex operations).
 *
 * All existing aggregation tests use scanTable/scanIndex root with readTuple
 * (PK lookup) children. This test exercises the MultiScanQuery path where
 * a child operation is also a scanIndex, returning 0..N rows per parent via
 * an ordered index. This is a fundamentally different DBSPJ execution path
 * (scanFrag_parent_row builds child scan bounds from parent row values).
 *
 * Tests:
 *   1. Basic scan-scan SUM GROUP BY
 *   2. Scan-scan COUNT/SUM no GROUP BY (global aggregation)
 *   3. Scan-scan with interpreted code filter on root
 *   4. Scan-scan all four aggregate types (COUNT/SUM/MIN/MAX)
 *   5. Scan-scan with NULL values in aggregation column
 *   6. 3-way scan-scan-lookup join (scan → scan → lookup)
 *   7. scanIndex root + scanIndex child (both bounded)
 *   8. Scan-scan with composite index bounds (2-column index)
 *
 * Usage: testJoinAggScanScan -c <connect_string> -m <mysql_port> [-v]
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

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *TEST_DB = "test_db";

/* Group A tables (Tests 1-4, 7) */
static const char *SS_DEPT = "ss_dept";
static const char *SS_EMP  = "ss_emp";

/* Group B tables (Test 5 — NULLs) */
static const char *SS_DEPT_N = "ss_dept_n";
static const char *SS_EMP_N  = "ss_emp_n";

/* Group C tables (Test 6 — 3-way) */
static const char *SS_REGION = "ss_region";
static const char *SS_DEPT_R = "ss_dept_r";
static const char *SS_STAT   = "ss_stat";

/* Group D tables (Test 8 — composite index) */
static const char *SS_PROJECT = "ss_project";
static const char *SS_TASK    = "ss_task";

/* Group E tables (Test 9 — eviction via ERROR_INSERT 4040) */
#if defined(VM_TRACE) || defined(ERROR_INSERT)
static const char *SS_STORE_E = "ss_store_e";
static const char *SS_SALE_E  = "ss_sale_e";
#endif

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

/* Helper: extract trimmed CHAR string from NdbAggregator column */
static std::string
extractCharColumn(NdbAggregator::Column &col)
{
  int nameLen = (int)col.byte_size();
  const char *namePtr = col.data();
  int effLen = (int)strnlen(namePtr, nameLen);
  while (effLen > 0 && namePtr[effLen - 1] == ' ')
    effLen--;
  return std::string(namePtr, effLen);
}

/* ------------------------------------------------------------------ */
/* Group A: ss_dept + ss_emp (Tests 1-4, 7)                            */
/* ------------------------------------------------------------------ */

static int
createGroupATables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_emp");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_dept");

  if (sqlExec(conn,
        "CREATE TABLE ss_dept ("
        "  dept_id INT NOT NULL PRIMARY KEY,"
        "  dept_name CHAR(20) NOT NULL,"
        "  region_id INT NOT NULL,"
        "  INDEX idx_dept_region (region_id)"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", SS_DEPT);

  if (sqlExec(conn,
        "CREATE TABLE ss_emp ("
        "  emp_id INT NOT NULL PRIMARY KEY,"
        "  dept_id INT NOT NULL,"
        "  salary BIGINT NOT NULL,"
        "  INDEX idx_emp_dept (dept_id)"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", SS_EMP);

  return 0;
}

static int
insertGroupAData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO ss_dept VALUES "
        "(1,'Engineering',1),(2,'Sales',1),(3,'Marketing',2),"
        "(4,'Support',2),(5,'Research',3)") != 0) return -1;
  V("Inserted 5 ss_dept rows\n");

  if (sqlExec(conn,
        "INSERT INTO ss_emp VALUES "
        "(101,1,80000),(102,1,90000),(103,1,75000),"
        "(104,2,60000),(105,2,65000),"
        "(106,3,55000),(107,3,58000),(108,3,52000),"
        "(109,4,45000),"
        "(110,5,95000),(111,5,88000)") != 0) return -1;
  V("Inserted 11 ss_emp rows\n");

  return 0;
}

static int
dropGroupATables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_emp");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_dept");
  V("Dropped ss_dept, ss_emp\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 1: Basic scan-scan SUM GROUP BY                                */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.dept_name, SUM(e.salary)                                 */
/*   FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id             */
/*   GROUP BY d.dept_name                                              */
/* ------------------------------------------------------------------ */

static int
testBasicSumGroupBy(Ndb *ndb, MYSQL *conn)
{
  printf("Test 1: Basic scan-scan SUM GROUP BY ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_DEPT);
  dict->invalidateTable(SS_EMP);
  dict->invalidateIndex("idx_emp_dept", SS_EMP);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(SS_EMP);
  const NdbDictionary::Index *empIdx =
      dict->getIndex("idx_emp_dept", SS_EMP);
  if (deptTab == nullptr || empTab == nullptr || empIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *deptNameCol = deptTab->getColumn("dept_name");

  /* Build aggregation: GROUP BY dept_name (linked), SUM(salary) */
  NdbAggregator agg(empTab);
  if (!agg.GroupByLinked(0, deptNameCol) ||
      !agg.LoadColumn("salary", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Build scan-scan query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Root: scan ss_dept */
  const NdbQueryTableScanOperationDef *deptOp = qb->scanTable(deptTab);
  if (deptOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Child: scanIndex on ss_emp, bound = dept.dept_id */
  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions empOpts;
  empOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  empOpts.setAggregation(agg);
  const NdbLinkedOperand *deptNameLink = qb->linkedValue(deptOp, "dept_name");
  empOpts.addLinkedProjection(deptNameLink);

  const NdbQueryIndexScanOperationDef *empOp =
      qb->scanIndex(empIdx, empTab, &bound, &empOpts);
  if (empOp == nullptr) {
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

  V("\n  Query prepared: %u operations, isScan=%d\n",
    queryDef->getNoOfOperations(), queryDef->isScanQuery());

  /* Execute */
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
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  V("  Scan consumed %u rows\n", rowCount);

  /* Collect aggregation results */
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
    std::string name = extractCharColumn(nameCol);
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[name] = sumVal;
    V("  dept_name='%s' SUM(salary)=%lld\n", name.c_str(), (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Verify */
  std::map<std::string, Int64> expected = {
    {"Engineering", 245000}, {"Sales", 125000}, {"Marketing", 165000},
    {"Support", 45000}, {"Research", 183000}
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

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT d.dept_name, SUM(e.salary) "
          "FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id "
          "GROUP BY d.dept_name ORDER BY d.dept_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      /* Trim trailing spaces from MySQL CHAR result */
      while (!name.empty() && name.back() == ' ') name.pop_back();
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

  printf("OK (5 groups, scan-scan SUM GROUP BY)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: Scan-scan COUNT/SUM no GROUP BY (global aggregation)        */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT COUNT(*), SUM(salary)                                      */
/*   FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id             */
/* ------------------------------------------------------------------ */

static int
testGlobalCountSum(Ndb *ndb, MYSQL *conn)
{
  printf("Test 2: Scan-scan COUNT/SUM no GROUP BY ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_DEPT);
  dict->invalidateTable(SS_EMP);
  dict->invalidateIndex("idx_emp_dept", SS_EMP);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(SS_EMP);
  const NdbDictionary::Index *empIdx =
      dict->getIndex("idx_emp_dept", SS_EMP);
  if (deptTab == nullptr || empTab == nullptr || empIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Aggregation: COUNT(salary), SUM(salary) — no GROUP BY */
  NdbAggregator agg(empTab);
  if (!agg.LoadColumn("salary", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *deptOp = qb->scanTable(deptTab);
  if (deptOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions empOpts;
  empOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  empOpts.setAggregation(agg);

  const NdbQueryIndexScanOperationDef *empOp =
      qb->scanIndex(empIdx, empTab, &bound, &empOpts);
  if (empOp == nullptr) {
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
    printf("FAILED (no result record)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result cntRes = rec.FetchAggregationResult();
  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 count = cntRes.data_int64();
  Int64 sum = sumRes.data_int64();

  V("  COUNT=%lld SUM=%lld\n", (long long)count, (long long)sum);

  query->close();
  trans->close();
  queryDef->destroy();

  if (count != 11 || sum != 763000) {
    printf("FAILED (expected COUNT=11,SUM=763000, got COUNT=%lld,SUM=%lld)\n",
           (long long)count, (long long)sum);
    return -1;
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT COUNT(*), SUM(e.salary) "
          "FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row == nullptr || atoll(row[0]) != 11 || atoll(row[1]) != 763000) {
      printf("FAILED (MySQL cross-check mismatch)\n");
      mysql_free_result(result);
      return -1;
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (COUNT=11, SUM=763000)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: Scan-scan with interpreted code filter on root              */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.dept_name, SUM(e.salary)                                 */
/*   FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id             */
/*   WHERE d.region_id = 1                                             */
/*   GROUP BY d.dept_name                                              */
/* ------------------------------------------------------------------ */

static int
testFilteredRoot(Ndb *ndb, MYSQL *conn)
{
  printf("Test 3: Scan-scan with filter on root ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_DEPT);
  dict->invalidateTable(SS_EMP);
  dict->invalidateIndex("idx_emp_dept", SS_EMP);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(SS_EMP);
  const NdbDictionary::Index *empIdx =
      dict->getIndex("idx_emp_dept", SS_EMP);
  if (deptTab == nullptr || empTab == nullptr || empIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *deptNameCol = deptTab->getColumn("dept_name");

  /* Aggregation: GROUP BY dept_name, SUM(salary) */
  NdbAggregator agg(empTab);
  if (!agg.GroupByLinked(0, deptNameCol) ||
      !agg.LoadColumn("salary", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  /* Interpreted code filter: region_id = 1 */
  Int32 one = 1;
  Uint32 regionIdAttrId = deptTab->getColumn("region_id")->getColumnNo();
  NdbInterpretedCode code(deptTab);
  code.branch_col_eq(&one, sizeof(one), regionIdAttrId, 0);
  code.interpret_exit_nok();
  code.def_label(0);
  code.interpret_exit_ok();
  if (code.finalise() != 0) {
    printf("FAILED (NdbInterpretedCode finalise: %s)\n",
           code.getNdbError().message);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Root: scan ss_dept with filter */
  NdbQueryOptions deptOpts;
  deptOpts.setInterpretedCode(code);
  const NdbQueryTableScanOperationDef *deptOp =
      qb->scanTable(deptTab, &deptOpts);
  if (deptOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Child: scanIndex on ss_emp */
  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions empOpts;
  empOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  empOpts.setAggregation(agg);
  const NdbLinkedOperand *deptNameLink = qb->linkedValue(deptOp, "dept_name");
  empOpts.addLinkedProjection(deptNameLink);

  const NdbQueryIndexScanOperationDef *empOp =
      qb->scanIndex(empIdx, empTab, &bound, &empOpts);
  if (empOp == nullptr) {
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
    std::string name = extractCharColumn(nameCol);
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    actual[name] = sumRes.data_int64();
    V("  dept_name='%s' SUM(salary)=%lld\n",
      name.c_str(), (long long)actual[name]);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  std::map<std::string, Int64> expected = {
    {"Engineering", 245000}, {"Sales", 125000}
  };

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() || it->second != e.second) {
      printf("FAILED (group '%s' mismatch)\n", e.first.c_str());
      return -1;
    }
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT d.dept_name, SUM(e.salary) "
          "FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id "
          "WHERE d.region_id = 1 "
          "GROUP BY d.dept_name ORDER BY d.dept_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      while (!name.empty() && name.back() == ' ') name.pop_back();
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

  printf("OK (2 groups, filtered root region_id=1)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: Scan-scan all four aggregate types                          */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.dept_name, COUNT(e.salary), SUM(e.salary),               */
/*          MIN(e.salary), MAX(e.salary)                               */
/*   FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id             */
/*   GROUP BY d.dept_name                                              */
/* ------------------------------------------------------------------ */

static int
testAllFourAggTypes(Ndb *ndb, MYSQL *conn)
{
  printf("Test 4: Scan-scan COUNT/SUM/MIN/MAX ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_DEPT);
  dict->invalidateTable(SS_EMP);
  dict->invalidateIndex("idx_emp_dept", SS_EMP);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(SS_EMP);
  const NdbDictionary::Index *empIdx =
      dict->getIndex("idx_emp_dept", SS_EMP);
  if (deptTab == nullptr || empTab == nullptr || empIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *deptNameCol = deptTab->getColumn("dept_name");

  NdbAggregator agg(empTab);
  if (!agg.GroupByLinked(0, deptNameCol) ||
      !agg.LoadColumn("salary", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Min(2, 0) ||
      !agg.Max(3, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *deptOp = qb->scanTable(deptTab);
  if (deptOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions empOpts;
  empOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  empOpts.setAggregation(agg);
  const NdbLinkedOperand *deptNameLink = qb->linkedValue(deptOp, "dept_name");
  empOpts.addLinkedProjection(deptNameLink);

  const NdbQueryIndexScanOperationDef *empOp =
      qb->scanIndex(empIdx, empTab, &bound, &empOpts);
  if (empOp == nullptr) {
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

  struct AggRow { Int64 count, sum, min_v, max_v; };
  std::map<std::string, AggRow> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    std::string name = extractCharColumn(nameCol);
    AggRow r;
    r.count = rec.FetchAggregationResult().data_int64();
    r.sum   = rec.FetchAggregationResult().data_int64();
    r.min_v = rec.FetchAggregationResult().data_int64();
    r.max_v = rec.FetchAggregationResult().data_int64();
    actual[name] = r;

    V("  dept_name='%s' COUNT=%lld SUM=%lld MIN=%lld MAX=%lld\n",
      name.c_str(), (long long)r.count, (long long)r.sum,
      (long long)r.min_v, (long long)r.max_v);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  struct Expected { Int64 count, sum, min_v, max_v; };
  std::map<std::string, Expected> expected = {
    {"Engineering", {3, 245000, 75000, 90000}},
    {"Sales",       {2, 125000, 60000, 65000}},
    {"Marketing",   {3, 165000, 52000, 58000}},
    {"Support",     {1, 45000,  45000, 45000}},
    {"Research",    {2, 183000, 88000, 95000}}
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
    const AggRow &r = it->second;
    const Expected &x = e.second;
    if (r.count != x.count || r.sum != x.sum ||
        r.min_v != x.min_v || r.max_v != x.max_v) {
      printf("FAILED (group '%s': expected C=%lld S=%lld m=%lld M=%lld, "
             "got C=%lld S=%lld m=%lld M=%lld)\n",
             e.first.c_str(),
             (long long)x.count, (long long)x.sum,
             (long long)x.min_v, (long long)x.max_v,
             (long long)r.count, (long long)r.sum,
             (long long)r.min_v, (long long)r.max_v);
      return -1;
    }
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT d.dept_name, COUNT(e.salary), SUM(e.salary), "
          "MIN(e.salary), MAX(e.salary) "
          "FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id "
          "GROUP BY d.dept_name ORDER BY d.dept_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      while (!name.empty() && name.back() == ' ') name.pop_back();
      auto it = actual.find(name);
      if (it == actual.end() ||
          it->second.count != atoll(row[1]) ||
          it->second.sum != atoll(row[2]) ||
          it->second.min_v != atoll(row[3]) ||
          it->second.max_v != atoll(row[4])) {
        printf("FAILED (MySQL cross-check: group '%s' mismatch)\n",
               name.c_str());
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (5 groups, all 4 aggregate types)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Group B: ss_dept_n + ss_emp_n (Test 5 — NULLs)                     */
/* ------------------------------------------------------------------ */

static int
createGroupBTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_emp_n");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_dept_n");

  if (sqlExec(conn,
        "CREATE TABLE ss_dept_n ("
        "  dept_id INT NOT NULL PRIMARY KEY,"
        "  dept_name CHAR(20) NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE ss_emp_n ("
        "  emp_id INT NOT NULL PRIMARY KEY,"
        "  dept_id INT NOT NULL,"
        "  salary BIGINT,"
        "  INDEX idx_empn_dept (dept_id)"
        ") ENGINE=NDB") != 0) return -1;

  V("Created %s, %s\n", SS_DEPT_N, SS_EMP_N);
  return 0;
}

static int
insertGroupBData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO ss_dept_n VALUES "
        "(1,'Alpha'),(2,'Beta'),(3,'Gamma')") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO ss_emp_n VALUES "
        "(201,1,80000),(202,1,NULL),(203,1,75000),"
        "(204,2,NULL),(205,2,NULL),"
        "(206,3,55000)") != 0) return -1;

  V("Inserted ss_dept_n/ss_emp_n data\n");
  return 0;
}

static int
dropGroupBTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_emp_n");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_dept_n");
  V("Dropped ss_dept_n, ss_emp_n\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: Scan-scan with NULL values                                  */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.dept_name, COUNT(e.salary), SUM(e.salary),               */
/*          MIN(e.salary), MAX(e.salary)                               */
/*   FROM ss_dept_n d JOIN ss_emp_n e ON e.dept_id = d.dept_id         */
/*   GROUP BY d.dept_name                                              */
/* ------------------------------------------------------------------ */

static int
testNullValues(Ndb *ndb, MYSQL *conn)
{
  printf("Test 5: Scan-scan with NULL values ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_DEPT_N);
  dict->invalidateTable(SS_EMP_N);
  dict->invalidateIndex("idx_empn_dept", SS_EMP_N);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT_N);
  const NdbDictionary::Table *empTab = dict->getTable(SS_EMP_N);
  const NdbDictionary::Index *empIdx =
      dict->getIndex("idx_empn_dept", SS_EMP_N);
  if (deptTab == nullptr || empTab == nullptr || empIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *deptNameCol = deptTab->getColumn("dept_name");

  NdbAggregator agg(empTab);
  if (!agg.GroupByLinked(0, deptNameCol) ||
      !agg.LoadColumn("salary", 0) ||
      !agg.Count(0, 0) ||
      !agg.Sum(1, 0) ||
      !agg.Min(2, 0) ||
      !agg.Max(3, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *deptOp = qb->scanTable(deptTab);
  if (deptOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions empOpts;
  empOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  empOpts.setAggregation(agg);
  const NdbLinkedOperand *deptNameLink = qb->linkedValue(deptOp, "dept_name");
  empOpts.addLinkedProjection(deptNameLink);

  const NdbQueryIndexScanOperationDef *empOp =
      qb->scanIndex(empIdx, empTab, &bound, &empOpts);
  if (empOp == nullptr) {
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
  struct AggRowNull { Int64 count; NullVal sum, min_v, max_v; };
  std::map<std::string, AggRowNull> actual;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    std::string name = extractCharColumn(nameCol);

    NdbAggregator::Result cntRes = rec.FetchAggregationResult();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxRes = rec.FetchAggregationResult();

    AggRowNull r;
    r.count = cntRes.data_int64();
    r.sum   = {sumRes.is_null(), sumRes.is_null() ? 0 : sumRes.data_int64()};
    r.min_v = {minRes.is_null(), minRes.is_null() ? 0 : minRes.data_int64()};
    r.max_v = {maxRes.is_null(), maxRes.is_null() ? 0 : maxRes.data_int64()};
    actual[name] = r;

    if (r.sum.null_flag) {
      V("  dept='%s' COUNT=%lld SUM=NULL MIN=NULL MAX=NULL\n",
        name.c_str(), (long long)r.count);
    } else {
      V("  dept='%s' COUNT=%lld SUM=%lld MIN=%lld MAX=%lld\n",
        name.c_str(), (long long)r.count,
        (long long)r.sum.value, (long long)r.min_v.value,
        (long long)r.max_v.value);
    }
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (actual.size() != 3) {
    printf("FAILED (expected 3 groups, got %zu)\n", actual.size());
    return -1;
  }

  /* Alpha: 2 non-null salaries (80000, 75000) + 1 NULL */
  {
    auto it = actual.find("Alpha");
    if (it == actual.end()) { printf("FAILED (missing Alpha)\n"); return -1; }
    const AggRowNull &r = it->second;
    if (r.count != 2 || r.sum.null_flag || r.sum.value != 155000 ||
        r.min_v.null_flag || r.min_v.value != 75000 ||
        r.max_v.null_flag || r.max_v.value != 80000) {
      printf("FAILED (Alpha values wrong)\n");
      return -1;
    }
  }

  /* Beta: all NULL salaries */
  {
    auto it = actual.find("Beta");
    if (it == actual.end()) { printf("FAILED (missing Beta)\n"); return -1; }
    const AggRowNull &r = it->second;
    if (r.count != 0 || !r.sum.null_flag ||
        !r.min_v.null_flag || !r.max_v.null_flag) {
      printf("FAILED (Beta: expected all NULL with COUNT=0)\n");
      return -1;
    }
  }

  /* Gamma: 1 non-null salary (55000) */
  {
    auto it = actual.find("Gamma");
    if (it == actual.end()) { printf("FAILED (missing Gamma)\n"); return -1; }
    const AggRowNull &r = it->second;
    if (r.count != 1 || r.sum.null_flag || r.sum.value != 55000 ||
        r.min_v.null_flag || r.min_v.value != 55000 ||
        r.max_v.null_flag || r.max_v.value != 55000) {
      printf("FAILED (Gamma values wrong)\n");
      return -1;
    }
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT d.dept_name, COUNT(e.salary), SUM(e.salary), "
          "MIN(e.salary), MAX(e.salary) "
          "FROM ss_dept_n d JOIN ss_emp_n e ON e.dept_id = d.dept_id "
          "GROUP BY d.dept_name ORDER BY d.dept_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      while (!name.empty() && name.back() == ' ') name.pop_back();
      auto it = actual.find(name);
      if (it == actual.end()) {
        printf("FAILED (MySQL cross-check: NDB missing group '%s')\n",
               name.c_str());
        mysql_free_result(result);
        return -1;
      }
      const AggRowNull &r = it->second;
      Int64 sqlCount = atoll(row[1]);
      bool sqlSumNull = (row[2] == nullptr);
      if (r.count != sqlCount || r.sum.null_flag != sqlSumNull) {
        printf("FAILED (MySQL cross-check: group '%s' mismatch)\n",
               name.c_str());
        mysql_free_result(result);
        return -1;
      }
      if (!sqlSumNull && r.sum.value != atoll(row[2])) {
        printf("FAILED (MySQL cross-check: group '%s' SUM mismatch)\n",
               name.c_str());
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (3 groups, NULL handling correct)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Group C: ss_region + ss_dept_r + ss_stat (Test 6 — 3-way)          */
/* ------------------------------------------------------------------ */

static int
createGroupCTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_stat");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_dept_r");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_region");

  if (sqlExec(conn,
        "CREATE TABLE ss_region ("
        "  region_id INT NOT NULL PRIMARY KEY,"
        "  region_name CHAR(20) NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE ss_dept_r ("
        "  dept_id INT NOT NULL PRIMARY KEY,"
        "  region_id INT NOT NULL,"
        "  INDEX idx_deptr_region (region_id)"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE ss_stat ("
        "  dept_id INT NOT NULL PRIMARY KEY,"
        "  budget BIGINT NOT NULL,"
        "  headcount INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  V("Created %s, %s, %s\n", SS_REGION, SS_DEPT_R, SS_STAT);
  return 0;
}

static int
insertGroupCData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO ss_region VALUES "
        "(1,'North'),(2,'South'),(3,'East')") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO ss_dept_r VALUES "
        "(1,1),(2,1),(3,2),(4,2),(5,3)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO ss_stat VALUES "
        "(1,500000,50),(2,300000,30),(3,400000,40),"
        "(4,200000,20),(5,600000,60)") != 0) return -1;

  V("Inserted ss_region/ss_dept_r/ss_stat data\n");
  return 0;
}

static int
dropGroupCTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_stat");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_dept_r");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_region");
  V("Dropped ss_region, ss_dept_r, ss_stat\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: 3-way scan-scan-lookup join                                 */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT r.region_name, SUM(s.budget), SUM(s.headcount)             */
/*   FROM ss_region r                                                  */
/*   JOIN ss_dept_r d ON d.region_id = r.region_id                     */
/*   JOIN ss_stat s ON s.dept_id = d.dept_id                           */
/*   GROUP BY r.region_name                                            */
/* ------------------------------------------------------------------ */

static int
testThreeWayScanScanLookup(Ndb *ndb, MYSQL *conn)
{
  printf("Test 6: 3-way scan-scan-lookup ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_REGION);
  dict->invalidateTable(SS_DEPT_R);
  dict->invalidateTable(SS_STAT);
  dict->invalidateIndex("idx_deptr_region", SS_DEPT_R);
  const NdbDictionary::Table *regionTab = dict->getTable(SS_REGION);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT_R);
  const NdbDictionary::Table *statTab = dict->getTable(SS_STAT);
  const NdbDictionary::Index *deptIdx =
      dict->getIndex("idx_deptr_region", SS_DEPT_R);
  if (regionTab == nullptr || deptTab == nullptr ||
      statTab == nullptr || deptIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *regionNameCol =
      regionTab->getColumn("region_name");

  /* Aggregation on stat (leaf): GROUP BY region_name (from grandparent),
   * SUM(budget), SUM(headcount) */
  NdbAggregator agg(statTab);
  if (!agg.GroupByLinked(0, regionNameCol) ||
      !agg.LoadColumn("budget", 0) ||
      !agg.Sum(0, 0) ||
      !agg.LoadColumn("headcount", 1) ||
      !agg.Sum(1, 1) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Node 0: scan ss_region (root) */
  const NdbQueryTableScanOperationDef *regionOp = qb->scanTable(regionTab);
  if (regionOp == nullptr) {
    printf("FAILED (scanTable region: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: scanIndex ss_dept_r (child scan, bound = region.region_id) */
  const NdbQueryOperand *deptBound[] = {
    qb->linkedValue(regionOp, "region_id"), nullptr
  };
  NdbQueryIndexBound deptIdxBound(deptBound);

  NdbQueryOptions deptOpts;
  deptOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryIndexScanOperationDef *deptOp =
      qb->scanIndex(deptIdx, deptTab, &deptIdxBound, &deptOpts);
  if (deptOp == nullptr) {
    printf("FAILED (scanIndex dept: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: readTuple ss_stat (lookup, key = dept.dept_id) */
  const NdbQueryOperand *statKey[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };

  NdbQueryOptions statOpts;
  statOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  statOpts.setAggregation(agg);
  const NdbLinkedOperand *regionNameLink =
      qb->linkedValue(regionOp, "region_name");
  statOpts.addLinkedProjection(regionNameLink);

  const NdbQueryLookupOperationDef *statOp =
      qb->readTuple(statTab, statKey, &statOpts);
  if (statOp == nullptr) {
    printf("FAILED (readTuple stat: %s)\n", qb->getNdbError().message);
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
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
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

  struct AggRow6 { Int64 sumBudget, sumHeadcount; };
  std::map<std::string, AggRow6> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    std::string name = extractCharColumn(nameCol);
    AggRow6 r;
    r.sumBudget = rec.FetchAggregationResult().data_int64();
    r.sumHeadcount = rec.FetchAggregationResult().data_int64();
    actual[name] = r;

    V("  region='%s' SUM(budget)=%lld SUM(headcount)=%lld\n",
      name.c_str(), (long long)r.sumBudget, (long long)r.sumHeadcount);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  struct Expected6 { Int64 sumBudget, sumHeadcount; };
  std::map<std::string, Expected6> expected = {
    {"North", {800000, 80}},
    {"South", {600000, 60}},
    {"East",  {600000, 60}}
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
    if (it->second.sumBudget != e.second.sumBudget ||
        it->second.sumHeadcount != e.second.sumHeadcount) {
      printf("FAILED (group '%s': expected budget=%lld hc=%lld, "
             "got budget=%lld hc=%lld)\n",
             e.first.c_str(),
             (long long)e.second.sumBudget, (long long)e.second.sumHeadcount,
             (long long)it->second.sumBudget,
             (long long)it->second.sumHeadcount);
      return -1;
    }
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT r.region_name, SUM(s.budget), SUM(s.headcount) "
          "FROM ss_region r "
          "JOIN ss_dept_r d ON d.region_id = r.region_id "
          "JOIN ss_stat s ON s.dept_id = d.dept_id "
          "GROUP BY r.region_name ORDER BY r.region_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      while (!name.empty() && name.back() == ' ') name.pop_back();
      auto it = actual.find(name);
      if (it == actual.end() ||
          it->second.sumBudget != atoll(row[1]) ||
          it->second.sumHeadcount != atoll(row[2])) {
        printf("FAILED (MySQL cross-check: group '%s' mismatch)\n",
               name.c_str());
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (3 groups, 3-way scan-scan-lookup)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 7: scanIndex root + scanIndex child (both bounded)             */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT d.dept_name, SUM(e.salary)                                 */
/*   FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id             */
/*   WHERE d.region_id BETWEEN 1 AND 2                                 */
/*   GROUP BY d.dept_name                                              */
/* ------------------------------------------------------------------ */

static int
testDualScanIndex(Ndb *ndb, MYSQL *conn)
{
  printf("Test 7: scanIndex root + scanIndex child ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_DEPT);
  dict->invalidateTable(SS_EMP);
  dict->invalidateIndex("idx_dept_region", SS_DEPT);
  dict->invalidateIndex("idx_emp_dept", SS_EMP);
  const NdbDictionary::Table *deptTab = dict->getTable(SS_DEPT);
  const NdbDictionary::Table *empTab = dict->getTable(SS_EMP);
  const NdbDictionary::Index *deptRegionIdx =
      dict->getIndex("idx_dept_region", SS_DEPT);
  const NdbDictionary::Index *empIdx =
      dict->getIndex("idx_emp_dept", SS_EMP);
  if (deptTab == nullptr || empTab == nullptr ||
      deptRegionIdx == nullptr || empIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *deptNameCol = deptTab->getColumn("dept_name");

  NdbAggregator agg(empTab);
  if (!agg.GroupByLinked(0, deptNameCol) ||
      !agg.LoadColumn("salary", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Root: scanIndex on ss_dept with region_id BETWEEN 1 AND 2 */
  const NdbQueryOperand *rootLow[] = {
    qb->constValue((Int32)1), nullptr
  };
  const NdbQueryOperand *rootHigh[] = {
    qb->constValue((Int32)2), nullptr
  };
  NdbQueryIndexBound rootBound(rootLow, true, rootHigh, true);

  const NdbQueryIndexScanOperationDef *deptOp =
      qb->scanIndex(deptRegionIdx, deptTab, &rootBound);
  if (deptOp == nullptr) {
    printf("FAILED (scanIndex dept: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Child: scanIndex on ss_emp, bound = dept.dept_id */
  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(deptOp, "dept_id"), nullptr
  };
  NdbQueryIndexBound empBound(childBound);

  NdbQueryOptions empOpts;
  empOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  empOpts.setAggregation(agg);
  const NdbLinkedOperand *deptNameLink = qb->linkedValue(deptOp, "dept_name");
  empOpts.addLinkedProjection(deptNameLink);

  const NdbQueryIndexScanOperationDef *empOp =
      qb->scanIndex(empIdx, empTab, &empBound, &empOpts);
  if (empOp == nullptr) {
    printf("FAILED (scanIndex emp: %s)\n", qb->getNdbError().message);
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
    std::string name = extractCharColumn(nameCol);
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    actual[name] = sumRes.data_int64();
    V("  dept_name='%s' SUM(salary)=%lld\n",
      name.c_str(), (long long)actual[name]);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Regions 1 and 2 cover Engineering, Sales, Marketing, Support */
  std::map<std::string, Int64> expected = {
    {"Engineering", 245000}, {"Sales", 125000},
    {"Marketing", 165000}, {"Support", 45000}
  };

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() || it->second != e.second) {
      printf("FAILED (group '%s' mismatch)\n", e.first.c_str());
      return -1;
    }
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT d.dept_name, SUM(e.salary) "
          "FROM ss_dept d JOIN ss_emp e ON e.dept_id = d.dept_id "
          "WHERE d.region_id BETWEEN 1 AND 2 "
          "GROUP BY d.dept_name ORDER BY d.dept_name") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string name(row[0]);
      while (!name.empty() && name.back() == ' ') name.pop_back();
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

  printf("OK (4 groups, dual scanIndex region_id BETWEEN 1 AND 2)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Group D: ss_project + ss_task (Test 8 — composite index)           */
/* ------------------------------------------------------------------ */

static int
createGroupDTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_task");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_project");

  if (sqlExec(conn,
        "CREATE TABLE ss_project ("
        "  proj_id INT NOT NULL PRIMARY KEY,"
        "  region_id INT NOT NULL,"
        "  proj_year INT NOT NULL"
        ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
        "CREATE TABLE ss_task ("
        "  task_id INT NOT NULL PRIMARY KEY,"
        "  region_id INT NOT NULL,"
        "  proj_year INT NOT NULL,"
        "  hours BIGINT NOT NULL,"
        "  INDEX idx_task_rg_yr (region_id, proj_year)"
        ") ENGINE=NDB") != 0) return -1;

  V("Created %s, %s\n", SS_PROJECT, SS_TASK);
  return 0;
}

static int
insertGroupDData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO ss_project VALUES "
        "(1,1,2024),(2,1,2025),(3,2,2024)") != 0) return -1;

  if (sqlExec(conn,
        "INSERT INTO ss_task VALUES "
        "(101,1,2024,100),(102,1,2024,150),"
        "(103,1,2025,200),"
        "(104,2,2024,120),(105,2,2024,180)") != 0) return -1;

  V("Inserted ss_project/ss_task data\n");
  return 0;
}

static int
dropGroupDTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_task");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_project");
  V("Dropped ss_project, ss_task\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 8: Scan-scan with composite index bounds                       */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT p.proj_year, SUM(t.hours)                                  */
/*   FROM ss_project p                                                 */
/*   JOIN ss_task t ON t.region_id = p.region_id                       */
/*                 AND t.proj_year = p.proj_year                       */
/*   GROUP BY p.proj_year                                              */
/* ------------------------------------------------------------------ */

static int
testCompositeIndexBounds(Ndb *ndb, MYSQL *conn)
{
  printf("Test 8: Scan-scan with composite index bounds ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_PROJECT);
  dict->invalidateTable(SS_TASK);
  dict->invalidateIndex("idx_task_rg_yr", SS_TASK);
  const NdbDictionary::Table *projTab = dict->getTable(SS_PROJECT);
  const NdbDictionary::Table *taskTab = dict->getTable(SS_TASK);
  const NdbDictionary::Index *taskIdx =
      dict->getIndex("idx_task_rg_yr", SS_TASK);
  if (projTab == nullptr || taskTab == nullptr || taskIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *projYearCol = projTab->getColumn("proj_year");

  /* Aggregation: GROUP BY proj_year (linked from project), SUM(hours) */
  NdbAggregator agg(taskTab);
  if (!agg.GroupByLinked(0, projYearCol) ||
      !agg.LoadColumn("hours", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Root: scan ss_project */
  const NdbQueryTableScanOperationDef *projOp = qb->scanTable(projTab);
  if (projOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Child: scanIndex on ss_task with composite bound
   * [region_id, proj_year] from project */
  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(projOp, "region_id"),
    qb->linkedValue(projOp, "proj_year"),
    nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions taskOpts;
  taskOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  taskOpts.setAggregation(agg);
  const NdbLinkedOperand *yearLink = qb->linkedValue(projOp, "proj_year");
  taskOpts.addLinkedProjection(yearLink);

  const NdbQueryIndexScanOperationDef *taskOp =
      qb->scanIndex(taskIdx, taskTab, &bound, &taskOpts);
  if (taskOp == nullptr) {
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

    NdbAggregator::Column yearCol = rec.FetchGroupbyColumn();
    Int32 year = yearCol.data_int32();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumHours = sumRes.data_int64();

    actual[year] = sumHours;
    V("  proj_year=%d SUM(hours)=%lld\n", year, (long long)sumHours);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* proj 1 (region=1,year=2024) → tasks 101(100),102(150) = 250
   * proj 3 (region=2,year=2024) → tasks 104(120),105(180) = 300
   * Total 2024 = 550
   * proj 2 (region=1,year=2025) → task 103(200) = 200
   * Total 2025 = 200 */
  std::map<Int32, Int64> expected = {
    {2024, 550}, {2025, 200}
  };

  if (actual.size() != expected.size()) {
    printf("FAILED (expected %zu groups, got %zu)\n",
           expected.size(), actual.size());
    return -1;
  }

  for (const auto &e : expected) {
    auto it = actual.find(e.first);
    if (it == actual.end() || it->second != e.second) {
      printf("FAILED (year %d: expected %lld, got %lld)\n",
             e.first, (long long)e.second,
             it == actual.end() ? -1LL : (long long)it->second);
      return -1;
    }
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT p.proj_year, SUM(t.hours) "
          "FROM ss_project p "
          "JOIN ss_task t ON t.region_id = p.region_id "
          "                AND t.proj_year = p.proj_year "
          "GROUP BY p.proj_year ORDER BY p.proj_year") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      Int32 year = atoi(row[0]);
      Int64 sqlSum = atoll(row[1]);
      auto it = actual.find(year);
      if (it == actual.end() || it->second != sqlSum) {
        printf("FAILED (MySQL cross-check: year %d mismatch)\n", year);
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (2 groups, composite index bounds)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Group E: ss_store_e + ss_sale_e (Test 9 — eviction)                 */
/* ------------------------------------------------------------------ */
#if defined(VM_TRACE) || defined(ERROR_INSERT)

static int
createGroupETables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_sale_e");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_store_e");

  if (sqlExec(conn,
        "CREATE TABLE ss_store_e ("
        "  store_id INT NOT NULL PRIMARY KEY,"
        "  region CHAR(20) NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", SS_STORE_E);

  if (sqlExec(conn,
        "CREATE TABLE ss_sale_e ("
        "  sale_id INT NOT NULL PRIMARY KEY,"
        "  store_id INT NOT NULL,"
        "  amount BIGINT NOT NULL,"
        "  INDEX idx_sale_store (store_id)"
        ") ENGINE=NDB") != 0) return -1;
  V("Created table %s\n", SS_SALE_E);

  return 0;
}

static int
insertGroupEData(MYSQL *conn)
{
  /*
   * 60 stores across 20 regions (3 per region):
   *   store 1-3  → "Region_01", 4-6  → "Region_02", ..., 58-60 → "Region_20"
   *
   * 600 sales: sale_id = 1..600, store_id = ((sale_id-1) % 60) + 1
   *   → 10 sales per store, 30 sales per region
   *   amount = sale_id * 10
   *
   * 20 groups ensures the eviction condition (gb_map size > 2) is well
   * exercised, and 600 rows gives many eviction opportunities (~85 fires
   * at modulo-7 with >=3 groups).
   */
  char buf[4096];
  int pos = snprintf(buf, sizeof(buf), "INSERT INTO ss_store_e VALUES ");
  for (int s = 1; s <= 60; s++) {
    if (s > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
    int region = ((s - 1) / 3) + 1;
    pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,'Region_%02d')",
                    s, region);
  }
  if (sqlExec(conn, buf) != 0) return -1;
  V("Inserted 60 ss_store_e rows\n");

  char bigbuf[32768];
  pos = snprintf(bigbuf, sizeof(bigbuf), "INSERT INTO ss_sale_e VALUES ");
  for (int i = 1; i <= 600; i++) {
    if (i > 1) pos += snprintf(bigbuf + pos, sizeof(bigbuf) - pos, ",");
    int storeId = ((i - 1) % 60) + 1;
    pos += snprintf(bigbuf + pos, sizeof(bigbuf) - pos, "(%d,%d,%d)",
                    i, storeId, i * 10);
  }
  if (sqlExec(conn, bigbuf) != 0) return -1;
  V("Inserted 600 ss_sale_e rows\n");

  return 0;
}

static int
dropGroupETables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS ss_sale_e");
  sqlExec(conn, "DROP TABLE IF EXISTS ss_store_e");
  V("Dropped ss_store_e, ss_sale_e\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 9: Forced eviction via ERROR_INSERT 4040 (scan-scan)           */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   SELECT s.region, SUM(l.amount)                                    */
/*   FROM ss_store_e s JOIN ss_sale_e l ON l.store_id = s.store_id     */
/*   GROUP BY s.region                                                 */
/*                                                                     */
/* 60 stores, 600 sales, 20 region groups (30 sales each).             */
/* ERROR_INSERT 4040 forces intermittent group eviction (~every 7th    */
/* row when >=3 groups). With 20 groups and 600 rows, eviction fires   */
/* frequently, creating many merge opportunities in DBSPJ.             */
/* ------------------------------------------------------------------ */

static int
testEvictionScanScan(Ndb *ndb, MYSQL *conn, NdbRestarter &restarter)
{
  printf("Test 9: Forced eviction (ERROR_INSERT 4040) scan-scan ... ");
  fflush(stdout);

  if (restarter.insertErrorInAllNodes(4040) != 0) {
    printf("FAILED (insertErrorInAllNodes(4040))\n");
    return -1;
  }
  V("\n  ERROR_INSERT 4040 set\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SS_STORE_E);
  dict->invalidateTable(SS_SALE_E);
  dict->invalidateIndex("idx_sale_store", SS_SALE_E);
  const NdbDictionary::Table *storeTab = dict->getTable(SS_STORE_E);
  const NdbDictionary::Table *saleTab = dict->getTable(SS_SALE_E);
  const NdbDictionary::Index *saleIdx =
      dict->getIndex("idx_sale_store", SS_SALE_E);
  if (storeTab == nullptr || saleTab == nullptr || saleIdx == nullptr) {
    printf("FAILED (table/index lookup: %s)\n", dict->getNdbError().message);
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  const NdbDictionary::Column *regionCol = storeTab->getColumn("region");

  NdbAggregator agg(saleTab);
  if (!agg.GroupByLinked(0, regionCol) ||
      !agg.LoadColumn("amount", 0) ||
      !agg.Sum(0, 0) ||
      !agg.Finalize()) {
    printf("FAILED (agg program: %s)\n", agg.GetError().err_msg_);
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  const NdbQueryTableScanOperationDef *storeOp = qb->scanTable(storeTab);
  if (storeOp == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    qb->linkedValue(storeOp, "store_id"), nullptr
  };
  NdbQueryIndexBound bound(childBound);

  NdbQueryOptions saleOpts;
  saleOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  saleOpts.setAggregation(agg);
  const NdbLinkedOperand *regionLink = qb->linkedValue(storeOp, "region");
  saleOpts.addLinkedProjection(regionLink);

  const NdbQueryIndexScanOperationDef *saleOp =
      qb->scanIndex(saleIdx, saleTab, &bound, &saleOpts);
  if (saleOp == nullptr) {
    printf("FAILED (scanIndex: %s)\n", qb->getNdbError().message);
    qb->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction: %s)\n", ndb->getNdbError().message);
    queryDef->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }
  V("  Scan consumed %u rows\n", rowCount);

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator returned nullptr)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    restarter.insertErrorInAllNodes(0);
    return -1;
  }

  std::map<std::string, Int64> actual;
  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column regionGbCol = rec.FetchGroupbyColumn();
    std::string region = extractCharColumn(regionGbCol);
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    Int64 sumVal = sumRes.data_int64();

    actual[region] = sumVal;
    V("  region='%s' SUM(amount)=%lld\n", region.c_str(), (long long)sumVal);
  }

  query->close();
  trans->close();
  queryDef->destroy();

  restarter.insertErrorInAllNodes(0);
  V("  ERROR_INSERT cleared\n");

  if (actual.size() != 20) {
    printf("FAILED (expected 20 groups, got %zu)\n", actual.size());
    return -1;
  }

  /* Cross-check with MySQL */
  {
    if (mysql_query(conn,
          "SELECT s.region, SUM(l.amount) "
          "FROM ss_store_e s JOIN ss_sale_e l ON l.store_id = s.store_id "
          "GROUP BY s.region ORDER BY s.region") != 0) {
      printf("FAILED (MySQL cross-check: %s)\n", mysql_error(conn));
      return -1;
    }
    MYSQL_RES *result = mysql_store_result(conn);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      std::string region(row[0]);
      while (!region.empty() && region.back() == ' ') region.pop_back();
      Int64 sqlSum = atoll(row[1]);
      auto it = actual.find(region);
      if (it == actual.end() || it->second != sqlSum) {
        printf("FAILED (MySQL cross-check: region '%s' mismatch: "
               "NDB=%lld SQL=%lld)\n",
               region.c_str(),
               it == actual.end() ? -1LL : (long long)it->second,
               (long long)sqlSum);
        mysql_free_result(result);
        return -1;
      }
    }
    mysql_free_result(result);
    V("  MySQL cross-check OK\n");
  }

  printf("OK (20 groups, eviction merge verified)\n");
  return 0;
}

#endif  /* VM_TRACE || ERROR_INSERT */

/* ------------------------------------------------------------------ */
/* usage / main                                                        */
/* ------------------------------------------------------------------ */

static void
usage(const char *prog)
{
  printf("Usage: %s [options]\n"
         "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
         "  -m <mysql_port>      MySQL port (default: 3306)\n"
         "  -v, --verbose        Verbose output\n"
         "  -h, --help           Show this help\n",
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

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("=== testJoinAggScanScan ===\n");
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
          /* Tests 1-4, 7: Group A tables (ss_dept + ss_emp) */
          if (createGroupATables(conn) == 0 &&
              insertGroupAData(conn) == 0) {
            if (testBasicSumGroupBy(&ndb, conn) != 0) exitCode = 1;
            if (testGlobalCountSum(&ndb, conn) != 0) exitCode = 1;
            if (testFilteredRoot(&ndb, conn) != 0) exitCode = 1;
            if (testAllFourAggTypes(&ndb, conn) != 0) exitCode = 1;
            if (testDualScanIndex(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropGroupATables(conn);

          /* Test 5: Group B tables (ss_dept_n + ss_emp_n with NULLs) */
          if (createGroupBTables(conn) == 0 &&
              insertGroupBData(conn) == 0) {
            if (testNullValues(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropGroupBTables(conn);

          /* Test 6: Group C tables (ss_region + ss_dept_r + ss_stat) */
          if (createGroupCTables(conn) == 0 &&
              insertGroupCData(conn) == 0) {
            if (testThreeWayScanScanLookup(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropGroupCTables(conn);

          /* Test 8: Group D tables (ss_project + ss_task) */
          if (createGroupDTables(conn) == 0 &&
              insertGroupDData(conn) == 0) {
            if (testCompositeIndexBounds(&ndb, conn) != 0) exitCode = 1;
          } else {
            exitCode = 1;
          }
          dropGroupDTables(conn);

          /* Test 9: Group E tables (ss_store_e + ss_sale_e) — eviction */
#if defined(VM_TRACE) || defined(ERROR_INSERT)
          {
            NdbRestarter restarter(connectString);
            if (createGroupETables(conn) == 0 &&
                insertGroupEData(conn) == 0) {
              if (testEvictionScanScan(&ndb, conn, restarter) != 0)
                exitCode = 1;
            } else {
              exitCode = 1;
            }
            dropGroupETables(conn);
          }
#else
          printf("Test 9: SKIPPED "
                 "(production build, ERROR_INSERT unavailable)\n");
#endif
        }

        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  if (exitCode == 0) {
    (void)write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return exitCode;
}
