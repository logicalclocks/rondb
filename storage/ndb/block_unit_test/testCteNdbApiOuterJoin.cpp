/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * testCteNdbApiOuterJoin — CTE operations participating in outer joins.
 *
 * Consolidated coverage of the outer-join shapes shipped for CTEs:
 *   Phase 1  — main SELECT scanTable LEFT JOIN lookupCte (API auto-fills
 *              NULL for unmatched parents via NdbResultStream).
 *   Phase 2  — scanCte as LEFT-side parent (cross-combination with the
 *              regular readTuple child NULL-row machinery).
 *   Phase 3  — dropped (CTE_SCAN as outer-join child — see
 *              cte_outer_join_phase_3.md).
 *
 * Schema (created via MySQL):
 *   oj_cte_src(pk INT PK, grp INT, val BIGINT)         -- CTE source
 *   oj_cte_virtual(grp INT PK, total BIGINT)           -- virtual table
 *                                                        for lookupCte/scanCte
 *   oj_rhs(id INT PK, label CHAR(8))                   -- outer-join RHS
 *
 * Test data:
 *   oj_cte_src: (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)
 *   → CTE0 (GROUP BY grp, SUM(val))
 *     = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)}
 *   oj_rhs:     (1,'one'),(3,'three'),(5,'five')
 *               -- id=1,3 match CTE groups; id=5 does NOT, so LEFT JOIN
 *               -- against CTE must NULL-fill cte cols for id=5.
 *
 * Usage: testCteNdbApiOuterJoin -c <connect_string> -m <mysql_port> [-v]
 *                               [--only N]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbAggregator.hpp>
#include <NdbSleep.h>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryBuilderImpl.hpp"
#include "NdbQueryOperation.hpp"

#include <mysql.h>

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

static const char *TEST_DB         = "test";
static const char *CTE_SRC_TABLE   = "oj_cte_src";
static const char *CTE_VIRT_TABLE  = "oj_cte_virtual";
static const char *RHS_TABLE       = "oj_rhs";

/* ------------------------------------------------------------------ */
/* MySQL helpers                                                       */
/* ------------------------------------------------------------------ */

static MYSQL *
connectMysql(int port)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) return nullptr;
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         TEST_DB, port, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

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

/* ------------------------------------------------------------------ */
/* Schema setup                                                        */
/* ------------------------------------------------------------------ */

static int
createTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj_cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_cte_virtual");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_rhs");

  if (sqlExec(conn,
      "CREATE TABLE oj_cte_src ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  grp INT NOT NULL,"
      "  val BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
      "CREATE TABLE oj_cte_virtual ("
      "  grp INT NOT NULL PRIMARY KEY,"
      "  total BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
      "CREATE TABLE oj_rhs ("
      "  id INT NOT NULL PRIMARY KEY,"
      "  label CHAR(8) NOT NULL"
      ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  return 0;
}

static int
insertTestData(MYSQL *conn)
{
  /* CTE 0 = SELECT grp, SUM(val) FROM oj_cte_src GROUP BY grp
   *        = {(1,30),(2,70),(3,50)} */
  if (sqlExec(conn,
      "INSERT INTO oj_cte_src VALUES "
      "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)") != 0) return -1;

  /* id=1,3 match CTE groups; id=5 does NOT (CTE has no grp=5).
   * Tests 1/2 drive scanCte vs oj_rhs (grp=2 unmatched on rhs side);
   * Tests 3/4 drive scanTable(oj_rhs) vs lookupCte (id=5 unmatched on
   * CTE side). */
  return sqlExec(conn,
      "INSERT INTO oj_rhs VALUES (1,'one'),(3,'three'),(5,'five')");
}

static void
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS oj_cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_cte_virtual");
  sqlExec(conn, "DROP TABLE IF EXISTS oj_rhs");
}

/* ------------------------------------------------------------------ */
/* Shared CTE 0 aggregator builder                                     */
/* ------------------------------------------------------------------ */

static bool
buildCteAgg(NdbAggregator &agg)
{
  return agg.GroupBy("grp") &&
         agg.LoadColumn("val", 0) &&
         agg.Sum(0, 0) &&
         agg.Finalize();
}

/* ------------------------------------------------------------------ */
/* Test 1 (Phase 2 baseline):                                          */
/*   scanCte(0) INNER JOIN readTuple(oj_rhs, id = cte.grp)             */
/*                                                                     */
/* CTE 0 materialises to 3 groups {1,2,3}; oj_rhs has id={1,3}.        */
/* With MatchNonNull on the child, only grp=1 and grp=3 survive the    */
/* join → 2 rows expected.                                             */
/*                                                                     */
/* Proves the scanCte-as-parent plumbing is correct without NULL-row   */
/* synthesis in the mix.                                               */
/* ------------------------------------------------------------------ */

static int
testScanCteInnerJoin(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 1: scanCte INNER JOIN readTuple ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRT_TABLE);
  dict->invalidateTable(RHS_TABLE);
  const NdbDictionary::Table *srcTab  = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRT_TABLE);
  const NdbDictionary::Table *rhsTab  = dict->getTable(RHS_TABLE);
  if (srcTab == nullptr || virtTab == nullptr || rhsTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!buildCteAgg(cteAgg)) {
    printf("FAILED (cteAgg build)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* Build CTE 0: scanTable -> readTuple(pk) aggregate-leaf. */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &leafOpts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  /* Main: scanCte(0) INNER JOIN readTuple(oj_rhs, id = cte.grp). */
  const NdbQueryCteScanOperationDef *cteScanOp =
      qb->scanCte(0, 2, virtTab, nullptr);
  if (cteScanOp == nullptr) {
    printf("FAILED (scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *rhsKey[] = {
      qb->linkedValue(cteScanOp, "grp"), nullptr
  };
  NdbQueryOptions innerOpts;
  innerOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  if (qb->readTuple(rhsTab, rhsKey, &innerOpts) == nullptr) {
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
  const Uint32 nOps = queryDef->getNoOfOperations();
  NdbQueryOperation *cteOp = query->getQueryOperation(nOps - 2);
  NdbQueryOperation *rhsOp = query->getQueryOperation(nOps - 1);
  NdbRecAttr *raGrp   = cteOp ? cteOp->getValue("grp")   : nullptr;
  NdbRecAttr *raTotal = cteOp ? cteOp->getValue("total") : nullptr;
  NdbRecAttr *raLabel = rhsOp ? rhsOp->getValue("label") : nullptr;

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 g = raGrp ? raGrp->int32_value() : -1;
    Int64 t = raTotal ? raTotal->int64_value() : -1;
    V("  row %u: grp=%d total=%lld label=%s\n",
      rowCount, g, (long long)t,
      (raLabel && raLabel->isNULL() == 0) ? raLabel->aRef() : "<NULL>");
    if (g == 2) {
      printf("FAILED (grp=2 should have been dropped by INNER JOIN)\n");
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != 2) {
    printf("FAILED (expected 2 rows, got %u)\n", rowCount);
    return -1;
  }
  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2 (Phase 2):                                                   */
/*   scanCte(0) LEFT JOIN readTuple(oj_rhs, id = cte.grp)              */
/*                                                                     */
/* Same shape as Test 1 but child built without setMatchType →         */
/* MatchAll (LEFT JOIN).  All 3 CTE groups must survive; grp=2 has no  */
/* matching oj_rhs row and must come back with a NULL label.           */
/*                                                                     */
/* Verifies that scanCte-driven parent rows integrate with the         */
/* existing scanFrag + lookup-child NULL-row machinery (API            */
/* NdbResultStream auto-fill path, same as Phase 1).                   */
/* ------------------------------------------------------------------ */

static int
testScanCteLeftJoin(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 2: scanCte LEFT JOIN readTuple ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRT_TABLE);
  dict->invalidateTable(RHS_TABLE);
  const NdbDictionary::Table *srcTab  = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRT_TABLE);
  const NdbDictionary::Table *rhsTab  = dict->getTable(RHS_TABLE);
  if (srcTab == nullptr || virtTab == nullptr || rhsTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!buildCteAgg(cteAgg)) {
    printf("FAILED (cteAgg build)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &leafOpts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  const NdbQueryCteScanOperationDef *cteScanOp =
      qb->scanCte(0, 2, virtTab, nullptr);
  if (cteScanOp == nullptr) {
    printf("FAILED (scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *rhsKey[] = {
      qb->linkedValue(cteScanOp, "grp"), nullptr
  };
  /* No setMatchType → default MatchAll = LEFT JOIN. */
  if (qb->readTuple(rhsTab, rhsKey, nullptr) == nullptr) {
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
  const Uint32 nOps = queryDef->getNoOfOperations();
  NdbQueryOperation *cteOp = query->getQueryOperation(nOps - 2);
  NdbQueryOperation *rhsOp = query->getQueryOperation(nOps - 1);
  NdbRecAttr *raGrp   = cteOp ? cteOp->getValue("grp")   : nullptr;
  NdbRecAttr *raTotal = cteOp ? cteOp->getValue("total") : nullptr;
  NdbRecAttr *raLabel = rhsOp ? rhsOp->getValue("label") : nullptr;

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  Uint32 grp2NullLabel = 0;
  bool seenGrp[4] = { false, false, false, false };
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 g = raGrp ? raGrp->int32_value() : -1;
    Int64 t = raTotal ? raTotal->int64_value() : -1;
    /* Outer-join unmatched-child detection is operation-level
     * (isRowNULL()), not column-level: readTuple has no MATCH the
     * NdbRecAttr was never populated for. */
    const bool rhsIsNull = (rhsOp == nullptr || rhsOp->isRowNULL());
    V("  row %u: grp=%d total=%lld label=%s\n",
      rowCount, g, (long long)t,
      rhsIsNull ? "<NULL>" : raLabel->aRef());
    if (g < 1 || g > 3 || seenGrp[g]) {
      printf("FAILED (unexpected grp=%d at row %u)\n", g, rowCount);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    seenGrp[g] = true;
    if (g == 2 && rhsIsNull) grp2NullLabel++;
    if (g != 2 && rhsIsNull) {
      printf("FAILED (grp=%d unexpectedly had NULL rhs row)\n", g);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != 3) {
    printf("FAILED (expected 3 rows, got %u)\n", rowCount);
    return -1;
  }
  if (grp2NullLabel != 1) {
    printf("FAILED (expected grp=2 with NULL label, got %u)\n",
           grp2NullLabel);
    return -1;
  }
  printf("OK (%u rows, grp=2 null-padded)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3 (Phase 1):                                                   */
/*   scanTable(oj_rhs) LEFT JOIN lookupCte(0, grp = rhs.id)            */
/*                                                                     */
/* oj_rhs has id={1,3,5}. CTE 0 has groups {1,2,3}. Every rhs row      */
/* survives the LEFT JOIN; id=5 has no CTE match so its CTE columns    */
/* come back NULL (detected via cteOp->isRowNULL()).                   */
/* ------------------------------------------------------------------ */

static int
testMainLookupCteLeftJoin(Ndb *ndb, MYSQL * /*conn*/, Uint32 batchSize)
{
  printf("Test %s: scanTable LEFT JOIN lookupCte%s ... ",
         batchSize == 1 ? "4" : "3",
         batchSize == 1 ? " (batchSize=1)" : "");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRT_TABLE);
  dict->invalidateTable(RHS_TABLE);
  const NdbDictionary::Table *srcTab  = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRT_TABLE);
  const NdbDictionary::Table *rhsTab  = dict->getTable(RHS_TABLE);
  if (srcTab == nullptr || virtTab == nullptr || rhsTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!buildCteAgg(cteAgg)) {
    printf("FAILED (cteAgg build)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &leafOpts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  /* Main: scanTable(oj_rhs) LEFT JOIN lookupCte(0, id). */
  const NdbQueryTableScanOperationDef *mainScan = qb->scanTable(rhsTab);
  if (mainScan == nullptr) {
    printf("FAILED (scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScan, "id"), nullptr
  };
  /* No setMatchType → default MatchAll (LEFT JOIN). */
  if (qb->lookupCte(0, 2, virtTab, cteKey, nullptr) == nullptr) {
    printf("FAILED (lookupCte: %s)\n", qb->getNdbError().message);
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
  const Uint32 nOps = queryDef->getNoOfOperations();
  NdbQueryOperation *mainOp = query->getQueryOperation(nOps - 2);
  NdbQueryOperation *cteOp  = query->getQueryOperation(nOps - 1);
  NdbRecAttr *raId    = mainOp ? mainOp->getValue("id")    : nullptr;
  NdbRecAttr *raGrp   = cteOp  ? cteOp->getValue("grp")    : nullptr;
  NdbRecAttr *raTotal = cteOp  ? cteOp->getValue("total")  : nullptr;

  if (batchSize > 0 && mainOp->setBatchSize(batchSize) != 0) {
    printf("FAILED (setBatchSize: %s)\n", query->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  Uint32 id5NullCte = 0;
  bool seenId[6] = { false, false, false, false, false, false };
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 i = raId ? raId->int32_value() : -1;
    const bool cteIsNull = (cteOp == nullptr || cteOp->isRowNULL());
    Int32 g = (!cteIsNull && raGrp)   ? raGrp->int32_value()   : -1;
    Int64 t = (!cteIsNull && raTotal) ? raTotal->int64_value() : -1;
    V("  row %u: id=%d cte=%s grp=%d total=%lld\n",
      rowCount, i, cteIsNull ? "<NULL>" : "", g, (long long)t);
    if (i < 1 || i > 5 || seenId[i]) {
      printf("FAILED (unexpected id=%d at row %u)\n", i, rowCount);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    seenId[i] = true;
    if (i == 5 && cteIsNull) id5NullCte++;
    if (i != 5 && cteIsNull) {
      printf("FAILED (id=%d unexpectedly had NULL cte row)\n", i);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != 3) {
    printf("FAILED (expected 3 rows, got %u)\n", rowCount);
    return -1;
  }
  if (id5NullCte != 1) {
    printf("FAILED (expected id=5 with NULL cte row, got %u)\n", id5NullCte);
    return -1;
  }
  printf("OK (%u rows, id=5 null-padded)\n", rowCount);
  return 0;
}

static int
testMainLookupCteLeftJoinDefaultBatch(Ndb *ndb, MYSQL *conn)
{
  return testMainLookupCteLeftJoin(ndb, conn, 0);
}

static int
testMainLookupCteLeftJoinSmallBatch(Ndb *ndb, MYSQL *conn)
{
  return testMainLookupCteLeftJoin(ndb, conn, 1);
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

struct TestEntry {
  int number;
  int (*fn)(Ndb *, MYSQL *);
};

static const TestEntry g_tests[] = {
    { 1, testScanCteInnerJoin },
    { 2, testScanCteLeftJoin  },
    { 3, testMainLookupCteLeftJoinDefaultBatch },
    { 4, testMainLookupCteLeftJoinSmallBatch },
};
static const size_t g_test_count = sizeof(g_tests) / sizeof(g_tests[0]);

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;
  int onlyTest = -1;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
      connectString = argv[++i];
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
      mysqlPort = atoi(argv[++i]);
    else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0)
      verbose = true;
    else if (strcmp(argv[i], "--only") == 0 && i + 1 < argc)
      onlyTest = atoi(argv[++i]);
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s -c <connect_string> -m <mysql_port> [-v] "
             "[--only N]\n", argv[0]);
      return 0;
    }
  }

  if (onlyTest != -1) {
    bool found = false;
    for (size_t i = 0; i < g_test_count; i++) {
      if (g_tests[i].number == onlyTest) { found = true; break; }
    }
    if (!found) {
      fprintf(stderr, "No such test: %d (valid: 1..%zu)\n",
              onlyTest, g_test_count);
      return 1;
    }
  }

  printf("=== testCteNdbApiOuterJoin ===\n");
  printf("Connect: %s, MySQL port: %d\n", connectString, mysqlPort);
  if (onlyTest != -1) printf("Filter: --only %d\n", onlyTest);
  printf("\n");

  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int exitCode = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster\n");
      exitCode = 1;
    }
    else if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      exitCode = 1;
    }
    else {
      MYSQL *conn = connectMysql(mysqlPort);
      if (conn == nullptr) {
        exitCode = 1;
      } else {
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
          for (size_t i = 0; i < g_test_count; i++) {
            if (onlyTest != -1 && g_tests[i].number != onlyTest) continue;
            if (g_tests[i].fn(&ndb, conn) != 0) exitCode = 1;
          }
        }

        dropTestTables(conn);
        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  if (exitCode == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return exitCode;
}
