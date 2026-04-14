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
 * testCteNdbApi — Integration test for CTE pushdown through the
 *                 NdbQueryBuilder API.
 *
 * Tests the NDB API CTE extensions:
 *   NdbQueryBuilder::beginCteSubtree() / endCteSubtree()
 *   NdbQueryBuilder::defineCte()
 *   NdbQueryBuilder::lookupCte()
 *
 * Schema (created via MySQL):
 *   cte_src(pk INT PK, grp INT, val BIGINT)       — source table
 *   cte_virtual(grp INT PK)                        — dummy for CTE_LOOKUP key
 *
 * Test data: (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)
 *   CTE GROUP BY grp, SUM(val): grp=1→30, grp=2→70, grp=3→50
 *   Self-join (pk=pk): COUNT(*)=5, SUM(val)=150
 *
 * Usage: testCteNdbApi -c <connect_string> -m <mysql_port> [-v]
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
#include <map>
#include <vector>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

static const char *TEST_DB = "test";
static const char *SRC_TABLE = "cte_src";
static const char *VIRTUAL_TABLE = "cte_virtual";

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
  sqlExec(conn, "DROP TABLE IF EXISTS cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual");

  if (sqlExec(conn,
      "CREATE TABLE cte_src ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  grp INT NOT NULL,"
      "  val BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  /* Virtual table for CTE_LOOKUP key binding.
   * PK (grp) matches the CTE GROUP BY key column type.
   * total column matches the CTE SUM aggregate result type. */
  if (sqlExec(conn,
      "CREATE TABLE cte_virtual ("
      "  grp INT NOT NULL PRIMARY KEY,"
      "  total BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  return 0;
}

static int
insertTestData(MYSQL *conn)
{
  return sqlExec(conn,
      "INSERT INTO cte_src VALUES "
      "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)");
}

static void
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual");
}

/* ------------------------------------------------------------------ */
/* Test 1: CTE + standard main query (CTEs unused by main)             */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) FROM cte_src t1               */
/*     JOIN cte_src t2 ON t1.pk = t2.pk GROUP BY grp)                  */
/*   SELECT COUNT(*), SUM(t2.val)                                      */
/*   FROM cte_src t1 JOIN cte_src t2 ON t1.pk = t2.pk                  */
/*                                                                     */
/* CTE is materialized but main query doesn't use it.                  */
/* Tests that CTE pipeline doesn't break normal aggregation.           */
/* Expected: COUNT=5, SUM=150                                          */
/* ------------------------------------------------------------------ */

static int
testCteWithStandardMain(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 1: CTE + standard main query ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  if (srcTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) */
  /* CTE 0 aggregation: GROUP BY grp (column "grp"), SUM(val).
   * Use direct column reference (NOT AGG_LINKED_COL_FLAG) since
   * the CTE scans a real table, not linked parent columns. */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregation: COUNT(*), SUM(val) — no GROUP BY */
  NdbAggregator mainAgg(srcTab);
  if (!mainAgg.LoadColumn("val", 0) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scan + lookup self-join */
  const NdbQueryOperationDef *subtreeOp = qb->beginCteSubtree(0);
  if (subtreeOp == nullptr) {
    printf("FAILED (beginCteSubtree: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  if (cteScanOp == nullptr) {
    printf("FAILED (CTE scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cteJoinKey[] = {
    qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  /* CTE leaf aggregation: uses direct GroupBy("grp"), no linked
   * projection needed (unlike main query where GROUP BY references
   * parent columns via AGG_LINKED_COL_FLAG). */
  cteLeafOpts.setAggregation(cteAgg);

  const NdbQueryLookupOperationDef *cteLeafOp =
      qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts);
  if (cteLeafOp == nullptr) {
    printf("FAILED (CTE readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  qb->endCteSubtree();

  /* Register CTE definition */
  int cteErr = qb->defineCte(0, srcTab, cteAgg);
  if (cteErr != 0) {
    printf("FAILED (defineCte: %d)\n", cteErr);
    qb->destroy();
    return -1;
  }

  /* Main query: scan + lookup self-join with aggregation */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *mainJoinKey[] = {
    qb->linkedValue(mainScanOp, "pk"), nullptr
  };
  NdbQueryOptions mainLeafOpts;
  mainLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  mainLeafOpts.setAggregation(mainAgg);

  const NdbQueryLookupOperationDef *mainLeafOp =
      qb->readTuple(srcTab, mainJoinKey, &mainLeafOpts);
  if (mainLeafOp == nullptr) {
    printf("FAILED (main readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Prepare */
  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  V("\n  Query prepared: %u operations\n", queryDef->getNoOfOperations());

  /* Execute */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
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
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan batches */
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Get aggregation results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 count = countRes.data_int64();
  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 sum = sumRes.data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  V("  Result: COUNT=%lld, SUM=%lld\n", (long long)count, (long long)sum);

  if (count == 5 && sum == 150) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }

  printf("FAILED (expected COUNT=5 SUM=150, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 2: Single CTE + main CTE_LOOKUP                                */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*     FROM cte_src t1 JOIN cte_src t2 ON t1.pk = t2.pk GROUP BY grp)  */
/*   SELECT cte0.grp, cte0.total                                       */
/*   FROM cte_src t1 JOIN cte0 ON t1.grp = cte0.grp                    */
/*                                                                     */
/* CTE materializes GROUP BY grp, SUM(val). Main query scans src and   */
/* looks up CTE by grp key. No main aggregation — raw CTE rows.        */
/* Expected: 5 rows: grp=1→30 (x2), grp=2→70 (x2), grp=3→50 (x1)     */
/* ------------------------------------------------------------------ */

static int
testCteLookupMain(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 2: CTE + main CTE_LOOKUP ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) — direct column refs */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree */
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  if (cteScanOp == nullptr) {
    printf("FAILED (CTE scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteJoinKey[] = {
    qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  const NdbQueryLookupOperationDef *cteLeafOp =
      qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts);
  if (cteLeafOp == nullptr) {
    printf("FAILED (CTE leaf: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->endCteSubtree();

  int cteErr = qb->defineCte(0, srcTab, cteAgg);
  if (cteErr != 0) {
    printf("FAILED (defineCte: %d)\n", cteErr);
    qb->destroy();
    return -1;
  }

  /* Main query: scan src, CTE_LOOKUP by grp */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cteKey[] = {
    qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  /* MatchNonNull = inner join: triggers CORR_FACTOR in DBSPJ's
   * parseDA so the NDB API can correlate CTE_LOOKUP results with
   * parent scan rows. */
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *cteLookupOp =
      qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);
  if (cteLookupOp == nullptr) {
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

  V("\n  Query prepared: %u operations\n", queryDef->getNoOfOperations());

  /* Execute */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
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

  /* Set up result projections. getValue defines what columns are read
   * and sizes the receive buffer accordingly. */
  NdbQueryOperation *mainQueryOp = query->getQueryOperation(3U);
  NdbQueryOperation *cteLookupQueryOp = query->getQueryOperation(4U);
  NdbRecAttr *raGrp = nullptr;
  if (mainQueryOp != nullptr) {
    raGrp = mainQueryOp->getValue("grp");
  }
  /* CTE_LOOKUP: getValue on the virtual table columns defines the
   * receive buffer size for the CTE result row. */
  if (cteLookupQueryOp != nullptr) {
    /* getValue defines the receive buffer size for CTE result columns.
     * The returned NdbRecAttr is not read in this test (row count only). */
    (void)cteLookupQueryOp->getValue("grp");
    (void)cteLookupQueryOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan batches */
  NdbQuery::NextResultOutcome outcome;
  Uint32 rowCount = 0;

  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    if (raGrp != nullptr) {
      V("  row: grp=%d\n", raGrp->int32_value());
    }
  }

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  V("  Rows: %u\n", rowCount);

  /* For now, verify row count only. Each main scan row (5 total)
   * should produce one CTE_LOOKUP result. */
  if (rowCount == 5) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }

  printf("FAILED (expected 5 rows, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 3: CTE_LOOKUP as aggregate leaf (CTE-fed aggregation)          */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (                                                    */
/*     SELECT grp, SUM(val) AS total                                   */
/*     FROM cte_src AS t1 JOIN cte_src AS t2 ON t1.pk = t2.pk          */
/*     GROUP BY grp)                                                   */
/*   SELECT COUNT(*), SUM(cte0.total)                                  */
/*   FROM cte_src AS s JOIN cte0 ON s.grp = cte0.grp                   */
/*                                                                     */
/* CTE 0 materializes GROUP BY grp, SUM(val): 3 groups.                */
/* Main query scans cte_src (5 rows), for each row does CTE_LOOKUP     */
/* into CTE 0 by grp key. The CTE_LOOKUP node is the main aggregate   */
/* leaf — its result feeds into the main JoinAggInterpreter via the    */
/* joinAggStateKey path (no FLUSH_AI to API).                          */
/*                                                                     */
/* Expected: COUNT=5, SUM=250 (30+30+70+70+50)                        */
/* ------------------------------------------------------------------ */

static int
testCteLookupAggLeaf(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 3: CTE_LOOKUP as aggregate leaf ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) — direct column refs */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregation: COUNT(*), SUM(cte0.total)
   * The "total" column comes from CTE_LOOKUP result via linked attributes.
   * Position 1 = second CTE result column (after grp at position 0).
   * Use the virtual table's "total" column for type info (BIGINT). */
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  if (totalCol == nullptr) {
    printf("FAILED (column lookup: total)\n");
    return -1;
  }
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, totalCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scan + lookup self-join */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *key[] = {
      qb->linkedValue(scan, "pk"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    if (qb->readTuple(srcTab, key, &opts) == nullptr) {
      printf("FAILED (CTE leaf: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(0, srcTab, cteAgg) != 0) {
    printf("FAILED (defineCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Main query: scan src, CTE_LOOKUP by grp as aggregate leaf */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cteKey[] = {
    qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setAggregation(mainAgg);
  const NdbQueryCteLookupOperationDef *cteLookupOp =
      qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);
  if (cteLookupOp == nullptr) {
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

  V("\n  Query prepared: %u operations\n", queryDef->getNoOfOperations());

  /* Execute */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
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
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan batches */
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Get aggregation results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 count = countRes.data_int64();
  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 sum = sumRes.data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  V("  Result: COUNT=%lld, SUM=%lld\n", (long long)count, (long long)sum);

  if (count == 5 && sum == 250) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }

  printf("FAILED (expected COUNT=5 SUM=250, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 4: CTE with scan filter (WHERE grp >= 2)                       */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*     FROM cte_src t1 JOIN cte_src t2 ON t1.pk = t2.pk                */
/*     WHERE t1.grp >= 2 GROUP BY grp)                                 */
/*   SELECT COUNT(*), SUM(t2.val)                                      */
/*   FROM cte_src t1 JOIN cte_src t2 ON t1.pk = t2.pk                  */
/*   WHERE t1.grp >= 2                                                 */
/*                                                                     */
/* The WHERE filter is pushed as NdbInterpretedCode on the CTE scan    */
/* and the main scan. Only rows with grp >= 2 are scanned.             */
/* Data: (3,2,30),(4,2,40),(5,3,50) pass the filter.                   */
/* Expected: COUNT=3, SUM=120                                          */
/* ------------------------------------------------------------------ */

static int
testCteWithScanFilter(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 4: CTE with scan filter (WHERE grp >= 2) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  if (srcTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = srcTab->getColumn("grp");
  if (grpCol == nullptr) {
    printf("FAILED (column lookup: grp)\n");
    return -1;
  }
  Uint32 grpColNo = grpCol->getColumnNo();

  /* Build scan filter: grp >= 2, using raw NdbInterpretedCode.
   * branch_col_le branches when col >= val (inverted semantics),
   * so branch_col_le(grp, 2, PASS) keeps rows where grp >= 2. */
  Uint32 codeBuf[128];
  NdbInterpretedCode filterCode(srcTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  {
    Uint32 valBuf = 2;
    const Uint32 PASS_LABEL = 0;
    if (filterCode.branch_col_le(&valBuf, sizeof(valBuf),
                                  grpColNo, PASS_LABEL) != 0 ||
        filterCode.interpret_exit_nok() != 0 ||
        filterCode.def_label(PASS_LABEL) != 0 ||
        filterCode.interpret_exit_ok() != 0) {
      printf("FAILED (filter build: err=%d)\n",
             filterCode.getNdbError().code);
      return -1;
    }
  }
  if (filterCode.finalise() != 0) {
    printf("FAILED (filter finalise: err=%d)\n",
           filterCode.getNdbError().code);
    return -1;
  }

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregation: COUNT(*), SUM(val) */
  NdbAggregator mainAgg(srcTab);
  if (!mainAgg.LoadColumn("val", 0) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scan (with filter) + lookup self-join */
  qb->beginCteSubtree(0);

  NdbQueryOptions cteScanOpts;
  cteScanOpts.setInterpretedCode(filterCode);
  const NdbQueryTableScanOperationDef *cteScanOp =
      qb->scanTable(srcTab, &cteScanOpts);
  if (cteScanOp == nullptr) {
    printf("FAILED (CTE scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cteJoinKey[] = {
    qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  if (qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts) == nullptr) {
    printf("FAILED (CTE readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  qb->endCteSubtree();

  if (qb->defineCte(0, srcTab, cteAgg) != 0) {
    printf("FAILED (defineCte)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scan (with filter) + lookup self-join + aggregation */
  NdbQueryOptions mainScanOpts;
  mainScanOpts.setInterpretedCode(filterCode);
  const NdbQueryTableScanOperationDef *mainScanOp =
      qb->scanTable(srcTab, &mainScanOpts);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scanTable: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *mainJoinKey[] = {
    qb->linkedValue(mainScanOp, "pk"), nullptr
  };
  NdbQueryOptions mainLeafOpts;
  mainLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  mainLeafOpts.setAggregation(mainAgg);
  if (qb->readTuple(srcTab, mainJoinKey, &mainLeafOpts) == nullptr) {
    printf("FAILED (main readTuple: %s)\n", qb->getNdbError().message);
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

  /* Execute */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
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
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
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
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 count = countRes.data_int64();
  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 sum = sumRes.data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  V("  Result: COUNT=%lld, SUM=%lld\n", (long long)count, (long long)sum);

  if (count == 3 && sum == 120) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }

  printf("FAILED (expected COUNT=3 SUM=120, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 5: CTE-to-CTE lookup (Part A)                                 */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total                          */
/*       FROM cte_src t1 JOIN cte_src t2 ON t1.pk = t2.pk              */
/*       GROUP BY grp),                                                */
/*     cte1 AS (SELECT cte0.grp, SUM(cte0.total) AS total              */
/*       FROM cte_src s JOIN cte0 ON s.grp = cte0.grp                  */
/*       GROUP BY cte0.grp)                                            */
/*   SELECT s.grp, cte1.total                                          */
/*   FROM cte_src s JOIN cte1 ON s.grp = cte1.grp                      */
/*                                                                     */
/* CTE 0 materializes via the existing Test 2 pattern.                 */
/* CTE 1 materializes by scanning cte_src and CTE_LOOKUP cte0 INSIDE   */
/* its subtree — this is the new Part A pattern under test.          */
/* Main query scans cte_src and CTE_LOOKUPs cte1.                     */
/*                                                                     */
/* Expected cte0 = {(1,30),(2,70),(3,50)}                              */
/* Expected cte1 per source row:                                       */
/*   row (1,1,10) → cte0(1).total=30 → grp=1, sum+=30                  */
/*   row (2,1,20) → cte0(1).total=30 → grp=1, sum+=30 → 60 for grp=1   */
/*   row (3,2,30) → cte0(2).total=70 → grp=2, sum+=70                  */
/*   row (4,2,40) → cte0(2).total=70 → grp=2, sum+=70 → 140 for grp=2  */
/*   row (5,3,50) → cte0(3).total=50 → grp=3, sum+=50 → 50 for grp=3   */
/* Expected cte1 = {(1,60),(2,140),(3,50)}                             */
/* Expected main = 5 rows, one per source row, each carries cte1.total */
/*                                                                     */
/* This test is expected to FAIL until all three Part A bugs are       */
/* fixed (see part_a_cte_to_cte_lookup_plan.md). Initial failure       */
/* symptom is DbspjErr::InvalidAggregateFlags (20022) raised by        */
/* validateAggregateFlags() because the nested lookupCte(0) with       */
/* setAggregation() is mis-classified as a main-query aggregate        */
/* leaf.                                                               */
/* ------------------------------------------------------------------ */

static int
testCteToCteLookup(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 5: CTE-to-CTE lookup (lookupCte inside subtree) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = virtTab->getColumn("grp");
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  if (grpCol == nullptr || totalCol == nullptr) {
    printf("FAILED (virt column lookup)\n");
    return -1;
  }

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) — Test 2 pattern */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1 aggregation: GROUP BY cte0.grp (linked pos 0),
   * SUM(cte0.total) (linked pos 1). The aggregator runs on the
   * CTE_LOOKUP result row (virtual table cte_virtual), so its
   * backing table for column type info is virtTab and both grouping
   * and value columns come via GroupByLinked / LoadLinkedColumn. */
  NdbAggregator cte1Agg(virtTab);
  if (!cte1Agg.GroupByLinked(0, grpCol) ||
      !cte1Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scan + readTuple self-join with agg */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 0 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *key[] = {
      qb->linkedValue(scan, "pk"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte0Agg);
    if (qb->readTuple(srcTab, key, &opts) == nullptr) {
      printf("FAILED (CTE 0 leaf: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(0, srcTab, cte0Agg, /*depMask=*/0) != 0) {
    printf("FAILED (defineCte 0)\n");
    qb->destroy();
    return -1;
  }

  /* CTE 1 subtree: scan cte_src + lookupCte(0) inside the subtree.
   * The lookupCte carries the aggregation — its result feeds CTE 1's
   * hash table. This is the new Part A pattern. */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 1 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *cte0Key[] = {
      qb->linkedValue(scan, "grp"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte1Agg);
    if (qb->lookupCte(0, 2, virtTab, cte0Key, &opts) == nullptr) {
      printf("FAILED (CTE 1 nested lookupCte: %s)\n",
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(1, virtTab, cte1Agg, /*depMask=*/1) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scan cte_src + lookupCte(1) */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cte1Key[] = {
    qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cte1LookupOpts;
  cte1LookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *mainCteLookupOp =
      qb->lookupCte(1, 2, virtTab, cte1Key, &cte1LookupOpts);
  if (mainCteLookupOp == nullptr) {
    printf("FAILED (main lookupCte: %s)\n", qb->getNdbError().message);
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

  /* Execute */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
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

  /* Wire up result projections on the main scan and the main CTE
   * lookup so we can verify per-row values. */
  NdbQueryOperation *mainScanQueryOp = query->getQueryOperation(6U);
  NdbQueryOperation *mainCteLookupQueryOp = query->getQueryOperation(7U);
  NdbRecAttr *raMainGrp = nullptr;
  NdbRecAttr *raCteGrp = nullptr;
  NdbRecAttr *raCteTotal = nullptr;
  if (mainScanQueryOp != nullptr) {
    raMainGrp = mainScanQueryOp->getValue("grp");
  }
  if (mainCteLookupQueryOp != nullptr) {
    raCteGrp = mainCteLookupQueryOp->getValue("grp");
    raCteTotal = mainCteLookupQueryOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Expected total per grp (cte1 values) */
  std::map<Int32, Int64> expected;
  expected[1] = 60;
  expected[2] = 140;
  expected[3] = 50;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 mainGrp = raMainGrp ? raMainGrp->int32_value() : -1;
    Int32 cteGrp = raCteGrp ? raCteGrp->int32_value() : -1;
    Int64 cteTotal = raCteTotal ? raCteTotal->int64_value() : -1;
    V("  row: mainGrp=%d cteGrp=%d cteTotal=%lld\n",
      mainGrp, cteGrp, (long long)cteTotal);
    auto it = expected.find(mainGrp);
    if (it == expected.end()) {
      printf("FAILED (unexpected grp=%d in result)\n", mainGrp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (cteTotal != it->second) {
      printf("FAILED (grp=%d expected total=%lld got %lld)\n",
             mainGrp, (long long)it->second, (long long)cteTotal);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
  }

  if (outcome == NdbQuery::NextResult_error) {
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (nextResult err %d: %s)\n", qErr.code, qErr.message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != 5) {
    printf("FAILED (expected 5 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: CTE-to-CTE lookup + main aggregation                        */
/*                                                                     */
/* Same CTE 0 / CTE 1 structure as Test 5, but the main query also     */
/* aggregates: SELECT COUNT(*), SUM(cte1.total) FROM cte_src s JOIN    */
/* cte1 ON s.grp = cte1.grp. This is the query structure that forces  */
/* validateAggregateFlags (Bug 3 from the original Part A plan) to    */
/* actually run — RT_AGGREGATE is set because the main query has its  */
/* own aggregate leaf.                                                 */
/*                                                                     */
/* Expected:                                                           */
/*   cte0 = {(1,30),(2,70),(3,50)}                                     */
/*   cte1 = {(1,60),(2,140),(3,50)}                                    */
/*   main scan produces 5 rows (1..5), each CTE_LOOKUPs cte1 by grp:  */
/*     (1,1,*) → cte1(1).total=60                                      */
/*     (2,1,*) → cte1(1).total=60                                      */
/*     (3,2,*) → cte1(2).total=140                                     */
/*     (4,2,*) → cte1(2).total=140                                     */
/*     (5,3,*) → cte1(3).total=50                                      */
/*   main aggregator: COUNT=5, SUM=60+60+140+140+50 = 450              */
/* ------------------------------------------------------------------ */

static int
testCteToCteLookupWithMainAgg(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 6: CTE-to-CTE lookup + main aggregation ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = virtTab->getColumn("grp");
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  if (grpCol == nullptr || totalCol == nullptr) {
    printf("FAILED (virt column lookup)\n");
    return -1;
  }

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1 aggregation: GROUP BY cte0.grp SUM(cte0.total) — Test 5 pattern */
  NdbAggregator cte1Agg(virtTab);
  if (!cte1Agg.GroupByLinked(0, grpCol) ||
      !cte1Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregation: COUNT(*), SUM(cte1.total) — Test 3 pattern.
   * This is what makes RT_AGGREGATE set in DBSPJ and causes
   * validateAggregateFlags to actually execute. */
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, totalCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scan + readTuple self-join with agg */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 0 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *key[] = {
      qb->linkedValue(scan, "pk"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte0Agg);
    if (qb->readTuple(srcTab, key, &opts) == nullptr) {
      printf("FAILED (CTE 0 leaf: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(0, srcTab, cte0Agg, /*depMask=*/0) != 0) {
    printf("FAILED (defineCte 0)\n");
    qb->destroy();
    return -1;
  }

  /* CTE 1 subtree: scan + nested lookupCte(0) with agg (Test 5 pattern) */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 1 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *cte0Key[] = {
      qb->linkedValue(scan, "grp"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte1Agg);
    if (qb->lookupCte(0, 2, virtTab, cte0Key, &opts) == nullptr) {
      printf("FAILED (CTE 1 nested lookupCte: %s)\n",
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(1, virtTab, cte1Agg, /*depMask=*/1) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scan cte_src + lookupCte(1) as aggregate leaf */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cte1Key[] = {
    qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions mainLookupOpts;
  mainLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  mainLookupOpts.setAggregation(mainAgg);
  if (qb->lookupCte(1, 2, virtTab, cte1Key, &mainLookupOpts) == nullptr) {
    printf("FAILED (main lookupCte: %s)\n", qb->getNdbError().message);
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

  /* Execute */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
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
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan batches */
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}

  if (outcome == NdbQuery::NextResult_error) {
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (nextResult err %d: %s)\n", qErr.code, qErr.message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Get aggregation results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result countRes = rec.FetchAggregationResult();
  Int64 count = countRes.data_int64();
  NdbAggregator::Result sumRes = rec.FetchAggregationResult();
  Int64 sum = sumRes.data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  V("  Result: COUNT=%lld, SUM=%lld\n", (long long)count, (long long)sum);

  if (count == 5 && sum == 450) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }

  printf("FAILED (expected COUNT=5 SUM=450, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
      connectString = argv[++i];
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
      mysqlPort = atoi(argv[++i]);
    else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0)
      verbose = true;
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s -c <connect_string> -m <mysql_port> [-v]\n", argv[0]);
      return 0;
    }
  }

  printf("=== testCteNdbApi ===\n");
  printf("Connect: %s, MySQL port: %d\n\n", connectString, mysqlPort);

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
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
          fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
          exitCode = 1;
        }
        else if (createTestTables(conn) != 0 || insertTestData(conn) != 0) {
          exitCode = 1;
        }
        else {
          if (testCteLookupMain(&ndb, conn) != 0) exitCode = 1;
          if (testCteWithStandardMain(&ndb, conn) != 0) exitCode = 1;
          if (testCteLookupAggLeaf(&ndb, conn) != 0) exitCode = 1;
          if (testCteWithScanFilter(&ndb, conn) != 0) exitCode = 1;
          if (testCteToCteLookup(&ndb, conn) != 0) exitCode = 1;
          if (testCteToCteLookupWithMainAgg(&ndb, conn) != 0)
            exitCode = 1;
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
