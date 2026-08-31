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
#ifdef NONE
#undef NONE
#endif
#include <Interpreter.hpp>
#include <signaldata/QueryTree.hpp>

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
static const char *SCALAR_VIRTUAL_TABLE = "cte_virtual_scalar";
/* Single-row CTE materialization (Tests 23-26,
 * cte_single_row_kernel_plan.md): a one-row source, an empty twin for
 * the miss case, and a 2-column virtual table matching the projected
 * (a, b) columns. */
static const char *SROW_SRC_TABLE = "cte_srow_src";
static const char *SROW_EMPTY_TABLE = "cte_srow_empty";
static const char *SROW_VIRTUAL_TABLE = "cte_virtual_srow";

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
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual_scalar");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_srow_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_srow_empty");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual_srow");

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

  /* Virtual table for scalar aggregate CTEs (no GROUP BY).
   * Single BIGINT column for the aggregate result. */
  if (sqlExec(conn,
      "CREATE TABLE cte_virtual_scalar ("
      "  result BIGINT NOT NULL PRIMARY KEY"
      ") ENGINE=NDB") != 0) return -1;

  /* Ordered index on val for scanIndex CTE tests (Test 18+) */
  if (sqlExec(conn,
      "CREATE INDEX idx_cte_src_val ON cte_src(val) USING BTREE") != 0)
    return -1;

  /* Ordered index on grp for scanCte parent + scanIndex child tests. */
  if (sqlExec(conn,
      "CREATE INDEX idx_cte_src_grp ON cte_src(grp) USING BTREE") != 0)
    return -1;

  /* Single-row CTE tables (Tests 23-26): a one-row source, an empty
   * twin, and the virtual table matching the projected (a, b). */
  if (sqlExec(conn,
      "CREATE TABLE cte_srow_src ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  a INT NOT NULL,"
      "  b BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;
  if (sqlExec(conn,
      "CREATE TABLE cte_srow_empty ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  a INT NOT NULL,"
      "  b BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;
  if (sqlExec(conn,
      "CREATE TABLE cte_virtual_srow ("
      "  a INT NOT NULL PRIMARY KEY,"
      "  b BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  return 0;
}

static int
insertTestData(MYSQL *conn)
{
  if (sqlExec(conn,
      "INSERT INTO cte_src VALUES "
      "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)") != 0) return -1;
  /* cte_srow_src holds exactly ONE row; cte_srow_empty stays empty. */
  return sqlExec(conn, "INSERT INTO cte_srow_src VALUES (7,42,4200)");
}

static void
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual_scalar");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_srow_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_srow_empty");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual_srow");
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
/* Why this test:                                                      */
/*   Smoke test that the CTE pipeline (build, materialize, release)   */
/*   doesn't break a query whose MAIN SELECT is a normal joined       */
/*   aggregate that doesn't reference the CTE.  CTE 0 is built and    */
/*   materialized but no main-query op consults it.                    */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container (g_CteSubtreeOpInfo)              */
/*   Node 1: scanTable(cte_src) inside CTE 0   T_CTE_SCAN, m_cteId=0  */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"),                                  */
/*           setAggregation(cteAgg))           T_AGGREGATE_LEAF,      */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=0             */
/*   Node 3: scanTable(cte_src)                — main root            */
/*           m_cteId=RNIL                                              */
/*   Node 4: readTuple(cte_src,                                       */
/*           key=linked(node3,"pk"),                                  */
/*           setAggregation(mainAgg))          T_AGGREGATE_LEAF,      */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=RNIL          */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   - Node 1 scans cte_src (5 rows).  T_CTE_SCAN causes              */
/*     scanFrag_send to set JoinAggFlag on the SCAN_FRAGREQ so DBLQH  */
/*     pipes the rows directly into the cte0 hash table at the        */
/*     materialization-aggregator level.                               */
/*   - Per parent row, node 2 (readTuple) fires keyed by linked pk   */
/*     and is the agg leaf — feeds cteAgg.                             */
/*   - cteAgg = GROUP BY grp, SUM(val) → cte0 = {(1,30),(2,70),(3,50)}*/
/*                                                                     */
/* Main query (after execCTE_START_MAIN_REQ):                          */
/*   - Node 3 scans cte_src (5 rows).                                 */
/*   - Per row, node 4 (readTuple) fires as the main agg leaf.       */
/*   - mainAgg = COUNT(*), SUM(val) over the 5 rows.                  */
/*                                                                     */
/* Expected aggregation result:                                        */
/*   COUNT = 5                                                         */
/*   SUM   = 150 (10+20+30+40+50)                                      */
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1: T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*                    */
/*   Node 2: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
/*   Node 3: T_INNER_JOIN | T_BUFFER_*                                 */
/*   Node 4: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
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
/* Why this test:                                                      */
/*   The first end-to-end test that has the main query CONSUME a CTE  */
/*   via a leaf CTE_LOOKUP_REQ.  Verifies the basic materialize-then- */
/*   lookup path with no main aggregation: each main scan row fires  */
/*   one CTE_LOOKUP whose result is delivered straight to the API.   */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src)                T_CTE_SCAN, m_cteId=0  */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"),                                  */
/*           setAggregation(cteAgg))           T_AGGREGATE_LEAF,      */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=0             */
/*   Node 3: scanTable(cte_src)                — main root            */
/*           m_cteId=RNIL, T_USER_PROJECTION,                         */
/*                                              T_BUFFER_MAP          */
/*           (T_BUFFER_MAP / T_CHK_CONGESTION set by appendTreeNode  */
/*           because node 4 below is a lookup child of this scan.)    */
/*   Node 4: lookupCte(0,                                             */
/*           key=linked(node3,"grp"))          T_USER_PROJECTION,     */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=RNIL,         */
/*                                              parent=node 3,        */
/*                                              NOT T_AGGREGATE_LEAF  */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   Same as Test 1: cte0 = {(1,30),(2,70),(3,50)}.                    */
/*                                                                     */
/* Main query:                                                         */
/*   - Node 3 scans cte_src — 5 rows total across all fragments.      */
/*   - Per main row, node 4 fires CTE_LOOKUP_REQ keyed by main.grp.  */
/*     DBLQH looks up cte0 hash table and FLUSH_AI sends the cte0    */
/*     row directly to the API receiver of node 4.                    */
/*       (pk=1,grp=1) → cte0(1)=(grp=1,total=30)                       */
/*       (pk=2,grp=1) → cte0(1)=(grp=1,total=30)  -- duplicate         */
/*       (pk=3,grp=2) → cte0(2)=(grp=2,total=70)                       */
/*       (pk=4,grp=2) → cte0(2)=(grp=2,total=70)  -- duplicate         */
/*       (pk=5,grp=3) → cte0(3)=(grp=3,total=50)                       */
/*                                                                     */
/* Expected result row count: 5                                        */
/*   {(grp=1,total=30) ×2, (grp=2,total=70) ×2, (grp=3,total=50) ×1} */
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1: T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*                    */
/*   Node 2: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
/*   Node 3: T_USER_PROJECTION | T_INNER_JOIN | T_BUFFER_MAP |        */
/*           T_CHK_CONGESTION                                          */
/*   Node 4: T_USER_PROJECTION | T_INNER_JOIN | T_LEAF                 */
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
/* Why this test:                                                      */
/*   Exercises CTE_LOOKUP_REQ in the "leaf with agg" role for the     */
/*   main SELECT.  The lookupCte is the main query's aggregate leaf, */
/*   so its result flows into the main JoinAggInterpreter directly   */
/*   via DBLQH's agg-feed path (joinAggStateKey != RNIL) and never   */
/*   reaches the API as a per-row TRANSID_AI.  The main aggregator's */
/*   final result is fetched via query->getAggregator().              */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src)                T_CTE_SCAN, m_cteId=0  */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"),                                  */
/*           setAggregation(cteAgg))           T_AGGREGATE_LEAF,      */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=0             */
/*   Node 3: scanTable(cte_src)                — main root            */
/*           m_cteId=RNIL                                              */
/*   Node 4: lookupCte(0,                                             */
/*           key=linked(node3,"grp"),                                 */
/*           setAggregation(mainAgg))          T_AGGREGATE_LEAF,      */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=RNIL          */
/*           cte_lookup_send computes                                  */
/*           joinAggStateKey from m_aggStateKeys[localNode].           */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   cte0 = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)}.     */
/*                                                                     */
/* Main query:                                                         */
/*   - Node 3 scans cte_src (5 rows).                                 */
/*   - Per main row, node 4 fires CTE_LOOKUP_REQ keyed by main.grp   */
/*     with joinAggStateKey set.  DBLQH walks the CTE 0 hash table   */
/*     to find the matching group, then takes the agg-feed branch:  */
/*     processRecWithLinkedAttrs() inserts the cte0 row into the     */
/*     main aggregator's local hash table on this node.  No          */
/*     TRANSID_AI is sent.                                             */
/*   - 5 inserts into mainAgg, one per main row:                      */
/*       (pk=1,grp=1) → cte0(1).total=30                               */
/*       (pk=2,grp=1) → cte0(1).total=30                               */
/*       (pk=3,grp=2) → cte0(2).total=70                               */
/*       (pk=4,grp=2) → cte0(2).total=70                               */
/*       (pk=5,grp=3) → cte0(3).total=50                               */
/*   - mainAgg has no GROUP BY → single result row.                   */
/*                                                                     */
/* Expected aggregation result:                                        */
/*   COUNT = 5                                                         */
/*   SUM   = 250 (30+30+70+70+50)                                      */
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1: T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*                    */
/*   Node 2: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
/*   Node 3: T_INNER_JOIN | T_BUFFER_*  (NO T_USER_PROJECTION         */
/*           because the main is an aggregate query — RT_AGGREGATE  */
/*           is set and parseDA's suppressFlushAI fires for non-leaf */
/*           main nodes)                                               */
/*   Node 4: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
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
/* Why this test:                                                      */
/*   Verifies that NdbInterpretedCode-style WHERE filters on the      */
/*   parent scan compose correctly with CTE materialization and the  */
/*   main aggregation.  Filter is applied on BOTH the CTE 0 scan    */
/*   and the main scan — only rows with grp >= 2 reach the agg      */
/*   leaves, so cte0 has 2 groups and main aggregator sees 3 rows.  */
/*                                                                     */
/* Tree shape: same as Test 3 except the two scanTable nodes carry   */
/* an interpreted filter program:                                      */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src, filter=grp>=2)                        */
/*           T_CTE_SCAN | T_ATTR_INTERPRETED, m_cteId=0               */
/*   Node 2: readTuple(cte_src, key=linked(node1,"pk"),               */
/*           setAggregation(cte0Agg))                                  */
/*           T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF, m_cteId=0      */
/*   Node 3: scanTable(cte_src, filter=grp>=2)                        */
/*           T_ATTR_INTERPRETED, m_cteId=RNIL                          */
/*   Node 4: readTuple(cte_src, key=linked(node3,"pk"),               */
/*           setAggregation(mainAgg))                                  */
/*           T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF, m_cteId=RNIL   */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   - Node 1's interpreted filter rejects rows with grp < 2          */
/*     (eliminates pk=1 grp=1 and pk=2 grp=1).                         */
/*   - Surviving cte_src rows: (pk=3,grp=2,val=30),                   */
/*     (pk=4,grp=2,val=40), (pk=5,grp=3,val=50).                       */
/*   - Per surviving row, node 2 fires with linked pk → matched      */
/*     rows feed cte0Agg: (grp=2,val=30),(grp=2,val=40),(grp=3,val=50)*/
/*   - cte0 = {(grp=2,total=70),(grp=3,total=50)} → 2 groups.          */
/*                                                                     */
/* Main query:                                                         */
/*   - Node 3's interpreted filter rejects pk=1, pk=2.                */
/*   - 3 surviving rows feed mainAgg: COUNT=3,                        */
/*     SUM(val) = 30+40+50 = 120.                                      */
/*                                                                     */
/* Expected aggregation result:                                        */
/*   COUNT = 3                                                         */
/*   SUM   = 120                                                       */
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1: T_CTE_SCAN | T_ATTR_INTERPRETED | T_INNER_JOIN |         */
/*           T_BUFFER_*                                                */
/*   Node 2: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
/*   Node 3: T_ATTR_INTERPRETED | T_INNER_JOIN | T_BUFFER_*            */
/*   Node 4: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
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
/* Why this test:                                                      */
/*   First test of CTE_LOOKUP_REQ in the "leaf with agg" role inside */
/*   a CTE materialization subtree (Part A pattern).  CTE 1's        */
/*   subtree has a scanTable parent and a NESTED lookupCte child    */
/*   that is the agg leaf for cte1Agg — its result is fed via       */
/*   joinAggStateKey directly into cte1's local aggregator at DBLQH. */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src)                T_CTE_SCAN, m_cteId=0  */
/*   Node 2: readTuple(cte_src, key=linked(node1,"pk"),               */
/*           setAggregation(cte0Agg))           T_AGGREGATE_LEAF,     */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=0             */
/*   Node 3: CTE 1 subtree container                                   */
/*   Node 4: scanTable(cte_src) inside CTE 1                          */
/*           T_CTE_SCAN, m_cteId=1, T_BUFFER_*                         */
/*           NOT the agg leaf — Part A walk-up sets                    */
/*           T_CTE_INDIRECT_FEED on this node (scan rows do NOT      */
/*           feed cte1 directly; the nested lookupCte does).          */
/*   Node 5: lookupCte(0, key=linked(node4,"grp"),                    */
/*           setAggregation(cte1Agg))           T_AGGREGATE_LEAF,     */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=1,            */
/*                                              parent=node 4         */
/*           cte_lookup_send computes joinAggStateKey from           */
/*           m_cteAggStateKeys[encCteIdx*MAX_NDB_NODES+localNode]    */
/*           because m_cteId=1 (inside CTE subtree).                  */
/*   Node 6: scanTable(cte_src)                — main root            */
/*           m_cteId=RNIL                                              */
/*   Node 7: lookupCte(1, key=linked(node6,"grp"))                    */
/*           T_USER_PROJECTION | T_INNER_JOIN | T_LEAF, m_cteId=RNIL */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   cte0 = {(1,30),(2,70),(3,50)}.                                    */
/*                                                                     */
/* Phase 1 (CTE 1 materialization on each DBSPJ instance):             */
/*   - Node 4 scans cte_src locally (5 rows).  T_CTE_INDIRECT_FEED  */
/*     causes scanFrag_send to NOT set JoinAggFlag on the SCAN_FRAGREQ*/
/*     so the scanned rows come back to DBSPJ as plain TRANSID_AI    */
/*     (not piped through any aggregator).                            */
/*   - Per scanned row, node 5 fires CTE_LOOKUP_REQ keyed by         */
/*     s.grp with joinAggStateKey set — DBLQH's agg-feed branch     */
/*     inserts each cte0 row into cte1Agg directly.                   */
/*   - 5 inserts into cte1Agg:                                        */
/*       s=(pk=1,grp=1) → cte0(1).total=30 → cte1Agg insert (grp=1,30)*/
/*       s=(pk=2,grp=1) → cte0(1).total=30 → insert (grp=1,30)        */
/*       s=(pk=3,grp=2) → cte0(2).total=70 → insert (grp=2,70)        */
/*       s=(pk=4,grp=2) → cte0(2).total=70 → insert (grp=2,70)        */
/*       s=(pk=5,grp=3) → cte0(3).total=50 → insert (grp=3,50)        */
/*   - cte1Agg = GROUP BY cte0.grp, SUM(cte0.total):                  */
/*       grp=1: 30+30 = 60                                             */
/*       grp=2: 70+70 = 140                                            */
/*       grp=3: 50                                                     */
/*   - cte1 = {(1,60),(2,140),(3,50)} → 3 groups.                      */
/*                                                                     */
/* Main query:                                                         */
/*   - Node 6 scans cte_src (5 rows).                                 */
/*   - Per main row, node 7 fires CTE_LOOKUP_REQ to cte1 keyed by   */
/*     main.grp.  Result delivered to API via FLUSH_AI.               */
/*                                                                     */
/* Expected result row count: 5                                        */
/*   {(grp=1,total=60) ×2, (grp=2,total=140) ×2, (grp=3,total=50) ×1}*/
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1: T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*                    */
/*   Node 2: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
/*   Node 4: T_CTE_SCAN | T_CTE_INDIRECT_FEED | T_INNER_JOIN |        */
/*           T_BUFFER_*                                                */
/*   Node 5: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF                  */
/*   Node 6: T_USER_PROJECTION | T_INNER_JOIN | T_BUFFER_MAP |        */
/*           T_CHK_CONGESTION                                          */
/*   Node 7: T_USER_PROJECTION | T_INNER_JOIN | T_LEAF                 */
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
/* aggregates.  Exercises RT_AGGREGATE on the main request AND the     */
/* nested CTE_LOOKUP agg-leaf inside a CTE subtree.                    */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY grp),*/
/*     cte1 AS (SELECT cte0.grp, SUM(cte0.total) AS total              */
/*              FROM cte_src s JOIN cte0 ON s.grp = cte0.grp           */
/*              GROUP BY cte0.grp)                                     */
/*   SELECT COUNT(*), SUM(cte1.total)                                  */
/*     FROM cte_src s JOIN cte1 ON s.grp = cte1.grp;                   */
/*                                                                     */
/* Tree shape (6 tree nodes, built in this order):                     */
/*   CTE 0 subtree (2 nodes):                                          */
/*     node 0: scanTable(cte_src)                       [parent=RNIL]  */
/*       ↳ node 1: readTuple(cte_src, pk=linkedValue(scan0,"pk")),     */
/*                 setAggregation(cte0Agg)              [parent=node 0]*/
/*   CTE 1 subtree (2 nodes):                                          */
/*     node 2: scanTable(cte_src)                       [parent=RNIL]  */
/*       ↳ node 3: lookupCte(0, grp=linkedValue(scan1,"grp")),         */
/*                 setAggregation(cte1Agg)              [parent=node 2]*/
/*   Main query (2 nodes):                                             */
/*     node 4: scanTable(cte_src)                       [parent=RNIL]  */
/*       ↳ node 5: lookupCte(1, grp=linkedValue(mainScan,"grp")),      */
/*                 setAggregation(mainAgg)              [parent=node 4]*/
/*                                                                     */
/* Roles / expected flag bits:                                         */
/*   node 0 (cte0 scanTable):  T_CTE_SCAN, m_cteId=0 — materialization */
/*          root for CTE 0 (drives scan during RT_CTE_PHASE).          */
/*   node 1 (cte0 readTuple): T_AGGREGATE_LEAF, m_cteId=0 —            */
/*          inserts groups into cte0's hash table via cte0Agg.         */
/*   node 2 (cte1 scanTable): T_CTE_SCAN + T_CTE_INDIRECT_FEED,        */
/*          m_cteId=1 — walk-up from node 3 marks this as the          */
/*          indirect-feed scan ancestor (no JoinAggFlag on its         */
/*          SCAN_FRAGREQ; rows come back to DBSPJ to drive node 3).    */
/*   node 3 (cte1 lookupCte(0)): T_AGGREGATE_LEAF, m_cteId=1 — fires   */
/*          a CTE_LOOKUP per parent row; result rows are routed into   */
/*          cte1Agg's local state via joinAggStateKey.                 */
/*   node 4 (main scanTable):   regular scan, m_cteId=RNIL — main      */
/*          root; its SCAN_FRAGREQ carries JoinAggFlag (RT_AGGREGATE). */
/*   node 5 (main lookupCte(1)): T_AGGREGATE_LEAF, m_cteId=RNIL —      */
/*          fires a CTE_LOOKUP per main row; result row goes to main   */
/*          aggregator via mainAgg's aggStateKey.                      */
/*                                                                     */
/* Step-by-step execution:                                             */
/*   1. Phase 0 (CTE 0 materialization):                               */
/*      - Each data node runs node 0 locally (scanFrag per fragment    */
/*        with fragment-per-node skip on cte_src).                     */
/*      - Per parent row, node 1 fires readTuple(pk=parent.pk) and     */
/*        cte0Agg inserts/updates a group keyed by "grp".              */
/*      - cte0 = {(1, sum 10+20 = 30), (2, sum 30+40=70), (3, 50)}.    */
/*   2. Phase 1 (CTE 1 materialization):                               */
/*      - Each data node runs node 2 locally. Per parent row, node 3  */
/*        fires CTE_LOOKUP_REQ(cte0, grp=parent.grp). DBLQH returns   */
/*        one matching cte0 row. cte1Agg inserts into cte1's hash     */
/*        table keyed on cte0.grp, summing cte0.total.                */
/*      - cte1 groups: grp=1 seen twice → total=60; grp=2 seen twice  */
/*        → total=140; grp=3 seen once → total=50.                     */
/*      - cte1 = {(1,60),(2,140),(3,50)}.                              */
/*   3. Main query:                                                    */
/*      - Node 4 scans cte_src (5 rows). Per parent row, node 5 fires */
/*        CTE_LOOKUP_REQ(cte1, grp=parent.grp); matching cte1 row is  */
/*        fed to mainAgg's local state via joinAggStateKey.           */
/*      - Expected per-row: (pk=1,grp=1)→cte1.total=60,                */
/*        (pk=2,grp=1)→60, (pk=3,grp=2)→140, (pk=4,grp=2)→140,         */
/*        (pk=5,grp=3)→50.                                             */
/*      - mainAgg: COUNT=5, SUM=60+60+140+140+50=450.                  */
/*   4. Main aggregator delivered to API.                              */
/*                                                                     */
/* Expected row counts:                                                */
/*   cte0=3 groups; cte1=3 groups; main=1 row.                         */
/*   COUNT=5, SUM=450.                                                 */
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
/* Test 7: Three-level CTE chain (CTE 2 → CTE 1 → CTE 0)               */
/*                                                                     */
/* Exercises multi-phase sequencing at depth 3 — verifies that the     */
/* CTE_INDIRECT_FEED / nested-agg-leaf machinery works for two nested  */
/* CTE lookups chained through three phases.                           */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY grp),*/
/*     cte1 AS (SELECT cte0.grp, SUM(cte0.total) AS total              */
/*              FROM cte_src s JOIN cte0 ON s.grp = cte0.grp           */
/*              GROUP BY cte0.grp),                                    */
/*     cte2 AS (SELECT cte1.grp, SUM(cte1.total) AS total              */
/*              FROM cte_src s JOIN cte1 ON s.grp = cte1.grp           */
/*              GROUP BY cte1.grp)                                     */
/*   SELECT s.grp, cte2.total                                          */
/*     FROM cte_src s JOIN cte2 ON s.grp = cte2.grp;                   */
/*                                                                     */
/* Tree shape (8 tree nodes):                                          */
/*   CTE 0 subtree:                                                    */
/*     node 0: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 1: readTuple(cte_src, pk=linkedValue(scan0,"pk")),     */
/*                 setAggregation(cte0Agg)               [parent=node 0]*/
/*   CTE 1 subtree:                                                    */
/*     node 2: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 3: lookupCte(0, grp=linkedValue(scan1,"grp")),         */
/*                 setAggregation(cte1Agg)               [parent=node 2]*/
/*   CTE 2 subtree:                                                    */
/*     node 4: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 5: lookupCte(1, grp=linkedValue(scan2,"grp")),         */
/*                 setAggregation(cte2Agg)               [parent=node 4]*/
/*   Main query:                                                       */
/*     node 6: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 7: lookupCte(2, grp=linkedValue(mainScan,"grp")),      */
/*                 T_USER_PROJECTION                    [parent=node 6]*/
/*                                                                     */
/* Roles / expected flag bits:                                         */
/*   node 0: T_CTE_SCAN, m_cteId=0 — cte0 materialization root.        */
/*   node 1: T_AGGREGATE_LEAF, m_cteId=0 — feeds cte0Agg.              */
/*   node 2: T_CTE_SCAN + T_CTE_INDIRECT_FEED, m_cteId=1 — scan        */
/*           ancestor of node 3; SCAN_FRAGREQ runs without JoinAggFlag.*/
/*   node 3: T_AGGREGATE_LEAF, m_cteId=1 — CTE_LOOKUP_REQ to cte0,     */
/*           result row routed into cte1Agg via joinAggStateKey.       */
/*   node 4: T_CTE_SCAN + T_CTE_INDIRECT_FEED, m_cteId=2 — same        */
/*           pattern as node 2 but for CTE 2 subtree.                  */
/*   node 5: T_AGGREGATE_LEAF, m_cteId=2 — CTE_LOOKUP_REQ to cte1,     */
/*           result row routed into cte2Agg via joinAggStateKey.       */
/*   node 6: regular main scan, m_cteId=RNIL.                          */
/*   node 7: T_USER_PROJECTION, m_cteId=RNIL — CTE_LOOKUP_REQ to cte2, */
/*           result row delivered to API via FLUSH_AI.                 */
/*                                                                     */
/* Step-by-step execution:                                             */
/*   1. Phase 0 (CTE 0 materialization):                               */
/*      - node 0 scans cte_src (5 rows); per row node 1 fires          */
/*        readTuple and cte0Agg groups by "grp".                        */
/*      - cte0 = {(1,30),(2,70),(3,50)}.                                */
/*   2. Phase 1 (CTE 1 materialization):                               */
/*      - node 2 scans cte_src locally; per parent row node 3 fires    */
/*        CTE_LOOKUP_REQ(cte0, grp=parent.grp). cte1Agg groups rows    */
/*        by cte0.grp, summing cte0.total.                              */
/*      - Per-grp sums (repeated per source row):                       */
/*          grp=1: seen twice (pk=1,pk=2) → 30+30=60                    */
/*          grp=2: seen twice (pk=3,pk=4) → 70+70=140                   */
/*          grp=3: seen once (pk=5)       → 50                          */
/*      - cte1 = {(1,60),(2,140),(3,50)}.                               */
/*   3. Phase 2 (CTE 2 materialization):                               */
/*      - node 4 scans cte_src; per parent row node 5 fires            */
/*        CTE_LOOKUP_REQ(cte1, grp=parent.grp). cte2Agg groups rows    */
/*        by cte1.grp, summing cte1.total.                              */
/*      - Per-grp sums:                                                 */
/*          grp=1: twice → 60+60=120                                    */
/*          grp=2: twice → 140+140=280                                  */
/*          grp=3: once → 50                                            */
/*      - cte2 = {(1,120),(2,280),(3,50)}.                              */
/*   4. Main query:                                                    */
/*      - node 6 scans cte_src (5 rows). Per row node 7 fires          */
/*        CTE_LOOKUP_REQ(cte2, grp=parent.grp). Matching cte2 row is   */
/*        delivered to API via FLUSH_AI.                                */
/*                                                                     */
/* Expected row counts:                                                */
/*   cte0=3 groups; cte1=3 groups; cte2=3 groups; main=5 rows.         */
/*   Per-row main result:                                              */
/*     (mainGrp=1, cteTotal=120), (mainGrp=1, cteTotal=120),            */
/*     (mainGrp=2, cteTotal=280), (mainGrp=2, cteTotal=280),            */
/*     (mainGrp=3, cteTotal=50).                                        */
/* ------------------------------------------------------------------ */

static int
testCteThreeLevelChain(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 7: Three-level CTE chain ... ");
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

  /* CTE 0 — GROUP BY grp, SUM(val) (Test 2 pattern) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1 — GROUP BY cte0.grp SUM(cte0.total) (Test 5 pattern) */
  NdbAggregator cte1Agg(virtTab);
  if (!cte1Agg.GroupByLinked(0, grpCol) ||
      !cte1Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 2 — GROUP BY cte1.grp SUM(cte1.total). Same shape as cte1Agg
   * because the CTE_LOOKUP result format is the same virtual table. */
  NdbAggregator cte2Agg(virtTab);
  if (!cte2Agg.GroupByLinked(0, grpCol) ||
      !cte2Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte2Agg.Sum(0, 0) ||
      !cte2Agg.Finalize()) {
    printf("FAILED (cte2Agg: %s)\n", cte2Agg.GetError().err_msg_);
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

  /* CTE 1 subtree: scan + nested lookupCte(0) as aggregate leaf */
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

  /* CTE 2 subtree: scan + nested lookupCte(1) as aggregate leaf */
  qb->beginCteSubtree(2);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 2 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *cte1Key[] = {
      qb->linkedValue(scan, "grp"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte2Agg);
    if (qb->lookupCte(1, 2, virtTab, cte1Key, &opts) == nullptr) {
      printf("FAILED (CTE 2 nested lookupCte: %s)\n",
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  /* CTE 2 depends on CTE 1 → depMask = 0b10 = 2 */
  if (qb->defineCte(2, virtTab, cte2Agg, /*depMask=*/2) != 0) {
    printf("FAILED (defineCte 2)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scan cte_src + lookupCte(2) */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *cte2Key[] = {
    qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions mainLookupOpts;
  mainLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *mainCteLookupOp =
      qb->lookupCte(2, 2, virtTab, cte2Key, &mainLookupOpts);
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

  /* Wire up result projections on the main scan and main CTE lookup
   * so we can verify per-row values. Main scan is op 9 (0..2 for
   * CTE 0, 3..5 for CTE 1, 6..8 for CTE 2, 9..10 for main) —
   * query->getNoOfOperations() is 11. */
  const Uint32 mainScanOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 mainCteLookupOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainScanQueryOp =
      query->getQueryOperation(mainScanOpNo);
  NdbQueryOperation *mainCteLookupQueryOp =
      query->getQueryOperation(mainCteLookupOpNo);
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

  /* Expected per-grp cte2 totals */
  std::map<Int32, Int64> expected;
  expected[1] = 120;
  expected[2] = 280;
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
/* Test 8: scanCte as main query root (plain row delivery)             */
/*                                                                     */
/* Simplest possible scanCte test — scan the CTE and deliver rows to   */
/* the API without a main aggregator or child operations.              */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*                   FROM cte_src GROUP BY grp)                        */
/*   SELECT grp, total FROM cte0;                                      */
/*                                                                     */
/* Tree shape (3 tree nodes):                                          */
/*   CTE 0 subtree:                                                    */
/*     node 0: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 1: readTuple(cte_src, pk=linkedValue(scan0,"pk")),     */
/*                 setAggregation(cte0Agg)               [parent=node 0]*/
/*   Main query:                                                       */
/*     node 2: scanCte(0)                                [parent=RNIL] */
/*                                                                     */
/* Roles / expected flag bits:                                         */
/*   node 0: T_CTE_SCAN, m_cteId=0 — cte0 materialization root.        */
/*   node 1: T_AGGREGATE_LEAF, m_cteId=0 — feeds cte0Agg.              */
/*   node 2: T_USER_PROJECTION, m_cteId=RNIL, no joinAggStateKey.      */
/*           Fragment-per-node skip applies: only fragments whose      */
/*           rootFragId < numDataNodes send CTE_SCAN_REQ; the others   */
/*           short-circuit with zero rows.                              */
/*                                                                     */
/* Step-by-step execution:                                             */
/*   1. Phase 0 (CTE 0 materialization):                               */
/*      - node 0 scans cte_src (5 rows); per row node 1 fires          */
/*        readTuple and cte0Agg groups by grp, summing val.             */
/*      - cte0 = {(1,30),(2,70),(3,50)}.                                */
/*   2. Main query:                                                    */
/*      - DBSPJ runs one SCAN_FRAGREQ-equivalent per fragment.         */
/*      - For rootFragId < numDataNodes, cte_scan_start sends          */
/*        CTE_SCAN_REQ to DBLQH which walks that node's local cte0     */
/*        partition and returns each group via TRANSID_AI to the API   */
/*        (direct via FLUSH_AI, resultRef/resultData set on the req).  */
/*      - For rootFragId >= numDataNodes, the fragment short-circuits  */
/*        with zero rows (no CTE_SCAN_REQ sent).                        */
/*                                                                     */
/* Expected row counts:                                                */
/*   cte0=3 groups; main=3 rows delivered to API.                      */
/*   Per-row result set: {(1,30),(2,70),(3,50)}.                       */
/* ------------------------------------------------------------------ */

static int
testScanCteMainRoot(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 8: scanCte as main query root ... ");
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

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
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

  /* Main query: scanCte(0) as root, no main aggregation */
  const NdbQueryCteScanOperationDef *mainScanCteOp =
      qb->scanCte(0, 2, virtTab);
  if (mainScanCteOp == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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

  /* Wire user projection on the scanCte root */
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
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

  /* Expected cte0 values */
  std::map<Int32, Int64> expected;
  expected[1] = 30;
  expected[2] = 70;
  expected[3] = 50;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 grp = raGrp ? raGrp->int32_value() : -1;
    Int64 total = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", grp, (long long)total);
    auto it = expected.find(grp);
    if (it == expected.end()) {
      printf("FAILED (unexpected grp=%d)\n", grp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (total != it->second) {
      printf("FAILED (grp=%d expected total=%lld got %lld)\n",
             grp, (long long)it->second, (long long)total);
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

  if (rowCount != 3) {
    printf("FAILED (expected 3 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 9: scanCte + lookupCte self-join (normal pushdown join)       */
/*                                                                     */
/* Exercises CTE_SCAN as the main root in a pushdown join where a     */
/* child CTE_LOOKUP is driven by each outer CTE row via linkedValue.  */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*                   FROM cte_src GROUP BY grp)                        */
/*   SELECT outer.grp, outer.total, inner.grp, inner.total             */
/*     FROM cte0 AS outer                                              */
/*     JOIN cte0 AS inner ON outer.grp = inner.grp;                    */
/*                                                                     */
/* Tree shape (4 tree nodes):                                          */
/*   CTE 0 subtree:                                                    */
/*     node 0: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 1: readTuple(cte_src, pk=linkedValue(scan0,"pk")),     */
/*                 setAggregation(cte0Agg)               [parent=node 0]*/
/*   Main query:                                                       */
/*     node 2: scanCte(0)                                [parent=RNIL] */
/*       ↳ node 3: lookupCte(0, grp=linkedValue(outerScan,"grp"))      */
/*                                                       [parent=node 2]*/
/*                                                                     */
/* Roles / expected flag bits:                                         */
/*   node 0: T_CTE_SCAN, m_cteId=0 — cte0 materialization root.        */
/*   node 1: T_AGGREGATE_LEAF, m_cteId=0 — feeds cte0Agg.              */
/*   node 2: T_USER_PROJECTION, m_cteId=RNIL — main scanCte outer.     */
/*           Outer rows flow back to DBSPJ (TRANSID_AI) and drive      */
/*           the child via cte_parent_row; simultaneously delivered   */
/*           to API via FLUSH_AI.                                      */
/*   node 3: T_USER_PROJECTION, T_LEAF, m_cteId=RNIL — inner           */
/*           lookupCte; one CTE_LOOKUP_REQ fired per parent row with   */
/*           grp=parent.grp as key. Result row delivered to API.       */
/*                                                                     */
/* Step-by-step execution:                                             */
/*   1. Phase 0 (CTE 0 materialization):                               */
/*      - node 0 scans cte_src (5 rows); per row node 1 fires          */
/*        readTuple and cte0Agg groups by grp.                          */
/*      - cte0 = {(1,30),(2,70),(3,50)}.                                */
/*   2. Main query:                                                    */
/*      - node 2 sends CTE_SCAN_REQ to DBLQH; walker returns each      */
/*        cte0 group back to DBSPJ (fragment-per-node skip applies).    */
/*      - For each parent row, cte_parent_row fires node 3 which      */
/*        sends CTE_LOOKUP_REQ(cte0, grp=parent.grp) to DBLQH.         */
/*      - Both parent and child rows are delivered to API via          */
/*        FLUSH_AI. Self-join by grp: each outer row matches exactly   */
/*        one inner row (same row).                                    */
/*                                                                     */
/* Expected row counts:                                                */
/*   cte0=3 groups; main=3 rows.                                       */
/*   Per-row result: outer.grp == inner.grp AND outer.total ==         */
/*   inner.total for all 3 rows.                                       */
/* ------------------------------------------------------------------ */

static int
testScanCteWithJoin(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 9: scanCte + lookupCte self-join ... ");
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

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
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

  /* Main query: scanCte(0) as outer root */
  const NdbQueryCteScanOperationDef *outerScan =
      qb->scanCte(0, 2, virtTab);
  if (outerScan == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Inner: lookupCte(0) keyed by outer.grp. For each outer CTE scan
   * row, the pushdown-join machinery should fire a CTE_LOOKUP_REQ
   * with grp from the outer row. Self-join by grp returns the same
   * row. */
  const NdbQueryOperand *innerKey[] = {
    qb->linkedValue(outerScan, "grp"), nullptr
  };
  NdbQueryOptions innerOpts;
  innerOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *innerLookup =
      qb->lookupCte(0, 2, virtTab, innerKey, &innerOpts);
  if (innerLookup == nullptr) {
    printf("FAILED (inner lookupCte: %s)\n", qb->getNdbError().message);
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

  /* Wire result projections on outer scan and inner lookup */
  const Uint32 outerOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 innerOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *outerQueryOp = query->getQueryOperation(outerOpNo);
  NdbQueryOperation *innerQueryOp = query->getQueryOperation(innerOpNo);
  NdbRecAttr *raOuterGrp = nullptr;
  NdbRecAttr *raOuterTotal = nullptr;
  NdbRecAttr *raInnerGrp = nullptr;
  NdbRecAttr *raInnerTotal = nullptr;
  if (outerQueryOp != nullptr) {
    raOuterGrp = outerQueryOp->getValue("grp");
    raOuterTotal = outerQueryOp->getValue("total");
  }
  if (innerQueryOp != nullptr) {
    raInnerGrp = innerQueryOp->getValue("grp");
    raInnerTotal = innerQueryOp->getValue("total");
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

  /* Expected cte0 values */
  std::map<Int32, Int64> expected;
  expected[1] = 30;
  expected[2] = 70;
  expected[3] = 50;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 oGrp = raOuterGrp ? raOuterGrp->int32_value() : -1;
    Int64 oTotal = raOuterTotal ? raOuterTotal->int64_value() : -1;
    Int32 iGrp = raInnerGrp ? raInnerGrp->int32_value() : -1;
    Int64 iTotal = raInnerTotal ? raInnerTotal->int64_value() : -1;
    V("  row: outer(grp=%d,total=%lld) inner(grp=%d,total=%lld)\n",
      oGrp, (long long)oTotal, iGrp, (long long)iTotal);
    auto it = expected.find(oGrp);
    if (it == expected.end()) {
      printf("FAILED (unexpected outer.grp=%d)\n", oGrp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (oTotal != it->second || iGrp != oGrp || iTotal != oTotal) {
      printf("FAILED (grp=%d expected total=%lld, got outer(%lld) "
             "inner(grp=%d,total=%lld))\n",
             oGrp, (long long)it->second, (long long)oTotal,
             iGrp, (long long)iTotal);
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

  if (rowCount != 3) {
    printf("FAILED (expected 3 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 10: CTE 1 reads from CTE 0 via scanCte (agg-feed path)         */
/*                                                                     */
/* Exercises the CTE_SCAN agg-feed path — a scanCte node inside a CTE  */
/* subtree that is BOTH the subtree root AND the aggregate leaf for   */
/* the enclosing CTE, so scanned rows are inserted directly into the  */
/* enclosing CTE's JoinAggInterpreter without going back to DBSPJ.    */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY grp),*/
/*     cte1 AS (SELECT grp, SUM(total) AS total FROM cte0 GROUP BY grp)*/
/*   SELECT grp, total FROM cte1;                                      */
/*                                                                     */
/* Tree shape (4 tree nodes):                                          */
/*   CTE 0 subtree:                                                    */
/*     node 0: scanTable(cte_src)                        [parent=RNIL] */
/*       ↳ node 1: readTuple(cte_src, pk=linkedValue(scan0,"pk")),     */
/*                 setAggregation(cte0Agg)               [parent=node 0]*/
/*   CTE 1 subtree (CTE-only; one node):                               */
/*     node 2: scanCte(0), setAggregation(cte1Agg)       [parent=RNIL] */
/*   Main query:                                                       */
/*     node 3: scanCte(1)                                [parent=RNIL] */
/*                                                                     */
/* Roles / expected flag bits:                                         */
/*   node 0: T_CTE_SCAN, m_cteId=0 — cte0 materialization root.        */
/*   node 1: T_AGGREGATE_LEAF, m_cteId=0 — feeds cte0Agg.              */
/*   node 2: T_CTE_SCAN + T_AGGREGATE_LEAF, m_cteId=1 —                */
/*           CTE-2-reads-CTE-1 special case: nested CTE_SCAN is also   */
/*           the subtree root (no scan ancestor), so the walk-up in   */
/*           DbspjMain.cpp marks node 2 itself as T_CTE_SCAN and       */
/*           records m_scanTreeNodeNo so RT_CTE_PHASE finds it.        */
/*           joinAggStateKey points at cte1's local aggStateKey; the  */
/*           DBLQH walker's agg-feed branch inserts each cte0 group   */
/*           into cte1's JoinAggInterpreter via                        */
/*           processRecWithLinkedAttrs().                              */
/*   node 3: T_USER_PROJECTION, m_cteId=RNIL — main scanCte(1),        */
/*           delivers cte1 rows to API via FLUSH_AI.                   */
/*                                                                     */
/* Step-by-step execution:                                             */
/*   1. Phase 0 (CTE 0 materialization):                               */
/*      - node 0 scans cte_src; per row node 1 fires and cte0Agg      */
/*        groups by grp, summing val.                                  */
/*      - cte0 = {(1,30),(2,70),(3,50)}.                                */
/*   2. Phase 1 (CTE 1 materialization):                               */
/*      - Each DBSPJ instance runs CTE 1's subtree. cte_scan_start    */
/*        computes joinAggStateKey (cte1 base + cte1 leafIndex) and   */
/*        sends CTE_SCAN_REQ to DBLQH with joinAggStateKey != RNIL.   */
/*      - DBLQH's walker visits each local cte0 group and takes the  */
/*        agg-feed branch: builds linked_attr_data from the group    */
/*        and calls cte1Agg's local                                    */
/*        targetInterp->processRecWithLinkedAttrs(), which inserts   */
/*        the row into cte1's hash table. No TRANSID_AI is sent back  */
/*        to DBSPJ for this node (execCTE_SCAN_CONF must detect      */
/*        joinAggStateKey != RNIL and NOT bump m_rows).                */
/*      - cte1 = {(1,30),(2,70),(3,50)} (identity since each grp has  */
/*        exactly one row in cte0).                                    */
/*   3. Main query:                                                    */
/*      - node 3 sends CTE_SCAN_REQ for cte1 (no joinAggStateKey);    */
/*        walker's non-agg-feed branch emits each cte1 group via      */
/*        FLUSH_AI directly to API.                                    */
/*                                                                     */
/* Expected row counts:                                                */
/*   cte0=3 groups; cte1=3 groups; main=3 rows delivered to API.      */
/*   Per-row result: {(1,30),(2,70),(3,50)}.                           */
/* ------------------------------------------------------------------ */

static int
testCteScanFeedsAgg(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 10: CTE-2-reads-CTE-1 via scanCte agg-feed ... ");
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

  /* CTE 0 aggregation: GROUP BY grp, SUM(val) — produces (grp, total) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1 aggregation: GROUP BY linked-pos 0 (cte0.grp),
   * SUM(linked-pos 1 = cte0.total).  The aggregator runs on the
   * scanCte(0) result row — input arrives via linked_attr_data with
   * key columns at positions 0..n_gb-1 and accumulator columns
   * starting at position n_gb.  virtTab supplies the type info. */
  NdbAggregator cte1Agg(virtTab);
  if (!cte1Agg.GroupByLinked(0, grpCol) ||
      !cte1Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: standard scanTable + readTuple agg-leaf */
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

  /* CTE 1 subtree: scanCte(0) with setAggregation(cte1Agg) — the
   * scanCte node IS the aggregate leaf, so DBLQH feeds each scanned
   * cte0 group into cte1's local JoinAggInterpreter directly.  No
   * MatchNonNull (no parent row to inner-join against — scanCte is
   * the root of CTE 1's subtree). */
  qb->beginCteSubtree(1);
  {
    NdbQueryOptions opts;
    opts.setAggregation(cte1Agg);
    if (qb->scanCte(0, 2, virtTab, &opts) == nullptr) {
      printf("FAILED (CTE 1 scanCte: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, virtTab, cte1Agg, /*depMask=*/(1ULL << 0)) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(1) — read cte1's hash table and deliver to API */
  const NdbQueryCteScanOperationDef *mainScan =
      qb->scanCte(1, 2, virtTab);
  if (mainScan == nullptr) {
    printf("FAILED (main scanCte(1): %s)\n", qb->getNdbError().message);
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

  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
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

  /* SUM(total) GROUP BY grp on (grp,total) input is the identity:
   * each grp has exactly one row in cte0, so cte1 has the same 3
   * rows with the same totals. */
  std::map<Int32, Int64> expected;
  expected[1] = 30;
  expected[2] = 70;
  expected[3] = 50;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 grp = raGrp ? raGrp->int32_value() : -1;
    Int64 total = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", grp, (long long)total);
    auto it = expected.find(grp);
    if (it == expected.end()) {
      printf("FAILED (unexpected grp=%d)\n", grp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (total != it->second) {
      printf("FAILED (grp=%d expected total=%lld got %lld)\n",
             grp, (long long)it->second, (long long)total);
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

  if (rowCount != 3) {
    printf("FAILED (expected 3 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 11: lookupCte as main query root (constant key) with child    */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*     FROM cte_src GROUP BY grp)                                      */
/*   SELECT cte0.grp, cte0.total, cte_src.pk, cte_src.val              */
/*   FROM cte0                       -- via lookupCte(0) with key=2   */
/*   JOIN cte_src ON cte_src.pk = cte0.grp                             */
/*                                                                     */
/* Why this test:                                                      */
/*   Exercises CTE_LOOKUP_REQ in the "main query root non-leaf" role  */
/*   — a CTE_LOOKUP that has NO parent (constant key, not driven by  */
/*   a parent row) and DOES have a child operation that consumes its  */
/*   result row.  T2 has lookupCte as a leaf; this test puts it at   */
/*   the root with a child below.                                      */
/*                                                                     */
/* Tree shape (built by NdbQueryBuilder):                              */
/*                                                                     */
/*   Node 0: CTE 0 subtree container (g_CteSubtreeOpInfo)              */
/*   Node 1: scanTable(cte_src) inside CTE 0  -- T_CTE_SCAN            */
/*           m_cteId=0, no T_AGGREGATE_LEAF                            */
/*   Node 2: readTuple(cte_src) inside CTE 0  -- T_AGGREGATE_LEAF      */
/*           m_cteId=0, T_INNER_JOIN, parent=node 1 (linkedValue pk)   */
/*   Node 3: lookupCte(0, key=constInt(2))    -- main root             */
/*           m_cteId=RNIL, T_USER_PROJECTION, T_INNER_JOIN,            */
/*           NO parent, has child node 4                               */
/*   Node 4: readTuple(cte_src, key=linked(node3,"grp"))               */
/*           m_cteId=RNIL, T_LEAF, T_USER_PROJECTION, T_INNER_JOIN,    */
/*           parent=node 3                                             */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   - scanTable(cte_src) reads 5 rows: pk=1..5                        */
/*   - For each row, readTuple fires (linkedValue pk) which is the    */
/*     agg leaf — feeds cte0Agg with the row.                          */
/*   - cte0Agg = GROUP BY grp, SUM(val) → 3 groups:                    */
/*       (grp=1, total=10+20=30)                                       */
/*       (grp=2, total=30+40=70)                                       */
/*       (grp=3, total=50)                                             */
/*   - CTE 0 hash table = {(1,30),(2,70),(3,50)}                       */
/*                                                                     */
/* Main query (after execCTE_START_MAIN_REQ):                          */
/*   - Fragment-per-node skip: only one DBSPJ fragment runs the main  */
/*     query (rootFragId 0 < numDataNodes 1).                          */
/*   - Node 3 (lookupCte) is the main root — fires ONE                 */
/*     CTE_LOOKUP_REQ to local DBLQH with constant key grp=2.          */
/*   - DBLQH looks up cte0 hash table, finds group (grp=2,total=70).  */
/*   - DBLQH sends TRANSID_AI back to DBSPJ with the cte0 row.        */
/*   - DBSPJ's cte_lookup_send / parent_row machinery processes the   */
/*     row and triggers the child node 4 (readTuple).                  */
/*   - Node 4: keyed by linkedValue(node3,"grp")=2 → does an LQHKEYREQ */
/*     on cte_src with pk=2, returns (pk=2, grp=1, val=20).            */
/*   - Both rows (cte0 row + cte_src row) are flushed to the API via  */
/*     FLUSH_AI with the same correlation.                             */
/*                                                                     */
/* Expected result row count: 1                                        */
/*   row[0]: cte0.grp=2, cte0.total=70,                               */
/*           cte_src.pk=2, cte_src.val=20                              */
/*                                                                     */
/* Flag bits expected to be observable in the DBSPJ tree dump:         */
/*   Node 1 (cte_src scan):    T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_* */
/*   Node 2 (cte0 readTuple):  T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF*/
/*   Node 3 (main lookupCte):  T_USER_PROJECTION | T_INNER_JOIN       */
/*   Node 4 (main readTuple):  T_USER_PROJECTION | T_INNER_JOIN |     */
/*                             T_LEAF                                  */
/* ------------------------------------------------------------------ */

static int
testLookupCteMainRootWithChild(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 11: lookupCte as main root + child readTuple ... ");
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

  /* CTE 0: GROUP BY grp, SUM(val) — Test 2 pattern */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanTable + readTuple agg-leaf (Test 2 pattern) */
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

  /* Main query root: lookupCte(0) with constant key grp=2.
   * Returns the single cte0 row (grp=2, total=70). */
  const NdbQueryOperand *mainCteKey[] = {
    qb->constValue(Int32(2)), nullptr
  };
  NdbQueryOptions mainCteOpts;
  mainCteOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *mainCteLookupOp =
      qb->lookupCte(0, 2, virtTab, mainCteKey, &mainCteOpts);
  if (mainCteLookupOp == nullptr) {
    printf("FAILED (main lookupCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Main child: readTuple(cte_src) keyed by linkedValue(mainLookup,"grp")
   * — the cte0 row's grp value is reused as a primary key into cte_src.
   * This forces cte_lookup_send's child-driving path to fire for the
   * single row returned by the constant-keyed mainCteLookup. */
  const NdbQueryOperand *childKey[] = {
    qb->linkedValue(mainCteLookupOp, "grp"), nullptr
  };
  NdbQueryOptions childOpts;
  childOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryOperationDef *childReadTupleOp =
      qb->readTuple(srcTab, childKey, &childOpts);
  if (childReadTupleOp == nullptr) {
    printf("FAILED (child readTuple: %s)\n", qb->getNdbError().message);
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

  /* Wire result projections on main lookupCte (cte0 columns) and the
   * child readTuple (cte_src columns).  numOps - 2 = main lookupCte,
   * numOps - 1 = child readTuple. */
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 childOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbQueryOperation *childOp = query->getQueryOperation(childOpNo);
  NdbRecAttr *raCteGrp = nullptr;
  NdbRecAttr *raCteTotal = nullptr;
  NdbRecAttr *raSrcPk = nullptr;
  NdbRecAttr *raSrcVal = nullptr;
  if (mainOp != nullptr) {
    raCteGrp = mainOp->getValue("grp");
    raCteTotal = mainOp->getValue("total");
  }
  if (childOp != nullptr) {
    raSrcPk = childOp->getValue("pk");
    raSrcVal = childOp->getValue("val");
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

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 cteGrp = raCteGrp ? raCteGrp->int32_value() : -1;
    Int64 cteTotal = raCteTotal ? raCteTotal->int64_value() : -1;
    Int32 srcPk = raSrcPk ? raSrcPk->int32_value() : -1;
    Int64 srcVal = raSrcVal ? raSrcVal->int64_value() : -1;
    V("  row: cte0(grp=%d,total=%lld) cte_src(pk=%d,val=%lld)\n",
      cteGrp, (long long)cteTotal, srcPk, (long long)srcVal);
    if (cteGrp != 2 || cteTotal != 70) {
      printf("FAILED (cte0 mismatch: grp=%d total=%lld, expected 2/70)\n",
             cteGrp, (long long)cteTotal);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (srcPk != 2 || srcVal != 20) {
      printf("FAILED (cte_src mismatch: pk=%d val=%lld, expected 2/20)\n",
             srcPk, (long long)srcVal);
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

  if (rowCount != 1) {
    printf("FAILED (expected 1 row, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 15: scanCte as main query agg leaf                            */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*     FROM cte_src GROUP BY grp)                                      */
/*   SELECT COUNT(*), SUM(cte0.total)                                  */
/*   FROM cte0   -- via scanCte(0) with main aggregator                */
/*                                                                     */
/* Why this test:                                                      */
/*   T10 already exercises scanCte with setAggregation, but the agg   */
/*   target there is the *enclosing CTE 1's* aggregator — it hits the */
/*   `m_cteAggStateKeys[encCteIdx*MAX_NDB_NODES + targetNodeId]`       */
/*   branch in cte_scan_start because the scanCte node has            */
/*   m_cteId != RNIL.                                                  */
/*                                                                     */
/*   T15 puts scanCte at the MAIN query root (m_cteId == RNIL) with   */
/*   setAggregation — this exercises the OTHER branch in              */
/*   cte_scan_start that uses `m_aggStateKeys[targetNodeId]`           */
/*   (the main query's per-node aggStateKey) instead.  Without this   */
/*   test the main-aggregator path was untested.                       */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src)                T_CTE_SCAN, m_cteId=0  */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"))           T_AGGREGATE_LEAF,      */
/*                                              T_INNER_JOIN, T_LEAF, */
/*                                              m_cteId=0             */
/*   Node 3: scanCte(0) with                   T_AGGREGATE_LEAF,      */
/*           setAggregation(mainAgg)            T_USER_PROJECTION,     */
/*                                              T_LEAF, m_cteId=RNIL  */
/*           data.m_joinAggStateKey points to                         */
/*           m_aggStateKeys[targetNodeId] (main aggregator,           */
/*           NOT m_cteAggStateKeys[*]).  This is the code path the    */
/*           test exercises.                                           */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   - Same as T2 / T11: scanTable + readTuple agg leaf builds        */
/*     cte0 = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)}.   */
/*                                                                     */
/* Main query:                                                         */
/*   - Each DBSPJ instance starts cte_scan_start for its rootFragId.  */
/*   - Fragment-per-node skip applies (RT_CTE_PHASE clear during      */
/*     main): only rootFragId < numDataNodes runs the scan.            */
/*   - The selected DBSPJ instance:                                   */
/*       1. Detects T_AGGREGATE_LEAF, m_cteId == RNIL.                */
/*       2. Computes joinAggStateKey =                                */
/*          encodeAggStateKey(m_aggStateKeys[targetNodeId],           */
/*                           m_agg_leaf_index).                       */
/*       3. Sends CTE_SCAN_REQ to local DBLQH with this              */
/*          joinAggStateKey set.                                       */
/*   - DBLQH walker enters the agg-feed branch:                       */
/*       For each of cte0's 3 groups, builds linked_attr_data and    */
/*       calls targetInterp->processRecWithLinkedAttrs() — feeding    */
/*       the row into the MAIN aggregator's local hash table.         */
/*   - CTE_SCAN_CONF returned to DBSPJ with numRows=3, but            */
/*     execCTE_SCAN_CONF skips m_rows / rowsExpecting because         */
/*     joinAggStateKey != RNIL (path (a) added in commit 0ccbf93).    */
/*                                                                     */
/* Main aggregator finalization:                                       */
/*   - mainAgg has no GroupBy → produces a single result row.          */
/*   - COUNT(*) over the 3 cte0 groups = 3                             */
/*   - SUM(linkedColumn=total) = 30+70+50 = 150                        */
/*   - The result row goes through DBTC's main-aggregation final      */
/*     pipeline (same as Test 3) and is fetched via                    */
/*     query->getAggregator()->FetchResultRecord().                    */
/*                                                                     */
/* Expected aggregation result:                                        */
/*   COUNT = 3                                                         */
/*   SUM   = 150                                                       */
/*                                                                     */
/* Flag bits to verify in the DBSPJ tree dump:                         */
/*   Node 1 scan:        T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*       */
/*   Node 2 readTuple:   T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF     */
/*   Node 3 scanCte:     T_AGGREGATE_LEAF | T_USER_PROJECTION |       */
/*                       T_LEAF, m_cteId=RNIL                          */
/*                       data.m_joinAggStateKey != RNIL                */
/* ------------------------------------------------------------------ */

static int
testScanCteMainAggLeaf(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 15: scanCte as main agg leaf ... ");
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
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  if (totalCol == nullptr) {
    printf("FAILED (column lookup: total)\n");
    return -1;
  }

  /* CTE 0: GROUP BY grp, SUM(val) — Test 2 pattern */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregator: COUNT(*), SUM(cte0.total).  No GROUP BY → single
   * result row.  The "total" column comes via linked_attr_data
   * position 1 (after the GROUP BY key columns).  CTE 0 has 1 GB
   * column and 1 agg result, so position 1 = first agg result =
   * cte0.total.  totalCol gives the type info (BIGINT). */
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, totalCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanTable + readTuple agg leaf */
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

  /* Main query: scanCte(0) at the root with the MAIN aggregator
   * attached.  No MatchNonNull (no parent row to inner-join against).
   * setAggregation with mainAgg makes the scanCte the main query's
   * aggregate leaf — its joinAggStateKey is computed from
   * m_aggStateKeys (NOT m_cteAggStateKeys) because m_cteId == RNIL. */
  NdbQueryOptions mainScanOpts;
  mainScanOpts.setAggregation(mainAgg);
  const NdbQueryCteScanOperationDef *mainScanCteOp =
      qb->scanCte(0, 2, virtTab, &mainScanOpts);
  if (mainScanCteOp == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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

  /* Drain any non-aggregation rows (there shouldn't be any — main
   * is an agg-only query — but the call drives the kernel state
   * machine to completion). */
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}

  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Fetch the main aggregator result (same pattern as Test 3) */
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

  if (count == 3 && sum == 150) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }

  printf("FAILED (expected COUNT=3 SUM=150, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 13: lookupCte as main query internal node                     */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*     FROM cte_src GROUP BY grp)                                      */
/*   SELECT main.pk, main.grp,                                         */
/*          cte0.grp, cte0.total,                                      */
/*          src2.pk, src2.val                                          */
/*   FROM cte_src AS main                                              */
/*   JOIN cte0 ON cte0.grp = main.grp                                  */
/*   JOIN cte_src AS src2 ON src2.pk = cte0.grp                        */
/*                                                                     */
/* Why this test:                                                      */
/*   Exercises CTE_LOOKUP_REQ in the "internal" role — it has a       */
/*   parent operation (the main scan that drives it via linkedValue)  */
/*   AND a child operation (the readTuple driven by its result via    */
/*   another linkedValue).  T2 / T5 / T6 etc. all use lookupCte as    */
/*   a leaf (no children below).  T13 places lookupCte in the middle */
/*   of a 3-level main query chain to verify cte_lookup_send's       */
/*   row-forwarding path drives child operations correctly.            */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src) inside CTE 0                          */
/*           T_CTE_SCAN, m_cteId=0                                     */
/*   Node 2: readTuple(cte_src, key=linked(node1,"pk"))                */
/*           T_AGGREGATE_LEAF, T_INNER_JOIN, T_LEAF, m_cteId=0        */
/*   Node 3: scanTable(cte_src)                — main root             */
/*           NOT T_CTE_SCAN, m_cteId=RNIL, T_USER_PROJECTION,         */
/*           T_BUFFER_MAP (because it has a lookup child via          */
/*           cte_lookup_send), has children                            */
/*   Node 4: lookupCte(0, key=linked(node3,"grp")) -- INTERNAL        */
/*           parent=node 3, has child=node 5                           */
/*           m_cteId=RNIL, T_USER_PROJECTION, T_INNER_JOIN,           */
/*           NOT T_AGGREGATE_LEAF, NOT T_LEAF                          */
/*   Node 5: readTuple(cte_src, key=linked(node4,"grp"))               */
/*           parent=node 4, T_LEAF, T_USER_PROJECTION, T_INNER_JOIN,  */
/*           m_cteId=RNIL                                              */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   Same as Test 2: cte0 = {(1,30),(2,70),(3,50)}.                    */
/*                                                                     */
/* Main query:                                                         */
/*   - Each DBSPJ instance starts the main scan (rootFragId per       */
/*     fragment).  Standard scanFrag — no fragment-per-node skip      */
/*     (skip only applies to scanCte, not scanTable).                  */
/*   - Main scan reads cte_src in some order.  Five rows total       */
/*     across all fragments:                                           */
/*       (pk=1,grp=1,val=10), (pk=2,grp=1,val=20),                    */
/*       (pk=3,grp=2,val=30), (pk=4,grp=2,val=40),                    */
/*       (pk=5,grp=3,val=50).                                          */
/*   - For each main row, DBSPJ fires CTE_LOOKUP_REQ to local DBLQH  */
/*     keyed by main.grp.  cte_lookup_send is in its "non-leaf"      */
/*     branch (cnt counts the TRANSID_AI plus the child ops it       */
/*     drives).  DBLQH returns the matching cte0 group:               */
/*       (pk=1,grp=1) → cte0 grp=1 → (1,30)                            */
/*       (pk=2,grp=1) → cte0 grp=1 → (1,30)                            */
/*       (pk=3,grp=2) → cte0 grp=2 → (2,70)                            */
/*       (pk=4,grp=2) → cte0 grp=2 → (2,70)                            */
/*       (pk=5,grp=3) → cte0 grp=3 → (3,50)                            */
/*   - The cte0 row arrives at DBSPJ as TRANSID_AI.  DBSPJ's          */
/*     execTRANSID_AI processes it and calls startNextNodes which    */
/*     fires the child node 5 (readTuple) with key=cte0.grp.          */
/*     pk values map directly:                                         */
/*       cte0 grp=1 → readTuple pk=1 → (pk=1,grp=1,val=10)             */
/*       cte0 grp=1 → readTuple pk=1 → (pk=1,grp=1,val=10)  (dup)      */
/*       cte0 grp=2 → readTuple pk=2 → (pk=2,grp=1,val=20)             */
/*       cte0 grp=2 → readTuple pk=2 → (pk=2,grp=1,val=20)  (dup)      */
/*       cte0 grp=3 → readTuple pk=3 → (pk=3,grp=2,val=30)             */
/*   - All three nodes (main scan, cte lookup, readTuple) carry      */
/*     T_USER_PROJECTION, so each contributes its columns to the     */
/*     final row delivered to the API via FLUSH_AI.                    */
/*                                                                     */
/* Expected result row count: 5                                        */
/*                                                                     */
/*   For each main row we verify: cte0.total matches the expected     */
/*   total per main.grp, and src2.val equals the val of the cte_src  */
/*   row whose pk equals main.grp.                                     */
/*                                                                     */
/* Flag bits to verify in the DBSPJ tree dump:                         */
/*   Node 1 cte0 scan:    T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*      */
/*   Node 2 cte0 leaf:    T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF    */
/*   Node 3 main scan:    T_USER_PROJECTION | T_INNER_JOIN |          */
/*                        T_BUFFER_MAP | T_CHK_CONGESTION             */
/*   Node 4 cte lookup:   T_USER_PROJECTION | T_INNER_JOIN  (NOT      */
/*                        T_LEAF, NOT T_AGGREGATE_LEAF)               */
/*   Node 5 main leaf:    T_USER_PROJECTION | T_INNER_JOIN | T_LEAF   */
/* ------------------------------------------------------------------ */

static int
testLookupCteMainInternal(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 13: lookupCte as main internal ... ");
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

  /* CTE 0: GROUP BY grp, SUM(val) — Test 2 pattern */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree (Test 2 pattern) */
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

  /* Main query root: scan cte_src */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Internal lookupCte: keyed by main.grp, has BOTH a parent (the
   * main scan via linkedValue) AND a child (the readTuple below). */
  const NdbQueryOperand *cteKey[] = {
    qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *cteLookupOp =
      qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);
  if (cteLookupOp == nullptr) {
    printf("FAILED (lookupCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Leaf readTuple: keyed by cte0.grp (forces cte_lookup_send to
   * route the row to a child operation, exercising the
   * "internal lookupCte" path). */
  const NdbQueryOperand *leafKey[] = {
    qb->linkedValue(cteLookupOp, "grp"), nullptr
  };
  NdbQueryOptions leafOpts;
  leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryOperationDef *leafReadTupleOp =
      qb->readTuple(srcTab, leafKey, &leafOpts);
  if (leafReadTupleOp == nullptr) {
    printf("FAILED (leaf readTuple: %s)\n", qb->getNdbError().message);
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

  /* Wire result projections on main scan, cte lookup, and leaf
   * readTuple.  Last three operations in the query def. */
  const Uint32 mainScanOpNo = queryDef->getNoOfOperations() - 3;
  const Uint32 cteLookupOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 leafOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainScanQOp = query->getQueryOperation(mainScanOpNo);
  NdbQueryOperation *cteLookupQOp = query->getQueryOperation(cteLookupOpNo);
  NdbQueryOperation *leafQOp = query->getQueryOperation(leafOpNo);
  NdbRecAttr *raMainPk = nullptr;
  NdbRecAttr *raMainGrp = nullptr;
  NdbRecAttr *raCteGrp = nullptr;
  NdbRecAttr *raCteTotal = nullptr;
  NdbRecAttr *raLeafPk = nullptr;
  NdbRecAttr *raLeafVal = nullptr;
  if (mainScanQOp != nullptr) {
    raMainPk = mainScanQOp->getValue("pk");
    raMainGrp = mainScanQOp->getValue("grp");
  }
  if (cteLookupQOp != nullptr) {
    raCteGrp = cteLookupQOp->getValue("grp");
    raCteTotal = cteLookupQOp->getValue("total");
  }
  if (leafQOp != nullptr) {
    raLeafPk = leafQOp->getValue("pk");
    raLeafVal = leafQOp->getValue("val");
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

  /* Per-grp expectations:
   *   grp 1 → cte0.total=30, leaf row pk=1 val=10
   *   grp 2 → cte0.total=70, leaf row pk=2 val=20
   *   grp 3 → cte0.total=50, leaf row pk=3 val=30 */
  std::map<Int32, std::pair<Int64, std::pair<Int32, Int64>>> expected;
  expected[1] = {30, {1, 10}};
  expected[2] = {70, {2, 20}};
  expected[3] = {50, {3, 30}};

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 mainPk = raMainPk ? raMainPk->int32_value() : -1;
    Int32 mainGrp = raMainGrp ? raMainGrp->int32_value() : -1;
    Int32 cteGrp = raCteGrp ? raCteGrp->int32_value() : -1;
    Int64 cteTotal = raCteTotal ? raCteTotal->int64_value() : -1;
    Int32 leafPk = raLeafPk ? raLeafPk->int32_value() : -1;
    Int64 leafVal = raLeafVal ? raLeafVal->int64_value() : -1;
    V("  row: main(pk=%d,grp=%d) cte0(grp=%d,total=%lld) "
      "leaf(pk=%d,val=%lld)\n",
      mainPk, mainGrp, cteGrp, (long long)cteTotal,
      leafPk, (long long)leafVal);
    auto it = expected.find(mainGrp);
    if (it == expected.end()) {
      printf("FAILED (unexpected main.grp=%d)\n", mainGrp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    Int64 expCteTotal = it->second.first;
    Int32 expLeafPk = it->second.second.first;
    Int64 expLeafVal = it->second.second.second;
    if (cteGrp != mainGrp || cteTotal != expCteTotal ||
        leafPk != expLeafPk || leafVal != expLeafVal) {
      printf("FAILED (mismatch for main.grp=%d: "
             "cte0(grp=%d,total=%lld), leaf(pk=%d,val=%lld) — "
             "expected cte0(%d,%lld), leaf(%d,%lld))\n",
             mainGrp, cteGrp, (long long)cteTotal, leafPk,
             (long long)leafVal, mainGrp, (long long)expCteTotal,
             expLeafPk, (long long)expLeafVal);
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
/* Test 12: lookupCte as CTE materialization root + child              */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY  */
/*              grp),                                                  */
/*     cte1 AS (SELECT cte_src.grp, SUM(cte_src.val) AS total          */
/*              FROM cte0 -- via lookupCte(0, key=1)                  */
/*              JOIN cte_src ON cte_src.pk = cte0.grp                  */
/*              GROUP BY cte_src.grp)                                  */
/*   SELECT * FROM cte1                                                */
/*                                                                     */
/* Why this test:                                                      */
/*   Exercises CTE_LOOKUP_REQ in the "root non-leaf" role inside a     */
/*   CTE materialization subtree.  CTE 1's subtree starts with a      */
/*   constant-keyed lookupCte (no parent row drives it — it fires    */
/*   once at materialization start) and that lookup's result drives   */
/*   a child readTuple which is the agg leaf for cte1.                */
/*                                                                     */
/*   Two kernel-side pieces enable this:                               */
/*    (1) DBSPJ build loop walk-up extension: when an agg leaf has    */
/*        no real-table scan above it but its subtree-root ancestor  */
/*        is itself a CTE_LOOKUP / CTE_SCAN, mark THAT ancestor with  */
/*        T_CTE_SCAN and record it on the CteContext.  This makes    */
/*        checkPrepareComplete / execCTE_PHASE_START_REQ find the    */
/*        CTE-only subtree root for materialization startup.          */
/*    (2) cte_start (the OpInfo m_start hook for CTE_LOOKUP nodes):  */
/*        when called on a node with T_CTE_SCAN set, fire the        */
/*        constant-keyed lookup directly via cte_lookup_send with    */
/*        a dummy RowPtr (the constant-key path doesn't read         */
/*        rowRef, only the T_KEYINFO_CONSTRUCTED path does).         */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src) inside CTE 0                          */
/*           T_CTE_SCAN, m_cteId=0                                     */
/*   Node 2: readTuple(cte_src, key=linked(node1,"pk"))                */
/*           T_AGGREGATE_LEAF, m_cteId=0                               */
/*   Node 3: CTE 1 subtree container                                   */
/*   Node 4: lookupCte(0, key=constInt(1))    -- CTE 1 subtree root   */
/*           m_cteId=1, T_CTE_SCAN (set by build-loop extension),     */
/*           NOT T_AGGREGATE_LEAF, has child node 5                    */
/*   Node 5: readTuple(cte_src, key=linked(node4,"grp"),               */
/*           setAggregation(cte1Agg))                                  */
/*           T_AGGREGATE_LEAF, T_INNER_JOIN, T_LEAF, m_cteId=1,       */
/*           parent=node 4, agg leaf for cte1                          */
/*   Node 6: scanCte(1)                       -- main query root      */
/*           m_cteId=RNIL, T_USER_PROJECTION                           */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   Standard Test 2 pattern: cte0 = {(1,30),(2,70),(3,50)}.           */
/*                                                                     */
/* Phase 1 (CTE 1 materialization):                                    */
/*   - execCTE_PHASE_START_REQ finds node 4 (T_CTE_SCAN, m_cteId=1,   */
/*     phase=1) and calls its m_start = cte_start.                     */
/*   - cte_start sees T_CTE_SCAN is set on node 4 (lookupCte) and     */
/*     fires cte_lookup_send with a dummy RowPtr.  cte_lookup_send   */
/*     takes the constant-key branch (no T_KEYINFO_CONSTRUCTED), uses */
/*     the pre-built keyInfoPtrI (constant key=1).                    */
/*   - DBLQH receives CTE_LOOKUP_REQ with key=1 → looks up cte0 for  */
/*     grp=1 → returns row (grp=1, total=30) via TRANSID_AI.          */
/*   - DBSPJ receives the row, runs startNextNodes for child node 5. */
/*   - Node 5 readTuple fires with pk=linkedValue(cteLookup,"grp")=1 */
/*     → returns cte_src row (pk=1, grp=1, val=10).                   */
/*   - readTuple is the agg leaf — feeds cte1Agg with row             */
/*     (cte_src.grp=1, cte_src.val=10).                                */
/*   - cte1Agg = GROUP BY cte_src.grp, SUM(cte_src.val) → 1 group:   */
/*     (grp=1, total=10).                                              */
/*                                                                     */
/* Main query:                                                         */
/*   - scanCte(1) at main root reads cte1 (1 group) and delivers     */
/*     1 row via FLUSH_AI to API.                                      */
/*                                                                     */
/* Expected result: 1 main row [grp=1, total=10].                     */
/*                                                                     */
/* Flag bits to verify in the DBSPJ tree dump:                         */
/*   Node 1: T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*                    */
/*   Node 2: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF (cte0 leaf)     */
/*   Node 4: T_CTE_SCAN | T_INNER_JOIN  -- the new build path        */
/*   Node 5: T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF (cte1 leaf)     */
/*   Node 6: T_USER_PROJECTION  (main scanCte)                         */
/* ------------------------------------------------------------------ */

static int
testLookupCteCteMatRoot(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 12: lookupCte as CTE materialization root ... ");
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

  /* CTE 0: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1: GROUP BY cte_src.grp, SUM(cte_src.val).  Built on srcTab
   * because the agg leaf is a readTuple over cte_src. */
  NdbAggregator cte1Agg(srcTab);
  if (!cte1Agg.GroupBy("grp") ||
      !cte1Agg.LoadColumn("val", 0) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: standard scanTable + readTuple agg leaf */
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

  /* CTE 1 subtree: lookupCte(0, key=constInt(1)) at the root,
   * readTuple as the agg leaf child. */
  qb->beginCteSubtree(1);
  {
    const NdbQueryOperand *cteLookupKey[] = {
      qb->constValue(Int32(1)), nullptr
    };
    NdbQueryOptions cteLookupOpts;
    cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    const NdbQueryCteLookupOperationDef *cteLookupOp =
        qb->lookupCte(0, 2, virtTab, cteLookupKey, &cteLookupOpts);
    if (cteLookupOp == nullptr) {
      printf("FAILED (CTE 1 lookupCte: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }

    const NdbQueryOperand *leafKey[] = {
      qb->linkedValue(cteLookupOp, "grp"), nullptr
    };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cte1Agg);
    if (qb->readTuple(srcTab, leafKey, &leafOpts) == nullptr) {
      printf("FAILED (CTE 1 leaf readTuple: %s)\n",
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, srcTab, cte1Agg, /*depMask=*/(1ULL << 0)) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(1) — read cte1 hash table to API */
  const NdbQueryCteScanOperationDef *mainScanCteOp =
      qb->scanCte(1, 2, virtTab);
  if (mainScanCteOp == nullptr) {
    printf("FAILED (main scanCte(1): %s)\n", qb->getNdbError().message);
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

  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
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

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 grp = raGrp ? raGrp->int32_value() : -1;
    Int64 total = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", grp, (long long)total);
    if (grp != 1 || total != 10) {
      printf("FAILED (cte1 mismatch: grp=%d total=%lld, expected 1/10)\n",
             grp, (long long)total);
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

  if (rowCount != 1) {
    printf("FAILED (expected 1 row, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 14: lookupCte as CTE materialization internal                  */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY  */
/*              grp),                                                  */
/*     cte1 AS (SELECT s2.grp, SUM(s2.val) AS total                    */
/*              FROM cte_src AS s1                                     */
/*              JOIN cte0 ON cte0.grp = s1.grp                         */
/*              JOIN cte_src AS s2 ON s2.pk = cte0.grp                 */
/*              GROUP BY s2.grp)                                       */
/*   SELECT * FROM cte1                                                */
/*                                                                     */
/* Why this test:                                                      */
/*   Exercises CTE_LOOKUP_REQ in the "internal" role inside a CTE     */
/*   materialization subtree.  The middle lookupCte has a parent     */
/*   (the subtree's scanTable that drives it via linkedValue) AND a  */
/*   child (the readTuple that consumes cte0.grp via linkedValue).   */
/*   This is the CTE-materialization counterpart to T13.              */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src)            T_CTE_SCAN, m_cteId=0      */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"))       T_AGGREGATE_LEAF, T_LEAF, */
/*                                          m_cteId=0                  */
/*   Node 3: CTE 1 subtree container                                   */
/*   Node 4: scanTable(cte_src) inside CTE 1                          */
/*           T_CTE_SCAN, m_cteId=1, T_BUFFER_*                         */
/*           (the build path that marks it T_CTE_SCAN is the          */
/*           pre-existing "non-CTE_LOOKUP/CTE_SCAN node inside        */
/*           subtree" branch.)                                         */
/*   Node 5: lookupCte(0,                                             */
/*           key=linked(node4,"grp"))       INTERNAL: parent=node 4,  */
/*                                          has child=node 6,         */
/*                                          m_cteId=1, T_INNER_JOIN,  */
/*                                          NOT T_AGGREGATE_LEAF      */
/*   Node 6: readTuple(cte_src,                                       */
/*           key=linked(node5,"grp"),                                 */
/*           setAggregation(cte1Agg))       T_AGGREGATE_LEAF, T_LEAF, */
/*                                          T_INNER_JOIN, m_cteId=1   */
/*   Node 7: scanCte(1)                     -- main query root        */
/*           m_cteId=RNIL, T_USER_PROJECTION                           */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   cte0 = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)}.     */
/*                                                                     */
/* Phase 1 (CTE 1 materialization on each DBSPJ instance):             */
/*   - Node 4 scanTable(cte_src) reads 5 rows from cte_src locally.  */
/*   - For each scanned row, node 5 (cteLookup) fires keyed by       */
/*     s1.grp.  cte_lookup_send takes the T_KEYINFO_CONSTRUCTED      */
/*     branch (key built from parent row).  DBLQH returns the cte0  */
/*     row for that grp:                                               */
/*       s1=(pk=1,grp=1) → cte0(1)=(1,30)                              */
/*       s1=(pk=2,grp=1) → cte0(1)=(1,30)                              */
/*       s1=(pk=3,grp=2) → cte0(2)=(2,70)                              */
/*       s1=(pk=4,grp=2) → cte0(2)=(2,70)                              */
/*       s1=(pk=5,grp=3) → cte0(3)=(3,50)                              */
/*   - The cte0 row arrives at DBSPJ as TRANSID_AI.  Node 5 is      */
/*     internal, so DBSPJ runs startNextNodes for the child node 6.  */
/*   - Node 6 readTuple fires keyed by cte0.grp:                      */
/*       cte0(1) → readTuple(pk=1) → s2=(pk=1,grp=1,val=10)            */
/*       cte0(1) → readTuple(pk=1) → s2=(pk=1,grp=1,val=10)            */
/*       cte0(2) → readTuple(pk=2) → s2=(pk=2,grp=1,val=20)            */
/*       cte0(2) → readTuple(pk=2) → s2=(pk=2,grp=1,val=20)            */
/*       cte0(3) → readTuple(pk=3) → s2=(pk=3,grp=2,val=30)            */
/*   - Node 6 is the agg leaf — feeds cte1Agg with 5 rows:           */
/*       (grp=1,val=10), (grp=1,val=10),                              */
/*       (grp=1,val=20), (grp=1,val=20),                              */
/*       (grp=2,val=30)                                                */
/*   - cte1Agg = GROUP BY s2.grp, SUM(s2.val):                        */
/*       grp=1: 10+10+20+20 = 60                                       */
/*       grp=2: 30                                                     */
/*       grp=3: not present (no s1 row maps via cte0 to a pk whose   */
/*              s2.grp=3 — pk=3's s2.grp is 2)                         */
/*   - cte1 = {(1,60),(2,30)} → 2 groups.                              */
/*                                                                     */
/* Main query:                                                         */
/*   scanCte(1) reads cte1 (2 groups) and delivers to API.            */
/*                                                                     */
/* Expected result rows (2):                                           */
/*   (grp=1, total=60)                                                 */
/*   (grp=2, total=30)                                                 */
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1 (cte0 scan):  T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*      */
/*   Node 2 (cte0 leaf):  T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF    */
/*   Node 4 (cte1 scan):  T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*      */
/*   Node 5 (cte1 lookupCte internal):                                 */
/*                        T_INNER_JOIN  (NOT T_LEAF, NOT              */
/*                        T_AGGREGATE_LEAF)                            */
/*   Node 6 (cte1 leaf):  T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF    */
/*   Node 7 (main scan):  T_USER_PROJECTION                            */
/* ------------------------------------------------------------------ */

static int
testLookupCteCteMatInternal(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 14: lookupCte as CTE materialization internal ... ");
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

  /* CTE 0: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1: GROUP BY s2.grp, SUM(s2.val) on cte_src rows */
  NdbAggregator cte1Agg(srcTab);
  if (!cte1Agg.GroupBy("grp") ||
      !cte1Agg.LoadColumn("val", 0) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree (Test 2 pattern) */
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

  /* CTE 1 subtree: scanTable → lookupCte (internal) → readTuple agg leaf */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *cte1Scan = qb->scanTable(srcTab);
    if (cte1Scan == nullptr) {
      printf("FAILED (CTE 1 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }

    const NdbQueryOperand *cteLookupKey[] = {
      qb->linkedValue(cte1Scan, "grp"), nullptr
    };
    NdbQueryOptions cteLookupOpts;
    cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    const NdbQueryCteLookupOperationDef *cteLookupOp =
        qb->lookupCte(0, 2, virtTab, cteLookupKey, &cteLookupOpts);
    if (cteLookupOp == nullptr) {
      printf("FAILED (CTE 1 lookupCte: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }

    const NdbQueryOperand *leafKey[] = {
      qb->linkedValue(cteLookupOp, "grp"), nullptr
    };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cte1Agg);
    if (qb->readTuple(srcTab, leafKey, &leafOpts) == nullptr) {
      printf("FAILED (CTE 1 leaf readTuple: %s)\n",
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, srcTab, cte1Agg, /*depMask=*/(1ULL << 0)) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(1) → API */
  const NdbQueryCteScanOperationDef *mainScanCteOp =
      qb->scanCte(1, 2, virtTab);
  if (mainScanCteOp == nullptr) {
    printf("FAILED (main scanCte(1): %s)\n", qb->getNdbError().message);
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

  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
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

  /* Expected cte1 = {(1,60),(2,30)} */
  std::map<Int32, Int64> expected;
  expected[1] = 60;
  expected[2] = 30;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 grp = raGrp ? raGrp->int32_value() : -1;
    Int64 total = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", grp, (long long)total);
    auto it = expected.find(grp);
    if (it == expected.end()) {
      printf("FAILED (unexpected cte1.grp=%d)\n", grp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (total != it->second) {
      printf("FAILED (cte1 grp=%d expected total=%lld got %lld)\n",
             grp, (long long)it->second, (long long)total);
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

  if (rowCount != 2) {
    printf("FAILED (expected 2 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 16: scanCte as CTE materialization root non-leaf with child   */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH                                                              */
/*     cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY  */
/*              grp),                                                  */
/*     cte1 AS (SELECT s.grp, SUM(s.val) AS total                      */
/*              FROM cte0 -- via scanCte(0)                            */
/*              JOIN cte_src AS s ON s.pk = cte0.grp                   */
/*              GROUP BY s.grp)                                        */
/*   SELECT * FROM cte1                                                */
/*                                                                     */
/* Why this test:                                                      */
/*   Exercises CTE_SCAN_REQ in the "root non-leaf" role inside a CTE  */
/*   materialization subtree.  CTE 1's subtree starts with a          */
/*   scanCte(0) (no setAggregation on the scanCte itself — it's       */
/*   the data source, not the agg leaf) that drives a child          */
/*   readTuple via linkedValue("grp").  The readTuple is the agg     */
/*   leaf for cte1.                                                    */
/*                                                                     */
/*   Three pieces enable this:                                         */
/*    (1) Build-loop walk-up extension (added in T12): when an agg   */
/*        leaf has a CTE-only subtree-root ancestor (g_CteScanOpInfo */
/*        or g_CteLookupOpInfo), mark the ancestor T_CTE_SCAN +      */
/*        record it on the CteContext.  T12 used this for           */
/*        lookupCte; T16 reuses it for scanCte.                       */
/*    (2) parseDA suppressFlushAI extension: a non-agg-leaf node     */
/*        inside a CTE subtree must NOT emit FLUSH_AI to API during  */
/*        materialization — its rows must come back to DBSPJ to     */
/*        drive the child operations.  RT_AGGREGATE is only set for */
/*        main-query nodes, so RT_AGGREGATE alone wasn't enough to  */
/*        trigger suppression for CTE subtree nodes.  Extended       */
/*        suppressFlushAI to also fire when                          */
/*        ctx.m_cteSubtreeRemaining > 0.                              */
/*    (3) Fragment-per-node skip restriction (existing): the skip    */
/*        only applies during main query (RT_CTE_PHASE clear), so   */
/*        every DBSPJ instance runs CTE 1 materialization on its    */
/*        local data.                                                 */
/*                                                                     */
/* Tree shape:                                                         */
/*                                                                     */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanTable(cte_src)            T_CTE_SCAN, m_cteId=0      */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"))       T_AGGREGATE_LEAF, T_LEAF, */
/*                                          m_cteId=0                  */
/*   Node 3: CTE 1 subtree container                                   */
/*   Node 4: scanCte(0)                    -- CTE 1 subtree root      */
/*           m_cteId=1, T_CTE_SCAN (set by build-loop walk-up         */
/*           when node 5 below is processed),                         */
/*           NOT T_USER_PROJECTION (suppressed by parseDA's          */
/*           extended suppressFlushAI logic),                         */
/*           NOT T_AGGREGATE_LEAF                                      */
/*   Node 5: readTuple(cte_src,                                       */
/*           key=linked(node4,"grp"),                                 */
/*           setAggregation(cte1Agg))       T_AGGREGATE_LEAF, T_LEAF, */
/*                                          T_INNER_JOIN, m_cteId=1,  */
/*                                          parent=node 4             */
/*   Node 6: scanCte(1)                     -- main query root        */
/*           m_cteId=RNIL, T_USER_PROJECTION                           */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   cte0 = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)}.     */
/*                                                                     */
/* Phase 1 (CTE 1 materialization on each DBSPJ instance):             */
/*   - execCTE_PHASE_START_REQ finds node 4 (T_CTE_SCAN, m_cteId=1)  */
/*     and calls cte_scan_start.                                       */
/*   - Fragment-per-node skip is NOT active in CTE phase, so every   */
/*     DBSPJ instance runs the local CTE 1 materialization.           */
/*   - cte_scan_start sends CTE_SCAN_REQ to local DBLQH for cte0.    */
/*     joinAggStateKey is RNIL because node 4 is NOT T_AGGREGATE_LEAF.*/
/*   - DBLQH walker takes the non-agg-feed branch.                    */
/*     T_USER_PROJECTION is NOT set on node 4 (suppressed) and        */
/*     parseDA didn't insert FLUSH_AI in finalR — so the walker      */
/*     emits the "legacy" path: raw [keys + agg cols + CORR_FACTOR32]*/
/*     to senderRef (DBSPJ).                                          */
/*   - DBSPJ receives the cte0 row at node 4 via execTRANSID_AI.     */
/*     startNextNodes drives the child node 5.                        */
/*   - Node 5 readTuple fires keyed by cte0.grp:                      */
/*       cte0(1) → readTuple(pk=1) → s=(pk=1,grp=1,val=10)            */
/*       cte0(2) → readTuple(pk=2) → s=(pk=2,grp=1,val=20)            */
/*       cte0(3) → readTuple(pk=3) → s=(pk=3,grp=2,val=30)            */
/*   - Node 5 is the agg leaf — feeds cte1Agg with 3 rows:           */
/*       (grp=1,val=10), (grp=1,val=20), (grp=2,val=30)               */
/*   - cte1Agg = GROUP BY s.grp, SUM(s.val):                          */
/*       grp=1: 10+20 = 30                                             */
/*       grp=2: 30                                                     */
/*   - cte1 = {(1,30),(2,30)} → 2 groups.                              */
/*                                                                     */
/* Main query:                                                         */
/*   scanCte(1) reads cte1 (2 groups) and delivers to API.            */
/*                                                                     */
/* Expected result rows (2):                                           */
/*   (grp=1, total=30)                                                 */
/*   (grp=2, total=30)                                                 */
/*                                                                     */
/* Flag bits to verify:                                                */
/*   Node 1 (cte0 scan):  T_CTE_SCAN | T_INNER_JOIN | T_BUFFER_*      */
/*   Node 2 (cte0 leaf):  T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF    */
/*   Node 4 (cte1 scanCte root):                                       */
/*                        T_CTE_SCAN, NOT T_USER_PROJECTION,          */
/*                        NOT T_AGGREGATE_LEAF                        */
/*   Node 5 (cte1 leaf):  T_AGGREGATE_LEAF | T_INNER_JOIN | T_LEAF    */
/*   Node 6 (main scan):  T_USER_PROJECTION                            */
/* ------------------------------------------------------------------ */

static int
testScanCteCteMatRootNonLeaf(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 16: scanCte as CTE materialization root non-leaf ... ");
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

  /* CTE 0: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1: GROUP BY cte_src.grp, SUM(cte_src.val) */
  NdbAggregator cte1Agg(srcTab);
  if (!cte1Agg.GroupBy("grp") ||
      !cte1Agg.LoadColumn("val", 0) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree (Test 2 pattern) */
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

  /* CTE 1 subtree: scanCte(0) → readTuple agg leaf */
  qb->beginCteSubtree(1);
  {
    /* No setAggregation here — scanCte is the data source for cte1's
     * materialization, not the agg leaf.  No MatchNonNull either —
     * scanCte at a subtree root has no parent row to inner-join
     * against. */
    const NdbQueryCteScanOperationDef *cte1ScanCteOp =
        qb->scanCte(0, 2, virtTab);
    if (cte1ScanCteOp == nullptr) {
      printf("FAILED (CTE 1 scanCte: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }

    const NdbQueryOperand *leafKey[] = {
      qb->linkedValue(cte1ScanCteOp, "grp"), nullptr
    };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cte1Agg);
    if (qb->readTuple(srcTab, leafKey, &leafOpts) == nullptr) {
      printf("FAILED (CTE 1 leaf readTuple: %s)\n",
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, srcTab, cte1Agg, /*depMask=*/(1ULL << 0)) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(1) → API */
  const NdbQueryCteScanOperationDef *mainScanCteOp =
      qb->scanCte(1, 2, virtTab);
  if (mainScanCteOp == nullptr) {
    printf("FAILED (main scanCte(1): %s)\n", qb->getNdbError().message);
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

  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
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

  /* Expected cte1 = {(1,30),(2,30)} */
  std::map<Int32, Int64> expected;
  expected[1] = 30;
  expected[2] = 30;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 grp = raGrp ? raGrp->int32_value() : -1;
    Int64 total = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", grp, (long long)total);
    auto it = expected.find(grp);
    if (it == expected.end()) {
      printf("FAILED (unexpected cte1.grp=%d)\n", grp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (total != it->second) {
      printf("FAILED (cte1 grp=%d expected total=%lld got %lld)\n",
             grp, (long long)it->second, (long long)total);
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

  if (rowCount != 2) {
    printf("FAILED (expected 2 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 17: readTuple as main root + lookupCte as child leaf           */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*     FROM cte_src GROUP BY grp)                                      */
/*   SELECT cte_src.pk, cte_src.grp, cte_src.val,                      */
/*          cte0.grp, cte0.total                                       */
/*   FROM cte_src                       -- via readTuple with pk=2     */
/*   JOIN cte0 ON cte0.grp = cte_src.grp                               */
/*                                                                     */
/* Why this test:                                                      */
/*   Reverse of Test 11.  The main query root is a plain readTuple     */
/*   (LQHKEYREQ) with a constant key, and the child/leaf is a          */
/*   lookupCte (CTE_LOOKUP_REQ) keyed by a linked value from the       */
/*   parent's result row.  This is a harder case than Test 11 because  */
/*   the root sends LQHKEYREQ through SCAN_TABREQ — the lookup node   */
/*   is consumed by lookup_build (not cte_build) via m_start_signal,   */
/*   and the CTE lookup child must fire from the parent_row path.      */
/*                                                                     */
/* Tree shape (built by NdbQueryBuilder):                              */
/*                                                                     */
/*   Node 0: CTE 0 subtree container (g_CteSubtreeOpInfo)              */
/*   Node 1: scanTable(cte_src) inside CTE 0  -- T_CTE_SCAN            */
/*           m_cteId=0                                                 */
/*   Node 2: readTuple(cte_src) inside CTE 0  -- T_AGGREGATE_LEAF      */
/*           m_cteId=0, parent=node 1                                  */
/*   Node 3: readTuple(cte_src, key=constInt(2)) -- main root          */
/*           m_cteId=RNIL, LQHKEYREQ, T_USER_PROJECTION,              */
/*           has child node 4                                          */
/*   Node 4: lookupCte(0, key=linked(node3,"grp")) -- child leaf       */
/*           m_cteId=RNIL, T_USER_PROJECTION, T_LEAF,                  */
/*           parent=node 3                                             */
/*                                                                     */
/* Expected execution:                                                 */
/*                                                                     */
/* Phase 0 (CTE 0 materialization):                                    */
/*   Same as Test 2/11: cte0 = {(1,30),(2,70),(3,50)}.                 */
/*                                                                     */
/* Main query (after execCTE_START_MAIN_REQ):                          */
/*   - Only rootFragId 0 proceeds with the main root lookup.           */
/*     Other fragments complete immediately.                           */
/*   - Node 3 fires LQHKEYREQ for cte_src pk=2.                        */
/*     Returns (pk=2, grp=1, val=20).                                  */
/*   - TRANSID_AI arrives at DBSPJ → startNextNodes fires child       */
/*     node 4 (lookupCte with key=linked(node3,"grp")=1).              */
/*   - Node 4: CTE_LOOKUP_REQ to DBLQH with key grp=1.                */
/*     DBLQH returns cte0 row (grp=1, total=30).                       */
/*   - Both rows flushed to API.                                       */
/*                                                                     */
/* Expected result row count: 1                                        */
/*   row[0]: cte_src.pk=2, cte_src.grp=1, cte_src.val=20,              */
/*           cte0.grp=1, cte0.total=30                                 */
/* ------------------------------------------------------------------ */

static int
testReadTupleRootWithCteLookupChild(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 17: readTuple root + lookupCte child leaf ... ");
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

  /* CTE 0: GROUP BY grp, SUM(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanTable + readTuple agg-leaf */
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

  /* Main query root: readTuple(cte_src) with constant key pk=2.
   * This is an LQHKEYREQ-based lookup through the SCAN_TABREQ path
   * (because isScanQuery() is true due to CTEs). */
  const NdbQueryOperand *rootKey[] = {
    qb->constValue(Int32(2)), nullptr
  };
  NdbQueryOptions rootOpts;
  rootOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryLookupOperationDef *rootReadOp =
      qb->readTuple(srcTab, rootKey, &rootOpts);
  if (rootReadOp == nullptr) {
    printf("FAILED (main readTuple: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Main child/leaf: lookupCte(0) keyed by linked(root,"grp").
   * The root's grp value (=1 for pk=2) drives the CTE lookup.
   * cte0 grp=1 → (grp=1, total=30). */
  const NdbQueryOperand *childCteKey[] = {
    qb->linkedValue(rootReadOp, "grp"), nullptr
  };
  NdbQueryOptions childOpts;
  childOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  const NdbQueryCteLookupOperationDef *childCteLookup =
      qb->lookupCte(0, 2, virtTab, childCteKey, &childOpts);
  if (childCteLookup == nullptr) {
    printf("FAILED (child lookupCte: %s)\n", qb->getNdbError().message);
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

  /* Wire projections: root readTuple (cte_src columns),
   * child lookupCte (cte0 columns). */
  const Uint32 rootOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 childOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *rootOp = query->getQueryOperation(rootOpNo);
  NdbQueryOperation *childOp = query->getQueryOperation(childOpNo);
  NdbRecAttr *raSrcPk = nullptr;
  NdbRecAttr *raSrcGrp = nullptr;
  NdbRecAttr *raSrcVal = nullptr;
  NdbRecAttr *raCteGrp = nullptr;
  NdbRecAttr *raCteTotal = nullptr;
  if (rootOp != nullptr) {
    raSrcPk = rootOp->getValue("pk");
    raSrcGrp = rootOp->getValue("grp");
    raSrcVal = rootOp->getValue("val");
  }
  if (childOp != nullptr) {
    raCteGrp = childOp->getValue("grp");
    raCteTotal = childOp->getValue("total");
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

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 srcPk = raSrcPk ? raSrcPk->int32_value() : -1;
    Int32 srcGrp = raSrcGrp ? raSrcGrp->int32_value() : -1;
    Int64 srcVal = raSrcVal ? raSrcVal->int64_value() : -1;
    Int32 cteGrp = raCteGrp ? raCteGrp->int32_value() : -1;
    Int64 cteTotal = raCteTotal ? raCteTotal->int64_value() : -1;
    V("  row: cte_src(pk=%d,grp=%d,val=%lld) cte0(grp=%d,total=%lld)\n",
      srcPk, srcGrp, (long long)srcVal, cteGrp, (long long)cteTotal);
    if (srcPk != 2 || srcGrp != 1 || srcVal != 20) {
      printf("FAILED (cte_src mismatch: pk=%d grp=%d val=%lld, "
             "expected 2/1/20)\n",
             srcPk, srcGrp, (long long)srcVal);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (cteGrp != 1 || cteTotal != 30) {
      printf("FAILED (cte0 mismatch: grp=%d total=%lld, expected 1/30)\n",
             cteGrp, (long long)cteTotal);
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

  if (rowCount != 1) {
    printf("FAILED (expected 1 row, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 18: scanIndex in CTE materialization subtree                    */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*                   FROM cte_src GROUP BY grp)                        */
/*   SELECT * FROM cte0;                                               */
/*                                                                     */
/* Why this test:                                                      */
/*   Verifies that scanIndex (ordered index scan) can replace          */
/*   scanTable inside a CTE subtree as the materialization scan.       */
/*   This is the foundation for the MIN/MAX index optimization         */
/*   where descending order + batch_size=1 yields O(fragments)         */
/*   instead of O(N) for MAX queries.                                  */
/*                                                                     */
/* Tree shape:                                                         */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanIndex(cte_src, idx_cte_src_val)  CTE materialization  */
/*   Node 2: readTuple(cte_src,                                       */
/*           key=linked(node1,"pk"),                                  */
/*           setAggregation(cte0Agg))           T_AGGREGATE_LEAF       */
/*   Node 3: scanCte(0)                          MAIN ROOT             */
/*                                                                     */
/* Expected:                                                           */
/*   cte0 = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)}       */
/*   3 rows from scanCte.                                              */
/* ------------------------------------------------------------------ */

static int
testScanIndexCteMaterialization(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 18: scanIndex in CTE materialization ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRTUAL_TABLE);
  dict->invalidateIndex("idx_cte_src_val", SRC_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRTUAL_TABLE);
  const NdbDictionary::Index *valIdx =
      dict->getIndex("idx_cte_src_val", SRC_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }
  if (valIdx == nullptr) {
    printf("FAILED (index lookup: %s)\n", dict->getNdbError().message);
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

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanIndex (ordered index on val) + readTuple leaf */
  qb->beginCteSubtree(0);
  {
    /* Use scanIndex instead of scanTable — no bounds (full scan),
     * no ordering (we just want to verify it works inside CTE subtree) */
    const NdbQueryIndexScanOperationDef *scan =
        qb->scanIndex(valIdx, srcTab, nullptr);
    if (scan == nullptr) {
      printf("FAILED (CTE 0 scanIndex: %s)\n", qb->getNdbError().message);
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

  /* Main query: scanCte(0) */
  const NdbQueryCteScanOperationDef *mainOp =
      qb->scanCte(0, 2, virtTab);
  if (mainOp == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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

  /* Wire user projection on the scanCte root */
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainQueryOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainQueryOp != nullptr) {
    raGrp = mainQueryOp->getValue("grp");
    raTotal = mainQueryOp->getValue("total");
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

  /* Expected cte0 values: grp→total */
  std::map<Int32, Int64> expected;
  expected[1] = 30;
  expected[2] = 70;
  expected[3] = 50;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 grp = raGrp ? raGrp->int32_value() : -1;
    Int64 total = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", grp, (long long)total);
    auto it = expected.find(grp);
    if (it == expected.end()) {
      printf("FAILED (unexpected grp=%d)\n", grp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (total != it->second) {
      printf("FAILED (grp=%d expected total=%lld got %lld)\n",
             grp, (long long)it->second, (long long)total);
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

  if (rowCount != 3) {
    printf("FAILED (expected 3 rows, got %u)\n", rowCount);
    return -1;
  }

  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 19: Scalar MAX(val) CTE with descending scanIndex + maxRows=1  */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT MAX(val) AS max_val FROM cte_src)            */
/*   SELECT * FROM cte0;                                               */
/*                                                                     */
/* Why this test:                                                      */
/*   Exercises the complete MIN/MAX index optimization:                */
/*   1. Scalar aggregate CTE (no GROUP BY) — tests cteScanEmitResults */
/*      handling of n_gb_cols==0 (m_agg_results path)                  */
/*   2. Descending ordered index scan (SFP_DESCENDING flag)            */
/*   3. maxRows=1 (close scan after first row per fragment)            */
/*                                                                     */
/* Tree shape:                                                         */
/*   Node 0: CTE 0 subtree container                                   */
/*   Node 1: scanIndex(cte_src, idx_cte_src_val, DESC, maxRows=1)      */
/*   Node 2: readTuple(cte_src, key=linked(node1,"pk"),               */
/*           setAggregation(MAX(val)))             T_AGGREGATE_LEAF    */
/*   Node 3: scanCte(0)                            MAIN ROOT           */
/*                                                                     */
/* Data: (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)                  */
/* Expected: MAX(val)=50, 1 result row.                                */
/* ------------------------------------------------------------------ */

static int
testMaxValWithDescScanIndex(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 19: Scalar MAX(val) CTE + DESC scanIndex + maxRows=1 ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(SCALAR_VIRTUAL_TABLE);
  dict->invalidateIndex("idx_cte_src_val", SRC_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *scalarVirtTab =
      dict->getTable(SCALAR_VIRTUAL_TABLE);
  const NdbDictionary::Index *valIdx =
      dict->getIndex("idx_cte_src_val", SRC_TABLE);
  if (srcTab == nullptr || scalarVirtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }
  if (valIdx == nullptr) {
    printf("FAILED (index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0 aggregation: MAX(val) — no GROUP BY (scalar aggregate) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Max(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* Build query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanIndex (DESC, maxRows=1) + readTuple leaf */
  qb->beginCteSubtree(0);
  {
    NdbQueryOptions scanOpts;
    scanOpts.setOrdering(NdbQueryOptions::ScanOrdering_descending);
    scanOpts.setMaxRows(1);

    const NdbQueryIndexScanOperationDef *scan =
        qb->scanIndex(valIdx, srcTab, nullptr, &scanOpts);
    if (scan == nullptr) {
      printf("FAILED (CTE 0 scanIndex: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *key[] = {
      qb->linkedValue(scan, "pk"), nullptr
    };
    NdbQueryOptions leafOpts;
    leafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
    leafOpts.setAggregation(cte0Agg);
    if (qb->readTuple(srcTab, key, &leafOpts) == nullptr) {
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

  /* Main query: scanCte(0) — returns 1 row with MAX(val).
   * scalarVirtTab has (result BIGINT PK). numResultCols=1 matches
   * the single aggregate result. */
  const NdbQueryCteScanOperationDef *mainOp =
      qb->scanCte(0, 1, scalarVirtTab);
  if (mainOp == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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

  /* Wire user projection — read the single aggregate result column */
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainQueryOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raMaxVal = nullptr;
  if (mainQueryOp != nullptr) {
    raMaxVal = mainQueryOp->getValue("result");
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

  Uint32 rowCount = 0;
  Int64 maxVal = -1;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    maxVal = raMaxVal ? raMaxVal->int64_value() : -1;
    V("  row: MAX(val)=%lld\n", (long long)maxVal);
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

  if (rowCount != 1) {
    printf("FAILED (expected 1 row, got %u)\n", rowCount);
    return -1;
  }
  if (maxVal != 50) {
    printf("FAILED (expected MAX(val)=50, got %lld)\n", (long long)maxVal);
    return -1;
  }

  printf("OK (MAX(val)=%lld)\n", (long long)maxVal);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 20: Cross-join of two scalar CTEs via lookupCte                */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte_max AS (SELECT MAX(val) AS result FROM cte_src),         */
/*        cte_min AS (SELECT MIN(val) AS result FROM cte_src)          */
/*   SELECT cte_max.result, cte_min.result                             */
/*   FROM cte_max, cte_min;                                            */
/*                                                                     */
/* Why this test:                                                      */
/*   Cross-join of two single-row scalar CTEs, the pattern needed for  */
/*   the Hopsworks watermark query. Uses scanCte(0) as main root and   */
/*   lookupCte(1, dummy_key) as child. The lookupCte on a scalar CTE   */
/*   ignores the key and returns m_agg_results directly.               */
/*                                                                     */
/* Tree shape:                                                         */
/*   Node 0: CteSubtree(0)                                             */
/*   Node 1: scanTable(cte_src)          CTE 0 materialization          */
/*   Node 2: readTuple(cte_src, pk)      agg leaf → MAX(val)           */
/*   Node 3: CteSubtree(1)                                             */
/*   Node 4: scanTable(cte_src)          CTE 1 materialization          */
/*   Node 5: readTuple(cte_src, pk)      agg leaf → MIN(val)           */
/*   Node 6: scanCte(0)                  MAIN ROOT (1 row: MAX=50)     */
/*   Node 7: lookupCte(1, dummy_key=0)   CHILD (1 row: MIN=10)        */
/*                                                                     */
/* Expected: 1 row with MAX=50, MIN=10.                                */
/* ------------------------------------------------------------------ */

static int
testCrossJoinTwoScalarCtes(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 20: Cross-join of two scalar CTEs ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(SCALAR_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *scalarVirtTab =
      dict->getTable(SCALAR_VIRTUAL_TABLE);
  if (srcTab == nullptr || scalarVirtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0: MAX(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Max(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1: MIN(val) */
  NdbAggregator cte1Agg(srcTab);
  if (!cte1Agg.LoadColumn("val", 0) ||
      !cte1Agg.Min(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanTable + readTuple leaf → MAX(val) */
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

  /* CTE 1 subtree: scanTable + readTuple leaf → MIN(val) */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 1 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *key[] = {
      qb->linkedValue(scan, "pk"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte1Agg);
    if (qb->readTuple(srcTab, key, &opts) == nullptr) {
      printf("FAILED (CTE 1 leaf: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, srcTab, cte1Agg, /*depMask=*/0) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(0) as root + lookupCte(1, dummy_key) as child.
   * For scalar CTEs (no GROUP BY), the lookup key is a dummy constant;
   * DBLQH ignores the key when n_gb_cols==0 and returns m_agg_results. */
  const NdbQueryCteScanOperationDef *mainRoot =
      qb->scanCte(0, 1, scalarVirtTab);
  if (mainRoot == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Dummy constant key for the scalar CTE lookup.
   * scalarVirtTab has (result BIGINT PK), so we need a BIGINT const.
   * setParent establishes the cross-join dependency (no linked value). */
  const NdbQueryOperand *dummyKey[] = {
    qb->constValue((Int64)0), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setParent(mainRoot);
  const NdbQueryCteLookupOperationDef *childLookup =
      qb->lookupCte(1, 1, scalarVirtTab, dummyKey, &cteLookupOpts);
  if (childLookup == nullptr) {
    printf("FAILED (lookupCte 1: %s)\n", qb->getNdbError().message);
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

  /* Wire projections: scanCte(0) → MAX, lookupCte(1) → MIN */
  NdbQueryOperation *rootOp = query->getQueryOperation(
      queryDef->getNoOfOperations() - 2);
  NdbQueryOperation *childOp = query->getQueryOperation(
      queryDef->getNoOfOperations() - 1);
  NdbRecAttr *raMax = rootOp ? rootOp->getValue("result") : nullptr;
  NdbRecAttr *raMin = childOp ? childOp->getValue("result") : nullptr;

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  Int64 maxVal = -1, minVal = -1;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    maxVal = raMax ? raMax->int64_value() : -1;
    minVal = raMin ? raMin->int64_value() : -1;
    V("  row: MAX=%lld MIN=%lld\n", (long long)maxVal, (long long)minVal);
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

  if (rowCount != 1) {
    printf("FAILED (expected 1 row, got %u)\n", rowCount);
    return -1;
  }
  if (maxVal != 50 || minVal != 10) {
    printf("FAILED (expected MAX=50 MIN=10, got MAX=%lld MIN=%lld)\n",
           (long long)maxVal, (long long)minVal);
    return -1;
  }

  printf("OK (MAX=%lld, MIN=%lld)\n", (long long)maxVal, (long long)minVal);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 21: GREATEST(MAX(val), MIN(val)) via CASE in aggregation       */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte_max AS (SELECT MAX(val) AS result FROM cte_src),         */
/*        cte_min AS (SELECT MIN(val) AS result FROM cte_src)          */
/*   SELECT GREATEST(cte_max.result, cte_min.result) AS watermark      */
/*   FROM cte_max, cte_min;                                            */
/*                                                                     */
/* Why this test:                                                      */
/*   End-to-end test of the Hopsworks watermark query pattern.         */
/*   GREATEST is expressed as CASE WHEN a >= b THEN a ELSE b END       */
/*   using an embedded interpreter comparison plus Mov.                 */
/*   The lookupCte child is the aggregate leaf whose program computes  */
/*   GREATEST of the parent's CTE0 result and its own CTE1 result.    */
/*                                                                     */
/* Tree shape:                                                         */
/*   Nodes 0-2: CTE 0 subtree (scanTable + agg leaf → MAX(val))        */
/*   Nodes 3-5: CTE 1 subtree (scanTable + agg leaf → MIN(val))        */
/*   Node 6: scanCte(0) — MAIN ROOT (1 row: MAX=50)                   */
/*   Node 7: lookupCte(1, dummy_key, parent=root,                     */
/*           setAggregation(GREATEST via CASE))  — AGG LEAF            */
/*                                                                     */
/* Aggregation program on node 7:                                      */
/*   LoadLinkedColumn(pos=0, reg0, col)       // reg0 = CTE0 MAX=50   */
/*   LoadLinkedColumn(pos=1, reg1, col)       // reg1 = CTE1 MIN=10   */
/*   EmbeddedInterp(...)                      // skip Mov if reg0>=reg1*/
/*   Mov(reg0, reg1)                          // reg0 = reg1           */
/*   Max(0, reg0)                             // agg[0] = reg0         */
/*                                                                     */
/* Linked positions: pos 0 = parent linked col (forwarded via          */
/*   AttrInfo subroutine section in CTE_LOOKUP_REQ).                   */
/*   pos 1 = CTE1 result col (appended by cteLookupAggFeed).           */
/*                                                                     */
/* Data: (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)                  */
/* Expected: GREATEST(50, 10) = 50, returned via aggregator.           */
/* ------------------------------------------------------------------ */

static int
testGreatestViaCaseAgg(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 21: GREATEST(MAX, MIN) via CASE in aggregation ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(SCALAR_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *scalarVirtTab =
      dict->getTable(SCALAR_VIRTUAL_TABLE);
  if (srcTab == nullptr || scalarVirtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0: MAX(val) */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Max(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1: MIN(val) */
  NdbAggregator cte1Agg(srcTab);
  if (!cte1Agg.LoadColumn("val", 0) ||
      !cte1Agg.Min(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg: %s)\n", cte1Agg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregation: GREATEST(parent.result, this.result)
   * = CASE WHEN reg0 >= reg1 THEN reg0 ELSE reg1 END
   * Expressed as:
   *   LoadLinkedColumn(0, reg0, col)
   *   LoadLinkedColumn(1, reg1, col)
   *   EmbeddedInterp(...)          // skip Mov if reg0 >= reg1
   *   Mov(reg0, reg1)              // reg0 = reg1
   *   Max(0, reg0)                 // store GREATEST in agg[0]
   */
  const NdbDictionary::Column *virtResultCol =
      scalarVirtTab->getColumn("result");
  NdbAggregator mainAgg(scalarVirtTab);
  if (!mainAgg.LoadLinkedColumn(0, 0, virtResultCol) ||
      !mainAgg.LoadLinkedColumn(1, 1, virtResultCol) ||
      !mainAgg.EmbeddedInterp(9) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::ReadAggRegIntoReg(0, 1)) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::ReadAggRegIntoReg(1, 2)) ||
      !mainAgg.EmitEmbeddedWord(
          Interpreter::Branch(Interpreter::BRANCH_GE_REG_REG,
                              /*Reg1=*/2, /*Reg2=*/1) | (4 << 16)) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::LoadConst16(3, 0)) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::WriteInterpreterOutput(3, 0)) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::ExitOK()) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::LoadConst16(3, 1)) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::WriteInterpreterOutput(3, 0)) ||
      !mainAgg.EmitEmbeddedWord(Interpreter::ExitOK()) ||
      !mainAgg.Mov(0, 1) ||
      !mainAgg.Max(0, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: scanTable + readTuple leaf → MAX(val) */
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

  /* CTE 1 subtree: scanTable + readTuple leaf → MIN(val) */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      printf("FAILED (CTE 1 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
    const NdbQueryOperand *key[] = {
      qb->linkedValue(scan, "pk"), nullptr
    };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte1Agg);
    if (qb->readTuple(srcTab, key, &opts) == nullptr) {
      printf("FAILED (CTE 1 leaf: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, srcTab, cte1Agg, /*depMask=*/0) != 0) {
    printf("FAILED (defineCte 1)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(0) root + lookupCte(1) aggregate leaf.
   * The lookupCte carries the GREATEST aggregation program and
   * a linked projection to access the parent's CTE0 result. */
  const NdbQueryCteScanOperationDef *mainRoot =
      qb->scanCte(0, 1, scalarVirtTab);
  if (mainRoot == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Link parent's "result" column so GREATEST can access CTE0 value */
  const NdbLinkedOperand *linkedParentResult =
      qb->linkedValue(mainRoot, "result");
  if (linkedParentResult == nullptr) {
    printf("FAILED (linkedValue: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *dummyKey[] = {
    qb->constValue((Int64)0), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setParent(mainRoot);
  cteLookupOpts.setAggregation(mainAgg);
  cteLookupOpts.addLinkedProjection(linkedParentResult);
  const NdbQueryCteLookupOperationDef *childLookup =
      qb->lookupCte(1, 1, scalarVirtTab, dummyKey, &cteLookupOpts);
  if (childLookup == nullptr) {
    printf("FAILED (lookupCte 1: %s)\n", qb->getNdbError().message);
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

  /* For aggregate queries, result comes from the aggregator, not getValue */
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans err %d: %s, query err %d: %s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Consume scan results */
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    /* nothing — aggregate results are fetched after scan completes */
  }
  if (outcome == NdbQuery::NextResult_error) {
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (nextResult err %d: %s)\n", qErr.code, qErr.message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Fetch aggregation result */
  NdbAggregator *agg = query->getAggregator();
  if (agg == nullptr) {
    printf("FAILED (getAggregator returned null)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  agg->PrepareResults();
  NdbAggregator::ResultRecord rr = agg->FetchResultRecord();
  if (rr.end()) {
    printf("FAILED (no aggregation results)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result res = rr.FetchAggregationResult();
  Int64 watermark = res.is_null() ? -1 : res.data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  if (watermark != 50) {
    printf("FAILED (expected GREATEST=50, got %lld)\n", (long long)watermark);
    return -1;
  }

  printf("OK (GREATEST=%lld)\n", (long long)watermark);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 22: scanCte parent + scanIndex child with linked bound         */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*                 FROM cte_src GROUP BY grp)                          */
/*   SELECT COUNT(t.pk), SUM(t.pk), SUM(t.val),                        */
/*          COUNT(cte0.grp), SUM(cte0.grp)                              */
/*   FROM cte0                                                         */
/*   JOIN cte_src AS t ON t.grp = cte0.grp;                            */
/*                                                                     */
/* Why this test:                                                      */
/*   Phase N.1 diagnostic for scanCte as parent of an ordered-index    */
/*   child.  The child is scanIndex(idx_cte_src_grp) and its equality  */
/*   bound is linkedValue(scanCte,"grp").  The aggregate reads both    */
/*   child-table columns and the linked CTE parent column so the result*/
/*   distinguishes child row production from linked-value delivery.    */
/* ------------------------------------------------------------------ */

static int
testScanCteParentScanIndexChild(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 22: scanCte parent + scanIndex child ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRTUAL_TABLE);
  dict->invalidateIndex("idx_cte_src_grp", SRC_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRTUAL_TABLE);
  const NdbDictionary::Index *grpIdx =
      dict->getIndex("idx_cte_src_grp", SRC_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }
  if (grpIdx == nullptr) {
    printf("FAILED (index lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *grpCol = virtTab->getColumn("grp");
  if (grpCol == nullptr) {
    printf("FAILED (column lookup: grp)\n");
    return -1;
  }

  /* CTE 0: GROUP BY grp, SUM(val). */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg: %s)\n", cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* Aggregate on the scanIndex child leaf.  Registers:
   *   r0 = t.pk, r1 = t.val, r2 = linked cte0.grp.
   */
  NdbAggregator mainAgg(srcTab);
  if (!mainAgg.LoadColumn("pk", 0) ||
      !mainAgg.LoadColumn("val", 1) ||
      !mainAgg.LoadLinkedColumn(0, 2, grpCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Sum(2, 1) ||
      !mainAgg.Count(3, 2) ||
      !mainAgg.Sum(4, 2) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

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
    opts.setAggregation(cte0Agg);
    if (qb->readTuple(srcTab, key, &opts) == nullptr) {
      printf("FAILED (CTE leaf: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(0, srcTab, cte0Agg, /*depMask=*/0) != 0) {
    printf("FAILED (defineCte 0: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryCteScanOperationDef *mainScanCte =
      qb->scanCte(0, 2, virtTab);
  if (mainScanCte == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbLinkedOperand *linkedGrp = qb->linkedValue(mainScanCte, "grp");
  if (linkedGrp == nullptr) {
    printf("FAILED (linkedValue grp: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *childBound[] = {
    linkedGrp, nullptr
  };
  NdbQueryIndexBound grpBound(childBound);

  NdbQueryOptions childOpts;
  childOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  childOpts.setAggregation(mainAgg);
  childOpts.addLinkedProjection(linkedGrp);

  const NdbQueryIndexScanOperationDef *childScan =
      qb->scanIndex(grpIdx, srcTab, &grpBound, &childOpts);
  if (childScan == nullptr) {
    printf("FAILED (child scanIndex: %s)\n", qb->getNdbError().message);
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

  NdbAggregator *agg = query->getAggregator();
  if (agg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  agg->PrepareResults();
  NdbAggregator::ResultRecord rec = agg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator::Result countPkRes = rec.FetchAggregationResult();
  NdbAggregator::Result sumPkRes = rec.FetchAggregationResult();
  NdbAggregator::Result sumValRes = rec.FetchAggregationResult();
  NdbAggregator::Result countLinkedRes = rec.FetchAggregationResult();
  NdbAggregator::Result sumLinkedRes = rec.FetchAggregationResult();

  const bool anyNull =
      countPkRes.is_null() || sumPkRes.is_null() || sumValRes.is_null() ||
      countLinkedRes.is_null() || sumLinkedRes.is_null();
  const Int64 countPk = countPkRes.is_null() ? -1 : countPkRes.data_int64();
  const Int64 sumPk = sumPkRes.is_null() ? -1 : sumPkRes.data_int64();
  const Int64 sumVal = sumValRes.is_null() ? -1 : sumValRes.data_int64();
  const Int64 countLinked =
      countLinkedRes.is_null() ? -1 : countLinkedRes.data_int64();
  const Int64 sumLinked =
      sumLinkedRes.is_null() ? -1 : sumLinkedRes.data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  V("  Result: countPk=%lld sumPk=%lld sumVal=%lld "
    "countLinked=%lld sumLinked=%lld null=%d\n",
    (long long)countPk, (long long)sumPk, (long long)sumVal,
    (long long)countLinked, (long long)sumLinked, anyNull ? 1 : 0);

  if (!anyNull && countPk == 5 && sumPk == 15 && sumVal == 150 &&
      countLinked == 5 && sumLinked == 9) {
    printf("OK (rows=%lld, sumPk=%lld, sumVal=%lld, sumLinked=%lld)\n",
           (long long)countPk, (long long)sumPk, (long long)sumVal,
           (long long)sumLinked);
    return 0;
  }

  printf("FAILED (expected rows=5 sumPk=15 sumVal=150 countLinked=5 "
         "sumLinked=9, got rows=%lld sumPk=%lld sumVal=%lld "
         "countLinked=%lld sumLinked=%lld null=%d)\n",
         (long long)countPk, (long long)sumPk, (long long)sumVal,
         (long long)countLinked, (long long)sumLinked, anyNull ? 1 : 0);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Tests 23-26: single-row CTE materialization                         */
/* (cte_single_row_kernel_plan.md)                                     */
/*                                                                     */
/* A single-row CTE ships a zero-aggregate projection program: every   */
/* projected column is a GROUP BY column (SetSingleRowMode() lets      */
/* Finalize() accept n_agg_results == 0) and defineCte carries         */
/* QN_CteSubtreeNode::CTE_SINGLE_ROW.  The kernel stores the row as a  */
/* key-only group record on the node that found it and the             */
/* redistribute step ships it to the constant DBTC-node owner.         */
/* ------------------------------------------------------------------ */

/* Shared frame for Tests 23/24: single-row CTE over `srcName`
 * consumed by a plain scanCte root (Test 8 consumer shape).  On a
 * row, verifies (a, b) == (42, 4200).  Returns 0 with *rowsOut set,
 * -1 on failure (message already printed). */
static int
runSingleRowCteScan(Ndb *ndb, const char *srcName, Uint32 *rowsOut)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(srcName);
  dict->invalidateTable(SROW_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(srcName);
  const NdbDictionary::Table *virtTab = dict->getTable(SROW_VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* Single-row projection program: GROUP BY every projected column,
   * zero aggregate slots. */
  NdbAggregator srowAgg(srcTab);
  srowAgg.SetSingleRowMode();
  if (!srowAgg.GroupBy("a") ||
      !srowAgg.GroupBy("b") ||
      !srowAgg.Finalize()) {
    printf("FAILED (srowAgg: %s)\n", srowAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }

  /* CTE 0 subtree: single-op body — scanTable with the projection
   * program attached (the RonSQL arm-A shape). */
  qb->beginCteSubtree(0);
  {
    NdbQueryOptions bodyOpts;
    bodyOpts.setAggregation(srowAgg);
    if (qb->scanTable(srcTab, &bodyOpts) == nullptr) {
      printf("FAILED (CTE 0 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();

  if (qb->defineCte(0, srcTab, srowAgg, /*depMask=*/0,
                    QN_CteSubtreeNode::CTE_SINGLE_ROW) != 0) {
    printf("FAILED (defineCte 0)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scanCte(0) as root, no main aggregation. */
  const NdbQueryCteScanOperationDef *mainScanCteOp =
      qb->scanCte(0, 2, virtTab);
  if (mainScanCteOp == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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

  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raA = nullptr;
  NdbRecAttr *raB = nullptr;
  if (mainOp != nullptr) {
    raA = mainOp->getValue("a");
    raB = mainOp->getValue("b");
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

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 a = raA ? raA->int32_value() : -1;
    Int64 b = raB ? raB->int64_value() : -1;
    V("  row: a=%d b=%lld\n", a, (long long)b);
    if (a != 42 || b != 4200) {
      printf("FAILED (expected (42,4200), got (%d,%lld))\n",
             a, (long long)b);
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
  *rowsOut = rowCount;
  return 0;
}

/* Test 23: the one-row body materializes and scanCte emits exactly
 * that row — feed, key-only group record, owner shipping, CTE_READY
 * barrier and the scan emit all with zero aggregate slots. */
static int
testSingleRowCteScanRoot(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 23: single-row CTE, scanCte root ... ");
  fflush(stdout);
  Uint32 rows = 0;
  if (runSingleRowCteScan(ndb, SROW_SRC_TABLE, &rows) != 0) return -1;
  if (rows != 1) {
    printf("FAILED (expected 1 row, got %u)\n", rows);
    return -1;
  }
  printf("OK (1 row)\n");
  return 0;
}

/* Test 24: an EMPTY body materializes an empty single-row CTE — the
 * query completes cleanly with zero rows (the future CTE_LOOKUP miss
 * semantics depend on the empty state being representable). */
static int
testSingleRowCteEmpty(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 24: single-row CTE, empty body ... ");
  fflush(stdout);
  Uint32 rows = 0;
  if (runSingleRowCteScan(ndb, SROW_EMPTY_TABLE, &rows) != 0) return -1;
  if (rows != 0) {
    printf("FAILED (expected 0 rows, got %u)\n", rows);
    return -1;
  }
  printf("OK (0 rows)\n");
  return 0;
}

/* Test 25: single-row contract violation — the body (a scan of the
 * 5-row cte_src projected as (grp, val)) materializes 5 distinct
 * rows.  The kernel must FAIL the query cleanly
 * (ZCTE_SINGLE_ROW_VIOLATION via JOIN_AGG_COMPLETE_REF), never crash
 * and never return rows. */
static int
testSingleRowCteViolation(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 25: single-row CTE violation fails cleanly ... ");
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

  NdbAggregator srowAgg(srcTab);
  srowAgg.SetSingleRowMode();
  if (!srowAgg.GroupBy("grp") ||
      !srowAgg.GroupBy("val") ||
      !srowAgg.Finalize()) {
    printf("FAILED (srowAgg: %s)\n", srowAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }
  qb->beginCteSubtree(0);
  {
    NdbQueryOptions bodyOpts;
    bodyOpts.setAggregation(srowAgg);
    if (qb->scanTable(srcTab, &bodyOpts) == nullptr) {
      printf("FAILED (CTE 0 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, srowAgg, /*depMask=*/0,
                    QN_CteSubtreeNode::CTE_SINGLE_ROW) != 0) {
    printf("FAILED (defineCte 0)\n");
    qb->destroy();
    return -1;
  }
  if (qb->scanCte(0, 2, virtTab) == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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
  NdbQueryOperation *mainOp =
      query->getQueryOperation(queryDef->getNoOfOperations() - 1);
  if (mainOp != nullptr) {
    (void)mainOp->getValue("grp");
    (void)mainOp->getValue("total");
  }

  bool sawError = false;
  Uint32 rowCount = 0;
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    sawError = true;
    V("\n  execute failed as expected: trans err %d: %s\n",
      trans->getNdbError().code, trans->getNdbError().message);
  } else {
    NdbQuery::NextResultOutcome outcome;
    while ((outcome = query->nextResult(true)) ==
           NdbQuery::NextResult_gotRow) {
      rowCount++;
    }
    if (outcome == NdbQuery::NextResult_error) {
      sawError = true;
      V("\n  nextResult failed as expected: err %d: %s\n",
        query->getNdbError().code, query->getNdbError().message);
    }
  }
  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != 0) {
    printf("FAILED (violating query returned %u rows)\n", rowCount);
    return -1;
  }
  if (!sawError) {
    printf("FAILED (violating query completed without error)\n");
    return -1;
  }
  printf("OK (clean error, 0 rows)\n");
  return 0;
}

/* Test 26: single-row CTE feeding a MAIN aggregator through the
 * CTE_SCAN agg feed (Test 15 consumer shape) — exercises
 * buildCteLinkedBuffer with zero aggregate slots: the linked buffer
 * carries only the GROUP BY key columns at positions 0..1. */
static int
testSingleRowCteFeedsAgg(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 26: single-row CTE feeds main aggregation ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SROW_SRC_TABLE);
  dict->invalidateTable(SROW_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SROW_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(SROW_VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }
  const NdbDictionary::Column *bCol = virtTab->getColumn("b");
  if (bCol == nullptr) {
    printf("FAILED (column lookup: b)\n");
    return -1;
  }

  NdbAggregator srowAgg(srcTab);
  srowAgg.SetSingleRowMode();
  if (!srowAgg.GroupBy("a") ||
      !srowAgg.GroupBy("b") ||
      !srowAgg.Finalize()) {
    printf("FAILED (srowAgg: %s)\n", srowAgg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregator: COUNT(*), SUM(r.b).  The CTE feed buffer is
   * [GROUP BY keys], so position 1 = column b. */
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, bCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }
  qb->beginCteSubtree(0);
  {
    NdbQueryOptions bodyOpts;
    bodyOpts.setAggregation(srowAgg);
    if (qb->scanTable(srcTab, &bodyOpts) == nullptr) {
      printf("FAILED (CTE 0 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, srowAgg, /*depMask=*/0,
                    QN_CteSubtreeNode::CTE_SINGLE_ROW) != 0) {
    printf("FAILED (defineCte 0)\n");
    qb->destroy();
    return -1;
  }

  NdbQueryOptions mainScanOpts;
  mainScanOpts.setAggregation(mainAgg);
  if (qb->scanCte(0, 2, virtTab, &mainScanOpts) == nullptr) {
    printf("FAILED (main scanCte: %s)\n", qb->getNdbError().message);
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

  if (count == 1 && sum == 4200) {
    printf("OK (COUNT=1, SUM=4200)\n");
    return 0;
  }
  printf("FAILED (expected COUNT=1 SUM=4200, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Tests 27-31: single-row CTE subset-key CTE_LOOKUP                   */
/* (cte_single_row_kernel_plan.md, commit 2)                           */
/*                                                                     */
/* A lookupCte against a single-row CTE may bind ANY SUBSET of the     */
/* projected columns via NdbQueryOptions::setCteKeyColumns —           */
/* including NONE (a pure existence probe).  DBSPJ routes the probe    */
/* to the constant DBTC-node owner and stamps each key                 */
/* AttributeHeader with the TRUE projected-column position; DBLQH      */
/* compares against the stored row and MISSES on an empty state.       */
/* ------------------------------------------------------------------ */

/* Shared frame: single-row CTE over `srcName` probed by a lookupCte
 * child under a scanTable(cte_srow_src) parent (1 parent row).
 * keyPositions/numKeys select the bound subset; keyCols names the
 * PARENT columns supplying the values (same order).  numKeys == 0 is
 * the zero-key existence probe (setParent attaches the child).
 * Expects `expectRows` result rows; each must read (42, 4200). */
static int
runSingleRowCteLookup(Ndb *ndb, const char *srcName,
                      const Uint32 *keyPositions, Uint32 numKeys,
                      const char *const *keyCols,
                      Uint32 expectRows, const char *testName)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(srcName);
  dict->invalidateTable(SROW_SRC_TABLE);
  dict->invalidateTable(SROW_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(srcName);
  const NdbDictionary::Table *parentTab = dict->getTable(SROW_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(SROW_VIRTUAL_TABLE);
  if (srcTab == nullptr || parentTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator srowAgg(srcTab);
  srowAgg.SetSingleRowMode();
  if (!srowAgg.GroupBy("a") ||
      !srowAgg.GroupBy("b") ||
      !srowAgg.Finalize()) {
    printf("FAILED (srowAgg: %s)\n", srowAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    printf("FAILED (create)\n");
    return -1;
  }
  qb->beginCteSubtree(0);
  {
    NdbQueryOptions bodyOpts;
    bodyOpts.setAggregation(srowAgg);
    if (qb->scanTable(srcTab, &bodyOpts) == nullptr) {
      printf("FAILED (CTE 0 scan: %s)\n", qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, srowAgg, /*depMask=*/0,
                    QN_CteSubtreeNode::CTE_SINGLE_ROW) != 0) {
    printf("FAILED (defineCte 0)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: 1-row parent scan + lookupCte child */
  const NdbQueryTableScanOperationDef *parentScan =
      qb->scanTable(parentTab);
  if (parentScan == nullptr) {
    printf("FAILED (parent scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryOperand *keys[QN_CteLookupNode::MaxKeyPositions + 1];
  for (Uint32 i = 0; i < numKeys; i++) {
    keys[i] = qb->linkedValue(parentScan, keyCols[i]);
    if (keys[i] == nullptr) {
      printf("FAILED (linkedValue %s: %s)\n", keyCols[i],
             qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  keys[numKeys] = nullptr;

  NdbQueryOptions lookupOpts;
  lookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  if (lookupOpts.setCteKeyColumns(keyPositions, numKeys) != 0) {
    printf("FAILED (setCteKeyColumns)\n");
    qb->destroy();
    return -1;
  }
  if (numKeys == 0) {
    /* Zero-key probe carries no linked operand — attach the child to
     * its parent explicitly (the ScalarDummy setParent precedent). */
    lookupOpts.setParent(parentScan);
  }
  const NdbQueryCteLookupOperationDef *lookupOp =
      qb->lookupCte(0, 2, virtTab, keys, &lookupOpts);
  if (lookupOp == nullptr) {
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

  /* Project on BOTH ops (the Test 2 convention): a scan op with no
   * getValue produces no TRANSID_AI while the completed-ops accounting
   * still announces its rows, so the API waits forever for them —
   * the first run of these tests hung exactly there. */
  const Uint32 parentOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 lookupOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *parentOp = query->getQueryOperation(parentOpNo);
  NdbQueryOperation *lkOp = query->getQueryOperation(lookupOpNo);
  NdbRecAttr *raPk = nullptr;
  NdbRecAttr *raA = nullptr;
  NdbRecAttr *raB = nullptr;
  if (parentOp != nullptr) {
    raPk = parentOp->getValue("pk");
  }
  if (lkOp != nullptr) {
    raA = lkOp->getValue("a");
    raB = lkOp->getValue("b");
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

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    /* MatchNonNull drops unmatched parents, so any row means a HIT
     * and must carry the materialized values. */
    if (raA == nullptr || raB == nullptr ||
        raA->isNULL() != 0 || raB->isNULL() != 0) {
      printf("FAILED (%s: NULL result on a hit)\n", testName);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    Int32 a = raA->int32_value();
    Int64 b = raB->int64_value();
    V("  row: pk=%d a=%d b=%lld\n",
      raPk ? raPk->int32_value() : -1, a, (long long)b);
    if (a != 42 || b != 4200) {
      printf("FAILED (%s: expected (42,4200), got (%d,%lld))\n",
             testName, a, (long long)b);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    rowCount++;
  }
  if (outcome == NdbQuery::NextResult_error) {
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (%s: nextResult err %d: %s)\n", testName,
           qErr.code, qErr.message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != expectRows) {
    printf("FAILED (%s: expected %u rows, got %u)\n", testName,
           expectRows, rowCount);
    return -1;
  }
  return 0;
}

/* Test 27: subset key binding ONLY position 1 (column b).  The old
 * sequential attrId normalization would have compared b's value
 * against column a and missed — the position stamping is what makes
 * this hit. */
static int
testSingleRowCteSubsetKeyHit(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 27: single-row CTE subset-key hit (position 1) ... ");
  fflush(stdout);
  static const Uint32 positions[] = { 1 };
  static const char *const cols[] = { "b" };
  if (runSingleRowCteLookup(ndb, SROW_SRC_TABLE, positions, 1, cols,
                            1, "t27") != 0) return -1;
  printf("OK (1 row)\n");
  return 0;
}

/* Test 28: subset key binding position 0 (column a) to the parent's
 * pk (7 != 42) — value mismatch => MISS, INNER drops the parent. */
static int
testSingleRowCteSubsetKeyMiss(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 28: single-row CTE subset-key miss ... ");
  fflush(stdout);
  static const Uint32 positions[] = { 0 };
  static const char *const cols[] = { "pk" };
  if (runSingleRowCteLookup(ndb, SROW_SRC_TABLE, positions, 1, cols,
                            0, "t28") != 0) return -1;
  printf("OK (0 rows)\n");
  return 0;
}

/* Test 29: zero-key existence probe — HIT iff the row exists. */
static int
testSingleRowCteExistenceProbe(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 29: single-row CTE zero-key existence probe ... ");
  fflush(stdout);
  if (runSingleRowCteLookup(ndb, SROW_SRC_TABLE, nullptr, 0, nullptr,
                            1, "t29") != 0) return -1;
  printf("OK (1 row)\n");
  return 0;
}

/* Test 30: zero-key probe against an EMPTY body — the empty-CTE MISS
 * drops the (existing) parent row: exact MySQL cross-join-with-empty
 * semantics, no guard tricks. */
static int
testSingleRowCteExistenceProbeEmpty(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 30: single-row CTE existence probe, empty body ... ");
  fflush(stdout);
  if (runSingleRowCteLookup(ndb, SROW_EMPTY_TABLE, nullptr, 0, nullptr,
                            0, "t30") != 0) return -1;
  printf("OK (0 rows)\n");
  return 0;
}

/* Test 31: both columns bound ({0,1}) — multi-entry stamping + typed
 * compare on INT and BIGINT. */
static int
testSingleRowCteFullSubsetHit(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 31: single-row CTE two-column subset hit ... ");
  fflush(stdout);
  static const Uint32 positions[] = { 0, 1 };
  static const char *const cols[] = { "a", "b" };
  if (runSingleRowCteLookup(ndb, SROW_SRC_TABLE, positions, 2, cols,
                            1, "t31") != 0) return -1;
  printf("OK (1 row)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

struct TestEntry {
  int number;
  int (*fn)(Ndb *, MYSQL *);
};

static const TestEntry g_tests[] = {
    { 1,  testCteWithStandardMain },
    { 2,  testCteLookupMain },
    { 3,  testCteLookupAggLeaf },
    { 4,  testCteWithScanFilter },
    { 5,  testCteToCteLookup },
    { 6,  testCteToCteLookupWithMainAgg },
    { 7,  testCteThreeLevelChain },
    { 8,  testScanCteMainRoot },
    { 9,  testScanCteWithJoin },
    { 10, testCteScanFeedsAgg },
    { 11, testLookupCteMainRootWithChild },
    { 12, testLookupCteCteMatRoot },
    { 13, testLookupCteMainInternal },
    { 14, testLookupCteCteMatInternal },
    { 15, testScanCteMainAggLeaf },
    { 16, testScanCteCteMatRootNonLeaf },
    { 17, testReadTupleRootWithCteLookupChild },
    { 18, testScanIndexCteMaterialization },
    { 19, testMaxValWithDescScanIndex },
    { 20, testCrossJoinTwoScalarCtes },
    { 21, testGreatestViaCaseAgg },
    { 22, testScanCteParentScanIndexChild },
    { 23, testSingleRowCteScanRoot },
    { 24, testSingleRowCteEmpty },
    { 25, testSingleRowCteViolation },
    { 26, testSingleRowCteFeedsAgg },
    { 27, testSingleRowCteSubsetKeyHit },
    { 28, testSingleRowCteSubsetKeyMiss },
    { 29, testSingleRowCteExistenceProbe },
    { 30, testSingleRowCteExistenceProbeEmpty },
    { 31, testSingleRowCteFullSubsetHit },
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
      printf("  --only N    run only test number N (1..31)\n");
      return 0;
    }
  }

  if (onlyTest != -1) {
    bool found = false;
    for (size_t i = 0; i < g_test_count; i++) {
      if (g_tests[i].number == onlyTest) { found = true; break; }
    }
    if (!found) {
      fprintf(stderr, "No such test: %d (valid: 1..22)\n", onlyTest);
      return 1;
    }
  }

  printf("=== testCteNdbApi ===\n");
  printf("Connect: %s, MySQL port: %d\n", connectString, mysqlPort);
  if (onlyTest != -1) printf("Filter: --only %d\n", onlyTest);
  printf("\n");

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int exitCode = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(30, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster: %s\n",
              clusterConn.get_latest_error_msg());
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
    ssize_t written = write(mtr_fd, "PASSED\n", 7);
    if (written != 7) exitCode = 1;
  }
  close(mtr_fd);

  return exitCode;
}
