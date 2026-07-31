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
 * testJoinAggIdempotency — Phase L D9 + D11 block-test coverage.
 *
 * D9 (cross-phase stale CONF): build a 2-level chained CTE and run
 *     it once.  The query exercises C's per-record correlation
 *     (every JOIN_AGG_COMPLETE_REQ carries a tagged requestId; CONFs
 *     index back into the AggCompleteRecord pool) and C.2's
 *     phase-number drop on stale CTE_PHASE_COMPLETE_REPs.  A correct
 *     row count + per-group sums verifies the unified record path.
 *
 * D11 (concurrency stress): same chained CTE query, N=100
 *     iterations.  Each iteration spans phase 0 → phase 1 → main,
 *     touching every Phase L code path (A: checkCteReady idempotency,
 *     B: continueJoinAggRedistribute idempotency, E.1: single-owner
 *     LDM routing) under realistic multi-LDM scheduling.
 *     Repeated execution catches regressions of the original
 *     ronsql_cte_multi_batch race that needed ~50 fresh kernel
 *     starts to manifest manually.
 *
 * Synthetic duplicate-injection ERROR_INSERTs (D7 / D8 dedicated
 * tests) are deliberately deferred — the assertion guards added in
 * Phase L commit 1 already crash debug builds whenever a duplicate
 * fires through any guarded handler, and the chained-CTE workload
 * here exercises every guarded path on the success side.  Adding
 * synthetic injection requires kernel-side scaffolding that lands
 * in a separate commit.
 *
 * SQL equivalent:
 *   WITH
 *     cte0 AS (SELECT grp, SUM(val) AS total FROM jagidem_src
 *              GROUP BY grp),
 *     cte1 AS (SELECT cte0.grp, SUM(cte0.total) AS total
 *              FROM jagidem_src s JOIN cte0 ON s.grp = cte0.grp
 *              GROUP BY cte0.grp)
 *   SELECT s.grp, cte1.total
 *     FROM jagidem_src s JOIN cte1 ON s.grp = cte1.grp;
 *
 * Test data:
 *   (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)
 *   → cte0 = {(1,30),(2,70),(3,50)}.
 *   → cte1 = SUM(cte0.total) per scanned-row's cte0 match
 *          = {(1,60),(2,140),(3,50)}.
 *   → Main query: 5 result rows, one per source row, with cte1
 *     totals 60,60,140,140,50.
 *
 * Usage: testJoinAggIdempotency -c <connect_string> -m <mysql_port>
 *                               [-v] [--iterations N]
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

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

static const char *TEST_DB    = "test";
static const char *SRC_TABLE  = "jagidem_src";
static const char *VIRT_TABLE = "jagidem_virt";

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
    fprintf(stderr, "FAILED (mysql_real_connect: %s)\n",
            mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

static int
runSql(MYSQL *conn, const char *sql)
{
  if (mysql_query(conn, sql) != 0) {
    fprintf(stderr, "FAILED (%s: %s)\n", sql, mysql_error(conn));
    return -1;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* Schema setup                                                        */
/* ------------------------------------------------------------------ */

static int
createTables(MYSQL *conn)
{
  if (runSql(conn, "DROP TABLE IF EXISTS jagidem_src") != 0) return -1;
  if (runSql(conn, "DROP TABLE IF EXISTS jagidem_virt") != 0) return -1;

  if (runSql(conn,
        "CREATE TABLE jagidem_src ("
        " pk INT NOT NULL,"
        " grp INT NOT NULL,"
        " val BIGINT NOT NULL,"
        " PRIMARY KEY USING HASH (pk)) ENGINE=NDB") != 0) return -1;

  /* Virtual table providing the type metadata for the chained
   * lookupCte primitives — same schema convention as testCteNdbApi.cpp. */
  if (runSql(conn,
        "CREATE TABLE jagidem_virt ("
        " grp INT NOT NULL,"
        " total BIGINT NOT NULL,"
        " PRIMARY KEY USING HASH (grp)) ENGINE=NDB") != 0) return -1;

  return 0;
}

static int
dropTables(MYSQL *conn)
{
  runSql(conn, "DROP TABLE IF EXISTS jagidem_src");
  runSql(conn, "DROP TABLE IF EXISTS jagidem_virt");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Data insertion via NDB API                                          */
/* ------------------------------------------------------------------ */

static int
insertOneRow(Ndb *ndb, const NdbDictionary::Table *tab,
             Int32 pk, Int32 grp, Int64 val)
{
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) return -1;
  NdbOperation *op = trans->getNdbOperation(tab);
  if (op == nullptr ||
      op->insertTuple() != 0 ||
      op->equal("pk", pk) != 0 ||
      op->setValue("grp", grp) != 0 ||
      op->setValue("val", val) != 0 ||
      trans->execute(NdbTransaction::Commit) != 0) {
    fprintf(stderr, "FAILED insert pk=%d: %s\n",
            pk, trans->getNdbError().message);
    trans->close();
    return -1;
  }
  trans->close();
  return 0;
}

static int
insertTestData(Ndb *ndb)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  if (tab == nullptr) {
    fprintf(stderr, "FAILED (getTable %s)\n", SRC_TABLE);
    return -1;
  }
  if (insertOneRow(ndb, tab, 1, 1, 10) != 0) return -1;
  if (insertOneRow(ndb, tab, 2, 1, 20) != 0) return -1;
  if (insertOneRow(ndb, tab, 3, 2, 30) != 0) return -1;
  if (insertOneRow(ndb, tab, 4, 2, 40) != 0) return -1;
  if (insertOneRow(ndb, tab, 5, 3, 50) != 0) return -1;
  return 0;
}

/* ------------------------------------------------------------------ */
/* Chained-CTE query construction + execution                          */
/* ------------------------------------------------------------------ */

/* Build + execute the chained-CTE query once, verify the 5 expected
 * result rows.  Returns 0 on success, -1 on failure.  Called from
 * runD9 and from runD11's loop body. */
static int
runChainedCteOnce(Ndb *ndb, Uint32 iterIdx)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  dict->invalidateTable(VIRT_TABLE);
  const NdbDictionary::Table *srcTab  = dict->getTable(SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(VIRT_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    fprintf(stderr, "FAILED (table lookup iter=%u: %s)\n",
            iterIdx, dict->getNdbError().message);
    return -1;
  }
  const NdbDictionary::Column *grpCol   = virtTab->getColumn("grp");
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  if (grpCol == nullptr || totalCol == nullptr) return -1;

  /* CTE 0 aggregator: GROUP BY grp, SUM(val). */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") ||
      !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) ||
      !cte0Agg.Finalize()) {
    fprintf(stderr, "FAILED (cte0Agg iter=%u: %s)\n",
            iterIdx, cte0Agg.GetError().err_msg_);
    return -1;
  }

  /* CTE 1 aggregator: GROUP BY cte0.grp (linked pos 0),
   * SUM(cte0.total) (linked pos 1).  Aggregates the lookupCte result
   * row coming from cte0. */
  NdbAggregator cte1Agg(virtTab);
  if (!cte1Agg.GroupByLinked(0, grpCol) ||
      !cte1Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    fprintf(stderr, "FAILED (cte1Agg iter=%u: %s)\n",
            iterIdx, cte1Agg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) return -1;

  /* CTE 0 subtree: scan + readTuple self-join feeding cte0Agg. */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      fprintf(stderr, "FAILED (cte0 scan iter=%u: %s)\n",
              iterIdx, qb->getNdbError().message);
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
      fprintf(stderr, "FAILED (cte0 leaf iter=%u: %s)\n",
              iterIdx, qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, cte0Agg, /*depMask=*/0) != 0) {
    fprintf(stderr, "FAILED (defineCte 0 iter=%u)\n", iterIdx);
    qb->destroy();
    return -1;
  }

  /* CTE 1 subtree: scan + lookupCte(0) feeding cte1Agg.  This is the
   * cross-phase boundary that C+C.2 protect: phase 0 must complete
   * fully (records dedup'd, phase report dedup'd) before the lookup
   * fan-out for phase 1 fires. */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    if (scan == nullptr) {
      fprintf(stderr, "FAILED (cte1 scan iter=%u: %s)\n",
              iterIdx, qb->getNdbError().message);
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
      fprintf(stderr, "FAILED (cte1 nested lookupCte iter=%u: %s)\n",
              iterIdx, qb->getNdbError().message);
      qb->destroy();
      return -1;
    }
  }
  qb->endCteSubtree();
  if (qb->defineCte(1, virtTab, cte1Agg, /*depMask=*/1) != 0) {
    fprintf(stderr, "FAILED (defineCte 1 iter=%u)\n", iterIdx);
    qb->destroy();
    return -1;
  }

  /* Main query: scan + lookupCte(1) — pass-through delivery to API. */
  const NdbQueryTableScanOperationDef *mainScan = qb->scanTable(srcTab);
  if (mainScan == nullptr) {
    fprintf(stderr, "FAILED (main scan iter=%u: %s)\n",
            iterIdx, qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cte1Key[] = {
    qb->linkedValue(mainScan, "grp"), nullptr
  };
  NdbQueryOptions cte1Opts;
  cte1Opts.setMatchType(NdbQueryOptions::MatchNonNull);
  if (qb->lookupCte(1, 2, virtTab, cte1Key, &cte1Opts) == nullptr) {
    fprintf(stderr, "FAILED (main lookupCte iter=%u: %s)\n",
            iterIdx, qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    fprintf(stderr, "FAILED (prepare iter=%u: %s)\n",
            iterIdx, qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    queryDef->destroy();
    return -1;
  }

  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    fprintf(stderr, "FAILED (createQuery iter=%u: %s)\n",
            iterIdx, trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  const Uint32 mainScanOpNo = queryDef->getNoOfOperations() - 2;
  const Uint32 mainCteLookupOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainScanOp = query->getQueryOperation(mainScanOpNo);
  NdbQueryOperation *mainCteLookup =
      query->getQueryOperation(mainCteLookupOpNo);
  NdbRecAttr *raMainGrp  = nullptr;
  NdbRecAttr *raCteGrp   = nullptr;
  NdbRecAttr *raCteTotal = nullptr;
  if (mainScanOp != nullptr) raMainGrp  = mainScanOp->getValue("grp");
  if (mainCteLookup != nullptr) {
    /* CTE_LOOKUP delivers all virtual result columns requested by the
     * serialized CTE attr list; registering both columns here also sizes
     * the API receive buffer for the full row. */
    raCteGrp = mainCteLookup->getValue("grp");
    raCteTotal = mainCteLookup->getValue("total");
  }
  if (raMainGrp == nullptr || raCteGrp == nullptr || raCteTotal == nullptr) {
    fprintf(stderr, "FAILED (getValue iter=%u)\n", iterIdx);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &qErr = query->getNdbError();
    fprintf(stderr, "FAILED (execute iter=%u: %d %s)\n",
            iterIdx, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Expected cte1 totals per group. */
  std::map<Int32, Int64> expected;
  expected[1] = 60;
  expected[2] = 140;
  expected[3] = 50;

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 mainGrp  = raMainGrp->int32_value();
    Int64 cteTotal = raCteTotal->int64_value();
    auto it = expected.find(mainGrp);
    if (it == expected.end()) {
      fprintf(stderr, "FAILED (iter=%u unexpected grp=%d)\n",
              iterIdx, mainGrp);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
    if (cteTotal != it->second) {
      fprintf(stderr, "FAILED (iter=%u grp=%d expected=%lld got=%lld)\n",
              iterIdx, mainGrp,
              (long long)it->second, (long long)cteTotal);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
  }
  if (outcome == NdbQuery::NextResult_error) {
    const NdbError &qErr = query->getNdbError();
    fprintf(stderr, "FAILED (nextResult iter=%u: %d %s)\n",
            iterIdx, qErr.code, qErr.message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != 5) {
    fprintf(stderr, "FAILED (iter=%u expected 5 rows, got %u)\n",
            iterIdx, rowCount);
    return -1;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* D9 — single chained-CTE run                                         */
/* ------------------------------------------------------------------ */

static int
runD9(Ndb *ndb)
{
  printf("D9: chained CTE single run ... ");
  fflush(stdout);
  if (runChainedCteOnce(ndb, /*iterIdx=*/0) != 0) {
    return -1;
  }
  printf("OK\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* D11 — chained-CTE stress loop                                       */
/* ------------------------------------------------------------------ */

static int
runD11(Ndb *ndb, Uint32 iterations)
{
  printf("D11: chained CTE stress (%u iterations) ... ", iterations);
  fflush(stdout);
  for (Uint32 i = 0; i < iterations; i++) {
    if (runChainedCteOnce(ndb, /*iterIdx=*/i + 1) != 0) {
      return -1;
    }
    if (verbose && (i + 1) % 10 == 0) {
      V("\n  iter %u/%u OK", i + 1, iterations);
    }
  }
  printf("OK\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;
  Uint32 iterations = 100;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
      connectString = argv[++i];
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
      mysqlPort = atoi(argv[++i]);
    else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0)
      verbose = true;
    else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc)
      iterations = (Uint32)atoi(argv[++i]);
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s -c <connect_string> -m <mysql_port> [-v] "
             "[--iterations N]\n", argv[0]);
      return 0;
    }
  }

  printf("=== testJoinAggIdempotency ===\n");
  printf("Connect: %s, MySQL port: %d, iterations: %u\n",
         connectString, mysqlPort, iterations);

  /* MTR integration: dup stdout, redirect stdout → stderr.  The
   * verbose progress output goes to stderr; only the final
   * "PASSED\n" line goes to the original stdout so the .result
   * file matches.  Same pattern as testCteNdbApiOuterJoin. */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int rc = 0;

  if (mysql_library_init(0, nullptr, nullptr) != 0) {
    fprintf(stderr, "mysql_library_init failed\n");
    ndb_end(0);
    return 1;
  }
  MYSQL *conn = connectMysql(mysqlPort);
  if (conn == nullptr) {
    mysql_library_end();
    ndb_end(0);
    return 1;
  }
  if (createTables(conn) != 0) {
    mysql_close(conn);
    mysql_library_end();
    ndb_end(0);
    return 1;
  }

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(30, 5, 0) != 0) {
      fprintf(stderr, "Cannot connect to cluster: %s\n",
              clusterConn.get_latest_error_msg());
      rc = 1;
      goto cleanup;
    }
    if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      rc = 1;
      goto cleanup;
    }

    Ndb ndb(&clusterConn, TEST_DB);
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed\n");
      rc = 1;
      goto cleanup;
    }

    if (insertTestData(&ndb) != 0) { rc = 1; goto cleanup; }
    if (runD9(&ndb) != 0)          { rc = 1; goto cleanup; }
    if (runD11(&ndb, iterations) != 0) { rc = 1; goto cleanup; }

    printf("\nAll tests passed.\n");
  }

cleanup:
  dropTables(conn);
  mysql_close(conn);
  mysql_library_end();
  ndb_end(0);
  if (rc == 0) {
    ssize_t written = write(mtr_fd, "PASSED\n", 7);
    if (written != 7) rc = 1;
  }
  close(mtr_fd);
  return rc;
}
