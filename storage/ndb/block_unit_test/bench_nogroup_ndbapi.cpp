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
 * bench_nogroup_ndbapi — Global aggregation benchmark (no GROUP BY).
 *                         3-table join, 5 aggregate results, CHAR filter.
 *
 * SQL QUERY:
 *   SELECT COUNT(*),
 *          SUM(l.l_extendedprice),
 *          SUM(l.l_quantity),
 *          MIN(l.l_extendedprice),
 *          MAX(l.l_extendedprice)
 *   FROM tpch_lineitem l
 *     JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
 *     JOIN tpch_customer c ON c.c_custkey = o.o_custkey
 *   WHERE c.c_mktsegment = 'AUTOMOBILE'
 *
 * JOIN TREE:
 *   Node 0: lineitem  (scanTable, root)
 *     +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
 *          +- Node 2: customer  (readTuple, key: orders.o_custkey,
 *                                 filter: c_mktsegment = 'AUTOMOBILE',
 *                                 setParent(orders), aggregate leaf)
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

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

static double elapsedMs(TimePoint start, TimePoint end)
{
  return std::chrono::duration<double, std::milli>(end - start).count();
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
                         "test", mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

static int
runBenchmark(Ndb *ndb, MYSQL *mysqlConn, int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();

  dict->invalidateTable("tpch_lineitem");
  dict->invalidateTable("tpch_orders");
  dict->invalidateTable("tpch_customer");

  const NdbDictionary::Table *lineitemTab = dict->getTable("tpch_lineitem");
  const NdbDictionary::Table *ordersTab = dict->getTable("tpch_orders");
  const NdbDictionary::Table *customerTab = dict->getTable("tpch_customer");

  if (lineitemTab == nullptr || ordersTab == nullptr ||
      customerTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *extendedpriceCol =
      lineitemTab->getColumn("l_extendedprice");
  const NdbDictionary::Column *quantityCol =
      lineitemTab->getColumn("l_quantity");

  if (extendedpriceCol == nullptr || quantityCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* Build aggregation: no GROUP BY, 5 aggregates */
  NdbAggregator agg(customerTab);
  if (!agg.LoadLinkedColumn(0, 0, extendedpriceCol) || /* reg0 = l_extendedprice */
      !agg.LoadLinkedColumn(1, 1, quantityCol) ||      /* reg1 = l_quantity */
      !agg.Count(0, 0) ||                              /* agg[0] = COUNT(*) */
      !agg.Sum(1, 0) ||                                /* agg[1] = SUM(l_extendedprice) */
      !agg.Sum(2, 1) ||                                /* agg[2] = SUM(l_quantity) */
      !agg.Min(3, 0) ||                                /* agg[3] = MIN(l_extendedprice) */
      !agg.Max(4, 0) ||                                /* agg[4] = MAX(l_extendedprice) */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /* Build CHAR equality filter: c_mktsegment = 'AUTOMOBILE' (CHAR(10)) */
  NdbInterpretedCode filter(customerTab);
  static const char AUTO_PADDED[10] = {
    'A','U','T','O','M','O','B','I','L','E'
  };
  Uint32 mktAttrId = customerTab->getColumn("c_mktsegment")->getColumnNo();
  filter.branch_col_eq(AUTO_PADDED, 10, mktAttrId, 0);
  filter.interpret_exit_nok();
  filter.def_label(0);
  filter.interpret_exit_ok();
  if (filter.finalise() != 0) {
    fprintf(stderr, "Filter finalise failed: %s\n",
            filter.getNdbError().message);
    return -1;
  }

  /* Build pushed join query */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    fprintf(stderr, "NdbQueryBuilder::create failed\n");
    return -1;
  }

  /* Node 0: scan lineitem (root) */
  const NdbQueryTableScanOperationDef *lineitemOp = qb->scanTable(lineitemTab);
  if (lineitemOp == nullptr) {
    fprintf(stderr, "scanTable(lineitem) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup orders */
  const NdbQueryOperand *ordersKey[] = {
    qb->linkedValue(lineitemOp, "l_orderkey"),
    nullptr
  };
  NdbQueryOptions ordersOpts;
  ordersOpts.setMatchType(NdbQueryOptions::MatchNonNull);

  const NdbQueryLookupOperationDef *ordersOp =
      qb->readTuple(ordersTab, ordersKey, &ordersOpts);
  if (ordersOp == nullptr) {
    fprintf(stderr, "readTuple(orders) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup customer (aggregate leaf, filter, no GROUP BY) */
  const NdbQueryOperand *customerKey[] = {
    qb->linkedValue(ordersOp, "o_custkey"),
    nullptr
  };
  NdbQueryOptions customerOpts;
  customerOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  customerOpts.setParent(ordersOp);
  customerOpts.setInterpretedCode(filter);
  customerOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(lineitemOp, "l_extendedprice"); /* pos 0 */
  const NdbLinkedOperand *link1 =
      qb->linkedValue(lineitemOp, "l_quantity");      /* pos 1 */

  if (link0 == nullptr || link1 == nullptr) {
    fprintf(stderr, "linkedValue failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  customerOpts.addLinkedProjection(link0);
  customerOpts.addLinkedProjection(link1);

  const NdbQueryLookupOperationDef *customerOp =
      qb->readTuple(customerTab, customerKey, &customerOpts);
  if (customerOp == nullptr) {
    fprintf(stderr, "readTuple(customer) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    fprintf(stderr, "prepare() failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  /* Execute query */
  auto t0 = Clock::now();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    queryDef->destroy();
    return -1;
  }

  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    fprintf(stderr, "createQuery failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    fprintf(stderr, "execute failed: %s\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  auto t1 = Clock::now();

  NdbQuery::NextResultOutcome outcome;
  Uint32 rowCount = 0;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
  }
  if (outcome == NdbQuery::NextResult_error) {
    fprintf(stderr, "nextResult error %d: %s\n",
            query->getNdbError().code, query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  auto t2 = Clock::now();
  V("  Scan consumed %u rows\n", rowCount);

  /* Retrieve aggregated results (1 record, no GROUP BY) */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    fprintf(stderr, "getAggregator returned nullptr\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Int64 ndbCount = 0;
  double ndbSumPrice = 0, ndbSumQty = 0, ndbMinPrice = 0, ndbMaxPrice = 0;

  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (!rec.end()) {
    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    NdbAggregator::Result sumPriceRes = rec.FetchAggregationResult();
    NdbAggregator::Result sumQtyRes = rec.FetchAggregationResult();
    NdbAggregator::Result minPriceRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxPriceRes = rec.FetchAggregationResult();

    ndbCount = countRes.data_int64();
    ndbSumPrice = sumPriceRes.data_double();
    ndbSumQty = sumQtyRes.data_double();
    ndbMinPrice = minPriceRes.data_double();
    ndbMaxPrice = maxPriceRes.data_double();

    V("  COUNT=%lld SUM(price)=%.2f SUM(qty)=%.2f MIN=%.2f MAX=%.2f\n",
      (long long)ndbCount, ndbSumPrice, ndbSumQty, ndbMinPrice, ndbMaxPrice);
  }

  query->close();
  trans->close();

  auto t3 = Clock::now();

  /* MySQL verification */
  double sqlMs = 0;
  int failures = 0;

  if (mysqlConn != nullptr) {
    auto tSql0 = Clock::now();
    if (mysql_query(mysqlConn,
            "SELECT COUNT(*), SUM(l.l_extendedprice), SUM(l.l_quantity), "
            "MIN(l.l_extendedprice), MAX(l.l_extendedprice) "
            "FROM tpch_lineitem l "
            "JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey "
            "JOIN tpch_customer c ON c.c_custkey = o.o_custkey "
            "WHERE c.c_mktsegment = 'AUTOMOBILE'") != 0) {
      fprintf(stderr, "SQL failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        MYSQL_ROW row = mysql_fetch_row(res);
        if (row != nullptr && row[0] && row[1] && row[2] && row[3] && row[4]) {
          Int64 sqlCount = atoll(row[0]);
          double sqlSumPrice = atof(row[1]);
          double sqlSumQty = atof(row[2]);
          double sqlMinPrice = atof(row[3]);
          double sqlMaxPrice = atof(row[4]);

          if (ndbCount != sqlCount) {
            fprintf(stderr, "FAIL COUNT: ndb=%lld sql=%lld\n",
                    (long long)ndbCount, (long long)sqlCount);
            failures++;
          }
          double tolSP = std::max(0.01, fabs(sqlSumPrice) * 1e-9);
          double tolSQ = std::max(0.01, fabs(sqlSumQty) * 1e-9);
          double tolMn = std::max(0.01, fabs(sqlMinPrice) * 1e-9);
          double tolMx = std::max(0.01, fabs(sqlMaxPrice) * 1e-9);

          if (fabs(ndbSumPrice - sqlSumPrice) > tolSP) {
            fprintf(stderr, "FAIL SUM(price): ndb=%.2f sql=%.2f\n",
                    ndbSumPrice, sqlSumPrice);
            failures++;
          }
          if (fabs(ndbSumQty - sqlSumQty) > tolSQ) {
            fprintf(stderr, "FAIL SUM(qty): ndb=%.2f sql=%.2f\n",
                    ndbSumQty, sqlSumQty);
            failures++;
          }
          if (fabs(ndbMinPrice - sqlMinPrice) > tolMn) {
            fprintf(stderr, "FAIL MIN: ndb=%.2f sql=%.2f\n",
                    ndbMinPrice, sqlMinPrice);
            failures++;
          }
          if (fabs(ndbMaxPrice - sqlMaxPrice) > tolMx) {
            fprintf(stderr, "FAIL MAX: ndb=%.2f sql=%.2f\n",
                    ndbMaxPrice, sqlMaxPrice);
            failures++;
          }

          if (failures == 0)
            V("  SQL verify: all match\n");
        } else {
          fprintf(stderr, "SQL returned no result\n");
          failures++;
        }
        mysql_free_result(res);
      }
    }
    auto tSql1 = Clock::now();
    sqlMs = elapsedMs(tSql0, tSql1);
  }

  /* Timing */
  double prepMs = elapsedMs(t0, t1);
  double scanMs = elapsedMs(t1, t2);
  double resultMs = elapsedMs(t2, t3);
  double totalMs = elapsedMs(t0, t3);

  printf("Iteration %d: %s\n", iteration,
         failures == 0 ? "PASS" : "FAIL");
  printf("  COUNT=%lld\n", (long long)ndbCount);
  printf("  Prepare+Execute: %8.2f ms\n", prepMs);
  printf("  Scan+Join:       %8.2f ms\n", scanMs);
  printf("  Results:         %8.2f ms\n", resultMs);
  printf("  Total:           %8.2f ms\n", totalMs);
  if (sqlMs > 0)
    printf("  SQL query:       %8.2f ms\n", sqlMs);

  queryDef->destroy();
  return failures > 0 ? -1 : 0;
}

static void
usage(const char *prog)
{
  fprintf(stderr,
          "Usage: %s [options]\n"
          "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
          "  -m <port>            MySQL port (default: 3306)\n"
          "  --iterations <N>     Benchmark iterations (default: 3)\n"
          "  -v, --verbose        Verbose output\n"
          "  -h, --help           Show help\n",
          prog);
}

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;
  int numIterations = 3;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
      numIterations = atoi(argv[++i]);
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }
  }

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("bench_nogroup_ndbapi: Global aggregation benchmark (NDB API)\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Iterations: %d\n\n", numIterations);

  ndb_init();
  int result = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm: %s\n", connectString);
      ndb_end(0);
      return 1;
    }
    if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30s\n");
      ndb_end(0);
      return 1;
    }
    V("Connected to cluster\n");

    MYSQL *mysqlConn = connectMysql(mysqlPort);
    if (mysqlConn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      ndb_end(0);
      return 1;
    }

    {
      Ndb ndb(&clusterConn, "test");
      if (ndb.init() != 0) {
        fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
        mysql_close(mysqlConn);
        ndb_end(0);
        return 1;
      }

      printf("Starting benchmark (%d iterations)...\n\n", numIterations);

      for (int iter = 1; iter <= numIterations; iter++) {
        if (runBenchmark(&ndb, mysqlConn, iter) != 0) {
          result = 1;
        }
        printf("\n");
      }
    }

    mysql_close(mysqlConn);
  }

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
