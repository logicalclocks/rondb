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
 * bench_orderscan_ndbapi — Ordered index scan with INT range bounds, 2-table
 *                           join, SUM/COUNT/MIN/MAX on o_totalprice, GROUP BY
 *                           o_orderyear.
 *
 * Creates index idx_orders_year on tpch_orders(o_orderyear) if needed.
 *
 * SQL QUERY:
 *   SELECT o.o_orderyear,
 *          SUM(o.o_totalprice), COUNT(*),
 *          MIN(o.o_totalprice), MAX(o.o_totalprice)
 *   FROM tpch_orders o
 *     JOIN tpch_customer c ON c.c_custkey = o.o_custkey
 *   WHERE o.o_orderyear BETWEEN 1994 AND 1996
 *   GROUP BY o.o_orderyear
 *   ORDER BY o.o_orderyear
 *
 * JOIN TREE:
 *   Node 0: orders    (scanIndex on idx_orders_year,
 *                       bounds: [1994, 1996] inclusive, root)
 *     +- Node 1: customer  (readTuple, key: orders.o_custkey,
 *                             aggregate leaf)
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
#include <map>
#include <set>
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

struct OrderScanResult {
  double sum;
  Int64  count;
  double min;
  double max;
};

static int
runBenchmark(Ndb *ndb, MYSQL *mysqlConn, int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();

  /* Create index if it doesn't exist */
  if (mysqlConn != nullptr && iteration == 1) {
    mysql_query(mysqlConn,
      "CREATE INDEX idx_orders_year ON tpch_orders(o_orderyear) USING BTREE");
  }

  dict->invalidateTable("tpch_orders");
  dict->invalidateTable("tpch_customer");
  dict->invalidateIndex("idx_orders_year", "tpch_orders");

  const NdbDictionary::Table *ordersTab = dict->getTable("tpch_orders");
  const NdbDictionary::Table *customerTab = dict->getTable("tpch_customer");

  if (ordersTab == nullptr || customerTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Index *yearIdx =
      dict->getIndex("idx_orders_year", "tpch_orders");
  if (yearIdx == nullptr) {
    fprintf(stderr, "Index idx_orders_year not found: %s\n",
            dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *orderyearCol =
      ordersTab->getColumn("o_orderyear");
  const NdbDictionary::Column *totalpriceCol =
      ordersTab->getColumn("o_totalprice");

  if (orderyearCol == nullptr || totalpriceCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* Build aggregation on customer (leaf) */
  NdbAggregator agg(customerTab);
  if (!agg.GroupByLinked(0, orderyearCol) ||            /* GROUP BY o_orderyear (linked pos 0) */
      !agg.LoadLinkedColumn(1, 0, totalpriceCol) ||     /* reg0 = o_totalprice */
      !agg.Sum(0, 0) ||                                 /* agg[0] = SUM(reg0) */
      !agg.Count(1, 0) ||                               /* agg[1] = COUNT(*) */
      !agg.Min(2, 0) ||                                 /* agg[2] = MIN(reg0) */
      !agg.Max(3, 0) ||                                 /* agg[3] = MAX(reg0) */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /* Build pushed join query with scanIndex */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    fprintf(stderr, "NdbQueryBuilder::create failed\n");
    return -1;
  }

  /* Build range bounds: [1994, 1996] inclusive */
  const NdbQueryOperand *lowBound[] = {
    qb->constValue((Int32)1994),
    nullptr
  };
  const NdbQueryOperand *highBound[] = {
    qb->constValue((Int32)1996),
    nullptr
  };
  NdbQueryIndexBound bound(lowBound, true, highBound, true);

  /* Node 0: ordered index scan on orders (root) */
  const NdbQueryIndexScanOperationDef *ordersOp =
      qb->scanIndex(yearIdx, ordersTab, &bound);
  if (ordersOp == nullptr) {
    fprintf(stderr, "scanIndex(orders) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup customer (aggregate leaf) */
  const NdbQueryOperand *customerKey[] = {
    qb->linkedValue(ordersOp, "o_custkey"),
    nullptr
  };
  NdbQueryOptions customerOpts;
  customerOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  customerOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(ordersOp, "o_orderyear");   /* pos 0: GROUP BY */
  const NdbLinkedOperand *link1 =
      qb->linkedValue(ordersOp, "o_totalprice");  /* pos 1: SUM/MIN/MAX */

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

  /* Retrieve aggregated results */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    fprintf(stderr, "getAggregator returned nullptr\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  std::map<Int32, OrderScanResult> ndbResults;
  Uint32 groupCount = 0;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column yearCol = rec.FetchGroupbyColumn();
    Int32 year = yearCol.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result cntRes = rec.FetchAggregationResult();
    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxRes = rec.FetchAggregationResult();

    OrderScanResult r;
    r.sum = sumRes.data_double();
    r.count = cntRes.data_int64();
    r.min = minRes.data_double();
    r.max = maxRes.data_double();

    ndbResults[year] = r;
    groupCount++;

    V("  %d: SUM=%.2f COUNT=%lld MIN=%.2f MAX=%.2f\n",
      year, r.sum, (long long)r.count, r.min, r.max);
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
            "SELECT o.o_orderyear, "
            "SUM(o.o_totalprice), COUNT(*), "
            "MIN(o.o_totalprice), MAX(o.o_totalprice) "
            "FROM tpch_orders o "
            "JOIN tpch_customer c ON c.c_custkey = o.o_custkey "
            "WHERE o.o_orderyear BETWEEN 1994 AND 1996 "
            "GROUP BY o.o_orderyear "
            "ORDER BY o.o_orderyear") != 0) {
      fprintf(stderr, "SQL failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        std::map<Int32, OrderScanResult> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1] && row[2] && row[3] && row[4]) {
            Int32 year = atoi(row[0]);
            OrderScanResult r;
            r.sum = atof(row[1]);
            r.count = atoll(row[2]);
            r.min = atof(row[3]);
            r.max = atof(row[4]);
            sqlResults[year] = r;
          }
        }
        mysql_free_result(res);

        std::set<Int32> allKeys;
        for (const auto &kv : ndbResults) allKeys.insert(kv.first);
        for (const auto &kv : sqlResults) allKeys.insert(kv.first);

        for (Int32 key : allKeys) {
          bool hasNdb = ndbResults.count(key) > 0;
          bool hasSql = sqlResults.count(key) > 0;
          if (!hasNdb || !hasSql) {
            fprintf(stderr, "FAIL: year=%d: missing in %s\n",
                    key, hasNdb ? "sql" : "ndbapi");
            failures++;
            continue;
          }
          const OrderScanResult &n = ndbResults[key];
          const OrderScanResult &s = sqlResults[key];

          double tolSum = std::max(0.01, fabs(s.sum) * 1e-9);
          double tolMin = std::max(0.01, fabs(s.min) * 1e-9);
          double tolMax = std::max(0.01, fabs(s.max) * 1e-9);

          if (fabs(n.sum - s.sum) > tolSum ||
              n.count != s.count ||
              fabs(n.min - s.min) > tolMin ||
              fabs(n.max - s.max) > tolMax) {
            fprintf(stderr, "FAIL: year=%d: ndb(%.2f,%lld,%.2f,%.2f) "
                    "sql(%.2f,%lld,%.2f,%.2f)\n",
                    key, n.sum, (long long)n.count, n.min, n.max,
                    s.sum, (long long)s.count, s.min, s.max);
            failures++;
          }
        }

        if (failures == 0)
          V("  SQL verify: %zu groups -- all match\n", sqlResults.size());
        else
          fprintf(stderr, "  SQL verify: %d mismatches out of %zu groups\n",
                  failures, allKeys.size());
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
  printf("  Groups: %u\n", groupCount);
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

  printf("bench_orderscan_ndbapi: Ordered index scan benchmark (NDB API)\n");
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

  printf(result == 0 ? "*** ALL ITERATIONS PASSED ***\n"
                      : "*** SOME ITERATIONS FAILED ***\n");
  return result;
}
