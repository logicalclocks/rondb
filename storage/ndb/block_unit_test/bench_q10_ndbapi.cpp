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
 * bench_q10_ndbapi — TPC-H Q10 style benchmark: 4-table join, SUM + COUNT,
 *                     VARCHAR(25) GROUP BY with very high cardinality
 *                     (one group per customer).
 *
 * SQL QUERY:
 *   SELECT c.c_name,
 *          SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
 *          COUNT(*)
 *   FROM tpch_lineitem l
 *     JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
 *     JOIN tpch_customer c ON c.c_custkey = o.o_custkey
 *     JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey
 *   GROUP BY c.c_name
 *   ORDER BY revenue DESC
 *
 * JOIN TREE:
 *   Node 0: lineitem  (scanTable, root)
 *     +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
 *          +- Node 2: customer  (readTuple, key: orders.o_custkey,
 *          |                      setParent(orders))
 *               +- Node 3: nation    (readTuple, key: customer.c_nationkey,
 *                                      setParent(customer), aggregate leaf)
 *
 * Note: VARCHAR(25) GROUP BY uses 1-byte length prefix in NDB storage.
 * FetchGroupbyColumn() returns raw NDB format; extract with length prefix.
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

struct Q10Result {
  double sum;
  Int64  count;
};

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
  dict->invalidateTable("tpch_nation");

  const NdbDictionary::Table *lineitemTab = dict->getTable("tpch_lineitem");
  const NdbDictionary::Table *ordersTab = dict->getTable("tpch_orders");
  const NdbDictionary::Table *customerTab = dict->getTable("tpch_customer");
  const NdbDictionary::Table *nationTab = dict->getTable("tpch_nation");

  if (lineitemTab == nullptr || ordersTab == nullptr ||
      customerTab == nullptr || nationTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *c_nameCol =
      customerTab->getColumn("c_name");
  const NdbDictionary::Column *extendedpriceCol =
      lineitemTab->getColumn("l_extendedprice");
  const NdbDictionary::Column *discountCol =
      lineitemTab->getColumn("l_discount");

  if (c_nameCol == nullptr || extendedpriceCol == nullptr ||
      discountCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* Build aggregation on nation (leaf) */
  NdbAggregator agg(nationTab);
  if (!agg.GroupByLinked(0, c_nameCol) ||               /* GROUP BY c_name (linked pos 0) */
      !agg.LoadLinkedColumn(1, 0, extendedpriceCol) ||  /* reg0 = l_extendedprice */
      !agg.LoadDouble(1.0, 1) ||                        /* reg1 = 1.0 */
      !agg.LoadLinkedColumn(2, 2, discountCol) ||       /* reg2 = l_discount */
      !agg.Minus(1, 2) ||                               /* reg1 = 1 - discount */
      !agg.Mul(0, 1) ||                                 /* reg0 = revenue */
      !agg.Sum(0, 0) ||                                 /* agg[0] = SUM(revenue) */
      !agg.LoadUint64(1, 1) ||                          /* reg1 = 1 */
      !agg.Count(1, 1) ||                               /* agg[1] = COUNT */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

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

  /* Node 2: lookup customer (parent: orders) */
  const NdbQueryOperand *customerKey[] = {
    qb->linkedValue(ordersOp, "o_custkey"),
    nullptr
  };
  NdbQueryOptions customerOpts;
  customerOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  customerOpts.setParent(ordersOp);

  const NdbQueryLookupOperationDef *customerOp =
      qb->readTuple(customerTab, customerKey, &customerOpts);
  if (customerOp == nullptr) {
    fprintf(stderr, "readTuple(customer) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup nation (parent: customer, aggregate leaf) */
  const NdbQueryOperand *nationKey[] = {
    qb->linkedValue(customerOp, "c_nationkey"),
    nullptr
  };
  NdbQueryOptions nationOpts;
  nationOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  nationOpts.setParent(customerOp);
  nationOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(customerOp, "c_name");            /* pos 0: GROUP BY */
  const NdbLinkedOperand *link1 =
      qb->linkedValue(lineitemOp, "l_extendedprice");   /* pos 1: computation */
  const NdbLinkedOperand *link2 =
      qb->linkedValue(lineitemOp, "l_discount");        /* pos 2: computation */

  if (link0 == nullptr || link1 == nullptr || link2 == nullptr) {
    fprintf(stderr, "linkedValue failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  nationOpts.addLinkedProjection(link0);
  nationOpts.addLinkedProjection(link1);
  nationOpts.addLinkedProjection(link2);

  const NdbQueryLookupOperationDef *nationOp =
      qb->readTuple(nationTab, nationKey, &nationOpts);
  if (nationOp == nullptr) {
    fprintf(stderr, "readTuple(nation) failed: %s\n",
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

  std::map<std::string, Q10Result> ndbResults;
  Uint32 groupCount = 0;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    /* GROUP BY col 0: c_name VARCHAR(25) (linked)
     * VARCHAR in NDB has a 1-byte length prefix for columns <= 255 bytes.
     * FetchGroupbyColumn() returns the raw NDB format. */
    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    const char *rawPtr = nameCol.data();
    Uint32 rawSize = nameCol.byte_size();

    std::string custName;
    if (rawSize > 0) {
      /* 1-byte length prefix for VARCHAR(25) with latin1 */
      Uint8 strLen = (Uint8)rawPtr[0];
      if (strLen > rawSize - 1) strLen = (Uint8)(rawSize - 1);
      custName.assign(rawPtr + 1, strLen);
    }

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result cntRes = rec.FetchAggregationResult();

    Q10Result r;
    r.sum = sumRes.data_double();
    r.count = cntRes.data_int64();

    ndbResults[custName] = r;
    groupCount++;
  }

  V("  %u groups retrieved\n", groupCount);

  query->close();
  trans->close();

  auto t3 = Clock::now();

  /* MySQL verification */
  double sqlMs = 0;
  int failures = 0;

  if (mysqlConn != nullptr) {
    auto tSql0 = Clock::now();
    if (mysql_query(mysqlConn,
            "SELECT c.c_name, "
            "SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
            "COUNT(*) "
            "FROM tpch_lineitem l "
            "JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey "
            "JOIN tpch_customer c ON c.c_custkey = o.o_custkey "
            "JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey "
            "GROUP BY c.c_name "
            "ORDER BY revenue DESC") != 0) {
      fprintf(stderr, "SQL failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        std::map<std::string, Q10Result> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1] && row[2]) {
            std::string name(row[0]);
            Q10Result r;
            r.sum = atof(row[1]);
            r.count = atoll(row[2]);
            sqlResults[name] = r;
          }
        }
        mysql_free_result(res);

        /* Compare group counts */
        if (ndbResults.size() != sqlResults.size()) {
          fprintf(stderr, "FAIL: group count mismatch: ndb=%zu sql=%zu\n",
                  ndbResults.size(), sqlResults.size());
          failures++;
        }

        /* Spot-check a sample of groups */
        Uint32 checked = 0;
        for (const auto &kv : sqlResults) {
          if (ndbResults.count(kv.first) == 0) {
            if (checked < 10)
              fprintf(stderr, "FAIL: '%s' missing in ndbapi\n",
                      kv.first.c_str());
            failures++;
            continue;
          }
          const Q10Result &n = ndbResults[kv.first];
          const Q10Result &s = kv.second;
          double tol = std::max(0.01, fabs(s.sum) * 1e-9);
          if (fabs(n.sum - s.sum) > tol || n.count != s.count) {
            if (checked < 10)
              fprintf(stderr, "FAIL: '%s': ndb(%.2f,%lld) sql(%.2f,%lld)\n",
                      kv.first.c_str(),
                      n.sum, (long long)n.count,
                      s.sum, (long long)s.count);
            failures++;
          }
          checked++;
        }

        if (failures == 0)
          V("  SQL verify: %zu groups -- all match\n", sqlResults.size());
        else
          fprintf(stderr, "  SQL verify: %d mismatches out of %zu groups\n",
                  failures, sqlResults.size());
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

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("bench_q10_ndbapi: TPC-H Q10 high-cardinality VARCHAR benchmark (NDB API)\n");
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
    (void)write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
