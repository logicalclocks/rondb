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
 * bench_minmax_ndbapi — TPC-H style benchmark testing all 4 aggregation types
 *                       (MIN, MAX, COUNT, SUM) via NDB API pushdown join.
 *
 * 2-table join (supplier -> nation), CHAR(25) GROUP BY, 25 groups.
 *
 * SQL QUERY:
 *   SELECT n.n_name,
 *          MIN(s.s_acctbal), MAX(s.s_acctbal),
 *          COUNT(*), SUM(s.s_acctbal)
 *   FROM tpch_supplier s
 *     JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
 *   GROUP BY n.n_name
 *   ORDER BY n.n_name
 *
 * JOIN TREE:
 *   Node 0: supplier  (scanTable, root)
 *     +- Node 1: nation   (readTuple, key: supplier.s_nationkey, aggregate leaf)
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

struct MinMaxResult {
  double min;
  double max;
  Int64  count;
  double sum;
};

static int
runBenchmark(Ndb *ndb, MYSQL *mysqlConn, int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();

  dict->invalidateTable("tpch_supplier");
  dict->invalidateTable("tpch_nation");

  const NdbDictionary::Table *supplierTab = dict->getTable("tpch_supplier");
  const NdbDictionary::Table *nationTab = dict->getTable("tpch_nation");

  if (supplierTab == nullptr || nationTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *acctbalCol =
      supplierTab->getColumn("s_acctbal");
  if (acctbalCol == nullptr) {
    fprintf(stderr, "Column s_acctbal not found\n");
    return -1;
  }

  /* Build aggregation: GROUP BY n_name, MIN/MAX/COUNT/SUM(s_acctbal) */
  NdbAggregator agg(nationTab);
  if (!agg.GroupBy("n_name") ||
      !agg.LoadLinkedColumn(0, 0, acctbalCol) ||   /* reg0 = s_acctbal */
      !agg.Min(0, 0) ||                            /* agg[0] = MIN(reg0) */
      !agg.Max(1, 0) ||                            /* agg[1] = MAX(reg0) */
      !agg.Count(2, 0) ||                          /* agg[2] = COUNT(*) */
      !agg.Sum(3, 0) ||                            /* agg[3] = SUM(reg0) */
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

  /* Node 0: scan supplier (root) */
  const NdbQueryTableScanOperationDef *supplierOp = qb->scanTable(supplierTab);
  if (supplierOp == nullptr) {
    fprintf(stderr, "scanTable(supplier) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup nation (key: supplier.s_nationkey, aggregate leaf) */
  const NdbQueryOperand *nationKey[] = {
    qb->linkedValue(supplierOp, "s_nationkey"),
    nullptr
  };
  NdbQueryOptions nationOpts;
  nationOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  nationOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(supplierOp, "s_acctbal");   /* pos 0: MIN/MAX/SUM */
  if (link0 == nullptr) {
    fprintf(stderr, "linkedValue(s_acctbal) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  nationOpts.addLinkedProjection(link0);

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

  std::map<std::string, MinMaxResult> ndbResults;
  Uint32 groupCount = 0;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    int nameLen = (int)nameCol.byte_size();
    const char *namePtr = nameCol.data();
    int effLen = (int)strnlen(namePtr, nameLen);
    while (effLen > 0 && namePtr[effLen - 1] == ' ')
      effLen--;
    std::string nation(namePtr, effLen);

    NdbAggregator::Result minRes = rec.FetchAggregationResult();
    NdbAggregator::Result maxRes = rec.FetchAggregationResult();
    NdbAggregator::Result cntRes = rec.FetchAggregationResult();
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();

    MinMaxResult r;
    r.min = minRes.data_double();
    r.max = maxRes.data_double();
    r.count = cntRes.data_int64();
    r.sum = sumRes.data_double();

    ndbResults[nation] = r;
    groupCount++;

    V("  %-25s MIN=%.2f MAX=%.2f COUNT=%lld SUM=%.2f\n",
      nation.c_str(), r.min, r.max, (long long)r.count, r.sum);
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
            "SELECT n.n_name, MIN(s.s_acctbal), MAX(s.s_acctbal), "
            "COUNT(*), SUM(s.s_acctbal) "
            "FROM tpch_supplier s "
            "JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey "
            "GROUP BY n.n_name "
            "ORDER BY n.n_name") != 0) {
      fprintf(stderr, "SQL failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        std::map<std::string, MinMaxResult> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1] && row[2] && row[3] && row[4]) {
            std::string name(row[0]);
            while (!name.empty() && name.back() == ' ') name.pop_back();
            MinMaxResult r;
            r.min = atof(row[1]);
            r.max = atof(row[2]);
            r.count = atoll(row[3]);
            r.sum = atof(row[4]);
            sqlResults[name] = r;
          }
        }
        mysql_free_result(res);

        std::set<std::string> allKeys;
        for (const auto &kv : ndbResults) allKeys.insert(kv.first);
        for (const auto &kv : sqlResults) allKeys.insert(kv.first);

        for (const auto &key : allKeys) {
          bool hasNdb = ndbResults.count(key) > 0;
          bool hasSql = sqlResults.count(key) > 0;
          if (!hasNdb || !hasSql) {
            fprintf(stderr, "FAIL: '%s': missing in %s\n",
                    key.c_str(), hasNdb ? "sql" : "ndbapi");
            failures++;
            continue;
          }
          const MinMaxResult &n = ndbResults[key];
          const MinMaxResult &s = sqlResults[key];

          double tolMin = std::max(0.01, fabs(s.min) * 1e-9);
          double tolMax = std::max(0.01, fabs(s.max) * 1e-9);
          double tolSum = std::max(0.01, fabs(s.sum) * 1e-9);

          if (fabs(n.min - s.min) > tolMin ||
              fabs(n.max - s.max) > tolMax ||
              n.count != s.count ||
              fabs(n.sum - s.sum) > tolSum) {
            fprintf(stderr, "FAIL: '%s': ndb(%.2f,%.2f,%lld,%.2f) "
                    "sql(%.2f,%.2f,%lld,%.2f)\n",
                    key.c_str(), n.min, n.max, (long long)n.count, n.sum,
                    s.min, s.max, (long long)s.count, s.sum);
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

  printf("bench_minmax_ndbapi: MIN/MAX/COUNT/SUM benchmark (NDB API)\n");
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
