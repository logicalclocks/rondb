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
 * bench_datescan_ndbapi — Ordered index scan with DATE range bounds, 2-table
 *                          join, SUM + COUNT, CHAR(10) GROUP BY on l_shipmode.
 *
 * Creates index idx_lineitem_shipdate on tpch_lineitem(l_shipdate) if needed.
 *
 * NDB DATE encoding: packed = (year << 9) | (month << 5) | day
 * Stored as 3-byte little-endian.
 *
 * SQL QUERY:
 *   SELECT l.l_shipmode,
 *          SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
 *          COUNT(*)
 *   FROM tpch_lineitem l
 *     JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
 *   WHERE l.l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'
 *   GROUP BY l.l_shipmode
 *   ORDER BY l.l_shipmode
 *
 * JOIN TREE:
 *   Node 0: lineitem  (scanIndex on idx_lineitem_shipdate,
 *                        bounds: [1994-01-01, 1994-12-31] inclusive, root)
 *     +- Node 1: orders    (readTuple, key: lineitem.l_orderkey,
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

struct DateScanResult {
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

  /* Create index if it doesn't exist */
  if (mysqlConn != nullptr && iteration == 1) {
    mysql_query(mysqlConn,
      "CREATE INDEX idx_lineitem_shipdate ON tpch_lineitem(l_shipdate) "
      "USING BTREE");
  }

  dict->invalidateTable("tpch_lineitem");
  dict->invalidateTable("tpch_orders");
  dict->invalidateIndex("idx_lineitem_shipdate", "tpch_lineitem");

  const NdbDictionary::Table *lineitemTab = dict->getTable("tpch_lineitem");
  const NdbDictionary::Table *ordersTab = dict->getTable("tpch_orders");

  if (lineitemTab == nullptr || ordersTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Index *shipIdx =
      dict->getIndex("idx_lineitem_shipdate", "tpch_lineitem");
  if (shipIdx == nullptr) {
    fprintf(stderr, "Index idx_lineitem_shipdate not found: %s\n",
            dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *shipmodeCol =
      lineitemTab->getColumn("l_shipmode");
  const NdbDictionary::Column *extendedpriceCol =
      lineitemTab->getColumn("l_extendedprice");
  const NdbDictionary::Column *discountCol =
      lineitemTab->getColumn("l_discount");

  if (shipmodeCol == nullptr || extendedpriceCol == nullptr ||
      discountCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* Build aggregation on orders (leaf) */
  NdbAggregator agg(ordersTab);
  if (!agg.GroupByLinked(0, shipmodeCol) ||             /* GROUP BY l_shipmode (linked pos 0) */
      !agg.LoadLinkedColumn(1, 0, extendedpriceCol) ||  /* reg0 = l_extendedprice */
      !agg.LoadDouble(1.0, 1) ||                        /* reg1 = 1.0 */
      !agg.LoadLinkedColumn(2, 2, discountCol) ||       /* reg2 = l_discount */
      !agg.Minus(1, 2) ||                               /* reg1 = 1 - discount */
      !agg.Mul(0, 1) ||                                 /* reg0 = revenue */
      !agg.Sum(0, 0) ||                                 /* agg[0] = SUM(revenue) */
      !agg.LoadUint64(1, 1) ||                          /* reg1 = 1 */
      !agg.Count(1, 1) ||                               /* agg[1] = COUNT(*) */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /* Build pushed join query with scanIndex + DATE bounds */
  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    fprintf(stderr, "NdbQueryBuilder::create failed\n");
    return -1;
  }

  /* Pack NDB DATE: (year << 9) | (month << 5) | day, 3 bytes little-endian */
  Uint32 lowPacked = (1994u << 9) | (1u << 5) | 1u;     /* 1994-01-01 */
  Uint32 highPacked = (1994u << 9) | (12u << 5) | 31u;   /* 1994-12-31 */
  char lowBuf[4] = {0}, highBuf[4] = {0};
  lowBuf[0] = (char)(lowPacked & 0xFF);
  lowBuf[1] = (char)((lowPacked >> 8) & 0xFF);
  lowBuf[2] = (char)((lowPacked >> 16) & 0xFF);
  highBuf[0] = (char)(highPacked & 0xFF);
  highBuf[1] = (char)((highPacked >> 8) & 0xFF);
  highBuf[2] = (char)((highPacked >> 16) & 0xFF);

  const NdbQueryOperand *lowBound[] = {
    qb->constValue(lowBuf, 3),
    nullptr
  };
  const NdbQueryOperand *highBound[] = {
    qb->constValue(highBuf, 3),
    nullptr
  };
  NdbQueryIndexBound bound(lowBound, true, highBound, true);

  /* Node 0: ordered index scan on lineitem (root) */
  const NdbQueryIndexScanOperationDef *lineitemOp =
      qb->scanIndex(shipIdx, lineitemTab, &bound);
  if (lineitemOp == nullptr) {
    fprintf(stderr, "scanIndex(lineitem) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup orders (aggregate leaf) */
  const NdbQueryOperand *ordersKey[] = {
    qb->linkedValue(lineitemOp, "l_orderkey"),
    nullptr
  };
  NdbQueryOptions ordersOpts;
  ordersOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  ordersOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(lineitemOp, "l_shipmode");       /* pos 0: GROUP BY */
  const NdbLinkedOperand *link1 =
      qb->linkedValue(lineitemOp, "l_extendedprice");  /* pos 1: computation */
  const NdbLinkedOperand *link2 =
      qb->linkedValue(lineitemOp, "l_discount");       /* pos 2: computation */

  if (link0 == nullptr || link1 == nullptr || link2 == nullptr) {
    fprintf(stderr, "linkedValue failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  ordersOpts.addLinkedProjection(link0);
  ordersOpts.addLinkedProjection(link1);
  ordersOpts.addLinkedProjection(link2);

  const NdbQueryLookupOperationDef *ordersOp =
      qb->readTuple(ordersTab, ordersKey, &ordersOpts);
  if (ordersOp == nullptr) {
    fprintf(stderr, "readTuple(orders) failed: %s\n",
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

  std::map<std::string, DateScanResult> ndbResults;
  Uint32 groupCount = 0;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column modeCol = rec.FetchGroupbyColumn();
    int modeLen = (int)modeCol.byte_size();
    const char *modePtr = modeCol.data();
    int effLen = (int)strnlen(modePtr, modeLen);
    while (effLen > 0 && modePtr[effLen - 1] == ' ')
      effLen--;
    std::string shipmode(modePtr, effLen);

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    NdbAggregator::Result cntRes = rec.FetchAggregationResult();

    DateScanResult r;
    r.sum = sumRes.data_double();
    r.count = cntRes.data_int64();

    ndbResults[shipmode] = r;
    groupCount++;

    V("  %-10s SUM=%.2f COUNT=%lld\n",
      shipmode.c_str(), r.sum, (long long)r.count);
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
            "SELECT l.l_shipmode, "
            "SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, "
            "COUNT(*) "
            "FROM tpch_lineitem l "
            "JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey "
            "WHERE l.l_shipdate BETWEEN '1994-01-01' AND '1994-12-31' "
            "GROUP BY l.l_shipmode "
            "ORDER BY l.l_shipmode") != 0) {
      fprintf(stderr, "SQL failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        std::map<std::string, DateScanResult> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1] && row[2]) {
            std::string mode(row[0]);
            while (!mode.empty() && mode.back() == ' ') mode.pop_back();
            DateScanResult r;
            r.sum = atof(row[1]);
            r.count = atoll(row[2]);
            sqlResults[mode] = r;
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
          const DateScanResult &n = ndbResults[key];
          const DateScanResult &s = sqlResults[key];

          double tol = std::max(0.01, fabs(s.sum) * 1e-9);
          if (fabs(n.sum - s.sum) > tol || n.count != s.count) {
            fprintf(stderr, "FAIL: '%s': ndb(%.2f,%lld) sql(%.2f,%lld)\n",
                    key.c_str(), n.sum, (long long)n.count,
                    s.sum, (long long)s.count);
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

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("bench_datescan_ndbapi: DATE index scan benchmark (NDB API)\n");
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
    ssize_t written = write(mtr_fd, "PASSED\n", 7);
    if (written != 7) result = 1;
  }
  close(mtr_fd);

  return result;
}
