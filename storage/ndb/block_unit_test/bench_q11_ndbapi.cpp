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
 * bench_q11_ndbapi — TPC-H Q11 style benchmark: 3-table join, SUM of product,
 *                     CHAR filter, high-cardinality INT GROUP BY.
 *
 * SQL QUERY:
 *   SELECT ps.ps_partkey,
 *          SUM(ps.ps_supplycost * ps.ps_availqty) AS value
 *   FROM tpch_partsupp ps
 *     JOIN tpch_supplier s ON s.s_suppkey = ps.ps_suppkey
 *     JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey
 *   WHERE n.n_name = 'GERMANY'
 *   GROUP BY ps.ps_partkey
 *   ORDER BY value DESC
 *
 * JOIN TREE:
 *   Node 0: partsupp  (scanTable, root)
 *     +- Node 1: supplier  (readTuple, key: partsupp.ps_suppkey)
 *          +- Node 2: nation    (readTuple, key: supplier.s_nationkey,
 *                                 filter: n_name = 'GERMANY',
 *                                 setParent(supplier), aggregate leaf)
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

static int
runBenchmark(Ndb *ndb, MYSQL *mysqlConn, int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();

  dict->invalidateTable("tpch_partsupp");
  dict->invalidateTable("tpch_supplier");
  dict->invalidateTable("tpch_nation");

  const NdbDictionary::Table *partsuppTab = dict->getTable("tpch_partsupp");
  const NdbDictionary::Table *supplierTab = dict->getTable("tpch_supplier");
  const NdbDictionary::Table *nationTab = dict->getTable("tpch_nation");

  if (partsuppTab == nullptr || supplierTab == nullptr ||
      nationTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *partkeyCol =
      partsuppTab->getColumn("ps_partkey");
  const NdbDictionary::Column *supplycostCol =
      partsuppTab->getColumn("ps_supplycost");
  const NdbDictionary::Column *availqtyCol =
      partsuppTab->getColumn("ps_availqty");

  if (partkeyCol == nullptr || supplycostCol == nullptr ||
      availqtyCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* Build aggregation on nation (leaf) */
  NdbAggregator agg(nationTab);
  if (!agg.GroupByLinked(0, partkeyCol) ||              /* GROUP BY ps_partkey (linked pos 0) */
      !agg.LoadLinkedColumn(1, 0, supplycostCol) ||     /* reg0 = ps_supplycost */
      !agg.LoadLinkedColumn(2, 1, availqtyCol) ||       /* reg1 = ps_availqty */
      !agg.Mul(0, 1) ||                                 /* reg0 = supplycost * availqty */
      !agg.Sum(0, 0) ||                                 /* agg[0] = SUM(reg0) */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /* Build CHAR equality filter: n_name = 'GERMANY' (CHAR(25)) */
  NdbInterpretedCode filter(nationTab);
  char GERMANY_PADDED[25];
  memset(GERMANY_PADDED, ' ', 25);
  memcpy(GERMANY_PADDED, "GERMANY", 7);
  Uint32 nNameAttrId = nationTab->getColumn("n_name")->getColumnNo();
  filter.branch_col_eq(GERMANY_PADDED, 25, nNameAttrId, 0);
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

  /* Node 0: scan partsupp (root) */
  const NdbQueryTableScanOperationDef *partsuppOp = qb->scanTable(partsuppTab);
  if (partsuppOp == nullptr) {
    fprintf(stderr, "scanTable(partsupp) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 1: lookup supplier */
  const NdbQueryOperand *supplierKey[] = {
    qb->linkedValue(partsuppOp, "ps_suppkey"),
    nullptr
  };
  NdbQueryOptions supplierOpts;
  supplierOpts.setMatchType(NdbQueryOptions::MatchNonNull);

  const NdbQueryLookupOperationDef *supplierOp =
      qb->readTuple(supplierTab, supplierKey, &supplierOpts);
  if (supplierOp == nullptr) {
    fprintf(stderr, "readTuple(supplier) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup nation (parent: supplier, filter, aggregate leaf) */
  const NdbQueryOperand *nationKey[] = {
    qb->linkedValue(supplierOp, "s_nationkey"),
    nullptr
  };
  NdbQueryOptions nationOpts;
  nationOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  nationOpts.setParent(supplierOp);
  nationOpts.setInterpretedCode(filter);
  nationOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(partsuppOp, "ps_partkey");    /* pos 0: GROUP BY */
  const NdbLinkedOperand *link1 =
      qb->linkedValue(partsuppOp, "ps_supplycost"); /* pos 1: computation */
  const NdbLinkedOperand *link2 =
      qb->linkedValue(partsuppOp, "ps_availqty");   /* pos 2: computation */

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

  std::map<Int32, double> ndbResults;
  Uint32 groupCount = 0;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column pkCol = rec.FetchGroupbyColumn();
    Int32 partkey = pkCol.data_int32();

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    double value = sumRes.data_double();

    ndbResults[partkey] = value;
    groupCount++;

    V("  partkey=%d value=%.2f\n", partkey, value);
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
            "SELECT ps.ps_partkey, "
            "SUM(ps.ps_supplycost * ps.ps_availqty) AS value "
            "FROM tpch_partsupp ps "
            "JOIN tpch_supplier s ON s.s_suppkey = ps.ps_suppkey "
            "JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey "
            "WHERE n.n_name = 'GERMANY' "
            "GROUP BY ps.ps_partkey "
            "ORDER BY value DESC") != 0) {
      fprintf(stderr, "SQL failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        std::map<Int32, double> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1]) {
            Int32 pk = atoi(row[0]);
            double val = atof(row[1]);
            sqlResults[pk] = val;
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
            fprintf(stderr, "FAIL: partkey=%d: missing in %s\n",
                    key, hasNdb ? "sql" : "ndbapi");
            failures++;
            continue;
          }
          double n = ndbResults[key];
          double s = sqlResults[key];
          double tol = std::max(0.01, fabs(s) * 1e-9);
          if (fabs(n - s) > tol) {
            fprintf(stderr, "FAIL: partkey=%d: ndb=%.2f sql=%.2f\n",
                    key, n, s);
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

  printf("bench_q11_ndbapi: TPC-H Q11 style benchmark (NDB API)\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Iterations: %d\n\n", numIterations);

  ndb_init();
  int result = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(30, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm %s: %s\n",
              connectString, clusterConn.get_latest_error_msg());
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
