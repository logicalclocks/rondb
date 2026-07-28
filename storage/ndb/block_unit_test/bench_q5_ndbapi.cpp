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
 * bench_q5_ndbapi — TPC-H Q5 style benchmark: deep 5-table join chain,
 *                    CHAR filter on region, SUM of revenue, GROUP BY nation.
 *
 * SQL QUERY:
 *   SELECT n.n_name,
 *          SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
 *   FROM tpch_lineitem l
 *     JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey
 *     JOIN tpch_customer c ON c.c_custkey = o.o_custkey
 *     JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey
 *     JOIN tpch_region r ON r.r_regionkey = n.n_regionkey
 *   WHERE r.r_name = 'ASIA'
 *   GROUP BY n.n_name
 *   ORDER BY n.n_name
 *
 * JOIN TREE:
 *   Node 0: lineitem  (scanTable, root)
 *     +- Node 1: orders    (readTuple, key: lineitem.l_orderkey)
 *          +- Node 2: customer  (readTuple, key: orders.o_custkey,
 *          |                      setParent(orders))
 *               +- Node 3: nation    (readTuple, key: customer.c_nationkey,
 *               |                      setParent(customer))
 *                    +- Node 4: region    (readTuple, key: nation.n_regionkey,
 *                                          filter: r_name = 'ASIA',
 *                                          setParent(nation), aggregate leaf)
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

  dict->invalidateTable("tpch_lineitem");
  dict->invalidateTable("tpch_orders");
  dict->invalidateTable("tpch_customer");
  dict->invalidateTable("tpch_nation");
  dict->invalidateTable("tpch_region");

  const NdbDictionary::Table *lineitemTab = dict->getTable("tpch_lineitem");
  const NdbDictionary::Table *ordersTab = dict->getTable("tpch_orders");
  const NdbDictionary::Table *customerTab = dict->getTable("tpch_customer");
  const NdbDictionary::Table *nationTab = dict->getTable("tpch_nation");
  const NdbDictionary::Table *regionTab = dict->getTable("tpch_region");

  if (lineitemTab == nullptr || ordersTab == nullptr ||
      customerTab == nullptr || nationTab == nullptr ||
      regionTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  const NdbDictionary::Column *n_nameCol =
      nationTab->getColumn("n_name");
  const NdbDictionary::Column *extendedpriceCol =
      lineitemTab->getColumn("l_extendedprice");
  const NdbDictionary::Column *discountCol =
      lineitemTab->getColumn("l_discount");

  if (n_nameCol == nullptr || extendedpriceCol == nullptr ||
      discountCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* Build aggregation on region (leaf) */
  NdbAggregator agg(regionTab);
  if (!agg.GroupByLinked(0, n_nameCol) ||               /* GROUP BY n_name (linked pos 0) */
      !agg.LoadLinkedColumn(1, 0, extendedpriceCol) ||  /* reg0 = l_extendedprice */
      !agg.LoadDouble(1.0, 1) ||                        /* reg1 = 1.0 */
      !agg.LoadLinkedColumn(2, 2, discountCol) ||       /* reg2 = l_discount */
      !agg.Minus(1, 2) ||                               /* reg1 = 1 - discount */
      !agg.Mul(0, 1) ||                                 /* reg0 = revenue */
      !agg.Sum(0, 0) ||                                 /* agg[0] = SUM(revenue) */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /* Build CHAR equality filter: r_name = 'ASIA' (CHAR(25)) */
  NdbInterpretedCode filter(regionTab);
  char ASIA_PADDED[25];
  memset(ASIA_PADDED, ' ', 25);
  memcpy(ASIA_PADDED, "ASIA", 4);
  Uint32 rNameAttrId = regionTab->getColumn("r_name")->getColumnNo();
  filter.branch_col_eq(ASIA_PADDED, 25, rNameAttrId, 0);
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

  /* Node 3: lookup nation (parent: customer) */
  const NdbQueryOperand *nationKey[] = {
    qb->linkedValue(customerOp, "c_nationkey"),
    nullptr
  };
  NdbQueryOptions nationOpts;
  nationOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  nationOpts.setParent(customerOp);

  const NdbQueryLookupOperationDef *nationOp =
      qb->readTuple(nationTab, nationKey, &nationOpts);
  if (nationOp == nullptr) {
    fprintf(stderr, "readTuple(nation) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 4: lookup region (parent: nation, filter, aggregate leaf) */
  const NdbQueryOperand *regionKey[] = {
    qb->linkedValue(nationOp, "n_regionkey"),
    nullptr
  };
  NdbQueryOptions regionOpts;
  regionOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  regionOpts.setParent(nationOp);
  regionOpts.setInterpretedCode(filter);
  regionOpts.setAggregation(agg);

  const NdbLinkedOperand *link0 =
      qb->linkedValue(nationOp, "n_name");              /* pos 0: GROUP BY */
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
  regionOpts.addLinkedProjection(link0);
  regionOpts.addLinkedProjection(link1);
  regionOpts.addLinkedProjection(link2);

  const NdbQueryLookupOperationDef *regionOp =
      qb->readTuple(regionTab, regionKey, &regionOpts);
  if (regionOp == nullptr) {
    fprintf(stderr, "readTuple(region) failed: %s\n",
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

  std::map<std::string, double> ndbResults;
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

    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    double revenue = sumRes.data_double();

    ndbResults[nation] = revenue;
    groupCount++;

    V("  %-25s revenue=%.2f\n", nation.c_str(), revenue);
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
            "SELECT n.n_name, "
            "SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue "
            "FROM tpch_lineitem l "
            "JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey "
            "JOIN tpch_customer c ON c.c_custkey = o.o_custkey "
            "JOIN tpch_nation n ON n.n_nationkey = c.c_nationkey "
            "JOIN tpch_region r ON r.r_regionkey = n.n_regionkey "
            "WHERE r.r_name = 'ASIA' "
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
        std::map<std::string, double> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1]) {
            std::string name(row[0]);
            while (!name.empty() && name.back() == ' ') name.pop_back();
            sqlResults[name] = atof(row[1]);
          }
        }
        mysql_free_result(res);

        std::set<std::string> allKeys;
        for (const auto &kv : ndbResults) allKeys.insert(kv.first);
        for (const auto &kv : sqlResults) allKeys.insert(kv.first);

        for (const auto &key : allKeys) {
          double n = ndbResults.count(key) ? ndbResults[key] : 0.0;
          double s = sqlResults.count(key) ? sqlResults[key] : 0.0;
          double tol = std::max(0.01, fabs(s) * 1e-9);
          if (fabs(n - s) > tol) {
            fprintf(stderr, "FAIL: '%s': ndb=%.2f sql=%.2f\n",
                    key.c_str(), n, s);
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
  const char *connectString = nullptr;
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

  if (connectString == nullptr) connectString = "localhost:1186";

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("bench_q5_ndbapi: TPC-H Q5 style 5-table join benchmark (NDB API)\n");
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
