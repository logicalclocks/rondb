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
 * bench_q9_ndbapi — TPC-H Q9 benchmark using the NDB API with pushdown
 *                    join aggregation via NdbQueryBuilder + NdbAggregator.
 *
 * Same query as bench_q9_dbtc but uses the high-level NDB API instead of
 * raw SignalSender signals. Results are verified against MySQL.
 *
 * Assumes tables are already loaded by load_tpch in the "test" database.
 *
 * Usage: bench_q9_ndbapi -c <connect_string> -m <mysql_port> [options]
 *
 * =====================================================================
 * SQL QUERY
 * =====================================================================
 *
 *   SELECT n_name, o_orderyear,
 *          SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity)
 *   FROM tpch_lineitem l
 *     JOIN tpch_part p      ON p_partkey   = l_partkey
 *     JOIN tpch_orders o    ON o_orderkey  = l_orderkey
 *     JOIN tpch_supplier s  ON s_suppkey   = l_suppkey
 *     JOIN tpch_partsupp ps ON ps_partkey  = l_partkey AND ps_suppkey = l_suppkey
 *     JOIN tpch_nation n    ON n_nationkey = s_nationkey
 *   WHERE p_name LIKE '%green%'
 *   GROUP BY n_name, o_orderyear
 *
 * =====================================================================
 * JOIN TREE (linear chain via setParent)
 * =====================================================================
 *
 *   Node 0: lineitem  (scanTable, root)
 *     +- Node 1: part      (readTuple, key: lineitem.l_partkey,
 *     |                      filter: p_name LIKE '%green%')
 *          +- Node 2: orders    (readTuple, key: lineitem.l_orderkey,
 *          |                      setParent(part))
 *               +- Node 3: supplier  (readTuple, key: lineitem.l_suppkey,
 *               |                      setParent(orders))
 *                    +- Node 4: partsupp (readTuple, key: [lineitem.l_partkey,
 *                    |                     lineitem.l_suppkey], setParent(supplier))
 *                         +- Node 5: nation   (readTuple, key: supplier.s_nationkey,
 *                                     setParent(partsupp), aggregate leaf)
 *
 * =====================================================================
 * AGGREGATION (NdbAggregator on nation)
 * =====================================================================
 *
 * GROUP BY: n_name (local), o_orderyear (linked pos 0)
 *
 * Linked projections on nation (via addLinkedProjection):
 *   pos 0: linkedValue(ordersOp, "o_orderyear")     -> GROUP BY
 *   pos 1: linkedValue(lineitemOp, "l_extendedprice") -> computation
 *   pos 2: linkedValue(lineitemOp, "l_discount")     -> computation
 *   pos 3: linkedValue(lineitemOp, "l_quantity")     -> computation
 *   pos 4: linkedValue(partsuppOp, "ps_supplycost")  -> computation
 *
 * Program:
 *   LoadColumn(linked 1, reg0)   // l_extendedprice
 *   LoadDouble(1.0, reg1)        // 1.0
 *   LoadColumn(linked 2, reg2)   // l_discount
 *   Minus(reg1, reg2)            // 1 - discount
 *   Mul(reg0, reg1)              // price * (1 - discount)
 *   LoadColumn(linked 4, reg2)   // ps_supplycost
 *   LoadColumn(linked 3, reg3)   // l_quantity
 *   Mul(reg2, reg3)              // cost * quantity
 *   Minus(reg0, reg2)            // revenue - cost
 *   Sum(0, reg0)                 // agg[0] = SUM(amount)
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

/* ------------------------------------------------------------------ */
/* MySQL helpers                                                       */
/* ------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------ */
/* Benchmark function                                                  */
/* ------------------------------------------------------------------ */

static int
runBenchmark(Ndb *ndb, MYSQL *mysqlConn, int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();

  /* Get all 6 tables */
  dict->invalidateTable("tpch_lineitem");
  dict->invalidateTable("tpch_part");
  dict->invalidateTable("tpch_orders");
  dict->invalidateTable("tpch_supplier");
  dict->invalidateTable("tpch_partsupp");
  dict->invalidateTable("tpch_nation");

  const NdbDictionary::Table *lineitemTab = dict->getTable("tpch_lineitem");
  const NdbDictionary::Table *partTab = dict->getTable("tpch_part");
  const NdbDictionary::Table *ordersTab = dict->getTable("tpch_orders");
  const NdbDictionary::Table *supplierTab = dict->getTable("tpch_supplier");
  const NdbDictionary::Table *partsuppTab = dict->getTable("tpch_partsupp");
  const NdbDictionary::Table *nationTab = dict->getTable("tpch_nation");

  if (lineitemTab == nullptr || partTab == nullptr ||
      ordersTab == nullptr || supplierTab == nullptr ||
      partsuppTab == nullptr || nationTab == nullptr) {
    fprintf(stderr, "Table lookup failed: %s\n"
            "Did you run load_tpch first?\n", dict->getNdbError().message);
    return -1;
  }

  /* Look up columns needed for aggregation GROUP BY and linked projections */
  const NdbDictionary::Column *orderyearCol =
      ordersTab->getColumn("o_orderyear");
  const NdbDictionary::Column *extendedpriceCol =
      lineitemTab->getColumn("l_extendedprice");
  const NdbDictionary::Column *discountCol =
      lineitemTab->getColumn("l_discount");
  const NdbDictionary::Column *quantityCol =
      lineitemTab->getColumn("l_quantity");
  const NdbDictionary::Column *supplycostCol =
      partsuppTab->getColumn("ps_supplycost");

  if (orderyearCol == nullptr || extendedpriceCol == nullptr ||
      discountCol == nullptr || quantityCol == nullptr ||
      supplycostCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }

  /* ---- Build aggregation program ---- */
  NdbAggregator agg(nationTab);
  if (!agg.GroupBy("n_name") ||
      !agg.GroupByLinked(0, orderyearCol) ||
      !agg.LoadLinkedColumn(1, 0, extendedpriceCol) || /* reg0 = l_extendedprice */
      !agg.LoadDouble(1.0, 1) ||                       /* reg1 = 1.0 */
      !agg.LoadLinkedColumn(2, 2, discountCol) ||      /* reg2 = l_discount */
      !agg.Minus(1, 2) ||                              /* reg1 = 1 - discount */
      !agg.Mul(0, 1) ||                                /* reg0 = price * (1-disc) */
      !agg.LoadLinkedColumn(4, 2, supplycostCol) ||    /* reg2 = ps_supplycost */
      !agg.LoadLinkedColumn(3, 3, quantityCol) ||      /* reg3 = l_quantity */
      !agg.Mul(2, 3) ||                                /* reg2 = cost * qty */
      !agg.Minus(0, 2) ||                              /* reg0 = amount */
      !agg.Sum(0, 0) ||                                /* agg[0] = SUM(reg0) */
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /* ---- Build LIKE filter for PART ---- */
  NdbInterpretedCode likeFilter(partTab);
  const char pattern[] = "%green%";
  Uint32 pNameAttrId = partTab->getColumn("p_name")->getColumnNo();
  likeFilter.branch_col_notlike(pattern, (Uint32)strlen(pattern),
                                pNameAttrId, 0);
  likeFilter.interpret_exit_ok();
  likeFilter.def_label(0);
  likeFilter.interpret_exit_nok();
  if (likeFilter.finalise() != 0) {
    fprintf(stderr, "LIKE filter finalise failed: %s\n",
            likeFilter.getNdbError().message);
    return -1;
  }

  /* ---- Build pushed join query via NdbQueryBuilder ---- */
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

  /* Node 1: lookup part (key: lineitem.l_partkey, filter: LIKE '%green%') */
  const NdbQueryOperand *partKey[] = {
    qb->linkedValue(lineitemOp, "l_partkey"),
    nullptr
  };
  NdbQueryOptions partOpts;
  partOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  partOpts.setInterpretedCode(likeFilter);

  const NdbQueryLookupOperationDef *partOp =
      qb->readTuple(partTab, partKey, &partOpts);
  if (partOp == nullptr) {
    fprintf(stderr, "readTuple(part) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 2: lookup orders (key: lineitem.l_orderkey, parent: part) */
  const NdbQueryOperand *ordersKey[] = {
    qb->linkedValue(lineitemOp, "l_orderkey"),
    nullptr
  };
  NdbQueryOptions ordersOpts;
  ordersOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  ordersOpts.setParent(partOp);

  const NdbQueryLookupOperationDef *ordersOp =
      qb->readTuple(ordersTab, ordersKey, &ordersOpts);
  if (ordersOp == nullptr) {
    fprintf(stderr, "readTuple(orders) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 3: lookup supplier (key: lineitem.l_suppkey, parent: orders) */
  const NdbQueryOperand *supplierKey[] = {
    qb->linkedValue(lineitemOp, "l_suppkey"),
    nullptr
  };
  NdbQueryOptions supplierOpts;
  supplierOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  supplierOpts.setParent(ordersOp);

  const NdbQueryLookupOperationDef *supplierOp =
      qb->readTuple(supplierTab, supplierKey, &supplierOpts);
  if (supplierOp == nullptr) {
    fprintf(stderr, "readTuple(supplier) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 4: lookup partsupp (composite key: [lineitem.l_partkey,
   * lineitem.l_suppkey], parent: supplier) */
  const NdbQueryOperand *partsuppKey[] = {
    qb->linkedValue(lineitemOp, "l_partkey"),
    qb->linkedValue(lineitemOp, "l_suppkey"),
    nullptr
  };
  NdbQueryOptions partsuppOpts;
  partsuppOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  partsuppOpts.setParent(supplierOp);

  const NdbQueryLookupOperationDef *partsuppOp =
      qb->readTuple(partsuppTab, partsuppKey, &partsuppOpts);
  if (partsuppOp == nullptr) {
    fprintf(stderr, "readTuple(partsupp) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Node 5: lookup nation (key: supplier.s_nationkey, parent: partsupp,
   * aggregate leaf with linked projections) */
  const NdbQueryOperand *nationKey[] = {
    qb->linkedValue(supplierOp, "s_nationkey"),
    nullptr
  };
  NdbQueryOptions nationOpts;
  nationOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  nationOpts.setParent(partsuppOp);
  nationOpts.setAggregation(agg);

  /* Linked projections: these ancestor columns are passed to the aggregate
   * leaf via the NI_ATTR_LINKED buffer. Position order matches the
   * AGG_LINKED_COL_FLAG references in the aggregation program. */
  const NdbLinkedOperand *link0 =
      qb->linkedValue(ordersOp, "o_orderyear");    /* pos 0: GROUP BY */
  const NdbLinkedOperand *link1 =
      qb->linkedValue(lineitemOp, "l_extendedprice"); /* pos 1: computation */
  const NdbLinkedOperand *link2 =
      qb->linkedValue(lineitemOp, "l_discount");   /* pos 2: computation */
  const NdbLinkedOperand *link3 =
      qb->linkedValue(lineitemOp, "l_quantity");   /* pos 3: computation */
  const NdbLinkedOperand *link4 =
      qb->linkedValue(partsuppOp, "ps_supplycost"); /* pos 4: computation */

  if (link0 == nullptr || link1 == nullptr || link2 == nullptr ||
      link3 == nullptr || link4 == nullptr) {
    fprintf(stderr, "linkedValue for nation projections failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  nationOpts.addLinkedProjection(link0);
  nationOpts.addLinkedProjection(link1);
  nationOpts.addLinkedProjection(link2);
  nationOpts.addLinkedProjection(link3);
  nationOpts.addLinkedProjection(link4);

  const NdbQueryLookupOperationDef *nationOp =
      qb->readTuple(nationTab, nationKey, &nationOpts);
  if (nationOp == nullptr) {
    fprintf(stderr, "readTuple(nation) failed: %s\n",
            qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  /* Prepare query definition */
  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    fprintf(stderr, "prepare() failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  V("  Query prepared: %u operations, isScan=%d\n",
    queryDef->getNoOfOperations(), queryDef->isScanQuery());

  /* ---- Execute query ---- */
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

  /* Do NOT call getValue() on aggregate query operations.
   * FLUSH_AI is suppressed for non-leaf aggregate nodes (including the root),
   * so PI_ATTR_LIST columns are prepended to NI_LINKED_ATTR columns in the
   * TRANSID_AI response, shifting col() indices used by child key patterns. */

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    fprintf(stderr, "execute failed: %s\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  auto t1 = Clock::now();

  /* Consume all scan batches */
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

  /* ---- Retrieve aggregated results ---- */
  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    fprintf(stderr, "getAggregator returned nullptr\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  std::map<std::pair<std::string, int>, double> ndbResults;
  Uint32 groupCount = 0;

  for (;;) {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) break;

    /* GROUP BY col 0: n_name CHAR(25) */
    NdbAggregator::Column nameCol = rec.FetchGroupbyColumn();
    int nameLen = (int)nameCol.byte_size();
    const char *namePtr = nameCol.data();
    /* Find effective string length: CHAR columns may contain data past the
       first null byte (NDB stores exactly N bytes).  Match MySQL behavior
       by treating the first null as the string terminator, then trim
       trailing spaces. */
    int effLen = (int)strnlen(namePtr, nameLen);
    while (effLen > 0 && namePtr[effLen - 1] == ' ')
      effLen--;
    std::string nation(namePtr, effLen);

    /* GROUP BY col 1: o_orderyear INT (linked) */
    NdbAggregator::Column yearCol = rec.FetchGroupbyColumn();
    Int32 year = yearCol.data_int32();

    /* agg[0]: SUM(amount) as DOUBLE */
    NdbAggregator::Result sumRes = rec.FetchAggregationResult();
    double amount = sumRes.data_double();

    ndbResults[{nation, year}] += amount;
    groupCount++;

    V("  %-15s %d: %.2f\n", nation.c_str(), year, amount);
  }

  query->close();
  trans->close();

  auto t3 = Clock::now();

  /* ---- MySQL verification ---- */
  double sqlMs = 0;
  int failures = 0;

  if (mysqlConn != nullptr) {
    auto tSql0 = Clock::now();
    if (mysql_query(mysqlConn,
            "SELECT n.n_name, o.o_orderyear, "
            "SUM(l.l_extendedprice * (1 - l.l_discount) "
            "- ps.ps_supplycost * l.l_quantity) AS amount "
            "FROM tpch_lineitem l "
            "JOIN tpch_part p ON p.p_partkey = l.l_partkey "
            "JOIN tpch_orders o ON o.o_orderkey = l.l_orderkey "
            "JOIN tpch_supplier s ON s.s_suppkey = l.l_suppkey "
            "JOIN tpch_partsupp ps ON ps.ps_partkey = l.l_partkey "
            "AND ps.ps_suppkey = l.l_suppkey "
            "JOIN tpch_nation n ON n.n_nationkey = s.s_nationkey "
            "WHERE p.p_name LIKE '%%green%%' "
            "GROUP BY n.n_name, o.o_orderyear "
            "ORDER BY n.n_name, o.o_orderyear") != 0) {
      fprintf(stderr, "SQL Q9 failed: %s\n", mysql_error(mysqlConn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(mysqlConn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(mysqlConn));
        failures++;
      } else {
        std::map<std::pair<std::string, int>, double> sqlResults;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res)) != nullptr) {
          if (row[0] && row[1] && row[2]) {
            std::string nation(row[0]);
            while (!nation.empty() && nation.back() == ' ') nation.pop_back();
            int year = atoi(row[1]);
            double amount = atof(row[2]);
            sqlResults[{nation, year}] = amount;
          }
        }
        mysql_free_result(res);

        /* Compare NDB API results with SQL results */
        std::set<std::pair<std::string, int>> allKeys;
        for (const auto &kv : ndbResults) allKeys.insert(kv.first);
        for (const auto &kv : sqlResults) allKeys.insert(kv.first);

        for (const auto &key : allKeys) {
          double pushdown = ndbResults.count(key) ? ndbResults[key] : 0.0;
          double sql = sqlResults.count(key) ? sqlResults[key] : 0.0;
          double diff = fabs(pushdown - sql);
          double tol = std::max(0.01, fabs(sql) * 1e-9);
          if (diff > tol) {
            fprintf(stderr, "FAIL: %s/%d: ndbapi=%.2f sql=%.2f diff=%.2f\n",
                    key.first.c_str(), key.second, pushdown, sql, diff);
            failures++;
          }
        }

        if (failures == 0) {
          V("  SQL verify: %zu groups — all match\n", sqlResults.size());
        } else {
          fprintf(stderr, "  SQL verify: %d mismatches out of %zu groups\n",
                  failures, allKeys.size());
        }
      }
    }
    auto tSql1 = Clock::now();
    sqlMs = elapsedMs(tSql0, tSql1);
  }

  /* ---- Timing ---- */
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

  if (verbose) {
    for (const auto &kv : ndbResults) {
      printf("    %-15s %d: %.2f\n",
             kv.first.first.c_str(), kv.first.second, kv.second);
    }
  }

  queryDef->destroy();
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

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

  printf("bench_q9_ndbapi: TPC-H Q9 6-table pushdown join benchmark (NDB API)\n");
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
