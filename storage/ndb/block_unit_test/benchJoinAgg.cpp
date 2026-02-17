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
 * benchJoinAgg — TPC-H Q12-inspired two-table join aggregation benchmark.
 *
 * Exercises pushdown join aggregation at the signal level, implementing a
 * two-table join between LINEITEM and ORDERS using linked attributes.
 *
 * Join tree:
 *   TreeNode 0: LINEITEM (root scan via NDB API) — intermediate
 *       |
 *       +-- linked attrs: l_orderkey (join key), l_shipmode (GROUP BY)
 *           |
 *           +-- TreeNode 1: ORDERS (LQHKEYREQ, leaf) — aggregation here
 *
 * Simplified TPC-H Q12:
 *   SELECT l_shipmode, SUM(o_orderpriority)
 *   FROM bench_lineitem l, bench_orders o
 *   WHERE l.l_orderkey = o.o_orderkey
 *   GROUP BY l_shipmode
 *
 * Key innovation: LINEITEM's l_shipmode flows to ORDERS via AttrInfo
 * linked-attr section (cinBuffer[4] = RsubLen). The aggregation program's
 * GROUP BY column uses bit-15 (0x8000 | attr_id) to read from the linked
 * buffer instead of the local table.
 *
 * Usage: benchJoinAgg -c <connect_string> [options]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include "../src/ndbapi/SignalSender.hpp"

#include <kernel/BlockNumbers.h>
#include <kernel/GlobalSignalNumbers.h>
#include <kernel/RefConvert.hpp>
#include <kernel/signaldata/JoinAgg.hpp>
#include <kernel/signaldata/ScanFrag.hpp>
#include <kernel/signaldata/TransIdAI.hpp>
#include <kernel/signaldata/LqhKey.hpp>
#include <kernel/signaldata/DumpStateOrd.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>

#include <NdbRestarter.hpp>
#include <util/rondb_hash.hpp>
#include <mysql.h>

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <map>
#include <set>
#include <vector>

/* ------------------------------------------------------------------ */
/* Verbose output control                                              */
/* ------------------------------------------------------------------ */

static bool verbose = false;
static MYSQL *g_mysql_conn = nullptr;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *LINEITEM_TABLE = "bench_lineitem";
static const char *ORDERS_TABLE   = "bench_orders";

static const Uint32 FAKE_TRANS_ID1 = 0xBE4C0001;
static const Uint32 FAKE_TRANS_ID2 = 0xBE4C0002;
static const Uint32 FAKE_REQUEST_ID = 2001;
static const Uint32 FAKE_SENDER_DATA = 99;
static const Uint32 WAIT_TIMEOUT_MS = 30000;

/* NDB types — BIGINT used throughout for simplicity */
static const Uint32 COL_TYPE_BIGINT = 9;   /* NDB_TYPE_BIGINT */
static const Uint32 INTERPRETER_EXIT_OK = 18;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;

/* Linked column flag: bit 15 set in kOpLoadCol colId or GROUP BY attrId */
static const Uint32 LINKED_COL_FLAG = 0x8000;

/* ------------------------------------------------------------------ */
/* Timing helper and statistics                                        */
/* ------------------------------------------------------------------ */

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

static double elapsedMs(TimePoint start, TimePoint end)
{
  return std::chrono::duration<double, std::milli>(end - start).count();
}

struct BenchStats {
  double setupMs;
  double lookupMs;
  double completeMs;
  double releaseMs;
  double totalMs;
  Uint32 lookupCount;
  double lookupOpsPerSec;
};

static void
printSummary(const std::vector<BenchStats> &runs)
{
  if (runs.empty()) return;
  size_t n = runs.size();

  auto stats = [&](auto getter, const char *label) {
    double sum = 0, mn = 1e18, mx = -1e18;
    for (const auto &r : runs) {
      double v = getter(r);
      sum += v; if (v < mn) mn = v; if (v > mx) mx = v;
    }
    double mean = sum / n;
    double var = 0;
    for (const auto &r : runs) {
      double d = getter(r) - mean;
      var += d * d;
    }
    double sd = (n > 1) ? sqrt(var / (n - 1)) : 0;
    printf("  %-12s  mean=%8.2f  min=%8.2f  max=%8.2f  stddev=%6.2f\n",
           label, mean, mn, mx, sd);
  };

  printf("\n=== Summary (%zu iterations) ===\n", n);
  stats([](const BenchStats &r){ return r.setupMs; }, "Setup ms:");
  stats([](const BenchStats &r){ return r.lookupMs; }, "Lookup ms:");
  stats([](const BenchStats &r){ return r.completeMs; }, "Complete ms:");
  stats([](const BenchStats &r){ return r.releaseMs; }, "Release ms:");
  stats([](const BenchStats &r){ return r.totalMs; }, "Total ms:");
  stats([](const BenchStats &r){ return r.lookupOpsPerSec; }, "Lookup ops/s:");
}

/* ------------------------------------------------------------------ */
/* Table metadata                                                      */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
  std::vector<Uint32> fragInstances;
  std::map<std::string, Uint32> attrIds;
};

/* ------------------------------------------------------------------ */
/* Aggregation program builder                                         */
/* ------------------------------------------------------------------ */

/*
 * Build aggregation program for ORDERS (leaf table):
 *
 *   GROUP BY l_shipmode   — linked column (from LINEITEM parent, bit 15)
 *   SUM(o_orderpriority)  — local column of ORDERS
 *
 * The l_shipmode value arrives in the linked attribute buffer.
 * GROUP BY column descriptor uses (0x8000 | l_shipmode_attr_id) << 16
 * so AggInterpreter reads the group key from the linked buffer.
 *
 * kOpLoadCol for SUM uses the local ORDERS column (no bit 15).
 */
static std::vector<Uint32>
buildAggProgram_Q12(Uint32 /*linkedShipmodeAttrId*/, Uint32 localPriorityAttrId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  /* Word 0: magic + total length */
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;

  /* Word 1: n_gb_cols=1, n_agg_results=1 */
  prog[1] = (1u << 16) | 1u;

  /* Word 2: version */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;

  /* Words 3-7: reserved */
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /* Word 8: GROUP BY l_shipmode — linked column from parent LINEITEM.
   * Bit 15 set signals linked-column resolution by position.
   * Format: (0x8000 | position_in_linked_buffer) << 16 */
  prog[8] = (LINKED_COL_FLAG | 0) << 16;  /* linked pos 0 = l_shipmode */

  /* Word 9: kOpLoadCol(type=BIGINT, reg=0, colId=o_orderpriority)
   * Local column of ORDERS — no bit 15. */
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             localPriorityAttrId;

  /* Word 10: kOpSum(reg=0, agg_idx=0) → SUM(o_orderpriority) */
  prog[10] = (kOpSum << 26) | (0 << 16) | 0;

  return prog;
}

/* ------------------------------------------------------------------ */
/* AttrInfo builder with linked attributes                             */
/* ------------------------------------------------------------------ */

/*
 * Build AttrInfo for LQHKEYREQ with linked attribute data.
 *
 * cinBuffer layout:
 *   [0] = initial read length (0)
 *   [1] = interpreter program length (1 = ExitOK)
 *   [2] = subroutine length (0)
 *   [3] = final read length (0)
 *   [4] = linked attr section length (RsubLen) in words
 *   [5] = ExitOK instruction
 *   [6..] = linked attribute data (AttributeHeader + data pairs)
 *
 * For a single BIGINT linked attribute (l_shipmode):
 *   [6] = AttributeHeader: (attrId << 16) | 8  (8 bytes for BIGINT)
 *   [7] = low 4 bytes of Int64
 *   [8] = high 4 bytes of Int64
 *   Total linked attr len = 3 words
 */
static std::vector<Uint32>
buildAttrInfoWithLinked(Uint32 linkedAttrId, Int64 linkedValue)
{
  const Uint32 LINKED_WORDS = 3;  /* 1 header + 2 data words for BIGINT */
  std::vector<Uint32> ai(6 + 1 + LINKED_WORDS);

  ai[0] = 0;              /* initial read section length */
  ai[1] = 1;              /* interpreter program length (ExitOK) */
  ai[2] = 0;              /* subroutine length */
  ai[3] = 0;              /* final read section length */
  ai[4] = 1 + LINKED_WORDS;  /* linked attr section length (paramLen + data) */
  ai[5] = INTERPRETER_EXIT_OK;

  /* paramLen word — DBSPJ prepends this via T_ATTRINFO_CONSTRUCTED;
   * the kernel skips it (sub_start + 1) before passing to AggInterpreter */
  ai[6] = LINKED_WORDS;

  /* Linked attribute: AttributeHeader + BIGINT data */
  ai[7] = (linkedAttrId << 16) | 8;  /* attrId << 16, byteSize = 8 */
  Uint32 valWords[2];
  memcpy(valWords, &linkedValue, sizeof(Int64));
  ai[8] = valWords[0];
  ai[9] = valWords[1];

  return ai;
}

/* ------------------------------------------------------------------ */
/* Table setup via MySQL                                               */
/* ------------------------------------------------------------------ */

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
loadTableMeta(Ndb *ndb, const char *tableName, TableMeta &meta)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(tableName);
  const NdbDictionary::Table *ptab = dict->getTable(tableName);
  if (ptab == nullptr) {
    fprintf(stderr, "getTable(%s) failed: %s\n",
            tableName, dict->getNdbError().message);
    return -1;
  }

  meta.tableId = ptab->getObjectId();
  meta.schemaVersion = ptab->getObjectVersion();
  meta.fragCount = ptab->getFragmentCount();

  for (int i = 0; i < ptab->getNoOfColumns(); i++) {
    const NdbDictionary::Column *col = ptab->getColumn(i);
    meta.attrIds[col->getName()] = col->getAttrId();
  }

  meta.fragNodes.resize(meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nodeId = 0;
    ptab->getFragmentNodes(f, &nodeId, 1);
    meta.fragNodes[f] = nodeId;
  }

  V("Table '%s': id=%u frags=%u\n", tableName, meta.tableId, meta.fragCount);
  return 0;
}

static int
createLineitemTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS bench_lineitem");
  if (sqlExec(conn,
        "CREATE TABLE bench_lineitem ("
        "  l_orderkey BIGINT NOT NULL,"
        "  l_linenumber BIGINT NOT NULL,"
        "  l_shipmode BIGINT NOT NULL,"
        "  PRIMARY KEY (l_orderkey, l_linenumber)"
        ") ENGINE=NDB") != 0)
    return -1;

  if (loadTableMeta(ndb, LINEITEM_TABLE, meta) != 0) return -1;

  V("Table '%s': id=%u frags=%u l_orderkey=%u l_linenumber=%u l_shipmode=%u\n",
    LINEITEM_TABLE, meta.tableId, meta.fragCount,
    meta.attrIds["l_orderkey"], meta.attrIds["l_linenumber"],
    meta.attrIds["l_shipmode"]);
  return 0;
}

static int
createOrdersTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS bench_orders");
  if (sqlExec(conn,
        "CREATE TABLE bench_orders ("
        "  o_orderkey BIGINT NOT NULL PRIMARY KEY,"
        "  o_orderpriority BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (loadTableMeta(ndb, ORDERS_TABLE, meta) != 0) return -1;

  V("Table '%s': id=%u frags=%u o_orderkey=%u o_orderpriority=%u\n",
    ORDERS_TABLE, meta.tableId, meta.fragCount,
    meta.attrIds["o_orderkey"], meta.attrIds["o_orderpriority"]);
  return 0;
}

static int
queryFragInstances(int mysqlPort, const char *tableName, TableMeta &meta)
{
  const int MAX_RETRIES = 5;

  for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      V("  Retry %d/%d for %s fragment instances...\n",
        attempt, MAX_RETRIES - 1, tableName);
      NdbSleep_SecSleep(1);
    }

    MYSQL *conn = mysql_init(nullptr);
    if (conn == nullptr) { fprintf(stderr, "mysql_init failed\n"); return -1; }

    if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                           "ndbinfo", mysqlPort, nullptr, 0) == nullptr) {
      fprintf(stderr, "mysql_real_connect: %s\n", mysql_error(conn));
      mysql_close(conn);
      continue;
    }

    char query[256];
    snprintf(query, sizeof(query),
             "SELECT node_id, fragment_num, block_instance "
             "FROM ndbinfo.operations_per_fragment "
             "WHERE table_id = %u ORDER BY node_id, fragment_num",
             meta.tableId);

    if (mysql_query(conn, query) != 0) {
      fprintf(stderr, "mysql_query: %s\n", mysql_error(conn));
      mysql_close(conn);
      continue;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    if (result == nullptr) {
      fprintf(stderr, "mysql_store_result: %s\n", mysql_error(conn));
      mysql_close(conn);
      continue;
    }

    std::map<std::pair<Uint32,Uint32>, Uint32> instMap;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      instMap[{(Uint32)atoi(row[0]), (Uint32)atoi(row[1])}] = (Uint32)atoi(row[2]);
    }
    mysql_free_result(result);
    mysql_close(conn);

    /* Check if we got all fragments */
    bool complete = true;
    meta.fragInstances.resize(meta.fragCount);
    for (Uint32 f = 0; f < meta.fragCount; f++) {
      auto it = instMap.find({meta.fragNodes[f], f});
      if (it == instMap.end()) {
        V("  Missing LDM instance for %s node %u frag %u (got %zu entries)\n",
          tableName, meta.fragNodes[f], f, instMap.size());
        complete = false;
        break;
      }
      meta.fragInstances[f] = it->second;
    }

    if (complete) {
      for (Uint32 f = 0; f < meta.fragCount; f++) {
        V("  %s frag %u → node %u, LDM %u\n",
          tableName, f, meta.fragNodes[f], meta.fragInstances[f]);
      }
      return 0;
    }
  }

  fprintf(stderr, "Failed to get LDM instances for %s after %d attempts\n",
          tableName, MAX_RETRIES);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Data loading                                                        */
/* ------------------------------------------------------------------ */

struct LineitemRow {
  Int64 l_orderkey;
  Int64 l_linenumber;
  Int64 l_shipmode;
};

static int
insertOrders(Ndb *ndb, Uint32 numOrders)
{
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(ORDERS_TABLE);
  if (ptab == nullptr) return -1;

  const Uint32 BATCH = 100;
  for (Uint32 s = 0; s < numOrders; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }
    Uint32 e = std::min(s + BATCH, numOrders);
    for (Uint32 i = s; i < e; i++) {
      NdbOperation *op = tx->getNdbOperation(ptab);
      if (op == nullptr) { tx->close(); return -1; }
      op->insertTuple();
      Int64 key = (Int64)(i + 1);
      Int64 prio = (Int64)((i % 5) + 1);  /* priority 1..5 */
      op->equal("o_orderkey", key);
      op->setValue("o_orderpriority", prio);
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert orders [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
  }
  V("Inserted %u rows into %s\n", numOrders, ORDERS_TABLE);
  return 0;
}

static int
insertLineitems(Ndb *ndb, Uint32 numOrders, Uint32 linesPerOrder,
                Uint32 numShipmodes,
                std::vector<LineitemRow> &allRows)
{
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(LINEITEM_TABLE);
  if (ptab == nullptr) return -1;

  Uint32 totalRows = numOrders * linesPerOrder;
  allRows.reserve(totalRows);

  const Uint32 BATCH = 100;
  Uint32 count = 0;
  NdbTransaction *tx = nullptr;

  for (Uint32 o = 0; o < numOrders; o++) {
    for (Uint32 l = 0; l < linesPerOrder; l++) {
      if (count % BATCH == 0) {
        if (tx != nullptr) {
          if (tx->execute(NdbTransaction::Commit) != 0) {
            fprintf(stderr, "insert lineitems: %s\n",
                    tx->getNdbError().message);
            tx->close();
            return -1;
          }
          tx->close();
        }
        tx = ndb->startTransaction();
        if (tx == nullptr) {
          fprintf(stderr, "startTransaction: %s\n",
                  ndb->getNdbError().message);
          return -1;
        }
      }

      NdbOperation *op = tx->getNdbOperation(ptab);
      if (op == nullptr) { tx->close(); return -1; }
      op->insertTuple();

      Int64 orderkey = (Int64)(o + 1);
      Int64 linenumber = (Int64)(l + 1);
      Int64 shipmode = (Int64)((o * linesPerOrder + l) % numShipmodes);

      op->equal("l_orderkey", orderkey);
      op->equal("l_linenumber", linenumber);
      op->setValue("l_shipmode", shipmode);

      allRows.push_back({orderkey, linenumber, shipmode});
      count++;
    }
  }

  if (tx != nullptr) {
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert lineitems final: %s\n",
              tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
  }

  V("Inserted %u rows into %s\n", count, LINEITEM_TABLE);
  return 0;
}

static int
dropTable(MYSQL *conn, const char *tableName)
{
  char query[256];
  snprintf(query, sizeof(query), "DROP TABLE IF EXISTS %s", tableName);
  return sqlExec(conn, query);
}

/* ------------------------------------------------------------------ */
/* Signal helpers (reused from testJoinAgg pattern)                    */
/* ------------------------------------------------------------------ */

static SimpleSignal *
waitForSignal(SignalSender &ss, Uint32 timeoutMs, const char *context)
{
  SimpleSignal *sig = ss.waitFor(timeoutMs);
  if (sig == nullptr)
    fprintf(stderr, "TIMEOUT waiting for signal (%s)\n", context);
  return sig;
}

static int getGsn(const SimpleSignal *sig) {
  return sig->header.readSignalNumber();
}

/* ------------------------------------------------------------------ */
/* JOIN_AGG_SETUP / COMPLETE / RELEASE helpers                         */
/* ------------------------------------------------------------------ */

static int
sendSetupReq(SignalSender &ss, Uint32 nodeId,
             const std::vector<Uint32> &aggProgram,
             const TableMeta &leafMeta, Uint32 strategy,
             Uint32 &aggStateKeyOut)
{
  V("  SETUP_REQ → node %u\n", nodeId);

  SimpleSignal ssig;
  JoinAggSetupReq *req =
    reinterpret_cast<JoinAggSetupReq *>(ssig.getDataPtrSend());

  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = FAKE_REQUEST_ID;
  req->transid[0] = FAKE_TRANS_ID1;
  req->transid[1] = FAKE_TRANS_ID2;
  req->tableId = leafMeta.tableId;
  req->expectedOpCount = 0;
  req->concurrencyStrategy = strategy;
  req->resultRef = ss.getOwnRef();
  req->resultData = FAKE_SENDER_DATA;
  req->routeRef = ss.getOwnRef();

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_SETUP_REQ,
           JoinAggSetupReq::SignalLength);
  Uint32 receiverId = FAKE_SENDER_DATA;
  ssig.header.m_noOfSections = 2;
  ssig.ptr[0].p = aggProgram.data();
  ssig.ptr[0].sz = (Uint32)aggProgram.size();
  ssig.ptr[1].p = &receiverId;
  ssig.ptr[1].sz = 1;

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SETUP_REQ failed\n");
    return -1;
  }

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SETUP_CONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_SETUP_CONF) {
    aggStateKeyOut =
      reinterpret_cast<const JoinAggSetupConf *>(resp->getDataPtr())
        ->aggStateKey;
    V("  SETUP_CONF: aggStateKey=%u\n", aggStateKeyOut);
    return 0;
  } else if (gsn == GSN_JOIN_AGG_SETUP_REF) {
    const JoinAggSetupRef *ref =
      reinterpret_cast<const JoinAggSetupRef *>(resp->getDataPtr());
    fprintf(stderr, "SETUP_REF: errorCode=%u errorLine=%u\n",
            ref->errorCode, ref->errorLine);
    return -1;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for SETUP_CONF\n", gsn);
  return -1;
}

static int
sendCompleteReq(SignalSender &ss, Uint32 nodeId,
                Uint32 aggStateKey, Uint32 maxBatchRows)
{
  V("  COMPLETE_REQ → node %u\n", nodeId);

  SimpleSignal ssig;
  JoinAggCompleteReq *req =
    reinterpret_cast<JoinAggCompleteReq *>(ssig.getDataPtrSend());

  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = FAKE_REQUEST_ID;
  req->transid[0] = FAKE_TRANS_ID1;
  req->transid[1] = FAKE_TRANS_ID2;
  req->aggStateKey = aggStateKey;
  req->maxBatchRows = maxBatchRows;

  Uint16 recBlock = numberToBlock(DBLQH, 1);
  ssig.set(ss, 0, recBlock, GSN_JOIN_AGG_COMPLETE_REQ,
           JoinAggCompleteReq::SignalLength);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal COMPLETE_REQ failed\n");
    return -1;
  }
  return 0;
}

static int
sendSendConf(SignalSender &ss, const JoinAggSendReq *sendReq,
             Uint32 maxBatchRows)
{
  SimpleSignal ssig;
  JoinAggSendConf *conf =
    reinterpret_cast<JoinAggSendConf *>(ssig.getDataPtrSend());

  conf->senderRef = ss.getOwnRef();
  conf->senderData = sendReq->senderData;
  conf->requestId = sendReq->requestId;
  conf->aggStateKey = sendReq->aggStateKey;
  conf->maxBatchRows = maxBatchRows;

  ssig.set(ss, 0, refToBlock(sendReq->senderRef),
           GSN_JOIN_AGG_SEND_CONF, JoinAggSendConf::SignalLength);

  Uint32 nodeId = refToNode(sendReq->senderRef);
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SEND_CONF failed\n");
    return -1;
  }
  return 0;
}

struct AggResult {
  Uint32 n_gb_cols;
  Uint32 n_agg_results;
  Uint32 n_groups;
  std::vector<std::pair<std::vector<Uint8>, std::vector<Uint8>>> groups;
};

static int
parseTransIdAI(const SimpleSignal *sig, AggResult &result)
{
  const Uint32 *data;
  Uint32 dataLen;

  if (sig->header.m_noOfSections > 0) {
    data = sig->ptr[0].p;
    dataLen = sig->ptr[0].sz;
  } else {
    data = sig->getDataPtr() + TransIdAI::HeaderLength;
    dataLen = sig->getLength() - TransIdAI::HeaderLength;
  }
  if (dataLen < 4) return -1;

  if (data[0] != ((AGG_RESULT_ATTR << 16) | AGG_MAGIC)) {
    fprintf(stderr, "Bad AGG_RESULT magic: 0x%08x\n", data[0]);
    return -1;
  }

  result.n_gb_cols = data[1] >> 16;
  result.n_agg_results = data[1] & 0xFFFF;
  result.n_groups = data[2];

  Uint32 entries = result.n_groups;
  if (result.n_gb_cols == 0 && entries == 0 && 3 < dataLen) entries = 1;

  Uint32 pos = 3;
  for (Uint32 g = 0; g < entries; g++) {
    if (pos >= dataLen) return -1;
    Uint32 key_len = data[pos] >> 16;
    Uint32 val_len = data[pos] & 0xFFFF;
    pos++;
    Uint32 total_words = (key_len + val_len + 3) / 4;
    if (pos + total_words > dataLen) return -1;

    const Uint8 *raw = reinterpret_cast<const Uint8 *>(&data[pos]);
    result.groups.emplace_back(
      std::vector<Uint8>(raw, raw + key_len),
      std::vector<Uint8>(raw + key_len, raw + key_len + val_len));
    pos += total_words;
  }
  return 0;
}

static int
receiveResults(SignalSender &ss, std::vector<AggResult> &allResults,
               Uint32 &totalGroups)
{
  bool done = false;
  while (!done) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "results");
    if (resp == nullptr) return -1;
    int gsn = getGsn(resp);

    if (gsn == GSN_TRANSID_AI) {
      AggResult r;
      if (parseTransIdAI(resp, r) != 0) return -1;
      totalGroups += r.n_groups;
      allResults.push_back(std::move(r));
    } else if (gsn == GSN_JOIN_AGG_SEND_REQ) {
      if (sendSendConf(ss,
            reinterpret_cast<const JoinAggSendReq *>(resp->getDataPtr()),
            10000) != 0)
        return -1;
    } else if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
      done = true;
    } else if (gsn == GSN_JOIN_AGG_COMPLETE_REF) {
      const JoinAggCompleteRef *ref =
        reinterpret_cast<const JoinAggCompleteRef *>(resp->getDataPtr());
      fprintf(stderr, "COMPLETE_REF: errorCode=%u errorLine=%u\n",
              ref->errorCode, ref->errorLine);
      return -1;
    }
  }
  return 0;
}

static int
sendReleaseReq(SignalSender &ss, Uint32 nodeId, Uint32 aggStateKey)
{
  SimpleSignal ssig;
  JoinAggReleaseReq *req =
    reinterpret_cast<JoinAggReleaseReq *>(ssig.getDataPtrSend());

  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = FAKE_REQUEST_ID;
  req->transid[0] = FAKE_TRANS_ID1;
  req->transid[1] = FAKE_TRANS_ID2;
  req->aggStateKey = aggStateKey;

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_RELEASE_REQ,
           JoinAggReleaseReq::SignalLength);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) return -1;

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "RELEASE_CONF");
  if (resp == nullptr) return -1;
  if (getGsn(resp) != GSN_JOIN_AGG_RELEASE_CONF) return -1;
  V("  RELEASE_CONF from node %u\n", nodeId);
  return 0;
}

/* ------------------------------------------------------------------ */
/* LQHKEYREQ send with linked attributes                              */
/* ------------------------------------------------------------------ */

/*
 * LqhKeyReqBuilder is a friend of LqhKeyReq (declared in LqhKey.hpp),
 * so it can access private members for raw signal construction.
 */
struct LqhKeyReqBuilder {
  static int send(SignalSender &ss,
                  Uint32 nodeId, Uint32 ldmInstance,
                  Uint32 fragId, Uint32 aggStateKey,
                  Uint32 precomputedHash,
                  Int64 keyValue,
                  const TableMeta &leafMeta,
                  const std::vector<Uint32> &attrInfo)
  {
    SimpleSignal ssig;
    LqhKeyReq *req = reinterpret_cast<LqhKeyReq *>(ssig.getDataPtrSend());
    memset(req, 0, sizeof(LqhKeyReq));

    req->clientConnectPtr = fragId;

    Uint32 attrLen = 0;
    LqhKeyReq::setJoinAggFlag(attrLen, 1);
    req->attrLen = attrLen;

    req->hashValue = precomputedHash;

    Uint32 requestInfo = 0;
    LqhKeyReq::setOperation(requestInfo, ZREAD);
    LqhKeyReq::setDirtyFlag(requestInfo, 1);
    LqhKeyReq::setSimpleFlag(requestInfo, 1);
    LqhKeyReq::setInterpretedFlag(requestInfo, 1);
    LqhKeyReq::setNoDiskFlag(requestInfo, 1);
    LqhKeyReq::setNormalProtocolFlag(requestInfo, 1);
    LqhKeyReq::setCorrFactorFlag(requestInfo, 1);
    LqhKeyReq::setKeyLen(requestInfo, 2);
    req->requestInfo = requestInfo;

    req->tcBlockref = ss.getOwnRef();
    req->tableSchemaVersion =
      leafMeta.tableId | (leafMeta.schemaVersion << 16);
    req->fragmentData = fragId;
    req->transId1 = FAKE_TRANS_ID1;
    req->transId2 = FAKE_TRANS_ID2;
    req->savePointId = 0;
    req->scanInfo = 0;

    req->variableData[0] = 0;  /* corrFactorLo */
    req->variableData[1] = 0;  /* corrFactorHi */
    req->variableData[2] = aggStateKey;
    Uint32 sigLen = LqhKeyReq::FixedSignalLength + 3;

    Uint16 recBlock = numberToBlock(V_QUERY, ldmInstance);
    ssig.set(ss, 0, recBlock, GSN_LQHKEYREQ, sigLen);

    Uint32 keyData[2];
    memcpy(keyData, &keyValue, sizeof(Int64));
    ssig.header.m_noOfSections = 2;
    ssig.ptr[0].p = keyData;
    ssig.ptr[0].sz = 2;
    ssig.ptr[1].p = attrInfo.data();
    ssig.ptr[1].sz = (Uint32)attrInfo.size();

    if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal LQHKEYREQ failed\n");
      return -1;
    }
    return 0;
  }
};

static int
waitForLqhKeyConf(SignalSender &ss)
{
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "LQHKEYCONF");
  if (resp == nullptr) return -1;
  int gsn = getGsn(resp);
  if (gsn == GSN_LQHKEYCONF) return 0;
  if (gsn == GSN_LQHKEYREF) {
    fprintf(stderr, "LQHKEYREF: errorCode=%u\n", resp->getDataPtr()[2]);
    return -1;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for LQHKEYCONF\n", gsn);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Result extraction                                                   */
/* ------------------------------------------------------------------ */

static Int64
extractGroupKey(const std::vector<Uint8> &key)
{
  const Uint32 HDR = 4;  /* AttributeHeader */
  if (key.size() < HDR + 8) return 0;
  Int64 v;
  memcpy(&v, key.data() + HDR, sizeof(Int64));
  return v;
}

static Int64
extractSumBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;  /* sizeof(AggResItem) on 64-bit */
  Uint32 off = aggIdx * ITEM_SIZE + 8;
  if (off + 8 > val.size()) return 0;
  Int64 v;
  memcpy(&v, val.data() + off, sizeof(Int64));
  return v;
}

/* ------------------------------------------------------------------ */
/* Main benchmark function                                             */
/* ------------------------------------------------------------------ */

/*
 * Pre-compute hash → (fragId, nodeId, ldmInstance) for each lineitem row
 * so we don't need the NDB dictionary during the SS-locked benchmark loop.
 */
struct LookupTarget {
  Uint32 fragId;
  Uint32 nodeId;
  Uint32 ldmInstance;
  Uint32 hashValue;   /* rondb_calc_hash result for LQHKEYREQ */
};

static int
precomputeTargets(Ndb *ndb,
                  const std::vector<LineitemRow> &rows,
                  const TableMeta &ordersMeta,
                  std::vector<LookupTarget> &targets)
{
  const NdbDictionary::Table *ordersTab =
    ndb->getDictionary()->getTable(ORDERS_TABLE);
  if (ordersTab == nullptr) return -1;

  bool useNewHash = ordersTab->use_new_hash_function();

  targets.resize(rows.size());
  for (size_t i = 0; i < rows.size(); i++) {
    Int64 orderkey = rows[i].l_orderkey;

    /* Compute partition hash (for fragment routing) */
    Uint32 hashValue = 0;
    Ndb::Key_part_ptr kp[2];
    kp[0].ptr = &orderkey;
    kp[0].len = sizeof(Int64);
    kp[1].ptr = nullptr;
    kp[1].len = 0;
    if (Ndb::computeHash(&hashValue, ordersTab, kp, nullptr, 0) != 0)
      return -1;

    Uint32 fragId = ordersTab->getPartitionId(hashValue);
    targets[i].fragId = fragId;
    targets[i].nodeId = ordersMeta.fragNodes[fragId];
    targets[i].ldmInstance = ordersMeta.fragInstances[fragId];

    /* Compute primary key hash (for LQHKEYREQ hashValue field) */
    Uint32 hashValues[4];
    Uint32 keyWords[2];
    memcpy(keyWords, &orderkey, sizeof(Int64));
    rondb_calc_hash(hashValues, (const char *)keyWords, 2, useNewHash);
    targets[i].hashValue = hashValues[0];
  }
  return 0;
}

static int
runBenchmark(SignalSender &ss,
             const TableMeta &lineitemMeta,
             const TableMeta &ordersMeta,
             const std::vector<LineitemRow> &lineitemRows,
             const std::vector<LookupTarget> &targets,
             Uint32 numShipmodes,
             Uint32 strategy,
             bool pipelined,
             bool validate,
             BenchStats &stats,
             int iteration)
{
  V("\n========================================\n");
  V("Iteration %d: %zu lineitem rows, %u shipmodes\n",
    iteration, lineitemRows.size(), numShipmodes);
  V("========================================\n");

  /* Leaf = ORDERS table */
  std::set<Uint32> uniqueNodes(ordersMeta.fragNodes.begin(),
                                ordersMeta.fragNodes.end());

  Uint32 linkedShipmodeAttrId = lineitemMeta.attrIds.at("l_shipmode");
  Uint32 localPriorityAttrId = ordersMeta.attrIds.at("o_orderpriority");

  auto aggProg = buildAggProgram_Q12(linkedShipmodeAttrId,
                                      localPriorityAttrId);

  /* ---- Phase 1: Setup ---- */
  auto t0 = Clock::now();

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, ordersMeta, strategy, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto t1 = Clock::now();

  /* ---- Phase 2: LQHKEYREQ lookups to ORDERS ---- */
  /* Scan timing is measured once outside the SS lock; here we only
   * measure the signal-level lookup throughput. */
  auto t4 = Clock::now();
  Uint32 sentCount = 0;

  if (pipelined) {
    /* Pipelined: send all LQHKEYREQs, then collect all CONFs */
    for (size_t i = 0; i < lineitemRows.size(); i++) {
      Int64 orderkey = lineitemRows[i].l_orderkey;
      Int64 shipmode = lineitemRows[i].l_shipmode;

      auto ai = buildAttrInfoWithLinked(linkedShipmodeAttrId, shipmode);

      if (LqhKeyReqBuilder::send(ss,
                                  targets[i].nodeId,
                                  targets[i].ldmInstance,
                                  targets[i].fragId,
                                  aggStateKeys[targets[i].nodeId],
                                  targets[i].hashValue,
                                  orderkey,
                                  ordersMeta, ai) != 0)
        return -1;
      sentCount++;
    }

    /* Collect all CONFs */
    for (Uint32 i = 0; i < sentCount; i++) {
      if (waitForLqhKeyConf(ss) != 0) return -1;
    }
  } else {
    /* Sequential: send + wait one at a time */
    for (size_t i = 0; i < lineitemRows.size(); i++) {
      Int64 orderkey = lineitemRows[i].l_orderkey;
      Int64 shipmode = lineitemRows[i].l_shipmode;

      auto ai = buildAttrInfoWithLinked(linkedShipmodeAttrId, shipmode);

      if (LqhKeyReqBuilder::send(ss,
                                  targets[i].nodeId,
                                  targets[i].ldmInstance,
                                  targets[i].fragId,
                                  aggStateKeys[targets[i].nodeId],
                                  targets[i].hashValue,
                                  orderkey,
                                  ordersMeta, ai) != 0)
        return -1;

      if (waitForLqhKeyConf(ss) != 0) return -1;
      sentCount++;

      if (sentCount % 1000 == 0)
        V("  sent %u/%zu LQHKEYREQs\n", sentCount, lineitemRows.size());
    }
  }

  auto t5 = Clock::now();

  /* ---- Phase 3: Complete + receive results ---- */
  auto t6 = Clock::now();

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 10000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }

  auto t7 = Clock::now();

  /* ---- Phase 5: Release ---- */
  auto t8 = Clock::now();

  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  auto t9 = Clock::now();

  /* ---- Validate ---- */
  int failures = 0;
  if (validate) {
    /* Expected: for each shipmode, SUM the o_orderpriority of matched orders.
     * o_orderpriority = ((orderkey-1) % 5) + 1 */
    std::map<Int64, Int64> expected;
    for (const auto &row : lineitemRows) {
      Int64 prio = ((row.l_orderkey - 1) % 5) + 1;
      expected[row.l_shipmode] += prio;
    }

    std::map<Int64, Int64> actual;
    for (const auto &res : allResults) {
      for (const auto &grp : res.groups) {
        Int64 key = extractGroupKey(grp.first);
        Int64 sum = extractSumBigint(grp.second, 0);
        actual[key] += sum;
      }
    }

    if (actual.size() != expected.size()) {
      fprintf(stderr, "FAIL: expected %zu groups, got %zu\n",
              expected.size(), actual.size());
      failures++;
    }
    for (const auto &exp : expected) {
      auto it = actual.find(exp.first);
      if (it == actual.end()) {
        fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)exp.first);
        failures++;
      } else if (it->second != exp.second) {
        fprintf(stderr, "FAIL: group(%lld) SUM expected %lld, got %lld\n",
                (long long)exp.first, (long long)exp.second,
                (long long)it->second);
        failures++;
      }
    }

    /* SQL verification */
    if (failures == 0 && g_mysql_conn != nullptr) {
      if (mysql_query(g_mysql_conn,
              "SELECT l.l_shipmode, SUM(o.o_orderpriority) "
              "FROM bench_lineitem l JOIN bench_orders o "
              "ON l.l_orderkey = o.o_orderkey GROUP BY l.l_shipmode") != 0) {
        fprintf(stderr, "SQL verify failed: %s\n", mysql_error(g_mysql_conn));
        failures++;
      } else {
        MYSQL_RES *res = mysql_store_result(g_mysql_conn);
        if (res == nullptr) {
          fprintf(stderr, "mysql_store_result failed: %s\n",
                  mysql_error(g_mysql_conn));
          failures++;
        } else {
          std::map<Int64, Int64> sqlGroups;
          MYSQL_ROW row;
          while ((row = mysql_fetch_row(res)) != nullptr) {
            if (row[0] && row[1])
              sqlGroups[(Int64)atoll(row[0])] = (Int64)atoll(row[1]);
          }
          mysql_free_result(res);
          if (sqlGroups != expected) {
            fprintf(stderr, "SQL verify mismatch: SQL=%zu groups, "
                    "expected=%zu\n", sqlGroups.size(), expected.size());
            failures++;
          } else {
            V("  SQL verify: %zu groups — matches\n", sqlGroups.size());
          }
        }
      }
    }
  }

  /* ---- Timing ---- */
  stats.setupMs = elapsedMs(t0, t1);
  stats.lookupMs = elapsedMs(t4, t5);
  stats.completeMs = elapsedMs(t6, t7);
  stats.releaseMs = elapsedMs(t8, t9);
  stats.totalMs = elapsedMs(t0, t9);
  stats.lookupCount = sentCount;
  stats.lookupOpsPerSec = (stats.lookupMs > 0) ?
    (sentCount / (stats.lookupMs / 1000.0)) : 0;

  printf("Iteration %d: %s\n", iteration,
         failures == 0 ? "PASS" : "FAIL");
  printf("  Lineitems: %zu  Lookups: %u  Groups: %u  Nodes: %zu\n",
         lineitemRows.size(), sentCount, totalGroups, uniqueNodes.size());
  printf("  Setup:     %8.2f ms\n", stats.setupMs);
  printf("  Lookups:   %8.2f ms  (%.0f ops/sec)%s\n",
         stats.lookupMs, stats.lookupOpsPerSec,
         pipelined ? " [pipelined]" : " [sequential]");
  printf("  Complete:  %8.2f ms  (%u groups)\n",
         stats.completeMs, totalGroups);
  printf("  Release:   %8.2f ms\n", stats.releaseMs);
  printf("  Total:     %8.2f ms\n", stats.totalMs);

  if (verbose) {
    std::map<Int64, Int64> merged;
    for (const auto &res : allResults)
      for (const auto &grp : res.groups)
        merged[extractGroupKey(grp.first)] += extractSumBigint(grp.second, 0);
    for (const auto &m : merged)
      printf("    shipmode=%lld: SUM=%lld\n",
             (long long)m.first, (long long)m.second);
  }

  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static const char *connectString = nullptr;
static int mysqlPort = 3306;
static Uint32 numOrders = 1000;
static Uint32 linesPerOrder = 4;
static Uint32 numShipmodes = 7;
static int numIterations = 5;
static Uint32 strategy = JoinAggSetupReq::STRATEGY_MUTEX_BASED;
static bool pipelined = true;
static bool doValidate = true;

int main(int argc, char **argv)
{
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf(
        "Usage: %s [options]\n\n"
        "TPC-H Q12-inspired two-table join aggregation benchmark\n"
        "using linked attributes.\n\n"
        "Options:\n"
        "  -c <connect_string>    NDB connect string (default: localhost:1186)\n"
        "  -m <mysql_port>        MySQL port for ndbinfo (default: 3306)\n"
        "  --orders <N>           Number of ORDERS rows (default: 1000)\n"
        "  --lines <N>            Lines per order (default: 4)\n"
        "  --shipmodes <N>        Distinct l_shipmode values (default: 7)\n"
        "  --iterations <N>       Benchmark iterations (default: 5)\n"
        "  --strategy <mutex|free> Concurrency strategy (default: mutex)\n"
        "  --pipelined            Pipeline LQHKEYREQ sends (default)\n"
        "  --sequential           Send LQHKEYREQ one at a time\n"
        "  --validate             Validate results (default)\n"
        "  --no-validate          Skip validation\n"
        "  -v, --verbose          Verbose output\n"
        "  -h, --help             Show this help\n",
        argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--orders") == 0 && i + 1 < argc) {
      numOrders = (Uint32)atoi(argv[++i]);
    } else if (strcmp(argv[i], "--lines") == 0 && i + 1 < argc) {
      linesPerOrder = (Uint32)atoi(argv[++i]);
    } else if (strcmp(argv[i], "--shipmodes") == 0 && i + 1 < argc) {
      numShipmodes = (Uint32)atoi(argv[++i]);
    } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
      numIterations = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--strategy") == 0 && i + 1 < argc) {
      i++;
      if (strcmp(argv[i], "free") == 0)
        strategy = JoinAggSetupReq::STRATEGY_MUTEX_FREE;
      else
        strategy = JoinAggSetupReq::STRATEGY_MUTEX_BASED;
    } else if (strcmp(argv[i], "--pipelined") == 0) {
      pipelined = true;
    } else if (strcmp(argv[i], "--sequential") == 0) {
      pipelined = false;
    } else if (strcmp(argv[i], "--validate") == 0) {
      doValidate = true;
    } else if (strcmp(argv[i], "--no-validate") == 0) {
      doValidate = false;
    }
  }

  if (connectString == nullptr) connectString = "localhost:1186";

  Uint32 totalLineitems = numOrders * linesPerOrder;
  printf("benchJoinAgg: TPC-H Q12 two-table join aggregation benchmark\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Orders: %u  Lines/order: %u  Total lineitems: %u\n",
         numOrders, linesPerOrder, totalLineitems);
  printf("  Shipmodes: %u  Iterations: %d  Strategy: %s  Mode: %s\n",
         numShipmodes, numIterations,
         strategy == JoinAggSetupReq::STRATEGY_MUTEX_FREE ? "free" : "mutex",
         pipelined ? "pipelined" : "sequential");
  printf("\n");

  ndb_init();
  int result = 0;
  MYSQL *conn = nullptr;

  {
    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      result = 1; goto done;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      result = 1; goto done;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init: %s\n", ndb.getNdbError().message);
      result = 1; goto done;
    }

    NdbRestarter restarter(connectString);

    /* ---- Create tables ---- */
    conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      result = 1; goto done;
    }
    g_mysql_conn = conn;

    TableMeta lineitemMeta, ordersMeta;

    printf("Creating tables...\n");
    if (createLineitemTable(conn, &ndb, lineitemMeta) != 0) {
      result = 1; goto done;
    }
    if (queryFragInstances(mysqlPort, LINEITEM_TABLE, lineitemMeta) != 0) {
      result = 1; goto done;
    }
    if (createOrdersTable(conn, &ndb, ordersMeta) != 0) {
      result = 1; goto done;
    }
    if (queryFragInstances(mysqlPort, ORDERS_TABLE, ordersMeta) != 0) {
      result = 1; goto done;
    }

    /* ---- Load data ---- */
    printf("Loading data: %u orders, %u lineitems...\n",
           numOrders, totalLineitems);
    auto tLoad = Clock::now();

    if (insertOrders(&ndb, numOrders) != 0) { result = 1; goto done; }

    std::vector<LineitemRow> lineitemRows;
    if (insertLineitems(&ndb, numOrders, linesPerOrder,
                        numShipmodes, lineitemRows) != 0) {
      result = 1; goto done;
    }

    printf("Data loaded in %.2f ms\n", elapsedMs(tLoad, Clock::now()));
    printf("Using %zu pre-loaded lineitem rows (scan skipped)\n",
           lineitemRows.size());

    /* ---- Pre-compute hash/fragment targets (NDB API, outside SS lock) ---- */
    std::vector<LookupTarget> targets;
    if (precomputeTargets(&ndb, lineitemRows, ordersMeta, targets) != 0) {
      result = 1; goto done;
    }
    V("Pre-computed %zu lookup targets\n", targets.size());

    /* ---- Run benchmark iterations (inside SS lock) ---- */
    printf("\nStarting benchmark (%d iterations)...\n\n", numIterations);
    std::vector<BenchStats> allStats;
    {
      SignalSender ss(&con);
      ss.lock();

      /* Bypass tc-node check so LQHKEYCONF can route to our API node.
       * Must be inside ss.lock() to ensure DUMP propagates before
       * any LQHKEYREQ is sent (same pattern as testJoinAgg).
       */
      {
        int dump[1] = {DumpStateOrd::LqhSkipTcNodeCheck};
        restarter.dumpStateAllNodes(dump, 1);
        NdbSleep_MilliSleep(100);
        V("DUMP LqhSkipTcNodeCheck sent to all nodes\n");
      }

      for (int iter = 1; iter <= numIterations; iter++) {
        BenchStats bs = {};
        if (runBenchmark(ss, lineitemMeta, ordersMeta,
                         lineitemRows, targets, numShipmodes,
                         strategy, pipelined, doValidate,
                         bs, iter) != 0) {
          result = 1;
        }
        allStats.push_back(bs);
        printf("\n");
      }

      /* Restore the tc-node check before unlocking */
      {
        int dump[1] = {DumpStateOrd::LqhRestoreTcNodeCheck};
        restarter.dumpStateAllNodes(dump, 1);
        V("DUMP LqhRestoreTcNodeCheck sent to all nodes\n");
      }

      ss.unlock();
    }

    printSummary(allStats);

    /* ---- Cleanup ---- */
    printf("\nCleaning up...\n");
    dropTable(conn, LINEITEM_TABLE);
    dropTable(conn, ORDERS_TABLE);
  }

done:
  if (conn != nullptr) mysql_close(conn);
  ndb_end(0);

  printf(result == 0 ? "\n*** ALL ITERATIONS PASSED ***\n"
                      : "\n*** SOME ITERATIONS FAILED ***\n");
  return result;
}
