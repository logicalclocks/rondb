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
 * bench_q12_tpch — Full TPC-H Q12 benchmark with embedded CASE aggregation.
 *
 * Exercises pushdown join aggregation with BRANCH_ATTR_OP_ARG for string
 * comparison on CHAR columns.  Two-table join between LINEITEM and ORDERS
 * using linked CHAR(10) attributes for GROUP BY and embedded interpreter
 * for SUM(CASE WHEN o_orderpriority IN (...) THEN 1 ELSE 0 END).
 *
 * Q12:
 *   SELECT l_shipmode,
 *     SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
 *              THEN 1 ELSE 0 END) AS high_line_count,
 *     SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
 *              THEN 1 ELSE 0 END) AS low_line_count
 *   FROM orders, lineitem
 *   WHERE o_orderkey = l_orderkey
 *     AND l_shipmode IN ('MAIL', 'SHIP')
 *     AND l_commitdate < l_receiptdate
 *     AND l_shipdate < l_commitdate
 *     AND l_receiptdate >= '1994-01-01'
 *     AND l_receiptdate < '1995-01-01'
 *   GROUP BY l_shipmode
 *
 * Usage: bench_q12_tpch -c <connect_string> -m <mysql_port> [options]
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

#ifdef NONE
#undef NONE
#endif
#include <kernel/Interpreter.hpp>

#include <NdbRestarter.hpp>
#include <util/rondb_hash.hpp>
#include <mysql.h>

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <map>
#include <set>
#include <string>
#include <vector>

/* ------------------------------------------------------------------ */
/* Verbose output control                                              */
/* ------------------------------------------------------------------ */

static bool verbose = false;
static MYSQL *g_mysql_conn = nullptr;
static Uint32 pipelineBatch = 1000;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *LINEITEM_TABLE = "q12_lineitem";
static const char *ORDERS_TABLE   = "q12_orders";

static const Uint32 FAKE_TRANS_ID1 = 0xBE4C0001;
static const Uint32 FAKE_TRANS_ID2 = 0xBE4C0002;
static const Uint32 FAKE_REQUEST_ID = 2001;
static const Uint32 FAKE_SENDER_DATA = 99;
static const Uint32 WAIT_TIMEOUT_MS = 30000;

static const Uint32 COL_TYPE_BIGINT = 9;   /* NDB_TYPE_BIGINT */
static const Uint32 INTERPRETER_EXIT_OK = 18;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;
static const Uint32 LINKED_COL_FLAG = 0x8000;

/*
 * Epoch days from 1992-01-01.
 * TPC-H STARTDATE=92001 (Jan 1 1992), TOTDATE=2557 (days to Dec 31 1998).
 * Q12 filters l_receiptdate in [1994-01-01, 1995-01-01).
 * 1994-01-01 = 366(1992 leap) + 365(1993) = day 731
 * 1995-01-01 = 731 + 365 = day 1096
 */
static const Uint32 DATE_19940101 = 731;
static const Uint32 DATE_19950101 = 1096;

/*
 * Convert epoch day offset (days since 1992-01-01) to NDB packed DATE format.
 * NDB DATE: 3 bytes little-endian, packed = (year << 9) | (month << 5) | day.
 */
static Uint32
epochDayToNdbDate(Uint32 epochDay)
{
  struct tm t = {};
  t.tm_year = 92;   /* 1992 */
  t.tm_mon = 0;     /* January */
  t.tm_mday = 1 + (int)epochDay;
  t.tm_isdst = -1;
  mktime(&t);
  int y = t.tm_year + 1900;
  int m = t.tm_mon + 1;
  int d = t.tm_mday;
  return ((Uint32)y << 9) | ((Uint32)m << 5) | (Uint32)d;
}

static void
storeNdbDate(char *buf, Uint32 packed)
{
  buf[0] = (char)(packed & 0xFF);
  buf[1] = (char)((packed >> 8) & 0xFF);
  buf[2] = (char)((packed >> 16) & 0xFF);
}

/* TPC-H shipmode values (CHAR(10), space-padded) */
static const char SHIPMODES[][11] = {
  "REG AIR   ", "AIR       ", "RAIL      ",
  "SHIP      ", "TRUCK     ", "MAIL      ", "FOB       "
};
static const Uint32 NUM_SHIPMODES = 7;

/* TPC-H order priority values (CHAR(15), space-padded) */
static const char PRIORITIES[][16] = {
  "1-URGENT       ", "2-HIGH         ", "3-MEDIUM       ",
  "4-NOT SPECIFIED", "5-LOW          "
};
static const Uint32 NUM_PRIORITIES = 5;

/* ------------------------------------------------------------------ */
/* Timing helpers                                                      */
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
  double sqlMs;
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
  stats([](const BenchStats &r){ return r.sqlMs; }, "SQL query ms:");
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
/* Aggregation program builder — Q12 with embedded CASE                */
/* ------------------------------------------------------------------ */

/*
 * Build aggregation program for ORDERS (leaf table):
 *
 *   GROUP BY l_shipmode   — linked CHAR(10) from lineitem (bit 15)
 *   SUM(CASE WHEN o_orderpriority = '1-URGENT' OR = '2-HIGH'
 *            THEN 1 ELSE 0 END)   → agg[0] (high_line_count)
 *   SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND <> '2-HIGH'
 *            THEN 1 ELSE 0 END)   → agg[1] (low_line_count)
 *
 * Program: 45 words total.
 *   Header (8) + GROUP BY (1) + EmbeddedInterp header (1) +
 *   Embedded (18) + Aggregation (17) = 46... wait let me recount.
 *
 * Actually: 8 + 1 + 1 + 18 + 17 = 45 words.
 */
static std::vector<Uint32>
buildAggProgram_Q12(Uint32 /*linkedShipmodeAttrId*/, Uint32 localPriorityAttrId)
{
  const Uint32 EMB_LEN = 18;
  const Uint32 AGG_AFTER = 17;
  const Uint32 PROG_LEN = 8 + 1 + 1 + EMB_LEN + AGG_AFTER; /* = 45 */

  std::vector<Uint32> prog(PROG_LEN);

  /* Header */
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 2u;  /* n_gb_cols=1, n_agg_results=2 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /* GROUP BY l_shipmode — linked pos 0 from parent LINEITEM */
  prog[8] = (LINKED_COL_FLAG | 0) << 16;  /* linked pos 0 = l_shipmode */

  /* kOpEmbeddedInterp header */
  prog[9] = (kOpEmbeddedInterp << 26) | EMB_LEN;

  Uint32 pos = 10;

  /*
   * Embedded program (18 words):
   *   emb[0-5]:  BRANCH_ATTR_OP_ARG(prio, EQ, '1-URGENT       ') → emb[15]
   *   emb[6-11]: BRANCH_ATTR_OP_ARG(prio, EQ, '2-HIGH         ') → emb[15]
   *   emb[12]:   LOAD_CONST16 reg2, 9  (ELSE skip_offset)
   *   emb[13]:   WRITE_INTERPRETER_OUTPUT reg2, 0
   *   emb[14]:   EXIT_OK
   *   emb[15]:   LOAD_CONST16 reg2, 0  (THEN skip_offset)
   *   emb[16]:   WRITE_INTERPRETER_OUTPUT reg2, 0
   *   emb[17]:   EXIT_OK
   */

  /* emb[0]: BRANCH_ATTR_OP_ARG EQ, offset=15 → emb[15] */
  prog[pos++] = Interpreter::BranchCol(Interpreter::EQ,
                                        Interpreter::NULL_CMP_EQUAL) |
                (15u << 16);
  /* emb[1]: (attrId << 16) | 15 */
  prog[pos++] = Interpreter::BranchCol_2(localPriorityAttrId, 15);
  /* emb[2-5]: '1-URGENT       ' (15 bytes, space-padded, + 1 zero pad = 16) */
  {
    char buf[16];
    memset(buf, ' ', 15);
    memcpy(buf, "1-URGENT", 8);
    buf[15] = 0;
    memcpy(&prog[pos], buf, 16);
    pos += 4;
  }

  /* emb[6]: BRANCH_ATTR_OP_ARG EQ, offset=9 → emb[15] */
  prog[pos++] = Interpreter::BranchCol(Interpreter::EQ,
                                        Interpreter::NULL_CMP_EQUAL) |
                (9u << 16);
  /* emb[7]: (attrId << 16) | 15 */
  prog[pos++] = Interpreter::BranchCol_2(localPriorityAttrId, 15);
  /* emb[8-11]: '2-HIGH         ' (15 bytes + 1 zero pad = 16) */
  {
    char buf[16];
    memset(buf, ' ', 15);
    memcpy(buf, "2-HIGH", 6);
    buf[15] = 0;
    memcpy(&prog[pos], buf, 16);
    pos += 4;
  }

  /* emb[12]: ELSE path — LOAD_CONST16 reg2, 9 (skip_offset) */
  prog[pos++] = Interpreter::LOAD_CONST16 | (2 << 6) | (9u << 16);

  /* emb[13]: WRITE_INTERPRETER_OUTPUT reg2, output_index=0 */
  prog[pos++] = Interpreter::WriteInterpreterOutput(2, 0);

  /* emb[14]: EXIT_OK */
  prog[pos++] = Interpreter::ExitOK();

  /* emb[15]: THEN path — LOAD_CONST16 reg2, 0 (skip_offset) */
  prog[pos++] = Interpreter::LOAD_CONST16 | (2 << 6) | (0u << 16);

  /* emb[16]: WRITE_INTERPRETER_OUTPUT reg2, output_index=0 */
  prog[pos++] = Interpreter::WriteInterpreterOutput(2, 0);

  /* emb[17]: EXIT_OK */
  prog[pos++] = Interpreter::ExitOK();

  assert(pos == 28);  /* 10 + 18 */

  /*
   * Aggregation instructions after embedded block (17 words):
   *   THEN path (skip_offset=0):
   *     A+0:  kOpLoadConst reg0, BIGINT, 1   [3 words]
   *     A+3:  kOpSum reg0, agg[0]            [1 word]
   *     A+4:  kOpLoadConst reg0, BIGINT, 0   [3 words]
   *     A+7:  kOpSum reg0, agg[1]            [1 word]
   *     A+8:  kOpSkip 8                      [1 word]
   *   ELSE path (skip_offset=9):
   *     A+9:  kOpLoadConst reg0, BIGINT, 0   [3 words]
   *     A+12: kOpSum reg0, agg[0]            [1 word]
   *     A+13: kOpLoadConst reg0, BIGINT, 1   [3 words]
   *     A+16: kOpSum reg0, agg[1]            [1 word]
   */

  /* A+0: THEN high=1 */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 1; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 0;

  /* A+4: THEN low=0 */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 0; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 1;

  /* A+8: kOpSkip 8 — skip ELSE block */
  prog[pos++] = (kOpSkip << 26) | 8;

  /* A+9: ELSE high=0 */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 0; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 0;

  /* A+13: ELSE low=1 */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 1; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 1;

  assert(pos == PROG_LEN);
  return prog;
}

/* ------------------------------------------------------------------ */
/* AttrInfo builder with linked CHAR(10) attribute                     */
/* ------------------------------------------------------------------ */

static std::vector<Uint32>
buildAttrInfoWithLinkedChar10(Uint32 linkedAttrId, const char *shipmodeValue,
                              Uint32 tableId, Uint32 schemaVersion)
{
  const Uint32 DATA_WORDS = 4;    /* 1 AH + 3 data words for CHAR(10) */
  const Uint32 ENTRY_WORDS = 2 + DATA_WORDS;  /* tableId + tableVersion + data */
  std::vector<Uint32> ai(6 + 1 + ENTRY_WORDS);

  ai[0] = 0;              /* initial read section length */
  ai[1] = 1;              /* interpreter program length (ExitOK) */
  ai[2] = 0;              /* subroutine length */
  ai[3] = 0;              /* final read section length */
  ai[4] = 1 + ENTRY_WORDS;  /* linked attr section length (paramLen + data) */
  ai[5] = INTERPRETER_EXIT_OK;

  /* paramLen word */
  ai[6] = ENTRY_WORDS;

  /* Linked entry: tableId, tableVersion, AH, data */
  ai[7] = tableId;
  ai[8] = schemaVersion;
  ai[9] = (linkedAttrId << 16) | 10;

  /* CHAR(10) data + 2 zero-padding bytes = 12 bytes = 3 words */
  char buf[12];
  memset(buf, 0, 12);
  memcpy(buf, shipmodeValue, 10);
  memcpy(&ai[10], buf, 12);

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
    fprintf(stderr, "getTable(%s): %s\n",
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
  sqlExec(conn, "DROP TABLE IF EXISTS q12_lineitem");
  if (sqlExec(conn,
        "CREATE TABLE q12_lineitem ("
        "  l_orderkey BIGINT NOT NULL,"
        "  l_linenumber INT NOT NULL,"
        "  l_shipmode CHAR(10) NOT NULL,"
        "  l_shipdate DATE NOT NULL,"
        "  l_commitdate DATE NOT NULL,"
        "  l_receiptdate DATE NOT NULL,"
        "  PRIMARY KEY (l_orderkey, l_linenumber)"
        ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0)
    return -1;

  if (loadTableMeta(ndb, LINEITEM_TABLE, meta) != 0) return -1;

  V("Table '%s': id=%u frags=%u\n", LINEITEM_TABLE, meta.tableId, meta.fragCount);
  return 0;
}

static int
createOrdersTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS q12_orders");
  if (sqlExec(conn,
        "CREATE TABLE q12_orders ("
        "  o_orderkey BIGINT NOT NULL PRIMARY KEY,"
        "  o_orderpriority CHAR(15) NOT NULL"
        ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0)
    return -1;

  if (loadTableMeta(ndb, ORDERS_TABLE, meta) != 0) return -1;

  V("Table '%s': id=%u frags=%u\n", ORDERS_TABLE, meta.tableId, meta.fragCount);
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
    if (conn == nullptr) return -1;

    if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                           "ndbinfo", mysqlPort, nullptr, 0) == nullptr) {
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
      mysql_close(conn);
      continue;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    if (result == nullptr) { mysql_close(conn); continue; }

    std::map<std::pair<Uint32,Uint32>, Uint32> instMap;
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result)) != nullptr) {
      instMap[{(Uint32)atoi(row[0]), (Uint32)atoi(row[1])}] = (Uint32)atoi(row[2]);
    }
    mysql_free_result(result);
    mysql_close(conn);

    bool complete = true;
    meta.fragInstances.resize(meta.fragCount);
    for (Uint32 f = 0; f < meta.fragCount; f++) {
      auto it = instMap.find({meta.fragNodes[f], f});
      if (it == instMap.end()) { complete = false; break; }
      meta.fragInstances[f] = it->second;
    }

    if (complete) {
      for (Uint32 f = 0; f < meta.fragCount; f++) {
        V("  %s frag %u -> node %u, LDM %u\n",
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
  Int64  l_orderkey;
  Int32  l_linenumber;
  char   l_shipmode[10];
  Uint32 l_shipdate;
  Uint32 l_commitdate;
  Uint32 l_receiptdate;
};

static int
insertOrders(Ndb *ndb, Uint32 numOrders)
{
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(ORDERS_TABLE);
  if (ptab == nullptr) return -1;

  const Uint32 BATCH = 500;
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
      op->equal("o_orderkey", key);
      op->setValue("o_orderpriority", PRIORITIES[i % NUM_PRIORITIES]);
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert orders [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
  }
  printf("Inserted %u rows into %s\n", numOrders, ORDERS_TABLE);
  return 0;
}

static int
insertLineitems(Ndb *ndb, Uint32 numOrders, Uint32 linesPerOrder,
                std::vector<LineitemRow> &allRows)
{
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(LINEITEM_TABLE);
  if (ptab == nullptr) return -1;

  Uint32 totalRows = numOrders * linesPerOrder;
  allRows.reserve(totalRows);

  const Uint32 BATCH = 500;
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
      Int32 linenumber = (Int32)(l + 1);
      Uint32 idx = o * linesPerOrder + l;
      Uint32 shipmodeIdx = idx % NUM_SHIPMODES;

      /*
       * TPC-H date generation (dbt3 build.c):
       *   orderdate  = STARTDATE + random(0, 2405)   -- per order
       *   l_shipdate = orderdate + random(1, 121)     -- independent
       *   l_commitdate = orderdate + random(30, 90)   -- independent
       *   l_receiptdate = l_shipdate + random(1, 30)  -- offset from shipdate
       *
       * Use coprime multipliers for pseudo-random independence:
       *   gcd(1013,2406)=1, gcd(13,121)=1, gcd(7,61)=1, gcd(17,30)=1
       */
      Uint32 orderdate = (o * 1013) % 2406;          /* 0..2405 */
      Uint32 s_offset = 1 + ((idx * 13) % 121);      /* 1..121  */
      Uint32 c_offset = 30 + ((idx * 7) % 61);       /* 30..90  */
      Uint32 r_offset = 1 + ((idx * 17) % 30);       /* 1..30   */
      Uint32 shipdate = orderdate + s_offset;
      Uint32 commitdate = orderdate + c_offset;
      Uint32 receiptdate = shipdate + r_offset;

      op->equal("l_orderkey", orderkey);
      op->equal("l_linenumber", linenumber);
      op->setValue("l_shipmode", SHIPMODES[shipmodeIdx]);
      char sd[4] = {0}, cd[4] = {0}, rd[4] = {0};
      storeNdbDate(sd, epochDayToNdbDate(shipdate));
      storeNdbDate(cd, epochDayToNdbDate(commitdate));
      storeNdbDate(rd, epochDayToNdbDate(receiptdate));
      op->setValue("l_shipdate", sd);
      op->setValue("l_commitdate", cd);
      op->setValue("l_receiptdate", rd);

      LineitemRow row;
      row.l_orderkey = orderkey;
      row.l_linenumber = linenumber;
      memcpy(row.l_shipmode, SHIPMODES[shipmodeIdx], 10);
      row.l_shipdate = shipdate;
      row.l_commitdate = commitdate;
      row.l_receiptdate = receiptdate;
      allRows.push_back(row);
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

  printf("Inserted %u rows into %s\n", count, LINEITEM_TABLE);
  return 0;
}

static int
dropTable(MYSQL *conn, const char *tableName)
{
  char query[256];
  snprintf(query, sizeof(query), "DROP TABLE IF EXISTS %s", tableName);
  return sqlExec(conn, query);
}

static void
generateLineitemRows(Uint32 numOrders, Uint32 linesPerOrder,
                     std::vector<LineitemRow> &allRows)
{
  Uint32 totalRows = numOrders * linesPerOrder;
  allRows.reserve(totalRows);

  for (Uint32 o = 0; o < numOrders; o++) {
    for (Uint32 l = 0; l < linesPerOrder; l++) {
      Uint32 idx = o * linesPerOrder + l;
      Uint32 shipmodeIdx = idx % NUM_SHIPMODES;
      Uint32 orderdate = (o * 1013) % 2406;
      Uint32 s_offset = 1 + ((idx * 13) % 121);
      Uint32 c_offset = 30 + ((idx * 7) % 61);
      Uint32 r_offset = 1 + ((idx * 17) % 30);

      LineitemRow row;
      row.l_orderkey = (Int64)(o + 1);
      row.l_linenumber = (Int32)(l + 1);
      memcpy(row.l_shipmode, SHIPMODES[shipmodeIdx], 10);
      row.l_shipdate = orderdate + s_offset;
      row.l_commitdate = orderdate + c_offset;
      row.l_receiptdate = row.l_shipdate + r_offset;
      allRows.push_back(row);
    }
  }
}

/* ------------------------------------------------------------------ */
/* WHERE filter (application-side)                                     */
/* ------------------------------------------------------------------ */

static void
filterLineitems(const std::vector<LineitemRow> &allRows,
                std::vector<size_t> &qualifiedIndices)
{
  static const char MAIL_PADDED[10] = {'M','A','I','L',' ',' ',' ',' ',' ',' '};
  static const char SHIP_PADDED[10] = {'S','H','I','P',' ',' ',' ',' ',' ',' '};

  for (size_t i = 0; i < allRows.size(); i++) {
    const auto &r = allRows[i];

    /* l_shipmode IN ('MAIL', 'SHIP') */
    if (memcmp(r.l_shipmode, MAIL_PADDED, 10) != 0 &&
        memcmp(r.l_shipmode, SHIP_PADDED, 10) != 0)
      continue;

    /* l_commitdate < l_receiptdate */
    if (r.l_commitdate >= r.l_receiptdate)
      continue;

    /* l_shipdate < l_commitdate */
    if (r.l_shipdate >= r.l_commitdate)
      continue;

    /* l_receiptdate >= DATE_19940101 AND l_receiptdate < DATE_19950101 */
    if (r.l_receiptdate < DATE_19940101 || r.l_receiptdate >= DATE_19950101)
      continue;

    qualifiedIndices.push_back(i);
  }
}

/* ------------------------------------------------------------------ */
/* Signal helpers                                                      */
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
  V("  SETUP_REQ -> node %u\n", nodeId);

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
  V("  COMPLETE_REQ -> node %u\n", nodeId);

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

static std::string
extractGroupKeyChar10(const std::vector<Uint8> &key)
{
  const Uint32 HDR = 4;  /* AttributeHeader */
  if (key.size() < HDR + 10) return "";
  /* Trim trailing spaces */
  int len = 10;
  while (len > 0 && key[HDR + len - 1] == ' ') len--;
  return std::string(reinterpret_cast<const char*>(key.data() + HDR), len);
}

static std::string
extractGroupKeyChar10Raw(const std::vector<Uint8> &key)
{
  const Uint32 HDR = 4;
  if (key.size() < HDR + 10) return "";
  return std::string(reinterpret_cast<const char*>(key.data() + HDR), 10);
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
/* Pre-compute hash/fragment targets                                   */
/* ------------------------------------------------------------------ */

struct LookupTarget {
  Uint32 fragId;
  Uint32 nodeId;
  Uint32 ldmInstance;
  Uint32 hashValue;
};

static int
precomputeTargets(Ndb *ndb,
                  const std::vector<LineitemRow> &rows,
                  const std::vector<size_t> &qualifiedIndices,
                  const TableMeta &ordersMeta,
                  std::vector<LookupTarget> &targets)
{
  const NdbDictionary::Table *ordersTab =
    ndb->getDictionary()->getTable(ORDERS_TABLE);
  if (ordersTab == nullptr) return -1;

  bool useNewHash = ordersTab->use_new_hash_function();

  targets.resize(qualifiedIndices.size());
  for (size_t qi = 0; qi < qualifiedIndices.size(); qi++) {
    Int64 orderkey = rows[qualifiedIndices[qi]].l_orderkey;

    Uint32 hashValue = 0;
    Ndb::Key_part_ptr kp[2];
    kp[0].ptr = &orderkey;
    kp[0].len = sizeof(Int64);
    kp[1].ptr = nullptr;
    kp[1].len = 0;
    if (Ndb::computeHash(&hashValue, ordersTab, kp, nullptr, 0) != 0)
      return -1;

    Uint32 fragId = ordersTab->getPartitionId(hashValue);
    targets[qi].fragId = fragId;
    targets[qi].nodeId = ordersMeta.fragNodes[fragId];
    targets[qi].ldmInstance = ordersMeta.fragInstances[fragId];

    Uint32 hashValues[4];
    Uint32 keyWords[2];
    memcpy(keyWords, &orderkey, sizeof(Int64));
    rondb_calc_hash(hashValues, (const char *)keyWords, 2, useNewHash);
    targets[qi].hashValue = hashValues[0];
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main benchmark function                                             */
/* ------------------------------------------------------------------ */

static int
runBenchmark(SignalSender &ss,
             const TableMeta &lineitemMeta,
             const TableMeta &ordersMeta,
             const std::vector<LineitemRow> &lineitemRows,
             const std::vector<size_t> &qualifiedIndices,
             const std::vector<LookupTarget> &targets,
             Uint32 strategy,
             bool pipelined,
             bool validate,
             BenchStats &stats,
             int iteration)
{
  V("\n========================================\n");
  V("Iteration %d: %zu qualifying rows\n", iteration, qualifiedIndices.size());
  V("========================================\n");

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

  /* ---- Phase 2: LQHKEYREQ lookups ---- */
  const Uint32 PIPELINE_BATCH = pipelineBatch;
  auto t4 = Clock::now();
  Uint32 sentCount = 0;
  Uint32 recvCount = 0;

  if (pipelined) {
    /*
     * Batched pipelining: send up to PIPELINE_BATCH LQHKEYREQs, then
     * collect that batch of CONFs before sending the next batch.
     * This bounds in-flight signals and avoids transporter buffer overflow.
     */
    for (size_t qi = 0; qi < qualifiedIndices.size(); qi++) {
      const auto &row = lineitemRows[qualifiedIndices[qi]];

      auto ai = buildAttrInfoWithLinkedChar10(linkedShipmodeAttrId,
                                               row.l_shipmode,
                                               lineitemMeta.tableId,
                                               lineitemMeta.schemaVersion);

      if (LqhKeyReqBuilder::send(ss,
                                  targets[qi].nodeId,
                                  targets[qi].ldmInstance,
                                  targets[qi].fragId,
                                  aggStateKeys[targets[qi].nodeId],
                                  targets[qi].hashValue,
                                  row.l_orderkey,
                                  ordersMeta, ai) != 0)
        return -1;
      sentCount++;

      if (sentCount - recvCount >= PIPELINE_BATCH) {
        while (recvCount < sentCount) {
          if (waitForLqhKeyConf(ss) != 0) return -1;
          recvCount++;
        }
      }
    }

    /* Drain remaining CONFs */
    while (recvCount < sentCount) {
      if (waitForLqhKeyConf(ss) != 0) return -1;
      recvCount++;
    }
  } else {
    for (size_t qi = 0; qi < qualifiedIndices.size(); qi++) {
      const auto &row = lineitemRows[qualifiedIndices[qi]];

      auto ai = buildAttrInfoWithLinkedChar10(linkedShipmodeAttrId,
                                               row.l_shipmode,
                                               lineitemMeta.tableId,
                                               lineitemMeta.schemaVersion);

      if (LqhKeyReqBuilder::send(ss,
                                  targets[qi].nodeId,
                                  targets[qi].ldmInstance,
                                  targets[qi].fragId,
                                  aggStateKeys[targets[qi].nodeId],
                                  targets[qi].hashValue,
                                  row.l_orderkey,
                                  ordersMeta, ai) != 0)
        return -1;

      if (waitForLqhKeyConf(ss) != 0) return -1;
      sentCount++;
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

  /* ---- Phase 4: Release ---- */
  auto t8 = Clock::now();

  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  auto t9 = Clock::now();

  /* ---- Validate ---- */
  int failures = 0;
  if (validate) {
    /* Expected: for each qualifying row, classify by shipmode and priority */
    std::map<std::string, Int64> expectedHigh, expectedLow;
    for (size_t qi = 0; qi < qualifiedIndices.size(); qi++) {
      const auto &row = lineitemRows[qualifiedIndices[qi]];
      std::string sm(row.l_shipmode, 10);
      Uint32 prioIdx = ((Uint32)(row.l_orderkey - 1)) % NUM_PRIORITIES;
      if (prioIdx == 0 || prioIdx == 1) {
        expectedHigh[sm]++;
      } else {
        expectedLow[sm]++;
      }
    }

    /* Merge actual results across nodes */
    std::map<std::string, Int64> actualHigh, actualLow;
    for (const auto &res : allResults) {
      for (const auto &grp : res.groups) {
        std::string key = extractGroupKeyChar10Raw(grp.first);
        actualHigh[key] += extractSumBigint(grp.second, 0);
        actualLow[key] += extractSumBigint(grp.second, 1);
      }
    }

    /* Compare */
    std::set<std::string> allKeys;
    for (const auto &kv : expectedHigh) allKeys.insert(kv.first);
    for (const auto &kv : expectedLow) allKeys.insert(kv.first);
    for (const auto &kv : actualHigh) allKeys.insert(kv.first);

    for (const auto &key : allKeys) {
      Int64 eh = expectedHigh.count(key) ? expectedHigh[key] : 0;
      Int64 el = expectedLow.count(key) ? expectedLow[key] : 0;
      Int64 ah = actualHigh.count(key) ? actualHigh[key] : 0;
      Int64 al = actualLow.count(key) ? actualLow[key] : 0;
      std::string display = extractGroupKeyChar10(
        std::vector<Uint8>{0,0,0,0,
          (Uint8)key[0],(Uint8)key[1],(Uint8)key[2],(Uint8)key[3],
          (Uint8)key[4],(Uint8)key[5],(Uint8)key[6],(Uint8)key[7],
          (Uint8)key[8],(Uint8)key[9]});

      if (ah != eh) {
        fprintf(stderr, "FAIL: %s high expected %lld, got %lld\n",
                display.c_str(), (long long)eh, (long long)ah);
        failures++;
      }
      if (al != el) {
        fprintf(stderr, "FAIL: %s low expected %lld, got %lld\n",
                display.c_str(), (long long)el, (long long)al);
        failures++;
      }
    }

    /* SQL verification and timing */
    if (g_mysql_conn != nullptr) {
      auto tSql0 = Clock::now();
      if (mysql_query(g_mysql_conn,
              "SELECT l.l_shipmode, "
              "SUM(CASE WHEN o.o_orderpriority IN ('1-URGENT','2-HIGH') "
              "THEN 1 ELSE 0 END), "
              "SUM(CASE WHEN o.o_orderpriority NOT IN ('1-URGENT','2-HIGH') "
              "THEN 1 ELSE 0 END) "
              "FROM q12_lineitem l JOIN q12_orders o "
              "ON l.l_orderkey = o.o_orderkey "
              "WHERE l.l_shipmode IN ('MAIL','SHIP') "
              "AND l.l_commitdate < l.l_receiptdate "
              "AND l.l_shipdate < l.l_commitdate "
              "AND l.l_receiptdate >= '1994-01-01' "
              "AND l.l_receiptdate < '1995-01-01' "
              "GROUP BY l.l_shipmode") != 0) {
        fprintf(stderr, "SQL verify failed: %s\n", mysql_error(g_mysql_conn));
        failures++;
      } else {
        MYSQL_RES *res = mysql_store_result(g_mysql_conn);
        if (res == nullptr) {
          fprintf(stderr, "mysql_store_result failed: %s\n",
                  mysql_error(g_mysql_conn));
          failures++;
        } else {
          std::map<std::string, Int64> sqlHigh, sqlLow;
          MYSQL_ROW row;
          while ((row = mysql_fetch_row(res)) != nullptr) {
            if (row[0] && row[1] && row[2]) {
              std::string sm(row[0]);
              while (sm.size() < 10) sm.push_back(' ');
              sqlHigh[sm] = (Int64)atoll(row[1]);
              sqlLow[sm] = (Int64)atoll(row[2]);
            }
          }
          mysql_free_result(res);
          if (sqlHigh != expectedHigh || sqlLow != expectedLow) {
            fprintf(stderr, "SQL verify mismatch\n");
            failures++;
          } else {
            V("  SQL verify: %zu shipmode groups — matches\n",
              sqlHigh.size());
          }
        }
      }
      auto tSql1 = Clock::now();
      stats.sqlMs = elapsedMs(tSql0, tSql1);
    }
  }

  /* ---- Timing ---- */
  stats.setupMs = elapsedMs(t0, t1);
  stats.lookupMs = elapsedMs(t4, t5);
  stats.completeMs = elapsedMs(t6, t7);
  stats.releaseMs = elapsedMs(t8, t9);
  stats.totalMs = elapsedMs(t0, t9);
  /* stats.sqlMs set inside SQL verification block, default 0 if skipped */
  stats.lookupCount = sentCount;
  stats.lookupOpsPerSec = (stats.lookupMs > 0) ?
    (sentCount / (stats.lookupMs / 1000.0)) : 0;

  printf("Iteration %d: %s\n", iteration,
         failures == 0 ? "PASS" : "FAIL");
  printf("  Qualifying: %zu  Lookups: %u  Groups: %u  Nodes: %zu\n",
         qualifiedIndices.size(), sentCount, totalGroups, uniqueNodes.size());
  printf("  Setup:     %8.2f ms\n", stats.setupMs);
  printf("  Lookups:   %8.2f ms  (%.0f ops/sec)%s\n",
         stats.lookupMs, stats.lookupOpsPerSec,
         pipelined ? " [pipelined]" : " [sequential]");
  printf("  Complete:  %8.2f ms  (%u groups)\n",
         stats.completeMs, totalGroups);
  printf("  Release:   %8.2f ms\n", stats.releaseMs);
  printf("  Total:     %8.2f ms\n", stats.totalMs);
  if (stats.sqlMs > 0)
    printf("  SQL query: %8.2f ms\n", stats.sqlMs);

  if (verbose) {
    std::map<std::string, std::pair<Int64, Int64>> merged;
    for (const auto &res : allResults) {
      for (const auto &grp : res.groups) {
        std::string key = extractGroupKeyChar10(grp.first);
        merged[key].first += extractSumBigint(grp.second, 0);
        merged[key].second += extractSumBigint(grp.second, 1);
      }
    }
    for (const auto &m : merged) {
      printf("    %s: high=%lld  low=%lld\n",
             m.first.c_str(), (long long)m.second.first,
             (long long)m.second.second);
    }
  }

  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static const char *connectString = nullptr;
static int mysqlPort = 3306;
static Uint32 numOrders = 1500000;
static Uint32 linesPerOrder = 4;
static int numIterations = 3;
static Uint32 strategy = JoinAggSetupReq::STRATEGY_MUTEX_BASED;
static bool pipelined = true;
static bool doValidate = true;
static bool keepTables = false;

static int
doRun()
{
  Uint32 totalLineitems = numOrders * linesPerOrder;

  Ndb_cluster_connection con(connectString);
  if (con.connect(12, 5, 1) != 0) {
    fprintf(stderr, "Failed to connect to management server\n");
    return 1;
  }
  if (con.wait_until_ready(30, 0) < 0) {
    fprintf(stderr, "Cluster not ready\n");
    return 1;
  }
  V("Connected to cluster\n");

  Ndb ndb(&con, "test");
  if (ndb.init() != 0) {
    fprintf(stderr, "Ndb::init: %s\n", ndb.getNdbError().message);
    return 1;
  }

  NdbRestarter restarter(connectString);

  MYSQL *conn = connectMysql(mysqlPort);
  if (conn == nullptr) {
    fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
    return 1;
  }
  g_mysql_conn = conn;

  /* ---- Create tables + load data (skip if --keep-tables and exist) ---- */
  TableMeta lineitemMeta, ordersMeta;
  std::vector<LineitemRow> lineitemRows;

  NdbDictionary::Dictionary *dict = ndb.getDictionary();
  bool tablesExist = keepTables &&
    dict->getTable(LINEITEM_TABLE) != nullptr &&
    dict->getTable(ORDERS_TABLE) != nullptr;

  if (tablesExist) {
    printf("Reusing existing tables (--keep-tables)\n");
    if (loadTableMeta(&ndb, LINEITEM_TABLE, lineitemMeta) != 0) {
      mysql_close(conn); return 1;
    }
    if (queryFragInstances(mysqlPort, LINEITEM_TABLE, lineitemMeta) != 0) {
      mysql_close(conn); return 1;
    }
    if (loadTableMeta(&ndb, ORDERS_TABLE, ordersMeta) != 0) {
      mysql_close(conn); return 1;
    }
    if (queryFragInstances(mysqlPort, ORDERS_TABLE, ordersMeta) != 0) {
      mysql_close(conn); return 1;
    }
    generateLineitemRows(numOrders, linesPerOrder, lineitemRows);
  } else {
    printf("Creating tables...\n");
    if (createLineitemTable(conn, &ndb, lineitemMeta) != 0) {
      mysql_close(conn); return 1;
    }
    if (queryFragInstances(mysqlPort, LINEITEM_TABLE, lineitemMeta) != 0) {
      mysql_close(conn); return 1;
    }
    if (createOrdersTable(conn, &ndb, ordersMeta) != 0) {
      mysql_close(conn); return 1;
    }
    if (queryFragInstances(mysqlPort, ORDERS_TABLE, ordersMeta) != 0) {
      mysql_close(conn); return 1;
    }

    auto tLoad = Clock::now();
    printf("Loading data: %u orders, %u lineitems...\n",
           numOrders, totalLineitems);

    if (insertOrders(&ndb, numOrders) != 0) { mysql_close(conn); return 1; }
    if (insertLineitems(&ndb, numOrders, linesPerOrder, lineitemRows) != 0) {
      mysql_close(conn); return 1;
    }

    printf("Data loaded in %.2f ms\n", elapsedMs(tLoad, Clock::now()));
  }

  /* ---- Filter lineitem rows (Q12 WHERE conditions) ---- */
  auto tFilter = Clock::now();
  std::vector<size_t> qualifiedIndices;
  filterLineitems(lineitemRows, qualifiedIndices);
  printf("Q12 filter: %zu qualifying rows from %zu total (%.1f%%) in %.2f ms\n",
         qualifiedIndices.size(), lineitemRows.size(),
         100.0 * qualifiedIndices.size() / lineitemRows.size(),
         elapsedMs(tFilter, Clock::now()));

  if (qualifiedIndices.empty()) {
    fprintf(stderr, "No qualifying rows — check data generation parameters\n");
    return 1;
  }

  /* ---- Pre-compute hash/fragment targets ---- */
  std::vector<LookupTarget> targets;
  if (precomputeTargets(&ndb, lineitemRows, qualifiedIndices,
                        ordersMeta, targets) != 0)
    return 1;
  V("Pre-computed %zu lookup targets\n", targets.size());

  /* ---- Run benchmark iterations (inside SS lock) ---- */
  printf("\nStarting benchmark (%d iterations)...\n\n", numIterations);
  std::vector<BenchStats> allStats;
  int result = 0;
  {
    SignalSender ss(&con);
    ss.lock();

    {
      int dump[1] = {DumpStateOrd::LqhSkipTcNodeCheck};
      restarter.dumpStateAllNodes(dump, 1);
      NdbSleep_MilliSleep(100);
      V("DUMP LqhSkipTcNodeCheck sent to all nodes\n");
    }

    for (int iter = 1; iter <= numIterations; iter++) {
      BenchStats bs = {};
      if (runBenchmark(ss, lineitemMeta, ordersMeta,
                       lineitemRows, qualifiedIndices, targets,
                       strategy, pipelined, doValidate,
                       bs, iter) != 0) {
        result = 1;
        /* Drain stale signals from failed iteration */
        for (int d = 0; d < 100; d++) {
          SimpleSignal *stale = ss.waitFor(50);
          if (stale == nullptr) break;
        }
      }
      allStats.push_back(bs);
      printf("\n");
    }

    {
      int dump[1] = {DumpStateOrd::LqhRestoreTcNodeCheck};
      restarter.dumpStateAllNodes(dump, 1);
      V("DUMP LqhRestoreTcNodeCheck sent to all nodes\n");
    }

    ss.unlock();
  }

  printSummary(allStats);

  /* ---- Cleanup ---- */
  if (keepTables) {
    printf("\nKeeping tables for next run (--keep-tables)\n");
  } else {
    printf("\nCleaning up...\n");
    dropTable(conn, LINEITEM_TABLE);
    dropTable(conn, ORDERS_TABLE);
  }
  mysql_close(conn);

  return result;
}

int main(int argc, char **argv)
{
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf(
        "Usage: %s [options]\n\n"
        "TPC-H Q12 benchmark with embedded CASE aggregation.\n"
        "Uses BRANCH_ATTR_OP_ARG for CHAR string comparison.\n\n"
        "Options:\n"
        "  -c <connect_string>    NDB connect string (default: localhost:1186)\n"
        "  -m <mysql_port>        MySQL port for ndbinfo (default: 3306)\n"
        "  --orders <N>           Number of orders (default: 1500000)\n"
        "  --lines <N>            Lines per order (default: 4)\n"
        "  --iterations <N>       Benchmark iterations (default: 3)\n"
        "  --batch <N>            Pipeline batch size (default: 1000)\n"
        "  --strategy <mutex|free> Concurrency strategy (default: mutex)\n"
        "  --pipelined            Pipeline LQHKEYREQ sends (default)\n"
        "  --sequential           Send one at a time\n"
        "  --keep-tables          Keep tables after benchmark for reuse\n"
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
    } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
      numIterations = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--batch") == 0 && i + 1 < argc) {
      pipelineBatch = (Uint32)atoi(argv[++i]);
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
    } else if (strcmp(argv[i], "--keep-tables") == 0) {
      keepTables = true;
    } else if (strcmp(argv[i], "--validate") == 0) {
      doValidate = true;
    } else if (strcmp(argv[i], "--no-validate") == 0) {
      doValidate = false;
    }
  }

  if (connectString == nullptr) connectString = "localhost:1186";

  Uint32 totalLineitems = numOrders * linesPerOrder;
  printf("bench_q12_tpch: TPC-H Q12 with embedded CASE aggregation\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Orders: %u  Lines/order: %u  Total lineitems: %u\n",
         numOrders, linesPerOrder, totalLineitems);
  printf("  Iterations: %d  Strategy: %s  Mode: %s\n",
         numIterations,
         strategy == JoinAggSetupReq::STRATEGY_MUTEX_FREE ? "free" : "mutex",
         pipelined ? "pipelined" : "sequential");
  printf("\n");

  ndb_init();
  int result = doRun();
  ndb_end(0);

  printf(result == 0 ? "\n*** ALL ITERATIONS PASSED ***\n"
                      : "\n*** SOME ITERATIONS FAILED ***\n");
  return result;
}
