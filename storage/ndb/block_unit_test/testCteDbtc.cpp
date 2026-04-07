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
 * testCteDbtc — Integration test for DBTC CTE orchestration via
 *               the full DBTC → DBSPJ → DBLQH signal path.
 *
 * Uses SignalSender to send SCAN_TABREQ with:
 *   - 2 CTE subtree definitions (QN_CTE_SUBTREE + embedded scan+lookup)
 *   - A main query (scan + lookup self-join with aggregation)
 *   - CTE aggregation programs appended to the KeyInfo section
 *
 * This exercises the DBTC CTE interface:
 *   1. Parse CTE definitions from KeyInfo (numCtes, tableId, agg programs)
 *   2. Send JoinAggSetupReq with CTE_MODE_FLAG for each CTE
 *   3. Collect SETUP_CONFs, pack CTE aggStateKeys for DBSPJ
 *   4. Handle CTE_SCAN_COMPLETE_REP from DBSPJ instances
 *   5. Send JOIN_AGG_COMPLETE_REQ for CTE redistribution
 *   6. Send CTE_START_MAIN_REQ to start the main query
 *   7. Collect aggregated results from the main query
 *
 * SQL equivalent:
 *   WITH
 *     cte1 AS (SELECT grp, SUM(val) FROM src GROUP BY grp),
 *     cte2 AS (SELECT grp, COUNT(*)  FROM src GROUP BY grp)
 *   SELECT COUNT(*), SUM(t2.val)
 *   FROM src t1 JOIN src t2 ON t1.pk = t2.pk;
 *
 * The main query is a standard self-join (same as testJoinAggSpj).
 * The CTEs are materialized in parallel by DBSPJ, but the main query
 * result is independent — what we verify is that the CTE pipeline
 * doesn't break the main query execution.
 *
 * QueryTree topology (8 nodes):
 *   Node 0: QN_CTE_SUBTREE (cteId=0, numNodes=2)
 *   Node 1:   QN_SCAN_FRAG on src (CTE 0 scan, linked: pk)
 *   Node 2:   QN_LOOKUP on src (CTE 0 agg leaf, self-join)
 *   Node 3: QN_CTE_SUBTREE (cteId=1, numNodes=2)
 *   Node 4:   QN_SCAN_FRAG on src (CTE 1 scan, linked: pk)
 *   Node 5:   QN_LOOKUP on src (CTE 1 agg leaf, self-join)
 *   Node 6: QN_SCAN_FRAG on src (main scan, linked: pk)
 *   Node 7: QN_LOOKUP on src (main agg leaf, self-join)
 *
 * Test variations:
 *   Test 1: Basic 2-CTE + main COUNT/SUM (5 rows)
 *   Test 2: 2-CTE + main GROUP BY SUM (3 groups)
 *   Test 3: 2-CTE + larger dataset (100 rows, 10 groups)
 *   Test 4: 2-CTE + empty table
 *
 * Usage: testCteDbtc -c <connect_string> -m <mysql_port> [-v]
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
#include <kernel/signaldata/ScanTab.hpp>
#include <kernel/signaldata/TransIdAI.hpp>
#include <kernel/signaldata/QueryTree.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>

#include <NdbRestarter.hpp>
#include <mysql.h>

#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <vector>

/* Verbose output control */
static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *TABLE_NAME = "cte_dbtc_test";
static const char *TABLE_NAME_3COL = "cte_dbtc_test3";
static const Uint32 FAKE_TRANS_ID1 = 0xCCDD0001;
static const Uint32 FAKE_TRANS_ID2 = 0xCCDD0002;
static const Uint32 WAIT_TIMEOUT_MS = 60000;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;

/* ------------------------------------------------------------------ */
/* Aggregation program builders                                        */
/* ------------------------------------------------------------------ */

static std::vector<Uint32>
buildAggProgram_CountSum(Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 2u;  /* n_gb_cols=0, n_agg_results=2 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             sumColId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;
  prog[10] = (kOpSum << 26) | (0 << 16) | 1;
  return prog;
}

static std::vector<Uint32>
buildAggProgram_SumGroupBy(Uint32 gbColId, Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;  /* n_gb_cols=1, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             sumColId;
  prog[10] = (kOpSum << 26) | (0 << 16) | 0;
  return prog;
}

static std::vector<Uint32>
buildAggProgram_CountOnly(Uint32 dummyColId)
{
  const Uint32 PROG_LEN = 10;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 1u;  /* n_gb_cols=0, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             dummyColId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;
  return prog;
}

/* ------------------------------------------------------------------ */
/* QueryTree builders                                                  */
/* ------------------------------------------------------------------ */

/*
 * Build a QueryTree with 2 CTE subtrees + a 2-node main query.
 *
 * 8 nodes total:
 *   Nodes 0-2: CTE 0 (subtree container + scan + lookup)
 *   Nodes 3-5: CTE 1 (subtree container + scan + lookup)
 *   Nodes 6-7: Main query (scan + lookup self-join)
 *
 * All scans and lookups use the same table (self-join pattern).
 * CTE embedded nodes have NI_AGGREGATE + NI_AGGREGATE_LEAF on the leaf.
 * Main query leaf also has NI_AGGREGATE_LEAF.
 */
static std::vector<Uint32>
buildQueryTreeWithCtes(Uint32 tableId, Uint32 tableVersion,
                       Uint32 pkAttrId, Uint32 receiverId)
{
  std::vector<Uint32> ai;

  const Uint32 cte_sub_len = 4;
  const Uint32 cte_scan_len = 5;
  const Uint32 cte_leaf_len = 7;
  const Uint32 main_scan_len = 5;
  const Uint32 main_leaf_len = 7;

  const Uint32 tree_len = 1 +
    cte_sub_len + cte_scan_len + cte_leaf_len +
    cte_sub_len + cte_scan_len + cte_leaf_len +
    main_scan_len + main_leaf_len;

  /* Tree header */
  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 8, tree_len);
  ai.push_back(cnt_len);

  /* ---- CTE 0 subtree ---- */

  /* Node 0: QN_CTE_SUBTREE (cteId=0) */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_CTE_SUBTREE, cte_sub_len);
  ai.push_back(n0_len);
  ai.push_back(0);          /* requestInfo: reserved */
  ai.push_back(0);          /* cteId = 0 */
  ai.push_back(2);          /* numNodes = 2 (scan + lookup) */

  /* Node 1: QN_SCAN_FRAG (CTE 0 scan on src) */
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_SCAN_FRAG, cte_scan_len);
  ai.push_back(n1_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((pkAttrId << 16) | 1);  /* 1 linked attr: pk */

  /* Node 2: QN_LOOKUP (CTE 0 agg leaf, self-join by pk) */
  Uint32 n2_len = 0;
  QueryNode::setOpLen(n2_len, QueryNode::QN_LOOKUP, cte_leaf_len);
  ai.push_back(n2_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((1 << 16) | 1);           /* parent: node 1 */
  ai.push_back((0 << 16) | 1);           /* key: 1 pattern word */
  ai.push_back(QueryPattern::col(0));     /* pk from parent linked[0] */

  /* ---- CTE 1 subtree ---- */

  /* Node 3: QN_CTE_SUBTREE (cteId=1) */
  Uint32 n3_len = 0;
  QueryNode::setOpLen(n3_len, QueryNode::QN_CTE_SUBTREE, cte_sub_len);
  ai.push_back(n3_len);
  ai.push_back(0);
  ai.push_back(1);          /* cteId = 1 */
  ai.push_back(2);          /* numNodes = 2 */

  /* Node 4: QN_SCAN_FRAG (CTE 1 scan on src) */
  Uint32 n4_len = 0;
  QueryNode::setOpLen(n4_len, QueryNode::QN_SCAN_FRAG, cte_scan_len);
  ai.push_back(n4_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((pkAttrId << 16) | 1);

  /* Node 5: QN_LOOKUP (CTE 1 agg leaf) */
  Uint32 n5_len = 0;
  QueryNode::setOpLen(n5_len, QueryNode::QN_LOOKUP, cte_leaf_len);
  ai.push_back(n5_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((4 << 16) | 1);           /* parent: node 4 */
  ai.push_back((0 << 16) | 1);
  ai.push_back(QueryPattern::col(0));

  /* ---- Main query ---- */

  /* Node 6: QN_SCAN_FRAG (main scan) */
  Uint32 n6_len = 0;
  QueryNode::setOpLen(n6_len, QueryNode::QN_SCAN_FRAG, main_scan_len);
  ai.push_back(n6_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((pkAttrId << 16) | 1);

  /* Node 7: QN_LOOKUP (main agg leaf, self-join by pk) */
  Uint32 n7_len = 0;
  QueryNode::setOpLen(n7_len, QueryNode::QN_LOOKUP, main_leaf_len);
  ai.push_back(n7_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((6 << 16) | 1);           /* parent: node 6 */
  ai.push_back((0 << 16) | 1);
  ai.push_back(QueryPattern::col(0));

  /* ---- Parameter section (8 params) ---- */

  /* Param 0: QN_CteSubtreeParameters (CTE 0 container) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_CTE_SUBTREE,
                                  QN_CteSubtreeParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
  }

  /* Param 1: QN_ScanFragParameters (CTE 0 scan) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_SCAN_FRAG,
                                  QN_ScanFragParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
    ai.push_back(256);
    ai.push_back(65536);
    ai.push_back(0);
    ai.push_back(0);
    ai.push_back(0);
  }

  /* Param 2: QN_LookupParameters (CTE 0 leaf) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_LOOKUP,
                                  QN_LookupParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
  }

  /* Param 3: QN_CteSubtreeParameters (CTE 1 container) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_CTE_SUBTREE,
                                  QN_CteSubtreeParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
  }

  /* Param 4: QN_ScanFragParameters (CTE 1 scan) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_SCAN_FRAG,
                                  QN_ScanFragParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
    ai.push_back(256);
    ai.push_back(65536);
    ai.push_back(0);
    ai.push_back(0);
    ai.push_back(0);
  }

  /* Param 5: QN_LookupParameters (CTE 1 leaf) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_LOOKUP,
                                  QN_LookupParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
  }

  /* Param 6: QN_ScanFragParameters (main scan) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_SCAN_FRAG,
                                  QN_ScanFragParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
    ai.push_back(256);
    ai.push_back(65536);
    ai.push_back(0);
    ai.push_back(0);
    ai.push_back(0);
  }

  /* Param 7: QN_LookupParameters (main leaf) */
  {
    Uint32 p_len = 0;
    QueryNodeParameters::setOpLen(p_len, QueryNodeParameters::QN_LOOKUP,
                                  QN_LookupParameters::NodeSize);
    ai.push_back(p_len);
    ai.push_back(0);
    ai.push_back(receiverId);
  }

  return ai;
}

/* ------------------------------------------------------------------ */
/* Table setup via MySQL + NDB dictionary                              */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 attrIdA;  /* pk / first column */
  Uint32 attrIdB;  /* second column */
  Uint32 attrIdC;  /* third column (optional) */
  Uint32 fragCount;
};

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

static int
getTableMeta(Ndb *ndb, const char *tableName, TableMeta &meta,
             const char *colA, const char *colB, const char *colC = nullptr)
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
  meta.attrIdA = ptab->getColumn(colA)->getAttrId();
  meta.attrIdB = ptab->getColumn(colB)->getAttrId();
  meta.attrIdC = colC ? ptab->getColumn(colC)->getAttrId() : 0;
  meta.fragCount = ptab->getFragmentCount();
  return 0;
}

static int
createTestTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_dbtc_test");
  if (sqlExec(conn,
        "CREATE TABLE cte_dbtc_test ("
        "  a BIGINT NOT NULL PRIMARY KEY,"
        "  b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;
  return getTableMeta(ndb, TABLE_NAME, meta, "a", "b");
}

static int
createTestTable3Col(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_dbtc_test3");
  if (sqlExec(conn,
        "CREATE TABLE cte_dbtc_test3 ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;
  return getTableMeta(ndb, TABLE_NAME_3COL, meta, "pk", "grp", "val");
}

static void dropTestTable(MYSQL *conn) {
  sqlExec(conn, "DROP TABLE IF EXISTS cte_dbtc_test");
}

static void dropTestTable3Col(MYSQL *conn) {
  sqlExec(conn, "DROP TABLE IF EXISTS cte_dbtc_test3");
}

/* ------------------------------------------------------------------ */
/* MySQL client                                                        */
/* ------------------------------------------------------------------ */

static MYSQL *
connectMysql(int mysqlPort)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) return nullptr;
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         "test", mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
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
/* TC connect seize / release                                          */
/* ------------------------------------------------------------------ */

static int
seizeTcConnect(SignalSender &ss, Uint32 nodeId,
               Uint32 &apiConnectPtrOut, Uint32 &tcRefOut)
{
  V("TCSEIZEREQ -> node %u\n", nodeId);
  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  data[0] = 0;
  data[1] = ss.getOwnRef();
  ssig.set(ss, 0, DBTC, GSN_TCSEIZEREQ, 2);
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal TCSEIZEREQ failed\n");
    return -1;
  }
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TCSEIZECONF");
  if (resp == nullptr) return -1;
  int gsn = getGsn(resp);
  if (gsn == GSN_TCSEIZECONF) {
    apiConnectPtrOut = resp->getDataPtr()[1];
    tcRefOut = resp->getDataPtr()[2];
    V("TCSEIZECONF: apiConnectPtr=%u tcRef=0x%08x\n",
      apiConnectPtrOut, tcRefOut);
    return 0;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for TCSEIZECONF\n", gsn);
  return -1;
}

static int
releaseTcConnect(SignalSender &ss, Uint32 nodeId,
                 Uint32 apiConnectPtr, Uint32 tcRef)
{
  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  data[0] = apiConnectPtr;
  data[1] = ss.getOwnRef();
  data[2] = 0;
  ssig.set(ss, 0, refToBlock(tcRef), GSN_TCRELEASEREQ, 3);
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) return -1;
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TCRELEASECONF");
  if (resp == nullptr) return -1;
  return (getGsn(resp) == GSN_TCRELEASECONF) ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* SCAN_TABREQ sender with CTE definitions                             */
/* ------------------------------------------------------------------ */

/*
 * Friend function of ScanTabReq — access private setter methods.
 */
Uint32 buildScanTabReqInfo()
{
  Uint32 requestInfo = 0;
  ScanTabReq::setReadCommittedFlag(requestInfo, 1);
  ScanTabReq::setNoDiskFlag(requestInfo, 1);
  ScanTabReq::setViaSPJFlag(requestInfo, 1);
  ScanTabReq::setJoinAggFlag(requestInfo, 1);
  ScanTabReq::setScanBatch(requestInfo, 256);
  ScanTabReq::setExtendedConf(requestInfo, 1);
  return requestInfo;
}

/*
 * Send SCAN_TABREQ with 2 CTE definitions appended to section 2.
 *
 * Section 2 layout (KeyInfo):
 *   [boundsLen=0]
 *   [aggReceiverId]
 *   [mainAggProgram...]        (self-describing: word[0] = (magic<<16)|len)
 *   [numCtes=2]
 *   [cte0_tableId] [cte0_schemaVersion] [cte0_progLen] [cte0_prog...]
 *   [cte1_tableId] [cte1_schemaVersion] [cte1_progLen] [cte1_prog...]
 */
static int
sendScanTabReqWithCtes(SignalSender &ss, Uint32 nodeId,
                       Uint32 apiConnectPtr, Uint32 tcRef,
                       const TableMeta &meta,
                       const std::vector<Uint32> &queryTree,
                       const std::vector<Uint32> &mainAggProgram,
                       const std::vector<Uint32> &cte0AggProgram,
                       const std::vector<Uint32> &cte1AggProgram,
                       Uint32 receiverId)
{
  V("SCAN_TABREQ -> node %u, table=%u (with 2 CTEs)\n",
    nodeId, meta.tableId);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  memset(data, 0, 25 * sizeof(Uint32));

  data[0] = apiConnectPtr;
  data[1] = 0;
  data[2] = buildScanTabReqInfo();
  data[3] = meta.tableId;
  data[4] = meta.schemaVersion;
  data[5] = 0xFFFF;              /* storedProcId = RNIL */
  data[6] = FAKE_TRANS_ID1;
  data[7] = FAKE_TRANS_ID2;
  data[8] = apiConnectPtr;        /* buddyConPtr = self */
  data[9] = 65536;                /* batch_byte_size */
  data[10] = 256;                 /* first_batch_size */
  data[15] = 1;                   /* scanParallelism */

  ssig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_TABREQ, 16);

  /* Build combined agg section: main program + CTE definitions */
  std::vector<Uint32> aggSection;
  aggSection.push_back(0);            /* boundsLen = 0 */
  aggSection.push_back(receiverId);   /* aggReceiverId */
  aggSection.insert(aggSection.end(),
                    mainAggProgram.begin(), mainAggProgram.end());

  /* CTE definitions */
  aggSection.push_back(2);            /* numCtes */
  /* CTE 0 */
  aggSection.push_back(meta.tableId);
  aggSection.push_back(meta.schemaVersion);
  aggSection.push_back((Uint32)cte0AggProgram.size());
  aggSection.insert(aggSection.end(),
                    cte0AggProgram.begin(), cte0AggProgram.end());
  /* CTE 1 */
  aggSection.push_back(meta.tableId);
  aggSection.push_back(meta.schemaVersion);
  aggSection.push_back((Uint32)cte1AggProgram.size());
  aggSection.insert(aggSection.end(),
                    cte1AggProgram.begin(), cte1AggProgram.end());

  ssig.header.m_noOfSections = 3;
  Uint32 dummyReceiverId = 0;
  ssig.ptr[0].p = &dummyReceiverId;
  ssig.ptr[0].sz = 1;
  ssig.ptr[1].p = queryTree.data();
  ssig.ptr[1].sz = (Uint32)queryTree.size();
  ssig.ptr[2].p = aggSection.data();
  ssig.ptr[2].sz = (Uint32)aggSection.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SCAN_TABREQ failed\n");
    return -1;
  }

  V("  Sent: queryTree=%zu words, aggSection=%zu words "
    "(main=%zu, cte0=%zu, cte1=%zu)\n",
    queryTree.size(), aggSection.size(),
    mainAggProgram.size(), cte0AggProgram.size(), cte1AggProgram.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Result parsing                                                      */
/* ------------------------------------------------------------------ */

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

static Int64
extractSumBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;
  Uint32 offset = aggIdx * ITEM_SIZE + 8;
  if (offset + 8 > val.size()) return 0;
  Int64 v;
  memcpy(&v, val.data() + offset, sizeof(Int64));
  return v;
}

static Uint64
extractCountBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;
  Uint32 offset = aggIdx * ITEM_SIZE + 8;
  if (offset + 8 > val.size()) return 0;
  Uint64 v;
  memcpy(&v, val.data() + offset, sizeof(Uint64));
  return v;
}

static Int64
extractGroupKey(const std::vector<Uint8> &key)
{
  const Uint32 ATTR_HEADER_SIZE = 4;
  if (key.size() < ATTR_HEADER_SIZE + 8) return 0;
  Int64 v;
  memcpy(&v, key.data() + ATTR_HEADER_SIZE, sizeof(Int64));
  return v;
}

/* ------------------------------------------------------------------ */
/* Result collection                                                   */
/* ------------------------------------------------------------------ */

static int
collectResults(SignalSender &ss,
               std::vector<AggResult> &allResults,
               Uint32 apiConnectPtr, Uint32 tcRef, Uint32 nodeId)
{
  V("Waiting for results...\n");
  bool done = false;
  while (!done) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS,
                                       "TRANSID_AI/SCAN_TABCONF");
    if (resp == nullptr) return -1;
    int gsn = getGsn(resp);

    if (gsn == GSN_TRANSID_AI) {
      AggResult result;
      if (parseTransIdAI(resp, result) != 0) return -1;
      V("  TRANSID_AI: n_gb_cols=%u n_agg=%u n_groups=%u\n",
        result.n_gb_cols, result.n_agg_results, result.n_groups);
      allResults.push_back(std::move(result));
    }
    else if (gsn == GSN_SCAN_TABCONF) {
      const Uint32 *d = resp->getDataPtr();
      Uint32 ri = d[1];
      bool endOfData = (ri & ScanTabConf::EndOfData) != 0;
      Uint32 ops = ri & 0xFF;
      V("  SCAN_TABCONF: ops=%u endOfData=%d\n", ops, (int)endOfData);

      if (endOfData) {
        done = true;
      } else {
        Uint32 sigLen = resp->header.theLength;
        Uint32 words_per_op = ops > 0 ? (sigLen - 4) / ops : 4;

        SimpleSignal nextSig;
        Uint32 *ndata = nextSig.getDataPtrSend();
        ndata[0] = apiConnectPtr;
        ndata[1] = 0;
        ndata[2] = FAKE_TRANS_ID1;
        ndata[3] = FAKE_TRANS_ID2;

        Uint32 ackCount = 0;
        for (Uint32 i = 0; i < ops; i++) {
          Uint32 tcPtrI = d[4 + i * words_per_op + 1];
          if (tcPtrI != RNIL) {
            ndata[4 + ackCount] = tcPtrI;
            ackCount++;
          }
        }

        nextSig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_NEXTREQ,
                    4 + ackCount);
        nextSig.header.m_noOfSections = 0;
        if (ss.sendSignal(nodeId, &nextSig) != SEND_OK) {
          fprintf(stderr, "sendSignal SCAN_NEXTREQ failed\n");
          return -1;
        }
      }
    }
    else if (gsn == GSN_SCAN_TABREF) {
      fprintf(stderr, "SCAN_TABREF: errorCode=%u\n",
              resp->getDataPtr()[3]);
      return -1;
    }
    else {
      V("  Ignoring GSN %d\n", gsn);
    }
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* SQL verification helpers                                            */
/* ------------------------------------------------------------------ */

static int
verifySqlCountSum(MYSQL *conn, const char *table,
                  const char *pkCol, const char *sumCol,
                  Uint64 expectedCount, Int64 expectedSum)
{
  char query[512];
  snprintf(query, sizeof(query),
           "SELECT COUNT(*), SUM(t2.%s) FROM %s t1 "
           "JOIN %s t2 ON t1.%s = t2.%s",
           sumCol, table, table, pkCol, pkCol);
  if (mysql_query(conn, query) != 0) return -1;
  MYSQL_RES *res = mysql_store_result(conn);
  if (res == nullptr) return -1;
  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == nullptr || row[0] == nullptr || row[1] == nullptr) {
    mysql_free_result(res);
    return -1;
  }
  Uint64 sqlCount = (Uint64)atoll(row[0]);
  Int64 sqlSum = (Int64)atoll(row[1]);
  mysql_free_result(res);
  if (sqlCount != expectedCount || sqlSum != expectedSum) {
    fprintf(stderr, "SQL verify mismatch: COUNT=%llu SUM=%lld vs "
            "expected COUNT=%llu SUM=%lld\n",
            (unsigned long long)sqlCount, (long long)sqlSum,
            (unsigned long long)expectedCount, (long long)expectedSum);
    return -1;
  }
  V("  SQL verify: COUNT=%llu SUM=%lld — matches\n",
    (unsigned long long)sqlCount, (long long)sqlSum);
  return 0;
}

static int
verifySqlSumGroupBy(MYSQL *conn, const char *table,
                    const char *pkCol, const char *grpCol,
                    const char *sumCol,
                    const std::map<Int64, Int64> &expectedGroups)
{
  char query[512];
  snprintf(query, sizeof(query),
           "SELECT t2.%s, SUM(t2.%s) FROM %s t1 "
           "JOIN %s t2 ON t1.%s = t2.%s GROUP BY t2.%s",
           grpCol, sumCol, table, table, pkCol, pkCol, grpCol);
  if (mysql_query(conn, query) != 0) return -1;
  MYSQL_RES *res = mysql_store_result(conn);
  if (res == nullptr) return -1;
  std::map<Int64, Int64> sqlGroups;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    if (row[0] && row[1])
      sqlGroups[(Int64)atoll(row[0])] = (Int64)atoll(row[1]);
  }
  mysql_free_result(res);
  if (sqlGroups != expectedGroups) {
    fprintf(stderr, "SQL verify mismatch: %zu groups vs %zu expected\n",
            sqlGroups.size(), expectedGroups.size());
    return -1;
  }
  V("  SQL verify: %zu groups — matches\n", sqlGroups.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test runner helper                                                   */
/* ------------------------------------------------------------------ */

/*
 * Common pattern: create table, seize TC, build tree, send, collect,
 * validate, release, cleanup.
 */
struct TestCtx {
  Ndb *ndb;
  SignalSender *ss;
  Uint32 nodeId;
  MYSQL *conn;
};

/* ------------------------------------------------------------------ */
/* Test 1: Basic 2-CTE + main COUNT/SUM (5 rows)                      */
/* ------------------------------------------------------------------ */

static int
testBasicTwoCtes(TestCtx &ctx)
{
  printf("Test 1: Basic 2-CTE + main COUNT/SUM (5 rows) ... ");
  fflush(stdout);

  ctx.ss->unlock();
  TableMeta meta;
  int rc = createTestTable(ctx.conn, ctx.ndb, meta);
  if (rc == 0)
    rc = sqlExec(ctx.conn,
                 "INSERT INTO cte_dbtc_test VALUES "
                 "(1,10),(2,20),(3,30),(4,40),(5,50)");
  ctx.ss->lock();
  if (rc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef) != 0) {
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  Uint32 receiverId = 100;

  /* Main agg: COUNT(*), SUM(b) */
  std::vector<Uint32> mainAgg = buildAggProgram_CountSum(meta.attrIdB);

  /* CTE 0 agg: SUM(b) GROUP BY (none) — just accumulate */
  std::vector<Uint32> cte0Agg = buildAggProgram_CountSum(meta.attrIdB);

  /* CTE 1 agg: COUNT(*) only */
  std::vector<Uint32> cte1Agg = buildAggProgram_CountOnly(meta.attrIdB);

  std::vector<Uint32> queryTree =
    buildQueryTreeWithCtes(meta.tableId, meta.schemaVersion,
                           meta.attrIdA, receiverId);

  V("QueryTree: %zu words, mainAgg: %zu, cte0Agg: %zu, cte1Agg: %zu\n",
    queryTree.size(), mainAgg.size(), cte0Agg.size(), cte1Agg.size());

  rc = sendScanTabReqWithCtes(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef,
                              meta, queryTree, mainAgg, cte0Agg, cte1Agg,
                              receiverId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(*ctx.ss, results, apiConnectPtr, tcRef, ctx.nodeId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);

  /* Merge results from all nodes */
  Uint64 totalCount = 0;
  Int64 totalSum = 0;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      totalCount += extractCountBigint(g.second, 0);
      totalSum += extractSumBigint(g.second, 1);
    }
  }

  V("  Result: COUNT=%llu, SUM=%lld\n",
    (unsigned long long)totalCount, (long long)totalSum);

  if (totalCount != 5 || totalSum != 150) {
    printf("FAIL (expected COUNT=5 SUM=150, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  ctx.ss->unlock();
  int sqlRc = verifySqlCountSum(ctx.conn, TABLE_NAME, "a", "b", 5, 150);
  ctx.ss->lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification)\n");
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  printf("PASS\n");
  ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: 2-CTE + main GROUP BY SUM (3 groups)                       */
/* ------------------------------------------------------------------ */

static int
testTwoCtesGroupBy(TestCtx &ctx)
{
  printf("Test 2: 2-CTE + main GROUP BY SUM (3 groups) ... ");
  fflush(stdout);

  ctx.ss->unlock();
  TableMeta meta;
  int rc = createTestTable3Col(ctx.conn, ctx.ndb, meta);
  if (rc == 0)
    rc = sqlExec(ctx.conn,
                 "INSERT INTO cte_dbtc_test3 VALUES "
                 "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,2,50),(6,3,60)");
  ctx.ss->lock();
  if (rc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef) != 0) {
    ctx.ss->unlock(); dropTestTable3Col(ctx.conn); ctx.ss->lock();
    return -1;
  }

  Uint32 receiverId = 101;

  /* Main agg: SUM(val) GROUP BY grp */
  std::vector<Uint32> mainAgg =
    buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  /* CTE 0 agg: SUM(val) GROUP BY grp (same program — materializes the CTE) */
  std::vector<Uint32> cte0Agg =
    buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  /* CTE 1 agg: COUNT(*) GROUP BY grp — COUNT per group */
  std::vector<Uint32> cte1Agg =
    buildAggProgram_CountOnly(meta.attrIdC);

  std::vector<Uint32> queryTree =
    buildQueryTreeWithCtes(meta.tableId, meta.schemaVersion,
                           meta.attrIdA, receiverId);

  rc = sendScanTabReqWithCtes(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef,
                              meta, queryTree, mainAgg, cte0Agg, cte1Agg,
                              receiverId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable3Col(ctx.conn); ctx.ss->lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(*ctx.ss, results, apiConnectPtr, tcRef, ctx.nodeId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable3Col(ctx.conn); ctx.ss->lock();
    return -1;
  }

  releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);

  /* Merge GROUP BY groups across nodes */
  std::map<Int64, Int64> groupSums;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      Int64 grpKey = extractGroupKey(g.first);
      Int64 sumVal = extractSumBigint(g.second, 0);
      groupSums[grpKey] += sumVal;
    }
  }

  V("  Groups: %zu\n", groupSums.size());
  for (auto &kv : groupSums)
    V("    group(%lld) = %lld\n", (long long)kv.first, (long long)kv.second);

  /* Expected: self-join on pk → each row appears once.
   * group(1): val 10+20 = 30, group(2): 30+40+50 = 120, group(3): 60 */
  std::map<Int64, Int64> expected = {{1, 30}, {2, 120}, {3, 60}};
  if (groupSums != expected) {
    printf("FAIL (unexpected groups)\n");
    ctx.ss->unlock(); dropTestTable3Col(ctx.conn); ctx.ss->lock();
    return -1;
  }

  ctx.ss->unlock();
  int sqlRc = verifySqlSumGroupBy(ctx.conn, TABLE_NAME_3COL,
                                   "pk", "grp", "val", expected);
  ctx.ss->lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification)\n");
    ctx.ss->unlock(); dropTestTable3Col(ctx.conn); ctx.ss->lock();
    return -1;
  }

  printf("PASS\n");
  ctx.ss->unlock(); dropTestTable3Col(ctx.conn); ctx.ss->lock();
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: 2-CTE + larger dataset (100 rows, 10 groups)                */
/* ------------------------------------------------------------------ */

static int
testTwoCtesLargeDataset(TestCtx &ctx)
{
  printf("Test 3: 2-CTE + larger dataset (100 rows, 10 groups) ... ");
  fflush(stdout);

  ctx.ss->unlock();
  TableMeta meta;
  int rc = createTestTable(ctx.conn, ctx.ndb, meta);
  if (rc == 0) {
    /* 100 rows: a=1..100, b=a → SUM(b) = 100*101/2 = 5050 */
    char buf[8192];
    int pos = snprintf(buf, sizeof(buf),
                       "INSERT INTO cte_dbtc_test VALUES ");
    for (int i = 1; i <= 100; i++) {
      if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
      pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d)", i, i);
    }
    rc = sqlExec(ctx.conn, buf);
  }
  ctx.ss->lock();
  if (rc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef) != 0) {
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  Uint32 receiverId = 102;
  std::vector<Uint32> mainAgg = buildAggProgram_CountSum(meta.attrIdB);
  std::vector<Uint32> cte0Agg = buildAggProgram_CountSum(meta.attrIdB);
  std::vector<Uint32> cte1Agg = buildAggProgram_CountOnly(meta.attrIdB);

  std::vector<Uint32> queryTree =
    buildQueryTreeWithCtes(meta.tableId, meta.schemaVersion,
                           meta.attrIdA, receiverId);

  rc = sendScanTabReqWithCtes(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef,
                              meta, queryTree, mainAgg, cte0Agg, cte1Agg,
                              receiverId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(*ctx.ss, results, apiConnectPtr, tcRef, ctx.nodeId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);

  Uint64 totalCount = 0;
  Int64 totalSum = 0;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      totalCount += extractCountBigint(g.second, 0);
      totalSum += extractSumBigint(g.second, 1);
    }
  }

  V("  Result: COUNT=%llu, SUM=%lld\n",
    (unsigned long long)totalCount, (long long)totalSum);

  if (totalCount != 100 || totalSum != 5050) {
    printf("FAIL (expected COUNT=100 SUM=5050, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  ctx.ss->unlock();
  int sqlRc = verifySqlCountSum(ctx.conn, TABLE_NAME, "a", "b", 100, 5050);
  ctx.ss->lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification)\n");
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  printf("PASS\n");
  ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: 2-CTE + empty table                                        */
/* ------------------------------------------------------------------ */

static int
testTwoCtesEmptyTable(TestCtx &ctx)
{
  printf("Test 4: 2-CTE + empty table ... ");
  fflush(stdout);

  ctx.ss->unlock();
  TableMeta meta;
  int rc = createTestTable(ctx.conn, ctx.ndb, meta);
  /* No data inserted */
  ctx.ss->lock();
  if (rc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef) != 0) {
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  Uint32 receiverId = 103;
  std::vector<Uint32> mainAgg = buildAggProgram_CountSum(meta.attrIdB);
  std::vector<Uint32> cte0Agg = buildAggProgram_CountSum(meta.attrIdB);
  std::vector<Uint32> cte1Agg = buildAggProgram_CountOnly(meta.attrIdB);

  std::vector<Uint32> queryTree =
    buildQueryTreeWithCtes(meta.tableId, meta.schemaVersion,
                           meta.attrIdA, receiverId);

  rc = sendScanTabReqWithCtes(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef,
                              meta, queryTree, mainAgg, cte0Agg, cte1Agg,
                              receiverId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(*ctx.ss, results, apiConnectPtr, tcRef, ctx.nodeId);
  if (rc != 0) {
    releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  releaseTcConnect(*ctx.ss, ctx.nodeId, apiConnectPtr, tcRef);

  Uint64 totalCount = 0;
  Int64 totalSum = 0;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      totalCount += extractCountBigint(g.second, 0);
      totalSum += extractSumBigint(g.second, 1);
    }
  }

  V("  Result: COUNT=%llu, SUM=%lld\n",
    (unsigned long long)totalCount, (long long)totalSum);

  if (totalCount != 0 || totalSum != 0) {
    printf("FAIL (expected COUNT=0 SUM=0, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
    return -1;
  }

  printf("PASS\n");
  ctx.ss->unlock(); dropTestTable(ctx.conn); ctx.ss->lock();
  return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
  const char *connectString = nullptr;
  int mysqlPort = 3306;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n\n"
             "Options:\n"
             "  -c <connect_string>  NDB mgmd connect string (default: localhost:1186)\n"
             "  -m <mysql_port>      MySQL port (default: 3306)\n"
             "  -v, --verbose        Verbose output\n"
             "  -h, --help           Show this help\n",
             argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if ((strcmp(argv[i], "-m") == 0 ||
                strcmp(argv[i], "--mysql-port") == 0) && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    }
  }
  if (connectString == nullptr) connectString = "localhost:1186";

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int result = 0;
  V("Connecting to cluster: %s\n", connectString);

  do {
    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      result = 1;
      break;
    }
    V("Connected to MySQL on port %d\n", mysqlPort);

    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      mysql_close(conn);
      result = 1;
      break;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30 seconds\n");
      mysql_close(conn);
      result = 1;
      break;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
      mysql_close(conn);
      result = 1;
      break;
    }

    NdbRestarter restarter(connectString);
    int nodeId = restarter.getDbNodeId(0);
    if (nodeId <= 0) {
      fprintf(stderr, "No data node found\n");
      mysql_close(conn);
      result = 1;
      break;
    }
    V("Using data node %d\n", nodeId);

    {
      SignalSender ss(&con);
      ss.lock();
      V("SignalSender: block=%u node=%u ref=0x%08x\n",
        refToBlock(ss.getOwnRef()),
        refToNode(ss.getOwnRef()),
        ss.getOwnRef());

      TestCtx tctx = {&ndb, &ss, (Uint32)nodeId, conn};

      if (testBasicTwoCtes(tctx) != 0) result = 1;
      if (testTwoCtesGroupBy(tctx) != 0) result = 1;
      if (testTwoCtesLargeDataset(tctx) != 0) result = 1;
      if (testTwoCtesEmptyTable(tctx) != 0) result = 1;

      ss.unlock();
    }

    mysql_close(conn);
  } while (0);

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
