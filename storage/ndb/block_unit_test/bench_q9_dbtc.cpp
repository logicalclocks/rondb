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
 * bench_q9_dbtc — TPC-H Q9 benchmark through the full DBTC→DBSPJ→DBLQH path.
 *
 * Sends SCAN_TABREQ to DBTC with a 6-node QueryTree encoding:
 *   Node 0: QN_SCAN_FRAG on LINEITEM (root)
 *   Node 1: QN_LOOKUP on PART (filter: p_name LIKE '%green%')
 *   Node 2: QN_LOOKUP on ORDERS (linked: o_orderyear)
 *   Node 3: QN_LOOKUP on SUPPLIER (linked: s_nationkey)
 *   Node 4: QN_LOOKUP on PARTSUPP (linked: ps_supplycost)
 *   Node 5: QN_LOOKUP on NATION (aggregate leaf, GROUP BY n_name, o_orderyear)
 *
 * Q9 query:
 *   SELECT n_name, o_orderyear,
 *          SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity)
 *   FROM lineitem JOIN part ON p_partkey = l_partkey
 *     JOIN orders ON o_orderkey = l_orderkey
 *     JOIN supplier ON s_suppkey = l_suppkey
 *     JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey
 *     JOIN nation ON n_nationkey = s_nationkey
 *   WHERE p_name LIKE '%green%'
 *   GROUP BY n_name, o_orderyear
 *
 * Assumes tables loaded by load_tpch.
 *
 * Usage: bench_q9_dbtc -c <connect_string> -m <mysql_port> [options]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include "../src/ndbapi/SignalSender.hpp"

#include <kernel/BlockNumbers.h>
#include <kernel/GlobalSignalNumbers.h>
#include <kernel/signaldata/ScanTab.hpp>
#include <kernel/signaldata/TransIdAI.hpp>
#include <kernel/signaldata/QueryTree.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>

#ifdef NONE
#undef NONE
#endif
#include <kernel/Interpreter.hpp>

#include <NdbRestarter.hpp>
#include <util/rondb_hash.hpp>
#include <mysql.h>

#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

/* ------------------------------------------------------------------ */
/* Verbose output control                                              */
/* ------------------------------------------------------------------ */

static bool verbose = false;
static Uint32 scanParallel = 8;
static Uint32 numReceivers = 1;
static MYSQL *g_mysql_conn = nullptr;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const Uint32 FAKE_TRANS_ID1 = 0xBE4D0001;
static const Uint32 FAKE_TRANS_ID2 = 0xBE4D0002;
static const Uint32 WAIT_TIMEOUT_MS = 120000;

static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;
static const Uint32 LINKED_COL_FLAG = 0x8000;

/* ------------------------------------------------------------------ */
/* Timing helpers                                                      */
/* ------------------------------------------------------------------ */

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

static double elapsedMs(TimePoint start, TimePoint end)
{
  return std::chrono::duration<double, std::milli>(end - start).count();
}

/* ------------------------------------------------------------------ */
/* Table metadata                                                      */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
  std::map<std::string, Uint32> attrIds;
};

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

  V("Table '%s': id=%u version=%u frags=%u\n",
    tableName, meta.tableId, meta.schemaVersion, meta.fragCount);
  return 0;
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
  V("TCSEIZEREQ → node %u\n", nodeId);

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
  } else if (gsn == GSN_TCSEIZEREF) {
    fprintf(stderr, "TCSEIZEREF: errorCode=%u\n", resp->getDataPtr()[1]);
    return -1;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for TCSEIZECONF\n", gsn);
  return -1;
}

static int
releaseTcConnect(SignalSender &ss, Uint32 nodeId,
                 Uint32 apiConnectPtr, Uint32 tcRef)
{
  V("TCRELEASEREQ → node %u, apiConnectPtr=%u\n", nodeId, apiConnectPtr);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  data[0] = apiConnectPtr;
  data[1] = ss.getOwnRef();
  data[2] = 0;

  ssig.set(ss, 0, refToBlock(tcRef), GSN_TCRELEASEREQ, 3);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal TCRELEASEREQ failed\n");
    return -1;
  }

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TCRELEASECONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_TCRELEASECONF) {
    V("TCRELEASECONF received\n");
    return 0;
  } else if (gsn == GSN_TCRELEASEREF) {
    fprintf(stderr, "TCRELEASEREF: errorCode=%u\n", resp->getDataPtr()[1]);
    return -1;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for TCRELEASECONF\n", gsn);
  return -1;
}

/* ------------------------------------------------------------------ */
/* PART LIKE filter (NDB interpreted program for PI_ATTR_INTERPRET)    */
/* ------------------------------------------------------------------ */

/*
 * Build an NDB interpreted program that filters: p_name LIKE '%green%'
 * Returns the instruction words (without label meta-info).
 */
static std::vector<Uint32>
buildPartFilter(const NdbDictionary::Table *partTab, Uint32 pNameAttrId)
{
  Uint32 buf[256];
  NdbInterpretedCode code(partTab, buf, sizeof(buf) / sizeof(buf[0]));

  const char pattern[] = "%green%";
  code.branch_col_notlike(pattern, (Uint32)strlen(pattern), pNameAttrId, 0);
  code.interpret_exit_ok();
  code.def_label(0);
  code.interpret_exit_nok();

  int rc = code.finalise();
  if (rc != 0) {
    fprintf(stderr, "NdbInterpretedCode::finalise() failed: %d\n", rc);
    return {};
  }

  Uint32 numLabels = 1;
  Uint32 len = code.getWordsUsed() - numLabels * 2;

  V("PART filter program: %u words (getWordsUsed=%u, labels=%u)\n",
    len, code.getWordsUsed(), numLabels);

  return std::vector<Uint32>(buf, buf + len);
}

/* ------------------------------------------------------------------ */
/* QueryTree builder — Q9 with 6-node linear chain                     */
/* ------------------------------------------------------------------ */

/*
 * 6-Node QueryTree (linear chain):
 *
 *   Node 0: LINEITEM scan (root)
 *     NI_AGGREGATE | NI_LINKED_ATTR
 *     linked[0..5]: l_orderkey, l_partkey, l_suppkey,
 *                   l_extendedprice, l_discount, l_quantity
 *
 *   Node 1: PART lookup (parent=0, key=col(1)=l_partkey)
 *     NI_HAS_PARENT | NI_KEY_LINKED | NI_INNER_JOIN | NI_AGGREGATE
 *     Filter: PI_ATTR_INTERPRET in parameters (not in tree)
 *
 *   Node 2: ORDERS lookup (parent=1, key=parent(1),col(0)=l_orderkey)
 *     NI_HAS_PARENT | NI_KEY_LINKED | NI_INNER_JOIN |
 *     NI_AGGREGATE | NI_LINKED_ATTR
 *     linked[0]: o_orderyear
 *
 *   Node 3: SUPPLIER lookup (parent=2, key=parent(2),col(2)=l_suppkey)
 *     NI_HAS_PARENT | NI_KEY_LINKED | NI_INNER_JOIN |
 *     NI_AGGREGATE | NI_LINKED_ATTR
 *     linked[0]: s_nationkey
 *
 *   Node 4: PARTSUPP lookup (parent=3,
 *            key=parent(3),col(1) + parent(3),col(2))
 *     NI_HAS_PARENT | NI_KEY_LINKED | NI_INNER_JOIN |
 *     NI_AGGREGATE | NI_LINKED_ATTR
 *     Composite key: l_partkey + l_suppkey from LINEITEM
 *     linked[0]: ps_supplycost
 *
 *   Node 5: NATION lookup (parent=4,
 *            key=parent(1),col(0)=s_nationkey from SUPPLIER)
 *     NI_HAS_PARENT | NI_KEY_LINKED | NI_INNER_JOIN |
 *     NI_AGGREGATE | NI_AGGREGATE_LEAF |
 *     NI_ATTR_INTERPRET | NI_ATTR_LINKED
 *     NI_ATTR_LINKED pattern: o_orderyear, l_extendedprice, l_discount,
 *                             l_quantity, ps_supplycost
 *
 * Parent chain from Node 5:
 *   parent(0)=PARTSUPP, (1)=SUPPLIER, (2)=ORDERS,
 *   (3)=PART, (4)=LINEITEM
 */
static std::vector<Uint32>
buildQueryTree_Q9(const TableMeta &lineitemMeta,
                  const TableMeta &partMeta,
                  const TableMeta &ordersMeta,
                  const TableMeta &supplierMeta,
                  const TableMeta &partsuppMeta,
                  const TableMeta &nationMeta,
                  const std::vector<Uint32> &partFilter,
                  Uint32 receiverId)
{
  /* LINEITEM linked attrs */
  Uint32 li_orderkey = lineitemMeta.attrIds.at("l_orderkey");
  Uint32 li_partkey = lineitemMeta.attrIds.at("l_partkey");
  Uint32 li_suppkey = lineitemMeta.attrIds.at("l_suppkey");
  Uint32 li_extendedprice = lineitemMeta.attrIds.at("l_extendedprice");
  Uint32 li_discount = lineitemMeta.attrIds.at("l_discount");
  Uint32 li_quantity = lineitemMeta.attrIds.at("l_quantity");

  /* ORDERS linked attr */
  Uint32 o_orderyear = ordersMeta.attrIds.at("o_orderyear");

  /* SUPPLIER linked attr */
  Uint32 s_nationkey = supplierMeta.attrIds.at("s_nationkey");

  /* PARTSUPP linked attr */
  Uint32 ps_supplycost = partsuppMeta.attrIds.at("ps_supplycost");

  std::vector<Uint32> ai;

  /* ---- Tree section ---- */

  /*
   * Node 0: QN_SCAN_FRAG (4 fixed + 4 NI_LINKED_ATTR = 8 words)
   *   6 linked attrs packed: 1 header + 3 data = 4 words
   */
  const Uint32 node0_len = 8;

  /*
   * Node 1: QN_LOOKUP (4 fixed + 1 parent + 2 key + 1 interp len
   *          + N interp program)
   *   NI_ATTR_INTERPRET: LIKE filter in tree (not PI_ATTR_INTERPRET)
   *   PI_ATTR_INTERPRET in params doesn't work for lookups without
   *   NI_LINKED_ATTR/NI_ATTR_INTERPRET/PI_ATTR_LIST (DBSPJ skips
   *   the entire attr-handling block).
   */
  const Uint32 node1_len = 7 + 1 + (Uint32)partFilter.size();

  /*
   * Node 2: QN_LOOKUP (4 fixed + 1 parent + 2 key + 1 linked = 8 + 1 key word)
   *   Key: parent(1), col(0) = 2 pattern words
   *   NI_LINKED_ATTR: 1 attr = 1 word
   */
  const Uint32 node2_len = 9;

  /*
   * Node 3: QN_LOOKUP (4 fixed + 1 parent + 3 key + 1 linked)
   *   Key: parent(2), col(2) = 2 pattern words
   *   NI_LINKED_ATTR: 1 attr = 1 word
   */
  const Uint32 node3_len = 9;

  /*
   * Node 4: QN_LOOKUP (4 fixed + 1 parent + 5 key + 1 linked)
   *   Key: parent(3),col(1), parent(3),col(2) = 4 pattern words
   *   NI_KEY_LINKED: (0 << 16) | 4 = 1 len word
   *   NI_LINKED_ATTR: 1 attr = 1 word
   */
  const Uint32 node4_len = 11;

  /*
   * Node 5: QN_LOOKUP (4 fixed + 1 parent + 3 key + 1 interp/linked len
   *          + 1 interp prog + 9 linked pattern)
   *   Key: parent(1), col(0) = 2 pattern words
   *   NI_ATTR_INTERPRET + NI_ATTR_LINKED: 1 len + 1 ExitOK + 9 pattern = 11
   *   Note: ps_supplycost uses attrInfo(0) directly (no parent prefix)
   *   because parent(0) with appendFromParent leaves targetRow uninitialized.
   */
  const Uint32 node5_len = 19;

  const Uint32 tree_len = 1 + node0_len + node1_len + node2_len +
                           node3_len + node4_len + node5_len;

  /* Word 0: QueryTree cnt_len */
  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 6, tree_len);
  ai.push_back(cnt_len);

  /* ---- Node 0: QN_SCAN_FRAG on LINEITEM ---- */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_SCAN_FRAG, node0_len);
  ai.push_back(n0_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(lineitemMeta.tableId);
  ai.push_back(lineitemMeta.schemaVersion);
  /* NI_LINKED_ATTR: 6 attrs packed */
  ai.push_back((li_orderkey << 16) | 6);
  ai.push_back(li_partkey | (li_suppkey << 16));
  ai.push_back(li_extendedprice | (li_discount << 16));
  ai.push_back(li_quantity);

  /* ---- Node 1: QN_LOOKUP on PART (filter via NI_ATTR_INTERPRET) ---- */
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_LOOKUP, node1_len);
  ai.push_back(n1_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_INNER_JOIN | DABits::NI_AGGREGATE |
               DABits::NI_ATTR_INTERPRET);
  ai.push_back(partMeta.tableId);
  ai.push_back(partMeta.schemaVersion);
  /* NI_HAS_PARENT: 1 parent (node 0) */
  ai.push_back((0 << 16) | 1);
  /* NI_KEY_LINKED: (param_cnt << 16) | pattern_len */
  ai.push_back((0 << 16) | 1);
  /* Key pattern: col(1) = l_partkey from parent LINEITEM */
  ai.push_back(QueryPattern::col(1));
  /* NI_ATTR_INTERPRET: (len_pattern << 16) | len_prg */
  ai.push_back((0 << 16) | (Uint32)partFilter.size());
  /* LIKE filter program */
  for (Uint32 w : partFilter) ai.push_back(w);

  /* ---- Node 2: QN_LOOKUP on ORDERS ---- */
  Uint32 n2_len = 0;
  QueryNode::setOpLen(n2_len, QueryNode::QN_LOOKUP, node2_len);
  ai.push_back(n2_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_INNER_JOIN | DABits::NI_AGGREGATE |
               DABits::NI_LINKED_ATTR);
  ai.push_back(ordersMeta.tableId);
  ai.push_back(ordersMeta.schemaVersion);
  /* NI_HAS_PARENT: 1 parent (node 1 = PART) */
  ai.push_back((1 << 16) | 1);
  /* NI_KEY_LINKED: 2 pattern words */
  ai.push_back((0 << 16) | 2);
  /* Key: parent(1), col(0) = l_orderkey from LINEITEM */
  ai.push_back(QueryPattern::parent(1));
  ai.push_back(QueryPattern::col(0));
  /* NI_LINKED_ATTR: 1 attr = o_orderyear */
  ai.push_back((o_orderyear << 16) | 1);

  /* ---- Node 3: QN_LOOKUP on SUPPLIER ---- */
  Uint32 n3_len = 0;
  QueryNode::setOpLen(n3_len, QueryNode::QN_LOOKUP, node3_len);
  ai.push_back(n3_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_INNER_JOIN | DABits::NI_AGGREGATE |
               DABits::NI_LINKED_ATTR);
  ai.push_back(supplierMeta.tableId);
  ai.push_back(supplierMeta.schemaVersion);
  /* NI_HAS_PARENT: 1 parent (node 2 = ORDERS) */
  ai.push_back((2 << 16) | 1);
  /* NI_KEY_LINKED: 2 pattern words */
  ai.push_back((0 << 16) | 2);
  /* Key: parent(2), col(2) = l_suppkey from LINEITEM */
  ai.push_back(QueryPattern::parent(2));
  ai.push_back(QueryPattern::col(2));
  /* NI_LINKED_ATTR: 1 attr = s_nationkey */
  ai.push_back((s_nationkey << 16) | 1);

  /* ---- Node 4: QN_LOOKUP on PARTSUPP ---- */
  Uint32 n4_len = 0;
  QueryNode::setOpLen(n4_len, QueryNode::QN_LOOKUP, node4_len);
  ai.push_back(n4_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_INNER_JOIN | DABits::NI_AGGREGATE |
               DABits::NI_LINKED_ATTR);
  ai.push_back(partsuppMeta.tableId);
  ai.push_back(partsuppMeta.schemaVersion);
  /* NI_HAS_PARENT: 1 parent (node 3 = SUPPLIER) */
  ai.push_back((3 << 16) | 1);
  /* NI_KEY_LINKED: 4 pattern words (composite key: l_partkey + l_suppkey) */
  ai.push_back((0 << 16) | 4);
  /* Key part 1: parent(3), col(1) = l_partkey from LINEITEM */
  ai.push_back(QueryPattern::parent(3));
  ai.push_back(QueryPattern::col(1));
  /* Key part 2: parent(3), col(2) = l_suppkey from LINEITEM */
  ai.push_back(QueryPattern::parent(3));
  ai.push_back(QueryPattern::col(2));
  /* NI_LINKED_ATTR: 1 attr = ps_supplycost */
  ai.push_back((ps_supplycost << 16) | 1);

  /* ---- Node 5: QN_LOOKUP on NATION (aggregate leaf) ---- */
  Uint32 n5_len = 0;
  QueryNode::setOpLen(n5_len, QueryNode::QN_LOOKUP, node5_len);
  ai.push_back(n5_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_INNER_JOIN | DABits::NI_AGGREGATE |
               DABits::NI_AGGREGATE_LEAF |
               DABits::NI_ATTR_INTERPRET | DABits::NI_ATTR_LINKED);
  ai.push_back(nationMeta.tableId);
  ai.push_back(nationMeta.schemaVersion);
  /* NI_HAS_PARENT: 1 parent (node 4 = PARTSUPP) */
  ai.push_back((4 << 16) | 1);
  /* NI_KEY_LINKED: 2 pattern words */
  ai.push_back((0 << 16) | 2);
  /* Key: parent(1), col(0) = s_nationkey from SUPPLIER */
  ai.push_back(QueryPattern::parent(1));
  ai.push_back(QueryPattern::col(0));
  /* NI_ATTR_INTERPRET/NI_ATTR_LINKED: (len_pattern << 16) | len_prg */
  ai.push_back((9 << 16) | 1);
  /* Interpret program: ExitOK (no filtering, just enables interpreter) */
  ai.push_back(Interpreter::ExitOK());
  /* NI_ATTR_LINKED pattern: 5 linked references = 9 pattern words */
  /* parent(2), attrInfo(0) — o_orderyear from ORDERS */
  ai.push_back(QueryPattern::parent(2));
  ai.push_back(QueryPattern::attrInfo(0));
  /* parent(4), attrInfo(3) — l_extendedprice from LINEITEM */
  ai.push_back(QueryPattern::parent(4));
  ai.push_back(QueryPattern::attrInfo(3));
  /* parent(4), attrInfo(4) — l_discount from LINEITEM */
  ai.push_back(QueryPattern::parent(4));
  ai.push_back(QueryPattern::attrInfo(4));
  /* parent(4), attrInfo(5) — l_quantity from LINEITEM */
  ai.push_back(QueryPattern::parent(4));
  ai.push_back(QueryPattern::attrInfo(5));
  /* attrInfo(0) — ps_supplycost from PARTSUPP (direct parent, no parent prefix) */
  ai.push_back(QueryPattern::attrInfo(0));

  /* ---- Parameter section (6 params) ---- */

  /* Param 0: QN_ScanFragParameters (LINEITEM, no filter) */
  Uint32 p0_param_size = QN_ScanFragParameters::NodeSize;
  Uint32 p0_len = 0;
  QueryNodeParameters::setOpLen(p0_len, QueryNodeParameters::QN_SCAN_FRAG,
                                p0_param_size);
  ai.push_back(p0_len);
  ai.push_back(0);                /* requestInfo */
  ai.push_back(receiverId);       /* resultData */
  ai.push_back(990);              /* batch_size_rows */
  ai.push_back(2*1024*1024);     /* batch_size_bytes */
  ai.push_back(0);
  ai.push_back(0);
  ai.push_back(0);

  /* Param 1: QN_LookupParameters (PART, no param filter — filter is in tree) */
  {
    Uint32 p1_len = 0;
    QueryNodeParameters::setOpLen(p1_len, QueryNodeParameters::QN_LOOKUP,
                                  QN_LookupParameters::NodeSize);
    ai.push_back(p1_len);
    ai.push_back(0);             /* requestInfo — no PI_ATTR_INTERPRET */
    ai.push_back(receiverId);
  }

  /* Params 2-5: QN_LookupParameters (no filter) */
  for (int p = 2; p <= 5; p++) {
    Uint32 pN_len = 0;
    QueryNodeParameters::setOpLen(pN_len, QueryNodeParameters::QN_LOOKUP,
                                  QN_LookupParameters::NodeSize);
    ai.push_back(pN_len);
    ai.push_back(0);             /* requestInfo */
    ai.push_back(receiverId);    /* resultData */
  }

  return ai;
}

/* ------------------------------------------------------------------ */
/* Aggregation program builder — Q9                                    */
/* ------------------------------------------------------------------ */

/*
 * GROUP BY: n_name (local CHAR(25)), o_orderyear (linked INT)
 * SUM: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
 *
 * DECIMAL(15,2) with scale!=0 auto-converts to DOUBLE at load time.
 *
 * Instructions:
 *   kOpLoadCol  DECIMAL(15,2) → reg1  (linked l_extendedprice)  + dec_info
 *   kOpLoadConst DOUBLE 1.0 → reg2  + 2 data words
 *   kOpLoadCol  DECIMAL(15,2) → reg3  (linked l_discount)       + dec_info
 *   kOpMinus    reg2 = reg2 - reg3     (1 - discount)
 *   kOpMul      reg1 = reg1 * reg2     (price * (1 - discount))
 *   kOpLoadCol  DECIMAL(15,2) → reg3  (linked ps_supplycost)    + dec_info
 *   kOpLoadCol  DECIMAL(15,2) → reg4  (linked l_quantity)       + dec_info
 *   kOpMul      reg3 = reg3 * reg4     (cost * qty)
 *   kOpMinus    reg1 = reg1 - reg3     (amount)
 *   kOpSum      reg1 → agg[0]
 */
static std::vector<Uint32>
buildAggProgram_Q9(const TableMeta &nationMeta,
                   const TableMeta & /*ordersMeta*/,
                   const TableMeta & /*lineitemMeta*/,
                   const TableMeta & /*partsuppMeta*/)
{
  Uint32 n_name = nationMeta.attrIds.at("n_name");

  /*
   * Header: 8 words
   * GB cols: 2 words (n_name, o_orderyear)
   * Instructions: 10 ops + 4 dec_info + 2 const data = ~16 words
   * Total: ~26 words
   */
  const Uint32 HEADER = 8;
  const Uint32 GB_COLS = 2;
  /* Count instructions carefully:
   *   kOpLoadCol(dec) = 2 words (instr + dec_info)
   *   kOpLoadConst(double) = 3 words (instr + 2 data)
   *   kOpMinus/kOpMul = 1 word each
   *   kOpSum = 1 word
   * Total: 2 + 3 + 2 + 1 + 1 + 2 + 2 + 1 + 1 + 1 = 16
   */
  const Uint32 INSTR = 16;
  const Uint32 PROG_LEN = HEADER + GB_COLS + INSTR;

  std::vector<Uint32> prog(PROG_LEN);

  /* Header */
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (2u << 16) | 1u;   /* 2 GB cols, 1 agg result */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /*
   * GROUP BY columns.
   * Linked columns use LINKED_COL_FLAG | position (0-based index into
   * the linked buffer built by NI_ATTR_LINKED, NOT table attrId).
   * Buffer order: [0]=o_orderyear, [1]=l_extendedprice, [2]=l_discount,
   *               [3]=l_quantity, [4]=ps_supplycost
   */
  prog[8] = n_name << 16;                        /* local CHAR(25) */
  prog[9] = (LINKED_COL_FLAG | 0) << 16;         /* linked pos 0 = o_orderyear */

  Uint32 pos = HEADER + GB_COLS;  /* = 10 */

  Uint32 dec_info_15_2 = (15 << 16) | 2;

  /* kOpLoadCol DECIMAL(15,2) → reg1 (linked pos 1 = l_extendedprice) */
  prog[pos++] = (kOpLoadCol << 26) | (NDB_TYPE_DECIMAL << 21) |
                (kReg1 << 16) | (LINKED_COL_FLAG | 1);
  prog[pos++] = dec_info_15_2;

  /* kOpLoadConst DOUBLE 1.0 → reg2 */
  prog[pos++] = (kOpLoadConst << 26) | (NDB_TYPE_DOUBLE << 21) |
                (kReg2 << 16);
  { double v = 1.0; memcpy(&prog[pos], &v, sizeof(double)); pos += 2; }

  /* kOpLoadCol DECIMAL(15,2) → reg3 (linked pos 2 = l_discount) */
  prog[pos++] = (kOpLoadCol << 26) | (NDB_TYPE_DECIMAL << 21) |
                (kReg3 << 16) | (LINKED_COL_FLAG | 2);
  prog[pos++] = dec_info_15_2;

  /* kOpMinus reg2 = reg2 - reg3  (1 - discount) */
  prog[pos++] = (kOpMinus << 26) | (kReg2 << 12) | (kReg3 << 8);

  /* kOpMul reg1 = reg1 * reg2  (price * (1 - discount)) */
  prog[pos++] = (kOpMul << 26) | (kReg1 << 12) | (kReg2 << 8);

  /* kOpLoadCol DECIMAL(15,2) → reg3 (linked pos 4 = ps_supplycost) */
  prog[pos++] = (kOpLoadCol << 26) | (NDB_TYPE_DECIMAL << 21) |
                (kReg3 << 16) | (LINKED_COL_FLAG | 4);
  prog[pos++] = dec_info_15_2;

  /* kOpLoadCol DECIMAL(15,2) → reg4 (linked pos 3 = l_quantity) */
  prog[pos++] = (kOpLoadCol << 26) | (NDB_TYPE_DECIMAL << 21) |
                (kReg4 << 16) | (LINKED_COL_FLAG | 3);
  prog[pos++] = dec_info_15_2;

  /* kOpMul reg3 = reg3 * reg4  (cost * qty) */
  prog[pos++] = (kOpMul << 26) | (kReg3 << 12) | (kReg4 << 8);

  /* kOpMinus reg1 = reg1 - reg3  (amount) */
  prog[pos++] = (kOpMinus << 26) | (kReg1 << 12) | (kReg3 << 8);

  /* kOpSum reg1 → agg[0] */
  prog[pos++] = (kOpSum << 26) | (kReg1 << 16) | 0;

  assert(pos == PROG_LEN);
  return prog;
}

/* ------------------------------------------------------------------ */
/* SCAN_TABREQ sender                                                  */
/* ------------------------------------------------------------------ */

Uint32 buildScanTabReqInfo()
{
  Uint32 requestInfo = 0;
  ScanTabReq::setReadCommittedFlag(requestInfo, 1);
  ScanTabReq::setNoDiskFlag(requestInfo, 1);
  ScanTabReq::setViaSPJFlag(requestInfo, 1);
  ScanTabReq::setJoinAggFlag(requestInfo, 1);
  ScanTabReq::setScanBatch(requestInfo, 990);
  ScanTabReq::setExtendedConf(requestInfo, 1);
  return requestInfo;
}

static int
sendScanTabReq(SignalSender &ss, Uint32 nodeId,
               Uint32 apiConnectPtr, Uint32 tcRef,
               const TableMeta &scanMeta,
               const std::vector<Uint32> &queryTree,
               const std::vector<Uint32> &aggProgram,
               Uint32 receiverIdBase, Uint32 parallelism,
               Uint32 numRecvIds)
{
  V("SCAN_TABREQ → node %u, table=%u\n", nodeId, scanMeta.tableId);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  memset(data, 0, 25 * sizeof(Uint32));

  data[0] = apiConnectPtr;
  data[1] = 0;

  Uint32 requestInfo = buildScanTabReqInfo();
  data[2] = requestInfo;

  data[3] = scanMeta.tableId;
  data[4] = scanMeta.schemaVersion;
  data[5] = 0xFFFF;             /* storedProcId = RNIL */
  data[6] = FAKE_TRANS_ID1;
  data[7] = FAKE_TRANS_ID2;
  data[8] = apiConnectPtr;      /* buddyConPtr */
  data[9] = 2 * 1024 * 1024;   /* batch_byte_size */
  data[10] = 990;               /* first_batch_size */

  data[15] = parallelism;

  ssig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_TABREQ, 16);

  /* Section 0: receiver IDs */
  std::vector<Uint32> receiverIds(numRecvIds);
  for (Uint32 i = 0; i < numRecvIds; i++) {
    receiverIds[i] = receiverIdBase + i;
  }

  ssig.header.m_noOfSections = 3;
  ssig.ptr[0].p = receiverIds.data();
  ssig.ptr[0].sz = numRecvIds;
  ssig.ptr[1].p = queryTree.data();
  ssig.ptr[1].sz = (Uint32)queryTree.size();
  ssig.ptr[2].p = aggProgram.data();
  ssig.ptr[2].sz = (Uint32)aggProgram.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SCAN_TABREQ failed\n");
    return -1;
  }

  V("  Sent SCAN_TABREQ: requestInfo=0x%08x, parallelism=%u, "
    "receivers=%u, queryTree=%zu words, aggProgram=%zu words\n",
    requestInfo, parallelism, numRecvIds,
    queryTree.size(), aggProgram.size());
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

/*
 * Extract GROUP BY key for Q9:
 *   GB col 0: n_name CHAR(25) — 4-byte attr header + 25 bytes data
 *   GB col 1: o_orderyear INT — 4-byte attr header + 4 bytes data
 * Returns (nation_name, orderyear).
 */
static std::pair<std::string, int>
extractQ9GroupKey(const std::vector<Uint8> &key)
{
  const Uint32 HDR = 4;  /* AttributeHeader size */
  const Uint32 NAME_DATA = 28;  /* ceil(25/4)*4 = 7 words */
  if (key.size() < HDR + NAME_DATA + HDR + 4) return {"", 0};

  /*
   * n_name: CHAR(25), word-aligned to 28 bytes in the key buffer.
   * NDB stores CHAR with null-byte padding (not space-padding) when
   * setValue is called with a C string shorter than the column width.
   * Truncate at the first null byte, then trim trailing spaces.
   */
  const char *nameStart = reinterpret_cast<const char*>(key.data() + HDR);
  int nameLen = (int)strnlen(nameStart, 25);
  while (nameLen > 0 && nameStart[nameLen - 1] == ' ') nameLen--;
  std::string nation(nameStart, nameLen);

  /* o_orderyear: INT (little-endian 4 bytes) after word-aligned n_name */
  Uint32 off = HDR + NAME_DATA + HDR;
  Int32 year;
  memcpy(&year, key.data() + off, sizeof(Int32));

  return {nation, (int)year};
}

/*
 * Extract SUM(double) from aggregation result value buffer.
 * Each agg result item: 24 bytes (type(4) + value(8) + flags(4) + padding(8))
 */
static double
extractSumDouble(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;
  Uint32 off = aggIdx * ITEM_SIZE + 8;
  if (off + 8 > val.size()) return 0.0;
  double v;
  memcpy(&v, val.data() + off, sizeof(double));
  return v;
}

/* ------------------------------------------------------------------ */
/* Result collection (TRANSID_AI + SCAN_TABCONF)                       */
/* ------------------------------------------------------------------ */

static int
collectResults(SignalSender &ss,
               std::vector<AggResult> &allResults,
               Uint32 apiConnectPtr, Uint32 tcRef, Uint32 nodeId,
               Uint32 &nextReqCount,
               Uint32 receiverIdBase [[maybe_unused]],
               Uint32 numRecvIds [[maybe_unused]])
{
  V("Waiting for results...\n");
  nextReqCount = 0;

  bool done = false;
  while (!done) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS,
                                        "TRANSID_AI/SCAN_TABCONF");
    if (resp == nullptr) return -1;

    int gsn = getGsn(resp);

    if (gsn == GSN_TRANSID_AI) {
      Uint32 connectPtr = resp->getDataPtr()[0];
      V("  TRANSID_AI: connectPtr=%u\n", connectPtr);

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
        V("  Sending SCAN_NEXTREQ with %u ack(s)\n", ackCount);

        nextSig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_NEXTREQ,
                    4 + ackCount);
        nextSig.header.m_noOfSections = 0;

        if (ss.sendSignal(nodeId, &nextSig) != SEND_OK) {
          fprintf(stderr, "sendSignal SCAN_NEXTREQ failed\n");
          return -1;
        }
        nextReqCount++;
      }
    }
    else if (gsn == GSN_SCAN_TABREF) {
      const Uint32 *d = resp->getDataPtr();
      Uint32 sigLen = resp->getLength();
      fprintf(stderr, "SCAN_TABREF: errorCode=%u closeNeeded=%u sigLen=%u\n",
              d[3], sigLen >= 5 ? d[4] : 0, sigLen);
      for (Uint32 w = 0; w < sigLen; w++)
        fprintf(stderr, "  d[%u] = 0x%08x\n", w, d[w]);
      return -1;
    }
    else {
      V("  Ignoring GSN %d\n", gsn);
    }
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Benchmark function                                                  */
/* ------------------------------------------------------------------ */

static int
runBenchmark(SignalSender &ss, Uint32 nodeId,
             const NdbDictionary::Table *partTab,
             const TableMeta &lineitemMeta,
             const TableMeta &partMeta,
             const TableMeta &ordersMeta,
             const TableMeta &supplierMeta,
             const TableMeta &partsuppMeta,
             const TableMeta &nationMeta,
             int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  Uint32 receiverId = 200 + iteration;

  /* Build PART LIKE filter */
  Uint32 pNameAttrId = partMeta.attrIds.at("p_name");
  std::vector<Uint32> partFilter = buildPartFilter(partTab, pNameAttrId);
  if (partFilter.empty()) return -1;

  /* Build QueryTree and AggProgram */
  std::vector<Uint32> queryTree =
    buildQueryTree_Q9(lineitemMeta, partMeta, ordersMeta, supplierMeta,
                      partsuppMeta, nationMeta, partFilter, receiverId);

  std::vector<Uint32> aggProgram =
    buildAggProgram_Q9(nationMeta, ordersMeta, lineitemMeta, partsuppMeta);

  V("QueryTree: %zu words, AggProgram: %zu words\n",
    queryTree.size(), aggProgram.size());
  if (verbose) {
    fprintf(stderr, "QueryTree hex dump:\n");
    for (size_t i = 0; i < queryTree.size(); i++)
      fprintf(stderr, "  [%2zu] 0x%08x\n", i, queryTree[i]);
    fprintf(stderr, "AggProgram hex dump:\n");
    for (size_t i = 0; i < aggProgram.size(); i++)
      fprintf(stderr, "  [%2zu] 0x%08x\n", i, aggProgram[i]);
  }

  /* Seize TC connect */
  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0)
    return -1;

  auto t0 = Clock::now();

  /* Send SCAN_TABREQ */
  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          lineitemMeta, queryTree, aggProgram,
                          receiverId, scanParallel, numReceivers);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    return -1;
  }

  auto t1 = Clock::now();

  /* Collect results */
  std::vector<AggResult> allResults;
  Uint32 nextReqCount = 0;
  rc = collectResults(ss, allResults, apiConnectPtr, tcRef, nodeId,
                      nextReqCount, receiverId, numReceivers);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    return -1;
  }

  auto t2 = Clock::now();

  /* Release TC connect */
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  auto t3 = Clock::now();

  /* Merge results: map<(nation, year), sum_amount> */
  std::map<std::pair<std::string, int>, double> mergedResults;
  Uint32 totalGroups = 0;
  for (const auto &res : allResults) {
    totalGroups += (Uint32)res.groups.size();
    for (const auto &grp : res.groups) {
      auto key = extractQ9GroupKey(grp.first);
      double amount = extractSumDouble(grp.second, 0);
      mergedResults[key] += amount;
    }
  }

  /* SQL verification and timing */
  double sqlMs = 0;
  int failures = 0;
  if (g_mysql_conn != nullptr) {
    auto tSql0 = Clock::now();
    if (mysql_query(g_mysql_conn,
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
      fprintf(stderr, "SQL Q9 failed: %s\n", mysql_error(g_mysql_conn));
      failures++;
    } else {
      MYSQL_RES *res = mysql_store_result(g_mysql_conn);
      if (res == nullptr) {
        fprintf(stderr, "mysql_store_result failed: %s\n",
                mysql_error(g_mysql_conn));
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

        /* Compare merged pushdown results with SQL results */
        std::set<std::pair<std::string, int>> allKeys;
        for (const auto &kv : mergedResults) allKeys.insert(kv.first);
        for (const auto &kv : sqlResults) allKeys.insert(kv.first);

        for (const auto &key : allKeys) {
          double pushdown = mergedResults.count(key) ? mergedResults[key] : 0.0;
          double sql = sqlResults.count(key) ? sqlResults[key] : 0.0;
          double diff = fabs(pushdown - sql);
          double tol = std::max(0.01, fabs(sql) * 1e-9);
          if (diff > tol) {
            fprintf(stderr, "FAIL: %s/%d: pushdown=%.2f sql=%.2f diff=%.2f\n",
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

  /* Timing */
  double scanMs = elapsedMs(t0, t1);
  double resultMs = elapsedMs(t1, t2);
  double releaseMs = elapsedMs(t2, t3);
  double totalMs = elapsedMs(t0, t3);

  printf("Iteration %d: %s\n", iteration,
         failures == 0 ? "PASS" : "FAIL");
  printf("  Groups: %u  TRANSID_AI: %zu  SCAN_NEXTREQ: %u\n",
         totalGroups, allResults.size(), nextReqCount);
  printf("  Scan+Join: %8.2f ms\n", scanMs + resultMs);
  printf("  Release:   %8.2f ms\n", releaseMs);
  printf("  Total:     %8.2f ms\n", totalMs);
  if (sqlMs > 0)
    printf("  SQL query: %8.2f ms\n", sqlMs);

  if (verbose) {
    for (const auto &kv : mergedResults) {
      printf("    %-15s %d: %.2f\n",
             kv.first.first.c_str(), kv.first.second, kv.second);
    }
  }

  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
  const char *connectString = nullptr;
  int mysqlPort = 3306;
  int numIterations = 3;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n"
        "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
        "  -m <port>            MySQL port (default: 3306)\n"
        "  --iterations <N>     Benchmark iterations (default: 3)\n"
        "  --parallel <N>       Scan parallelism (default: 8)\n"
        "  --receivers <N>      Number of receiver IDs (default: 1)\n"
        "  -v, --verbose        Verbose output\n"
        "  -h, --help           Show help\n",
        argv[0]);
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
    } else if (strcmp(argv[i], "--parallel") == 0 && i + 1 < argc) {
      scanParallel = (Uint32)atoi(argv[++i]);
    } else if (strcmp(argv[i], "--receivers") == 0 && i + 1 < argc) {
      numReceivers = (Uint32)atoi(argv[++i]);
    }
  }

  if (connectString == nullptr) connectString = "localhost:1186";

  printf("bench_q9_dbtc: TPC-H Q9 6-table pushdown join benchmark\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Iterations: %d  Parallel: %u  Receivers: %u\n\n",
         numIterations, scanParallel, numReceivers);

  ndb_init();
  int result = 0;

  do {
    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      result = 1; break;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      result = 1; break;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init: %s\n", ndb.getNdbError().message);
      result = 1; break;
    }

    NdbRestarter restarter(connectString);
    int dataNodeId = restarter.getDbNodeId(0);
    if (dataNodeId <= 0) {
      fprintf(stderr, "No data node found\n");
      result = 1; break;
    }
    V("Using data node %d\n", dataNodeId);

    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      result = 1; break;
    }
    g_mysql_conn = conn;

    /* Load all 6 table metadata */
    TableMeta lineitemMeta, partMeta, ordersMeta;
    TableMeta supplierMeta, partsuppMeta, nationMeta;

    if (loadTableMeta(&ndb, "tpch_lineitem", lineitemMeta) != 0 ||
        loadTableMeta(&ndb, "tpch_part", partMeta) != 0 ||
        loadTableMeta(&ndb, "tpch_orders", ordersMeta) != 0 ||
        loadTableMeta(&ndb, "tpch_supplier", supplierMeta) != 0 ||
        loadTableMeta(&ndb, "tpch_partsupp", partsuppMeta) != 0 ||
        loadTableMeta(&ndb, "tpch_nation", nationMeta) != 0) {
      fprintf(stderr, "Failed to load table metadata. "
              "Did you run load_tpch first?\n");
      mysql_close(conn);
      result = 1; break;
    }

    if (verbose) {
      const char *tables[] = {"tpch_lineitem", "tpch_part", "tpch_orders",
                              "tpch_supplier", "tpch_partsupp", "tpch_nation"};
      const TableMeta *metas[] = {&lineitemMeta, &partMeta, &ordersMeta,
                                   &supplierMeta, &partsuppMeta, &nationMeta};
      for (int t = 0; t < 6; t++) {
        fprintf(stderr, "%s: tableId=%u schemaVersion=%u frags=%u\n",
                tables[t], metas[t]->tableId, metas[t]->schemaVersion,
                metas[t]->fragCount);
        for (const auto &kv : metas[t]->attrIds)
          fprintf(stderr, "  %-20s attrId=%u\n", kv.first.c_str(), kv.second);
      }
    }

    const NdbDictionary::Table *partTab =
        ndb.getDictionary()->getTable("tpch_part");
    if (partTab == nullptr) {
      fprintf(stderr, "getTable(tpch_part) failed: %s\n",
              ndb.getDictionary()->getNdbError().message);
      mysql_close(conn);
      result = 1; break;
    }

    /* Run benchmark iterations */
    printf("Starting benchmark (%d iterations)...\n\n", numIterations);

    {
      SignalSender ss(&con);
      ss.lock();

      for (int iter = 1; iter <= numIterations; iter++) {
        if (runBenchmark(ss, (Uint32)dataNodeId, partTab,
                         lineitemMeta, partMeta, ordersMeta,
                         supplierMeta, partsuppMeta, nationMeta,
                         iter) != 0) {
          result = 1;
          for (int d = 0; d < 100; d++) {
            SimpleSignal *stale = ss.waitFor(50);
            if (stale == nullptr) break;
          }
        }
        printf("\n");
      }

      ss.unlock();
    }

    mysql_close(conn);
  } while (0);

  ndb_end(0);

  printf(result == 0 ? "\n*** ALL ITERATIONS PASSED ***\n"
                      : "\n*** SOME ITERATIONS FAILED ***\n");
  return result;
}
