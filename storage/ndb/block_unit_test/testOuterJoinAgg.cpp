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
 * testOuterJoinAgg — Outer join aggregation pushdown tests
 *
 * Tests the case where a LEFT OUTER JOIN lookup child has no match
 * (key not found or key is NULL), and the parent row must still
 * contribute to aggregation with NULL child columns.
 *
 * Uses two tables:
 *   parent_oj (pk BIGINT PK, grp BIGINT)
 *   child_oj  (pk BIGINT PK, val BIGINT)
 *
 * QueryTree:
 *   Node 0: QN_SCAN_FRAG on parent_oj (NI_AGGREGATE | NI_LINKED_ATTR)
 *   Node 1: QN_LOOKUP on child_oj (NI_AGGREGATE_LEAF, no NI_INNER_JOIN)
 *           Key = parent.pk via NI_KEY_LINKED
 *
 * Aggregation program references:
 *   - parent.grp via linked column (col_id | 0x8000) for GROUP BY
 *   - child.val via local column for SUM
 *   - COUNT(*) for row counting
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
#include <vector>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *PARENT_TABLE = "parent_oj";
static const char *CHILD_TABLE = "child_oj";
static const Uint32 FAKE_TRANS_ID1 = 0xABCD1234;
static const Uint32 FAKE_TRANS_ID2 = 0x5678DCBA;
static const Uint32 WAIT_TIMEOUT_MS = 60000;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;

/* Linked column flag: bit 15 set means "read from linked_attr_data" */
static const Uint32 LINKED_COL_FLAG = 0x8000;

/* ------------------------------------------------------------------ */
/* Table metadata                                                      */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 attrIdPk;
  Uint32 attrIdCol2;   // grp for parent, val for child
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
};

/* ------------------------------------------------------------------ */
/* Aggregation program builders                                        */
/* ------------------------------------------------------------------ */

/*
 * COUNT(*), SUM(child.val)
 * No GROUP BY.
 * child.val is a local column (attrId without LINKED_COL_FLAG).
 * COUNT(*) uses a constant register (always non-null) so that null-extended
 * rows from outer joins are still counted.
 */
static std::vector<Uint32>
buildAggProgram_CountSum(Uint32 childValAttrId)
{
  const Uint32 PROG_LEN = 14;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 2u;  /* n_gb_cols=0, n_agg_results=2 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /* LoadConst 1 → register 1 (for COUNT(*), always non-null) */
  prog[8] = (kOpLoadConst << 26) | (NDB_TYPE_BIGUNSIGNED << 21) | (1 << 16);
  Uint64 one = 1;
  memcpy(&prog[9], &one, sizeof(Uint64));

  /* LoadCol child.val → register 0 (for SUM, null when no child match) */
  prog[11] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
              childValAttrId;
  prog[12] = (kOpCount << 26) | (1 << 16) | 0;   /* COUNT(reg1) → agg[0] */
  prog[13] = (kOpSum << 26) | (0 << 16) | 1;     /* SUM(reg0) → agg[1] */

  return prog;
}

/*
 * GROUP BY parent.grp (linked), COUNT(*), SUM(child.val) (local)
 *
 * parent.grp is referenced via linked column index.
 * The linked column index is the position in the NI_LINKED_ATTR list
 * of node 0 (the parent scan). If NI_LINKED_ATTR lists [pk, grp],
 * then grp is at linked index 1. In the agg program, we reference it
 * as (1 | LINKED_COL_FLAG) = 0x8001.
 */
static std::vector<Uint32>
buildAggProgram_GroupByCountSum(Uint32 linkedGrpIdx, Uint32 childValAttrId)
{
  const Uint32 PROG_LEN = 15;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 2u;  /* n_gb_cols=1, n_agg_results=2 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /* GROUP BY: linked column at index linkedGrpIdx */
  prog[8] = (linkedGrpIdx | LINKED_COL_FLAG) << 16;

  /* LoadConst 1 → register 1 (for COUNT(*), always non-null) */
  prog[9] = (kOpLoadConst << 26) | (NDB_TYPE_BIGUNSIGNED << 21) | (1 << 16);
  Uint64 one = 1;
  memcpy(&prog[10], &one, sizeof(Uint64));

  /* LoadCol child.val → register 0 (for SUM, null when no child match) */
  prog[12] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
              childValAttrId;
  prog[13] = (kOpCount << 26) | (1 << 16) | 0;  /* COUNT(reg1) → agg[0] */
  prog[14] = (kOpSum << 26) | (0 << 16) | 1;    /* SUM(reg0) → agg[1] */

  return prog;
}

/* ------------------------------------------------------------------ */
/* QueryTree builder for outer join                                    */
/* ------------------------------------------------------------------ */

/*
 * Build QueryTree for a 2-node outer join with aggregation:
 *
 *   Node 0: QN_SCAN_FRAG on parentTable
 *           NI_AGGREGATE | NI_LINKED_ATTR (passes pk + grp to child)
 *
 *   Node 1: QN_LOOKUP on childTable (aggregate leaf)
 *           NI_HAS_PARENT | NI_KEY_LINKED | NI_AGGREGATE | NI_AGGREGATE_LEAF
 *           NO NI_INNER_JOIN (outer join semantics)
 *           Key = parent.pk (linked col 0)
 *
 * linkedAttrCount: number of attributes in NI_LINKED_ATTR list.
 * If 1: only pk (no GROUP BY from linked).
 * If 2: pk + grp (GROUP BY from linked col 1).
 */
static std::vector<Uint32>
buildOuterJoinQueryTree(const TableMeta &parent, const TableMeta &child,
                        Uint32 receiverId, bool includeGrpLinked)
{
  std::vector<Uint32> ai;

  Uint32 linkedAttrCount = includeGrpLinked ? 2 : 1;
  const Uint32 node0_len = 4 + linkedAttrCount;
  /* node1: 4 fixed + 1 parent + 2 key pattern + (2 if NI_ATTR_LINKED) */
  const Uint32 node1_len = 7 + (includeGrpLinked ? 2 : 0);
  const Uint32 tree_len = 1 + node0_len + node1_len;

  /* Word 0: QueryTree cnt_len */
  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 2, tree_len);
  ai.push_back(cnt_len);

  /* Node 0: QN_SCAN_FRAG (root scan on parent) */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_SCAN_FRAG, node0_len);
  ai.push_back(n0_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(parent.tableId);
  ai.push_back(parent.schemaVersion);
  /* NI_LINKED_ATTR: packed list header = (first_attrId << 16) | count */
  ai.push_back((parent.attrIdPk << 16) | linkedAttrCount);
  if (includeGrpLinked) {
    /* Second linked attr: parent.grp
     * unpackList reads subsequent words' LOWER 16 bits first, so
     * attrId goes in the lower half, padding in the upper half. */
    ai.push_back(parent.attrIdCol2);
  }

  /* Node 1: QN_LOOKUP (aggregate leaf, outer join) */
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_LOOKUP, node1_len);
  ai.push_back(n1_len);
  /* NO NI_INNER_JOIN — this is the outer join flag */
  Uint32 n1_bits = DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
                   DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF;
  if (includeGrpLinked) {
    n1_bits |= DABits::NI_ATTR_LINKED;
  }
  ai.push_back(n1_bits);
  ai.push_back(child.tableId);
  ai.push_back(child.schemaVersion);
  /* NI_HAS_PARENT: parent is node 0 */
  ai.push_back((0 << 16) | 1);
  /* NI_KEY_LINKED: key pattern length = 1 */
  ai.push_back((0 << 16) | 1);
  /* Key pattern: col(0) = linked col 0 = parent.pk */
  ai.push_back(QueryPattern::col(0));
  if (includeGrpLinked) {
    /* NI_ATTR_LINKED: (len_prg=0 | len_pattern=1<<16), pattern=attrInfo(1)
     * Passes parent.grp (linked index 1) to child's attrinfo subroutine. */
    ai.push_back((1u << 16) | 0u);
    ai.push_back(QueryPattern::attrInfo(1));
  }

  /* ---- Parameter section ---- */

  /* Param 0: QN_ScanFragParameters (NodeSize=8) */
  Uint32 p0_len = 0;
  QueryNodeParameters::setOpLen(p0_len, QueryNodeParameters::QN_SCAN_FRAG,
                                QN_ScanFragParameters::NodeSize);
  ai.push_back(p0_len);
  ai.push_back(0);             /* requestInfo */
  ai.push_back(receiverId);    /* resultData */
  ai.push_back(256);           /* batch_size_rows */
  ai.push_back(65536);         /* batch_size_bytes */
  ai.push_back(0);
  ai.push_back(0);
  ai.push_back(0);

  /* Param 1: QN_LookupParameters */
  Uint32 p1_param_len = QN_LookupParameters::NodeSize +
                         (includeGrpLinked ? 2 : 0);
  Uint32 p1_len = 0;
  QueryNodeParameters::setOpLen(p1_len, QueryNodeParameters::QN_LOOKUP,
                                p1_param_len);
  ai.push_back(p1_len);
  ai.push_back(includeGrpLinked ? DABits::PI_ATTR_INTERPRET : 0);
  ai.push_back(receiverId);    /* resultData */
  if (includeGrpLinked) {
    /* PI_ATTR_INTERPRET: minimal ExitOK program creates interpreter framing
     * so NI_ATTR_LINKED subroutine section can carry linked parent data. */
    ai.push_back(1);                   /* program_len=1 */
    ai.push_back(18);                     /* EXIT_OK instruction (opcode 18) */
  }

  return ai;
}

/* ------------------------------------------------------------------ */
/* MySQL helpers                                                       */
/* ------------------------------------------------------------------ */

static int
sqlExec(MYSQL *conn, const char *query)
{
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL error: %s\nQuery: %s\n", mysql_error(conn), query);
    return -1;
  }
  MYSQL_RES *res = mysql_store_result(conn);
  if (res != nullptr) mysql_free_result(res);
  return 0;
}

static MYSQL *
connectMysql(int port)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) return nullptr;
  if (mysql_real_connect(conn, "127.0.0.1", "root", "", "test",
                         port, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

static int
getTableMeta(Ndb *ndb, const char *tableName, TableMeta &meta,
             const char *pkCol, const char *col2)
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
  meta.attrIdPk = ptab->getColumn(pkCol)->getAttrId();
  meta.attrIdCol2 = ptab->getColumn(col2)->getAttrId();
  meta.fragCount = ptab->getFragmentCount();

  meta.fragNodes.resize(meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nodeId = 0;
    ptab->getFragmentNodes(f, &nodeId, 1);
    meta.fragNodes[f] = nodeId;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* Signal helpers                                                      */
/* ------------------------------------------------------------------ */

static int getGsn(const SimpleSignal *sig)
{
  return sig->readSignalNumber();
}

static SimpleSignal *
waitForSignal(SignalSender &ss, Uint32 timeoutMs, const char *desc)
{
  SimpleSignal *sig = ss.waitFor(timeoutMs);
  if (sig == nullptr) {
    fprintf(stderr, "Timeout waiting for %s\n", desc);
    return nullptr;
  }
  V("  Received GSN %d (%s)\n", getGsn(sig), desc);
  return sig;
}

/* ------------------------------------------------------------------ */
/* TC connect management                                               */
/* ------------------------------------------------------------------ */

static int
seizeTcConnect(SignalSender &ss, Uint32 nodeId,
               Uint32 &apiConnectPtrOut, Uint32 &tcRefOut)
{
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

  if (getGsn(resp) == GSN_TCSEIZECONF) {
    apiConnectPtrOut = resp->getDataPtr()[1];
    tcRefOut = resp->getDataPtr()[2];
    V("TCSEIZECONF: apiConnectPtr=%u tcRef=0x%08x\n",
      apiConnectPtrOut, tcRefOut);
    return 0;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for TCSEIZECONF\n", getGsn(resp));
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

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal TCRELEASEREQ failed\n");
    return -1;
  }

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TCRELEASECONF");
  if (resp == nullptr) return -1;

  if (getGsn(resp) == GSN_TCRELEASECONF) {
    V("TCRELEASECONF received\n");
    return 0;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for TCRELEASECONF\n",
          getGsn(resp));
  return -1;
}

/* ------------------------------------------------------------------ */
/* SCAN_TABREQ                                                         */
/* ------------------------------------------------------------------ */

Uint32
buildScanTabReqInfo()
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

static int
sendScanTabReq(SignalSender &ss, Uint32 nodeId,
               Uint32 apiConnectPtr, Uint32 tcRef,
               const TableMeta &parentMeta,
               const std::vector<Uint32> &queryTree,
               const std::vector<Uint32> &aggProgram,
               Uint32 receiverId)
{
  V("SCAN_TABREQ → node %u, table=%u\n", nodeId, parentMeta.tableId);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  memset(data, 0, 25 * sizeof(Uint32));

  data[0] = apiConnectPtr;
  data[1] = 0;
  Uint32 requestInfo = buildScanTabReqInfo();
  data[2] = requestInfo;
  data[3] = parentMeta.tableId;
  data[4] = parentMeta.schemaVersion;
  data[5] = 0xFFFF;             /* storedProcId = RNIL */
  data[6] = FAKE_TRANS_ID1;
  data[7] = FAKE_TRANS_ID2;
  data[8] = apiConnectPtr;      /* buddyConPtr = self */
  data[9] = 65536;              /* batch_byte_size */
  data[10] = 256;               /* first_batch_size */
  data[15] = 1;                 /* scanParallelism */

  ssig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_TABREQ, 16);

  /* Build combined agg section: [boundsLen=0, receiverId, aggProgram...] */
  std::vector<Uint32> aggSection;
  aggSection.push_back(0);  /* boundsLen = 0 */
  aggSection.push_back(receiverId);
  aggSection.insert(aggSection.end(), aggProgram.begin(), aggProgram.end());

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

  V("  Sent SCAN_TABREQ: requestInfo=0x%08x, queryTree=%zu words, "
    "aggProgram=%zu words\n",
    requestInfo, queryTree.size(), aggProgram.size());
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

  if (dataLen < 4) {
    fprintf(stderr, "TRANSID_AI too short: %u words\n", dataLen);
    return -1;
  }

  Uint32 magic = data[0];
  if (magic != ((AGG_RESULT_ATTR << 16) | AGG_MAGIC)) {
    fprintf(stderr, "Bad AGG_RESULT magic: 0x%08x (expected 0x%08x)\n",
            magic, (AGG_RESULT_ATTR << 16) | AGG_MAGIC);
    return -1;
  }

  result.n_gb_cols = data[1] >> 16;
  result.n_agg_results = data[1] & 0xFFFF;
  result.n_groups = data[2];

  Uint32 entries_to_read = result.n_groups;
  if (result.n_gb_cols == 0 && result.n_groups == 0 && 3 < dataLen) {
    entries_to_read = 1;
  }

  Uint32 pos = 3;
  for (Uint32 g = 0; g < entries_to_read; g++) {
    if (pos >= dataLen) {
      fprintf(stderr, "TRANSID_AI truncated at group %u\n", g);
      return -1;
    }
    Uint32 key_len = data[pos] >> 16;
    Uint32 val_len = data[pos] & 0xFFFF;
    pos++;

    Uint32 total_bytes = key_len + val_len;
    Uint32 total_words = (total_bytes + 3) / 4;
    if (pos + total_words > dataLen) {
      fprintf(stderr, "TRANSID_AI truncated in group %u data\n", g);
      return -1;
    }

    const Uint8 *raw = reinterpret_cast<const Uint8 *>(&data[pos]);
    std::vector<Uint8> key(raw, raw + key_len);
    std::vector<Uint8> val(raw + key_len, raw + key_len + val_len);
    result.groups.emplace_back(std::move(key), std::move(val));

    pos += total_words;
  }

  return 0;
}

static Int64
extractSumBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;
  Uint32 offset = aggIdx * ITEM_SIZE + 8;
  if (offset + 8 > val.size()) {
    fprintf(stderr, "extractSumBigint: val too short\n");
    return 0;
  }
  Int64 v;
  memcpy(&v, val.data() + offset, sizeof(Int64));
  return v;
}

static Uint64
extractCountBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;
  Uint32 offset = aggIdx * ITEM_SIZE + 8;
  if (offset + 8 > val.size()) {
    fprintf(stderr, "extractCountBigint: val too short\n");
    return 0;
  }
  Uint64 v;
  memcpy(&v, val.data() + offset, sizeof(Uint64));
  return v;
}

static Int64
extractGroupKey(const std::vector<Uint8> &key)
{
  const Uint32 ATTR_HEADER_SIZE = 4;
  if (key.size() < ATTR_HEADER_SIZE + 8) {
    fprintf(stderr, "extractGroupKey: key too short\n");
    return 0;
  }
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
        V("  Sending SCAN_NEXTREQ with %u ack(s)\n", ackCount);

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
      const Uint32 *d = resp->getDataPtr();
      fprintf(stderr, "SCAN_TABREF: errorCode=%u\n", d[3]);
      return -1;
    }
    else {
      V("  Ignoring GSN %d\n", gsn);
    }
  }

  return 0;
}

/* ------------------------------------------------------------------ */
/* Table setup and teardown                                            */
/* ------------------------------------------------------------------ */

static int
createTables(MYSQL *conn, Ndb *ndb, TableMeta &parentMeta, TableMeta &childMeta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS child_oj");
  sqlExec(conn, "DROP TABLE IF EXISTS parent_oj");

  if (sqlExec(conn,
        "CREATE TABLE parent_oj ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (sqlExec(conn,
        "CREATE TABLE child_oj ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, PARENT_TABLE, parentMeta, "pk", "grp") != 0) return -1;
  if (getTableMeta(ndb, CHILD_TABLE, childMeta, "pk", "val") != 0) return -1;

  V("Parent '%s': id=%u version=%u pk=%u grp=%u frags=%u\n",
    PARENT_TABLE, parentMeta.tableId, parentMeta.schemaVersion,
    parentMeta.attrIdPk, parentMeta.attrIdCol2, parentMeta.fragCount);
  V("Child '%s': id=%u version=%u pk=%u val=%u frags=%u\n",
    CHILD_TABLE, childMeta.tableId, childMeta.schemaVersion,
    childMeta.attrIdPk, childMeta.attrIdCol2, childMeta.fragCount);
  return 0;
}

static void
dropTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS child_oj");
  sqlExec(conn, "DROP TABLE IF EXISTS parent_oj");
}

/* ------------------------------------------------------------------ */
/* Test 1: Partial match — some parent rows have no child              */
/* ------------------------------------------------------------------ */

/*
 * parent_oj: pk=1..10, grp = (pk-1)/3 + 1 → groups {1,1,1, 2,2,2, 3,3,3, 4}
 * child_oj:  pk=1..5, val = pk * 10
 *
 * LEFT JOIN: parent rows 6-10 have no child match.
 * Expected:
 *   COUNT(*) = 10 (all parent rows)
 *   SUM(child.val) = 10+20+30+40+50 = 150 (only matched rows contribute)
 *
 * With GROUP BY parent.grp:
 *   grp=1: pk=1,2,3 → child: val=10,20,30 → COUNT=3, SUM=60
 *   grp=2: pk=4,5,6 → child: val=40,50,NULL → COUNT=3, SUM=90
 *   grp=3: pk=7,8,9 → child: all NULL → COUNT=3, SUM=0 (NULL)
 *   grp=4: pk=10    → child: NULL → COUNT=1, SUM=0 (NULL)
 */
static int
testPartialMatch(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 1: Partial match (10 parent, 5 child) COUNT/SUM ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta parentMeta, childMeta;
  int rc = createTables(conn, ndb, parentMeta, childMeta);
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO parent_oj VALUES "
      "(1,1),(2,1),(3,1),(4,2),(5,2),(6,2),(7,3),(8,3),(9,3),(10,4)");
  }
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO child_oj VALUES "
      "(1,10),(2,20),(3,30),(4,40),(5,50)");
  }
  ss.lock();
  if (rc != 0) { printf("FAIL (setup)\n"); return -1; }

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (TC seize)\n");
    return -1;
  }

  Uint32 receiverId = 42;
  std::vector<Uint32> queryTree =
    buildOuterJoinQueryTree(parentMeta, childMeta, receiverId, false);
  std::vector<Uint32> aggProgram =
    buildAggProgram_CountSum(childMeta.attrIdCol2);

  rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                      parentMeta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (send)\n");
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  if (rc != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (collect)\n");
    return -1;
  }

  /* Merge multi-node results */
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

  if (totalCount != 10 || totalSum != 150) {
    printf("FAIL (expected COUNT=10 SUM=150, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ss.unlock(); dropTables(conn); ss.lock();
    return -1;
  }

  /* SQL verification */
  ss.unlock();
  {
    char query[512];
    snprintf(query, sizeof(query),
             "SELECT COUNT(*), SUM(c.val) FROM parent_oj p "
             "LEFT JOIN child_oj c ON p.pk = c.pk");
    if (mysql_query(conn, query) != 0) {
      fprintf(stderr, "SQL verify failed: %s\n", mysql_error(conn));
      dropTables(conn); ss.lock();
      printf("FAIL (SQL)\n");
      return -1;
    }
    MYSQL_RES *res = mysql_store_result(conn);
    MYSQL_ROW row = mysql_fetch_row(res);
    Uint64 sqlCount = row && row[0] ? (Uint64)atoll(row[0]) : 0;
    Int64 sqlSum = row && row[1] ? (Int64)atoll(row[1]) : 0;
    mysql_free_result(res);
    if (sqlCount != 10 || sqlSum != 150) {
      fprintf(stderr, "SQL verify mismatch: COUNT=%llu SUM=%lld\n",
              (unsigned long long)sqlCount, (long long)sqlSum);
      dropTables(conn); ss.lock();
      printf("FAIL (SQL verify)\n");
      return -1;
    }
    V("  SQL verify: COUNT=%llu SUM=%lld — matches\n",
      (unsigned long long)sqlCount, (long long)sqlSum);
  }
  dropTables(conn);
  ss.lock();

  printf("PASS\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: Partial match with GROUP BY                                 */
/* ------------------------------------------------------------------ */

static int
testPartialMatchGroupBy(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 2: Partial match GROUP BY parent.grp, COUNT/SUM ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta parentMeta, childMeta;
  int rc = createTables(conn, ndb, parentMeta, childMeta);
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO parent_oj VALUES "
      "(1,1),(2,1),(3,1),(4,2),(5,2),(6,2),(7,3),(8,3),(9,3),(10,4)");
  }
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO child_oj VALUES "
      "(1,10),(2,20),(3,30),(4,40),(5,50)");
  }
  ss.lock();
  if (rc != 0) { printf("FAIL (setup)\n"); return -1; }

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (TC seize)\n");
    return -1;
  }

  Uint32 receiverId = 42;
  /* includeGrpLinked=true: NI_LINKED_ATTR has [pk, grp] */
  std::vector<Uint32> queryTree =
    buildOuterJoinQueryTree(parentMeta, childMeta, receiverId, true);
  /* GROUP BY linked col 1 (parent.grp), COUNT(*) → agg[0], SUM(child.val) → agg[1] */
  std::vector<Uint32> aggProgram =
    buildAggProgram_GroupByCountSum(0, childMeta.attrIdCol2);

  rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                      parentMeta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (send)\n");
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  if (rc != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (collect)\n");
    return -1;
  }

  /* Merge multi-node results into group map */
  std::map<Int64, std::pair<Uint64, Int64>> groupResults;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      Int64 grpKey = extractGroupKey(g.first);
      Uint64 cnt = extractCountBigint(g.second, 0);
      Int64 sum = extractSumBigint(g.second, 1);
      groupResults[grpKey].first += cnt;
      groupResults[grpKey].second += sum;
    }
  }

  V("  Groups:\n");
  for (const auto &kv : groupResults) {
    V("    grp=%lld: COUNT=%llu SUM=%lld\n",
      (long long)kv.first,
      (unsigned long long)kv.second.first,
      (long long)kv.second.second);
  }

  /* Verify:
   *   grp=1: COUNT=3, SUM=60  (all 3 parents matched)
   *   grp=2: COUNT=3, SUM=90  (2 matched: val=40+50, 1 unmatched)
   *   grp=3: COUNT=3, SUM=0   (no matches)
   *   grp=4: COUNT=1, SUM=0   (no match)
   */
  bool ok = true;
  if (groupResults.size() != 4) {
    printf("FAIL (expected 4 groups, got %zu)\n", groupResults.size());
    ok = false;
  }
  if (ok) {
    struct Expected { Int64 grp; Uint64 cnt; Int64 sum; };
    Expected exp[] = {{1, 3, 60}, {2, 3, 90}, {3, 3, 0}, {4, 1, 0}};
    for (const auto &e : exp) {
      auto it = groupResults.find(e.grp);
      if (it == groupResults.end()) {
        printf("FAIL (missing group %lld)\n", (long long)e.grp);
        ok = false; break;
      }
      if (it->second.first != e.cnt || it->second.second != e.sum) {
        printf("FAIL (grp=%lld: expected COUNT=%llu SUM=%lld, "
               "got COUNT=%llu SUM=%lld)\n",
               (long long)e.grp,
               (unsigned long long)e.cnt, (long long)e.sum,
               (unsigned long long)it->second.first,
               (long long)it->second.second);
        ok = false; break;
      }
    }
  }

  /* SQL verification */
  ss.unlock();
  {
    char query[512];
    snprintf(query, sizeof(query),
             "SELECT p.grp, COUNT(*), SUM(c.val) FROM parent_oj p "
             "LEFT JOIN child_oj c ON p.pk = c.pk "
             "GROUP BY p.grp ORDER BY p.grp");
    if (mysql_query(conn, query) != 0) {
      fprintf(stderr, "SQL verify failed: %s\n", mysql_error(conn));
      dropTables(conn); ss.lock();
      printf("FAIL (SQL)\n");
      return -1;
    }
    MYSQL_RES *res = mysql_store_result(conn);
    MYSQL_ROW row;
    V("  SQL verify:\n");
    while ((row = mysql_fetch_row(res)) != nullptr) {
      V("    grp=%s COUNT=%s SUM=%s\n",
        row[0] ? row[0] : "NULL",
        row[1] ? row[1] : "NULL",
        row[2] ? row[2] : "NULL");
    }
    mysql_free_result(res);
  }
  dropTables(conn);
  ss.lock();

  if (!ok) return -1;

  printf("PASS\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: All parent rows match (inner join equivalent)               */
/* ------------------------------------------------------------------ */

static int
testAllMatch(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 3: All match (5 parent, 5 child) COUNT/SUM ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta parentMeta, childMeta;
  int rc = createTables(conn, ndb, parentMeta, childMeta);
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO parent_oj VALUES "
      "(1,1),(2,1),(3,2),(4,2),(5,3)");
  }
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO child_oj VALUES "
      "(1,10),(2,20),(3,30),(4,40),(5,50)");
  }
  ss.lock();
  if (rc != 0) { printf("FAIL (setup)\n"); return -1; }

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (TC seize)\n");
    return -1;
  }

  Uint32 receiverId = 42;
  std::vector<Uint32> queryTree =
    buildOuterJoinQueryTree(parentMeta, childMeta, receiverId, false);
  std::vector<Uint32> aggProgram =
    buildAggProgram_CountSum(childMeta.attrIdCol2);

  rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                      parentMeta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (send)\n");
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  if (rc != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (collect)\n");
    return -1;
  }

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
    ss.unlock(); dropTables(conn); ss.lock();
    return -1;
  }

  ss.unlock(); dropTables(conn); ss.lock();
  printf("PASS\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: No matches at all                                           */
/* ------------------------------------------------------------------ */

static int
testNoMatch(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 4: No match (5 parent, child pks 100-104) COUNT/SUM ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta parentMeta, childMeta;
  int rc = createTables(conn, ndb, parentMeta, childMeta);
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO parent_oj VALUES "
      "(1,1),(2,1),(3,2),(4,2),(5,3)");
  }
  if (rc == 0) {
    rc = sqlExec(conn,
      "INSERT INTO child_oj VALUES "
      "(100,10),(101,20),(102,30),(103,40),(104,50)");
  }
  ss.lock();
  if (rc != 0) { printf("FAIL (setup)\n"); return -1; }

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (TC seize)\n");
    return -1;
  }

  Uint32 receiverId = 42;
  std::vector<Uint32> queryTree =
    buildOuterJoinQueryTree(parentMeta, childMeta, receiverId, false);
  std::vector<Uint32> aggProgram =
    buildAggProgram_CountSum(childMeta.attrIdCol2);

  rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                      parentMeta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (send)\n");
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  if (rc != 0) {
    ss.unlock(); dropTables(conn); ss.lock();
    printf("FAIL (collect)\n");
    return -1;
  }

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

  /* All parent rows have no child: COUNT=5 (parent rows counted),
   * SUM=0 (no matched child.val values). */
  if (totalCount != 5 || totalSum != 0) {
    printf("FAIL (expected COUNT=5 SUM=0, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ss.unlock(); dropTables(conn); ss.lock();
    return -1;
  }

  ss.unlock(); dropTables(conn); ss.lock();
  printf("PASS\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
  const char *connectString = nullptr;
  int mysqlPort = 3306;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n\n"
             "Options:\n"
             "  -c <connect_string>  NDB management server connect string\n"
             "                       (default: localhost:1186)\n"
             "  -m <mysql_port>      MySQL server port for SQL verification\n"
             "                       (default: 3306)\n"
             "  -v, --verbose        Show detailed progress output\n"
             "  -h, --help           Show this help message\n",
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
  if (connectString == nullptr) {
    connectString = "localhost:1186";
  }

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

      if (testPartialMatch(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testPartialMatchGroupBy(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testAllMatch(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testNoMatch(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;

      ss.unlock();
    }

    mysql_close(conn);
  } while (0);

  ndb_end(0);

  if (result == 0) {
    printf("\n*** ALL TESTS PASSED ***\n");
  } else {
    printf("\n*** SOME TESTS FAILED ***\n");
  }

  return result;
}
