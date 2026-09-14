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
 * testCteProtocol - RONDB-1120 Phase 2, block-level protocol and
 * state-machine cases (node_failure_test_plan.md, section 5).  Drives
 * DblqhProxy and DBLQH directly with SignalSender: the test is the
 * coordinator, so every word of every request is under its control.
 *
 * Section 5.3, proxy teardown and RELEASE identity:
 *
 *   TD-1  SETUP, then two RELEASEs back to back: two CONFs, one
 *         teardown, pools clean.
 *   TD-2  RELEASE with the teardown held (error insert 5146), require
 *         the matching hold event, then a second RELEASE mid-chain:
 *         it CONFs and starts nothing; pools clean after clearing.
 *   TD-3  RELEASE with a wrong requestId: acknowledged and ignored, the
 *         state stays alive (a COMPLETE afterwards returns the groups),
 *         then the correct RELEASE.
 *   TD-4  RELEASE from another SignalSender (wrong senderRef):
 *         acknowledged to that sender and ignored; COMPLETE returns
 *         the scanned groups before the owner releases the state.
 *   TD-5  RELEASE with a zero transid and the correct requestId, the
 *         stale-SETUP reclaim form: accepted.
 *   TD-6  Error insert 5136 makes the proxy queue an exact duplicate of
 *         a RELEASE behind it: two CONFs, one teardown, pools clean.
 *   TD-7  Full-length SETUP_REQ whose setupNodes names a node that is
 *         not a connected data node: SETUP_REF 286, no state seized, no
 *         placeholder left; the same request naming exactly the data
 *         nodes is accepted (cte_owner_list.md).
 *   TD-8  CTE-mode SETUP_REQ of the pre-RONDB-1120 length
 *         (SignalLength_v1): accepted, the owner list is the receiver's
 *         connected data nodes.
 *
 * After every case the leak-check DUMPs 2361 (join-agg states) and 2363
 * (identity table, placeholders and park records) run on every node;
 * each crashes its node on any surviving record. Require a clean-check
 * event for each dump and unchanged node connection counters, so an
 * automatic restart cannot hide a crash. A RELEASE's teardown runs as a
 * CONTINUEB chain after its CONF; allow it time to drain before checking.
 *
 * Usage: testCteProtocol [-c connectstring] [-m mysql_port] [-v]
 * Prints PASSED or FAILED on stdout (the MTR wrapper diffs that);
 * everything else goes to stderr.
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>
#include <NdbTick.h>
#include <InputStream.hpp>
#include <mgmapi_debug.h>
#include "mgmapi_internal.h"
#include "../src/ndbapi/SignalSender.hpp"
#include <kernel/BlockNumbers.h>
#include <kernel/GlobalSignalNumbers.h>
#include <kernel/RefConvert.hpp>
#include <kernel/NodeBitmask.hpp>
#include <kernel/signaldata/JoinAgg.hpp>
#include <kernel/signaldata/ScanFrag.hpp>
#include <kernel/signaldata/TransIdAI.hpp>
#include <kernel/signaldata/DumpStateOrd.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include <NdbRestarter.hpp>
#include <mysql.h>

#include <map>
#include <vector>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while (0)

static const char *connectString = nullptr;
static int mysqlPort = 3306;

static const char *TABLE_NAME = "ctep_test";
static const Uint32 FAKE_TRANS_ID1 = 0x1120c7e0;
static const Uint32 FAKE_TRANS_ID2 = 0x0e7c0211;
static const Uint32 FAKE_REQUEST_ID = 2001;
static const Uint32 FAKE_SENDER_DATA = 77;
static const Uint32 WAIT_TIMEOUT_MS = 30000;
/* Enough rows that every data node holds a few groups. */
static const Uint32 ROWS = 40;
/* A RELEASE's teardown chain must have drained before a leak check. */
static const Uint32 TEARDOWN_SETTLE_MS = 500;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 INTERPRETER_EXIT_OK = 18;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;
static const Uint32 ZNODEFAIL_BEFORE_COMMIT = 286;

/* ------------------------------------------------------------------ */
/* Aggregation program: SELECT SUM(b) FROM t GROUP BY a                 */
/* ------------------------------------------------------------------ */

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
buildAttrInfo()
{
  std::vector<Uint32> ai(6);
  ai[0] = 0;
  ai[1] = 1;
  ai[2] = 0;
  ai[3] = 0;
  ai[4] = 0;
  ai[5] = INTERPRETER_EXIT_OK;
  return ai;
}

/* ------------------------------------------------------------------ */
/* Table setup via MySQL                                               */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 attrIdA;
  Uint32 attrIdB;
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;      /* primary node for each fragment */
  std::vector<Uint32> fragInstances;  /* LDM instance for each fragment */
};

static void
appendLocalBigintMetaEntry(std::vector<Uint32> &meta,
                           const TableMeta &tableMeta, Uint32 columnId,
                           Uint32 programOffset, Uint32 slotIndex,
                           Uint32 flags)
{
  meta.push_back(JOIN_AGG_META_SOURCE_LOCAL_COLUMN);
  meta.push_back(columnId);
  meta.push_back(programOffset);
  meta.push_back(slotIndex);
  meta.push_back(tableMeta.tableId);
  meta.push_back(tableMeta.schemaVersion);
  meta.push_back(columnId);
  meta.push_back(COL_TYPE_BIGINT);
  meta.push_back(8);
  meta.push_back(0);
  meta.push_back(0);
  meta.push_back(flags);
}

static std::vector<Uint32>
buildJoinAggMetadata(const std::vector<Uint32> &aggProgram,
                     const TableMeta &tableMeta)
{
  std::vector<Uint32> meta;
  meta.push_back(JOIN_AGG_META_MARKER);
  meta.push_back(JOIN_AGG_META_VERSION);
  meta.push_back(0);
  Uint32 entryCount = 0;
  const Uint32 nGbCols = aggProgram[1] >> 16;
  for (Uint32 i = 0; i < nGbCols && (8 + i) < aggProgram.size(); i++) {
    const Uint32 programOffset = 8 + i;
    const Uint32 columnId = (aggProgram[programOffset] >> 16) & 0xFFFF;
    appendLocalBigintMetaEntry(meta, tableMeta, columnId, programOffset, i,
                               JOIN_AGG_META_FLAG_GROUP_BY);
    entryCount++;
  }
  for (Uint32 i = 8 + nGbCols; i < aggProgram.size(); i++) {
    const Uint32 op = (aggProgram[i] >> 26) & 0x3F;
    if (op != kOpLoadCol) continue;
    const Uint32 columnId = aggProgram[i] & 0xFFFF;
    appendLocalBigintMetaEntry(meta, tableMeta, columnId, i, RNIL,
                               JOIN_AGG_META_FLAG_LOAD_COLUMN);
    entryCount++;
  }
  meta[2] = entryCount;
  return meta;
}

static int
sqlExec(MYSQL *conn, const char *query)
{
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL failed: %s\n  query: %s\n", mysql_error(conn),
            query);
    return -1;
  }
  return 0;
}

static MYSQL *
connectMysql(const char *db)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) return nullptr;
  if (mysql_real_connect(conn, "127.0.0.1", "root", "", db, mysqlPort,
                         nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

/* LDM instance per fragment from ndbinfo: the scan feeds go to the
 * fragment's own LDM through the V_QUERY router. */
static int
queryFragInstances(TableMeta &meta)
{
  MYSQL *conn = connectMysql("ndbinfo");
  if (conn == nullptr) return -1;
  char query[256];
  snprintf(query, sizeof(query),
           "SELECT node_id, fragment_num, block_instance "
           "FROM ndbinfo.operations_per_fragment WHERE table_id = %u "
           "ORDER BY node_id, fragment_num", meta.tableId);
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "mysql_query failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  MYSQL_RES *result = mysql_store_result(conn);
  if (result == nullptr) {
    fprintf(stderr, "mysql_store_result failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  std::map<std::pair<Uint32, Uint32>, Uint32> instMap;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result)) != nullptr) {
    instMap[{(Uint32)atoi(row[0]), (Uint32)atoi(row[1])}] =
        (Uint32)atoi(row[2]);
  }
  mysql_free_result(result);
  mysql_close(conn);

  meta.fragInstances.resize(meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    auto it = instMap.find({meta.fragNodes[f], f});
    if (it == instMap.end()) {
      fprintf(stderr, "No LDM instance for node %u frag %u\n",
              meta.fragNodes[f], f);
      return -1;
    }
    meta.fragInstances[f] = it->second;
    V("  fragment %u -> node %u, LDM instance %u\n", f, meta.fragNodes[f],
      meta.fragInstances[f]);
  }
  return 0;
}

static int
createTestTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  char q[256];
  snprintf(q, sizeof(q), "DROP TABLE IF EXISTS %s", TABLE_NAME);
  sqlExec(conn, q);
  snprintf(q, sizeof(q),
           "CREATE TABLE %s (a BIGINT NOT NULL PRIMARY KEY, "
           "b BIGINT NOT NULL) ENGINE=NDB", TABLE_NAME);
  if (sqlExec(conn, q) != 0) return -1;

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(TABLE_NAME);
  const NdbDictionary::Table *ptab = dict->getTable(TABLE_NAME);
  if (ptab == nullptr) {
    fprintf(stderr, "getTable(%s) failed: %s\n", TABLE_NAME,
            dict->getNdbError().message);
    return -1;
  }
  meta.tableId = ptab->getObjectId();
  meta.schemaVersion = ptab->getObjectVersion();
  meta.attrIdA = ptab->getColumn("a")->getAttrId();
  meta.attrIdB = ptab->getColumn("b")->getAttrId();
  meta.fragCount = ptab->getFragmentCount();
  meta.fragNodes.resize(meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nodeId = 0;
    ptab->getFragmentNodes(f, &nodeId, 1);
    meta.fragNodes[f] = nodeId;
  }
  V("Table %s: id=%u version=%u frags=%u\n", TABLE_NAME, meta.tableId,
    meta.schemaVersion, meta.fragCount);
  return queryFragInstances(meta);
}

static int
insertRows(Ndb *ndb, Uint32 rows)
{
  const NdbDictionary::Table *ptab = ndb->getDictionary()->getTable(TABLE_NAME);
  if (ptab == nullptr) return -1;
  for (Uint32 i = 1; i <= rows; i++) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) return -1;
    NdbOperation *op = trans->getNdbOperation(ptab);
    if (op == nullptr || op->insertTuple() != 0 ||
        op->equal("a", (Int64)i) != 0 ||
        op->setValue("b", (Int64)(i * 10)) != 0 ||
        trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert %u failed: %s\n", i,
              trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }
  return 0;
}

static void
dropTestTable(MYSQL *conn)
{
  char q[128];
  snprintf(q, sizeof(q), "DROP TABLE IF EXISTS %s", TABLE_NAME);
  sqlExec(conn, q);
}

/* ------------------------------------------------------------------ */
/* Signal helpers                                                      */
/* ------------------------------------------------------------------ */

static SimpleSignal *
waitForSignal(SignalSender &ss, const char *context)
{
  SimpleSignal *sig = ss.waitFor(WAIT_TIMEOUT_MS);
  if (sig == nullptr) {
    fprintf(stderr, "TIMEOUT waiting for %s\n", context);
  }
  return sig;
}

static int
getGsn(const SimpleSignal *sig)
{
  return sig->header.readSignalNumber();
}

struct SetupParams {
  Uint32 requestId;
  Uint32 transid[2];
  Uint32 queryTag;
  Uint32 strategy;
  Uint32 cteIndex;
  bool fullLength;             /* send setupNodes (RONDB-1120 length) */
  NdbNodeBitmask setupNodes;   /* only with fullLength */
  SetupParams()
      : requestId(FAKE_REQUEST_ID), queryTag(FAKE_REQUEST_ID),
        strategy(JoinAggSetupReq::STRATEGY_MUTEX_FREE), cteIndex(RNIL),
        fullLength(false) {
    transid[0] = FAKE_TRANS_ID1;
    transid[1] = FAKE_TRANS_ID2;
    setupNodes.clear();
  }
};

/* 0 = CONF (keyOut, ownerOut set), 1 = REF (refCodeOut set), -1 = error */
static int
sendSetupReq(SignalSender &ss, Uint32 nodeId,
             const std::vector<Uint32> &aggProgram, const TableMeta &meta,
             const SetupParams &p, Uint32 &keyOut, Uint32 &ownerOut,
             Uint32 &refCodeOut)
{
  SimpleSignal ssig;
  JoinAggSetupReq *req =
      reinterpret_cast<JoinAggSetupReq *>(ssig.getDataPtrSend());
  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = p.requestId;
  req->transid[0] = p.transid[0];
  req->transid[1] = p.transid[1];
  req->tableId = meta.tableId;
  req->expectedOpCount = 0;
  req->concurrencyStrategy = p.strategy;
  req->resultRef = ss.getOwnRef();
  req->resultData = FAKE_SENDER_DATA;
  req->routeRef = ss.getOwnRef();
  req->cteIndex = p.cteIndex;
  req->queryTag = p.queryTag;
  Uint32 len = JoinAggSetupReq::SignalLength_v1;
  if (p.fullLength) {
    p.setupNodes.copyto(NdbNodeBitmask::Size, req->setupNodes);
    len = JoinAggSetupReq::SignalLength;
  }
  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_SETUP_REQ, len);
  Uint32 receiverId = FAKE_SENDER_DATA;
  const std::vector<Uint32> metadata = buildJoinAggMetadata(aggProgram, meta);
  ssig.header.m_noOfSections = 3;
  ssig.ptr[0].p = aggProgram.data();
  ssig.ptr[0].sz = (Uint32)aggProgram.size();
  ssig.ptr[1].p = &receiverId;
  ssig.ptr[1].sz = 1;
  ssig.ptr[2].p = metadata.data();
  ssig.ptr[2].sz = (Uint32)metadata.size();
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SETUP_REQ failed\n");
    return -1;
  }
  SimpleSignal *resp = waitForSignal(ss, "SETUP_CONF");
  if (resp == nullptr) return -1;
  const int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_SETUP_CONF) {
    const JoinAggSetupConf *conf =
        reinterpret_cast<const JoinAggSetupConf *>(resp->getDataPtr());
    keyOut = conf->aggStateKey;
    ownerOut = conf->ownerInstance;
    V("SETUP_CONF: key=%u owner=%u\n", keyOut, ownerOut);
    return 0;
  }
  if (gsn == GSN_JOIN_AGG_SETUP_REF) {
    const JoinAggSetupRef *ref =
        reinterpret_cast<const JoinAggSetupRef *>(resp->getDataPtr());
    refCodeOut = ref->errorCode;
    V("SETUP_REF: errorCode=%u errorLine=%u\n", ref->errorCode,
      ref->errorLine);
    return 1;
  }
  fprintf(stderr, "Unexpected GSN %d waiting for SETUP_CONF\n", gsn);
  return -1;
}

/* Feed the state on `nodeId` from every fragment whose primary is there;
 * rowsOut counts rowsExamined, which equals groups for this unfiltered
 * scan of unique a values. completedOps counts returned rows, not rows
 * consumed by join aggregation. */
static int
runScans(SignalSender &ss, const TableMeta &meta, Uint32 nodeId,
         Uint32 aggStateKey, Uint32 &rowsOut)
{
  const std::vector<Uint32> attrInfo = buildAttrInfo();
  rowsOut = 0;
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (meta.fragNodes[f] != nodeId) continue;
    SimpleSignal ssig;
    Uint32 *data = ssig.getDataPtrSend();
    memset(data, 0, 25 * sizeof(Uint32));
    ScanFragReq *scanReq = reinterpret_cast<ScanFragReq *>(data);
    scanReq->senderData = f;
    scanReq->resultRef = ss.getOwnRef();
    scanReq->savePointId = 0;
    Uint32 requestInfo = 0;
    ScanFragReq::setReadCommittedFlag(requestInfo, 1);
    ScanFragReq::setCorrFactorFlag(requestInfo, 1);
    ScanFragReq::setJoinAggFlag(requestInfo, 1);
    scanReq->requestInfo = requestInfo;
    scanReq->tableId = meta.tableId;
    scanReq->fragmentNoKeyLen = f;
    scanReq->schemaVersion = meta.schemaVersion;
    scanReq->transId1 = FAKE_TRANS_ID1;
    scanReq->transId2 = FAKE_TRANS_ID2;
    scanReq->resultData = FAKE_SENDER_DATA;
    scanReq->batch_size_rows = 100;
    scanReq->batch_size_bytes = 65536;
    Uint32 varIdx = 0;
    scanReq->variableData[varIdx++] = 0;  /* corrFactorLo */
    scanReq->variableData[varIdx++] = 0;  /* corrFactorHi */
    scanReq->variableData[varIdx++] = aggStateKey;
    const Uint32 sigLen = ScanFragReq::SignalLength + varIdx;
    const Uint16 recBlock = numberToBlock(V_QUERY, meta.fragInstances[f]);
    ssig.set(ss, 0, recBlock, GSN_SCAN_FRAGREQ, sigLen);
    ssig.header.m_noOfSections = 1;
    ssig.ptr[0].p = attrInfo.data();
    ssig.ptr[0].sz = (Uint32)attrInfo.size();
    if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal SCAN_FRAGREQ failed\n");
      return -1;
    }
    SimpleSignal *resp = waitForSignal(ss, "SCAN_FRAGCONF");
    if (resp == nullptr) return -1;
    const int gsn = getGsn(resp);
    if (gsn != GSN_SCAN_FRAGCONF) {
      fprintf(stderr, "Unexpected GSN %d waiting for SCAN_FRAGCONF\n", gsn);
      return -1;
    }
    const ScanFragConf *conf =
        reinterpret_cast<const ScanFragConf *>(resp->getDataPtr());
    if (resp->getLength() < ScanFragConf::SignalLength_v2) {
      fprintf(stderr, "SCAN_FRAGCONF lacks rowsExamined\n");
      return -1;
    }
    rowsOut += conf->rowsExamined;
  }
  V("Scanned %u rows into the state on node %u\n", rowsOut, nodeId);
  return 0;
}

static int
sendCompleteReq(SignalSender &ss, Uint32 nodeId, Uint32 aggStateKey,
                Uint32 ownerInstance)
{
  SimpleSignal ssig;
  JoinAggCompleteReq *req =
      reinterpret_cast<JoinAggCompleteReq *>(ssig.getDataPtrSend());
  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = FAKE_REQUEST_ID;
  req->transid[0] = FAKE_TRANS_ID1;
  req->transid[1] = FAKE_TRANS_ID2;
  req->aggStateKey = aggStateKey;
  req->maxBatchRows = 1000;
  req->heartbeatScanFragPtrI = RNIL;
  req->identWord = RNIL;  /* keyed form */
  ssig.set(ss, 0, numberToBlock(DBLQH, ownerInstance),
           GSN_JOIN_AGG_COMPLETE_REQ, JoinAggCompleteReq::SignalLength);
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal COMPLETE_REQ failed\n");
    return -1;
  }
  return 0;
}

static int
sendSendConf(SignalSender &ss, const JoinAggSendReq *sendReq)
{
  SimpleSignal ssig;
  JoinAggSendConf *conf =
      reinterpret_cast<JoinAggSendConf *>(ssig.getDataPtrSend());
  conf->senderRef = ss.getOwnRef();
  conf->senderData = sendReq->senderData;
  conf->requestId = sendReq->requestId;
  conf->aggStateKey = sendReq->aggStateKey;
  conf->maxBatchRows = 1000;
  ssig.set(ss, 0, refToBlock(sendReq->senderRef), GSN_JOIN_AGG_SEND_CONF,
           JoinAggSendConf::SignalLength);
  return ss.sendSignal(refToNode(sendReq->senderRef), &ssig) == SEND_OK
             ? 0 : -1;
}

/* Count the groups a COMPLETE streams back, answering its flow control. */
static int
receiveResults(SignalSender &ss, Uint32 &groupsOut)
{
  groupsOut = 0;
  for (;;) {
    SimpleSignal *resp = waitForSignal(ss, "COMPLETE results");
    if (resp == nullptr) return -1;
    const int gsn = getGsn(resp);
    if (gsn == GSN_TRANSID_AI) {
      const Uint32 *data;
      Uint32 dataLen;
      if (resp->header.m_noOfSections > 0) {
        data = resp->ptr[0].p;
        dataLen = resp->ptr[0].sz;
      } else {
        data = resp->getDataPtr() + TransIdAI::HeaderLength;
        dataLen = resp->getLength() - TransIdAI::HeaderLength;
      }
      if (dataLen < 3 || data[0] != ((AGG_RESULT_ATTR << 16) | AGG_MAGIC)) {
        fprintf(stderr, "Bad AGG_RESULT block (len %u)\n", dataLen);
        return -1;
      }
      groupsOut += data[2];
    } else if (gsn == GSN_JOIN_AGG_SEND_REQ) {
      const JoinAggSendReq *sendReq =
          reinterpret_cast<const JoinAggSendReq *>(resp->getDataPtr());
      if (sendSendConf(ss, sendReq) != 0) return -1;
    } else if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
      const JoinAggCompleteConf *conf =
          reinterpret_cast<const JoinAggCompleteConf *>(resp->getDataPtr());
      V("COMPLETE_CONF: numResultRows=%u\n", conf->numResultRows);
      return 0;
    } else if (gsn == GSN_JOIN_AGG_COMPLETE_REF) {
      const JoinAggCompleteRef *ref =
          reinterpret_cast<const JoinAggCompleteRef *>(resp->getDataPtr());
      fprintf(stderr, "COMPLETE_REF: errorCode=%u errorLine=%u\n",
              ref->errorCode, ref->errorLine);
      return -1;
    } else {
      fprintf(stderr, "Unexpected GSN %d during COMPLETE results\n", gsn);
      return -1;
    }
  }
}

static int
sendReleaseReq(SignalSender &ss, Uint32 nodeId, Uint32 aggStateKey,
               Uint32 requestId, Uint32 transid1, Uint32 transid2,
               Uint32 noReply)
{
  SimpleSignal ssig;
  JoinAggReleaseReq *req =
      reinterpret_cast<JoinAggReleaseReq *>(ssig.getDataPtrSend());
  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = requestId;
  req->transid[0] = transid1;
  req->transid[1] = transid2;
  req->aggStateKey = aggStateKey;
  req->noReply = noReply;
  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_RELEASE_REQ,
           JoinAggReleaseReq::SignalLength);
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal RELEASE_REQ failed\n");
    return -1;
  }
  return 0;
}

static int
waitReleaseConf(SignalSender &ss, const char *context)
{
  SimpleSignal *resp = waitForSignal(ss, context);
  if (resp == nullptr) return -1;
  const int gsn = getGsn(resp);
  if (gsn != GSN_JOIN_AGG_RELEASE_CONF) {
    fprintf(stderr, "Unexpected GSN %d waiting for %s\n", gsn, context);
    return -1;
  }
  return 0;
}

/* Keep a client available to transporter broadcasts while another
 * client is polled or the test waits on a management event. */
struct ScopedSenderUnlock {
  SignalSender &sender;
  explicit ScopedSenderUnlock(SignalSender &s) : sender(s) {
    sender.unlock();
  }
  ~ScopedSenderUnlock() { sender.lock(); }
};

/* NdbRestarter's insert helpers can return success after a management
 * error. Check the API result and the server's reply explicitly. */
static bool
setErrorInsert(NdbRestarter &restarter, Uint32 node, int error, int extra)
{
  ndb_mgm_reply reply = {};
  if (restarter.handle == nullptr ||
      ndb_mgm_insert_error2(restarter.handle, node, error, extra, &reply) == -1 ||
      reply.return_code != 0) {
    fprintf(stderr, "Setting error insert %d on node %u failed\n",
            error, node);
    return false;
  }
  return true;
}

struct ErrorInsertGuard {
  NdbRestarter &restarter;
  Uint32 node;
  bool active;

  bool clear() {
    if (!active) return true;
    if (!setErrorInsert(restarter, node, 0, 0)) return false;
    active = false;
    return true;
  }
  ~ErrorInsertGuard() { (void)clear(); }
};

/* Subscribe before releasing the state so the hold event cannot be
 * missed. Unrelated events do not extend the timeout. */
struct ProtocolEventListener {
  NdbSocket socket;
  ~ProtocolEventListener() {
    if (socket.is_valid()) socket.close();
  }
  bool open(NdbRestarter &restarter) {
    if (restarter.handle == nullptr) return false;
    int filter[] = {2, NDB_MGM_EVENT_CATEGORY_INFO, 0};
    socket = ndb_mgm_listen_event_internal(restarter.handle, filter, 0, true);
    return socket.is_valid();
  }
  bool waitFor(const char *marker) {
    SocketInputStream input(socket, 100);
    const Uint64 start = NdbTick_CurrentMillisecond();
    char line[1024] = {};
    while (NdbTick_CurrentMillisecond() - start < WAIT_TIMEOUT_MS) {
      input.reset_timeout();
      if (input.gets(line, sizeof(line)) == nullptr) {
        fprintf(stderr, "Failed to read management events\n");
        return false;
      }
      if (strstr(line, marker) != nullptr) return true;
    }
    fprintf(stderr, "TIMEOUT waiting for %s\n", marker);
    return false;
  }
};

/* Snapshot every data node, requiring STARTED without waiting for a
 * failed node to restart. The management connection counter changes
 * on disconnect/reconnect even if STARTED is observed again. */
static bool
readStartedNodeConnections(NdbRestarter &restarter,
                           std::map<int, int> &connections,
                           const char *label)
{
  connections.clear();
  const ndb_mgm_node_type types[] = {
      NDB_MGM_NODE_TYPE_NDB, NDB_MGM_NODE_TYPE_UNKNOWN};
  ndb_mgm_cluster_state *state =
      restarter.handle != nullptr
          ? ndb_mgm_get_status2(restarter.handle, types) : nullptr;
  if (state == nullptr) {
    fprintf(stderr, "%s: cannot read data-node status\n", label);
    return false;
  }
  bool ok = true;
  for (int i = 0; i < state->no_of_nodes; i++) {
    const ndb_mgm_node_state &node = state->node_states[i];
    if (node.node_status != NDB_MGM_NODE_STATUS_STARTED ||
        node.connect_count < 0) {
      fprintf(stderr, "%s: node %d is not ready for leak verification "
                      "(status=%d connect_count=%d)\n",
              label, node.node_id, (int)node.node_status, node.connect_count);
      ok = false;
      break;
    }
    connections[node.node_id] = node.connect_count;
  }
  free(state);
  if (connections.empty()) {
    fprintf(stderr, "%s: no started data nodes found\n", label);
    return false;
  }
  return ok;
}

/* Require positive completion of DUMP 2361 and 2363 on every node.
 * A management success only confirms that the dump was sent. */
static int
checkLeaks(SignalSender &ss, NdbRestarter &restarter, const char *label)
{
  /* Management waits must not block this API client's heartbeats. */
  ScopedSenderUnlock unlockSender(ss);
  std::map<int, int> before, after;
  if (!readStartedNodeConnections(restarter, before, label)) return -1;
  ProtocolEventListener events;
  if (!events.open(restarter)) {
    fprintf(stderr, "%s: cannot subscribe to leak-check events\n", label);
    return -1;
  }
  /* Each invocation uses a new cookie so a late event cannot satisfy
   * the next case's check. Node and dump code are matched as well. */
  static Uint32 nextCookie = 0;
  const Uint32 cookie = ++nextCookie;
  NdbSleep_MilliSleep(TEARDOWN_SETTLE_MS);
  const int codes[] = {DumpStateOrd::LqhDumpJoinAggStates,
                       DumpStateOrd::LqhDumpJoinAggIdentity};
  for (const auto &node : before) {
    for (unsigned i = 0; i < NDB_ARRAY_SIZE(codes); i++) {
      const int dump[] = {codes[i], (int)cookie};
      ndb_mgm_reply reply = {};
      if (ndb_mgm_dump_state(restarter.handle, node.first, dump, 2,
                             &reply) == -1 ||
          reply.return_code != 0) {
        fprintf(stderr, "%s: DUMP %d on node %d failed\n",
                label, codes[i], node.first);
        return -1;
      }
      char marker[128];
      snprintf(marker, sizeof(marker),
               "[JOIN_AGG_LEAK_CHECK_OK node=%u dump=%u cookie=%u]",
               (Uint32)node.first, (Uint32)codes[i], cookie);
      if (!events.waitFor(marker)) return -1;
    }
  }
  if (!readStartedNodeConnections(restarter, after, label)) return -1;
  if (before != after) {
    fprintf(stderr, "%s: data-node connections changed during leak "
                    "verification\n", label);
    return -1;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* Cases                                                               */
/* ------------------------------------------------------------------ */

struct Ctx {
  SignalSender &ss;
  Ndb_cluster_connection &con;
  NdbRestarter &restarter;
  const TableMeta &meta;
  const std::vector<Uint32> &prog;
  Uint32 node;   /* the data node driven by every case */
};

static int
setupDefault(Ctx &c, Uint32 &key, Uint32 &owner)
{
  SetupParams p;
  Uint32 refCode = 0;
  const int rc = sendSetupReq(c.ss, c.node, c.prog, c.meta, p, key, owner,
                              refCode);
  if (rc != 0) {
    fprintf(stderr, "SETUP not accepted (rc=%d refCode=%u)\n", rc, refCode);
    return -1;
  }
  return 0;
}

static int
releaseAndWait(Ctx &c, Uint32 key)
{
  if (sendReleaseReq(c.ss, c.node, key, FAKE_REQUEST_ID, FAKE_TRANS_ID1,
                     FAKE_TRANS_ID2, 0) != 0)
    return -1;
  return waitReleaseConf(c.ss, "RELEASE_CONF");
}

/* TD-1: two RELEASEs back to back, two CONFs, one teardown. */
static int
td1(Ctx &c)
{
  Uint32 key, owner;
  if (setupDefault(c, key, owner) != 0) return -1;
  for (int i = 0; i < 2; i++) {
    if (sendReleaseReq(c.ss, c.node, key, FAKE_REQUEST_ID, FAKE_TRANS_ID1,
                       FAKE_TRANS_ID2, 0) != 0)
      return -1;
  }
  if (waitReleaseConf(c.ss, "first RELEASE_CONF") != 0) return -1;
  if (waitReleaseConf(c.ss, "second RELEASE_CONF") != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "TD-1");
}

/* TD-2: require the 5146 hold event before the second RELEASE.
 * It must CONF without another teardown; pools clean after clearing. */
static int
td2(Ctx &c)
{
  ProtocolEventListener events;
  if (!events.open(c.restarter)) {
    fprintf(stderr, "TD-2: failed to subscribe to teardown events\n");
    return -1;
  }
  Uint32 key, owner, rows;
  if (setupDefault(c, key, owner) != 0) return -1;
  if (runScans(c.ss, c.meta, c.node, key, rows) != 0) return -1;
  /* Install before arming: a failed call may still deliver the insert. */
  ErrorInsertGuard insertGuard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 5146, 1)) return -1;
  if (releaseAndWait(c, key) != 0) return -1;
  char marker[160];
  snprintf(marker, sizeof(marker),
           "[CTE_NF10_TEARDOWN_HELD node=%u iteration=1 "
           "coordinator=%u key=%u]",
           c.node, refToNode(c.ss.getOwnRef()), key);
  {
    ScopedSenderUnlock unlockOwner(c.ss);
    if (!events.waitFor(marker)) return -1;
  }
  if (releaseAndWait(c, key) != 0) return -1;
  if (!insertGuard.clear()) return -1;
  return checkLeaks(c.ss, c.restarter, "TD-2");
}

/* TD-3: a RELEASE with a wrong requestId is acknowledged and ignored;
 * the state answers a COMPLETE afterwards with its groups. */
static int
td3(Ctx &c)
{
  Uint32 key, owner, rows;
  if (setupDefault(c, key, owner) != 0) return -1;
  if (runScans(c.ss, c.meta, c.node, key, rows) != 0) return -1;
  if (sendReleaseReq(c.ss, c.node, key, FAKE_REQUEST_ID + 1, FAKE_TRANS_ID1,
                     FAKE_TRANS_ID2, 0) != 0)
    return -1;
  if (waitReleaseConf(c.ss, "RELEASE_CONF (wrong requestId)") != 0)
    return -1;
  Uint32 groups = 0;
  if (sendCompleteReq(c.ss, c.node, key, owner) != 0) return -1;
  if (receiveResults(c.ss, groups) != 0) {
    fprintf(stderr, "TD-3: the state did not survive the mis-addressed "
                    "RELEASE\n");
    return -1;
  }
  if (releaseAndWait(c, key) != 0) return -1;
  if (groups != rows) {
    fprintf(stderr, "TD-3: %u groups after COMPLETE, expected %u\n", groups,
            rows);
    return -1;
  }
  return checkLeaks(c.ss, c.restarter, "TD-3");
}

/* TD-4: a RELEASE from another sender is acknowledged to that sender and
 * ignored; COMPLETE must still return the scanned groups before release. */
static int
td4(Ctx &c)
{
  Uint32 key, owner, rows;
  if (setupDefault(c, key, owner) != 0) return -1;
  if (runScans(c.ss, c.meta, c.node, key, rows) != 0) return -1;
  {
    /* Polling or closing another client can deliver a broadcast to the
     * owner. Keep it unlocked until other has also been destroyed. */
    ScopedSenderUnlock unlockOwner(c.ss);
    SignalSender other(&c.con);
    other.lock();
    int rc = sendReleaseReq(other, c.node, key, FAKE_REQUEST_ID,
                            FAKE_TRANS_ID1, FAKE_TRANS_ID2, 0);
    if (rc == 0) rc = waitReleaseConf(other, "RELEASE_CONF (other sender)");
    other.unlock();
    if (rc != 0) return -1;
  }
  /* RELEASE_CONF also acknowledges an already-freed state. COMPLETE
   * must prove that the wrong sender left the original state intact. */
  Uint32 groups = 0;
  if (sendCompleteReq(c.ss, c.node, key, owner) != 0) return -1;
  if (receiveResults(c.ss, groups) != 0) {
    fprintf(stderr, "TD-4: the state did not survive the wrong-sender "
                    "RELEASE\n");
    return -1;
  }
  if (releaseAndWait(c, key) != 0) return -1;
  if (groups != rows) {
    fprintf(stderr, "TD-4: %u groups after COMPLETE, expected %u\n",
            groups, rows);
    return -1;
  }
  return checkLeaks(c.ss, c.restarter, "TD-4");
}

/* TD-5: the stale-reclaim form, zero transid with the right requestId. */
static int
td5(Ctx &c)
{
  Uint32 key, owner;
  if (setupDefault(c, key, owner) != 0) return -1;
  if (sendReleaseReq(c.ss, c.node, key, FAKE_REQUEST_ID, 0, 0, 0) != 0)
    return -1;
  if (waitReleaseConf(c.ss, "RELEASE_CONF (zero transid)") != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "TD-5");
}

/* TD-6: the proxy queues an exact duplicate behind the RELEASE (5136). */
static int
td6(Ctx &c)
{
  Uint32 key, owner;
  if (setupDefault(c, key, owner) != 0) return -1;
  if (c.restarter.insertErrorInNode((int)c.node, 5136) != 0) {
    fprintf(stderr, "insertErrorInNode(5136) failed\n");
    return -1;
  }
  int rc = releaseAndWait(c, key);
  if (rc == 0) rc = waitReleaseConf(c.ss, "duplicate RELEASE_CONF");
  (void)c.restarter.insertErrorInNode((int)c.node, 0);  /* 5136 self-clears */
  if (rc != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "TD-6");
}

/* TD-7: a full-length SETUP naming an unreachable node is refused with
 * 286 before any state is seized; naming exactly the data nodes is
 * accepted. */
static int
td7(Ctx &c)
{
  NdbNodeBitmask dataNodes;
  for (int i = 0; i < c.restarter.getNumDbNodes(); i++) {
    dataNodes.set((Uint32)c.restarter.getDbNodeId(i));
  }
  Uint32 badNode = 0;
  for (Uint32 n = 1; n < 48 && badNode == 0; n++) {
    if (!dataNodes.get(n)) badNode = n;
  }
  if (badNode == 0) {
    fprintf(stderr, "TD-7: no free node id below 48\n");
    return -1;
  }
  {
    SetupParams p;
    p.strategy |= JoinAggSetupReq::CTE_MODE_FLAG;
    p.cteIndex = 0;
    p.fullLength = true;
    p.setupNodes = dataNodes;
    p.setupNodes.set(badNode);
    Uint32 key = RNIL, owner = 0, refCode = 0;
    const int rc = sendSetupReq(c.ss, c.node, c.prog, c.meta, p, key, owner,
                                refCode);
    if (rc == 0) {
      fprintf(stderr, "TD-7: SETUP naming unreachable node %u was accepted\n",
              badNode);
      (void)releaseAndWait(c, key);
      return -1;
    }
    if (rc != 1 || refCode != ZNODEFAIL_BEFORE_COMMIT) {
      fprintf(stderr, "TD-7: expected SETUP_REF 286, got rc=%d code=%u\n", rc,
              refCode);
      return -1;
    }
    V("TD-7: SETUP naming node %u refused with 286\n", badNode);
  }
  {
    SetupParams p;
    p.strategy |= JoinAggSetupReq::CTE_MODE_FLAG;
    p.cteIndex = 0;
    p.fullLength = true;
    p.setupNodes = dataNodes;
    Uint32 key = RNIL, owner = 0, refCode = 0;
    if (sendSetupReq(c.ss, c.node, c.prog, c.meta, p, key, owner, refCode) !=
        0) {
      fprintf(stderr, "TD-7: full-length SETUP naming the data nodes was not "
                      "accepted (code=%u)\n", refCode);
      return -1;
    }
    if (releaseAndWait(c, key) != 0) return -1;
  }
  return checkLeaks(c.ss, c.restarter, "TD-7");
}

/* TD-8: a CTE-mode SETUP of the pre-RONDB-1120 length is accepted. */
static int
td8(Ctx &c)
{
  SetupParams p;
  p.strategy |= JoinAggSetupReq::CTE_MODE_FLAG;
  p.cteIndex = 0;
  Uint32 key = RNIL, owner = 0, refCode = 0;
  if (sendSetupReq(c.ss, c.node, c.prog, c.meta, p, key, owner, refCode) !=
      0) {
    fprintf(stderr, "TD-8: legacy-length CTE SETUP not accepted (code=%u)\n",
            refCode);
    return -1;
  }
  if (releaseAndWait(c, key) != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "TD-8");
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [-c connectstring] [-m mysql_port] [-v]\n", argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    }
  }
  if (connectString == nullptr) connectString = "localhost:1186";

  /* Only PASSED / FAILED goes to the real stdout (the MTR wrapper). */
  const int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int result = 0;
  do {
    MYSQL *conn = connectMysql("test");
    if (conn == nullptr) { result = 1; break; }

    Ndb_cluster_connection con(connectString);
    if (con.connect(30, 5, 1) != 0 || con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cannot connect to the cluster at %s\n", connectString);
      mysql_close(conn);
      result = 1;
      break;
    }
    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
      mysql_close(conn);
      result = 1;
      break;
    }
    NdbRestarter restarter(connectString);

    TableMeta meta;
    if (createTestTable(conn, &ndb, meta) != 0 ||
        insertRows(&ndb, ROWS) != 0) {
      mysql_close(conn);
      result = 1;
      break;
    }
    const std::vector<Uint32> prog =
        buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

    {
      SignalSender ss(&con);
      ss.lock();
      Ctx c = {ss, con, restarter, meta, prog,
               (Uint32)restarter.getDbNodeId(0)};
      V("Driving data node %u from ref 0x%08x\n", c.node, ss.getOwnRef());

      struct Case { const char *name; int (*fn)(Ctx &); };
      const Case cases[] = {
          {"TD-1 two RELEASEs back to back", td1},
          {"TD-2 second RELEASE during a held teardown", td2},
          {"TD-3 RELEASE with a wrong requestId", td3},
          {"TD-4 RELEASE from another sender", td4},
          {"TD-5 RELEASE with a zero transid", td5},
          {"TD-6 duplicate RELEASE queued by the proxy", td6},
          {"TD-7 SETUP naming an unreachable node", td7},
          {"TD-8 legacy-length CTE SETUP", td8},
      };
      for (unsigned i = 0; i < NDB_ARRAY_SIZE(cases); i++) {
        const int rc = cases[i].fn(c);
        fprintf(stderr, "%s: %s\n", cases[i].name,
                rc == 0 ? "PASSED" : "FAILED");
        if (rc != 0) {
          result = 1;
          /* A failed case may leave its state allocated. All cases use
           * the same identity, so another SETUP could collide with it. */
          fprintf(stderr, "Stopping after %s failed\n", cases[i].name);
          break;
        }
      }
      ss.unlock();
    }
    dropTestTable(conn);
    mysql_close(conn);
  } while (0);

  const char *verdict = result == 0 ? "PASSED\n" : "FAILED\n";
  if (write(mtr_fd, verdict, strlen(verdict)) < 0) result = 1;
  ndb_end(0);
  return result;
}
