/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * testCtePhase6 — Comprehensive Phase 6 tests for CTE pushdown in DBLQH.
 *
 * Tests the full CTE lifecycle with data validation:
 *   1. JOIN_AGG_SETUP_REQ (CTE mode) → hash table created
 *   2. SCAN_FRAGREQ → rows fed into aggregation
 *   3. JOIN_AGG_COMPLETE_REQ → CTE_READY (with redistribution on multi-node)
 *   4. CTE_LOOKUP_REQ → TRANSID_AI with verified column data
 *   5. JOIN_AGG_RELEASE_REQ → cleanup
 *
 * The test program acts as both DBTC and NDB API via SignalSender.
 *
 * Test matrix:
 *   Test 1: SUM data validation — verify actual returned values
 *   Test 2: Multi-row groups — multiple rows per GROUP BY key
 *   Test 3: All aggregate types — COUNT+SUM+MAX+MIN in one program
 *   Test 4: Batch lookups — lookup all groups, verify all values
 *   Test 5: Many groups — 50 groups stress test
 *   Test 6: Multi-node redistribution — 2-node cluster, verify post-redist lookups
 *   Test 7: Multi-node all aggregates — 2-node with COUNT+SUM+MAX+MIN
 *
 * Usage: testCtePhase6 -c <connect_string> -m <mysql_port> [-v]
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
#include <kernel/signaldata/CteLookup.hpp>
#include <kernel/signaldata/ScanFrag.hpp>
#include <kernel/signaldata/TransIdAI.hpp>
#include <kernel/AttributeHeader.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>

#include <kernel/signaldata/LqhKey.hpp>

#include <NdbRestarter.hpp>
#include <mysql.h>

#include <unistd.h>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <map>
#include <set>
#include <vector>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const Uint32 FAKE_TRANS_ID1 = 0xCCEE0001;
static const Uint32 FAKE_TRANS_ID2 = 0xCCEE0002;
static const Uint32 FAKE_REQUEST_ID = 3001;
static const Uint32 FAKE_SENDER_DATA = 77;
static const Uint32 WAIT_TIMEOUT_MS = 30000;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 INTERPRETER_EXIT_OK = 18;
static const Uint32 AGG_MAGIC = 0x0721;

/* ------------------------------------------------------------------ */
/* Table metadata                                                      */
/* ------------------------------------------------------------------ */

struct TableMeta {
  const char *name;
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
  std::vector<Uint32> fragInstances;
  /* Column attr IDs — up to 3 columns */
  Uint32 attrIds[3];
  Uint32 numCols;
};

static void
appendLocalBigintMetaEntry(std::vector<Uint32> &meta,
                           const TableMeta &tableMeta,
                           Uint32 columnId,
                           Uint32 programOffset,
                           Uint32 slotIndex,
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
buildJoinAggMetadata(const std::vector<Uint32> &aggProg,
                     const TableMeta &tableMeta)
{
  std::vector<Uint32> meta;
  meta.push_back(JOIN_AGG_META_MARKER);
  meta.push_back(JOIN_AGG_META_VERSION);
  meta.push_back(0);

  if (aggProg.size() < 8 || (aggProg[0] >> 16) != AGG_MAGIC) {
    return meta;
  }

  Uint32 entryCount = 0;
  const Uint32 nGbCols = aggProg[1] >> 16;
  for (Uint32 i = 0; i < nGbCols && (8 + i) < aggProg.size(); i++) {
    const Uint32 programOffset = 8 + i;
    const Uint32 columnId = (aggProg[programOffset] >> 16) & 0xFFFF;
    appendLocalBigintMetaEntry(meta, tableMeta, columnId, programOffset, i,
                               JOIN_AGG_META_FLAG_GROUP_BY);
    entryCount++;
  }

  for (Uint32 i = 8 + nGbCols; i < aggProg.size(); i++) {
    const Uint32 op = (aggProg[i] >> 26) & 0x3F;
    if (op != kOpLoadCol) continue;
    const Uint32 columnId = aggProg[i] & 0xFFFF;
    appendLocalBigintMetaEntry(meta, tableMeta, columnId, i, RNIL,
                               JOIN_AGG_META_FLAG_LOAD_COLUMN);
    entryCount++;
  }

  meta[2] = entryCount;
  return meta;
}

/* ------------------------------------------------------------------ */
/* Aggregation program builders                                        */
/* ------------------------------------------------------------------ */

static std::vector<Uint32>
buildAggProgram_SumGroupBy(Uint32 gbColId, Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;  /* 1 GB col, 1 agg result */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | sumColId;
  prog[10] = (kOpSum << 26) | (0 << 16) | 0;
  return prog;
}

static std::vector<Uint32>
buildAggProgram_CountSumGroupBy(Uint32 gbColId, Uint32 valColId)
{
  const Uint32 PROG_LEN = 13;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 2u;  /* 1 GB col, 2 agg results */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | valColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;  /* COUNT → agg[0] */
  prog[11] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | valColId;
  prog[12] = (kOpSum << 26) | (0 << 16) | 1;    /* SUM → agg[1] */
  return prog;
}

/*
 * GROUP BY gbColId: COUNT(*), SUM(val), MAX(val), MIN(val)
 * n_agg_results = 4
 */
static std::vector<Uint32>
buildAggProgram_AllAggsGroupBy(Uint32 gbColId, Uint32 valColId)
{
  const Uint32 PROG_LEN = 14;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 4u;  /* 1 GB col, 4 agg results */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | valColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;  /* COUNT → agg[0] */
  prog[11] = (kOpSum << 26) | (0 << 16) | 1;    /* SUM → agg[1] */
  prog[12] = (kOpMax << 26) | (0 << 16) | 2;    /* MAX → agg[2] */
  prog[13] = (kOpMin << 26) | (0 << 16) | 3;    /* MIN → agg[3] */
  return prog;
}

/* ------------------------------------------------------------------ */
/* AttrInfo builders                                                    */
/* ------------------------------------------------------------------ */

static std::vector<Uint32> buildScanAttrInfo()
{
  std::vector<Uint32> ai(6);
  ai[0] = 0;  /* Initial read section length */
  ai[1] = 1;  /* Interpreter program length */
  ai[2] = 0;  /* Subroutine length */
  ai[3] = 0;  /* Final read section length */
  ai[4] = 0;  /* Linked attr section length */
  ai[5] = INTERPRETER_EXIT_OK;
  return ai;
}

static std::vector<Uint32>
buildCteLookupAttrInfo(Uint32 numGbCols, Uint32 numAggResults,
                       Uint32 resultRef, Uint32 resultData, Uint32 routeRef)
{
  std::vector<Uint32> ai;
  ai.push_back(0);    /* [0] InitReadLen */
  ai.push_back(1);    /* [1] ExecRegionLen (ExitOK) */
  ai.push_back(0);    /* [2] FinalUpdateLen */
  Uint32 finalRLenIdx = (Uint32)ai.size();
  ai.push_back(0);    /* [3] FinalRLen (patched below) */
  ai.push_back(0);    /* [4] SubLen */
  /* Interpreter program */
  ai.push_back(INTERPRETER_EXIT_OK);
  /* Final read section */
  Uint32 finalRStart = (Uint32)ai.size();
  for (Uint32 i = 0; i < numGbCols; i++)
    ai.push_back(i << 16);
  for (Uint32 i = 0; i < numAggResults; i++)
    ai.push_back((numGbCols + i) << 16);
  /* FLUSH_AI */
  ai.push_back(AttributeHeader::FLUSH_AI << 16);
  ai.push_back(resultRef);
  ai.push_back(resultData);
  ai.push_back(routeRef);
  /* CORR_FACTOR */
  ai.push_back(AttributeHeader::CORR_FACTOR32 << 16);
  /* Patch FinalRLen */
  ai[finalRLenIdx] = (Uint32)(ai.size() - finalRStart);
  return ai;
}

/* ------------------------------------------------------------------ */
/* Key builder                                                         */
/* ------------------------------------------------------------------ */

static void
buildBigintKey(Uint32 *buf, Uint32 &sizeWords, Uint32 &sizeBytes,
               Uint32 attrId, Int64 value)
{
  AttributeHeader::init(&buf[0], attrId, 8);
  memcpy(&buf[1], &value, 8);
  sizeWords = 3;
  sizeBytes = 12;
}

/* ------------------------------------------------------------------ */
/* MySQL / NDB helpers                                                 */
/* ------------------------------------------------------------------ */

static int sqlExec(MYSQL *conn, const char *query)
{
  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL failed: %s\n  query: %s\n",
            mysql_error(conn), query);
    return -1;
  }
  return 0;
}

static MYSQL *connectMysql(int mysqlPort)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) { fprintf(stderr, "mysql_init failed\n"); return nullptr; }
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         "test", mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
}

static int
getTableMeta(Ndb *ndb, TableMeta &meta)
{
  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(meta.name);
  const NdbDictionary::Table *ptab = dict->getTable(meta.name);
  if (ptab == nullptr) {
    fprintf(stderr, "getTable(%s) failed: %s\n",
            meta.name, dict->getNdbError().message);
    return -1;
  }
  meta.tableId = ptab->getObjectId();
  meta.schemaVersion = ptab->getObjectVersion();
  meta.fragCount = ptab->getFragmentCount();
  meta.fragNodes.resize(meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nodeId = 0;
    ptab->getFragmentNodes(f, &nodeId, 1);
    meta.fragNodes[f] = nodeId;
  }
  return 0;
}

static int
queryFragInstances(int mysqlPort, TableMeta &meta)
{
  MYSQL *conn = connectMysql(mysqlPort);
  if (conn == nullptr) return -1;

  char query[512];
  snprintf(query, sizeof(query),
    "SELECT fragment_num, block_instance FROM ndbinfo.operations_per_fragment "
    "WHERE table_id = %u AND fragment_num < %u "
    "GROUP BY fragment_num, block_instance "
    "ORDER BY fragment_num",
    meta.tableId, meta.fragCount);

  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "queryFragInstances failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return -1;
  }
  MYSQL_RES *res = mysql_store_result(conn);
  meta.fragInstances.resize(meta.fragCount, 1);
  if (res) {
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
      Uint32 frag = atoi(row[0]);
      Uint32 inst = atoi(row[1]);
      if (frag < meta.fragCount)
        meta.fragInstances[frag] = inst;
    }
    mysql_free_result(res);
  }
  mysql_close(conn);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Signal helpers                                                      */
/* ------------------------------------------------------------------ */

static int getGsn(const SimpleSignal *sig) {
  return sig->readSignalNumber();
}

static SimpleSignal *
waitForSignal(SignalSender &ss, Uint32 timeoutMs, const char *label)
{
  SimpleSignal *sig = ss.waitFor(timeoutMs);
  if (sig == nullptr) {
    fprintf(stderr, "TIMEOUT waiting for %s\n", label);
  }
  return sig;
}

/* ------------------------------------------------------------------ */
/* Signal senders                                                      */
/* ------------------------------------------------------------------ */

static int
sendSetupReq(
  SignalSender &ss, Uint32 nodeId,
  const std::vector<Uint32> &aggProg, const TableMeta &meta,
  Uint32 &aggStateKeyOut,
  Uint32 &ownerInstanceOut)
{
  Uint32 receiverId = FAKE_SENDER_DATA;
  SimpleSignal ssig;
  JoinAggSetupReq *req =
    reinterpret_cast<JoinAggSetupReq *>(ssig.getDataPtrSend());

  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = FAKE_REQUEST_ID;
  req->transid[0] = FAKE_TRANS_ID1;
  req->transid[1] = FAKE_TRANS_ID2;
  req->tableId = meta.tableId;
  req->expectedOpCount = 0;
  req->concurrencyStrategy =
      JoinAggSetupReq::STRATEGY_MUTEX_BASED | JoinAggSetupReq::CTE_MODE_FLAG;
  req->resultRef = ss.getOwnRef();
  req->resultData = FAKE_SENDER_DATA;
  req->routeRef = ss.getOwnRef();
  req->cteIndex = RNIL;

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_SETUP_REQ,
           JoinAggSetupReq::SignalLength);
  const std::vector<Uint32> metadata = buildJoinAggMetadata(aggProg, meta);
  ssig.header.m_noOfSections = 3;
  ssig.ptr[0].p = const_cast<Uint32*>(aggProg.data());
  ssig.ptr[0].sz = (Uint32)aggProg.size();
  ssig.ptr[1].p = &receiverId;
  ssig.ptr[1].sz = 1;
  ssig.ptr[2].p = const_cast<Uint32*>(metadata.data());
  ssig.ptr[2].sz = (Uint32)metadata.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SETUP_REQ failed\n");
    return -1;
  }

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SETUP_CONF");
  if (resp == nullptr) return -1;
  if (getGsn(resp) == GSN_JOIN_AGG_SETUP_CONF) {
    const JoinAggSetupConf *conf =
      reinterpret_cast<const JoinAggSetupConf *>(resp->getDataPtr());
    aggStateKeyOut = conf->aggStateKey;
    ownerInstanceOut = conf->ownerInstance;
    V("  SETUP_CONF: node=%u aggStateKey=%u ownerInstance=%u\n",
      nodeId, aggStateKeyOut, ownerInstanceOut);
    return 0;
  }
  if (getGsn(resp) == GSN_JOIN_AGG_SETUP_REF) {
    const JoinAggSetupRef *ref =
      reinterpret_cast<const JoinAggSetupRef *>(resp->getDataPtr());
    fprintf(stderr, "SETUP_REF: error=%u line=%u\n", ref->errorCode, ref->errorLine);
  } else {
    fprintf(stderr, "Expected SETUP_CONF, got GSN=%d\n", getGsn(resp));
  }
  return -1;
}

static int
sendScanFragReq(SignalSender &ss, Uint32 nodeId, Uint32 fragId,
                Uint32 ldmInst, Uint32 aggStateKey,
                const TableMeta &meta, const std::vector<Uint32> &attrInfo)
{
  SimpleSignal ssig;
  ScanFragReq *scanReq =
    reinterpret_cast<ScanFragReq *>(ssig.getDataPtrSend());
  memset(scanReq, 0, sizeof(ScanFragReq));

  scanReq->senderData = fragId;
  scanReq->resultRef = ss.getOwnRef();
  scanReq->savePointId = 0;

  Uint32 requestInfo = 0;
  ScanFragReq::setReadCommittedFlag(requestInfo, 1);
  ScanFragReq::setCorrFactorFlag(requestInfo, 1);
  ScanFragReq::setJoinAggFlag(requestInfo, 1);
  scanReq->requestInfo = requestInfo;
  scanReq->tableId = meta.tableId;
  scanReq->fragmentNoKeyLen = fragId;
  scanReq->schemaVersion = meta.schemaVersion;
  scanReq->transId1 = FAKE_TRANS_ID1;
  scanReq->transId2 = FAKE_TRANS_ID2;
  scanReq->resultData = FAKE_SENDER_DATA;
  scanReq->batch_size_rows = 1000;
  scanReq->batch_size_bytes = 65536;

  Uint32 varIdx = 0;
  scanReq->variableData[varIdx++] = 0;  /* corrFactorLo */
  scanReq->variableData[varIdx++] = 0;  /* corrFactorHi */
  scanReq->variableData[varIdx++] = aggStateKey;
  Uint32 sigLen = ScanFragReq::SignalLength + varIdx;

  Uint16 recBlock = numberToBlock(V_QUERY, ldmInst);
  ssig.set(ss, 0, recBlock, GSN_SCAN_FRAGREQ, sigLen);
  ssig.header.m_noOfSections = 1;
  ssig.ptr[0].p = const_cast<Uint32*>(attrInfo.data());
  ssig.ptr[0].sz = (Uint32)attrInfo.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SCAN_FRAGREQ failed\n");
    return -1;
  }
  return 0;
}

static int
waitForScanConf(SignalSender &ss, Uint32 &rowsScanned)
{
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SCAN_FRAGCONF");
  if (resp == nullptr) return -1;
  if (getGsn(resp) == GSN_SCAN_FRAGCONF) {
    const ScanFragConf *conf =
      reinterpret_cast<const ScanFragConf *>(resp->getDataPtr());
    rowsScanned = conf->completedOps;
    V("  SCAN_FRAGCONF: completed=%u\n", rowsScanned);
    return 0;
  }
  fprintf(stderr, "Expected SCAN_FRAGCONF, got GSN=%d\n", getGsn(resp));
  return -1;
}

static int
sendCompleteReq(
  SignalSender &ss, Uint32 nodeId, Uint32 aggStateKey,
  Uint32 ownerInstance,
  const std::map<Uint32, Uint32> &allAggKeys,
  const std::map<Uint32, Uint32> &allOwners)
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

  std::vector<Uint32> keyTriples;
  for (auto &kv : allAggKeys) {
    keyTriples.push_back(kv.first);
    keyTriples.push_back(kv.second);
    auto it = allOwners.find(kv.first);
    keyTriples.push_back(it != allOwners.end() ? it->second : 1);
  }

  Uint16 recBlock = numberToBlock(DBLQH, ownerInstance);
  ssig.set(ss, 0, recBlock, GSN_JOIN_AGG_COMPLETE_REQ,
           JoinAggCompleteReq::SignalLength);
  ssig.header.m_noOfSections = 1;
  ssig.ptr[0].p = keyTriples.data();
  ssig.ptr[0].sz = (Uint32)keyTriples.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal COMPLETE_REQ failed\n");
    return -1;
  }
  return 0;
}

static int
waitForCompleteConf(SignalSender &ss)
{
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "COMPLETE_CONF");
  if (resp == nullptr) return -1;
  int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
    V("  COMPLETE_CONF received\n");
    return 0;
  }
  if (gsn == GSN_TRANSID_AI) {
    fprintf(stderr, "FAIL: Got TRANSID_AI before COMPLETE_CONF in CTE mode!\n");
    return -1;
  }
  fprintf(stderr, "Expected COMPLETE_CONF, got GSN=%d\n", gsn);
  return -1;
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
  req->noReply = 0;

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_RELEASE_REQ,
           JoinAggReleaseReq::SignalLength);
  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal RELEASE_REQ failed\n");
    return -1;
  }
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "RELEASE_CONF");
  if (resp == nullptr) return -1;
  if (getGsn(resp) == GSN_JOIN_AGG_RELEASE_CONF) {
    V("  RELEASE_CONF\n");
    return 0;
  }
  fprintf(stderr, "Expected RELEASE_CONF, got GSN=%d\n", getGsn(resp));
  return -1;
}

static int
sendCteLookupReq(SignalSender &ss, Uint32 nodeId, Uint32 ldmInst,
                 Uint32 aggStateKey, Uint32 correlationId,
                 const Uint32 *keyBuf, Uint32 keySizeWords, Uint32 keyLenBytes,
                 const std::vector<Uint32> &attrInfo,
                 bool route = true)
{
  SimpleSignal ssig;
  CteLookupReq *req =
    reinterpret_cast<CteLookupReq *>(ssig.getDataPtrSend());
  req->senderRef = ss.getOwnRef();
  req->senderData = correlationId;
  req->aggStateKey = aggStateKey;
  req->keyLen = keyLenBytes;
  req->resultRef = ss.getOwnRef();
  req->resultData = correlationId;
  req->routeRef = ss.getOwnRef();
  req->correlation = 0;
  req->joinAggStateKey = RNIL;
  req->flags = route ? CteLookupReq::CTE_LOOKUP_ROUTE_FLAG : 0;

  Uint16 recBlock = numberToBlock(DBLQH, ldmInst);
  ssig.set(ss, 0, recBlock, GSN_CTE_LOOKUP_REQ, CteLookupReq::SignalLength);
  ssig.header.m_noOfSections = 2;
  ssig.ptr[0].p = const_cast<Uint32*>(keyBuf);
  ssig.ptr[0].sz = keySizeWords;
  ssig.ptr[1].p = const_cast<Uint32*>(attrInfo.data());
  ssig.ptr[1].sz = (Uint32)attrInfo.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal CTE_LOOKUP_REQ failed\n");
    return -1;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* CTE lifecycle: setup + scan + complete                              */
/* ------------------------------------------------------------------ */

static int
setupScanComplete(SignalSender &ss, const TableMeta &meta,
                  const std::vector<Uint32> &aggProg,
                  std::map<Uint32, Uint32> &aggStateKeys)
{
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Setup on all nodes */
  std::map<Uint32, Uint32> ownerInstances;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    Uint32 owner = 0;
    if (sendSetupReq(ss, nd, aggProg, meta, key, owner) != 0) return -1;
    aggStateKeys[nd] = key;
    ownerInstances[nd] = owner;
  }

  /* Scan all fragments */
  auto scanAI = buildScanAttrInfo();
  Uint32 totalRows = 0;
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nd = meta.fragNodes[f];
    Uint32 inst = meta.fragInstances[f];
    if (sendScanFragReq(ss, nd, f, inst, aggStateKeys[nd], meta, scanAI) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, rows) != 0) return -1;
    totalRows += rows;
  }
  V("  Total rows scanned: %u\n", totalRows);

  /* Complete on all nodes — sends COMPLETE_REQ to all, then waits for all CONFs.
   * On multi-node clusters, redistribution happens between COMPLETE_REQ and CONF. */
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], ownerInstances[nd],
      aggStateKeys, ownerInstances) != 0)
      return -1;
  }
  for (Uint32 nd [[maybe_unused]] : uniqueNodes) {
    if (waitForCompleteConf(ss) != 0) return -1;
  }

  return 0;
}

static void
releaseAll(SignalSender &ss, const std::map<Uint32, Uint32> &aggStateKeys)
{
  for (auto &kv : aggStateKeys) {
    sendReleaseReq(ss, kv.first, kv.second);
  }
}

/* ------------------------------------------------------------------ */
/* TRANSID_AI result parser for CTE_LOOKUP responses                   */
/* ------------------------------------------------------------------ */

/*
 * Parsed result from a CTE_LOOKUP_REQ response.
 * The FLUSH_AI TRANSID_AI contains AttributeHeader-encoded columns:
 *   [AttrHeader(id, byteLen)] [data words...] per column
 * Column 0..numGbCols-1 are GROUP BY keys, rest are aggregates.
 * All values stored as Int64 (BIGINT).
 */
struct LookupResult {
  bool found;
  Int64 gbKey;                    /* First (and only) GROUP BY key value */
  std::vector<Int64> aggValues;   /* Aggregate result values */
};

/*
 * Parse the FLUSH_AI TRANSID_AI section data.
 * Returns column values as Int64 vector.
 */
static std::vector<Int64>
parseAttrColumns(const Uint32 *data, Uint32 dataLen)
{
  std::vector<Int64> cols;
  Uint32 pos = 0;
  while (pos < dataLen) {
    Uint32 dataSize = AttributeHeader::getDataSize(data[pos]);
    pos++;
    if (dataSize == 0) {
      /* NULL column */
      cols.push_back(INT64_MIN);
    } else if (dataSize >= 2) {
      Int64 v;
      memcpy(&v, &data[pos], sizeof(Int64));
      cols.push_back(v);
    } else {
      /* Unexpected size */
      cols.push_back(0);
    }
    pos += dataSize;
  }
  return cols;
}

/*
 * Send CTE_LOOKUP_REQ and parse the full response:
 *   TRANSID_AI (FLUSH_AI columns) → TRANSID_AI (CORR_FACTOR) → CONF
 * or:
 *   CTE_LOOKUP_REF (not found / error)
 */
static int
lookupAndParse(SignalSender &ss, Uint32 nodeId, Uint32 ldmInst,
               Uint32 aggStateKey, Uint32 corrId,
               Uint32 gbAttrId, Int64 keyValue,
               Uint32 numGbCols, Uint32 numAggResults,
               LookupResult &result, bool route = true)
{
  result.found = false;
  result.gbKey = 0;
  result.aggValues.clear();

  auto attrInfo = buildCteLookupAttrInfo(numGbCols, numAggResults,
                                          ss.getOwnRef(), FAKE_SENDER_DATA,
                                          ss.getOwnRef());
  Uint32 keyBuf[3];
  Uint32 keyWords, keyBytes;
  buildBigintKey(keyBuf, keyWords, keyBytes, gbAttrId, keyValue);

  if (sendCteLookupReq(ss, nodeId, ldmInst, aggStateKey, corrId,
                        keyBuf, keyWords, keyBytes, attrInfo, route) != 0)
    return -1;

  /* Wait for first response */
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "LOOKUP response");
  if (resp == nullptr) return -1;

  if (getGsn(resp) == GSN_CTE_LOOKUP_REF) {
    const CteLookupRef *ref =
      reinterpret_cast<const CteLookupRef *>(resp->getDataPtr());
    V("  CTE_LOOKUP_REF: errorCode=%u\n", ref->errorCode);
    result.found = false;
    return 0;
  }

  if (getGsn(resp) != GSN_TRANSID_AI) {
    fprintf(stderr, "Expected TRANSID_AI or REF, got GSN=%d\n", getGsn(resp));
    return -1;
  }

  /* Parse FLUSH_AI TRANSID_AI — long signal with section data */
  {
    const Uint32 *data;
    Uint32 dataLen;
    if (resp->header.m_noOfSections > 0) {
      data = resp->ptr[0].p;
      dataLen = resp->ptr[0].sz;
    } else {
      data = resp->getDataPtr() + TransIdAI::HeaderLength;
      dataLen = resp->getLength() - TransIdAI::HeaderLength;
    }

    auto cols = parseAttrColumns(data, dataLen);
    if (cols.size() < numGbCols + numAggResults) {
      fprintf(stderr, "TRANSID_AI: expected %u columns, got %zu\n",
              numGbCols + numAggResults, cols.size());
      return -1;
    }
    result.found = true;
    result.gbKey = cols[0];
    for (Uint32 i = 0; i < numAggResults; i++) {
      result.aggValues.push_back(cols[numGbCols + i]);
    }
  }

  /* Wait for second TRANSID_AI (CORR_FACTOR) */
  resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(corr)");
  if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
    fprintf(stderr, "Expected second TRANSID_AI (CORR_FACTOR)\n");
    return -1;
  }

  /* Wait for CTE_LOOKUP_CONF */
  resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_CONF");
  if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_CONF) {
    fprintf(stderr, "Expected CTE_LOOKUP_CONF\n");
    return -1;
  }

  return 0;
}

/*
 * Multi-node lookup: try each node until we find the group.
 * After redistribution, each group lives on exactly one node.
 * Returns 0 on success (found or confirmed not in any node).
 */
static int
lookupAnyNode(SignalSender &ss, const std::map<Uint32, Uint32> &aggStateKeys,
              Uint32 gbAttrId, Int64 keyValue,
              Uint32 numGbCols, Uint32 numAggResults,
              LookupResult &result)
{
  Uint32 corrId = 1000 + (Uint32)keyValue;
  for (auto &kv : aggStateKeys) {
    Uint32 nodeId = kv.first;
    Uint32 aggStateKey = kv.second;
    if (lookupAndParse(ss, nodeId, 1, aggStateKey, corrId,
                        gbAttrId, keyValue, numGbCols, numAggResults,
                        result) != 0)
      return -1;
    if (result.found)
      return 0;
  }
  /* Group not found on any node */
  result.found = false;
  return 0;
}

/* ------------------------------------------------------------------ */
/* Table setup                                                         */
/* ------------------------------------------------------------------ */

/*
 * Table 1: cte_p6_simple (a BIGINT PK, b BIGINT)
 * One row per group key. 5 rows: (1,10),(2,20),(3,30),(4,40),(5,50)
 */
static int
createSimpleTable(MYSQL *conn, Ndb *ndb, int mysqlPort, TableMeta &meta)
{
  meta.name = "cte_p6_simple";
  sqlExec(conn, "DROP TABLE IF EXISTS cte_p6_simple");
  if (sqlExec(conn,
        "CREATE TABLE cte_p6_simple ("
        "  a BIGINT NOT NULL PRIMARY KEY,"
        "  b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, meta) != 0) return -1;
  if (queryFragInstances(mysqlPort, meta) != 0) return -1;

  const NdbDictionary::Table *ptab = ndb->getDictionary()->getTable(meta.name);
  meta.attrIds[0] = ptab->getColumn("a")->getAttrId();
  meta.attrIds[1] = ptab->getColumn("b")->getAttrId();
  meta.numCols = 2;

  /* Insert data */
  Int64 rows[][2] = {{1,10},{2,20},{3,30},{4,40},{5,50}};
  for (auto &r : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    NdbOperation *op = trans->getNdbOperation(ptab);
    op->insertTuple();
    op->equal("a", r[0]);
    op->setValue("b", r[1]);
    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "Insert failed: %s\n", trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }
  V("Created table %s with 5 rows\n", meta.name);
  return 0;
}

/*
 * Table 2: cte_p6_multi (id BIGINT PK, grp BIGINT, val BIGINT)
 * Multiple rows per group.
 *   grp=1: val=10,20,30 → count=3, sum=60, max=30, min=10
 *   grp=2: val=40,50     → count=2, sum=90, max=50, min=40
 *   grp=3: val=100       → count=1, sum=100, max=100, min=100
 *   grp=4: val=5,15,25,35,45 → count=5, sum=125, max=45, min=5
 */
static int
createMultiTable(MYSQL *conn, Ndb *ndb, int mysqlPort, TableMeta &meta)
{
  meta.name = "cte_p6_multi";
  sqlExec(conn, "DROP TABLE IF EXISTS cte_p6_multi");
  if (sqlExec(conn,
        "CREATE TABLE cte_p6_multi ("
        "  id BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, meta) != 0) return -1;
  if (queryFragInstances(mysqlPort, meta) != 0) return -1;

  const NdbDictionary::Table *ptab = ndb->getDictionary()->getTable(meta.name);
  meta.attrIds[0] = ptab->getColumn("id")->getAttrId();
  meta.attrIds[1] = ptab->getColumn("grp")->getAttrId();
  meta.attrIds[2] = ptab->getColumn("val")->getAttrId();
  meta.numCols = 3;

  /* id, grp, val */
  Int64 rows[][3] = {
    {1, 1, 10}, {2, 1, 20}, {3, 1, 30},
    {4, 2, 40}, {5, 2, 50},
    {6, 3, 100},
    {7, 4, 5}, {8, 4, 15}, {9, 4, 25}, {10, 4, 35}, {11, 4, 45}
  };
  for (auto &r : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    NdbOperation *op = trans->getNdbOperation(ptab);
    op->insertTuple();
    op->equal("id", r[0]);
    op->setValue("grp", r[1]);
    op->setValue("val", r[2]);
    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "Insert failed: %s\n", trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }
  V("Created table %s with 11 rows (4 groups)\n", meta.name);
  return 0;
}

/*
 * Table 3: cte_p6_many (id BIGINT PK, grp BIGINT, val BIGINT)
 * 50 groups with varying row counts for stress testing.
 * grp=g: rows with val=g*10+i for i in 1..(g%5+1)
 */
static int
createManyGroupsTable(MYSQL *conn, Ndb *ndb, int mysqlPort, TableMeta &meta)
{
  meta.name = "cte_p6_many";
  sqlExec(conn, "DROP TABLE IF EXISTS cte_p6_many");
  if (sqlExec(conn,
        "CREATE TABLE cte_p6_many ("
        "  id BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, meta) != 0) return -1;
  if (queryFragInstances(mysqlPort, meta) != 0) return -1;

  const NdbDictionary::Table *ptab = ndb->getDictionary()->getTable(meta.name);
  meta.attrIds[0] = ptab->getColumn("id")->getAttrId();
  meta.attrIds[1] = ptab->getColumn("grp")->getAttrId();
  meta.attrIds[2] = ptab->getColumn("val")->getAttrId();
  meta.numCols = 3;

  Int64 id = 1;
  for (Int64 g = 1; g <= 50; g++) {
    Int64 rowsPerGroup = (g % 5) + 1;  /* 1-5 rows per group */
    for (Int64 i = 1; i <= rowsPerGroup; i++) {
      Int64 val = g * 10 + i;
      NdbTransaction *trans = ndb->startTransaction();
      NdbOperation *op = trans->getNdbOperation(ptab);
      op->insertTuple();
      op->equal("id", id);
      op->setValue("grp", g);
      op->setValue("val", val);
      if (trans->execute(NdbTransaction::Commit) != 0) {
        fprintf(stderr, "Insert id=%lld failed: %s\n",
                (long long)id, trans->getNdbError().message);
        trans->close();
        return -1;
      }
      trans->close();
      id++;
    }
  }
  V("Created table %s with %lld rows (50 groups)\n", meta.name, (long long)(id-1));
  return 0;
}

/* ------------------------------------------------------------------ */
/* Expected values for multi-group table                               */
/* ------------------------------------------------------------------ */

struct ExpectedGroup {
  Int64 grp;
  Int64 count;
  Int64 sum;
  Int64 max_val;
  Int64 min_val;
};

static std::vector<ExpectedGroup> getMultiTableExpected()
{
  return {
    {1, 3, 60, 30, 10},
    {2, 2, 90, 50, 40},
    {3, 1, 100, 100, 100},
    {4, 5, 125, 45, 5}
  };
}

static std::vector<ExpectedGroup> getManyGroupsExpected()
{
  std::vector<ExpectedGroup> expected;
  for (Int64 g = 1; g <= 50; g++) {
    Int64 n = (g % 5) + 1;
    Int64 sum = 0, mx = INT64_MIN, mn = INT64_MAX;
    for (Int64 i = 1; i <= n; i++) {
      Int64 v = g * 10 + i;
      sum += v;
      if (v > mx) mx = v;
      if (v < mn) mn = v;
    }
    expected.push_back({g, n, sum, mx, mn});
  }
  return expected;
}

/* ------------------------------------------------------------------ */
/* Test 1: SUM data validation (single-row groups)                     */
/* ------------------------------------------------------------------ */

static int
testSumValidation(SignalSender &ss, const TableMeta &meta)
{
  printf("Test 1: SUM data validation (single-row groups)...\n");

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIds[0], meta.attrIds[1]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;

  /* Expected: GROUP BY a, SUM(b) → key=1:sum=10, key=2:sum=20, ... */
  struct { Int64 key; Int64 expectedSum; } checks[] = {
    {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}
  };

  for (auto &c : checks) {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[0], c.key,
                       1, 1, result) != 0) {
      failures++; continue;
    }
    if (!result.found) {
      fprintf(stderr, "FAIL: key=%lld not found\n", (long long)c.key);
      failures++; continue;
    }
    if (result.gbKey != c.key) {
      fprintf(stderr, "FAIL: key=%lld returned gbKey=%lld\n",
              (long long)c.key, (long long)result.gbKey);
      failures++;
    }
    if (result.aggValues[0] != c.expectedSum) {
      fprintf(stderr, "FAIL: key=%lld SUM=%lld expected %lld\n",
              (long long)c.key, (long long)result.aggValues[0],
              (long long)c.expectedSum);
      failures++;
    } else {
      V("  key=%lld → SUM=%lld OK\n",
        (long long)c.key, (long long)result.aggValues[0]);
    }
  }

  /* Key not in table → not found */
  {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[0], 999, 1, 1, result) != 0) {
      failures++;
    } else if (result.found) {
      fprintf(stderr, "FAIL: key=999 should not be found\n");
      failures++;
    } else {
      V("  key=999 → not found OK\n");
    }
  }

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: Multi-row groups (COUNT + SUM)                              */
/* ------------------------------------------------------------------ */

static int
testMultiRowGroups(SignalSender &ss, const TableMeta &meta)
{
  printf("Test 2: Multi-row groups (COUNT + SUM)...\n");

  /* GROUP BY grp (attrId[1]), COUNT+SUM of val (attrId[2]) */
  auto aggProg = buildAggProgram_CountSumGroupBy(meta.attrIds[1], meta.attrIds[2]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;
  auto expected = getMultiTableExpected();

  for (auto &e : expected) {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[1], e.grp,
                       1, 2, result) != 0) {
      failures++; continue;
    }
    if (!result.found) {
      fprintf(stderr, "FAIL: grp=%lld not found\n", (long long)e.grp);
      failures++; continue;
    }
    Int64 gotCount = result.aggValues[0];
    Int64 gotSum = result.aggValues[1];
    if (gotCount != e.count) {
      fprintf(stderr, "FAIL: grp=%lld COUNT=%lld expected %lld\n",
              (long long)e.grp, (long long)gotCount, (long long)e.count);
      failures++;
    }
    if (gotSum != e.sum) {
      fprintf(stderr, "FAIL: grp=%lld SUM=%lld expected %lld\n",
              (long long)e.grp, (long long)gotSum, (long long)e.sum);
      failures++;
    }
    V("  grp=%lld → COUNT=%lld SUM=%lld OK\n",
      (long long)e.grp, (long long)gotCount, (long long)gotSum);
  }

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: All aggregate types (COUNT+SUM+MAX+MIN)                     */
/* ------------------------------------------------------------------ */

static int
testAllAggTypes(SignalSender &ss, const TableMeta &meta)
{
  printf("Test 3: All aggregate types (COUNT+SUM+MAX+MIN)...\n");

  /* GROUP BY grp, COUNT+SUM+MAX+MIN of val */
  auto aggProg = buildAggProgram_AllAggsGroupBy(meta.attrIds[1], meta.attrIds[2]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;
  auto expected = getMultiTableExpected();

  for (auto &e : expected) {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[1], e.grp,
                       1, 4, result) != 0) {
      failures++; continue;
    }
    if (!result.found) {
      fprintf(stderr, "FAIL: grp=%lld not found\n", (long long)e.grp);
      failures++; continue;
    }
    Int64 gotCount = result.aggValues[0];
    Int64 gotSum = result.aggValues[1];
    Int64 gotMax = result.aggValues[2];
    Int64 gotMin = result.aggValues[3];
    bool ok = true;
    if (gotCount != e.count) {
      fprintf(stderr, "FAIL: grp=%lld COUNT=%lld expected %lld\n",
              (long long)e.grp, (long long)gotCount, (long long)e.count);
      ok = false;
    }
    if (gotSum != e.sum) {
      fprintf(stderr, "FAIL: grp=%lld SUM=%lld expected %lld\n",
              (long long)e.grp, (long long)gotSum, (long long)e.sum);
      ok = false;
    }
    if (gotMax != e.max_val) {
      fprintf(stderr, "FAIL: grp=%lld MAX=%lld expected %lld\n",
              (long long)e.grp, (long long)gotMax, (long long)e.max_val);
      ok = false;
    }
    if (gotMin != e.min_val) {
      fprintf(stderr, "FAIL: grp=%lld MIN=%lld expected %lld\n",
              (long long)e.grp, (long long)gotMin, (long long)e.min_val);
      ok = false;
    }
    if (!ok) failures++;
    else V("  grp=%lld → COUNT=%lld SUM=%lld MAX=%lld MIN=%lld OK\n",
           (long long)e.grp, (long long)gotCount, (long long)gotSum,
           (long long)gotMax, (long long)gotMin);
  }

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: Batch lookups — lookup all groups sequentially               */
/* ------------------------------------------------------------------ */

static int
testBatchLookups(SignalSender &ss, const TableMeta &meta)
{
  printf("Test 4: Batch lookups (all groups sequential)...\n");

  auto aggProg = buildAggProgram_CountSumGroupBy(meta.attrIds[1], meta.attrIds[2]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;
  auto expected = getMultiTableExpected();
  Uint32 foundCount = 0;

  /* Look up every group + several non-existent keys */
  std::vector<Int64> lookupKeys;
  for (auto &e : expected) lookupKeys.push_back(e.grp);
  lookupKeys.push_back(99);
  lookupKeys.push_back(100);
  lookupKeys.push_back(-1);

  for (Int64 key : lookupKeys) {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[1], key,
                       1, 2, result) != 0) {
      failures++; continue;
    }
    /* Find in expected */
    bool shouldExist = false;
    for (auto &e : expected) {
      if (e.grp == key) {
        shouldExist = true;
        if (!result.found) {
          fprintf(stderr, "FAIL: grp=%lld should exist but not found\n",
                  (long long)key);
          failures++;
        } else {
          if (result.aggValues[0] != e.count || result.aggValues[1] != e.sum) {
            fprintf(stderr, "FAIL: grp=%lld values wrong\n", (long long)key);
            failures++;
          } else {
            foundCount++;
          }
        }
        break;
      }
    }
    if (!shouldExist && result.found) {
      fprintf(stderr, "FAIL: grp=%lld should not exist but found\n",
              (long long)key);
      failures++;
    }
  }

  V("  Found %u/%zu expected groups\n", foundCount, expected.size());

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: Many groups (50 groups)                                     */
/* ------------------------------------------------------------------ */

static int
testManyGroups(SignalSender &ss, const TableMeta &meta)
{
  printf("Test 5: Many groups (50 groups, all aggs)...\n");

  auto aggProg = buildAggProgram_AllAggsGroupBy(meta.attrIds[1], meta.attrIds[2]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;
  auto expected = getManyGroupsExpected();
  Uint32 verified = 0;

  for (auto &e : expected) {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[1], e.grp,
                       1, 4, result) != 0) {
      failures++; continue;
    }
    if (!result.found) {
      fprintf(stderr, "FAIL: grp=%lld not found\n", (long long)e.grp);
      failures++; continue;
    }
    bool ok = true;
    if (result.aggValues[0] != e.count) {
      fprintf(stderr, "FAIL: grp=%lld COUNT=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[0], (long long)e.count);
      ok = false;
    }
    if (result.aggValues[1] != e.sum) {
      fprintf(stderr, "FAIL: grp=%lld SUM=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[1], (long long)e.sum);
      ok = false;
    }
    if (result.aggValues[2] != e.max_val) {
      fprintf(stderr, "FAIL: grp=%lld MAX=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[2], (long long)e.max_val);
      ok = false;
    }
    if (result.aggValues[3] != e.min_val) {
      fprintf(stderr, "FAIL: grp=%lld MIN=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[3], (long long)e.min_val);
      ok = false;
    }
    if (!ok) failures++;
    else verified++;
  }

  V("  Verified %u/%zu groups\n", verified, expected.size());

  /* Verify a non-existent group */
  {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[1], 999, 1, 4, result) != 0)
      failures++;
    else if (result.found) {
      fprintf(stderr, "FAIL: grp=999 should not exist\n");
      failures++;
    }
  }

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: Multi-node redistribution                                   */
/* ------------------------------------------------------------------ */

/*
 * On a multi-node cluster, after COMPLETE, groups are redistributed so each
 * group lives on exactly one node (its hash-owner). This test verifies:
 *   a) All groups are reachable after redistribution
 *   b) Each group is on exactly one node
 *   c) Returned values are correct
 *
 * Skipped on single-node clusters (redistribution is a no-op there).
 */
static int
testMultiNodeRedist(SignalSender &ss, const TableMeta &meta)
{
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  if (uniqueNodes.size() < 2) {
    printf("Test 6: Multi-node redistribution... SKIP (single node)\n");
    return 0;
  }
  printf("Test 6: Multi-node redistribution (%zu nodes)...\n",
         uniqueNodes.size());

  auto aggProg = buildAggProgram_CountSumGroupBy(meta.attrIds[1], meta.attrIds[2]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;
  auto expected = getMultiTableExpected();

  for (auto &e : expected) {
    /* Try every node — exactly one should have the group */
    Uint32 foundOnNodes = 0;
    Int64 gotCount = 0, gotSum = 0;

    for (auto &kv : aggKeys) {
      Uint32 nodeId = kv.first;
      Uint32 aggStateKey = kv.second;
      LookupResult result;
      Uint32 corrId = 6000 + (Uint32)e.grp * 100 + nodeId;
      if (lookupAndParse(ss, nodeId, 1, aggStateKey, corrId,
                          meta.attrIds[1], e.grp, 1, 2, result,
                          false /* no routing — test redistribution */) != 0) {
        failures++;
        continue;
      }
      if (result.found) {
        foundOnNodes++;
        gotCount = result.aggValues[0];
        gotSum = result.aggValues[1];
        V("  grp=%lld found on node %u\n", (long long)e.grp, nodeId);
      }
    }

    if (foundOnNodes == 0) {
      fprintf(stderr, "FAIL: grp=%lld not found on any node\n",
              (long long)e.grp);
      failures++;
    } else if (foundOnNodes > 1) {
      fprintf(stderr, "FAIL: grp=%lld found on %u nodes (expected 1)\n",
              (long long)e.grp, foundOnNodes);
      failures++;
    } else {
      /* Verify values */
      if (gotCount != e.count) {
        fprintf(stderr, "FAIL: grp=%lld COUNT=%lld expected %lld\n",
                (long long)e.grp, (long long)gotCount, (long long)e.count);
        failures++;
      }
      if (gotSum != e.sum) {
        fprintf(stderr, "FAIL: grp=%lld SUM=%lld expected %lld\n",
                (long long)e.grp, (long long)gotSum, (long long)e.sum);
        failures++;
      }
    }
  }

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 7: Multi-node all aggregates                                   */
/* ------------------------------------------------------------------ */

static int
testMultiNodeAllAggs(SignalSender &ss, const TableMeta &meta)
{
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  if (uniqueNodes.size() < 2) {
    printf("Test 7: Multi-node all aggregates... SKIP (single node)\n");
    return 0;
  }
  printf("Test 7: Multi-node all aggregates (%zu nodes, 50 groups)...\n",
         uniqueNodes.size());

  auto aggProg = buildAggProgram_AllAggsGroupBy(meta.attrIds[1], meta.attrIds[2]);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, aggKeys) != 0) return -1;

  int failures = 0;
  auto expected = getManyGroupsExpected();
  Uint32 verified = 0;

  for (auto &e : expected) {
    LookupResult result;
    if (lookupAnyNode(ss, aggKeys, meta.attrIds[1], e.grp,
                       1, 4, result) != 0) {
      failures++; continue;
    }
    if (!result.found) {
      fprintf(stderr, "FAIL: grp=%lld not found on any node\n",
              (long long)e.grp);
      failures++; continue;
    }
    bool ok = true;
    if (result.aggValues[0] != e.count) {
      fprintf(stderr, "FAIL: grp=%lld COUNT=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[0], (long long)e.count);
      ok = false;
    }
    if (result.aggValues[1] != e.sum) {
      fprintf(stderr, "FAIL: grp=%lld SUM=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[1], (long long)e.sum);
      ok = false;
    }
    if (result.aggValues[2] != e.max_val) {
      fprintf(stderr, "FAIL: grp=%lld MAX=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[2], (long long)e.max_val);
      ok = false;
    }
    if (result.aggValues[3] != e.min_val) {
      fprintf(stderr, "FAIL: grp=%lld MIN=%lld expected %lld\n",
              (long long)e.grp, (long long)result.aggValues[3], (long long)e.min_val);
      ok = false;
    }
    if (!ok) failures++;
    else verified++;
  }

  V("  Verified %u/%zu groups across %zu nodes\n",
    verified, expected.size(), uniqueNodes.size());

  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

static const char *connectString = "localhost:1186";
static int mysqlPort = 3306;

int main(int argc, char **argv)
{
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
      connectString = argv[++i];
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
      mysqlPort = atoi(argv[++i]);
    else if (strcmp(argv[i], "-v") == 0)
      verbose = true;
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: testCtePhase6 -c <connect_string> -m <mysql_port> [-v]\n");
      return 0;
    }
  }

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int result = 0;

  do {
    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) { result = 1; break; }

    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      mysql_close(conn); result = 1; break;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30 seconds\n");
      mysql_close(conn); result = 1; break;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
      mysql_close(conn); result = 1; break;
    }

    /* Create test tables */
    TableMeta simpleMeta, multiMeta, manyMeta;
    if (createSimpleTable(conn, &ndb, mysqlPort, simpleMeta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (createMultiTable(conn, &ndb, mysqlPort, multiMeta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (createManyGroupsTable(conn, &ndb, mysqlPort, manyMeta) != 0) {
      mysql_close(conn); result = 1; break;
    }

    {
      SignalSender ss(&con);
      ss.lock();
      V("SignalSender: ref=0x%08x\n", ss.getOwnRef());

      /* Tests 1-5: work on any cluster size */
      if (testSumValidation(ss, simpleMeta) != 0) result = 1;
      if (testMultiRowGroups(ss, multiMeta) != 0) result = 1;
      if (testAllAggTypes(ss, multiMeta) != 0) result = 1;
      if (testBatchLookups(ss, multiMeta) != 0) result = 1;
      if (testManyGroups(ss, manyMeta) != 0) result = 1;

      /* Tests 6-7: multi-node specific (skip on single node) */
      if (testMultiNodeRedist(ss, multiMeta) != 0) result = 1;
      if (testMultiNodeAllAggs(ss, manyMeta) != 0) result = 1;

      ss.unlock();
    }

    /* Cleanup */
    sqlExec(conn, "DROP TABLE IF EXISTS cte_p6_simple");
    sqlExec(conn, "DROP TABLE IF EXISTS cte_p6_multi");
    sqlExec(conn, "DROP TABLE IF EXISTS cte_p6_many");
    mysql_close(conn);
  } while (false);

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
