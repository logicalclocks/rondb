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
 * testCteLookup — Unit test for CTE_LOOKUP_REQ handling in DBLQH.
 *
 * Tests the CTE hash table lookup path:
 * 1. JOIN_AGG_SETUP_REQ with CTE_MODE_FLAG → hash table created
 * 2. SCAN_FRAGREQ feeds rows into aggregation
 * 3. JOIN_AGG_COMPLETE_REQ → CTE_READY (no TRANSID_AI sent)
 * 4. CTE_LOOKUP_REQ → TRANSID_AI with column data + CTE_LOOKUP_CONF
 * 5. CTE_LOOKUP_REQ for missing key → CTE_LOOKUP_REF
 * 6. JOIN_AGG_RELEASE_REQ → cleanup
 *
 * Usage: testCteLookup -c <connect_string> -m <mysql_port> [-v]
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
#include <map>
#include <set>
#include <vector>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *TABLE_NAME = "cte_test";
static const Uint32 FAKE_TRANS_ID1 = 0xCCEE0001;
static const Uint32 FAKE_TRANS_ID2 = 0xCCEE0002;
static const Uint32 FAKE_REQUEST_ID = 2001;
static const Uint32 FAKE_SENDER_DATA = 77;
static const Uint32 WAIT_TIMEOUT_MS = 30000;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 INTERPRETER_EXIT_OK = 18;
static const Uint32 AGG_MAGIC = 0x0721;

/* ------------------------------------------------------------------ */
/* Aggregation program builder                                         */
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
buildAggProgram_CountGroupBy(Uint32 gbColId, Uint32 countColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | countColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;
  return prog;
}

static std::vector<Uint32>
buildAggProgram_CountSumGroupBy(Uint32 gbColId, Uint32 sumColId)
{
  const Uint32 PROG_LEN = 13;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 2u;  /* 1 GB col, 2 agg results */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | sumColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;  /* COUNT → agg[0] */
  prog[11] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | sumColId;
  prog[12] = (kOpSum << 26) | (0 << 16) | 1;    /* SUM → agg[1] */
  return prog;
}

/* ------------------------------------------------------------------ */
/* AttrInfo builder for scan (minimal)                                 */
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

/* ------------------------------------------------------------------ */
/* AttrInfo builder for CTE_LOOKUP_REQ                                 */
/* ------------------------------------------------------------------ */

static std::vector<Uint32>
buildCteLookupAttrInfo(Uint32 numGbCols, Uint32 numAggResults,
                       Uint32 resultRef, Uint32 resultData, Uint32 routeRef)
{
  std::vector<Uint32> ai;
  /* 5-word header */
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
  /* Virtual columns: 0..numGbCols-1 (GB keys), numGbCols..N-1 (agg results) */
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
/* Lookup key builder (AttributeHeader-encoded BIGINT)                 */
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
/* Table setup via MySQL                                               */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 attrIdA;
  Uint32 attrIdB;
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
  std::vector<Uint32> fragInstances;
};

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
  dict->invalidateTable(TABLE_NAME);
  const NdbDictionary::Table *ptab = dict->getTable(TABLE_NAME);
  if (ptab == nullptr) {
    fprintf(stderr, "getTable(%s) failed: %s\n",
            TABLE_NAME, dict->getNdbError().message);
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

static int
createTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_test");
  if (sqlExec(conn,
        "CREATE TABLE cte_test ("
        "  a BIGINT NOT NULL PRIMARY KEY,"
        "  b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;
  if (getTableMeta(ndb, meta) != 0) return -1;
  V("Table '%s': id=%u version=%u attrA=%u attrB=%u frags=%u\n",
    TABLE_NAME, meta.tableId, meta.schemaVersion,
    meta.attrIdA, meta.attrIdB, meta.fragCount);
  return 0;
}

static void dropTable(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_test");
}

static int
insertTestData(Ndb *ndb)
{
  const NdbDictionary::Table *ptab = ndb->getDictionary()->getTable(TABLE_NAME);
  if (ptab == nullptr) return -1;

  /* 5 rows: (1,10), (2,20), (3,30), (4,40), (5,50) */
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
  V("Inserted 5 rows into %s\n", TABLE_NAME);
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
/* SETUP / SCAN / COMPLETE / RELEASE helpers                           */
/* ------------------------------------------------------------------ */

static int
sendSetupReq(SignalSender &ss, Uint32 nodeId,
             const std::vector<Uint32> &aggProg, const TableMeta &meta,
             Uint32 &aggStateKeyOut)
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
  /* CTE mode: MUTEX_BASED + CTE_MODE_FLAG */
  req->concurrencyStrategy =
      JoinAggSetupReq::STRATEGY_MUTEX_BASED | JoinAggSetupReq::CTE_MODE_FLAG;
  req->resultRef = ss.getOwnRef();
  req->resultData = FAKE_SENDER_DATA;
  req->routeRef = ss.getOwnRef();
  req->cteIndex = RNIL;

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_SETUP_REQ,
           JoinAggSetupReq::SignalLength);
  ssig.header.m_noOfSections = 2;
  ssig.ptr[0].p = const_cast<Uint32*>(aggProg.data());
  ssig.ptr[0].sz = (Uint32)aggProg.size();
  ssig.ptr[1].p = &receiverId;
  ssig.ptr[1].sz = 1;

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
    V("  SETUP_CONF: aggStateKey=%u\n", aggStateKeyOut);
    return 0;
  }
  fprintf(stderr, "Expected SETUP_CONF, got GSN=%d\n", getGsn(resp));
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
  scanReq->batch_size_rows = 100;
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
sendCompleteReq(SignalSender &ss, Uint32 nodeId, Uint32 aggStateKey)
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

  Uint16 recBlock = numberToBlock(DBLQH, 1);
  ssig.set(ss, 0, recBlock, GSN_JOIN_AGG_COMPLETE_REQ,
           JoinAggCompleteReq::SignalLength);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal COMPLETE_REQ failed\n");
    return -1;
  }
  return 0;
}

/* Wait for COMPLETE_CONF — in CTE mode it should arrive immediately
 * with no TRANSID_AI signals before it. */
static int
waitForCompleteConf(SignalSender &ss)
{
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "COMPLETE_CONF");
  if (resp == nullptr) return -1;
  int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
    const JoinAggCompleteConf *conf =
      reinterpret_cast<const JoinAggCompleteConf *>(resp->getDataPtr());
    V("  COMPLETE_CONF: rows=%u bytes=%u\n",
      conf->numResultRows, conf->resultBytes);
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

/* ------------------------------------------------------------------ */
/* CTE_LOOKUP_REQ sender                                               */
/* ------------------------------------------------------------------ */

static int
sendCteLookupReq(SignalSender &ss, Uint32 nodeId, Uint32 ldmInst,
                 Uint32 aggStateKey, Uint32 correlationId,
                 const Uint32 *keyBuf, Uint32 keySizeWords, Uint32 keyLenBytes,
                 const std::vector<Uint32> &attrInfo)
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
/* Common: setup + scan + complete lifecycle for CTE mode               */
/* ------------------------------------------------------------------ */

static int
setupScanComplete(SignalSender &ss, const TableMeta &meta,
                  const std::vector<Uint32> &aggProg,
                  int /*mysqlPort*/,
                  std::map<Uint32, Uint32> &aggStateKeys)
{
  /* Collect unique data nodes */
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Setup on all nodes */
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta, key) != 0) return -1;
    aggStateKeys[nd] = key;
  }

  /* Scan all fragments */
  auto scanAI = buildScanAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nd = meta.fragNodes[f];
    Uint32 inst = meta.fragInstances[f];
    if (sendScanFragReq(ss, nd, f, inst, aggStateKeys[nd], meta, scanAI) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, rows) != 0) return -1;
  }

  /* Complete on all nodes (CTE mode: no TRANSID_AI) */
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
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
/* Test 1: Basic CTE lookup (GROUP BY + SUM)                           */
/* ------------------------------------------------------------------ */

static int
testBasicLookup(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
                int mysqlPort)
{
  printf("Test 1: Basic CTE lookup (SUM GROUP BY)...\n");

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, mysqlPort, aggKeys) != 0) return -1;

  /* Pick first node for lookups */
  Uint32 nodeId = aggKeys.begin()->first;
  Uint32 aggStateKey = aggKeys.begin()->second;

  auto attrInfo = buildCteLookupAttrInfo(1, 1, ss.getOwnRef(),
                                          FAKE_SENDER_DATA, ss.getOwnRef());
  int failures = 0;

  /* Lookup key=1 → expect SUM(b)=10 */
  {
    Uint32 keyBuf[3];
    Uint32 keyWords, keyBytes;
    buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 1);

    if (sendCteLookupReq(ss, nodeId, 1, aggStateKey, 100,
                          keyBuf, keyWords, keyBytes, attrInfo) != 0) {
      failures++; goto cleanup;
    }

    /* Expect: TRANSID_AI (FLUSH_AI columns), TRANSID_AI (CORR_FACTOR), CONF */
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(1)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected first TRANSID_AI for key=1\n");
      failures++;
    } else {
      V("  Got TRANSID_AI(1) for key=1 (FLUSH_AI columns)\n");
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(2)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected second TRANSID_AI for key=1\n");
      failures++;
    } else {
      V("  Got TRANSID_AI(2) for key=1 (CORR_FACTOR)\n");
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_CONF");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_CONF) {
      fprintf(stderr, "FAIL: expected CTE_LOOKUP_CONF\n");
      failures++;
    } else {
      V("  Got CTE_LOOKUP_CONF for key=1\n");
    }
  }

  /* Lookup key=999 → expect REF (not found) */
  {
    Uint32 keyBuf[3];
    Uint32 keyWords, keyBytes;
    buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 999);

    if (sendCteLookupReq(ss, nodeId, 1, aggStateKey, 101,
                          keyBuf, keyWords, keyBytes, attrInfo) != 0) {
      failures++; goto cleanup;
    }

    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_REF");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_REF) {
      fprintf(stderr, "FAIL: expected CTE_LOOKUP_REF for key=999\n");
      failures++;
    } else {
      V("  Got CTE_LOOKUP_REF for key=999 (not found)\n");
    }
  }

cleanup:
  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: CTE lookup with COUNT(*)                                    */
/* ------------------------------------------------------------------ */

static int
testCountLookup(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
                int mysqlPort)
{
  printf("Test 2: CTE lookup (COUNT GROUP BY)...\n");

  auto aggProg = buildAggProgram_CountGroupBy(meta.attrIdA, meta.attrIdB);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, mysqlPort, aggKeys) != 0) return -1;

  Uint32 nodeId = aggKeys.begin()->first;
  Uint32 aggStateKey = aggKeys.begin()->second;
  auto attrInfo = buildCteLookupAttrInfo(1, 1, ss.getOwnRef(),
                                          FAKE_SENDER_DATA, ss.getOwnRef());
  int failures = 0;

  Uint32 keyBuf[3];
  Uint32 keyWords, keyBytes;
  buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 2);

  if (sendCteLookupReq(ss, nodeId, 1, aggStateKey, 200,
                        keyBuf, keyWords, keyBytes, attrInfo) != 0) {
    failures++; goto cleanup;
  }

  {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(1)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected first TRANSID_AI for COUNT key=2\n");
      failures++;
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(2)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected second TRANSID_AI for COUNT key=2\n");
      failures++;
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_CONF");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_CONF) {
      fprintf(stderr, "FAIL: expected CTE_LOOKUP_CONF\n");
      failures++;
    }
  }

cleanup:
  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: Multiple aggregates (COUNT + SUM)                           */
/* ------------------------------------------------------------------ */

static int
testMultiAgg(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
             int mysqlPort)
{
  printf("Test 3: CTE lookup (COUNT + SUM GROUP BY)...\n");

  auto aggProg = buildAggProgram_CountSumGroupBy(meta.attrIdA, meta.attrIdB);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, mysqlPort, aggKeys) != 0) return -1;

  Uint32 nodeId = aggKeys.begin()->first;
  Uint32 aggStateKey = aggKeys.begin()->second;
  auto attrInfo = buildCteLookupAttrInfo(1, 2, ss.getOwnRef(),
                                          FAKE_SENDER_DATA, ss.getOwnRef());
  int failures = 0;

  Uint32 keyBuf[3];
  Uint32 keyWords, keyBytes;
  buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 5);

  if (sendCteLookupReq(ss, nodeId, 1, aggStateKey, 300,
                        keyBuf, keyWords, keyBytes, attrInfo) != 0) {
    failures++; goto cleanup;
  }

  {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(1)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected first TRANSID_AI for multi-agg key=5\n");
      failures++;
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(2)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected second TRANSID_AI for multi-agg key=5\n");
      failures++;
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_CONF");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_CONF) {
      fprintf(stderr, "FAIL: expected CTE_LOOKUP_CONF\n");
      failures++;
    }
  }

cleanup:
  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: CTE mode — no TRANSID_AI on COMPLETE                       */
/* ------------------------------------------------------------------ */

static int
testCteModeSilentComplete(Ndb * /*ndb*/, SignalSender &ss,
                          const TableMeta &meta, int mysqlPort)
{
  printf("Test 4: CTE mode — silent COMPLETE (no TRANSID_AI)...\n");

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);
  std::map<Uint32, Uint32> aggKeys;
  /* setupScanComplete already validates no TRANSID_AI before COMPLETE_CONF */
  if (setupScanComplete(ss, meta, aggProg, mysqlPort, aggKeys) != 0) {
    fprintf(stderr, "  FAIL\n");
    releaseAll(ss, aggKeys);
    return -1;
  }

  /* Verify hash table survives: do a lookup */
  Uint32 nodeId = aggKeys.begin()->first;
  Uint32 aggStateKey = aggKeys.begin()->second;
  auto attrInfo = buildCteLookupAttrInfo(1, 1, ss.getOwnRef(),
                                          FAKE_SENDER_DATA, ss.getOwnRef());
  Uint32 keyBuf[3];
  Uint32 keyWords, keyBytes;
  buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 3);

  int failures = 0;
  if (sendCteLookupReq(ss, nodeId, 1, aggStateKey, 400,
                        keyBuf, keyWords, keyBytes, attrInfo) != 0) {
    failures++; goto cleanup;
  }

  {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(1)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: hash table not alive after CTE COMPLETE\n");
      failures++;
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(2)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected second TRANSID_AI after CTE COMPLETE\n");
      failures++;
    }
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_CONF");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_CONF) {
      failures++;
    }
  }

cleanup:
  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: FLUSH_AI routing                                            */
/* ------------------------------------------------------------------ */

static int
testFlushAIRouting(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
                   int mysqlPort)
{
  printf("Test 5: FLUSH_AI routing...\n");

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);
  std::map<Uint32, Uint32> aggKeys;
  if (setupScanComplete(ss, meta, aggProg, mysqlPort, aggKeys) != 0) return -1;

  Uint32 nodeId = aggKeys.begin()->first;
  Uint32 aggStateKey = aggKeys.begin()->second;

  /* Build AttrInfo with FLUSH_AI directing output to a specific resultData.
   * After FLUSH_AI, CORR_FACTOR goes to the remaining output which is
   * sent to the original senderRef with senderData as connectPtr.
   *
   * Layout: [col 0] [col 1] [FLUSH_AI → resultData=88] [CORR_FACTOR32]
   */
  const Uint32 FLUSH_RESULT_DATA = 88;
  auto attrInfo = buildCteLookupAttrInfo(1, 1, ss.getOwnRef(),
                                          FLUSH_RESULT_DATA, ss.getOwnRef());
  int failures = 0;
  Uint32 keyBuf[3];
  Uint32 keyWords, keyBytes;
  buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 4);

  const Uint32 CORR_ID = 500;
  if (sendCteLookupReq(ss, nodeId, 1, aggStateKey, CORR_ID,
                        keyBuf, keyWords, keyBytes, attrInfo) != 0) {
    failures++; goto cleanup;
  }

  {
    /* First TRANSID_AI: from FLUSH_AI, connectPtr should be FLUSH_RESULT_DATA */
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(flush)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected TRANSID_AI from FLUSH_AI\n");
      failures++;
    } else {
      const TransIdAI *tai =
        reinterpret_cast<const TransIdAI *>(resp->getDataPtr());
      if (tai->connectPtr != FLUSH_RESULT_DATA) {
        fprintf(stderr, "FAIL: FLUSH_AI TRANSID_AI connectPtr=%u, expected %u\n",
                tai->connectPtr, FLUSH_RESULT_DATA);
        failures++;
      } else {
        V("  FLUSH_AI TRANSID_AI: connectPtr=%u (correct)\n", tai->connectPtr);
      }
    }

    /* Second TRANSID_AI: remaining output (CORR_FACTOR), connectPtr=senderData */
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "TRANSID_AI(corr)");
    if (resp == nullptr || getGsn(resp) != GSN_TRANSID_AI) {
      fprintf(stderr, "FAIL: expected second TRANSID_AI with CORR_FACTOR\n");
      failures++;
    } else {
      const TransIdAI *tai =
        reinterpret_cast<const TransIdAI *>(resp->getDataPtr());
      if (tai->connectPtr != CORR_ID) {
        fprintf(stderr, "FAIL: CORR TRANSID_AI connectPtr=%u, expected %u\n",
                tai->connectPtr, CORR_ID);
        failures++;
      } else {
        V("  CORR TRANSID_AI: connectPtr=%u (correct)\n", tai->connectPtr);
      }
    }

    /* CTE_LOOKUP_CONF */
    resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "CTE_LOOKUP_CONF");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_CONF) {
      fprintf(stderr, "FAIL: expected CTE_LOOKUP_CONF\n");
      failures++;
    }
  }

cleanup:
  releaseAll(ss, aggKeys);
  if (failures == 0) printf("  PASS\n");
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: Error cases                                                 */
/* ------------------------------------------------------------------ */

static int
testErrorCases(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
               int mysqlPort)
{
  printf("Test 6: Error cases...\n");
  int failures = 0;

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);
  auto attrInfo = buildCteLookupAttrInfo(1, 1, ss.getOwnRef(),
                                          FAKE_SENDER_DATA, ss.getOwnRef());
  Uint32 keyBuf[3];
  Uint32 keyWords, keyBytes;
  buildBigintKey(keyBuf, keyWords, keyBytes, meta.attrIdA, 1);

  Uint32 nodeId = meta.fragNodes[0];

  /* 6a: Invalid aggStateKey */
  {
    V("  6a: Invalid aggStateKey...\n");
    if (sendCteLookupReq(ss, nodeId, 1, 0xFFFFFF, 600,
                          keyBuf, keyWords, keyBytes, attrInfo) != 0) {
      failures++; goto done;
    }
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "REF(invalid key)");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_REF) {
      fprintf(stderr, "FAIL 6a: expected CTE_LOOKUP_REF\n");
      failures++;
    } else {
      V("    Got CTE_LOOKUP_REF (correct)\n");
    }
  }

  /* 6b: Malformed AttrInfo (too short) */
  {
    V("  6b: Malformed AttrInfo...\n");
    std::map<Uint32, Uint32> aggKeys;
    if (setupScanComplete(ss, meta, aggProg, mysqlPort, aggKeys) != 0) {
      failures++; goto done;
    }
    Uint32 stateKey = aggKeys.begin()->second;

    /* AttrInfo with only 3 words (minimum is 8) */
    std::vector<Uint32> badAI = {0, 1, 0};
    if (sendCteLookupReq(ss, nodeId, 1, stateKey, 601,
                          keyBuf, keyWords, keyBytes, badAI) != 0) {
      failures++;
      releaseAll(ss, aggKeys);
      goto done;
    }
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "REF(malformed)");
    if (resp == nullptr || getGsn(resp) != GSN_CTE_LOOKUP_REF) {
      fprintf(stderr, "FAIL 6b: expected CTE_LOOKUP_REF for malformed AttrInfo\n");
      failures++;
    } else {
      V("    Got CTE_LOOKUP_REF for malformed AttrInfo (correct)\n");
    }
    releaseAll(ss, aggKeys);
  }

done:
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
  /* Parse args */
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
      connectString = argv[++i];
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
      mysqlPort = atoi(argv[++i]);
    else if (strcmp(argv[i], "-v") == 0)
      verbose = true;
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: testCteLookup -c <connect_string> -m <mysql_port> [-v]\n");
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

    TableMeta meta;
    if (createTable(conn, &ndb, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (insertTestData(&ndb) != 0) {
      mysql_close(conn); result = 1; break;
    }

    {
      SignalSender ss(&con);
      ss.lock();
      V("SignalSender: ref=0x%08x\n", ss.getOwnRef());

      if (testBasicLookup(&ndb, ss, meta, mysqlPort) != 0) result = 1;
      if (testCountLookup(&ndb, ss, meta, mysqlPort) != 0) result = 1;
      if (testMultiAgg(&ndb, ss, meta, mysqlPort) != 0) result = 1;
      if (testCteModeSilentComplete(&ndb, ss, meta, mysqlPort) != 0) result = 1;
      if (testFlushAIRouting(&ndb, ss, meta, mysqlPort) != 0) result = 1;
      if (testErrorCases(&ndb, ss, meta, mysqlPort) != 0) result = 1;

      ss.unlock();
    }

    dropTable(conn);
    mysql_close(conn);
  } while (false);

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
