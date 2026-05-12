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
 * testStarJoinAgg — Unit test for multi-leaf star schema aggregation
 * at the DBLQH level using SignalSender.
 *
 * Tests the multi-leaf format in JOIN_AGG_SETUP_REQ Section 0:
 *   [numLeaves, progLen0, prog0..., progLen1, prog1...]
 *
 * And the leaf-index-encoded aggStateKey in SCAN_FRAGREQ:
 *   encodedKey = (leafIndex << 24) | baseKey
 *
 * Uses a single table scanned multiple times with different leaf indices.
 *
 * Usage: testStarJoinAgg -c <connect_string> -m <mysql_port> [--verbose]
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
#include <ndbapi/NdbAggregationCommon.hpp>

#include <kernel/signaldata/LqhKey.hpp>

#include <NdbRestarter.hpp>
#include <util/rondb_hash.hpp>
#include <kernel/signaldata/DumpStateOrd.hpp>
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

static MYSQL *g_mysql_conn = nullptr;
static const char *TABLE_NAME = "star_ml_test";
static const Uint32 FAKE_TRANS_ID1 = 0x12345678;
static const Uint32 FAKE_TRANS_ID2 = 0x87654321;
static const Uint32 FAKE_REQUEST_ID = 1001;
static const Uint32 FAKE_SENDER_DATA = 42;
static const Uint32 WAIT_TIMEOUT_MS = 30000;

/* NDB column type for Bigint (NDB_TYPE_BIGINT = 9 in ndb_constants.h) */
static const Uint32 COL_TYPE_BIGINT = 9;

/* Interpreter ExitOK instruction */
static const Uint32 INTERPRETER_EXIT_OK = 18;

/* Aggregation program magic */
static const Uint32 AGG_MAGIC = 0x0721;

/* AttributeHeader::AGG_RESULT */
static const Uint32 AGG_RESULT_ATTR = 0xFF00;

/* ------------------------------------------------------------------ */
/* Aggregation program builders                                        */
/* ------------------------------------------------------------------ */

/*
 * Build aggregation program for:
 *   SELECT SUM(sumCol) FROM t GROUP BY gbCol
 *
 * PROG_LEN=11: header(8) + gb_col(1) + LoadCol(1) + Sum(1)
 */
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

/*
 * Build aggregation program for:
 *   SELECT COUNT(countCol) FROM t GROUP BY gbCol
 *
 * PROG_LEN=11: header(8) + gb_col(1) + LoadCol(1) + Count(1)
 */
static std::vector<Uint32>
buildAggProgram_CountGroupBy(Uint32 gbColId, Uint32 countColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;  /* n_gb_cols=1, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             countColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;

  return prog;
}

/*
 * Build aggregation program for:
 *   SELECT SUM(colId) FROM t   (no GROUP BY)
 *
 * PROG_LEN=10: header(8) + LoadCol(1) + Sum(1)
 */
static std::vector<Uint32>
buildAggProgram_SumNoGroupBy(Uint32 colId)
{
  const Uint32 PROG_LEN = 10;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 1u;  /* n_gb_cols=0, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | colId;
  prog[9] = (kOpSum << 26) | (0 << 16) | 0;

  return prog;
}

/*
 * Build aggregation program for:
 *   SELECT COUNT(colId) FROM t   (no GROUP BY)
 *
 * PROG_LEN=10: header(8) + LoadCol(1) + Count(1)
 */
static std::vector<Uint32>
buildAggProgram_CountNoGroupBy(Uint32 colId)
{
  const Uint32 PROG_LEN = 10;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 1u;  /* n_gb_cols=0, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | colId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;

  return prog;
}

/* ------------------------------------------------------------------ */
/* Multi-leaf section builder                                          */
/* ------------------------------------------------------------------ */

/*
 * Build the multi-leaf aggregation program section for Section 0 of
 * JOIN_AGG_SETUP_REQ.
 *
 * Format: [numLeaves, progLen0, prog0..., progLen1, prog1...]
 */
static std::vector<Uint32>
buildMultiLeafSection(const std::vector<std::vector<Uint32>> &programs)
{
  Uint32 numLeaves = (Uint32)programs.size();

  /* Calculate total size: 1 (numLeaves) + sum(1 + progLen_i) */
  Uint32 totalSize = 1;
  for (const auto &prog : programs) {
    totalSize += 1 + (Uint32)prog.size();
  }

  std::vector<Uint32> section(totalSize);
  section[0] = (0x0722 << 16) | numLeaves;

  Uint32 pos = 1;
  for (const auto &prog : programs) {
    section[pos++] = (Uint32)prog.size();
    for (Uint32 w = 0; w < (Uint32)prog.size(); w++) {
      section[pos++] = prog[w];
    }
  }

  return section;
}

/* ------------------------------------------------------------------ */
/* AttrInfo section builder for SCAN_FRAGREQ                          */
/* ------------------------------------------------------------------ */

static std::vector<Uint32>
buildAttrInfo()
{
  std::vector<Uint32> ai(6);
  ai[0] = 0;  /* initial read section length */
  ai[1] = 1;  /* interpreter program length (1 word) */
  ai[2] = 0;  /* interpreter subroutine length */
  ai[3] = 0;  /* final read section length */
  ai[4] = 0;  /* linked attr section length (RsubLen) */
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
  Uint32 attrIdC;       /* for 3-column tables (0 if unused) */
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;      /* primary node for each fragment */
  std::vector<Uint32> fragInstances;  /* LDM instance for each fragment */
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

  meta.fragNodes.resize(meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nodeId = 0;
    ptab->getFragmentNodes(f, &nodeId, 1);
    meta.fragNodes[f] = nodeId;
  }
  return 0;
}

static int
createTestTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS star_ml_test");
  if (sqlExec(conn,
        "CREATE TABLE star_ml_test ("
        "  a BIGINT NOT NULL PRIMARY KEY,"
        "  b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, TABLE_NAME, meta, "a", "b") != 0) return -1;

  V("Table '%s': id=%u version=%u attrA=%u attrB=%u frags=%u\n",
         TABLE_NAME, meta.tableId, meta.schemaVersion,
         meta.attrIdA, meta.attrIdB, meta.fragCount);
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    V("  fragment %u -> node %u\n", f, meta.fragNodes[f]);
  }

  return 0;
}

static int
queryFragInstances(int mysqlPort, TableMeta &meta)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) {
    fprintf(stderr, "mysql_init failed\n");
    return -1;
  }

  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         "ndbinfo", mysqlPort, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect failed: %s\n", mysql_error(conn));
    mysql_close(conn);
    return -1;
  }

  char query[256];
  snprintf(query, sizeof(query),
           "SELECT node_id, fragment_num, block_instance "
           "FROM ndbinfo.operations_per_fragment "
           "WHERE table_id = %u "
           "ORDER BY node_id, fragment_num",
           meta.tableId);

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

  std::map<std::pair<Uint32,Uint32>, Uint32> instMap;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result)) != nullptr) {
    Uint32 nodeId = (Uint32)atoi(row[0]);
    Uint32 fragNum = (Uint32)atoi(row[1]);
    Uint32 blockInst = (Uint32)atoi(row[2]);
    instMap[{nodeId, fragNum}] = blockInst;
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
    V("  fragment %u -> node %u, LDM instance %u\n",
           f, meta.fragNodes[f], meta.fragInstances[f]);
  }

  return 0;
}

static int
insertTestData(Ndb *ndb)
{
  struct Row { Int64 a; Int64 b; };
  static const Row rows[] = {
    {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}
  };

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *ptab = dict->getTable(TABLE_NAME);
  if (ptab == nullptr) return -1;

  for (const auto &row : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) {
      fprintf(stderr, "startTransaction: %s\n",
              ndb->getNdbError().message);
      return -1;
    }

    NdbOperation *op = trans->getNdbOperation(ptab);
    if (op == nullptr) {
      fprintf(stderr, "getNdbOperation: %s\n",
              trans->getNdbError().message);
      trans->close();
      return -1;
    }

    op->insertTuple();
    op->equal("a", row.a);
    op->setValue("b", row.b);

    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert(%lld,%lld) failed: %s\n",
              (long long)row.a, (long long)row.b,
              trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }

  V("Inserted %zu rows into %s\n",
    sizeof(rows)/sizeof(rows[0]), TABLE_NAME);
  return 0;
}

static int
dropTestTable(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS star_ml_test");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Signal helpers                                                      */
/* ------------------------------------------------------------------ */

static SimpleSignal *
waitForSignal(SignalSender &ss, Uint32 timeoutMs, const char *context)
{
  SimpleSignal *sig = ss.waitFor(timeoutMs);
  if (sig == nullptr) {
    fprintf(stderr, "TIMEOUT waiting for signal (%s)\n", context);
  }
  return sig;
}

static int
getGsn(const SimpleSignal *sig)
{
  return sig->header.readSignalNumber();
}

/* ------------------------------------------------------------------ */
/* JOIN_AGG_SETUP_REQ — multi-leaf format                              */
/* ------------------------------------------------------------------ */

/*
 * Send JOIN_AGG_SETUP_REQ with multi-leaf section format.
 * Section 0 contains: [numLeaves, progLen0, prog0..., progLen1, prog1...]
 * Section 1 contains: receiverId
 */
static int
sendMultiLeafSetupReq(SignalSender &ss, Uint32 nodeId,
                      const std::vector<Uint32> &multiLeafSection,
                      const TableMeta &meta,
                      Uint32 strategy,
                      Uint32 &aggStateKeyOut,
                      Uint32 &ownerInstanceOut)
{
  V("\n--- JOIN_AGG_SETUP_REQ (multi-leaf) -> node %u ---\n", nodeId);

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
  req->concurrencyStrategy = strategy;
  req->resultRef = ss.getOwnRef();
  req->resultData = FAKE_SENDER_DATA;
  req->routeRef = ss.getOwnRef();
  req->cteIndex = RNIL;

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_SETUP_REQ,
           JoinAggSetupReq::SignalLength);
  Uint32 receiverId = FAKE_SENDER_DATA;
  ssig.header.m_noOfSections = 2;
  ssig.ptr[0].p = multiLeafSection.data();
  ssig.ptr[0].sz = (Uint32)multiLeafSection.size();
  ssig.ptr[1].p = &receiverId;
  ssig.ptr[1].sz = 1;

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SETUP_REQ failed\n");
    return -1;
  }

  /* Wait for SETUP_CONF or SETUP_REF */
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SETUP_CONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_SETUP_CONF) {
    const JoinAggSetupConf *conf =
      reinterpret_cast<const JoinAggSetupConf *>(resp->getDataPtr());
    aggStateKeyOut = conf->aggStateKey;
    /* Phase L (E.1): owner LDM instance for routing every subsequent
     * JOIN_AGG_COMPLETE_REQ — must be addressed to numberToBlock(
     * DBLQH, ownerInstance) on the same nodeId. */
    ownerInstanceOut = conf->ownerInstance;
    V("SETUP_CONF: aggStateKey=%u ownerInstance=%u\n",
      aggStateKeyOut, ownerInstanceOut);
    return 0;
  } else if (gsn == GSN_JOIN_AGG_SETUP_REF) {
    const JoinAggSetupRef *ref =
      reinterpret_cast<const JoinAggSetupRef *>(resp->getDataPtr());
    fprintf(stderr, "SETUP_REF: errorCode=%u errorLine=%u\n",
            ref->errorCode, ref->errorLine);
    return -1;
  } else {
    fprintf(stderr, "Unexpected GSN %d waiting for SETUP_CONF\n", gsn);
    return -1;
  }
}

/* ------------------------------------------------------------------ */
/* SCAN_FRAGREQ with encoded leaf index                                */
/* ------------------------------------------------------------------ */

/*
 * Send SCAN_FRAGREQ with the aggStateKey encoding the leaf index
 * in the upper 8 bits: encodedKey = (leafIndex << 24) | baseKey
 */
static int
sendScanFragReqEncoded(SignalSender &ss, Uint32 nodeId,
                       Uint32 fragId, Uint32 ldmInstance,
                       Uint32 baseAggStateKey,
                       Uint32 leafIndex,
                       const TableMeta &meta,
                       const std::vector<Uint32> &attrInfo)
{
  Uint32 encodedKey = (leafIndex << 24) | baseAggStateKey;
  V("  SCAN_FRAGREQ -> node %u, frag %u, LDM %u, leaf %u, "
         "encodedKey=0x%08x\n",
         nodeId, fragId, ldmInstance, leafIndex, encodedKey);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  memset(data, 0, 25 * sizeof(Uint32));

  ScanFragReq *scanReq = reinterpret_cast<ScanFragReq *>(data);
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
  scanReq->variableData[varIdx++] = encodedKey;

  Uint32 sigLen = ScanFragReq::SignalLength + varIdx;

  Uint16 recBlock = numberToBlock(V_QUERY, ldmInstance);
  ssig.set(ss, 0, recBlock, GSN_SCAN_FRAGREQ, sigLen);
  ssig.header.m_noOfSections = 1;
  ssig.ptr[0].p = attrInfo.data();
  ssig.ptr[0].sz = (Uint32)attrInfo.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SCAN_FRAGREQ failed\n");
    return -1;
  }
  return 0;
}

/* ------------------------------------------------------------------ */
/* SCAN_FRAGCONF wait                                                  */
/* ------------------------------------------------------------------ */

static int
waitForScanConf(SignalSender &ss, Uint32 /*fragId*/, Uint32 &rowsScanned)
{
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SCAN_FRAGCONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_SCAN_FRAGCONF) {
    const ScanFragConf *conf =
      reinterpret_cast<const ScanFragConf *>(resp->getDataPtr());
    rowsScanned = conf->completedOps;
    Uint32 sigLen = resp->header.theLength;
    Uint32 rowsExamined =
      (sigLen >= ScanFragConf::SignalLength_v2) ? conf->rowsExamined : 0;
    V("  SCAN_FRAGCONF: frag=%u completed=%u fragDone=%u"
           " rowsExamined=%u (sigLen=%u)\n",
           conf->senderData, conf->completedOps, conf->fragmentCompleted,
           rowsExamined, sigLen);
    return 0;
  } else if (gsn == GSN_SCAN_FRAGREF) {
    const Uint32 *d = resp->getDataPtr();
    fprintf(stderr, "  SCAN_FRAGREF: errorCode=%u\n", d[3]);
    return -1;
  } else {
    fprintf(stderr, "Unexpected GSN %d waiting for SCAN_FRAGCONF\n", gsn);
    return -1;
  }
}

/* ------------------------------------------------------------------ */
/* Result parsing and extraction                                       */
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
    fprintf(stderr, "extractSumBigint: val too short (size=%zu, need=%u)\n",
            val.size(), offset + 8);
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
    fprintf(stderr, "extractGroupKey: key too short (%zu, need %u)\n",
            key.size(), ATTR_HEADER_SIZE + 8);
    return 0;
  }
  Int64 v;
  memcpy(&v, key.data() + ATTR_HEADER_SIZE, sizeof(Int64));
  return v;
}

/* ------------------------------------------------------------------ */
/* Complete + receive results + release                                */
/* ------------------------------------------------------------------ */

static int
sendCompleteReq(SignalSender &ss, Uint32 nodeId,
                Uint32 aggStateKey,
                Uint32 ownerInstance,
                Uint32 maxBatchRows)
{
  V("\n--- JOIN_AGG_COMPLETE_REQ -> node %u inst %u ---\n",
    nodeId, ownerInstance);

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

  /* Phase L (E.1): COMPLETE_REQ must reach the owner LDM for the
   * aggStateKey's state, not instance 1.  Owner came back in
   * SETUP_CONF; reproduce it here. */
  Uint16 recBlock = numberToBlock(DBLQH, ownerInstance);
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
  Uint32 nodeId = refToNode(sendReq->senderRef);

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

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SEND_CONF failed\n");
    return -1;
  }
  V("  -> sent JOIN_AGG_SEND_CONF to node %u\n", nodeId);
  return 0;
}

static int
receiveResults(SignalSender &ss, std::vector<AggResult> &allResults,
               Uint32 &totalGroups)
{
  V("Waiting for results...\n");
  Uint32 nodeGroups = 0;
  bool done = false;

  while (!done) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "results");
    if (resp == nullptr) return -1;

    int gsn = getGsn(resp);

    if (gsn == GSN_TRANSID_AI) {
      AggResult result;
      if (parseTransIdAI(resp, result) != 0) return -1;
      V("  TRANSID_AI: n_gb_cols=%u n_agg_results=%u n_groups=%u\n",
             result.n_gb_cols, result.n_agg_results, result.n_groups);
      nodeGroups += result.n_groups;
      totalGroups += result.n_groups;
      allResults.push_back(std::move(result));
    }
    else if (gsn == GSN_JOIN_AGG_SEND_REQ) {
      const JoinAggSendReq *sendReq =
        reinterpret_cast<const JoinAggSendReq *>(resp->getDataPtr());
      V("  JOIN_AGG_SEND_REQ: rowsSent=%u bytes=%u\n",
             sendReq->numRowsSent, sendReq->resultBytes);
      if (sendSendConf(ss, sendReq, 1000) != 0) return -1;
    }
    else if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
      const JoinAggCompleteConf *conf =
        reinterpret_cast<const JoinAggCompleteConf *>(resp->getDataPtr());
      V("JOIN_AGG_COMPLETE_CONF: numResultRows=%u resultBytes=%u\n",
             conf->numResultRows, conf->resultBytes);
      done = true;
    }
    else if (gsn == GSN_JOIN_AGG_COMPLETE_REF) {
      const JoinAggCompleteRef *ref =
        reinterpret_cast<const JoinAggCompleteRef *>(resp->getDataPtr());
      fprintf(stderr, "COMPLETE_REF: errorCode=%u errorLine=%u\n",
              ref->errorCode, ref->errorLine);
      return -1;
    }
    else {
      fprintf(stderr, "Unexpected GSN %d during result reception\n", gsn);
    }
  }

  V("Received %u groups from this node (%u total) across %zu TRANSID_AI signals\n",
         nodeGroups, totalGroups, allResults.size());
  return 0;
}

static int
sendReleaseReq(SignalSender &ss, Uint32 nodeId, Uint32 aggStateKey)
{
  V("\n--- JOIN_AGG_RELEASE_REQ -> node %u ---\n", nodeId);

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

  int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_RELEASE_CONF) {
    V("RELEASE_CONF received\n");
    return 0;
  } else {
    fprintf(stderr, "Unexpected GSN %d waiting for RELEASE_CONF\n", gsn);
    return -1;
  }
}

/* ------------------------------------------------------------------ */
/* SQL verification helpers                                            */
/* ------------------------------------------------------------------ */

static int
verifySqlGroupByMap(const char *query,
                    const std::map<Int64, Int64> &expected)
{
  if (g_mysql_conn == nullptr) return 0;

  if (mysql_query(g_mysql_conn, query) != 0) {
    fprintf(stderr, "SQL verify failed: %s\n  query: %s\n",
            mysql_error(g_mysql_conn), query);
    return -1;
  }

  MYSQL_RES *res = mysql_store_result(g_mysql_conn);
  if (res == nullptr) {
    fprintf(stderr, "mysql_store_result failed: %s\n",
            mysql_error(g_mysql_conn));
    return -1;
  }

  std::map<Int64, Int64> sqlGroups;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    if (row[0] == nullptr || row[1] == nullptr) continue;
    sqlGroups[(Int64)atoll(row[0])] = (Int64)atoll(row[1]);
  }
  mysql_free_result(res);

  if (sqlGroups != expected) {
    fprintf(stderr, "SQL verify mismatch (SQL=%zu groups, expected=%zu)\n",
            sqlGroups.size(), expected.size());
    for (const auto &exp : expected) {
      auto it = sqlGroups.find(exp.first);
      if (it == sqlGroups.end())
        fprintf(stderr, "  missing group(%lld)\n", (long long)exp.first);
      else if (it->second != exp.second)
        fprintf(stderr, "  group(%lld): SQL=%lld expected=%lld\n",
                (long long)exp.first, (long long)it->second,
                (long long)exp.second);
    }
    return -1;
  }
  V("  SQL verify: %zu groups -- matches\n", sqlGroups.size());
  return 0;
}

static int
verifySqlScalars(const char *query, const Int64 *expected, Uint32 numCols)
{
  if (g_mysql_conn == nullptr) return 0;

  if (mysql_query(g_mysql_conn, query) != 0) {
    fprintf(stderr, "SQL verify failed: %s\n  query: %s\n",
            mysql_error(g_mysql_conn), query);
    return -1;
  }

  MYSQL_RES *res = mysql_store_result(g_mysql_conn);
  if (res == nullptr) {
    fprintf(stderr, "mysql_store_result failed: %s\n",
            mysql_error(g_mysql_conn));
    return -1;
  }

  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == nullptr) {
    fprintf(stderr, "SQL verify: no result row\n");
    mysql_free_result(res);
    return -1;
  }

  int failures = 0;
  for (Uint32 i = 0; i < numCols; i++) {
    Int64 sqlVal = row[i] != nullptr ? (Int64)atoll(row[i]) : 0;
    if (sqlVal != expected[i]) {
      fprintf(stderr, "SQL verify mismatch: col %u SQL=%lld expected=%lld\n",
              i, (long long)sqlVal, (long long)expected[i]);
      failures++;
    }
  }
  mysql_free_result(res);

  if (failures > 0) return -1;
  V("  SQL verify: scalars match\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 1: 2 leaves — SUM(b) and COUNT(b) GROUP BY a                   */
/* ------------------------------------------------------------------ */

static int
test_2leaf_sum_count(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n========================================\n");
  V("Test 1: 2-leaf star — SUM(b) + COUNT(b) GROUP BY a\n");
  V("========================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Build two individual programs */
  auto prog0 = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);
  auto prog1 = buildAggProgram_CountGroupBy(meta.attrIdA, meta.attrIdB);

  /* Build multi-leaf section: [numLeaves=2, len0, prog0..., len1, prog1...] */
  std::vector<std::vector<Uint32>> programs = {prog0, prog1};
  auto multiLeafSection = buildMultiLeafSection(programs);

  V("Multi-leaf section: %zu words (numLeaves=%u)\n",
    multiLeafSection.size(), multiLeafSection[0]);

  /* 1. Setup on all data nodes */
  std::map<Uint32, Uint32> aggStateKeys;
  std::map<Uint32, Uint32> ownerInstances;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    Uint32 owner = 0;
    if (sendMultiLeafSetupReq(ss, nd, multiLeafSection, meta,
                              JoinAggSetupReq::STRATEGY_MUTEX_BASED,
                              key, owner) != 0)
      return -1;
    aggStateKeys[nd] = key;
    ownerInstances[nd] = owner;
  }

  /* 2. Scan all fragments TWICE — once per leaf */
  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsScanned = 0;

  for (Uint32 leaf = 0; leaf < 2; leaf++) {
    V("\n--- Scanning for leaf %u ---\n", leaf);
    for (Uint32 f = 0; f < meta.fragCount; f++) {
      Uint32 fragNodeId = meta.fragNodes[f];
      Uint32 ldmInst = meta.fragInstances[f];
      Uint32 baseKey = aggStateKeys[fragNodeId];
      if (sendScanFragReqEncoded(ss, fragNodeId, f, ldmInst,
                                 baseKey, leaf, meta, attrInfo) != 0)
        return -1;

      Uint32 rows = 0;
      if (waitForScanConf(ss, f, rows) != 0)
        return -1;
      totalRowsScanned += rows;
    }
  }

  V("\nTotal rows scanned: %u\n", totalRowsScanned);

  /* 3. Complete + receive results */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd],
                        ownerInstances[nd], 1000) != 0)
      return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0)
      return -1;
  }

  /* 4. Release */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  /* 5. Validate */
  V("\n--- Validation ---\n");

  /*
   * Expected: 5 groups, each with combined accumulators:
   *   leaf 0 accumulator (agg[0]): SUM(b)
   *   leaf 1 accumulator (agg[1]): COUNT(b)
   */
  if (totalGroups != 5) {
    fprintf(stderr, "FAIL: expected 5 groups, got %u\n", totalGroups);
    return -1;
  }

  int failures = 0;
  std::map<Int64, Int64> expectedSum;
  expectedSum[1] = 10; expectedSum[2] = 20; expectedSum[3] = 30;
  expectedSum[4] = 40; expectedSum[5] = 50;

  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);   /* leaf 0: SUM at agg[0] */
      Uint64 count = extractCountBigint(grp.second, 1); /* leaf 1: COUNT at agg[1] */
      V("  group(%lld) = SUM %lld, COUNT %llu\n",
             (long long)key, (long long)sum, (unsigned long long)count);

      auto it = expectedSum.find(key);
      if (it == expectedSum.end()) {
        fprintf(stderr, "FAIL: unexpected group(%lld)\n", (long long)key);
        failures++;
      } else {
        if (sum != it->second) {
          fprintf(stderr, "FAIL: group(%lld) SUM expected %lld, got %lld\n",
                  (long long)key, (long long)it->second, (long long)sum);
          failures++;
        }
        if (count != 1) {
          fprintf(stderr, "FAIL: group(%lld) COUNT expected 1, got %llu\n",
                  (long long)key, (unsigned long long)count);
          failures++;
        }
      }
    }
  }

  /* SQL cross-check */
  if (failures == 0) {
    if (verifySqlGroupByMap(
          "SELECT a, SUM(b) FROM star_ml_test GROUP BY a",
          expectedSum) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 1 — 2-leaf SUM+COUNT GROUP BY, "
           "all %u groups correct\n", totalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: 2 leaves — SUM(b) and COUNT(b), no GROUP BY                 */
/* ------------------------------------------------------------------ */

static int
test_2leaf_no_groupby(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n========================================\n");
  V("Test 2: 2-leaf star — SUM(b) + COUNT(b), no GROUP BY\n");
  V("========================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Build two no-group-by programs */
  auto prog0 = buildAggProgram_SumNoGroupBy(meta.attrIdB);
  auto prog1 = buildAggProgram_CountNoGroupBy(meta.attrIdB);

  std::vector<std::vector<Uint32>> programs = {prog0, prog1};
  auto multiLeafSection = buildMultiLeafSection(programs);

  /* 1. Setup */
  std::map<Uint32, Uint32> aggStateKeys;
  std::map<Uint32, Uint32> ownerInstances;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    Uint32 owner = 0;
    if (sendMultiLeafSetupReq(ss, nd, multiLeafSection, meta,
                              JoinAggSetupReq::STRATEGY_MUTEX_BASED,
                              key, owner) != 0)
      return -1;
    aggStateKeys[nd] = key;
    ownerInstances[nd] = owner;
  }

  /* 2. Scan all fragments twice (once per leaf) */
  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsScanned = 0;

  for (Uint32 leaf = 0; leaf < 2; leaf++) {
    V("\n--- Scanning for leaf %u ---\n", leaf);
    for (Uint32 f = 0; f < meta.fragCount; f++) {
      Uint32 fragNodeId = meta.fragNodes[f];
      Uint32 ldmInst = meta.fragInstances[f];
      Uint32 baseKey = aggStateKeys[fragNodeId];
      if (sendScanFragReqEncoded(ss, fragNodeId, f, ldmInst,
                                 baseKey, leaf, meta, attrInfo) != 0)
        return -1;

      Uint32 rows = 0;
      if (waitForScanConf(ss, f, rows) != 0)
        return -1;
      totalRowsScanned += rows;
    }
  }

  V("\nTotal rows scanned: %u\n", totalRowsScanned);

  /* 3. Complete + receive results */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd],
                        ownerInstances[nd], 1000) != 0)
      return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0)
      return -1;
  }

  /* 4. Release */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  /* 5. Validate: single result row with SUM=150 at agg[0], COUNT=5 at agg[1] */
  V("\n--- Validation ---\n");

  if (allResults.empty()) {
    fprintf(stderr, "FAIL: no results received\n");
    return -1;
  }

  int failures = 0;

  /* For no-group-by, results may come from multiple nodes; we need to
   * sum the SUM values and COUNT values across nodes */
  Int64 totalSum = 0;
  Uint64 totalCount = 0;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 sum = extractSumBigint(grp.second, 0);
      Uint64 count = extractCountBigint(grp.second, 1);
      V("  partial: SUM=%lld, COUNT=%llu\n",
             (long long)sum, (unsigned long long)count);
      totalSum += sum;
      totalCount += count;
    }
  }

  V("  total: SUM=%lld, COUNT=%llu\n",
         (long long)totalSum, (unsigned long long)totalCount);

  if (totalSum != 150) {
    fprintf(stderr, "FAIL: expected SUM=150, got %lld\n", (long long)totalSum);
    failures++;
  }
  if (totalCount != 5) {
    fprintf(stderr, "FAIL: expected COUNT=5, got %llu\n",
            (unsigned long long)totalCount);
    failures++;
  }

  /* SQL cross-check */
  if (failures == 0) {
    Int64 expected[2] = {150, 5};
    if (verifySqlScalars(
          "SELECT SUM(b), COUNT(b) FROM star_ml_test",
          expected, 2) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 2 — 2-leaf SUM+COUNT no GROUP BY, "
           "SUM=%lld COUNT=%llu\n",
           (long long)totalSum, (unsigned long long)totalCount);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: Single leaf in multi-leaf format (backward compatibility)    */
/* ------------------------------------------------------------------ */

static int
test_single_leaf_compat(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n========================================\n");
  V("Test 3: Single leaf in multi-leaf format (compat)\n");
  V("========================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Single program: SUM(b) GROUP BY a */
  auto prog0 = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  /* Multi-leaf format with numLeaves=1 */
  std::vector<std::vector<Uint32>> programs = {prog0};
  auto multiLeafSection = buildMultiLeafSection(programs);

  V("Multi-leaf section: %zu words (numLeaves=%u)\n",
    multiLeafSection.size(), multiLeafSection[0]);

  /* 1. Setup */
  std::map<Uint32, Uint32> aggStateKeys;
  std::map<Uint32, Uint32> ownerInstances;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    Uint32 owner = 0;
    if (sendMultiLeafSetupReq(ss, nd, multiLeafSection, meta,
                              JoinAggSetupReq::STRATEGY_MUTEX_BASED,
                              key, owner) != 0)
      return -1;
    aggStateKeys[nd] = key;
    ownerInstances[nd] = owner;
  }

  /* 2. Scan all fragments once with leaf index 0 */
  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsScanned = 0;

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 fragNodeId = meta.fragNodes[f];
    Uint32 ldmInst = meta.fragInstances[f];
    Uint32 baseKey = aggStateKeys[fragNodeId];
    if (sendScanFragReqEncoded(ss, fragNodeId, f, ldmInst,
                               baseKey, /*leafIndex=*/0, meta, attrInfo) != 0)
      return -1;

    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0)
      return -1;
    totalRowsScanned += rows;
  }

  V("\nTotal rows scanned: %u\n", totalRowsScanned);

  /* 3. Complete + receive */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd],
                        ownerInstances[nd], 1000) != 0)
      return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0)
      return -1;
  }

  /* 4. Release */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  /* 5. Validate: same as regular single-program SUM GROUP BY */
  V("\n--- Validation ---\n");

  if (totalGroups != 5) {
    fprintf(stderr, "FAIL: expected 5 groups, got %u\n", totalGroups);
    return -1;
  }

  int failures = 0;
  std::map<Int64, Int64> expected;
  expected[1] = 10; expected[2] = 20; expected[3] = 30;
  expected[4] = 40; expected[5] = 50;

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] = sum;
      V("  group(%lld) = SUM %lld\n",
             (long long)key, (long long)sum);
    }
  }

  for (const auto &exp : expected) {
    auto it = actual.find(exp.first);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)exp.first);
      failures++;
    } else if (it->second != exp.second) {
      fprintf(stderr, "FAIL: group(%lld) expected SUM=%lld, got %lld\n",
              (long long)exp.first, (long long)exp.second,
              (long long)it->second);
      failures++;
    }
  }

  if (failures == 0) {
    if (verifySqlGroupByMap(
          "SELECT a, SUM(b) FROM star_ml_test GROUP BY a",
          expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 3 — single leaf in multi-leaf format, "
           "all %u groups correct\n", totalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static const char *connectString = nullptr;
static int mysqlPort = 3306;

int main(int argc, char **argv)
{
  /* Parse -c <connect_string> -m <mysql_port> -v/--verbose -h/--help */
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n\n"
             "Options:\n"
             "  -c <connect_string>  NDB management server connect string\n"
             "                       (default: localhost:1186)\n"
             "  -m <mysql_port>      MySQL server port for ndbinfo queries\n"
             "                       (default: 3306)\n"
             "  -v, --verbose        Show detailed progress output\n"
             "  -h, --help           Show this help message\n",
             argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    }
  }
  if (connectString == nullptr) {
    connectString = "localhost:1186";
  }

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
      result = 1; break;
    }
    g_mysql_conn = conn;
    V("Connected to MySQL on port %d\n", mysqlPort);

    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      mysql_close(conn);
      result = 1; break;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30 seconds\n");
      mysql_close(conn);
      result = 1; break;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
      mysql_close(conn);
      result = 1; break;
    }

    /* Create table and insert test data */
    TableMeta meta;
    if (createTestTable(conn, &ndb, meta) != 0) {
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
      V("SignalSender: block=%u node=%u ref=0x%08x\n",
        refToBlock(ss.getOwnRef()),
        refToNode(ss.getOwnRef()),
        ss.getOwnRef());

      if (test_2leaf_sum_count(&ndb, ss, meta) != 0) result = 1;
      if (test_2leaf_no_groupby(&ndb, ss, meta) != 0) result = 1;
      if (test_single_leaf_compat(&ndb, ss, meta) != 0) result = 1;

      ss.unlock();
    }

    dropTestTable(conn);
    mysql_close(conn);
  } while (0);

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return result;
}
