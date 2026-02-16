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
 * testCaseAgg — Unit test for embedded interpreter CASE support in
 * join aggregation.
 *
 * Tests SUM(CASE WHEN col_b <= 20 THEN 1 ELSE 0 END) by building
 * an aggregation program with kOpEmbeddedInterp + kOpSkip, and
 * verifying results via the join aggregation signal protocol.
 *
 * Usage: testCaseAgg -c <connect_string> -m <mysql_port> [-v]
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
#ifdef NONE
#undef NONE
#endif
#include <kernel/Interpreter.hpp>

#include <kernel/signaldata/LqhKey.hpp>

#include <NdbRestarter.hpp>
#include <util/rondb_hash.hpp>
#include <mysql.h>

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

static const char *TABLE_NAME = "case_agg_test";
static const Uint32 FAKE_TRANS_ID1 = 0x12345678;
static const Uint32 FAKE_TRANS_ID2 = 0x87654321;
static const Uint32 FAKE_REQUEST_ID = 2001;
static const Uint32 FAKE_SENDER_DATA = 42;
static const Uint32 WAIT_TIMEOUT_MS = 30000;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 INTERPRETER_EXIT_OK = 18;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;

/* ------------------------------------------------------------------ */
/* Aggregation program builder with embedded interpreter               */
/* ------------------------------------------------------------------ */

/*
 * Build aggregation program for:
 *   SELECT SUM(CASE WHEN col_b <= 20 THEN 1 ELSE 0 END) FROM t
 *
 * No GROUP BY, n_agg_results=1.
 *
 * Layout:
 *   Words 0-7: header (magic, counts, version, reserved)
 *   Word 8: kOpEmbeddedInterp [len=EMB_LEN]
 *   Words 9..9+EMB_LEN-1: embedded old-interpreter instructions
 *   After embedded block: THEN arm, ELSE arm with kOpSkip
 *
 * Embedded program evaluates: col_b <= 20
 *   If TRUE:  WRITE_INTERPRETER_OUTPUT reg2=0 to output[0], EXIT_OK
 *   If FALSE: WRITE_INTERPRETER_OUTPUT reg2=3 to output[0], EXIT_OK
 *
 * Aggregation after embedded block:
 *   A+0: kOpLoadConst reg0, 1  (THEN value) [3 words]
 *   A+3: kOpSum reg0, agg[0]   [1 word]
 *   A+4: kOpSkip 3             [1 word] (skip ELSE block)
 *   A+5: kOpLoadConst reg0, 0  (ELSE value) [3 words]
 *   A+8: kOpSum reg0, agg[0]   [1 word]
 *
 * skip_offset=0 → land at A+0 (THEN path)
 * skip_offset=5 → land at A+5 (ELSE path)
 */
static std::vector<Uint32>
buildAggProgram_CaseSum(Uint32 colBAttrId)
{
  /*
   * Embedded interpreter instructions:
   *   [0] READ_ATTR_INTO_REG reg0, col(colBAttrId)
   *   [1] LOAD_CONST16 reg1, 20
   *   [2] BRANCH_LE_REG_REG reg0, reg1, offset=3 → emb[5]
   *   --- ELSE path ---
   *   [3] LOAD_CONST16 reg2, 5 (skip_offset for ELSE)
   *   [4] WRITE_INTERPRETER_OUTPUT reg2, output_index=0
   *   [5] EXIT_OK
   *   --- THEN path (branch target) ---
   *   [6] LOAD_CONST16 reg2, 0 (skip_offset for THEN)
   *   [7] WRITE_INTERPRETER_OUTPUT reg2, output_index=0
   *   [8] EXIT_OK
   */
  const Uint32 EMB_LEN = 9;

  /*
   * Aggregation instructions after embedded block:
   *   [0-2] kOpLoadConst reg0, 1 (THEN value) - 3 words
   *   [3]   kOpSum reg0, agg[0]
   *   [4]   kOpSkip 4 (skip past ELSE block: 3 words LoadConst + 1 word Sum)
   *   [5-7] kOpLoadConst reg0, 0 (ELSE value) - 3 words
   *   [8]   kOpSum reg0, agg[0]
   */
  const Uint32 AGG_AFTER = 9;

  /* Header = 8 words, 1 kOpEmbeddedInterp header, EMB_LEN embedded, AGG_AFTER agg */
  const Uint32 PROG_LEN = 8 + 1 + EMB_LEN + AGG_AFTER;

  std::vector<Uint32> prog(PROG_LEN);

  /* Header */
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 1u;  /* n_gb_cols=0, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  Uint32 pos = 8;

  /* kOpEmbeddedInterp header */
  prog[pos++] = (kOpEmbeddedInterp << 26) | EMB_LEN;

  /* Embedded instructions */
  Uint32 emb_start = pos;

  /* emb[0]: READ_ATTR_INTO_REG reg0, col(colBAttrId) */
  prog[pos++] = Interpreter::READ_ATTR_INTO_REG | (0 << 6) |
                (colBAttrId << 16);

  /* emb[1]: LOAD_CONST16 reg1, 20 */
  prog[pos++] = Interpreter::LOAD_CONST16 | (1 << 6) | (20 << 16);

  /*
   * emb[2]: BRANCH_LE_REG_REG reg0, reg1, offset=3
   * Branch target: emb[2+3] = emb[5] (wait, brancher does pc + offset
   * where pc is the instruction position itself)
   *
   * Actually: brancher() does TprogramCounter-- (undo the ++ in main loop)
   * then TprogramCounter + TbranchLength for forward.
   * So if branch is at pc=2, target = 2 + offset.
   * We want to reach emb[5], so offset = 5 - 2 = 3.
   * But wait: we want THEN path at emb[6] (after the EXIT_OK at emb[5]).
   * Actually emb[5] is EXIT_OK (end of ELSE path). The THEN path starts
   * at emb[6]. So offset should be 6 - 2 = 4.
   *
   * Let me re-layout:
   *   emb[0]: READ_ATTR_INTO_REG
   *   emb[1]: LOAD_CONST16
   *   emb[2]: BRANCH_LE_REG_REG → if true, go to THEN path at emb[6]
   *   emb[3]: LOAD_CONST16 reg2, 5 (ELSE skip_offset)
   *   emb[4]: WRITE_INTERPRETER_OUTPUT
   *   emb[5]: EXIT_OK
   *   emb[6]: LOAD_CONST16 reg2, 0 (THEN skip_offset)
   *   emb[7]: WRITE_INTERPRETER_OUTPUT
   *   emb[8]: EXIT_OK
   *
   * offset = 6 - 2 = 4
   */
  prog[pos++] = Interpreter::BRANCH_LE_REG_REG | (0 << 6) | (1 << 9) |
                (4 << 16);

  /* ELSE path: emb[3..5] */
  /* emb[3]: LOAD_CONST16 reg2, 5 (skip_offset to reach ELSE agg block) */
  prog[pos++] = Interpreter::LOAD_CONST16 | (2 << 6) | (5 << 16);

  /* emb[4]: WRITE_INTERPRETER_OUTPUT reg2, output_index=0 */
  prog[pos++] = Interpreter::WriteInterpreterOutput(2, 0);

  /* emb[5]: EXIT_OK */
  prog[pos++] = Interpreter::ExitOK();

  /* THEN path: emb[6..8] */
  /* emb[6]: LOAD_CONST16 reg2, 0 (skip_offset=0, land at THEN agg block) */
  prog[pos++] = Interpreter::LOAD_CONST16 | (2 << 6) | (0 << 16);

  /* emb[7]: WRITE_INTERPRETER_OUTPUT reg2, output_index=0 */
  prog[pos++] = Interpreter::WriteInterpreterOutput(2, 0);

  /* emb[8]: EXIT_OK */
  prog[pos++] = Interpreter::ExitOK();

  assert(pos == emb_start + EMB_LEN);

  /*
   * Aggregation instructions after embedded block.
   * THEN path (skip_offset=0 → lands here):
   *   kOpLoadConst(type=BIGINT, reg=0, value=1) — 3 words
   *   kOpSum(reg=0, agg_idx=0) — 1 word
   *   kOpSkip 4 — skip past ELSE block (3 LoadConst words + 1 Sum word)
   *
   * ELSE path (skip_offset=5 → lands here):
   *   kOpLoadConst(type=BIGINT, reg=0, value=0) — 3 words
   *   kOpSum(reg=0, agg_idx=0) — 1 word
   */

  /* THEN: kOpLoadConst reg0, BIGINT, value=1 */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  Int64 then_val = 1;
  memcpy(&prog[pos], &then_val, sizeof(Int64));
  pos += 2;

  /* THEN: kOpSum reg0, agg[0] */
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 0;

  /* THEN: kOpSkip 4 (skip past ELSE block) */
  prog[pos++] = (kOpSkip << 26) | 4;

  /* ELSE: kOpLoadConst reg0, BIGINT, value=0 */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  Int64 else_val = 0;
  memcpy(&prog[pos], &else_val, sizeof(Int64));
  pos += 2;

  /* ELSE: kOpSum reg0, agg[0] */
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 0;

  assert(pos == PROG_LEN);

  return prog;
}

/* ------------------------------------------------------------------ */
/* AttrInfo builder                                                    */
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
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
  std::vector<Uint32> fragInstances;
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
             const char *colA, const char *colB)
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
  sqlExec(conn, "DROP TABLE IF EXISTS case_agg_test");
  if (sqlExec(conn,
        "CREATE TABLE case_agg_test ("
        "  a BIGINT NOT NULL PRIMARY KEY,"
        "  b BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, TABLE_NAME, meta, "a", "b") != 0) return -1;

  V("Table '%s': id=%u version=%u attrA=%u attrB=%u frags=%u\n",
         TABLE_NAME, meta.tableId, meta.schemaVersion,
         meta.attrIdA, meta.attrIdB, meta.fragCount);

  return 0;
}

static int
queryFragInstances(int mysqlPort, TableMeta &meta)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) { fprintf(stderr, "mysql_init failed\n"); return -1; }

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
    fprintf(stderr, "mysql_store_result failed\n");
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
    V("  fragment %u → node %u, LDM instance %u\n",
           f, meta.fragNodes[f], meta.fragInstances[f]);
  }

  return 0;
}

static int
insertTestData(Ndb *ndb)
{
  /*
   * Insert rows: (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)
   *
   * For CASE WHEN b <= 20: rows 1,2 match (b=10,20), rows 3,4,5 don't
   * Expected SUM(CASE WHEN b <= 20 THEN 1 ELSE 0 END) = 2
   */
  struct Row { Int64 a; Int64 b; };
  static const Row rows[] = {
    {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}
  };

  for (const auto &r : rows) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }
    NdbOperation *op = tx->getNdbOperation(TABLE_NAME);
    if (op == nullptr) {
      fprintf(stderr, "getNdbOperation: %s\n", tx->getNdbError().message);
      ndb->closeTransaction(tx);
      return -1;
    }
    op->insertTuple();
    op->equal("a", r.a);
    op->setValue("b", r.b);
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "execute insert (%lld,%lld): %s\n",
              (long long)r.a, (long long)r.b, tx->getNdbError().message);
      ndb->closeTransaction(tx);
      return -1;
    }
    ndb->closeTransaction(tx);
  }

  V("Inserted %zu rows\n", sizeof(rows)/sizeof(rows[0]));
  return 0;
}

static void
dropTestTable(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS case_agg_test");
}

/* ------------------------------------------------------------------ */
/* Signal helpers (same patterns as testJoinAgg.cpp)                    */
/* ------------------------------------------------------------------ */

static SimpleSignal *
waitForSignal(SignalSender &ss, Uint32 timeout, const char *ctx)
{
  SimpleSignal *resp = ss.waitFor(timeout);
  if (resp == nullptr) {
    fprintf(stderr, "Timeout waiting for %s\n", ctx);
  }
  return resp;
}

static int getGsn(const SimpleSignal *sig) {
  return sig->header.theVerId_signalNumber;
}

static int
sendSetupReq(SignalSender &ss, Uint32 nodeId,
             const std::vector<Uint32> &aggProgram,
             const TableMeta &meta,
             Uint32 strategy,
             Uint32 &aggStateKeyOut)
{
  V("\n--- JOIN_AGG_SETUP_REQ → node %u ---\n", nodeId);

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

  ssig.set(ss, 0, DBLQH, GSN_JOIN_AGG_SETUP_REQ,
           JoinAggSetupReq::SignalLength);
  ssig.header.m_noOfSections = 1;
  ssig.ptr[0].p = aggProgram.data();
  ssig.ptr[0].sz = (Uint32)aggProgram.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SETUP_REQ failed\n");
    return -1;
  }

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SETUP_CONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_JOIN_AGG_SETUP_CONF) {
    const JoinAggSetupConf *conf =
      reinterpret_cast<const JoinAggSetupConf *>(resp->getDataPtr());
    aggStateKeyOut = conf->aggStateKey;
    V("SETUP_CONF: aggStateKey=%u\n", aggStateKeyOut);
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

static int
sendScanFragReq(SignalSender &ss, Uint32 nodeId,
                Uint32 fragId, Uint32 ldmInstance,
                Uint32 aggStateKey,
                const TableMeta &meta,
                const std::vector<Uint32> &attrInfo)
{
  V("  SCAN_FRAGREQ → node %u, frag %u, LDM %u\n",
         nodeId, fragId, ldmInstance);

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
  scanReq->variableData[varIdx++] = aggStateKey;

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

  /* Wait for SCAN_FRAGCONF */
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SCAN_FRAGCONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_SCAN_FRAGCONF) {
    V("  SCAN_FRAGCONF received\n");
    return 0;
  } else {
    fprintf(stderr, "Unexpected GSN %d waiting for SCAN_FRAGCONF\n", gsn);
    return -1;
  }
}

static int
sendCompleteReq(SignalSender &ss, Uint32 nodeId,
                Uint32 aggStateKey, Uint32 maxBatchRows)
{
  V("\n--- JOIN_AGG_COMPLETE_REQ → node %u ---\n", nodeId);

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
  return 0;
}

/* ------------------------------------------------------------------ */
/* Result parsing (same patterns as testJoinAgg.cpp)                    */
/* ------------------------------------------------------------------ */

struct AggResult {
  Uint32 n_gb_cols;
  Uint32 n_agg_results;
  Uint32 n_groups;
  /* For each group: key bytes → value bytes */
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
  Uint32 offset = aggIdx * ITEM_SIZE + 8;  /* skip type(4) + padding(4) */
  if (offset + 8 > val.size()) {
    fprintf(stderr, "extractSumBigint: val too short (size=%zu, need=%u)\n",
            val.size(), offset + 8);
    return 0;
  }
  Int64 v;
  memcpy(&v, val.data() + offset, sizeof(Int64));
  return v;
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
  V("\n--- JOIN_AGG_RELEASE_REQ → node %u ---\n", nodeId);

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
/* Test: SUM(CASE WHEN b <= 20 THEN 1 ELSE 0 END)                     */
/* ------------------------------------------------------------------ */

static int
testCaseSum(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test: SELECT SUM(CASE WHEN b<=20 THEN 1 ELSE 0 END) FROM %s\n",
         TABLE_NAME);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  auto aggProg = buildAggProgram_CaseSum(meta.attrIdB);
  auto attrInfo = buildAttrInfo();

  /* 1. Setup on all data nodes */
  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED,
                     key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  /* 2. Scan each fragment */
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 nd = meta.fragNodes[f];
    Uint32 inst = meta.fragInstances[f];
    if (sendScanFragReq(ss, nd, f, inst, aggStateKeys[nd],
                        meta, attrInfo) != 0)
      return -1;
  }

  /* 3. Complete + 4. Receive results from all nodes */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0)
      return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0)
      return -1;
  }

  /* 5. Release on all nodes */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  /* 6. Validate — non-group-by: n_gb_cols=0, 1 agg result (SUM) */
  V("\n--- Validation ---\n");

  if (allResults.empty()) {
    fprintf(stderr, "FAIL: no results received\n");
    return -1;
  }

  /* For non-group-by, each node returns one TRANSID_AI with n_groups=0
   * containing partial accumulators. Sum across all nodes.
   */
  Int64 sum = 0;
  for (const auto &res : allResults) {
    if (res.n_gb_cols != 0) {
      fprintf(stderr, "FAIL: expected n_gb_cols=0, got %u\n", res.n_gb_cols);
      return -1;
    }
    if (res.groups.empty()) {
      fprintf(stderr, "FAIL: no data in result\n");
      return -1;
    }
    const auto &val = res.groups[0].second;
    sum += extractSumBigint(val, 0);
  }

  V("  SUM(CASE WHEN b<=20 THEN 1 ELSE 0 END) = %lld\n", (long long)sum);

  /* Expected: rows (1,10) and (2,20) match b<=20, so SUM=2 */
  if (sum != 2) {
    fprintf(stderr, "FAIL: expected SUM=2, got %lld\n", (long long)sum);
    return -1;
  }

  printf("PASS: Test — SUM(CASE WHEN b<=20 THEN 1 ELSE 0 END) = 2\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static const char *connectString = nullptr;
static int mysqlPort = 3306;

int main(int argc, char **argv)
{
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s [options]\n\n"
             "Options:\n"
             "  -c <connect_string>  NDB management server connect string\n"
             "  -m <mysql_port>      MySQL server port (default: 3306)\n"
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

  ndb_init();

  int result = 0;
  V("Connecting to cluster: %s\n", connectString);

  {
    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      ndb_end(0);
      return 1;
    }
    V("Connected to MySQL on port %d\n", mysqlPort);

    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      mysql_close(conn);
      ndb_end(0);
      return 1;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30 seconds\n");
      mysql_close(conn);
      ndb_end(0);
      return 1;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
      mysql_close(conn);
      ndb_end(0);
      return 1;
    }

    TableMeta meta;
    if (createTestTable(conn, &ndb, meta) != 0) {
      mysql_close(conn); ndb_end(0); return 1;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); ndb_end(0); return 1;
    }
    if (insertTestData(&ndb) != 0) {
      mysql_close(conn); ndb_end(0); return 1;
    }

    {
      SignalSender ss(&con);
      ss.lock();
      V("SignalSender: block=%u node=%u ref=0x%08x\n",
        refToBlock(ss.getOwnRef()),
        refToNode(ss.getOwnRef()),
        ss.getOwnRef());

      if (testCaseSum(&ndb, ss, meta) != 0) result = 1;

      ss.unlock();
    }

    dropTestTable(conn);
    mysql_close(conn);
  }

  ndb_end(0);

  if (result == 0) {
    printf("\n*** ALL TESTS PASSED ***\n");
  } else {
    printf("\n*** SOME TESTS FAILED ***\n");
  }

  return result;
}
