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
 * Section 5.4, identity validation on keyed peer signals.  Each case
 * builds a CTE state on every data node (CTE-mode SETUP, fragment scans,
 * COMPLETE with the per-node key triples) and forges the peer signals a
 * node can receive from a stale or hostile sender after its pool slot
 * was reused: every one names the state's real pool key.
 *
 *   ID-1  JOIN_AGG_REDISTRIBUTE_REQ with a wrong identWord, then with a
 *         wrong transid: REDISTRIBUTE_REF 1251 echoing the sender's own
 *         key, identWord and transid; the state still reaches CTE_READY
 *         and a scan returns every group.
 *   ID-2  JOIN_AGG_REDISTRIBUTE_CONF with a wrong transid, then with a
 *         wrong identWord: ignored (an accepted CONF would resume a
 *         redistribution this state never started); the state
 *         completes normally.
 *   ID-3  JOIN_AGG_REDISTRIBUTE_REF with a wrong identWord: ignored, the
 *         state completes normally.  Control: the same REF with the
 *         correct identity aborts the state, whose COMPLETE then answers
 *         COMPLETE_REF 1251.
 *   ID-4  JOIN_AGG_FINAL_REP with a wrong identWord, then with a wrong
 *         transid, each with success and error reports: ignored, the
 *         state completes normally. Accepting an error report aborts
 *         the state even with only one data node.
 *   ID-5  CTE_SCAN_REQ continuation tokens: a token no record ever had,
 *         the live token of a paused scan used from another API node, the
 *         token released by EndOfData and a token released by a close
 *         request all answer CTE_SCAN_REF 1251; the legitimate
 *         continuations in between return every group; 2362 clean.
 *   ID-6  CTE_SCAN_REQ, CTE_LOOKUP_REQ and JOIN_AGG_NULL_ROW_REQ whose
 *         coordinator reference names DBTC on a node that is down: REF 286
 *         before any state access; the same requests naming DBTC on a
 *         live data node and an unknown key answer 1251 instead.
 *   ID-7  (not run) a shorter-than-required signal asserts in the
 *         receiver by contract; it would crash the node.
 *   ID-8  a CTE COMPLETE without its per-node key section: COMPLETE_REF
 *         1251, and the state fails with it (ERROR, peers told), so a
 *         late REDISTRIBUTE_REQ with the correct identity answers
 *         REDISTRIBUTE_REF 1251 instead of a CONF and a queued group.
 *
 * Section 6, parking.  A consumer signal that arrives before its SETUP
 * parks on an identity placeholder; the SETUP's flush replays it, the 10
 * ms sweeper aborts it when no SETUP comes.  Error insert 5138 holds
 * selected SETUPs and the sweepers until cleared, DUMP 2365 reports the
 * parks per GSN class.  The NDB API queries (CteQueryUtil.hpp shapes,
 * tables created through MySQL) run on a second thread so the test can
 * watch the parks and release the hold.
 *
 *   PK-1  every parkable GSN parks and replays: (a) an identity-addressed
 *         COMPLETE, a redistributed row and a FINAL_REP parked by hand
 *         before the SETUP, then flushed: the COMPLETE streams its
 *         result, the row appears in the CTE scan; (b) LQHKEYREQ feeds
 *         (LookupMain, CTE0's SETUP held), SCAN_FRAGREQ feeds
 *         (ScanAggMain, main SETUP held), LQHKEYREQ + NULL_ROW_REQ
 *         (OuterAggMain, main SETUP held); each query's result is
 *         correct after the release.
 *   PK-2  identity-addressed COMPLETE: 8310 (SETUP_CONF delayed) and
 *         8311 (RNIL key forced) each resolve; result correct.
 *   PK-3  5139 rejects one SETUP after 200 ms: the unfilled identity
 *         is swept; the delayed REF lets DBTC finish cleanup.  The query
 *         fails with the temporary 1274 (a retry would succeed).
 *   PK-4  5148 caps the park pool at 4 while holding the SETUPs: the
 *         fifth consumer takes the resource-error path and the query
 *         fails with the temporary 1275; the four parked are replayed
 *         after the release into the aborting request; pools clean.
 *   PK-5  5149 refuses every SETUP as if the identity table were full:
 *         the query fails with OutOfQueryMemory (20008).
 *   PK-6  8313 delays a SETUP_CONF addressed to an absent DBTC scan:
 *         require the hold and stale-reclaim events, then clean pools.
 *   PK-7  a second SETUP with the same identity is refused with
 *         InvalidRequest (20002); identity-addressed COMPLETE must
 *         still return the first state's scanned groups.
 *   PK-8  pending (parked replay after coordinator death).
 *   PK-9  5138 holds the main SETUP of ScanAggMain (no CTE) until its
 *         consumer scans park; then the SETUP runs and 8315 fails the scan
 *         right before DBTC accounts its CONF, which meets the cancelled
 *         round with the fragment closes in flight: DBTC must keep the
 *         key for the scan's teardown (JOIN_AGG_SETUP_CONF_DEFERRED)
 *         instead of a stale-SETUP reclaim that frees the state under the
 *         replayed consumers; the query fails with 270; pools clean.
 *
 * After every case the leak-check DUMPs 2361 (join-agg states), 2362
 * (CTE scan iterator records) and 2363 (identity table, placeholders and
 * park records) run on every node;
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
#include "../src/ndbapi/SignalSender.hpp"
#include <kernel/BlockNumbers.h>
#include <kernel/GlobalSignalNumbers.h>
#include <kernel/RefConvert.hpp>
#include <kernel/NodeBitmask.hpp>
#include <kernel/signaldata/JoinAgg.hpp>
#include <kernel/signaldata/ScanFrag.hpp>
#include <kernel/signaldata/TransIdAI.hpp>
#include <kernel/signaldata/DumpStateOrd.hpp>
#include <kernel/signaldata/CteScan.hpp>
#include <kernel/signaldata/CteLookup.hpp>
#include <kernel/AttributeHeader.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include <NdbRestarter.hpp>
#include "JoinAggTestUtil.hpp"
#include <CteQueryUtil.hpp>
#include <thread>
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
static const Uint32 ZJOIN_AGG_STATE_NOT_FOUND = 1251;
/* Temporary (TR): a race or a resource shortage a retry survives. */
static const Uint32 ZJOIN_AGG_SETUP_NOT_RECEIVED = 1274;
static const Uint32 ZJOIN_AGG_PARK_POOL_EXHAUSTED = 1275;

/* Section 5.4.  The CTE identity every keyed peer signal must carry:
 * JoinAggregationState::packIdentWord(queryTag, cteId, 0) in the kernel. */
static const Uint32 IDENT_CTE_MAIN = 0x7F;
static const Uint32 CTE_INDEX = 0;
static const Uint32 CTE_SCAN_BATCH = 4;
/* The forged sender's own state key, echoed in REDISTRIBUTE_CONF / REF. */
static const Uint32 FORGED_SENDER_KEY = 0x5EED;
/* A pool index no state can have: rejected before the pool is touched. */
static const Uint32 UNKNOWN_STATE_KEY = 0x00FFFFFE;
/* A continuation token no iterator record ever had. */
static const Uint32 UNKNOWN_SCAN_TOKEN = 0x3FFFFFFF;

static Uint32
packIdentWord(Uint32 queryTag, Uint32 cteId)
{
  const Uint32 cte7 = (cteId == RNIL) ? IDENT_CTE_MAIN : cteId;
  return (queryTag & 0xFFFF) | (cte7 << 16);
}

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
  Uint32 senderRef;            /* 0 = the SignalSender; PK-6 uses DBTC */
  Uint32 senderData;
  bool fullLength;             /* send setupNodes (RONDB-1120 length) */
  NdbNodeBitmask setupNodes;   /* only with fullLength */
  SetupParams()
      : requestId(FAKE_REQUEST_ID), queryTag(FAKE_REQUEST_ID),
        strategy(JoinAggSetupReq::STRATEGY_MUTEX_FREE), cteIndex(RNIL),
        senderRef(0), senderData(FAKE_SENDER_DATA), fullLength(false) {
    transid[0] = FAKE_TRANS_ID1;
    transid[1] = FAKE_TRANS_ID2;
    setupNodes.clear();
  }
};

/* Send the SETUP_REQ without waiting (the reply may interleave with
 * replies to signals parked before it). */
static int
sendSetupRaw(SignalSender &ss, Uint32 nodeId,
             const std::vector<Uint32> &aggProgram, const TableMeta &meta,
             const SetupParams &p)
{
  SimpleSignal ssig;
  JoinAggSetupReq *req =
      reinterpret_cast<JoinAggSetupReq *>(ssig.getDataPtrSend());
  req->senderRef = p.senderRef != 0 ? p.senderRef : ss.getOwnRef();
  req->senderData = p.senderData;
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
  return 0;
}

/* 0 = CONF (keyOut, ownerOut set), 1 = REF (refCodeOut set), -1 = error */
static int
sendSetupReq(SignalSender &ss, Uint32 nodeId,
             const std::vector<Uint32> &aggProgram, const TableMeta &meta,
             const SetupParams &p, Uint32 &keyOut, Uint32 &ownerOut,
             Uint32 &refCodeOut)
{
  if (sendSetupRaw(ss, nodeId, aggProgram, meta, p) != 0) return -1;
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
  req->identWord = aggStateKey == RNIL
                       ? packIdentWord(FAKE_REQUEST_ID, RNIL) : RNIL;
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

/* The DBLQH leak checks (JoinAggTestUtil.hpp): 2361 join-agg states,
 * 2362 CTE scan iterator records, 2363 identity table and park records.
 * A RELEASE's teardown runs as a CONTINUEB chain after its CONF; the
 * settle time lets it drain before the dumps run. */
static int
checkLeaks(SignalSender &ss, NdbRestarter &restarter, const char *label)
{
  return joinAggCheckLeaks(&ss, restarter, label, JOIN_AGG_LQH_LEAK_DUMPS,
                           NDB_ARRAY_SIZE(JOIN_AGG_LQH_LEAK_DUMPS),
                           TEARDOWN_SETTLE_MS, WAIT_TIMEOUT_MS);
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
  Ndb_cluster_connection *con2;  /* another API node id; null if none */
  Ndb *ndbq;                     /* NDB API queries, on their own thread */
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

/* The lowest node id below 48 that is not a data node.  It was never
 * started, so every DBLQH host record keeps it ZNODE_DOWN (the same
 * test the coordinator-failure paths use) and no SETUP owner list may
 * contain it.  0 if none. */
static Uint32
pickNonDataNodeId(NdbRestarter &restarter, NdbNodeBitmask *dataNodesOut)
{
  NdbNodeBitmask dataNodes;
  for (int i = 0; i < restarter.getNumDbNodes(); i++) {
    dataNodes.set((Uint32)restarter.getDbNodeId(i));
  }
  if (dataNodesOut != nullptr) *dataNodesOut = dataNodes;
  for (Uint32 n = 1; n < 48; n++) {
    if (!dataNodes.get(n)) return n;
  }
  fprintf(stderr, "no free node id below 48\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Section 5.4: a CTE state on every data node, then forged peers       */
/* ------------------------------------------------------------------ */

struct CteNode {
  Uint32 node;
  Uint32 key;
  Uint32 owner;   /* owner LDM instance from SETUP_CONF */
};

struct PeerIdentity {
  Uint32 identWord;
  Uint32 transid[2];
};

static PeerIdentity
cteIdentity()
{
  PeerIdentity id = {packIdentWord(FAKE_REQUEST_ID, CTE_INDEX),
                     {FAKE_TRANS_ID1, FAKE_TRANS_ID2}};
  return id;
}

/* CTE-mode SETUP on every data node.  The legacy length makes each node
 * take the connected data nodes as the owner list, so every node needs a
 * state before any of them may COMPLETE. */
static int
cteSetupAll(Ctx &c, std::vector<CteNode> &nodes)
{
  nodes.clear();
  for (int i = 0; i < c.restarter.getNumDbNodes(); i++) {
    CteNode n = {(Uint32)c.restarter.getDbNodeId(i), RNIL, 0};
    SetupParams p;
    p.strategy |= JoinAggSetupReq::CTE_MODE_FLAG;
    p.cteIndex = CTE_INDEX;
    Uint32 refCode = 0;
    if (sendSetupReq(c.ss, n.node, c.prog, c.meta, p, n.key, n.owner,
                     refCode) != 0) {
      fprintf(stderr, "CTE SETUP on node %u not accepted (code=%u)\n",
              n.node, refCode);
      return -1;
    }
    nodes.push_back(n);
  }
  return 0;
}

static int
cteScanFragments(Ctx &c, const std::vector<CteNode> &nodes, Uint32 &rowsOut)
{
  rowsOut = 0;
  for (const CteNode &n : nodes) {
    Uint32 rows = 0;
    if (runScans(c.ss, c.meta, n.node, n.key, rows) != 0) return -1;
    rowsOut += rows;
  }
  return 0;
}

static int
cteSetupAndScan(Ctx &c, std::vector<CteNode> &nodes, Uint32 &rowsOut)
{
  if (cteSetupAll(c, nodes) != 0) return -1;
  return cteScanFragments(c, nodes, rowsOut);
}

/* COMPLETE every node with the per-node key triples, then collect one
 * reply per node: in CTE mode a COMPLETE_CONF without result rows once
 * the state is CTE_READY.  With expectRefCode != 0 every node must
 * answer COMPLETE_REF and refNode must report that code. Validate each
 * reply's owner reference and echoed request fields; no participant may
 * supply a second reply in place of another participant's first. */
static int
cteCompleteAll(Ctx &c, const std::vector<CteNode> &nodes,
               Uint32 expectRefCode, Uint32 refNode)
{
  std::vector<Uint32> triples;
  std::map<Uint32, bool> replied;
  for (const CteNode &n : nodes) {
    replied.emplace(numberToRef(DBLQH, n.owner, n.node), false);
    triples.push_back(n.node);
    triples.push_back(n.key);
    triples.push_back(n.owner);
  }
  for (const CteNode &n : nodes) {
    SimpleSignal ssig;
    JoinAggCompleteReq *req =
        reinterpret_cast<JoinAggCompleteReq *>(ssig.getDataPtrSend());
    req->senderRef = c.ss.getOwnRef();
    req->senderData = FAKE_SENDER_DATA;
    req->requestId = FAKE_REQUEST_ID;
    req->transid[0] = FAKE_TRANS_ID1;
    req->transid[1] = FAKE_TRANS_ID2;
    req->aggStateKey = n.key;
    req->maxBatchRows = 1000;
    req->heartbeatScanFragPtrI = RNIL;
    req->identWord = RNIL;  /* keyed form */
    ssig.set(c.ss, 0, numberToBlock(DBLQH, n.owner),
             GSN_JOIN_AGG_COMPLETE_REQ, JoinAggCompleteReq::SignalLength);
    ssig.header.m_noOfSections = 1;
    ssig.ptr[0].p = triples.data();
    ssig.ptr[0].sz = (Uint32)triples.size();
    if (c.ss.sendSignal(n.node, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal CTE COMPLETE_REQ failed\n");
      return -1;
    }
  }
  bool refNodeSeen = false;
  for (size_t i = 0; i < nodes.size(); i++) {
    SimpleSignal *resp = waitForSignal(c.ss, "CTE COMPLETE reply");
    if (resp == nullptr) return -1;
    const int gsn = getGsn(resp);
    if (gsn != GSN_JOIN_AGG_COMPLETE_CONF &&
        gsn != GSN_JOIN_AGG_COMPLETE_REF) {
      fprintf(stderr, "Unexpected GSN %d waiting for a CTE COMPLETE reply\n",
              gsn);
      return -1;
    }
    const Uint32 expectedLength = gsn == GSN_JOIN_AGG_COMPLETE_CONF
        ? JoinAggCompleteConf::SignalLength : JoinAggCompleteRef::SignalLength;
    if (resp->getLength() < expectedLength) {
      fprintf(stderr, "Short CTE COMPLETE reply: GSN %d length %u\n",
              gsn, resp->getLength());
      return -1;
    }
    /* CONF and REF share the senderRef, senderData, requestId header. */
    const Uint32 *data = resp->getDataPtr();
    const Uint32 senderRef = data[0];
    auto participant = replied.find(senderRef);
    if (participant == replied.end()) {
      fprintf(stderr, "CTE COMPLETE reply from unexpected owner 0x%x\n",
              senderRef);
      return -1;
    }
    if (data[1] != FAKE_SENDER_DATA || data[2] != FAKE_REQUEST_ID) {
      fprintf(stderr, "CTE COMPLETE reply from 0x%x has senderData=%u "
                      "requestId=%u\n", senderRef, data[1], data[2]);
      return -1;
    }
    if (participant->second) {
      fprintf(stderr, "Duplicate CTE COMPLETE reply from owner 0x%x\n",
              senderRef);
      return -1;
    }
    participant->second = true;
    if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
      if (expectRefCode != 0) {
        fprintf(stderr, "COMPLETE_CONF from a state that should be "
                        "aborted\n");
        return -1;
      }
      continue;
    }
    if (gsn == GSN_JOIN_AGG_COMPLETE_REF) {
      const JoinAggCompleteRef *ref =
          reinterpret_cast<const JoinAggCompleteRef *>(resp->getDataPtr());
      const Uint32 from = refToNode(ref->senderRef);
      if (expectRefCode == 0) {
        fprintf(stderr, "COMPLETE_REF from node %u: errorCode=%u "
                        "errorLine=%u\n",
                from, ref->errorCode, ref->errorLine);
        return -1;
      }
      V("COMPLETE_REF from node %u: errorCode=%u\n", from, ref->errorCode);
      if (from == refNode) {
        refNodeSeen = true;
        if (ref->errorCode != expectRefCode) {
          fprintf(stderr, "node %u answered COMPLETE_REF %u, expected %u\n",
                  from, ref->errorCode, expectRefCode);
          return -1;
        }
      }
      continue;
    }
  }
  if (expectRefCode != 0 && !refNodeSeen) {
    fprintf(stderr, "no COMPLETE_REF from node %u\n", refNode);
    return -1;
  }
  return 0;
}

static int
cteReleaseAll(Ctx &c, const std::vector<CteNode> &nodes)
{
  int rc = 0;
  for (const CteNode &n : nodes) {
    if (sendReleaseReq(c.ss, n.node, n.key, FAKE_REQUEST_ID, FAKE_TRANS_ID1,
                       FAKE_TRANS_ID2, 0) != 0 ||
        waitReleaseConf(c.ss, "CTE RELEASE_CONF") != 0)
      rc = -1;
  }
  return rc;
}

/* CTE_SCAN_REQ without AttrInfo: DBLQH emits every group as a raw
 * TRANSID_AI to senderRef, the nested-CTE feed form, so the rows are
 * counted without parsing.  The first request has no token; a
 * continuation carries the token of the CONF that paused; a close
 * request carries CloseFlag. */
struct CteScanParams {
  Uint32 node;
  Uint32 instance;       /* DBLQH instance: the owner LDM */
  Uint32 key;
  Uint32 batchSize;
  Uint32 scanIterI;
  Uint32 flags;
  Uint32 coordinatorRef;
  Uint32 senderRef;
  CteScanParams(const CteNode &n, SignalSender &ss)
      : node(n.node), instance(n.owner), key(n.key),
        batchSize(CTE_SCAN_BATCH), scanIterI(RNIL), flags(0),
        coordinatorRef(ss.getOwnRef()), senderRef(ss.getOwnRef()) {}
};

static int
sendCteScanReq(SignalSender &ss, const CteScanParams &p)
{
  SimpleSignal ssig;
  CteScanReq *req = reinterpret_cast<CteScanReq *>(ssig.getDataPtrSend());
  req->senderRef = p.senderRef;
  req->senderData = FAKE_SENDER_DATA;
  req->aggStateKey = p.key;
  req->transId1 = FAKE_TRANS_ID1;
  req->transId2 = FAKE_TRANS_ID2;
  req->batchSize = p.batchSize;
  req->resultRef = p.senderRef;
  req->resultData = FAKE_SENDER_DATA;
  req->joinAggStateKey = RNIL;
  req->coordinatorRef = p.coordinatorRef;
  req->scanIterI = p.scanIterI;
  req->flags = p.flags;
  Uint32 len = CteScanReq::SignalLength;
  if (p.flags != 0) {
    len = CteScanReq::SignalLengthClose;
  } else if (p.scanIterI != RNIL) {
    len = CteScanReq::SignalLengthContinue;
  }
  ssig.set(ss, 0, numberToBlock(DBLQH, p.instance), GSN_CTE_SCAN_REQ, len);
  if (ss.sendSignal(p.node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal CTE_SCAN_REQ failed\n");
    return -1;
  }
  return 0;
}

/* Rows precede the reply.  0 = CONF, 1 = REF, -1 = error. */
static int
waitCteScanReply(SignalSender &ss, const char *context, Uint32 &rows,
                 CteScanConf &conf, Uint32 &refCode)
{
  rows = 0;
  for (;;) {
    SimpleSignal *resp = waitForSignal(ss, context);
    if (resp == nullptr) return -1;
    const int gsn = getGsn(resp);
    if (gsn == GSN_TRANSID_AI) {
      rows++;
    } else if (gsn == GSN_CTE_SCAN_CONF) {
      conf = *reinterpret_cast<const CteScanConf *>(resp->getDataPtr());
      return 0;
    } else if (gsn == GSN_CTE_SCAN_REF) {
      refCode =
          reinterpret_cast<const CteScanRef *>(resp->getDataPtr())->errorCode;
      return 1;
    } else {
      fprintf(stderr, "Unexpected GSN %d waiting for %s\n", gsn, context);
      return -1;
    }
  }
}

/* One batch that must CONF; p.scanIterI becomes the CONF's token. */
static int
cteScanBatch(SignalSender &ss, CteScanParams &p, const char *context,
             Uint32 &rows, bool &endOfData)
{
  if (sendCteScanReq(ss, p) != 0) return -1;
  CteScanConf conf = {};
  Uint32 refCode = 0;
  const int rc = waitCteScanReply(ss, context, rows, conf, refCode);
  if (rc != 0) {
    if (rc == 1) {
      fprintf(stderr, "%s: CTE_SCAN_REF errorCode=%u\n", context, refCode);
    }
    return -1;
  }
  endOfData = (conf.flags & CteScanConf::EndOfData) != 0;
  if (endOfData != (conf.scanIterI == RNIL) || conf.numRows != rows) {
    fprintf(stderr, "%s: CONF flags=%u scanIterI=%u numRows=%u after %u "
                    "rows\n", context, conf.flags, conf.scanIterI,
            conf.numRows, rows);
    return -1;
  }
  p.scanIterI = conf.scanIterI;
  return 0;
}

/* A forged request that must answer CTE_SCAN_REF with the given code
 * and no rows. */
static int
cteScanExpectRef(SignalSender &ss, const CteScanParams &p,
                 const char *context, Uint32 expected)
{
  if (sendCteScanReq(ss, p) != 0) return -1;
  Uint32 rows = 0, refCode = 0;
  CteScanConf conf = {};
  const int rc = waitCteScanReply(ss, context, rows, conf, refCode);
  if (rc < 0) return -1;
  if (rc == 0) {
    fprintf(stderr, "%s: answered CONF (numRows=%u flags=%u), expected "
                    "REF %u\n", context, conf.numRows, conf.flags, expected);
    return -1;
  }
  if (refCode != expected || rows != 0) {
    fprintf(stderr, "%s: REF %u after %u rows, expected REF %u\n", context,
            refCode, rows, expected);
    return -1;
  }
  V("%s: REF %u as expected\n", context, expected);
  return 0;
}

/* Every group on every listed node, scanned to EndOfData. */
static int
cteScanGroups(Ctx &c, const std::vector<CteNode> &nodes, Uint32 &groupsOut)
{
  groupsOut = 0;
  for (const CteNode &n : nodes) {
    CteScanParams p(n, c.ss);
    p.batchSize = 1000;
    bool endOfData = false;
    while (!endOfData) {
      Uint32 rows = 0;
      if (cteScanBatch(c.ss, p, "CTE_SCAN", rows, endOfData) != 0) return -1;
      groupsOut += rows;
    }
  }
  return 0;
}

/* The state must still complete: COMPLETE_CONF from every node, then a
 * scan returning every group. */
static int
cteCompleteAndVerify(Ctx &c, const std::vector<CteNode> &nodes,
                     Uint32 expectedGroups, const char *label)
{
  if (cteCompleteAll(c, nodes, 0, 0) != 0) {
    fprintf(stderr, "%s: the state did not reach CTE_READY\n", label);
    return -1;
  }
  Uint32 groups = 0;
  if (cteScanGroups(c, nodes, groups) != 0) return -1;
  if (groups != expectedGroups) {
    fprintf(stderr, "%s: the scan returned %u groups, expected %u\n", label,
            groups, expectedGroups);
    return -1;
  }
  return 0;
}

/* Forged owner-plane peer signals, all naming the state's real key and
 * addressed to its owner LDM, the way a peer node sends them. */
static int
sendRedistributeReq(SignalSender &ss, const CteNode &n,
                    const PeerIdentity &id)
{
  SimpleSignal ssig;
  JoinAggRedistributeReq *req =
      reinterpret_cast<JoinAggRedistributeReq *>(ssig.getDataPtrSend());
  req->aggStateKey = n.key;
  req->senderAggStateKey = FORGED_SENDER_KEY;
  req->keyLen = 4;
  req->valueLen = 4;
  req->requestInfo = JoinAggRedistributeReq::RI_NEED_CONF;
  req->identWord = id.identWord;
  req->transid[0] = id.transid[0];
  req->transid[1] = id.transid[1];
  req->senderRef = ss.getOwnRef();
  ssig.set(ss, 0, numberToBlock(DBLQH, n.owner),
           GSN_JOIN_AGG_REDISTRIBUTE_REQ,
           JoinAggRedistributeReq::SignalLength);
  /* Never parsed: the identity check precedes the section reads. */
  Uint32 keyWord = 0, valueWord = 0;
  ssig.header.m_noOfSections = 2;
  ssig.ptr[JoinAggRedistributeReq::KeySectionNum].p = &keyWord;
  ssig.ptr[JoinAggRedistributeReq::KeySectionNum].sz = 1;
  ssig.ptr[JoinAggRedistributeReq::ValueSectionNum].p = &valueWord;
  ssig.ptr[JoinAggRedistributeReq::ValueSectionNum].sz = 1;
  if (ss.sendSignal(n.node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal REDISTRIBUTE_REQ failed\n");
    return -1;
  }
  return 0;
}

static int
expectRedistributeRef(SignalSender &ss, const CteNode &n,
                      const PeerIdentity &id, const char *context)
{
  SimpleSignal *resp = waitForSignal(ss, context);
  if (resp == nullptr) return -1;
  const int gsn = getGsn(resp);
  if (gsn != GSN_JOIN_AGG_REDISTRIBUTE_REF) {
    fprintf(stderr, "%s: got GSN %d, expected REDISTRIBUTE_REF\n", context,
            gsn);
    return -1;
  }
  const JoinAggRedistributeRef *ref =
      reinterpret_cast<const JoinAggRedistributeRef *>(resp->getDataPtr());
  if (ref->errorCode != ZJOIN_AGG_STATE_NOT_FOUND ||
      ref->aggStateKey != n.key || ref->senderNodeId != n.node ||
      ref->senderAggStateKey != FORGED_SENDER_KEY ||
      ref->identWord != id.identWord ||
      ref->transid[0] != id.transid[0] ||
      ref->transid[1] != id.transid[1]) {
    fprintf(stderr, "%s: REF errorCode=%u key=%u node=%u senderKey=0x%x "
                    "identWord=0x%x transid=0x%x/0x%x\n",
            context, ref->errorCode, ref->aggStateKey, ref->senderNodeId,
            ref->senderAggStateKey, ref->identWord, ref->transid[0],
            ref->transid[1]);
    return -1;
  }
  V("%s: REF %u echoing the sender's words\n", context, ref->errorCode);
  return 0;
}

static int
sendRedistributeConf(SignalSender &ss, const CteNode &n,
                     const PeerIdentity &id)
{
  SimpleSignal ssig;
  JoinAggRedistributeConf *conf =
      reinterpret_cast<JoinAggRedistributeConf *>(ssig.getDataPtrSend());
  conf->aggStateKey = FORGED_SENDER_KEY;
  conf->senderNodeId = n.node;
  conf->senderAggStateKey = n.key;  /* the state an accepted CONF resumes */
  conf->identWord = id.identWord;
  conf->transid[0] = id.transid[0];
  conf->transid[1] = id.transid[1];
  ssig.set(ss, 0, numberToBlock(DBLQH, n.owner),
           GSN_JOIN_AGG_REDISTRIBUTE_CONF,
           JoinAggRedistributeConf::SignalLength);
  if (ss.sendSignal(n.node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal REDISTRIBUTE_CONF failed\n");
    return -1;
  }
  return 0;
}

static int
sendRedistributeRef(SignalSender &ss, const CteNode &n,
                    const PeerIdentity &id)
{
  SimpleSignal ssig;
  JoinAggRedistributeRef *ref =
      reinterpret_cast<JoinAggRedistributeRef *>(ssig.getDataPtrSend());
  ref->aggStateKey = FORGED_SENDER_KEY;
  ref->senderNodeId = n.node;
  ref->errorCode = ZJOIN_AGG_STATE_NOT_FOUND;
  ref->senderAggStateKey = n.key;  /* the state an accepted REF aborts */
  ref->identWord = id.identWord;
  ref->transid[0] = id.transid[0];
  ref->transid[1] = id.transid[1];
  ssig.set(ss, 0, numberToBlock(DBLQH, n.owner),
           GSN_JOIN_AGG_REDISTRIBUTE_REF,
           JoinAggRedistributeRef::SignalLength);
  if (ss.sendSignal(n.node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal REDISTRIBUTE_REF failed\n");
    return -1;
  }
  return 0;
}

static int
sendFinalRep(SignalSender &ss, const CteNode &n, const PeerIdentity &id,
             Uint32 senderNodeId, Uint32 errorCode)
{
  SimpleSignal ssig;
  JoinAggFinalRep *rep =
      reinterpret_cast<JoinAggFinalRep *>(ssig.getDataPtrSend());
  rep->aggStateKey = n.key;
  rep->senderNodeId = senderNodeId;
  rep->identWord = id.identWord;
  rep->transid[0] = id.transid[0];
  rep->transid[1] = id.transid[1];
  rep->redistributeCountLo = 0;
  rep->redistributeCountHi = 0;
  rep->errorCode = errorCode;
  ssig.set(ss, 0, numberToBlock(DBLQH, n.owner), GSN_JOIN_AGG_FINAL_REP,
           JoinAggFinalRep::SignalLength);
  if (ss.sendSignal(n.node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal FINAL_REP failed\n");
    return -1;
  }
  return 0;
}

/* Consumer requests for ID-6: the coordinator check precedes every
 * state access, so no state and no AttrInfo are needed. */
static int
sendCteLookupReq(SignalSender &ss, Uint32 node, Uint32 instance, Uint32 key,
                 Uint32 coordinatorRef)
{
  SimpleSignal ssig;
  CteLookupReq *req = reinterpret_cast<CteLookupReq *>(ssig.getDataPtrSend());
  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->aggStateKey = key;
  req->keyLen = 8;
  req->resultRef = ss.getOwnRef();
  req->resultData = FAKE_SENDER_DATA;
  req->routeRef = coordinatorRef;
  req->correlation = FAKE_SENDER_DATA;
  req->joinAggStateKey = RNIL;
  req->flags = 0;
  ssig.set(ss, 0, numberToBlock(DBLQH, instance), GSN_CTE_LOOKUP_REQ,
           CteLookupReq::SignalLength);
  Uint32 keySection[3];
  AttributeHeader::init(&keySection[0], 0, 8);
  keySection[1] = 1;
  keySection[2] = 0;
  ssig.header.m_noOfSections = 1;
  ssig.ptr[CteLookupReq::KeySectionNum].p = keySection;
  ssig.ptr[CteLookupReq::KeySectionNum].sz = 3;
  if (ss.sendSignal(node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal CTE_LOOKUP_REQ failed\n");
    return -1;
  }
  return 0;
}

static int
sendNullRowReq(SignalSender &ss, Uint32 node, Uint32 instance, Uint32 key,
               Uint32 coordinatorRef)
{
  SimpleSignal ssig;
  JoinAggNullRowReq *req =
      reinterpret_cast<JoinAggNullRowReq *>(ssig.getDataPtrSend());
  req->senderRef = ss.getOwnRef();
  req->aggStateKey = key;
  req->transId[0] = FAKE_TRANS_ID1;
  req->transId[1] = FAKE_TRANS_ID2;
  req->requestPtrI = FAKE_SENDER_DATA;
  req->treeNodePtrI = FAKE_SENDER_DATA + 1;
  req->identWord = RNIL;
  req->coordinatorRef = coordinatorRef;
  ssig.set(ss, 0, numberToBlock(DBLQH, instance), GSN_JOIN_AGG_NULL_ROW_REQ,
           JoinAggNullRowReq::SignalLength);
  if (ss.sendSignal(node, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal NULL_ROW_REQ failed\n");
    return -1;
  }
  return 0;
}

/* One REF of the given GSN with the expected error code and echo. */
static int
expectRef(SignalSender &ss, int gsn, Uint32 expected, const char *context)
{
  SimpleSignal *resp = waitForSignal(ss, context);
  if (resp == nullptr) return -1;
  const int got = getGsn(resp);
  if (got != gsn) {
    fprintf(stderr, "%s: got GSN %d, expected GSN %d\n", context, got, gsn);
    return -1;
  }
  Uint32 code = 0;
  bool echoOk = true;
  if (gsn == GSN_CTE_LOOKUP_REF) {
    const CteLookupRef *ref =
        reinterpret_cast<const CteLookupRef *>(resp->getDataPtr());
    code = ref->errorCode;
    echoOk = ref->senderData == FAKE_SENDER_DATA &&
             ref->correlation == FAKE_SENDER_DATA;
  } else if (gsn == GSN_JOIN_AGG_NULL_ROW_REF) {
    const JoinAggNullRowRef *ref =
        reinterpret_cast<const JoinAggNullRowRef *>(resp->getDataPtr());
    code = ref->errorCode;
    echoOk = ref->requestPtrI == FAKE_SENDER_DATA &&
             ref->treeNodePtrI == FAKE_SENDER_DATA + 1;
  } else if (gsn == GSN_JOIN_AGG_COMPLETE_REF) {
    const JoinAggCompleteRef *ref =
        reinterpret_cast<const JoinAggCompleteRef *>(resp->getDataPtr());
    code = ref->errorCode;
    echoOk = ref->senderData == FAKE_SENDER_DATA &&
             ref->requestId == FAKE_REQUEST_ID;
  } else {
    fprintf(stderr, "%s: unsupported GSN %d\n", context, gsn);
    return -1;
  }
  if (code != expected || !echoOk) {
    fprintf(stderr, "%s: REF %u (echo %s), expected REF %u\n", context, code,
            echoOk ? "ok" : "wrong", expected);
    return -1;
  }
  V("%s: REF %u as expected\n", context, expected);
  return 0;
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
  const Uint32 badNode = pickNonDataNodeId(c.restarter, &dataNodes);
  if (badNode == 0) return -1;
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

/* ID-1: a REDISTRIBUTE_REQ naming the state's key under another identity
 * is refused with a REF echoing the sender's own words; the state is
 * untouched. */
static int
id1(Ctx &c)
{
  std::vector<CteNode> nodes;
  Uint32 rows = 0;
  if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
  const CteNode &target = nodes[0];
  PeerIdentity forged = cteIdentity();
  forged.identWord = packIdentWord(FAKE_REQUEST_ID, CTE_INDEX + 1);
  if (sendRedistributeReq(c.ss, target, forged) != 0 ||
      expectRedistributeRef(c.ss, target, forged, "ID-1 wrong identWord") !=
          0)
    return -1;
  forged = cteIdentity();
  forged.transid[1] ^= 1;
  if (sendRedistributeReq(c.ss, target, forged) != 0 ||
      expectRedistributeRef(c.ss, target, forged, "ID-1 wrong transid") != 0)
    return -1;
  if (cteCompleteAndVerify(c, nodes, rows, "ID-1") != 0) return -1;
  if (cteReleaseAll(c, nodes) != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "ID-1");
}

/* ID-2: a REDISTRIBUTE_CONF for the state's key under another identity is
 * ignored.  An accepted CONF would resume a redistribution this state
 * never started; the COMPLETE that follows from the same sender runs
 * after the forgeries on the same owner LDM. */
static int
id2(Ctx &c)
{
  std::vector<CteNode> nodes;
  Uint32 rows = 0;
  if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
  PeerIdentity forged = cteIdentity();
  forged.transid[0] ^= 0x10;
  if (sendRedistributeConf(c.ss, nodes[0], forged) != 0) return -1;
  forged = cteIdentity();
  forged.identWord = packIdentWord(FAKE_REQUEST_ID + 1, CTE_INDEX);
  if (sendRedistributeConf(c.ss, nodes[0], forged) != 0) return -1;
  if (cteCompleteAndVerify(c, nodes, rows, "ID-2") != 0) return -1;
  if (cteReleaseAll(c, nodes) != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "ID-2");
}

/* ID-3: a REDISTRIBUTE_REF under another identity is ignored and the state
 * completes; the control shows that the same REF with the right identity
 * does abort it (COMPLETE answers COMPLETE_REF 1251). */
static int
id3(Ctx &c)
{
  {
    std::vector<CteNode> nodes;
    Uint32 rows = 0;
    if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
    PeerIdentity forged = cteIdentity();
    forged.identWord = packIdentWord(FAKE_REQUEST_ID + 1, CTE_INDEX);
    if (sendRedistributeRef(c.ss, nodes[0], forged) != 0) return -1;
    if (cteCompleteAndVerify(c, nodes, rows, "ID-3 forged REF") != 0)
      return -1;
    if (cteReleaseAll(c, nodes) != 0) return -1;
  }
  {
    std::vector<CteNode> nodes;
    Uint32 rows = 0;
    if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
    if (sendRedistributeRef(c.ss, nodes[0], cteIdentity()) != 0) return -1;
    if (cteCompleteAll(c, nodes, ZJOIN_AGG_STATE_NOT_FOUND, nodes[0].node) !=
        0) {
      fprintf(stderr, "ID-3 control: a REF with the correct identity did "
                      "not abort the state\n");
      return -1;
    }
    if (cteReleaseAll(c, nodes) != 0) return -1;
  }
  return checkLeaks(c.ss, c.restarter, "ID-3");
}

/* ID-4: success and error FINAL_REPs under another identity are ignored.
 * An accepted error report aborts even a single-node state, whose READY
 * check does not depend on the local node's successful FINAL bit. */
static int
id4(Ctx &c)
{
  std::vector<CteNode> nodes;
  Uint32 rows = 0;
  if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
  const Uint32 peer = nodes.size() > 1 ? nodes[1].node : nodes[0].node;
  PeerIdentity forged = cteIdentity();
  forged.identWord = packIdentWord(FAKE_REQUEST_ID, CTE_INDEX + 1);
  if (sendFinalRep(c.ss, nodes[0], forged, peer, 0) != 0 ||
      sendFinalRep(c.ss, nodes[0], forged, peer,
                    ZJOIN_AGG_STATE_NOT_FOUND) != 0)
    return -1;
  forged = cteIdentity();
  forged.transid[0] ^= 0x10;
  if (sendFinalRep(c.ss, nodes[0], forged, peer, 0) != 0 ||
      sendFinalRep(c.ss, nodes[0], forged, peer,
                    ZJOIN_AGG_STATE_NOT_FOUND) != 0)
    return -1;
  if (cteCompleteAndVerify(c, nodes, rows, "ID-4") != 0) return -1;
  if (cteReleaseAll(c, nodes) != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "ID-4");
}

/* ID-5: continuation tokens.  A paused scan's token is bound to the
 * requesting node and freed on EndOfData or close; anything else answers
 * CTE_SCAN_REF 1251 without touching the record. */
static int
id5(Ctx &c)
{
  std::vector<CteNode> nodes;
  Uint32 rows = 0;
  if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
  if (cteCompleteAll(c, nodes, 0, 0) != 0) return -1;
  const CteNode &src = nodes[0];

  CteScanParams live(src, c.ss);
  live.batchSize = 1;
  bool endOfData = false;
  Uint32 batchRows = 0;
  if (cteScanBatch(c.ss, live, "ID-5 first batch", batchRows, endOfData) != 0)
    return -1;
  if (endOfData) {
    fprintf(stderr, "ID-5: node %u holds too few groups to pause a scan\n",
            src.node);
    return -1;
  }
  Uint32 groups = batchRows;
  const Uint32 token = live.scanIterI;

  {
    CteScanParams p = live;
    p.scanIterI = UNKNOWN_SCAN_TOKEN;
    if (cteScanExpectRef(c.ss, p, "ID-5 unknown token",
                         ZJOIN_AGG_STATE_NOT_FOUND) != 0)
      return -1;
  }
  if (c.con2 != nullptr) {
    /* The live token presented by another API node. */
    ScopedSenderUnlock unlockOwner(c.ss);
    SignalSender other(c.con2);
    other.lock();
    CteScanParams p(src, other);
    p.scanIterI = token;
    const int rc = cteScanExpectRef(other, p, "ID-5 token from another node",
                                    ZJOIN_AGG_STATE_NOT_FOUND);
    other.unlock();
    if (rc != 0) return -1;
  } else {
    fprintf(stderr, "ID-5: [SKIPPED] no second API node for the "
                    "wrong-node token\n");
  }
  /* The legitimate continuations still drain the scan. */
  live.batchSize = CTE_SCAN_BATCH;
  while (!endOfData) {
    if (cteScanBatch(c.ss, live, "ID-5 continuation", batchRows,
                     endOfData) != 0)
      return -1;
    groups += batchRows;
  }
  {
    std::vector<CteNode> rest(nodes.begin() + 1, nodes.end());
    Uint32 restGroups = 0;
    if (cteScanGroups(c, rest, restGroups) != 0) return -1;
    groups += restGroups;
  }
  if (groups != rows) {
    fprintf(stderr, "ID-5: %u groups scanned, expected %u\n", groups, rows);
    return -1;
  }
  {
    CteScanParams p = live;
    p.scanIterI = token;
    if (cteScanExpectRef(c.ss, p, "ID-5 token released by EndOfData",
                         ZJOIN_AGG_STATE_NOT_FOUND) != 0)
      return -1;
  }
  {
    /* A second paused scan, released by a close request. */
    CteScanParams p(src, c.ss);
    p.batchSize = 1;
    if (cteScanBatch(c.ss, p, "ID-5 second scan", batchRows, endOfData) != 0)
      return -1;
    if (endOfData) {
      fprintf(stderr, "ID-5: the second scan did not pause\n");
      return -1;
    }
    const Uint32 token2 = p.scanIterI;
    CteScanParams close = p;
    close.flags = CteScanReq::CloseFlag;
    if (sendCteScanReq(c.ss, close) != 0) return -1;
    CteScanConf conf = {};
    Uint32 refCode = 0;
    const int rc = waitCteScanReply(c.ss, "ID-5 close", batchRows, conf,
                                    refCode);
    if (rc != 0 || batchRows != 0 || conf.numRows != 0 ||
        (conf.flags & CteScanConf::EndOfData) == 0 || conf.scanIterI != RNIL) {
      fprintf(stderr, "ID-5 close: rc=%d rows=%u numRows=%u flags=%u "
                      "scanIterI=%u refCode=%u\n",
              rc, batchRows, conf.numRows, conf.flags, conf.scanIterI,
              refCode);
      return -1;
    }
    p.scanIterI = token2;
    if (cteScanExpectRef(c.ss, p, "ID-5 token released by close",
                         ZJOIN_AGG_STATE_NOT_FOUND) != 0)
      return -1;
  }
  if (cteReleaseAll(c, nodes) != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "ID-5");
}

/* ID-6: a consumer request whose coordinator is DBTC on a node that is
 * down is refused with 286 before any state access; with DBTC on a live
 * data node the same request reaches the state lookup (1251). */
static int
id6(Ctx &c)
{
  const Uint32 node = (Uint32)c.restarter.getDbNodeId(0);
  const Uint32 downNode = pickNonDataNodeId(c.restarter, nullptr);
  if (downNode == 0) return -1;
  const Uint32 downTc = numberToRef(DBTC, downNode);
  /* An API reference bypasses the node-status check entirely. */
  const Uint32 liveTc = numberToRef(DBTC, node);
  V("ID-6: coordinator reference 0x%08x names node %u\n", downTc, downNode);
  {
    CteNode n = {node, UNKNOWN_STATE_KEY, 1};
    CteScanParams p(n, c.ss);
    p.coordinatorRef = downTc;
    if (cteScanExpectRef(c.ss, p, "ID-6 CTE_SCAN_REQ, coordinator down",
                         ZNODEFAIL_BEFORE_COMMIT) != 0)
      return -1;
    p.coordinatorRef = liveTc;
    if (cteScanExpectRef(c.ss, p, "ID-6 CTE_SCAN_REQ, coordinator live",
                         ZJOIN_AGG_STATE_NOT_FOUND) != 0)
      return -1;
  }
  if (sendCteLookupReq(c.ss, node, 1, UNKNOWN_STATE_KEY, downTc) != 0 ||
      expectRef(c.ss, GSN_CTE_LOOKUP_REF, ZNODEFAIL_BEFORE_COMMIT,
                "ID-6 CTE_LOOKUP_REQ, coordinator down") != 0)
    return -1;
  if (sendCteLookupReq(c.ss, node, 1, UNKNOWN_STATE_KEY, liveTc) != 0 ||
      expectRef(c.ss, GSN_CTE_LOOKUP_REF, ZJOIN_AGG_STATE_NOT_FOUND,
                "ID-6 CTE_LOOKUP_REQ, coordinator live") != 0)
    return -1;
  if (sendNullRowReq(c.ss, node, 1, UNKNOWN_STATE_KEY, downTc) != 0 ||
      expectRef(c.ss, GSN_JOIN_AGG_NULL_ROW_REF, ZNODEFAIL_BEFORE_COMMIT,
                "ID-6 NULL_ROW_REQ, coordinator down") != 0)
    return -1;
  if (sendNullRowReq(c.ss, node, 1, UNKNOWN_STATE_KEY, liveTc) != 0 ||
      expectRef(c.ss, GSN_JOIN_AGG_NULL_ROW_REF, ZJOIN_AGG_STATE_NOT_FOUND,
                "ID-6 NULL_ROW_REQ, coordinator live") != 0)
    return -1;
  return checkLeaks(c.ss, c.restarter, "ID-6");
}

/* ID-8: a CTE COMPLETE refused for its missing per-node key section
 * fails the state, not only the request.  A peer's group sent before
 * that peer aborted may arrive afterwards with the correct identity: it
 * is REFed with the state's recorded cause (1251), not acknowledged and
 * queued into a state its coordinator is about to release (the rule at
 * DblqhProxy::execJOIN_AGG_RELEASE_REQ). */
static int
id8(Ctx &c)
{
  std::vector<CteNode> nodes;
  Uint32 rows = 0;
  if (cteSetupAndScan(c, nodes, rows) != 0) return -1;
  const CteNode &target = nodes[0];
  {
    SimpleSignal ssig;
    JoinAggCompleteReq *req =
        reinterpret_cast<JoinAggCompleteReq *>(ssig.getDataPtrSend());
    req->senderRef = c.ss.getOwnRef();
    req->senderData = FAKE_SENDER_DATA;
    req->requestId = FAKE_REQUEST_ID;
    req->transid[0] = FAKE_TRANS_ID1;
    req->transid[1] = FAKE_TRANS_ID2;
    req->aggStateKey = target.key;
    req->maxBatchRows = 1000;
    req->heartbeatScanFragPtrI = RNIL;
    req->identWord = RNIL;  /* keyed form, but no key section */
    ssig.set(c.ss, 0, numberToBlock(DBLQH, target.owner),
             GSN_JOIN_AGG_COMPLETE_REQ, JoinAggCompleteReq::SignalLength);
    if (c.ss.sendSignal(target.node, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal CTE COMPLETE_REQ failed\n");
      return -1;
    }
  }
  if (expectRef(c.ss, GSN_JOIN_AGG_COMPLETE_REF, ZJOIN_AGG_STATE_NOT_FOUND,
                "ID-8 COMPLETE without its key section") != 0)
    return -1;
  /* Before the fix the state stayed SETUP_COMPLETE: this answered
   * REDISTRIBUTE_CONF and queued the group. */
  if (sendRedistributeReq(c.ss, target, cteIdentity()) != 0 ||
      expectRedistributeRef(c.ss, target, cteIdentity(),
                            "ID-8 late group to the failed state") != 0)
    return -1;
  /* The failed owner told its peers (error FINAL_REP by identity); let
   * those reach their states before releasing them. */
  NdbSleep_MilliSleep(TEARDOWN_SETTLE_MS);
  if (cteReleaseAll(c, nodes) != 0) return -1;
  return checkLeaks(c.ss, c.restarter, "ID-8");
}

/* ------------------------------------------------------------------ */
/* Section 6: parking                                                  */
/* ------------------------------------------------------------------ */

static const Uint32 NF_ROWS = 64;
static const Uint32 NF_GROUPS = 8;
/* DUMP 2365 classes, in the event's order. */
enum ParkClass {
  PARK_LQHKEY = 0, PARK_SCANFRAG, PARK_NULLROW, PARK_COMPLETE, PARK_REDIST,
  PARK_FINAL, PARK_CLASSES
};
static const char *const PARK_NAMES[PARK_CLASSES] = {
    "LQHKEYREQ", "SCAN_FRAGREQ", "NULL_ROW_REQ", "COMPLETE_REQ",
    "REDISTRIBUTE_REQ", "FINAL_REP"};
/* Error insert 5138 extra values: which SETUPs to hold. */
static const Uint32 HOLD_CTE0_SETUP = 1;
static const Uint32 HOLD_MAIN_SETUP = 0xFFFF;
static const Uint32 HOLD_NO_SETUP = 0xFFFE;   /* sweeper hold only */
static const Uint32 DBSPJ_INVALID_REQUEST = 20002;
static const Uint32 DBSPJ_OUT_OF_QUERY_MEMORY = 20008;
static const Uint32 PARK_POLL_MS = 50;
static const Uint32 PARK_WAIT_MS = 2000;

/* The CTE query tables, created through MySQL (CteQueryUtil::createTables
 * would use the NDB API, which is not allowed beside live mysqlds). */
static int
createCteNfTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_nf_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_nf_virtual");
  if (sqlExec(conn,
              "CREATE TABLE cte_nf_src ("
              "  pk INT NOT NULL PRIMARY KEY,"
              "  grp INT NOT NULL,"
              "  val BIGINT NOT NULL,"
              "  nk INT NULL"
              ") ENGINE=NDB") != 0)
    return -1;
  return sqlExec(conn,
                 "CREATE TABLE cte_nf_virtual ("
                 "  grp INT NOT NULL PRIMARY KEY,"
                 "  total BIGINT NOT NULL"
                 ") ENGINE=NDB");
}

static void
dropCteNfTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS cte_nf_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_nf_virtual");
}

struct ParkStats {
  Uint32 counts[PARK_CLASSES];
  Uint32 inUse;
};

/* DUMP 2365 on c.node, answered by regular instance 1 with the cookie. */
static int
readParkStats(Ctx &c, ParkStats &s)
{
  ScopedSenderUnlock unlock(c.ss);
  ProtocolEventListener events;
  events.timeoutMs = WAIT_TIMEOUT_MS;
  if (!events.open(c.restarter)) return -1;
  static Uint32 nextCookie = 0x50000;
  const Uint32 cookie = ++nextCookie;
  const int dump[] = {DumpStateOrd::LqhDumpJoinAggParkStats, (int)cookie};
  ndb_mgm_reply reply = {};
  if (ndb_mgm_dump_state(c.restarter.handle, (int)c.node, dump, 2,
                         &reply) == -1 ||
      reply.return_code != 0) {
    fprintf(stderr, "DUMP 2365 on node %u failed\n", c.node);
    return -1;
  }
  char marker[64];
  snprintf(marker, sizeof(marker), "cookie=%u]", cookie);
  if (!events.waitFor(marker)) return -1;
  const char *p = strstr(events.matched, "[JOIN_AGG_PARK_STATS");
  Uint32 node = 0;
  if (p == nullptr ||
      sscanf(p, "[JOIN_AGG_PARK_STATS node=%u lqhkey=%u scanfrag=%u "
                "nullrow=%u complete=%u redist=%u final=%u inuse=%u",
             &node, &s.counts[0], &s.counts[1], &s.counts[2], &s.counts[3],
             &s.counts[4], &s.counts[5], &s.inUse) != 8) {
    fprintf(stderr, "Unparsable park statistics: %s\n", events.matched);
    return -1;
  }
  return 0;
}

/* Poll until every class in `mask` grew past `base` on c.node. */
static int
waitParked(Ctx &c, const ParkStats &base, Uint32 mask, ParkStats &now,
           const char *label)
{
  const Uint64 start = NdbTick_CurrentMillisecond();
  for (;;) {
    if (readParkStats(c, now) != 0) return -1;
    bool all = true;
    for (Uint32 k = 0; k < PARK_CLASSES; k++) {
      if ((mask & (1u << k)) != 0 && now.counts[k] <= base.counts[k]) {
        all = false;
      }
    }
    if (all) {
      V("%s: parked", label);
      for (Uint32 k = 0; k < PARK_CLASSES; k++) {
        V(" %s=%u", PARK_NAMES[k], now.counts[k] - base.counts[k]);
      }
      V(" inuse=%u\n", now.inUse);
      return 0;
    }
    if (NdbTick_CurrentMillisecond() - start > PARK_WAIT_MS) {
      fprintf(stderr, "%s: not parked within %u ms:", label, PARK_WAIT_MS);
      for (Uint32 k = 0; k < PARK_CLASSES; k++) {
        if ((mask & (1u << k)) != 0 && now.counts[k] <= base.counts[k]) {
          fprintf(stderr, " %s", PARK_NAMES[k]);
        }
      }
      fprintf(stderr, "\n");
      return -1;
    }
    NdbSleep_MilliSleep(PARK_POLL_MS);
  }
}

/* Poll until at least `inUse` park records are held on c.node. */
static int
waitParkRecords(Ctx &c, Uint32 inUse, ParkStats &now, const char *label)
{
  const Uint64 start = NdbTick_CurrentMillisecond();
  for (;;) {
    if (readParkStats(c, now) != 0) return -1;
    if (now.inUse >= inUse) return 0;
    if (NdbTick_CurrentMillisecond() - start > PARK_WAIT_MS) {
      fprintf(stderr, "%s: only %u park records in use after %u ms, "
                      "expected %u\n", label, now.inUse, PARK_WAIT_MS, inUse);
      return -1;
    }
    NdbSleep_MilliSleep(PARK_POLL_MS);
  }
}

/* An NDB API CTE query on its own thread and Ndb object. */
struct QueryRun {
  Ndb *ndb;
  SignalSender &sender;
  CteQueryUtil::Options opt;
  CteQueryUtil::Result res;
  int rc;
  std::thread thread;
  QueryRun(Ndb *n, SignalSender &s) : ndb(n), sender(s), rc(-3) {}
  void start() {
    thread = std::thread([this] {
      rc = CteQueryUtil::runQuery(ndb, opt, res);
    });
  }
  void join() {
    if (thread.joinable()) {
      // Broadcast delivery may need this client's mutex while the query
      // thread polls. Holding it across join can also stop API heartbeats.
      ScopedSenderUnlock unlock(sender);
      thread.join();
    }
  }
  ~QueryRun() { join(); }
};

static int
checkQueryResult(const QueryRun &q, const char *label)
{
  if (q.rc != 0) {
    fprintf(stderr, "%s: query failed at %s (rc=%d ndbError=%d)\n", label,
            q.res.failedAt, q.rc, q.res.ndbError);
    return -1;
  }
  if (!CteQueryUtil::resultMatches(q.opt.shape, q.res, NF_ROWS, NF_GROUPS)) {
    fprintf(stderr, "%s: wrong result: rows=%llu count=%lld sum=%lld\n",
            label, (unsigned long long)q.res.rows, (long long)q.res.aggCount,
            (long long)q.res.aggSum);
    return -1;
  }
  return 0;
}

static void
setQueryOptions(Ctx &c, QueryRun &q, CteQueryUtil::Shape shape)
{
  q.opt.shape = shape;
  q.opt.tcNodeId = c.node;
  q.opt.crossNodeLeaf = c.restarter.getNumDbNodes() > 1;
}

/* Hold the SETUPs selected by holdSel on c.node, run the query, require
 * the masked classes to park, release, verify the result. */
static int
runParkedQuery(Ctx &c, CteQueryUtil::Shape shape, Uint32 holdSel,
               Uint32 mask, const char *label)
{
  ParkStats base, now;
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 5138, (int)holdSel)) return -1;
  if (readParkStats(c, base) != 0) return -1;
  QueryRun q(c.ndbq, c.ss);
  setQueryOptions(c, q, shape);
  q.start();
  int rc = waitParked(c, base, mask, now, label);
  // Let SETUP run while sweepers remain held. Clearing both together
  // races their independent timers and can turn replay into an abort.
  if (!setErrorInsert(c.restarter, c.node, 5138, (int)HOLD_NO_SETUP)) {
    (void)guard.clear();
    rc = -1;
  }
  q.join();
  if (!guard.clear()) rc = -1;
  // A prepare/execute failure also prevents parking. Report it even
  // when the parking wait failed, while preserving that wait failure.
  if (checkQueryResult(q, label) != 0) rc = -1;
  return rc;
}

/* First row of a CTE scan in its raw form: [AH(0,8) grp][AH(1,8) sum]. */
static int
cteScanOneRow(SignalSender &ss, const CteScanParams &p, Int64 &grp,
              Int64 &sum, Uint32 &rows)
{
  if (sendCteScanReq(ss, p) != 0) return -1;
  rows = 0;
  bool haveRow = false;
  for (;;) {
    SimpleSignal *resp = waitForSignal(ss, "CTE_SCAN row");
    if (resp == nullptr) return -1;
    const int gsn = getGsn(resp);
    if (gsn == GSN_TRANSID_AI) {
      rows++;
      if (!haveRow) {
        const Uint32 *data;
        Uint32 len;
        if (resp->header.m_noOfSections > 0) {
          data = resp->ptr[0].p;
          len = resp->ptr[0].sz;
        } else {
          data = resp->getDataPtr() + TransIdAI::HeaderLength;
          len = resp->getLength() - TransIdAI::HeaderLength;
        }
        if (len < 6) {
          fprintf(stderr, "CTE scan row too short (%u words)\n", len);
          return -1;
        }
        memcpy(&grp, &data[1], sizeof(grp));
        memcpy(&sum, &data[4], sizeof(sum));
        haveRow = true;
      }
    } else if (gsn == GSN_CTE_SCAN_CONF) {
      const CteScanConf *conf =
          reinterpret_cast<const CteScanConf *>(resp->getDataPtr());
      if ((conf->flags & CteScanConf::EndOfData) == 0) {
        fprintf(stderr, "CTE scan paused, expected EndOfData\n");
        return -1;
      }
      return 0;
    } else if (gsn == GSN_CTE_SCAN_REF) {
      fprintf(stderr, "CTE_SCAN_REF %u\n",
              reinterpret_cast<const CteScanRef *>(resp->getDataPtr())
                  ->errorCode);
      return -1;
    } else {
      fprintf(stderr, "Unexpected GSN %d during the CTE scan\n", gsn);
      return -1;
    }
  }
}

/* PK-1 (a): the owner-plane signals park by hand and replay. */
static int
pk1OwnerPlane(Ctx &c)
{
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 5138, (int)HOLD_NO_SETUP))
    return -1;
  ParkStats base, now;

  /* (i) A non-CTE state: the identity-addressed COMPLETE parks; the
   * SETUP flushes it and its result stream follows the SETUP_CONF in
   * either order. */
  if (readParkStats(c, base) != 0) return -1;
  {
    SimpleSignal ssig;
    JoinAggCompleteReq *req =
        reinterpret_cast<JoinAggCompleteReq *>(ssig.getDataPtrSend());
    req->senderRef = c.ss.getOwnRef();
    req->senderData = FAKE_SENDER_DATA;
    req->requestId = FAKE_REQUEST_ID;
    req->transid[0] = FAKE_TRANS_ID1;
    req->transid[1] = FAKE_TRANS_ID2;
    req->aggStateKey = RNIL;
    req->maxBatchRows = 1000;
    req->heartbeatScanFragPtrI = RNIL;
    req->identWord = packIdentWord(FAKE_REQUEST_ID, RNIL);
    ssig.set(c.ss, 0, numberToBlock(DBLQH, 1), GSN_JOIN_AGG_COMPLETE_REQ,
             JoinAggCompleteReq::SignalLength);
    if (c.ss.sendSignal(c.node, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal COMPLETE_REQ failed\n");
      return -1;
    }
  }
  if (waitParked(c, base, 1u << PARK_COMPLETE, now, "PK-1 COMPLETE") != 0)
    return -1;
  {
    SetupParams p;
    if (sendSetupRaw(c.ss, c.node, c.prog, c.meta, p) != 0) return -1;
    Uint32 key = RNIL, groups = 0;
    bool haveSetup = false, haveComplete = false;
    while (!(haveSetup && haveComplete)) {
      SimpleSignal *resp = waitForSignal(c.ss, "SETUP_CONF / COMPLETE_CONF");
      if (resp == nullptr) return -1;
      const int gsn = getGsn(resp);
      if (gsn == GSN_JOIN_AGG_SETUP_CONF) {
        key = reinterpret_cast<const JoinAggSetupConf *>(resp->getDataPtr())
                  ->aggStateKey;
        haveSetup = true;
      } else if (gsn == GSN_JOIN_AGG_COMPLETE_CONF) {
        haveComplete = true;
      } else if (gsn == GSN_JOIN_AGG_SEND_REQ) {
        if (sendSendConf(c.ss, reinterpret_cast<const JoinAggSendReq *>(
                                   resp->getDataPtr())) != 0)
          return -1;
      } else if (gsn == GSN_TRANSID_AI) {
        const Uint32 *data = resp->header.m_noOfSections > 0
                                 ? resp->ptr[0].p
                                 : resp->getDataPtr() + TransIdAI::HeaderLength;
        groups += data[2];
      } else {
        fprintf(stderr, "PK-1: unexpected GSN %d after the SETUP\n", gsn);
        return -1;
      }
    }
    if (groups != 0) {
      fprintf(stderr, "PK-1: the replayed COMPLETE returned %u groups\n",
              groups);
      return -1;
    }
    if (releaseAndWait(c, key) != 0) return -1;
  }

  /* (ii) Redistribution needs multiple CTE participants. Single-node
   * COMPLETE deliberately skips the redistribution queue. The parked
   * COMPLETE case above still runs on a single-node cluster. */
  if (c.restarter.getNumDbNodes() < 2) {
    fprintf(stderr, "PK-1a: [SKIPPED] parked redistribution needs "
                    "at least two data nodes\n");
    if (!guard.clear()) return -1;
    return checkLeaks(c.ss, c.restarter, "PK-1a");
  }

  /* A synthetic API-origin row and a FINAL_REP park; SETUP flushes
   * them. Complete every real participant so redistribution can move
   * the merged group to its hash owner, then scan all owners. */
  const Int64 REDIST_GRP = 7, REDIST_SUM = 4242;
  if (readParkStats(c, base) != 0) return -1;
  {
    const PeerIdentity id = cteIdentity();
    SimpleSignal ssig;
    JoinAggRedistributeReq *req =
        reinterpret_cast<JoinAggRedistributeReq *>(ssig.getDataPtrSend());
    req->aggStateKey = RNIL;
    req->senderAggStateKey = FORGED_SENDER_KEY;
    req->keyLen = 12;
    req->valueLen = sizeof(AggResItem);
    req->requestInfo = 0;
    req->identWord = id.identWord;
    req->transid[0] = id.transid[0];
    req->transid[1] = id.transid[1];
    req->senderRef = c.ss.getOwnRef();
    ssig.set(c.ss, 0, numberToBlock(DBLQH, 1), GSN_JOIN_AGG_REDISTRIBUTE_REQ,
             JoinAggRedistributeReq::SignalLength);
    /* The group key as stored: one AttributeHeader-led BIGINT column. */
    Uint32 keyWords[3];
    AttributeHeader::init(&keyWords[0], 0, 8);
    memcpy(&keyWords[1], &REDIST_GRP, sizeof(REDIST_GRP));
    AggResItem acc;
    memset(&acc, 0, sizeof(acc));
    acc.type = COL_TYPE_BIGINT;
    acc.value.val_int64 = REDIST_SUM;
    Uint32 valueWords[(sizeof(AggResItem) + 3) / 4];
    memset(valueWords, 0, sizeof(valueWords));
    memcpy(valueWords, &acc, sizeof(acc));
    ssig.header.m_noOfSections = 2;
    ssig.ptr[JoinAggRedistributeReq::KeySectionNum].p = keyWords;
    ssig.ptr[JoinAggRedistributeReq::KeySectionNum].sz = 3;
    ssig.ptr[JoinAggRedistributeReq::ValueSectionNum].p = valueWords;
    ssig.ptr[JoinAggRedistributeReq::ValueSectionNum].sz =
        NDB_ARRAY_SIZE(valueWords);
    if (c.ss.sendSignal(c.node, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal REDISTRIBUTE_REQ failed\n");
      return -1;
    }
    SimpleSignal fsig;
    JoinAggFinalRep *rep =
        reinterpret_cast<JoinAggFinalRep *>(fsig.getDataPtrSend());
    rep->aggStateKey = RNIL;
    rep->senderNodeId = c.node;
    rep->identWord = id.identWord;
    rep->transid[0] = id.transid[0];
    rep->transid[1] = id.transid[1];
    rep->redistributeCountLo = 0;
    rep->redistributeCountHi = 0;
    rep->errorCode = 0;
    fsig.set(c.ss, 0, numberToBlock(DBLQH, 1), GSN_JOIN_AGG_FINAL_REP,
             JoinAggFinalRep::SignalLength);
    if (c.ss.sendSignal(c.node, &fsig) != SEND_OK) {
      fprintf(stderr, "sendSignal FINAL_REP failed\n");
      return -1;
    }
  }
  if (waitParked(c, base, (1u << PARK_REDIST) | (1u << PARK_FINAL), now,
                 "PK-1 REDISTRIBUTE/FINAL") != 0)
    return -1;
  {
    std::vector<CteNode> nodes;
    if (cteSetupAll(c, nodes) != 0) return -1;
    if (cteCompleteAll(c, nodes, 0, 0) != 0) return -1;
    Uint32 totalRows = 0;
    bool correct = true;
    for (const CteNode &n : nodes) {
      CteScanParams sp(n, c.ss);
      sp.batchSize = 1000;
      Int64 grp = 0, sum = 0;
      Uint32 rows = 0;
      if (cteScanOneRow(c.ss, sp, grp, sum, rows) != 0) return -1;
      totalRows += rows;
      if (rows != 0 &&
          (rows != 1 || grp != REDIST_GRP || sum != REDIST_SUM)) {
        fprintf(stderr, "PK-1: unexpected row on node %u: rows=%u "
                        "grp=%lld sum=%lld\n", n.node, rows,
                        (long long)grp, (long long)sum);
        correct = false;
      }
    }
    if (cteReleaseAll(c, nodes) != 0) return -1;
    if (totalRows != 1 || !correct) {
      fprintf(stderr, "PK-1: expected the replayed group exactly once, "
                      "got %u rows\n", totalRows);
      return -1;
    }
  }
  if (!guard.clear()) return -1;
  return checkLeaks(c.ss, c.restarter, "PK-1a");
}

/* PK-1 (b): consumer feeds park under a held SETUP and replay. */
static int
pk1Feeds(Ctx &c)
{
  if (runParkedQuery(c, CteQueryUtil::LookupMain, HOLD_CTE0_SETUP,
                     1u << PARK_LQHKEY, "PK-1 LookupMain") != 0)
    return -1;
  if (runParkedQuery(c, CteQueryUtil::ScanAggMain, HOLD_MAIN_SETUP,
                     1u << PARK_SCANFRAG, "PK-1 ScanAggMain") != 0)
    return -1;
  if (runParkedQuery(c, CteQueryUtil::OuterAggMain, HOLD_MAIN_SETUP,
                     (1u << PARK_LQHKEY) | (1u << PARK_NULLROW),
                     "PK-1 OuterAggMain") != 0)
    return -1;
  return checkLeaks(c.ss, c.restarter, "PK-1b");
}

/* PK-2: the identity-addressed COMPLETE resolves under 8310 (SETUP_CONF
 * delayed) and 8311 (RNIL key forced); the result is correct. */
static int
pk2(Ctx &c)
{
  const int inserts[] = {8310, 8311};
  for (unsigned i = 0; i < NDB_ARRAY_SIZE(inserts); i++) {
    ErrorInsertGuard guard = {c.restarter, c.node, true};
    if (!setErrorInsert(c.restarter, c.node, inserts[i], 0)) return -1;
    QueryRun q(c.ndbq, c.ss);
    setQueryOptions(c, q, CteQueryUtil::LookupMain);
    q.start();
    q.join();
    if (!guard.clear()) return -1;
    char label[32];
    snprintf(label, sizeof(label), "PK-2 insert %d", inserts[i]);
    if (checkQueryResult(q, label) != 0) return -1;
  }
  return checkLeaks(c.ss, c.restarter, "PK-2");
}

/* PK-3: 5139 leaves one identity unfilled; the sweeper REFs the
 * consumers and a delayed SETUP_REF drains DBTC's SETUP accounting.
 * Require the sweep event as well as the query's temporary 1274. */
static int
pk3(Ctx &c)
{
  ProtocolEventListener events;
  events.timeoutMs = WAIT_TIMEOUT_MS;
  if (!events.open(c.restarter)) return -1;
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 5139, 0)) return -1;
  QueryRun q(c.ndbq, c.ss);
  setQueryOptions(c, q, CteQueryUtil::LookupMain);
  q.start();
  q.join();
  if (!guard.clear()) return -1;
  if (q.rc != -1 || q.res.ndbError != (int)ZJOIN_AGG_SETUP_NOT_RECEIVED ||
      q.res.ndbErrorStatus != NdbError::TemporaryError) {
    fprintf(stderr, "PK-3: expected temporary failure %u, got rc=%d "
                    "ndbError=%d status=%d at %s (rows=%llu)\n",
            ZJOIN_AGG_SETUP_NOT_RECEIVED, q.rc, q.res.ndbError,
            (int)q.res.ndbErrorStatus, q.res.failedAt,
            (unsigned long long)q.res.rows);
    return -1;
  }
  if (q.res.queryMillis > 3000) {
    fprintf(stderr, "PK-3: the failure took %llu ms\n",
            (unsigned long long)q.res.queryMillis);
    return -1;
  }
  char marker[96];
  snprintf(marker, sizeof(marker), "[JOIN_AGG_PARK_SWEPT node=%u failed=0 ",
           c.node);
  {
    ScopedSenderUnlock unlock(c.ss);
    if (!events.waitFor(marker)) return -1;
  }
  V("PK-3: %s\n", events.matched);
  return checkLeaks(c.ss, c.restarter, "PK-3");
}

/* PK-4: 5148 holds every SETUP and caps the park pool at 4: the fifth
 * consumer takes the resource-error path, the query fails with the
 * temporary 1275, and the four parked are replayed into the aborting
 * request after the release. */
static int
pk4(Ctx &c)
{
  const Uint32 cap = 4;
  ParkStats base, now;
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 5148, (int)cap)) return -1;
  if (readParkStats(c, base) != 0) return -1;
  QueryRun q(c.ndbq, c.ss);
  setQueryOptions(c, q, CteQueryUtil::LookupMain);
  q.start();
  int rc = waitParkRecords(c, cap, now, "PK-4");
  /* Give the refused consumer's REF time to abort the request. */
  NdbSleep_MilliSleep(200);
  if (!guard.clear()) rc = -1;
  q.join();
  if (rc != 0) return -1;
  if (q.rc != -1 || q.res.ndbError != (int)ZJOIN_AGG_PARK_POOL_EXHAUSTED ||
      q.res.ndbErrorStatus != NdbError::TemporaryError) {
    fprintf(stderr, "PK-4: expected temporary failure %u, got rc=%d "
                    "ndbError=%d status=%d at %s (rows=%llu)\n",
            ZJOIN_AGG_PARK_POOL_EXHAUSTED, q.rc, q.res.ndbError,
            (int)q.res.ndbErrorStatus, q.res.failedAt,
            (unsigned long long)q.res.rows);
    return -1;
  }
  return checkLeaks(c.ss, c.restarter, "PK-4");
}

/* PK-5: 5149 refuses every SETUP as if the identity table were full. */
static int
pk5(Ctx &c)
{
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 5149, 0)) return -1;
  QueryRun q(c.ndbq, c.ss);
  setQueryOptions(c, q, CteQueryUtil::LookupMain);
  q.start();
  q.join();
  if (!guard.clear()) return -1;
  if (q.rc != -1 || q.res.ndbError != (int)DBSPJ_OUT_OF_QUERY_MEMORY) {
    fprintf(stderr, "PK-5: expected failure %u, got rc=%d ndbError=%d at %s\n",
            DBSPJ_OUT_OF_QUERY_MEMORY, q.rc, q.res.ndbError, q.res.failedAt);
    return -1;
  }
  return checkLeaks(c.ss, c.restarter, "PK-5");
}

/* PK-6: create a real state naming DBTC as coordinator but RNIL as
 * its scan record. Its delayed SETUP_CONF is necessarily stale: there
 * is no API query whose normal close could release this state. */
static int
pk6(Ctx &c)
{
  ProtocolEventListener events;
  events.timeoutMs = WAIT_TIMEOUT_MS;
  if (!events.open(c.restarter)) return -1;
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  if (!setErrorInsert(c.restarter, c.node, 8313, 0)) return -1;
  SetupParams p;
  p.senderRef = numberToRef(DBTC, 1, c.node);
  p.senderData = RNIL;
  p.requestId = FAKE_REQUEST_ID + 6;
  p.queryTag = p.requestId;
  if (sendSetupRaw(c.ss, c.node, c.prog, c.meta, p) != 0) return -1;
  {
    ScopedSenderUnlock unlock(c.ss);
    char marker[160];
    snprintf(marker, sizeof(marker),
             "[JOIN_AGG_SETUP_CONF_HELD node=%u instance=1 scan=%u "
             "request=%u ", c.node, p.senderData, p.requestId);
    if (!events.waitFor(marker)) return -1;
    snprintf(marker, sizeof(marker),
             "[JOIN_AGG_STALE_SETUP_RECLAIM node=%u instance=1 scan=%u "
             "request=%u ", c.node, p.senderData, p.requestId);
    if (!events.waitFor(marker)) return -1;
  }
  if (!guard.clear()) return -1;
  return checkLeaks(c.ss, c.restarter, "PK-6");
}

/* PK-7: a second SETUP with the same identity is refused and the first
 * state stays releasable. */
static int
pk7(Ctx &c)
{
  Uint32 key, owner;
  if (setupDefault(c, key, owner) != 0) return -1;
  SetupParams p;
  Uint32 key2 = RNIL, owner2 = 0, refCode = 0;
  const int rc = sendSetupReq(c.ss, c.node, c.prog, c.meta, p, key2, owner2,
                              refCode);
  if (rc == 0) {
    fprintf(stderr, "PK-7: a duplicate SETUP was accepted (key %u)\n", key2);
    (void)releaseAndWait(c, key2);
    (void)releaseAndWait(c, key);
    return -1;
  }
  if (rc != 1 || refCode != DBSPJ_INVALID_REQUEST) {
    fprintf(stderr, "PK-7: expected SETUP_REF %u, got rc=%d code=%u\n",
            DBSPJ_INVALID_REQUEST, rc, refCode);
    (void)releaseAndWait(c, key);
    return -1;
  }
  Uint32 rows = 0, groups = 0;
  if (runScans(c.ss, c.meta, c.node, key, rows) != 0) return -1;
  // Resolve by identity, not by the saved key: duplicate rejection must
  // preserve both the first state and its identity-table entry.
  if (sendCompleteReq(c.ss, c.node, RNIL, 1) != 0 ||
      receiveResults(c.ss, groups) != 0)
    return -1;
  if (releaseAndWait(c, key) != 0) return -1;
  if (rows == 0 || groups != rows) {
    fprintf(stderr, "PK-7: %u groups after duplicate SETUP, expected %u\n",
            groups, rows);
    return -1;
  }
  return checkLeaks(c.ss, c.restarter, "PK-7");
}

/* PK-9: 8315 fails a main aggregation (no CTE) just before DBTC accounts
 * one SETUP_CONF, so that CONF meets the cancelled SETUP round with the
 * fragment closes still in flight.  That node's consumers found the state
 * by identity and may still feed it: DBTC must keep the key for the
 * scan's teardown (JOIN_AGG_SETUP_CONF_DEFERRED), not reclaim it at once
 * (JOIN_AGG_STALE_SETUP_RECLAIM: a RELEASE that frees the programs and
 * interpreters under those consumers).  5138 holds the main SETUP until
 * the consumer scans have parked, so no fragment can finish before the
 * SETUP runs and its CONF (the one 8315 acts on) is accounted.  The query
 * fails with DBTC's scan error 270; the leak checks show the teardown
 * released the state. */
static const Uint32 DBTC_SCAN_LQH_ERROR = 270;

static int
pk9(Ctx &c)
{
  ProtocolEventListener events;
  events.timeoutMs = WAIT_TIMEOUT_MS;
  if (!events.open(c.restarter)) return -1;
  ErrorInsertGuard guard = {c.restarter, c.node, true};
  // Two blocks: 8315 arms DBTC, 5138 DBLQH (error inserts route by range).
  if (!setErrorInsert(c.restarter, c.node, 8315, 0) ||
      !setErrorInsert(c.restarter, c.node, 5138, (int)HOLD_MAIN_SETUP))
    return -1;
  ParkStats base, now;
  if (readParkStats(c, base) != 0) return -1;
  QueryRun q(c.ndbq, c.ss);
  setQueryOptions(c, q, CteQueryUtil::ScanAggMain);
  q.start();
  int rc = waitParked(c, base, 1u << PARK_SCANFRAG, now, "PK-9");
  // Let the SETUP run while the sweepers stay held (as runParkedQuery):
  // its CONF reaches DBTC while the replayed consumers start.
  if (rc == 0 &&
      !setErrorInsert(c.restarter, c.node, 5138, (int)HOLD_NO_SETUP))
    rc = -1;
  if (rc == 0) {
    ScopedSenderUnlock unlock(c.ss);
    char marker[160];
    snprintf(marker, sizeof(marker),
             "[JOIN_AGG_SETUP_CONF_AFTER_CANCEL node=%u ", c.node);
    const char *at =
        events.waitFor(marker) ? strstr(events.matched, marker) : nullptr;
    Uint32 node = 0, instance = 0, scan = 0, request = 0, key = 0;
    if (at == nullptr ||
        sscanf(at,
               "[JOIN_AGG_SETUP_CONF_AFTER_CANCEL node=%u instance=%u "
               "scan=%u request=%u key=%u]",
               &node, &instance, &scan, &request, &key) != 5) {
      fprintf(stderr, "PK-9: no valid JOIN_AGG_SETUP_CONF_AFTER_CANCEL "
                      "marker: %s\n", events.matched);
      rc = -1;
    } else {
      snprintf(marker, sizeof(marker),
               "[JOIN_AGG_SETUP_CONF_DEFERRED node=%u instance=%u scan=%u "
               "request=%u key=%u]",
               node, instance, scan, request, key);
      if (!events.waitFor(marker)) {
        fprintf(stderr, "PK-9: the CONF of key %u was not kept for the "
                        "scan's teardown while its fragments closed\n", key);
        rc = -1;
      }
    }
  }
  q.join();
  if (!guard.clear()) rc = -1;
  if (rc != 0) return -1;
  if (q.rc != -1 || q.res.ndbError != (int)DBTC_SCAN_LQH_ERROR) {
    fprintf(stderr, "PK-9: expected failure %u, got rc=%d ndbError=%d at %s\n",
            DBTC_SCAN_LQH_ERROR, q.rc, q.res.ndbError, q.res.failedAt);
    return -1;
  }
  return checkLeaks(c.ss, c.restarter, "PK-9");
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

    /* Section 6: the NDB API CTE queries run on their own Ndb object
     * from a second thread; their tables are created through MySQL. */
    if (createCteNfTables(conn) != 0 ||
        CteQueryUtil::loadTable(&ndb, NF_ROWS, NF_GROUPS) != 0) {
      fprintf(stderr, "Cannot create or load the cte_nf tables\n");
      mysql_close(conn);
      result = 1;
      break;
    }
    Ndb ndbq(&con, "test");
    if (ndbq.init() != 0) {
      fprintf(stderr, "Ndb::init failed: %s\n", ndbq.getNdbError().message);
      mysql_close(conn);
      result = 1;
      break;
    }

    /* A second API node id for ID-5 (a paused scan's token presented
     * from another node).  Optional: that sub-case is skipped without it.
     * Destroyed before ndb and con, after the SignalSender using it. */
    Ndb_cluster_connection con2(connectString);
    Ndb_cluster_connection *con2Ptr = nullptr;
    if (con2.connect(2, 1, 0) == 0 && con2.wait_until_ready(30, 0) >= 0) {
      con2Ptr = &con2;
    } else {
      fprintf(stderr, "No second API node id available\n");
    }

    {
      SignalSender ss(&con);
      ss.lock();
      Ctx c = {ss, con, restarter, meta, prog,
               (Uint32)restarter.getDbNodeId(0), con2Ptr, &ndbq};
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
          {"ID-1 REDISTRIBUTE_REQ under another identity", id1},
          {"ID-2 REDISTRIBUTE_CONF under another identity", id2},
          {"ID-3 REDISTRIBUTE_REF under another identity", id3},
          {"ID-4 FINAL_REP under another identity", id4},
          {"ID-5 CTE scan continuation tokens", id5},
          {"ID-6 consumer requests of a failed coordinator", id6},
          {"ID-8 late group after a refused COMPLETE", id8},
          {"PK-1a owner-plane signals parked and replayed", pk1OwnerPlane},
          {"PK-1b consumer feeds parked and replayed", pk1Feeds},
          {"PK-2 identity-addressed COMPLETE", pk2},
          {"PK-3 sweeper REFs after a lost SETUP", pk3},
          {"PK-4 park pool exhaustion", pk4},
          {"PK-5 identity table exhaustion", pk5},
          {"PK-6 stale SETUP_CONF reclaim", pk6},
          {"PK-7 duplicate identity refused", pk7},
          {"PK-9 SETUP_CONF after a cancel, fragments closing", pk9},
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
    dropCteNfTables(conn);
    dropTestTable(conn);
    mysql_close(conn);
  } while (0);

  const char *verdict = result == 0 ? "PASSED\n" : "FAILED\n";
  if (write(mtr_fd, verdict, strlen(verdict)) < 0) result = 1;
  ndb_end(0);
  return result;
}
