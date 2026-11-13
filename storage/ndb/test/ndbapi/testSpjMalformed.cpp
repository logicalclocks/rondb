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

/**
 * Robustness test for the DBSPJ attribute-information protocol.
 *
 * A data node must survive malformed pushed-join requests from an API
 * node: the intended response to detected malicious input is
 * reportMaliciousSignal(), which disconnects the API node, and a plain
 * REF is equally fine.  A data node that STOPS is the failure.  Each
 * test case therefore counts as PASS when, after the request, every
 * data node is still started (NdbRestarter) and the reply was a
 * REF-like signal or nothing (we were disconnected).  It counts as FAIL
 * only when a data node went down.
 *
 * The cases target the suspected weaknesses documented in
 * storage/ndb/claude_files/dbspj/dbspj_attrinfo_protocol.html (P-1 ..).
 * P-25 .. P-31 send malformed join aggregation programs, which DBLQH
 * must reject at JOIN_AGG_SETUP_REQ (AggInterpreterBase::checkProgram).
 * P-32 sends one whose rows read a register they did not load, which
 * fails each row (K-1, AggInterpreterBase m_loaded_regs).
 * Each malformed request is built by taking a known-good, word-for-word
 * SPJ encoding (the POSITIVE CONTROL below, encoded exactly as
 * NdbQueryBuilder would) and perturbing it as little as possible, so a
 * crash points at one specific protocol defect.  The positive control
 * must itself succeed (SCAN_TABCONF): without it a harness that fails
 * to reach DBSPJ at all would look just like a passing hardening test.
 *
 * Every case runs in its own process, as its own MTR test
 * (test_spj_malformed_<case>, after the positive control), because a
 * correct node disconnects us, so later cases need a fresh API
 * connection, and because MTR fails a test as soon as a data node
 * crashes, which would hide the result of later cases.
 *
 * The request is assembled with raw signals (SignalSender), modelled on
 * testTcReleaseId.cpp.  A TC API-connect record is seized with
 * TCSEIZEREQ, exactly as that test does.
 */

#include <GlobalSignalNumbers.h>
#include <BlockNumbers.h>
#include <NdbTick.h>
#include <ndb_version.h>
#include <NDBT.hpp>
#include <NDBT_Test.hpp>
#include <NdbRestarter.hpp>
#include <HugoTransactions.hpp>
#include <AttributeHeader.hpp>
#include <NdbSleep.h>
#include <mgmapi.h>
#include <signaldata/ScanTab.hpp>
#include <signaldata/TcKeyReq.hpp>
#include <signaldata/QueryTree.hpp>
#include <signaldata/DbspjErr.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include <cstring>
#include <vector>
#include "../../src/ndbapi/SignalSender.hpp"

/**
 * Fills the fixed part of a SCAN_TABREQ or TCKEYREQ.  Their members and
 * setters are private, this struct is a friend of ScanTabReq and TcKeyReq
 * (see "Test programs" there), so it must stay outside the anonymous
 * namespace.  Each returns the signal length in words.
 */
struct SpjMalformedSignals {
  static Uint32 fillScanTabReq(Uint32 *data, Uint32 apiConnectPtr,
                               Uint32 tableId, Uint32 schemaVersion,
                               Uint32 transId1, Uint32 transId2,
                               Uint32 batchRows, Uint32 batchBytes,
                               bool joinAgg, Uint32 scanParallelism) {
    /* Also zeroes the optional words, the flags leave them unused. */
    std::memset(data, 0, sizeof(Uint32) * (ScanTabReq::StaticLength + 5));
    ScanTabReq *req = (ScanTabReq *)data;
    req->apiConnectPtr = apiConnectPtr;
    req->spare = 0;
    Uint32 ri = 0;
    ScanTabReq::setViaSPJFlag(ri, 1);
    ScanTabReq::setPassAllConfsFlag(ri, 1);
    ScanTabReq::setExtendedConf(ri, 1);
    ScanTabReq::setReadCommittedFlag(ri, 1);
    ScanTabReq::setLockMode(ri, 0);
    ScanTabReq::setHoldLockFlag(ri, 0);
    ScanTabReq::setTupScanFlag(ri, 1);
    ScanTabReq::setNoDiskFlag(ri, 1);
    ScanTabReq::setScanBatch(ri, batchRows);
    if (joinAgg) ScanTabReq::setJoinAggFlag(ri, 1);
    req->requestInfo = ri;
    req->tableId = tableId;
    req->tableSchemaVersion = schemaVersion;
    req->storedProcId = 0xFFFF;
    req->transId1 = transId1;
    req->transId2 = transId2;
    req->buddyConPtr = apiConnectPtr;
    req->batch_byte_size = batchBytes;
    req->first_batch_size = batchRows;
    if (joinAgg) {
      req->scanParallelism = scanParallelism; /* DATA 15 */
      return ScanTabReq::StaticLength + 5;
    }
    return ScanTabReq::StaticLength;
  }

  /* A dirty, simple ViaSPJ read, its AttrInfo length given by section. */
  static Uint32 fillTcKeyReq(Uint32 *data, Uint32 apiConnectPtr,
                             Uint32 apiOperationPtr, Uint32 tableId,
                             Uint32 schemaVersion, Uint32 transId1,
                             Uint32 transId2) {
    std::memset(data, 0, sizeof(Uint32) * TcKeyReq::StaticLength);
    TcKeyReq *req = (TcKeyReq *)data;
    req->apiConnectPtr = apiConnectPtr;
    req->apiOperationPtr = apiOperationPtr;
    Uint32 al = 0;
    TcKeyReq::setAttrinfoLen(al, 0);
    req->attrLen = al;
    req->tableId = tableId;
    Uint32 ri = 0;
    TcKeyReq::setOperationType(ri, 0 /* ReadRequest */);
    TcKeyReq::setViaSPJFlag(ri, 1);
    TcKeyReq::setKeyLength(ri, 0);
    TcKeyReq::setAIInTcKeyReq(ri, 0);
    TcKeyReq::setInterpretedFlag(ri, 0);
    TcKeyReq::setStartFlag(ri, 1);
    TcKeyReq::setExecuteFlag(ri, 1);
    TcKeyReq::setNoDiskFlag(ri, 1);
    TcKeyReq::setDirtyFlag(ri, 1);
    TcKeyReq::setSimpleFlag(ri, 1);
    TcKeyReq::setCommitFlag(ri, 0);
    req->requestInfo = ri;
    req->tableSchemaVersion = schemaVersion;
    req->transId1 = transId1;
    req->transId2 = transId2;
    return TcKeyReq::StaticLength;
  }
};

namespace {

constexpr Uint32 ReplyMillis = 10000;  /* wait for a REF/CONF */
constexpr Uint32 SettleMillis = 3000;  /* let a crash surface before status */
constexpr int NumRows = 32;

/* A pushed-join request we can send with raw signals, built as a set of
 * word vectors so a case can perturb exactly one thing. */
struct SpjReq {
  std::vector<Uint32> tree;     /* QueryTree header + nodes */
  std::vector<Uint32> params;   /* QueryParameters nodes */
  std::vector<Uint32> keyInfo;  /* root bounds, or JoinAgg combined key */
  std::vector<Uint32> rcvIds;   /* section 0 receiver ids */
  bool viaScan{true};           /* SCAN_TABREQ (true) or TCKEYREQ (false) */
  bool joinAgg{false};
  bool withAttrInfo{true};      /* TCKEYREQ: attach the AttrInfo section */
  bool toTcProxy{false};        /* send to DBTC instance 0, not the seized one */
  /* Skip the SCAN_TABCONF of each completed fragment, wait for the
   * SCAN_TABREF or the EndOfData SCAN_TABCONF. */
  bool waitForEnd{false};
  Uint32 scanParallelism{1};
};

class SpjMalformedChecker {
 public:
  SpjMalformedChecker(SignalSender *ss, NdbRestarter *restarter,
                      const NdbDictionary::Table *tab)
      : m_ss(ss), m_restarter(restarter), m_tableId(tab->getObjectId()),
        m_schemaVersion(tab->getObjectVersion()),
        m_fragmentCount(tab->getFragmentCount()) {}

  /* ---- the AttrInfo of a well-formed single-node leaf scan ------------ *
   * Root QN_SCAN_FRAG, no parent, no key, user projection {col0,
   * CORR_FACTOR64}.  This mirrors what NdbQueryBuilder serializes for a
   * one-table pushed scan and what NdbQueryOperation serializes for its
   * parameters (QueryTree.hpp / NdbQueryOperation.cpp:6113). */
  void makeWellFormedScan(SpjReq &r) {
    r = SpjReq();
    /* tree: header + 1 node (4 header words, no optional part) */
    r.tree.resize(1 + QN_ScanFragNode::NodeSize);
    QueryNode *node = (QueryNode *)&r.tree[1];
    QueryNode::setOpLen(node->len, QueryNode::QN_SCAN_FRAG,
                        QN_ScanFragNode::NodeSize);
    node->requestInfo = 0;  /* no parent, no key, no linked attr */
    node->tableId = m_tableId;
    node->tableVersion = m_schemaVersion;
    QueryTree::setCntLen(r.tree[0], 1, (Uint32)r.tree.size());

    /* params: QN_ScanFragParameters (8 words) + PI_ATTR_LIST projection */
    const Uint32 base = QN_ScanFragParameters::NodeSize;
    r.params.resize(base);
    QN_ScanFragParameters *p = (QN_ScanFragParameters *)&r.params[0];
    p->requestInfo = DABits::PI_ATTR_LIST;
    p->requestInfo |= QN_ScanFragParameters::SFP_PARALLEL;
    p->resultData = kReceiverId;
    p->batch_size_rows = kBatchRows;
    p->batch_size_bytes = kBatchBytes;
    p->unused0 = 0;
    p->unused1 = 0;
    p->maxRows = 0;
    /* user projection: [n][AH(col0)][AH(CORR_FACTOR64)] */
    r.params.push_back(2);
    r.params.push_back(0 << 16);  /* AttributeHeader(attrId=0, size=0) */
    r.params.push_back(AttributeHeader::CORR_FACTOR64 << 16);
    QueryNodeParameters::setOpLen(((QueryNodeParameters *)&r.params[0])->len,
                                  QueryNodeParameters::QN_SCAN_FRAG,
                                  (Uint32)r.params.size());

    r.rcvIds.assign(1, kReceiverId);
    r.scanParallelism = 1;
  }

  /* A well-formed 2-node scan+lookup: root scan projects col1 as a linked
   * attr to the child, child QN_LOOKUP keys on it (NI_KEY_LINKED,
   * P_COL(0)).  Used as the base for the row-driven cases. */
  void makeWellFormedScanLookup(SpjReq &r) {
    r = SpjReq();
    std::vector<Uint32> &t = r.tree;
    t.push_back(0);  /* tree header, patched at end */
    /* node 0: root scan, NI_LINKED_ATTR (projects col1 to child) */
    {
      size_t at = t.size();
      t.resize(at + QN_ScanFragNode::NodeSize);
      QueryNode *n = (QueryNode *)&t[at];
      QueryNode::setOpLen(n->len, QueryNode::QN_SCAN_FRAG,
                          QN_ScanFragNode::NodeSize + 1);
      n->requestInfo = DABits::NI_LINKED_ATTR;
      n->tableId = m_tableId;
      n->tableVersion = m_schemaVersion;
      /* optional part 4: linked list {cnt=1, item0=attr 1}; cnt is odd so
       * the list is a single word (unpackList consumes one word). */
      t.push_back(1u | (1u << 16));
    }
    /* node 1: child lookup on the same table's PK, NI_HAS_PARENT,
     * NI_KEY_LINKED, NI_INNER_JOIN.  Key pattern P_COL(0). */
    m_childNodeTreeOffset = (Uint32)t.size();
    {
      size_t at = t.size();
      /* header(4) + parent list(1) + key header(1) + P_COL(1) = 7 */
      t.resize(at + 4);
      QueryNode *n = (QueryNode *)&t[at];
      n->requestInfo = DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
                       DABits::NI_INNER_JOIN;
      n->tableId = m_tableId;
      n->tableVersion = m_schemaVersion;
      t.push_back(1u | (0u << 16));       /* parent list: cnt=1, item0=node 0 */
      t.push_back(0x00000001);            /* key header: 0 params, length 1 */
      t.push_back(QueryPattern::col(0));  /* P_COL(0): parent linked col 0 */
      /* n may dangle after the push_backs reallocated; re-fetch. */
      QueryNode::setOpLen(((QueryNode *)&t[at])->len, QueryNode::QN_LOOKUP,
                          (Uint32)(t.size() - at));
    }
    QueryTree::setCntLen(t[0], 2, (Uint32)t.size());

    /* params node 0: scan params + projection {col0, CORR64} */
    std::vector<Uint32> &p = r.params;
    {
      size_t at = p.size();
      p.resize(at + QN_ScanFragParameters::NodeSize);
      QN_ScanFragParameters *pp = (QN_ScanFragParameters *)&p[at];
      pp->requestInfo = DABits::PI_ATTR_LIST;
      pp->requestInfo |= QN_ScanFragParameters::SFP_PARALLEL;
      pp->resultData = kReceiverId;
      pp->batch_size_rows = kBatchRows;
      pp->batch_size_bytes = kBatchBytes;
      pp->unused0 = 0;
      pp->unused1 = 0;
      pp->maxRows = 0;
      p.push_back(2);
      p.push_back(0 << 16);
      p.push_back(AttributeHeader::CORR_FACTOR64 << 16);
      QueryNodeParameters::setOpLen(((QueryNodeParameters *)&p[at])->len,
                                    QueryNodeParameters::QN_SCAN_FRAG,
                                    (Uint32)(p.size() - at));
    }
    /* params node 1: lookup params + projection {col0, CORR64} */
    {
      size_t at = p.size();
      p.resize(at + QN_LookupParameters::NodeSize);
      QN_LookupParameters *pp = (QN_LookupParameters *)&p[at];
      pp->requestInfo = DABits::PI_ATTR_LIST;
      pp->resultData = kReceiverId + 1;
      p.push_back(2);
      p.push_back(0 << 16);
      p.push_back(AttributeHeader::CORR_FACTOR64 << 16);
      QueryNodeParameters::setOpLen(((QueryNodeParameters *)&p[at])->len,
                                    QueryNodeParameters::QN_LOOKUP,
                                    (Uint32)(p.size() - at));
    }
    r.rcvIds.assign(1, kReceiverId);
    r.scanParallelism = 1;
  }

  /* ---- join aggregation programs (P-25 .. P-32) ------------------------ *
   * A JoinAgg SCAN_TABREQ carries the aggregation program in its KeyInfo
   * section, laid out as NdbQueryImpl::prepareAggregation and
   * NdbQueryImpl::doSend do: [boundsLen 0][aggReceiverId]
   * [(0x0722 << 16) | numLeaves] and per leaf [length][program].  DBTC
   * sends the programs to every data node in JOIN_AGG_SETUP_REQ, where
   * DblqhProxy checks every leaf and answers a malformed one with
   * JOIN_AGG_SETUP_REF(DbspjErr::InvalidRequest); DBTC then fails the
   * scan with SCAN_TABREF.
   *
   * The scan is the one-node scan of the control, made the aggregation
   * leaf, with one receiver per fragment (aggregation scans start every
   * fragment at once).  The program has two leaves.  Only leaf 0 runs,
   * on the table's rows: leaf 1 is checked at SETUP or never. */

  /* One leaf's program, as NdbAggregator::Finalize lays it out: the
   * 8-word header with no GROUP BY column, then body. */
  static std::vector<Uint32> aggProgram(Uint32 nAggResults,
                                        const std::vector<Uint32> &body) {
    std::vector<Uint32> p(8, 0);
    p.insert(p.end(), body.begin(), body.end());
    p[0] = (0x0721u << 16) | Uint32(p.size());
    p[1] = nAggResults;  /* (n_gb_cols << 16) | n_agg_results */
    p[2] = PUSHDOWN_AGGREGATION_VERSION;
    return p;
  }

  static Uint32 aggOp(Uint32 op) { return op << 26; }

  /* COUNT of the BIGINT constant 1: kOpLoadConst into reg (the constant
   * in two words, little-endian as int8store writes it), then kOpCount
   * of reg into slot. */
  static std::vector<Uint32> countBody(Uint32 reg, Uint32 slot) {
    return {aggOp(kOpLoadConst) | (NDB_TYPE_BIGINT << 21) | (reg << 16),
            1, 0, aggOp(kOpCount) | (reg << 16) | slot};
  }

  /* A well-formed leaf: one slot, COUNT(1). */
  static std::vector<Uint32> countLeaf() {
    return aggProgram(1, countBody(0, 0));
  }

  void makeJoinAggScan(SpjReq &r,
                       const std::vector<std::vector<Uint32>> &leaves) {
    makeWellFormedScan(r);
    ((QueryNode *)&r.tree[1])->requestInfo = DABits::NI_INNER_JOIN |
                                             DABits::NI_AGGREGATE |
                                             DABits::NI_AGGREGATE_LEAF;
    r.joinAgg = true;
    r.scanParallelism = m_fragmentCount;
    r.rcvIds.clear();
    for (Uint32 i = 0; i < m_fragmentCount; i++) {
      r.rcvIds.push_back(kReceiverId + i);
    }
    r.keyInfo.clear();
    r.keyInfo.push_back(0);  /* boundsLen */
    r.keyInfo.push_back(kAggReceiverId);
    r.keyInfo.push_back((0x0722u << 16) | Uint32(leaves.size()));
    for (const std::vector<Uint32> &leaf : leaves) {
      r.keyInfo.push_back(Uint32(leaf.size()));
      r.keyInfo.insert(r.keyInfo.end(), leaf.begin(), leaf.end());
    }
  }

  /* ---------------------- the cases ----------------------------------- */

  int runPositiveControl() {
    SpjReq r;
    makeWellFormedScan(r);
    Reply rep = sendScanTabReq(r);
    printReply("CONTROL (well-formed scan)", rep);
    if (rep.gsn != GSN_SCAN_TABCONF) {
      g_err << "POSITIVE CONTROL did not get SCAN_TABCONF (got "
            << gsnName(rep.gsn) << ", err " << rep.errorCode
            << ").  The raw SPJ encoding or the TC handshake is wrong; "
               "fix this before trusting any malformed-case result."
            << endl;
      return NDBT_FAILED;
    }
    /* Health is implied by a CONF, but check anyway. */
    return clusterHealthy() ? NDBT_OK : NDBT_FAILED;
  }

  int runP1() {  /* v1 scan-param widening underflow */
    SpjReq r;
    makeWellFormedScan(r);
    /* Make node 0 a QN_SCAN_INDEX_v1 and give its param block length 3,
     * one short of QN_ScanIndexParameters_v1::NodeSize (4): the in-place
     * widen in Dbspj::build() computes (param_len - NodeSize) and
     * underflows. */
    QueryNode *node = (QueryNode *)&r.tree[1];
    QueryNode::setOpLen(node->len, QueryNode::QN_SCAN_INDEX_v1,
                        QN_ScanFragNode::NodeSize);
    r.params.resize(3);
    QueryNodeParameters::setOpLen(((QueryNodeParameters *)&r.params[0])->len,
                                  QueryNodeParameters::QN_SCAN_INDEX_v1, 3);
    return expectSurvive("P-1 v1 param underflow", r);
  }

  int runP2() {  /* scan tableId indexes arrays before validation */
    SpjReq r;
    makeWellFormedScan(r);
    ((QueryNode *)&r.tree[1])->tableId = 0xFFFF0000u;
    return expectSurvive("P-2 bad scan tableId", r);
  }

  int runP3() {  /* QN_LOOKUP root of a scan request + huge KeyInfo ->
                  * lookup_start -> computeHash copies into a 1023-word
                  * stack buffer (P-3, reached via P-18). */
    SpjReq r;
    makeWellFormedScan(r);
    /* Node 0 becomes a QN_LOOKUP (same 4-word header), no NI_KEY_* bits:
     * the scan-mode root takes its key straight from the attached KeyInfo
     * section. */
    QueryNode *node = (QueryNode *)&r.tree[1];
    node->requestInfo = 0;
    QueryNode::setOpLen(node->len, QueryNode::QN_LOOKUP,
                        QN_LookupNode::NodeSize);
    QueryTree::setCntLen(r.tree[0], 1, (Uint32)r.tree.size());
    /* The parameter block op must match the node op, or build() rejects
     * the request with error 20003 before reaching lookup_start.  Rebuild
     * it as a QN_LOOKUP param block with the same {col0, CORR64}
     * projection. */
    r.params.clear();
    r.params.resize(QN_LookupParameters::NodeSize);
    QN_LookupParameters *p = (QN_LookupParameters *)&r.params[0];
    p->requestInfo = DABits::PI_ATTR_LIST;
    p->resultData = kReceiverId;
    r.params.push_back(2);
    r.params.push_back(0 << 16);
    r.params.push_back(AttributeHeader::CORR_FACTOR64 << 16);
    QueryNodeParameters::setOpLen(((QueryNodeParameters *)&r.params[0])->len,
                                  QueryNodeParameters::QN_LOOKUP,
                                  (Uint32)r.params.size());
    /* Root KeyInfo far longer than MAX_KEY_SIZE_IN_WORDS (1023). */
    r.keyInfo.assign(4000, 0x41414141u);
    return expectSurvive("P-3/P-18 huge root lookup key", r);
  }

  int runP6() {  /* NULL constant key -> ndbabort in parseDA */
    SpjReq r;
    makeWellFormedScanLookup(r);
    /* change the child key pattern from P_COL(0) to P_DATA(len 0) and make
     * it NI_KEY_CONSTS instead of NI_KEY_LINKED: a const key with a
     * zero-length (NULL) data token. */
    QueryNode *child = (QueryNode *)&r.tree[m_childNodeTreeOffset];
    child->requestInfo =
        DABits::NI_HAS_PARENT | DABits::NI_KEY_CONSTS | DABits::NI_INNER_JOIN;
    /* key header word is tree word (childOffset + 4 + 1); the pattern word
     * after it is P_COL -> replace with P_DATA(0). */
    const Uint32 keyHdrIdx = m_childNodeTreeOffset + 4 + 1;
    r.tree[keyHdrIdx + 1] = QueryPattern::data(0);  /* P_DATA, length 0 */
    return expectSurvive("P-6 NULL const key", r);
  }

  int runP7() {  /* P_PARENT depth ndbrequire at build */
    SpjReq r;
    makeWellFormedScanLookup(r);
    /* child key pattern: [P_PARENT(5)][P_COL(0)] with pattern length 2 */
    const Uint32 keyHdrIdx = m_childNodeTreeOffset + 4 + 1;
    r.tree[keyHdrIdx] = 0x00000002;  /* 0 params, length 2 */
    r.tree[keyHdrIdx + 1] = QueryPattern::parent(5);
    r.tree.insert(r.tree.begin() + keyHdrIdx + 2, QueryPattern::col(0));
    fixupTreeHeaderAndChildLen(r, 1);
    return expectSurvive("P-7 P_PARENT past root", r);
  }

  int runP8() {  /* child lookup with a parent but no key -> computeHash(RNIL) */
    SpjReq r;
    makeWellFormedScanLookup(r);
    QueryNode *child = (QueryNode *)&r.tree[m_childNodeTreeOffset];
    const Uint32 childLen = QueryNode::getLength(child->len);
    /* drop the key header + P_COL (2 words) and the NI_KEY_LINKED bit */
    child->requestInfo = DABits::NI_HAS_PARENT | DABits::NI_INNER_JOIN;
    const Uint32 keyHdrIdx = m_childNodeTreeOffset + 4 + 1;
    r.tree.erase(r.tree.begin() + keyHdrIdx, r.tree.begin() + keyHdrIdx + 2);
    QueryNode::setOpLen(((QueryNode *)&r.tree[m_childNodeTreeOffset])->len,
                        QueryNode::QN_LOOKUP, childLen - 2);
    QueryTree::setCntLen(r.tree[0], 2, (Uint32)r.tree.size());
    return expectSurvive("P-8 child lookup without key", r);
  }

  int runP9() {  /* parent column not validated -> P_UNQ_PK ndbrequire */
    SpjReq r;
    makeWellFormedScanLookup(r);
    /* P_COL(0) -> P_UNQ_PK(0): the parent's linked column 0 is an INT, not
     * an NDB$PK column, so appendPkColToSection() fails ndbrequire(len > 1)
     * on the first parent row.  An out-of-range P_COL (e.g. P_COL(300))
     * shows the same missing check, but only as a silent read inside the
     * TRANSID_AI stack buffer, which this test cannot detect. */
    const Uint32 keyHdrIdx = m_childNodeTreeOffset + 4 + 1;
    r.tree[keyHdrIdx + 1] = QueryPattern::colPk(0);
    return expectSurvive("P-9 P_UNQ_PK on an INT parent column", r);
  }

  int runP10() {  /* JoinAgg flag but no KeyInfo section -> AttrInfo read as
                   * aggKeys */
    SpjReq r;
    makeWellFormedScan(r);
    r.joinAgg = true;      /* set JoinAgg, but leave keyInfo empty */
    r.keyInfo.clear();     /* => only 2 sections reach DBSPJ */
    return expectSurvive("P-10 JoinAgg without aggKeys section", r);
  }

  int runP11() {  /* ViaSPJ TCKEYREQ without AttrInfo -> ndbrequire */
    SpjReq r;
    makeWellFormedScan(r);  /* tree/params only used to size nothing here */
    r.viaScan = false;
    r.withAttrInfo = false;
    r.keyInfo.assign(1, 0x00000001u);  /* a 1-word key, section 0 */
    return expectSurvive("P-11 ViaSPJ TCKEYREQ without AttrInfo", r);
  }

  int runP14() {  /* DBTC JoinAgg KeyInfo: boundsLen wraps the ndbrequire */
    SpjReq r;
    makeWellFormedScan(r);
    r.joinAgg = true;
    /* JoinAgg combined KeyInfo: word0 = boundsLen.  0xFFFFFFFF wraps
     * ndbrequire(2 + boundsLen <= keyLen) in Dbtc::parseJoinAggKeyInfo. */
    r.keyInfo.assign(4, 0);
    r.keyInfo[0] = 0xFFFFFFFFu;  /* boundsLen */
    return expectSurvive("P-14 JoinAgg boundsLen overflow", r);
  }

  int runP24() {  /* SCAN_TABREQ to the DbtcProxy -> "GSN not added" */
    SpjReq r;
    makeWellFormedScan(r);
    /* The well-formed control request, sent to DBTC instance 0 instead of
     * the instance TCSEIZEREQ returned.  On ndbmtd that is the DbtcProxy,
     * which has no SCAN_TABREQ handler.  SCAN_TABREQ is External, so
     * checkSignalSender() lets it through and handle_execute_error()
     * stops the node. */
    r.toTcProxy = true;
    return expectSurvive("P-24 SCAN_TABREQ to the DbtcProxy", r);
  }

  int runP25() {  /* leaf 1 aggregate slot out of range */
    /* Only leaf 0 went through JoinAggInterpreter::Init; leaf 1 ran per
     * row unchecked, its slot indexing the combined row. */
    return expectAggRejected(
        "P-25 join aggregation leaf 1 slot out of range",
        {countLeaf(), aggProgram(1, countBody(0, 0xFFFF))});
  }

  int runP26() {  /* n_agg_results above MAX_AGG_N_RESULTS */
    /* The header's count sized the slot initialisation of a
     * MAX_AGG_N_RESULTS buffer (a debug build asserted). */
    return expectAggRejected(
        "P-26 join aggregation n_agg_results 65535",
        {aggProgram(0xFFFF, countBody(0, 0)), countLeaf()});
  }

  int runP27() {  /* leaves with more than MAX_AGG_N_RESULTS slots in all */
    /* 200 + 100 slots: each leaf is within the limit, the combined row
     * is not (ndbrequire in setTotalAggResults). */
    return expectAggRejected(
        "P-27 join aggregation leaves with 300 slots in all",
        {aggProgram(200, countBody(0, 0)), aggProgram(100, countBody(0, 0))});
  }

  int runP28() {  /* register index 15, the register file has 8 */
    return expectAggRejected(
        "P-28 join aggregation register 15",
        {aggProgram(1, countBody(15, 0)), countLeaf()});
  }

  int runP29() {  /* DECIMAL scale above precision */
    /* kOpLoadCol of column 0 as DECIMAL(10,40): precision << 16 | scale
     * in the word after it, then COUNT of the register.  bin2decimal
     * sizes its read from them (the checks were asserts only). */
    const std::vector<Uint32> body = {
        aggOp(kOpLoadCol) | (NDB_TYPE_DECIMAL << 21) | (0u << 16) | 0u,
        (10u << 16) | 40u, aggOp(kOpCount) | (0u << 16) | 0u};
    return expectAggRejected("P-29 join aggregation DECIMAL(10,40)",
                             {aggProgram(1, body), countLeaf()});
  }

  int runP30() {  /* leaf 0 with a bad magic word */
    /* checkProgram did not check the header word 0, and
     * JoinAggInterpreter::Init of leaf 0 asserted it (K-1). */
    std::vector<Uint32> leaf = countLeaf();
    leaf[0] = (0x0722u << 16) | Uint32(leaf.size());
    return expectAggRejected("P-30 join aggregation bad magic word",
                             {leaf, countLeaf()});
  }

  int runP31() {  /* leaf 0 with a reserved flag bit */
    /* Only bit 0 (AGG_PROG_FLAG_REUSABLE) of prog[3] is defined;
     * JoinAggInterpreter::Init of leaf 0 asserted the others (K-1). */
    std::vector<Uint32> leaf = countLeaf();
    leaf[3] = 0x2;
    return expectAggRejected("P-31 join aggregation reserved flag bit",
                             {leaf, countLeaf()});
  }

  int runP32() {  /* SUM of a register the row skipped the load of */
    /* kOpSkip over the 3-word kOpLoadConst lands on kOpSumBigint of the
     * same register, which every row reads without loading it.  The
     * program is well formed, so checkProgram accepts it; each row
     * fails with 1868 instead (K-1).  The interpreter's SumBigint
     * asserted the register's type, so a debug data node stopped; a
     * release node summed an undefined register.  With the JIT on
     * (test_spj_malformed_p32_jit) the compiler accepted the program
     * and summed 0 for each row; it now refuses the program, which then
     * runs on the interpreter. */
    const std::vector<Uint32> body = {
        aggOp(kOpSkip) | 3u,
        aggOp(kOpLoadConst) | (NDB_TYPE_BIGINT << 21) | (0u << 16), 1, 0,
        aggOp(kOpSumBigint) | (0u << 16) | 0u};
    return expectAggRejected("P-32 join aggregation SUM of an unloaded "
                             "register",
                             {aggProgram(1, body), countLeaf()});
  }

 private:
  SignalSender *const m_ss;
  NdbRestarter *const m_restarter;
  const Uint32 m_tableId;
  const Uint32 m_schemaVersion;
  const Uint32 m_fragmentCount;
  Uint32 m_userPointer{0};
  Uint32 m_childNodeTreeOffset{0};

  static constexpr Uint32 kReceiverId = 0x00000001u;
  /* Above the per-fragment receiver ids of a join aggregation scan */
  static constexpr Uint32 kAggReceiverId = 0x00010000u;
  static constexpr Uint32 kBatchRows = 16;
  static constexpr Uint32 kBatchBytes = 32768;

  struct Reply {
    Uint32 gsn{0};
    Uint32 errorCode{0};
    bool disconnected{false};
  };

  static const char *gsnName(Uint32 gsn) {
    switch (gsn) {
      case GSN_SCAN_TABCONF: return "SCAN_TABCONF";
      case GSN_SCAN_TABREF: return "SCAN_TABREF";
      case GSN_TCKEYCONF: return "TCKEYCONF";
      case GSN_TCKEYREF: return "TCKEYREF";
      case 0: return "(no reply / disconnected)";
      default: return "(other)";
    }
  }

  void printReply(const char *what, const Reply &rep) {
    g_info << what << ": reply " << gsnName(rep.gsn);
    if (rep.gsn == GSN_SCAN_TABREF || rep.gsn == GSN_TCKEYREF) {
      g_info << " errorCode " << rep.errorCode;
    }
    if (rep.disconnected) g_info << " (send disconnected)";
    g_info << endl;
  }

  /* Seize a TC API-connect record on nodeId, as testTcReleaseId does.
   * `seizeUp` is the ndbapiConnect value we register; DBTC echoes it in
   * word 0 of the later SCAN_TABCONF/REF, so the caller matches on it. */
  bool seize(Uint32 nodeId, Uint32 &apiConnectPtr, Uint32 &tcRef,
             Uint32 &seizeUp) {
    seizeUp = ++m_userPointer;
    SimpleSignal req(false);
    Uint32 *d = req.getDataPtrSend();
    d[0] = seizeUp;            /* API connect ptr (ndbapiConnect) */
    d[1] = m_ss->getOwnRef();  /* ndbapiBlockref */
    d[2] = 0;                  /* any TC instance */
    Uint32 reply[25];
    m_ss->lock();
    const bool sent =
        m_ss->sendSignal(nodeId, req, DBTC, GSN_TCSEIZEREQ, 3) == SEND_OK;
    const Uint32 gsn =
        sent ? waitReplyLocked(GSN_TCSEIZECONF, GSN_TCSEIZEREF, seizeUp,
                               nodeId, ReplyMillis, reply)
             : 0;
    m_ss->unlock();
    if (gsn != GSN_TCSEIZECONF) {
      g_err << "TCSEIZEREQ to node " << nodeId << " got " << gsn << endl;
      return false;
    }
    apiConnectPtr = reply[1];
    tcRef = reply[2];
    return true;
  }

  /* Build and send one SCAN_TABREQ (or TCKEYREQ) and wait for its reply.
   * The request goes to the DBTC instance that TCSEIZEREQ returned in
   * tcRef (plain DBTC is instance 0, the DbtcProxy on ndbmtd), unless
   * r.toTcProxy. */
  Reply sendScanTabReq(SpjReq &r) {
    Reply rep;
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) {
      rep.disconnected = true;
      return rep;
    }
    const Uint16 tcBlock = r.toTcProxy ? Uint16(DBTC) : refToBlock(tcRef);
    const Uint32 userPointer = ++m_userPointer;  /* TCKEYREQ apiOperationPtr */
    const Uint32 transId1 = 0x5b100000u + nodeId;
    const Uint32 transId2 = userPointer;

    /* AttrInfo = tree followed by params. */
    std::vector<Uint32> attrInfo(r.tree);
    attrInfo.insert(attrInfo.end(), r.params.begin(), r.params.end());

    SimpleSignal sig(false);
    Uint32 *d = sig.getDataPtrSend();
    Uint16 gsn;
    Uint32 len;

    if (r.viaScan) {
      len = SpjMalformedSignals::fillScanTabReq(
          d, apiConnectPtr, m_tableId, m_schemaVersion, transId1, transId2,
          kBatchRows, kBatchBytes, r.joinAgg, r.scanParallelism);
      gsn = GSN_SCAN_TABREQ;
      /* sections: 0 = receiver ids, 1 = AttrInfo, 2 = KeyInfo (optional) */
      sig.ptr[ScanTabReq::ReceiverIdSectionNum].p = r.rcvIds.data();
      sig.ptr[ScanTabReq::ReceiverIdSectionNum].sz = (Uint32)r.rcvIds.size();
      sig.ptr[ScanTabReq::AttrInfoSectionNum].p = attrInfo.data();
      sig.ptr[ScanTabReq::AttrInfoSectionNum].sz = (Uint32)attrInfo.size();
      Uint32 nSec = 2;
      if (!r.keyInfo.empty()) {
        sig.ptr[ScanTabReq::KeyInfoSectionNum].p = r.keyInfo.data();
        sig.ptr[ScanTabReq::KeyInfoSectionNum].sz = (Uint32)r.keyInfo.size();
        nSec = 3;
      }
      sig.header.m_noOfSections = nSec;

      m_ss->lock();
      const int sent =
          m_ss->sendFragmentedSignal(nodeId, sig, tcBlock, gsn, len);
      Uint32 reply[25];
      if (sent == SEND_OK) {
        rep.gsn = waitReplyLocked(GSN_SCAN_TABCONF, GSN_SCAN_TABREF,
                                  seizeUp, nodeId, ReplyMillis, reply);
        /* reply[1] is ScanTabConf::requestInfo */
        while (r.waitForEnd && rep.gsn == GSN_SCAN_TABCONF &&
               (reply[1] & ScanTabConf::EndOfData) == 0) {
          rep.gsn = waitReplyLocked(GSN_SCAN_TABCONF, GSN_SCAN_TABREF,
                                    seizeUp, nodeId, ReplyMillis, reply);
        }
        if (rep.gsn == GSN_SCAN_TABREF) rep.errorCode = reply[3];
      } else {
        rep.disconnected = true;
      }
      m_ss->unlock();
    } else {
      /* TCKEYREQ, ViaSPJ.  KeyInfo in section 0, AttrInfo in section 1. */
      len = SpjMalformedSignals::fillTcKeyReq(d, apiConnectPtr, userPointer,
                                              m_tableId, m_schemaVersion,
                                              transId1, transId2);
      gsn = GSN_TCKEYREQ;

      sig.ptr[TcKeyReq::KeyInfoSectionNum].p = r.keyInfo.data();
      sig.ptr[TcKeyReq::KeyInfoSectionNum].sz = (Uint32)r.keyInfo.size();
      Uint32 nSec = 1;
      if (r.withAttrInfo) {
        sig.ptr[TcKeyReq::AttrInfoSectionNum].p = attrInfo.data();
        sig.ptr[TcKeyReq::AttrInfoSectionNum].sz = (Uint32)attrInfo.size();
        nSec = 2;
      }
      sig.header.m_noOfSections = nSec;

      m_ss->lock();
      const SendStatus sent = m_ss->sendSignal(nodeId, sig, tcBlock, gsn, len);
      Uint32 reply[25];
      if (sent == SEND_OK) {
        rep.gsn = waitReplyLocked(GSN_TCKEYCONF, GSN_TCKEYREF, userPointer,
                                  nodeId, ReplyMillis, reply);
      } else {
        rep.disconnected = true;
      }
      m_ss->unlock();
    }
    return rep;
  }

  /* Caller holds the lock.  Return the first gsn1/gsn2 from nodeId whose
   * word 0 is word0Match, or 0 on timeout. */
  Uint32 waitReplyLocked(Uint32 gsn1, Uint32 gsn2, Uint32 word0Match,
                         Uint32 nodeId, Uint32 timeoutMillis, Uint32 *data) {
    const NDB_TICKS start = NdbTick_getCurrentTicks();
    while (true) {
      const Uint64 waited =
          NdbTick_Elapsed(start, NdbTick_getCurrentTicks()).milliSec();
      if (waited >= timeoutMillis) return 0;
      const SimpleSignal *s = m_ss->waitFor(Uint32(timeoutMillis - waited));
      if (s == nullptr) return 0;
      const Uint32 gsn = s->readSignalNumber();
      if ((gsn == gsn1 || gsn == gsn2) &&
          refToNode(s->header.theSendersBlockRef) == nodeId &&
          s->getLength() >= 1 && s->getDataPtr()[0] == word0Match) {
        memcpy(data, s->getDataPtr(), 4 * s->getLength());
        return gsn;
      }
    }
  }

  Uint32 pickDbNode() { return (Uint32)m_restarter->getDbNodeId(0); }

  /* True if every data node reports STARTED. */
  bool clusterHealthy() {
    const int n = m_restarter->getNumDbNodes();
    for (int i = 0; i < n; i++) {
      const int nodeId = m_restarter->getDbNodeId(i);
      if (m_restarter->getNodeStatus(nodeId) != NDB_MGM_NODE_STATUS_STARTED) {
        g_err << "Data node " << nodeId << " is not STARTED (status "
              << m_restarter->getNodeStatus(nodeId) << ")" << endl;
        return false;
      }
    }
    return true;
  }

  /* Send the malformed request, then require every data node to still be
   * started.  A REF or a disconnect is fine; a node down is the failure. */
  int expectSurvive(const char *findingId, SpjReq &r) {
    Reply rep = sendScanTabReq(r);
    printReply(findingId, rep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (clusterHealthy()) {
      g_info << findingId << ": PASS (all data nodes started; reply "
             << gsnName(rep.gsn) << ")" << endl;
      return NDBT_OK;
    }
    return failNodeDown(findingId);
  }

  int failNodeDown(const char *findingId) {
    g_err << findingId
          << ": FAIL -- a data node went down on the malformed request. "
             "Attempting recovery for later cases." << endl;
    if (m_restarter->waitClusterStarted(120) != 0) {
      (void)m_restarter->restartAll(false, false, false);
      m_restarter->waitClusterStarted(120);
    }
    return NDBT_FAILED;
  }

  /* A join aggregation case: first the well-formed two-leaf request,
   * which must get SCAN_TABCONF (without it a harness that never reaches
   * the aggregation setup would pass every case), then the request with
   * the malformed leaves, which must get SCAN_TABREF and leave every data
   * node started.  The REF comes from JOIN_AGG_SETUP_REF with
   * DbspjErr::InvalidRequest, for P-32 from the failed row (1868); the
   * error code is printed, not required, as an error of a fragment scan
   * reaching DBTC first would win. */
  int expectAggRejected(const char *findingId,
                        const std::vector<std::vector<Uint32>> &leaves) {
    SpjReq good;
    makeJoinAggScan(good, {countLeaf(), countLeaf()});
    const Reply control = sendScanTabReq(good);
    printReply("CONTROL (well-formed join aggregation)", control);
    if (control.gsn != GSN_SCAN_TABCONF) {
      g_err << "JOIN AGGREGATION CONTROL did not get SCAN_TABCONF (got "
            << gsnName(control.gsn) << ", err " << control.errorCode
            << ").  The raw join aggregation encoding is wrong; fix this "
               "before trusting the result of "
            << findingId << "." << endl;
      return clusterHealthy() ? NDBT_FAILED : failNodeDown(findingId);
    }

    SpjReq r;
    makeJoinAggScan(r, leaves);
    r.waitForEnd = true;
    const Reply rep = sendScanTabReq(r);
    printReply(findingId, rep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (rep.gsn != GSN_SCAN_TABREF) {
      g_err << findingId << ": FAIL -- expected SCAN_TABREF, got "
            << gsnName(rep.gsn)
            << ": the malformed aggregation program was not rejected"
            << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; SCAN_TABREF "
           << rep.errorCode << ", JOIN_AGG_SETUP_REF sends "
           << Uint32(DbspjErr::InvalidRequest) << ")" << endl;
    return NDBT_OK;
  }

  /* Patch the tree header count/length and the child node length after an
   * insertion of `added` words inside the child node. */
  void fixupTreeHeaderAndChildLen(SpjReq &r, Uint32 added) {
    QueryNode *child = (QueryNode *)&r.tree[m_childNodeTreeOffset];
    QueryNode::setOpLen(child->len, QueryNode::QN_LOOKUP,
                        QueryNode::getLength(child->len) + added);
    QueryTree::setCntLen(r.tree[0], 2, (Uint32)r.tree.size());
  }
};

/* ----- one STEP per finding, so NDBT runs them one by one (-n <name>) --- */

SpjMalformedChecker *makeChecker(NDBT_Context *ctx, NDBT_Step *step,
                                 Ndb *&pNdb, SignalSender *&ss,
                                 NdbRestarter *&restarter) {
  pNdb = GETNDB(step);
  const NdbDictionary::Table *tab = ctx->getTab();
  HugoTransactions hugo(*tab);
  if (hugo.loadTable(pNdb, NumRows) != 0) {
    g_err << "Failed to load " << NumRows << " rows" << endl;
    return nullptr;
  }
  ss = new SignalSender(&pNdb->get_ndb_cluster_connection());
  restarter = new NdbRestarter();
  return new SpjMalformedChecker(ss, restarter, tab);
}

#define CASE_STEP(fn, method)                                      \
  int fn(NDBT_Context *ctx, NDBT_Step *step) {                     \
    Ndb *pNdb = nullptr;                                           \
    SignalSender *ss = nullptr;                                    \
    NdbRestarter *restarter = nullptr;                             \
    SpjMalformedChecker *c = makeChecker(ctx, step, pNdb, ss, restarter); \
    if (c == nullptr) return NDBT_FAILED;                          \
    const int res = c->method();                                   \
    delete c;                                                      \
    delete ss;                                                     \
    delete restarter;                                              \
    return res;                                                    \
  }

CASE_STEP(runControl, runPositiveControl)
CASE_STEP(runP1, runP1)
CASE_STEP(runP2, runP2)
CASE_STEP(runP3, runP3)
CASE_STEP(runP6, runP6)
CASE_STEP(runP7, runP7)
CASE_STEP(runP8, runP8)
CASE_STEP(runP9, runP9)
CASE_STEP(runP10, runP10)
CASE_STEP(runP11, runP11)
CASE_STEP(runP14, runP14)
CASE_STEP(runP24, runP24)
CASE_STEP(runP25, runP25)
CASE_STEP(runP26, runP26)
CASE_STEP(runP27, runP27)
CASE_STEP(runP28, runP28)
CASE_STEP(runP29, runP29)
CASE_STEP(runP30, runP30)
CASE_STEP(runP31, runP31)
CASE_STEP(runP32, runP32)

}  // namespace

NDBT_TESTSUITE(testSpjMalformed);
TESTCASE("Control",
         "Positive control: a well-formed raw SPJ scan must get "
         "SCAN_TABCONF") {
  STEP(runControl);
}
TESTCASE("P1", "v1 scan-parameter widening underflow (DbspjMain build())") {
  STEP(runP1);
}
TESTCASE("P2", "scan tableId used as an array index before validation") {
  STEP(runP2);
}
TESTCASE("P3", "QN_LOOKUP root of a scan request with an over-long KeyInfo") {
  STEP(runP3);
}
TESTCASE("P6", "NULL constant key reaches ndbabort in parseDA") {
  STEP(runP6);
}
TESTCASE("P7", "P_PARENT depth past the root (ndbrequire at build)") {
  STEP(runP7);
}
TESTCASE("P8", "child lookup with a parent but no key") {
  STEP(runP8);
}
TESTCASE("P9",
         "P_UNQ_PK on a non-PK parent column (parent column not "
         "validated)") {
  STEP(runP9);
}
TESTCASE("P10", "JoinAgg flag without an aggKeys/KeyInfo section") {
  STEP(runP10);
}
TESTCASE("P11", "ViaSPJ TCKEYREQ without an AttrInfo section") {
  STEP(runP11);
}
TESTCASE("P14", "DBTC JoinAgg KeyInfo boundsLen overflow") {
  STEP(runP14);
}
TESTCASE("P24", "SCAN_TABREQ to DBTC instance 0 (the DbtcProxy)") {
  STEP(runP24);
}
TESTCASE("P25",
         "join aggregation: leaf 1 of 2 with an aggregate slot out of "
         "range") {
  STEP(runP25);
}
TESTCASE("P26", "join aggregation: n_agg_results above MAX_AGG_N_RESULTS") {
  STEP(runP26);
}
TESTCASE("P27",
         "join aggregation: leaves with more than MAX_AGG_N_RESULTS "
         "aggregate slots in all") {
  STEP(runP27);
}
TESTCASE("P28", "join aggregation: register index 15 (8 registers)") {
  STEP(runP28);
}
TESTCASE("P29", "join aggregation: DECIMAL scale above precision") {
  STEP(runP29);
}
TESTCASE("P30", "join aggregation: leaf 0 with a bad magic word") {
  STEP(runP30);
}
TESTCASE("P31", "join aggregation: leaf 0 with a reserved flag bit") {
  STEP(runP31);
}
TESTCASE("P32",
         "join aggregation: rows read a register they skipped the load "
         "of") {
  STEP(runP32);
}
NDBT_TESTSUITE_END(testSpjMalformed)

int main(int argc, const char **argv) {
  ndb_init();
  NDBT_TESTSUITE_INSTANCE(testSpjMalformed);
  return testSpjMalformed.execute(argc, argv);
}
