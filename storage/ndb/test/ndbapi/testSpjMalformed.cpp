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
 * P-33 sends a SCAN_FRAGREQ straight to DBLQH, which only a debug data
 * node takes from an API node: an aggregation scan without an
 * interpreted program, which DBLQH must refuse with SCAN_FRAGREF.
 * P-35 gives a join aggregation leaf a linked attribute buffer whose
 * entry runs past its end, which fails each row (RONDB-1124 bug 12).
 * P-36 and P-37 send embedded programs whose inline-type compare has a
 * charset that does not fit the type (validateEmbeddedProgram).
 * P-40 reads a linked attribute entry as a type wider than the entry,
 * which reads as NULL (RONDB-1124).
 * P-41 is a plain interpreted read, not through DBSPJ, whose AttrInfo
 * header lengths wrap in 32 bits, which DBTUP refuses (RONDB-1124).
 * P-43 and P-44 send GROUP BY column metadata whose collation does not
 * fit its type, which JOIN_AGG_SETUP_REQ must refuse
 * (AggInterpreterBase::initGBColType).  P-45 compares a staged linked
 * value with a constant of another length (BRANCH_MEM_OP_ARG_INLINE_TYPE):
 * CHAR values must compare padded, a VARCHAR length byte past its value
 * must fail the row (NdbSqlUtil::check_compare_values).
 * P-46 is a plain interpreted range scan, not through DBSPJ, with two
 * ranges whose parameter blocks run past the parameter area of the
 * program, or whose header lengths wrap in 32 bits, which DBTUP refuses
 * (RONDB-1124).  DBTUP reads parameter blocks only for a scan from DBSPJ,
 * so the block cases run under error insert 4047, which has it read them
 * for this scan as well (data nodes built with ERROR_INSERT only).
 * P-49 is an NDB API range scan with two ranges whose program calls a
 * subroutine, which must return the rows of both ranges: DBTUP read the
 * subroutine as parameter blocks from the second range on (RONDB-1124).
 * P-48 reads a linked attribute entry shorter than its type, as P-40
 * does, on the JIT (test_spj_malformed_p48_jit), which decoded it as a
 * value, and for the NULL-extended row of an outer join, which failed
 * the row; it reads as NULL there too (RONDB-1124).
 * P-50 sends interpreted programs whose subroutine part, or whose
 * pattern without NI_ATTR_LINKED, went to the subroutine section of the
 * AttrInfo, which DBSPJ must refuse (RONDB-1124).
 * P-38 and P-39 send CASE programs that put a string MIN / MAX and a
 * number in one aggregate slot (checkAggSlotKinds).
 * Q-1 .. Q-5 send DBTC requests that carry the mark of a rate queue
 * re-send (passQueue), which only DBTC itself may set (RONDB-1124).
 * S-1 and S-2 send join aggregation signals that only data nodes send
 * (SignalScopes.hpp), which a data node must drop and report: S-1
 * JOIN_AGG_SEND_REQ to DBTC, S-2 a JOIN_AGG_FINAL_REP that stopped a
 * data node, on release data nodes only (RONDB-1124).
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
#include <CteLinkedAttr.hpp>
/* ndb_opts.h, included through NDBT.hpp, defines NONE as an empty
 * macro; Interpreter.hpp has an enumerator of that name. */
#ifdef NONE
#undef NONE
#endif
#include <Interpreter.hpp>
#include <NdbSleep.h>
#include <mgmapi.h>
#include <signaldata/ScanFrag.hpp>
#include <signaldata/ScanTab.hpp>
#include <signaldata/TcKeyReq.hpp>
#include <signaldata/QueryTree.hpp>
#include <signaldata/DbspjErr.hpp>
#include <signaldata/JoinAgg.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include <algorithm>
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

  /* The mark that DBTC's rate queue handler puts on a request it re-sends
   * to itself (the Q cases send it from the API). */
  static void setTcKeyReqPassQueue(Uint32 *data) {
    TcKeyReq *req = (TcKeyReq *)data;
    Uint32 ri = req->requestInfo;
    TcKeyReq::setPassQueueingFlag(ri, 1);
    req->requestInfo = ri;
  }

  static void setScanTabReqPassQueue(Uint32 *data) {
    ScanTabReq *req = (ScanTabReq *)data;
    Uint32 ri = req->requestInfo;
    ScanTabReq::setPassQueueingFlag(ri, 1);
    req->requestInfo = ri;
  }

  /* A plain interpreted read (P-41): not ViaSPJ, and the AttrInfo
   * section is an interpreted program with its 5-word header. */
  static void setTcKeyReqInterpretedRead(Uint32 *data) {
    TcKeyReq *req = (TcKeyReq *)data;
    Uint32 ri = req->requestInfo;
    TcKeyReq::setViaSPJFlag(ri, 0);
    TcKeyReq::setInterpretedFlag(ri, 1);
    req->requestInfo = ri;
  }

  /* A plain range scan of an ordered index (P-46): not ViaSPJ, not a
   * TUP scan, and the AttrInfo section is an interpreted program with
   * its 5-word header (DBTC never sets the not-interpreted flag). */
  static void setScanTabReqPlainRangeScan(Uint32 *data) {
    ScanTabReq *req = (ScanTabReq *)data;
    Uint32 ri = req->requestInfo;
    ScanTabReq::setViaSPJFlag(ri, 0);
    ScanTabReq::setTupScanFlag(ri, 0);
    ScanTabReq::setRangeScanFlag(ri, 1);
    req->requestInfo = ri;
  }

  /* The key length of a short TCKEYREQ / TCINDXREQ, in requestInfo bits
   * 20-31. */
  static void setTcKeyReqKeyLength(Uint32 *data, Uint32 keyLen) {
    TcKeyReq *req = (TcKeyReq *)data;
    Uint32 ri = req->requestInfo;
    TcKeyReq::setKeyLength(ri, keyLen);
    req->requestInfo = ri;
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
  bool passQueue{false};        /* set DBTC's rate queue re-send flag */
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

  /* The answer to one request: its GSN and error code, or a disconnect.
   * Declared first: a member function's return type is looked up where
   * the function is declared, and P-41's sendInterpretedRead, among
   * others, comes before the send helpers below. */
  struct Reply {
    Uint32 gsn{0};
    Uint32 errorCode{0};
    bool disconnected{false};
  };

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

  /* A leaf whose rows first run an embedded program, laid out as RonSQL
   * emits a compare of a linked column with a constant, then COUNT(1):
   *   0 READ_LINKED_TO_MEM of position 0
   *   1 BRANCH_MEM_OP_ARG_INLINE_TYPE EQ to word 5, NULL continues
   *   2 (typeId << 16) | 4         the constant has 4 bytes
   *   3 (4 << 16) | csNumber       the column has 4 bytes
   *   4 the constant "abcd"
   *   5 LOAD_CONST16 0 into register 0
   *   6 WRITE_INTERPRETER_OUTPUT 0 of register 0: go on after it
   *   7 EXIT_OK
   * The scanned table has no linked columns, so the staged value is
   * NULL and the compare does not run here; the program is otherwise
   * well formed, so only the check of the type word refuses it. */
  static std::vector<Uint32> inlineTypeLeaf(Uint32 typeId, Uint32 csNumber) {
    const Uint32 nulls = Interpreter::IF_NULL_CONTINUE;
    const std::vector<Uint32> emb = {
        Interpreter::READ_LINKED_TO_MEM | (0u << 16),
        Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE | (nulls << 6) |
            (Uint32(Interpreter::EQ) << 12) | (4u << 16),
        Interpreter::BranchMem_2(typeId, 4),
        (4u << 16) | csNumber,
        0x64636261u,
        Interpreter::LoadConst16(0, 0),
        Interpreter::WriteInterpreterOutput(0, 0),
        Interpreter::ExitOK()};
    return embeddedCountLeaf(emb);
  }

  /* A leaf whose rows run the embedded program emb, then COUNT(1). */
  static std::vector<Uint32> embeddedCountLeaf(
      const std::vector<Uint32> &emb) {
    std::vector<Uint32> body;
    body.push_back(aggOp(kOpEmbeddedInterp) | Uint32(emb.size()));
    body.insert(body.end(), emb.begin(), emb.end());
    const std::vector<Uint32> count = countBody(0, 0);
    body.insert(body.end(), count.begin(), count.end());
    return aggProgram(1, body);
  }

  /* kOpLoadCol of column col, of type typeId (below 32, so without the
   * 6th type bit), into reg. */
  static Uint32 loadCol(Uint32 reg, Uint32 typeId, Uint32 col) {
    return aggOp(kOpLoadCol) | (typeId << 21) | (reg << 16) | col;
  }

  /* kOpLoadConst of a BIGINT or DOUBLE into reg, the 64-bit constant in
   * two words, low word first. */
  static std::vector<Uint32> loadConst(Uint32 reg, Uint32 typeId,
                                       Uint32 low, Uint32 high) {
    return {aggOp(kOpLoadConst) | (typeId << 21) | (reg << 16), low, high};
  }

  /* An aggregate op of reg into slot. */
  static Uint32 aggUpdate(Uint32 op, Uint32 reg, Uint32 slot) {
    return aggOp(op) | (reg << 16) | slot;
  }

  /* A leaf with one slot whose rows run a CASE, laid out as RonSQL emits
   * it, then the words of after:
   *   kOpEmbeddedInterp of 8 words:
   *     0 READ_ATTR_INTO_REG column 0 into register 1
   *     1 BRANCH_REG_EQ_NULL of register 1 to word 5
   *     2 LOAD_CONST16 0 into register 2         not NULL: the THEN arm
   *     3 WRITE_INTERPRETER_OUTPUT 0 of register 2
   *     4 EXIT_OK
   *     5 LOAD_CONST16 (THEN arm + 1) into register 2   NULL: the ELSE arm
   *     6 WRITE_INTERPRETER_OUTPUT 0 of register 2
   *     7 EXIT_OK
   *   the THEN arm, then kOpSkip over the ELSE arm
   *   the ELSE arm
   * Both skips are LOAD_CONST16 values, so the data node knows which
   * words the rows of each arm run.  Used as leaf 1, which no row runs:
   * the column numbers are not looked up. */
  static std::vector<Uint32> caseLeaf(const std::vector<Uint32> &thenArm,
                                      const std::vector<Uint32> &elseArm,
                                      const std::vector<Uint32> &after) {
    const Uint32 elseSkip = Uint32(thenArm.size()) + 1;
    const std::vector<Uint32> emb = {
        Interpreter::Read(0, 1),
        Interpreter::Branch(Interpreter::BRANCH_REG_EQ_NULL, 0, 1) |
            (4u << 16),
        Interpreter::LoadConst16(2, 0),
        Interpreter::WriteInterpreterOutput(2, 0),
        Interpreter::ExitOK(),
        Interpreter::LoadConst16(2, elseSkip),
        Interpreter::WriteInterpreterOutput(2, 0),
        Interpreter::ExitOK()};
    std::vector<Uint32> body;
    body.push_back(aggOp(kOpEmbeddedInterp) | Uint32(emb.size()));
    body.insert(body.end(), emb.begin(), emb.end());
    body.insert(body.end(), thenArm.begin(), thenArm.end());
    body.push_back(aggOp(kOpSkip) | Uint32(elseArm.size()));
    body.insert(body.end(), elseArm.begin(), elseArm.end());
    body.insert(body.end(), after.begin(), after.end());
    return aggProgram(1, body);
  }

  /* Arms of caseLeaf: a column of typeId loaded into reg, and then, when
   * op is not kOpUnknown, op of reg into slot 0. */
  static std::vector<Uint32> colArm(Uint32 reg, Uint32 typeId, Uint32 col,
                                    Uint32 op) {
    std::vector<Uint32> arm = {loadCol(reg, typeId, col)};
    if (op != kOpUnknown) {
      arm.push_back(aggUpdate(op, reg, 0));
    }
    return arm;
  }

  static std::vector<Uint32> constArm(Uint32 reg, Uint32 typeId,
                                      Uint32 low, Uint32 high, Uint32 op) {
    std::vector<Uint32> arm = loadConst(reg, typeId, low, high);
    if (op != kOpUnknown) {
      arm.push_back(aggUpdate(op, reg, 0));
    }
    return arm;
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

  /* ---- the linked attribute buffer of an aggregation leaf (P-35) ------ *
   * The scan+lookup request as a join aggregation: the root scan
   * projects col0, the primary key, as its linked attribute, so every
   * child lookup finds its row, and the child lookup is the aggregation
   * leaf.  The leaf's NI_ATTR_LINKED pattern (optional part 3) is one
   * P_DATA token.  DBSPJ copies its words verbatim into the subroutine
   * section of every LQHKEYREQ of the leaf, and DBTUP takes them as the
   * row's linked attribute buffer ([word 0][word 1][AttributeHeader]
   * [data] per entry, CteLinkedAttr.hpp).  So the API chooses that
   * buffer word for word. */
  void makeJoinAggScanLookup(SpjReq &r, const std::vector<Uint32> &linked,
                             const std::vector<Uint32> &leaf) {
    makeWellFormedScanLookup(r);
    ((QueryNode *)&r.tree[1])->requestInfo =
        DABits::NI_LINKED_ATTR | DABits::NI_AGGREGATE;
    /* Root linked attribute list: {cnt=1, item0=attr 0}. */
    r.tree[1 + QN_ScanFragNode::NodeSize] = 1u | (0u << 16);
    ((QueryNode *)&r.tree[m_childNodeTreeOffset])->requestInfo |=
        DABits::NI_ATTR_LINKED | DABits::NI_AGGREGATE |
        DABits::NI_AGGREGATE_LEAF;
    /* Optional part 3 of the child, its last part: the header
     * [pattern length << 16 | program length 0], then the pattern. */
    const Uint32 patternLen = 1 + Uint32(linked.size());
    r.tree.push_back(patternLen << 16);
    r.tree.push_back(QueryPattern::data(Uint32(linked.size())));
    r.tree.insert(r.tree.end(), linked.begin(), linked.end());
    fixupTreeHeaderAndChildLen(r, 1 + patternLen);

    r.joinAgg = true;
    r.scanParallelism = m_fragmentCount;
    r.rcvIds.clear();
    for (Uint32 i = 0; i < m_fragmentCount; i++) {
      r.rcvIds.push_back(kReceiverId + i);
    }
    r.keyInfo.clear();
    r.keyInfo.push_back(0);  /* boundsLen */
    r.keyInfo.push_back(kAggReceiverId);
    r.keyInfo.push_back((0x0722u << 16) | 1u);  /* one leaf */
    r.keyInfo.push_back(Uint32(leaf.size()));
    r.keyInfo.insert(r.keyInfo.end(), leaf.begin(), leaf.end());
  }

  /* A leaf that groups by nGbCols linked columns, each of them linked
   * position 0 as a BIGINT, and counts the rows. */
  static std::vector<Uint32> linkedGroupByLeaf(Uint32 nGbCols) {
    std::vector<Uint32> body;
    for (Uint32 i = 0; i < nGbCols; i++) {
      body.push_back((Uint32(AGG_LINKED_COL_FLAG | 0u) << 16) |
                     (Uint32(NDB_TYPE_BIGINT) & AGG_GB_COL_TYPE_MASK));
    }
    const std::vector<Uint32> count = countBody(0, 0);
    body.insert(body.end(), count.begin(), count.end());
    std::vector<Uint32> p = aggProgram(1, body);
    p[1] = (nGbCols << 16) | 1u;  /* (n_gb_cols << 16) | n_agg_results */
    return p;
  }

  /* A leaf (P-40) whose rows load linked position 0 as a BIGINT and go
   * on to COUNT(1) only when the value is NULL:
   *   0 READ_LINKED_COLUMN_TO_REG of position 0, BIGINT, into register 1
   *   1 BRANCH_REG_EQ_NULL of register 1 to word 3
   *   2 EXIT_OK without output 0: the row fails (1872)
   *   3 LOAD_CONST16 0 into register 0
   *   4 WRITE_INTERPRETER_OUTPUT 0 of register 0: go on after it
   *   5 EXIT_OK
   * So the scan gets SCAN_TABCONF when every row reads NULL and
   * SCAN_TABREF when they read a value. */
  static std::vector<Uint32> linkedNullLeaf() {
    const std::vector<Uint32> emb = {
        Interpreter::ReadLinkedColumnIntoReg(1, 0, NDB_TYPE_BIGINT),
        Interpreter::BRANCH_REG_EQ_NULL | (1u << 6) | (2u << 16),
        Interpreter::ExitOK(),
        Interpreter::LoadConst16(0, 0),
        Interpreter::WriteInterpreterOutput(0, 0),
        Interpreter::ExitOK()};
    return embeddedCountLeaf(emb);
  }

  /* A leaf (P-48) whose rows load linked position 0 as a BIGINT and
   * compare it with 7, then COUNT(1):
   *   0 READ_LINKED_COLUMN_TO_REG of position 0, BIGINT, into register 1
   *   1 LOAD_CONST16 7 into register 2
   *   2 BRANCH_EQ_REG_REG of registers 1 and 2 to word 3
   *   3 LOAD_CONST16 0 into register 0
   *   4 WRITE_INTERPRETER_OUTPUT 0 of register 0: go on after it
   *   5 EXIT_OK
   * Both outcomes of the compare go on, so a row that reads a value is
   * counted, while DBTUP fails a row that reads NULL: a compare with a
   * NULL register is ZREGISTER_INIT_ERROR, and the row 1872.  So the
   * scan gets SCAN_TABCONF when every row reads a value and SCAN_TABREF
   * when they read NULL.  The JIT compiles the program
   * (ndb_jit_bridge.c: OP_LOAD_LINKED_COL, a constant and an integer
   * compare, without a NULL guard); its registers hold no NULL, and a
   * row whose load would give NULL takes the per-row fallback, which
   * runs it on DBTUP's interpreter. */
  static std::vector<Uint32> linkedCompareLeaf() {
    const std::vector<Uint32> emb = {
        Interpreter::ReadLinkedColumnIntoReg(1, 0, NDB_TYPE_BIGINT),
        Interpreter::LoadConst16(2, 7),
        Interpreter::Branch(Interpreter::BRANCH_EQ_REG_REG, 2, 1) |
            (1u << 16),
        Interpreter::LoadConst16(0, 0),
        Interpreter::WriteInterpreterOutput(0, 0),
        Interpreter::ExitOK()};
    return embeddedCountLeaf(emb);
  }

  /* A leaf (P-51) whose rows load linked position 0 as a BIGINT, and
   * load register 3 only for a value other than 7:
   *   0 READ_LINKED_COLUMN_TO_REG of position 0, BIGINT, into register 1
   *   1 LOAD_CONST16 7 into register 2
   *   2 BRANCH_EQ_REG_REG of registers 1 and 2 to word 4
   *   3 LOAD_CONST16 1 into register 3
   *   4 BRANCH_REG_EQ_NULL of register 3 to word 8
   *   5 LOAD_CONST16 0 into register 0
   *   6 WRITE_INTERPRETER_OUTPUT 0 of register 0: go on after it
   *   7 EXIT_OK
   *   8 BRANCH_EQ_REG_REG of registers 3 and 2 to word 9
   *   9 LOAD_CONST16 0 into register 0
   *  10 WRITE_INTERPRETER_OUTPUT 0 of register 0
   *  11 EXIT_OK
   * then COUNT(1).  The embedded program starts with its registers
   * NULL, so for a value of 7 register 3 is NULL at word 4, the guard
   * is taken, and the compare at word 8 fails the row
   * (ZREGISTER_INIT_ERROR, 1872): SCAN_TABREF.  For another value the
   * rows are counted: SCAN_TABCONF.  The JIT folds the guard to "not
   * taken" (ndb_jit_bridge.c; its registers hold no NULL), so the
   * compiled program counted the rows of a 7. */
  static std::vector<Uint32> linkedSkipLoadLeaf() {
    const std::vector<Uint32> emb = {
        Interpreter::ReadLinkedColumnIntoReg(1, 0, NDB_TYPE_BIGINT),
        Interpreter::LoadConst16(2, 7),
        Interpreter::Branch(Interpreter::BRANCH_EQ_REG_REG, 2, 1) |
            (2u << 16),
        Interpreter::LoadConst16(3, 1),
        Interpreter::Branch(Interpreter::BRANCH_REG_EQ_NULL, 0, 3) |
            (4u << 16),
        Interpreter::LoadConst16(0, 0),
        Interpreter::WriteInterpreterOutput(0, 0),
        Interpreter::ExitOK(),
        Interpreter::Branch(Interpreter::BRANCH_EQ_REG_REG, 2, 3) |
            (1u << 16),
        Interpreter::LoadConst16(0, 0),
        Interpreter::WriteInterpreterOutput(0, 0),
        Interpreter::ExitOK()};
    return embeddedCountLeaf(emb);
  }

  /* The join aggregation of makeJoinAggScanLookup with its leaf
   * outer-joined (P-48), and keyed on the root's KOL2 instead of its
   * primary key.  Hugo loads KOL2 with values above 10^8, none of them
   * the key of a loaded row, so every lookup of the leaf misses, and
   * DBLQH runs the leaf program for a NULL-extended row
   * (Dblqh::feedOuterJoinAggNullRow,
   * JoinAggInterpreter::evalEmbeddedForNullRow) with the linked buffer
   * of the P_DATA token, as DBTUP runs it for a row that was found. */
  void makeOuterJoinAggScanLookup(SpjReq &r,
                                  const std::vector<Uint32> &linked,
                                  const std::vector<Uint32> &leaf) {
    makeJoinAggScanLookup(r, linked, leaf);
    /* Root linked attribute list: {cnt=1, item0=attr 1}. */
    r.tree[1 + QN_ScanFragNode::NodeSize] = 1u | (1u << 16);
    ((QueryNode *)&r.tree[m_childNodeTreeOffset])->requestInfo &=
        ~Uint32(DABits::NI_INNER_JOIN);
  }

  /* ---- GROUP BY column metadata (P-43, P-44) --------------------------- *
   * A leaf that groups by column 0 of the table, its primary key (T1's
   * KOL1, an Unsigned), and counts the rows. */
  static std::vector<Uint32> localGroupByLeaf() {
    std::vector<Uint32> body;
    body.push_back((0u << 16) |
                   (Uint32(NDB_TYPE_UNSIGNED) & AGG_GB_COL_TYPE_MASK));
    const std::vector<Uint32> count = countBody(0, 0);
    body.insert(body.end(), count.begin(), count.end());
    std::vector<Uint32> p = aggProgram(1, body);
    p[1] = (1u << 16) | 1u;  /* one GROUP BY column, one result */
    return p;
  }

  /* Appends the column metadata of the main program to a join
   * aggregation's KeyInfo, after its leaves, as NdbQueryImpl lays it out
   * (NdbQueryOperation.cpp, appendJoinAggColumnMetaEntry): a container
   * [marker][version][1 block], the block's [kind MAIN][no CTE][length],
   * and the block [marker][version][1 entry][entry].  The entry describes
   * the GROUP BY column of localGroupByLeaf with typeId, maxBytes and
   * csNumber.  DBTC sends the block with JOIN_AGG_SETUP_REQ, and
   * DblqhProxy gives it to AggInterpreterBase::initGBTypesFromMetadata. */
  void appendGroupByMeta(SpjReq &r, Uint32 typeId, Uint32 maxBytes,
                         Uint32 csNumber) {
    const std::vector<Uint32> entry = {
        JOIN_AGG_META_SOURCE_LOCAL_COLUMN,
        0,  /* source: column 0 */
        8,  /* program offset of the GROUP BY column word */
        0,  /* GROUP BY slot 0 */
        m_tableId,
        m_schemaVersion,
        0,  /* column id */
        typeId,
        maxBytes,
        csNumber,
        0,  /* precision << 16 | scale */
        JOIN_AGG_META_FLAG_GROUP_BY};
    r.keyInfo.push_back(JOIN_AGG_META_MARKER);
    r.keyInfo.push_back(JOIN_AGG_META_VERSION);
    r.keyInfo.push_back(1);  /* one block */
    r.keyInfo.push_back(JOIN_AGG_META_KIND_MAIN);
    r.keyInfo.push_back(kNoCteIndex);
    r.keyInfo.push_back(3 + Uint32(entry.size()));
    r.keyInfo.push_back(JOIN_AGG_META_MARKER);
    r.keyInfo.push_back(JOIN_AGG_META_VERSION);
    r.keyInfo.push_back(1);  /* one entry */
    r.keyInfo.insert(r.keyInfo.end(), entry.begin(), entry.end());
  }

  /* ---- inline-type compare of a staged value (P-45) -------------------- *
   * A leaf whose rows compare linked position 0 with a constant and are
   * counted when the two are equal.  A row that finds them unequal ends
   * its embedded program without writing the output, which fails the
   * row (ZAGG_EMBEDDED_INTERP_ERROR), as does a compare that DBTUP
   * refuses (error 40):
   *   0     READ_LINKED_TO_MEM of position 0
   *   1     BRANCH_MEM_OP_ARG_INLINE_TYPE NE to the last word
   *   2     (typeId << 16) | the constant's length
   *   3     (columnSize << 16) | csNumber
   *   4..   the constant
   *   n-4   LOAD_CONST16 0 into register 0
   *   n-3   WRITE_INTERPRETER_OUTPUT 0 of register 0: go on after it
   *   n-2   EXIT_OK
   *   n-1   EXIT_OK without an output */
  static std::vector<Uint32> inlineCompareLeaf(
      Uint32 typeId, Uint32 columnSize, Uint32 csNumber,
      const std::vector<Uint32> &constant, Uint32 constantLen) {
    const Uint32 nulls = Interpreter::IF_NULL_CONTINUE;
    std::vector<Uint32> emb;
    emb.push_back(Interpreter::READ_LINKED_TO_MEM | (0u << 16));
    emb.push_back(0);  /* the branch, once its target is known */
    emb.push_back(Interpreter::BranchMem_2(typeId, constantLen));
    emb.push_back((columnSize << 16) | csNumber);
    emb.insert(emb.end(), constant.begin(), constant.end());
    emb.push_back(Interpreter::LoadConst16(0, 0));
    emb.push_back(Interpreter::WriteInterpreterOutput(0, 0));
    emb.push_back(Interpreter::ExitOK());
    emb.push_back(Interpreter::ExitOK());
    const Uint32 offset = Uint32(emb.size()) - 2;  /* from word 1 */
    emb[1] = Interpreter::BRANCH_MEM_OP_ARG_INLINE_TYPE | (nulls << 6) |
             (Uint32(Interpreter::NE) << 12) | (offset << 16);
    std::vector<Uint32> body;
    body.push_back(aggOp(kOpEmbeddedInterp) | Uint32(emb.size()));
    body.insert(body.end(), emb.begin(), emb.end());
    const std::vector<Uint32> count = countBody(0, 0);
    body.insert(body.end(), count.begin(), count.end());
    return aggProgram(1, body);
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

  /* ---- P-33: a raw SCAN_FRAGREQ straight to DBLQH ---------------------- *
   * An aggregation scan without an interpreted program: the aggregation
   * flag together with the not-interpreted flag.  execSCAN_FRAGREQ
   * stopped the node on ndbrequire(opExec).  DBTC and DBSPJ never set
   * the not-interpreted flag (only BACKUP does, without aggregation), so
   * only a raw SCAN_FRAGREQ gets there.  Only a debug data node takes a
   * SCAN_FRAGREQ from an API node (NDB_SCOPE_REMOTE_EXCEPT_DEBUG in
   * SignalScopes.hpp); a release node drops it and disconnects us, so
   * the case needs a debug build.  DBLQH now answers SCAN_FRAGREF 1868,
   * as LQHKEYREQ does for an aggregation read without a program.  The
   * check comes before the fragment lookup, so any LDM instance will
   * do: DBLQH instance 1, which has every table defined.
   * kAggWrongOperation is ZAGG_READ_WRONG_OPERATION in Dblqh.hpp. */
  static constexpr Uint32 kAggWrongOperation = 1868;

  int runP33() {
    const char *const findingId =
        "P-33 aggregation SCAN_FRAGREQ without an interpreted program";
    const Uint32 nodeId = pickDbNode();
    const Uint32 senderData = ++m_userPointer;
    SimpleSignal sig(false);
    Uint32 *d = sig.getDataPtrSend();
    std::memset(d, 0, sizeof(Uint32) * ScanFragReq::SignalLength);
    ScanFragReq *req = (ScanFragReq *)d;
    req->senderData = senderData;
    req->resultRef = m_ss->getOwnRef();
    req->savePointId = 0;
    Uint32 ri = 0;
    ScanFragReq::setReadCommittedFlag(ri, 1);
    ScanFragReq::setAggregationFlag(ri, 1);
    ScanFragReq::setNotInterpretedFlag(ri, 1);
    req->requestInfo = ri;
    req->tableId = m_tableId;
    req->fragmentNoKeyLen = 0;
    req->schemaVersion = m_schemaVersion;
    req->transId1 = 0x5b330000u + nodeId;
    req->transId2 = senderData;
    req->resultData = senderData;
    req->batch_size_rows = kBatchRows;
    req->batch_size_bytes = kBatchBytes;
    /* AttrInfo: a plain (not interpreted) read of column 0 */
    Uint32 attrInfo[1] = {0u << 16};
    sig.ptr[ScanFragReq::AttrInfoSectionNum].p = attrInfo;
    sig.ptr[ScanFragReq::AttrInfoSectionNum].sz = 1;
    sig.header.m_noOfSections = 1;

    Uint32 reply[25] = {0};
    m_ss->lock();
    const SendStatus sent =
        m_ss->sendSignal(nodeId, sig, numberToBlock(DBLQH, 1),
                         GSN_SCAN_FRAGREQ, ScanFragReq::SignalLength);
    const Uint32 gsn =
        sent == SEND_OK
            ? waitReplyLocked(GSN_SCAN_FRAGREF, GSN_SCAN_FRAGCONF, senderData,
                              nodeId, ReplyMillis, reply)
            : 0;
    m_ss->unlock();
    g_info << findingId << ": reply " << gsnName(gsn);
    if (gsn == GSN_SCAN_FRAGREF) g_info << " errorCode " << reply[3];
    g_info << endl;

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (gsn != GSN_SCAN_FRAGREF || reply[3] != kAggWrongOperation) {
      g_err << findingId << ": FAIL -- expected SCAN_FRAGREF "
            << kAggWrongOperation << ", got " << gsnName(gsn)
            << ": the request did not reach the check (a release data "
               "node drops a SCAN_FRAGREQ from an API node)" << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; SCAN_FRAGREF "
           << reply[3] << ")" << endl;
    return NDBT_OK;
  }

  int runP35() {  /* a linked entry that runs past the linked buffer */
    /* The one entry of the buffer is a BIGINT CTE column (word 0 is the
     * typed marker, so no column metadata is needed).  Its
     * AttributeHeader says 0x7FFF bytes, 8192 data words, and the
     * buffer ends after the header.  The GROUP BY walk copied 8193
     * words per column from past the end of the linked buffer into the
     * 18000-word DBTUP read buffer, so 16 columns overran the read
     * buffer by 113 088 words; the other walks read past the linked
     * buffer too.  The entry is now checked against the end of the
     * buffer (CteLinkedAttr::linkedEntryAH), and each row fails with
     * 1869.  The control is the same request with a well-formed entry,
     * the value 7, which must get SCAN_TABCONF (without it a harness
     * that never reaches the leaf rows would pass). */
    constexpr Uint32 kGbCols = 16;
    const std::vector<Uint32> leaf = linkedGroupByLeaf(kGbCols);
    const Uint32 w0 = CteLinkedAttr::encodeWord0(NDB_TYPE_BIGINT, 8);
    const Uint32 w1 = CteLinkedAttr::encodeWord1(0);

    SpjReq good;
    makeJoinAggScanLookup(good, {w0, w1, (0u << 16) | 8u, 7u, 0u}, leaf);
    const Reply control = sendScanTabReq(good);
    printReply("CONTROL (join aggregation, well-formed linked entry)",
               control);
    if (control.gsn != GSN_SCAN_TABCONF) {
      g_err << "P-35 CONTROL did not get SCAN_TABCONF (got "
            << gsnName(control.gsn) << ", err " << control.errorCode
            << ").  The raw scan+lookup join aggregation encoding is "
               "wrong; fix this before trusting the result of P-35."
            << endl;
      return clusterHealthy() ? NDBT_FAILED
                              : failNodeDown("P-35 control");
    }

    SpjReq bad;
    makeJoinAggScanLookup(bad, {w0, w1, (0u << 16) | 0x7FFFu}, leaf);
    bad.waitForEnd = true;
    const char *findingId = "P-35 linked entry past the linked buffer";
    const Reply rep = sendScanTabReq(bad);
    printReply(findingId, rep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (rep.gsn != GSN_SCAN_TABREF) {
      g_err << findingId << ": FAIL -- expected SCAN_TABREF, got "
            << gsnName(rep.gsn)
            << ": the malformed linked buffer was not rejected" << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; SCAN_TABREF "
           << rep.errorCode << ")" << endl;
    return NDBT_OK;
  }

  int runP36() {  /* inline-type CHAR compare with charset 0 */
    /* Charset 0 left the collation nullptr, and cmpChar dereferenced it
     * for a row with a linked value: a debug data node failed its
     * assert, a release one crashed.  The NDB API (branch_linked_inline_*,
     * NdbAggregator::EmitEmbeddedWord) sends the word as given. */
    return expectAggRejected(
        "P-36 join aggregation inline-type CHAR compare with charset 0",
        {inlineTypeLeaf(NDB_TYPE_CHAR, 0), countLeaf()});
  }

  int runP37() {  /* inline-type BINARY compare with a charset */
    /* latin1 (8) on a BINARY: cmpBinary asserts that it has no
     * collation, so a debug data node stopped. */
    return expectAggRejected(
        "P-37 join aggregation inline-type BINARY compare with a charset",
        {inlineTypeLeaf(NDB_TYPE_BINARY, 8), countLeaf()});
  }

  int runP40() {  /* a linked entry shorter than the type it is read as */
    /* READ_LINKED_COLUMN_TO_REG decoded up to 8 bytes from an entry
     * without checking the entry's byte size: a BIGINT read of a 4-byte
     * entry took 4 bytes from past its data, here past the end of the
     * linked buffer (RONDB-1124).  Such an entry now reads as NULL, as a
     * missing one does.  The leaf (linkedNullLeaf) goes on only for a
     * NULL, so the request with the 4-byte entry must get SCAN_TABCONF.
     * The controls: a NULL entry must get SCAN_TABCONF and an 8-byte one
     * SCAN_TABREF (without them a harness whose rows never reach the
     * program would pass).  The 8-byte one runs last, so that no request
     * expected to succeed follows a failed scan.  With the JIT on
     * (test_spj_malformed_p40_jit) the compiler refuses the leaf, whose
     * value path ends without output 0, and the rows run on the
     * interpreter; the compiled program counted the rows of the 8-byte
     * entry, which fail on the interpreter. */
    const std::vector<Uint32> leaf = linkedNullLeaf();
    const Uint32 w0 = CteLinkedAttr::encodeWord0(NDB_TYPE_BIGINT, 8);
    const Uint32 w1 = CteLinkedAttr::encodeWord1(0);
    const char *findingId = "P-40 linked entry shorter than its type";

    SpjReq nullEntry;
    makeJoinAggScanLookup(nullEntry, {w0, w1, (0u << 16) | 0u}, leaf);
    nullEntry.waitForEnd = true;
    const Reply nullRep = sendScanTabReq(nullEntry);
    printReply("CONTROL (NULL linked entry)", nullRep);

    SpjReq shortEntry;
    makeJoinAggScanLookup(shortEntry, {w0, w1, (0u << 16) | 4u, 7u}, leaf);
    shortEntry.waitForEnd = true;
    const Reply rep = sendScanTabReq(shortEntry);
    printReply(findingId, rep);

    SpjReq valueEntry;
    makeJoinAggScanLookup(valueEntry, {w0, w1, (0u << 16) | 8u, 7u, 0u},
                          leaf);
    valueEntry.waitForEnd = true;
    const Reply valueRep = sendScanTabReq(valueEntry);
    printReply("CONTROL (8-byte linked entry)", valueRep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (nullRep.gsn != GSN_SCAN_TABCONF || valueRep.gsn != GSN_SCAN_TABREF) {
      g_err << "P-40 CONTROLS: expected SCAN_TABCONF for the NULL entry and "
               "SCAN_TABREF for the 8-byte one, got "
            << gsnName(nullRep.gsn) << " and " << gsnName(valueRep.gsn)
            << ".  The rows do not reach the leaf program; fix this before "
               "trusting the result of P-40." << endl;
      return NDBT_FAILED;
    }
    if (rep.gsn != GSN_SCAN_TABCONF) {
      g_err << findingId << ": FAIL -- expected SCAN_TABCONF (the entry "
            << "reads as NULL), got " << gsnName(rep.gsn) << " "
            << rep.errorCode << ": the entry was decoded as a value"
            << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; the entry "
           << "read as NULL)" << endl;
    return NDBT_OK;
  }

  /* ---- P-41: the interpreted AttrInfo header of a plain key read ------ *
   * A TCKEYREQ, not ViaSPJ, for an interpreted dirty read of the row with
   * primary key 1.  Its AttrInfo section is the 5-word header
   * [initReadLen][interpretLen][finalUpdateLen][finalReadLen][subLen]
   * followed by one program word, and reaches Dbtup::interpreterStartLab
   * as the API sent it.  A dirty read is answered by the DBLQH that read
   * the row, with TCKEYREF carrying the operation pointer in word 0. */
  static constexpr Uint32 kRefuseCode = 899;     /* EXIT_REFUSE's code */
  static constexpr Uint32 kTotalLenError = 882;  /* ZTOTAL_LEN_ERROR */
  /* DBTUP reads the parameter area of a scan not from DBSPJ as one
   * parameter block per range (P-46, ERROR_codes.txt). */
  static constexpr int kParamBlocksInsert = 4047;

  Reply sendInterpretedRead(const std::vector<Uint32> &attrInfo) {
    Reply rep;
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) {
      rep.disconnected = true;
      return rep;
    }
    const Uint32 userPointer = ++m_userPointer;
    SimpleSignal sig(false);
    Uint32 *d = sig.getDataPtrSend();
    const Uint32 len = SpjMalformedSignals::fillTcKeyReq(
        d, apiConnectPtr, userPointer, m_tableId, m_schemaVersion,
        0x5b410000u + nodeId, userPointer);
    SpjMalformedSignals::setTcKeyReqInterpretedRead(d);
    Uint32 key[1] = {1};  /* KOL1, the primary key of a loaded row */
    std::vector<Uint32> ai(attrInfo);
    sig.ptr[TcKeyReq::KeyInfoSectionNum].p = key;
    sig.ptr[TcKeyReq::KeyInfoSectionNum].sz = 1;
    sig.ptr[TcKeyReq::AttrInfoSectionNum].p = ai.data();
    sig.ptr[TcKeyReq::AttrInfoSectionNum].sz = (Uint32)ai.size();
    sig.header.m_noOfSections = 2;

    m_ss->lock();
    const SendStatus sent =
        m_ss->sendSignal(nodeId, sig, refToBlock(tcRef), GSN_TCKEYREQ, len);
    Uint32 reply[25];
    if (sent == SEND_OK) {
      /* From whichever data node holds the row (node 0: any). */
      rep.gsn = waitReplyLocked(GSN_TCKEYCONF, GSN_TCKEYREF, userPointer, 0,
                                ReplyMillis, reply);
      if (rep.gsn == GSN_TCKEYREF) rep.errorCode = reply[3];
    } else {
      rep.disconnected = true;
    }
    m_ss->unlock();
    return rep;
  }

  int runP41() {  /* interpreted header lengths that wrap in 32 bits */
    /* interpreterStartLab summed the five lengths in 32 bits:
     * 0x80000000 + 0x80000001 is 1, so the section of 6 words passed
     * its check with an exec region of 2^31 words and the subroutine
     * region 8 GB past cinBuffer, where the CALL of the program jumped
     * (RONDB-1124).  The sum is now 64 bits and the read fails with
     * ZTOTAL_LEN_ERROR.  The control, the same read with a well-formed
     * header and the program EXIT_REFUSE, must fail with the refuse
     * code: without it a harness whose read never reaches the
     * interpreter would pass. */
    const char *findingId = "P-41 interpreted header lengths that wrap";
    const std::vector<Uint32> good = {
        0, 1, 0, 0, 0, Interpreter::EXIT_REFUSE | (kRefuseCode << 16)};
    const Reply control = sendInterpretedRead(good);
    printReply("CONTROL (interpreted read, EXIT_REFUSE)", control);
    if (control.gsn != GSN_TCKEYREF || control.errorCode != kRefuseCode) {
      g_err << "P-41 CONTROL did not get TCKEYREF " << kRefuseCode
            << " (got " << gsnName(control.gsn) << ", err "
            << control.errorCode << ").  The raw interpreted read is "
               "wrong; fix this before trusting the result of P-41."
            << endl;
      return clusterHealthy() ? NDBT_FAILED : failNodeDown(findingId);
    }

    const std::vector<Uint32> bad = {
        0, 0x80000000u, 0, 0, 0x80000001u,
        Interpreter::CALL | (0u << 16)};
    const Reply rep = sendInterpretedRead(bad);
    printReply(findingId, rep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (rep.gsn != GSN_TCKEYREF || rep.errorCode != kTotalLenError) {
      g_err << findingId << ": FAIL -- expected TCKEYREF " << kTotalLenError
            << ", got " << gsnName(rep.gsn) << " " << rep.errorCode
            << ": the wrapped header was not refused" << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; TCKEYREF "
           << rep.errorCode << ")" << endl;
    return NDBT_OK;
  }

  int runP43() {  /* GROUP BY metadata: a CHAR column with collation 0 */
    /* initGBTypesFromMetadata left the collation nullptr for charset 0.
     * The group hash table then compared the keys as bytes, but the
     * ORDER BY top-N compare of a CTE (compareGroupsByOrderSpec) called
     * cmpChar with it: a debug data node failed its assert, a release
     * one crashed.  The NDB API sends 0 for a column without a
     * collation. */
    return expectGroupByMetaRejected(
        "P-43 join aggregation GROUP BY CHAR column with collation 0",
        NDB_TYPE_CHAR, 0);
  }

  int runP44() {  /* GROUP BY metadata: an Unsigned column with latin1 */
    /* The group hash table compares the keys of a column with a
     * collation with its compare function, and cmpUnsigned asserts that
     * it has no collation: a debug data node stopped when two groups
     * met in one bucket. */
    return expectGroupByMetaRejected(
        "P-44 join aggregation GROUP BY Unsigned column with a collation",
        NDB_TYPE_UNSIGNED, 8);
  }

  int runP45() {  /* inline-type compare: lengths other than the staged */
    /* (a) The staged value is a CHAR(8) 'abcd    ', the constant the 4
     * bytes 'abcd', and the column size of the compare 12.  DBTUP
     * passed 12 and 4 to cmpChar, which asserts one length (a debug
     * data node stopped), and read 4 bytes past the staged value.  It
     * now compares no more than was staged, and CHAR values of
     * different lengths padded (cmpCharAnyLength): the two are equal,
     * every row is counted, SCAN_TABCONF.  This also shows that the
     * raw request reaches the compare. */
    const Uint32 w0Char = CteLinkedAttr::encodeWord0(NDB_TYPE_CHAR, 8);
    const Uint32 w1 = CteLinkedAttr::encodeWord1(kLatin1);
    SpjReq good;
    makeJoinAggScanLookup(
        good, {w0Char, w1, (0u << 16) | 8u, kAbcd, kFourSpaces},
        inlineCompareLeaf(NDB_TYPE_CHAR, 12, kLatin1, {kAbcd}, 4));
    good.waitForEnd = true;
    const char *goodId =
        "P-45a join aggregation CHAR(8) compared as CHAR(12) with 'abcd'";
    const Reply control = sendScanTabReq(good);
    printReply(goodId, control);
    NdbSleep_MilliSleep(SettleMillis);
    if (!clusterHealthy()) {
      return failNodeDown(goodId);
    }
    if (control.gsn != GSN_SCAN_TABCONF) {
      g_err << goodId << ": FAIL -- expected SCAN_TABCONF, got "
            << gsnName(control.gsn) << ", err " << control.errorCode
            << ": the compare did not find the padded values equal, or "
               "the raw request is wrong" << endl;
      return NDBT_FAILED;
    }
    g_info << goodId << ": PASS (all data nodes started; SCAN_TABCONF)"
           << endl;

    /* (b) The staged value is a VARCHAR(10) 'abcd' (length byte 4, 5
     * bytes), the column size of the compare 2.  cmpVarchar requires
     * the length byte to fit the size it is given, so every data node
     * stopped, debug or release.  The compare is now refused (error 40),
     * which fails the row: SCAN_TABREF. */
    const Uint32 w0Varchar = CteLinkedAttr::encodeWord0(NDB_TYPE_VARCHAR, 11);
    /* [4]'abc' 'd' */
    const std::vector<Uint32> varcharAbcd = {0x63626104u, 0x00000064u};
    SpjReq bad;
    makeJoinAggScanLookup(
        bad, {w0Varchar, w1, (0u << 16) | 5u, varcharAbcd[0],
              varcharAbcd[1]},
        inlineCompareLeaf(NDB_TYPE_VARCHAR, 2, kLatin1, varcharAbcd, 5));
    bad.waitForEnd = true;
    const char *badId =
        "P-45b join aggregation VARCHAR 'abcd' compared with size 2";
    const Reply rep = sendScanTabReq(bad);
    printReply(badId, rep);
    NdbSleep_MilliSleep(SettleMillis);
    if (!clusterHealthy()) {
      return failNodeDown(badId);
    }
    if (rep.gsn != GSN_SCAN_TABREF) {
      g_err << badId << ": FAIL -- expected SCAN_TABREF, got "
            << gsnName(rep.gsn)
            << ": the compare of a value longer than its size was not "
               "refused" << endl;
      return NDBT_FAILED;
    }
    g_info << badId << ": PASS (all data nodes started; SCAN_TABREF "
           << rep.errorCode << ")" << endl;
    return NDBT_OK;
  }

  /* ---- P-46: the parameter blocks of a multi-range scan --------------- *
   * A SCAN_TABREQ, not ViaSPJ, for a read-committed range scan of an
   * ordered index on column 0 (T1's KOL1) with two EQ ranges, KOL1 = 1
   * and KOL1 = 2, and one receiver, so DBTC scans one fragment at a
   * time.  Its AttrInfo section is an interpreted program, which DBTC
   * and DBLQH pass on as the API sent it:
   *   [0][1][0][1][paramLen] EXIT_OK, read KOL1, the parameter area
   * Every fragment scans both ranges.  For the second one of a scan from
   * DBSPJ, DBLQH has DBTUP move on to the next parameter block
   * (nextAttrInfoParam) and copy it to the start of the parameter area
   * (scanCopyAttrinfo).  DBSPJ writes one block per range,
   * [blockLen][blockLen - 1 words].  This scan comes from DBTC, so its
   * parameter area is the subroutine region of the program, used whole
   * for every range (P-49), unless error insert 4047 has DBTUP read it as
   * blocks, whose lengths the API then writes. */
  static std::vector<Uint32> rangeScanProgram(
      const std::vector<Uint32> &params) {
    std::vector<Uint32> ai = {0, 1, 0, 1, Uint32(params.size()),
                              Interpreter::ExitOK(),
                              AttributeHeader(0, 0).m_value};
    ai.insert(ai.end(), params.begin(), params.end());
    return ai;
  }

  /* An EQ range on index column 0, as NdbIndexScanOperation writes it:
   * [range length << 16 | range number << 4 | bound type], then the
   * bound, 4 bytes. */
  static void appendEqRange(std::vector<Uint32> &keyInfo, Uint32 rangeNo,
                            Uint32 value) {
    keyInfo.push_back((3u << 16) | (rangeNo << 4) |
                      Uint32(NdbIndexScanOperation::BoundEQ));
    keyInfo.push_back(AttributeHeader(0, 4).m_value);
    keyInfo.push_back(value);
  }

  int runP46(Uint32 indexId, Uint32 indexVersion) {
    /* For each range after the first, DBTUP read the length of its
     * parameter block at the offset reached by adding up the lengths of
     * the blocks before it, and copied that many words to the start of
     * the area.  Only ndbassert checked the offset and the length, so a
     * release data node read and wrote out of DBTUP's interpreter
     * buffer (8 GB here) and a debug one failed the assert.  The header
     * was summed in 32 bits, so the area itself could start 8 GB past
     * the buffer (RONDB-1124).  The header must now fit the section and
     * each block lie within the area, or the scan fails with
     * ZTOTAL_LEN_ERROR before it reads a row of the range.  The
     * control, two well-formed blocks of 2 and 3 words, must get
     * SCAN_TABCONF: without it a harness whose scan never reaches the
     * second range would pass.
     *
     * Only DBSPJ writes parameter blocks, and DBTUP now reads them only
     * for a scan from DBSPJ: for this scan from DBTC the parameter area
     * is the subroutine region of the program, whatever its words are
     * (P-49).  So P-46a and P-46b run under error insert 4047, which has
     * DBTUP read this scan's area as blocks, and the control runs once
     * more under it.  The insert needs data nodes built with
     * ERROR_INSERT; MTR runs this program from the same build as the
     * data nodes, so it runs them when it was built with ERROR_INSERT
     * itself.  P-46c, the header, is checked for every scan. */
    const char *findingId = "P-46 parameter blocks of a multi-range scan";
    const std::vector<Uint32> controlProgram =
        rangeScanProgram({2, 0x11111111u, 3, 0x22222222u, 0x33333333u});
    const Reply control =
        sendInterpretedRangeScan(indexId, indexVersion, controlProgram);
    printReply("CONTROL (two ranges, two well-formed parameter blocks)",
               control);
    if (control.gsn != GSN_SCAN_TABCONF) {
      g_err << "P-46 CONTROL did not get SCAN_TABCONF (got "
            << gsnName(control.gsn) << ", err " << control.errorCode
            << ").  The raw range scan is wrong; fix this before trusting "
               "the result of P-46." << endl;
      return clusterHealthy() ? NDBT_FAILED : failNodeDown(findingId);
    }

    const char *const ids[3] = {
        /* nextAttrInfoParam moved the offset 8 GB past the buffer. */
        "P-46a the parameter block of the first range runs past the area",
        /* scanCopyAttrinfo copied 8 GB into the buffer. */
        "P-46b the parameter block of the second range runs past the area",
        /* The area starts at word 0x80000005 and 0x80000005 +
         * 0x80000001 is 6 in 32 bits, which passed the ndbassert of
         * the 7-word section.  Each row fails in interpreterStartLab
         * (P-41), but for a fragment without KOL1 = 1 nextAttrInfoParam
         * read the first block 8 GB past the cached section. */
        "P-46c header lengths that wrap in 32 bits"};
    const std::vector<Uint32> programs[3] = {
        rangeScanProgram({0x7FFFFFFFu}),
        rangeScanProgram({2, 0x11111111u, 0x7FFFFFFFu}),
        {0, 0x80000000u, 0, 0, 0x80000001u, Interpreter::ExitOK(),
         AttributeHeader(0, 0).m_value}};
    Reply reps[3];
    bool ran[3] = {false, false, false};
    /* P-46c without the insert: the header is checked for every scan. */
    reps[2] = sendInterpretedRangeScan(indexId, indexVersion, programs[2]);
    ran[2] = true;
    printReply(ids[2], reps[2]);
    int result = NDBT_OK;
#ifdef ERROR_INSERT
    Reply blocksControl;
    if (m_restarter->insertErrorInAllNodes(kParamBlocksInsert) != 0) {
      g_err << "P-46: inserting error " << kParamBlocksInsert
            << " failed" << endl;
      result = NDBT_FAILED;
    } else {
      blocksControl =
          sendInterpretedRangeScan(indexId, indexVersion, controlProgram);
      printReply("CONTROL under error insert 4047 (the two well-formed "
                 "blocks read as blocks)",
                 blocksControl);
      for (int i = 0; i < 2; i++) {
        reps[i] =
            sendInterpretedRangeScan(indexId, indexVersion, programs[i]);
        ran[i] = true;
        printReply(ids[i], reps[i]);
      }
    }
    if (m_restarter->insertErrorInAllNodes(0) != 0) {
      g_err << "P-46: clearing error " << kParamBlocksInsert << " failed"
            << endl;
      result = NDBT_FAILED;
    }
#else
    g_info << "P-46a, P-46b: not run, they need error insert "
           << kParamBlocksInsert
           << ", which only data nodes built with ERROR_INSERT have"
           << endl;
#endif

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
#ifdef ERROR_INSERT
    if (ran[0] && blocksControl.gsn != GSN_SCAN_TABCONF) {
      g_err << "P-46 CONTROL under error insert " << kParamBlocksInsert
            << ": FAIL -- expected SCAN_TABCONF, got "
            << gsnName(blocksControl.gsn) << " " << blocksControl.errorCode
            << ": well-formed blocks were refused" << endl;
      result = NDBT_FAILED;
    }
#endif
    for (int i = 0; i < 3; i++) {
      if (!ran[i]) {
        continue;
      }
      if (reps[i].gsn != GSN_SCAN_TABREF ||
          reps[i].errorCode != kTotalLenError) {
        g_err << ids[i] << ": FAIL -- expected SCAN_TABREF "
              << kTotalLenError << ", got " << gsnName(reps[i].gsn) << " "
              << reps[i].errorCode << ": the program was not refused"
              << endl;
        result = NDBT_FAILED;
      } else {
        g_info << ids[i] << ": PASS (all data nodes started; SCAN_TABREF "
               << reps[i].errorCode << ")" << endl;
      }
    }
    return result;
  }

  /* ---- P-49: a multi-range scan whose program calls a subroutine ------ *
   * Not raw signals: an NdbIndexScanOperation of the ordered index on
   * column 0 (T1's KOL1) with two ranges, KOL1 in [1, 4] and KOL1 in
   * [10, 13], and an NdbInterpretedCode program whose subroutine 0 reads
   * KOL1 into register 1; the main program calls it and refuses the rows
   * whose KOL1 is 3 or 12:
   *   call_sub 0; R2 = 3; R1 == R2 -> refuse; R2 = 12; R1 == R2 -> refuse;
   *   exit_ok; refuse: exit_nok; sub 0: R1 = KOL1; ret_sub
   * NdbScanOperation sends the subroutine as the parameter area of the
   * program (header word 4), and DBTC passes it on.  From the second
   * range on DBTUP read that area as parameter blocks, which only DBSPJ
   * writes: the subroutine's first word, a READ_ATTR_INTO_REG
   * instruction, is no block length within the 2-word area, so every
   * such scan failed with 882 (RONDB-1124).  Now every range of a scan
   * that is not from DBSPJ runs the whole program, and the scan must
   * return KOL1 1, 2 and 4 of the first range and 10, 11 and 13 of the
   * second. */
  int runP49(Ndb *pNdb, const NdbDictionary::Table *tab,
             const NdbDictionary::Index *index) {
    const char *const findingId =
        "P-49 multi-range scan whose program calls a subroutine";
    const Uint32 kol1AttrId = 0;
    const char *const kol1Name = tab->getColumn(kol1AttrId)->getName();
    const Uint32 regKol1 = 1;
    const Uint32 regConst = 2;
    const Uint32 refuseLabel = 0;
    const Uint32 refusedInFirst = 3;
    const Uint32 refusedInSecond = 12;
    NdbInterpretedCode code(tab);
    if (code.call_sub(0) != 0 ||
        code.load_const_u32(regConst, refusedInFirst) != 0 ||
        code.branch_eq(regKol1, regConst, refuseLabel) != 0 ||
        code.load_const_u32(regConst, refusedInSecond) != 0 ||
        code.branch_eq(regKol1, regConst, refuseLabel) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(int(refuseLabel)) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_sub(0) != 0 ||
        code.read_attr(regKol1, kol1AttrId) != 0 ||
        code.ret_sub() != 0 ||
        code.finalise() != 0) {
      g_err << findingId << ": building the program failed: "
            << code.getNdbError() << endl;
      return NDBT_FAILED;
    }

    /* [low, high] of each range, both inclusive */
    const Uint32 bounds[2][2] = {{1, 4}, {10, 13}};
    const std::vector<Uint32> expected = {1, 2, 4, 10, 11, 13};

    NdbTransaction *trans = pNdb->startTransaction();
    if (trans == nullptr) {
      g_err << findingId << ": startTransaction failed: "
            << pNdb->getNdbError() << endl;
      return NDBT_FAILED;
    }
    NdbIndexScanOperation *op = trans->getNdbIndexScanOperation(index);
    NdbRecAttr *kol1Value = nullptr;
    bool defined =
        op != nullptr &&
        op->readTuples(NdbOperation::LM_CommittedRead,
                       NdbScanOperation::SF_MultiRange) == 0;
    for (Uint32 r = 0; defined && r < 2; r++) {
      defined = op->setBound(kol1Name, NdbIndexScanOperation::BoundLE,
                             &bounds[r][0]) == 0 &&
                op->setBound(kol1Name, NdbIndexScanOperation::BoundGE,
                             &bounds[r][1]) == 0 &&
                op->end_of_bound(r) == 0;
    }
    if (defined) {
      defined = op->setInterpretedCode(&code) == 0 &&
                (kol1Value = op->getValue(kol1Name)) != nullptr;
    }
    if (!defined) {
      g_err << findingId << ": defining the scan failed: "
            << trans->getNdbError() << endl;
      trans->close();
      return NDBT_FAILED;
    }

    std::vector<Uint32> rows;
    int errorCode = 0;
    if (trans->execute(NdbTransaction::NoCommit) != 0) {
      errorCode = trans->getNdbError().code;
    } else {
      int rc;
      while ((rc = op->nextResult(true)) == 0) {
        rows.push_back(kol1Value->u_32_value());
      }
      if (rc != 1) {
        errorCode = op->getNdbError().code;
        if (errorCode == 0) errorCode = trans->getNdbError().code;
        if (errorCode == 0) errorCode = -1;
      }
    }
    trans->close();

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (errorCode != 0) {
      g_err << findingId << ": FAIL -- the scan failed with error "
            << errorCode
            << (errorCode == int(kTotalLenError)
                    ? ": DBTUP read the subroutine of the program as "
                      "parameter blocks at the second range"
                    : "")
            << endl;
      return NDBT_FAILED;
    }
    std::sort(rows.begin(), rows.end());
    if (rows != expected) {
      g_err << findingId << ": FAIL -- expected KOL1";
      for (Uint32 v : expected) g_err << " " << v;
      g_err << ", got";
      for (Uint32 v : rows) g_err << " " << v;
      g_err << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; KOL1 1, 2, 4 "
           << "of the first range, 10, 11, 13 of the second)" << endl;
    return NDBT_OK;
  }

  int runP48() {  /* a short linked entry: the JIT, an outer join */
    /* P-40 tests that DBTUP reads a linked entry shorter than its type
     * as NULL, but on the interpreter only, and for a row that was
     * found.  P-48 tests the two other readers of such an entry, each
     * with a 4-byte entry read as a BIGINT, and with the controls of a
     * NULL entry and an 8-byte one (without them a harness whose rows
     * never reach the program would pass).
     *
     * The compare (linkedCompareLeaf), on rows that are found: with the
     * JIT on (test_spj_malformed_p48_jit) the program runs compiled.
     * ndb_jit_h_load_linked_col decoded 8 bytes from the entry, and the
     * row was counted; it now takes the per-row fallback, where DBTUP
     * reads NULL and fails the row, so the request must get
     * SCAN_TABREF, as for a NULL entry, while the 8-byte entry gets
     * SCAN_TABCONF.  With the JIT off the rows run on DBTUP's
     * interpreter, with the same results.
     *
     * The outer join (linkedNullLeaf, the leaf of P-40, on parent rows
     * that have no match, makeOuterJoinAggScanLookup): DBLQH runs the
     * program for NULL-extended rows.  Its evaluator failed the row for
     * the short entry (EMB_NULL_ROW_ERROR, 1872); it reads it as NULL
     * now, so the request must get SCAN_TABCONF, as for a NULL entry,
     * while the 8-byte entry gets SCAN_TABREF.  NULL-extended rows
     * never run on the JIT.
     *
     * Requests expected to succeed run first, so that none of them
     * follows a failed scan. */
    const std::vector<Uint32> compareLeaf = linkedCompareLeaf();
    const std::vector<Uint32> nullLeaf = linkedNullLeaf();
    const Uint32 w0 = CteLinkedAttr::encodeWord0(NDB_TYPE_BIGINT, 8);
    const Uint32 w1 = CteLinkedAttr::encodeWord1(0);
    const std::vector<Uint32> nullEntry = {w0, w1, (0u << 16) | 0u};
    const std::vector<Uint32> shortEntry = {w0, w1, (0u << 16) | 4u, 7u};
    const std::vector<Uint32> valueEntry = {w0, w1, (0u << 16) | 8u, 7u,
                                            0u};
    const char *cmpId = "P-48 compare of a linked entry shorter than its type";
    const char *ojId =
        "P-48 outer join NULL row, linked entry shorter than its type";

    SpjReq cmpValue;
    makeJoinAggScanLookup(cmpValue, valueEntry, compareLeaf);
    cmpValue.waitForEnd = true;
    const Reply cmpValueRep = sendScanTabReq(cmpValue);
    printReply("CONTROL (compare, 8-byte linked entry)", cmpValueRep);

    SpjReq ojNull;
    makeOuterJoinAggScanLookup(ojNull, nullEntry, nullLeaf);
    ojNull.waitForEnd = true;
    const Reply ojNullRep = sendScanTabReq(ojNull);
    printReply("CONTROL (outer join NULL row, NULL linked entry)",
               ojNullRep);

    SpjReq ojShort;
    makeOuterJoinAggScanLookup(ojShort, shortEntry, nullLeaf);
    ojShort.waitForEnd = true;
    const Reply ojRep = sendScanTabReq(ojShort);
    printReply(ojId, ojRep);

    SpjReq ojValue;
    makeOuterJoinAggScanLookup(ojValue, valueEntry, nullLeaf);
    ojValue.waitForEnd = true;
    const Reply ojValueRep = sendScanTabReq(ojValue);
    printReply("CONTROL (outer join NULL row, 8-byte linked entry)",
               ojValueRep);

    SpjReq cmpNull;
    makeJoinAggScanLookup(cmpNull, nullEntry, compareLeaf);
    cmpNull.waitForEnd = true;
    const Reply cmpNullRep = sendScanTabReq(cmpNull);
    printReply("CONTROL (compare, NULL linked entry)", cmpNullRep);

    SpjReq cmpShort;
    makeJoinAggScanLookup(cmpShort, shortEntry, compareLeaf);
    cmpShort.waitForEnd = true;
    const Reply cmpRep = sendScanTabReq(cmpShort);
    printReply(cmpId, cmpRep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown("P-48");
    }
    if (cmpValueRep.gsn != GSN_SCAN_TABCONF ||
        cmpNullRep.gsn != GSN_SCAN_TABREF) {
      g_err << "P-48 CONTROLS (compare): expected SCAN_TABCONF for the "
               "8-byte entry and SCAN_TABREF for the NULL one, got "
            << gsnName(cmpValueRep.gsn) << " and " << gsnName(cmpNullRep.gsn)
            << ".  The rows do not reach the leaf program; fix this before "
               "trusting the result of P-48." << endl;
      return NDBT_FAILED;
    }
    if (ojNullRep.gsn != GSN_SCAN_TABCONF ||
        ojValueRep.gsn != GSN_SCAN_TABREF) {
      g_err << "P-48 CONTROLS (outer join NULL row): expected SCAN_TABCONF "
               "for the NULL entry and SCAN_TABREF for the 8-byte one, got "
            << gsnName(ojNullRep.gsn) << " and " << gsnName(ojValueRep.gsn)
            << ".  The NULL-extended rows do not reach the leaf program; "
               "fix this before trusting the result of P-48." << endl;
      return NDBT_FAILED;
    }
    if (cmpRep.gsn != GSN_SCAN_TABREF) {
      g_err << cmpId << ": FAIL -- expected SCAN_TABREF (the entry reads "
            << "as NULL), got " << gsnName(cmpRep.gsn)
            << ": the entry was decoded as a value (with the JIT on, by "
               "ndb_jit_h_load_linked_col)" << endl;
      return NDBT_FAILED;
    }
    if (ojRep.gsn != GSN_SCAN_TABCONF) {
      g_err << ojId << ": FAIL -- expected SCAN_TABCONF (the entry "
            << "reads as NULL), got " << gsnName(ojRep.gsn) << " "
            << ojRep.errorCode << ": the NULL-row evaluator failed the row"
            << endl;
      return NDBT_FAILED;
    }
    g_info << "P-48: PASS (all data nodes started; the entry read as NULL "
           << "on found rows and on NULL-extended rows)" << endl;
    return NDBT_OK;
  }

  /* ---- P-50: the subroutine section of an interpreted AttrInfo ------- *
   * DBSPJ gives the AttrInfo of an interpreted node the 5-word header
   * [initReadLen][interpretLen][finalUpdateLen][finalReadLen][subLen]
   * (Dbspj::parseDA).  The subroutine section, word 4, is DBTUP's
   * parameter area, where an NI_ATTR_LINKED node puts the parameters
   * that DBSPJ expands from each parent row.  Two parts of a request
   * went there too, through code that stopped a debug data node on
   * ndbassert(false) ("Need validation before being used"):
   *   the subroutine part of a PI_ATTR_INTERPRET program,
   *     [(subroutine length << 16) | program length][program][subroutines]
   *   the pattern of an NI_ATTR_INTERPRET node without NI_ATTR_LINKED,
   *     [(pattern length << 16) | program length][program][pattern]
   * The NDB API sends neither.  DBSPJ now refuses both. */

  /* Gives the parameters of the node at params[at], whose fixed part has
   * fixedSize words, the PI_ATTR_INTERPRET part interp, in front of its
   * PI_ATTR_LIST, where parseDA reads it. */
  static void addParamInterpret(SpjReq &r, Uint32 at, Uint32 fixedSize,
                                Uint32 opType,
                                const std::vector<Uint32> &interp) {
    const Uint32 len = QueryNodeParameters::getLength(r.params[at]);
    ((QueryNodeParameters *)&r.params[at])->requestInfo |=
        DABits::PI_ATTR_INTERPRET;
    r.params.insert(r.params.begin() + at + fixedSize, interp.begin(),
                    interp.end());
    /* The insert may have reallocated; re-fetch. */
    QueryNodeParameters::setOpLen(((QueryNodeParameters *)&r.params[at])->len,
                                  opType, len + Uint32(interp.size()));
  }

  /* The scan of the control with the program interp in its parameters,
   * as the NDB API sends a pushed scan filter. */
  void makeParamInterpretedScan(SpjReq &r,
                                const std::vector<Uint32> &interp) {
    makeWellFormedScan(r);
    addParamInterpret(r, 0, QN_ScanFragParameters::NodeSize,
                      QueryNodeParameters::QN_SCAN_FRAG, interp);
  }

  /* The scan of the control with the program in its tree node: the node
   * has NI_ATTR_INTERPRET, and part is its optional part 3. */
  void makeTreeInterpretedScan(SpjReq &r, const std::vector<Uint32> &part) {
    makeWellFormedScan(r);
    ((QueryNode *)&r.tree[1])->requestInfo = DABits::NI_ATTR_INTERPRET;
    r.tree.insert(r.tree.end(), part.begin(), part.end());
    QueryNode::setOpLen(((QueryNode *)&r.tree[1])->len,
                        QueryNode::QN_SCAN_FRAG,
                        QN_ScanFragNode::NodeSize + Uint32(part.size()));
    QueryTree::setCntLen(r.tree[0], 1, (Uint32)r.tree.size());
  }

  /* The join aggregation of the P-35 control (its leaf an NI_ATTR_LINKED
   * lookup, which DBSPJ gives a linked buffer of one well-formed BIGINT
   * entry at each send), counting the rows, with the program interp in
   * the parameters of the leaf, as the NDB API sends an aggregation leaf
   * with linked columns (NdbQueryOperationImpl::prepareAttrInfo). */
  void makeLinkedLeafInterpreted(SpjReq &r,
                                 const std::vector<Uint32> &interp) {
    const Uint32 w0 = CteLinkedAttr::encodeWord0(NDB_TYPE_BIGINT, 8);
    const Uint32 w1 = CteLinkedAttr::encodeWord1(0);
    makeJoinAggScanLookup(r, {w0, w1, (0u << 16) | 8u, 7u, 0u}, countLeaf());
    addParamInterpret(r, QueryNodeParameters::getLength(r.params[0]),
                      QN_LookupParameters::NodeSize,
                      QueryNodeParameters::QN_LOOKUP, interp);
  }

  int runP50() {
    /* Each request first without the part that went to word 4, which
     * must get SCAN_TABCONF (without it a harness whose request never
     * reaches the parse of the program would pass), then with it, which
     * must get SCAN_TABREF with the error of the refusal:
     *   a  a scan whose PI_ATTR_INTERPRET program has a subroutine part
     *      of one word.  A release data node put it in the parameter
     *      area; a debug one stopped.
     *   b  the same subroutine part on an NI_ATTR_LINKED aggregation
     *      leaf.  A release data node stopped as well, on the
     *      ndbrequire that no parameter area was built for a node that
     *      constructs its own at each send.
     *   c  a scan whose NI_ATTR_INTERPRET program in the tree node is
     *      followed by a pattern of a P_DATA of one word, without
     *      NI_ATTR_LINKED.  A release data node put the word in the
     *      parameter area; a debug one stopped.
     * The malformed requests run after every control, b last. */
    const Uint32 exitOk = Interpreter::ExitOK();
    const std::vector<Uint32> program = {1u, exitOk};
    const std::vector<Uint32> withSubroutine = {(1u << 16) | 1u, exitOk, 0u};
    const std::vector<Uint32> withPattern = {
        (2u << 16) | 1u, exitOk, QueryPattern::data(1), 0x11111111u};
    const char *const controlIds[3] = {
        "CONTROL (P-50a scan program without a subroutine part)",
        "CONTROL (P-50b aggregation leaf program without a subroutine "
        "part)",
        "CONTROL (P-50c tree node program without a pattern)"};
    const char *const ids[3] = {
        "P-50a scan program with a subroutine part",
        "P-50b NI_ATTR_LINKED aggregation leaf program with a subroutine "
        "part",
        "P-50c tree node program with a pattern but no NI_ATTR_LINKED"};
    const Uint32 expected[3] = {
        Uint32(DbspjErr::InvalidTreeParametersSpecification),
        Uint32(DbspjErr::InvalidTreeParametersSpecification),
        Uint32(DbspjErr::InvalidTreeNodeSpecification)};

    SpjReq good[3];
    makeParamInterpretedScan(good[0], program);
    makeLinkedLeafInterpreted(good[1], program);
    makeTreeInterpretedScan(good[2], program);
    SpjReq bad[3];
    makeParamInterpretedScan(bad[0], withSubroutine);
    makeLinkedLeafInterpreted(bad[1], withSubroutine);
    makeTreeInterpretedScan(bad[2], withPattern);
    bad[1].waitForEnd = true;

    for (int i = 0; i < 3; i++) {
      const Reply control = sendScanTabReq(good[i]);
      printReply(controlIds[i], control);
      if (control.gsn != GSN_SCAN_TABCONF) {
        g_err << controlIds[i] << " did not get SCAN_TABCONF (got "
              << gsnName(control.gsn) << ", err " << control.errorCode
              << ").  The raw interpreted request is wrong; fix this "
                 "before trusting the result of P-50." << endl;
        return clusterHealthy() ? NDBT_FAILED : failNodeDown(controlIds[i]);
      }
    }

    const int order[3] = {0, 2, 1};
    Reply reps[3];
    for (int k = 0; k < 3; k++) {
      const int i = order[k];
      reps[i] = sendScanTabReq(bad[i]);
      printReply(ids[i], reps[i]);
    }

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown("P-50");
    }
    int result = NDBT_OK;
    for (int i = 0; i < 3; i++) {
      if (reps[i].gsn != GSN_SCAN_TABREF ||
          reps[i].errorCode != expected[i]) {
        g_err << ids[i] << ": FAIL -- expected SCAN_TABREF " << expected[i]
              << ", got " << gsnName(reps[i].gsn) << " "
              << reps[i].errorCode << ": the request was not refused"
              << endl;
        result = NDBT_FAILED;
      } else {
        g_info << ids[i] << ": PASS (all data nodes started; SCAN_TABREF "
               << reps[i].errorCode << ")" << endl;
      }
    }
    return result;
  }

  int runP51() {  /* a null test of a register a path did not write */
    /* The leaf (linkedSkipLoadLeaf) skips the load of register 3 for a
     * linked value of 7, so its BRANCH_REG_EQ_NULL of register 3 is
     * taken and the row fails; for another value the row is counted.
     * The JIT folds such a guard to "not taken", which is right for a
     * register that every path wrote: on a row the JIT completes, it
     * holds a value.  With the JIT on (test_spj_malformed_p51_jit) the
     * compiled program counted the rows of a 7 (RONDB-1124); the
     * compiler now refuses a program that reads a register a path did
     * not write, and the rows run on the interpreter.  The request with
     * an entry of 7 must get SCAN_TABREF, and the control, an entry of
     * 8, SCAN_TABCONF (without it a harness whose rows never reach the
     * program would pass).  The control runs first, so that no request
     * expected to succeed follows a failed scan. */
    const std::vector<Uint32> leaf = linkedSkipLoadLeaf();
    const Uint32 w0 = CteLinkedAttr::encodeWord0(NDB_TYPE_BIGINT, 8);
    const Uint32 w1 = CteLinkedAttr::encodeWord1(0);
    const char *findingId =
        "P-51 null test of a register a path did not write";

    SpjReq eight;
    makeJoinAggScanLookup(eight, {w0, w1, (0u << 16) | 8u, 8u, 0u}, leaf);
    eight.waitForEnd = true;
    const Reply eightRep = sendScanTabReq(eight);
    printReply("CONTROL (linked value 8)", eightRep);

    SpjReq seven;
    makeJoinAggScanLookup(seven, {w0, w1, (0u << 16) | 8u, 7u, 0u}, leaf);
    seven.waitForEnd = true;
    const Reply rep = sendScanTabReq(seven);
    printReply(findingId, rep);

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (eightRep.gsn != GSN_SCAN_TABCONF) {
      g_err << "P-51 CONTROL: expected SCAN_TABCONF for the linked value 8, "
               "got " << gsnName(eightRep.gsn) << " " << eightRep.errorCode
            << ".  The rows do not reach the leaf program; fix this before "
               "trusting the result of P-51." << endl;
      return NDBT_FAILED;
    }
    if (rep.gsn != GSN_SCAN_TABREF) {
      g_err << findingId << ": FAIL -- expected SCAN_TABREF (register 3 is "
            << "NULL at the guard), got " << gsnName(rep.gsn)
            << ": the guard was not taken (with the JIT on, the compiled "
               "program folded it)" << endl;
      return NDBT_FAILED;
    }
    g_info << findingId << ": PASS (all data nodes started; the guard "
           << "was taken)" << endl;
    return NDBT_OK;
  }

  /* ---- P-38 / P-39: a string MIN / MAX and a number in one slot ------- *
   * A string MIN / MAX keeps in its slot a pointer to the data node's
   * copy of the value; every other aggregate keeps a number there.  A
   * CASE whose arms mix them (bug 1c of the RonSQL analysis) let a
   * numeric MIN overwrite the pointer, or minMaxString take a number
   * for a pointer, and the data node crashed.  NdbAggregator::RepeatAgg
   * refuses such arms, but EmitEmbeddedWord takes raw words.
   * PushdownInterpreter::checkAggSlotKinds (in checkProgram) refuses the
   * program at JOIN_AGG_SETUP_REQ.  The program is leaf 1, which no row
   * runs: before the check DblqhProxy accepted it and the scan got
   * SCAN_TABCONF.  Each case first sends the same CASE with arms of one
   * kind, which must still be accepted. */
  int runP38() {  /* THEN: string MIN, ELSE: kOpMinBigint, one slot */
    const int control = expectAggAccepted(
        "P-38 control: CASE with VARCHAR MIN in both arms",
        {countLeaf(),
         caseLeaf(colArm(0, NDB_TYPE_VARCHAR, 1, kOpMin),
                  colArm(1, NDB_TYPE_VARCHAR, 2, kOpMin), {})});
    if (control != NDBT_OK) {
      return control;
    }
    return expectAggRejected(
        "P-38 join aggregation CASE with a VARCHAR MIN and a BIGINT MIN "
        "in one slot",
        {countLeaf(),
         caseLeaf(colArm(0, NDB_TYPE_VARCHAR, 1, kOpMin),
                  constArm(1, NDB_TYPE_BIGINT, 1, 0, kOpMinBigint), {})});
  }

  int runP39() {  /* THEN: a BIGINT, ELSE: a CHAR, one MAX after */
    /* One kOpMax after the CASE reads a register that the THEN arm loads
     * with a number and the ELSE arm with a string.  The control's arms
     * are a BIGINT and a DOUBLE (1.0), numbers of two types, which
     * OptimizeProgramBuffer leaves to the generic Max (bug 1). */
    const int control = expectAggAccepted(
        "P-39 control: CASE with a BIGINT and a DOUBLE, then MAX",
        {countLeaf(),
         caseLeaf(constArm(0, NDB_TYPE_BIGINT, 1, 0, kOpUnknown),
                  constArm(0, NDB_TYPE_DOUBLE, 0, 0x3FF00000u, kOpUnknown),
                  {aggUpdate(kOpMax, 0, 0)})});
    if (control != NDBT_OK) {
      return control;
    }
    return expectAggRejected(
        "P-39 join aggregation MAX of a register that is a BIGINT or a "
        "CHAR",
        {countLeaf(),
         caseLeaf(constArm(0, NDB_TYPE_BIGINT, 1, 0, kOpUnknown),
                  colArm(0, NDB_TYPE_CHAR, 1, kOpUnknown),
                  {aggUpdate(kOpMax, 0, 0)})});
  }

  /* ---- Q cases: DBTC's rate queue re-send mark sent by the API -------- *
   * DBTC's rate queue handler re-sends a queued request to its own DBTC
   * instance with a mark: the passQueue flag of TCKEYREQ, TCINDXREQ and
   * SCAN_TABREQ, bit 2 of SCAN_NEXTREQ's stopScan.  DBTC took the mark
   * from any sender.  From an API it skipped the rate limit, and since a
   * request that was never queued holds none of the counts the mark takes
   * it off, an ndbrequire stopped the data node.  DBTC now reports such a
   * request as tc_queue_flag_not_internal (Tier A, disconnect); the MTR
   * wrappers check the count.  An invalid connection pointer reaches the
   * ndbrequire(!flag) of the error paths. */
  static constexpr Uint32 kInvalidConnectPtr = 0xFFFFFF00u; /* RNIL */

  int runQ1() {  /* TCKEYREQ with passQueue -> ndbrequire(queued > 0) */
    SpjReq r;
    makeWellFormedScan(r);
    r.viaScan = false;
    r.keyInfo.assign(1, 0x00000001u);
    r.passQueue = true;
    return expectSurvive("Q-1 TCKEYREQ with the rate queue flag", r);
  }

  int runQ2() {  /* long TCINDXREQ with passQueue, invalid connection */
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) return NDBT_FAILED;
    std::vector<Uint32> words(TcKeyReq::StaticLength);
    const Uint32 userPointer = ++m_userPointer;
    SpjMalformedSignals::fillTcKeyReq(words.data(), kInvalidConnectPtr,
                                      userPointer, m_tableId, m_schemaVersion,
                                      0x5b200000u + nodeId, userPointer);
    SpjMalformedSignals::setTcKeyReqPassQueue(words.data());
    return expectSurviveRaw("Q-2 TCINDXREQ with the rate queue flag", nodeId,
                            tcRef, GSN_TCINDXREQ, words,
                            std::vector<Uint32>(1, 0x00000001u));
  }

  int runQ3() {  /* SCAN_NEXTREQ sent-from-queue bit, invalid connection */
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) return NDBT_FAILED;
    /* apiConnectPtr, stopScan (bit 2: sent from the queue), transId1,
     * transId2 */
    const std::vector<Uint32> words = {kInvalidConnectPtr, 2u,
                                       0x5b300000u + nodeId, ++m_userPointer};
    return expectSurviveRaw("Q-3 SCAN_NEXTREQ marked as sent from the queue",
                            nodeId, tcRef, GSN_SCAN_NEXTREQ, words,
                            std::vector<Uint32>());
  }

  int runQ4() {  /* short TCINDXREQ, key length 16, invalid connection */
    /* Not a malicious request: bit 24 of a short TCINDXREQ is bit 4 of
     * its key length, here 16 words (8 in the signal, the rest would
     * follow in KEYINFO).  DBTC read it as the passQueue flag, and its
     * ndbrequire(!passQueueingFlag) on an invalid or released connection
     * stopped the data node.  It must be dropped without a report. */
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) return NDBT_FAILED;
    std::vector<Uint32> words(TcKeyReq::StaticLength);
    const Uint32 userPointer = ++m_userPointer;
    SpjMalformedSignals::fillTcKeyReq(words.data(), kInvalidConnectPtr,
                                      userPointer, m_tableId, m_schemaVersion,
                                      0x5b400000u + nodeId, userPointer);
    SpjMalformedSignals::setTcKeyReqKeyLength(words.data(), 16);
    words.resize(TcKeyReq::StaticLength + TcKeyReq::MaxKeyInfo, 0x01010101u);
    return expectSurviveRaw("Q-4 short TCINDXREQ with a 16-word key", nodeId,
                            tcRef, GSN_TCINDXREQ, words,
                            std::vector<Uint32>());
  }

  int runQ5() {  /* SCAN_TABREQ with passQueue */
    /* Not a crash before: DBTC took the flag only from a DBTC block. */
    SpjReq r;
    makeWellFormedScan(r);
    r.passQueue = true;
    return expectSurvive("Q-5 SCAN_TABREQ with the rate queue flag", r);
  }

  /* ---- S cases: join aggregation signals from the API ----------------- *
   * DBTC, DBSPJ, DBLQH and the DBLQH proxy pass the JOIN_AGG_* and CTE_*
   * signals between data nodes only, so SignalScopes.hpp declares them
   * Remote, or Local for the three that never leave their node.  They
   * had no scope, so a data node ran their handlers for an API node too.
   * A data node now drops a Remote one that an API node sends and
   * reports it as signal_scope_remote_violation (violation 32, Tier A:
   * the API node is disconnected); the MTR wrappers check the count. */

  int runS1() {  /* JOIN_AGG_SEND_REQ to DBTC */
    /* DBTC answered a JOIN_AGG_SEND_REQ from any node with a
     * JOIN_AGG_SEND_CONF to the block reference in the request, so an
     * API node could make DBTC send a SEND_CONF to any DBLQH.  No block
     * unit test sends SEND_REQ, so it is Remote in debug builds too and
     * the case runs against debug and release data nodes.  The request
     * names us: a SEND_CONF back means that DBTC took it.  Its word 0
     * is the reference of the DBTC instance that answers. */
    const char *const findingId = "S-1 JOIN_AGG_SEND_REQ from the API to DBTC";
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) return NDBT_FAILED;
    SimpleSignal sig(false);
    JoinAggSendReq *req =
        reinterpret_cast<JoinAggSendReq *>(sig.getDataPtrSend());
    req->senderRef = m_ss->getOwnRef();
    req->senderData = ++m_userPointer;
    req->requestId = 0;
    req->aggStateKey = 0;
    req->numRowsSent = 0;
    req->resultBytes = 0;
    Uint32 reply[25] = {0};
    m_ss->lock();
    const SendStatus sent =
        m_ss->sendSignal(nodeId, sig, refToBlock(tcRef),
                         GSN_JOIN_AGG_SEND_REQ, JoinAggSendReq::SignalLength);
    /* The wait also gives a crash time to surface. */
    const Uint32 gsn =
        sent == SEND_OK
            ? waitReplyLocked(GSN_JOIN_AGG_SEND_CONF, GSN_JOIN_AGG_SEND_CONF,
                              tcRef, nodeId, SettleMillis, reply)
            : 0;
    m_ss->unlock();
    g_info << findingId << ": sent " << (sent == SEND_OK ? "OK" : "FAILED")
           << endl;

    if (!clusterHealthy()) {
      return failNodeDown(findingId);
    }
    if (gsn == GSN_JOIN_AGG_SEND_CONF) {
      g_err << findingId << ": FAIL -- DBTC answered with JOIN_AGG_SEND_CONF: "
               "the data node took a JOIN_AGG_SEND_REQ from an API node"
            << endl;
      return NDBT_FAILED;
    }
    g_info << findingId
           << ": PASS (all data nodes started; no JOIN_AGG_SEND_CONF)" << endl;
    return NDBT_OK;
  }

  int runS2() {  /* JOIN_AGG_FINAL_REP to DBLQH, sender node past the max */
    /* execJOIN_AGG_FINAL_REP stopped the data node on
     * ndbrequire(senderNodeId < ABS_MAX_NDB_NODES), so one FINAL_REP
     * from an API node stopped a data node.  The block unit tests send
     * FINAL_REP from an API node, so a debug data node still takes it
     * (NDB_SCOPE_REMOTE_EXCEPT_DEBUG in SignalScopes.hpp) and stops: the
     * case needs release data nodes, and its MTR wrapper skips on debug
     * ones.  The check comes before any state lookup, so any LDM
     * instance will do: DBLQH instance 1. */
    const char *const findingId =
        "S-2 JOIN_AGG_FINAL_REP from the API, sender node past the maximum";
    const Uint32 nodeId = pickDbNode();
    SimpleSignal sig(false);
    JoinAggFinalRep *rep =
        reinterpret_cast<JoinAggFinalRep *>(sig.getDataPtrSend());
    rep->aggStateKey = 0;
    rep->senderNodeId = 0xFFFF;
    rep->identWord = 0xFFFFFF00u; /* RNIL */
    rep->transid[0] = 0x5b510000u + nodeId;
    rep->transid[1] = ++m_userPointer;
    rep->redistributeCountLo = 0;
    rep->redistributeCountHi = 0;
    rep->errorCode = 0;
    m_ss->lock();
    const SendStatus sent =
        m_ss->sendSignal(nodeId, sig, numberToBlock(DBLQH, 1),
                         GSN_JOIN_AGG_FINAL_REP, JoinAggFinalRep::SignalLength);
    m_ss->unlock();
    g_info << findingId << ": sent " << (sent == SEND_OK ? "OK" : "FAILED")
           << endl;

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (clusterHealthy()) {
      g_info << findingId << ": PASS (all data nodes started)" << endl;
      return NDBT_OK;
    }
    return failNodeDown(findingId);
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
  /* The cteIndex of the main program's column metadata block (RNIL) */
  static constexpr Uint32 kNoCteIndex = 0xFFFFFF00u;
  static constexpr Uint32 kLatin1 = 8;     /* latin1_swedish_ci */
  static constexpr Uint32 kAbcd = 0x64636261u;        /* "abcd" */
  static constexpr Uint32 kFourSpaces = 0x20202020u;  /* "    " */

  static const char *gsnName(Uint32 gsn) {
    switch (gsn) {
      case GSN_SCAN_TABCONF: return "SCAN_TABCONF";
      case GSN_SCAN_TABREF: return "SCAN_TABREF";
      case GSN_TCKEYCONF: return "TCKEYCONF";
      case GSN_TCKEYREF: return "TCKEYREF";
      case GSN_SCAN_FRAGCONF: return "SCAN_FRAGCONF";
      case GSN_SCAN_FRAGREF: return "SCAN_FRAGREF";
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
      if (r.passQueue) SpjMalformedSignals::setScanTabReqPassQueue(d);
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
      if (r.passQueue) SpjMalformedSignals::setTcKeyReqPassQueue(d);
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

  /* Send the P-46 range scan of the ordered index with the AttrInfo
   * section attrInfo, and wait for its first reply. */
  Reply sendInterpretedRangeScan(Uint32 indexId, Uint32 indexVersion,
                                 const std::vector<Uint32> &attrInfo) {
    Reply rep;
    const Uint32 nodeId = pickDbNode();
    Uint32 apiConnectPtr = 0, tcRef = 0, seizeUp = 0;
    if (!seize(nodeId, apiConnectPtr, tcRef, seizeUp)) {
      rep.disconnected = true;
      return rep;
    }
    const Uint32 userPointer = ++m_userPointer;
    SimpleSignal sig(false);
    Uint32 *d = sig.getDataPtrSend();
    const Uint32 len = SpjMalformedSignals::fillScanTabReq(
        d, apiConnectPtr, indexId, indexVersion, 0x5b460000u + nodeId,
        userPointer, kBatchRows, kBatchBytes, false, 1);
    SpjMalformedSignals::setScanTabReqPlainRangeScan(d);

    std::vector<Uint32> ai(attrInfo);
    std::vector<Uint32> keyInfo;
    appendEqRange(keyInfo, 0, 1);
    appendEqRange(keyInfo, 1, 2);
    std::vector<Uint32> rcvIds(1, kReceiverId);
    sig.ptr[ScanTabReq::ReceiverIdSectionNum].p = rcvIds.data();
    sig.ptr[ScanTabReq::ReceiverIdSectionNum].sz = (Uint32)rcvIds.size();
    sig.ptr[ScanTabReq::AttrInfoSectionNum].p = ai.data();
    sig.ptr[ScanTabReq::AttrInfoSectionNum].sz = (Uint32)ai.size();
    sig.ptr[ScanTabReq::KeyInfoSectionNum].p = keyInfo.data();
    sig.ptr[ScanTabReq::KeyInfoSectionNum].sz = (Uint32)keyInfo.size();
    sig.header.m_noOfSections = 3;

    m_ss->lock();
    const int sent = m_ss->sendFragmentedSignal(nodeId, sig, refToBlock(tcRef),
                                                GSN_SCAN_TABREQ, len);
    Uint32 reply[25];
    if (sent == SEND_OK) {
      /* The first fragment's SCAN_TABCONF (it has read both ranges), or
       * the SCAN_TABREF of the scan. */
      rep.gsn = waitReplyLocked(GSN_SCAN_TABCONF, GSN_SCAN_TABREF, seizeUp,
                                nodeId, ReplyMillis, reply);
      if (rep.gsn == GSN_SCAN_TABREF) rep.errorCode = reply[3];
    } else {
      rep.disconnected = true;
    }
    m_ss->unlock();
    return rep;
  }

  /* Caller holds the lock.  Return the first gsn1/gsn2 from nodeId (any
   * node for 0) whose word 0 is word0Match, or 0 on timeout. */
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
          (nodeId == 0 ||
           refToNode(s->header.theSendersBlockRef) == nodeId) &&
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

  /* Send one signal of `words` with up to one section to the DBTC
   * instance tcRef on nodeId, expect no reply, then require every data
   * node to still be started. */
  int expectSurviveRaw(const char *findingId, Uint32 nodeId, Uint32 tcRef,
                       Uint16 gsn, std::vector<Uint32> words,
                       std::vector<Uint32> section0) {
    SimpleSignal sig(false);
    std::memcpy(sig.getDataPtrSend(), words.data(),
                sizeof(Uint32) * words.size());
    Uint32 nSec = 0;
    if (!section0.empty()) {
      sig.ptr[0].p = section0.data();
      sig.ptr[0].sz = (Uint32)section0.size();
      nSec = 1;
    }
    sig.header.m_noOfSections = nSec;
    m_ss->lock();
    const SendStatus sent = m_ss->sendSignal(nodeId, sig, refToBlock(tcRef),
                                             gsn, (Uint32)words.size());
    m_ss->unlock();
    g_info << findingId << ": sent " << (sent == SEND_OK ? "OK" : "FAILED")
           << endl;

    /* Give a crash time to surface, then check status. */
    NdbSleep_MilliSleep(SettleMillis);

    if (clusterHealthy()) {
      g_info << findingId << ": PASS (all data nodes started)" << endl;
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
  /* A join aggregation whose leaves the data node must accept: it must
   * get SCAN_TABCONF, as the control of expectAggRejected.  Send it
   * before any malformed request, which may disconnect us. */
  int expectAggAccepted(const char *findingId,
                        const std::vector<std::vector<Uint32>> &leaves) {
    SpjReq r;
    makeJoinAggScan(r, leaves);
    const Reply rep = sendScanTabReq(r);
    printReply(findingId, rep);
    if (rep.gsn != GSN_SCAN_TABCONF) {
      g_err << findingId << ": FAIL -- expected SCAN_TABCONF, got "
            << gsnName(rep.gsn) << ", err " << rep.errorCode
            << ": a well-formed aggregation program was rejected" << endl;
      return clusterHealthy() ? NDBT_FAILED : failNodeDown(findingId);
    }
    g_info << findingId << ": PASS (SCAN_TABCONF)" << endl;
    return NDBT_OK;
  }

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

  /* A GROUP BY metadata case: first the join aggregation of
   * localGroupByLeaf with the metadata of its column (Unsigned, 4 bytes,
   * no collation), which must get SCAN_TABCONF, then the same with
   * typeId and csNumber in the metadata, which JOIN_AGG_SETUP_REQ must
   * refuse (DbspjErr::InvalidRequest) with every data node started. */
  int expectGroupByMetaRejected(const char *findingId, Uint32 typeId,
                                Uint32 csNumber) {
    SpjReq good;
    makeJoinAggScan(good, {localGroupByLeaf()});
    appendGroupByMeta(good, NDB_TYPE_UNSIGNED, 4, 0);
    const Reply control = sendScanTabReq(good);
    printReply("CONTROL (join aggregation with GROUP BY metadata)", control);
    if (control.gsn != GSN_SCAN_TABCONF) {
      g_err << "GROUP BY METADATA CONTROL did not get SCAN_TABCONF (got "
            << gsnName(control.gsn) << ", err " << control.errorCode
            << ").  The raw column metadata encoding is wrong; fix this "
               "before trusting the result of "
            << findingId << "." << endl;
      return clusterHealthy() ? NDBT_FAILED : failNodeDown(findingId);
    }

    SpjReq r;
    makeJoinAggScan(r, {localGroupByLeaf()});
    appendGroupByMeta(r, typeId, 4, csNumber);
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
            << ": the GROUP BY metadata was not rejected" << endl;
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
CASE_STEP(runP33, runP33)
CASE_STEP(runP35, runP35)
CASE_STEP(runP36, runP36)
CASE_STEP(runP37, runP37)
CASE_STEP(runP40, runP40)
CASE_STEP(runP41, runP41)
CASE_STEP(runP43, runP43)
CASE_STEP(runP44, runP44)
CASE_STEP(runP45, runP45)
CASE_STEP(runP48, runP48)
CASE_STEP(runP50, runP50)
CASE_STEP(runP51, runP51)
CASE_STEP(runP38, runP38)
CASE_STEP(runP39, runP39)
CASE_STEP(runQ1, runQ1)
CASE_STEP(runQ2, runQ2)
CASE_STEP(runQ3, runQ3)
CASE_STEP(runQ4, runQ4)
CASE_STEP(runQ5, runQ5)
CASE_STEP(runS1, runS1)
CASE_STEP(runS2, runS2)

/* Creates the ordered index indexName on column 0 of tab, after dropping
 * one that an earlier run left.  Returns it, or nullptr. */
const NdbDictionary::Index *createOrderedIndexOnColumn0(
    NdbDictionary::Dictionary *dict, const NdbDictionary::Table *tab,
    const char *indexName, const char *caseId) {
  (void)dict->dropIndex(indexName, tab->getName());  /* an earlier run's */
  NdbDictionary::Index idx(indexName);
  idx.setTable(tab->getName());
  idx.setType(NdbDictionary::Index::OrderedIndex);
  idx.setLogging(false);
  idx.addColumnName(tab->getColumn(0)->getName());
  if (dict->createIndex(idx) != 0) {
    g_err << caseId << ": creating ordered index " << indexName
          << " failed: " << dict->getNdbError() << endl;
    return nullptr;
  }
  const NdbDictionary::Index *created =
      dict->getIndex(indexName, tab->getName());
  if (created == nullptr) {
    g_err << caseId << ": ordered index " << indexName << " not found: "
          << dict->getNdbError() << endl;
  }
  return created;
}

/* P-46 scans an ordered index on column 0 of the table, which the step
 * creates and drops after the case. */
int runP46(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = nullptr;
  SignalSender *ss = nullptr;
  NdbRestarter *restarter = nullptr;
  SpjMalformedChecker *c = makeChecker(ctx, step, pNdb, ss, restarter);
  if (c == nullptr) return NDBT_FAILED;
  const NdbDictionary::Table *tab = ctx->getTab();
  NdbDictionary::Dictionary *dict = pNdb->getDictionary();
  const char *const indexName = "P46_ORDERED_COL0";
  int res = NDBT_FAILED;
  const NdbDictionary::Index *created =
      createOrderedIndexOnColumn0(dict, tab, indexName, "P-46");
  if (created != nullptr) {
    res = c->runP46(Uint32(created->getObjectId()),
                    Uint32(created->getObjectVersion()));
  }
  (void)dict->dropIndex(indexName, tab->getName());
  delete c;
  delete ss;
  delete restarter;
  return res;
}

/* P-49 scans an ordered index on column 0 of the table through the NDB
 * API; the step creates the index and drops it after the case. */
int runP49(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = nullptr;
  SignalSender *ss = nullptr;
  NdbRestarter *restarter = nullptr;
  SpjMalformedChecker *c = makeChecker(ctx, step, pNdb, ss, restarter);
  if (c == nullptr) return NDBT_FAILED;
  const NdbDictionary::Table *tab = ctx->getTab();
  NdbDictionary::Dictionary *dict = pNdb->getDictionary();
  const char *const indexName = "P49_ORDERED_COL0";
  int res = NDBT_FAILED;
  const NdbDictionary::Index *created =
      createOrderedIndexOnColumn0(dict, tab, indexName, "P-49");
  if (created != nullptr) {
    res = c->runP49(pNdb, tab, created);
  }
  (void)dict->dropIndex(indexName, tab->getName());
  delete c;
  delete ss;
  delete restarter;
  return res;
}

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
TESTCASE("P33",
         "aggregation SCAN_FRAGREQ without an interpreted program, sent "
         "straight to DBLQH (debug data nodes only)") {
  STEP(runP33);
}
TESTCASE("P35",
         "join aggregation: a linked attribute entry that runs past the "
         "linked buffer") {
  STEP(runP35);
}
TESTCASE("P36",
         "join aggregation: inline-type CHAR compare with charset 0") {
  STEP(runP36);
}
TESTCASE("P37",
         "join aggregation: inline-type BINARY compare with a charset") {
  STEP(runP37);
}
TESTCASE("P40",
         "join aggregation: a linked attribute entry shorter than the "
         "type it is read as") {
  STEP(runP40);
}
TESTCASE("P41",
         "interpreted read whose AttrInfo header lengths wrap in 32 "
         "bits") {
  STEP(runP41);
}
TESTCASE("P43",
         "join aggregation: GROUP BY CHAR column metadata with collation 0") {
  STEP(runP43);
}
TESTCASE("P44",
         "join aggregation: GROUP BY Unsigned column metadata with a "
         "collation") {
  STEP(runP44);
}
TESTCASE("P45",
         "join aggregation: inline-type compare of values of other "
         "lengths than the staged one") {
  STEP(runP45);
}
TESTCASE("P46",
         "interpreted range scan with two ranges whose parameter blocks "
         "run past the parameter area (the block cases on data nodes "
         "built with ERROR_INSERT only)") {
  STEP(runP46);
}
TESTCASE("P48",
         "join aggregation: a linked attribute entry shorter than its "
         "type, compared (on the JIT when it is on) and on an outer "
         "join's NULL-extended row") {
  STEP(runP48);
}
TESTCASE("P49",
         "NDB API index scan with two ranges whose program calls a "
         "subroutine returns the rows of both ranges") {
  STEP(runP49);
}
TESTCASE("P50",
         "interpreted programs with a subroutine part, or a pattern "
         "without NI_ATTR_LINKED, which went to the subroutine section "
         "of the AttrInfo") {
  STEP(runP50);
}
TESTCASE("P51",
         "join aggregation: a null test of an embedded register that a "
         "path did not write (refused by the JIT when it is on)") {
  STEP(runP51);
}
TESTCASE("P38",
         "join aggregation: string MIN and BIGINT MIN in one slot") {
  STEP(runP38);
}
TESTCASE("P39",
         "join aggregation: MAX of a register that is a BIGINT or a CHAR") {
  STEP(runP39);
}
TESTCASE("Q1", "TCKEYREQ with the rate queue flag from the API") {
  STEP(runQ1);
}
TESTCASE("Q2",
         "TCINDXREQ with the rate queue flag from the API, invalid "
         "connection") {
  STEP(runQ2);
}
TESTCASE("Q3",
         "SCAN_NEXTREQ marked as sent from the rate queue by the API, "
         "invalid connection") {
  STEP(runQ3);
}
TESTCASE("Q4",
         "short TCINDXREQ whose key length sets the rate queue flag bit, "
         "invalid connection") {
  STEP(runQ4);
}
TESTCASE("Q5", "SCAN_TABREQ with the rate queue flag from the API") {
  STEP(runQ5);
}
TESTCASE("S1", "JOIN_AGG_SEND_REQ from the API to DBTC") {
  STEP(runS1);
}
TESTCASE("S2",
         "JOIN_AGG_FINAL_REP from the API, sender node past the maximum "
         "(release data nodes only)") {
  STEP(runS2);
}
NDBT_TESTSUITE_END(testSpjMalformed)

int main(int argc, const char **argv) {
  ndb_init();
  NDBT_TESTSUITE_INSTANCE(testSpjMalformed);
  return testSpjMalformed.execute(argc, argv);
}
