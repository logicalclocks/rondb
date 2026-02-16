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
 * bench_q12_dbtc — TPC-H Q12 benchmark through the full DBTC→DBSPJ→DBLQH path.
 *
 * Sends SCAN_TABREQ to DBTC with a 2-node QueryTree encoding:
 *   Node 0: QN_SCAN_FRAG on LINEITEM (passes l_orderkey + l_shipmode to child)
 *   Node 1: QN_LOOKUP on ORDERS (aggregate leaf, GROUP BY l_shipmode)
 *
 * Unlike bench_q12_tpch (which sends signals directly to DBLQH), this
 * exercises the full orchestration: DBTC handles JOIN_AGG_SETUP/COMPLETE/RELEASE
 * internally, and DBSPJ coordinates the scan-to-lookup join.
 *
 * The TPC-H Q12 WHERE clause (l_shipmode IN ('MAIL','SHIP'), date range,
 * ordering predicates) is pushed down to data nodes as an NDB interpreted
 * program in QN_ScanFragParameters (PI_ATTR_INTERPRET).  Results are
 * validated against expected values computed from the known data pattern.
 *
 * Usage: bench_q12_dbtc -c <connect_string> -m <mysql_port> [options]
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

#ifdef NONE
#undef NONE
#endif
#include <kernel/Interpreter.hpp>

#include <NdbRestarter.hpp>
#include <mysql.h>

#include <cassert>
#include <chrono>
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
static bool noFilter = false;
static MYSQL *g_mysql_conn = nullptr;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *LINEITEM_TABLE = "q12_lineitem";
static const char *ORDERS_TABLE   = "q12_orders";

static const Uint32 FAKE_TRANS_ID1 = 0xBE4D0001;
static const Uint32 FAKE_TRANS_ID2 = 0xBE4D0002;
static const Uint32 WAIT_TIMEOUT_MS = 60000;

static const Uint32 COL_TYPE_BIGINT = 9;
static const Uint32 AGG_MAGIC = 0x0721;
static const Uint32 AGG_RESULT_ATTR = 0xFF00;
static const Uint32 LINKED_COL_FLAG = 0x8000;

/*
 * Epoch days from 1992-01-01.
 * TPC-H Q12 filters l_receiptdate in [1994-01-01, 1995-01-01).
 * 1994-01-01 = 366(1992 leap) + 365(1993) = day 731
 * 1995-01-01 = 731 + 365 = day 1096
 */
static const Uint32 DATE_19940101 = 731;
static const Uint32 DATE_19950101 = 1096;

/* TPC-H shipmode values (CHAR(10), space-padded) */
static const char SHIPMODES[][11] = {
  "REG AIR   ", "AIR       ", "RAIL      ",
  "SHIP      ", "TRUCK     ", "MAIL      ", "FOB       "
};
static const Uint32 NUM_SHIPMODES = 7;

/* TPC-H order priority values (CHAR(15), space-padded) */
static const char PRIORITIES[][16] = {
  "1-URGENT       ", "2-HIGH         ", "3-MEDIUM       ",
  "4-NOT SPECIFIED", "5-LOW          "
};
static const Uint32 NUM_PRIORITIES = 5;

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
/* Aggregation program builder — Q12 with embedded CASE                */
/* ------------------------------------------------------------------ */

/*
 * Reused from bench_q12_tpch.  Builds aggregation program for ORDERS leaf:
 *   GROUP BY l_shipmode (linked CHAR(10) from parent)
 *   SUM(CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH') THEN 1 ELSE 0)
 *   SUM(CASE WHEN ... THEN 0 ELSE 1)
 */
static std::vector<Uint32>
buildAggProgram_Q12(Uint32 linkedShipmodeAttrId, Uint32 localPriorityAttrId)
{
  const Uint32 EMB_LEN = 18;
  const Uint32 AGG_AFTER = 17;
  const Uint32 PROG_LEN = 8 + 1 + 1 + EMB_LEN + AGG_AFTER;

  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 2u;
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  prog[8] = (LINKED_COL_FLAG | linkedShipmodeAttrId) << 16;

  prog[9] = (kOpEmbeddedInterp << 26) | EMB_LEN;

  Uint32 pos = 10;

  /* BRANCH_ATTR_OP_ARG EQ '1-URGENT' → emb[15] */
  prog[pos++] = Interpreter::BranchCol(Interpreter::EQ,
                                        Interpreter::NULL_CMP_EQUAL) |
                (15u << 16);
  prog[pos++] = Interpreter::BranchCol_2(localPriorityAttrId, 15);
  {
    char buf[16];
    memset(buf, ' ', 15);
    memcpy(buf, "1-URGENT", 8);
    buf[15] = 0;
    memcpy(&prog[pos], buf, 16);
    pos += 4;
  }

  /* BRANCH_ATTR_OP_ARG EQ '2-HIGH' → emb[15] */
  prog[pos++] = Interpreter::BranchCol(Interpreter::EQ,
                                        Interpreter::NULL_CMP_EQUAL) |
                (9u << 16);
  prog[pos++] = Interpreter::BranchCol_2(localPriorityAttrId, 15);
  {
    char buf[16];
    memset(buf, ' ', 15);
    memcpy(buf, "2-HIGH", 6);
    buf[15] = 0;
    memcpy(&prog[pos], buf, 16);
    pos += 4;
  }

  /* ELSE: LOAD_CONST16 reg2, 9; WRITE_INTERPRETER_OUTPUT; EXIT_OK */
  prog[pos++] = Interpreter::LOAD_CONST16 | (2 << 6) | (9u << 16);
  prog[pos++] = Interpreter::WriteInterpreterOutput(2, 0);
  prog[pos++] = Interpreter::ExitOK();

  /* THEN: LOAD_CONST16 reg2, 0; WRITE_INTERPRETER_OUTPUT; EXIT_OK */
  prog[pos++] = Interpreter::LOAD_CONST16 | (2 << 6) | (0u << 16);
  prog[pos++] = Interpreter::WriteInterpreterOutput(2, 0);
  prog[pos++] = Interpreter::ExitOK();

  assert(pos == 28);

  /* Agg instructions: THEN path (skip=0) */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 1; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 0;

  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 0; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 1;

  prog[pos++] = (kOpSkip << 26) | 8;

  /* ELSE path (skip=9) */
  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 0; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 0;

  prog[pos++] = (kOpLoadConst << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16);
  { Int64 v = 1; memcpy(&prog[pos], &v, sizeof(Int64)); pos += 2; }
  prog[pos++] = (kOpSum << 26) | (0 << 16) | 1;

  assert(pos == PROG_LEN);
  return prog;
}

/* ------------------------------------------------------------------ */
/* Pushdown WHERE filter (NDB interpreted program)                     */
/* ------------------------------------------------------------------ */

/*
 * Build an NDB interpreted program for the TPC-H Q12 WHERE clause:
 *
 *   l_shipmode IN ('MAIL', 'SHIP')
 *   AND l_commitdate < l_receiptdate
 *   AND l_shipdate < l_commitdate
 *   AND l_receiptdate >= DATE_19940101
 *   AND l_receiptdate < DATE_19950101
 *
 * This is pushed down to data nodes via PI_ATTR_INTERPRET in the
 * QN_ScanFragParameters section.  DBSPJ appends it to the 5-word
 * interpreter header and forwards it in SCAN_FRAGREQ to DBLQH,
 * where it executes before reading attributes.
 */
static std::vector<Uint32>
buildScanFilter(const NdbDictionary::Table *tab, const TableMeta &meta)
{
  Uint32 buf[256];
  NdbInterpretedCode code(tab, buf, sizeof(buf) / sizeof(buf[0]));

  Uint32 shipmode_id = meta.attrIds.at("l_shipmode");
  Uint32 shipdate_id = meta.attrIds.at("l_shipdate");
  Uint32 commitdate_id = meta.attrIds.at("l_commitdate");
  Uint32 receiptdate_id = meta.attrIds.at("l_receiptdate");

  static const char MAIL_PADDED[10] = {'M','A','I','L',' ',' ',' ',' ',' ',' '};
  static const char SHIP_PADDED[10] = {'S','H','I','P',' ',' ',' ',' ',' ',' '};

  /*
   * NB: NDB interpreter inequality branches are INVERTED from their names!
   * (see NdbScanFilter.cpp line 560: "implemented backwards")
   *   branch_col_lt → actually branches when col > val
   *   branch_col_le → actually branches when col >= val
   *   branch_col_gt → actually branches when col < val
   *   branch_col_ge → actually branches when col <= val
   * EQ and NE are correct (not inverted).
   *
   * l_shipmode IN ('MAIL', 'SHIP'):
   *   if l_shipmode == 'MAIL' → jump to label 0 (pass_shipmode)
   *   if l_shipmode != 'SHIP' → jump to label 1 (reject)
   *   fall through = l_shipmode == 'SHIP' → continue
   */
  code.branch_col_eq(MAIL_PADDED, 10, shipmode_id, 0);
  code.branch_col_ne(SHIP_PADDED, 10, shipmode_id, 1);
  code.def_label(0);  /* pass_shipmode */

  /* l_commitdate < l_receiptdate: reject if commitdate >= receiptdate */
  code.branch_col_le(commitdate_id, receiptdate_id, 1);

  /* l_shipdate < l_commitdate: reject if shipdate >= commitdate */
  code.branch_col_le(shipdate_id, commitdate_id, 1);

  /* l_receiptdate >= DATE_19940101: reject if receiptdate < 731 */
  Uint32 val731 = DATE_19940101;
  code.branch_col_gt(&val731, sizeof(val731), receiptdate_id, 1);

  /* l_receiptdate < DATE_19950101: reject if receiptdate >= 1096 */
  Uint32 val1096 = DATE_19950101;
  code.branch_col_le(&val1096, sizeof(val1096), receiptdate_id, 1);

  code.interpret_exit_ok();
  code.def_label(1);  /* reject */
  code.interpret_exit_nok();

  int rc = code.finalise();
  if (rc != 0) {
    fprintf(stderr, "NdbInterpretedCode::finalise() failed: %d\n", rc);
    return {};
  }

  /*
   * getWordsUsed() includes label meta-info words stored at the end of the
   * buffer (2 words per label).  The actual interpreter program is only the
   * first m_instructions_length words, but that field is private.  Subtract
   * the meta-info overhead: 2 labels × 2 words each = 4 words.
   */
  Uint32 numLabels = 2;
  Uint32 len = code.getWordsUsed() - numLabels * 2;

  if (verbose) {
    printf("Scan filter program: %u words (getWordsUsed=%u, labels=%u)\n",
           len, code.getWordsUsed(), numLabels);
    /* Walk the program and decode opcodes */
    Uint32 pc = 0;
    while (pc < len) {
      Uint32 w0 = buf[pc];
      Uint32 opcode = w0 & 0x3f;
      Uint32 isize = 1;  /* default instruction size */
      switch (opcode) {
        case Interpreter::BRANCH_ATTR_OP_ARG: {
          Uint32 cond = (w0 >> 12) & 0xf;
          Uint32 offset = (w0 >> 16) & 0x7fff;
          Uint32 dir = w0 >> 31;
          Uint32 w1 = buf[pc + 1];
          Uint32 attrId = (w1 >> 16) & 0xffff;
          Uint32 vlen = w1 & 0xffff;
          Uint32 dataWords = (vlen + 3) / 4;
          printf("  [%2u] BRANCH_ATTR_OP_ARG cond=%u attrId=%u len=%u "
                 "offset=%s%u  (data %u words)\n",
                 pc, cond, attrId, vlen, dir ? "-" : "+", offset, dataWords);
          /* Print inline data in hex */
          for (Uint32 d = 0; d < dataWords; d++)
            printf("       data[%u] = H'%08x\n", d, buf[pc + 2 + d]);
          isize = 2 + dataWords;
          break;
        }
        case Interpreter::BRANCH_ATTR_OP_ATTR: {
          Uint32 cond = (w0 >> 12) & 0xf;
          Uint32 offset = (w0 >> 16) & 0x7fff;
          Uint32 dir = w0 >> 31;
          Uint32 w1 = buf[pc + 1];
          Uint32 attrId1 = (w1 >> 16) & 0xffff;
          Uint32 attrId2 = w1 & 0xffff;
          printf("  [%2u] BRANCH_ATTR_OP_ATTR cond=%u attrId1=%u attrId2=%u "
                 "offset=%s%u\n",
                 pc, cond, attrId1, attrId2, dir ? "-" : "+", offset);
          isize = 2;
          break;
        }
        case Interpreter::EXIT_OK:
          printf("  [%2u] EXIT_OK\n", pc);
          break;
        case Interpreter::EXIT_REFUSE:
          printf("  [%2u] EXIT_REFUSE\n", pc);
          break;
        default:
          printf("  [%2u] UNKNOWN opcode=%u  H'%08x\n", pc, opcode, w0);
          break;
      }
      pc += isize;
    }
    /* Print raw hex for full verification */
    printf("Raw hex:");
    for (Uint32 i = 0; i < len; i++) printf(" %08x", buf[i]);
    printf("\n");
  }

  return std::vector<Uint32>(buf, buf + len);
}

/* ------------------------------------------------------------------ */
/* QueryTree builder — 2-table join: LINEITEM scan + ORDERS lookup     */
/* ------------------------------------------------------------------ */

/*
 * Build AttrInfo containing an SPJ QueryTree for 2-table join with
 * pushdown aggregation.  The AttrInfo encodes both the tree section
 * (node definitions) and the parameter section (per-node runtime params).
 *
 * == Signal flow ==
 *
 * API sends SCAN_TABREQ to DBTC with 3 sections:
 *   Section 0: ReceiverIds
 *   Section 1: AttrInfo  (QueryTree + Parameters, built here)
 *   Section 2: KeyInfo   (AggProgram — the aggregation bytecode)
 *
 * DBTC extracts the AggProgram from KeyInfo and sends
 * JOIN_AGG_SETUP_REQ to each data node (via DblqhProxy), which creates
 * an AggInterpreter per LDM thread.  Then DBTC forwards the AttrInfo
 * to DBSPJ via SCAN_FRAGREQ.
 *
 * DBSPJ parses the QueryTree and, for each parent row from the root
 * scan, sends LQHKEYREQ to DBLQH for child lookups.  When a child
 * node has T_ATTRINFO_CONSTRUCTED (set by NI_ATTR_LINKED), DBSPJ
 * dynamically constructs the attrInfo per row by expanding the
 * m_attrParamPattern with linked parent column data (see the extensive
 * comment in DbspjMain.cpp at the T_ATTRINFO_CONSTRUCTED block).
 *
 * DBLQH calls DBTUP::execTUPKEYREQ.  For NI_AGGREGATE_LEAF nodes,
 * DBTUP runs the aggregation interpreter on each matched row.  The
 * linked parent data (e.g. l_shipmode) is extracted from the
 * subroutine section of the interpreter buffer, skipping the paramLen
 * word prepended by DBSPJ.
 *
 * == QueryTree encoding ==
 *
 *   Node 0: QN_SCAN_FRAG on LINEITEM (root scan)
 *     requestInfo: NI_AGGREGATE | NI_LINKED_ATTR
 *     tableId, tableVersion
 *     NI_LINKED_ATTR packed list: [l_orderkey, l_shipmode]
 *       Format: word0 = (first_attrId << 16) | count
 *               word1 = second_attrId (remaining attrs packed 2 per word)
 *     The linked attrs are read for each scan row and forwarded to the
 *     child.  l_orderkey is used as the lookup key (via NI_KEY_LINKED
 *     on the child).  l_shipmode is forwarded as linked data for the
 *     GROUP BY (via NI_ATTR_LINKED + P_ATTRINFO pattern on the child).
 *
 *   Node 1: QN_LOOKUP on ORDERS (aggregate leaf)
 *     requestInfo: NI_HAS_PARENT | NI_KEY_LINKED |
 *                  NI_AGGREGATE | NI_AGGREGATE_LEAF |
 *                  NI_ATTR_INTERPRET | NI_ATTR_LINKED
 *     tableId, tableVersion
 *     NI_HAS_PARENT: parent = node 0, distance = 1
 *     NI_KEY_LINKED: key pattern = [P_COL(0)] → l_orderkey from parent
 *     NI_ATTR_INTERPRET: ExitOK (no filtering, just enables interpreter)
 *     NI_ATTR_LINKED: pattern = [P_ATTRINFO(1)] → l_shipmode with header
 *       This tells DBSPJ to copy l_shipmode (index 1 in parent's linked
 *       list) INCLUDING its AttributeHeader into the subroutine section
 *       of the constructed attrInfo.  At runtime, DBSPJ sets
 *       T_ATTRINFO_CONSTRUCTED and expands this pattern per parent row.
 *
 * The NI_ATTR_INTERPRET + NI_ATTR_LINKED combination is required because
 * the AggInterpreter's LINKED_COL_FLAG column references (e.g. GROUP BY
 * l_shipmode) need linked parent data in the interpreter buffer.  Without
 * these flags, DBTUP's non-interpreter path has no linked data, and
 * LINKED_COL_FLAG columns fall through to readAttributes where the
 * high bit is misinterpreted as a pseudo-column attribute ID.
 */
static std::vector<Uint32>
buildQueryTree_Q12(const TableMeta &lineitemMeta,
                   const TableMeta &ordersMeta,
                   const std::vector<Uint32> &scanFilter,
                   Uint32 receiverId)
{
  Uint32 li_orderkey = lineitemMeta.attrIds.at("l_orderkey");
  Uint32 li_shipmode = lineitemMeta.attrIds.at("l_shipmode");

  std::vector<Uint32> ai;

  /* ---- Tree section ---- */

  /*
   * Node 0: QN_SCAN_FRAG (6 words: 4 fixed + 2 NI_LINKED_ATTR)
   * Node 1: QN_LOOKUP (10 words: 4 fixed + 1 parent + 2 key pattern
   *          + 1 attr_interpret/linked len + 1 interp prog + 1 linked pattern)
   */
  const Uint32 node0_len = 6;
  const Uint32 node1_len = 10;
  const Uint32 tree_len = 1 + node0_len + node1_len;

  /* Word 0: QueryTree cnt_len */
  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 2, tree_len);
  ai.push_back(cnt_len);

  /* Node 0: QN_SCAN_FRAG on LINEITEM */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_SCAN_FRAG, node0_len);
  ai.push_back(n0_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(lineitemMeta.tableId);
  ai.push_back(lineitemMeta.schemaVersion);
  /*
   * NI_LINKED_ATTR packed list: 2 attributes [l_orderkey, l_shipmode]
   * Format: (first_attrId << 16) | count, then remaining attrs packed
   * 2 per word in (high16 | low16) — for count=2 the second attr is
   * in the low 16 bits of the next word.
   */
  ai.push_back((li_orderkey << 16) | 2);
  ai.push_back(li_shipmode);

  /* Node 1: QN_LOOKUP on ORDERS (aggregate leaf)
   *
   * NI_ATTR_INTERPRET + NI_ATTR_LINKED: DBSPJ constructs an interpreted
   * program for each LQHKEYREQ. The interpret section is just ExitOK
   * (no filtering). The linked pattern (P_ATTRINFO) tells DBSPJ to copy
   * l_shipmode (column index 1 in parent's linked list) including its
   * AttributeHeader into the subroutine section.  DBTUP's interpreter
   * path then extracts this subroutine section as linked_data for the
   * AggInterpreter, which reads l_shipmode via LINKED_COL_FLAG.
   */
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_LOOKUP, node1_len);
  ai.push_back(n1_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF |
               DABits::NI_ATTR_INTERPRET | DABits::NI_ATTR_LINKED);
  ai.push_back(ordersMeta.tableId);
  ai.push_back(ordersMeta.schemaVersion);
  /* NI_HAS_PARENT: 1 parent (node 0) */
  ai.push_back((0 << 16) | 1);
  /* NI_KEY_LINKED: (param_cnt << 16) | pattern_len */
  ai.push_back((0 << 16) | 1);
  /* Key pattern: col(0) = l_orderkey from parent linked list */
  ai.push_back(QueryPattern::col(0));
  /* NI_ATTR_INTERPRET/NI_ATTR_LINKED: (len_pattern << 16) | len_prg */
  ai.push_back((1 << 16) | 1);
  /* Interpret program: ExitOK (just passes through) */
  ai.push_back(Interpreter::ExitOK());
  /* Linked pattern: attrInfo(1) = l_shipmode with AttributeHeader */
  ai.push_back(QueryPattern::attrInfo(1));

  /* ---- Parameter section ---- */

  /* Param 0: QN_ScanFragParameters, optionally with PI_ATTR_INTERPRET */
  Uint32 p0_param_size = QN_ScanFragParameters::NodeSize;
  Uint32 p0_requestInfo = 0;
  if (!scanFilter.empty()) {
    p0_param_size += 1 + (Uint32)scanFilter.size();
    p0_requestInfo = DABits::PI_ATTR_INTERPRET;
  }
  Uint32 p0_len = 0;
  QueryNodeParameters::setOpLen(p0_len, QueryNodeParameters::QN_SCAN_FRAG,
                                p0_param_size);
  ai.push_back(p0_len);
  ai.push_back(p0_requestInfo);
  ai.push_back(receiverId);    /* resultData */
  ai.push_back(990);            /* batch_size_rows */
  ai.push_back(2*1024*1024);   /* batch_size_bytes */
  ai.push_back(0);
  ai.push_back(0);
  ai.push_back(0);
  if (!scanFilter.empty()) {
    /* PI_ATTR_INTERPRET: (subroutine_len << 16) | program_len */
    ai.push_back((Uint32)scanFilter.size());
    for (Uint32 w : scanFilter) ai.push_back(w);
  }

  /* Param 1: QN_LookupParameters */
  Uint32 p1_len = 0;
  QueryNodeParameters::setOpLen(p1_len, QueryNodeParameters::QN_LOOKUP,
                                QN_LookupParameters::NodeSize);
  ai.push_back(p1_len);
  ai.push_back(0);             /* requestInfo */
  ai.push_back(receiverId);    /* resultData */

  return ai;
}

/* ------------------------------------------------------------------ */
/* Table setup via MySQL                                               */
/* ------------------------------------------------------------------ */

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

static int
createLineitemTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS q12_lineitem");
  if (sqlExec(conn,
        "CREATE TABLE q12_lineitem ("
        "  l_orderkey BIGINT NOT NULL,"
        "  l_linenumber INT NOT NULL,"
        "  l_shipmode CHAR(10) NOT NULL,"
        "  l_shipdate INT UNSIGNED NOT NULL,"
        "  l_commitdate INT UNSIGNED NOT NULL,"
        "  l_receiptdate INT UNSIGNED NOT NULL,"
        "  PRIMARY KEY (l_orderkey, l_linenumber)"
        ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0)
    return -1;

  if (loadTableMeta(ndb, LINEITEM_TABLE, meta) != 0) return -1;

  V("Table '%s': id=%u version=%u frags=%u\n",
    LINEITEM_TABLE, meta.tableId, meta.schemaVersion, meta.fragCount);
  return 0;
}

static int
createOrdersTable(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS q12_orders");
  if (sqlExec(conn,
        "CREATE TABLE q12_orders ("
        "  o_orderkey BIGINT NOT NULL PRIMARY KEY,"
        "  o_orderpriority CHAR(15) NOT NULL"
        ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0)
    return -1;

  if (loadTableMeta(ndb, ORDERS_TABLE, meta) != 0) return -1;

  V("Table '%s': id=%u version=%u frags=%u\n",
    ORDERS_TABLE, meta.tableId, meta.schemaVersion, meta.fragCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Data loading                                                        */
/* ------------------------------------------------------------------ */

static int
insertOrders(Ndb *ndb, Uint32 numOrders)
{
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(ORDERS_TABLE);
  if (ptab == nullptr) return -1;

  const Uint32 BATCH = 500;
  for (Uint32 s = 0; s < numOrders; s += BATCH) {
    NdbTransaction *tx = ndb->startTransaction();
    if (tx == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }
    Uint32 e = std::min(s + BATCH, numOrders);
    for (Uint32 i = s; i < e; i++) {
      NdbOperation *op = tx->getNdbOperation(ptab);
      if (op == nullptr) { tx->close(); return -1; }
      op->insertTuple();
      Int64 key = (Int64)(i + 1);
      op->equal("o_orderkey", key);
      op->setValue("o_orderpriority", PRIORITIES[i % NUM_PRIORITIES]);
    }
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert orders [%u..%u): %s\n",
              s, e, tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
  }
  printf("Inserted %u rows into %s\n", numOrders, ORDERS_TABLE);
  return 0;
}

static int
insertLineitems(Ndb *ndb, Uint32 numOrders, Uint32 linesPerOrder)
{
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(LINEITEM_TABLE);
  if (ptab == nullptr) return -1;

  const Uint32 BATCH = 500;
  Uint32 count = 0;
  NdbTransaction *tx = nullptr;

  for (Uint32 o = 0; o < numOrders; o++) {
    for (Uint32 l = 0; l < linesPerOrder; l++) {
      if (count % BATCH == 0) {
        if (tx != nullptr) {
          if (tx->execute(NdbTransaction::Commit) != 0) {
            fprintf(stderr, "insert lineitems: %s\n",
                    tx->getNdbError().message);
            tx->close();
            return -1;
          }
          tx->close();
        }
        tx = ndb->startTransaction();
        if (tx == nullptr) {
          fprintf(stderr, "startTransaction: %s\n",
                  ndb->getNdbError().message);
          return -1;
        }
      }

      NdbOperation *op = tx->getNdbOperation(ptab);
      if (op == nullptr) { tx->close(); return -1; }
      op->insertTuple();

      Int64 orderkey = (Int64)(o + 1);
      Int32 linenumber = (Int32)(l + 1);
      Uint32 idx = o * linesPerOrder + l;
      Uint32 shipmodeIdx = idx % NUM_SHIPMODES;

      Uint32 orderdate = (o * 1013) % 2406;
      Uint32 s_offset = 1 + ((idx * 13) % 121);
      Uint32 c_offset = 30 + ((idx * 7) % 61);
      Uint32 r_offset = 1 + ((idx * 17) % 30);

      op->equal("l_orderkey", orderkey);
      op->equal("l_linenumber", linenumber);
      op->setValue("l_shipmode", SHIPMODES[shipmodeIdx]);
      op->setValue("l_shipdate", orderdate + s_offset);
      op->setValue("l_commitdate", orderdate + c_offset);
      op->setValue("l_receiptdate", orderdate + s_offset + r_offset);
      count++;
    }
  }

  if (tx != nullptr) {
    if (tx->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert lineitems final: %s\n",
              tx->getNdbError().message);
      tx->close();
      return -1;
    }
    tx->close();
  }

  printf("Inserted %u rows into %s\n", count, LINEITEM_TABLE);
  return 0;
}

static int
dropTable(MYSQL *conn, const char *tableName)
{
  char query[256];
  snprintf(query, sizeof(query), "DROP TABLE IF EXISTS %s", tableName);
  return sqlExec(conn, query);
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
/* TC connect seize / release (from testJoinAggSpj)                    */
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
               Uint32 receiverId)
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

  ssig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_TABREQ,
           ScanTabReq::StaticLength);

  ssig.header.m_noOfSections = 3;
  ssig.ptr[0].p = &receiverId;
  ssig.ptr[0].sz = 1;
  ssig.ptr[1].p = queryTree.data();
  ssig.ptr[1].sz = (Uint32)queryTree.size();
  ssig.ptr[2].p = aggProgram.data();
  ssig.ptr[2].sz = (Uint32)aggProgram.size();

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

static std::string
extractGroupKeyChar10(const std::vector<Uint8> &key)
{
  const Uint32 HDR = 4;
  if (key.size() < HDR + 10) return "";
  int len = 10;
  while (len > 0 && key[HDR + len - 1] == ' ') len--;
  return std::string(reinterpret_cast<const char*>(key.data() + HDR), len);
}

static std::string
extractGroupKeyChar10Raw(const std::vector<Uint8> &key)
{
  const Uint32 HDR = 4;
  if (key.size() < HDR + 10) return "";
  return std::string(reinterpret_cast<const char*>(key.data() + HDR), 10);
}

static Int64
extractSumBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  const Uint32 ITEM_SIZE = 24;
  Uint32 off = aggIdx * ITEM_SIZE + 8;
  if (off + 8 > val.size()) return 0;
  Int64 v;
  memcpy(&v, val.data() + off, sizeof(Int64));
  return v;
}

/* ------------------------------------------------------------------ */
/* Result collection (TRANSID_AI + SCAN_TABCONF)                       */
/* ------------------------------------------------------------------ */

static int
collectResults(SignalSender &ss,
               std::vector<AggResult> &allResults,
               Uint32 apiConnectPtr, Uint32 tcRef, Uint32 nodeId,
               Uint32 &nextReqCount)
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
/* Expected result computation                                         */
/* ------------------------------------------------------------------ */

/*
 * Compute expected Q12 results from known data generation pattern.
 * Applies the same WHERE filter as the pushdown interpreted program:
 *   l_shipmode IN ('MAIL', 'SHIP')
 *   AND l_commitdate < l_receiptdate
 *   AND l_shipdate < l_commitdate
 *   AND l_receiptdate >= DATE_19940101
 *   AND l_receiptdate < DATE_19950101
 */
static void
computeExpected(Uint32 numOrders, Uint32 linesPerOrder,
                std::map<std::string, Int64> &expectedHigh,
                std::map<std::string, Int64> &expectedLow)
{
  for (Uint32 o = 0; o < numOrders; o++) {
    Uint32 prioIdx = o % NUM_PRIORITIES;
    for (Uint32 l = 0; l < linesPerOrder; l++) {
      Uint32 idx = o * linesPerOrder + l;
      Uint32 shipmodeIdx = idx % NUM_SHIPMODES;

      if (!noFilter) {
        /* l_shipmode IN ('MAIL', 'SHIP') — SHIP=index 3, MAIL=index 5 */
        if (shipmodeIdx != 3 && shipmodeIdx != 5) continue;

        /* Reproduce the date generation from insertLineitems() */
        Uint32 orderdate = (o * 1013) % 2406;
        Uint32 s_offset = 1 + ((idx * 13) % 121);
        Uint32 c_offset = 30 + ((idx * 7) % 61);
        Uint32 r_offset = 1 + ((idx * 17) % 30);

        Uint32 shipdate = orderdate + s_offset;
        Uint32 commitdate = orderdate + c_offset;
        Uint32 receiptdate = orderdate + s_offset + r_offset;

        /* l_commitdate < l_receiptdate */
        if (commitdate >= receiptdate) continue;
        /* l_shipdate < l_commitdate */
        if (shipdate >= commitdate) continue;
        /* l_receiptdate >= DATE_19940101 AND l_receiptdate < DATE_19950101 */
        if (receiptdate < DATE_19940101 || receiptdate >= DATE_19950101)
          continue;
      }

      std::string sm(SHIPMODES[shipmodeIdx], 10);
      if (prioIdx == 0 || prioIdx == 1) {
        expectedHigh[sm]++;
      } else {
        expectedLow[sm]++;
      }
    }
  }
}

/* ------------------------------------------------------------------ */
/* Benchmark function                                                  */
/* ------------------------------------------------------------------ */

static int
runBenchmark(SignalSender &ss, Uint32 nodeId,
             const NdbDictionary::Table *lineitemTab,
             const TableMeta &lineitemMeta,
             const TableMeta &ordersMeta,
             Uint32 numOrders, Uint32 linesPerOrder,
             bool validate, int iteration)
{
  V("\n========================================\n");
  V("Iteration %d\n", iteration);
  V("========================================\n");

  Uint32 receiverId = 100 + iteration;

  /* Build scan filter (pushdown WHERE clause) */
  std::vector<Uint32> scanFilter;
  if (!noFilter) {
    scanFilter = buildScanFilter(lineitemTab, lineitemMeta);
    if (scanFilter.empty()) return -1;
  }

  /* Build QueryTree and AggProgram */
  std::vector<Uint32> queryTree =
    buildQueryTree_Q12(lineitemMeta, ordersMeta, scanFilter, receiverId);

  Uint32 linkedShipmodeAttrId = lineitemMeta.attrIds.at("l_shipmode");
  Uint32 localPriorityAttrId = ordersMeta.attrIds.at("o_orderpriority");
  std::vector<Uint32> aggProgram =
    buildAggProgram_Q12(linkedShipmodeAttrId, localPriorityAttrId);

  V("ScanFilter: %zu words, QueryTree: %zu words, AggProgram: %zu words\n",
    scanFilter.size(), queryTree.size(), aggProgram.size());
  if (verbose) {
    fprintf(stderr, "QueryTree hex dump:\n");
    for (size_t i = 0; i < queryTree.size(); i++)
      fprintf(stderr, "  [%2zu] 0x%08x\n", i, queryTree[i]);
    fprintf(stderr, "AggProgram hex dump (first 10):\n");
    for (size_t i = 0; i < std::min(aggProgram.size(), (size_t)10); i++)
      fprintf(stderr, "  [%2zu] 0x%08x\n", i, aggProgram[i]);
  }

  /* Seize TC connect */
  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0)
    return -1;

  auto t0 = Clock::now();

  /* Send SCAN_TABREQ */
  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          lineitemMeta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    return -1;
  }

  auto t1 = Clock::now();

  /* Collect results */
  std::vector<AggResult> allResults;
  Uint32 nextReqCount = 0;
  rc = collectResults(ss, allResults, apiConnectPtr, tcRef, nodeId,
                      nextReqCount);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    return -1;
  }

  auto t2 = Clock::now();

  /* Release TC connect */
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  auto t3 = Clock::now();

  /* Merge results across nodes */
  std::map<std::string, Int64> actualHigh, actualLow;
  Uint32 totalGroups = 0;
  for (const auto &res : allResults) {
    totalGroups += (Uint32)res.groups.size();
    for (const auto &grp : res.groups) {
      std::string key = extractGroupKeyChar10Raw(grp.first);
      actualHigh[key] += extractSumBigint(grp.second, 0);
      actualLow[key] += extractSumBigint(grp.second, 1);
    }
  }

  /* Validate */
  int failures = 0;
  if (validate) {
    std::map<std::string, Int64> expectedHigh, expectedLow;
    computeExpected(numOrders, linesPerOrder, expectedHigh, expectedLow);

    std::set<std::string> allKeys;
    for (const auto &kv : expectedHigh) allKeys.insert(kv.first);
    for (const auto &kv : expectedLow) allKeys.insert(kv.first);
    for (const auto &kv : actualHigh) allKeys.insert(kv.first);

    for (const auto &key : allKeys) {
      Int64 eh = expectedHigh.count(key) ? expectedHigh[key] : 0;
      Int64 el = expectedLow.count(key) ? expectedLow[key] : 0;
      Int64 ah = actualHigh.count(key) ? actualHigh[key] : 0;
      Int64 al = actualLow.count(key) ? actualLow[key] : 0;
      std::string display = extractGroupKeyChar10(
        std::vector<Uint8>{0,0,0,0,
          (Uint8)key[0],(Uint8)key[1],(Uint8)key[2],(Uint8)key[3],
          (Uint8)key[4],(Uint8)key[5],(Uint8)key[6],(Uint8)key[7],
          (Uint8)key[8],(Uint8)key[9]});

      if (ah != eh) {
        fprintf(stderr, "FAIL: %s high expected %lld, got %lld\n",
                display.c_str(), (long long)eh, (long long)ah);
        failures++;
      }
      if (al != el) {
        fprintf(stderr, "FAIL: %s low expected %lld, got %lld\n",
                display.c_str(), (long long)el, (long long)al);
        failures++;
      }
    }

    /* SQL verification */
    if (failures == 0 && g_mysql_conn != nullptr) {
      if (mysql_query(g_mysql_conn,
              "SELECT l.l_shipmode, "
              "SUM(CASE WHEN o.o_orderpriority IN ('1-URGENT','2-HIGH') "
              "THEN 1 ELSE 0 END), "
              "SUM(CASE WHEN o.o_orderpriority NOT IN ('1-URGENT','2-HIGH') "
              "THEN 1 ELSE 0 END) "
              "FROM q12_lineitem l JOIN q12_orders o "
              "ON l.l_orderkey = o.o_orderkey "
              "WHERE l.l_shipmode IN ('MAIL','SHIP') "
              "AND l.l_commitdate < l.l_receiptdate "
              "AND l.l_shipdate < l.l_commitdate "
              "AND l.l_receiptdate >= 731 "
              "AND l.l_receiptdate < 1096 "
              "GROUP BY l.l_shipmode") != 0) {
        fprintf(stderr, "SQL verify failed: %s\n", mysql_error(g_mysql_conn));
        failures++;
      } else {
        MYSQL_RES *res = mysql_store_result(g_mysql_conn);
        if (res == nullptr) {
          fprintf(stderr, "mysql_store_result failed: %s\n",
                  mysql_error(g_mysql_conn));
          failures++;
        } else {
          std::map<std::string, Int64> sqlHigh, sqlLow;
          MYSQL_ROW row;
          while ((row = mysql_fetch_row(res)) != nullptr) {
            if (row[0] && row[1] && row[2]) {
              std::string sm(row[0]);
              while (sm.size() < 10) sm.push_back(' ');
              sqlHigh[sm] = (Int64)atoll(row[1]);
              sqlLow[sm] = (Int64)atoll(row[2]);
            }
          }
          mysql_free_result(res);
          if (sqlHigh != expectedHigh || sqlLow != expectedLow) {
            fprintf(stderr, "SQL verify mismatch\n");
            failures++;
          } else {
            V("  SQL verify: %zu shipmode groups — matches\n",
              sqlHigh.size());
          }
        }
      }
    }
  }

  /* Timing */
  double scanMs = elapsedMs(t0, t1);
  double resultMs = elapsedMs(t1, t2);
  double releaseMs = elapsedMs(t2, t3);
  double totalMs = elapsedMs(t0, t3);

  printf("Iteration %d: %s\n", iteration,
         failures == 0 ? "PASS" : "FAIL");
  printf("  Lineitems: %u  Groups: %u  TRANSID_AI: %zu  SCAN_NEXTREQ: %u\n",
         numOrders * linesPerOrder, totalGroups, allResults.size(),
         nextReqCount);
  printf("  Scan+Join: %8.2f ms\n", scanMs + resultMs);
  printf("  Release:   %8.2f ms\n", releaseMs);
  printf("  Total:     %8.2f ms\n", totalMs);

  if (verbose) {
    for (const auto &kv : actualHigh) {
      std::string display = extractGroupKeyChar10(
        std::vector<Uint8>{0,0,0,0,
          (Uint8)kv.first[0],(Uint8)kv.first[1],(Uint8)kv.first[2],
          (Uint8)kv.first[3],(Uint8)kv.first[4],(Uint8)kv.first[5],
          (Uint8)kv.first[6],(Uint8)kv.first[7],(Uint8)kv.first[8],
          (Uint8)kv.first[9]});
      printf("    %s: high=%lld  low=%lld\n",
             display.c_str(), (long long)kv.second,
             (long long)(actualLow.count(kv.first) ? actualLow[kv.first] : 0));
    }
  }

  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

static const char *connectString = nullptr;
static int mysqlPort = 3306;
static Uint32 numOrders = 1500000;
static Uint32 linesPerOrder = 4;
static int numIterations = 3;
static bool doValidate = true;
static bool keepTables = false;

int main(int argc, char **argv)
{
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf(
        "Usage: %s [options]\n\n"
        "TPC-H Q12 benchmark through DBTC→DBSPJ→DBLQH path.\n"
        "2-table join: LINEITEM scan + ORDERS lookup with\n"
        "GROUP BY l_shipmode, SUM(CASE WHEN o_orderpriority ...).\n\n"
        "Options:\n"
        "  -c <connect_string>    NDB connect string (default: localhost:1186)\n"
        "  -m <mysql_port>        MySQL port (default: 3306)\n"
        "  --orders <N>           Number of orders (default: 1500000)\n"
        "  --lines <N>            Lines per order (default: 4)\n"
        "  --iterations <N>       Benchmark iterations (default: 3)\n"
        "  --keep-tables          Keep tables after benchmark\n"
        "  --validate             Validate results (default)\n"
        "  --no-validate          Skip validation\n"
        "  --no-filter            Disable pushdown WHERE filter\n"
        "  -v, --verbose          Verbose output\n"
        "  -h, --help             Show this help\n",
        argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--orders") == 0 && i + 1 < argc) {
      numOrders = (Uint32)atoi(argv[++i]);
    } else if (strcmp(argv[i], "--lines") == 0 && i + 1 < argc) {
      linesPerOrder = (Uint32)atoi(argv[++i]);
    } else if (strcmp(argv[i], "--iterations") == 0 && i + 1 < argc) {
      numIterations = atoi(argv[++i]);
    } else if (strcmp(argv[i], "--keep-tables") == 0) {
      keepTables = true;
    } else if (strcmp(argv[i], "--validate") == 0) {
      doValidate = true;
    } else if (strcmp(argv[i], "--no-validate") == 0) {
      doValidate = false;
    } else if (strcmp(argv[i], "--no-filter") == 0) {
      noFilter = true;
    }
  }

  if (connectString == nullptr) connectString = "localhost:1186";

  Uint32 totalLineitems = numOrders * linesPerOrder;
  printf("bench_q12_dbtc: TPC-H Q12 through DBTC→DBSPJ→DBLQH\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);
  printf("  Orders: %u  Lines/order: %u  Total lineitems: %u\n",
         numOrders, linesPerOrder, totalLineitems);
  printf("  Iterations: %d\n\n", numIterations);

  ndb_init();
  int result = 0;

  do {
    Ndb_cluster_connection con(connectString);
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
      result = 1;
      break;
    }
    if (con.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      result = 1;
      break;
    }
    V("Connected to cluster\n");

    Ndb ndb(&con, "test");
    if (ndb.init() != 0) {
      fprintf(stderr, "Ndb::init: %s\n", ndb.getNdbError().message);
      result = 1;
      break;
    }

    NdbRestarter restarter(connectString);
    int dataNodeId = restarter.getDbNodeId(0);
    if (dataNodeId <= 0) {
      fprintf(stderr, "No data node found\n");
      result = 1;
      break;
    }
    V("Using data node %d\n", dataNodeId);

    /* Create tables and load data */
    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      result = 1; break;
    }
    g_mysql_conn = conn;

    TableMeta lineitemMeta, ordersMeta;

    NdbDictionary::Dictionary *dict = ndb.getDictionary();
    bool tablesExist = keepTables &&
      dict->getTable(LINEITEM_TABLE) != nullptr &&
      dict->getTable(ORDERS_TABLE) != nullptr;

    if (tablesExist) {
      printf("Reusing existing tables (--keep-tables)\n");
      if (loadTableMeta(&ndb, LINEITEM_TABLE, lineitemMeta) != 0) {
        mysql_close(conn); result = 1; break;
      }
      if (loadTableMeta(&ndb, ORDERS_TABLE, ordersMeta) != 0) {
        mysql_close(conn); result = 1; break;
      }
    } else {
      printf("Creating tables...\n");
      if (createLineitemTable(conn, &ndb, lineitemMeta) != 0) {
        mysql_close(conn); result = 1; break;
      }
      if (createOrdersTable(conn, &ndb, ordersMeta) != 0) {
        mysql_close(conn); result = 1; break;
      }

      auto tLoad = Clock::now();
      printf("Loading data: %u orders, %u lineitems...\n",
             numOrders, totalLineitems);

      if (insertOrders(&ndb, numOrders) != 0) {
        mysql_close(conn); result = 1; break;
      }
      if (insertLineitems(&ndb, numOrders, linesPerOrder) != 0) {
        mysql_close(conn); result = 1; break;
      }

      printf("Data loaded in %.2f ms\n", elapsedMs(tLoad, Clock::now()));
    }

    /* Dump table metadata for debugging */
    if (verbose) {
      fprintf(stderr, "LINEITEM: tableId=%u schemaVersion=%u fragCount=%u\n",
              lineitemMeta.tableId, lineitemMeta.schemaVersion,
              lineitemMeta.fragCount);
      for (const auto &kv : lineitemMeta.attrIds)
        fprintf(stderr, "  %-20s attrId=%u\n", kv.first.c_str(), kv.second);
      fprintf(stderr, "ORDERS: tableId=%u schemaVersion=%u fragCount=%u\n",
              ordersMeta.tableId, ordersMeta.schemaVersion,
              ordersMeta.fragCount);
      for (const auto &kv : ordersMeta.attrIds)
        fprintf(stderr, "  %-20s attrId=%u\n", kv.first.c_str(), kv.second);
    }

    /* Get table pointer for NdbInterpretedCode (scan filter) */
    const NdbDictionary::Table *lineitemTab =
        ndb.getDictionary()->getTable(LINEITEM_TABLE);
    if (lineitemTab == nullptr) {
      fprintf(stderr, "getTable(%s) failed: %s\n",
              LINEITEM_TABLE, ndb.getDictionary()->getNdbError().message);
      result = 1;
      break;
    }

    /* Run benchmark iterations */
    printf("\nStarting benchmark (%d iterations)...\n\n", numIterations);

    {
      SignalSender ss(&con);
      ss.lock();

      for (int iter = 1; iter <= numIterations; iter++) {
        if (runBenchmark(ss, (Uint32)dataNodeId,
                         lineitemTab,
                         lineitemMeta, ordersMeta,
                         numOrders, linesPerOrder,
                         doValidate, iter) != 0) {
          result = 1;
          /* Drain stale signals */
          for (int d = 0; d < 100; d++) {
            SimpleSignal *stale = ss.waitFor(50);
            if (stale == nullptr) break;
          }
        }
        printf("\n");
      }

      ss.unlock();
    }

    /* Cleanup */
    if (keepTables) {
      printf("Keeping tables for next run (--keep-tables)\n");
    } else {
      printf("Cleaning up...\n");
      dropTable(conn, LINEITEM_TABLE);
      dropTable(conn, ORDERS_TABLE);
    }
    mysql_close(conn);
  } while (0);

  ndb_end(0);

  printf(result == 0 ? "\n*** ALL ITERATIONS PASSED ***\n"
                      : "\n*** SOME ITERATIONS FAILED ***\n");
  return result;
}
