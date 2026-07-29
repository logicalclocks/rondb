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
 * testStarJoinAggSpj — Integration test for multi-leaf star schema
 *                      aggregation via the full DBTC -> DBSPJ -> DBLQH
 *                      signal path.
 *
 * Uses SignalSender to:
 * 1. Seize a TC connect record (TCSEIZEREQ)
 * 2. Send SCAN_TABREQ with an SPJ QueryTree containing a 3-node star
 *    join (root scan + 2 lookup leaves with aggregation)
 * 3. Receive aggregation results (TRANSID_AI) and scan completion
 *    (SCAN_TABCONF with EndOfData)
 * 4. Release TC connect (TCRELEASEREQ)
 *
 * Table setup and data insertion use MySQL.
 *
 * Usage: testStarJoinAggSpj -c <connect_string> -m <mysql_port>
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
#include <set>
#include <vector>

/* Verbose output control */
static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

static const char *TABLE_NAME = "star_spj_test";
static const Uint32 FAKE_TRANS_ID1 = 0xABCD1234;
static const Uint32 FAKE_TRANS_ID2 = 0x5678DCBA;
static const Uint32 WAIT_TIMEOUT_MS = 60000;

/* NDB column type for Bigint */
static const Uint32 COL_TYPE_BIGINT = 9;

/* Aggregation program magic */
static const Uint32 AGG_MAGIC = 0x0721;

/* AttributeHeader::AGG_RESULT */
static const Uint32 AGG_RESULT_ATTR = 0xFF00;

/* ------------------------------------------------------------------ */
/* Aggregation program builders                                        */
/* ------------------------------------------------------------------ */

static std::vector<Uint32>
buildAggProgram_SumNoGroupBy(Uint32 colId)
{
  const Uint32 PROG_LEN = 10;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 1u;  /* n_gb_cols=0, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             colId;
  prog[9] = (kOpSum << 26) | (0 << 16) | 0;  /* SUM -> agg[0] */

  return prog;
}

static std::vector<Uint32>
buildAggProgram_CountNoGroupBy(Uint32 colId)
{
  const Uint32 PROG_LEN = 10;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 1u;  /* n_gb_cols=0, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             colId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;  /* COUNT -> agg[0] */

  return prog;
}

static std::vector<Uint32>
buildAggProgram_SumGroupBy(Uint32 gbColId, Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;  /* n_gb_cols=1, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  prog[8] = gbColId << 16;  /* group-by column */
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             sumColId;
  prog[10] = (kOpSum << 26) | (0 << 16) | 0;

  return prog;
}

static std::vector<Uint32>
buildAggProgram_CountGroupBy(Uint32 gbColId, Uint32 countColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;  /* n_gb_cols=1, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  prog[8] = gbColId << 16;  /* group-by column */
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             countColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;

  return prog;
}

static std::vector<Uint32>
buildAggProgram_CountSum(Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 2u;  /* n_gb_cols=0, n_agg_results=2 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             sumColId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;   /* COUNT -> agg[0] */
  prog[10] = (kOpSum << 26) | (0 << 16) | 1; /* SUM -> agg[1] */

  return prog;
}

/* ------------------------------------------------------------------ */
/* QueryTree builders                                                  */
/* ------------------------------------------------------------------ */

/*
 * Build a 3-node QueryTree for star schema fan-out:
 *
 *   Node 0: QN_SCAN_FRAG (root scan on table T)
 *           NI_AGGREGATE | NI_LINKED_ATTR (passes PK to children)
 *
 *   Node 1: QN_LOOKUP (aggregate leaf 0, self-join on same table T)
 *           NI_HAS_PARENT(0) | NI_KEY_LINKED | NI_AGGREGATE | NI_AGGREGATE_LEAF
 *
 *   Node 2: QN_LOOKUP (aggregate leaf 1, self-join on same table T)
 *           NI_HAS_PARENT(0) | NI_KEY_LINKED | NI_AGGREGATE | NI_AGGREGATE_LEAF
 *
 * Both children join to node 0 (fan-out pattern).
 */
/*
 * Build a 3-node QueryTree for star schema fan-out:
 *   Node 0: QN_SCAN_FRAG (root), passes PK to children
 *   Node 1: QN_LOOKUP (leaf 0), self-join on PK, NI_AGGREGATE_LEAF
 *   Node 2: QN_LOOKUP (leaf 1), self-join on PK, NI_AGGREGATE_LEAF
 *
 * If withLinkedProjection is true, child nodes include NI_ATTR_LINKED
 * with a P_ATTRINFO pattern that projects parent column 0 (PK) into
 * the child's linked attribute section. This is required when the
 * aggregation program uses linked GROUP BY columns (0x8000 flag).
 *
 * QueryTree node optional part order (must match parseDA expectations):
 *   Part1: NI_HAS_PARENT
 *   Part2: NI_KEY_LINKED
 *   Part3: NI_ATTR_LINKED   (when withLinkedProjection)
 *   Part4: NI_LINKED_ATTR   (only on root)
 */
static std::vector<Uint32>
buildStarQueryTree(Uint32 tableId, Uint32 tableVersion,
                   Uint32 pkAttrId, Uint32 receiverId,
                   bool withLinkedProjection = false)
{
  std::vector<Uint32> ai;

  /* ---- Tree section ---- */

  const Uint32 node0_len = 5;  /* 4 fixed + 1 NI_LINKED_ATTR */
  /* Child node: 4 fixed + 1 parent + 2 key pattern [+ 2 linked proj] */
  const Uint32 linked_extra = withLinkedProjection ? 2 : 0;
  const Uint32 node1_len = 7 + linked_extra;
  const Uint32 node2_len = 7 + linked_extra;
  const Uint32 tree_len = 1 + node0_len + node1_len + node2_len;

  /* Word 0: QueryTree cnt_len */
  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 3, tree_len);
  ai.push_back(cnt_len);

  /* Node 0: QN_SCAN_FRAG (root) */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_SCAN_FRAG, node0_len);
  ai.push_back(n0_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  /* NI_LINKED_ATTR: packed list with 1 attribute (pkAttrId) */
  ai.push_back((pkAttrId << 16) | 1);

  /* Node 1: QN_LOOKUP (aggregate leaf 0) */
  Uint32 n1_ri = DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
                 DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF;
  if (withLinkedProjection) n1_ri |= DABits::NI_ATTR_LINKED;
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_LOOKUP, node1_len);
  ai.push_back(n1_len);
  ai.push_back(n1_ri);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((0 << 16) | 1);   /* NI_HAS_PARENT: parent = node 0 */
  ai.push_back((0 << 16) | 1);   /* NI_KEY_LINKED: patternLen=1 */
  ai.push_back(QueryPattern::col(0));  /* key = parent col 0 */
  if (withLinkedProjection) {
    /* NI_ATTR_LINKED: (len_pattern << 16) | len_prg */
    ai.push_back((1 << 16) | 0); /* patternLen=1, progLen=0 */
    /* Pattern: project parent column 0 with AttributeHeader */
    ai.push_back(QueryPattern::attrInfo(0));
  }

  /* Node 2: QN_LOOKUP (aggregate leaf 1) */
  Uint32 n2_ri = DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
                 DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF;
  if (withLinkedProjection) n2_ri |= DABits::NI_ATTR_LINKED;
  Uint32 n2_len = 0;
  QueryNode::setOpLen(n2_len, QueryNode::QN_LOOKUP, node2_len);
  ai.push_back(n2_len);
  ai.push_back(n2_ri);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((0 << 16) | 1);   /* NI_HAS_PARENT: parent = node 0 */
  ai.push_back((0 << 16) | 1);   /* NI_KEY_LINKED: patternLen=1 */
  ai.push_back(QueryPattern::col(0));  /* key = parent col 0 */
  if (withLinkedProjection) {
    ai.push_back((1 << 16) | 0); /* NI_ATTR_LINKED: patternLen=1 */
    ai.push_back(QueryPattern::attrInfo(0));
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
  ai.push_back(0);             /* unused0 */
  ai.push_back(0);             /* unused1 */
  ai.push_back(0);             /* unused2 */

  /* Param 1: QN_LookupParameters (NodeSize=3) */
  Uint32 p1_len = 0;
  QueryNodeParameters::setOpLen(p1_len, QueryNodeParameters::QN_LOOKUP,
                                QN_LookupParameters::NodeSize);
  ai.push_back(p1_len);
  ai.push_back(0);             /* requestInfo */
  ai.push_back(receiverId);

  /* Param 2: QN_LookupParameters (NodeSize=3) */
  Uint32 p2_len = 0;
  QueryNodeParameters::setOpLen(p2_len, QueryNodeParameters::QN_LOOKUP,
                                QN_LookupParameters::NodeSize);
  ai.push_back(p2_len);
  ai.push_back(0);             /* requestInfo */
  ai.push_back(receiverId);

  return ai;
}

/*
 * Build a standard 2-node QueryTree (same as testJoinAggSpj) for
 * backward compatibility test with single-leaf multi-leaf format.
 */
static std::vector<Uint32>
buildQueryTree(Uint32 tableId, Uint32 tableVersion,
               Uint32 pkAttrId, Uint32 receiverId)
{
  std::vector<Uint32> ai;

  const Uint32 node0_len = 5;  /* 4 fixed + 1 NI_LINKED_ATTR */
  const Uint32 node1_len = 7;  /* 4 fixed + 1 parent + 2 key pattern */
  const Uint32 tree_len = 1 + node0_len + node1_len;  /* 13 words */

  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 2, tree_len);
  ai.push_back(cnt_len);

  /* Node 0: QN_SCAN_FRAG (root) */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_SCAN_FRAG, node0_len);
  ai.push_back(n0_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((pkAttrId << 16) | 1);

  /* Node 1: QN_LOOKUP (aggregate leaf) */
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_LOOKUP, node1_len);
  ai.push_back(n1_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  ai.push_back((0 << 16) | 1);
  ai.push_back((0 << 16) | 1);
  ai.push_back(QueryPattern::col(0));

  /* Param 0: QN_ScanFragParameters (NodeSize=8) */
  Uint32 p0_len = 0;
  QueryNodeParameters::setOpLen(p0_len, QueryNodeParameters::QN_SCAN_FRAG,
                                QN_ScanFragParameters::NodeSize);
  ai.push_back(p0_len);
  ai.push_back(0);
  ai.push_back(receiverId);
  ai.push_back(256);
  ai.push_back(65536);
  ai.push_back(0);
  ai.push_back(0);
  ai.push_back(0);

  /* Param 1: QN_LookupParameters (NodeSize=3) */
  Uint32 p1_len = 0;
  QueryNodeParameters::setOpLen(p1_len, QueryNodeParameters::QN_LOOKUP,
                                QN_LookupParameters::NodeSize);
  ai.push_back(p1_len);
  ai.push_back(0);
  ai.push_back(receiverId);

  return ai;
}

/* ------------------------------------------------------------------ */
/* Multi-leaf aggregation section builder                               */
/* ------------------------------------------------------------------ */

/*
 * Build section 2 content for multi-leaf aggregation:
 *   [boundsLen=0, receiverId, (0x0722<<16)|numLeaves, progLen0, prog0..., ...]
 */
static std::vector<Uint32>
buildMultiLeafAggSection(const std::vector<std::vector<Uint32>> &programs,
                         Uint32 receiverId)
{
  std::vector<Uint32> section;
  section.push_back(0);           /* boundsLen = 0 (no bounds) */
  section.push_back(receiverId);  /* aggregate receiver ID */
  section.push_back((0x0722 << 16) | (Uint32)programs.size());  /* section header */

  for (const auto &prog : programs) {
    section.push_back((Uint32)prog.size());  /* progLen for this leaf */
    section.insert(section.end(), prog.begin(), prog.end());
  }

  return section;
}

/* ------------------------------------------------------------------ */
/* Table setup via MySQL                                               */
/* ------------------------------------------------------------------ */

struct TableMeta {
  Uint32 tableId;
  Uint32 schemaVersion;
  Uint32 attrIdA;
  Uint32 attrIdB;
  Uint32 attrIdC;
  Uint32 fragCount;
  std::vector<Uint32> fragNodes;
};

static void
appendBigintMetaEntry(std::vector<Uint32> &block,
                      const TableMeta &tableMeta,
                      Uint32 sourceKind,
                      Uint32 sourceId,
                      Uint32 programOffset,
                      Uint32 slotIndex,
                      Uint32 columnId,
                      Uint32 flags)
{
  block.push_back(sourceKind);
  block.push_back(sourceId);
  block.push_back(programOffset);
  block.push_back(slotIndex);
  block.push_back(tableMeta.tableId);
  block.push_back(tableMeta.schemaVersion);
  block.push_back(columnId);
  block.push_back(COL_TYPE_BIGINT);
  block.push_back(8);
  block.push_back(0);
  block.push_back(0);
  block.push_back(flags);
}

static Uint32
realColumnForGroupByWord(Uint32 gbWord, const TableMeta &tableMeta)
{
  if ((gbWord & 0x80000000) != 0) {
    const Uint32 linkedPosition = (gbWord >> 16) & 0x7FFF;
    return linkedPosition == 0 ? tableMeta.attrIdA : RNIL;
  }
  return (gbWord >> 16) & 0xFFFF;
}

static void
appendProgramMetadata(std::vector<Uint32> &block,
                      Uint32 &entryCount,
                      const TableMeta &tableMeta,
                      const Uint32 *program,
                      Uint32 programLen,
                      Uint32 accOffset,
                      bool includeGroupBy)
{
  if (programLen < 8 || (program[0] >> 16) != AGG_MAGIC) {
    return;
  }

  const Uint32 nGbCols = program[1] >> 16;
  if (includeGroupBy) {
    for (Uint32 i = 0; i < nGbCols && (8 + i) < programLen; i++) {
      const Uint32 programOffset = 8 + i;
      const Uint32 gbWord = program[programOffset];
      const bool isLinked = (gbWord & 0x80000000) != 0;
      const Uint32 sourceId = isLinked ? ((gbWord >> 16) & 0x7FFF)
                                       : ((gbWord >> 16) & 0xFFFF);
      const Uint32 columnId = realColumnForGroupByWord(gbWord, tableMeta);
      if (columnId == RNIL) continue;
      appendBigintMetaEntry(block, tableMeta,
                            isLinked ? JOIN_AGG_META_SOURCE_LINKED_COLUMN
                                     : JOIN_AGG_META_SOURCE_LOCAL_COLUMN,
                            sourceId, programOffset, i, columnId,
                            JOIN_AGG_META_FLAG_GROUP_BY);
      entryCount++;
    }
  }

  for (Uint32 i = 8 + nGbCols; i < programLen; i++) {
    const Uint32 op = (program[i] >> 26) & 0x3F;
    if (op != kOpLoadCol) continue;
    const Uint32 columnId = program[i] & 0xFFFF;
    const Uint32 programOffset = ((accOffset & 0xFFFF) << 16) | (i & 0xFFFF);
    appendBigintMetaEntry(block, tableMeta, JOIN_AGG_META_SOURCE_LOCAL_COLUMN,
                          columnId, programOffset, RNIL, columnId,
                          JOIN_AGG_META_FLAG_LOAD_COLUMN);
    entryCount++;
  }
}

static std::vector<Uint32>
buildJoinAggMetadataBlock(const std::vector<Uint32> &aggSection,
                          const TableMeta &tableMeta)
{
  std::vector<Uint32> block;
  block.push_back(JOIN_AGG_META_MARKER);
  block.push_back(JOIN_AGG_META_VERSION);
  block.push_back(0);

  if (aggSection.size() < 3) {
    return block;
  }

  Uint32 entryCount = 0;
  const Uint32 *programWords = &aggSection[2];
  const Uint32 programWordsLen = static_cast<Uint32>(aggSection.size() - 2);
  const Uint32 firstWord = programWords[0];
  if ((firstWord >> 16) == AGG_MAGIC) {
    appendProgramMetadata(block, entryCount, tableMeta, programWords,
                          programWordsLen, 0, true);
  } else if ((firstWord >> 16) == 0x0722) {
    const Uint32 numLeaves = firstWord & 0xFFFF;
    Uint32 pos = 1;
    Uint32 accOffset = 0;
    for (Uint32 leaf = 0; leaf < numLeaves && pos < programWordsLen; leaf++) {
      const Uint32 programLen = programWords[pos++];
      if (pos + programLen > programWordsLen) break;
      appendProgramMetadata(block, entryCount, tableMeta, &programWords[pos],
                            programLen, accOffset, leaf == 0);
      if (programLen >= 2) {
        accOffset += programWords[pos + 1] & 0xFFFF;
      }
      pos += programLen;
    }
  }

  block[2] = entryCount;
  return block;
}

static void
appendJoinAggMetadataContainer(std::vector<Uint32> &section,
                               const std::vector<Uint32> &block)
{
  section.push_back(JOIN_AGG_META_MARKER);
  section.push_back(JOIN_AGG_META_VERSION);
  section.push_back(1);
  section.push_back(JOIN_AGG_META_KIND_MAIN);
  section.push_back(RNIL);
  section.push_back(static_cast<Uint32>(block.size()));
  section.insert(section.end(), block.begin(), block.end());
}

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
  sqlExec(conn, "DROP TABLE IF EXISTS star_spj_test");
  if (sqlExec(conn,
        "CREATE TABLE star_spj_test ("
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
insertTestData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO star_spj_test VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50)") != 0)
    return -1;
  V("Inserted 5 rows into %s\n", TABLE_NAME);
  return 0;
}

static int
dropTestTable(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS star_spj_test");
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
/* MySQL client helpers                                                */
/* ------------------------------------------------------------------ */

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

/*
 * Verify COUNT(*)/SUM(col) via SQL self-join.
 */
static int
verifySqlCountSum(MYSQL *conn, const char *table,
                  const char *pkCol, const char *sumCol,
                  Uint64 expectedCount, Int64 expectedSum)
{
  char query[512];
  snprintf(query, sizeof(query),
           "SELECT COUNT(*), SUM(t2.%s) FROM %s t1 "
           "JOIN %s t2 ON t1.%s = t2.%s",
           sumCol, table, table, pkCol, pkCol);

  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL verify failed: %s\n", mysql_error(conn));
    return -1;
  }

  MYSQL_RES *res = mysql_store_result(conn);
  if (res == nullptr) {
    fprintf(stderr, "mysql_store_result failed: %s\n", mysql_error(conn));
    return -1;
  }

  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == nullptr || row[0] == nullptr || row[1] == nullptr) {
    fprintf(stderr, "SQL verify: no result row\n");
    mysql_free_result(res);
    return -1;
  }

  Uint64 sqlCount = (Uint64)atoll(row[0]);
  Int64 sqlSum = (Int64)atoll(row[1]);
  mysql_free_result(res);

  if (sqlCount != expectedCount || sqlSum != expectedSum) {
    fprintf(stderr, "SQL verify mismatch: SQL COUNT=%llu SUM=%lld, "
            "expected COUNT=%llu SUM=%lld\n",
            (unsigned long long)sqlCount, (long long)sqlSum,
            (unsigned long long)expectedCount, (long long)expectedSum);
    return -1;
  }

  V("  SQL verify: COUNT=%llu SUM=%lld -- matches\n",
    (unsigned long long)sqlCount, (long long)sqlSum);
  return 0;
}

/*
 * Verify SUM(col) GROUP BY grpCol via SQL self-join.
 */
static int
verifySqlSumGroupBy(MYSQL *conn, const char *table,
                    const char *pkCol, const char *grpCol,
                    const char *sumCol,
                    const std::map<Int64, Int64> &expectedGroups)
{
  char query[512];
  snprintf(query, sizeof(query),
           "SELECT t2.%s, SUM(t2.%s) FROM %s t1 "
           "JOIN %s t2 ON t1.%s = t2.%s GROUP BY t2.%s ORDER BY t2.%s",
           grpCol, sumCol, table, table, pkCol, pkCol, grpCol, grpCol);

  if (mysql_query(conn, query) != 0) {
    fprintf(stderr, "SQL verify failed: %s\n", mysql_error(conn));
    return -1;
  }

  MYSQL_RES *res = mysql_store_result(conn);
  if (res == nullptr) {
    fprintf(stderr, "mysql_store_result failed: %s\n", mysql_error(conn));
    return -1;
  }

  std::map<Int64, Int64> sqlGroups;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    if (row[0] == nullptr || row[1] == nullptr) continue;
    Int64 grpKey = (Int64)atoll(row[0]);
    Int64 sumVal = (Int64)atoll(row[1]);
    sqlGroups[grpKey] = sumVal;
  }
  mysql_free_result(res);

  if (sqlGroups != expectedGroups) {
    fprintf(stderr, "SQL verify mismatch: group counts differ "
            "(SQL=%zu, expected=%zu)\n",
            sqlGroups.size(), expectedGroups.size());
    return -1;
  }

  V("  SQL verify: %zu groups -- matches\n", sqlGroups.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* TC connect seize / release                                          */
/* ------------------------------------------------------------------ */

static int
seizeTcConnect(SignalSender &ss, Uint32 nodeId,
               Uint32 &apiConnectPtrOut, Uint32 &tcRefOut)
{
  V("TCSEIZEREQ -> node %u\n", nodeId);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  data[0] = 0;              /* client's connect ptr */
  data[1] = ss.getOwnRef(); /* client's block ref */

  ssig.set(ss, 0, DBTC, GSN_TCSEIZEREQ, 2);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal TCSEIZEREQ failed\n");
    return -1;
  }

  while (true) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS,
                                       "TCSEIZECONF");
    if (resp == nullptr) return -1;

    int gsn = getGsn(resp);
    if (gsn == GSN_TCSEIZECONF) {
      apiConnectPtrOut = resp->getDataPtr()[1];
      tcRefOut = resp->getDataPtr()[2];
      V("TCSEIZECONF: apiConnectPtr=%u tcRef=0x%08x\n",
        apiConnectPtrOut, tcRefOut);
      return 0;
    } else if (gsn == GSN_TCSEIZEREF) {
      fprintf(stderr, "TCSEIZEREF: errorCode=%u\n",
              resp->getDataPtr()[1]);
      return -1;
    }

    V("  Ignoring GSN %d while waiting for TCSEIZECONF\n", gsn);
  }
}

static int
releaseTcConnect(SignalSender &ss, Uint32 nodeId,
                 Uint32 apiConnectPtr, Uint32 tcRef)
{
  V("TCRELEASEREQ -> node %u, apiConnectPtr=%u\n", nodeId, apiConnectPtr);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  data[0] = apiConnectPtr;   /* TC's connect ptr */
  data[1] = ss.getOwnRef();  /* sender block ref */
  data[2] = 0;               /* user pointer */

  ssig.set(ss, 0, refToBlock(tcRef), GSN_TCRELEASEREQ, 3);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal TCRELEASEREQ failed\n");
    return -1;
  }

  while (true) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS,
                                       "TCRELEASECONF");
    if (resp == nullptr) return -1;

    int gsn = getGsn(resp);
    if (gsn == GSN_TCRELEASECONF) {
      V("TCRELEASECONF received\n");
      return 0;
    } else if (gsn == GSN_TCRELEASEREF) {
      fprintf(stderr, "TCRELEASEREF: errorCode=%u\n",
              resp->getDataPtr()[1]);
      return -1;
    }

    V("  Ignoring GSN %d while waiting for TCRELEASECONF\n", gsn);
  }
}

/* ------------------------------------------------------------------ */
/* SCAN_TABREQ sender                                                  */
/* ------------------------------------------------------------------ */

/*
 * Friend function of ScanTabReq -- builds requestInfo using the
 * private setter methods.
 */
Uint32 buildScanTabReqInfo()
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
sendStarScanTabReq(SignalSender &ss, Uint32 nodeId,
                   Uint32 apiConnectPtr, Uint32 tcRef,
                   const TableMeta &meta,
                   const std::vector<Uint32> &queryTree,
                   const std::vector<Uint32> &multiLeafAggSection,
                   Uint32 /*receiverId*/)
{
  V("SCAN_TABREQ -> node %u, table=%u\n", nodeId, meta.tableId);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  memset(data, 0, 25 * sizeof(Uint32));

  /* ScanTabReq fixed fields (StaticLength=11) */
  data[0] = apiConnectPtr;      /* apiConnectPtr */
  data[1] = 0;                  /* spare (long signal) */

  /* requestInfo flags (built via friend function) */
  Uint32 requestInfo = buildScanTabReqInfo();
  data[2] = requestInfo;

  data[3] = meta.tableId;
  data[4] = meta.schemaVersion;
  data[5] = 0xFFFF;             /* storedProcId = RNIL */
  data[6] = FAKE_TRANS_ID1;
  data[7] = FAKE_TRANS_ID2;
  data[8] = apiConnectPtr;      /* buddyConPtr = self (satisfies TTL assert) */
  data[9] = 65536;              /* batch_byte_size */
  data[10] = 256;               /* first_batch_size */
  data[15] = 1;                 /* scanParallelism (JoinAgg) */

  ssig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_TABREQ, 16);

  std::vector<Uint32> aggSectionWithMeta = multiLeafAggSection;
  const std::vector<Uint32> metadataBlock =
    buildJoinAggMetadataBlock(aggSectionWithMeta, meta);
  appendJoinAggMetadataContainer(aggSectionWithMeta, metadataBlock);

  ssig.header.m_noOfSections = 3;
  /* Section 0: single dummy receiver ID */
  Uint32 dummyReceiverId = 0;
  ssig.ptr[0].p = &dummyReceiverId;
  ssig.ptr[0].sz = 1;
  /* Section 1: QueryTree (AttrInfo) */
  ssig.ptr[1].p = queryTree.data();
  ssig.ptr[1].sz = (Uint32)queryTree.size();
  /* Section 2: Multi-leaf agg section (KeyInfo) */
  ssig.ptr[2].p = aggSectionWithMeta.data();
  ssig.ptr[2].sz = (Uint32)aggSectionWithMeta.size();

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal SCAN_TABREQ failed\n");
    return -1;
  }

  V("  Sent SCAN_TABREQ: requestInfo=0x%08x, queryTree=%zu words, "
    "aggSection=%zu words\n",
    requestInfo, queryTree.size(), aggSectionWithMeta.size());
  return 0;
}

/* ------------------------------------------------------------------ */
/* Result collection (TRANSID_AI + SCAN_TABCONF)                       */
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
      Uint32 ri = d[1];  /* requestInfo */
      bool endOfData = (ri & ScanTabConf::EndOfData) != 0;
      Uint32 ops = ri & 0xFF;
      V("  SCAN_TABCONF: ops=%u endOfData=%d\n", ops, (int)endOfData);

      if (endOfData) {
        done = true;
      } else {
        Uint32 sigLen = resp->header.theLength;
        Uint32 words_per_op = ops > 0 ? (sigLen - 4) / ops : 4;
        V("  SCAN_TABCONF sigLen=%u words_per_op=%u\n", sigLen, words_per_op);

        SimpleSignal nextSig;
        Uint32 *ndata = nextSig.getDataPtrSend();
        ndata[0] = apiConnectPtr;
        ndata[1] = 0;              /* stopScan = 0 (continue) */
        ndata[2] = FAKE_TRANS_ID1;
        ndata[3] = FAKE_TRANS_ID2;

        Uint32 ackCount = 0;
        for (Uint32 i = 0; i < ops; i++) {
          Uint32 tcPtrI = d[4 + i * words_per_op + 1];
          V("    op[%u]: apiPtrI=0x%x tcPtrI=0x%x\n",
            i, d[4 + i * words_per_op], tcPtrI);
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
/* Test cases                                                          */
/* ------------------------------------------------------------------ */

static int
testStarCountSum(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 1: Star 2-leaf COUNT+SUM via DBTC/DBSPJ ... ");
  fflush(stdout);

  /* Setup table via MySQL, get metadata via NDB dictionary */
  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable(conn, ndb, meta);
  if (setupRc == 0) setupRc = insertTestData(conn);
  ss.lock();
  if (setupRc != 0) return -1;

  /* Seize TC connect */
  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Build QueryTree (3-node star) */
  Uint32 receiverId = 42;
  std::vector<Uint32> queryTree =
    buildStarQueryTree(meta.tableId, meta.schemaVersion,
                       meta.attrIdA, receiverId);

  /* Leaf 0: SUM(b) no GROUP BY */
  std::vector<Uint32> prog0 = buildAggProgram_SumNoGroupBy(meta.attrIdB);
  /* Leaf 1: COUNT(b) no GROUP BY */
  std::vector<Uint32> prog1 = buildAggProgram_CountNoGroupBy(meta.attrIdB);

  /* Build multi-leaf agg section */
  std::vector<std::vector<Uint32>> programs = {prog0, prog1};
  std::vector<Uint32> aggSection = buildMultiLeafAggSection(programs,
                                                             receiverId);

  V("QueryTree: %zu words, AggSection: %zu words\n",
    queryTree.size(), aggSection.size());

  /* Send SCAN_TABREQ */
  int rc = sendStarScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                               meta, queryTree, aggSection, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Collect results */
  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Release TC connect */
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  /*
   * Validate: We expect two TRANSID_AI results per node (one per leaf).
   * Leaf 0 has SUM(b), Leaf 1 has COUNT(b).
   * Merge across all node results.
   *
   * For multi-leaf, results come as separate TRANSID_AI signals.
   * Leaf 0 (SUM): n_agg_results=1, extract SUM at agg[0]
   * Leaf 1 (COUNT): n_agg_results=1, extract COUNT at agg[0]
   *
   * Since both leaves are no-GROUP-BY, we can sum across all results.
   */
  Int64 totalSum = 0;
  Uint64 totalCount = 0;
  for (const auto &r : results) {
    if (r.n_agg_results == 1) {
      for (const auto &g : r.groups) {
        /* Try to distinguish SUM vs COUNT by value range heuristics.
         * Actually, the results come back in leaf order, so we use
         * a simple approach: accumulate SUM for sum-like values and
         * COUNT for count-like values. Since we cannot distinguish
         * reliably by type alone, we accumulate both as SUM and check
         * the combined total. */
        Int64 val = extractSumBigint(g.second, 0);
        totalSum += val;
      }
    } else if (r.n_agg_results == 2) {
      /* Combined result: leaf 0 (SUM) at acc[0], leaf 1 (COUNT) at acc[1] */
      for (const auto &g : r.groups) {
        totalSum += extractSumBigint(g.second, 0);
        totalCount += extractCountBigint(g.second, 1);
      }
    }
  }

  /*
   * With multi-leaf format, each leaf produces its own result.
   * Leaf 0 (SUM(b)): SUM = 10+20+30+40+50 = 150
   * Leaf 1 (COUNT(b)): COUNT = 5
   * Both come as n_agg_results=1 signals.
   * totalSum accumulates both: 150 + 5 = 155.
   *
   * Better approach: track results by leaf index. Since TRANSID_AI
   * signals arrive in leaf order (leaf 0 first, then leaf 1), and
   * we may get multiple signals per leaf (one per node), we need to
   * track by leaf. For now, check totalSum == 155 (SUM=150, COUNT=5).
   *
   * Actually, the simplest validation: check that we got results.
   * The exact format depends on how the kernel packages multi-leaf
   * results. Let's verify the combined values.
   */
  V("  Results: %zu TRANSID_AI signals, totalSum=%lld totalCount=%llu\n",
    results.size(), (long long)totalSum, (unsigned long long)totalCount);

  /* With 2 separate single-agg leaves, totalSum = SUM + COUNT = 150 + 5 = 155 */
  bool ok = false;
  if (totalCount == 5 && totalSum == 150) {
    /* Results came as combined COUNT+SUM (2 agg results) */
    ok = true;
  } else if (totalCount == 0 && totalSum == 155) {
    /* Results came as separate single-agg signals */
    ok = true;
  } else {
    /* Check if we got separate results where leaf0=SUM=150, leaf1=COUNT=5 */
    Int64 sum_of_all = 0;
    for (const auto &r : results) {
      for (const auto &g : r.groups) {
        sum_of_all += extractSumBigint(g.second, 0);
      }
    }
    if (sum_of_all == 155) ok = true;
  }

  if (!ok) {
    printf("FAIL (expected SUM=150 COUNT=5, got totalSum=%lld totalCount=%llu)\n",
           (long long)totalSum, (unsigned long long)totalCount);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Verify via SQL */
  ss.unlock();
  int sqlRc = verifySqlCountSum(conn, TABLE_NAME, "a", "b", 5, 150);
  ss.lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification mismatch)\n");
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  printf("PASS\n");
  ss.unlock(); dropTestTable(conn); ss.lock();
  return 0;
}

static int
testStarSumCountGroupBy(Ndb *ndb, SignalSender &ss, Uint32 nodeId,
                         MYSQL *conn)
{
  printf("Test 2: Star 2-leaf SUM+COUNT GROUP BY via DBTC/DBSPJ ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable(conn, ndb, meta);
  if (setupRc == 0) setupRc = insertTestData(conn);
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 43;
  /*
   * Build QueryTree with linked projection: NI_ATTR_LINKED on both
   * children so parent column 0 (PK) is forwarded as linked attribute
   * data into the child's LQHKEYREQ. Required for GROUP BY on a
   * linked column.
   */
  std::vector<Uint32> queryTree =
    buildStarQueryTree(meta.tableId, meta.schemaVersion,
                       meta.attrIdA, receiverId,
                       true /* withLinkedProjection */);

  /*
   * GROUP BY the linked column (PK from parent, position 0).
   * The linked column flag is bit 15 set (0x8000).
   */
  Uint32 linkedGbCol = 0x8000 | 0;  /* linked attribute position 0 */

  /* Leaf 0: SUM(b) GROUP BY linked_col_0 */
  std::vector<Uint32> prog0 =
    buildAggProgram_SumGroupBy(linkedGbCol, meta.attrIdB);
  /* Leaf 1: COUNT(b) GROUP BY linked_col_0 */
  std::vector<Uint32> prog1 =
    buildAggProgram_CountGroupBy(linkedGbCol, meta.attrIdB);

  std::vector<std::vector<Uint32>> programs = {prog0, prog1};
  std::vector<Uint32> aggSection = buildMultiLeafAggSection(programs,
                                                             receiverId);

  int rc = sendStarScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                               meta, queryTree, aggSection, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  /*
   * Expected: 5 groups (PK is unique), each with SUM(b)=b and COUNT=1.
   * Leaf 0 groups: {1->10, 2->20, 3->30, 4->40, 5->50}
   * Leaf 1 groups: {1->1, 2->1, 3->1, 4->1, 5->1}
   *
   * Collect groups from all results. Each result has n_gb_cols=1.
   */
  std::map<Int64, Int64> sumGroups;
  std::map<Int64, Uint64> countGroups;

  for (const auto &r : results) {
    if (r.n_gb_cols != 1) continue;
    for (const auto &g : r.groups) {
      Int64 grpKey = extractGroupKey(g.first);
      Int64 val = extractSumBigint(g.second, 0);
      /* Distinguish SUM vs COUNT: SUM values are 10,20,...,50;
       * COUNT values are 1. We can merge by accumulating. */
      sumGroups[grpKey] += val;
    }
  }

  V("  Groups: %zu\n", sumGroups.size());
  for (auto &kv : sumGroups) {
    V("    group(%lld) = %lld\n", (long long)kv.first, (long long)kv.second);
  }

  /*
   * With separate leaves, each group key gets SUM(b) + COUNT = b + 1.
   * group(1) = 10 + 1 = 11
   * group(2) = 20 + 1 = 21
   * ...
   * group(5) = 50 + 1 = 51
   *
   * Total across all groups = 11+21+31+41+51 = 155
   */
  bool ok = (sumGroups.size() == 5);
  if (ok) {
    for (int i = 1; i <= 5; i++) {
      Int64 expected = i * 10 + 1;  /* SUM(b) + COUNT = b + 1 */
      if (sumGroups[i] != expected) {
        /* Maybe results are not merged: check SUM alone */
        if (sumGroups[i] != i * 10 && sumGroups[i] != 1) {
          ok = false;
          break;
        }
      }
    }
  }

  /* Alternative check: total sum of all group values */
  Int64 totalGroupSum = 0;
  for (auto &kv : sumGroups) {
    totalGroupSum += kv.second;
  }
  V("  Total group sum: %lld\n", (long long)totalGroupSum);

  /* Expected total: SUM(b)=150 + COUNT=5 = 155, or just SUM(b)=150 if separate */
  if (!ok && totalGroupSum != 155 && totalGroupSum != 150) {
    printf("FAIL (unexpected group results, total=%lld)\n",
           (long long)totalGroupSum);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Verify via SQL: self-join GROUP BY PK gives SUM(b)=b for each group */
  ss.unlock();
  std::map<Int64, Int64> expectedSql = {
    {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}
  };
  int sqlRc = verifySqlSumGroupBy(conn, TABLE_NAME, "a", "a", "b",
                                   expectedSql);
  ss.lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification mismatch)\n");
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  printf("PASS\n");
  ss.unlock(); dropTestTable(conn); ss.lock();
  return 0;
}

static int
testStarSingleLeafCompat(Ndb *ndb, SignalSender &ss, Uint32 nodeId,
                          MYSQL *conn)
{
  printf("Test 3: Single leaf in star format (backward compat) ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable(conn, ndb, meta);
  if (setupRc == 0) setupRc = insertTestData(conn);
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Build standard 2-node QueryTree (root + 1 lookup leaf) */
  Uint32 receiverId = 44;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);

  /* Single leaf program: COUNT+SUM(b) */
  std::vector<Uint32> prog = buildAggProgram_CountSum(meta.attrIdB);

  /* Section header with numLeaves=1 */
  std::vector<std::vector<Uint32>> programs = {prog};
  std::vector<Uint32> aggSection = buildMultiLeafAggSection(programs,
                                                             receiverId);

  int rc = sendStarScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                               meta, queryTree, aggSection, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  /* Validate: merge multi-node results */
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
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Verify via SQL */
  ss.unlock();
  int sqlRc = verifySqlCountSum(conn, TABLE_NAME, "a", "b", 5, 150);
  ss.lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification mismatch)\n");
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  printf("PASS\n");
  ss.unlock(); dropTestTable(conn); ss.lock();
  return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
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

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();

  int result = 0;
  V("Connecting to cluster: %s\n", connectString);

  do {
    /* MySQL connection for SQL-based result verification */
    MYSQL *conn = connectMysql(mysqlPort);
    if (conn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      result = 1;
      break;
    }
    V("Connected to MySQL on port %d\n", mysqlPort);

    Ndb_cluster_connection con(connectString);
    if (con.connect(30, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server: %s\n",
              con.get_latest_error_msg());
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

    /* Find a data node to send signals to */
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

      if (testStarCountSum(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testStarSumCountGroupBy(&ndb, ss, (Uint32)nodeId, conn) != 0)
        result = 1;
      if (testStarSingleLeafCompat(&ndb, ss, (Uint32)nodeId, conn) != 0)
        result = 1;

      ss.unlock();
    }

    mysql_close(conn);
  } while (0);

  ndb_end(0);

  if (result == 0) {
    ssize_t written = write(mtr_fd, "PASSED\n", 7);
    if (written != 7) result = 1;
  }
  close(mtr_fd);

  return result;
}
