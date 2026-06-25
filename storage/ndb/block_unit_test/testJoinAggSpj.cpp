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
 * testJoinAggSpj — Integration test for pushdown join aggregation via
 *                  the full DBTC → DBSPJ → DBLQH signal path.
 *
 * Uses SignalSender to:
 * 1. Seize a TC connect record (TCSEIZEREQ)
 * 2. Send SCAN_TABREQ with an SPJ QueryTree containing a 2-node pushed
 *    join (root scan + lookup leaf with aggregation)
 * 3. Receive aggregation results (TRANSID_AI) and scan completion
 *    (SCAN_TABCONF with EndOfData)
 * 4. Release TC connect (TCRELEASEREQ)
 *
 * Table setup and data insertion use the NDB API.
 *
 * Usage: testJoinAggSpj -c <connect_string> -m <mysql_port>
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

static const char *TABLE_NAME = "jspj_test";
static const char *TABLE_NAME_3COL = "jspj_test3";
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
/* Aggregation program builders (reused from testJoinAgg)              */
/* ------------------------------------------------------------------ */

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
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;   /* COUNT → agg[0] */
  prog[10] = (kOpSum << 26) | (0 << 16) | 1; /* SUM → agg[1] */

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
buildAggProgram_CountMaxMin(Uint32 colId)
{
  const Uint32 PROG_LEN = 12;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 3u;  /* n_gb_cols=0, n_agg_results=3 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) | colId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;   /* COUNT → agg[0] */
  prog[10] = (kOpMax << 26) | (0 << 16) | 1;    /* MAX → agg[1] */
  prog[11] = (kOpMin << 26) | (0 << 16) | 2;    /* MIN → agg[2] */
  return prog;
}

/* ------------------------------------------------------------------ */
/* QueryTree builder                                                   */
/* ------------------------------------------------------------------ */

/*
 * Build the AttrInfo section containing an SPJ QueryTree for a 2-node
 * self-join with aggregation:
 *
 *   Node 0: QN_SCAN_FRAG (root scan on table T)
 *           NI_AGGREGATE | NI_LINKED_ATTR (passes PK to child)
 *
 *   Node 1: QN_LOOKUP (aggregate leaf, self-join on same table T)
 *           NI_HAS_PARENT | NI_KEY_LINKED | NI_AGGREGATE | NI_AGGREGATE_LEAF
 *
 * The lookup key is the PK column from the parent scan row.
 */
static std::vector<Uint32>
buildQueryTree(Uint32 tableId, Uint32 tableVersion,
               Uint32 pkAttrId, Uint32 receiverId)
{
  std::vector<Uint32> ai;

  /* ---- Tree section ---- */

  const Uint32 node0_len = 5;  /* 4 fixed + 1 NI_LINKED_ATTR */
  const Uint32 node1_len = 7;  /* 4 fixed + 1 parent + 2 key pattern */
  const Uint32 tree_len = 1 + node0_len + node1_len;  /* 13 words */

  /* Word 0: QueryTree cnt_len */
  Uint32 cnt_len = 0;
  QueryTree::setCntLen(cnt_len, 2, tree_len);
  ai.push_back(cnt_len);

  /* Node 0: QN_SCAN_FRAG (root) */
  Uint32 n0_len = 0;
  QueryNode::setOpLen(n0_len, QueryNode::QN_SCAN_FRAG, node0_len);
  ai.push_back(n0_len);
  ai.push_back(DABits::NI_AGGREGATE | DABits::NI_LINKED_ATTR);  /* requestInfo */
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  /* NI_LINKED_ATTR: packed list with 1 attribute (pkAttrId) */
  ai.push_back((pkAttrId << 16) | 1);

  /* Node 1: QN_LOOKUP (aggregate leaf) */
  Uint32 n1_len = 0;
  QueryNode::setOpLen(n1_len, QueryNode::QN_LOOKUP, node1_len);
  ai.push_back(n1_len);
  ai.push_back(DABits::NI_HAS_PARENT | DABits::NI_KEY_LINKED |
               DABits::NI_AGGREGATE | DABits::NI_AGGREGATE_LEAF);
  ai.push_back(tableId);
  ai.push_back(tableVersion);
  /* NI_HAS_PARENT: packed list with 1 parent (node 0) */
  ai.push_back((0 << 16) | 1);
  /* NI_KEY_LINKED: (param_cnt << 16) | pattern_len = (0 << 16) | 1 */
  ai.push_back((0 << 16) | 1);
  /* Key pattern: QueryPattern::col(0) = get column 0 from parent */
  ai.push_back(QueryPattern::col(0));

  /* ---- Parameter section ---- */

  /* Param 0: QN_ScanFragParameters (NodeSize=8) */
  Uint32 p0_len = 0;
  QueryNodeParameters::setOpLen(p0_len, QueryNodeParameters::QN_SCAN_FRAG,
                                QN_ScanFragParameters::NodeSize);
  ai.push_back(p0_len);
  ai.push_back(0);             /* requestInfo: no special bits */
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
  ai.push_back(0);             /* requestInfo: no special bits */
  ai.push_back(receiverId);    /* resultData */

  return ai;
}

/* ------------------------------------------------------------------ */
/* Table setup via NDB API                                             */
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
appendLocalBigintMetaEntry(std::vector<Uint32> &block,
                           const TableMeta &tableMeta,
                           Uint32 columnId,
                           Uint32 programOffset,
                           Uint32 slotIndex,
                           Uint32 flags)
{
  block.push_back(JOIN_AGG_META_SOURCE_LOCAL_COLUMN);
  block.push_back(columnId);
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

static std::vector<Uint32>
buildJoinAggMetadataBlock(const std::vector<Uint32> &aggProgram,
                          const TableMeta &tableMeta)
{
  std::vector<Uint32> block;
  block.push_back(JOIN_AGG_META_MARKER);
  block.push_back(JOIN_AGG_META_VERSION);
  block.push_back(0);

  if (aggProgram.size() < 8 || (aggProgram[0] >> 16) != AGG_MAGIC) {
    return block;
  }

  Uint32 entryCount = 0;
  const Uint32 nGbCols = aggProgram[1] >> 16;
  for (Uint32 i = 0; i < nGbCols && (8 + i) < aggProgram.size(); i++) {
    const Uint32 programOffset = 8 + i;
    const Uint32 columnId = (aggProgram[programOffset] >> 16) & 0xFFFF;
    appendLocalBigintMetaEntry(block, tableMeta, columnId, programOffset, i,
                               JOIN_AGG_META_FLAG_GROUP_BY);
    entryCount++;
  }

  for (Uint32 i = 8 + nGbCols; i < aggProgram.size(); i++) {
    const Uint32 op = (aggProgram[i] >> 26) & 0x3F;
    if (op != kOpLoadCol) continue;
    const Uint32 columnId = aggProgram[i] & 0xFFFF;
    appendLocalBigintMetaEntry(block, tableMeta, columnId, i, RNIL,
                               JOIN_AGG_META_FLAG_LOAD_COLUMN);
    entryCount++;
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

/*
 * Create/drop tables and insert data via MySQL.
 * Tables must be created through MySQL (not NDB API) to be visible
 * to MySQL servers.  Metadata lookup uses NDB dictionary API.
 */

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
  sqlExec(conn, "DROP TABLE IF EXISTS jspj_test");
  if (sqlExec(conn,
        "CREATE TABLE jspj_test ("
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
createTestTable3Col(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jspj_test3");
  if (sqlExec(conn,
        "CREATE TABLE jspj_test3 ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL,"
        "  val BIGINT NOT NULL"
        ") ENGINE=NDB") != 0)
    return -1;

  if (getTableMeta(ndb, TABLE_NAME_3COL, meta, "pk", "grp", "val") != 0)
    return -1;

  V("Table '%s': id=%u version=%u pk=%u grp=%u val=%u frags=%u\n",
    TABLE_NAME_3COL, meta.tableId, meta.schemaVersion,
    meta.attrIdA, meta.attrIdB, meta.attrIdC, meta.fragCount);
  return 0;
}

static int
insertTestData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO jspj_test VALUES "
        "(1,10),(2,20),(3,30),(4,40),(5,50)") != 0)
    return -1;
  V("Inserted 5 rows into %s\n", TABLE_NAME);
  return 0;
}

static int
insertMultiRowData(MYSQL *conn)
{
  if (sqlExec(conn,
        "INSERT INTO jspj_test3 VALUES "
        "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,2,50),(6,3,60)") != 0)
    return -1;
  V("Inserted 6 rows into %s\n", TABLE_NAME_3COL);
  return 0;
}

static int
dropTestTable(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jspj_test");
  return 0;
}

static int
dropTestTable3Col(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jspj_test3");
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
/* Result parsing (reused from testJoinAgg)                            */
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
/* MySQL client helpers (for SQL-based result verification)             */
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
 * Runs: SELECT COUNT(*), SUM(t2.<sumCol>) FROM <table> t1
 *       JOIN <table> t2 ON t1.<pkCol> = t2.<pkCol>
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

  V("  SQL verify: COUNT=%llu SUM=%lld — matches\n",
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

  V("  SQL verify: %zu groups — matches\n", sqlGroups.size());
  return 0;
}

/*
 * Verify COUNT/MAX/MIN via SQL self-join.
 */
static int
verifySqlCountMaxMin(MYSQL *conn, const char *table,
                     const char *pkCol, const char *valCol,
                     Uint64 expectedCount, Int64 expectedMax,
                     Int64 expectedMin)
{
  char query[512];
  snprintf(query, sizeof(query),
           "SELECT COUNT(*), MAX(t2.%s), MIN(t2.%s) FROM %s t1 "
           "JOIN %s t2 ON t1.%s = t2.%s",
           valCol, valCol, table, table, pkCol, pkCol);

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
  if (row == nullptr || row[0] == nullptr ||
      row[1] == nullptr || row[2] == nullptr) {
    fprintf(stderr, "SQL verify: no result row\n");
    mysql_free_result(res);
    return -1;
  }

  Uint64 sqlCount = (Uint64)atoll(row[0]);
  Int64 sqlMax = (Int64)atoll(row[1]);
  Int64 sqlMin = (Int64)atoll(row[2]);
  mysql_free_result(res);

  if (sqlCount != expectedCount || sqlMax != expectedMax ||
      sqlMin != expectedMin) {
    fprintf(stderr, "SQL verify mismatch: SQL COUNT=%llu MAX=%lld MIN=%lld, "
            "expected COUNT=%llu MAX=%lld MIN=%lld\n",
            (unsigned long long)sqlCount, (long long)sqlMax, (long long)sqlMin,
            (unsigned long long)expectedCount,
            (long long)expectedMax, (long long)expectedMin);
    return -1;
  }

  V("  SQL verify: COUNT=%llu MAX=%lld MIN=%lld — matches\n",
    (unsigned long long)sqlCount, (long long)sqlMax, (long long)sqlMin);
  return 0;
}

/* ------------------------------------------------------------------ */
/* TC connect seize / release                                          */
/* ------------------------------------------------------------------ */

static int
seizeTcConnect(SignalSender &ss, Uint32 nodeId,
               Uint32 &apiConnectPtrOut, Uint32 &tcRefOut)
{
  V("TCSEIZEREQ → node %u\n", nodeId);

  SimpleSignal ssig;
  Uint32 *data = ssig.getDataPtrSend();
  data[0] = 0;              /* client's connect ptr */
  data[1] = ss.getOwnRef(); /* client's block ref */

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
  } else {
    fprintf(stderr, "Unexpected GSN %d waiting for TCSEIZECONF\n", gsn);
    return -1;
  }
}

static int
releaseTcConnect(SignalSender &ss, Uint32 nodeId,
                 Uint32 apiConnectPtr, Uint32 tcRef)
{
  V("TCRELEASEREQ → node %u, apiConnectPtr=%u\n", nodeId, apiConnectPtr);

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
      fprintf(stderr, "TCRELEASEREF: errorCode=%u\n", resp->getDataPtr()[1]);
      return -1;
    }

    V("  Ignoring GSN %d while waiting for TCRELEASECONF\n", gsn);
  }
}

/* ------------------------------------------------------------------ */
/* SCAN_TABREQ sender                                                  */
/* ------------------------------------------------------------------ */

/*
 * Friend function of ScanTabReq — builds requestInfo using the
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
sendScanTabReq(SignalSender &ss, Uint32 nodeId,
               Uint32 apiConnectPtr, Uint32 tcRef,
               const TableMeta &meta,
               const std::vector<Uint32> &queryTree,
               const std::vector<Uint32> &aggProgram,
               Uint32 receiverId)
{
  V("SCAN_TABREQ → node %u, table=%u\n", nodeId, meta.tableId);

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
  data[15] = 1;                  /* scanParallelism (JoinAgg) */

  /* Sections:
   *   [0] ReceiverIds
   *   [1] AttrInfo (QueryTree)
   *   [2] KeyInfo: combined [boundsLen, bounds..., aggReceiverId, aggProgram...]
   */
  ssig.set(ss, 0, refToBlock(tcRef), GSN_SCAN_TABREQ, 16);

  /* Build combined agg section: [boundsLen=0, receiverId, aggProgram...] */
  std::vector<Uint32> aggSection;
  aggSection.push_back(0);  // boundsLen = 0 (no bounds)
  aggSection.push_back(receiverId);
  aggSection.insert(aggSection.end(), aggProgram.begin(), aggProgram.end());
  if (!aggProgram.empty() && (aggProgram[0] >> 16) == AGG_MAGIC) {
    const std::vector<Uint32> metadataBlock =
      buildJoinAggMetadataBlock(aggProgram, meta);
    appendJoinAggMetadataContainer(aggSection, metadataBlock);
  }

  ssig.header.m_noOfSections = 3;
  /* Section 0: single dummy receiver ID (DBTC ignores section 0 for JoinAgg;
   * aggregate receiver ID comes from section 2). */
  Uint32 dummyReceiverId = 0;
  ssig.ptr[0].p = &dummyReceiverId;
  ssig.ptr[0].sz = 1;
  ssig.ptr[1].p = queryTree.data();
  ssig.ptr[1].sz = (Uint32)queryTree.size();
  ssig.ptr[2].p = aggSection.data();
  ssig.ptr[2].sz = (Uint32)aggSection.size();

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
        /*
         * Extract tcPtrI values from SCAN_TABCONF OpData.
         * Format: header (4 words) + per-op data (words_per_op each).
         * tcPtrI is at offset 1 within each OpData entry.
         * We set ExtendedConf so words_per_op is 4 or 5 (never 3).
         */
        Uint32 sigLen = resp->header.theLength;
        Uint32 words_per_op = ops > 0 ? (sigLen - 4) / ops : 4;
        V("  SCAN_TABCONF sigLen=%u words_per_op=%u\n", sigLen, words_per_op);

        SimpleSignal nextSig;
        Uint32 *ndata = nextSig.getDataPtrSend();
        ndata[0] = apiConnectPtr;
        ndata[1] = 0;              /* stopScan = 0 (continue) */
        ndata[2] = FAKE_TRANS_ID1;
        ndata[3] = FAKE_TRANS_ID2;

        /* Append tcPtrI values (non-RNIL) to acknowledge delivered frags */
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
testBasicCountSum(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 1: Basic COUNT(*), SUM(b) via DBTC/DBSPJ ... ");
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

  /* Build QueryTree and AggProgram */
  Uint32 receiverId = 42;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram = buildAggProgram_CountSum(meta.attrIdB);

  V("QueryTree: %zu words, AggProgram: %zu words\n",
    queryTree.size(), aggProgram.size());

  /* Send SCAN_TABREQ */
  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
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

static int
testBasicSumGroupBy(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 2: SUM(val) GROUP BY grp via DBTC/DBSPJ ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable3Col(conn, ndb, meta);
  if (setupRc == 0) setupRc = insertMultiRowData(conn);
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 43;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  /* GROUP BY grp (attrIdB), SUM(val) (attrIdC) */
  std::vector<Uint32> aggProgram =
    buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  /* Collect groups from all node results */
  std::map<Int64, Int64> groupSums;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      Int64 grpKey = extractGroupKey(g.first);
      Int64 sumVal = extractSumBigint(g.second, 0);
      groupSums[grpKey] += sumVal;
    }
  }

  V("  Groups: %zu\n", groupSums.size());
  for (auto &kv : groupSums) {
    V("    group(%lld) = %lld\n", (long long)kv.first, (long long)kv.second);
  }

  /* Expected: group(1)=30, group(2)=120, group(3)=60 */
  bool ok = (groupSums.size() == 3 &&
             groupSums[1] == 30 &&
             groupSums[2] == 120 &&
             groupSums[3] == 60);

  if (!ok) {
    printf("FAIL (unexpected group sums)\n");
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  /* Verify via SQL */
  ss.unlock();
  std::map<Int64, Int64> expected = {{1, 30}, {2, 120}, {3, 60}};
  int sqlRc = verifySqlSumGroupBy(conn, TABLE_NAME_3COL,
                                   "pk", "grp", "val", expected);
  ss.lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification mismatch)\n");
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  printf("PASS\n");
  ss.unlock(); dropTestTable3Col(conn); ss.lock();
  return 0;
}

static int
testBasicAllAggs(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 3: COUNT/MAX/MIN via DBTC/DBSPJ ... ");
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

  Uint32 receiverId = 44;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram = buildAggProgram_CountMaxMin(meta.attrIdB);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
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

  /* Merge COUNT/MAX/MIN across nodes.
   * Skip MAX/MIN from nodes with COUNT=0 (no rows → uninitialized). */
  Uint64 totalCount = 0;
  Int64 maxVal = LLONG_MIN;
  Int64 minVal = LLONG_MAX;
  for (Uint32 ri = 0; ri < results.size(); ri++) {
    const auto &r = results[ri];
    for (const auto &g : r.groups) {
      Uint64 cnt = extractCountBigint(g.second, 0);
      Int64 mx = extractSumBigint(g.second, 1);
      Int64 mn = extractSumBigint(g.second, 2);
      V("    node[%u]: COUNT=%llu MAX=%lld MIN=%lld\n",
        ri, (unsigned long long)cnt, (long long)mx, (long long)mn);
      totalCount += cnt;
      if (cnt > 0) {
        if (mx > maxVal) maxVal = mx;
        if (mn < minVal) minVal = mn;
      }
    }
  }

  V("  Result: COUNT=%llu, MAX=%lld, MIN=%lld\n",
    (unsigned long long)totalCount, (long long)maxVal, (long long)minVal);

  if (totalCount != 5 || maxVal != 50 || minVal != 10) {
    printf("FAIL (expected COUNT=5 MAX=50 MIN=10, got COUNT=%llu MAX=%lld MIN=%lld)\n",
           (unsigned long long)totalCount, (long long)maxVal, (long long)minVal);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Verify via SQL */
  ss.unlock();
  int sqlRc = verifySqlCountMaxMin(conn, TABLE_NAME, "a", "b", 5, 50, 10);
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
testEmptyTable(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 4: Empty table COUNT(*), SUM(b) ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable(conn, ndb, meta);
  /* No data inserted — table is empty */
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 45;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram = buildAggProgram_CountSum(meta.attrIdB);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
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

  /* With kernel fix, empty nodes skip TRANSID_AI → 0 results expected */
  Uint64 totalCount = 0;
  Int64 totalSum = 0;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      totalCount += extractCountBigint(g.second, 0);
      totalSum += extractSumBigint(g.second, 1);
    }
  }

  V("  Result: COUNT=%llu, SUM=%lld, TRANSID_AI signals=%zu\n",
    (unsigned long long)totalCount, (long long)totalSum, results.size());

  if (totalCount != 0 || totalSum != 0) {
    printf("FAIL (expected COUNT=0 SUM=0, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  printf("PASS\n");
  ss.unlock(); dropTestTable(conn); ss.lock();
  return 0;
}

static int
testSingleRow(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 5: Single row COUNT(*), SUM(b) ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable(conn, ndb, meta);
  if (setupRc == 0)
    setupRc = sqlExec(conn, "INSERT INTO jspj_test VALUES (1, 42)");
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 46;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram = buildAggProgram_CountSum(meta.attrIdB);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
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

  if (totalCount != 1 || totalSum != 42) {
    printf("FAIL (expected COUNT=1 SUM=42, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  ss.unlock();
  int sqlRc = verifySqlCountSum(conn, TABLE_NAME, "a", "b", 1, 42);
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
testLargeDataset(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 6: Large dataset (200 rows) COUNT(*), SUM(b) ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable(conn, ndb, meta);
  if (setupRc == 0) {
    /* Insert 200 rows: b = row_number (1..200), SUM = 200*201/2 = 20100 */
    char buf[8192];
    int pos = snprintf(buf, sizeof(buf), "INSERT INTO jspj_test VALUES ");
    for (int i = 1; i <= 200; i++) {
      if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
      pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d)", i, i);
    }
    setupRc = sqlExec(conn, buf);
    if (setupRc == 0) V("Inserted 200 rows into %s\n", TABLE_NAME);
  }
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 47;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram = buildAggProgram_CountSum(meta.attrIdB);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
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

  if (totalCount != 200 || totalSum != 20100) {
    printf("FAIL (expected COUNT=200 SUM=20100, got COUNT=%llu SUM=%lld)\n",
           (unsigned long long)totalCount, (long long)totalSum);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  ss.unlock();
  int sqlRc = verifySqlCountSum(conn, TABLE_NAME, "a", "b", 200, 20100);
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
testManyGroups(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn)
{
  printf("Test 7: Many groups (10 groups, 100 rows) GROUP BY ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable3Col(conn, ndb, meta);
  if (setupRc == 0) {
    /* 100 rows: pk=1..100, grp=(pk-1)%10 + 1, val=pk
     * Group 1: pk=1,11,21,...,91 → SUM = 1+11+21+31+41+51+61+71+81+91 = 460
     * Group 2: pk=2,12,22,...,92 → SUM = 2+12+22+32+42+52+62+72+82+92 = 470
     * ...
     * Group 10: pk=10,20,...,100 → SUM = 10+20+30+40+50+60+70+80+90+100 = 550
     */
    char buf[8192];
    int pos = snprintf(buf, sizeof(buf), "INSERT INTO jspj_test3 VALUES ");
    for (int i = 1; i <= 100; i++) {
      if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
      int grp = ((i - 1) % 10) + 1;
      pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d,%d)", i, grp, i);
    }
    setupRc = sqlExec(conn, buf);
    if (setupRc == 0) V("Inserted 100 rows into %s\n", TABLE_NAME_3COL);
  }
  ss.lock();
  if (setupRc != 0) return -1;

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 48;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram =
    buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  /* Merge groups from all node results */
  std::map<Int64, Int64> groupSums;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      Int64 grpKey = extractGroupKey(g.first);
      Int64 sumVal = extractSumBigint(g.second, 0);
      groupSums[grpKey] += sumVal;
    }
  }

  V("  Groups: %zu\n", groupSums.size());
  for (auto &kv : groupSums) {
    V("    group(%lld) = %lld\n", (long long)kv.first, (long long)kv.second);
  }

  /* Expected: 10 groups, group(g) = sum of pk where ((pk-1)%10)+1 == g
   * = 10*g + 10*(0+10+20+...+90)/10... no, let's compute directly:
   * group(g): pk values are g, g+10, g+20, ..., g+90
   * SUM = 10*g + 10*(0+10+20+...+90)/10... simpler:
   * SUM(pk) for group g = g + (g+10) + (g+20) + ... + (g+90)
   *                      = 10*g + (0+10+20+...+90)
   *                      = 10*g + 450
   */
  std::map<Int64, Int64> expected;
  for (int g = 1; g <= 10; g++) {
    expected[g] = 10 * g + 450;
  }

  if (groupSums != expected) {
    printf("FAIL (unexpected group sums, got %zu groups)\n", groupSums.size());
    for (auto &kv : groupSums) {
      fprintf(stderr, "  group(%lld) = %lld (expected %lld)\n",
              (long long)kv.first, (long long)kv.second,
              (long long)expected[kv.first]);
    }
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  ss.unlock();
  int sqlRc = verifySqlSumGroupBy(conn, TABLE_NAME_3COL,
                                   "pk", "grp", "val", expected);
  ss.lock();
  if (sqlRc != 0) {
    printf("FAIL (SQL verification mismatch)\n");
    ss.unlock(); dropTestTable3Col(conn); ss.lock();
    return -1;
  }

  printf("PASS\n");
  ss.unlock(); dropTestTable3Col(conn); ss.lock();
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 8: Forced eviction via ERROR_INSERT 4040                       */
/* ------------------------------------------------------------------ */
#if defined(VM_TRACE) || defined(ERROR_INSERT)

static int
testForcedEviction(Ndb *ndb, SignalSender &ss, Uint32 nodeId, MYSQL *conn,
                   NdbRestarter &restarter)
{
  printf("Test 8: Forced eviction (ERROR_INSERT 4040) GROUP BY ... ");
  fflush(stdout);

  ss.unlock();
  TableMeta meta;
  int setupRc = createTestTable3Col(conn, ndb, meta);
  if (setupRc == 0) {
    char buf[8192];
    int pos = snprintf(buf, sizeof(buf), "INSERT INTO jspj_test3 VALUES ");
    for (int i = 1; i <= 100; i++) {
      if (i > 1) pos += snprintf(buf + pos, sizeof(buf) - pos, ",");
      int grp = ((i - 1) % 10) + 1;
      pos += snprintf(buf + pos, sizeof(buf) - pos, "(%d,%d,%d)", i, grp, i);
    }
    setupRc = sqlExec(conn, buf);
    if (setupRc == 0) V("Inserted 100 rows into %s\n", TABLE_NAME_3COL);
  }

  if (setupRc != 0) {
    ss.lock();
    return -1;
  }

  if (restarter.insertErrorInAllNodes(4040) != 0) {
    fprintf(stderr, "FAIL: insertErrorInAllNodes(4040) failed\n");
    dropTestTable3Col(conn);
    ss.lock();
    return -1;
  }
  V("ERROR_INSERT 4040 set in all nodes\n");
  ss.lock();

  Uint32 apiConnectPtr = 0, tcRef = 0;
  if (seizeTcConnect(ss, nodeId, apiConnectPtr, tcRef) != 0) {
    ss.unlock();
    restarter.insertErrorInAllNodes(0);
    dropTestTable3Col(conn);
    ss.lock();
    return -1;
  }

  Uint32 receiverId = 48;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);
  std::vector<Uint32> aggProgram =
    buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                          meta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock();
    restarter.insertErrorInAllNodes(0);
    dropTestTable3Col(conn);
    ss.lock();
    return -1;
  }

  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc != 0) {
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock();
    restarter.insertErrorInAllNodes(0);
    dropTestTable3Col(conn);
    ss.lock();
    return -1;
  }

  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);

  /* Merge groups from all node results */
  std::map<Int64, Int64> groupSums;
  for (const auto &r : results) {
    for (const auto &g : r.groups) {
      Int64 grpKey = extractGroupKey(g.first);
      Int64 sumVal = extractSumBigint(g.second, 0);
      groupSums[grpKey] += sumVal;
    }
  }

  V("  Groups: %zu\n", groupSums.size());
  for (auto &kv : groupSums) {
    V("    group(%lld) = %lld\n", (long long)kv.first, (long long)kv.second);
  }

  /* Expected: same as testManyGroups — 10 groups, group(g) = 10*g + 450 */
  std::map<Int64, Int64> expected;
  for (int g = 1; g <= 10; g++) {
    expected[g] = 10 * g + 450;
  }

  ss.unlock();
  restarter.insertErrorInAllNodes(0);
  V("ERROR_INSERT cleared\n");

  if (groupSums != expected) {
    printf("FAIL (unexpected group sums, got %zu groups)\n", groupSums.size());
    for (auto &kv : groupSums) {
      fprintf(stderr, "  group(%lld) = %lld (expected %lld)\n",
              (long long)kv.first, (long long)kv.second,
              (long long)expected[kv.first]);
    }
    dropTestTable3Col(conn);
    ss.lock();
    return -1;
  }

  int sqlRc = verifySqlSumGroupBy(conn, TABLE_NAME_3COL,
                                   "pk", "grp", "val", expected);
  if (sqlRc != 0) {
    printf("FAIL (SQL verification mismatch)\n");
    dropTestTable3Col(conn);
    ss.lock();
    return -1;
  }

  printf("PASS\n");
  dropTestTable3Col(conn);
  ss.lock();
  return 0;
}

#endif  /* VM_TRACE || ERROR_INSERT */

/* ------------------------------------------------------------------ */
/* Test: Reject too many leaves in multi-leaf agg program (> 32)       */
/* ------------------------------------------------------------------ */

static int
testRejectTooManyLeaves(Ndb *ndb, SignalSender &ss, Uint32 nodeId,
                        MYSQL *conn)
{
  printf("Test 9: Reject multi-leaf agg with 33 leaves (> NDB_SPJ_MAX) ... ");
  fflush(stdout);

  TableMeta meta;
  ss.unlock();
  int setupRc = createTestTable(conn, ndb, meta);
  ss.lock();
  if (setupRc != 0) {
    printf("FAIL (table setup)\n"); return -1;
  }

  Uint32 apiConnectPtr, tcRef;
  if (seizeTcConnect(ss, (Uint32)nodeId, apiConnectPtr, tcRef) != 0) {
    printf("FAIL (TC seize)\n");
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  Uint32 receiverId = 0xBEEF;
  std::vector<Uint32> queryTree =
    buildQueryTree(meta.tableId, meta.schemaVersion,
                   meta.attrIdA, receiverId);

  /*
   * Build a fake multi-leaf agg program with 33 leaves using the 0x0722
   * wire format.  DblqhProxy should reject this because
   * numLeaves (33) > NDB_SPJ_MAX_TREE_NODES (32).
   *
   * Format: (0x0722 << 16) | numLeaves, then per leaf: [progLen, prog...]
   * We use a minimal valid program for each leaf.
   */
  std::vector<Uint32> aggProgram;
  const Uint32 numFakeLeaves = 33;
  aggProgram.push_back((0x0722 << 16) | numFakeLeaves);

  /* Build a minimal single-leaf program to replicate for each fake leaf */
  std::vector<Uint32> oneProg = buildAggProgram_CountSum(meta.attrIdB);
  for (Uint32 i = 0; i < numFakeLeaves; i++) {
    aggProgram.push_back((Uint32)oneProg.size());
    aggProgram.insert(aggProgram.end(), oneProg.begin(), oneProg.end());
  }

  int rc = sendScanTabReq(ss, nodeId, apiConnectPtr, tcRef,
                           meta, queryTree, aggProgram, receiverId);
  if (rc != 0) {
    printf("FAIL (send)\n");
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  /* Expect SCAN_TABREF (error) since DblqhProxy rejects 33 leaves */
  std::vector<AggResult> results;
  rc = collectResults(ss, results, apiConnectPtr, tcRef, nodeId);
  if (rc == 0) {
    printf("FAIL (should have been rejected, but got results)\n");
    releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
    ss.unlock(); dropTestTable(conn); ss.lock();
    return -1;
  }

  V("\n  Correctly rejected with error (SCAN_TABREF)\n");
  releaseTcConnect(ss, nodeId, apiConnectPtr, tcRef);
  ss.unlock(); dropTestTable(conn); ss.lock();
  printf("PASS\n");
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
    if (con.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Failed to connect to management server\n");
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

      if (testBasicCountSum(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testBasicSumGroupBy(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testBasicAllAggs(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testEmptyTable(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testSingleRow(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testLargeDataset(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
      if (testManyGroups(&ndb, ss, (Uint32)nodeId, conn) != 0) result = 1;
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      if (testForcedEviction(&ndb, ss, (Uint32)nodeId, conn,
                             restarter) != 0) result = 1;
#else
      printf("Test 8: SKIPPED (production build, ERROR_INSERT unavailable)\n");
      (void)restarter;
#endif
      if (testRejectTooManyLeaves(&ndb, ss, (Uint32)nodeId, conn) != 0)
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
