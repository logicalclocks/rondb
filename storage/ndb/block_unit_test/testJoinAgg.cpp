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
 * testJoinAgg — Unit test for DBLQH join aggregation pushdown.
 *
 * Acts as both DBSPJ and API:
 * 1. Creates a test table and inserts known data via NDB API.
 * 2. Uses SignalSender to send raw join aggregation signals to DBLQH:
 *    JOIN_AGG_SETUP_REQ → SCAN_FRAGREQ (with join agg flag) →
 *    JOIN_AGG_COMPLETE_REQ → receive TRANSID_AI results →
 *    JOIN_AGG_RELEASE_REQ
 * 3. Validates aggregation results against expected values.
 *
 * Usage: testJoinAgg -c <connect_string>
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
static const char *TABLE_NAME = "jagg_test";
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

/* ScanFragReq requestInfo flags set via ScanFragReq::setXxxFlag() */

/* ------------------------------------------------------------------ */
/* Aggregation program builder                                         */
/* ------------------------------------------------------------------ */

/*
 * Build a simple aggregation program for:
 *   SELECT SUM(col_b) FROM t GROUP BY col_a
 *
 * Program layout:
 *   Words 0-7: header (magic, counts, version, reserved)
 *   Word 8: group-by column ID (col_a)
 *   Word 9: kOpLoadCol — load col_b into register 0
 *   Word 10: kOpSum — accumulate reg 0 into agg_result[0]
 */
static std::vector<Uint32>
buildAggProgram_SumGroupBy(Uint32 gbColId, Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  /* Word 0: magic + total length */
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;

  /* Word 1: n_gb_cols=1, n_agg_results=1 */
  prog[1] = (1u << 16) | 1u;

  /* Word 2: version */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;

  /* Words 3-7: reserved */
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /* Word 8: group-by column (AttributeHeader: attrId << 16) */
  prog[8] = gbColId << 16;

  /* Word 9: kOpLoadCol(type=BIGINT, reg=0, colId=sumColId) */
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             sumColId;

  /* Word 10: kOpSum(reg=0, agg_idx=0) */
  prog[10] = (kOpSum << 26) | (0 << 16) | 0;

  return prog;
}

/*
 * Build aggregation program for:
 *   SELECT COUNT(*), SUM(col_b) FROM t  (no GROUP BY)
 *
 * n_gb_cols=0, n_agg_results=2
 * Instructions: kOpLoadCol(col_b→reg0), kOpCount(reg0→agg[0]),
 *               kOpSum(reg0→agg[1])
 */
static std::vector<Uint32>
buildAggProgram_CountSum(Uint32 sumColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);

  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (0u << 16) | 2u;  /* n_gb_cols=0, n_agg_results=2 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;

  /* No group-by columns — instructions start at word 8 */
  prog[8] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             sumColId;
  prog[9] = (kOpCount << 26) | (0 << 16) | 0;   /* COUNT → agg[0] */
  prog[10] = (kOpSum << 26) | (0 << 16) | 1; /* SUM → agg[1] */

  return prog;
}

/*
 * Build aggregation program for:
 *   SELECT MAX(col) FROM t GROUP BY gbCol
 */
static std::vector<Uint32>
buildAggProgram_MaxGroupBy(Uint32 gbColId, Uint32 maxColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;  /* n_gb_cols=1, n_agg_results=1 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             maxColId;
  prog[10] = (kOpMax << 26) | (0 << 16) | 0;
  return prog;
}

/*
 * Build aggregation program for:
 *   SELECT MIN(col) FROM t GROUP BY gbCol
 */
static std::vector<Uint32>
buildAggProgram_MinGroupBy(Uint32 gbColId, Uint32 minColId)
{
  const Uint32 PROG_LEN = 11;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 1u;
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             minColId;
  prog[10] = (kOpMin << 26) | (0 << 16) | 0;
  return prog;
}

/*
 * Build aggregation program for:
 *   SELECT COUNT(*) FROM t GROUP BY gbCol
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
 *   SELECT COUNT(*), MAX(col), MIN(col) FROM t  (no GROUP BY)
 *
 * n_gb_cols=0, n_agg_results=3
 */
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

/*
 * Build aggregation program for:
 *   SELECT COUNT(*), SUM(val), MAX(val), MIN(val) FROM t GROUP BY grp
 *
 * n_gb_cols=1, n_agg_results=4
 */
static std::vector<Uint32>
buildAggProgram_AllAggsGroupBy(Uint32 gbColId, Uint32 valColId)
{
  const Uint32 PROG_LEN = 14;
  std::vector<Uint32> prog(PROG_LEN);
  prog[0] = (AGG_MAGIC << 16) | PROG_LEN;
  prog[1] = (1u << 16) | 4u;  /* n_gb_cols=1, n_agg_results=4 */
  prog[2] = PUSHDOWN_AGGREGATION_VERSION;
  prog[3] = prog[4] = prog[5] = prog[6] = prog[7] = 0;
  prog[8] = gbColId << 16;
  prog[9] = (kOpLoadCol << 26) | (COL_TYPE_BIGINT << 21) | (0 << 16) |
             valColId;
  prog[10] = (kOpCount << 26) | (0 << 16) | 0;  /* COUNT → agg[0] */
  prog[11] = (kOpSum << 26) | (0 << 16) | 1;    /* SUM → agg[1] */
  prog[12] = (kOpMax << 26) | (0 << 16) | 2;    /* MAX → agg[2] */
  prog[13] = (kOpMin << 26) | (0 << 16) | 3;    /* MIN → agg[3] */
  return prog;
}

/* ------------------------------------------------------------------ */
/* AttrInfo section builder for SCAN_FRAGREQ                          */
/* ------------------------------------------------------------------ */

/*
 * Build minimal AttrInfo for interpreted execution.
 * Layout: 5-word header + 1-word ExitOK program.
 *
 * cinBuffer sections:
 *   [0] = initial read length (0)
 *   [1] = program length (1 = ExitOK)
 *   [2] = subroutine length (0)
 *   [3] = final read length (0)
 *   [4] = linked attrs / RsubLen (0)
 *   [5] = ExitOK instruction
 */
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
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_test");
  if (sqlExec(conn,
        "CREATE TABLE jagg_test ("
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

/*
 * Query ndbinfo.operations_per_fragment via MySQL client to get the
 * LDM instance (block_instance) for each fragment on each node.
 */
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

  /*
   * Build a map: (node_id, fragment_num) → block_instance.
   * Then fill meta.fragInstances using the primary node for each fragment.
   */
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
   * Insert rows:
   *   (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)
   *
   * Expected GROUP BY a:
   *   group(1)=10, group(2)=20, group(3)=30, group(4)=40, group(5)=50
   * Expected SUM(b) without GROUP BY: 150
   * Expected COUNT(*): 5
   */
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
insertManyRows(Ndb *ndb, Uint32 count)
{
  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *ptab = dict->getTable(TABLE_NAME);
  if (ptab == nullptr) return -1;

  for (Uint32 i = 1; i <= count; i++) {
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
    Int64 a = (Int64)i;
    Int64 b = (Int64)(i * 10);
    op->equal("a", a);
    op->setValue("b", b);

    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert(%u) failed: %s\n",
              i, trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }

  V("Inserted %u rows into %s\n", count, TABLE_NAME);
  return 0;
}

static int
dropTestTable(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_test");
  return 0;
}

/* ------------------------------------------------------------------ */
/* 3-column table setup (pk, grp, val) for multi-row-per-group tests  */
/* ------------------------------------------------------------------ */

static const char *TABLE_NAME_3COL = "jagg_test3";

static int
createTestTable3Col(MYSQL *conn, Ndb *ndb, TableMeta &meta)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_test3");
  if (sqlExec(conn,
        "CREATE TABLE jagg_test3 ("
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
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    V("  fragment %u -> node %u\n", f, meta.fragNodes[f]);
  }

  return 0;
}

static int
dropTestTable3Col(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS jagg_test3");
  return 0;
}

/*
 * Insert rows into 3-col table with multiple rows per group:
 *   (pk=1, grp=1, val=10), (pk=2, grp=1, val=20)
 *   (pk=3, grp=2, val=30), (pk=4, grp=2, val=40), (pk=5, grp=2, val=50)
 *   (pk=6, grp=3, val=60)
 *
 * Expected GROUP BY grp:
 *   group(1): COUNT=2, SUM=30, MAX=20, MIN=10
 *   group(2): COUNT=3, SUM=120, MAX=50, MIN=30
 *   group(3): COUNT=1, SUM=60, MAX=60, MIN=60
 */
static int
insertMultiRowData(Ndb *ndb)
{
  struct Row { Int64 pk; Int64 grp; Int64 val; };
  static const Row rows[] = {
    {1, 1, 10}, {2, 1, 20},
    {3, 2, 30}, {4, 2, 40}, {5, 2, 50},
    {6, 3, 60}
  };

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *ptab = dict->getTable(TABLE_NAME_3COL);
  if (ptab == nullptr) return -1;

  for (const auto &row : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }
    NdbOperation *op = trans->getNdbOperation(ptab);
    if (op == nullptr) {
      fprintf(stderr, "getNdbOperation: %s\n", trans->getNdbError().message);
      trans->close();
      return -1;
    }
    op->insertTuple();
    op->equal("pk", row.pk);
    op->setValue("grp", row.grp);
    op->setValue("val", row.val);
    if (trans->execute(NdbTransaction::Commit) != 0) {
      fprintf(stderr, "insert3col(%lld,%lld,%lld) failed: %s\n",
              (long long)row.pk, (long long)row.grp, (long long)row.val,
              trans->getNdbError().message);
      trans->close();
      return -1;
    }
    trans->close();
  }

  V("Inserted %zu rows into %s\n",
    sizeof(rows)/sizeof(rows[0]), TABLE_NAME_3COL);
  return 0;
}

/*
 * Insert rows with negative values into the 2-col table:
 *   (1, -100), (2, 50), (3, -200), (4, 300), (5, -50)
 *
 * Expected: COUNT=5, SUM=0, MAX=300, MIN=-200
 */
static int
insertNegativeData(Ndb *ndb)
{
  struct Row { Int64 a; Int64 b; };
  static const Row rows[] = {
    {1, -100}, {2, 50}, {3, -200}, {4, 300}, {5, -50}
  };

  const NdbDictionary::Dictionary *dict = ndb->getDictionary();
  const NdbDictionary::Table *ptab = dict->getTable(TABLE_NAME);
  if (ptab == nullptr) return -1;

  for (const auto &row : rows) {
    NdbTransaction *trans = ndb->startTransaction();
    if (trans == nullptr) {
      fprintf(stderr, "startTransaction: %s\n", ndb->getNdbError().message);
      return -1;
    }
    NdbOperation *op = trans->getNdbOperation(ptab);
    if (op == nullptr) {
      fprintf(stderr, "getNdbOperation: %s\n", trans->getNdbError().message);
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

  V("Inserted %zu rows (negative values) into %s\n",
    sizeof(rows)/sizeof(rows[0]), TABLE_NAME);
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
/* JOIN_AGG_SETUP_REQ / CONF                                           */
/* ------------------------------------------------------------------ */

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
  req->expectedOpCount = 0;  /* unknown */
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

  /* Wait for SETUP_CONF or SETUP_REF */
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

/* ------------------------------------------------------------------ */
/* SCAN_FRAGREQ / SCAN_FRAGCONF                                        */
/* ------------------------------------------------------------------ */

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

  /* ScanFragReq fixed fields (12 words) */
  ScanFragReq *scanReq = reinterpret_cast<ScanFragReq *>(data);
  scanReq->senderData = fragId;  /* use fragId to correlate response */
  scanReq->resultRef = ss.getOwnRef();
  scanReq->savePointId = 0;

  /* requestInfo: ReadCommitted=1, CorrFactor=1, JoinAgg=1 */
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

  /*
   * variableData layout (CorrFactor=1, JoinAgg=1):
   *   [0] = corrFactorLo
   *   [1] = corrFactorHi
   *   [2] = aggStateKey
   */
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
  return 0;
}

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
/* JOIN_AGG_COMPLETE_REQ / result reception                            */
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
    /* Long signal: data in section 0 */
    data = sig->ptr[0].p;
    dataLen = sig->ptr[0].sz;
  } else {
    /* Short signal: data after TransIdAI header */
    data = sig->getDataPtr() + TransIdAI::HeaderLength;
    dataLen = sig->getLength() - TransIdAI::HeaderLength;
  }

  if (dataLen < 4) {
    fprintf(stderr, "TRANSID_AI too short: %u words\n", dataLen);
    return -1;
  }

  /* Word 0: (AGG_RESULT_ATTR << 16) | AGG_MAGIC */
  Uint32 magic = data[0];
  if (magic != ((AGG_RESULT_ATTR << 16) | AGG_MAGIC)) {
    fprintf(stderr, "Bad AGG_RESULT magic: 0x%08x (expected 0x%08x)\n",
            magic, (AGG_RESULT_ATTR << 16) | AGG_MAGIC);
    return -1;
  }

  result.n_gb_cols = data[1] >> 16;
  result.n_agg_results = data[1] & 0xFFFF;
  result.n_groups = data[2];

  /*
   * For non-group-by (n_gb_cols=0), DBLQH sets n_groups=0 but still
   * writes one entry with key_len=0 and val_len=agg_bytes.
   * We need to read that entry.
   */
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

/*
 * Wait for SCAN_FRAGCONF, handling interleaved TRANSID_AI signals
 * from evicted groups during the scan phase.
 */
static int
waitForScanConfWithEviction(SignalSender &ss, Uint32 /*fragId*/,
                            Uint32 &rowsScanned,
                            std::vector<AggResult> &evictedResults,
                            Uint32 &evictedGroups)
{
  for (;;) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS,
                                       "SCAN_FRAGCONF/TRANSID_AI");
    if (resp == nullptr) return -1;

    int gsn = getGsn(resp);
    if (gsn == GSN_TRANSID_AI) {
      AggResult result;
      if (parseTransIdAI(resp, result) != 0) return -1;
      V("  TRANSID_AI (evicted): n_groups=%u\n", result.n_groups);
      evictedGroups += result.n_groups;
      evictedResults.push_back(std::move(result));
    }
    else if (gsn == GSN_SCAN_FRAGCONF) {
      const ScanFragConf *conf =
        reinterpret_cast<const ScanFragConf *>(resp->getDataPtr());
      rowsScanned = conf->completedOps;
      Uint32 sigLen = resp->header.theLength;
      Uint32 rowsExamined =
        (sigLen >= ScanFragConf::SignalLength_v2) ? conf->rowsExamined : 0;
      V("  SCAN_FRAGCONF: frag=%u completed=%u rowsExamined=%u\n",
             conf->senderData, conf->completedOps, rowsExamined);
      return 0;
    }
    else if (gsn == GSN_SCAN_FRAGREF) {
      const Uint32 *d = resp->getDataPtr();
      fprintf(stderr, "  SCAN_FRAGREF: errorCode=%u\n", d[3]);
      return -1;
    }
    else {
      fprintf(stderr, "Unexpected GSN %d in waitForScanConfWithEviction\n",
              gsn);
      return -1;
    }
  }
}

/*
 * Extract the Int64 sum value from an AggResItem in the result bytes.
 * AggResItem layout (assuming 8-byte aligned):
 *   offset 0: DataType type (4 bytes)
 *   offset 4: padding (4 bytes)
 *   offset 8: DataValue value (8 bytes) — the sum as Int64
 *   offset 16: bool is_unsigned (1 byte)
 *   offset 17: bool is_null (1 byte)
 *   Total: ~24 bytes
 *
 * We only extract the Int64 at offset 8.
 */
static Int64
extractSumBigint(const std::vector<Uint8> &val, Uint32 aggIdx)
{
  /* sizeof(AggResItem) should be 24 on 64-bit with 8-byte alignment */
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

/*
 * Extract BIGINT group-by key value.
 * Key format per column: [AttributeHeader(4 bytes)][column data].
 * For a single BIGINT column: 4 + 8 = 12 bytes total.
 */
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
                Uint32 maxBatchRows)
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

  /* COMPLETE_REQ is handled by Dblqh instance 1 (not the proxy) */
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
  V("  → sent JOIN_AGG_SEND_CONF to node %u\n", nodeId);
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
      /* Flow control: DBLQH wants permission to send more */
      const JoinAggSendReq *sendReq =
        reinterpret_cast<const JoinAggSendReq *>(resp->getDataPtr());
      V("  JOIN_AGG_SEND_REQ: rowsSent=%u bytes=%u\n",
             sendReq->numRowsSent, sendReq->resultBytes);
      /* Reply with SEND_CONF to continue, large batch */
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
      /* Ignore and continue */
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
/* SQL verification helpers                                            */
/* ------------------------------------------------------------------ */

/*
 * Run a GROUP BY query expecting 2-column result (key, value).
 * Compare with expected map<Int64, Int64>.
 */
static int
verifySqlGroupByMap(const char *query,
                    const std::map<Int64, Int64> &expected)
{
  if (g_mysql_conn == nullptr) return 0;  /* skip if no MySQL */

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
  V("  SQL verify: %zu groups — matches\n", sqlGroups.size());
  return 0;
}

/*
 * Run a scalar query (no GROUP BY), compare up to 3 Int64 values.
 */
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
/* Test 1: SUM(b) GROUP BY a                                           */
/* ------------------------------------------------------------------ */

static int
testSumGroupBy(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n========================================\n");
  V("Test 1: SELECT SUM(b) FROM %s GROUP BY a\n", TABLE_NAME);
  V("========================================\n");

  /* Collect unique data nodes */
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Build aggregation program */
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  /* 1. Setup on all data nodes */
  std::map<Uint32, Uint32> aggStateKeys;  /* nodeId → aggStateKey */
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED,
                     key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  /* 2. Scan all fragments */
  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsScanned = 0;

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 fragNodeId = meta.fragNodes[f];
    Uint32 ldmInst = meta.fragInstances[f];
    Uint32 stateKey = aggStateKeys[fragNodeId];
    if (sendScanFragReq(ss, fragNodeId, f, ldmInst,
                        stateKey, meta, attrInfo) != 0)
      return -1;

    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0)
      return -1;
    totalRowsScanned += rows;
  }

  V("\nTotal rows scanned: %u\n", totalRowsScanned);

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

  /* 6. Validate */
  V("\n--- Validation ---\n");

  /* Expected: 5 groups, each with SUM(b)=b (since each key is unique) */
  std::map<Int64, Int64> expected;
  expected[1] = 10;
  expected[2] = 20;
  expected[3] = 30;
  expected[4] = 40;
  expected[5] = 50;

  if (totalGroups != 5) {
    fprintf(stderr, "FAIL: expected 5 groups, got %u\n", totalGroups);
    return -1;
  }

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

  int failures = 0;
  for (const auto &exp : expected) {
    auto it = actual.find(exp.first);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n",
              (long long)exp.first);
      failures++;
    } else if (it->second != exp.second) {
      fprintf(stderr, "FAIL: group(%lld) expected SUM=%lld, got %lld\n",
              (long long)exp.first, (long long)exp.second,
              (long long)it->second);
      failures++;
    }
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, SUM(b) FROM jagg_test GROUP BY a",
                            expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 1 — all %u groups match expected values\n",
           totalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: COUNT(*), SUM(b) without GROUP BY                           */
/* ------------------------------------------------------------------ */

static int
testCountSumNoGroupBy(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=========================================\n");
  V("Test 2: SELECT COUNT(*), SUM(b) FROM %s\n", TABLE_NAME);
  V("=========================================\n");

  /* Collect unique data nodes */
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  /* Build aggregation program */
  auto aggProg = buildAggProgram_CountSum(meta.attrIdB);

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

  /* 2. Scan all fragments */
  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsScanned = 0;

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 fragNodeId = meta.fragNodes[f];
    Uint32 ldmInst = meta.fragInstances[f];
    Uint32 stateKey = aggStateKeys[fragNodeId];
    if (sendScanFragReq(ss, fragNodeId, f, ldmInst,
                        stateKey, meta, attrInfo) != 0)
      return -1;

    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0)
      return -1;
    totalRowsScanned += rows;
  }

  V("\nTotal rows scanned: %u\n", totalRowsScanned);

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

  /* 6. Validate — non-group-by: n_gb_cols=0, 1 result row, 2 agg results */
  V("\n--- Validation ---\n");

  if (allResults.empty()) {
    fprintf(stderr, "FAIL: no results received\n");
    return -1;
  }

  /* For non-group-by, each node returns one TRANSID_AI with n_groups=0
   * containing partial accumulators. Sum across all nodes.
   */
  Uint64 count = 0;
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
    count += extractCountBigint(val, 0);
    sum += extractSumBigint(val, 1);
  }

  V("  COUNT(*) = %llu\n", (unsigned long long)count);
  V("  SUM(b) = %lld\n", (long long)sum);

  int failures = 0;
  if (count != 5) {
    fprintf(stderr, "FAIL: expected COUNT(*)=5, got %llu\n",
            (unsigned long long)count);
    failures++;
  }
  if (sum != 150) {
    fprintf(stderr, "FAIL: expected SUM(b)=150, got %lld\n",
            (long long)sum);
    failures++;
  }

  if (failures == 0) {
    Int64 exp[] = {5, 150};
    if (verifySqlScalars("SELECT COUNT(*), SUM(b) FROM jagg_test", exp, 2) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 2 — COUNT(*)=5, SUM(b)=150\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: High-cardinality GROUP BY (MUTEX_FREE)                      */
/* ------------------------------------------------------------------ */

static int
testHighCardinalityGroupBy(Ndb * /*ndb*/, SignalSender &ss,
                           const TableMeta &meta, Uint32 numRows)
{
  V("\n=============================================\n");
  V("Test 3: SELECT SUM(b) FROM %s GROUP BY a (%u rows, MUTEX_FREE)\n",
    TABLE_NAME, numRows);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  /* 1. Setup on all data nodes — MUTEX_FREE strategy */
  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_FREE,
                     key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  /* 2. Scan all fragments */
  auto attrInfo = buildAttrInfo();

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 fragNodeId = meta.fragNodes[f];
    Uint32 ldmInst = meta.fragInstances[f];
    Uint32 stateKey = aggStateKeys[fragNodeId];
    if (sendScanFragReq(ss, fragNodeId, f, ldmInst,
                        stateKey, meta, attrInfo) != 0)
      return -1;

    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0)
      return -1;
  }

  /* 3. Complete + receive results from all nodes */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0)
      return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0)
      return -1;
  }

  /* 4. Release */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  /* 5. Validate — numRows groups, each SUM(b) = a*10 */
  V("\n--- Validation ---\n");

  if (totalGroups != numRows) {
    fprintf(stderr, "FAIL: expected %u groups, got %u\n", numRows, totalGroups);
    return -1;
  }

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] = sum;
    }
  }

  int failures = 0;
  for (Uint32 i = 1; i <= numRows; i++) {
    Int64 key = (Int64)i;
    Int64 expectedSum = (Int64)(i * 10);
    auto it = actual.find(key);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)key);
      failures++;
    } else if (it->second != expectedSum) {
      fprintf(stderr, "FAIL: group(%lld) expected SUM=%lld, got %lld\n",
              (long long)key, (long long)expectedSum, (long long)it->second);
      failures++;
    }
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, SUM(b) FROM jagg_test GROUP BY a",
                            actual) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 3 — all %u groups match (MUTEX_FREE, high cardinality)\n",
           totalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: Eviction via ERROR_INSERT 5090                              */
/* ------------------------------------------------------------------ */

static int
testEviction(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
             Uint32 numRows, NdbRestarter &restarter)
{
  V("\n=============================================\n");
  V("Test 4: Eviction test (%u rows, ERROR_INSERT 5090)\n", numRows);
  V("=============================================\n");

  /* Inject error 5090 in all data nodes — limits max_groups to 3 */
  if (restarter.insertErrorInAllNodes(5090) != 0) {
    fprintf(stderr, "FAIL: insertErrorInAllNodes(5090) failed\n");
    return -1;
  }
  V("ERROR_INSERT 5090 set in all nodes\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());

  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  /* 1. Setup on all data nodes */
  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED,
                     key) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
    aggStateKeys[nd] = key;
  }

  /* 2. Scan all fragments — expect interleaved TRANSID_AI (evictions) */
  auto attrInfo = buildAttrInfo();
  std::vector<AggResult> evictedResults;
  Uint32 evictedGroups = 0;

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    Uint32 fragNodeId = meta.fragNodes[f];
    Uint32 ldmInst = meta.fragInstances[f];
    Uint32 stateKey = aggStateKeys[fragNodeId];
    if (sendScanFragReq(ss, fragNodeId, f, ldmInst,
                        stateKey, meta, attrInfo) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }

    Uint32 rows = 0;
    if (waitForScanConfWithEviction(ss, f, rows,
                                    evictedResults, evictedGroups) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
  }

  V("Evicted %u groups during scan phase\n", evictedGroups);

  /* 3. Complete + receive remaining results */
  std::vector<AggResult> finalResults;
  Uint32 finalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
    if (receiveResults(ss, finalResults, finalGroups) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
  }

  /* 4. Release */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
  }

  /* Clear error insert */
  restarter.insertErrorInAllNodes(0);
  V("ERROR_INSERT cleared\n");

  /* 5. Validate — merge evicted + finalized groups */
  V("\n--- Validation ---\n");
  V("Evicted groups: %u, finalized groups: %u\n", evictedGroups, finalGroups);

  /*
   * Evicted groups: sent during scan phase (one at a time, SUM is partial
   * since a group may be evicted then re-created and evicted again).
   * For the same key, we need to SUM the partial results.
   * Finalized groups: sent during complete phase (accumulated after eviction).
   */
  std::map<Int64, Int64> actual;
  for (const auto &res : evictedResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] += sum;
    }
  }
  for (const auto &res : finalResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] += sum;
    }
  }

  if (actual.size() != numRows) {
    fprintf(stderr, "FAIL: expected %u distinct groups, got %zu\n",
            numRows, actual.size());
    return -1;
  }

  int failures = 0;
  for (Uint32 i = 1; i <= numRows; i++) {
    Int64 key = (Int64)i;
    Int64 expectedSum = (Int64)(i * 10);
    auto it = actual.find(key);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)key);
      failures++;
    } else if (it->second != expectedSum) {
      fprintf(stderr, "FAIL: group(%lld) expected SUM=%lld, got %lld\n",
              (long long)key, (long long)expectedSum, (long long)it->second);
      failures++;
    }
  }

  if (evictedGroups == 0) {
    fprintf(stderr, "FAIL: expected evictions but got 0 evicted groups\n");
    failures++;
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, SUM(b) FROM jagg_test GROUP BY a",
                            actual) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 4 — eviction test, %u groups correct "
           "(%u evicted + %u finalized)\n",
           (Uint32)actual.size(), evictedGroups, finalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: LQHKEYREQ with join aggregation                            */
/* ------------------------------------------------------------------ */

struct LqhKeyReqBuilder {
  static int send(SignalSender &ss, Ndb *ndb,
                  Uint32 nodeId, Uint32 ldmInstance,
                  Uint32 fragId, Uint32 aggStateKey,
                  Int64 keyValue,
                  const TableMeta &meta,
                  const std::vector<Uint32> &attrInfo)
  {
    V("  LQHKEYREQ → node %u, frag %u, LDM %u, key=%lld\n",
      nodeId, fragId, ldmInstance, (long long)keyValue);

    /* Compute hash for the key */
    const NdbDictionary::Table *ptab =
      ndb->getDictionary()->getTable(TABLE_NAME);
    if (ptab == nullptr) {
      fprintf(stderr, "getTable for hash: %s\n",
              ndb->getDictionary()->getNdbError().message);
      return -1;
    }

    /* Ndb::computeHash returns values[1] (distribution hash).
     * But LQHKEYREQ needs values[0] (ACC primary key hash).
     * Compute both using rondb_calc_hash directly.
     */
    Uint32 hashValues[4];
    Uint32 keyWords[2];
    memcpy(keyWords, &keyValue, sizeof(Int64));
    rondb_calc_hash(hashValues, (const char *)keyWords, 2,
                    ptab->use_new_hash_function());
    Uint32 accHash = hashValues[0];     /* For LQHKEYREQ hashValue (ACC) */
    (void)hashValues[1];  /* Distribution hash — used by caller via computeHash */

    SimpleSignal ssig;
    LqhKeyReq *req = reinterpret_cast<LqhKeyReq *>(ssig.getDataPtrSend());
    memset(req, 0, sizeof(LqhKeyReq));

    req->clientConnectPtr = fragId;

    Uint32 attrLen = 0;
    LqhKeyReq::setJoinAggFlag(attrLen, 1);
    req->attrLen = attrLen;

    req->hashValue = accHash;

    Uint32 requestInfo = 0;
    LqhKeyReq::setOperation(requestInfo, ZREAD);
    LqhKeyReq::setDirtyFlag(requestInfo, 1);
    LqhKeyReq::setSimpleFlag(requestInfo, 1);
    LqhKeyReq::setInterpretedFlag(requestInfo, 1);
    LqhKeyReq::setNoDiskFlag(requestInfo, 1);
    LqhKeyReq::setNormalProtocolFlag(requestInfo, 1);
    LqhKeyReq::setCorrFactorFlag(requestInfo, 1);
    LqhKeyReq::setKeyLen(requestInfo, 2);
    req->requestInfo = requestInfo;

    /* tcBlockref = our SignalSender.  The tc-node-status check in LQH
     * is temporarily disabled via DUMP LqhSkipTcNodeCheck (debug builds).
     * LQH sends LQHKEYCONF directly to us (send_packed=false since our
     * block is not DBTC/DBLQH).
     */
    req->tcBlockref = ss.getOwnRef();

    req->tableSchemaVersion =
      meta.tableId | (meta.schemaVersion << 16);

    req->fragmentData = fragId;  // Low 16 bits = fragmentId

    req->transId1 = FAKE_TRANS_ID1;
    req->transId2 = FAKE_TRANS_ID2;

    req->savePointId = 0;
    req->scanInfo = 0;

    req->variableData[0] = 0;  /* corrFactorLo */
    req->variableData[1] = 0;  /* corrFactorHi */
    req->variableData[2] = aggStateKey;
    Uint32 sigLen = LqhKeyReq::FixedSignalLength + 3;

    Uint16 recBlock = numberToBlock(V_QUERY, ldmInstance);
    ssig.set(ss, 0, recBlock, GSN_LQHKEYREQ, sigLen);

    Uint32 keyData[2];
    memcpy(keyData, &keyValue, sizeof(Int64));
    ssig.header.m_noOfSections = 2;
    ssig.ptr[0].p = keyData;
    ssig.ptr[0].sz = 2;
    ssig.ptr[1].p = attrInfo.data();
    ssig.ptr[1].sz = (Uint32)attrInfo.size();

    if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
      fprintf(stderr, "sendSignal LQHKEYREQ failed\n");
      return -1;
    }
    return 0;
  }
};

static int
waitForLqhKeyConf(SignalSender &ss)
{
  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "LQHKEYCONF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  if (gsn == GSN_LQHKEYCONF) {
    V("  LQHKEYCONF received\n");
    return 0;
  } else if (gsn == GSN_LQHKEYREF) {
    const Uint32 *d = resp->getDataPtr();
    fprintf(stderr, "  LQHKEYREF: errorCode=%u\n", d[2]);
    return -1;
  } else {
    fprintf(stderr, "Unexpected GSN %d waiting for LQHKEYCONF\n", gsn);
    return -1;
  }
}

static int
testLqhKeyReq(Ndb *ndb, SignalSender &ss, const TableMeta &meta,
              NdbRestarter &restarter)
{
  V("\n=============================================\n");
  V("Test 5: LQHKEYREQ with join aggregation\n");
  V("=============================================\n");

  /* We use the 5-row table from Tests 1-2.
   * Send LQHKEYREQ for each key, each goes through handleJoinAggRow.
   * Then COMPLETE to get aggregated results.
   */
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

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

  /* 2. Send LQHKEYREQ for each key value */
  auto attrInfo = buildAttrInfo();

  /* Determine which fragment each key maps to.
   * Use computeHash + getPartitionId to find the fragment.
   */
  const NdbDictionary::Table *ptab =
    ndb->getDictionary()->getTable(TABLE_NAME);
  if (ptab == nullptr) {
    fprintf(stderr, "getTable for partition: %s\n",
            ndb->getDictionary()->getNdbError().message);
    return -1;
  }

  /* Debug: verify rows are readable via NDB API before LQHKEYREQ */
  {
    NdbTransaction *verifyTx = ndb->startTransaction();
    if (verifyTx == nullptr) {
      fprintf(stderr, "startTransaction for verify failed: %s\n",
              ndb->getNdbError().message);
      return -1;
    }
    for (Int64 v = 1; v <= 5; v++) {
      NdbOperation *vop = verifyTx->getNdbOperation(TABLE_NAME);
      vop->readTuple(NdbOperation::LM_CommittedRead);
      vop->equal("a", v);
      NdbRecAttr *ra = vop->getValue("b");
      if (verifyTx->execute(NdbTransaction::NoCommit) != 0) {
        fprintf(stderr, "NDB API read key=%lld failed: %s\n",
                (long long)v, verifyTx->getNdbError().message);
        ndb->closeTransaction(verifyTx);
        return -1;
      }
      V("  NDB API verify: key=%lld → b=%lld\n", (long long)v,
        (long long)ra->int64_value());
    }
    ndb->closeTransaction(verifyTx);
  }

  /* Disable the tc-node-status check in LQH so that tcBlockref can
   * reference our API node (debug/test builds only).
   */
  {
    int dumpSkip[1] = {DumpStateOrd::LqhSkipTcNodeCheck};
    restarter.dumpStateAllNodes(dumpSkip, 1);
    V("  DUMP LqhSkipTcNodeCheck sent to all nodes\n");
  }

  for (Int64 keyVal = 1; keyVal <= 5; keyVal++) {
    Uint32 hashValue = 0;
    Ndb::Key_part_ptr keyParts[2];
    keyParts[0].ptr = &keyVal;
    keyParts[0].len = sizeof(Int64);
    keyParts[1].ptr = nullptr;
    keyParts[1].len = 0;
    if (Ndb::computeHash(&hashValue, ptab, keyParts, nullptr, 0) != 0) {
      fprintf(stderr, "computeHash failed for key %lld\n", (long long)keyVal);
      return -1;
    }
    Uint32 fragId = ptab->getPartitionId(hashValue);
    Uint32 fragNodeId = meta.fragNodes[fragId];
    Uint32 ldmInst = meta.fragInstances[fragId];
    Uint32 stateKey = aggStateKeys[fragNodeId];

    V("  key=%lld → hash=0x%08x frag=%u node=%u LDM=%u\n",
      (long long)keyVal, hashValue, fragId, fragNodeId, ldmInst);

    if (LqhKeyReqBuilder::send(ss, ndb, fragNodeId, ldmInst, fragId,
                               stateKey, keyVal, meta, attrInfo) != 0)
      return -1;

    if (waitForLqhKeyConf(ss) != 0)
      return -1;
  }

  /* Restore the tc-node-status check */
  {
    int dumpRestore[1] = {DumpStateOrd::LqhRestoreTcNodeCheck};
    restarter.dumpStateAllNodes(dumpRestore, 1);
    V("  DUMP LqhRestoreTcNodeCheck sent to all nodes\n");
  }

  /* 3. Complete + receive results from all nodes */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0)
      return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0)
      return -1;
  }

  /* 4. Release */
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  /* 5. Validate — 5 groups, each SUM(b) = key*10 */
  V("\n--- Validation ---\n");

  if (totalGroups != 5) {
    fprintf(stderr, "FAIL: expected 5 groups, got %u\n", totalGroups);
    return -1;
  }

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] = sum;
      V("  group(%lld) = SUM %lld\n", (long long)key, (long long)sum);
    }
  }

  int failures = 0;
  std::map<Int64, Int64> expected;
  expected[1] = 10; expected[2] = 20; expected[3] = 30;
  expected[4] = 40; expected[5] = 50;
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
    if (verifySqlGroupByMap("SELECT a, SUM(b) FROM jagg_test GROUP BY a",
                            expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 5 — LQHKEYREQ join aggregation, 5 groups correct\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: MAX(b) GROUP BY a                                           */
/* ------------------------------------------------------------------ */

static int
testMaxGroupBy(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n========================================\n");
  V("Test 6: SELECT MAX(b) FROM %s GROUP BY a\n", TABLE_NAME);
  V("========================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_MaxGroupBy(meta.attrIdA, meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (totalGroups != 5) {
    fprintf(stderr, "FAIL: expected 5 groups, got %u\n", totalGroups);
    return -1;
  }

  /* With unique keys, MAX(b) = b for each group */
  std::map<Int64, Int64> expected;
  expected[1] = 10; expected[2] = 20; expected[3] = 30;
  expected[4] = 40; expected[5] = 50;

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 val = extractSumBigint(grp.second, 0);  /* same layout */
      actual[key] = val;
      V("  group(%lld) = MAX %lld\n", (long long)key, (long long)val);
    }
  }

  int failures = 0;
  for (const auto &exp : expected) {
    auto it = actual.find(exp.first);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)exp.first);
      failures++;
    } else if (it->second != exp.second) {
      fprintf(stderr, "FAIL: group(%lld) expected MAX=%lld, got %lld\n",
              (long long)exp.first, (long long)exp.second,
              (long long)it->second);
      failures++;
    }
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, MAX(b) FROM jagg_test GROUP BY a",
                            expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 6 — MAX(b) GROUP BY a, all %u groups correct\n",
           totalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 7: MIN(b) GROUP BY a                                           */
/* ------------------------------------------------------------------ */

static int
testMinGroupBy(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n========================================\n");
  V("Test 7: SELECT MIN(b) FROM %s GROUP BY a\n", TABLE_NAME);
  V("========================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_MinGroupBy(meta.attrIdA, meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (totalGroups != 5) {
    fprintf(stderr, "FAIL: expected 5 groups, got %u\n", totalGroups);
    return -1;
  }

  std::map<Int64, Int64> expected;
  expected[1] = 10; expected[2] = 20; expected[3] = 30;
  expected[4] = 40; expected[5] = 50;

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 val = extractSumBigint(grp.second, 0);
      actual[key] = val;
      V("  group(%lld) = MIN %lld\n", (long long)key, (long long)val);
    }
  }

  int failures = 0;
  for (const auto &exp : expected) {
    auto it = actual.find(exp.first);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)exp.first);
      failures++;
    } else if (it->second != exp.second) {
      fprintf(stderr, "FAIL: group(%lld) expected MIN=%lld, got %lld\n",
              (long long)exp.first, (long long)exp.second,
              (long long)it->second);
      failures++;
    }
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, MIN(b) FROM jagg_test GROUP BY a",
                            expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 7 — MIN(b) GROUP BY a, all %u groups correct\n",
           totalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 8: COUNT(*), MAX(b), MIN(b) without GROUP BY                   */
/* ------------------------------------------------------------------ */

static int
testMaxMinNoGroupBy(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 8: SELECT COUNT(*), MAX(b), MIN(b) FROM %s\n", TABLE_NAME);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_CountMaxMin(meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (allResults.empty()) {
    fprintf(stderr, "FAIL: no results received\n");
    return -1;
  }

  /*
   * For MAX/MIN across multiple nodes: take max of maxes, min of mins.
   * COUNT sums across nodes.
   */
  Uint64 count = 0;
  Int64 maxVal = INT64_MIN;
  Int64 minVal = INT64_MAX;
  bool first = true;
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
    Uint64 nodeCount = extractCountBigint(val, 0);
    Int64 nodeMax = extractSumBigint(val, 1);
    Int64 nodeMin = extractSumBigint(val, 2);
    count += nodeCount;
    if (nodeCount > 0) {
      if (first) {
        maxVal = nodeMax;
        minVal = nodeMin;
        first = false;
      } else {
        if (nodeMax > maxVal) maxVal = nodeMax;
        if (nodeMin < minVal) minVal = nodeMin;
      }
    }
  }

  V("  COUNT(*) = %llu\n", (unsigned long long)count);
  V("  MAX(b) = %lld\n", (long long)maxVal);
  V("  MIN(b) = %lld\n", (long long)minVal);

  int failures = 0;
  if (count != 5) {
    fprintf(stderr, "FAIL: expected COUNT(*)=5, got %llu\n",
            (unsigned long long)count);
    failures++;
  }
  if (maxVal != 50) {
    fprintf(stderr, "FAIL: expected MAX(b)=50, got %lld\n",
            (long long)maxVal);
    failures++;
  }
  if (minVal != 10) {
    fprintf(stderr, "FAIL: expected MIN(b)=10, got %lld\n",
            (long long)minVal);
    failures++;
  }

  if (failures == 0) {
    Int64 exp[] = {5, 50, 10};
    if (verifySqlScalars("SELECT COUNT(*), MAX(b), MIN(b) FROM jagg_test",
                          exp, 3) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 8 — COUNT(*)=5, MAX(b)=50, MIN(b)=10\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 9: Empty table — COUNT(*), SUM(b)                              */
/* ------------------------------------------------------------------ */

static int
testEmptyTable(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 9: SELECT COUNT(*), SUM(b) FROM %s (empty)\n", TABLE_NAME);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_CountSum(meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsScanned = 0;
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
    totalRowsScanned += rows;
  }

  V("\nTotal rows scanned: %u (should be 0)\n", totalRowsScanned);

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  /*
   * With the kernel fix, nodes with 0 processed rows skip TRANSID_AI.
   * An empty table means all nodes have 0 rows → 0 TRANSID_AI signals.
   */
  Uint64 count = 0;
  Int64 sum = 0;
  for (const auto &res : allResults) {
    if (res.groups.empty()) continue;
    const auto &val = res.groups[0].second;
    count += extractCountBigint(val, 0);
    sum += extractSumBigint(val, 1);
  }

  V("  COUNT(*) = %llu (from %zu TRANSID_AI signals)\n",
    (unsigned long long)count, allResults.size());
  V("  SUM(b) = %lld\n", (long long)sum);

  int failures = 0;
  if (count != 0) {
    fprintf(stderr, "FAIL: expected COUNT(*)=0, got %llu\n",
            (unsigned long long)count);
    failures++;
  }
  if (sum != 0) {
    fprintf(stderr, "FAIL: expected SUM(b)=0, got %lld\n", (long long)sum);
    failures++;
  }
  if (totalRowsScanned != 0) {
    fprintf(stderr, "FAIL: expected 0 rows scanned, got %u\n",
            totalRowsScanned);
    failures++;
  }

  if (failures == 0) {
    Int64 exp[] = {0, 0};
    if (verifySqlScalars("SELECT COUNT(*), COALESCE(SUM(b),0) FROM jagg_test",
                          exp, 2) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 9 — empty table, COUNT(*)=0, SUM(b)=0\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 10: Multiple rows per group — SUM(val) GROUP BY grp            */
/* ------------------------------------------------------------------ */

static int
testMultiRowPerGroup(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 10: SELECT SUM(val) FROM %s GROUP BY grp\n", TABLE_NAME_3COL);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  /* GROUP BY grp (attrIdB), SUM val (attrIdC) */
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (totalGroups != 3) {
    fprintf(stderr, "FAIL: expected 3 groups, got %u\n", totalGroups);
    return -1;
  }

  std::map<Int64, Int64> expected;
  expected[1] = 30;   /* 10 + 20 */
  expected[2] = 120;  /* 30 + 40 + 50 */
  expected[3] = 60;

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] += sum;
      V("  group(%lld) += SUM %lld\n", (long long)key, (long long)sum);
    }
  }

  int failures = 0;
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
    if (verifySqlGroupByMap("SELECT grp, SUM(val) FROM jagg_test3 GROUP BY grp",
                            expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 10 — multi-row-per-group SUM, 3 groups correct\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 11: All aggregates — COUNT, SUM, MAX, MIN GROUP BY grp         */
/* ------------------------------------------------------------------ */

static int
testAllAggsGroupBy(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 11: SELECT COUNT(*), SUM(val), MAX(val), MIN(val) "
    "FROM %s GROUP BY grp\n", TABLE_NAME_3COL);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_AllAggsGroupBy(meta.attrIdB, meta.attrIdC);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (totalGroups != 3) {
    fprintf(stderr, "FAIL: expected 3 groups, got %u\n", totalGroups);
    return -1;
  }

  struct Expected { Uint64 count; Int64 sum; Int64 max; Int64 min; };
  std::map<Int64, Expected> expected;
  expected[1] = {2, 30, 20, 10};
  expected[2] = {3, 120, 50, 30};
  expected[3] = {1, 60, 60, 60};

  /* Collect per-group results — aggregate partial results across nodes */
  struct Actual { Uint64 count; Int64 sum; Int64 max; Int64 min; bool init; };
  std::map<Int64, Actual> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Uint64 cnt = extractCountBigint(grp.second, 0);
      Int64 s = extractSumBigint(grp.second, 1);
      Int64 mx = extractSumBigint(grp.second, 2);
      Int64 mn = extractSumBigint(grp.second, 3);
      auto &a = actual[key];
      if (!a.init) {
        a.count = cnt; a.sum = s; a.max = mx; a.min = mn; a.init = true;
      } else {
        a.count += cnt;
        a.sum += s;
        if (mx > a.max) a.max = mx;
        if (mn < a.min) a.min = mn;
      }
      V("  group(%lld): COUNT=%llu SUM=%lld MAX=%lld MIN=%lld\n",
        (long long)key, (unsigned long long)cnt, (long long)s,
        (long long)mx, (long long)mn);
    }
  }

  int failures = 0;
  for (const auto &exp : expected) {
    auto it = actual.find(exp.first);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)exp.first);
      failures++;
      continue;
    }
    const auto &a = it->second;
    const auto &e = exp.second;
    if (a.count != e.count) {
      fprintf(stderr, "FAIL: group(%lld) COUNT=%llu, expected %llu\n",
              (long long)exp.first, (unsigned long long)a.count,
              (unsigned long long)e.count);
      failures++;
    }
    if (a.sum != e.sum) {
      fprintf(stderr, "FAIL: group(%lld) SUM=%lld, expected %lld\n",
              (long long)exp.first, (long long)a.sum, (long long)e.sum);
      failures++;
    }
    if (a.max != e.max) {
      fprintf(stderr, "FAIL: group(%lld) MAX=%lld, expected %lld\n",
              (long long)exp.first, (long long)a.max, (long long)e.max);
      failures++;
    }
    if (a.min != e.min) {
      fprintf(stderr, "FAIL: group(%lld) MIN=%lld, expected %lld\n",
              (long long)exp.first, (long long)a.min, (long long)e.min);
      failures++;
    }
  }

  if (failures == 0) {
    std::map<Int64, Int64> expectedSum;
    expectedSum[1] = 30; expectedSum[2] = 120; expectedSum[3] = 60;
    if (verifySqlGroupByMap(
            "SELECT grp, SUM(val) FROM jagg_test3 GROUP BY grp",
            expectedSum) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 11 — COUNT/SUM/MAX/MIN GROUP BY, 3 groups correct\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 12: Flow control with small batch size                         */
/* ------------------------------------------------------------------ */

/*
 * receiveResults variant that uses a fixed small batch size for
 * SEND_CONF replies, forcing multiple SEND_REQ/SEND_CONF round-trips.
 */
static int
receiveResultsSmallBatch(SignalSender &ss,
                         std::vector<AggResult> &allResults,
                         Uint32 &totalGroups,
                         Uint32 batchSize,
                         Uint32 &sendReqCount)
{
  V("Waiting for results (small batch=%u)...\n", batchSize);
  Uint32 nodeGroups = 0;
  bool done = false;

  while (!done) {
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "results");
    if (resp == nullptr) return -1;

    int gsn = getGsn(resp);
    if (gsn == GSN_TRANSID_AI) {
      AggResult result;
      if (parseTransIdAI(resp, result) != 0) return -1;
      nodeGroups += result.n_groups;
      totalGroups += result.n_groups;
      allResults.push_back(std::move(result));
    }
    else if (gsn == GSN_JOIN_AGG_SEND_REQ) {
      const JoinAggSendReq *sendReq =
        reinterpret_cast<const JoinAggSendReq *>(resp->getDataPtr());
      V("  JOIN_AGG_SEND_REQ: rowsSent=%u bytes=%u\n",
        sendReq->numRowsSent, sendReq->resultBytes);
      sendReqCount++;
      /* Reply with small batch to force more rounds */
      if (sendSendConf(ss, sendReq, batchSize) != 0) return -1;
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

  V("Received %u groups, %u SEND_REQ round-trips\n",
    nodeGroups, sendReqCount);
  return 0;
}

static int
testFlowControl(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
                Uint32 numRows)
{
  V("\n=============================================\n");
  V("Test 12: Flow control (%u rows, batch=2)\n", numRows);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  /* Use maxBatchRows=2 in COMPLETE_REQ and in SEND_CONF replies */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  Uint32 totalSendReqs = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 2) != 0) return -1;
    Uint32 sendReqs = 0;
    if (receiveResultsSmallBatch(ss, allResults, totalGroups,
                                  2, sendReqs) != 0)
      return -1;
    totalSendReqs += sendReqs;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  V("Total SEND_REQs: %u\n", totalSendReqs);

  if (totalGroups != numRows) {
    fprintf(stderr, "FAIL: expected %u groups, got %u\n", numRows, totalGroups);
    return -1;
  }

  /* Verify all groups have correct SUM */
  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] = sum;
    }
  }

  int failures = 0;
  for (Uint32 i = 1; i <= numRows; i++) {
    Int64 key = (Int64)i;
    Int64 expectedSum = (Int64)(i * 10);
    auto it = actual.find(key);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)key);
      failures++;
    } else if (it->second != expectedSum) {
      fprintf(stderr, "FAIL: group(%lld) expected SUM=%lld, got %lld\n",
              (long long)key, (long long)expectedSum, (long long)it->second);
      failures++;
    }
  }

  /* With 200 groups and batch_size=2, we expect many SEND_REQ round-trips */
  if (totalSendReqs == 0) {
    fprintf(stderr, "FAIL: expected SEND_REQ round-trips but got 0\n");
    failures++;
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, SUM(b) FROM jagg_test GROUP BY a",
                            actual) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 12 — flow control, %u groups correct, "
           "%u SEND_REQ round-trips\n", totalGroups, totalSendReqs);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 13: Single row table                                           */
/* ------------------------------------------------------------------ */

static int
testSingleRow(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 13: SELECT COUNT(*), SUM(b) FROM %s (1 row)\n", TABLE_NAME);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_CountSum(meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  Uint64 count = 0;
  Int64 sum = 0;
  for (const auto &res : allResults) {
    if (res.groups.empty()) continue;
    const auto &val = res.groups[0].second;
    count += extractCountBigint(val, 0);
    sum += extractSumBigint(val, 1);
  }

  V("  COUNT(*) = %llu\n", (unsigned long long)count);
  V("  SUM(b) = %lld\n", (long long)sum);

  int failures = 0;
  if (count != 1) {
    fprintf(stderr, "FAIL: expected COUNT(*)=1, got %llu\n",
            (unsigned long long)count);
    failures++;
  }
  if (sum != 42) {
    fprintf(stderr, "FAIL: expected SUM(b)=42, got %lld\n", (long long)sum);
    failures++;
  }

  if (failures == 0) {
    Int64 exp[] = {1, 42};
    if (verifySqlScalars("SELECT COUNT(*), SUM(b) FROM jagg_test",
                          exp, 2) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 13 — single row, COUNT(*)=1, SUM(b)=42\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 14: Negative values — COUNT, SUM, MAX, MIN                     */
/* ------------------------------------------------------------------ */

static int
testNegativeValues(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 14: SELECT COUNT(*), MAX(b), MIN(b) FROM %s (negative values)\n",
    TABLE_NAME);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  /* COUNT(*), SUM(b) and also COUNT(*), MAX(b), MIN(b) — use CountMaxMin */
  auto aggProg = buildAggProgram_CountMaxMin(meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  Uint64 count = 0;
  Int64 maxVal = INT64_MIN;
  Int64 minVal = INT64_MAX;
  bool first = true;
  for (const auto &res : allResults) {
    if (res.groups.empty()) continue;
    const auto &val = res.groups[0].second;
    Uint64 nodeCount = extractCountBigint(val, 0);
    Int64 nodeMax = extractSumBigint(val, 1);
    Int64 nodeMin = extractSumBigint(val, 2);
    count += nodeCount;
    if (nodeCount > 0) {
      if (first) {
        maxVal = nodeMax; minVal = nodeMin; first = false;
      } else {
        if (nodeMax > maxVal) maxVal = nodeMax;
        if (nodeMin < minVal) minVal = nodeMin;
      }
    }
  }

  V("  COUNT(*) = %llu\n", (unsigned long long)count);
  V("  MAX(b) = %lld\n", (long long)maxVal);
  V("  MIN(b) = %lld\n", (long long)minVal);

  /* Data: (1,-100), (2,50), (3,-200), (4,300), (5,-50) */
  int failures = 0;
  if (count != 5) {
    fprintf(stderr, "FAIL: expected COUNT(*)=5, got %llu\n",
            (unsigned long long)count);
    failures++;
  }
  if (maxVal != 300) {
    fprintf(stderr, "FAIL: expected MAX(b)=300, got %lld\n",
            (long long)maxVal);
    failures++;
  }
  if (minVal != -200) {
    fprintf(stderr, "FAIL: expected MIN(b)=-200, got %lld\n",
            (long long)minVal);
    failures++;
  }

  if (failures == 0) {
    Int64 exp[] = {5, 300, -200};
    if (verifySqlScalars("SELECT COUNT(*), MAX(b), MIN(b) FROM jagg_test",
                          exp, 3) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 14 — negative values, COUNT=5 MAX=300 MIN=-200\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 15: MUTEX_FREE with multiple rows per group                    */
/* ------------------------------------------------------------------ */

static int
testMutexFreeMultiRowGroup(Ndb * /*ndb*/, SignalSender &ss,
                            const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 15: SUM(val) GROUP BY grp, MUTEX_FREE\n");
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdB, meta.attrIdC);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_FREE, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (totalGroups != 3) {
    fprintf(stderr, "FAIL: expected 3 groups, got %u\n", totalGroups);
    return -1;
  }

  std::map<Int64, Int64> expected;
  expected[1] = 30;
  expected[2] = 120;
  expected[3] = 60;

  std::map<Int64, Int64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 sum = extractSumBigint(grp.second, 0);
      actual[key] += sum;
      V("  group(%lld) += SUM %lld\n", (long long)key, (long long)sum);
    }
  }

  int failures = 0;
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
    if (verifySqlGroupByMap("SELECT grp, SUM(val) FROM jagg_test3 GROUP BY grp",
                            expected) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 15 — MUTEX_FREE multi-row group, 3 groups correct\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 16: COUNT merge in MUTEX_FREE                                  */
/* ------------------------------------------------------------------ */

static int
testCountMergeMutexFree(Ndb * /*ndb*/, SignalSender &ss,
                         const TableMeta &meta, Uint32 numRows)
{
  V("\n=============================================\n");
  V("Test 16: COUNT(*) GROUP BY a, MUTEX_FREE (%u rows)\n", numRows);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_CountGroupBy(meta.attrIdA, meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_FREE, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  if (totalGroups != numRows) {
    fprintf(stderr, "FAIL: expected %u groups, got %u\n", numRows, totalGroups);
    return -1;
  }

  /* Each key is unique, so COUNT should be 1 per group.
   * extractCountBigint at agg_idx=0. */
  std::map<Int64, Uint64> actual;
  for (const auto &res : allResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Uint64 cnt = extractCountBigint(grp.second, 0);
      actual[key] += cnt;
    }
  }

  int failures = 0;
  for (Uint32 i = 1; i <= numRows; i++) {
    Int64 key = (Int64)i;
    auto it = actual.find(key);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)key);
      failures++;
    } else if (it->second != 1) {
      fprintf(stderr, "FAIL: group(%lld) expected COUNT=1, got %llu\n",
              (long long)key, (unsigned long long)it->second);
      failures++;
    }
  }

  if (failures == 0) {
    std::map<Int64, Int64> expectedCount;
    for (Uint32 i = 1; i <= numRows; i++)
      expectedCount[(Int64)i] = 1;
    if (verifySqlGroupByMap("SELECT a, COUNT(*) FROM jagg_test GROUP BY a",
                            expectedCount) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 16 — COUNT merge MUTEX_FREE, %u groups all COUNT=1\n",
           numRows);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 17: Non-GROUP-BY with MUTEX_FREE                               */
/* ------------------------------------------------------------------ */

static int
testNoGroupByMutexFree(Ndb * /*ndb*/, SignalSender &ss,
                        const TableMeta &meta, Uint32 numRows)
{
  V("\n=============================================\n");
  V("Test 17: COUNT(*), SUM(b) no GROUP BY, MUTEX_FREE (%u rows)\n", numRows);
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_CountSum(meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_FREE, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  Uint64 count = 0;
  Int64 sum = 0;
  for (const auto &res : allResults) {
    if (res.groups.empty()) continue;
    const auto &val = res.groups[0].second;
    count += extractCountBigint(val, 0);
    sum += extractSumBigint(val, 1);
  }

  V("  COUNT(*) = %llu\n", (unsigned long long)count);
  V("  SUM(b) = %lld\n", (long long)sum);

  /* Data: i*10 for i=1..numRows → SUM = 10 * numRows*(numRows+1)/2 */
  Int64 expectedSum = (Int64)(10) * (Int64)numRows * (Int64)(numRows + 1) / 2;

  int failures = 0;
  if (count != numRows) {
    fprintf(stderr, "FAIL: expected COUNT(*)=%u, got %llu\n",
            numRows, (unsigned long long)count);
    failures++;
  }
  if (sum != expectedSum) {
    fprintf(stderr, "FAIL: expected SUM(b)=%lld, got %lld\n",
            (long long)expectedSum, (long long)sum);
    failures++;
  }

  if (failures == 0) {
    Int64 exp[] = {(Int64)numRows, expectedSum};
    if (verifySqlScalars("SELECT COUNT(*), SUM(b) FROM jagg_test",
                          exp, 2) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 17 — non-GROUP-BY MUTEX_FREE, COUNT=%llu SUM=%lld\n",
           (unsigned long long)count, (long long)sum);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 18: Eviction with MUTEX_FREE strategy                          */
/* ------------------------------------------------------------------ */

static int
testEvictionMutexFree(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta,
                       Uint32 numRows, NdbRestarter &restarter)
{
  V("\n=============================================\n");
  V("Test 18: Eviction MUTEX_FREE (%u rows, ERROR_INSERT 5090)\n", numRows);
  V("=============================================\n");

  if (restarter.insertErrorInAllNodes(5090) != 0) {
    fprintf(stderr, "FAIL: insertErrorInAllNodes(5090) failed\n");
    return -1;
  }
  V("ERROR_INSERT 5090 set in all nodes\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_FREE, key) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  std::vector<AggResult> evictedResults;
  Uint32 evictedGroups = 0;

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
    Uint32 rows = 0;
    if (waitForScanConfWithEviction(ss, f, rows,
                                    evictedResults, evictedGroups) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
  }

  V("Evicted %u groups during scan phase\n", evictedGroups);

  std::vector<AggResult> finalResults;
  Uint32 finalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
    if (receiveResults(ss, finalResults, finalGroups) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) {
      restarter.insertErrorInAllNodes(0);
      return -1;
    }
  }

  restarter.insertErrorInAllNodes(0);
  V("ERROR_INSERT cleared\n");

  V("\n--- Validation ---\n");
  V("Evicted groups: %u, finalized groups: %u\n", evictedGroups, finalGroups);

  std::map<Int64, Int64> actual;
  for (const auto &res : evictedResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 s = extractSumBigint(grp.second, 0);
      actual[key] += s;
    }
  }
  for (const auto &res : finalResults) {
    for (const auto &grp : res.groups) {
      Int64 key = extractGroupKey(grp.first);
      Int64 s = extractSumBigint(grp.second, 0);
      actual[key] += s;
    }
  }

  if (actual.size() != numRows) {
    fprintf(stderr, "FAIL: expected %u distinct groups, got %zu\n",
            numRows, actual.size());
    return -1;
  }

  int failures = 0;
  for (Uint32 i = 1; i <= numRows; i++) {
    Int64 key = (Int64)i;
    Int64 expectedSum = (Int64)(i * 10);
    auto it = actual.find(key);
    if (it == actual.end()) {
      fprintf(stderr, "FAIL: missing group(%lld)\n", (long long)key);
      failures++;
    } else if (it->second != expectedSum) {
      fprintf(stderr, "FAIL: group(%lld) expected SUM=%lld, got %lld\n",
              (long long)key, (long long)expectedSum, (long long)it->second);
      failures++;
    }
  }

  if (evictedGroups == 0) {
    fprintf(stderr, "FAIL: expected evictions but got 0 evicted groups\n");
    failures++;
  }

  if (failures == 0) {
    if (verifySqlGroupByMap("SELECT a, SUM(b) FROM jagg_test GROUP BY a",
                            actual) != 0)
      failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 18 — eviction MUTEX_FREE, %u groups correct "
           "(%u evicted + %u finalized)\n",
           (Uint32)actual.size(), evictedGroups, finalGroups);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 19: COMPLETE_REF error — invalid aggStateKey                   */
/* ------------------------------------------------------------------ */

static int
testCompleteRefError(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 19: COMPLETE_REF with invalid aggStateKey\n");
  V("=============================================\n");

  /* Send COMPLETE_REQ with a bogus aggStateKey to any data node */
  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  Uint32 nodeId = *uniqueNodes.begin();

  const Uint32 INVALID_AGG_STATE_KEY = 0xFFFF;

  V("  Sending COMPLETE_REQ with invalid aggStateKey=%u to node %u\n",
    INVALID_AGG_STATE_KEY, nodeId);

  SimpleSignal ssig;
  JoinAggCompleteReq *req =
    reinterpret_cast<JoinAggCompleteReq *>(ssig.getDataPtrSend());

  req->senderRef = ss.getOwnRef();
  req->senderData = FAKE_SENDER_DATA;
  req->requestId = FAKE_REQUEST_ID;
  req->transid[0] = FAKE_TRANS_ID1;
  req->transid[1] = FAKE_TRANS_ID2;
  req->aggStateKey = INVALID_AGG_STATE_KEY;
  req->maxBatchRows = 100;

  Uint16 recBlock = numberToBlock(DBLQH, 1);
  ssig.set(ss, 0, recBlock, GSN_JOIN_AGG_COMPLETE_REQ,
           JoinAggCompleteReq::SignalLength);

  if (ss.sendSignal(nodeId, &ssig) != SEND_OK) {
    fprintf(stderr, "sendSignal COMPLETE_REQ failed\n");
    return -1;
  }

  SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "COMPLETE_REF");
  if (resp == nullptr) return -1;

  int gsn = getGsn(resp);
  int failures = 0;

  if (gsn == GSN_JOIN_AGG_COMPLETE_REF) {
    const JoinAggCompleteRef *ref =
      reinterpret_cast<const JoinAggCompleteRef *>(resp->getDataPtr());
    V("  COMPLETE_REF: errorCode=%u errorLine=%u\n",
      ref->errorCode, ref->errorLine);
    /* Error 1251 = ZJOIN_AGG_STATE_NOT_FOUND */
    if (ref->errorCode != 1251) {
      fprintf(stderr, "FAIL: expected errorCode=1251, got %u\n",
              ref->errorCode);
      failures++;
    }
  } else {
    fprintf(stderr, "FAIL: expected COMPLETE_REF (GSN=%d), got GSN %d\n",
            GSN_JOIN_AGG_COMPLETE_REF, gsn);
    failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 19 — COMPLETE_REF with errorCode=1251\n");
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 20: rowsExamined validation                                    */
/* ------------------------------------------------------------------ */

static int
testRowsExamined(Ndb * /*ndb*/, SignalSender &ss, const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 20: rowsExamined validation (5 rows)\n");
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_CountSum(meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  Uint32 totalRowsExamined = 0;

  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;

    /* Inline waitForScanConf to also capture rowsExamined */
    SimpleSignal *resp = waitForSignal(ss, WAIT_TIMEOUT_MS, "SCAN_FRAGCONF");
    if (resp == nullptr) return -1;
    int gsn = getGsn(resp);
    if (gsn == GSN_SCAN_FRAGCONF) {
      const ScanFragConf *conf =
        reinterpret_cast<const ScanFragConf *>(resp->getDataPtr());
      Uint32 sigLen = resp->header.theLength;
      Uint32 rowsExamined =
        (sigLen >= ScanFragConf::SignalLength_v2) ? conf->rowsExamined : 0;
      totalRowsExamined += rowsExamined;
      V("  frag %u: completedOps=%u rowsExamined=%u\n",
        conf->senderData, conf->completedOps, rowsExamined);
    } else if (gsn == GSN_SCAN_FRAGREF) {
      const Uint32 *d = resp->getDataPtr();
      fprintf(stderr, "  SCAN_FRAGREF: errorCode=%u\n", d[3]);
      return -1;
    } else {
      fprintf(stderr, "Unexpected GSN %d\n", gsn);
      return -1;
    }
  }

  /* Complete + release */
  std::vector<AggResult> allResults;
  Uint32 totalGroups = 0;
  for (Uint32 nd : uniqueNodes) {
    if (sendCompleteReq(ss, nd, aggStateKeys[nd], 1000) != 0) return -1;
    if (receiveResults(ss, allResults, totalGroups) != 0) return -1;
  }
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0) return -1;
  }

  V("\n--- Validation ---\n");
  V("  totalRowsExamined = %u\n", totalRowsExamined);

  int failures = 0;
  if (totalRowsExamined != 5) {
    fprintf(stderr, "FAIL: expected rowsExamined=5, got %u\n",
            totalRowsExamined);
    failures++;
  }

  if (failures == 0) {
    printf("PASS: Test 20 — rowsExamined=%u (expected 5)\n",
           totalRowsExamined);
  }
  return failures > 0 ? -1 : 0;
}

/* ------------------------------------------------------------------ */
/* Test 21: Release without COMPLETE                                   */
/* ------------------------------------------------------------------ */

static int
testReleaseWithoutComplete(Ndb * /*ndb*/, SignalSender &ss,
                            const TableMeta &meta)
{
  V("\n=============================================\n");
  V("Test 21: SETUP -> SCAN -> RELEASE (no COMPLETE)\n");
  V("=============================================\n");

  std::set<Uint32> uniqueNodes(meta.fragNodes.begin(), meta.fragNodes.end());
  auto aggProg = buildAggProgram_SumGroupBy(meta.attrIdA, meta.attrIdB);

  std::map<Uint32, Uint32> aggStateKeys;
  for (Uint32 nd : uniqueNodes) {
    Uint32 key = 0;
    if (sendSetupReq(ss, nd, aggProg, meta,
                     JoinAggSetupReq::STRATEGY_MUTEX_BASED, key) != 0)
      return -1;
    aggStateKeys[nd] = key;
  }

  auto attrInfo = buildAttrInfo();
  for (Uint32 f = 0; f < meta.fragCount; f++) {
    if (sendScanFragReq(ss, meta.fragNodes[f], f, meta.fragInstances[f],
                        aggStateKeys[meta.fragNodes[f]], meta, attrInfo) != 0)
      return -1;
    Uint32 rows = 0;
    if (waitForScanConf(ss, f, rows) != 0) return -1;
  }

  /* Skip COMPLETE — go directly to RELEASE */
  V("  Skipping COMPLETE, sending RELEASE directly\n");
  for (Uint32 nd : uniqueNodes) {
    if (sendReleaseReq(ss, nd, aggStateKeys[nd]) != 0)
      return -1;
  }

  printf("PASS: Test 21 — release without COMPLETE, no crash\n");
  return 0;
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

    NdbRestarter restarter(connectString);

    /* Phase 1: 5-row tests (Tests 1, 2, 5, 6, 7, 8) */
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

      if (testSumGroupBy(&ndb, ss, meta) != 0) result = 1;
      if (testCountSumNoGroupBy(&ndb, ss, meta) != 0) result = 1;
      if (testLqhKeyReq(&ndb, ss, meta, restarter) != 0) result = 1;
      if (testMaxGroupBy(&ndb, ss, meta) != 0) result = 1;
      if (testMinGroupBy(&ndb, ss, meta) != 0) result = 1;
      if (testMaxMinNoGroupBy(&ndb, ss, meta) != 0) result = 1;
      if (testCompleteRefError(&ndb, ss, meta) != 0) result = 1;
      if (testRowsExamined(&ndb, ss, meta) != 0) result = 1;
      if (testReleaseWithoutComplete(&ndb, ss, meta) != 0) result = 1;

      ss.unlock();
    }

    /* Phase 2: 200-row tests (Tests 3, 4, 12, 16, 17, 18) */
    dropTestTable(conn);

    const Uint32 MANY_ROWS = 200;
    if (createTestTable(conn, &ndb, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (insertManyRows(&ndb, MANY_ROWS) != 0) {
      mysql_close(conn); result = 1; break;
    }

    {
      SignalSender ss(&con);
      ss.lock();

      if (testHighCardinalityGroupBy(&ndb, ss, meta, MANY_ROWS) != 0)
        result = 1;
      if (testEviction(&ndb, ss, meta, MANY_ROWS, restarter) != 0)
        result = 1;
      if (testFlowControl(&ndb, ss, meta, MANY_ROWS) != 0)
        result = 1;
      if (testCountMergeMutexFree(&ndb, ss, meta, MANY_ROWS) != 0)
        result = 1;
      if (testNoGroupByMutexFree(&ndb, ss, meta, MANY_ROWS) != 0)
        result = 1;
      if (testEvictionMutexFree(&ndb, ss, meta, MANY_ROWS, restarter) != 0)
        result = 1;

      ss.unlock();
    }

    dropTestTable(conn);

    /* Phase 3: Empty table test (Test 9) */
    if (createTestTable(conn, &ndb, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    /* No data inserted — table is empty */

    {
      SignalSender ss(&con);
      ss.lock();
      if (testEmptyTable(&ndb, ss, meta) != 0) result = 1;
      ss.unlock();
    }

    dropTestTable(conn);

    /* Phase 4: 3-column table tests (Tests 10, 11, 15) */
    if (createTestTable3Col(conn, &ndb, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (insertMultiRowData(&ndb) != 0) {
      mysql_close(conn); result = 1; break;
    }

    {
      SignalSender ss(&con);
      ss.lock();
      if (testMultiRowPerGroup(&ndb, ss, meta) != 0) result = 1;
      if (testAllAggsGroupBy(&ndb, ss, meta) != 0) result = 1;
      if (testMutexFreeMultiRowGroup(&ndb, ss, meta) != 0) result = 1;
      ss.unlock();
    }

    dropTestTable3Col(conn);

    /* Phase 5: Single row test (Test 13) */
    if (createTestTable(conn, &ndb, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    {
      /* Insert a single row: (1, 42) */
      const NdbDictionary::Table *ptab =
        ndb.getDictionary()->getTable(TABLE_NAME);
      NdbTransaction *trans = ndb.startTransaction();
      NdbOperation *op = trans->getNdbOperation(ptab);
      op->insertTuple();
      Int64 a = 1, b = 42;
      op->equal("a", a);
      op->setValue("b", b);
      trans->execute(NdbTransaction::Commit);
      trans->close();
      V("Inserted 1 row (1, 42) into %s\n", TABLE_NAME);
    }

    {
      SignalSender ss(&con);
      ss.lock();
      if (testSingleRow(&ndb, ss, meta) != 0) result = 1;
      ss.unlock();
    }

    dropTestTable(conn);

    /* Phase 6: Negative values test (Test 14) */
    if (createTestTable(conn, &ndb, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (queryFragInstances(mysqlPort, meta) != 0) {
      mysql_close(conn); result = 1; break;
    }
    if (insertNegativeData(&ndb) != 0) {
      mysql_close(conn); result = 1; break;
    }

    {
      SignalSender ss(&con);
      ss.lock();
      if (testNegativeValues(&ndb, ss, meta) != 0) result = 1;
      ss.unlock();
    }

    dropTestTable(conn);
    mysql_close(conn);
  } while (0);

  ndb_end(0);

  if (result == 0) {
    printf("\n*** ALL TESTS PASSED ***\n");
  } else {
    printf("\n*** SOME TESTS FAILED ***\n");
  }

  return result;
}
