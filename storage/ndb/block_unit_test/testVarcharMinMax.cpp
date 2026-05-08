/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * testVarcharMinMax — Phase I.6 F.2 / F.3 isolated end-to-end test for
 * MIN / MAX over VARCHAR / CHAR / Longvarchar columns.
 *
 * Exercises the kernel string MIN/MAX path (per-(group, slot) val_ptr
 * state, F.2-K.4) + the AGG_CHAR_RESULT wire format (F.2-K.5) +
 * NdbAggregator API parse + Result::data_str (F.2-K.5d-1, K.5d-3).
 *
 * Single-table scalar and GROUP BY aggregation only — no joins and no
 * CTE.  These are the simplest paths that touch the new wire format
 * end-to-end.  Join and CTE_LOOKUP-fed string MIN/MAX have wider
 * surfaces (linked-attr string format, CTE delivery substitution)
 * tracked by follow-up coverage.
 *
 * Schema (created by this test):
 *   vctest_t (
 *     id INT NOT NULL PRIMARY KEY,
 *     grp INT NOT NULL,
 *     vname VARCHAR(20) NOT NULL,
 *     cname CHAR(8) NOT NULL,
 *     lname VARCHAR(300) NOT NULL
 *   ) ENGINE=NDB
 *
 * Test rows: 'Alice'/'A1', 'Bob'/'B2', 'Charlie'/'C3', 'Dave'/'D4',
 * 'Eve'/'E5' plus 260-byte Longvarchar payloads.
 * MIN(vname)='Alice', MAX(vname)='Eve';
 * MIN(cname)='A1', MAX(cname)='E5';
 * MIN(lname)=260 x 'a', MAX(lname)=260 x 'z'.
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbAggregator.hpp>
#include <ndbapi/NdbAggregationCommon.hpp>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryOperation.hpp"

#include <mysql.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

static MYSQL *connectMysql(int mysqlPort) {
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

static int runQuery(MYSQL *conn, const char *sql) {
  if (mysql_query(conn, sql) != 0) {
    fprintf(stderr, "SQL failed (%s): %s\n", sql, mysql_error(conn));
    return -1;
  }
  return 0;
}

static int setupSchema(MYSQL *conn) {
  if (runQuery(conn, "DROP TABLE IF EXISTS vctest_virt") != 0) return -1;
  if (runQuery(conn, "DROP TABLE IF EXISTS vctest_groups") != 0) return -1;
  if (runQuery(conn, "DROP TABLE IF EXISTS vctest_t") != 0) return -1;
  if (runQuery(conn,
        "CREATE TABLE vctest_t ("
        "  id INT NOT NULL PRIMARY KEY,"
        "  grp INT NOT NULL,"
        "  vname VARCHAR(20) NOT NULL,"
        "  cname CHAR(8) NOT NULL,"
        "  lname VARCHAR(300) NOT NULL"
        ") ENGINE=NDB") != 0) return -1;
  if (runQuery(conn,
        "CREATE TABLE vctest_groups ("
        "  grp INT NOT NULL PRIMARY KEY"
        ") ENGINE=NDB") != 0) return -1;
  if (runQuery(conn,
        "CREATE TABLE vctest_virt ("
        "  grp INT NOT NULL PRIMARY KEY,"
        "  min_v VARCHAR(20),"
        "  max_v VARCHAR(20)"
        ") ENGINE=NDB") != 0) return -1;
  if (runQuery(conn,
        "INSERT INTO vctest_t VALUES "
        "  (1, 1, 'Alice',   'A1', REPEAT('z', 260)),"
        "  (2, 1, 'Bob',     'B2', REPEAT('a', 260)),"
        "  (3, 2, 'Charlie', 'C3', REPEAT('m', 260)),"
        "  (4, 2, 'Dave',    'D4', REPEAT('n', 260)),"
        "  (5, 2, 'Eve',     'E5', REPEAT('e', 260))") != 0) return -1;
  if (runQuery(conn,
        "INSERT INTO vctest_groups VALUES (1), (2)") != 0) return -1;
  return 0;
}

static void dropSchema(MYSQL *conn) {
  runQuery(conn, "DROP TABLE IF EXISTS vctest_virt");
  runQuery(conn, "DROP TABLE IF EXISTS vctest_groups");
  runQuery(conn, "DROP TABLE IF EXISTS vctest_t");
}

/*
 * Compare a Result::data_str payload to the expected bytes, ignoring
 * any CHAR space-padding past the expected length.  Returns 0 on
 * match, -1 on mismatch (and prints diagnostics).
 */
static int verifyString(const char *label, NdbAggregator::Result &result,
                        const char *expected) {
  if (result.is_null()) {
    fprintf(stderr, "FAIL %s: result is NULL, expected '%s'\n",
            label, expected);
    return -1;
  }
  Uint32 plen = 0;
  const char *p = result.data_str(&plen);
  Uint32 elen = (Uint32)strlen(expected);
  // CHAR may be space-padded to declared size; match the prefix.
  Uint32 effective = plen;
  while (effective > elen && p[effective - 1] == ' ') effective--;
  if (effective != elen || memcmp(p, expected, elen) != 0) {
    fprintf(stderr, "FAIL %s: got '%.*s' (len=%u), expected '%s'\n",
            label, (int)plen, p, plen, expected);
    return -1;
  }
  V("  OK  %s = '%s' (len=%u)\n", label, expected, plen);
  return 0;
}

static void fillExpectedLong(char *buf, char ch) {
  memset(buf, ch, 260);
  buf[260] = 0;
}

static int verifyVarcharRecAttr(const char *label, const NdbRecAttr *attr,
                                const char *expected) {
  if (attr == nullptr) {
    fprintf(stderr, "FAIL %s: missing NdbRecAttr\n", label);
    return -1;
  }
  if (attr->isNULL()) {
    fprintf(stderr, "FAIL %s: value is NULL, expected '%s'\n",
            label, expected);
    return -1;
  }
  const unsigned char *raw =
      reinterpret_cast<const unsigned char *>(attr->aRef());
  const Uint32 len = raw[0];
  const char *payload = reinterpret_cast<const char *>(raw + 1);
  const Uint32 expected_len = (Uint32)strlen(expected);
  if (len != expected_len || memcmp(payload, expected, expected_len) != 0) {
    fprintf(stderr, "FAIL %s: got '%.*s' (len=%u), expected '%s'\n",
            label, (int)len, payload, len, expected);
    return -1;
  }
  V("  OK  %s = '%s'\n", label, expected);
  return 0;
}

static int buildStringCte(NdbQueryBuilder *qb,
                          const NdbDictionary::Table *srcTab,
                          NdbAggregator &cteAgg) {
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScan = qb->scanTable(srcTab);
  if (cteScan == nullptr) {
    fprintf(stderr, "CTE scan build failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(cteScan, "id"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  if (qb->readTuple(srcTab, cteKey, &cteLeafOpts) == nullptr) {
    fprintf(stderr, "CTE leaf build failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, cteAgg) != 0) {
    fprintf(stderr, "defineCte failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  return 0;
}

static int runScalarTest(Ndb *ndb) {
  V("\n=== Test: scalar MIN/MAX over VARCHAR + CHAR ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  const NdbDictionary::Table *tab = dict->getTable("vctest_t");
  if (tab == nullptr) {
    fprintf(stderr, "Cannot find vctest_t: %s\n", dict->getNdbError().message);
    return -1;
  }
  const NdbDictionary::Column *vnameCol = tab->getColumn("vname");
  const NdbDictionary::Column *cnameCol = tab->getColumn("cname");
  const NdbDictionary::Column *lnameCol = tab->getColumn("lname");
  if (vnameCol == nullptr || cnameCol == nullptr || lnameCol == nullptr) {
    fprintf(stderr, "Column lookup failed\n");
    return -1;
  }
  if (lnameCol->getType() != NdbDictionary::Column::Longvarchar) {
    fprintf(stderr, "FAIL scalar: lname is type %d, expected Longvarchar\n",
            (int)lnameCol->getType());
    return -1;
  }

  /*
   * Build aggregation program: no GROUP BY, four aggregates over two
   * string columns.
   *   reg0 = vname (VARCHAR)
   *   reg1 = cname (CHAR)
   *   agg[0] = MIN(vname)   -> 'Alice'
   *   agg[1] = MAX(vname)   -> 'Eve'
   *   agg[2] = MIN(cname)   -> 'A1'
   *   agg[3] = MAX(cname)   -> 'E5'
   *   agg[4] = MIN(lname)   -> 260 x 'a'
   *   agg[5] = MAX(lname)   -> 260 x 'z'
   */
  NdbAggregator agg(tab);
  if (!agg.LoadColumn(vnameCol->getAttrId(), 0) ||
      !agg.Min(0, 0) ||
      !agg.Max(1, 0) ||
      !agg.LoadColumn(cnameCol->getAttrId(), 1) ||
      !agg.Min(2, 1) ||
      !agg.Max(3, 1) ||
      !agg.LoadColumn(lnameCol->getAttrId(), 2) ||
      !agg.Min(4, 2) ||
      !agg.Max(5, 2) ||
      !agg.Finalize()) {
    fprintf(stderr, "Aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }
  V("  Aggregation program: %u words\n", agg.instructions_length());

  /*
   * Single-table scan + setAggregationCode + DoAggregation — the
   * legacy NdbScanOperation aggregation surface.  NdbQueryBuilder's
   * scanTable+setAggregation requires a non-root aggregate leaf
   * (i.e. multi-table query), which is overkill for an isolated
   * F.2 wire-path test.
   */
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == nullptr) {
    fprintf(stderr, "getNdbScanOperation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    fprintf(stderr, "readTuples failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->setAggregationCode(&agg) != 0) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->DoAggregation() != 0) {
    fprintf(stderr, "DoAggregation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }

  int failures = 0;
  NdbAggregator::ResultRecord rec = agg.FetchResultRecord();
  if (rec.end()) {
    fprintf(stderr, "No aggregation result returned\n");
    failures = 1;
  } else {
    NdbAggregator::Result minV = rec.FetchAggregationResult();
    NdbAggregator::Result maxV = rec.FetchAggregationResult();
    NdbAggregator::Result minC = rec.FetchAggregationResult();
    NdbAggregator::Result maxC = rec.FetchAggregationResult();
    NdbAggregator::Result minL = rec.FetchAggregationResult();
    NdbAggregator::Result maxL = rec.FetchAggregationResult();
    char expectedA[261];
    char expectedZ[261];
    fillExpectedLong(expectedA, 'a');
    fillExpectedLong(expectedZ, 'z');
    if (verifyString("MIN(vname)", minV, "Alice") != 0) failures++;
    if (verifyString("MAX(vname)", maxV, "Eve")   != 0) failures++;
    if (verifyString("MIN(cname)", minC, "A1")    != 0) failures++;
    if (verifyString("MAX(cname)", maxC, "E5")    != 0) failures++;
    if (verifyString("MIN(lname)", minL, expectedA) != 0) failures++;
    if (verifyString("MAX(lname)", maxL, expectedZ) != 0) failures++;
    NdbAggregator::Result end = rec.FetchAggregationResult();
    if (!end.end()) {
      fprintf(stderr, "FAIL scalar: expected end after 6 aggregate slots\n");
      failures++;
    }
  }

  trans->close();
  return failures == 0 ? 0 : -1;
}

static int runGroupByTest(Ndb *ndb) {
  V("\n=== Test: GROUP BY MIN/MAX over VARCHAR + CHAR ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  const NdbDictionary::Table *tab = dict->getTable("vctest_t");
  if (tab == nullptr) {
    fprintf(stderr, "Cannot find vctest_t: %s\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator agg(tab);
  if (!agg.GroupBy("grp") ||
      !agg.LoadColumn("vname", 0) ||
      !agg.Min(0, 0) ||
      !agg.Max(1, 0) ||
      !agg.LoadColumn("cname", 1) ||
      !agg.Min(2, 1) ||
      !agg.Max(3, 1) ||
      !agg.Finalize()) {
    fprintf(stderr, "GROUP BY aggregation program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == nullptr) {
    fprintf(stderr, "getNdbScanOperation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    fprintf(stderr, "readTuples failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->setAggregationCode(&agg) != 0) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->DoAggregation() != 0) {
    fprintf(stderr, "DoAggregation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }

  int failures = 0;
  bool seenGrp1 = false;
  bool seenGrp2 = false;
  while (true) {
    NdbAggregator::ResultRecord rec = agg.FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grp = rec.FetchGroupbyColumn();
    if (grp.is_null()) {
      fprintf(stderr, "FAIL group: GROUP BY column is NULL\n");
      failures++;
      continue;
    }
    Int32 groupNo = grp.data_int32();

    NdbAggregator::Result minV = rec.FetchAggregationResult();
    NdbAggregator::Result maxV = rec.FetchAggregationResult();
    NdbAggregator::Result minC = rec.FetchAggregationResult();
    NdbAggregator::Result maxC = rec.FetchAggregationResult();
    NdbAggregator::Result end = rec.FetchAggregationResult();
    if (!end.end()) {
      fprintf(stderr, "FAIL group %d: expected end after 4 aggregate slots\n",
              groupNo);
      failures++;
    }

    if (groupNo == 1) {
      seenGrp1 = true;
      if (verifyString("grp1 MIN(vname)", minV, "Alice") != 0) failures++;
      if (verifyString("grp1 MAX(vname)", maxV, "Bob")   != 0) failures++;
      if (verifyString("grp1 MIN(cname)", minC, "A1")    != 0) failures++;
      if (verifyString("grp1 MAX(cname)", maxC, "B2")    != 0) failures++;
    } else if (groupNo == 2) {
      seenGrp2 = true;
      if (verifyString("grp2 MIN(vname)", minV, "Charlie") != 0) failures++;
      if (verifyString("grp2 MAX(vname)", maxV, "Eve")     != 0) failures++;
      if (verifyString("grp2 MIN(cname)", minC, "C3")      != 0) failures++;
      if (verifyString("grp2 MAX(cname)", maxC, "E5")      != 0) failures++;
    } else {
      fprintf(stderr, "FAIL group: unexpected group %d\n", groupNo);
      failures++;
    }
  }

  if (!seenGrp1 || !seenGrp2) {
    fprintf(stderr, "FAIL group: missing expected groups, seen 1=%d 2=%d\n",
            seenGrp1, seenGrp2);
    failures++;
  }

  trans->close();
  return failures == 0 ? 0 : -1;
}

static int runLongvarcharGroupByTest(Ndb *ndb) {
  V("\n=== Test: GROUP BY MIN/MAX over Longvarchar ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  const NdbDictionary::Table *tab = dict->getTable("vctest_t");
  if (tab == nullptr) {
    fprintf(stderr, "Cannot find vctest_t: %s\n", dict->getNdbError().message);
    return -1;
  }

  NdbAggregator agg(tab);
  if (!agg.GroupBy("grp") ||
      !agg.LoadColumn("lname", 0) ||
      !agg.Min(0, 0) ||
      !agg.Max(1, 0) ||
      !agg.Finalize()) {
    fprintf(stderr, "Longvarchar GROUP BY program build failed: %s\n",
            agg.GetError().err_msg_);
    return -1;
  }

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    return -1;
  }
  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == nullptr) {
    fprintf(stderr, "getNdbScanOperation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    fprintf(stderr, "readTuples failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->setAggregationCode(&agg) != 0) {
    fprintf(stderr, "setAggregationCode failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }
  if (scan->DoAggregation() != 0) {
    fprintf(stderr, "DoAggregation failed: %s\n",
            trans->getNdbError().message);
    trans->close();
    return -1;
  }

  char expectedA[261];
  char expectedE[261];
  char expectedN[261];
  char expectedZ[261];
  fillExpectedLong(expectedA, 'a');
  fillExpectedLong(expectedE, 'e');
  fillExpectedLong(expectedN, 'n');
  fillExpectedLong(expectedZ, 'z');

  int failures = 0;
  bool seenGrp1 = false;
  bool seenGrp2 = false;
  while (true) {
    NdbAggregator::ResultRecord rec = agg.FetchResultRecord();
    if (rec.end()) break;

    NdbAggregator::Column grp = rec.FetchGroupbyColumn();
    if (grp.is_null()) {
      fprintf(stderr, "FAIL long group: GROUP BY column is NULL\n");
      failures++;
      continue;
    }
    Int32 groupNo = grp.data_int32();

    NdbAggregator::Result minL = rec.FetchAggregationResult();
    NdbAggregator::Result maxL = rec.FetchAggregationResult();
    NdbAggregator::Result end = rec.FetchAggregationResult();
    if (!end.end()) {
      fprintf(stderr,
              "FAIL long group %d: expected end after 2 aggregate slots\n",
              groupNo);
      failures++;
    }

    if (groupNo == 1) {
      seenGrp1 = true;
      if (verifyString("grp1 MIN(lname)", minL, expectedA) != 0) failures++;
      if (verifyString("grp1 MAX(lname)", maxL, expectedZ) != 0) failures++;
    } else if (groupNo == 2) {
      seenGrp2 = true;
      if (verifyString("grp2 MIN(lname)", minL, expectedE) != 0) failures++;
      if (verifyString("grp2 MAX(lname)", maxL, expectedN) != 0) failures++;
    } else {
      fprintf(stderr, "FAIL long group: unexpected group %d\n", groupNo);
      failures++;
    }
  }

  if (!seenGrp1 || !seenGrp2) {
    fprintf(stderr,
            "FAIL long group: missing expected groups, seen 1=%d 2=%d\n",
            seenGrp1, seenGrp2);
    failures++;
  }

  trans->close();
  return failures == 0 ? 0 : -1;
}

static int runCteScanStringDeliveryTest(Ndb *ndb) {
  V("\n=== Test: CTE_SCAN delivery of string MIN/MAX ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  dict->invalidateTable("vctest_virt");
  const NdbDictionary::Table *srcTab = dict->getTable("vctest_t");
  const NdbDictionary::Table *virtTab = dict->getTable("vctest_virt");
  if (srcTab == nullptr || virtTab == nullptr) {
    fprintf(stderr, "Cannot find CTE string test tables: %s\n",
            dict->getNdbError().message);
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("vname", 0) ||
      !cteAgg.Min(0, 0) ||
      !cteAgg.Max(1, 0) ||
      !cteAgg.Finalize()) {
    fprintf(stderr, "CTE string aggregation build failed: %s\n",
            cteAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    fprintf(stderr, "NdbQueryBuilder::create failed\n");
    return -1;
  }
  if (buildStringCte(qb, srcTab, cteAgg) != 0) return -1;
  if (qb->scanCte(0, 3, virtTab) == nullptr) {
    fprintf(stderr, "scanCte build failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    fprintf(stderr, "prepare failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    queryDef->destroy();
    return -1;
  }
  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    fprintf(stderr, "createQuery failed: %s\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *grpAttr = nullptr;
  NdbRecAttr *minAttr = nullptr;
  NdbRecAttr *maxAttr = nullptr;
  if (mainOp != nullptr) {
    grpAttr = mainOp->getValue("grp");
    minAttr = mainOp->getValue("min_v");
    maxAttr = mainOp->getValue("max_v");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    fprintf(stderr, "execute failed: %s\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  bool seenGrp1 = false;
  bool seenGrp2 = false;
  int failures = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    if (grpAttr == nullptr || grpAttr->isNULL()) {
      fprintf(stderr, "FAIL CTE_SCAN: missing grp\n");
      failures++;
      continue;
    }
    const Int32 grp = grpAttr->int32_value();
    if (grp == 1) {
      seenGrp1 = true;
      if (verifyVarcharRecAttr("cte grp1 MIN(vname)", minAttr,
                               "Alice") != 0) failures++;
      if (verifyVarcharRecAttr("cte grp1 MAX(vname)", maxAttr,
                               "Bob") != 0) failures++;
    } else if (grp == 2) {
      seenGrp2 = true;
      if (verifyVarcharRecAttr("cte grp2 MIN(vname)", minAttr,
                               "Charlie") != 0) failures++;
      if (verifyVarcharRecAttr("cte grp2 MAX(vname)", maxAttr,
                               "Eve") != 0) failures++;
    } else {
      fprintf(stderr, "FAIL CTE_SCAN: unexpected group %d\n", grp);
      failures++;
    }
  }
  if (outcome == NdbQuery::NextResult_error) {
    fprintf(stderr, "nextResult failed: %s\n", query->getNdbError().message);
    failures++;
  }
  if (!seenGrp1 || !seenGrp2) {
    fprintf(stderr, "FAIL CTE_SCAN: missing groups, seen 1=%d 2=%d\n",
            seenGrp1, seenGrp2);
    failures++;
  }

  query->close();
  trans->close();
  queryDef->destroy();
  return failures == 0 ? 0 : -1;
}

static int runCteLookupStringAggFeedTest(Ndb *ndb) {
  V("\n=== Test: CTE_LOOKUP feeds string MIN/MAX aggregation ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  dict->invalidateTable("vctest_groups");
  dict->invalidateTable("vctest_virt");
  const NdbDictionary::Table *srcTab = dict->getTable("vctest_t");
  const NdbDictionary::Table *groupTab = dict->getTable("vctest_groups");
  const NdbDictionary::Table *virtTab = dict->getTable("vctest_virt");
  if (srcTab == nullptr || groupTab == nullptr || virtTab == nullptr) {
    fprintf(stderr, "Cannot find CTE lookup string test tables: %s\n",
            dict->getNdbError().message);
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("vname", 0) ||
      !cteAgg.Min(0, 0) ||
      !cteAgg.Max(1, 0) ||
      !cteAgg.Finalize()) {
    fprintf(stderr, "CTE string aggregation build failed: %s\n",
            cteAgg.GetError().err_msg_);
    return -1;
  }

  const NdbDictionary::Column *minCol = virtTab->getColumn("min_v");
  const NdbDictionary::Column *maxCol = virtTab->getColumn("max_v");
  if (minCol == nullptr || maxCol == nullptr) {
    fprintf(stderr, "Virtual string column lookup failed\n");
    return -1;
  }

  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, minCol) ||
      !mainAgg.Min(0, 0) ||
      !mainAgg.LoadLinkedColumn(2, 1, maxCol) ||
      !mainAgg.Max(1, 1) ||
      !mainAgg.Finalize()) {
    fprintf(stderr, "Main string aggregation build failed: %s\n",
            mainAgg.GetError().err_msg_);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) {
    fprintf(stderr, "NdbQueryBuilder::create failed\n");
    return -1;
  }
  if (buildStringCte(qb, srcTab, cteAgg) != 0) return -1;

  const NdbQueryTableScanOperationDef *mainScan = qb->scanTable(groupTab);
  if (mainScan == nullptr) {
    fprintf(stderr, "main scan build failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScan, "grp"), nullptr
  };
  NdbQueryOptions lookupOpts;
  lookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  lookupOpts.setAggregation(mainAgg);
  if (qb->lookupCte(0, 3, virtTab, cteKey, &lookupOpts) == nullptr) {
    fprintf(stderr, "lookupCte build failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    fprintf(stderr, "prepare failed: %s\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    fprintf(stderr, "startTransaction failed: %s\n",
            ndb->getNdbError().message);
    queryDef->destroy();
    return -1;
  }
  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    fprintf(stderr, "createQuery failed: %s\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    fprintf(stderr, "execute failed: %s\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  int failures = 0;
  if (outcome == NdbQuery::NextResult_error) {
    fprintf(stderr, "nextResult failed: %s\n", query->getNdbError().message);
    failures++;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    fprintf(stderr, "FAIL CTE_LOOKUP agg feed: missing result aggregator\n");
    failures++;
  } else {
    NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
    if (rec.end()) {
      fprintf(stderr, "FAIL CTE_LOOKUP agg feed: no result rows\n");
      failures++;
    } else {
      NdbAggregator::Result minV = rec.FetchAggregationResult();
      NdbAggregator::Result maxV = rec.FetchAggregationResult();
      if (verifyString("linked CTE MIN(min_v)", minV, "Alice") != 0) {
        failures++;
      }
      if (verifyString("linked CTE MAX(max_v)", maxV, "Eve") != 0) {
        failures++;
      }
    }
  }

  query->close();
  trans->close();
  queryDef->destroy();
  return failures == 0 ? 0 : -1;
}

static int runBuilderRejectTest(Ndb *ndb) {
  V("\n=== Test: builder rejects SUM over string registers ===\n");

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable("vctest_t");
  const NdbDictionary::Table *tab = dict->getTable("vctest_t");
  if (tab == nullptr) {
    fprintf(stderr, "Cannot find vctest_t: %s\n", dict->getNdbError().message);
    return -1;
  }

  const char *columns[] = {"vname", "cname", "lname"};
  const char *labels[] = {"VARCHAR", "CHAR", "Longvarchar"};
  for (Uint32 i = 0; i < 3; i++) {
    NdbAggregator agg(tab);
    if (!agg.LoadColumn(columns[i], 0)) {
      fprintf(stderr, "LoadColumn(%s) unexpectedly failed: %s\n",
              columns[i], agg.GetError().err_msg_);
      return -1;
    }
    if (agg.Sum(0, 0)) {
      fprintf(stderr, "FAIL builder: SUM(%s) unexpectedly succeeded\n",
              labels[i]);
      return -1;
    }
    V("  OK  SUM(%s) rejected: %s\n",
      labels[i], agg.GetError().err_msg_);
  }
  return 0;
}

static void usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s [options]\n"
          "  -c <connect_string>  NDB connect string (default: localhost:1186)\n"
          "  -m <port>            MySQL port (default: 3306)\n"
          "  -v, --verbose        Verbose output\n"
          "  -h, --help           Show help\n",
          prog);
}

int main(int argc, char **argv) {
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    } else if (strcmp(argv[i], "-v") == 0 ||
               strcmp(argv[i], "--verbose") == 0) {
      verbose = true;
    } else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
      connectString = argv[++i];
    } else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) {
      mysqlPort = atoi(argv[++i]);
    } else {
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }
  }

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout. */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  printf("testVarcharMinMax: Phase I.6 F.2 string MIN/MAX kernel-API test\n");
  printf("  Connect: %s  MySQL port: %d\n", connectString, mysqlPort);

  ndb_init();
  int result = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster mgm: %s\n", connectString);
      ndb_end(0);
      return 1;
    }
    if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready within 30s\n");
      ndb_end(0);
      return 1;
    }

    MYSQL *mysqlConn = connectMysql(mysqlPort);
    if (mysqlConn == nullptr) {
      fprintf(stderr, "Cannot connect to MySQL on port %d\n", mysqlPort);
      ndb_end(0);
      return 1;
    }

    if (setupSchema(mysqlConn) != 0) {
      mysql_close(mysqlConn);
      ndb_end(0);
      return 1;
    }

    {
      Ndb ndb(&clusterConn, "test");
      if (ndb.init() != 0) {
        fprintf(stderr, "Ndb::init failed: %s\n", ndb.getNdbError().message);
        dropSchema(mysqlConn);
        mysql_close(mysqlConn);
        ndb_end(0);
        return 1;
      }
      if (runScalarTest(&ndb) != 0) result = 1;
      if (runGroupByTest(&ndb) != 0) result = 1;
      if (runLongvarcharGroupByTest(&ndb) != 0) result = 1;
      if (runCteScanStringDeliveryTest(&ndb) != 0) result = 1;
      if (runCteLookupStringAggFeedTest(&ndb) != 0) result = 1;
      if (runBuilderRejectTest(&ndb) != 0) result = 1;
    }

    dropSchema(mysqlConn);
    mysql_close(mysqlConn);
  }

  ndb_end(0);

  if (result == 0) {
    write(mtr_fd, "PASSED\n", 7);
  } else {
    write(mtr_fd, "FAILED\n", 7);
  }
  close(mtr_fd);

  return result;
}
