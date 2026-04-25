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
 * testCteNdbApiFilter — Integration test for WHERE-clause filters on
 *                       pushdown CTE queries via the NdbQueryBuilder API.
 *
 * Phase A (this file, tests 1-12) covers:
 *   - Tests 1-5: baseline filter tests on ordinary scan operations.
 *     These prove the refactor of DBTUP's interpreterNextLab (switch
 *     -> extracted InterpreterContext handlers) is healthy before any
 *     CTE-specific code runs.  If one of these fails, any later CTE
 *     test failure would be unrelated to CTE code.
 *   - Tests 6-12 (added in step A.6): filter semantics on CTE_LOOKUP_REQ
 *     via lookupCte() + NdbQueryOptions::setInterpretedCode().
 *
 * Schema (created via MySQL):
 *   filter_src(pk INT PK, grp INT, val BIGINT, tag CHAR(8))
 *
 * Test data (6 rows):
 *   (1, 1, 10, 'alpha')
 *   (2, 1, 20, 'alpha')
 *   (3, 2, 30, 'beta ')    -- CHAR(8) rpad
 *   (4, 2, 40, 'beta ')
 *   (5, 3, 50, 'gamma')
 *   (6, 3, 60, 'gamma')
 *
 * Usage: testCteNdbApiFilter -c <connect_string> -m <mysql_port> [-v]
 *                           [--only N]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbAggregator.hpp>
#include <NdbSleep.h>
#include "NdbQueryBuilder.hpp"
#include "NdbQueryBuilderImpl.hpp"
#include "NdbQueryOperation.hpp"
#include "mysql/strings/m_ctype.h"  /* CHARSET_INFO body for csNumber lookup */

#include <mysql.h>

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while(0)

static const char *TEST_DB = "test";
static const char *SRC_TABLE = "filter_src";
static const char *CTE_SRC_TABLE = "cte_src";
static const char *CTE_VIRTUAL_TABLE = "cte_virtual";

/* ------------------------------------------------------------------ */
/* MySQL helpers                                                       */
/* ------------------------------------------------------------------ */

static MYSQL *
connectMysql(int port)
{
  MYSQL *conn = mysql_init(nullptr);
  if (conn == nullptr) return nullptr;
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         TEST_DB, port, nullptr, 0) == nullptr) {
    fprintf(stderr, "mysql_real_connect: %s\n", mysql_error(conn));
    mysql_close(conn);
    return nullptr;
  }
  return conn;
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

/* ------------------------------------------------------------------ */
/* Schema setup                                                        */
/* ------------------------------------------------------------------ */

static int
createTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS filter_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual");

  /* Flat table for the baseline scan-filter tests (Tests 1-5). */
  if (sqlExec(conn,
      "CREATE TABLE filter_src ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  grp INT NOT NULL,"
      "  val BIGINT NOT NULL,"
      "  tag CHAR(8) NOT NULL"
      ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  /* CTE source + virtual tables, mirroring testCteNdbApi.cpp so Phase A.6
   * CTE_LOOKUP filter tests can be written against a familiar schema. */
  if (sqlExec(conn,
      "CREATE TABLE cte_src ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  grp INT NOT NULL,"
      "  val BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
      "CREATE TABLE cte_virtual ("
      "  grp INT NOT NULL PRIMARY KEY,"
      "  total BIGINT NOT NULL"
      ") ENGINE=NDB") != 0) return -1;

  return 0;
}

static int
insertTestData(MYSQL *conn)
{
  if (sqlExec(conn,
      "INSERT INTO filter_src VALUES "
      "(1,1,10,'alpha'),(2,1,20,'alpha'),"
      "(3,2,30,'beta'), (4,2,40,'beta'),"
      "(5,3,50,'gamma'),(6,3,60,'gamma')") != 0) return -1;

  /* CTE 0 = SELECT grp, SUM(val) FROM cte_src GROUP BY grp
   * Data: (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)
   * → cte0 = {(grp=1,total=30),(grp=2,total=70),(grp=3,total=50)} */
  return sqlExec(conn,
      "INSERT INTO cte_src VALUES "
      "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)");
}

static void
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS filter_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_src");
  sqlExec(conn, "DROP TABLE IF EXISTS cte_virtual");
}

/* ------------------------------------------------------------------ */
/* Common helpers: run a scan with an interpreted filter               */
/* ------------------------------------------------------------------ */

struct ScanRow {
  Int32 pk;
  Int32 grp;
  Int64 val;
  char  tag[9];   /* CHAR(8) + NUL */
};

/*
 * Executes the given scan operation with the provided (already-finalised)
 * filter and returns the number of accepted rows.  On error, returns -1
 * and prints a reason.  rowsOut (optional) is filled with up to
 * maxRows collected rows, in arrival order.
 */
static int
runScanWithFilter(Ndb *ndb,
                  const NdbDictionary::Table *tab,
                  const NdbInterpretedCode *filterCode,
                  ScanRow *rowsOut, int maxRows)
{
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("(startTransaction: %s) ", ndb->getNdbError().message);
    return -1;
  }

  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == nullptr) {
    printf("(getNdbScanOperation: %s) ", trans->getNdbError().message);
    trans->close();
    return -1;
  }

  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    printf("(readTuples: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  NdbRecAttr *pkAttr  = scan->getValue("pk");
  NdbRecAttr *grpAttr = scan->getValue("grp");
  NdbRecAttr *valAttr = scan->getValue("val");
  NdbRecAttr *tagAttr = scan->getValue("tag");
  if (pkAttr == nullptr || grpAttr == nullptr ||
      valAttr == nullptr || tagAttr == nullptr) {
    printf("(getValue: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  if (filterCode != nullptr &&
      scan->setInterpretedCode(filterCode) != 0) {
    printf("(setInterpretedCode: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("(execute: %s) ", trans->getNdbError().message);
    trans->close();
    return -1;
  }

  int count = 0;
  int rc;
  while ((rc = scan->nextResult(true)) == 0) {
    if (rowsOut != nullptr && count < maxRows) {
      rowsOut[count].pk  = pkAttr->int32_value();
      rowsOut[count].grp = grpAttr->int32_value();
      rowsOut[count].val = valAttr->int64_value();
      /* NDB stores CHAR zero-padded; trim trailing 0x00 and 0x20 */
      const char *tagRaw = tagAttr->aRef();
      int len = 8;
      while (len > 0 &&
             (tagRaw[len - 1] == '\0' || tagRaw[len - 1] == ' ')) len--;
      if (len > 8) len = 8;
      memcpy(rowsOut[count].tag, tagRaw, len);
      rowsOut[count].tag[len] = '\0';
    }
    count++;
  }

  if (rc < 0) {
    printf("(nextResult: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  scan->close();
  trans->close();
  return count;
}

/* ------------------------------------------------------------------ */
/* Test 1: scan with filter that accepts all rows                      */
/*                                                                     */
/* Filter program: just interpret_exit_ok() — no branches, no compares.*/
/* Verifies baseline EXIT_OK handler + minimal interpreter loop.       */
/* Expected: 6 rows (all).                                             */
/* ------------------------------------------------------------------ */

static int
testScanFilterAcceptAll(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 1: scan filter — accept all ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  if (tab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  Uint32 codeBuf[32];
  NdbInterpretedCode code(tab, codeBuf, sizeof(codeBuf) / sizeof(codeBuf[0]));
  if (code.interpret_exit_ok() != 0 || code.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", code.getNdbError().message);
    return -1;
  }

  int count = runScanWithFilter(ndb, tab, &code, nullptr, 0);
  if (count != 6) {
    printf("FAILED (expected 6 rows, got %d)\n", count);
    return -1;
  }

  V("  Result: %d rows\n", count);
  printf("OK (%d rows)\n", count);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 2: scan filter on a single column (grp >= 2)                   */
/*                                                                     */
/* Filter: branch_col_le(grp, 2, PASS); exit_nok; def_label(PASS);     */
/*         exit_ok.                                                    */
/*                                                                     */
/* branch_col_le has inverted semantics — branches when col >= val,    */
/* per CLAUDE.md.  So this keeps rows with grp >= 2.                   */
/*                                                                     */
/* Expected: 4 rows (pk 3, 4, 5, 6).                                   */
/* ------------------------------------------------------------------ */

static int
testScanFilterSingleCol(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 2: scan filter — grp >= 2 ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  const NdbDictionary::Column *grpCol = tab->getColumn("grp");
  Uint32 grpColNo = grpCol->getColumnNo();

  Uint32 codeBuf[64];
  NdbInterpretedCode code(tab, codeBuf, sizeof(codeBuf) / sizeof(codeBuf[0]));
  Uint32 v = 2;
  const Uint32 PASS = 0;
  if (code.branch_col_le(&v, sizeof(v), grpColNo, PASS) != 0 ||
      code.interpret_exit_nok() != 0 ||
      code.def_label(PASS) != 0 ||
      code.interpret_exit_ok() != 0 ||
      code.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", code.getNdbError().message);
    return -1;
  }

  ScanRow rows[8];
  int count = runScanWithFilter(ndb, tab, &code, rows, 8);
  if (count != 4) {
    printf("FAILED (expected 4 rows, got %d)\n", count);
    return -1;
  }

  /* Sanity: all returned rows should have grp >= 2 */
  for (int i = 0; i < count; i++) {
    V("  row %d: pk=%d grp=%d val=%lld tag=%s\n", i,
      rows[i].pk, rows[i].grp, (long long)rows[i].val, rows[i].tag);
    if (rows[i].grp < 2) {
      printf("FAILED (row pk=%d has grp=%d < 2)\n",
             rows[i].pk, rows[i].grp);
      return -1;
    }
  }

  printf("OK (%d rows)\n", count);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 3: scan filter combining two columns (grp >= 2 AND val > 30)   */
/*                                                                     */
/* Pattern: sequential rejects, fall-through-to-accept.                */
/*   branch_col_le(grp, 2, P1)   -- if grp >= 2 goto P1                */
/*   exit_nok                    -- else reject                        */
/*   def_label(P1)                                                      */
/*   branch_col_gt(val, 30, P2)  -- branch_col_gt is inverted =>       */
/*                                   branches when col < val.  So goto */
/*                                   P2 when val < 30 (i.e. reject).   */
/*   exit_ok                     -- val >= 30: accept                   */
/*     (but we want val > 30 strictly — use branch_col_ge instead:     */
/*      branch_col_ge branches when val <= 30 => goto reject)           */
/*   def_label(P2)                                                      */
/*   exit_nok                                                           */
/*                                                                     */
/* Using inverted-semantics table (CLAUDE.md):                         */
/*   branch_col_ge(col,v) -> branches when col <= v.                   */
/*                                                                     */
/* So "val > 30" (keep) <=> "reject when val <= 30"                    */
/*   branch_col_ge(val, 30, REJECT)                                     */
/*                                                                     */
/* Expected: 3 rows (pk 4 [grp=2,val=40], pk 5 [grp=3,val=50],         */
/*                   pk 6 [grp=3,val=60]).                              */
/* ------------------------------------------------------------------ */

static int
testScanFilterTwoCol(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 3: scan filter — grp >= 2 AND val > 30 ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  Uint32 grpColNo = tab->getColumn("grp")->getColumnNo();
  Uint32 valColNo = tab->getColumn("val")->getColumnNo();

  Uint32 codeBuf[64];
  NdbInterpretedCode code(tab, codeBuf, sizeof(codeBuf) / sizeof(codeBuf[0]));

  Uint32 grpVal = 2;
  Int64  valVal = 30;
  const Uint32 PASS_GRP = 0;
  const Uint32 REJECT   = 1;

  /* Filter: grp >= 2 AND val > 30 */
  if (code.branch_col_le(&grpVal, sizeof(grpVal), grpColNo, PASS_GRP) != 0 ||
      code.interpret_exit_nok() != 0 ||
      code.def_label(PASS_GRP) != 0 ||
      code.branch_col_ge(&valVal, sizeof(valVal), valColNo, REJECT) != 0 ||
      code.interpret_exit_ok() != 0 ||
      code.def_label(REJECT) != 0 ||
      code.interpret_exit_nok() != 0 ||
      code.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", code.getNdbError().message);
    return -1;
  }

  ScanRow rows[8];
  int count = runScanWithFilter(ndb, tab, &code, rows, 8);
  if (count != 3) {
    printf("FAILED (expected 3 rows, got %d)\n", count);
    return -1;
  }

  for (int i = 0; i < count; i++) {
    V("  row %d: pk=%d grp=%d val=%lld\n", i,
      rows[i].pk, rows[i].grp, (long long)rows[i].val);
    if (rows[i].grp < 2 || rows[i].val <= 30) {
      printf("FAILED (row pk=%d grp=%d val=%lld violates filter)\n",
             rows[i].pk, rows[i].grp, (long long)rows[i].val);
      return -1;
    }
  }

  printf("OK (%d rows)\n", count);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 4: scan filter on CHAR column (tag = 'beta')                   */
/*                                                                     */
/* Filter: branch_col_ne(tag, 'beta', REJECT); exit_ok;                */
/*         def_label(REJECT); exit_nok.                                 */
/*                                                                     */
/* branch_col_ne is NOT inverted; it branches when col != val.         */
/*                                                                     */
/* The comparison value must be padded with spaces to the column       */
/* width (CHAR(8)).  The column's type descriptor drives the charset-  */
/* aware comparison.                                                    */
/*                                                                     */
/* Expected: 2 rows (pk 3, 4).                                         */
/* ------------------------------------------------------------------ */

static int
testScanFilterStringEq(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 4: scan filter — tag = 'beta' ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  Uint32 tagColNo = tab->getColumn("tag")->getColumnNo();

  Uint32 codeBuf[64];
  NdbInterpretedCode code(tab, codeBuf, sizeof(codeBuf) / sizeof(codeBuf[0]));

  /* CHAR(8) with latin1 charset: MySQL stores the value space-padded
   * to the full column width.  The NDB interpreter uses the column's
   * charset-aware cmp, which under latin1_swedish_ci uses PAD SPACE
   * semantics (trailing 0x20 ignored on both sides).  Must pad with
   * spaces to match, not NULs. */
  char tagVal[8];
  memset(tagVal, ' ', sizeof(tagVal));
  memcpy(tagVal, "beta", 4);

  const Uint32 REJECT = 0;
  if (code.branch_col_ne(tagVal, sizeof(tagVal), tagColNo, REJECT) != 0 ||
      code.interpret_exit_ok() != 0 ||
      code.def_label(REJECT) != 0 ||
      code.interpret_exit_nok() != 0 ||
      code.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", code.getNdbError().message);
    return -1;
  }

  ScanRow rows[8];
  int count = runScanWithFilter(ndb, tab, &code, rows, 8);
  if (count != 2) {
    printf("FAILED (expected 2 rows, got %d)\n", count);
    return -1;
  }

  for (int i = 0; i < count; i++) {
    V("  row %d: pk=%d tag='%s'\n", i, rows[i].pk, rows[i].tag);
    if (strcmp(rows[i].tag, "beta") != 0) {
      printf("FAILED (row pk=%d tag='%s' != 'beta')\n",
             rows[i].pk, rows[i].tag);
      return -1;
    }
  }

  printf("OK (%d rows)\n", count);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 5: scan filter rejecting every row (grp > 99)                  */
/*                                                                     */
/* Filter: branch_col_le(grp, 99, PASS) — branches when grp >= 99.     */
/* All our grp values are 1..3, so the branch is never taken, and      */
/* the fall-through is exit_nok.  Verifies scan-level empty result.    */
/*                                                                     */
/* Expected: 0 rows.                                                   */
/* ------------------------------------------------------------------ */

static int
testScanFilterRejectAll(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 5: scan filter — reject all (grp > 99) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *tab = dict->getTable(SRC_TABLE);
  Uint32 grpColNo = tab->getColumn("grp")->getColumnNo();

  Uint32 codeBuf[32];
  NdbInterpretedCode code(tab, codeBuf, sizeof(codeBuf) / sizeof(codeBuf[0]));

  Uint32 v = 99;
  const Uint32 PASS = 0;
  if (code.branch_col_le(&v, sizeof(v), grpColNo, PASS) != 0 ||
      code.interpret_exit_nok() != 0 ||
      code.def_label(PASS) != 0 ||
      code.interpret_exit_ok() != 0 ||
      code.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", code.getNdbError().message);
    return -1;
  }

  int count = runScanWithFilter(ndb, tab, &code, nullptr, 0);
  if (count != 0) {
    printf("FAILED (expected 0 rows, got %d)\n", count);
    return -1;
  }

  printf("OK (0 rows)\n");
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 6: CTE_LOOKUP with filter on a CTE virtual column              */
/*                                                                     */
/* SQL equivalent:                                                     */
/*   WITH cte0 AS (SELECT grp, SUM(val) AS total                       */
/*                 FROM cte_src GROUP BY grp)                          */
/*   SELECT s.grp                                                      */
/*   FROM cte_src s JOIN cte0 ON cte0.grp = s.grp                      */
/*   WHERE cte0.total > 40                                             */
/*                                                                     */
/* cte0 = {(1,30),(2,70),(3,50)}.  Main scans cte_src (5 rows);        */
/* only lookups with cte0.total > 40 pass the filter:                  */
/*   pk=1 (grp=1, total=30) -> filter rejects                          */
/*   pk=2 (grp=1, total=30) -> filter rejects                          */
/*   pk=3 (grp=2, total=70) -> filter accepts                          */
/*   pk=4 (grp=2, total=70) -> filter accepts                          */
/*   pk=5 (grp=3, total=50) -> filter accepts                          */
/* With MatchNonNull (inner join), rejects drop the main row, so 3     */
/* main rows survive.                                                  */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterSingleCol(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 6: CTE_LOOKUP filter — cte0.total > 40 ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0 = GROUP BY grp, SUM(val) */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Build filter: WHERE cte0.total > 40
   * Virtual layout per buildCteLinkedBuffer:
   *   position 0 = grp    (GB key)
   *   position 1 = total  (aggregate result)
   * Use inverted-inequality: branch_linked_mem_ge(total, 40, REJECT)
   * branches when col <= val, i.e. reject when total <= 40. */
  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 40;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = virtTab->getColumn("total")->getColumnNo();
  if (filterCode.branch_linked_mem_ge(
          /*position=*/1, virtTab, totalAttrId,
          &threshold, sizeof(threshold), REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", filterCode.getNdbError().message);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) { printf("FAILED (create)\n"); return -1; }

  /* CTE 0 subtree: scan cte_src + readTuple(self-join) aggregating */
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  if (cteScanOp == nullptr) {
    printf("FAILED (CTE scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  if (qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts) == nullptr) {
    printf("FAILED (CTE leaf: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, cteAgg) != 0) {
    printf("FAILED (defineCte)\n");
    qb->destroy();
    return -1;
  }

  /* Main query: scan cte_src, CTE_LOOKUP by grp, filter applied. */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setInterpretedCode(filterCode);
  if (qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts) == nullptr) {
    printf("FAILED (lookupCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
    queryDef->destroy();
    return -1;
  }
  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Define the row projection so the receive buffers are sized. */
  NdbQueryOperation *mainQOp = query->getQueryOperation(3U);
  NdbQueryOperation *cteQOp = query->getQueryOperation(4U);
  NdbRecAttr *raGrp = nullptr;
  if (mainQOp != nullptr) raGrp = mainQOp->getValue("grp");
  if (cteQOp != nullptr) {
    (void)cteQOp->getValue("grp");
    (void)cteQOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans %d:%s, query %d:%s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    if (raGrp != nullptr) V("  row: main.grp=%d\n", raGrp->int32_value());
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount == 3) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }
  printf("FAILED (expected 3 rows, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 7: CTE_LOOKUP filter — filter rejects every group              */
/*                                                                     */
/* Query: same as Test 6, but the filter threshold is higher than any  */
/* possible CTE total (cte.total > 1000).  All CTE_LOOKUPs reject, so  */
/* every main row is dropped by MatchNonNull.                          */
/*                                                                     */
/* Exercises the reject path on every iteration and verifies the scan  */
/* returns gracefully — no hang, no partial results, correct 0-count.  */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterNoMatch(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 7: CTE_LOOKUP filter — cte0.total > 1000 (rejects all) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 1000;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = virtTab->getColumn("total")->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts);
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setInterpretedCode(filterCode);
  qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  NdbQueryOperation *mainQOp = query->getQueryOperation(3U);
  NdbQueryOperation *cteQOp = query->getQueryOperation(4U);
  if (mainQOp != nullptr) (void)mainQOp->getValue("grp");
  if (cteQOp != nullptr) {
    (void)cteQOp->getValue("grp");
    (void)cteQOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow)
    rowCount++;
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount == 0) {
    printf("OK (0 rows)\n");
    return 0;
  }
  printf("FAILED (expected 0, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 8: CTE_LOOKUP filter — unconditional BRANCH opcode             */
/*                                                                     */
/* Verifies that the plain BRANCH opcode (dispatch slot 9) is wired    */
/* in s_cte_filter_handlers.  The filter uses branch_label to jump    */
/* over an exit_nok, landing on exit_ok — effectively accept-all, but  */
/* via the BRANCH instruction.  True backward-jump behaviour (looping  */
/* filters) can't be constructed from NdbInterpretedCode's high-level  */
/* builder API in Phase A, and becomes a negative test in Phase C     */
/* when IFLAG_DISALLOW_BACKWARD_JUMPS is enforced for agg mode.        */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterBranchOpcode(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 8: CTE_LOOKUP filter — BRANCH opcode ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  /* Filter: branch over the reject instruction (unconditional),
   * then fall through to exit_ok.  Equivalent to accept-all, but
   * exercises the BRANCH opcode (dispatch table slot 9) via the
   * CTE filter path. */
  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  const Uint32 SKIP = 0;
  if (filterCode.branch_label(SKIP) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.def_label(SKIP) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", filterCode.getNdbError().message);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts);
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setInterpretedCode(filterCode);
  qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  NdbQueryOperation *mainQOp = query->getQueryOperation(3U);
  NdbQueryOperation *cteQOp = query->getQueryOperation(4U);
  if (mainQOp != nullptr) (void)mainQOp->getValue("grp");
  if (cteQOp != nullptr) {
    (void)cteQOp->getValue("grp");
    (void)cteQOp->getValue("total");
  }
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow)
    rowCount++;
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* All 5 main rows should pass (filter is accept-all via branch). */
  if (rowCount == 5) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }
  printf("FAILED (expected 5, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 9: CTE_LOOKUP filter — runs BEFORE agg feed                    */
/*                                                                     */
/* CTE_LOOKUP is an aggregate leaf (setAggregation on the lookup op).  */
/* The filter must run BEFORE the result is fed into the main          */
/* aggregation, so rejected rows don't contribute to COUNT / SUM.      */
/*                                                                     */
/*   mainAgg = COUNT(*), SUM(cte0.total)                               */
/*   Filter on CTE_LOOKUP: cte0.total > 40                             */
/*                                                                     */
/* Without filter the 5 main rows would feed the main agg with         */
/* COUNT=5, SUM(total) = 30+30+70+70+50 = 250.                         */
/* With filter the 2 rows where grp=1 (total=30) are rejected:         */
/* COUNT=3, SUM(total) = 70+70+50 = 190.                               */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterInAggFeed(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 9: CTE_LOOKUP filter — runs before agg feed ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup)\n");
    return -1;
  }

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  /* Main agg over the CTE_LOOKUP's virtual `total` column at linked
   * position 1 (position 0 = grp key, position 1 = total aggregate). */
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, totalCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Filter on cte0.total > 40 — reject when total <= 40. */
  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 40;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = totalCol->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts);
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setAggregation(mainAgg);
  cteLookupOpts.setInterpretedCode(filterCode);
  qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  Int64 count = rec.FetchAggregationResult().data_int64();
  Int64 sum = rec.FetchAggregationResult().data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  if (count == 3 && sum == 190) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }
  printf("FAILED (expected COUNT=3 SUM=190, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 10: CTE_LOOKUP filter — LEFT JOIN semantics                    */
/*                                                                     */
/* Same query as Test 6, but without MatchNonNull on the lookupCte —   */
/* i.e. LEFT JOIN semantics.  Filter-rejected lookups emit             */
/* CTE_LOOKUP_REF GROUP_NOT_FOUND, which DBSPJ maps to a NULL CTE row  */
/* instead of dropping the main row.  Every one of the 5 main rows    */
/* survives; verify by iterating and counting.                         */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterLeftJoin(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 10: CTE_LOOKUP filter — LEFT JOIN null-row ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 40;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = virtTab->getColumn("total")->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts);
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  /* No setMatchType -> default (LEFT JOIN): filter-rejected lookups
   * yield NULL CTE rows but the main row still emits. */
  cteLookupOpts.setInterpretedCode(filterCode);
  qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  NdbQueryOperation *mainQOp = query->getQueryOperation(3U);
  NdbQueryOperation *cteQOp = query->getQueryOperation(4U);
  if (mainQOp != nullptr) (void)mainQOp->getValue("grp");
  if (cteQOp != nullptr) {
    (void)cteQOp->getValue("grp");
    (void)cteQOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow)
    rowCount++;
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount == 5) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }
  printf("FAILED (expected 5, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 11: Two CTEs, each with its own filter — no cross-contamination*/
/*                                                                     */
/* Define cte0 and cte1 as identical GROUP BY grp, SUM(val) CTEs       */
/* (both materialise {(1,30),(2,70),(3,50)}), then attach a different  */
/* filter to each of two CTE_LOOKUP ops in the same main query:        */
/*                                                                     */
/*   filter0 on cte0-lookup: total > 40  (accepts grp=2, grp=3)        */
/*   filter1 on cte1-lookup: total > 60  (accepts only grp=2)          */
/*                                                                     */
/* With MatchNonNull on both lookups, a main row survives only when    */
/* BOTH filters accept it.  Only grp=2 meets both, so pk=3 and pk=4    */
/* survive.                                                             */
/*                                                                     */
/* If either filter were leaking into the other's execution (e.g.,     */
/* shared buffer clobbering), the count would be wrong.                */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterMultipleCte(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 11: CTE_LOOKUP filter — two CTEs, two filters ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  /* Two independent NdbAggregators (same definition, separate instances
   * — each CTE has its own aggregation state). */
  NdbAggregator cteAgg0(srcTab), cteAgg1(srcTab);
  for (NdbAggregator *a : {&cteAgg0, &cteAgg1}) {
    if (!a->GroupBy("grp") || !a->LoadColumn("val", 0) ||
        !a->Sum(0, 0) || !a->Finalize()) {
      printf("FAILED (cteAgg)\n");
      return -1;
    }
  }

  const Uint32 totalAttrId = virtTab->getColumn("total")->getColumnNo();

  /* Filter 0: total > 40 */
  Uint32 code0Buf[64];
  NdbInterpretedCode filter0(virtTab, code0Buf,
                              sizeof(code0Buf) / sizeof(code0Buf[0]));
  {
    Int64 t = 40;
    const Uint32 R = 0;
    if (filter0.branch_linked_mem_ge(1, virtTab, totalAttrId, &t, sizeof(t), R) != 0 ||
        filter0.interpret_exit_ok() != 0 ||
        filter0.def_label(R) != 0 ||
        filter0.interpret_exit_nok() != 0 ||
        filter0.finalise() != 0) {
      printf("FAILED (build filter0)\n");
      return -1;
    }
  }

  /* Filter 1: total > 60 */
  Uint32 code1Buf[64];
  NdbInterpretedCode filter1(virtTab, code1Buf,
                              sizeof(code1Buf) / sizeof(code1Buf[0]));
  {
    Int64 t = 60;
    const Uint32 R = 0;
    if (filter1.branch_linked_mem_ge(1, virtTab, totalAttrId, &t, sizeof(t), R) != 0 ||
        filter1.interpret_exit_ok() != 0 ||
        filter1.def_label(R) != 0 ||
        filter1.interpret_exit_nok() != 0 ||
        filter1.finalise() != 0) {
      printf("FAILED (build filter1)\n");
      return -1;
    }
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* cte0 */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg0);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg0);

  /* cte1 — identical definition, separate subtree & agg */
  qb->beginCteSubtree(1);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg1);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(1, srcTab, cteAgg1);

  /* Main: scan + CTE_LOOKUP cte0 with filter0 + CTE_LOOKUP cte1 with filter1 */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);

  const NdbQueryOperand *key0[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions opts0;
  opts0.setMatchType(NdbQueryOptions::MatchNonNull);
  opts0.setInterpretedCode(filter0);
  qb->lookupCte(0, 2, virtTab, key0, &opts0);

  const NdbQueryOperand *key1[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions opts1;
  opts1.setMatchType(NdbQueryOptions::MatchNonNull);
  opts1.setInterpretedCode(filter1);
  qb->lookupCte(1, 2, virtTab, key1, &opts1);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);

  /* Op indices: 0=cte0 subtree, 1=cte0 scan, 2=cte0 readTuple,
   *             3=cte1 subtree, 4=cte1 scan, 5=cte1 readTuple,
   *             6=main scan,    7=lookupCte(0), 8=lookupCte(1) */
  NdbQueryOperation *mainQOp = query->getQueryOperation(6U);
  NdbQueryOperation *lk0QOp  = query->getQueryOperation(7U);
  NdbQueryOperation *lk1QOp  = query->getQueryOperation(8U);
  if (mainQOp != nullptr) (void)mainQOp->getValue("grp");
  if (lk0QOp != nullptr) {
    (void)lk0QOp->getValue("grp");
    (void)lk0QOp->getValue("total");
  }
  if (lk1QOp != nullptr) {
    (void)lk1QOp->getValue("grp");
    (void)lk1QOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }
  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow)
    rowCount++;
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount == 2) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }
  printf("FAILED (expected 2, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 12: CTE_SCAN filter on root scan — cte0.total > 40             */
/*                                                                     */
/* Main query is `scanCte(0)` with a filter on the scanned virtual     */
/* rows.  CTE 0 = GROUP BY grp, SUM(val): {(1,30),(2,70),(3,50)}.      */
/* Filter `total > 40` keeps grp=2 (total=70) and grp=3 (total=50);    */
/* grp=1 (total=30) is rejected — 2 rows.                              */
/*                                                                     */
/* Exercises the per-group filter gate in cteScanEmitResults (API      */
/* emit via FLUSH_AI) — every emitted group goes through the filter   */
/* before being routed to the API as a TRANSID_AI row.                 */
/* ------------------------------------------------------------------ */

static int
testCteScanFilterRoot(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 12: CTE_SCAN filter on root — cte0.total > 40 ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 40;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = virtTab->getColumn("total")->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  /* Main query: scanCte(0) with filter on the emitted CTE rows */
  NdbQueryOptions scanOpts;
  scanOpts.setInterpretedCode(filterCode);
  if (qb->scanCte(0, 2, virtTab, &scanOpts) == nullptr) {
    printf("FAILED (scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp   = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 g = raGrp ? raGrp->int32_value() : -1;
    Int64 t = raTotal ? raTotal->int64_value() : -1;
    V("  row: grp=%d total=%lld\n", g, (long long)t);
    if (t <= 40) {
      printf("FAILED (emitted row with total=%lld violates filter)\n",
             (long long)t);
      query->close();
      trans->close();
      queryDef->destroy();
      return -1;
    }
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount == 2) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }
  printf("FAILED (expected 2, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 13: CTE_SCAN filter + agg-feed — rejects don't reach target agg*/
/*                                                                     */
/* scanCte(0) is an aggregate leaf (setAggregation).  mainAgg sums     */
/* cte0.total.  Filter runs BEFORE the feed so rejected groups don't   */
/* contribute to the sum.                                              */
/*                                                                     */
/* Without filter, SUM(total) = 30+70+50 = 150 (COUNT=3).              */
/* With filter total > 40, SUM(total) = 70+50 = 120 (COUNT=2).         */
/*                                                                     */
/* Exercises the filter gate in cteScanAggFeed (first batch) along     */
/* with the B.2 DBSPJ AttrInfo-forwarding already present for          */
/* CTE_SCAN_REQ (no cte_scan_send release bug to fix here).            */
/* ------------------------------------------------------------------ */

static int
testCteScanFilterAggFeed(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 13: CTE_SCAN filter — runs before agg feed ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, totalCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 40;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = totalCol->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  NdbQueryOptions scanOpts;
  scanOpts.setAggregation(mainAgg);
  scanOpts.setInterpretedCode(filterCode);
  if (qb->scanCte(0, 2, virtTab, &scanOpts) == nullptr) {
    printf("FAILED (scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (rec.end()) {
    printf("FAILED (no result rows)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  Int64 count = rec.FetchAggregationResult().data_int64();
  Int64 sum = rec.FetchAggregationResult().data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  if (count == 2 && sum == 120) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }
  printf("FAILED (expected COUNT=2 SUM=120, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 14: CTE_SCAN filter — rejects every scanned group              */
/*                                                                     */
/* scanCte(0) as root with filter cte0.total > 1000 (no group matches).*/
/* Every group is rejected by cteScanEmitResults's filter gate.        */
/* CteScanConf emits numRows=0 and EndOfData=0x1 — the scan terminates */
/* cleanly with no TRANSID_AI rows.                                    */
/* ------------------------------------------------------------------ */

static int
testCteScanFilterEmptyResult(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 14: CTE_SCAN filter — rejects all groups ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 1000;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = virtTab->getColumn("total")->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  NdbQueryOptions scanOpts;
  scanOpts.setInterpretedCode(filterCode);
  qb->scanCte(0, 2, virtTab, &scanOpts);

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  if (mainOp != nullptr) {
    (void)mainOp->getValue("grp");
    (void)mainOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow)
    rowCount++;
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount == 0) {
    printf("OK (0 rows)\n");
    return 0;
  }
  printf("FAILED (expected 0, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 15: CTE_SCAN filter — spans batch boundary (>256 groups)       */
/*                                                                     */
/* CTE_SCAN_AGG_FEED_BATCH = 256, so a CTE with more than 256 groups   */
/* triggers a CONTINUEB self-signal between batches.  CONTINUEB can't  */
/* carry AttrInfo — the filter program survives only because           */
/* execCTE_SCAN_REQ now caches it in a CteScanIterState pool record    */
/* keyed through theData[9].  Verify by seeding 300 unique groups and  */
/* running an aggregation over them with a filter that rejects half.   */
/*                                                                     */
/* Dataset: cte_src(pk=grp=1..300, val=2*grp).  Re-seeded inside this  */
/* test (after tests 1-14 have run against the baseline 5 rows).       */
/*                                                                     */
/* CTE:    SELECT grp, SUM(val) AS total FROM cte_src GROUP BY grp     */
/*         => 300 groups, total = 2*grp for each (val=2*grp, one row). */
/* Main:   SUM(cte0.total) scalar aggregation, fed from scanCte(0)     */
/*         with filter total >= 302 (i.e. reject when total <= 300,    */
/*         keeping grp >= 151..300 = 150 groups).                      */
/* Expected: COUNT=150, SUM = 2*(151+152+...+300) = 67650.             */
/*                                                                     */
/* Without the CONTINUEB filter-forwarding fix, batch 2 (44 groups)    */
/* would skip the filter entirely, giving COUNT/SUM that depend on    */
/* hash ordering and exceed the expected values.                       */
/* ------------------------------------------------------------------ */

static int
testCteScanFilterBatchBoundary(Ndb *ndb, MYSQL *conn)
{
  printf("Test 15: CTE_SCAN filter across batch boundary (300 groups) ... ");
  fflush(stdout);

  /* Re-seed cte_src with 300 distinct groups (bulk-insert in chunks
   * of 50 to keep per-statement size modest but setup time short). */
  if (sqlExec(conn, "DELETE FROM cte_src") != 0) {
    printf("FAILED (delete)\n");
    return -1;
  }
  char sql[4096];
  int pos = 0;
  bool first = true;
  for (int g = 1; g <= 300; g++) {
    if (first) {
      pos = snprintf(sql, sizeof(sql), "INSERT INTO cte_src VALUES ");
      first = false;
    } else {
      pos += snprintf(sql + pos, sizeof(sql) - pos, ",");
    }
    pos += snprintf(sql + pos, sizeof(sql) - pos, "(%d,%d,%d)", g, g, 2 * g);
    if (g % 50 == 0 || g == 300) {
      if (sqlExec(conn, sql) != 0) {
        printf("FAILED (seed insert at g=%d)\n", g);
        return -1;
      }
      first = true;
    }
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  NdbAggregator mainAgg(virtTab);
  if (!mainAgg.LoadLinkedColumn(1, 0, totalCol) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Sum(1, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Filter: reject when total <= 300, i.e. keep grp >= 151. */
  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 300;
  const Uint32 REJECT = 0;
  const Uint32 totalAttrId = totalCol->getColumnNo();
  if (filterCode.branch_linked_mem_ge(1, virtTab, totalAttrId,
                                       &threshold, sizeof(threshold),
                                       REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  NdbQueryOptions scanOpts;
  scanOpts.setAggregation(mainAgg);
  scanOpts.setInterpretedCode(filterCode);
  if (qb->scanCte(0, 2, virtTab, &scanOpts) == nullptr) {
    printf("FAILED (scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {}
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  Int64 count = rec.FetchAggregationResult().data_int64();
  Int64 sum = rec.FetchAggregationResult().data_int64();

  query->close();
  trans->close();
  queryDef->destroy();

  /* Expected: 150 groups pass the filter (grp 151..300),
   *   SUM = 2*(151+152+...+300) = 2 * 33825 = 67650. */
  if (count == 150 && sum == 67650) {
    printf("OK (COUNT=%lld, SUM=%lld)\n", (long long)count, (long long)sum);
    return 0;
  }
  printf("FAILED (expected COUNT=150 SUM=67650, got COUNT=%lld SUM=%lld)\n",
         (long long)count, (long long)sum);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 16: CTE_SCAN root, large result (600 groups), no top agg       */
/*                                                                     */
/* Exercises the DBSPJ-mediated CTE_SCAN_REQ continuation path         */
/* (T_USER_PROJECTION / FLUSH_AI → API) across multiple batches.       */
/* CteScanData::m_batchSize is 256, so 600 groups spans 3 batches;     */
/* DBSPJ echoes the scanIterI from each CTE_SCAN_CONF back as the      */
/* next REQ's scanIterI (SignalLengthContinue) so DBLQH resumes from   */
/* the saved CteScanIterState pool record instead of restarting from   */
/* bucket 0.  Without Phase 1's fix this would deliver duplicate rows  */
/* (or loop forever) once more than 256 groups are present.            */
/* ------------------------------------------------------------------ */
static int
testCteScanRootLargeResult(Ndb *ndb, MYSQL *conn)
{
  const int NUM_GROUPS = 600;
  printf("Test 16: CTE_SCAN root — large result (%d groups) ... ",
         NUM_GROUPS);
  fflush(stdout);

  /* Re-seed cte_src with NUM_GROUPS distinct groups.  Bulk-insert in
   * chunks to keep per-statement size reasonable. */
  if (sqlExec(conn, "DELETE FROM cte_src") != 0) {
    printf("FAILED (delete)\n");
    return -1;
  }
  char sql[8192];
  int pos = 0;
  bool first = true;
  for (int g = 1; g <= NUM_GROUPS; g++) {
    if (first) {
      pos = snprintf(sql, sizeof(sql), "INSERT INTO cte_src VALUES ");
      first = false;
    } else {
      pos += snprintf(sql + pos, sizeof(sql) - pos, ",");
    }
    pos += snprintf(sql + pos, sizeof(sql) - pos, "(%d,%d,%d)", g, g, g);
    if (g % 100 == 0 || g == NUM_GROUPS) {
      if (sqlExec(conn, sql) != 0) {
        printf("FAILED (seed insert at g=%d)\n", g);
        return -1;
      }
      first = true;
      pos = 0;
    }
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  /* Trivial accept-all filter.  The presence of an interpreted program
   * is what routes rows through cteScanEmitResults (T_USER_PROJECTION
   * path) rather than the raw-emit legacy path, matching the shape
   * Test 12 uses but without row-dropping. */
  Uint32 codeBuf[16];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  if (filterCode.interpret_exit_ok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  /* Main query: scanCte(0) with trivial filter, NO top-level
   * aggregation.  Each group flows back to the API as TRANSID_AI via
   * FLUSH_AI. */
  NdbQueryOptions scanOpts;
  scanOpts.setInterpretedCode(filterCode);
  if (qb->scanCte(0, 2, virtTab, &scanOpts) == nullptr) {
    printf("FAILED (scanCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = nullptr;
  NdbRecAttr *raTotal = nullptr;
  if (mainOp != nullptr) {
    raGrp   = mainOp->getValue("grp");
    raTotal = mainOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Verify each grp in [1..NUM_GROUPS] appears exactly once and
   * total == grp (val == grp, each group has exactly one row). */
  bool seen[NUM_GROUPS + 1] = { false };
  Uint32 rowCount = 0;
  Int64 sumTotal = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 g = raGrp ? raGrp->int32_value() : -1;
    Int64 t = raTotal ? raTotal->int64_value() : -1;
    if (g < 1 || g > NUM_GROUPS) {
      printf("FAILED (out-of-range grp=%d)\n", g);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    if (seen[g]) {
      printf("FAILED (duplicate grp=%d at row %u)\n", g, rowCount);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    seen[g] = true;
    if (t != (Int64)g) {
      printf("FAILED (grp=%d total=%lld expected %d)\n",
             g, (long long)t, g);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    sumTotal += t;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Expected: NUM_GROUPS rows, SUM = 1+2+...+NUM_GROUPS = N*(N+1)/2 */
  const Int64 expectedSum = (Int64)NUM_GROUPS * (NUM_GROUPS + 1) / 2;
  if (rowCount == (Uint32)NUM_GROUPS && sumTotal == expectedSum) {
    printf("OK (%u rows, SUM=%lld)\n", rowCount, (long long)sumTotal);
    return 0;
  }
  printf("FAILED (expected rows=%d sum=%lld, got rows=%u sum=%lld)\n",
         NUM_GROUPS, (long long)expectedSum, rowCount, (long long)sumTotal);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 17: CTE_SCAN root + setBatchSize, forces SCAN_NEXTREQ cycles   */
/*                                                                     */
/* 500 groups, setBatchSize(50) on the scanCte root.  Phase 2 wires    */
/* CTE_SCAN root into the standard SCAN_FRAGCONF/SCAN_NEXTREQ cycle,   */
/* so DBLQH emits 50 rows → DBSPJ sends SCAN_FRAGCONF(                 */
/* fragmentCompleted=0) → DBTC → API → SCAN_NEXTREQ →                  */
/* cte_scan_execSCAN_NEXTREQ → next CTE_SCAN_REQ (SignalLengthContinue */
/* + scanIterI).  Expect ~10 API round-trips; test verifies            */
/* correctness only — no round-trip counter surface.                   */
/* ------------------------------------------------------------------ */
static int
testCteScanRootSmallBatch(Ndb *ndb, MYSQL *conn)
{
  const int NUM_GROUPS = 500;
  const Uint32 BATCH = 50;
  printf("Test 17: CTE_SCAN root — setBatchSize(%u), %d groups ... ",
         BATCH, NUM_GROUPS);
  fflush(stdout);

  if (sqlExec(conn, "DELETE FROM cte_src") != 0) {
    printf("FAILED (delete)\n");
    return -1;
  }
  char sql[8192];
  int pos = 0;
  bool first = true;
  for (int g = 1; g <= NUM_GROUPS; g++) {
    if (first) {
      pos = snprintf(sql, sizeof(sql), "INSERT INTO cte_src VALUES ");
      first = false;
    } else {
      pos += snprintf(sql + pos, sizeof(sql) - pos, ",");
    }
    pos += snprintf(sql + pos, sizeof(sql) - pos, "(%d,%d,%d)", g, g, g);
    if (g % 100 == 0 || g == NUM_GROUPS) {
      if (sqlExec(conn, sql) != 0) {
        printf("FAILED (seed insert)\n");
        return -1;
      }
      first = true;
      pos = 0;
    }
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  Uint32 codeBuf[16];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  if (filterCode.interpret_exit_ok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  NdbQueryOptions scanOpts;
  scanOpts.setInterpretedCode(filterCode);
  if (qb->scanCte(0, 2, virtTab, &scanOpts) == nullptr) {
    printf("FAILED (scanCte)\n");
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare)\n");
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp = mainOp->getValue("grp");
  NdbRecAttr *raTotal = mainOp->getValue("total");
  if (mainOp->setBatchSize(BATCH) != 0) {
    printf("FAILED (setBatchSize: %s)\n", query->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute)\n");
    trans->close(); queryDef->destroy();
    return -1;
  }

  bool seen[NUM_GROUPS + 1] = { false };
  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 g = raGrp->int32_value();
    Int64 t = raTotal->int64_value();
    if (g < 1 || g > NUM_GROUPS || seen[g] || t != (Int64)g) {
      printf("FAILED (bad row grp=%d total=%lld at row %u)\n",
             g, (long long)t, rowCount);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    seen[g] = true;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != (Uint32)NUM_GROUPS) {
    printf("FAILED (expected %d rows, got %u)\n", NUM_GROUPS, rowCount);
    return -1;
  }
  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 18: CTE_SCAN root — early close mid-scan                       */
/*                                                                     */
/* Read a few rows, then close() the query before exhausting it.       */
/* This hits the user-close SCAN_NEXTREQ path (ScanFragNextReq         */
/* CloseFlag), which DBSPJ converts to abort() → cte_scan_abort →      */
/* fire-and-forget CTE_SCAN_REQ(CloseFlag) to every DBLQH holding a    */
/* CteScanIterState pool record.  Exercises the close path without    */
/* leaving pool records behind.                                        */
/* ------------------------------------------------------------------ */
static int
testCteScanRootEarlyClose(Ndb *ndb, MYSQL *conn)
{
  const int NUM_GROUPS = 200;
  const Uint32 BATCH = 20;
  const int READ_FIRST = 30;  // > 1 batch, < full scan
  printf("Test 18: CTE_SCAN root — early close after %d/%d rows ... ",
         READ_FIRST, NUM_GROUPS);
  fflush(stdout);

  if (sqlExec(conn, "DELETE FROM cte_src") != 0) {
    printf("FAILED (delete)\n");
    return -1;
  }
  char sql[4096];
  int pos = snprintf(sql, sizeof(sql), "INSERT INTO cte_src VALUES ");
  for (int g = 1; g <= NUM_GROUPS; g++) {
    pos += snprintf(sql + pos, sizeof(sql) - pos, "%s(%d,%d,%d)",
                    (g == 1 ? "" : ","), g, g, g);
  }
  if (sqlExec(conn, sql) != 0) {
    printf("FAILED (seed)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  Uint32 codeBuf[16];
  NdbInterpretedCode filterCode(virtTab, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  if (filterCode.interpret_exit_ok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  NdbQueryOptions scanOpts;
  scanOpts.setInterpretedCode(filterCode);
  if (qb->scanCte(0, 2, virtTab, &scanOpts) == nullptr) {
    printf("FAILED (scanCte)\n");
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare)\n");
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  mainOp->getValue("grp");
  mainOp->getValue("total");
  if (mainOp->setBatchSize(BATCH) != 0) {
    printf("FAILED (setBatchSize)\n");
    trans->close(); queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute)\n");
    trans->close(); queryDef->destroy();
    return -1;
  }

  int got = 0;
  NdbQuery::NextResultOutcome outcome;
  while (got < READ_FIRST &&
         (outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    got++;
  }
  if (got < READ_FIRST) {
    printf("FAILED (got only %d rows before close)\n", got);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  /* Close mid-scan.  Drives user-close SCAN_NEXTREQ → DBSPJ abort →
   * cte_scan_abort → close REQs to every DBLQH holding a pool record. */
  query->close();
  trans->close();
  queryDef->destroy();

  printf("OK (closed after %d/%d)\n", got, NUM_GROUPS);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 19: real-table main scan + CTE_LOOKUP child, multi-batch       */
/*                                                                     */
/* WITH cte AS (SELECT grp, SUM(val) FROM cte_src GROUP BY grp)        */
/* SELECT s.pk, s.grp, cte.total FROM cte_src s JOIN cte ON s.grp=cte.grp */
/*                                                                     */
/* cte_src seeded with NUM_ROWS rows across NUM_GROUPS groups          */
/* (ROWS_PER_GROUP rows each, val=1).  Each group's cte.total ==       */
/* ROWS_PER_GROUP.  No explicit setBatchSize — relies on the default   */
/* batch size being < NUM_ROWS so SCAN_NEXTREQ fires.  Exercises       */
/* Phase 3.3: JoinAggregationState stays CTE_READY across scanFrag     */
/* batch pauses, so CTE_LOOKUPs keep resolving after each SCAN_NEXTREQ.*/
/* NUM_ROWS is large enough to exceed HighlyCongestedLimit (256), so   */
/* DBSPJ's congestion-control path gets exercised too — this fix      */
/* requires execCTE_LOOKUP_CONF/REF to call resumeCongestedNode (same  */
/* as lookup_countSignal does for regular LQH lookups).               */
/* ------------------------------------------------------------------ */
static int
testCteLookupMainMultiBatch(Ndb *ndb, MYSQL *conn)
{
  const int NUM_GROUPS = 100;
  const int ROWS_PER_GROUP = 10;
  const int NUM_ROWS = NUM_GROUPS * ROWS_PER_GROUP;  // 1000
  const Int64 EXPECTED_TOTAL = ROWS_PER_GROUP;  // val=1 per row
  printf("Test 19: real-root + CTE_LOOKUP child, %d rows ... ",
         NUM_ROWS);
  fflush(stdout);

  if (sqlExec(conn, "DELETE FROM cte_src") != 0) {
    printf("FAILED (delete)\n");
    return -1;
  }
  char sql[16384];
  int pos = 0;
  bool first = true;
  for (int pk = 1; pk <= NUM_ROWS; pk++) {
    int grp = ((pk - 1) % NUM_GROUPS) + 1;
    if (first) {
      pos = snprintf(sql, sizeof(sql), "INSERT INTO cte_src VALUES ");
      first = false;
    } else {
      pos += snprintf(sql + pos, sizeof(sql) - pos, ",");
    }
    pos += snprintf(sql + pos, sizeof(sql) - pos, "(%d,%d,1)", pk, grp);
    if (pk % 200 == 0 || pk == NUM_ROWS) {
      if (sqlExec(conn, sql) != 0) {
        printf("FAILED (seed at pk=%d)\n", pk);
        return -1;
      }
      first = true;
      pos = 0;
    }
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") || !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) || !cteAgg.Finalize()) {
    printf("FAILED (cteAgg)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cteAgg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cteAgg);

  /* Main query: real-table scan + CTE_LOOKUP by grp. */
  const NdbQueryTableScanOperationDef *mainScan = qb->scanTable(srcTab);
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScan, "grp"), nullptr
  };
  NdbQueryOptions lookupOpts;
  lookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  if (qb->lookupCte(0, 2, virtTab, cteKey, &lookupOpts) == nullptr) {
    printf("FAILED (lookupCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare)\n");
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 2;  // main scan
  const Uint32 cteOpNo  = queryDef->getNoOfOperations() - 1;  // cte lookup
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbQueryOperation *cteOp  = query->getQueryOperation(cteOpNo);
  /* Request columns in attrId order — NdbReceiver matches incoming
   * attributes to the NdbRecAttr list by order, so reversing produces
   * handle_rec_attrs "attribute in wrong order" errors.
   * cte_src: pk=0, grp=1, val=2.
   * cte_virtual: grp=0, total=1.  CTE_LOOKUP's DBLQH-side emits every
   * column the virtual row has (GB keys + aggregate results), so even
   * if we only care about total, we must also receive grp. */
  NdbRecAttr *raPk  = mainOp->getValue("pk");
  NdbRecAttr *raGrp = mainOp->getValue("grp");
  (void)cteOp->getValue("grp");
  NdbRecAttr *raTotal = cteOp->getValue("total");

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  bool seenPk[NUM_ROWS + 1] = { false };
  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 pk  = raPk->int32_value();
    Int32 grp = raGrp->int32_value();
    Int64 tot = raTotal->int64_value();
    if (pk < 1 || pk > NUM_ROWS || seenPk[pk] ||
        grp != ((pk - 1) % NUM_GROUPS) + 1 || tot != EXPECTED_TOTAL) {
      printf("FAILED (bad row pk=%d grp=%d total=%lld at row %u)\n",
             pk, grp, (long long)tot, rowCount);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    seenPk[pk] = true;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != (Uint32)NUM_ROWS) {
    printf("FAILED (expected %d rows, got %u)\n", NUM_ROWS, rowCount);
    return -1;
  }
  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 20: chained CTEs with multi-batch main-SELECT output           */
/*                                                                     */
/* WITH                                                                */
/*   cte0 AS (SELECT grp, SUM(val) AS total FROM cte_src GROUP BY grp),*/
/*   cte1 AS (SELECT grp, SUM(total) AS total FROM cte0 GROUP BY grp)  */
/* SELECT grp, total FROM cte1                                         */
/*                                                                     */
/* cte_src: NUM_GROUPS distinct groups, 1 row each, val = grp.         */
/* cte0 = {(grp=G, total=G) for G in 1..NUM_GROUPS}                    */
/* cte1 = identity of cte0 (SUM of a singleton group sums to itself).  */
/* Main scanCte(1) with setBatchSize(50) → multiple SCAN_NEXTREQ       */
/* rounds across cte1 groups.  Exercises Phase 3.4: both cte0 and      */
/* cte1 JoinAggregationStates stay CTE_READY across main-SELECT batch  */
/* pauses.                                                             */
/* ------------------------------------------------------------------ */
static int
testCteChainedMultiBatch(Ndb *ndb, MYSQL *conn)
{
  const int NUM_GROUPS = 250;
  const Uint32 BATCH = 50;
  printf("Test 20: chained CTEs, %d groups, batch %u ... ",
         NUM_GROUPS, BATCH);
  fflush(stdout);

  if (sqlExec(conn, "DELETE FROM cte_src") != 0) {
    printf("FAILED (delete)\n");
    return -1;
  }
  char sql[8192];
  int pos = 0;
  bool first = true;
  for (int g = 1; g <= NUM_GROUPS; g++) {
    if (first) {
      pos = snprintf(sql, sizeof(sql), "INSERT INTO cte_src VALUES ");
      first = false;
    } else {
      pos += snprintf(sql + pos, sizeof(sql) - pos, ",");
    }
    pos += snprintf(sql + pos, sizeof(sql) - pos, "(%d,%d,%d)", g, g, g);
    if (g % 100 == 0 || g == NUM_GROUPS) {
      if (sqlExec(conn, sql) != 0) {
        printf("FAILED (seed)\n");
        return -1;
      }
      first = true;
      pos = 0;
    }
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);

  /* cte0 aggregator: groups (grp, SUM(val)) off the real table. */
  NdbAggregator cte0Agg(srcTab);
  if (!cte0Agg.GroupBy("grp") || !cte0Agg.LoadColumn("val", 0) ||
      !cte0Agg.Sum(0, 0) || !cte0Agg.Finalize()) {
    printf("FAILED (cte0Agg)\n");
    return -1;
  }

  /* cte1 aggregator: reads virtual (grp, total) and SUMs total. */
  const NdbDictionary::Column *grpCol   = virtTab->getColumn("grp");
  const NdbDictionary::Column *totalCol = virtTab->getColumn("total");
  NdbAggregator cte1Agg(virtTab);
  if (!cte1Agg.GroupByLinked(0, grpCol) ||
      !cte1Agg.LoadLinkedColumn(1, 0, totalCol) ||
      !cte1Agg.Sum(0, 0) ||
      !cte1Agg.Finalize()) {
    printf("FAILED (cte1Agg)\n");
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();

  /* CTE 0: scanTable(cte_src) + readTuple agg-leaf. */
  qb->beginCteSubtree(0);
  {
    const NdbQueryTableScanOperationDef *scan = qb->scanTable(srcTab);
    const NdbQueryOperand *key[] = { qb->linkedValue(scan, "pk"), nullptr };
    NdbQueryOptions opts;
    opts.setMatchType(NdbQueryOptions::MatchNonNull);
    opts.setAggregation(cte0Agg);
    qb->readTuple(srcTab, key, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(0, srcTab, cte0Agg);

  /* CTE 1: scanCte(0) agg-feeds into cte1's aggregator. */
  qb->beginCteSubtree(1);
  {
    NdbQueryOptions opts;
    opts.setAggregation(cte1Agg);
    qb->scanCte(0, 2, virtTab, &opts);
  }
  qb->endCteSubtree();
  qb->defineCte(1, virtTab, cte1Agg, /*depMask=*/(1ULL << 0));

  /* Main query: scanCte(1) — deliver cte1 rows to the API. */
  if (qb->scanCte(1, 2, virtTab) == nullptr) {
    printf("FAILED (main scanCte(1): %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare)\n");
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  NdbQuery *query = trans->createQuery(queryDef);
  const Uint32 mainOpNo = queryDef->getNoOfOperations() - 1;
  NdbQueryOperation *mainOp = query->getQueryOperation(mainOpNo);
  NdbRecAttr *raGrp   = mainOp->getValue("grp");
  NdbRecAttr *raTotal = mainOp->getValue("total");

  if (mainOp->setBatchSize(BATCH) != 0) {
    printf("FAILED (setBatchSize: %s)\n", query->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    printf("FAILED (execute: %s)\n", trans->getNdbError().message);
    trans->close(); queryDef->destroy();
    return -1;
  }

  bool seen[NUM_GROUPS + 1] = { false };
  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    Int32 g = raGrp->int32_value();
    Int64 t = raTotal->int64_value();
    if (g < 1 || g > NUM_GROUPS || seen[g] || t != (Int64)g) {
      printf("FAILED (bad row grp=%d total=%lld at row %u)\n",
             g, (long long)t, rowCount);
      query->close(); trans->close(); queryDef->destroy();
      return -1;
    }
    seen[g] = true;
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close(); trans->close(); queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  if (rowCount != (Uint32)NUM_GROUPS) {
    printf("FAILED (expected %d rows, got %u)\n", NUM_GROUPS, rowCount);
    return -1;
  }
  printf("OK (%u rows)\n", rowCount);
  return 0;
}

/* ------------------------------------------------------------------ */
/* Test 21: CTE_LOOKUP filter — inline-type opcode, numeric SUM result */
/*                                                                     */
/* Mirrors Test 6 (cte0.total > 40) but builds the filter with         */
/* branch_linked_inline_ge instead of branch_linked_mem_ge.  The new   */
/* opcode (BRANCH_MEM_OP_ARG_INLINE_TYPE = 40) carries type/length/    */
/* charset metadata inline in the program rather than indirecting      */
/* through a registered NDB tableId+attrId — required for filtering    */
/* on synthesized aggregate result types whose virtual columns are     */
/* not registered in DBTUP's tablerec[].  Server-side handler:         */
/* Dbtup::InterpreterContext::handleBranchMemOpArgInlineType.          */
/*                                                                     */
/* Same expected result as Test 6 (3 rows survive the filter): the     */
/* filter program is logically equivalent — only the encoding differs. */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterInlineTypeNumeric(Ndb *ndb, MYSQL *conn)
{
  printf("Test 21: CTE_LOOKUP filter (inline-type, numeric SUM) ... ");
  fflush(stdout);

  /* Tests 19 and 20 reseed cte_src with thousands of rows for their
   * multi-batch coverage and don't restore the original 5-row
   * dataset.  Reseed to the same data insertTestData uses so the
   * expected row count below stays meaningful. */
  if (sqlExec(conn, "DELETE FROM cte_src") != 0 ||
      sqlExec(conn,
              "INSERT INTO cte_src VALUES "
              "(1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,50)") != 0) {
    printf("FAILED (reseed cte_src)\n");
    return -1;
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(CTE_SRC_TABLE);
  dict->invalidateTable(CTE_VIRTUAL_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(CTE_SRC_TABLE);
  const NdbDictionary::Table *virtTab = dict->getTable(CTE_VIRTUAL_TABLE);
  if (srcTab == nullptr || virtTab == nullptr) {
    printf("FAILED (table lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }

  /* CTE 0 = GROUP BY grp, SUM(val) */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("grp") ||
      !cteAgg.LoadColumn("val", 0) ||
      !cteAgg.Sum(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Build filter via the inline-type opcode.  No virtTab indirection;
   * type/length/charset are encoded directly in the program. */
  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(/*table=*/nullptr, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  Int64 threshold = 40;
  const Uint32 REJECT = 0;
  const Uint32 typeId =
      static_cast<Uint32>(NdbDictionary::Column::Bigint);
  const Uint32 columnSizeBytes = 8;  /* SUM result is Uint64-encoded */
  const Uint32 csNumber = 0;          /* numeric, no charset */
  if (filterCode.branch_linked_inline_ge(
          /*position=*/1, typeId, columnSizeBytes, csNumber,
          &threshold, sizeof(threshold), REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", filterCode.getNdbError().message);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) { printf("FAILED (create)\n"); return -1; }

  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  if (cteScanOp == nullptr) {
    printf("FAILED (CTE scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  if (qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts) == nullptr) {
    printf("FAILED (CTE leaf: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, cteAgg) != 0) {
    printf("FAILED (defineCte)\n");
    qb->destroy();
    return -1;
  }

  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "grp"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setInterpretedCode(filterCode);
  if (qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts) == nullptr) {
    printf("FAILED (lookupCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
    queryDef->destroy();
    return -1;
  }
  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbQueryOperation *mainQOp = query->getQueryOperation(3U);
  NdbQueryOperation *cteQOp  = query->getQueryOperation(4U);
  NdbRecAttr *raGrp = nullptr;
  if (mainQOp != nullptr) raGrp = mainQOp->getValue("grp");
  if (cteQOp != nullptr) {
    (void)cteQOp->getValue("grp");
    (void)cteQOp->getValue("total");
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans %d:%s, query %d:%s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  Uint32 rowCount = 0;
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    rowCount++;
    if (raGrp != nullptr) V("  row: main.grp=%d\n", raGrp->int32_value());
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult: %s)\n", query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* Same expected count as Test 6: cte_src has 5 rows; CTE groups
   * are grp=1/total=30, grp=2/total=70, grp=3/total=50.  Filter
   * total > 40 keeps grp=2 and grp=3.  Main scan over cte_src has
   * pk=1/grp=1 (reject), pk=2/grp=1 (reject), pk=3/grp=2 (match),
   * pk=4/grp=2 (match), pk=5/grp=3 (match) → 3 surviving main rows. */
  if (rowCount == 3) {
    printf("OK (%u rows)\n", rowCount);
    return 0;
  }
  printf("FAILED (expected 3 rows, got %u)\n", rowCount);
  return -1;
}

/* ------------------------------------------------------------------ */
/* Test 22: CTE_LOOKUP filter — inline-type opcode, CHAR GB key        */
/*                                                                     */
/* Architectural validation for the inline-type opcode's charset       */
/* round-trip (csNumber → all_charsets[csNumber] in DBTUP).            */
/*                                                                     */
/* Builds a CTE over filter_src (which has a CHAR(8) latin1 column     */
/* `tag`):                                                             */
/*   SELECT tag, COUNT(*) FROM filter_src GROUP BY tag                 */
/* The CTE virt-table is constructed *in memory* (not registered in    */
/* MySQL / NDB dictionary) — the whole point of the inline-type        */
/* opcode is that DBTUP doesn't need a registered tableId for type     */
/* resolution.  RonSQL already uses synthetic virt tables routinely    */
/* (build_cte_virtual_tables); this test mirrors that pattern at the   */
/* block-test layer.                                                   */
/*                                                                     */
/* Result delivery is via aggregator (query->getAggregator()), not     */
/* per-row nextResult.  RonSQL's CTE queries take the same path        */
/* because synthetic virt-table columns leave m_attrSize=0 (no         */
/* public NdbDictionary API populates it from setType+setLength), so   */
/* NdbReceiver's row-buffer sizing (which uses getSizeInBytes()) is    */
/* undersized on the per-row delivery path.  The aggregator-delivery   */
/* path bypasses that buffer entirely, matching the architectural      */
/* shape RonSQL already exercises.  Main aggregator: COUNT(*).         */
/*                                                                     */
/* Filter: tag = 'beta'.  CHAR(8) latin1 → typeId=Char, columnSize=8,  */
/* csNumber=tag's source charset id.  Constant 'beta' padded to 8      */
/* bytes with spaces (PAD-SPACE collation, same as Test 4).            */
/*                                                                     */
/* Expected COUNT: 2 (the two 'beta' rows in filter_src).              */
/* ------------------------------------------------------------------ */

static int
testCteLookupFilterInlineTypeChar(Ndb *ndb, MYSQL * /*conn*/)
{
  printf("Test 22: CTE_LOOKUP filter (inline-type, CHAR GB key) ... ");
  fflush(stdout);

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  dict->invalidateTable(SRC_TABLE);
  const NdbDictionary::Table *srcTab = dict->getTable(SRC_TABLE);
  if (srcTab == nullptr) {
    printf("FAILED (filter_src lookup: %s)\n", dict->getNdbError().message);
    return -1;
  }
  const NdbDictionary::Column *tagSrcCol = srcTab->getColumn("tag");
  if (tagSrcCol == nullptr) {
    printf("FAILED (filter_src.tag lookup)\n");
    return -1;
  }
  CHARSET_INFO *tagCharset = tagSrcCol->getCharset();
  if (tagCharset == nullptr) {
    printf("FAILED (filter_src.tag has no charset)\n");
    return -1;
  }
  const Uint32 csNumber = (Uint32)tagCharset->number;

  /* Build the CTE virt-table in memory.  Schema mirrors the CTE
   * outputs: [tag CHAR(8) PK, n BIGINT].  No MySQL-side CREATE
   * TABLE — the inline-type filter opcode resolves type and
   * charset from inline metadata, so DBTUP does not need to
   * find this table in tablerec[]. */
  NdbDictionary::Table virtTabSyn("__cte_text_virt");
  {
    NdbDictionary::Column tagCol;
    tagCol.setName("tag");
    tagCol.setType(NdbDictionary::Column::Char);
    tagCol.setLength(8);
    tagCol.setCharset(tagCharset);
    tagCol.setPrimaryKey(true);
    tagCol.setNullable(false);
    virtTabSyn.addColumn(tagCol);

    NdbDictionary::Column nCol;
    nCol.setName("n");
    nCol.setType(NdbDictionary::Column::Bigunsigned);
    nCol.setLength(1);
    nCol.setPrimaryKey(false);
    nCol.setNullable(true);
    virtTabSyn.addColumn(nCol);
  }
  /* aggregate() finalises Table-level metadata (PK count etc.) which
   * addColumn does NOT auto-update.  Without this lookupCte's key-count
   * check rejects with QRY_TOO_MANY_KEY_VALUES because
   * getNoOfPrimaryKeys() still reports 0.  Same reason build_cte_
   * virtual_tables() in RonSQL calls aggregate() after addColumn. */
  {
    NdbError vtErr;
    if (virtTabSyn.aggregate(vtErr) != 0) {
      printf("FAILED (virtTab aggregate: %d %s)\n",
             vtErr.code, vtErr.message);
      return -1;
    }
  }
  const NdbDictionary::Table *virtTab = &virtTabSyn;

  /* CTE 0 = GROUP BY tag, COUNT(*) FROM filter_src */
  NdbAggregator cteAgg(srcTab);
  if (!cteAgg.GroupBy("tag") ||
      !cteAgg.LoadUint64(1, 0) ||
      !cteAgg.Count(0, 0) ||
      !cteAgg.Finalize()) {
    printf("FAILED (cteAgg: %s)\n", cteAgg.GetError().err_msg_);
    return -1;
  }

  /* Main aggregator: COUNT(*) over CTE_LOOKUP-matched main rows.
   * No GROUP BY → single result record carrying the count. */
  NdbAggregator mainAgg(srcTab);
  if (!mainAgg.LoadUint64(1, 0) ||
      !mainAgg.Count(0, 0) ||
      !mainAgg.Finalize()) {
    printf("FAILED (mainAgg: %s)\n", mainAgg.GetError().err_msg_);
    return -1;
  }

  /* Build filter via inline-type opcode.  Position 0 is the GB key
   * (tag) — buildCteLinkedBuffer copies GB-key bytes verbatim, so
   * the linked-buffer slot stores 8 bytes of CHAR(8) latin1 data. */
  Uint32 codeBuf[64];
  NdbInterpretedCode filterCode(/*table=*/nullptr, codeBuf,
                                sizeof(codeBuf) / sizeof(codeBuf[0]));
  char tagVal[8];
  memset(tagVal, ' ', sizeof(tagVal));
  memcpy(tagVal, "beta", 4);
  const Uint32 REJECT = 0;
  const Uint32 typeId =
      static_cast<Uint32>(NdbDictionary::Column::Char);
  const Uint32 columnSizeBytes = 8;  /* CHAR(8) is fixed 8 bytes */
  if (filterCode.branch_linked_inline_ne(
          /*position=*/0, typeId, columnSizeBytes, csNumber,
          tagVal, sizeof(tagVal), REJECT) != 0 ||
      filterCode.interpret_exit_ok() != 0 ||
      filterCode.def_label(REJECT) != 0 ||
      filterCode.interpret_exit_nok() != 0 ||
      filterCode.finalise() != 0) {
    printf("FAILED (build filter: %s)\n", filterCode.getNdbError().message);
    return -1;
  }

  NdbQueryBuilder *qb = NdbQueryBuilder::create();
  if (qb == nullptr) { printf("FAILED (create)\n"); return -1; }

  qb->beginCteSubtree(0);
  const NdbQueryTableScanOperationDef *cteScanOp = qb->scanTable(srcTab);
  if (cteScanOp == nullptr) {
    printf("FAILED (CTE scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteJoinKey[] = {
      qb->linkedValue(cteScanOp, "pk"), nullptr
  };
  NdbQueryOptions cteLeafOpts;
  cteLeafOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLeafOpts.setAggregation(cteAgg);
  if (qb->readTuple(srcTab, cteJoinKey, &cteLeafOpts) == nullptr) {
    printf("FAILED (CTE leaf: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->endCteSubtree();
  if (qb->defineCte(0, srcTab, cteAgg) != 0) {
    printf("FAILED (defineCte)\n");
    qb->destroy();
    return -1;
  }

  /* Main: scan filter_src; lookupCte by tag. */
  const NdbQueryTableScanOperationDef *mainScanOp = qb->scanTable(srcTab);
  if (mainScanOp == nullptr) {
    printf("FAILED (main scan: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  const NdbQueryOperand *cteKey[] = {
      qb->linkedValue(mainScanOp, "tag"), nullptr
  };
  NdbQueryOptions cteLookupOpts;
  cteLookupOpts.setMatchType(NdbQueryOptions::MatchNonNull);
  cteLookupOpts.setInterpretedCode(filterCode);
  cteLookupOpts.setAggregation(mainAgg);
  if (qb->lookupCte(0, 2, virtTab, cteKey, &cteLookupOpts) == nullptr) {
    printf("FAILED (lookupCte: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }

  const NdbQueryDef *queryDef = qb->prepare(ndb);
  if (queryDef == nullptr) {
    printf("FAILED (prepare: %s)\n", qb->getNdbError().message);
    qb->destroy();
    return -1;
  }
  qb->destroy();

  NdbTransaction *trans = ndb->startTransaction();
  if (trans == nullptr) {
    printf("FAILED (startTransaction)\n");
    queryDef->destroy();
    return -1;
  }
  NdbQuery *query = trans->createQuery(queryDef);
  if (query == nullptr) {
    printf("FAILED (createQuery: %s)\n", trans->getNdbError().message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  if (trans->execute(NdbTransaction::NoCommit) != 0) {
    const NdbError &tErr = trans->getNdbError();
    const NdbError &qErr = query->getNdbError();
    printf("FAILED (execute: trans %d:%s, query %d:%s)\n",
           tErr.code, tErr.message, qErr.code, qErr.message);
    trans->close();
    queryDef->destroy();
    return -1;
  }

  /* Drain to make the aggregator finalise its result. */
  NdbQuery::NextResultOutcome outcome;
  while ((outcome = query->nextResult(true)) == NdbQuery::NextResult_gotRow) {
    /* nothing — main aggregator carries the count */
  }
  if (outcome == NdbQuery::NextResult_error) {
    printf("FAILED (nextResult drain: %s)\n",
           query->getNdbError().message);
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }

  NdbAggregator *resultAgg = query->getAggregator();
  if (resultAgg == nullptr) {
    printf("FAILED (getAggregator)\n");
    query->close();
    trans->close();
    queryDef->destroy();
    return -1;
  }
  Int64 count = -1;
  NdbAggregator::ResultRecord rec = resultAgg->FetchResultRecord();
  if (!rec.end()) {
    NdbAggregator::Result countRes = rec.FetchAggregationResult();
    count = countRes.data_int64();
  }

  query->close();
  trans->close();
  queryDef->destroy();

  /* filter_src has 6 rows; 2 of them have tag='beta'.  CTE has
   * 3 groups (alpha/beta/gamma); filter accepts only 'beta'.
   * So exactly 2 main rows satisfy MatchNonNull → COUNT=2. */
  if (count == 2) {
    printf("OK (COUNT=%lld)\n", (long long)count);
    return 0;
  }
  printf("FAILED (expected COUNT=2, got %lld)\n", (long long)count);
  return -1;
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

struct TestEntry {
  int number;
  int (*fn)(Ndb *, MYSQL *);
};

static const TestEntry g_tests[] = {
    { 1, testScanFilterAcceptAll },
    { 2, testScanFilterSingleCol },
    { 3, testScanFilterTwoCol },
    { 4, testScanFilterStringEq },
    { 5, testScanFilterRejectAll },
    { 6, testCteLookupFilterSingleCol },
    { 7, testCteLookupFilterNoMatch },
    { 8, testCteLookupFilterBranchOpcode },
    { 9, testCteLookupFilterInAggFeed },
    { 10, testCteLookupFilterLeftJoin },
    { 11, testCteLookupFilterMultipleCte },
    { 12, testCteScanFilterRoot },
    { 13, testCteScanFilterAggFeed },
    { 14, testCteScanFilterEmptyResult },
    { 15, testCteScanFilterBatchBoundary },
    { 16, testCteScanRootLargeResult },
    { 17, testCteScanRootSmallBatch },
    { 18, testCteScanRootEarlyClose },
    { 19, testCteLookupMainMultiBatch },
    { 20, testCteChainedMultiBatch },
    { 21, testCteLookupFilterInlineTypeNumeric },
    { 22, testCteLookupFilterInlineTypeChar },
};
static const size_t g_test_count = sizeof(g_tests) / sizeof(g_tests[0]);

int main(int argc, char **argv)
{
  const char *connectString = "localhost:1186";
  int mysqlPort = 3306;
  int onlyTest = -1;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0 && i + 1 < argc)
      connectString = argv[++i];
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc)
      mysqlPort = atoi(argv[++i]);
    else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0)
      verbose = true;
    else if (strcmp(argv[i], "--only") == 0 && i + 1 < argc)
      onlyTest = atoi(argv[++i]);
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      printf("Usage: %s -c <connect_string> -m <mysql_port> [-v] "
             "[--only N]\n", argv[0]);
      printf("  --only N    run only test number N\n");
      return 0;
    }
  }

  if (onlyTest != -1) {
    bool found = false;
    for (size_t i = 0; i < g_test_count; i++) {
      if (g_tests[i].number == onlyTest) { found = true; break; }
    }
    if (!found) {
      fprintf(stderr, "No such test: %d (valid: 1..%zu)\n",
              onlyTest, g_test_count);
      return 1;
    }
  }

  printf("=== testCteNdbApiFilter ===\n");
  printf("Connect: %s, MySQL port: %d\n", connectString, mysqlPort);
  if (onlyTest != -1) printf("Filter: --only %d\n", onlyTest);
  printf("\n");

  /* Redirect stdout to stderr; only PASSED/FAILED goes to real stdout */
  int mtr_fd = dup(fileno(stdout));
  dup2(fileno(stderr), fileno(stdout));

  ndb_init();
  int exitCode = 0;

  {
    Ndb_cluster_connection clusterConn(connectString);
    if (clusterConn.connect(12, 5, 1) != 0) {
      fprintf(stderr, "Cannot connect to cluster\n");
      exitCode = 1;
    }
    else if (clusterConn.wait_until_ready(30, 0) < 0) {
      fprintf(stderr, "Cluster not ready\n");
      exitCode = 1;
    }
    else {
      MYSQL *conn = connectMysql(mysqlPort);
      if (conn == nullptr) {
        exitCode = 1;
      } else {
        Ndb ndb(&clusterConn, TEST_DB);
        if (ndb.init() != 0) {
          fprintf(stderr, "Ndb::init failed: %s\n",
                  ndb.getNdbError().message);
          exitCode = 1;
        }
        else if (createTestTables(conn) != 0 ||
                 insertTestData(conn) != 0) {
          exitCode = 1;
        }
        else {
          for (size_t i = 0; i < g_test_count; i++) {
            if (onlyTest != -1 && g_tests[i].number != onlyTest) continue;
            if (g_tests[i].fn(&ndb, conn) != 0) exitCode = 1;
          }
        }

        dropTestTables(conn);
        mysql_close(conn);
      }
    }
  }

  ndb_end(0);

  if (exitCode == 0) {
    write(mtr_fd, "PASSED\n", 7);
  }
  close(mtr_fd);

  return exitCode;
}
