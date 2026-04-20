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
