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

  if (sqlExec(conn,
      "CREATE TABLE filter_src ("
      "  pk INT NOT NULL PRIMARY KEY,"
      "  grp INT NOT NULL,"
      "  val BIGINT NOT NULL,"
      "  tag CHAR(8) NOT NULL"
      ") ENGINE=NDB DEFAULT CHARSET=latin1") != 0) return -1;

  return 0;
}

static int
insertTestData(MYSQL *conn)
{
  return sqlExec(conn,
      "INSERT INTO filter_src VALUES "
      "(1,1,10,'alpha'),(2,1,20,'alpha'),"
      "(3,2,30,'beta'), (4,2,40,'beta'),"
      "(5,3,50,'gamma'),(6,3,60,'gamma')");
}

static void
dropTestTables(MYSQL *conn)
{
  sqlExec(conn, "DROP TABLE IF EXISTS filter_src");
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
