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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
*/

/*
 * testInterpreterTypedRegs
 *
 * Focused NDB API coverage for the DBTUP normal interpreter typed-register
 * semantics introduced for RONDB-1050 Phase I.18.  The test intentionally
 * drives the real interpreter through NdbInterpretedCode instead of copying
 * helper logic into a standalone harness.
 *
 * Usage: testInterpreterTypedRegs -c <connect_string> -m <mysql_port> [-v]
 */

#include <ndb_global.h>
#include <ndb_opts.h>
#include <NdbApi.hpp>
#include <NdbInterpretedCode.hpp>

#include <mysql.h>

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <unistd.h>

static bool verbose = false;
#define V(...) do { if (verbose) printf(__VA_ARGS__); } while (0)

static const char *TEST_DB = "test";
static const char *TABLE_NAME = "interp_typed_regs";
static const char *BOUNDARY_TABLE_NAME = "interp_typed_regs_iwidth";

static MYSQL *
connectMysql(int port)
{
  MYSQL *conn = mysql_init(NULL);
  if (conn == NULL) return NULL;
  if (mysql_real_connect(conn, "127.0.0.1", "root", "",
                         TEST_DB, port, NULL, 0) == NULL) {
    fprintf(stderr, "mysql_real_connect: %s\n", mysql_error(conn));
    mysql_close(conn);
    return NULL;
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

static void
dropTestTable(MYSQL *conn)
{
  (void)sqlExec(conn, "DROP TABLE IF EXISTS interp_typed_regs_iwidth");
  (void)sqlExec(conn, "DROP TABLE IF EXISTS interp_typed_regs");
}

static int
createBoundaryTable(MYSQL *conn)
{
  if (sqlExec(conn,
      "CREATE TABLE interp_typed_regs_iwidth ("
      "  pk INT NOT NULL,"
      "  s_tiny TINYINT NOT NULL,"
      "  s_tiny2 TINYINT NOT NULL,"
      "  s_small SMALLINT NOT NULL,"
      "  s_small2 SMALLINT NOT NULL,"
      "  s_medium MEDIUMINT NOT NULL,"
      "  s_medium2 MEDIUMINT NOT NULL,"
      "  s_int INT NOT NULL,"
      "  s_int2 INT NOT NULL,"
      "  s_big BIGINT NOT NULL,"
      "  s_big2 BIGINT NOT NULL,"
      "  u_tiny TINYINT UNSIGNED NOT NULL,"
      "  u_tiny2 TINYINT UNSIGNED NOT NULL,"
      "  u_small SMALLINT UNSIGNED NOT NULL,"
      "  u_small2 SMALLINT UNSIGNED NOT NULL,"
      "  u_medium MEDIUMINT UNSIGNED NOT NULL,"
      "  u_medium2 MEDIUMINT UNSIGNED NOT NULL,"
      "  u_int INT UNSIGNED NOT NULL,"
      "  u_int2 INT UNSIGNED NOT NULL,"
      "  u_big BIGINT UNSIGNED NOT NULL,"
      "  u_big2 BIGINT UNSIGNED NOT NULL,"
      "  PRIMARY KEY USING HASH (pk)"
      ") ENGINE=NDB") != 0) return -1;

  return sqlExec(conn,
      "INSERT INTO interp_typed_regs_iwidth VALUES"
      " (1, -128, -128, -32768, -32768, -8388608, -8388608,"
      "     -2147483648, -2147483648,"
      "     -9223372036854775808, -9223372036854775808,"
      "     0, 0, 0, 0, 0, 0, 0, 0, 0, 0),"
      " (2, -127, -127, -32767, -32767, -8388607, -8388607,"
      "     -2147483647, -2147483647,"
      "     -9223372036854775807, -9223372036854775807,"
      "     1, 1, 1, 1, 1, 1, 1, 1, 1, 1),"
      " (3, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,"
      "     128, 128, 32768, 32768, 8388608, 8388608,"
      "     2147483648, 2147483648,"
      "     9223372036854775808, 9223372036854775808),"
      " (4, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,"
      "     254, 255, 65534, 65535, 16777214, 16777215,"
      "     4294967294, 4294967295,"
      "     18446744073709551614, 18446744073709551615),"
      " (5, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0,"
      "     255, 255, 65535, 65535, 16777215, 16777215,"
      "     4294967295, 4294967295,"
      "     18446744073709551615, 18446744073709551615),"
      " (6, 126, 126, 32766, 32766, 8388606, 8388606,"
      "     2147483646, 2147483646,"
      "     9223372036854775806, 9223372036854775806,"
      "     42, 43, 4242, 4243, 424242, 424243,"
      "     42424242, 42424243, 4242424242, 4242424243),"
      " (7, 127, 127, 32767, 32767, 8388607, 8388607,"
      "     2147483647, 2147483647,"
      "     9223372036854775807, 9223372036854775807,"
      "     255, 255, 65535, 65535, 16777215, 16777215,"
      "     4294967295, 4294967295,"
      "     18446744073709551615, 18446744073709551615)");
}

static int
createTestTable(MYSQL *conn)
{
  dropTestTable(conn);
  if (sqlExec(conn,
      "CREATE TABLE interp_typed_regs ("
      "  pk INT NOT NULL,"
      "  s_tiny TINYINT NOT NULL,"
      "  s_small SMALLINT NOT NULL,"
      "  s_medium MEDIUMINT NOT NULL,"
      "  s_int INT NOT NULL,"
      "  s_int2 INT NOT NULL,"
      "  s_big BIGINT NOT NULL,"
      "  u_tiny TINYINT UNSIGNED NOT NULL,"
      "  u_small SMALLINT UNSIGNED NOT NULL,"
      "  u_medium MEDIUMINT UNSIGNED NOT NULL,"
      "  u_int INT UNSIGNED NOT NULL,"
      "  u_big BIGINT UNSIGNED NOT NULL,"
      "  f_val FLOAT NOT NULL,"
      "  d_val DOUBLE NOT NULL,"
      "  n_int INT NULL,"
      "  n_double DOUBLE NULL,"
      "  f_val2 FLOAT NOT NULL,"
      "  d_val2 DOUBLE NOT NULL,"
      "  n_float FLOAT NULL,"
      "  c_val CHAR(8) NOT NULL,"
      "  v_val VARCHAR(20) NOT NULL,"
      "  bit_val BIT(8) NOT NULL,"
      "  PRIMARY KEY USING HASH (pk)"
      ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
      "INSERT INTO interp_typed_regs VALUES"
      " (1, -5, -1000, -100000, -1000000, 0, -7,"
      "     5, 1000, 100000, 4000000000, 9223372036854775808,"
      "     -1.5, 4.5, NULL, NULL,"
      "     -1.5, 4.5, NULL, 'alpha', '12345', b'00001111'),"
      " (2,  5,   500,   50000,   500000, 500000, -1,"
      "    20, 2000, 200000, 3000000000, 10,"
      "      2.5, 8.0, 7, 8.0,"
      "      2.5, 8.0, 8.0, 'beta', '77', b'11110000'),"
      " (3, 20, 20000,  500000, 20000000, 10000000, 9223372036854775807,"
      "   250, 65000, 8000000, 1, 9223372036854775813,"
      "     12.5, 16.25, -3, 16.25,"
      "     16.25, 12.5, 16.25, 'gamma', '-8', b'10101010'),"
      " (4, -1,    -1,      -1,       -1, 2, 42,"
      "     1,    1,      1, 2, 42,"
      "      0.0, -2.0, 0, -2.0,"
      "     -0.0, 0.0, -0.0, 'delta', '0', b'00000000'),"
      " (5, -128, -32768, -8388608, -2147483648, -1, -9223372036854775808,"
      "     0, 0, 0, 0, 0,"
      "     -3.25, -64.0, NULL, NULL,"
      "     -3.25, -64.0, NULL, 'epsilon', '922', b'01010101'),"
      " (6, 127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807,"
      "     255, 65535, 16777215, 4294967295, 18446744073709551615,"
      "     3.25, 64.0, 2147483647, 64.0,"
      "     64.0, 3.25, 64.0, 'zeta', '42', b'11111111')"
      ) != 0) return -1;

  return createBoundaryTable(conn);
}

static const NdbDictionary::Table *
getNamedTable(Ndb *ndb, const char *name)
{
  const NdbDictionary::Table *tab =
      ndb->getDictionary()->getTable(name);
  if (tab == NULL) {
    printf("(getTable %s: %s) ",
           name, ndb->getDictionary()->getNdbError().message);
  }
  return tab;
}

static const NdbDictionary::Table *
getTable(Ndb *ndb)
{
  return getNamedTable(ndb, TABLE_NAME);
}

static Uint32
attrId(const NdbDictionary::Table *tab, const char *name)
{
  const NdbDictionary::Column *col = tab->getColumn(name);
  if (col == NULL) return 0xFFFFFFFF;
  return (Uint32)col->getColumnNo();
}

static int
finishAcceptReject(NdbInterpretedCode *code, Uint32 acceptLabel)
{
  if (code->interpret_exit_nok() != 0 ||
      code->def_label(acceptLabel) != 0 ||
      code->interpret_exit_ok() != 0 ||
      code->finalise() != 0) {
    return -1;
  }
  return 0;
}

static int
finishRuntimeErrorProgram(NdbInterpretedCode *code)
{
  if (code->interpret_exit_ok() != 0 ||
      code->finalise() != 0) {
    return -1;
  }
  return 0;
}

static int
runScanWithFilter(Ndb *ndb,
                  const NdbDictionary::Table *tab,
                  const NdbInterpretedCode *filterCode,
                  std::vector<int> *pksOut,
                  bool expectExecuteError)
{
  NdbTransaction *trans = ndb->startTransaction();
  if (trans == NULL) {
    printf("(startTransaction: %s) ", ndb->getNdbError().message);
    return -1;
  }

  NdbScanOperation *scan = trans->getNdbScanOperation(tab);
  if (scan == NULL) {
    printf("(getNdbScanOperation: %s) ", trans->getNdbError().message);
    trans->close();
    return -1;
  }

  if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
    printf("(readTuples: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  NdbRecAttr *pkAttr = scan->getValue("pk");
  if (pkAttr == NULL) {
    printf("(getValue: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  if (filterCode != NULL &&
      scan->setInterpretedCode(filterCode) != 0) {
    printf("(setInterpretedCode: %s) ", scan->getNdbError().message);
    trans->close();
    return -1;
  }

  int execRc = trans->execute(NdbTransaction::NoCommit);
  if (expectExecuteError) {
    if (execRc != 0) {
      V("expected execute error: %u %s\n",
        trans->getNdbError().code, trans->getNdbError().message);
      trans->close();
      return 0;
    }

    int rc;
    while ((rc = scan->nextResult(true)) == 0) {}
    if (rc != 1) {
      V("expected scan error: %u %s\n",
        scan->getNdbError().code, scan->getNdbError().message);
      trans->close();
      return 0;
    }

    printf("(expected interpreter error) ");
    trans->close();
    return -1;
  }
  if (execRc != 0) {
    printf("(execute: %u %s) ",
           trans->getNdbError().code, trans->getNdbError().message);
    trans->close();
    return -1;
  }

  pksOut->clear();
  int rc;
  while ((rc = scan->nextResult(true)) == 0) {
    pksOut->push_back(pkAttr->int32_value());
  }
  if (rc != 1) {
    printf("(nextResult: %u %s) ",
           scan->getNdbError().code, scan->getNdbError().message);
    trans->close();
    return -1;
  }

  trans->close();
  std::sort(pksOut->begin(), pksOut->end());
  return 0;
}

static bool
samePks(const std::vector<int>& got, const int *expected, size_t nExpected)
{
  if (got.size() != nExpected) return false;
  for (size_t i = 0; i < nExpected; i++) {
    if (got[i] != expected[i]) return false;
  }
  return true;
}

static int
expectPks(const char *name, Ndb *ndb, const NdbDictionary::Table *tab,
          NdbInterpretedCode *code, const int *expected, size_t nExpected)
{
  std::vector<int> got;
  printf("%s ... ", name);
  if (runScanWithFilter(ndb, tab, code, &got, false) != 0) {
    printf("FAILED\n");
    return -1;
  }
  if (!samePks(got, expected, nExpected)) {
    printf("FAILED (expected");
    for (size_t i = 0; i < nExpected; i++) printf(" %d", expected[i]);
    printf(", got");
    for (size_t i = 0; i < got.size(); i++) printf(" %d", got[i]);
    printf(")\n");
    return -1;
  }
  printf("OK\n");
  return 0;
}

static int
expectAllPks(const char *name, Ndb *ndb, const NdbDictionary::Table *tab,
             NdbInterpretedCode *code)
{
  static const int expected[] = { 1, 2, 3, 4, 5, 6 };
  return expectPks(name, ndb, tab, code, expected, 6);
}

static int
expectNoPks(const char *name, Ndb *ndb, const NdbDictionary::Table *tab,
            NdbInterpretedCode *code)
{
  return expectPks(name, ndb, tab, code, NULL, 0);
}

static int
expectRuntimeError(const char *name, Ndb *ndb,
                   const NdbDictionary::Table *tab,
                   NdbInterpretedCode *code)
{
  std::vector<int> got;
  printf("%s ... ", name);
  if (runScanWithFilter(ndb, tab, code, &got, true) != 0) {
    printf("FAILED\n");
    return -1;
  }
  printf("OK\n");
  return 0;
}

static int
buildReadCompareConst(NdbInterpretedCode *code,
                      Uint32 attr, Uint16 constant,
                      int (NdbInterpretedCode::*branch)(Uint32, Uint32, Uint32))
{
  const Uint32 ACCEPT = 0;
  if (code->read_attr(0, attr) != 0 ||
      code->load_const_u16(1, constant) != 0 ||
      (code->*branch)(0, 1, ACCEPT) != 0 ||
      finishAcceptReject(code, ACCEPT) != 0) {
    return -1;
  }
  return 0;
}

static int
loadMemoryConst(NdbInterpretedCode *code,
                Uint32 offsetReg,
                Uint32 sizeReg,
                const char *mem,
                Uint32 memSize)
{
  if (code->load_const_u16(offsetReg, 0) != 0 ||
      code->load_const_u16(sizeReg, memSize) != 0 ||
      code->load_const_mem(offsetReg, sizeReg, memSize, mem) != 0) {
    return -1;
  }
  return 0;
}

static int
expectReadCompareU64(const char *name,
                     Ndb *ndb,
                     const NdbDictionary::Table *tab,
                     Uint32 attr,
                     Uint64 constant,
                     int (NdbInterpretedCode::*branch)(Uint32, Uint32, Uint32),
                     const int *expected,
                     size_t nExpected)
{
  Uint32 buf[128];
  NdbInterpretedCode code(tab, buf, 128);
  if (code.read_attr(0, attr) != 0 ||
      code.load_const_u64(1, constant) != 0 ||
      (code.*branch)(0, 1, 0) != 0 ||
      finishAcceptReject(&code, 0) != 0) {
    printf("%s ... FAILED (build)\n", name);
    return -1;
  }
  return expectPks(name, ndb, tab, &code, expected, nExpected);
}

static int
expectColumnEq(const char *name,
               Ndb *ndb,
               const NdbDictionary::Table *tab,
               Uint32 attr1,
               Uint32 attr2,
               const int *expected,
               size_t nExpected)
{
  Uint32 buf[128];
  NdbInterpretedCode code(tab, buf, 128);
  if (code.branch_col_eq(attr1, attr2, 0) != 0 ||
      finishAcceptReject(&code, 0) != 0) {
    printf("%s ... FAILED (build)\n", name);
    return -1;
  }
  return expectPks(name, ndb, tab, &code, expected, nExpected);
}

static int
expectUnsignedBigintMax(const char *name,
                        Ndb *ndb,
                        const NdbDictionary::Table *tab,
                        Uint32 uBigAttr,
                        Uint32 uBig2Attr,
                        Uint32 uIntAttr,
                        const int *expected,
                        size_t nExpected)
{
  const Uint32 REJECT = 0;
  const Uint32 ACCEPT = 1;
  Uint32 buf[160];
  NdbInterpretedCode code(tab, buf, 160);
  if (code.read_attr(0, uBigAttr) != 0 ||
      code.read_attr(1, uBig2Attr) != 0 ||
      code.branch_ne(0, 1, REJECT) != 0 ||
      code.load_const_u64(2, 9223372036854775807ULL) != 0 ||
      code.branch_le(0, 2, REJECT) != 0 ||
      code.read_attr(3, uIntAttr) != 0 ||
      code.load_const_u64(4, 4294967295ULL) != 0 ||
      code.branch_ne(3, 4, REJECT) != 0 ||
      code.branch_label(ACCEPT) != 0 ||
      code.def_label(REJECT) != 0 ||
      code.interpret_exit_nok() != 0 ||
      code.def_label(ACCEPT) != 0 ||
      code.interpret_exit_ok() != 0 ||
      code.finalise() != 0) {
    printf("%s ... FAILED (build)\n", name);
    return -1;
  }
  return expectPks(name, ndb, tab, &code, expected, nExpected);
}

static int
testSignedSubBigintLoads(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 tinyAttr = attrId(tab, "s_tiny");
  Uint32 smallAttr = attrId(tab, "s_small");
  Uint32 mediumAttr = attrId(tab, "s_medium");
  Uint32 intAttr = attrId(tab, "s_int");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const int expected[] = { 1, 4, 5 };
    if (buildReadCompareConst(&code, tinyAttr, 0,
                              &NdbInterpretedCode::branch_lt) != 0 ||
        expectPks("Test 1a: TINYINT sign extension", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const int expected[] = { 1, 4, 5 };
    if (buildReadCompareConst(&code, smallAttr, 0,
                              &NdbInterpretedCode::branch_lt) != 0 ||
        expectPks("Test 1b: SMALLINT sign extension", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const int expected[] = { 1, 4, 5 };
    if (buildReadCompareConst(&code, mediumAttr, 0,
                              &NdbInterpretedCode::branch_lt) != 0 ||
        expectPks("Test 1c: MEDIUMINT sign extension", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const int expected[] = { 1, 4, 5 };
    if (buildReadCompareConst(&code, intAttr, 0,
                              &NdbInterpretedCode::branch_lt) != 0 ||
        expectPks("Test 1d: INT sign extension", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  return rc;
}

static int
testMixedSignedUnsignedCompare(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sBigAttr = attrId(tab, "s_big");
  Uint32 uBigAttr = attrId(tab, "u_big");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 2, 3, 5, 6 };
    if (code.read_attr(0, uBigAttr) != 0 ||
        code.read_attr(1, sBigAttr) != 0 ||
        code.branch_gt(0, 1, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 2a: BIGUNSIGNED greater than BIGINT max",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 2, 3, 5, 6 };
    if (code.read_attr(0, sBigAttr) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.branch_lt(0, 1, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 2b: negative signed less than unsigned",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }
  return rc;
}

static int
testDoubleCompareAndArithmetic(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 dAttr = attrId(tab, "d_val");
  Uint32 sIntAttr = attrId(tab, "s_int");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.load_double_const(1, 10.5) != 0 ||
        code.branch_gt(0, 1, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 3a: DOUBLE column vs LOAD_DOUBLE_CONST",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.load_const_u16(1, 2) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, 18.0) != 0 ||
        code.branch_gt(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 3b: DOUBLE arithmetic promotion",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 5 };
    if (code.read_attr(0, sIntAttr) != 0 ||
        code.load_const_u16(1, 5) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.load_const_u16(3, 0) != 0 ||
        code.branch_lt(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 3c: signed integer arithmetic",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  return rc;
}

static int
testNullBranches(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 nIntAttr = attrId(tab, "n_int");
  Uint32 nDoubleAttr = attrId(tab, "n_double");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 5 };
    if (code.read_attr(0, nIntAttr) != 0 ||
        code.branch_eq_null(0, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 4a: nullable INT branch_eq_null",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 2, 3, 4, 6 };
    if (code.read_attr(0, nDoubleAttr) != 0 ||
        code.branch_ne_null(0, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 4b: nullable DOUBLE branch_ne_null",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }
  return rc;
}

static int
testBitwiseAndShifts(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 uIntAttr = attrId(tab, "u_int");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 6 };
    if (code.read_attr(0, uIntAttr) != 0 ||
        code.and_const_reg(1, 0, 0x8000) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_gt(1, 2, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 5a: unsigned bitwise AND",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    if (code.load_const_u64(0, 1) != 0 ||
        code.lshift_const_reg(1, 0, 63) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_lt(1, 2, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 5b: signed left shift by 63",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 1) != 0 ||
        code.lshift_const_reg(1, 0, 64) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 5c: shift by 64 rejected",
                           ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testMemorySpillWriters(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    Uint64 doubleBits;
    double d = 8.0;
    memcpy(&doubleBits, &d, 8);
    if (code.load_double_const(0, 8.0) != 0 ||
        code.write_reg_to_mem_any_const(0, 0) != 0 ||
        code.read_int64_to_reg_const(1, 0) != 0 ||
        code.load_const_u64(2, doubleBits) != 0 ||
        code.branch_eq(1, 2, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 6a: WRITE_REG_TO_MEM_ANY accepts DOUBLE",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_double_const(0, 8.0) != 0 ||
        code.write_int64_reg_to_mem_const(0, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 6b: strict WRITE_INT64 rejects DOUBLE",
                           ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testBoundaryComparisons(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sBigAttr = attrId(tab, "s_big");
  Uint32 sIntAttr = attrId(tab, "s_int");
  Uint32 uBigAttr = attrId(tab, "u_big");
  Uint32 uIntAttr = attrId(tab, "u_int");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 2, 5 };
    if (code.read_attr(0, sBigAttr) != 0 ||
        code.load_const_u16(1, 0) != 0 ||
        code.branch_lt(0, 1, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 7a: signed BIGINT negative boundary",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 3, 6 };
    if (code.read_attr(0, uBigAttr) != 0 ||
        code.load_const_u64(1, 9223372036854775807ULL) != 0 ||
        code.branch_gt(0, 1, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 7b: unsigned BIGINT above INT64_MAX",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 2, 4, 5, 6 };
    if (code.read_attr(0, uIntAttr) != 0 ||
        code.read_attr(1, sIntAttr) != 0 ||
        code.branch_gt(0, 1, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 7c: UINT32 vs signed INT boundaries",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }
  return rc;
}

static int
testArithmeticErrorHandlers(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u16(0, 1) != 0 ||
        code.load_const_u16(1, 0) != 0 ||
        code.div_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8a: integer divide by zero",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u16(0, 1) != 0 ||
        code.load_const_u16(1, 0) != 0 ||
        code.mod_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8b: integer modulo by zero",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_double_const(0, 1.25) != 0 ||
        code.load_double_const(1, 0.0) != 0 ||
        code.div_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8c: double divide by zero",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775807ULL) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8d: signed add overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775808ULL) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.sub_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8e: signed subtract overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testRegisterErrorHandlers(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.branch_eq(0, 1, 0) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 9a: branch on uninitialised registers",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_double_const(0, 3.5) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.and_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 9b: bitwise rejects DOUBLE",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_null(0) != 0 ||
        code.write_reg_to_mem_any_const(0, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 9c: WRITE_REG_TO_MEM_ANY rejects NULL",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_null(0) != 0 ||
        code.read_uint8_to_reg_reg(1, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 9d: memory-offset register NULL",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u16(0, 1) != 0 ||
        code.write_uint8_reg_to_mem_const(0, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 9e: strict WRITE_UINT8 rejects signed",
                           ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testMixedTypeOperations(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sIntAttr = attrId(tab, "s_int");
  Uint32 sTinyAttr = attrId(tab, "s_tiny");
  Uint32 uIntAttr = attrId(tab, "u_int");
  Uint32 uTinyAttr = attrId(tab, "u_tiny");
  Uint32 dAttr = attrId(tab, "d_val");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 2, 3, 6 };
    if (code.read_attr(0, uIntAttr) != 0 ||
        code.read_attr(1, sIntAttr) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.branch_gt(2, 0, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 10a: UINT32 plus signed INT mixed arithmetic",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.read_attr(1, uTinyAttr) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, 100.0) != 0 ||
        code.branch_gt(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 10b: DOUBLE plus unsigned tiny promotion",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.read_attr(1, dAttr) != 0 ||
        code.mul_reg(2, 0, 1) != 0 ||
        code.load_const_u16(3, 0) != 0 ||
        code.branch_lt(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 10c: signed tiny times DOUBLE",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 4, 5 };
    if (code.read_attr(0, uIntAttr) != 0 ||
        code.read_attr(1, sIntAttr) != 0 ||
        code.sub_reg(2, 0, 1) != 0 ||
        code.branch_gt(2, 0, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 10d: UINT32 minus signed INT mixed arithmetic",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  return rc;
}

static int
testConstBranchesAndShifts(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sTinyAttr = attrId(tab, "s_tiny");
  Uint32 uTinyAttr = attrId(tab, "u_tiny");
  Uint32 uBigAttr = attrId(tab, "u_big");
  Uint32 buf[128];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 128);
    const int expected[] = { 1, 4, 5 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.branch_lt_const(0, 0, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 11a: branch_lt_const on signed tiny",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const int expected[] = { 2, 3, 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.branch_ge_const(0, 20, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 11b: branch_ge_const on unsigned tiny",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.add_const_reg(1, 0, 10) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.branch_gt(1, 2, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 11c: add_const_reg on signed tiny",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    if (code.load_const_u64(0, 18446744073709551608ULL) != 0 ||
        code.rshift_const_reg(1, 0, 1) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_lt(1, 2, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 11d: signed arithmetic right shift",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    const Uint32 ACCEPT = 0;
    const int expected[] = { 1, 3, 6 };
    if (code.read_attr(0, uBigAttr) != 0 ||
        code.rshift_const_reg(1, 0, 63) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_gt(1, 2, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectPks("Test 11e: unsigned logical right shift",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  return rc;
}

static int
testMemoryTypedLoadsAndWrites(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 buf[160];
  int rc = 0;
  char mem[16];
  memset(mem, 0, sizeof(mem));
  mem[0] = (char)0xFE;
  mem[1] = (char)0x34;
  mem[2] = (char)0x12;
  mem[4] = (char)0x78;
  mem[5] = (char)0x56;
  mem[6] = (char)0x34;
  mem[7] = (char)0x12;
  mem[8] = (char)0xFE;
  mem[9] = (char)0xFF;
  mem[10] = (char)0xFF;
  mem[11] = (char)0xFF;
  mem[12] = (char)0xFF;
  mem[13] = (char)0xFF;
  mem[14] = (char)0xFF;
  mem[15] = (char)0xFF;

  {
    NdbInterpretedCode code(tab, buf, 160);
    const Uint32 ACCEPT = 0;
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint8_to_reg_const(2, 0) != 0 ||
        code.load_const_u16(3, 250) != 0 ||
        code.branch_gt(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 12a: READ_UINT8_MEM_TO_REG unsigned value",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const Uint32 ACCEPT = 0;
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint16_to_reg_const(2, 1) != 0 ||
        code.load_const_u16(3, 0x1234) != 0 ||
        code.branch_eq(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 12b: READ_UINT16_MEM_TO_REG",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const Uint32 ACCEPT = 0;
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint32_to_reg_const(2, 4) != 0 ||
        code.load_const_u32(3, 0x12345678) != 0 ||
        code.branch_eq(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 12c: READ_UINT32_MEM_TO_REG",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const Uint32 ACCEPT = 0;
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_int64_to_reg_const(2, 8) != 0 ||
        code.load_const_u16(3, 0) != 0 ||
        code.branch_lt(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 12d: READ_INT64_MEM_TO_REG signed value",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const Uint32 ACCEPT = 0;
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint8_to_reg_const(2, 0) != 0 ||
        code.write_uint8_reg_to_mem_const(2, 12) != 0 ||
        code.read_uint8_to_reg_const(3, 12) != 0 ||
        code.branch_eq(2, 3, ACCEPT) != 0 ||
        finishAcceptReject(&code, ACCEPT) != 0 ||
        expectAllPks("Test 12e: strict WRITE_UINT8 accepts UINT register",
                     ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testRegisterBranchCoverage(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sTinyAttr = attrId(tab, "s_tiny");
  Uint32 uTinyAttr = attrId(tab, "u_tiny");
  Uint32 buf[160];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.load_const_u16(1, 5) != 0 ||
        code.branch_eq(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13a: branch_eq register", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 3, 4, 5, 6 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.load_const_u16(1, 5) != 0 ||
        code.branch_ne(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13b: branch_ne register", ndb, tab,
                  &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 2, 4, 5 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.load_const_u16(1, 5) != 0 ||
        code.branch_le(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13c: branch_le register", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2, 3, 6 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.load_const_u16(1, 5) != 0 ||
        code.branch_ge(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13d: branch_ge register", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.branch_gt_const(0, 50, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13e: branch_gt_const unsigned", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 4, 5 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.branch_le_const(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13f: branch_le_const unsigned", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.branch_eq_const(0, 20, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13g: branch_eq_const unsigned", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 3, 4, 5, 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.branch_ne_const(0, 20, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 13h: branch_ne_const unsigned", ndb, tab,
                  &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.branch_label(0) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(0) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectAllPks("Test 13i: branch_label unconditional",
                     ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testArithmeticOpcodeCoverage(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sTinyAttr = attrId(tab, "s_tiny");
  Uint32 uTinyAttr = attrId(tab, "u_tiny");
  Uint32 buf[160];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 2, 4, 5 };
    if (code.read_attr(0, sTinyAttr) != 0 ||
        code.sub_const_reg(1, 0, 10) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_lt(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14a: sub_const_reg", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.mul_const_reg(1, 0, 2) != 0 ||
        code.load_const_u16(2, 400) != 0 ||
        code.branch_gt(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14b: mul_const_reg unsigned", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.div_const_reg(1, 0, 5) != 0 ||
        code.load_const_u16(2, 4) != 0 ||
        code.branch_eq(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14c: div_const_reg unsigned", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2, 3, 5 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.mod_const_reg(1, 0, 10) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_eq(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14d: mod_const_reg unsigned", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 4, 5 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.or_const_reg(1, 0, 1) != 0 ||
        code.load_const_u16(2, 1) != 0 ||
        code.branch_eq(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14e: or_const_reg unsigned", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.xor_const_reg(1, 0, 255) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_eq(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14f: xor_const_reg unsigned", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.or_reg(2, 0, 1) != 0 ||
        code.load_const_u16(3, 250) != 0 ||
        code.branch_gt(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14g: or_reg unsigned", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.load_const_u16(1, 255) != 0 ||
        code.xor_reg(2, 0, 1) != 0 ||
        code.load_const_u16(3, 0) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14h: xor_reg unsigned", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_u64(0, 0) != 0 ||
        code.not_reg(1, 0) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.branch_lt(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 14i: not_reg unsigned",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, uTinyAttr) != 0 ||
        code.move_reg(1, 0) != 0 ||
        code.load_const_u16(2, 100) != 0 ||
        code.branch_gt(1, 2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 14j: move_reg unsigned", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_u16(0, 1) != 0 ||
        code.load_const_u16(1, 4) != 0 ||
        code.lshift_reg(2, 0, 1) != 0 ||
        code.load_const_u16(3, 16) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 14k: lshift_reg",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_u16(0, 16) != 0 ||
        code.load_const_u16(1, 4) != 0 ||
        code.rshift_reg(2, 0, 1) != 0 ||
        code.load_const_u16(3, 1) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 14l: rshift_reg",
                     ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testColumnBranchCoverage(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 sIntAttr = attrId(tab, "s_int");
  Uint32 sInt2Attr = attrId(tab, "s_int2");
  Uint32 nIntAttr = attrId(tab, "n_int");
  Uint32 cAttr = attrId(tab, "c_val");
  Uint32 bitAttr = attrId(tab, "bit_val");
  Uint32 buf[192];
  int rc = 0;
  Int32 zero = 0;
  Int32 value = 500000;
  char alpha[8] = { 'a', 'l', 'p', 'h', 'a', ' ', ' ', ' ' };
  char likePattern[2] = { 'a', '%' };
  Uint32 lowNibble = 0x0F;

  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2 };
    if (code.branch_col_eq(&value, sizeof(value), sIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15a: branch_col_eq value", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 3, 4, 5, 6 };
    if (code.branch_col_ne(&value, sizeof(value), sIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15b: branch_col_ne value", ndb, tab,
                  &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 6 };
    if (code.branch_col_lt(&zero, sizeof(zero), sIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15c: branch_col_lt value", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 4, 5 };
    if (code.branch_col_gt(&zero, sizeof(zero), sIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15d: branch_col_gt value", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 6 };
    if (code.branch_col_le(&value, sizeof(value), sIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15e: branch_col_le value", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 2, 4, 5 };
    if (code.branch_col_ge(&value, sizeof(value), sIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15f: branch_col_ge value", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 6 };
    if (code.branch_col_eq(sIntAttr, sInt2Attr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15g: branch_col_eq attr-attr", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 3, 4, 5 };
    if (code.branch_col_ne(sIntAttr, sInt2Attr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15h: branch_col_ne attr-attr", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 5 };
    if (code.branch_col_eq_null(nIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15i: branch_col_eq_null", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 6 };
    if (code.branch_col_ne_null(nIntAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15j: branch_col_ne_null", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    if (code.branch_col_eq(alpha, sizeof(alpha), cAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectNoPks("Test 15k: branch_col_eq CHAR",
                    ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (code.branch_col_like(likePattern, sizeof(likePattern), cAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15l: branch_col_like CHAR", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 5, 6 };
    if (code.branch_col_notlike(likePattern, sizeof(likePattern),
                                cAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15m: branch_col_notlike CHAR", ndb, tab,
                  &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 6 };
    if (code.branch_col_and_mask_eq_mask(&lowNibble, sizeof(lowNibble),
                                         bitAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15n: branch_col_and_mask_eq_mask", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 5 };
    if (code.branch_col_and_mask_ne_mask(&lowNibble, sizeof(lowNibble),
                                         bitAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15o: branch_col_and_mask_ne_mask", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 4 };
    if (code.branch_col_and_mask_eq_zero(&lowNibble, sizeof(lowNibble),
                                         bitAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15p: branch_col_and_mask_eq_zero", ndb, tab,
                  &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 3, 5, 6 };
    if (code.branch_col_and_mask_ne_zero(&lowNibble, sizeof(lowNibble),
                                         bitAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 15q: branch_col_and_mask_ne_zero", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  return rc;
}

static int
testMemoryAndLibraryOpcodeCoverage(Ndb *ndb,
                                   const NdbDictionary::Table *tab)
{
  Uint32 vAttr = attrId(tab, "v_val");
  Uint32 buf[256];
  int rc = 0;
  char numbers[8];
  char text[8] = { '1', '2', '3', '4', '5', '6', '7', '8' };
  char sort16[6];
  char interval16[8];
  memset(numbers, 0, sizeof(numbers));
  numbers[0] = 30;
  numbers[2] = 10;
  numbers[4] = 20;
  memset(sort16, 0, sizeof(sort16));
  sort16[0] = 30;
  sort16[2] = 10;
  sort16[4] = 20;
  memset(interval16, 0, sizeof(interval16));
  interval16[0] = 10;
  interval16[2] = 20;
  interval16[4] = 30;
  interval16[6] = 40;

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, text, sizeof(text)) != 0 ||
        code.load_const_u16(2, 2) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.bzero(2, 3) != 0 ||
        code.read_uint8_to_reg_const(4, 2) != 0 ||
        code.load_const_u16(5, 0) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 16a: bzero memory",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, text, sizeof(text)) != 0 ||
        code.load_const_u16(2, 4) != 0 ||
        code.str_to_int64(3, 0, 2) != 0 ||
        code.load_const_u16(4, 1234) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 16b: str_to_int64 from memory",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, numbers, sizeof(numbers)) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.qsort_instr(0, 3, 2) != 0 ||
        code.binary_search_16(2, 0, 3, 4, 0) != 0 ||
        code.load_const_u16(5, 1) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 16c: qsort plus binary_search_16",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, interval16, sizeof(interval16)) != 0 ||
        code.load_const_u16(2, 15) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_16(2, 0, 3, 4, 0) != 0 ||
        code.load_const_u16(5, 0) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 16d: search_interval_16",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 0) != 0 ||
        code.load_const_u16(1, 32) != 0 ||
        code.read_full(vAttr, 0, 2) != 0 ||
        code.load_const_u16(3, 0) != 0 ||
        code.load_const_u16(4, 1) != 0 ||
        code.read_partial(vAttr, 0, 3, 4, 5) != 0 ||
        code.load_const_u16(6, 1) != 0 ||
        code.branch_eq(5, 6, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 16e: read_full and read_partial",
                     ndb, tab, &code) != 0) rc = -1;
  }
  return rc;
}

static int
testNullAndFloatOpcodeCoverage(Ndb *ndb,
                               const NdbDictionary::Table *tab)
{
  Uint32 nDoubleAttr = attrId(tab, "n_double");
  Uint32 dAttr = attrId(tab, "d_val");
  Uint32 buf[160];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 17a: arithmetic rejects NULL",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.read_attr(0, nDoubleAttr) != 0 ||
        code.load_double_const(1, 0.0) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 17b: float branch rejects NULL register",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.load_double_const(1, 8.0) != 0 ||
        code.branch_eq(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 17c: branch_eq DOUBLE", ndb, tab,
                  &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 3, 4, 5, 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.load_double_const(1, 8.0) != 0 ||
        code.branch_ne(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 17d: branch_ne DOUBLE", ndb, tab,
                  &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 2, 4, 5 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.load_double_const(1, 8.0) != 0 ||
        code.branch_le(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 17e: branch_le DOUBLE", ndb, tab,
                  &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 2, 3, 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.load_double_const(1, 8.0) != 0 ||
        code.branch_ge(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 17f: branch_ge DOUBLE", ndb, tab,
                  &code, expected, 3) != 0) rc = -1;
  }
  return rc;
}

struct AttrPair {
  const char *lhs;
  const char *rhs;
  const char *label;
};

struct AttrConst {
  const char *attr;
  const char *label;
  Uint64 constant;
};

static int
testIntegerWidthBoundaryMatrix(Ndb *ndb,
                               const NdbDictionary::Table *mainTab)
{
  (void)mainTab;
  const NdbDictionary::Table *tab = getNamedTable(ndb, BOUNDARY_TABLE_NAME);
  if (tab == NULL) return -1;

  static const int signedNegative[] = { 1, 2, 3 };
  static const int signedMax[] = { 7 };
  static const int signedEqTwin[] = { 1, 2, 3, 6, 7 };
  static const int unsignedEqTwin[] = { 1, 2, 3, 5, 7 };
  static const int unsignedHighBit[] = { 3, 4, 5, 7 };
  static const int unsignedMax[] = { 5, 7 };

  const AttrConst signedAttrs[] = {
    { "s_tiny", "TINYINT", 127ULL },
    { "s_small", "SMALLINT", 32767ULL },
    { "s_medium", "MEDIUMINT", 8388607ULL },
    { "s_int", "INT", 2147483647ULL },
    { "s_big", "BIGINT", 9223372036854775807ULL }
  };
  const AttrConst unsignedAttrs[] = {
    { "u_tiny", "TINYINT UNSIGNED", 127ULL },
    { "u_small", "SMALLINT UNSIGNED", 32767ULL },
    { "u_medium", "MEDIUMINT UNSIGNED", 8388607ULL },
    { "u_int", "INT UNSIGNED", 2147483647ULL },
    { "u_big", "BIGINT UNSIGNED", 9223372036854775807ULL }
  };
  const AttrConst unsignedMaxAttrs[] = {
    { "u_tiny", "TINYINT UNSIGNED", 255ULL },
    { "u_small", "SMALLINT UNSIGNED", 65535ULL },
    { "u_medium", "MEDIUMINT UNSIGNED", 16777215ULL },
    { "u_int", "INT UNSIGNED", 4294967295ULL }
  };
  const AttrPair signedPairs[] = {
    { "s_tiny", "s_tiny2", "TINYINT" },
    { "s_small", "s_small2", "SMALLINT" },
    { "s_medium", "s_medium2", "MEDIUMINT" },
    { "s_int", "s_int2", "INT" },
    { "s_big", "s_big2", "BIGINT" }
  };
  const AttrPair unsignedPairs[] = {
    { "u_tiny", "u_tiny2", "TINYINT UNSIGNED" },
    { "u_small", "u_small2", "SMALLINT UNSIGNED" },
    { "u_medium", "u_medium2", "MEDIUMINT UNSIGNED" },
    { "u_int", "u_int2", "INT UNSIGNED" },
    { "u_big", "u_big2", "BIGINT UNSIGNED" }
  };

  int rc = 0;
  char name[128];
  size_t i;

  for (i = 0; i < sizeof(signedAttrs) / sizeof(signedAttrs[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18a.%zu: %s negative boundary", i + 1,
             signedAttrs[i].label);
    if (expectReadCompareU64(name, ndb, tab, attrId(tab, signedAttrs[i].attr),
                             0ULL, &NdbInterpretedCode::branch_lt,
                             signedNegative, 3) != 0) rc = -1;
  }

  for (i = 0; i < sizeof(signedAttrs) / sizeof(signedAttrs[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18b.%zu: %s max boundary", i + 1,
             signedAttrs[i].label);
    if (expectReadCompareU64(name, ndb, tab, attrId(tab, signedAttrs[i].attr),
                             signedAttrs[i].constant,
                             &NdbInterpretedCode::branch_eq,
                             signedMax, 1) != 0) rc = -1;
  }

  for (i = 0; i < sizeof(signedPairs) / sizeof(signedPairs[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18c.%zu: %s attr-attr equality", i + 1,
             signedPairs[i].label);
    if (expectColumnEq(name, ndb, tab,
                       attrId(tab, signedPairs[i].lhs),
                       attrId(tab, signedPairs[i].rhs),
                       signedEqTwin, 5) != 0) rc = -1;
  }

  for (i = 0; i < sizeof(unsignedPairs) / sizeof(unsignedPairs[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18d.%zu: %s attr-attr equality", i + 1,
             unsignedPairs[i].label);
    if (expectColumnEq(name, ndb, tab,
                       attrId(tab, unsignedPairs[i].lhs),
                       attrId(tab, unsignedPairs[i].rhs),
                       unsignedEqTwin, 5) != 0) rc = -1;
  }

  for (i = 0; i < sizeof(unsignedAttrs) / sizeof(unsignedAttrs[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18e.%zu: %s high-bit boundary", i + 1,
             unsignedAttrs[i].label);
    if (expectReadCompareU64(name, ndb, tab, attrId(tab, unsignedAttrs[i].attr),
                             unsignedAttrs[i].constant,
                             &NdbInterpretedCode::branch_gt,
                             unsignedHighBit, 4) != 0) rc = -1;
  }

  for (i = 0; i < sizeof(unsignedMaxAttrs) / sizeof(unsignedMaxAttrs[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18f.%zu: %s max boundary", i + 1,
             unsignedMaxAttrs[i].label);
    if (expectReadCompareU64(name, ndb, tab,
                             attrId(tab, unsignedMaxAttrs[i].attr),
                             unsignedMaxAttrs[i].constant,
                             &NdbInterpretedCode::branch_eq,
                             unsignedMax, 2) != 0) rc = -1;
  }

  if (expectUnsignedBigintMax("Test 18f.5: BIGINT UNSIGNED max boundary",
                              ndb, tab,
                              attrId(tab, "u_big"),
                              attrId(tab, "u_big2"),
                              attrId(tab, "u_int"),
                              unsignedMax, 2) != 0) rc = -1;

  return rc;
}

static int
testSignedUnsignedPromotionMatrix(Ndb *ndb,
                                  const NdbDictionary::Table *mainTab)
{
  (void)mainTab;
  const NdbDictionary::Table *tab = getNamedTable(ndb, BOUNDARY_TABLE_NAME);
  if (tab == NULL) return -1;

  Uint32 pkAttr = attrId(tab, "pk");
  Uint32 sTinyAttr = attrId(tab, "s_tiny");
  Uint32 sIntAttr = attrId(tab, "s_int");
  Uint32 sBigAttr = attrId(tab, "s_big");
  Uint32 uTinyAttr = attrId(tab, "u_tiny");
  Uint32 uIntAttr = attrId(tab, "u_int");
  Uint32 uBigAttr = attrId(tab, "u_big");
  Uint32 buf[160];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 2, 3, 4, 5, 7 };
    if (code.read_attr(0, sIntAttr) != 0 ||
        code.read_attr(1, uIntAttr) != 0 ||
        code.branch_lt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 19a: signed INT less than unsigned INT",
                  ndb, tab, &code, expected, 6) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 6 };
    if (code.read_attr(0, sIntAttr) != 0 ||
        code.read_attr(1, uIntAttr) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 19b: signed INT greater than unsigned INT",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 2, 3, 4, 5, 7 };
    if (code.read_attr(0, uBigAttr) != 0 ||
        code.read_attr(1, sBigAttr) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 19c: unsigned BIGINT greater than signed BIGINT",
                  ndb, tab, &code, expected, 6) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1, 2, 3, 4, 5, 7 };
    if (code.read_attr(0, sBigAttr) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.branch_lt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 19d: signed BIGINT less than unsigned BIGINT",
                  ndb, tab, &code, expected, 6) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 1 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 1, REJECT) != 0 ||
        code.read_attr(1, uIntAttr) != 0 ||
        code.read_attr(2, sIntAttr) != 0 ||
        code.add_reg(3, 1, 2) != 0 ||
        code.load_const_u16(4, 0) != 0 ||
        code.branch_lt(3, 4, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 19e: UINT plus negative INT stays negative",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 3 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 3, REJECT) != 0 ||
        code.read_attr(1, uIntAttr) != 0 ||
        code.read_attr(2, sIntAttr) != 0 ||
        code.add_reg(3, 1, 2) != 0 ||
        code.load_const_u64(4, 2147483647ULL) != 0 ||
        code.branch_eq(3, 4, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 19f: UINT high-bit plus signed -1",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 3 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 3, REJECT) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.read_attr(2, sBigAttr) != 0 ||
        code.sub_reg(3, 1, 2) != 0 ||
        code.branch_gt(3, 1, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 19g: unsigned BIGINT subtract signed -1",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 6 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 6, REJECT) != 0 ||
        code.read_attr(1, uTinyAttr) != 0 ||
        code.read_attr(2, sTinyAttr) != 0 ||
        code.mul_reg(3, 1, 2) != 0 ||
        code.load_const_u64(4, 5000ULL) != 0 ||
        code.branch_gt(3, 4, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 19h: unsigned TINYINT times signed TINYINT",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 RUN = 1;
    NdbInterpretedCode code(tab, buf, 160);
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 5, REJECT) != 0 ||
        code.branch_label(RUN) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(RUN) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.load_const_u16(2, 1) != 0 ||
        code.add_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19i: unsigned add overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 RUN = 1;
    char oneByte[1];
    NdbInterpretedCode code(tab, buf, 160);
    oneByte[0] = 1;
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 1, REJECT) != 0 ||
        code.branch_label(RUN) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(RUN) != 0 ||
        code.read_attr(1, uIntAttr) != 0 ||
        loadMemoryConst(&code, 2, 3, oneByte, sizeof(oneByte)) != 0 ||
        code.read_uint8_to_reg_const(4, 0) != 0 ||
        code.sub_reg(5, 1, 4) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19j: unsigned subtract underflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 RUN = 1;
    NdbInterpretedCode code(tab, buf, 160);
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 3, REJECT) != 0 ||
        code.branch_label(RUN) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(RUN) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.read_attr(2, uTinyAttr) != 0 ||
        code.mul_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19k: unsigned multiply overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }

  return rc;
}

static int
testFloatDoubleMatrix(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 pkAttr = attrId(tab, "pk");
  Uint32 fAttr = attrId(tab, "f_val");
  Uint32 f2Attr = attrId(tab, "f_val2");
  Uint32 dAttr = attrId(tab, "d_val");
  Uint32 d2Attr = attrId(tab, "d_val2");
  Uint32 nFloatAttr = attrId(tab, "n_float");
  Uint32 sIntAttr = attrId(tab, "s_int");
  Uint32 uIntAttr = attrId(tab, "u_int");
  Uint32 buf[192];
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, fAttr) != 0 ||
        code.load_double_const(1, 3.0) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20a: FLOAT read and compare",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 3, 5, 6 };
    if (code.read_attr(0, f2Attr) != 0 ||
        code.read_attr(1, d2Attr) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20b: FLOAT greater than DOUBLE",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 6 };
    if (code.read_attr(0, sIntAttr) != 0 ||
        code.read_attr(1, fAttr) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20c: signed INT greater than FLOAT",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 2, 4, 5, 6 };
    if (code.read_attr(0, uIntAttr) != 0 ||
        code.read_attr(1, dAttr) != 0 ||
        code.branch_gt(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20d: unsigned INT greater than DOUBLE",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 4 };
    if (code.read_attr(0, f2Attr) != 0 ||
        code.read_attr(1, d2Attr) != 0 ||
        code.branch_eq(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20e: positive zero equals negative zero",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 2, 4, 5 };
    if (code.read_attr(0, fAttr) != 0 ||
        code.read_attr(1, f2Attr) != 0 ||
        code.branch_eq(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20f: fractional FLOAT equality",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 6 };
    if (code.read_attr(0, nFloatAttr) != 0 ||
        code.branch_ne_null(0, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20g: nullable FLOAT branch_ne_null",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    if (code.read_attr(0, nFloatAttr) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 20h: nullable FLOAT arithmetic rejects NULL",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 3, 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.read_attr(1, fAttr) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, 20.0) != 0 ||
        code.branch_gt(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20i: DOUBLE plus FLOAT",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 6 };
    if (code.read_attr(0, dAttr) != 0 ||
        code.read_attr(1, fAttr) != 0 ||
        code.sub_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, 60.0) != 0 ||
        code.branch_gt(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20j: DOUBLE minus FLOAT",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (code.read_attr(0, fAttr) != 0 ||
        code.read_attr(1, dAttr) != 0 ||
        code.mul_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, 0.0) != 0 ||
        code.branch_lt(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20k: FLOAT times DOUBLE",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 6 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 6, REJECT) != 0 ||
        code.read_attr(1, dAttr) != 0 ||
        code.read_attr(2, fAttr) != 0 ||
        code.div_reg(3, 1, 2) != 0 ||
        code.load_double_const(4, 10.0) != 0 ||
        code.branch_gt(3, 4, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 20l: DOUBLE divided by FLOAT",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 RUN = 1;
    NdbInterpretedCode code(tab, buf, 192);
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 4, REJECT) != 0 ||
        code.branch_label(RUN) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(RUN) != 0 ||
        code.read_attr(1, dAttr) != 0 ||
        code.read_attr(2, fAttr) != 0 ||
        code.div_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 20m: DOUBLE divide by zero rejects",
                           ndb, tab, &code) != 0) rc = -1;
  }

  return rc;
}

struct TestEntry {
  int number;
  int (*fn)(Ndb *, const NdbDictionary::Table *);
};

static const TestEntry g_tests[] = {
  { 1, testSignedSubBigintLoads },
  { 2, testMixedSignedUnsignedCompare },
  { 3, testDoubleCompareAndArithmetic },
  { 4, testNullBranches },
  { 5, testBitwiseAndShifts },
  { 6, testMemorySpillWriters },
  { 7, testBoundaryComparisons },
  { 8, testArithmeticErrorHandlers },
  { 9, testRegisterErrorHandlers },
  { 10, testMixedTypeOperations },
  { 11, testConstBranchesAndShifts },
  { 12, testMemoryTypedLoadsAndWrites },
  { 13, testRegisterBranchCoverage },
  { 14, testArithmeticOpcodeCoverage },
  { 15, testColumnBranchCoverage },
  { 16, testMemoryAndLibraryOpcodeCoverage },
  { 17, testNullAndFloatOpcodeCoverage },
  { 18, testIntegerWidthBoundaryMatrix },
  { 19, testSignedUnsignedPromotionMatrix },
  { 20, testFloatDoubleMatrix }
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
      return 0;
    }
  }

  printf("=== testInterpreterTypedRegs ===\n");
  printf("Connect: %s, MySQL port: %d\n", connectString, mysqlPort);
  if (onlyTest != -1) printf("Filter: --only %d\n", onlyTest);
  printf("\n");

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
      if (conn == NULL) {
        exitCode = 1;
      } else {
        Ndb ndb(&clusterConn, TEST_DB);
        if (ndb.init() != 0) {
          fprintf(stderr, "Ndb::init failed: %s\n",
                  ndb.getNdbError().message);
          exitCode = 1;
        }
        else if (createTestTable(conn) != 0) {
          exitCode = 1;
        }
        else {
          const NdbDictionary::Table *tab = getTable(&ndb);
          if (tab == NULL) {
            exitCode = 1;
          } else {
            for (size_t i = 0; i < g_test_count; i++) {
              if (onlyTest != -1 && g_tests[i].number != onlyTest) continue;
              if (g_tests[i].fn(&ndb, tab) != 0) exitCode = 1;
            }
          }
        }
        dropTestTable(conn);
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
