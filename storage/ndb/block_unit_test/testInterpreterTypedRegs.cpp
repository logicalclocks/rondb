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
#include <NdbSqlUtil.hpp>
#include <decimal_utils.hpp>

#include <mysql.h>

#include <algorithm>
#include <cfloat>
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
  if (sqlExec(conn, "SET time_zone = '+00:00'") != 0) return -1;
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
      "  b_val BINARY(6) NOT NULL,"
      "  vb_val VARBINARY(8) NOT NULL,"
      "  c_wide CHAR(12) NOT NULL,"
      "  v_wide VARCHAR(16) NOT NULL,"
      "  date_val DATE NOT NULL,"
      "  date_val2 DATE NOT NULL,"
      "  dt_val DATETIME NOT NULL,"
      "  dt_val2 DATETIME NOT NULL,"
      "  ts_val TIMESTAMP NULL,"
      "  time_val TIME NOT NULL,"
      "  year_val YEAR NOT NULL,"
      "  dec_val DECIMAL(9,2) NOT NULL,"
      "  dec_val2 DECIMAL(9,2) NOT NULL,"
      "  PRIMARY KEY USING HASH (pk)"
      ") ENGINE=NDB") != 0) return -1;

  if (sqlExec(conn,
      "INSERT INTO interp_typed_regs VALUES"
      " (1, -5, -1000, -100000, -1000000, 0, -7,"
      "     5, 1000, 100000, 4000000000, 9223372036854775808,"
      "     -1.5, 4.5, NULL, NULL,"
      "     -1.5, 4.5, NULL, 'alpha', '12345', b'00001111',"
      "     x'6162007A20FF', x'', 'pad', '',"
      "     '2024-01-01', '2024-01-01',"
      "     '2024-01-01 10:00:00', '2024-01-01 10:00:00',"
      "     '2024-01-01 00:00:00', '01:02:03', 2024, 10.50, 10.50),"
      " (2,  5,   500,   50000,   500000, 500000, -1,"
      "    20, 2000, 200000, 3000000000, 10,"
      "      2.5, 8.0, 7, 8.0,"
      "      2.5, 8.0, 8.0, 'beta', '77', b'11110000',"
      "     x'000102030405', x'41', 'a', 'one',"
      "     '2024-06-15', '2024-01-01',"
      "     '2024-06-15 12:00:00', '2024-01-01 00:00:00',"
      "     '2024-06-15 00:00:00', '12:00:00', 2025, -1.25, 0.00),"
      " (3, 20, 20000,  500000, 20000000, 10000000, 9223372036854775807,"
      "   250, 65000, 8000000, 1, 9223372036854775813,"
      "     12.5, 16.25, -3, 16.25,"
      "     16.25, 12.5, 16.25, 'gamma', '-8', b'10101010',"
      "     x'FF807F000102', x'000041FF', 'alpha beta',"
      "     'sixteen-byte-str',"
      "     '1999-12-31', '2000-01-01',"
      "     '1999-12-31 23:59:59', '2000-01-01 00:00:00',"
      "     '2000-01-01 00:00:00', '23:59:59', 1999, 99999.99, 99999.99),"
      " (4, -1,    -1,      -1,       -1, 2, 42,"
      "     1,    1,      1, 2, 42,"
      "      0.0, -2.0, 0, -2.0,"
      "     -0.0, 0.0, -0.0, 'delta', '0', b'00000000',"
      "     x'202020202020', x'616263', 'trail   ', 'trail   ',"
      "     '2024-01-01', '2024-01-02',"
      "     '2024-01-01 00:00:00', '2024-01-01 00:01:00',"
      "     NULL, '00:00:00', 2000, 0.00, -0.01),"
      " (5, -128, -32768, -8388608, -2147483648, -1, -9223372036854775808,"
      "     0, 0, 0, 0, 0,"
      "     -3.25, -64.0, NULL, NULL,"
      "     -3.25, -64.0, NULL, 'epsilon', '922', b'01010101',"
      "     x'404040404040', x'FFFE', '', 'z',"
      "     '2026-12-31', '2026-12-30',"
      "     '2026-12-31 23:59:59', '2026-12-30 00:00:00',"
      "     '2026-12-31 00:00:00', '23:59:59', 2155, -99999.99, -99999.99),"
      " (6, 127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807,"
      "     255, 65535, 16777215, 4294967295, 18446744073709551615,"
      "     3.25, 64.0, 2147483647, 64.0,"
      "     64.0, 3.25, 64.0, 'zeta', '42', b'11111111',"
      "     x'6162007A20FD', x'6162636465666768', 'pad', 'abc%def',"
      "     '2024-01-02', '2024-01-01',"
      "     '2024-01-02 06:30:00', '2024-01-02 06:30:00',"
      "     '2030-01-01 00:00:00', '06:30:00', 1901, 123.45, 120.00)"
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
expectColumnValueBranch(
    const char *name,
    Ndb *ndb,
    const NdbDictionary::Table *tab,
    Uint32 attr,
    const void *value,
    Uint32 valueBytes,
    int (NdbInterpretedCode::*branch)(const void *, Uint32, Uint32, Uint32),
    NdbInterpretedCode::UnknownHandling nullHandling,
    bool useSqlNullSemantics,
    const int *expected,
    size_t nExpected)
{
  Uint32 buf[128];
  NdbInterpretedCode code(tab, buf, 128);
  if (useSqlNullSemantics)
    code.set_sql_null_semantics(nullHandling);
  if ((code.*branch)(value, valueBytes, attr, 0) != 0 ||
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
expectReadAddConstCompareU64(const char *name,
                             Ndb *ndb,
                             const NdbDictionary::Table *tab,
                             Uint32 attr,
                             Uint16 addend,
                             Uint64 expectedValue,
                             const int *expected,
                             size_t nExpected)
{
  Uint32 buf[128];
  NdbInterpretedCode code(tab, buf, 128);
  if (code.read_attr(0, attr) != 0 ||
      code.add_const_reg(1, 0, addend) != 0 ||
      code.load_const_u64(2, expectedValue) != 0 ||
      code.branch_eq(1, 2, 0) != 0 ||
      finishAcceptReject(&code, 0) != 0) {
    printf("%s ... FAILED (build)\n", name);
    return -1;
  }
  return expectPks(name, ndb, tab, &code, expected, nExpected);
}

static int
expectColumnBranch(
    const char *name,
    Ndb *ndb,
    const NdbDictionary::Table *tab,
    Uint32 attr1,
    Uint32 attr2,
    int (NdbInterpretedCode::*branch)(Uint32, Uint32, Uint32),
    const int *expected,
    size_t nExpected)
{
  Uint32 buf[128];
  NdbInterpretedCode code(tab, buf, 128);
  if ((code.*branch)(attr1, attr2, 0) != 0 ||
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
  /*
   * Typed-register arithmetic / overflow coverage audit:
   *
   * Operation   Reg-Reg   Reg-Const   Signed edge       Unsigned edge
   * ADD         8d,19i    8h          LLONG_MAX+1       UINT64_MAX+1
   * SUB         8e,19j    8i          LLONG_MIN-1       0-1
   * MUL         26f,19k   8j          >LLONG_MAX        >UINT64_MAX
   * DIV         8a,8f     8k          zero,MIN/-1       zero, nonzero max
   * MOD         8b,8g     8l          zero,MIN%-1       zero, nonzero max
   *
   * Mixed signed/unsigned edges are covered by Test 19e..19s.
   * Source-width promotion is covered by Test 18g..18h.
   * Float arithmetic behavior is documented by Test 20m..20o.
   * Shift and bitwise error boundaries are covered by Test 26c..26e.6.
   */
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
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775808ULL) != 0 ||
        code.load_const_u64(1, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.div_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8f: signed divide overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775808ULL) != 0 ||
        code.load_const_u64(1, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.mod_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8g: signed modulo overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775807ULL) != 0 ||
        code.add_const_reg(1, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8h: add_const_reg signed overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775808ULL) != 0 ||
        code.sub_const_reg(1, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8i: sub_const_reg signed overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u64(0, 9223372036854775807ULL) != 0 ||
        code.mul_const_reg(1, 0, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8j: mul_const_reg signed overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u16(0, 1) != 0 ||
        code.div_const_reg(1, 0, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8k: div_const_reg divide by zero",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 128);
    if (code.load_const_u16(0, 1) != 0 ||
        code.mod_const_reg(1, 0, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 8l: mod_const_reg divide by zero",
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
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a: ADD propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.add_const_reg(1, 0, 10) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.1: ADD const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.sub_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.2: SUB propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.sub_const_reg(1, 0, 10) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.3: SUB const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 3) != 0 ||
        code.mul_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.4: MUL propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.mul_const_reg(1, 0, 3) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.5: MUL const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 3) != 0 ||
        code.div_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.6: DIV propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.div_const_reg(1, 0, 3) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.7: DIV const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 2) != 0 ||
        code.lshift_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.8: LSHIFT propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.lshift_const_reg(1, 0, 2) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.9: LSHIFT const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 2) != 0 ||
        code.rshift_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.10: RSHIFT propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.rshift_const_reg(1, 0, 2) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.11: RSHIFT const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 3) != 0 ||
        code.and_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.12: AND propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.and_const_reg(1, 0, 3) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.13: AND const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 3) != 0 ||
        code.or_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.14: OR propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.or_const_reg(1, 0, 3) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.15: OR const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 3) != 0 ||
        code.xor_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.16: XOR propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.xor_const_reg(1, 0, 3) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.17: XOR const propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 3) != 0 ||
        code.mod_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.18: MOD propagates NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 160);
    if (code.load_const_null(0) != 0 ||
        code.mod_const_reg(1, 0, 3) != 0 ||
        code.branch_eq_null(1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 17a.19: MOD const propagates NULL",
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
  static const int signedPromotionMax[] = { 7 };
  static const int unsignedPromotionMax[] = { 5, 7 };

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
  const AttrConst signedPromotedMaxPlusOne[] = {
    { "s_tiny", "TINYINT", 128ULL },
    { "s_small", "SMALLINT", 32768ULL },
    { "s_medium", "MEDIUMINT", 8388608ULL },
    { "s_int", "INT", 2147483648ULL }
  };
  const AttrConst unsignedPromotedMaxPlusOne[] = {
    { "u_tiny", "TINYINT UNSIGNED", 256ULL },
    { "u_small", "SMALLINT UNSIGNED", 65536ULL },
    { "u_medium", "MEDIUMINT UNSIGNED", 16777216ULL },
    { "u_int", "INT UNSIGNED", 4294967296ULL }
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

  for (i = 0; i < sizeof(signedPromotedMaxPlusOne) /
                  sizeof(signedPromotedMaxPlusOne[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18g.%zu: %s max plus one promotes to Int64", i + 1,
             signedPromotedMaxPlusOne[i].label);
    if (expectReadAddConstCompareU64(
            name, ndb, tab, attrId(tab, signedPromotedMaxPlusOne[i].attr),
            1, signedPromotedMaxPlusOne[i].constant,
            signedPromotionMax, 1) != 0) rc = -1;
  }

  for (i = 0; i < sizeof(unsignedPromotedMaxPlusOne) /
                  sizeof(unsignedPromotedMaxPlusOne[0]); i++) {
    snprintf(name, sizeof(name),
             "Test 18h.%zu: %s max plus one promotes to Uint64", i + 1,
             unsignedPromotedMaxPlusOne[i].label);
    if (expectReadAddConstCompareU64(
            name, ndb, tab, attrId(tab, unsignedPromotedMaxPlusOne[i].attr),
            1, unsignedPromotedMaxPlusOne[i].constant,
            unsignedPromotionMax, 2) != 0) rc = -1;
  }

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
        code.read_attr(1, uBigAttr) != 0 ||
        loadMemoryConst(&code, 2, 3, oneByte, sizeof(oneByte)) != 0 ||
        code.read_uint8_to_reg_const(4, 0) != 0 ||
        code.sub_reg(5, 1, 4) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19j: unsigned BIGINT subtract underflow",
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
        code.load_const_u16(2, 2) != 0 ||
        code.mul_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19l: unsigned BIGINT max times two",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 5 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 5, REJECT) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.load_const_u16(2, 1) != 0 ||
        code.div_reg(3, 1, 2) != 0 ||
        code.branch_eq(3, 1, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 19m: unsigned BIGINT max divide by one",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 ACCEPT = 1;
    NdbInterpretedCode code(tab, buf, 160);
    const int expected[] = { 5 };
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 5, REJECT) != 0 ||
        code.read_attr(1, uBigAttr) != 0 ||
        code.load_const_u16(2, 1) != 0 ||
        code.mod_reg(3, 1, 2) != 0 ||
        code.load_const_u16(4, 0) != 0 ||
        code.branch_eq(3, 4, ACCEPT) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_nok() != 0 ||
        code.def_label(ACCEPT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.finalise() != 0 ||
        expectPks("Test 19n: unsigned BIGINT max modulo one",
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
        code.load_const_u64(2, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.sub_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19o: unsigned max minus signed -1",
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
        code.load_const_u16(2, 2) != 0 ||
        code.mul_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19p: unsigned high-bit times signed two",
                           ndb, tab, &code) != 0) rc = -1;
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
        code.load_const_u64(1, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.read_attr(2, uBigAttr) != 0 ||
        code.sub_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19q: signed -1 minus unsigned max",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 RUN = 1;
    NdbInterpretedCode code(tab, buf, 160);
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 1, REJECT) != 0 ||
        code.branch_label(RUN) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(RUN) != 0 ||
        code.read_attr(1, sBigAttr) != 0 ||
        code.read_attr(2, uBigAttr) != 0 ||
        code.div_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19r: signed negative divide unsigned zero",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    const Uint32 REJECT = 0;
    const Uint32 RUN = 1;
    NdbInterpretedCode code(tab, buf, 160);
    if (code.read_attr(0, pkAttr) != 0 ||
        code.branch_ne_const(0, 1, REJECT) != 0 ||
        code.branch_label(RUN) != 0 ||
        code.def_label(REJECT) != 0 ||
        code.interpret_exit_ok() != 0 ||
        code.def_label(RUN) != 0 ||
        code.read_attr(1, sBigAttr) != 0 ||
        code.read_attr(2, uBigAttr) != 0 ||
        code.mod_reg(3, 1, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 19s: signed negative modulo unsigned zero",
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
    const int expected[] = { 1, 5 };
    if (code.read_attr(0, nFloatAttr) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 20h: nullable FLOAT ADD propagates NULL",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
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
  {
    NdbInterpretedCode code(tab, buf, 192);
    if (code.load_double_const(0, DBL_MAX) != 0 ||
        code.load_double_const(1, DBL_MAX) != 0 ||
        code.add_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, DBL_MAX) != 0 ||
        code.branch_gt(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 20n: DOUBLE add overflow yields infinity",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    if (code.load_double_const(0, DBL_MAX) != 0 ||
        code.load_double_const(1, 2.0) != 0 ||
        code.mul_reg(2, 0, 1) != 0 ||
        code.load_double_const(3, DBL_MAX) != 0 ||
        code.branch_gt(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 20o: DOUBLE multiply overflow yields infinity",
                     ndb, tab, &code) != 0) rc = -1;
  }

  return rc;
}

static void
packUint24(Uint32 value, unsigned char out[3])
{
  out[0] = (unsigned char)(value & 0xFF);
  out[1] = (unsigned char)((value >> 8) & 0xFF);
  out[2] = (unsigned char)((value >> 16) & 0xFF);
}

static void
packInt24(Int32 value, unsigned char out[3])
{
  packUint24((Uint32)value, out);
}

static int
testColumnPredicateTypeMatrix(Ndb *ndb,
                              const NdbDictionary::Table *mainTab)
{
  const NdbDictionary::Table *widthTab =
      getNamedTable(ndb, BOUNDARY_TABLE_NAME);
  if (widthTab == NULL) return -1;

  Uint32 nIntAttr = attrId(mainTab, "n_int");
  Uint32 nDoubleAttr = attrId(mainTab, "n_double");
  Uint32 nFloatAttr = attrId(mainTab, "n_float");
  Uint32 fAttr = attrId(mainTab, "f_val");
  Uint32 dAttr = attrId(mainTab, "d_val");
  Uint32 d2Attr = attrId(mainTab, "d_val2");

  Int8 sTinyMax = 127;
  Int16 sSmallMax = 32767;
  unsigned char sMediumMax[3];
  Int32 sIntMax = 2147483647;
  Int64 sBigMax = INT64_MAX;
  Uint8 uTinyThreshold = 127;
  Uint16 uSmallThreshold = 32767;
  unsigned char uMediumThreshold[3];
  Uint32 uIntThreshold = 2147483647U;
  Uint64 uBigThreshold = 9223372036854775807ULL;
  float fTwoPointFive = 2.5F;
  double dZero = 0.0;
  Int32 zero = 0;
  int rc = 0;

  static const int signedMax[] = { 7 };
  static const int unsignedBelowThreshold[] = { 1, 2, 6 };
  static const int signedColLtTwin[] = { 5 };
  static const int floatEq[] = { 2 };
  static const int doublePositive[] = { 1, 2, 3, 6 };
  static const int doubleEqTwin[] = { 1, 2, 5 };
  static const int doubleColGtTwin[] = { 3, 6 };
  static const int nullRows[] = { 1, 5 };
  static const int notNullRows[] = { 2, 3, 4, 6 };
  static const int nullsAndNegative[] = { 1, 3, 5 };
  static const int onlyNegative[] = { 3 };

  packInt24(8388607, sMediumMax);
  packUint24(8388607U, uMediumThreshold);

  /*
   * The NdbInterpretedCode value variants compare in API order:
   *   *value <op> column
   * The expected row sets below are written in that order.
   */
  {
    struct ColValCase {
      const char *name;
      const char *attr;
      const void *value;
      Uint32 valueBytes;
    };
    const ColValCase signedCases[] = {
      { "Test 21a.1: branch_col_eq TINYINT max",
        "s_tiny", &sTinyMax, sizeof(sTinyMax) },
      { "Test 21a.2: branch_col_eq SMALLINT max",
        "s_small", &sSmallMax, sizeof(sSmallMax) },
      { "Test 21a.3: branch_col_eq MEDIUMINT max",
        "s_medium", sMediumMax, sizeof(sMediumMax) },
      { "Test 21a.4: branch_col_eq INT max",
        "s_int", &sIntMax, sizeof(sIntMax) },
      { "Test 21a.5: branch_col_eq BIGINT max",
        "s_big", &sBigMax, sizeof(sBigMax) }
    };
    const ColValCase unsignedCases[] = {
      { "Test 21b.1: branch_col_gt TINYINT UNSIGNED threshold",
        "u_tiny", &uTinyThreshold, sizeof(uTinyThreshold) },
      { "Test 21b.2: branch_col_gt SMALLINT UNSIGNED threshold",
        "u_small", &uSmallThreshold, sizeof(uSmallThreshold) },
      { "Test 21b.3: branch_col_gt MEDIUMINT UNSIGNED threshold",
        "u_medium", uMediumThreshold, sizeof(uMediumThreshold) },
      { "Test 21b.4: branch_col_gt INT UNSIGNED threshold",
        "u_int", &uIntThreshold, sizeof(uIntThreshold) },
      { "Test 21b.5: branch_col_gt BIGINT UNSIGNED threshold",
        "u_big", &uBigThreshold, sizeof(uBigThreshold) }
    };
    size_t i;
    for (i = 0; i < sizeof(signedCases) / sizeof(signedCases[0]); i++) {
      if (expectColumnValueBranch(
              signedCases[i].name, ndb, widthTab,
              attrId(widthTab, signedCases[i].attr),
              signedCases[i].value, signedCases[i].valueBytes,
              &NdbInterpretedCode::branch_col_eq,
              NdbInterpretedCode::CmpHasNoUnknowns, false,
              signedMax, 1) != 0) rc = -1;
    }
    for (i = 0; i < sizeof(unsignedCases) / sizeof(unsignedCases[0]); i++) {
      if (expectColumnValueBranch(
              unsignedCases[i].name, ndb, widthTab,
              attrId(widthTab, unsignedCases[i].attr),
              unsignedCases[i].value, unsignedCases[i].valueBytes,
              &NdbInterpretedCode::branch_col_gt,
              NdbInterpretedCode::CmpHasNoUnknowns, false,
              unsignedBelowThreshold, 3) != 0) rc = -1;
    }
  }

  {
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
    char name[128];
    size_t i;
    /*
     * The NdbInterpretedCode attr-attr variants compare in API order:
     *   attr2 <op> attr1
     */
    for (i = 0; i < sizeof(signedPairs) / sizeof(signedPairs[0]); i++) {
      snprintf(name, sizeof(name),
               "Test 21c.%zu: branch_col_lt %s attr-attr",
               i + 1, signedPairs[i].label);
      if (expectColumnBranch(name, ndb, widthTab,
                             attrId(widthTab, signedPairs[i].lhs),
                             attrId(widthTab, signedPairs[i].rhs),
                             &NdbInterpretedCode::branch_col_lt,
                             signedColLtTwin, 1) != 0) rc = -1;
    }
    for (i = 0; i < sizeof(unsignedPairs) / sizeof(unsignedPairs[0]); i++) {
      snprintf(name, sizeof(name),
               "Test 21d.%zu: branch_col_lt %s attr-attr",
               i + 1, unsignedPairs[i].label);
      if (expectColumnBranch(name, ndb, widthTab,
                             attrId(widthTab, unsignedPairs[i].lhs),
                             attrId(widthTab, unsignedPairs[i].rhs),
                             &NdbInterpretedCode::branch_col_lt,
                             NULL, 0) != 0) rc = -1;
    }
  }

  if (expectColumnValueBranch("Test 21e: branch_col_eq FLOAT",
                              ndb, mainTab, fAttr,
                              &fTwoPointFive, sizeof(fTwoPointFive),
                              &NdbInterpretedCode::branch_col_eq,
                              NdbInterpretedCode::CmpHasNoUnknowns, false,
                              floatEq, 1) != 0) rc = -1;

  if (expectColumnValueBranch("Test 21f: branch_col_lt DOUBLE",
                              ndb, mainTab, dAttr,
                              &dZero, sizeof(dZero),
                              &NdbInterpretedCode::branch_col_lt,
                              NdbInterpretedCode::CmpHasNoUnknowns, false,
                              doublePositive, 4) != 0) rc = -1;

  if (expectColumnBranch("Test 21g: branch_col_eq DOUBLE attr-attr",
                         ndb, mainTab, dAttr, d2Attr,
                         &NdbInterpretedCode::branch_col_eq,
                         doubleEqTwin, 3) != 0) rc = -1;

  if (expectColumnBranch("Test 21h: branch_col_gt DOUBLE attr-attr",
                         ndb, mainTab, d2Attr, dAttr,
                         &NdbInterpretedCode::branch_col_gt,
                         doubleColGtTwin, 2) != 0) rc = -1;

  {
    Uint32 buf[128];
    NdbInterpretedCode code(mainTab, buf, 128);
    if (code.branch_col_eq_null(nFloatAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 21i: branch_col_eq_null FLOAT",
                  ndb, mainTab, &code, nullRows, 2) != 0) rc = -1;
  }
  {
    Uint32 buf[128];
    NdbInterpretedCode code(mainTab, buf, 128);
    if (code.branch_col_ne_null(nDoubleAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 21j: branch_col_ne_null DOUBLE",
                  ndb, mainTab, &code, notNullRows, 4) != 0) rc = -1;
  }

  if (expectColumnValueBranch("Test 21k: SQL NULL semantics BranchIfUnknown",
                              ndb, mainTab, nIntAttr,
                              &zero, sizeof(zero),
                              &NdbInterpretedCode::branch_col_gt,
                              NdbInterpretedCode::BranchIfUnknown, true,
                              nullsAndNegative, 3) != 0) rc = -1;

  if (expectColumnValueBranch("Test 21l: SQL NULL semantics ContinueIfUnknown",
                              ndb, mainTab, nIntAttr,
                              &zero, sizeof(zero),
                              &NdbInterpretedCode::branch_col_gt,
                              NdbInterpretedCode::ContinueIfUnknown, true,
                              onlyNegative, 1) != 0) rc = -1;

  return rc;
}

static int
testStringAndBinaryTypeMatrix(Ndb *ndb,
                              const NdbDictionary::Table *tab)
{
  Uint32 bAttr = attrId(tab, "b_val");
  Uint32 vbAttr = attrId(tab, "vb_val");
  Uint32 cWideAttr = attrId(tab, "c_wide");
  Uint32 vWideAttr = attrId(tab, "v_wide");
  Uint32 buf[192];
  int rc = 0;

  const unsigned char binaryAbz[] =
      { 0x61, 0x62, 0x00, 0x7A, 0x20, 0xFF };
  const unsigned char binaryThreshold[] =
      { 0x61, 0x62, 0x00, 0x7A, 0x20, 0xFE };
  const unsigned char vbEmpty[] = { 0 };
  const unsigned char vbOne[] = { 1, 0x41 };
  const unsigned char vbEmbeddedHigh[] = { 4, 0x00, 0x00, 0x41, 0xFF };
  const unsigned char vbMax[] =
      { 8, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68 };
  const unsigned char vEmpty[] = { 0 };
  const unsigned char vOne[] = { 3, 'o', 'n', 'e' };
  const unsigned char vThreshold[] = { 1, 'm' };
  const char padPattern[] = { 'p', 'a', 'd', '%' };
  const char trailPattern[] = { 't', 'r', 'a', 'i', 'l', '%' };
  const char abcPattern[] = { 'a', 'b', 'c', '%' };

  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (code.branch_col_eq(binaryAbz, sizeof(binaryAbz), bAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22a: BINARY exact equality with embedded zero",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 3 };
    if (code.branch_col_lt(binaryThreshold, sizeof(binaryThreshold),
                           bAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22b: BINARY ordering with high-bit bytes",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 5, 6 };
    if (code.branch_col_ne(binaryAbz, sizeof(binaryAbz), bAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22c: BINARY inequality",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }

  /*
   * VARBINARY and VARCHAR constants are in normal NDB column format:
   * a one-byte length prefix followed by payload bytes.
   */
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (code.branch_col_eq(vbEmpty, sizeof(vbEmpty), vbAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22d: VARBINARY empty string equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2 };
    if (code.branch_col_eq(vbOne, sizeof(vbOne), vbAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22e: VARBINARY one-byte equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 3 };
    if (code.branch_col_eq(vbEmbeddedHigh, sizeof(vbEmbeddedHigh),
                           vbAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22f: VARBINARY embedded zero and high-bit equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 6 };
    if (code.branch_col_eq(vbMax, sizeof(vbMax), vbAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22g: VARBINARY max-length equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 6 };
    if (code.branch_col_like(padPattern, sizeof(padPattern),
                             cWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22h: CHAR LIKE padded short value",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 4 };
    if (code.branch_col_like(trailPattern, sizeof(trailPattern),
                             cWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22i: CHAR LIKE explicit trailing spaces",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 5 };
    if (code.branch_col_notlike(padPattern, sizeof(padPattern),
                                cWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22j: CHAR NOT LIKE padded short value",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (code.branch_col_eq(vEmpty, sizeof(vEmpty), vWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22k: VARCHAR empty string equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2 };
    if (code.branch_col_eq(vOne, sizeof(vOne), vWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22l: VARCHAR short string equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 4, 5 };
    if (code.branch_col_lt(vThreshold, sizeof(vThreshold),
                           vWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22m: VARCHAR ordering",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 6 };
    if (code.branch_col_like(abcPattern, sizeof(abcPattern),
                             vWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22n: VARCHAR LIKE with literal percent data",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 2, 3, 4, 5 };
    if (code.branch_col_notlike(abcPattern, sizeof(abcPattern),
                                vWideAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 22o: VARCHAR NOT LIKE",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }

  return rc;
}

static int
packDateConst(const NdbDictionary::Table *tab, Uint32 attr,
              uint year, uint month, uint day,
              unsigned char *out, Uint32 *bytes)
{
  const NdbDictionary::Column *col = tab->getColumn(attr);
  if (col == NULL) return -1;
  memset(out, 0, 16);
  NdbSqlUtil::Date value;
  value.year = year;
  value.month = month;
  value.day = day;
  if (col->getType() != NdbDictionary::Column::Date) return -1;
  NdbSqlUtil::pack_date(value, out);
  *bytes = (Uint32)col->getSizeInBytes();
  return 0;
}

static int
packDatetimeConst(const NdbDictionary::Table *tab, Uint32 attr,
                  uint year, uint month, uint day,
                  uint hour, uint minute, uint second,
                  unsigned char *out, Uint32 *bytes)
{
  const NdbDictionary::Column *col = tab->getColumn(attr);
  if (col == NULL) return -1;
  memset(out, 0, 16);
  if (col->getType() == NdbDictionary::Column::Datetime) {
    NdbSqlUtil::Datetime value;
    value.year = year;
    value.month = month;
    value.day = day;
    value.hour = hour;
    value.minute = minute;
    value.second = second;
    NdbSqlUtil::pack_datetime(value, out);
  } else if (col->getType() == NdbDictionary::Column::Datetime2) {
    NdbSqlUtil::Datetime2 value;
    value.sign = 1;
    value.year = year;
    value.month = month;
    value.day = day;
    value.hour = hour;
    value.minute = minute;
    value.second = second;
    value.fraction = 0;
    NdbSqlUtil::pack_datetime2(value, out, (uint)col->getPrecision());
  } else {
    return -1;
  }
  *bytes = (Uint32)col->getSizeInBytes();
  return 0;
}

static int
packTimestampConst(const NdbDictionary::Table *tab, Uint32 attr,
                   uint second, unsigned char *out, Uint32 *bytes)
{
  const NdbDictionary::Column *col = tab->getColumn(attr);
  if (col == NULL) return -1;
  memset(out, 0, 16);
  if (col->getType() == NdbDictionary::Column::Timestamp) {
    NdbSqlUtil::Timestamp value;
    value.second = second;
    NdbSqlUtil::pack_timestamp(value, out);
  } else if (col->getType() == NdbDictionary::Column::Timestamp2) {
    NdbSqlUtil::Timestamp2 value;
    value.second = second;
    value.fraction = 0;
    NdbSqlUtil::pack_timestamp2(value, out, (uint)col->getPrecision());
  } else {
    return -1;
  }
  *bytes = (Uint32)col->getSizeInBytes();
  return 0;
}

static int
packTimeConst(const NdbDictionary::Table *tab, Uint32 attr,
              uint hour, uint minute, uint second,
              unsigned char *out, Uint32 *bytes)
{
  const NdbDictionary::Column *col = tab->getColumn(attr);
  if (col == NULL) return -1;
  memset(out, 0, 16);
  if (col->getType() == NdbDictionary::Column::Time) {
    NdbSqlUtil::Time value;
    value.sign = 1;
    value.hour = hour;
    value.minute = minute;
    value.second = second;
    NdbSqlUtil::pack_time(value, out);
  } else if (col->getType() == NdbDictionary::Column::Time2) {
    NdbSqlUtil::Time2 value;
    value.sign = 1;
    value.interval = 0;
    value.hour = hour;
    value.minute = minute;
    value.second = second;
    value.fraction = 0;
    NdbSqlUtil::pack_time2(value, out, (uint)col->getPrecision());
  } else {
    return -1;
  }
  *bytes = (Uint32)col->getSizeInBytes();
  return 0;
}

static int
packYearConst(const NdbDictionary::Table *tab, Uint32 attr,
              uint year, unsigned char *out, Uint32 *bytes)
{
  const NdbDictionary::Column *col = tab->getColumn(attr);
  if (col == NULL) return -1;
  memset(out, 0, 16);
  NdbSqlUtil::Year value;
  value.year = year;
  if (col->getType() != NdbDictionary::Column::Year) return -1;
  NdbSqlUtil::pack_year(value, out);
  *bytes = (Uint32)col->getSizeInBytes();
  return 0;
}

static int
packDecimal9_2Const(const char *value, unsigned char out[5])
{
  int rc = decimal_str2bin(value, (int)strlen(value), 9, 2, out, 5);
  return (rc == E_DEC_OK) ? 0 : -1;
}

static int
testDateTimeAndDecimalTriage(Ndb *ndb,
                             const NdbDictionary::Table *tab)
{
  Uint32 dateAttr = attrId(tab, "date_val");
  Uint32 date2Attr = attrId(tab, "date_val2");
  Uint32 dtAttr = attrId(tab, "dt_val");
  Uint32 dt2Attr = attrId(tab, "dt_val2");
  Uint32 tsAttr = attrId(tab, "ts_val");
  Uint32 timeAttr = attrId(tab, "time_val");
  Uint32 yearAttr = attrId(tab, "year_val");
  Uint32 decAttr = attrId(tab, "dec_val");
  Uint32 dec2Attr = attrId(tab, "dec_val2");
  Uint32 buf[192];
  unsigned char value[16];
  Uint32 valueBytes = 0;
  int rc = 0;

  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 4 };
    if (packDateConst(tab, dateAttr, 2024, 1, 1,
                      value, &valueBytes) != 0 ||
        code.branch_col_eq(value, valueBytes, dateAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23a: DATE equality",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 5, 6 };
    if (packDateConst(tab, dateAttr, 2024, 1, 1,
                      value, &valueBytes) != 0 ||
        code.branch_col_lt(value, valueBytes, dateAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23b: DATE ordering",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (code.branch_col_eq(dateAttr, date2Attr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23c: DATE attr-attr equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 2, 5, 6 };
    if (packDatetimeConst(tab, dtAttr, 2024, 1, 1, 0, 0, 0,
                          value, &valueBytes) != 0 ||
        code.branch_col_lt(value, valueBytes, dtAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23d: DATETIME ordering",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 5 };
    if (code.branch_col_lt(dtAttr, dt2Attr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23e: DATETIME attr-attr ordering",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 2, 3, 5, 6 };
    if (code.branch_col_ne_null(tsAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23f: TIMESTAMP IS NOT NULL",
                  ndb, tab, &code, expected, 5) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 5, 6 };
    if (packTimestampConst(tab, tsAttr, 1704067200U,
                           value, &valueBytes) != 0 ||
        code.branch_col_lt(value, valueBytes, tsAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23g: TIMESTAMP ordering",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 4 };
    if (packTimeConst(tab, timeAttr, 0, 0, 0,
                      value, &valueBytes) != 0 ||
        code.branch_col_eq(value, valueBytes, timeAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23h: TIME equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 3, 5, 6 };
    if (packTimeConst(tab, timeAttr, 6, 0, 0,
                      value, &valueBytes) != 0 ||
        code.branch_col_lt(value, valueBytes, timeAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23i: TIME ordering",
                  ndb, tab, &code, expected, 4) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1 };
    if (packYearConst(tab, yearAttr, 2024, value, &valueBytes) != 0 ||
        code.branch_col_eq(value, valueBytes, yearAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23j: YEAR equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 2, 5 };
    if (packYearConst(tab, yearAttr, 2024, value, &valueBytes) != 0 ||
        code.branch_col_lt(value, valueBytes, yearAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23k: YEAR ordering",
                  ndb, tab, &code, expected, 2) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    unsigned char decValue[5];
    const int expected[] = { 1 };
    if (packDecimal9_2Const("10.50", decValue) != 0 ||
        code.branch_col_eq(decValue, sizeof(decValue), decAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23l: DECIMAL equality",
                  ndb, tab, &code, expected, 1) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    unsigned char decValue[5];
    const int expected[] = { 1, 3, 6 };
    if (packDecimal9_2Const("0.00", decValue) != 0 ||
        code.branch_col_lt(decValue, sizeof(decValue), decAttr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23m: DECIMAL ordering",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 192);
    const int expected[] = { 1, 3, 5 };
    if (code.branch_col_eq(decAttr, dec2Attr, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectPks("Test 23n: DECIMAL attr-attr equality",
                  ndb, tab, &code, expected, 3) != 0) rc = -1;
  }

  return rc;
}

static int
testMemoryOpcodeExpansion(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 buf[256];
  int rc = 0;
  char mem[32];
  memset(mem, 0, sizeof(mem));
  mem[0] = (char)0xFA;
  mem[1] = (char)0x34;
  mem[2] = (char)0x12;
  mem[4] = (char)0x78;
  mem[5] = (char)0x56;
  mem[6] = (char)0x34;
  mem[7] = (char)0x12;
  mem[8] = (char)0xF0;
  mem[9] = (char)0xDE;
  mem[10] = (char)0xBC;
  mem[11] = (char)0x9A;
  mem[12] = (char)0x78;
  mem[13] = (char)0x56;
  mem[14] = (char)0x34;
  mem[15] = (char)0x12;

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.read_uint8_to_reg_reg(3, 2) != 0 ||
        code.load_const_u16(4, 0xFA) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24a: read_uint8 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 1) != 0 ||
        code.read_uint16_to_reg_reg(3, 2) != 0 ||
        code.load_const_u16(4, 0x1234) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24b: read_uint16 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 4) != 0 ||
        code.read_uint32_to_reg_reg(3, 2) != 0 ||
        code.load_const_u32(4, 0x12345678) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24c: read_uint32 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 8) != 0 ||
        code.read_int64_to_reg_reg(3, 2) != 0 ||
        code.load_const_u64(4, 0x123456789ABCDEF0ULL) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24d: read_int64 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint8_to_reg_const(2, 0) != 0 ||
        code.write_uint8_reg_to_mem_const(2, 20) != 0 ||
        code.read_uint8_to_reg_const(3, 20) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24e: write_uint8 const offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint16_to_reg_const(2, 1) != 0 ||
        code.write_uint16_reg_to_mem_const(2, 20) != 0 ||
        code.read_uint16_to_reg_const(3, 20) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24f: write_uint16 const offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint32_to_reg_const(2, 4) != 0 ||
        code.write_uint32_reg_to_mem_const(2, 20) != 0 ||
        code.read_uint32_to_reg_const(3, 20) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24g: write_uint32 const offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u64(2, 0x123456789ABCDEF0ULL) != 0 ||
        code.write_int64_reg_to_mem_const(2, 20) != 0 ||
        code.read_int64_to_reg_const(3, 20) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24h: write_int64 const offset",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint8_to_reg_const(2, 0) != 0 ||
        code.load_const_u16(3, 20) != 0 ||
        code.write_uint8_reg_to_mem_reg(2, 3) != 0 ||
        code.read_uint8_to_reg_const(4, 20) != 0 ||
        code.branch_eq(2, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24i: write_uint8 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint16_to_reg_const(2, 1) != 0 ||
        code.load_const_u16(3, 20) != 0 ||
        code.write_uint16_reg_to_mem_reg(2, 3) != 0 ||
        code.read_uint16_to_reg_const(4, 20) != 0 ||
        code.branch_eq(2, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24j: write_uint16 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint32_to_reg_const(2, 4) != 0 ||
        code.load_const_u16(3, 20) != 0 ||
        code.write_uint32_reg_to_mem_reg(2, 3) != 0 ||
        code.read_uint32_to_reg_const(4, 20) != 0 ||
        code.branch_eq(2, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24k: write_uint32 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u64(2, 0x123456789ABCDEF0ULL) != 0 ||
        code.load_const_u16(3, 20) != 0 ||
        code.write_int64_reg_to_mem_reg(2, 3) != 0 ||
        code.read_int64_to_reg_const(4, 20) != 0 ||
        code.branch_eq(2, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24l: write_int64 register offset",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u64(0, 0xFFFFFFFFFFFFFF85ULL) != 0 ||
        code.write_reg_to_mem_any_const(0, 0) != 0 ||
        code.read_int64_to_reg_const(1, 0) != 0 ||
        code.branch_eq(0, 1, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24m: WRITE_REG_TO_MEM_ANY signed",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.read_uint8_to_reg_const(2, 0) != 0 ||
        code.write_reg_to_mem_any_const(2, 8) != 0 ||
        code.read_int64_to_reg_const(3, 8) != 0 ||
        code.load_const_u16(4, 0xFA) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24n: WRITE_REG_TO_MEM_ANY unsigned",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 1) != 0 ||
        code.convert_size(3, 2) != 0 ||
        code.load_const_u16(4, 0x1234) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24o: convert_size",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 20) != 0 ||
        code.load_const_u16(1, 300) != 0 ||
        code.write_size_mem(1, 0) != 0 ||
        code.read_uint16_to_reg_const(2, 20) != 0 ||
        code.load_const_u16(3, 300) != 0 ||
        code.branch_eq(2, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24p: write_size_mem",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 20) != 0 ||
        code.load_const_u64(1, 0xFFFFFFFFFFFFCFC7ULL) != 0 ||
        code.int64_to_str(2, 0, 1) != 0 ||
        code.str_to_int64(3, 0, 2) != 0 ||
        code.branch_eq(1, 3, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 24q: int64_to_str round trip",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.read_uint16_to_reg_const(0, 65535) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 24r: read_uint16 const offset bounds",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u64(0, 65535) != 0 ||
        code.read_uint16_to_reg_reg(1, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 24s: read_uint16 register offset bounds",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 65535) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.write_size_mem(1, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 24t: write_size_mem offset bounds",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 20) != 0 ||
        code.load_const_u16(1, 0) != 0 ||
        code.write_size_mem(1, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 24u: write_size_mem rejects zero size",
                           ndb, tab, &code) != 0) rc = -1;
  }

  return rc;
}

static void
putUint16(char *mem, Uint32 offset, Uint16 value)
{
  mem[offset] = (char)(value & 0xFF);
  mem[offset + 1] = (char)((value >> 8) & 0xFF);
}

static void
putUint32(char *mem, Uint32 offset, Uint32 value)
{
  mem[offset] = (char)(value & 0xFF);
  mem[offset + 1] = (char)((value >> 8) & 0xFF);
  mem[offset + 2] = (char)((value >> 16) & 0xFF);
  mem[offset + 3] = (char)((value >> 24) & 0xFF);
}

static void
putUint64(char *mem, Uint32 offset, Uint64 value)
{
  Uint32 i;
  for (i = 0; i < 8; i++) {
    mem[offset + i] = (char)((value >> (i * 8)) & 0xFF);
  }
}

static void
putOdd(char *mem, Uint32 offset, Uint64 value, Uint32 bytes)
{
  Uint32 i;
  for (i = 0; i < bytes; i++) {
    mem[offset + i] = (char)((value >> (i * 8)) & 0xFF);
  }
}

static int
testSearchSortStringLibraryOpcodes(Ndb *ndb,
                                   const NdbDictionary::Table *tab)
{
  Uint32 buf[256];
  int rc = 0;

  {
    char mem[6];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint16(mem, 0, 10);
    putUint16(mem, 2, 20);
    putUint16(mem, 4, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.binary_search_16(2, 0, 3, 4, 0) != 0 ||
        code.load_const_u16(5, 1) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25a: binary_search_16 exact success",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[6];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint16(mem, 0, 10);
    putUint16(mem, 2, 20);
    putUint16(mem, 4, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 30) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.binary_search_16(2, 0, 3, 4, 0) != 0 ||
        code.branch_eq_null(4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25b: binary_search_16 exact not found",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[12];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint32(mem, 0, 10);
    putUint32(mem, 4, 20);
    putUint32(mem, 8, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 30) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.binary_search_32(2, 0, 3, 4, 1) != 0 ||
        code.load_const_u16(5, 2) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25c: binary_search_32 smaller mode",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[24];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint64(mem, 0, 10);
    putUint64(mem, 8, 30);
    putUint64(mem, 16, 50);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 30) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.binary_search_64(2, 0, 3, 4, 4) != 0 ||
        code.load_const_u16(5, 1) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25d: binary_search_64 larger-equal mode",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[9];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putOdd(mem, 0, 5, 3);
    putOdd(mem, 3, 300, 3);
    putOdd(mem, 6, 70000, 3);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u32(2, 70000) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.binary_search_odd(2, 0, 3, 4, 0, 3) != 0 ||
        code.load_const_u16(5, 2) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25e: binary_search_odd exact success",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[9];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putOdd(mem, 0, 5, 3);
    putOdd(mem, 3, 300, 3);
    putOdd(mem, 6, 70000, 3);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 301) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.binary_search_odd(2, 0, 3, 4, 2, 3) != 0 ||
        code.load_const_u16(5, 2) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25f: binary_search_odd larger mode",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    char mem[8];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint16(mem, 0, 10);
    putUint16(mem, 2, 20);
    putUint16(mem, 4, 30);
    putUint16(mem, 6, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 15) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_16(2, 0, 3, 4, 0) != 0 ||
        code.load_const_u16(5, 0) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25g: search_interval_16 left-closed success",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[8];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint16(mem, 0, 10);
    putUint16(mem, 2, 20);
    putUint16(mem, 4, 30);
    putUint16(mem, 6, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 25) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_16(2, 0, 3, 4, 0) != 0 ||
        code.branch_eq_null(4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25h: search_interval_16 not found",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[16];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint32(mem, 0, 10);
    putUint32(mem, 4, 20);
    putUint32(mem, 8, 30);
    putUint32(mem, 12, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_32(2, 0, 3, 4, 1) != 0 ||
        code.load_const_u16(5, 0) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25i: search_interval_32 right-closed success",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[32];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint64(mem, 0, 10);
    putUint64(mem, 8, 20);
    putUint64(mem, 16, 30);
    putUint64(mem, 24, 40);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 35) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_64(2, 0, 3, 4, 0) != 0 ||
        code.load_const_u16(5, 2) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25j: search_interval_64 left-closed success",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[12];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putOdd(mem, 0, 10, 3);
    putOdd(mem, 3, 20, 3);
    putOdd(mem, 6, 30, 3);
    putOdd(mem, 9, 40, 3);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 35) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_odd(2, 0, 3, 4, 0, 3) != 0 ||
        code.load_const_u16(5, 2) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25k: search_interval_odd left-closed success",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    char mem[3] = { 3, 1, 2 };
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 3) != 0 ||
        code.qsort_instr(0, 2, 1) != 0 ||
        code.read_uint8_to_reg_const(3, 1) != 0 ||
        code.load_const_u16(4, 2) != 0 ||
        code.branch_eq(3, 4, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25l: qsort 1-byte elements",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[12];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint32(mem, 0, 300);
    putUint32(mem, 4, 100);
    putUint32(mem, 8, 200);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 3) != 0 ||
        code.qsort_instr(0, 2, 4) != 0 ||
        code.load_const_u16(3, 200) != 0 ||
        code.binary_search_32(3, 0, 2, 4, 0) != 0 ||
        code.load_const_u16(5, 1) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25m: qsort 4-byte elements",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[24];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint64(mem, 0, 300);
    putUint64(mem, 8, 100);
    putUint64(mem, 16, 200);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 3) != 0 ||
        code.qsort_instr(0, 2, 8) != 0 ||
        code.load_const_u16(3, 300) != 0 ||
        code.binary_search_64(3, 0, 2, 4, 0) != 0 ||
        code.load_const_u16(5, 2) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25n: qsort 8-byte elements",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    char mem[12];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint32(mem, 0, 1);
    putUint32(mem, 4, 0x00020304);
    putUint32(mem, 8, 0x00050607);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 3) != 0 ||
        code.compress_num_array(0, 2, 4, 3) != 0 ||
        code.load_const_u32(3, 0x00020304) != 0 ||
        code.binary_search_odd(3, 0, 2, 4, 0, 3) != 0 ||
        code.load_const_u16(5, 1) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25o: compress_num_array 4-to-3",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[24];
    NdbInterpretedCode code(tab, buf, 256);
    memset(mem, 0, sizeof(mem));
    putUint64(mem, 0, 1);
    putUint64(mem, 8, 0x0000000203040506ULL);
    putUint64(mem, 16, 0x0000000506070809ULL);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 3) != 0 ||
        code.compress_num_array(0, 2, 8, 5) != 0 ||
        code.load_const_u64(3, 0x0000000203040506ULL) != 0 ||
        code.binary_search_odd(3, 0, 2, 4, 0, 5) != 0 ||
        code.load_const_u16(5, 1) != 0 ||
        code.branch_eq(4, 5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25p: compress_num_array 8-to-5",
                     ndb, tab, &code) != 0) rc = -1;
  }

  {
    char mem[12] = { 'a', 'b', 'c', 'd', 'e', 'f',
                     'a', 'b', 'c', 'd', 'e', 'f' };
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 12) != 0 ||
        code.load_const_u16(3, 3) != 0 ||
        code.load_const_u16(4, 3) != 0 ||
        code.string_search(0, 2, 3, 4, 5) != 0 ||
        code.load_const_u16(6, 3) != 0 ||
        code.branch_eq(5, 6, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25q: string_search success",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    char mem[12] = { 'a', 'b', 'c', 'd', 'e', 'f',
                     'x', 'y', 'z', '0', '0', '0' };
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 6) != 0 ||
        code.load_const_u16(3, 6) != 0 ||
        code.load_const_u16(4, 3) != 0 ||
        code.string_search(0, 2, 3, 4, 5) != 0 ||
        code.branch_eq_null(5, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 25r: string_search not found",
                     ndb, tab, &code) != 0) rc = -1;
  }

  return rc;
}

static int
testErrorHandlerMatrix(Ndb *ndb, const NdbDictionary::Table *tab)
{
  Uint32 buf[256];
  int rc = 0;
  char mem[16];
  memset(mem, 0, sizeof(mem));
  putUint16(mem, 0, 10);
  putUint16(mem, 2, 20);
  putUint16(mem, 4, 30);
  putUint16(mem, 6, 40);

  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.add_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 26a: ADD over NULL registers returns NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_null(0) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.and_reg(2, 0, 1) != 0 ||
        code.branch_eq_null(2, 0) != 0 ||
        finishAcceptReject(&code, 0) != 0 ||
        expectAllPks("Test 26b: AND over NULL returns NULL",
                     ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_double_const(0, 1.0) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.lshift_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26c: shift rejects DOUBLE lhs",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 1) != 0 ||
        code.load_const_u16(1, 64) != 0 ||
        code.lshift_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26d: lshift_reg rejects shift by 64",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 16) != 0 ||
        code.rshift_const_reg(1, 0, 64) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e: rshift_const_reg rejects shift by 64",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 16) != 0 ||
        code.load_const_u16(1, 64) != 0 ||
        code.rshift_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e.1: rshift_reg rejects shift by 64",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 1) != 0 ||
        code.load_const_u64(1, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.lshift_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e.2: lshift_reg rejects negative shift",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 16) != 0 ||
        code.load_const_u64(1, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.rshift_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e.3: rshift_reg rejects negative shift",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_double_const(0, 1.0) != 0 ||
        code.lshift_const_reg(1, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e.4: lshift_const_reg rejects DOUBLE",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 1) != 0 ||
        code.load_double_const(1, 1.0) != 0 ||
        code.or_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e.5: or_reg rejects DOUBLE rhs",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_double_const(0, 1.0) != 0 ||
        code.xor_const_reg(1, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26e.6: xor_const_reg rejects DOUBLE",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u64(0, 3037000500ULL) != 0 ||
        code.load_const_u64(1, 3037000500ULL) != 0 ||
        code.mul_reg(2, 0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26f: signed multiply overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 2, 3, mem, sizeof(mem)) != 0 ||
        code.load_double_const(0, 1.0) != 0 ||
        code.read_uint16_to_reg_const(1, 0) != 0 ||
        code.write_uint16_reg_to_mem_reg(1, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26g: memory offset rejects DOUBLE",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u64(0, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.load_const_u16(1, 1) != 0 ||
        code.bzero(0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26h: bzero rejects negative offset",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 65535) != 0 ||
        code.load_const_u16(1, 2) != 0 ||
        code.bzero(0, 1) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26i: bzero rejects range overflow",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 20) != 0 ||
        code.load_const_u32(1, 65535) != 0 ||
        code.write_size_mem(1, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26j: write_size_mem rejects huge size",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u64(2, 0xFFFFFFFFFFFFFFFFULL) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.binary_search_16(2, 0, 3, 4, 0) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26k: binary_search rejects negative ordinal",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.binary_search_16(2, 0, 3, 4, 9) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26l: binary_search rejects search mode",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.search_interval_16(2, 0, 3, 4, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26m: search_interval rejects mode",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 20) != 0 ||
        code.load_const_u16(3, 4) != 0 ||
        code.binary_search_odd(2, 0, 3, 4, 0, 2) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26n: binary_search_odd rejects size",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 4) != 0 ||
        code.qsort_instr(0, 2, 7) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26o: qsort rejects unsupported size",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (loadMemoryConst(&code, 0, 1, mem, sizeof(mem)) != 0 ||
        code.load_const_u16(2, 4) != 0 ||
        code.compress_num_array(0, 2, 4, 4) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26p: compress_num_array rejects sizes",
                           ndb, tab, &code) != 0) rc = -1;
  }
  {
    NdbInterpretedCode code(tab, buf, 256);
    if (code.load_const_u16(0, 65535) != 0 ||
        code.load_const_u16(1, 2) != 0 ||
        code.load_const_u16(2, 0) != 0 ||
        code.load_const_u16(3, 1) != 0 ||
        code.string_search(0, 1, 2, 3, 4) != 0 ||
        finishRuntimeErrorProgram(&code) != 0 ||
        expectRuntimeError("Test 26q: string_search rejects memory bounds",
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
  { 20, testFloatDoubleMatrix },
  { 21, testColumnPredicateTypeMatrix },
  { 22, testStringAndBinaryTypeMatrix },
  { 23, testDateTimeAndDecimalTriage },
  { 24, testMemoryOpcodeExpansion },
  { 25, testSearchSortStringLibraryOpcodes },
  { 26, testErrorHandlerMatrix }
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
