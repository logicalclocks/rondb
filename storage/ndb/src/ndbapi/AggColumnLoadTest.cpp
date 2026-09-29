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
 * The shared column decode of include/util/AggColumnLoad.hpp, used by the
 * DBTUP aggregation interpreters and by aggregation in the RonSQL layer
 * (RONDB-1124 WP-J J5): every loadable type from its NDB storage bytes to
 * the register the aggregation kernels accumulate.
 */

#include <ndb_global.h>
#include <AggColumnLoad.hpp>

#include <cmath>
#include <cstdio>
#include <cstring>

#define CHECK(condition)                                                \
  do {                                                                  \
    if (!(condition)) {                                                 \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);        \
      return false;                                                     \
    }                                                                   \
  } while (0)

static decimal_digit_t g_dec_buf[9];

static decimal_t scratchDecimal() {
  decimal_t d;
  d.buf = g_dec_buf;
  d.len = 9;
  return d;
}

static AggLoadStatus load(DataType type, const Uint8* data, Uint32 size,
                          Register* reg, Int32 precision = 0,
                          Int32 scale = 0, bool is_null = false) {
  decimal_t scratch = scratchDecimal();
  return aggLoadColumnValue(type, aggIsUnsignedType(type), precision, scale,
                            data, size, is_null, &scratch, reg);
}

static bool runIntegerCases() {
  Register r;
  Uint8 b[8];

  b[0] = 0x80;  // -128
  CHECK(load(NDB_TYPE_TINYINT, b, 1, &r) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_BIGINT && !r.is_unsigned && !r.is_null);
  CHECK(r.value.val_int64 == -128);

  int2store(b, static_cast<uint16>(-300));
  CHECK(load(NDB_TYPE_SMALLINT, b, 2, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_int64 == -300);

  int3store(b, static_cast<uint32>(-70000) & 0xFFFFFF);
  CHECK(load(NDB_TYPE_MEDIUMINT, b, 3, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_int64 == -70000);

  int4store(b, static_cast<uint32>(-5));
  CHECK(load(NDB_TYPE_INT, b, 4, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_int64 == -5);

  int8store(b, static_cast<uint64>(INT64_MIN));
  CHECK(load(NDB_TYPE_BIGINT, b, 8, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_int64 == INT64_MIN);

  b[0] = 0xFF;
  CHECK(load(NDB_TYPE_TINYUNSIGNED, b, 1, &r) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_BIGINT && r.is_unsigned);
  CHECK(r.value.val_uint64 == 255);

  int2store(b, 65535);
  CHECK(load(NDB_TYPE_SMALLUNSIGNED, b, 2, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_uint64 == 65535);

  int3store(b, 16777215);
  CHECK(load(NDB_TYPE_MEDIUMUNSIGNED, b, 3, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_uint64 == 16777215);

  int4store(b, 4294967295U);
  CHECK(load(NDB_TYPE_UNSIGNED, b, 4, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_uint64 == 4294967295ULL);

  int8store(b, UINT64_MAX);
  CHECK(load(NDB_TYPE_BIGUNSIGNED, b, 8, &r) == AggLoadStatus::Ok);
  CHECK(r.is_unsigned && r.value.val_uint64 == UINT64_MAX);
  return true;
}

static bool runFloatCases() {
  Register r;
  Uint8 b[8];
  float f = 1.25f;
  floatstore(b, f);
  CHECK(load(NDB_TYPE_FLOAT, b, 4, &r) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_DOUBLE && !r.is_unsigned);
  CHECK(r.value.val_double == 1.25);

  double d = -2.5;
  doublestore(b, d);
  CHECK(load(NDB_TYPE_DOUBLE, b, 8, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_double == -2.5);
  return true;
}

static bool runTemporalCases() {
  Register r;
  Uint8 b[8];

  const Uint32 date = (2026u << 9) | (6u << 5) | 1u;
  int3store(b, date);
  CHECK(load(NDB_TYPE_DATE, b, 3, &r) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_BIGINT && r.is_unsigned);
  CHECK(r.value.val_uint64 == date);

  b[0] = 126;
  CHECK(load(NDB_TYPE_YEAR, b, 1, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_uint64 == 126);

  // Packed temporals are big-endian and folded over their stored width:
  // TIMESTAMP(0) 4 bytes, TIMESTAMP(3) 6, DATETIME(0) 5.
  const Uint8 ts0[4] = {0x6A, 0x0B, 0x1C, 0x2D};
  CHECK(load(NDB_TYPE_TIMESTAMP2, ts0, 4, &r) == AggLoadStatus::Ok);
  CHECK(r.is_unsigned && r.value.val_uint64 == 0x6A0B1C2DULL);

  const Uint8 ts3[6] = {0x6A, 0x0B, 0x1C, 0x2D, 0x01, 0xF4};
  CHECK(load(NDB_TYPE_TIMESTAMP2, ts3, 6, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_uint64 == 0x6A0B1C2D01F4ULL);

  const Uint8 dt0[5] = {0x99, 0xB4, 0x42, 0x00, 0x00};
  CHECK(load(NDB_TYPE_DATETIME2, dt0, 5, &r) == AggLoadStatus::Ok);
  CHECK(r.value.val_uint64 == 0x99B4420000ULL);
  return true;
}

static bool encodeDecimal(const char* text, int precision, int scale,
                          Uint8* out) {
  decimal_digit_t buf[9];
  decimal_t d;
  d.buf = buf;
  d.len = 9;
  const char* end = text + strlen(text);
  if (string2decimal(text, &d, &end) != E_DEC_OK) return false;
  return decimal2bin(&d, out, precision, scale) == E_DEC_OK;
}

static bool runDecimalCases() {
  Register r;
  Uint8 b[16];

  CHECK(encodeDecimal("1234.56", 12, 2, b));
  CHECK(load(NDB_TYPE_DECIMAL, b,
             static_cast<Uint32>(decimal_bin_size(12, 2)), &r,
             12, 2) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_DOUBLE && !r.is_unsigned);
  CHECK(r.value.val_double == 1234.56);

  CHECK(encodeDecimal("-123456789012", 18, 0, b));
  CHECK(load(NDB_TYPE_DECIMAL, b,
             static_cast<Uint32>(decimal_bin_size(18, 0)), &r,
             18, 0) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_BIGINT && !r.is_unsigned);
  CHECK(r.value.val_int64 == -123456789012LL);

  CHECK(encodeDecimal("42", 10, 0, b));
  CHECK(load(NDB_TYPE_DECIMALUNSIGNED, b,
             static_cast<Uint32>(decimal_bin_size(10, 0)), &r,
             10, 0) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_BIGINT && r.is_unsigned);
  CHECK(r.value.val_uint64 == 42);

  CHECK(encodeDecimal("7.25", 10, 2, b));
  CHECK(load(NDB_TYPE_DECIMALUNSIGNED, b,
             static_cast<Uint32>(decimal_bin_size(10, 2)), &r,
             10, 2) == AggLoadStatus::Ok);
  CHECK(r.type == NDB_TYPE_DOUBLE && r.value.val_double == 7.25);

  // A negative value in an unsigned DECIMAL column is a conversion error,
  // as the interpreter reports it (ZAGG_DECIMAL_CONV_ERROR).
  CHECK(encodeDecimal("-5", 10, 0, b));
  CHECK(load(NDB_TYPE_DECIMALUNSIGNED, b,
             static_cast<Uint32>(decimal_bin_size(10, 0)), &r,
             10, 0) == AggLoadStatus::DecimalConvError);
  return true;
}

static bool runNullStringAndTypeCases() {
  Register r;
  Uint8 b[16] = {};

  CHECK(load(NDB_TYPE_INT, b, 4, &r, 0, 0, /*is_null=*/true) ==
        AggLoadStatus::Ok);
  CHECK(r.is_null && r.type == NDB_TYPE_BIGINT && r.value.val_int64 == 0);
  CHECK(load(NDB_TYPE_VARCHAR, b, 1, &r, 0, 0, /*is_null=*/true) ==
        AggLoadStatus::Ok);
  CHECK(r.is_null && r.type == NDB_TYPE_VARCHAR);

  const Uint8 vc[4] = {3, 'a', 'b', 'c'};
  CHECK(load(NDB_TYPE_VARCHAR, vc, 4, &r) == AggLoadStatus::String);
  CHECK(r.type == NDB_TYPE_VARCHAR && !r.is_null && r.value.val_int64 == 0);
  Uint16 prefix = 0, payload = 0;
  aggStringPayload(NDB_TYPE_VARCHAR, vc, 0, &prefix, &payload);
  CHECK(prefix == 1 && payload == 3);

  const Uint8 lvc[2] = {0x2C, 0x01};  // 300
  aggStringPayload(NDB_TYPE_LONGVARCHAR, lvc, 0, &prefix, &payload);
  CHECK(prefix == 2 && payload == 300);

  aggStringPayload(NDB_TYPE_CHAR, b, 10, &prefix, &payload);
  CHECK(prefix == 0 && payload == 10);

  CHECK(!aggTypeSupported(NDB_TYPE_BLOB));
  CHECK(aggAlignedType(NDB_TYPE_BLOB, 0) == NDB_TYPE_UNDEFINED);
  CHECK(load(NDB_TYPE_BLOB, b, 8, &r) == AggLoadStatus::WrongType);
  CHECK(aggTypeSupported(NDB_TYPE_TIMESTAMP2) &&
        aggIsUnsignedType(NDB_TYPE_TIMESTAMP2));
  CHECK(!aggIsUnsignedType(NDB_TYPE_DECIMAL) &&
        aggIsUnsignedType(NDB_TYPE_DECIMALUNSIGNED));
  return true;
}

int main() {
  if (ndb_init() != 0) return 1;
  bool passed = true;
  if (!runIntegerCases()) passed = false;
  if (!runFloatCases()) passed = false;
  if (!runTemporalCases()) passed = false;
  if (!runDecimalCases()) passed = false;
  if (!runNullStringAndTypeCases()) passed = false;
  ndb_end(0);
  printf("%s\n", passed
      ? "PASSED: integer, float, temporal, decimal, NULL, string and "
        "type-support decode cases"
      : "FAILED");
  return passed ? 0 : 1;
}
