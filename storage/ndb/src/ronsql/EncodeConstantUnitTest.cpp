/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

/*
 * T-16: RonSQLPreparer::encode_constant, the bytes a literal is sent to
 * the data nodes as (scan filters, index bounds, keys, CASE conditions),
 * for each column type, run without a cluster.  The preparer is one
 * prepared for EXPLAIN without a connection, and the columns are
 * NdbDictionary::Column objects built here.
 */

#include "RonSQLPreparer.hpp"
#include "RonSQLParser.y.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <sstream>
#include <string>

#include <ndb_init.h>
#include "mysql/strings/m_ctype.h"

// The private members of RonSQLPreparer this test calls (a friend).
class RonSQLPreparerTestAccess
{
public:
  static raw_value encode_constant(RonSQLPreparer& preparer,
                                   ConditionalExpression* ce,
                                   const NdbDictionary::Column* col)
  {
    return preparer.encode_constant(ce, col);
  }
};

static int g_failures = 0;

#define CHECK(condition)                                               \
  do {                                                                 \
    if (!(condition)) {                                                \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);     \
      g_failures++;                                                    \
    }                                                                  \
  } while (0)

/*
 * A statement prepared for EXPLAIN without a cluster connection, as
 * ronsql_oom_test prepares one.
 */
struct Statement
{
  ArenaMalloc arena;
  std::string sql;
  std::ostringstream out;
  std::ostringstream err;
  RonSQLExecParams params;

  explicit Statement(const char* text)
    : arena(RonSQLExecParams::ARENA_MALLOC_PAGE_SIZE),
      sql(text)
  {
    sql.append(2, '\0');
    params.sql_buffer = &sql[0];
    params.sql_len = sql.size();
    params.amalloc = &arena;
    params.ndb = NULL;
    params.explain_mode = RonSQLExecParams::ExplainMode::FORCE;
    params.output_format = RonSQLExecParams::OutputFormat::TEXT;
    params.out_stream = &out;
    params.err_stream = &err;
  }
};

static RonSQLPreparer* g_preparer = NULL;

/*
 * Constants
 */

static ConditionalExpression
int_ce(Int64 value)
{
  ConditionalExpression ce = ConditionalExpression();
  ce.op = T_INT;
  ce.constant_integer = value;
  return ce;
}

// A number with a fraction (a literal) or an integer beyond Int64 (a
// subquery result, X-4), with its digits as text; text NULL for a
// substituted number without text.
static ConditionalExpression
float_ce(double value, const char* text)
{
  ConditionalExpression ce = ConditionalExpression();
  ce.op = T_FLOAT;
  ce.constant_float.dbl = value;
  ce.constant_float.ls.str = text;
  ce.constant_float.ls.len = (text != NULL) ? strlen(text) : 0;
  return ce;
}

static ConditionalExpression
string_ce(const char* text)
{
  ConditionalExpression ce = ConditionalExpression();
  ce.op = T_STRING;
  ce.string.str = text;
  ce.string.len = strlen(text);
  return ce;
}

static ConditionalExpression
string_ce(const std::string& text)
{
  ConditionalExpression ce = ConditionalExpression();
  ce.op = T_STRING;
  ce.string.str = text.data();
  ce.string.len = text.size();
  return ce;
}

static NdbDictionary::Column
column(const char* name, NdbDictionary::Column::Type type)
{
  NdbDictionary::Column col(name);
  col.setType(type);
  return col;
}

/*
 * Checks
 */

static raw_value
encode(ConditionalExpression ce, const NdbDictionary::Column& col)
{
  return RonSQLPreparerTestAccess::encode_constant(*g_preparer, &ce, &col);
}

// The literal encodes as the expected bytes.
static void
check_bytes(int line, ConditionalExpression ce,
            const NdbDictionary::Column& col, const void* expected,
            size_t expected_len)
{
  try
  {
    const raw_value rv = encode(ce, col);
    if (rv.len != expected_len ||
        memcmp(rv.val, expected, expected_len) != 0)
    {
      fprintf(stderr, "FAIL line %d: encoded as", line);
      const unsigned char* b = static_cast<const unsigned char*>(rv.val);
      for (size_t i = 0; i < rv.len; i++)
        fprintf(stderr, " %02x", static_cast<unsigned int>(b[i]));
      fprintf(stderr, " (%zu bytes, expected %zu)\n",
              static_cast<size_t>(rv.len), expected_len);
      g_failures++;
    }
  }
  catch (const std::exception& e)
  {
    fprintf(stderr, "FAIL line %d: threw %s\n", line, e.what());
    g_failures++;
  }
}

#define CHECK_BYTES(ce, col, literal) \
  check_bytes(__LINE__, (ce), (col), literal, sizeof(literal) - 1)

#define CHECK_VALUE(ce, col, value) \
  check_bytes(__LINE__, (ce), (col), &(value), sizeof(value))

/*
 * The literal is refused with a permanent error of class cls: its value
 * lies outside the column's domain, the type cannot take it, or the
 * column's type is not compared.  T-18: none of these is a
 * RonSQLMaybeStaleSchema, which would make the statement reload every
 * table and index it holds only to find the schema unchanged.
 */
static void
check_permanent(int line, ConditionalExpression ce,
                const NdbDictionary::Column& col, RonSQLErrorClass cls)
{
  try
  {
    encode(ce, col);
    fprintf(stderr, "FAIL line %d: no error\n", line);
    g_failures++;
  }
  catch (const RonSQLPermanentError& e)
  {
    if (e.error_class() != cls)
    {
      fprintf(stderr, "FAIL line %d: class %s: %s\n", line,
              e.error_class_name(), e.what());
      g_failures++;
    }
  }
  catch (const std::exception& e)
  {
    fprintf(stderr, "FAIL line %d: not a permanent error: %s\n", line,
            e.what());
    g_failures++;
  }
}

#define CHECK_PERMANENT(ce, col, cls) \
  check_permanent(__LINE__, (ce), (col), (cls))

typedef NdbDictionary::Column C;
static const RonSQLErrorClass kSemantic = RonSQLErrorClass::SEMANTIC;
static const RonSQLErrorClass kUnsupported = RonSQLErrorClass::UNSUPPORTED;

/*
 * Integers: the low bytes of the value; a literal outside the type's
 * range is refused, permanently.
 */
static void
test_integers()
{
  const C tiny = column("tiny", C::Tinyint);
  CHECK_BYTES(int_ce(127), tiny, "\x7f");
  CHECK_BYTES(int_ce(-128), tiny, "\x80");
  CHECK_PERMANENT(int_ce(128), tiny, kSemantic);
  CHECK_PERMANENT(int_ce(-129), tiny, kSemantic);
  const C utiny = column("utiny", C::Tinyunsigned);
  CHECK_BYTES(int_ce(255), utiny, "\xff");
  CHECK_PERMANENT(int_ce(-1), utiny, kSemantic);
  const C small = column("small", C::Smallint);
  CHECK_BYTES(int_ce(-32768), small, "\x00\x80");
  CHECK_PERMANENT(int_ce(32768), small, kSemantic);
  const C usmall = column("usmall", C::Smallunsigned);
  CHECK_BYTES(int_ce(65535), usmall, "\xff\xff");
  CHECK_PERMANENT(int_ce(65536), usmall, kSemantic);
  // MEDIUMINT is 3 bytes.
  const C medium = column("medium", C::Mediumint);
  CHECK_BYTES(int_ce(8388607), medium, "\xff\xff\x7f");
  CHECK_BYTES(int_ce(-8388608), medium, "\x00\x00\x80");
  CHECK_PERMANENT(int_ce(8388608), medium, kSemantic);
  const C umedium = column("umedium", C::Mediumunsigned);
  CHECK_BYTES(int_ce(16777215), umedium, "\xff\xff\xff");
  CHECK_PERMANENT(int_ce(16777216), umedium, kSemantic);
  const C i = column("i", C::Int);
  CHECK_BYTES(int_ce(INT32_MIN), i, "\x00\x00\x00\x80");
  CHECK_BYTES(int_ce(2147483647), i, "\xff\xff\xff\x7f");
  CHECK_PERMANENT(int_ce(2147483648LL), i, kSemantic);
  const C u = column("u", C::Unsigned);
  CHECK_BYTES(int_ce(4294967295LL), u, "\xff\xff\xff\xff");
  CHECK_PERMANENT(int_ce(4294967296LL), u, kSemantic);
  CHECK_PERMANENT(int_ce(-1), u, kSemantic);
  const C big = column("big", C::Bigint);
  CHECK_BYTES(int_ce(INT64_MIN), big, "\x00\x00\x00\x00\x00\x00\x00\x80");
  // X-4: an integer above 2^63 - 1 (a subquery result, digits as text)
  // only for BIGINT UNSIGNED.
  CHECK_PERMANENT(float_ce(9223372036854775808.0, "9223372036854775808"),
                  big, kSemantic);
  const C ubig = column("ubig", C::Bigunsigned);
  CHECK_BYTES(int_ce(5), ubig, "\x05\x00\x00\x00\x00\x00\x00\x00");
  CHECK_BYTES(float_ce(18446744073709551615.0, "18446744073709551615"), ubig,
              "\xff\xff\xff\xff\xff\xff\xff\xff");
  CHECK_PERMANENT(float_ce(18446744073709551616.0, "18446744073709551616"),
                  ubig, kSemantic);
  CHECK_PERMANENT(int_ce(-1), ubig, kSemantic);
  // Anything but an integer is refused.
  CHECK_PERMANENT(float_ce(1.5, "1.5"), ubig, kSemantic);
  CHECK_PERMANENT(float_ce(2.5, NULL), i, kSemantic);
  CHECK_PERMANENT(string_ce("1"), i, kSemantic);
}

// FLOAT and DOUBLE: the value in the column's type.
static void
test_floats()
{
  const C f = column("f", C::Float);
  const float three = 3.0f;
  CHECK_VALUE(int_ce(3), f, three);
  const float tenth = static_cast<float>(0.1);
  CHECK_VALUE(float_ce(0.1, "0.1"), f, tenth);
  const C d = column("d", C::Double);
  const double minus_seven = -7.0;
  CHECK_VALUE(int_ce(-7), d, minus_seven);
  const double tenth_d = 0.1;
  CHECK_VALUE(float_ce(0.1, "0.1"), d, tenth_d);
  CHECK_PERMANENT(string_ce("0.1"), d, kSemantic);
}

// DECIMAL: MySQL's binary format at the column's precision and scale.
static void
test_decimals()
{
  C dec = column("dec", C::Decimal);
  dec.setPrecision(10);
  dec.setScale(2);
  // 8 integer digits in 4 bytes, 2 fractional digits in 1, the sign bit
  // set for a positive number and every bit inverted for a negative one.
  CHECK_BYTES(int_ce(5), dec, "\x80\x00\x00\x05\x00");
  CHECK_BYTES(float_ce(-1.5, "-1.5"), dec, "\x7f\xff\xff\xfe\xcd");
  CHECK_BYTES(float_ce(-0.01, "-0.01"), dec, "\x7f\xff\xff\xff\xfe");
  // A number without text is written at the column's scale.
  CHECK_BYTES(float_ce(2.5, NULL), dec, "\x80\x00\x00\x02\x32");
  // More fractional bytes or integer digits than the column has.
  CHECK_PERMANENT(float_ce(1.005, "1.005"), dec, kSemantic);
  CHECK_PERMANENT(float_ce(123456789.5, "123456789.5"), dec, kSemantic);
  CHECK_PERMANENT(string_ce("5"), dec, kSemantic);
  C dec5 = column("dec5", C::Decimal);
  dec5.setPrecision(5);
  dec5.setScale(2);
  CHECK_BYTES(int_ce(999), dec5, "\x83\xe7\x00");
  CHECK_PERMANENT(int_ce(100000), dec5, kSemantic);
  C udec = column("udec", C::Decimalunsigned);
  udec.setPrecision(10);
  udec.setScale(2);
  CHECK_BYTES(int_ce(5), udec, "\x80\x00\x00\x05\x00");
  CHECK_PERMANENT(int_ce(-1), udec, kSemantic);
  CHECK_PERMANENT(float_ce(-0.5, "-0.5"), udec, kSemantic);
}

// CHAR, VARCHAR and LONGVARCHAR: the literal in the column's character
// set, CHAR padded with spaces, the others with their length prefix.
static void
test_strings()
{
  C ch = column("ch", C::Char);
  ch.setLength(5);
  ch.setCharset(&my_charset_latin1);
  CHECK_BYTES(string_ce("ab"), ch, "ab   ");
  CHECK_BYTES(string_ce(""), ch, "     ");
  CHECK_BYTES(string_ce("\xc3\xa9"), ch, "\xe9    ");
  CHECK_BYTES(string_ce("\xe2\x82\xac"), ch, "\x80    ");
  CHECK_PERMANENT(string_ce("abcdef"), ch, kSemantic);
  // A character latin1 cannot hold.
  CHECK_PERMANENT(string_ce("\xe3\x81\x82"), ch, kSemantic);
  CHECK_PERMANENT(int_ce(1), ch, kSemantic);
  CHECK_PERMANENT(float_ce(1.5, "1.5"), ch, kSemantic);

  C vc = column("vc", C::Varchar);
  vc.setLength(10);
  vc.setCharset(&my_charset_latin1);
  CHECK_BYTES(string_ce("abc"), vc, "\x03" "abc");
  CHECK_BYTES(string_ce(""), vc, "\x00");
  CHECK_BYTES(string_ce("\xc3\xa9"), vc, "\x01\xe9");
  const std::string too_long(256, 'x');
  CHECK_PERMANENT(string_ce(too_long), vc, kSemantic);

  C lvc = column("lvc", C::Longvarchar);
  lvc.setLength(1000);
  lvc.setCharset(&my_charset_latin1);
  CHECK_BYTES(string_ce("abc"), lvc, "\x03\x00" "abc");
  const std::string value(300, 'y');
  const std::string expected = std::string("\x2c\x01") + value;
  check_bytes(__LINE__, string_ce(value), lvc, expected.data(),
              expected.size());
}

// DATE, DATETIME and TIMESTAMP: the column's stored bytes, at its
// fractional-seconds precision (truncated).
static void
test_temporals()
{
  const C date = column("date", C::Date);
  // (year * 16 + month) * 32 + day, 3 bytes little-endian.
  CHECK_BYTES(string_ce("2026-11-19"), date, "\x73\xd5\x0f");
  ConditionalExpression date_add = ConditionalExpression();
  date_add.op = I_MYSQL_TIME;
  date_add.mysql_time.year = 2026;
  date_add.mysql_time.month = 11;
  date_add.mysql_time.day = 19;
  date_add.mysql_time.time_type = MYSQL_TIMESTAMP_DATE;
  CHECK_BYTES(date_add, date, "\x73\xd5\x0f");
  // The time part must be absent for a DATE and present otherwise.
  CHECK_PERMANENT(string_ce("2026-11-19 10:00:00"), date, kSemantic);
  CHECK_PERMANENT(string_ce("not a date"), date, kSemantic);
  CHECK_PERMANENT(int_ce(20261119), date, kSemantic);

  // DATETIME2: 5 bytes, offset 0x8000000000, then (precision + 1) / 2
  // bytes of fraction.
  C dt0 = column("dt0", C::Datetime2);
  dt0.setPrecision(0);
  CHECK_BYTES(string_ce("2026-11-19 12:34:56"), dt0, "\x99\xbb\x66\xc8\xb8");
  CHECK_PERMANENT(string_ce("2026-11-19"), dt0, kSemantic);
  C dt3 = column("dt3", C::Datetime2);
  dt3.setPrecision(3);
  CHECK_BYTES(string_ce("2026-11-19 12:34:56.789"), dt3,
              "\x99\xbb\x66\xc8\xb8\x1e\xd2");
  C dt2 = column("dt2", C::Datetime2);
  dt2.setPrecision(2);
  CHECK_BYTES(string_ce("2026-11-19 12:34:56.789"), dt2,
              "\x99\xbb\x66\xc8\xb8\x4e");

  // TIMESTAMP2: the UTC epoch, 4 bytes big-endian, then the fraction.
  C ts0 = column("ts0", C::Timestamp2);
  ts0.setPrecision(0);
  CHECK_BYTES(string_ce("2023-11-14 22:13:20"), ts0, "\x65\x53\xf1\x00");
  CHECK_BYTES(string_ce("2038-01-19 03:14:07"), ts0, "\x7f\xff\xff\xff");
  CHECK_PERMANENT(string_ce("1970-01-01 00:00:00"), ts0, kSemantic);
  CHECK_PERMANENT(string_ce("2038-01-19 03:14:08"), ts0, kSemantic);
  C ts2 = column("ts2", C::Timestamp2);
  ts2.setPrecision(2);
  CHECK_BYTES(string_ce("2023-11-14 22:13:20.12"), ts2,
              "\x65\x53\xf1\x00\x0c");
}

// Column types no comparison constant is encoded for.
static void
test_unsupported_types()
{
  CHECK_PERMANENT(int_ce(2026), column("y", C::Year), kUnsupported);
  CHECK_PERMANENT(string_ce("12:00:00"), column("t", C::Time2), kUnsupported);
  CHECK_PERMANENT(string_ce("ab"), column("bin", C::Binary), kUnsupported);
  CHECK_PERMANENT(int_ce(1), column("bit", C::Bit), kUnsupported);
}

int
main()
{
  if (ndb_init() != 0)
  {
    printf("FAIL: ndb_init\n");
    return 1;
  }
  int result = 0;
  {
    Statement statement("SELECT COUNT(*) FROM t;");
    try
    {
      RonSQLPreparer preparer(statement.params);
      g_preparer = &preparer;
      test_integers();
      test_floats();
      test_decimals();
      test_strings();
      test_temporals();
      test_unsupported_types();
      g_preparer = NULL;
    }
    catch (const std::exception& e)
    {
      fprintf(stderr, "FAIL: %s\n%s", e.what(), statement.err.str().c_str());
      g_failures++;
    }
  }
  if (g_failures != 0)
  {
    printf("FAIL: %d checks failed\n", g_failures);
    result = 1;
  }
  else
  {
    printf("OK\n");
  }
  ndb_end(0);
  return result;
}
