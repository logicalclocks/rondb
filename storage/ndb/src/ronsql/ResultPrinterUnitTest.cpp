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
 * T-16: the formatting helpers of the result printer and the typed HAVING
 * values (ResultPrinterInternal.hpp), run without a cluster: DOUBLE text
 * (RONSQL-44, O-4), DECIMAL at a scale (O-5, O-6), ORDER BY at a scale
 * (O-9), JSON, JSON_ASCII and TEXT escaping (F9, O-2), binary values
 * (base64 and the mysql client's escaping), packed temporal values
 * (RONSQL-45) and HAVING arithmetic and comparisons (O-8).
 */

#include "ResultPrinterInternal.hpp"
#include "RonSQLParser.y.hpp"

#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <sstream>
#include <string>

#include "my_time.h"
#include "mysql/strings/dtoa.h"
#include "mysql/strings/m_ctype.h"

using ronsql_printer_internal::compare_having_typed;
using ronsql_printer_internal::decode_temporal_packed;
using ronsql_printer_internal::double_to_fixed;
using ronsql_printer_internal::format_fixed_scale;
using ronsql_printer_internal::having_arithmetic;
using ronsql_printer_internal::having_negate;
using ronsql_printer_internal::print_base64;
using ronsql_printer_internal::print_binary_tsv;
using ronsql_printer_internal::print_json_ascii;
using ronsql_printer_internal::print_string;
using ronsql_printer_internal::print_temporal_packed;

typedef ResultPrinter::TemporalDisplay TempDisp;
typedef HavingValue::Kind HKind;
typedef RonSQLExecParams::OutputFormat OutFmt;

static int g_failures = 0;

#define CHECK(condition)                                               \
  do {                                                                 \
    if (!(condition)) {                                                \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);     \
      g_failures++;                                                    \
    }                                                                  \
  } while (0)

#define CHECK_STR(actual, expected) \
  check_str(__LINE__, (actual), (expected))

// statement must throw a RonSQLPermanentError of class cls.
#define CHECK_THROWS(statement, cls)                                   \
  do {                                                                 \
    bool thrown = false;                                               \
    try {                                                              \
      statement;                                                       \
    } catch (const RonSQLPermanentError& e) {                          \
      thrown = true;                                                   \
      if (e.error_class() != (cls)) {                                  \
        fprintf(stderr, "FAIL line %d: %s threw class %s: %s\n",       \
                __LINE__, #statement, e.error_class_name(), e.what()); \
        g_failures++;                                                  \
      }                                                                \
    }                                                                  \
    if (!thrown) {                                                     \
      fprintf(stderr, "FAIL line %d: %s did not throw\n", __LINE__,    \
              #statement);                                             \
      g_failures++;                                                    \
    }                                                                  \
  } while (0)

// s with every byte outside printable ASCII as \xNN.
static std::string
visible(const std::string& s)
{
  std::string out;
  for (size_t i = 0; i < s.size(); i++)
  {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    if (c >= 0x20 && c < 0x7f)
    {
      out.push_back(static_cast<char>(c));
    }
    else
    {
      char hex[16];
      snprintf(hex, sizeof(hex), "\\x%02x", static_cast<unsigned int>(c));
      out += hex;
    }
  }
  return out;
}

static void
check_str(int line, const std::string& actual, const std::string& expected)
{
  if (actual == expected) return;
  fprintf(stderr, "FAIL line %d: got \"%s\", expected \"%s\"\n", line,
          visible(actual).c_str(), visible(expected).c_str());
  g_failures++;
}

/*
 * double_to_fixed
 */

static std::string
as_fixed(double value, bool* error)
{
  char buf[FLOATING_POINT_BUFFER + 1];
  bool err = false;
  const size_t len = double_to_fixed(value, buf, &err);
  if (error != NULL) *error = err;
  return std::string(buf, len);
}

static void
test_double_to_fixed()
{
  CHECK_STR(as_fixed(0.0, NULL), "0");
  CHECK_STR(as_fixed(1.5, NULL), "1.5");
  CHECK_STR(as_fixed(-1.5, NULL), "-1.5");
  CHECK_STR(as_fixed(0.1, NULL), "0.1");
  CHECK_STR(as_fixed(123456.789, NULL), "123456.789");
  CHECK_STR(as_fixed(999999999999999.0, NULL), "999999999999999");
  // RONSQL-44: below 0.1 the shortest digits of my_gcvt(), in fixed
  // point also where my_gcvt() writes an exponent (below 1e-15).
  CHECK_STR(as_fixed(0.05, NULL), "0.05");
  CHECK_STR(as_fixed(1e-15, NULL), "0." + std::string(14, '0') + "1");
  CHECK_STR(as_fixed(1.5e-16, NULL), "0." + std::string(15, '0') + "15");
  CHECK_STR(as_fixed(1e-20, NULL), "0." + std::string(19, '0') + "1");
  CHECK_STR(as_fixed(-2.5e-17, NULL), "-0." + std::string(16, '0') + "25");
  // The longest text, 327 characters.
  const std::string dbl_min = as_fixed(-2.2250738585072014e-308, NULL);
  CHECK_STR(dbl_min, "-0." + std::string(307, '0') + "22250738585072014");
  CHECK(dbl_min.size() == 327);
  // O-4: from 1e15 up, mysqld's text: an exponent when the shortest
  // digits end before the decimal point.
  CHECK_STR(as_fixed(1e15, NULL), "1e15");
  CHECK_STR(as_fixed(-1.5e15, NULL), "-1.5e15");
  CHECK_STR(as_fixed(1000000000000000.5, NULL), "1000000000000000.5");
  CHECK_STR(as_fixed(9007199254740992.0, NULL), "9.007199254740992e15");
  CHECK_STR(as_fixed(1e20, NULL), "1e20");
  CHECK_STR(as_fixed(1e100, NULL), "1e100");
  // Inf and NaN are an error (the printer writes NULL).
  bool error = false;
  as_fixed(std::numeric_limits<double>::infinity(), &error);
  CHECK(error);
  error = false;
  as_fixed(-std::numeric_limits<double>::infinity(), &error);
  CHECK(error);
  error = false;
  as_fixed(std::numeric_limits<double>::quiet_NaN(), &error);
  CHECK(error);
  error = true;
  as_fixed(2.0, &error);
  CHECK(!error);
}

/*
 * format_fixed_scale and ronsql_compare_at_scale
 */

static std::string
fixed_scale(double value, int scale)
{
  char buf[FLOATING_POINT_BUFFER];
  const size_t len = format_fixed_scale(value, scale, buf);
  return std::string(buf, len);
}

static void
test_fixed_scale()
{
  // O-5: the shortest digits rounded HALF_UP, not the binary value
  // rounded to even.
  CHECK_STR(fixed_scale(0.03125, 4), "0.0313");
  CHECK_STR(fixed_scale(1.005, 2), "1.01");
  CHECK_STR(fixed_scale(2.5, 0), "3");
  CHECK_STR(fixed_scale(-2.5, 0), "-3");
  CHECK_STR(fixed_scale(123.456, 1), "123.5");
  CHECK_STR(fixed_scale(-1234.5678, 2), "-1234.57");
  CHECK_STR(fixed_scale(999.995, 2), "1000.00");
  CHECK_STR(fixed_scale(0.1 + 0.2, 2), "0.30");
  CHECK_STR(fixed_scale(5.0, 3), "5.000");
  // O-6: a value that rounds to zero has no sign.
  CHECK_STR(fixed_scale(-2.8e-17, 2), "0.00");
  CHECK_STR(fixed_scale(-0.001, 2), "0.00");
  // Inf and NaN: 0 (the printer writes NULL).
  CHECK_STR(fixed_scale(std::numeric_limits<double>::infinity(), 2), "");
  CHECK_STR(fixed_scale(std::numeric_limits<double>::quiet_NaN(), 2), "");
}

static void
test_compare_at_scale()
{
  // O-9: sums that are equal DECIMALs compare equal at the scale.
  CHECK(ronsql_compare_at_scale(0.1 + 0.2, 0.3, 2) == 0);
  CHECK(ronsql_compare_at_scale(0.3, 0.1 + 0.2, 2) == 0);
  CHECK(ronsql_compare_at_scale(1.004, 1.001, 2) == 0);
  CHECK(ronsql_compare_at_scale(-0.001, 0.001, 2) == 0);
  CHECK(ronsql_compare_at_scale(2.0, 2.0, 2) == 0);
  CHECK(ronsql_compare_at_scale(1.004, 1.006, 2) == -1);
  CHECK(ronsql_compare_at_scale(1.006, 1.004, 2) == 1);
  CHECK(ronsql_compare_at_scale(1.0, 2.0, 2) == -1);
  CHECK(ronsql_compare_at_scale(2.0, 1.0, 2) == 1);
}

/*
 * String escaping
 */

static std::string
json(const std::string& s, OutFmt format)
{
  std::ostringstream out;
  ronsql_print_json_string(out, s.data(), s.size(), format);
  return out.str();
}

static std::string
printed(const std::string& s, CHARSET_INFO* cs, bool json_escape,
        bool utf8_output, bool trim_space_suffix)
{
  std::ostringstream out;
  print_string(out, LexString(s.data(), s.size()), cs, json_escape,
               utf8_output, trim_space_suffix);
  return out.str();
}

static void
test_json_escape()
{
  const OutFmt J = OutFmt::JSON;
  const OutFmt A = OutFmt::JSON_ASCII;
  CHECK_STR(json("plain", J), "plain");
  CHECK_STR(json("a\"b\\c/d", J), "a\\\"b\\\\c/d");
  CHECK_STR(json("\b\f\n\r\t", J), "\\b\\f\\n\\r\\t");
  CHECK_STR(json(std::string("x\0y", 3), J), "x\\u0000y");
  CHECK_STR(json("\x01\x1f\x7f", J), "\\u0001\\u001f\\u007f");
  // C1 controls are escaped in both formats.
  CHECK_STR(json("\xc2\x85", J), "\\u0085");
  CHECK_STR(json("\xc2\x85", A), "\\u0085");
  // JSON keeps UTF-8, JSON_ASCII escapes every character from U+00A0.
  CHECK_STR(json("\xc2\xa0", J), "\xc2\xa0");
  CHECK_STR(json("\xc2\xa0", A), "\\u00a0");
  CHECK_STR(json("\xc3\xa9", J), "\xc3\xa9");
  CHECK_STR(json("\xc3\xa9", A), "\\u00e9");
  CHECK_STR(json("\xe2\x82\xac", J), "\xe2\x82\xac");
  CHECK_STR(json("\xe2\x82\xac", A), "\\u20ac");
  // Outside the BMP: a surrogate pair under JSON_ASCII.
  CHECK_STR(json("\xf0\x9f\x98\x80", J), "\xf0\x9f\x98\x80");
  CHECK_STR(json("\xf0\x9f\x98\x80", A), "\\ud83d\\ude00");
  // A byte that starts no UTF-8 character is U+FFFD; a character cut
  // short at the end is one U+FFFD.
  CHECK_STR(json("a\x80z", J), "a\xef\xbf\xbdz");
  CHECK_STR(json("a\x80z", A), "a\\ufffdz");
  CHECK_STR(json("a\xc3", J), "a\xef\xbf\xbd");
  CHECK_STR(json("a\xc3", A), "a\\ufffd");
}

static void
test_text_escape()
{
  CHARSET_INFO* utf8 = &my_charset_utf8mb4_bin;
  CHARSET_INFO* latin1 = &my_charset_latin1;
  // TEXT: the mysql client's batch-mode escaping.
  CHECK_STR(printed("a\tb\nc\\d", utf8, false, true, false),
            "a\\tb\\nc\\\\d");
  CHECK_STR(printed(std::string("x\0y", 3), utf8, false, true, false),
            "x\\0y");
  CHECK_STR(printed("\r\"'", utf8, false, true, false), "\r\"'");
  CHECK_STR(printed("\xc3\xa9", utf8, false, true, false), "\xc3\xa9");
  CHECK_STR(printed("\xc2\x85", utf8, false, true, false), "\xc2\x85");
  // CHAR values drop trailing spaces, and only those.
  CHECK_STR(printed("ab  ", utf8, false, true, true), "ab");
  CHECK_STR(printed("a b ", utf8, false, true, true), "a b");
  CHECK_STR(printed("ab  ", utf8, false, true, false), "ab  ");
  CHECK_STR(printed("x \t ", utf8, true, true, true), "x \\t");
  // Another character set is converted to UTF-8 or \u-escaped; latin1
  // is cp1252 (0x80 is the euro sign).
  CHECK_STR(printed("\xe9", latin1, true, true, false), "\xc3\xa9");
  CHECK_STR(printed("\xe9", latin1, true, false, false), "\\u00e9");
  CHECK_STR(printed("\x80", latin1, true, true, false), "\xe2\x82\xac");
  CHECK_STR(printed("\x80", latin1, true, false, false), "\\u20ac");
  CHECK_STR(printed("\xe9", latin1, false, true, false), "\xc3\xa9");
}

static void
test_json_ascii()
{
  std::ostringstream out;
  print_json_ascii(out, "{\"a\":\"\xc3\xa9\xf0\x9f\x98\x80\",\"b\":1}");
  CHECK_STR(out.str(), "{\"a\":\"\\u00e9\\ud83d\\ude00\",\"b\":1}");
  // ASCII, escapes included, is copied as it is.
  std::ostringstream ascii;
  print_json_ascii(ascii, "[\"x\\\\y\\n\",null,-1.5e3]");
  CHECK_STR(ascii.str(), "[\"x\\\\y\\n\",null,-1.5e3]");
}

/*
 * Binary values
 */

static std::string
base64(const std::string& s)
{
  std::ostringstream out;
  print_base64(out, reinterpret_cast<const unsigned char*>(s.data()),
               s.size());
  return out.str();
}

static void
test_binary()
{
  // RFC 4648 test vectors: no line breaks, "=" padding.
  CHECK_STR(base64(""), "");
  CHECK_STR(base64("f"), "Zg==");
  CHECK_STR(base64("fo"), "Zm8=");
  CHECK_STR(base64("foo"), "Zm9v");
  CHECK_STR(base64("foob"), "Zm9vYg==");
  CHECK_STR(base64("fooba"), "Zm9vYmE=");
  CHECK_STR(base64("foobar"), "Zm9vYmFy");
  CHECK_STR(base64(std::string("\x00\xff\xfe", 3)), "AP/+");
  // 60 bytes give 80 characters on one line.
  const std::string long_value(60, 'a');
  const std::string long_base64 = base64(long_value);
  CHECK(long_base64.size() == 80);
  CHECK(long_base64.find('\n') == std::string::npos);

  const unsigned char bytes[] = {'a', 0x00, '\t', '\n', '\\', '\r', 0xff};
  std::ostringstream tsv;
  print_binary_tsv(tsv, bytes, sizeof(bytes));
  CHECK_STR(tsv.str(), "a\\0\\t\\n\\\\\r\xff");
}

/*
 * Packed temporal values
 */

static std::string
temporal(Uint64 w, TempDisp display, int fsp, const char* quote)
{
  std::ostringstream out;
  print_temporal_packed(out, w, display, fsp, quote);
  return out.str();
}

// The big-endian value of n bytes, as the kernel loads a DATETIME2,
// TIME2 or TIMESTAMP2 column into a register.
static Uint64
big_endian(const unsigned char* bytes, Uint32 n)
{
  Uint64 w = 0;
  for (Uint32 i = 0; i < n; i++)
    w = (w << 8) | bytes[i];
  return w;
}

static Uint64
datetime_packed(unsigned int year, unsigned int month, unsigned int day,
                unsigned int hour, unsigned int minute, unsigned int second,
                unsigned long usec, int fsp)
{
  MYSQL_TIME mt = MYSQL_TIME();
  mt.year = year;
  mt.month = month;
  mt.day = day;
  mt.hour = hour;
  mt.minute = minute;
  mt.second = second;
  mt.second_part = usec;
  mt.time_type = MYSQL_TIMESTAMP_DATETIME;
  unsigned char bytes[8];
  my_datetime_packed_to_binary(TIME_to_longlong_datetime_packed(mt), bytes,
                               static_cast<unsigned int>(fsp));
  return big_endian(bytes, 5u + static_cast<Uint32>(fsp + 1) / 2u);
}

static Uint64
time_packed(bool neg, unsigned int hour, unsigned int minute,
            unsigned int second, unsigned long usec, int fsp)
{
  MYSQL_TIME mt = MYSQL_TIME();
  mt.neg = neg;
  mt.hour = hour;
  mt.minute = minute;
  mt.second = second;
  mt.second_part = usec;
  mt.time_type = MYSQL_TIMESTAMP_TIME;
  unsigned char bytes[8];
  my_time_packed_to_binary(TIME_to_longlong_time_packed(mt), bytes,
                           static_cast<unsigned int>(fsp));
  return big_endian(bytes, 3u + static_cast<Uint32>(fsp + 1) / 2u);
}

static Uint64
timestamp_packed(Int64 sec, Int64 usec, int fsp)
{
  my_timeval tv;
  tv.m_tv_sec = sec;
  tv.m_tv_usec = usec;
  unsigned char bytes[8];
  my_timestamp_to_binary(&tv, bytes, static_cast<unsigned int>(fsp));
  return big_endian(bytes, 4u + static_cast<Uint32>(fsp + 1) / 2u);
}

static void
test_temporal_print()
{
  // DATE: (year << 9) | (month << 5) | day.
  CHECK_STR(temporal(0xFD573, TempDisp::DATE, 0, ""), "2026-11-19");
  CHECK_STR(temporal(0xFD573, TempDisp::DATE, 0, "\""), "\"2026-11-19\"");
  CHECK_STR(temporal(0, TempDisp::DATE, 0, ""), "0000-00-00");
  // YEAR (RONSQL-45): 0 is 0000, any other byte the year - 1900.
  CHECK_STR(temporal(0, TempDisp::YEAR, 0, ""), "0000");
  CHECK_STR(temporal(1, TempDisp::YEAR, 0, ""), "1901");
  CHECK_STR(temporal(126, TempDisp::YEAR, 0, "\""), "\"2026\"");
  CHECK_STR(temporal(255, TempDisp::YEAR, 0, ""), "2155");
  // DATETIME2: the 5 + (fsp + 1) / 2 bytes, offset 0x8000000000.
  CHECK_STR(temporal(0x99BB66C8B8ULL, TempDisp::DATETIME2, 0, ""),
            "2026-11-19 12:34:56");
  CHECK(datetime_packed(2026, 11, 19, 12, 34, 56, 0, 0) == 0x99BB66C8B8ULL);
  CHECK_STR(temporal(datetime_packed(2026, 11, 19, 12, 34, 56, 789000, 3),
                     TempDisp::DATETIME2, 3, ""),
            "2026-11-19 12:34:56.789");
  CHECK_STR(temporal(datetime_packed(2026, 11, 19, 12, 34, 56, 123456, 6),
                     TempDisp::DATETIME2, 6, ""),
            "2026-11-19 12:34:56.123456");
  CHECK_STR(temporal(datetime_packed(1000, 1, 1, 0, 0, 0, 500000, 1),
                     TempDisp::DATETIME2, 1, "\""),
            "\"1000-01-01 00:00:00.5\"");
  CHECK_STR(temporal(datetime_packed(0, 0, 0, 0, 0, 0, 0, 0),
                     TempDisp::DATETIME2, 0, ""),
            "0000-00-00 00:00:00");
  // TIME2, also negative and above 24 hours.
  CHECK_STR(temporal(time_packed(false, 12, 34, 56, 0, 0),
                     TempDisp::TIME2, 0, ""),
            "12:34:56");
  CHECK_STR(temporal(time_packed(true, 12, 34, 56, 789000, 3),
                     TempDisp::TIME2, 3, ""),
            "-12:34:56.789");
  CHECK_STR(temporal(time_packed(false, 838, 59, 59, 0, 0),
                     TempDisp::TIME2, 0, ""),
            "838:59:59");
  CHECK_STR(temporal(time_packed(true, 0, 0, 1, 500000, 1),
                     TempDisp::TIME2, 1, ""),
            "-00:00:01.5");
  // TIMESTAMP2: the UTC epoch, big-endian, then the fraction; 0 is the
  // zero timestamp, not 1970-01-01 00:00:00.
  CHECK_STR(temporal(0x6553F100, TempDisp::TIMESTAMP2, 0, ""),
            "2023-11-14 22:13:20");
  CHECK(timestamp_packed(1700000000, 0, 0) == 0x6553F100ULL);
  CHECK_STR(temporal(timestamp_packed(1700000000, 120000, 2),
                     TempDisp::TIMESTAMP2, 2, ""),
            "2023-11-14 22:13:20.12");
  CHECK_STR(temporal(timestamp_packed(1700000000, 123456, 6),
                     TempDisp::TIMESTAMP2, 6, ""),
            "2023-11-14 22:13:20.123456");
  CHECK_STR(temporal(timestamp_packed(1, 0, 0), TempDisp::TIMESTAMP2, 0, ""),
            "1970-01-01 00:00:01");
  CHECK_STR(temporal(timestamp_packed(2147483647, 0, 0),
                     TempDisp::TIMESTAMP2, 0, ""),
            "2038-01-19 03:14:07");
  CHECK_STR(temporal(0, TempDisp::TIMESTAMP2, 0, ""), "0000-00-00 00:00:00");
}

static void
test_temporal_decode()
{
  MYSQL_TIME mt;
  decode_temporal_packed(0xFD573, TempDisp::DATE, 0, &mt);
  CHECK(mt.year == 2026 && mt.month == 11 && mt.day == 19);
  CHECK(mt.time_type == MYSQL_TIMESTAMP_DATE);
  decode_temporal_packed(126, TempDisp::YEAR, 0, &mt);
  CHECK(mt.year == 2026 && mt.month == 0 && mt.day == 0);
  decode_temporal_packed(0, TempDisp::YEAR, 0, &mt);
  CHECK(mt.year == 0);
  decode_temporal_packed(datetime_packed(2026, 11, 19, 12, 34, 56, 789000, 3),
                         TempDisp::DATETIME2, 3, &mt);
  CHECK(mt.year == 2026 && mt.month == 11 && mt.day == 19);
  CHECK(mt.hour == 12 && mt.minute == 34 && mt.second == 56);
  CHECK(mt.second_part == 789000);
  CHECK(mt.time_type == MYSQL_TIMESTAMP_DATETIME);
  decode_temporal_packed(time_packed(true, 12, 34, 56, 0, 0),
                         TempDisp::TIME2, 0, &mt);
  CHECK(mt.neg && mt.hour == 12 && mt.minute == 34 && mt.second == 56);
  CHECK(mt.time_type == MYSQL_TIMESTAMP_TIME);
  decode_temporal_packed(timestamp_packed(1700000000, 123456, 6),
                         TempDisp::TIMESTAMP2, 6, &mt);
  CHECK(mt.year == 2023 && mt.month == 11 && mt.day == 14);
  CHECK(mt.hour == 22 && mt.minute == 13 && mt.second == 20);
  CHECK(mt.second_part == 123456);
}

/*
 * HAVING values (O-8)
 */

static void
set_exact(HavingValue& v, const char* text)
{
  const char* end = text + strlen(text);
  CHECK(string2decimal(text, v.exact(), &end) == E_DEC_OK);
}

static void
set_approx(HavingValue& v, double d)
{
  v.kind = HKind::APPROX;
  v.dbl = d;
}

static void
set_temporal(HavingValue& v, Uint64 w, TempDisp display, int fsp)
{
  v.kind = HKind::TEMPORAL;
  v.temporal = display;
  decode_temporal_packed(w, display, fsp, &v.time);
}

static void
set_string(HavingValue& v, const char* s)
{
  v.kind = HKind::STRING;
  v.str = s;
  v.len = static_cast<Uint32>(strlen(s));
  v.charset = NULL;
  v.node = NULL;
}

static std::string
exact_text(HavingValue& v)
{
  char buf[128];
  int len = static_cast<int>(sizeof(buf));
  if (v.kind != HKind::EXACT ||
      decimal2string(v.decimal(), buf, &len) != E_DEC_OK)
    return "(not exact)";
  return std::string(buf, static_cast<size_t>(len));
}

// The comparison of an EXACT value with text.
static int
compare_exact(HavingValue& v, const char* text)
{
  HavingValue other;
  set_exact(other, text);
  return compare_having_typed(v, other);
}

static void
test_having_arithmetic()
{
  {
    HavingValue a, b, out;
    set_exact(a, "1.10");
    set_exact(b, "2.205");
    having_arithmetic(T_PLUS, a, b, out);
    CHECK_STR(exact_text(out), "3.305");
  }
  {
    HavingValue a, b, out;
    set_exact(a, "1");
    set_exact(b, "3");
    having_arithmetic(T_MINUS, a, b, out);
    CHECK_STR(exact_text(out), "-2");
  }
  {
    HavingValue a, b, out;
    set_exact(a, "1.5");
    set_exact(b, "-2");
    having_arithmetic(T_MULTIPLY, a, b, out);
    CHECK_STR(exact_text(out), "-3.0");
  }
  {
    // Beyond 2^53 the sum stays exact.
    HavingValue a, b, out;
    set_exact(a, "9007199254740992");
    set_exact(b, "1");
    having_arithmetic(T_PLUS, a, b, out);
    CHECK_STR(exact_text(out), "9007199254740993");
  }
  {
    // / keeps four more digits, truncated at a word of nine digits.
    HavingValue a, b, out;
    set_exact(a, "1");
    set_exact(b, "3");
    having_arithmetic(T_SLASH, a, b, out);
    CHECK(out.kind == HKind::EXACT);
    CHECK(compare_exact(out, "0.333333333") == 0);
    CHECK(compare_exact(out, "0.3333333334") == -1);
    CHECK(compare_exact(out, "0.3333") == 1);
  }
  {
    HavingValue a, b, out;
    set_exact(a, "10");
    set_exact(b, "4");
    having_arithmetic(T_SLASH, a, b, out);
    CHECK(compare_exact(out, "2.5") == 0);
  }
  {
    // Division by zero is NULL, exact or not.
    HavingValue a, b, out;
    set_exact(a, "5");
    set_exact(b, "0");
    having_arithmetic(T_SLASH, a, b, out);
    CHECK(out.kind == HKind::NULL_VALUE);
  }
  {
    HavingValue a, b, out;
    set_approx(a, 5.0);
    set_exact(b, "0");
    having_arithmetic(T_SLASH, a, b, out);
    CHECK(out.kind == HKind::NULL_VALUE);
  }
  {
    // A FLOAT or DOUBLE operand makes a DOUBLE.
    HavingValue a, b, out;
    set_exact(a, "1");
    set_approx(b, 0.5);
    having_arithmetic(T_PLUS, a, b, out);
    CHECK(out.kind == HKind::APPROX && out.dbl == 1.5);
  }
  {
    // NaN is NULL.
    HavingValue a, b, out;
    set_approx(a, std::numeric_limits<double>::infinity());
    set_approx(b, std::numeric_limits<double>::infinity());
    having_arithmetic(T_MINUS, a, b, out);
    CHECK(out.kind == HKind::NULL_VALUE);
  }
  {
    // NULL in, NULL out.
    HavingValue a, b, out;
    set_exact(b, "1");
    having_arithmetic(T_PLUS, a, b, out);
    CHECK(out.kind == HKind::NULL_VALUE);
  }
  {
    // A temporal operand is its number, exactly: DATE YYYYMMDD.
    HavingValue a, b, out;
    set_temporal(a, 0xFD573, TempDisp::DATE, 0);
    set_exact(b, "1");
    having_arithmetic(T_PLUS, a, b, out);
    CHECK(compare_exact(out, "20261120") == 0);
  }
  {
    // YEAR is its year.
    HavingValue a, b, out;
    set_temporal(a, 126, TempDisp::YEAR, 0);
    set_exact(b, "2");
    having_arithmetic(T_MULTIPLY, a, b, out);
    CHECK_STR(exact_text(out), "4052");
  }
  {
    // TIME is [-]hhmmss.
    HavingValue a, b, out;
    set_temporal(a, time_packed(true, 12, 34, 56, 0, 0), TempDisp::TIME2, 0);
    set_exact(b, "0");
    having_arithmetic(T_PLUS, a, b, out);
    CHECK(compare_exact(out, "-123456") == 0);
  }
  {
    HavingValue a, b, out;
    set_string(a, "12");
    set_exact(b, "1");
    CHECK_THROWS(having_arithmetic(T_PLUS, a, b, out),
                 RonSQLErrorClass::UNSUPPORTED);
  }
}

static void
test_having_negate()
{
  {
    HavingValue v;
    set_exact(v, "5");
    having_negate(v);
    CHECK_STR(exact_text(v), "-5");
  }
  {
    // No negative zero.
    HavingValue v;
    set_exact(v, "0");
    having_negate(v);
    CHECK_STR(exact_text(v), "0");
    CHECK(compare_exact(v, "0") == 0);
  }
  {
    HavingValue v;
    set_approx(v, 2.5);
    having_negate(v);
    CHECK(v.kind == HKind::APPROX && v.dbl == -2.5);
  }
  {
    HavingValue v;
    having_negate(v);
    CHECK(v.kind == HKind::NULL_VALUE);
  }
  {
    HavingValue v;
    set_temporal(v, 126, TempDisp::YEAR, 0);
    having_negate(v);
    CHECK_STR(exact_text(v), "-2026");
  }
}

static void
test_having_compare()
{
  // Integers and DECIMALs exactly.
  {
    HavingValue a;
    set_exact(a, "0.30");
    CHECK(compare_exact(a, "0.3") == 0);
  }
  {
    HavingValue a;
    set_exact(a, "9007199254740993");
    CHECK(compare_exact(a, "9007199254740992") == 1);
  }
  // Any other two numbers as doubles.
  {
    HavingValue a, b;
    set_exact(a, "0.1");
    set_approx(b, 0.1);
    CHECK(compare_having_typed(a, b) == 0);
  }
  {
    HavingValue a, b;
    set_approx(a, 1.5);
    set_exact(b, "2");
    CHECK(compare_having_typed(a, b) == -1);
  }
  // A temporal value with a number as a double of its number, a YEAR as
  // an integer.
  {
    HavingValue a;
    set_temporal(a, 0xFD573, TempDisp::DATE, 0);
    CHECK(compare_exact(a, "20261119") == 0);
  }
  {
    HavingValue a;
    set_temporal(a, 0xFD573, TempDisp::DATE, 0);
    CHECK(compare_exact(a, "20261120") == -1);
  }
  {
    HavingValue a;
    set_temporal(a, 126, TempDisp::YEAR, 0);
    CHECK(compare_exact(a, "2026") == 0);
  }
  {
    HavingValue a, b;
    set_temporal(a, 126, TempDisp::YEAR, 0);
    set_approx(b, 2025.5);
    CHECK(compare_having_typed(a, b) == 1);
  }
  // DATE, DATETIME and TIMESTAMP as datetimes.
  {
    HavingValue a, b;
    set_temporal(a, 0xFD573, TempDisp::DATE, 0);
    set_temporal(b, datetime_packed(2026, 11, 19, 0, 0, 0, 0, 0),
                 TempDisp::DATETIME2, 0);
    CHECK(compare_having_typed(a, b) == 0);
  }
  {
    HavingValue a, b;
    set_temporal(a, 0xFD573, TempDisp::DATE, 0);
    set_temporal(b, datetime_packed(2026, 11, 19, 0, 0, 1, 0, 0),
                 TempDisp::DATETIME2, 0);
    CHECK(compare_having_typed(a, b) == -1);
  }
  {
    HavingValue a, b;
    set_temporal(a, timestamp_packed(1700000000, 0, 0),
                 TempDisp::TIMESTAMP2, 0);
    set_temporal(b, datetime_packed(2023, 11, 14, 22, 13, 20, 0, 0),
                 TempDisp::DATETIME2, 0);
    CHECK(compare_having_typed(a, b) == 0);
  }
  {
    HavingValue a, b;
    set_temporal(a, time_packed(true, 0, 0, 1, 0, 0), TempDisp::TIME2, 0);
    set_temporal(b, time_packed(false, 0, 0, 0, 0, 0), TempDisp::TIME2, 0);
    CHECK(compare_having_typed(a, b) == -1);
  }
  // A TIME only with a TIME, a YEAR only with a YEAR or a number.
  {
    HavingValue a, b;
    set_temporal(a, time_packed(false, 1, 0, 0, 0, 0), TempDisp::TIME2, 0);
    set_temporal(b, 0xFD573, TempDisp::DATE, 0);
    CHECK_THROWS(compare_having_typed(a, b), RonSQLErrorClass::UNSUPPORTED);
  }
  {
    HavingValue a, b;
    set_temporal(a, 126, TempDisp::YEAR, 0);
    set_temporal(b, 0xFD573, TempDisp::DATE, 0);
    CHECK_THROWS(compare_having_typed(a, b), RonSQLErrorClass::UNSUPPORTED);
  }
  // A string with a DATE, DATETIME or TIMESTAMP as a datetime.
  {
    HavingValue a, b;
    set_string(a, "2026-11-19");
    set_temporal(b, 0xFD573, TempDisp::DATE, 0);
    CHECK(compare_having_typed(a, b) == 0);
  }
  {
    HavingValue a, b;
    set_string(a, "2026-11-19 12:00:00");
    set_temporal(b, datetime_packed(2026, 11, 19, 11, 0, 0, 0, 0),
                 TempDisp::DATETIME2, 0);
    CHECK(compare_having_typed(a, b) == 1);
    CHECK(compare_having_typed(b, a) == -1);
  }
  {
    HavingValue a, b;
    set_string(a, "2026-13-01");
    set_temporal(b, 0xFD573, TempDisp::DATE, 0);
    CHECK_THROWS(compare_having_typed(a, b), RonSQLErrorClass::UNSUPPORTED);
  }
  // A string beside a number, a TIME or a YEAR is refused.
  {
    HavingValue a, b;
    set_string(a, "12");
    set_exact(b, "12");
    CHECK_THROWS(compare_having_typed(a, b), RonSQLErrorClass::UNSUPPORTED);
  }
  {
    HavingValue a, b;
    set_temporal(a, time_packed(false, 1, 0, 0, 0, 0), TempDisp::TIME2, 0);
    set_string(b, "01:00:00");
    CHECK_THROWS(compare_having_typed(a, b), RonSQLErrorClass::UNSUPPORTED);
  }
}

int
main()
{
  test_double_to_fixed();
  test_fixed_scale();
  test_compare_at_scale();
  test_json_escape();
  test_text_escape();
  test_json_ascii();
  test_binary();
  test_temporal_print();
  test_temporal_decode();
  test_having_arithmetic();
  test_having_negate();
  test_having_compare();
  if (g_failures != 0)
  {
    printf("FAIL: %d checks failed\n", g_failures);
    return 1;
  }
  printf("OK\n");
  return 0;
}
