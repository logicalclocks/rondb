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

#ifndef STORAGE_NDB_SRC_RONSQL_RESULTPRINTERINTERNAL_HPP
#define STORAGE_NDB_SRC_RONSQL_RESULTPRINTERINTERNAL_HPP 1

/*
 * T-16: the formatting and HAVING helpers of ResultPrinter.cpp, which
 * ResultPrinterUnitTest runs without a cluster.  Only ResultPrinter.cpp
 * and that test include this header; the rest of RonSQL and RDRS use
 * ResultPrinter.hpp.
 */

#include <cstddef>
#include <ostream>
#include <string>

#include "decimal.h"
#include "mysql_time.h"
#include "ResultPrinter.hpp"

/*
 * O-8: one HAVING operand, typed as MySQL types it.  HAVING compared and
 * computed every operand as a double: a BIGINT above 2^53 equalled its
 * neighbours, a SUM over a DECIMAL of 0.10 and 0.20 was not 0.3, a
 * temporal MIN / MAX was its packed integer, and a string MIN / MAX failed
 * as a bug.  An operand is now
 *  - EXACT, an integer or a DECIMAL (MySQL's INT_RESULT and
 *    DECIMAL_RESULT), held as a decimal: COUNT, a SUM, MIN or MAX the data
 *    nodes return as an integer, a SUM, MIN or MAX over an integer or
 *    DECIMAL column that they return as a double (taken at the column's
 *    scale, as print_fixed_scale() prints it), AVG over either, an
 *    integer literal, a literal with a fraction (a DECIMAL in MySQL; the
 *    lexer has no exponent form) and a number a subquery returned (its
 *    exact digits, X-4);
 *  - APPROX, a FLOAT or DOUBLE (REAL_RESULT): an aggregate over a FLOAT or
 *    DOUBLE column, and one over an expression whose result is a double
 *    (its type is not known here);
 *  - STRING: a string MIN / MAX, a string literal, or a string a subquery
 *    returned;
 *  - TEMPORAL: a MIN / MAX over a DATE, DATETIME, TIMESTAMP, TIME or YEAR
 *    column, decoded from the packed value the data nodes return;
 *  - or NULL.
 */
struct HavingValue
{
  enum class Kind { NULL_VALUE, EXACT, APPROX, STRING, TEMPORAL };
  Kind kind = Kind::NULL_VALUE;
  // EXACT: dec over digits, 81 digits as in a my_decimal.  A copy keeps
  // the buf of the original, so decimal() sets it before every use.
  decimal_digit_t digits[9];
  decimal_t dec;
  // APPROX
  double dbl = 0.0;
  // STRING: the bytes, in the charset of the MIN / MAX they come from, or
  // for a constant (charset NULL) in UTF-8, the string of node.
  const char* str = NULL;
  Uint32 len = 0;
  CHARSET_INFO* charset = NULL;
  const ConditionalExpression* node = NULL;
  // TEMPORAL: the decoded value; a YEAR sets only time.year.
  ResultPrinter::TemporalDisplay temporal =
      ResultPrinter::TemporalDisplay::NONE;
  MYSQL_TIME time;

  decimal_t* decimal()
  {
    dec.buf = digits;
    return &dec;
  }
  // Makes this an EXACT value for a decimal function to write.
  decimal_t* exact()
  {
    kind = Kind::EXACT;
    dec.len = 9;
    return decimal();
  }
  // decimal_cmp() refuses a negative zero, which a truncated quotient
  // can be.
  void clear_negative_zero()
  {
    if (dec.sign && decimal_is_zero(decimal()))
      dec.sign = false;
  }
};

namespace ronsql_printer_internal
{

// RONSQL-44, O-4: a DOUBLE result in mysqld's text: fixed point below
// 1e15, also below 0.1 (where my_gcvt() writes an exponent), and from
// 1e15 up my_gcvt() as Protocol_text::store_double writes it.  `to` holds
// FLOATING_POINT_BUFFER + 1 bytes.  Sets *error for Inf and NaN.
size_t double_to_fixed(double value, char* to, bool* error);

// O-5, O-6: a DECIMAL result carried as a double, at `scale` (0 to 30),
// as mysqld prints it: the shortest digits rounded HALF_UP, no negative
// zero.  `to` holds FLOATING_POINT_BUFFER bytes.  Returns the length, or
// 0 for Inf and NaN.
size_t format_fixed_scale(double value, int scale, char* to);

// The characters of ls, in `charset`, as the content of a JSON string
// (json_escape) or with the mysql client's batch-mode escaping;
// utf8_output false \u-escapes U+00A0 and above (JSON_ASCII);
// trim_space_suffix drops trailing spaces (CHAR).
void print_string(std::ostream& out,
                  LexString ls,
                  CHARSET_INFO* charset,
                  bool json_escape,
                  bool utf8_output,
                  bool trim_space_suffix);

// RFC 4648 base64 with "=" padding and no line breaks (binary values
// under JSON).
void print_base64(std::ostream& out, const unsigned char* bytes, size_t len);

// Binary values under TEXT: NUL, tab, newline and backslash escaped as
// the mysql client does in batch mode.
void print_binary_tsv(std::ostream& out, const unsigned char* bytes,
                      size_t len);

// AVRO() JSON under JSON_ASCII: every non-ASCII character \u-escaped.
void print_json_ascii(std::ostream& out, const std::string& json);

// A packed temporal value (the column's bytes as the kernel loads them
// into a register) in MySQL's text, between two `quote`s; `fsp` is the
// fractional-seconds precision of DATETIME2, TIME2 and TIMESTAMP2.
void print_temporal_packed(std::ostream& out, Uint64 w,
                           ResultPrinter::TemporalDisplay temporal, int fsp,
                           const char* quote);

// O-8: the same value decoded into *mt; a YEAR sets only mt->year.
void decode_temporal_packed(Uint64 w, ResultPrinter::TemporalDisplay temporal,
                            int fsp, MYSQL_TIME* mt);

// O-8: unary minus of a HAVING operand.
void having_negate(HavingValue& v);

// O-8: left op right (T_PLUS, T_MINUS, T_MULTIPLY, any other op divides)
// into out, as MySQL computes it.
void having_arithmetic(TokenKind op, HavingValue& left, HavingValue& right,
                       HavingValue& out);

// O-8: -1, 0 or 1 as the non-NULL left is less than, equal to or greater
// than the non-NULL right, compared as MySQL compares the two types.  Two
// strings are compared by ResultPrinter::compare_having_values, in the
// collation of the MIN or MAX.
int compare_having_typed(HavingValue& left, HavingValue& right);

}  // namespace ronsql_printer_internal

#endif
