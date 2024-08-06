/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

// todo: Make sure this test gets run in the standard way
// todo: Maybe rename to testOverflow-t

#include "RonSQLCommon.hpp"

extern bool
int64_add_overflow(Int64 x, Int64 y);
extern bool
int64_sub_overflow(Int64 x, Int64 y);
extern bool
int64_mul_overflow(Int64 x, Int64 y);

/*
 * The approach to test Int64 overflow checks is:
 * - Convert to int128.
 * - Perform the operation, which will never overflow in int128.
 * - Check whether the int128 result is within [INT64_MIN, INT64_MAX] range.
 * - Compare that check with the overflow detecting function.
 * For all three operations, we test all combinations of certain interesting
 * numbers listed in `Int64 ns[]` below.
 *
 * We also check commutativity and perform a santiy check for the int128 logic.
 */

bool
has_overflown(__int128 result)
{
  constexpr __int128 int64min128 = (__int128)INT64_MIN;
  constexpr __int128 int64max128 = (__int128)INT64_MAX;
  return (result < int64min128) || (int64max128 < result);
}

int
main()
{
  bool ok = true;

  Int64 b = INT64_MIN;
  Int64 z = 0;
  Int64 t = INT64_MAX;

  Int64 ns[] = {
              b, b+1, b+2,  // Numbers around INT64_MIN
    z-2, z-1, z, z+1, z+2,  // Numbers around 0
    t-2, t-1, t,            // Numbers around INT64_MAX
    // divisors for INT64_MAX + 2 and INT64_MIN - 1
    129LL, -129LL, 71499008037633921LL, -71499008037633921LL,
    // divisors for INT64_MAX + 1 and INT64_MIN
    128LL, -128LL, 72057594037927936LL, -72057594037927936LL,
    // divisors for INT64_MAX     and INT64_MIN + 1
    337LL, -337LL, 27369056489183311LL, -27369056489183311LL,
    // divisors for INT64_MAX - 1 and INT64_MIN + 2
    6LL, -6LL, 1537228672809129301LL, -1537228672809129301LL,
    // divisors for UINT64_MAX + 2 and UINT64_MIN - 1
    274177LL, -274177LL, 67280421310721LL, -67280421310721LL,
    // divisors for UINT64_MAX + 1 and UINT64_MIN
    256LL, -256LL, 72057594037927936LL, -72057594037927936LL,
    // divisors for UINT64_MAX     and UINT64_MIN + 1
    255LL, -255LL, 72340172838076673LL, -72340172838076673LL,
    // divisors for UINT64_MAX - 1 and UINT64_MIN + 2
    337LL, -337LL, 54738112978366622LL, -54738112978366622LL,
    // Numbers around ±sqrt(INT64_MAX)
    3037000498LL, -3037000498LL,
    3037000499LL, -3037000499LL,
    3037000500LL, -3037000500LL,
    3037000501LL, -3037000501LL,
    3037000502LL, -3037000502LL,
    // Numbers around ±sqrt(UINT64_MAX)
    4294967294LL, -4294967294LL,
    4294967295LL, -4294967295LL,
    4294967296LL, -4294967296LL,
    4294967297LL, -4294967297LL,
    4294967298LL, -4294967298LL,
  };
  Uint32 ns_count = ARRAY_LEN(ns);
  for(Uint32 y_idx = 0; y_idx < ns_count; y_idx++)
  {
    Int64 y = ns[y_idx];
    for(Uint32 x_idx = 0; x_idx < ns_count; x_idx++)
    {
      Int64 x = ns[x_idx];
      // Use 128-bit SIMD to test overflow detection
      __int128 x128 = (__int128)x;
      __int128 y128 = (__int128)y;
      __int128 add128 = x128 + y128;
      bool add_actual_overflow = has_overflown(add128);
      if (int64_add_overflow(x, y) != add_actual_overflow)
      {
        ok = false;
        printf("int64_add_overflow(%lld, %lld) returned wrong result.\n", x, y);
      }
      __int128 sub128 = x128 - y128;
      bool sub_actual_overflow = has_overflown(sub128);
      if (int64_sub_overflow(x, y) != sub_actual_overflow)
      {
        ok = false;
        printf("int64_sub_overflow(%lld, %lld) returned wrong result.\n", x, y);
      }
      __int128 mul128 = x128 * y128;
      bool mul_actual_overflow = has_overflown(mul128);
      if (int64_mul_overflow(x, y) != mul_actual_overflow)
      {
        ok = false;
        printf("int64_mul_overflow(%lld, %lld) returned wrong result.\n", x, y);
      }
      // Test commutativity
      if (int64_add_overflow(x, y) !=
          int64_add_overflow(y, x))
      {
        ok = false;
        printf("int64_add_overflow not commutative wrt %lld and %lld.\n", x, y);
      }
      bool sub_should_commute = Int64((Uint64(x)-Uint64(y)))!=b;
      if (sub_should_commute)
      {
        if (int64_sub_overflow(x, y) !=
            int64_sub_overflow(y, x))
        {
          ok = false;
          printf("int64_sub_overflow not commutative wrt %lld and %lld.\n", x, y);
        }
      }
      else
      {
        if (int64_sub_overflow(x, y) ==
            int64_sub_overflow(y, x))
        {
          ok = false;
          printf("int64_sub_overflow commutative wrt %lld and %lld but should not.\n", x, y);
        }
      }
      if (int64_mul_overflow(x, y) !=
          int64_mul_overflow(y, x))
      {
        ok = false;
        printf("int64_mul_overflow not commutative wrt %lld and %lld.\n", x, y);
      }
    }
  }

  // Sanity check for has_overflown():
  assert(has_overflown(((__int128)INT64_MIN)-((__int128)1)));
  assert(!has_overflown((__int128)INT64_MIN));
  assert(!has_overflown((__int128)0));
  assert(!has_overflown((__int128)INT64_MAX));
  assert(has_overflown(((__int128)INT64_MAX)+((__int128)1)));

  if (!ok)
  {
    printf("FAIL\n");
    return 1;
  }
  printf("OK\n");
}
