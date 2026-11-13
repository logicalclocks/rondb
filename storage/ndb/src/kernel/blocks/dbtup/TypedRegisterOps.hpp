/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.

 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.

 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.

 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef TYPED_REGISTER_OPS_HPP
#define TYPED_REGISTER_OPS_HPP

/*
 * Compare and arithmetic on two typed registers of the normal
 * interpreter (Phase I.18), shared by Dbtup's interpreter handlers and
 * JoinAggInterpreter::evalEmbeddedForNullRow, which evaluates an
 * embedded program for a NULL-extended row of an outer join without
 * DBTUP.  The register type words are those of Interpreter.hpp: byte 0
 * is the NOT_NULL flag, byte 1 the UNSIGNED flag and byte 2 the FLOAT
 * flag.  The error codes come from Dbtup.hpp.
 */

#include <climits>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <ndb_types.h>
#include <Interpreter.hpp>
#include "Dbtup.hpp"

/* Type-aware register-vs-register three-way comparison.  Returns
 * -1 / 0 / 1 by analogy with `memcmp`.  Both operands must be non-NULL
 * (the caller checks via `(leftType & rightType) != 0`).
 *
 * Single-byte loads keep the dispatch cheap on most CPUs. */
inline int compareTypedRegs(Uint32 leftType, Uint64 leftBits,
                            Uint32 rightType, Uint64 rightBits) {
  const Uint8* lw = reinterpret_cast<const Uint8*>(&leftType);
  const Uint8* rw = reinterpret_cast<const Uint8*>(&rightType);
  bool leftFloat  = lw[2] != 0;
  bool rightFloat = rw[2] != 0;
  if (unlikely(leftFloat || rightFloat)) {
    double l;
    double r;
    if (leftFloat) {
      memcpy(&l, &leftBits, 8);
    } else if (lw[1] != 0) {
      l = static_cast<double>(leftBits);
    } else {
      l = static_cast<double>(static_cast<Int64>(leftBits));
    }
    if (rightFloat) {
      memcpy(&r, &rightBits, 8);
    } else if (rw[1] != 0) {
      r = static_cast<double>(rightBits);
    } else {
      r = static_cast<double>(static_cast<Int64>(rightBits));
    }
    return (l < r) ? -1 : (l > r) ? 1 : 0;
  }
  bool leftUnsigned  = lw[1] != 0;
  bool rightUnsigned = rw[1] != 0;
  if (likely(leftUnsigned == rightUnsigned)) {
    if (leftUnsigned) {
      return (leftBits < rightBits) ? -1 :
             (leftBits > rightBits) ?  1 : 0;
    }
    Int64 ls = static_cast<Int64>(leftBits);
    Int64 rs = static_cast<Int64>(rightBits);
    return (ls < rs) ? -1 : (ls > rs) ? 1 : 0;
  }
  /* Mixed signed / unsigned: a negative signed operand is strictly
   * less than any unsigned operand.  Otherwise both fit non-negative
   * in Uint64 so the unsigned comparison is exact. */
  if (leftUnsigned) {
    if (static_cast<Int64>(rightBits) < 0) return 1;
    return (leftBits < rightBits) ? -1 :
           (leftBits > rightBits) ?  1 : 0;
  }
  if (static_cast<Int64>(leftBits) < 0) return -1;
  return (leftBits < rightBits) ? -1 :
         (leftBits > rightBits) ?  1 : 0;
}

/* Type-aware register arithmetic.  Op is one of '+' '-' '*' '/' '%'.
 * Both operands must be non-NULL.  Promotion rules:
 *
 *   - either operand FLOAT  → double arithmetic, result REG_TYPE_DOUBLE.
 *     Div/Mod by 0.0 returns -ZDIV_BY_ZERO_ERROR.
 *   - else either UNSIGNED  → exact mixed signed/unsigned integer
 *     arithmetic.  Negative results are tagged REG_TYPE_INT when
 *     they fit Int64; non-negative results are tagged REG_TYPE_UINT
 *     when they fit Uint64.  Add/Sub/Mul reject overflow.  Div/Mod
 *     by 0 rejected.
 *   - else both signed      → Int64 arithmetic, result REG_TYPE_INT.
 *     Add/Sub/Mul reject overflow.  Div/Mod reject divide-by-zero
 *     and the LLONG_MIN / -1 signed-overflow case.
 *
 * Returns 0 on success, negative error code on failure. */
inline int applyTypedArith(char op,
                           Uint32 leftType, Uint64 leftBits,
                           Uint32 rightType, Uint64 rightBits,
                           Uint32* resultType,
                           Uint64* resultBits) {
  const Uint8* lw = reinterpret_cast<const Uint8*>(&leftType);
  const Uint8* rw = reinterpret_cast<const Uint8*>(&rightType);
  bool leftFloat  = lw[2] != 0;
  bool rightFloat = rw[2] != 0;
  if (unlikely(leftFloat || rightFloat)) {
    double l;
    double r;
    if (leftFloat) {
      memcpy(&l, &leftBits, 8);
    } else if (lw[1] != 0) {
      l = static_cast<double>(leftBits);
    } else {
      l = static_cast<double>(static_cast<Int64>(leftBits));
    }
    if (rightFloat) {
      memcpy(&r, &rightBits, 8);
    } else if (rw[1] != 0) {
      r = static_cast<double>(rightBits);
    } else {
      r = static_cast<double>(static_cast<Int64>(rightBits));
    }
    double res;
    switch (op) {
      case '+': res = l + r; break;
      case '-': res = l - r; break;
      case '*': res = l * r; break;
      case '/':
        if (r == 0.0) return -ZDIV_BY_ZERO_ERROR;
        res = l / r;
        break;
      case '%':
        if (r == 0.0) return -ZDIV_BY_ZERO_ERROR;
        res = std::fmod(l, r);
        break;
      default: return -ZCALC_OVERFLOW_ERROR;
    }
    *resultType = Interpreter::REG_TYPE_DOUBLE;
    memcpy(resultBits, &res, 8);
    return 0;
  }
  bool leftUnsigned  = lw[1] != 0;
  bool rightUnsigned = rw[1] != 0;
  if (leftUnsigned || rightUnsigned) {
    __int128 l = leftUnsigned
        ? static_cast<__int128>(leftBits)
        : static_cast<__int128>(static_cast<Int64>(leftBits));
    __int128 r = rightUnsigned
        ? static_cast<__int128>(rightBits)
        : static_cast<__int128>(static_cast<Int64>(rightBits));
    __int128 res;
    switch (op) {
      case '+':
        res = l + r;
        break;
      case '-':
        res = l - r;
        break;
      case '*':
        res = l * r;
        break;
      case '/':
        if (r == 0) return -ZDIV_BY_ZERO_ERROR;
        res = l / r;
        break;
      case '%':
        if (r == 0) return -ZDIV_BY_ZERO_ERROR;
        res = l % r;
        break;
      default: return -ZCALC_OVERFLOW_ERROR;
    }
    if (res < 0) {
      if (unlikely(leftUnsigned && rightUnsigned)) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      if (unlikely(res < static_cast<__int128>(LLONG_MIN))) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      *resultType = Interpreter::REG_TYPE_INT;
      *resultBits = static_cast<Uint64>(static_cast<Int64>(res));
    } else {
      if (unlikely(static_cast<unsigned __int128>(res) >
                   static_cast<unsigned __int128>(UINT64_MAX))) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      *resultType = Interpreter::REG_TYPE_UINT;
      *resultBits = static_cast<Uint64>(res);
    }
    return 0;
  }
  /* Both signed integer. */
  Int64 l = static_cast<Int64>(leftBits);
  Int64 r = static_cast<Int64>(rightBits);
  Int64 res;
  switch (op) {
    case '+':
      if (unlikely((r >= 0 && LLONG_MAX - r < l) ||
                   (r <  0 && LLONG_MIN - r > l))) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      res = l + r;
      break;
    case '-':
      if (unlikely((r >= 0 && LLONG_MIN + r > l) ||
                   (r <  0 && LLONG_MAX + r < l))) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      res = l - r;
      break;
    case '*':
    {
      __int128 wide =
          static_cast<__int128>(l) * static_cast<__int128>(r);
      if (unlikely(wide > static_cast<__int128>(LLONG_MAX) ||
                   wide < static_cast<__int128>(LLONG_MIN))) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      res = static_cast<Int64>(wide);
      break;
    }
    case '/':
      if (r == 0) return -ZDIV_BY_ZERO_ERROR;
      if (unlikely(l == LLONG_MIN && r == -1)) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      res = l / r;
      break;
    case '%':
      if (r == 0) return -ZDIV_BY_ZERO_ERROR;
      if (unlikely(l == LLONG_MIN && r == -1)) {
        return -ZCALC_OVERFLOW_ERROR;
      }
      res = l % r;
      break;
    default: return -ZCALC_OVERFLOW_ERROR;
  }
  *resultType = Interpreter::REG_TYPE_INT;
  *resultBits = static_cast<Uint64>(res);
  return 0;
}

#endif
