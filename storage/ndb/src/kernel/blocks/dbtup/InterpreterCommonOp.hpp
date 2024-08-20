/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#ifndef INTERPRETER_COMMON_OP_H_
#define INTERPRETER_COMMON_OP_H_

#include "NdbAggregationCommon.hpp"

bool TestIfSumOverflowsUint64(Uint64 arg1, Uint64 arg2) {
  return ULLONG_MAX - arg1 < arg2;
}

void SetRegisterNull(Register* reg) {
  reg->is_null = true;
  reg->value.val_int64 = 0;
  reg->is_unsigned = false;
}

void ResetRegister(Register* reg) {
  reg->type = NDB_TYPE_UNDEFINED;
  SetRegisterNull(reg);
}

Int32 RegPlusReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (unlikely(a.is_null || b.is_null)) {
    // if either register a or b has a null value, the result
    // will also be null
    SetRegisterNull(res);
    // Set the result type to be the resolved one
    res->type = res_type;
    // If we're doing Plus on two integer values, must set is_unsigned
    // correctly
    if (res_type == NDB_TYPE_BIGINT) {
      // Set the result is_unsigned correctly even the result value is NULL
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
    }
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = b.value.val_int64;
    Int64 res_val = static_cast<Uint64>(val0) + static_cast<Uint64>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (b.is_unsigned || val1 >= 0) {
        if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
          // overflows;
          return -1;
        } else {
          res_unsigned = true;
        }
      } else {
        if ((Uint64)val0 > (Uint64)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (b.is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)val1)) {
            // overflows;
            return -1;
          } else {
            res_unsigned = true;
          }
        } else {
          if ((Uint64)val1 > (Uint64)(LLONG_MAX)) {
            res_unsigned = true;
          }
        }
      } else {
        if (val0 >= 0 && val1 >= 0) {
          res_unsigned = true;
        } else if (val0 < 0 && val1 < 0 && res_val >= 0) {
          // overflow
          return -1;
        }
      }
    }

    // Check if res_val is overflow
    bool unsigned_flag = false;
    if (res_type == NDB_TYPE_BIGINT) {
      unsigned_flag = (a.is_unsigned | b.is_unsigned);
    } else {
      assert(res_type == NDB_TYPE_DOUBLE);
      unsigned_flag = (a.is_unsigned & b.is_unsigned);
    }
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      // overload
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (b.type == NDB_TYPE_DOUBLE) ?
                     b.value.val_double :
                     ((b.is_unsigned == true) ?
                       static_cast<double>(b.value.val_uint64) :
                       static_cast<double>(b.value.val_int64));
    double res_val = val0 + val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
    res->is_unsigned = false;
  }

  res->type = res_type;

  return 0;
}

Int32 RegMinusReg(const Register& a, const Register& b,
                           Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (unlikely(a.is_null || b.is_null)) {
    // if either register a or b has a null value, the result
    // will also be null
    SetRegisterNull(res);
    // Set the result type to be the resolved one
    res->type = res_type;
    // If we're doing Minus on two integer values, must set is_unsigned
    // correctly
    if (res_type == NDB_TYPE_BIGINT) {
      // Set the result is_unsigned correctly even the result value is NULL
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
    }
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = b.value.val_int64;
    Int64 res_val = static_cast<Uint64>(val0) - static_cast<Uint64>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (b.is_unsigned) {
        if (static_cast<Uint64>(val0) < static_cast<Uint64>(val1)) {
          if (res_val >= 0) {
            // overflow
            return -1;
          } else {
            res_unsigned = true;
          }
        }
      } else {
        if (val1 >= 0) {
          if (static_cast<Uint64>(val0) > static_cast<Uint64>(val1)) {
            res_unsigned = true;
          }
        } else {
          if (TestIfSumOverflowsUint64((Uint64)val0, (Uint64)-val1)) {
            // overflow
            return -1;
          } else {
            res_unsigned = true;
          }
        }
      }
    } else {
      if (b.is_unsigned) {
        if (static_cast<Uint64>(val0) - LLONG_MIN <
            static_cast<Uint64>(val1)) {
          // overflow
          return -1;
        } else {
          if (val0 >= 0 && val1 < 0) {
            res_unsigned = true;
          } else if (val0 < 0 && val1 > 0 && res_val >= 0) {
            // overflow
            return -1;
          }
        }
      }
    }
    // Check if res_val is overflow
    bool unsigned_flag = false;
    if (res_type == NDB_TYPE_BIGINT) {
      unsigned_flag = (a.is_unsigned | b.is_unsigned);
    } else {
      assert(res_type == NDB_TYPE_DOUBLE);
      unsigned_flag = (a.is_unsigned & b.is_unsigned);
    }
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      // overflow
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (b.type == NDB_TYPE_DOUBLE) ?
                     b.value.val_double :
                     ((b.is_unsigned == true) ?
                       static_cast<double>(b.value.val_uint64) :
                       static_cast<double>(b.value.val_int64));
    double res_val = val0 - val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
  }

  res->type = res_type;
  return 0;
}

Int32 RegMulReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (unlikely(a.is_null || b.is_null)) {
    // if either register a or b has a null value, the result
    // will also be null
    SetRegisterNull(res);
    // Set the result type to be the resolved one
    res->type = res_type;
    // If we're doing MUL on two integer values, must set is_unsigned
    // correctly
    if (res_type == NDB_TYPE_BIGINT) {
      // Set the result is_unsigned correctly even the result value is NULL
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
    }
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = b.value.val_int64;
    Int64 res_val;
    Uint64 res_val0;
    Uint64 res_val1;

    if (val0 == 0 || val1 == 0) {
      res->value.val_int64 = 0;
      res->type = res_type;
      // Set the result is_unsigned correctly even the result value is 0
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
      return 0;
    }

    const bool a_negative = (!a.is_unsigned && val0 < 0);
    const bool b_negative = (!b.is_unsigned && val1 < 0);
    const bool res_unsigned = (a_negative == b_negative);

    if (a_negative && val0 == INT_MIN64) {
      if (val1 == 1) {
        // Check if val0 is overflow
        bool unsigned_flag = false;
        if (res_type == NDB_TYPE_BIGINT) {
          unsigned_flag = (a.is_unsigned | b.is_unsigned);
        } else {
          assert(res_type == NDB_TYPE_DOUBLE);
          unsigned_flag = (a.is_unsigned & b.is_unsigned);
        }
        if ((unsigned_flag && !res_unsigned && val0 < 0) ||
            (!unsigned_flag && res_unsigned &&
             (Uint64)val0 > (Uint64)LLONG_MAX)) {
          // overflow
          return -1;
        } else {
          if (unsigned_flag) {
            res->value.val_uint64 = val0;
          } else {
            res->value.val_int64 = val0;
          }
        }
        res->is_unsigned = unsigned_flag;
        res->type = res_type;
        return 0;
      }
    }
    if (b_negative && val1 == INT_MIN64) {
      if (val0 == 1) {
        // Check if val1 is overflow
        bool unsigned_flag = false;
        if (res_type == NDB_TYPE_BIGINT) {
          unsigned_flag = (a.is_unsigned | b.is_unsigned);
        } else {
          assert(res_type == NDB_TYPE_DOUBLE);
          unsigned_flag = (a.is_unsigned & b.is_unsigned);
        }
        if ((unsigned_flag && !res_unsigned && val1 < 0) ||
            (!unsigned_flag && res_unsigned &&
             (Uint64)val1 > (Uint64)LLONG_MAX)) {
          // overflow
          return -1;
        } else {
          if (unsigned_flag) {
            res->value.val_uint64 = val1;
          } else {
            res->value.val_int64 = val1;
          }
        }
        res->is_unsigned = unsigned_flag;
        res->type = res_type;
        return 0;
      }
    }

    if (a_negative) {
      val0 = -val0;
    }
    if (b_negative) {
      val1 = -val1;
    }

    Uint32 a0 = 0xFFFFFFFFUL & val0;
    Uint32 a1 = static_cast<Uint64>(val0) >> 32;
    Uint32 b0 = 0xFFFFFFFFUL & val1;
    Uint32 b1 = static_cast<Uint64>(val1) >> 32;

    if (a1 && b1) {
      // overflow
      return -1;
    }

    res_val1 = static_cast<Uint64>(a1) * b0 + static_cast<Uint64>(a0) * b1;
    if (res_val1 > 0xFFFFFFFFUL) {
      // overflow
      return -1;
    }

    res_val1 = res_val1 << 32;
    res_val0 = static_cast<Uint64>(a0) * b0;
    if (TestIfSumOverflowsUint64(res_val1, res_val0)) {
      // overflow
      return -1;
    } else {
      res_val = res_val1 + res_val0;
    }

    if (a_negative != b_negative) {
      if (static_cast<Uint64>(res_val) > static_cast<Uint64>(LLONG_MAX)) {
        // overflow
        return -1;
      } else {
        res_val = -res_val;
      }
    }

    // Check if res_val is overflow
    bool unsigned_flag = false;
    if (res_type == NDB_TYPE_BIGINT) {
      unsigned_flag = (a.is_unsigned | b.is_unsigned);
    } else {
      assert(res_type == NDB_TYPE_DOUBLE);
      unsigned_flag = (a.is_unsigned & b.is_unsigned);
    }
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
        (!unsigned_flag && res_unsigned &&
         (Uint64)res_val > (Uint64)LLONG_MAX)) {
      // overflow
      return -1;
    } else {
      if (unsigned_flag) {
        res->value.val_uint64 = res_val;
      } else {
        res->value.val_int64 = res_val;
      }
    }
    res->is_unsigned = unsigned_flag;
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (b.type == NDB_TYPE_DOUBLE) ?
                     b.value.val_double :
                     ((b.is_unsigned == true) ?
                       static_cast<double>(b.value.val_uint64) :
                       static_cast<double>(b.value.val_int64));
    double res_val = val0 * val1;
    if (std::isfinite(res_val)) {
      res->value.val_double = res_val;
    } else {
      // overflow
      return -1;
    }
  }
  res->type = res_type;
  return 0;
}

int32_t RegDivReg(const Register& tmp_a, const Register& tmp_b, Register* res,
                  bool is_div_int) {

  Register a(tmp_a);
  Register b(tmp_b);

  if (!is_div_int) {
    if (a.type == NDB_TYPE_BIGINT) {
      int64_t val0 = a.value.val_int64;
      bool val0_negtive = !a.is_unsigned && val0 < 0;
      uint64_t uval0 = static_cast<uint64_t>(val0_negtive &&
                       val0 != LLONG_MIN ? -val0 : val0);
      if (!val0_negtive && uval0 > static_cast<int64_t>(pow(2, 53) - 1)) {
        // overflow
        return -1;
      } else if (val0_negtive && val0 < -static_cast<int64_t>(pow(2, 53))) {
        // overflow
        return -1;
      }

      a.value.val_double = static_cast<double>(val0);
      a.type = NDB_TYPE_DOUBLE;
      a.is_unsigned = false;
    }
    if (b.type == NDB_TYPE_BIGINT) {
      int64_t val0 = b.value.val_int64;
      bool val0_negtive = !b.is_unsigned && val0 < 0;
      uint64_t uval0 = static_cast<uint64_t>(val0_negtive &&
                       val0 != LLONG_MIN ? -val0 : val0);
      if (!val0_negtive && uval0 > static_cast<int64_t>(pow(2, 53) - 1)) {
        // overflow
        return -1;
      } else if (val0_negtive && val0 < -static_cast<int64_t>(pow(2, 53))) {
        // overflow
        return -1;
      }

      b.value.val_double = static_cast<double>(val0);
      b.type = NDB_TYPE_DOUBLE;
      b.is_unsigned = false;
    }
  }

  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (unlikely(a.is_null || b.is_null)) {
    // if either register a or b has a null value, the result
    // will also be null
    SetRegisterNull(res);
    // Set the result type to be the resolved one
    res->type = res_type;
    if (is_div_int) {
      res->type = NDB_TYPE_BIGINT;
      // If we're doing DIV on two integer values, must set is_unsigned
      // correctly
      if (a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT) {
        // Set the result is_unsigned correctly even the result value is NULL
        res->is_unsigned = (a.is_unsigned | b.is_unsigned);
      }
    }
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    assert(is_div_int);
    Int64 val0 = a.value.val_int64;
    Int64 val1 = b.value.val_int64;
    bool val0_negative, val1_negative, res_negative, res_unsigned;
    Uint64 uval0, uval1, res_val;

    val0_negative = !a.is_unsigned && val0 < 0;
    val1_negative = !b.is_unsigned && val1 < 0;
    res_negative = val0_negative != val1_negative;
    res_unsigned = !res_negative;

    if (val1 == 0) {
      // Divide by zero
      SetRegisterNull(res);
      // Set the result is_unsigned correctly even the result value is NULL
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
    } else {
      uval0 = static_cast<Uint64>(val0_negative &&
          val0 != LLONG_MIN ? -val0 : val0);
      uval1 = static_cast<Uint64>(val1_negative &&
          val1 != LLONG_MIN ? -val1 : val1);
      res_val = uval0 / uval1;
      if (res_negative) {
        if (res_val > static_cast<Uint64>(LLONG_MAX)) {
          // overflow
          return -1;
        } else {
          res_val = static_cast<Uint64>(-static_cast<Int64>(res_val));
        }
      }
      // Check if res_val is overflow
      bool unsigned_flag = false;
      if (res_type == NDB_TYPE_BIGINT) {
        unsigned_flag = (a.is_unsigned | b.is_unsigned);
      } else {
        assert(res_type == NDB_TYPE_DOUBLE);
        unsigned_flag = (a.is_unsigned & b.is_unsigned);
      }
      if ((unsigned_flag && !res_unsigned && (Int64)res_val < 0) ||
          (!unsigned_flag && res_unsigned &&
           (Uint64)res_val > (Uint64)LLONG_MAX)) {
        // overflow
        return -1;
      } else {
        if (unsigned_flag) {
          res->value.val_uint64 = res_val;
        } else {
          res->value.val_int64 = res_val;
        }
      }
      res->is_unsigned = unsigned_flag;
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (b.type == NDB_TYPE_DOUBLE) ?
                     b.value.val_double :
                     ((b.is_unsigned == true) ?
                       static_cast<double>(b.value.val_uint64) :
                       static_cast<double>(b.value.val_int64));
    if (val1 == 0) {
      // Divided by zero
      SetRegisterNull(res);
      if (is_div_int) {
        res_type = NDB_TYPE_BIGINT;
        assert(!(a.is_unsigned | b.is_unsigned));
      }
    } else {
      double res_val = val0 / val1;
      if (std::isfinite(res_val)) {
        res->value.val_double = res_val;

        if (is_div_int) {
          res_type = NDB_TYPE_BIGINT;
          double val = res->value.val_double;
          if (val > 0) {
            res->value.val_int64 = std::floor(val);
          } else if (val < 0) {
            res->value.val_int64 = std::ceil(val);
          } else {
            res->value.val_int64 = 0;
          }
          assert(!res->is_unsigned);
        }
      } else {
        // overflow
        return -1;
      }
    }
  }
  res->type = res_type;
  if (res->is_null) {
    return 1;
  }
  return 0;
}

Int32 RegModReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (unlikely(a.is_null || b.is_null)) {
    // if either register a or b has a null value, the result
    // will also be null
    SetRegisterNull(res);
    // Set the result type to be the resolved one
    res->type = res_type;
    // If we're doing MOD on two integer values, must set is_unsigned
    // correctly
    if (res_type == NDB_TYPE_BIGINT) {
      // Set the result is_unsigned correctly even the result value is NULL
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
    }
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    Int64 val0 = a.value.val_int64;
    Int64 val1 = b.value.val_int64;
    bool val0_negative, val1_negative, res_unsigned;
    Uint64 uval0, uval1, res_val;

    val0_negative = !a.is_unsigned && val0 < 0;
    val1_negative = !b.is_unsigned && val1 < 0;
    res_unsigned = !val0_negative;

    if (val1 == 0) {
      // Divide by zero
      SetRegisterNull(res);
      // Set the result is_unsigned correctly even the result value is NULL
      res->is_unsigned = (a.is_unsigned | b.is_unsigned);
    } else {
      uval0 = static_cast<Uint64>(val0_negative &&
          val0 != LLONG_MIN ? -val0 : val0);
      uval1 = static_cast<Uint64>(val1_negative &&
          val1 != LLONG_MIN ? -val1 : val1);
      res_val = uval0 % uval1;
      res_val = res_unsigned ? res_val : -res_val;

      // Check if res_val is overflow
      bool unsigned_flag = false;
      if (res_type == NDB_TYPE_BIGINT) {
        unsigned_flag = (a.is_unsigned | b.is_unsigned);
      } else {
        assert(res_type == NDB_TYPE_DOUBLE);
        unsigned_flag = (a.is_unsigned & b.is_unsigned);
      }
      if ((unsigned_flag && !res_unsigned && (Int64)res_val < 0) ||
          (!unsigned_flag && res_unsigned &&
           (Uint64)res_val > (Uint64)LLONG_MAX)) {
        return -1;
      } else {
        if (unsigned_flag) {
          res->value.val_uint64 = res_val;
        } else {
          res->value.val_int64 = res_val;
        }
      }
      res->is_unsigned = unsigned_flag;
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (b.type == NDB_TYPE_DOUBLE) ?
                     b.value.val_double :
                     ((b.is_unsigned == true) ?
                       static_cast<double>(b.value.val_uint64) :
                       static_cast<double>(b.value.val_int64));
    if (val1 == 0) {
      // Divided by zero
      SetRegisterNull(res);
    } else {
      res->value.val_double = std::fmod(val0, val1);
    }
  }
  res->type = res_type;
  if (res->is_null) {
    return 1;
  }
  return 0;
}

#endif  // INTERPRETER_COMMON_OP_H_
