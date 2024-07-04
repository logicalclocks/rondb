/*
 * Copyright [2024] <Copyright Hopsworks AB>
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

bool TestIfSumOverflowsUint64(uint64_t arg1, uint64_t arg2) {
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

int32_t RegPlusReg(const Register& a, const Register& b, Register* res) {
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
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = b.value.val_int64;
    int64_t res_val = static_cast<uint64_t>(val0) + static_cast<uint64_t>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (b.is_unsigned || val1 >= 0) {
        if (TestIfSumOverflowsUint64((uint64_t)val0, (uint64_t)val1)) {
          // overflows;
          return -1;
        } else {
          res_unsigned = true;
        }
      } else {
        if ((uint64_t)val0 > (uint64_t)(LLONG_MAX)) {
          res_unsigned = true;
        }
      }
    } else {
      if (b.is_unsigned) {
        if (val0 >= 0) {
          if (TestIfSumOverflowsUint64((uint64_t)val0, (uint64_t)val1)) {
            // overflows;
            return -1;
          } else {
            res_unsigned = true;
          }
        } else {
          if ((uint64_t)val1 > (uint64_t)(LLONG_MAX)) {
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
         (uint64_t)res_val > (uint64_t)LLONG_MAX)) {
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

int32_t RegMinusReg(const Register& a, const Register& b,
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
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = b.value.val_int64;
    int64_t res_val = static_cast<uint64_t>(val0) - static_cast<uint64_t>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (b.is_unsigned) {
        if (static_cast<uint64_t>(val0) < static_cast<uint64_t>(val1)) {
          if (res_val >= 0) {
            // overflow
            return -1;
          } else {
            res_unsigned = true;
          }
        }
      } else {
        if (val1 >= 0) {
          if (static_cast<uint64_t>(val0) > static_cast<uint64_t>(val1)) {
            res_unsigned = true;
          }
        } else {
          if (TestIfSumOverflowsUint64((uint64_t)val0, (uint64_t)-val1)) {
            // overflow
            return -1;
          } else {
            res_unsigned = true;
          }
        }
      }
    } else {
      if (b.is_unsigned) {
        if (static_cast<uint64_t>(val0) - LLONG_MIN <
            static_cast<uint64_t>(val1)) {
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
         (uint64_t)res_val > (uint64_t)LLONG_MAX)) {
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

int32_t RegMulReg(const Register& a, const Register& b, Register* res) {
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
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = b.value.val_int64;
    int64_t res_val;
    uint64_t res_val0;
    uint64_t res_val1;

    if (val0 == 0 || val1 == 0) {
      res->type = res_type;
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
             (uint64_t)val0 > (uint64_t)LLONG_MAX)) {
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
             (uint64_t)val1 > (uint64_t)LLONG_MAX)) {
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

    uint32_t a0 = 0xFFFFFFFFUL & val0;
    uint32_t a1 = static_cast<uint64_t>(val0) >> 32;
    uint32_t b0 = 0xFFFFFFFFUL & val1;
    uint32_t b1 = static_cast<uint64_t>(val1) >> 32;

    if (a1 && b1) {
      // overflow
      return -1;
    }

    res_val1 = static_cast<uint64_t>(a1) * b0 + static_cast<uint64_t>(a0) * b1;
    if (res_val1 > 0xFFFFFFFFUL) {
      // overflow
      return -1;
    }

    res_val1 = res_val1 << 32;
    res_val0 = static_cast<uint64_t>(a0) * b0;
    if (TestIfSumOverflowsUint64(res_val1, res_val0)) {
      // overflow
      return -1;
    } else {
      res_val = res_val1 + res_val0;
    }

    if (a_negative != b_negative) {
      if (static_cast<uint64_t>(res_val) > static_cast<uint64_t>(LLONG_MAX)) {
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
         (uint64_t)res_val > (uint64_t)LLONG_MAX)) {
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

int32_t RegDivReg(const Register& a, const Register& b, Register* res) {
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
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = b.value.val_int64;
    bool val0_negative, val1_negative, res_negative, res_unsigned;
    uint64_t uval0, uval1, res_val;

    val0_negative = !a.is_unsigned && val0 < 0;
    val1_negative = !b.is_unsigned && val1 < 0;
    res_negative = val0_negative != val1_negative;
    res_unsigned = !res_negative;

    if (val1 == 0) {
      // Divide by zero
      SetRegisterNull(res);
      res->is_unsigned = res_unsigned;
    } else {
      uval0 = static_cast<uint64_t>(val0_negative &&
          val0 != LLONG_MIN ? -val0 : val0);
      uval1 = static_cast<uint64_t>(val1_negative &&
          val1 != LLONG_MIN ? -val1 : val1);
      res_val = uval0 / uval1;
      if (res_negative) {
        if (res_val > static_cast<uint64_t>(LLONG_MAX)) {
          // overflow
          return -1;
        } else {
          res_val = static_cast<uint64_t>(-static_cast<int64_t>(res_val));
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
      if ((unsigned_flag && !res_unsigned && (int64_t)res_val < 0) ||
          (!unsigned_flag && res_unsigned &&
           (uint64_t)res_val > (uint64_t)LLONG_MAX)) {
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
    } else {
      double res_val = val0 / val1;
      if (std::isfinite(res_val)) {
        res->value.val_double = res_val;
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

int32_t RegModReg(const Register& a, const Register& b, Register* res) {
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
    // NULL
    return 1;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = b.value.val_int64;
    bool val0_negative, val1_negative, res_unsigned;
    uint64_t uval0, uval1, res_val;

    val0_negative = !a.is_unsigned && val0 < 0;
    val1_negative = !b.is_unsigned && val1 < 0;
    res_unsigned = !val0_negative;

    if (val1 == 0) {
      // Divide by zero
      SetRegisterNull(res);
      res->is_unsigned = res_unsigned;
    } else {
      uval0 = static_cast<uint64_t>(val0_negative &&
          val0 != LLONG_MIN ? -val0 : val0);
      uval1 = static_cast<uint64_t>(val1_negative &&
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
      if ((unsigned_flag && !res_unsigned && (int64_t)res_val < 0) ||
          (!unsigned_flag && res_unsigned &&
           (uint64_t)res_val > (uint64_t)LLONG_MAX)) {
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
