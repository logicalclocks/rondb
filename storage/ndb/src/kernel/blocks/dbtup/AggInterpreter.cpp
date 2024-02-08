/*
 * Copyright [2024] <Copyright Hopsworks AB>
 *
 * Author: Zhao Song
 */
#include <cstdint>
#include <cstring>
#include <utility>

#include "signaldata/TransIdAI.hpp"
#include "include/my_byteorder.h"
#include "AggInterpreter.hpp"

uint32_t AggInterpreter::g_buf_len_ = READ_BUF_WORD_SIZE;
uint32_t AggInterpreter::g_result_header_size_ = 3 * sizeof(uint32_t);
uint32_t AggInterpreter::g_result_header_size_per_group_ = sizeof(uint32_t);

bool AggInterpreter::Init() {
  if (inited_) {
    return true;
  }

  uint32_t value = 0;

  /*
   * 1. Double check the magic num and  total length of program.
   */
  value = prog_[cur_pos_++];
  assert(((value & 0xFFFF0000) >> 16) == 0x0721);
  assert((value & 0xFFFF) == prog_len_);

  /*
   * 2. Get num of columns for group by and num of aggregation results;
   */
  value = prog_[cur_pos_++];
  n_gb_cols_ = (value >> 16) & 0xFFFF;
  n_agg_results_ = value & 0xFFFF;

  /*
   * 3. Get all the group by columns id.
   */
  if (n_gb_cols_) {
#ifdef MOZ_AGG_MALLOC
    assert(n_gb_cols_ <= MAX_AGG_N_GROUPBY_COLS);
    gb_cols_ = gb_cols_buf_;
#else
    gb_cols_ = new uint32_t[n_gb_cols_];
#endif // MOZ_AGG_MALLOC

    uint32_t i = 0;
    while (i < n_gb_cols_ && cur_pos_ < prog_len_) {
      gb_cols_[i++] = prog_[cur_pos_++];
    }
#ifdef MOZ_AGG_MALLOC
    gb_map_ = &gb_map_buf_;
#else
    gb_map_ = new std::map<GBHashEntry, GBHashEntry, GBHashEntryCmp>;
#endif // MOZ_AGG_MALLOC
  }

  /*
   * 4. Reset all aggregation results
   */
  if (n_agg_results_) {
#ifdef MOZ_AGG_MALLOC
    assert(n_agg_results_ <= MAX_AGG_N_RESULTS);
    agg_results_ = agg_results_buf_;
#else
    agg_results_ = new AggResItem[n_agg_results_];
#endif // MOZ_AGG_MALLOC
    uint32_t i = 0;
    while (i < n_agg_results_) {
      agg_results_[i].type = NDB_TYPE_UNDEFINED;
      agg_results_[i].value.val_int64 = 0;
      agg_results_[i].is_unsigned = false;
      agg_results_[i].is_null = true;
      i++;
    }
  }

  inited_ = true;
  agg_prog_start_pos_ = cur_pos_;
  memset(registers_, 0, sizeof(registers_));

  return true;
}

static bool TypeSupported(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return true;
    default:
      return false;
  }
  return false;
}

static bool IsUnsigned(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:
      return true;
    default:
      return false;
  }
  return false;
}

static DataType AlignedType(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYINT:
    case NDB_TYPE_SMALLINT:
    case NDB_TYPE_MEDIUMINT:
    case NDB_TYPE_INT:
    case NDB_TYPE_BIGINT:

    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:

      return NDB_TYPE_BIGINT;
    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return NDB_TYPE_DOUBLE;
    default:
      assert(0);
  }
  return NDB_TYPE_UNDEFINED;
}

static bool TestIfSumOverflowsUint64(uint64_t arg1, uint64_t arg2) {
  return ULLONG_MAX - arg1 < arg2;
}

static void SetRegisterNull(Register* reg) {
  reg->is_null = true;
  reg->value.val_int64 = 0;
  reg->is_unsigned = false;
}

static void ResetRegister(Register* reg) {
  reg->type = NDB_TYPE_UNDEFINED;
  SetRegisterNull(reg);
}

static int32_t RegPlusReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (a.is_null || b.is_null) {
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

static int32_t RegMinusReg(const Register& a, const Register& b,
                           Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (a.is_null || b.is_null) {
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

static int32_t RegMulReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (a.is_null || b.is_null) {
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

static int32_t RegDivReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (a.is_null || b.is_null) {
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

static int32_t RegModReg(const Register& a, const Register& b, Register* res) {
  assert(a.type != NDB_TYPE_UNDEFINED && b.type != NDB_TYPE_UNDEFINED);

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || b.type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT && b.type == NDB_TYPE_BIGINT);
    res_type = NDB_TYPE_BIGINT;
  }

  if (a.is_null || b.is_null) {
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

static void PrintValue(const AggResItem* res) {
  if (res->type == NDB_TYPE_BIGINT) {
    if (res->is_unsigned) {
      fprintf(stderr, "[%lu, %d, %d, %d]\n",
          res->value.val_uint64, res->type, res->is_unsigned, res->is_null);
    } else {
      fprintf(stderr, "[%ld, %d, %d, %d]\n",
          res->value.val_int64, res->type, res->is_unsigned, res->is_null);
    }
  } else {
    assert(res->type == NDB_TYPE_DOUBLE);
    fprintf(stderr, "[%lf, %d, %d, %d]\n",
        res->value.val_double, res->type, res->is_unsigned, res->is_null);
  }
}

static int32_t Sum(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
    if (print) {
      fprintf(stderr, "Moz, Sum() init AggRes to ");
      PrintValue(res);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    int64_t val0 = a.value.val_int64;
    int64_t val1 = res->value.val_int64;
    int64_t res_val = static_cast<uint64_t>(val0) + static_cast<uint64_t>(val1);
    bool res_unsigned = false;

    if (a.is_unsigned) {
      if (res->is_unsigned || val1 >= 0) {
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
      if (res->is_unsigned) {
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
      unsigned_flag = (a.is_unsigned | res->is_unsigned);
    } else {
      assert(res_type == NDB_TYPE_DOUBLE);
      unsigned_flag = (a.is_unsigned & res->is_unsigned);
    }
    if ((unsigned_flag && !res_unsigned && res_val < 0) ||
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
  } else {
    double val0 = (a.type == NDB_TYPE_DOUBLE) ?
                     a.value.val_double :
                     ((a.is_unsigned == true) ?
                       static_cast<double>(a.value.val_uint64) :
                       static_cast<double>(a.value.val_int64));
    double val1 = (res->type == NDB_TYPE_DOUBLE) ?
                     res->value.val_double :
                     ((res->is_unsigned == true) ?
                       static_cast<double>(res->value.val_uint64) :
                       static_cast<double>(res->value.val_int64));
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
  res->is_null = false;

  if (print) {
    fprintf(stderr, "Moz, Sum(), update AggRes to ");
    PrintValue(res);
  }
  return 0;
}

static int32_t Max(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
    if (print) {
      fprintf(stderr, "Moz, Max(), init AggRes to ");
      PrintValue(res);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 > res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 > res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
        res->value.val_uint64 = a.value.val_uint64;
      } else {
        res->value.val_uint64 = a.value.val_uint64 >
                static_cast<uint64_t>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<uint64_t>(res->value.val_int64);
      }
      res->is_unsigned = true;
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = static_cast<uint64_t>(a.value.val_int64) >
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double > res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

  if (print) {
    fprintf(stderr, "Moz, Max(), update AggRes to ");
    PrintValue(res);
  }

  return 0;
}

static int32_t Min(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    *res = a;
    if (print) {
      fprintf(stderr, "Moz, Min(), init AggRes to ");
      PrintValue(res);
    }
    assert(res->type != NDB_TYPE_UNDEFINED);
    return 1;
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  if (res->is_null) {
    assert(res->value.val_int64 == 0);
  }

  DataType res_type = NDB_TYPE_UNDEFINED;
  if (a.type == NDB_TYPE_DOUBLE || res->type == NDB_TYPE_DOUBLE) {
    res_type = NDB_TYPE_DOUBLE;
  } else {
    assert(a.type == NDB_TYPE_BIGINT &&
          (res->type == NDB_TYPE_BIGINT || res->type == NDB_TYPE_UNDEFINED));
    res_type = NDB_TYPE_BIGINT;
  }

  if (res_type == NDB_TYPE_BIGINT) {
    if (!a.is_unsigned && !res->is_unsigned) {
      res->value.val_int64 = (a.value.val_int64 < res->value.val_int64) ?
                              a.value.val_int64 : res->value.val_int64;
    } else if (a.is_unsigned && res->is_unsigned) {
      res->value.val_uint64 = (a.value.val_uint64 < res->value.val_uint64) ?
                              a.value.val_uint64 : res->value.val_uint64;
    } else if (a.is_unsigned && !res->is_unsigned) {
      if (res->value.val_int64 < 0) {
      } else {
        res->value.val_uint64 = a.value.val_uint64 <
                static_cast<uint64_t>(res->value.val_int64) ?
                a.value.val_uint64 :
                static_cast<uint64_t>(res->value.val_int64);
        res->is_unsigned = true;
      }
    } else {
      assert(!a.is_unsigned && res->is_unsigned);
      if (a.value.val_int64 < 0) {
        res->value.val_int64 = a.value.val_int64;
        res->is_unsigned = false;
      } else {
        res->value.val_uint64 = static_cast<uint64_t>(a.value.val_int64) <
                                res->value.val_uint64 ?
                                static_cast<uint64_t>(a.value.val_int64) :
                                res->value.val_uint64;
      }
    }
  } else {
    assert(res_type == NDB_TYPE_DOUBLE);
    res->value.val_double = (a.value.val_double < res->value.val_double) ?
                             a.value.val_double : res->value.val_double;
  }
  res->is_null = false;

  if (print) {
    fprintf(stderr, "Moz, Min(), update AggRes to ");
    PrintValue(res);
  }

  return 0;
}

static int32_t Count(const Register& a, AggResItem* res, bool print) {
  assert(a.type != NDB_TYPE_UNDEFINED);
  if (res->type == NDB_TYPE_UNDEFINED) {
    // Agg result first initialized
    res->type = NDB_TYPE_BIGINT;
    res->value.val_uint64 = 0;
    res->is_unsigned = true;
    res->is_null = false;
    if (print) {
      fprintf(stderr, "Moz, Count(), init AggRes to ");
      PrintValue(res);
    }
  }

  if (a.is_null) {
    // Register has a null value
    return 1;
  }

  assert(res->type == NDB_TYPE_BIGINT &&
      res->is_null == false && res->is_unsigned == true);
  res->value.val_uint64 += 1;

  if (print) {
    fprintf(stderr, "Moz, Count(), update AggRes to ");
    PrintValue(res);
  }

  return 0;
}

bool AggInterpreter::ProcessRec(Dbtup* block_tup,
        Dbtup::KeyReqStruct* req_struct) {
  assert(inited_);
  assert(req_struct->read_length == 0);

  AggResItem* agg_res_ptr = nullptr;
  if (n_gb_cols_) {
    char* agg_rec = nullptr;

    AttributeHeader* header = nullptr;
    buf_pos_ = 0;
    for (uint32_t i = 0; i < n_gb_cols_; i++) {
      int ret = block_tup->readAttributes(req_struct, &(gb_cols_[i]), 1,
                    buf_ + buf_pos_, g_buf_len_ - buf_pos_);
#ifdef NDEBUG
      (void)ret;
#endif // NDEBUG
      assert(ret >= 0);
      header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
      buf_pos_ += (1 + header->getDataSize());
    }

    uint32_t len_in_char = buf_pos_ * sizeof(uint32_t);
    GBHashEntry entry{reinterpret_cast<char*>(buf_), len_in_char};
    auto iter = gb_map_->find(entry);
    if (iter != gb_map_->end()) {
      header = reinterpret_cast<AttributeHeader*>(iter->first.ptr);
      agg_res_ptr = reinterpret_cast<AggResItem*>(iter->second.ptr);
      if (print_) {
        fprintf(stderr, "Moz, Found GBHashEntry, id: %u, byte_size: %u, "
            "data_size: %u, is_null: %u\n",
            header->getAttributeId(), header->getByteSize(),
            header->getDataSize(), header->isNULL());
      }
    } else {
      /*
       * update req_struct->read_length here, which will update the
       * Dblqh::ScanRecord::m_curr_batch_size_bytes later in the
       * Dblqh::scanTupkeyConfLab, even we don't use that variable
       * to decide whether reaches batch limitation. Only increase
       * Dblqh::ScanRecord::m_curr_batch_size_bytes when new group
       * item is inserted into gb_map_.
       * For aggregation,
       * we use Dblqh::ScanRecord::m_agg_curr_batch_size_bytes to
       * indicate batch limitation
       */
      req_struct->read_length = (len_in_char +
                       n_agg_results_ * sizeof(AggResItem)) / sizeof(int32_t);

      // we use result_size_ to decide whether need to send some aggregation
      // results to API.
      result_size_ += len_in_char +
                       n_agg_results_ * sizeof(AggResItem);
#ifdef MOZ_AGG_MALLOC
      agg_rec = MemAlloc(len_in_char +
                          n_agg_results_ * sizeof(AggResItem));
#else
      agg_rec = new char[len_in_char +
                        n_agg_results_ * sizeof(AggResItem)];
#endif // MOZ_AGG_MALLOC
      memset(agg_rec, 0, len_in_char +
                        n_agg_results_ * sizeof(AggResItem));
      memcpy(agg_rec, reinterpret_cast<char*>(buf_), len_in_char);
      GBHashEntry new_entry{agg_rec, len_in_char};

      gb_map_->insert(std::make_pair<GBHashEntry, GBHashEntry>(
                      std::move(new_entry), std::move(
            GBHashEntry{agg_rec + len_in_char,
            static_cast<uint32_t>(n_agg_results_ * sizeof(AggResItem))})));
      n_groups_ = gb_map_->size();
      agg_res_ptr = reinterpret_cast<AggResItem*>(agg_rec + len_in_char);

      for (uint32_t i = 0; i < n_agg_results_; i++) {
        agg_res_ptr[i].type = agg_results_[i].type;
      }
    }
  } else {
    agg_res_ptr = agg_results_;
  }

  uint32_t col_index;

  uint32_t value;
  DataType type;
  bool is_unsigned;
  uint32_t reg_index;

  uint32_t reg_index2;

  uint32_t agg_index;

  const uint32_t* attrDescriptor = nullptr;

  uint32_t exec_pos = agg_prog_start_pos_;
  while (exec_pos < prog_len_) {
    value = prog_[exec_pos++];
    uint8_t op = (value & 0xFC000000) >> 26;
    int ret = 0;
    buf_pos_ = 0;
    AttributeHeader* header = nullptr;

    switch (op) {
      case kOpPlus:
        reg_index = (value & 0x0000F000) >> 12;
        reg_index2 = (value & 0x00000F00) >> 8;

        ret = RegPlusReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          printf("Overflow[PLUS], value is out of range\n");
        }
        assert(ret >= 0);
        break;
      case kOpMinus:
        reg_index = (value & 0x0000F000) >> 12;
        reg_index2 = (value & 0x00000F00) >> 8;

        ret = RegMinusReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          printf("Overflow[MINUS], value is out of range\n");
        }
        assert(ret >= 0);
        break;
      case kOpMul:
        reg_index = (value & 0x0000F000) >> 12;
        reg_index2 = (value & 0x00000F00) >> 8;

        ret = RegMulReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          printf("Overflow[MUL], value is out of range\n");
        }
        assert(ret >= 0);
        break;
      case kOpDiv:
        reg_index = (value & 0x0000F000) >> 12;
        reg_index2 = (value & 0x00000F00) >> 8;

        ret = RegDivReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          printf("Overflow[DIV], value is out of range\n");
        }
        assert(ret >= 0);
        break;
      case kOpMod:
        reg_index = (value & 0x0000F000) >> 12;
        reg_index2 = (value & 0x00000F00) >> 8;

        ret = RegModReg(registers_[reg_index], registers_[reg_index2],
                  &registers_[reg_index]);
        if (ret < 0) {
          printf("Overflow[MOD], value is out of range\n");
        }
        assert(ret >= 0);
        break;
      case kOpLoadCol:
        type = (value & 0x03E00000) >> 21;
        is_unsigned = IsUnsigned(type);
        reg_index = (value & 0x000F0000) >> 16;
        col_index = (value & 0x0000FFFF) << 16;

        ret = block_tup->readAttributes(req_struct, &(col_index), 1,
                  buf_ + buf_pos_, g_buf_len_ - buf_pos_);
        assert(ret >= 0);
        header = reinterpret_cast<AttributeHeader*>(buf_ + buf_pos_);
        attrDescriptor = req_struct->tablePtrP->tabDescriptor +
          (((col_index) >> 16) * ZAD_SIZE);
        assert(header->getAttributeId() == (col_index >> 16));

        assert(type == AttributeDescriptor::getType(attrDescriptor[0]));
        if (!TypeSupported(type)) {
          // TODO (Zhao)
          // Catch error
        }
        assert(TypeSupported(type));

        ResetRegister(&registers_[reg_index]);
        registers_[reg_index].type = AlignedType(type);
        registers_[reg_index].is_unsigned = is_unsigned;
        registers_[reg_index].is_null = header->isNULL();
        if (registers_[reg_index].is_null) {
          // Column has a null value
          // fprintf(stderr, "Moz-Intp: Load NULL, type: %u\n",
          //     registers_[reg_index].type);
          registers_[reg_index].value.val_int64 = 0;
          break;
        }
        switch (type) {
          case NDB_TYPE_TINYINT:
            registers_[reg_index].value.val_int64 =
                *reinterpret_cast<int8_t*>(&buf_[buf_pos_ + 1]);
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_TINYINT %ld\n",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_SMALLINT:
            registers_[reg_index].value.val_int64 =
                sint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_SMALLINT %ld\n",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_MEDIUMINT:
            registers_[reg_index].value.val_int64 =
                sint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_MEDIUM %ld\n",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_INT:
            registers_[reg_index].value.val_int64 =
                sint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_INT %ld\n",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_BIGINT %ld\n",
            //     registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_TINYUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                *reinterpret_cast<uint8_t*>(&buf_[buf_pos_ + 1]);
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_TINYUNSIGNED %lu\n",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_SMALLUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint2korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_SMALLUNSIGNED %lu\n",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_MEDIUMUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint3korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_MEDIUMUNSIGNED %lu\n",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_UNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint4korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_UNSIGNED %lu\n",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_BIGUNSIGNED %lu\n",
            //     registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_FLOAT:
            registers_[reg_index].value.val_double =
                floatget(reinterpret_cast<unsigned char*>(&buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_FLOAT %lf\n",
            //     registers_[reg_index].value.val_double);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &buf_[buf_pos_ + 1]));
            // fprintf(stderr, "Moz-Intp: Load NDB_TYPE_DOUBLE %lf\n",
            //     registers_[reg_index].value.val_double);
            break;

          default:
            assert(0);
        }
        break;
      case kOpLoadConst:
        type = (value & 0x03E00000) >> 21;
        reg_index = (value & 0x000F0000) >> 16;
        assert(type == NDB_TYPE_BIGINT || type == NDB_TYPE_BIGUNSIGNED ||
               type == NDB_TYPE_DOUBLE);
        ResetRegister(&registers_[reg_index]);
        registers_[reg_index].type = AlignedType(type);
        registers_[reg_index].is_unsigned = IsUnsigned(type);
        registers_[reg_index].is_null = false;
        switch (type) {
          case NDB_TYPE_BIGINT:
            registers_[reg_index].value.val_int64 =
                sint8korr(reinterpret_cast<char*>(&prog_[exec_pos]));
            // fprintf(stderr, "Moz-Intp: LoadConst[%u] NDB_TYPE_BIGINT %ld\n",
            //     reg_index, registers_[reg_index].value.val_int64);
            break;
          case NDB_TYPE_BIGUNSIGNED:
            registers_[reg_index].value.val_uint64 =
                uint8korr(reinterpret_cast<char*>(&prog_[exec_pos]));
            // fprintf(stderr, "Moz-Intp: LoadConst[%u] "
            //                 "NDB_TYPE_BIGUNSIGNED %lu\n",
            //     reg_index, registers_[reg_index].value.val_uint64);
            break;
          case NDB_TYPE_DOUBLE:
            registers_[reg_index].value.val_double =
                doubleget(reinterpret_cast<unsigned char*>(
                      &prog_[exec_pos]));
            // fprintf(stderr, "Moz-Intp: LoadConst[%u] NDB_TYPE_DOUBLE %lf\n",
            //     reg_index, registers_[reg_index].value.val_double);
            break;
          default:
            assert(0);
        }
        exec_pos += 2;
        break;
      case kOpSum:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Sum(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        if (ret < 0) {
          fprintf(stderr, "Overflow[SUM], value is out of range\n");
        }
        assert(ret >= 0);
        break;
      case kOpMax:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Max(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        assert(ret >= 0);
        break;
      case kOpMin:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Min(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        assert(ret >= 0);
        break;
      case kOpCount:
        reg_index = (value & 0x000F0000) >> 16;
        agg_index = (value & 0x0000FFFF);

        ret = Count(registers_[reg_index], &agg_res_ptr[agg_index], print_);
        assert(ret >= 0);
        break;

      default:
        assert(0);
    }
  }
  processed_rows_++;
  return true;
}

void AggInterpreter::Print() {
  // if (!print_) {
  //   return;
  // }
  if (n_gb_cols_) {
    if (gb_map_) {
      fprintf(stderr, "Group by columns: [");
      for (uint32_t i = 0; i < n_gb_cols_; i++) {
        if (i != n_gb_cols_ - 1) {
          fprintf(stderr, "%u ", gb_cols_[i] >> 16);
        } else {
          fprintf(stderr, "%u", gb_cols_[i] >> 16);
        }
      }
      fprintf(stderr, "]\n");
      fprintf(stderr, "Num of groups: %lu\n", gb_map_->size());
      fprintf(stderr, "Aggregation results:\n");

      for (auto iter = gb_map_->begin(); iter != gb_map_->end(); iter++) {
        int pos = 0;
        fprintf(stderr, "(");
        for (uint32_t i = 0; i < n_gb_cols_; i++) {
          if (i != n_gb_cols_ - 1) {
            fprintf(stderr, "%u: %p, ", i, iter->first.ptr + pos);
          } else {
            fprintf(stderr, "%u: %p): ", i, iter->first.ptr + pos);
          }
        }

        AggResItem* item = reinterpret_cast<AggResItem*>(iter->second.ptr);
        for (uint32_t i = 0; i < n_agg_results_; i++) {
          fprintf(stderr, "(%u, %u, %u)", item[i].type,
                  item[i].is_unsigned, item[i].is_null);
          if (item[i].is_null) {
            fprintf(stderr, "[NULL]");
          } else {
            switch (item[i].type) {
              case NDB_TYPE_BIGINT:
                fprintf(stderr, "[%15ld]", item[i].value.val_int64);
                break;
              case NDB_TYPE_DOUBLE:
                fprintf(stderr, "[%31.16f]", item[i].value.val_double);
                break;
              default:
                assert(0);
            }
          }
        }
        fprintf(stderr, "\n");
      }
    }
  } else {
    AggResItem* item = agg_results_;
    for (uint32_t i = 0; i < n_agg_results_; i++) {
      fprintf(stderr, "(%u, %u, %u)", item[i].type,
              item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        fprintf(stderr, "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            fprintf(stderr, "[%15ld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            fprintf(stderr, "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    fprintf(stderr, "\n");
  }
}

// NOTICE: Need to define agg_ops[] before using this func.
void AggInterpreter::MergePrint(const AggInterpreter* in1,
                                   const AggInterpreter* in2) {
  assert(in1 != nullptr && in2 != nullptr);
  assert(in1->n_agg_results_ == in2->n_agg_results_);
  auto iter1 = in1->gb_map_->begin();
  auto iter2 = in2->gb_map_->begin();

  while (iter1 != in1->gb_map_->end() && iter2 != in2->gb_map_->end()) {
    uint32_t len1 = iter1->first.len;
    uint32_t len2 = iter2->first.len;
#ifdef NDEBUG
    (void)len2;
#endif // NDEBUG
    assert(len1 == len2);

    int ret = memcmp(iter1->first.ptr, iter2->first.ptr, len1);
    if (ret < 0) {
      int pos = 0;
      fprintf(stderr, "(");
      for (uint32_t i = 0; i < in1->n_gb_cols_; i++) {
        if (i != in1->n_gb_cols_ - 1) {
          fprintf(stderr, "%u: %p, ", i, iter1->first.ptr + pos);
        } else {
          fprintf(stderr, "%u: %p): ", i, iter1->first.ptr + pos);
        }
      }
      AggResItem* item = reinterpret_cast<AggResItem*>(iter1->second.ptr);
      for (uint32_t i = 0; i < in1->n_agg_results_; i++) {
        fprintf(stderr, "(%u, %u, %u)", item[i].type,
            item[i].is_unsigned, item[i].is_null);
        if (item[i].is_null) {
          fprintf(stderr, "[NULL]");
        } else {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              fprintf(stderr, "[%15ld]", item[i].value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              fprintf(stderr, "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      fprintf(stderr, "\n");
      iter1++;
    } else if (ret > 0) {
      int pos = 0;
      fprintf(stderr, "(");
      for (uint32_t i = 0; i < in2->n_gb_cols_; i++) {
        if (i != in2->n_gb_cols_ - 1) {
          fprintf(stderr, "%u: %p, ", i, iter2->first.ptr + pos);
        } else {
          fprintf(stderr, "%u: %p): ", i, iter2->first.ptr + pos);
        }
      }
      AggResItem* item = reinterpret_cast<AggResItem*>(iter2->second.ptr);
      for (uint32_t i = 0; i < in2->n_agg_results_; i++) {
        fprintf(stderr, "(%u, %u, %u)", item[i].type,
            item[i].is_unsigned, item[i].is_null);
        if (item[i].is_null) {
          fprintf(stderr, "[NULL]");
        } else {
          switch (item[i].type) {
            case NDB_TYPE_BIGINT:
              fprintf(stderr, "[%15ld]", item[i].value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              fprintf(stderr, "[%31.16f]", item[i].value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      fprintf(stderr, "\n");
      iter2++;
    } else {
      int pos = 0;
      fprintf(stderr, "(");
      for (uint32_t i = 0; i < in1->n_gb_cols_; i++) {
        if (i != in1->n_gb_cols_ - 1) {
          fprintf(stderr, "%u: %p, ", i, iter1->first.ptr + pos);
        } else {
          fprintf(stderr, "%u: %p): ", i, iter1->first.ptr + pos);
        }
      }
      AggResItem* item1 = reinterpret_cast<AggResItem*>(iter1->second.ptr);
      AggResItem* item2 = reinterpret_cast<AggResItem*>(iter2->second.ptr);
      AggResItem result;
      // NOTICE: Need to define agg_ops[] first.
      Uint32 agg_ops[32];
      for (uint32_t i = 0; i < in1->n_agg_results_; i++) {
        assert(((item1[i].type == NDB_TYPE_BIGINT &&
                item1[i].is_unsigned == item2[i].is_unsigned) ||
                item1[i].type == NDB_TYPE_DOUBLE) &&
                item1[i].type == item2[i].type);
        if (item1[i].is_null) {
          result = item2[i];
        } else if (item2[i].is_null) {
          result = item1[i];
        } else {
          result.type = item1[i].type;
          result.is_unsigned = item1[i].is_unsigned;
          switch (agg_ops[i]) {
            case kOpSum:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 = (item1[i].value.val_uint64 +
                                                 item2[i].value.val_uint64);
                } else {
                  result.value.val_int64 = (item1[i].value.val_int64 +
                                                 item2[i].value.val_int64);
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double = (item1[i].value.val_double +
                                               item2[i].value.val_double);
              }
              break;
            case kOpCount:
              assert(item1[i].type == NDB_TYPE_BIGINT);
              assert(item1[i].is_unsigned == 1);
              result.value.val_int64 = (item1[i].value.val_int64 +
                                             item2[i].value.val_int64);
              break;
            case kOpMax:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 =
                    item1[i].value.val_uint64 >= item2[i].value.val_uint64 ?
                    item1[i].value.val_uint64 : item2[i].value.val_uint64;
                } else {
                  result.value.val_int64 =
                    item1[i].value.val_int64 >= item2[i].value.val_int64 ?
                    item1[i].value.val_int64 : item2[i].value.val_int64;
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double =
                  item1[i].value.val_double >= item2[i].value.val_double ?
                  item1[i].value.val_double : item2[i].value.val_double;
              }
              break;
            case kOpMin:
              if (item1[i].type == NDB_TYPE_BIGINT) {
                if (item1[i].is_unsigned) {
                  result.value.val_uint64 =
                    item1[i].value.val_uint64 <= item2[i].value.val_uint64 ?
                    item1[i].value.val_uint64 : item2[i].value.val_uint64;
                } else {
                  result.value.val_int64 =
                    item1[i].value.val_int64 <= item2[i].value.val_int64 ?
                    item1[i].value.val_int64 : item2[i].value.val_int64;
                }
              } else {
                assert(item1[i].type == NDB_TYPE_DOUBLE);
                result.value.val_double =
                  item1[i].value.val_double <= item2[i].value.val_double ?
                  item1[i].value.val_double : item2[i].value.val_double;
              }
              break;
            default:
              assert(0);
              break;
          }
        }
        fprintf(stderr, "(%u, %u, %u)", result.type,
            result.is_unsigned, result.is_null);
        if (result.is_null) {
          fprintf(stderr, "[NULL]");
        } else {
          switch (result.type) {
            case NDB_TYPE_BIGINT:
              fprintf(stderr, "[%15ld]", result.value.val_int64);
              break;
            case NDB_TYPE_DOUBLE:
              fprintf(stderr, "[%31.16f]", result.value.val_double);
              break;
            default:
              assert(0);
          }
        }
      }
      fprintf(stderr, "\n");
      iter1++;
      iter2++;
    }
  }
  while (iter1 != in1->gb_map_->end()) {
    int pos = 0;
    fprintf(stderr, "(");
    for (uint32_t i = 0; i < in1->n_gb_cols_; i++) {
      if (i != in1->n_gb_cols_ - 1) {
        fprintf(stderr, "%u: %p, ", i, iter1->first.ptr + pos);
      } else {
        fprintf(stderr, "%u: %p): ", i, iter1->first.ptr + pos);
      }
    }
    AggResItem* item = reinterpret_cast<AggResItem*>(iter1->second.ptr);
    for (uint32_t i = 0; i < in1->n_agg_results_; i++) {
      fprintf(stderr, "(%u, %u, %u)", item[i].type,
          item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        fprintf(stderr, "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            fprintf(stderr, "[%15ld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            fprintf(stderr, "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    fprintf(stderr, "\n");
    iter1++;
  }
  while (iter2 != in2->gb_map_->end()) {
    int pos = 0;
    fprintf(stderr, "(");
    for (uint32_t i = 0; i < in2->n_gb_cols_; i++) {
      if (i != in2->n_gb_cols_ - 1) {
        fprintf(stderr, "%u: %p, ", i, iter2->first.ptr + pos);
      } else {
        fprintf(stderr, "%u: %p): ", i, iter2->first.ptr + pos);
      }
    }
    AggResItem* item = reinterpret_cast<AggResItem*>(iter2->second.ptr);
    for (uint32_t i = 0; i < in2->n_agg_results_; i++) {
      fprintf(stderr, "(%u, %u, %u)", item[i].type,
          item[i].is_unsigned, item[i].is_null);
      if (item[i].is_null) {
        fprintf(stderr, "[NULL]");
      } else {
        switch (item[i].type) {
          case NDB_TYPE_BIGINT:
            fprintf(stderr, "[%15ld]", item[i].value.val_int64);
            break;
          case NDB_TYPE_DOUBLE:
            fprintf(stderr, "[%31.16f]", item[i].value.val_double);
            break;
          default:
            assert(0);
        }
      }
    }
    fprintf(stderr, "\n");
    iter2++;
  }
}


uint32_t AggInterpreter::PrepareAggResIfNeeded(Signal* signal, bool force) {
  // Limitation
  uint32_t total_size = result_size_ +
                  (gb_map_ ?
                   gb_map_->size() * g_result_header_size_per_group_ : 0) +
                  g_result_header_size_;
  if (!force && (gb_map_ == nullptr ||
        total_size < DEF_AGG_RESULT_BATCH_BYTES)) {
    return 0;
  }
  if (force &&
      (n_gb_cols_ != 0 && (gb_map_ == nullptr || gb_map_->size() == 0))) {
    assert(result_size_ == 0);
    return 0;
  }
  Uint32* data_buf = (&signal->theData[25]);
  uint32_t pos = 0;
  assert(n_gb_cols_ < 0xFFFF);
  assert(n_agg_results_ < 0xFFFF);

  if (n_gb_cols_) {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = n_gb_cols_ << 16 | n_agg_results_;
    data_buf[pos++] = gb_map_->size();
    for (auto iter = gb_map_->begin(); iter != gb_map_->end();) {
      assert(iter->first.len % 4 == 0 && iter->first.len < 0xFFFF);
      assert(iter->second.len % 4 == 0 && iter->second.len < 0xFFFF);
      data_buf[pos++] = iter->first.len << 16 | iter->second.len;
      assert(iter->first.ptr + (iter->first.len + iter->second.len) ==
          iter->second.ptr + iter->second.len);
      MEMCOPY_NO_WORDS(&data_buf[pos], iter->first.ptr,
          (iter->first.len + iter->second.len) >> 2);
      pos += ((iter->first.len + iter->second.len) >> 2);
#ifndef MOZ_AGG_MALLOC
      delete[] iter->first.ptr;
#endif // !MOZ_AGG_MALLOC
      gb_map_->erase(iter++);
      result_size_ = 0;
    }
#ifdef MOZ_AGG_MALLOC
    alloc_len_ = 0;
#endif // MOZ_AGG_MALLOC
    assert(gb_map_->empty());
  } else {
    data_buf[pos++] = AttributeHeader::AGG_RESULT << 16 | 0x0721;
    data_buf[pos++] = n_gb_cols_ << 16 | n_agg_results_;
    data_buf[pos++] = 0;
    data_buf[pos++] = 0 << 16 | (n_agg_results_ * sizeof(AggResItem));
    assert(gb_map_ == nullptr);
    MEMCOPY_NO_WORDS(&data_buf[pos], agg_results_,
        (n_agg_results_ * sizeof(AggResItem)) >> 2);
    pos += ((n_agg_results_ * sizeof(AggResItem)) >> 2);
  }

#if defined(MOZ_AGG_CHECK) && !defined(NDEBUG)
  /*
   * Moz
   * Validation
   */
  uint32_t data_len = pos;
  uint32_t parse_pos = 0;

  while (parse_pos < data_len) {
    AttributeHeader agg_checker_ah(data_buf[parse_pos++]);
    assert(agg_checker_ah.getAttributeId() == AttributeHeader::AGG_RESULT &&
           agg_checker_ah.getByteSize() == 0x0721);
    uint32_t n_gb_cols = data_buf[parse_pos] >> 16;
    uint32_t n_agg_results = data_buf[parse_pos++] & 0xFFFF;
    uint32_t n_res_items = data_buf[parse_pos++];
    // fprintf(stderr, "Moz, GB cols: %u, AGG results: %u, RES items: %u\n",
    //         n_gb_cols, n_agg_results, n_res_items);

    if (n_gb_cols) {
      for (uint32_t i = 0; i < n_res_items; i++) {
        uint32_t gb_cols_len = data_buf[parse_pos] >> 16;
        uint32_t agg_res_len = data_buf[parse_pos++] & 0xFFFF;
        // remove compile warnings
        (void)gb_cols_len;
        (void)agg_res_len;
        for (uint32_t j = 0; j < n_gb_cols; j++) {
          AttributeHeader ah(data_buf[parse_pos++]);
          // fprintf(stderr,
          //     "[id: %u, sizeB: %u, sizeW: %u, gb_len: %u, "
          //     "res_len: %u, value: ",
          //     ah.getAttributeId(), ah.getByteSize(),
          //     ah.getDataSize(), gb_cols_len, agg_res_len);
          assert(ah.getDataPtr() != &data_buf[parse_pos]);
          // char* ptr = (char*)(&data_buf[parse_pos]);
          // for (uint32_t i = 0; i < ah.getByteSize(); i++) {
          //   fprintf(stderr, " %x", ptr[i]);
          // }
          parse_pos += ah.getDataSize();
          // fprintf(stderr, "]");
        }
        for (uint32_t i = 0; i < n_agg_results; i++) {
          // AggResItem* ptr = (AggResItem*)(&data_buf[parse_pos]);
          // fprintf(stderr, "(type: %u, is_unsigned: %u, is_null: %u, value: ",
          //         ptr->type, ptr->is_unsigned, ptr->is_null);
          // switch (ptr->type) {
          //   case NDB_TYPE_BIGINT:
          //     fprintf(stderr, "%15ld", ptr->value.val_int64);
          //     break;
          //   case NDB_TYPE_DOUBLE:
          //     fprintf(stderr, "%31.16f", ptr->value.val_double);
          //     break;
          //   default:
          //     assert(0);
          // }
          // fprintf(stderr, ")");
          parse_pos += (sizeof(AggResItem) >> 2);
        }
        // fprintf(stderr, "\n");
      }
    } else {
      assert(n_gb_cols == 0);
      assert(n_agg_results == n_agg_results_);
      assert(n_res_items == 0);
      uint32_t gb_cols_len = data_buf[parse_pos] >> 16;
      uint32_t agg_res_len = data_buf[parse_pos++] & 0xFFFF;
      assert(gb_cols_len == 0);
      assert(agg_res_len == n_agg_results_ * sizeof(AggResItem));
      parse_pos += (agg_res_len >> 2);
    }
  }
  assert(parse_pos == data_len);
#endif // MOZ_AGG_CHECK && !NDEBUG
  return pos;
}

uint32_t AggInterpreter::NumOfResRecords() {
  if (gb_map_) {
    return gb_map_->size();
  } else {
    return 1;
  }
}

#ifdef MOZ_AGG_MALLOC
char* AggInterpreter::MemAlloc(uint32_t len) {
  if (alloc_len_ + len >= MAX_AGG_RESULT_BATCH_BYTES) {
    return nullptr;
  } else {
    char* ptr = &(mem_buf_[alloc_len_]);
    alloc_len_ += len;
    return ptr;
  }
}

void AggInterpreter::Distruct(AggInterpreter* ptr) {
  if (ptr == nullptr) {
    return;
  }
  Ndbd_mem_manager* _mm = ptr->mm();
  uint32_t _page_ref = ptr->page_ref();
  _mm->release_page(RT_DBTUP_PAGE, _page_ref);
}
#endif // MOZ_AGG_MALLOC
