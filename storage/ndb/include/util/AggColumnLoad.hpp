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

#ifndef AGG_COLUMN_LOAD_HPP
#define AGG_COLUMN_LOAD_HPP

/*
 * Stored column value -> aggregation Register.
 *
 * Shared by the pushdown aggregation interpreters in DBTUP
 * (AggInterpreterBase::loadColumnTypedFromBuf, the kOpLoadCol arm of
 * AggInterpreter and JoinAggInterpreter) and by aggregation in the RonSQL
 * layer (RONDB-1124 WP-J J5, ronsql_fs_support_plan.md), so that both turn
 * a stored value into the same accumulator type and value: the kernel's
 * SUM / MIN / MAX / COUNT kernels and the shared merge helpers in
 * NdbAggregationCommon.hpp then give identical results on either side.
 *
 * The input is the value bytes in NDB storage format, as DBTUP reads them
 * after the AttributeHeader and as NdbRecAttr::aRef() returns them.
 *
 * Strings (CHAR / VARCHAR / LONGVARCHAR) are not decoded into the
 * register: their bytes stay where they are and each side captures them
 * its own way (aggStringPayload gives the length prefix and payload).
 *
 * The compiled-code bridge in DbtupJitGlue.cpp has its own register
 * layout and keeps its own copy of the numeric decode.
 */

#include <cassert>
#include <ndb_types.h>
#include <ndb_constants.h>
#include <NdbAggregationCommon.hpp>
#include "decimal.h"
#include "my_byteorder.h"

enum class AggLoadStatus : Uint32 {
  Ok = 0,
  // CHAR / VARCHAR / LONGVARCHAR, not NULL: the register carries the type
  // and a zero value; the caller captures the string bytes.
  String,
  // Not a type aggLoadColumnValue decodes.
  WrongType,
  DecimalParseOverflow,
  DecimalParseError,
  DecimalConvOverflow,
  DecimalConvError
};

// The column types the aggregation interpreters can load.
static inline bool aggTypeSupported(DataType type) {
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

    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:

    // D17: MIN/MAX over DATE.  A DATE is a 3-byte little-endian
    // uint3korr packed value (w = (year<<9)|(month<<5)|day) that is
    // monotonic with chronological order, so it is handled exactly
    // like NDB_TYPE_MEDIUMUNSIGNED at the numeric level (unsigned,
    // aligned to BIGINT).  Only the result *display* differs —
    // RonSQL unpacks w -> YYYY-MM-DD.  Sum/Avg over DATE stay rejected
    // (meaningless); see cte_date_minmax_plan.md.
    case NDB_TYPE_DATE:

    // Temporal extension: YEAR (1-byte unsigned, like TINYUNSIGNED),
    // and DATETIME2 / TIME2 (big-endian memcmp-comparable packed bytes,
    // read MSB-first into the register so unsigned compare == memcmp ==
    // chronological order).  All three reduce to an unsigned integer for
    // MIN/MAX; RonSQL decodes the result for display.  Sum/Avg rejected.
    // TIMESTAMP2 is also big-endian memcmp-comparable; its on-disk epoch
    // ordering is absolute (timezone-independent), so MIN/MAX is exact and
    // TZ only matters at display time (handled in RonSQL).
    case NDB_TYPE_YEAR:
    case NDB_TYPE_DATETIME2:
    case NDB_TYPE_TIME2:
    case NDB_TYPE_TIMESTAMP2:

    // Phase I.6 (F.2): MIN/MAX over CHAR / VARCHAR / Longvarchar.
    // Sum is rejected separately (see Sum()).  Count is
    // type-agnostic and works for any column type.
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return true;
    default:
      return false;
  }
}

// Whether a loaded column compares and accumulates as unsigned.
static inline bool aggIsUnsignedType(DataType type) {
  switch (type) {
    case NDB_TYPE_TINYUNSIGNED:
    case NDB_TYPE_SMALLUNSIGNED:
    case NDB_TYPE_MEDIUMUNSIGNED:
    case NDB_TYPE_UNSIGNED:
    case NDB_TYPE_BIGUNSIGNED:
    case NDB_TYPE_DECIMALUNSIGNED:
    // D17: DATE packed value is an unsigned 3-byte integer; the
    // unsigned compare path (val_uint64) sorts 0000-00-00 (w=0)
    // lowest, as MySQL DATE MIN/MAX requires.
    case NDB_TYPE_DATE:
    // Temporal extension: YEAR / DATETIME2 / TIME2 / TIMESTAMP2 all compare
    // as unsigned (big-endian memcmp order for the "2" types).
    case NDB_TYPE_YEAR:
    case NDB_TYPE_DATETIME2:
    case NDB_TYPE_TIME2:
    case NDB_TYPE_TIMESTAMP2:
      return true;
    default:
      return false;
  }
}

// The register (accumulator) type of a loaded column: integers and the
// packed temporals are BIGINT, FLOAT / DOUBLE are DOUBLE, DECIMAL is BIGINT
// at scale 0 and DOUBLE otherwise, and strings keep their type (the wire
// format stays the source's [length prefix][payload]).  NDB_TYPE_UNDEFINED
// for a type aggTypeSupported() rejects.
static inline DataType aggAlignedType(DataType type, int scale) {
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

    case NDB_TYPE_DATE:
    case NDB_TYPE_YEAR:
    case NDB_TYPE_DATETIME2:
    case NDB_TYPE_TIME2:
    case NDB_TYPE_TIMESTAMP2:
      return NDB_TYPE_BIGINT;
    case NDB_TYPE_FLOAT:
    case NDB_TYPE_DOUBLE:
      return NDB_TYPE_DOUBLE;
    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED:
      return scale == 0 ? NDB_TYPE_BIGINT : NDB_TYPE_DOUBLE;
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return type;
    default:
      return NDB_TYPE_UNDEFINED;
  }
}

/*
 * Decode one stored value of a column of `type` into *reg: type from
 * aggAlignedType(type, scale), is_unsigned as given (aggIsUnsignedType of
 * the column type), is_null as given (the value is then 0).  `data` points
 * at the value bytes and `byte_size` is their length, which the packed
 * temporals (DATETIME2 / TIME2 / TIMESTAMP2) fold MSB-first.  `precision`
 * and `scale` are only read for DECIMAL; `scratch` is a decimal_t with a
 * buffer for bin2decimal (unused for the other types).
 *
 * Strings return AggLoadStatus::String with the register typed and zero.
 */
static inline AggLoadStatus aggLoadColumnValue(DataType type,
                                               bool is_unsigned,
                                               Int32 precision, Int32 scale,
                                               const Uint8* data,
                                               Uint32 byte_size,
                                               bool is_null,
                                               decimal_t* scratch,
                                               Register* reg) {
  reg->type = aggAlignedType(type, scale);
  reg->is_unsigned = is_unsigned;
  reg->is_null = is_null;
  reg->value.val_int64 = 0;
  if (is_null) return AggLoadStatus::Ok;

  const char* cdata = reinterpret_cast<const char*>(data);
  switch (type) {
    case NDB_TYPE_TINYINT:
      reg->value.val_int64 = *reinterpret_cast<const Int8*>(data);
      return AggLoadStatus::Ok;
    case NDB_TYPE_SMALLINT:
      reg->value.val_int64 = sint2korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_MEDIUMINT:
      reg->value.val_int64 = sint3korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_INT:
      reg->value.val_int64 = sint4korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_BIGINT:
      reg->value.val_int64 = sint8korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_TINYUNSIGNED:
      reg->value.val_uint64 = *data;
      return AggLoadStatus::Ok;
    case NDB_TYPE_SMALLUNSIGNED:
      reg->value.val_uint64 = uint2korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_MEDIUMUNSIGNED:
      reg->value.val_uint64 = uint3korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_UNSIGNED:
      reg->value.val_uint64 = uint4korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_BIGUNSIGNED:
      reg->value.val_uint64 = uint8korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_DATE:
      reg->value.val_uint64 = uint3korr(cdata);
      return AggLoadStatus::Ok;
    case NDB_TYPE_YEAR:
      reg->value.val_uint64 = *data;
      return AggLoadStatus::Ok;
    case NDB_TYPE_DATETIME2:
    case NDB_TYPE_TIME2:
    case NDB_TYPE_TIMESTAMP2: {
      Uint64 v = 0;
      for (Uint32 i = 0; i < byte_size; i++) {
        v = (v << 8) | static_cast<Uint64>(data[i]);
      }
      reg->value.val_uint64 = v;
      return AggLoadStatus::Ok;
    }
    case NDB_TYPE_FLOAT:
      reg->value.val_double = floatget(data);
      return AggLoadStatus::Ok;
    case NDB_TYPE_DOUBLE:
      reg->value.val_double = doubleget(data);
      return AggLoadStatus::Ok;
    case NDB_TYPE_DECIMAL:
    case NDB_TYPE_DECIMALUNSIGNED: {
      assert(static_cast<Uint32>(decimal_bin_size(precision, scale)) ==
             byte_size);
      const bool unsigned_dec = (type == NDB_TYPE_DECIMALUNSIGNED);
      assert(is_unsigned == unsigned_dec);
      int dec_ret = bin2decimal(data, scratch, precision, scale);
      if (dec_ret != E_DEC_OK) {
        return dec_ret == E_DEC_OVERFLOW ? AggLoadStatus::DecimalParseOverflow
                                         : AggLoadStatus::DecimalParseError;
      }
      if (unsigned_dec && scratch->sign) {
        return AggLoadStatus::DecimalConvError;
      }
      if (scale != 0) {
        double dbl = 0;
        dec_ret = decimal2double(scratch, &dbl);
        reg->value.val_double = dbl;
      } else if (unsigned_dec) {
        ulonglong ull = 0;
        dec_ret = decimal2ulonglong(scratch, &ull);
        reg->value.val_uint64 = ull;
      } else {
        longlong ll = 0;
        dec_ret = decimal2longlong(scratch, &ll);
        reg->value.val_int64 = ll;
      }
      if (dec_ret != E_DEC_OK) {
        return dec_ret == E_DEC_OVERFLOW ? AggLoadStatus::DecimalConvOverflow
                                         : AggLoadStatus::DecimalConvError;
      }
      return AggLoadStatus::Ok;
    }
    case NDB_TYPE_CHAR:
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_LONGVARCHAR:
      return AggLoadStatus::String;
    default:
      return AggLoadStatus::WrongType;
  }
}

/*
 * The length prefix and payload length of a stored CHAR / VARCHAR /
 * LONGVARCHAR value at `data`: CHAR has no prefix and its payload is
 * `char_len` (the declared width, or the stored byte size where no
 * declaration is at hand), VARCHAR a 1-byte and LONGVARCHAR a 2-byte
 * little-endian length.
 */
static inline void aggStringPayload(DataType type, const Uint8* data,
                                    Uint32 char_len, Uint16* prefix_bytes,
                                    Uint16* payload_len) {
  if (type == NDB_TYPE_CHAR) {
    *prefix_bytes = 0;
    *payload_len = static_cast<Uint16>(char_len);
  } else if (type == NDB_TYPE_VARCHAR) {
    *prefix_bytes = 1;
    *payload_len = static_cast<Uint16>(data[0]);
  } else {
    *prefix_bytes = 2;
    *payload_len = static_cast<Uint16>(
        data[0] | (static_cast<Uint16>(data[1]) << 8));
  }
}

#endif  // AGG_COLUMN_LOAD_HPP
