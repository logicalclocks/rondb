/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <ndb_global.h>
#include <mysql/strings/m_ctype.h>
#include <NdbAggregator.hpp>
#include <kernel/AttributeHeader.hpp>

#include <cstdio>
#include <cstring>
#include <vector>

#define CHECK(condition)                                                \
  do {                                                                  \
    if (!(condition)) {                                                 \
      fprintf(stderr, "FAIL line %d: %s\n", __LINE__, #condition);        \
      return false;                                                     \
    }                                                                   \
  } while (0)

/* One wire record: MIN(string), SUM(BIGINT), MAX(string), optionally
 * grouped by one INT key. Both strings have one payload byte. */
static std::vector<Uint32>
makePacket(bool grouped, Uint32 stringType, bool nullStrings,
           char before, char after, Int64 number) {
  const Uint32 keyBytes = grouped ? 8 : 0;
  AggResItem slots[3] = {};
  slots[0].type = slots[2].type = stringType;
  slots[0].is_null = slots[2].is_null = nullStrings;
  slots[1].type = NDB_TYPE_BIGINT;
  slots[1].value.val_int64 = number;

  std::vector<Uint32> words(4 + keyBytes / 4 + sizeof(slots) / 4, 0);
  words[0] = AttributeHeader(AttributeHeader::AGG_CHAR_RESULT, 0).m_value;
  words[1] = (Uint32(grouped) << 16) | 3;
  words[2] = 1;
  if (grouped) {
    words[4] = AttributeHeader(0, sizeof(Int32)).m_value;
    words[5] = 7;
  }
  memcpy(words.data() + 4 + keyBytes / 4, slots, sizeof(slots));
  if (!nullStrings) {
    const Uint32 prefix = stringType == NDB_TYPE_CHAR ? 0 :
                          stringType == NDB_TYPE_VARCHAR ? 1 : 2;
    for (char value : {before, after}) {
      unsigned char bytes[4] = {};
      if (prefix != 0) bytes[0] = 1;
      bytes[prefix] = static_cast<unsigned char>(value);
      Uint32 payload;
      memcpy(&payload, bytes, sizeof(payload));
      words.push_back(prefix + 1);
      words.push_back(payload);
    }
  }
  const Uint32 valueBytes = (words.size() - 4) * 4 - keyBytes;
  words[3] = (keyBytes << 16) | valueBytes;
  return words;
}

static bool checkString(NdbAggregator::Result value, char expected) {
  CHECK(!value.end() && !value.is_null());
  CHECK(value.type() == NdbDictionary::Column::Char ||
        value.type() == NdbDictionary::Column::Varchar ||
        value.type() == NdbDictionary::Column::Longvarchar);
  Uint32 length = 0;
  const char *text = value.data_str(&length);
  CHECK(length == 1 && text[0] == expected);
  return true;
}

static bool runCase(bool grouped, Uint32 stringType, Uint32 ownershipCase) {
  /* Case 0 replaces the first string, case 1 retains it, and case 2
   * transfers a first non-NULL string into the accumulator. The trailing
   * string is decoded but not visited when SUM fails. */
  const bool nullSeed = ownershipCase == 2;
  const char before = ownershipCase == 1 ? 'z' : 'a';
  NdbDictionary::Column stringColumn("s");
  stringColumn.setType(
      static_cast<NdbDictionary::Column::Type>(stringType));
  stringColumn.setLength(stringType == NDB_TYPE_LONGVARCHAR ? 300 : 8);
  stringColumn.setCharset(&my_charset_bin);
  const NdbDictionary::Column *columns[] = {
    &stringColumn, nullptr, &stringColumn
  };

  std::vector<Uint32> program(8 + Uint32(grouped) + 3, 0);
  program[1] = (Uint32(grouped) << 16) | 3;
  if (grouped) program[8] = NDB_TYPE_INT;
  const Uint32 instructions = 8 + Uint32(grouped);
  program[instructions] = Uint32(kOpMin) << 26;
  program[instructions + 1] = (Uint32(kOpSum) << 26) | 1;
  program[instructions + 2] = (Uint32(kOpMax) << 26) | 2;

  auto seed = makePacket(grouped, stringType, nullSeed, 'm', 'm', INT64_MAX);
  auto contribution = makePacket(grouped, stringType, false, before, 'z', 1);

  {
    NdbAggregator agg(nullptr);
    agg.initForResults(program.data(), program.size(), nullptr, 0, columns, 3);
    CHECK(agg.ProcessRes(reinterpret_cast<char*>(seed.data())) ==
          static_cast<Int32>(seed.size()));
    const Int32 result =
        agg.ProcessRes(reinterpret_cast<char*>(contribution.data()));
    CHECK(result == -1860);
    CHECK(!agg.finished());
    /* Destruction must free transferred strings exactly once. Run this
     * test with ASan/LSan to also check the untransferred temporary strings
     * released by ProcessRes, including the unvisited trailing slot. */
  }

  /* In-range values must still merge successfully. A fresh aggregator
   * also verifies recovery after failure/destruction. */
  seed = makePacket(grouped, stringType, nullSeed, 'm', 'm', 10);
  contribution = makePacket(grouped, stringType, false, before, 'z', 20);
  {
    NdbAggregator agg(nullptr);
    agg.initForResults(program.data(), program.size(), nullptr, 0, columns, 3);
    CHECK(agg.ProcessRes(reinterpret_cast<char*>(seed.data())) ==
          static_cast<Int32>(seed.size()));
    CHECK(agg.ProcessRes(reinterpret_cast<char*>(contribution.data())) ==
          static_cast<Int32>(contribution.size()));
    agg.PrepareResults();
    auto row = agg.FetchResultRecord();
    CHECK(!row.end());
    CHECK(checkString(row.FetchAggregationResult(),
                      ownershipCase == 1 ? 'm' : 'a'));
    auto sum = row.FetchAggregationResult();
    CHECK(!sum.end() && !sum.is_null());
    CHECK(sum.type() == NdbDictionary::Column::Bigint);
    CHECK(sum.data_int64() == 30);
    CHECK(checkString(row.FetchAggregationResult(), 'z'));
    CHECK(row.FetchAggregationResult().end());
    CHECK(agg.FetchResultRecord().end());
  }
  return true;
}

static AggResItem signedSlot(Int64 value) {
  AggResItem slot = {};
  slot.type = NDB_TYPE_BIGINT;
  slot.value.val_int64 = value;
  return slot;
}

static AggResItem unsignedSlot(Uint64 value) {
  AggResItem slot = {};
  slot.type = NDB_TYPE_BIGINT;
  slot.is_unsigned = true;
  slot.value.val_uint64 = value;
  return slot;
}

static AggResItem doubleSlot(double value) {
  AggResItem slot = {};
  slot.type = NDB_TYPE_DOUBLE;
  slot.value.val_double = value;
  return slot;
}

static bool checkValue(NdbAggregator::Result actual, AggResItem expected) {
  NdbAggregator::Result wanted(&expected, false);
  CHECK(!actual.end() && !actual.is_null());
  CHECK(actual.type() == wanted.type());
  if (expected.type == NDB_TYPE_DOUBLE)
    CHECK(actual.data_double() == expected.value.val_double);
  else if (expected.is_unsigned)
    CHECK(actual.data_uint64() == expected.value.val_uint64);
  else
    CHECK(actual.data_int64() == expected.value.val_int64);
  return true;
}

static std::vector<Uint32>
makeNumericPacket(bool grouped, const AggResItem& slot) {
  const Uint32 keyBytes = grouped ? 8 : 0;
  std::vector<Uint32> words(4 + keyBytes / 4 + sizeof(slot) / 4, 0);
  words[0] = AttributeHeader(AttributeHeader::AGG_RESULT, 0).m_value;
  words[1] = (Uint32(grouped) << 16) | 1;
  words[2] = 1;
  words[3] = (keyBytes << 16) | sizeof(slot);
  if (grouped) {
    words[4] = AttributeHeader(0, sizeof(Int32)).m_value;
    words[5] = 7;
  }
  memcpy(words.data() + 4 + keyBytes / 4, &slot, sizeof(slot));
  return words;
}

static bool runNumericCase(AggResItem left, AggResItem right,
                           AggResItem expected, bool overflow,
                           Uint32 op, bool grouped) {
  // Check the shared kernel/API helper, including no mutation on failure.
  AggResItem merged = left;
  CHECK(aggMergeNumericSlot(&merged, right, op) == (overflow ? 1860 : 0));
  CHECK(checkValue(NdbAggregator::Result(&merged, false),
                   overflow ? left : expected));

  std::vector<Uint32> program(9 + Uint32(grouped), 0);
  program[1] = (Uint32(grouped) << 16) | 1;
  if (grouped) program[8] = NDB_TYPE_INT;
  program[8 + Uint32(grouped)] = op << 26;
  auto seed = makeNumericPacket(grouped, left);
  auto contribution = makeNumericPacket(grouped, right);
  NdbAggregator agg(nullptr);
  agg.initForResults(program.data(), program.size());
  CHECK(agg.ProcessRes(reinterpret_cast<char*>(seed.data())) ==
        static_cast<Int32>(seed.size()));
  const Int32 result =
      agg.ProcessRes(reinterpret_cast<char*>(contribution.data()));
  if (overflow) {
    CHECK(result == -1860);
    CHECK(!agg.finished());
  } else {
    CHECK(result == static_cast<Int32>(contribution.size()));
    agg.PrepareResults();
    auto row = agg.FetchResultRecord();
    CHECK(!row.end());
    CHECK(checkValue(row.FetchAggregationResult(), expected));
    CHECK(row.FetchAggregationResult().end());
    CHECK(agg.FetchResultRecord().end());
  }
  return true;
}

static bool runNumericCases() {
  const auto S = signedSlot;
  const auto U = unsignedSlot;
  const auto D = doubleSlot;
  const Uint64 highBit = Uint64(1) << 63;
  struct Case {
    AggResItem left, right, expected;
    bool overflow = false;
  };
  const Case cases[] = {
    {S(INT64_MAX), S(1), {}, true},
    {S(INT64_MIN), S(-1), {}, true},
    {S(INT64_MAX), S(INT64_MAX), {}, true},
    {S(INT64_MIN), S(INT64_MIN), {}, true},
    {S(INT64_MAX - 1), S(1), S(INT64_MAX)},
    {S(INT64_MIN + 1), S(-1), S(INT64_MIN)},
    {S(INT64_MIN), S(INT64_MAX), S(-1)},
    {S(INT64_MAX), S(0), S(INT64_MAX)},
    {S(INT64_MIN), S(0), S(INT64_MIN)},
    {U(UINT64_MAX), U(1), {}, true},
    {U(UINT64_MAX), U(UINT64_MAX), {}, true},
    {U(UINT64_MAX - 1), U(1), U(UINT64_MAX)},
    {U(UINT64_MAX), U(0), U(UINT64_MAX)},
    {U(highBit), U(highBit), {}, true},
    {U(UINT64_MAX), S(1), {}, true},
    {U(UINT64_MAX - 1), S(1), U(UINT64_MAX)},
    {U(1), S(INT64_MAX), U(highBit)},
    {U(0), S(-1), {}, true},
    {U(1), S(-2), {}, true},
    {U(1), S(-1), U(0)},
    {U(2), S(-1), U(1)},
    {U(highBit - 1), S(INT64_MIN), {}, true},
    {U(highBit), S(INT64_MIN), U(0)},
    {U(UINT64_MAX), S(INT64_MIN), U(INT64_MAX)},
    {U(UINT64_MAX), S(-1), U(UINT64_MAX - 1)},
    {U(UINT64_MAX), S(0), U(UINT64_MAX)},
    {U(0), S(0), U(0)},
    {D(1.5), D(2.25), D(3.75)},
    {D(0.5), S(-2), D(-1.5)},
    {D(0.5), U(2), D(2.5)},
  };
  Uint32 index = 0;
  for (const auto& c : cases) {
    for (bool reverse : {false, true}) {
      for (bool grouped : {false, true}) {
        for (Uint32 op : {Uint32(kOpSum), Uint32(kOpSumBigint),
                          Uint32(kOpSumDouble)}) {
          if (!runNumericCase(reverse ? c.right : c.left,
                              reverse ? c.left : c.right,
                              c.expected, c.overflow, op, grouped)) {
            fprintf(stderr, "numeric case=%u reverse=%u grouped=%u op=%u\n",
                    index, unsigned(reverse), unsigned(grouped), op);
            return false;
          }
        }
      }
    }
    index++;
  }
  return true;
}

int main() {
  if (ndb_init() != 0) return 1;
  bool passed = runNumericCases();
  for (bool grouped : {false, true}) {
    for (Uint32 type : {Uint32(NDB_TYPE_CHAR), Uint32(NDB_TYPE_VARCHAR),
                        Uint32(NDB_TYPE_LONGVARCHAR)}) {
      for (Uint32 ownershipCase = 0; ownershipCase < 3; ownershipCase++) {
        if (!runCase(grouped, type, ownershipCase)) {
          fprintf(stderr, "grouped=%u type=%u ownershipCase=%u\n",
                  unsigned(grouped), type, ownershipCase);
          passed = false;
        }
      }
    }
  }
  ndb_end(0);
  printf("%s\n", passed
      ? "PASSED: numeric boundaries and 18 string failure/recovery cases"
      : "FAILED");
  return passed ? 0 : 1;
}
