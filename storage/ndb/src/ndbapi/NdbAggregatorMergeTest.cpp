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
#include "NdbDictionaryImpl.hpp"
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

static bool runResultSizeCases() {
  // Exercise complete wire records above the old limit and around the
  // new limit. Synthetic dictionary sizes avoid needing a cluster.
  const Uint32 recordSizes[] = {12 * 1024, 16 * 1024 - 4,
                               16 * 1024, 16 * 1024 + 4};
  for (Uint32 recordBytes : recordSizes) {
    for (Uint32 op : {Uint32(kOpCount), Uint32(kOpMin), Uint32(kOpMax)}) {
      NdbColumnImpl column;
      column.setType(NdbDictionary::Column::Longvarchar);
      column.setCharset(&my_charset_bin);
      column.m_attrSize = 1;
      // Three result-header words, one group-header word, one slot,
      // and either a key AttributeHeader or a string length word.
      column.m_arraySize = recordBytes - 5 * sizeof(Uint32) -
                           sizeof(AggResItem);

      NdbAggregator agg(nullptr);
      if (op == kOpCount) {
        CHECK(agg.GroupByLinked(0, &column));
        CHECK(agg.LoadInt64(1, 0));
        CHECK(agg.Count(0, 0));
      } else {
        CHECK(agg.LoadLinkedColumn(0, 0, &column));
        CHECK(op == kOpMin ? agg.Min(0, 0) : agg.Max(0, 0));
      }
      const bool accepted = agg.Finalize();
      CHECK(accepted == (recordBytes <= 16 * 1024));
      if (!accepted) CHECK(agg.GetError().errno_ == kErrTooBigResult);
    }
  }

  // COUNT over a wide string returns only a numeric slot, no payload.
  NdbColumnImpl column;
  column.setType(NdbDictionary::Column::Longvarchar);
  column.setCharset(&my_charset_bin);
  column.m_attrSize = 1;
  column.m_arraySize = 16 * 1024;
  NdbAggregator count(nullptr);
  CHECK(count.LoadLinkedColumn(0, 0, &column));
  CHECK(count.Count(0, 0));
  CHECK(count.Finalize());
  return true;
}

/* COUNT, SUM, MIN and MAX grouped by one INT key. */
static const Uint32 kBatchOps[] = {kOpCount, kOpSum, kOpMin, kOpMax};
static const Uint32 kBatchSlots = sizeof(kBatchOps) / sizeof(kBatchOps[0]);

struct BatchGroup {
  Int32 key;
  Uint64 count;
  Int64 sum, min, max;
};

static BatchGroup mergeBatchGroups(const BatchGroup& a, const BatchGroup& b) {
  return {a.key, a.count + b.count, a.sum + b.sum,
          a.min < b.min ? a.min : b.min, a.max > b.max ? a.max : b.max};
}

/* Pack groups into wire records the way the data node does after bounded
 * result packing: stop before a group that would take the record past
 * MAX_AGG_RESULT_BATCH_BYTES, but always emit at least one group. */
static std::vector<std::vector<Uint32>>
packBatchGroups(const std::vector<BatchGroup>& groups) {
  const Uint32 keyBytes = 2 * sizeof(Uint32);
  const Uint32 valueBytes = kBatchSlots * sizeof(AggResItem);
  const Uint32 groupBytes = sizeof(Uint32) + keyBytes + valueBytes;
  std::vector<std::vector<Uint32>> records;
  size_t next = 0;
  while (next < groups.size()) {
    std::vector<Uint32> words = {
        AttributeHeader(AttributeHeader::AGG_RESULT, 0).m_value,
        (1u << 16) | kBatchSlots, 0};
    Uint32 n_groups = 0;
    while (next < groups.size() &&
           (n_groups == 0 || words.size() * sizeof(Uint32) + groupBytes <=
                                 MAX_AGG_RESULT_BATCH_BYTES)) {
      const BatchGroup& g = groups[next++];
      const AggResItem slots[kBatchSlots] = {
          unsignedSlot(g.count), signedSlot(g.sum), signedSlot(g.min),
          signedSlot(g.max)};
      words.push_back((keyBytes << 16) | valueBytes);
      words.push_back(AttributeHeader(0, sizeof(Int32)).m_value);
      words.push_back(static_cast<Uint32>(g.key));
      const Uint32 *raw = reinterpret_cast<const Uint32 *>(slots);
      words.insert(words.end(), raw, raw + valueBytes / sizeof(Uint32));
      n_groups++;
    }
    words[2] = n_groups;
    records.push_back(std::move(words));
  }
  return records;
}

/* Feed the records to a fresh aggregator and compare every merged group
 * against `expected`, indexed by key. */
static bool checkBatchRecords(
    const std::vector<std::vector<Uint32>>& records,
    const std::vector<BatchGroup>& expected) {
  NdbDictionary::Column keyColumn("k");
  keyColumn.setType(NdbDictionary::Column::Int);
  const NdbDictionary::Column *gbColumns[] = {&keyColumn};
  std::vector<Uint32> program(8 + 1 + kBatchSlots, 0);
  program[1] = (1u << 16) | kBatchSlots;
  program[8] = NDB_TYPE_INT;
  for (Uint32 i = 0; i < kBatchSlots; i++) {
    program[9 + i] = (kBatchOps[i] << 26) | i;
  }

  NdbAggregator agg(nullptr);
  agg.initForResults(program.data(), program.size(), gbColumns, 1);
  for (const auto& record : records) {
    CHECK(record.size() * sizeof(Uint32) <= MAX_AGG_RESULT_BATCH_BYTES);
    std::vector<Uint32> copy = record;
    CHECK(agg.ProcessRes(reinterpret_cast<char*>(copy.data())) ==
          static_cast<Int32>(copy.size()));
  }
  agg.PrepareResults();
  std::vector<bool> seen(expected.size(), false);
  Uint32 rows = 0;
  for (auto row = agg.FetchResultRecord(); !row.end();
       row = agg.FetchResultRecord()) {
    auto key = row.FetchGroupbyColumn();
    CHECK(!key.end() && !key.is_null());
    const Int32 k = key.data_int32();
    CHECK(k >= 0 && Uint32(k) < expected.size() && !seen[k]);
    seen[k] = true;
    const BatchGroup& g = expected[k];
    CHECK(g.key == k);
    CHECK(checkValue(row.FetchAggregationResult(), unsignedSlot(g.count)));
    CHECK(checkValue(row.FetchAggregationResult(), signedSlot(g.sum)));
    CHECK(checkValue(row.FetchAggregationResult(), signedSlot(g.min)));
    CHECK(checkValue(row.FetchAggregationResult(), signedSlot(g.max)));
    CHECK(row.FetchAggregationResult().end());
    rows++;
  }
  CHECK(rows == expected.size());
  return true;
}

static bool runResultBatchCases() {
  const Uint32 groupBytes =
      3 * sizeof(Uint32) + kBatchSlots * sizeof(AggResItem);

  // One full record: more groups than the old 8 KB record could carry.
  const Uint32 fullGroups =
      (MAX_AGG_RESULT_BATCH_BYTES - 3 * sizeof(Uint32)) / groupBytes;
  std::vector<BatchGroup> groups;
  for (Uint32 k = 0; k < fullGroups; k++) {
    groups.push_back({Int32(k), 1, Int64(k), Int64(k), Int64(k)});
  }
  auto records = packBatchGroups(groups);
  CHECK(records.size() == 1);
  CHECK(records[0].size() * sizeof(Uint32) > 8192);
  CHECK(records[0].size() * sizeof(Uint32) + groupBytes >
        MAX_AGG_RESULT_BATCH_BYTES);
  if (!checkBatchRecords(records, groups)) return false;

  // One more group starts a second record.
  groups.push_back({Int32(fullGroups), 1, Int64(fullGroups),
                    Int64(fullGroups), Int64(fullGroups)});
  records = packBatchGroups(groups);
  CHECK(records.size() == 2 && records[1][2] == 1);
  if (!checkBatchRecords(records, groups)) return false;

  // A fragment drains under memory pressure, resumes scanning and drains
  // again at the end. Keys 500-999 arrive in both drains and must merge;
  // each drain spans several bounded records.
  std::vector<BatchGroup> first, second, expected;
  for (Int32 k = 0; k < 1000; k++) {
    first.push_back({k, 1, k, k, k});
  }
  for (Int32 k = 500; k < 1500; k++) {
    second.push_back({k, 2, 2 * k + 1000, k - 1000, k + 1000});
  }
  for (Int32 k = 0; k < 1500; k++) {
    if (k < 500) {
      expected.push_back(first[k]);
    } else if (k < 1000) {
      expected.push_back(mergeBatchGroups(first[k], second[k - 500]));
    } else {
      expected.push_back(second[k - 500]);
    }
  }
  records = packBatchGroups(first);
  CHECK(records.size() > 1);
  const auto resumed = packBatchGroups(second);
  CHECK(resumed.size() > 1);
  records.insert(records.end(), resumed.begin(), resumed.end());
  return checkBatchRecords(records, expected);
}

int main() {
  if (ndb_init() != 0) return 1;
  bool passed = runNumericCases();
  if (!runResultSizeCases()) passed = false;
  if (!runResultBatchCases()) passed = false;
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
      ? "PASSED: result-size boundaries, bounded result batches, "
        "numeric boundaries and 18 string failure/recovery cases"
      : "FAILED");
  return passed ? 0 : 1;
}
