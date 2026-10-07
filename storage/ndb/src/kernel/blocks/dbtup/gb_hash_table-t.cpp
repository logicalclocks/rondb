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

/*
 * GBHashTable (AggHashTable.hpp) without a data node: growth by linear
 * hashing and the growth hint of m3_run6_plan.md D1a (a table that takes
 * the hinted number of buckets at its first split).  Keys are 8-byte
 * values on the raw comparison path (no column type metadata).  Bucket
 * segments come from malloc through this file's agg_gb_segment_alloc,
 * which counts what is live and can be told to fail one allocation.
 * BUCKET_COUNT is 16, so growth crosses many segments with few keys.
 */

#include <util/NdbTap.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "NdbAggregationCommon.hpp"
#include <AttributeHeader.hpp>
#include "AggHashTable.hpp"

static int g_attempts = 0;     // agg_gb_segment_alloc calls
static int g_live = 0;         // allocations not yet freed
static int g_fail_attempt = -1;

void *agg_gb_segment_alloc(size_t bytes, Uint32) {
  if (g_attempts++ == g_fail_attempt) {
    return nullptr;
  }
  void *p = malloc(bytes);
  if (p != nullptr) g_live++;
  return p;
}

void agg_gb_segment_free(void *ptr) {
  g_live--;
  free(ptr);
}

typedef GBHashTable<16> Table;
static const Uint32 KEY_LEN = sizeof(Uint64);
static uchar g_xfrm[64];

/* Group records: the link header followed by the key. */
struct Records {
  std::vector<char *> raw;
  char *make(Uint64 key) {
    char *r = static_cast<char *>(calloc(1, Table::OVERHEAD + KEY_LEN));
    *reinterpret_cast<Uint32 *>(r + Table::KEY_LEN_OFFSET) = KEY_LEN;
    memcpy(r + Table::OVERHEAD, &key, KEY_LEN);
    raw.push_back(r);
    return r + Table::OVERHEAD;
  }
  ~Records() {
    for (char *r : raw) free(r);
  }
};

static Uint64 key_of(Uint32 i) { return Uint64(i) * 0x9e3779b97f4a7c15ULL + 7; }

static Uint32 bucket_of(Table &t, Uint64 key) {
  return t.hashKey(reinterpret_cast<const char *>(&key), KEY_LEN, g_xfrm,
                   sizeof(g_xfrm));
}

static bool has(Table &t, Uint64 key) {
  return t.find(reinterpret_cast<const char *>(&key), KEY_LEN, g_xfrm,
                sizeof(g_xfrm)) != nullptr;
}

/* Insert as JoinAggInterpreter::ProcessRec does: one hash, the lookup's
 * bucket for the insert. */
static void add(Table &t, Records &recs, Uint64 key) {
  const Uint32 b = bucket_of(t, key);
  OK(t.findInBucket(b, reinterpret_cast<const char *>(&key), KEY_LEN) ==
     nullptr);
  t.insertRawInBucket(b, recs.make(key), g_xfrm, sizeof(g_xfrm));
}

static void add_range(Table &t, Records &recs, Uint32 from, Uint32 to) {
  for (Uint32 i = from; i < to; i++) add(t, recs, key_of(i));
}

static void check_all(Table &t, Uint32 n) {
  OK(t.size() == n);
  for (Uint32 i = 0; i < n; i++) OK(has(t, key_of(i)));
  Uint32 walked = 0;
  for (Table::Iterator it = t.begin(); it.valid(); t.next(it)) walked++;
  OK(walked == n);
}

static Uint32 segments_for(Uint32 groups) {
  return (groups + 15) / 16 * 16;
}

/* Without a hint: one split per insert past load factor one. */
static void test_growth_without_hint() {
  Table t;
  Records recs;
  t.init(0);
  add_range(t, recs, 0, 16);
  OK(t.bucketCount() == 16);
  add_range(t, recs, 16, 5000);
  OK(t.bucketCount() == 5000);
  OK(t.peakSize() == 5000);
  check_all(t, 5000);
  t.clear();
  OK(g_live == 0);
  OK(t.peakSize() == 0);
}

/* With a hint: the inline buckets fill, the first split takes the hinted
 * buckets at once, and every entry moves to a bucket congruent to its old
 * one, at the same or a higher index (the resumable-walk guarantee). */
static void test_hint_jump() {
  Table t;
  Records recs;
  t.init(0);
  t.setGrowthHint(3000);
  add_range(t, recs, 0, 16);
  OK(t.bucketCount() == 16);
  Uint32 before[16];
  for (Uint32 i = 0; i < 16; i++) before[i] = bucket_of(t, key_of(i));
  add(t, recs, key_of(16));
  OK(t.bucketCount() == segments_for(3000));
  for (Uint32 i = 0; i < 16; i++) {
    const Uint32 now = bucket_of(t, key_of(i));
    OK(now >= before[i]);
    OK(now % 16 == before[i]);
  }
  check_all(t, 17);
  add_range(t, recs, 17, segments_for(3000));
  OK(t.bucketCount() == segments_for(3000));  // no split up to the hint
  add_range(t, recs, segments_for(3000), 5000);
  OK(t.bucketCount() == 5000);                // then splitting goes on
  check_all(t, 5000);
  t.clear();
  OK(g_live == 0);
}

/* A hint inside the inline buckets changes nothing; init() drops a hint. */
static void test_no_jump() {
  Table t;
  Records recs;
  t.init(0);
  t.setGrowthHint(10);
  add_range(t, recs, 0, 17);
  OK(t.bucketCount() == 17);
  t.clear();
  Records recs2;
  t.setGrowthHint(3000);
  t.init(0);
  add_range(t, recs2, 0, 17);
  OK(t.bucketCount() == 17);
  t.clear();
  OK(g_live == 0);
}

/* A failed allocation during the jump releases what it took, drops the
 * hint and leaves the table splitting as before. */
static void test_jump_allocation_failure() {
  for (int fail = 0; fail < 3; fail++) {  // the directory, segment 1, 2
    Table t;
    Records recs;
    t.init(0);
    t.setGrowthHint(3000);
    add_range(t, recs, 0, 16);
    g_fail_attempt = g_attempts + fail;
    add(t, recs, key_of(16));
    g_fail_attempt = -1;
    OK(t.bucketCount() == 17);
    add_range(t, recs, 17, 1000);
    OK(t.bucketCount() == 1000);
    check_all(t, 1000);
    t.clear();
    OK(g_live == 0);
  }
}

/* The hint is capped at MAX_SEGMENTS, after which growth stops. */
static void test_hint_cap() {
  Table t;
  Records recs;
  t.init(0);
  t.setGrowthHint(0xFFFFFFFF);
  add_range(t, recs, 0, 17);
  OK(t.bucketCount() == 16 * Table::MAX_SEGMENTS);
  check_all(t, 17);
  t.clear();
  OK(g_live == 0);
}

/* Two tables with the same hint and no growth beyond it share their
 * geometry, so JoinAggInterpreter::mergeFrom reuses the source bucket. */
static void test_same_geometry() {
  Table a, b;
  Records recs;
  a.init(0);
  b.init(0);
  a.setGrowthHint(3000);
  b.setGrowthHint(3000);
  add_range(a, recs, 0, 2000);
  for (Uint32 i = 2000; i < 2500; i++) add(b, recs, key_of(i));
  OK(a.sameGeometry(b));
  for (Uint32 i = 0; i < 2000; i++) {
    OK(bucket_of(a, key_of(i)) == bucket_of(b, key_of(i)));
  }
  a.clear();
  b.clear();
  OK(g_live == 0);
}

/* The peak survives erasing, release() resets it. */
static void test_peak() {
  Table t;
  Records recs;
  t.init(0);
  add_range(t, recs, 0, 100);
  Uint32 erased = 0;
  Table::Iterator it = t.begin();
  while (it.valid() && erased < 60) {
    t.eraseAndNext(it);
    erased++;
  }
  OK(t.size() == 40);
  OK(t.peakSize() == 100);
  while (t.popNext(nullptr) != nullptr) {
  }
  t.release();
  OK(t.peakSize() == 0);
  OK(g_live == 0);
}

TAPTEST(GBHashTable) {
  test_growth_without_hint();
  test_hint_jump();
  test_no_jump();
  test_jump_allocation_failure();
  test_hint_cap();
  test_same_geometry();
  test_peak();
  printf("GBHashTable growth and growth hint: all checks passed\n");
  return 1;
}
