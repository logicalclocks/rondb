/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

/*
 * RONDB-1144: the request/response buffer pools. Cluster-free. Each test
 * uses its own RS_BufferPool or RS_BufferArrayManager with explicit
 * inputs, so the global pool and the configuration play no part; the cap
 * derivation and memory-limit detection are pure functions tested on files
 * in a temp directory. The last test drives the /metrics event-counter
 * update from several threads against a live pool.
 */

#include <gtest/gtest.h>

#include <NdbMutex.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "buffer_manager.hpp"
#include "metrics.hpp"

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as error_handling_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

namespace {

constexpr Uint32 kBytes = 64 * 1024;

BufferPoolStats stats_of(RS_BufferPool &pool) {
  BufferPoolStats st{};
  pool.get_stats(st);
  return st;
}

std::string write_temp(const std::string &content) {
  const char *dir = getenv("TMPDIR");
  std::string path = std::string(dir != nullptr ? dir : "/tmp") +
                     "/rdrs_limit_XXXXXX";
  const int fd = mkstemp(path.data());
  EXPECT_GE(fd, 0);
  std::ofstream out(path);
  out << content;
  out.close();
  close(fd);
  return path;
}

}  // namespace

TEST(BufferPool, PreallocatesAndTracksInUseAndFree) {
  RS_BufferPool pool;
  pool.init("test", kBytes, 2, 8, 0);
  BufferPoolStats st = stats_of(pool);
  EXPECT_EQ(st.allocated, 2);
  EXPECT_EQ(st.free, 2);
  EXPECT_EQ(st.in_use, 0);

  RS_Buffer a = pool.get();
  RS_Buffer b = pool.get();
  RS_Buffer c = pool.get();  // third one is mapped on demand
  ASSERT_NE(a.buffer, nullptr);
  ASSERT_NE(b.buffer, nullptr);
  ASSERT_NE(c.buffer, nullptr);
  EXPECT_EQ(a.size, kBytes);
  st = stats_of(pool);
  EXPECT_EQ(st.allocated, 3);
  EXPECT_EQ(st.in_use, 3);
  EXPECT_EQ(st.peak_in_use, 3);
  EXPECT_EQ(st.free, 0);
  EXPECT_EQ(st.total_allocations, 3);

  // A buffer is writable over its whole size.
  memset(c.buffer, 0x5a, c.size);

  pool.put(a);
  pool.put(b);
  pool.put(c);
  st = stats_of(pool);
  EXPECT_EQ(st.in_use, 0);
  EXPECT_EQ(st.free, 3);
  EXPECT_EQ(st.allocated, 3);
  pool.shutdown();
}

/* The cap refuses with a null buffer and counts it; a return makes the pool
 * usable again. */
TEST(BufferPool, CapRefusesAndRecovers) {
  RS_BufferPool pool;
  pool.init("test", kBytes, 1, 2, 0);
  RS_Buffer a = pool.get();
  RS_Buffer b = pool.get();
  ASSERT_NE(a.buffer, nullptr);
  ASSERT_NE(b.buffer, nullptr);

  RS_Buffer refused = pool.get();
  EXPECT_EQ(refused.buffer, nullptr);
  EXPECT_EQ(refused.size, 0U);
  BufferPoolStats st = stats_of(pool);
  EXPECT_EQ(st.rejected, 1);
  EXPECT_EQ(st.allocated, 2);
  EXPECT_EQ(st.in_use, 2);

  pool.put(a);
  RS_Buffer again = pool.get();
  EXPECT_NE(again.buffer, nullptr);
  pool.put(again);
  pool.put(b);
  pool.shutdown();
}

/* Request and response stats are independent. Before RONDB-1144 the
 * request accessor returned the response pool's counters. */
TEST(BufferArrayManager, StatsPerPool) {
  RS_BufferArrayManager mgr;
  mgr.init(kBytes, 2 * kBytes, 1, 8, 50, 0, 1ULL << 30);
  RS_Buffer r1 = mgr.get_req_buffer();
  RS_Buffer r2 = mgr.get_req_buffer();
  RS_Buffer r3 = mgr.get_req_buffer();
  RS_Buffer p1 = mgr.get_resp_buffer();
  BufferPoolStats req{};
  BufferPoolStats resp{};
  mgr.get_req_buffers_stats(req);
  mgr.get_resp_buffers_stats(resp);
  EXPECT_EQ(req.in_use, 3);
  EXPECT_EQ(resp.in_use, 1);
  EXPECT_EQ(r1.size, kBytes);
  EXPECT_EQ(p1.size, 2 * kBytes);
  EXPECT_EQ(mgr.req_buffer_bytes(), kBytes);
  EXPECT_EQ(mgr.resp_buffer_bytes(), 2 * kBytes);
  EXPECT_EQ(mgr.max_buffers(), 8U);
  mgr.return_req_buffer(r1);
  mgr.return_req_buffer(r2);
  mgr.return_req_buffer(r3);
  mgr.return_resp_buffer(p1);
  mgr.get_req_buffers_stats(req);
  mgr.get_resp_buffers_stats(resp);
  EXPECT_EQ(req.in_use, 0);
  EXPECT_EQ(req.free, req.allocated);
  EXPECT_EQ(resp.in_use, 0);
  mgr.shutdown();
}

/* Shrink never touches buffers that are out, and only frees after the
 * quiet period. shrink() takes the clock as a parameter so the test does
 * not have to wait. */
TEST(BufferPool, ShrinkKeepsInFlightAndWaitsForIdle) {
  RS_BufferPool pool;
  pool.init("test", kBytes, 1, 8, 10);
  const Int64 t0 = RS_BufferArrayManager::now_seconds();
  RS_Buffer a = pool.get();
  RS_Buffer b = pool.get();
  RS_Buffer c = pool.get();
  RS_Buffer d = pool.get();
  pool.put(c);
  pool.put(d);
  // Two out, two free (one above the preallocated count); not idle long
  // enough.
  EXPECT_EQ(pool.shrink(t0 + 5), 0U);
  BufferPoolStats st = stats_of(pool);
  EXPECT_EQ(st.allocated, 4);

  // Idle long enough: only the free surplus goes, down to preallocate.
  EXPECT_EQ(pool.shrink(t0 + 11), 1U);
  st = stats_of(pool);
  EXPECT_EQ(st.allocated, 3);
  EXPECT_EQ(st.free, 1);
  EXPECT_EQ(st.in_use, 2);
  EXPECT_EQ(st.shrunk, 1);
  // The buffers still out are intact.
  memset(a.buffer, 1, a.size);
  memset(b.buffer, 2, b.size);
  pool.put(a);
  pool.put(b);
  // Returning does not restart the clock; the surplus is freed next time.
  EXPECT_EQ(pool.shrink(t0 + 12), 2U);
  st = stats_of(pool);
  EXPECT_EQ(st.allocated, 1);
  EXPECT_EQ(st.free, 1);
  EXPECT_EQ(st.shrunk, 3);
  pool.shutdown();
}

/* A get() that puts more than the preallocated count in use restarts the
 * quiet period. The pool reads the clock through now_seconds(), which the test
 * shifts to place the demand nine seconds into a ten second period. */
TEST(BufferPool, DemandRestartsTheQuietPeriod) {
  RS_BufferPool pool;
  pool.init("test", kBytes, 1, 8, 10);
  const Int64 t0 = RS_BufferArrayManager::now_seconds();
  RS_Buffer a = pool.get();
  RS_Buffer b = pool.get();
  pool.put(a);
  pool.put(b);
  // Fresh demand at t0 + 9: more than the preallocated one in use again.
  RS_BufferArrayManager::set_clock_offset_for_tests(9);
  RS_Buffer c = pool.get();
  RS_Buffer d = pool.get();
  pool.put(c);
  pool.put(d);
  RS_BufferArrayManager::set_clock_offset_for_tests(0);
  EXPECT_EQ(pool.shrink(t0 + 11), 0U) << "period restarted by the demand";
  EXPECT_EQ(pool.shrink(t0 + 19), 1U);
  pool.shutdown();
}

TEST(BufferPool, ShrinkDisabledWhenZero) {
  RS_BufferPool pool;
  pool.init("test", kBytes, 1, 8, 0);
  RS_Buffer a = pool.get();
  RS_Buffer b = pool.get();
  pool.put(a);
  pool.put(b);
  EXPECT_EQ(pool.shrink(RS_BufferArrayManager::now_seconds() + 1000), 0U);
  EXPECT_EQ(stats_of(pool).allocated, 2);
  pool.shutdown();
}

TEST(BufferArrayManager, DeriveMaxBuffers) {
  const Uint64 one_gib = 1ULL << 30;
  const Uint64 pair = 12ULL << 20;  // 12 MiB
  // 50% of 1 GiB over 12 MiB pairs: 42.
  EXPECT_EQ(RS_BufferArrayManager::derive_max_buffers(one_gib, 50, pair, 0, 2),
            42U);
  // The override wins as is.
  EXPECT_EQ(RS_BufferArrayManager::derive_max_buffers(one_gib, 50, pair, 8, 2),
            8U);
  // Never below preallocate + 1, however small the limit.
  EXPECT_EQ(RS_BufferArrayManager::derive_max_buffers(1 << 20, 50, pair, 0, 32),
            33U);
  // 500 MiB pod, 768 KiB pair (26.02 computed sizes): about 333.
  EXPECT_EQ(RS_BufferArrayManager::derive_max_buffers(500ULL << 20, 50,
                                                       768ULL << 10, 0, 32),
            333U);
}

TEST(BufferArrayManager, DetectMemoryLimitFromCgroupFiles) {
  const std::string v2_bytes = write_temp("536870912\n");
  const std::string v2_max = write_temp("max\n");
  const std::string v1_bytes = write_temp("1073741824\n");
  const std::string v1_unlimited = write_temp("9223372036854771712\n");
  const std::string missing = "/nonexistent/rdrs/memory.max";

  EXPECT_EQ(RS_BufferArrayManager::detect_memory_limit(v2_bytes.c_str(),
                                                       v1_bytes.c_str()),
            536870912ULL);
  // v2 says "max": fall through to v1.
  EXPECT_EQ(RS_BufferArrayManager::detect_memory_limit(v2_max.c_str(),
                                                       v1_bytes.c_str()),
            1073741824ULL);
  // Neither file is a limit: physical RAM.
  const Uint64 physical = RS_BufferArrayManager::physical_memory_bytes();
  EXPECT_GT(physical, 0ULL);
  EXPECT_EQ(RS_BufferArrayManager::detect_memory_limit(v2_max.c_str(),
                                                       v1_unlimited.c_str()),
            physical);
  EXPECT_EQ(RS_BufferArrayManager::detect_memory_limit(missing.c_str(),
                                                       missing.c_str()),
            physical);

  remove(v2_bytes.c_str());
  remove(v2_max.c_str());
  remove(v1_bytes.c_str());
  remove(v1_unlimited.c_str());
}

/* The manager derives the cap from the limit it is given. */
TEST(BufferArrayManager, InitDerivesCapFromLimit) {
  RS_BufferArrayManager mgr;
  mgr.init(kBytes, kBytes, 2, 0, 50, 0, 64ULL * kBytes);
  // 50% of 64 buffers' worth, over 2-buffer pairs: 16.
  EXPECT_EQ(mgr.max_buffers(), 16U);
  EXPECT_EQ(mgr.memory_limit_bytes(), 64ULL * kBytes);
  mgr.shutdown();
}

/* Concurrent /metrics scrapes must count every pool event exactly once.
 * One thread makes a capped pool refuse buffers (each refusal is a
 * "rejected" event) while four scrapers read the pool's cumulative stats
 * and claim the growth through claim_counter_delta, as writeMetrics does,
 * adding their claims to one counter. Scrapers race each other and may
 * hold an older total than another; a final scrape after the refusals
 * stop must leave the counter equal to the pool's rejected total. */
TEST(BufferPoolMetrics, ConcurrentScrapesCountEachEventOnce) {
  RS_BufferPool pool;
  pool.init("test", kBytes, 1, 1, 0);
  RS_Buffer held = pool.get();
  ASSERT_NE(held.buffer, nullptr);

  constexpr int kRefusals = 20000;
  constexpr int kScrapers = 4;
  std::atomic<Int64> last{0};
  std::atomic<Int64> counter{0};
  std::atomic<bool> done{false};

  auto scrape = [&]() {
    BufferPoolStats st;
    pool.get_stats(st);
    counter.fetch_add(rdrs_metrics::claim_counter_delta(last, st.rejected));
  };
  std::vector<std::thread> scrapers;
  for (int i = 0; i < kScrapers; i++) {
    scrapers.emplace_back([&]() {
      while (!done.load()) scrape();
    });
  }
  for (int i = 0; i < kRefusals; i++) {
    ASSERT_EQ(pool.get().buffer, nullptr);
  }
  done.store(true);
  for (auto &t : scrapers) t.join();
  scrape();

  BufferPoolStats st;
  pool.get_stats(st);
  EXPECT_EQ(st.rejected, kRefusals);
  EXPECT_EQ(counter.load(), kRefusals);
  EXPECT_EQ(last.load(), kRefusals);
  pool.put(held);
  pool.shutdown();
}
