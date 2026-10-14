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
 * RONDB-1144: process startup settings. These tests are cluster-free.
 * The memory observations are Linux-only (prctl, glibc malloc_info,
 * /proc); elsewhere they are skipped, not faked.
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

#include "config_structs.hpp"
#include "json_parser.hpp"
#include "process_setup.hpp"

#ifdef __linux__
#include <malloc.h>
#include <sys/prctl.h>
#include <unistd.h>
#endif

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as error_handling_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

namespace {

#ifdef __linux__
/* Resident set of this process in bytes, from /proc/self/statm. */
long resident_bytes() {
  std::ifstream statm("/proc/self/statm");
  long size_pages = 0;
  long resident_pages = 0;
  statm >> size_pages >> resident_pages;
  return resident_pages * sysconf(_SC_PAGESIZE);
}

/* Value of one "Key:" line in /proc/self/status, or "" when absent. */
std::string proc_status_field(const char *key) {
  std::ifstream status("/proc/self/status");
  std::string line;
  const std::string prefix = std::string(key) + ":";
  while (std::getline(status, line)) {
    if (line.compare(0, prefix.size(), prefix) == 0) {
      std::string value = line.substr(prefix.size());
      const size_t first = value.find_first_not_of(" \t");
      return first == std::string::npos ? "" : value.substr(first);
    }
  }
  return "";
}

/* Number of malloc arenas glibc reports, counted from malloc_info's
 * <heap nr="..."> elements. M_ARENA_MAX has no getter, so this is the only
 * observable. */
int glibc_arena_count() {
  char *text = nullptr;
  size_t text_len = 0;
  FILE *stream = open_memstream(&text, &text_len);
  if (stream == nullptr) {
    return -1;
  }
  malloc_info(0, stream);
  fclose(stream);
  int count = 0;
  const char *needle = "<heap nr=";
  for (const char *p = strstr(text, needle); p != nullptr;
       p = strstr(p + 1, needle)) {
    count++;
  }
  free(text);
  return count;
}
#endif

}  // namespace

/*
 * The THP opt-out is per process and inherited by threads created after
 * the call, which is why main() issues it before start_fs_cache().
 */
TEST(ProcessSetup, ThpDisabledForProcessAndLaterThreads) {
#ifdef __linux__
  const ProcessSetupResult setup = rdrs_process_setup();
  EXPECT_TRUE(setup.thp_disabled) << "errno " << setup.errno_thp;
  EXPECT_EQ(prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0), 1);

  // Kernels that report the flag in /proc/self/status must show it off.
  const std::string reported = proc_status_field("THP_enabled");
  if (!reported.empty()) {
    EXPECT_EQ(reported, "0");
  }

  int seen_by_thread = -1;
  std::thread t([&seen_by_thread]() {
    seen_by_thread = prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0);
  });
  t.join();
  EXPECT_EQ(seen_by_thread, 1);

  // Idempotent: a second call reports success again.
  EXPECT_TRUE(rdrs_process_setup().thp_disabled);
#else
  const ProcessSetupResult setup = rdrs_process_setup();
  EXPECT_FALSE(setup.thp_disabled);
  EXPECT_EQ(setup.errno_thp, 0);
  GTEST_SKIP() << "transparent huge pages are a Linux facility";
#endif
}

/*
 * With M_ARENA_MAX in place, 64 threads allocating and freeing at the
 * same time share at most that many arenas instead of creating one each.
 */
TEST(ProcessSetup, MallocArenaCountIsCapped) {
#if defined(__linux__) && defined(__GLIBC__)
  const ProcessSetupResult setup = rdrs_process_setup();
  ASSERT_TRUE(setup.arena_max_set);

  constexpr int kThreads = 64;
  constexpr size_t kBlock = 256 * 1024;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; i++) {
    threads.emplace_back([]() {
      for (int round = 0; round < 8; round++) {
        void *block = malloc(kBlock);
        ASSERT_NE(block, nullptr);
        memset(block, 1, kBlock);
        free(block);
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  const int arenas = glibc_arena_count();
  ASSERT_GE(arenas, 1) << "malloc_info unavailable";
  EXPECT_LE(arenas, RDRS_MALLOC_ARENA_MAX);
#else
  GTEST_SKIP() << "glibc malloc arenas are a Linux facility";
#endif
}

/*
 * The per-thread JSON parse buffer must not be resident until used.
 * std::make_unique<char[]> zero-filled it, which made 64 x 4 MiB resident
 * at startup; the plain new char[] does not touch the pages.
 */
TEST(ProcessSetup, ParseBufferIsNotResidentUntilUsed) {
#ifdef __linux__
  constexpr unsigned kBufferBytes = 256U * 1024U * 1024U;
  const unsigned saved = globalConfigs.internal.maxReqSize;
  globalConfigs.internal.maxReqSize = kBufferBytes;

  const long before = resident_bytes();
  JSONParser *parser = new JSONParser();
  const long after = resident_bytes();
  delete parser;
  globalConfigs.internal.maxReqSize = saved;

  // Allow for the parser object itself and allocator bookkeeping, but not
  // for the buffer: a zero-filled buffer would add the full 256 MiB.
  EXPECT_LT(after - before, 4L * 1024L * 1024L)
      << "parse buffer pages were touched at allocation";
#else
  GTEST_SKIP() << "resident-set accounting through /proc is Linux-only";
#endif
}
