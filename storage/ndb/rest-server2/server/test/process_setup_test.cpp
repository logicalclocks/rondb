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
 * The memory observations are Linux-only (prctl, /proc); elsewhere they
 * are skipped, not faked.
 */

#include <gtest/gtest.h>

#include <NdbMutex.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>

#include "config_structs.hpp"
#include "json_parser.hpp"
#include "process_setup.hpp"

#ifdef __linux__
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

#endif

}  // namespace

/*
 * The THP opt-out belongs to the address space, so every thread sees it;
 * main() issues it before start_fs_cache() because before Linux 5.0 it
 * only applied to mappings created after the call.
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
 * Internal.DisableTransparentHugePages false undoes the startup opt-out
 * for the whole process, threads included; the opt-out can be made again.
 */
TEST(ProcessSetup, ThpCanBeAllowedAgain) {
#ifdef __linux__
  ASSERT_TRUE(rdrs_process_setup().thp_disabled);
  const ProcessSetupResult allowed = rdrs_allow_transparent_huge_pages();
  EXPECT_FALSE(allowed.thp_disabled) << "errno " << allowed.errno_thp;
  EXPECT_EQ(prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0), 0);
  int seen_by_thread = -1;
  std::thread t([&seen_by_thread]() {
    seen_by_thread = prctl(PR_GET_THP_DISABLE, 0, 0, 0, 0);
  });
  t.join();
  EXPECT_EQ(seen_by_thread, 0);
  // Leave the process as the other tests expect it.
  EXPECT_TRUE(rdrs_process_setup().thp_disabled);
#else
  EXPECT_FALSE(rdrs_allow_transparent_huge_pages().thp_disabled);
  GTEST_SKIP() << "transparent huge pages are a Linux facility";
#endif
}

/*
 * The per-thread JSON parse buffer must not be resident until used.
 * std::make_unique<char[]> zero-filled it, which made NumThreads x
 * MaxHttpReqSize resident at startup (64 x 4 MiB with the old defaults);
 * the plain new char[] does not touch the pages.
 */
TEST(ProcessSetup, ParseBufferIsNotResidentUntilUsed) {
#ifdef __linux__
  constexpr unsigned kBufferBytes = 256U * 1024U * 1024U;
  const unsigned saved = globalConfigs.internal.maxHttpReqSize;
  globalConfigs.internal.maxHttpReqSize = kBufferBytes;

  const long before = resident_bytes();
  JSONParser *parser = new JSONParser();
  const long after = resident_bytes();
  delete parser;
  globalConfigs.internal.maxHttpReqSize = saved;

  // Allow for the parser object itself and allocator bookkeeping, but not
  // for the buffer: a zero-filled buffer would add the full 256 MiB.
  EXPECT_LT(after - before, 4L * 1024L * 1024L)
      << "parse buffer pages were touched at allocation";
#else
  GTEST_SKIP() << "resident-set accounting through /proc is Linux-only";
#endif
}
