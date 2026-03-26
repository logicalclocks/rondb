/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef STORAGE_NDB_SRC_RONSQL_RONSQLPERF_HPP
#define STORAGE_NDB_SRC_RONSQL_RONSQLPERF_HPP 1

/*
 * RonSQL performance timing.
 *
 * Uncomment the #define below to enable timing of RonSQL preparation
 * and execution phases.  Output goes to stderr (RDRS captures this
 * in its log file).  Available in both debug and production builds.
 *
 * Usage:
 *   PERF_TS(t0);          // capture timestamp
 *   ... work ...
 *   PERF_TS(t1);          // capture another timestamp
 *   PERF_LOG("label", t0, t1);  // log elapsed time in ms
 */

#define RONSQL_PERF_TIMING 1

#ifdef RONSQL_PERF_TIMING

#include <chrono>
#include <cstdint>
#include <cstdio>

static inline uint64_t ronsql_perf_now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

#define PERF_TS(var) uint64_t var = ronsql_perf_now_us()
#define PERF_LOG(msg, t0, t1) do { \
  fprintf(stderr, "RONSQL_PERF %s: %.3f ms\n", \
          msg, (double)((t1) - (t0)) / 1000.0); \
} while (0)

#else

#define PERF_TS(var)          do {} while (0)
#define PERF_LOG(msg, t0, t1) do {} while (0)

#endif  // RONSQL_PERF_TIMING

#endif  // STORAGE_NDB_SRC_RONSQL_RONSQLPERF_HPP
