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
 * RonSQL performance timing.  Two mechanisms:
 *
 * 1. RONSQL_PHASE_STATS — per-request phase statistics (STAT_TS / STAT_SET).
 *    Enabled by default; comment out the #define to compile out all phase
 *    capture and the RDRS x-ronsql-phases response header.  Capture is
 *    additionally guarded at runtime on the caller having supplied a
 *    RonSQLPhaseStats sink via RonSQLExecParams::phase_stats (RDRS does;
 *    ronsql_cli and the parse-only authorization path do not), so callers
 *    without a sink pay one null test per phase boundary.
 *
 *    Usage (stats is a RonSQLPhaseStats*, may be NULL):
 *      STAT_TS(stats, s0);
 *      ... work ...
 *      STAT_TS(stats, s1);
 *      STAT_SET(stats, parse_us, s0, s1);
 *      STAT_COUNT(stats, rows_drained, n);
 *
 * 2. RONSQL_PERF_TIMING — compile-time developer tracing (PERF_TS /
 *    PERF_LOG).  Uncomment the #define below to log phase timings to
 *    stderr (RDRS captures this in its log file).  Available in both
 *    debug and production builds.
 *
 *    Usage:
 *      PERF_TS(t0);          // capture timestamp
 *      ... work ...
 *      PERF_TS(t1);          // capture another timestamp
 *      PERF_LOG("label", t0, t1);  // log elapsed time in ms
 */

#define RONSQL_PHASE_STATS 1
//#define RONSQL_PERF_TIMING 1

#if defined(RONSQL_PHASE_STATS) || defined(RONSQL_PERF_TIMING)

#include <chrono>
#include <cstdint>

static inline uint64_t ronsql_perf_now_us() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

#endif

#ifdef RONSQL_PHASE_STATS

#define STAT_TS(stats, var) \
  const uint64_t var = ((stats) != nullptr) ? ronsql_perf_now_us() : 0
#define STAT_SET(stats, field, t0, t1) do { \
  if ((stats) != nullptr) (stats)->field = (t1) - (t0); \
} while (0)
#define STAT_COUNT(stats, field, value) do { \
  if ((stats) != nullptr) (stats)->field = (value); \
} while (0)

#else

#define STAT_TS(stats, var)             do {} while (0)
#define STAT_SET(stats, field, t0, t1)  do {} while (0)
#define STAT_COUNT(stats, field, value) do {} while (0)

#endif  // RONSQL_PHASE_STATS

#ifdef RONSQL_PERF_TIMING

#include <cstdio>

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
