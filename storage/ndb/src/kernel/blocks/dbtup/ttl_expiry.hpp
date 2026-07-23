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

#ifndef DBTUP_TTL_EXPIRY_HPP
#define DBTUP_TTL_EXPIRY_HPP

/*
 * TTL related
 * The pure arithmetic of the per-row TTL expiry decision, shared between
 * Dbtup::checkTTL() (DbtupExecQuery.cpp) and the differential unit test
 * ttl_expiry-t.cpp, a boundary-biased differential sweep asserting that
 * these functions return the same verdict as the original calendar path
 * (ttl_utc_sec_to_TIME / TIME_from_longlong_datetime_packed ->
 * date_add_interval(INTERVAL_SECOND) -> my_time_compare vs UTC "now").
 * Keep these functions free of kernel dependencies so the test can link
 * them against mysys alone.
 *
 * The integer compare is a bit-exact replacement of that path, not an
 * approximation: MySQL's calendar has no DST, no leap seconds and
 * 86400-second days, and date_add_interval()'s INTERVAL_SECOND branch is
 * itself a linearization:
 *   daynr = calc_daynr(year, month, 1)
 *         + floor((Uint32(day - 1) * 86400 + hms + ttl_sec) / 86400)
 * failing (-> row treated as never expiring) iff daynr exceeds
 * MAX_DAY_NUMBER, while my_time_compare() orders the resulting valid
 * datetimes exactly by that second count. That arithmetic is reproduced
 * below verbatim -- including the unsigned (day - 1) wrap, which is what
 * routes day-of-month 0 (zero date / zero-day date under permissive
 * sql_mode) into the never-expiring overflow branch -- so every storable
 * value keeps the exact pre-optimization verdict. "now" linearizes to
 * now_sec + TTL_EPOCH_DAYNR * 86400 (ttl_utc_sec_to_TIME is the plain
 * inverse of that). Callers use only sign(cmp_ret) ("<= 0" == expired),
 * so the equal case maps to -1.
 */

#include <ndb_global.h>

#include "my_time.h"

/* calc_daynr(1970, 1, 1) == 719528 (days from year 0 to the Unix epoch) */
static constexpr Int64 TTL_EPOCH_DAYNR = 719528;

/* Callers pass epoch seconds beyond 2038 (up to 2^32 - 1) through time_t. */
static_assert(sizeof(time_t) >= 8, "TTL expiry needs a 64-bit time_t");

/*
 * TTL related
 * Lock-free replacement for gmtime_r() on the TTL path.
 *
 * glibc's gmtime_r()/localtime_r() funnel through __tz_convert(), which
 * unconditionally takes the process-global tzset_lock (a private futex) even
 * for the UTC case. This routine converts UTC epoch seconds to a broken-down
 * MYSQL_TIME using only the in-tree calendar arithmetic
 * (get_date_from_daynr), so it touches no glibc lock.
 *
 * It is field-for-field equivalent to MySQL's sec_to_TIME(out, t, 0) -- the
 * lock-free Time_zone_offset / my_tz_OFFSET0 path in sql/tztime.cc -- and is
 * reimplemented here because tztime.cc is part of the mysqld server and is
 * not linked into ndbmtd (get_date_from_daynr lives in mysys/my_time.cc,
 * which is). Since the integer expiry compare took over the hot path, this
 * is only used by ttl_expiry_cmp_datetime2's ttl_sec == 0 cold branch.
 */
static inline void ttl_utc_sec_to_TIME(time_t t, MYSQL_TIME *out) {
  int64_t days = t / 86400;
  int32_t secs = static_cast<int32_t>(t % 86400);
  if (secs < 0) { /* t < 0: normalize into [0, 86400) */
    secs += 86400;
    days -= 1;
  }
  unsigned int year, month, day;
  get_date_from_daynr(days + TTL_EPOCH_DAYNR, &year, &month, &day);
  out->neg = false;
  out->second_part = 0;
  out->year = year;
  out->month = month;
  out->day = day;
  out->hour = secs / 3600;
  out->minute = (secs % 3600) / 60;
  out->second = secs % 60;
  out->time_zone_displacement = 0;
  out->time_type = MYSQL_TIMESTAMP_DATETIME;
}

/*
 * TTL related
 * Expiry verdict for a TIMESTAMP2(0) TTL column: stored_sec is the column's
 * UTC epoch seconds (my_timestamp_from_binary), so this is a pure integer
 * compare. date_add_interval()'s overflow branch is unreachable here: epoch
 * seconds (< 2^32) plus ttl_sec (< 2^33) stay far below the year-9999 limit
 * (~2.5e11 epoch seconds), and with ttl_sec == 0 the direct compare of two
 * valid UTC datetimes is this integer compare.
 */
static inline int ttl_expiry_cmp_timestamp2(Int64 stored_sec, Int64 ttl_sec,
                                            Int64 now_sec) {
  return (stored_sec + ttl_sec <= now_sec) ? -1 : 1;
}

/*
 * TTL related
 * Expiry verdict for a DATETIME2(0) TTL column holding a UTC wall-clock
 * datetime (see the equivalence note at the top of this file). On the
 * date_add_interval() invalid_date outcome (expiry would pass 9999-12-31,
 * incl. the day-of-month 0 wrap) the row never expires; *overflow lets the
 * caller log that case.
 */
static inline int ttl_expiry_cmp_datetime2(const MYSQL_TIME &dt, Int64 ttl_sec,
                                           Int64 now_sec, bool *overflow) {
  *overflow = false;
  if (likely(ttl_sec != 0)) {
    /* Linearize to seconds since year 0 with date_add_interval()'s own
       arithmetic. */
    const Int64 expiry_sec =
        (Int64(calc_daynr(dt.year, dt.month, 1)) +
         Int64(Uint32(dt.day - 1u))) * 86400 +
        Int64(dt.hour) * 3600 + Int64(dt.minute) * 60 + Int64(dt.second) +
        ttl_sec;
    if (unlikely(expiry_sec >= (MAX_DAY_NUMBER + 1) * Int64(86400))) {
      *overflow = true;
      return 1;
    }
    return (expiry_sec <= now_sec + TTL_EPOCH_DAYNR * 86400) ? -1 : 1;
  }
  /*
   * ttl_sec == 0: a TTL=0 table scanned without a purge window. The
   * original path skips date_add_interval() here and my_time_compare()
   * orders the degenerate dates permissive sql_modes can store (zero
   * month/day, ALLOW_INVALID_DATES) lexicographically, which no
   * linearization reproduces -- so keep the broken-down compare
   * verbatim. Cold: every valid row of such a table expires the second
   * it is written.
   */
  MYSQL_TIME curr_dt;
  ttl_utc_sec_to_TIME((time_t)now_sec, &curr_dt);
  return my_time_compare(dt, curr_dt);
}

#endif  // DBTUP_TTL_EXPIRY_HPP
