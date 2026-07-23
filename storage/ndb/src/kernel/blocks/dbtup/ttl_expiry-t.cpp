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
 * Differential oracle for the TTL expiry decision (ttl_expiry.hpp):
 * optimized integer compare vs the original calendar path.
 *
 * Links the REAL mysys/my_time.cc, so date_add_interval / my_time_compare /
 * calc_daynr / get_date_from_daynr are the exact production functions.
 *
 * OLD = verbatim copy of the pre-optimization Dbtup::checkTTL decision
 *       (ttl_utc_sec_to_TIME + date_add_interval + my_time_compare),
 *       kept here as the specification. It uses its own private copy of
 *       the UTC conversion (oracle_utc_sec_to_TIME) so a future edit to
 *       the production ttl_utc_sec_to_TIME cannot hide from this test.
 * NEW = the production functions from ttl_expiry.hpp, the ones
 *       Dbtup::checkTTL executes.
 *
 * Compared per case: expired == (cmp_ret <= 0) (the only thing callers
 * use), and for DATETIME2 also whether the overflow/never-expires verdict
 * matches date_add_interval()'s failure. A mismatch means either the
 * optimized formula or MySQL's calendar implementation changed semantics
 * -- both must be reconciled, not papered over.
 */

#include <util/NdbTap.hpp>

#include <cstdio>
#include <cstring>
#include <cstdlib>

#include "ttl_expiry.hpp"

/* ---- Oracle's own UTC conversion: a frozen copy of the production
   ttl_utc_sec_to_TIME, so this test also catches edits to that helper ---- */
static void oracle_utc_sec_to_TIME(time_t t, MYSQL_TIME *out) {
  const Int64 EPOCH_DAYNR = 719528;
  int64_t days = t / 86400;
  int32_t secs = static_cast<int32_t>(t % 86400);
  if (secs < 0) {
    secs += 86400;
    days -= 1;
  }
  unsigned int year, month, day;
  get_date_from_daynr(days + EPOCH_DAYNR, &year, &month, &day);
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

/* ---- OLD decision paths (baseline checkTTL, decode onwards) ---- */
static int old_check_common(MYSQL_TIME dt, Uint64 ttl_sec, Int64 now_sec,
                            bool *add_failed) {
  *add_failed = false;
  if (ttl_sec != 0) {
    Interval interval;
    memset(&interval, 0, sizeof(interval));
    interval.second = ttl_sec;
    if (date_add_interval(&dt, INTERVAL_SECOND, interval, nullptr))
      *add_failed = true;
  }
  if (*add_failed) return 1;
  MYSQL_TIME curr_dt;
  oracle_utc_sec_to_TIME((time_t)now_sec, &curr_dt);
  return my_time_compare(dt, curr_dt);
}
static int old_check_ts2(Uint32 row_sec, Uint64 ttl_sec, Int64 now_sec,
                         bool *add_failed) {
  MYSQL_TIME dt;
  oracle_utc_sec_to_TIME((time_t)row_sec, &dt);
  return old_check_common(dt, ttl_sec, now_sec, add_failed);
}
static int old_check_dt2(const MYSQL_TIME &dt, Uint64 ttl_sec,
                         Int64 now_sec, bool *add_failed) {
  return old_check_common(dt, ttl_sec, now_sec, add_failed);
}

static long long n_checked = 0, n_mismatch = 0;

/* "now" domain: [0, 2^32).  The kernel caches "now" as Uint32 (max = year
   2106) and the per-op fallback is the real machine clock; a clock past year
   9999 (2.5e11) makes the OLD path itself decode "now" as a zero date
   (get_date_from_daynr wrong-daynr fallback) and is physically unreachable. */
static void check_dt2(const MYSQL_TIME &dt, Uint64 ttl, Int64 now) {
  if (now < 0 || now > 4294967295ll) return;
  bool add_failed = false;
  int o = old_check_dt2(dt, ttl, now, &add_failed);
  bool overflow = false;
  int n = ttl_expiry_cmp_datetime2(dt, (Int64)ttl, now, &overflow);
  n_checked++;
  if ((o <= 0) != (n <= 0) || overflow != add_failed) {
    if (n_mismatch++ < 20)
      printf("DT2 MISMATCH %04u-%02u-%02u %02u:%02u:%02u ttl=%llu now=%lld "
             "old=%d new=%d add_failed=%d overflow=%d\n",
             dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
             (unsigned long long)ttl, (long long)now, o, n,
             (int)add_failed, (int)overflow);
  }
}
static void check_ts2(Uint32 row, Uint64 ttl, Int64 now) {
  if (now < 0 || now > 4294967295ll) return;
  bool add_failed = false;
  int o = old_check_ts2(row, ttl, now, &add_failed);
  int n = ttl_expiry_cmp_timestamp2((Int64)row, (Int64)ttl, now);
  n_checked++;
  /* add_failed must stay false: TIMESTAMP2's optimized path has no overflow
     branch on the grounds that date_add_interval can't fail for it. */
  if ((o <= 0) != (n <= 0) || add_failed) {
    if (n_mismatch++ < 20)
      printf("TS2 MISMATCH row=%u ttl=%llu now=%lld old=%d new=%d "
             "add_failed=%d\n", row,
             (unsigned long long)ttl, (long long)now, o, n, (int)add_failed);
  }
}

static MYSQL_TIME mk(unsigned y, unsigned m, unsigned d, unsigned h,
                     unsigned mi, unsigned s) {
  MYSQL_TIME t;
  memset(&t, 0, sizeof(t));
  t.year = y;
  t.month = m;
  t.day = d;
  t.hour = h;
  t.minute = mi;
  t.second = s;
  t.neg = false;
  t.second_part = 0;
  t.time_type = MYSQL_TIMESTAMP_DATETIME;
  return t;
}

static Int64 dt_linear(const MYSQL_TIME &dt) {
  return (Int64(calc_daynr(dt.year, dt.month, 1)) + Int64(dt.day) - 1) *
             86400 +
         Int64(dt.hour) * 3600 + Int64(dt.minute) * 60 + Int64(dt.second);
}

TAPTEST(TtlExpiry) {
  /* ttl values: 0, tiny, sub-day, day, ~68y, Uint32 max-ish, RNIL-1 + max
     window (the biggest sum the kernel can form) */
  const Uint64 ttls[] = {0,          1,          3599,        86399,
                         86400,      2147483647, 4294967294u, 4294967295u,
                         8589934588ull};
  /* "now" absolute samples: epoch 0, 2001, 2026, 2038 wrap, 2106 (uint32 max) */
  const Int64 nows[] = {0, 1000000000, 1782000000, 2147483648ll, 4294967295ll};

  /* --- TIMESTAMP2 grid --- */
  const Uint32 rows[] = {0u, 1u, 999999999u, 2147483647u, 2147483648u,
                         4294967294u, 4294967295u};
  for (Uint32 row : rows)
    for (Uint64 ttl : ttls) {
      for (Int64 now : nows) check_ts2(row, ttl, now);
      /* now around the exact expiry boundary */
      Int64 e = (Int64)row + (Int64)ttl;
      for (Int64 d = -2; d <= 2; d++) check_ts2(row, ttl, e + d);
    }

  /* --- DATETIME2 edge dates (incl. degenerate zero-date / zero-in-date) --- */
  MYSQL_TIME dts[] = {
      mk(0, 0, 0, 0, 0, 0),        /* zero date */
      mk(0, 0, 0, 12, 30, 30),     /* zero date + time */
      mk(2009, 0, 0, 0, 0, 0),     /* zero month+day */
      mk(2009, 0, 15, 10, 0, 0),   /* zero month */
      mk(2009, 1, 0, 10, 0, 0),    /* zero day */
      mk(0, 1, 1, 0, 0, 0),        /* year 0 */
      mk(1, 1, 1, 0, 0, 0),        mk(999, 12, 31, 23, 59, 59),
      mk(1000, 1, 1, 0, 0, 0),     mk(1969, 12, 31, 23, 59, 59),
      mk(1970, 1, 1, 0, 0, 0),     mk(2024, 2, 29, 12, 0, 0), /* leap day */
      mk(2023, 2, 28, 23, 59, 59), mk(2026, 7, 7, 12, 0, 0),
      mk(2038, 1, 19, 3, 14, 8),   mk(2106, 2, 7, 6, 28, 15),
      mk(9998, 12, 31, 23, 59, 59), mk(9999, 12, 31, 23, 59, 59),
      mk(9999, 12, 31, 0, 0, 0),
  };
  for (const MYSQL_TIME &dt : dts)
    for (Uint64 ttl : ttls) {
      for (Int64 now : nows) check_dt2(dt, ttl, now);
      /* now around the exact linearized expiry boundary (map back to epoch) */
      Int64 e_epoch = dt_linear(dt) + (Int64)ttl - TTL_EPOCH_DAYNR * 86400;
      for (Int64 d = -2; d <= 2; d++) check_dt2(dt, ttl, e_epoch + d);
    }

  /* --- randomized sweep: all decodable field combos, boundary-biased --- */
  srand(20260707);
  for (int i = 0; i < 5000000; i++) {
    MYSQL_TIME dt = mk(rand() % 10000, rand() % 13, rand() % 32, rand() % 24,
                       rand() % 60, rand() % 60);
    Uint64 ttl = ttls[rand() % 9];
    if (rand() % 4 == 0) ttl = (Uint64)rand() % 200000;
    Int64 now;
    switch (rand() % 3) {
      case 0: /* near this row's expiry */
        now = dt_linear(dt) + (Int64)ttl - TTL_EPOCH_DAYNR * 86400 +
              (rand() % 7 - 3);
        break;
      case 1: /* realistic clock */
        now = 1500000000 + rand() % 600000000;
        break;
      default: /* anywhere a 32-bit clock can be */
        now = ((Int64)((Uint32)rand() * 2654435761u)) & 0xffffffffll;
        break;
    }
    check_dt2(dt, ttl, now);
    /* TS2 too */
    Uint32 row = (Uint32)rand() * 2654435761u;
    Int64 e = (Int64)row + (Int64)ttl;
    check_ts2(row, ttl, (rand() % 2) ? e + (rand() % 7 - 3) : now);
  }

  printf("checked=%lld mismatches=%lld -> %s\n", n_checked, n_mismatch,
         n_mismatch == 0 ? "BIT-EXACT" : "DIVERGENT");
  fflush(stdout); /* keep the diagnostics if OK() aborts */
  OK(n_mismatch == 0);
  return 1;
}
