/*
 * Copyright (C) 2022 Hopsworks AB
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

#ifndef DATA_ACCESS_RONDB_SRC_RONDB_LIB_RDRS_DATE_HPP_
#define DATA_ACCESS_RONDB_SRC_RONDB_LIB_RDRS_DATE_HPP_

#include <stdio.h>
#include <string.h>
#include <mysql_time.h>
#include <iostream>
#include <NdbApi.hpp>

typedef unsigned char uchar;
typedef Uint32 uint32;
typedef Int32 int32;
typedef Int64 int64;
typedef Uint64 uint64;
typedef Uint64 ulonglong;
typedef Int64 longlong;

constexpr const std::size_t MAX_DATE_STRING_REP_LENGTH =
    sizeof("YYYY-MM-DD AM HH:MM:SS.FFFFFF+HH:MM");
typedef struct MYSQL_TIME_STATUS {
  int warnings{0};
  unsigned int fractional_digits{0};
  unsigned int nanoseconds{0};
} MYSQL_TIME_STATUS;

using my_time_flags_t = unsigned int;
bool str_to_datetime(const char *str, std::size_t length, MYSQL_TIME *l_time, my_time_flags_t flags,
                     MYSQL_TIME_STATUS *status);

bool str_to_time(const char *str, std::size_t length, MYSQL_TIME *l_time, MYSQL_TIME_STATUS *status,
                 my_time_flags_t flags);

int my_date_to_str(const MYSQL_TIME &my_time, char *to);

int my_TIME_to_str(const MYSQL_TIME &my_time, char *to, uint dec);

longlong TIME_to_longlong_datetime_packed(const MYSQL_TIME &my_time);

void my_date_to_binary(const MYSQL_TIME *ltime, uchar *ptr);

void my_datetime_packed_to_binary(longlong nr, uchar *ptr, uint dec);

void TIME_from_longlong_datetime_packed(MYSQL_TIME *ltime, longlong tmp);

void TIME_from_longlong_time_packed(MYSQL_TIME *ltime, longlong tmp);

longlong my_time_packed_from_binary(const uchar *ptr, uint dec);

longlong my_datetime_packed_from_binary(const uchar *ptr, uint dec);

longlong TIME_to_longlong_time_packed(const MYSQL_TIME &my_time);

void my_time_packed_to_binary(longlong nr, uchar *ptr, uint dec);


// 22.01.X
// struct my_timeval {
// int64_t m_tv_sec;
// int64_t m_tv_usec;
// };
// void my_timestamp_to_binary(const my_timeval *tm, unsigned char *ptr, unsigned int dec);
// void my_timestamp_from_binary(my_timeval *tm, const unsigned char *ptr, unsigned int dec);
//

void my_timestamp_to_binary(const timeval *tm, unsigned char *ptr, unsigned int dec);
void my_timestamp_from_binary(timeval *tm, const unsigned char *ptr, unsigned int dec);

static inline uint32 uint3korr(const uchar *A) {
  return static_cast<uint32>((static_cast<uint32>(A[0])) + ((static_cast<uint32>(A[1])) << 8) +
                             ((static_cast<uint32>(A[2])) << 16));
}

inline void my_unpack_date(MYSQL_TIME *l_time, const void *d) {
  uchar b[4];
  memcpy(b, d, 3);
  b[3]        = 0;
  uint w      = (uint)uint3korr(b);
  l_time->day = (w & 31);
  w >>= 5;
  l_time->month = (w & 15);
  w >>= 4;
  l_time->year      = w;
  l_time->time_type = MYSQL_TIMESTAMP_DATE;
}
#endif  // DATA_ACCESS_RONDB_SRC_RONDB_LIB_RDRS_DATE_HPP_
