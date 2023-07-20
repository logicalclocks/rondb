/*
<<<<<<<< HEAD:storage/ndb/include/kernel/signaldata/GetCpuUsage.hpp
   Copyright (c) 2023, 2023, Hopsworks and/or its affiliates.
========
   Copyright (c) 2022, 2023, Oracle and/or its affiliates.
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:storage/ndb/include/util/ndb_openssl3_compat.h

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

<<<<<<<< HEAD:storage/ndb/include/kernel/signaldata/GetCpuUsage.hpp
#ifndef GET_CPU_USAGE_HPP
#define GET_CPU_USAGE_HPP

#define JAM_FILE_ID 49


/**
 * Get CPU measurement
 */
class GetCpuUsageReq {
  
  /**
   * Receiver(s)
   */
  friend class Thrman;
  
  /**
   * Sender
   */
  friend class Backup;
  friend class Dblqh;

public:
  static constexpr Uint32 SignalLength = 1;
public:
  enum RequestType {
    PerSecond = 0,
    LastMeasure = 1
  };
  Uint32 requestType;
};

class GetCpuUsageConf {

  /**
   * Sender(s)
   */
  friend class Thrman;
  
  /**
   * Receiver(s)
   */
  friend class Backup;
  friend class Dblqh;

public:
  static constexpr Uint32 SignalLength = 4;
  
public:
  Uint32 real_exec_time;
  Uint32 os_exec_time;
  Uint32 exec_time;
  Uint32 block_exec_time;
};
#undef JAM_FILE_ID

#endif
========

/* Enable NDB code to use OpenSSL 3 APIs unconditionally
   with any OpenSSL version starting from 1.0.2
*/

#ifndef NDB_PORTLIB_OPENSSL_COMPAT_H
#define NDB_PORTLIB_OPENSSL_COMPAT_H
#include <openssl/ssl.h>


#if OPENSSL_VERSION_NUMBER < 0x30000000L && OPENSSL_VERSION_NUMBER > 0x10002000L

EVP_PKEY * EVP_RSA_gen(unsigned int bits);
int EVP_PKEY_eq(const EVP_PKEY *a, const EVP_PKEY *b);
EVP_PKEY * EVP_EC_generate(const char * curve);

#else

#define EVP_EC_generate(curve) EVP_PKEY_Q_keygen(nullptr,nullptr,"EC",curve)

#endif  /* OPENSSL_VERSION_NUMBER */

#endif  /* NDB_PORTLIB_OPENSSL_COMPAT_H */
>>>>>>>> 057f5c9509c6c9ea3ce3acdc619f3353c09e6ec6:storage/ndb/include/util/ndb_openssl3_compat.h
