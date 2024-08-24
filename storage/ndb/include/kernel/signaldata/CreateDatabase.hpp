/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

#ifndef CREATE_DATABASE_HPP
#define CREATE_DATABASE_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 550


struct CreateDatabaseReq
{
  static constexpr Uint32 SignalLength = 8;

  enum RequestType {
  };

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 requestInfo;
  Uint32 transId;
  Uint32 transKey;
  Uint32 databaseId;
  Uint32 databaseVersion;
  Uint32 requestType;
  SECTION( DICT_TAB_INFO = 0 );

  friend bool printCREATE_DATABASE_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CreateDatabaseConf {
  static constexpr Uint32 SignalLength = 5;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 transId;
  Uint32 databaseId;
  Uint32 databaseVersion;

  friend bool printCREATE_DATABASE_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CreateDatabaseRef {
  static constexpr Uint32 SignalLength = 9;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 transId;
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorNodeId;
  Uint32 masterNodeId;
  Uint32 errorStatus;
  Uint32 errorKey;

  friend bool printCREATE_DATABASE_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct DatabaseQuotaRep {
  static constexpr Uint32 SignalLength = 5;

  Uint32 databaseId;
  Uint32 requestInfo;
  Uint32 diff_disk_space_usage;
  Uint32 diff_memory_usage;
  Uint32 rate_usage;

  friend bool printDATABASE_QUOTA_REP(File *, const Uint32 *, Uint32, Uint16);
};

struct DatabaseRateOrd {
  static constexpr Uint32 SignalLength = 3;

  Uint32 databaseId;
  Uint32 requestInfo;
  Uint32 delay_us;

  friend bool printDATABASE_RATE_ORD(File *, const Uint32 *, Uint32, Uint16);
};

struct RateOverloadRep {
  static constexpr Uint32 SignalLength = 4;

  Uint32 databaseId;
  Uint32 requestInfo;
  Uint32 current_used_rate_low;
  Uint32 current_used_rate_high;

  friend bool printRATE_OVERLOAD_REP(File *, const Uint32 *, Uint32, Uint16);
};

struct QuotaOverloadRep {
  static constexpr Uint32 SignalLength = 5;

  Uint32 databaseId;
  Uint32 requestInfo;
  Uint32 is_memory_quota_exceeded;
  Uint32 is_disk_quota_exceeded;
  Uint32 continue_delay;

  friend bool printQUOTA_OVERLOAD_REP(File *, const Uint32 *, Uint32, Uint16);
};


#undef JAM_FILE_ID

#endif
