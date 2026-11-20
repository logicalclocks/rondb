/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.

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

#ifndef QUERY_DATABASE_HPP
#define QUERY_DATABASE_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 562

/**
 * requestInfo bits of GetDatabaseReq and ListDatabaseReq. UserKind picks
 * the user objects, USER ALIAS objects included, so the NDB API name cache
 * resolves an alias name with no client change. The alias bits narrow it:
 * the USER commands pass ExcludeAlias, the USER ALIAS commands AliasOnly,
 * and an AliasOnly reply carries the alias's target name in a section.
 */
struct QueryDatabaseFilter {
  static constexpr Uint32 UserKind = 1;
  static constexpr Uint32 AliasOnly = 2;
  static constexpr Uint32 ExcludeAlias = 4;
};

struct GetDatabaseReq {
  static constexpr Uint32 SignalLength = 3;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 requestInfo;

  SECTION( DATABASE_NAME = 0 );

  friend bool printGET_DATABASE_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct GetDatabaseConf {
  static constexpr Uint32 SignalLength = 10;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 databaseId;
  Uint32 databaseVersion;
  Uint32 InMemorySizeMB;
  Uint32 DiskSpaceSizeGB;
  Uint32 RatePerSec;
  Uint32 MaxTransactionSize;
  Uint32 MaxParallelTransactions;
  Uint32 MaxParallelComplexQueries;

  /* Present only in the reply to an AliasOnly request. */
  SECTION( ALIAS_TARGET = 0 );

  friend bool printGET_DATABASE_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct GetDatabaseRef {
  static constexpr Uint32 SignalLength = 5;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorNodeId;

  friend bool printGET_DATABASE_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct ListDatabaseReq {
  static constexpr Uint32 SignalLength = 4;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 requestInfo;
  Uint32 nextDatabaseId;

  friend bool printLIST_DATABASE_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct ListDatabaseConf {
  static constexpr Uint32 SignalLength = 10;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 databaseId;
  Uint32 databaseVersion;
  Uint32 InMemorySizeMB;
  Uint32 DiskSpaceSizeGB;
  Uint32 RatePerSec;
  Uint32 MaxTransactionSize;
  Uint32 MaxParallelTransactions;
  Uint32 MaxParallelComplexQueries;
  SECTION( DATABASE_NAME = 0 );
  /* Present only in the reply to an AliasOnly request. */
  SECTION( ALIAS_TARGET = 1 );

  friend bool printLIST_DATABASE_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct ListDatabaseRef {
  static constexpr Uint32 SignalLength = 5;

  union { Uint32 clientRef, senderRef; };
  union { Uint32 clientData, senderData; };
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorNodeId;

  friend bool printGET_DATABASE_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

/**
 * USER CAPACITY asks every data node, since each sizes its rate pool from
 * its own threads: the pool is numThreads x 1,000,000 rate units, of which
 * RateLimitBudgetPercent may be allocated, and allocatedRate is what USER
 * and DATABASE QUOTA objects hold on the node now. DBDICT always confirms.
 */
struct GetRateCapacityReq {
  static constexpr Uint32 SignalLength = 2;

  Uint32 senderRef;
  Uint32 senderData;
};

struct GetRateCapacityConf {
  static constexpr Uint32 SignalLength = 5;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 numThreads;
  Uint32 rateLimitBudgetPercent;
  Uint32 allocatedRate;
};

#undef JAM_FILE_ID

#endif
