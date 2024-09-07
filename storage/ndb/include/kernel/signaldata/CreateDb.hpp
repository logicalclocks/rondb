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

#ifndef CREATE_DB_HPP
#define CREATE_DB_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 553


struct CreateDbReq
{
  static constexpr Uint32 SignalLengthTC = 11;
  static constexpr Uint32 SignalLengthLQH = 8;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;
  Uint32 databaseVersion;
  Uint32 requestType;
  Uint32 inMemorySizeMB;
  Uint32 diskSpaceSizeGB;
  Uint32 ratePerSec;
  Uint32 maxTransactionSize;
  Uint32 maxParallelTransactions;
  Uint32 maxParallelComplexQueries;

  friend bool printCREATE_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CreateDbConf {
  static constexpr Uint32 SignalLength = 3;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;

  friend bool printCREATE_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CreateDbRef {
  static constexpr Uint32 SignalLength = 7;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorKey;
  Uint32 errorStatus;

  friend bool printCREATE_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct ConnectTableDbReq {
  static constexpr Uint32 SignalLength = 5;

  enum {
    CREATE_DB = 0,
    CONNECT_GLOBAL_INSTANCE = 1,
    CREATE_TABLE = 2,
    RENAME_TABLE = 3
  };
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 databaseId;
  Uint32 tableId;

  friend bool printCONNECT_TABLE_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct ConnectTableDbConf {
  static constexpr Uint32 SignalLength = 5;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 databaseId;
  Uint32 tableId;

  friend bool printCONNECT_TABLE_DB_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct ConnectTableDbRef {
  static constexpr Uint32 SignalLength = 9;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;
  Uint32 tableId;
  Uint32 requestType;
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorKey;
  Uint32 errorStatus;

  friend bool printCONNECT_TABLE_DB_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct DisconnectTableDbReq {
  static constexpr Uint32 SignalLength = 5;

  enum {
    DROP_DB = 0,
    RENAME_TABLE = 1
  };
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 databaseId;
  Uint32 tableId;

  friend bool printDISCONNECT_TABLE_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct DisconnectTableDbConf {
  static constexpr Uint32 SignalLength = 5;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 databaseId;
  Uint32 tableId;

  friend bool printDISCONNECT_TABLE_DB_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct DisconnectTableDbRef {
  static constexpr Uint32 SignalLength = 9;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 databaseId;
  Uint32 tableId;
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorKey;
  Uint32 errorStatus;

  friend bool printDISCONNECT_TABLE_DB_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CommitDbReq {
  static constexpr Uint32 SignalLength = 3;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;

  friend bool printCOMMIT_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CommitDbConf {
  static constexpr Uint32 SignalLength = 3;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;

  friend bool printCOMMIT_DB_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct CommitDbRef {
  static constexpr Uint32 SignalLength = 7;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;
  Uint32 errorCode;
  Uint32 errorLine; 
  Uint32 errorKey;
  Uint32 errorStatus;

  friend bool printCOMMIT_DB_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

#undef JAM_FILE_ID

#endif
