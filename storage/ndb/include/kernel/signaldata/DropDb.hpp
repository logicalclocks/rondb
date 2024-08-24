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

#ifndef DROP_DB_HPP
#define DROP_DB_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 554


struct DropDbReq {
  static constexpr Uint32 SignalLength = 5;

  enum {
    PREPARE_DROP = 0,
    COMMIT_DROP = 1
  };
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestType;
  Uint32 databaseId;
  Uint32 databaseVersion;

  friend bool printDROP_DB_REQ(FILE *, const Uint32 *, Uint32, Uint16);
};

struct DropDbConf {
  static constexpr Uint32 SignalLength = 4;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;
  Uint32 requestType;

  friend bool printDROP_DB_CONF(FILE *, const Uint32 *, Uint32, Uint16);
};

struct DropDbRef {
  static constexpr Uint32 SignalLength = 7;

  enum ErrorCode {
    InvalidTableState = 1
  };
  
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 databaseId;
  Uint32 errorCode;
  Uint32 errorKey;
  Uint32 errorLine;
  Uint32 errorStatus;

  friend bool printDROP_DB_REF(FILE *, const Uint32 *, Uint32, Uint16);
};

#undef JAM_FILE_ID

#endif
