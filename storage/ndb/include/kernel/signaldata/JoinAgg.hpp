/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef JOIN_AGG_HPP
#define JOIN_AGG_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 564

struct JoinAggSetupReq {
  static constexpr Uint32 SignalLength = 11;
  static constexpr Uint32 AggProgramSectionNum = 0;
  static constexpr Uint32 ReceiverIdsSectionNum = 1;
  static constexpr Uint32 STRATEGY_MUTEX_BASED = 0;
  static constexpr Uint32 STRATEGY_MUTEX_FREE = 1;

  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 transid[2];
  Uint32 tableId;
  Uint32 expectedOpCount;
  Uint32 concurrencyStrategy;
  Uint32 resultRef;
  Uint32 resultData;
  Uint32 routeRef;
  // Long section 0: Aggregation program
  // Long section 1: Receiver IDs for hash-partitioned aggregation results
};

struct JoinAggSetupConf {
  static constexpr Uint32 SignalLength = 4;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 aggStateKey;     // Pool index for O(1) lookup
};

struct JoinAggSetupRef {
  static constexpr Uint32 SignalLength = 5;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 errorCode;
  Uint32 errorLine;
};

struct JoinAggCompleteReq {
  static constexpr Uint32 SignalLength = 7;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 transid[2];
  Uint32 aggStateKey;
  Uint32 maxBatchRows;
};

struct JoinAggCompleteConf {
  static constexpr Uint32 SignalLength = 5;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 numResultRows;
  Uint32 resultBytes;
};

struct JoinAggCompleteRef {
  static constexpr Uint32 SignalLength = 5;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 errorCode;
  Uint32 errorLine;
};

struct JoinAggReleaseReq {
  static constexpr Uint32 SignalLength = 7;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 transid[2];
  Uint32 aggStateKey;
  Uint32 noReply;  // If set, DBLQH will not send RELEASE_CONF
};

struct JoinAggReleaseConf {
  static constexpr Uint32 SignalLength = 3;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
};

// DBLQH → DBSPJ: "I've sent a batch of group rows, pausing."
struct JoinAggSendReq {
  static constexpr Uint32 SignalLength = 6;
  Uint32 senderRef;       // DBLQH worker block ref (DBSPJ needs this to reply)
  Uint32 senderData;
  Uint32 requestId;
  Uint32 aggStateKey;
  Uint32 numRowsSent;     // cumulative rows sent so far
  Uint32 resultBytes;     // cumulative bytes sent so far
};

// DBSPJ → DBLQH: "API is ready, continue sending group rows."
struct JoinAggSendConf {
  static constexpr Uint32 SignalLength = 5;
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 requestId;
  Uint32 aggStateKey;
  Uint32 maxBatchRows;    // next batch limit
};

// DBDIH → DblqhProxy: "All blocks completed node failure handling for this
// node. Release all join aggregation states in NODE_FAIL_ABORT state owned
// by the failed node."
struct JoinAggNodeFailRep {
  static constexpr Uint32 SignalLength = 1;
  Uint32 failedNodeId;
};

// DBSPJ → DblqhProxy: inject a null-extended row for outer join aggregation
// when DBSPJ skips LQHKEYREQ because the key is NULL.
struct JoinAggNullRowReq {
  static constexpr Uint32 SignalLength = 6;
  Uint32 senderRef;
  Uint32 aggStateKey;
  Uint32 transId[2];
  Uint32 requestPtrI;   // DBSPJ request pointer (for routing CONF back)
  Uint32 treeNodePtrI;  // DBSPJ tree node pointer
  // Long section 0: linked_attr_data (parent column values with table metadata)
};

// DblqhProxy → DBSPJ: null-extended row has been processed
struct JoinAggNullRowConf {
  static constexpr Uint32 SignalLength = 4;
  Uint32 senderRef;
  Uint32 aggStateKey;
  Uint32 requestPtrI;
  Uint32 treeNodePtrI;
};

// DBSPJ → DBLQH instance 1: request match bitmask for outer join null row injection
struct JoinAggMatchReq {
  static constexpr Uint32 SignalLength = 6;
  Uint32 senderRef;
  Uint32 aggStateKey;
  Uint32 transId[2];
  Uint32 requestPtrI;
  Uint32 treeNodePtrI;
};

// DBLQH instance 1 → DBSPJ: match bitmask response
// Long section 0: matched_ranges bitmask (ceil(numRanges/32) words)
struct JoinAggMatchConf {
  static constexpr Uint32 SignalLength = 5;
  Uint32 senderRef;
  Uint32 aggStateKey;
  Uint32 requestPtrI;
  Uint32 treeNodePtrI;
  Uint32 numBitmaskWords;
};

#undef JAM_FILE_ID

#endif
