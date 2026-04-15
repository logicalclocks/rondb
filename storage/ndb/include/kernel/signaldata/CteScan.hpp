/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

#ifndef NDB_CTE_SCAN_HPP
#define NDB_CTE_SCAN_HPP

#include <ndb_types.h>

/**
 * CTE_SCAN_COMPLETE_REP — DBSPJ → DBTC
 *
 * Sent when all CTE materialization scans on this DBSPJ instance have
 * completed.  DBTC tracks these from all DBSPJ instances; when all
 * report, DBTC proceeds to the JOIN_AGG_COMPLETE phase (redistribution).
 */
struct CteScanCompleteRep {
  Uint32 senderRef;     // DBSPJ block reference
  Uint32 senderData;    // scanPtr.i in DBTC (echoed from SCAN_FRAGREQ)

  static constexpr Uint32 SignalLength = 2;
};

/**
 * CTE_START_MAIN_REQ — DBTC → DBSPJ
 *
 * Sent after all CTE hash tables are READY (redistribution complete).
 * Tells DBSPJ to transition CTEs to CTE_READY and start the main
 * SELECT query tree.
 *
 * senderData + transId are the same values that DBTC used in the
 * original SCAN_FRAGREQ, so DBSPJ can look up the Request via its
 * scan request hash.
 */
struct CteStartMainReq {
  Uint32 senderRef;     // DBTC block reference
  Uint32 senderData;    // scanPtr.i in DBTC (echoed back as DBSPJ m_senderData)
  Uint32 transId1;
  Uint32 transId2;

  static constexpr Uint32 SignalLength = 4;
};

/**
 * CTE_PHASE_COMPLETE_REP — DBSPJ → DBTC
 *
 * Sent when all CTE scans for a specific execution phase have completed
 * on this DBSPJ instance.  DBTC tracks these per phase; when all
 * instances report for a phase, DBTC redistributes that phase's CTEs
 * and either advances to the next phase or starts the main query.
 */
struct CtePhaseCompleteRep {
  Uint32 senderRef;     // DBSPJ block reference
  Uint32 senderData;    // ScanFragRec.i in DBTC (echoed from SCAN_FRAGREQ)
  Uint32 phase;         // Which CTE phase completed

  static constexpr Uint32 SignalLength = 3;
};

/**
 * CTE_PHASE_START_REQ — DBTC → DBSPJ
 *
 * Sent after a CTE phase's hash tables are redistributed and READY.
 * Tells DBSPJ to transition that phase's CTEs to CTE_READY and start
 * the next phase's CTE scans.
 */
struct CtePhaseStartReq {
  Uint32 senderRef;     // DBTC block reference
  Uint32 senderData;    // ScanFragRec.i (for DBSPJ hash lookup)
  Uint32 transId1;
  Uint32 transId2;
  Uint32 phase;         // Which CTE phase to start

  static constexpr Uint32 SignalLength = 5;
};

/**
 * CTE_SCAN_REQ — DBSPJ → DBLQH
 *
 * Scan groups from a materialized CTE hash table.  On the first call,
 * DBLQH initializes iteration state in JoinAggregationState and sends
 * up to batchSize groups as TRANSID_AI (AttributeHeader-encoded GROUP BY
 * keys + aggregate results + CORR_FACTOR), followed by CTE_SCAN_CONF.
 *
 * On subsequent calls (when m_cteScan_groupsSent > 0 in the state),
 * DBLQH resumes from the saved position and sends the next batch.
 *
 * Used by QN_CTE_SCAN nodes when a CTE reads from an earlier CTE.
 *
 * Long section 0: optional AttrInfo with 5-word header + interpreted
 * program whose final-read section carries the user projection and an
 * encoded FLUSH_AI [resultRef, resultData, routeRef].  When present,
 * DBLQH walks the final-read section per group and routes TRANSID_AI
 * according to FLUSH_AI (API delivery) instead of back to DBSPJ.
 * When absent, DBLQH emits the raw key + aggregates + CORR_FACTOR32
 * tuple to senderRef (legacy DBSPJ-internal feed used by nested CTEs).
 */
struct CteScanReq {
  Uint32 senderRef;       // DBSPJ block reference
  Uint32 senderData;      // TreeNode pointer (echoed in TRANSID_AI connectPtr)
  Uint32 aggStateKey;     // CTE hash table to scan (JoinAggregationState key)
  Uint32 transId1;
  Uint32 transId2;
  Uint32 batchSize;       // Max groups to send in this batch
  Uint32 resultRef;       // FLUSH_AI target: API block reference (per-fragment)
  Uint32 resultData;      // FLUSH_AI connect ptr / CORR_FACTOR64 root rcvr id
                          // — set from requestPtr.m_rootResultData so each
                          // fragment's worker gets a CORR_FACTOR64 that
                          // routes to the right NdbWorker on the API side.
                          // Without this, parseDA's FLUSH_AI carries the
                          // common (worker[0]) receiverId from the API's
                          // getIdOfReceiver(), and all fragments would
                          // route their rows to the same worker.

  static constexpr Uint32 SignalLength = 8;
  enum { AttrInfoSectionNum = 0 };
};

struct CteScanConf {
  Uint32 senderRef;       // DBLQH block reference
  Uint32 senderData;      // TreeNode pointer (echoed from REQ)
  Uint32 numRows;         // Number of groups sent as TRANSID_AI in this batch
  Uint32 flags;           // Flags (EndOfData)

  static constexpr Uint32 SignalLength = 4;
  enum { EndOfData = 0x1 };
};

struct CteScanRef {
  Uint32 senderRef;
  Uint32 senderData;
  Uint32 errorCode;

  static constexpr Uint32 SignalLength = 3;
};

#endif  // NDB_CTE_SCAN_HPP
