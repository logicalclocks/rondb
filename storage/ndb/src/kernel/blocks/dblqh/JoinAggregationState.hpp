/*
   Copyright (c) 2025, 2025, Hopsworks and/or its affiliates.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef NDB_DBLQH_JOIN_AGGREGATION_STATE_HPP
#define NDB_DBLQH_JOIN_AGGREGATION_STATE_HPP

#include <atomic>
#include <ndb_types.h>
#include <kernel_types.h>

#define JAM_FILE_ID 447

class AggInterpreter;

/**
 * JoinAggregationState
 *
 * Holds aggregation state shared across multiple LQHKEYREQ/SCAN_FRAGREQ
 * operations that belong to the same SPJ join aggregation request.
 *
 * Lifecycle:
 *   - Created by DblqhProxy on JOIN_AGG_SETUP_REQ (single-threaded)
 *   - Accessed by Dblqh workers during operations (any thread, via pool key)
 *   - Finalized by a Dblqh worker on JOIN_AGG_COMPLETE_REQ (via V_QUERY)
 *   - Released by DblqhProxy on JOIN_AGG_RELEASE_REQ (single-threaded)
 *
 * Thread safety:
 *   - Immutable fields set at creation, no locking needed for reads
 *   - Atomic counters for operation tracking
 *   - MUTEX_BASED strategy: per-group locking in AggInterpreter
 *   - MUTEX_FREE strategy: per-thread interpreters, no locking during ops
 *
 * Managed by a static ArrayPool in SimulatedBlock. The nextPool field
 * is required by ArrayPool for free-list management.
 */
struct JoinAggregationState {
  //------------------------------------------------------------------
  // ArrayPool free-list link
  //------------------------------------------------------------------
  Uint32 nextPool;

  //------------------------------------------------------------------
  // Concurrency Strategy
  //------------------------------------------------------------------
  enum ConcurrencyStrategy : Uint32 {
    MUTEX_BASED = 0,    // One shared AggInterpreter with per-group mutex
    MUTEX_FREE = 1      // One AggInterpreter per thread, no mutexes
  };

  //------------------------------------------------------------------
  // State Machine
  //------------------------------------------------------------------
  enum State : Uint32 {
    IDLE = 0,
    SETUP_COMPLETE = 1,    // Ready to receive operations
    ACCUMULATING = 2,      // Receiving operations, accumulating results
    FINALIZING = 3,        // All ops done, preparing results
    SENDING_RESULTS = 4,   // Sending results to API
    COMPLETED = 5,         // All results sent
    ERROR = 6,
    ABORTING = 7
  };

  //------------------------------------------------------------------
  // Identification (immutable after creation)
  //------------------------------------------------------------------
  Uint32 m_transid[2];           // Transaction ID
  Uint32 m_senderData;           // SPJ request identifier
  Uint32 m_requestId;            // Unique request ID for this aggregation
  BlockReference m_senderRef;    // DBSPJ block reference
  BlockReference m_apiRef;       // API block reference for results

  //------------------------------------------------------------------
  // Aggregation Program (immutable after creation)
  // Allocated via ndbd_malloc, freed at release time.
  //------------------------------------------------------------------
  Uint32* m_agg_program;         // Copy of aggregation program
  Uint32 m_agg_program_len;      // Program length in words

  //------------------------------------------------------------------
  // Concurrency Strategy (immutable after creation)
  //------------------------------------------------------------------
  ConcurrencyStrategy m_strategy;
  Uint32 m_num_threads;          // Number of query threads on this node

  //------------------------------------------------------------------
  // Aggregation State
  //
  // MUTEX_BASED: single shared interpreter with per-group locking
  // MUTEX_FREE:  per-thread interpreters, no mutexes during operations
  //------------------------------------------------------------------
  AggInterpreter* m_agg_interpreter;       // MUTEX_BASED: shared interpreter
  AggInterpreter** m_per_thread_interpreters;
                                           // MUTEX_FREE: one per thread
                                           // Allocated via ndbd_malloc:
                                           //   m_num_threads * sizeof(ptr)

  //------------------------------------------------------------------
  // Operation Tracking (atomic — updated by any LDM thread)
  //------------------------------------------------------------------
  std::atomic<Uint32> m_outstanding_ops;   // Operations still in progress
  std::atomic<Uint32> m_completed_ops;     // Operations completed successfully
  std::atomic<Uint32> m_failed_ops;        // Operations that failed
  Uint32 m_total_ops_expected;             // Total operations (0 = unknown)

  //------------------------------------------------------------------
  // Result Tracking
  //------------------------------------------------------------------
  Uint32 m_agg_curr_batch_size_rows;       // Aggregated result rows (groups)
  Uint32 m_agg_curr_batch_size_bytes;      // Aggregated result bytes
  Uint32 m_rows_sent;                      // Rows already sent to API

  //------------------------------------------------------------------
  // Result Routing (immutable after creation)
  //------------------------------------------------------------------
  Uint32 m_resultRef;            // API reference for results
  Uint32 m_resultData;           // API data reference
  Uint32 m_routeRef;             // Route reference (TC block)

  //------------------------------------------------------------------
  // State Machine (atomic — checked by any thread, set single-threaded)
  //------------------------------------------------------------------
  std::atomic<State> m_state;
  Uint32 m_error_code;           // Error code if m_state == ERROR

  //------------------------------------------------------------------
  // Key-based access — pool index assigned at seize time
  //------------------------------------------------------------------
  Uint32 m_key;

  //------------------------------------------------------------------
  // Timeout Management
  //------------------------------------------------------------------
  Uint32 m_creation_time;
  Uint32 m_last_activity_time;

  JoinAggregationState() :
    nextPool(RNIL),
    m_senderData(RNIL),
    m_requestId(0),
    m_senderRef(0),
    m_apiRef(0),
    m_agg_program(nullptr),
    m_agg_program_len(0),
    m_strategy(MUTEX_BASED),
    m_num_threads(0),
    m_agg_interpreter(nullptr),
    m_per_thread_interpreters(nullptr),
    m_outstanding_ops(0),
    m_completed_ops(0),
    m_failed_ops(0),
    m_total_ops_expected(0),
    m_agg_curr_batch_size_rows(0),
    m_agg_curr_batch_size_bytes(0),
    m_rows_sent(0),
    m_resultRef(0),
    m_resultData(0),
    m_routeRef(0),
    m_state(IDLE),
    m_error_code(0),
    m_key(RNIL),
    m_creation_time(0),
    m_last_activity_time(0)
  {
    m_transid[0] = 0;
    m_transid[1] = 0;
  }

  ~JoinAggregationState() {}
};

#undef JAM_FILE_ID

#endif
