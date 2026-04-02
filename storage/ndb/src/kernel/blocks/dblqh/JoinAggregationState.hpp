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
#include <kernel/NodeBitmask.hpp>
#include <kernel/ndb_limits.h>
#include <util/rondb_hash.hpp>

#define JAM_FILE_ID 447

class JoinAggInterpreter;

/**
 * LeafProgram
 *
 * Per-leaf aggregation program descriptor for multi-leaf star schema
 * aggregation. Each leaf in a fan-out query tree has its own aggregation
 * program that writes to its own accumulator slots within the shared
 * group rows. Allocated via lc_ndbd_pool_malloc as a dynamic array.
 */
/**
 * LeafProgram
 *
 * Per-leaf aggregation program descriptor for multi-leaf star schema
 * aggregation. Each leaf in a fan-out query tree has its own aggregation
 * program that writes to its own accumulator slots within the shared
 * group rows.
 *
 * m_agg_program points into the shared m_all_programs_buf allocation
 * in JoinAggregationState — it is NOT individually freed.
 */
struct LeafProgram {
  Uint32* m_agg_program;        // Points into m_all_programs_buf (NOT owned)
  Uint32  m_agg_program_len;    // Program length in words
  Uint32  m_acc_offset;         // First accumulator index for this leaf
  Uint32  m_n_agg_results;      // Number of accumulators for this leaf
  Uint32  m_agg_prog_start_pos; // Instruction start offset within program
};

/**
 * JoinAggregationState
 *
 * Holds aggregation state shared across multiple LQHKEYREQ/SCAN_FRAGREQ
 * operations that belong to the same SPJ join aggregation request.
 *
 * For multi-leaf star schema queries, all leaves share a single hash map
 * with combined accumulator layout. Each leaf has its own aggregation
 * program stored in the m_leaf_programs array.
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
 *   - MUTEX_BASED strategy: per-group locking in JoinAggInterpreter
 *   - MUTEX_FREE strategy: per-thread interpreters, no locking during ops
 *
 * Managed by a static ArrayPool in SimulatedBlock. The nextPool field
 * is required by ArrayPool for free-list management.
 *
 * aggStateKey encoding (multi-leaf):
 *   Bits 31..24: leaf index (0..255)
 *   Bits 23..0:  base state key (pool index)
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
    MUTEX_BASED = 0,    // One shared JoinAggInterpreter with per-group mutex
    MUTEX_FREE = 1      // One JoinAggInterpreter per thread, no mutexes
  };

  //------------------------------------------------------------------
  // State Machine
  //------------------------------------------------------------------
  enum State : Uint32 {
    IDLE = 0,
    SETUP_COMPLETE = 1,      // Ready to receive operations
    FINALIZING = 3,          // All ops done, preparing results
    SENDING_RESULTS = 4,     // Sending results to API
    COMPLETED = 5,           // All results sent
    ERROR = 6,
    ABORTING = 7,
    WAITING_SEND_CONF = 8,   // Paused at batch limit, waiting for SEND_CONF
    NODE_FAIL_ABORT = 9,     // DBTC node failed, scans closed, awaiting release
    CTE_REDISTRIBUTING = 10, // Sending groups to hash-owner nodes
    CTE_READY = 11           // Distributed hash table ready for CTE lookups
  };

  //------------------------------------------------------------------
  // Identification (immutable after creation)
  //------------------------------------------------------------------
  Uint32 m_transid[2];           // Transaction ID
  Uint32 m_senderData;           // SPJ request identifier
  Uint32 m_requestId;            // Unique request ID for this aggregation
  BlockReference m_senderRef;    // DBTC block reference
  BlockReference m_apiRef;       // API block reference for results

  //------------------------------------------------------------------
  // Aggregation Programs (immutable after creation)
  //
  // Multi-leaf: m_leaf_programs is a dynamically allocated array of
  // LeafProgram structs, one per aggregate leaf. Each leaf has its own
  // program and accumulator offset. All leaves share the same GROUP BY.
  //
  // Single-leaf: m_num_leaves == 1, m_leaf_programs[0] holds the program.
  //
  // Allocated via lc_ndbd_pool_malloc, freed at release time.
  //------------------------------------------------------------------
  Uint32       m_num_leaves;        // Number of aggregate leaves (1 for single)
  LeafProgram* m_leaf_programs;     // Array of per-leaf descriptors [m_num_leaves]
  Uint32       m_total_agg_results; // Sum of all leaf m_n_agg_results
  Uint32*      m_all_programs_buf;  // Single allocation holding all leaf programs
                                    // LeafProgram::m_agg_program points into this

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
  JoinAggInterpreter* m_agg_interpreter;       // MUTEX_BASED: shared interpreter
  JoinAggInterpreter** m_per_thread_interpreters;
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
  // Memory Budget (immutable after creation)
  // Budget in 32KB pages, determined at setup from global availability.
  //------------------------------------------------------------------
  Uint32 m_memory_budget_pages;

  //------------------------------------------------------------------
  // Result Tracking
  //------------------------------------------------------------------
  Uint32 m_agg_curr_batch_size_rows;       // Aggregated result rows (groups)
  Uint32 m_agg_curr_batch_size_bytes;      // Aggregated result bytes
  Uint32 m_rows_sent;                      // Rows already sent to API
  Uint32 m_max_batch_rows;                 // Max rows per send batch (from COMPLETE_REQ)

  //------------------------------------------------------------------
  // Result Routing (immutable after creation)
  //------------------------------------------------------------------
  Uint32 m_resultRef;            // API reference for results
  Uint32 m_resultData;           // API data reference
  Uint32 m_routeRef;             // Route reference (TC block)

  //------------------------------------------------------------------
  // Receiver IDs for hash-partitioned aggregation results
  // Each group row is routed to receiverIds[hash(key) % numReceiverIds]
  //------------------------------------------------------------------
  Uint32 *m_receiverIds;         // Receiver IDs array (ndbd_malloc'd)
  Uint32 m_numReceiverIds;       // Count of receiver IDs

  Uint32 selectReceiverData(const char *key, Uint32 key_len) const {
    if (m_numReceiverIds <= 1) return m_receiverIds[0];
    Uint64 h = rondb_xxhash_std(key, key_len);
    return m_receiverIds[static_cast<Uint32>(h) % m_numReceiverIds];
  }

  //------------------------------------------------------------------
  // Outer join scan aggregation flag.
  // Match tracking is now done per-ScanRecord in DBLQH (local bitmask
  // attached to SCAN_FRAGCONF on close). No shared state needed.
  //------------------------------------------------------------------
  bool m_outer_join_agg_scan;

  //------------------------------------------------------------------
  // CTE Materialization Mode
  // When m_cte_mode is true, COMPLETE skips the send-and-erase phase.
  // Instead, the hash table stays alive for point lookups via
  // CTE_LOOKUP_REQ, and group rows are redistributed across nodes
  // so each group lives on exactly one node (its hash-partition owner).
  //------------------------------------------------------------------
  bool m_cte_mode;                          // True if this is a CTE materialization
  NdbNodeBitmask m_cte_nodes_finalized;     // Bitmask of nodes that sent FINAL_REP
                                            // (prevents duplicate FINAL from same node)

  // CTE node distribution (set at SETUP, immutable after)
  Uint32 m_cte_node_list[MAX_DATA_NODE_ID]; // Live data node IDs at setup time
  Uint32 m_cte_num_nodes;                   // Number of live data nodes
  bool m_cte_redistribution_done;           // This node finished sending
  Uint32 m_cte_node_fail_count;             // Snapshot of s_node_fail_count at SETUP

  // Global node failure counter — incremented by execNODE_FAILREP.
  // Each CTE state snapshots this at SETUP and checks it hasn't changed
  // before and during redistribution. If changed, the query is aborted.
  static std::atomic<Uint32> s_node_fail_count;

  // Queue for REDISTRIBUTE_ORD groups arriving before local finalization.
  // Stored as a singly-linked list of variable-size entries allocated via
  // lc_ndbd_pool_malloc. Processed after local merge/finalize completes.
  struct RedistQueueEntry {
    RedistQueueEntry *next;
    Uint32 keyLen;      // Key length in bytes
    Uint32 valueLen;    // Accumulator data length in bytes
    Uint32 data[1];     // Variable: [key_data (keyLen bytes)] [value_data (valueLen bytes)]
  };
  RedistQueueEntry *m_redist_queue_head;
  RedistQueueEntry *m_redist_queue_tail;
  Uint32 m_redist_queue_count;

  // Sender info saved from COMPLETE_REQ for sending COMPLETE_CONF after redistribution
  Uint32 m_cte_complete_senderRef;
  Uint32 m_cte_complete_senderData;
  Uint32 m_cte_complete_requestId;

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
    m_num_leaves(0),
    m_leaf_programs(nullptr),
    m_total_agg_results(0),
    m_all_programs_buf(nullptr),
    m_strategy(MUTEX_BASED),
    m_num_threads(0),
    m_agg_interpreter(nullptr),
    m_per_thread_interpreters(nullptr),
    m_outstanding_ops(0),
    m_completed_ops(0),
    m_failed_ops(0),
    m_total_ops_expected(0),
    m_memory_budget_pages(0),
    m_agg_curr_batch_size_rows(0),
    m_agg_curr_batch_size_bytes(0),
    m_rows_sent(0),
    m_max_batch_rows(0),
    m_resultRef(0),
    m_resultData(0),
    m_routeRef(0),
    m_receiverIds(nullptr),
    m_numReceiverIds(0),
    m_outer_join_agg_scan(false),
    m_cte_mode(false),
    m_cte_num_nodes(0),
    m_cte_redistribution_done(false),
    m_cte_node_fail_count(0),
    m_redist_queue_head(nullptr),
    m_redist_queue_tail(nullptr),
    m_redist_queue_count(0),
    m_cte_complete_senderRef(0),
    m_cte_complete_senderData(0),
    m_cte_complete_requestId(0),
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

  //------------------------------------------------------------------
  // aggStateKey encoding: upper 8 bits = leaf index, lower 24 bits = base key.
  // A join can have at most 64 tables, so 256 leaf indexes is sufficient.
  //------------------------------------------------------------------
  static Uint32 encodeAggStateKey(Uint32 baseKey, Uint32 leafIndex) {
    return (leafIndex << 24) | (baseKey & 0x00FFFFFF);
  }
  static Uint32 decodeBaseKey(Uint32 aggStateKey) {
    return aggStateKey & 0x00FFFFFF;
  }
  static Uint32 decodeLeafIndex(Uint32 aggStateKey) {
    return aggStateKey >> 24;
  }
};

#undef JAM_FILE_ID

#endif
