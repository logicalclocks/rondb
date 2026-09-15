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

/*
 * JoinAggTestUtil.hpp - helpers shared by the SignalSender-driven
 * join-aggregation block tests (testCteProtocol, testCteDbtc):
 *
 *   - error-insert setting with checked management results and a
 *     clearing guard;
 *   - a management event listener for the InfoEvent markers the kernel
 *     test hooks emit;
 *   - positive leak verification: DUMP 2361 / 2362 / 2363 (DBLQH),
 *     2560 (DBTC) and 2650 (DBSPJ) accept a test cookie as a second word
 *     and answer [JOIN_AGG_LEAK_CHECK_OK node=N dump=D cookie=C] from
 *     instance 1 after a clean check; each crashes its node on a leak.
 *     Node connection counters are compared before and after so an
 *     automatic restart cannot hide such a crash.
 *
 * Header-only; every function is inline so a test may use a subset.
 */

#ifndef NDB_BLOCK_UNIT_TEST_JOIN_AGG_TEST_UTIL_HPP
#define NDB_BLOCK_UNIT_TEST_JOIN_AGG_TEST_UTIL_HPP

#include <ndb_global.h>
#include <NdbSleep.h>
#include <NdbTick.h>
#include <InputStream.hpp>
#include <mgmapi_debug.h>
#include "mgmapi_internal.h"
#include "../src/ndbapi/SignalSender.hpp"
#include <kernel/signaldata/DumpStateOrd.hpp>
#include <NdbRestarter.hpp>

#include <cstdio>
#include <cstring>
#include <map>

/* Keep a client available to transporter broadcasts while another
 * client is polled or the test waits on a management event. */
struct ScopedSenderUnlock {
  SignalSender &sender;
  explicit ScopedSenderUnlock(SignalSender &s) : sender(s) {
    sender.unlock();
  }
  ~ScopedSenderUnlock() { sender.lock(); }
};

/* NdbRestarter's insert helpers can return success after a management
 * error. Check the API result and the server's reply explicitly. */
static inline bool
setErrorInsert(NdbRestarter &restarter, Uint32 node, int error, int extra)
{
  ndb_mgm_reply reply = {};
  if (restarter.handle == nullptr ||
      ndb_mgm_insert_error2(restarter.handle, node, error, extra,
                            &reply) == -1 ||
      reply.return_code != 0) {
    fprintf(stderr, "Setting error insert %d on node %u failed\n",
            error, node);
    return false;
  }
  return true;
}

struct ErrorInsertGuard {
  NdbRestarter &restarter;
  Uint32 node;
  bool active;

  bool clear() {
    if (!active) return true;
    if (!setErrorInsert(restarter, node, 0, 0)) return false;
    active = false;
    return true;
  }
  ~ErrorInsertGuard() { (void)clear(); }
};

/* Subscribe before the action that emits the marker so it cannot be
 * missed. Unrelated events do not extend the timeout. */
struct ProtocolEventListener {
  NdbSocket socket;
  Uint32 timeoutMs = 30000;
  ~ProtocolEventListener() {
    if (socket.is_valid()) socket.close();
  }
  bool open(NdbRestarter &restarter) {
    if (restarter.handle == nullptr) return false;
    int filter[] = {2, NDB_MGM_EVENT_CATEGORY_INFO, 0};
    socket = ndb_mgm_listen_event_internal(restarter.handle, filter, 0, true);
    return socket.is_valid();
  }
  bool waitFor(const char *marker) {
    SocketInputStream input(socket, 100);
    const Uint64 start = NdbTick_CurrentMillisecond();
    char line[1024] = {};
    while (NdbTick_CurrentMillisecond() - start < timeoutMs) {
      input.reset_timeout();
      if (input.gets(line, sizeof(line)) == nullptr) {
        fprintf(stderr, "Failed to read management events\n");
        return false;
      }
      if (strstr(line, marker) != nullptr) return true;
    }
    fprintf(stderr, "TIMEOUT waiting for %s\n", marker);
    return false;
  }
};

/* Snapshot every data node, requiring STARTED without waiting for a
 * failed node to restart. The management connection counter changes
 * on disconnect/reconnect even if STARTED is observed again. */
static inline bool
readStartedNodeConnections(NdbRestarter &restarter,
                           std::map<int, int> &connections,
                           const char *label)
{
  connections.clear();
  const ndb_mgm_node_type types[] = {
      NDB_MGM_NODE_TYPE_NDB, NDB_MGM_NODE_TYPE_UNKNOWN};
  ndb_mgm_cluster_state *state =
      restarter.handle != nullptr
          ? ndb_mgm_get_status2(restarter.handle, types) : nullptr;
  if (state == nullptr) {
    fprintf(stderr, "%s: cannot read data-node status\n", label);
    return false;
  }
  bool ok = true;
  for (int i = 0; i < state->no_of_nodes; i++) {
    const ndb_mgm_node_state &node = state->node_states[i];
    if (node.node_status != NDB_MGM_NODE_STATUS_STARTED ||
        node.connect_count < 0) {
      fprintf(stderr, "%s: node %d is not ready for leak verification "
                      "(status=%d connect_count=%d)\n",
              label, node.node_id, (int)node.node_status, node.connect_count);
      ok = false;
      break;
    }
    connections[node.node_id] = node.connect_count;
  }
  free(state);
  if (connections.empty()) {
    fprintf(stderr, "%s: no started data nodes found\n", label);
    return false;
  }
  return ok;
}

/* The DBLQH leak checks, and all five join-aggregation leak checks. */
inline constexpr int JOIN_AGG_LQH_LEAK_DUMPS[] = {
    DumpStateOrd::LqhDumpJoinAggStates,
    DumpStateOrd::LqhDumpCteIterStates,
    DumpStateOrd::LqhDumpJoinAggIdentity};
inline constexpr int JOIN_AGG_ALL_LEAK_DUMPS[] = {
    DumpStateOrd::LqhDumpJoinAggStates,
    DumpStateOrd::LqhDumpCteIterStates,
    DumpStateOrd::LqhDumpJoinAggIdentity,
    DumpStateOrd::TcDumpJoinAggRecords,
    DumpStateOrd::SpjDumpRequests};

/* Require positive completion of every listed DUMP on every node.
 * A management success only confirms that the dump was sent. settleMs
 * gives CONTINUEB teardown chains time to drain first. */
static inline int
joinAggCheckLeaks(SignalSender &ss, NdbRestarter &restarter,
                  const char *label, const int *codes, unsigned numCodes,
                  Uint32 settleMs, Uint32 timeoutMs)
{
  /* Management waits must not block this API client's heartbeats. */
  ScopedSenderUnlock unlockSender(ss);
  std::map<int, int> before, after;
  if (!readStartedNodeConnections(restarter, before, label)) return -1;
  ProtocolEventListener events;
  events.timeoutMs = timeoutMs;
  if (!events.open(restarter)) {
    fprintf(stderr, "%s: cannot subscribe to leak-check events\n", label);
    return -1;
  }
  /* Each invocation uses a new cookie so a late event cannot satisfy
   * the next case's check. Node and dump code are matched as well. */
  static Uint32 nextCookie = 0;
  const Uint32 cookie = ++nextCookie;
  NdbSleep_MilliSleep(settleMs);
  for (const auto &node : before) {
    for (unsigned i = 0; i < numCodes; i++) {
      const int dump[] = {codes[i], (int)cookie};
      ndb_mgm_reply reply = {};
      if (ndb_mgm_dump_state(restarter.handle, node.first, dump, 2,
                             &reply) == -1 ||
          reply.return_code != 0) {
        fprintf(stderr, "%s: DUMP %d on node %d failed\n",
                label, codes[i], node.first);
        return -1;
      }
      char marker[128];
      snprintf(marker, sizeof(marker),
               "[JOIN_AGG_LEAK_CHECK_OK node=%u dump=%u cookie=%u]",
               (Uint32)node.first, (Uint32)codes[i], cookie);
      if (!events.waitFor(marker)) return -1;
    }
  }
  if (!readStartedNodeConnections(restarter, after, label)) return -1;
  if (before != after) {
    fprintf(stderr, "%s: data-node connections changed during leak "
                    "verification\n", label);
    return -1;
  }
  return 0;
}

#endif  // NDB_BLOCK_UNIT_TEST_JOIN_AGG_TEST_UTIL_HPP
