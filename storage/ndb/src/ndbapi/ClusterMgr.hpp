/*
   Copyright (c) 2003, 2026, Oracle and/or its affiliates.
   Copyright (c) 2021, 2025, Hopsworks and/or its affiliates.

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

#ifndef ClusterMgr_H
#define ClusterMgr_H

#include <NdbCondition.h>
#include <NdbMutex.h>
#include <NdbThread.h>
#include <atomic>
#include <ndb_limits.h>
#include <signaldata/ArbitSignalData.hpp>
#include <signaldata/DisconnectRep.hpp>
#include <signaldata/NodeStateSignalData.hpp>
#include "trp_client.hpp"
#include "trp_node.hpp"
#include "util/require.h"

extern "C" void *runClusterMgr_C(void *me);

/**
  @class ClusterMgr
  This class runs a heartbeat protocol between nodes, to detect if remote
  nodes are reachable or not. This protocol is needed because the underlying
  transporter connection may need a long time (or even forever) to detect
  node or network failure. (TCP typically gives up retransmission after about
  20 minutes).
  Therefore API_REGREQ signal are sent on regular intervals. If more than
  three signals are unanswered (by API_REGCONF) the node is presumed dead or
  unreachable, and the transporter is disconnected.
  This class handles heart beat between the following types of node pairs:
  API-DB, MGMD-DB and MGMD-MGMD, where DB means data node. There is another
  heart beat mechanism between pairs of data nodes, using the CM_HEARTBEAT
  signal.
 */
class ClusterMgr : public trp_client {
  friend class TransporterFacade;
  friend class ArbitMgr;
  friend void *runClusterMgr_C(void *me);

 public:
  ClusterMgr(class TransporterFacade &);
  ~ClusterMgr() override;

  /**
   * (Re)configure the node table. Returns false if the configuration
   * could not be applied: allocation failure, or an online (mgmd)
   * change that is not growth-only. The caller then fails
   * TransporterFacade::configure(), which the mgmd answers with its
   * existing 'this node need a restart' fallback.
   */
  bool configure(Uint32 nodeId, const ndb_mgm_configuration *config);

  void reportConnected(NodeId nodeId);
  void reportDisconnected(NodeId nodeId);
  void setProcessInfoUri(const char *scheme, const char *host, int port,
                         const char *path);
  void doStop();
  void startThread();

  /**
   * This method isn't used by the NDB code, it can be used by an API
   * user through a public method on TransporterFacade if he wants to
   * force the API node to use a different heartbeat interval than the
   * one decided by the data node.
   *
   * The variable isn't protected and there is no need for it to be.
   */
  void set_max_api_reg_req_interval(unsigned int millisec) {
    m_max_api_reg_req_interval = millisec;
  }

  void lock() {
    NdbMutex_Lock(clusterMgrThreadMutex);
    trp_client::lock();
  }
  void unlock() {
    trp_client::unlock();
    NdbMutex_Unlock(clusterMgrThreadMutex);
  }

 private:
  // 100ms is the smallest heart beat interval supported.
  static const Uint32 minHeartBeatInterval = 100;
  // 60000ms (1 min) is the max time we will wait for the first REGCONF signal
  // from data node
  static const Uint32 maxTimeWithoutFirstApiRegConfMillis = 60000;
  static const Uint32 maxIntervalsWithoutFirstApiRegConf =
      maxTimeWithoutFirstApiRegConfMillis / minHeartBeatInterval;

  void startup();
  void threadMain();

  int theStop;
  /**
   * We could end up in a situation where signals are delayed for more
   * than 100 ms, either due to slow operation or due to that we're
   * closing the TransporterFacade object. To avoid sending more than
   * signal to ourself in these cases we add this boolean variable to
   * indicate if we already sent a signal to ourself, this signal will
   * eventually arrive since it's a local signal within the same process.
   */
  bool m_sent_API_REGREQ_to_myself;
  class TransporterFacade &theFacade;
  class ArbitMgr *theArbitMgr;

  enum Cluster_state {
    CS_waiting_for_clean_cache = 0,
    CS_waiting_for_first_connect,
    CS_connected
  };

 public:
  /**
   * The node state is protected for updates by ClusterMgrThreadMutex.
   * One can call hb_received and set hbMissed to 0 though without
   * protection since this is safe. All other uses of hbCheckInterval
   * and hbMissed is internal to ClusterMgr and done with
   * protection of ClusterMgrThreadMutex.
   *
   * The node data is often read without protection as a way to decide
   * which node to communicate to. If the information read is old it
   * will mean a non-optimal decision is taken, but no specific error
   * will be the result of reading stale node info data.
   *
   * getNoOfConnectedNodes is only used by a test program, so is essentially
   * also a private method.
   */
  struct Node : public trp_node {
    Node();

    /**
     * Heartbeat stuff
     */
    Uint32 hbCheckInterval;  // Heartbeat interval (ms)
    Uint32 hbMissed;         // # missed heartbeats
    NDB_TICKS nextHbSend;
    NDB_TICKS nextHbCheck;

    bool processInfoSent;  // ProcessInfo Report has been sent to node
  };

  const trp_node &getNodeInfo(NodeId) const;
  Uint32 getNoOfConnectedNodes() const;
  void hb_received(NodeId);

  /**
   * This variable isn't protected, it's used when the last node disconnects to
   * ensure that the ClusterMgr stops and doesn't perform any reconnects.
   */
  int m_auto_reconnect;
  Uint32 m_connect_count;

 private:
  Uint32 m_max_api_reg_req_interval;
  Uint32 noOfAliveNodes;
  Uint32 noOfConnectedNodes;
  Uint32 noOfConnectedDBNodes;
  Uint32 minDbVersion;
  Uint32 minApiVersion;

  /**
   * Node slots, indexed by node id. Allocated once per configured node
   * id by configure(): 64 KB of pointers instead of the previous
   * 1.5 MB embedded Node[ABS_MAX_NODES] array in every process holding
   * an Ndb_cluster_connection. Slots are:
   * - published with a release-store only after the Node is completely
   *   initialized (including 'defined'), so the lock-free readers
   *   (getNodeInfo(), hb_received() and the node state getters) that
   *   acquire-load the pointer never observe a half-built node;
   * - never freed or moved until destruction: functions keep plain
   *   Node&/trp_node& references across their work and hb_received()
   *   writes without any lock, so entries must stay stable even across
   *   a mgmd online configuration change (growth-only, see
   *   configure());
   * - null for ids that were never configured; getNodeInfo() then
   *   returns a shared immutable default node with 'defined == false',
   *   preserving the probing semantics of the old embedded array.
   */
  std::atomic<Node *> theNodes[ABS_MAX_NODES];

  /* Set once the first configure() has completed; distinguishes the
     single-threaded initial configuration from a mgmd online
     reconfiguration with live readers. */
  bool m_configured;

  /**
   * Slot access policy:
   * - get_node_slot(): for internally driven paths where the node id
   *   comes from our own configuration view (configure(), startup(),
   *   TransporterRegistry connect/disconnect callbacks). A missing
   *   slot is a broken invariant: crash deliberately.
   * - find_node_slot(): for signal driven paths where the node id was
   *   read out of signal data. Ids outside our configuration view must
   *   be ignored, never trusted (the same policy that keeps data nodes
   *   from crashing on API_VERSION_REQ for node ids outside their
   *   configuration view).
   */
  Node *get_node_slot(NodeId nodeId) {
    require(nodeId > 0 && nodeId < ABS_MAX_NODES);
    Node *slot = theNodes[nodeId].load(std::memory_order_acquire);
    require(slot != nullptr);
    return slot;
  }
  Node *find_node_slot(NodeId nodeId) {
    if (unlikely(nodeId == 0 || nodeId >= ABS_MAX_NODES)) return nullptr;
    return theNodes[nodeId].load(std::memory_order_acquire);
  }

  NdbThread *theClusterMgrThread;

  NdbCondition *waitForHBCond;
  class ProcessInfo *m_process_info;

  enum Cluster_state m_cluster_state;
  /**
   * We use the trp_client lock to protect the variables inside of the
   * ClusterMgr. We use the clusterMgrThreadMutex to control start of
   * the ClusterMgr main thread. It also protects the theStop variable
   * against concurrent usage. Finally we need to use the clusterMgrThreadMutex
   * to protect against concurrent close of trp_client and call of
   * do_poll.
   */
  NdbMutex *clusterMgrThreadMutex;

  /**
    The rate (in milliseconds) at which this node expects to receive
    API_REGREQ heartbeat messages.
   */
  Uint32 m_hbCheckInterval;

  /**
   * The maximal time between connection attempts to data nodes.
   * start_connect_backoff_max_time is used before connection
   * to the first data node has succeeded.
   */
  Uint32 start_connect_backoff_max_time;
  Uint32 connect_backoff_max_time;

  /**
   * Should we print error messages to stderr
   *
   * Has a state change occurred that makes it worthwhile to
   * print a new error message.
   *
   * A mutex to ensure that we change the state of nodes in a
   * controlled fashion.
   *
   * The node state counter is used to know when to recalculate
   * the primary replicas in the table objects.
   */
  bool m_error_print;
  bool m_state_changed;
  bool m_ever_connected;
  Uint32 m_node_change_count;
  NdbMutex *m_node_state_mutex;

  /**
   * Signals received
   */
  void execSET_DOMAIN_ID_REQ  (const Uint32 * theData);
  void execSET_HOSTNAME_REQ(const NdbApiSignal*, const LinearSectionPtr ptr[]);
  void execACTIVATE_REQ  (const Uint32 * theData);
  void execDEACTIVATE_REQ  (const Uint32 * theData);
  void execAPI_REGREQ    (const Uint32 * theData);
  void execAPI_REGCONF   (const NdbApiSignal*, const LinearSectionPtr ptr[]);
  void execAPI_REGREF    (const Uint32 * theData);
  void execDUMP_STATE_ORD(const NdbApiSignal*, const LinearSectionPtr ptr[]);
  void execNODE_FAILREP  (const NdbApiSignal*, const LinearSectionPtr ptr[]);
  void execNF_COMPLETEREP(const NdbApiSignal*, const LinearSectionPtr ptr[3]);

  void sendSET_DOMAIN_ID_REF(Uint32, Uint32, Uint32, NodeId, Uint32, Uint32);
  void check_wait_for_hb(NodeId nodeId);

  void is_cluster_completely_unavailable(Int32 & error, Uint32 line);
  bool get_node_alive(trp_node& node)
  {
    return node.m_alive;
  }
  inline void set_node_alive(trp_node& node, bool alive){

    // Only DB nodes can be "alive"
    assert(!alive || (alive && node.m_info.getType() == NodeInfo::DB));

    if (node.m_alive && !alive) {
      assert(noOfAliveNodes);
      noOfAliveNodes--;
    } else if (!node.m_alive && alive) {
      noOfAliveNodes++;
    }
    node.m_alive = alive;
  }

  void set_node_dead(trp_node &);

  void print_nodes(const char *where, NdbOut &out = ndbout);
  void recalcMinDbVersion();
  void recalcMinApiVersion();
  void sendProcessInfoReport(NodeId nodeId);
  Uint32 get_send_heartbeat_interval(const Node &node) const;

public:
  void set_error_print(bool val)
  {
    m_error_print = val;
  }
  /**
   * trp_client interface
   *
   * This method is called from do_poll which is called from the ClusterMgr
   * main thread, we keep the clusterMgrThreadMutex when calling this method,
   * so all signal methods are protected.
   */
  void trp_deliver_signal(const NdbApiSignal *,
                          const LinearSectionPtr p[3]) override;
  Uint32 get_node_change_count();
  void lock_node_state();
  void unlock_node_state();
  int db_nodes_all_alive();
};

inline const trp_node &ClusterMgr::getNodeInfo(NodeId nodeId) const {
  /**
   * Lock-free hot path (node selection per transaction): acquire-load
   * the slot published by configure(). Ids never configured (or out of
   * range) return a reference to a shared immutable default node with
   * 'defined == false', preserving the semantics the embedded array
   * gave such probes. The default node is const and function-local
   * (thread-safe initialization); no non-const reference to it may
   * ever escape - one writer through it would poison every probe in
   * the process.
   */
  static const Node undefined_node;
  // Check array bounds
  assert(nodeId < ABS_MAX_NODES);
  if (unlikely(nodeId >= ABS_MAX_NODES)) return undefined_node;
  const Node *slot = theNodes[nodeId].load(std::memory_order_acquire);
  if (unlikely(slot == nullptr)) return undefined_node;
  return *slot;
}

inline
Uint32
ClusterMgr::get_node_change_count()
{
  return m_node_change_count;
}

inline
void
ClusterMgr::lock_node_state()
{
  NdbMutex_Lock(m_node_state_mutex);
}

inline
void
ClusterMgr::unlock_node_state()
{
  NdbMutex_Unlock(m_node_state_mutex);
}

inline
Uint32
ClusterMgr::getNoOfConnectedNodes() const {
  return noOfConnectedNodes;
}

inline void ClusterMgr::hb_received(NodeId nodeId) {
  // Check array bounds + don't allow node 0 to be touched
  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);
  if (unlikely(nodeId == 0 || nodeId >= ABS_MAX_NODES)) return;
  /**
   * Documented as safe without lock: writing hbMissed = 0 through a
   * stable slot. A heartbeat implies a configured node, so a null slot
   * is unreachable for real traffic - simply ignore it.
   */
  Node *slot = theNodes[nodeId].load(std::memory_order_acquire);
  if (unlikely(slot == nullptr)) return;
  slot->hbMissed = 0;
}

/*****************************************************************************/

extern "C" void *runArbitMgr_C(void *me);

/**
 * @class ArbitMgr
 * Arbitration manager.  Runs in separate thread.
 * Started only by a request from the kernel.
 */

class ArbitMgr {
 public:
  ArbitMgr(class ClusterMgr &);
  ~ArbitMgr();

  inline void setRank(unsigned n) { theRank = n; }
  inline void setDelay(unsigned n) { theDelay = n; }
  inline unsigned getRank() const { return theRank; }
  inline unsigned getDelay() const { return theDelay; }

  void doStart(const Uint32 *theData);
  void doChoose(const Uint32 *theData);
  void doStop(const Uint32 *theData);

  /*
   * True once the arbitrator thread has accepted the kernel's
   * ARBIT_STARTREQ, entered the started state, and sent
   * ARBIT_STARTCONF back to Qmgr.  Stays true after stop, since the
   * startup gate in MgmtSrvr::start() only cares that the role was
   * assumed at least once.
   */
  bool isActiveArbitrator() const {
    return m_active_arbitrator.load(std::memory_order_acquire);
  }

  friend void *runArbitMgr_C(void *me);

 private:
  class ClusterMgr &m_clusterMgr;
  unsigned theRank;
  unsigned theDelay;

  void threadMain();
  NdbThread *theThread;
  NdbMutex *theThreadMutex;  // not really needed

  struct ArbitSignal {
    GlobalSignalNumber gsn;
    ArbitSignalData data;
    NDB_TICKS startticks;

    ArbitSignal() {}

    inline void init(GlobalSignalNumber aGsn, const Uint32 *aData) {
      gsn = aGsn;
      if (aData != nullptr)
        memcpy(&data, aData, sizeof(data));
      else
        memset(&data, 0, sizeof(data));
    }

    inline void setTimestamp() { startticks = NdbTick_getCurrentTicks(); }

    inline Uint64 getTimediff() {
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      return NdbTick_Elapsed(startticks, now).milliSec();
    }
  };

  NdbMutex *theInputMutex;
  NdbCondition *theInputCond;
  int theInputTimeout;
  bool theInputFull;           // the predicate
  ArbitSignal theInputBuffer;  // shared buffer

  void sendSignalToThread(ArbitSignal &aSignal);

  enum State {  // thread states
    StateInit,
    StateStarted,  // thread started
    StateChoose1,  // received one valid REQ
    StateChoose2,  // received two valid REQs
    StateFinished  // finished one way or other
  };
  State theState;

  enum Stop {         // stop code in ArbitSignal.data.code
    StopExit = 1,     // at API exit
    StopRequest = 2,  // request from kernel
    StopRestart = 3   // stop before restart
  };

  void threadStart(ArbitSignal &aSignal);  // handle thread events
  void threadChoose(ArbitSignal &aSignal);
  void threadTimeout();
  void threadStop(ArbitSignal &aSignal);

  ArbitSignal theStartReq;
  ArbitSignal theChooseReq1;
  ArbitSignal theChooseReq2;
  ArbitSignal theStopOrd;

  void sendStartConf(ArbitSignal &aSignal, Uint32);
  void sendChooseRef(ArbitSignal &aSignal, Uint32);
  void sendChooseConf(ArbitSignal &aSignal, Uint32);
  void sendStopRep(ArbitSignal &aSignal, Uint32);

  void sendSignalToQmgr(ArbitSignal &aSignal);

  std::atomic<bool> m_active_arbitrator{false};
};

#endif
