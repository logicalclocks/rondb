/*
   Copyright (c) 2003, 2026, Oracle and/or its affiliates.
   Copyright (c) 2021, 2026, Hopsworks and/or its affiliates.

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

#include <ndb_global.h>
#include <ndb_limits.h>
#include <util/version.h>

#include <kernel/GlobalSignalNumbers.h>
#include "TransporterFacade.hpp"

#include <NdbSleep.h>
#include <NdbTick.h>
#include <NdbTimestamp.h>
#include <Logger.hpp>
#include <EventLogger.hpp>
#include <IPCConfig.hpp>
#include <NdbOut.hpp>
#include <OwnProcessInfo.hpp>
#include <ProcessInfo.hpp>
#include "ClusterMgr.hpp"
#include "NdbApiSignal.hpp"
#include "ndb_internal.hpp"

#include <signaldata/AlterTable.hpp>
#include <signaldata/ApiRegSignalData.hpp>
#include <signaldata/NFCompleteRep.hpp>
#include <signaldata/NodeFailRep.hpp>
#include <signaldata/ProcessInfoRep.hpp>
#include <signaldata/SumaImpl.hpp>
#include "kernel/signaldata/DumpStateOrd.hpp"
#include "kernel/signaldata/TestOrd.hpp"
#include <signaldata/Activate.hpp>
#include <signaldata/SetHostname.hpp>
#include <signaldata/SetDomainId.hpp>
#include <signaldata/CreateDatabase.hpp>
#include <signaldata/DropDatabase.hpp>
#include <signaldata/QueryDatabase.hpp>
#include <signaldata/TcKeyRef.hpp>

#include <mgmapi.h>
#include <mgmapi_config_parameters.h>
#include <EventLogger.hpp>
#include <mgmapi_configuration.hpp>
#include <rondb_hash.hpp>

#if 0
#define DEBUG_FPRINTF(arglist) \
  do {                         \
    fprintf arglist;           \
  } while (0)
#else
#define DEBUG_FPRINTF(a)
#endif

#if defined(__GNUC__) && !defined(__clang__)
#  define PRINTF_FMT(fmt_idx, arg_idx) \
       __attribute__((format(gnu_printf, fmt_idx, arg_idx)))
#elif defined(__clang__)
#  define PRINTF_FMT(fmt_idx, arg_idx) \
       __attribute__((format(printf, fmt_idx, arg_idx)))
#else
#  define PRINTF_FMT(fmt_idx, arg_idx)
#endif

int global_flag_skip_invalidate_cache = 0;
int global_flag_skip_waiting_for_clean_cache = 0;
extern "C"
void
error_printer(const char *fmt, ...) PRINTF_FMT(1, 2);

extern "C"
void
error_printer(const char * fmt, ...)
{
  va_list ap;
  char buf[400];

  char timestamp[64];
  std::timespec now = NdbTimestamp_GetCurrentTime();
  Logger::format_timestamp(&now, timestamp, sizeof(timestamp));
  va_start(ap, fmt);
  size_t len = BaseString::vsnprintf(buf, sizeof(buf)-1, fmt, ap);
  if (len > sizeof(buf) - 2) len = sizeof(buf) - 2;
  memcpy(&buf[len], "\n", 2);
  fprintf(stderr, "%s:[RonDB] %s", timestamp, buf);
  va_end(ap);
}
// #define DEBUG_REG
#define DEBUG_USER

extern EventLogger *g_eventLogger;

// Just a C wrapper for threadMain
extern "C" void *runClusterMgr_C(void *me) {
  ((ClusterMgr *)me)->threadMain();

  return nullptr;
}

ClusterMgr::ClusterMgr(TransporterFacade & _facade):
  theStop(0),
  m_sent_API_REGREQ_to_myself(false),
  theFacade(_facade),
  theArbitMgr(nullptr),
  m_connect_count(0),
  m_max_api_reg_req_interval(~0),
  noOfAliveNodes(0),
  noOfConnectedNodes(0),
  noOfConnectedDBNodes(0),
  minDbVersion(0),
  minApiVersion(0),
  theClusterMgrThread(nullptr),
  m_process_info(nullptr),
  m_cluster_state(CS_waiting_for_clean_cache),
  m_hbCheckInterval(0),
  m_error_print(false),
  m_state_changed(true),
  m_ever_connected(false),
  m_node_change_count(0)
{
  DBUG_ENTER("ClusterMgr::ClusterMgr");
  clusterMgrThreadMutex = NdbMutex_Create();
  m_node_state_mutex = NdbMutex_Create();
  waitForHBCond= NdbCondition_Create();
  m_auto_reconnect = -1;

  Uint32 ret = this->open(&theFacade, API_CLUSTERMGR);
  if (unlikely(ret == 0)) {
    fprintf(stderr,
            "%s NDBAPI FATAL ERROR : Failed to register "
            "ClusterMgr! ret: %d\n",
            Logger::Timestamp().c_str(), ret);
    abort();
  }
  createUserIdHash();
  DBUG_VOID_RETURN;
}

ClusterMgr::~ClusterMgr() {
  DBUG_ENTER("ClusterMgr::~ClusterMgr");
  assert(theStop == 1);
  if (theArbitMgr != nullptr) {
    delete theArbitMgr;
    theArbitMgr = nullptr;
  }
  NdbCondition_Destroy(waitForHBCond);
  NdbMutex_Destroy(clusterMgrThreadMutex);
  NdbMutex_Destroy(m_node_state_mutex);
  ProcessInfo::release(m_process_info);
  releaseUserIdHash();
  DBUG_VOID_RETURN;
}

/**
 * This method is called from start of cluster connection instance and
 * before we have started any socket services and thus it needs no
 * mutex protection since the ClusterMgr object isn't known by any other
 * thread at this point in time.
 */
void ClusterMgr::configure(Uint32 nodeId, const ndb_mgm_configuration *config) {
  ndb_mgm_configuration_iterator iter(config, CFG_SECTION_NODE);
  for (iter.first(); iter.valid(); iter.next()) {
    Uint32 nodeId = 0;
    if (iter.get(CFG_NODE_ID, &nodeId)) continue;

    // Check array bounds + don't allow node 0 to be touched
    assert(nodeId > 0 && nodeId < ABS_MAX_NODES);
    trp_node &theNode = theNodes[nodeId];
    theNode.defined = true;

    unsigned type;
    if (iter.get(CFG_TYPE_OF_SECTION, &type)) continue;

    switch (type) {
      case NODE_TYPE_DB:
        theNode.m_info.m_type = NodeInfo::DB;
        break;
      case NODE_TYPE_API:
        theNode.m_info.m_type = NodeInfo::API;
        break;
      case NODE_TYPE_MGM:
        theNode.m_info.m_type = NodeInfo::MGM;
        break;
      default:
        break;
    }
    Uint32 is_active = 1;
    iter.get(CFG_NODE_ACTIVE, &is_active);
    theNode.m_node_active = (is_active == 1);
  }

  /* Mark all non existing nodes as not defined */
  for (Uint32 i = 0; i < ABS_MAX_NODES; i++) {
    if (iter.first()) continue;

    if (iter.find(CFG_NODE_ID, i)) theNodes[i] = Node();
  }

#if 0
  print_nodes("init");
#endif

  // Configure arbitrator
  Uint32 rank = 0;
  iter.first();
  iter.find(CFG_NODE_ID, nodeId);  // let not found in config mean rank=0
  iter.get(CFG_NODE_ARBIT_RANK, &rank);

  if (rank > 0) {
    // The arbitrator should be active
    if (!theArbitMgr) theArbitMgr = new ArbitMgr(*this);
    theArbitMgr->setRank(rank);

    Uint32 delay = 0;
    iter.get(CFG_NODE_ARBIT_DELAY, &delay);
    theArbitMgr->setDelay(delay);
  } else if (theArbitMgr) {
    // No arbitrator should be started
    theArbitMgr->doStop(nullptr);
    delete theArbitMgr;
    theArbitMgr = nullptr;
  }

  // Configure heartbeats.
  unsigned hbCheckInterval = 0;
  iter.get(CFG_MGMD_MGMD_HEARTBEAT_INTERVAL, &hbCheckInterval);
  m_hbCheckInterval = static_cast<Uint32>(hbCheckInterval);

  // Configure max backoff time for connection attempts to first
  // data node.
  Uint32 backoff_max_time = 0;
  iter.get(CFG_START_CONNECT_BACKOFF_MAX_TIME, &backoff_max_time);
  start_connect_backoff_max_time = backoff_max_time;

  // Configure max backoff time for connection attempts to data
  // nodes.
  backoff_max_time = 0;
  iter.get(CFG_CONNECT_BACKOFF_MAX_TIME, &backoff_max_time);
  connect_backoff_max_time = backoff_max_time;

  theFacade.get_registry()->set_connect_backoff_max_time_in_ms(
      start_connect_backoff_max_time);

  m_process_info = ProcessInfo::forNodeId(nodeId);
}

void ClusterMgr::startThread() {
  DBUG_ENTER("ClusterMgr::startThread");
  /**
   * We use the clusterMgrThreadMutex as a signalling object between this
   * thread and the main thread of the ClusterMgr.
   * The clusterMgrThreadMutex also protects the theStop-variable.
   */
  Guard g(clusterMgrThreadMutex);

  theStop = -1;
  theClusterMgrThread =
      NdbThread_Create(runClusterMgr_C, (void **)this,
                       0,  // default stack size
                       "ndb_clustermgr", NDB_THREAD_PRIO_HIGH);
  if (theClusterMgrThread == nullptr) {
    fprintf(stderr,
            "%s NDBAPI FATAL ERROR : ClusterMgr::startThread:"
            " Failed to create thread for cluster management.\n",
            Logger::Timestamp().c_str());
    abort();
    DBUG_VOID_RETURN;
  }

  Uint32 cnt = 0;
  while (theStop == -1 && cnt < 60) {
    NdbCondition_WaitTimeout(waitForHBCond, clusterMgrThreadMutex, 1000);
  }

  assert(theStop == 0);
  DBUG_VOID_RETURN;
}

void ClusterMgr::doStop() {
  DBUG_ENTER("ClusterMgr::doStop");
  {
    /* Ensure stop is only executed once */
    Guard g(clusterMgrThreadMutex);
    if (theStop == 1) {
      DBUG_VOID_RETURN;
    }
    theStop = 1;
  }

  void *status;
  if (theClusterMgrThread) {
    NdbThread_WaitFor(theClusterMgrThread, &status);
    NdbThread_Destroy(&theClusterMgrThread);
  }

  if (theArbitMgr != nullptr) {
    theArbitMgr->doStop(nullptr);
  }
  {
    /**
     * Need protection against concurrent execution of do_poll in main
     * thread. We cannot rely only on the trp_client lock since it is
     * not supposed to be locked when calling close (it is locked as
     * part of the close logic.
     */
    Guard g(clusterMgrThreadMutex);
    this->close();  // disconnect from TransporterFacade
  }

  DBUG_VOID_RETURN;
}

void ClusterMgr::startup() {
  assert(theStop == -1);
  Uint32 nodeId = getOwnNodeId();
  Node &cm_node = theNodes[nodeId];
  trp_node &theNode = cm_node;
  assert(theNode.defined);

  lock();
  theFacade.startConnecting(nodeId);
  flush_send_buffers();
  unlock();

  // Wait for the async 'connecting' protocol to report 'connected'
  for (Uint32 i = 0; i < 3000; i++) {
    theFacade.request_connection_check();
    prepare_poll();
    do_poll(0);
    complete_poll();

    if (theNode.is_connected()) break;
    NdbSleep_MilliSleep(20);
  }

  assert(theNode.is_connected());
  Guard g(clusterMgrThreadMutex);
  /* Signalling to creating thread that we are done with thread startup */
  theStop = 0;
  NdbCondition_Broadcast(waitForHBCond);
}

Uint32 ClusterMgr::get_send_heartbeat_interval(const Node &cm_node) const {
  // Send heartbeat twice as frequent than checking them.
  return std::min(m_max_api_reg_req_interval, cm_node.hbCheckInterval / 2);
}

void ClusterMgr::threadMain() {
  startup();

  NdbApiSignal signal(numberToRef(API_CLUSTERMGR, theFacade.ownId()));

  signal.theVerId_signalNumber = GSN_API_REGREQ;
  signal.theTrace = 0;
  signal.theLength = ApiRegReq::SignalLength;

  ApiRegReq *req = CAST_PTR(ApiRegReq, signal.getDataPtrSend());
  req->ref = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  req->version = NDB_VERSION;
  req->mysql_version = NDB_MYSQL_VERSION_D;

  NdbApiSignal nodeFail_signal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
  nodeFail_signal.theVerId_signalNumber = GSN_NODE_FAILREP;
  nodeFail_signal.theReceiversBlockNumber = API_CLUSTERMGR;
  nodeFail_signal.theTrace = 0;
  nodeFail_signal.theLength = NodeFailRep::SignalLengthLong;

  NDB_TICKS now = NdbTick_getCurrentTicks();

  while (!theStop) {
    /* Sleep 1/5 of minHeartBeatInterval between each check */
    for (Uint32 i = 0; i < 5; i++) {
      NdbSleep_MilliSleep(minHeartBeatInterval / 5);
      {
        /**
         * prepare_poll does lock the trp_client and complete_poll
         * releases this lock. This means that this protects
         * against concurrent calls to send signals in ArbitMgr.
         * We do however need to protect also against concurrent
         * close in doStop, so to avoid this problem we need to
         * also lock clusterMgrThreadMutex before we start the
         * poll.
         */
        Guard g(clusterMgrThreadMutex);
        prepare_poll();
        do_poll(0);
        complete_poll();
      }
    }
    now = NdbTick_getCurrentTicks();

    lock();
    if (m_cluster_state == CS_waiting_for_clean_cache &&
        theFacade.m_globalDictCache) {
      if (!global_flag_skip_waiting_for_clean_cache) {
        theFacade.m_globalDictCache->lock();
        unsigned sz = theFacade.m_globalDictCache->get_size();
        theFacade.m_globalDictCache->unlock();
        if (sz) {
          unlock();
          continue;
        }
      }
      m_cluster_state = CS_waiting_for_first_connect;
    }

    Uint32 *nodeFailData = nodeFail_signal.getDataPtrSend();
    Uint32 theAllNodes[NodeBitmask::Size];
    Uint32 noOfNodes = 0;
    NodeBitmask::clear(theAllNodes);

    for (int i = 1; i < ABS_MAX_NODES; i++) {
      /**
       * Send register request (heartbeat) to all available nodes
       * at specified timing intervals
       */
      const NodeId nodeId = i;
      // Check array bounds + don't allow node 0 to be touched
      assert(nodeId > 0 && nodeId < ABS_MAX_NODES);
      Node &cm_node = theNodes[nodeId];
      trp_node &theNode = cm_node;

      if (!theNode.defined) continue;

      if (theNode.is_connected() == false) {
        theFacade.startConnecting(nodeId);
        continue;
      }

      if (!theNode.compatible) {
        continue;
      }

      if (nodeId == getOwnNodeId()) {
        /**
         * Don't send HB to self more than once
         * (once needed to avoid weird special cases in e.g ConfigManager)
         */
        if (m_sent_API_REGREQ_to_myself) {
          continue;
        }
      }

      // Check missed heartbeat
      if (cm_node.hbCheckInterval == 0 ||
          NdbTick_Compare(now, cm_node.nextHbCheck) >= 0) {
        cm_node.hbMissed++;
        cm_node.nextHbCheck =
            NdbTick_AddMilliseconds(now, cm_node.hbCheckInterval);
        if (cm_node.hbMissed >= 2 && cm_node.hbCheckInterval > 0) {
          g_eventLogger->warning("Node %u missed heartbeat %u from node %u.",
                                 getOwnNodeId(), cm_node.hbMissed - 1, nodeId);
        }
      }

      /**
       * It is now time to send a new Heartbeat
       */

      if (cm_node.hbCheckInterval == 0 ||
          NdbTick_Compare(now, cm_node.nextHbSend) >= 0) {
        if (theNode.m_info.m_type != NodeInfo::DB)
          signal.theReceiversBlockNumber = API_CLUSTERMGR;
        else
          signal.theReceiversBlockNumber = QMGR;

#ifdef DEBUG_REG
        g_eventLogger->info("ClusterMgr: Sending API_REGREQ to node %d",
                            (int)nodeId);
#endif
        if (nodeId == getOwnNodeId()) {
          /* Set flag to ensure we only send once to ourself */
          m_sent_API_REGREQ_to_myself = true;
        }
        raw_sendSignal(&signal, nodeId);
        assert(m_max_api_reg_req_interval > 0);
        Uint32 send_interval = get_send_heartbeat_interval(cm_node);
        if (send_interval > 0)
          cm_node.nextHbSend = NdbTick_AddMilliseconds(now, send_interval);
      }  // if

      /**
       * Node can be reported as disconnected in two different ways
       * 1 - Node was reported as connected, hbCheckInterval already configured
       * (arrived as part of an earlier API_REGCONF signal received) but no
       * API_REGCONF arriving for, at least, 3 * hbCheckInterval milliseconds.
       * 2 - Node reported as connected, first API_REGCONF missed for more
       * them maxTimeWithoutFirstApiRegConfMillis / minHeartBeatInterval
       * (60 seconds).
       */
      if ((cm_node.hbMissed == 4 && cm_node.hbCheckInterval > 0) ||
          (cm_node.hbMissed == maxIntervalsWithoutFirstApiRegConf &&
           cm_node.hbCheckInterval == 0)) {
        g_eventLogger->error(
            "Node %u disconnecting node %u "
            "due to missed heartbeat",
            getOwnNodeId(), nodeId);
        noOfNodes++;
        NodeBitmask::set(theAllNodes, nodeId);
      }
    }
    flush_send_buffers();
    unlock();

    if (noOfNodes) {
      nodeFailData[NodeFailRep::NoOfNodesIndex] = noOfNodes;
      lock();
      LinearSectionPtr lsptr[3];
      lsptr[0].p = theAllNodes;
      lsptr[0].sz = NodeBitmask::getPackedLengthInWords(theAllNodes);

      raw_sendSignal(&nodeFail_signal, getOwnNodeId(), lsptr, 1);
      flush_send_buffers();
      unlock();
    }
  }
}

/**
 * We're holding the trp_client lock while performing poll from
 * ClusterMgr. So we always execute all the execSIGNAL-methods in
 * ClusterMgr with protection other methods that use the trp_client
 * lock (reportDisconnect, reportConnect, is_cluster_completely_unavailable,
 * ArbitMgr (sendSignalToQmgr)).
 */
void ClusterMgr::trp_deliver_signal(const NdbApiSignal *sig,
                                    const LinearSectionPtr ptr[3]) {
  const Uint32 gsn = sig->theVerId_signalNumber;
  const Uint32 *theData = sig->getDataPtr();

  switch (gsn) {
    case GSN_LIST_DATABASE_CONF:
      execLIST_DATABASE_CONF(theData, ptr);
      break;

    case GSN_DROP_DATABASE_REP:
      execDROP_DATABASE_REP(theData, ptr);
      break;

    case GSN_CREATE_DATABASE_REP:
      execCREATE_DATABASE_REP(theData, ptr);
      break;

    case GSN_SET_DOMAIN_ID_REQ:
      execSET_DOMAIN_ID_REQ(theData);
      break;

    case GSN_ACTIVATE_REQ:
      execACTIVATE_REQ(theData);
      break;

    case GSN_DEACTIVATE_REQ:
      execDEACTIVATE_REQ(theData);
      break;

    case GSN_SET_HOSTNAME_REQ:
      execSET_HOSTNAME_REQ(sig, ptr);
      break;

    case GSN_API_REGREQ:
      execAPI_REGREQ(theData);
      break;

    case GSN_API_REGCONF:
      execAPI_REGCONF(sig, ptr);
      break;

    case GSN_API_REGREF:
      execAPI_REGREF(theData);
      break;

    case GSN_DUMP_STATE_ORD:
      execDUMP_STATE_ORD(sig, ptr);
      break;

    case GSN_NODE_FAILREP:
      execNODE_FAILREP(sig, ptr);
      break;

    case GSN_NF_COMPLETEREP:
      execNF_COMPLETEREP(sig, ptr);
      break;
    case GSN_ARBIT_STARTREQ:
      if (theArbitMgr != nullptr) theArbitMgr->doStart(theData);
      break;

    case GSN_ARBIT_CHOOSEREQ:
      if (theArbitMgr != nullptr) theArbitMgr->doChoose(theData);
      break;

    case GSN_ARBIT_STOPORD:
      if (theArbitMgr != nullptr) theArbitMgr->doStop(theData);
      break;

    case GSN_ALTER_TABLE_REP: {
      if (theFacade.m_globalDictCache == nullptr) break;
      const AlterTableRep *rep = (const AlterTableRep *)theData;
      theFacade.m_globalDictCache->lock();
      theFacade.m_globalDictCache->alter_table_rep(
          (const char *)ptr[0].p, rep->tableId, rep->tableVersion,
          rep->changeType == AlterTableRep::CT_ALTERED);
      theFacade.m_globalDictCache->unlock();
      break;
    }
    case GSN_SUB_GCP_COMPLETE_REP: {
      /**
       * Report
       */
      theFacade.for_each(this, sig, ptr);

      /**
       * Reply
       */
      {
        BlockReference ownRef = numberToRef(API_CLUSTERMGR, theFacade.ownId());
        NdbApiSignal tSignal(*sig);
        Uint32 *send = tSignal.getDataPtrSend();
        memcpy(send, theData, tSignal.getLength() << 2);
        CAST_PTR(SubGcpCompleteAck, send)->rep.senderRef = ownRef;
        Uint32 ref = sig->theSendersBlockRef;
        Uint32 aNodeId = refToNode(ref);
        tSignal.theReceiversBlockNumber = refToBlock(ref);
        tSignal.theVerId_signalNumber = GSN_SUB_GCP_COMPLETE_ACK;
        tSignal.theSendersBlockRef = API_CLUSTERMGR;

        // Send signal without delay, otherwise, Suma buffers may
        // overflow, resulting into the API node being disconnected.
        // SUB_GCP_COMPLETE_ACK will be sent per node per epoch, with
        // minimum interval of TimeBetweenEpochs.
        safe_sendSignal(&tSignal, aNodeId);

        /**
         * Note:
         * After fixing #Bug#22705935 'sendSignal() flush optimization isses',
         * we could likely just as well have used safe_noflush_sendSignal()
         * above. (and several other places) That patch ensures that any
         * buffered signals sent while delivering signals are flushed as soon as
         * we have processed the chunk of signals to be delivered.
         */
      }
      break;
    }
    case GSN_TAKE_OVERTCCONF: {
      /**
       * Report
       */
      theFacade.for_each(this, sig, ptr);
      return;
    }
    case GSN_CLOSE_COMREQ: {
      theFacade.perform_close_clnt(this);
      return;
    }
    case GSN_EXPAND_CLNT: {
      theFacade.expand_clnt();
      return;
    }
    default:
      break;
  }
  return;
}

ClusterMgr::Node::Node() : hbCheckInterval(0), processInfoSent(false) {}

/**
 * recalcMinDbVersion
 *
 * This method is called whenever the 'minimum DB node
 * version' data for the connected DB nodes changes
 * It calculates the minimum version of all the connected
 * DB nodes.
 * This information is cached by Ndb object instances.
 * This information is useful when implementing API compatibility
 * with older DB nodes
 */
void ClusterMgr::recalcMinDbVersion() {
  Uint32 newMinDbVersion = ~(Uint32)0;

  for (Uint32 i = 0; i < ABS_MAX_NODES; i++) {
    trp_node &node = theNodes[i];

    if (node.is_connected() && node.is_confirmed() &&
        node.m_info.getType() == NodeInfo::DB) {
      /* Include this node in the set of nodes used to
       * compute the lowest current DB node version
       */
      assert(node.m_info.m_version);

      if (node.minDbVersion < newMinDbVersion) {
        newMinDbVersion = node.minDbVersion;
      }
    }
  }

  /* Now update global min Db version if we have one.
   * Otherwise set it to 0
   */
  newMinDbVersion = (newMinDbVersion == ~(Uint32)0) ? 0 : newMinDbVersion;

  // #ifdef DEBUG_MINVER

#ifdef DEBUG_MINVER
  if (newMinDbVersion != minDbVersion) {
    ndbout << "Previous min Db node version was " << NdbVersion(minDbVersion)
           << " new min is " << NdbVersion(newMinDbVersion) << endl;
  } else {
    ndbout << "MinDbVersion recalculated, but is same : "
           << NdbVersion(minDbVersion) << endl;
  }
#endif

  minDbVersion = newMinDbVersion;
}

/**
 * recalcMinApiVersion
 *
 * This method is called whenever the 'minimum API node
 * version' data for the connected DB nodes changes
 * It calculates the minimum version of all the connected
 * API nodes.
 * This information is cached by Ndb object instances.
 * This information is useful when implementing API compatibility
 * with older API nodes
 */
void ClusterMgr::recalcMinApiVersion() {
  Uint32 newMinApiVersion = ~(Uint32)0;

  for (Uint32 i = 0; i < ABS_MAX_NODES; i++) {
    trp_node &node = theNodes[i];

    if (node.is_connected() && node.is_confirmed() &&
        node.m_info.getType() == NodeInfo::DB) {
      /* Include this node in the set of nodes used to
       * compute the lowest current API node version
       */
      assert(node.m_info.m_version);

      if (node.minApiVersion < newMinApiVersion) {
        newMinApiVersion = node.minApiVersion;
      }
    }
  }

  /* Now update global min Api version if we have one.
   * Otherwise set it to 0
   */
  newMinApiVersion = (newMinApiVersion == ~(Uint32)0) ? 0 : newMinApiVersion;

  minApiVersion = newMinApiVersion;
}

void
ClusterMgr::sendSET_DOMAIN_ID_REF(Uint32 ref,
                                  Uint32 senderId,
                                  Uint32 senderRef,
                                  NodeId changeNodeId,
                                  Uint32 locationDomainId,
                                  Uint32 errorCode) {
  NdbApiSignal signal(ref);
  SetDomainIdRef * const ref_sig =
    CAST_PTR(SetDomainIdRef, signal.getDataPtrSend());
  signal.theVerId_signalNumber = GSN_SET_DOMAIN_ID_REF;
  signal.theReceiversBlockNumber = refToMain(senderRef);
  signal.theTrace = 0;
  signal.theLength = SetDomainIdRef::SignalLength;
  ref_sig->senderId = senderId;
  ref_sig->senderRef = ref;
  ref_sig->changeNodeId = changeNodeId;
  ref_sig->locationDomainId = locationDomainId;
  ref_sig->errorCode = errorCode;
  safe_sendSignal(&signal, refToNode(senderRef));
  DEBUG_FPRINTF((stderr, "Send SET_DOMAIN_ID_REF to %u about node %u, err: %u",
                 refToNode(senderRef),
                 changeNodeId,
                 errorCode));

}

void
ClusterMgr::execSET_DOMAIN_ID_REQ(const Uint32 *theData)
{
  const SetDomainIdReq * const setDomainIdReq =
    (const SetDomainIdReq *)&theData[0];
  Uint32 senderId = setDomainIdReq->senderId;
  Uint32 senderRef = setDomainIdReq->senderRef;
  NodeId changeNodeId = setDomainIdReq->changeNodeId;
  Uint32 locationDomainId = setDomainIdReq->locationDomainId;
  Uint32 ref = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  if (changeNodeId < 1 || MAX_NODES_ID < changeNodeId)
  {
    /* Should never happen, thus error code 0 */
    sendSET_DOMAIN_ID_REF(ref,
                          senderId,
                          senderRef,
                          changeNodeId,
                          locationDomainId,
                          0);
    return;
  }
  Ndb_cluster_connection_impl *ndb_cluster_connection =
    theFacade.get_ndb_cluster_connection();
  int error_code = 0;
  if (ndb_cluster_connection != nullptr) {
    error_code =
      ndb_cluster_connection->set_location_domain_id(changeNodeId,
                                                     locationDomainId);
  }
  if (error_code != 0) {
    sendSET_DOMAIN_ID_REF(ref,
                          senderId,
                          senderRef,
                          changeNodeId,
                          locationDomainId,
                          Uint32(error_code));
    return;
  }
  NdbApiSignal signal(ref);
  SetDomainIdConf * const conf =
    CAST_PTR(SetDomainIdConf, signal.getDataPtrSend());
  signal.theVerId_signalNumber   = GSN_SET_DOMAIN_ID_CONF;
  signal.theReceiversBlockNumber = refToMain(senderRef);
  signal.theTrace = 0;
  signal.theLength = SetDomainIdConf::SignalLength;
  conf->senderId = theFacade.ownId();
  conf->senderRef = ref;
  conf->changeNodeId = changeNodeId;
  conf->locationDomainId = locationDomainId;
  safe_sendSignal(&signal, refToNode(senderRef));
  DEBUG_FPRINTF((stderr, "Send SET_DOMAIN_ID_CONF to %u about node %u",
                 refToNode(senderRef),
                 changeNodeId));
}

void
ClusterMgr::execACTIVATE_REQ(const Uint32 *theData)
{
  const ActivateReq * const activateReq = (const ActivateReq *)&theData[0];
  TransporterRegistry *tr = theFacade.get_registry();
  Uint32 senderRef = activateReq->senderRef;
  Uint32 activateNodeId = activateReq->activateNodeId;
  Uint32 ref = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  NdbApiSignal signal(ref);
  if (activateNodeId > ABS_MAX_NODES)
  {
    ActivateRef * const ref_sig =
      CAST_PTR(ActivateRef, signal.getDataPtrSend());
    signal.theVerId_signalNumber   = GSN_ACTIVATE_REF;
    signal.theReceiversBlockNumber = refToMain(senderRef);
    signal.theTrace                = 0;
    signal.theLength               = ActivateRef::SignalLength;
    ref_sig->senderRef = ref;
    ref_sig->senderNodeId = theFacade.ownId();
    ref_sig->activateNodeId = activateNodeId;
    safe_sendSignal(&signal, refToNode(senderRef));
    DEBUG_FPRINTF((stderr, "Send ACTIVATE_REF to %u about node %u",
                   refToNode(senderRef),
                   activateNodeId));
    return;
  }
  /* Perform the actual activation of the node in the transporter setup */
  tr->set_active_node(activateNodeId, 1, false);
  ActivateConf * const conf = CAST_PTR(ActivateConf, signal.getDataPtrSend());
  signal.theVerId_signalNumber   = GSN_ACTIVATE_CONF;
  signal.theReceiversBlockNumber = refToMain(senderRef);
  signal.theTrace                = 0;
  signal.theLength               = ActivateConf::SignalLength;
  conf->senderRef = ref;
  conf->senderNodeId = theFacade.ownId();
  conf->activateNodeId = activateNodeId;
  safe_sendSignal(&signal, refToNode(senderRef));
  DEBUG_FPRINTF((stderr, "Send ACTIVATE_CONF to %u about node %u",
                 refToNode(senderRef),
                 activateNodeId));
}

void
ClusterMgr::execDEACTIVATE_REQ(const Uint32 *theData)
{
  const DeactivateReq * const deactivateReq = (const DeactivateReq *)&theData[0];
  TransporterRegistry *tr = theFacade.get_registry();
  Uint32 senderRef = deactivateReq->senderRef;
  Uint32 deactivateNodeId = deactivateReq->deactivateNodeId;
  Uint32 ref = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  NdbApiSignal signal(ref);
  if (deactivateNodeId > ABS_MAX_NODES)
  {
    DeactivateRef * const ref_sig =
      CAST_PTR(DeactivateRef, signal.getDataPtrSend());
    signal.theVerId_signalNumber   = GSN_DEACTIVATE_REF;
    signal.theReceiversBlockNumber = refToMain(senderRef);
    signal.theTrace                = 0;
    signal.theLength               = DeactivateRef::SignalLength;
    ref_sig->senderRef = ref;
    ref_sig->senderNodeId = theFacade.ownId();
    ref_sig->deactivateNodeId = deactivateNodeId;
    safe_sendSignal(&signal, refToNode(senderRef));
    DEBUG_FPRINTF((stderr, "Send DEACTIVATE_REF to %u about node %u",
                   refToNode(senderRef),
                   deactivateNodeId));
    return;
  }
  /* Perform the actual deactivation of the node in the transporter setup */
  tr->set_active_node(deactivateNodeId, 0, false);
  DeactivateConf * const conf = CAST_PTR(DeactivateConf, signal.getDataPtrSend());
  signal.theVerId_signalNumber   = GSN_DEACTIVATE_CONF;
  signal.theReceiversBlockNumber = refToMain(senderRef);
  signal.theTrace                = 0;
  signal.theLength               = DeactivateConf::SignalLength;
  conf->senderRef = ref;
  conf->senderNodeId = theFacade.ownId();
  conf->deactivateNodeId = deactivateNodeId;
  safe_sendSignal(&signal, refToNode(senderRef));
  DEBUG_FPRINTF((stderr, "Send DEACTIVATE_CONF to %u about node %u",
                 refToNode(senderRef),
                 deactivateNodeId));
}

void
ClusterMgr::execSET_HOSTNAME_REQ(const NdbApiSignal* sig,
                                 const LinearSectionPtr ptr[])
{
  TransporterRegistry *tr = theFacade.get_registry();
  const Uint32 * theData = sig->getDataPtr();
  const SetHostnameReq * const setHostnameReq =
    (const SetHostnameReq *)&theData[0];
  Uint32 senderRef = setHostnameReq->senderRef;
  Uint32 changeNodeId = setHostnameReq->changeNodeId;
  bool ok = true;
  if (changeNodeId > ABS_MAX_NODES)
  {
    ok = false;
  }
  else
  {
    Uint32 activeFlag = tr->get_active_node(changeNodeId);
    if (activeFlag)
    {
      ok = false;
    }
    else if (ptr[0].sz > 64)
    {
      ok = false;
    }
  }
  Uint32 ref = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  NdbApiSignal signal(ref);
  if (!ok)
  {
    SetHostnameRef * const ref_sig =
      CAST_PTR(SetHostnameRef, signal.getDataPtrSend());
    signal.theVerId_signalNumber   = GSN_SET_HOSTNAME_REF;
    signal.theReceiversBlockNumber = refToMain(senderRef);
    signal.theTrace                = 0;
    signal.theLength               = SetHostnameRef::SignalLength;
    ref_sig->senderRef = ref;
    ref_sig->senderNodeId = theFacade.ownId();
    ref_sig->changeNodeId = changeNodeId;
    safe_sendSignal(&signal, refToNode(senderRef));
    DEBUG_FPRINTF((stderr, "Send SET_HOSTNAME_REF to %u about node %u",
                   refToNode(senderRef),
                   changeNodeId));
    return;
  }
  union
  {
    char hostname_buf[256];
    Uint32 hostname_buf32[64];
  };
  memset(&hostname_buf[0], 0, 256);
  memcpy(&hostname_buf[0], ptr[0].p, 4 * ptr[0].sz);

  /* Perform the actual change of hostname in the transporter setup */
  tr->set_hostname(changeNodeId, &hostname_buf[0]);

  SetHostnameConf * const conf =
    CAST_PTR(SetHostnameConf, signal.getDataPtrSend());
  signal.theVerId_signalNumber   = GSN_SET_HOSTNAME_CONF;
  signal.theReceiversBlockNumber = refToMain(senderRef);
  signal.theTrace                = 0;
  signal.theLength               = SetHostnameConf::SignalLength;
  conf->senderRef = ref;
  conf->senderNodeId = theFacade.ownId();
  conf->changeNodeId = changeNodeId;
  safe_sendSignal(&signal, refToNode(senderRef));
  DEBUG_FPRINTF((stderr, "Send SET_HOSTNAME_CONF to %u about node %u"
                         ", new hostname: %s",
                 refToNode(senderRef),
                 changeNodeId,
                 hostname_buf));
}

#define USER_ID_HASH_SIZE 1024
void ClusterMgr::execLIST_DATABASE_CONF(const Uint32 * theData,
                                        const LinearSectionPtr ptr[3]) {
  DBUG_ENTER("ClusterMgr::execLIST_DATABASE_CONF");
  const ListDatabaseConf * const listDatabaseConf =
    (const ListDatabaseConf *)&theData[0];
  Uint32 senderRef = listDatabaseConf->senderRef;
  Uint32 databaseId = listDatabaseConf->databaseId;
  if (databaseId == RNIL) {
    /* No more users */
    m_initialised_user_id_cache = true;
    m_initialising_user_id_cache = false;
    DBUG_VOID_RETURN;
  }
  Uint32 databaseVersion = listDatabaseConf->databaseVersion;
  const char *username = (const char*)ptr[0].p;
  Uint32 username_len = strnlen(username, MAX_DB_NAME_SIZE);
  require(username_len < ptr[0].sz * 4);
  DBUG_PRINT("info", ("List user %s, id: %u, version: %u",
    username, databaseId, databaseVersion));
  if (username_len <= MAX_DB_NAME_SIZE) {
    int ret_code = insertUserId(username,
                                username_len,
                                databaseId,
                                databaseVersion);
    if (ret_code != 0) {
      /* Failed to build user id cache, fallback to request handling */
      DBUG_VOID_RETURN;
    }
  }
  Uint32 node_id = refToNode(senderRef);
  fillingUserIdCache(node_id, databaseId + 1);
  DBUG_VOID_RETURN;
}

void ClusterMgr::execDROP_DATABASE_REP(const Uint32 * theData,
                                       const LinearSectionPtr ptr[3]) {
  DBUG_ENTER("ClusterMgr::execDROP_DATABASE_REP");
  const DropDatabaseRep * const dropDatabaseRep =
    (const DropDatabaseRep *)&theData[0];
  Uint32 databaseId = dropDatabaseRep->databaseId;
  Uint32 databaseVersion = dropDatabaseRep->databaseVersion;
  Uint32 databaseNameLen = dropDatabaseRep->databaseNameLen;
  const char *username = (const char*)ptr[0].p;
  require(databaseNameLen < ptr[0].sz * 4);
  DBUG_PRINT("info", ("Drop user %s, id: %u, version: %u",
    username, databaseId, databaseVersion));
  if (databaseNameLen <= MAX_DB_NAME_SIZE) {
    deleteUserId(username,
                 databaseNameLen,
                 databaseId,
                 databaseVersion);
  }
  DBUG_VOID_RETURN;
}

void ClusterMgr::execCREATE_DATABASE_REP(const Uint32 * theData,
                                         const LinearSectionPtr ptr[3]) {
  DBUG_ENTER("ClusterMgr::execCREATE_DATABASE_REP");
  const CreateDatabaseRep * const createDatabaseRep =
    (const CreateDatabaseRep *)&theData[0];
  Uint32 databaseId = createDatabaseRep->databaseId;
  Uint32 databaseVersion = createDatabaseRep->databaseVersion;
  Uint32 databaseNameLen = createDatabaseRep->databaseNameLen;
  const char *username = (const char*)ptr[0].p;
  require(databaseNameLen < ptr[0].sz * 4);
  DBUG_PRINT("info", ("Create user %s, id: %u, version: %u",
    username, databaseId, databaseVersion));
  if (m_initialising_user_id_cache == true ||
      m_initialised_user_id_cache == true) {
    if (databaseNameLen <= MAX_DB_NAME_SIZE) {
      insertUserId(username,
                   databaseNameLen,
                   databaseId,
                   databaseVersion);
    }
  }
  DBUG_VOID_RETURN;
}

void ClusterMgr::fillingUserIdCache(Uint32 node_id, Uint32 nextDatabaseId) {
  DBUG_ENTER("ClusterMgr::fillingUserIdCache");
  Uint32 ref = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  NdbApiSignal tSignal(ref);
  ListDatabaseReq * const req =
    CAST_PTR(ListDatabaseReq, tSignal.getDataPtrSend());
  req->senderRef = ref;
  req->requestInfo = 1; //is_user
  req->nextDatabaseId = nextDatabaseId;
  safe_sendSignal(&tSignal, node_id);
  DBUG_VOID_RETURN;
}

void ClusterMgr::rateOverflowError(const char *username,
                                   Uint32 username_len) {
  DBUG_ENTER("ClusterMgr::rateOverflowError");
  /**
   * The RonDB data node reported a rate overflow error, in this case we
   * will give the user a 1 second pause where it cannot use the data node
   * until this second has passed. This will ensure that we won't overload
   * the data nodes with requests that will all be deemed rate overflow
   * errrors.
   */
  NdbMutex_Lock(theUserIdMutex);
  if (m_num_in_user_id_cache == RNIL) {
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_VOID_RETURN;
  }
  Uint32 hash_val = rondb_calc_hash_val(username,
                                        username_len,
                                        true);
  Uint32 inx = hash_val & (USER_ID_HASH_SIZE - 1);
  struct UserIdHashEntry *entry = theUserIdHash[inx];
  while (entry != nullptr) {
    if (entry->m_username_len != username_len ||
        entry->m_user_id == RNIL ||
        memcmp(entry->m_username, username, username_len) != 0) {
      entry = entry->next_entry;
      continue;
    }
    NDB_TICKS now = NdbTick_getCurrentTicks();
    entry->m_error_time = now;
    break;
  }
  NdbMutex_Unlock(theUserIdMutex);
  DBUG_VOID_RETURN;
}

int ClusterMgr::retrieveUserId(const char *username,
                               Uint32 username_len,
                               Uint32 &userId,
                               Uint32 &userIdVersion,
                               Uint32 node_id) {
  DBUG_ENTER("ClusterMgr::retrieveUserId");
  NdbMutex_Lock(theUserIdMutex);
  if (m_num_in_user_id_cache == RNIL) {
    userId = RNIL;
    userIdVersion = 0;
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_RETURN(0);
  }
  if (!ndbd_support_user_rate_limits(minDbVersion)) {
    userId = RNIL;
    userIdVersion = 0;
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_RETURN(0);
  }
  if (m_initialising_user_id_cache == false) {
    m_initialising_user_id_cache = true;
    fillingUserIdCache(node_id, 0);
  }
  Uint32 hash_val = rondb_calc_hash_val(username,
                                        username_len,
                                        true);
  Uint32 inx = hash_val & (USER_ID_HASH_SIZE - 1);
  struct UserIdHashEntry *entry = theUserIdHash[inx];
  bool first = true;
  while (entry != nullptr) {
    if (entry->m_username_len != username_len ||
        memcmp(entry->m_username, username, username_len) != 0) {
      entry = entry->next_entry;
      continue;
    }
    Uint32 user_id = entry->m_user_id;
    if (first == false || user_id != RNIL) {
      if (NdbTick_IsValid(entry->m_error_time) != 0) {
        NDB_TICKS now = NdbTick_getCurrentTicks();
        /* We had an overload error, check if error time has expired */
        if (NdbTick_Elapsed(entry->m_error_time, now).milliSec() > 1000) {
          NdbTick_Invalidate(&entry->m_error_time);
        } else {
          NdbMutex_Unlock(theUserIdMutex);
          return TcKeyRef::WriteRateOverflowError;
        }
      }
      userId = entry->m_user_id;
      userIdVersion = entry->m_user_id_version;
      NdbMutex_Unlock(theUserIdMutex);
      DBUG_RETURN(0);
    }
    entry->m_wait_for_entry = true;
    NdbCondition_WaitTimeout(theUserIdCond, theUserIdMutex, 10);
    entry = theUserIdHash[inx];
    first = false;
  }
  if (m_initialised_user_id_cache) {
    /**
     * When the user id cache is fully initialised we treat a missing
     * entry as no user with that name exists.
     */
    NdbMutex_Unlock(theUserIdMutex);
    userId = RNIL;
    userIdVersion = 0;
    DBUG_RETURN(0);
  }
  /**
   * We didn't find any entry, we will insert an entry into the hash
   * table, we will set user id to RNIL to indicate we are still
   * retrieving the user id from the RonDB data nodes.
   */
  int ret_code = insertUserId(username,
                              username_len,
                              RNIL,
                              0);
  if (ret_code != 0) {
    userId = RNIL;
    userIdVersion = 0;
    DBUG_RETURN(0);
  }
  /* User thread will send GET_DATABASE_REQ to get userId */
  DBUG_RETURN(1);
}

int ClusterMgr::insertUserId(const char *username,
                             Uint32 username_len,
                             Uint32 userId,
                             Uint32 userIdVersion) {
  DBUG_ENTER("ClusterMgr::insertUserId");
  NdbMutex_Lock(theUserIdMutex);
  if (m_num_in_user_id_cache == RNIL) {
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_RETURN(-1);
  }
  struct UserIdHashEntry *new_entry = (struct UserIdHashEntry*)
    malloc(sizeof(struct UserIdHashEntry) + username_len + 1);
  if (new_entry == nullptr) {
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_RETURN(-1);
  }
  Uint32 hash_val = rondb_calc_hash_val(username,
                                        username_len,
                                        true);
  Uint32 inx = hash_val & (USER_ID_HASH_SIZE - 1);
  new_entry->next_entry = theUserIdHash[inx];
  new_entry->m_username_len = username_len;
  new_entry->m_user_id = userId;
  new_entry->m_user_id_version = userIdVersion;
  new_entry->m_wait_for_entry = false;
  NdbTick_Invalidate(&new_entry->m_error_time);
  std::memcpy(&new_entry->m_username[0],
              username,
              username_len + 1);
  theUserIdHash[inx] = new_entry;
  NdbMutex_Lock(theUserIdMutex);
  DBUG_RETURN(0);
}

int ClusterMgr::updateUserId(const char *username,
                             Uint32 username_len,
                             Uint32 userId,
                             Uint32 userIdVersion) {
  DBUG_ENTER("ClusterMgr::updateUserId");
  NdbMutex_Lock(theUserIdMutex);
  if (m_num_in_user_id_cache == RNIL) {
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_RETURN(0);
  }
  Uint32 hash_val = rondb_calc_hash_val(username,
                                        username_len,
                                        true);
  Uint32 inx = hash_val & (USER_ID_HASH_SIZE - 1);
  UserIdHashEntry *entry = theUserIdHash[inx];
  while (entry != nullptr) {
    if (entry->m_username_len != username_len ||
        memcmp(entry->m_username, username, username_len) != 0) {
      entry = entry->next_entry;
      continue;
    } else {
      entry->m_user_id = userId;
      entry->m_user_id_version = userIdVersion;
      if (entry->m_wait_for_entry) {
        entry->m_wait_for_entry = false;
        NdbCondition_Broadcast(theUserIdCond);
      }
      NdbMutex_Unlock(theUserIdMutex);
      DBUG_RETURN(0);
    }
  }
  NdbMutex_Unlock(theUserIdMutex);
#ifdef DEBUG_USER
  g_eventLogger->info("Failed to update user id, "
                      "name: %s, namelen: %u, id: %u, version: %u",
    username,
    username_len,
    userId,
    userIdVersion);
#endif
  DBUG_RETURN(-1);
}

void ClusterMgr::deleteUserId(const char *username,
                              Uint32 username_len,
                              Uint32 userId,
                              Uint32 userIdVersion) {
  DBUG_ENTER("ClusterMgr::deleteUserId");
  if (m_num_in_user_id_cache == RNIL) {
    NdbMutex_Unlock(theUserIdMutex);
    DBUG_VOID_RETURN;
  }
  Uint32 hash_val = rondb_calc_hash_val(username,
                                        username_len,
                                        true);
  Uint32 inx = hash_val & (USER_ID_HASH_SIZE - 1);
  struct UserIdHashEntry *entry = theUserIdHash[inx];
  struct UserIdHashEntry *prev_entry = nullptr;
  while (entry != nullptr) {
    if (entry->m_username_len != username_len ||
        memcmp(entry->m_username, username, username_len) != 0) {
      prev_entry = entry;
      entry = entry->next_entry;
      continue;
    } else {
      if (entry->m_user_id == userId &&
          entry->m_user_id_version == userIdVersion) {
        if (entry->m_wait_for_entry) {
          entry->m_wait_for_entry = false;
          NdbCondition_Broadcast(theUserIdCond);
        }
        if (prev_entry == nullptr) {
          theUserIdHash[inx] = entry->next_entry;
        } else {
          prev_entry->next_entry = entry->next_entry;
        }
        NdbMutex_Unlock(theUserIdMutex);
        std::free(entry);
        DBUG_VOID_RETURN;
      }
      NdbMutex_Unlock(theUserIdMutex);
#ifdef DEBUG_USER
      g_eventLogger->info("deleteUserId found same name, different id, "
                          "name: %s, namelen: %u, id: %u, version: %u"
                          ", found userId: %u, version: %u",
                          username,
                          username_len,
                          userId,
                          userIdVersion,
                          entry->m_user_id,
                          entry->m_user_id_version);
#endif
      DBUG_VOID_RETURN;
    }
  }
  NdbMutex_Unlock(theUserIdMutex);
  DBUG_VOID_RETURN;
}

void ClusterMgr::createUserIdHash() {
  DBUG_ENTER("ClusterMgr::createUserIdHash");
  m_num_in_user_id_cache = 0;
  theUserIdMutex = NdbMutex_Create();
  theUserIdCond = NdbCondition_Create();
  theUserIdHash = (struct UserIdHashEntry**)
    std::calloc(1, sizeof(struct UserIdHashEntry*) * USER_ID_HASH_SIZE);
  if (theUserIdHash == nullptr) {
    m_num_in_user_id_cache = RNIL;
  }
  m_initialised_user_id_cache = false;
  m_initialising_user_id_cache = false;
  DBUG_VOID_RETURN;
}

void ClusterMgr::releaseUserIdHash() {
  DBUG_ENTER("ClusterMgr::releaseUserIdHash");
  NdbMutex_Lock(theUserIdMutex);
  m_num_in_user_id_cache = RNIL;
  if (theUserIdHash != nullptr) {
    for (Uint32 i = 0; i < USER_ID_HASH_SIZE; i++) {
      struct UserIdHashEntry *entry = theUserIdHash[i];
      while (entry != nullptr) {
        struct UserIdHashEntry *next_entry = entry->next_entry;
        std::free(entry);
        entry = next_entry;
      }
    }
    std::free(theUserIdHash);
    theUserIdHash = nullptr;
  }
  NdbMutex_Unlock(theUserIdMutex);
  NdbCondition_Destroy(theUserIdCond);
  NdbMutex_Destroy(theUserIdMutex);
  DBUG_VOID_RETURN;
}

/******************************************************************************
 * Send PROCESSINFO_REP
 ******************************************************************************/
void ClusterMgr::sendProcessInfoReport(NodeId nodeId) {
  LinearSectionPtr ptr[3];
  LinearSectionPtr &pathSection = ptr[ProcessInfoRep::PathSectionNum];
  LinearSectionPtr &hostSection = ptr[ProcessInfoRep::HostSectionNum];
  BlockReference ownRef = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  NdbApiSignal signal(ownRef);
  int nsections = 0;
  signal.theVerId_signalNumber = GSN_PROCESSINFO_REP;
  signal.theReceiversBlockNumber = QMGR;
  signal.theTrace = 0;
  signal.theLength = ProcessInfoRep::SignalLength;

  ProcessInfoRep *report = CAST_PTR(ProcessInfoRep, signal.getDataPtrSend());
  m_process_info->buildProcessInfoReport(report);

  const char *uri_path = m_process_info->getUriPath();
  pathSection.p = (const Uint32 *)uri_path;
  pathSection.sz = ProcessInfo::UriPathLengthInWords;
  if (uri_path[0]) {
    nsections = 1;
  }

  const char *hostAddress = m_process_info->getHostAddress();
  if (hostAddress[0]) {
    nsections = 2;
    hostSection.p = (const Uint32 *)hostAddress;
    hostSection.sz = ProcessInfo::AddressStringLengthInWords;
  }
  safe_noflush_sendSignal(&signal, nodeId, ptr, nsections);
}

/******************************************************************************
 * API_REGREQ and friends
 ******************************************************************************/

void ClusterMgr::execAPI_REGREQ(const Uint32 *theData) {
  const ApiRegReq *const apiRegReq = (const ApiRegReq *)&theData[0];
  const NodeId nodeId = refToNode(apiRegReq->ref);

#ifdef DEBUG_REG
  g_eventLogger->info("ClusterMgr: Recd API_REGREQ from node %d", nodeId);
#endif

  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);

  Node &cm_node = theNodes[nodeId];
  trp_node &node = cm_node;
  assert(node.defined == true);
  assert(node.is_connected() == true);

  /*
     API nodes send API_REGREQ once to themselves. Other than that, there are
     no API-API heart beats.
  */
  assert(cm_node.m_info.m_type != NodeInfo::API ||
         (nodeId == getOwnNodeId() && !cm_node.is_confirmed()));

  if (node.m_info.m_version != apiRegReq->version) {
    node.m_info.m_version = apiRegReq->version;
    node.m_info.m_mysql_version = apiRegReq->mysql_version;

    if (getMajor(node.m_info.m_version) < getMajor(NDB_VERSION) ||
        getMinor(node.m_info.m_version) < getMinor(NDB_VERSION)) {
      node.compatible = false;
    } else {
      node.compatible = true;
    }
  }

  NdbApiSignal signal(numberToRef(API_CLUSTERMGR, theFacade.ownId()));
  signal.theVerId_signalNumber = GSN_API_REGCONF;
  signal.theReceiversBlockNumber = API_CLUSTERMGR;
  signal.theTrace = 0;
  signal.theLength = ApiRegConf::SignalLength;

  ApiRegConf *const conf = CAST_PTR(ApiRegConf, signal.getDataPtrSend());
  conf->qmgrRef = numberToRef(API_CLUSTERMGR, theFacade.ownId());
  conf->version = NDB_VERSION;
  conf->mysql_version = NDB_MYSQL_VERSION_D;

  /*
    This is the interval (in centiseonds) at which we want the other node
    to send API_REGREQ messages.
  */
  conf->apiHeartbeatInterval = m_hbCheckInterval / 10;

  conf->minDbVersion = 0;
  conf->minApiVersion = 0;
  conf->nodeState = node.m_state;

  DEBUG_FPRINTF((stderr, "set_confirmed on node: %u\n", nodeId));
  node.set_confirmed(true);
  if (safe_sendSignal(&signal, nodeId) != 0) {
    DEBUG_FPRINTF((stderr, "reset_confirmed on node: %u\n", nodeId));
    node.set_confirmed(false);
  }
}

void ClusterMgr::execAPI_REGCONF(const NdbApiSignal *signal,
                                 const LinearSectionPtr ptr[]) {
  const ApiRegConf *apiRegConf =
      CAST_CONSTPTR(ApiRegConf, signal->getDataPtr());
  const NodeId nodeId = refToNode(apiRegConf->qmgrRef);

  DBUG_PRINT("info", ("API_REGCONF from node %u", nodeId));
#ifdef DEBUG_REG
  g_eventLogger->info("ClusterMgr: Recd API_REGCONF from node %d", nodeId);
#endif

  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);

  Node & cm_node = theNodes[nodeId];
  trp_node & node = cm_node;
  bool prev_compatible = node.compatible;
  assert(node.defined == true);
  assert(node.is_connected() == true);

  if (node.m_info.m_version != apiRegConf->version) {
    DBUG_PRINT("info", ("Incompatible versions"));
    node.m_info.m_version = apiRegConf->version;
    node.m_info.m_mysql_version = apiRegConf->mysql_version;

    if (theNodes[theFacade.ownId()].m_info.m_type == NodeInfo::MGM)
      node.compatible =
          ndbCompatible_mgmt_ndb(NDB_VERSION, node.m_info.m_version);
    else
      node.compatible =
          ndbCompatible_api_ndb(NDB_VERSION, node.m_info.m_version);
  }

  DEBUG_FPRINTF((stderr, "2:set_confirmed on node %u\n", nodeId));

  node.set_confirmed(true);

  if (node.minDbVersion != apiRegConf->minDbVersion) {
    node.minDbVersion = apiRegConf->minDbVersion;
    recalcMinDbVersion();
  }

  if (ndbd_send_min_api_version(apiRegConf->mysql_version) &&
      node.minApiVersion != apiRegConf->minApiVersion) {
    node.minApiVersion = apiRegConf->minApiVersion;
    recalcMinApiVersion();
  }


  node.m_state = apiRegConf->nodeState;

  if (node.m_info.m_type == NodeInfo::DB) {
    /**
     * Only set DB nodes to "alive"
     */
    DBUG_PRINT("info", ("DB node, startLevel: %u, singleMode: %u",
      node.m_state.startLevel, node.m_state.getSingleUserMode()));
    /**
     * A data node that reports SL_STARTING with start phase >= 110 is
     * parked at the restart barrier (RONDB-1096): it is fully
     * recovered and accepts transactions although it does not yet
     * report started (which keeps e.g. Kubernetes orchestration
     * waiting for it). Treat such a node as alive. Version guarded:
     * only data nodes that support the restart barrier accept remote
     * TCSEIZEREQ in this state, older nodes would refuse with error
     * 203.
     */
    const bool recovered_at_barrier =
      node.m_state.startLevel == NodeState::SL_STARTING &&
      node.m_state.getNodeRecovered() &&
      ndbd_restart_phase_110_barrier(node.m_info.m_version);
    if (node.compatible && (node.m_state.startLevel == NodeState::SL_STARTED ||
                            node.m_state.getSingleUserMode() ||
                            recovered_at_barrier))
    {
      NdbMutex_Lock(m_node_state_mutex);
      if (!get_node_alive(node))
      {
        m_state_changed = true;
        m_node_change_count++;
        if (m_error_print)
        {
          char our_buf[128];
          char node_buf[128];
          const char *started_ptr =
            (node.m_state.startLevel == NodeState::SL_STARTED) ?
              "started" : (recovered_at_barrier ?
                "recovered, waiting at the restart barrier" :
                "in single user mode");
          const char *our_version_ptr =
            ndbGetVersionString(NDB_VERSION,
                                0,
                                nullptr,
                                our_buf,
                                sizeof(our_buf));
          const char *node_version_ptr =
            ndbGetVersionString(node.m_info.m_version,
                                0,
                                nullptr,
                                node_buf,
                                sizeof(node_buf));
          error_printer("(N%u) Node %u is now alive, Our version: %s is compatible"
                        " with node version: %s, node is %s",
                        getOwnNodeId(),
                        nodeId,
                        our_version_ptr,
                        node_version_ptr,
                        started_ptr);
        }
      }
      set_node_alive(node, true);
      NdbMutex_Unlock(m_node_state_mutex);
    }
    else
    {
      NdbMutex_Lock(m_node_state_mutex);
      if (get_node_alive(node))
      {
        m_state_changed = true;
        m_node_change_count++;
        if (m_error_print)
        {
          char our_buf[128];
          char node_buf[128];
          Uint32 startLevel = node.m_state.startLevel;
          const char *compatible_ptr =
            (prev_compatible) ?
              "compatible" : "incompatible";
          const char *our_version_ptr =
            ndbGetVersionString(NDB_VERSION,
                                0,
                                nullptr,
                                our_buf,
                                sizeof(our_buf));
          const char *node_version_ptr =
            ndbGetVersionString(node.m_info.m_version,
                                0,
                                nullptr,
                                node_buf,
                                sizeof(node_buf));
          const char *start_level_ptr =
            (startLevel == NodeState::SL_NOTHING) ? "NOTHING" :
            (startLevel == NodeState::SL_CMVMI) ? "CMVMI started" :
            (startLevel == NodeState::SL_STARTING) ? "STARTING" :
            (startLevel == NodeState::SL_STARTED) ? "STARTED" :
            (startLevel == NodeState::SL_SINGLEUSER) ? "SINGLE USER" :
            (startLevel == NodeState::SL_STOPPING_1) ? "STOPPING_1" :
            (startLevel == NodeState::SL_STOPPING_2) ? "STOPPING_2" :
            (startLevel == NodeState::SL_STOPPING_3) ? "STOPPING_3" :
            (startLevel == NodeState::SL_STOPPING_4) ? "STOPPING_4" :
              "unknown";

          error_printer("(N%u) Node %u is now dead, but connected,"
                        " our version: %s, Node version: %s, "
                        "previously node had a %s version, "
                        "startLevel: %s",
                        getOwnNodeId(),
                        nodeId,
                        our_version_ptr,
                        node_version_ptr,
                        compatible_ptr,
                        start_level_ptr);
        }
      }
      set_node_alive(node, false);
      NdbMutex_Unlock(m_node_state_mutex);
    }
  }

  cm_node.hbMissed = 0;
  /*
    By convention, conf->apiHeartbeatInterval is in centiseconds rather than
    milliseconds. See also Qmgr::sendApiRegConf().
   */
  Int64 interval = static_cast<Int64>(apiRegConf->apiHeartbeatInterval) * 10;

  if (interval > UINT_MAX32) {
    // In case of overflow.
    assert(false); /* Note this assert fails on some upgrades... */
    interval = UINT_MAX32;
  } else if (interval < minHeartBeatInterval) {
    /**
     * We use minHeartBeatInterval as a lower limit. This also prevents
     * against underflow.
     */
    interval = minHeartBeatInterval;
  }
  if (cm_node.hbCheckInterval == 0) {
    // Initiate nextHbCheck and nextHbSend
    NDB_TICKS now = NdbTick_getCurrentTicks();
    cm_node.hbCheckInterval = interval;
    cm_node.nextHbCheck = NdbTick_AddMilliseconds(now, cm_node.hbCheckInterval);
    unsigned send_interval = get_send_heartbeat_interval(cm_node);
    if (send_interval > 0)
      cm_node.nextHbSend = NdbTick_AddMilliseconds(now, send_interval);
  } else if (cm_node.hbCheckInterval != interval) {
    // Adjust nextHbCheck and nextHbSend
    Int64 old_send_interval = get_send_heartbeat_interval(cm_node);
    cm_node.hbCheckInterval = interval;
    Int64 new_send_interval = get_send_heartbeat_interval(cm_node);
    cm_node.nextHbCheck = NdbTick_AddMilliseconds(
        cm_node.nextHbCheck, interval - cm_node.hbCheckInterval);
    if (cm_node.hbCheckInterval == 0)
      NdbTick_Invalidate(&cm_node.nextHbSend);
    else {
      cm_node.nextHbSend = NdbTick_AddMilliseconds(
          cm_node.nextHbSend, new_send_interval - old_send_interval);
    }
  }

  // If responding nodes indicates that it is connected to other
  // nodes, that makes it probable that those nodes are alive and
  // available also for this node.
  for (int db_node_id = 1; db_node_id <= MAX_DATA_NODE_ID; db_node_id++) {
    if (node.m_state.m_connected_nodes.get(db_node_id)) {
      // Tell this nodes start clients thread that db_node_id
      // is up and probable connectable.
      theFacade.theTransporterRegistry->indicate_node_up(db_node_id);
    }
  }

  /* Send ProcessInfo Report to a newly connected DB node */
  if (cm_node.m_info.m_type == NodeInfo::DB &&
      ndbd_supports_processinfo(cm_node.m_info.m_version) &&
      (!cm_node.processInfoSent)) {
    sendProcessInfoReport(nodeId);
    cm_node.processInfoSent = true;
  }

  // Distribute signal to all threads/blocks
  // TODO only if state changed...
  theFacade.for_each(this, signal, ptr);
}

void ClusterMgr::execAPI_REGREF(const Uint32 *theData) {
  const ApiRegRef *ref = (const ApiRegRef *)theData;

  const NodeId nodeId = refToNode(ref->ref);

  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);

  Node &cm_node = theNodes[nodeId];
  trp_node &node = cm_node;

  assert(node.is_connected() == true);
  assert(node.defined == true);
  /* Only DB nodes will send API_REGREF */
  assert(node.m_info.getType() == NodeInfo::DB);

  NdbMutex_Lock(m_node_state_mutex);
  if (node.compatible || get_node_alive(node))
  {
    char buf[128];
    const char *version_ptr = ndbGetVersionString(ref->version,
                                                  0,
                                                  nullptr,
                                                  buf,
                                                  sizeof(buf));
    m_state_changed = true;
    m_node_change_count++;
    if (m_error_print)
    {
      error_printer("(N%u) API_REGREF from node %u version %s",
                    getOwnNodeId(),
                    nodeId,
                    version_ptr);
    }
  }
  node.compatible = false;
  set_node_alive(node, false);
  node.m_state = NodeState::SL_NOTHING;
  node.m_info.m_version = ref->version;
  NdbMutex_Unlock(m_node_state_mutex);

  switch (ref->errorCode) {
    case ApiRegRef::WrongType:
      fprintf(stderr,
              "%s NDBAPI FATAL ERROR : Node %d reports that "
              "this node %d should be an NDB node\n",
              Logger::Timestamp().c_str(), nodeId, getOwnNodeId());
      abort();
    case ApiRegRef::UnsupportedVersion:
    default:
      break;
  }
}

void ClusterMgr::execDUMP_STATE_ORD(const NdbApiSignal *signal,
                                    const LinearSectionPtr ptr[]) {
  const Uint32 *data = signal->getDataPtr();
  const Uint32 length = signal->getLength();
  if (length < 1) {
    return;
  }
  switch (data[0]) {
    case DumpStateOrd::CmvmiDummySignal: {
      /* Log in event logger that signal sent by dump command
       * CmvmiSendDummySignal is received.  Include information about
       * signal size and its sections and which node sent it.
       *
       * Use rep node as reporting node, typically a data node.
       */
      const Uint32 rep_node_id = data[1];
      const Uint32 node_id = data[2];
      const Uint32 num_secs = signal->m_noOfSections;
      char msg[24 * 4];
      snprintf(msg, sizeof(msg),
               "Receiving CmvmiDummySignal"
               " (size %u+%u+%u+%u+%u) from %u to %u.",
               length, num_secs, (num_secs > 0) ? ptr[0].sz : 0,
               (num_secs > 1) ? ptr[1].sz : 0, (num_secs > 2) ? ptr[2].sz : 0,
               node_id, getOwnNodeId());
      const Uint32 len = strlen(msg) + 1;
      assert(len <= 24 * 4);
      NdbApiSignal aSignal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
      aSignal.theTrace = TestOrd::TraceAPI;
      aSignal.theReceiversBlockNumber = CMVMI;
      aSignal.theVerId_signalNumber = GSN_EVENT_REP;
      aSignal.theLength = ((len + 3) / 4) + 1;
      Uint32 *data = aSignal.getDataPtrSend();
      data[0] = NDB_LE_InfoEvent;
      memcpy(&data[1], msg, len);
      safe_sendSignal(&aSignal, rep_node_id);
      return;
    }
    case DumpStateOrd::CmvmiSendDummySignal: {
      /* Send a CmvmiDummySignal to specified node with specified size and
       * sections.  This is used to verify that messages with certain
       * signal sizes and sections can be sent and received.
       *
       * The sending is also logged in event logger.  This log entry should
       * be matched with corresponding log when receiving the
       * CmvmiDummySignal dump command.  See preceding dump command above.
       *
       * args: rep-node dest-node padding frag-size
       *       #secs sec#1-len sec#2-len sec#3-len
       */
      if (length < 5) {
        // Not enough words to send a dummy signal
        return;
      }
      const Uint32 rep_node_id = data[1];
      const Uint32 node_id = data[2];
      const Uint32 fill_word = data[3];
      const Uint32 frag_size = data[4];
      if (frag_size != 0) {
        // Fragmented signals are not supported yet.
        return;
      }
      const Uint32 num_secs = (length > 5) ? data[5] : 0;
      if (num_secs > 3) {
        return;
      }
      LinearSectionPtr ptr[3];
      Uint32 sec_max_len = 0;
      for (Uint32 i = 0; i < num_secs; i++) {
        const Uint32 sec_len = data[6 + i];
        if (sec_len > sec_max_len) {
          sec_max_len = sec_len;
        }
        ptr[i].sz = sec_len;
      }
      Uint32 *dummy_data = new Uint32[sec_max_len];
      for (Uint32 i = 0; i < sec_max_len; i++) {
        dummy_data[i] = fill_word;
      }
      for (Uint32 i = 0; i < num_secs; i++) {
        ptr[i].p = dummy_data;
      }
      for (Uint32 i = num_secs; i < 3; i++) {
        ptr[i].sz = 0;
        ptr[i].p = nullptr;
      }
      NdbApiSignal dummy_signal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
      Uint32 *dummy_sigdata = dummy_signal.getDataPtrSend();
      dummy_sigdata[0] = DumpStateOrd::CmvmiDummySignal;
      for (Uint32 i = 1; i < length; i++) {
        dummy_sigdata[i] = data[i];
      }
      dummy_sigdata[2] = getOwnNodeId();
      dummy_signal.theVerId_signalNumber = GSN_DUMP_STATE_ORD;
      const trp_node &theNode = theNodes[node_id];
      dummy_signal.theReceiversBlockNumber =
          (theNode.m_info.m_type == NodeInfo::DB) ? CMVMI : API_CLUSTERMGR;
      dummy_signal.theTrace = 0;
      dummy_signal.theLength = length;
      dummy_signal.m_noOfSections = num_secs;
      safe_sendSignal(&dummy_signal, node_id, ptr, num_secs);
      delete[] dummy_data;

      /* Send event log about the sending of CmvmiDummySignal.
       * Use rep node as reporting node, typically a data node.
       */
      char msg[24 * sizeof(Uint32)];
      snprintf(msg, sizeof(msg),
               "Sending CmvmiDummySignal"
               " (size %u+%u+%u+%u+%u) from %u to %u.",
               length, num_secs, ptr[0].sz, ptr[1].sz, ptr[2].sz,
               getOwnNodeId(), node_id);
      const Uint32 len = strlen(msg) + 1;
      assert(len <= 24 * 4);
      NdbApiSignal aSignal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
      aSignal.theTrace = TestOrd::TraceAPI;
      aSignal.theReceiversBlockNumber = CMVMI;
      aSignal.theVerId_signalNumber = GSN_EVENT_REP;
      aSignal.theLength = (Uint32)((len + 3) / 4) + 1;
      Uint32 *data = aSignal.getDataPtrSend();
      data[0] = NDB_LE_InfoEvent;
      memcpy(&data[1], msg, len);
      safe_sendSignal(&aSignal, rep_node_id);
      return;
    }
    default:
      return;
  }
}

void ClusterMgr::execNF_COMPLETEREP(const NdbApiSignal *signal,
                                    const LinearSectionPtr ptr[3]) {
  const NFCompleteRep *nfComp =
      CAST_CONSTPTR(NFCompleteRep, signal->getDataPtr());
  const NodeId nodeId = nfComp->failedNodeId;
  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);

  trp_node &node = theNodes[nodeId];
  if (node.nfCompleteRep == false) {
    node.nfCompleteRep = true;
    theFacade.for_each(this, signal, ptr);
  }
}

/**
 * ::reportConnected() and ::reportDisconnected()
 *
 * Should be called from the client thread being the poll owner,
 * which could either be ClusterMgr itself, or another API client.
 *
 * As ClusterMgr maintains shared global data, updating
 * its connection state needs m_mutex being locked.
 * If ClusterMgr is the poll owner, it already owns that
 * lock, else it has to be locked now.
 */
void ClusterMgr::reportConnected(NodeId nodeId) {
  DBUG_ENTER("ClusterMgr::reportConnected");
  DBUG_PRINT("info", ("nodeId: %u", nodeId));
  assert(theFacade.is_poll_owner_thread());

  if (theFacade.m_poll_owner != this) lock();

  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);
  if (nodeId != getOwnNodeId()) {
    noOfConnectedNodes++;
  }

  Node &cm_node = theNodes[nodeId];
  trp_node &theNode = cm_node;

  if (theNode.m_info.m_type == NodeInfo::DB) {
    noOfConnectedDBNodes++;
    if (noOfConnectedDBNodes == 1) {
      // Data node connected, use ConnectBackoffMaxTime
      theFacade.get_registry()->set_connect_backoff_max_time_in_ms(
        connect_backoff_max_time);
    }
    NdbMutex_Lock(m_node_state_mutex);
    m_state_changed = true;
    if (m_error_print)
    {
      error_printer("(N%u) Node %d Connected", getOwnNodeId(), nodeId);
    }
    m_ever_connected = true;
    NdbMutex_Unlock(m_node_state_mutex);
  }

  /**
   * Ensure that we are sending heartbeat every 100 ms
   * until we have got the first reply from NDB providing
   * us with the real time-out period to use.
   */
  cm_node.hbMissed = 0;
  cm_node.hbCheckInterval = 0;
  NdbTick_Invalidate(&cm_node.nextHbSend);
  NdbTick_Invalidate(&cm_node.nextHbCheck);
  cm_node.processInfoSent = false;

  assert(theNode.is_connected() == false);

  /**
   * make sure the node itself is marked connected even
   * if first API_REGCONF has not arrived
   */
  DEBUG_FPRINTF((stderr, "(%u)theNode.set_connected(true) for node: %u\n",
                 getOwnNodeId(), nodeId));
  theNode.set_connected(true);
  //theNode.m_state.m_connected_nodes.set(nodeId);
  theNode.m_info.m_version = 0;
  theNode.compatible = true;
  theNode.nfCompleteRep = true;
  theNode.m_node_fail_rep = false;
  theNode.m_state.startLevel = NodeState::SL_NOTHING;
  theNode.minDbVersion = 0;
  theNode.minApiVersion = 0;

  /**
   * End of protected ClusterMgr updates of shared global data.
   * Informing other API client does not need a global protection
   */
  if (theFacade.m_poll_owner != this) unlock();

  /**
   * We are called by the poll owner (asserted above), so we can
   * tell each API client about the CONNECT_REP ourself.
   */
  NdbApiSignal signal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
  signal.theVerId_signalNumber = GSN_CONNECT_REP;
  signal.theReceiversBlockNumber = API_CLUSTERMGR;
  signal.theTrace = 0;
  signal.theLength = 1;
  signal.getDataPtrSend()[0] = nodeId;
  theFacade.for_each(this, &signal, nullptr);
  DBUG_VOID_RETURN;
}

void ClusterMgr::reportDisconnected(NodeId nodeId) {
  assert(theFacade.is_poll_owner_thread());
  assert(nodeId > 0 && nodeId < ABS_MAX_NODES);
  if (theFacade.m_poll_owner != this)
    lock();

  Node &cm_node = theNodes[nodeId];
  trp_node &theNode = cm_node;

  const bool node_failrep = theNode.m_node_fail_rep;
  const bool node_connected = theNode.is_connected();
  DEBUG_FPRINTF((stderr, "(%u)theNode.set_connected(false) for node: %u\n",
                 getOwnNodeId(), nodeId));
  theNode.set_connected(false);

  /**
   * Remaining processing should only be done if the node
   * actually completed connecting...
   */
  if (unlikely(!node_connected))
  {
    set_node_dead(theNode);
    if (theFacade.m_poll_owner != this)
      unlock();
    return;
  }

  assert(noOfConnectedNodes > 0);

  noOfConnectedNodes--;
  if (noOfConnectedNodes == 0) {
    if (!global_flag_skip_invalidate_cache && theFacade.m_globalDictCache) {
      theFacade.m_globalDictCache->lock();
      theFacade.m_globalDictCache->invalidate_all();
      theFacade.m_globalDictCache->unlock();
      m_connect_count++;
      m_cluster_state = CS_waiting_for_clean_cache;
    }

    if (m_auto_reconnect == 0) {
      theStop = 2;
    }
  }

  if (theNode.m_info.m_type == NodeInfo::DB) {
    assert(noOfConnectedDBNodes > 0);
    noOfConnectedDBNodes--;
    if (noOfConnectedDBNodes == 0) {
      // No data nodes connected, use StartConnectBackoffMaxTime
      theFacade.get_registry()->set_connect_backoff_max_time_in_ms(
        start_connect_backoff_max_time);
    }
    NdbMutex_Lock(m_node_state_mutex);
    if (get_node_alive(theNode))
    {
      m_state_changed = true;
      m_node_change_count++;
    }
    set_node_dead(theNode);
    if (m_error_print)
    {
      error_printer("(N%u) Node %d Disconnected", getOwnNodeId(), nodeId);
    }
    NdbMutex_Unlock(m_node_state_mutex);
  }
  else
  {
    set_node_dead(theNode);
  }

  /**
   * End of protected ClusterMgr updates of shared global data.
   * Informing other API client does not need a global protection
   */
  if (theFacade.m_poll_owner != this) unlock();

  if (node_failrep == false) {
    /**
     * Inform API
     *
     * We are called by the poll owner (asserted above), so we can
     * tell each API client about the NODE_FAILREP ourself.
     */
    NdbApiSignal signal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
    signal.theVerId_signalNumber = GSN_NODE_FAILREP;
    signal.theReceiversBlockNumber = API_CLUSTERMGR;
    signal.theTrace = 0;
    signal.theLength = NodeFailRep::SignalLengthLong;
    signal.m_noOfSections = 1;

    Uint32 *signalData = signal.getDataPtrSend();
    signalData[NodeFailRep::FailNoIndex] = 0;
    signalData[NodeFailRep::MasterNodeIdIndex] = 0;
    signalData[NodeFailRep::NoOfNodesIndex] = 1;
    Uint32 theAllNodes[NodeBitmask::Size];
    NodeBitmask::clear(theAllNodes);
    NodeBitmask::set(theAllNodes, nodeId);
    LinearSectionPtr lsptr[3];
    lsptr[0].p = theAllNodes;
    lsptr[0].sz = NodeBitmask::getPackedLengthInWords(theAllNodes);
    execNODE_FAILREP(&signal, lsptr);
  }
}

void ClusterMgr::execNODE_FAILREP(const NdbApiSignal *sig,
                                  const LinearSectionPtr ptr[]) {
  DBUG_ENTER("ClusterMgr::execNODE_FAILREP");
  const NodeFailRep *rep = CAST_CONSTPTR(NodeFailRep, sig->getDataPtr());
  NodeBitmask mask;
  if (sig->getLength() == NodeFailRep::SignalLengthLong_v1) {
    mask.assign(NodeBitmask::Size, rep->theAllNodes);
  } else if (sig->getLength() == NodeFailRep::SignalLength_v1) {
    mask.assign(NdbNodeBitmask48::Size, rep->theNodes);
  } else {
    assert(sig->m_noOfSections == 1);
    mask.assign(ptr[0].sz, ptr[0].p);
  }

  NdbApiSignal signal(sig->theSendersBlockRef);
  signal.theVerId_signalNumber = GSN_NODE_FAILREP;
  signal.theReceiversBlockNumber = API_CLUSTERMGR;
  signal.theTrace = 0;
  signal.theLength = NodeFailRep::SignalLengthLong;
  signal.m_noOfSections = 1;

  Uint32 *copyData = signal.getDataPtrSend();
  copyData[NodeFailRep::FailNoIndex] = 0;
  copyData[NodeFailRep::MasterNodeIdIndex] = 0;
  Uint32 noOfNodes = 0;
  Uint32 theAllNodes[NodeBitmask::Size];
  NodeBitmask::clear(theAllNodes);

  for (Uint32 i = mask.find_first(); i != NodeBitmask::NotFound;
       i = mask.find_next(i + 1)) {
    Node &cm_node = theNodes[i];
    trp_node &theNode = cm_node;

    bool node_failrep = theNode.m_node_fail_rep;
    bool connected = theNode.is_connected();
    NdbMutex_Lock(m_node_state_mutex);
    if (get_node_alive(theNode))
    {
      m_state_changed = true;
      m_node_change_count++;
      if (m_error_print)
      {
        error_printer("(N%u) Node %u is dead due to missed heartbeats",
                      getOwnNodeId(),
                      i);
      }
    }
    DBUG_PRINT("info", ("set_node_dead(%u), connected: %u",
      i, connected));
    set_node_dead(theNode);
    NdbMutex_Unlock(m_node_state_mutex);

    if (node_failrep == false) {
      theNode.m_node_fail_rep = true;
      NodeBitmask::set(theAllNodes, i);
      noOfNodes++;
    }

    if (connected) {
      theFacade.startDisconnecting(i);
    }
  }

  recalcMinDbVersion();
  recalcMinApiVersion();
  if (noOfNodes) {
    copyData[NodeFailRep::NoOfNodesIndex] = noOfNodes;
    LinearSectionPtr lsptr[3];
    lsptr[0].p = theAllNodes;
    lsptr[0].sz = NodeBitmask::getPackedLengthInWords(theAllNodes);
    theFacade.for_each(this, &signal, lsptr);  // report GSN_NODE_FAILREP
  }

  if (noOfAliveNodes == 0) {
    NdbApiSignal signal(numberToRef(API_CLUSTERMGR, getOwnNodeId()));
    signal.theVerId_signalNumber = GSN_NF_COMPLETEREP;
    signal.theReceiversBlockNumber = 0;
    signal.theTrace = 0;
    signal.theLength = NFCompleteRep::SignalLength;

    NFCompleteRep *rep = CAST_PTR(NFCompleteRep, signal.getDataPtrSend());
    rep->blockNo = 0;
    rep->nodeId = getOwnNodeId();
    rep->unused = 0;
    rep->from = __LINE__;

    for (Uint32 i = 1; i < ABS_MAX_NODES; i++) {
      trp_node &theNode = theNodes[i];
      if (theNode.defined && theNode.nfCompleteRep == false) {
        rep->failedNodeId = i;
        execNF_COMPLETEREP(&signal, nullptr);
      }
    }
  }
  DBUG_VOID_RETURN;
}

void ClusterMgr::set_node_dead(trp_node &theNode) {
  set_node_alive(theNode, false);
  theNode.set_confirmed(false);
  theNode.m_state.m_connected_nodes.clear();
  theNode.m_state.startLevel = NodeState::SL_NOTHING;
  theNode.m_info.m_connectCount++;
  theNode.nfCompleteRep = false;
}

void
ClusterMgr::is_cluster_completely_unavailable(Int32 &error,
                                              Uint32 line)
{
  Uint32 num_defined_nodes = 0;
  Uint32 num_single_user_nodes = 0;
  Uint32 num_stopping_nodes = 0;
  Uint32 num_incompatible_nodes = 0;
  Uint32 num_alive_nodes = 0;
  Uint32 num_starting_nodes = 0;
  Uint32 num_started_nodes = 0;
  /**
   * This method (and several other 'node state getters') allow
   * reading of theNodes[] from multiple block threads while
   * ClusterMgr concurrently updates them. Thus, a mutex should
   * have been expected here. See bug#20391191, and addendum patches
   * to bug#19524096, to understand what prevents us from locking (yet)
   */
  NdbMutex_Lock(m_node_state_mutex);
  for (NodeId n = 1; n < ABS_MAX_NDB_NODES ; n++)
  {
    const trp_node& node = theNodes[n];
    if (!node.defined)
    {
      /**
       * Node isn't even part of configuration.
       */
      continue;
    }
    if (node.m_info.m_type != NodeInfo::DB)
    {
      /**
       * Ignore API and MGM nodes
       */
      continue;
    }
    num_defined_nodes++;
    if (node.m_state.startLevel == NodeState::SL_SINGLEUSER)
    {
      num_single_user_nodes++;
      continue;
    }
    if (node.m_state.startLevel > NodeState::SL_SINGLEUSER)
    {
      /**
       * Node is stopping, so isn't available for any transactions,
       * so not available for us to use.
       */
      num_stopping_nodes++;
      continue;
    }
    if (!node.compatible) {
      /**
       * The node isn't compatible with ours, so we can't use it
       */
      num_incompatible_nodes++;
      continue;
    }
    if (node.m_alive)
    {
      num_alive_nodes++;
      continue;
    }
    if (node.m_state.startLevel == NodeState::SL_STARTING)
    {
      num_starting_nodes++;
      continue;
    }
    if (node.m_state.startLevel == NodeState::SL_STARTED)
    {
      num_started_nodes++;
      continue;
    }
  }
  if (num_alive_nodes > 0)
  {
    /**
     * We have alive nodes, but could not find any connection records
     */
    error = 4036;
  }
  else if (num_started_nodes > 0)
  {
    /**
     * We have no alive nodes, but we have started nodes, weird state
     */
    error = 4035;
  }
  else if (num_starting_nodes > 0)
  {
    /**
     * We have nodes that are starting up
     */
    error = 4037;
  }
  else if (num_incompatible_nodes > 0)
  {
    /**
     * We have alive nodes that are using an incompatible version
     */
    error = 4038;
  }
  else if (num_single_user_nodes > 0)
  {
    /**
     * We have alive nodes that are in single user mode.
     */
    error = 4041;
  }
  else if (num_stopping_nodes > 0)
  {
    /**
     * Accessible nodes are shutting down
     */
    error = 4039;
  }
  else if (m_ever_connected)
  {
    /**
     * No data nodes are available, but we have connected at least one
     * node in this cluster. Most likely cluster is down.
     */
    error = 4009;
  }
  else
  {
    /**
     * No data nodes are available and neither have any ever connected.
     * Most likely a firewall problem.
     */
    error = 4040;
  }
  if (m_error_print && m_state_changed)
  {
    m_state_changed = false;
    const char *ever_connected_ptr =
      (m_ever_connected) ?
      "nodes have been connected" : "no node ever connected";
    error_printer("(N%u) Reported %d, line: %u error with %u defined nodes, "
                  " %u alive nodes, %u started"
                  " nodes, %u starting nodes, %u incompatible nodes, "
                  "%u stopping nodes, %u nodes in single user mode, %s",
                  getOwnNodeId(),
                  error,
                  line,
                  num_defined_nodes,
                  num_alive_nodes,
                  num_started_nodes,
                  num_starting_nodes,
                  num_incompatible_nodes,
                  num_stopping_nodes,
                  num_single_user_nodes,
                  ever_connected_ptr);
  }
  NdbMutex_Unlock(m_node_state_mutex);
}

void ClusterMgr::print_nodes(const char *where, NdbOut &out) {
  out << where << " >>" << endl;
  for (NodeId n = 1; n < ABS_MAX_NODES; n++) {
    const trp_node node = getNodeInfo(n);
    if (!node.defined) continue;
    out << "node: " << n << endl;
    out << " -";
    out << " connected: " << node.is_connected();
    out << ", compatible: " << node.compatible;
    out << ", nf_complete_rep: " << node.nfCompleteRep;
    out << ", alive: " << node.m_alive;
    out << ", confirmed: " << node.is_confirmed();
    out << endl;

    out << " - " << node.m_info << endl;
    out << " - " << node.m_state << endl;
  }
  out << "<<" << endl;
}

int ClusterMgr::db_nodes_all_alive() {
  int all_alive = 1;
  TransporterRegistry *tr = theFacade.get_registry();
  for (NodeId n = 1; n < ABS_MAX_NODES; n++) {
    const trp_node node = getNodeInfo(n);
    if (!node.defined) continue;
    /*
     * Note:
     * We don’t use node.m_node_active to determine whether the node is
     * activated or deactivated, because the node info in ClusterMgr
     * may be outdated.
     */
    if (node.m_info.getType() == NODE_TYPE_DB
        && tr->get_active_node(n) &&!node.m_alive) {
      all_alive = 0;
      break;
    }
  }
  return all_alive;
}

void ClusterMgr::setProcessInfoUri(const char *scheme,
                                   const char *address_string,
                                   int port,
                                   const char *path) {
  Guard g(clusterMgrThreadMutex);

  m_process_info->setUriScheme(scheme);
  m_process_info->setHostAddress(address_string);
  m_process_info->setPort(port);
  m_process_info->setUriPath(path);

  /* Set flag to resend ProcessInfo Report */
  for (int i = 1; i < ABS_MAX_NODES; i++) {
    Node &node = theNodes[i];
    if (node.is_connected()) node.processInfoSent = false;
  }
}

/******************************************************************************
 * Arbitrator
 ******************************************************************************/
ArbitMgr::ArbitMgr(ClusterMgr &c)
    : m_clusterMgr(c),
      theRank(0),
      theDelay(0),
      theThread(nullptr),
      theInputTimeout(0),
      theInputFull(false),
      theInputBuffer(),
      theState(StateInit),
      theStartReq(),
      theChooseReq1(),
      theChooseReq2(),
      theStopOrd() {
  DBUG_ENTER("ArbitMgr::ArbitMgr");

  theThreadMutex = NdbMutex_Create();
  theInputCond = NdbCondition_Create();
  theInputMutex = NdbMutex_Create();

  DBUG_VOID_RETURN;
}

ArbitMgr::~ArbitMgr() {
  DBUG_ENTER("ArbitMgr::~ArbitMgr");
  NdbMutex_Destroy(theThreadMutex);
  NdbCondition_Destroy(theInputCond);
  NdbMutex_Destroy(theInputMutex);
  DBUG_VOID_RETURN;
}

// Start arbitrator thread.  This is kernel request.
// First stop any previous thread since it is a left-over
// which was never used and which now has wrong ticket.
void ArbitMgr::doStart(const Uint32 *theData) {
  DBUG_ENTER("ArbitMgr::doStart");
  ArbitSignal aSignal;
  NdbMutex_Lock(theThreadMutex);
  if (theThread != nullptr) {
    aSignal.init(GSN_ARBIT_STOPORD, nullptr);
    aSignal.data.code = StopRestart;
    sendSignalToThread(aSignal);
    void *value;
    NdbThread_WaitFor(theThread, &value);
    NdbThread_Destroy(&theThread);
    theState = StateInit;
    theInputFull = false;
  }
  aSignal.init(GSN_ARBIT_STARTREQ, theData);
  sendSignalToThread(aSignal);
  theThread = NdbThread_Create(runArbitMgr_C, (void **)this,
                               0,  // default stack size
                               "ndb_arbitmgr", NDB_THREAD_PRIO_HIGH);
  if (theThread == nullptr) {
    fprintf(stderr,
            "%s NDBAPI FATAL ERROR : ArbitMgr::doStart: Failed to "
            "create thread for arbitration.\n",
            Logger::Timestamp().c_str());
    abort();
  }
  NdbMutex_Unlock(theThreadMutex);
  DBUG_VOID_RETURN;
}

// The "choose me" signal from a candidate.
void ArbitMgr::doChoose(const Uint32 *theData) {
  ArbitSignal aSignal;
  aSignal.init(GSN_ARBIT_CHOOSEREQ, theData);
  sendSignalToThread(aSignal);
}

// Stop arbitrator thread via stop signal from the kernel
// or when exiting API program.
void ArbitMgr::doStop(const Uint32 *theData) {
  DBUG_ENTER("ArbitMgr::doStop");
  ArbitSignal aSignal;
  NdbMutex_Lock(theThreadMutex);
  if (theThread != nullptr) {
    aSignal.init(GSN_ARBIT_STOPORD, theData);
    if (theData == nullptr) {
      aSignal.data.code = StopExit;
    } else {
      aSignal.data.code = StopRequest;
    }
    sendSignalToThread(aSignal);
    void *value;
    NdbThread_WaitFor(theThread, &value);
    NdbThread_Destroy(&theThread);
    theState = StateInit;
  }
  NdbMutex_Unlock(theThreadMutex);
  DBUG_VOID_RETURN;
}

// private methods

extern "C" void *runArbitMgr_C(void *me) {
  ((ArbitMgr *)me)->threadMain();
  return nullptr;
}

void ArbitMgr::sendSignalToThread(ArbitSignal &aSignal) {
#ifdef DEBUG_ARBIT
  char buf[17] = "";
  ndbout << "arbit recv: ";
  ndbout << " gsn=" << aSignal.gsn;
  ndbout << " send=" << aSignal.data.sender;
  ndbout << " code=" << aSignal.data.code;
  ndbout << " node=" << aSignal.data.node;
  ndbout << " ticket=" << aSignal.data.ticket.getText(buf, sizeof(buf));
  ndbout << " mask=" << aSignal.data.mask.getText(buf, sizeof(buf));
  ndbout << endl;
#endif
  aSignal.setTimestamp();  // signal arrival time
  NdbMutex_Lock(theInputMutex);
  while (theInputFull) {
    NdbCondition_WaitTimeout(theInputCond, theInputMutex, 1000);
  }
  theInputBuffer = aSignal;
  theInputFull = true;
  NdbCondition_Signal(theInputCond);
  NdbMutex_Unlock(theInputMutex);
}

void ArbitMgr::threadMain() {
  ArbitSignal aSignal;
  aSignal = theInputBuffer;
  threadStart(aSignal);
  bool stop = false;
  while (!stop) {
    NdbMutex_Lock(theInputMutex);
    while (!theInputFull) {
      NdbCondition_WaitTimeout(theInputCond, theInputMutex, theInputTimeout);
      threadTimeout();
    }
    aSignal = theInputBuffer;
    theInputFull = false;
    NdbCondition_Signal(theInputCond);
    NdbMutex_Unlock(theInputMutex);
    switch (aSignal.gsn) {
      case GSN_ARBIT_CHOOSEREQ:
        threadChoose(aSignal);
        break;
      case GSN_ARBIT_STOPORD:
        stop = true;
        break;
    }
  }
  threadStop(aSignal);
}

// handle events in the thread

void ArbitMgr::threadStart(ArbitSignal &aSignal) {
  theStartReq = aSignal;
  theState = StateStarted;
  theInputTimeout = 1000;
  sendStartConf(theStartReq, ArbitCode::ApiStart);
  /*
   * Qmgr sends ARBIT_STARTREQ only after the ARBIT_PREP2 round has
   * completed: the president has sent the selected arbitrator node and
   * ticket to every current data node and received ARBIT_PREPCONF from
   * all of them.  At this point all data nodes in that arbitration view
   * agree on who the arbitrator is.
   *
   * With two data nodes, the remaining ordering is still safe.  If the
   * president fails after sending ARBIT_STARTREQ, it had already made the
   * decision and distributed it during PREP2.  If the non-president fails
   * before the president reaches ARBIT_RUN, the president will finish the
   * START phase as soon as this ARBIT_STARTCONF arrives; that transition
   * does not depend on the non-president.  Therefore this is the right
   * local point to report that the mgmd arbitrator is active and ready for
   * any later ARBIT_CHOOSEREQ.
   */
  m_active_arbitrator.store(true, std::memory_order_release);
}

void ArbitMgr::threadChoose(ArbitSignal &aSignal) {
  switch (theState) {
    case StateStarted:  // first REQ
      if (!theStartReq.data.match(aSignal.data)) {
        sendChooseRef(aSignal, ArbitCode::ErrTicket);
        break;
      }
      theChooseReq1 = aSignal;
      if (theDelay == 0) {
        sendChooseConf(aSignal, ArbitCode::WinChoose);
        theState = StateFinished;
        theInputTimeout = 1000;
        break;
      }
      theState = StateChoose1;
      theInputTimeout = 1;
      return;
    case StateChoose1:  // second REQ within Delay
      if (!theStartReq.data.match(aSignal.data)) {
        sendChooseRef(aSignal, ArbitCode::ErrTicket);
        break;
      }
      theChooseReq2 = aSignal;
      theState = StateChoose2;
      theInputTimeout = 1;
      return;
    case StateChoose2:  // too many REQs - refuse all
      if (!theStartReq.data.match(aSignal.data)) {
        sendChooseRef(aSignal, ArbitCode::ErrTicket);
        break;
      }
      sendChooseRef(theChooseReq1, ArbitCode::ErrToomany);
      sendChooseRef(theChooseReq2, ArbitCode::ErrToomany);
      sendChooseRef(aSignal, ArbitCode::ErrToomany);
      theState = StateFinished;
      theInputTimeout = 1000;
      return;
    default:
      sendChooseRef(aSignal, ArbitCode::ErrState);
      break;
  }
}

void ArbitMgr::threadTimeout() {
  switch (theState) {
    case StateStarted:
      break;
    case StateChoose1:
      if (theChooseReq1.getTimediff() < theDelay) break;
      sendChooseConf(theChooseReq1, ArbitCode::WinChoose);
      theState = StateFinished;
      theInputTimeout = 1000;
      break;
    case StateChoose2:
      sendChooseConf(theChooseReq1, ArbitCode::WinChoose);
      sendChooseConf(theChooseReq2, ArbitCode::LoseChoose);
      theState = StateFinished;
      theInputTimeout = 1000;
      break;
    default:
      break;
  }
}

void ArbitMgr::threadStop(ArbitSignal &aSignal) {
  switch (aSignal.data.code) {
    case StopExit:
      switch (theState) {
        case StateStarted:
          sendStopRep(theStartReq, 0);
          break;
        case StateChoose1:  // just in time
          sendChooseConf(theChooseReq1, ArbitCode::WinChoose);
          break;
        case StateChoose2:
          sendChooseConf(theChooseReq1, ArbitCode::WinChoose);
          sendChooseConf(theChooseReq2, ArbitCode::LoseChoose);
          break;
        case StateInit:
        case StateFinished:
          //??
          break;
      }
      break;
    case StopRequest:
      break;
    case StopRestart:
      break;
  }
}

// output routines

void ArbitMgr::sendStartConf(ArbitSignal &aSignal, Uint32 code) {
  ArbitSignal copySignal = aSignal;
  copySignal.gsn = GSN_ARBIT_STARTCONF;
  copySignal.data.code = code;
  sendSignalToQmgr(copySignal);
}

void ArbitMgr::sendChooseConf(ArbitSignal &aSignal, Uint32 code) {
  ArbitSignal copySignal = aSignal;
  copySignal.gsn = GSN_ARBIT_CHOOSECONF;
  copySignal.data.code = code;
  sendSignalToQmgr(copySignal);
}

void ArbitMgr::sendChooseRef(ArbitSignal &aSignal, Uint32 code) {
  ArbitSignal copySignal = aSignal;
  copySignal.gsn = GSN_ARBIT_CHOOSEREF;
  copySignal.data.code = code;
  sendSignalToQmgr(copySignal);
}

void ArbitMgr::sendStopRep(ArbitSignal &aSignal, Uint32 code) {
  ArbitSignal copySignal = aSignal;
  copySignal.gsn = GSN_ARBIT_STOPREP;
  copySignal.data.code = code;
  sendSignalToQmgr(copySignal);
}

/**
 * Send signal to QMGR.  The input includes signal number and
 * signal data.  The signal data is normally a copy of a received
 * signal so it contains expected arbitrator node id and ticket.
 * The sender in signal data is the QMGR node id.
 */
void ArbitMgr::sendSignalToQmgr(ArbitSignal &aSignal) {
  NdbApiSignal signal(numberToRef(API_CLUSTERMGR, m_clusterMgr.getOwnNodeId()));

  signal.theVerId_signalNumber = aSignal.gsn;
  signal.theReceiversBlockNumber = QMGR;
  signal.theTrace = 0;
  signal.theLength = ArbitSignalData::SignalLength;

  ArbitSignalData *sd = CAST_PTR(ArbitSignalData, signal.getDataPtrSend());

  sd->sender = numberToRef(API_CLUSTERMGR, m_clusterMgr.getOwnNodeId());
  sd->code = aSignal.data.code;
  sd->node = aSignal.data.node;
  sd->ticket = aSignal.data.ticket;
  sd->mask = aSignal.data.mask;

#ifdef DEBUG_ARBIT
  char buf[17] = "";
  ndbout << "arbit send: ";
  ndbout << " gsn=" << aSignal.gsn;
  ndbout << " recv=" << aSignal.data.sender;
  ndbout << " code=" << aSignal.data.code;
  ndbout << " node=" << aSignal.data.node;
  ndbout << " ticket=" << aSignal.data.ticket.getText(buf, sizeof(buf));
  ndbout << " mask=" << aSignal.data.mask.getText(buf, sizeof(buf));
  ndbout << endl;
#endif

  {
    m_clusterMgr.lock();
    m_clusterMgr.raw_sendSignal(&signal, aSignal.data.sender);
    m_clusterMgr.flush_send_buffers();
    m_clusterMgr.unlock();
  }
}
