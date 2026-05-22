/* 
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.
   Copyright (c) 2022, 2025, Hopsworks and/or its affiliates.

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
#include "util/require.h"

#include <cstring>

#include <IPCConfig.hpp>

#include <TransporterRegistry.hpp>

#include <NdbSpin.h>
#include <mgmapi.h>
#include <mgmapi_configuration.hpp>

#include <EventLogger.hpp>
extern EventLogger * g_eventLogger;

/* Return true if node with "nodeId" is a MGM node */
static bool is_mgmd(Uint32 nodeId, const ndb_mgm_configuration *config) {
  ndb_mgm_configuration_iterator iter(config, CFG_SECTION_NODE);
  require(iter.find(CFG_NODE_ID, nodeId) == 0);

  Uint32 type;
  require(iter.get(CFG_TYPE_OF_SECTION, &type) == 0);

  return (type == NODE_TYPE_MGM);
}

bool IPCConfig::configureTransporters(Uint32 nodeId,
                                      const ndb_mgm_configuration *config,
                                      class TransporterRegistry &tr,
                                      bool transporter_to_self) {
  bool result = true;

  DBUG_ENTER("IPCConfig::configureTransporters");

  if (!is_mgmd(nodeId, config)) {
    /**
     * Iterate over all MGM's and construct a connectstring
     * create mgm_handle and give it to the Transporter Registry
     */

    const char *separator = "";
    BaseString connect_string;
    ndb_mgm_configuration_iterator iter(config, CFG_SECTION_NODE);
    for (iter.first(); iter.valid(); iter.next()) {
      Uint32 type;
      if (iter.get(CFG_TYPE_OF_SECTION, &type)) continue;
      if (type != NODE_TYPE_MGM) continue;
      const char *hostname;
      Uint32 port;
      if (iter.get(CFG_NODE_HOST, &hostname)) continue;
      if (strlen(hostname) == 0) continue;
      if (iter.get(CFG_MGM_PORT, &port)) continue;
      connect_string.appfmt("%s%s %u", separator, hostname, port);
      separator = ",";
    }
    NdbMgmHandle h = ndb_mgm_create_handle();
    if (h && connect_string.length() > 0) {
      ndb_mgm_set_connectstring(h, connect_string.c_str());
      tr.set_mgm_handle(h);
    }
  }

  /* Remove transporter to nodes that does not exist anymore */
  for (int i = 1; i < ABS_MAX_NODES; i++) {
    ndb_mgm_configuration_iterator iter(config, CFG_SECTION_NODE);
    if (tr.get_node_transporter(i) && iter.find(CFG_NODE_ID, i)) {
      // Transporter exist in TransporterRegistry but not
      // in configuration
      g_eventLogger->info(
          "Node %u connection to node %d could not be removed at this time",
          nodeId, i);
      result = false;  // Need restart
    }
  }
  /**
   * Set active node status on all nodes according to the received
   * configuration. This will ensure that we don't attempt to connect
   * or allow connections from these nodes until we have received
   * word that they are being activated. Also non-existing nodes
   * are set to non-active just in case.
   */
  for (int i= 1; i < ABS_MAX_NODES; i++)
  {
    ndb_mgm_configuration_iterator iter(config, CFG_SECTION_NODE);
    if (!iter.find(CFG_NODE_ID, i))
    {
      Uint32 is_active = 1;
      iter.get(CFG_NODE_ACTIVE, &is_active);
      tr.set_active_node(i, is_active, !transporter_to_self);
    }
    else
    {
      /* Nodes not existing will be set as not active nodes. */
      tr.set_active_node(i, 0, false);
    }
  }

  TransporterConfiguration conf;
  TransporterConfiguration loopback_conf;
  ndb_mgm_configuration_iterator iter(config, CFG_SECTION_CONNECTION);
  for (iter.first(); iter.valid(); iter.next()) {
    std::memset(&conf, 0, sizeof(conf));
    Uint32 nodeId1, nodeId2, remoteNodeId;
    const char *remoteHostName = nullptr, *localHostName = nullptr;
    if (iter.get(CFG_CONNECTION_NODE_1, &nodeId1)) continue;
    if (iter.get(CFG_CONNECTION_NODE_2, &nodeId2)) continue;

    if (nodeId1 != nodeId && nodeId2 != nodeId) continue;
    remoteNodeId = (nodeId == nodeId1 ? nodeId2 : nodeId1);

    if (nodeId1 == nodeId && nodeId2 == nodeId) {
      transporter_to_self = false; // One already present..ignore extra arg
    }

    {
      const char *host1 = nullptr, *host2 = nullptr;
      iter.get(CFG_CONNECTION_HOSTNAME_1, &host1);
      iter.get(CFG_CONNECTION_HOSTNAME_2, &host2);
      localHostName  = (nodeId == nodeId1 ? host1 : host2);
      remoteHostName = (nodeId == nodeId1 ? host2 : host1);
    }

    Uint32 sendSignalId = 1;
    Uint32 checksum = 1;
    Uint32 preSendChecksum = 0;
    if (iter.get(CFG_CONNECTION_SEND_SIGNAL_ID, &sendSignalId)) continue;
    if (iter.get(CFG_CONNECTION_CHECKSUM, &checksum)) continue;
    iter.get(CFG_CONNECTION_PRESEND_CHECKSUM, &preSendChecksum);

    Uint32 type = ~0;
    if (iter.get(CFG_TYPE_OF_SECTION, &type)) continue;

    Uint32 server_port = 0;
    if (iter.get(CFG_CONNECTION_SERVER_PORT, &server_port)) break;
    
    Uint32 nodeIdServer = 0;
    if (iter.get(CFG_CONNECTION_NODE_ID_SERVER, &nodeIdServer)) break;

    if (is_mgmd(nodeId1, config) || is_mgmd(nodeId2, config)) {
      // All connections with MGM uses the mgm port as server
      conf.isMgmConnection = true;
    } else
      conf.isMgmConnection = false;

    Uint32 bindInAddrAny = 0;
    iter.get(CFG_TCP_BIND_INADDR_ANY, &bindInAddrAny);

    bool requireTls = false;
    if (type == CONNECTION_TYPE_TCP && (nodeId1 != nodeId2)) {
      Uint32 useTls = 0;
      iter.get(CFG_TCP_REQUIRE_TLS, &useTls);
      requireTls = useTls;
    }

    if (nodeId == nodeIdServer && !conf.isMgmConnection) {
      tr.add_transporter_interface(remoteNodeId,
                                   !bindInAddrAny ? localHostName : "",
                                   server_port, requireTls);
    }
    
    DBUG_PRINT("info", ("Transporter between this node %d and node %d using "
                        "port %d, signalId %d, checksum %d,"
        "preSendChecksum %d",
                        nodeId, remoteNodeId, server_port, sendSignalId,
                        checksum, preSendChecksum));
    /*
      This may be a dynamic port. It depends on when we're getting
      our configuration. If we've been restarted, we'll be getting
      a configuration with our old dynamic port in it, hence the number
      here is negative (and we try the old port number first).

      On a first-run, server_port will be zero (with dynamic ports)

      If we're not using dynamic ports, we don't do anything.
    */

    conf.localNodeId    = nodeId;
    conf.remoteNodeId   = remoteNodeId;
    conf.checksum       = checksum;
    conf.preSendChecksum = preSendChecksum;
    conf.signalId       = sendSignalId;
    conf.s_port         = server_port;
    conf.localHostName  = localHostName;
    conf.remoteHostName = remoteHostName;
    conf.serverNodeId = nodeIdServer;
    conf.requireTls = requireTls;

    Uint32 spintime = 0;
    Uint32 shm_send_buffer_size = 2 * 1024 * 1024;
    switch (type) {
    case CONNECTION_TYPE_SHM:
        if (iter.get(CFG_SHM_KEY, &conf.shm.shmKey)) break;
        if (iter.get(CFG_SHM_BUFFER_MEM, &conf.shm.shmSize)) break;

      iter.get(CFG_SHM_SPINTIME, &spintime);
        conf.shm.shmSpintime = spintime;
      iter.get(CFG_SHM_SEND_BUFFER_SIZE, &shm_send_buffer_size);
        conf.shm.sendBufferSize = shm_send_buffer_size;

      conf.type = tt_SHM_TRANSPORTER;

        if (!tr.configureTransporter(&conf)) {
          DBUG_PRINT("error", ("Failed to configure SHM Transporter "
                               "from %d to %d",
                               conf.localNodeId, conf.remoteNodeId));
          g_eventLogger->info(
              "Node %u failed to configure SHM transporter to node %u", nodeId,
              conf.remoteNodeId);
          result = false;
        }
        DBUG_PRINT("info", ("Configured SHM Transporter using shmkey %d, "
                            "buf size = %d",
                            conf.shm.shmKey, conf.shm.shmSize));
      break;

    case CONNECTION_TYPE_TCP:
        if (iter.get(CFG_TCP_SEND_BUFFER_SIZE, &conf.tcp.sendBufferSize)) break;
        if (iter.get(CFG_TCP_RECEIVE_BUFFER_SIZE, &conf.tcp.maxReceiveSize))
          break;
      iter.get(CFG_TCP_SPINTIME, &spintime);
        conf.tcp.tcpSpintime = spintime;
      
        const char *proxy;
      if (!iter.get(CFG_TCP_PROXY, &proxy)) {
	if (strlen(proxy) > 0 && nodeId2 == nodeId) {
	  // TODO handle host:port
	  conf.s_port = atoi(proxy);
	}
      }

      iter.get(CFG_TCP_SND_BUF_SIZE, &conf.tcp.tcpSndBufSize);
      iter.get(CFG_TCP_RCV_BUF_SIZE, &conf.tcp.tcpRcvBufSize);
      iter.get(CFG_TCP_MAXSEG_SIZE, &conf.tcp.tcpMaxsegSize);
      iter.get(CFG_CONNECTION_OVERLOAD, &conf.tcp.tcpOverloadLimit);

      conf.type = tt_TCP_TRANSPORTER;
      
        if (!tr.configureTransporter(&conf)) {
          g_eventLogger->info(
              "Node %u failed to configure TCP transporter to node %u", nodeId,
              conf.remoteNodeId);
          result = false;
        }
        DBUG_PRINT("info", ("Configured TCP Transporter: sendBufferSize = %d, "
                            "maxReceiveSize = %d",
                            conf.tcp.sendBufferSize, conf.tcp.maxReceiveSize));
        loopback_conf = conf;  // reuse it...
        break;

    case CONNECTION_TYPE_RDMA:
#ifndef NDB_RDMA_TRANSPORTER_SUPPORTED
      g_eventLogger->info(
          "RDMA Transporter requested from %u to %u but this binary "
          "was not compiled with WITH_NDB_RDMA. Skipping connection.",
          nodeId, remoteNodeId);
      result = false;
      break;
#else
    {
      /*
       * The RDMA section parser. Every value is read with iter.get() and on
       * failure we break out of the switch and report a configuration error.
       * No verbs resources are created here; this only fills the config.
       */
      Uint32 rdma_send_buf = 0;
      Uint32 rdma_recv_buf = 0;
      Uint32 rdma_queue_depth = 0;
      Uint32 rdma_inline_threshold = 0;
      Uint32 rdma_poll_budget = 0;
      Uint32 rdma_spintime = 0;
      Uint32 rdma_port = 0;
      Uint32 rdma_gid_index = 0;
      Uint32 rdma_traffic_class = 0;
      Uint32 rdma_retry_count = 0;
      Uint32 rdma_rnr_retry_count = 0;
      Uint32 rdma_post_batch_max = 0;
      Uint32 rdma_overload_limit = 0;
      const char *rdma_device_name = nullptr;

      if (iter.get(CFG_RDMA_SEND_BUFFER_SIZE, &rdma_send_buf)) break;
      if (iter.get(CFG_RDMA_RECV_BUFFER_SIZE, &rdma_recv_buf)) break;
      if (iter.get(CFG_RDMA_QUEUE_DEPTH, &rdma_queue_depth)) break;
      if (iter.get(CFG_RDMA_INLINE_THRESHOLD, &rdma_inline_threshold)) break;
      if (iter.get(CFG_RDMA_COMPLETION_POLL_BUDGET, &rdma_poll_budget)) break;
      if (iter.get(CFG_RDMA_SPINTIME, &rdma_spintime)) break;
      if (iter.get(CFG_RDMA_PORT, &rdma_port)) break;
      if (iter.get(CFG_RDMA_GID_INDEX, &rdma_gid_index)) break;
      if (iter.get(CFG_RDMA_TRAFFIC_CLASS, &rdma_traffic_class)) break;
      if (iter.get(CFG_RDMA_RETRY_COUNT, &rdma_retry_count)) break;
      if (iter.get(CFG_RDMA_RNR_RETRY_COUNT, &rdma_rnr_retry_count)) break;
      /*
       * Phase 4: the post-batch max is optional from the ConfigInfo
       * schema's perspective (a fresh schema always carries a default),
       * but reading it with iter.get() lets the existing mgm-config
       * defaulting fall through naturally. A failure here is non-fatal:
       * the runtime caps the chain length on its own and a zero value
       * is interpreted as "use the conservative default of 1".
       */
      iter.get(CFG_RDMA_POST_BATCH_MAX, &rdma_post_batch_max);
      iter.get(CFG_CONNECTION_OVERLOAD, &rdma_overload_limit);
      iter.get(CFG_RDMA_DEVICE_NAME, &rdma_device_name);

      conf.rdma.sendBufferSize = rdma_send_buf;
      conf.rdma.recvBufferSize = rdma_recv_buf;
      conf.rdma.queueDepth = rdma_queue_depth;
      conf.rdma.inlineThreshold = rdma_inline_threshold;
      conf.rdma.completionPollBudget = rdma_poll_budget;
      conf.rdma.spintime = rdma_spintime;
      conf.rdma.rdmaPort = rdma_port;
      conf.rdma.gidIndex = rdma_gid_index;
      conf.rdma.trafficClass = rdma_traffic_class;
      conf.rdma.retryCount = rdma_retry_count;
      conf.rdma.rnrRetryCount = rdma_rnr_retry_count;
      conf.rdma.postBatchMax = rdma_post_batch_max;
      conf.rdma.overloadLimit = rdma_overload_limit;
      conf.rdma.deviceName = rdma_device_name;

      conf.type = tt_RDMA_TRANSPORTER;

      if (!tr.configureTransporter(&conf)) {
        g_eventLogger->info(
            "Node %u failed to configure RDMA transporter to node %u", nodeId,
            conf.remoteNodeId);
        result = false;
      }
      DBUG_PRINT("info",
                 ("Configured RDMA Transporter: send=%u recv=%u qd=%u",
                  conf.rdma.sendBufferSize, conf.rdma.recvBufferSize,
                  conf.rdma.queueDepth));
      break;
    }
#endif

    default:
        g_eventLogger->info("Transporter from node %u to node %u: unknown type",
                            nodeId, remoteNodeId);
      break;
    }  // switch
  }    // for

  if (transporter_to_self) {
    loopback_conf.remoteNodeId = nodeId;
    loopback_conf.localNodeId = nodeId;
    loopback_conf.serverNodeId = 0; // always client
    loopback_conf.remoteHostName = "localhost";
    loopback_conf.localHostName = "localhost";
    loopback_conf.s_port = 1; // prevent asking ndb_mgmd for port...
    loopback_conf.type = tt_TCP_TRANSPORTER;
    loopback_conf.checksum = 0;
    loopback_conf.signalId = 0;
    loopback_conf.tcp.sendBufferSize = 1024 * 1024;
    loopback_conf.tcp.maxReceiveSize = 1024 * 1024;
    loopback_conf.tcp.tcpSndBufSize = 0;
    loopback_conf.tcp.tcpRcvBufSize = 0;
    loopback_conf.tcp.tcpMaxsegSize = 256 * 1024;
    loopback_conf.tcp.tcpOverloadLimit = 768 * 1024;
    loopback_conf.requireTls = false;

    if (!tr.configureTransporter(&loopback_conf)) {
      g_eventLogger->info("Node %u failed to configure loopback transporter",
                          nodeId);
      result = false;
    }
  }

  DBUG_RETURN(result);
}
