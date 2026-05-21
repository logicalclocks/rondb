/*
   Copyright (c) 2003, 2024, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.

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

#ifndef TransporterDefinitions_H
#define TransporterDefinitions_H

#include <kernel_types.h> 
#include <ndb_global.h>
#include <NdbOut.hpp>
#include "SocketAuthenticator.hpp"  // TlsAuth

/**
 * The sendbuffer limit after which the contents of the buffer is sent
 */
const int TCP_SEND_LIMIT = 64000;

enum SendStatus { 
  SEND_OK = 0, 
  SEND_BLOCKED = 1, 
  SEND_DISCONNECTED = 2, 
  SEND_BUFFER_FULL = 3,
  SEND_MESSAGE_TOO_BIG = 4,
  SEND_UNKNOWN_NODE = 5
};

enum TransporterType {
  tt_TCP_TRANSPORTER = 1,
  tt_SHM_TRANSPORTER = 3,
  /**
   * Native RonDB RDMA transporter (RC SEND/RECV) for explicit DB-DB and
   * API-DB traffic. Only available when NDB_RDMA_TRANSPORTER_SUPPORTED is defined.
   * Values 1 and 3 are preserved for wire/log compatibility with TCP/SHM.
   */
  tt_RDMA_TRANSPORTER = 4
};

enum SB_LevelType {
  SB_NO_RISK_LEVEL = 0,
  SB_LOW_LEVEL = 1,
  SB_MEDIUM_LEVEL = 2,
  SB_HIGH_LEVEL = 3,
  SB_RISK_LEVEL = 4,
  SB_CRITICAL_LEVEL = 5
};

/**
 * Maximum message sizes
 * ---------------------
 * Maximum byte sizes for sent and received messages.
 * The maximum send message size is temporarily smaller than 
 * the maximum receive message size to support online
 * upgrade
 * Maximum received size increased in :
 *   mysql-5.1-telco-6.3.18 from 16516 bytes to 32768
 * Maximum send size increased in :
 *   mysql-5.1-telco-6.4.0 from 16516 bytes to 32768
 *
 * Therefore mysql-5.1-telco-6.4.0 cannot safely communicate 
 * with nodes at versions lower than mysql-5.1-telco-6.3.18 
 * 
 */
constexpr Uint32 MAX_RECV_MESSAGE_BYTESIZE = 32768;
constexpr Uint32 MAX_SEND_MESSAGE_BYTESIZE = 32768;

/**
 * TransporterConfiguration
 *
 * used for setting up a transporter. the union member specific is for
 * information specific to a transporter type.
 */
struct TransporterConfiguration {
  Int32 s_port; // negative port number implies dynamic port
  const char *remoteHostName;
  const char *localHostName;
  TrpId transporterIndex;
  NodeId remoteNodeId;
  NodeId localNodeId;
  NodeId serverNodeId;
  bool requireTls;
  bool checksum;
  bool signalId;
  bool isMgmConnection; // is a mgm connection, requires transforming
  TransporterType type;
  bool preSendChecksum;

  union { // Transporter specific configuration information

    struct {
      Uint32 sendBufferSize;     // Size of SendBuffer of priority B 
      Uint32 maxReceiveSize;     // Maximum no of bytes to receive
      Uint32 tcpSndBufSize;
      Uint32 tcpRcvBufSize;
      Uint32 tcpMaxsegSize;
      Uint32 tcpOverloadLimit;
      Uint32 tcpSpintime;
    } tcp;
    
    struct {
      Uint32 shmKey;
      Uint32 shmSize;
      Uint32 shmSpintime;
      Uint32 sendBufferSize;
    } shm;

    /**
     * RDMA transporter configuration. Only consumed when
     * NDB_RDMA_TRANSPORTER_SUPPORTED is defined and the connection
     * section type is CONNECTION_TYPE_RDMA. Sizes are validated by
     * the transporter against device capabilities.
     */
    struct {
      Uint32 sendBufferSize;       // Bytes of staged send memory
      Uint32 recvBufferSize;       // Bytes of staged receive memory
      Uint32 queueDepth;           // Send/recv WR queue depth
      Uint32 inlineThreshold;      // Max bytes posted as inline SEND
      Uint32 completionPollBudget; // Max WCs reaped per poll call
      Uint32 spintime;             // Microseconds to spin before sleep
      Uint32 rdmaPort;             // HCA physical port number
      Uint32 gidIndex;             // GID index for RoCE
      Uint32 trafficClass;         // DSCP-like traffic class
      Uint32 retryCount;           // QP retry count
      Uint32 rnrRetryCount;        // QP RNR retry count
      Uint32 overloadLimit;        // Unsent bytes overload threshold
      const char *deviceName;      // ibverbs device name, NULL = first available
    } rdma;
  };
};

struct SignalHeader {	
  Uint32 theVerId_signalNumber;    // 4 bit ver id - 16 bit gsn
  Uint32 theReceiversBlockNumber;  // Only 16 bit blocknum  
  Uint32 theSendersBlockRef;
  Uint32 theLength;
  Uint32 theSendersSignalId;
  Uint32 theSignalId;
  Uint32 theThreadSenderSignalId;
  Uint16 theTrace;
  Uint16 theSenderThreadId;
  Uint16  m_unused;
  Uint8  m_noOfSections;
  Uint8  m_fragmentInfo;
}; /** 7x4 + 3x2 + 2x1 = 36 Bytes */

class NdbOut &operator<<(class NdbOut &out, SignalHeader &sh);

#define TE_DO_DISCONNECT 0x8000

enum TransporterError {
  TE_NO_ERROR = 0,
  /**
   * TE_ERROR_CLOSING_SOCKET
   *
   *   Error found during closing of socket
   *
   * Recommended behavior: Ignore
   */
  TE_ERROR_CLOSING_SOCKET = 0x1,

  /**
   * TE_ERROR_IN_SELECT_BEFORE_ACCEPT
   *
   *   Error found during accept (just before)
   *     The transporter will retry.
   *
   * Recommended behavior: Ignore
   *   (or possible do setPerformState(PerformDisconnect)
   */
  TE_ERROR_IN_SELECT_BEFORE_ACCEPT = 0x2,

  /**
   * TE_INVALID_MESSAGE_LENGTH
   *
   *   Error found in message (message length)
   *
   * Recommended behavior: setPerformState(PerformDisconnect)
   */
  TE_INVALID_MESSAGE_LENGTH = 0x3 | TE_DO_DISCONNECT,

  /**
   * TE_INVALID_CHECKSUM
   *
   *   Error found in message (checksum)
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  TE_INVALID_CHECKSUM = 0x4 | TE_DO_DISCONNECT,

  /**
   * TE_COULD_NOT_CREATE_SOCKET
   *
   *   Error found while creating socket
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  TE_COULD_NOT_CREATE_SOCKET = 0x5,

  /**
   * TE_COULD_NOT_BIND_SOCKET
   *
   *   Error found while binding server socket
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  TE_COULD_NOT_BIND_SOCKET = 0x6,

  /**
   * TE_LISTEN_FAILED
   *
   *   Error found while listening to server socket
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  TE_LISTEN_FAILED = 0x7,

  /**
   * TE_ACCEPT_RETURN_ERROR
   *
   *   Error found during accept
   *     The transporter will retry.
   *
   * Recommended behavior: Ignore
   *   (or possible do setPerformState(PerformDisconnect)
   */
  TE_ACCEPT_RETURN_ERROR = 0x8

  /**
   * TE_SHM_DISCONNECT
   *
   *    The remote node has disconnected
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_SHM_DISCONNECT = 0xb | TE_DO_DISCONNECT

  /**
   * TE_SHM_IPC_STAT
   *
   *    Unable to check shm segment
   *      probably because remote node
   *      has disconnected and removed it
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_SHM_IPC_STAT = 0xc | TE_DO_DISCONNECT

  /**
   * Permanent error
   */
  ,
  TE_SHM_IPC_PERMANENT = 0x21

  /**
   * TE_SHM_UNABLE_TO_CREATE_SEGMENT
   *
   *    Unable to create shm segment
   *      probably os something error
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_SHM_UNABLE_TO_CREATE_SEGMENT = 0xd

  /**
   * TE_SHM_UNABLE_TO_ATTACH_SEGMENT
   *
   *    Unable to attach shm segment
   *      probably invalid group / user
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_SHM_UNABLE_TO_ATTACH_SEGMENT = 0xe

  /**
   * TE_SHM_UNABLE_TO_REMOVE_SEGMENT
   *
   *    Unable to remove shm segment
   *
   * Recommended behavior: Ignore (not much to do)
   *                       Print warning to logfile
   */
  ,
  TE_SHM_UNABLE_TO_REMOVE_SEGMENT = 0xf

  ,
  TE_TOO_SMALL_SIGID = 0x10,
  TE_TOO_LARGE_SIGID = 0x11,
  TE_WAIT_STACK_FULL = 0x12 | TE_DO_DISCONNECT,
  TE_RECEIVE_BUFFER_FULL = 0x13 | TE_DO_DISCONNECT

  /**
   * TE_SIGNAL_LOST_SEND_BUFFER_FULL
   *
   *   Send buffer is full, and trying to force send fails
   *   a signal is dropped!! very bad very bad
   *
   */
  ,
  TE_SIGNAL_LOST_SEND_BUFFER_FULL = 0x14 | TE_DO_DISCONNECT

  /**
   * TE_SIGNAL_LOST
   *
   *   Send failed for unknown reason
   *   a signal is dropped!! very bad very bad
   *
   */
  ,
  TE_SIGNAL_LOST = 0x15

  /**
   * TE_SEND_BUFFER_FULL
   *
   *   The send buffer was full, but sleeping for a while solved it
   */
  ,
  TE_SEND_BUFFER_FULL = 0x16

  /* Used 0x16 - 0x22 */

  /**
   * TE_UNSUPPORTED_BYTE_ORDER
   *
   *   Error found in message (byte order)
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_UNSUPPORTED_BYTE_ORDER = 0x23 | TE_DO_DISCONNECT

  /**
   * TE_COMPRESSED_UNSUPPORTED
   *
   *   Error found in message (compressed flag)
   *
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_COMPRESSED_UNSUPPORTED = 0x24 | TE_DO_DISCONNECT

  /**
   *
   * Error found in signal, not following NDB protocol
   * Recommended behavior: setPerformState(PerformDisonnect)
   */
  ,
  TE_INVALID_SIGNAL = 0x25 | TE_DO_DISCONNECT

  /* RDMA transporter errors. All disconnect the link.
   * Reserved range 0x30-0x3F.
   */
  ,
  /** Generic RDMA setup failure (device open, PD, MR, QP). */
  TE_RDMA_INIT_FAILED = 0x30 | TE_DO_DISCONNECT

  ,
  /** RDMA endpoint exchange over control socket failed or mismatched. */
  TE_RDMA_ENDPOINT_EXCHANGE_FAILED = 0x31 | TE_DO_DISCONNECT

  ,
  /** Wire-format header was rejected (length, version, flags). */
  TE_RDMA_INVALID_HEADER = 0x32 | TE_DO_DISCONNECT

  ,
  /** QP transitioned to error state. */
  TE_RDMA_QP_ERROR = 0x33 | TE_DO_DISCONNECT

  ,
  /** Completion queue reported a non-success status. */
  TE_RDMA_CQ_ERROR = 0x34 | TE_DO_DISCONNECT

  ,
  /** Receive-credit protocol was violated by the peer. */
  TE_RDMA_CREDIT_PROTOCOL_ERROR = 0x35 | TE_DO_DISCONNECT

  ,
  /** Retry counter or RNR retry counter exhausted. */
  TE_RDMA_RETRY_EXHAUSTED = 0x36 | TE_DO_DISCONNECT

  ,
  /** RDMA support not compiled into this build. */
  TE_RDMA_NOT_SUPPORTED = 0x37 | TE_DO_DISCONNECT
};

#endif // Define of TransporterDefinitions_H
