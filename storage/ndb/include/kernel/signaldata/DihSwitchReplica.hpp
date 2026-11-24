/*
   Copyright (c) 2003, 2026, Oracle and/or its affiliates.

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

#ifndef DIH_SWITCH_REPLICA_HPP
#define DIH_SWITCH_REPLICA_HPP

#define JAM_FILE_ID 172

/**
 * This signal is sent from master DIH to all DIH's
 *   switches primary / backup nodes for replica(s)
 *
 */
class DihSwitchReplicaReq {
  /**
   * Sender/Reciver
   */
  friend class Dbdih;

 public:
  static constexpr Uint32 SignalLength = 4 + MAX_REPLICAS;

 private:
  /**
   * Request Info
   *
   */
  Uint32 senderRef;
  Uint32 tableId;
  Uint32 fragNo;
  Uint32 noOfReplicas;
  Uint32 newNodeOrder[MAX_REPLICAS];
};

class DihSwitchReplicaRef {
  /**
   * Sender/Reciver
   */
  friend class Dbdih;

 public:
  static constexpr Uint32 SignalLength = 2;

 private:
  Uint32 senderNode;
  Uint32 errorCode;  // See StopPermRef::ErrorCode
};

class DihSwitchReplicaConf {
  /**
   * Sender/Reciver
   */
  friend class Dbdih;

 public:
  static constexpr Uint32 SignalLength = 1;

 private:
  Uint32 senderNode;
};

/**
 * Rebalance of the primary replicas after node restarts.
 *
 * Request: the DIH of a restarted node asks the master DIH in start
 *   phase 120, after the restart barrier. The master answers with
 *   DihRebalanceConf(Done) when a rebalance covering the node has
 *   completed, or right away when it cannot run one.
 * Run: the master asks every alive DIH, itself included, to recompute
 *   the primary replicas of all tables; each answers with
 *   DihRebalanceConf(RunDone).
 */
class DihRebalanceReq {
  /**
   * Sender/Reciver
   */
  friend class Dbdih;

 public:
  static constexpr Uint32 SignalLength = 4;

  enum RequestType { Request = 1, Run = 2 };

 private:
  Uint32 senderRef;
  Uint32 requestType;
  Uint32 runId;   // Run only
  Uint32 failNo;  // the sender's Dbdih::cfailurenr
};

class DihRebalanceConf {
  /**
   * Sender/Reciver
   */
  friend class Dbdih;

 public:
  static constexpr Uint32 SignalLength = 5;

  enum ConfType { RunDone = 1, Done = 2 };

  /* Result of Done */
  enum Result {
    Completed = 0,
    /* A node start, node stop or master take-over is in progress */
    SkippedBusy = 1,
    /* Node failure handling recomputes the primaries instead */
    SkippedNodeFailure = 2,
    /* Some data node does not support the rebalance */
    SkippedNotSupported = 3
  };

 private:
  Uint32 senderRef;
  Uint32 confType;
  Uint32 runId;              // RunDone
  Uint32 result;             // Done
  Uint32 changedFragments;   // RunDone: fragments whose primary changed
};

#undef JAM_FILE_ID

#endif
