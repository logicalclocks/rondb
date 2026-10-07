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

#ifndef SET_CONFIG_PARAM_HPP
#define SET_CONFIG_PARAM_HPP

#include "SignalData.hpp"
#include <mgmapi_config_parameters.h>

#define JAM_FILE_ID 565

class SetConfigParamReq
{
  /**
   * Receiver(s)
   */
  friend class Cmvmi;
  friend class Qmgr;        // API node parameters, see forApiNodes()
  friend class ClusterMgr;  // the API node itself

  /**
   * Sender(s)
   */
  friend class MgmtSrvr;

public:
  static constexpr Uint32 SignalLength = 4;

  /**
   * A parameter of the API nodes ([api] / [mysqld] sections) rather than
   * of the data nodes.  API nodes have no transporter to the management
   * server, so mgmd sends the request (ApiSignalLength, with
   * targetNodeId) to CMVMI on every data node; CMVMI hands it to QMGR,
   * which forwards it to API_CLUSTERMGR on each connected API node of a
   * version that supports it (or only on targetNodeId), and answers mgmd
   * once they have replied.  An API node gets one copy per data node;
   * applying a value is idempotent.
   */
  static bool forApiNodes(Uint32 configKey)
  {
    return configKey == CFG_API_ADAPTIVE_SEND_THRESHOLD;
  }
  static constexpr Uint32 ApiSignalLength = 5;

private:
  Uint32 senderRef;
  Uint32 configParamKey;
  Uint32 configParamValueHigh;
  Uint32 configParamValueLow;
  Uint32 targetNodeId;  // ApiSignalLength: one API node, or 0 for all
};

class SetConfigParamConf
{
  /**
   * Sender(s)
   */
  friend class Cmvmi;
  friend class Qmgr;
  friend class ClusterMgr;

  /**
   * Receiver(s)
   */
  friend class MgmtSrvr;

public:
  static constexpr Uint32 SignalLength = 2;
  /* QMGR to mgmd for an API node parameter */
  static constexpr Uint32 ApiSignalLength = 3;

private:
  Uint32 senderRef;
  Uint32 configParamKey;
  Uint32 apiNodesApplied;  // ApiSignalLength: API nodes that took the value
};

class SetConfigParamRef
{
  /**
   * Sender(s)
   */
  friend class Cmvmi;
  friend class Qmgr;
  friend class ClusterMgr;

  /**
   * Receiver(s)
   */
  friend class MgmtSrvr;

public:
  static constexpr Uint32 SignalLength = 3;

private:
  Uint32 senderRef;
  Uint32 configParamKey;
  Uint32 errorCode;
};
#undef JAM_FILE_ID
#endif
