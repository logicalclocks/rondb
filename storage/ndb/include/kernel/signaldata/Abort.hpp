/*
   Copyright (c) 2003, 2021, Oracle and/or its affiliates.
   Copyright (c) 2021, 2021, Logical Clocks and/or its affiliates.

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

#ifndef ABORT_HPP
#define ABORT_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 545


class Abort {
  /**
   * Reciver(s)
   */
  friend class Dblqh;

  /**
   * Sender
   */
  friend class Dbtc;
  friend class Trpman;

public:
  static constexpr Uint32 SignalLength = 4;
  static constexpr Uint32 SignalLengthKey = 5;
  static constexpr Uint32 SignalLengthDistr = 7;
  
  Uint32 tcOprec;
  Uint32 tcBlockref;
  Uint32 transid1;
  Uint32 transid2;
  Uint32 instanceKey;
  Uint32 threadId;
  Uint32 senderThreadSignalId;
};

class Aborted {
  /**
   * Reciver(s)
   */
  friend class Dbtc;
  
  /**
   * Sender
   */
  friend class Dblqh;

public:
  static constexpr Uint32 SignalLength = 5;
  
public:
  Uint32 senderData;
  Uint32 transid1;
  Uint32 transid2;
  Uint32 nodeId;
  Uint32 lastLqhIndicator;
};

class SendPushAbortReq
{
  /**
   * Reciver(s)
   */
  friend class Trpman;
  
  /**
   * Sender
   */
  friend class Dblqh;

public:
  static constexpr Uint32 StaticSignalLength = 8;
  /**
   * Carry the ABORT signal along
   */
  Uint32 tcOprec;
  Uint32 tcBlockref;
  Uint32 transid1;
  Uint32 transid2;

  /**
   * Information for TRPMAN to ensure signals are pushed
   * to the threads in the list.
   */
  Uint32 senderRef;
  Uint32 sendThreadSignalId;
  Uint32 numThreads;
  Uint32 threadId;
  Uint32 threadIds[0];
};

class SendPushAbortConf
{
  /**
   * Reciver(s)
   */
  friend class Dblqh;

  /**
   * Sender
   */
  friend class Trpman;

public:
  static constexpr Uint32 SignalLength = 6;
  /**
   * Carry the ABORT signal along
   */
  Uint32 tcOprec;
  Uint32 tcBlockref;
  Uint32 transid1;
  Uint32 transid2;

  Uint32 threadId;
  Uint32 sendThreadSignalId;
};

class PushAbortTrainOrd
{
  /**
   * Reciver(s)
   * Sender
   *
   * Sent routed through TRPMAN
   */
  friend class Dblqh;

public:
  static constexpr Uint32 SignalLength = 8;

  Uint32 tcOprec;
  Uint32 tcBlockref;
  Uint32 transid1;
  Uint32 transid2;

  Uint32 threadId; // Receive thread thr_no
  Uint32 sendThreadSignalId;
  Uint32 indexQueryThread;
  Uint32 abortRef;
};

#undef JAM_FILE_ID

#endif

