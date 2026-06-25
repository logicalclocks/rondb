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

#ifndef NDB_TC_DEADLOCK_REP_HPP
#define NDB_TC_DEADLOCK_REP_HPP

#include <ndb_types.h>

/**
 * TC_DEADLOCK_REP — DBTC → NDB API  (RONDB-1062, proactive deadlock discovery)
 *
 * Optional, version-gated enrichment of a deadlock abort.  When DBTC detects a
 * deadlock cycle and is about to abort the victim, it first sends this report
 * to the victim's API node IFF that node understands the signal
 * (ndbd_deadlock_detail_supported(version) — see ndb_version.h).  The usual
 * abort signal (TCROLLBACKREP with 266, or SCAN_TABREF with 296) follows
 * unchanged, so the externally visible error code and existing retry logic are
 * unaffected; this report only lets a new API expose extra detail.
 *
 * Fire-and-forget (no CONF/REF).  The API matches it to the transaction by the
 * carried apiConnectPtr and validates with the transaction id; a stale or
 * unmatched report is dropped.  Because it is gated on the API node version,
 * an old API never receives it, and a new API that receives an (older) abort
 * with no preceding report simply reports "no deadlock detail".
 */
struct TcDeadlockRep {
  Uint32 apiConnectPtr;  // API's connection object ptr (== ApiConnectRecord::
                         // ndbapiConnect); how the API finds the NdbTransaction
  Uint32 transId1;       // transaction id, for validation against the API txn
  Uint32 transId2;
  Uint32 deadlockReason; // bitmask, see Reason; bit0 set == a real detected
                         // deadlock cycle (vs the plain timeout backstop)
  Uint32 tableId1;       // a table involved in the cycle (RNIL if unknown)
  Uint32 tableId2;       // the other table involved (== tableId1 if the cycle
                         // contends on a single table; RNIL if unknown)
  Uint32 victimOpRef;    // the aborted victim's deadlocking operation, as the
                         // API operation pointer (TcConnectRecord::clientData);
                         // RNIL for a pure scan victim or if not resolvable

  static constexpr Uint32 SignalLength = 7;

  enum Reason {
    RealDeadlock = 0x1   // a cycle was detected (always set by this sender)
  };
};

#endif  // NDB_TC_DEADLOCK_REP_HPP
