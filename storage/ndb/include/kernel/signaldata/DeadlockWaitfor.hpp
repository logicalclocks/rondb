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

#ifndef NDB_DEADLOCK_WAITFOR_HPP
#define NDB_DEADLOCK_WAITFOR_HPP

#include <ndb_types.h>

/**
 * DBACC_WAITFOR_REP — DBACC → DBTC  (RONDB-1062, proactive deadlock discovery)
 *
 * Sent by DBACC the moment a lock request must be placed on a row's serial
 * (wait) queue behind a conflicting lock held by a different transaction.
 * It describes one directed wait-for edge:
 *
 *     waiter transaction  ── waits-for ──▶  lock-owner transaction
 *
 * The signal is routed to the edge's "collector": the endpoint with the
 * smaller hash(transid) of the two transactions (tie-broken by raw transid).
 * Because both edges of a 2-cycle (W→O and O→W) share the same endpoint set,
 * they compute the same collector and converge on the same DBTC, which can
 * then detect the cycle and abort the (local) collector transaction.
 *
 * Both endpoints are described fully so DBTC can resolve its local
 * (collector) transaction by tcOprec and validate it by transid.  The
 * CollectorIsWaiter flag tells DBTC which endpoint is the collector (needed
 * when both endpoints happen to be owned by the same TC instance).
 *
 * This is a fire-and-forget report (no CONF/REF).  A stale or already-
 * resolved edge is simply dropped by the receiver.
 */
struct DeadlockWaitforRep {
  Uint32 senderRef;       // DBACC block reference (diagnostics)
  Uint32 flags;           // bit0 = CollectorIsWaiter (see Flags)

  // The waiting transaction (tail of the wait-for edge):
  Uint32 waiterTransId1;
  Uint32 waiterTransId2;
  Uint32 waiterTcRef;     // TC block reference (node+instance) owning the waiter
  Uint32 waiterTcOprec;   // waiter's TcConnectRecord i-value in that TC

  // The lock-owning transaction (head of the wait-for edge):
  Uint32 ownerTransId1;
  Uint32 ownerTransId2;
  Uint32 ownerTcRef;      // TC block reference owning the lock owner
  Uint32 ownerTcOprec;    // owner's TcConnectRecord i-value in that TC

  // RONDB-1062 deadlock enrichment (Phase A): the table the waiter and owner
  // contend on (both endpoints want a lock on the same row, hence the same
  // table/fragment).  Carried so DBTC can later report the tables involved in a
  // detected deadlock to the NDB API without changing the error code.  RNIL if
  // the reporter could not supply it.
  Uint32 contendedTableId;

  static constexpr Uint32 SignalLength = 11;

  enum Flags {
    CollectorIsWaiter = 0x1,  // collector == waiter (else collector == owner)
    CollectorIsScan = 0x2     // collector is a locking scan: DBTC resolves it
                              // via ScanFragRec -> ScanRecord -> ApiConnect and
                              // aborts it with scanError().  Else it is a key
                              // op: resolved via TcConnectRecord + tcOprec.
  };
};

#endif  // NDB_DEADLOCK_WAITFOR_HPP
