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

#ifndef SIGNAL_DATA_H
#define SIGNAL_DATA_H

#include <kernel/GlobalSignalNumbers.h>
#include <kernel/kernel_types.h>
#include <kernel/ndb_limits.h>
#include <kernel/signaldata/SignalScopes.hpp>
#include <ndb_global.h>

#define JAM_FILE_ID 61

#define ASSERT_BOOL(flag, message) assert(flag <= 1)
#define ASSERT_RANGE(value, min, max, message) \
  assert((value) >= (min) && (value) <= (max))
#define ASSERT_MAX(value, max, message) assert((value) <= (max))

#define SECTION(x) static constexpr Uint32 x

template <typename T>
inline T *cast_ptr(Uint32 *ptr) {
  NDB_ASSERT_POD(T);
  return new (ptr) T;
}

template <typename T>
inline const T *cast_constptr(const Uint32 *ptr) {
  NDB_ASSERT_POD(T);
  return const_cast<const T *>(new (const_cast<Uint32 *>(ptr)) T);
}

#define CAST_PTR(Y, X) cast_ptr<Y>(X)
#define CAST_CONSTPTR(Y, X) cast_constptr<Y>(X)

// defines for setter and getters on commonly used member data in signals

#define GET_SET_SENDERDATA                      \
  Uint32 getSenderData() { return senderData; } \
  void setSenderData(Uint32 _s) { senderData = _s; }

#define GET_SET_SENDERREF                     \
  Uint32 getSenderRef() { return senderRef; } \
  void setSenderRef(Uint32 _s) { senderRef = _s; }

#define GET_SET_PREPAREID                     \
  Uint32 getPrepareId() { return prepareId; } \
  void setPrepareId(Uint32 _s) { prepareId = _s; }

#define GET_SET_ERRORCODE                     \
  Uint32 getErrorCode() { return errorCode; } \
  void setErrorCode(Uint32 _s) { errorCode = _s; }

#define GET_SET_TCERRORCODE                       \
  Uint32 getTCErrorCode() { return TCErrorCode; } \
  void setTCErrorCode(Uint32 _s) { TCErrorCode = _s; }

void printHex(FILE * output, const Uint32 * theData, Uint32 len,
              const char * prefix);

#define GSN_PRINT_SIGNATURE(f) bool f(FILE *, const Uint32 *, Uint32, Uint16)

GSN_PRINT_SIGNATURE(printTCKEYREQ);
GSN_PRINT_SIGNATURE(printTCKEYCONF);
GSN_PRINT_SIGNATURE(printTCKEYREF);
GSN_PRINT_SIGNATURE(printLQHKEYREQ);
GSN_PRINT_SIGNATURE(printLQHKEYCONF);
GSN_PRINT_SIGNATURE(printLQHKEYREF);
GSN_PRINT_SIGNATURE(printTUPKEYREQ);
GSN_PRINT_SIGNATURE(printTUPKEYCONF);
GSN_PRINT_SIGNATURE(printTUPKEYREF);
GSN_PRINT_SIGNATURE(printTUPCOMMITREQ);
GSN_PRINT_SIGNATURE(printCONTINUEB);
GSN_PRINT_SIGNATURE(printFSOPENREQ);
GSN_PRINT_SIGNATURE(printFSCLOSEREQ);
GSN_PRINT_SIGNATURE(printFSREADWRITEREQ);
GSN_PRINT_SIGNATURE(printFSREADWRITEREQ);
GSN_PRINT_SIGNATURE(printFSREF);
GSN_PRINT_SIGNATURE(printFSREF);
GSN_PRINT_SIGNATURE(printFSREF);
GSN_PRINT_SIGNATURE(printFSREF);
GSN_PRINT_SIGNATURE(printFSREF);
GSN_PRINT_SIGNATURE(printFSCONF);
GSN_PRINT_SIGNATURE(printFSCONF);
GSN_PRINT_SIGNATURE(printFSCONF);
GSN_PRINT_SIGNATURE(printFSCONF);
GSN_PRINT_SIGNATURE(printFSCONF);
GSN_PRINT_SIGNATURE(printCLOSECOMREQCONF);
GSN_PRINT_SIGNATURE(printCLOSECOMREQCONF);
GSN_PRINT_SIGNATURE(printPACKED_SIGNAL);
GSN_PRINT_SIGNATURE(printPREPFAILREQREF);
GSN_PRINT_SIGNATURE(printPREPFAILREQREF);
GSN_PRINT_SIGNATURE(printALTER_TABLE_REQ);
GSN_PRINT_SIGNATURE(printALTER_TABLE_CONF);
GSN_PRINT_SIGNATURE(printALTER_TABLE_REF);
GSN_PRINT_SIGNATURE(printALTER_TAB_REQ);
GSN_PRINT_SIGNATURE(printALTER_TAB_CONF);
GSN_PRINT_SIGNATURE(printALTER_TAB_REF);
GSN_PRINT_SIGNATURE(printCREATE_TRIG_REQ);
GSN_PRINT_SIGNATURE(printCREATE_TRIG_CONF);
GSN_PRINT_SIGNATURE(printCREATE_TRIG_REF);
GSN_PRINT_SIGNATURE(printALTER_TRIG_REQ);
GSN_PRINT_SIGNATURE(printALTER_TRIG_CONF);
GSN_PRINT_SIGNATURE(printALTER_TRIG_REF);
GSN_PRINT_SIGNATURE(printDROP_TRIG_REQ);
GSN_PRINT_SIGNATURE(printDROP_TRIG_CONF);
GSN_PRINT_SIGNATURE(printDROP_TRIG_REF);
GSN_PRINT_SIGNATURE(printFIRE_TRIG_ORD);
GSN_PRINT_SIGNATURE(printTRIG_ATTRINFO);
GSN_PRINT_SIGNATURE(printCREATE_INDX_REQ);
GSN_PRINT_SIGNATURE(printCREATE_INDX_CONF);
GSN_PRINT_SIGNATURE(printCREATE_INDX_REF);
GSN_PRINT_SIGNATURE(printDROP_INDX_REQ);
GSN_PRINT_SIGNATURE(printDROP_INDX_CONF);
GSN_PRINT_SIGNATURE(printDROP_INDX_REF);
GSN_PRINT_SIGNATURE(printALTER_INDX_REQ);
GSN_PRINT_SIGNATURE(printALTER_INDX_CONF);
GSN_PRINT_SIGNATURE(printALTER_INDX_REF);
GSN_PRINT_SIGNATURE(printTCINDXREQ);
GSN_PRINT_SIGNATURE(printTCINDXCONF);
GSN_PRINT_SIGNATURE(printTCINDXREF);
GSN_PRINT_SIGNATURE(printINDXKEYINFO);
GSN_PRINT_SIGNATURE(printINDXATTRINFO);
GSN_PRINT_SIGNATURE(printFSAPPENDREQ);
GSN_PRINT_SIGNATURE(printBACKUP_REQ);
GSN_PRINT_SIGNATURE(printBACKUP_DATA);
GSN_PRINT_SIGNATURE(printBACKUP_REF);
GSN_PRINT_SIGNATURE(printBACKUP_CONF);
GSN_PRINT_SIGNATURE(printABORT_BACKUP_ORD);
GSN_PRINT_SIGNATURE(printBACKUP_ABORT_REP);
GSN_PRINT_SIGNATURE(printBACKUP_COMPLETE_REP);
GSN_PRINT_SIGNATURE(printBACKUP_NF_COMPLETE_REP);
GSN_PRINT_SIGNATURE(printDEFINE_BACKUP_REQ);
GSN_PRINT_SIGNATURE(printDEFINE_BACKUP_REF);
GSN_PRINT_SIGNATURE(printDEFINE_BACKUP_CONF);
GSN_PRINT_SIGNATURE(printSTART_BACKUP_REQ);
GSN_PRINT_SIGNATURE(printSTART_BACKUP_REF);
GSN_PRINT_SIGNATURE(printSTART_BACKUP_CONF);
GSN_PRINT_SIGNATURE(printBACKUP_FRAGMENT_REQ);
GSN_PRINT_SIGNATURE(printBACKUP_FRAGMENT_REF);
GSN_PRINT_SIGNATURE(printBACKUP_FRAGMENT_CONF);
GSN_PRINT_SIGNATURE(printSTOP_BACKUP_REQ);
GSN_PRINT_SIGNATURE(printSTOP_BACKUP_REF);
GSN_PRINT_SIGNATURE(printSTOP_BACKUP_CONF);
GSN_PRINT_SIGNATURE(printBACKUP_STATUS_REQ);
GSN_PRINT_SIGNATURE(printBACKUP_STATUS_CONF);
GSN_PRINT_SIGNATURE(printUTIL_SEQUENCE_REQ);
GSN_PRINT_SIGNATURE(printUTIL_SEQUENCE_REF);
GSN_PRINT_SIGNATURE(printUTIL_SEQUENCE_CONF);
GSN_PRINT_SIGNATURE(printUTIL_PREPARE_REQ);
GSN_PRINT_SIGNATURE(printUTIL_PREPARE_REF);
GSN_PRINT_SIGNATURE(printUTIL_PREPARE_CONF);
GSN_PRINT_SIGNATURE(printUTIL_EXECUTE_REQ);
GSN_PRINT_SIGNATURE(printUTIL_EXECUTE_REF);
GSN_PRINT_SIGNATURE(printUTIL_EXECUTE_CONF);
GSN_PRINT_SIGNATURE(printSCANTABREQ);
GSN_PRINT_SIGNATURE(printSCANTABCONF);
GSN_PRINT_SIGNATURE(printSCANTABREF);
GSN_PRINT_SIGNATURE(printSCANNEXTREQ);
GSN_PRINT_SIGNATURE(printSCANFRAGNEXTREQ);
GSN_PRINT_SIGNATURE(printLQHFRAGREQ);
GSN_PRINT_SIGNATURE(printLQHFRAGREF);
GSN_PRINT_SIGNATURE(printLQHFRAGCONF);
GSN_PRINT_SIGNATURE(printPREP_DROP_TAB_REQ);
GSN_PRINT_SIGNATURE(printPREP_DROP_TAB_REF);
GSN_PRINT_SIGNATURE(printPREP_DROP_TAB_CONF);
GSN_PRINT_SIGNATURE(printDROP_TAB_REQ);
GSN_PRINT_SIGNATURE(printDROP_TAB_REF);
GSN_PRINT_SIGNATURE(printDROP_TAB_CONF);
GSN_PRINT_SIGNATURE(printLCP_FRAG_ORD);
GSN_PRINT_SIGNATURE(printLCP_FRAG_REP);
GSN_PRINT_SIGNATURE(printLCP_COMPLETE_REP);
GSN_PRINT_SIGNATURE(printSTART_LCP_REQ);
GSN_PRINT_SIGNATURE(printSTART_LCP_CONF);
GSN_PRINT_SIGNATURE(printMASTER_LCP_REQ);
GSN_PRINT_SIGNATURE(printMASTER_LCP_REF);
GSN_PRINT_SIGNATURE(printMASTER_LCP_CONF);
GSN_PRINT_SIGNATURE(printCOPY_GCI_REQ);
GSN_PRINT_SIGNATURE(printSYSTEM_ERROR);
GSN_PRINT_SIGNATURE(printSTART_REC_REQ);
GSN_PRINT_SIGNATURE(printSTART_REC_CONF);
GSN_PRINT_SIGNATURE(printNF_COMPLETE_REP);
GSN_PRINT_SIGNATURE(printSIGNAL_DROPPED_REP);
GSN_PRINT_SIGNATURE(printFAIL_REP);
GSN_PRINT_SIGNATURE(printDISCONNECT_REP);
GSN_PRINT_SIGNATURE(printSUB_CREATE_REQ);
GSN_PRINT_SIGNATURE(printSUB_CREATE_CONF);
GSN_PRINT_SIGNATURE(printSUB_CREATE_REF);
GSN_PRINT_SIGNATURE(printSUB_REMOVE_REQ);
GSN_PRINT_SIGNATURE(printSUB_REMOVE_CONF);
GSN_PRINT_SIGNATURE(printSUB_REMOVE_REF);
GSN_PRINT_SIGNATURE(printSUB_START_REQ);
GSN_PRINT_SIGNATURE(printSUB_START_REF);
GSN_PRINT_SIGNATURE(printSUB_START_CONF);
GSN_PRINT_SIGNATURE(printSUB_STOP_REQ);
GSN_PRINT_SIGNATURE(printSUB_STOP_REF);
GSN_PRINT_SIGNATURE(printSUB_STOP_CONF);
GSN_PRINT_SIGNATURE(printSUB_SYNC_REQ);
GSN_PRINT_SIGNATURE(printSUB_SYNC_REF);
GSN_PRINT_SIGNATURE(printSUB_SYNC_CONF);
GSN_PRINT_SIGNATURE(printSUB_META_DATA);
GSN_PRINT_SIGNATURE(printSUB_TABLE_DATA);
GSN_PRINT_SIGNATURE(printSUB_SYNC_CONTINUE_REQ);
GSN_PRINT_SIGNATURE(printSUB_SYNC_CONTINUE_REF);
GSN_PRINT_SIGNATURE(printSUB_SYNC_CONTINUE_CONF);
GSN_PRINT_SIGNATURE(printSUB_GCP_COMPLETE_REP);
GSN_PRINT_SIGNATURE(printCREATE_FRAGMENTATION_REQ);
GSN_PRINT_SIGNATURE(printCREATE_FRAGMENTATION_REF);
GSN_PRINT_SIGNATURE(printCREATE_FRAGMENTATION_CONF);
GSN_PRINT_SIGNATURE(printUTIL_CREATE_LOCK_REQ);
GSN_PRINT_SIGNATURE(printUTIL_CREATE_LOCK_REF);
GSN_PRINT_SIGNATURE(printUTIL_CREATE_LOCK_CONF);
GSN_PRINT_SIGNATURE(printUTIL_DESTROY_LOCK_REQ);
GSN_PRINT_SIGNATURE(printUTIL_DESTROY_LOCK_REF);
GSN_PRINT_SIGNATURE(printUTIL_DESTROY_LOCK_CONF);
GSN_PRINT_SIGNATURE(printUTIL_LOCK_REQ);
GSN_PRINT_SIGNATURE(printUTIL_LOCK_REF);
GSN_PRINT_SIGNATURE(printUTIL_LOCK_CONF);
GSN_PRINT_SIGNATURE(printUTIL_UNLOCK_REQ);
GSN_PRINT_SIGNATURE(printUTIL_UNLOCK_REF);
GSN_PRINT_SIGNATURE(printUTIL_UNLOCK_CONF);
GSN_PRINT_SIGNATURE(printCNTR_START_REQ);
GSN_PRINT_SIGNATURE(printCNTR_START_REF);
GSN_PRINT_SIGNATURE(printCNTR_START_CONF);
GSN_PRINT_SIGNATURE(printREAD_NODES_CONF);
GSN_PRINT_SIGNATURE(printTUX_MAINT_REQ);
GSN_PRINT_SIGNATURE(printACC_LOCKREQ);
GSN_PRINT_SIGNATURE(printLQH_TRANSCONF);
GSN_PRINT_SIGNATURE(printSCAN_FRAGREQ);
GSN_PRINT_SIGNATURE(printSCAN_FRAGCONF);

GSN_PRINT_SIGNATURE(printCONTINUEB_NDBFS);
GSN_PRINT_SIGNATURE(printCONTINUEB_DBDIH);
GSN_PRINT_SIGNATURE(printSTART_FRAG_REQ);

GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_BEGIN_REQ);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_BEGIN_CONF);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_BEGIN_REF);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_END_REQ);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_END_CONF);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_END_REF);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_END_REP);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_IMPL_REQ);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_IMPL_CONF);
GSN_PRINT_SIGNATURE(printSCHEMA_TRANS_IMPL_REF);
GSN_PRINT_SIGNATURE(printCREATE_TAB_REQ);
GSN_PRINT_SIGNATURE(printCREATE_TAB_CONF);
GSN_PRINT_SIGNATURE(printCREATE_TAB_REF);
GSN_PRINT_SIGNATURE(printCREATE_TABLE_REQ);
GSN_PRINT_SIGNATURE(printCREATE_TABLE_CONF);
GSN_PRINT_SIGNATURE(printCREATE_TABLE_REF);
GSN_PRINT_SIGNATURE(printDROP_TABLE_REQ);
GSN_PRINT_SIGNATURE(printDROP_TABLE_REF);
GSN_PRINT_SIGNATURE(printDROP_TABLE_CONF);

GSN_PRINT_SIGNATURE(printGET_TABINFO_REQ);
GSN_PRINT_SIGNATURE(printGET_TABINFO_REF);
GSN_PRINT_SIGNATURE(printGET_TABINFO_CONF);

GSN_PRINT_SIGNATURE(printCREATE_TRIG_IMPL_REQ);
GSN_PRINT_SIGNATURE(printCREATE_TRIG_IMPL_CONF);
GSN_PRINT_SIGNATURE(printCREATE_TRIG_IMPL_REF);
GSN_PRINT_SIGNATURE(printDROP_TRIG_IMPL_REQ);
GSN_PRINT_SIGNATURE(printDROP_TRIG_IMPL_CONF);
GSN_PRINT_SIGNATURE(printDROP_TRIG_IMPL_REF);
GSN_PRINT_SIGNATURE(printALTER_TRIG_IMPL_REQ);
GSN_PRINT_SIGNATURE(printALTER_TRIG_IMPL_CONF);
GSN_PRINT_SIGNATURE(printALTER_TRIG_IMPL_REF);

GSN_PRINT_SIGNATURE(printCREATE_INDX_IMPL_REQ);
GSN_PRINT_SIGNATURE(printCREATE_INDX_IMPL_CONF);
GSN_PRINT_SIGNATURE(printCREATE_INDX_IMPL_REF);
GSN_PRINT_SIGNATURE(printDROP_INDX_IMPL_REQ);
GSN_PRINT_SIGNATURE(printDROP_INDX_IMPL_CONF);
GSN_PRINT_SIGNATURE(printDROP_INDX_IMPL_REF);
GSN_PRINT_SIGNATURE(printALTER_INDX_IMPL_REQ);
GSN_PRINT_SIGNATURE(printALTER_INDX_IMPL_CONF);
GSN_PRINT_SIGNATURE(printALTER_INDX_IMPL_REF);

GSN_PRINT_SIGNATURE(printBUILD_INDX_REQ);
GSN_PRINT_SIGNATURE(printBUILD_INDX_CONF);
GSN_PRINT_SIGNATURE(printBUILD_INDX_REF);
GSN_PRINT_SIGNATURE(printBUILD_INDX_IMPL_REQ);
GSN_PRINT_SIGNATURE(printBUILD_INDX_IMPL_CONF);
GSN_PRINT_SIGNATURE(printBUILD_INDX_IMPL_REF);

GSN_PRINT_SIGNATURE(printAPI_VERSION_REQ);
GSN_PRINT_SIGNATURE(printAPI_VERSION_CONF);

GSN_PRINT_SIGNATURE(printLOCAL_ROUTE_ORD);

GSN_PRINT_SIGNATURE(printDBINFO_SCAN);
GSN_PRINT_SIGNATURE(printDBINFO_SCAN_REF);

GSN_PRINT_SIGNATURE(printNODE_PING_REQ);
GSN_PRINT_SIGNATURE(printNODE_PING_CONF);

GSN_PRINT_SIGNATURE(printINDEX_STAT_REQ);
GSN_PRINT_SIGNATURE(printINDEX_STAT_CONF);
GSN_PRINT_SIGNATURE(printINDEX_STAT_REF);
GSN_PRINT_SIGNATURE(printINDEX_STAT_IMPL_REQ);
GSN_PRINT_SIGNATURE(printINDEX_STAT_IMPL_CONF);
GSN_PRINT_SIGNATURE(printINDEX_STAT_IMPL_REF);
GSN_PRINT_SIGNATURE(printINDEX_STAT_REP);

GSN_PRINT_SIGNATURE(printGET_CONFIG_REQ);
GSN_PRINT_SIGNATURE(printGET_CONFIG_REF);
GSN_PRINT_SIGNATURE(printGET_CONFIG_CONF);

GSN_PRINT_SIGNATURE(printALLOC_NODEID_REQ);
GSN_PRINT_SIGNATURE(printALLOC_NODEID_CONF);
GSN_PRINT_SIGNATURE(printALLOC_NODEID_REF);

GSN_PRINT_SIGNATURE(printLCP_STATUS_REQ);
GSN_PRINT_SIGNATURE(printLCP_STATUS_CONF);
GSN_PRINT_SIGNATURE(printLCP_STATUS_REF);

GSN_PRINT_SIGNATURE(printLCP_PREPARE_REQ);
GSN_PRINT_SIGNATURE(printLCP_PREPARE_CONF);
GSN_PRINT_SIGNATURE(printLCP_PREPARE_REF);

GSN_PRINT_SIGNATURE(printSYNC_PAGE_CACHE_REQ);
GSN_PRINT_SIGNATURE(printSYNC_PAGE_CACHE_CONF);

GSN_PRINT_SIGNATURE(printEND_LCPREQ);
GSN_PRINT_SIGNATURE(printEND_LCPCONF);

GSN_PRINT_SIGNATURE(printRESTORE_LCP_REQ);
GSN_PRINT_SIGNATURE(printRESTORE_LCP_CONF);
GSN_PRINT_SIGNATURE(printRESTORE_LCP_REF);

GSN_PRINT_SIGNATURE(printCREATE_FK_REQ);
GSN_PRINT_SIGNATURE(printCREATE_FK_REF);
GSN_PRINT_SIGNATURE(printCREATE_FK_CONF);
GSN_PRINT_SIGNATURE(printDROP_FK_REQ);
GSN_PRINT_SIGNATURE(printDROP_FK_REF);
GSN_PRINT_SIGNATURE(printDROP_FK_CONF);

GSN_PRINT_SIGNATURE(printISOLATE_ORD);

GSN_PRINT_SIGNATURE(printPROCESSINFO_REP);
GSN_PRINT_SIGNATURE(printTRP_KEEP_ALIVE);
GSN_PRINT_SIGNATURE(printCREATE_EVNT_CONF);
GSN_PRINT_SIGNATURE(printCREATE_EVNT_REQ);
GSN_PRINT_SIGNATURE(printCREATE_EVNT_REF);

GSN_PRINT_SIGNATURE(printCOMMIT);
GSN_PRINT_SIGNATURE(printCOMMITREQ);
GSN_PRINT_SIGNATURE(printCOMMITTED);
GSN_PRINT_SIGNATURE(printCOMMITCONF);

GSN_PRINT_SIGNATURE(printCOMPLETE);
GSN_PRINT_SIGNATURE(printCOMPLETEREQ);
GSN_PRINT_SIGNATURE(printCOMPLETED);
GSN_PRINT_SIGNATURE(printCOMPLETECONF);

GSN_PRINT_SIGNATURE(printDATABASE_QUOTA_REP);
GSN_PRINT_SIGNATURE(printDATABASE_RATE_ORD);
GSN_PRINT_SIGNATURE(printRATE_OVERLOAD_REP);
GSN_PRINT_SIGNATURE(printQUOTA_OVERLOAD_REP);

GSN_PRINT_SIGNATURE(printCREATE_DATABASE_REQ);
GSN_PRINT_SIGNATURE(printCREATE_DATABASE_CONF);
GSN_PRINT_SIGNATURE(printCREATE_DATABASE_REF);

GSN_PRINT_SIGNATURE(printALTER_DATABASE_REQ);
GSN_PRINT_SIGNATURE(printALTER_DATABASE_CONF);
GSN_PRINT_SIGNATURE(printALTER_DATABASE_REF);

GSN_PRINT_SIGNATURE(printDROP_DATABASE_REQ);
GSN_PRINT_SIGNATURE(printDROP_DATABASE_CONF);
GSN_PRINT_SIGNATURE(printDROP_DATABASE_REF);

GSN_PRINT_SIGNATURE(printGET_DATABASE_REQ);
GSN_PRINT_SIGNATURE(printGET_DATABASE_CONF);
GSN_PRINT_SIGNATURE(printGET_DATABASE_REF);

GSN_PRINT_SIGNATURE(printLIST_DATABASE_REQ);
GSN_PRINT_SIGNATURE(printLIST_DATABASE_CONF);
GSN_PRINT_SIGNATURE(printLIST_DATABASE_REF);

GSN_PRINT_SIGNATURE(printCREATE_DB_REQ);
GSN_PRINT_SIGNATURE(printCREATE_DB_CONF);
GSN_PRINT_SIGNATURE(printCREATE_DB_REF);

GSN_PRINT_SIGNATURE(printALTER_DB_REQ);
GSN_PRINT_SIGNATURE(printALTER_DB_CONF);
GSN_PRINT_SIGNATURE(printALTER_DB_REF);

GSN_PRINT_SIGNATURE(printDROP_DB_REQ);
GSN_PRINT_SIGNATURE(printDROP_DB_CONF);
GSN_PRINT_SIGNATURE(printDROP_DB_REF);

GSN_PRINT_SIGNATURE(printCOMMIT_DB_REQ);
GSN_PRINT_SIGNATURE(printCOMMIT_DB_CONF);
GSN_PRINT_SIGNATURE(printCOMMIT_DB_REF);

GSN_PRINT_SIGNATURE(printCONNECT_TABLE_DB_REQ);
GSN_PRINT_SIGNATURE(printCONNECT_TABLE_DB_CONF);
GSN_PRINT_SIGNATURE(printCONNECT_TABLE_DB_REF);

GSN_PRINT_SIGNATURE(printDISCONNECT_TABLE_DB_REQ);
GSN_PRINT_SIGNATURE(printDISCONNECT_TABLE_DB_CONF);
GSN_PRINT_SIGNATURE(printDISCONNECT_TABLE_DB_REF);

  /**
     Signal scope monitoring

   Any signal can be received via any connected transporter.
   Signals are sent between all node types (API, MGMD, Data nodes).
   By adding checks to the data nodes about where signals were received from,
   we can improve the robustness and security of the system.
   The main goal is to ensure that only allowed cluster nodes can send certain
   signals. To achieve this we distinguish between remote and local signals and
   add checks when particular signals are received.

   The signals can be defined with the following signal sending scopes. A
   violation does not restart the receiving node: the offending signal is
   dropped and reported to QMGR as a Tier A violation, which disconnects the
   sending node (see SimulatedBlock::handle_sender_error).

   Local:
   This signal should only be received from blocks on the same data node, this
   can be effectively checked. Any such signal received from another node is a
   violation.

   Remote:
   This specifies a signal can be received from any data node. Any such signal
   received from an API/MGM node is a violation.

   Management:
   This specifies a signal can only be received from an MGM node or a data node,
   but not an API node. Any such signal sent from an API node is a violation.

   External:
   This specifies the signal can be received from any node. No check is done.
   It has the same runtime semantics as Unclassified, but records that the
   signal's send sites were audited and found to be legitimately open.

   Unclassified:
   The default for a GSN with no entry in SignalScopes.hpp: nobody has audited
   its send sites yet. Unrestricted at runtime (fail open) - it exists purely to
   distinguish "reviewed and deliberately open" (External) from "never looked
   at", so audit progress is measurable. The audit work-list is exactly the
   GSNs defined in GlobalSignalNumbers.h that have no SIGNAL_SCOPES entry.

   The signal scope is defined in conjunction with setting up signal handler
   functions for a block during node startup. This is done by the addRecSignal
   calls.

   All per-GSN signal scopes are declared centrally in SignalScopes.hpp (the
   single source of truth), as entries of the form:

   DECLARE_SIGNAL_SCOPE(GlobalSignalNumber, SignalScope)

   That list is expanded below into signal_property<> specialisations and into
   g_signal_scope_table, which seeds every block's signal handler array.
*/
enum SignalScope { Local, Remote, Management, External, Unclassified };

/*
  The ordinals must run least -> most permissive, and the two unrestricted
  scopes must be the highest ones. Two things depend on it:
   - SimulatedBlock::addSignalScopeImpl resolves duplicate registrations with
     MIN(), i.e. most restrictive wins. With Unclassified last, an explicit
     classification always beats the default; it can never loosen one.
   - SimulatedBlock::checkSignalSender skips the check with a single
     (scope >= External) comparison.
*/
static_assert(Local < Remote && Remote < Management && Management < External &&
                  External < Unclassified,
              "SignalScope ordinals must run least->most permissive");

template <GlobalSignalNumber GSN>
struct signal_property {
  // A GSN with no specialisation (i.e. no SignalScopes.hpp entry) has not been
  // audited yet. Unrestricted at runtime, same as External.
  static constexpr SignalScope scope = Unclassified;
};

// Macro to define a template specialisation for a specific GSN
#define DECLARE_SIGNAL_SCOPE(gsn, theScope)        \
  template <>                                      \
  struct signal_property<gsn> {                    \
    static constexpr SignalScope scope = theScope; \
  }

/*
  Expand the central scope list (SignalScopes.hpp) into a signal_property<>
  specialisation per GSN. Because SignalData.hpp is included by every block,
  every translation unit sees every specialisation.
*/
#define DECLARE_SIGNAL_SCOPE_ENTRY(gsn, theScope) \
  DECLARE_SIGNAL_SCOPE(gsn, theScope);
SIGNAL_SCOPES(DECLARE_SIGNAL_SCOPE_ENTRY)
#undef DECLARE_SIGNAL_SCOPE_ENTRY

/*
  The same list as a GSN-indexed table, so a scope can be looked up at runtime
  from a GSN that is only known as a value. SimulatedBlock uses it to seed
  every block's signal handler array, which makes SignalScopes.hpp authoritative
  regardless of how a block installs its handler: addRecSignal applies the scope
  through signal_property<>, but handlers installed by direct assignment (see
  installSimulatedBlockFunctions) bypass that path and would otherwise silently
  ignore their classification.
*/
struct SignalScopeTable {
  SignalScope m_scope[MAX_GSN + 1];
  constexpr SignalScopeTable() : m_scope() {
    for (GlobalSignalNumber i = 0; i <= MAX_GSN; i++) m_scope[i] = Unclassified;
#define SIGNAL_SCOPE_TABLE_ENTRY(gsn, theScope) m_scope[gsn] = theScope;
    SIGNAL_SCOPES(SIGNAL_SCOPE_TABLE_ENTRY)
#undef SIGNAL_SCOPE_TABLE_ENTRY
  }
};
inline constexpr SignalScopeTable g_signal_scope_table{};

/*
  The FS*REF signals are the only classified GSNs whose handlers are installed
  by direct assignment rather than addRecSignal. They used to carry a hand
  written scope there; this pins that the table still supplies it.
*/
static_assert(g_signal_scope_table.m_scope[GSN_FSOPENREF] == Local &&
                  g_signal_scope_table.m_scope[GSN_FSCLOSEREF] == Local &&
                  g_signal_scope_table.m_scope[GSN_FSWRITEREF] == Local &&
                  g_signal_scope_table.m_scope[GSN_FSREADREF] == Local &&
                  g_signal_scope_table.m_scope[GSN_FSREMOVEREF] == Local &&
                  g_signal_scope_table.m_scope[GSN_FSSYNCREF] == Local &&
                  g_signal_scope_table.m_scope[GSN_FSAPPENDREF] == Local,
              "The FS*REF signals must stay Local in SignalScopes.hpp");

GSN_PRINT_SIGNATURE(printCOPY_ACTIVEREQ);
GSN_PRINT_SIGNATURE(printCOPY_ACTIVECONF);
GSN_PRINT_SIGNATURE(printCOPY_ACTIVEREF);
GSN_PRINT_SIGNATURE(printUPDATE_FRAG_DIST_KEY_ORD);
GSN_PRINT_SIGNATURE(printCOPY_FRAG_DONE_REP);
GSN_PRINT_SIGNATURE(printCOPY_FRAGREQ);
GSN_PRINT_SIGNATURE(printCOPY_FRAGCONF);
GSN_PRINT_SIGNATURE(printCOPY_FRAGREF);
GSN_PRINT_SIGNATURE(printHALT_COPY_FRAG_REQ);
GSN_PRINT_SIGNATURE(printHALT_COPY_FRAG_CONF);
GSN_PRINT_SIGNATURE(printHALT_COPY_FRAG_REF);
GSN_PRINT_SIGNATURE(printRESUME_COPY_FRAG_REQ);
GSN_PRINT_SIGNATURE(printRESUME_COPY_FRAG_CONF);
GSN_PRINT_SIGNATURE(printRESUME_COPY_FRAG_REF);
GSN_PRINT_SIGNATURE(printPREPARE_COPY_FRAG_REQ);
GSN_PRINT_SIGNATURE(printPREPARE_COPY_FRAG_CONF);
GSN_PRINT_SIGNATURE(printPREPARE_COPY_FRAG_REF);

#undef JAM_FILE_ID

#endif
