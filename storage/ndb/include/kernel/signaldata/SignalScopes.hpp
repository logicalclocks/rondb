/*
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.
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

#ifndef NDB_SIGNAL_SCOPES_HPP
#define NDB_SIGNAL_SCOPES_HPP

/*
  Central source of truth for per-GSN signal-scope classification.

  Each entry is X(GSN, Scope) where Scope is one of the SignalScope values
  (defined in SignalData.hpp):

    Local        - only ever sent within the same node (never on the wire)
    Remote       - may be sent over the wire, but only by a data (DB) node
    Management   - may be sent by a DB or MGM node, never by an API node
    External     - may be sent by any node, including API. Unrestricted at
                   runtime, but records that the send sites were audited and
                   found legitimately open
    Unclassified - the default; unrestricted at runtime, see below

  This list is the single place scopes are declared. SignalData.hpp expands it
  into signal_property<GSN> template specialisations and into the GSN-indexed
  g_signal_scope_table that seeds every block's handler array. Because
  SignalData.hpp is included by every block, every translation unit sees every
  specialisation (avoiding the ODR hazard of a per-signal-header declaration
  that a given block's .cpp might not include).

  A GSN with no entry here is Unclassified: nobody has audited it yet. It is
  unrestricted at runtime, exactly like External - the distinction is
  bookkeeping, so that audit progress is measurable and "reviewed, open to all"
  is not confused with "never looked at". The audit work-list is therefore just
  the GSNs defined in GlobalSignalNumbers.h with no entry below:

    grep -c '^#define GSN_' ../GlobalSignalNumbers.h   (defined GSNs)
    grep -cE '^  X\(GSN_' SignalScopes.hpp             (classified)

  An explicit X(GSN_x, Unclassified) entry means something different from
  absence: the GSN was audited but its full sender set could not be determined,
  so it deliberately stays open. Never guess a restrictive scope on incomplete
  evidence - a wrong restrictive call disconnects a healthy node.

  Keep entries grouped by scope and, within Local, by subsystem. Do not use //
  comments inside the macro body (line-continuation splicing would swallow
  entries); use block comments only.

  This same list is the intended source for a future transporter-boundary
  bitmap (flood defense); keep it authoritative.
*/

/*
  Debug-build exception: the block unit tests (storage/ndb/block_unit_test/)
  impersonate DBTC/DBSPJ from an API node, driving DBLQH directly with
  LQHKEYREQ and SCAN_FRAGREQ. In production only data nodes send these two
  signals, so they are Remote and an API-node sender is disconnected (Tier A).
  Debug builds (VM_TRACE, also set by WITH_NDB_DEBUG) relax exactly these two
  entries to Unclassified so the tests can run; their MTR wrappers source
  include/have_ndb_debug.inc, which probes the same VM_TRACE capability, so
  the tests skip on non-debug clusters.
*/
#ifdef VM_TRACE
#define NDB_SCOPE_REMOTE_EXCEPT_DEBUG Unclassified
#else
#define NDB_SCOPE_REMOTE_EXCEPT_DEBUG Remote
#endif

#define SIGNAL_SCOPES(X)                                                       \
  /* ===================== Local (same node only) ===================== */     \
  X(GSN_CONTINUEB, Local)                                                      \
  X(GSN_FSSUSPENDORD, Local)                                                   \
  X(GSN_MALICIOUS_SIGNAL_REPORT, Local)                                        \
  /* Filesystem (NDBFS) - REQ */                                              \
  X(GSN_FSOPENREQ, Local)                                                      \
  X(GSN_FSCLOSEREQ, Local)                                                     \
  X(GSN_FSREADREQ, Local)                                                      \
  X(GSN_FSWRITEREQ, Local)                                                     \
  X(GSN_FSSYNCREQ, Local)                                                      \
  X(GSN_FSREMOVEREQ, Local)                                                    \
  X(GSN_FSAPPENDREQ, Local)                                                    \
  /* Filesystem (NDBFS) - CONF */                                             \
  X(GSN_FSOPENCONF, Local)                                                     \
  X(GSN_FSCLOSECONF, Local)                                                    \
  X(GSN_FSREADCONF, Local)                                                     \
  X(GSN_FSWRITECONF, Local)                                                    \
  X(GSN_FSSYNCCONF, Local)                                                     \
  X(GSN_FSREMOVECONF, Local)                                                   \
  X(GSN_FSAPPENDCONF, Local)                                                   \
  /* Filesystem (NDBFS) - REF */                                              \
  X(GSN_FSOPENREF, Local)                                                      \
  X(GSN_FSCLOSEREF, Local)                                                     \
  X(GSN_FSREADREF, Local)                                                      \
  X(GSN_FSWRITEREF, Local)                                                     \
  X(GSN_FSSYNCREF, Local)                                                      \
  X(GSN_FSREMOVEREF, Local)                                                    \
  X(GSN_FSAPPENDREF, Local)                                                    \
  /* Startup / config sequencing (NDBCNTR -> local blocks) */                 \
  X(GSN_STTOR, Local)                                                          \
  X(GSN_NDB_STTOR, Local)                                                      \
  X(GSN_STTORRY, Local)                                                        \
  X(GSN_NDB_STTORRY, Local)                                                    \
  X(GSN_READ_CONFIG_REQ, Local)                                               \
  X(GSN_READ_CONFIG_CONF, Local)                                              \
  /* Index build / fragmentation / memory alloc (internal DICT/DIH/mem) */    \
  X(GSN_BUILDINDXREQ, Local)                                                   \
  X(GSN_BUILDINDXCONF, Local)                                                  \
  X(GSN_BUILDINDXREF, Local)                                                   \
  X(GSN_CREATE_FRAGMENTATION_REQ, Local)                                       \
  X(GSN_CREATE_FRAGMENTATION_REF, Local)                                       \
  X(GSN_CREATE_FRAGMENTATION_CONF, Local)                                      \
  X(GSN_ALLOC_MEM_REQ, Local)                                                  \
  X(GSN_ALLOC_MEM_REF, Local)                                                  \
  X(GSN_ALLOC_MEM_CONF, Local)                                                 \
  /* ================== Remote (data nodes only) ===================== */     \
  X(GSN_FAIL_REP, Remote)                                                      \
  /* Remote in production, Unclassified in debug builds - see the comment  */  \
  /* above the SIGNAL_SCOPES define.                                       */  \
  X(GSN_LQHKEYREQ, NDB_SCOPE_REMOTE_EXCEPT_DEBUG)                              \
  X(GSN_SCAN_FRAGREQ, NDB_SCOPE_REMOTE_EXCEPT_DEBUG)                           \
  X(GSN_GCP_PREPARE, Remote)                                                   \
  X(GSN_LCP_FRAG_ORD, Remote)                                                  \
  X(GSN_COPY_FRAGREQ, Remote)                                                  \
  /* ============= Management (DB or MGM, never API) ================= */     \
  X(GSN_START_ORD, Management)                                                 \
  X(GSN_STOP_REQ, Management)                                                  \
  X(GSN_RESUME_REQ, Management)                                                \
  X(GSN_CREATE_NODEGROUP_REQ, Management)                                      \
  X(GSN_DROP_NODEGROUP_REQ, Management)                                        \
  X(GSN_ALLOC_NODEID_REQ, Management)                                          \
  X(GSN_EVENT_SUBSCRIBE_REQ, Management)                                       \
  X(GSN_BACKUP_REQ, Management)                                                \
  X(GSN_ABORT_BACKUP_ORD, Management)                                          \
  /* ========== External (any node incl. API; reviewed) ============= */      \
  X(GSN_TCKEYREQ, External)                                                    \
  X(GSN_SCAN_TABREQ, External)                                                 \
  X(GSN_CREATE_INDX_REQ, External)                                             \
  X(GSN_API_REGREQ, External)                                                  \
  X(GSN_SUB_START_REQ, External)                                               \
  X(GSN_DUMP_STATE_ORD, External)                                              \
  /* ===== Unclassified (audited, sender set could not be traced) ===== */     \
  /* Same runtime effect as absence from this list; an entry here records   */ \
  /* that the GSN was looked at and deliberately left open. None so far.    */

#endif /* NDB_SIGNAL_SCOPES_HPP */
