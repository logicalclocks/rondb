/*
   Copyright (c) 2026, Hopsworks and/or its affiliates.

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

#ifndef MALICIOUS_SIGNAL_REPORT_HPP
#define MALICIOUS_SIGNAL_REPORT_HPP

#include "SignalData.hpp"

#define JAM_FILE_ID 566

/**
 * Data node security: a kernel block reports a detected malformed/malicious
 * signal to QMGR, which owns all per-node security state and disconnect
 * decisions. Blocks never disconnect directly — they report.
 *
 * Sent JBA to QMGR_REF via SimulatedBlock::reportMaliciousSignal(). The tier is
 * derived by the sender from g_violation_info[violationType] (ViolationType.hpp)
 * so a newer sender's tier interpretation travels even if the receiving QMGR was
 * built without the corresponding violation type.
 *
 * Design reference: claude_files/data_node_security/tiered_response_policy.md §8.2
 */
struct MaliciousSignalReport {
  static constexpr Uint32 SignalLength = 6;

  Uint32 offendingNodeId;  // node that sent the offending signal
  Uint32 tier;             // ViolationTier (0 = A immediate disconnect, 1 = B log-only)
  Uint32 violationType;    // ViolationType enum; resolves reason string at QMGR
  Uint32 sourceBlockRef;   // reporting block reference, for forensics
  Uint32 sourceLine;       // __LINE__ at the detection site, for forensics
  Uint32 suppressedCount;  // reports batched since last send (report-rate limiting)
};

DECLARE_SIGNAL_SCOPE(GSN_MALICIOUS_SIGNAL_REPORT, Local);

#undef JAM_FILE_ID

#endif
