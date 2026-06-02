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

#ifndef NDB_VIOLATION_TYPE_HPP
#define NDB_VIOLATION_TYPE_HPP

#include <ndb_types.h>

/**
 * Data node security: tiered malicious-input response policy.
 *
 * A violation type is the single source of truth for how a malformed signal is
 * handled. Each type maps to exactly one tier and one human-readable reason
 * string (see g_violation_info[] below). Blocks report a ViolationType; the
 * sender derives the tier from the table and includes it in the signal, so an
 * older QMGR that does not recognise a newer violation type still receives a
 * usable tier (rolling-upgrade safety). QMGR uses the violation type only to
 * resolve the reason string for logging, falling back to VT_UNKNOWN when the
 * value is outside its known range.
 *
 * Design reference: claude_files/data_node_security/tiered_response_policy.md
 *
 * To add a new violation type:
 *   1. Add an enum value before VT_UNKNOWN.
 *   2. Add a matching row to g_violation_info[] at the same index.
 *   3. For a Tier A type, verify it is impossible to trigger via valid SQL /
 *      HTTP / REST / Redis user inputs (the categorization rule). Misclassifying
 *      a user-triggerable violation as Tier A reintroduces punishment laundering.
 * Never renumber existing values: the integer travels on the wire and old
 * receivers index this table by it.
 */

enum ViolationTier : Uint32 {
  TIER_A = 0,  // immediate disconnect: no honest client can trigger
  TIER_B = 1   // log-only forensic observability: a buggy client could trigger
};

enum ViolationType : Uint32 {
  // ---- DBTC (see DbtcMain.cpp call sites; Section 7 catalog) ----
  VT_UNEXPECTED_API_STATE = 0,     // A: signal in unexpected apiConnectRecord state
  VT_APICONNECT_OWNERSHIP,         // A: apiConnectRecord owned by different node
  VT_START_FLAG_DURING_ABORT,      // A: start flag during active abort
  VT_KEYINFO_INVALID_APICONNECT,   // A: invalid apiConnectPtr in KEYINFO
  VT_KEYINFO_OWNERSHIP,            // A: KEYINFO apiConnectPtr not owned by sender
  VT_KEYINFO_SIGNAL_LENGTH,        // B: KEYINFO signal length mismatch
  VT_ATTRINFO_INVALID_APICONNECT,  // A: invalid apiConnectPtr in ATTRINFO
  VT_ATTRINFO_OWNERSHIP,           // A: ATTRINFO apiConnectPtr not owned by sender
  VT_ATTRINFO_SIGNAL_TOO_SHORT,    // A: ATTRINFO signal too short
  VT_TCKEYREQ_SIGNAL_TOO_SHORT,    // A: TCKEYREQ signal too short
  VT_TCKEYREQ_KEYINFO_TOO_LARGE,   // B: TCKEYREQ KeyInfo section too large
  VT_TCKEYREQ_ATTRINFO_TOO_LARGE,  // B: TCKEYREQ AttrInfo section too large
  VT_TCKEYREQ_INVALID_APICONNECT,  // A: invalid apiConnectPtr in TCKEYREQ
  VT_TCKEYREQ_OWNERSHIP,           // A: TCKEYREQ apiConnectPtr not owned by sender
  VT_TCKEYREQ_TABLE_OUT_OF_BOUNDS, // A: table index out of bounds in TCKEYREQ
  VT_REORG_INVALID_OP_TYPE,        // B: reorg flag with invalid operation type
  VT_TCKEYREQ_LONG_SIGNAL_LENGTH,  // B: TCKEYREQ long signal length mismatch
  VT_TCKEYREQ_SHORT_SIGNAL_LENGTH, // B: TCKEYREQ short signal length mismatch
  VT_UNLOCK_WITHOUT_DISTKEY,       // B: UNLOCK without distribution key
  VT_COMMIT_WITHOUT_EXEC,          // B: CommitFlag without ExecFlag
  VT_KEY_LENGTH_EXCEEDED,          // B: key length exceeds MAX_KEY_SIZE_IN_WORDS
  VT_SCANTABREQ_MISSING_SECTION,   // A: SCAN_TABREQ missing required section 0
  VT_SCANTABREQ_INVALID_APICONNECT,// A: invalid apiConnectPtr in SCAN_TABREQ

  // ---- RONDIS (separate per-connection counter system) ----
  VT_RONDIS_OVERSIZE_VALUE,        // B: oversize SET value
  VT_RONDIS_SELECT_OUT_OF_RANGE,   // B: SELECT db index out of range

  // ---- Framework-internal ----
  VT_RATE_LIMIT_EXCEEDED,          // A: Tier C cluster-side safety net breach
  VT_WRONG_SENDER_TYPE_FOR_GSN,    // A: internal-only signal from an API node (scaffold)
  VT_COUNTER_RESET,                // B: operator cleared a node's counters (audit)
  VT_FRAGMENT_INVALID_SECTION_NO,  // A: fragmented signal carried a section number >= 3

  VT_UNKNOWN,                      // fallback for out-of-range/rolling-upgrade values
  NUM_VIOLATION_TYPES              // sentinel — keep last
};

struct ViolationInfo {
  ViolationTier tier;
  const char *reason;  // lowercase_underscore, used in SECURITY_EVENT: log lines
};

/**
 * Canonical table. Header-only (inline) so both SimulatedBlock (sender side,
 * tier derivation) and QMGR (receiver side, reason-string lookup) link the same
 * definition without a separate translation unit or build-system change.
 *
 * Rows are positional and MUST stay in the same order as the ViolationType enum
 * (array designators like [VT_X] = ... are a non-standard C extension, so we
 * rely on order). The static_assert below guards the count; if you add an enum
 * value you will get a compile error until you add the matching row here.
 */
inline constexpr ViolationInfo g_violation_info[NUM_VIOLATION_TYPES] = {
    {TIER_A, "unexpected_api_state"},        // VT_UNEXPECTED_API_STATE
    {TIER_A, "apiconnect_ownership"},         // VT_APICONNECT_OWNERSHIP
    {TIER_A, "start_flag_during_abort"},      // VT_START_FLAG_DURING_ABORT
    {TIER_A, "keyinfo_invalid_apiconnect"},   // VT_KEYINFO_INVALID_APICONNECT
    {TIER_A, "keyinfo_ownership"},            // VT_KEYINFO_OWNERSHIP
    {TIER_B, "keyinfo_signal_length_mismatch"}, // VT_KEYINFO_SIGNAL_LENGTH
    {TIER_A, "attrinfo_invalid_apiconnect"},  // VT_ATTRINFO_INVALID_APICONNECT
    {TIER_A, "attrinfo_ownership"},           // VT_ATTRINFO_OWNERSHIP
    {TIER_A, "attrinfo_signal_too_short"},    // VT_ATTRINFO_SIGNAL_TOO_SHORT
    {TIER_A, "tckeyreq_signal_too_short"},    // VT_TCKEYREQ_SIGNAL_TOO_SHORT
    {TIER_B, "tckeyreq_keyinfo_too_large"},   // VT_TCKEYREQ_KEYINFO_TOO_LARGE
    {TIER_B, "tckeyreq_attrinfo_too_large"},  // VT_TCKEYREQ_ATTRINFO_TOO_LARGE
    {TIER_A, "tckeyreq_invalid_apiconnect"},  // VT_TCKEYREQ_INVALID_APICONNECT
    {TIER_A, "tckeyreq_ownership"},           // VT_TCKEYREQ_OWNERSHIP
    {TIER_A, "tckeyreq_table_out_of_bounds"}, // VT_TCKEYREQ_TABLE_OUT_OF_BOUNDS
    {TIER_B, "reorg_invalid_op_type"},        // VT_REORG_INVALID_OP_TYPE
    {TIER_B, "tckeyreq_long_signal_length"},  // VT_TCKEYREQ_LONG_SIGNAL_LENGTH
    {TIER_B, "tckeyreq_short_signal_length"}, // VT_TCKEYREQ_SHORT_SIGNAL_LENGTH
    {TIER_B, "unlock_without_distkey"},       // VT_UNLOCK_WITHOUT_DISTKEY
    {TIER_B, "commit_without_exec"},          // VT_COMMIT_WITHOUT_EXEC
    {TIER_B, "key_length_exceeded"},          // VT_KEY_LENGTH_EXCEEDED
    {TIER_A, "scantabreq_missing_section"},   // VT_SCANTABREQ_MISSING_SECTION
    {TIER_A, "scantabreq_invalid_apiconnect"},// VT_SCANTABREQ_INVALID_APICONNECT
    {TIER_B, "rondis_oversize_value"},        // VT_RONDIS_OVERSIZE_VALUE
    {TIER_B, "rondis_select_out_of_range"},   // VT_RONDIS_SELECT_OUT_OF_RANGE
    {TIER_A, "rate_limit_exceeded"},          // VT_RATE_LIMIT_EXCEEDED
    {TIER_A, "wrong_sender_type_for_gsn"},    // VT_WRONG_SENDER_TYPE_FOR_GSN
    {TIER_B, "counter_reset"},                // VT_COUNTER_RESET
    {TIER_A, "fragment_invalid_section_no"},  // VT_FRAGMENT_INVALID_SECTION_NO
    {TIER_A, "unknown_violation_type"},       // VT_UNKNOWN
};

static_assert(sizeof(g_violation_info) / sizeof(g_violation_info[0]) ==
                  NUM_VIOLATION_TYPES,
              "g_violation_info[] must have exactly one row per ViolationType; "
              "add the matching row when you add an enum value.");

/**
 * Tier for a violation type, used on the sender side to tag the report.
 * VT_UNKNOWN (and any out-of-range value) defaults to Tier A: an unrecognised
 * violation is treated as maximally severe rather than silently ignored.
 */
inline ViolationTier violation_tier(Uint32 vt) {
  return (vt < NUM_VIOLATION_TYPES) ? g_violation_info[vt].tier
                                    : g_violation_info[VT_UNKNOWN].tier;
}

/**
 * Reason string for a violation type. Out-of-range values (e.g. a newer sender's
 * type unknown to this build) resolve to "unknown_violation_type".
 */
inline const char *violation_reason(Uint32 vt) {
  return (vt < NUM_VIOLATION_TYPES) ? g_violation_info[vt].reason
                                    : g_violation_info[VT_UNKNOWN].reason;
}

#endif
