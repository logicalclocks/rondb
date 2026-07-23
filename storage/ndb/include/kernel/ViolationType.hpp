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
 * string (see g_violation_info[] below). Blocks report a ViolationType; QMGR
 * derives the tier locally from g_violation_info[] and uses the type to resolve
 * the reason string for logging, falling back to VT_UNKNOWN when the value is
 * outside its known range.
 *
 * Design reference: claude_files/data_node_security/tiered_response_policy.md
 *
 * To add a new violation type:
 *   1. Add an enum value before VT_UNKNOWN.
 *   2. Add a matching row to g_violation_info[] at the same index, with the
 *      enum value as the first field (id). The static_assert below will fire
 *      at compile time if the id does not match the row's array position.
 *   3. For a Tier A type, verify it is impossible to trigger via valid SQL /
 *      HTTP / REST / Redis user inputs (the categorization rule). Misclassifying
 *      a user-triggerable violation as Tier A reintroduces punishment laundering.
 *   4. Tier A call sites must NOT call abortErrorLab or releaseAtErrorLab —
 *      see tiered_response_policy.md Section 10 (Tier assignment invariant).
 * Never renumber existing values. The MaliciousSignalReport signal is delivered
 * to the local node's QMGR (QMGR_REF = numberToRef(QMGR, 0)), so the type
 * integer does not cross the network and there is no cross-version wire concern
 * for the signal itself. The reason it is still fixed: the same integer is
 * published as ndbinfo.security_violations.violation_id, a stable external
 * contract that monitoring (Prometheus/Grafana dashboards, the static catalog)
 * indexes by value. Renumbering would silently corrupt historical metrics.
 */

enum ViolationTier : Uint32 {
  TIER_A = 0,  // immediate disconnect: no honest client can trigger
  TIER_B = 1   // log-only forensic observability: a buggy client could trigger
};

enum ViolationType : Uint32 {
  // ---- DBTC (see DbtcMain.cpp call sites; Section 7 catalog) ----
  VT_UNEXPECTED_API_STATE = 0,     // A: signal in unexpected apiConnectRecord state
  VT_APICONNECT_OWNERSHIP = 1,     // A: apiConnectRecord owned by different node
  VT_START_FLAG_DURING_ABORT = 2,      // A: start flag during active abort
  VT_KEYINFO_INVALID_APICONNECT = 3,   // A: invalid apiConnectPtr in KEYINFO
  VT_KEYINFO_OWNERSHIP = 4,            // A: KEYINFO apiConnectPtr not owned by sender
  VT_KEYINFO_SIGNAL_LENGTH = 5,        // B: KEYINFO signal length mismatch
  VT_ATTRINFO_INVALID_APICONNECT = 6,  // A: invalid apiConnectPtr in ATTRINFO
  VT_ATTRINFO_OWNERSHIP = 7,           // A: ATTRINFO apiConnectPtr not owned by sender
  VT_ATTRINFO_SIGNAL_TOO_SHORT = 8,    // A: ATTRINFO signal too short
  VT_TCKEYREQ_SIGNAL_TOO_SHORT = 9,    // A: TCKEYREQ signal too short
  VT_TCKEYREQ_KEYINFO_TOO_LARGE = 10,   // B: TCKEYREQ KeyInfo section too large
  VT_TCKEYREQ_ATTRINFO_TOO_LARGE = 11,  // B: TCKEYREQ AttrInfo section too large
  VT_TCKEYREQ_INVALID_APICONNECT = 12,  // A: invalid apiConnectPtr in TCKEYREQ
  VT_TCKEYREQ_OWNERSHIP = 13,           // A: TCKEYREQ apiConnectPtr not owned by sender
  VT_TCKEYREQ_TABLE_OUT_OF_BOUNDS = 14, // A: table index out of bounds in TCKEYREQ
  VT_REORG_INVALID_OP_TYPE = 15,        // B: reorg flag with invalid operation type
  VT_TCKEYREQ_LONG_SIGNAL_LENGTH = 16,  // B: TCKEYREQ long signal length mismatch
  VT_TCKEYREQ_SHORT_SIGNAL_LENGTH = 17, // B: TCKEYREQ short signal length mismatch
  VT_UNLOCK_WITHOUT_DISTKEY = 18,       // B: UNLOCK without distribution key
  VT_COMMIT_WITHOUT_EXEC = 19,          // B: CommitFlag without ExecFlag
  VT_KEY_LENGTH_EXCEEDED = 20,          // B: key length exceeds MAX_KEY_SIZE_IN_WORDS
  VT_SCANTABREQ_MISSING_SECTION = 21,   // A: SCAN_TABREQ missing required section 0
  VT_SCANTABREQ_INVALID_APICONNECT = 22,// A: invalid apiConnectPtr in SCAN_TABREQ

  // ---- RONDIS (separate per-connection counter system) ----
  VT_RONDIS_OVERSIZE_VALUE = 23,        // B: oversize SET value
  VT_RONDIS_SELECT_OUT_OF_RANGE = 24,   // B: SELECT db index out of range

  // ---- Framework-internal ----
  VT_FRAGMENT_INVALID_SECTION_NO = 25,  // A: fragmented signal carried a section number >= 3

  // ---- DBSPJ (Phase 2 audit; see DbspjMain.cpp parseDA) ----
  VT_SPJ_PARENT_INDEX_OUT_OF_BOUNDS = 26, // A: NI_HAS_PARENT parent index outside built-node range
  VT_SPJ_ATTR_LIST_LENGTH_MISMATCH = 27,  // B: PI_ATTR_LIST declared length exceeds param section
  VT_SPJ_SECTION_LENGTH_MISMATCH = 28,    // B: parseDA declared length (key pattern / interpret
                                          //    program / attr pattern) exceeds its tree/param section
  VT_SPJ_KEY_PARAM_COUNT_OUT_OF_BOUNDS = 29, // A: key-param cnt exceeds MAX_ATTRIBUTES_IN_TABLE
                                             //    (parseDA key params, or scanFrag_build prune params)
  VT_SPJ_SCAN_FRAG_FLAG_INCONSISTENCY = 30, // B: parseScanFrag SF_PRUNE_PATTERN cnt==0 disagrees
                                            //    with SF_PRUNE_PARAMS/SFP_PRUNE_PARAMS flags

  VT_UNKNOWN = 31,                      // fallback for out-of-range/rolling-upgrade values
  NUM_VIOLATION_TYPES = 32              // sentinel — keep last
};

struct ViolationInfo {
  ViolationType id;    // must equal the array index — verified by static_assert below
  ViolationTier tier;
  const char *reason;  // lowercase_underscore, used in SECURITY_EVENT: log lines
};

/**
 * Canonical table. Header-only (inline) so both SimulatedBlock (sender side,
 * tier derivation) and QMGR (receiver side, reason-string lookup) link the same
 * definition without a separate translation unit or build-system change.
 *
 * Each row carries its own ViolationType id. The static_assert below verifies at
 * compile time that every row's id matches its array position — a misplaced or
 * out-of-order row is a build error, not a silent mismatch.
 *
 * To add a new violation type:
 *   1. Add an enum value before VT_UNKNOWN.
 *   2. Add a matching row to g_violation_info[] with the same enum value as id.
 *      Position it at the same index as the enum value (i.e. at the end, before
 *      the VT_UNKNOWN and VT_FRAGMENT_INVALID_SECTION_NO rows).
 *   3. For a Tier A type, verify it is impossible to trigger via valid SQL /
 *      HTTP / REST / Redis user inputs (the categorization rule). Misclassifying
 *      a user-triggerable violation as Tier A reintroduces punishment laundering.
 *   4. Tier A call sites must NOT call abortErrorLab or releaseAtErrorLab before
 *      returning — see tiered_response_policy.md Section 10 (Tier assignment
 *      invariant).
 */
inline constexpr ViolationInfo g_violation_info[NUM_VIOLATION_TYPES] = {
    {VT_UNEXPECTED_API_STATE,      TIER_A, "unexpected_api_state"},
    {VT_APICONNECT_OWNERSHIP,      TIER_A, "apiconnect_ownership"},
    {VT_START_FLAG_DURING_ABORT,   TIER_A, "start_flag_during_abort"},
    {VT_KEYINFO_INVALID_APICONNECT,TIER_A, "keyinfo_invalid_apiconnect"},
    {VT_KEYINFO_OWNERSHIP,         TIER_A, "keyinfo_ownership"},
    {VT_KEYINFO_SIGNAL_LENGTH,     TIER_B, "keyinfo_signal_length_mismatch"},
    {VT_ATTRINFO_INVALID_APICONNECT,TIER_A,"attrinfo_invalid_apiconnect"},
    {VT_ATTRINFO_OWNERSHIP,        TIER_A, "attrinfo_ownership"},
    {VT_ATTRINFO_SIGNAL_TOO_SHORT, TIER_A, "attrinfo_signal_too_short"},
    {VT_TCKEYREQ_SIGNAL_TOO_SHORT, TIER_A, "tckeyreq_signal_too_short"},
    {VT_TCKEYREQ_KEYINFO_TOO_LARGE,TIER_B, "tckeyreq_keyinfo_too_large"},
    {VT_TCKEYREQ_ATTRINFO_TOO_LARGE,TIER_B,"tckeyreq_attrinfo_too_large"},
    {VT_TCKEYREQ_INVALID_APICONNECT,TIER_A,"tckeyreq_invalid_apiconnect"},
    {VT_TCKEYREQ_OWNERSHIP,        TIER_A, "tckeyreq_ownership"},
    {VT_TCKEYREQ_TABLE_OUT_OF_BOUNDS,TIER_A,"tckeyreq_table_out_of_bounds"},
    {VT_REORG_INVALID_OP_TYPE,     TIER_B, "reorg_invalid_op_type"},
    {VT_TCKEYREQ_LONG_SIGNAL_LENGTH,TIER_B,"tckeyreq_long_signal_length"},
    {VT_TCKEYREQ_SHORT_SIGNAL_LENGTH,TIER_B,"tckeyreq_short_signal_length"},
    {VT_UNLOCK_WITHOUT_DISTKEY,    TIER_B, "unlock_without_distkey"},
    {VT_COMMIT_WITHOUT_EXEC,       TIER_B, "commit_without_exec"},
    {VT_KEY_LENGTH_EXCEEDED,       TIER_B, "key_length_exceeded"},
    {VT_SCANTABREQ_MISSING_SECTION,TIER_A, "scantabreq_missing_section"},
    {VT_SCANTABREQ_INVALID_APICONNECT,TIER_A,"scantabreq_invalid_apiconnect"},
    {VT_RONDIS_OVERSIZE_VALUE,     TIER_B, "rondis_oversize_value"},
    {VT_RONDIS_SELECT_OUT_OF_RANGE,TIER_B, "rondis_select_out_of_range"},
    {VT_FRAGMENT_INVALID_SECTION_NO,TIER_A,"fragment_invalid_section_no"},
    {VT_SPJ_PARENT_INDEX_OUT_OF_BOUNDS,TIER_A,"spj_parent_index_out_of_bounds"},
    {VT_SPJ_ATTR_LIST_LENGTH_MISMATCH, TIER_B,"spj_attr_list_length_mismatch"},
    {VT_SPJ_SECTION_LENGTH_MISMATCH,   TIER_B,"spj_section_length_mismatch"},
    {VT_SPJ_KEY_PARAM_COUNT_OUT_OF_BOUNDS,TIER_A,"spj_key_param_count_out_of_bounds"},
    {VT_SPJ_SCAN_FRAG_FLAG_INCONSISTENCY,TIER_B,"spj_scan_frag_flag_inconsistency"},
    {VT_UNKNOWN,                   TIER_A, "unknown_violation_type"},
};

static_assert(
    []() constexpr {
      for (Uint32 i = 0; i < NUM_VIOLATION_TYPES; i++) {
        if (static_cast<Uint32>(g_violation_info[i].id) != i) return false;
      }
      return true;
    }(),
    "g_violation_info[] has an out-of-order or missing entry: each row's id "
    "must equal its array index (i.e. match the ViolationType enum value).");

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
