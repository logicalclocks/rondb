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

#ifndef NDB_CTE_LOOKUP_HPP
#define NDB_CTE_LOOKUP_HPP

#include <ndb_types.h>

/**
 * CTE_LOOKUP_REQ — look up a single group in a materialized CTE hash table.
 *
 * Sent from DBSPJ to DBLQH. Behaves like LQHKEYREQ: if found, the result
 * row is sent as TRANSID_AI via flushAI, then CTE_LOOKUP_CONF follows.
 * If not found, CTE_LOOKUP_REF is sent (LEFT JOIN produces NULL row in DBSPJ).
 *
 * Long section 0: lookup key (AttributeHeader-encoded GROUP BY columns)
 */
struct CteLookupReq {
  Uint32 senderRef;       // DBSPJ block reference
  Uint32 senderData;      // TreeNode pointer (echoed in CONF/REF)
  Uint32 aggStateKey;     // JoinAggregationState pool index (hash table handle)
  Uint32 keyLen;          // Key length in bytes
  Uint32 resultRef;       // FLUSH_AI target: API block reference
  Uint32 resultData;      // FLUSH_AI connect ptr: API receiver ID
  Uint32 routeRef;        // RouteRef for TRANSID_AI_R routing
  Uint32 correlation;     // Parent-child correlation (root receiverId + tuple corr)
  Uint32 joinAggStateKey; // RNIL = send to API via FLUSH_AI;
                          // else = encoded aggStateKey for target JoinAggInterpreter
                          // (CTE_LOOKUP feeds result into aggregation instead of API)
  Uint32 flags;           // CTE_LOOKUP_ROUTE_FLAG: debug fallback for DBLQH routing

  static constexpr Uint32 SignalLength = 10;
  enum { KeySectionNum = 0, AttrInfoSectionNum = 1, LinkedSectionNum = 2 };

  // Flags
  static constexpr Uint32 CTE_LOOKUP_ROUTE_FLAG = 0x1;
  // Anti-join: when set, on a found-match the agg-feed step is
  // suppressed and DBLQH responds with bare CONF.  Used by the
  // LEFT-anti-join idiom (`LEFT JOIN cte WHERE cte.col IS NULL`)
  // to deliver only NULL-injected unmatched parents into the
  // aggregator.  See cte_filter_phase_k.md.
  static constexpr Uint32 CTE_LOOKUP_ANTI_JOIN_FLAG = 0x2;
  // Outer-joined (incl. anti) CTE_LOOKUP that is an intermediate node
  // of an aggregating main query (set by DBSPJ cte_lookup_send).  The
  // exec plan chains the next op behind this node and drives it from
  // this node's result rows, so DBLQH must answer row-shaped: on a
  // miss (or filter reject) emit a NULL-extended result row + CONF
  // instead of REF(GROUP_NOT_FOUND), so the unmatched parent row still
  // continues down the chain (LEFT JOIN semantics).  Combined with
  // CTE_LOOKUP_ANTI_JOIN_FLAG, a found match instead answers
  // REF(GROUP_NOT_FOUND) so the matched parent row is dropped
  // (anti-join semantics).  Not set for pass-through (non-aggregate)
  // requests — those keep REF-on-miss and the API's NULL-fill.
  static constexpr Uint32 CTE_LOOKUP_OUTER_CHAIN_FLAG = 0x4;
};

/**
 * CTE_LOOKUP_CONF — lookup succeeded, result row already sent as TRANSID_AI.
 */
struct CteLookupConf {
  Uint32 senderRef;       // DBLQH block reference
  Uint32 senderData;      // TreeNode pointer (echo back)

  static constexpr Uint32 SignalLength = 2;
};

/**
 * CTE_LOOKUP_REF — lookup failed (group not found, or internal error).
 */
struct CteLookupRef {
  Uint32 senderRef;       // DBLQH block reference
  Uint32 senderData;      // TreeNode pointer (echo back)
  Uint32 errorCode;       // Error code (0 = not found, >0 = internal error)
  Uint32 correlation;     // Echo of CteLookupReq::correlation — lets DBSPJ
                          // locate the parent row on outer-join NULL-row
                          // synthesis, since m_send.m_correlation gets
                          // overwritten by later parent rows.

  static constexpr Uint32 SignalLength = 4;

  // Error codes (mirrors ZCTE_LOOKUP_* in Dblqh.hpp / ndberror.cpp)
  static constexpr Uint32 GROUP_NOT_FOUND = 1263;
  static constexpr Uint32 STATE_NOT_READY = 1264;
  static constexpr Uint32 AGG_FEED_SELF_REFERENCE = 1269;
};

#endif  // NDB_CTE_LOOKUP_HPP
