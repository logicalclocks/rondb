# CTE filter Phase K (restart) — ANTI_JOIN for `WHERE col IS NULL` on LEFT JOIN RHS, kernel + RonSQL

## Status

**Shipped.**  All five steps in place; Tests 28-30 pass under
multi-node MTR (`--suite=ronsql ronsql_cte_basic`).  The kernel
+ RonSQL fix replaced the previous defensive reject from Phase
I.1.

## Context

Phase I.1 closed `IS NULL` / `IS NOT NULL` for INNER JOIN but
defensively rejected `WHERE col IS NULL` on a LEFT JOIN's RHS.
A first attempt at Phase K tried to lift this with a pure-RonSQL
ANTI_JOIN promotion (`match_type = ANTI_JOIN` →
`setMatchType(MatchNullOnly)`).  Test 28 still produced wrong
results: all four parents passed through with their matched data
intact.

Investigation: `MatchNullOnly` correctly translates to
`NI_ANTI_JOIN` in the QueryTree, and `Dbspj::parseDA` sets
`T_FIRST_MATCH` on the tree node from that bit
(`DbspjMain.cpp:14372-14376`).  The API-level anti-join filter at
`NdbResultStream::prepareResultSet` (`NdbQueryOperation.cpp:1387`)
DOES suppress matched rows for non-aggregating queries.  But for
**aggregating** queries — which are RonSQL's CTE bread and butter —
results don't go through `prepareResultSet`; matched rows feed
into the kernel-side aggregator via `Dblqh::cteLookupAggFeed`.
The aggregator never gets told about anti-join, so it processes
matched rows alongside unmatched ones.

The fix needs a kernel signal: tell DBLQH "this is an anti-join,
suppress the agg feed when a match is found".

## Goal

Implement anti-join semantics end-to-end for the
`LEFT JOIN cte WHERE cte.col IS NULL` idiom on aggregating
queries.  Behaviour:

- **Matched parent:** CTE_LOOKUP succeeds, but the row is NOT fed
  into the aggregator.  Treated as if no match was found for the
  aggregation pipeline.
- **Unmatched parent:** existing
  `JOIN_AGG_NULL_ROW_REQ` path fires — DBLQH feeds a NULL-extended
  row into the aggregator.  Unchanged.

End result: only the unmatched parents' NULL rows reach the
aggregator, exactly the user's intent for the LEFT-anti-join
idiom.

## Design — five steps

### Step 1 — kernel signal: new `CTE_LOOKUP_ANTI_JOIN_FLAG`

Add a flag bit in `CteLookupReq::flags`
(`storage/ndb/include/kernel/signaldata/CteLookup.hpp`):

```cpp
static constexpr Uint32 CTE_LOOKUP_ROUTE_FLAG = 0x1;
static constexpr Uint32 CTE_LOOKUP_ANTI_JOIN_FLAG = 0x2;  // NEW
```

Backward-compatible: senders that don't set the bit get the
existing behaviour.  Per the user's standing rule for this branch,
all CTE+pushdown-join code lands together, so no signal-version
gating needed.

### Step 2 — DBSPJ: emit the flag in `cte_lookup_send`

In `Dbspj::cte_lookup_send` (`DbspjMain.cpp:6180`), where
`lookupFlags` is built (around line 6252-6253):

```cpp
Uint32 lookupFlags =
    (m_numDataNodes > 1) ? CteLookupReq::CTE_LOOKUP_ROUTE_FLAG : 0;

// Anti-join: parseDA sets T_FIRST_MATCH from NI_ANTI_JOIN, but
// only ANTI_JOIN distinguishes itself from FirstMatch via the
// absence of T_INNER_JOIN (FirstMatch is a SEMI_JOIN
// optimization that wraps an INNER_JOIN; ANTI_JOIN is a
// LEFT_OUTER variant).
const bool isAntiJoin =
    (treeNodePtr.p->m_bits & TreeNode::T_FIRST_MATCH) &&
    !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN);
if (isAntiJoin) {
  lookupFlags |= CteLookupReq::CTE_LOOKUP_ANTI_JOIN_FLAG;
}
```

The discriminator `T_FIRST_MATCH && !T_INNER_JOIN` matches what
the API uses to set `Is_Anti_Join` on `NdbResultStream`
(`NdbQueryOperation.cpp:937-944`): `MatchNullOnly` is the only
flag combination that sets `NI_ANTI_JOIN` without `NI_INNER_JOIN`.

### Step 2.5 — DBLQH: preserve flag during `routeCteLookup`

When DBSPJ sends `CTE_LOOKUP_REQ` to its local DBLQH and the key
hashes to a remote owner, `Dblqh::routeCteLookup`
(`DblqhMain.cpp:19437`) forwards the request to that owner.  The
forwarder previously did `fwd->flags = 0` to clear the route flag
— which silently stripped every other flag, including the new
`CTE_LOOKUP_ANTI_JOIN_FLAG`.  In a multi-node cluster the remote
owner DBLQH would then run the regular agg-feed path on matched
rows, leaking matched groups into the main aggregator.  Fix:

```cpp
fwd->flags = req->flags & ~CteLookupReq::CTE_LOOKUP_ROUTE_FLAG;
```

The route flag is cleared (no further forwarding); every other
flag survives.  Without this fix, multi-node Tests 28-30 produce
the full LEFT-JOIN result instead of the anti-join filtered one.

### Step 3 — DBLQH: handle the flag in `execCTE_LOOKUP_REQ`

In `Dblqh::execCTE_LOOKUP_REQ` (`DblqhMain.cpp:19511`), the matched
path currently calls `cteLookupAggFeed` to forward the result to
the aggregator.  When the anti-join flag is set, **suppress the
agg feed** but still respond to DBSPJ with `CteLookupConf` so its
`m_outstanding` accounting drains correctly:

```cpp
// Existing match-found path (before agg feed):
if (req.flags & CteLookupReq::CTE_LOOKUP_ANTI_JOIN_FLAG) {
  jam();
  // Anti-join: matched row is suppressed (the user's WHERE
  // col IS NULL filter wants only unmatched parents to feed the
  // aggregator).  Send CONF without invoking cteLookupAggFeed.
  CteLookupConf *conf = (CteLookupConf *)signal->getDataPtrSend();
  conf->senderRef = reference();
  conf->senderData = req.senderData;
  sendSignal(req.senderRef, GSN_CTE_LOOKUP_CONF,
             signal, CteLookupConf::SignalLength, JBB);
  return;
}
// Else: existing agg feed path.
cteLookupAggFeed(signal, req, ...);
```

The unmatched path is **unchanged** — already routes through
`JOIN_AGG_NULL_ROW_REQ` to feed a NULL row to the aggregator
(`DbspjMain.cpp:9533-9651`).  ANTI_JOIN's "deliver only unmatched"
semantic is the unmatched path's existing behaviour, so no edits
on that side.

### Step 4 — RonSQL: detection logic (same as previous attempt)

Same `is_anti_join_promotable` helper and second branch in
`promote_left_to_inner_for_where` as the original Phase K plan.
Promote `LEFT_OUTER` → `ANTI_JOIN` when WHERE consists of `IS
NULL` conjuncts on provably non-NULL CTE columns (GB key, SUM,
COUNT).  Clear `join_where_ce[t]` so emit_cte_lookup_filter
doesn't run.  MIN/MAX outputs stay under I.1's defensive reject
(can be NULL on matched groups via all-NULL source).

### Step 5 — Tests

**Block test** in `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp`:

```cpp
// New test: parent LEFT JOIN cteLookup with anti-join + agg feed.
//   - 5 parent rows: 3 with matching CTE entries, 2 with no match.
//   - Aggregator on the lookup result counts unmatched-only.
//   - Expected: COUNT == 2 (only unmatched parents reach the agg).
```

The test exercises Step 1+2+3 together at the NDB-API level
without RonSQL in the loop.  Pattern matches the existing
`scanCte LEFT JOIN readTuple` tests in the same file (Phase 2).

**MTR tests** in `mysql-test/suite/ronsql/t/ronsql_cte_basic.test`
— restore the original Phase K Tests 28-30:

| # | Shape | Expected |
|---|---|---|
| 28 | `LEFT JOIN sums ... WHERE sums.t IS NULL` (SUM) | `(400, NULL)` only |
| 29 | `LEFT JOIN sums ... WHERE sums.k IS NULL` (GB key) | same |
| 30 | `LEFT JOIN sums ... WHERE sums.t IS NULL AND sums.k IS NULL` | same |

## Files

- `storage/ndb/include/kernel/signaldata/CteLookup.hpp` —
  `CTE_LOOKUP_ANTI_JOIN_FLAG = 0x2`.
- `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` — set flag
  in `cte_lookup_send` based on `T_FIRST_MATCH && !T_INNER_JOIN`.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` —
  handle flag in `execCTE_LOOKUP_REQ` matched path; suppress agg
  feed and send bare CONF.
- `storage/ndb/src/ronsql/RonSQLPreparer.{hpp,cpp}` —
  `is_anti_join_promotable` helper + second branch in
  `promote_left_to_inner_for_where`.
- `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` —
  new test for anti-join + lookupCte + aggregator.
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — Tests
  28-30.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` —
  re-record.
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
  — already references this doc.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_i1.md`
  — update "Limitations" to point at K shipped (replacing the
  current "K aborted" note).

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd \
    ronsql_cli rdrs2 testCteNdbApiOuterJoin testCteNdbApiFilter
cd debug_build/mysql-test
./mtr --suite=ndb_push_agg testCteNdbApiOuterJoin   # block test
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql            # full suite — no regressions
./mtr --suite=ndb_push_agg      # block tests — no regressions
```

## Risks

1. **`m_outstanding` accounting drains.**  The CONF-without-agg-feed
   path must use the same accounting as the existing CONF-after-feed
   path.  Existing CteLookupConf already decrements
   `m_cteLookup_data.m_outstanding` on the DBSPJ side
   (`DbspjMain.cpp:6072-6078`); the suppressed-match path sends the
   same signal so accounting is identical.
2. **Anti-join discriminator collision with FirstMatch.**  The
   `T_FIRST_MATCH && !T_INNER_JOIN` discriminator must not
   misidentify a SEMI_JOIN (FirstMatch over INNER) as ANTI_JOIN.
   `SEMI_JOIN` sets `MatchNonNull | MatchFirst`, which gives
   `NI_INNER_JOIN | NI_FIRST_MATCH` → both `T_INNER_JOIN` and
   `T_FIRST_MATCH` set.  Discriminator returns false for SEMI.
   `ANTI_JOIN` sets `MatchNullOnly`, which gives `NI_ANTI_JOIN` →
   `T_FIRST_MATCH` only (no `T_INNER_JOIN`).  Discriminator
   returns true for ANTI.  Verified against
   `NdbQueryOperation.cpp:925-944`.
3. **Existing CTE_LOOKUP tests in `testCteNdbApiFilter` and
   `testCteNdbApi`.**  None set `MatchNullOnly` today, so the new
   flag bit is always zero and the new branch is never taken.
   Backward-compatible by construction.  Re-run the full block-test
   suite to confirm.
4. **MIN/MAX-over-all-NULL edge case.**  Phase K's promotion still
   excludes MIN/MAX outputs (a matched group can have a NULL
   MIN/MAX from all-NULL source; ANTI_JOIN would skip that legit
   match).  Stays under I.1's defensive reject; documented in the
   helper and in `cte_filter_phase_i1.md`.

## What we're not doing

- **Lookup-without-aggregation anti-join.**  The non-aggregating
  path already works via `NdbResultStream::prepareResultSet`
  line 1387 (`isAntiJoin()` skip).  RonSQL doesn't reach that
  path today (CTE queries always aggregate); if a non-agg CTE
  query becomes reachable later (e.g. via Phase E.3 expansion),
  no kernel changes are needed for that path.
- **Anti-join on a CTE_SCAN child.**  Phase G already rejects
  CTE_SCAN as outer-join child.  Out of scope.
- **`IS NOT NULL OR ...` mixed conjuncts.**  Not in pushdown
  grammar.  I.2 territory.
- **MIN/MAX `IS NULL` lift.**  Stays defensively rejected.
  Lifting needs a post-aggregation re-check of the MIN/MAX value
  — separate phase if a use case appears.
