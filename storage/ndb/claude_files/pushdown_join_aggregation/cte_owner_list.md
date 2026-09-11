# CTE owner list: one decision per query (RONDB-1120)

Status: implemented 2026-09-11, follow-up of finding F-1 in
`node_failure_test_plan.md` (section 11).

## Problem

CTE groups are hashed over an ordered list of data nodes, owner =
hash % count, on three sides: DBLQH when it redistributes groups and
answers probes, DBSPJ when it routes CTE_LOOKUP probes and maps virtual
CTE fragments to nodes, and DBTC when it decides which nodes receive
JOIN_AGG_SETUP_REQ. Each side built its own list from
`getNodeInfo(n).m_connected`: DBLQH per query at SETUP, DBSPJ from a
block-level copy refreshed only at start and on NODE_FAILREP, DBTC per
query. After a node rejoined, DBSPJ still held a one-entry list while
DBLQH hashed over two: every probe went to the local node and the
groups owned by the rejoined node were reported missing, with no error
(F-1: 3108 of 4096 rows).

## Design

- DBTC (`sendJoinAggSetupReqs`) snapshots its connected data nodes
  once per query, preserving the existing SETUP target selection.
  Recovered nodes can serve queries while parked in start phase 110,
  before NDBCNTR includes them in `c_startedNodeSet`; excluding them
  could send aggregation work to a node with no SETUP state.
  DBTC always includes its own node (the constant owner of single-row
  and LIMIT CTEs), sends one SETUP_REQ per node in the
  set and stamps the set into every request as
  `JoinAggSetupReq::setupNodes`. `buildAggKeysSection` already listed
  the same set per CTE (`m_aggNodes`) in the aggKeys section on the
  root SCAN_FRAGREQ.
- DblqhProxy (`execJOIN_AGG_SETUP_REQ`) builds `m_cte_node_list` from
  `setupNodes` in ascending node order. Before it seizes a state it
  checks that every listed node is a connected data node from its own
  point of view; otherwise it answers JOIN_AGG_SETUP_REF with 286 (node
  failure, retryable) instead of building a state whose redistribute or
  FINAL_REP traffic to that node could never be delivered. A request of
  the previous length (`SignalLength_v1`, sent by block unit tests that
  drive DBLQH directly) falls back to the connected data nodes.
- DBSPJ parses the per-CTE node list of the aggKeys section into
  `Request::m_cteOwnerNodes` / `m_cteOwnerCount` (ascending order
  enforced) and routes through `cteOwnerCount` / `cteOwnerNode`:
  CTE_LOOKUP owner selection, virtual fragment K to the K-th owner in
  `cte_scan_start`, the scan-all-nodes fan-out, and the node bitmasks
  the failure handling uses. `m_dataNodeList` and `buildDataNodeList`
  are gone.

## Why the owner list travels with the query

Participants can observe connectivity changes at different times. A
query that begins in that window must not hash over different owner
lists. Every participant therefore uses DBTC's per-query snapshot,
instead of independently rebuilding the list. No new shared mask is
published between NDBCNTR and DBTC threads.

## Wire

`JoinAggSetupReq::SignalLength` 13 to 18 (`setupNodes[NdbNodeBitmask::Size]`).
No version gate: the 26.04 line has no upgrade support. The aggKeys
section is unchanged.

## Tests

- NF-1's post-recovery check (`ndb_cte` `cte_nodefail_close`) covers the
  rejoin case that exposed F-1.
- Planned, section 5.3 of the plan: TD-7 (`setupNodes` listing an
  unreachable node gives REF 286 and seizes no state) and TD-8 (a
  `SignalLength_v1` request builds the connected-node list).
- Regression: the `ronsql_cte*` suites and the block unit tests that
  send SETUP_REQ directly (`testCteLookup`, `testCtePhase6`,
  `testJoinAgg`, `testStarJoinAgg`, `testCaseAgg`).
