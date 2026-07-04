# LOCAL Execution Mode for Pushdown Join Aggregation — Plan

**Status: PLAN (not yet implemented)**

New RonSQL feature: a per-query / per-CTE `LOCAL` modifier that changes the
*placement* of a pushdown-join-aggregation query:

- all SPJ root-fragment work runs on DBSPJ instances on the node where DBTC
  executes (the TC node);
- exactly one aggregation state (per main query / per LOCAL CTE) lives on the
  TC node, so `JOIN_AGG_SETUP_REQ` / `JOIN_AGG_COMPLETE_REQ` /
  `JOIN_AGG_RELEASE_REQ` become node-local signals;
- **row reads are unrestricted** — fragments and lookups are read wherever
  the data lives, exactly as today; a row read on another node is shipped
  over the network to the single aggregation state (the networked agg feed,
  the one genuinely new mechanism in this plan).

LOCAL changes where coordination and aggregation happen, not where rows are
read.

## 1. Motivation and trade-off

Today every aggregating pushed join pays a cluster-wide control-plane tax
regardless of query size:

- `sendJoinAggSetupReqs` fans SETUP out to **every connected DB node** — for
  the main aggregation and again per CTE (`DbtcMain.cpp:30022-30024`,
  `:30117-30119`); COMPLETE and RELEASE iterate the same node set
  (`DbtcMain.cpp:31241-31335`, `:31382-31432`). That is 3×(1+numCTEs)
  round-trip waves × N nodes, plus per-node interpreter/state setup.
- Multi-node CTEs additionally pay the group redistribute + all-to-all
  `JOIN_AGG_FINAL_REP` + `checkCteReady` barrier
  (`DblqhMain.cpp:21229-21460`, `:21836-21892`).
- Root fragments scatter across DBSPJ instances on all fragment-owning nodes
  (`DbtcMain.cpp:18042-18055`).

The trade LOCAL makes selectable per query:

| | Distributed (today) | LOCAL |
|---|---|---|
| Agg feed | always node-local (rows aggregate where read) | local when the row is read on the TC node, else one feed signal per row over the network |
| SETUP/COMPLETE/RELEASE | N-node fan-out waves | node-local signals |
| CTE completion | redistribute + all-to-all barrier | none (single state) |
| Per-node state/interpreter setup | N× | 1× |

LOCAL wins when the number of rows actually reaching the aggregation feed is
modest (selective WHERE, lookup-driven joins, online Feature-Store-style
queries from the `fs_*` family) and the fixed fan-out cost dominates.
Distributed mode remains right for wide scans feeding millions of rows. On
the common RonDB deployment — single node group with ReadBackup tables —
DBSPJ's existing own-node replica preference makes most reads land on the TC
node anyway, so LOCAL queries there run with near-zero cross-node traffic.

## 2. Syntax

There is **no standard SQL** for execution placement. Industry practice is
optimizer hints (MySQL/Oracle `/*+ ... */`, SQL Server `OPTION(...)`).
RonSQL's lexer has no comment support at all (`RonSQLLexer.l` — `/`, `*`, `+`
tokenize individually), so hint-comments would require new lexer machinery.
The `LOCAL` keyword is the better fit:

- `LOCAL` is **already reserved** in RonSQL — it sits in
  `keywords_defined_in_mysql[]` (`Keywords.hpp:458`), so it can never be an
  unquoted identifier today. Promoting it to
  `keywords_implemented_in_ronsql[]` breaks nothing.
- There is a direct precedent for a statement-leading modifier: `EXPLAIN`
  (`explain_opt`, `RonSQLParser.y:345-347`) which sets a bool on
  `SelectStatement` (`RonSQLCommon.hpp:254`).

Grammar (all statement forms):

```
[EXPLAIN] [LOCAL] [WITH x AS ( [LOCAL] SELECT ... ) [, ...]] SELECT ...
```

- **Statement-head `LOCAL`** (before `WITH`/`SELECT`): whole execution
  placement is local — main aggregation state, *all* CTE states, and SPJ
  root-fragment placement on the TC node.
- **Per-CTE `LOCAL`** (`WITH x AS (LOCAL SELECT ...)`): only that CTE's
  aggregation state is single-node (on the TC node); the rest of the query
  stays distributed. Useful for a small, heavily-probed CTE that should skip
  the redistribute barrier while the big main scan stays distributed. Depends
  on the networked feed (a CTE body materialized by a distributed main query
  reads rows on many nodes), so it is parsed from day one but enabled in the
  feed phase.

Once complete, LOCAL never fails because of data placement — reads are
unrestricted. Errors remain only for old-version clusters (capability gate)
and unsupported statement shapes (single-table path, §6 Phase 2).

## 3. Current mechanics this plan builds on (facts)

| Mechanism | Where | Relevance |
|---|---|---|
| SETUP fan-out = all connected DB nodes | `DbtcMain.cpp:30022-30024` (main), `:30117-30119` (CTE) | The loop to restrict |
| Single-node CTE setup already exists (`CTE_SINGLE_ROW` → one node, break after first) | `DbtcMain.cpp:30111-30124` | End-to-end precedent: one-node state, nodeCount=1 key triples |
| COMPLETE/RELEASE iterate the SETUP-confirmed bitmask `m_aggNodes` | `DbtcMain.cpp:31318-31322`, `:31420-31422` | Follow automatically once SETUP is restricted |
| Per-node agg keys → DBSPJ via extra SCAN_FRAGREQ section | `DbtcMain.cpp:19477-19480`, parse `DbspjMain.cpp:1558-1662` | Format already marker-structured (`CTE_KEYS_MARKER`), extensible |
| SPJ instance node = fragment's preferred replica node; own-node forced only for `scanNoFrag==1` pruned scans | `DbtcMain.cpp:18042-18055` | Generalize the own-node arm to all fragments |
| DBSPJ drives root/child fragment scans on **remote** DBLQH already (per-fragment DIH resolve, remote send path) | `DbspjMain.cpp:11379-11496`, `:12657-12886` | Local SPJ placement needs no new read machinery |
| DBSPJ prefers the own node among replicas for READ_BACKUP tables (root scans and lookups) | `DbspjMain.cpp:11454-11470`, `:10549-10565` | Keeps most feeds local on single-node-group clusters, for free |
| Leaf agg key attached per *reading* node: `m_aggStateKeys[nodeId]` into LQHKEYREQ `variableData[var_index+4]` / SCAN_FRAGREQ `variableData[var_index+2]` | `DbspjMain.cpp:8626-8635`, `:12684-12690` | The per-reading-node assumption LOCAL replaces with owner-node key + owner ref |
| Networked row-feed precedent: `cte_lookup_send` routes a row + source/target agg keys to a chosen node's owner LDM; `cteLookupAggFeed` feeds the target interpreter from buffers only (no live tuple; Step 4d `tablePtrP=nullptr` hardening) | `DbspjMain.cpp:6540-6849`, `DblqhMain.cpp:19556-19667` | Template for the remote feed receive side |
| Typed column packing into linked buffers; buffer-based column loads | `buildCteLinkedBuffer` `DblqhMain.cpp:19404`; `loadColumnTypedFromBuf` (Step 4b) | Template for feed payload encode/decode |
| CTE single-node short-circuit: `m_cte_num_nodes <= 1` skips redistribute/FINAL_REP entirely | `DblqhMain.cpp:18937-18951` | Fires automatically once the state's node list is `{own}` |
| Non-CTE aggregation has **no cross-node merge in the kernel** — each node streams partial groups to the API, API merges | `DblqhMain.cpp:19129-19173`, `JoinAggregationState.hpp:208-211` | With one state the API receives one already-merged stream; no protocol change |
| Scalar CTE redistribute owner is already "the TC-co-located node" (`refToNode(m_senderRef)`) | `DblqhMain.cpp:21278-21297`, `:20564-20571` | LOCAL degenerates this to a no-op |
| `JoinAggregationState::m_total_ops_expected` exists, is set from `JoinAggSetupReq::expectedOpCount` (always 0 today) and **is never used as a barrier** | `DblqhProxy.cpp:2493`; per-row counter `m_completed_ops` maintained at `DbtupExecQuery.cpp:5274`, `DblqhMain.cpp:19660` etc. | Dormant fields tailor-made for the feed completion barrier |
| Agg-consumed rows never count into `Request::m_rows` (self-continue / DELIVERED-hang invariants) | `DbspjMain.cpp:6916-6919`, `:13195-13199`, `:3712-3718` | Feed rows must preserve this |
| ScanTabReq requestInfo: **all 32 bits allocated** | `ScanTab.hpp:171-265` | Flag must ride elsewhere |
| Extended requestInfo = upper 16 bits of `storedProcId` (in-flight work, bits 31/30 already used; today the API sends 0xFFFF, `NdbQueryOperation.cpp:4528`) | in-flight branch | LOCAL takes the next free extended bit |
| Version-gating template | `ndb_version.h.in:1191-1239`, API gate `NdbQueryOperation.cpp:4602-4618`, kernel re-check `DbtcMain.cpp:16606-16608` | Copy exactly |

## 4. Design

LOCAL binds to **aggregation-state scope**: the main query and each CTE own
one `JoinAggregationState` per participating node today; LOCAL shrinks the
participating set of the chosen scope(s) to `{TC node}`.

### 4.1 Placement (mostly configuration of existing machinery)

1. **DBTC** sends SETUP for LOCAL scopes only to its own node (local signal
   to its own DblqhProxy). `m_aggNodes` then holds one bit, so the
   COMPLETE and RELEASE loops need **zero change**.
2. **DblqhProxy SETUP** learns the mode from a `LOCAL_MODE_FLAG` in
   `JoinAggSetupReq::concurrencyStrategy` (next to `CTE_MODE_FLAG`) and
   builds `m_cte_node_list = {own node}` instead of all connected DB nodes
   (`DblqhProxy.cpp:2476-2486`). The existing `m_cte_num_nodes <= 1`
   short-circuit then removes redistribute + FINAL_REP + `checkCteReady`
   from the completion path with no further edits.
3. **DBTC SPJ placement** (whole-query LOCAL only): every root fragment gets
   a DBSPJ ref on the own node — generalizing the existing
   `nodeId == ownNodeId && scanNoFrag == 1` arm at `DbtcMain.cpp:18047-18051`
   to all fragments, keeping `cspjInstanceRR` for instance spread across the
   local TC threads. `m_fragsPerWorker == 1` already holds for agg queries,
   so the MultiFrag path is untouched. DBSPJ then resolves the actual data
   reads itself, exactly as today: local replica when READ_BACKUP offers one,
   remote DBLQH otherwise (`DbspjMain.cpp:11453-11474`, `:12657-12886`).
4. **DBSPJ owner routing**: for a LOCAL scope, consumers and feed targets
   derive the owner from the per-scope key map (which now has exactly one
   node) instead of `m_dataNodeList[h % m_numDataNodes]` — affects
   `cte_lookup_send` (`:6636-6637`) and `cte_scan_sendReq` (`:7597-7637`).
   Verify how `CTE_SINGLE_ROW` consumers route today and generalize that.

### 4.2 The networked agg feed (the core new mechanism)

Where a row is read on a node other than the aggregation state's node, the
reading DBLQH ships the aggregation inputs to the state's owner LDM.

**What the interpreter needs per row**: `processRecWithLinkedAttrs` consumes
(a) the linked-attr buffer (already carried *in the request* — the AttrInfo
subroutine region, `DbtupExecQuery.cpp:5156-5202`) and (b) the leaf row's
columns loaded by the program (`kOpLoadCol` / GROUP BY columns, today read
from the live tuple). The reading node has neither state nor program in LOCAL
mode, so the required leaf columns must be declared **in-band in the
request**:

- The **API** (which owns the agg programs — `NdbAggregator`) computes each
  leaf's column read-set at `prepareAggregation()` time and serializes it
  with the query (per-op parameter / attrinfo read-set, gated on LOCAL).
- **DBSPJ** attaches to each leaf request feeding a LOCAL scope: the owner
  node's encoded state key (instead of the reading node's), plus one new
  word `ownerRef = numberToRef(DBLQH, ownerInstance, ownerNode)`. To have
  `ownerInstance` for the main scope, the main entries of the aggKeys
  section become `[nodeId, key, ownerInstance]` triples like the CTE entries
  (format change covered by the version gate). The
  `ndbrequire(m_aggNodes.get(nodeId))` at `DbspjMain.cpp:12686` becomes a
  LOCAL-aware branch.
- The **reading DBLQH/DBTUP**, seeing `refToNode(ownerRef) != own node`,
  skips the local `getJoinAggState` entirely (the key is a pool index valid
  only on the owner node): it materializes the declared read-set into the
  per-LDM scratch (Step 4a `getAggAttrReadBuf` precedent), appends the
  columns as typed entries to the linked buffer
  (`buildCteLinkedBuffer`-style encoding), and sends
  **`GSN_JOIN_AGG_FEED_REQ`** (new signal, `JoinAgg.hpp`) to `ownerRef` with
  the buffer as a section (`sendBatchedFragmentedSignal` for large rows).
- The **owner LDM** handler feeds `processRecWithLinkedAttrs` in
  buffer-only mode — the shape `cteLookupAggFeed` already runs (Step 4d
  `tablePtrP = nullptr` hardening), with leaf-column loads resolving from
  the shipped buffer via the `loadColumnTypedFromBuf` machinery (Step 4b),
  including the `AGG_EVICT_NEEDED` retry loop, and increments
  `m_completed_ops`.

Rows read *on* the owner node keep today's zero-copy local feed path
unchanged — both paths coexist per query.

Outer-join NULL-row injection needs no feed: `JOIN_AGG_NULL_ROW_REQ` is sent
by DBSPJ itself (`DbspjMain.cpp:10141-10167`), which in whole-query LOCAL is
co-located with the state.

### 4.3 Feed completion barrier

DBTC sends COMPLETE only after all scan/lookup CONFs; an in-flight feed
(reading node → owner) must not lose the race against COMPLETE
(DBTC → owner). Two options:

- **Option A — transporter ordering (zero new protocol)**: in whole-query
  LOCAL, DBSPJ, DBTC and the owner DBLQH are all on the same node. If the
  reading DBLQH sends `JOIN_AGG_FEED_REQ` *before* the corresponding
  LQHKEYCONF / SCAN_FRAGCONF toward that node, per-link signal ordering
  delivers the feed before DBSPJ/DBTC can react to the CONF, transitively
  before COMPLETE. **Caveat**: RonDB multi-socket transporters may not
  preserve ordering across different receiver threads (owner LDM vs SPJ
  instance) — must be verified before relying on this.
- **Option B — feed counting via the dormant `expectedOpCount` (recommended)**:
  each reading op reports how many feed rows it produced (lookups: 0/1 in an
  LQHKEYCONF spare/flag; scans: a new `aggFeedRows` word in SCAN_FRAGCONF,
  version-gated exactly like `rowsExamined`, `ScanFrag.hpp:272-274`). DBSPJ
  sums per request; DBTC accumulates and sends the total in
  `JOIN_AGG_COMPLETE_REQ` (reviving `expectedOpCount` /
  `m_total_ops_expected`). The owner defers finalize until
  `m_completed_ops` reaches the expected total — the same
  defer-then-continue shape as the redistribute queue
  (`DblqhMain.cpp:21557-21600`). Robust regardless of transporter layout.

Plan assumes Option B unless Option A's ordering guarantee is confirmed.

Invariants preserved: feed rows never count into `Request::m_rows` (the
API-visible row flow is unchanged — results still only arrive after
COMPLETE); `rowsExamined` is still counted at the reading scan; batch flow
control / `handleJoinAggNextBatch` self-continue untouched.

## 5. Flag transport

```
RonSQL AST (SelectStatement::execution_mode, per stmt incl. CTE stmts)
  → NdbQueryBuilder: query-wide setLocalExecution() + per-CTE local flag
    → SCAN_TABREQ: extended-requestInfo bit in storedProcId upper 16 bits
      (whole-query); per-CTE bit in the serialized CTE metadata that
      parseJoinAggKeyInfo already reads (precedent: the CTE_SINGLE_ROW flag)
      → DBTC: scanP->m_joinAggLocal / per-CTE local flags
        → JoinAggSetupReq: LOCAL_MODE_FLAG in concurrencyStrategy
        → aggKeys SCAN_FRAGREQ section: explicit flags word + main-entry
          triples [nodeId, key, ownerInstance] (DbtcMain.cpp:30341-30429)
          → DBSPJ: Request-level local flag + per-CTE local flags
            → leaf LQHKEYREQ / SCAN_FRAGREQ: owner key + ownerRef word
```

- `ndb_version.h.in`: `ndbd_support_local_join_agg()` following the
  `ndbd_support_agg_wide_type` template (`:1222-1239`). One gate covers all
  the section/signal format extensions above.
- API gate before setting the bit (`NdbQueryOperation.cpp` doSend, next to
  the existing agg gates at `:4602-4618`); DBTC re-checks with the 4003
  pattern (`DbtcMain.cpp:16606-16608`).
- Exact extended-bit number in `storedProcId`: **coordinate with the
  in-flight extended-requestInfo work** (bits 31 and 30 already used there);
  LOCAL claims the next free bit with `get/setLocalExecFlag` accessors in
  `ScanTab.hpp`.

## 6. Phases

Phased so each step is testable, but the goal — remote reads feeding the
TC-node aggregation state — is the committed end state, not an optional
extension.

### Phase 0 — Flag transport + NDB API surface (small)

- `ndb_version.h.in`: `ndbd_support_local_join_agg`.
- `ScanTab.hpp`: extended-requestInfo accessors on `storedProcId` (rebase on
  the in-flight work; claim next free bit).
- `NdbQueryBuilder.hpp/.cpp`, `NdbQueryBuilderImpl.hpp`: builder-level
  `setLocalExecution()` stored on `NdbQueryDefImpl` (alongside the derived
  `m_hasAggregation`, `NdbQueryBuilder.cpp:1705-1740`); per-CTE `local` flag
  on the CTE definition entry (`m_cteDefs`), serialized next to the existing
  single-row flag.
- `NdbQueryOperation.cpp` doSend: version gate + set the extended bit; error
  `Err_FunctionNotImplemented` on old clusters.
- `JoinAgg.hpp`: `LOCAL_MODE_FLAG` next to `CTE_MODE_FLAG`.

### Phase 1 — Placement + local signals (small; interim read-locality gate)

DBTC (`DbtcMain.cpp`):
- `execSCAN_TABREQ`: parse extended bit → `scanP->m_joinAggLocal`; per-CTE
  flags in `parseJoinAggKeyInfo`.
- `sendJoinAggSetupReqs`: LOCAL scopes iterate `{getOwnNodeId()}` only
  (whole-query LOCAL ⇒ main + every CTE; template: the `CTE_SINGLE_ROW`
  one-node arm `:30111-30124`); set `LOCAL_MODE_FLAG`. COMPLETE/RELEASE:
  no change.
- `sendDihGetNodeReq`: when `m_joinAggLocal`, pin the DBSPJ ref node to
  `getOwnNodeId()` for every fragment (instance via `cspjInstanceRR`);
  fragment data location still resolved by DBSPJ.
- aggKeys section: flags word + main triples (§5).

DblqhProxy: `execJOIN_AGG_SETUP_REQ` builds `m_cte_node_list = {own node}`
on `LOCAL_MODE_FLAG` — the single-node short-circuit and the scalar owner
rule then just work.

DBSPJ: parse the flags word; owner routing from the per-scope key maps
(§4.1.4); attach owner key + ownerRef to leaf requests (§4.2). **Interim
gate**: until Phase 3, if a leaf request feeding a LOCAL scope resolves to a
node ≠ owner node, abort the request with new error **1271
`ZLOCAL_AGG_REMOTE_READ`** — a temporary scaffold (not feature semantics)
that keeps Phase 1 shippable and self-verifying; it never fires on
single-node-group + ReadBackup clusters, where reads resolve to the TC node
via the existing own-node preference.

Block tests: `testJoinAggNdbApi` / `testJoinAggSpj` LOCAL variants — on one
node group expect success + results identical to distributed runs (all agg
types, GROUP BY, CTE shapes, outer joins); on ≥2 node groups expect the
interim 1271 until Phase 3.

### Phase 2 — RonSQL syntax, plumbing, EXPLAIN, MTR (small)

- `Keywords.hpp`: `kwdef(LOCAL)` into `keywords_implemented_in_ronsql[]`
  (sorted between `LIMIT` and `MAX`); stays in the MySQL list — the only
  combo `KeywordsUnitTest.cpp:213` accepts.
- `RonSQLParser.y`: token `T_LOCAL`; `local_opt` nonterminal; statement head
  becomes `explain_opt local_opt cte_opt T_SELECT ...` setting
  `ast_root.execution_mode`; `cte_def` gains `local_opt` before its inner
  `T_SELECT`.
- `RonSQLCommon.hpp`: `enum ExecutionMode { EXEC_DISTRIBUTED, EXEC_LOCAL }`
  on `SelectStatement` (default distributed), next to `do_explain`.
- Prepare-time rules (`require_prm` / `RonSQLPermanentError` pattern):
  statement-head LOCAL propagates to all CTE stmts; LOCAL on the
  single-table (non-join, non-CTE) path rejected ("LOCAL requires a join/CTE
  aggregation query" — that path has no SETUP/COMPLETE state to place);
  per-CTE LOCAL without statement-head LOCAL → `feature_not_implemented`
  until Phase 4.
- `execute_join()`: `qb->setLocalExecution()`; per-CTE flags at CTE emission.
- EXPLAIN `print()`: `[LOCAL]` annotation on the `[ROOT]` line (`:11552`)
  and CTE `Body root:` lines — same style as `[I.10 MIN_ASC maxRows=1]`
  (`:11529-11535`).
- MTR: `ronsql_local_exec.test` in the `ronsql_cte` suite + topology
  siblings. Core pattern: run existing green-envelope aggregate shapes twice
  (distributed vs LOCAL) via `ronsql_compare.inc` — results must be
  identical. ng1r3 exercises multi-node success already in Phase 1;
  ng2r2/ng2r3/ng4r2 hit the interim 1271 until Phase 3 flips them to
  result-compare.

### Phase 3 — The networked agg feed (the core mechanism; moderate)

Implements §4.2 + §4.3 and removes the interim 1271 gate:

- `JoinAgg.hpp`: `GSN_JOIN_AGG_FEED_REQ` (+ REF for error paths); Option B
  counting fields (`aggFeedRows` in SCAN_FRAGCONF next to `rowsExamined`;
  LQHKEYCONF feed indication; `JoinAggCompleteReq::expectedOpCount` revived).
- NDB API: per-leaf column read-set emission at `prepareAggregation()`
  (`NdbQueryOperation.cpp:3906-3971`), serialized per-op, gated on LOCAL.
- DBSPJ: pass the read-set through to leaf requests; feed-count aggregation
  into its CONFs toward DBTC.
- DBTC: accumulate feed counts; carry the total in COMPLETE.
- Reading-side DBLQH/DBTUP: the branch replacing the local feed when
  `refToNode(ownerRef) != own node` — read-set materialization, typed
  packing, batched send.
- Owner-side DBLQH: `execJOIN_AGG_FEED_REQ` — buffer-only
  `processRecWithLinkedAttrs` (Step 4d/4b machinery), eviction retry,
  `m_completed_ops`; COMPLETE defers finalize until the expected count is
  reached (redistribute-queue defer pattern).
- Tests: block tests forcing remote feeds (≥2 node groups); flip the
  ng2r2/ng2r3/ng4r2 MTR suites to result-compare; ERROR_INSERT for
  feed/COMPLETE race coverage (delay a feed, assert COMPLETE waits).

### Phase 4 — Per-CTE LOCAL + benchmarks (small)

- Remove the Phase 2 per-CTE reject; per-CTE LOCAL states get the same feed
  path (CTE body leaf rows read anywhere ship to the one CTE state; the
  distributed main query is untouched).
- rondb-cli: LOCAL variants of selected `fs_*` / `cte_tpch_q*` named queries
  in `tools/rondb-cli/internal/shell/ronsql_bench.go` for `.bench_ronsql`
  A/B latency; measure the §8.4 parallelism question.

## 7. What deliberately does NOT change

- COMPLETE/RELEASE send loops, the AggCompleteRecord requestId/dedup
  machinery (Phase L), heartbeats — all keyed off `m_aggNodes`, which simply
  contains one node.
- Read routing/replica selection — DIH resolution, READ_BACKUP own-node
  preference, location domains all behave exactly as today.
- The API-side result merge — it already merges per-node partial streams;
  with LOCAL it receives one (already fully merged) stream.
- Non-LOCAL queries: every new branch is behind the flag; distributed
  behavior is bit-for-bit untouched.
- MySQL handler (mysqld pushdown) — RonSQL-only feature for now.
- TC-node choice — the client already controls it
  (`Ndb::startTransaction(nodeId, ...)` hints); LOCAL composes: execution
  lands wherever the transaction's TC is.
- Single-table aggregation path (ScanTabReq bit 7) — no shared state; LOCAL
  rejected in RonSQL.

## 8. Open questions

1. **Extended-bit number** in `storedProcId` upper 16 bits — assigned when
   rebasing on the in-flight extended-requestInfo work (bits 31/30 taken).
2. **Barrier choice** (§4.3): confirm or refute per-link ordering under
   RonDB multi-socket transporters; if confirmed, Option A saves the
   counting plumbing, else Option B (recommended default).
3. **Read-set emission point**: per-op QueryTree parameter vs extending the
   linked-attr subroutine region — decide when writing Phase 3 (whichever
   lets the reading node reuse the most of the existing attrinfo read
   machinery).
4. **Parallelism on one node**: all root fragments on own-node SPJ instances
   round-robined via `cspjInstanceRR` — enough TC/recv-thread spread, or
   should LOCAL cap `scanParallelism`? Measure in Phase 4 benchmarks.
5. **Non-aggregating pushed joins**: should statement-head LOCAL also pin
   SPJ placement for projection-only join queries (no agg states)? Cheap to
   add (the DBTC placement change is shared), deferred for scope.

## 9. Estimated impact

| Phase | Size | Nature |
|---|---|---|
| 0 transport/API | ~150 lines | flags, gates, one builder setter |
| 1 placement | ~250-300 lines | loop restrictions, one node-list init, owner routing, interim gate |
| 2 RonSQL | ~200 lines + tests | keyword, 2 grammar sites, enum, rejects, EXPLAIN annotation |
| 3 networked feed | ~500-700 lines + tests | the one genuinely new mechanism: read-set emission, feed signal, buffer-only feed, counting barrier |
| 4 per-CTE + bench | ~100 lines | reject removal, benchmark registry |

Phases 0-2 are configuration of existing machinery. Phase 3 is the real
mechanism, and every piece of it composes precedented components: CTE feed
receive shape (4d), typed buffer packing (`buildCteLinkedBuffer`),
buffer-based column loads (4b), version-gated CONF fields (`rowsExamined`),
and the dormant `expectedOpCount`/`m_completed_ops` pair for the barrier.
