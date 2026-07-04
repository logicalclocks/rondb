# Aggregation as a Query-Tree Node — Alternative Plan

**Status: PLAN / INVESTIGATION (not yet implemented).** Alternative to the
feed mechanism in `local_execution_mode_plan.md` §4.2/§4.3 (its Phase 3).
The LOCAL syntax, flag transport, and placement phases of that plan (Phases
0-2) are unaffected by this choice — this document decides *how rows reach
the aggregation state*, not where the state lives.

## 1. Concept

Today the aggregation is invisible to the query tree: the last table's op is
tagged `NI_AGGREGATE_LEAF`, DBSPJ smuggles a per-node agg state key into the
leaf LQHKEYREQ/SCAN_FRAGREQ, and the *reading* DBLQH/DBTUP runs the
interpreter in place (`handleJoinAggRow`), consuming the row where it is
read.

The alternative makes aggregation an explicit operation: a new QueryTree
node type (**`QN_AGGREGATE`**, next free op type 0x9; 0x5 is a historical
gap) serialized as the last node of the tree, child of the former "last
table" op. The last table becomes a normal internal node:

```
today:    root ─ t2 ─ t3(leaf, NI_AGGREGATE_LEAF, interpreter runs at reading LQH)
proposed: root ─ t2 ─ t3(internal, NI_LINKED_ATTR projection) ─ AGG(leaf)
```

- SPJ sends the reads for the last table as for any internal node
  (`NormalProtocolFlag=1`, so its TRANSID_AI rows route to DBSPJ, not the
  API — `DbspjMain.cpp:8551-8555`); its projection is a standard
  `NI_LINKED_ATTR` column list + `CORR_FACTOR32`
  (`DbspjMain.cpp:15418-15450`).
- The aggregation TreeNode receives each row via the standard
  `m_parent_row` hook, assembles the aggregation inputs (last-table columns
  from the incoming row + any ancestor columns via the existing
  `appendFromParent` pattern machinery, `DbspjMain.cpp:14450-14594`), and
  sends one **`JOIN_AGG_FEED_REQ`** to the DBLQH `JoinAggregationState` it
  is routed to. The state, interpreter, and the DBTC SETUP/COMPLETE/RELEASE
  orchestration remain exactly as today.

**Orthogonality to LOCAL** (the point of this alternative): the feed target
comes from the same per-node key map SPJ already parses
(`Request::m_aggStateKeys[nodeId]`, `DbspjMain.cpp:1566-1653`):

| Placement | Feed routing | Feed cost per row |
|---|---|---|
| LOCAL (single state on TC node) | the one state — and SPJ is pinned to that node, so the feed is a **node-local signal** | row already travels reading-LQH → SPJ (standard internal-node TRANSID_AI); feed adds a local hop |
| Distributed (state per node, today's placement) | `m_aggStateKeys[getOwnNodeId()]` — each SPJ instance feeds its own node's state; partial-state merge/redistribute unchanged | remote-read rows now travel to the SPJ node (today they aggregate in place for free) |
| Future: hash-routed | owner node by GROUP BY hash (exactly `cte_lookup_send`'s owner routing, `DbspjMain.cpp:6610-6649`) | one network leg per row; **eliminates** CTE redistribute *and* the API-side partial-group merge — kernel-complete GROUP BY |

Same node type, same code — only the routing entry differs.

## 2. Why this shape is credible: the `cte_lookup` precedent

`cte_lookup` is a full TreeNode type doing almost exactly this already:

- Registered OpInfo table `g_CteLookupOpInfo` (`DbspjMain.cpp:6046-6065`)
  dispatched from `getOpInfo`'s op-type switch (`DbspjMain.cpp:14089-14109`);
  the CTE trio (QN_CTE_SUBTREE/LOOKUP/SCAN = 0x6/0x7/0x8) is the most recent
  node-type addition and the template to copy.
- Per parent row (`cte_lookup_parent_row`, `DbspjMain.cpp:6370-6434`) it
  builds a signal from the parent row + linked ancestor attrs and sends it
  to a *specific node's DBLQH agg machinery* (`cte_lookup_send`,
  `DbspjMain.cpp:6540-6846`); when it is itself an aggregate feed it sets
  `joinAggStateKey` and expects **only a CONF, no TRANSID_AI back**
  (`cnt = 1`, `DbspjMain.cpp:6811-6814`).
- Completion is structural: send bumps `Request::m_outstanding` + the
  node-local counter (`DbspjMain.cpp:6828-6830`); CONF/REF decrement and set
  `m_completed_tree_nodes` at zero (`execCTE_LOOKUP_CONF`,
  `DbspjMain.cpp:6869-6886`); `checkBatchComplete` gates the batch on
  `m_outstanding == 0` (`DbspjMain.cpp:3511-3545`). **The request cannot
  report complete to DBTC while a feed is in flight** — the feed/COMPLETE
  race that `local_execution_mode_plan.md` §4.3 needed a counting protocol
  for simply cannot occur here.
- The receive side (`execCTE_LOOKUP_REQ` → `cteLookupAggFeed`,
  `DblqhMain.cpp:19999`, `:19556-19667`) already feeds an interpreter from
  buffers only (`tablePtrP = nullptr`, Step 4d hardening), with the
  `AGG_EVICT_NEEDED` retry loop.

The new `QN_AGGREGATE` op is `cte_lookup` minus the CTE probe (no hash
lookup into a materialized table, no CteContext readiness state machine, no
cached-row path) plus trivial routing — its DBSPJ footprint should be
*smaller* than cte_lookup's (~1000 lines including send/CONF/REF).

## 3. What each layer looks like

### 3.1 NDB API (serialization)

- The API **synthesizes** the aggregation node when `setAggregation()` was
  attached — the user/RonSQL API is unchanged. Precedents for nodes the
  user never declared: the unique-index two-node expansion
  (`NdbQueryBuilder.cpp:3386-3537`, internalOpNo+1 reservation at
  `:388-390`) and the `QN_CTE_SUBTREE` container op on a dummy table
  (`s_cteDummyTable`, `NdbQueryBuilder.cpp:87, 356-365`).
- Invariants are all satisfiable: parents must precede children
  (`appendParentList` asserts monotonic ordering,
  `NdbQueryBuilder.cpp:2507-2517`) — the agg node takes the highest
  internalOpNo; node count = last internalOpNo + 1
  (`NdbQueryBuilder.cpp:1766-1770`); +1 node (+1 per aggregate leaf, see
  multi-leaf below) against `NDB_SPJ_MAX_TREE_NODES = 32`
  (`ndb_limits.h:403`); receivers are allocated per op automatically
  (`m_workerCount * getNoOfOperations()`, `NdbQueryOperation.cpp:3758`).
- The former leaf's needed columns become its normal `NI_LINKED_ATTR`
  child projection (`appendChildProjection`, `NdbQueryBuilder.cpp:2522-2538`);
  ancestor columns the program needs are the agg node's param-pattern
  references (`appendParamConstructor` / `NI_ATTR_LINKED`,
  `NdbQueryBuilder.cpp:3033-3095`) — i.e. today's `addLinkedProjection`
  machinery, now attached to the agg node instead of the leaf.
- Program transport is **unchanged**: SCAN_TABREQ section 2 → DBTC →
  JOIN_AGG_SETUP (`prepareAggregation`, `NdbQueryOperation.cpp:3906-4260`),
  states created per node as today. (Long-term the program could move into
  the node parameters — `PI_ATTR_AGGREGATE = 0x20` is defined but never
  emitted, `QueryTree.hpp:210` — but that is not needed and not proposed
  here.)
- Version gating: exactly the `ndbd_spj_multifrag_scan` pattern
  (`NdbQueryBuilder.cpp:3550`) — emit `QN_AGGREGATE` only when
  `ndbd_support_agg_tree_node()` holds; an old ndbd's `getOpInfo` default
  arm rejects unknown op types at build anyway.

### 3.2 DBSPJ (the new op type)

`g_AggregateOpInfo` implementing `m_build`, `m_parent_row`, `m_countSignal`,
`m_cleanup`, `m_checkNode`, `m_dumpNode` (the `g_CteLookupOpInfo` subset)
plus dedicated `execJOIN_AGG_FEED_CONF/REF` handlers:

- `agg_build`: `createNode`, table ids = 0 (the `m_primaryTableId == 0`
  "no real table" convention CTE nodes use, `DbspjMain.cpp:6095-6097`),
  common `parseDA` for the attr patterns.
- `agg_parent_row`: expand patterns from the parent row / buffered
  ancestors, pack the feed buffer (typed entries — `CteLinkedAttr`
  encoding as in `buildCteLinkedBuffer`), resolve the target from
  `m_aggStateKeys` (+ owner instance — main-entry triples as in the LOCAL
  plan §5), send `JOIN_AGG_FEED_REQ`, bump outstanding.
- CONF/REF: decrement, complete node at zero; REF aborts the request.
- Classification touch points a non-table node must satisfy (all
  enumerated, all small): membership in the `isLookup()` OpInfo comparison
  list (`Dbspj.hpp:1032-1038`), skip in table-error prepare
  (`m_tableOrIndexId == 0` skip exists, `DbspjMain.cpp:3360-3370`),
  `validateAggregateFlags` reworked — the agg node *is* the leaf now, so
  the "every `T_AGGREGATE_LEAF` must be a leaf" check
  (`DbspjMain.cpp:2404-2437`) and the node-count balance (`:2424-2428`)
  update rather than grow.

### 3.3 DBLQH (receive side)

`execJOIN_AGG_FEED_REQ`: look up the state by key, run the interpreter in
buffer-only mode — the `cteLookupAggFeed` shape (`DblqhMain.cpp:19556`)
with leaf-column loads resolving from the shipped buffer. **Design choice
(recommended: kernel-side resolution):** feed-buffer entries carry real
AttributeHeaders, and `kOpLoadCol` by attr-id resolves against the buffer
(the `loadColumnTypedFromBuf` machinery from Step 4b) — so aggregation
programs stay byte-identical between the in-place and TreeNode paths, and
no program builder (NdbAggregator users, RonSQL, MySQL handler) changes.
The alternative — rewriting builders to emit `LoadLinkedColumn` for
ex-leaf columns — touches every program producer and is rejected.

### 3.4 What is NOT touched

- **The reading-side hot paths.** No new branches in `execLQHKEYREQ` /
  SCAN_FRAGREQ variableData parsing, no `handleJoinAggRow` changes, no
  per-LDM read-set materialization. The reading node serves a completely
  ordinary internal-node read. (Contrast: the LOCAL plan's Phase 3 adds a
  remote-feed branch inside DBLQH/DBTUP row processing.)
- DBTC beyond the LOCAL plan's placement phases: no feed counting, no
  `expectedOpCount` revival, no new CONF fields — the barrier is
  structural in SPJ.
- SETUP/COMPLETE/RELEASE orchestration, the state/interpreter machinery,
  result emission, API-side merge.

## 4. Special cases the TreeNode model simplifies

- **Outer-join NULL rows**: today DBSPJ injects them via a separate
  `JOIN_AGG_NULL_ROW_REQ` (`DbspjMain.cpp:10141-10167`) with its own
  bitmask bookkeeping (`m_agg_match_bitmask`, `T_NULL_ROW_DEFERRED_RESTART`
  deferred restarts, `Dbspj.hpp:1225-1232, 1363-1364`). With an agg node, a
  NULL-extended row is just another `m_parent_row` invocation with
  null-marked columns → one uniform feed path. (Initial implementation may
  keep NULL_ROW_REQ and simplify later.)
- **CTE indirect feed** (`T_CTE_INDIRECT_FEED`, `Dbspj.hpp:1241-1251`): the
  "CTE_LOOKUP results feed an enclosing aggregator" special case dissolves
  — the CTE_LOOKUP op's results flow to SPJ and then into the agg node
  like any parent rows.
- **Multi-leaf (star schema)**: one agg feed node per aggregate leaf, each
  the child of its ex-leaf table, all sharing the state with their
  `m_agg_leaf_index` — the single-parent tree constraint
  (`appendParentList` emits exactly one parent) is respected, and the
  common-parent validation (`DbspjMain.cpp:2445-2469`) carries over.
- **CTE bodies**: the source-CTE aggregator gets its own agg node at the
  bottom of the `QN_CTE_SUBTREE` (the container's `numNodes` back-patching
  precedent, `NdbQueryBuilder.cpp:1279-1285`).

## 5. Honest cost: the row transfer

The structural price is that last-table rows now flow reading-LQH → SPJ
(standard internal-node TRANSID_AI) instead of being consumed where read:

- **LOCAL mode: no extra network.** Rows read remotely must cross the
  network once in any design; here that leg *is* the standard TRANSID_AI to
  the (TC-node-pinned) SPJ, and the feed to the state is node-local. Versus
  the LOCAL plan's direct reading-LQH → owner-LQH feed: same network legs,
  plus one local hop and SPJ per-row CPU — in exchange for zero bespoke
  transport, zero barrier protocol, and an untouched read path.
- **Distributed mode: a real regression for high-cardinality feeds.** Today
  a scattered leaf row aggregates in place at zero cost; through the tree
  it pays a network transfer to the SPJ node whenever reading node ≠ SPJ
  node. This is why adoption should start LOCAL-only (§6) — distributed
  placement keeps the in-place path unless/until hash-routed feeds (which
  buy kernel-complete GROUP BY for that price) justify migrating it.
- SPJ also pays batch buffering for the ex-leaf's parent rows where
  children need them (`T_BUFFER_*` rules, `DbspjMain.cpp:2660-2694`,
  `:3118-3213`) — for the agg node streaming per-row with no random-access
  requirement, no buffering of the last table's rows should be needed
  (rows stream through `startNextNodes` → `m_parent_row`,
  `DbspjMain.cpp:5958-6014`).
- Batch flow control: agg-feed rows must not count into `Request::m_rows`
  (API row accounting) — same invariant the CTE feed already maintains
  (`DbspjMain.cpp:6916-6919`); the `handleJoinAggNextBatch` self-continue
  (`:3712-3718`) is unaffected.

## 6. Comparison and recommended adoption

| Dimension | LOCAL plan Phase 3 (feed words) | Aggregation TreeNode |
|---|---|---|
| Program-input declaration | new per-leaf read-set serialization + reading-node materialization | standard `NI_LINKED_ATTR` projection + param patterns (existing parseDA/appendFromParent) |
| Feed/COMPLETE barrier | new counting protocol (CONF fields + `expectedOpCount`) or fragile transporter ordering | structural: `m_outstanding` gating, cte_lookup idiom — nothing new |
| New signal | `JOIN_AGG_FEED_REQ` DBLQH→DBLQH cross-node | `JOIN_AGG_FEED_REQ` SPJ→DBLQH (node-local under LOCAL) |
| Reading-side kernel changes | new branch in DBLQH/DBTUP row processing + read-set machinery | none |
| DBTC changes | feed-count accumulation + COMPLETE payload | none beyond shared placement phases |
| DBSPJ changes | owner-ref words on leaf requests | new op type (~500-700 lines, < cte_lookup) |
| API changes | read-set emission | node synthesis + serialization (~250-350 lines) |
| Per-row cost (LOCAL) | 1 network leg (direct) | 1 network leg (TRANSID_AI) + 1 local hop + SPJ CPU |
| Distributed mode | untouched | untouched under LOCAL-only adoption; full migration costs row transfer |
| Architecture | ad-hoc extension of the leaf-tagging model | aggregation as a first-class query-tree operator |
| Future paths | — | hash-routed feeds → kernel-complete GROUP BY (no redistribute, no API merge, distributed top-N groundwork) |

**Recommendation:** adopt the TreeNode as the LOCAL feed mechanism —
i.e. replace `local_execution_mode_plan.md` Phase 3 with the phases below,
keeping Phases 0-2 (syntax, flag transport, placement, local signals) as
committed. Under LOCAL the TreeNode's only real cost (row transfer to SPJ)
is a leg the row had to travel anyway, and it eliminates the three weakest
parts of the committed Phase 3: the read-set mechanism, the barrier
protocol, and the reading-side hot-path branch. Distributed queries keep
the proven in-place path; the same node type later enables per-node or
hash-routed feeds without new transport if that trade is ever wanted.

The API can emit `QN_AGGREGATE` only for LOCAL queries initially (both
serializations coexist behind the query-level flag), so the blast radius
stays confined to the new mode.

## 7. Phasing (replaces LOCAL plan Phase 3 if chosen)

- **Phase 3a — API**: `ndbd_support_agg_tree_node` gate; synthesize + 
  serialize `QN_AGGREGATE` (+ parameters) for LOCAL aggregation queries;
  agg node carries the linked-input patterns; ex-leaf op loses
  `NI_AGGREGATE_LEAF` and gains the column projection. Main aggKeys
  entries become `[nodeId, key, ownerInstance]` triples (shared need with
  the committed plan).
- **Phase 3b — DBSPJ**: `g_AggregateOpInfo` (build / parent_row / feed
  send / CONF / REF / countSignal / cleanup); `validateAggregateFlags`
  update; classification touch points; routing from `m_aggStateKeys`.
- **Phase 3c — DBLQH**: `JOIN_AGG_FEED_REQ/CONF/REF` in `JoinAgg.hpp`;
  `execJOIN_AGG_FEED_REQ` feeding buffer-only `processRecWithLinkedAttrs`
  with attr-id loads resolved from the feed buffer; eviction retry.
- **Phase 3d — tests**: block tests on ≥2 node groups (remote reads feeding
  the TC-node state through the tree); flip the ng2r2/ng2r3/ng4r2 MTR
  suites from the interim 1271 error to result-compare; ERROR_INSERT
  delaying a feed CONF to prove the structural barrier.

## 8. Open questions

1. **Feed granularity**: one FEED_REQ per row (cte_lookup style) vs
   batching multiple rows per signal per target (better for scans; the
   send is already section-based, so a multi-row section is a natural
   extension). Recommend per-row first, batch as measured optimization.
2. **kOpLoadCol resolution**: confirm the attr-id-keyed buffer lookup
   covers all type paths (wide types, strings, DECIMAL) that
   `loadColumnTypedFromBuf` handles today.
3. **Eviction results** (`AGG_EVICT` mid-scan rows to the API,
   `DbspjMain.cpp:9101-9110`, `:13195-13199`): under LOCAL the state's
   eviction path is unchanged (state-side), but the readLen-based
   accounting on lookup CONFs disappears with leaf-tagging — verify the
   eviction row accounting has an equivalent on the feed CONF.
4. **Op-type value**: take 0x9 (0x5 is a historical gap — confirm it is
   not reserved before reusing it).
5. **Full migration criteria**: if hash-routed feeds are ever pursued
   (kernel-complete GROUP BY), distributed mode migrates to the tree and
   the leaf-tagging path retires — what row-rate threshold makes the
   row-transfer trade acceptable there?

## 9. Estimated impact

| Piece | Size |
|---|---|
| API synthesis + serialization | ~250-350 lines |
| DBSPJ op type | ~500-700 lines |
| DBLQH feed handler | ~150-250 lines (mostly cteLookupAggFeed-shaped) |
| Validation/classification updates | ~100 lines |
| Removed vs committed Phase 3 | read-set machinery, barrier protocol, DBTC counting, reading-side branch (~300-400 lines not written) |

Net: comparable total size to the committed Phase 3, but concentrated in
one new, well-precedented DBSPJ op type instead of spread across the
DBLQH/DBTUP read path, DBTC, and a new counting protocol — and it leaves a
first-class architectural seam (per-node / single / hash-routed feeds) that
the feed-words approach does not.
