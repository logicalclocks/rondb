# RONDB-1120: overlapping JOIN_AGG_SETUP with query execution

**Status: P0 + P1 + P2a IMPLEMENTED (September 2026, pending user
build + block suites + full ronsql regression — zero behavior change
expected while the gate holds); P2b/P2c + P3 planned.  Idea: send JOIN_AGG_SETUP_REQ to the nodes and start query
execution immediately, letting LQHKEYREQ / SCAN_FRAGREQ (and the CTE
probe/scan signals) find the JoinAggregationState by identity instead
of by the pool keys returned in SETUP_CONF.**

P2 decomposition (found while scoping): un-gating cannot ship alone —
the aggKeys section is built pre-CONF under the new flow, so DBTC has
no keys to put in it, meaning the FEED signals must survive
key-less (identity-authoritative) BEFORE the gate can move.  P2 is
therefore split: **P2a (shipped)** = the complete parking machinery +
identity-authoritative feed handling + H3 stale drops, all correct
and dormant while the gate holds; **P2b** = key/owner transport for
DBSPJ's post-READY needs (CTE probe keys / owners riding the per-CTE
READY broadcast + CTE_START_MAIN_REQ, dual with the section keys for
verification); **P2c** = flip the gate + the H2 COMPLETE-boundary
straggler wait + ERROR_INSERTs + benchmarks.

P2a outcome notes:
- **Park machinery (plan 2.2)**: `JoinAggParkRec` pool (64, shared
  free mutex) saving the ORIGINAL signal — words, length, header
  sender, detached section IVals in each signal's own section order
  (LQHKEYREQ Key0/Attr1; SCAN_FRAGREQ Attr0/Key1).
  `joinAggIdentityResolveOrPark` does the mutex-held three-way
  (RESOLVED on the SETUP-won race / PARKED / PARKED_NEW) with the
  speculative-entry non-nesting pattern; `joinAggIdentitySweep`
  detaches + removes a still-unfilled placeholder.
- **DBLQH**: shared `parkJoinAggConsumer` pulls the section IVals off
  the op record (restoring them on RESOLVED/FAILED) and schedules the
  placeholder's 10 ms sweeper (ZCONTINUE_JOIN_AGG_PARK_SWEEP).  The
  LQHKEYREQ arm unwinds via the record half of earlyKeyReqAbort (no
  REF); initScanrec returns the internal ZJOIN_AGG_PARKED sentinel
  (1279, never on the wire) and execSCAN_FRAGREQ's parked arm parks +
  runs the error_handler2 unwind with the REF suppressed (ja_parked).
  The scan's RESOLVED race (initScanrec aborted midway) re-executes
  the saved request via the flush path instead of continuing inline.
  Re-execution (ZCONTINUE_JOIN_AGG_FLUSH_PARKED) rebuilds the signal
  INCLUDING the original header sender — SCAN_FRAGCONF targets the
  header sender, so a proxy-sent re-dispatch would corrupt the scan
  protocol; the proxy therefore only sends a flush tick to the
  parking LDM, which reconstructs and invokes its own handler
  directly (DBTC legacy-translator precedent).  The sweeper REFs
  parked requests with ZJOIN_AGG_STATE_NOT_FOUND (LQHKEYREF /
  send_scan_fragref to the saved sender) and releases parked
  sections.
- **Identity-authoritative feed handling**: both consumer arms now
  branch on wireKey == RNIL — dual addressing (P1 cross-check) when
  the key is present, identity-only with park-on-miss when absent.
  Nothing emits key-less feed signals until P2c, so the path is
  dormant; an ERROR_INSERT delaying SETUP processing will force it in
  P2c's tests.
- **H3 (DBTC)**: execJOIN_AGG_SETUP_CONF/REF drop stale replies
  (getValidPtr + state/JoinAgg checks + silent drop) instead of
  ndbrequiring WAIT_JOIN_AGG_SETUP — CONFs may arrive with the scan
  RUNNING or gone once P2c lands.

P1 outcome notes (consumers resolve by identity, wire keys stay
authoritative): DBTC prepends [QUERY_TAG_MARKER 0xCCEE0001, scanptr.i]
to the aggKeys section; DBSPJ parses it into
Request::m_joinAggQueryTag and, when the tag fits 16 bits (RNIL /
oversized -> identity silently disabled, keys still work), sets the
new JoinAggIdentityFlag (bit 30 in ScanFragReq::requestInfo and
LqhKeyReq's scanInfo word) and appends ONE identity word after the
aggStateKey variableData word in BOTH feed signals.  Maintainer
review tightened this from two words to one: the transid is already
in both signals, and queryTag(16 — the TC scan pool is config-capped
far below 64k) + cteId(7, 0x7F = main) + leafIdx(8) pack into
`JoinAggregationState::packIdentWord` with a spare bit.  25-word
budget: LQHKEYREQ from DBSPJ = 11 fixed + AppAddr(2) + CorrFactor(2)
+ key(1) + ident(1) = 17; SCAN_FRAGREQ = 12 fixed + corr(2) + key(1)
+ ident(1) + rangeCount(1) = 17 — and P3 removes the key word, so the
end state has NO net signal growth.  CTE-feed arms use the raw base
key = leaf 0; the SCAN_FRAGREQ outer-join rangeCount word moved
behind the identity word, emission now flag-driven not
agg_extra-magic.  DBLQH's two attach points (execLQHKEYREQ
variableData walk, initScanrec) call the shared Dblqh member
`jaiResolveConsumerKey` (unpacks the identity word): identity lookup
-> encode(baseKey, leafIdx) -> cross-check against the wire key; any miss/mismatch logs loudly +
ndbasserts + falls back to the wire key, so a hash bug cannot affect
results while the machinery is validated by every JoinAgg query.
Direct-DBLQH block tests / benches (testJoinAgg, benchJoinAgg,
bench_q12_tpch, testCaseAgg, ...) DON'T set the flag and stay on the
single-word pool-key form — they gate on their own SETUP_CONF
explicitly and never need identity addressing; the DBTC-orchestrated
path (testJoinAggSpj, all MTR / RonSQL traffic) exercises the full
identity chain.  CTE_LOOKUP / CTE_SCAN / COMPLETE / RELEASE stay
pool-key addressed per 2.4 (post-CONF signals).

P0 outcome notes: the identity needed NO new wire fields —
JoinAggSetupReq already carries transid[2], senderData (the TC scan
record index = the queryTag) and cteIndex, and JoinAggregationState
already stores all three (m_transid / m_senderData / m_cte_index), so
removal is reverse-lookup from the state.  New
`SimulatedBlock::{init,insert,lookup,remove}JoinAggIdentity*` statics
beside the state pool (SimulatedBlock.cpp): 32 partitions x 1024
buckets selected from DISJOINT bit ranges of one shared hash of the
full identity (low 5 bits partition, next 10 bucket — maintainer
direction; the transid-only partition choice was dropped with it), one
NdbMutex per partition (plan 2.2), one shared free list under its own
mutex.  All 16384 entries (~500 kB per node, maintainer sizing) are
allocated up front at init — the array never moves, so
partition-mutex readers never interact with the allocator; exhaustion
(more live identities than entries) is a proper setup failure.  Entry
struct carries m_waiterHead reserved for the P2 waiter queues.  DblqhProxy: init at READ_CONFIG beside
initJoinAggStatePool; insert placed after FULL state construction and
immediately before the CONF (the failure arms never register — and
under P2 this ordering plus the partition mutex is the publication
edge); remove at RELEASE processing time (back-to-back re-registration
under link FIFO) with a VM_TRACE cross-check
(lookup == key || RNIL — duplicate node-fail RELEASEs are legal), plus
a safety-net idempotent removal inside releaseJoinAggState for
bypassing release paths (setup-failure cleanup frees states without a
RELEASE_REQ).  Insert honors the 2.2 model's three cases already: absent -> new
entry; present as a consumer PLACEHOLDER (aggStateKey == RNIL) ->
filled in place with the waiter queue handed back via waitersOut for
re-dispatch (dormant until the P2 consumer side lands); present with a
live key -> DUPLICATE.  The pre-allocated entry is taken outside the
partition mutex (the two mutexes never nest) and rolled back when the
identity is found present.  A failed insert is a failed SETUP —
JOIN_AGG_SETUP_REF (maintainer direction): DUPLICATE ->
DbspjErr::InvalidRequest with a debug ndbassert (a queryTag collision
or missed removal is a bug, and from P1 on a lookup would resolve to
the WRONG live state — the mis-addressing class), NO_MEMORY (chunk cap
/ RG_QUERY_MEMORY exhausted) -> DbspjErr::OutOfQueryMemory like every
other setup resource failure; both via sendJoinAggSetupRef, whose
partial-state cleanup runs the key-qualified safety-net removal so the
other live entry survives the duplicate case.  REFing already in P0
keeps P0 -> P1 failure semantics identical.  Remove is key-qualified:
a stale duplicate release cannot tear down an identity re-registered
by a newer query.

## 1. What is serialized today (verified in code)

The setup round sits fully on the critical path of every JoinAgg
query:

```
SCAN_TABREQ → DIH resolution (local EXECUTE_DIRECT, fast)
  → sendDihGetNodesLab tail (DbtcMain.cpp:18259):
      scanState = WAIT_JOIN_AGG_SETUP
      sendJoinAggSetupReqs          ← fan-out, (1 + numCtes) reqs/node
  → ... one full round-trip to the SLOWEST node ...
  → execJOIN_AGG_SETUP_CONF (all nodes confirmed)
      → build aggKeys section ([nodeId, aggKey] pairs + CTE_KEYS block
        with per-CTE per-node key/owner triples, DbtcMain.cpp:31264+)
      → sendFragScansLab            ← first SCAN_FRAGREQ leaves HERE
```

Three load-bearing facts:

- **The fan-out is DIH-independent.**  `sendJoinAggSetupReqs`
  (DbtcMain.cpp:30904) loops `nodeId = 1..MAX, connected && DB` — it
  targets every DB node unconditionally and consumes nothing from the
  fragment locations.  Nothing data-dependent forces it after DIH.
- **What SETUP_CONF actually delivers** is only addressing:
  `aggStateKey` (a DblqhProxy pool index) and `ownerInstance`
  (derived on the node as `(key % ndbMtLqhWorkers) + 1`,
  DblqhProxy.cpp:2473).  Everything else the nodes need (programs,
  receiver ids, column metadata, strategy flags) travels IN the
  SETUP_REQ itself.
- **Key resolution is not per-row.**  DBLQH caches the key on the
  operation record / ScanRecord (`m_join_agg_state_key`,
  DblqhMain.cpp:9722/:16731); `getJoinAggState(key)` is a pool-index
  deref performed once per op / scan attach / probe, then
  `handleJoinAggRow` uses the cached pointer.  So replacing the key
  with a hash lookup costs one hash probe per op attach, not per row.

The proxy-side SETUP work (state seize from the TransientPool, program
buffer copy, receiver-id/meta copies, field init) is small; the
dominant serialized cost is the exchange itself.  On this development
laptop one such exchange measured ~90 µs (the JOIN_AGG_RELEASE
analysis); on a real network it is one LAN RTT to the slowest node
plus proxy processing.

## 2. Proposed design

### 2.1 Identity tuple — (cteId, transId) is NOT quite unique enough

The transaction id is unique per transaction, but the NDB API permits
several concurrent (or back-to-back) scans in one transaction; two
JoinAgg queries in the same transaction would collide on
(transId, cteId = MAIN).  RonSQL never does this today, but the kernel
contract must not depend on that.  Fix is one extra word:

```
identity = (transId1, transId2, queryTag, cteId)
  queryTag = DBTC-generated per-query discriminator
             (e.g. (tcInstance << 24) | seq, or simply the TC
              scanPtr.i qualified by refToNode+instance of senderRef)
  cteId    = RNIL/sentinel for the main aggregation, else 0..63
```

The transId stays in the tuple as the *validation* half — a stale or
malicious lookup with the wrong transId misses cleanly.  (This is an
addressing-integrity IMPROVEMENT over today's raw pool indices: the
D22/D25 bug class was mis-addressed pool keys silently hitting the
wrong live state; a hash identity is self-validating.)

### 2.2 Concurrency model: partitioned waiter-queue hash (maintainer design)

A set of hash tables, each protected by its own mutex; the partition
is chosen from TRANSID (spreads contention across the LDM threads).
Entries map identity = (TRANSID, QUERY_TAG, CTEID) to a reference to
the JoinAggregationState created by JOIN_AGG_SETUP_REQ.

**Consumer arrival (LQHKEYREQ / SCAN_FRAGREQ on an LDM thread):**
take the partition mutex, look up the identity:

- **Found with a valid reference** — use it, release the mutex; cache
  the resolved key/pointer exactly where `m_join_agg_state_key` is
  cached today (one lookup per attach, nothing per row).
- **Not found** — insert a PLACEHOLDER entry: JoinAgg reference =
  RNIL plus a queue of waiting requests; enqueue this request
  (parked with its signal data + sections) and schedule the entry's
  10 ms failure sweeper (see below).  One sweeper per placeholder,
  scheduled by the inserter.
- **Found with RNIL reference** — a placeholder already exists;
  append this request to its wait queue.

**SETUP arrival (proxy thread, `execJOIN_AGG_SETUP_REQ`):** construct
the state fully, then under the partition mutex either insert the
filled entry or fill an existing placeholder and DRAIN its wait
queue — the parked signals are re-dispatched to their original
LDM/SPJ instances (sendSignal with the saved data + sections), whose
re-execution now finds the filled entry.  Waiters therefore wake
exactly when setup lands, with no polling; the mutex hand-off is
also the happens-before edge that publishes the constructed state to
the consumer threads (see 2.5 H1).

**RELEASE (`execJOIN_AGG_RELEASE_REQ` processing):** remove the entry
from the hash — at RELEASE processing time, NOT at the end of the
CONTINUEB-sliced teardown, so a back-to-back query on the same
transaction can re-insert immediately (DBTC→node link FIFO guarantees
RELEASE(q1) precedes SETUP(q2) at the proxy).

**The failure case (SETUP_REF):** when setup fails, DBTC starts the
abort — but the abort waits for LQHKEYCONF/REF and
SCAN_FRAGCONF/REF, and a request that arrived AFTER the REF was sent
would sit parked on a placeholder forever, deadlocking the abort.
The placeholder's 10 ms CONTINUEB sweeper closes this: when it fires,
if the entry is gone from the hash — nothing to do; if it is still
there with an RNIL reference — abort every queued request (send
LQHKEYREF / SCAN_FRAGREF so DBTC's abort completes) and remove the
placeholder.  (Implementation detail: the CONTINUEB fires on the
inserting LDM's thread while queued requests may target OTHER
instances — either send the REFs on their behalf, which requesters
must accept since they correlate by senderData, or re-dispatch the
parked signals with an abort marker so each original instance
responds itself.)  A later-arriving request simply inserts a fresh
placeholder with its own sweeper.  The sweeper is pure failure-path:
on the happy path SETUP normally wins outright (no placeholder ever
exists), and when execution wins by a hair the waiters park for
microseconds until the proxy fills the entry.

Two compatible refinements, both optional: on SETUP_REF the proxy can
drain an already-existing placeholder's queue with the error
immediately (fast-fail instead of up-to-10 ms), and DBTC's abort
already fire-and-forget-RELEASEs every node it SETUP'd (m_aggNodes is
set at send time), which removes filled entries; the sweeper's
remove-on-abort covers the placeholder case.

Lazy state creation by the first consumer is NOT an alternative: the
aggregation program and metadata travel only in SETUP_REQ.

### 2.3 Why the race window is small (but must be handled)

DBTC sends SETUP_REQ (to each node's proxy) and SCAN_FRAGREQs (to
DBSPJ instances) back-to-back.  Per-link FIFO delivers SETUP first to
each node, but the proxy thread and the SPJ/LDM threads execute
concurrently — ordering after delivery is not guaranteed.  In
practice the first row reaches `handleJoinAggRow` several hops after
SETUP's single hop (SPJ parse → SCAN_FRAGREQ to LDMs → first row), so
placeholders are rare; an ERROR_INSERT delaying SETUP processing must
force the waiter-queue path in tests.

Cross-node consumers (CTE_LOOKUP forwarding, REDISTRIBUTE, FINAL_REP)
are all preceded by local feeds and the COMPLETE phase, so their
states provably exist; they keep wire-learned addressing (see 2.4/2.7).

### 2.4 Owner instances: no derivation needed (heterogeneous-LDM safe)

Clusters legally run DIFFERENT LDM counts per node, and today's owner
machinery already handles that with zero cross-node agreement: each
node computes its own owner locally (`(poolKey % its own
ndbMtLqhWorkers) + 1`, DblqhProxy.cpp:2473) and the VALUE rides the
wire (SETUP_CONF -> aggKeys triples -> m_cte_remote_ownerInstances).

Two correct ways to keep that under the new scheme:

- **(preferred) Keep wire-learned owners — derivation is simply not
  needed.**  The proposal stops GATING on SETUP_CONFs, not sending
  them.  Every owner-addressed signal (JOIN_AGG_COMPLETE,
  REDISTRIBUTE / FINAL_REP, CTE_LOOKUP owner routing) fires only
  after a scan/materialization stage completes — by which time the
  CONFs have provably returned and populated the owner maps exactly
  as today.  The only signals in the un-gated window are the
  feed-path ones (SCAN_FRAGREQ / LQHKEYREQ to fragment LDMs and the
  node-local handleJoinAggRow attach), which use no owner addressing
  at all — they need only the LOCAL state, i.e. the identity-hash
  lookup.  Consequence: DBSPJ's key/owner maps survive unchanged;
  only their arrival time moves (e.g. CTE probe keys can ride the
  per-CTE READY broadcast / CTE_START_MAIN_REQ instead of the initial
  SCAN_FRAGREQ section), which shrinks the change inventory in 2.7.
- **(fallback, if some future signal needs a pre-CONF owner)**
  Per-node derivation is still legal on heterogeneous clusters:
  every node already knows every other node's LDM count
  (`getNodeInfo(nodeId).m_lqh_workers`, distributed via NodeInfo),
  and `1 + ((key - 1) % remote_lqh_workers)` is the house fold NDB
  uses to address remote LDMs (SimulatedBlock::getInstanceNo,
  SimulatedBlock.cpp:333).  So `owner(N) = 1 + (hash(identity) %
  m_lqh_workers(N))` — per-node, sender-computable, and consistent
  with what node N computes for itself.  A node cannot change its
  LDM count without restarting, which kills its states and refreshes
  NodeInfo, so no stale-count window exists.

The original draft's "all nodes must agree on lqhWorkers" was wrong
on both counts: no agreement is needed today (values ride the wire),
and none would be needed under derivation (per-node counts are
cluster-distributed knowledge).

### 2.5 Concurrency with SETUP / SETUP_CONF processing

The park-and-retry race (2.3) is only the visible tip; un-gating makes
execution concurrent with the whole setup machinery, which today
assumes exclusivity (execJOIN_AGG_SETUP_CONF hard-requires
scanState == WAIT_JOIN_AGG_SETUP, DbtcMain.cpp:31145).  Four hazards:

**H1 — the identity hash IS the publication mechanism (memory
model).**  Today an LDM thread can safely read the proxy-constructed
JoinAggregationState because the pool key it uses traveled
DBTC → DBSPJ → LDM through message passing that CHAINS FROM the
CONF — the wire round-trip is the happens-before edge between the
proxy's construction and the LDM's first read.  Un-gating severs that
chain, and the 2.2 model rebuilds it: the state is fully constructed
BEFORE the proxy takes the partition mutex to fill the entry, and
consumers read the reference only under the same mutex — the mutex
hand-off is the publication edge.  (This is why the fill must happen
strictly after construction completes, and why consumers must not
cache a placeholder's RNIL and retry outside the mutex.)

**H2 — the gate does not vanish; it moves to the COMPLETE
boundary.**  SETUP fans to ALL DB nodes, but scan work touches only
fragment nodes — a fragment-less or busy node's CONF can still be in
flight when the scans finish.  COMPLETE_REQ addressing (kept
key-based per 2.4) needs that node's key, so
sendJoinAggCompleteReqs / sendCteCompleteReqsForCte gain a deferral:
if setup CONFs are outstanding (per-CTE per-node for the DAG
scheduler), wait for them before sending that COMPLETE.  Normally
free — CONFs return in microseconds while scans run — but the state
machine must have the wait.  (Alternative: address COMPLETE by
identity too, removing the wait at the cost of re-widening the
identity surface.)

**H3 — stale CONF/REF at DBTC.**  CONFs now arrive with the scan
RUNNING, CLOSING, or GONE — a tiny query can finish and (with the
fire-and-forget release) free its scan record before the slowest
node's SETUP_CONF returns.  The :31145 ndbrequire is replaced by the
Phase L drop discipline: getValidPtr + transid check + silent drop of
stale CONF/REF.

**H4 — SETUP_REF vs parked consumers.**  Handled by the 2.2 model's
placeholder sweeper: every placeholder carries a 10 ms CONTINUEB that
aborts (LQHKEYREF / SCAN_FRAGREF) and removes a still-RNIL entry, so
DBTC's abort — which waits for the CONF/REF of every in-flight
LQHKEYREQ / SCAN_FRAGREQ — can never deadlock on a request that
arrived after the SETUP_REF went out.  Optional fast-fail: the proxy
drains an existing placeholder's queue with the error at REF time.
DBTC's REF-while-running abort rides the existing scanError
machinery, and its fire-and-forget RELEASE fan-out (m_aggNodes set at
send time) removes filled entries on the nodes that DID confirm.

(Per-node partial setup — main state inserted, a CTE state not yet —
needs nothing special: parking is per-identity, one entry per state.)

### 2.6 What SETUP_CONF becomes

Pure liveness/error reporting.  DBTC still counts CONFs/REFs (node
failure and OOM detection) but no longer gates execution on them; a
late SETUP_REF aborts a running query through the existing scanError
machinery (new interleaving to test: abort-with-scans-running is
already a supported path).

### 2.7 Signal / struct inventory (the "how big" core)

| Surface | Change |
|---|---|
| `JoinAggSetupReq` | + queryTag (transid already present) |
| `JoinAggSetupConf` | unchanged — still returns key + owner; DBTC just no longer gates on it (owners provably arrive before any owner-addressed signal fires) |
| SCAN_FRAGREQ aggKeys section | keys + owner triples DROPPED; CTE metadata block (cteId, depMask, flags) kept — buildable at parse time, which is what unlocks immediate sendFragScansLab |
| `ScanFragReq`/`LqhKeyReq` joinAggStateKey word | becomes (cteId, leafIdx) selector; state resolved via signal's transid + queryTag |
| `CteLookupReq` | + transid[2] + queryTag (carries neither today); aggStateKey/joinAggStateKey → identity + leafIdx forms |
| `CteScanReq` | same treatment |
| `JoinAggCompleteReq`/`ReleaseReq` | UNCHANGED — sent post-CONF, keys/owners in hand as today |
| `JoinAggRedistributeReq` / FINAL_REP | UNCHANGED — post-COMPLETE, wire-learned owners |
| DblqhProxy | identity hash table + park/retry protocol + release-time unhash |
| DBSPJ | keeps its key/owner maps; they arrive later (READY broadcast / START_MAIN sections instead of the initial SCAN_FRAGREQ section); pre-CONF feed signals carry identity instead of keys |
| DBTC | send SETUP + proceed straight to sendFragScansLab; CONF counting decoupled from scanState; WAIT_JOIN_AGG_SETUP retired from the happy path |

Block tests: `testJoinAgg` / `testJoinAggSpj` drive SETUP + keys
explicitly at the signal level — they need reworking, not just
re-running.  Plus a new ERROR_INSERT (delay SETUP processing) to force
the park path, and node-failure interleavings (SETUP_REF after rows
flowed).

**Size estimate: comparable to Phase 7 + Phase L combined — roughly a
1,500–2,500 line diff across DBTC / DBSPJ / DBLQH / DblqhProxy /
signal headers, plus significant test surface.  Risk class is the
worst we have (distributed addressing, the D22/D25 family), mitigated
by the self-validating identity.  Multi-week, needs the full ronsql
regression ×5 + block suites + new race tests.**

## 3. Latency assessment

What the change removes from the critical path:

- One fan-out round-trip to the slowest DB node (the SETUP exchange)
  plus DBTC's CONF-side section building.  Measured proxy exchange on
  the bench laptop: ~90 µs; LAN: ~1 RTT (50–200 µs+).
- For CTE queries the SETUP round already covers main + all CTEs in
  one exchange, so the gain is ~one exchange per query regardless of
  CTE count — but it applies to EVERY JoinAgg query, including the
  fs_* point/floor feature-store shapes where total request time is
  ~128 µs.  There, ~90 µs of serialized setup is the single largest
  remaining fixed cost (~40% of the request, next to the ~25 µs
  COUNT(*) shortcut already taken and the release round already made
  fire-and-forget).

What it does NOT remove: the proxy still performs the same setup work
per node — it just overlaps with DBSPJ parse + scan ramp-up on the
same nodes.  If setup processing ever exceeded the scan ramp, the park
path would absorb the difference (bounded, still faster than today's
full gating).

How to measure before/after: the `x-ronsql-phases` header's
send/firstbatch split (SETUP currently sits inside "send" from the
API's viewpoint), the AGGT probe timeline (re-enable DEBUG_AGGT:
SETUP send → CONF-complete → first SCAN_FRAGREQ timestamps), and
`.bench_ronsql fs_point / fs_floor / fs_batch` plus `cte_tpch_q*` on
prod_build.

## 4. Cheaper variants considered (and why they gain less)

1. **Overlap SETUP with the DIH round only** (send SETUP at
   SCAN_TABREQ time, keep gating before SCAN_FRAGREQ): trivially
   small change (the fan-out is DIH-independent), but DIH resolution
   is a local EXECUTE_DIRECT — there is almost nothing to hide the
   RTT behind.  Gains single-digit µs.  Worth doing only as a
   stepping stone.
2. **Piggyback setup on the first per-node signal**: SETUP targets
   the proxy (state shared across LDMs) while execution signals hit
   SPJ/LDM threads — first-consumer-creates has the same race with a
   worse owner (any LDM), and the program would ride every first
   signal.  Strictly worse than park-and-retry.
3. **Persistent per-connection agg sessions** (amortize setup across
   queries): different, bigger idea — overlaps with
   `filter_program_setup_transport_plan.md` and the LOCAL-mode
   backlog; not a substitute since the first query still pays.

## 5. Verdict

Real and measurable win (~one exchange, ~90 µs on the bench box, the
largest remaining serialized fixed cost for point queries), but a
large, high-risk change — the entire JoinAgg addressing model moves
from returned pool keys to a derived identity, touching every signal
that carries an aggStateKey and requiring a new park/retry protocol
plus reworked signal-level block tests.  If pursued, phase it:

- **P0**: queryTag plumbing + the partitioned mutex-protected hash
  (2.2) alongside the existing keys (dual addressing, nothing gated
  differently; entries always filled before any consumer looks) —
  proves the hash, the mutex publication edge (H1), and release
  ordering with zero behavior change.
- **P1**: switch consumers to identity resolution (keys still
  returned/ignored); block tests migrate here.
- **P2**: stop gating on SETUP_CONF (the actual latency win) + the
  2.2 waiter-queue path with its placeholder sweeper + the
  COMPLETE-boundary straggler gate (H2) and stale-CONF drops (H3).
  ERROR_INSERTs: delay SETUP processing (forces the waiter queue),
  delay ONE node's CONF past scan completion (forces H2), REF after
  rows flowed (forces the sweeper abort + abort interleaving), and
  REF racing an already-parked request (fast-fail drain).
- **P3**: move the key/owner map transport to the post-CONF signals
  (READY broadcast / START_MAIN) and retire WAIT_JOIN_AGG_SETUP from
  the happy path.  Owner derivation is NOT part of any phase (2.4).

Benchmarks after P2 decide whether P3's cleanup ships with it.
