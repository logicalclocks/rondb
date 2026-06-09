# Proactive Deadlock Discovery — Design & Implementation Plan (RONDB-1062)

## 1. Problem statement & current behaviour

A deadlock is a cycle in the *wait-for graph*: transaction T1 waits for a lock held by
T2, T2 waits for a lock held by T3, …, Tn waits for a lock held by T1. Today RonDB has
**no wait-for graph and no cycle detection**. Deadlocks are resolved only as a side
effect of the per-transaction timeout:

1. A lock request that conflicts is parked on the row's serial lock queue in DBACC
   (`placeSerialQueue`, `DbaccMain.cpp:3019`). No timer runs in DBACC/DBLQH for the wait;
   DBLQH explicitly comments that "lock timeouts are handled by DBTC"
   (`DblqhMain.cpp:22524`).
2. DBTC sweeps all transactions every ~500ms (`timeOutLoopStartLab`, `DbtcMain.cpp:10876`).
   For each it compares elapsed time against `ctimeOutValue` (from
   `TransactionDeadlockDetectionTimeout`) **plus a random jitter** (`ndb_rand() & mask`,
   `DbtcMain.cpp:10943`) whose explicit purpose (comment `:10860-10874`) is to avoid
   timing out *both* partners of a 2-transaction deadlock.
3. On expiry, `timeOutFoundLab` (`DbtcMain.cpp:11333`) sets
   `returncode = ZTIME_OUT_ERROR (266)` and drives the abort (`abort010Lab` /
   `abortErrorLab`). The code travels to the API via `TcRollbackRep`
   (`DbtcMain.cpp:19815`) → `NdbTransaction::receiveTCROLLBACKREP`
   (`NdbTransaction.cpp:2628`) → ndberror class `TO` → `HA_ERR_LOCK_WAIT_TIMEOUT`
   (TemporaryError, retryable).

**Weaknesses:** latency bounded by the timeout (default ~1200 ms, `ConfigInfo.cpp:1109`);
false positives (slow ≠ deadlocked); the random jitter is a coin-flip on which victim
dies and adds yet more latency.

**Objective:** detect true cycles *as soon as the wait is established* (or shortly after),
pick a single deterministic victim, abort it immediately, and keep the application-visible
error code identical to today (266) so nothing downstream changes.

> Constraint from the requester, preserved throughout: *"the abort code should be possible
> to retain."* See §7.

## 2. Design overview

### 2.1 The wait-for edge

When DBACC must enqueue a lock request to wait, it has, in scope, both endpoints of a
wait-for edge:

- **Waiter** `W` = `operationRecPtr` — `transId1/2`, `userptr` (LQH op idx), `userblockref`.
- **Blocker(s)** = the lock owner `lockOwnerPtr` **and** every other transaction currently
  co-holding the lock in the owner's *parallel queue* (a shared read lock can be held by
  several transactions). Walk `lockOwnerPtr → nextParallelQue` to enumerate all distinct
  foreign transactions (`getNoParallelTransaction`, `DbaccMain.cpp:2359`, already does this).

For each distinct blocking transaction `O` with `O.transid != W.transid` we have a wait-for
edge `W → O`.

### 2.2 Collector selection and victim policy (the core idea)

Rather than every block maintaining a global graph, each edge `W → O` is sent to **one
"collector" transaction**. Selection is by a **deterministic hash of the transaction id**, so
that (a) every node independently agrees on the same choice and (b) the choice is unbiased —
unlike raw-transid ordering, which would systematically victimise the oldest transaction or
transactions from lower-numbered API nodes (transid encodes the API node + a monotonic
sequence — see Component B).

```
key(T)         = hash(T.transId1, T.transId2)       // pure function, identical on every node
                                                     // tie-break on equal hash: raw (transId2, transId1)
collector(W,O) = the endpoint with the smaller key
victim         = the endpoint with the smaller key  // == the collector, therefore always LOCAL
```

Using the *same* min-key rule for both routing and victim selection is deliberate: it makes
the collector and the victim the **same transaction**, which is local to the collector's TC,
so a detected 2-cycle is aborted with a purely local action — no remote-abort signal needed
(that is a Phase 5 concern). `hash` can be any fast deterministic integer mix of the two
transid words (e.g. a multiplicative/xor mix); the only requirements are determinism across
nodes and a total order via the tie-break.

The edge is forwarded to the **TC instance that owns the collector transaction**, which
accumulates every edge for which its transaction is the min-key endpoint. Initially **a single
collector per edge**; "send to both endpoints" is a possible later extension (§6.2).

**Why min-key routing catches all 2-cycles.** In a 2-cycle the two edges are `W → O` and
`O → W`. Both have the same endpoint set `{W, O}`, so both compute `collector = min-key(W, O)`
and land at the *same* TC. That TC holds both edges, sees the cycle `W → O → W`, and aborts the
min-key endpoint (itself). Because every node computes the identical key, both partners
nominate the **same single victim** — replacing the timeout path's random-jitter coin-flip with
deterministic, unbiased victim selection. This is the guaranteed-correct core; longer cycles
are best-effort (§5).

### 2.3 "Immediately or after a short time"

Most lock waits are short and harmless; only waits that *persist* are deadlock candidates.
So sending the edge is **deferred by a short delay** rather than sent on every enqueue:

- On enqueue (`placeSerialQueue`), DBACC schedules a delayed self-signal
  (`sendSignalWithDelay`, `SimulatedBlock.hpp:845`) after a threshold
  `T_detect` (a small fraction of `TransactionDeadlockDetectionTimeout`, e.g. 10–20 ms,
  configurable).
- When the delayed signal fires, DBACC re-validates that the operation is *still waiting*
  in the serial queue (re-resolve the transient `Operationrec` via `getValidPtr`; ops may
  have been granted/aborted meanwhile). If still waiting, it computes the wait-for edge(s)
  and forwards them to the collector TC(s).
- Phase 1 may send **immediately** for simplicity; the delay is a tuning optimisation
  (§9, Phase 4).

This bounds the cost: transient waits never generate signals; only genuinely stuck waits do,
and they are detected an order of magnitude faster than the full timeout.

### 2.4 End-to-end flow

```
DBACC (LDM thread owning the fragment)
  placeSerialQueue(W behind O)         ── enqueue, start lock-stats wait timer
    │  (immediately, or after T_detect via a delayed self-signal)
    │  for each foreign blocker O in owner's parallel queue:
    │     edge = { W.transid, W.tc(ref,oprec),  O.transid, O.tc(ref,oprec) }
    │     dst  = TC of collector(W,O)      // smaller key = hash(transid)
    └──► sendSignal(dst, GSN_DBACC_WAITFOR_REP, edge, JBB)

DBTC (collector's TC instance)
  execDBACC_WAITFOR_REP(edge):
    resolve collector ApiConnectRecord (local) by tc oprec/transid; verify transid
    append edge to ApiConnectRecord.waitForEdges          // per-transaction edge set
    run cycle detection over the accumulated edges
    if cycle found:
        victim = the min-key endpoint within the cycle (== the collector itself, local)
        set victim.returncode = ZTIME_OUT_ERROR (266)     // retained code
        abort victim  (abortErrorLab / abort010Lab)
```

## 3. Component breakdown

The design decomposes into four parts (matching the research fan-out). Each section lists
the responsibilities, the code anchors, and the new/changed pieces.

### Component A — DBACC: wait-for edge capture & routing

**Where.** The serial-queue placement functions, all under the ACC fragment mutex (so the
lock state is consistent while we read it; do *no* heavy work here):
- `placeWriteInLockQueue` `serial:` label — `DbaccMain.cpp:2850`
- `placeReadInLockQueue` `serial:` label — `DbaccMain.cpp:2966`
- `placeSerialQueue(lockOwner, op)` — `DbaccMain.cpp:3019` (single choke point; both callers
  reach it, so instrumenting here covers reads, writes, and — via `checkNextBucketLab` — scans).

**What DBACC must do:**
1. Skip self-edges: never emit when `operationRecPtr.p->is_same_trans(blocker)` (re-entrant
   lock / lock upgrade never deadlocks against itself). The `serial:` labels are only reached
   for foreign-transaction conflicts, but assert this.
2. Enumerate distinct foreign blockers by walking the owner's parallel queue
   (`nextParallelQue`), de-duplicating by transid.
3. For waiter and each blocker, obtain TC identity:
   `m_ldm_instance_used->c_lqh->get_tc_ref(op.userptr, tcOprec, tcBlockref)`
   (`Dblqh.hpp:6276`; existing call site `DbaccMain.cpp:1553`). **Query threads:** must use
   `m_ldm_instance_used->c_lqh`, never a bare `c_lqh`.
4. Compute `collector = min-key endpoint` (min `hash(transid)`, tie-broken by raw transid),
   build the edge signal, and either send now (Phase 1) or schedule the delayed self-signal
   (§2.3, Phase 4).
5. On the delayed path: re-resolve the waiter `Operationrec` (`getValidPtr`) and confirm it
   is still `OP_STATE_WAITING` in the serial queue before sending.

**Identity fields available** (`Dbacc.hpp:777-783`): `transId1`, `transId2`, `userptr`,
`userblockref`. **Gotchas:** `Operationrec` is a `TransientPool` record — capture transid +
userptr *by value*, never cache raw pointers across signals. The owner's LQH op lives in the
same LDM instance (same fragment), so `get_tc_ref` against the local LQH works, but the
resulting `tcBlockref` may point at a *different TC instance/node* (that is exactly why we
route by `tcBlockref`, not by transid — the transid encodes the *API* node, not the TC; see
Component B note).

**New state in DBACC:** for the delayed-send variant, a small marker on `Operationrec`
(e.g. one bit in `m_op_bits`: "wait-for report scheduled") to avoid scheduling twice and to
let `startNext` (the wakeup path, `DbaccMain.cpp:1794`) cancel/ignore a pending report when
the lock is granted. If we send immediately (Phase 1), no new state is needed.

### Component B — The new signal(s) DBACC → DBTC

**Routing fact (critical):** the transaction id encodes the **API node** in `transId2`
bits, *not* the TC node/instance (`Ndbif.cpp:225`, `NdbTransaction.cpp:1535`). You **cannot**
derive a TC block reference from a transid. You must carry the TC `BlockReference` explicitly
(obtained via `get_tc_ref`). DBTC verifies the transid against its resolved
`ApiConnectRecord` to guard against stale `tcOprec` reuse (same pattern as `execLQHKEYCONF`,
`DbtcMain.cpp:6585`).

**New signal `DBACC_WAITFOR_REP` (DBACC → DBTC, fire-and-forget `_REP`).** Suggested payload
(all `Uint32`, well under the 25-word limit):

| Word | Field | Source |
|------|-------|--------|
| 0 | `senderRef` (DBACC ref) | `reference()` |
| 1 | `collectorTcOprec` | collector's TC op index (so TC finds the ApiConnectRecord cheaply) |
| 2 | `waiterTransId1` | `W.transId1` |
| 3 | `waiterTransId2` | `W.transId2` |
| 4 | `waiterTcRef` | waiter's `tcBlockref` |
| 5 | `waiterTcOprec` | waiter's `tcOprec` |
| 6 | `ownerTransId1` | `O.transId1` |
| 7 | `ownerTransId2` | `O.transId2` |
| 8 | `ownerTcRef` | owner's `tcBlockref` |
| 9 | `ownerTcOprec` | owner's `tcOprec` |
| 10 | `flags` | e.g. bit0 = "you are the waiter endpoint" / "you are the owner endpoint"; bit1 = lock mode |

(Diagnostic extras — table id, fragment, lock mode — are optional and can be added later for
`ndbinfo`/DUMP visibility.)

**Implementation checklist** (verified file set — closest template is `CteScan.hpp`'s
`CtePhaseCompleteRep`, a DBSPJ→DBTC `_REP`):

1. **New header** `storage/ndb/include/kernel/signaldata/DeadlockWaitfor.hpp` — modern
   `struct` style (no `JAM_FILE_ID`, no friend classes), `#include <ndb_types.h>`, fields +
   `static constexpr Uint32 SignalLength`.
2. **GSN** `storage/ndb/include/kernel/GlobalSignalNumbers.h` — add
   `#define GSN_DBACC_WAITFOR_REP <next free>` after the current highest (`GSN_CTE_SCAN_REF`
   = 978 at line 1320) and bump `MAX_GSN` (line 40).
3. **Name table** `storage/ndb/src/common/debugger/signaldata/SignalNames.cpp` — add
   `,{ GSN_DBACC_WAITFOR_REP, "DBACC_WAITFOR_REP" }` before the closing `};` (line 1091).
4. **DBTC declare** `Dbtc.hpp` — `void execDBACC_WAITFOR_REP(Signal*);` near the exec decls.
5. **DBTC register** `DbtcInit.cpp` — `addRecSignal(GSN_DBACC_WAITFOR_REP,
   &Dbtc::execDBACC_WAITFOR_REP);` in the constructor (`:524-566` block).
6. **DBTC implement** `DbtcMain.cpp` — `#include <signaldata/DeadlockWaitfor.hpp>` (near
   line 79) + the `execDBACC_WAITFOR_REP` handler.
7. **DBACC send** `DbaccMain.cpp` — `#include` the header + build with `getDataPtrSend()` +
   `sendSignal(dstTcRef, GSN_DBACC_WAITFOR_REP, signal, DeadlockWaitforRep::SignalLength, JBB)`
   (or `sendSignalWithDelay` for the deferred variant).
8. **(Optional)** signal printer (`SignalDataPrint.cpp` + a `.cpp` + `CMakeLists.txt`) — skip
   initially; the `CteScan`/`JoinAgg` precedent ships without one.

**Possible second signal (Phase 5, multi-node victim abort):** `DBTC_ABORT_VICTIM_REQ`
(TC → TC) to abort a victim that is *remote* to the collector. Phase 1–3 avoid this by always
choosing a **local** victim (see Component C / §5).

### Component C — DBTC: edge collection, cycle detection, victim selection

**Where to store edges.** On `ApiConnectRecord` (the per-transaction record, keyed by
`transid[2]`, `Dbtc.hpp:1124`) — it is the natural wait-for-graph node and the unit `abort`
acts on. Recommended pattern mirrors the existing `theFiredTriggers` list
(`Dbtc.hpp:1251`):
- A new small record `WaitForEdge { Uint32 ownerTransId1, ownerTransId2, ownerTcRef,
  ownerTcOprec; }` (+ waiter info if we also need it).
- A new `TransientPool<WaitForEdge>` with a new pool index — bump `c_transient_pool_count`
  (currently 16, `Dbtc.hpp:3091`) and register in `c_transient_pools`.
- A `...::Head waitForEdges` member on `ApiConnectRecord`.
- Init the head in the `ApiConnectRecord` constructor (`DbtcMain.cpp:7472`) and clear/release
  it in `initApiConnectRec` (`DbtcMain.cpp:3350`) and on transaction release — so edges never
  leak across transaction reuse.

**Resolving an incoming edge.** `execDBACC_WAITFOR_REP`:
1. `tcConnectRecord.getValidPtr(collectorTcOprec)` → `apiConnect` → `ApiConnectRecord`.
2. Verify `apiConnectptr.p->transid[]` equals the collector endpoint's transid (reject stale).
3. Confirm the transaction is in an **abortable** state — one of the active/prepare states
   `CS_STARTED / CS_RECEIVING / CS_REC_COMMITTING / CS_START_COMMITTING /
   CS_SEND_FIRE_TRIG_REQ / CS_WAIT_FIRE_TRIG_REQ / CS_START_SCAN` (`Dbtc.hpp:263`). If it is
   already committing/completing/aborting, drop the edge (too late to help).
4. Append the edge to `waitForEdges` (dedup by owner transid).

**Cycle detection (Phase 1 — 2-cycles).** The collector is, by construction, one endpoint of
the edge. A 2-cycle is detected when the collector holds **both** `W→O` and `O→W`, i.e. it
has an edge to `X` *and* an edge from `X` (here both are reported as edges incident to the
collector). Concretely: the collector C has received `C→X` (C waits on X) and `X→C` (X waits
on C). Detecting this is a constant-time check over the small edge set each time a new edge
arrives. (See §5 for the precise predicate and longer cycles.)

**Victim selection.** The victim is the **min-key endpoint** (smallest `hash(transid)`), which
by construction is the **collector's own transaction** — *local* to this TC (no remote abort
needed) and provably part of the cycle. Hashing gives unbiased, deterministic selection: every
node agrees on the same victim without the random jitter the timeout path uses. (Smarter,
cost-based victim selection — fewest locks held, least work done — is a Phase 5 refinement that
may require a remote abort signal, Component B.)

**Abort.** Reuse the existing timeout path so behaviour and code are identical to today:
- Set `apiConnectptr.p->returncode = ZTIME_OUT_ERROR (266)` and
  `returnsignal = RS_TCROLLBACKREP`, then call `abort010Lab` — exactly as
  `timeOutFoundLab` does at `DbtcMain.cpp:11362-11364`; **or**
- set `terrorCode = ZTIME_OUT_ERROR` and call `abortErrorLab(signal, apiConnectptr)`
  (`DbtcMain.cpp:10418`), which respects the retain guard (`if (returncode==0)`, `:10438`).

**Which TC owns a transaction / enumerating its operations.** A transaction lives entirely in
one TC instance (the API picks it). Operations are walked via
`LocalTcConnectRecord_fifo tcConList(tcConnectRecord, apiPtr->tcConnect)` (e.g.
`DbtcMain.cpp:10479` in `abort015Lab`). This matters only for Phase 5 smarter victim
selection; Phase 1–3 act on the whole `ApiConnectRecord`.

### Component D — Abort code retention

Two distinct "retain" requirements, both satisfied:

1. **Retain the externally-visible code (266).** A proactively-detected deadlock must look
   identical to today's timeout deadlock so SQL/replication retry logic is unaffected. Set
   `returncode = ZTIME_OUT_ERROR (266)`; it flows verbatim through `TcRollbackRep`
   (`DbtcMain.cpp:19815`) → API (`NdbTransaction.cpp:2631`, no remapping) → ndberror class
   `TO` (`ndberror.cpp:351`) → `HA_ERR_LOCK_WAIT_TIMEOUT` / `TemporaryError`. **Zero changes**
   to the ndberror table or API are required to keep 266. (A *distinct* "proactively detected
   deadlock" code is possible but would need a new `TimeoutExpired`-class entry in
   `ndberror.cpp` to stay retryable — deferred, not needed.)
2. **Retain the first/most-specific code if already aborting.** If a transaction is being
   aborted for another reason when the deadlock edge arrives, do not clobber its code.
   `abortErrorLab`'s `if (returncode == 0)` guard (`DbtcMain.cpp:10438`) already preserves the
   first code; the `execDBACC_WAITFOR_REP` handler also drops the edge for non-abortable /
   already-aborting states (Component C step 3). So the existing code wins.

## 4. Why DBACC → DBTC (not DBACC → DBLQH → DBTC)

DBACC is normally driven by DBLQH (`DBTC → DBLQH → DBACC`), but it can send a signal directly
to any block reference. It already knows how to reach TC: every operation carries the LQH op
index (`userptr`), and `c_lqh->get_tc_ref(userptr, …)` yields the owning TC's `tcBlockref`
(node + instance) and `tcOprec`. This is the same routing LQH uses for `LQHKEYCONF` /
`ABORTED` replies (`DblqhMain.cpp:6739`, `38991`). Sending straight to TC avoids an extra hop
and keeps the lock-manager change localised.

## 5. Algorithmic scope — what is caught, what is not

- **2-cycles: always caught** (smaller-transid routing converges both edges, §2.2).
- **Longer cycles (≥3): best-effort.** With per-endpoint routing a single collector sees only
  edges incident to its transaction, so a 3-cycle `T1→T2→T3→T1` is *not* closed by any one
  collector from incident edges alone. Two extensions improve this (Phase 5+):
  - **Send-to-both (§6.2):** route each edge to *both* endpoints. Increases the chance that
    enough of a cycle co-locates, and is cheap, but still does not guarantee ≥3-cycles.
  - **Path-pushing (Chandy–Misra–Haas / Obermarck style):** when a collector C holds `A→C`
    and also learns `C→D` (C is itself blocked), it forwards the *composed path* `A→…→D` to
    D's collector. A cycle is declared when a path returns to its origin. This catches longer
    cycles at the cost of more signals and careful termination. Designed as an optional later
    phase.
- **The existing timeout stays as a backstop** for everything proactive detection misses, and
  for non-lock waits. Proactive detection just makes the common cases (2-cycles, the
  overwhelming majority in practice) near-instant and deterministic.

**Victim uniqueness.** Because the victim is chosen deterministically (the `min`-key
collector), a 2-cycle aborts exactly one transaction even though both endpoints may detect the
cycle — both compute the same `hash(transid)` values and the same `min`, so both nominate the
same victim. This *removes* the need for the current random-jitter coin-flip in the timeout
path for the cases we catch.

## 6. Design variants / knobs

Guiding principle (per the requester): **simple first, complex later.** The DECIDED Phase-1
choice is marked in each.

### 6.1 Immediate vs delayed reporting — **DECIDED: immediate first**
- **Immediate** (Phase 1, chosen): simplest; one signal per foreign blocker per enqueue.
  Higher signal volume under contention, accepted for now.
- **Delayed `T_detect`** (possible Phase 4 extension): one delayed self-signal per wait; only
  persistent waits report. Lower volume, slightly later detection. `T_detect` configurable
  (default a small fraction of `TransactionDeadlockDetectionTimeout`).

### 6.2 Send to one collector vs both endpoints — **DECIDED: one collector first**
- **One** (min-key endpoint, chosen): minimal traffic, guarantees 2-cycles.
- **Both** (possible later extension): ~2× traffic, improves coverage of complex cycles,
  prerequisite for path-pushing (§5).

### 6.3 Victim selection — **DECIDED: smallest hash(transid)**
- **Min `hash(transid)`** (chosen): deterministic and unbiased; coincides with the collector
  so the abort is local and single. Tie-break by raw transid for a total order.
- **Cost-based** (possible Phase 5 extension): pick fewest-locks / least-work victim; may make
  the victim remote, pulling in a TC→TC abort signal.

### 6.4 Reuse 266 vs new dedicated code — **DECIDED: reuse 266**
- **Reuse 266** (chosen): zero API/table churn, transparent to existing retry logic.
- **New code** (deferred): better observability, but must be `TimeoutExpired`-class to stay
  retryable; touches `ndberror.cpp` and risks confusing existing clients.

## 7. Configuration

Reuse / extend the existing `TransactionDeadlockDetectionTimeout`
(`CFG_DB_TRANSACTION_DEADLOCK_TIMEOUT`, `ConfigInfo.cpp:1109`, read `DbtcMain.cpp:1485`) as the
backstop, and add (Phase 4) a new sub-parameter for the proactive `T_detect` delay (or derive
it as a fixed fraction of the existing timeout to avoid a new config param initially). Keep the
timeout's role unchanged so behaviour degrades gracefully to today's when proactive detection
is disabled or misses.

## 8. Edge cases & gotchas (consolidated from research)

- **Transient records.** `Operationrec` (ACC) and `TcConnectRecord`/`ApiConnectRecord` (TC)
  are `TransientPool` records. Pass transid/oprec *by value* in signals; always re-validate
  with `getValidPtr` + transid comparison on receipt.
- **Query (read-only) threads (Dbqacc).** Locked reads and scans on query threads hit the same
  serial-queue path. Use `m_ldm_instance_used->c_lqh` for `get_tc_ref`. Lock op records are
  owned by the fragment's LDM thread.
- **Shared locks / parallel queue.** Emit one edge per *distinct* foreign transaction in the
  owner's parallel queue, not just the head owner.
- **Serial-queue ordering.** A new waiter also effectively waits behind earlier serial
  waiters, not only the owner. For 2-cycle detection the waiter→owner edge suffices; richer
  cycle detection may also want waiter→(preceding serial op).
- **Dirty reads & `OP_NOWAIT`.** Never wait — no edge (dirty read bypasses the queue,
  `DbaccMain.cpp:2187`; `OP_NOWAIT` returns `ZNOWAIT_ERROR` 635 before enqueue, `:2852`).
- **TTL probe op.** Ignore the throwaway `tmp_op_rec` (fake i-value `0x880721`,
  `DbaccMain.cpp:3435`).
- **Scans.** Scan locks wait via `checkNextBucketLab` → `IsBlocked` (`DbaccMain.cpp:3375`),
  identity in the scan record. Phase 1 targets key-op locks; scans are a follow-on.
- **Cross-instance/node.** Waiter and owner TCs may differ in instance and node. Route by
  `tcBlockref`. The collector's chosen victim is kept local (Phase 1–3) to avoid remote abort.
- **Don't fight the jitter.** Keep the timeout jitter for cases proactive detection doesn't
  cover; for covered 2-cycles the deterministic victim makes the jitter irrelevant.

## 9. Phased implementation plan

**Phase 0 — scaffolding & wiring (no behaviour change).**
- Add the `DBACC_WAITFOR_REP` signal header, GSN, name (Component B steps 1–3).
- Add an empty `execDBACC_WAITFOR_REP` in DBTC that just counts received edges
  (declare/register/implement, Component B steps 4–6).
- Verify it compiles and a hand-sent signal reaches DBTC (DUMP/ERROR_INSERT trigger).

**Phase 1 — 2-cycle detection, immediate reporting, single collector, min-hash local victim.**
- DBACC: at `placeSerialQueue`, immediately send the edge to `collector = min-key` TC
  (Component A, no delay, no new ACC state, single collector). Single foreign blocker first;
  then enumerate the parallel queue.
- DBTC: add `waitForEdges` storage on `ApiConnectRecord` (Component C), implement the 2-cycle
  predicate, choose the min-key endpoint (== the collector) as victim, abort with
  `returncode = 266` reusing the `timeOutFoundLab` path. Retain code per Component D.
- This delivers the headline win: instant, deterministic 2-cycle resolution.

**Phase 2 — correctness hardening & observability.**
- Re-validation everywhere (stale oprec/transid, state checks).
- `ndbinfo` table / DUMP code to expose accumulated wait-for edges and detected cycles for
  debugging.
- MTR tests (§10).

**Phase 3 — shared-lock fan-out & scan locks.**
- Emit edges for all foreign transactions in the parallel queue.
- Cover the scan `IsBlocked` path.

**Phase 4 — deferred reporting (`T_detect`).**
- Add the delayed self-signal in DBACC + the "report scheduled" op marker + cancel-on-wakeup
  in `startNext`. Add/derive the `T_detect` knob (§7).

**Phase 5 — longer cycles & smarter victims (optional / research).**
- Send-to-both (§6.2) and/or path-pushing (§5).
- Remote victim abort signal (`DBTC_ABORT_VICTIM_REQ`) + cost-based victim selection.

## 10. Testing strategy

- **Unit / signal-level:** ERROR_INSERT hooks to force two operations into a 2-cycle on the
  same row pair across two transactions; assert exactly one is aborted with error 266 and the
  other commits, well before `TransactionDeadlockDetectionTimeout`.
- **MTR:** a deterministic two-session deadlock (session A locks row1 then waits on row2;
  session B locks row2 then waits on row1); assert `HA_ERR_LOCK_WAIT_TIMEOUT` is returned
  *fast* and to the deterministic victim. Cross-instance and cross-node variants.
- **Regression:** confirm the application-visible code is still 266 and still classified
  TemporaryError (existing retry behaviour unchanged).
- **Negative:** slow-but-not-deadlocked transactions must *not* be aborted by proactive
  detection (only by the backstop timeout).
- **Per the project convention, the user runs builds and tests.** This plan only lists the
  commands/cases; it does not run them.

## 11. Decisions (resolved) & remaining questions

**Decided (requester, "simple first, complex later"):**
1. **Selection key:** `min hash(transid)`, tie-broken by raw transid. Used for *both* collector
   routing and victim selection, so the victim == collector == local. Hashing is deterministic
   (all nodes agree) and unbiased (no systematic old-transaction / low-node penalty). §2.2, §6.3.
2. **Collectors:** a **single** collector per edge initially; "send to both" is a later
   extension. §6.2.
3. **Timing:** **immediate** reporting first; deferred `T_detect` is a later extension. §6.1.
4. **Error code:** **reuse 266** (retained, app-transparent). §6.4, §7, Component D.

**Still open / to confirm:**
5. **Scope of Phase 1:** key-op locks only, or include scan locks (`IsBlocked` path) from the
   start? (Default: key ops first, scans in Phase 3.)
6. **Hash function:** any fast deterministic mix is fine; confirm whether to reuse an existing
   NDB hash helper or a small inline multiplicative/xor mix.

## 12. File-by-file change map (Phases 0–1)

| File | Change |
|------|--------|
| `storage/ndb/include/kernel/signaldata/DeadlockWaitfor.hpp` | **NEW** — `DeadlockWaitforRep` struct + `SignalLength` |
| `storage/ndb/include/kernel/GlobalSignalNumbers.h` | `#define GSN_DBACC_WAITFOR_REP` + bump `MAX_GSN` (line 40) |
| `storage/ndb/src/common/debugger/signaldata/SignalNames.cpp` | name-table entry (before `};` at line 1091) |
| `storage/ndb/src/kernel/blocks/dbacc/Dbacc.hpp` | (Phase 4) op marker bit; helper decl for edge send |
| `storage/ndb/src/kernel/blocks/dbacc/DbaccMain.cpp` | `#include` header; edge capture + `sendSignal` in `placeSerialQueue` / `placeReadInLockQueue` / `placeWriteInLockQueue`; `get_tc_ref` for waiter+owner; collector selection |
| `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp` | `execDBACC_WAITFOR_REP` decl; `WaitForEdge` record; `waitForEdges` head on `ApiConnectRecord`; pool index (+ bump `c_transient_pool_count`) |
| `storage/ndb/src/kernel/blocks/dbtc/DbtcInit.cpp` | `addRecSignal(GSN_DBACC_WAITFOR_REP, …)` |
| `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` | `#include` header; `execDBACC_WAITFOR_REP` (resolve, verify, store, detect, abort); pool registration; init/release of `waitForEdges` in ctor / `initApiConnectRec` / release |
| `storage/ndb/src/common/debugger/signaldata/{DeadlockWaitfor.cpp,CMakeLists.txt}` + `SignalDataPrint.cpp` | **OPTIONAL** signal printer |

## 13. Implementation status

**Phase 0 + Phase 1 implemented** (2-cycle detection, immediate reporting, single
collector, min-hash local victim). Files changed:

- `signaldata/DeadlockWaitfor.hpp` (NEW) — `DeadlockWaitforRep` (10 words: senderRef,
  flags, and {transId1,transId2,tcRef,tcOprec} for waiter + owner). `CollectorIsWaiter`
  flag tells DBTC which endpoint is the local collector.
- `GlobalSignalNumbers.h` — `GSN_DBACC_WAITFOR_REP = 979`, `MAX_GSN → 979`.
- `SignalNames.cpp` — name-table entry.
- `dblqh/Dblqh.hpp` — `try_get_tc_ref()` (guarded `get_tc_ref`: returns false instead of
  crashing when the lock owner is not a key-op TcConnectionrec, e.g. a scan).
- `dbacc/DbaccMain.cpp` — file-local `deadlock_transid_hash()` / `deadlock_a_is_collector()`;
  in `accIsLockedLab`, on the `ZSERIAL_QUEUE` result, capture the waiter→owner edge under
  the frag mutex, then after releasing it send `DBACC_WAITFOR_REP` to the min-hash
  collector TC (reusing `signal`, restoring `theData[0]=RNIL` for the blocked-path caller).
- `dbtc/Dbtc.hpp` — `ApiConnectRecord::m_deadlock_edges[4]` (inline fixed array;
  `DeadlockEdge` = other transid + timer + direction bitmask) + `DLD_WAITS_ON/DLD_WAITED_BY`;
  decls for `execDBACC_WAITFOR_REP` + `recordDeadlockEdge`.
- `dbtc/DbtcInit.cpp` — `addRecSignal(GSN_DBACC_WAITFOR_REP, …)`.
- `dbtc/DbtcMain.cpp` — `#include`; clear edges in ctor + `initApiConnectRec`;
  `execDBACC_WAITFOR_REP` (resolve local collector by tcOprec, validate transid + active
  state, record edge, on 2-cycle call `timeOutFoundLab(…, ZTIME_OUT_ERROR)`);
  `recordDeadlockEdge` (per-other-txn directed-edge cache with a freshness window =
  `ctimeOutValue`, evicts oldest slot when full).

**Deviation from §3 Component C (deliberate, "simple first"):** edges are stored in a small
**inline fixed array** on `ApiConnectRecord` (capacity 4) rather than a `TransientPool`.
This avoids the pool-plumbing (bumping `c_transient_pool_count`, RSS snapshots) at the cost
of ~64 bytes per `ApiConnectRecord` and a bounded edge count (overflow evicts oldest; the
timeout backstop still catches anything dropped). Migrating to a `TransientPool` to remove
the per-record footprint and the capacity bound is a Phase 2 hardening item.

**Phase 3 implemented** (shared-lock parallel-queue fan-out + scan coverage):

- **Shared-lock fan-out** — `accIsLockedLab` now walks the lock owner's *parallel
  queue* and emits one wait-for edge per *distinct foreign transaction* co-holding the
  lock (capped at 8/wait), instead of only the head owner. Captured under the frag mutex
  into a local array, sent after release.
- **Scan coverage** — see §15 for the model. (The initial `scanTcrec`-based resolution was
  superseded; scans are now handled via the kind-aware routing in §15, which never makes a
  scan the collector/victim and so needs no scan-side TC resolution.)

**Not yet done:** deferred `T_detect` reporting (Phase 4), pool-backed edge storage +
edge cleanup on wakeup (hardening), `ndbinfo`/DUMP observability (Phase 2), longer-cycle
handling and full scan-as-collector support (Phase 5). The existing
`TransactionDeadlockDetectionTimeout` remains the backstop for all of these.

## 14. Bug fix, tracing, and the MTR test

**Bug fixed (owner-side edge was dropped).** The first `execDBACC_WAITFOR_REP` rejected any
edge whose collector operation was not `OS_OPERATING`. For a 2-cycle the two edges resolve at
the same collector transaction via *two different* operations: the waiter-side op (still
blocked → `OS_OPERATING`) and the owner-side op (already holds the lock, so it completed →
`OS_PREPARED`). The owner-side edge was therefore always dropped, so only the `WAITS_ON`
direction was recorded and the cycle never closed — it fell back to the timeout. **Fix:**
removed the op-level `tcConnectstate` check; safety is preserved by `getValidPtr` (magic) on
the `TcConnectRecord`, the **transid match** on the resolved `ApiConnectRecord`, and the
transaction-level abortable-state switch.

**Tracing.** A `DEB_DEADLOCK` macro (gated on `DEBUG_DEADLOCK`, off by default) in both
`DbaccMain.cpp` and `DbtcMain.cpp` logs every edge sent, the per-serial-wait waiter/owner
resolution, the received edge, **every** DBTC drop point with its reason, the accumulated
per-other-transaction direction bitmask, and the cycle/victim-abort. Enable by flipping
`//#define DEBUG_DEADLOCK 1` in both files.

**Test.** `mysql-test/suite/ndb/ndb_deadlock_discovery.{test,cnf,result}`:
- The `.cnf` sets `TransactionDeadlockDetectionTimeout = 60000`, so a deadlock resolved in
  well under that proves *proactive* detection fired rather than the timeout.
- Scenario 1 (negative): a plain lock wait (no cycle) must not be falsely aborted.
- Scenario 2: a true 2-row cycle; the victim is chosen by hash (either connection), so the
  per-connection outcome is suppressed and the test asserts (a) no hang, (b) resolution
  `< 20s` (the proactive proof, via a `--die` guard), and (c) consistent committed data.

## 15. Scan locking and deadlock nodes (revised model)

The Phase-3 scan handling (resolve a scan endpoint via `scanTcrec`, route by min-hash,
drop scan-as-collector) is **superseded** by the model below, which is both simpler and a
better fit for how NDB scan locks actually behave.

**Key fact (lock lifetime + abortability).** A locking scan holds its row locks only for
the lifetime of a *batch*; at the next `SCAN_NEXTREQ` they are released, unless the API
**takes over** a lock — which converts it into a normal key operation held to commit.
Crucially, a scan fragment **cannot be aborted in isolation**: takeover ties some locks
into the transaction, and there is no support for aborting a subset of a transaction's
operations. So **aborting a scan ⇒ aborting its whole transaction.**

**Consequences:**
1. The deadlock-graph **node for a scan is its transaction** (keyed by the transaction's
   transid, already on the scan op as `scanTrid1/2`), *not* a separate "batch" node — a
   batch is not an independently-abortable victim. The batch concept only affects edge
   *lifetime* (a batch-held lock is released sooner than commit), which the freshness
   window (and, later, deferred reporting) already handle.
2. A **taken-over** lock is a key op held to commit → it is a transaction (key-op) node.
   The discriminator is the ACC op's `scanRecPtr`: `!= RNIL` ⇒ a transient *batch* scan
   lock (kind = scan); `== RNIL` ⇒ a key op / taken-over lock (kind = key-op). Ownership of
   a lock therefore flips from scan-node to transaction-node exactly at takeover, for free.

**Victim policy — "don't always abort scans".** Aborting a scan transaction is the costly,
messy path (cascade through takeover + scan teardown), so proactive detection avoids it.
This is achieved purely in the **collector-routing rule** (no remote abort needed, victim
stays = collector = local):

```
collector(W, O) =
    if exactly one of {W, O} is a scan:  the NON-scan (key-op) endpoint
    else (both key-op, or both scan):    the smaller-hash endpoint   (as before)
```

- **scan ↔ key-op deadlock:** both edges have endpoint set {scanTxn, keyOpTxn} with exactly
  one scan, so both route to the **key-op** transaction → it is the collector and the
  victim. The key-op is aborted via the existing `timeOutFoundLab` path; the scan survives
  and proceeds once the lock is released. *Proactive detection never aborts the scan.*
- **scan ↔ scan deadlock:** both endpoints are scans → min-hash → falls back to the
  **timeout** (left unhandled proactively for now; rare). Could be added later if needed.
- **key-op ↔ key-op:** min-hash, exactly as today.

**Why this is also simpler.** In the scan↔key-op case the collector is always a key op, so
DBTC resolves it via `tcOprec → TcConnectRecord` (already works) and the scan endpoint is
just the "other", recorded by **transid** (`scanTrid`). No `scanTcrec`/`ScanFragRec`
resolution, no scan-as-collector path, no cross-instance scan lookup. DBACC only needs, per
endpoint: its transid, its kind (`scanRecPtr != RNIL`), and — for the key-op endpoint only —
its `(tcOprec, tcRef)` via `get_tc_ref`.

**Status: IMPLEMENTED.** Changes from the initial Phase-3 scan handling:
- `Dbacc::DeadlockEndpoint` + `describe_deadlock_endpoint()` classify each endpoint by
  `scanRecPtr` (`!= RNIL` ⇒ scan) and resolve the TC handle only for key ops
  (`try_get_tc_ref`); scan endpoints carry just transid + `isScan` (handle 0).
- `send_deadlock_waitfor()` applies the kind-aware collector rule and drops scan↔scan
  edges; used by both the `accIsLockedLab` fan-out and the `checkNextBucketLab` scan hook.
- The `get_op_tc_ref`/`try_get_scan_tc_ref` (`scanTcrec`) resolution was removed.
- The DBTC handler is unchanged (the collector is always a key op in the cases we act on).

**Verified:** a normal key-op ACC op has `scanRecPtr == RNIL` on every non-scan seize path
(`DbaccMain.cpp` key-op init, explicit/takeover lock req, copy-frag), and only the batch
scan seize sets it; so the discriminator is reliable and takeover locks correctly classify
as key ops. Aborting the key-op victim frees the scan's awaited lock via the standard
transaction-abort path.

**Note (scan↔scan):** two locking scans can deadlock only if at least one takes exclusive
locks (two shared scans don't conflict). These are dropped proactively and left to the
timeout; supporting them would require a scan-victim abort path (abort the scan ⇒ abort its
transaction, since a scan fragment can't be aborted in isolation — takeover entanglement +
no partial abort).
