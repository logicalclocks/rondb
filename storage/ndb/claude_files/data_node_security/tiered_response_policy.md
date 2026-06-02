# Tiered Malicious Input Response Policy

**Status:** FINAL — ready for implementation plan
**Author:** security-intern initiative
**Related:** [tckeyreq_security.md](tckeyreq_security.md), [fragmented_signal_security.md](fragmented_signal_security.md)

---

## 1. Overview

RonDB currently disconnects an entire node on the *first* malicious signal it detects (the existing [`disconnectMaliciousNode()`](../../src/kernel/blocks/dbtc/DbtcMain.cpp) in DBTC). This design extends that model to a graduated, multi-tier response so that:

- Active malice — signals only a compromised or fundamentally malformed sender could produce — is cut immediately (Tier A).
- Semantic violations that *could* indicate either a buggy client or an attacker are *logged for forensics* with no automated disconnect; humans review and decide whether to intervene (Tier B).
- Volumetric (well-formed-but-excessive) traffic is rate-limited *upstream* at the API node where per-user identity is available, with a coarse cluster-side safety net for catastrophic overload (Tier C).

The framework is **detection-agnostic**: any kernel block can report a violation to a central manager (QMGR), which holds per-node counter state, applies tier-specific handling, and triggers disconnect via the existing path only for Tier A or the Tier C safety net.

The model prioritizes customer experience over paranoia, consistent with the deployment threat model in Section 3.

---

## 2. Goals

| Priority | Goal |
|---|---|
| Primary | **Defense.** Prevent crashes, OOM, memory corruption, transaction hijacking, and resource exhaustion from peer-originated signals. |
| Primary | **No false-positive disconnects of multi-tenant API nodes.** No legitimate user should lose their connection because of another user's behavior (the "punishment laundering" concern, Section 3.2). |
| Secondary | **Observability.** Surface per-node violation counts, types, and timing via ndbinfo and structured cluster log lines so operators can distinguish a buggy client from an active attack. Operator-facing monitoring is treated as load-bearing, not optional polish. |
| Non-goal | **Per-request authentication.** This policy does not add identity/auth checks. The cluster's existing trust model (NodeId established at cluster join) is preserved. |
| Non-goal | **TLS / transport encryption.** Orthogonal — handled by existing transporter TLS configuration. |

---

## 3. Threat Model

### 3.1 Actors

The threat actor is assumed to be a peer that has already passed cluster authentication and is now sending arbitrary signals. Concretely:

- **Compromised host** running an API node.
- **Malicious user** driving a legitimate API node (e.g., crafted SQL through mysqld, or direct NDB API use).
- **Compromised data node.** Catastrophic — full cluster trust. Treated as zero-tolerance (any violation → Tier A by override).
- **Buggy client library** sending malformed signals unintentionally. Distinguished from attackers by logged-not-disconnected response for ambiguous cases.
- **Version-skew client** sending signals the current server doesn't accept.

**Out of model:**
- Network-layer attackers between nodes (mitigated by transport TLS).
- Side-channel attacks (timing, memory pressure).
- Authentication-layer attacks on cluster join.

### 3.2 Punishment laundering — recognized threat class

Multi-tenant API nodes (mysqld, RDRS) multiplex many users through a single NodeId. If our policy disconnected an API node for *any* user-attributable bad behavior, an attacker could deliberately trigger violations to weaponize our defense into a denial-of-service: get the API node disconnected, every legitimate user on that mysqld loses their connection. We call this *punishment laundering*.

The NDB signal protocol does not carry user-level identity (no SQL session ID, no HTTP client ID). The cluster sees only per-NodeId attribution. We cannot disconnect "just the offending user" through the signal layer — fixing that requires either protocol extensions or cooperation from the API node, both of which are out of v1 scope (see Section 12).

Our defense against laundering is structural and applied in two places:

1. **Tier A criteria are defined to be impossible to trigger via valid user inputs** (Section 6's categorization rule). When a Tier A violation fires, the API node itself is the problem — not a user on it — so disconnecting the API node is the correct action.
2. **Tier B violations (which *could* be user-triggerable) are log-only.** No automated disconnect. The laundering attack has no payload.

### 3.3 Deployment assumptions

RonDB deployments typically run on network-segmented infrastructure with API nodes accessible only from trusted application tiers. This means:

- External attackers usually cannot reach the cluster directly.
- The dominant threat is *internal* (a vulnerable internal service, an insider, or a compromised application user).
- False-positive disconnects are correspondingly more harmful than missed alerts, because real attacks are rare and customer impact is the primary cost driver.

This assumption tilts the policy toward log-and-investigate (Tier B) over automated disconnect for ambiguous cases. **If a deployment cannot rely on segmentation, operators should treat Tier B accumulations as higher-priority signals and consider tightening upstream rate limits in their API node configuration.** The policy itself does not change between segmented and non-segmented deployments — operators tune Tier C upstream rate limits and monitoring thresholds for their environment.

### 3.4 Open-source attack-model considerations

The RonDB source is publicly available, so attackers can read every validation check, every tier definition, and every detection site. The defenses are designed to hold under that assumption — input validation, categorization rules, and disconnect mechanisms work the same regardless of attacker knowledge. Security-through-obscurity is not part of the threat model.

The one implication worth being explicit about: a skilled attacker reading the source can see exactly which violations are Tier B (log-only, non-disconnecting) and may attempt to use them as cover, for timing probes, or for resource consumption. **Tier C upstream rate limits are therefore the load-bearing defense against Tier-B-as-noise attacks** — not optional polish. Deployments without effective upstream rate limits should treat Tier B alert volume more aggressively (Section 11.3) and tighten the cluster-side safety-net threshold (Section 8.4) accordingly.

A second implication: the security-state ndbinfo views (Section 11.1) provide *live confirmation* of detection to anyone with read access. An attacker who can query `ndbinfo.security_events` can probe and immediately see whether their attack incremented a counter — a runtime feedback channel that's qualitatively more useful than the static source-code knowledge alone. The mitigation is access control: these views require the `PROCESS` privilege, not default read access. In multi-tenant deployments, the views also disclose per-NodeId security activity across all tenants on a shared mysqld; restricting to operator/DBA accounts is the standard expectation. We acknowledge this disclosure surface but accept it as a cost of meaningful observability — operators need the live view to act.

**The same disclosure surface exists in the cluster log.** Structured `SECURITY_EVENT:` and `SECURITY_KILLSWITCH:` lines (Sections 11.2 and 8.8) carry the same information as the ndbinfo views — `node_id`, `violation_type`, `source_line`, etc. — and many deployments ship cluster logs to external log aggregators (Loki, Splunk, Elasticsearch). If those aggregators have weaker access control than mysqld's `PROCESS` gate, the same attacker-feedback channel exists through a different door. Customers locking down ndbinfo must also lock down log access correspondingly. The doc cannot enforce this at the code level; it's an operational requirement that operators must internalize.

---

## 4. Background: What "Malicious" Means Today

A code audit of [DbtcMain.cpp](../../src/kernel/blocks/dbtc/DbtcMain.cpp) identifies **23 call sites** of `disconnectMaliciousNode()` plus 2 in RONDIS. These cluster into five categories:

| Category | Count | Single-signal kill? | Honest client could trigger? |
|---|---|---|---|
| Out-of-bounds index/pointer (`apiConnectPtr`, `tableId`) | 6 | Yes | No |
| Size/length DoS (oversize key/value/section, length mismatch) | 7 | Mixed | Sometimes (oversize values, oversize sections) |
| Semantic protocol constraints (flag combinations, state machine) | 5 | Yes | Yes (state machine bugs, version skew) |
| Fragment-assembly state corruption (multi-fragment attacks) | 4 | No — needs multiple signals | No |
| Transaction hijacking (`apiConnectPtr` not owned by sender) | 4 | Yes | No |

**Key observation:** 19 of 23 violations are caught and rejected on the *first* signal. Only fragment-assembly attacks (4 sites) require multi-signal accumulation, and those are already mitigated in `assembleFragments` Phase 1 hardening (commit `fa1713b4709`).

The existing recent commits (`d729dcd5d3a`, `effbed3eb44`, `6abe152f1f1`, `fa1713b4709`, `d9062c7d4cc`) addressed known bug classes in DBTC and RONDIS. **This policy does not assume those commits cover all possible vulnerabilities** — see Section 14.

---

## 5. Architectural Findings

### 5.1 Why HostRecord is the wrong home for counters

[`HostRecord` in DBTC](../../src/kernel/blocks/dbtc/Dbtc.hpp) was initially considered as the counter location because it is already a per-NodeId array allocated at `MAX_NODES` at startup ([DbtcInit.cpp:178](../../src/kernel/blocks/dbtc/DbtcInit.cpp)). It was rejected for three reasons:

1. **NDBMT fragmentation.** In multi-threaded ndbmtd, DBTC runs as multiple instances (TC threads). Each instance has its own `hostRecord[]`. A counter in HostRecord fragments across TC instances; the apparent "per-node count" is actually "per-(node, TC-thread) count." For Tier C rate detection, this fragmentation is a real problem.

2. **Cross-block detection.** Tier A violations can be detected in any block (DBTC, DBSPJ, DBLQH, DBDICT, ...). A counter in DBTC's HostRecord requires awkward cross-block signaling to update.

3. **Scope mismatch.** HostRecord holds transactional state (LQH packing, timing histograms, API failure handling). Security policy belongs alongside node-management state, not transactional state.

### 5.2 Why QMGR is the right home

[QMGR](../../src/kernel/blocks/qmgr/) is:

- **Singleton per data node** (not per MT instance) — no fragmentation.
- **Already in the disconnect path.** The current `disconnectMaliciousNode()` already sends `DUMP_STATE_ORD 900/939` to QMGR, which executes the actual disconnect via `api_failed()` / `node_failed()`.
- **Already responsible for node lifecycle** (heartbeat, failure detection, cluster membership). Adding "track per-node security state" is a natural extension of role.

### 5.3 Tier C lives in TransporterRegistry

[`TransporterRegistry`](../../include/transporter/TransporterRegistry.hpp) already maintains per-NodeId rate state: `get_overload_count(NodeId)`, `get_slowdown_count(NodeId)`, `m_status_overloaded` / `m_status_slowdown` bitmasks, `m_bytes_received`, `receiveCount`. The cluster-side safety net for volumetric attacks co-locates naturally with this existing infrastructure.

### 5.4 RONDIS stays separate

[RONDIS](../../src/rondis/) is architecturally separate — each Redis client has its own TCP connection. RONDIS-level violations never traverse the NDB transporter. RONDIS gets its own per-Redis-connection counter system. This is a feature: per-Redis-client granularity is finer than NodeId, so even when RONDIS chooses to enforce (e.g., per-connection rate limits), the disconnect blast radius is one Redis client, not the whole RONDIS NodeId.

---

## 6. The Three-Tier Response Policy

### Tier A — Immediate disconnect

**What it is:** violations that no protocol-compliant implementation could ever produce from valid user inputs. Active malice or fundamental compromise of the sender.

**Action:** disconnect the offending node via the existing QMGR path (`api_failed()` for API nodes, `node_failed()` for data nodes). No grace, no threshold. Subject to the master kill switch (Section 8.8).

**Categorization rule (HARD REQUIREMENT):** A violation is Tier A *only if* it is **impossible to trigger via valid SQL, HTTP, REST, or Redis user inputs** at any multi-tenant API node. This rule exists to prevent punishment-laundering attacks (Section 3.2). When adding new violation types in future work, this rule must be verified explicitly; misclassifying a user-triggerable violation as Tier A reintroduces the laundering attack vector.

If a Tier A violation fires from a multi-tenant API node, the implication is that the API node *itself* is compromised, fundamentally buggy, or running incompatible code — not that a specific user did something bad. Disconnecting the API node is correct regardless of which users are on it. The alternative — letting a compromised or fundamentally-broken API node continue submitting signals — is strictly worse than the false-positive customer impact.

**Examples** (verified user-untriggerable):
- Signal with zero sections when sections required
- `sectionNo >= 3` in fragmented signal
- Signal length shorter than declared static header
- Negative-where-unsigned values
- `apiConnectPtr` past pool end (server-issued handle; honest API node returns it verbatim)
- `apiConnectPtr` not owned by sender (hijacking; API node manages this internally)
- Any violation from a data node (data nodes are peers running identical code; no honest-mistake mode)
- Start flag during `CS_ABORTING` (state machine attack pattern)

### Tier B — Log-only forensic observability

**What it is:** semantic violations that *could* indicate a buggy client, version skew, or an attacker — and that *could* be reachable from valid user inputs at a multi-tenant API node.

**Action:** increment counters (cumulative + sliding window), emit a structured `SECURITY_EVENT:` cluster log entry, update ndbinfo. **No automated disconnect.** Humans review the pattern; if a deployment determines the activity is malicious, an operator can manually disconnect via existing administrative interfaces.

**Rationale:**
- The safety property (no crash, no OOM, no memory corruption) is provided by the input-validation/cap logic that *already* rejects the offending operation. The transaction has already failed; the policy layer adds visibility, not safety.
- Tier B violations are reachable from valid user inputs at multi-tenant API nodes. Automated disconnect would create the laundering attack vector.
- Network segmentation (Section 3.3) makes real attacks rare; false-positive customer impact dominates the cost calculus.
- An attacker doing genuinely concerning Tier B activity accumulates visible patterns in the sliding window; operators have time to investigate and respond manually.

**Examples:**
- Oversize KeyInfo / AttrInfo section
- Key length exceeds `MAX_KEY_SIZE_IN_WORDS`
- `CommitFlag` without `ExecFlag`
- `UNLOCK` without distribution key
- Reorg flag with invalid operation type
- Signal length mismatch in KEYINFO / ATTRINFO / TCKEYREQ
- RONDIS oversize SET value (handled per-Redis-connection inside RONDIS)
- RONDIS SELECT db index out of range

### Tier C — Upstream rate limits, cluster-side safety net

**What it is:** volumetric or resource-exhaustion attacks via well-formed signals. Per-signal validation cannot see these because each individual signal is legal.

**Enforcement layers** (in order of preference):

1. **At the API node, per-user.** Where finer-grained identity is available, this is the right place:
   - **mysqld**: existing MySQL session/user rate-limit features.
   - **RDRS**: HTTP-level rate limits per client IP / API key (RDRS-side implementation).
   - **RONDIS**: per-Redis-connection rate limits inside RONDIS — closing a single Redis TCP connection does not affect other clients.
2. **Cluster-side safety net.** A coarse per-NodeId rate check in TransporterRegistry. Threshold set high enough that *only* a buggy or compromised API node could reach it ("this signal volume is not produced by any legitimate workload"). At that point, disconnecting the API node is justified — the upstream protection has failed and the cluster is being overwhelmed. The safety net emits a `MALICIOUS_SIGNAL_REPORT` at Tier A with `violation_type = RATE_LIMIT_EXCEEDED`.

### Override rule

Any Tier B violation, when committed by a data node sender (NodeId is type `NODE_TYPE_DB`), is escalated to Tier A. Data nodes are cluster peers running identical code; they have no plausible "honest mistake" mode.

### Why Tier B has no automatic disconnect

This is the principled choice that resolves the punishment-laundering concern. Summary of the reasoning:

| Question | Answer |
|---|---|
| What would Tier B disconnect *defend against*? | Sustained semantic violations from one node. |
| What does input validation already cover? | The actual safety property: no crash, no OOM, no corruption. Operation rejected. |
| What does Tier B disconnect *cost*? | False-positive disconnects of multi-tenant API nodes, harming legitimate users (laundering attack). |
| What does disconnect *uniquely add*? | Operator attention via pager alerts vs. silent log lines. |
| Can we get attention without disconnect? | Yes — via structured `SECURITY_EVENT:` log lines and ndbinfo visibility (Section 11). |

Disconnect is the wrong tool for Tier B. Logging plus operator-facing observability achieves the attention without the customer impact.

### Defense-in-depth via block-level audit, not transporter-layer validation

A transporter-layer "validate every signal against per-GSN minimum length and section bounds" check was considered and rejected. It would have provided universal coverage against the simplest class of structural bugs in unaudited blocks, but at a permanent ~0.2-0.5% receive-path overhead — meaningful in RonDB's performance-first context. The class of bug it catches (signal too short, section count below expected) is the same class that per-block audit catches reliably during signal-handler review.

Instead, defense against unknown structural bugs in unaudited blocks is provided by **prioritized per-block audit as the explicit next phase after v1 ships** (Section 12). One-time audit work substitutes for permanent runtime overhead — a better fit for a database whose differentiator is throughput.

---

## 7. Tier Assignment Catalog

Each known violation site mapped to its tier. Categorization is hardcoded.

### DBTC sites (from [DbtcMain.cpp](../../src/kernel/blocks/dbtc/DbtcMain.cpp))

| Line | Reason string | Tier | Notes |
|---|---|---|---|
| 2462 | signal in unexpected apiConnectRecord state | **A** | State machine attack — user-untriggerable |
| 2486 | apiConnectRecord owned by different node | **A** | Hijacking — user-untriggerable |
| 2658 | start flag during active abort | **A** | State machine attack — user-untriggerable |
| 2883 | invalid apiConnectPtr in KEYINFO | **A** | Out-of-bounds pointer — server-issued handle |
| 2895 | KEYINFO apiConnectPtr not owned by sender | **A** | Hijacking — user-untriggerable |
| 2990 | KEYINFO signal length mismatch | **B** | Could be lib bug; log only |
| 3037 | invalid apiConnectPtr in ATTRINFO | **A** | Out-of-bounds pointer |
| 3051 | ATTRINFO apiConnectPtr not owned by sender | **A** | Hijacking |
| 3075 | ATTRINFO signal too short | **A** | Structural — wire-format level |
| 3624 | TCKEYREQ signal too short | **A** | Structural |
| 3641 | TCKEYREQ KeyInfo section too large | **B** | Reachable via large WHERE/INSERT; log only |
| 3651 | TCKEYREQ AttrInfo section too large | **B** | Reachable via large attribute payload; log only |
| 3682 | invalid apiConnectPtr in TCKEYREQ | **A** | Out-of-bounds pointer |
| 3702 | TCKEYREQ apiConnectPtr not owned by sender | **A** | Hijacking |
| 3735 | table index out of bounds in TCKEYREQ | **A** | Out-of-bounds |
| 4443 | reorg flag with invalid operation type | **B** | Version-skew or lib bug; log only |
| 4497 | TCKEYREQ long signal length mismatch | **B** | Could be lib bug; log only |
| 4515 | TCKEYREQ short signal length mismatch | **B** | Could be lib bug; log only |
| 4532 | UNLOCK without distribution key | **B** | Semantic constraint; log only |
| 4800 | CommitFlag without ExecFlag | **B** | Semantic constraint; log only |
| 4917 | key length exceeds MAX_KEY_SIZE_IN_WORDS | **B** | Reachable via long-key schema; log only |
| 15913 | SCAN_TABREQ missing required section 0 | **A** | Structural |
| 15970 | invalid apiConnectPtr in SCAN_TABREQ | **A** | Out-of-bounds pointer |

### RONDIS sites (from [storage/ndb/src/rondis/](../../src/rondis/))

| Component | Violation | Tier | Notes |
|---|---|---|---|
| commands.cc | Oversize SET value (> REDIS_MAX_VALUE_LEN) | **B** | Reachable from any Redis client; RONDIS counter, per Redis connection |
| rondb.cc | SELECT db index < 0 or >= g_num_databases | **B** | Reachable from any Redis client; RONDIS counter, per Redis connection |

### Override rule (restated)

Any Tier B violation from a data node sender is escalated to Tier A. Data nodes are cluster peers with no honest-mistake mode.

### Block-routing bypass (recognized attack class, comprehensive coverage deferred)

Some NDB signal types are internal-only — DICT-internal, SUMA-internal, kernel-coordination signals — and have no legitimate user-callable path. A compromised or buggy API node could send these to a data node. The receiving block typically handles them assuming a trusted internal sender; the result of receiving them from an API node ranges from harmless to crash, depending on the signal.

Per the categorization rule (Section 6), any such signal from an API node satisfies Tier A criteria: no legitimate user input causes mysqld / RDRS / RONDIS to construct an internal-only signal. The framework supports this — a new `ViolationType::WRONG_SENDER_TYPE_FOR_GSN` plus per-call-site checks fit the existing pattern.

**v1 status:** the framework supports this attack class but does not include the comprehensive GSN-to-sender-type allow-list needed to enforce it broadly. Building that list requires auditing NDB signal types across [storage/ndb/include/kernel/signaldata/](../../include/kernel/signaldata/) and classifying each as API-callable / DB-internal / MGM-only. This parallels the broader block-audit deferral (Section 12.1): framework first, catalog after.

What gets v1 coverage:
- The existing "any violation from a data node → Tier A" override is already a form of sender-type check.
- Per-block sender-type checks discovered during the call-site migration (Section 15 item 6) are brought into the framework with the new violation type tag.

What is post-v1:
- The comprehensive allow-list and the corresponding `reportMaliciousSignal` calls at every signal entry point.

**Honest expected hit rate:** mostly buggy custom NDB API clients (development errors), occasionally a compromised host probing privileged signal types. The rare "compromise" cases are the high-value catches because they overlap with privilege-escalation surface.

---

## 8. Counter Infrastructure Design

### 8.1 QMGR counter state

QMGR holds a per-NodeId security state, indexed directly by node id:

```cpp
struct NodeSecurityState {
  // Cumulative-since-cluster-start counters for observability/ndbinfo.
  // Per-violation-type granularity is preserved in the cumulative totals via
  // m_last_violation_type plus the cluster log; only the *live window* is
  // per-node (not per-type) — see the design note below.
  Uint64 m_total_tier_a_strikes;
  Uint64 m_total_tier_b_strikes;
  Uint64 m_total_disconnects;
  NDB_TICKS m_last_strike_time;
  Uint32 m_last_violation_type;
  Uint32 m_last_source_line;

  // Per-NODE sliding window: one 10-bucket × 30 s ring (5 minute window),
  // shared across all violation types. ~80 bytes/node.
  // On a strike: idx = epoch % NUM_BUCKETS (epoch = now / 30s); if the bucket's
  // stored epoch != current epoch, reset it; then ++count. current_window_count
  // = sum of buckets whose epoch is within the last NUM_BUCKETS epochs.
  Uint32 m_window_count[NUM_BUCKETS];
  Uint32 m_window_epoch[NUM_BUCKETS];
};
```

**Per-node window, not per-type (memory decision).** An earlier draft kept a
separate sliding window *per violation type* (`m_buckets[NUM_VIOLATION_TYPES]
[NUM_BUCKETS]`), ~4.5 KB/node. That carries a multiplier equal to the size of the
violation catalog — and the catalog *grows* as the Phase 2 block audit adds
types, so the per-type window gets more expensive precisely as we do more
security work. We instead keep a single window per node (~80 bytes/node) shared
across types. Per-type detail is still available in the cumulative counters and
the cluster log; only the live "what's happening right now" window is coarsened
to per-node. This is a deliberate trade favouring RonDB's efficiency-first
identity.

**Fixed allocation (no lazy/sparse map, no allocation-failure path).** Because the
per-node state is now compact (~128 bytes/node including cumulative counters), it
is held in a fixed array sized by the compile-time node-id ceiling
(`MAX_NODES_ID + 1`), allocated once in the QMGR singleton (~260 KB total). There
is therefore **no runtime allocation to fail** — the earlier `SECURITY_DEGRADED`
alloc-failure machinery is removed entirely. (The heavier per-type design was what
made lazy allocation worthwhile; the cheap per-node design makes a plain fixed
array the simpler and equally efficient choice.)

**Window is computed lazily at read time.** `current_window_count` is summed from
the ring only when the ndbinfo view is queried — no periodic sweep / CONTINUEB
timer is needed.

**Note:** The sliding window never triggers a disconnect. It exists purely so the
ndbinfo `current_window_count` column shows recent activity ("this node struck 3
times in the last 5 min") rather than only lifetime totals. Tier A fires on the
first strike; Tier B never fires automatically.

### 8.2 The MALICIOUS_SIGNAL_REPORT signal

New internal signal. Replaces direct calls to `disconnectMaliciousNode()` from blocks.

```
GSN_MALICIOUS_SIGNAL_REPORT (sent JBA to QMGR_REF):
  theData[0] = offending node ID
  theData[1] = tier (0 = A, 1 = B)
  theData[2] = violation_type (enum)
  theData[3] = source block reference (for forensics)
  theData[4] = source line (__LINE__, for forensics)
  theData[5] = reason string ID (lookup table; avoids variable-length strings)
  theData[6] = suppressed report count (see Section 8.3)
```

QMGR's handler:

1. Look up `NodeSecurityState[nodeId]`.
2. Increment the current sliding-window bucket for `violation_type`. Update cumulative counters and `m_last_*` fields.
3. Emit a structured `SECURITY_EVENT:` cluster log line containing all fields above (Section 11.2).
4. If `tier == A` **and** the master kill switch (Section 8.8) is enabled: invoke the existing disconnect path. Increment `m_total_disconnects`.
5. If `tier == B`: no further action. Counter increment, log line, and ndbinfo update are the entire response.

**Rolling-upgrade safety.** The QMGR handler must validate `violation_type < NUM_VIOLATION_TYPES_KNOWN_TO_RECEIVER` before indexing into the bucket array. Out-of-range values — which can occur when a newer-version sender emits a violation type the receiver was built without — are routed to a generic `UNKNOWN_VIOLATION_TYPE` bucket and a cluster log line. This ensures rolling upgrades don't crash older nodes when newer nodes emit new violation types. The receiver still counts and logs the event; it just can't categorize it.

### 8.3 Report-rate limiting (DoS-amplification prevention)

If every malicious signal generated a `MALICIOUS_SIGNAL_REPORT` to QMGR, an attacker would force N+1 signals worth of work per bad signal. Mitigation:

- Each detection block keeps **lazily-allocated** per-NodeId suppression state. State for a NodeId is created on first violation, freed when the NodeId disconnects.
- Suppression granularity is per-NodeId (not per-(NodeId, violation_type)). A single bad node throttles all its violation types together. This loses some granularity but cuts steady-state memory from ~500 KB/block-instance to ~16 KB/block-instance, and across NDBMT × all detection blocks reduces total memory from ~20 MB to under 1 MB. The lost granularity costs us very little — a node sustaining multiple distinct violation types simultaneously is unusual enough that throttling them together is fine.
- Suppress reports within `K` milliseconds of the previous report for that NodeId (default K = 100 ms, configurable per Section 8.9).
- Suppressed events still increment a local counter; on the next non-suppressed report, include the suppressed count in `theData[6]` so QMGR credits them in batch and the counter remains accurate.

### 8.4 Tier C cluster-side safety net (per-NodeId overload-count sampling)

Implementation lives in QMGR (singleton, already runs a periodic timer) rather
than the TransporterRegistry, and uses the **existing per-NodeId receive-overload
counter** (`globalTransporterRegistry.get_overload_count(NodeId)`) as the rate
metric. No new per-link buckets, no new hot-path counters.

Once per second (gated inside the existing `ZTIMER_HANDLING` tick), QMGR
samples the cumulative overload count for every connected API/MGM node and
computes the delta against the previous sample. If the per-second delta exceeds
the configured threshold, QMGR emits `GSN_MALICIOUS_SIGNAL_REPORT` to itself
with `tier = A` and `violation_type = VT_RATE_LIMIT_EXCEEDED` — the normal
handler then runs the disconnect (gated by the master kill switch).

State held in QMGR:

```cpp
Uint32 m_securityRateLimitOverloadsPerSec;   // config; 0 = disabled
NDB_TICKS m_lastRateCheckTicks;
Uint32 m_nodeOverloadSample[MAX_NODES_ID + 1];  // last cumulative reading
```

Excludes data-node senders: legitimate replica-sync / LCP bursts can spike
inter-DB transporter overload, and a false-positive disconnect of a data node
would partition the cluster. The threat model is a compromised API node, not a
peer DB node.

**Threshold methodology.** Set to ~10× the empirical peak overload-delta
observed in healthy operation. See
[tier_c_baseline_methodology.md](tier_c_baseline_methodology.md) for the
measurement procedure. Default value is `0` (Tier C disabled) — operators enable
after measurement.

**Deviation from the original draft.** This section originally specified
separate `SecurityRateLimitClusterSignalsPerSec` / `…BytesPerSec` thresholds
backed by 60-bucket per-link rings. The shipped v1 uses a single
`SecurityRateLimitClusterOverloadsPerSec` for two reasons: (a) no per-NodeId
signal-count counter exists in TransporterRegistry — adding one would mean
modifying the receive hot path, against v1's no-hot-path-overhead mandate; (b)
`get_overload_count(NodeId)` is a *more meaningful* "this node is overwhelming
us" signal than raw bytes/sec, since it only ticks up when the receiver feels
buffer pressure. Bytes/sec and signals/sec metrics remain candidates for
follow-up if operational evidence shows they catch attacks overload-count
misses.

### 8.5 Sliding-window mechanics

```
window = 5 minutes
bucket_granularity = 30 seconds
num_buckets = 10

On report for (nodeId, violation_type):
  current_bucket = (now / 30s) % 10
  if buckets[current_bucket].window_start != current_bucket_start:
    buckets[current_bucket].count = 0
    buckets[current_bucket].window_start = current_bucket_start
  buckets[current_bucket].count += 1

window_count = sum(buckets where window_start within last 5 min)
```

For Tier A: no threshold (immediate action). Window is forensic.
For Tier B: no threshold. Window drives ndbinfo's `current_window_count` field.
For Tier C cluster-side safety net: window is checked against the rate threshold.

Old buckets age out naturally as their `window_start` falls outside the window — addressing the concern that long-running nodes shouldn't accumulate noise that misrepresents current activity.

### 8.6 Persistence across reconnect

**Decision:** Counters persist across disconnect/reconnect using the sliding window. A reconnecting node does not get a clean slate — strikes within the window still count. Strikes age out by time, not by disconnect events.

Implementation: keyed by NodeId, retained in QMGR memory until cluster restart. No persistence to disk in v1 (cluster restart resets counters — acceptable, see Section 14).

### 8.7 RONDIS counter system (separate)

RONDIS implements its own counter inside the RONDIS process, scoped per Redis connection. Design mirrors the QMGR model but with finer granularity:

- Cumulative + sliding-window counters per Redis connection.
- Tier-B-equivalent events (oversize SET, bad SELECT): log + observability counter increment, no automated disconnect of the Redis connection.
- RONDIS-internal Tier C (per-Redis-client rate limits): may close an individual Redis connection if rate is unreasonable — this is *upstream* enforcement in the Tier C sense, and only the one Redis client is affected.
- RONDIS emits its log entries in the same `SECURITY_EVENT:` format as the kernel side (Section 11.2) so operators see a unified stream.

### 8.8 Master kill switch

A single boolean config parameter, `EnableSecurityDisconnect` (default `true`). When set to `false`, the system enters **observation mode**:

- All Tier A disconnect paths and the Tier C cluster-side safety net are skipped.
- Counters still increment, log lines still emit, and ndbinfo is updated.
- Effect: every violation is detected, logged, and counted — nothing is disconnected.

Observation mode serves two distinct purposes:

1. **Safer rollout.** Operators can deploy with `EnableSecurityDisconnect=false`, watch real traffic against Tier A sites in the cluster log and ndbinfo, verify no false positives exist, then flip to enforcement. Without this mode there is no way to validate detection behavior before trusting it with live disconnects.

2. **Emergency disable.** If a future code change causes false-positive Tier A detection (e.g., a logic error categorizing legitimate signals as malicious), operators can disable the disconnect mechanism in seconds without redeploying the cluster.

**Observation mode is not zero-cost.** Logging and ndbinfo overhead continue exactly as in enforcement mode. Operators should not expect observation mode to restore pre-feature performance; it only removes the disconnect action.

Changing to observation mode is an action that should be alerted on by monitoring (Section 11.3). Once tripped, the parameter stays off until explicitly cleared by an operator.

**Dedicated audit log line.** Every state change of `EnableSecurityDisconnect` emits a structured `SECURITY_KILLSWITCH:` cluster log line — separate from the `SECURITY_EVENT:` stream — so monitoring can alert on this specifically without parsing generic config-change events:

```
SECURITY_KILLSWITCH: action=disabled by_node=10 timestamp=... operator=<mgm_connection_info>
SECURITY_KILLSWITCH: action=enabled  by_node=10 timestamp=... operator=<mgm_connection_info>
```

Both the disable and the re-enable are logged. Operators reviewing an incident later can correlate the disable timestamp with whatever they were investigating.

### 8.9 Numeric tunability vs hardcoded policy

The **policy** is hardcoded:

- Which violations belong in which tier (Section 7).
- The Tier A categorization rule (Section 6).
- The master action per tier (A → disconnect, B → log, C → safety net via A).

Operators cannot move a violation between tiers at runtime.

The **numeric parameters** are config-tunable via NDB config:

| Parameter | Default | Purpose | Change mechanism |
|---|---|---|---|
| `EnableSecurityDisconnect` | `true` | Master enforcement switch; `false` = observation mode — log everything, disconnect nothing (Section 8.8) | Live `ndb_mgm SET` |
| `SecurityReportSuppressionMs` | `100` | Report-rate limiting window (Section 8.3) | Live `ndb_mgm SET` |
| `SecurityWindowDurationSec` | `300` | Sliding window length (Section 8.5) | Restart required |
| `SecurityBucketGranularitySec` | `30` | Bucket size within window | Restart required |
| `SecurityRateLimitClusterOverloadsPerSec` | `0` (disabled) | Tier C threshold: per-API-node receive-overload events per second classed as volumetric attack. Set to ~10× empirical peak after baseline measurement (Section 8.4). | Live `ndb_mgm SET` |

Defaults are chosen for sensible behavior; operators tune for their workload, especially the safety-net thresholds.

**Implementation pattern.** Parameters live in the cluster `config.ini`. Runtime-settable parameters use the existing NDB mechanism documented at [set_config_param/](../set_config_param/) — operators issue `ndb_mgm> SET <NodeId> <ParamName> <Value>` and the change applies in ~1–2 seconds without restart. `SET` values are live-only; persistent changes require editing `config.ini`. Window-size and bucket-granularity parameters change the dimensions of allocated state (per-NodeId bucket arrays sized at startup), so they require a restart to take effect — these are expected to be tuned once based on initial observation, not iteratively.

**Emergency kill-switch playbook.** If the security feature is misbehaving (false-positive disconnects, log storm, etc.): `ndb_mgm> SET <node> EnableSecurityDisconnect 0` drops into observation mode cluster-wide within seconds — counters and logging continue uninterrupted, only the disconnect action stops. Re-enable with `SET ... 1` once the cause is understood and remediated. `SET` commands are logged in the cluster log for audit.

**Initial rollout playbook.** Deploy with `EnableSecurityDisconnect=false`. Monitor `SECURITY_EVENT:` log lines and ndbinfo for the first observation window. If no unexpected Tier A hits are seen against known-legitimate traffic, set `EnableSecurityDisconnect=true` to enable enforcement. The `SECURITY_KILLSWITCH:` log line records both transitions for audit.

---

## 9. Generalization of disconnect primitive

[`disconnectMaliciousNode()` at DbtcMain.cpp:2497](../../src/kernel/blocks/dbtc/DbtcMain.cpp) is currently DBTC-only and directly invokes disconnect. The new pattern is:

1. Move and rename the primitive to `reportMaliciousSignal()` in [`SimulatedBlock`](../../src/kernel/vm/SimulatedBlock.hpp) as a protected method.
2. The method sends `GSN_MALICIOUS_SIGNAL_REPORT` to QMGR. **It does not disconnect.** Whether disconnect happens is QMGR's decision based on the tier in the report plus the master kill switch state.
3. Migrate existing DBTC call sites to the new signature.
4. Document the pattern in [data_node_security/CLAUDE.md](CLAUDE.md) for future block hardening.

After this refactor, any block adds malicious-input handling by:

```cpp
if (unlikely(violation_detected)) {
  jam();
  REPORT_MALICIOUS_SIGNAL(signal, senderNodeId, VT_INVALID_FOO);
  return;
}
```

A thin macro captures `__LINE__` automatically:

```cpp
#define REPORT_MALICIOUS_SIGNAL(signal, nodeId, violationType) \
  reportMaliciousSignal(signal, nodeId, violationType, __LINE__)
```

The call site passes only the violation type. The **tier is derived** from the
violation type via the canonical `g_violation_info[]` table (ViolationType.hpp) —
a single source of truth, so a call site can never tag the wrong tier. The sender
includes the derived tier in the report, which also keeps rolling upgrades safe
(a newer sender's tier interpretation travels even if the receiving QMGR was built
without that violation type). `__LINE__` is a compile-time constant; the macro
adds zero runtime cost, following the existing NDB `jam()` macro pattern.

The block reports; QMGR decides; the existing disconnect path (`api_failed()` / `node_failed()`) executes if and only if tier and kill switch agree.

---

## 10. Framework Extensibility

The v1 framework should accommodate future audit findings without architectural change. Specifically:

- **Adding a new violation type:** add an enum value to `ViolationType` (before the `VT_UNKNOWN`/`NUM_VIOLATION_TYPES` sentinels) and a matching row to `g_violation_info[]` (tier + reason string) in `ViolationType.hpp`. A `static_assert` enforces the row count. No QMGR logic change.
- **Adding coverage for a new block:** the block includes `SimulatedBlock`, calls `REPORT_MALICIOUS_SIGNAL()`. No QMGR change.
- **Per-violation-type tier:** tier is a field in each `g_violation_info[]` row (the single source of truth), not a global constant. Different violations carry different tiers without any code change beyond that table.

This is why the v1 framework must ship **before** broader block audits — see Section 12.1.

A meta-requirement for ongoing maintenance: **whenever a new violation type is added to Tier A, the Tier A categorization rule (Section 6) must be explicitly verified.** A user-triggerable violation misclassified as Tier A reintroduces the laundering attack vector. This belongs in a code-review checklist; the implementation plan should formalize it.

---

## 11. Observability — load-bearing requirement

Because Tier B is log-only, the observability layer **is** the security feature for the majority of detections. The requirements below are not optional polish; they are required for v1.

### 11.1 ndbinfo virtual table

Expose counters through a new ndbinfo virtual table queryable via SQL:

```sql
SELECT * FROM ndbinfo.security_events;
```

Columns (one row per offending NodeId — the window is per-node, not per-type, per Section 8.1):

| Column | Type | Description |
|---|---|---|
| `node_id` | int | Offending NodeId |
| `node_type` | enum | API, DB, MGM |
| `total_tier_a` | bigint | Cumulative Tier A strikes since cluster start |
| `total_tier_b` | bigint | Cumulative Tier B strikes since cluster start |
| `total_disconnects` | bigint | Times this node has been disconnected (Tier A) |
| `current_window_count` | int | Strikes in the active 5-minute window (any tier) |
| `last_violation` | string | Reason string of most recent strike (resolved from enum) |
| `last_source_line` | int | `__LINE__` of most recent strike — forensic pointer |
| `last_strike_seconds_ago` | int | Age of most recent strike |

There is **no separate `security_summary` view** — with one row per offending node, `security_events` is already the summary. Per-type detail, when needed, comes from the `SECURITY_EVENT:` cluster log (Section 11.2), which carries the violation type on every line.

**Access control (grant-based).** ndbinfo views carry no built-in privilege enforcement in this codebase — they are open and governed by ordinary MySQL grants. `security_events` ships like every other ndbinfo view; operators **must restrict it via `REVOKE`/`GRANT`** to DBA/operator accounts, especially in multi-tenant deployments where it exposes per-NodeId activity across tenants (and gives a read-capable attacker a runtime confirmation channel — Section 3.4). True `PROCESS`-privilege gating in code is **follow-up work**, not v1: the ndbinfo framework does not provide it for free and adding it is a non-trivial SQL/plugin-layer change that exceeds v1's lightweight scope.

**Cluster aggregation.** Each data node's QMGR reports the violations *it* observed; the SQL view unions them and aggregates per offending node: `SUM` of `total_*` / `current_window_count`, `MAX` of `last_strike` recency, `GROUP BY node_id`. Implementation uses the same ndbinfo virtual-table dispatch pattern as `ndbinfo.processes`.

**Iteration efficiency.** No active-set bitmap is needed at this scale: the per-node state is a fixed array of ≤2040 small structs, and the scan handler simply skips entries with zero total strikes. An ndbinfo query is infrequent (operator action / scraper poll) and iterating a few thousand structs is trivial. (A bitmap remains a possible future optimization only if the node-id space or query rate grows dramatically.)

### 11.2 Structured cluster log format

All security events emit a cluster log line with a stable, monitoring-friendly prefix:

```
SECURITY_EVENT: tier=B node_id=5 node_type=API violation=keyinfo_signal_length_mismatch source_block=DBTC source_line=2990 window_count=3 total_count=14
```

The fixed `SECURITY_EVENT:` prefix allows external log monitoring (Loki, Splunk, ELK, etc.) to filter for these lines without false matches. The `key=value` format is machine-parseable.

All detection sites — kernel-side and RONDIS — emit lines in this format so operators see a unified stream regardless of source component.

### 11.3 Monitoring integration

The v1 documentation must include working examples for common monitoring stacks. Without these, "log-only" becomes "ignored," and the policy is theater.

Required examples:

- **Prometheus:** scrape `ndbinfo.security_events` via `mysqld_exporter`. Example metric names and scrape config included in the monitoring documentation.
- **Log aggregation:** ready-to-paste filter queries for Loki and Splunk targeting the `SECURITY_EVENT:` prefix.
- **Alerting recommendations:**
  - **First Tier A from a given node within a rolling window** (e.g., 15 minutes): page on-call immediately.
  - **Subsequent Tier A from the same node within the window:** group / suppress at the alerting layer. Naive page-per-event would silence the operator's pager — a buggy client library or a runaway state-machine violation can produce many Tier A events in succession.
  - **Sustained Tier A across multiple distinct nodes simultaneously:** escalate as a higher-priority alert (this pattern indicates either a coordinated attack or a widespread client bug, both of which warrant immediate attention).
  - Tier B: page when `current_window_count > threshold` per `(node_id, violation_type)`. Default threshold suggestions provided; operators tune for their environment.
  - Master kill switch state: page when `EnableSecurityDisconnect=false`. Use the dedicated `SECURITY_KILLSWITCH:` log line (Section 8.8) for direct alerting rather than parsing generic config-change events.

These artifacts are part of v1 scope (Section 14), not follow-up work.

---

## 12. Out-of-Scope for v1

| Item | Why deferred | When to revisit |
|---|---|---|
| **Multi-block audit of DBSPJ / DBLQH / DBDICT / SUMA** — explicit Phase 2 commitment, not indefinite deferral | Framework first (Section 12.1). Once framework exists, audit findings are drop-in additions to the catalog. Promoted to "explicit next phase" because we dropped categorical structural validation in favor of doing the audit work properly rather than paying ongoing runtime cost for partial coverage. | **Phase 2, immediately after v1 ships.** Sequenced as part of the same security initiative, not a separate future project. |
| Cluster-wide counter aggregation | A peer attacking data node A is invisible to B's QMGR. Cross-QMGR sync is a real distributed-systems problem. | When empirical data justifies the complexity. |
| Persistence to disk | Counter state lost on cluster restart. Restart launders strikes — unrealistic (restart is hugely visible). | If a customer reports the gap. |
| RDRS / REST server hardening | Different connection model (HTTP), different attack surface. Needs its own design doc. | Separate initiative. |
| Side-channel and timing attack mitigation | Different threat class entirely. | Out of scope. |
| **Kick-session feedback path** | Considered and **rejected**, not deferred. The path would let a data node send "this transaction's owning session should be killed" back to the API node, narrowing a disconnect to a specific session at the API-node layer. With our current policy this is unnecessary: Tier A criteria are user-untriggerable by design (Section 6), so when Tier A fires the *API node* is the problem and disconnecting it is correct — there is no innocent session to narrow to. Tier B is log-only, so there is nothing to narrow there either. Implementing the path would only be valuable if Tier B were ever made enforcing, which we have explicitly chosen against. | Only if the policy itself is revisited. Not expected. |
| NDB protocol session-tag extension | Would require carrying user identity in every signal. Massive protocol change, version negotiation, indefinite scope. | Reconsider only if the threat model shifts decisively (e.g., RonDB deployed in a non-segmented context where laundering becomes a real attack rather than a hypothetical). |
| **RONDIS-side categorical structural validation** | Parallel to the NDB transporter's per-GSN min-length / section-count check, but for the RESP protocol (command name length sanity, argument count, bulk string size). RONDIS catches its known violation classes already; the marginal universal-coverage value is lower at the RESP layer (simpler protocol, fewer "unknown unknowns" of that shape). | v2 candidate after v1 deployment, if observed RONDIS bug shapes justify it. |
| **Comprehensive GSN-to-sender-type allow-list** | The block-routing-bypass attack class (Section 7) is recognized but not comprehensively enforced. Building the allow-list requires per-signal audit work. | After v1 ships, alongside the broader DBSPJ / DBLQH / DBDICT audit. |

### 12.1 Why framework before audit?

The user asked whether broader audits of DBSPJ/DBLQH/DBDICT should happen *before* building the framework. The answer is no:

1. **Audit findings need somewhere to land.** Without the framework, each new "should disconnect" finding either inlines a copy of the disconnect logic (the pattern we are trying to escape) or accumulates as a backlog with nothing to do with it.
2. **The 23 existing DBTC sites are a sufficient sample.** The framework's tiers, persistence model, and signal flow are stable regardless of what additional sites future audits reveal. We have enough data.
3. **The framework's shape is what's hardest to change later.** Tier definitions, signal contracts, and QMGR's state model are foundational. Adding violation types is a one-line change; redesigning the foundation isn't.
4. **Iterative scope.** Build the framework as the contained, well-scoped v1 deliverable. Then conduct other-block audits as a follow-up project, each producing only a small diff per audited site.

The one risk: if a future audit reveals a violation pattern the three tiers truly cannot accommodate, the model needs extension. The extensibility design (Section 10) handles this — adding tiers or types is additive, not disruptive.

**Why audit instead of transporter-layer categorical validation?** An earlier draft proposed universal per-GSN structural validation at the transporter receive path as defense in depth. It was rejected because (a) RonDB's performance-first identity makes any per-signal cost on the hot path a serious tradeoff, (b) the class of bugs categorical validation catches is the same class that audit catches reliably during signal-handler review, and (c) audit work is one-time per block while categorical validation is permanent overhead. Audit also covers value-level bugs (out-of-bounds pointers, ownership violations, state-machine issues) that categorical validation never could.

---

## 13. Blind Spots and Accepted Risks

Risks we are explicitly accepting, with rationale.

| Risk | Rationale for acceptance |
|---|---|
| **Cluster-wide attack distribution.** Coordinated attackers can spread strikes across data nodes. | v1 accepts the gap. Mitigation requires cross-QMGR aggregation. Revisit when justified by empirical data. |
| **Cluster restart launders counters.** Counter state is in-memory only. | Cluster restart is itself hugely visible and disruptive; an attacker forcing one would be loud. Disk persistence is complex and probably not worth v1. |
| **Receive-thread DoS amplification.** Each bad signal could generate report traffic. | Mitigated by report-rate limiting (Section 8.3). Accept residual risk; monitor in practice. |
| **Heartbeat/overload signal masking.** Under severe volumetric attack, signals may drop from buffers before validation runs. | Tier C catches this. The three tiers complement each other. |
| **Unaudited blocks (DBSPJ / DBLQH / DBDICT).** If those blocks crash on bad inputs before reaching validation, the counter infrastructure alone doesn't help. | Counter infrastructure is necessary but not sufficient. The per-block audit is committed as the explicit Phase 2 immediately after v1 ships (Section 12). We chose audit work over a permanent runtime cost from transporter-layer categorical validation because audits provide deeper coverage (value-level bugs too, not just structural) at one-time cost. Until Phase 2 lands, unaudited blocks remain a real gap. |
| **Tier B log-only has no automatic enforcement.** A persistently-misbehaving API node will keep producing Tier B events forever without being disconnected. | Explicit policy choice. False-positive disconnect of multi-tenant API nodes is strictly worse than relying on operators to act on logs. The mitigation is operator-facing observability (Section 11) — if operators ignore the logs, the feature is ineffective. |
| **Operator alert fatigue.** If Tier B fires frequently from benign causes, real attacks become noise. | Mitigated by per-(NodeId, violation_type) grouping in the sliding window — operators see patterns, not individual events. Alerting thresholds in Section 11.3 are operator-tunable. |
| **Reconnect oscillation for Tier A.** A node that keeps reconnecting and immediately triggering a Tier A violation gets disconnected each time, consuming cluster resources. | Acceptable cost: the loop is bounded by reconnect delay and surfaces the issue to operators rapidly. Worst case = the offending node burns its own and slightly the cluster's resources until an operator intervenes (e.g., flips the master kill switch or removes the buggy node). |
| **Tier A categorization fragility.** If a future violation type is misclassified as Tier A but is user-triggerable, the laundering attack returns. | Mitigation: the explicit categorization rule (Section 6) plus a review checklist for adding new Tier A types (formalized in the implementation plan). |
| **Deployment dependency on network segmentation.** If a customer deployment is *not* segmented, the threat profile shifts and Tier B log-only may be insufficient on its own. | Section 3.3 documents the assumption. Non-segmented deployments should tighten upstream rate limits and treat Tier B accumulations as high-priority. The policy itself does not vary by deployment; operators tune. |
| **Bugs in our own code causing false-positive Tier A.** | Mitigated by the master kill switch (Section 8.8). Operators can disable disconnect in one config change without redeploying. |
| **Block-routing bypass coverage is partial.** A compromised or buggy API node sending internal-only signal types is recognized as Tier A (Section 7 "Block-routing bypass"), but comprehensive enforcement requires a GSN-to-sender-type allow-list that's post-v1 audit work. | The framework supports the attack class once violation types and call sites are added. v1 captures whatever sender-type checks already exist in DBTC during call-site migration; broader coverage tracked separately. |
| **Live observability is also a live attacker feedback channel.** An attacker with `PROCESS` access to ndbinfo can probe and confirm detection. | Accept as a cost of meaningful observability — operators need the live view to act. Access restricted to `PROCESS` privilege; multi-tenant deployments must not delegate this broadly. Documented in Section 3.4 and Section 11.1. |

---

## 14. v1 Implementation Scope

Concrete deliverables for v1. An implementation plan derived from this design doc will sequence these.

1. **Generalize disconnect primitive into `SimulatedBlock`** as `reportMaliciousSignal()`. Refactor existing DBTC call sites to use the new pattern. Block-side behavior change: no longer invokes disconnect directly; all decisions flow through QMGR.
2. **Define `GSN_MALICIOUS_SIGNAL_REPORT`.** Add the signal, its handler in QMGR, the violation-type enum, the reason-string lookup table.
3. **Implement `NodeSecurityState` in QMGR.** Allocation, sliding-window mechanics, persistence semantics per Section 8.6.
4. **Implement tier-specific QMGR handling.** Tier A → existing disconnect path (gated by master kill switch). Tier B → counter + structured log entry + ndbinfo update, no disconnect.
5. **Implement report-rate limiting in `SimulatedBlock::reportMaliciousSignal()`** per Section 8.3.
6. **Categorize existing call sites by tier per Section 7.** Replace direct disconnect with tier-tagged reports. Apply the data-node override rule.
7. **Implement Tier C cluster-side safety net in QMGR** (per-API-node overload-count delta sampled once per second from `globalTransporterRegistry`; per Section 8.4). On threshold breach, QMGR emits a Tier A `VT_RATE_LIMIT_EXCEEDED` report to itself (gated by master kill switch).
8. **Implement `EnableSecurityDisconnect` master kill switch** and the other numeric config parameters per Section 8.9.
9. **Implement the `ndbinfo.security_events` virtual table** (one row per offending node) per Section 11.1. The scan handler skips zero-strike entries; no active-set bitmap is needed at this scale.
10. **Implement structured `SECURITY_EVENT:` cluster log format** per Section 11.2. Applied consistently across all detection sites.
11. **Monitoring documentation and reference artifacts** per Section 11.3: example Prometheus scrape config, Loki/Splunk filter queries, and alerting-rule examples.
12. **RONDIS-side counter system.** Per-Redis-connection cumulative + sliding window for the two known violation types. Cluster-log emission in the same `SECURITY_EVENT:` format for unified monitoring. Independent of QMGR.
13. **Integration tests.** Extend [mysql-test/suite/rondis/](../../../../mysql-test/suite/rondis/) (or a new suite) with tests that:
    - Inject Tier A violations via NDB API or mock client → verify disconnect + log entry.
    - Inject Tier B violations → verify log entry + counter increment + ndbinfo update, *and verify no disconnect*.
    - Verify the master kill switch disables disconnect while preserving logging.
    - Verify `ndbinfo.security_events` returns expected counter state under load and requires `PROCESS` privilege.
    Without these, the policy is not verifiable.
14. **Baseline rate-data collection for Tier C threshold defaults.** Document the measurement methodology so the threshold is not a guess; deliver alongside the threshold defaults.
15. **Counter reset capability.** Admin command (via `ndb_mgm` DUMP-style or `SET`) to reset all counters for a specified offending NodeId, or globally. Useful after investigation/remediation to clear stale state without cluster restart. The reset itself is logged in `SECURITY_EVENT:` format (with a dedicated violation type like `COUNTER_RESET`) for audit.
16. **Dedicated `SECURITY_KILLSWITCH:` log format and emission** per Section 8.8 — separate from `SECURITY_EVENT:` so monitoring can alert on kill-switch state changes specifically.
17. **Rolling-upgrade safety for `violation_type` enum** per Section 8.2 — QMGR's handler validates `violation_type` against the receiver's known range and falls back to `UNKNOWN_VIOLATION_TYPE` for out-of-range values.
18. **ndbinfo access control (grant-based).** `ndbinfo.security_events` ships open like other ndbinfo views; document operator `REVOKE`/`GRANT` restriction. True `PROCESS`-privilege gating in code is follow-up, not v1 (Section 11.1).
19. **Block-routing-bypass scaffolding.** Add `ViolationType::WRONG_SENDER_TYPE_FOR_GSN` to the violation-type enum. Migrate existing DBTC sender-type checks (including the "data node sender → Tier A" override) into the framework with this tag. Comprehensive GSN allow-list is Phase 2 audit work (Section 12).

**Not in v1:**

- Audit of any block outside DBTC (deferred per Section 12.1).
- Disk persistence of counters.
- Cross-cluster counter aggregation.
- RDRS hardening (separate initiative).
- Kick-session feedback path (rejected per Section 12).
- Configurable tier assignments (hardcoded by design).

---

---

## 15. Testing Strategy

The policy is only as good as our ability to verify it behaves as designed. Eight test categories, in roughly the order they should be developed.

### 15.1 Unit tests

Lowest cost, highest correctness coverage.

- Sliding-window arithmetic: bucket rotation across the window-edge, expiry of buckets older than the window, correct `current_window_count` sum.
- Tier dispatch in QMGR: every combination of `(tier, EnableSecurityDisconnect)` produces the correct action.
- Report-rate limiting: suppression timer correctness, batch-credit of suppressed counts via `theData[6]`.
- Counter operations: increment, reset, rolling-upgrade fallback to `UNKNOWN_VIOLATION_TYPE`.

### 15.2 Integration tests via mysql-test

Required v1 deliverable (Section 14 item 13). Concrete cases:

- Inject Tier A violation via NDB API or mock client → verify `api_failed()` / `node_failed()` invoked, structured log line emitted, ndbinfo counter incremented.
- Inject Tier B violation → verify counter increment, log line, ndbinfo update, **and explicit verification that no disconnect occurred**.
- Master kill switch flip → verify disconnect path skipped while logging continues.
- `ndbinfo.security_events` access without `PROCESS` privilege → verify denied; with `PROCESS` → verify expected rows.

### 15.3 End-to-end attack simulation

The most important set for validating the **policy**, not just the code. These verify that the design decisions actually deliver the intended security properties.

- **Punishment-laundering test.** Drive sustained Tier B violations from one SQL session through mysqld. Verify other SQL sessions on that mysqld stay connected; verify mysqld is not disconnected from the cluster. This is the test that the most-discussed policy decision actually holds.
- **Tier-B-as-cover test.** Generate Tier B volume from one NodeId up to but not beyond the Tier C threshold. Verify Tier B logs but no disconnect. Cross the Tier C threshold. Verify the safety-net path fires and disconnect occurs.
- **Reconnect-dodging test.** Trigger violations, force node disconnect, reconnect with the same NodeId within the sliding window. Verify counters persist (strikes still count) per Section 8.6.
- **Block-routing-bypass test** (where the framework path exists). Send an internal-only signal type from an API NodeId; verify detection. For v1, may test with a synthetic violation type until the full allow-list lands.

### 15.4 Adversarial fuzzing

Catches what we didn't think to write tests for.

- Targeted fuzzing of audited signal handlers (DBTC `TCKEYREQ` / `KEYINFO` / `ATTRINFO` families) with malformed signals.
- Goal: no crashes, no undefined behavior. Every malformed input either rejects cleanly or triggers `reportMaliciousSignal`.
- Run as a regular CI job, not one-off.

### 15.5 Performance regression tests

RonDB's positioning is performance-first; these tests are not optional and the budgets below are hard constraints, not aspirations.

**Hot-path budgets (every signal pays):**
- The framework deliberately adds no per-signal work in the receive path — categorical structural validation was considered and rejected for this reason (Section 6). Per-signal overhead from the security feature should measure as **statistical noise** in representative-throughput benchmarks (1M+ signals/sec/receive-thread). If any per-signal cost appears, investigate before merging — it likely indicates an implementation error rather than a design cost.

**Warm-path budgets (per detection event):**
- Single `reportMaliciousSignal()` end-to-end latency (block detection → QMGR handler → log emission → ndbinfo update): no specific budget, but should not show up in flame graphs under normal load.
- Counter-increment overhead under sustained report traffic at the report-rate-limit ceiling: no measurable impact on unrelated QMGR work.

**Memory budgets (steady-state):**
- QMGR `NodeSecurityState` array: fixed ~260 KB (≈128 bytes × `MAX_NODES_ID`), allocated once in the singleton, regardless of activity.
- Per-block-instance report-rate-limiting state: <100 KB per block instance under normal load.

**Throughput budgets under attack load:**
- ndbinfo scrape latency: <100 ms even with thousands of offending NodeIds (linear scan of a fixed array, skipping zero-strike entries).
- Cluster log write throughput: must sustain Tier B event volume up to the report-rate-limit ceiling × MAX_NODES without dropping unrelated log entries.

**Regression detection:** these benchmarks run in CI on every change to the security-feature code path. Any change that exceeds the budget triggers automatic review before merge.

### 15.6 Rolling-upgrade compatibility

NDB does rolling upgrades; this matters.

- Mixed-version cluster: new and old data nodes coexisting under load.
- New-version node emits an `UNKNOWN_VIOLATION_TYPE` (or higher) to an old QMGR → must not crash; old QMGR routes to the fallback bucket and emits a log line.
- ndbinfo views work correctly during the upgrade transition.

### 15.7 Operator workflow tests

- `ndb_mgm> SET <node> EnableSecurityDisconnect 0` → effect propagates within seconds. Counters and logging continue uninterrupted. Verify `SECURITY_KILLSWITCH:` log line is emitted.
- `ndb_mgm> SET <node> EnableSecurityDisconnect 1` → re-enables. Verify second `SECURITY_KILLSWITCH:` log line.
- Counter reset command → ndbinfo reflects cleared state for the specified NodeId. Audit log line emitted.

### 15.8 Negative / failure-mode tests

Defense against our own bugs and against degraded-cluster conditions.

- QMGR overloaded with `MALICIOUS_SIGNAL_REPORT` traffic → verify report-rate limiting actually limits; no signal-buffer exhaustion; no degradation of unrelated QMGR work.
- Cluster log unavailable or log writes blocked → counters still increment (log degradation acceptable; security state must not degrade).

### 15.9 What "done" looks like

A v1 ship-readiness checklist driven from this strategy:

- All categories 1, 2, 3 implemented and passing in CI.
- Category 4 (fuzzing) running on a recurring schedule with no open crash findings.
- Categories 5, 6, 7, 8 have at least minimal coverage; gaps documented.
- The three end-to-end attack-simulation tests (15.3) explicitly pass — these are the contractual guarantees of the design.

---

## 16. Document Status

**Key design commitments:**

- **Policy model:** Tier A (immediate disconnect, user-untriggerable by design) / Tier B (log-only forensic observability) / Tier C (upstream rate limits with coarse cluster-side safety net).
- **Tier A categorization rule:** must be impossible to trigger via valid user inputs at any multi-tenant API node. Reviewed explicitly whenever a new Tier A type is added.
- **Hardcoded policy assignments, tunable numeric thresholds.** Operators cannot move violations between tiers; they can tune numeric parameters via NDB config.
- **Master kill switch (`EnableSecurityDisconnect`) with observation mode.** Setting to `false` drops to log-everything/disconnect-nothing without redeploying. Observation mode is the recommended initial rollout posture and the emergency disable mechanism. Logging and ndbinfo overhead continue in observation mode; only the disconnect action is skipped.
- **Observability is load-bearing.** Section 11 deliverables (ndbinfo views, structured log format, monitoring integration artifacts) are required v1 scope, not optional.
- **Sliding window is per-node** (one 10×30 s ring, ~80 B/node), kept purely as forensic display for the ndbinfo `current_window_count` column; never an automatic disconnect trigger. Per-type granularity lives in cumulative counters and the cluster log, avoiding a per-type memory multiplier that would grow with the violation catalog.
- **No new per-signal hot-path overhead** from the security feature beyond a trivial Tier C bucket counter increment alongside existing transporter housekeeping. Categorical transporter-layer structural validation was considered and rejected as inconsistent with RonDB's performance-first identity; the same coverage is provided by prioritized per-block audit in Phase 2 instead.
- **Phase 2 (multi-block audit) is committed**, not deferred. The DBSPJ / DBLQH / DBDICT / SUMA audit is the explicit next phase after v1 ships, sequenced as part of the same security initiative.
- **Per-node security state is a fixed array** (~260 KB, allocated once in the QMGR singleton). Because it is compact and never dynamically allocated, there is no allocation-failure path and no `SECURITY_DEGRADED` machinery — and no active-set bitmap is needed, since the infrequent ndbinfo scan simply skips zero-strike entries.

**Next step:** derive an implementation plan with concrete task breakdown, owner assignments, and estimates.
