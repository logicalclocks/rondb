# Tiered Malicious Input Response Policy

**Status:** v2, FINAL — shipped
**Author:** security-intern initiative
**Related:** [tckeyreq_security.md](tckeyreq_security.md), [fragmented_signal_security.md](fragmented_signal_security.md)

---

## 1. Overview

RonDB data nodes disconnect offending nodes on malformed signals rather than crashing. This design extends that model to a graduated, two-tier response:

- **Tier A** — Active malice or fundamental compromise of the sender: disconnect immediately.
- **Tier B** — Violations that *could* indicate a buggy client or an attacker: log for forensics, no automated disconnect; humans review.

The framework is **detection-agnostic**: any kernel block reports a violation to QMGR, which holds the counter array, applies tier-specific handling, and triggers disconnect only for Tier A.

The model prioritizes customer experience over paranoia, consistent with the deployment threat model in Section 3.

---

## 2. Goals

| Priority | Goal |
|---|---|
| Primary | **Defense.** Prevent crashes, OOM, memory corruption, transaction hijacking, and resource exhaustion from peer-originated signals. |
| Primary | **No false-positive disconnects of multi-tenant API nodes.** No legitimate user should lose their connection because of another user's behavior (the "punishment laundering" concern, Section 3.2). |
| Secondary | **Observability.** Surface per-violation-type counts and structured cluster log lines so operators can distinguish a buggy client from an active attack. Operator-facing monitoring is load-bearing, not optional polish. |
| Non-goal | **Per-request authentication.** The cluster's existing trust model (NodeId established at cluster join) is preserved. |
| Non-goal | **TLS / transport encryption.** Orthogonal — handled by existing transporter TLS configuration. |

---

## 3. Threat Model

### 3.1 Actors

The threat actor has already passed cluster authentication and is now sending arbitrary signals:

- **Compromised host** running an API node.
- **Malicious user** driving a legitimate API node (crafted SQL through mysqld, or direct NDB API).
- **Compromised data node.** Catastrophic — full cluster trust. Treated as zero-tolerance (any violation → Tier A override).
- **Buggy client library** sending malformed signals unintentionally. Distinguished from attackers by logged-not-disconnected response for ambiguous cases.
- **Version-skew client** sending signals the current server doesn't accept.

**Out of model:** network-layer attackers between nodes (mitigated by TLS); side-channel attacks; authentication-layer attacks on cluster join.

### 3.2 Punishment laundering — recognized threat class

Multi-tenant API nodes (mysqld, RDRS) multiplex many users through a single NodeId. If our policy disconnected an API node for any user-attributable bad behavior, an attacker could deliberately trigger violations to weaponize our defense as a denial-of-service. We call this *punishment laundering*.

The NDB signal protocol carries no user-level identity. We cannot disconnect "just the offending user" — fixing that requires protocol extensions out of scope here.

Defense against laundering is structural:

1. **Tier A criteria are defined to be impossible to trigger via valid user inputs** (Section 6). When a Tier A fires, the API node itself is the problem — disconnecting it is correct.
2. **Tier B violations are log-only.** No automated disconnect. The laundering attack has no payload.

### 3.3 Deployment assumptions

RonDB deployments typically run on network-segmented infrastructure with API nodes accessible only from trusted application tiers. The dominant threat is internal (vulnerable internal service, insider, or compromised application user). False-positive disconnects are correspondingly more harmful than missed alerts, because real attacks are rare and customer impact is the primary cost driver.

This tilts the policy toward log-and-investigate (Tier B) over automated disconnect for ambiguous cases. If a deployment cannot rely on segmentation, operators should treat Tier B accumulations as higher-priority signals and tighten connection-level controls at the API node.

### 3.4 Open-source attack-model considerations

The RonDB source is public; attackers can read every violation check and tier definition. The defenses work regardless of attacker knowledge.

An attacker reading the source can see which violations are Tier B (log-only, non-disconnecting) and may attempt to use them as cover, timing probes, or log-flooding attacks. **There is no in-kernel rate limiter on Tier B log emission** — this is a known, accepted risk (Section 13). Operators bound inflow via connection-level controls and monitor `ndbinfo.security_violation_counts` for persistent elevation.

The `security_violation_counts` ndbinfo view requires the `PROCESS` privilege — restricting it to DBA/operator accounts prevents a read-capable attacker from getting live confirmation of detection. The same information lives in the cluster log; operators must lock down log access correspondingly in multi-tenant deployments.

---

## 4. Background: What "Malicious" Means

A code audit of [DbtcMain.cpp](../../src/kernel/blocks/dbtc/DbtcMain.cpp) identifies **23 call sites** of `reportMaliciousSignal()` plus 2 in RONDIS. These cluster into five categories:

| Category | Count | Single-signal kill? | Honest client could trigger? |
|---|---|---|---|
| Out-of-bounds index/pointer (`apiConnectPtr`, `tableId`) | 6 | Yes | No |
| Size/length DoS | 7 | Mixed | Sometimes (oversize values, oversize sections) |
| Semantic protocol constraints | 5 | Yes | Yes (state machine bugs, version skew) |
| Fragment-assembly state corruption | 4 | No | No |
| Transaction hijacking | 4 | Yes | No |

**Key observation:** 19 of 23 violations are caught and rejected on the first signal. Fragment-assembly attacks are already mitigated in `assembleFragments` Phase 1 hardening (commit `fa1713b4709`).

---

## 5. Architectural Findings

### 5.1 Why HostRecord is the wrong home for counters

[`HostRecord` in DBTC](../../src/kernel/blocks/dbtc/Dbtc.hpp) was rejected for three reasons:

1. **NDBMT fragmentation.** In multi-threaded ndbmtd, DBTC runs as multiple instances. Each has its own `hostRecord[]`; per-node counters fragment across TC threads.
2. **Cross-block detection.** Tier A violations can be detected in any block. A counter in DBTC requires awkward cross-block signaling to update.
3. **Scope mismatch.** HostRecord holds transactional state; security policy belongs alongside node-management state.

### 5.2 Why QMGR is the right home

[QMGR](../../src/kernel/blocks/qmgr/) is:

- **Singleton per data node** — no fragmentation.
- **Already in the disconnect path.** The existing `disconnectMaliciousNode()` already sends `DUMP_STATE_ORD 900/939` to QMGR.
- **Already responsible for node lifecycle** (heartbeat, failure detection, cluster membership).

---

## 6. The Two-Tier Response Policy

### Tier A — Immediate disconnect

**What it is:** violations that no protocol-compliant implementation could produce from valid user inputs. Active malice or fundamental compromise of the sender.

**Action:** disconnect the offending node via the existing QMGR path (`api_failed()` for API nodes, `node_failed()` for data nodes). No grace, no threshold. Subject to the master kill switch (Section 8.3).

**Categorization rule (HARD REQUIREMENT):** A violation is Tier A *only if* it is **impossible to trigger via valid SQL, HTTP, REST, or Redis user inputs** at any multi-tenant API node. Misclassifying a user-triggerable violation as Tier A reintroduces the laundering attack vector.

**Examples** (verified user-untriggerable):
- Signal with zero sections when sections required
- `sectionNo >= 3` in fragmented signal
- Signal length shorter than declared static header
- `apiConnectPtr` past pool end (server-issued handle)
- `apiConnectPtr` not owned by sender (hijacking)
- Any violation from a data node (zero-tolerance — see override rule)
- Start flag during `CS_ABORTING` (state machine attack pattern)

### Tier B — Log-only forensic observability

**What it is:** semantic violations that *could* indicate a buggy client, version skew, or an attacker — and that *could* be reachable from valid user inputs at a multi-tenant API node.

**Action:** increment `m_violationCounts[vtype]`, emit a structured `SECURITY_EVENT:` cluster log entry, update ndbinfo. **No automated disconnect.**

**Rationale:**
- The safety property (no crash, no OOM, no corruption) is provided by the input validation code that already rejects the offending operation.
- Tier B violations are reachable from valid user inputs at multi-tenant API nodes. Automated disconnect creates the laundering attack vector.
- Network segmentation makes real attacks rare; false-positive customer impact dominates the cost calculus.

**Examples:**
- Oversize KeyInfo / AttrInfo section
- Key length exceeds `MAX_KEY_SIZE_IN_WORDS`
- `CommitFlag` without `ExecFlag`
- `UNLOCK` without distribution key
- Reorg flag with invalid operation type
- Signal length mismatch in KEYINFO / ATTRINFO / TCKEYREQ
- RONDIS oversize SET value
- RONDIS SELECT db index out of range

### Override rule

Any Tier B violation from a data node sender (NodeId is type `NODE_TYPE_DB`) is escalated to Tier A. Data nodes are cluster peers running identical code; they have no plausible "honest mistake" mode. This is enforced in QMGR — call sites do not need to implement it.

### Why Tier B has no automatic disconnect

| Question | Answer |
|---|---|
| What does input validation already cover? | The actual safety property: no crash, no OOM, no corruption. Operation rejected. |
| What does Tier B disconnect *cost*? | False-positive disconnects of multi-tenant API nodes (laundering attack). |
| What does disconnect *uniquely add*? | Operator attention vs. log lines — achievable via `SECURITY_EVENT:` without disconnect. |

---

## 7. Tier Assignment Catalog

See [audit_implementation_guide.md](audit_implementation_guide.md) for the full table of all 25 current sites and the decision process for categorizing new ones.

---

## 8. Counter Infrastructure Design (v2 Lean Model)

### 8.1 QMGR counter state

QMGR holds a single fixed array — the complete v2 security state:

```cpp
Uint64  m_violationCounts[NUM_VIOLATION_TYPES];   // ~240 bytes
bool    m_enableSecurityDisconnect;               // master kill switch
```

`m_violationCounts[vtype]` is the lifetime cumulative count for that violation type, across all senders and all time since cluster start. It is a single cluster-wide counter per type, not per offending node. Zero-initialized at startup; no allocation-failure path.

**Why lean?** An earlier design held `NodeSecurityState[MAX_NODES_ID+1]` (~260 KB) with per-node history, sliding window, `last_source_line`, and Tier C rate buckets. This was stripped after three realizations:

1. External tooling (Prometheus/Grafana) provides time-series and alerting; the data node should store raw counts and do no in-kernel interpretation.
2. The sliding window was never a disconnect trigger (the policy only disconnects on first strike for Tier A; Tier B never fires automatically); it existed purely for a `current_window_count` ndbinfo column.
3. Per-node state grows linearly with the node-id space and multiplicatively with the violation catalog — the wrong trajectory for ongoing security work.

**Per-offender attribution.** The offending node_id is in the `SECURITY_EVENT:` cluster log line. The counter array intentionally carries no offender attribution — this is the fundamental tension documented in Section 13 (Tier B flood risk).

### 8.2 The MALICIOUS_SIGNAL_REPORT signal

```
GSN_MALICIOUS_SIGNAL_REPORT (sent JBB to QMGR_REF):
  theData[0] = offendingNodeId
  theData[1] = violationType   (ViolationType enum; QMGR derives tier via violation_tier())
  SignalLength = 2
```

`QMGR_REF = numberToRef(QMGR, 0)` is local-node — this signal never crosses the network wire. The signal is sent on JBB (job-buffer normal priority), not JBA (high priority); the disconnect path runs asynchronously.

QMGR's handler (`execMALICIOUS_SIGNAL_REPORT`):
1. Validate `violationType < NUM_VIOLATION_TYPES`; route out-of-range to `VT_UNKNOWN` bucket.
2. Recompute `tier = violation_tier(vtype)`.
3. If sender is `NODE_TYPE_DB` and tier is B: escalate to A.
4. `m_violationCounts[vtype]++`.
5. Emit `SECURITY_EVENT:` cluster log line via `g_eventLogger`.
6. If `tier == A && m_enableSecurityDisconnect`: invoke `securityDisconnectNode()`.

**Rolling-upgrade safety.** Out-of-range `violationType` values (from a newer sender) route to `VT_UNKNOWN` and emit a log line. Older receivers do not crash. The `violation_id` integer (which is published as `ndbinfo.security_violations.violation_id`) is a stable external contract; renumbering existing values would silently corrupt historical metrics.

### 8.3 Master kill switch (observation mode)

A single config parameter, `EnableSecurityDisconnect` (default `true`). When `false`, the system is in **observation mode**:

- All Tier A disconnect paths are skipped.
- Counters still increment, log lines still emit, ndbinfo is updated.

In debug builds, toggled via `ndb_mgm -e "ALL DUMP 9101 0"` (observation) / `DUMP 9101 1` (enforcement). In production, set in `config.ini`.

Two uses:
1. **Safer rollout.** Deploy with `EnableSecurityDisconnect=false`, watch real traffic, verify no false positives, then flip to enforcement.
2. **Emergency disable.** If a future code change causes false-positive Tier A detection, operators can disable the disconnect mechanism in seconds without redeploying.

**Observation mode is not zero-cost.** Logging and ndbinfo overhead continue exactly as in enforcement mode.

### 8.4 DUMP injector

In debug builds, `DUMP 9100 <offendingNodeId> <violationType>` synthesizes a `GSN_MALICIOUS_SIGNAL_REPORT` from `ndb_mgm`, exercising the QMGR handler without a real malicious client. Used by `mysql-test/suite/ndb/t/ndb_security*.test`.

`DUMP 9101 0/1` toggles `m_enableSecurityDisconnect` at runtime (debug builds only).

---

## 9. Generalization of Disconnect Primitive

`disconnectMaliciousNode()` was DBTC-only and directly invoked disconnect. It was replaced by:

```cpp
// Protected method on SimulatedBlock (SimulatedBlock.hpp):
void reportMaliciousSignal(Signal* signal,
                           NodeId offendingNodeId,
                           Uint32 violationType);
```

The method sends `GSN_MALICIOUS_SIGNAL_REPORT` to QMGR. **It does not disconnect.** Whether disconnect happens is QMGR's decision based on tier + kill switch state.

The `REPORT_MALICIOUS_SIGNAL` macro (which auto-captured `__LINE__`) was removed in v2 when `sourceLine` was dropped from the signal. Call sites now call `reportMaliciousSignal()` directly. `sourceLine` was found to carry little diagnostic value (the violation type + reason string already pinpoints the check; `__LINE__` is only useful to navigate to the exact line, which grepping for the violation type in the source is equally effective at).

Any block adds malicious-input handling by:

```cpp
if (unlikely(violation_detected)) {
  jam();
  reportMaliciousSignal(signal, offendingNodeId, VT_YOUR_VIOLATION);
  return;
}
```

The call site passes only the violation type. The **tier is derived** from `g_violation_info[]` (ViolationType.hpp) — a single source of truth, so a call site can never tag the wrong tier.

---

## 10. Framework Extensibility

- **Adding a new violation type:** add an enum value (before `VT_UNKNOWN`/`NUM_VIOLATION_TYPES`) and a matching row to `g_violation_info[]`. A `static_assert` enforces the row count. No QMGR logic change.
- **Adding coverage for a new block:** the block calls `reportMaliciousSignal()`. No QMGR change.
- **Per-violation-type tier:** tier is a field in each `g_violation_info[]` row — a single source of truth.

**Meta-requirement:** whenever a new Tier A violation type is added, the Tier A categorization rule (Section 6) must be explicitly verified in the code review. A user-triggerable violation misclassified as Tier A reintroduces the laundering attack vector.

---

## 11. Observability — load-bearing requirement

Because Tier B is log-only, the observability layer **is** the security feature for the majority of detections.

### 11.1 ndbinfo virtual tables

Two tables exposed via SQL:

**`ndbinfo.security_violations`** (public — no PROCESS privilege required):

```sql
SELECT * FROM ndbinfo.security_violations;
-- violation_id | tier | reason
-- 1            | A    | signal_in_unexpected_state
-- 5            | B    | keyinfo_signal_length_mismatch
-- ...
```

Static catalog — one row per known violation type, emitted by every data node from `g_violation_info[]`. `DISTINCT` deduplicated in the SQL view. Use to look up violation_ids or enumerate known types.

**`ndbinfo.security_violation_counts`** (requires `PROCESS` privilege):

```sql
SELECT * FROM ndbinfo.security_violation_counts;
-- reporting_node_id | violation_id | tier | reason                          | total_count
-- 1                 | 5            | B    | keyinfo_signal_length_mismatch  | 42
-- 2                 | 5            | B    | keyinfo_signal_length_mismatch  | 17
```

One row per (reporting_node_id, violation_id) with non-zero count. Aggregate with `SUM(total_count) GROUP BY violation_id` for cluster-wide totals.

**Access control.** `security_violations` is open like other ndbinfo views. `security_violation_counts` is gated by `PROCESS` privilege. In multi-tenant deployments, restrict both to DBA/operator accounts — the counts view gives a read-capable attacker live confirmation of detection.

### 11.2 Structured cluster log format

```
SECURITY_EVENT: tier=B node_id=5 node_type=API violation=keyinfo_signal_length_mismatch
```

RONDIS (stdout, not cluster log) appends client attribution:

```
SECURITY_EVENT: tier=B node_id=0 node_type=API violation=rondis_oversize_value client=10.0.0.1:49221 worker=3
```

The fixed `SECURITY_EVENT:` prefix allows external log monitoring to filter without false matches. All detection sites — kernel and RONDIS — emit lines in this format, so operators see a unified stream.

`node_id` is the offending NodeId from the kernel side; `0` from RONDIS (below NDB NodeId granularity). `node_type` is rendered as `DB`, `API`, or `MGM`.

### 11.3 Monitoring integration

Working examples for common monitoring stacks are in [monitoring.md](monitoring.md):

- **Prometheus:** `mysqld_exporter` custom query against `ndbinfo.security_violation_counts`.
- **Log aggregation:** Loki and Splunk filter queries for the `SECURITY_EVENT:` prefix.
- **Alerting:** page on first Tier A from any node; notify on sustained Tier B rate from one node; page when `EnableSecurityDisconnect=false`.

---

## 12. Out-of-Scope for v2

| Item | Why deferred | When to revisit |
|---|---|---|
| **Multi-block audit (DBSPJ / DBLQH / DBDICT / SUMA)** | Framework-first. Findings are drop-in additions to the catalog. | **Phase 2, immediately after v2 ships.** Committed, not open-ended. |
| Cluster-wide counter aggregation | Cross-QMGR sync is a real distributed-systems problem. | When empirical data justifies the complexity. |
| Persistence to disk | Counter state lost on cluster restart. Restart is hugely visible; restart launders strikes unrealistically. | If a customer reports the gap. |
| RDRS / REST server hardening | Different connection model (HTTP), different attack surface. | Separate initiative. |
| **Kick-session feedback path** | **Rejected, not deferred.** Would let a data node tell an API node to kill a specific session. Unnecessary: Tier A criteria are user-untriggerable (API node is the problem, disconnect is correct); Tier B is log-only (nothing to narrow). | Only if policy is revisited. Not expected. |
| NDB protocol session-tag extension | Massive protocol change. | Only if threat model shifts to non-segmented deployments decisively. |
| **Comprehensive GSN-to-sender-type allow-list** | Block-routing-bypass attack class recognized but not comprehensively enforced. Building the list requires per-signal audit. | After v2 ships, alongside DBSPJ / DBLQH audit. |
| Tier C cluster-side volumetric safety net | Designed in v1; removed in v2 lean redesign. Would require per-node state reintroduction. | If empirical data shows volumetric attacks not caught by Tier A/B. Currently, Tier A self-limits (offender disconnected on first strike); Tier B is log-only with known flood risk. |

---

## 13. Blind Spots and Accepted Risks

| Risk | Rationale for acceptance |
|---|---|
| **Tier B log flood (audited, accepted 2026).** A connected, authenticated client can sustain high Tier B violation rates; each emits one unthrottled `SECURITY_EVENT:` line, churning the 6×1 MB rotating cluster log. There is NO in-kernel or upstream rate limiter. | Worst case is post-auth forensic-integrity loss, NOT crash or OOM (counter array is fixed size). Adding any throttle either reintroduces per-node state (against the lean goal) or discards offender attribution — the offending node_id lives ONLY in the log line, not in `m_violationCounts[]`. Tier A self-limits (disconnect on first strike). Operators bound inflow via connection-level controls; monitor `security_violation_counts` externally. Full rationale in code comment at `QmgrMain.cpp execMALICIOUS_SIGNAL_REPORT`. |
| **Cluster-wide attack distribution.** Coordinated attackers spread strikes across data nodes. | v2 accepts the gap. Mitigation requires cross-QMGR aggregation. Revisit when justified. |
| **Cluster restart launders counters.** Counter state is in-memory only. | Cluster restart is hugely visible and disruptive; an attacker forcing one would be loud. |
| **Unaudited blocks (DBSPJ / DBLQH / DBDICT).** If those blocks crash on bad inputs before reaching validation, counter infrastructure alone doesn't help. | Counter infrastructure is necessary but not sufficient. Per-block audit is committed as Phase 2 immediately after v2 ships. |
| **Tier B log-only has no automatic enforcement.** A persistently-misbehaving API node produces Tier B events forever. | Explicit policy choice against punishment laundering. The mitigation is operator-facing observability and connection-level controls. |
| **Reconnect oscillation for Tier A.** A node that keeps reconnecting and triggering Tier A gets disconnected each time. | Bounded by reconnect delay; surfaces the issue to operators rapidly. |
| **Tier A categorization fragility.** Future violation misclassified as Tier A but user-triggerable → laundering returns. | Mitigation: explicit categorization rule + code-review checklist for new Tier A types. |
| **Live observability is also a live attacker feedback channel.** Attacker with `PROCESS` access can probe and confirm detection. | Accepted cost of meaningful observability. Restrict `PROCESS` to DBA/operator accounts; lock down log access similarly. |
| **Bugs in our own code causing false-positive Tier A.** | Mitigated by the master kill switch. Operators can disable disconnect in one config change without redeploying. |

---

## 14. v2 Implementation Scope (Shipped)

Concrete deliverables for v2:

1. **`reportMaliciousSignal()` in `SimulatedBlock`.** Replaces `disconnectMaliciousNode()`. Sends `GSN_MALICIOUS_SIGNAL_REPORT`; does not disconnect.
2. **`GSN_MALICIOUS_SIGNAL_REPORT` (2-field signal).** `offendingNodeId` + `violationType`; tier derived in QMGR.
3. **Lean QMGR counter state.** `m_violationCounts[NUM_VIOLATION_TYPES]` + `m_enableSecurityDisconnect`. Replaces `NodeSecurityState[MAX_NODES_ID+1]` (~260 KB per-node array).
4. **Tier dispatch in QMGR.** Tier A + kill switch → `securityDisconnectNode()`. Tier B → counter increment + log + ndbinfo update, no disconnect.
5. **ViolationType catalog pruning.** Removed `VT_RATE_LIMIT_EXCEEDED` (Tier C, never implemented), `VT_WRONG_SENDER_TYPE_FOR_GSN` (scaffold, no call sites), `VT_COUNTER_RESET`. Lean catalog, `static_assert` enforces count.
6. **Migration of all 25 existing call sites.** All DBTC + RONDIS sites via `reportMaliciousSignal()` (kernel) or `RONDIS_SECURITY_EVENT` macro (RONDIS).
7. **Two ndbinfo tables.** `security_violations` (static catalog; replaces old `security_events`) and `security_violation_counts` (per-type counters).
8. **SQL views.** `ndbinfo.security_violations` (public) and `ndbinfo.security_violation_counts` (PROCESS-gated). Views in strict alphabetical order (assertion in `ha_ndbinfo_sql.cc`).
9. **Structured cluster log (4-field format).** `tier=`, `node_id=`, `node_type=`, `violation=`. `node_type` rendered as `DB`/`API`/`MGM`. No `source_block`, `source_line`, `window_count`, `total_count`.
10. **RONDIS log alignment.** `RONDIS_SECURITY_EVENT` macro in `common.h` emits the same 4-field prefix, plus `client=` and `worker=` attribution fields.
11. **DUMP 9100/9101 injector/kill-switch.** Debug-build test tools; used by MTR suite.
12. **Integration tests.** `ndb_security` (counter/catalog queries), `ndbinfo_security_events_priv` (PROCESS privilege gating), `ndb_security_enforce` (Tier A disconnect against connection-pool node), `rondis_security` (RONDIS Tier B paths).
13. **Documentation update.** This doc, monitoring.md, team_briefing.md, audit_implementation_guide.md all reflect v2.

**Not in v2:**
- Tier C cluster-side safety net (removed).
- Sliding window / per-node history (removed).
- `REPORT_MALICIOUS_SIGNAL` macro + `sourceLine` (removed).
- `SecurityRateLimitClusterOverloadsPerSec` config param (removed with Tier C).
- Audit of any block outside DBTC (deferred per §12).
- Disk persistence of counters.
- Cross-cluster counter aggregation.
- RDRS hardening (separate initiative).

---

## 15. Testing

### MTR integration tests

All tests live in `mysql-test/suite/ndb/`:

| Test | What it covers |
|---|---|
| `ndb_security` | Static catalog queries via `security_violations`; counter increments via DUMP 9100 injector; both observation mode and enforcement mode; PROCESS privilege gating on `security_violation_counts` |
| `ndbinfo_security_events_priv` | PROCESS privilege enforcement — query denied without it, succeeds with it |
| `ndb_security_enforce` | End-to-end Tier A enforcement: injects via DUMP 9100 against connection-pool node 1600, polls `ndbinfo.processes` to observe disconnect, waits for reconnect |
| `rondis_security` | RONDIS Tier B paths: oversize SET value, out-of-range SELECT; error messages deliberately omit exact limits |

### Observation mode

The master kill switch is tested in `ndb_security` (DUMP 9101 0/1). The key invariant: with `EnableSecurityDisconnect=false`, a Tier A violation increments counters and emits a log line but does not disconnect the node.

### Verification

```bash
./mtr --suite=ndb ndb_security ndbinfo_security_events_priv ndb_security_enforce
./mtr --suite=rondis rondis_security
./mtr --suite=ndb ndb_security ndbinfo_security_events_priv ndb_security_enforce --nowarnings
```

---

## 16. Document Status

**Key design commitments in v2:**

- **Policy model:** Tier A (immediate disconnect, user-untriggerable by design) / Tier B (log-only forensic observability). No Tier C.
- **Tier A categorization rule:** must be impossible to trigger via valid user inputs. Reviewed explicitly whenever a new Tier A type is added.
- **Lean counter design:** `Uint64 m_violationCounts[NUM_VIOLATION_TYPES]` (~240 bytes) + one bool. No per-node history, no sliding window, no allocation-failure path.
- **Two ndbinfo tables:** `security_violations` (static public catalog) + `security_violation_counts` (cumulative per-type, PROCESS-gated).
- **4-field cluster log format:** `tier=`, `node_id=`, `node_type=`, `violation=`. Node type as human-readable string. RONDIS appends `client=` and `worker=`.
- **Master kill switch (`EnableSecurityDisconnect`)** with observation mode. Observation mode is the recommended initial rollout posture.
- **Tier B flood: known, accepted risk.** No in-kernel throttle. Documented in this section (§13), in `QmgrMain.cpp`, and in `memory/project_tierb_flood_accepted_risk.md`. Operators must use connection-level controls and external monitoring.
- **Observability is load-bearing.** The two ndbinfo tables plus structured log format are required, not optional. `monitoring.md` provides working Prometheus/Loki/Splunk artifacts.
- **Phase 2 (multi-block audit) is committed**, not deferred. The DBSPJ / DBLQH / DBDICT / SUMA audit is the explicit next phase after v2 ships.
