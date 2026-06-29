# RonDB Data Node Security — Tiered Malicious Input Response Policy

**Status:** v2, FINAL — shipped
**Scope:** NDB data nodes (kernel blocks + RONDIS Redis server)

---

## Component Glossary

| Component | What it is |
|---|---|
| **QMGR** | Cluster membership block — singleton per data node (not replicated across threads). Owns heartbeat, failure detection, and node lifecycle. In v2, also owns all security counter state. |
| **DBTC** | Transaction Coordinator block — handles client API signals (TCKEYREQ, KEYINFO, ATTRINFO, SCAN_TABREQ, …). Main source of detection call sites (23 sites). |
| **SimulatedBlock** | Base class for all NDB kernel blocks. `reportMaliciousSignal()` lives here so any block can report a violation without depending on DBTC. |
| **RONDIS** | Redis-protocol server layered on top of NDB. Architecturally separate from the NDB transporter — Redis clients connect via TCP; RONDIS uses NDB API internally but is not itself an NDB kernel block. |
| **ndbinfo** | NDB information schema — a set of virtual tables queryable via SQL through any mysqld attached to the cluster. Backed by data node scan handlers. |
| **NDB signal** | The interprocess-communication primitive for NDB kernel blocks. Signals are typed (by GSN — Global Signal Number), have fixed headers, and carry up to 25 32-bit words of data plus optional sections. |
| **NodeId** | The cluster-level identity of a connected node (data node, API node, management node). Assigned at cluster join. NDB signal protocol carries NodeId; it carries no user-level identity (SQL session, HTTP client, Redis connection). |
| **JBB / JBA** | Job-buffer priority levels. JBA = high priority (ahead of normal work). JBB = normal priority. `GSN_MALICIOUS_SIGNAL_REPORT` uses JBB — security reporting is not time-critical and should not starve the data plane. |

---

## 1. Overview

RonDB data nodes previously crashed or behaved unpredictably on malformed NDB protocol signals. This work replaces that behavior with a graduated, two-tier structured response:

- **Tier A** — Active malice or fundamental compromise of the sender: disconnect immediately.
- **Tier B** — Violations that *could* indicate a buggy client or an attacker: log for forensics, no automated disconnect; humans review.

Any kernel block can report a violation by calling `reportMaliciousSignal()`. The signal goes to QMGR, which holds the per-violation-type counters, decides the action, and — for Tier A — invokes the existing disconnect path.

The model prioritizes customer experience over paranoia, consistent with the deployment threat model in Section 3.

---

## 2. Goals

| Priority | Goal |
|---|---|
| Primary | **Defense.** Prevent crashes, OOM, memory corruption, transaction hijacking, and resource exhaustion from peer-originated signals. |
| Primary | **No false-positive disconnects of multi-tenant API nodes.** No legitimate user should lose their connection because of another user's behavior ("punishment laundering" — Section 3.2). |
| Secondary | **Observability.** Surface per-violation-type counts and structured cluster log lines so operators can distinguish a buggy client from an active attack. Monitoring is load-bearing, not optional polish. |
| Non-goal | **Per-request authentication.** The cluster's existing trust model (NodeId established at cluster join) is preserved. |
| Non-goal | **TLS / transport encryption.** Orthogonal — handled by existing transporter TLS. |

---

## 3. Threat Model

### 3.1 Actors

The threat actor has already passed cluster authentication and is now sending arbitrary signals:

- **Compromised host** running an API node.
- **Malicious user** driving a legitimate API node (crafted SQL through mysqld, or direct NDB API calls).
- **Compromised data node.** Catastrophic — full cluster trust. Any violation from a data node is zero-tolerance (Tier A override regardless of violation type).
- **Buggy client library** sending malformed signals unintentionally. Distinguished from attackers by logged-not-disconnected response for ambiguous cases.
- **Version-skew client** sending signals the current server doesn't accept.

**Out of model:** network-layer attackers between nodes (mitigated by TLS); side-channel attacks; authentication-layer attacks on cluster join.

### 3.2 Punishment laundering — recognized threat class

Multi-tenant API nodes (mysqld, RDRS) multiplex many users through a single NodeId. If the policy disconnected an API node for any user-attributable bad behavior, an attacker could deliberately trigger violations to disconnect everyone else on that node — weaponizing our defense as a denial-of-service. We call this *punishment laundering*.

The NDB signal protocol carries no user-level identity (no SQL session ID, no HTTP client ID, no Redis connection ID). The cluster sees only per-NodeId attribution. Disconnecting "just the offending user" requires protocol extensions that are out of scope.

Defense against laundering is structural:

1. **Tier A criteria are defined to be impossible to trigger via valid user inputs** (Section 6). When Tier A fires, the API node itself is the problem — disconnecting it is correct regardless of who is on it.
2. **Tier B violations are log-only.** No automated disconnect. The laundering attack has no payload.

### 3.3 Deployment assumptions

RonDB deployments typically run on network-segmented infrastructure with API nodes accessible only from trusted application tiers. The dominant threat is internal (vulnerable internal service, insider, compromised application user). False-positive disconnects are more harmful than missed alerts, because real attacks are rare and customer impact is the primary cost driver.

This tilts the policy toward log-and-investigate (Tier B) over automated disconnect for ambiguous cases. Deployments without effective network segmentation should treat Tier B accumulations as higher-priority signals and tighten connection-level controls at the API node.

### 3.4 Open-source attack-model considerations

The RonDB source is public; attackers can read every violation check and tier definition. The defenses work regardless of attacker knowledge.

An attacker reading the source can see which violations are Tier B (log-only, non-disconnecting) and exploit this: generate Tier B violations as cover, as timing probes, or as a log-flooding attack. **There is no in-kernel rate limiter on Tier B log emission.** This is a known, accepted risk documented in full in Section 13.

The `ndbinfo.security_violation_counts` view requires `PROCESS` privilege. In multi-tenant deployments, a read-capable attacker with this privilege gets live confirmation of detection — a feedback channel. Operators must restrict `PROCESS` to DBA accounts and lock down log access correspondingly.

---

## 4. Background: Vulnerable Call Sites

A code audit of `DbtcMain.cpp` identified **23 call sites** that previously crashed or behaved unsafely on malformed inputs, plus 2 in RONDIS and 1 in SimulatedBlock (fragment assembly). These cluster into five categories:

| Category | Count | Single-signal kill? | Honest client could trigger? |
|---|---|---|---|
| Out-of-bounds index/pointer (`apiConnectPtr`, `tableId`) | 6 | Yes | No |
| Size/length constraints (oversize key/value/section, length mismatch) | 7 | Mixed | Sometimes (oversize values, oversize sections) |
| Semantic protocol constraints (flag combinations, state machine) | 5 | Yes | Yes (state machine bugs, version skew) |
| Fragment-assembly state corruption | 4 | No (multi-signal) | No |
| Transaction hijacking (`apiConnectPtr` not owned by sender) | 4 | Yes | No |

Key observation: 19 of 23 violations are caught and rejected on the first signal. Fragment-assembly attacks require multi-signal accumulation and were already mitigated in Phase 1 hardening (commit `fa1713b`).

---

## 5. Architectural Decisions

### Why QMGR owns counter state

- **Singleton per data node.** In multi-threaded ndbmtd, DBTC runs as multiple instances, each with its own `hostRecord[]`. A counter in DBTC would fragment across TC threads. QMGR is a singleton — no fragmentation.
- **Already in the disconnect path.** The existing `disconnectMaliciousNode()` sent `DUMP_STATE_ORD 900/939` to QMGR. QMGR executes `api_failed()` / `node_failed()`.
- **Cross-block detection.** Any block can send `GSN_MALICIOUS_SIGNAL_REPORT` to QMGR. A counter in DBTC would require awkward cross-block signaling.

### Why lean: fixed per-type array, not per-node history

An earlier design held a `NodeSecurityState[MAX_NODES_ID+1]` array (~260 KB) with per-node sliding windows, cumulative per-node counters, last-strike metadata, and a Tier C volumetric rate-checker. This was stripped for three reasons:

1. External tooling (Prometheus/Grafana) provides time-series; the data node should store raw counts and do no in-kernel interpretation.
2. The sliding window was never a disconnect trigger (Tier A fires on first strike; Tier B never fires automatically) — it existed only to populate a `current_window_count` ndbinfo column.
3. Per-node state grows linearly with the node-id space and multiplicatively with the violation catalog — the wrong trajectory as more security work adds violation types.

The surviving state is ~240 bytes: `Uint64 m_violationCounts[NUM_VIOLATION_TYPES]`.

### Why RONDIS is a separate path

RONDIS is not an NDB kernel block. Redis clients connect to RONDIS via TCP; RONDIS uses the NDB API internally but never sends NDB kernel signals. There is no legitimate path for RONDIS to send `GSN_MALICIOUS_SIGNAL_REPORT` to QMGR. Instead, RONDIS logs directly to its own stdout via the `RONDIS_SECURITY_EVENT` macro.

This has an important ndbinfo consequence: **RONDIS violation types appear in `ndbinfo.security_violations` (the static catalog) but not in `ndbinfo.security_violation_counts` (the counter table).** The catalog lists them because they are defined in `g_violation_info[]`; the counter table has no rows for them because RONDIS never increments `m_violationCounts[]`.

---

## 6. The Two-Tier Response Policy

### Tier A — Immediate disconnect

**What it is:** violations that no protocol-compliant implementation could produce from valid user inputs. Active malice or fundamental compromise.

**Action:** disconnect the offending node via the existing QMGR path (`api_failed()` for API nodes, `node_failed()` for data nodes). No grace, no threshold.

**Categorization rule (HARD REQUIREMENT):** A violation is Tier A *only if* it is **impossible to trigger via valid SQL, HTTP, REST, or Redis user inputs** at any multi-tenant API node. Misclassifying a user-triggerable violation as Tier A reintroduces the laundering attack vector. When adding new Tier A types in future block audits, this must be explicitly verified in the code review.

**Examples** (verified user-untriggerable):
- Signal with zero sections when sections are required
- `sectionNo >= 3` in a fragmented signal (a transporter-framing detail no user input influences)
- Signal length shorter than its declared static header
- `apiConnectPtr` past pool end (server-issued handle; honest client returns it verbatim)
- `apiConnectPtr` not owned by the sender (hijacking)
- Any violation from a data node sender (data nodes run identical code; no "honest mistake" mode)
- Start flag during `CS_ABORTING` (state machine attack pattern)

### Tier B — Log-only forensic observability

**What it is:** semantic violations that *could* indicate a buggy client, version skew, or an attacker — and that *could* be reachable from valid user inputs at a multi-tenant API node.

**Action:** increment `m_violationCounts[vtype]`, emit a `SECURITY_EVENT:` cluster log line, update ndbinfo. **No automated disconnect.**

**Rationale:**
- The safety property (no crash, no OOM, no corruption) is provided by the validation code that already rejects the operation. Tier B adds visibility, not safety.
- Tier B violations are reachable from valid user inputs at multi-tenant API nodes. Automated disconnect would create the laundering attack vector.

**Examples:**
- Oversize KeyInfo / AttrInfo section (reachable via large SQL WHERE / INSERT)
- Signal length mismatch in KEYINFO / ATTRINFO / TCKEYREQ (version-skew plausible)
- `CommitFlag` without `ExecFlag` — ambiguous between attacker and buggy lib
- `UNLOCK` without distribution key
- Reorg flag with invalid operation type
- Key length exceeds `MAX_KEY_SIZE_IN_WORDS`
- RONDIS oversize SET value
- RONDIS SELECT db index out of range

### Data node override rule

Any Tier B violation from a data node sender (NodeId is type `NODE_TYPE_DB`) is automatically escalated to Tier A. Data nodes run identical code; they have no plausible "honest mistake" mode. This is enforced in QMGR — call sites do not need to implement it.

### Fail-secure for unknown violation types

`VT_UNKNOWN` — and any `violationType` integer outside the receiver's known range — resolves to `{TIER_A, "unknown_violation_type"}`. **Unknown violations default to Tier A, not Tier B.** This is the fail-secure behavior for rolling upgrades: when a newer sender emits a violation type the older QMGR was built without, the receiver treats it as maximally severe rather than silently ignoring it. QMGR routes the counter increment to the `VT_UNKNOWN` bucket and emits a log line so the unknown type is visible.

### Why Tier B has no automatic disconnect

| Question | Answer |
|---|---|
| What does input validation already cover? | The safety property: no crash, no OOM, no corruption. Operation is already rejected. |
| What does Tier B disconnect add that logging doesn't? | Operator attention — achievable via `SECURITY_EVENT:` log lines without disconnect. |
| What does Tier B disconnect cost? | False-positive disconnects of multi-tenant API nodes (laundering attack). |

---

## 7. Complete Violation Catalog

All 26 violation types, in enum order (matches `violation_id` in `ndbinfo.security_violations`).

### DBTC (DbtcMain.cpp)

| ID | Enum | Reason string | Tier | Why |
|---|---|---|---|---|
| 0 | VT_UNEXPECTED_API_STATE | unexpected_api_state | **A** | State machine attack — user-untriggerable |
| 1 | VT_APICONNECT_OWNERSHIP | apiconnect_ownership | **A** | Hijacking — user-untriggerable |
| 2 | VT_START_FLAG_DURING_ABORT | start_flag_during_abort | **A** | State machine attack |
| 3 | VT_KEYINFO_INVALID_APICONNECT | keyinfo_invalid_apiconnect | **A** | Out-of-bounds pointer — server-issued handle |
| 4 | VT_KEYINFO_OWNERSHIP | keyinfo_ownership | **A** | Hijacking |
| 5 | VT_KEYINFO_SIGNAL_LENGTH | keyinfo_signal_length_mismatch | **B** | Version-skew or lib bug plausible |
| 6 | VT_ATTRINFO_INVALID_APICONNECT | attrinfo_invalid_apiconnect | **A** | Out-of-bounds pointer |
| 7 | VT_ATTRINFO_OWNERSHIP | attrinfo_ownership | **A** | Hijacking |
| 8 | VT_ATTRINFO_SIGNAL_TOO_SHORT | attrinfo_signal_too_short | **A** | Structural wire-format violation |
| 9 | VT_TCKEYREQ_SIGNAL_TOO_SHORT | tckeyreq_signal_too_short | **A** | Structural |
| 10 | VT_TCKEYREQ_KEYINFO_TOO_LARGE | tckeyreq_keyinfo_too_large | **B** | Reachable via large WHERE/INSERT |
| 11 | VT_TCKEYREQ_ATTRINFO_TOO_LARGE | tckeyreq_attrinfo_too_large | **B** | Reachable via large attribute payload |
| 12 | VT_TCKEYREQ_INVALID_APICONNECT | tckeyreq_invalid_apiconnect | **A** | Out-of-bounds pointer |
| 13 | VT_TCKEYREQ_OWNERSHIP | tckeyreq_ownership | **A** | Hijacking |
| 14 | VT_TCKEYREQ_TABLE_OUT_OF_BOUNDS | tckeyreq_table_out_of_bounds | **A** | Out-of-bounds |
| 15 | VT_REORG_INVALID_OP_TYPE | reorg_invalid_op_type | **B** | Version-skew or lib bug |
| 16 | VT_TCKEYREQ_LONG_SIGNAL_LENGTH | tckeyreq_long_signal_length | **B** | Version-skew plausible |
| 17 | VT_TCKEYREQ_SHORT_SIGNAL_LENGTH | tckeyreq_short_signal_length | **B** | Version-skew plausible |
| 18 | VT_UNLOCK_WITHOUT_DISTKEY | unlock_without_distkey | **B** | Semantic constraint |
| 19 | VT_COMMIT_WITHOUT_EXEC | commit_without_exec | **B** | Semantic constraint |
| 20 | VT_KEY_LENGTH_EXCEEDED | key_length_exceeded | **B** | Reachable via long-key schema |
| 21 | VT_SCANTABREQ_MISSING_SECTION | scantabreq_missing_section | **A** | Structural |
| 22 | VT_SCANTABREQ_INVALID_APICONNECT | scantabreq_invalid_apiconnect | **A** | Out-of-bounds pointer |

### RONDIS (stdout path — no QMGR involvement)

| ID | Enum | Reason string | Tier | Notes |
|---|---|---|---|---|
| 23 | VT_RONDIS_OVERSIZE_VALUE | rondis_oversize_value | **B** | Reachable from any Redis client |
| 24 | VT_RONDIS_SELECT_OUT_OF_RANGE | rondis_select_out_of_range | **B** | Reachable from any Redis client |

**Important:** these appear in `ndbinfo.security_violations` (static catalog), but NOT in `ndbinfo.security_violation_counts` — RONDIS bypasses QMGR and never increments `m_violationCounts[]`.

### SimulatedBlock (fragment assembly, SimulatedBlock.cpp)

| ID | Enum | Reason string | Tier | Notes |
|---|---|---|---|---|
| 25 | VT_FRAGMENT_INVALID_SECTION_NO | fragment_invalid_section_no | **A** | `sectionNo >= 3` — transporter-framing; user-untriggerable |

### Framework-internal

| ID | Enum | Reason string | Tier | Notes |
|---|---|---|---|---|
| 26 | VT_UNKNOWN | unknown_violation_type | **A** | Fallback for out-of-range values; fail-secure |

---

## 8. Signal Flow and Implementation Mechanics

### 8.1 Detection → Report → QMGR

```
Kernel block (e.g. DBTC)                    QMGR (singleton per data node)
─────────────────────────────────────────    ─────────────────────────────────────────
1. Signal handler runs
2. Validation check fails (bad pointer,
   wrong length, ownership mismatch, …)
3. reportMaliciousSignal(signal,             
     offendingNodeId, VT_KEYINFO_SIGNAL_LENGTH)
   → derives tier from g_violation_info[]
   → builds 2-word signal:
       theData[0] = offendingNodeId
       theData[1] = VT_KEYINFO_SIGNAL_LENGTH
   → sendSignal(QMGR_REF, GSN_MALICIOUS_SIGNAL_REPORT,
                signal, 2, JBB)
4. returns (no further processing)           5. execMALICIOUS_SIGNAL_REPORT fires
                                             6. vtype = theData[1]
                                             7. if vtype >= NUM_VIOLATION_TYPES:
                                                  vtype = VT_UNKNOWN  (→ Tier A!)
                                             8. tier = violation_tier(vtype)
                                             9. if sender is NODE_TYPE_DB && tier == B:
                                                  tier = TIER_A  (escalation)
                                             10. m_violationCounts[vtype]++
                                             11. emit SECURITY_EVENT: cluster log line
                                             12. if tier == A:
                                                   securityDisconnectNode(offendingNodeId)
```

`QMGR_REF = numberToRef(QMGR, 0)` is the local node's QMGR — this signal never crosses the network wire. JBB priority ensures security reporting does not starve normal data-plane work.

### 8.2 The MALICIOUS_SIGNAL_REPORT signal

Defined in `storage/ndb/include/kernel/signaldata/MaliciousSignalReport.hpp`:

```cpp
struct MaliciousSignalReport {
  Uint32 offendingNodeId;  // theData[0]
  Uint32 violationType;    // theData[1] — ViolationType enum; QMGR derives tier
  static constexpr Uint32 SignalLength = 2;
};
```

### 8.3 QMGR security state

The complete v2 security state in QMGR (Qmgr.hpp):

```cpp
Uint64  m_violationCounts[NUM_VIOLATION_TYPES];   // ~240 bytes; one counter per type
```

`m_violationCounts[vtype]` is the lifetime cumulative count for that violation type across all senders since cluster start. Zero-initialized at startup. No per-node history, no sliding window, no allocation-failure path. Counter reset on cluster restart only.

### 8.4 ViolationType catalog

`ViolationType.hpp` is the single source of truth. It is header-only (`inline constexpr`) so both the sender side (SimulatedBlock, tier derivation) and the receiver side (QMGR, reason-string lookup) link the same definition:

```cpp
enum ViolationType : Uint32 { VT_UNEXPECTED_API_STATE = 0, ..., VT_UNKNOWN, NUM_VIOLATION_TYPES };

struct ViolationInfo {
  ViolationType id;      // must equal the array index — verified by static_assert below
  ViolationTier tier;
  const char *reason;
};

inline constexpr ViolationInfo g_violation_info[NUM_VIOLATION_TYPES] = {
    {VT_UNEXPECTED_API_STATE, TIER_A, "unexpected_api_state"},
    // ... one row per enum value; id field must match array position ...
    {VT_UNKNOWN,              TIER_A, "unknown_violation_type"},
};

static_assert(
    []() constexpr {
      for (Uint32 i = 0; i < NUM_VIOLATION_TYPES; i++) {
        if (static_cast<Uint32>(g_violation_info[i].id) != i) return false;
      }
      return true;
    }(),
    "g_violation_info[] has an out-of-order or missing entry: each row's id "
    "must equal its array index.");
```

The `static_assert` uses a constexpr lambda to verify that each row's `id` field matches its array index. Adding a row in the wrong position or omitting a row causes a compile-time error — a misordered row can no longer silently survive.

### 8.5 reportMaliciousSignal() — the call site API

Protected method on `SimulatedBlock` (available to every kernel block):

```cpp
void reportMaliciousSignal(Signal* signal,
                           NodeId offendingNodeId,
                           Uint32 violationType);
```

Usage at a detection site:

```cpp
if (unlikely(apiConnectptr.i >= c_apiConnectRecordPool.getSize())) {
  jam();
  releaseSections(handle);  // if called before seizeTcRecord
  reportMaliciousSignal(signal,
    refToNode(signal->getSendersBlockRef()),
    ViolationType::VT_TCKEYREQ_INVALID_APICONNECT);
  return;  // always return immediately — no further processing
}
```

There is no macro. `sourceLine` was removed from the signal in v2 — the violation type + reason string already pinpoints the check; `__LINE__` adds marginal value and costs a word in the signal.

### 8.6 RONDIS detection — stdout path

RONDIS violations never go through QMGR. The `RONDIS_SECURITY_EVENT` macro (in `storage/ndb/src/rondis/include/common.h`) prints directly to stdout:

```cpp
#define RONDIS_SECURITY_EVENT(violation)                               \
  do {                                                                 \
    printf("SECURITY_EVENT: tier=B node_id=0 node_type=API "           \
           "violation=" violation " client=%s worker=%d\n",            \
           g_client_ip_port.c_str(), g_dbg_worker_id);                 \
  } while (0)
```

`g_client_ip_port` is a thread-local `std::string` set to the Redis client's `ip:port` at the start of each connection's command handling. `g_dbg_worker_id` is a thread-local int set by the RONDIS worker dispatcher.

Usage:

```cpp
if (value.size() > REDIS_MAX_VALUE_LEN) {
  RONDIS_SECURITY_EVENT("rondis_oversize_value");
  assign_class_err_to_response(response, REDIS_VALUE_TOO_LARGE);
  return;
}
```

### 8.7 Debug test injector

In debug builds, `ndb_mgm -e "1 DUMP 9100 <offendingNodeId> <violationType>"` synthesizes a `GSN_MALICIOUS_SIGNAL_REPORT` from the management client, exercising the QMGR handler without a real malicious client. Used by the MTR test suite (`ndb_security`, `ndb_security_enforce`).

---

## 9. Observability

### 9.1 Cluster log format

All kernel-side violations emit a line to the NDB cluster log via `NDB_LE_SecurityEvent` → CMVMI → cluster log daemon:

```
SECURITY_EVENT: tier=A node_id=5 node_type=API violation=tckeyreq_invalid_apiconnect
SECURITY_EVENT: tier=B node_id=5 node_type=API violation=keyinfo_signal_length_mismatch
SECURITY_EVENT: tier=A node_id=2 node_type=DB  violation=unexpected_api_state
```

Fields:
- `tier` — `A` (disconnect-eligible) or `B` (log-only forever).
- `node_id` — the offending NodeId.
- `node_type` — `DB`, `API`, or `MGM` (rendered as a string; not a number).
- `violation` — reason string from `g_violation_info[]`.

RONDIS violations go to **RONDIS stdout** (not the cluster log) with the same 4-field prefix plus two attribution fields:

```
SECURITY_EVENT: tier=B node_id=0 node_type=API violation=rondis_oversize_value client=10.0.0.1:49221 worker=3
SECURITY_EVENT: tier=B node_id=0 node_type=API violation=rondis_select_out_of_range client=10.0.0.1:49221 worker=3
```

`node_id=0` means "no NDB NodeId at Redis-client granularity." `client=ip:port` is the Redis TCP connection source; `worker=N` is the RONDIS worker thread.

**The offending node_id for Tier B events lives only in these log lines.** `ndbinfo.security_violation_counts` carries no offender attribution — see Section 9.2.

### 9.2 ndbinfo tables

Two virtual tables, backed by QMGR scan handlers:

#### `ndbinfo.security_violations` — static catalog (no PROCESS privilege required)

```sql
SELECT * FROM ndbinfo.security_violations;
```

| Column | Description |
|---|---|
| `violation_id` | Stable integer — the `ViolationType` enum value. Never renumbered (ndbinfo dashboards index by this). |
| `tier` | `A` or `B` |
| `reason` | Lowercase underscore reason string from `g_violation_info[]` |

One row per known violation type. QMGR on each data node independently emits these rows from `g_violation_info[]`; the SQL view uses `DISTINCT` to deduplicate (2 data nodes → 2 raw rows → 1 view row per type). RONDIS types (ids 23–24) appear here from the catalog but have no corresponding counts rows.

Sample output:

```
violation_id | tier | reason
-------------+------+---------------------------------------
           0 | A    | unexpected_api_state
           5 | B    | keyinfo_signal_length_mismatch
          10 | B    | tckeyreq_keyinfo_too_large
          23 | B    | rondis_oversize_value
          25 | A    | fragment_invalid_section_no
          26 | A    | unknown_violation_type
```

#### `ndbinfo.security_violation_counts` — cumulative counts (PROCESS privilege required)

```sql
SELECT * FROM ndbinfo.security_violation_counts;
```

| Column | Description |
|---|---|
| `reporting_node_id` | Data node that observed this count |
| `violation_id` | FK into `security_violations` |
| `tier` | `A` or `B` (denormalized for convenience) |
| `reason` | Reason string (denormalized) |
| `total_count` | Cumulative count since cluster start |

One row per `(reporting_node_id, violation_id)` with a non-zero count. Zero-count pairs are omitted. **RONDIS violations (ids 23–24) never appear here** — RONDIS bypasses QMGR and never increments `m_violationCounts[]`.

Cluster-wide totals:

```sql
SELECT reason, tier, SUM(total_count) AS cluster_total
FROM ndbinfo.security_violation_counts
GROUP BY violation_id, reason, tier
ORDER BY cluster_total DESC;
```

**Prometheus scrape user** must have both `SELECT` on `ndbinfo.*` AND `PROCESS` privilege.

### 9.3 Log aggregation

```bash
# All security events across all data nodes:
grep "^SECURITY_EVENT:" /var/log/ndb/ndb_*_cluster.log  # path varies by deployment

# Tier A only:
grep "^SECURITY_EVENT: tier=A" /var/log/ndb/ndb_*_cluster.log

# One specific violation:
grep "violation=keyinfo_signal_length_mismatch" /var/log/ndb/ndb_*_cluster.log

# RONDIS events (separate stdout stream — configure log collector for both):
grep "^SECURITY_EVENT:" /var/log/rondis/rondis.log
```

**Loki:**

```logql
{job="ndb_cluster_log"} |= "SECURITY_EVENT:"
  | pattern `SECURITY_EVENT: tier=<tier> node_id=<node_id> node_type=<node_type> violation=<violation>`
```

**Splunk:**

```spl
index=ndb_cluster "SECURITY_EVENT:"
  | rex "SECURITY_EVENT: tier=(?<tier>[AB]) node_id=(?<node_id>\d+) node_type=(?<node_type>\S+) violation=(?<violation>\S+)"
  | stats count by tier, node_id, node_type, violation
```

### 9.4 Prometheus scrape

`mysqld_exporter` custom query collector (`queries.yaml`):

```yaml
ndb_security_violation_counts:
  query: |
    SELECT reporting_node_id, violation_id, reason, tier, total_count
    FROM ndbinfo.security_violation_counts
  metrics:
    - reporting_node_id:
        usage: LABEL
    - violation_id:
        usage: LABEL
        description: Stable integer; use to JOIN with security_violations catalog
    - reason:
        usage: LABEL
    - tier:
        usage: LABEL
    - total_count:
        usage: COUNTER
        description: Cumulative count since cluster start
```

The scraping MySQL user needs `SELECT` on `ndbinfo.*` + `PROCESS` privilege. Scrape interval 30–60 s is sufficient; don't scrape sub-second.

Example alert rules:

```yaml
groups:
  - name: ndb_security
    rules:
      - alert: NdbTierAViolation
        expr: increase(ndb_security_violation_counts_total_count{tier="A"}[5m]) > 0
        for: 0m
        labels:
          severity: page
        annotations:
          summary: "NDB Tier A violation — node {{ $labels.reporting_node_id }}: {{ $labels.reason }}"

      - alert: NdbTierBElevated
        expr: rate(ndb_security_violation_counts_total_count{tier="B"}[10m]) > 0.1
        for: 5m
        labels:
          severity: warn
        annotations:
          summary: "NDB Tier B elevated — {{ $labels.reason }} from node {{ $labels.reporting_node_id }}"
```

### 9.5 Alerting recommendations

| Trigger | Severity | Action |
|---|---|---|
| Any `tier=A` line in cluster log | Page | Cluster disconnected a node — investigate immediately. |
| `tier=A` from multiple distinct `node_id` values simultaneously | Higher-priority page | Coordinated attack or widespread client bug. |
| Sustained Tier B rate from one node | Notify (not page) | Buggy client or active probing. Default threshold: >10 violations/5 min for same `(node_id, violation)`. |

The offending `node_id` for Tier B is only in the log line. Use `violation_id` + `reporting_node_id` as the ndbinfo grouping key.

---

## 10. Framework Extensibility

Adding a new violation site:

1. **Add to `ViolationType.hpp`:** enum value (before `VT_UNKNOWN`) + matching `g_violation_info[]` row. `static_assert` fires at compile time if count mismatches.
2. **Call `reportMaliciousSignal()`** at the detection site, return immediately.
3. **Write a test:** injection → counter increment + log line; Tier A → disconnect.

**Meta-requirement for Tier A:** every new Tier A type must be explicitly verified as user-untriggerable in the code review. Misclassification reintroduces punishment laundering.

**Do not renumber existing values.** `violation_id` is a stable external contract indexed by ndbinfo monitoring dashboards.

### Tier assignment invariant: transaction-aborting sites must stay Tier B

The system maintains a clean single-cleanup guarantee: for any given violation, either the call-site cleanup (abort/release) runs, or the QMGR-triggered node-failure-path cleanup runs — never both for the same transaction. This holds because every call site that performs transaction-level cleanup (`abortErrorLab`, `releaseAtErrorLab`) is currently Tier B, and Tier B never triggers a disconnect. Conversely, Tier A sites that do trigger a disconnect perform no transaction-level cleanup, deliberately deferring to the node-failure path.

**This invariant must be preserved when modifying or adding violation types.** Specifically: a call site that calls `abortErrorLab` or `releaseAtErrorLab` before returning must **not** be assigned Tier A. If such a site were promoted to Tier A, both cleanups would run for the same `ApiConnectRecord` — the call-site abort would drive it to `CS_ABORTING`, and the disconnect-triggered node-failure scan would then process it a second time. The `handleFailedApiConnection` state machine in DBTC does handle `CS_ABORTING` gracefully (it detects in-progress aborts and avoids starting a second one), so this is not a correctness crash — but it violates the single-cleanup contract and creates unnecessary complexity in the control flow.

If you believe a transaction-aborting site genuinely qualifies as Tier A (user-untriggerable), verify the call site can be restructured to skip `abortErrorLab` and rely solely on the disconnect path for cleanup before assigning Tier A.

This guarantee holds in both single-threaded `ndbd` and multi-threaded `ndbmtd`. In `ndbmtd`, each DBTC worker instance owns its own `ApiConnectRecord` pool and processes signals run-to-completion per instance. The call-site cleanup finishes entirely within one scheduler slot; the `API_FAILREQ` that eventually arrives from the disconnect path is a later signal, queued after any pending signals from the failing node (enforced by the CMVMI routing in `sendApiFailReq`). No cross-thread conflict is possible because no two DBTC instances share a record.

---

## 11. Test Coverage

All tests in `mysql-test/suite/ndb/` and `mysql-test/suite/rondis/`:

| Test | What it covers |
|---|---|
| `ndb_security` | Static catalog queries via `security_violations`; counter increments via DUMP 9100 injector; PROCESS privilege gating on `security_violation_counts` |
| `ndbinfo_security_events_priv` | PROCESS privilege enforcement — query denied without it, succeeds with it |
| `ndb_security_enforce` | End-to-end Tier A enforcement: DUMP 9100 against connection-pool node 1600; polls `ndbinfo.processes` to observe disconnect; waits for reconnect; leaves cluster healthy |
| `rondis_security` | RONDIS Tier B paths: oversize SET (> 512000 bytes), out-of-range SELECT; error messages deliberately omit exact limits so the wire response doesn't disclose internal thresholds |

---

## 12. Out of Scope

| Item | Status |
|---|---|
| **Multi-block audit (DBSPJ / DBLQH / DBDICT / SUMA)** | **Committed Phase 2**, immediately after v2 ships. Framework-first makes each finding a one-line addition. |
| Cluster-wide counter aggregation | A peer attacking data node A is invisible to B's QMGR. Complex distributed problem; revisit with empirical data. |
| Persistence to disk | Counter state resets on cluster restart. Restart is hugely visible; restart laundering is unrealistic. |
| RDRS / REST server hardening | Different connection model (HTTP), different attack surface; separate initiative. |
| Kick-session feedback path | **Rejected, not deferred.** Tier A criteria are user-untriggerable by design (API node is the problem; disconnect is correct). Tier B is log-only (nothing to narrow). |
| Comprehensive GSN-to-sender-type allow-list | Block-routing-bypass attack class recognized; building the list requires per-signal audit work; deferred to Phase 2. |

---

## 13. Known and Accepted Risks

| Risk | Accepted because |
|---|---|
| **Tier B log flood (audited 2026).** No in-kernel or upstream rate limiter exists. A connected, authenticated client can sustain thousands/sec of Tier B violations, each emitting one unthrottled `SECURITY_EVENT:` line, churning the rotating 6×1 MB cluster log. Tier A self-limits (offender disconnected on first strike). | Worst case is post-auth forensic-integrity loss, NOT crash or OOM (counter array is fixed size). Adding a throttle either reintroduces per-node state (against lean goal) or discards offender attribution — the offending `node_id` lives ONLY in the log line, not in `m_violationCounts[]`. Decision documented in code comment at `QmgrMain.cpp execMALICIOUS_SIGNAL_REPORT`. Operators bound inflow via connection-level controls; monitor `security_violation_counts` externally. |
| **Cluster-wide attack distribution.** Coordinated attackers spread strikes across data nodes; each QMGR sees only its own counters. | Cross-QMGR aggregation is a real distributed-systems problem. Revisit when justified by empirical data. |
| **Cluster restart resets counters.** No disk persistence. | Cluster restart is itself hugely visible and disruptive; forcing one to launder strikes is a very loud attack. |
| **Unaudited blocks (DBSPJ / DBLQH / DBDICT).** If those blocks crash on bad inputs before reaching validation, counter infrastructure alone doesn't help. | Per-block audit is committed as explicit Phase 2 immediately after v2 ships. Unaudited blocks remain a real gap until then. |
| **Tier B log-only has no automatic enforcement.** A persistently-misbehaving API node produces Tier B events forever. | Explicit policy choice against punishment laundering. Operators use monitoring to detect patterns and intervene manually. |
| **Reconnect oscillation for Tier A.** Node keeps reconnecting and triggering Tier A each time. | Bounded by reconnect delay; surfaces the issue rapidly. Worst case: offender burns its own and slightly the cluster's resources until an operator disconnects the node at the network level. |
| **Live ndbinfo is a live attacker feedback channel.** With `PROCESS` privilege, an attacker can probe and confirm detection in real time. | Accepted cost of meaningful observability. Restrict `PROCESS` to DBA accounts; lock down log access correspondingly. |
| **RONDIS oversize value: read-before-reject.** The RESP parser fully buffers bulk strings (up to 512 000 bytes) before calling the command handler; the size check fires on the materialized buffer, not the length header. This means a sustained stream of max-size values is buffered repeatedly before rejection — a memory pressure path, not just a log-flood. | REDIS_MAX_VALUE_LEN (512 000 bytes) caps the buffer; no single value can OOM the process. The same limit that produces the Tier B violation also bounds the memory cost. |
