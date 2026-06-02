# Security Audit Implementation Guide

**Purpose:** Reference for adding new vulnerability patches during the Phase 2 block audit (DBSPJ, DBLQH, DBDICT, SUMA) and for any future DBTC additions. Read this before touching any security-related code path.

**Full design doc:** [tiered_response_policy.md](tiered_response_policy.md)

---

## System overview

The security framework intercepts malformed NDB protocol signals before they can crash or corrupt state. Each detection site in a kernel block calls a single macro; a signal is sent asynchronously to QMGR, which holds all per-node counter state and decides whether to disconnect the sender.

```
Kernel block (e.g. DBTC)          QMGR (singleton per data node)
  detects bad input
  → REPORT_MALICIOUS_SIGNAL()      ← receives GSN_MALICIOUS_SIGNAL_REPORT
    sends signal to QMGR            increments per-node counters
                                    writes SECURITY_EVENT: cluster log line
                                    if Tier A + kill switch on: disconnects node
                                    if Tier B: stops here (log-only)
```

RONDIS has a separate per-Redis-connection counter system that mirrors this model but does not use QMGR (RONDIS never traverses the NDB transporter).

**Key constraint:** The block-side code never disconnects directly. It only reports. QMGR owns all disconnect decisions.

---

## Key file locations

| File | Purpose |
|---|---|
| `storage/ndb/src/kernel/vm/SimulatedBlock.hpp` | Where `reportMaliciousSignal()` and `REPORT_MALICIOUS_SIGNAL` macro live |
| `storage/ndb/src/kernel/blocks/qmgr/` | QMGR block — owns `NodeSecurityState`, handles `GSN_MALICIOUS_SIGNAL_REPORT` |
| `storage/ndb/include/kernel/signaldata/MaliciousSignalReport.hpp` | Signal layout for `GSN_MALICIOUS_SIGNAL_REPORT` (to be created in v1) |
| `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` | 23 existing migrated call sites — use as reference |
| `storage/ndb/src/rondis/src/commands.cc` | RONDIS Tier B site: oversize SET value |
| `storage/ndb/src/rondis/src/rondb.cc` | RONDIS Tier B site: SELECT db index out of range |
| `storage/ndb/include/kernel/GlobalSignalNumbers.h` | Where `GSN_MALICIOUS_SIGNAL_REPORT` is registered |
| `storage/ndb/include/ndb_limits.h` | `MAX_NODES`, `MAX_KEY_SIZE_IN_WORDS`, and similar bounds used in checks |

---

## How to categorize a new violation

Every new violation site must be assigned **Tier A or Tier B** before writing any code. Use this decision tree:

### Tier A criteria (immediate disconnect)

Assign Tier A **only if ALL of the following are true:**

1. **Impossible to trigger via valid user inputs.** A legitimate SQL query, HTTP/REST request, or Redis command at any multi-tenant API node (mysqld, RDRS, RONDIS) cannot cause this violation — not through a bug in the user's query, not through a large payload, not through version skew.
2. **Verified specifically for multi-tenant API nodes.** Consider mysqld (SQL users), RDRS (HTTP clients), and RONDIS (Redis clients). If any of these *could* produce the signal pattern through normal operation, it is Tier B.
3. **The API node itself must be the problem**, not a user on it. Tier A fires mean the API node is compromised, running incompatible code, or fundamentally broken.

**Examples that qualify:**
- Signal with zero sections when the handler requires sections (no SQL produces a zero-section signal)
- `apiConnectPtr` outside valid pool range (the server issues the handle; honest clients return it verbatim)
- Transaction handle claimed by a different node's API connection (ownership hijacking)
- Internal-only signal type sent by an API node (`ViolationType::WRONG_SENDER_TYPE_FOR_GSN`)
- Any violation from a data node sender (data nodes run identical code; no honest-mistake mode applies — see override rule below)

**If in doubt, assign Tier B.** Misclassifying a user-triggerable violation as Tier A enables punishment laundering attacks (an attacker triggers Tier A to disconnect all users on a shared mysqld). This is the highest-risk classification error.

### Tier B criteria (log-only)

Assign Tier B if:
- A buggy client library, version-skew mismatch, or edge-case user input could plausibly produce this violation, **OR**
- You are not certain the violation is user-untriggerable.

Tier B fires log and count but never disconnect. The safety property (no crash, no memory corruption) is provided by the validation code that already rejects the operation — not by the disconnect. Tier B adds visibility, not safety.

### Data node override rule

**Any Tier B violation from a data node sender is automatically escalated to Tier A.** Data nodes are cluster peers running the same code as the receiving node. They have no plausible "honest mistake" mode. A data node sending a Tier B violation is a sign of code incompatibility or compromise.

Check the sender: `refToNode(signal->getSendersBlockRef())`. If that NodeId is type `NODE_TYPE_DB`, the tier is A regardless of what the violation type says. This is already enforced in QMGR's handler — call sites do not need to implement it manually.

### When you find a new signal path in an unaudited block

Systematically check each signal handler for:
1. **Pointer/index bounds** — any field used as an array index or pointer offset without bounds checking → likely Tier A
2. **Ownership validation** — any handle or record pointer that should belong to the sender → likely Tier A
3. **State machine transitions** — any signal that should only arrive in certain states → likely Tier A
4. **Size/length constraints** — oversized payloads → Tier B (user-reachable), structural truncation (signal too short for declared header) → Tier A
5. **Flag combinations** — semantic constraints on flag fields → Tier B (version-skew plausible)
6. **Internal-only signal types** — signals that should only travel between data nodes but have no guard → Tier A (`WRONG_SENDER_TYPE_FOR_GSN`)

---

## Step-by-step: implementing a new violation site

### Step 1: Verify the existing guard

Confirm the validation code **already safely rejects** the malformed input (returns early, sends error, etc.) before calling `REPORT_MALICIOUS_SIGNAL`. The framework adds observability and potential disconnect; it does not add safety. If the guard is absent or incomplete, fix the guard first.

### Step 2: Add a ViolationType enum value AND its catalog row

Everything about a violation type — its tier and its reason string — lives in one
place: `storage/ndb/include/kernel/ViolationType.hpp`. Add the enum value (before
the `VT_UNKNOWN` / `NUM_VIOLATION_TYPES` sentinels):

```cpp
enum ViolationType : Uint32 {
  // ... existing values ...
  VT_YOUR_NEW_VIOLATION_TYPE,   // A or B: brief description
  // ...
  VT_UNKNOWN,                   // rolling-upgrade fallback — keep second-last
  NUM_VIOLATION_TYPES           // sentinel — keep last
};
```

Then add the matching row to `g_violation_info[]` **at the same position** (the
rows are positional and must stay in enum order — a `static_assert` enforces the
count):

```cpp
{TIER_A, "your_new_violation_type"},   // VT_YOUR_NEW_VIOLATION_TYPE
```

The tier (`TIER_A`/`TIER_B`) and the reason string live in this single row. The
reason string appears in `SECURITY_EVENT:` cluster log lines — keep it under ~60
chars, lowercase, underscore-separated. **There is no separate QMGR lookup table**
— QMGR resolves the reason via `violation_reason(vtype)` from this same header.

### Step 3: Call the macro at the detection site

```cpp
if (unlikely(/* violation condition */)) {
  jam();
  REPORT_MALICIOUS_SIGNAL(signal, offendingNodeId,
                          ViolationType::VT_YOUR_NEW_VIOLATION_TYPE);
  return;  // always return immediately after — no further processing
}
```

The call site passes **only the violation type** — the tier is derived from the
catalog row, so a call site can never tag the wrong tier. `REPORT_MALICIOUS_SIGNAL`
expands to `reportMaliciousSignal(signal, nodeId, violationType, __LINE__)`; the
`__LINE__` capture is automatic and zero-cost.

**Getting `offendingNodeId`:** This is the NodeId of the sender. In most signal handlers:
```cpp
NodeId offendingNodeId = refToNode(signal->getSendersBlockRef());
```
In some handlers the sender NodeId is carried explicitly in signal data fields (e.g., `TCKEYREQ` has `apiConnectPtr` from which the NodeId can be derived). Match what the existing DBTC call sites do in the same signal handler.

### Step 4: Write the test

Every new call site requires at minimum:

1. **A Tier A injection test:** send the malformed signal from an NDB API test client → verify the sending node is disconnected, verify a `SECURITY_EVENT: tier=A` log line is emitted, verify the offending node's `total_disconnects` increments in ndbinfo.
2. **A Tier B injection test (if Tier B):** same injection → verify no disconnect, verify counter increments, verify log line.
3. **A kill-switch test:** flip `EnableSecurityDisconnect=false`, inject Tier A violation → verify no disconnect, verify log line still emits.

Tests live in `mysql-test/suite/rondis/` (existing) or a new `mysql-test/suite/ndb_security/` suite. Use the NDB API or a mock client to inject malformed signals below the SQL/RDRS layer.

---

## API reference

### REPORT_MALICIOUS_SIGNAL macro

```cpp
// In SimulatedBlock.hpp:
#define REPORT_MALICIOUS_SIGNAL(signal, offendingNodeId, violationType) \
  reportMaliciousSignal((signal), (offendingNodeId), (violationType), __LINE__)
```

### reportMaliciousSignal() signature

```cpp
// Protected method on SimulatedBlock:
void reportMaliciousSignal(Signal* signal,
                           NodeId offendingNodeId,
                           Uint32 violationType,   // ViolationType; tier derived
                           Uint32 sourceLine);
```

The tier is derived inside the sender from `violation_tier(violationType)` and
travels in the report signal, so the call site never specifies it.

### Tier constants

```cpp
// enum ViolationTier in kernel/ViolationType.hpp
enum ViolationTier : Uint32 { TIER_A = 0, TIER_B = 1 };
```

### GSN_MALICIOUS_SIGNAL_REPORT layout (MaliciousSignalReport.hpp)

```
theData[0] = offendingNodeId
theData[1] = tier            (ViolationTier; derived by sender from violation type)
theData[2] = violationType   (ViolationType enum value; QMGR resolves reason via violation_reason())
theData[3] = sourceBlockRef  (reporting block reference, for forensics)
theData[4] = sourceLine      (__LINE__ at the call site, for forensics)
theData[5] = suppressedCount (reports batched since last send; report-rate limiting)
```

### QMGR NodeSecurityState structure

Per-node state, indexed directly by node id in a fixed array
(`m_nodeSecurity[MAX_NODES_ID + 1]`). The sliding window is **per node, not
per violation type** — one shared 10×30 s ring (~80 bytes/node):

```cpp
struct NodeSecurityState {
  Uint64 m_total_tier_a_strikes;
  Uint64 m_total_tier_b_strikes;
  Uint64 m_total_disconnects;
  NDB_TICKS m_last_strike_time;
  Uint32 m_last_violation_type;
  Uint32 m_last_source_line;
  Uint32 m_window_count[10];   // strikes per 30 s slice
  Uint32 m_window_epoch[10];   // epoch (now/30s) each slice represents
};
```

State is a **fixed array allocated once** in the QMGR singleton (~260 KB total) —
no lazy allocation, no allocation-failure path. Per-violation-type granularity is
preserved in the cumulative counters and the `SECURITY_EVENT:` cluster log, not in
the live window (this keeps the window cost flat as the violation catalog grows).

---

## Cluster log format

All detection events write a line in this format:

```
SECURITY_EVENT: tier=<A|B> node_id=<N> node_type=<API|DB|MGM> violation=<reason_string> source_block=<BLOCK> source_line=<N> window_count=<N> total_count=<N>
```

The per-node security state is a fixed array, so there is no allocation-failure
path and no `SECURITY_DEGRADED` log line. A typed `SECURITY_KILLSWITCH:` event for
live kill-switch changes is deferred together with the live-`ndb_mgm SET` plumbing
(until then the kill-switch state is written once at startup as a plain info line).

---

## Existing violation catalog

All 25 currently-known sites, for reference during audits of new blocks. Use these as examples of correctly-categorized violations.

### DBTC (DbtcMain.cpp)

| Line | Violation | Tier | Why |
|---|---|---|---|
| 2462 | signal in unexpected apiConnectRecord state | A | State machine attack — user-untriggerable |
| 2486 | apiConnectRecord owned by different node | A | Hijacking |
| 2658 | start flag during active abort | A | State machine attack |
| 2883 | invalid apiConnectPtr in KEYINFO | A | Out-of-bounds pointer |
| 2895 | KEYINFO apiConnectPtr not owned by sender | A | Hijacking |
| 2990 | KEYINFO signal length mismatch | B | Version-skew plausible |
| 3037 | invalid apiConnectPtr in ATTRINFO | A | Out-of-bounds pointer |
| 3051 | ATTRINFO apiConnectPtr not owned by sender | A | Hijacking |
| 3075 | ATTRINFO signal too short | A | Structural wire-format violation |
| 3624 | TCKEYREQ signal too short | A | Structural |
| 3641 | TCKEYREQ KeyInfo section too large | B | Reachable via large WHERE/INSERT |
| 3651 | TCKEYREQ AttrInfo section too large | B | Reachable via large attribute payload |
| 3682 | invalid apiConnectPtr in TCKEYREQ | A | Out-of-bounds pointer |
| 3702 | TCKEYREQ apiConnectPtr not owned by sender | A | Hijacking |
| 3735 | table index out of bounds in TCKEYREQ | A | Out-of-bounds |
| 4443 | reorg flag with invalid operation type | B | Version-skew or lib bug |
| 4497 | TCKEYREQ long signal length mismatch | B | Version-skew plausible |
| 4515 | TCKEYREQ short signal length mismatch | B | Version-skew plausible |
| 4532 | UNLOCK without distribution key | B | Semantic constraint |
| 4800 | CommitFlag without ExecFlag | B | Semantic constraint |
| 4917 | key length exceeds MAX_KEY_SIZE_IN_WORDS | B | Reachable via long-key schema |
| 15913 | SCAN_TABREQ missing required section 0 | A | Structural |
| 15970 | invalid apiConnectPtr in SCAN_TABREQ | A | Out-of-bounds pointer |

### RONDIS (separate per-Redis-connection counter system)

| File | Violation | Tier | Notes |
|---|---|---|---|
| commands.cc | Oversize SET value (> REDIS_MAX_VALUE_LEN) | B | Reachable from any Redis client |
| rondb.cc | SELECT db index < 0 or >= g_num_databases | B | Reachable from any Redis client |

---

## What to look for when auditing a new block

When auditing DBSPJ, DBLQH, DBDICT, SUMA, or any other block, systematically grep for these patterns in each signal handler:

```bash
# Patterns indicating existing guards that should be migrated to the framework:
grep -n "disconnect\|ASSERT\|ndbabort\|ndbrequire\|return.*false\|goto.*error" <block>Main.cpp

# Patterns indicating potential missing guards:
grep -n "\[.*Ptr\|->m_\|arrayAccess\|\[tableId\]\|\[nodeId\]" <block>Main.cpp
```

For each signal handler, check:
- Every array index derived from signal data fields — bounds checked?
- Every handle/pointer that the server issued to the client — returned verbatim? Ownership verified?
- Every state machine transition — valid from current state?
- Every signal that should only come from a data node — sender type checked?
- Signal declared length vs. actual received length — matched?

When you find an unguarded path that would crash or corrupt state: add the guard first, then add `REPORT_MALICIOUS_SIGNAL` after the guard, following the steps above.

---

## Pre-commit checklist for new Tier A sites

Before merging any PR that adds a Tier A call site:

- [ ] **Verified user-untriggerable:** Can a legitimate SQL query, HTTP request, or Redis command produce this violation at a multi-tenant API node? If yes → demote to Tier B.
- [ ] **Verified for all API node types:** mysqld, RDRS, and RONDIS all considered?
- [ ] **Data node escalation not needed:** The Tier B → Tier A escalation for data-node senders is handled in QMGR automatically. No manual check needed at the call site.
- [ ] **Guard is correct and complete:** The validation code rejects the bad input independently of the `REPORT_MALICIOUS_SIGNAL` call.
- [ ] **ViolationType enum value added** before the `VT_UNKNOWN`/`NUM_VIOLATION_TYPES` sentinels in `ViolationType.hpp`.
- [ ] **Catalog row added** to `g_violation_info[]` at the matching position (tier + reason string); `static_assert` passes.
- [ ] **Return immediately** after `REPORT_MALICIOUS_SIGNAL` — no further processing of the malformed signal.
- [ ] **Injection test written** per Section 15.2 of the policy doc.

---

## Config parameters reference

| Parameter | Default | Change mechanism | Notes |
|---|---|---|---|
| `EnableSecurityDisconnect` | `true` | Live `ndb_mgm SET` | `false` = observation mode (log only, no disconnect) |
| `SecurityReportSuppressionMs` | `100` | Live `ndb_mgm SET` | Per-node report rate limiting window |
| `SecurityWindowDurationSec` | `300` | Restart required | Sliding window length (5 minutes) |
| `SecurityBucketGranularitySec` | `30` | Restart required | Bucket size (10 buckets per window) |
| `SecurityRateLimitClusterSignalsPerSec` | unset | Live `ndb_mgm SET` | Tier C safety net; set to 10× empirical peak |
| `SecurityRateLimitClusterBytesPerSec` | unset | Live `ndb_mgm SET` | Tier C bytes safety net |

---

## Rolling-upgrade safety

When adding a new `ViolationType` value, the receiving QMGR may be an older version that doesn't know about it. QMGR's handler validates `violation_type < NUM_VIOLATION_TYPES_KNOWN_TO_RECEIVER` before indexing the bucket array. Out-of-range values route to a generic `UNKNOWN_VIOLATION_TYPE` bucket and emit a cluster log line. No special handling is needed at new call sites — this is handled automatically in QMGR's receive path.

**Do not** insert new enum values in the middle of the existing list. Always append before `NUM_VIOLATION_TYPES`. Changing existing values' integer representation breaks rolling upgrade compatibility.

---

## RONDIS violations (different path)

RONDIS has its own per-Redis-connection counter system, independent of QMGR, because RONDIS connections never traverse the NDB transporter. When adding a new RONDIS violation:

- Increment the per-connection Tier B counter (no QMGR signal needed)
- Emit a `SECURITY_EVENT:` cluster log line in the same format as kernel-side events
- Do **not** call `REPORT_MALICIOUS_SIGNAL` — that's for NDB kernel blocks only
- Tier B only for RONDIS violations (all are user-reachable by definition from a Redis client)

The RONDIS per-connection rate limiter (Tier C for RONDIS) may close an individual Redis TCP connection if per-connection rates are unreasonable. This is the upstream enforcement layer; it only affects the one Redis client.
