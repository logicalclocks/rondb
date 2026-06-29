# Security Audit Implementation Guide

**Purpose:** Reference for adding new vulnerability patches during the Phase 2 block audit (DBSPJ, DBLQH, DBDICT, SUMA) and for any future DBTC additions. Read this before touching any security-related code path.

**Full design doc:** [tiered_response_policy.md](tiered_response_policy.md)

---

## System overview

The security framework intercepts malformed NDB protocol signals before they can crash or corrupt state. Each detection site in a kernel block calls a single function; a signal is sent asynchronously to QMGR, which holds the per-violation-type counter array and decides whether to disconnect the sender.

```
Kernel block (e.g. DBTC)            QMGR (singleton per data node)
  detects bad input
  → reportMaliciousSignal()          ← receives GSN_MALICIOUS_SIGNAL_REPORT
    sends signal to QMGR              increments m_violationCounts[vtype]
                                      writes SECURITY_EVENT: cluster log line
                                      if Tier A: disconnects node
                                      if Tier B: stops here (log-only)
```

RONDIS has a separate path — it writes directly to stdout via `RONDIS_SECURITY_EVENT` macro and never traverses the NDB transporter. No QMGR signal is involved for RONDIS violations.

**Key constraint:** The block-side code never disconnects directly. It only reports. QMGR owns all disconnect decisions.

---

## Key file locations

| File | Purpose |
|---|---|
| `storage/ndb/src/kernel/vm/SimulatedBlock.hpp` | Where `reportMaliciousSignal()` lives (protected method) |
| `storage/ndb/src/kernel/blocks/qmgr/QmgrMain.cpp` | QMGR handler `execMALICIOUS_SIGNAL_REPORT`; holds `m_violationCounts[]` |
| `storage/ndb/include/kernel/signaldata/MaliciousSignalReport.hpp` | 2-field signal layout |
| `storage/ndb/include/kernel/ViolationType.hpp` | Violation enum + `g_violation_info[]` catalog (tier + reason string) |
| `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp` | 23 existing call sites — use as reference |
| `storage/ndb/src/rondis/include/common.h` | `RONDIS_SECURITY_EVENT` macro |
| `storage/ndb/src/rondis/src/commands.cc` | RONDIS Tier B site: oversize SET value |
| `storage/ndb/src/rondis/src/rondb.cc` | RONDIS Tier B site: SELECT db index out of range |
| `storage/ndb/include/kernel/GlobalSignalNumbers.h` | `GSN_MALICIOUS_SIGNAL_REPORT` registration |
| `storage/ndb/include/ndb_limits.h` | `MAX_NODES`, `MAX_KEY_SIZE_IN_WORDS`, and similar bounds |

---

## How to categorize a new violation

Every new violation site must be assigned **Tier A or Tier B** before writing any code. Use this decision tree:

### Tier A criteria (immediate disconnect)

Assign Tier A **only if ALL of the following are true:**

1. **Impossible to trigger via valid user inputs.** A legitimate SQL query, HTTP/REST request, or Redis command at any multi-tenant API node (mysqld, RDRS, RONDIS) cannot cause this violation — not through a bug in the user's query, not through a large payload, not through version skew.
2. **Verified specifically for multi-tenant API nodes.** Consider mysqld (SQL users), RDRS (HTTP clients), and RONDIS (Redis clients). If any of these *could* produce the signal pattern through normal operation, it is Tier B.
3. **The API node itself must be the problem**, not a user on it.

**Examples that qualify:**
- Signal with zero sections when the handler requires sections
- `apiConnectPtr` outside valid pool range (server issues the handle; honest clients return it verbatim)
- Transaction handle claimed by a different node (ownership hijacking)
- Internal-only signal type sent by an API node
- Any violation from a data node sender (data nodes run identical code; no honest-mistake mode)

**If in doubt, assign Tier B.** Misclassifying a user-triggerable violation as Tier A enables punishment laundering attacks (an attacker triggers Tier A to disconnect all users on a shared mysqld). This is the highest-risk classification error.

### Tier B criteria (log-only)

Assign Tier B if:
- A buggy client library, version-skew mismatch, or edge-case user input could plausibly produce this violation, **OR**
- You are not certain the violation is user-untriggerable.

Tier B fires log and count but never disconnect. The safety property (no crash, no memory corruption) is provided by the validation code that already rejects the operation — Tier B adds visibility, not safety.

### Data node override rule

**Any Tier B violation from a data node sender is automatically escalated to Tier A.** This is already enforced in QMGR's handler — call sites do not need to implement it manually.

---

## Step-by-step: implementing a new violation site

### Step 1: Verify the existing guard

Confirm the validation code **already safely rejects** the malformed input before calling `reportMaliciousSignal`. The framework adds observability and potential disconnect; it does not add safety. Fix the guard first if it is absent or incomplete.

### Step 2: Add a ViolationType enum value AND its catalog row

Everything about a violation type — its tier and its reason string — lives in one place: `storage/ndb/include/kernel/ViolationType.hpp`. Add the enum value (before the `VT_UNKNOWN` / `NUM_VIOLATION_TYPES` sentinels):

```cpp
enum ViolationType : Uint32 {
  // ... existing values ...
  VT_YOUR_NEW_VIOLATION_TYPE,   // A or B: brief description
  // ...
  VT_UNKNOWN,                   // rolling-upgrade fallback — keep second-last
  NUM_VIOLATION_TYPES           // sentinel — keep last
};
```

Then add the matching row to `g_violation_info[]` **at the same position** (positional — a `static_assert` enforces the count):

```cpp
{TIER_A, "your_new_violation_type"},   // VT_YOUR_NEW_VIOLATION_TYPE
```

The tier and the reason string live in this single row. The reason string appears in `SECURITY_EVENT:` cluster log lines — keep it under ~60 chars, lowercase, underscore-separated.

### Step 3: Call reportMaliciousSignal() at the detection site

```cpp
if (unlikely(/* violation condition */)) {
  jam();
  reportMaliciousSignal(signal, offendingNodeId,
                        ViolationType::VT_YOUR_NEW_VIOLATION_TYPE);
  return;  // always return immediately after — no further processing
}
```

The call site passes **only the violation type** — tier is derived from `g_violation_info[]` inside the sender, so a call site can never tag the wrong tier.

**Getting `offendingNodeId`:** This is the NodeId of the sender. In most signal handlers:
```cpp
NodeId offendingNodeId = refToNode(signal->getSendersBlockRef());
```
In some handlers the sender NodeId is explicit in signal data fields (e.g., `apiConnectptr.p->ndbapiBlockref`). Match what the existing DBTC call sites do in the same handler.

### Step 4: Write the test

Every new call site requires at minimum:

1. **Tier A injection test:** trigger the violation → verify disconnect + `SECURITY_EVENT: tier=A` log line + `total_count` increment in `ndbinfo.security_violation_counts`.
2. **Tier B injection test (if Tier B):** same → verify no disconnect, counter increments, log line.
Tests live in `mysql-test/suite/ndb/`. Use `DUMP 9100 <offendingNodeId> <violationType>` as the injector (debug builds only — no need for a custom NDB API client).

---

## API reference

### reportMaliciousSignal() signature

```cpp
// Protected method on SimulatedBlock (SimulatedBlock.hpp):
void reportMaliciousSignal(Signal* signal,
                           NodeId offendingNodeId,
                           Uint32 violationType);   // ViolationType; tier derived
```

The tier is derived inside the sender from `violation_tier(violationType)` and travels in the report signal. There is no `sourceLine` parameter — the macro was removed in v2.

### GSN_MALICIOUS_SIGNAL_REPORT layout (MaliciousSignalReport.hpp)

```
theData[0] = offendingNodeId   (NodeId of the sender)
theData[1] = violationType     (ViolationType enum value; QMGR derives tier via violation_tier())
SignalLength = 2
```

QMGR's handler (`execMALICIOUS_SIGNAL_REPORT`):
1. Bounds-check `violationType < NUM_VIOLATION_TYPES`; route out-of-range to `VT_UNKNOWN`.
2. Recompute `tier = violation_tier(vtype)`.
3. If sender is a data node (NODE_TYPE_DB) and tier is B: escalate to A.
4. Increment `m_violationCounts[vtype]`.
5. Emit `SECURITY_EVENT:` cluster log line.
6. If `tier == A`: invoke disconnect path.

### QMGR security state

```cpp
// In Qmgr.hpp — the complete v2 security state:
Uint64  m_violationCounts[NUM_VIOLATION_TYPES];   // ~240 bytes
```

Fixed array, zero-initialized at startup. No per-node state, no sliding window, no allocation-failure path.

### Tier constants

```cpp
// ViolationType.hpp
enum ViolationTier : Uint32 { TIER_A = 0, TIER_B = 1 };
```

---

## Cluster log format

All detection events write a line in this format:

```
SECURITY_EVENT: tier=<A|B> node_id=<N> node_type=<DB|API|MGM> violation=<reason_string>
```

RONDIS adds two extra fields after `violation=`:
```
SECURITY_EVENT: tier=B node_id=0 node_type=API violation=<reason> client=<ip:port> worker=<id>
```

There are no `source_block`, `source_line`, `window_count`, or `total_count` fields in v2.

---

## Existing violation catalog

All 25 currently-known sites. Use as examples of correctly-categorized violations.

### DBTC (DbtcMain.cpp)

| Line (approx) | Violation | Tier | Why |
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

### RONDIS (separate path — no QMGR signal)

| ID | File | Violation | Tier | Notes |
|---|---|---|---|---|
| 23 | commands.cc | Oversize SET value (> REDIS_MAX_VALUE_LEN) | B | Reachable from any Redis client |
| 24 | rondb.cc | SELECT db index < 0 or >= g_num_databases | B | Reachable from any Redis client |

RONDIS types appear in `ndbinfo.security_violations` (static catalog, from `g_violation_info[]`) but NOT in `ndbinfo.security_violation_counts` — RONDIS bypasses QMGR.

### SimulatedBlock (fragment assembly — SimulatedBlock.cpp)

| ID | Location | Violation | Tier | Notes |
|---|---|---|---|---|
| 25 | SimulatedBlock.cpp `assembleFragmentsSlow` | `sectionNo >= 3` in fragmented signal | A | Transporter-framing detail; user-untriggerable |

RONDIS violations use `RONDIS_SECURITY_EVENT("reason_string")` from `storage/ndb/src/rondis/include/common.h`. Do **not** call `reportMaliciousSignal` from RONDIS — that is for NDB kernel blocks only.

---

## What to look for when auditing a new block

Systematically grep each signal handler for:

```bash
# Patterns indicating existing guards to migrate:
grep -n "disconnect\|ASSERT\|ndbabort\|ndbrequire\|return.*false\|goto.*error" <block>Main.cpp

# Patterns indicating potentially missing guards:
grep -n "\[.*Ptr\|\->m_\|arrayAccess\|\[tableId\]\|\[nodeId\]" <block>Main.cpp
```

For each signal handler, check:
- Every array index derived from signal data fields — bounds checked?
- Every handle/pointer the server issued to the client — returned verbatim? Ownership verified?
- Every state machine transition — valid from current state?
- Every signal that should only come from a data node — sender type checked?
- Signal declared length vs. actual received length — matched?

When you find an unguarded path that would crash or corrupt state: add the guard first, then add `reportMaliciousSignal` after the guard.

---

## Pre-commit checklist for new Tier A sites

- [ ] **Verified user-untriggerable:** Can a legitimate SQL query, HTTP request, or Redis command produce this violation at a multi-tenant API node? If yes → demote to Tier B.
- [ ] **Verified for all API node types:** mysqld, RDRS, and RONDIS all considered?
- [ ] **Guard is correct and complete:** Validation code rejects the bad input independently of `reportMaliciousSignal`.
- [ ] **ViolationType enum value added** before `VT_UNKNOWN`/`NUM_VIOLATION_TYPES` sentinels in `ViolationType.hpp`.
- [ ] **Catalog row added** to `g_violation_info[]` at the matching position; `static_assert` passes.
- [ ] **Return immediately** after `reportMaliciousSignal` — no further processing of the malformed signal.
- [ ] **Call site does NOT call `abortErrorLab` or `releaseAtErrorLab`:** Tier A sites must not perform transaction-level cleanup. The QMGR-triggered disconnect drives the node-failure path in DBTC, which is the sole cleanup owner for Tier A violations. If your call site needs `abortErrorLab`/`releaseAtErrorLab`, it must be Tier B. Promoting a transaction-aborting site to Tier A violates the single-cleanup invariant — see _Tier assignment invariant_ in `tiered_response_policy.md` Section 10.
- [ ] **Injection test written** targeting `ndb_security` or a new suite.

---

---

## Rolling-upgrade safety

When adding a new `ViolationType` value, the receiving QMGR may be an older version that doesn't know about it. QMGR's handler validates `violationType < NUM_VIOLATION_TYPES` before indexing `m_violationCounts[]`; out-of-range values route to the `VT_UNKNOWN` bucket and emit a cluster log line. No special handling needed at new call sites.

**Do not** insert new enum values in the middle of the existing list. Always append before `VT_UNKNOWN`/`NUM_VIOLATION_TYPES`. The `violation_id` integer is a stable external contract indexed by ndbinfo monitoring dashboards — renumbering silently corrupts historical metrics.

---

## RONDIS violations (different path)

RONDIS is architecturally separate from the NDB transporter — Redis clients have their own TCP connections and RONDIS never sends GSN signals. When adding a new RONDIS violation:

- Emit `RONDIS_SECURITY_EVENT("your_reason_string")` from `common.h`
- **Do not** call `reportMaliciousSignal` — that is for NDB kernel blocks only
- All RONDIS violations are Tier B by definition (reachable from any Redis client)
- Add the reason string to `ViolationType.hpp` as a `VT_RONDIS_*` enum value and a Tier B catalog row, so the `security_violations` catalog lists it (even though RONDIS counts never flow through `m_violationCounts`)
