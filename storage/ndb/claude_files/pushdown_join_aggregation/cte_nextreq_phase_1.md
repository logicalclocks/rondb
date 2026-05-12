# Phase 1 — Fix `CTE_SCAN_REQ` continuation plumbing

## Goal

Make the existing back-to-back DBSPJ→DBLQH continuation correct so
multi-batch `CTE_SCAN` works end-to-end on its own. Phase 2 then
replaces the back-to-back pattern with the SCAN_NEXTREQ cycle, but
cannot be built on top of a broken continuation.

## Problem

In `Dbspj::execCTE_SCAN_CONF`
(`storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp:7102-7151`) the
continuation `CTE_SCAN_REQ` is built and sent with
`CteScanReq::SignalLength` (9) and **never sets `req->scanIterI`**. On
the DBLQH side (`DblqhMain.cpp:20415-20419`) `scanIterI` is only read
when `signal->getLength() >= CteScanReq::SignalLengthContinue` (10), so
every continuation REQ currently arrives with `scanIterI = RNIL` —
DBLQH restarts the hash-walk from bucket 0.

This is latent today because `CteScanData::m_batchSize` is hard-coded
to 256 (`DbspjMain.cpp:6722`), and no existing test scans a CTE with
more than 256 groups through the DBSPJ-mediated continuation path. The
`testCteScanFilterBatchBoundary` test exercises a different path
(`cteScanAggFeed` with CONTINUEB), which has its own state preservation
via `CteScanIterState`.

## Implementation

### 1. Extend `CteScanData`

File: `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` (~line 665).

Add single-node field and per-source-node slots. Keep it inline for
now; if the struct grows too large we can move the multi-node array
to a pool record in a follow-up.

```cpp
struct CteScanData {
  Uint32 m_cteId;
  Uint32 m_numResultCols;
  Uint32 m_aggStateKey;
  Uint32 m_outstanding;
  Uint32 m_rowsReceived;
  Uint32 m_rowsExpecting;
  Uint32 m_batchSize;
  bool m_endOfData;
  Uint32 m_api_resultRef;
  Uint32 m_joinAggStateKey;

  /* Per-source-node scan iterator state for multi-batch continuation.
   * Index 0 is used for the single-node / single-targetNodeId path.
   * m_numNodeSlots tracks how many slots are populated. */
  struct NodeSlot {
    Uint32 m_sourceNodeId;   // 0 = unused
    Uint32 m_scanIterI;      // RNIL = fresh scan OR after EndOfData
    bool m_endOfData;
  };
  static constexpr Uint32 MAX_CTE_SCAN_NODE_SLOTS = MAX_NDB_NODES;
  NodeSlot m_nodeSlots[MAX_CTE_SCAN_NODE_SLOTS];
  Uint32 m_numNodeSlots;
};
```

Initialize in `cte_scan_build` (`DbspjMain.cpp:6714-6730`): zero the
slot array and set `m_numNodeSlots = 0`.

### 2. Extract `cte_scan_sendReq` helper

Pull the REQ-building body out of `cte_scan_start` at
`DbspjMain.cpp:6899-6939` and `6952-6984` (there are two near-identical
copies — single-node and fan-out) into a single helper:

```cpp
/* Build and send a CTE_SCAN_REQ to the DBLQH on sourceNodeId.
 * scanIterI is RNIL on the first REQ (SignalLength) and the stashed
 * CteScanIterState i-value on continuations (SignalLengthContinue). */
void Dbspj::cte_scan_sendReq(Signal *signal,
                             Ptr<Request> requestPtr,
                             Ptr<TreeNode> treeNodePtr,
                             Uint32 sourceNodeId,
                             Uint32 aggStateKey,
                             Uint32 scanIterI);
```

Call from:
- `cte_scan_start` — both the "one node" branch and the
  fan-out-to-all-nodes branch, with `scanIterI = RNIL`.
- `execCTE_SCAN_CONF` continuation — with the stashed per-slot
  `scanIterI`.

The helper:
- Builds the `CteScanReq` in `signal->getDataPtrSend()`.
- Duplicates the AttrInfo section (`treeNodePtr.p->m_send.m_attrInfoPtrI`,
  same pattern as line 7127-7138) and attaches it.
- Picks `SignalLength` vs. `SignalLengthContinue` based on whether
  `scanIterI == RNIL`.
- Sends to `numberToRef(DBLQH, 1, sourceNodeId)`.
- Increments `data.m_outstanding`.

Also consolidates the two near-identical send blocks so future changes
(Phase 2 adds batch-size plumbing, close flag) touch one call site.

### 3. Slot lookup / update in `execCTE_SCAN_CONF`

At the top of `execCTE_SCAN_CONF`
(`DbspjMain.cpp:7027-7185`), find or allocate the slot for the source
node:

```cpp
const Uint32 srcNode = refToNode(conf->senderRef);
CteScanData::NodeSlot *slot = find_or_add_node_slot(data, srcNode);
```

Before the `endOfData` branch:

```cpp
slot->m_scanIterI = conf->scanIterI;  // RNIL when EndOfData
```

When `endOfData`:

```cpp
slot->m_endOfData = true;
slot->m_scanIterI = RNIL;  // DBLQH has already released the pool record
```

In the non-EndOfData branch at line 7102, replace the inline REQ build
(7111-7144) with:

```cpp
cte_scan_sendReq(signal, requestPtr, treeNodePtr,
                 srcNode, sourceAggKey, slot->m_scanIterI);
```

The `m_outstanding` bump moves into the helper.

### 4. Slot lookup / update in `cte_scan_start`

When fanning out initial REQs, seed a slot per target node with
`m_scanIterI = RNIL`, `m_endOfData = false`. The helper then sends
with `SignalLength` (9). No behavioural change — just wired through
the new storage.

### 5. DBLQH — no code changes

Verified: `execCTE_SCAN_REQ` at `DblqhMain.cpp:20415-20419` already
handles both REQ lengths; `cteScanEmitResults` at `19979-19997` resumes
from the pool record and `20244-20259` seizes/reuses it. No changes
required for Phase 1.

## Regression test

Extend `storage/ndb/block_unit_test/testCteNdbApi.cpp` (or add a new
test in `testCteNdbApiFilter.cpp`) with a scenario that produces more
than 256 distinct CTE groups so the continuation path is exercised.
Target ≥ 512 groups to comfortably cross a batch boundary. Assert:

- total row count == number of distinct groups.
- A monotonic per-group value / checksum matches expected.
- No duplicate groups, no gaps.

Data loading follows the existing `sqlExec` INSERT pattern; the
baseline-aggregate-then-scan query form of `testCteNdbApi` is the
right template.

## Acceptance

- Debug build of `ndbd` / `ndbmtd` succeeds.
- `testCteNdbApi`, `testCteNdbApiFilter`,
  `testCteScanFilterBatchBoundary` all still pass.
- New >512-group test passes.
- `DEB_CTE` trace (with `DEBUG_CTE` enabled) shows
  `scanIterI != RNIL` on continuation REQs from DBSPJ and matching
  pool-record reuse in DBLQH.

## Out of scope for Phase 1

- No SCAN_NEXTREQ wiring (Phase 2).
- No change to `m_batchSize = 256` hard-coding (Phase 2).
- No close/abort path (Phase 2.7).
- No CTE lifetime audit (Phase 3).
- No new end-to-end NDB-API tests beyond the >512-group regression
  (Phase 4).
