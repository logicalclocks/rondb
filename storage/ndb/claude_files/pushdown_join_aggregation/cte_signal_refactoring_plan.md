# Plan: Refactor CTE_LOOKUP/CTE_SCAN to Use Standard Correlation Logic

## Problem

CTE_LOOKUP_REQ and CTE_SCAN_REQ bypass DBSPJ's standard lookup/scan
correlation machinery. This means:
- No CORR_FACTOR correlation (NDB API can't match child rows to parents)
- No proper FLUSH_AI routing
- No inner/outer join semantics

## Design: Reuse lookup_send/scanFrag_send Logic

Keep CTE_LOOKUP_REQ and CTE_SCAN_REQ as the actual signals sent to
DBLQH. But set them up using the same logic as `lookup_send` and
`scanFrag_send` — same CORR_FACTOR, FLUSH_AI, result routing,
outstanding tracking.

### DBSPJ: CTE_LOOKUP via shared lookup logic

Extract the common logic from `lookup_send` into a shared helper that:
1. Counts expected replies (CONF/REF + TRANSID_AI)
2. Builds the key section (expand from parent row)
3. Builds the AttrInfo section (FLUSH_AI + CORR_FACTOR + user projection)
4. Tracks outstanding signals
5. Sets correlation data (root receiver ID + tuple correlation)

Then `lookup_send` calls the helper and sends LQHKEYREQ.
`cte_lookup_send` calls the same helper for steps 1-5, then sends
CTE_LOOKUP_REQ with the prepared sections.

The key difference: LQHKEYREQ has its own signal format (LqhKeyReq
struct, variableData, requestInfo bits). CTE_LOOKUP_REQ has a simpler
format (senderRef, senderData, aggStateKey, keyLen). But both carry
the same KeyInfo and AttrInfo sections.

### DBSPJ: CTE_SCAN via shared scan logic

Similarly, `scanFrag_send` has logic for:
1. Building SCAN_FRAGREQ with batch parameters
2. Setting up correlation for scan batches
3. Tracking outstanding fragments
4. Handling SCAN_FRAGCONF / scan-batch-complete

`cte_scan_start` should reuse the batch tracking, correlation setup,
and outstanding management from `scanFrag_send`. The actual signal
sent is CTE_SCAN_REQ instead of SCAN_FRAGREQ, but the correlation
and result routing is the same.

### DBLQH: CTE handlers include CORR_FACTOR

The DBLQH `execCTE_LOOKUP_REQ` handler already processes CORR_FACTOR
in the AttrInfo final-read section. It just needs the AttrInfo to
actually contain CORR_FACTOR — which happens when DBSPJ builds the
AttrInfo using the same logic as lookup_send.

The DBLQH `execCTE_SCAN_REQ` handler needs to include CORR_FACTOR
in its TRANSID_AI output, similar to how regular scan rows include it.

## Implementation Steps

### Step 1: Extract common AttrInfo building from lookup_send

**File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

Create a helper function (or refactor inline) that builds the AttrInfo
sections common to both LQHKEYREQ and CTE_LOOKUP_REQ:
- Duplicate m_send.m_attrInfoPtrI
- Append CORR_FACTOR with proper correlation value
- Append FLUSH_AI targeting the API client
- Track T_EXPECT_TRANSID_AI and outstanding counts

`lookup_send` uses this to build LQHKEYREQ's AttrInfo.
`cte_lookup_send` uses the same AttrInfo for CTE_LOOKUP_REQ.

### Step 2: Refactor cte_lookup_send to use shared logic

**File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

Change `cte_lookup_send` to:
1. Use `lookup_send`'s key expansion logic (already does this)
2. Use the shared AttrInfo builder (FLUSH_AI + CORR_FACTOR)
3. Build and send CTE_LOOKUP_REQ (instead of LQHKEYREQ)
4. Track outstanding using the same pattern as lookup_send

The CTE_LOOKUP_REQ signal format stays unchanged. The AttrInfo section
attached to it now includes FLUSH_AI and CORR_FACTOR.

### Step 3: DBLQH CTE_LOOKUP_REQ — verify CORR_FACTOR handling

**File:** `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`

The existing `execCTE_LOOKUP_REQ` handler already processes CORR_FACTOR
in the final-read section. Verify it works correctly with the new
AttrInfo format from step 2. The CORR_FACTOR output writes
`senderData` (TreeNode.i) — this may need to be the actual correlation
value. Check what `lookup_send` puts in the CORR_FACTOR variableData.

### Step 4: Extract common scan-batch logic from scanFrag_send

**File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`

The CTE_SCAN needs:
- Batch size management (rows per batch, outstanding tracking)
- CORR_FACTOR in TRANSID_AI rows
- SCAN_FRAGCONF-equivalent batch completion

Identify the reusable parts of `scanFrag_send` and make them
available to `cte_scan_start`.

### Step 5: Refactor cte_scan_start to use shared scan logic

Similar to step 2 but for scans. The CTE_SCAN_REQ signal format
stays unchanged, but the batch/correlation handling reuses scan logic.

### Step 6: NDB API — remove CTE_LOOKUP special cases

**File:** `storage/ndb/src/ndbapi/NdbQueryOperation.cpp`

With proper CORR_FACTOR in CTE_LOOKUP results:
- Remove skip of CorrelationData parsing for CteLookup type
- CTE_LOOKUP results flow through standard join correlation
- testCteNdbApi Test 2 should work

### Step 7: Verify

- testCteDbtc: All existing tests still pass (CTE_LOOKUP_REQ/CTE_SCAN_REQ
  wire format unchanged, AttrInfo now includes CORR_FACTOR)
- testCteNdbApi: Test 2 (CTE_LOOKUP) works end-to-end
- testJoinAggSpj, testJoinAggNdbApi: Regression

## Key Insight

The CTE signals (CTE_LOOKUP_REQ, CTE_SCAN_REQ) are the RIGHT signals
for DBLQH — they're simpler than LQHKEYREQ/SCAN_FRAGREQ and
purpose-built for hash table access. The issue is only in how DBSPJ
SETS THEM UP — it needs to use the same correlation and result routing
logic that the standard lookup/scan paths use.
