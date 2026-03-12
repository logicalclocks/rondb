# Plan: Chained Outer Join Aggregation in DBSPJ

## Problem Statement

Pushdown aggregation with chained LEFT JOINs (3+ tables) produces wrong results.
Only 2-way outer joins (scan → leaf) work correctly. In a chain like
`dept LEFT JOIN emp LEFT JOIN task`, when an intermediate node (emp) gets no match,
its descendants (task) never execute and the parent row disappears from results.

The test program `testMultiOuterJoinAggNdbApi` demonstrates the failures.

## Root Cause

All three null row injection code paths are gated by `T_AGGREGATE_LEAF`:

```cpp
if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) &&
    !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN))
```

When an intermediate outer join node (Node 1: MatchAll, NOT aggregate leaf) gets
no match, no null row is injected and its children are never triggered:

1. **`lookup_parent_row` (line 5622)**: NULL key → returns without triggering children
2. **`lookup_execLQHKEYREF` (line 5354)**: KEYREF(626) → `lookup_stop_branch()` stops
   the entire branch; children never execute
3. **`scanFrag_complete` (line 8799)**: Only fires for T_AGGREGATE_LEAF scan nodes

## Design Approach

### Core Idea: Walk Down to Leaf and Send Null Row

When an intermediate MatchAll (outer join) node has no match, we don't need to
create synthetic null rows or trigger child processing. Instead, we walk down
the query tree from the unmatched node toward the aggregate leaf, checking
whether any intermediate node blocks null propagation (MatchNonNull / INNER JOIN).
If the path is clear, we call `sendJoinAggNullRow()` on the aggregate leaf with
the original parent row from the scan ancestor.

This works because:
- `sendJoinAggNullRow()` expands `m_attrParamPattern` from the provided `rowRef`
- The `attrParamPattern` contains linked projection values (GROUP BY columns)
- These linked values reference the scan root or other ancestors above the gap
- The aggregate engine treats local columns as NULL (correct for unmatched rows)

### What About Linked Values from Intermediate Nodes?

If the aggregate leaf's `attrParamPattern` references columns from an unmatched
intermediate node (e.g., `GROUP BY emp.salary` when emp didn't match), the
`expand()` → `appendFromParent()` call would fail because no row is buffered
for that intermediate node.

**Phase 1 handles the common case**: linked projections only from the scan root.
This covers the typical pattern of `GROUP BY root_column, SUM(leaf_column)`.

**Phase 3 adds general support**: synthesize NULL values when `appendFromParent()`
encounters an unbuffered intermediate node.

### New Flag: T_AGGREGATE_ANCESTOR

A new TreeNode bit flag `T_AGGREGATE_ANCESTOR` marks every node that is a proper
ancestor of `T_AGGREGATE_LEAF`. This enables efficient checks without tree walks
at runtime. Set during `build_query_tree()` by walking up from the leaf.

---

## Phase 1: Lookup Chains (scan → LEFT lookup → ... → LEFT lookup [leaf])

**Status: IMPLEMENTED**

This is the simplest and most common case: a scan root with a chain of lookup
children, all LEFT JOIN, with aggregation on the leaf.

### Step 1.1: Add T_AGGREGATE_ANCESTOR Flag

**File: `Dbspj.hpp`**

Add to `TreeNode::m_bits` enum:

```cpp
/**
 * Set on every TreeNode that is a proper ancestor of the
 * T_AGGREGATE_LEAF node. Used to detect intermediate outer join
 * nodes that need null row propagation for chained outer joins.
 */
T_AGGREGATE_ANCESTOR = 0x4000000,
```

**File: `DbspjMain.cpp`** (in `build_query_tree()` or nearby setup code)

After `T_AGGREGATE_LEAF` is set on the leaf node, walk up via `m_parentPtrI`
and set `T_AGGREGATE_ANCESTOR` on every ancestor up to (but not including) the
root:

```cpp
if (treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) {
  // Mark all ancestors
  Uint32 parentPtrI = treeNodePtr.p->m_parentPtrI;
  while (parentPtrI != RNIL) {
    Ptr<TreeNode> ancestorPtr;
    ndbrequire(m_treenode_pool.getPtr(ancestorPtr, parentPtrI));
    ancestorPtr.p->m_bits |= TreeNode::T_AGGREGATE_ANCESTOR;
    parentPtrI = ancestorPtr.p->m_parentPtrI;
  }
}
```

### Step 1.2: Helper Function `propagateNullToAggLeaf()`

**File: `DbspjMain.cpp`**

New helper that walks from an unmatched intermediate node down to the aggregate
leaf, checking for INNER JOIN blockers:

```cpp
/**
 * propagateNullToAggLeaf()
 *
 * Called when an intermediate outer join node (MatchAll, T_AGGREGATE_ANCESTOR)
 * has no match. Walks down the tree toward the aggregate leaf. If any
 * intermediate node has MatchNonNull (INNER JOIN), the null row is blocked
 * and we return without injecting. Otherwise, call sendJoinAggNullRow()
 * on the aggregate leaf with the original parent row.
 *
 * @param treeNodePtr  The unmatched intermediate node
 * @param rowRef       The parent row that triggered this node
 * @return 0 on success, error code on failure
 */
Uint32 Dbspj::propagateNullToAggLeaf(Signal *signal,
                                      Ptr<Request> requestPtr,
                                      Ptr<TreeNode> treeNodePtr,
                                      const RowPtr &rowRef)
{
  // Walk down from treeNodePtr toward the aggregate leaf.
  // At each level, find the child that has T_AGGREGATE_ANCESTOR or
  // T_AGGREGATE_LEAF (there is exactly one such child on the path).
  Ptr<TreeNode> current = treeNodePtr;
  for (;;) {
    // Iterate children of current node
    LocalArenaPool<DataBufferSegment<14>> pool(requestPtr.p->m_arena,
                                               m_dependency_map_pool);
    Local_dependency_map children(pool, current.p->m_child_nodes);
    Dependency_map::ConstDataBufferIterator it;
    bool found = false;

    for (children.first(it); !it.isNull(); children.next(it)) {
      Ptr<TreeNode> childPtr;
      ndbrequire(m_treenode_pool.getPtr(childPtr, *it.data));

      if (childPtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) {
        // Reached the leaf — send null row
        return sendJoinAggNullRow(signal, requestPtr, childPtr, rowRef);
      }

      if (childPtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) {
        // This child is on the path to the leaf
        if (childPtr.p->m_bits & TreeNode::T_INNER_JOIN) {
          // INNER JOIN blocks null propagation — this parent row
          // is correctly filtered out (no contribution to results)
          return 0;
        }
        current = childPtr;
        found = true;
        break;
      }
    }
    ndbrequire(found);  // Must find a child on the path
  }
}
```

### Step 1.3: lookup_parent_row (NULL Key Path) — Revised

**File: `DbspjMain.cpp`, line ~5622**

**Original plan**: Add inline `propagateNullToAggLeaf()` call for
`T_AGGREGATE_ANCESTOR` in the NULL key path (parallel to T_AGGREGATE_LEAF).

**Revised (after Phase 2 design)**: No inline propagation for intermediate
ancestors. Instead, a comment explains that the completion-time match tracking
handler (`handleAggAncestorLookupComplete`) covers both NULL key and KEYREF(626)
cases. When the key is NULL, no LQHKEYREQ is sent, no TRANSID_AI arrives, so
the match bit is never set — the completion handler detects this as unmatched.

The existing `T_AGGREGATE_LEAF` inline path remains unchanged (the aggregate
leaf sends its own null row directly via `sendJoinAggNullRow`).

### Step 1.4: lookup_execLQHKEYREF (KEYREF 626 Path) — Revised

**Original plan**: Add inline null propagation in KEYREF handler, requiring
parent row recovery from correlation tracking.

**Revised**: No changes to `lookup_execLQHKEYREF` for intermediate ancestors.
The KEYREF is handled normally (stops the branch), and the match bit is never
set (no TRANSID_AI arrived). The completion-time handler detects the unmatched
parent row.

Note: The original plan's "parent row recovery" problem (LqhKeyRef only carries
connectPtr, not correlation) is completely avoided by this approach.

### Step 1.5: Handle Completion Counting

When `propagateNullToAggLeaf()` calls `sendJoinAggNullRow()`, it increments
`m_outstanding` and clears the completion bit for the aggregate leaf. The
intermediate node that triggered the propagation must NOT be double-counted.

Verify that the existing completion tracking in `lookup_countSignal()` /
`handleTreeNodeComplete()` remains correct when the intermediate node's
branch stops (KEYREF or NULL key) while the leaf has a pending null row.

The intermediate node's branch still stops normally (no children triggered
via `startNextNodes`). The null row is sent directly to the leaf as a separate
path. The leaf's outstanding count increases, so it won't complete prematurely.

### Step 1.6: Test Verification

Build and run:
```bash
make -j$(sysctl -n hw.ncpu) testMultiOuterJoinAggNdbApi
./testMultiOuterJoinAggNdbApi -c <connect_string> -m <port> --only 1
```

Test 1 (3-way outer join) should now pass: all 5 groups present with correct
COUNT and SUM values.

---

## Phase 2: Completion-Time Match Tracking for Lookup Ancestors

**Status: IMPLEMENTED**

This phase solves the KEYREF(626) problem for intermediate aggregate ancestors.
The core insight: `LqhKeyRef` only carries `connectPtr` (= treeNodePtr.i) and
`errorCode` — no correlation value, so there's no way to identify which parent
row triggered the failed lookup inline. Instead of trying to recover the parent
row in the KEYREF handler, we use **completion-time match tracking**.

### Design: Defer to Completion

When an intermediate aggregate ancestor lookup node completes (all outstanding
lookups done), iterate the scan ancestor's buffered rows and check match bits.
Any parent row whose match bit was never set for this node represents an
unmatched outer join — inject a null row for that parent via
`propagateNullToAggLeaf()`.

This approach handles both failure modes uniformly:
- **NULL key** (detected in `lookup_parent_row`): No LQHKEYREQ sent → no
  TRANSID_AI arrives → match bit never set → detected at completion
- **KEYREF(626)** (row not found): LQHKEYREF arrives but no TRANSID_AI →
  match bit never set → detected at completion

### Step 2.1: Enable Match Tracking on Scan Ancestor

**File: `DbspjMain.cpp`** (in `validateAggregateFlags()`)

After the T_AGGREGATE_ANCESTOR marking loop, enable T_BUFFER_MATCH on the
scan ancestor for any intermediate outer join aggregate ancestor.

**Critical**: `validateAggregateFlags()` runs BEFORE `setupAncestors()`, so
`m_scanAncestorPtrI` is still RNIL at this point. Must walk `m_parentPtrI`
to find the nearest scan ancestor instead:

```cpp
for (list.first(treeNodePtr); !treeNodePtr.isNull();
     list.next(treeNodePtr)) {
  if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) &&
      !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN)) {
    jam();
    // Walk m_parentPtrI to find scan ancestor (m_scanAncestorPtrI
    // is not yet set — setupAncestors() runs after validateAggregateFlags)
    Ptr<TreeNode> scanAncestorPtr;
    Uint32 parentPtrI = treeNodePtr.p->m_parentPtrI;
    bool found = false;
    while (parentPtrI != RNIL) {
      ndbrequire(m_treenode_pool.getPtr(scanAncestorPtr, parentPtrI));
      if (scanAncestorPtr.p->isScan()) { found = true; break; }
      parentPtrI = scanAncestorPtr.p->m_parentPtrI;
    }
    if (!found) continue;
    scanAncestorPtr.p->m_bits |=
        TreeNode::T_BUFFER_ROW | TreeNode::T_BUFFER_MAP |
        TreeNode::T_BUFFER_MATCH;
    requestPtr.p->m_bits |= Request::RT_AGG_ANCESTOR_MATCH;
    break;  // Only need to set this once (all ancestors share scan root)
  }
}
```

### Step 2.2: New Request Bit RT_AGG_ANCESTOR_MATCH

**File: `Dbspj.hpp`**

Added `RT_AGG_ANCESTOR_MATCH = 0x100` to the Request bit flags. This enables
the match tracking code path in `execTRANSID_AI` for single-scan queries
(without `RT_MULTI_SCAN`).

### Step 2.3: Extend execTRANSID_AI Match Tracking

**File: `DbspjMain.cpp`** (in `execTRANSID_AI`)

The existing match bit setting code only fires when `RT_MULTI_SCAN` is set
(queries with multiple scan children). Extended to also fire for
`RT_AGG_ANCESTOR_MATCH`:

```cpp
if ((requestPtr.p->m_bits &
     (Request::RT_MULTI_SCAN | Request::RT_AGG_ANCESTOR_MATCH)) != 0) {
  // Set 'matched' bit in previous scan ancestors
  ...
}
```

### Step 2.4: Remove Phase 1 Inline Propagation

**File: `DbspjMain.cpp`** (in `lookup_parent_row`)

Removed the Phase 1 inline `propagateNullToAggLeaf()` call for
`T_AGGREGATE_ANCESTOR` in the NULL key path. The completion-time handler
covers both NULL key and KEYREF(626) cases uniformly — the match bit simply
won't be set for unmatched parents.

### Step 2.5: New Function handleAggAncestorLookupComplete()

**File: `DbspjMain.cpp`**

Called at lookup node completion (when `m_outstanding == 0`). Iterates scan
ancestor's buffered rows, checks match bit for this node's `m_node_no`,
and calls `propagateNullToAggLeaf()` for each unmatched parent:

```cpp
Uint32 Dbspj::handleAggAncestorLookupComplete(Signal *signal,
                                               Ptr<Request> requestPtr,
                                               Ptr<TreeNode> treeNodePtr) {
  // Guard: only for outer join aggregate ancestors with scan ancestor
  if (!(treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) ||
      (treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN) ||
      treeNodePtr.p->m_scanAncestorPtrI == RNIL)
    return 0;

  // Iterate scan ancestor rows, find unmatched
  for (each row in scanAncestor->m_rows) {
    if (!row.m_matched->get(treeNodePtr.p->m_node_no)) {
      propagateNullToAggLeaf(signal, requestPtr, treeNodePtr, parentRow);
    }
  }
  return 0;
}
```

### Step 2.6: Call Sites (4 Lookup Completion Points)

Added `handleAggAncestorLookupComplete()` call before `handleTreeNodeComplete`
at all 4 lookup completion sites:

1. **`lookup_execLQHKEYREF`**: After `lookup_stop_branch`, when outstanding == 0
2. **`execTRANSID_AI`**: Normal completion path when outstanding == 0
3. **`lookup_execLQHKEYCONF`**: When outstanding == 0 (aggregate leaf path)
4. **`lookup_parent_row`**: NULL key path when outstanding == 0

### Key Insight: DBLQH Handles Aggregate Leaf KEYREF

For the **aggregate leaf** node (not intermediate ancestors), DBLQH's
`handleOuterJoinAggKeyNotFound()` (line 15044) intercepts KEYREF(626) and:
1. Processes the null row locally via `processNullExtendedRow()`
2. Sends `LQHKEYCONF` (not LQHKEYREF) back to DBSPJ

This means DBSPJ **never receives KEYREF** for aggregate leaf outer join lookups.
The completion-time match tracking is only needed for **intermediate ancestor**
nodes.

---

## Phase 3: 4-Way Mixed Join (INNER JOIN in the Chain)

Test 2 in `testMultiOuterJoinAggNdbApi` exercises:
`scan → LEFT lookup → INNER lookup → LEFT lookup [leaf]`

### Step 3.1: INNER JOIN Blocks Null Propagation

When Node 1 (LEFT) gets no match, `propagateNullToAggLeaf()` walks to Node 2
(INNER) and finds `T_INNER_JOIN` → returns 0 (no null row). This is correct:
the INNER JOIN filters out null-extended rows.

**This already works** with the Phase 1 implementation. The walk-down logic
checks `T_INNER_JOIN` at each intermediate node and stops.

### Step 3.2: Verify KEYREF at Different Levels

- Node 1 (LEFT) KEYREF → walk finds Node 2 (INNER) → blocked → correct
- Node 2 (INNER) KEYREF → Node 2 is NOT MatchAll, so no propagation → correct
- Node 3 (LEFT leaf) KEYREF → already handled by existing T_AGGREGATE_LEAF code

### Step 3.3: Test Verification

```bash
./testMultiOuterJoinAggNdbApi -c <connect_string> -m <port> --only 2
```

Test 2 should pass with Phase 1 implementation alone (the INNER JOIN correctly
filters null rows).

---

## Phase 4: Scan Intermediate Nodes — IMPLEMENTATION PLAN

### Overview

Extend chained outer join aggregation to handle cases where intermediate nodes
are **scans** (scanIndex via NdbQueryIndexBound), not lookups. Currently only
lookup intermediates work (Phase 2). Tests 4 and 6 in testMultiOuterJoinAggNdbApi
demonstrate the gap:

- Test 4: `scan → LEFT scan → LEFT lookup [leaf]` — scan intermediate, 2 missing groups
- Test 6: `scan → LEFT scan → LEFT scan → LEFT lookup [leaf]` — two scan intermediates

### Background: Why Lookup Intermediates Work but Scan Intermediates Don't

For **lookup** intermediates (Phase 2), `handleAggAncestorLookupComplete()` fires
when the lookup node's `m_lookup_data.m_outstanding` reaches 0. At that point,
TRANSID_AI match tracking has already set `m_matched` bits on the scan ancestor's
buffered rows for every lookup that succeeded. Unmatched rows get null injection
via `propagateNullToAggLeaf()`.

For **scan** intermediates, the analogous completion point is when
`data.m_frags_complete == data.m_fragCount` in `scanFrag_execSCAN_FRAGCONF()`.
Currently this only triggers null injection for `T_AGGREGATE_LEAF` scan nodes
(line 9116). The existing match tracking mechanisms are:

1. **Bitmask exchange** (`JOIN_AGG_MATCH_REQ/CONF`): DBLQH tracks which ranges
   matched using the aggregate state. This is DBLQH-side tracking — only works
   for the aggregate leaf because only the leaf has a DBLQH aggregate state.
   **Cannot be used for intermediate scans.**

2. **Inline match** (`T_AGG_INLINE_MATCH`): TRANSID_AI processing in DBSPJ
   sets `m_matched->set(treeNodePtr.p->m_node_no)` on the scan ancestor's
   buffered rows (line 4052 in `execTRANSID_AI`). This is DBSPJ-side tracking
   and already works per-node. **Can be used for intermediate scans.**

3. **RT_AGG_ANCESTOR_MATCH**: The mechanism added in Phase 2 for lookup
   intermediates. Also sets `m_matched` bits via TRANSID_AI. **Already works
   for scan intermediates** — TRANSID_AI from scan child rows flows through
   the same match tracking code.

**Key insight**: The `m_matched` bits on the scan ancestor's rows are ALREADY
being set correctly for scan intermediate nodes (via TRANSID_AI processing in
`execTRANSID_AI`). The only missing piece is the **completion-time check** —
iterating unmatched rows and calling `propagateNullToAggLeaf()` when the
intermediate scan finishes.

### Prerequisites and Constraints

1. **Inline match mode required**: The bitmask exchange protocol
   (`JOIN_AGG_MATCH_REQ/CONF`) cannot be used for intermediates because it
   relies on DBLQH aggregate state which only exists for the leaf. All
   intermediate scan ancestor null tracking must use inline match
   (`m_matched` bits set via TRANSID_AI).

2. **Scan ancestor must have T_BUFFER_MATCH**: The scan ancestor (root scan)
   must buffer rows with match bitmasks. This is already set up in
   `validateAggregateFlags()` when `RT_AGG_ANCESTOR_MATCH` is enabled.

3. **Parent match check required**: Same as Phase 2 — deeper intermediates
   must check that their parent's match bit is set before injecting, to
   prevent duplicate injection when a higher-level node already handled
   the null row.

4. **Multi-batch considerations**: Scan children may span multiple batches
   via SCAN_NEXTREQ. The completion point (`m_frags_complete == m_fragCount`)
   is final — it fires exactly once when all fragments report "no more rows."
   This is safe for null injection because all scan results have been
   delivered by that point.

5. **Scan-scan scheduling**: In RT_MULTI_SCAN queries, scan children are
   deferred (TN_EXEC_WAIT) and resumed (TN_RESUME_NODE) when the parent
   scan completes a batch. The match bits are checked in `resumeBufferedNode()`
   for scheduling purposes. Phase 4 null injection should happen AFTER the
   intermediate scan's own completion, not during resume.

### Step 4.1: scanFrag_parent_row — NULL Key Path

**File: `DbspjMain.cpp`, `scanFrag_parent_row()` line ~7669**

When a scan child's key expansion produces NULL values (`hasNull == true`),
the scan is known to produce zero matches. Currently only `T_AGGREGATE_LEAF`
gets null injection here. Add `T_AGGREGATE_ANCESTOR` handling:

```cpp
// Existing code (line 7681):
if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) &&
    !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN)) {
  jam();
  err = sendJoinAggNullRow(signal, requestPtr, treeNodePtr, rowRef);
  if (unlikely(err != 0)) break;
}

// ADD after the existing block:
if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) &&
    !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN)) {
  jam();
  err = propagateNullToAggLeaf(signal, requestPtr, treeNodePtr, rowRef);
  if (unlikely(err != 0)) break;
}
```

**Note**: For the NULL key case, we have `rowRef` available (the parent row
that triggered this scan child). `propagateNullToAggLeaf()` walks down to
the aggregate leaf and computes the correct `parentLevelAdjust` by measuring
the distance from the leaf's parent to the scan ancestor.

**Important**: The `rowRef` here may come from the scan ancestor (root scan)
or from an intermediate scan. `propagateNullToAggLeaf()` already handles both
cases correctly — `parentLevelAdjust` is computed from the leaf's perspective,
not from the unmatched node's.

### Step 4.2: scanFrag_execSCAN_FRAGCONF — Completion-Time Null Injection

**File: `DbspjMain.cpp`, `scanFrag_execSCAN_FRAGCONF()` line ~9116**

Add a new code block **after** the existing `T_AGGREGATE_LEAF` completion check
(or restructure to handle both). When an intermediate scan with
`T_AGGREGATE_ANCESTOR` completes all fragments:

```cpp
// After the existing T_AGGREGATE_LEAF block (line 9116-9215):
if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) &&
    !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN) &&
    treeNodePtr.p->m_scanAncestorPtrI != RNIL &&
    data.m_frags_complete == data.m_fragCount) {
  jam();

  Ptr<TreeNode> scanAncestorPtr;
  ndbrequire(m_treenode_pool.getPtr(scanAncestorPtr,
                                    treeNodePtr.p->m_scanAncestorPtrI));

  if (scanAncestorPtr.p->m_bits & TreeNode::T_BUFFER_MATCH) {
    jam();

    // Find nearest outer join T_AGGREGATE_ANCESTOR parent (same as Phase 2)
    Uint32 parentNodeNo = RNIL;
    {
      Ptr<TreeNode> walkPtr;
      Uint32 parentPtrI = treeNodePtr.p->m_parentPtrI;
      while (parentPtrI != RNIL && parentPtrI != scanAncestorPtr.i) {
        ndbrequire(m_treenode_pool.getPtr(walkPtr, parentPtrI));
        if ((walkPtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) &&
            !(walkPtr.p->m_bits & TreeNode::T_INNER_JOIN)) {
          parentNodeNo = walkPtr.p->m_node_no;
          break;
        }
        parentPtrI = walkPtr.p->m_parentPtrI;
      }
    }

    RowIterator iter;
    for (first(scanAncestorPtr.p->m_rows, iter);
         !iter.isNull(); next(iter)) {
      jam();
      RowPtr parentRow;
      parentRow.m_src_node_ptrI = scanAncestorPtr.i;
      setupRowPtr(scanAncestorPtr, parentRow, iter.m_base.m_row_ptr);

      if (parentRow.m_matched == nullptr ||
          !parentRow.m_matched->get(treeNodePtr.p->m_node_no)) {
        jam();
        // Parent match check: skip if a higher-level ancestor
        // already handled null injection for this row
        if (parentNodeNo != RNIL &&
            (parentRow.m_matched == nullptr ||
             !parentRow.m_matched->get(parentNodeNo))) {
          jam();
          continue;
        }

        Uint32 err = propagateNullToAggLeaf(signal, requestPtr,
                                             treeNodePtr, parentRow);
        if (unlikely(err != 0)) {
          abort(signal, requestPtr, err);
          return;
        }
      }
    }
  }
}
```

**Key differences from the T_AGGREGATE_LEAF path**:

1. Uses `propagateNullToAggLeaf()` instead of `sendJoinAggNullRow()` — walks
   down the tree to find the leaf, checking for INNER JOIN blockers.
2. Includes parent match check (same as `handleAggAncestorLookupComplete()` in
   Phase 2) to prevent duplicate injection.
3. Does NOT use bitmask exchange — relies entirely on `m_matched` bits set by
   TRANSID_AI processing.
4. Does NOT need `T_AGG_INLINE_MATCH` to be explicitly set on the intermediate
   scan. The `m_matched` bits are already set by the general TRANSID_AI match
   tracking code enabled by `RT_AGG_ANCESTOR_MATCH`.

**Completion timing**: `data.m_frags_complete == data.m_fragCount` means ALL
fragments of this scan have reported completion. No more TRANSID_AI will arrive
for this scan. All match bits are final. This is the correct point to iterate
unmatched rows.

**No double-invocation risk**: Unlike lookup intermediates (where m_outstanding
can oscillate to 0 multiple times), scan completion fires ONCE — when all
fragments are done. The scan never restarts within the same batch cycle. So
the null injection happens exactly once per intermediate scan per batch.

### Step 4.3: validateAggregateFlags — Ensure Match Tracking for Scan Intermediates

**File: `DbspjMain.cpp`, `validateAggregateFlags()` line ~1837**

The current code already handles scan intermediates in the second loop
(which sets `T_BUFFER_ROW | T_BUFFER_MAP | T_BUFFER_MATCH` on the scan ancestor
and `RT_AGG_ANCESTOR_MATCH` on the request). However, verify that:

1. The loop covers scan nodes with `T_AGGREGATE_ANCESTOR` (not just lookup nodes).
   The current condition is:
   ```cpp
   if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) &&
       !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN))
   ```
   This correctly matches both scan and lookup intermediates.

2. The `break` after the first match still makes sense. Currently it breaks
   after finding the first (closest to root) outer join ancestor. With scan
   intermediates, there may be multiple scan intermediates at different levels.
   All of them share the same scan ancestor (the root scan), so one
   `T_BUFFER_MATCH` setup is sufficient.

3. `RT_AGG_ANCESTOR_MATCH` causes TRANSID_AI match tracking for ALL nodes
   (scan and lookup alike). This is already the case — the check at line 4031
   is `(requestPtr.p->m_bits & (RT_MULTI_SCAN | RT_AGG_ANCESTOR_MATCH))`.

**Expected result**: No code changes needed in `validateAggregateFlags()`.
The existing Phase 2 setup already covers scan intermediates.

### Step 4.4: Verify TRANSID_AI Match Tracking for Scan Children

The TRANSID_AI match tracking in `execTRANSID_AI` (line 4030-4054) uses
`treeNodePtr.p->m_scanAncestorPtrI` to find the scan ancestor and
`scanAncestorRow.m_src_correlation >> 16` to find the row position.

For a scan child (scanIndex), when a matching row is found, LDM sends
TRANSID_AI to DBSPJ with the child's treeNode and correlation. The correlation
includes the parent row position from the SCAN_FRAGREQ range key (set by
`scanFrag_fixupBound()` which encodes `corrVal` from `rowRef.m_src_correlation`).

**Verification needed**: Confirm that:
1. TRANSID_AI for scan child rows correctly sets `m_matched` bits on the scan
   ancestor's buffered rows.
2. The `corrVal >> 16` extraction gives the correct row position for the SCAN
   root's row buffer (not an intermediate scan's buffer).

This should work because the correlation flows through the same chain as for
lookup nodes: scan root row → `scanFrag_parent_row` → SCAN_FRAGREQ range key →
LDM → TRANSID_AI → `execTRANSID_AI` → `m_matched->set()`.

**For scan-scan-scan trees** (root scan → scan A → scan B → leaf), scan B's
`m_scanAncestorPtrI` points to scan A (not the root scan). The match tracking
code walks up through `scanAncestorPtrI` chain (line 4053:
`scanAncestorPtrI = scanAncestorPtr.p->m_scanAncestorPtrI`). So scan B's
TRANSID_AI would first set bits on scan A's buffered rows, then walk up to the
root scan's buffered rows. This means the root scan's `m_matched` bits are set
for BOTH scan A and scan B nodes.

However, for the completion-time check in Step 4.2, the intermediate scan (A)
uses `treeNodePtr.p->m_scanAncestorPtrI` which points to the ROOT scan. This
is correct — it iterates root scan rows and checks if scan A's bit is set.

**Potential issue**: In a scan-scan-scan tree, scan A's `m_scanAncestorPtrI` is
the root scan. But for `propagateNullToAggLeaf()`, the `rowRef` comes from the
root scan. The `parentLevelAdjust` computation walks from the leaf's parent up
to `childPtr.p->m_scanAncestorPtrI` (the leaf's scan ancestor). If the leaf's
scan ancestor is scan A (not the root), the `parentLevelAdjust` would be
computed relative to scan A, but the rowRef comes from the root scan. This
would be a mismatch.

**Resolution**: For scan-scan-scan trees, `propagateNullToAggLeaf()` needs to
use the same scan ancestor that the unmatched intermediate iterates. Since
`handleAggAncestorLookupComplete` and the new scan completion code both iterate
the root scan's rows, the rowRef is from the root scan. The
`parentLevelAdjust` must be the distance from the leaf's parent to the ROOT
scan (not to the leaf's scan ancestor). For test 4 (scan → LEFT scan → LEFT
lookup [leaf]), the leaf's scan ancestor IS the scan intermediate (not the
root), so `childPtr.p->m_scanAncestorPtrI` in `propagateNullToAggLeaf()` would
give the wrong anchor.

**Fix**: In `propagateNullToAggLeaf()`, compute `parentLevelAdjust` by walking
from `current` (the leaf's parent) up to the node whose `.i` matches
`rowRef.m_src_node_ptrI` (the actual source of the rowRef), instead of walking
up to the leaf's scan ancestor. This is already correct because the current
implementation walks until `walkPtr.i == scanAncestorPtr.i`, and for the
intermediate scan completion the rowRef comes from the scan ancestor that was
iterated (which IS `scanAncestorPtrI` of the intermediate scan, i.e., the
root scan). BUT for test 6 with two scan intermediates (root → scan A → scan B
→ leaf), scan B's m_scanAncestorPtrI is scan A. If scan A's completion handler
iterates root scan rows and passes rowRef from root scan, then
`propagateNullToAggLeaf()` would walk to scan B (leaf's parent), then up to the
leaf's scan ancestor (scan A), and compute levelAdjust = distance to scan A.
But rowRef is from root scan, not scan A!

**Corrected fix**: `propagateNullToAggLeaf()` should compute `parentLevelAdjust`
by walking from `current` to `rowRef.m_src_node_ptrI` (the actual row source),
NOT to `childPtr.p->m_scanAncestorPtrI`. Change the walk loop:

```cpp
// CURRENT code (walks to leaf's scan ancestor):
ndbrequire(m_treenode_pool.getPtr(scanAncestorPtr,
                                   childPtr.p->m_scanAncestorPtrI));
while (walkPtr.i != scanAncestorPtr.i) { ... }

// FIXED code (walks to the rowRef's source node):
while (walkPtr.i != rowRef.m_src_node_ptrI) {
  levelAdjust++;
  ndbrequire(walkPtr.p->m_parentPtrI != RNIL);
  ndbrequire(m_treenode_pool.getPtr(walkPtr, walkPtr.p->m_parentPtrI));
}
```

This is correct for ALL cases:
- Lookup intermediates: rowRef from root scan, walks from leaf's parent to root
- Scan intermediates: rowRef from root scan, walks from leaf's parent to root
- Nested scan-scan: rowRef from root scan, walks from leaf's parent to root

### Step 4.5: Tests (Already Written)

Tests 4 and 6 in `testMultiOuterJoinAggNdbApi.cpp` already exist and currently
report KNOWN LIMITATION. After Phase 4 implementation, they should report OK:

- **Test 4**: scan → LEFT scan → LEFT lookup [leaf] (4 groups expected)
- **Test 6**: scan → LEFT scan → LEFT scan → LEFT lookup [leaf] (3 groups expected)

Update the test output messages from "KNOWN LIMITATION" to "FAILED" once the
Phase 4 implementation is in place, or better, leave them as KNOWN LIMITATION
detection and verify they now pass.

### Implementation Order

| Step | Description | Risk |
|------|-------------|------|
| 4.4  | Verify TRANSID_AI match tracking for scan children | Low — read/debug only |
| 4.1  | NULL key path in scanFrag_parent_row | Low — mirrors lookup path |
| 4.3  | Verify validateAggregateFlags covers scan intermediates | Low — likely no changes |
| 4.2  | Completion-time null injection in scanFrag_execSCAN_FRAGCONF | Medium — new code path |
| 4.4* | Fix propagateNullToAggLeaf to walk to rowRef source node | Medium — affects all callers |

**Step 4.4\* (parentLevelAdjust fix)** is the most subtle. The change from
walking to `childPtr.p->m_scanAncestorPtrI` to walking to
`rowRef.m_src_node_ptrI` is correct for all existing callers because:
- Phase 2 lookup intermediates: rowRef always comes from the scan ancestor,
  and `childPtr.p->m_scanAncestorPtrI` == scan ancestor == `rowRef.m_src_node_ptrI`
- Phase 4 scan intermediates: same relationship holds when iterating the root
  scan's rows

However, this needs careful testing with all existing tests (1-3, 5, 7) to
ensure no regression.

### Risk Assessment

**Low risk**: Steps 4.1 and 4.3 are straightforward — they mirror existing
patterns from Phase 2.

**Medium risk**: Step 4.2 is new code in the scan completion path. The key
difference from lookup intermediates is that scan completion fires EXACTLY
ONCE (no oscillation risk), making it simpler than the lookup case.

**Medium risk**: Step 4.4* (parentLevelAdjust fix) affects the core
`propagateNullToAggLeaf()` function used by all callers. Must verify with
all existing passing tests.

**Low risk for multi-batch**: Scan intermediates complete when all fragments
are done — this is a final state, not a transient one. No risk of
re-triggering null injection in subsequent batches.

### Verification Checklist

1. Tests 1-3, 5, 7 in testMultiOuterJoinAggNdbApi still pass (no regression)
2. Tests 1-17 in testOuterJoinAggNdbApi still pass (2-way outer join)
3. Test 4 passes: scan → LEFT scan → LEFT lookup (4 groups)
4. Test 6 passes: scan → LEFT scan → LEFT scan → LEFT lookup (3 groups)
5. Test 7 still passes: scan → LEFT scan → INNER → LEFT (INNER blocks gap)

---

## Phase 5: General Linked Projection Support

Handle cases where the aggregate leaf's `attrParamPattern` references columns
from an unmatched intermediate node.

### Step 5.1: Detect Unreachable Pattern References

In `propagateNullToAggLeaf()`, before calling `sendJoinAggNullRow()`, check
whether the leaf's `attrParamPattern` references any node between the unmatched
node and the leaf. If so, those column values should be NULL.

### Step 5.2: Modify `expand()` for Null-Extended Rows

Add a `NullableNodes` bitmask parameter to `expand()` (or to `sendJoinAggNullRow`).
When expanding a `P_PARENT` reference that lands on a node in the NullableNodes
set, emit NULL values instead of trying to read the buffered row.

```cpp
Uint32 Dbspj::sendJoinAggNullRow(Signal *signal, Ptr<Request> requestPtr,
                                  Ptr<TreeNode> treeNodePtr,
                                  const RowPtr &rowRef,
                                  TreeNodeBitMask nullableNodes)  // NEW param
{
  // ... in expand() call, pass nullableNodes
  // When appendFromParent encounters a node in nullableNodes,
  // emit NULL-valued column data instead of reading buffered row
}
```

### Step 5.3: Build NullableNodes Bitmask

`propagateNullToAggLeaf()` builds the bitmask: set bits for the unmatched node
and all nodes between it and the leaf that are on the path.

---

## Phase 6: Complex Topologies

### Step 6.1: Sibling Outer Join Branches

If the query tree has branches (not just a linear chain), multiple children at
the same level may have `T_AGGREGATE_ANCESTOR`. The current design handles this
because `propagateNullToAggLeaf()` walks `m_child_nodes` and follows the path
marked with `T_AGGREGATE_ANCESTOR` / `T_AGGREGATE_LEAF`.

### Step 6.2: Multiple Aggregate Leaves

Currently the protocol supports exactly one aggregate leaf. If future work adds
support for multiple aggregate operations at different levels, the walk-down
logic would need to fan out to multiple leaves.

### Step 6.3: Deeply Nested Chains (5+ tables)

The walk-down approach scales to arbitrary depth. Each intermediate MatchAll
node that gets no match calls `propagateNullToAggLeaf()`, which walks all
the way down. No additional changes needed beyond Phase 1.

---

## Implementation Order

| Phase | Scope | Effort | Status |
|-------|-------|--------|--------|
| 1 | T_AGGREGATE_ANCESTOR flag + propagateNullToAggLeaf() | Medium | **DONE** |
| 2 | Completion-time match tracking for lookup ancestors | Medium | **DONE** |
| 3 | 4-way mixed join (INNER blockers) | Small | Already works (Phase 1) |
| 4 | Scan-scan intermediate nodes | Large | Not started |
| 5 | General linked projection | Medium | Not started |
| 6 | Complex topologies | Small | Not started |

**Phase 1 + 2 are implemented and compile.** Combined, they handle both NULL
key and KEYREF(626) cases for intermediate lookup ancestors via completion-time
match tracking. Phase 3 works automatically (INNER JOIN blocks propagation).
Phase 4 is the major follow-on work for scan intermediate nodes.

---

## Key Files to Modify

| File | Changes |
|------|---------|
| `Dbspj.hpp` | Add `T_AGGREGATE_ANCESTOR` flag; declare `propagateNullToAggLeaf()` |
| `DbspjMain.cpp` | Set `T_AGGREGATE_ANCESTOR` in build; add `propagateNullToAggLeaf()` helper; modify `lookup_parent_row` NULL key path; modify `lookup_execLQHKEYREF` KEYREF path; (Phase 4) modify `scanFrag_parent_row`; (Phase 4) modify `scanFrag_complete` |
| `testMultiOuterJoinAggNdbApi.cpp` | (Phase 4) Add scan-scan tests |

---

## Risk Assessment

**Completed**: Phase 1+2 — completion-time match tracking avoids the KEYREF
parent row recovery problem entirely. Uses existing match infrastructure
(T_BUFFER_MATCH, m_matched bitmask, execTRANSID_AI match setting).

**Medium risk**: Phase 4 (scan intermediate) — scan completion logic is complex
(bitmask exchange, multi-batch, fragment tracking). Extending it for intermediate
nodes requires careful design.

**Low risk**: Phase 5 (linked projections) — modifying `expand()` with a null
bitmask is conceptually simple but touches a foundational function.
