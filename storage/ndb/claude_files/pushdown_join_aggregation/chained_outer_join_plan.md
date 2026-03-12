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

## Phase 4: Scan-Scan Intermediate Nodes

Extend to handle cases where intermediate nodes are scans (not lookups).

### Step 4.1: Modify `scanFrag_parent_row` (NULL Key Path)

**File: `DbspjMain.cpp`, line ~7364**

Same pattern as Step 1.3: add `T_AGGREGATE_ANCESTOR` check after existing
`T_AGGREGATE_LEAF` check:

```cpp
if ((treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_ANCESTOR) &&
    !(treeNodePtr.p->m_bits & TreeNode::T_INNER_JOIN)) {
  jam();
  err = propagateNullToAggLeaf(signal, requestPtr, treeNodePtr, rowRef);
  if (unlikely(err != 0)) break;
}
```

### Step 4.2: Modify `scanFrag_complete` for Intermediate Scan Nodes

When an intermediate scan node with `T_AGGREGATE_ANCESTOR` (not `T_AGGREGATE_LEAF`)
completes, it must check for parent rows that had no child scan match and
propagate null rows to the aggregate leaf.

This is similar to the existing scan completion logic at line 8799, but for
ancestors instead of leaves. The match tracking (bitmask exchange or inline
match) must work per-intermediate-node.

**This is significantly more complex** because:
- The bitmask exchange protocol (`JOIN_AGG_MATCH_REQ/CONF`) is specific to
  the aggregate leaf's DBLQH state
- Inline match bits (`m_matched`) are set per-node, so they can track intermediate
  nodes too
- A new bitmask exchange mechanism (or reuse of inline match) is needed for
  intermediate scan ancestors

**Recommendation**: For Phase 4, require `T_AGG_INLINE_MATCH` mode for queries
with intermediate outer join scan nodes. The inline match bits already support
per-node tracking via `m_matched->set(treeNodePtr.p->m_node_no)`.

### Step 4.3: Add New Tests

Add scan-scan intermediate outer join tests to `testMultiOuterJoinAggNdbApi`:
- Test 3: `scan → LEFT scan → LEFT lookup [leaf]` (requires index on intermediate)
- Test 4: `scan → LEFT scan → LEFT scan [leaf]` (all scans)

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
