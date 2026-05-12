# CTE_LOOKUP_REQ / CTE_SCAN_REQ Role Coverage Matrix

Goal: ensure each CTE op (CTE_LOOKUP_REQ, CTE_SCAN_REQ) is exercised
in every meaningful role × context combination by `testCteNdbApi`.

CTEs without aggregation are deferred to a future step — every CTE
in this matrix has an aggregator.

## Roles

For an op X in a query subtree:

| Role | Definition |
|---|---|
| **Root non-leaf** | X has no parent (top of subtree) AND has at least one child driven by its rows |
| **Internal** | X has a parent (driven by parent rows) AND has at least one child below |
| **Leaf with agg** | X is a leaf (no children below it) AND carries `setAggregation()` — its result feeds a `JoinAggInterpreter` (either an enclosing CTE's aggregator or the main query aggregator) |
| **Leaf without agg** | X is a leaf AND does NOT carry `setAggregation()` — its result is delivered as TRANSID_AI to API (main query) or DBSPJ (intermediate).  Only meaningful in the main SELECT context — a leaf without agg in CTE materialization has nothing to do, deferred. |

Notes:
- For CTE_LOOKUP, the root non-leaf form requires a *constant* key
  (no parent row to drive a `linkedValue`).  The lookupCte returns
  at most one row, and that row drives child operations.
- For CTE_SCAN, the natural form is root.  An internal scanCte
  (re-scan the entire CTE for each parent row) is semantically
  cartesian-product and not currently supported by the planner —
  excluded from the matrix.

## Contexts

| Context | Definition |
|---|---|
| **CTE materialization (CTE input)** | The op lives inside a CTE subtree and produces rows that ultimately feed the enclosing CTE's hash table (either via an aggregate leaf or by being the aggregate leaf itself).  This is the "CTE as input to a new CTE" case. |
| **Main SELECT** | The op lives in the main query tree.  Its rows ultimately go to the API. |

## Existing coverage (Tests 1–10)

| Op | Role | Context | Test |
|---|---|---|---|
| CTE_LOOKUP | Leaf no-agg | Main | T2 (main `lookupCte(0)` keyed by linkedValue from main scan) |
| CTE_LOOKUP | Leaf with-agg | Main | T3 (main `lookupCte(0)` is `T_AGGREGATE_LEAF`, feeds main aggregator) |
| CTE_LOOKUP | Leaf with-agg | CTE materialization | T5 (`lookupCte(0)` inside CTE 1 subtree, feeds cte1 aggregator) |
| CTE_LOOKUP | Leaf with-agg | CTE materialization (chained) | T7 (3-level chain, each CTE's aggregator fed by a `lookupCte` agg-leaf) |
| CTE_LOOKUP | Leaf with-agg | Main + CTE | T6 (main and CTE both have agg-leaf `lookupCte`) |
| CTE_SCAN | Leaf no-agg / Root | Main | T8 (`scanCte(0)` is the only main op) |
| CTE_SCAN | Root non-leaf | Main | T9 (`scanCte(0)` outer of self-join, drives child `lookupCte`) |
| CTE_SCAN | Leaf with-agg / Root | CTE materialization | T10 (`scanCte(0)` is CTE 1's agg leaf, feeds cte1 aggregator) |

## Gaps to fill (ignoring "leaf without agg" in CTE materialization)

### CTE_LOOKUP_REQ

| Role | Main SELECT | CTE materialization |
|---|---|---|
| Root non-leaf | **NEW: T11** | **NEW: T12** |
| Internal | **NEW: T13** | **NEW: T14** |
| Leaf with agg | T3 ✓ | T5/T6/T7 ✓ |
| Leaf without agg | T2 ✓ | (deferred) |

### CTE_SCAN_REQ

| Role | Main SELECT | CTE materialization |
|---|---|---|
| Root non-leaf | T9 ✓ | **NEW: T16** |
| Internal | n/a (cartesian) | n/a |
| Leaf with agg | **NEW: T15** | T10 ✓ |
| Leaf without agg | T8 ✓ | (deferred) |

(Tests renumbered after dropping the two "leaf without agg in CTE
materialization" entries.)

## Test header requirement

Every new test below MUST have a comment header that documents:

1. The SQL equivalent of the query.
2. The shape of the built tree (parent/child relationships per node).
3. The role each node plays (CTE materialization root, agg leaf,
   main query root, etc.) AND the TreeNode flag bits expected on it
   (`T_CTE_SCAN`, `T_AGGREGATE_LEAF`, `T_USER_PROJECTION`,
   `T_INNER_JOIN`, `T_CTE_INDIRECT_FEED`, etc.).
4. Step-by-step execution: which phase processes which CTE, which
   rows are fetched at each step, where rows go (API / DBSPJ /
   aggregator via joinAggStateKey), and what the final result set
   looks like.
5. Expected row counts at each level (CTE materialization sizes,
   main result row count).

The header is the first thing read when a test fails — vague
headers force re-deriving the intent from the build code, which
slows debugging.  See `MEMORY.md` for the full guideline.

## New test specifications

### T11 — `lookupCte` as main query root (constant key) with child

Tree:
```
lookupCte(0, key=constInt(2))         -- main root, T_USER_PROJECTION
  ↳ readTuple(cte_src, key=linkedValue(mainLookup, "grp"))
                                        -- child leaf, T_LEAF
```

Expected flags:
- main `lookupCte`: T_USER_PROJECTION (delivers to API), m_cteId=RNIL
- child `readTuple`: T_LEAF, parent=mainLookup

Expected execution:
1. CTE 0 materialization phase (Test 2 pattern) populates cte0 with
   `{(1,30),(2,70),(3,50)}`.
2. Main query starts, fragment-per-node skip applies (only one
   fragment runs the main query).
3. The `lookupCte(0)` is a single LQHKEYREQ-style root — DBSPJ sends
   one CTE_LOOKUP_REQ with constant key `grp=2`.  DBLQH returns the
   matching cte0 row (grp=2, total=70) via FLUSH_AI to API.
4. The child `readTuple(cte_src, pk=linkedValue("grp"))` fires for
   that one row: pk=2, returns row `(pk=2, grp=1, val=20)` — the
   cte_src row whose pk equals the cte0 grp value.
5. API receives one main row with the joined columns from both ops.

Expected result: 1 row `[cte0.grp=2, cte0.total=70, cte_src.pk=2,
cte_src.grp=1, cte_src.val=20]`.

### T12 — `lookupCte` as CTE root (constant key) with child

Tree:
```
CTE 0 subtree:
  scanTable(cte_src) → readTuple(cte_src, agg leaf) -- standard cte0 from T2
CTE 1 subtree:
  lookupCte(0, key=constInt(1))           -- subtree root, T_CTE_SCAN bit
                                              (because it's the materialization
                                              root for CTE 1)
    ↳ readTuple(cte_src, key=linkedValue(cte1Lookup, "grp"),
                T_AGGREGATE_LEAF, setAggregation(cte1Agg))
Main query:
  scanCte(1)                              -- read cte1, deliver to API
```

Expected flags:
- cte1Lookup: T_CTE_SCAN (subtree materialization root —
  cte_scan_build / cte_lookup_build needs to recognize this case
  the same way T_AGGREGATE_LEAF + nested CTE_SCAN does in
  Test 10's build path), m_cteId=1
- child readTuple: T_AGGREGATE_LEAF, m_cteId=1, parent=cte1Lookup
- main scanCte(1): T_USER_PROJECTION, m_cteId=RNIL

Expected execution:
1. Phase 0: CTE 0 materializes → `{(1,30),(2,70),(3,50)}`.
2. Phase 1: each DBSPJ instance runs CTE 1's subtree.
   - The cte1Lookup root fires once with constant key grp=1 →
     returns cte0 row `(grp=1,total=30)`.
   - The child readTuple fires with pk=1 → returns cte_src row
     `(pk=1, grp=1, val=10)`.
   - The readTuple's aggregator (cte1Agg) inserts one row into
     cte1's hash table.  cte1 now has one group.
3. Main scanCte(1) reads cte1 (one group) and delivers to API.

Expected result: 1 row in cte1, 1 row delivered to API.

### T13 — `lookupCte` as main query internal

Tree:
```
scanTable(cte_src)                              -- main root scan
  ↳ lookupCte(0, key=linkedValue(mainScan,"grp"))
                                                  -- internal: parent + child
      ↳ readTuple(cte_src, key=linkedValue(cteLookup,"grp"))
                                                  -- leaf
```

Expected flags:
- mainScan: m_cteId=RNIL, has children
- cteLookup: parent=mainScan, has child=readTuple, m_cteId=RNIL,
  T_USER_PROJECTION (delivers row), NOT T_AGGREGATE_LEAF
- readTuple leaf: parent=cteLookup, T_LEAF

Expected execution:
1. CTE 0 materializes (5 src rows → 3 groups).
2. Main scan fires one SCAN_FRAGREQ per fragment (with the
   fragment-per-node skip applied for cte_src).
3. For each main scan row, the cteLookup fires keyed by
   `mainScan.grp`.  Result: the cte0 row for that grp.
4. For each cte0 row, the readTuple fires keyed by
   `cteLookup.grp` (= cte0 row's grp).  Result: the cte_src row
   whose pk equals that grp.
5. API receives 5 rows `[mainScan.*, cteLookup.*, readTuple.*]`.

Expected result: 5 rows.

### T14 — `lookupCte` as CTE materialization internal

Tree (CTE 1 subtree):
```
scanTable(cte_src)                              -- subtree root
  ↳ lookupCte(0, key=linkedValue(cte1Scan,"grp"))
                                                  -- internal
      ↳ readTuple(cte_src, key=linkedValue(cteLookup,"grp"),
                  T_AGGREGATE_LEAF, setAggregation(cte1Agg))
                                                  -- leaf, agg leaf
Main query:
  scanCte(1)                                      -- read cte1
```

Expected flags:
- cte1Scan: m_cteId=1, T_CTE_SCAN
- cteLookup: parent=cte1Scan, has child=readTuple, m_cteId=1,
  NOT T_AGGREGATE_LEAF (just an intermediate, not a leaf)
- readTuple: parent=cteLookup, T_AGGREGATE_LEAF, m_cteId=1
- main scanCte(1): T_USER_PROJECTION, m_cteId=RNIL

Expected execution:
1. Phase 0: CTE 0 materializes → 3 groups.
2. Phase 1: CTE 1 subtree on each node.
   - cte1Scan reads cte_src locally (5 rows total).
   - For each row, cteLookup fires (5 × 1 = 5 cte0 lookups), each
     returns the corresponding cte0 row.
   - For each cte0 row, readTuple fires keyed by cte0 grp, returns
     the matching cte_src row.
   - readTuple is the agg leaf — inserts 5 rows into cte1.
3. cte1 = 3 groups (assuming cte1Agg groups by some column).
4. Main scanCte(1) reads cte1 and delivers to API.

Expected result: 3 cte1 groups, 3 main rows.

### T15 — `scanCte` as main query leaf with agg

Tree:
```
scanCte(0)                                  -- main root + leaf
  with setAggregation(mainAgg)              -- T_AGGREGATE_LEAF, T_USER_PROJECTION
```

Expected flags:
- main scanCte(0): T_AGGREGATE_LEAF, T_USER_PROJECTION, m_cteId=RNIL,
  data.m_joinAggStateKey points to the *main* aggStateKey (NOT a CTE
  aggStateKey — exercises the `m_aggStateKeys[targetNodeId]` branch
  in cte_scan_start that T10 doesn't hit because T10 uses the
  enclosing CTE's aggStateKey)

Expected execution:
1. CTE 0 materializes via Test 2 pattern → 3 groups.
2. Main query starts.
3. scanCte(0) walks cte0 locally on each data node.
4. The walker's agg-feed branch (joinAggStateKey != RNIL) inserts
   each group into the main aggregator's local state.
5. After all main scan workers complete, the main aggregator's
   evicted/final groups are delivered to API.

Expected result: depends on mainAgg shape.  Simplest: GROUP BY
none, SUM(total) = 30+70+50 = 150.  One row, `{sum=150}`.

### T16 — `scanCte` as CTE materialization root non-leaf with child

Tree (CTE 1 subtree):
```
scanCte(0)                                  -- subtree root
  ↳ readTuple(cte_src, key=linkedValue(scanCte,"grp"),
              T_AGGREGATE_LEAF, setAggregation(cte1Agg))
                                              -- leaf, agg leaf
Main query:
  scanCte(1)
```

Expected flags:
- cte1's scanCte(0): m_cteId=1, NOT T_AGGREGATE_LEAF (data source
  only, drives the child).  Needs `T_CTE_SCAN` set so phase startup
  picks it up.  May need a new build branch — the existing code at
  line 6580 (T10 path) sets T_CTE_SCAN only for the
  T_AGGREGATE_LEAF case.  T16 is "scanCte at subtree root, NOT
  agg leaf".
- child readTuple: parent=scanCte, T_AGGREGATE_LEAF, m_cteId=1
- main scanCte(1): T_USER_PROJECTION, m_cteId=RNIL

Expected execution:
1. Phase 0: CTE 0 materializes → 3 groups.
2. Phase 1: each DBSPJ instance runs CTE 1's subtree.
   - scanCte(0) walks local cte0 partition (3 groups on each node
     in the local-only case; but for single-node test this is
     simply 3 groups).
   - Walker emits TRANSID_AI back to DBSPJ for each group (NOT the
     agg-feed path — joinAggStateKey is RNIL since scanCte itself
     has no setAggregation).
   - DBSPJ drives the child readTuple for each parent row.
   - readTuple fires with pk=parent.grp.
   - readTuple's agg-feed inserts one row into cte1 per parent row.
3. cte1 = 3 groups (one per cte0 group).
4. Main scanCte(1) reads cte1 and delivers to API.

Expected result: 3 main rows.

Implementation note: T16 may require build-loop changes to mark
the scanCte at a CTE subtree root as T_CTE_SCAN even when it's
NOT the agg leaf (the T10 fix only handles the agg-leaf case).
That extension is part of T16.

## Implementation order

Lowest risk first — leaf and root variants before internal variants
(internal needs both parent linking and child driving), and main-SELECT
variants before CTE-materialization variants (simpler subtree shape):

1. **T11** — main lookupCte root + child readTuple (constant key)
2. **T15** — scanCte as main agg leaf (exercises `m_aggStateKeys` branch)
3. **T13** — main lookupCte internal
4. **T12** — CTE materialization lookupCte root + child
5. **T14** — CTE materialization lookupCte internal
6. **T16** — CTE materialization scanCte root non-leaf with child
   (may need build-loop extension for non-agg-leaf scanCte at
   subtree root)

After T11–T16 are passing, multi-node validation (item 3 from the
overall plan) becomes the next step.
