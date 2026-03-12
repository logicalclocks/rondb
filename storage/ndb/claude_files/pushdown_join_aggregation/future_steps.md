# Future Steps: Pushdown Join Aggregation

## Current State

RonSQL generates **linear chain** query trees for pushdown aggregation — a
single path from scan root through intermediate lookup/scan nodes to one
aggregate leaf. All nodes are on the aggregation path. The protocol enforces
exactly one aggregate leaf per query tree (T_AGGREGATE_LEAF on one node,
single AggInterpreter, single JoinAggregationState).

Sibling branches (nodes not on the aggregation path) are tolerated but serve
no purpose — their TRANSID_AI is silently consumed without processing.

## Multiple Aggregate Leaves

Multiple aggregate leaves would allow independent aggregation at different
nodes in the same query tree. Three scenarios where this matters:

### Scenario 1: Sibling Aggregations (Different Tables)

```sql
SELECT d.name,
       SUM(e.salary),      -- aggregate over employees
       SUM(p.budget)        -- aggregate over projects (independent table)
FROM dept d
LEFT JOIN emp e ON e.dept_id = d.id
LEFT JOIN project p ON p.dept_id = d.id
GROUP BY d.name
```

Query tree with two aggregate leaves:
```
dept (scan, root)
  ├── emp (lookup, agg leaf 1: SUM salary)
  └── project (lookup, agg leaf 2: SUM budget)
```

Today this requires two separate pushdown queries or handling the fan-out at
the MySQL handler level. With two aggregate leaves, the data node could
compute both sums in a single pass.

### Scenario 2: Multi-Level Aggregation (Different Granularity)

```sql
SELECT d.name,
       COUNT(DISTINCT e.id),  -- count at emp level
       SUM(t.hours)           -- sum at task level (finer granularity)
FROM dept d
LEFT JOIN emp e ON e.dept_id = d.id
LEFT JOIN task t ON t.emp_id = e.id
GROUP BY d.name
```

With the current single leaf at the task level, COUNT(DISTINCT e.id) is
problematic because the emp→task fan-out causes each employee to appear
multiple times. Would need:
```
dept (scan, root)
  └── emp (lookup, agg leaf 1: COUNT DISTINCT)
        └── task (lookup, agg leaf 2: SUM hours)
```

### Scenario 3: Correlated Subqueries With Aggregation

```sql
SELECT o.id,
       (SELECT COUNT(*) FROM items i WHERE i.order_id = o.id),
       (SELECT SUM(cost) FROM shipments s WHERE s.order_id = o.id)
FROM orders o
```

Each subquery has its own aggregation. Today RonSQL decorrelates these into
separate queries. With multiple aggregate leaves, both could run in a single
query tree.

## What RonDB Would Need for Multiple Aggregate Leaves

### 1. Flag-Setting (DBSPJ build phase)

Remove the `break` after first leaf in T_AGGREGATE_ANCESTOR setup
(DbspjMain.cpp, build phase). Each leaf walks UP via m_parentPtrI and marks
its own ancestor path. Nodes on multiple paths get the flag from both.

### 2. Multiple AggInterpreter Instances (DBLQH)

Each leaf needs its own aggregation program, its own JoinAggregationState,
its own GROUP BY hash map. The JOIN_AGG_SETUP signal would need to set up
N interpreters. Each LQHKEYREQ/SCAN_FRAGREQ would carry an aggStateKey
identifying which interpreter to use.

### 3. Null Propagation Fan-Out (DBSPJ)

`propagateNullToAggLeaf()` currently follows ONE child with
T_AGGREGATE_ANCESTOR and breaks. It would need to follow ALL children with
the flag, sending null rows to each leaf independently. The nullNodes
bitmask would need to be per-leaf.

### 4. Result Merging (NDB API)

The API currently expects one NdbAggregator result stream. With multiple
leaves, either the results are interleaved (with leaf IDs) or returned as
separate streams. The NdbQueryBuilder API would need to support multiple
setAggregation() calls on different operations.

### 5. Completion Tracking (DBSPJ)

`handleAggAncestorComplete()` currently handles one leaf's match tracking.
With multiple leaves, unmatched rows might need null injection to different
leaves independently.

### 6. JOIN_AGG_COMPLETE (DblqhProxy)

The finalize/merge step in DblqhProxy would need to merge results from N
interpreters, not just one. The TRANSID_AI result stream would need to
identify which leaf produced each result group.

## Effort Estimate

The single-leaf assumption is deeply embedded in the signal protocol
(JOIN_AGG_SETUP/COMPLETE/RELEASE), the AggInterpreter setup, the DBSPJ
flag propagation, and the NDB API result path. This is a natural extension
of the current architecture but represents a significant amount of work
across all layers (DBLQH, DblqhProxy, DBSPJ, NDB API, RonSQL).
