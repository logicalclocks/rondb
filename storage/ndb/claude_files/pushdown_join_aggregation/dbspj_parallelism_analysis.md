# DBSPJ Scan Chain Parallelism Analysis

## Observation

In a 3-level scan chain `customer → orders → lineitem` with DBSPJ
instance (2) on node 2:

```
scanFrag_CONF node=0 frag=0 rows=1 done=2 frags_complete=0/1  ← customer frag 0 done
scanFrag_send FRAGREQ node=1 frag=0 rangeCnt=1                 ← orders starts IMMEDIATELY
...8 order SCAN_FRAGREQs sent...
scanFrag_CONF node=0 frag=6 rows=1 done=2 frags_complete=0/1  ← customer frag 6 done
scanFrag_send FRAGREQ node=1 frag=6 rangeCnt=1                 ← orders starts AGAIN
...8 more order SCAN_FRAGREQs sent...
```

Two separate sets of 8-fragment order scans running simultaneously.

## Root Cause: m_fragCount=1 Per DBSPJ Instance

The key log line: `frags_complete=0/1` — the DBSPJ instance has
`m_fragCount=1` for the root scan. Each instance processes ONE root
fragment at a time.

With `scanParallelism=8`, DBTC distributes the 8 root scan fragments
across DBSPJ instances. Each instance handles its assigned fragments
sequentially. When its single fragment completes:

1. `m_frags_complete == m_fragCount` (1 == 1) → node INACTIVE
2. `reportAncestorsComplete` → `parent_batch_complete(orders)`
3. Orders SCAN_FRAGREQ sent for all 8 order fragments
4. DBTC sends SCAN_NEXTREQ for the next customer fragment
5. New customer fragment arrives → new cycle begins
6. Orders SCAN_FRAGREQ sent AGAIN for all 8 order fragments

**Result**: Two (or more) concurrent order scan batches, each from a
different customer fragment.

## Is This By Design?

**Yes — this is DBSPJ's pipelined execution model.**

For non-aggregation joins, this is correct and efficient:
- Each parent row triggers child operations immediately
- Results stream to the API without waiting for all parents
- Maximizes throughput by overlapping scan I/O

For aggregation with outer join match tracking, this creates a problem:
- `m_agg_range_cnt` resets between customer fragment cycles
- Orders from different customer fragments get the same range_no
- Match bitmask can't distinguish them → groups lost

## Sequence Diagram

```
DBTC            DBSPJ(2)         DBLQH (orders frags)    DBLQH (lineitem frags)
  |               |                    |                      |
  |--SCAN_FRAGREQ→|  (customer frag 0) |                      |
  |               |                    |                      |
  |               |←SCAN_FRAGCONF--    |  customer frag 0: 1 row (cust_id=3)
  |               |  m_fragCount=1     |
  |               |  done → INACTIVE   |
  |               |                    |
  |               |  parent_batch_complete(orders)
  |               |--SCAN_FRAGREQ×8--→ |  orders: bound cust_id=3
  |               |                    |  → returns orders 30, 31
  |               |                    |
  |               |  orders rows trigger scanFrag_parent_row(lineitem)
  |               |  order 30: range_no=0
  |               |  order 31: range_no=1
  |               |                    |
  |               |  parent_batch_complete(lineitem)
  |               |----SCAN_FRAGREQ×8--|--------------------→ | lineitem scan
  |               |                    |                      | rangeCnt=2
  |               |                    |                      |
  |--SCAN_NEXTREQ→|  (customer frag 6) |                      |
  |               |                    |                      |
  |               |←SCAN_FRAGCONF--    |  customer frag 6: 1 row (cust_id=1)
  |               |  done → INACTIVE   |
  |               |                    |
  |               |  parent_batch_complete(orders)
  |               |--SCAN_FRAGREQ×8--→ |  orders: bound cust_id=1
  |               |                    |  → returns orders 10, 11
  |               |                    |
  |               |  orders rows trigger scanFrag_parent_row(lineitem)
  |               |  order 10: range_no=0  ← COLLISION with order 30!
  |               |  order 11: range_no=1  ← COLLISION with order 31!
  |               |                    |
  |               |  parent_batch_complete(lineitem)
  |               |----SCAN_FRAGREQ×8--|--------------------→ | lineitem scan #2
  |               |                    |                      | rangeCnt=2
  |               |                    |                      |
```

## Impact on Aggregation

The lineitem AggInterpreter has a SINGLE shared hash map across all
batches. Groups are keyed by order_id (via linked projection from
orders). The range_no collision doesn't affect the GROUP BY hash map
(order_ids are unique). But the outer join match bitmask uses range_no,
causing incorrect NULL row injection when checked at completion.

The GROUP BY aggregation itself works correctly — the hash map produces
the right groups. The bug is specifically in the outer join NULL row
injection path (handleAggLeafScanComplete) which uses range_no to
determine which parent rows had matches.

## Fix Options

### Option A: Cumulative range_no across customer fragments

Don't reset `m_agg_range_cnt` between customer fragment cycles.
Each order gets a globally unique range_no. At final completion,
iterate ALL parent rows against the cumulative bitmask.

**Challenge**: Bitmask size must accommodate ALL ranges across all
cycles. With many customer fragments × many orders each, the bitmask
could be large. Also, DBLQH allocates the bitmask per SCAN_FRAGREQ
based on rangeCount — it would need the cumulative count.

### Option B: Per-cycle NULL injection

After each customer fragment cycle completes (orders scan done,
lineitem scan done), inject NULL rows for that cycle's unmatched
parent rows. Reset range_no for the next cycle.

**Challenge**: Need to know when a cycle ends vs when the overall
scan ends. The lifecycle is: customer frag N → orders scan → lineitem
scan → all complete → SCAN_NEXTREQ → customer frag N+1. The per-cycle
injection must happen at the "all complete" point.

### Option C: Disable pipelining for aggregation

For aggregate queries, wait for ALL customer fragments before starting
orders. This eliminates the multi-cycle issue.

**Challenge**: Changes the execution model significantly. May hurt
performance for large datasets.
