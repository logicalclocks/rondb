# SCAN-SCAN Join Batch Size Analysis

How batch size flows through multi-scan join trees in NDB pushed queries,
and how DBSPJ handles the potential combinatorial explosion.

## Single Scan (Root) with Lookup Children

In Q9 (bench_q9_ndbapi), lineitem is the only scan; all others are lookups.
Batch calculation is straightforward:
- Root scan batch: `batch_size_rows` (e.g., 990) ÷ `parallelism` (# parallel fragments)
- Each lookup child: triggered once per parent row (1:1 fanout)

## Multi-Scan: Combinatorial Explosion Risk

With SCAN-SCAN joins (e.g., lineitem scan → orders scan), the first scan
generates rows that trigger the second scan. Total in-flight work becomes
roughly the product of batch sizes:
- Parent scan: 100 rows/batch
- Child scan: 100 rows/batch
- Total work: up to 100 × 100 = 10,000 rows

## How DBSPJ Handles This

### 1. Fanout Estimation (DbspjMain.cpp:7231-7274)

`estmMaxKeys()` walks the tree and estimates how many keys can be sent to
each child scan before batch buffers overflow:

```
maxKeys = available_batch_rows / (fanout × fragment_count)
```

The recursive walk returns the **bottleneck** (minimum) across the entire tree.
If a leaf scan needs 1000 rows but its parent can only send 100 keys before
filling the buffer, the bottleneck is 100 keys.

Statistics are only available after the first batch (`m_recsPrKeyStat`).

### 2. Parallelism Division (DbspjMain.cpp:7139-7223)

`scanFrag_parallelism()` divides parallelism between sibling fragments
based on batch capacity:

```
per_fragment_rows = available_batch_rows / parallelism
```

Capped at `MAX_PARALLEL_OP_PER_SCAN_SPJ` (4000) per fragment.

### 3. Per-Fragment Batch Capping (DbspjMain.cpp:7337-7338)

Each fragment's SCAN_FRAGREQ gets:
```cpp
bs_rows = MIN(availableBatchRows / parallelism, MAX_PARALLEL_OP_PER_SCAN_SPJ);
bs_bytes = availableBatchBytes / parallelism;
```

For child scans, these are divided again by the child's parallelism.

### 4. DBSPJ Does NOT Automatically Divide Between Parent and Child

DBSPJ does not have a mechanism to split batch resources between a parent
scan and its child scan. Each scan node gets its own `batch_size_rows` from
the SCAN_FRAGREQ. The parent's batch determines how many keys flow to the
child, and the child's batch determines how many rows each key produces.

## NDB API Side

### Batch Calculation (NdbQueryOperation.cpp:4830-4911)

`calculateBatchedRows()` recursively traverses children:
- Each scan operation gets `m_maxBatchRows` set to `MIN(parent_limit, child_limit)`
- Lookup children constrain parent batch; scan children do NOT constrain parent
  (scan children return `0xffffffff` from `calculateBatchedRows`)

### 12-Bit Correlation ID Limit (Hard Ceiling)

Parent scan batch is capped at `4096 / fragsPerWorker`:
```cpp
if (m_children.size() > 0) {
  maxBatchRows = MIN(maxBatchRows, 0x1000 / fragsPerWorker);
}
```
With 4 fragments/worker → max 1024 parent rows per batch.

### Memory Allocation is Linear, Not Exponential

Memory per fragment = `sizeof(batch_buf) × num_operations × 2` (double-buffered).
For 3-way scan join with 100 rows each:
- Parent: 100 rows × 2 buffers
- Child1: 100 rows × 2 buffers
- Child2: 100 rows × 2 buffers
- Total: 600 row-batches per fragment (manageable)

The **work** is multiplicative (100 × 100 × 100), but **buffers** are linear.

## Recommendations for Aggregate SCAN-SCAN Queries

### 1. Explicitly Set Batch Sizes

```cpp
parent_op->setBatchSize(100);   // Parent: 100 rows/batch
child_op->setBatchSize(10);     // Child: 10 rows/batch
```
Total in-flight work: 100 × 10 = 1,000 rows (manageable).

### 2. Use Index Bounds on Child Scans

Replace unbound child scan with index range scan + bounds from parent columns.
This reduces child result set per parent key.

### 3. Let DBSPJ Fanout Statistics Adapt

After the first batch, DBSPJ has runtime statistics and adjusts parallelism
automatically. The first batch may be larger than optimal.

### 4. Monitor via scanParallelism

`scanParallelism` controls concurrent root SCAN_FRAGREQs through DBTC.
For SCAN-SCAN, this determines how many parallel trees execute simultaneously.

## Summary Table

| Scenario | DBSPJ Behavior | Practical Limit |
|----------|---------------|-----------------|
| SCAN-LOOKUP | Parent batch ÷ fragments | 12-bit correlation limit |
| SCAN-SCAN (unbound) | No auto-division between parent/child | Memory + correlation limit |
| SCAN-SCAN (bounded) | Fanout reduces parent batch via statistics | Parent batch (~100 rows) |
| Multi-SCAN chain | Recursive fanout bottleneck detection | Bottleneck scan's batch |

## Key Code Locations

- `DbspjMain.cpp:7104-7136` — `scanFrag_getBatchSize()`
- `DbspjMain.cpp:7139-7223` — `scanFrag_parallelism()`
- `DbspjMain.cpp:7231-7274` — `estmMaxKeys()` (fanout estimation)
- `DbspjMain.cpp:7276-7365` — `scanFrag_send()` (initial SCAN_FRAGREQ)
- `NdbQueryOperation.cpp:4830-4911` — `calculateBatchedRows()` (NDB API)
- `NdbReceiver.cpp:510-534` — `calculate_batch_size()` (NDB API)
- `ndb_limits.h:226` — `MAX_PARALLEL_OP_PER_SCAN_SPJ = 4000`
- `ndb_limits.h:231` — `DEF_BATCH_SIZE = 384`
