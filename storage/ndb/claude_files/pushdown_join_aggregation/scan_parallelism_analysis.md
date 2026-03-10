# scanParallelism Analysis

How `scanParallelism` in SCAN_TABREQ affects each layer of the NDB stack.

## DBTC (Coordinator)

`scanParallelism` controls **ScanFragRec allocation** — the number of handles for
concurrent fragment scans.

### Signal Reading (DbtcMain.cpp:15695)

For JoinAgg queries, read directly from `scanTabReq->scanParallelism`.
For regular queries, derived from the number of receiver IDs in section 0.

### ScanFragRec Allocation (DbtcMain.cpp:16398)

`scanParallel` ScanFragRecs are seized in a loop. This is the **hard limit** on
concurrent SCAN_FRAGREQs sent to DBSPJ.

### DIH Overwrite (DbtcMain.cpp:16677)

After DIH returns the actual fragment count (`tfragCount`):
```cpp
scanptr.p->scanParallel = tfragCount;
scanptr.p->scanNoFrag = tfragCount;
```
This overwrites the parallelism value but does NOT change the number of
already-allocated ScanFragRecs.

### sendFragScansLab (DbtcMain.cpp:17330-17530)

Iterates over fragment locations, pairing each with an IDLE ScanFragRec:

- **scanParallelism >= tfragCount**: All fragments get a ScanFragRec. Excess
  ScanFragRecs become "empty result" replies (lines 17496-17525) — they get
  `m_ops=0, m_hasMore=0` and are queued for delivery as empty SCAN_TABCONFs.

- **scanParallelism < tfragCount**: Only `scanParallelism` SCAN_FRAGREQs can be
  in-flight simultaneously. Remaining fragments wait until a ScanFragRec is freed
  via SCAN_NEXTREQ after SCAN_FRAGCONF (sequential fragment scanning).

### Batch Size

Completely independent of scanParallelism. Set from `ScanTabReq::getScanBatch(ri)`
and `batch_byte_size` (DbtcMain.cpp:16332-16344).

## NDB API (Sender)

For aggregate queries: `scanParallelism = m_workerCount * m_fragsPerWorker = rootFragments`.

### What it determines

- **`m_workerCount`** (NdbQueryOperation.cpp:3099): Number of `NdbWorker` objects,
  each with an `NdbResultStream` and `NdbReceiver`
- **`m_numAggReceivers`** (NdbQueryOperation.cpp:3225): One aggregation receiver
  per worker for group routing
- **Buffer allocation** (NdbQueryOperation.cpp:3135): `rootFragments * totalBuffSize`
- **Section 0 size** (NdbQueryOperation.cpp:3590): `m_workerCount` receiver IDs

### Aggregate queries force m_fragsPerWorker = 1

NdbQueryOperation.cpp:3041-3049 disables multi-fragment bundling for aggregate
queries so each fragment gets its own DBSPJ instance across TC threads.

## DBSPJ (Executor)

**DBSPJ does NOT read `scanParallelism` from incoming signals.** It has its own
independent, adaptive parallelism per TreeNode.

### scanFrag_parallelism() (DbspjMain.cpp:7148-7223)

Calculates parallelism based on:
- `batchSizeRows`: rows expected in current batch
- `data.m_fragCount - data.m_frags_complete`: remaining fragments
- `requestPtr.p->m_rootFragCnt`: root fragment count
- Runtime statistics (`m_recsPrKeyStat`): estimated rows per key for fanout

Algorithm:
```
maxParallelism = MIN(batchSizeRows, frags_not_complete)
minParallelism = MIN(m_rootFragCnt, maxParallelism)

If T_SCAN_PARALLEL flag → use maxParallelism
If no statistics → use minParallelism
With statistics → estimate based on RecsPrKey fanout, clamp to [min, max]
```

### Recalculated per batch

- Initial: `scanFrag_send()` (line 7328)
- After SCAN_FRAGCONF: `scanFrag_execSCAN_FRAGCONF()` (line 8269)
- After SCAN_NEXTREQ: `scanFrag_execSCAN_NEXTREQ()` (lines 8359-8376)

### JoinAgg interaction

JoinAgg flag affects **result routing** (to aggregation engine vs API), not
parallelism. Root scan has JoinAggFlag cleared before sending to DBLQH (line 7412);
only child lookups retain it.

## Summary Table

| Level      | What scanParallelism controls                                    |
|------------|------------------------------------------------------------------|
| **DBTC**   | Number of concurrent ScanFragRec handles → max root SCAN_FRAGREQs |
| **NDB API**| Worker count, receiver count, buffer allocation                  |
| **DBSPJ**  | **Nothing** — uses its own adaptive parallelism per TreeNode     |

For aggregate queries with `scanParallelism = rootFragments`, all root fragments
scan concurrently through DBTC. DBSPJ then independently decides child scan
parallelism. The value only matters when `scanParallelism < tfragCount` — it
serializes some root fragment scans.
