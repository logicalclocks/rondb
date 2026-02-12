# NDB Block Unit Test Guide

Guide for writing unit tests that send raw signals to NDB kernel blocks
using the SignalSender API. Based on lessons learned from testJoinAgg.

## Setup

### Starting a Cluster (MTR)

```bash
cd debug_build/mysql-test
./mtr --start-and-exit ndb_basic
```

This starts a cluster with connect string `localhost:13010` and mysqld
on port 13011. The cluster configuration includes 2 data nodes and
typically 4 LDM threads per node.

### SignalSender Basics

```cpp
#include <ndbapi/SignalSender.hpp>

Ndb_cluster_connection conn("localhost:13010");
conn.connect(12, 5, 1);
conn.wait_until_ready(30, 0);

SignalSender ss(&conn);
ss.lock();  // REQUIRED before any sendSignal/waitFor

// ... send signals, wait for responses ...

ss.unlock();  // REQUIRED before cleanup
```

**Critical**: `ss.lock()` and `ss.unlock()` are mandatory around all
signal operations. Forgetting `ss.lock()` causes:
```
Assertion failed: (m_poll.m_locked) in raw_sendSignal
```

### Sending Signals

```cpp
SimpleSignal ssig;
Uint32 *data = ssig.getDataPtrSend();

// Fill signal data...

ssig.set(ss, /*trace=*/0, recBlock, GSN_xxx, sigLen);
ssig.header.m_noOfSections = N;
ssig.ptr[0].p = sectionData;
ssig.ptr[0].sz = sectionLen;

ss.sendSignal(nodeId, &ssig);
```

### Waiting for Responses

```cpp
SimpleSignal *sig = ss.waitFor(/*timeout_ms=*/5000);
int gsn = sig->readSignalNumber();
const Uint32 *data = sig->getDataPtr();
```

## Block References and Instance Numbers

### The V_QUERY Virtual Block

When sending scan or key operations to data nodes, use `V_QUERY` (0x111)
instead of `DBLQH` (247). V_QUERY dynamically routes the signal to the
correct LDM thread based on load balancing.

**Important**: V_QUERY requires a valid LDM instance number. Sending to
V_QUERY with instance 0 causes a segfault in `Trpman::distribute_signal`
because `get_scan_fragreq_ref` accesses `m_rr_group[instance_no - 1]`.

### Encoding Instance Numbers

The `recBlock` parameter in `ssig.set()` is a 16-bit value that encodes
both block number and instance. Use `numberToBlock()` from RefConvert.hpp:

```cpp
#include <kernel/RefConvert.hpp>

// Create block number with instance for V_QUERY
Uint16 recBlock = numberToBlock(V_QUERY, ldmInstance);
ssig.set(ss, 0, recBlock, GSN_SCAN_FRAGREQ, sigLen);
```

### Finding LDM Instance for a Fragment

The LDM instance that owns a fragment can be queried via ndbinfo tables
through the mysqld SQL interface (default port 13011 in MTR):

```sql
-- Get fragment-to-LDM-instance mapping
SELECT fragment_num, block_instance
FROM ndbinfo.operations_per_fragment
WHERE table_id = <table_id> AND node_id = <target_node>
GROUP BY fragment_num, block_instance;

-- Alternative: table_fragments table
SELECT * FROM ndbinfo.table_fragments
WHERE table_id = <table_id>;
```

The `block_instance` value is the LDM instance number (1-based) to use
with `numberToBlock(V_QUERY, block_instance)`.

## Signal Construction Tips

### ScanFragReq

```cpp
ScanFragReq *scanReq = reinterpret_cast<ScanFragReq *>(data);
scanReq->senderData = fragId;
scanReq->resultRef = ss.getOwnRef();
scanReq->tableId = tableId;
scanReq->fragmentNoKeyLen = fragId;
scanReq->schemaVersion = schemaVersion;
scanReq->transId1 = transId1;
scanReq->transId2 = transId2;

// Use helper methods for requestInfo bits
Uint32 requestInfo = 0;
ScanFragReq::setReadCommittedFlag(requestInfo, 1);
ScanFragReq::setCorrFactorFlag(requestInfo, 1);
ScanFragReq::setJoinAggFlag(requestInfo, 1);
scanReq->requestInfo = requestInfo;
```

### AttrInfo / Aggregation Programs

Aggregation programs follow the NdbAggregator wire format:

- Group-by columns: stored as `attrId << 16` (AttributeHeader format with
  attrId in bits 16-31), NOT bare column IDs
- `kOpLoadCol` column ID: uses `getAttrId()` in bits 0-15 (no shift)
- Use `kOpSum` (generic, value=10), not `kOpSumBigint` (type-specific) —
  AggInterpreter resolves the type internally
- `NDB_TYPE_BIGINT = 9` (not 8)
- `sizeof(AggResItem)` = 24 bytes on 64-bit (alignment padding)

### Result Parsing (TRANSID_AI)

Join aggregation results arrive as TRANSID_AI signals:

**Non-group-by result** (e.g., `SELECT COUNT(*) FROM t`):
- Word 2: `n_groups = 0` but one data entry still follows
- Entry format: `key_len=0`, `val_len=agg_bytes`

**Group-by result** (e.g., `SELECT SUM(b) FROM t GROUP BY a`):
- Each TRANSID_AI has exactly `n_groups=1`
- Multiple groups arrive as separate TRANSID_AI signals
- Key format: `[AttributeHeader(4 bytes)][column_data]` per group-by column

## Common Pitfalls

1. **Missing ss.lock()**: Causes assertion failure in SignalSender
2. **V_QUERY with instance 0**: Segfault in distribute_signal
3. **Pool not initialized**: If testing new signal types, ensure the
   corresponding pool is initialized in the block's `callREAD_CONFIG_REQ`
4. **Long sections not released on error paths**: Causes
   `Check signal->header.m_noOfSections == 0 failed` in subsequent signals
5. **Wrong block for signal routing**: Sending SCAN_FRAGREQ to DBLQH
   instance 0 (proxy) fails with "Illegal signal received" because the
   proxy doesn't handle scan signals

## Crash Debugging

See `storage/ndb/src/kernel/TRACE_FILE_ANALYSIS.md` for how to analyze
data node crashes, including:
- Error logs (`ndb_N_error.log`)
- Program output with stack traces (`ndbd.log`)
- Per-thread jam traces (`ndb_N_trace.log.M_tN`)

## Building

```bash
cd debug_build
make -j$(sysctl -n hw.ncpu) testJoinAgg   # Build the test
make -j$(sysctl -n hw.ncpu) ndbmtd        # Rebuild data node if kernel code changed
```
