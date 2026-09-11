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

### JoinAggSetupReq from a block test

`JoinAggSetupReq::SignalLength` includes `setupNodes`
(`NdbNodeBitmask::Size` words): the data nodes DBTC set the query up on,
which every DBLQH turns into its CTE owner list (RONDB-1120, see
`claude_files/pushdown_join_aggregation/cte_owner_list.md`). A block test
that sends SETUP_REQ itself can either fill `setupNodes` with the
connected data nodes and send `SignalLength`, or send `SignalLength_v1`
(13 words), in which case DblqhProxy builds the list from its own view of
the connected data nodes. Use one form for all nodes of a query, or the
owner lists differ. With the full length, a listed node the receiver is
not connected to is answered with JOIN_AGG_SETUP_REF error 286.

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

## LQHKEYREQ Construction

Key operations with join aggregation use LQHKEYREQ instead of SCAN_FRAGREQ.
The signal has 11 fixed words plus variable data whose layout depends on flags:

```
Fixed 11 words: [clientConnectPtr, attrLen, hashValue, requestInfo,
                 tcBlockref, tableSchemaVersion, fragmentData,
                 transId1, transId2, savePointId, scanInfo]

Variable data (parsed sequentially based on flags):
  CorrFactorFlag → 2 words (corrFactorLo, corrFactorHi)
  JoinAggFlag    → 1 word (aggStateKey)

Long sections:
  Section 0: KeyInfo (primary key)
  Section 1: AttrInfo (interpreter program)
```

**Key points:**
- `tcBlockref = ss.getOwnRef()` — routes LQHKEYCONF back to test program
- `JoinAggFlag` is bit 28 in `attrLen` field (`LqhKeyReq::getJoinAggFlag()`)
- Requires NormalProtocolFlag for dirty reads to get LQHKEYCONF
- Hash computation: use `rondb_calc_hash()` for both ACC hash (values[0])
  and distribution hash (values[1])

**DUMP commands for tc-node check bypass** (debug/test builds only):
- `DUMP 2359` (LqhSkipTcNodeCheck) — allows LQHKEYCONF to non-data-node refs
- `DUMP 2360` (LqhRestoreTcNodeCheck) — restores normal behavior
- Use `NdbRestarter::dumpStateAllNodes()` to send

## ERROR_INSERT for Testing

- **5090**: Force `setMaxGroups(3)` on AggInterpreters in DblqhProxy setup,
  triggering eviction when a 4th distinct group arrives during scan.
  Use `NdbRestarter::insertErrorInAllNodes(5090)` before test,
  `insertErrorInAllNodes(0)` after.

Node-failure / parking hooks (see `node_failure_test_plan.md`, §3):

| Code | Block | Effect | "once" clears itself |
|---|---|---|---|
| 5121 / 5122 / 5123 | Proxy / DBLQH / Proxy | crash node on SETUP_REQ / COMPLETE_REQ / RELEASE_REQ | no (crash) |
| 5124 / 5125 | DBLQH / Proxy | COMPLETE_REF once / SETUP_REF once | yes |
| 5127 | Proxy | hold ONE SETUP_REQ 20 ms (feeds park) | yes |
| 5128 | DBLQH `cteScanEmitResults` | rows sent, CTE_SCAN_CONF swallowed once | yes |
| 5129 | DBLQH `cteScanReqImpl` | ONE continuation answered CTE_SCAN_REF(1251), token released | yes |
| 5130 | DBLQH `cteScanAggFeed` | one group per continuation round while set | no |
| 5131 | DBLQH `cteLookupReqImpl` | hold ONE lookup 50 ms | yes |
| 5132 | DBLQH `joinAggNullRowReqImpl` | ONE null-row injection REFed | yes |
| 5133 | DBLQH `execJOIN_AGG_REDISTRIBUTE_REQ` | every inbound redistribute delayed 200 ms while set | no |
| 5134 | DBLQH `redistAlloc` | 512-byte redistribution pages while set | no |
| 5135 | DBLQH `execSCAN_NEXTREQ` | ONE scan close swallowed (arm right before the close; logs when it fires) | yes |
| 5136 | Proxy `execJOIN_AGG_RELEASE_REQ` | ONE release duplicated to self | yes |
| 5137 | Proxy `continueJoinAggTeardown` | one group per teardown round while set | no |
| 5138 | Proxy `execJOIN_AGG_SETUP_REQ` | every SETUP_REQ held until cleared | no |
| 5139 | Proxy `execJOIN_AGG_SETUP_REQ` | ONE SETUP_REQ dropped (no reply) | yes |
| 5140 | DBLQH `execJOIN_AGG_REDISTRIBUTE_REQ` | every inbound redistribute held, 200 ms at a time, until cleared | no |
| 5141 | DBLQH `cteLookupReqImpl` | every inbound CTE lookup held, 200 ms at a time, until cleared; one CTE_NF3_LOOKUP_HELD event per instance | no |
| 5142 | DBLQH `cteScanEmitResults` | rows sent, CTE_SCAN_CONF to every remote requester swallowed while set; one CTE_NF4_CONF_HELD event per instance | no |
| 5143 | DBLQH `cteScanEmitResults` | diagnostic only: CTE_NF5_SCAN_PAUSED event naming the remote requester of each saved iterator; rows and CONF unchanged | no |
| 5144 | DBLQH `cteScanAggFeed` | every aggregation feed held between rounds, 20 ms at a time, until cleared; one CTE_NF6_FEED_HELD event per instance naming the remote requester | no |
| 8310 | DBTC `execJOIN_AGG_SETUP_CONF` | ONE SETUP_CONF delayed 20 ms | yes |
| 8311 | DBTC `sendJoinAggCompleteReqs` | ONE COMPLETE sent with aggStateKey RNIL | yes |
| 8312 | DBTC release senders | crash after sending RELEASE_REQs | no (crash) |
| 8313 | DBTC `execJOIN_AGG_SETUP_CONF` | ONE SETUP_CONF delayed 5 s (stale reclaim) | yes |
| 17532 | DBSPJ `cte_scan_sendReq` | crash when a second CTE scan batch is requested | no (crash) |
| 17533 | DBSPJ `execSCAN_NEXTREQ` | ONE close from DBTC swallowed, request left waiting (arm right before the close; logs when it fires) | yes |

Leak-check DUMP codes (each crashes the node on a leak, so run them at the
end of a test with `NdbRestarter::dumpStateAllNodes`):
2361 join-agg states, 2362 CTE scan iterator records (per worker),
2363 identity table + park records, 2560 DBTC completion records / CTE
scan-fragment handles / scans left in a join-agg or closing state,
2650 DBSPJ requests.

## Building

```bash
cd debug_build
make -j$(sysctl -n hw.ncpu) testJoinAgg   # Build the test
make -j$(sysctl -n hw.ncpu) ndbmtd        # Rebuild data node if kernel code changed
```
