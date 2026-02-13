# DBLQH Block

DBLQH (DataBase Local Query Handler) handles scan and key operations on data node LDM threads.

## Pushdown Join Aggregation (RONDB-733)

DBLQH supports aggregation for pushdown join queries, allowing aggregation to be
pushed down to data nodes so intermediate results don't round-trip to the API.

### Documentation
- `PUSHDOWN_JOIN_AGGREGATION.md` — Architecture overview and signal flow
- `PUSHDOWN_JOIN_AGGREGATION_IMPL.md` — Implementation plan with code-level detail

### Key Source Files
- `DblqhMain.cpp` — Signal handlers for JOIN_AGG_SETUP/COMPLETE/RELEASE, scan processing, sendScanFragConf
- `DblqhProxy.cpp` — Routes setup/complete/release signals; manages JoinAggregationState pool
- `Dblqh.hpp` — ScanRecord (m_rows_examined, m_join_agg_state_key, m_join_agg_evict_rows)
- `JoinAggregationState.hpp` — Shared state struct for join aggregation across LDM threads

### Related Files in Other Blocks
- `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.hpp/.cpp` — Aggregation engine (chunk allocator, group-by hash map)
- `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` — `handleJoinAggRow()` interception point
- `storage/ndb/src/kernel/blocks/dbspj/CLAUDE.md` — DBSPJ protocol documentation
- `storage/ndb/include/kernel/signaldata/JoinAgg.hpp` — Signal definitions
- `storage/ndb/include/kernel/signaldata/ScanFrag.hpp` — ScanFragConf (includes rowsExamined field)

### Signal Flow
```
DBSPJ → JOIN_AGG_SETUP_REQ → DblqhProxy → creates JoinAggregationState
DBSPJ → SCAN_FRAGREQ (with join agg flag) → LDM threads → handleJoinAggRow → AggInterpreter
DBSPJ → JOIN_AGG_COMPLETE_REQ → DblqhProxy → merge + finalize → TRANSID_AI results
DBSPJ → JOIN_AGG_RELEASE_REQ → DblqhProxy → cleanup
```

### Testing
- Unit test: `storage/ndb/block_unit_test/testJoinAgg.cpp`
- Build: `make -j$(sysctl -n hw.ncpu) testJoinAgg` (from debug_build)
- Run: `testJoinAgg -c <connect_string> -m <mysql_port> [--verbose]`

### ERROR_INSERT Codes
- **5090** (DblqhProxy.cpp): Forces `setMaxGroups(3)` on AggInterpreters during
  JOIN_AGG_SETUP_REQ, triggering eviction when a 4th distinct group arrives

### DUMP State Commands (debug/test builds only)
- **2359** (LqhSkipTcNodeCheck): Disables the `get_node_status(refToNode(tcRef)) != ZNODE_UP`
  check in LQHKEYREQ processing, allowing LQHKEYCONF to be sent to non-data-node refs
  (e.g., test programs connected as API nodes)
- **2360** (LqhRestoreTcNodeCheck): Re-enables the tc node status check

Both are guarded by `#if defined(VM_TRACE) || defined(ERROR_INSERT)`.

### Key Design Patterns
- **TransientPool::seize() doesn't call constructors** — all JoinAggregationState fields
  must be explicitly initialized after seize in DblqhProxy::execJOIN_AGG_SETUP_REQ
- **Two interpreters**: NDB interpreter (WHERE clause) runs first, then AggInterpreter
  (aggregation) runs via handleJoinAggRow, just before sendReadAttrinfo
- **Version-gated signals**: New fields (e.g. rowsExamined in ScanFragConf) use
  ndbd_support_xxx() checks in ndb_version.h.in for backward compatibility
- **Batch counter save-before-reset**: In sendScanFragConf, save counters to locals
  before the batch reset block, since reset happens before signal population
