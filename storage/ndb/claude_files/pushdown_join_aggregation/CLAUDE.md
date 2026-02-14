# DBLQH Block

DBLQH (DataBase Local Query Handler) handles scan and key operations on data node LDM threads.

## Pushdown Join Aggregation (RONDB-733)

DBLQH supports aggregation for pushdown join queries, allowing aggregation to be
pushed down to data nodes so intermediate results don't round-trip to the API.

### Documentation (in this directory)
- `local_database_research.md` — Local database (DBLQH/DBTUP) research
- `local_database_implementation.md` — Local database implementation details
- `coordinator.md` — DBSPJ coordinator design
- `coordinator_research.md` — DBSPJ coordinator research
- `coordinator_implementation.md` — DBSPJ coordinator implementation details
- `trace_file_analysis.md` — NDB trace/crash log analysis guide
- `next_steps.md` — Remaining work: integration tests, NDB API, secondary features

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

### NdbInterpretedCode: Inverted Inequality Branches

**CRITICAL**: The NDB interpreter's inequality branch instructions are implemented
**backwards** from their names. EQ and NE work as expected, but LT/LE/GT/GE are
inverted (see `NdbScanFilter.cpp` line 560: "the interpreter cmp-code has been
implemented backwards, such that it branch on non-matches").

| Method name       | Actually branches when |
|-------------------|------------------------|
| `branch_col_eq`   | col == val (correct)   |
| `branch_col_ne`   | col != val (correct)   |
| `branch_col_lt`   | col **>** val          |
| `branch_col_le`   | col **>=** val         |
| `branch_col_gt`   | col **<** val          |
| `branch_col_ge`   | col **<=** val         |

This applies to both attr-vs-constant and attr-vs-attr variants.

**Root cause**: In `DbtupExecQuery.cpp` interpreterNextLab(), the comparison result
`res1 = cmp(col, val)` is tested with inverted conditions:
```cpp
case Interpreter::LT:  res = (res1 > 0);  break;  // branches when col > val
case Interpreter::GE:  res = (res1 <= 0); break;  // branches when col <= val
```

**NdbScanFilter compensates** by double-inverting: it negates the user's condition
(for AND groups) then looks up the already-inverted branch method in table3/table4.

**When building raw interpreter programs** (e.g., for PI_ATTR_INTERPRET in
QN_ScanFragParameters), use the opposite inequality method:
- To reject when `col >= val`: use `branch_col_le` (not `branch_col_ge`)
- To reject when `col < val`: use `branch_col_gt` (not `branch_col_lt`)

### NdbInterpretedCode::getWordsUsed() Includes Meta-Info

`getWordsUsed()` returns `m_instructions_length + numLabels * 2`, but the label
meta-info is stored at the END of the buffer (not after instructions). Reading
`buf[0..getWordsUsed()-1]` includes garbage from the gap between instructions
and meta-info. Subtract `numLabels * CODEMETAINFO_WORDS` (2 words per label)
to get the actual instruction count:
```cpp
Uint32 numLabels = 2;
Uint32 len = code.getWordsUsed() - numLabels * 2;
```

### Debug Trace Macros (DEB_XXX Pattern)

NDB kernel blocks use a consistent pattern for conditional debug logging:

```cpp
// 1. The #define DEBUG_XXX is inside the VM_TRACE/ERROR_INSERT guard
//    to ensure it can never be accidentally enabled in production builds.
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_AGG 1
#endif

// 2. The DEB_XXX macro is always defined (empty in non-debug),
//    so call sites compile in all builds without #ifdef wrappers.
#ifdef DEBUG_AGG
#define DEB_AGG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AGG(arglist) do { } while (0)
#endif
```

Usage: `DEB_AGG(("format string %u", value));` — note the **double parentheses**
(outer for macro, inner for function call in the arglist expansion).

To enable: uncomment `#define DEBUG_AGG 1` and rebuild. Only works in debug
builds (VM_TRACE or ERROR_INSERT defined).

**Important**: Do NOT use `g_eventLogger->debug(...)` for debug tracing — it
requires special runtime configuration to produce output. Use the DEB_XXX
pattern instead for developer-activated trace output.

Similar patterns in the codebase:
- `DEB_CONT_SCAN` / `DEBUG_CONT_SCAN` — DblqhMain.cpp, DbtcMain.cpp, DbtupBuffer.cpp
- `DEB_TRANSID_AI` / `DEBUG_TRANSID_AI` — DbtupBuffer.cpp
- `DEB_RATE_QUEUE_DROP` / `DEBUG_RATE_QUEUE_DROP` — DbtcMain.cpp
- `DEB_AGG` / `DEBUG_AGG` — AggInterpreter.cpp
- `PA_INTERP_TRACE` / `DEBUG_PA_INTERP` — AggInterpreter.cpp (partition-filtered variant)
