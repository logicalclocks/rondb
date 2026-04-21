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
- `next_steps.md` — Remaining work: MySQL handler integration, secondary features
  (references RonSQL subquery plan in `storage/ndb/src/ronsql/ronsql_join_phase7.md`)
- `ndbapi_integration_plan.md` — NDB API integration plan (Steps 1-10, all complete)
- `ndbapi_integration_implementation.md` — NDB API integration implementation details
- `mysql_join_agg.md` — MySQL handler integration architecture (Phase 9)
- `mysql_handler_implementation.md` — MySQL handler implementation plan (Phases 1-12)
- `chained_outer_join_plan.md` — Chained outer join aggregation fix plan (Phases 1-6)
- `star_schema_plan.md` — Star schema fan-out aggregation design (RONDB-1044)
- `star_schema_implementation.md` — Star schema implementation plan (Steps 1-12)
- `cte_filter_plan.md` — CTE filter + 3rd interpreter (jump-table) overview + phase index
- `cte_filter_phase_a.md` — Phase A: CTE_LOOKUP_REQ filter support
- `cte_filter_phase_b.md` — Phase B: CTE_SCAN_REQ (root) filter support
- `cte_filter_phase_c.md` — Phase C: aggregation interpreter reuses the jump-table interpreter
- `cte_nextreq_plan.md` — SCAN_NEXTREQ flow control for CTE main-SELECTs: overview + phase index
- `cte_nextreq_phase_1.md` — Phase 1: fix CTE_SCAN_REQ continuation plumbing (scanIterI, SignalLengthContinue)
- `cte_nextreq_phase_2.md` — Phase 2: SCAN_NEXTREQ flow control for CTE_SCAN root
- `cte_nextreq_phase_3.md` — Phase 3: CTE state lifetime audit across SCAN_NEXTREQ pauses
- `cte_nextreq_phase_4.md` — Phase 4: NDB API tests for multi-batch CTE main-SELECTs

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
DBTC → JOIN_AGG_SETUP_REQ → DblqhProxy → creates JoinAggregationState
DBSPJ → SCAN_FRAGREQ (with join agg flag) → LDM threads → handleJoinAggRow → AggInterpreter
DBSPJ → JOIN_AGG_COMPLETE_REQ → DblqhProxy → merge + finalize → TRANSID_AI results
DBSPJ → JOIN_AGG_RELEASE_REQ → DblqhProxy → cleanup
```

### Testing

All tests in `storage/ndb/block_unit_test/`. Build from debug_build, run with
`-c <connect_string> -m <mysql_port> [--verbose]`.

| Target | Signal Path | Description |
|--------|-------------|-------------|
| testJoinAgg | Direct DBLQH | All agg types, GROUP BY, eviction, mutex-free, flow control |
| testJoinAggSpj | DBTC→DBSPJ→DBLQH | Full QueryTree path, empty/single row, large dataset |
| testJoinAggNdbApi | NdbQueryBuilder API | 4 tests: SUM/GROUP BY, COUNT+SUM, multi-agg, 3-way join |
| testOuterJoinAggNdbApi | NdbQueryBuilder API | 2-way outer join: scan-lookup, scan-scan, COUNT(*), multi-batch |
| testMultiOuterJoinAggNdbApi | NdbQueryBuilder API | Chained outer join: 3-way LEFT, 4-way mixed LEFT+INNER |
| testCaseAgg | Direct DBLQH | CASE expression in aggregation |
| benchJoinAgg | Direct DBLQH | Performance: pipelined lookups with linked attrs |
| bench_q12_tpch | Direct DBLQH | TPC-H Q12 with CASE, CHAR comparison, date filters |
| bench_q12_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q12 through full orchestration |
| bench_q9_dbtc | DBTC→DBSPJ→DBLQH | TPC-H Q9: 6-table join, multi-level linked attrs |
| load_tpch | — | TPC-H data loader for bench_q9_dbtc |

Build all: `make -j$(sysctl -n hw.ncpu) testJoinAgg testJoinAggSpj testJoinAggNdbApi`

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

### NDB API Multi-Fragment Workers vs DBSPJ Parallelism

The NDB API's multi-fragment worker model (`m_fragsPerWorker > 1`) bundles multiple
fragments into a single SCAN_FRAGREQ to a single DBSPJ instance. This is controlled
by the `MultiFragFlag` in `SCAN_TABREQ`. The flow:

1. `NdbQueryOperation.cpp`: `m_fragsPerWorker = rootFragments / numberOfDataNodes`
2. If `m_fragsPerWorker > 1`, `MultiFragFlag` is set in `SCAN_TABREQ`
3. In DBTC `sendDihGetNodesLab()`: all fragments on the same node are assigned the
   **same DBSPJ instance** via round-robin (`cspjInstanceRR`), then sorted by
   `primaryBlockRef` so they group together
4. In DBTC `sendScanFragReq()`: fragments with matching `primaryBlockRef` are bundled
   into a **single SCAN_FRAGREQ** with multiple fragment IDs

**Without MultiFragFlag** (the non-multi-frag path): each fragment gets a **different**
round-robin DBSPJ instance, resulting in separate SCAN_FRAGREQs that run on different
TC threads in parallel.

**Impact**: With 1 data node and 4 fragments, MultiFragFlag causes all 4 fragments to
go to 1 DBSPJ instance (1 SCAN_FRAGREQ), while without it, 4 SCAN_FRAGREQs go to 4
different DBSPJ instances across TC threads.

**Aggregate queries disable multi-fragment workers** (`m_fragsPerWorker = 1`) because:
- Aggregation results are per-node, not per-fragment — the API round-trip savings from
  multi-fragment bundling don't apply
- Distributing fragments across DBSPJ instances gives better parallelism across TC threads
- `scanParallelism` in SCAN_TABREQ equals `rootFragments` (one handle per fragment)

Key code locations:
- `NdbQueryOperation.cpp:3044` — `m_hasAggregation` forces `m_fragsPerWorker = 1`
- `DbtcMain.cpp:16861-16914` — multi-frag SPJ instance assignment and sorting
- `DbtcMain.cpp:17241-17243` — non-multi-frag round-robin SPJ instance per fragment
- `DbtcMain.cpp:18525-18604` — MultiFragFlag fragment grouping in `sendScanFragReq`

#### scanParallelism in SCAN_TABREQ

For JoinAgg queries, DBTC reads `scanParallelism` from the SCAN_TABREQ signal
(`DbtcMain.cpp:15692`) and uses it to allocate that many `ScanFragRec` handles
in `initScanrec`. This determines the **maximum concurrent fragments**. After
DIH reports the actual fragment count (`tfragCount`), `scanParallel` is overridden
to `tfragCount` for fragment iteration, but the number of pre-allocated handles
remains fixed. Excess handles (if `scanParallelism > tfragCount`) are set to
"empty result" in `sendFragScansLab`.

For aggregate queries, `scanParallelism = rootFragments` (since `m_fragsPerWorker = 1`
and `m_workerCount = rootFragments`), which matches `tfragCount` exactly.
