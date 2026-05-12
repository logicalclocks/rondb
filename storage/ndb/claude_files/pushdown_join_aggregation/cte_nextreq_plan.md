# SCAN_NEXTREQ flow control for CTE main-SELECT queries — Overview

## Context

CTE materialization on the RONDB-1050 branch works end-to-end via
`JOIN_AGG_SETUP/COMPLETE` + the jump-table interpreter. Aggregate-top
queries return results through a bespoke `JOIN_AGG_SEND_REQ/CONF`
scheme, which sidesteps `SCAN_FRAGCONF`/`SCAN_NEXTREQ`. When the main
SELECT does **not** end in aggregation (it just consumes rows produced
by the CTEs), results must go back to the API through the normal DBSPJ
protocol: DBLQH accumulates up to `batch_size_rows` rows as
`TRANSID_AI`, DBSPJ sends `SCAN_FRAGCONF(fragmentCompleted=0)` to DBTC,
DBTC sends `SCAN_TABCONF` to the API, and the API drives the next batch
with `SCAN_NEXTREQ`.

That second path is not wired up today when CTEs are involved.
Specifically, when `CTE_SCAN_REQ` is the main-SELECT root:

- `g_CteScanOpInfo` (`DbspjMain.cpp:6666`) has `execSCAN_NEXTREQ = 0`.
- `CteScanData` has no `scanIterI` field; the continuation sent from
  `execCTE_SCAN_CONF` (`DbspjMain.cpp:7111-7144`) uses `SignalLength=9`
  and does **not** set `req->scanIterI`, so DBLQH would restart
  iteration from bucket 0 on every batch. Hidden today because
  `CteScanData::m_batchSize = 256` is larger than the test corpus.
- `m_batchSize` is hard-coded to 256 (`DbspjMain.cpp:6722`), independent
  of the API's `batch_size_rows`.
- `execCTE_SCAN_CONF` sends the next `CTE_SCAN_REQ` back-to-back with no
  API round-trip, so the API has no back-pressure.

For `CTE_LOOKUP_REQ` under a real-table main-scan root, the normal
`scanFrag` path handles `SCAN_NEXTREQ` already and CTE state lives in
`JoinAggregationState` until `JOIN_AGG_RELEASE_REQ`, so functionally it
*should* work — but it is untested with multi-batch scans.

**Goal:** (1) make `CTE_SCAN_REQ` as main-SELECT root participate in the
standard SCAN_FRAGCONF/SCAN_NEXTREQ cycle, (2) verify that CTE state
survives the API round-trip for both `CTE_SCAN` and `CTE_LOOKUP`
main-SELECT shapes, (3) add NDB-API test binaries that actually exercise
multi-batch results.

## Target signal flow (CTE_SCAN root, `batch_size_rows = B`, total `N > B`)

```
API                DBTC             DBSPJ                      DBLQH
 SCAN_TABREQ ----->
                   SCAN_FRAGREQ(B)->
                                    cte_scan_start
                                    CTE_SCAN_REQ(B) ---------> emit ≤B rows
                              <-- TRANSID_AI × B (via FLUSH_AI) ------
                                    CTE_SCAN_CONF(EndOfData=0,
                                    scanIterI=X) <------------ alloc/reuse
                                                               CteScanIterState
                                    stash scanIterI, do NOT
                                    re-fire, leave TN_ACTIVE
                   <-- SCAN_FRAGCONF(fragmentCompleted=0,
                       activeMask[root]=1, completedOps=B) ----
 <-- SCAN_TABCONF (moreMask) ---
 SCAN_NEXTREQ ---> SCAN_NEXTREQ ->
                                    cte_scan_execSCAN_NEXTREQ
                                    CTE_SCAN_REQ(
                                     SignalLengthContinue,
                                     scanIterI=X, B) --------> resume from X
 ... batch loop repeats until the final CTE_SCAN_CONF(EndOfData=1) ...
                                    SCAN_FRAGCONF(fragmentCompleted=1)
 <-- SCAN_TABCONF (no moreMask)
```

Multi-node case (`m_cteScanAllNodes`): DBSPJ tracks one `scanIterI` per
source data node; a single `SCAN_NEXTREQ` fans out a continuation
`CTE_SCAN_REQ` to every node whose prior CONF carried `EndOfData=0`.
DBSPJ batches into DBTC only after all outstanding per-node REQs have
answered for this API batch.

## Phase index

Each phase is its own file. Phases are ordered — Phase 1 must land
green before Phase 2.

| File | Phase | Scope | Status |
|---|---|---|---|
| [cte_nextreq_phase_1.md](cte_nextreq_phase_1.md) | **1** | Fix CTE_SCAN_REQ continuation plumbing (scanIterI, SignalLengthContinue, helper refactor) + regression test | pending |
| [cte_nextreq_phase_2.md](cte_nextreq_phase_2.md) | **2** | SCAN_NEXTREQ flow control for CTE_SCAN root (execSCAN_NEXTREQ handler, cursor registration, batch-size from SCAN_FRAGREQ, close path) | pending |
| [cte_nextreq_phase_3.md](cte_nextreq_phase_3.md) | **3** | CTE state lifetime audit across SCAN_NEXTREQ pauses | pending |
| [cte_nextreq_phase_4.md](cte_nextreq_phase_4.md) | **4** | NDB API test binaries exercising multi-batch CTE main-SELECTs | pending |

## Critical files (cross-phase)

- `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` — `CteScanData`
  (line 665).
- `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` —
  `g_CteScanOpInfo` (6653-6672), `cte_scan_build` (6674-6762),
  `cte_scan_start` (6764-6994), `execCTE_SCAN_CONF` (7027-7185),
  `sendConf` activeMask filter (3752-3854), generic `execSCAN_NEXTREQ`
  dispatch (4575-4696), `scanFrag_execSCAN_NEXTREQ` template.
- `storage/ndb/include/kernel/signaldata/CteScan.hpp` —
  `CteScanReq::SignalLengthContinue`, possibly add a `CloseFlag`.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` —
  `execCTE_SCAN_REQ` (20277), `cteScanEmitResults` (19970),
  `releaseCteScanIterState` (19673).
- `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp` —
  `CteScanIterState` pool (5160-5180).
- `storage/ndb/block_unit_test/CMakeLists.txt` — new test binaries.

## Cross-phase risks / unknowns

1. **Cursor-list registration.** `execSCAN_NEXTREQ` dispatches via
   `m_cursor_nodes` (`DbspjMain.cpp:4653-4663`); without adding the
   CTE_SCAN TreeNode to that list at `cte_scan_start`, SCAN_NEXTREQ
   silently drops the scan. Sanity-check early in Phase 2.
2. **Close/abort cleanup.** `CteScanIterState` would leak on
   API-initiated close without a dedicated close path (Phase 2.7).
3. **Multi-node per-source `scanIterI` layout.** Inline array vs. pool
   record. Try inline first; move to pool if the struct grows too large.
4. **Batch-size derivation source.** `QN_CteScanParameters` has no
   `batch_size_rows` field. Read from the originating SCAN_FRAGREQ via
   `ctx.m_start_signal` rather than extending the wire format.
5. **CTE state lifetime.** If `JoinAggregationState` is released earlier
   than `JOIN_AGG_RELEASE_REQ` under some trigger, Phase 3 has to
   extend its lifetime. Current behaviour is unverified because
   `m_batchSize = 256` masks it for all existing tests.
6. **Scenario: real-root + correlated `CTE_SCAN` child.**
   `g_CteScanOpInfo.parent_row == 0` and `parent_*` handlers are null,
   so correlated `CTE_SCAN` is not supported today. Confirm and defer.

## Verification

Per-phase: after Phase 1, the existing CTE test suite
(`testCteNdbApi`, `testCteNdbApiFilter`,
`testCteScanFilterBatchBoundary`) must stay green and the new
>512-group variant must pass. After Phase 2, the Phase-4 Scenario-1
test must pass and show multiple SCAN_NEXTREQ round-trips; existing
tests must still be green. Phase 3 is findings-only. Phase 4 delivers
the full test coverage.

Smoke runs from `debug_build/`:

```bash
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd
make -j$(sysctl -n hw.ncpu) testCteNdbApi testCteNdbApiFilter \
     testCteNdbApiNextReq testCteNdbApiNextReqRealRoot \
     testCteNdbApiNextReqChained testJoinAggNdbApi
./runtime_output_directory/testCteNdbApi              -c <cs> -m 3306 -v
./runtime_output_directory/testCteNdbApiFilter        -c <cs> -m 3306 -v
./runtime_output_directory/testCteNdbApiNextReq       -c <cs> -m 3306 -v
./runtime_output_directory/testCteNdbApiNextReqRealRoot -c <cs> -m 3306 -v
./runtime_output_directory/testCteNdbApiNextReqChained  -c <cs> -m 3306 -v
./runtime_output_directory/testJoinAggNdbApi          -c <cs> -m 3306 -v
```
