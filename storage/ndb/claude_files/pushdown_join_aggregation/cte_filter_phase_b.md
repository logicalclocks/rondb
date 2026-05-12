# Phase B — CTE_SCAN_REQ (root) filter support

> **Prereq reading**: [cte_filter_plan.md](cte_filter_plan.md) — architecture overview, accepted/unsupported opcodes. Then [cte_filter_phase_a.md](cte_filter_phase_a.md) — Phase A must be landed and green before starting Phase B. Phase B reuses the dispatch table, `Dbtup::interpreterFilterCte`, the serializer fix, and the helper `Dblqh::buildCteLinkedBuffer` built in Phase A.

This phase extends filter execution to `CTE_SCAN_REQ` when it acts as the root scan of a query (the only configuration supported at this time — CTE_SCAN_REQ as a child operation is future work per the user's stated scope). The AttrInfo section format for `CTE_SCAN_REQ` already reserves a 5-word interpreter header + program slot per the doc comment at `CteScan.hpp:110-116`; DBLQH just doesn't execute the program today.

---

## B.1 — Baseline scan tests

Already covered by Phase A.1 (`testScanFilterSingleCol`, `testScanFilterTwoCol`, `testScanFilterStringEq`). Skip. If any of those regressed during Phase A work, fix before proceeding.

---

## B.2 — Run filter per scanned group in DBLQH

**Goal**: for each group produced by the CTE scan, run the filter program; skip rejected groups.

### Files

- **MODIFY** `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`: the CTE scan has two emit paths that iterate groups:
  - `cteScanEmitBatch` (or whatever the API/FLUSH_AI emit function is called) — emits each group as `TRANSID_AI` via the routing in the AttrInfo section's projection.
  - `cteScanAggFeed` at `DblqhMain.cpp:19565+` — feeds groups into a target `JoinAggInterpreter` when `joinAggStateKey != RNIL`.
- In each per-group iteration, call a new helper:
  ```cpp
  enum CteFilterResult { CTE_FILTER_ACCEPT, CTE_FILTER_REJECT, CTE_FILTER_ERROR };
  CteFilterResult runCteFilter(const Uint32* cinBuf, Uint32 attrInfoLen,
                               JoinAggInterpreter* interp,
                               const char* groupData,
                               Uint32* errOut);
  ```
  which encapsulates the filter-extraction logic from Phase A.4. Factor it out of Phase A's inline code in `execCTE_LOOKUP_REQ` — both paths call the same helper.
- **CTE_SCAN_REQ** iteration:
  - On `ACCEPT`: proceed with the existing emit (TRANSID_AI or agg feed).
  - On `REJECT`: `continue` to next group. **No `CteScanRef` is sent**; the filter simply doesn't emit this group.
  - On `ERROR`: send `CteScanRef` with new error code `ZCTE_SCAN_FILTER_ERROR` and abort the scan.
- Keep the batching at `CTE_SCAN_AGG_FEED_BATCH = 256` (`DblqhMain.cpp:19551`). Batches may return fewer than 256 groups due to filter rejects; `CteScanConf.numRows` already reflects actual emitted rows.
- Continuation state (`scanIterI`) is **not** affected by filter rejects — the iterator walks the source hash table, which has nothing to do with filter outcome. Each `CTE_SCAN_REQ` continuation resumes where the iterator left off and re-runs the filter on the next batch of groups.

- **MODIFY** `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp`: add
  ```cpp
  static constexpr Uint32 ZCTE_SCAN_FILTER_ERROR = 1267;   // or next free
  ```
- **MODIFY** `ndberror.cpp`: register the new code.

### Design note — "filter rejects all groups in a batch"

This must not cause the scan to hang. The continuation signals (`CTE_SCAN_REQ` with `SignalLengthContinue=10`) always carry the iterator state forward. The `CteScanConf.numRows=0` case must still mark `EndOfData=0x1` only when the iterator truly reached the end — not when the batch filtered to zero. Verify by tracing a batch that filter-rejects every group; the outer `SCAN_FRAGREQ` loop should continue until the iterator returns empty.

---

## B.3 — NdbQueryBuilder changes

The A.5 serializer fix (`!hasInterpretedCode()` guard on the `QN_CTE_SCAN` case at `NdbQueryOperation.cpp:5585-5589`) already handles this. Verify by inspecting the serialized AttrInfo in a test program that attaches a filter to `scanCte()`.

The `Interpreter::validateCteSafe` validator from A.5 already runs in `NdbQueryCteScanOperationDefImpl::serializeOperation`. No additional change.

---

## B.4 — CTE_SCAN filter tests

**Goal**: prove filter is applied per scanned group in both API-emit and agg-feed paths, including batch boundaries.

### Files

Append to `testCteNdbApiFilter.cpp` (continues numbering from Phase A).

### Tests

| # | Name | SQL-equivalent | Verifies |
|---|---|---|---|
| 13 | `testCteScanFilterRoot` | `SELECT * FROM cte0 WHERE cte0.val > 10` with `cte0` as main root via `scanCte()` | Filter applied per scanned group in API-emit path |
| 14 | `testCteScanFilterAggFeed` | CTE₂ = `SELECT g, SUM(v) FROM cte₁ WHERE g > 5 GROUP BY g` | Filter runs before feeding into target agg interpreter |
| 15 | `testCteScanFilterBatchBoundary` | Dataset ≥ 257 groups, filter rejecting most | Continuation iterator state correct after partial batches; no hang if a batch filter-rejects everything |
| 16 | `testCteScanFilterEmptyResult` | Filter rejects all groups | `CteScanConf.numRows=0, EndOfData=0x1` correctly set |

### Dataset requirement

Test 15 needs enough groups to span at least two 256-group batches. Seed the `cte_src` table with ≥ 300 distinct `grp` values (or lower the batch size for testing if there's a debug hook).

### Verify

```bash
cd debug_build
make -j$(sysctl -n hw.ncpu) testCteNdbApiFilter
./runtime_output_directory/testCteNdbApiFilter -c localhost:1186 -m 3306 -v
```

All 16 tests must pass. Run full CTE regression suite:
```bash
./runtime_output_directory/testCteNdbApi -c localhost:1186 -m 3306 -v
```

---

## Phase B done when

- `testCteNdbApiFilter` tests 13-16 pass.
- `testCteNdbApi` (existing 21 tests) still passes unchanged.
- `Dblqh::runCteFilter` helper is in place and used by both `execCTE_LOOKUP_REQ` (Phase A) and the CTE_SCAN emit paths.
- No new warnings in debug build.

## Files touched (checklist)

- [ ] `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` (execCTE_SCAN_REQ, `cteScanAggFeed`, `cteScanEmitBatch`)
- [ ] `storage/ndb/src/kernel/blocks/dblqh/Dblqh.hpp` (new error code)
- [ ] `ndberror.cpp` (register error code)
- [ ] `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp` (tests 13-16)
