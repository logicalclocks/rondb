# Phase 4 — NDB API tests for multi-batch CTE main-SELECTs

## Goal

Provide end-to-end NDB-API test coverage for main-SELECT queries that
use CTEs and produce results large enough to require multiple
SCAN_NEXTREQ round-trips.

Depends on Phase 1 (so continuation works at all) and Phase 2 (so
SCAN_NEXTREQ is wired for CTE_SCAN root).

## Test pattern

Template: `storage/ndb/block_unit_test/testCteNdbApi.cpp` and
`testCteNdbApiFilter.cpp`. All tests:

- Create tables via MySQL (`sqlExec`) per project convention
  (`mysql_trees/rondb_1050_cte_filter/CLAUDE.md` — "Always create NDB
  tables through MySQL"). Drop and recreate on each run for isolation.
- Bulk-load rows with `INSERT ... VALUES (...), (...), ...` or a loop.
- Build the query with `NdbQueryBuilder` —
  `beginCteSubtree`/`endCteSubtree`/`defineCte` for the CTE part,
  `scanTable`/`readTuple` for the main SELECT.
- Force multi-batch via `NdbQueryOperation::setBatchSize(small)`
  (see `NdbQueryOperation.cpp:2016-2017, 3859-3869`).
- Drive iteration with `query->nextResult(true)` in a loop.
- Assert: (i) total rows, (ii) checksum / per-row value of a known
  column, (iii) that multiple SCAN_NEXTREQ actually fired. Option for
  (iii): make N large enough that the internal receive buffer can't
  possibly hold the whole result; a cleaner check is counting
  `SCAN_NEXTREQ` via DBSPJ trace lines in the debug build.

Register each new binary in
`storage/ndb/block_unit_test/CMakeLists.txt` using the same
`NDB_ADD_EXECUTABLE(... NDBTEST NDBCLIENT MYSQLCLIENT)` pattern as
`testCteNdbApi`.

## Scenarios

### 4.1 `testCteNdbApiNextReq` — CTE_SCAN root → API, no top aggregation

Query shape:

```sql
WITH cte AS (SELECT grp, SUM(val) AS s FROM t GROUP BY grp)
SELECT grp, s FROM cte
```

Setup: ~5000 rows in `t` with ~1000 distinct `grp` values.
`setBatchSize(100)`. Expected ≥ 8 SCAN_NEXTREQ round-trips.

Assertions:
- Row count returned == 1000.
- Every `grp` in 0..999 appears exactly once.
- `s == expected_sum_for_grp` for each row.

This is the primary test for Phase 2 — it fails without the
`execSCAN_NEXTREQ` handler even with Phase 1's continuation fix,
because the back-to-back path is removed.

### 4.2 `testCteNdbApiNextReqRealRoot` — real scan root + `CTE_LOOKUP` child

Query shape:

```sql
WITH cte AS (SELECT grp, SUM(val) AS s FROM t GROUP BY grp)
SELECT t.pk, t.grp, cte.s FROM t JOIN cte ON t.grp = cte.grp
```

Setup: ~5000 rows in `t`, ~1000 groups. `setBatchSize(100)`. Expected
~50 SCAN_NEXTREQ round-trips.

Assertions:
- Row count == 5000.
- For each row, `cte.s == expected_sum_for_t.grp`.

Proves that the existing scanFrag + CTE_LOOKUP path Just Works across
SCAN_NEXTREQ boundaries; no code changes expected from Phase 2, just
coverage.

### 4.3 `testCteNdbApiNextReqChained` — nested / chained CTEs

Query shape:

```sql
WITH
  cte1 AS (SELECT grp, val FROM t),
  cte2 AS (SELECT grp, SUM(val) AS s FROM cte1 GROUP BY grp)
SELECT grp, s FROM cte2
```

Setup similar to 4.1. Asserts the Phase 2 flow still works when the
main SELECT's `CTE_SCAN` reads from a CTE that was itself populated by
a CTE-reads-CTE materialization. Multiple CTE phases exercise the
lifetime invariants from Phase 3.

### 4.4 (optional) — low-batch regressions

Add `setBatchSize(1)` variants to existing `testCteNdbApi` /
`testCteNdbApiFilter` fixtures to stress the smallest-batch path.
Guard behind a separate test command so they don't bloat the default
run.

## Out of scope

**Real-root + correlated `CTE_SCAN` child.** `g_CteScanOpInfo` has
`parent_row = 0` at `DbspjMain.cpp:6662` and all `parent_*` handlers
are null, so correlated `CTE_SCAN` is not supported today. Do not add
a test for this; instead, add a one-line note to
`storage/ndb/claude_files/pushdown_join_aggregation/next_steps.md`
flagging it as a gap.

## Acceptance

- All three new binaries build and pass.
- Each test completes in < 30s on the local dev cluster.
- Debug trace (`DEB_CTE`) shows the expected SCAN_NEXTREQ cadence
  (for 4.1 and 4.3) and normal scanFrag cadence (for 4.2).
- All pre-existing tests still pass.
