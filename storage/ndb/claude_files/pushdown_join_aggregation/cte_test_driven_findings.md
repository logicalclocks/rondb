# Test-Driven CTE Phase — Findings & Next-Phase Backlog

Produced by the data-driven RonSQL CTE test phase (suites `ronsql_cte` +
topology siblings `ronsql_cte_ng1r3 / ng2r2 / ng2r3 / ng4r2`). The suites build
a realistic TPC-H-lite dataset (region/nation/customer/supplier/part/orders/
lineitem; 300 custkeys so GROUP BY o_custkey crosses the 256-row API batch
boundary; data hashes across all node groups) and sweep aggregating-CTE shapes
diffed RonSQL-vs-MySQL via `suite/ronsql/include/ronsql_compare.inc`.

The differential sweep surfaced **17 distinct gaps/bugs** in RonSQL CTE support
on this build — numbered D1–D22 (D7–D9/D13/D14 were transient
SUM-over-DECIMAL-rooted symptoms that resolved once the SUM backbone was fixed,
so they are retired) — including **3 reproducible data-node crashes** and
**6 hangs**. The green suite (~80 cases ×5 topologies) is the regression net for
what works; this doc is the prioritized backlog for the next development phase.

## Suite layout

- `mysql-test/suite/ronsql_cte/` — base (2 data nodes = 1 NG × 2 replicas).
  `include/cte_schema.inc` + `cte_data.inc` (shared schema+data),
  `include/body_<family>.inc` (6 self-contained family bodies), thin
  `t/ronsql_cte_dd_<family>.test` wrappers, `findings/` (this phase's notes +
  `crash_artifacts/`).
- `ronsql_cte_ng1r3` (3 nodes, 1 NG×3 rep), `ronsql_cte_ng2r2` (4, 2×2),
  `ronsql_cte_ng2r3` (6, 2×3), `ronsql_cte_ng4r2` (8, 4×2) — identical wrappers +
  results, sourcing the base body includes; only `my.cnf` topology differs.
- Six families: **agg** (aggregate×type matrix), **filter** (WHERE matrix),
  **index** (CTE-body index usage + EXPLAIN), **joins** (join shapes),
  **chain_scalar** (chained + scalar CTEs), **mainmode** (projection vs
  aggregating main).

## Confirmed GREEN envelope (the regression net)

The stable shape is: **an aggregating main SELECT** (scalar aggregate, or
`GROUP BY` a parent/CTE column with aggregates) **over a CTE keyed by a
moderate-cardinality key joined to a SMALL parent** (o_custkey/300→customer,
s_nationkey→nation, p_brand/p_size→part, c_nationkey→nation), or over a
moderate CTE_SCAN root. Within that:

- CTE-body aggregates: `COUNT(*)`, `SUM` over **integer** columns, `MIN`/`MAX`
  over **integer** widths (TINYINT/SMALLINT-U/MEDIUMINT/INT/BIGINT-U) and
  **scale-0 DECIMAL**. GROUP BY one or multiple direct columns.
- Main: scalar re-aggregation, `GROUP BY` a parent column over a CTE agg leaf
  (P-GB), INNER + LEFT joins, anti-join (`WHERE cte_col IS NULL`),
  LEFT→INNER promotion, multiple CTEs, `cte JOIN real_table`, chained
  CTE-of-CTE, scalar CTEs (single, consumed by an aggregating main),
  IS NULL/IS NOT NULL on a CTE aggregate output, OR/DNF + AND + all 6
  comparison operators in CTE-body and main WHERE, signed-int col-vs-col on
  orders→customer, multi-batch (>256-group) redistribution.

Green case counts (base suite): agg 8, filter 27, index 12, joins 13,
chain_scalar 14, mainmode 7 (+ smoke) — **~81 green cases**, replicated ×4
topologies.

## NEXT-PHASE backlog (severity-ordered)

### CRASHES (data node) — highest priority

| ID | Shape | Crash | Repro / location |
|----|-------|-------|------------------|
| **D6** | scalar agg over a CTE keyed by a high-cardinality key (l_orderkey/1500) used as CTE_LOOKUP child of a LARGE root scan (orders), OR as a high-cardinality CTE_SCAN root | DBSPJ ndbassert **Error 2343, DbspjMain.cpp:7825, `requestPtr.p->m_cnt_active == 0`** (both nodes) | `WITH li AS (SELECT l_orderkey AS k, SUM(l_quantity) AS sq FROM lineitem GROUP BY l_orderkey) SELECT SUM(li.sq) FROM orders AS o JOIN li ON li.k=o.o_orderkey;` — agg-06/agg-19. Traces in `findings/crash_artifacts/`. Same-shape agg-04 (300-key→small parent) PASSES |
| **D18** | main query RE-AGGREGATES a string (CHAR/VARCHAR) MIN/MAX CTE output | DATA NODE CRASH **NDB Error 6000 / Signal 6 abort** | `WITH ord AS (SELECT o_custkey AS k, MIN(o_orderstatus) AS mn, MAX(o_orderstatus) AS mx FROM orders GROUP BY o_custkey) SELECT MIN(ord.mn), MAX(ord.mx) FROM customer AS c JOIN ord ON ord.k=c.c_custkey;` — agg-11..15. Numeric MIN/MAX re-agg works; string does not |
| **D23** | projection-only main SELECT where a CTE_LOOKUP child is joined but has NO user-selected columns | DATA NODE CRASH / **SIGSEGV (NDB Error 6000 / Signal 11)** — kernel CTE_LOOKUP emit/projection segfault, projection-only-path-specific (anti-join/aggregating J8/J9 with an unprojected CTE child are fine); surfaced after the D3 fix | `WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT c.c_custkey FROM customer AS c JOIN cust ON cust.k = c.c_custkey WHERE c.c_custkey <= 20;` — MM17 (disabled). Traces: findings/crash_artifacts/d23_* |

### HANGS

| ID | Shape | Repro |
|----|-------|-------|
| **D2 ✅ RESOLVED by D3 (2026-06-08)** | `COUNT(<column>)` (vs `COUNT(*)`) in a CTE body | The agg-02 repro was projection-only over a CTE_LOOKUP (D3), not a COUNT(col) bug. COUNT(col) verified correct in aggregating + projection-only main; regression test `ronsql_cte_dd_d2_countcol` |
| **D3 ✅ FIXED (2026-06-08)** | projection-only main SELECT over a CTE_LOOKUP child (no main aggregate) | `SELECT cte.col FROM real JOIN cte ON ...` (agg-03, mainmode MM1-6/13/14/16). Fixed: `execute_passthrough_drain` now registers a dummy `getValue` on the main root op when no output reads it (RonSQLPreparer.cpp; mirrors testCteNdbApi.cpp Test 17). Regression: `ronsql_cte_dd_d3_hang.test` |
| **D4 ✅ REJECTED (2026-06-08)** | CTE-body signed-int col-vs-col over lineitem feeding P-GB | `... WHERE l_quantity < l_partkey ...` (filter-13). Was a hang (non-indexed left col → TABLE_SCAN attr-vs-attr NdbScanFilter); now a clean `RonSQLPermanentError` via `check_no_cte_body_col_vs_col` in `build_cte_scopes`. Rejection-asserts: filter-13 + `ronsql_cte_dd_d4_colvscol`. Supporting col-vs-col in a CTE-body filter is tracked for a later phase |
| **D5 ✅ REJECTED (was crash, 2026-06-08)** | 3-table chain `real JOIN cte JOIN real` (real-table child alongside a CTE_LOOKUP child under one parent) | customer JOIN cust JOIN nation (J16/J17). **NOT a hang — it CRASHED the RDRS server** (`NdbQueryBuilder.cpp:3020` `appendLinkedOperand` assert / null deref). The pushed aggregation needs columns from BOTH children of customer (SUM over CTE child `cust` + GROUP BY real child `nation`) — a fan-out whose agg references a NON-ANCESTOR sibling, which the NDB API ancestor-only linked-operand model cannot serialize (only CTE_LOOKUP siblings get linearized via `tree_parent_op_idx`). RonSQL now rejects cleanly at prepare (`linked_source_is_leaf_ancestor` guard in `emit_child_ops`). Re-enabled as rejection-asserts: J16/J17. Supporting fan-out agg across a real-table/CTE sibling is a later-phase feature |
| **D19 ✅ NOT-REPRODUCIBLE (2026-06-08)** | `real JOIN cte` with a main-query WHERE on a parent column | `WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT cust.k, SUM(cust.t) FROM customer AS c JOIN cust ON cust.k=c.c_custkey WHERE c.c_nationkey=5 GROUP BY cust.k;` (J18). Does not reproduce on the current build — executes correctly + matches MySQL. EXPLAIN showed the parent WHERE leaves the plan structurally identical (ROOT TABLE_SCAN + CTE_LOOKUP agg-leaf); the recorded hang was a **stale rdrs2/ndbmtd at parallel-authoring time** (CTE join-agg routing/teardown hardening was already in HEAD). Re-enabled green: J18 + standalone `ronsql_cte_dd_d19_hang`. **Lesson: re-verify other disabled hangs (D5/D20) against freshly-built binaries before treating them as live bugs.** |
| **D20 ✅ NOT-REPRODUCIBLE (2026-06-08)** | multi-key CTE complete-key lookup (any predicate order) | `JOIN x ON x.k1=.. AND x.k2=..` over a 2-col virt-PK CTE (J14/J15). Does not reproduce on the current build — executes correctly + matches MySQL in both predicate orders (verified J15 reversed; J14 in-order re-enabled). Another stale-binary artifact like D19. Re-enabled green: J14 + J15 |

### ERRORS (clean permanent / retryable)

| ID | Shape | Error |
|----|-------|-------|
| **D1** | `SUM(<DECIMAL col>)` in a CTE body | "SUM over this column type in CTE not yet supported" (RonSQLPreparer.cpp:7090 — SUM supports int + FLOAT/DOUBLE only) |
| **D10** | `IS NULL` / `IS NOT NULL` in a **CTE body** WHERE | "Non-boolean term in WHERE condition" (RonSQLPreparer.cpp:8507; no `T_IS` arm in `apply_filter`). Works on the main-query CTE_LOOKUP output |
| **D11** | `GREATEST`/`LEAST` in a **CTE body** WHERE term | parser "Syntax error" (grammar allows them only in top-level SELECT scalar position) |
| **D12 ✅ REJECTED (2026-06-08)** | CTE-body signed-int col-vs-col over orders | Was `RonSQLRetryableError` ×10 (indexed left col mis-classified as an index bound → `encode_constant` fails on the column RHS, thrown retryable). Now a clean `RonSQLPermanentError` via the same `check_no_cte_body_col_vs_col` fix as D4. Rejection-assert: filter-12 + `ronsql_cte_dd_d4_colvscol` |
| **D17** | `MIN`/`MAX` over a **DATE** column in a CTE | "Failed writing aggregation program. Please report a bug." |

### WRONG VALUES / FORMAT / SEMANTICS

| ID | Shape | Divergence |
|----|-------|-----------|
| **D15** | scale-2 DECIMAL MIN/MAX delivered to output (and re-aggregated) | RonSQL drops trailing zeros (`20055.00`→`20055`, `5275.50`→`5275.5`); values correct, scale lost (agg-01/08/09, J3, cs09/cs11). Could be made green via output canonicalization |
| **D16** | scalar `COUNT(*)` over **empty** input | returns NULL (RonSQL) vs 0 (MySQL) (index-17, cs20) |
| **D21** | partial-key / wrong-column-bound multi-key CTE lookup | EXECUTED + returned rows instead of cleanly rejecting; value-correctness vs MySQL unverified (J19/J20 removed; clean rejection covered by `ronsql/t/ronsql_cte_partial_key.test`) |

Full per-row detail (symptoms, exact repros, source line cites) is in
`mysql-test/suite/ronsql_cte/findings/_discovery_log.md`; per-family disposition
in `findings/<family>.md`.

## Verification

- Build: `cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2` (already built).
- Base: `cd mysql-test && ../debug_build/mysql-test/mtr --suite=ronsql_cte` — all green.
- Topologies: `... --suite=ronsql_cte_ng1r3` (and ng2r2/ng2r3/ng4r2) — same green
  set under 3/4/6/8 data nodes; any divergence is a multi-node redistribution gap.
- Regression guard: `../debug_build/mysql-test/mtr --suite=ronsql` still green
  (the new suites only read its include files).

## Notes for the next dev phase

- The two **crashes (D6, D18)** are the priority — both have preserved traces /
  exact asserts and minimal repros above.
- The **projection-only-over-CTE_LOOKUP hang (D3)** and **multi-key lookup hang
  (D20)** block whole categories of otherwise-natural queries.
- **D1 (SUM over DECIMAL)** and **D15 (DECIMAL MIN/MAX scale)** together mean
  DECIMAL aggregate support is weak; D15 specifically may be a cheap win
  (output formatting).
- To re-enable a backlog item once fixed: find its `# NEXT-PHASE` marker in the
  relevant `body_<family>.inc`, restore the query, and `--record`.
