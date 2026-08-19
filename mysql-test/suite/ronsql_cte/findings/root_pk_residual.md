# root_pk_residual family — findings

Phase 0a of `non_aggregate_phase_0.md` (root PK-equality cover +
residual WHERE conjuncts).  Cases rpr-1..12 lock in the fixed behavior;
before the fix, rpr-1..10 returned wrong results (residual conjuncts
silently dropped by the PK-covered `emit_root_op` branches) and rpr-P1
succeeded wrongly.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| Full-key CTE-root lookup + col-vs-col residual atom | `WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > cf.k;` | rejection-assert (rpr-P1) | CTE_LOOKUP jump-table filter has no col-vs-col atom emission (RonSQL surface; kernel supports it — see cte_filter_plan.md) | body_root_pk_residual.inc |
| Aggregate query, no CTE, full-PK-covered root (lookup-rooted TCKEYREQ + JoinAgg) | `SELECT COUNT(*) AS c FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey WHERE o.o_orderkey = 77;` | **data-node crash found by rpr-1 on first record; FIXED** — emit-side suppression (RonSQLPreparer emit_root_op scan fallbacks) + prepare-time QRY_WRONG_OPERATION_TYPE in NdbQueryDefImpl | JoinAgg protocol has no lookup-request setup path (DBTC JOIN_AGG_SETUP is SCAN_TABREQ-only); DBSPJ lookup_send ndbrequire(m_aggNodes.get(nodeId)) at DbspjMain.cpp:8748 fired on empty m_aggNodes.  Pre-existing (reachable pre-Phase-0a); every earlier green PK-equality case involved a CTE (scan-rooted compound query, fpw-6 shape) | body_root_pk_residual.inc |
