# root_pk_residual family — findings

Phase 0a of `non_aggregate_phase_0.md` (root PK-equality cover +
residual WHERE conjuncts).  Cases rpr-1..15 lock in the fixed behavior;
before the fix, rpr-1..10 returned wrong results (residual conjuncts
silently dropped by the PK-covered `emit_root_op` branches) and rpr-P1
succeeded wrongly.

**~~Tracked defect — join-path `unload_schema` always reports "schema
changed".~~  FIXED (August 2026): join-aware version walk.**  The old
comparison read only `m_main_scope.table` + `m_indexes` (populated only
by `load_single_table`), so on the join path `new_indexes_count >=
old_indexes_count` (N ≥ 0) fired at the first online ordered index and
EVERY `RonSQLMaybeStaleSchema` became retryable (10 doomed RDRS
attempts / 3 CLI attempts).  `unload_schema` now branches on
`is_join_query()`: the join arm snapshots the (id, version) of every
HELD dictionary object — each scope's root table, every join-plan op's
table and index, every `body_indexes` entry, across the main scope and
all CTE scopes (the same enumeration the invalidation lambdas walk) —
invalidates everything FIRST (the old code only invalidated the root
pre-reload, so a child's `getTable()` returned the stale cache), then
reloads by name and compares.  The single-table comparison is
byte-identical (st-20 / ronsql_rdrs_basic `RMS->RPE` pins unchanged).
The old CTE-root NULL-table "no change" short-circuit is replaced by
the walk (child ops' real tables now inspected).  Scope limit
(documented in-code): only HELD objects are compared, so a newly ADDED
index on a join table is not detected — fine, since join-path "no
suitable index" errors are RonSQLPermanentError (never RMS).  rpr-P3
pins the single-attempt `RMS->RPE` on a join query with an
ordered-indexed root; rpr-P4 pins the CTE-root arm.  The out-of-range
literal classification stays permanent at the throw site (rpr-P2).

Post-review fix: the original rpr-11/12 were aggregate no-CTE shapes,
which `lookup_root_supported` suppresses BEFORE the 64-word count runs
— they never exercised the over-cap fallback.  Rewritten as
lookup-eligible shapes (pass-through CTE chains; rpr-12 on the
ordered-PK `acct` root) plus new rpr-15 (aggregate + CTE over-cap), so
all three over-cap fallbacks are genuinely entered.

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|---|---|---|---|---|
| Full-key CTE-root lookup + col-vs-col residual atom | `WITH cf AS (SELECT o_custkey AS k, COUNT(*) AS n FROM orders GROUP BY o_custkey) SELECT cf.k, cf.n FROM cf WHERE cf.k = 7 AND cf.n > cf.k;` | **SHIPPED** (rpr-16 rejecting / rpr-16b accepting, `cte_filter_phase_i26.md`): typed-register col-vs-col emission via `read_linked_column_to_reg` — all 10 integer widths + Float/Double, mixed signedness included (here INT GB key vs Bigunsigned COUNT) | cte_filter_phase_i26.md | body_root_pk_residual.inc |
| Aggregate query, no CTE, full-PK-covered root (lookup-rooted TCKEYREQ + JoinAgg) | `SELECT COUNT(*) AS c FROM orders AS o JOIN customer AS cu ON cu.c_custkey = o.o_custkey WHERE o.o_orderkey = 77;` | **data-node crash found by rpr-1 on first record; FIXED** — emit-side suppression (RonSQLPreparer emit_root_op scan fallbacks) + prepare-time QRY_WRONG_OPERATION_TYPE in NdbQueryDefImpl | JoinAgg protocol has no lookup-request setup path (DBTC JOIN_AGG_SETUP is SCAN_TABREQ-only); DBSPJ lookup_send ndbrequire(m_aggNodes.get(nodeId)) at DbspjMain.cpp:8748 fired on empty m_aggNodes.  Pre-existing (reachable pre-Phase-0a); every earlier green PK-equality case involved a CTE (scan-rooted compound query, fpw-6 shape) | body_root_pk_residual.inc |
