# single_row_cte family — findings

Single-row key-lookup CTE bodies (`cte_single_row_kernel_plan.md`,
commit 3): `WITH r AS (SELECT cols FROM t WHERE pk = const)` emitted
with the kernel's CTE_SINGLE_ROW mode — every projected column a
GROUP BY column, zero aggregate slots, states on all nodes with the
constant DBTC-node redistribute owner, subset-key CTE_LOOKUP
consumers (any subset of the outputs, including none = the comma
cross join existence probe).  Cases srb-1..22 lock in the supported
envelope; srb-P1..P11 pin the rejections.

| Shape | Minimal repro query | Disposition | Notes | Location |
|---|---|---|---|---|
| DATE watermark compare via comma join | `... FROM orders AS o2, r WHERE o2.o_orderdate > r.d;` | rejection-assert (srb-P11) | the cross-column jump-table filter's typed-register set (10 int widths + Float/Double) has no DATE arm; DATE EQUI-joins work via subset keys (srb-14) | body_single_row_cte.inc |
| No-FROM qualified reference | `WITH r AS (...) SELECT MAX(r.k);` | rejection-assert (srb-P10) | the scalar-CTE auto-FROM convenience is deliberately not extended to single-row CTEs (an implicit MAX over an EMPTY single-row CTE would turn the empty result into a NULL row); the message points at the explicit comma join | body_single_row_cte.inc |
| Duplicate output columns | `SELECT o_custkey AS a, o_custkey AS b ... WHERE pk = const` | rejection-assert (srb-P7) | not a candidate — needs virt-PK aliasing (deferred); srb-P7 is also the first pin of analyze_ctes' "must contain at least one aggregate function." | body_single_row_cte.inc |
| HAVING in a single-row body | srb-P8 | rejection-assert | HAVING is parsed but unapplied in CTE bodies — the candidacy guard keeps that pre-existing gap from silently extending to single-row bodies | body_single_row_cte.inc |
| `pk = NULL` body | `WITH r AS (SELECT o_custkey AS k FROM orders WHERE o_orderkey = NULL) ...` | rejected (constant accept-list excludes T_NULL) | MySQL returns an empty result; deliberate v1 edge | RonSQLPreparer.cpp enforce_single_row_cte_body |
| DECIMAL / fractional-second temporal outputs on the lookupCte path | (srb-13 covers DECIMAL on the CTE_SCAN/FROM-root path) | known-unexercised edge | `build_cte_virtual_tables` attr-size `default:` arm leaves them at 0 (fixed-width Date/Year/Datetime/Timestamp now populated); the lookupCte packed_rowsize path asserts on 0 — see `project_synthetic_virt_table_gotcha.md` | — |
| Empty intermediate projection starves the API | (block-test finding, not MTR) | pre-existing sharp edge, named follow-up | a scan op with no getValue produces no TRANSID_AI while the completed-ops accounting still announces its rows; testCteNdbApi 27-31 hung on it until they projected on the parent op | cte_single_row_kernel_plan.md |

Baselines pinned unchanged by this feature: gc-P3 (no-WHERE
non-aggregating body — not a candidate, parse-gate coverage message)
×5, cs-probe-5 (aggregate present — not a candidate) ×5,
`ronsql/ronsql_cte_scalar` Test 5.
