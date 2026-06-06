ronsql_cte_dd_agg is intentionally OMITTED from this (2 node group x 3 replica,
6 data node) topology suite: agg-05 (P-GB SUM(COUNT) over a CTE_LOOKUP) returns
wrong group counts ONLY on this topology (D22 — a redistribution / CTE_LOOKUP
fan-out bug; green on 2/3/4/8 nodes). See
storage/ndb/claude_files/pushdown_join_aggregation/cte_test_driven_findings.md (D22).
