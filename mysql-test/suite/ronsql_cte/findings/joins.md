# Findings — `joins` family (data-driven CTE suite)

Join shapes with an aggregating CTE. MAIN cases exercise SUPPORTED shapes from
the capability matrix; probes record unsupported / uncertain shapes.

## MAIN cases (15 enabled, all aggregate in main — expected green)

Every enabled MAIN case has an AGGREGATING main SELECT over a CTE keyed by
o_custkey → customer (or the CTE as root joined to customer). None projection-only
(the corrected envelope: projection-only main SELECTs over a CTE_LOOKUP HANG).

| Id | Shape |
|----|-------|
| J1 | real_table JOIN cte, complete key, INNER; SUM re-aggregated in main, GROUP BY CTE key |
| J2 | INNER JOIN cte (COUNT per custkey), main groups by parent VARCHAR column (c_mktsegment) |
| J3 | INNER JOIN cte with MIN+MAX+COUNT, main groups by parent c_nationkey |
| J4 | LEFT JOIN, CTE omits keys 151..300 (`WHERE o_custkey <= 150`) → genuine NULL injection, SUM over LEFT-joined aggregate per custkey |
| J5 | LEFT JOIN over omitted-key CTE, main groups by parent c_nationkey (mix of matched + NULL-injected groups) |
| J6 | cte JOIN real_table (CTE as scanned root, customer joined), group by parent column |
| J7 | cte JOIN real_table, CTE root, group by CTE key |
| J8 | anti-join: `LEFT JOIN cte ... WHERE cust.t IS NULL` (only omitted customers 151..300 survive); main now AGGREGATES the surviving parent rows (`COUNT(*), MIN/MAX(c_custkey), SUM(c_nationkey)`) — converted from projection-only per the corrected envelope |
| J9 | anti-join on CTE GB key (`WHERE cust.k IS NULL`), aggregate over parent |
| J10 | two independent aggregating CTEs (totals, counts) joined to the same parent |
| J11 | two CTEs (SUM, MAX), both joined to one parent, group by CTE key |
| J12 | LEFT→INNER promotion: `LEFT JOIN cust + WHERE cust.t > 50000` (non-null-preserving on SUM output) |
| J13 | LEFT→INNER promotion via `WHERE cust.k > 100` (on CTE GB key) |
| J14 | multi-key CTE complete-key lookup: GROUP BY (o_custkey, o_orderstatus); join binds BOTH virtual-PK cols (k1, k2), main aggregates |
| J15 | same multi-key CTE, reversed predicate order (k2 first) — accepted/reordered, main aggregates |

### DISABLED MAIN cases

| Id | Shape | Disposition |
|----|-------|-------------|
| J16 | 3-table INNER chain: customer JOIN orders-CTE JOIN nation, group by n_regionkey, SUM over INT | NEXT-PHASE-disabled (HANG, D5) |
| J17 | 3-table INNER chain, COUNT+SUM, group by n_name | NEXT-PHASE-disabled (HANG, D5) |
| J18 | real_table JOIN cte with main-query WHERE on a parent column (c_nationkey = 5) | NEXT-PHASE-disabled (HANG, D19) |

J16 was previously shielded by the SUM-over-DECIMAL prepare error (D1). After the
global SUM-fix (`SUM(o_shippriority)`, an INT), J16 now actually EXECUTES the same
3-table real-cte-real chain shape as J17 and ALSO hangs — so it is disabled too
(same D5 finding: a real-table child alongside a CTE_LOOKUP child under one parent
appears unsupported / buggy on this build). Both are recorded commented-out under
NEXT-PHASE markers in body_joins.inc.

Note on NULL injection (J4/J5/J8/J9/J12/J13): every custkey 1..300 has 5 orders,
so an unfiltered orders CTE matches every customer. To force unmatched parent
rows, the CTE body filters `WHERE o_custkey <= 150`, materialising only keys
1..150; customers 151..300 then inject NULL on the LEFT JOIN. (The guide's hint
to use `WHERE o_orderkey <= 750` does NOT omit any custkey because
`o_custkey = ((orderkey-1)%300)+1` means orderkeys 1..300 alone already cover
all 300 custkeys — corrected to a direct `o_custkey <=` predicate here.)

J14/J15 reference `o_orderstatus` (CHAR(1)) only as a join column; there is no
string LITERAL in the SQL, so the `let $QUERY=` path is safe (no QUERY_FILE
needed). QUERY_FILE would only be required if a status value literal like
`'O'` appeared in the query text.

## Probes

| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |
|-------|---------------------|-------------|----------------------------|----------|
| 3-table INNER chain: real parent JOIN CTE_LOOKUP child JOIN real-table child (region key group), SUM over INT | `WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT n.n_regionkey, SUM(cust.t) FROM customer AS c JOIN cust ON cust.k = c.c_custkey JOIN nation AS n ON n.n_nationkey = c.c_nationkey GROUP BY n.n_regionkey;` | NEXT-PHASE-disabled (HANG) | D5 — real-table child alongside a CTE_LOOKUP child under one parent | body_joins.inc J16 (commented) |
| 3-table INNER chain: real parent JOIN CTE_LOOKUP child JOIN real-table child (n_name group), COUNT+SUM over INT | `WITH cust AS (SELECT o_custkey AS k, COUNT(*) AS n, SUM(o_shippriority) AS prio FROM orders GROUP BY o_custkey) SELECT n.n_name, SUM(cust.n), SUM(cust.prio) FROM customer AS c JOIN cust ON cust.k = c.c_custkey JOIN nation AS n ON n.n_nationkey = c.c_nationkey GROUP BY n.n_name;` | NEXT-PHASE-disabled (HANG) | D5 — same chain shape as J16 | body_joins.inc J17 (commented) |
| real_table JOIN cte with a main-query WHERE on a parent column (was J18) | `WITH cust AS (SELECT o_custkey AS k, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey) SELECT cust.k, SUM(cust.t) FROM customer AS c JOIN cust ON cust.k = c.c_custkey WHERE c.c_nationkey = 5 GROUP BY cust.k;` | NEXT-PHASE-disabled (HANG) | D19 — a main-query WHERE on a parent column over a real_table JOIN cte hangs RonSQL/RDRS | body_joins.inc (J18 commented) |
| Partial-key CTE lookup to a NON-root parent (binds 1 of 2 virt-PK cols, I.16c rewrite cannot apply) | `WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, SUM(o_shippriority) AS t FROM orders GROUP BY o_custkey, o_orderstatus) SELECT n.n_name, SUM(x.t) AS s FROM customer AS c JOIN nation AS n ON n.n_nationkey = c.c_nationkey JOIN x ON x.k1 = c.c_custkey GROUP BY n.n_name;` | rejection-assert | Phase I.16a / I.20 (partial-key CTE_LOOKUP clean reject) | body_joins.inc J19 |
| Full key-count but wrong CTE output column bound (binds k1 + agg output cnt, not k2 — virt PK not covered) | `WITH x AS (SELECT o_custkey AS k1, o_orderstatus AS k2, COUNT(*) AS cnt FROM orders GROUP BY o_custkey, o_orderstatus) SELECT o2.o_custkey, SUM(x.cnt) AS s FROM orders AS o2 JOIN x ON x.k1 = o2.o_custkey AND x.cnt = o2.o_shippriority GROUP BY o2.o_custkey;` | rejection-assert | Phase I.20 (ordered virt-PK coverage check) | body_joins.inc J20 |
| CTE_SCAN as an outer-join child (scanned CTE on the dependent side of a LEFT JOIN, not bound as CTE_LOOKUP) | `WITH bycust AS (SELECT o_custkey AS k, SUM(o_totalprice) AS t FROM orders GROUP BY o_custkey) SELECT n.n_nationkey, SUM(bycust.t) FROM nation AS n LEFT JOIN bycust ON bycust.k > n.n_nationkey GROUP BY n.n_nationkey;` | NEXT-PHASE-disabled | defensively rejected — cte_outer_join_phase_3.md (kernel drop), RonSQL Phase G defensive reject | body_joins.inc (commented) |
| AVG in a CTE body joined to a parent | `WITH cust AS (SELECT o_custkey AS k, AVG(o_totalprice) AS a FROM orders GROUP BY o_custkey) SELECT cust.k, SUM(cust.a) FROM customer AS c JOIN cust ON cust.k = c.c_custkey GROUP BY cust.k;` | NEXT-PHASE-disabled | AVG deferred (matrix: only COUNT/SUM/MIN/MAX) | body_joins.inc (commented) |

### Notes / surprises

- The two rejection-asserts are kept ENABLED (run via `$RONSQL_CLI_EXE
  ... --error 1`) per the guide: prepare-time rejection means the join
  dictionary-cache caveat that forces `$suppress_ronsql_cli=yes` for the green
  RDRS-compare cases does not apply to a query that never reaches execution.
- J19 deliberately makes the partial-key CTE join target a non-root parent
  (`nation` is the joined real table; the CTE binds against `c.c_custkey` where
  `c` is no longer the rewrite-eligible root in this chain) so the I.16b/c
  auto-rewrite to a CTE_SCAN root cannot rescue it — mirroring
  ronsql_cte_partial_key.test Test 10. A bare two-table partial-key INNER join
  would instead be auto-rewritten to a CTE_SCAN root (supported, NOT a
  rejection) — see ronsql_cte_partial_key.test Test 4.
- The CTE_SCAN-outer-join-child probe is left commented (form 2) rather than as
  a `--error 1` assert because it is uncertain whether the front-end emits a
  clean permanent reject or whether the shape would reach the kernel's
  defensive path; the orchestrator can promote it to a rejection-assert at
  record time if it proves to fail cleanly at prepare.
