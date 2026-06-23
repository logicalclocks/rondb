# Data-driven CTE test authoring guide (ronsql_cte suite)

## ⚠️ CORRECTED ENVELOPE (post-discovery — SUPERSEDES the capability matrix below)

A first recording pass revealed the original matrix was too optimistic. The
real, empirically-confirmed envelope for THIS build:

**GREEN (confirmed works) — build MAIN cases ONLY from these:**
- The main SELECT **MUST aggregate** — either a scalar aggregate
  (`SELECT MIN(cte.x), MAX(cte.y), SUM(cte.z), COUNT(*) FROM ...`) or
  `GROUP BY <col>` with aggregates. **Projection-only main SELECTs over a
  CTE_LOOKUP (selecting CTE columns without wrapping them in an aggregate)
  HANG** — never do it.
- CTE-body aggregates: `COUNT(*)` (NOT `COUNT(<col>)` — hangs); `SUM` over
  **integer** columns only (o_shippriority, l_quantity, p_size, s_total_sales,
  n_regionkey, c_nationkey — NOT DECIMAL: already globally fixed, do not
  reintroduce `SUM(<decimal>)`); `MIN`/`MAX` over ANY type incl DECIMAL / CHAR /
  VARCHAR / int / float / double.
- CTE keyed by a moderate-cardinality key joined to a **SMALL parent**:
  o_custkey(300)→customer, s_nationkey→supplier/nation, p_brand→part,
  n_regionkey→region. **AVOID** a CTE keyed by a high-cardinality key
  (l_orderkey/1500) used as a lookup under a LARGE root scan (orders) — CRASHES.
- GROUP BY a parent-table column over a CTE aggregate leaf (P-GB) works.
- CTE-body WHERE (=, !=, <, <=, >, >=, AND, OR, IS NULL/IS NOT NULL on o_clerk),
  string-equality (QUERY_FILE), GREATEST/LEAST — all with an AGGREGATING main.

**FORBIDDEN (disable as `# NEXT-PHASE` probe + record in findings — DO NOT keep enabled):**
- projection-only main SELECT over a CTE_LOOKUP (HANG, D2/D3)
- `COUNT(<col>)` in a CTE (HANG, D2)
- `SUM(<DECIMAL col>)` in a CTE (ERROR, D1) — already sed-fixed; keep one probe
- CTE-body col-vs-col over lineitem feeding P-GB (HANG, D4)
- 3-table chain `real JOIN cte JOIN real` (HANG, D5)
- scalar agg over a high-cardinality-key CTE under a large root scan (CRASH, D6)
- chained CTE-of-CTE referencing an inner aggregate (ERROR "unresolved source
  column", D7 — may resolve now that the inner SUM is int; if it still errors,
  disable)
- scalar-CTE comma cross-join / watermark GREATEST over two scalar CTEs (ERROR
  "Failed to create child operation", D8)
- CTE-body index-scan root on DATE / composite bounds (ERROR "Failed to create
  CTE body index-scan root", D9)
- projection emulated via `GROUP BY <all cte cols>` (ERROR "Not an aggregate")
- ORDER BY / LIMIT / DISTINCT / HAVING / AVG / COUNT(DISTINCT) (unsupported)

**Conversion rule:** if a case was projection-only for type coverage (e.g.
`SELECT cte.mn, cte.mx FROM real JOIN cte`), CONVERT it to aggregating
(`SELECT MIN(cte.mn), MAX(cte.mx), SUM(cte.n) FROM real JOIN cte`) so the type
coverage stays GREEN, and add ONE representative disabled probe documenting the
projection-only-hang gap. Leave the `--let $strict_diff=` line exactly as is
(the orchestrator flips it). Do NOT run mtr.

---


Shared reference for the test-driven CTE phase. Each "family" subagent reads
this, then authors its body include + thin wrapper + findings fragment.

## Mission & philosophy

Author ONE family of MTR tests for the `ronsql_cte` suite. Tests run the SAME
SQL via the mysql client (baseline/oracle) and via RonSQL (RDRS REST); the
harness sorts both TSV outputs and diffs them — a divergence fails the test.
CTEs always do aggregation; main SELECTs may be normal joins or aggregation.
The point of this phase is to (a) lock in supported shapes on realistic data
and (b) surface unsupported shapes as recorded, disabled probes.

## Hard constraints

- **DO NOT run mtr, start any cluster, or run ronsql_cli.** Author files only.
  The orchestrator records `.result` files and drives the discovery loop.
- Repo root: `/Users/mikael/mysql_trees/rondb_1072_CTE_test`
- **Every table alias MUST use explicit `AS`** (RonSQL's parser rejects
  implicit aliases like `orders o`; write `orders AS o`). CTE names are
  referenced WITHOUT an alias, e.g. `JOIN cust ON cust.k = c.c_custkey`.
- Do NOT create `.result` files. Do NOT modify cte_schema.inc / cte_data.inc.

## Schema (created by suite/ronsql_cte/include/cte_schema.inc, all ENGINE=NDB)

- `region(r_regionkey TINYINT PK, r_name CHAR(12))` — 5 rows, keys 0..4
- `nation(n_nationkey TINYINT PK, n_name CHAR(15), n_regionkey TINYINT)`
  idx(n_regionkey) — 25 rows, keys 0..24, n_regionkey = key%5
- `customer(c_custkey INT PK, c_nationkey TINYINT, c_mktsegment VARCHAR(12),
  c_acctbal DECIMAL(12,2))` idx(c_nationkey), idx(c_mktsegment) — 300 rows, keys 1..300
- `supplier(s_suppkey INT PK, s_nationkey TINYINT, s_acctbal DECIMAL(12,2),
  s_total_sales BIGINT UNSIGNED, s_margin DECIMAL(8,0))` idx(s_nationkey) — 50 rows, keys 1..50
- `part(p_partkey INT PK, p_name VARCHAR(40), p_brand CHAR(10), p_size MEDIUMINT,
  p_retailprice DECIMAL(12,2))` idx(p_brand), idx(p_size) — 100 rows, keys 1..100
- `orders(o_orderkey INT PK, o_custkey INT, o_orderdate DATE, o_orderstatus CHAR(1),
  o_totalprice DECIMAL(12,2), o_shippriority SMALLINT UNSIGNED, o_clerk INT NULL)`
  idx(o_custkey), idx(o_orderdate), idx(o_orderstatus,o_orderdate), idx(o_clerk)
  — 1500 rows, keys 1..1500
- `lineitem(l_orderkey INT, l_linenumber TINYINT, l_partkey INT, l_suppkey INT,
  l_quantity INT, l_extendedprice DECIMAL(12,2), l_discount FLOAT, l_tax DOUBLE,
  l_shipdate DATE, l_returnflag CHAR(1), PK(l_orderkey,l_linenumber))`
  idx(l_partkey), idx(l_shipdate) — 3000 rows (2 per order)

## Data semantics (deterministic — for choosing meaningful thresholds; you do NOT compute expected values, the mysql client does at record time)

- customer: c_nationkey = custkey%25; c_mktsegment = ELT(custkey%5+1,
  'AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'); c_acctbal =
  custkey*17.25+100.50 (≈117..5275)
- supplier: s_nationkey = suppkey%25; s_acctbal = suppkey*31.40+50; s_total_sales
  = suppkey*100000; s_margin = suppkey*3
- part: p_name = CONCAT('part#',partkey); p_brand = ELT(partkey%5+1,'BRAND#11',
  'BRAND#22','BRAND#33','BRAND#44','BRAND#55'); p_size = partkey%50+1;
  p_retailprice = partkey*9.10+900
- orders: o_custkey = ((orderkey-1)%300)+1 (custkeys 1..300, 5 orders each);
  o_orderdate = '1995-01-01' + (orderkey%1000) days; o_orderstatus =
  ELT(orderkey%3+1,'O','F','P'); o_totalprice = orderkey*13.37 (≈13..20055);
  o_shippriority = orderkey%5 (0..4); o_clerk = NULL when orderkey%7==0 else
  (orderkey%40)+1  (~1/7 NULLs)
- lineitem (per order, linenumber k∈{1,2}): l_partkey=(orderkey*7+k)%100+1;
  l_suppkey=(orderkey*3+k)%50+1; l_quantity=(orderkey+k)%50+1;
  l_extendedprice=(orderkey+k)*7.77; l_discount=(orderkey%10)*0.01 (FLOAT 0..0.09);
  l_tax=(orderkey%8)*0.01 (DOUBLE); l_shipdate='1995-01-02'+(orderkey*2+k)%1000 days;
  l_returnflag=ELT((orderkey+k)%3+1,'N','R','A')

Key facts: `GROUP BY o_custkey` → 300 groups (>256, crosses the API batch
boundary). o_clerk has ~1/7 NULLs (use for NULL-semantics tests).

## Body-include template

Create `suite/ronsql_cte/include/body_<FAMILY>.inc`, self-contained:

```
# body_<FAMILY>.inc — <Family> family, data-driven CTE suite.
--source include/have_ndb.inc
--disable_warnings
call mtr.add_suppression("Schema dist coordinator detected timeout");
call mtr.add_suppression("Participant timeout");
--enable_warnings
--let $suppress_ronsql_cli=yes
--let $strict_diff=yes
--source suite/ronsql_cte/include/cte_schema.inc
--source suite/ronsql_cte/include/cte_data.inc

DELIMITER |;

--echo
--echo === <id>: <description> ===
let $QUERY=
<query, multi-line OK, trailing semicolon then |>;|
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
<SAME query, no leading 'let', ends with | and NO trailing semicolon>|

# ... more cases ...

DELIMITER ;|

--source suite/ronsql_cte/include/cte_drop.inc
```

Thin wrapper `suite/ronsql_cte/t/ronsql_cte_dd_<FAMILY>.test` = ONE line:
`--source suite/ronsql_cte/include/body_<FAMILY>.inc`

For EACH query include BOTH the compare step (`--source .../ronsql_compare.inc`)
and the `== Expected result ==` / `--sorted_result <query>` echo, exactly as shown.

## String-literal queries (single-quoted SQL literals, e.g. `o_orderstatus = 'O'`)

The `let $QUERY=` path breaks on single quotes. Use QUERY_FILE for the compare:

```
--echo === <id>: <description> ===
--let QUERY_FILE=$MYSQL_TMP_DIR/q_<id>.sql
--write_file $QUERY_FILE
<query with 'literals'>;
EOF
--source suite/ronsql/include/ronsql_compare.inc
--echo == Expected result ==
--sorted_result
<same query with 'literals'>|
```

The `--sorted_result` reference runs via mtr's own mysql connection (single
quotes fine there). Read `suite/ronsql/t/ronsql_cte_basic.test` Test 20 for the
exact pattern. Keep these within the `DELIMITER |; ... DELIMITER ;|` region.

## EXPLAIN assertions (index family only)

Read `suite/ronsql/include/ronsql_explain.inc` and
`suite/ronsql/t/ronsql_cte_minmax_index.test`. Pattern:
```
--let $EXPLAIN_FILE=$MYSQL_TMP_DIR/expl_<id>.out
let $QUERY=<query>;|
--source suite/ronsql/include/ronsql_explain.inc
--exec grep -qF 'INDEX_SCAN using <idxname>' $EXPLAIN_FILE
--remove_file $EXPLAIN_FILE
```
Use a STABLE substring (e.g. the index name) in `grep -qF`; the orchestrator
will tighten the exact string at record time.

## Capability matrix

**SUPPORTED** (build MAIN/green cases from these):
- CTE-body aggregates: COUNT(*), COUNT(col), SUM, MIN, MAX. GROUP BY one or more
  DIRECT columns (no expressions).
- CTE-body WHERE: `= != < <= > >=`; AND; OR/DNF; IS NULL / IS NOT NULL;
  column-vs-column (SIGNED integer only); CASE (conditions on CTE COLUMN
  projections); GREATEST/LEAST (2+ args, columns and/or constants).
- CTE-body index: WHERE bound on an indexed col → INDEX_SCAN; composite-index
  leading-eq + range; scalar MIN/MAX over a NOT NULL indexed col via ordered
  index + maxRows=1; index hints FORCE/USE/IGNORE on the CTE-body ROOT scan only.
- MIN/MAX over: all int widths (signed + unsigned), FLOAT, DOUBLE, DECIMAL
  (scale 0 → bigint, scale>0 → double; precision guard signed≤18 / unsigned≤19),
  CHAR/VARCHAR.
- Joins: CTE complete-key lookup child (INNER + LEFT); CTE as scan root;
  anti-join (`LEFT JOIN cte ... WHERE cte_col IS NULL`); multiple CTEs;
  LEFT→INNER promotion (LEFT JOIN + WHERE on a non-null-preserving cte column);
  multi-key CTE complete-key lookup; chained CTE-of-CTE; scalar CTEs (no GROUP
  BY; comma cross-join; watermark GREATEST/LEAST over scalar outputs).
- Main SELECT: real_table JOIN cte; cte JOIN real_table; projection-only joins;
  aggregation over joins; GROUP BY a parent-table column over a CTE aggregate
  leaf (P-GB).
- Multi-batch (>256 groups), multi-node redistribution.

**NOT SUPPORTED / DEFERRED** (use as PROBES):
- AVG; COUNT(DISTINCT).
- GROUP BY an expression; SELECT of a post-aggregation expression (e.g. SUM(x)+1).
- CASE conditions over a CTE AGGREGATE output (CASE over a CTE column projection
  IS supported).
- Partial-key CTE_LOOKUP (binds fewer than all virtual-PK cols) — clean REJECT.
- CTE_SCAN as an outer-join child — defensively rejected.
- String column-vs-column comparison (only signed-int col-vs-col supported).
- MIN/MAX-via-index on a NULLABLE column (falls back to TABLE_SCAN; VALUES still
  correct, only the EXPLAIN plan differs).
- ORDER BY / LIMIT / DISTINCT / HAVING on CTE queries.
- Index hints on a JOINED (non-root) table — rejected.
- BETWEEN, IN(...) list, LIKE in a CTE-body WHERE — UNCERTAIN, treat as probes.
- MIN/MAX over a DATE column — UNCERTAIN, probe.
- FLOAT/DOUBLE SUM may format differently between engines — if used, mark a probe.

## Probe disposition (the recording mechanism)

MAIN cases use high-confidence SUPPORTED shapes. For each uncertain/unsupported
shape, write a PROBE in ONE of two forms:

(1) **CLEAN PERMANENT REJECTION** (matrix says RonSQL refuses, e.g. partial-key):
keep ENABLED as a rejection-assert via ronsql_cli (rejection happens at
prepare time, so the join dictionary-cache caveat does not apply):
```
--echo === <id> (rejection): <description> ===
--error 1
--exec $RONSQL_CLI_EXE --connect-string $NDB_CONNECTSTRING -D test -e '<single-line query>' 2>&1
```

(2) **GENUINE GAP / UNCERTAIN** (wrong answer, crash, or not implemented): write
the query COMMENTED OUT under a marker; do NOT execute it:
```
# NEXT-PHASE: <capability> — <why / matrix reference>
#   <probe query, commented>
```

## Findings fragment

Create `suite/ronsql_cte/findings/<FAMILY>.md` with a table, one row per probe:

`| Shape | Minimal repro query | Disposition | Suspected capability/phase | Location |`

Disposition = `rejection-assert` or `NEXT-PHASE-disabled`. Add a short prose note
on anything surprising.

## Deliverables (author only, no mtr)

1. `suite/ronsql_cte/include/body_<FAMILY>.inc`
2. `suite/ronsql_cte/t/ronsql_cte_dd_<FAMILY>.test` (one-line wrapper)
3. `suite/ronsql_cte/findings/<FAMILY>.md`

Aim for ~12–25 MAIN cases + ~4–10 probes. Read `suite/ronsql/t/ronsql_cte_basic.test`
for exact conventions before writing. Return a concise summary of MAIN vs PROBES.
