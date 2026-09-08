# Shape catalog — RonSQL statements emitted by the Hopsworks builder (P1, draft v1)

**Status: draft v1 (2026-09-08), transcribed from
`PreparedStatementBuilder.java` @ hopsworks_ronsql `f85a653bc`
(branch collect-operation). Support-status column is a reading of this
tree's RonSQL sources and plan docs; nothing was executed. E1/E3
replace every "verify" with a recorded result.**

Source lines refer to
`hopsworks-api/src/main/java/io/hops/hopsworks/api/featurestore/trainingdataset/PreparedStatementBuilder.java`
unless stated otherwise.

## 0. Conventions shared by all shapes

- **Table name**: `` `<fg.name>_<fg.version>` ``, unqualified. The
  database name rides on the DTO (`ronsqlDatabase` = the project's
  online feature-store DB). MySQL twins are `` `<db>`.`<table>` ``.
- **Identifiers**: always backticked, never escaped (upstream feature
  name validation is the guarantee). Column order = feature-group
  declared order (`LinkedHashMap`, :239-250).
- **`?` markers** (`ServingPreparedStatementDTO.java:38-41`): RonSQL
  has no binding; the client substitutes typed literals in parameter
  order. Rules the framework's `bind` layer must implement:
  - integer/decimal types → bare number; DOUBLE/FLOAT → bare number;
  - string types (`VARCHAR(100)`) → `'…'` with `'` doubled;
  - DATE → `'YYYY-MM-DD'`; TIMESTAMP → `'YYYY-MM-DD HH:MM:SS[.ffffff]'`
    in UTC (RonSQL is UTC-only; the MySQL session must run with
    `time_zone='+00:00'`);
  - batch `IN (?)` → `IN (v1, v2, …)` typed as above, no tuples;
  - the trailing window `?` (when `aggregateWindow != null`) →
    TIMESTAMP literal of `now_utc - aggregateWindow seconds`, computed
    once per read and shared with the MySQL twin.
- **Entity (bind) keys** (:301-309): the FG primary keys minus the
  collect order column (collect) or minus `fg.eventTime` (aggregate).
  Order = PK declared order. Batch statements need exactly one entity
  key (tuple `IN` is unverified → gate).
- **Select features** (`getSelectFeatures` :1249): non-label, non-
  training-helper features sorted by index; batch adds the PKs;
  offline-only features excluded. Collect FGs replace this with
  `collectSourceFeatures` (:361: PKs first, then struct fields with the
  order column first, deduplicated). Aggregate FGs use
  `aggregateSourceFeatures` for the MySQL point read only.
- **Filters** (`conjunctiveFilterConditions` :703, `renderRonsqlCondition`
  :723, `renderRonsqlLiteral` :754): only for collect/aggregate FGs;
  the persisted tree must be AND-only (an OR → exception); each leaf
  must be `feature <op> literal` with op ∈ `= <> > >= < <= LIKE`; a
  condition pinned to another join index is skipped; numeric literal
  must match `-?\d+(\.\d+)?`; string literal quoted with `''`
  doubling, control chars / backslash refused; boolean refused. The
  rendered text is appended verbatim as `` AND `f` <op> <lit> `` to
  both the RonSQL template and the MySQL twin.
- **Client-side post-processing** is part of the contract and is
  modelled by the L2 oracle (see `fs_ronsql_plan.md` D4).

## 1. Shape table

| Id | Name | Emitter | Single | Batch | RonSQL engine path (expected) | Status here |
|---|---|---|---|---|---|---|
| S1 | Aggregate, point | `applyRonsqlAggregate` | yes | — | single-table aggregate; PK-prefix range on the ordered PK index | supported (verify) |
| S2 | Aggregate, windowed | same + `event_time >= ?` | yes | — | leading-eq + range on `(entity, event_time)` | supported (verify TIMESTAMP literal) |
| S3 | Aggregate, batch | same, `IN (?) … GROUP BY k` | — | yes (1 key) | IN → OR chain; GROUP BY entity | supported; plan/perf to verify |
| S4 | Aggregate + filters | S1-S3 + `renderRonsqlCondition` | yes | yes | residual filter in interpreted code | supported (verify `<>`, LIKE collation) |
| S5 | Aggregate GREATEST/LEAST fold | `MAX(GREATEST(a,b)) AS a_b_greatest` | yes | yes | arith GREATEST inside MAX | supported (integer operands) |
| S6 | Collect last-N, CTE form | `buildRonsqlCollectTemplate` | yes | — | non-aggregate CTE body + ORDER BY/LIMIT, projection-only main over CTE_SCAN | **likely rejected (R1)** |
| S6b | Collect last-N, direct form | (not emitted by Hopsworks today) | yes | — | single-table pass-through, `SF_OrderBy` index-order streaming | supported (Phase 4b) |
| S7 | Snowflake all-INNER, combined | `buildSnowflakeStatement` | yes | yes (1 root key) | CTE_SCAN root → PK_LOOKUP chain, projection-only main | supported per Hopsworks live-verification on 26.04 (verify here) |
| S8 | Snowflake all-LEFT, per-chain | `buildRonsqlSnowflakeTemplates` allLeft | yes | yes | same as S7, one statement per nested node | as S7 |
| S8b | Snowflake LEFT, single statement | (future) `LEFT JOIN … ON` from `b` | yes | yes | CTE_SCAN root → LEFT PK_LOOKUP | engine supports LEFT JOIN; not emitted |
| S9 | Composite entity key variants of S1/S2/S4/S6/S7 | same emitters | yes | gated off | multi-column PK prefix | supported (verify) |
| S10 | String entity key variants | same emitters | yes | yes | VARCHAR PK lookup / IN / GROUP BY | verify (collation) |

### S1 — Aggregate, point (`applyRonsqlAggregate` :484)

```
SELECT <outputs> FROM `<fg>_<v>` WHERE `<k1>` = ?[ AND `<k2>` = ?][<extraWhere>];
```
`<outputs>` (`aggregateOutputs` :442) in aggSpec insertion order, one
term per (key, fn):

| aggSpec key | fn | output term |
|---|---|---|
| `f` | count/sum/min/max/avg | `` FN(`f`) AS `f_fn` `` |
| `*` | count | `` COUNT(*) AS `count` `` |
| `a,b[,c…]` | greatest/least | `` MAX(GREATEST(`a`, `b`)) AS `a_b_greatest` `` |

Example (test :375-379): `` SELECT COUNT(`regular_feature`) AS
`regular_feature_count`, MAX(GREATEST(`regular_feature`,
`regular_feature_2`)) AS `regular_feature_regular_feature_2_greatest`
FROM `fg_test_1` WHERE `pk_feature` = ?; ``

MySQL twin (`applyMysqlAggregate` :531): identical outputs but aliases
carry the join prefix, table qualified with the DB, no trailing `;`:
`` SELECT SUM(`regular_feature`) AS `regular_feature_sum` FROM
`project_fs1`.`fg_test_1` WHERE `pk_feature` = ? ``. Result is always
exactly one row (scalar aggregate): COUNT → 0, others → NULL when no
rows match. Client applies the prefix after the RonSQL fetch;
`aggregateFeatureNames` names the outputs.

Data requirements: history layout, PK `(entity…, event_time)` with the
default hash+ordered PK index (a `USING HASH` PK has **no** ordered
index, so an entity-prefix lookup becomes a table scan on both
engines — include one hash-only FG to measure that).

Definition-time constraints (`QueryController.java:323-455`): fn ∈
{count,sum,min,max,avg}; sum/avg need numeric; min/max not over
complex types; greatest/least need ≥ 2 **integer** features; output
names unique.

### S2 — Aggregate, windowed

S1 plus `` AND `<eventTime>` >= ? `` appended last (:507-510) when
`aggWindow != null` and the FG declares an event time. The DTO carries
`aggregateWindow` (seconds). Gates at definition time: event_time is
TIMESTAMP and the **last** PK column, window ≤ 100 years and ≤ TTL.

Bound literal: `'YYYY-MM-DD HH:MM:SS'` (UTC). Inclusivity: `>=`.
Data requirements: `event_time` values that straddle typical window
bounds (e.g. rows at exactly `now - window`) so inclusivity is tested;
TIMESTAMP(0) and TIMESTAMP(3) columns (fractional-second formatting).

### S3 — Aggregate, batch (single entity key)

```
SELECT `<k>`, <outputs> FROM `<fg>_<v>` WHERE `<k>` IN (?)[<extraWhere>][ AND `<eventTime>` >= ?] GROUP BY `<k>`;
```
(test :403-404). Composite entity keys → no RonSQL statement (the
per-entry single statements remain). MySQL twin: `` SELECT `k` [AS
`prefixk`], outputs … WHERE `k` IN (?) … GROUP BY `k` `` (row-
constructor `IN` for composite keys, MySQL only). The client
synthesizes defaults (COUNT 0, others NULL) for entities missing from
the GROUP BY output.

Engine notes: RonSQL rewrites `IN (a,b,c)` into `k = a OR k = b OR k =
c` at parse time (`RonSQLParser.y` in_list rule). Verify the plan for
10 / 100 / 1000 keys (index ranges vs table scan + filter) and the
request-size limit (`rdrs_size_limits.md`).

### S4 — Filters

Appended to S1–S3 and S6 as `` AND `f` <op> <lit> `` per AND-leaf.
Operator map: `=`, `<>`, `>`, `>=`, `<`, `<=`, `LIKE`. Literals per
§0. Verify on RonSQL: `<>` spelling accepted by the lexer; `LIKE` on
`VARCHAR(100)`; **collation** — Hopsworks FG tables take the server
default charset/collation (`buildCreateStatement` sets none), so MySQL
compares strings case-insensitively under `utf8mb4_0900_ai_ci` while
RonSQL may compare bytes; include mixed-case data and record the
behaviour (potential ledger entry, possibly a Hopsworks gate).

### S5 — GREATEST/LEAST fold

Covered by the S1 output table. Distinct gate: integer operands only
(`isIntegerType`: int, integer, bigint, smallint, tinyint, long). The
RonSQL grammar lowers `GREATEST(...)` in `arith_expr` to an n-ary
program node; MAX over it is a regular aggregate. Also test LEAST and
3-argument lists, and NULL operands (MySQL GREATEST returns NULL if any
argument is NULL).

### S6 — Collect last-N, CTE form (`buildRonsqlCollectTemplate` :780)

```
WITH t AS (SELECT <cols> FROM `<fg>_<v>` WHERE `<k1>` = ?[ AND `<k2>` = ?][<extraWhere>] ORDER BY `<order>` DESC LIMIT <N>) SELECT <cols> FROM t;
```
(test :652-653). `<cols>` = `query.getFeatures()` = PKs first, then the
struct's source columns with the order column first. Always `DESC`;
`collectAscending` only affects the client sort. Batch → no RonSQL
statement (MySQL `ROW_NUMBER()` window handles batches). N is inlined
as an integer.

MySQL twins: `queryOnline` = `` SELECT * FROM (SELECT cols, ROW_NUMBER()
OVER (PARTITION BY pk… ORDER BY order DESC) AS hopsworks_collect_rank
FROM db.fg WHERE pk = ?) AS <alias> WHERE hopsworks_collect_rank <= ?
ORDER BY hopsworks_collect_rank [DESC] `` (`wrapOnlineCollect`,
`ConstructorController.java:658`); `queryOnlineScan` = `` SELECT cols
FROM db.fg WHERE pk = ? ORDER BY order DESC LIMIT ? `` (`wrapOnlineScan`
:702). Client: sort rows by the order column (newest-first, or
oldest-first when `collectAscending`), fold into one
`array<struct<…>>` feature named `<fg>_collect`.

Engine status (R1): `analyze_ctes()` requires GROUP BY + ≥ 1 aggregate
unless the body is a *single-row key lookup* (all PK columns bound by
constants, `RonSQLPreparer.cpp:1905-1925`). This body binds only the
entity prefix, so expect a clean rejection. Record the exact error
text in E1. Options: (a) engine: accept a non-aggregate CTE body with
ORDER BY/LIMIT as a materialized top-N (the kernel already applies
body ORDER BY/LIMIT for aggregate CTEs, `cte_orderby_limit_plan.md`
L4); (b) Hopsworks: emit S6b. The framework keeps both shapes.

### S6b — Collect last-N, direct form (target shape)

```
SELECT <cols> FROM `<fg>_<v>` WHERE `<k1>` = ?[…][<extraWhere>] ORDER BY `<order>` DESC LIMIT <N>;
```
Supported since ORDER BY/LIMIT Phases 3-4b (`ronsql_orderby_limit_plan.md`):
with the ordered PK index `(entity, event_time)` and the entity bound
by equality, EXPLAIN should print `ORDER BY: index order (SF_OrderBy |
SF_Descending …)` and the drain stops at the LIMIT. Pin this with
`ronsql_explain.inc` and `ronsql_phase_rows.inc` (`rows=N`).

### S7 — Snowflake all-INNER, combined (`buildSnowflakeStatement` :1067)

Single-entity (test :533-536):
```
WITH `b` AS (SELECT `<hop1>`[, `<hop2>`…], COUNT(*) AS `hw_cnt` FROM `<root>_<v>` WHERE `<rootpk>` = ?[ AND …] GROUP BY `<hop1>`[, …])
SELECT `j<i>`.`<col>` AS `<prefix><col>`, … FROM `b` JOIN `<child>_<v>` AS `j<i>` ON `j<i>`.`<right>` = `b`.`<left>`[ AND `j<i>`.`<right2>` = `b`.`<left2>`][ JOIN `<gc>_<v>` AS `j<k>` ON `j<k>`.`<right>` = `j<i>`.`<left>`]…;
```
Batch (test :541-544): the root PK is prepended to the CTE select and
GROUP BY, `WHERE `<rootpk>` IN (?)`, and `` , `b`.`<rootpk>` AS
`<rootpk>` `` is appended to the projection so the client can map rows
to entities.

Rules: CTE columns = hop columns of joins whose parent is the root
(+ root PKs in batch), deduplicated, in first-use order; they must be
real columns of the root FG (:1130-1135) or no template. Aliases `b`
and `j<joinIndex>`; parent alias is `b` when the parent is the subtree
root, else `j<parentIndex>`. Projections = each projection join's
selected features (`getTrainingDatasetFeatures`), aliased
`<prefix><name>`; an empty projection → no template. Multi-child
subtrees emit several `JOIN`s off `b`; chained depth keys `j3` off a
`j2` column. Join conditions are rendered in persisted order, `AND`-ed.

Gates (`buildRonsqlSnowflakeTemplates` :966): batch root key must be a
single column; every nested join must be INNER or LEFT (uniform), in
the same feature store, with ≥ 1 condition, and its right-hand columns
must equal the child's full PK set; broken parent chains → null.

MySQL twin: the Calcite nested LEFT/INNER join statement over
`` `db`.`table` `` with `fg<i>` aliases (not emitted by the RonSQL
port; the framework hand-builds an equivalent `SELECT … FROM root
[LEFT] JOIN child ON … WHERE rootpk = ?`). Client: nested-subtree rows
are overlaid onto the vector by prefixed name; batch rows are matched
on the projected root PK.

Engine notes: projection-only main SELECT over a CTE_SCAN root with
real-table lookup children (E.3 / I.8-I.12 shapes). Hopsworks reports
live verification on 26.04 for multi-child, chained depth, and batch.
Verify here in E3 with EXPLAIN pins (`[ROOT] CTE_SCAN b`, `[INNER]
PK_LOOKUP … AS j2`). Data: NULL hop values (nullable FK) drop the
row on both engines under INNER; hop values with no dimension row
(dangling FK) test misses.

### S8 — Snowflake all-LEFT, per-chain templates

One S7-shaped statement per nested join: traversed joins = the path
from the subtree root to that node, projection = that node's features
only (test :589-599). The engine executes inner lookups; a miss drops
the row of that chain only, so the client overlay leaves that node's
features missing while other chains still contribute. Difference from
MySQL LEFT JOIN: missing vs NULL — the L2 oracle treats "missing" as
NULL when comparing vectors and records the difference as a known
Hopsworks semantic (ledger tag `hopsworks`).

### S8b — Snowflake LEFT, single statement (future)

`FROM `b` LEFT JOIN `<child>` AS `j2` ON … LEFT JOIN … `. The RonSQL
grammar accepts `LEFT [OUTER] JOIN`; `INNER JOIN below a LEFT JOIN`
is rejected by the engine, and CTE_SCAN as an outer-join child is
rejected. E7 probes this shape so Hopsworks can switch to one
statement per subtree if it is green.

### S9 — Composite entity keys

Single statements with `` `k1` = ? AND `k2` = ? `` (S1, S2, S4, S6,
S7 root). Batch variants are gated off in RonSQL (MySQL uses a row
constructor). Data: `balances_1 (account_id, currency)` and
`balance_hist_1 (account_id, currency, event_time)`.

### S10 — String entity keys

All shapes with a `VARCHAR(100)` entity key (`customers_str_1`):
quoting, `''` doubling, IN lists of strings, GROUP BY on a string
key, collation-sensitive equality.

## 2. Gate matrix (Hopsworks-side, must be reproduced by the port)

| Gate | Where | Effect |
|---|---|---|
| Batch + composite entity key | `applyRonsqlAggregate` :489, `buildRonsqlSnowflakeTemplates` :976 | no RonSQL template |
| Collect batch | `buildDTO` :686 | no RonSQL template |
| Filter tree contains OR | `conjunctiveFilterConditions` :711 | exception for collect/aggregate FGs |
| Filter leaf not renderable (op, feature-vs-feature, unknown feature, unsafe literal, boolean) | `renderRonsqlCondition` :723 | exception `COLLECT_UNSUPPORTED_ONLINE_FILTER` |
| Snowflake + filters on collect/aggregate FGs | `createSnowflakePreparedStatementDTOS` :818 | exception |
| Nested join type not INNER/LEFT, or mixed | :996-1033 | no templates |
| Hop does not cover child's full PK | :1004-1013 | no templates |
| Cross-featurestore FG | :1010 | no templates |
| Conditionless join | :1013 | no templates |
| Hop column not a root FG column | `buildSnowflakeStatement` :1130 | no template |
| Empty projection for a chain | :1125 | that chain skipped |
| Definition time: fn/type matrix, integer GREATEST operands, TIMESTAMP event_time last in PK, window ≤ 100y and ≤ TTL, unique output names, spec ≤ 2000 chars | `QueryController.java:323-530` | feature view creation fails |

## 3. Engine envelope beyond Hopsworks (input to E7)

Constructs RonSQL supports (or documents) that Hopsworks does not emit
yet, worth generating over the same schema:

- `LEFT [OUTER] JOIN` from a CTE root (S8b); anti-join `LEFT JOIN … WHERE
  x IS NULL`.
- Several CTEs, one per feature group, joined in the main query on the
  entity key ("CTE per feature group"), with and without a main-query
  aggregate; scalar CTEs (no GROUP BY) and the comma cross-join.
- `HAVING`, `ORDER BY` on GROUP BY columns / aggregate aliases + `LIMIT`
  on aggregate queries; ORDER BY/LIMIT inside CTE bodies.
- `IN (list)` sizes 1 / 10 / 100 / 1000; `LIKE`; `IS [NOT] NULL`;
  `DATE_SUB(…, INTERVAL n DAY)` bounds; `EXTRACT`.
- `AVG`, `COUNT(col)` vs `COUNT(*)`, DECIMAL / DOUBLE aggregates
  (formatting), string MIN/MAX.
- Index hints on the root scan; `FRAGS_PER_WORKER = n`.
- Known-rejected probes (expect `CLEAN-REJECT`): partial-key CTE
  lookup, CTE_SCAN as outer-join child, INNER below LEFT, ORDER BY in
  subqueries, GROUP BY expression, post-aggregation expressions,
  `DISTINCT`, `BETWEEN`, `UNION`, implicit table aliases (`orders o`).

## 4. Data requirements summary (input to P2)

- History FGs with PK `(entity, event_time TIMESTAMP)` and hash+ordered
  PK (default) — S1-S6; one hash-only twin for the plan comparison.
- Entity FGs with INT/BIGINT keys and one with VARCHAR keys (S10).
- Dimension chain `customers → regions → countries` with dangling and
  NULL hops (S7/S8), and a hop from a history FG (`transactions →
  merchants`).
- Composite entity key FGs (S9).
- Integer pairs for GREATEST/LEAST (S5), DECIMAL / DOUBLE / FLOAT
  columns (formatting), tinyint booleans (refused in filters),
  TIMESTAMP(0) and TIMESTAMP(3) event times, mixed-case strings (S4).
- Rows per entity: mixture of 0, 1, exactly N, and ≫ N (collect and
  window semantics), plus rows exactly at window bounds.
