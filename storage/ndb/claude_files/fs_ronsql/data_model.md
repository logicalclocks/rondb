# Data model and generator design (P2, v1 — 2026-09-08)

**Status: design complete, nothing built.** Consumed by E1 (schema +
loaders), E3 (golden MTR tests), E5 (benchmarks), E6/E7 (fuzzers).
Engine gaps found by the framework are handled in a separate tree; this
model is engine-independent and only depends on what Hopsworks creates.

Companion documents: `fs_ronsql_plan.md` (D3), `shape_catalog.md` §4
(data requirements each shape needs).

---

## 1. Principles

1. **Tables are created exactly like Hopsworks creates online tables.**
   The DDL is produced by the Go port of
   `OnlineFeaturegroupController.buildCreateStatement` from a
   feature-group spec (feature name, *offline* type, primary flag,
   optional online-type override, index config). Nothing is hand-written.
   Consequences: `<name>_<version>` table names, `VARCHAR(100)` for
   `string`, `tinyint` for `boolean`, `VARBINARY(100)` for complex
   types, `STORAGE MEMORY` on the event-time column, `ENGINE=ndbcluster
   COMMENT='NDB_TABLE=READ_BACKUP=1'`, no charset clause (server
   default `utf8mb4` / `utf8mb4_0900_ai_ci`), PK hash+ordered by
   default, `USING HASH` only when requested, optional `KEY
   ttl_index(event_time)` and `KEY idx_<cols>(...)`.
2. **Every value is a closed-form function of the row coordinates**
   (entity id `c`, row index `i`), with no random numbers. The same
   formulas are implemented once in Go (`fsq/data`) and rendered into
   SQL for the MTR-scale include, so both loaders produce identical
   bytes; a checksum test pins that (§8).
3. **One reference clock.** `FS_NOW = '2026-06-01 00:00:00'` UTC. All
   event times are `FS_NOW - age(c, i)`. Windowed statements bind
   `FS_NOW - window`, never the wall clock, on both engines; benchmarks
   too. RonSQL is UTC-only, so every MySQL session used by the
   framework runs `SET time_zone = '+00:00'`.
4. **Row-count classes, not distributions.** Each entity's number of
   history rows is a fixed class of `c mod 16`, so test authors can
   pick an entity with 0, 1, exactly N, or ≫ N rows by arithmetic
   (§6) and the batch boundary (> 256 groups) is crossed at every
   scale.
5. **Deterministic floating point.** DOUBLE/FLOAT columns hold
   multiples of 0.25 with magnitude < 2^40, so any summation order
   gives the same exact result on both engines (dyadic rationals, no
   rounding).

---

## 2. Scale factors

`E = 100 000 × sf` customers. Dimension tables are fixed-size except
merchants.

| sf | customers `E` | use | cluster |
|---|---|---|---|
| 0.01 | 1 000 | MTR default (`fs_test`) | any suite config (raise `DataMemory` to 200M in `ronsql_fs/my.cnf`) |
| 0.1 | 10 000 | interactive / fuzz | `ronsql_fs setup --start-and-exit` |
| 1 | 100 000 | benchmark default (`fs_bench`) | `ronsqlcrunch` (DataMemory 4G) |
| 3 | 300 000 | large benchmark | `ronsqlcrunch`, hash twin disabled |

Row counts per table are in §7.

---

## 3. Feature groups

Naming: feature group `x` version 1 → table `` `x_1` ``. Offline types
are what a Hopsworks user declares; online types follow `getOnlineType`
(§1) unless an override is listed.

### 3.1 `customers_1` — entity FG, star root, snowflake root

| feature | offline type | online type | PK | notes |
|---|---|---|---|---|
| `customer_id` | bigint | BIGINT | 1 | `c` in 1..E |
| `region_id` | int | INT | | hop to `regions_1`; NULL / dangling per §5 |
| `tier` | string | VARCHAR(100) | | `bronze silver gold platinum` |
| `is_active` | boolean | TINYINT | | boolean → filters refused by Hopsworks (gate) |
| `age` | int | INT | | GREATEST/LEAST operand |
| `credit_score` | int | INT | | GREATEST/LEAST operand |
| `credit` | decimal(12,2) | DECIMAL(12,2) | | DECIMAL aggregate formatting |
| `signup_ts` | timestamp | TIMESTAMP | | not the event time; unique per customer |
| `tags` | array<string> | VARBINARY(100) | | complex type: never selected by the spec generator, E7 probe only |

Indexes: `idx_region_id` (secondary, so the reverse hop
`regions → customers` is joinable). Event time: none. PK index: default
(hash + ordered).

### 3.2 `customers_str_1` — entity FG with a string key (S10)

Same columns as `customers_1` with the key replaced by
`customer_key string → VARCHAR(100)`. Values: `cust-00000042`
(`CONCAT('cust-', LPAD(c, 8, '0'))`), except every 10th customer uses
the upper-case prefix `CUST-` (collation probe, §5.4). PK default.

### 3.3 `regions_1` — dimension, snowflake hop 1

| feature | offline type | online type | PK |
|---|---|---|---|
| `region_id` | int | INT | 1 |
| `country_id` | int | INT | |
| `region_name` | string | VARCHAR(100) | |
| `population` | bigint | BIGINT | |

200 rows at every scale. `country_id` NULL / dangling per §5.

### 3.4 `countries_1` — dimension, snowflake hop 2

| feature | offline type | online type | PK |
|---|---|---|---|
| `country_id` | int | INT | 1 |
| `country_name` | string | VARCHAR(100) | |
| `continent` | string | VARCHAR(100) | |
| `gdp` | double | DOUBLE | |

40 rows at every scale.

### 3.5 `merchants_1` — dimension reached from a history FG

| feature | offline type | online type | PK |
|---|---|---|---|
| `merchant_id` | int | INT | 1 |
| `mcc` | int | INT | |
| `name` | string | VARCHAR(100) | |

`M = max(100, 10 000 × sf)` rows.

### 3.6 `transactions_1` — history FG (S1–S6)

| feature | offline type | online type | PK | notes |
|---|---|---|---|---|
| `customer_id` | bigint | BIGINT | 1 | entity key |
| `event_time` | timestamp | TIMESTAMP | 2 | event time, last PK column (`STORAGE MEMORY`) |
| `amount` | bigint | BIGINT | | 100..999 |
| `fee` | int | INT | | -10..39, negatives for GREATEST/LEAST |
| `merchant_id` | int | INT | | hop to `merchants_1`; NULL / dangling per §5 |
| `category` | string | VARCHAR(100) | | mixed case, §5.4 |
| `score` | double | DOUBLE | | multiples of 0.25 |
| `amount_dec` | decimal(12,2) | DECIMAL(12,2) | | 0.00..899.99 |
| `flag` | boolean | TINYINT | | |

Event time `event_time`. Indexes: `idx_merchant_id`. PK default
(hash + ordered — required for entity-prefix range reads).

### 3.7 `transactions_hash_1` — hash-only twin of 3.6

Identical columns and rows to `transactions_1` for customers
`c ≤ E` but created with `PRIMARY KEY (...) USING HASH`
(`primaryKeyIndexType = HASH`). Purpose: measure and pin the plan
difference (no ordered PK index → table scan for `customer_id = ?`)
on both engines. Loaded only when `--hash-twin` is given (default on at
sf ≤ 0.1, off above).

### 3.8 `transactions_str_1` — history FG with a string entity key (S10)

Columns of 3.6 with `customer_key string → VARCHAR(100)` as the entity
key. Rows only for customers `c ≤ E / 10` (same `n_tx` class and
formulas as 3.6 with `c` mapped through `customer_key(c)`).

### 3.9 `sessions_1` — second history FG on the same entity, fractional seconds, TTL

| feature | offline type | online type | PK | notes |
|---|---|---|---|---|
| `customer_id` | bigint | BIGINT | 1 | |
| `event_time` | timestamp | **TIMESTAMP(3)** (override) | 2 | fractional seconds |
| `duration` | int | INT | | seconds |
| `pages` | int | INT | | |
| `device` | string | VARCHAR(100) | | `ios android web WEB` |
| `bytes` | bigint | BIGINT | | |

Event time `event_time`. TTL enabled with `ttl = 3 153 600 000` s
(100 years, so nothing is ever purged) → the DDL gains `KEY
ttl_index(event_time)` and the comment `NDB_TABLE=READ_BACKUP=1,TTL=3153600000@event_time`.
Purpose: exercise the TTL DDL path and an ordered index on the
event-time column alone (a candidate for the window bound).

### 3.10 `balances_1` — composite entity key (S9)

| feature | offline type | online type | PK |
|---|---|---|---|
| `account_id` | bigint | BIGINT | 1 |
| `currency` | string | VARCHAR(100) | 2 |
| `balance` | decimal(18,2) | DECIMAL(18,2) | |
| `updated_ts` | timestamp | TIMESTAMP | |
| `overdraft` | int | INT | |

`A = E / 2` accounts; account `a` has `1 + (a mod 3)` currencies from
`USD EUR SEK` in that order. No event time.

### 3.11 `balance_hist_1` — history FG with a composite entity key (S9 + S2)

| feature | offline type | online type | PK |
|---|---|---|---|
| `account_id` | bigint | BIGINT | 1 |
| `currency` | string | VARCHAR(100) | 2 |
| `event_time` | timestamp | TIMESTAMP | 3 |
| `delta` | bigint | BIGINT | |
| `channel` | string | VARCHAR(100) | |

Event time `event_time`. Rows per (account, currency) by class of
`a mod 5` (§4).

### 3.12 Snowflake fixture (mirrors the Hopsworks unit-test fixture)

`profiles_1 (user_id bigint PK, region_id int, tier string)` with
`E` rows (`user_id = c`, `region_id` as in `customers_1`) lets the
golden tests reproduce the Hopsworks fixture strings verbatim
(`profiles_1` / `regions_1` / `countries_1`, aliases `b`, `j2`, `j3`,
prefixes `r_` and `c_`). It is a projection of `customers_1`.

---

## 4. Row-count classes

`n_tx(c)`, the number of `transactions_1` rows for customer `c`:

| `c mod 16` | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `n_tx` | 0 | 1 | 2 | 3 | 5 | 5 | 8 | 8 | 12 | 12 | 20 | 20 | 30 | 50 | 100 | 300 |

Sum over a class cycle = 576 → mean 36 rows per customer. The classes
cover: no rows (0), single row (1), exactly N for N ∈ {1, 5, 50}, and
≫ N (100, 300). 300 > 256 exercises multi-batch per entity.

`n_sess(c)` for `sessions_1`, by `c mod 8`:

| `c mod 8` | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|---|
| `n_sess` | 0 | 1 | 2 | 4 | 6 | 8 | 12 | 20 |

Mean 6.625. `n_bal(a)` for `balance_hist_1`, per (account, currency),
by `a mod 5`: `0 1 2 5 10` (mean 3.6).

Row index `i` runs 1..n; `i = 1` is always the newest row.

---

## 5. Value formulas

All arithmetic is integer unless stated; `mod` is non-negative.
`ELT(k, …)` is 1-based as in MySQL.

### 5.1 Time

`age(c, i) = i × spacing(c) + offset(c)`, `event_time = FS_NOW - age`.

- `spacing(c)` by `(c div 16) mod 4`: `0 → 1 hour`, `1 → 6 hours`,
  `2 → 1 day`, `3 → 7 days`.
- `offset(c)` by `c mod 3`: `0 → 0`, `1 → 17 minutes`, `2 → 41 minutes`.
- Strictly increasing in `i` → unique `(customer_id, event_time)`.
- Customers with `offset = 0` and `spacing = 1 day` have rows at
  **exactly** 1, 7, 30, 90 days before `FS_NOW` (when `n_tx ≥ i`), so
  `event_time >= FS_NOW - 7 DAY` tests `>=` inclusivity. Example:
  `c = 111` (300 daily rows, offset 0).
- Oldest row: 300 × 7 days ≈ 5.75 years before `FS_NOW` (2020-09),
  inside the TIMESTAMP range.

`sessions_1`: `age = i × 90 minutes + (c mod 60) seconds + (i mod 1000)
milliseconds` (fractional part exercises TIMESTAMP(3)).
`balance_hist_1`: `age = i × 12 hours + (a mod 7) hours`.
`customers_1.signup_ts = FS_NOW - c × 10 minutes` (unique; E = 300 000
→ 5.7 years). `balances_1.updated_ts = FS_NOW - a × 1 minute`.

### 5.2 `transactions_1(c, i)`

| column | formula |
|---|---|
| `amount` | `100 + (c × 31 + i × 17) mod 900` |
| `fee` | `((c + i) mod 50) - 10` |
| `merchant_id` | NULL if `(c + i) mod 11 = 0`; else `M + 1 + (i mod 3)` (dangling) if `(c + i) mod 23 = 0`; else `((c × 7 + i) mod M) + 1` |
| `category` | `ELT(((c + i) mod 6) + 1, 'grocery', 'Grocery', 'fuel', 'travel', 'Travel', 'online')` |
| `score` | `(c mod 97) + i × 0.25` |
| `amount_dec` | `((c × 31 + i × 17) mod 90000) / 100` |
| `flag` | `(c + i) mod 2` |

Same formulas for `transactions_hash_1` and `transactions_str_1`.

### 5.3 Other tables

`customers_1(c)`:
`region_id` = NULL if `c mod 13 = 0`; `201 + (c mod 5)` (dangling) if
`c mod 29 = 0`; else `(c mod 200) + 1`. `tier = ELT((c mod 4) + 1,
'bronze', 'silver', 'gold', 'platinum')`. `is_active = (c mod 5 ≠ 0)`.
`age = 18 + (c mod 70)`. `credit_score = 300 + (c × 37) mod 551`.
`credit = c × 1.25 + 0.50` (DECIMAL exact). `tags = UNHEX('0102')`
(constant). `profiles_1` = `(customer_id, region_id, tier)`.

`regions_1(r)`, r = 1..200: `country_id` = NULL if `r mod 17 = 0`;
`41 + (r mod 3)` (dangling) if `r mod 19 = 0`; else `((r - 1) mod 40)
+ 1`. `region_name = CONCAT('Region ', r)`. `population = r × 12345`.

`countries_1(k)`, k = 1..40: `country_name = CONCAT('Country ', k)`,
`continent = ELT((k mod 5) + 1, 'Europe', 'Asia', 'Africa', 'America',
'Oceania')`, `gdp = k × 1000.25`.

`merchants_1(m)`, m = 1..M: `mcc = 5000 + (m mod 100)`, `name =
CONCAT('Merchant ', m)`.

`sessions_1(c, i)`: `duration = 30 + (c × 13 + i × 7) mod 3600`,
`pages = 1 + (c + i) mod 40`, `device = ELT(((c + i) mod 4) + 1, 'ios',
'android', 'web', 'WEB')`, `bytes = (c × 1009 + i × 4093) mod 10 000 000`.

`balances_1(a, cur)`, cur index j = 0..(a mod 3): `currency = ELT(j + 1,
'USD', 'EUR', 'SEK')`, `balance = a × 3.75 + j`, `overdraft = (a mod
4) × 500`. `balance_hist_1(a, cur, i)`: `delta = ((a + i × 7) mod 200)
- 100`, `channel = ELT(((a + i) mod 3) + 1, 'card', 'wire', 'atm')`.

### 5.4 Strings and collation

Alphabet: ASCII letters, digits, space, `-`, `#`. No quotes, no
backslashes, no control characters (Hopsworks refuses those in
literals anyway). Mixed-case pairs (`grocery`/`Grocery`,
`travel`/`Travel`, `web`/`WEB`, `cust-`/`CUST-`) are deliberate:
MySQL compares them equal under `utf8mb4_0900_ai_ci`; whether RonSQL's
`=`, `LIKE`, `GROUP BY`, `MIN/MAX` agree is recorded in E3 (a
divergence is a ledger entry, and possibly a Hopsworks gate on string
filters). No non-ASCII data in v1; a UTF-8 probe FG can be added later.

---

## 6. Key classes (how tests pick entities without computing results)

| Property | Rule | Example (sf = 0.01) |
|---|---|---|
| no transactions | `c mod 16 = 0` | 16, 32, 800 |
| exactly 1 / 5 / 50 / 300 transactions | `c mod 16 = 1 / 4 or 5 / 13 / 15` | 17 / 20, 21 / 29 / 31 |
| rows at exact day bounds | `c mod 3 = 0` and `(c div 16) mod 4 = 2` | 111 (300 rows), 108 (30 rows) |
| hourly rows (window 1h has ≤ 1 row) | `(c div 16) mod 4 = 0` | 1..15 |
| NULL region hop | `c mod 13 = 0` | 13, 26 |
| dangling region hop | `c mod 29 = 0` | 29, 58 |
| region with NULL country | `region_id mod 17 = 0` → customers `c mod 200 = 16, 33, …` | c = 16 → region 17 |
| region with dangling country | `region_id mod 19 = 0` | c = 18 → region 19 |
| no sessions | `c mod 8 = 0` | 8 |
| upper-case string key | `c mod 10 = 0` | `CUST-00000010` |
| account with 3 currencies | `a mod 3 = 2` | 2, 5 |
| account with no history | `a mod 5 = 0` | 5, 10 |

The Go package exposes `fsdata.Pick(class, n)` and
`fsdata.Rows(table, c)` (regenerates the rows of one entity in memory)
so `.fs_verify` and the fuzzers can choose keys by class and can
sanity-check simple expectations (row counts, MIN/MAX of `event_time`)
without MySQL.

---

## 7. Row counts and memory

Rows per table as a function of `E` (customers):

| table | rows | sf 0.01 | sf 1 | sf 3 |
|---|---|---|---|---|
| `customers_1` | E | 1 000 | 100 000 | 300 000 |
| `customers_str_1` | E | 1 000 | 100 000 | 300 000 |
| `profiles_1` | E | 1 000 | 100 000 | 300 000 |
| `regions_1` | 200 | 200 | 200 | 200 |
| `countries_1` | 40 | 40 | 40 | 40 |
| `merchants_1` | max(100, 10 000 sf) | 100 | 10 000 | 30 000 |
| `transactions_1` | 36 E | 36 000 | 3 600 000 | 10 800 000 |
| `transactions_hash_1` | 36 E (optional) | 36 000 | (off) | (off) |
| `transactions_str_1` | 3.6 E | 3 600 | 360 000 | 1 080 000 |
| `sessions_1` | 6.625 E | 6 625 | 662 500 | 1 987 500 |
| `balances_1` | E (A = E/2, mean 2 currencies) | 1 000 | 100 000 | 300 000 |
| `balance_hist_1` | 3.6 E | 3 600 | 360 000 | 1 080 000 |
| **total** | ≈ 54 E (+36 E twin) | ≈ 54 k (+36 k) | ≈ 5.4 M | ≈ 16.1 M |

Memory estimate (per data node, all fragments incl. replicas on a
2-node group): `transactions_1` ≈ 50 B data + ≈ 90 B row/hash/ordered-
index overhead + ≈ 25 B secondary index ≈ 165 B/row → 0.6 GB at sf 1;
whole set ≈ 0.85 GB at sf 1, ≈ 2.6 GB at sf 3 (fits `ronsqlcrunch`
DataMemory 4G; sf 3 leaves no room for the hash twin). MTR sf 0.01 ≈
15 MB: set `DataMemory=200M` in `ronsql_fs/my.cnf` (default is 30M).

---

## 8. Loading

### 8.1 Single source of truth

`tools/rondb-cli/internal/fsq/data/`:
- `schema.go` — the feature-group specs of §3 as Go values (also
  dumped as `fs_schema.json` for humans and P3).
- `formulas.go` — §4/§5 as pure functions `NTx(c)`, `TxRow(c, i)`, …
  returning typed Go values (`int64`, `sql.NullInt64`, `string`,
  `time.Time` UTC, `decimal string`).
- `sqlgen.go` — renders the *same* formulas as MySQL expressions
  (`MOD`, `ELT`, `IF`, `TIMESTAMPADD`, `LPAD`) for the MTR include.
- The DDL comes from `fsq/ddl` (port of `buildCreateStatement`).

### 8.2 MTR scale — generated SQL include (`.fs_emit_mtr <dir> --sf 0.01`)

Emits `include/fs_schema.inc` (DDL via the port, one `CREATE TABLE`
per FG, `CREATE DATABASE fs_test`), `include/fs_data.inc`, and
`include/fs_drop.inc`, following `ronsql_cte/include/cte_data.inc`:

- `SET time_zone = '+00:00'` first (TIMESTAMP literals are stored in
  UTC).
- MEMORY helper tables `_d` (0..9) and `_seq` (1..100 000, five-digit
  cross join) dropped at the end.
- Dimension and entity tables: one `INSERT … SELECT … FROM _seq WHERE n
  <= E`.
- History tables: `INSERT … SELECT … FROM _seq AS c JOIN _seq AS i ON i.n
  <= n_tx(c.n) WHERE c.n BETWEEN lo AND hi`, chunked by customer
  ranges so no statement inserts more than 4 000 rows (MTR's
  `MaxNoOfConcurrentOperations=10000`); at sf 0.01 that is 10 chunks
  for `transactions_1` (100 customers ≈ 3 600 rows each).
- Hash twin: `INSERT INTO transactions_hash_1 SELECT * FROM
  transactions_1` in the same chunks.
- The include ends with a **checksum block** (§8.4) so a wrong
  regeneration fails at record time.

The generated files are checked in; MTR never needs Go at run time.

### 8.3 Bench scale — Go loader (`.fs_load <sf> [threads] [batch] [--db fs_bench] [--hash-twin]`)

- DDL via `fsq/ddl` over the MySQL client (`CREATE DATABASE IF NOT
  EXISTS`, `CREATE TABLE IF NOT EXISTS`).
- Rows via **MySQL multi-row `INSERT`** (`batch` rows per statement,
  default 500), one connection per thread with `SET time_zone =
  '+00:00'`, work split by entity ranges exactly like
  `loadTPCHTable`. MySQL is preferred over the RDRS batch write for
  exact DECIMAL/TIMESTAMP typing and because it is what MTR uses; the
  RDRS `pk-write` sink (which does support `Timestamp2`) can be added
  behind `--sink rdrs` if load throughput matters.
- Progress and rows/s reporting reuse the TPC-H loader helpers.
- `.fs_drop [--db]` drops the database.

### 8.4 Equivalence and invariants

Both loaders must produce identical tables. Pinned by a checksum
statement per table run after each load (MTR include and `.fs_load`):

```
SELECT COUNT(*), SUM(customer_id), SUM(amount), SUM(fee),
       COUNT(merchant_id), SUM(score), SUM(amount_dec),
       MIN(event_time), MAX(event_time)
FROM transactions_1;
```

Expected values are computed by `fsq/data` in Go (`Checksum(table,
sf)`); the MTR include echoes them next to the query so the `.result`
carries both. Global invariants (documented for authors):

- `COUNT(*) FROM transactions_1 = 36 E` exactly when `E mod 16 = 0`.
- Every customer with `n_tx ≥ 1` has `MAX(event_time) = FS_NOW -
  spacing(c) - offset(c)`.
- `SUM(score)` is exact on both engines (multiples of 0.25).
- `COUNT(*) GROUP BY customer_id` yields `15 E / 16` groups (> 256 at
  every scale).

---

## 9. Feature-group spec format (input to P3)

`fs_schema.json` (also the Go type `fsq/spec.FeatureGroup`):

```json
{
  "name": "transactions", "version": 1, "eventTime": "event_time",
  "ttl": null,
  "online": {"primaryKeyIndexType": null, "secondaryIndexes": [["merchant_id"]]},
  "features": [
    {"name": "customer_id", "type": "bigint", "primary": true},
    {"name": "event_time", "type": "timestamp", "primary": true},
    {"name": "amount", "type": "bigint"},
    {"name": "score", "type": "double"},
    {"name": "category", "type": "string"},
    {"name": "tags", "type": "array<string>", "complex": true}
  ]
}
```

`type` is the offline type (drives both the DDL port and the
definition-time gates: integer/numeric/complex classification), and
`onlineType` overrides it (`sessions_1.event_time: "timestamp(3)"`).
`complex: true` is a framework-only hint that keeps the spec generator
from selecting the feature.

---

## 10. Items to confirm in E1 (recorded in `phase_e1.md`)

1. MySQL accepts the exact Hopsworks DDL text on this server
   (`STORAGE MEMORY` on a TIMESTAMP column, `COMMENT='NDB_TABLE=…'`,
   `TTL=…@event_time` with a 100-year TTL, `USING HASH`).
2. `TIMESTAMP(3)` override round-trips through both engines with the
   same text formatting.
3. Load time of the MTR include at sf 0.01 (target < 30 s) and of
   `.fs_load 1 8 500` on the crunch cluster (target < 5 min).
4. The checksum block matches between the MTR include and the Go
   loader.
5. The mixed-case collation behaviour (§5.4) on RonSQL vs MySQL — a
   data-model fact, not a shape fact, so it is settled here first.
