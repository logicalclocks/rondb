# Findings — spec-level random generator (E6)

Format as `smoke.md`; `#` continues the shared numbering (F14 …).
Family: spec (engine finding on a Hopsworks-emitted shape) / hopsworks
(semantic drift between the RonSQL template path and the MySQL path) /
framework (generator, binder or canonicalization bug).  One row per
shape signature (`CASE … <signature>`), with the hit count; the minimal
repro comes from `--shrink`.

| # | Family | Signature | Symptom | Repro (minimal) | Disposition |
|---|--------|-----------|---------|-----------------|-------------|
| F14 | spec (engine) | `serving\|root=customers_str\|…\|snow=INNER:1` / `snow=LEFT:1` / `snow=LEFT:2` (6 of 200 cases, seed 1) | **WRONG RESULT**: a snowflake template whose CTE body is keyed by the VARCHAR entity key returns **no rows** through `CTE_SCAN` + `PK_LOOKUP`, while MySQL returns the region/country row; the CTE body alone (`SELECT region_id, COUNT(*) AS hw_cnt FROM customers_str_1 WHERE customer_key = 'cust-00000044' GROUP BY region_id`) returns `45, 1` on RonSQL, and the same statement over the integer-keyed `customers_1` returns the row. EXPLAIN is identical for both keys (`Body root: INDEX_SCAN using PRIMARY`, `[ROOT] CTE_SCAN`, `[INNER] PK_LOOKUP regions_1`). Both the lower-case and the stored upper-case key form fail. | `WITH b AS (SELECT region_id, COUNT(*) AS hw_cnt FROM customers_str_1 WHERE customer_key = 'cust-00000044' GROUP BY region_id) SELECT j2.country_id AS r_country_id FROM b JOIN regions_1 AS j2 ON j2.region_id = b.region_id;` (sf 0.01, database test; MySQL: `5`) | OPEN — engine tree: the CTE materialisation / CTE_SCAN join path over an aggregate body bound on a VARCHAR primary key loses the group (the standalone aggregate path over the same bound works, S10). Hopsworks feature views with a string entity key and a snowflake join are affected. Expectation table `F14`; the fuzzer reports `KNOWN-WRONG` for the structural pattern (string root key, snowflake template, zero RonSQL rows vs rows on MySQL). |
| F3 (rule) | framework | `agg:transactions` with `AVG(amount_dec)` (1 of 1000 cases, seed 2, `--vectors --direct-collect --shrink`; shrunk in 11 steps to one batch aggregate with one filter) | MySQL `299.716667`, RonSQL `299.7167` for a mean of three DECIMAL(12,2) values: RonSQL prints AVG with four decimals (F3), which the exact DECIMAL comparison rejected | `SELECT customer_id, AVG(amount_dec) AS amount_dec_avg FROM transactions_1 WHERE customer_id IN (965) GROUP BY customer_id;` | FRAMEWORK, fixed: `canon.CellsEqualCol` / `Compare` accept AVG outputs (`<source>_avg`) within 1e-4 (the F3 allowance the E1 ledger prescribed); the engine-side F3 row stays open. |
| F8 (rule) | framework | `agg:transactions` with `MIN(category)` / `MAX(category)` (4 of 200 cases) | MySQL `Grocery`, RonSQL `grocery` (collation-equal values, unspecified representative) | — | FRAMEWORK, fixed: the generator samples only `COUNT` over `category` / `device` (the columns whose domain holds collation-equal variants), per the F8 rule of E1. |

Known outcomes that are not findings: `CLEAN-REJECT` on every collect
case (F0, the Hopsworks CTE form), `KNOWN-ERROR` on MIN/MAX over the
event time (F9), `GATED` / `NO-TEMPLATE` for the gated shapes.
