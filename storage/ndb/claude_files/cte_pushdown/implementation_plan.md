# CTE Pushdown Implementation Plan

This document breaks the design plan into concrete implementation tasks with file-level detail, test cases, and dependencies.

---

## Step 1: RonSQL CTE Parsing

**Goal:** Parse WITH ... AS syntax, build AST, validate structure. No execution.

### 1.1 Add WITH keyword
- **File:** `storage/ndb/src/ronsql/Keywords.hpp`
- Add `WITH` to keyword table
- Add `AS` if not already present (verify)

### 1.2 AST data structures
- **File:** `storage/ndb/src/ronsql/RonSQLCommon.hpp`
- Add `CteDefinition` struct:
  ```cpp
  struct CteDefinition {
    const char* name;
    SelectStatement* stmt;
    CteDefinition* next;
  };
  ```
- Add `CteDefinition* cte_list` field to `SelectStatement` (init to nullptr)

### 1.3 Parser grammar
- **File:** `storage/ndb/src/ronsql/RonSQLParser.y`
- New rules:
  ```
  statement: cte_list selectstatement SEMICOLON | selectstatement SEMICOLON ;
  cte_list: WITH cte_def | cte_list COMMA cte_def ;
  cte_def: IDENTIFIER AS T_LEFT selectstatement T_RIGHT ;
  ```
- Allocate `CteDefinition` nodes from the parser arena
- Store linked list in `ast_root.cte_list`

### 1.4 Lexer
- **File:** `storage/ndb/src/ronsql/RonSQLLexer.l`
- Verify `WITH` token is handled (should be automatic via Keywords.hpp)

### Tests — Step 1

**New test file:** `mysql-test/suite/ronsql/t/ronsql_cte_parse.test`

```sql
-- T1.1: Basic CTE parse (no execution, just EXPLAIN or parse check)
RONSQL EXPLAIN
WITH purchase_agg AS (
  SELECT user_id, COUNT(*) AS cnt FROM purchases GROUP BY user_id
)
SELECT f.user_id, f.age, p.cnt
FROM user_profile f
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
WHERE f.user_id = 1;

-- T1.2: Multiple CTEs
RONSQL EXPLAIN
WITH
  agg1 AS (SELECT user_id, COUNT(*) AS c FROM purchases GROUP BY user_id),
  agg2 AS (SELECT user_id, SUM(duration) AS d FROM page_views GROUP BY user_id)
SELECT f.*, a1.c, a2.d
FROM user_profile f
LEFT JOIN agg1 a1 ON a1.user_id = f.user_id
LEFT JOIN agg2 a2 ON a2.user_id = f.user_id
WHERE f.user_id = 1;

-- T1.3: CTE with WHERE filter
RONSQL EXPLAIN
WITH recent_purchases AS (
  SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
  FROM purchases
  WHERE ts > '2026-01-01'
  GROUP BY user_id
)
SELECT f.user_id, r.cnt, r.total
FROM user_profile f
LEFT JOIN recent_purchases r ON r.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3);

-- T1.4: Error — CTE without GROUP BY (should fail validation)
-- T1.5: Error — CTE name conflicts with real table
-- T1.6: Error — CTE referenced but not defined
-- T1.7: CTE with aggregate functions: COUNT, SUM, AVG, MIN, MAX
```

---

## Step 2: RonSQL CTE Planning

**Goal:** Resolve CTE table references, add CTE_LOOKUP to join plan, compile CTE aggregation programs.

### 2.1 CTE analysis
- **File:** `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- New function `analyze_ctes()` called from `load()`:
  - For each CTE in `m_ast.cte_list`:
    - Parse the CTE's `SelectStatement` (call `load_single_table()` or `load_join()` recursively)
    - Validate GROUP BY exists
    - Extract virtual schema: GROUP BY columns = PK, aggregate outputs = value columns
    - If CTE contains JOINs, plan as full join tree via `QueryPlanner::plan()`
    - Store analyzed CTE info in new `m_cte_infos[]` array

### 2.2 Table reference resolution
- **File:** `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- In `load_join()`, before `dict->getTable()`:
  - Check `m_ast.cte_list` for matching table name
  - If found, mark join operation with CTE reference instead of NDB table
  - Populate column mapping from CTE's virtual schema

### 2.3 JoinOp extension
- **File:** `storage/ndb/src/ronsql/QueryPlanner.hpp`
- Add `CTE_LOOKUP` to `JoinOp::Type` enum
- Add `CteDefinition* cte_ref` field to `JoinOp`

### 2.4 Compound query tree compilation
- **File:** `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- In `compile()`:
  - For each CTE, compile its aggregation program via `AggregationAPICompiler`
  - Generate main query tree with CTE_LOOKUP nodes referencing CTE ids
  - Package both into a single NdbQueryDef

### Tests — Step 2

```sql
-- T2.1: EXPLAIN shows CTE materialization plan
RONSQL EXPLAIN
WITH purchase_agg AS (
  SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
  FROM purchases GROUP BY user_id
)
SELECT f.user_id, f.age, p.cnt, p.total
FROM user_profile f
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
WHERE f.user_id = 1;
-- Verify: plan shows CTE scan + main query with CTE_LOOKUP

-- T2.2: EXPLAIN with CTE containing internal join
RONSQL EXPLAIN
WITH enriched_purchases AS (
  SELECT t.user_id, COUNT(*) AS cnt, SUM(t.amount) AS total
  FROM transactions t
  JOIN merchants m ON m.merchant_id = t.merchant_id
  WHERE m.risk_score > 0.5
  GROUP BY t.user_id
)
SELECT f.user_id, e.cnt, e.total
FROM user_profile f
LEFT JOIN enriched_purchases e ON e.user_id = f.user_id
WHERE f.user_id = 1;

-- T2.3: EXPLAIN with multiple CTEs + multiple PK-PK feature tables
RONSQL EXPLAIN
WITH
  purchase_agg AS (
    SELECT user_id, COUNT(*) AS cnt FROM purchases GROUP BY user_id
  ),
  view_agg AS (
    SELECT user_id, COUNT(*) AS views FROM page_views GROUP BY user_id
  ),
  click_agg AS (
    SELECT user_id, SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS convs
    FROM clicks GROUP BY user_id
  )
SELECT f.*, e.bert_enc_0, h.churn_score,
       p.cnt, v.views, c.convs
FROM user_profile f
JOIN user_embeddings e ON e.user_id = f.user_id
JOIN user_history h ON h.user_id = f.user_id
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
LEFT JOIN view_agg v ON v.user_id = f.user_id
LEFT JOIN click_agg c ON c.user_id = f.user_id
WHERE f.user_id IN (1, 2);
```

---

## Step 3: NDB API — Compound Query Tree

**Goal:** Serialize CTE sub-trees and CTE_LOOKUP nodes in the QueryTree signal.

### 3.1 QueryTree node types
- **File:** `storage/ndb/include/kernel/signaldata/QueryTree.hpp`
- Add `QN_CTE_SUBTREE = 0x6` and `QN_CTE_LOOKUP = 0x7` to `OpType` enum
- Define `QN_CteSubtreeNode` struct (cteId, numNodes, followed by standard nodes)
- Define `QN_CteLookupNode` struct (cteId, key column mapping, result column count)
- Add `NI_CTE_REF = 0x8000` DABit

### 3.2 NdbQueryBuilder extension
- **File:** `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`
- New `NdbQueryCteScanOperationDef` class for CTE sub-tree root
- New `NdbQueryCteLookupOperationDef` class for CTE lookup in main tree
- Builder methods: `scanCte()`, `lookupCte()`

### 3.3 Serialization
- **File:** `storage/ndb/src/ndbapi/NdbQueryOperation.cpp`
- In `doSend()`: serialize QN_CTE_SUBTREE nodes first, then main tree nodes
- Each QN_CTE_SUBTREE wraps standard QN_SCAN_FRAG / QN_LOOKUP nodes

### Tests — Step 3

Unit tests in NDB API test framework:
- T3.1: Build and serialize a query tree with one CTE + one main lookup
- T3.2: Build and serialize with multiple CTEs
- T3.3: Build CTE sub-tree that is itself a join (scan + lookup)
- T3.4: Verify serialized format matches expected byte layout

---

## Step 4: DBSPJ — Compound Tree Build

**Goal:** DBSPJ parses compound tree, creates CteContext + embedded Request per CTE.

### 4.1 CteContext struct
- **File:** `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`
- Add `CteContext` struct (cteId, state, requestPtrI, hashTableHandles, numPartitions)
- Add CTE fields to `Request` struct (m_cteContexts, m_numCtes, m_ctesReady, m_isCte, m_parentRequestPtrI)

### 4.2 Build logic
- **File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`
- In `build()`: detect QN_CTE_SUBTREE, allocate CteContext + child Request
- Parse embedded nodes into child Request's TreeNodes using existing build logic
- Set `childRequest->m_isCte = true`
- For QN_CTE_LOOKUP: create TreeNode with `g_CteOpInfo`, link to CteContext via cteId

### 4.3 CteOpInfo handlers
- **File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`
- `cte_build()`: parse QN_CTE_LOOKUP, store cteId
- `cte_prepare()`: no-op (CTE scans started separately)
- `cte_execNODE()`: placeholder (will be implemented in Step 6)
- `cte_cleanup()`: release hash table handles

### Tests — Step 4

- T4.1: Send compound query tree to DBSPJ, verify build completes without crash
- T4.2: Verify CteContext created with correct cteId
- T4.3: Verify child Request has correct number of TreeNodes
- T4.4: Verify main tree CTE_LOOKUP nodes linked to correct CteContexts
- (Debug logging / ndbinfo verification)

---

## Step 5: DBSPJ — Parallel Execution

**Goal:** Start CTE scans and main query simultaneously.

### 5.1 CTE materialization trigger
- **File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`
- In `start()` or `exec()`: iterate m_cteContexts, for each CTE:
  - Send JOIN_AGG_SETUP_REQ to LDM threads via the CTE's child Request
  - Set `cteContext.m_state = MATERIALIZING`

### 5.2 Main query start
- Same `start()` function: also begin the main query root scan/lookup
- PK lookups to real NDB tables proceed immediately

### 5.3 CTE completion handling
- Handle JOIN_AGG_COMPLETE_CONF for CTE Requests:
  - Store hash table handles in CteContext
  - Set `m_state = READY`
  - Flush pending CTE_LOOKUP queue (implemented in Step 6)

### 5.4 Result routing for CTE Requests
- In aggregate result handlers (TRANSID_AI path):
  - Check `request->m_isCte`
  - If true: route results to hash table instead of sending to API
  - This is the key behavioral change for CTE sub-trees

### Tests — Step 5

- T5.1: Single CTE materializes (hash table built), main query PK lookups succeed
- T5.2: Verify via timing that PK lookups start before CTE scan completes
- T5.3: Multiple CTEs materialize in parallel
- T5.4: CTE scan failure (e.g., table not found) — verify graceful error propagation

---

## Step 6: CTE_LOOKUP Signal + Handler

**Goal:** Hash table lookup signal, DBLQH handler, DBSPJ integration.

### 6.1 Signal definition
- **New file:** `storage/ndb/include/kernel/signaldata/CteLookup.hpp`
- Define `CteLookupReq` (senderRef, senderData, cteHandle, keyLen + key section)
- Define `CteLookupConf` (senderRef, senderData, found + row data section)
- Register GSN_CTE_LOOKUP_REQ and GSN_CTE_LOOKUP_CONF

### 6.2 JoinAggInterpreter lookup interface
- **File:** `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp`
- Add `lookupGroup(key, keyLen)` method
- Add `extractGroupResult(groupData, outBuffer, bufSize, bytesWritten)` method

### 6.3 DBLQH handler
- **File:** `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`
- New signal handler `execCTE_LOOKUP_REQ()`:
  - Resolve hash table handle to JoinAggInterpreter
  - Call `lookupGroup()` with key
  - If found: `extractGroupResult()`, send CteLookupConf with row data
  - If not found: send CteLookupConf with `found = 0`

### 6.4 DBSPJ CTE_LOOKUP execution
- **File:** `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`
- `cte_execNODE()` implementation:
  - If CTE READY: hash key to find target LDM thread, send CTE_LOOKUP_REQ
  - If CTE MATERIALIZING: queue (parent_row, correlation) in pending list
- `execCTE_LOOKUP_CONF()`:
  - If found: create result row, propagate to children/API
  - If not found and LEFT JOIN: propagate NULL row
  - If not found and INNER JOIN: skip row

### 6.5 CTE completion flush
- When CTE transitions to READY, iterate pending lookups queue, send all CTE_LOOKUP_REQs

### Tests — Step 6

**Test file:** `mysql-test/suite/ronsql/t/ronsql_cte_basic.test`

```sql
-- Schema setup
CREATE TABLE user_profile (
  user_id INT PRIMARY KEY,
  age INT, country VARCHAR(32), signup_date DATE,
  account_age_days INT
) ENGINE=NDB;

CREATE TABLE purchases (
  purchase_id INT PRIMARY KEY,
  user_id INT, amount DECIMAL(10,2), ts DATETIME,
  category VARCHAR(32)
) ENGINE=NDB;

CREATE TABLE page_views (
  view_id INT PRIMARY KEY,
  user_id INT, page VARCHAR(64), duration INT, ts DATETIME
) ENGINE=NDB;

-- Load test data: 10 users, 100 purchases, 200 page views

-- T6.1: Single CTE, single user
RONSQL
WITH purchase_agg AS (
  SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
  FROM purchases GROUP BY user_id
)
SELECT f.user_id, f.age, p.cnt, p.total
FROM user_profile f
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
WHERE f.user_id = 1;

-- T6.2: Single CTE, batch of users
RONSQL
WITH purchase_agg AS (
  SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
  FROM purchases GROUP BY user_id
)
SELECT f.user_id, f.age, p.cnt, p.total
FROM user_profile f
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3, 4, 5);

-- T6.3: LEFT JOIN with missing groups (user has no purchases)
RONSQL
WITH purchase_agg AS (
  SELECT user_id, COUNT(*) AS cnt FROM purchases GROUP BY user_id
)
SELECT f.user_id, f.age, p.cnt
FROM user_profile f
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
WHERE f.user_id = 999;
-- Expect: user_id=999, age=..., cnt=NULL

-- T6.4: CTE with WHERE filter on timestamp
RONSQL
WITH recent_purchases AS (
  SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
  FROM purchases
  WHERE ts > '2026-03-01'
  GROUP BY user_id
)
SELECT f.user_id, r.cnt, r.total
FROM user_profile f
LEFT JOIN recent_purchases r ON r.user_id = f.user_id
WHERE f.user_id = 1;

-- T6.5: CTE with all aggregate functions
RONSQL
WITH purchase_stats AS (
  SELECT user_id,
         COUNT(*) AS cnt,
         SUM(amount) AS total,
         AVG(amount) AS avg_amount,
         MIN(amount) AS min_amount,
         MAX(amount) AS max_amount
  FROM purchases GROUP BY user_id
)
SELECT f.user_id, ps.cnt, ps.total, ps.avg_amount,
       ps.min_amount, ps.max_amount
FROM user_profile f
LEFT JOIN purchase_stats ps ON ps.user_id = f.user_id
WHERE f.user_id = 1;

-- T6.6: CTE + PK-PK feature table join (presentation slide 4)
RONSQL
WITH
  purchase_agg AS (
    SELECT user_id, COUNT(*) AS n_purchases, SUM(amount) AS total_spend
    FROM purchases WHERE ts > '2026-01-01' GROUP BY user_id
  ),
  view_agg AS (
    SELECT user_id, COUNT(*) AS n_views, SUM(duration) AS total_time
    FROM page_views WHERE ts > '2026-01-01' GROUP BY user_id
  )
SELECT f.user_id, f.age, f.country,
       e.bert_enc_0, e.item2vec_0,
       h.total_orders, h.churn_score,
       p.n_purchases, p.total_spend,
       v.n_views, v.total_time
FROM user_profile f
JOIN user_embeddings e ON e.user_id = f.user_id
JOIN user_history h ON h.user_id = f.user_id
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
LEFT JOIN view_agg v ON v.user_id = f.user_id
WHERE f.user_id IN (1, 2);
```

---

## Step 7: CTE with Internal Joins

**Goal:** CTE sub-trees that are themselves multi-table join trees.

### 7.1 CTE join planning
- **File:** `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- In `analyze_ctes()`: when CTE's SelectStatement has joins, call `QueryPlanner::plan()` to produce a join tree for the CTE
- The CTE sub-tree becomes: scan root + lookup children + aggregate at leaf

### 7.2 CTE join serialization
- CTE sub-tree in QueryTree contains QN_SCAN_FRAG + QN_LOOKUP nodes (standard types)
- DBSPJ builds these into the CTE's child Request using existing OpInfo handlers

### 7.3 Result routing
- The aggregate leaf in the CTE join tree writes to hash table (m_isCte flag)
- All intermediate join results stay within the CTE's Request scope

### Tests — Step 7

**Test file:** `mysql-test/suite/ronsql/t/ronsql_cte_joins.test`

```sql
-- Schema for join-inside-CTE tests
CREATE TABLE transactions (
  txn_id INT PRIMARY KEY,
  user_id INT, merchant_id INT,
  amount DECIMAL(10,2), ts DATETIME
) ENGINE=NDB;

CREATE TABLE merchants (
  merchant_id INT PRIMARY KEY,
  merchant_name VARCHAR(64),
  category VARCHAR(32),
  risk_score DECIMAL(3,2)
) ENGINE=NDB;

CREATE TABLE categories (
  category_id INT PRIMARY KEY,
  category_name VARCHAR(64),
  department VARCHAR(32)
) ENGINE=NDB;

CREATE TABLE login_events (
  login_id INT PRIMARY KEY,
  user_id INT, ip_address VARCHAR(45), ts DATETIME
) ENGINE=NDB;

-- T7.1: CTE with join (presentation slide 10 — fraud detection)
RONSQL
WITH risky_purchases AS (
  SELECT t.user_id,
         COUNT(*) AS n_risky_txns,
         SUM(t.amount) AS risky_spend,
         MAX(m.risk_score * t.amount) AS max_risk_exposure
  FROM transactions t
  JOIN merchants m ON m.merchant_id = t.merchant_id
  WHERE t.ts > '2026-03-27' AND m.risk_score > 0.7
  GROUP BY t.user_id
)
SELECT f.user_id, f.age, f.country,
       r.n_risky_txns, r.risky_spend, r.max_risk_exposure
FROM user_profile f
LEFT JOIN risky_purchases r ON r.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3);

-- T7.2: CTE with join + simple CTE together (presentation slide 10 full)
RONSQL
WITH
  risky_purchases AS (
    SELECT t.user_id,
           COUNT(*) AS n_risky_txns,
           SUM(t.amount) AS risky_spend
    FROM transactions t
    JOIN merchants m ON m.merchant_id = t.merchant_id
    WHERE m.risk_score > 0.7
    GROUP BY t.user_id
  ),
  login_patterns AS (
    SELECT user_id,
           COUNT(*) AS n_logins
    FROM login_events
    WHERE ts > '2026-03-28 12:00:00'
    GROUP BY user_id
  )
SELECT f.user_id, f.age,
       h.total_orders, h.churn_score,
       r.n_risky_txns, r.risky_spend,
       l.n_logins
FROM user_profile f
JOIN user_history h ON h.user_id = f.user_id
LEFT JOIN risky_purchases r ON r.user_id = f.user_id
LEFT JOIN login_patterns l ON l.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3, 4, 5);

-- T7.3: CTE join enriching purchase categories
RONSQL
WITH category_spend AS (
  SELECT p.user_id,
         COUNT(*) AS n_purchases,
         SUM(p.amount) AS total_spend
  FROM purchases p
  JOIN categories c ON c.category_id = p.category_id
  WHERE c.department = 'Electronics'
  GROUP BY p.user_id
)
SELECT f.user_id, f.age, cs.n_purchases, cs.total_spend
FROM user_profile f
LEFT JOIN category_spend cs ON cs.user_id = f.user_id
WHERE f.user_id IN (1, 2);

-- T7.4: CTE join — LEFT JOIN with no matching groups
-- (user exists but has no transactions with risky merchants)
```

---

## Step 8: Multi-CTE Feature Store End-to-End

**Goal:** Full feature store query pattern with many CTEs, many feature columns.

### Tests — Step 8

**Test file:** `mysql-test/suite/ronsql/t/ronsql_cte_feature_store.test`

```sql
-- Full schema: feature tables with many columns
CREATE TABLE user_profile (
  user_id INT PRIMARY KEY,
  age INT, gender VARCHAR(1), country VARCHAR(32),
  signup_date DATE, account_age_days INT,
  email_verified TINYINT, phone_verified TINYINT,
  preferred_language VARCHAR(8), timezone VARCHAR(32)
) ENGINE=NDB;

CREATE TABLE user_embeddings (
  user_id INT PRIMARY KEY,
  bert_enc_0 FLOAT, bert_enc_1 FLOAT, bert_enc_2 FLOAT, bert_enc_3 FLOAT,
  bert_enc_4 FLOAT, bert_enc_5 FLOAT, bert_enc_6 FLOAT, bert_enc_7 FLOAT,
  item2vec_0 FLOAT, item2vec_1 FLOAT, item2vec_2 FLOAT, item2vec_3 FLOAT,
  interest_0 FLOAT, interest_1 FLOAT, interest_2 FLOAT, interest_3 FLOAT
) ENGINE=NDB;

CREATE TABLE user_history (
  user_id INT PRIMARY KEY,
  total_orders INT, total_spend DECIMAL(12,2),
  avg_order_value DECIMAL(10,2), max_order_value DECIMAL(10,2),
  days_since_last_order INT, churn_score FLOAT,
  lifetime_value DECIMAL(12,2), return_rate FLOAT
) ENGINE=NDB;

CREATE TABLE purchases (
  purchase_id INT PRIMARY KEY,
  user_id INT, amount DECIMAL(10,2), ts DATETIME,
  category_id INT
) ENGINE=NDB;

CREATE TABLE page_views (
  view_id INT PRIMARY KEY,
  user_id INT, page VARCHAR(64), duration INT, ts DATETIME,
  device VARCHAR(16)
) ENGINE=NDB;

CREATE TABLE clicks (
  click_id INT PRIMARY KEY,
  user_id INT, item_id INT, converted TINYINT, ts DATETIME
) ENGINE=NDB;

-- Load: 100 users, 5000 purchases, 10000 page_views, 8000 clicks

-- T8.1: Fraud detection Feature View (presentation slide 5)
RONSQL
WITH
  purchase_agg AS (
    SELECT user_id,
           COUNT(*) AS n_purchases_24h,
           SUM(amount) AS spend_24h,
           MAX(amount) AS max_purchase_24h
    FROM purchases
    WHERE ts > '2026-03-27'
    GROUP BY user_id
  ),
  login_agg AS (
    SELECT user_id,
           COUNT(*) AS n_logins_1h
    FROM login_events
    WHERE ts > '2026-03-28 12:00:00'
    GROUP BY user_id
  )
SELECT f.*, h.*,
       p.n_purchases_24h, p.spend_24h, p.max_purchase_24h,
       l.n_logins_1h
FROM user_profile f
JOIN user_history h ON h.user_id = f.user_id
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
LEFT JOIN login_agg l ON l.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3);

-- T8.2: Recommendation Feature View (presentation slide 5)
RONSQL
WITH
  view_agg AS (
    SELECT user_id,
           COUNT(*) AS n_views_7d,
           SUM(duration) AS total_view_time_7d
    FROM page_views
    WHERE ts > '2026-03-21'
    GROUP BY user_id
  ),
  click_agg AS (
    SELECT user_id,
           COUNT(*) AS n_clicks_7d,
           SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS conversions_7d
    FROM clicks
    WHERE ts > '2026-03-21'
    GROUP BY user_id
  )
SELECT f.user_id,
       e.bert_enc_0, e.bert_enc_1, e.bert_enc_2, e.bert_enc_3,
       e.item2vec_0, e.item2vec_1, e.item2vec_2, e.item2vec_3,
       e.interest_0, e.interest_1, e.interest_2, e.interest_3,
       v.n_views_7d, v.total_view_time_7d,
       c.n_clicks_7d, c.conversions_7d
FROM user_profile f
JOIN user_embeddings e ON e.user_id = f.user_id
LEFT JOIN view_agg v ON v.user_id = f.user_id
LEFT JOIN click_agg c ON c.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3, 4, 5);

-- T8.3: Credit scoring Feature View (presentation slide 5)
RONSQL
WITH payment_agg AS (
  SELECT user_id,
         COUNT(*) AS n_payments_90d,
         SUM(amount) AS total_paid_90d,
         AVG(amount) AS avg_payment_90d
  FROM purchases
  WHERE ts > '2025-12-28'
  GROUP BY user_id
)
SELECT f.user_id, f.age, f.country,
       h.total_orders, h.total_spend, h.lifetime_value,
       h.churn_score, h.return_rate,
       pa.n_payments_90d, pa.total_paid_90d, pa.avg_payment_90d
FROM user_profile f
JOIN user_history h ON h.user_id = f.user_id
LEFT JOIN payment_agg pa ON pa.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

-- T8.4: Maximum complexity — 3 CTEs + 3 feature tables + batch lookup
RONSQL
WITH
  purchase_agg AS (
    SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total,
           AVG(amount) AS avg_amt, MAX(amount) AS max_amt
    FROM purchases GROUP BY user_id
  ),
  view_agg AS (
    SELECT user_id, COUNT(*) AS views,
           SUM(duration) AS view_time
    FROM page_views GROUP BY user_id
  ),
  click_agg AS (
    SELECT user_id, COUNT(*) AS clicks,
           SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS convs
    FROM clicks GROUP BY user_id
  )
SELECT f.user_id, f.age, f.country, f.signup_date,
       f.email_verified, f.phone_verified,
       e.bert_enc_0, e.bert_enc_1, e.bert_enc_2, e.bert_enc_3,
       e.bert_enc_4, e.bert_enc_5, e.bert_enc_6, e.bert_enc_7,
       e.item2vec_0, e.item2vec_1, e.item2vec_2, e.item2vec_3,
       e.interest_0, e.interest_1, e.interest_2, e.interest_3,
       h.total_orders, h.total_spend, h.avg_order_value,
       h.max_order_value, h.days_since_last_order,
       h.churn_score, h.lifetime_value, h.return_rate,
       p.cnt, p.total, p.avg_amt, p.max_amt,
       v.views, v.view_time,
       c.clicks, c.convs
FROM user_profile f
JOIN user_embeddings e ON e.user_id = f.user_id
JOIN user_history h ON h.user_id = f.user_id
LEFT JOIN purchase_agg p ON p.user_id = f.user_id
LEFT JOIN view_agg v ON v.user_id = f.user_id
LEFT JOIN click_agg c ON c.user_id = f.user_id
WHERE f.user_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
-- 38 output columns: 10 profile + 16 embeddings + 8 history + 4 purchase aggs

-- T8.5: Empty CTE results — all users have no events
-- T8.6: Single user, large event volume (1000+ purchases per user)
-- T8.7: CTE with CASE expression in aggregate
RONSQL
WITH purchase_segments AS (
  SELECT user_id,
         SUM(CASE WHEN amount > 100 THEN 1 ELSE 0 END) AS high_value_count,
         SUM(CASE WHEN amount <= 100 THEN amount ELSE 0 END) AS low_value_spend
  FROM purchases
  GROUP BY user_id
)
SELECT f.user_id, ps.high_value_count, ps.low_value_spend
FROM user_profile f
LEFT JOIN purchase_segments ps ON ps.user_id = f.user_id
WHERE f.user_id = 1;
```

---

## Step 9: Optimization

**Goal:** Performance tuning for production workloads.

### 9.1 Partition alignment detection
- In DBSPJ or RonSQL planner: verify CTE source table partition key matches GROUP BY key
- If not aligned, return error (or fall through to merge phase in future)

### 9.2 Batched CTE lookups
- Instead of one CTE_LOOKUP_REQ per parent row, batch multiple keys into a single signal
- Reduces signal overhead for large batch lookups

### 9.3 Memory limits
- Add configurable max hash table size (max_groups)
- If limit exceeded during materialization, abort the CTE and return error to API
- Eviction is NOT applicable for CTE hash tables — the table must be complete before any CTE_LOOKUP can execute. Unlike streaming aggregation where partial results can be sent to the API, CTE hash tables are random-access lookup structures that must contain all groups.

### 9.4 Non-aligned GROUP BY (future)
- Merge phase: after all LDM threads complete CTE scans, merge partial aggregates for groups that span multiple partitions
- Requires cross-thread communication within the data node

### Tests — Step 9

- T9.1: Performance benchmark — CTE pushdown vs equivalent subquery
- T9.2: Performance benchmark — CTE pushdown vs client-side aggregation + join
- T9.3: Large hash table — 100K groups, verify memory usage
- T9.4: Partition alignment — error on non-aligned GROUP BY key
- T9.5: Latency measurement — verify PK lookups complete before CTE scans

---

## Dependencies Between Steps

```
Step 1 (Parse) ──► Step 2 (Plan) ──► Step 3 (API) ──► Step 4 (DBSPJ Build)
                                                            │
                                                            ▼
                                        Step 5 (Parallel Exec) ──► Step 6 (CTE_LOOKUP)
                                                                         │
                                                            ┌────────────┤
                                                            ▼            ▼
                                                    Step 7 (CTE Joins)  Step 8 (E2E)
                                                                         │
                                                                         ▼
                                                                    Step 9 (Optimize)
```

Steps 1-6 are strictly sequential. Steps 7 and 8 can overlap. Step 9 is independent optimization work.

---

## Test Summary

| Step | Test File | Test Count | Focus |
|------|-----------|------------|-------|
| 1 | `ronsql_cte_parse.test` | 7 | Parsing, validation, error cases |
| 2 | (extends parse tests) | 3 | EXPLAIN plan output |
| 3 | API unit tests | 4 | Serialization correctness |
| 4 | Debug/ndbinfo verification | 4 | Build correctness |
| 5 | Timing/ordering tests | 4 | Parallel execution |
| 6 | `ronsql_cte_basic.test` | 6 | Single CTE, basic aggregates, NULL propagation |
| 7 | `ronsql_cte_joins.test` | 4 | CTE internal joins, mixed CTE types |
| 8 | `ronsql_cte_feature_store.test` | 7 | Feature store patterns, many columns, CASE |
| 9 | Performance benchmarks | 5 | Latency, memory, alignment |
| **Total** | | **44** | |
