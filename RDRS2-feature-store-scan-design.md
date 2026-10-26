# RDRS2 feature_store_scan design

> Confluence: https://hopsworks.atlassian.net/wiki/spaces/RONDB/pages/1662550018/RDRS2+feature_store_scan+design
> (version 1, 2026-09-29). **This file is the working copy.** The Confluence page is a snapshot and will not be updated; all changes and decisions go here.

## 0. Decision log

Decisions taken after the Confluence snapshot. Each is folded into the relevant section below.

| # | Decision | Where |
| --- | --- | --- |
| D1 | Endpoint name is `/feature_store_scan` (underscore style, matching `feature_store` / `batch_feature_store`). | §2 |
| D2 | Keep the per-row `status` array. It is semantically necessary for a Feature View (joined FG rows can be missing); its byte cost is ~11 B/row and compresses to almost nothing. Add `options.includeStatus` (default `true`) so clients that know their view is always complete can switch it off. | §3.4, §3.5, §7.5 |
| D3 | **Primary-key fast path.** When the filter tree is a full-primary-key equality (or an `OR` of them) and no `index` block is present, rewrite the request to the `/batch_feature_store` execution path. No scan is issued. | §4.1 |
| D4 | Response-size policy: keep the positional `features` + single `metadata` list shape (already the compact form), honour the existing `UseCompression` config flag, keep `detailedStatus` opt-in. No further field trimming. | §7.5 |
| D5 | Requests with neither `filters` nor `index` are allowed but return a `warning` field. Not rejected. | §3, §3.4 |
| D6 | **Residual predicates on the fast path, in v1.** "Full-PK equality AND other predicates" also takes the fast path: the residual subtree is compiled with the existing scan-filter compiler into an `NdbInterpretedCode` and attached to the root FG's primary-key read (`OO_INTERPRETED`). A rejected row (NDB error 626 on the root op, the reject code `NdbScanFilter` emits in this fork) is dropped. No server-side comparator code. | §4.1 |
| D7 | **Fast path drops non-existent root keys.** A key with no row in the root FG produces *no output row* on the fast path, exactly as on the scan path. It is never reported as `MISSING`. This makes `limit` accounting and result semantics identical across the two paths. | §4.1, §4.2, §8 |
| D8 | **`limit` on the fast path counts surviving rows.** Entries are deduplicated, processed in request order in chunks of `FeatureStoreScanBatchSize`, and execution stops once `limit` rows have been produced. Branch count is capped at `FeatureStoreScanMaxLimit`. | §4.2 |
| D10 | **Same key, different residuals → scan path.** Branch deduplication only collapses exact duplicates; if two branches share a root key and either carries a residual, the request takes the scan path rather than merging predicates. Keeps the fast path simple and always correct. | §4.2 |
| D9 | `FeatureStoreScanDefaultLimit = 1000`, `FeatureStoreScanMaxLimit = 10000`, per-request cap only (not additionally bounded by `ScanRespBufferSize`). Both are config keys, so they can be tuned per deployment; revisit the defaults after benchmarking. | §7.2 |

No open decisions.

## 1. What is feature_store_scan?

`feature_store_scan` is a new rdrs2 REST endpoint that returns **feature vectors of a Feature View** selected by a **scan** instead of by primary-key lookup. The existing `/feature_store` and `/batch_feature_store` endpoints are strictly primary-key reads: the caller must already know the serving-key values of every row it wants. `feature_store_scan` removes that requirement. The caller describes *which rows* it wants with filters and/or an index range on the Feature View's primary (root) Feature Group, and the server does the rest: it scans the root FG online table, joins the remaining FGs of the Feature View by primary key, Avro-decodes complex features, and returns fully assembled feature vectors in the same positional shape as `/batch_feature_store`.

Typical uses:

* "Give me the feature vectors of all users in country `SE` that signed up in the last 30 days."
* "Give me the 500 most recent transactions of card `4242…`, with the joined customer and merchant features."
* "Give me the feature vector for user `42`" — a primary-key lookup expressed as an equality filter. Supported, and since D3 it executes on the same path as `/feature_store` (see §4.1).

### 1.1 Who should use it?

* You serve from a Hopsworks Feature View and need rows selected by a non-primary-key predicate (or a range on a secondary index).
* Your Feature View is a **star schema**: every Feature Group in the view shares the same primary key as the root Feature Group (see §8).
* You want complex features (`array<…>`, `map<…>`, `struct<…>`) decoded server-side, exactly as `/feature_store` does today.

If you already know the primary keys, `/feature_store` or `/batch_feature_store` are the direct APIs (though `feature_store_scan` with a PK-equality filter now costs the same, §4.1).

## 2. Endpoint

```plaintext
POST /0.1.0/feature_store_scan
```

* Method: `POST`.
* No `{db}/{table}` in the path. The online database and tables are derived from the Feature View metadata, as for `/feature_store`.
* Auth: same Hopsworks API-key header (`X-API-KEY`) as `/feature_store`. The key must have access to every Feature Store referenced by the Feature View (shared Feature Stores included).
* Path constant: `FEATURE_STORE_SCAN_PATH = "/" API_VERSION "/feature_store_scan"` in `constants.hpp`, handled by a new `FeatureStoreScanCtrl`.

## 3. Request body

The request body is the `/batch_feature_store` identification block **without** `entries` / `passedFeatures`, **plus** the row-selection fields of `/scan`:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `featureStoreName` | string | yes | Name of the Feature Store that owns the Feature View. |
| `featureViewName` | string | yes | Name of the Feature View. |
| `featureViewVersion` | int | yes | Version of the Feature View. |
| `filters` | object | no | Binary-tree filter, same grammar as `/scan` (`op` ∈ `AND`, `OR`, `NAND`, `NOR`, `CMP`, `ISNULL`, `ISNOTNULL`; `cond` ∈ `EQ`, `NE`, `LT`, `LE`, `GT`, `GE`; logic nodes carry children in `args`). **Columns must be features of the root Feature Group** (see §3.2). |
| `index` | object | no | Ordered index scan on the root Feature Group online table, same shape as `/scan`: `name`, `key_columns`, `ranges[]` with `lower`/`upper` bounds, `order` (`asc`/`desc`). Key columns must be root-FG features. |
| `limit` | int | no | Maximum number of **root-FG rows** (= feature vectors) returned. Defaults to `FeatureStoreScanDefaultLimit` (proposed 1000); capped at `FeatureStoreScanMaxLimit` (proposed 10000). See §7.2. |
| `metadataOptions` | object | no | Same as `/feature_store`: `{ "featureName": bool, "featureType": bool }`. Global toggles; default both `null`. |
| `options` | object | no | `{ "includeDetailedStatus": bool, "includeStatus": bool }`. See §3.5. `validatePassedFeatures` is not accepted (there are no passed features). |

Fields that are deliberately **absent** compared to the sibling endpoints:

* `entries` / `passedFeatures` (from `/batch_feature_store`) — rows are selected by the scan, not enumerated by the client.
* `readColumns` (from `/scan`) — the Feature View defines the output columns. Every non-label feature of the view is returned, in Feature View order.

At least one of `filters` or `index` is **recommended** but not required (D5). A request with neither is a full scan of the root FG online table, bounded by `limit`. This is allowed for small tables but the response carries a `warning` (see §3.4).

### 3.1 Example schema

Feature View `user_360_fv` v1 in Feature Store `mystore`, built from three cached Feature Groups that all have `user_id` as their primary key (star schema):

| Feature Group (online table) | Role | Columns (NDB type → offline type) |
| --- | --- | --- |
| `user_profile_fg` v1 (`mystore_featurestore.user_profile_fg_1`) | **Root** (joinIndex 0) | `user_id` BIGINT PK → bigint<br>`country` VARCHAR(2) → string<br>`age` INT → int<br>`signup_ts` BIGINT → bigint<br>`interests` VARBINARY(1000) → `array<string>`<br>Secondary index `idx_country_signup (country, signup_ts)` |
| `user_activity_fg` v1 (`mystore_featurestore.user_activity_fg_1`) | Joined on `user_id` | `user_id` BIGINT PK → bigint<br>`sessions_30d` INT → int<br>`last_login_ts` BIGINT → bigint<br>`tags` VARBINARY(500) → `map<string,string>` |
| `user_risk_fg` v1 (`mystore_featurestore.user_risk_fg_1`) | Joined on `user_id` | `user_id` BIGINT PK → bigint<br>`risk_score` DOUBLE → double<br>`flags` VARBINARY(200) → `array<string>` |

The Feature View selects, in this order: `user_id, country, age, signup_ts, interests, sessions_30d, last_login_ts, tags, risk_score, flags`.

```sql
CREATE TABLE user_profile_fg_1 (
  user_id    BIGINT       NOT NULL,
  country    VARCHAR(2),
  age        INT,
  signup_ts  BIGINT,
  interests  VARBINARY(1000),
  PRIMARY KEY (user_id),
  KEY idx_country_signup (country, signup_ts)
) ENGINE=ndbcluster;

CREATE TABLE user_activity_fg_1 (
  user_id        BIGINT NOT NULL,
  sessions_30d   INT,
  last_login_ts  BIGINT,
  tags           VARBINARY(500),
  PRIMARY KEY (user_id)
) ENGINE=ndbcluster;

CREATE TABLE user_risk_fg_1 (
  user_id     BIGINT NOT NULL,
  risk_score  DOUBLE,
  flags       VARBINARY(200),
  PRIMARY KEY (user_id)
) ENGINE=ndbcluster;
```

### 3.2 Filter and index columns: naming rules

* A filter `column` or index `key_column` names a **feature of the root Feature Group** as exposed by the Feature View — that is, `prefix + featureName` if the root join carries a prefix, else the bare feature name. The server resolves it to the physical online column of the root FG table.
* Root-FG **primary-key columns are always filterable**, even if the Feature View excludes them from its feature list (they are the serving keys).
* Naming a feature that belongs to a non-root Feature Group is rejected with `400 FEATURE_NOT_IN_ROOT_FG`. v1 does not push filters into joined FGs (see §8).
* Label features of the root FG are not filterable (they are not present in the online table).
* Complex (Avro-encoded) features are not filterable: NDB filters compare raw bytes, which is meaningless for Avro payloads. Rejected with `400 FILTER_ON_COMPLEX_FEATURE`.

### 3.3 Example requests

**Example A — non-primary-key filter.** All adult Swedish users, at most 100 vectors, with column metadata:

```json
POST /0.1.0/feature_store_scan
{
  "featureStoreName":   "mystore",
  "featureViewName":    "user_360_fv",
  "featureViewVersion": 1,
  "filters": {
    "op": "AND",
    "args": [
      { "op": "CMP", "column": "country", "cond": "EQ", "value": "SE" },
      { "op": "CMP", "column": "age",     "cond": "GE", "value": 18   }
    ]
  },
  "limit": 100,
  "metadataOptions": { "featureName": true, "featureType": true }
}
```

**Example B — ordered index range on the root FG.** The 50 most recent Swedish sign-ups since 2026-01-01, newest first, excluding rows with no interests:

```json
POST /0.1.0/feature_store_scan
{
  "featureStoreName":   "mystore",
  "featureViewName":    "user_360_fv",
  "featureViewVersion": 1,
  "index": {
    "name": "idx_country_signup",
    "key_columns": ["country", "signup_ts"],
    "ranges": [{
      "lower": { "values": ["SE", 1767225600000], "inclusive": true },
      "upper": { "values": ["SE"],                "inclusive": true }
    }],
    "order": "desc"
  },
  "filters": { "op": "ISNOTNULL", "column": "interests" },
  "limit": 50
}
```

**Example C — primary-key lookup expressed as a scan.** Functionally equivalent to a `/feature_store` call with `entries: {"user_id": 42}`:

```json
POST /0.1.0/feature_store_scan
{
  "featureStoreName":   "mystore",
  "featureViewName":    "user_360_fv",
  "featureViewVersion": 1,
  "filters": { "op": "CMP", "column": "user_id", "cond": "EQ", "value": 42 },
  "limit": 1
}
```

Since D3 this request is detected as a full-primary-key equality and executed on the `/batch_feature_store` path: one batched primary-key read across all three FGs, no scan (§4.1). Latency is the same as `/feature_store`. If user 42 does not exist in the root FG the response is `{"features": [], "metadata": [...], "status": [], "rows": 0}` (D7), the same as a scan that matched nothing.

**Example E — primary key plus a residual predicate.** Also on the fast path (D6): the `user_id` equality selects the row, the `age` predicate is pushed into the primary-key read as an interpreted program. If user 42 is under 18 the response has zero rows.

```json
POST /0.1.0/feature_store_scan
{
  "featureStoreName":   "mystore",
  "featureViewName":    "user_360_fv",
  "featureViewVersion": 1,
  "filters": {
    "op": "AND",
    "args": [
      { "op": "CMP", "column": "user_id", "cond": "EQ", "value": 42 },
      { "op": "CMP", "column": "age",     "cond": "GE", "value": 18 }
    ]
  }
}
```

**Example D — several primary keys in one call.** An `OR` of full-PK equalities is also rewritten, into a batch with one entry per branch. Note that the `/scan` filter grammar is a **binary tree**: every logic node takes exactly two `args`, so an n-way `OR` (or `AND`) is written nested. The server flattens nested same-group nodes before fast-path detection, so nesting depth does not matter:

```json
POST /0.1.0/feature_store_scan
{
  "featureStoreName":   "mystore",
  "featureViewName":    "user_360_fv",
  "featureViewVersion": 1,
  "filters": {
    "op": "OR",
    "args": [
      { "op": "CMP", "column": "user_id", "cond": "EQ", "value": 42 },
      { "op": "OR",
        "args": [
          { "op": "CMP", "column": "user_id", "cond": "EQ", "value": 77 },
          { "op": "CMP", "column": "user_id", "cond": "EQ", "value": 91 }
        ] }
    ]
  }
}
```

### 3.4 Example response

Response for Example A. The shape is `BatchFeatureStoreResponse`: positional `features` rows in Feature View order, one `metadata` entry per column, one `status` per row.

```json
{
  "features": [
    [42, "SE", 34, 1774000000000, ["hiking", "jazz"],
      17, 1779222191000, { "plan": "pro", "beta": "true" },
      0.12, []],
    [77, "SE", 51, 1770000000000, ["chess"],
      3,  1779100000000, { "plan": "free" },
      0.61, ["chargeback_2025"]],
    [91, "SE", 19, 1779000000000, null,
      null, null, null,
      null, null]
  ],
  "metadata": [
    { "featureName": "user_id",       "featureType": "bigint" },
    { "featureName": "country",       "featureType": "string" },
    { "featureName": "age",           "featureType": "int" },
    { "featureName": "signup_ts",     "featureType": "bigint" },
    { "featureName": "interests",     "featureType": "array<string>" },
    { "featureName": "sessions_30d",  "featureType": "int" },
    { "featureName": "last_login_ts", "featureType": "bigint" },
    { "featureName": "tags",          "featureType": "map<string,string>" },
    { "featureName": "risk_score",    "featureType": "double" },
    { "featureName": "flags",         "featureType": "array<string>" }
  ],
  "status": ["COMPLETE", "COMPLETE", "MISSING"],
  "rows": 3
}
```

Reading the third row: user 91 exists in the root FG (so it matched the scan) but has no row yet in `user_activity_fg` or `user_risk_fg`. Its non-root features are `null` and the row status is `MISSING`, exactly as `/batch_feature_store` reports a partially-missing entry today.

Response fields:

* `features` — array of rows; each row is a positional array ordered by the Feature View's feature order (label features excluded). Complex features are decoded JSON values, never base64.
* `metadata` — N entries, positional with the row columns, when at least one `metadataOptions` flag is set; otherwise an empty array (see below). `featureType` is the Hopsworks offline type.
* `status` — one entry per row: `COMPLETE` (every FG returned a row), `MISSING` (at least one non-root FG had no row; those features are `null`), `ERROR` (a per-row failure, e.g. Avro decode; see §5). Rows from spine FGs are always `MISSING`, as today. Omitted entirely when `options.includeStatus = false` (D2).
* `detailedStatus` — present only when `options.includeDetailedStatus = true`; per row, per FG `{ "httpStatus", "featureGroupId" }`. Same as `/batch_feature_store`.
* `rows` — number of rows in `features`. Convenience field mirroring `/scan`.
* `warning` — optional string, emitted only when the request had neither `filters` nor `index` (`"full scan of root feature group; consider adding filters or an index range"`).

If `metadataOptions` is omitted, or both flags are `false`, the `metadata` array is present but **empty** (`"metadata": []`), matching `/feature_store`. (`/batch_feature_store` instead emits one `{ "featureName": null, "featureType": null }` entry per feature.) When at least one flag is set the array has one entry per feature, with the unrequested field `null`.

Compare with what `/scan` returns today for the root table alone (keyed objects, column names repeated per row, VARBINARY as base64):

```json
{
  "data": [
    { "user_id": 42, "country": "SE", "age": 34, "signup_ts": 1774000000000,
      "interests": "AAQMaGlraW5nCGphenoA" }
  ],
  "rows": 1
}
```

### 3.5 metadataOptions and options

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `metadataOptions.featureName` | bool | `null` | Populate `metadata[i].featureName`. |
| `metadataOptions.featureType` | bool | `null` | Populate `metadata[i].featureType` with the Hopsworks offline type. |
| `options.includeStatus` | bool | `true` | Emit the per-row `status` array. Set to `false` only when the client knows every FG in the view always has a row for every root key (D2). |
| `options.includeDetailedStatus` | bool | `false` | Emit `detailedStatus` per row / per FG. Implies `includeStatus`. |

Unknown sub-option keys are rejected with `400 ERROR_INVALID_BODY`, same parser strictness as `/feature_store`.

## 4. Execution model

The endpoint composes two existing building blocks: the index/table scan of `/scan` and the per-FG primary-key fan-out plus vector assembly of `/batch_feature_store`.

1. **Parse and validate.** Body parsed with simdjson into a new `FeatureStoreScanRequest` (Feature View identifiers + `ScanReadParams`-style `filterRoot`, `index`, `limit`). Validation of the scan sub-tree reuses `ValidateScanFilter` / `ValidateScanIndex`.
2. **Resolve Feature View metadata.** `FeatureViewMetadataCache_Get(featureStoreName, featureViewName, featureViewVersion)` — the same in-process cache, ref-counting and event-driven invalidation as `/feature_store`. No new cache is introduced.
3. **Identify the root Feature Group and check the star-schema constraint.** The root FG is the `FeatureGroupFeatures` entry with `joinIndex == 0`. For every other FG in the view, each of its serving keys must resolve (via `ServingKey.joinOn` / `requiredJoinKeyMap`) to a primary-key column of the root FG, and the set must cover that FG's full primary key. If not, reject with `400 FEATURE_VIEW_NOT_STAR_SCHEMA`. This check is done once per cache entry and memoised on the metadata object.
4. **Resolve filter/index columns.** Each column name is looked up in the root FG's feature list (prefix-aware) plus its primary-key serving keys, and mapped to the online column. Errors: `FEATURE_NOT_IN_ROOT_FG`, `FILTER_ON_COMPLEX_FEATURE`.
5. **Primary-key fast-path detection (D3, D6).** Walk the resolved filter tree; if it qualifies (§4.1) and no `index` block is present, build one `entries` map (plus optional residual `NdbInterpretedCode`) per branch, dedup, and run the chunked `pk_batch_read` loop of §4.2 with the `/batch_feature_store` machinery. Rows whose root op returns 626 (absent, or rejected by the residual program) are dropped (D7). Otherwise continue with the scan path.
6. **Authenticate.** API key checked against all Feature Stores of the view, identical to `/feature_store`. Rate-limit identity derived from the key.
7. **Scan the root FG online table.** One `scan_read`-style NDB index/table scan on `{fs}_featurestore.{rootFg}_{version}`, reading the root FG's non-label feature columns **plus all of its primary-key columns** (they are needed for the join even if the view does not expose them). Filters and index ranges are compiled exactly as in `/scan`. The scan stops at `limit` rows.
8. **Fan out primary-key reads to the remaining FGs.** For each scanned root row, the root PK values are copied into a `PKReadParams` for every non-root, non-spine FG using the existing serving-key mapping (`FillPrimaryKey`). All operations are executed as **one** `pk_batch_read` call per chunk of `FeatureStoreScanBatchSize` root rows (proposed 256), i.e. `rows × (numFGs − 1)` operations in `ceil(rows / 256)` round-trips. This reuses the batching path of `/batch_feature_store` verbatim.
9. **Assemble vectors and decode.** Per row, values are placed by `featureIndexLookup` into a positional vector; complex features are decoded with the decoders in `FeatureViewMetadata.complexFeatures` (`DeserialiseComplexFeature`). Root-FG values come from the scan buffer, joined-FG values from the batch response. A missing joined row leaves `null`s and marks the row `MISSING`.
10. **Serialise.** Output written with the existing `BatchFeatureStoreResponse` printer, plus `rows` and optional `warning`. Content type follows `UseCompression` (§7.5).

```plaintext
client ──POST──▶ FeatureStoreScanCtrl
                   │  1-4  parse, FV metadata (cache), root-FG + star-schema check, column resolution
                   │  5    PK fast path?  ──yes──▶  entries → pk_batch_read [all FGs × N]  ──▶ 9
                   │  6    auth                 (same as /batch_feature_store)
                   │  7    scan  mystore_featurestore.user_profile_fg_1  (filters/index/limit)
                   │          └─▶ N root rows with PK values
                   │  8    pk_batch_read  [ user_activity_fg_1 × N , user_risk_fg_1 × N ]  (chunked)
                   │  9    assemble N vectors in FV order, Avro-decode complex columns
                   ▼  10   { features[N], metadata, status[N], rows }
```

### 4.1 Primary-key fast path (D3)

**Why.** A scan filter on the primary key does *not* become a primary-key read inside NDB. The predicate compiles to an `NdbScanFilter` interpreted program that runs on every fragment of the root table; each data node reads through its share of rows and returns the matches. NDB's primary key is a hash index, not an ordered one, so the `index` block cannot use it either (unless a separate ordered index exists on the same columns). On top of that the scan path needs two NDB round-trips (root scan, then fan-out) where `/feature_store` needs one batched read, and the scan pays fixed setup cost (scan transaction, filter compilation, waiting on every fragment, scan close).

| Path | Rows examined | NDB round trips |
| --- | --- | --- |
| `/feature_store` | 1 per FG | 1 batched |
| `feature_store_scan`, PK-equality filter, **without** fast path | all rows of the root table, then 1 per joined FG | 2 |
| `feature_store_scan`, PK-equality filter, **with** fast path | 1 per FG | 1 batched |

**Detection rule.** After column resolution (step 4), nested same-group logic nodes are flattened (`AND(AND(a,b),c)` → leaves `a,b,c`; likewise for `OR`), which is lossless because both operators are associative and lets the binary-tree grammar express n-ary conjunctions/disjunctions. The tree then qualifies when it has one of these shapes:

1. A single `CMP` node with `cond: EQ` on the root's only primary-key column.
2. An `AND` whose leaves contain one `CMP EQ` node per root primary-key column (each column exactly once; a repeated PK equality is treated as residual). Any **other** leaves form the *residual predicate* (D6). For a single-column PK this covers `user_id EQ 42 AND age GE 18` (Example E).
3. An `OR` whose leaves are each of shape 1 or 2. Becomes a batch with one entry per branch (Example D), each branch carrying its own residual.

Everything else stays on the scan path: a partial composite key; `NE` or a range on a PK column; a PK equality with a `null` value; a PK equality nested under `NAND` / `NOR`; a PK equality under an `OR` that also contains a non-key predicate (e.g. `(user_id EQ 42 OR age GE 18)`). An `index` block in the request also disables the rewrite, since the caller explicitly asked for an ordered scan.

**What the rewrite does.** Each qualifying branch becomes an entries map in the form `feature_store_parse` produces today: serving-key name (root prefix + feature name) → JSON value. From there the request is identical to a `/batch_feature_store` call: `GetBatchPkReadParams`, one `pk_batch_read` across all Feature Groups in parallel, then the same assembly and Avro decoding. The response shape is unchanged, so the caller cannot tell which path ran except by latency and by row order (§4.2).

**Residual predicate (D6).** The residual subtree is pushed into NDB rather than evaluated in rdrs2:

1. Wrap the residual children in an `AND` `FilterNode` (or take the single child as-is).
2. Compile it with the compiler the scan path already uses (`rdrs_dal.cpp`, `NdbInterpretedCode filter_code(*table_rec); NdbScanFilter filter(&filter_code)`) against the **root FG table record**. Nothing in that function is scan-specific.
3. On the root FG's primary-key read in `pkr_operation.cpp` (`trans->readTuple(NdbRecord…, &opts)`), set `opts.optionsPresent |= OO_INTERPRETED; opts.interpretedCode = &filter_code;`. Only the root FG op carries the program; joined-FG reads are unchanged.
4. When the program reaches `interpret_exit_nok`, the root operation fails with NDB error **626** (in this fork `NdbInterpretedCode::interpret_exit_nok()` defaults to 626, *Tuple did not exist*, so a rejected row is reported exactly like an absent one: classification `NoDataFound`, sub-response 404). The controller **drops the whole vector**, discarding the joined-FG results for that key, which were issued in the same batch. No special-casing in the PK response handler is needed.

The comparison semantics are NDB's own, identical to the scan path, so the two paths cannot disagree on type coercion, collation or null handling. Any predicate the scan path accepts on root columns works here, including `ISNULL`, ranges and nested logic. The joined-FG reads for a rejected key are wasted; the alternative (root read first, fan-out only for survivors) adds a round trip and defeats the purpose.

**Non-existent root keys (D7).** A root-FG read that returns *no row* (NDB 626) produces **no output row**, exactly like the scan path. It is *not* reported as `MISSING`; `MISSING` is reserved for "root row found, some joined FG row absent". Joined-FG results for that key are discarded. Consequently the fast path and the scan path return the same rows for the same predicate; only the order may differ (§4.2).

| Root op result | Output |
| --- | --- |
| row returned, all joined FGs returned | row, `COMPLETE` |
| row returned, some joined FG missing (626) | row with `null`s, `MISSING` |
| row returned, Avro decode failure | row, `ERROR` |
| 626 (key not in root FG, or rejected by the residual predicate) | dropped |
| any other non-200 sub-response on the root op | row, `ERROR` (never mistaken for an absent key) |
| other NDB error | request fails (§6) |

**Cost of detection.** Linear in the number of filter nodes, no NDB access; the root primary-key column set is already on the cached metadata. Residual compilation is the same cost the scan path pays for its filter.

### 4.2 `limit` and ordering on the fast path (D8)

`limit` counts **rows produced**, on both paths. On the fast path the candidate set is the list of `OR` branches, so:

1. **Deduplicate entries**, keeping the first occurrence. A key listed twice in an `OR` must yield one row, as a scan would. Deduplication compares the *value*, not its JSON spelling: `42`, `42.0` and `"42"` select the same row (RonDB coerces to the column type) and collapse to one branch. Two branches that name the **same key with different residuals** (e.g. `(id = 1 AND a > 5) OR (id = 1 AND b < 3)`, or a bare `id = 1` next to `id = 1 AND …`) are not plain duplicates: their union would require merging the residual programs. Such a request is **not** served by the fast path; it falls back to the scan, which evaluates the whole tree as one filter (D10).
2. **Cap the branch count.** More than `FeatureStoreScanMaxLimit` branches → `400 LIMIT_EXCEEDED`, the same bound a scan has.
3. **Without a residual predicate and with D7**, a branch yields at most one row, but it may yield none (key absent). So the same chunked loop as below is used; in the common case where every key exists it terminates after `ceil(limit / FeatureStoreScanBatchSize)` chunks.
4. **Chunked execution.** Process entries in request order in chunks of `FeatureStoreScanBatchSize`. After each chunk, append surviving rows (root op 200) to the output. Stop issuing chunks once the output holds `limit` rows. If the last chunk overshoots, truncate to `limit`. Waste is bounded to one chunk.

Ordering: the fast path returns rows in **branch order** (deterministic). The scan path without `index` returns rows in NDB fragment/scan order (unspecified). Clients that need a defined order must use `index`, which disables the fast path anyway, so there is no conflict — but the difference is documented so nobody relies on fast-path order being reproducible on the scan path.

## 5. What gets decoded?

Decoding follows the Feature View metadata, not the request:

* Every feature in `FeatureViewMetadata.complexFeatures` (any FG, root or joined) whose online column is `VARBINARY`/`LONGVARBINARY` is Avro-decoded and emitted as native JSON (array, object, nested record…).
* All other features are emitted as `/feature_store` emits them today.
* There is no way to opt out of decoding on this endpoint. Callers who want raw bytes should use `/scan`.
* An Avro decode failure marks **that row** `ERROR` (with a `detailedStatus` entry naming the FG) rather than failing the whole response. This mirrors `/batch_feature_store`; the per-row status array makes the softer behaviour possible.

## 6. Errors

| HTTP | Code | Reason |
| --- | --- | --- |
| 400 | `ERROR_INVALID_BODY` | Missing/malformed `featureStoreName`, `featureViewName`, `featureViewVersion`; unknown `metadataOptions`/`options` keys; `entries`, `passedFeatures` or `readColumns` present. |
| 404 | `FV_NOT_EXIST` / `FS_NOT_EXIST` | Feature View or Feature Store not found. These are the existing feature-store error codes and they carry HTTP **404**, exactly as `/feature_store` and `/batch_feature_store` report them; the endpoint keeps that for consistency with its siblings. |
| 400 | `FEATURE_VIEW_NOT_STAR_SCHEMA` | Some Feature Group in the view is not joined on the root FG's full primary key. |
| 400 | `ROOT_FG_NOT_ONLINE` | The root FG is a spine or on-demand FG; there is no online table to scan. |
| 400 | `FEATURE_NOT_IN_ROOT_FG` | A filter/index column is not a feature (or PK) of the root Feature Group. |
| 400 | `FILTER_ON_COMPLEX_FEATURE` | A filter/index column is an Avro-encoded feature. |
| 400 | `LIMIT_EXCEEDED` | `limit` above `FeatureStoreScanMaxLimit`. |
| 400 | (scan validation) | Any error the `/scan` parser and validators produce for `filters` / `index`: logic nodes must have exactly two `args`, `index` must carry `ranges`, unknown index, bad bound arity, type mismatch, … |
| 400 | (pk validation) | On the fast path: the same primary-key value errors `/feature_store` reports (`WRONG_DATA_TYPE`, …). |
| 401 / 403 | — | API key invalid or lacks access to a Feature Store of the view. |
| 429 | `TOO_MANY_REQUESTS` | Rate limit hit, either on the scan or on the fan-out batch. |
| 500 | — | Hopsworks metadata query failure while populating the cache; NDB error during scan or batch read that is not row-scoped. |

Row-scoped problems (a joined FG row missing, an Avro decode failure on one row) do **not** fail the request; they surface in `status[i]` / `detailedStatus[i]`.

## 7. Performance

### 7.1 Cost model

For a Feature View with `G` Feature Groups and a request returning `N` rows on the **scan path**:

* 1 NDB scan (index range or filtered table scan) on the root FG, cost proportional to rows *examined*, not rows returned. A filter without a supporting index examines the whole root table — same caveat as `/scan`.
* `N × (G − 1)` primary-key reads, batched into `ceil(N / FeatureStoreScanBatchSize)` `pk_batch_read` calls. Each batch is one NDB round-trip with internal parallelism, the same path `/batch_feature_store` uses for `N` entries.
* One Avro unmarshal + JSON re-serialise per complex feature per row (dominant CPU cost on rows with many complex columns).

Rule of thumb: `feature_store_scan(limit=N)` ≈ `/scan(limit=N)` on the root table + `/batch_feature_store` with `N` entries.

On the **fast path** (§4.1) the cost is exactly that of `/batch_feature_store` with `N` entries.

### 7.2 Limits and configuration

| Config key (rdrs2 JSON config) | Proposed default | Meaning |
| --- | --- | --- |
| `FeatureStoreScanDefaultLimit` | 1000 | `limit` applied when the request omits it (D9). |
| `FeatureStoreScanMaxLimit` | 10000 | Hard cap on `limit` and on the number of fast-path branches; larger requests are rejected with 400. Protects the server from unbounded fan-out (D9). |
| `FeatureStoreScanBatchSize` | 256 | Root rows per `pk_batch_read` round-trip in step 8. |
| `ScanRespBufferSize` | existing | Reused for the root scan buffer. |
| `UseCompression` | existing (`true`) | Same meaning as for the other endpoints, see §7.5. |

### 7.3 Caching

No new cache. Feature View metadata, serving-key maps and Avro decoders come from the existing Feature View metadata cache, with the same preload at start-up, the same event-watcher invalidation on Hopsworks metadata changes, and the same concurrent-first-miss deduplication. The star-schema verdict, the root-FG column map and the root primary-key column set (used by §4.1) are computed once and memoised on the cache entry.

### 7.4 Metrics

A new `FeatureStoreScanEndPointMetricsUpdater` exposes the endpoint in `/metrics` with request count, latency histogram, `rows_fetched`, the number of fan-out batches per request, and a counter of fast-path vs scan-path executions. Scan-phase timing hooks (`ScanPhaseTiming`) are reused so slow root scans appear in `/0.1.0/slow-scans` with the Feature View name in the context.

### 7.5 Response size (D2, D4)

The response uses positional row arrays plus a single `metadata` list instead of per-row keyed objects, inherited through the `BatchFeatureStoreResponse` shape. Column names appear once per response, not once per row; for a ten-feature vector with short values this roughly halves the payload compared with the keyed `/scan` format.

The per-row `status` field is kept (D2):

* For a single FG every returned row is by definition complete, so `status` carries no information there. For a Feature View it does: a joined FG can have no row for a scanned key, and the client must be able to tell "value is null" from "row was missing".
* The cost is ~11 bytes per row (`"COMPLETE",`) against vectors that are typically well over 100 bytes, i.e. under 10 %, and it gzips to almost nothing because it is the same string repeated.
* Clients that know their view is always complete can set `options.includeStatus = false`.

Other decisions (D4):

* **Honour `UseCompression`.** The existing endpoints read `globalConfigs.rest.useCompression` and set the content type to `application/json` when true and `application/octet-stream` when false; Drogon gzips the former when the client sends `Accept-Encoding: gzip`. The new controller does the same. On a scan returning hundreds of rows gzip outweighs any field-level trimming.
* **`detailedStatus` stays opt-in.** Per-row per-FG objects would dominate the payload if on by default.
* **`metadata` values stay `null` by default**, as in `/feature_store`; the array itself is always present for positional alignment.
* No further trimming (e.g. omitting `metadata` entirely, or omitting `status` when all rows are `COMPLETE`): optional-key semantics complicate clients for a marginal gain.

## 8. Limitations & FAQ

**Q: Why only star schema?**
Because the join in step 8 is a primary-key read on each joined FG using values taken from the *root row*. That only works if the root row carries the full primary key of every other FG, i.e. every FG shares the root's PK. Snowflake joins (FG B keyed by a column of FG A that is not the root PK) would need a second level of fan-out; not in v1.

**Q: Why can filters only reference the root Feature Group?**
A filter on a joined FG (e.g. `risk_score > 0.5`) cannot be pushed into the root scan, so it would have to be applied *after* the fan-out. That makes `limit` semantics unpredictable (the server might scan 10,000 root rows to find 100 matches) and turns the request into a join-then-filter with unbounded cost. v1 rejects it with `FEATURE_NOT_IN_ROOT_FG`. Post-join filtering with a separate `scanBudget` is a candidate for v2.

**Q: Is a PK-equality filter slower than `/feature_store`?**
Not since D3. It is detected and executed on the same path (§4.1), including when extra predicates are AND-ed with the key (D6). Result rows are identical to what the scan path would return: a key that does not exist in the root FG, or that fails the residual predicate, produces no row (D7). The only observable differences from the scan path are latency and row order (§4.2). The only observable difference from `/batch_feature_store` is that absent root keys are dropped instead of reported as `MISSING`.

**Q: Can I choose a subset of features to return?**
Not in v1: the Feature View defines the output. A `readFeatures` subset (which would also let the server skip fan-out to FGs that contribute nothing) is a natural follow-up.

**Q: Is there pagination?**
No cursor in v1. Use an ordered index scan and move the lower bound forward on the client (keyset pagination). A server-side cursor would require holding an NDB scan open across HTTP requests, which the stateless request model does not support.

**Q: What about on-demand and spine Feature Groups?**
On-demand FGs are not readable online and are skipped; spine FGs yield `MISSING` status as today. A view whose *root* FG is spine or on-demand is rejected with `400 ROOT_FG_NOT_ONLINE` — there is nothing to scan.

**Q: What about self-joins (the same FG appearing twice with different prefixes)?**
Supported as long as both occurrences are joined on the root PK; `joinIndex` disambiguates them, as today.

**Q: Does this change `/feature_store`, `/batch_feature_store` or `/scan`?**
No. It is purely additive. Shared helpers (`FillPrimaryKey`, `DeserialiseComplexFeature`, `ValidateScanFilter`, `GetBatchPkReadParams`, …) may move to a common file but keep their behaviour.

**Q: Can I use it without a Hopsworks API key?**
Only when the cluster runs with `useHopsworksAPIKeys = false`, same as the other endpoints.

## 9. Relationship to existing endpoints

| Endpoint | Path | Row selection | Scope | Decodes complex VARBINARY? | Hopsworks metadata |
| --- | --- | --- | --- | --- | --- |
| `/scan` | `/0.1.0/{db}/{table}/scan` | filters / index range | one table | No (base64) | No |
| `/feature_store` | `/0.1.0/feature_store` | primary key (1 entry) | Feature View | Yes | FV-keyed |
| `/batch_feature_store` | `/0.1.0/batch_feature_store` | primary key (N entries) | Feature View | Yes | FV-keyed |
| **`/feature_store_scan`** | `/0.1.0/feature_store_scan` | **filters / index range on root FG; PK equality auto-routed to batch path** | **Feature View (star schema)** | **Yes** | **FV-keyed** |

## 10. Implementation sketch

New/changed files under `storage/ndb/rest-server2/server/src/`:

* `feature_store_scan_ctrl.{hpp,cpp}` — Drogon controller, `FEATURE_STORE_SCAN_PATH`, steps 1–10 above, including the fast-path branch.
* `feature_store_scan_data_structs.hpp` — `FeatureStoreScanRequest` (FV identifiers, `filterRoot`, `index`, `limit`, `metadataRequest`, `optionsRequest` with the new `includeStatus`).
* `json_parser.{hpp,cpp}` — `feature_store_scan_parse()` composed from the existing `feature_store_parse` identifier handling and the `scan_parse` filter/index/limit handling.
* `metadata.{hpp,cpp}` — `FeatureViewMetadata::rootFgIndex` (index into `featureGroupFeatures`), `isStarSchema`; populated once per cache entry. Filter columns are resolved through `prefixFeaturesLookup` and the root FG's serving keys. `ServingKeyEntryNames()` gives the entry names of a serving key.
* `feature_store_scan_pk_rewrite.{hpp,cpp}` (or inside the ctrl) — the §4.1 tree walk: `bool TryRewriteToEntries(const FilterNode&, const RootFgInfo&, std::vector<PkBranch>&)` where `PkBranch = { EntriesMap entries; std::shared_ptr<FilterNode> residual; }`; plus dedup and the chunked-limit loop of §4.2.
* `rdrs_dal.{cpp,hpp}` — expose the scan-filter compiler (`FilterNode` → `NdbInterpretedCode` for a given table record) so the fast path can compile residuals.
* `db_operations/pk/pkr_operation.{cpp,hpp}` — optional per-operation residual `FilterNode` (passed as a side array through `pk_batch_read`), compiled into an `NdbInterpretedCode` and attached with `OO_INTERPRETED` to that read. A rejected row comes back as 626, i.e. exactly like "not found"; no change to response handling.
* `config_structs_def.hpp` — the three new config keys of §7.2.
* `metrics.{hpp,cpp}` — `FeatureStoreScanEndPointMetricsUpdater`.
* `constants.hpp` — `FEATURE_STORE_SCAN`, `FEATURE_STORE_SCAN_PATH`, `INCLUDE_STATUS`.
* `CMakeLists.txt` — add the new sources.

Tests (MTR-style REST tests alongside the existing feature-store tests):

1. Star-schema FV with 3 FGs: filter + index + limit (Examples A, B).
2. `MISSING` rows when a joined FG lacks the key; `includeStatus = false` omits the array.
3. Every 400 in §6, including `FEATURE_VIEW_NOT_STAR_SCHEMA` on a snowflake view and `ROOT_FG_NOT_ONLINE`.
4. Complex-feature decoding across root and joined FGs; per-row `ERROR` on a corrupt Avro payload.
5. Fast path: Examples C and D produce byte-identical `features`/`metadata` to the equivalent `/feature_store` / `/batch_feature_store` calls when every key exists; an absent root key yields no row (D7) while `/batch_feature_store` yields `MISSING`; a composite-PK view with a partial-key filter does **not** take the fast path; `index` present disables it; metrics counter increments.
6. Residual predicate (Example E): row dropped when the predicate fails, kept when it passes; result set equals the scan path's for the same filter; `ISNULL` / range / nested `OR` residuals; residual on a composite-PK view.
7. `limit` on the fast path: duplicate keys in an `OR` collapse to one row; `limit` smaller than the surviving-row count truncates in branch order; `limit` larger than the survivor count returns all survivors; more branches than `FeatureStoreScanMaxLimit` → 400; chunk boundary case (`limit` reached mid-chunk).
8. Rate-limit 429 on the fan-out.
9. `UseCompression` true/false content types.

## 10.1 Implementation status (2026-09-29)

Implemented on branch `26.02-main` (uncommitted), built and exercised against the `rdrs2-golang` MTR cluster:

| Area | Files |
| --- | --- |
| Endpoint | `feature_store_scan_ctrl.{hpp,cpp}` (controller, column resolution, fast-path detection, scan sink, fan-out, printer), `feature_store_scan_data_structs.hpp`, `constants.hpp`, `CMakeLists.txt` |
| Parser | `json_parser.{hpp,cpp}` — `feature_store_scan_parse()`; also fixes a pre-existing crash in `handle_simdjson_error()` on an empty request body (affected every endpoint) |
| Metadata | `metadata.{hpp,cpp}` — root FG index, star-schema verdict, root column lookup, PK entry names; computed once per cache entry |
| Scan reuse | `scan_row_sink.hpp`, `rdrs_dal.{h,cpp}` — `ScanRowSink`, `scan_read_rows()`, `ScanColumnToJson()`, `ScanColumnRawBytes()`, `CompileFilterProgram()`; `/scan` output unchanged |
| PK residual | `pk_data_structs.hpp` (filter helper declarations), `rdrs_dal.{h,cpp}` (`pk_batch_read(..., op_filters)`), `db_operations/pk/pkr_operation.{hpp,cpp}` (`OO_INTERPRETED` on the root read) |
| Config | `config_structs_def.hpp` — `FeatureStoreScanDefaultLimit`, `FeatureStoreScanMaxLimit`, `FeatureStoreScanBatchSize` |
| Errors | `feature_store_error_code.hpp` — codes 21–25 |
| Metrics | `metrics.{hpp,cpp}` — `FeatureStoreScanEndPointMetricsUpdater`, request/latency/rows/key-request/fast-path counters under endpoint `feature_store_scan` |
| Tests | Go: `test_go/internal/integrationtests/featurestorescan/` (fast path, residual, dedup, limit, composite key, scan path, ordered index scan, complex types, self-join, options, error table); C++: `test/feature_store_scan_test.cpp` (detection and column resolution, no cluster needed) |

Behavioural notes discovered while testing, now reflected above: the reject code is 626 (§4.1); logic nodes are binary (Example D, §6); unknown feature view / store return 404 like the sibling endpoints (§6).

## 11. Open decisions

* [x] Default and max `limit` values → 1000 / 10000 as proposed, per-request cap only (D9). Revisit after benchmarking.
* [x] Fast path for "full-PK equality AND residual predicate" → yes, in v1, via interpreted program on the root PK read (D6).
* [x] Absent root keys on the fast path → dropped, never `MISSING` (D7).
* [x] `limit` on the fast path → counts surviving rows, chunked execution, dedup, branch cap (D8).
* [x] Endpoint name → `/feature_store_scan` (D1).
* [x] Keep per-row `status` → yes, with `options.includeStatus` opt-out (D2).
* [x] Rewrite full-PK equality to the batch path → yes, in v1 (D3).
* [x] Filter-less / index-less requests → allowed with `warning` (D5).
