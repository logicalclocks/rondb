# M1 — detailed plan: emitted branches served, proper error codes

Milestone M1 of `ronsql_fs_support_plan.md`, expanded to implementation
detail.  Five packages; M1.0 is new (requested 2026-09-14): RonSQL errors
carry the right HTTP status instead of 500 for everything.

| package | finding | requirement row | effort | order |
|---|---|---|---|---|
| M1.0 | proper error codes (was BUGS_TODO "HTTP status") | — (client contract) | 2–3 days | first: touches every throw site |
| M1.1 | F9 temporal MIN/MAX unquoted in JSON | R-A5-types (part) | hours | second |
| M1.2 | F1 string aggregate re-use crash | R-F1 | 1–3 days (verify first) | third |
| M1.3 | F0 collect CTE form | R-S6 | ~1 week | parallel with M1.4 |
| M1.4 | F7 binary projections | R-A2-binary | ~1 week | parallel with M1.3 |

Exit: `.fs_verify --requirements --all --vectors` shows R-S6, R-F1 and
R-A2-binary SUPPORTED on the base, JIT and ng2r2 arms (R-A5-types stays
UNSUPPORTED through F4/F5/F6 until M2); `known-error` is 0 in both
fuzzers; the error-code tests of M1.0 pass; the three MTR suites are green
×3 with `ronsql_fs_jit` strict-armed.

---

## M1.0 — Proper HTTP status codes for RonSQL errors

**Status: DONE 2026-09-14 on RONDB-1124 — verified: `rdrs2-golang_gotest`
(incl. `TestErrorStatusByClass`), `ronsql`, `ronsql_cte`, `ronsql_fs`,
`ronsql_fs_jit`, `ronsql_fs_ng2r2` green; results embedding RDRS error
bodies re-recorded for the `[<class>]` prefix.**  Implementation notes: the class lives on
`RonSQLPermanentError` (`RonSQLCommon.hpp`), set explicitly at the parser
error, the `feature_not_implemented` macro, the OOM / no-Ndb paths and the
NDB-error rethrow (classified by `NdbError::Classification`, NDB code
attached), and by `ronsql_classify_message()` for every other site — an
unrecognized wording stays INTERNAL (500) so it is visible as
`[internal]`.  `ronsql_op` maps the class to 400 / 413 / 503 / 500,
prefixes the body with `[<class>]` and exports the class and NDB code
through `RonSQLExecParams`; `ronsql_ctrl` sets the status from the
operation and adds `X-RonSQL-Error-Class` / `X-RonSQL-NDB-Error`.
Framework: `exec.Classify` accepts the new statuses and keeps the old
framing; `exec.ErrorClass` parses the prefix.
Verification notes: the controller's parse-only pre-check had its own
catch (unprefixed, 500) — now classified through the shared
`ronsql_http_code_for`; the unknown-column error surfaces through the
stale-schema path as "Could not find column" (added to the SEMANTIC
vocabulary).  While running `rdrs2-golang_gotest` a pre-existing, unrelated
failure surfaced once the `ronsql` package passed: the runner stops at the
first failing package, and `ronsqltpch` (written for the dedicated
`rdrs2-ronsqltpch` suite) fails with 401 in the generic suite since
RONDB-1104 enabled API keys there; it is now excluded from the generic
run (`run_gotest.inc`).

### Today

`storage/ndb/rest-server2/server/src/db_operations/ronsql/ronsql_operation.cpp`
maps every RonSQL outcome to one of three codes:

| exception | HTTP | body |
|---|---|---|
| `RonSQLRateLimitError` | 429 (via `__RONDB_ERROR_CODE_HTTP_CODE`) | `ERROR_RONSQL_RATE_LIMIT` |
| `RonSQLRetryableError` after the retry budget | 500 (`RS_SERVER_ERROR`) | `ERROR_RONSQL_TEMPORARY` + detail |
| `RonSQLPermanentError` | 500 (`RS_SERVER_ERROR`) | `ERROR_RONSQL_PERMANENT` + `Caught exception: <what>` |
| anything else | `abort()` | — |

`RonSQLPermanentError` (`RonSQLCommon.hpp:392`) is a bare
`std::runtime_error`: a syntax error (`RonSQLPreparer.cpp:1104`), an unknown
table, an unsupported feature, an oversized LIMIT and a genuine internal
error ("Please report a bug") are indistinguishable to the client, to load
balancers and alerting (every client mistake looks like a server failure),
and to the framework, whose `exec.Classify` keys on the `Caught exception:`
text.  There are 103 permanent-error throw sites (`RonSQLPreparer.cpp` 80,
`QueryPlanner.cpp` 13, `ResultPrinter.cpp` 10) plus the `require_bug` /
"Please report a bug" internal paths.

### Design

1. **Classify at the source.**  Give `RonSQLPermanentError` an error class
   and an optional NDB error code:

   ```cpp
   enum class RonSQLErrorClass { SYNTAX, SEMANTIC, UNSUPPORTED, LIMIT, RESOURCE, INTERNAL };
   class RonSQLPermanentError : public std::runtime_error {
    public:
     RonSQLPermanentError(RonSQLErrorClass c, const std::string& msg, int ndb_error = 0);
     RonSQLErrorClass error_class() const; int ndb_error_code() const;
     // legacy one-argument constructor = INTERNAL, to be removed once every site is classified
   };
   ```

   Retrofit the throw sites in one mechanical pass (they are greppable by
   message):

   | class | sites (message prefixes) | HTTP |
   |---|---|---|
   | SYNTAX | the parser error path (`Syntax error.`, `Parser stack exceeded its maximum depth.`, `Empty input`) | 400 |
   | SEMANTIC | `Table not found.`, `Column not found.`, `Unknown table alias.`, `Unknown join column.`, `Ambiguous column.`, `Not unique table/alias.`, `Duplicate CTE name.`, `Join references unknown table.`, `Join column type mismatch.`, `Join key order mismatch.`, `Not an aggregate query.`, `Non-boolean term in WHERE condition`, `LIKE requires …`, `IS NULL … requires a column name`, `For comparison operators, at least one of the …`, `INTERVAL …`, `DATE_ADD or DATE_SUB failed`, `Failed converting float literal to DECIMAL` | 400 |
   | UNSUPPORTED | `RonSQL feature not implemented: …`, `… is not supported …` (`Non-aggregating CTE body …`, `Partial CTE lookup key …`, `CTE_SCAN as outer-join child …`, `INNER JOIN below LEFT JOIN …`, `ORDER BY / LIMIT in a subquery …`, `GREATEST/LEAST with = or != …`, `AVG over string/temporal columns …`, `Index hints …`, `Cross-table WHERE …`, `BLOB/TEXT columns cannot …`, `Unsupported column type … in pass-through result.`, `Cannot push join: no suitable index …`), `CTE without aggregate function.` | 400 (or 422 if a distinct code is wanted for "valid SQL we do not run") |
   | LIMIT | `Too many joined tables.`, `Too many join key columns.`, `Too many ORDER BY columns in CTE body.`, `CTE body LIMIT value too large.`, `Cross-table WHERE filter has too many OR/AND atoms.`, `Pushed join too large.` → 400; `Pass-through ORDER BY result too large.` and the `Internal.MaxRespSize` cap → 413 | 400 / 413 |
   | RESOURCE | `Out of memory while processing the query.`, `No NDB object` | 503 |
   | INTERNAL | `Please report a bug` (`Failed writing aggregation program`, `Got record with fewer aggregates than expected`, `Bug in RonSQLPreparer::encode_constant`, `require_bug`), `Failed to compile aggregation program.`, `Pass-through drain failed.`, `Join query execution failed.` (with the NDB code attached) | 500 |

   Kernel/NDB errors that reach the client as permanent (e.g. 1860
   overflow, 4120) are SEMANTIC-with-code when the statement caused them
   (1860) and INTERNAL otherwise; the NDB code goes in the body either way.

2. **Map in `ronsql_operation.cpp`.**  Replace the single
   `RS_SERVER_ERROR` for permanent errors with a switch on the class; keep
   429 for rate limiting; report an exhausted retryable error as **503**
   with the temporary-error message (it is a "try again later"; note the
   change for clients that retried on 500).  `catch (...)` stays an abort:
   it is the assertion that every path throws a classified error.

3. **Body.**  Keep the text form for compatibility, prefixed by the class:
   `[syntax] Caught exception: Syntax error.` is the minimal change; when
   the request asked for JSON output, emit a JSON error object instead of
   the partial `{"data":` stream — `{"error":{"class":"syntax","message":"…","ndbErrorCode":0}}`
   — and never a body that starts as data and ends as text.  RDRS's
   `ronsql_ctrl.cpp` writes `{"data":` before calling `ronsql_dal`; move
   that prefix after the status is known.

4. **Framework follow-up (same change set).**  `exec.Classify`
   (`tools/rondb-cli/internal/fsq/exec/exec.go`): 400 and 413 → `CleanReject`
   with the body as message; 503 → `Retryable`; 500 → `Error` unless the
   body carries `[internal]`/`Caught exception:` (keep the old rule during
   the transition); 429 unchanged.  The expectation tables match on message
   substrings and need no change; add the class to the `CASE` line message
   so a misclassified site shows up as `[internal]` in fuzz output.

### Tests

- rest-server2 C++ endpoint tests: one request per class (syntax → 400,
  unknown table → 400, unsupported feature → 400, `Pass-through ORDER BY
  result too large` → 413, exhausted retryable via an error insert → 503,
  an injected internal error → 500) asserting status and body shape;
  JSON-format error body is valid JSON.
- MTR: the `rdrs2-golang` RonSQL tests assert status codes; the `ronsql`
  suites through `ronsql_cli` are unaffected (no HTTP).
- Framework: `.fs_fuzz envelope --seed 1 --count 200` must still show the
  probes as `clean-reject`; a probe that now reports `[internal]` is a
  classification miss.

### Effort / risk

2–3 days; the risk is a client that keyed on 500 for RonSQL rejections
(Hopsworks' RDRS client treats non-200 as an error uniformly; confirm).

---

## M1.1 — F9: quote temporal MIN/MAX in JSON

**Status: DONE 2026-09-15 on RONDB-1124 — verified: `ronsql.ronsql_temporal_json`
recorded and reviewed (all twelve bodies parse, every temporal aggregate
quoted, values and NULL groups correct); `ronsql_fs` and `ronsql_fs_jit`
`ronsql_fs_fuzz_spec` + `ronsql_fs_fuzz_env` green on the new SUMMARY
lines; `go test ./internal/fsq/... ./internal/shell/...` green.  The
`ronsql_fs_ng2r2` / `ronsql_fs_ng4r2` fuzz results carry the same SUMMARY
lines (same seed; they were identical before) and are confirmed the next
time those mirrors run.**  Implementation notes: `print_aggregate_result`
passes `m_quote` (the only aggregate branch that printed a string-typed
value without it; BIGINT/DOUBLE need none, string MIN/MAX already quoted);
new `suite/ronsql` test `ronsql_temporal_json` with the include
`ronsql_json_check.inc` (ronsql_cli JSON, RDRS explicit and default
`outputFormat`, every body decoded by perl `JSON::PP` and printed
canonically) over DATE / YEAR / DATETIME(6) / TIMESTAMP(0,3,6) / TIME(3),
GROUP BY with NULL and all-NULL groups, and a pass-through control row.
Framework: `Known["F9"]`, `Expect.UnquotedTemporal`, `MatchesError`,
`Case.KnownError`, the envelope `temporal-minmax` row and `EnvCase.Known`,
the spec `Expect.KnownError` and the `KNOWN-ERROR` / `PASS(was-known-error)`
statuses are removed (the `temporal-minmax` construct tag stays in the
envelope signature); the eight fuzz `.result` SUMMARY lines lose their
`known-error=3`: the envelope cases become `pass`, and of the three spec
cases one becomes `pass` while two report `clean-reject` (a case reports
its worst template status, and those two also carry an F0 collect
template).

**Site.** `ResultPrinter::print_aggregate_value`
(`storage/ndb/src/ronsql/ResultPrinter.cpp:2401`): the D17 temporal
decode calls `print_temporal_packed(out, …, temporal_fsp, "")` with an
empty quote; the pass-through printer (`:1415`) passes `m_quote`.  Under
JSON output — the RDRS default, and what Hopsworks gets since it never
sets `outputFormat` — the value is printed bare (`"d_min":1970-01-01`)
and the body is unparsable.

**Change.** Pass `m_quote`; audit the other aggregate branches of the
same switch for the same omission (DECIMAL/DOUBLE/BIGINT need no quote;
string MIN/MAX must already quote).

**Tests.** Extend `suite/ronsql` with a JSON-format aggregate over DATE /
TIMESTAMP(3) / TIMESTAMP(6) (ronsql_cli `--format json` if available,
otherwise an rdrs2-golang test) asserting the body parses; framework:
`EDGE-date-range`, `EDGE-ts0-batch`, `EDGE-ts3-cutoff`, `EDGE-ts6-cutoff`,
`EDGE-float-exact` go from `KNOWN-ERROR` to `PASS(was-known-error)` →
remove `Known["F9"]` (`cases/known.go`), the `temporal-minmax` row's F9 in
`fuzz/envelope.go` and the `KnownError` marker in `fuzz/spec.go`; both
fuzzers report `known-error=0`.

**Effort.** Hours.

---

## M1.2 — F1: string feature aggregated twice

**Status: DONE 2026-09-15 on RONDB-1124 — verified: `ronsql_fs`,
`ronsql_fs_jit` (strict-armed, fallback pin still 0), `ronsql_fs_ng2r2`,
`ronsql_fs_ng4r2` templates + smoke + fuzz_spec recorded and reviewed
(both F1 templates cases: empty diff, `2 15 Beta` / `3 12 z`; the combined
smoke probe's only diff is F2's DECIMAL scale, as in EDGE-NULL-A2; the
seed-1 spec SUMMARY is unchanged with the rebuilt sampler because the
status classes are shape-driven); `go test ./internal/fsq/...
./internal/shell/...` incl. the templates golden test.**  Step 1's evidence exists without a separate
`.fs_verify` run: the kernel fix's own test `ronsql_string_agg_interleaved`
runs the two framework statements (its queries 1 and 6) on the same
`edge_hist_1` rows the loader writes, through RDRS and ronsql_cli, on
both arms, and is green.  Step 2 done: hazards cleared (asserted templates
cases, notes carry the fixing commit), the smoke `NEXT-PHASE F1` block is
enabled as probes EDGE-NULL-A5 / EDGE-NULL-A, `validAggregate` draws one
to three functions per string column (F8 rule kept; the seed-1 spec
distribution shifts), R-F1 title without "hazard".  Step 3 resolved as
(i) before this package: RONDB-1056 item 16 (2026-09-09) lowers
non-adjacent string consumers, so the strict-armed JIT mirror keeps its
fallback pin at 0 (see `suite/ronsql_jit/t/ronsql_string_agg_interleaved.test`).

**State.** Two faces were recorded on 2026-09-09/10 (`smoke.md` F1):
(a) RDRS/ronsql_cli abort in `NdbSqlUtil::cmpLongvarchar`
(`require(lb + m1 <= n1 && lb + m2 <= n2)`) while merging per-fragment
partials; (b) the data node crashing in `minMaxString` on the same
`require`.  On 2026-09-09 RONDB-1056 commit `10561b78d1e` fixed the kernel
cause — the string register was clobbered by a later numeric load because
`ProcessRec` rewound the attribute read position to 0 per opcode; a
per-row high-water mark (`m_attr_read_hwm`, `AggInterpreterBase.cpp:470`)
now keeps captured strings live — with its own MTR test
(`ronsql_string_agg_interleaved`, on the reporter's `edge_hist_1` rows).
The two framework cases have not been re-run since: they are flagged
`Hazard` and report `HAZARD-SKIPPED`.  The commit also notes that under
`CompiledInterpreter=ON` this shape is a bridge `TYPE_MISMATCH` reject
(the fused string-load lowering takes only consecutive consumers), so it
falls back to the interpreter; lowering non-adjacent string consumers is a
RONDB-1056 backlog item.

**Steps.**

1. Verify: `.fs_verify --case EDGE-F1-string-reuse --include-hazards` and
   `EDGE-F1-string-reuse-nonull` on the interpreter arm and on the JIT arm
   (`--start-and-exit` of `ronsql_fs_jit`).  Expected after
   `10561b78d1e`: `PASS`.  If face (a) still aborts, the partial merge is
   the remaining defect: `NdbAggregator::ProcessRes` hands
   `AGG_CHAR_RESULT` payloads to `aggMergeMin/Max` and the comparator gets
   a buffer length that does not cover the length prefix — pass the
   allocated payload size (`hdr[1]`) rather than the declared column size,
   or normalize partials to the kernel's `[len16][cap16][payload]` layout
   before comparing.
2. Clear `Hazard` on both cases (`cases/cases.go`), regenerate the
   templates test (they leave the commented-out block and become asserted
   cases; `.fs_emit_mtr … --cases`, re-record base / jit / ng2r2), lift the
   "one function per string column" avoidance in `fuzz/spec.go`
   (`validAggregate`; keep the F8 collation rule), R-F1 → SUPPORTED.
3. JIT: with `ronsql_fs_jit` strict-armed, the un-hazarded cases fail the
   query under `ERROR_INSERT 4064` unless the JIT lowers non-adjacent
   string consumers.  Decide: (i) lower them (RONDB-1056 backlog: extend
   the fused string-load lowering to non-consecutive consumers), which
   keeps the "every Hopsworks program compiles" property; or (ii) record
   the two cases non-strict in the JIT mirror only (note in the suite's
   `my.cnf`).  (i) is the right end state because `category:
   [count, min, max]` is a legal Hopsworks aggregate spec; (ii) is the
   interim.

**Tests.** The existing `ronsql_string_agg_interleaved` (+ JIT mirror) and
the two framework cases; the spec fuzzer at seed 1/2 after lifting the
avoidance (string columns then draw up to three functions).

**Effort.** 1 day if the kernel fix covers both faces; +2 days for the
merge if not; the JIT lowering is RONDB-1056 work (separate estimate).

---

## M1.3 — F0: the emitted collect CTE form

**Shape.** For every collect feature Hopsworks emits

```
WITH t AS (SELECT `pk…`, `f1`, `f2` FROM `fg_1`
           WHERE `entity_key` = ? [AND <online filters>]
           ORDER BY `event_time` DESC|ASC LIMIT n)
SELECT `pk…`, `f1`, `f2` FROM t;
```

(`emit.buildRonsqlCollectTemplate`; n ≤ 50, the order column is the last
PK column, the select list repeats the body's columns).  RonSQL rejects it
in `RonSQLPreparer::enforce_single_row_cte_body` (`RonSQLPreparer.cpp:5338`):
a non-aggregating CTE body is only allowed as a single-row PK lookup.
The body alone (S6b) is served by the single-table pass-through ORDER BY
path (`passthrough_orderby`, `RonSQLPreparer.cpp:3178`: an ordered index
scan with LIMIT, SF_OrderBy streaming) and is correct and faster than
MySQL's `ROW_NUMBER()` twin (`benchmarks.md` §8: 1.1–1.3×).

**Design: collapse the projection-only CTE into its body.**  In
`RonSQLPreparer::load()` before `build_cte_scopes()`, recognize the
*collect pattern* and rewrite the AST so the pass-through path runs on the
body; everything else keeps today's rule.

Pattern (all must hold):

- exactly one CTE, referenced exactly once, as the main query's only FROM
  source; the main query has no joins, WHERE, GROUP BY, HAVING, ORDER BY or
  LIMIT, and no aggregate functions;
- the main select list is a list of the CTE's output columns (a subset or
  permutation; aliases allowed);
- the body is single-table, non-aggregating, no GROUP BY, with `ORDER BY`
  and `LIMIT` (LIMIT > 0; the body may carry any WHERE — Hopsworks appends
  online filters as extra conjuncts, and the PK prefix bound is what makes
  the ordered scan cheap but is not required for correctness).

Rewrite: make the body the root statement, apply the main's projection
(column order / aliases) to the body's select list, and drop the CTE.
The result is exactly the S6b statement the framework already verifies.
Expose it in EXPLAIN as `CTE 't' collapsed into the pass-through ORDER BY
scan` so the plan pins can assert it.  A body that is neither an
aggregate, nor a single-row lookup, nor this pattern keeps the existing
message — but classified UNSUPPORTED (M1.0).

Not needed: a general materialized non-aggregating CTE (CTE_SCAN over an
ordered body) — no Hopsworks shape requires it; the batch collect has no
RonSQL template (MySQL-only, `buildDTO` emits it only when `!batch`).

**Edge cases to test.** ASC and DESC; LIMIT 1 and LIMIT 50 (the
`MaxCollectN`); a body WHERE with an extra filter (`category = 'grocery'`,
the S4-on-collect form the emitter produces for `collectFilters`); a
missing entity (0 rows); fewer rows than n; a select list that reorders or
aliases the columns; the main selecting a strict subset; ORDER BY on a
non-indexed column (falls to the buffered sort — still correct); JIT arm
(pass-through, no aggregation program: the delta pin stays 0).

**Tests.** `suite/ronsql`: a new `ronsql_cte_collect_collapse` test with
the cases above through `ronsql_compare.inc`, plus an EXPLAIN pin; the
CTE suite's `single_row_cte` family must keep rejecting the shapes that
are neither pattern (regression: the collapse must not swallow a
non-aggregating CTE with a join in the main).  Framework: `S6-cte-k21 /
k31 / k16` from `REJECT(expected)` to `PASS(was-expected-reject)`, spec
`V-S6-cte-n5` PASS, `.fs_fuzz spec` collect cases from `CLEAN-REJECT` to
`PASS` (seed 1: `clean-reject` drops by 27), retire `Known["S6-cte"]` and
the `cte-body-orderby-nonagg` row of `fuzz/envelope.go`; R-S6 SUPPORTED;
`.bench_ronsql fs_hw`: add `fs_hw_collect5_cte` / `collect50_cte` entries
(the emitted form) and pin them to the same plan and cost as the direct
form.

**Effort.** ~1 week (pattern matcher + AST rewrite + EXPLAIN + tests).

---

## M1.4 — F7: binary and complex projections through snowflake templates

**Shape.** Hopsworks projects `binary` features (embeddings) and
serialized `array` features through the snowflake templates:
`SELECT j2.label, j2.payload FROM b JOIN edge_child_1 AS j2 ON …` where
`payload` is `VARBINARY(100)` (the DDL port maps Hopsworks `binary` to
VARBINARY; large ones to BLOB).  `ResultPrinter` pass-through rejects it:
`Unsupported column type (17) in pass-through result.` (`:1438`, the
`default:` of the type switch that handles the integer, floating, DECIMAL,
CHAR/VARCHAR and temporal types).

**Design.**

1. Printer: add `Binary`, `Varbinary`, `Longvarbinary` to the pass-through
   switch.  TEXT output: the raw bytes, as the MySQL client prints them
   (byte parity for the MTR compare).  JSON output: a base64 string in
   quotes — the convention RDRS already uses for binary columns in pk-read
   responses (`encoding_helper.cpp:129`, libbase64), so the Hopsworks
   client decodes both endpoints identically.  `Blob`/`Text` stay rejected
   in this step (BLOB reading needs the blob API; scope it if Hopsworks
   maps large binaries to BLOB — check `ddl` for the threshold).
2. Aggregates over binary columns remain rejected (Hopsworks never emits
   them: the definition validator forbids min/max over complex types).
3. Document the JSON encoding in the RonSQL output contract and in
   `shape_catalog.md` S7/S8.

**Framework follow-up.** `canon` gains a binary kind (MySQL types
`VARBINARY`/`BINARY`/`BLOB`): the RonSQL JSON cell is base64-decoded
before the byte comparison; `.fs_verify --golden` vectors for
`snowflake_binary` compare bytes; the MTR templates case (TEXT) compares
raw bytes as today.

**Tests.** `suite/ronsql`: pass-through of a VARBINARY column (TEXT) and,
through rdrs2, the JSON base64 form incl. bytes that are not valid UTF-8
and the empty value; framework: `EDGE-comp-binary` from `REJECT(expected)`
to `PASS`, `Known["F7"]` retired, `snowflake_binary` golden vectors green,
R-A2-binary SUPPORTED.

**Effort.** ~1 week including the framework decoder and the client
contract.

---

## Order and dependencies

1. **M1.0** first, alone: it edits every throw site, so landing it before
   the feature work avoids conflicts, and it makes the framework's
   classification independent of message text framing.
2. **M1.1** (hours) immediately after.
3. **M1.2** verification run; the fix, if any, is small; the JIT decision
   is recorded either way.
4. **M1.3** and **M1.4** in parallel (disjoint files: `RonSQLPreparer.cpp`
   vs `ResultPrinter.cpp`).
5. Close M1 with the acceptance run on the three arms and the ×3 suites;
   retire every expectation-table entry that reports `PASS(was-…)`.

## Framework changes bundled with M1 (`tools/rondb-cli`)

| change | package |
|---|---|
| `exec.Classify`: 400/413 → CleanReject, 503 → Retryable | M1.0 |
| `CASE` message carries the error class | M1.0 |
| retire `Known["F9"]`, envelope `temporal-minmax` F9, spec `KnownError` | M1.1 |
| clear `Hazard` on `EDGE-F1-*`; lift the string-function limit in `fuzz/spec.go` | M1.2 |
| retire `Known["S6-cte"]` and `cte-body-orderby-nonagg`; bench entries for the CTE collect form | M1.3 |
| `canon` binary kind (base64 decode); retire `Known["F7"]` | M1.4 |
| regenerate `ronsql_fs_templates`, re-record base / jit / ng2r2 (+ ng4r2) | all |
