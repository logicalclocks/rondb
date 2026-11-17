# RonSQL executor review, slice A: dispatch, single-table paths, subqueries, transaction lifecycle and error handling

Baseline: 26.10-main, HEAD 50eb491eb3f. Read-only source review. Nothing was built or run.
Line numbers are for HEAD. Function names are the stable references.
Files: `storage/ndb/src/ronsql/RonSQLPreparer.cpp` (abbreviated "RSP"), `RonSQLCommon.hpp`,
`RonSQLPerf.hpp`, `RdrsSchemaCache.hpp`, `ronsql_cli.cpp`, the REST wrapper
`storage/ndb/rest-server2/server/src/db_operations/ronsql/ronsql_operation.cpp` (`ronsql_op`),
its callers `rdrs_dal.cpp` (`ronsql_dal`) and `ronsql_ctrl.cpp` (`RonSQLRequest::execute`,
ParseOnly authorization).

I skimmed `ronsql_parser_review_plan.md` (I1-I4, C1-C9, HAVING scalar NULL, IN-operand
rejection) and `claude_files/dbspj/README.md` (S-*, E-*, R-*, the earlier X-1/X-2 RonSQL
findings in that README). None of the findings below repeats them. Where a finding is
next to one of them, I say so.

## Summary table (most severe first)

| ID | Title | Category | Severity | Confidence |
|----|-------|----------|----------|------------|
| X-1 | A lexical error after an unaliased output longer than 64 characters aborts the process (`set_err_state`). In RDRS this happens before authorization | bug | crash | verified-by-reading (generated parser tables checked) |
| X-2 | The COUNT(\*) shortcut counts the hidden meta rows of ring-buffer tables | bug | wrong-result | likely |
| X-3 | A scalar subquery that returns NULL or no rows fails in the main WHERE (filter, index bound and PK key) instead of returning an empty result | bug | wrong-result (error instead of rows) | verified-by-reading |
| X-4 | Subquery results go through text. The constant type depends on the data, AVG always fails against an INT column, BIGINT UNSIGNED above 2^63 fails, and DECIMAL loses precision | design/bug | wrong-result | verified-by-reading (a, c, e), likely (d) |
| X-5 | The output of an inner IN or scalar subquery is buffered without a limit before the 1000-value or one-row check. Duplicate values count toward the 1000 limit | design/bug | hang-or-leak (memory), wrong error | verified-by-reading |
| X-6 | A temporary error in a streaming IN subquery becomes permanent. Rows written to the private buffer count as "output started" | bug | error-handling | verified-by-reading |
| X-7 | The retry policy depends on output already sent, but REST buffers the whole body. A temporary error in the middle of a result gives HTTP 500 `[internal]` | design | error-handling | verified-by-reading |
| X-8 | Unknown-result NDB errors (4008, 4012, 4009, 4035-4041) are permanent `[internal]` 500s and never retried | design | error-handling | verified-by-reading |
| X-9 | The `std::runtime_error` arm reads `Ndb::theError`, which can be stale on pooled Ndb objects. Dictionary errors are never consulted, so a temporary `getTable` failure becomes a 400 "Failed to get table" | design | error-handling | likely |
| X-10 | A stale `RdrsSchemaCache` entry after DROP INDEX makes one request fail permanently with "Failed to get index." | bug | error-handling | likely |
| X-11 | `throw_key_read_error` handles a per-operation SchemaError by reading the transaction's first error, possibly 626 | bug | error-handling | speculative |
| X-12 | An exception thrown inside `handle_ronsql_exception` escapes unclassified. `std::bad_alloc` from there reaches `catch(...) abort()` in both callers | design | crash (OOM only) | speculative |
| X-13 | `m_output_started` is set at only 2 of about 8 output sites. A stale-schema error during printing could be retried after partial output | design | error-handling | speculative trigger, verified invariant gap |
| X-14 | The state machine has no EXECUTED/FAILED state for `execute()`. `execute()` mutates the AST. Executor invariants use `ndbrequire`/`abort()`, which take down RDRS | design | maintainability (crash on bug) | verified-by-reading |
| X-15 | Subqueries run as full nested preparers: no schema cache, the inner error log is discarded, and the test hook is not propagated | design | performance / diagnosability | verified-by-reading |
| X-16 | PK-lookup paths start an unhinted transaction, unlike RDRS pk-read | design | performance | verified-by-reading (impact likely) |
| X-17 | Small error-reporting gaps: no class header for 429, no NDB code on 503, error body is the raw multi-attempt diagnostic log, and a TO error is retried 10 times | design | error-handling | verified-by-reading |
| X-18 .. X-27 | Duplication (see section 2) | duplication | maintainability | verified-by-reading |
| X-28 .. X-36 | Test gaps (see section 3) | test-gap | — | verified by grep |

---

## 1. Findings

### X-1 A lexical error after an unaliased output longer than 64 characters aborts the process
- Category: bug. Severity: crash. Confidence: verified-by-reading. I analyzed the bison tables of `debug_build/.../RonSQLParser.y.cpp`.
- Location: `RonSQLPreparer::Context::set_err_state` (RSP:20187, `ndbrequire` at RSP:20205), triggered from `RonSQLParser.y:397-409` (rule `output: nonaliased_output`).
- Evidence:
  ```
  // RSP set_err_state
  else {
    ndbrequire((m_err_pos < err_pos) ||
               (m_err_pos == err_pos && m_err_len <= err_len));
  }
  // RonSQLParser.y
  output: nonaliased_output
    { if ($1->output_name.len > 64) { context->set_err_state(
          RonSQLPreparer::ErrState::TOO_LONG_UNALIASED_OUTPUT, (@$).begin, ...); YYERROR; } ...}
  | nonaliased_output T_AS identifier
  ```
  The state after `nonaliased_output` (generated state 29) shifts `T_AS`, so it is not a consistent state. Bison therefore calls the lexer for the lookahead. When the lookahead is a lexical error, the lexer's `return_err(...)` first records an error at the lookahead position and returns `T_ERR`. `T_ERR` has no entry for state 29, so yacc.c takes `yydefault` and reduces rule 17. My script checked `yypact`, `yycheck` and `yydefact`: "state 29 defact rule 17 consistent False T_ERR action default". The action then records TOO_LONG_UNALIASED_OUTPUT at the beginning of the output, which is to the left of the stored error. The `ndbrequire` fails and calls `std::abort()` (see `ndb_require.h`).
- Trigger: `SELECT SUM(x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x+x) UNION SELECT 1;`. The output name `SUM(...)` is 68 characters. `UNION` is in `Keywords.hpp` and gives LEX_UNIMPLEMENTED_KEYWORD. An unterminated `'...` string, an identifier over 64 characters, or any other lexical error works the same way. In RDRS with Hopsworks API keys, `RonSQLRequest` builds the `ParseOnly` preparer before `authenticate()` (ronsql_ctrl.cpp:341-369). Any HTTP client that can reach the endpoint can therefore stop RDRS, together with every request in flight. `ronsql_cli` dumps core.
- Suggested fix: in `set_err_state`, replace the `ndbrequire` with "keep the leftmost error". Overwrite when `err_pos < m_err_pos`, or when the positions are equal and `err_len < m_err_len`. Never abort. Separately, for errors detected in grammar actions, consider `%define parse.lac full` or check the length before the lookahead is read.
- Suggested test: CLI rejection cases in `ronsql.ronsql_parsing` (no cluster needed, `--explain-mode=FORCE`). Use a >64-character unaliased aggregate followed by `UNION`, by an unterminated string, and by a 65-character identifier. Expect `--error 1` and a syntax/lexer message, not a signal. Add the same queries to the `ronsql_parse_only_test` inventory with the ParseOnly constructor.

### X-2 The COUNT(\*) shortcut counts the hidden meta rows of ring-buffer tables
- Category: bug. Severity: wrong-result. Confidence: likely. mysqld documents the meta-row behaviour. I did not check DBTUP myself.
- Location: `is_count_star_only_query` (RSP:9842), `execute_count_star_shortcut` (RSP:9872), dispatch at RSP:9652.
- Evidence: mysqld avoids the same statistic for ring-buffer tables:
  ```
  // plugin/ha_ndbcluster.cc:1244 (records())
  /* Ring buffer: stats.records is the physical row count, which includes
     the hidden meta rows (one per PK prefix). Use the base-class scan count ... */
  if (m_table != nullptr && m_table->isRingBuffer()) return handler::records(num_rows);
  // :14252  if (m_table->isRingBuffer()) f &= ~HA_COUNT_ROWS_INSTANT;
  ```
  The RonSQL comment says it uses "the same statistic mysqld's fast path reads (parity by construction)". `is_count_star_only_query` checks only the query shape, and `grep -i ringbuffer storage/ndb/src/ronsql` finds nothing.
- Trigger: `CREATE TABLE rb (id INT, ring_idx INT DEFAULT 0, ring_meta VARBINARY(64), data VARCHAR(100), PRIMARY KEY(id, ring_idx)) ENGINE=NDB COMMENT="NDB_TABLE=MAX_ROWS_PER_PK=3@ring_idx@ring_meta"`, then insert 3 rows for `id=1`. MySQL `SELECT COUNT(*) FROM rb` gives 3. RonSQL's shortcut gives 4: 3 rows plus one meta row per PK prefix. `SELECT COUNT(*), COUNT(data)` takes the scan path and is correct, so the two forms disagree.
- Suggested fix: add `if (m_main_scope.table->isRingBuffer()) return false;` to `is_count_star_only_query`. The EXPLAIN branch at RSP:19763 calls the same predicate and stays consistent. Also check the other uses of the ROW_COUNT pseudo-column in RonSQL, if there are any.
- Suggested test: in `ronsql`, `ronsql_count_star_ring_buffer.test` with the DDL above, two PK prefixes and wrapped rows. Compare `SELECT COUNT(*) FROM rb` and `SELECT COUNT(1), COUNT(0) FROM rb` against MySQL with `ronsql_compare.inc`. Use `ronsql_explain.inc` to check that it is not the fragment-stats plan.

### X-3 A scalar subquery that returns NULL or no rows fails in the main WHERE
- Category: bug. Severity: wrong-result (an error where MySQL returns 0 rows or COUNT 0). Confidence: verified-by-reading.
- Location: `execute_subqueries` (RSP:9152), `substitute_subquery_results_ce` I_SUBQUERY arm (RSP:9507-9530), `encode_constant` (RSP:16375; INT arm 16460, FLOAT 16483, DECIMAL 16536, final 16702). The values reach it through `apply_filter_cmp` (RSP:16190), the index bound in `open_single_table_scan_op` (RSP:10051) and the PK key in both PK arms (RSP:10764, 10783, 9778).
- Evidence:
  ```
  else if (result_str == "NULL" || result_str.empty()) { info.result.is_null = true; }   // 9152
  if (result.is_null) { ce->op = T_NULL; }                                              // 9517
  // encode_constant: T_NULL is handled by no arm
  if (tk == INT) { throw RonSQLMaybeStaleSchema("Integer type column compared to an"   // 16461
                                                " incompatible value. ...");
  ```
  Planning treats I_SUBQUERY placeholders as eligible bound and PK constants (RSP:5360, 4222, and `detect_pk_lookup` takes any non-identifier side). After substitution, every WHERE emitter calls `encode_constant(T_NULL, col)`. INT, FLOAT and DECIMAL columns raise RMS. `handle_ronsql_exception` then calls `unload_schema()`, which invalidates the table, its indexes and the RDRS schema-cache entry, finds no change, and fails permanently: SEMANTIC/400 via "compared to". CHAR and temporal columns end in "Bug in RonSQLPreparer::encode_constant" (INTERNAL/500). The parser review fixed only the HAVING case (Tests 55-60).
- Trigger: with the `ronsql_subquery` fixtures, `SELECT COUNT(*) FROM nq_outer WHERE o_k > (SELECT MAX(e_val) FROM empty_table);`. MySQL returns 0. RonSQL fails. The PK form (`WHERE o_id = (SELECT MAX(e_val) FROM empty_table)`, a pass-through PK lookup) and the indexed-column form fail the same way. Each failure also costs the following queries a dictionary refetch.
- Suggested fix: after `substitute_subquery_results()`, fold comparisons with a T_NULL operand to UNKNOWN. Simplest: rewrite `x OP NULL` to the existing always-false shape (`x != x`), keeping the polarity rules already used for IN. Under NOT, UNKNOWN must stay "drop row", so use `x IS NULL AND x IS NOT NULL`, as `always_false` does at RSP:9466. Alternatively, have `encode_constant` callers short-circuit T_NULL. That is harder for bounds: a NULL bound on an index column must become an empty range, not `BoundEQ NULL`.
- Suggested test: `ronsql_subquery` strict MySQL/CLI/RDRS comparisons over an empty inner table and an all-NULL inner value: (1) filter arm on a non-indexed INT column, (2) indexed column (bound arm, check the plan with EXPLAIN), (3) PK equality in a projection-only query (PK-lookup arm), (4) PK equality in `COUNT(*)` (index bound), (5) CHAR column (`c = (SELECT MAX(s) ...)`, for the INTERNAL path), (6) under NOT.

### X-4 Subquery results go through text
- Category: design/bug. Severity: wrong-result (an error for valid SQL, and silently wrong comparisons). Confidence: (a), (c) and (e) verified-by-reading, (d) likely.
- Location: `execute_subqueries` (RSP:9014-9205) prints the inner result as TEXT_NOHEADER and parses it with `strtoll`/`strtod`. Also `parse_subquery_value` (RSP:6563) and `encode_constant` (RSP:16375).
- Evidence: the substituted constant's type comes from the printed digits, not from the inner result type:
  ```
  long long ll = strtoll(...); if (endptr != NULL && *endptr == '\0' && errno == 0) -> T_INT
  else { double dbl = strtod(...); ... -> T_FLOAT }
  ```
  - (a) `AVG(int_col)` prints with scale 4 (`avg_arg_scale` returns 4, ResultPrinter.cpp:2663), for example `2.5000` or `3.0000`. It is always T_FLOAT, and `encode_constant(T_FLOAT, INT column)` throws RMS ("Integer type column compared to an incompatible value"). `WHERE int_col > (SELECT AVG(int_col) FROM t)` therefore always fails, and also invalidates the schema cache. MySQL runs it. No test uses `(SELECT AVG`.
  - (b) `MAX(double_col)` prints `3` for 3.0 (`my_fcvt_compact` drops trailing zeros) and `3.5` otherwise. `int_col > (SELECT MAX(d) ...)` works or fails depending on the data.
  - (c) A BIGINT UNSIGNED value above INT64_MAX makes `strtoll` fail with ERANGE. It becomes T_FLOAT 1.8446744073709552e19 and then fails against the column (RMS). `WHERE u = (SELECT MAX(u) FROM t)` errors where MySQL returns the row.
  - (d) DECIMAL with more than about 15 significant digits: `strtod` rounds, and `encode_constant` re-renders with `%.*f` at the column scale (RSP:16525). The comparison constant differs from the stored value, so the result is silently wrong. The cause is independent of the kernel's DECIMAL-as-DOUBLE issue (Q-6/E-16): the text round trip loses the digits even if the kernel aggregation were exact.
  - (e) An empty string as a scalar result is taken as NULL (RSP:9152). In an IN result, blank lines are skipped (`next_subquery_output_line`). A CHAR/VARCHAR value `'NULL'` is taken as SQL NULL. Temporal and string results are rejected as "non-numeric" (SEMANTIC). The CTE test only mentions `o_date >= (SELECT MIN(o_date) ...)` as a comment.
- Suggested fix: run the inner preparer with a typed result sink, a `RonSQLExecParams` callback that receives `NdbAggregator::Result` / `NdbRecAttr` per value, instead of a text stream. Build `ConditionalExpression` constants with the inner column's type: T_INT, T_FLOAT, T_STRING, I_MYSQL_TIME, or a DECIMAL kept as text in `constant_float.ls`. Until then, at least: (1) let `encode_constant` accept T_FLOAT against integer columns for subquery-substituted constants (`ls.str == NULL`) with exact SQL comparison semantics, for example `int_col > 2.5` becomes `int_col >= 3`; (2) keep the decimal text in `ls` so DECIMAL columns use the exact digits.
- Suggested test: `ronsql_subquery` strict comparisons: `WHERE int_col > (SELECT AVG(int_col) ...)`, `WHERE int_col > (SELECT MAX(dbl) ...)` with integral and fractional maxima, a BIGINT UNSIGNED value of 18446744073709551615, and `DECIMAL(30,2)` equality against `(SELECT MAX(d) ...)` with 20+ significant digits.

### X-5 Inner subquery output is buffered without a limit
- Category: design/bug. Severity: hang-or-leak (RDRS memory), plus a wrong error. Confidence: verified-by-reading.
- Location: `execute_subqueries` (RSP:9036 `std::ostringstream oss;`, 9137, 9091, 9145).
- Evidence: the inner preparer writes its whole result into a heap `std::ostringstream` (not the arena, and not bounded by `Internal.MaxRespSize`). The limits are checked only after full execution and parsing:
  ```
  if (info.in_values->size() > 1000) throw RonSQLPermanentError("IN-subquery returned more than 1000 values.");
  else if (result_str.find('\n') != std::string::npos) throw ... "A scalar subquery returned more than one row."
  ```
  `reject_ignored_orderby_limit` forbids LIMIT inside subqueries, so the user cannot bound it either. Explicit IN subqueries are not deduplicated: EXISTS gets `GROUP BY` from `decorrelate_exists`, but `x IN (SELECT b FROM t2)` does not.
- Trigger: `SELECT COUNT(*) FROM t WHERE a IN (SELECT b FROM big)` with a 100M-row `big` streams the whole table into `oss` (hundreds of MB to GB in one RDRS worker) and then fails. With 1200 inner rows but 3 distinct values, it fails with "more than 1000 values" where MySQL succeeds. A non-aggregate scalar `(SELECT b FROM big)` also buffers the whole table before reporting "more than one row".
- Suggested fix: add a row cap to `RonSQLExecParams` (`max_result_rows`) that the pass-through drains enforce: 1001 for IN, 2 for scalar, and 1001 for correlated scalar. Stop the scan early, as LIMIT does (`closeNoWait`). Deduplicate IN values before applying the cap, or rewrite explicit IN inners to `SELECT b ... GROUP BY b` the way EXISTS is rewritten. That also shrinks the OR chain.
- Suggested test: `ronsql_subquery`: an IN subquery over 1200 rows with 3 distinct values (expect MySQL parity), and an IN subquery over 1100 distinct values (expect the limit error). For RDRS, measure peak RSS with a large inner table: `ronsql_large` suite, or a Go test checking that the request fails quickly.

### X-6 A temporary error in a streaming IN subquery becomes permanent
- Category: bug. Severity: error-handling. Confidence: verified-by-reading.
- Location: `execute_subqueries` (RSP:9040-9058, catch at 9178); `execute_single_table_passthrough` sets `m_output_started` at RSP:11019; `handle_ronsql_exception` RSP:15736.
- Evidence: the inner preparer gets a private `oss` that is discarded on failure. Its pass-through drain still sets `m_output_started = true` after the first row, and its error handling then turns a temporary error permanent:
  ```
  if (temporary && m_output_started) { err << ",od"; temporary = false; }   // 15736
  ```
  The outer query rethrows it as-is (`catch (const RonSQLPermanentError&) { throw; }`, RSP:9178). The outer query has produced no output at this point, because subqueries run before any outer transaction, so a retry would be safe.
- Trigger: `SELECT COUNT(*) FROM t WHERE a IN (SELECT b FROM t2 WHERE c > 5)` while a data node restarts during the inner scan. The inner streams some rows, `nextResult` returns 4028, 4010, 1204, 499, 274 or similar, and the whole statement fails permanently. The NDB classification is NR/TO/TR, which falls into `default` and becomes INTERNAL, so REST answers 500 `[internal]` instead of retrying. EXISTS is not affected: its rewritten inner is an aggregate.
- Suggested fix: add `bool output_discardable` to `RonSQLExecParams` and set it for the inner preparer. `handle_ronsql_exception` and `throw_classified_ndb_error` then skip the ",od" downgrade when it is set. See also X-7 for REST.
- Suggested test: with error injection on a data node (a DBLQH scan error insert that returns a temporary error after the first batch), run an IN subquery whose inner returns several batches. Expect transparent retry: same result as MySQL, `attempts > 1` in `x-ronsql-phases`.

### X-7 The retry policy depends on output already sent, but REST buffers the whole body
- Category: design. Severity: error-handling. Confidence: verified-by-reading.
- Location: `handle_ronsql_exception` RSP:15736; `throw_classified_ndb_error` RSP:15589-15596; `ronsql_op` (ronsql_operation.cpp:58-165); `RonSQLRequest::execute` (ronsql_ctrl.cpp:449-660).
- Evidence: REST writes into a `CappedOStream` and sends nothing until `ronsql_dal` returns (`std::string out_str = out_stream.take(); ... resp->setBody(std::move(out_str));`). On a non-200 status, the body is `err_str` and `out_str` is dropped. `ronsql_op` reuses the same `out_stream` across attempts without rewinding it, which is the only reason the ",od" rule exists in REST. The fallback class is also wrong: a TemporaryError after output falls through to the switch, and NR/TR/TO/OL/NS/IT end up as `INTERNAL`, so the reply is 500 `[internal]`.
- Trigger: a pass-through query over a large table (`SELECT a, b FROM big`) loses a data node in the middle of the drain. REST returns 500 `[internal]` although nothing reached the client and a retry would succeed.
- Suggested fix: in `ronsql_op`, record `out_stream` size before each attempt and truncate back to it on retry. `CappedOStream` already exposes `size()`; add `truncate(size_t)`. Pass `output_discardable = true` (see X-6) so the executor keeps temporary errors retryable. Only `ronsql_cli`, which writes to stdout, needs the ",od" rule. In both classification switches, map temporary statuses to RESOURCE (503) when they become permanent.
- Suggested test: a Go integration test with a data-node error insert during a multi-batch pass-through. Expect 200 with the full result, and no duplicated rows or `[` in the body.

### X-8 Unknown-result NDB errors are permanent `[internal]` 500s
- Category: design. Severity: error-handling. Confidence: verified-by-reading.
- Location: `handle_ronsql_exception` RSP:15730-15776; `throw_classified_ndb_error` RSP:15587-15615.
- Evidence: only `status == TemporaryError` or `mysql_code == HA_ERR_LOCK_WAIT_TIMEOUT` is retried. `ndberror.cpp` maps UR to `ST_U`: 4008 "Receive from NDB failed", 4012 "Request ndbd time-out", 4009 "No data node(s) available", and 4035-4041 for data nodes starting, shutting down or in single-user mode. These are neither retried nor matched by the class switch, so they become INTERNAL (500). For a read-only statement, retrying an unknown result is safe.
- Trigger: a RonSQL request during a cluster restart. `startTransaction()` fails with 4009 or 4035, and RDRS answers 500 `[internal]` instead of 503 or a transparent retry. A data-node stall that produces 4012/4008 gives the same.
- Suggested fix: treat `NdbError::UnknownResultError` (and `HA_ERR_NO_CONNECTION`) as retryable for RonSQL, which is always read-only. At minimum, classify them as RESOURCE.
- Suggested test: Go or MTR test that stops all data nodes (or uses single-user mode, giving 4041) and runs a RonSQL query. Expect 503 `[resource]`. After restart, expect success.

### X-9 The `std::runtime_error` arm can read a stale `Ndb::theError`, and dictionary errors are ignored
- Category: design. Severity: error-handling (wrong class, wrong `X-RonSQL-NDB-Error`, spurious retries possible). Confidence: likely.
- Location: `handle_ronsql_exception` RSP:15681-15690; producers RSP:10640 and 9816; prepare-time `require_run` sites (RSP:3583, 4144, 4579, 4608, 4662, 6025, 6043, 6141, 6159); `load_single_table` RSP:1929-1932.
- Evidence:
  ```
  if (m_trans != NULL) { ndb_err = m_trans->getNdbError(); }
  else if (ndb != NULL) { ndb_err = DBG(ndb->getNdbError()); }      // 15688
  ```
  `Ndb::theError` persists until the next `startTransaction()`. RonSQL itself sets it through `Ndb::getNdbError(int code)`, which assigns `theError.code = code` (Ndberr.cpp:34), at RSP:10640 and RSP:9816 with codes 1860-1866. A failed `startTransaction()` also sets it. RDRS returns Ndb objects to a pool (`ronsql_dal`). The next request on that object that fails in a prepare-time `require_run` (before its own `startTransaction`) is then classified from the previous request's error. Separately, dictionary failures are never consulted. `getTable()` returning NULL is a `require_prm` with "Failed to get table. Note that RonSQL only supports tables with ENGINE=NDB.", so a temporary dictionary error (4009, 4012, 701) gives a permanent 400 `[semantic]` with a misleading message.
- Trigger: request A fails with 1860 (SUM overflow in a J5 or IN-list key-read aggregation), leaving 1860 in the pooled `Ndb::theError`. Request B on the same Ndb object hits an internal invariant during prepare. It is reported `[semantic]` 400 with `X-RonSQL-NDB-Error: 1860` instead of `[internal]` 500. If the stale error is temporary, B is retried 10 times and ends with 503.
- Suggested fix: replace bare `std::runtime_error` (`require_run`) with an exception that says where the NDB error is: `{TRANS, OP(ptr), QUERY(ptr), NDB, DICT, NONE}`. `handle_ronsql_exception` then reads exactly that source. Internal invariants use NONE and become INTERNAL. In `load_single_table` (and the join loader), read `m_dict->getNdbError()` when `getTable` returns NULL, and treat 723 "No such table" as SEMANTIC, temporary statuses as retryable, and the rest as INTERNAL.
- Suggested test: a C++ test in `ronsql_oom_test.cpp` style. Call `ndb.getNdbError(1860)`, then construct a preparer whose prepare hits a forced `require_run`. Expect INTERNAL and NDB code 0. For the dictionary part, use a Go test with data nodes in single-user mode and a cold dictionary cache.

### X-10 A stale index list after DROP INDEX makes one request fail permanently
- Category: bug. Severity: error-handling. Confidence: likely.
- Location: `load_single_table` (RSP:1937-2003), `unload_schema` single-table branch (RSP:15994-16098).
- Evidence: the `RdrsSchemaCache` entry stays valid while `tableId` and `schemaVersion` are unchanged and the TTL has not expired. Per its own header comment, index DDL does not bump the base table version. A dropped index name from the cache makes `getIndex()` return NULL. `err_failed_to_get_index` is set and the index is not pushed to `m_indexes`, and RMS "Failed to get index." is thrown. `unload_schema` compares `m_indexes`, which lacks the dropped index, with a fresh `listIndexes()`, which also lacks it. The counts and versions match, so it returns false and the error is permanent (400 `[semantic]` by "Failed to get index"). The schema cache is invalidated on the way out, so only that request fails.
- Trigger: RDRS with `schema_cache`. Run a query on `t` (which fills the cache), `DROP INDEX i ON t`, then run any query on `t` whose Ndb object no longer caches `i` locally.
- Suggested fix: record `m_load_saw_stale_index = true` when `getIndex` fails or returns a non-Retrieved object while the cache supplied the list, and make `unload_schema` return true in that case. Alternatively, bypass the cache and use `listIndexes()` on any RMS retry.
- Suggested test: an RDRS test (Go or MTR with RDRS) that queries, drops an index through mysqld, and queries again. Expect 200 on the second query, possibly after an internal retry.

### X-11 `throw_key_read_error` reads the transaction's first error for a per-operation SchemaError
- Category: bug. Severity: error-handling. Confidence: speculative. It needs operations in one batch to end differently.
- Location: `throw_key_read_error` (RSP:15566-15572), used from both PK arms.
- Evidence:
  ```
  if (op_err.classification == NdbError::SchemaError) { throw std::runtime_error(what); }
  ```
  The handler then classifies `m_trans->getNdbError()`. With AO_IgnoreError reads, that is the first error of the batch. These per-operation walks run only when `execute()` returned 0 or the transaction error was NoDataFound. If key 1 misses (626) and key 2 gets 241 or 284 (concurrent DDL), the request fails permanently as SEMANTIC with code 626, and the schema is never reloaded.
- Suggested fix: `throw RonSQLMaybeStaleSchema(what)`, as the join drains already do at RSP:11872 and RSP:12284.
- Suggested test: hard to trigger in MTR. Cover it with a unit test of the dispatch helper if X-18 below is adopted.

### X-12 Exceptions thrown inside `handle_ronsql_exception` escape unclassified
- Category: design. Severity: crash (OOM only). Confidence: speculative.
- Location: `handle_ronsql_exception` RMS arm (RSP:15647-15660) and SRE arm (RSP:15723); `unload_schema` (RSP:15784; `std::vector`/`std::string` pushes, `m_amalloc->alloc_exc`, `require_prm` at 16027/16045/16062-16074); `ronsql_op` `catch (...) { abort(); }`; `run_ronsql` `catch (...) { abort(); }`.
- Evidence: `unload_schema()` runs inside a catch handler. A `std::bad_alloc` thrown there leaves the handler and reaches the caller's `catch(...)`, which aborts RDRS or the CLI. A `RonSQLPermanentError` from `require_prm(dict->listIndexes(...) == 0, ...)` escapes without the cleanup and class decisions of the original error. For example, a temporary dictionary failure during reload becomes a permanent "Failed to list indexes while reloading schema.".
- Suggested fix: wrap the `unload_schema()` call in `try { ... } catch (...) { schema_changed = false; }` (or treat it as retryable), and make `ronsql_op`/`run_ronsql` map unknown exceptions to INTERNAL instead of `abort()`.
- Suggested test: extend the I4d fault-injection hook with an `UNLOAD_SCHEMA` site and expect RESOURCE/503 and no process abort.

### X-13 `m_output_started` is maintained at only two output sites
- Category: design. Severity: error-handling. Confidence: the invariant gap is verified, the trigger is speculative.
- Location: `m_output_started` is set only at RSP:11019 (single-table pass-through) and RSP:12247 (join pass-through). Other output writers: `print_result` (RSP:9697, 10440, 11890), `print_count_star_result` (RSP:9907), the PK arm (RSP:10850-10885), and the sorted drains (`sort_and_print_passthrough_rows`).
- Evidence: `ResultPrinter` throws `RonSQLMaybeStaleSchema` while printing (ResultPrinter.cpp:147 `require_sch`, used at 833, 852, 1013, 1458-1479, 2878). The RMS arm retries when `schema_changed && !m_output_started`. On these paths the flag is false although header and rows may already be in `out_stream`, so a REST retry would append a second copy to a 200 body. This is reachable only if a charset lookup fails after something was printed together with a concurrent schema change. It is unlikely today, but every new printer error or output site has to remember the flag.
- Suggested fix: track output at the stream. Record `out_stream->tellp()` or `CappedOStream::size()` at the start of `execute()` and compute "output started" when the error happens. Or route all output through one `ResultPrinter` method that sets the flag. With X-7's truncation, REST would not need the flag at all.
- Suggested test: none practical. Covered by the structural change.

### X-14 The state machine and the abort-on-invariant policy
- Category: design. Severity: maintainability. A bug elsewhere becomes an RDRS crash. Confidence: verified-by-reading.
- Location: `Status` (RonSQLPreparer.hpp:261-268), constructors (RSP:262-385), `execute` (RSP:9580-9705), `~RonSQLPreparer` (RSP:20180).
- Evidence:
  - The states are BEGIN, PARSED, PREPARED and FAILED. A constructor that sets FAILED always rethrows, so no FAILED object exists, and `require_prm(m_status != Status::FAILED, ...)` in `execute()` is dead code.
  - `execute()` never changes `m_status`, but it is not re-runnable. `substitute_subquery_results` overwrites AST nodes in place (`*ce = *result`, RSP:9372 and 9499; `ce->op = T_INT`, 9527), which are also referenced from `m_toplevel_conditions`, `m_pk_lookup_const` and the scan-config bounds. `m_output_started` and the phase stats also persist. Today every caller builds a new preparer per attempt, so this is latent.
  - Misuse (`execute()` on a ParseOnly preparer) and every executor invariant use `ndbrequire`, which is `std::abort()`. There are about 108 `ndbrequire` and 26 `abort()` in RSP and 9+18 in ResultPrinter.cpp. In scope: RSP:9376, 9503 and 9534 (`ndbrequire(false)` when a SubqueryInfo is missing), 9650, 9719, 10393, 10408, 10429, 10629, 11092 and 11115. In a multi-tenant RDRS, one bad statement shape takes down every in-flight request.
  - The destructor does not call `cleanup_trans()`. That is correct today, because every `execute()` exit closes, but nothing enforces it (see X-27).
- Suggested fix: add `EXECUTING`/`DONE` states. Make `execute()` single-shot with a `require_bug` (INTERNAL) on re-entry, and set FAILED on an exception. Put `cleanup_trans()` in the destructor as a backstop. Over time, convert executor-side `ndbrequire` that inputs can reach into `require_bug` (a permanent INTERNAL error). Keep `ndbrequire` only where memory corruption would follow.
- Suggested test: a C++ unit test that `execute()` twice on the same preparer gives an INTERNAL error, not an abort.

### X-15 Subqueries run as full nested preparers
- Category: design. Severity: performance and diagnosability. Confidence: verified-by-reading.
- Location: `execute_subqueries` (RSP:9040-9058, 9195-9204).
- Evidence: `inner_params` does not set `schema_cache`, so every subquery of every request calls `dict->listIndexes()`, which `RdrsSchemaCache.hpp` puts at about 400 µs. It also leaves out `phase_stats` (the inner phases are invisible), `allocation_test_hook` (I4d cannot inject into inner preparation) and `avro_decoder`. The inner `err_stream` (`ess`) is discarded, so the NDB error text of a failing subquery never reaches the RDRS body or log. Only `e.what()` survives, for example "Failed to execute transaction (single-table pass-through).". Each subquery also re-lexes, re-parses and re-loads its SQL text and runs in its own transaction. That is acceptable for read-committed semantics, but costs one round of planning per subquery.
- Suggested fix: pass `schema_cache` and `allocation_test_hook` through. Append `ess.str()` to the outer `err_stream` on failure. Longer term, prepare inner statements once in the outer constructor (they are already parsed as `SelectStatement`s) and execute them with a typed sink (X-4).
- Suggested test: `ronsql_phase_stats` with a subquery query. Check that the subquery phase has no `listIndexes` cost after warm-up, for example through a schema-cache hit counter if one is added.

### X-16 PK-lookup paths start an unhinted transaction
- Category: design. Severity: performance. Confidence: verified-by-reading. The latency impact is likely but not measured.
- Location: `start_transaction` (RSP:11093), used by the pass-through PK arm (RSP:10611) and `execute_pk_lookup_aggregate`.
- Evidence: `m_trans = DBG(ndb->startTransaction());` passes no table or key hint. RDRS pk-read uses `startTransaction(record, row, buf, ...)` (pkr_operation.cpp:441, 462) so the TC runs on the node that holds the primary replica. A single-key RonSQL lookup (`WHERE pk = 5`) usually pays one extra hop.
- Suggested fix: for `n_ops == 1`, start the transaction with the key (`startTransaction(table, keyData, keyLen)`, or the NdbRecord variant when the key row has been built).
- Suggested test: performance only. `ronsql_fs` or benchmark comparison of the single-key pass-through.

### X-17 Small error-reporting gaps
- Category: design. Severity: error-handling. Confidence: verified-by-reading.
- `ronsql_op`'s RonSQLRateLimitError arm does not set `params.error_class`, so 429 has no `X-RonSQL-Error-Class`. RonSQLRetryableError carries no NDB code, so a 503 after exhausted retries has no `X-RonSQL-NDB-Error`.
- The non-200 body is the whole `err_stream`: the "Error handling: SRE,te,nu->RRE" trace plus the full NdbError text of every attempt (up to 10). It is useful for operators, but it is internal detail in a client-facing body, and it grows with the retries.
- Timeouts (TO: 266, 274, 237) are retried 10 times with backoff. For a heavy scan that timed out, that multiplies the load by 10. Consider a smaller budget for TO than for NR/TR.
- `ronsql_cli`, after 3 retryable failures, prints "caught retryable error after 3 attempts" without `e.what()`.

---

## 2. Duplication (X-18 .. X-27)

For each: sites, differences, proposed helper, rough lines saved and risk. Where copies have diverged, the divergence is a potential bug.

### X-18 NdbError to exception classification (two copies, diverged)
- Sites: `throw_classified_ndb_error` (RSP:15575-15616) and the `std::runtime_error` arm of `handle_ronsql_exception` (RSP:15671-15778). The RMS arm (RSP:15647-15660) and the join drains (RSP:11871-11883, 12279-12293) repeat the "SchemaError means RMS, else classify" decision.
- Differences: the handler maps `SchemaError` to SEMANTIC, while `throw_classified_ndb_error` has no SchemaError case (INTERNAL). The handler renders the error before `cleanup_trans()`. The RMS message classification uses message text, the SRE arm uses the NDB classification. The ",od" and temporary rules are duplicated literally.
- Helper: `[[noreturn]] void raise_ndb_error(const NdbError& e, const char* what, const char* where, SchemaPolicy p /*RELOAD or PERMANENT*/)`. It covers the rate limit, schema reload, temporary/od decision, class mapping and rendering. `handle_ronsql_exception` calls it with the error source from X-9.
- Lines saved: about 40. Risk: low to medium. The diagnostic strings on `err_stream` (",te", ",nu", ",od") are checked by tests ("NDB Permanent error 1860,"). Keep them.

### X-19 Key-read scaffolding (aggregate PK lookup vs pass-through PK arm)
- Sites: `execute_pk_lookup_aggregate` (RSP:9717-9827) and `execute_single_table_passthrough` PK arm (RSP:10668-10885).
- Shared: the default-record scaffolding (`getDefaultRecord`, `getRecordRowLength`, `result_row`, `zero_mask`: 9725-9736 vs 10705-10728); the residual filter build (9738-9756 vs 10729-10748, identical; a third, non-throwing copy is the detect-time trial at 5170-5186); the `key_const` array (9765-9767 vs 10703-10704); NdbRecord key-row encoding (9771-9782 vs 10776-10791); `execute(Commit)` with NoDataFound tolerance (9790-9796 vs 10818-10826, identical); and the per-operation error walk (9800-9808 vs the pre-walk at 10838-10850 and the print walk at 10855-10870).
- Helpers: `NdbInterpretedCode* build_lookup_residual(NdbInterpretedCode& code)`; `const char* encode_key_row(const NdbRecord*, Uint32 rowlen, ConditionalExpression* const* key_const)`; `void execute_key_reads(const char* what)`; `Uint32 check_key_read_errors(const NdbOperation* const* ops, Uint32 n, Int64 limit)`, which returns the number of present rows.
- Lines saved: about 70. Risk: low. Both arms have EXPLAIN and `ronsql_in_list_*` coverage.

### X-20 Scan-operation creation (three copies, diverged)
- Sites: `open_single_table_scan_op` table-scan arm (RSP:9928-9933), its index-scan arm (RSP:9953-9990), and `execute_count_star_shortcut` (RSP:9875-9880).
- Divergence: a NULL from `getNdbScanOperation` is `require_sch` (RMS: schema reload, then permanent), but a NULL from `getNdbIndexScanOperation` is `require_run` (NDB investigation). A failed `readTuples` is `require_prm` (never investigated, never retried, class INTERNAL by message) in the table-scan and count-star arms, but `require_run` in the index arm. The same NDB condition (out of operation records 4000, or a scan resource error) is therefore classified differently depending on whether an index was chosen.
- Helper: `NdbScanOperation* new_scan_op(const NdbDictionary::Table*, const NdbDictionary::Index*, Uint32 scan_flags, Uint32 batch_rows)` using `require_run` consistently.
- Lines saved: about 15. Risk: low.

### X-21 Pass-through drains (single-table vs join)
- Sites: `execute_single_table_passthrough` scan arm (RSP:10893-11087) and `execute_passthrough_drain` (RSP:11920-12320).
- Shared: resolving the ORDER BY slot or adding an extra `getValue` (10931-10975 vs about 12080-12142), `PassthroughSortBuffer` handling, the LIMIT loop, deferred TSV header and JSON `[` with `m_output_started`, empty-result JSON framing, phase-stat bookkeeping (identical `drain_us` formula at 11030-11035 and 12268-12273), and sort-then-print.
- Divergence: (1) error classification: the single-table arm uses `require_run(rc == 1, ...)` and reads the transaction error, while the join arm reads `query->getNdbError()` with an explicit RMS for SchemaError. (2) Early close: `closeNoWait()` vs a blocking `query->close()` in the caller. (3) Only the single-table arm limits the batch size by LIMIT (`batch_rows`, 10908-10912). (4) The single-table arm also serves J5 (`api_rows`).
- Helper: a `PassthroughSink` class (`bool accept(const NdbRecAttr* const*, NdbQueryOperation* const*)` returning false at LIMIT, `void finish(const NdbRecAttr* const*)`) that owns the header, row, finish, sort-buffer and LIMIT state and sets `m_output_started`.
- Lines saved: about 100. Risk: medium. Output format is sensitive; the many `ronsql_compare` tests cover it.

### X-22 Subquery value parsing (three copies)
- Sites: `parse_subquery_value` (RSP:6563-6591), the IN loop (RSP:9103-9133) and the scalar branch (RSP:9157-9176).
- Differences: only the error text differs. The scalar branch also maps an empty result to NULL, while the IN loop skips blank lines.
- Helper: `bool try_parse_subquery_value(const char*, SubqueryResult&)`. Callers throw their own message.
- Lines saved: about 45. Risk: low. Superseded by X-4's typed sink if that is done.

### X-23 Constant and node construction in substitution
- Sites: the `node` lambda defined twice (RSP:9240 and RSP:9446); the T_INT/T_FLOAT constant built at 9259-9269, 9288-9298, 9398-9409 and in place at 9519-9528; the `T_IS` node at 9333 and 9455 (`is_null` lambda).
- Helper: members `ConditionalExpression* make_node(TokenKind, ConditionalExpression*, ConditionalExpression*)`, `ConditionalExpression* make_constant(const SubqueryResult&)`, `ConditionalExpression* make_is_null(ConditionalExpression*, bool)`.
- Lines saved: about 50. Risk: low.

### X-24 `getValue` registration walks
- Sites: `register_passthrough_getvalues` (RSP:10118-10137) and `build_passthrough_getvalue_specs` (RSP:10140-10167).
- Difference: identical validation. One calls `getValue`, the other fills a `GetValueSpec`.
- Helper: `const NdbDictionary::Column* passthrough_dict_column(Uint32 col_idx)`.
- Lines saved: about 12. Risk: low.

### X-25 STAT_ timing boilerplate
- Sites: 85 `STAT_TS/SET/ADD/COUNT` uses in RSP. The pattern is `STAT_TS(a); work; STAT_TS(b); STAT_SET(field, a, b);`, for example at 9617-9622, 9677-9685, 9696-9700, 10439-10443 and 11050-11056.
- Helper: an RAII `PhaseTimer t(m_conf.phase_stats, &RonSQLPhaseStats::print_us);` with `t.add()` for the summed `close_us`.
- Lines saved: about 40. Risk: low, but note the semantic change: a phase interrupted by an exception would then be recorded. The stats are documented as last-attempt-wins, so that is acceptable.

### X-26 Printing an aggregate result with timing (four copies)
- Sites: RSP:9694-9700 (single-table), 10439-10443 (J5), 9905-9910 (count-star, a different printer method) and 11886-11895 (join).
- Helper: `void print_aggregate_result(NdbAggregator*)`.
- Lines saved: about 12. Risk: low.

### X-27 Transaction close at every exit
- Sites: `cleanup_trans()` at five return points in `execute()` (RSP:9626, 9634, 9642, 9659, 9703) and in five arms of `handle_ronsql_exception`.
- Helper: an RAII guard in `execute()`, or a single `dispatch()` followed by one `cleanup_trans()`, plus `cleanup_trans()` in the destructor.
- Lines saved: about 8. Risk: low. Removes the risk that a future early return leaks the transaction.

---

## 3. Test gaps specific to this slice (X-28 .. X-36)

Checked with grep over `mysql-test/suite/ronsql*/t`, `rest-server2/server/test_go/internal/integrationtests/ronsql*` and `rest-server2/server/test/ronsql_*_test.cpp`.

- **X-28 Retry paths are untested.** The only error inserts in RonSQL suites are 4042/4043 in `ronsql_agg_drain` (drain shape, not errors) and 4060 in `ronsql_cte_jit_census`. No test produces a temporary NDB error before the first row (expect a transparent retry), after the first row in a pass-through (expect a permanent error and no duplicated output, CLI and REST), inside an IN subquery (X-6), or at `startTransaction` (X-8). `status_test.go` covers 400 classes only, with no 500 or 503 from NDB.
- **X-29 The schema-change paths (RMS, `unload_schema`) are untested.** No RonSQL test runs ALTER, DROP INDEX or CREATE INDEX between queries with RDRS's schema cache (X-10), or during a query (the 241/284/20021 paths in both single-table and join).
- **X-30 Scalar subquery NULL in WHERE** (X-3), and AVG or DOUBLE scalar results against INT columns (X-4): no `(SELECT AVG` anywhere; WHERE scalars always return non-NULL in `ronsql_subquery`/`ronsql_cte_subquery`.
- **X-31 Ring-buffer tables** are never used in RonSQL suites (X-2).
- **X-32 Parser crash** (X-1): no test pairs TOO_LONG_UNALIASED_OUTPUT with a following lexical error, in either the CLI or the ParseOnly authorization path.
- **X-33 Large or duplicate IN-subquery results** (X-5): no case with more than 1000 inner rows and few distinct values, and no large inner result.
- **X-34 PK arms with mixed per-key outcomes.** Present, missing (626) and filter-rejected keys are covered (`ronsql_in_list_lookups`, `ronsql_in_list_agg_lookups`). A key-read temporary error in the pre-walk (RSP:10838) is not, and needs an error insert on LQHKEYREQ.
- **X-35 CLI exit code 3** (retry budget exhausted) is never asserted, and the CLI's partial output on a permanent error after the stream started (unterminated JSON) is unspecified.
- **X-36 Inner-preparer fault injection.** `allocation_test_hook` is not propagated (X-15), so I4d cannot cover allocation failures inside subquery preparation and execution.

## 4. Answers to the brief, in short

- **State machine:** sound for the constructor, missing for `execute()` (X-14). One real crash in `set_err_state` (X-1).
- **Ownership:** `NdbTransaction` is closed on every normal and exceptional path (`cleanup_trans` in each arm). Scan operations, NdbQuery and aggregator programs are owned by the transaction. The stack `NdbAggregator`/`NdbInterpretedCode` objects die before `closeTransaction()` on unwind. The NDB API no longer touches them on close: `m_aggregation_code` is cleared on reuse (NdbScanOperation.cpp:141-152) and the close paths do not dereference it. I found no leak, double close or use after close. `closeNoWait()` parks the scan on the pooled Ndb object, and the next transaction on it reaps it. The arena outlives the preparer. The inner subquery arena is scoped. Exceptions raised inside the handler are the only gap (X-12).
- **Output on error:** the CLI streams to stdout, so partial rows plus exit 1 on a permanent error. REST discards the body on any non-200 status. The ",od" rule is therefore needed only because `ronsql_op` reuses the stream (X-7), and it misfires for subqueries (X-6) and leaves the class INTERNAL.
- **Retryable classification:** TemporaryError (NR/TR/TO/OL/NS/IT) is retried. Unknown-result is not (X-8). Dictionary errors are not consulted (X-9). The source of the error read can be stale (X-9).
- **Dispatch guards:** `execute()`'s order (join, J5, pass-through, count-star, PK aggregate, scan aggregate) matches what planning guarantees. `detect_pk_lookup` requires an IN column for aggregates (RSP:5139), which `execute_pk_lookup_aggregate`'s `ndbrequire` relies on. `is_count_star_only_query` excludes CTEs, subqueries, J5 and joins. SELECT-list subqueries are rewritten into joins before dispatch. I found no query that falls into the wrong path. The missing ring-buffer check (X-2) is a semantic gap in the eligibility test, not a dispatch-order problem.
- **Subquery substitution:** NULL handling is correct in HAVING and for IN polarity (parser review). It is broken for scalar NULL in WHERE (X-3). The type, size and precision of substituted values are lossy (X-4). The 1000 cap counts duplicates and is enforced after unbounded buffering (X-5).
