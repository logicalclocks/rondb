# RonSQL executor analysis (RONDB-1124)

This review covers the RonSQL executor: design weaknesses, code that could be
shared through common functions, and weak spots in the tests. It follows the
parser review (`storage/ndb/src/ronsql/ronsql_parser_review_plan.md`) and the
DBSPJ review (`../dbspj/README.md`) and does not repeat their findings.

Baseline: 50eb491eb3f (26.10-main). Seven reviews ran in parallel and only read
code; nothing was built, run or edited. Findings marked **verified** were
re-checked in the code after the reviews, by reading both the emitting and the
consuming side where there is one. Other findings carry the confidence the
reviewer gave. No finding has been reproduced by a test run yet.

Line numbers refer to `storage/ndb/src/ronsql/RonSQLPreparer.cpp` at the
baseline unless another file is named.

## Documents

| File | Scope | IDs |
|------|-------|-----|
| `review_dispatch_lifecycle.md` | `execute()` dispatch, subquery execution and substitution, single-table paths (pass-through, API-side aggregate, count(\*) shortcut, PK-lookup aggregate), transaction lifecycle, exception classification, REST retry loop, CLI | X-1..X-36 |
| `review_join.md` | `execute_join`, root emitters, `emit_child_ops`, pass-through drain, LEFT→INNER promotion, EXPLAIN vs execution | J-1..J-22 |
| `review_cte.md` | `build_cte_virtual_tables`, `emit_cte_lookup_filter`, CTE ORDER BY/LIMIT, `programAggregator_join`, API-side checks of data-node limits | C-1..C-15 |
| `review_filter_agg_codegen.md` | `apply_filter*`, `encode_constant`, embedded interpreted-code emitters, `programAggregator`, `AggregationAPICompiler` | F-1..F-22 |
| `review_output.md` | `ResultPrinter` (TEXT, JSON, JSON_ASCII, Avro), per-type formatting vs mysqld, API-side HAVING/ORDER BY/LIMIT, column metadata | O-1..O-17 (+ its own D-1..D-8 duplication list) |
| `review_tests.md` | Coverage map from executor code paths to MTR/unit/Go tests, weak assertions, mirror suites, flakiness | T-1..T-24 |
| `review_duplication.md` | Cross-region duplication sweep (script-based), diverged copies, longest functions | D-1..D-17 |

Several findings were reported by more than one review:
D-1 = F-3; D-5 = J-4 = C-7; D-4 = F-7 = C-4; J-3 = C-5 (cause: C-13);
X-3 = J-11; X-4 = F-10 (in part) = T-10; J-5 = C-8 = D-9 = F-20;
J-6 = C-9; J-17 = C-11 = D-2; X-18 = D-14; X-21 = D-8; X-22 = D-11;
O-10 = D-7; D-3 = F-17; D-6 = output-review D-1; X-14 = J-10 = O-17;
T-1 contains O-7; C-15 overlaps T-3. J-2 has the same root cause as C-1.

## Most important findings

### Process crashes (RDRS or ronsql_cli)

- **X-1 (verified): a lexical error after an unaliased output longer than 64
  characters aborts the process.**
  - The lexer records its error at the lookahead token.
  - Bison then reduces `output: nonaliased_output` (`RonSQLParser.y:397`) by default and records TOO_LONG_UNALIASED_OUTPUT further to the left.
  - The `ndbrequire` in `Context::set_err_state` (20205) fails.
  - In RDRS the ParseOnly authorization parse runs before `authenticate()` (`ronsql_ctrl.cpp:342-369`), so any HTTP client that can reach the endpoint can stop RDRS.
  - Trigger: `SELECT SUM(x+x+…32 terms…) UNION SELECT 1`.
  - Fix: keep the leftmost error instead of requiring it.
- **F-1 (verified): `AVG(CASE …)` in the main query aborts the process.**
  - Main-scope AVG compiles the same CASE twice, once for SUM and once for COUNT.
  - The THEN arm passes `is_first`, but the ELSE arm's `AggRepeat` always passes `true` (`AggregationAPICompiler.cpp:615`).
  - On the second compile `svm_use` fails its `ndbrequire`.
  - One-line fix. No test uses `AVG(CASE`.
- **O-1 (likely; both halves verified): chained-CTE string GROUP BY key with ORDER BY can segfault RDRS.**
  - `build_result_column_metadata` (8796-8826) drops the charset that `resolve_chained_column_type` returns.
  - The GROUP BY arm of `compare_rows` (`ResultPrinter.cpp:1094`) then calls `cmpVarchar` with a NULL charset.
  - The aggregate arm has a `require_sch(spec.charset != nullptr)` guard; this arm does not.
  - The same missing metadata prints DECIMAL(p,0) keys from an uninitialized buffer (O-14) and temporal values as packed integers.
- **C-3 (likely): CTE virtual columns of type DATETIME2, TIMESTAMP2, TIME2, DECIMAL, BINARY, VARBINARY or BIT get size 0.**
  - The size switch is at 13994-14075.
  - A pass-through CTE_LOOKUP child undersizes its receive buffer: a debug assert, or a heap overflow in a release RDRS.
- **F-2 (likely): unary minus over a float scalar subquery dereferences NULL** (`simplify_ce` T_MINUS, 16950). Trigger: `WHERE o.total > -(SELECT MIN(dec_col) FROM p)`.
- **K-1 (reproduced; fixed by bug 14, K-1a and K-1b): the data node runs an aggregation program that reads a register the row never loaded.**
  - Found while fixing D-1. The kernel side is tracked in `../pushdown_join_aggregation/ronsql_exec_fixes_handoff.md` §4.2.
  - Before the D-1 fix, `ronsql_subquery_filter_decimal` sd-1 aborted both data nodes of a debug build on `assert(a.type != NDB_TYPE_UNDEFINED)` in `AggInterpreterBase::Sum`. A release build silently uses an undefined or stale value: registers are zeroed once in Init, not per row.
  - Bug 14 (`8878794d1fd`, `AggInterpreterBase::checkProgram`) now refuses at setup: bad header counts and versions, out-of-range registers and result slots (the unchecked `kOpSum`/`kOpMax`/`kOpMin`/`kOpCount` index), operands past the end, and a `kOpSkip` into an instruction. `embeddedSkipTarget` refuses a per-row skip onto an operand word.
  - Also fixed:
    - Fixed (K-1b): an aggregate reading a register not loaded in this row. The skip after an embedded program is computed per row, so it cannot be checked before the program runs. `AggInterpreterBase::m_loaded_regs` is a per-row bitmask of loaded registers: loads, constants and `kOpSetRegNull` set a bit, `kOpMov` and arithmetic set their result's. An aggregate, `kOpMov` or arithmetic reading a register outside it fails the row with 1868, and an embedded `READ_AGG_REG_TO_REG` (DBTUP and the NULL-row evaluator) with 878. The JIT's register file is zeroed per row, so it would have aggregated 0: its compiler (`ndb_jit_bridge.c`) tracks the registers loaded on every path and refuses such a program (`JIT_BRIDGE_REG_NOT_LOADED`), which then runs on the interpreter.
    - Fixed (K-1a): `peekProgramHeader` checked the magic word, `prog_len`, the reserved flag bits and the 1024-word limit with `assert` only. `checkProgramHeader` now refuses them for `checkProgram` (a join setup gets 20002) and `peekProgramHeader` (Init fails, a single-table scan gets 1868). Tests: `testSpjMalformed` P30 (bad magic word) and P31 (reserved flag bit), MTR `ndb_spj.test_spj_malformed_p30`/`p31`.
  - Test: `testSpjMalformed` P32, a leaf whose `kOpSkip` jumps over the load of the register its `kOpSumBigint` reads (MTR `ndb_spj.test_spj_malformed_p32`, and `_p32_jit` with the JIT on); `bridge_tests` T85a-e.
- **E-1 (found while fixing X-3; fixed): EXPLAIN of a query whose WHERE holds LIKE, GREATEST, LEAST or a subquery aborts the process.**
  - EXPLAIN prints the WHERE of the parse tree, and `print(ConditionalExpression*)` ended in `abort()` for an operator without a case: `T_LIKE`, `T_GREATEST`/`T_LEAST`, the scalar subquery placeholder `I_SUBQUERY`, `I_IN_SUBQUERY` (IN subqueries and decorrelated EXISTS), `I_CORR_SCALAR` (decorrelated correlated comparisons), `T_EXISTS` (without a connection) and `T_NULL`. The EXPLAIN output list ended in `abort()` for a `SUBQUERY_AGG` output.
  - Through the /ronsql endpoint (`explainMode`, or `EXPLAIN SELECT`) one such query stopped RDRS. No test ran EXPLAIN on any of these shapes.
- **X-14 = J-10 = O-17 (fixed, except the state machine): invariant checks in the executor and printer abort RDRS on API-side conditions.**
  - There are about 108 `ndbrequire` and 26 `abort()` calls, including `ndbrequire(qb != NULL)` when out of memory.
  - In a long-running server they should throw an internal error instead.
  - Fixed in four sweeps, one commit each (Fix status 4.3-4.6). A broken invariant now fails the statement with a permanent INTERNAL error ("... Please report a bug.", HTTP 500 in RDRS) through `ronsql_bug` / `ronsql_require_bug` (`RonSQLCommon.hpp`).
  - Sweep 1 of 3 (printer) done: the 28 `ndbrequire` and `abort()` of `ResultPrinter.cpp` (O-17). One was reachable from a statement: a TEXT or BLOB GROUP BY key passed up two CTEs gets its metadata from `resolve_chained_column_type`, which, unlike the stored-column path fixed by RONSQL-39, keeps the inline and part sizes as precision and scale, so `compile()` failed `ndbrequire(precision <= 65)` (test `ronsql_internal_errors` ie-1, ie-2, now `ronsql_groupby_blob_chained` gbc-3, gbc-9). The two `abort()` of `ronsql_cli.cpp` stay: no input reaches them, and the CLI runs one statement.
  - Fixed (RONSQL-39 through CTEs, Fix status 3.9): a TEXT or BLOB GROUP BY key passed up through CTEs is refused at prepare time with the error of a stored TEXT or BLOB GROUP BY column, "GROUP BY on a BLOB or TEXT column is not supported." (class unsupported, HTTP 400 in RDRS). Before, two CTEs up it was the internal error of sweep 1 ("A GROUP BY output has a precision above 65. Please report a bug.", HTTP 500), and one CTE up it failed at execute time on its virtual column ("CTE output column `txt` has a column type that CTEs do not support.", C-3). `check_cte_groupby_key_types`, run after `validate_cte_execution_shapes`, takes each GROUP BY key of each CTE body and of the main query that is a CTE output, follows it down the chain with `resolve_chained_column_type` (the type `build_cte_virtual_tables` gives its virtual column) and refuses BLOB and TEXT, before the printer is built. Stored keys stay with `GroupBy()` / `GroupByLinked()`, and a TEXT column a CTE passes on without a GROUP BY on it keeps the C-3 error (RonSQL prints no TEXT or BLOB value). The printer's precision check is unchanged.
  - Sweep 2 of 3 (aggregate compiler) done: the 48 `ndbrequire` and 8 `abort()` of `AggregationAPICompiler.cpp` and its `require_status` and `require_reg` macros (46 uses), and the two `ndbrequire` of `RonSQLListBuilder::append` (`RonSQLPreparer.hpp`). None was found reachable from a statement; F-1 had been one (`svm_use`).
  - Sweep 3 of 3 (preparer and executor) done: 120 of the 129 `ndbrequire` and `abort()` of `RonSQLPreparer.cpp`. J-10: `NdbQueryBuilder::create()` returning NULL throws `std::bad_alloc` (a RESOURCE error, "Out of memory while processing the query."); `resultAgg`, `execute_passthrough_drain`, `emit_child_ops`' `default` and its bound operand count are INTERNAL errors. X-14: the three missing-SubqueryInfo checks of `substitute_subquery_results_ce`, the state checks of the constructors, `get_accessed_*` and `execute()`, and the `catch (...)` of `handle_ronsql_exception`, which now closes the transaction and fails the statement. Kept, with a comment: the seven `err_stream` checks (the error handler writes to `err_stream`; the VM_TRACE one in `configure()` now runs first), `cleanup_trans()`'s `ndbrequire(ndb)` and the one in `handle_ronsql_exception` (the error path must reach `closeTransaction`). None was found reachable from a statement.
  - Sweep 4 (lexer, parser, RDRS `ronsql_op`) done: the four `abort()` of `RonSQLLexer.l` and `RonSQLParser.y`, the two of `ronsql_op` and the output-format `default: abort()` of `RonSQLRequest::execute` (`ronsql_ctrl.cpp`). Lexer: the identifier length check of the `[a-z_]{1,18}` rule (the rule matches at most 18 bytes) returns `LEX_TOO_LONG_IDENTIFIER`, as the `{UNQUOTED_IDENTIFIER_CHARACTER}+` rule does; `rsqlp_realloc` is an INTERNAL error (flex reallocates only to grow a buffer it fills from a file or to grow the buffer stack; `yy_scan_buffer` scans the SQL in place and allocates the stack once). Parser: the `default` of `cond_expr`'s aggregate switch (`aggfun` gives only SUM, MIN and MAX) and the third case of `rsqlp_error` (the error location ends at the end of the last lexer match or at or before its start) are INTERNAL errors; grammar actions and `rsqlp_error` run inside `yyparse`, which other actions already throw through. RDRS: an exception of another type than the three RonSQL ones (only a `std::bad_alloc` thrown while `handle_ronsql_exception` handles another could be one) and the end of the retry loop (every arm returns on the last attempt) get the response of an INTERNAL `RonSQLPermanentError` (HTTP 500, class `internal`, "... Please report a bug."); an unknown output format in `ronsql_ctrl.cpp` answers HTTP 500 with class `internal`. None was found reachable. The two `abort()` of `ronsql_cli.cpp` (sweep 1) and the startup `abort()` of `buffer_manager.cpp` stay.
  - Remaining: the `EXECUTING`/`DONE` state machine of the X-14 write-up (`execute()` single-shot, FAILED on an exception, `cleanup_trans()` in the destructor).

### Wrong results

- **C-1 (verified): CTE output columns are numbered two different ways.**
  - The API numbers virtual-table columns in output order (`m_attrId = cidx`, 13980).
  - The consumers (`lookup_relation_column`, `programAggregator_join`, `emit_cte_lookup_filter`) use the same output order.
  - The kernel numbers attributes as GROUP BY keys in GROUP BY order, then aggregate slots (`DblqhMain.cpp` `emitCteGroupOutput`, about 21885).
  - Nothing reorders or rejects other layouts.
  - Examples:
    - `SELECT COUNT(*) n, k … GROUP BY k` returns `n` and `k` swapped (or fails on the type mismatch).
    - Output order different from GROUP BY order swaps the keys.
    - `SUM(x) s1, SUM(x) s2` are deduplicated into one slot, so `s2` is NULL.
    - A GROUP BY column missing from the outputs shifts every column.
  - J-2, the CTE root key lookup using output order for the key, is the same defect.
  - Every existing test uses the "keys first, in order" layout.
- **J-1 (verified): a PK-equality index-scan root binds key constants to a permuted index.**
  - `QueryPlanner::findOrderedIndex` accepts any permutation of the key columns.
  - The planner reorders child keys for exactly this reason (`QueryPlanner.cpp:376`).
  - `emit_pk_equality_index_scan_root` (12529) binds in PK order, so with `PRIMARY KEY USING HASH (a,b)` and `INDEX (b,a)`, `WHERE a=1 AND b=2` reads (a=2, b=1).
- **D-1 = F-3 (verified): DECIMAL inside a CASE arm misplaces the jumps.**
  - `AggregationAPICompiler::raw_word_size` counts a Load as 1 word; `NdbAggregator::LoadColumn` and `LoadLinkedColumn` emit 2 for DECIMAL.
  - THEN arm: a false condition lands on the Skip, so the ELSE arm never runs. `SUM(CASE … THEN dec ELSE 0 END)` gives NULL for groups with no match, and COUNT is off.
  - ELSE arm: THEN rows land on `AggRepeat` and add a stale register.
  - The multi-leaf join aggregator hard-codes the same size (11451).
  - Every existing SUM(CASE) test uses INT or BIGINT columns.
  - Reproduced by `ronsql_subquery_filter_decimal` on the multi-leaf path: in a debug build both data nodes aborted (K-1).
- **F-4 (verified): right-nested arithmetic in a cross-table filter overwrites the left operand.**
  - `emit_embedded_filter_expr` (17466) has two registers and swaps them for the right subtree.
  - `WHERE o.x < l.y * (l.z + 1)` is evaluated with `l.y` overwritten.
- **C-2 (verified): a cross-table OR in WHERE promotes a LEFT JOIN to INNER.**
  - `is_null_rejecting` (3895) has no table argument.
  - The cross-table filter loop in `promote_left_to_inner_for_where` (4061-4069) treats `c.n > 4 OR cu.x = 3` as rejecting the NULL-extended `c` row, so rows with `cu.x = 3` and no match are lost.
- **J-3 = C-5 (verified at the emitter): the CTE root lookup narrows integer constants with unchecked `(Int32)` / `(Uint32)` casts** (12905-12930). `WHERE k = 4294967297` on an INT key finds group 1. The real-table root path range-checks (C-13).
- **D-4 = F-7 = C-4 (verified by reading; fixed): a CTE body with a cross-table residual filter left phantom groups, and an AVG in such a body failed.**
  - The kernel creates the group before the filter stops the row, so a group whose rows all fail keeps the hidden sentinel COUNT at 0. Only the main query's printer suppressed such groups. A CTE body is materialized and read on the data nodes, so the outer query saw `(k, 0, NULL)` groups: an extra row, COUNT(\*) over the CTE one too high, a LEFT JOIN matching with `n = 0` instead of NULL, a place of the body's LIMIT taken.
  - Both sentinel-slot computations skipped AVG, which a CTE body compiles as one Avg slot. When AVG was the last aggregate, the sentinel took its slot and `Count()` refused it: "Failed writing aggregation program. Please report a bug."
  - Fixed: one helper, `cross_filter_sentinel_slot`, counts Avg. A grouped CTE body's program ends with a `kOpDropZeroCount` trailer naming the sentinel, and the owner erases the groups whose sentinel is 0 in `Dblqh::checkCteReady`, before the AVG divide, the ORDER BY / LIMIT finalize and CTE_READY. Without GROUP BY the one row is kept, as after F-6 (Fix status 3.8).
- **S-1 (checked; not a bug): correlated scalar substitution skips the NULL pairs.** Leaving out an inner group whose value is NULL, or an outer key without a group, gives FALSE where MySQL has UNKNOWN, which differs only under NOT.
  - `decorrelate_scalar` rewrites only a comparison that is a top-level AND conjunct of WHERE. It removes the NOT and `!` around it and negates the operator (`strip_not`, `negate_cmp_op`), and NOT (a OP b) equals a NEG(OP) b also when a or b is NULL. So `I_CORR_SCALAR` is never under NOT, and a comparison under OR, XOR or a NOT over AND is not decorrelated.
  - Covered by `ronsql_subquery` tests 34-37 (no inner group, NULL outer key, NULL inner key, COUNT) and the new tests 69-70 (an inner group whose SUM or MAX is NULL, under NOT) (Fix status 1.11).
- **F-6 (verified; fixed): a cross-table filter on an aggregate without GROUP BY returns no row instead of `0`** (`ResultPrinter.cpp:1163-1186`). Example: `SELECT COUNT(*) FROM o JOIN l … WHERE l.q > o.c + 1000000`.
  - Without GROUP BY `NdbAggregator` always returns its one row, and the hidden sentinel COUNT reads 0 when no row passed the filter, so `print_result_ordered` dropped it. MySQL returns COUNT 0 and NULL for the other aggregates.
  - Fixed: the sentinel suppresses GROUP BY groups only (Fix status 2.4).
- **F-5 (verified by reading; fixed): the GREATEST/LEAST rewrite in WHERE was wrong under NOT or XOR with a NULL argument.**
  - `rewrite_minmax_comparison` gives FALSE where a NULL argument makes the comparison UNKNOWN: the added IS NOT NULL guards, and `a < c AND b < c` with a NULL `a` and `b >= c`. That drops the same rows only where no NOT is above the comparison.
  - `simplify_ce` applied a NOT, and the `NOT x` of the XOR expansion, to the rewritten tree, so NOT FALSE kept rows that mysqld drops. `WHERE NOT (GREATEST(a, 5) > 10)` kept every row with a NULL `a`.
  - Fix: `simplify_ce` moves a NOT down to a GREATEST/LEAST comparison before the rewrite (`negate_minmax_condition`: the inverted comparison, De Morgan, `NOT (x XOR y)` = `x XOR NOT y`), so the rewrite runs only where no NOT is above it.
  - A comparison that a constant argument makes FALSE, such as `GREATEST(a, 20) <= 10` (what `NOT (GREATEST(a, 20) > 10)` now becomes), is `a IS NULL AND a IS NOT NULL` instead of the "constant contradiction" error.
- **C-6, O-3, O-10 = D-7: CTE MIN/MAX of temporal columns stay packed integers.**
  - In CTE filters (`c.d >= 19950101` compares packed values).
  - In GROUP BY over the CTE output (prints `1036389`).
  - In GREATEST/LEAST display.
- **F-8, F-9, F-10, F-11: type handling of constants.**
  - Cross-table filter leaf columns are loaded as raw integer bits.
  - FLOAT columns compare as float in WHERE but as double in CASE.
  - A float subquery result is rounded to the DECIMAL column's scale.
  - Temporal literals are truncated to the column's fractional precision.
- **X-2 (verified; fixed): the COUNT(\*) shortcut counts the hidden meta rows of ring-buffer tables.** mysqld skips the fast path for `isRingBuffer()`.
- **X-4: subquery results go through text.**
  - `int_col > (SELECT AVG(…))` always fails.
  - BIGINT UNSIGNED above 2^63 fails.
  - DECIMAL beyond about 15 digits loses precision.
  - `''` is treated as NULL.
- **Output vs mysqld:**
  - O-2 (verified; fixed): pass-through JSON does not escape column names (`ResultPrinter.cpp:1698`); `` `x"y` `` gives invalid JSON. cfd835ae18d fixed only the operationId sibling.
  - O-4: DOUBLE values of 1e15 or more print in fixed point.
  - O-5 (fixed): negative zero. A DECIMAL SUM/AVG accumulated as a double that rounds to zero printed `-0.00`; the fixed-scale printer now rounds through a decimal, which clears the sign.
  - O-6 (fixed): AVG rounds ties to even. The fixed-scale printer now rounds HALF_UP as mysqld prints a DECIMAL (`double2decimal` + `decimal_round(HALF_UP)`), for AVG, its ORDER BY key and the CTE outputs.
  - O-7: DECIMAL with precision above 15 drops its scale (recorded as expected output, see T-1).
  - O-8: HAVING is evaluated in DOUBLE (BIGINT above 2^53, DECIMAL equality, temporal and string MIN/MAX).
  - O-9: ORDER BY SUM(DECIMAL) ties.
  - O-11: duplicate JSON keys.

### Valid queries that fail, and error handling

- **X-3 = J-11 (verified): an empty or NULL scalar subquery in WHERE, a bound or a key fails.**
  - The substituted `T_NULL` (9517) reaches `encode_constant`, which throws `RonSQLMaybeStaleSchema`.
  - The schema is reloaded and the request fails with 400/500; MySQL returns zero rows.
  - The parser review fixed only HAVING.
- **S-2 (found while fixing X-3; fixed): a scalar subquery used directly as a condition, `WHERE (SELECT …)`, failed with "Non-boolean term" whatever its result.**
  - Prepare accepted the subquery there. At execution its result replaced the node as a number, and `apply_filter`, which builds the NdbScanFilter from predicates only, refused the number. MySQL takes a value used as a condition as `0 <> value` (`make_condition()`): TRUE when it is not zero (0.4 too), FALSE when it is zero (0.00 too), UNKNOWN when it is NULL.
  - Fixed: `substitute_subquery_results_ce` knows when a node is the whole WHERE or an operand of AND, OR, XOR or NOT, and `substitute_condition_result` writes the result there as TRUE (`NULL IS NULL`), FALSE (`NULL IS NOT NULL`) or UNKNOWN (`0 <> NULL`, also for no row), the constant conditions that the X-3 fold already folds. The fold removes them from every WHERE structure of the plan.
  - Unchanged: a bare column, a bare constant and arithmetic, also over a subquery (`WHERE (SELECT …) + 0`), are still a "Non-boolean term"; HAVING still refuses a subquery used as a condition at prepare time. A string result fails with "Subquery returned a non-numeric result." (MySQL converts it to a number with a warning), and `''` counts as NULL (X-4).
  - A correlated subquery used as a condition is refused at prepare time (`reject_correlated_scalar_conditions`): `decorrelate_scalar` rewrites one only in a comparison, so it would run alone, with the outer column read as a column of the inner table or not found. Test: Fix status 1.10.
- **K-2 (reproduced while fixing X-3; fixed): a scalar CTE scanned with a filter that rejects its row hangs the query.**
  - The scalar arm of `Dblqh::cteScanEmitResults` (`DblqhMain.cpp` ~23444) sets `numRowsSent = 1` after its filter gate, also when the filter rejected the row. The comment in the reject branch says it stays 0.
  - `Dbspj::execCTE_SCAN_CONF` adds `numRows` to the rows it reports to the API (`DbspjMain.cpp:8869`), so the API waits for a row that never comes until the receive timeout ends the query.
  - Found by the first version of X-3, which scanned a scalar CTE root whose WHERE is never TRUE with a reject-all filter (sn-6). RonSQL no longer builds the shape: every filtered scalar CTE root is a dummy-key lookupCte. A raw NDB API client still can. `testCteNdbApiFilter` test 14 rejects every group of a grouped CTE only.
  - Fixed: the scalar arm counts the row only when the filter accepted it, as the scalar agg-feed arm of `cteScanAggFeed` already did. Test: `testCteNdbApiFilter` test 32 (MTR `ndb_push_agg.testCteNdbApiFilter`, `ndb_push_agg_jit` with the JIT on): SUM(val) of `cte_src` as a scanCte root, a filter that keeps the row (one row of 150) and one that rejects it (no row).
- **D-5 = J-4 = C-7: a filtered scalar CTE root with a VARCHAR first output fails.** The root dummy-key copy (12713) lacks the sc-20 fix that its child copy (15385) has. Fixed: both now use `scalar_cte_dummy_key` (Fix status 3.5).
- **J-5 = C-8 = D-9 = F-20 (verified by reading; fixed): some sites ignored the return codes of `filter.end()`, `finalise()` and `setInterpretedCode()`.**
  - Six join/CTE sites ignored them; a failure silently dropped the WHERE filter. The single-table copies check them.
  - Reachable from SQL: `NdbInterpretedCode` grows to `NDB_MAX_SCANFILTER_SIZE_IN_WORDS` (15359 words). `apply_filter` checks every comparison, but the last `end()` adds 4 words (EXIT_OK, the AND group's label, EXIT_REFUSE), so a WHERE whose comparisons take 15356-15359 words fails only there (4294). The program stayed unfinalised, `setInterpretedCode` refused it (4519) and the operation ran unfiltered: a root scan, a joined table, a CTE body root and an index-scan root returned the rows the WHERE removes; the PK index-scan root, which checked `setInterpretedCode`, failed with an error classified from the transaction (the PK lookup root fell through to it, the unfinalised program being over 64 words).
  - Fixed: `build_where_filter` (begin, `apply_filter`, end, finalise) and `set_where_filter` (`setInterpretedCode`) check each step and throw the NDB error of the filter or the program through `throw_classified_ndb_error`; the join and CTE sites use them, and the builders stay separate (Fix status 2.5).
- **J-6 = C-9 (verified by reading; fixed): builder errors were lost.**
  - `require_run` classified from `m_trans->getNdbError()`, never from `qb->getNdbError()`: a builder failure was an internal error (HTTP 500) with code 0.
  - The 32-operation limit of one NdbQueryDef was not checked before building: QueryPlanner counts the main plan only, and the CTE subtrees went uncounted. A query over the limit failed at the operation that crossed it ("Failed to create child operation.").
  - The `beginCteSubtree` result was ignored; the next body op failed with "Failed to create CTE body scan root."
  - QRY_UNRELATED_INDEX (4809) reloaded the schema only for child operations, not for a root or CTE-body index scan.
  - Fixed: `check_pushed_query_size` counts the operations at prepare time (per CTE its subtree, its body and the MIN/MAX self-join; a unique-index lookup counts 2) and refuses more than 32 with a LIMIT error. Every builder call goes through `require_qb`, which raises the builder's own error with its code (`throw_query_builder_error`): 4809 reloads the schema for every operation, 4812 is LIMIT, 4000 RESOURCE, anything else INTERNAL. `beginCteSubtree` and `endCteSubtree` are checked (Fix status 4.2).
- **Retries:**
  - X-6: rows a streaming IN subquery wrote to its private buffer count as "output started", so a temporary error becomes permanent.
  - X-7: REST buffers the whole body, so the no-retry-after-output rule is unnecessary there.
  - X-8: unknown-result errors 4008, 4012 and 4035-4041 are permanent 500s; RonSQL is read-only, so retrying them is safe.
  - X-9: possible stale `Ndb::theError`.
  - X-10: a stale schema cache entry after DROP INDEX.
  - X-13: `m_output_started` is set at only 2 of about 8 output sites.
- **X-5: IN and scalar subquery output is buffered without a limit** before the 1000-value and one-row checks, outside `MaxRespSize`.
- **F-12..F-15:**
  - Out-of-range literals raise errors instead of folding to true or false.
  - WHERE and CASE accept different predicate sets.
  - `!` is not lowered in WHERE.
  - A program over 8192 words says "Please report a bug".
- **O-13: unsupported types are found only while printing.** GROUP BY on YEAR, TIME, BINARY or BIT, and pass-through of BIT or BLOB, fail after the scan and after a partial header.

## Design weaknesses

1. **Two layouts for one CTE row, with no invariant check (C-1, J-2).**
   - The API's virtual table, linked-column positions and lookup keys use output order; DBLQH uses GROUP BY order then slot order.
   - Either build the virtual table in kernel order with an output→attribute map, or reject other layouts in `analyze_ctes`.
   - An `ndbrequire`-free consistency check at virtual-table build time would catch future drift.
2. **Program word counts are kept by hand next to the emitters (D-1, F-16, F-22).**
   - `raw_word_size`, the CASE arm sizes, the multi-leaf constants and `embedded_filter_expr_word_count` are all separate from the code that emits.
   - A single per-instruction size source, or emitting first and measuring, removes the D-1 class of bug.
3. **Subqueries run as full nested preparers and exchange results as text (X-4, X-5, X-15).**
   - There is no schema cache and no size bound while buffering.
   - Types are lost, and the inner error log and test hooks are dropped.
   - Typed results with a cap checked while streaming would fix X-3, X-4 and X-5 together.
4. **Error classification is spread over many sites.**
   - `require_run` throws `std::runtime_error` and the handler guesses the NDB error from the transaction.
   - Two classification copies have diverged (X-18).
   - Dictionary errors are not consulted (X-9). Builder errors are, since the J-6 fix.
5. **Invariants abort a multi-tenant server (X-14, J-10, O-17).**
6. **EXPLAIN does not show what execution does (J-9, T-13).**
   - The root access path (readTuple, PK index scan, lookupCte, G4 probe) and the root filter are not printed.
   - `<- parent` prints the key source.
   - "Aggregation leaf" is printed for pass-through joins.
7. **Two embedded predicate emitters and an NdbScanFilter path accept different predicate sets and handle types differently** (F-9, F-13, F-19).
8. **Performance:**
   - J-7: `listIndexes` per execution, bypassing the schema cache.
   - J-8: multi-op CTE bodies never get a root index scan.
   - C-10: unreferenced CTEs are still materialized.
   - X-16: PK lookups use an unhinted transaction.
   - O-12: the pass-through ORDER BY memory cap undercounts by up to 8x.
   - O-16: full sort for ORDER BY … LIMIT on aggregates.

## Duplicated code (ranked)

| Cluster | Sites | ~Lines saved | Diverged copies (bugs) | Source IDs |
|---------|-------|-------------|------------------------|------------|
| `ResultPrinter::print_record` / `print_stored_record` | 2 | 200-260 | none | D-6, output D-1 |
| CTE output type derivation (`resolve_chained_column_type` / `build_cte_virtual_tables`), incl. display-scale block ×6, AVG scale rule ×4 | 2 | 170-250 | none today (history: obc-11) | D-2, J-17, C-11 |
| Per-type value formatter in ResultPrinter; fixed-scale double formatting ×6 with 3 different gates | 3 / 6 | ~150 | O-3, O-5, O-7, O-13 | output D-2, D-3 |
| `emit_cte_lookup_filter` inverted-branch switch ×5; inline `flip_cmp_op` ×5; dead operand-swap code | 10 | ~150 | none | C-12, D-15 |
| SVM instruction → NdbAggregator translation (`programAggregator`, `programAggregator_join`, multi-leaf in `execute_join`), plus the word-size logic | 3 | ~110 | D-1, unsupported-type error only in one copy | D-3, F-17, D-1 |
| Pass-through drain skeleton (single-table vs `execute_passthrough_drain`) | 2 | 90-100 | error classification, early close, LIMIT batch size | D-8, X-21 |
| Embedded-interpreter emit primitives (exit stanza ×10, op→branch mapping ×4) | 14 | ~70 | none | D-10 |
| Subquery value parsing ×3; hand-built ConditionalExpression nodes | ~20 | ~70 | none | D-11, X-22, X-23 |
| CTE root/child key and dummy-key operand builders | 3 | ~60 | J-3/C-5 (range checks), J-4/C-7 (VAR length) | J-13, C-13, D-5 |
| Aggregate-argument metadata lookups (scale, temporal, charset, float display…) | 6 | ~60 | O-10 (only scale looks through GREATEST/LEAST) | D-7, O-10 |
| EXPLAIN condition-tree printer | 3 | ~60 | cosmetic | D-16 |
| NdbScanFilter build-and-attach | 10-11 | ~45 | unchecked return codes at the join/CTE sites | D-9, J-12, F-20 |
| NdbError → exception classification | 2 (+2 tails) | ~40 | SchemaError SEMANTIC vs INTERNAL (latent) | D-14, X-18 |
| CTE output → source column resolution | 5 | ~40 | none | D-12 |
| Column-type family switches / "is integer type" | ~15-16 | ~80 | mostly deliberate policy; some inconsistent | D-13, F-18 |
| Scan-op creation | 3 | ~40 | same NDB error classified differently with vs without an index | X-20 |
| Linked-projection attach loop in `emit_child_ops` | 3 | ~30 | none | J-15, C-14 |
| Small: `quoted_identifier` ×4, `feature_not_implemented`/`require_sch` macros defined twice, STAT_ timing pairs (85 uses), aggregate print + timing ×4, transaction close at each exit | many | ~100 | none | D-17, X-25..X-27 |

Longest executor functions, with the suggested split (D section of `review_duplication.md`):

| Function | Lines | Suggested split |
|----------|-------|-----------------|
| `emit_cte_lookup_filter` | 844 | The 680-line `emit_atom` lambda into IS NULL, column-vs-column, ancestor-vs-constant and CTE-output-vs-constant emitters, plus the DNF driver |
| `execute_join` | 617 | `build_multi_leaf_aggregators`, `emit_cte_subtrees`, `run_aggregating_join` |
| `emit_root_op` | 587 | `emit_cte_root` (with the shared key-operand builder), `emit_pk_lookup_root`, `emit_table_scan_root` |
| `generate_embedded_condition` | 577 | `resolve_case_side`, `plan_case_atom`, `emit_case_atom` on top of the D-10 primitives |
| `build_cte_virtual_tables` | 565 | The D-2 type helper and `populate_virtual_column_sizes`, which would also fix C-3 |

## Testing weak spots

- **T-1: wrong results are recorded as expected output.**
  - `ronsql_compare.inc` runs its mysqld diff with `|| true` unless strict mode is set.
  - `ronsql_fs_smoke`/`ronsql_fs_templates` (plus 4 mirrors) record DECIMAL(18,2) MAX 999999999999999.99 as `1000000000000000`.
  - `ronsql_hopsworks.result` records 6 DECIMAL aggregate blocks computed as double; `ronsql_dbt3_1_2.result:34` and `ronsql_overflow.result:36` are also affected.
  - `ronsql_basic.test` rewrites values on all three outputs before comparing.
  - T-24: 14 of the 16 non-strict files have no recorded difference and can be made strict now.
- **T-2 (verified): `ronsql_jit` results are stale.**
  - `ronsql_jit/r/ronsql_constants.result` lacks the unary-minus sections added in 735863c9086.
  - `subquery`, `subquery_agg`, `cte_case`, `cte_index_body` and `join_agg` are behind too.
  - These tests should fail on the next run.
- **T-3, C-15: the mirror suites miss most targeted regression tests.**
  - `ronsql_jit` lacks 10 tests added since 2026-08-27.
  - `ronsql_cte_jit` lacks 29 of 63 tests, including all `spj_*`, `cte_*` and `count_star`, and the CTE-body ORDER BY/LIMIT test.
  - The ng suites lack 30.
  - The 80-test `ronsql` suite never runs on more than one node group.
- **T-4, X-28, T-17: no NDB error or retry is injected on the single-table paths.**
  - Fault injection exists only for CTE/join.
  - Only one execute-time allocation site is OOM-injected; about 148 arena sites are not.
  - Error-site tally (8900-19170): 34 of 172 user-facing error messages and 4 of 194 NDB-failure sites appear in any test.
- **T-7: silent retries are invisible.** Asserting `attempts=1` from `x-ronsql-phases` inside `ronsql_compare.inc` would harden about 2,100 comparisons with one edit.
- **T-6:** `--sleep 3` after DROP/CREATE INDEX in `ronsql_subquery_agg_ext.test:1354` avoids exactly the 4809 and stale-schema retry paths.
- **T-8:** a stale workaround (`suppress_ronsql_cli`, "CLI does not refresh the dictionary for joins") disables the CLI check in 121 files.
- **T-11:** 44 ORDER BY queries are compared only after sorting both outputs.
- **T-12, output review:**
  - JSON and JSON_ASCII output is never compared with mysqld, and JSON framing per path is untested.
  - EXPLAIN with JSON output, the RDRS default, is untested.
  - The Go tests use ASCII aliases only.
- **T-13:** EXPLAIN cannot show the root choice, so root-path tests can only check results.
- **T-14:** the NULL/NOT/AND/OR matrix exists only for single-table filters, not for join child filters, the PK residual, CTE filters or CASE.
- **T-15:** ENUM and SET columns are never tested; BIT appears in only 2 files.
- **T-16, F-22:** there are no executor unit tests that run without a cluster.
  - Good candidates: `encode_constant`, the ResultPrinter formatting helpers, NDB-error classification and HTTP mapping.
  - Also a test comparing computed word counts against emitted programs.
- **No test reaches any of the verified bugs above.** Missing cases:
  - DECIMAL in a CASE arm, and AVG(CASE).
  - A CTE output layout other than keys-first.
  - A root with a permuted or hash-only PK index.
  - A cross-table OR with a LEFT JOIN, and right-nested cross-table arithmetic.
  - An out-of-range CTE key literal.
  - A NULL or empty scalar subquery in WHERE.
  - The >64-character unaliased output followed by a lexer error.
  - Ring-buffer tables with COUNT(\*).
  - Chained CTEs with string, DECIMAL(p,0) or temporal keys.
- **T-22..T-24, maintenance:**
  - About 3.4 MB of result files are copied across mirror suites.
  - The ng variants are byte-identical to the base suite and could be symlinks.
  - The JIT copies differ only by the pin, which could be made silent.
  - A consistency check should fail when a base test has no mirror.
  - Proposed: one `ronsql_check.inc` that takes the query once, is strict by default, takes optional EXPLAIN greps and `rows=`/`attempts=`, and accepts a known-diff file for known bugs.

## Suggested order of work

1. **Regression tests first**, one MTR case per verified bug, recorded against mysqld in strict mode:
   - X-1: `ronsql_parsing` CLI rejection, plus the ParseOnly inventory.
   - F-1 and D-1: `ronsql_cte_case` or a new `ronsql_case_decimal`.
   - C-1, the CTE output layout: new `ronsql_cte_dd_output_layout`.
   - J-1: permuted PK index root.
   - F-4, C-2, J-3: cross-table filters and the CTE root key.
   - X-3: NULL scalar subquery.
   - O-2: JSON alias escaping, in a Go test.
   - Also refresh the stale `ronsql_jit` results (T-2).
2. **Crash fixes:**
   - X-1: keep the leftmost error in `set_err_state`.
   - F-1: pass `is_first` to the ELSE `AggRepeat`.
   - O-1: carry the chained charset and guard the GROUP BY comparator.
   - C-3: complete the virtual column size switch.
   - F-2.
   - K-1: the per-row loaded-register check in the data node (the rest is bug 14, `8878794d1fd`).
3. **Wrong-result fixes:**
   - C-1: a design decision, either kernel-order virtual tables with a map or rejecting other layouts.
   - J-1: bind constants in index column order.
   - D-1: a single word-size source.
   - F-4, C-2, J-3/C-5, X-3 and D-5.
4. **Test infrastructure:**
   - Strict comparisons by default (T-24), with known-diff files for the T-1 cases.
   - An `attempts=1` assertion in `ronsql_compare.inc` (T-7).
   - Generated or symlinked mirror suites (T-22).
5. **Refactors, each as its own patch after the fixes it overlaps:**
   - Mechanical first: D-15, D-17, D-13, D-10, D-11, D-9 (which also fixes J-5).
   - Then D-2, D-3, D-6, D-8, D-14, D-16, checking the EXPLAIN baselines.

## Fix status

Branches `RONDB-1124-lqh-fixes` (1.1-3.1, merged in #1230) and
`RONDB-1124-lqh-fix2` (from 1.2, rebased onto `RONDB-1124-exec-fixes` and pushed there, #1231). One commit per fix, each with its regression
test. Lanes: 1 = parse and aggregate compile, 2 = join planning and
cross-table filters, 3 = CTE virtual tables and result metadata,
4 = execution paths and retries.

| Lane.step | Finding | Status | Change | Test |
|-----------|---------|--------|--------|------|
| 1.1 | X-1 | fixed | `Context::set_err_state` keeps the left-most error instead of requiring it was recorded first | `ronsql_parsing` (+ `ronsql_jit` mirror): two >64-byte unaliased outputs followed by a lexer error |
| 2.1 | J-1 | fixed | `emit_pk_equality_index_scan_root` binds each index column to the constant of the PK column with the same name | new `ronsql_join_root_pk_index`: hash-only and ordered PK with a permuted index, aggregate, pass-through, residual, mixed key types |
| 3.1 | C-3 | fixed | `build_cte_virtual_tables` sizes Datetime2/Timestamp2/Time2, DECIMAL, BINARY/VARBINARY/LONGVARBINARY and BIT virtual columns as the dictionary does; any other type is refused instead of left at size 0 | new `ronsql_cte_dd_virtcol_sizes` (+ `ronsql_cte_jit` and the four ng mirrors): pass-through child, lookupCte root and single-group root keyed on these types |
| 1.2 | F-1 | fixed | The ELSE arm's `AggRepeat` counts the use of its value only on the first compilation of the CASE, as the THEN arm does | new `ronsql_case_agg` (+ `ronsql_jit` mirror): AVG(CASE) with and without GROUP BY, SUM/COUNT and AVG/SUM sharing a CASE, join aggregate |
| 2.2 | C-2 | fixed | `is_null_rejecting` counts only the LEFT-joined table's columns as NULL; a WHERE condition or cross-table filter still on a LEFT-joined op (CTE_LOOKUP included) is rejected instead of being applied as a join condition | `ronsql_left_join_where`: lj-5/lj-6 (OR on the right side still promotes, real table and CTE), lj-r5/lj-r6 (two-table OR rejected), lj-r7 (CTE IS NULL in OR) |
| 1.3 | D-1 | fixed | The SELECT-list subquery path skips a failed cross-table filter past a DECIMAL column load as 3 words (the load has a precision / scale word), not 2; the CASE-arm part of D-1 was fixed upstream in `c57d796fafa` | new `ronsql_subquery_filter_decimal` (+ `ronsql_jit` mirror): SUM, MIN/MAX, COUNT and merged filtered/unfiltered subqueries over DECIMAL and DECIMAL UNSIGNED |
| 3.2 | O-1 | fixed | `build_result_column_metadata` keeps the charset of a string and the precision of a DECIMAL (scale 0 included) for a chained CTE output; the GROUP BY arm of `compare_rows` requires a charset for string keys, as the aggregate arm does | new `ronsql_cte_dd_chained_keys` (+ `ronsql_cte_jit` and the four ng mirrors): VARCHAR key through two and three CTEs with and without ORDER BY, DECIMAL(10,0) key with and without ORDER BY, DECIMAL(10,2) control |
| 1.4 | F-2 | fixed | `substitute_subquery_results_ce` folds a unary minus over a scalar subquery into the substituted constant (-NULL is NULL), so the single-table path no longer refuses it and the join and CTE paths no longer negate a T_FLOAT through its missing source text; `negate_numeric_constant`, shared with `simplify_ce`, keeps a T_FLOAT without text and refuses -INT64_MIN as MySQL does | new `ronsql_subquery_unary_minus` (+ `ronsql_jit` mirror): join, double minus, INT result, single-table aggregate and pass-through, HAVING, WITH query joined to its CTE |
| 1.5 | K-1 (header) | fixed | `AggInterpreterBase::checkProgramHeader` refuses a bad magic word, reserved flag bits and a program over 1024 words in `checkProgram` (join setup, 20002) and `peekProgramHeader` (Init fails, 1868), which also refuses a header length that differs from the program; `program_rejected()` keeps such a failure from being reported as out of query memory | `testSpjMalformed` P30, P31 (MTR `ndb_spj.test_spj_malformed_p30`, `p31`): leaf 0 with a bad magic word, with a reserved flag bit |
| 1.6 | K-1 (per row) | fixed | `AggInterpreterBase::m_loaded_regs` marks the registers each row loads; an aggregate, `kOpMov` or arithmetic reading another fails the row with 1868 instead of an assert or a stale value, and `READ_AGG_REG_TO_REG` (DBTUP, NULL-row evaluator) fails with 878. The JIT compiler tracks the registers loaded on every path and refuses a program that may read another (`JIT_BRIDGE_REG_NOT_LOADED`) | `testSpjMalformed` P32 (MTR `ndb_spj.test_spj_malformed_p32`, `_p32_jit` with `CompiledInterpreter=ON`): SUM of a register a `kOpSkip` jumps the load of; `bridge_tests` T85a-e, T8/T51f/T58e/T62f/T64d now write their registers with `kOpSetRegNull` |
| 1.7 | X-3 = J-11 | fixed | `fold_where_after_substitution`, run right after the substitution, folds the constants a scalar subquery substitutes into every WHERE structure planned at prepare time (single-table conjuncts, the join root's `join_where_ce[0]`, index-scan root conjuncts) with three-valued logic: a comparison with NULL, also through arithmetic, is UNKNOWN, folded to FALSE under an even number of NOTs and to TRUE under an odd number; XOR is expanded first; IS [NOT] NULL of a constant is TRUE or FALSE. A TRUE conjunct is dropped. A FALSE one sets `m_where_never_true`: a single-table aggregate prepares the result of no rows, and pass-through and join/CTE roots scan with a filter that rejects every row (a scalar CTE root: its dummy-key lookupCte with that filter, as any filtered scalar root; a filtered scalar scanCte hung before the K-2 fix), instead of encoding the NULL as a key or bound | new `ronsql_subquery_null_where` (+ `ronsql_jit` mirror): single-table filter, PK lookup, index bound, NOT, OR, XOR, IN list, IS [NOT] NULL, arithmetic; join root filter, PK key, index bound and residual, LEFT JOIN, aggregates without GROUP BY; new `ronsql_cte_dd_subquery_null_root` (+ `ronsql_cte_jit` and the four ng mirrors): grouped and scalar CTE roots (sn-11: aggregate over a scalar root), real table joined to a CTE |
| 2.3 | F-4 | fixed | `emit_embedded_filter_expr` evaluates first the operand of + - * that needs both registers, as `filter_expr_reg_depth` assumes when it accepts the side, and keeps the left value as the first source so SUB keeps its order; before, the right operand of `l.y * (l.z + 1)` loaded its 1 over `l.y` | new `ronsql_cross_arith_nested` (+ `ronsql_jit` mirror): MUL over ADD and SUB over SUB on the right, nested side left of the comparison, both sides nested, a nullable column inside the nested operand, OR, no GROUP BY, constant minus nested SUB |
| 3.3 | J-3 = C-5 | fixed | `emit_root_op` builds the integer keys of a lookupCte root with `encode_constant` + `query_const_value`, as the real-table root does: a literal outside the key column's domain is refused with the permanent out-of-range error instead of being cast into another key (INT 4294967297 probed key 1), and a MEDIUMINT key is sent as 3 bytes, not 4. Folding such a literal to FALSE as MySQL does is F-12, for every path | new `ronsql_cte_key_range` (+ `ronsql_cte_jit` and the four ng mirrors): INT key hit and miss, pass-through and aggregate; both ends of every integer type's domain through two-column keys; rejection probes just above and below each width, INT 4294967297, BIGINT UNSIGNED -1, aggregate main, two-column key with one literal out of range |
| 3.4 | K-2 | fixed | The scalar arm of `Dblqh::cteScanEmitResults` sets `CTE_SCAN_CONF.numRows` to 1 only when the filter accepted the row; a rejected row was counted, DBSPJ passed the count to the API, and the API waited for the row until the receive timeout | `testCteNdbApiFilter` test 32 (MTR `ndb_push_agg.testCteNdbApiFilter`, `ndb_push_agg_jit` mirror): a scanCte root over SUM(val) without GROUP BY, a filter that keeps the row and one that rejects it |
| 1.8 | E-1 | fixed | `RonSQLPreparer::print(ConditionalExpression*)` prints LIKE, GREATEST and LEAST (arguments in the order written, `print_arg_list`), NULL, a scalar subquery, an IN subquery, EXISTS and a correlated comparison (subqueries by their table, `print_subquery_source`) and the HAVING aggregates; the EXPLAIN output list prints a `SUBQUERY_AGG` output. An operator or output without a case fails the statement instead of calling `abort()` | new `ronsql_explain_conditions`: EXPLAIN FORCE through ronsql_cli with a connection (LIKE, GREATEST of three columns, LIKE and LEAST under NOT, scalar, IN, correlated EXISTS and NOT EXISTS, correlated comparison), without one (EXISTS) and through RDRS (LIKE); prints the WHERE of the parse tree. No `ronsql_jit` mirror: EXPLAIN runs no program |
| 3.5 | D-5 = J-4 = C-7 | fixed | The filtered scalar CTE root (also with a WHERE that is never TRUE, X-3) and the scalar cross-join child build their dummy lookupCte key with one helper, `scalar_cte_dummy_key`, which passes an empty value for a VARCHAR / LONGVARCHAR virtual primary key (the first output) as the child already did (sc-20); the root passed `getSizeInBytes()` zero bytes, which include the length prefix that the generic const operand adds itself, so lookupCte refused the key (QRY_CHAR_OPERAND_TRUNCATED) and the query failed with "Failed to create scalar lookupCte root." | new `ronsql_cte_dd_scalar_root_varchar` (+ `ronsql_cte_jit` and the four ng mirrors): MAX/MIN of VARCHAR(8) and VARCHAR(300) with a string filter that keeps or rejects the row, CHAR(4) control, a filter on a numeric second output, an aggregate over the root, a WHERE never TRUE after a NULL subquery |
| 3.6 | O-2 | fixed | `print_passthrough_row` writes each output name through `print_string` with JSON escaping, as the aggregate printer (`PRINT_STR_JSON`) and the COUNT(\*) shortcut already did: a quote, a backslash or a control character in an alias no longer makes the JSON invalid or reads back as another name, and JSON_ASCII \u-escapes a non-ASCII name | new `ronsql_json_alias_escape` (new include `ronsql_json_check_file.inc`, every body parsed by JSON::PP): pass-through aliases with a quote, a backslash, both (several rows, ORDER BY) and a tab; plain alias, aggregate rows and the COUNT(\*) shortcut as controls; a non-ASCII alias under JSON and JSON_ASCII |
| 4.1 | X-2 | fixed | `is_count_star_only_query` refuses a ring-buffer table, as mysqld does (`ha_ndbcluster::table_flags()` clears `HA_COUNT_ROWS_INSTANT` and `records()` scans when `isRingBuffer()`): the per-fragment ROW_COUNT includes the hidden meta row of each primary key prefix, which every scan skips, so the shortcut counted one row too many per prefix | new `ronsql_count_star_ring_buffer` (+ `ronsql_jit` mirror): COUNT(\*), COUNT(1) and two COUNTs over a ring of four prefixes, one wrapped (EXPLAIN pin: not the fragment-stats plan), the ring's COUNT(\*) as a scalar subquery; controls: GROUP BY id over the ring, a plain table still on the fragment-stats plan |
| 2.4 | F-6 | fixed | `print_result_ordered` applies the cross-table filter sentinel only with GROUP BY: it hides the groups the kernel creates for rows the filter then rejects, but without GROUP BY it dropped the one aggregate row whenever no row passed (MySQL returns COUNT 0 and NULL) | new `ronsql_cross_filter_scalar_agg` (+ `ronsql_jit` mirror): COUNT(\*), SUM, COUNT(col), MIN, MAX and AVG with no row passing, no row reaching the filter, HAVING COUNT(\*) = 0 and > 0; controls with rows passing and GROUP BY with and without a dropped group |
| 1.9 | F-5 | fixed | `simplify_ce` moves a NOT down to a GREATEST/LEAST comparison before `rewrite_minmax_comparison` lowers it (`negate_minmax_condition`: the comparison with the inverted operator, NOT NOT x is x, De Morgan for AND and OR, NOT (x XOR y) is x XOR NOT y), and the XOR expansion forms its NOT x arm the same way, so the lowered form, which is FALSE where a NULL argument makes the comparison UNKNOWN, is used only where no NOT is above it; under NOT, FALSE became TRUE and kept rows that mysqld drops. A comparison that a constant argument makes FALSE (`GREATEST(a, 20) <= 10`, what `NOT (GREATEST(a, 20) > 10)` now becomes) is `a IS NULL AND a IS NOT NULL` instead of the "constant contradiction" error; = and != stay refused | new `ronsql_where_greatest_least_not` (+ `ronsql_jit` mirror): NOT over GREATEST < and >, over LEAST >, over LEAST on the right of the comparison, two of them as aggregates; NOT over a comparison a constant argument makes TRUE; a comparison a constant argument makes FALSE, alone and in an OR; XOR and NOT over XOR; NOT over AND and over OR; NOT over GREATEST on the joined table of a join; controls without NOT and with NOT NOT; = under NOT still refused |
| 3.7 | O-5, O-6 | fixed | One fixed-scale printer, `print_fixed_scale` (`format_fixed_scale`), for every double that is a DECIMAL in MySQL: SUM/MIN/MAX over a DECIMAL with a scale (`print_aggregate_result`), AVG over an exact type (`print_avg_result` and its ORDER BY key `avg_sort_key`), and the CTE outputs of both (`print_passthrough_value`, both GROUP BY column Double arms). It formats as mysqld prints the double result of a pushed SUM or AVG: `double2decimal` (shortest digits), `decimal_round(HALF_UP)`, `decimal2string`. `my_fcvt` and `snprintf("%.*f")` rounded an exact tie to even (AVG 0.03125 printed 0.0312, mysqld 0.0313) and kept the sign of a value that rounds to zero (a DECIMAL sum of -2.8e-17 printed -0.00). FLOAT/DOUBLE results keep their compact form; the precision > 15 gate (O-7) is unchanged | new `ronsql_decimal_round_half_up`, compared with mysqld: AVG(INT) over groups of 32 rows with sums 1, 3, 5, -1 and 32000005 (ties at the fifth decimal) with and without GROUP BY, ORDER BY the AVG ASC and DESC with LIMIT (a 319-row group that prints 0.0313 ties with 0.03125), the same through a CTE (pass-through, GROUP BY key with and without ORDER BY, MIN/MAX); SUM/AVG over a DECIMAL(10,2) group of 0.30, -0.10, -0.20 and an AVG tie at the seventh decimal, with and without GROUP BY, ORDER BY, through a CTE (pass-through, GROUP BY keys, MIN/MAX); controls: sum 3, 7/3, MIN/MAX of the DECIMAL, AVG over a DOUBLE |
| 1.10 | S-2 | fixed | `substitute_subquery_results_ce` marks the whole WHERE and the operands of AND, OR, XOR and NOT as conditions, and `substitute_condition_result` writes a scalar subquery there as TRUE (`NULL IS NULL`) when its result is not zero, FALSE (`NULL IS NOT NULL`) when it is zero and UNKNOWN (`0 <> NULL`) when it is NULL or there is no row, as MySQL's `0 <> value`; the X-3 fold then removes it from the WHERE structures of the plan, so no filter sees it. A correlated subquery used as a condition is refused at prepare time (`reject_correlated_scalar_conditions`, with `has_correlation_predicate` taken out of `decorrelate_scalar`); a string result still fails as non-numeric, and a bare column or arithmetic is still a non-boolean term | new `ronsql_subquery_as_condition` (+ `ronsql_jit` mirror): results 1, 0, NULL, no row, AVG 0.4, DECIMAL 0.40 and 0.00 as the whole WHERE and under NOT, AND, OR and XOR, beside an index bound and a primary key equality; join root filter, never TRUE, OR, index-scan root; grouped and scalar CTE roots; refusals of a correlated subquery (whole WHERE, under NOT in an OR), a string result, a bare column and arithmetic; EXPLAIN of the subquery under NOT and AND |
| 1.11 | S-1 | not a bug | None: `decorrelate_scalar` decorrelates only a top-level WHERE conjunct and folds a NOT around it into the operator, so leaving out the NULL pairs is FALSE only where MySQL drops the row for UNKNOWN | `ronsql_subquery` (+ `ronsql_jit` mirror) tests 69-70: NOT around a comparison with a correlated SUM and MAX whose inner group exists with a NULL value, beside an outer key without a group and a NULL outer column |
| 3.8 | D-4 = F-7 = C-4 | fixed | A grouped CTE body with a cross-table filter ends its program with a `kOpDropZeroCount` trailer (`NdbAggregator::DropZeroCountGroups`) naming the hidden sentinel COUNT; `Dblqh::checkCteReady` erases on the owner, before the AVG divide, the ORDER BY / LIMIT finalize and CTE_READY, every group whose sentinel is 0 (`JoinAggInterpreter::finalizeDropZeroSlice`, CONTINUEB-sliced, ZCONTINUE_CTE_DROP_ZERO_FINALIZE), the groups the kernel creates for rows the filter then rejects; without GROUP BY the one row is kept (F-6). The JIT and the interpreter run the trailer as a no-op per row. One helper, `cross_filter_sentinel_slot`, computes the sentinel slot for the main query and the CTE body and counts the CTE body's Avg slot: with AVG last the sentinel took its slot and the query failed with "Please report a bug" | new `ronsql_cte_cross_filter_groups` (+ `ronsql_jit` mirror): a grouped body where every row of one group fails, read as pass-through ORDER BY, COUNT(\*), MIN/SUM/COUNT, GROUP BY over its COUNT output, JOIN and LEFT JOIN from a real table; AVG as the only aggregate, AVG last, AVG without GROUP BY; without GROUP BY with no row passing (one row kept); body ORDER BY / LIMIT 2 (the drop runs first); controls where every group has a passing row |
| 2.5 | J-5 = C-8 = D-9 = F-20 | fixed | `build_where_filter` builds the WHERE filter of a join or CTE operation (NdbScanFilter begin, `apply_filter`, end, finalise) and `set_where_filter` attaches it (`NdbQueryOptions::setInterpretedCode`), each step checked; a failure throws the NDB error of the filter or the program through `throw_classified_ndb_error`. Used by the CTE body root scan, `emit_index_scan_root`, `emit_pk_equality_index_scan_root` (its checked `setInterpretedCode` kept), the root PK lookup residual, the root table scan and `emit_child_ops` (joined table and CTE_LOOKUP child). Before, a filter whose comparisons fit the 15359-word program buffer but whose last `end()` did not (4294) was dropped: the operation ran unfiltered | new `ronsql_join_filter_errors` (+ `ronsql_jit` mirror): 9 comparisons on a VARCHAR(8000) latin1 column taking 15357 words fail with 4294 at a root table scan, a joined table, a CTE body root scan, an index-scan root, a PK index-scan root and a PK lookup root (before: wrong rows at the first four); controls with the same shapes and one comparison compared with mysqld |
| 4.2 | J-6 = C-9 | fixed | `check_pushed_query_size` counts, at prepare time, the operations `execute_join` builds into one NdbQueryDef: per CTE its subtree operation, its body's operations and the readTuple self-join of a scalar MIN/MAX body, then the main plan (a unique-index lookup counts 2); more than 32 is a LIMIT error that names the count, where the builder's QRY_DEFINITION_TOO_LARGE at the operation that crossed the limit was an internal error with code 0 ("Failed to create child operation.", or "Failed to create CTE body scan root." after an unchecked `beginCteSubtree`). Every NdbQueryBuilder call goes through `require_qb`, which raises the builder's own error with its code (`throw_query_builder_error`): QRY_UNRELATED_INDEX (4809) reloads the schema and retries if it changed for every operation, root and CTE-body index scans included (before: child operations only); 4812 is LIMIT, 4000 RESOURCE, a schema or temporary error as for a transaction, anything else INTERNAL. `beginCteSubtree` and `endCteSubtree` are checked | new `ronsql_join_builder_errors` (+ `ronsql_jit` mirror): 10 CTEs, a primary-key lookup and 10 CTE lookups, 32 operations, compared with mysqld; one more primary-key lookup (33), a unique-index lookup counted 2 (33), 17 CTEs (52): limit errors through ronsql_cli and RDRS (`[limit]`). No SQL statement alone reaches a builder error after the prepare check: 4809 needs a cached table and index from either side of a concurrent DROP / CREATE INDEX, and `beginCteSubtree` fails only past the node limit |
| 4.3 | X-14 = O-17 (1/3) | fixed | Sweep 1 of 3, the printer: the 28 `ndbrequire` and `abort()` of `ResultPrinter.cpp` (precision and scale of a GROUP BY output, AVG scale, output format and output type switches, program command switches, `double_to_fixed`, `format_fixed_scale`, `avg_arg_scale`) throw a permanent INTERNAL error through the new `ronsql_bug` / `ronsql_require_bug` of `RonSQLCommon.hpp` instead of aborting RDRS. A TEXT GROUP BY key passed up two CTEs reached the precision check (the chained metadata keeps the TEXT inline size 256 as a precision); it now fails with "A GROUP BY output has a precision above 65. Please report a bug." The `abort()` of `ronsql_cli.cpp` are kept | new `ronsql_internal_errors`: ie-1 the chained TEXT key through ronsql_cli, ie-2 the same through RDRS (HTTP 500, class internal) followed by a query RDRS still answers |
| 4.4 | X-14 (2/3) | fixed | Sweep 2 of 3, the aggregate compiler: the 48 `ndbrequire` and 8 `abort()` of `AggregationAPICompiler.cpp` (expression and aggregate construction, constant folding, the symbolic VM, register allocation, the compile-time correctness check, dead code elimination, the program printer, `raw_word_size`) and its `require_status` and `require_reg` macros, and the two `ndbrequire` of `RonSQLListBuilder::append` in `RonSQLPreparer.hpp`, throw a permanent INTERNAL error through `ronsql_bug` / `ronsql_require_bug`; none kept | none new: no statement reaches them. Covered by every aggregate test of the `ronsql`, `ronsql_jit`, `ronsql_cte*` and `ronsql_fs*` suites (`ronsql_case_agg` for CASE, the EXPLAIN tests for the program printer) and by `ronsql_oom_test` for the list builder |
| 4.5 | X-14 = J-10 (3/3) | fixed | Sweep 3 of 3, the preparer and executor: 120 of the 129 `ndbrequire` and `abort()` of `RonSQLPreparer.cpp` throw a permanent INTERNAL error through `ronsql_bug` / `ronsql_require_bug` (the J-10 sites `resultAgg`, `execute_passthrough_drain`, `emit_child_ops`; the X-14 sites: missing SubqueryInfo, the state checks of the constructors, `get_accessed_*` and `execute()`, and the rest); `NdbQueryBuilder::create()` returning NULL throws `std::bad_alloc` (RESOURCE); the `catch (...)` of `handle_ronsql_exception` closes the transaction and fails the statement. Kept with a comment: the seven `err_stream` checks, `cleanup_trans()`'s `ndbrequire(ndb)` and the one in `handle_ronsql_exception`. The `EXECUTING`/`DONE` state machine is not done | none new: no statement reaches them. Covered by the `ronsql`, `ronsql_jit`, `ronsql_cte*`, `ronsql_fs*` and `ronsql_large` suites and `ronsql_internal_errors` |
| 4.6 | X-14 (4/4) | fixed | Sweep 4, the lexer, the parser and RDRS: the identifier length check of the lexer's `[a-z_]{1,18}` rule returns `LEX_TOO_LONG_IDENTIFIER` instead of aborting; `rsqlp_realloc`, the `default` of `cond_expr`'s aggregate switch and the third case of `rsqlp_error` throw a permanent INTERNAL error through `ronsql_bug`; RDRS's `ronsql_op` answers an exception of an unexpected type and the end of its retry loop as an INTERNAL `RonSQLPermanentError` (HTTP 500, class `internal`; `ronsql_op_internal_error`), and `RonSQLRequest::execute` an unknown output format with HTTP 500, class `internal`. None kept but the `abort()` of `ronsql_cli.cpp` and `buffer_manager.cpp` (startup) | none new: no input reaches them. Covered by `ronsql_parsing`, `ronsql_keyword_identifiers`, `ronsql_having_refs`, `ronsql_parser_cte`, `ronsql_parser_oom`, `ronsql_rdrs_basic`, `ronsql_operation_id`, `ronsql_internal_errors` and `rdrs2-golang.ronsql`, and by the ctests `ronsql_parser_oom_test` (lexer allocations, the parser depth limit through `rsqlp_error`, `ronsql_op`), `ronsql_parser_list_stack_test`, `ronsql_parser_access_test`, `KeywordsUnitTest` and `OverflowUnitTest` |
| 3.9 | X-14 (RONSQL-39 chained) | fixed | `check_cte_groupby_key_types`, at prepare time after `validate_cte_execution_shapes`, refuses a GROUP BY key of a CTE body or of the main query that is a CTE output whose type, followed down the CTE chain by `resolve_chained_column_type`, is BLOB or TEXT, with the error `GroupBy()` / `GroupByLinked()` give a stored one: "GROUP BY on a BLOB or TEXT column is not supported." (unsupported, HTTP 400; the message is shared through `GROUPBY_BLOB_UNSUPPORTED`). Before, two CTEs up the printer took the TEXT inline size 256 as a DECIMAL precision ("A GROUP BY output has a precision above 65. Please report a bug.", internal, HTTP 500), and one CTE up the statement failed at execute time on the key's virtual column (C-3's "CTE output column ... has a column type that CTEs do not support."). Stored keys and a TEXT column passed through a CTE without a GROUP BY on it are unchanged | `ronsql_internal_errors` renamed `ronsql_groupby_blob_chained` and rewritten: TEXT and BLOB keys one and two CTEs up, in the GROUP BY of the main query (gbc-1..4) and of a CTE body (gbc-5..8), through ronsql_cli; gbc-3 through RDRS (HTTP 400, class unsupported, then HTTP 200); a TEXT column passed on without GROUP BY keeps its C-3 error (gbc-10); controls compared with mysqld: VARCHAR key one and two CTEs up, INT key three CTEs up, pass-through of a grouped CTE. No `ronsql_jit` mirror: the error comes at prepare time |
