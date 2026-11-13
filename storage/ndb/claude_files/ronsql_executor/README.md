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
- **X-14 = J-10 = O-17: invariant checks in the executor and printer abort RDRS on API-side conditions.**
  - There are about 108 `ndbrequire` and 26 `abort()` calls, including `ndbrequire(qb != NULL)` when out of memory.
  - In a long-running server they should throw an internal error instead.

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
- **D-4 = F-7 = C-4 (likely): a CTE body with a cross-table residual filter leaves phantom groups.**
  - The kernel creates the group before the filter stops the row, and only the main query's printer suppresses sentinel-0 groups.
  - Both sentinel-slot computations (8682, 18180) skip AVG, so an AVG in such a body collides with the sentinel.
- **S-1 (possible; found while fixing X-3): correlated scalar substitution skips the NULL pairs.** That is right in a positive position and wrong under NOT, and `decorrelate_scalar` accepts negated comparisons. Not checked.
- **F-6: a cross-table filter on an aggregate without GROUP BY returns no row instead of `0`** (`ResultPrinter.cpp:1163-1186`). Example: `SELECT COUNT(*) FROM o JOIN l … WHERE l.q > o.c + 1000000`.
- **F-5: the GREATEST/LEAST rewrite in WHERE is wrong under NOT or XOR with a NULL argument.** The added IS NOT NULL guards turn UNKNOWN into FALSE.
- **C-6, O-3, O-10 = D-7: CTE MIN/MAX of temporal columns stay packed integers.**
  - In CTE filters (`c.d >= 19950101` compares packed values).
  - In GROUP BY over the CTE output (prints `1036389`).
  - In GREATEST/LEAST display.
- **F-8, F-9, F-10, F-11: type handling of constants.**
  - Cross-table filter leaf columns are loaded as raw integer bits.
  - FLOAT columns compare as float in WHERE but as double in CASE.
  - A float subquery result is rounded to the DECIMAL column's scale.
  - Temporal literals are truncated to the column's fractional precision.
- **X-2 (likely): the COUNT(\*) shortcut counts the hidden meta rows of ring-buffer tables.** mysqld skips the fast path for `isRingBuffer()`.
- **X-4: subquery results go through text.**
  - `int_col > (SELECT AVG(…))` always fails.
  - BIGINT UNSIGNED above 2^63 fails.
  - DECIMAL beyond about 15 digits loses precision.
  - `''` is treated as NULL.
- **Output vs mysqld:**
  - O-2 (verified): pass-through JSON does not escape column names (`ResultPrinter.cpp:1698`); `` `x"y` `` gives invalid JSON. cfd835ae18d fixed only the operationId sibling.
  - O-4: DOUBLE values of 1e15 or more print in fixed point.
  - O-5: negative zero.
  - O-6: AVG rounds ties to even.
  - O-7: DECIMAL with precision above 15 drops its scale (recorded as expected output, see T-1).
  - O-8: HAVING is evaluated in DOUBLE (BIGINT above 2^53, DECIMAL equality, temporal and string MIN/MAX).
  - O-9: ORDER BY SUM(DECIMAL) ties.
  - O-11: duplicate JSON keys.

### Valid queries that fail, and error handling

- **X-3 = J-11 (verified): an empty or NULL scalar subquery in WHERE, a bound or a key fails.**
  - The substituted `T_NULL` (9517) reaches `encode_constant`, which throws `RonSQLMaybeStaleSchema`.
  - The schema is reloaded and the request fails with 400/500; MySQL returns zero rows.
  - The parser review fixed only HAVING.
- **S-2 (found while fixing X-3): a scalar subquery used directly as a condition, `WHERE (SELECT …)`, fails with "Non-boolean term" whatever its result.**
- **K-2 (reproduced while fixing X-3; fixed): a scalar CTE scanned with a filter that rejects its row hangs the query.**
  - The scalar arm of `Dblqh::cteScanEmitResults` (`DblqhMain.cpp` ~23444) sets `numRowsSent = 1` after its filter gate, also when the filter rejected the row. The comment in the reject branch says it stays 0.
  - `Dbspj::execCTE_SCAN_CONF` adds `numRows` to the rows it reports to the API (`DbspjMain.cpp:8869`), so the API waits for a row that never comes until the receive timeout ends the query.
  - Found by the first version of X-3, which scanned a scalar CTE root whose WHERE is never TRUE with a reject-all filter (sn-6). RonSQL no longer builds the shape: every filtered scalar CTE root is a dummy-key lookupCte. A raw NDB API client still can. `testCteNdbApiFilter` test 14 rejects every group of a grouped CTE only.
  - Fixed: the scalar arm counts the row only when the filter accepted it, as the scalar agg-feed arm of `cteScanAggFeed` already did. Test: `testCteNdbApiFilter` test 32 (MTR `ndb_push_agg.testCteNdbApiFilter`, `ndb_push_agg_jit` with the JIT on): SUM(val) of `cte_src` as a scanCte root, a filter that keeps the row (one row of 150) and one that rejects it (no row).
- **D-5 = J-4 = C-7: a filtered scalar CTE root with a VARCHAR first output fails.** The root dummy-key copy (12713) lacks the sc-20 fix that its child copy (15385) has. Fixed: both now use `scalar_cte_dummy_key` (Fix status 3.5).
- **J-5 = C-8 = D-9 = F-20: some sites ignore the return codes of `filter.end()`, `finalise()` and `setInterpretedCode()`.**
  - Six join/CTE sites ignore them; a failure silently drops the WHERE filter.
  - The single-table copies do check.
- **J-6 = C-9: builder errors are lost.**
  - `require_run` classifies from `m_trans->getNdbError()`, never from `qb->getNdbError()`.
  - The 32-node tree limit is not checked before building.
  - The `beginCteSubtree` result is ignored.
  - QRY_UNRELATED_INDEX (4809) triggers a schema reload only for child operations, not the root.
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
   - Builder and dictionary errors are not consulted (J-6, X-9).
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
cross-table filters, 3 = CTE virtual tables and result metadata.

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
