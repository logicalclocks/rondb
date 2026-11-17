# RonSQL executor review F: test coverage map and weak spots

Worktree `/Users/mikael/mysql_trees/rondb_2610_main`, branch 26.10-main, HEAD 50eb491eb3f.
This was a read-only review: no repository file was edited, and nothing was built or run.
Line numbers refer to `storage/ndb/src/ronsql/RonSQLPreparer.cpp` unless another file is named.

## Method

- **Executor paths.** I listed the paths from the dispatch in `execute()` (9580), `execute_join` (11291), `emit_root_op` (12599), `emit_child_ops` (15097), the pass-through drains (10594, 11920), the CTE builders, the filter emitters and `handle_ronsql_exception` (15618).
- **Tests examined.** I mapped those paths against:
  - every MTR suite whose name starts with `ronsql`, plus `rdrs2-ronsqltpch`;
  - the Go tests in `rest-server2/server/test_go/internal/integrationtests/{ronsql,ronsqltpch}`;
  - the C++ tests in `rest-server2/server/test` (`ronsql_oom_test`, `ronsql_parse_only_test`);
  - the two unit tests in `src/ronsql`.
- **Signals.** I used several signals:
  - EXPLAIN substrings, which are grepped in `.test`/`.inc` files because `ronsql_explain.inc` writes EXPLAIN output to a file;
  - `rows=`/`attempts=` from `x-ronsql-phases`;
  - error text and `Error handling: ...` markers recorded in `.result` files;
  - query shapes.
- **Scripts.** The analysis scripts are in `.../scratchpad/executor_review/scripts/`:
  - `errsites.py` lists every error site in lines 8900–19170 and greps its message across all tests. Its output is `errsites.tsv`.
  - `diffs.py` finds diffs that `.result` files record as the expected output.
  - `orderby.py` finds ORDER BY queries that are compared only after sorting.
  - `expdrift.py` checks the compared query against its "Expected result" copy.
  - `limit_noorder.py` finds compared LIMIT queries that have no ORDER BY.
  - `sig.py` counts EXPLAIN signals.

## Overall picture

**Strengths.** The newer data-driven suites test each feature well. Each case asserts:
- the values, against mysqld with a strict diff;
- the chosen plan, through EXPLAIN greps;
- the drain behaviour, through `rows=` and `fetched=`.

These suites are `ronsql_cte/include/body_*.inc`, `ronsql_in_list_*`, `ronsql_cte_*` in `ronsql`, `ronsql_large_*` and `ronsql_fs_*`.

**Weaknesses.** They are in five areas:
- the older tests, which use non-strict compares and symmetric canonicalization;
- the mirror suites, where results and the list of mirrored tests have drifted;
- NDB error, retry and stale-schema handling outside the CTE/join paths;
- JSON framing on each path;
- visibility of decisions the executor makes at run time.

**Error-site tally** (`errsites.py`, lines 8900–19170):

| Error site class | Sites | Message found in any test |
|---|---|---|
| User-facing permanent errors (`require_prm`, `require_unsupported`, `throw RonSQLPermanentError`, `feature_not_implemented`) | 172 | 34 |
| Internal-consistency guards | 73 | 1 |
| NDB/API failure sites (`require_run`, `require_sch`, `throw_classified_ndb_error`, `throw_key_read_error`) | 194 | 4 |
| `ndbrequire`/`abort` | 190 | 2 |

The heuristic is rough: many "user-facing" sites are really internal guards. The NDB/API row is the important one. Without fault injection, those sites can only be reached by kernel error inserts, and all such inserts target CTE/join shapes.

## Coverage table

Confidence key:
- **H**: an EXPLAIN grep or phase counter pins the path, and values are compared strictly.
- **M**: only the results show the path was taken, or only some sub-branches are covered.
- **L**: indirect or partial.
- **NONE**: no test hits the path.

### Dispatch in `execute()` (9580–9714)

| Path | Tests | Conf. |
|---|---|---|
| EXPLAIN TEXT/TEXT_NOHEADER → `print()` | `ronsql_explain.inc` users (~30 files), `ronsql_rdrs_basic`, `ronsql_parsing` (no NDB), `ronsql_parser_cte` | H |
| EXPLAIN JSON / JSON_ASCII → `feature_not_implemented` (9601–9605) | none. `ronsql_rdrs_basic.test:94,99,111,121` add `"outputFormat":"TEXT"` with a "todo" to avoid it | NONE |
| `execute_subqueries` + substitute (9014–9578) | `ronsql_subquery` (59 cmp), `_agg` (29), `_agg_ext` (43), `ronsql_cte_subquery`, `join_where_classify` jw-5, Go `TestRateLimitSubquery` | H |
| ↳ >1000 correlated rows (9092), >1000 IN values (9139), non-numeric IN result (9130), NULL in IN result outside AND/OR/NOT (9487) | — | NONE |
| ↳ results ≥2^63 (BIGINT UNSIGNED), DECIMAL >2^53, 17-digit DOUBLE, DATE | DECIMAL(10,2)/DOUBLE small values only (`cte_subquery` Tests 7–8); DATE marked NEXT-PHASE (`:355`) | L |
| `execute_join` | most of `ronsql_cte`, `ronsql_join*`, `ronsql_cte_*`, `ronsql_large_*`, `ronsql_fs_*` | H |
| `execute_api_side_aggregate` (J5, last N) | `body_lastn_agg.inc` (5 expl), `ronsql_cte_feature_store_docs` | H |
| ↳ sort-buffer cap (10511–10519) | — | NONE |
| `execute_single_table_passthrough` | `body_passthrough_single_table` (14 expl, 9 rows), `_orderby`, `_orderby_index` (16 expl, 20 rows), `_limit`, `ronsql_in_list_lookups` (15 expl), `binary_passthrough`, `temporal_json` | H |
| ↳ PK lookup ± residual, IN list, LIMIT early close (`closeNoWait`), client sort, SF_OrderBy | the files above (poi-1..19, st-*, IN-list cases) | H |
| ↳ NDB error before or after output (10818–10870, 11058) | — | NONE |
| `is_count_star_only_query` / `execute_count_star_shortcut` (9842–9912) | `body_count_star.inc` (2 expl, 12 cmp; runs in the ng variants, not in `ronsql_cte_jit`) | H |
| `execute_pk_lookup_aggregate` (F1b, 9717–9840) | `ronsql_in_list_agg_lookups` (8 expl, 14 cmp, 1860 via `ronsql_sum64_check.inc`) | H on 1 NG only |
| Scan aggregate, table scan | `ronsql_basic`, `ronsql_hopsworks` (78 expl), `emptytable_and_nulls`, many | H |
| Scan aggregate, index scan / multi-range / bound that excludes NULL (10067–10104) | `hopsworks` (188 expl), `in_list_ranges` (16 expl), `body_nullable_bounds` (8 expl) | H |

### Roots of joins and CTE bodies

| Path | Tests | Conf. |
|---|---|---|
| Real root `scanTable`, with and without an interpreted filter (13160–13190) | `body_filter`, `ronsql_join_agg`, many | H (but EXPLAIN shows `[ROOT] TABLE_SCAN` for every real-table root decision) |
| INDEX_SCAN root, `emit_index_scan_root` (12337) incl. IN multi-range | `body_main_root_index` (8×`[ROOT] INDEX_SCAN`), `ronsql_in_list_spj` | H |
| `readTuple` root (PK covered, no scan child) ± residual | `body_root_pk_residual` rpr-7/8/13/14, `ronsql_cte_pk_join` | M (not visible in EXPLAIN) |
| `emit_pk_equality_index_scan_root` (12529) | rpr-3/4/12 | M |
| Hash-only PK fallback; residual over 64 words → fallback (13110–13150) | rpr-11/12/15 | M |
| F19 `outer_agg_needs_scan_root` (12620) | `cte_lookup_root_outer.test` (not in the jit/ng mirrors) | M |
| CTE_SCAN root (12640), scalar-CTE filter, unsupported outer join (12686) | `ronsql_cte_scan`, `body_passthrough_scalar_cte`, `body_passthrough_groupby_cte` | H |
| `lookupCte` root (root PK covered by constants, 12740–12990) | `ronsql_cte_root_lookup`, rpr-5/6/16 | M. The "Expected integer constant" errors (12909–12939) are NONE |
| CTE body roots: TABLE_SCAN (11623), INDEX_SCAN (11659), I.10 MIN_ASC/MAX_DESC maxRows=1 (11677), CTE_SCAN chain (11717), multi-op body (11724) | `body_index` (7 I.10 greps), `ronsql_cte_minmax_index`, `spj_state_s1`, `ronsql_cte_chained`, `body_chain_scalar`, `ronsql_cte_body_join_explain`, `spj_ronsql_s6b` | H |
| `FRAGS_PER_WORKER` root option | `body_frags_per_worker` (1 expl) | M (the effect cannot be observed) |

### Child operations (`emit_child_ops`)

| Path | Tests | Conf. |
|---|---|---|
| PK_LOOKUP INNER/LEFT | `body_nest_semantics`, `body_passthrough_snowflake`, `spj_ronsql_e3/e4/e6/x1`, `spj_state_s15` | H |
| UNIQUE_LOOKUP INNER | `body_passthrough_snowflake` (1 expl) | M |
| UNIQUE_LOOKUP LEFT; UNIQUE_LOOKUP under aggregation | — | NONE |
| INDEX_SCAN child, with and without child-local bounds (15260–15333) | `body_child_bounds` (12 expl, `Residual filter: yes`), `spj_ronsql_e7`, `ronsql_cte_body_join_explain` | H |
| CTE_LOOKUP INNER/LEFT | `ronsql_cte_basic`, `body_joins`, `spj_ronsql_e5/e23`, `spj_state_s5/s6` | H |
| CTE_LOOKUP ANTI (Phase K, 4052–4058, 15112) | `ronsql_cte_basic` Tests 28–30 (values only, no `[ANTI]` grep anywhere) | M |
| CTE_SCAN child (cross join, 15466–15482) | `body_passthrough_scalar_cte`, `ronsql_cte_scalar` | M |
| SEMI_JOIN (15108) | the planner never emits it | NONE (dead code) |
| `QRY_UNRELATED_INDEX` 4809 → RMS retry (15487–15503, RONDB-1036) | none. `ronsql_subquery_agg_ext.test:1354` sleeps 3 s precisely so that it is avoided | NONE |
| No index on the join column → reject | Go `status_test.go` "No suitable index" | H |

### CTE machinery

| Path | Tests | Conf. |
|---|---|---|
| `build_cte_virtual_tables` type/width matrix | `body_dtwide` (21 cmp), `body_agg`, `body_avg`, `ronsql_cte_decimal` | H |
| ↳ SUM/AVG over scale-0 DECIMAL wider than 64 bits (13291, 13482, 13679, 13892) | — (only the MIN/MAX variant is hit) | NONE |
| Single-row CTE (11580) | `body_single_row_cte` (3 expl) | H |
| ↳ more than `MAX_GROUPBY_COLS` outputs (11591) | — | NONE |
| `emit_cte_lookup_filter` supported forms | `ronsql_cte_or`, `_colvscol`, `body_filter` (57 cmp), `greatest_least_v*` | H |
| ↳ rejection branches: >N OR disjuncts (14134), IS NULL on a LEFT-JOIN RHS (14251), unsupported atom (14273), cross-CTE column (14563), MIN/MAX of unsupported type (14746), unsupported aggregate output (14757) | 6 of the 60 sites have tests | L |
| `emit_cte_orderby_limit` | `body_orderby_limit_cte` (3 expl) | H; 2 of 8 errors |
| Last-N / flatten / collapse / API aggregate rewrites | `body_lastn_agg`, `ronsql_cte_single_group_flatten` (7 expl), `body_single_group_cte`, `ronsql_cte_collect_collapse`, `feature_store_docs` | H |

### Filter and expression emitters

| Path | Tests | Conf. |
|---|---|---|
| `apply_filter` (NdbScanFilter), single-table scan | `emptytable_and_nulls` (3-valued-logic matrix over NULL data, 232–291), `ronsql_filtering` | H |
| `apply_filter` as join child filter, PK-lookup residual, pushed-join root filter | many value tests; none with the NULL/NOT matrix | M |
| LIKE | 5 files | M |
| NOT LIKE | 0 | NONE |
| `generate_embedded_condition` (CASE, GREATEST/LEAST, cross-table filter) | `ronsql_cte_case`, `greatest_least*`, `ronsql_join_agg` | H; 1 of 16 errors |
| `emit_embedded_filter_expr` (+ − * in cross-table WHERE) | `ronsql_join_agg` Tests 64–66, `subquery_agg_ext` | M |
| Cap of 32 OR atoms in `generate_embedded_filter_condition` | `join_where_classify` jw-or-31/32/33 | H |
| `encode_constant`: DECIMAL vs out-of-range int or float literal (16505, 16514) | — | NONE |
| `simplify_ce` DATE_ADD/INTERVAL errors (16766–16855) | — | NONE |
| `rewrite_minmax_comparison` errors (17022–17094) | — | NONE |

### Output formats

| Path | Tests | Conf. |
|---|---|---|
| TEXT | everything | H |
| TEXT_NOHEADER | `ronsql_cli_silent`, inner subqueries, Go status/avro | M |
| JSON | `rdrs_basic` (single-table GROUP BY), `temporal_json` (aggregate + PK pass-through), `binary_passthrough` (PK/scan pass-through), `cte_park_sweeper` (CTE pass-through), fs vectors/fuzz via rondb-cli (`--skip` when the binary is missing) | M |
| JSON framing of empty / LIMIT 0 / early-close / sorted / PK-miss / join-aggregate / count(*) results | — | NONE |
| JSON_ASCII | `rdrs_basic`, `operation_id`, Go `TestAvroJSONASCII` | L |
| Avro | Go `avro_test.go` (3 formats, 5 errors) | M. The CLI rejection (8958) is NONE |

### Errors and robustness

| Path | Tests | Conf. |
|---|---|---|
| RPE classification | 52 result files | H |
| RMS→RPE (schema unchanged) | `rdrs_basic` (ALTER), `avg_sum_types`, rpr-P3/P4, st-20 | H |
| RMS→RRE (schema changed, retried) | ALTER in `rdrs_basic` only, and the retry is invisible | L |
| SRE,te → RPE (1860) | `ronsql_overflow`, `ronsql_sum64` / `in_list_agg_lookups` via `sum64_check` | H |
| Temporary error before output → retry (CLI ×3 and RDRS ×10 with backoff) | `cte_park_sweeper` (CLI + RDRS `attempts=2`), `cte_nodefail_*` (ng2r2), `mem_leak_errors` e5125/e5139 | H for CTE/join; NONE for single-table paths |
| Temporary error after output (`,od`, 15596/15753) | `mem_leak_errors` e5129 accepts either outcome | L |
| Retry budget exhausted → 503 | `mem_leak_errors` e5149a | M |
| Rate limit → 429 | Go `ratelimit_test.go`: scan aggregate, pass-through scan, join, IN-subquery | M. PK-lookup aggregate (15589 branch), PK pass-through, count(*), CTE, API aggregate are NONE |
| `bad_alloc` → RESOURCE | `ronsql_oom_test` (only `DECIMAL_BUFFER` reaches execution) | L. The other ~148 arena sites are NONE |
| Join `nextResult` SchemaError → RMS (11871, 12283); `unload_schema` failure branches (16030–16076); `throw_key_read_error` SchemaError branch (15569) | — | NONE |

## Findings

Ordered by importance. Severity reflects how likely a real regression is to slip through.

### T-1 (High) Wrong results are recorded as expected baselines

`ronsql_compare.inc` runs the diff with `|| true` unless `$strict_diff` is set, so the diff is written into the `.result` file. Canonicalization also runs on all three outputs, so it can rewrite a wrong value into the right one. Current cases:

- **`ronsql_fs_smoke.result:391,461,579` and `ronsql_fs_templates.result:1493`.**
  - DECIMAL(18,2) `MAX` of 999999999999999.99 is recorded as `1000000000000000`.
  - `10.50` is recorded as `10.5` (F2, F5).
  - Four mirrors carry these lines too.
- **`ronsql_hopsworks.result:6820,7219,8284,8835,8935,9334`.** SUM/AVG/MIN/MAX over DECIMAL(30,10) and DECIMAL(65,30) are computed in double. For example, SUM `300000000000000000061.4999999997` is recorded as `300000000000000000000`.
- **`ronsql_dbt3_1_2.result:34`.** DECIMAL arithmetic is evaluated in double, giving `26777.986559999998`.
- **`ronsql_overflow.result:36`.** `0.000000000000000000000000000000` is recorded as `0`.
- **`ronsql_basic.test:210`** has `--let $canonicalization_script=...;s/2524649312/2524649310/;`. It rewrites a SUM that is off by 2 into the MySQL value on every side, and it truncates all decimals to one digit.
- **`ronsql_basic.test:120`** rounds every number to 2 decimals.
- **`ronsql_basic.test:166`** maps `-0` to `0`.

These tests pass while the bugs are present. A change in the wrong value shows up only as "re-record".

**Fix.** Make `ronsql_compare.inc` strict by default. Add `$known_diff=<file>`, a checked-in expected-diff file named after the bug ID. The test then asserts that the diff equals that file exactly and echoes `KNOWN-DIFF F5`. Delete the value-rewriting canonicalization in `ronsql_basic`. Turn it into a known-diff probe that names the bug.

### T-2 (High) Six `ronsql_jit` results are stale

The mirrors `--source` the originals, but their results were last refreshed between 2026-08-27 and 2026-10-28. The originals changed on 11-03 and 11-05. The mirrors lack whole sections:

- `ronsql_join_agg` lacks Tests 64–66.
- `ronsql_cte_case` lacks Test 12 onward.
- `ronsql_cte_index_body` lacks the Boolean-collection cases.
- `ronsql_subquery` lacks Test 48.
- `ronsql_subquery_agg` lacks the HAVING IN alias case.
- `ronsql_constants` lacks the unary-minus block and has an old overflow message.

These 6 tests will fail on the next `--suite=ronsql_jit` run, or that suite is not being run.

**Fix.** Run `storage/ndb/claude_files/compiled_interpreter/refresh_jit_mirror_results.py` and re-record the pins. Then see T-22 for how to stop this recurring.

### T-3 (High) The JIT and topology arms miss most targeted regression tests, and the `ronsql` suite never runs on more than one node group

**Unmirrored tests.**
- `ronsql_jit` lacks 10 executable tests, all added after the mirror was created on 2026-08-27: `agg_drain`, `avg_sum_types`, `cte_body_join_explain`, `groupby_blob`, `groupby_date`, `having_refs`, `join_where_classify`, `keyword_identifiers`, `left_join_where`, `temporal_json`.
- `ronsql_cte_jit` lacks 29 of the 63 `ronsql_cte` tests: every `spj_*` and `cte_*` regression, plus `dd_count_star` (count(*) runs an interpreted program), `cte_dag`, `orderby_limit_cte`, `outer_chain_miss`, `single_group_cte` and `feature_store_docs`.
- The four `ronsql_cte_ngXrY` suites lack 30 tests. These include `cte_redist_pages`, `cte_scan_batches`, `cte_lookup_root_outer` and `cte_null_key_outer`, the tests most sensitive to topology.

**Single node group only.** All 80 `ronsql` tests run only on 2 data nodes in 1 node group. That includes the 37 `ronsql_cte_*.test` files, the subqueries, the IN-list PK lookups and aggregates, `join_agg`, `orderby` and `agg_drain`.

Several paths depend on the fragment count or the topology:
- the count(*) per-fragment sum;
- the PK-lookup partial merge;
- the double SUM merge order (F21: `6.0600000000000005` on 2 node groups vs `6.06` on 1);
- multi-range IN with SF_MultiRange;
- the SF_OrderBy merge.

Outside the `ronsql_cte` dd families and the `ronsql_fs` mirrors, these are untested on more than one node group.

**Fix.** Add a `ronsql_ng2r2` arm. At minimum it should cover `in_list_*`, `subquery*`, `join_agg`, `emptytable_and_nulls`, `orderby`, `orderby_stress`, `agg_drain` and `count_star`. Generate all mirrors from a list (T-22).

### T-4 (High) NDB errors and retries are never driven through the single-table executor paths

Every fault-injection test targets CTE or join shapes: `cte_park_sweeper`, `cte_nodefail`, `ronsql_large_mem_leak_errors`. No test raises a temporary or permanent NDB error in:
- `execute_single_table_passthrough` (10818–10870 PK lookup; 10973–11058 scan);
- `execute_pk_lookup_aggregate` (9799–9830 → `throw_key_read_error` → `throw_classified_ndb_error`);
- `execute_count_star_shortcut` (9895–9903);
- the scan aggregate `DoAggregation` (9705);
- `execute_api_side_aggregate`.

The `,od` verdict (an error after rows reached `out_stream` must not be retried) is checked only by e5129, which accepts both outcomes. Only 4 of the 194 NDB/API error sites have their message in any test.

**Tests.**
- (a) Add `ronsql/t/ronsql_single_table_faults.test` (needs `have_debug` + `have_ndb_debug`). Arm a one-shot temporary scan or LQHKEY error insert on every data node. Run, through RDRS with `x-ronsql-phases`:
  - `SELECT SUM(l_quantity) FROM lineitem`;
  - `SELECT o_orderkey FROM orders WHERE o_orderkey IN (1,2,3)`;
  - `SELECT SUM(o_totalprice) FROM orders WHERE o_orderkey IN (1,2,3)`;
  - `SELECT COUNT(*) FROM orders`;
  - `SELECT l_orderkey, l_linenumber FROM lineitem ORDER BY l_orderkey`.

  Data is `cte_data.inc` (lineitem has 3000 rows). Assert that the result equals mysqld and that `attempts=2`.
- (b) Make the error fire on the second SCAN_NEXTREQ of `SELECT l_orderkey, l_linenumber FROM lineitem`, which is unsorted and spans more than one batch per fragment. Assert a non-200 response and no retry (`attempts=1`). On the CLI with `2>&1`, assert the `,od->RPE` marker.

If no suitable kernel insert exists, add an inert `RonSQLExecParams::executor_fault_hook {site, after_rows, ndb_code}`, modelled on `RonSQLAllocationTestHook`. Drive it from a new `rest-server2/server/test/ronsql_executor_fault_test.cpp` that sweeps every executor path without a debug data node.

### T-5 (Medium-High) Rate limiting is untested for five executor paths

Go `ratelimit_test.go` covers the scan aggregate, pass-through scan, join and IN-subquery. Untested:
- the PK-lookup aggregate and PK pass-through. These classify through `throw_classified_ndb_error`'s RLE branch (15589), not through `handle_ronsql_exception`;
- the count(*) shortcut;
- CTE join aggregates;
- the API-side last-N aggregate.

**Tests.** Add these to `runRateLimitTest`:
- `SELECT SUM(val_1) FROM tiny_tbl WHERE pk IN (1,2,3);`
- `SELECT pk, val_1 FROM tiny_tbl WHERE pk IN (1,2);`
- `SELECT COUNT(*) FROM tiny_tbl;`
- `WITH c AS (SELECT pk AS k, SUM(val_1) AS s FROM big_tbl GROUP BY pk) SELECT SUM(c.s) FROM tiny_tbl AS t JOIN c ON c.k = t.pk;`

### T-6 (Medium-High) Stale-schema paths are untested, and `--sleep 3` hides one of them

`ronsql_subquery_agg_ext.test:1354` runs DROP INDEX / CREATE INDEX and then `--sleep 3` "to allow NDB dictionary cache on REST server to refresh". That sleep skips the paths it should test:
- the QRY_UNRELATED_INDEX 4809 → RonSQLMaybeStaleSchema retry (15487–15503, RONDB-1036, "Linux-only race");
- the join `nextResult` SchemaError → RMS (11871, 12283);
- the `unload_schema` error branches.

No test covers them, and the sleep is itself a flakiness risk.

**Test.** Add `ronsql_cte/t/ronsql_schema_change_join.test`:
1. Warm RDRS with `SELECT o.id, SUM(l.price) FROM k_orders o JOIN k_lineitem l ON l.order_id = o.id GROUP BY o.id`, plus a pass-through join and a CTE whose body uses the index.
2. Run `DROP INDEX idx_k_li_order ON k_lineitem; CREATE INDEX idx_k_li_order_qty ON k_lineitem (order_id, qty);`.
3. With no sleep, re-run each query through RDRS with `x-ronsql-phases`. Assert the mysqld result and `attempts<=2`.
4. Repeat with `ALTER TABLE ... ADD COLUMN` on the root.

### T-7 (Medium) Silent retries cannot be seen by any comparison

`ronsql_compare.inc` does not request `x-ronsql-phases`, and `ronsql_phase_stats.result` masks `attempts=N`. A query that fails its first attempt and succeeds on retry passes all ~2,100 comparisons. Such a failure can be temporary, a stale schema, or a 1227-style kernel error.

**Fix.** Add `-H "x-ronsql-phases: 1" -D hdr` to the compare curl and assert `attempts=1` unless `$allow_retry` is set. This is a one-file change that hardens every test.

### T-8 (Medium) A stale workaround disables the CLI arm in 121 files

121 files set `$suppress_ronsql_cli=yes` with the note "ronsql_cli does not properly refresh NDB dictionary cache for join queries ... 1227". But:
- `ronsql_join.test` runs joins through the CLI arm without suppression, and its result has no cli-vs-rdrs diff;
- 95 direct `$RONSQL_CLI_EXE ... JOIN` invocations exist.

Either the issue is gone, and the CLI arm is wasted, or it is real and RDRS hides it by retrying (T-7).

**Action.** Re-enable the CLI arm in one CTE family (e.g. `body_joins.inc`) to settle it. Then remove the workaround, or fix the cause.

### T-9 (Medium) Execute-time user errors and limits without tests

| Site | Test to add |
|---|---|
| EXPLAIN with JSON/JSON_ASCII (9601–9605). JSON is the RDRS default, so `{"query":"EXPLAIN SELECT ..."}` hits it | `ronsql_rdrs_basic`: RDRS EXPLAIN without `outputFormat` → `--error 22`; record the message and `X-RonSQL-Error-Class: unsupported` |
| Subquery limits (9092, 9139), non-numeric IN result (9130), NULL IN result (9487) | `ronsql_subquery.test`: a 1001-row `_seq` table; `SELECT COUNT(*) FROM t WHERE a IN (SELECT MAX(v) FROM big GROUP BY id)` with 1001 groups; `... IN (SELECT MAX(name) FROM t2 GROUP BY id)` |
| Pass-through and API-aggregate sort cap (10511–10519, 1M rows / 256 MB) | Needs a test-only knob that lowers the cap. Without it, `ronsql_large` would need more than 1M rows |
| `emit_cte_lookup_filter` rejections (14134, 14251, 14273, 14563, 14746, 14757) | `ronsql_cte_or.test` / `ronsql_cte_colvscol.test` rejection cases, e.g. 65 OR'd disjuncts; `LEFT JOIN cte ... WHERE cte.minstr IS NULL` |
| `generate_embedded_condition` 18723, 18830, 18834, 18861 | `ronsql_cte_case.test` rejection cases |
| `encode_constant` DECIMAL vs out-of-range int/float (16505, 16514) | `ronsql_hopsworks.pl` / `ronsql_constants`: `WHERE dec10_2 = 1e30`, `WHERE dec10_2 = 99999999999999999999` |
| DATE_ADD/INTERVAL errors (16766–16855); GREATEST/LEAST WHERE errors (17022–17094) | `ronsql_date_sub.test`, `ronsql_join_where_classify.test` |
| SUM/AVG over scale-0 DECIMAL wider than 64 bits in a CTE (13291, 13482, 13679, 13892) | `ronsql_cte_decimal.test` |
| AVRO via the CLI (8958) | `ronsql_cli` → `--error 1` |

### T-10 (Medium) Subquery result values go through text, and the boundaries are untested

`execute_subqueries` (9065–9180) prints the inner result as TEXT_NOHEADER and re-parses it with `strtoll`, then `strtod`. Consequences:
- BIGINT UNSIGNED values ≥2^63 overflow `strtoll` and become doubles, so they lose precision;
- DECIMAL beyond 2^53 and 17-digit DOUBLEs depend on the printer round-tripping exactly;
- DATE is unproven (`ronsql_cte_subquery.test:355`).

No subquery test uses UNSIGNED.

**Test.** Add `ronsql/t/ronsql_subquery_types.test`:
- Table: `u(id INT PRIMARY KEY, ub BIGINT UNSIGNED, d DECIMAL(30,10), f DOUBLE, dt DATE)`.
- Rows: `ub` ∈ {18446744073709551615, 9223372036854775808, 9223372036854775807}; `d=12345678901234567890.1234567890`; `f=0.30000000000000004`.
- Queries, all strict against mysqld:
  - `SELECT COUNT(*) FROM u WHERE ub = (SELECT MAX(ub) FROM u)`;
  - `... WHERE ub IN (SELECT MAX(ub) FROM u GROUP BY id)`;
  - the same two forms for `d` and `f`;
  - a correlated form.

### T-11 (Medium) ORDER BY results are compared after sorting

`ronsql_compare.inc` sorts body lines unless `$skip_sort` is set. 44 compares have a top-level ORDER BY but are sorted:
- `ronsql_join_agg`: 15;
- `ronsql_subquery_agg_ext`: 18;
- `ronsql_basic`: 3;
- `float_formatting`, `avg_formatting`: 2 each;
- `dbt3`, `body_bigquery`, `body_count_star`: 1 each.

The order of merged SELECT-list-subquery results (`ResultPrinter` ordered printing over the merged leaves) is therefore never checked.

**Fix.** In `ronsql_compare.inc`, detect a top-level ORDER BY with a small perl check and set skip_sort automatically. `orderby.py` has the parser.

### T-12 (Medium) JSON framing is not tested on each path

JSON is the RDRS default and the format Hopsworks reads. Several paths have their own framing code:
- empty result: 11079–11082 and 12306–12311;
- LIMIT 0 or early close: 11048–11052;
- sorted pass-through;
- PK miss: 10853;
- join-aggregate `print_result`;
- count(*).

None of these is tested in MTR. The fs vectors and fuzz tests cover JSON only for FS shapes, and they `--skip` when rondb-cli is not built.

**Test.** Add `ronsql/t/ronsql_json_paths.test` using `ronsql_json_check.inc`. Cases:
- `SELECT o_orderkey FROM orders WHERE o_orderkey > 99999`;
- the same with `LIMIT 0`;
- `SELECT l_orderkey FROM lineitem LIMIT 2`;
- `... ORDER BY o_totalprice DESC LIMIT 3`;
- PK miss `WHERE o_orderkey = -1`;
- IN list where every key misses;
- join aggregate `SELECT c.c_nationkey, SUM(o.o_totalprice) FROM customer c JOIN orders o ON o.o_custkey = c.c_custkey GROUP BY c.c_nationkey`;
- empty join pass-through;
- `COUNT(*)` on an empty table;
- the last-N API aggregate.

### T-13 (Medium) EXPLAIN does not show the root that `emit_root_op` actually builds

`emit_root_op` (12599–13190) chooses one of:
- a `readTuple` root;
- a `lookupCte` root;
- a PK-equality ordered-index scan;
- a hash-PK or over-cap table-scan fallback;
- a scan root for an outer child under aggregation (F19).

`print_join_plan_ops` (19170) prints `[ROOT] TABLE_SCAN` in every case. So rpr-1..16 and `cte_lookup_root_outer` can assert only values. A regression to a full scan, or back to an aggregate lookup root (the DbspjMain crash described in the header of `body_root_pk_residual.inc`), would not be pinned. Phase K `[ANTI]` is never grepped either.

**Fix.** Factor the root decision into a function that both the emitter and `print()` call. Print `Root access: readTuple|lookupCte|PK index scan <idx>|scanTable+filter`. Add the greps to rpr-* and to `ronsql_cte_basic` Tests 28–30.

### T-14 (Medium) The NULL / three-valued-logic matrix exists for one emitter only

`ronsql_emptytable_and_nulls.test:232–291` runs 30 predicates mixing NOT/AND/OR/=/!= over NULL data. They are applied only through the single-table NdbScanFilter. The same predicates are never run against NULL data on:
- join child filters (15153–15163);
- the PK-lookup residual (9739–9755, 10708–10746);
- the CTE_LOOKUP filter;
- embedded CASE conditions;
- the pushed-join root filter.

NOT LIKE has no test at all.

**Test.** Add `ronsql/t/ronsql_null_logic_paths.test`. Generate the 30 predicates × 5 placements with perl, as `ronsql_cte_index_body` does. Use `tbl1` data with NULL in every column combination:
- child: `SELECT COUNT(*), SUM(c.pk) FROM p JOIN tbl1 c ON c.pk = p.id WHERE <pred on c>`;
- PK residual: `... FROM tbl1 WHERE pk IN (1..24) AND <pred>`;
- CTE outputs: a CTE grouped by pk with `MIN(flt) AS flt, MIN(gr) AS gr`;
- CASE: `SUM(CASE WHEN <pred> THEN 1 ELSE 0 END)`;
- join root: `<pred on p>`.

All strict against mysqld.

### T-15 (Medium) ENUM and SET are never tested; BIT barely

NDB stores ENUM and SET as `NDBCOL::Char` (`plugin/ha_ndbcluster.cc:9083–9090`), so RonSQL sees binary CHAR. It probably prints index or bitmap bytes, and groups and compares on the index rather than the label. No test uses `ENUM(` or `SET(`. BIT appears only in `ronsql_orderby.test:37` and `body_passthrough_types.inc:60`.

**Test.** Add `ronsql/t/ronsql_enum_set.test`:
- Table: `es(id INT PRIMARY KEY, e ENUM('b','a','c'), s SET('x','y','z'), bt BIT(5))`, including NULLs.
- Queries, strict against mysqld (or expect a clean rejection):
  - pass-through `SELECT id, e, s, bt FROM es`;
  - `GROUP BY e`;
  - `MIN(e)`;
  - `WHERE e = 'a'`;
  - `ORDER BY e`;
  - a join on `e`.

### T-16 (Medium) No executor unit tests run without a cluster

`src/ronsql/CMakeLists.txt` registers only `KeywordsUnitTest` and `OverflowUnitTest`. Pure code that has caused regressions is tested only end-to-end:
- `ResultPrinter` static helpers: `double_to_fixed` (2568), `print_json_ascii` (2236), `print_binary_tsv` (2280), base64, temporal conversion (68–127), `convert_result_to_double` (2897), HAVING evaluation (~3099). Past regressions here include RONSQL-45 year padding, F9 JSON quoting and F3 AVG formatting;
- the pass-through row comparator (10483);
- `ronsql_classify_message` + `ronsql_http_code_for`. These map message wording to HTTP status, so rewording a message changes the status;
- the NDB-error verdict in `handle_ronsql_exception` (15700–15775);
- `encode_constant` (16375), which works on `NdbDictionary::Column` objects that can be built in-process.

**Proposal.**
- Add `ResultPrinterUnitTest.cpp`, exposing the statics through an internal header.
- Add `ErrorClassUnitTest.cpp`, with a table test over all messages and a refactored pure `ndb_error_verdict(err, output_started, has_rl_identity)`.
- Add `EncodeConstantUnitTest.cpp`.

### T-17 (Medium) OOM injection reaches execution at one allocation site

`RonSQLAllocationTestHook` knows three sites, and only `DECIMAL_BUFFER` is reached during execution (`ronsql_oom_test.cpp:371`). Unexercised:
- 140 `alloc_exc`/`realloc_exc` sites in `RonSQLPreparer.cpp`: sort buffer, PK-op arrays, `JoinQueryObjects`, subquery buffers;
- 8 in `ResultPrinter.cpp`.

The path that should handle them goes `bad_alloc` → OOM→RPE → `cleanup_trans`, with no retry and with `objs` released (15660–15675, F34).

**Fix.** Add `Site::ARENA` to the request `ArenaMalloc`. Sweep `fail_on=1..N` per executor shape in `ronsql_oom_test`. Assert:
- RESOURCE class;
- `attempts==fail_on` (no retry);
- no open transaction.

### T-18 (Low-Medium) Assertions weaker than they look

- **`ronsql_join_agg.test:852` (Test 44).** It arms ERROR_INSERT 5126 with `--error 0,1,255`, and the insert is a no-op in release builds, so the eviction path may never run. Nothing checks that it did. Add `have_debug` or drop the test; `mem_leak_errors` e5126 covers this properly.
- **`ronsql_cte_dd_passthrough_single_table.result:685` (st-20).** It pins `RMS->RPE` for a TIME residual. "Unsupported type" is classified as possibly stale schema, so every such query unloads the schema. The baseline locks that classification in.

### T-19 (Low-Medium) Flakiness risks

- **Sleeps and polling:**
  - `--sleep 3` at `subquery_agg_ext:1354`;
  - `--sleep 1` at `cte_park_sweeper:90`, which waits for the 200 ms delayed REF;
  - the polling loops in `cte_nodefail.inc`.
- **Double SUM depends on merge order.** The values are recorded verbatim in non-strict baselines (`ronsql_hopsworks.result:8287–8288`) and noted in F21 and BUGS_TODO EDGE-FLOAT-B.
- **Intermittent abort** in `ronsql_join` Test 3 (BUGS_TODO:710).
- **LIMIT without ORDER BY** in 14 compares (`body_passthrough_limit`, `body_orderby_limit:218/530`, `in_list_lookups:248`). They are correct only because of data invariants (e.g. every customer has exactly 5 orders), and nothing documents those invariants in the test.

### T-20 (Low) Unreachable code paths

- `JoinOp::SEMI_JOIN` (15108, 19184): the planner never produces it.
- CTE_SCAN child with linked keys (15474).
- `execute()` in the FAILED state (9583).

Delete them, or cover them with unit tests.

### T-21 (Low) External-harness weaknesses

- **TPC-H Go test** (`ronsqltpch/handler_test.go:260–330`):
  - it pairs rows after a lexicographic sort of whole lines and uses a 1e-9 relative tolerance;
  - ORDER BY is not verified;
  - Q9 is skipped.
- **Silent skips.** `ronsqltpch.test:5–7` `--skip`s without `load_tpch`. `ronsql_fs_{golden,vectors,fuzz_spec,fuzz_env}` `--skip` without rondb-cli. CI can lose JSON and FS coverage without anyone noticing. Fail instead when an environment variable such as `MTR_REQUIRE_RONSQL_TOOLS` is set.

### T-22 (Maintenance, High) Mirror suites duplicate about 3.4 MB of results

- **Topology mirrors.** `ronsql_cte_ng1r3`, `ng2r3` and `ng4r2` results are byte-identical to `ronsql_cte` (33/33). `ng2r2` matches 34/36; the other two are its own node-failure tests.
- **FS topology mirrors.** `fs_ng2r2` and `fs_ng4r2` differ only by a one-line topology pin.
- **JIT mirrors.** They differ only by the pin's SET and SELECT lines.
- **Cost.** 109 commits touched mirror result directories since August. `refresh_jit_mirror_results.py` covers only 3 pairs: not `ronsql_fs_jit`, not the ng mirrors.

**Proposals.**
1. Make each ng mirror's `r/` (and `t/`) a git symlink to `../ronsql_cte/r` (`t`). Move the node-failure tests into `ronsql_cte`, where they `--skip` unless 4 nodes are present.
2. Make the JIT pin silent: compute the delta and `--die` if it differs from the expected value stored in the mirror `.test`. The mirror result then equals the base result and can be a symlink as well.
3. Generate mirror `.test` files from the base list plus an exclusion list with reasons. Add a consistency check, for example a perl block in an always-run test, that fails when a base test has no mirror and no exclusion. This would have caught T-2 and T-3.

### T-23 (Maintenance, Medium) Each query is written two to four times per case

- **Duplication.** A case repeats its query text for compare, explain, phase rows, and an "== Expected result ==" block that runs the query a 4th time (1161 such blocks).
- **Two forms.** The `$QUERY` and `$QUERY_FILE` forms both exist because the shell-quoted `-e '$QUERY'` breaks on quotes.
- **Drift.** `expdrift.py` found 4 compare/expected pairs that differ: `subquery_agg_ext:347`, `body_passthrough_types:250`, `fs_smoke:339` (all intentional) and `body_passthrough_orderby_index:436` (likely a mis-pairing by the script's parser). Nothing keeps the copies in sync.

**Proposal.** One `ronsql_check.inc`:
- write the query once to a file;
- run it through mysql, RDRS and optionally the CLI;
- print the sorted `mysql.out` as the expected block (no 4th run);
- take optional `$EXPLAIN_GREPS` (one per line), `$EXPECT_ROWS`, `$EXPECT_FETCHED_MAX` and `$EXPECT_ATTEMPTS` (default 1);
- skip sorting automatically for a top-level ORDER BY;
- be strict by default, with `$known_diff`.

### T-24 (Maintenance, Medium) Non-strict mode is the default, and 14 non-strict tests could be strict today

16 test files still compare non-strictly:
- `ronsql_basic`;
- `emptytable_and_nulls` (48 compares);
- `orderby` (40), `orderby_stress` (31);
- `float_formatting`, `avg_formatting`, `filtering`, `formatting`, `timestamp`, `regressions`, `string_agg_interleaved`;
- `join`, `date_sub`, `encoding`;
- `dbt3`, `overflow`.

`ronsql_hopsworks` is a 17th, driven by `.pl`. 14 of the 16 have no recorded diff, so they can be switched to strict now. `ronsql_basic` is clean only because of its canonicalization (T-1).

## Commands for the user (not run here)

Validate T-2: `cd mysql-test && ./mtr --suite=ronsql_jit ronsql_join_agg ronsql_cte_case ronsql_subquery` (expect result mismatches).

Then refresh: `python3 storage/ndb/claude_files/compiled_interpreter/refresh_jit_mirror_results.py` from `mysql-test/`, and re-run `./mtr --suite=ronsql_jit`.

Check T-8 quickly: remove `--let $suppress_ronsql_cli=yes` from `suite/ronsql_cte/include/body_joins.inc` locally and run `./mtr --suite=ronsql_cte ronsql_cte_dd_joins`.
