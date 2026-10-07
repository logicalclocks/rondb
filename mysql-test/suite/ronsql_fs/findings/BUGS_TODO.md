# RonSQL feature-store bugs and deferred coverage

This is the work queue, not a requirements-acceptance gate. Regression
runs may succeed when supported cases pass and known limitations are
reported separately. A known defect must not excuse unrelated failures.
Engine fixes belong in separate tasks.

The F0-F9 reproductions and original observations are in [smoke.md](smoke.md).
F8 was a framework fixture issue and is already fixed.

## Engine and protocol work

- [x] F0: support the Hopsworks CTE-form collect query over a partial key.
  FIXED 2026-09-15 (RONDB-1124 M1.3): the projection-only main over a
  non-aggregating single-table body with ORDER BY and LIMIT collapses into
  the body at parse time (`collapse_collect_cte`); regression test
  `ronsql.ronsql_cte_collect_collapse`.
- [x] F1: reused string aggregate storage (client/data-node crashes and
  possible wrong values). FIXED by RONDB-1056 `10561b78d1e` (per-row string
  high-water mark in both interpreters); the framework cases and smoke
  probes are asserted since M1.2 (2026-09-15).
- [ ] F2: resolve DECIMAL MIN/MAX scale formatting differences.
- [ ] F3: AVG column formatting fixed (`2ca8fc243f5`), with strict smoke
  coverage for INT/DOUBLE AVG. Arithmetic expressions retain four digits;
  DECIMAL precision limitations remain.
- [x] F4: FLOAT display fixed (`df820fa3540`) with passing base/JIT
  regressions. The framework exemption is removed; smoke/templates passed
  on base, JIT, ng2r2, and ng4r2, and the requirements probe reports PASS.
- [ ] F5: preserve DECIMAL values beyond 2^53 cents exactly.
- [x] F6: first-version SUM uses checked signed/unsigned 64-bit accumulators.
  Overflow must report NDB error 1860; MySQL's wider DECIMAL result remains
  an intentional range difference. Template and smoke probes assert this
  contract; it does not satisfy the original exact-DECIMAL requirement.
- [x] F7: support emitted VARBINARY/complex snowflake projections. FIXED
  2026-09-15 (RONDB-1124 M1.4): the pass-through printer prints BINARY /
  VARBINARY (raw bytes in TEXT, base64 in JSON); regression test
  `ronsql.ronsql_binary_passthrough`; the framework compares base64 cells
  by their bytes.
  The user-run A6 corpus reproduced the type-17 pass-through rejection
  for snowflake_binary DTO 1, single/1, single/2 and single/9; all ten
  MySQL twin comparisons passed. Golden mode recognizes only that DTO's
  named type-17 rejection as REJECT(expected); binary support remains open.
- [x] F9: quote temporal MIN/MAX values in JSON output. FIXED 2026-09-14
  (RONDB-1124 M1.1): `ResultPrinter::print_aggregate_result` passes
  `m_quote` to the temporal decoder; regression test
  `ronsql.ronsql_temporal_json`. Malformed JSON is still decoded as an
  error, never a successful result; the F9 exemption (`KNOWN-ERROR`) is
  retired.
- [x] F10 (bench.md): mysqld crashes (`NdbSqlUtil::likeLongvarchar` require in the
  ordered-scan sorted merge) on a pushed aggregate with a VARCHAR GROUP BY key and
  an IN list (`fs_hw_strkey_batch100`, pushdown ON). FIXED 2026-10-01; the fs_hw
  matrix no longer needs `--engines ronsql,mysqld_nopush` to avoid it.
  - Cause (2026-09-30, code reading), two faults in the single-table push
    (`ha_ndbcluster_push_agg.cc`, `ha_ndbcluster::ordered_index_scan`):
    - The GROUP BY took its order from PRIMARY, so the scan was asked for
      `SF_OrderByFull`. `DoAggregation()` drains it through `nextResult()`,
      whose sorted merge compares each fragment's current row by the index
      key, but under a pushed aggregation the receive buffers hold aggregate
      records. An integer key compared garbage harmlessly; a VARCHAR key read
      a garbage length and hit the `require` in `likeLongvarchar`.
    - With `m_stm_aggregator` set, MRR is off, so every range of the IN list
      is its own `read_range_first()` → `ordered_index_scan()` →
      `DoAggregation()` into the same `NdbAggregator`, which is deleted only
      in `reset()` at the end of the statement. Its group map keeps earlier
      ranges' groups (they come back again), and a group or scalar aggregate
      that spans ranges comes back once per range: wrong results for integer
      keys too, never seen because the pushed arm only benchmarked them.
  - Fix (2026-10-01, tests pass):
    - `ndb_aggregate_reads_ranges_separately()`: no single-table push when
      the access reads several ranges in one execution (INDEX_RANGE_SCAN
      with more than one range, REF_OR_NULL, index merge). One range stays
      pushed. Unpushed MySQL was the faster plan for these lists anyway
      (F12: 1.8 ms vs 10 ms pushed for 100 keys).
    - `ordered_index_scan()` never asks for a sorted scan when the
      aggregation is pushed; F30's `ndb_aggregate_order_from_index()` keeps
      the push off plans that need the index order.
    - Regression `ndb_push_agg.ndb_pushdown_agg_ranges` + JIT mirror: r-1..r-5
      (IN lists incl. the F10 VARCHAR shape, a scalar and a cross-range
      GROUP BY, key OR NULL) pushed=0; s-1..s-3 (one range, incl. a VARCHAR
      PRIMARY range) stay pushed; each compared with pushdown OFF.
  - Related, not fixed: a single-range pushed aggregate that runs more than
    once in one statement (a correlated subquery) reuses the same
    `NdbAggregator` too, and nothing resets it between executions. Untested;
    worth a test before relying on pushdown in correlated subqueries.
- [x] F11 (bench.md): pushed point aggregates fail with NDB error 4120 'Scan already
  complete' (`fs_hw_agg_point`, `_filter`, `strkey_point` at sf 1); unpushed and the
  windowed / GREATEST variants work.
  - Cause (2026-10-01, code reading): the same read-after-drain as F30. The
    equality on the key prefix is a REF on PRIMARY: `index_read()` starts the
    pushed single-table aggregation, whose `DoAggregation()` drains and
    completes the scan, and MySQL reads the next row with `index_next_same()`,
    which had no aggregator branch and called `fetch_next()` on the completed
    scan: 4120. The window bound makes it a range scan, whose
    `read_range_next()` had the branch; the GREATEST set is not pushed.
  - Fixed by F30's backstop (`3b2787828d0`): `next_result()` serves every row
    of a pushed single-table aggregation from the aggregator, which covers
    `index_next_same()`. F30's control c-3 (`WHERE k = 1`, scalar, pushed)
    already ran this shape. Regression `ndb_push_agg.ndb_pushdown_agg_ranges`
    p-1..p-4: the F11 forms (integer and VARCHAR key prefix, a non-key
    filter, a key with no rows) stay pushed and match pushdown OFF; they
    pass (2026-10-01). To confirm on fs_bench: `.bench_sql fs_hw_agg_point`
    with `ndb_pushdown_aggregate=ON`.
- [x] F12 (bench.md): RonSQL executes `IN (k1..kn)` as a table scan with an OR filter:
  S3 batch serving is 200–1000× slower than MySQL (204 ms for 10 keys, 4.4 s for 1000).
  Index ranges per key needed; the fs_hw plan pins record the table scan as observed.
  FIXED by WP-F (`m3_wpf_plan.md`, as F23): `0a34cea7cf9` (F1a PK lookups),
  `a52a7498d78` (F2 multi-range scans, F1b aggregating PK reads), `dd46e068447`
  (F3 CTE bodies / join roots). Census run 6 (2026-09-24, benchmark computer, 8
  threads, 4 LDMs): every batch plan is `SF_MultiRange` with one range per key, and
  every `m3_wpf_plan.md` §0 target is met — agg_batch10 / 100 / 1000 352 µs /
  2.60 ms / 23.5 ms (MySQL nopush 709 µs / 7.83 ms / 185 ms, sf 0.1), core_in_pk100
  460 µs (sf 1). Pins now expect the ranges. Closed; further benchmark runs
  (e.g. the 15-LDM configuration) are separate tasks.
- [x] F13 (bench.md): snowflake point reads cost ~350 µs of CTE_SCAN round trips over the
  2–3 PK reads MySQL does (483–525 µs vs 120–172 µs).
  CLOSED 2026-09-22 (census run 4, benchmark computer): 192–227 µs after the
  RONDB-1120 CTE work, −60 %; run 6 (8 threads) 334 / 382 µs for snow1 / snow2.
  The single-SPJ-tree design (WP-G) stays an optional step towards ~100 µs.
- [x] F14 (spec_fuzz.md): a snowflake CTE body keyed by a VARCHAR entity key
  returns no rows through CTE_SCAN (`customers_str_1` root); the body alone and
  the integer-keyed twin work. Found by the E6 fuzzer, seed 1.
  Root cause (WP-F F3, 2026-09-23): RonSQL passed `encode_constant`'s
  length-prefixed VARCHAR bytes to `NdbQueryBuilder::constValue(ptr, len)`,
  which adds the prefix itself, so every VARCHAR bound or key of a pushed
  query was doubly prefixed (the standalone aggregate uses the old API, which
  takes the prefixed form); the query-root KEYINFO path of the API
  (`serializeConstOp`) added one more.  Fix in the F3 change set
  (`query_const_value`, `serializeConstOp`, `NdbCharConstOperandImpl::convertVChar`),
  regression in `ronsql_in_list_spj`.  Confirmed 2026-09-24 (spec seed 1
  known-wrong 2 → 0, envelope seed 1 nine cases → PASS); fuzzer markers
  retired.  Framework follow-up (WP-E): string-keyed snowflake cases and
  manifest rows `R-S7-str` / `R-S8-str`.
- [x] F15 (envelope_fuzz.md): data node crash in DBSPJ `sendJoinAggNullRow` /
  `appendFromParent` (`DbspjMain.cpp:15201`, error 2343 failed ndbassert) on a
  LEFT JOIN from a CTE feeding a join-aggregation leaf. Found by the E7 envelope
  fuzzer, seed 1; isolate with `--threads 1`. Pushdown join aggregation (RONDB-733).
  Root cause (2026-09-30): the leaf's CTE_LOOKUP_REF miss NULL-extended the scan
  root's row instead of its direct parent's (a chained LEFT-joined CTE), so
  `P_PARENT` walked above the root. Fix in `DbspjMain.cpp` (buffer the leaf's
  direct parent, NULL-extend its row); regression
  `ronsql_cte{,_ng2r2}.ronsql_cte_dd_outer_chain_miss`, both green 2026-09-30.
  Hazard retired; `cte-per-fg` emits LEFT joins again (60 % per CTE).
- [x] F22 (smoke.md): unchecked BIGINT SUM merge fixed by `8f064822249`,
  with passing distributed and RonSQL regressions (`bd9c41158cd`,
  `2f618dae6bf`). The framework no longer exempts wrapped results.
  Smoke probes check CLI error 1860 and HTTP 400 with semantic/NDB-1860
  headers. Base, JIT, ng2r2, and ng4r2 validation passed; the requirements
  probe reports REJECT(expected), with F6 retaining the MySQL range difference.
- [ ] F21 (smoke.md): SUM over DECIMAL(18,2) is topology-dependent on RonSQL
  (`6.0600000000000005` on 2 node groups vs `6.06` on 1; MySQL exact) — the F5
  DOUBLE path combines per-fragment partial sums in topology order. Found by the
  E8 ng2r2 mirror of the smoke test's recorded section.
- [x] F20 (envelope_fuzz.md): RDRS crashes from a reference-count bug in the
  RONDB-1092 parking machinery — a by-pointer invalidateTable/invalidateIndex of
  a parked object dropped the newer incarnation's entry by name and released the
  parked reference twice. FIXED 2026-09-12: `NdbDictionaryImpl::
  detach_local_reference` (+ null guards in getIndex / park_stale_object);
  regression `testDict -n InvalidateParkedByPointer` OK; fs suites green ×3.
  Still worth a run: the batchpkread Go suite (TestUnloadSchema) under ASAN.
- [x] F18 (envelope_fuzz.md): a partial-key CTE lookup now runs (was rejected)
  and returns the wrong row count. Silent correctness regression. E7 fuzzer, seed 2.
  Root cause (2026-09-30): not a lost guard. The I.16b/c rewrite
  (`maybe_rewrite_partial_key_cte_root`) promotes the CTE to a CTE_SCAN
  root and demotes `balances_1` to a child joined on `account_id`, a
  prefix of its primary key, so the child is an INDEX_SCAN below the CTE
  scan: the scanCte parent + scanIndex child shape of the unfinished
  Phase N.1 (`pushdown_join_aggregation/cte_filter_phase_n1.md`), never
  run by an MTR test (the rewrite tests all demote to a PK_LOOKUP).
  FIXED 2026-09-30: `validate_cte_execution_shapes()` rejects
  an index or table scan below a CTE_SCAN root at prepare time, with the
  I.16a `Partial CTE lookup key not supported.` wording when the scan is
  the rewrite's demoted root and a generic message otherwise (the same
  shape written with the CTE as root). Lookup children are unaffected;
  Hopsworks snowflakes only join on a child's full primary key.
  Regression: `ronsql.ronsql_cte_partial_key` Tests 11-13,
  `ronsql.ronsql_parser_cte` (rewrite pin moved to a PK-keyed demoted
  root, plus two rejections); the fuzzer row `cte-partial-key` is a
  clean reject again. Envelope seed 2 (1000 cases) rerun: the four
  `cte-partial-key` cases are CLEAN-REJECT (F18); the hand-written MTR
  results pass. Implementing N.1 lifts the rejection.
- [x] F19 (envelope_fuzz.md): CTE_SCAN as an outer-join child now runs (was
  rejected) with a divergent result. E7 fuzzer, seed 2. FIXED 2026-09-30.
  - Misnamed: the planner makes every CTE child a CTE_LOOKUP. The probe
    (`merchants_1 AS m LEFT JOIN t ON t.k = m.merchant_id WHERE
    m.merchant_id = 1 GROUP BY m.mcc`, t grouped by merchant) is a
    LEFT-joined CTE_LOOKUP under a PK-bound root. On a miss RonSQL
    returned no row (an empty result, hence "output names differ") where
    MySQL returns `(mcc, NULL)`.
  - Cause: an aggregating main query of a CTE-containing statement takes
    a `readTuple` root when the WHERE binds the root's PK
    (`emit_root_op`, the fpw-6 shape); a key-bound or single-group CTE
    root takes a `lookupCte` root (I.7 / G4). DBSPJ builds the
    NULL-extended row for an outer CTE_LOOKUP miss from the parent row
    buffered on the nearest scan ancestor (`execCTE_LOOKUP_REF`;
    `handleAggAncestorComplete` for an outer intermediate); under a
    lookup root there is none, the injection is skipped, and the parent
    row drops out of the aggregate. Pass-through queries are unaffected
    (the API NULL-fills).
  - Fix (2026-09-30, tests pass): `emit_root_op` gives an
    aggregating scope with a LEFT / ANTI_JOIN child a scan root (a
    PK-bound ordered-index scan, a filtered table scan for a hash-only PK,
    scanCte for a CTE root). The filtered scalar CTE root has only the
    lookup form and is rejected cleanly (`Outer join below a filtered
    scalar CTE root not supported.`). Regression
    `ronsql_cte.cte_lookup_root_outer` (f19-1..8); the fuzzer's F19
    known-wrong row is retired, and the seed-1 envelope SUMMARY is now
    `clean-reject=20 pass=180` (the one known-wrong case was this probe).
- [x] F17 (envelope_fuzz.md): HAVING + ORDER BY (aggregate alias) + LIMIT →
  internal error `Got record with fewer aggregates than expected. Please report a
  bug.`; plain HAVING rejects cleanly. Found by the E7 envelope fuzzer.
  Root cause (2026-09-30): a HAVING identifier is read as the aggregate register
  numbered like its column index (`having_agg.agg_index` shares a union with
  `col_idx`); the same bug made a plain column in HAVING compare against an
  unrelated aggregate value. Fix: `validate_having_references` rejects every
  HAVING identifier except a SELECT-list subquery alias. Also fixed: HAVING in a
  CTE body was parsed but never applied; `analyze_ctes` now rejects it.
  Regression `ronsql.ronsql_having_refs`, srb-P8 re-pinned. Verified 2026-09-30.
- [x] F16 (envelope_fuzz.md): AVG over a VARCHAR column reports the generic
  `Failed writing aggregation program. Please report a bug.` instead of the
  specific `AVG over string columns is not supported.` guard (which fires for
  temporal AVG). Found by the E7 envelope fuzzer. Functionally a clean reject.
  - Also a 500: "Please report a bug" forces the class internal.
  - Cause: the specific AVG guards live only in `build_cte_virtual_tables`
    (CTE outputs). A main-query AVG / SUM reaches `NdbAggregator::Sum`,
    which refuses a string or temporal register with its own codes
    (`kErrUnsupportedStringOperation` / `kErrUnsupportedTemporalOperation`),
    and `programAggregator_do_or_fail` threw the generic bug message. A
    type the interpreter cannot load at all (BINARY, BLOB, BIT, the old
    temporal formats) failed the same way at `LoadColumn`.
  - FIXED 2026-10-01:
    `RonSQLPreparer::throw_sum_avg_emit_error` translates the two codes at
    the Sum sites (single table, join) and the CTE Avg site into `AVG / SUM
    over string columns is not supported.` / `... over temporal columns is
    not supported — only MIN / MAX / COUNT.` (class unsupported; AVG when
    the failed slot is an AVG output's sum slot); `kErrUnSupportedColumn`
    at LoadColumn becomes `Aggregation over a column of this type is not
    supported.` Arithmetic feeding SUM / AVG is covered too (the register
    keeps the column's type). Not covered: arithmetic on a string inside
    MIN / MAX, which the API does not refuse. Regression
    `ronsql.ronsql_avg_sum_types` (f16-1..7); the fuzzer's avg-string /
    avg-temporal expectations now match the specific messages.
- [x] F28 (2026-09-29, WP-J J0 test `ronsql_cte.ronsql_cte_dd_lastn_agg` lastn-7):
  `RonSQLPreparer::encode_constant` returned the widest length for every
  DATETIME / TIMESTAMP constant (8 / 7 bytes) instead of the column's width
  (5 / 4 + (precision + 1) / 2). A CTE body range on a TIMESTAMP(0) key
  failed with `Failed to create index-scan root: Incompatible datatype
  specified in operand argument (4803)`, because `NdbQueryBuilder::constValue`
  requires exactly `getSizeInBytes()` for fixed-size columns. The key-row
  paths (`memcpy(dst, rv.val, rv.len)` for PK lookups and IN-list lookups)
  overran the 4-byte slot by 3 bytes. Single-table windowed aggregates were
  unaffected (`setBound` takes the column's length). FIXED in `3ab0fcd1df2`:
  the exact width is returned from a zeroed widest-size buffer.
  Regression cases lastn-W1 (PK lookup), W2 (IN on the TIMESTAMP key) and
  W3 (single-row CTE keyed on it), recorded green in all six `ronsql_cte`
  mirrors and kept green through the later WP-J re-records.
- [x] F29 (2026-09-29, WP-J J0 first form, lastn-12): a join whose key is a
  CTE MIN/MAX output over a narrower integer column fails with an internal
  error. Repro: `WITH t AS (SELECT customer_id, event_time,
  MAX(merchant_id) AS merchant_id FROM lastn_tx WHERE customer_id = 8
  GROUP BY customer_id, event_time ORDER BY event_time DESC LIMIT 10)
  SELECT m.mcc, COUNT(*) FROM t JOIN lastn_merchants AS m ON m.merchant_id
  = t.merchant_id GROUP BY m.mcc` gives `[internal] Caught exception:
  Failed to create child operation.` Cause: `build_cte_virtual_tables`
  widens a MIN/MAX output to the wire type (INT → BIGINT), and
  `NdbLinkedOperandImpl::bindOperand` requires identical parent and child
  types (QRY_OPERAND_HAS_WRONG_TYPE). The same applies to FLOAT → DOUBLE,
  DECIMAL → BIGINT / DOUBLE and temporal → Bigunsigned outputs. Either
  link through a converted value or reject cleanly at plan time with a
  permanent error naming the type mismatch. Not needed by WP-J (J0 / J1
  carry non-key columns as GROUP BY keys, which keep their types).
  - Wider than MIN/MAX: any linked key with a CTE side. QueryPlanner's
    real-table pre-check ("Join column type mismatch.") skipped CTE key
    sources, and nothing checked them later; the reverse direction, a
    real column keying a CTE_LOOKUP whose GROUP BY column has another
    type, failed the same way (`lookupCte` binds with the same rule).
  - FIXED 2026-09-30 with the clean reject.
    `RonSQLPreparer::check_cte_join_key_types`, called per child op in
    `emit_child_ops` once the virtual tables exist (for a CTE_LOOKUP
    child after its key-shape checks, so a wrong-column or partial key
    keeps its own message), applies bindOperand's
    rule (type, precision, scale, length, charset) to every key with a CTE
    side and throws `Join column type mismatch.`, naming both columns and
    their types, and explaining the 64-bit carrying of CTE aggregate
    outputs when one side is one. Converting in the link (the NDB API's
    own "TODO: Allow and autoconvert compatible datatypes") would need a
    new DBSPJ key-pattern op and is not done. Regression
    `ronsql_cte.cte_join_key_types` (f29-1..4: both reject directions,
    MAX(INT) keying a BIGINT key, the GROUP BY carrier).
- [x] F30 (2026-09-30, WP-J spot run, `.bench_sql
  fs_hw_agg_last10_tx300_grouped`): mysqld with single-table aggregation
  pushdown (`ndb_pushdown_aggregate=ON`, default OFF; on in the
  `ronsqlcrunch` config and the user's benchmark cluster) fails on the J0
  grouped form with `Error 1296 (HY000): Got error 4120 'Scan already
  complete' from NDBCLUSTER`. Repro (fs_bench, customer 31 has 300 rows):
  `WITH t AS (SELECT customer_id, event_time, amount, fee, COUNT(*) AS
  grp_rows FROM transactions_1 WHERE customer_id = 31 GROUP BY
  customer_id, event_time, amount, fee ORDER BY event_time DESC LIMIT 10)
  SELECT COUNT(amount), AVG(amount), MAX(fee) FROM t`. RonSQL runs it
  correctly; the MTR twin checks pass because their mysqld keeps the
  default OFF.
  - Mechanism, from code reading (`ha_ndbcluster.cc`,
    `ha_ndbcluster_push_agg.cc`), not yet confirmed in a debugger: the
    optimizer serves GROUP BY + ORDER BY with a reverse scan of PRIMARY and
    no sort. `ndb_push_single_table_aggregation()` replaces that scan with
    `DoAggregation()`, which drains every row, merges the groups on the API
    side and completes the scan. The first group returns;
    `ha_ndbcluster::index_prev()` has no `m_stm_aggregator` branch (nor has
    `index_next_same()`; only `index_next()`, `read_range_next()` and
    `rnd_next()` do), so it calls `next_result()` on the completed scan:
    4120.
  - Suspected silent wrong result: the push never checks whether the plan
    relies on index order. The pushed groups come back in the
    `NdbAggregator` map order (memcmp of the group key; little-endian
    integers do not sort numerically), so the ASC variant, which goes
    through the dispatched `index_next()`, may keep the wrong 10 groups.
    To confirm: run both directions with the pushdown ON and OFF and
    compare, and `EXPLAIN FORMAT=TREE` to see the plan.
  - Fix: do not push (single-table or join aggregation) when the plan uses
    an ordered index for GROUP BY / ORDER BY (`JOIN::m_ordered_index_usage
    != ORDERED_INDEX_VOID`, or a descending / sorted scan with no later
    sort); make `index_prev()` / `index_next_same()` read the pushed result
    or fail cleanly as a backstop. Regression test: the grouped form in
    both directions with `ndb_pushdown_aggregate=ON`, compared with OFF.
  - Not a WP-J blocker: the benchmark compares RonSQL on the grouped form
    and MySQL only on the natural statements.
  - Fix committed 2026-09-30:
    - `ndb_aggregate_order_from_index(join, root_path)`
      (`ha_ndbcluster_push_agg.cc`) is true when three things hold: the
      block groups (`JOIN::group_list` is not empty), the statement has an
      ORDER BY (`query_block->order_list`), and no SORT lies on the path
      from the root down to the table access.
      - It reads `order_list` rather than `JOIN::order` because
        `optimize_distinct_group_order()` folds an ORDER BY that is a
        prefix of the GROUP BY into it and clears `JOIN::order`. That is
        exactly the J0 grouped form.
      - `ndbcluster_push_to_engine()` then pushes neither the join
        aggregation nor the single-table aggregation.
    - Backstop: `ha_ndbcluster::next_result()` serves every row of a pushed
      single-table aggregation from the aggregator. That covers
      `index_prev()` and `index_next_same()` as well.
  - Test: `ndb_push_agg.ndb_pushdown_agg_index_order` plus its JIT
    mirror (JIT fallback delta 0). Integer keys cross 256. Every case must
    return the same rows in the same order as with pushdown OFF.
    - o-3..o-5 read PRIMARY in order with no sort, in both directions, and
      report pushed=0. o-4 is the descending read that failed with 4120.
    - o-1 / o-2 keep the F30 grouped-CTE shape. On the test data the
      optimizer sorts that body, so they stay pushed (pushed=1) and still
      match. The fs_bench plan without a sort is the one o-4 covers.
  - Confirmed 2026-09-30 on the benchmark cluster after the mysqld rebuild:
    `.bench_sql fs_hw_agg_last10_tx300_grouped` runs. It is slightly faster
    than RonSQL on the same grouped form, which keeps the CTE plan.
    - Controls c-1..c-3 (ORDER BY an aggregate, GROUP BY without ORDER BY,
      one row) stay pushed.
- [x] F31 (DONE, performance, 2026-09-30): do not wait for the scan close
  confirmation in the user thread.
  - Today: a scan stopped early (a pass-through LIMIT with fragment scans
    still open) is closed by `NdbScanOperation::close_impl`, which sends
    the close and blocks in `wait_scan()` until TC confirms
    (`SCAN_TABCONF`, one round trip). The transaction and its TC connect
    record are only reusable after that. That was 73 µs of
    `fs_hw_agg_last10_tx300`'s 161 µs execute (`benchmarks.md` §8).
    - Since `1e7a1d9a27e`, RDRS closes after the reply
      (`RonSQLExecParams::deferred_close`). The reply no longer waits, but
      the RonSQL worker still does.
    - The deferred close then overlaps the next request's first batch: a
      second thread waiting in the NDB API means a hand-over between the
      thread receiving for all waiters and the waiter, about +17 µs of
      `firstbatch` at one client thread.
  - Why it is doable: the close confirmation is executed by whichever
    thread receives it (the receive thread or the thread currently
    receiving for all waiters), which updates the scan's receiver counts
    under the Ndb's client lock. The wakeup (`Ndbif.cpp`, `GSN_SCAN_TABCONF`
    → `theWaiter.signal(NO_WAIT)`) happens only because the owner waits in
    `WAIT_SCAN`. The receiving thread must not hand the transaction back
    itself, because the Ndb free lists belong to the owning thread; but it
    does not need to.
  - Sketch: an asynchronous close in the NDB API.
    - The close sends the close request (`send_next_scan(…, true)`) and
      returns. The transaction goes on a per-Ndb closing list instead of
      back to `theConnectionArray[node]`, so its TC record is not reused
      while TC still has the scan open.
    - `Ndbif` must not signal the waiter for a transaction on the closing
      list. Otherwise the owner, already waiting on the next query's scan,
      gets a spurious wakeup.
    - The owner reaps finished closes at its next API call
      (`startTransaction` / `closeTransaction`) and moves them to the idle
      pool. An unfinished one makes `startTransaction` use another idle
      connection; each Ndb object then keeps one or two extra.
    - Also handle: node failure releasing transactions on the closing list;
      the Ndb destructor, and RDRS returning an Ndb object to its pool,
      waiting for or cleaning up pending closes; the 4008 scan timeout;
      statistics.
    - NDB API tests: async close, node failure during the close, Ndb
      teardown with closes pending.
  - Gain: the worker time and the hand-over. The data-node close work
    stays; only a per-fragment row limit in the data nodes (a fragment
    stops after N rows and reports the scan complete, like DBSPJ's
    `setMaxRows` for pushed queries) removes the close altogether.
  - Status (2026-09-30): done. Built and benchmarked (`benchmarks.md`
    §8); `ndb.ndb_scan_close_nowait`, `ndb.ndb_scan_close_nowait_nf` (after
    the fixes below) and the RonSQL suites pass.
    - NDB API, `NdbScanOperation::closeNoWait()`:
      - `close_send_nowait()` is the sending half of `close_impl()`. It
        runs under the client lock and first checks, without changing
        state, that something is open at the data nodes.
      - The operation is unlinked from the user's transaction
        (`NdbTransaction::unlinkScanOperation`) and parked on
        `NdbImpl::m_parked_scan_closes` with its scan transaction.
      - `Ndb::reapParkedScanCloses()` runs from `startTransactionLocal()`,
        `closeTransaction()`, and `doDisconnect()` (the last one waits).
        It uses `parked_close_done()` (node failure, timeout) and
        `release_parked_close()` (the tail of `close()`).
      - A `SCAN_TABREF` during a parked close (the TC failed the scan
        itself) needs nothing more: the close already sent is the one the
        TC waits for, and its `SCAN_TABCONF(EndOfData)` finishes the parked
        close, as in the final wait of `close_impl()`.
      - `NdbTransaction::m_scan_close_nowait` keeps `Ndbif` from waking
        the waiter on `SCAN_TABCONF` / `SCAN_TABREF` for a parked scan.
      - Falls back to `close()`: not executed, a continuous scan, batches
        in flight, a kernel error, 4008, or a failed data node.
      - Batches in flight rule out most unordered scans (a LIMIT without
        ORDER BY), which request the next batches of all fragments before
        returning rows. Ordered scans (J5, collect) wait for each batch
        before returning rows, so they have nothing in flight and park.
    - RonSQL: the single-table pass-through early close calls
      `closeNoWait()`; the JSON framing of an empty result is emitted
      before it.
    - The close-after-reply plumbing of `1e7a1d9a27e`
      (`RonSQLExecParams::deferred_close`, `ronsql_dal_finish`, the
      request guard) is removed: the reply now waits only for the close
      request to be sent. `ronsql_cli` gets the same behaviour.
    - Not covered: the pushed-query drain (`NdbQuery::close` in
      `execute_join`).
    - Test: `testScan -nScanCloseNoWait` (MTR `ndb.ndb_scan_close_nowait`).
      Table scans and ordered index scans in both directions are stopped
      after 0-4 rows with batch 1; the test checks full reads afterwards,
      bounded NdbTransaction objects, deleting an Ndb with parked closes,
      and the TC resource snapshot check.
    - Node failure: `testScan -nScanCloseNoWaitNodeFailure` (MTR
      `ndb.ndb_scan_close_nowait_nf`, debug builds).
      - New DBTC error insert 8316 holds every API scan close 3 s, so
        ordered-scan closes stay parked. Two Ndb objects park them
        alternately on the victim and on the survivor as TC, then the
        victim is killed. `ClusterMgr::set_node_dead` counts the node's
        connection generation up, which `parked_close_done()` sees.
      - The survivor fails its scans that had fragments on the victim
        (`SCAN_TABREF`, close needed) while their closes are held.
      - Ndb A keeps scanning and finishes the parked closes, those of the
        failed node without a TC round trip. Its NdbTransaction objects
        stay bounded.
      - Ndb B is deleted with parked closes on both nodes: the waiting reap
        returns.
      - After the node restart both kinds of scan work again.
      - First run (debug build) hung 6 minutes in the waiting reap of
        Ndb B, then segfaulted. Three bugs:
        - 8316 re-held its own re-sent close forever: the check
          `refToBlock(sender) != DBTC` is true for DBTC instances 1-3
          (the instance bits are part of the block number), so the
          survivor never processed the closes. Now: only signals from
          another node are held; the held one is marked (stopScan bit
          value 4) and carries the API's reference, which is restored on
          arrival, so the "Confirming scan close" and "Wrong transid"
          replies still go to the API.
        - `parked_close_done()` sent a second close after a
          `SCAN_TABREF` (close needed). `close_impl()` never does: the
          first close answers it. The duplicate could reach a reused TC
          record. Removed.
        - The 4008 path called `setErrorCode(4008)`, which records the
          error on `m_transConnection` (nullptr for a parked close): the
          segfault. Removed; the log line stays.
    - Not covered by a test: the 4008 timeout of a waiting reap.
- [x] HTTP status: distinguish invalid SQL/syntax from server failures — RONDB-1124 M1.0:
  error classes → 400/413/503/500, `[<class>]` body prefix, X-RonSQL-Error-Class /
  X-RonSQL-NDB-Error headers; verified (rdrs2-golang_gotest incl. TestErrorStatusByClass,
  ronsql / ronsql_cte / fs suites green, results re-recorded for the prefix).
- [x] HTTP status follow-up (2026-09-30): user errors still answered 500
  "[internal]", and some messages named internal plan files.
  - Cause: a one-argument `RonSQLPermanentError` is classified by its
    wording (`ronsql_classify_message`), and wordings outside its
    vocabulary fell through to INTERNAL: "supports only …", "unsupported
    operator" (lowercase), "only =, …, >= supported.", the string-literal
    date errors, the planner's "No suitable index for join columns." /
    "BLOB/TEXT join column." / "ON parents not on one ancestor chain.",
    constant-folding overflow / divide by zero, CTE-body ORDER BY rejects,
    and "Failed to compile aggregation program" (register exhaustion). A
    script over every throw site found 145 permanent errors classified
    INTERNAL, ~30 of them user errors; the rest are internal checks.
    MaxRespSize exceeded answered 500 without a class.
  - FIXED 2026-10-01: explicit classes at those sites (new
    `require_unsupported`; UNSUPPORTED / SEMANTIC / LIMIT constructors),
    classifier vocabulary for the remaining wordings ("unsupported",
    "supports only", "support only", "string literal") so the
    MaybeStaleSchema rethrow and future sites classify too, MaxRespSize
    as 413 "[limit]" with the class header. Plan-file / phase references
    removed from messages (the projection-only reject's
    non_aggregate_pushdown_plan.md, "later phase", "deferred to I.5 / I.6",
    "Phase I.6 F.1", the EXPLAIN FRAGS_PER_WORKER note's
    frags_per_worker_plan.md); 30 recorded results updated for the
    wording. Tests: rdrs2 `TestErrorStatusByClass` (join without index,
    constant-folding overflow), `ronsql.ronsql_size_limits` rl-3 (413).
  - F16 and F17, the user errors reported as internal bugs that needed
    guards rather than classes, are fixed in their own entries. The
    EXPLAIN `[I.10 …]` tags keep their phase names (pinned by
    ronsql_cte_minmax_index).
- [x] F24 (bench.md): aggregation with many groups. (a) CTE / join aggregation:
  fixes 1-5 done 2026-09-24 (`39ebf4b30cc` linear-hashing group table,
  `61c04d45e4a` redistribution resumes at its bucket, `2671e046dee` merge adopts
  the largest per-thread table, `92e7f26cade` no per-row atomic, `462026d0cf8`
  batched redistribution); run 6: offline_fs_scalar 993 -> 168 ms, tpch q2 / q13 /
  q22 ~1.1 s -> 113-139 ms. Next for (a): split the per-node owner role so these
  queries scale with LDMs. (b) Drained single-table aggregates
  (`core_group_many`, 1.03 s vs 161-168 ms through the CTE path): ~1 M partials
  reach the API merge; `m3_run6_plan.md` C1. Targets: core_group_many <= 2x
  core_group_few, tpch_q2 <= MySQL.
  2026-10-06: the partial flood of (b) was fixed on 2026-09-26 (`7b9f9959b7a`
  groups kept until query memory runs low, `f562608d14c` 16 KB result batches,
  `89928a208f9`, `d5d9417de18`). Mac: core_group_few / _2k / _many 171 / 169 /
  401 ms (target 342). Profiles (`m3_experiments.md` X2 Result): ~299 ms data
  nodes (the many-group part is a cache miss per row in `findInBucket`), ~67 ms
  API merge (~44 ms `std::map`), ~30 ms print. Fix 6 (branch RONDB-1124-f24):
  hash-index merge in `NdbAggregator`, one sort for output order: Mac
  core_group_many 401 -> 305 ms, core_group_few / _2k unchanged, target met.
  Closed 2026-10-06: both targets are met (core_group_many 305 ms <= 2x
  core_group_few on the Mac; tpch_q2 113-139 ms vs MySQL 274 ms in run 6).
  Further speed-ups are separate tasks: the per-node owner split for (a), the
  per-row group-record cache miss on the data nodes.
- [ ] F25 (bench.md): a ~1 ms idle-wake stall on a share of requests, both engines
  (sets the serving p99). Diagnose on the benchmark computer: CPU idle states,
  data-node spinning, or the NDB API receive path (`m3_experiments.md` X1).
  Code analysis 2026-10-05 (`m3_experiments.md` X1 Result): likely a send left
  queued by a receive thread that then sleeps in `pollReceive(1 ms)` — in the
  `NumCPUs=4` layout every block runs in a receive thread, and a failed
  send-lock try without `m_force_send` is followed by the 1 ms epoll sleep when
  spinning is off. To verify with runs 5 / 7 (separate block threads) and a
  `NumCPUs=4` before / after measurement of the proposed `mt.cpp` fix.
  Fix `a22b016ab15` (branch RONDB-1124-f25): on the Mac with spinning forced
  off every ~1.2 ms event at T=1 is gone and averages are unchanged (X1
  Results). Open: arms D / E / F on the benchmark computer.
- [ ] NDB API adaptive send never defers (found 2026-10-05 while reading F25):
  `TransporterFacade::add_to_poll_queue` has `if (m_poll_waiters >
  m_max_poll_waiters) m_max_poll_waiters;` — the assignment is missing since
  `bf9fadc2e1a` (RONDB-564), so `m_use_poll_waiters` stays 0 and rule 2 of
  `do_send_adaptive` always sends at once. Latency is unaffected; the intended
  batching under many client threads never happens. Fix: `m_max_poll_waiters =
  m_poll_waiters;`, then measure throughput at T=8 / T=64.
- [x] Every scanned row read the whole slowdown NodeBitmask (found 2026-10-06 in the
  F24 data-node profile): `Dblqh::scanTupkeyConfLab` checked
  `get_status_slowdown().isclear()` before testing the API node's bit, and with
  node ids up to 8191 `isclear()` reads 256 words when no transporter is slowed
  down (64 on 25.10-26.05, 8 on 24.10). ~19 % of the data nodes' busy samples
  during core_group_many, ~100 ns per row. Fixed in `9640795ba3c` (tests only the
  API node's bit; cherry-picks cleanly to 24.10-26.10). Mac, T=1, against F24:
  RonSQL core_group_few / _2k / _many 175 / 168 / 305 -> 147 / 146 / 269 ms; MySQL
  without pushdown 437 / 1283 / 1346 -> 409 / 1141 / 1203 ms (ndb wait -26 /
  -136 / -137 ms); the loop's self samples fell from 20 % to 1.2 % of busy.
- [ ] F27 (bench.md, P0): data node failure under concurrent many-group CTE
  queries. (a) DBSPJ `do_init` stopped the node on exhausted query memory: fixed
  in `914cbf9bbb7` (OutOfQueryMemory REF, error insert 17534). (b) 1869 instead of
  a temporary memory error: SETUP now answers 20008 and the join-aggregation
  out-of-memory paths report 20008 / 1870 (temporary); the eviction
  `ndbrequire` on an empty table is gone (`m3_run6_plan.md` A2). Census run 6:
  offline_fs_batch completes at T=8 (run 4 lost its data nodes, run 5 failed with
  1869). Open: (c) the query-memory budget of many-group CTE queries (tpch_q2
  ~110 MB per query; `m3_run6_plan.md` C5); confirm (a) / (b) closed and update
  `bench.md`.
- [x] F32 (2026-09-30, review): a WHERE condition on the right side of a LEFT
  JOIN of real tables was pushed into that table's operation as its filter, i.e.
  applied as part of the join condition; `b.col IS NULL` kept rows WHERE removes.
  A LIKE on any joined table counted as a constant condition and was filtered on
  the root with the joined column's attribute number (NDB error 40, or a wrong
  column). Fixed in `37f537465b0` (PR #1125): such conditions are rejected,
  LIKE and NOT over a NULL-rejecting test promote the join to INNER, and LIKE is
  classified by its operands. Regression `ronsql.ronsql_left_join_where`.
- [x] F32b (2026-10-06, code reading confirmed, regression test recorded):
  `classify_ce_table_resolved` counted every operator without a case as a
  constant, so on a joined table XOR, GREATEST / LEAST (lowered only later, in
  `simplify_ce`), the outer side of `IN (subquery)` (including a correlated
  EXISTS, rewritten to IN on the outer column) and a correlated scalar
  subquery went to the root table's filter, where `apply_filter_cmp` uses the
  joined column's attribute number against the root (as LIKE did, F32).  They
  are now classified by their operands; a subquery condition on a joined table
  is rejected, since its result replaces the node only at execution time and is
  known to reach the root table's filter only.  Regression
  `ronsql.ronsql_join_where_classify` (tables laid out so the old behaviour
  filters different rows; jw-5 is the first IN (subquery) on a join's root in
  the suite).
- [ ] Follow-ups to clean rejections (features, not bugs):
  - Real-table anti-join: `LEFT JOIN b ... WHERE b.col IS NULL` with `b.col NOT
    NULL` is now rejected (F32); it could run as `ANTI_JOIN` (MatchNullOnly) as
    the CTE form does, once aggregating joins are known not to feed matched rows.
  - HAVING on GROUP BY columns or on the alias of an ordinary aggregate output
    (rejected since F17's fix): needs typed comparisons in `ResultPrinter`
    (its HAVING evaluator works in doubles).
  - HAVING inside a CTE body (rejected since F17's fix): would be applied at the
    CTE finalize barrier, after the cross-node merge.
  - EXPLAIN does not show the HAVING condition.
- [ ] Observation (2026-09-14, unrelated to RONDB-1124): one run of `ronsql.ronsql_join`
  Test 3 (`SELECT o.o_custkey, MIN(l.l_price), MAX(l.l_price) FROM orders AS o JOIN
  lineitem AS l ON l.l_orderkey = o.o_id GROUP BY o.o_custkey`) aborted `ronsql_cli` on
  the debug assertion `m_finalWorkers < getWorkerCount()` in
  `NdbQueryImpl::setFetchTerminated` (NdbQueryOperation.cpp:3511, pushed-join
  worker accounting on an abort path; area last changed by RONDB-1107 on 2026-09-01);
  20 consecutive reruns passed. Intermittent; for the RONDB-1107 / join-aggregation owners.
- [ ] Observation (2026-09-15, unrelated to RONDB-1124): the `== Expected result ==`
  display of smoke probe EDGE-FLOAT-B (`SUM(f_double)` over `edge_hist_1` entity 4)
  is a MySQL-side, order-dependent double sum that flips between two one-ulp
  neighbours (`123457.38900010001` in every recording except the M1.0 recording of
  `ronsql_fs_ng4r2`, which had `123457.3890001`; the compare block through
  `ronsql_compare.inc` has always shown `…10001`). Only the display line moves, so it
  is a rare re-record diff, not a wrong result. Fix when it bites: `--replace_regex`
  on that display statement, or drop the DOUBLE sum from the display as was done
  for the DECIMAL sum (F21).
  instead of returning HTTP 500 for these client errors. Preserve the
  current permanent-error classification until the protocol is changed.

## Deferred framework coverage

- [ ] Optional ronsql_cli execution: the adapter exists, but .fs_verify
  currently compares MySQL with RonSQL through RDRS only. Do not claim
  that adding cli to --engines executes an additional comparison.
- [x] E4: implement vector-level comparison (--vectors) using bound DTO
  groups, reconstructed MySQL queries and independent data-model vectors.
  The six review corrections are applied; Go unit tests passed.
- [ ] E4 review verification (user-run): confirm ronsql_fs_templates and
  ronsql_fs_vectors against their existing recorded results after review.
  Passing A6 MTR runs do not replace these separate L1/L2 tests.
- [x] A6 implementation: --golden runs captured Java queryOnline against
  matching fixture data, compares the Go MySQL twin and RonSQL vectors,
  and retains shared keys/time, provenance and comparison reports.
- [x] A6 cluster regression (user-run, 2026-09-10): all 45 fixtures
  processed, zero failures; 183 MySQL passes, 125 RonSQL passes,
  15 expected rejections (12 F0, 3 F7) and 43 RonSQL-untested requests.
  No setup/cleanup errors or remaining owned databases were reported.
  Evidence and the empty-result F7 caveat are in phase_e4.md §7.
- [x] A6 Go unit tests: user confirmed the post-fix
  go test ./internal/fsq/... ./internal/shell run passed.
- [x] A6 cleanup verification: the passing MTR test independently checked
  zero remaining fixture databases.
- [ ] Evidence retention: preserve report artifacts and record binary
  revisions on subsequent runs.
- [x] A6 automation implementation: ronsql_fs_golden runs --golden,
  retains uniquely named report directories, compares the observed summary
  baseline and checks both fixture databases are gone.
- [x] A6 automation verification (user-confirmed, 2026-09-10):
  ronsql_fs_golden passed both once and with --repeat=2.
  Retain needed logs before another MTR invocation clears them.
- [ ] Track coverage of MySQL-only/point-read fallback and queryOnlineScan
  separately: --golden compares MySQL-only DTOs on both MySQL paths, but
  neither golden nor vector mode executes the pk-read fallback or scan
  twin. Absent templates do not establish why Hopsworks gated them.
- [x] E8: implement requirements mode (--requirements). This flag still
  adds no checks; successful selected L1 or L2 regression checks do not
  establish full Hopsworks requirements acceptance.
  DONE (E8, `phase_e8.md`): `.fs_verify --requirements`
  (`internal/shell/fs_requirements.go`) resolves the `req-v1` manifest and runs
  L1, L2 and Java conformance per requirement. Reports in
  `requirements_reports/2026-09-12` and `2026-09-15` (base, JIT, ng2r2). The
  2026-09-15 reports: 15 SUPPORTED, 4 HOPSWORKS-GATED, 1 UNSUPPORTED (R-A5-types:
  EDGE-decimal-large and EDGE-float-rounding KNOWN-WRONG, EDGE-big-overflow
  REJECT(expected)), so acceptance=FAIL as designed. F4 (FLOAT) was fixed after
  that run; the remaining gap is F5 (DECIMAL) and F6's intended range difference.
- [ ] Report deferred modes explicitly when requested without making
  their absence fail otherwise successful regression runs.
- [x] Stale CTE hazards (2026-10-06, verified: `ronsql_fs_fuzz_env` and its jit /
  ng2r2 / ng4r2 mirrors green):
  `mysql-test/suite/ronsql_cte/findings/_discovery_log.md` rows D3, D4, D5, D6,
  D12, D18, D19, D20 and D23 are closed with their commits (`e5c34e39ce4` D3,
  `bcbab15fd1d` + `088d8b25537` D4 / D12 (col-vs-col, supported for identical
  types since RONDB-1107), `ea50c8a56e3` D5 rejection and D19 / D20,
  `77629762cb4` D6 / D18 / D23) and the MTR cases that run them.
  `fuzz/hazards.go` has no open hazard left; the translations moved to
  `FormerHazards`, run by the new `former-hazard` envelope production (D5 as the
  `fanout-sibling` rejection; D4 compares INT with INT, its old INT-vs-BIGINT
  form is the `colcol-mixed-type` probe).  The hazard test now parses bold ids
  (`**D6**` was never read).  D18 is translated over `customers_1.tier`, not
  `transactions_1.category`: the first run compared MAX over category's
  collation-equal `travel` / `Travel` (MySQL `Travel`, RonSQL `travel`, the F8
  rule); a unit test now rejects MIN / MAX over category or device in any
  compared envelope case or former hazard.
- [x] Envelope fuzzer coverage gaps seen while fixing F17 / F32 (2026-10-06,
  verified on all four arms: seed 1 186 pass / 14 clean-reject, seed 2 100 /
  20, JIT fallback delta 0): `having-order` also emits HAVING over aggregate
  functions (`having-agg`, must match MySQL); the new `real-join` production
  joins customers_1 to regions_1 (INNER / LEFT) with one WHERE condition on the
  joined table: comparison, LIKE, GREATEST, LEAST, XOR, NOT (IS NULL), IS NULL,
  an OR with IS NULL, IN (subquery).  Expectation rows `left-join-where-null`
  (F32), `subquery-joined` (F32b), `fanout-sibling` (D5), `colcol-mixed-type`
  (D4).  Case ids are `env-v2-…`; the recorded `ronsql_fs_fuzz_env` summary
  changes.
- [x] XOR on a LEFT JOIN's right side (2026-10-07, verified: ronsql, ronsql_cte
  and the four ronsql_fs_fuzz_env arms green; seed 2 now 104 pass / 16 clean-reject): it was rejected
  although MySQL treats it as NULL-rejecting.  `is_null_rejecting` now accepts
  an XOR (and NOT over one) with an operand certainly NULL for the NULL-extended
  row (`is_null_for_null_columns`: a column under operators that propagate
  NULL); an XOR of IS NULL tests can be TRUE there and stays rejected.
  `ronsql.ronsql_left_join_where` lj-3 / lj-4 compare with MySQL, lj-r4 pins the
  rejection; the fuzzer's LEFT JOIN XOR cases must now match MySQL.

## Shape reporting

In L1, edge probes retain their EDGE label and SQL, but also contribute to the
serving shapes listed in their explicit associations. --shape selection
includes those probes once; JSON results expose the associations as shapes.
SHAPE summaries describe the selected checks, not complete requirements
acceptance. Known defects and skipped hazards remain non-failing but mark
their associated shapes UNSUPPORTED. Direct collect probes do not replace
the emitted S6 CTE cases; E8 acceptance remains deferred.

In vector mode, skipped MySQL-only groups are counted. A spec with no
compared cells, or an executable group with no compared cells, is UNTESTED
(non-failing), never SUPPORTED. Missing MySQL references are errors.
--allow-reject permits only clean rejections, reported as REJECT(allowed)
and UNSUPPORTED; malformed JSON and other errors remain failures, as do
earlier vector or data-model mismatches. LEFT-MISS applies only to missing
RonSQL snowflake chains against explicit MySQL NULLs.
