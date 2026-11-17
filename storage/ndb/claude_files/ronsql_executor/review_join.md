# RonSQL executor review, slice B: join execution

Reviewer B. Baseline: 26.10-main at 50eb491eb3f. File: storage/ndb/src/ronsql/RonSQLPreparer.cpp
(line numbers are at that HEAD; function names are the durable references).

Scope: `promote_left_joins`, `collect_pk_equalities`, `build_root_residual`, `JoinQueryObjects`,
`execute_join`, `execute_passthrough_drain`, `emit_index_scan_root`,
`emit_pk_equality_index_scan_root`, `emit_root_op`, `resolve_chained_column_type`, `emit_child_ops`,
`print_join_plan_ops`. Read-only review. Nothing was built or run.

Not re-reported: the parser/preparation review (ronsql_parser_review_plan.md) and the DBSPJ/data-node
review (claude_files/dbspj/README.md, E-1..E-30 and the others). Where an API-side finding depends on a data-node
contract, the finding says so.

## Contents

| ID | Title | Category | Severity | Confidence |
|----|-------|----------|----------|------------|
| J-1 | PK-equality index-scan root binds PK constants to a permuted ordered index | bug | wrong-result | verified-by-reading |
| J-2 | CTE root key lookup sends keys in output order; the CTE group key uses GROUP BY order | bug | wrong-result | likely (high) |
| J-3 | CTE root key lookup narrows integer constants with unchecked C casts | bug | wrong-result | verified-by-reading |
| J-4 | Scalar CTE root dummy key fails for a VARCHAR/LONGVARCHAR first output; the child copy has the fix | bug (diverged copy) | error-handling (valid query fails) | verified-by-reading (reachability likely) |
| J-5 | Join emitters ignore filter.end()/finalise()/setInterpretedCode() results, so a failure silently drops the WHERE filter | bug (diverged copies) | wrong-result (OOM/edge) | verified-by-reading (trigger speculative) |
| J-6 | NdbQueryBuilder errors are lost or misclassified; no check of the internal-node budget across CTE subtrees; 4809 retry only on child ops | design/bug | error-handling | verified-by-reading |
| J-7 | emit_pk_equality_index_scan_root lists indexes through the dictionary on every execution, bypassing the schema cache | design | performance | verified-by-reading |
| J-8 | Multi-op CTE bodies never get a root index scan | design | performance | verified-by-reading |
| J-9 | EXPLAIN does not describe the root access path, prints the key source as parent, and marks an aggregation leaf in pass-through queries | design | maintainability (misleading EXPLAIN) | verified-by-reading |
| J-10 | Process-aborting ndbrequire/abort on API-side conditions (OOM, API contract) | bug | crash (OOM only) | verified-by-reading |
| J-11 | A NULL scalar-subquery value in a join root key or bound gives a retried "stale schema" error, not an empty result | bug (cross-slice) | error-handling | likely |
| J-12 | Duplication: scan-filter build+attach block (6 copies in the slice, 2 checked copies elsewhere) | duplication | maintainability | verified-by-reading |
| J-13 | Duplication: CTE key / dummy-key operand builders (3 copies, 2 diverged) | duplication | maintainability | verified-by-reading |
| J-14 | Duplication: constant PK key encoding loops | duplication | maintainability | verified-by-reading |
| J-15 | Duplication: linked-projection attach loop ×3 in emit_child_ops | duplication | maintainability | verified-by-reading |
| J-16 | Duplication: NdbQuery error → exception in both drains | duplication | maintainability | verified-by-reading |
| J-17 | Duplication: CTE output type derivation in resolve_chained_column_type and build_cte_virtual_tables | duplication | maintainability | verified-by-reading |
| J-18 | execute_join is ~630 lines; redundant linkedValue operands; dummy projection column choice | duplication/design | maintainability, minor performance | verified-by-reading |
| J-19 | Test gap: composite / permuted / hash-only PK roots in joins | test-gap | — | — |
| J-20 | Test gap: CTE-root key lookups (permuted outputs, out-of-range, string scalar root) | test-gap | — | — |
| J-21 | Test gap: size limits and builder-error classification | test-gap | — | — |
| J-22 | Test gap: EXPLAIN access path, stale root index retry, NULL subquery in join bounds | test-gap | — | — |

A section at the end lists what was checked and found sound.

---

## J-1: PK-equality index-scan root binds PK constants to a permuted ordered index

- Category: bug. Severity: wrong-result. Confidence: verified-by-reading.
- Location: `emit_pk_equality_index_scan_root` (12529-12578), reached from `emit_root_op` (13153-13167).
- Evidence:
  ```cpp
  const char* pk_col_names[NDB_MAX_NO_OF_ATTRIBUTES_IN_KEY];
  for (int k = 0; k < nkeys; k++)
    pk_col_names[k] = root_table->getPrimaryKey(k);
  const NdbDictionary::Index* pk_ordered_idx =
      QueryPlanner::findOrderedIndex(
          m_dict, root_table, pk_col_names, (Uint32)nkeys);
  ...
  for (int k = 0; k < nkeys; k++) {
    ... encode_constant(pk_const[k], pk_col);
    pk_keys[k] = query_const_value(qb, rv, pk_col);          // PK order
  }
  ...
  NdbQueryIndexBound bound(pk_keys);
  qb->scanIndex(pk_ordered_idx, root_table, &bound, &rootOpts);
  ```
  `QueryPlanner::findOrderedIndex` accepts any ordered index whose first `nkeys` columns hold the PK columns
  in **any order** (QueryPlanner.cpp 653-663). The planner documents this and re-normalizes its own key
  arrays (QueryPlanner.cpp ~376: "findOrderedIndex accept a PERMUTED key set, but emit binds key operands
  positionally"). The builder binds bound operand `i` to index column `i`
  (`NdbQueryIndexScanOperationDefImpl::bindRangeOperands`, NdbQueryBuilder.cpp 2363-2385). The constants
  stay in PK order. The consumed equalities are also removed from the residual (`build_root_residual`), so
  nothing re-checks them.
- Path: for an aggregate query, `lookup_root_supported` is false (13090-13104). For any query, a scan
  child also skips the readTuple root. Both go to this function. `root_pk_equality_covered` makes
  `select_root_scan_config` skip such roots (3628-3638), so the scan-config index path is never used.
  Multi-op CTE bodies get here too.
- Trigger (deterministic when the PK is hash-only):
  ```sql
  CREATE TABLE r (a INT NOT NULL, b INT NOT NULL, v INT,
                  PRIMARY KEY USING HASH (a, b), INDEX ix_ba (b, a)) ENGINE=NDB;
  CREATE TABLE c (id INT PRIMARY KEY, ra INT, x INT, INDEX ix_ra (ra)) ENGINE=NDB;
  INSERT INTO r VALUES (1,2,12),(2,1,21);  INSERT INTO c VALUES (1,1,100),(2,2,200);
  SELECT r.v, SUM(c.x) FROM r JOIN c ON c.ra = r.a WHERE r.a = 1 AND r.b = 2 GROUP BY r.v;
  ```
  MySQL returns (12, 100). The bound is ix_ba (b=1, a=2), which reads row (2,1), so the result is (21, 200).
  With mixed PK types (e.g. INT, BIGINT) the generic operand fails its size check, and the query fails
  with "Failed to create root PK index scan." instead.
  With a normal (ordered) PK the result depends on index-list order: an index such as `a_idx(b,a)` can come
  before PRIMARY.
- Fix: prefer the index named "PRIMARY". Whatever index is picked, build `pk_keys` in index-column order
  (for each `idx->getColumn(i)`, find the PK position whose name matches), as the planner does. Or require an
  exact positional prefix match. Folding this into the J-14 helper fixes every caller.
- Test (ronsql or ronsql_cte, compare against mysqld): the shape above, aggregate and pass-through (with
  the scan child). Add a variant with `PRIMARY KEY (a,b)` plus `INDEX a_ix (b,a)`.

## J-2: CTE root key lookup sends keys in output order; the CTE group key uses GROUP BY order

- Category: bug. Severity: wrong-result. Confidence: likely (high). The API side is verified. The kernel
  contract is inferred from the child-path convention and from DBLQH `lookupGroup` comparing the raw key.
- Location: `emit_root_op`, CTE_SCAN-root branch. The root_pk_covered path (12746-12768) and the G4 path
  (12787-12860) feed the key loop at 12889-12957.
- Evidence: the virtual table's PK columns are added in **output** order
  (`build_cte_virtual_tables` adds one column per `cte->stmt->outputs`, with `setPrimaryKey(is_groupby)`).
  `NdbDictionary::Table::getPrimaryKey(k)` walks columns in that order (NdbDictionary.cpp 472-480). The
  root path builds keys in that order:
  ```cpp
  const char* pk_name = cteVirtualTables[0]->getPrimaryKey(k);   // 12893
  ... lookup_keys[k] = ...root_pk_const[k]...
  opDefs[0] = qb->lookupCte(..., lookup_keys, &rootOpts);
  ```
  The CTE aggregator programs `GroupBy()` in `groupby_columns` order (`programAggregator_join`
  17903-17972). DBLQH probes `interp->lookupGroup(keyBuf, keyLen, ...)` with the raw key bytes
  (DblqhMain.cpp ~22581). The child path follows exactly this contract: `cte_key_coverage` builds
  `pk_names` in GROUP BY order (2199-2215), `normalize_cte_join_key_order` reorders the ON conditions into
  that order, and emit_child_ops rejects anything else with "CTE lookup keys were not ordered by the virtual
  primary key. Please report a bug." (15357-15359). The root path does no such reordering.
- Trigger:
  ```sql
  WITH p AS (SELECT o_amt AS amt, o_grp AS k, COUNT(*) AS cnt
             FROM rl_orders GROUP BY o_grp, o_amt)
  SELECT k, amt, cnt FROM p WHERE k = 100 AND amt = 60;
  ```
  The key goes out as (60, 100) where the group key is (o_grp=100, o_amt=60). The query returns an empty row
  set, or the (60,100) group if one exists. The G4 single-group path has the same flaw
  (`WITH p AS (SELECT o_amt AS amt, o_grp AS k, ... WHERE o_grp=100 AND o_amt=60 GROUP BY o_grp, o_amt)
  SELECT * FROM p`).
  Related child-path issue: keys in GROUP BY order are bound positionally to output-ordered virtual PK
  columns. When the permuted columns differ in type, `bindOperand` fails, and the user sees "Failed to
  create child operation." instead of a result. `check_cte_join_key_types` compares by name and does not
  see this.
- Fix: build root keys in GROUP BY order (reuse `cte_key_coverage`/pk_names), as the child path does.
  Better, add the virtual PK columns to the virtual table in GROUP BY order, so the binding check and the
  wire order agree on both paths. ronsql_cte_root_lookup Test 2 passes only because its output order
  equals its GROUP BY order.
- Test (ronsql/ronsql_cte_root_lookup): the query above. Also the same-type permuted child join
  `... JOIN p ON p.amt = x.a AND p.k = x.g`, and a mixed-type permutation (INT, VARCHAR).

## J-3: CTE root key lookup narrows integer constants with unchecked C casts

- Category: bug. Severity: wrong-result. Confidence: verified-by-reading.
- Location: `emit_root_op` CTE root lookupCte key switch, 12900-12928.
- Evidence:
  ```cpp
  case NdbDictionary::Column::Int:
    lookup_keys[k] = qb->constValue((Int32) ce_const->constant_integer);   // 12911
  ...
  case NdbDictionary::Column::Unsigned:
    lookup_keys[k] = qb->constValue((Uint32) ce_const->constant_integer);  // 12924
  case NdbDictionary::Column::Bigunsigned:
    lookup_keys[k] = qb->constValue((Uint64) ce_const->constant_integer);
  ```
  These bypass `encode_constant`, which range-checks and throws "out of range". The builder range-checks
  only the already-narrowed value (NdbQueryBuilder.cpp 1961-2019).
- Trigger: `WITH s AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) SELECT k, n FROM s WHERE k = 4294967297`
  with `k INT` returns the k=1 group; MySQL returns no row. `k = -1` with `k INT UNSIGNED` probes
  4294967295. Tinyint/Smallint/Mediumint fail with "Failed to create lookupCte root." (QRY_NUM_OPERAND_RANGE).
  That is still a valid query failing, where MySQL returns an empty result.
- Fix: range-check against the virtual column type before casting. Out of range → empty result
  (equality can never hold). The simplest correct emission is a scanCte with a filter that is always
  false, or a short-circuit to "no rows". Or route integers through `encode_constant` +
  `query_const_value`: `build_cte_virtual_tables` now sets m_attrSize/m_arraySize (13975-14060), so the
  original "synthetic m_attrSize==0" reason for the typed overloads is gone. That also unifies the code
  with J-13.
- Test (ronsql_cte_root_lookup): INT key with 4294967297 and with -1; UNSIGNED key with -1; TINYINT key
  with 300. Expect an empty result, as mysqld gives.

## J-4: Scalar CTE root dummy key fails for a VARCHAR/LONGVARCHAR first output

- Category: bug (diverged copy). Severity: error-handling (a valid query fails). Confidence:
  verified-by-reading on the API contract; reachability likely.
- Location: `emit_root_op` scalar CTE root, 12713-12744, compared with the child copy in `emit_child_ops`
  15385-15421.
- Evidence (root):
  ```cpp
  default: {
    const Uint32 rpksz = root_pkc->getSizeInBytes();
    ...
    scalar_keys[0] = qb->constValue(static_cast<const void*>(rzero), rpksz);
  ```
  The child copy explains why this fails for VAR columns and passes length 0:
  "passing getSizeInBytes() there overflows by the prefix size and fails bindOperand with
  QRY_CHAR_OPERAND_TRUNCATED (first surfaced by the sc-20 string scalar watermark ...)".
  `NdbGenericConstOperandImpl::convert2ColumnType` rejects `len + 1 > maxSize` for SHORT_VAR
  (NdbQueryBuilder.cpp 2091-2092). For a virtual VARCHAR, `getSizeInBytes()` = 1 + length (13995-14036),
  so the check always fails.
- Trigger: the scalar CTE's first output is MIN/MAX over a VARCHAR column (its type is kept, per
  `resolve_chained_column_type`), and the CTE is the FROM root with a WHERE:
  `WITH s AS (SELECT MAX(name) AS mx, COUNT(*) AS n FROM t) SELECT mx, n FROM s WHERE n > 0;`
  The query fails with "Failed to create scalar lookupCte root." ronsql_cte_scalar Tests 10-12 cover only
  numeric first outputs.
- Fix: one shared dummy-key helper (J-13) used by both sites, with `dummy_len = 0` for VAR columns.
- Test (ronsql_cte_scalar or ronsql_minmax_string): the query above, with VARCHAR and with a VARCHAR(300)
  (LONGVARCHAR) column, pass-through and aggregating main.

## J-5: Join emitters ignore filter.end()/finalise()/setInterpretedCode() results

- Category: bug (diverged copies). Severity: wrong-result when it fires; the trigger is OOM or
  a filter that exactly fills the buffer. Confidence: verified-by-reading that the results are
  unchecked; the trigger is speculative.
- Location (all in the slice): CTE single-table body root 11645-11653; `emit_index_scan_root`
  12475-12482; `emit_pk_equality_index_scan_root` 12560-12568 (setInterpretedCode is checked here);
  `emit_root_op` readTuple residual 13118-13126; table-scan fallback 13174-13182; `emit_child_ops`
  15154-15164 and 15177.
- Evidence:
  ```cpp
  filter.end();                 // result ignored
  child_code_storage.finalise();// result ignored
  opts.setInterpretedCode(child_code_storage);   // result ignored (15163)
  ```
  `NdbQueryOptionsImpl::copyInterpretedCode` returns Err_FinaliseNotCalled when finalise failed, and
  Err_MemoryAlloc on OOM (NdbQueryBuilder.cpp 769-781). The operation is then built **without** its
  filter, so every row passes. The single-table copies check all three
  (`require_run(DBG(filter.end()) >= 0 ...)`, 9753/9754, 10744/10746). The DBSPJ review notes the
  mysqld twin of this pattern ("The same applies when setInterpretedCode fails", E-11).
- Trigger: OOM in `copyInterpretedCode`. Or a WHERE whose nested comparisons fit the 15359-word dynamic
  buffer (NDB_MAX_SCANFILTER_SIZE_IN_WORDS) while the outermost `end()` does not. Both are rare. The
  consequence is silent extra rows.
- Fix: the J-12 helper, which checks all three and throws (bad_alloc → RESOURCE, otherwise permanent).
- Test: an allocation-failure hook (as RonSQLAllocationTestHook does for the parser) cannot reach
  NdbQueryOptions. A unit test of the helper with a code object that is not finalised is enough.

## J-6: NdbQueryBuilder errors are lost or misclassified; no internal-node budget check across CTE subtrees

- Category: design/bug. Severity: error-handling. Confidence: verified-by-reading.
- Location: every `require_run(opDefs[i] != NULL, ...)`/`require_run(queryDef != NULL, ...)` in
  `execute_join`, `emit_root_op`, `emit_child_ops`, `emit_index_scan_root`; `qb->beginCteSubtree(c)` at
  11621 (return ignored); `handle_ronsql_exception` SRE arm (15693-15770).
- Evidence: `require_run` throws `std::runtime_error` (620-625). The SRE handler classifies by
  `m_trans->getNdbError()` (15700-15705). A builder failure leaves its error on `qb->getNdbError()`, which
  the handler never reads, and the builder has already been destroyed by `~JoinQueryObjects` by the time
  the handler runs. Results:
  - the NDB code is lost (it reaches the client as 0/INTERNAL). Only `emit_index_scan_root` puts the
    builder message in the text.
  - QRY_UNRELATED_INDEX (4809), a stale index after concurrent DROP/CREATE INDEX, is routed to
    `RonSQLMaybeStaleSchema` only in `emit_child_ops` (15500). The same condition on a root index scan
    (`emit_index_scan_root`, `emit_pk_equality_index_scan_root`, the CTE-body index-scan roots) is a
    permanent error with no schema reload.
  - QRY_DEFINITION_TOO_LARGE has no API-side pre-check for the whole tree. QueryPlanner counts only the
    main plan's internal nodes (QueryPlanner.cpp 521-541), and says "CTE subtree nodes are accounted by
    the API's own prepare-time cap". But every `beginCteSubtree` op, every body op, the I.10 MIN/MAX
    self-join leaf and each unique lookup (2 nodes) count toward `m_internalOpNo < 32`
    (NdbQueryBuilder.cpp 2643). For example, 11 single-table CTEs (22 nodes) plus a main root and 11
    CTE_LOOKUPs (12 nodes) fail with "Failed to create child operation." (INTERNAL), not a LIMIT error.
  - `beginCteSubtree` failure (cteId >= 64, node budget) is ignored. The builder enters an error state and
    the next op fails with a misleading message ("Failed to create CTE body scan root.").
  - QRY_DEFINITION_TOO_LARGE from `prepare()` (tree > 0xFFFF words: big IN lists, many filters)
    reaches the client as "Failed to prepare query." / INTERNAL.
- Fix: a `check_qb()` helper (J-18 lists it with the duplications). On NULL it reads
  `qb->getNdbError()` and maps 4809 → `RonSQLMaybeStaleSchema`, 4812 → `RonSQLPermanentError(LIMIT, ...)`,
  Err_MemoryAlloc → `std::bad_alloc`, anything else → permanent with the code and the builder message. Add a
  prepare-time count of internal nodes over all CTE scopes plus the main plan (1 per subtree + body ops
  (+1 MIN/MAX, +1 per UNIQUE_LOOKUP)), with a clear LIMIT error.
- Test: see J-21.

## J-7: emit_pk_equality_index_scan_root lists indexes through the dictionary on every execution

- Category: design. Severity: performance (plus an error ignored → silent plan downgrade). Confidence:
  verified-by-reading.
- Location: `emit_pk_equality_index_scan_root` 12538-12541.
- Evidence: `QueryPlanner::findOrderedIndex(m_dict, root_table, pk_col_names, nkeys)` passes no
  `cache`/`database`. `getIndexList` then calls `dict->listIndexes(fallback_list, *table)` (QueryPlanner.cpp
  38-50), a LIST_TABLES round trip to DICT on the execution hot path of every PK-equality join root that
  aggregates or has a scan child. Its error return is swallowed: the index comes back NULL and the query
  silently falls back to a full table scan with a filter. All other callers pass
  `m_conf.schema_cache, database`. With a hash-only PK (`USING HASH`, as in ronsql_join_agg), the
  PK-equality join root becomes a full scan of every fragment. That is the API-side counterpart of the
  DBSPJ E-29 "roots are never pruned".
- Fix: pass the schema cache and database. Pick the index at prepare time (store it on the scope,
  like `body_scan_config`), so emit does no dictionary work. Combine with the J-1 fix.
- Test: ronsql_phase_stats-style timing is not reliable. A unit-level check that `findOrderedIndex` is
  called with a cache is enough. Functionally covered by J-19.

## J-8: Multi-op CTE bodies never get a root index scan

- Category: design. Severity: performance. Confidence: verified-by-reading.
- Location: `build_cte_scopes` 7476 (`if (scope->join_plan.num_ops == 1) select_root_scan_config(...)`),
  feeding `emit_root_op`.
- Evidence: the main scope calls `select_root_scan_config(m_main_scope, join_where_ce[0], root_ref)` for
  multi-op plans (3628-3638), and `emit_root_op` is shared by both. For a multi-op CTE body, a range or
  secondary-index predicate on the body root is always a full table scan with an interpreted filter. A
  FORCE INDEX hint on such a body root is also ignored: `select_root_scan_config` is where the hint is
  enforced.
- Trigger: `WITH c AS (SELECT o.k, SUM(l.x) AS s FROM orders o JOIN lineitem l ON l.ok = o.id
  WHERE o.d >= '2024-01-01' GROUP BY o.k) ...` scans all of orders.
- Fix: call `select_root_scan_config(*scope, scope->join_where_ce[0], cte->stmt->root_table)` for
  multi-op bodies too, guarded by `!root_pk_equality_covered(*scope)` as in the main scope.
  `emit_root_op` already dispatches INDEX_SCAN roots to `emit_index_scan_root`.
- Test (ronsql_cte, dd style): the query above with EXPLAIN (body root INDEX_SCAN) and a result compared
  against mysqld. Add a FORCE INDEX variant.

## J-9: EXPLAIN does not describe the executed root access path

- Category: design. Severity: maintainability (EXPLAIN disagrees with execution). Confidence:
  verified-by-reading.
- Location: `print_join_plan_ops` 19170-19364; `print` 19514-19519; body-root line 19473-19505.
- Evidence:
  1. The root line prints only `op.type`. A TABLE_SCAN root can run as readTuple (pass-through, or main
     scope with CTEs), as a PK ordered-index scan (`emit_pk_equality_index_scan_root`, see J-1), or as a
     filtered table scan when no ordered PK index exists. F19 (`outer_agg_needs_scan_root`) can also change
     the plan. A CTE_SCAN root can run as a scalar lookupCte, a full-key lookupCte, or a G4 keyed probe.
     None of this is printed, and the root's residual filter is not shown either. Multi-op CTE body roots
     ("Body root: TABLE_SCAN") have the same problem.
  2. `out << "  <- " << jp.ops[op.parent_op_idx].alias` (19207) prints the key source, but the SPJ tree
     parent is `tree_parent_op_idx` (`emit_child_ops` pins it with `setParent`, 15129-15132). For chained
     CTE_LOOKUPs the printed topology (a star) differs from the executed one (a chain). The comment above
     says the line was added to show the topology.
  3. `else if (jp.agg_leaf_idx == i && !op.is_root) out << "** Aggregation leaf **"` (19360) prints for
     pass-through joins too: the planner always sets `agg_leaf_idx = num_ops - 1` (QueryPlanner.cpp 545).
  4. "Residual filter: yes" is printed only for real-table children. CTE_LOOKUP filters and root filters
     are not shown.
- Fix: compute the root access decision once (a small `RootAccess` struct filled at prepare time, or by a
  dry run of the `emit_root_op` decision logic without qb), store it on the scope, and use it for both
  emit and EXPLAIN. Print the tree parent. Gate the aggregation-leaf line on `m_is_aggregate_query` (or
  `scope.agg`).
- Test: J-22.

## J-10: Process-aborting ndbrequire/abort on API-side conditions

- Category: bug. Severity: crash (RDRS process) on OOM; otherwise invariant-only. Confidence:
  verified-by-reading.
- Location: `execute_join` 11528 `ndbrequire(qb != NULL)` (NdbQueryBuilder::create returns NULL on OOM),
  11889 `ndbrequire(resultAgg != NULL)`; `execute_passthrough_drain` `ndbrequire(o->type == COLUMN)`,
  `ndbrequire(vcol != NULL)`; `emit_child_ops` `default: abort();` and
  `ndbrequire(num_cb_operands < MAX_JOIN_KEY_COLS * 2)`.
- Evidence and trigger: `NdbQueryBuilder::create()` under memory pressure aborts the whole REST server,
  not just the query. `handle_ronsql_exception` already maps `std::bad_alloc` to a RESOURCE error.
- Fix: `if (qb == NULL) throw std::bad_alloc();`. Turn the others into `require_bug(...)`.
- Test: none practical beyond review.

## J-11: A NULL scalar-subquery value in a join root key or bound gives a retried "stale schema" error

- Category: bug (cross-slice; the subquery reviewer may own the root cause). Severity: error-handling.
  Confidence: likely.
- Location: `emit_index_scan_root` 12382-12384, `emit_pk_equality_index_scan_root` 12550, `emit_root_op`
  13138, `emit_child_ops` `bound_operand` 15298 (all call `encode_constant`).
- Evidence: `child_const_bound_op` / `build_scan_config_candidates` accept `I_SUBQUERY` as bound-eligible
  at prepare time (4219-4223, 5355-5361), and `collect_pk_equalities` accepts any non-identifier. At
  execution, `substitute_subquery_results_ce` turns an empty scalar result into `T_NULL` in place (9514-9518).
  `encode_constant(T_NULL, INT col)` then throws `RonSQLMaybeStaleSchema("Integer type column compared to
  an incompatible value...")` (16461-16465). That reloads the schema and fails permanently (if the schema
  is unchanged), where MySQL returns an empty result. The filter path (`apply_filter_cmp`) fails the same
  way, so this is not specific to joins. The join paths make it reachable through bounds and keys as well.
- Trigger: `SELECT t.g, SUM(c.x) FROM t JOIN c ON c.tk = t.id WHERE t.id = (SELECT MAX(id) FROM empty_t)
  GROUP BY t.g`.
- Fix: in `substitute_subquery_results_ce`, a comparison with a NULL scalar becomes FALSE (the polarity
  machinery already handles IN). Or emit an always-false root.
- Test (ronsql_subquery): the query above, and a range variant `t.k >= (SELECT MAX(...) FROM empty_t)` on
  an indexed join root.

## J-12: Duplication: the scan-filter build+attach block

- Category: duplication (diverged → J-5). Severity: maintainability.
- Sites: 11645-11653, 12475-12482, 12560-12568, 13118-13126, 13174-13182, 15154-15164 (+ the checked
  single-table copies 9745-9755, 10740-10747). Differences: only the single-table copies check
  `end()/finalise()`, and only 12567 and 13140 check `setInterpretedCode`. 13118 also caps the size
  (`LOOKUP_FILTER_MAX_WORDS`).
- Helper:
  ```cpp
  // Builds AND(ce) into code (finalised); attaches it to opts when opts != NULL.
  // Throws on any NDB API failure. Returns code.getWordsUsed().
  Uint32 build_scan_filter(NdbInterpretedCode& code, QueryScope& scope,
                           ConditionalExpression* ce, NdbQueryOptions* opts);
  ```
  About 45 lines saved. Risk: low (identical semantics plus checks). The lookup cap stays at the readTuple
  call site.

## J-13: Duplication: CTE key / dummy-key operand builders

- Category: duplication (diverged → J-3, J-4). Severity: maintainability.
- Sites: scalar-root dummy key 12713-12744; root lookupCte typed keys 12900-12955; child ScalarDummy key
  15385-15421. The root dummy uses the size in bytes for VAR columns (broken); the child dummy uses 0
  (fixed). The root keys use narrowing casts (J-3).
- Helper:
  ```cpp
  // value == NULL: a dummy key of the column's type (value ignored by the kernel).
  const NdbQueryOperand* cte_key_operand(NdbQueryBuilder* qb,
                                         const NdbDictionary::Column* virt_col,
                                         ConditionalExpression* value);
  ```
  Real values go through `encode_constant` + `query_const_value`. The virtual columns have real attr sizes
  since 13975-14060, so the typed-overload workaround is no longer needed. About 70 lines saved. Risk:
  medium. The Double/Float encoding and DECIMAL-widened virtual columns need a test (ronsql_cte_decimal
  covers those types).

## J-14: Duplication: constant PK key encoding loops

- Category: duplication. Severity: maintainability.
- Sites: `emit_pk_equality_index_scan_root` 12544-12553; `emit_root_op` readTuple 13131-13142 (and the
  single-table 9778, 10764, 10783 for reference). Identical apart from the error text.
- Helper:
  ```cpp
  // Fills out[0..n) in the column order given by col_names; out[n] = nullptr.
  void build_const_key_operands(NdbQueryBuilder* qb, const NdbDictionary::Table* tab,
                                const char* const col_names[],
                                ConditionalExpression* const consts_by_pk[], int n,
                                const NdbQueryOperand* out[]);
  ```
  It maps the order explicitly (`col_names` = index column order or PK order), which fixes J-1. About
  15 lines saved. Risk: low.

## J-15: Duplication: the linked-projection attach loop ×3 in emit_child_ops

- Category: duplication. Severity: maintainability.
- Sites: 15189-15201, 15208-15220, 15232-15244. Only the "not an ancestor" message differs.
- Helper: `void add_linked_projections(NdbQueryBuilder*, const JoinPlan&, const NdbQueryOperationDef**,
  Uint32 op_idx, NdbQueryOptions&, const char* not_ancestor_msg);` About 28 lines saved. Risk: low.

## J-16: Duplication: NdbQuery error → exception in both drains

- Category: duplication. Severity: maintainability.
- Sites: `execute_join` 11853-11881 and `execute_passthrough_drain` 12275-12293. Both print
  "<label> failed: msg (code)", map SchemaError → `RonSQLMaybeStaleSchema`, and otherwise call
  `throw_classified_ndb_error`. Small divergences: the aggregate path copies the error and clears
  `details`; the pass-through path adds the "after rows were delivered" wording.
- Helper: `[[noreturn]] void throw_query_error(NdbQuery*, const char* label, const char* what,
  const char* where);` that uses `m_output_started` for the wording. About 25 lines saved. Risk: low.

## J-17: Duplication: CTE output type derivation

- Category: duplication. Severity: maintainability.
- Sites: `resolve_chained_column_type` (13192-13505, aggregate arms) and `build_cte_virtual_tables`
  (13611-13800, aggregate arms). They carry the same SUM/MIN/MAX/AVG widening tables and the same D1/D15/
  obc-11 display rules, kept in step by "mirror" comments. No divergence found today. The two differ in that
  build_cte_virtual_tables throws per-type messages where resolve returns false.
- Helper: `bool derive_cte_output_type(QueryScope& cte_scope, const Outputs* o, bool strict,
  DerivedType& out);`. `resolve_chained_column_type` keeps only the StoredColumn/CteResultColumn
  recursion, and `build_cte_virtual_tables` calls the helper with `strict=true`. About 200 lines saved.
  Risk: medium (display metadata). ronsql_cte_decimal, ronsql_avg_formatting and ronsql_cte_chained
  cover it.

## J-18: execute_join size, redundant linked operands, dummy projection column

- Category: duplication/design. Severity: maintainability; minor performance.
- `execute_join` (11291-11917) inlines three separable pieces: SELECT-list subquery leaf aggregator
  programming (11326-11515), CTE subtree emission (11536-11770) and the aggregate drain (11826-11905).
  Extracting `program_merged_leaf_aggregators()`, `emit_cte_subtrees()` and `drain_join_aggregate()`
  would leave a ~120-line orchestrator. The CTE single-table TABLE_SCAN body root (11634-11658) also
  re-implements `emit_root_op`'s table-scan fallback.
- `emit_child_ops` creates `keys[k] = qb->linkedValue(...)` for every op (15140-15146). The bounded
  INDEX_SCAN branch then creates a second set of linked operands for the same keys (15272-15276). The
  first set is never bound. That is harmless, but it is wasted builder work, and the duplicated parent
  column references are left to dedup. The parent-linked EQ bounds also create separate low and high
  operands (`bound_operand` memoizes only constants), so `appendBoundPattern` cannot collapse them to
  BoundEQ.
- The `JoinOp::CTE_SCAN` child arm (15465-15484) is dead: the planner creates CTE_SCAN only as a root
  (QueryPlanner.cpp 111).
- `execute_passthrough_drain` registers its dummy getValue on column 0 (12156-12172). That may be a wide
  CHAR/VARCHAR or a TEXT head. A PK column would be cheaper.
- Also: `check_qb()` (the J-6 fix) replaces about 30 `require_run(x != NULL, "Failed to create ...")` sites.

---

## J-19: Test gap: composite / permuted / hash-only PK roots in joins

- Every join test with a PK-equality root uses a single-column PK (ronsql_join_agg Test 14, the
  body_root_pk_residual.inc rpr-* cases). Nothing exercises `emit_pk_equality_index_scan_root` with a
  composite PK, a hash-only composite PK plus a permuted secondary index, or a residual that is too large
  for the lookup cap (13127).
- Add (ronsql_cte dd style, compared against mysqld): the J-1 shapes; the same with a residual
  > 64 words (for example a 30-value IN on a non-key column) and a pass-through readTuple root, so the
  fallback to the ordered-index scan is exercised.

## J-20: Test gap: CTE-root key lookups

- ronsql_cte_root_lookup covers only GROUP BY order = output order, in-range integers and numeric types.
  ronsql_cte_scalar Tests 10-12 cover only numeric scalar roots.
- Add: J-2 (permuted outputs, at the root and as a child, same and mixed types); J-3 (out-of-range
  INT/UNSIGNED/TINYINT keys); J-4 (string scalar root with WHERE, VARCHAR and LONGVARCHAR); a G4
  single-group root with permuted outputs.

## J-21: Test gap: size limits and builder-error classification

- No test reaches "Pushed join too large" (QueryPlanner) or the builder's QRY_DEFINITION_TOO_LARGE
  (grep found no result file with either). Add: (a) 16 joined tables where every child is a
  UNIQUE_LOOKUP (planner error, 2 nodes each); (b) 11+ CTEs, each looked up by the main query (builder node
  budget); (c) a root IN list of several thousand values on a non-indexed column (tree > 0xFFFF words).
  Each case asserts a clean LIMIT error (HTTP 400/413-class) rather than INTERNAL/500. With J-6 unfixed
  these document the current behaviour.

## J-22: Test gap: EXPLAIN access path, stale root index, NULL subquery in join bounds

- No EXPLAIN result shows a PK-equality root, a readTuple root or a lookupCte root (ronsql_parser_cte
  results show only TABLE_SCAN roots without WHERE). Add EXPLAIN cases for each root kind once J-9 adds
  the information, plus a chained-CTE_LOOKUP topology case.
- The QRY_UNRELATED_INDEX retry is documented only for child ops. Add a concurrent DROP INDEX / CREATE
  INDEX on the join root's ordered index between two RonSQL requests (the pattern ronsql_join_agg uses for
  schema-version suppressions) and expect success on retry.
- J-11: an empty scalar subquery in a join root equality and in a root range bound.

---

## Checked and found sound (no finding)

- `promote_left_joins`: ON equalities are null-rejecting, and `check_join_conditions` forces
  `child_table == joined alias`, so `parent_table` really is an earlier alias. The backward walk promotes
  transitively. The capacity caps are bounded by the planner's 32-op and 8-key limits. Subquery leaves
  appended later are always LEFT (`use_inner_join = false`, 8213), so no promotion is missed.
- `JoinQueryObjects` ordering: `singleAgg` (a local declared after `objs`) is destroyed first, but
  `setAggregation`/`defineCte` deep-copy the program (NdbQueryBuilder.cpp 799-811, 1404-1412), so there is
  no dangling use. The query is closed before its definition, and the definition before the virtual
  tables, on every path. The transaction is closed later by `cleanup_trans` (success) or
  `handle_ronsql_exception` (errors).
- Pass-through op mapping `numCteSubtreeOps + plan_op_idx`: `emit_root_op` and `emit_child_ops` each
  create exactly one operation per main plan op, in plan order. Extra ops (MIN/MAX self-join) live only
  in CTE subtrees.
- A NULL parent value in a parent-linked range bound: DBSPJ `scanFrag_parent_row` drops any key or
  bound pattern with `hasNull` (DbspjMain.cpp 13345-13398), which matches SQL UNKNOWN.
- Nullable high-only bounds: guarded for the root (`allow_nullable_high_bound=false`), cross-table
  bounds and child-constant bounds (4182-4192, 4234-4240).
- Root bound contiguity: `build_scan_config_candidates` blocks later columns whenever an eligible
  conjunct on another column is seen, so `emit_index_scan_root`'s `break` never drops a mapped conjunct.
  At most one low and one high conjunct map to a column, so the sticky `k_low_incl` is safe.
- A readTuple root that finds no row: NdbQuery treats 626 as a soft error (NdbQueryOperation.cpp
  3542-3547), and `execute()` returns 0.
- Pass-through LIMIT/ORDER BY: early close on LIMIT without ORDER BY; full buffering with caps
  (1M rows / 256 MB) for ORDER BY; NULLs first in ASC; `m_output_started` set only when rows stream, so a
  temporary error during a sorted drain is still retried.
