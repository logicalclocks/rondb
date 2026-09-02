# nullable_bounds family — findings

CONFIRMED wrong-results bug (by kernel-to-planner code audit; fixed
before any recorded baseline could manifest it): NULL sorts below every
value in an NDB ordered index (`NdbPack::Iter::cmp`; DBTUX
`m_storeNullKey = true`), and a HIGH-only index bound leaves the low
side empty so `execTUX_BOUND_INFO`/`searchToScan` position at the index
head — NULL entries included — while SQL comparison is UNKNOWN for
NULL.  RonSQL consumed `col <= X` on a nullable indexed column as a
bare bound (never re-applied as a filter), returning/aggregating NULL
rows SQL excludes.  Nullability was never consulted on any root bound
path.  Three instances, three fixes (`body_nullable_bounds.inc` nb-1..9
pins them ×5 topology suites):

| Instance | Path | Fix | Pinned by |
|---|---|---|---|
| Single-table bounds (aggregate + pass-through arms share `open_single_table_scan_op`) | `plan_index_and_filter` → `build_scan_config_candidates` → `setBound` | KEEP the bound and append the mysqld range-optimizer idiom `setBound(col, BoundLT, /*aValue=*/NULL)` = "col > NULL" (`setBoundHelperOldApi` sets the null bit; `range_analysis.cc`'s `// > NULL` is the precedent) — plan unchanged | nb-1..3 (EXPLAIN pins the bound is KEPT), nb-9 (ORDER BY index-order interplay) |
| Join-root + CTE-body root (`emit_index_scan_root` via `select_root_scan_config`) | NdbQueryBuilder emit — `constValue` rejects NULL pointers, no `constNull`, `paramValue` rejects null actuals: a NULL-excluding low bound is INEXPRESSIBLE | generator guard: `build_scan_config_candidates` gained `table` + `allow_nullable_high_bound` params; when false, a nullable high-only column's conjuncts revert to residual filters (candidate typically falls back to TABLE_SCAN) | nb-6 (join root), nb-7 (CTE body) |
| Cross-table child bounds (RONDB-1044, `cross_table_bound_op`) | same NdbQueryBuilder emit | precise guard: normalized `T_LT`/`T_LE` on a nullable child column returns no-match (stays a cross-table filter); EQ pairs and low bounds exclude NULLs naturally and stay bounds | nb-8 (high → filter), nb-8b (low → bound kept) |
| Embedded cross-table filter NULL operands (found by nb-8's FIRST record: data-node "Embedded interpreter error (code 1872)" = ZAGG_EMBEDDED_INTERP_ERROR ← ZREGISTER_INIT_ERROR — the reg-reg branches error on NULL registers, failing the whole query instead of filtering the row; classified TemporaryError → a 10-attempt retry storm) | `generate_embedded_filter_condition` / `emit_embedded_filter_expr` | nullable operands now emit a `BRANCH_REG_EQ_NULL` guard after their load (UNKNOWN rejects the atom: AND → false exit, OR → next disjunct; word counts + branch offsets threaded via `cur_pos`/`null_fail_target`); the linked-side load upgraded from the pre-v5 `READ_LINKED_TO_MEM` + raw `READ_*_MEM_TO_REG` pair (no NULL flag, zero-extended signed sub-Bigint) to the typed `READ_LINKED_COLUMN_TO_REG` | nb-8 (nullable leaf operand with NULL rows) |

Safe directions (bounds kept, pinned): low-only (`>=`, nb-4 — the
range starts above the NULL block), closed ranges (nb-5), equality.
`child_const_bound_op`'s v1 conservative full-nullable reject predates
this and stays as-is (cb-8's recorded baseline).

Near-miss corpus notes: `ronsql_dbt3_1_2` / `ronsql_date_sub` have the
exact shape (`l_shipDATE <= date_sub(...)` on nullable indexed) with
NULL-free fixtures — fix B keeps their index plans;
`ronsql_emptytable_and_nulls` / `ronsql_basic` bound nullable columns
only in closed ranges.  All four are regression-pinned unchanged.
