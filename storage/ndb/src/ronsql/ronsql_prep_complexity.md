# RonSQL Preparation Phase — Algorithmic Complexity Analysis

Date: 2026-03-26

## Context

RonSQL query preparation takes ~2ms for a typical 2-3 table join with ~20 columns.
This is not the bottleneck (data node execution dominates), but documents areas
for optimization if preparation becomes significant with complex queries.

## Critical: O(n²) patterns

### 1. merge_same_table_subqueries() — O(S² × C)

**Location:** RonSQLPreparer.cpp lines ~2832-2882

Pairwise comparison of all subqueries (O(S²)), each with 3-4 `strncmp()` calls
on table name, inner join column, and outer join column. For each match, a linear
scan of ALL columns (O(C)) remaps qualifiers via `strncmp()`.

With MAX_SQL_SUBQUERIES=128, worst case is 16K pairs × C string comparisons.

**Fix:** Hash map of (table_name, join_col, outer_col) → leaf_idx, and direct
index lookup for column qualifier remapping instead of scanning all columns.

### 2. analyze_select_subqueries() — O(S × N² × C)

**Location:** RonSQLPreparer.cpp lines ~2532-2807

For each subquery output, flattens WHERE into conjuncts, then searches for the
correlation predicate (O(N)). For each match, rebuilds the remaining filter by
looping conjuncts again (O(N)). Column qualifier remapping at lines ~2870-2877
scans all columns per merge.

**Fix:** Pre-flatten conjuncts once; use index-based filter reconstruction.

## Medium: O(n × m) patterns

### 3. load_join() column resolution — O(C × T)

**Location:** RonSQLPreparer.cpp lines ~843-919

For each column, linear scan through all join table aliases with `strcmp()`.
For unqualified columns, also calls `getColumn()` on every table.

With 50 columns and 5 tables = 250 table lookups with string comparison.

**Fix:** Pre-build alias → op_idx hash map. O(C × T) → O(C).

### 4. find_or_add_linked_proj() — O(L) per call

**Location:** RonSQLPreparer.cpp line ~1204

Linear scan with `strcmp()` through existing linked projections. Called in loops
from `build_agg_linked_projections()`, giving O(P × L) where P = program
instructions and L = linked projections.

Bounded by MAX_LINKED_PROJS=16, so at most 16 comparisons per call.

**Fix:** Hash map of (op_idx, col_name) → position. Minor impact due to small L.

### 5. generate_scan_config_candidates() — O(I × C × W)

**Location:** RonSQLPreparer.cpp lines ~1404-1486

Triple nested loop: indexes × index columns × WHERE conditions. Each iteration
does `strcmp()` to match column names.

Typical: 5 indexes × 4 columns × 20 conditions = 400 string comparisons.

**Fix:** Pre-build column_name → conditions map. O(I × C × W) → O(I × C + W).

## Summary

| Phase | Complexity | Dominant cost | Bounded by |
|-------|-----------|---------------|------------|
| merge_same_table_subqueries | O(S² × C) | strncmp in nested loops | S=128, C=columns |
| analyze_select_subqueries | O(S × N² × C) | conjunct search + remap | S=128, N=conjuncts |
| load_join column resolution | O(C × T) | strcmp per column per table | T=MAX_SPJ_TREE_NODES=32 |
| find_or_add_linked_proj | O(P × L) | strcmp per call | L=MAX_LINKED_PROJS=16 |
| scan_config_candidates | O(I × C × W) | strcmp triple loop | I=indexes, C=cols, W=conditions |

For current workloads (2-5 tables, <50 columns, <10 subqueries), these are all
fast. The quadratic patterns would become noticeable with >10 subqueries or >100
columns. All can be fixed with hash maps if needed.
