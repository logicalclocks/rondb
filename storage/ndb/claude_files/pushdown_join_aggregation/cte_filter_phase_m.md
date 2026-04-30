# Phase M — Remove aggregation-program BranchReg control flow

## Status

**Planned.  Must run before continuing Phase I.**

Phase M fixes an architectural violation that entered through the
RONDB-1044 cross-table filter work and was then reused by the Phase
I.5 v2b draft.  Aggregation programs must not contain their own
branch/filter semantics.  Filtering belongs to filter execution, and
conditional value selection inside aggregation must be expressed
through `kOpEmbeddedInterp`, which delegates to the normal interpreter.

## Problem

The aggregation interpreter currently has `kOpBranchReg*` opcodes:

- `kOpBranchRegLt`
- `kOpBranchRegLe`
- `kOpBranchRegGt`
- `kOpBranchRegGe`
- `kOpBranchRegEq`
- `kOpBranchRegNe`

These duplicate functionality already present in the normal
interpreter.  They also mix two different concerns:

1. **Filter execution**: decide whether a row participates at all.
2. **Aggregation execution**: apply aggregation logic for rows that
   passed filtering.

The current `BranchReg` use can skip aggregate instructions entirely.
That makes aggregation execution act as a filter, which is not the
intended execution model.

## Existing Use Cases To Remove

### 1. RonSQL cross-table subquery filters

Committed RONDB-1044 code emits `BranchReg*` for correlated subquery
filters such as:

```sql
SELECT o.id AS order_id, o.min_qty,
  (SELECT SUM(l.qty)
   FROM k_lineitem AS l
   WHERE l.order_id = o.id
     AND l.qty > o.min_qty) AS filtered_qty
FROM k_customer AS c
JOIN k_orders AS o ON o.cust_id = c.id;
```

Current shape:

```text
LoadLinkedColumn(o.min_qty)
LoadColumn(l.qty)
BranchRegLe(l.qty, o.min_qty, skip aggregation)
LoadColumn(l.qty)
Sum(...)
```

This path is in `RonSQLPreparer.cpp` around the cross-table filter
emission and is covered by `ronsql_subquery_agg_ext` tests K4, M2,
M4, M7, and related cross-table filter tests.

### 2. RonSQL merged / join aggregation cross-table filters

Later RonSQL code emits the same `BranchReg*` family for remaining
cross-table filters in join aggregation programs, including OR / DNF
fallback shapes.

Current shape: branch inside the aggregation program to skip the
aggregate instruction block.

### 3. NDB API direct BranchReg test

`testCteNdbApi.cpp` Test 21 uses:

```cpp
mainAgg.BranchRegGe(0, 1, 1);
mainAgg.Mov(0, 1);
```

This must be rewritten to use `EmbeddedInterp` if the test remains, or
removed if the test exists only to cover the obsolete BranchReg API.

### 4. Phase I.5 v2b draft

The current uncommitted v2b draft models `GREATEST` / `LEAST` as
`Greatest2` / `Least2` SVM pair ops that expand to:

```text
BranchRegGe + Mov
BranchRegLe + Mov
```

This must be replaced before v2b proceeds.  Phase I should pause until
Phase M is complete.

## Target Architecture

### Filter Execution

Filters must execute before aggregation.

If a predicate can be expressed as a normal scan/key filter, keep it
there.  If the predicate needs linked parent columns, CTE linked
columns, or other values not available to a plain table filter, express
it through the normal interpreter path that can load:

- local columns,
- linked columns,
- constants,
- and branch with the existing normal interpreter branch opcodes.

The plan must not use aggregation-program `BranchReg*` to reject rows.

### Aggregation Execution

Aggregation execution applies aggregate operations to rows that passed
filter execution.

Conditional value selection inside aggregation is still allowed, but
must use `kOpEmbeddedInterp`.  The embedded program is delegated to the
normal interpreter and returns the skip offset via
`WRITE_INTERPRETER_OUTPUT`, as existing CASE lowering already does.

Allowed control flow in aggregation programs after Phase M:

- `kOpEmbeddedInterp` as the bridge to the normal interpreter,
- `kOpSkip` only as structural CASE-arm control emitted by the SVM
  compiler,
- no `kOpBranchReg*` opcodes.

## Implementation Plan

### M.1 Inventory and Guards

Search and classify every use of:

```text
BranchRegLt
BranchRegLe
BranchRegGt
BranchRegGe
BranchRegEq
BranchRegNe
kOpBranchReg
FORALL_PAIR_OPS
Greatest2
Least2
```

Known files to inspect:

- `storage/ndb/include/ndbapi/NdbAggregationCommon.hpp`
- `storage/ndb/include/ndbapi/NdbAggregator.hpp`
- `storage/ndb/src/ndbapi/NdbAggregator.cpp`
- `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp`
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp`
- `storage/ndb/src/kernel/blocks/dbtup/PushdownInterpreter.cpp`
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/src/ronsql/AggregationAPICompiler.*`
- `storage/ndb/block_unit_test/testCteNdbApi.cpp`
- `mysql-test/suite/ronsql/t/ronsql_subquery_agg_ext.test`
- `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v2b.test`

Add a temporary review checklist to the commit notes proving no
production path emits aggregation `BranchReg*` after the phase.

### M.2 Replace Cross-Table Filter Fallbacks

For cross-table predicates that currently become conditional
aggregation:

1. Move any predicate that can be represented as an index bound or
   child scan filter into normal filter execution.
2. For predicates that require linked parent values, emit an embedded
   interpreter filter program instead of `BranchReg*`.
3. Ensure the embedded program can access the linked-attribute buffer
   through the existing `READ_LINKED_TO_MEM` path.
4. The embedded program must reject the row before aggregate update, not
   skip aggregate instructions as an aggregation concern.

The resulting execution shape should be:

```text
Filter execution:
  load local / linked values
  branch through normal interpreter
  accept or reject row

Aggregation execution:
  LoadColumn(...)
  Sum/Count/Min/Max(...)
```

For cases where the current aggregation program still needs a CASE-like
choice between two aggregate expressions, keep using
`kOpEmbeddedInterp` plus SVM CASE arms.  Do not use `BranchReg*`.

### M.3 Replace Merged / OR Cross-Table Filter Emission

The current DNF fallback emits BranchReg chains and `Skip` instructions.
Replace it with a single embedded interpreter condition where possible.

Required cases:

- simple comparison,
- top-level AND,
- top-level OR / DNF,
- EQ / NE,
- LT / LE / GT / GE,
- linked parent columns,
- local leaf columns,
- constants.

The embedded interpreter already owns the branching semantics, so the
RonSQL emitter should build condition bytecode, not aggregation skip
bytecode.

### M.4 Remove Phase I.5 v2b BranchReg Design

Before committing v2b:

1. Remove `Greatest2` / `Least2` SVM pair ops if their only runtime
   lowering is `BranchReg* + Mov`.
2. Rework n-ary `GREATEST` / `LEAST` to use existing CASE /
   `kOpEmbeddedInterp` machinery, or defer v2b until the needed normal
   interpreter value-selection support exists.
3. Update `cte_filter_phase_i5_v2b.md` and related docs so they no
   longer describe `BranchReg*` as the intended implementation.
4. Keep v2a explicit CASE col-vs-col support separate; it already goes
   through embedded interpreter bytecode and is the desired model.

### M.5 NDB API Surface Cleanup

Decide whether `NdbAggregator::BranchReg*` can be removed immediately.

Preferred outcome:

- remove `BranchRegLt/Le/Gt/Ge/Eq/Ne` from `NdbAggregator.hpp`,
- remove their implementations from `NdbAggregator.cpp`,
- remove `kOpBranchReg*` from `NdbAggregationCommon.hpp`,
- remove handlers and preprocessor cases from `AggInterpreter`,
  `JoinAggInterpreter`, and `PushdownInterpreter`.

If ABI/API compatibility requires a transition:

- mark the API as unsupported/deprecated,
- make the methods fail cleanly,
- keep no production RonSQL use,
- create a follow-up removal ticket.

### M.6 Test Rewrite

Update tests so they verify the intended execution model:

- `ronsql_subquery_agg_ext`: K4, M2, M4, M7, and related cross-table
  filter tests should still pass, but the implementation must use
  normal filter / embedded interpreter execution, not BranchReg.
- Add a negative/trace-oriented check where practical to ensure no
  `BranchReg*` opcodes are emitted in aggregation programs.
- Rewrite or remove `testCteNdbApi.cpp` Test 21.
- Remove v2b tests that depend on `Greatest2` / `Least2` BranchReg
  lowering; reintroduce them after the non-BranchReg design is ready.

### M.7 Documentation Cleanup

Update or retire documents that currently describe BranchReg as valid
aggregation-program control flow:

- `cte_filter_phase_i5_v2.md`
- `cte_filter_phase_i5_v2b.md`
- `cte_filter_phase_i5.md`
- `cte_filter_phase_i.md`
- `cte_filter_phase_i4.md`
- `ronsql_index_join_analysis.html`
- `j4_execution_flow.html`
- `dbspj_parallelism_analysis.html`
- `CLAUDE.md`

The documentation should clearly state:

- filter execution happens before aggregation execution,
- aggregation programs do not contain branch/filter logic,
- conditional aggregation expression selection uses
  `kOpEmbeddedInterp`,
- row rejection uses normal filter execution.

## Acceptance Criteria

- `rg "BranchReg|kOpBranchReg"` finds no production emitter or
  aggregation interpreter handler, except in historical docs explicitly
  marked obsolete or in compatibility shims if temporarily retained.
- No RonSQL query path emits `NdbAggregator::BranchReg*`.
- Cross-table subquery filter tests still pass.
- CASE aggregation tests still pass.
- Phase I.5 v2b work is paused or redesigned so it does not depend on
  aggregation-program branch opcodes.
- Any remaining public NDB API branch methods are either removed or fail
  cleanly with a documented compatibility rationale.
