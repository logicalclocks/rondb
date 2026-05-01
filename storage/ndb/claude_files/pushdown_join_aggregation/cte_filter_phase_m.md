# Phase M — Remove aggregation-program BranchReg control flow

## Status

**Shipped** in commit `a28ed6d817f`
(`RONDB-1050: remove aggregation BranchReg control flow`).
Inventory checklist landed alongside as
`cte_filter_phase_m1_inventory.txt`.

Phase M fixed an architectural violation that entered through the
RONDB-1044 cross-table filter work and was then reused by the Phase
I.5 v2b draft.  Aggregation programs no longer contain their own
branch / filter semantics — every comparison-and-skip lives in the
normal interpreter, reached via `kOpEmbeddedInterp`.

## What shipped

- **`BranchReg*` removed everywhere.**  Opcodes
  (`kOpBranchRegLt/Le/Gt/Ge/Eq/Ne`) gone from
  `NdbAggregationCommon.hpp`; public API
  (`NdbAggregator::BranchRegLt/Le/Gt/Ge/Eq/Ne`) removed from
  `NdbAggregator.hpp` / `.cpp`; handlers retired from
  `AggInterpreter.cpp`, `JoinAggInterpreter.cpp`, and
  `PushdownInterpreter.cpp`.
- **Aggregation→normal-interpreter bridge.**  New
  `READ_AGG_REG_TO_REG` opcode (`Interpreter::ReadAggRegIntoReg`)
  copies an aggregation register into a normal interpreter register
  so an embedded program can compare it with the normal-interpreter
  branch family.  The opcode is only valid from `kOpEmbeddedInterp`;
  it is not exposed through normal interpreted code or CTE filters.
- **Pair-op emission rewritten.**  `Greatest2` / `Least2` SVM ops
  (kept in place) now lower at `programAggregator` /
  `programAggregator_join` time to a 9-word embedded normal-
  interpreter program followed by `Mov(dest, src)`:
  ```text
  EmbeddedInterp(9)
    READ_AGG_REG_TO_REG(dest → reg1)
    READ_AGG_REG_TO_REG(src  → reg2)
    BRANCH_(GE|LE)_REG_REG(reg2, reg1, +4)   // skip the "set output=0"
    LoadConst16(reg3, 0); WriteInterpreterOutput(reg3); ExitOK
    LoadConst16(reg3, 1); WriteInterpreterOutput(reg3); ExitOK
  Mov(dest, src)                              // skipped iff output==1
  ```
  The aggregation interpreter only consumes the embedded program's
  scalar output; comparison + branching live in the normal
  interpreter.
- **Cross-table subquery filter / DNF emission rewritten.**  The
  RONDB-1044 conditional-aggregation path (`ronsql_subquery_agg_ext`
  K4 / M2 / M4 / M7 etc.) and the merged-leaf DNF fallback now build
  embedded normal-interpreter condition bytecode instead of
  aggregation-program `BranchReg*` chains.  `kOpEmbeddedInterp`
  gained a "stop aggregation program for this row" outcome so a
  failed predicate skips the rest of the aggregate updates without
  the aggregation interpreter running its own branch.
- **Test rewrites.**  `testCteNdbApi.cpp` Test 21 reworked to use
  `EmbeddedInterp`; one new helper added in `testJoinAggNdbApi.cpp`.
  `ronsql_cte_greatest_least_v2b.test` comments refreshed to
  describe the embedded-program shape.
- **NDB API surface cleanup.**  All public BranchReg methods on
  `NdbAggregator` removed in this same commit — there is no
  compatibility shim, since no external consumer exists yet.

## Background (pre-Phase-M state)

The aggregation interpreter shipped with `kOpBranchReg*` opcodes
(`Lt`, `Le`, `Gt`, `Ge`, `Eq`, `Ne`).  These duplicated functionality
already present in the normal interpreter and mixed two different
concerns:

1. **Filter execution**: decide whether a row participates at all.
2. **Aggregation execution**: apply aggregation logic for rows that
   passed filtering.

The pre-Phase-M `BranchReg` use could skip aggregate instructions
entirely.  That made aggregation execution act as a filter, which was
not the intended execution model.

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

## Use cases addressed

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

### 4. Phase I.5 v2b (already shipped at `959d45a20ce`)

v2b's `Greatest2` / `Least2` SVM pair ops originally expanded to
`BranchRegGe + Mov` / `BranchRegLe + Mov`.  Phase M kept the SVM-level
ops (the value-producing pair-op shape was the right model) and only
rewrote the kernel emission to use `kOpEmbeddedInterp +
READ_AGG_REG_TO_REG + BRANCH_(GE|LE)_REG_REG` followed by `Mov`, as
documented above.  Phase I can resume once Phase M's other follow-ups
(v3 / v4 / v5 / v6) are picked up.

## Architecture (current)

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
If it simplifies implementation, `kOpEmbeddedInterp` may be extended so
the embedded normal-interpreter program can return a special
"stop aggregation program for this row" code.  That is acceptable
because the branch semantics still live in the normal interpreter; the
aggregation interpreter only consumes the embedded interpreter result.

### Aggregation Execution

Aggregation execution applies aggregate operations to rows that passed
filter execution.

Conditional value selection inside aggregation is still allowed, but
must use `kOpEmbeddedInterp`.  The embedded program is delegated to the
normal interpreter and returns the skip offset via
`WRITE_INTERPRETER_OUTPUT`, as existing CASE lowering already does.
Phase M may add a second embedded-interpreter outcome that stops the
rest of the aggregation program for the current row.  This is only for
row-rejection predicates that cannot conveniently be moved into a
separate filter program; it must not reintroduce comparison opcodes into
the aggregation interpreter.

Phase M may also add aggregation-embedded-only normal-interpreter opcodes
needed to bridge values into the embedded program.  In particular,
`READ_AGG_REG_TO_REG` copies an aggregation register into a normal
interpreter register so the embedded program can compare it with the
normal interpreter's existing branch opcodes.  This opcode is only valid
from `kOpEmbeddedInterp`; it is not exposed through normal interpreted
code or CTE filters.

Allowed control flow in aggregation programs after Phase M:

- `kOpEmbeddedInterp` as the bridge to the normal interpreter,
- optional `kOpEmbeddedInterp` stop-program result for row rejection,
- `kOpSkip` only as structural CASE-arm control emitted by the SVM
  compiler,
- no `kOpBranchReg*` opcodes.

## Implementation log (reference)

The original step-by-step plan is preserved below for posterity; each
step is now done.  Use this section as a map back into the commit if
a follow-up question arises.

### M.1 Inventory and guards (done)

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

### M.2 Cross-table filter fallbacks rewritten (done)

For cross-table predicates that currently become conditional
aggregation:

1. Move any predicate that can be represented as an index bound or
   child scan filter into normal filter execution.
2. For predicates that require linked parent values, emit an embedded
   interpreter filter program instead of `BranchReg*`.
3. Ensure the embedded program can access the linked-attribute buffer
   through the existing `READ_LINKED_TO_MEM` path.
4. The embedded program must reject the row before aggregate update.
   This can be implemented either as a separate filter program, or as
   `kOpEmbeddedInterp` returning a special stop-program result consumed
   by the aggregation interpreter.

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

Alternative acceptable shape:

```text
Aggregation execution:
  EmbeddedInterp(...)
    normal interpreter loads local / linked values
    normal interpreter branches
    returns continue or stop-aggregation-program
  LoadColumn(...)
  Sum/Count/Min/Max(...)
```

For cases where the current aggregation program still needs a CASE-like
choice between two aggregate expressions, keep using
`kOpEmbeddedInterp` plus SVM CASE arms.  Do not use `BranchReg*`.

### M.3 Merged / OR cross-table filter emission rewritten (done)

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

### M.4 Phase I.5 v2b BranchReg lowering replaced (done)

The pair-op SVM shape (`Greatest2` / `Least2`) was kept — it remains
the right model — but the kernel emission was rewritten to use
`kOpEmbeddedInterp + READ_AGG_REG_TO_REG + BRANCH_(GE|LE)_REG_REG`
plus `Mov`.  v2a's explicit-CASE col-vs-col path is unchanged; it
already went through embedded interpreter bytecode and was the desired
model from the start.  v2b doc + I.5 doc updated to describe the new
emission.

### M.5 NDB API surface cleanup (done — full removal, no shim)

- `BranchRegLt/Le/Gt/Ge/Eq/Ne` removed from `NdbAggregator.hpp`.
- Implementations removed from `NdbAggregator.cpp`.
- `kOpBranchReg*` removed from `NdbAggregationCommon.hpp`.
- Handlers and preprocessor cases removed from `AggInterpreter`,
  `JoinAggInterpreter`, and `PushdownInterpreter`.

No deprecation shim landed because there are no external consumers of
the API yet.

### M.6 Test rewrite (done)

- `ronsql_subquery_agg_ext` K4 / M2 / M4 / M7 still pass; emission now
  goes through normal filter / embedded interpreter execution.
- `testCteNdbApi.cpp` Test 21 reworked to use `EmbeddedInterp` instead
  of the retired `BranchRegGe + Mov` API.
- `testJoinAggNdbApi.cpp` extended with one helper used by the rewrite.
- `ronsql_cte_greatest_least_v2b.test` comments refreshed to describe
  the new embedded-interpreter pair-op shape (no test cases removed).

### M.7 Documentation cleanup (done in this commit)

Refreshed alongside Phase M's flip-to-shipped:

- `cte_filter_phase_m.md` (this file)
- `cte_filter_phase_i5_v2b.md` — pair-op emission described as the
  embedded-interpreter shape, not `BranchReg + Mov`
- `cte_filter_phase_i5_v2.md` — v2b retro-update
- `cte_filter_phase_i5.md` — pointer to v2b's current emission
- `cte_filter_phase_i.md` — catalogue entry refreshed
- `CLAUDE.md` index — Phase M entry already present
- Memory `project_cte_branch_state.md` — Phase M added; v2b entry
  rewritten to describe the embedded-interpreter emission

Documents now consistently state:

- filter execution happens before aggregation execution,
- aggregation programs do not contain branch/filter logic,
- conditional aggregation value selection uses `kOpEmbeddedInterp`,
- row rejection uses normal filter execution (with the optional
  embedded-interpreter "stop aggregation program for this row"
  outcome).

## Acceptance criteria (verified)

- `rg "BranchReg|kOpBranchReg"` finds no production emitter or
  aggregation-interpreter handler.  Remaining hits live only in
  historical / inventory docs explicitly marked as such.
- No RonSQL query path emits `NdbAggregator::BranchReg*` (the API is
  gone).
- Cross-table subquery filter tests pass via the embedded-interpreter
  path.
- CASE aggregation tests pass.
- Phase I.5 v2b runs through the embedded-interpreter pair-op
  emission, not aggregation-program branch opcodes.
- Public NDB API branch methods removed; no compatibility shim
  required.
