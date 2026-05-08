# Phase I.3 — Column-vs-column on CTE_LOOKUP output

## Status

**Shipped** in `8f8a8104a81` ("RONDB-1050: Phase I.3 -
column-vs-column on CTE_LOOKUP (Bigint-only)").  Builds on Phase
I.1 / I.2 (`emit_cte_lookup_filter` already walks atoms in DNF and
accepts column-vs-constant + IS [NOT] NULL).

## Scope

Lift `emit_cte_lookup_filter`'s column-vs-constant restriction so a
WHERE atom can reference **two columns from the same CTE_LOOKUP**:

```
WHERE  cte.a  <op>  cte.b
```

Both atoms must come from the same `op_idx` (same CTE), and both
must resolve to a `Bigint` virt-table column.  Operator set is the
same as today: `= != < <= > >=`.  Atoms can be combined via DNF /
AND under the existing I.1 / I.2 driver — no change there.

This covers the common useful case "two aggregate outputs" or "GB
column vs aggregate" *after* D2 widening promotes both sides to
`Bigint`:

- `SUM(...) > SUM(...)` — both Bigint by construction.
- `MAX(...) > MIN(...)` — D2 widens both to Bigint when source is
  signed integer.
- `bigint_gb_col != SUM(...)` — both Bigint.

**Out of scope, rejected with clear errors:**

- **Cross-CTE / cross-table comparisons.**  The kernel CTE filter
  dispatch (`s_cte_filter_handlers`) intentionally rejects
  `READ_ATTR_INTO_REG` and the `BRANCH_ATTR_*` family — there is no
  parent-table tuple bound to a CTE filter program.  A future phase
  would need to either widen the dispatch (kernel work) or push the
  predicate into the join condition.
- **Bigunsigned vs Bigunsigned, signed vs unsigned.**  The
  register-based comparison in DBTUP (`handleBranchLtRegReg` etc.)
  reads `Int64` and compares signed.  Bigunsigned values with the
  high bit set would mis-order.  Reject for now; can be added with a
  new kernel `BRANCH_*_REG_REG_UNSIGNED` opcode later.
- **Mixed-width columns** (Int vs Bigint, Smallint vs Bigint, etc.).
  Avoidable in the common case: aggregate outputs are 8-byte after
  D2; if a user still hits a non-Bigint column projection, they can
  cast in the CTE body (`SELECT CAST(col AS SIGNED) AS col_b ...`).
  Reject cleanly otherwise.
- **Float / Double**, **CHAR / VARCHAR / DECIMAL** — no
  register-based comparison primitive for these.  Reject.

## Background

The kernel's CTE filter dispatch table (`s_cte_filter_handlers` in
`DbtupExecQuery.cpp:8843`) already accepts every opcode this phase
needs:

- `READ_LINKED_TO_MEM` (39) — copies one entry of the linked-attr
  buffer (4-byte AttrHeader + N-byte data) into `cheapMemory[0]`.
- `READ_INT64_MEM_TO_REG` (52) — copies 8 bytes from a
  `cheapMemory` offset into a register slot.
- `BRANCH_EQ/NE/LT/LE/GT/GE_REG_REG` (12-17) — signed Int64 reg
  comparison.

No kernel change is needed.  The only API gap is that
`NdbInterpretedCode` does not currently expose `READ_LINKED_TO_MEM`
as a standalone instruction — it is emitted only inside the
`branch_linked_mem_*` / `branch_linked_inline_*` helpers.  Phase
I.3 adds a public method to emit it on its own.

## Codegen design

Per col-vs-col atom, RonSQL emits five instructions:

```
   READ_LINKED_TO_MEM  pos_left          ; cheapMemory[0] = AH+data_left
   READ_INT64_MEM_TO_REG  R1, 4          ; R1 = data_left (skip 4-byte AH)
   READ_LINKED_TO_MEM  pos_right         ; cheapMemory[0] = AH+data_right
   READ_INT64_MEM_TO_REG  R2, 4          ; R2 = data_right
   BRANCH_<inv-op>_REG_REG  R1, R2, fail ; jump to fail on NOT predicate
```

`<inv-op>` is the inverted operator (same trick as the constant path):
the user predicate is `R1 op R2` and we want to jump to `fail` when
the predicate is FALSE, so we emit the kernel's `BRANCH_<inv>_REG_REG`
that branches when the predicate is FALSE.  Mapping:

| User op  | Kernel inverse-branch          |
|---------|-------------------------------|
| `=`     | `BRANCH_NE_REG_REG`           |
| `!=`    | `BRANCH_EQ_REG_REG`           |
| `<`     | `BRANCH_GE_REG_REG`           |
| `<=`    | `BRANCH_GT_REG_REG`           |
| `>`     | `BRANCH_LE_REG_REG`           |
| `>=`    | `BRANCH_LT_REG_REG`           |

(Identical to the BranchReg* mapping in `emit_negated_branch` at
`RonSQLPreparer.cpp:7658`.)

Register numbers `R1=1, R2=2` are arbitrary but stable; the CTE
filter program has no contention on registers because each atom
emits its own load-load-compare sequence and registers are scratch.

The 5-instruction sequence has the same topology as the existing
`branch_linked_mem_eq` (which was a single helper that internally
emits READ_LINKED_TO_MEM + BRANCH_MEM_OP_ARG), so the per-atom
fail-label parametrisation from I.2 applies unchanged: `fail` is
either `REJECT` (last disjunct) or `FAIL_i` (non-last disjunct).

## API change

`storage/ndb/include/ndbapi/NdbInterpretedCode.hpp` /
`storage/ndb/src/ndbapi/NdbInterpretedCode.cpp` — add public method:

```cpp
/* Emit a standalone READ_LINKED_TO_MEM instruction.  Used by
 * column-vs-column CTE filter pushdown to load two linked-attr
 * buffer entries into cheapMemory before reading them into
 * registers.  Position is the 0-based index into the linked-attr
 * buffer (same as branch_linked_*).  Returns 0 on success. */
int read_linked_to_mem(Uint32 position);
```

Implementation is one line: `add1(Interpreter::READ_LINKED_TO_MEM | (position << 16))`.

## RonSQL change

In `emit_cte_lookup_filter`'s `emit_atom` lambda, between the
`is_cmp` check and the `is_const(left/right)` dispatch, branch on
**both sides T_IDENTIFIER**:

```cpp
if (left->op == T_IDENTIFIER && right->op == T_IDENTIFIER) {
  // Phase I.3: column-vs-column on the same CTE_LOOKUP.
  Uint32 col_l = left->col_idx;
  Uint32 col_r = right->col_idx;
  require_prm(scope.column_table_idx[col_l] == op_idx,
              "CTE_LOOKUP filter col-vs-col: left side must reference "
              "this CTE.");
  require_prm(scope.column_table_idx[col_r] == op_idx,
              "CTE_LOOKUP filter col-vs-col: right side must reference "
              "the same CTE.  Cross-CTE / cross-table comparisons are "
              "not yet supported.");
  Uint32 pos_l = (Uint32)scope.column_attrId_map[col_l];
  Uint32 pos_r = (Uint32)scope.column_attrId_map[col_r];
  const NdbDictionary::Column* vc_l = virtTab->getColumn((int)pos_l);
  const NdbDictionary::Column* vc_r = virtTab->getColumn((int)pos_r);
  require_prm(vc_l != NULL && vc_r != NULL,
              "CTE_LOOKUP filter col-vs-col: missing virt-table column.");
  const auto t_l = vc_l->getType();
  const auto t_r = vc_r->getType();
  require_prm(t_l == NdbDictionary::Column::Bigint &&
              t_r == NdbDictionary::Column::Bigint,
              "CTE_LOOKUP filter col-vs-col: both columns must resolve to "
              "Bigint (signed 64-bit).  Cast in the CTE body or use a "
              "constant-vs-column comparison.");

  const Uint32 R1 = 1, R2 = 2;
  require_prm(code.read_linked_to_mem(pos_l) == 0, "...");
  require_prm(code.read_int64_to_reg_const(R1, 4) == 0, "...");
  require_prm(code.read_linked_to_mem(pos_r) == 0, "...");
  require_prm(code.read_int64_to_reg_const(R2, 4) == 0, "...");

  int rc = -1;
  switch (atom->op) {
  case T_EQUALS:     rc = code.branch_ne(R1, R2, fail_label); break;
  case T_NOT_EQUALS: rc = code.branch_eq(R1, R2, fail_label); break;
  case T_LT:         rc = code.branch_ge(R1, R2, fail_label); break;
  case T_LE:         rc = code.branch_gt(R1, R2, fail_label); break;
  case T_GT:         rc = code.branch_le(R1, R2, fail_label); break;
  case T_GE:         rc = code.branch_lt(R1, R2, fail_label); break;
  default:
    require_prm(false,
                "CTE_LOOKUP filter col-vs-col: unsupported operator.");
  }
  require_prm(rc == 0,
              "CTE_LOOKUP filter col-vs-col: failed to emit reg-cmp "
              "branch.");
  return;
}
```

The existing `is_const` dispatch fires only when at least one side
is constant, so adding the col-vs-col branch above it cleanly splits
the two cases.  The pre-I.3 rejection text inside the final `else`
stays as the catch-all for "neither side is column or constant"
(e.g. expression on one side).

## Files

- `storage/ndb/include/ndbapi/NdbInterpretedCode.hpp` — new
  `read_linked_to_mem` method declaration.
- `storage/ndb/src/ndbapi/NdbInterpretedCode.cpp` — implementation
  (1-line `add1`).
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — col-vs-col branch
  inside `emit_atom`.
- `mysql-test/suite/ronsql/t/ronsql_cte_colvscol.test` — new file.
- `mysql-test/suite/ronsql/r/ronsql_cte_colvscol.result` — recorded.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_i.md`
  — flip I.3 status to "shipped".

## Test plan

New file `mysql-test/suite/ronsql/t/ronsql_cte_colvscol.test`,
fixture identical to `ronsql_cte_or` (cte_orders / cte_customer)
but with a second CTE that emits two Bigint outputs so col-vs-col
makes sense.

Use a CTE that GROUP BYs `o_custkey` (cast to Bigint via
`SUM(o_custkey)` trick or use `SELECT o_custkey AS k, SUM(o_amt) AS
total, MIN(o_amt) AS lo, MAX(o_amt) AS hi`) so we have multiple
Bigint aggregate outputs to compare.

Actually simpler: use two SUM-aggregate CTE columns, where
multiplying / adding the source column gives different totals:

```sql
WITH stats AS (
  SELECT o_custkey AS k,
         SUM(o_amt) AS gross,
         SUM(o_amt * 2) AS gross2  -- expression in aggregate may not be supported
  FROM cte_orders GROUP BY o_custkey)
```

If aggregate expressions aren't supported in RonSQL, fall back to
two CTEs joined.  Concrete tests:

| # | Shape | Verifies |
|---|-------|----------|
| 1 | `WHERE stats.gross > stats.lo` | `>` between two SUM-class Bigint outputs |
| 2 | `WHERE stats.gross != stats.hi` | `!=` |
| 3 | `WHERE stats.lo <= stats.hi` | `<=` (always true given semantics) |
| 4 | `WHERE stats.gross = stats.lo OR stats.k = 100` | OR mixing col-vs-col and col-vs-const |
| 5 | regression: `WHERE stats.gross > 100` (col-vs-const) | I.2 path unchanged |
| 6 | rejection: `WHERE c.c_id < stats.gross` (cross-table) | `--error 1` |
| 7 | rejection: `WHERE stats.k > stats.cnt` where `cnt` is Bigunsigned | `--error 1` |

Tests 1-5 use `ronsql_compare.inc`; 6-7 use `--exec
$RONSQL_CLI_EXE` + `--error 1`.

## Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_colvscol
./mtr --suite=ronsql                       # full suite — no regressions
./mtr --suite=ndb_push_agg                 # block tests — no regressions
```

## Commit cadence

Single commit:

| Commit | Contents | Approx LoC |
|---|---|---|
| 1 | NdbInterpretedCode `read_linked_to_mem` + RonSQL col-vs-col emit + MTR tests + status flip | ~150-200 |

## Risks

1. **AttrHeader skipping.**  `READ_LINKED_TO_MEM` writes
   `[AttrHeader (4B)][data]` to cheapMemory[0..].  We must read from
   offset 4, not 0, to skip the header.  Verified by reading
   `handleReadLinkedToMem` in `DbtupExecQuery.cpp:6664`.
2. **Register slot collisions inside DNF.**  Each atom uses scratch
   registers R1/R2 fresh — no carry-over expected, but verify by
   running the I.2 OR tests with col-vs-col atoms after this lands.
3. **NULL semantics.**  `BRANCH_*_REG_REG` raises
   `ZREGISTER_INIT_ERROR` if either reg has the NULL marker.  CTE
   columns under INNER JOIN are non-NULL; under LEFT JOIN the I.1
   guard already rejects.  An aggregate output of an empty group
   (e.g. SUM with no matching rows) — does the kernel inject NULL or
   zero?  Verify in-test with an unmatched-key shape; if NULL is
   surfaced, document and reject the LEFT-JOIN col-vs-col case.
4. **D2 widening edge cases.**  MIN/MAX over Bigunsigned source
   produces Bigunsigned virt-col → fails our Bigint check, rejected
   cleanly.  No silent mis-comparison.

## What we're not doing

- Cross-table / cross-CTE / parent-vs-CTE comparisons (kernel work
  needed to enable `READ_ATTR_INTO_REG` in CTE filter mode, plus a
  way to address the parent's tuple from a child's filter program).
- Bigunsigned full support (needs unsigned-aware reg-reg branch
  opcodes in DBTUP).
- Float / Double / DECIMAL / CHAR / VARCHAR.
- Expression on either side (`WHERE col1 + 1 > col2`) — would need
  arithmetic ops in the CTE filter program; currently rejected by
  the same `else` branch.
