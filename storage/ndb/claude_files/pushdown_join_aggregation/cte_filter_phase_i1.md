# CTE filter Phase I.1 — `IS NULL` / `IS NOT NULL` on CTE column or aggregate output

## Status

**Plan only.**  No code yet.

## Context

`emit_cte_lookup_filter` (`RonSQLPreparer.cpp:5641-5647`) currently
accepts only the six binary comparisons (`= != < <= > >=`).  Lifting
the rejection for `T_IS` (parser already produces `T_IS` nodes with
`is.arg` and `is.null`, see `RonSQLParser.y:539-540`) gives users
the standard SQL `IS NULL` / `IS NOT NULL` predicates on CTE
columns and aggregate outputs.

The interpreter today has direct-column null-branches
(`branch_col_eq_null` / `branch_col_ne_null`,
`NdbInterpretedCode.hpp:869-870`) but no equivalent for **linked**
(parent-table-projected) columns.  CTE values arrive at the kernel
filter via the linked-attr buffer, so the existing direct-column
opcodes don't apply.

The catalogue entry (`cte_filter_phase_i.md` I.1) flagged this as
"S/M" — that estimate held only if existing opcodes already
supported the operation.  After investigation, a new kernel opcode
is needed to do it correctly.  Scope realistic for this phase:
**M/L** (kernel opcode + NDB-API method + RonSQL emit + Phase J
interaction).

## Why a new kernel opcode is needed

Looking at the linked-attr-buffer format
(`Dblqh::buildCteLinkedBuffer`, `cteScanAggFeed`):

```
[tableId][schemaVersion][AttributeHeader][data ...]
```

For a NULL value the AttributeHeader is `init(attrId, 0)` — `dataSize
== 0`, no payload — and `AttributeHeader::isNULL()` returns true.

`READ_LINKED_TO_MEM` (`Interpreter.hpp:196`,
`DbtupExecQuery.cpp:6664-6698`) handles NULL correctly: it copies
the AH (size 0) into `cheapMemory[0]` and returns INTERP_CONTINUE.
At that point `AttributeHeader(memory_ptr[0]).isNULL()` is the
authoritative check.

What's missing is a branch opcode that examines just the
just-loaded AH's NULL flag.  `BRANCH_MEM_OP_ARG` and
`BRANCH_MEM_OP_ARG_INLINE_TYPE` both do value comparisons; they
don't have an "is-null" cond code today.

## Why we can't shortcut to "always-reject / always-accept"

A tempting shortcut: CTE GB keys are never NULL by construction
(GB groups don't include NULL keys), and SUM / COUNT aggregate
results are never NULL on a non-empty group.  So `WHERE col IS
NULL` could be emitted as "always reject" and `IS NOT NULL` as
"always accept", with no kernel changes.

That shortcut breaks for **MIN / MAX over an all-NULL source**:
when every input row in a group has NULL for the aggregate's
input, MIN/MAX returns NULL.  The current pushdown grammar already
accepts MIN/MAX on numeric-source CTE outputs (Phase D2), so the
shortcut would silently give wrong results for any group with all
NULLs at the source.

A real null-flag check is correct in all cases and adds 1 opcode
of surface area.

## Design

### Step 1 — kernel opcode `BRANCH_LINKED_NULL`

Add a new opcode at slot 41 (the next free slot per
`Interpreter.hpp:232` "41-46 free"):

```cpp
/**
 * BRANCH_LINKED_NULL: Branch based on AttributeHeader.isNULL() of
 * the entry already loaded into cheapMemory[0] by READ_LINKED_TO_MEM.
 *
 * Used by CTE filter mode for `WHERE col IS NULL` /
 * `WHERE col IS NOT NULL` on a CTE column or aggregate output.
 *
 * Word layout:
 *   Word 0: opcode | sense_bit | branch_offset
 *           sense_bit (bit 16): 0 = branch when isNULL, 1 = branch
 *           when not isNULL.  Two senses are emitted by separate
 *           NDB-API methods (branch_linked_isnull /
 *           branch_linked_isnotnull).
 *   Single-word instruction.
 */
static constexpr Uint32 BRANCH_LINKED_NULL = 41;
```

Handler in DbtupExecQuery.cpp:

```cpp
static inline int handleBranchLinkedNull(InterpreterContext& ctx) {
  ctx.RnoOfInstructions += 1;
  Uint32* memory_ptr = (Uint32*)&ctx.TheapMemoryChar[0];
  AttributeHeader ah(memory_ptr[0]);
  bool sense_isnotnull = (ctx.theInstruction >> 16) & 0x1;
  bool branch = sense_isnotnull ? !ah.isNULL() : ah.isNULL();
  if (branch) {
    ctx.TprogramCounter =
        ctx.tup->brancher(ctx.theInstruction, ctx.TprogramCounter);
  }
  return INTERP_CONTINUE;
}
```

Wire into the dispatch tables alongside the other 38/39/40 entries
(both `s_op_handlers` and the lookup-mode disabled list).

### Step 2 — NDB-API methods

Add to `NdbInterpretedCode.hpp/cpp`:

```cpp
/* Branch when linked column at `position` IS NULL. */
int branch_linked_isnull(Uint32 position, Uint32 label);

/* Branch when linked column at `position` IS NOT NULL. */
int branch_linked_isnotnull(Uint32 position, Uint32 label);
```

Both emit a 2-instruction sequence:
1. `READ_LINKED_TO_MEM` with the position
2. `BRANCH_LINKED_NULL` with the appropriate sense bit

(Same compound-emit pattern as `branch_linked_mem_eq` /
`branch_linked_inline_eq`.)

### Step 3 — RonSQL `emit_cte_lookup_filter` extension

In the conjunct loop at `RonSQLPreparer.cpp:5638-5885`, accept
`atom->op == T_IS` ahead of the existing `is_cmp` check:

```cpp
for (Uint32 c = 0; c < num_conjuncts; c++) {
  ConditionalExpression* atom = conjuncts[c];

  if (atom->op == T_IS) {
    ConditionalExpression* col_side = atom->is.arg;
    require_prm(col_side != NULL && col_side->op == T_IDENTIFIER,
                "CTE_LOOKUP filter: IS NULL operand must be a "
                "column reference.");
    Uint32 col_idx = col_side->col_idx;
    require_prm(scope.column_table_idx[col_idx] == op_idx,
                "CTE_LOOKUP filter: IS conjunct references a "
                "column not on this CTE.");
    Uint32 cte_col_idx = (Uint32)scope.column_attrId_map[col_idx];
    Uint32 position = cte_col_idx;

    int rc;
    if (atom->is.null) {
      // IS NULL — keep when col IS NULL, reject when col IS NOT NULL
      rc = code.branch_linked_isnotnull(position, REJECT);
    } else {
      // IS NOT NULL — keep when col IS NOT NULL, reject when col IS NULL
      rc = code.branch_linked_isnull(position, REJECT);
    }
    require_prm(rc == 0,
                "CTE_LOOKUP filter: failed to emit IS-null branch.");
    continue;
  }

  bool is_cmp = (atom->op == T_EQUALS || atom->op == T_NOT_EQUALS ||
                 atom->op == T_LT || atom->op == T_LE ||
                 atom->op == T_GT || atom->op == T_GE);
  // ... existing path ...
}
```

No virt-table column or source-column resolution needed — the
opcode just checks the AttributeHeader at `position`, no type
info required.

### Step 4 — Phase J interaction

Phase J's `promote_left_to_inner_for_where` assumes every WHERE
conjunct in `join_where_ce[t]` is null-rejecting.  With I.1's
`IS NULL` accepted, that assumption breaks: a WHERE composed
solely of `cte.col IS NULL` is null-**preserving**, and the LEFT
JOIN must NOT be promoted (the user wants to find unmatched rows).

Refine the promotion check to require **at least one
null-rejecting conjunct** in the WHERE for that table:

```cpp
static bool is_null_rejecting(const ConditionalExpression* ce) {
  if (ce == NULL) return false;
  switch (ce->op) {
  case T_EQUALS: case T_NOT_EQUALS:
  case T_LT: case T_LE: case T_GT: case T_GE:
    return true;
  case T_IS:
    return !ce->is.null;  // IS NOT NULL rejects NULL; IS NULL preserves
  case T_AND:
    return is_null_rejecting(ce->args.left) ||
           is_null_rejecting(ce->args.right);
  case T_OR:
    return is_null_rejecting(ce->args.left) &&
           is_null_rejecting(ce->args.right);
  default:
    return false;
  }
}
```

(For AND, any null-rejecting child makes the whole AND
null-rejecting.  For OR, every branch must reject NULL — current
pushdown grammar doesn't accept top-level OR on a single table
anyway, so this branch is reachable only via cross-table filters
which Phase J handles separately.)

`promote_left_to_inner_for_where` then uses
`is_null_rejecting(scope.join_where_ce[t])` instead of the simple
non-NULL check.

### Step 5 — `classify_where_by_table` extension

The existing classifier (`RonSQLPreparer.cpp:1262`,
`classify_ce_table`) handles `T_IS` already at line 274:

```cpp
case T_IS:
  return classify_ce_table(ce->is.arg, col_table_idx);
```

So `IS NULL` / `IS NOT NULL` conjuncts already route to the right
table slot.  No classifier change needed.

## Test plan

New tests in `mysql-test/suite/ronsql/t/ronsql_cte_basic.test`,
appended after current Test 25 (Phase J last test).

| # | Shape | Notes |
|---|---|---|
| 26 | INNER JOIN + `WHERE cte.k IS NOT NULL` | always-true filter; same result as no WHERE |
| 27 | INNER JOIN + `WHERE cte.t IS NULL` | always-false filter; zero rows |
| 28 | LEFT JOIN + `WHERE cte.t IS NULL` | finds unmatched rows; Phase J does NOT promote |
| 29 | LEFT JOIN + `WHERE cte.t IS NOT NULL OR cte.k > 100` | mixed; Phase J does promote (OR-with-null-rejecting branch) — actually skip if OR not in current grammar |

Test 28 is the user-visible win: it reproduces the standard
"find unmatched LEFT JOIN rows" idiom and proves Phase J's
promotion check correctly defers when WHERE is null-preserving.

If the OR-grammar doesn't accept Test 29's shape today, drop it —
that's I.2 territory.

For MIN/MAX-over-all-NULL source coverage, add Test 30 in the
basic file:

```sql
WITH bounds AS (SELECT k, MIN(some_nullable) AS m
                FROM tab GROUP BY k)
SELECT bounds.k FROM real_tab JOIN bounds ON ...
WHERE bounds.m IS NULL;
```

Requires a fixture table with a nullable column and a group whose
input rows are all NULL.  Add to the existing fixtures or a small
new table — kept minimal.

## Files

- `storage/ndb/include/kernel/Interpreter.hpp` — add
  `BRANCH_LINKED_NULL = 41` constant + Doxygen.
- `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` —
  new `handleBranchLinkedNull`; wire into dispatch tables.
- `storage/ndb/include/ndbapi/NdbInterpretedCode.hpp` — declare
  `branch_linked_isnull` / `branch_linked_isnotnull` methods.
- `storage/ndb/src/ndbapi/NdbInterpretedCode.cpp` — implement.
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` —
  - Extend `emit_cte_lookup_filter` for `T_IS`.
  - Add `is_null_rejecting` static helper near
    `promote_left_to_inner_for_where` and use it.
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — new tests.
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp` —
  optional NDB-API direct test for the new opcode (mirror Test
  21 but with IS NULL filter).
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
  — add `cte_filter_phase_i1.md` to the index.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_i.md`
  — mark I.1 as "shipped" / "in progress".

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd \
    ronsql_cli rdrs2 testCteNdbApiFilter
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql                # full suite — no regressions
./mtr --suite=ndb_push_agg          # block tests — no regressions
```

Spot-check that the new opcode 41 is reachable only via the new
NDB-API methods — older programs never set the opcode value, so
backward compatibility on existing tests is by construction.

## What we're not doing

- **`IS NULL` on a non-CTE column** — the existing
  `branch_col_eq_null` / `branch_col_ne_null` opcodes already cover
  this for direct columns.  RonSQL's main aggregator and
  WHERE-on-real-table paths can wire to those if needed; orthogonal
  to this phase.
- **`IS NULL` on a column-vs-column expression
  (`a.col IS NULL OR b.col IS NULL`)** — OR conjuncts are I.2.
- **`COALESCE(col, default)` and other null-tolerant rewrites** —
  separate parser/preparer work.

## Risks

1. **Sense bit encoding.**  The `BRANCH_LINKED_NULL` opcode word
   layout must not collide with the brancher's `branch_offset`
   field.  Current branch instructions use bits 8-31 for offset;
   bit 16 is currently used by other branches as part of the
   condition encoding.  Verify the bit layout against
   `Interpreter::brancher` before settling on bit 16 — bit 8 or
   another free bit may be a cleaner choice.
2. **Phase J promotion regression.**  After the
   `is_null_rejecting` refactor, every existing test that had
   Phase J promote LEFT-to-INNER must still see promotion.
   Audit: every existing Phase J test (Tests 23, 24, 25 in
   `ronsql_cte_basic.test`) uses comparison operators only, all
   of which `is_null_rejecting` returns true for — no behavior
   change expected.
3. **Aggregator-output IS NULL semantics.**  For SUM / COUNT
   over a non-empty group: result is never NULL, so the new
   opcode always sees AH.isNULL() == false on the matched-parent
   path.  Test 27 is the canary for this.  For MIN/MAX over an
   all-NULL group: result IS NULL — Test 30 is the canary.
4. **Backward compatibility — none.**  Per the user's standing
   rule for this branch, all CTE/pushdown-join code lands together;
   no signal-version gating is needed for the new opcode.
