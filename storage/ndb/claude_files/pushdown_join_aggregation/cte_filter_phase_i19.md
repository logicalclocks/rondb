# Phase I.19 - CASE literal normalisation and boundary fixes

## Status

**Planned.**  This phase follows the review of the Phase I.5 v3b
commits.  v3b made float / double column-vs-literal CASE atoms work by
loading the literal with `LOAD_DOUBLE_CONST` and comparing through
typed registers.  The review found three follow-ups that should be
handled together because they all sit on the same embedded CASE
col-vs-const path.

## Scope

1. **Simplify the whole CASE atom before side resolution**

   `generate_embedded_condition()` currently resolves
   `atom->args.left` as a column before any CASE-local simplification.
   Phase I.5 v3b only simplified `atom->args.right` to fold
   `T_MINUS(T_FLOAT|T_INT)`.

   That fixes:

   ```sql
   CASE WHEN o.o_double < -50.5 THEN ...
   ```

   but still leaves the symmetric shape exposed:

   ```sql
   CASE WHEN 100.0 < o.o_double THEN ...
   ```

   The general WHERE-expression path calls `simplify_ce()` on the
   whole comparison, which swaps literal-vs-column atoms into
   column-vs-literal form and inverts the inequality.  CASE conditions
   do not go through that pass, so I.19 should do it locally before
   the embedded-condition pre-pass.

   Proposed implementation:

   - **Order:** flatten the AND / OR tree into the atoms list first,
     then simplify each leaf atom.  Simplification runs per leaf, not
     on the AND / OR tree, so it cannot disturb the DNF / OR
     flattening that has already happened.
   - In the per-atom loop, call
     `simplify_ce(atoms[a], /*maxdepth=*/-1)` once.  `-1` is the
     "unlimited depth" convention used by every other call site in
     this file (see `RonSQLPreparer.cpp:7950`); `simplify_ce` returns
     a possibly-new `ConditionalExpression*` allocated on
     `m_amalloc`, it does not mutate the input.
   - Overwrite `atoms[a]` with the returned pointer.  No parallel
     `simplified_atoms` array — keeping two views of the same data is
     a footgun for the second pass.  The original AST is still
     reachable from the caller's root if anything else needs it.
   - Use the rewritten `atoms[a]` in both the word-count pre-pass and
     the emit pass.  Both passes MUST see byte-identical input,
     otherwise emitted code length and reserved buffer length can
     diverge.  This is why simplification has to run before the
     pre-pass, not between them.
   - Drop the existing nested RHS-only simplify call.  Whole-atom
     simplification subsumes it, so leaving the inner call in place
     just doubles the work and risks the two simplifies disagreeing
     on output if `simplify_ce` is ever extended.

   Required checks after simplification, in this order:

   1. **Constant-fold outcome.**  `simplify_ce` can collapse a
      comparison whose both sides are constants into a single
      `T_TRUE` / `T_FALSE` literal (see the constant-folding arms
      around `RonSQLPreparer.cpp:8127`).  Handle this explicitly:
      - `T_TRUE` atom inside an AND group: drop the atom from the
        list (it is the AND identity).  If the AND list becomes
        empty, the whole CASE condition is unconditionally true —
        emit the THEN branch with no test.
      - `T_FALSE` atom inside an AND group: the whole AND group is
        false — drop the entire group from the OR list.
      - `T_TRUE` atom standing alone (or as a single OR group):
        unconditionally true CASE branch.
      - `T_FALSE` atom standing alone: unconditionally false CASE
        branch — emit the ELSE branch directly, skip the WHEN.
      All four cases reduce to a one-time fold during the pre-pass.
      No new opcode, no runtime cost.
   2. The simplified atom (after the constant-fold step above) must
      be one of `=`, `!=`, `<`, `<=`, `>`, `>=`.
   3. The simplified LHS must be `T_IDENTIFIER`; otherwise reject
      with the existing clean "CASE condition atom" unsupported-shape
      error, not an assert.

2. **Fix signed integer literal bounds in `encode_constant()` for
   every signed width**

   The same off-by-one bug almost certainly exists on every signed
   integer width, because the bound table was written by the same
   hand.  Concretely, audit and fix all five widths:

   | Type        | Wrong bounds (suspected)              | Correct bounds                          |
   |-------------|----------------------------------------|------------------------------------------|
   | `Tinyint`   | `-127`         …  `+128`               | `-128`         …  `+127`                 |
   | `Smallint`  | `-32767`       …  `+32768`             | `-32768`       …  `+32767`               |
   | `Mediumint` | `-8388607`     …  `+8388608`           | `-8388608`     …  `+8388607`             |
   | `Int`       | `-2147483647`  …  `+2147483648`        | `-2147483648`  …  `+2147483647`          |
   | `Bigint`    | `-9223372036854775807` … `+9223372036854775808` | `-9223372036854775808` … `+9223372036854775807` |

   The first four columns are known wrong from the v3b review; the
   `Bigint` row needs to be confirmed by reading `encode_constant()`
   directly.  If `Bigint` is correct, drop it from the table and note
   that explicitly.

   Consequences today, per width:

   - The true minimum (`INT_MIN` etc.) can be rejected even though it
     is a valid SQL / NDB literal.
   - One-past-max (`INT_MAX + 1`) can be accepted, then encoded into N
     bytes as the signed minimum bit pattern.  This is the more
     dangerous direction — the parser silently lets a too-large
     literal through and the kernel sees the wrong value.

   Two implementation notes:

   - `Bigint`'s correct max (`+9223372036854775807`) is `INT64_MAX`,
     and the wrong max (`+9223372036854775808`) is one above
     `INT64_MAX` and therefore not even representable as `int64_t`.
     The bound check has to use the correct types — `int64_t` for the
     min and `uint64_t` for the max comparison (or just use
     `INT64_MIN` / `INT64_MAX` from `<cstdint>`).
   - For unsigned widths (`Tinyunsigned` … `Bigunsigned`) the same
     pass should also confirm `min == 0` and `max == 2^N - 1`, but
     these are less likely to have an off-by-one because the lower
     bound is 0.

   v3b's CASE-local negative literal folding makes these boundaries
   reachable from embedded CASE tests, which is what makes I.19 the
   right place to fix them.

3. **Fix and extend v3b MTR coverage**

   Test 20 in `ronsql_cte_greatest_least_v5.test` is labelled as
   "leaf FLOAT" but uses `o.o_double`.  Rename the label/comment or
   switch the query to `o.o_float`; preferably add both forms so the
   test matrix explicitly covers leaf FLOAT and leaf DOUBLE against
   negative fractional literals.

## Test plan

Extend `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v5.test`
and re-record the matching result file.  The tests should keep using
the existing strict MySQL-vs-RonSQL diff harness except for deliberate
RonSQL rejection tests.

### Normalisation tests

1. **Literal on left, leaf DOUBLE on right**

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN 100.0 < o.o_double THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This proves whole-atom simplification
   swaps the atom to `o.o_double > 100.0` before side resolution.

2. **Literal on left, linked FLOAT on right**

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN 0 <= c.c_float THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This covers the linked-side v3b path and
   the inequality inversion performed by `simplify_ce()`.

3. **Nested AND / OR with one literal-left atom**

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN (100.0 < o.o_double OR c.c_float < 0)
                   THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This guards that per-atom simplification
   still works after the embedded-condition OR flattener.

### Negative literal and FLOAT / DOUBLE coverage

4. **Leaf DOUBLE vs negative fractional literal**

   Keep or rename the existing Test 20 shape:

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN o.o_double < -50.5 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  The test label must say DOUBLE if the
   query uses `o.o_double`.

5. **Leaf FLOAT vs negative fractional literal**

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN o.o_float < -50.5 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This is the coverage the existing Test 20
   label implied but did not actually exercise.

6. **Linked FLOAT vs negative integer literal**

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN c.c_float > -1 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This confirms the whole-atom / RHS
   simplification still folds negative integer constants used by the
   v3b linked-float path.

### Signed integer boundary coverage

Cover every signed width that section 2 fixes.  Add the matching
nullable / signed-minimum fixture rows so MIN can be exercised end to
end.  Each width gets a "minimum accepted" pair (leaf and linked) and
a "one-past-max rejected" leaf test.

7. **Leaf signed minimums accepted**

   Five queries, one per signed width.  `o_tinyint = -128`,
   `o_smallint = -32768`, `o_mediumint = -8388608`,
   `o_int = -2147483648`, `o_bigint = -9223372036854775808`:

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN o.o_int = -2147483648 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Same shape for the other four widths.  Expected: matches MySQL.
   Proves `encode_constant()` accepts the true signed minimum on
   every width.

8. **Leaf signed positive overflow rejected**

   Five queries, one per signed width: `+128`, `+32768`, `+8388608`,
   `+2147483648`, `+9223372036854775808`:

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN o.o_int = 2147483648 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Same shape for the other four widths.  Expected: clean RonSQL
   permanent error before execution.  The value must not be encoded
   as the signed minimum bit pattern.  For `Bigint` the parser may
   reject the literal even before reaching `encode_constant` since
   `+9223372036854775808` is unrepresentable as `int64_t` — that is
   acceptable as long as the rejection is clean.

9. **Linked signed minimums accepted**

   Mirror Test 7 against the linked side.  Add fixture columns
   `c.c_tinyint`, `c.c_smallint`, `c.c_mediumint`, `c.c_int`,
   `c.c_bigint` populated with the corresponding minima:

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN c.c_int = -2147483648 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Same shape for the other four widths.  Expected: matches MySQL.
   This is not a separate code path for constant encoding but it
   confirms linked-column register loading composes with the
   boundary literal on every signed sub-Bigint width — i.e., that
   v5's `READ_LINKED_COLUMN_TO_REG` zero-extension does not corrupt
   the comparison when the boundary literal is involved.

10. **Literal-on-left + linked sub-Bigint signed**

    Combines section 1's literal-on-left normalisation with v5's
    linked sub-Bigint zero-extension path:

    ```sql
    SELECT c.c_id,
           SUM(CASE WHEN -1 > c.c_smallint THEN 1 ELSE 0 END) AS s
    FROM v5_customer AS c
    JOIN v5_orders AS o ON o.o_custkey = c.c_id
    GROUP BY c.c_id;
    ```

    Expected: matches MySQL.  Whole-atom simplify swaps to
    `c.c_smallint < -1`; the linked-side sub-Bigint zero-extension
    must not turn the negative `c.c_smallint` value into a large
    positive register value before the compare.

## Non-goals

- No new kernel opcode.
- No change to `LOAD_DOUBLE_CONST`.
- No new float comparison semantics.  This phase only ensures CASE
  atoms reach the v3b register-compare path in the same normalised
  shape as WHERE atoms.
- No change to NULL branch semantics.

## Implementation checklist

1. In `RonSQLPreparer::generate_embedded_condition()`, after the
   AND / OR flattening already produces the per-leaf `atoms[]` list
   and BEFORE either the word-count pre-pass or the emit pass runs,
   call `simplify_ce(atoms[a], -1)` and overwrite `atoms[a]` in
   place with the returned pointer.  Drop the existing nested
   RHS-only simplify call.
2. Add the constant-fold handling described in section 1: drop
   `T_TRUE` AND-conjuncts, drop AND groups containing a `T_FALSE`,
   short-circuit standalone `T_TRUE` / `T_FALSE` to direct THEN /
   ELSE emission.  Run this fold once during the pre-pass so the
   word count and the emit pass see identical input.
3. Replace assertion-only LHS assumptions with a clear permanent
   RonSQL rejection if the simplified atom is not column-vs-column
   or column-vs-constant.
4. Audit `encode_constant()` and correct the literal bounds for
   every signed integer width (`Tinyint`, `Smallint`, `Mediumint`,
   `Int`, and `Bigint` if confirmed wrong).  Use `INT*_MIN` /
   `INT*_MAX` constants from `<cstdint>` rather than open-coded
   literals; this also makes the `Bigint` case readable.  Spot-check
   the unsigned widths while the file is open.
5. Update `ronsql_cte_greatest_least_v5.test` and recorded result
   with the test plan above.  Add the corresponding fixture columns
   and rows for the signed-integer boundary tests on both
   `v5_customer` and `v5_orders`.
6. Rebuild the touched binaries before running MTR:

   ```bash
   cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
   ```

7. Run the focused MTR:

   ```bash
   cd debug_build/mysql-test
   ./mtr --suite=ronsql ronsql_cte_greatest_least_v5
   ```

8. Run the regression suites the rest of Phase I.5 / Phase I.9 used:

   ```bash
   ./mtr --suite=ronsql
   ./mtr --suite=ndb_push_agg
   ```

   Confirm zero regressions before committing.

## Expected outcome

- CASE atoms behave like WHERE atoms for literal-on-left comparisons.
- Negative numeric literals in CASE work for both float and integer
  column-vs-literal paths.
- Signed INT boundary constants are encoded correctly.
- v3b MTR coverage accurately reflects FLOAT vs DOUBLE and leaf vs
  linked paths.
