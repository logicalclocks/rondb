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

   - In the per-atom loop, call `simplify_ce(atom, -1)` once.
   - Store the simplified atom in `atoms[a]` or in a parallel
     `simplified_atoms` array.
   - Use that same simplified atom in both the word-count pre-pass and
     the emit pass.
   - Keep the existing RHS simplification only if it is still needed
     after whole-atom simplification; otherwise remove the nested call
     to avoid doing duplicate work.

   Required checks:

   - The simplified atom must still be one of `=`, `!=`, `<`, `<=`,
     `>`, `>=`.
   - The simplified LHS must be `T_IDENTIFIER`; otherwise reject with
     the existing clean "CASE condition atom" unsupported-shape error,
     not an assert.
   - AND / OR flattening should continue to work.  Whole-atom
     simplification is per flattened atom, not on the complete AND / OR
     tree, so it should not change the existing DNF handling.

2. **Fix signed INT literal bounds in `encode_constant()`**

   The current `NdbDictionary::Column::Int` bounds are off by one:

   ```cpp
   min = -2147483647LL;
   max =  2147483648LL;
   ```

   The valid SQL / NDB signed INT range is:

   ```cpp
   min = -2147483648LL;
   max =  2147483647LL;
   ```

   Consequences today:

   - `-2147483648` can be rejected even though it is valid.
   - `2147483648` can be accepted, then encoded into four bytes as
     `0x80000000`, which is the signed INT minimum bit pattern.

   This is pre-existing, but v3b's CASE-local negative literal folding
   makes the boundary reachable from embedded CASE tests, so it belongs
   in this follow-up.

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

### Signed INT boundary coverage

7. **Leaf signed INT minimum accepted**

   Ensure the fixture contains at least one `o.o_int =
   -2147483648`.  Then run:

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN o.o_int = -2147483648 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This proves `encode_constant()` accepts
   the true signed INT minimum.

8. **Leaf signed INT positive overflow rejected**

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN o.o_int = 2147483648 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: clean RonSQL permanent error before execution.  The value
   must not be encoded as the signed INT minimum bit pattern.

9. **Linked signed INT minimum accepted**

   If the fixture can add `c.c_int = -2147483648`, add the linked-side
   mirror:

   ```sql
   SELECT c.c_id,
          SUM(CASE WHEN c.c_int = -2147483648 THEN 1 ELSE 0 END) AS s
   FROM v5_customer AS c
   JOIN v5_orders AS o ON o.o_custkey = c.c_id
   GROUP BY c.c_id;
   ```

   Expected: matches MySQL.  This is not a separate code path for
   constant encoding, but it confirms linked-column register loading
   still composes with the boundary literal.

## Non-goals

- No new kernel opcode.
- No change to `LOAD_DOUBLE_CONST`.
- No new float comparison semantics.  This phase only ensures CASE
  atoms reach the v3b register-compare path in the same normalised
  shape as WHERE atoms.
- No change to NULL branch semantics.

## Implementation checklist

1. Update `RonSQLPreparer::generate_embedded_condition()` to simplify
   each flattened atom before side resolution and reuse the simplified
   atom for emission.
2. Replace assertion-only LHS assumptions with a clear permanent RonSQL
   rejection if the simplified atom is not column-vs-column or
   column-vs-constant.
3. Correct `NdbDictionary::Column::Int` literal bounds in
   `encode_constant()`.
4. Update `ronsql_cte_greatest_least_v5.test` and recorded result with
   the test plan above.
5. Run the focused MTR:

   ```bash
   ./mtr --suite=ronsql ronsql_cte_greatest_least_v5
   ```

6. Run the broader RonSQL / pushdown aggregation regression set used
   for the surrounding Phase I.5 work.

## Expected outcome

- CASE atoms behave like WHERE atoms for literal-on-left comparisons.
- Negative numeric literals in CASE work for both float and integer
  column-vs-literal paths.
- Signed INT boundary constants are encoded correctly.
- v3b MTR coverage accurately reflects FLOAT vs DOUBLE and leaf vs
  linked paths.
