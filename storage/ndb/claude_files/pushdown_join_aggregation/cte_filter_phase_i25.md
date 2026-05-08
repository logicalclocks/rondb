# Phase I.25 - Deferred string CTE query failures from F.4 coverage

## Status

Planned follow-up after Phase I.6 F.4 string MIN/MAX delivery.

During `ronsql_minmax_string` coverage expansion we temporarily
narrowed two queries so the F.4 tests could focus on the string CTE
delivery path.  The original failures are still real follow-up items
and should be fixed in a dedicated Phase I step rather than hidden in
the F.4 test.

## Problem 1 - CTE string output joined to non-unique indexed table

The failing shape was:

```sql
WITH s AS (
  SELECT grp, MIN(v) AS min_v, MAX(v) AS max_v
  FROM str_minmax
  GROUP BY grp
)
SELECT MIN(s.min_v) AS min_v, MAX(s.max_v) AS max_v
FROM s
JOIN str_minmax AS t ON t.grp = s.grp;
```

With no index on `str_minmax.grp`, RonSQL correctly rejected the
shape as having no suitable join index.  After adding an ordered index,
the query executed but produced `NULL, NULL` instead of the MySQL
result `'', 'echo'`.

The narrowed F.4 test now joins the CTE to a small `str_groups`
primary-key table instead, which proves linked string CTE outputs can
feed downstream aggregation.  It does not prove that a CTE root can
drive a non-unique ordered-index child scan and still deliver linked
CTE string aggregate outputs correctly.

### Fix direction

1. Reproduce this exact shape in a small MTR test after F.4 lands.
2. Trace the SPJ topology: CTE_SCAN/CTE root parent, ordered-index
   child, and aggregation on the child.
3. Verify that the aggregation leaf receives both:
   - the joined child rows from the non-unique ordered-index scan;
   - the linked projection entries for `s.min_v` and `s.max_v` in
     the positions emitted by `programAggregator_join()`.
4. If the indexed child is not producing rows, fix the CTE-root to
   index-scan child path.
5. If rows arrive but the linked CTE columns are NULL or shifted,
   fix the linked-projection numbering/order between
   `appendParamConstructor()`, DBSPJ `expand(... addTableMeta ...)`,
   and `JoinAggInterpreter::LoadLinkedColumn`.
6. Add a regression using the original query and keep the current
   primary-key helper-table test as the narrower linked-string
   baseline.

## Problem 2 - CTE output predicate comparison matrix

The intended filter test was:

```sql
WITH s AS (
  SELECT grp, MIN(v) AS min_v, MAX(v) AS max_v
  FROM str_minmax
  GROUP BY grp
)
SELECT grp, min_v, max_v
FROM s
WHERE min_v < 'beta';
```

The first failure came from the MTR compare helper invoking:

```sh
mysql -e '... WHERE min_v < 'beta';'
```

The shell quote ended at `'beta'`, so MySQL saw `beta` as an
identifier and reported `Unknown column 'beta'`.  Switching the test
to double quotes avoided the shell issue but RonSQL rejected the SQL
with `Illegal token`, since double-quoted string literals are not
accepted by the RonSQL parser today.

The active F.4 test now uses `WHERE min_v < max_v`, which still tests
filtering on string CTE outputs but does not cover string literals.
That form also exposed a separate current limitation: CTE_LOOKUP
filter col-vs-col comparisons are limited to signed 64-bit integer
columns, so string col-vs-col comparisons are rejected with a clear
prepare-time error.  The active F.4 test therefore uses
`WHERE min_v IS NOT NULL` as the narrow string-output filter
baseline.

The real work item is broader than string literals.  The CTE_LOOKUP
filter emitter should have an explicit type matrix for all comparison
shapes it accepts:

- `cte_col <op> cte_col`
- `constant <op> cte_col`
- `cte_col <op> constant`

The current implementation has grown one case at a time, so support is
uneven.  Signed BIGINT col-vs-col works, while string col-vs-col is
rejected.  Constant-vs-column and column-vs-constant paths also need
to be checked across the same type families rather than assumed from
one direction.

### Type coverage target

For each of the three comparison shapes, decide and test the intended
behaviour for:

- signed integer virtual columns: TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT
  widened or loaded as signed 64-bit;
- unsigned integer virtual columns: corresponding unsigned types,
  including high-bit values and mixed signed/unsigned constants;
- FLOAT/DOUBLE virtual columns, including negative constants and
  literal-left comparisons;
- DECIMAL virtual columns, respecting the current I.22 64-bit guard
  and scale-to-DOUBLE widening rules;
- CHAR/VARCHAR/Longvarchar virtual columns, using charset-aware string
  comparison with the CTE inline metadata;
- DATE/TIME/DATETIME/TIMESTAMP virtual columns if RonSQL exposes them
  through CTE outputs in the relevant phase, or a clear prepare-time
  rejection if they remain out of scope;
- NULL semantics for all accepted families: comparison with NULL
  should behave like SQL UNKNOWN and reject the row in a WHERE filter,
  while `IS NULL` / `IS NOT NULL` remains the dedicated null test path.

Do not silently fall back to BIGINT for unsupported families.  If a
type family is not implemented in this phase, reject at prepare time
with a message that names the unsupported comparison shape and type.

### Fix direction

1. Make the RonSQL MTR compare path safe for SQL text containing
   single-quoted literals.  Prefer writing the query to a temporary
   file or otherwise passing it to `mysql` without shell-breaking
   interpolation, rather than forcing tests to avoid string literals.
2. Add explicit RonSQL parser/preparer coverage for single-quoted
   string literals in CTE output predicates if it is not already
   covered.
3. Decide whether double-quoted string literals should be supported or
   deliberately rejected.  If rejected, keep that behaviour but add a
   clear parser error test.  If supported, implement it consistently
   with MySQL SQL mode expectations.
4. Restore the literal predicate regression:
   `WHERE min_v < 'beta'`, and verify both MySQL and RonSQL paths
   compare the same rows.
5. Add string col-vs-col CTE output predicate regression:
   `WHERE min_v < max_v`.
6. Add a compact comparison matrix test file covering col-vs-col,
   const-vs-col, and col-vs-const for the type families above.  Keep
   accepted cases as MySQL-vs-RonSQL comparisons and rejected cases as
   explicit error tests.
7. Check both operand directions for asymmetric literal handling:
   `cte_col < 10` and `10 < cte_col` must either both work with the
   correct inverted operator or both reject clearly.  Repeat for
   strings and floats because those historically exercised separate
   code paths.

## Validation

Run at minimum:

- `ronsql.ronsql_minmax_string`
- the new CTE_LOOKUP comparison matrix test
- the full RonSQL MTR suite, because both fixes touch shared query
  emission/compare infrastructure or common CTE join topology

The F.4 tests should keep both forms:

- the narrowed primary-key join, to isolate linked string CTE
  aggregation;
- the restored non-unique indexed join, to cover the broader topology.
