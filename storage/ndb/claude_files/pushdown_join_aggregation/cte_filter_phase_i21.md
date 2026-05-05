# Phase I.21 - Scalar CTE semantic guardrails after I.17

## Status

**Planned.**  This phase follows review of the shipped Phase I.17
scalar aggregate CTE work.

Phase I.17 added the user-visible scalar CTE shapes needed for
watermark queries:

```sql
WITH max_update AS (
  SELECT MAX(update_dt) AS latest_update FROM feature_store),
max_insert AS (
  SELECT MAX(insert_dt) AS latest_insert FROM feature_store)
SELECT GREATEST(max_update.latest_update, max_insert.latest_insert);
```

To make the scalar cross-join shape fit the existing `lookupCte`
kernel/API interface, I.17 uses a structural workaround: a scalar CTE
virtual table has no natural `GROUP BY` key, so RonSQL marks the first
scalar output column as a virtual primary key and then uses a dummy
lookup key for scalar CTE children.

That is a reasonable narrow bridge for the Test 20 / watermark shape,
but the review found places where the structural key can leak into
normal SQL semantics.  I.21 should make the scalar CTE workaround
explicitly local to the child cross-join implementation and prevent it
from changing result semantics, nullability, or top-level function
behaviour.

## Problems

### 1. Root scalar CTE `WHERE` can be incorrectly converted to `lookupCte`

Phase I.7 added a root `CTE_SCAN` to `lookupCte` optimisation:

```sql
WITH grouped AS (...)
SELECT ...
FROM grouped
WHERE grouped.pk = 10;
```

That optimisation is correct for grouped CTEs whose virtual primary
key is derived from the CTE body's `GROUP BY` columns.

After I.17, scalar CTEs also appear to have a virtual primary key: the
first output column.  A root query such as:

```sql
WITH s AS (
  SELECT MAX(v) AS m FROM t)
SELECT m
FROM s
WHERE m = 999;
```

can therefore look like a fully-bound root lookup.  However, the
scalar CTE lookup path deliberately ignores the lookup key because
there is no real scalar CTE key.  If the root optimisation converts
the query to `lookupCte()` and returns without applying the filter, the
predicate can be bypassed and RonSQL can return the scalar row even
when MySQL would return no rows.

This is a semantic bug, not only a missing error message.

### 2. The scalar dummy key changes user-visible nullability metadata

Scalar aggregate outputs can be NULL.  For example:

```sql
WITH s AS (
  SELECT MAX(v) AS m FROM empty_table)
SELECT m FROM s;
```

The correct scalar row contains `NULL`.

I.17's first-output-as-PK workaround can make that first output column
look non-nullable in the synthetic virtual table metadata.  That is
wrong for `MAX`, `MIN`, `SUM`, and other nullable aggregate outputs
over empty input.  Even if the runtime row currently survives, the
metadata mismatch is dangerous because later planner, filter, result
buffer, or client formatting code can reasonably trust the column's
nullable flag.

The structural key should not alter user-visible output metadata.

### 3. Top-level `GREATEST` / `LEAST` lowering is too broad

I.17 accepts top-level `GREATEST` / `LEAST` by wrapping the expression
in an implicit aggregate.  That is intended for scalar CTE values,
where there is exactly one logical row:

```sql
WITH a AS (SELECT MAX(x) AS x FROM t),
     b AS (SELECT MAX(y) AS y FROM t)
SELECT GREATEST(a.x, b.y);
```

The parser/lowering path must not make this transformation visible for
ordinary row-producing tables.  A query such as:

```sql
SELECT GREATEST(v, 1) AS g
FROM t;
```

has row-wise MySQL semantics.  If RonSQL lowers it to an implicit
aggregate, the query can return one row containing
`MAX(GREATEST(v, 1))`, which is a silent wrong result.

The implementation should either constrain the lowering to proven
scalar CTE expressions or reject unsupported top-level `GREATEST` /
`LEAST` shapes clearly.

## Required fixes and alternatives

### Fix 1. Prevent root scalar CTE predicates from using the dummy-key lookup path

The safest immediate rule is:

- root `CTE_SCAN` to `lookupCte` conversion is allowed only for CTEs
  whose virtual primary key comes from `GROUP BY`;
- scalar aggregate CTE roots stay on `scanCte` so normal filters remain
  attached and evaluated.

This keeps the I.7 optimisation for grouped CTEs and removes the risk
that a scalar dummy key is treated as a real SQL key.

Valid alternatives:

1. **Disable root lookup only for scalar CTEs with filters.**

   A scalar root without `WHERE` could still use a keyless/dummy
   lookup path.  This is slightly more permissive, but it preserves two
   root execution paths for scalar CTEs and therefore leaves more room
   for future divergence.

2. **Allow root scalar lookup but always apply a residual filter.**

   This would preserve the lookup plan shape while making the semantics
   correct.  It is only preferable if the NDB API path can attach and
   execute the same filter after scalar lookup.  If adding a residual
   filter requires special scalar-only plumbing, it is likely more
   complex than the benefit justifies.

Recommendation for final plan: use the simple grouped-only root
lookup rule.  It is easy to reason about: only real virtual primary
keys may drive root lookup optimisation.

### Fix 2. Stop encoding scalar identity by making a real output column non-nullable

RonSQL needs a way to satisfy `lookupCte`'s key shape for scalar CTE
children without changing the nullability of user-visible aggregate
outputs.

Valid alternatives:

1. **Add a hidden synthetic dummy key column to scalar CTE virtual tables.**

   The virtual table would contain one internal key column plus the
   real scalar outputs.  `lookupCte` would bind the hidden key; result
   projection would continue to expose only the real outputs.  This is
   the cleanest semantic model because the dummy key is represented as
   what it really is: an internal implementation column.

   Costs / risks:

   - all virtual-column id mappings must account for the hidden column;
   - CTE result column registration must avoid exposing the hidden key;
   - existing scalar tests must prove output ordinals remain stable.

2. **Keep the first output as the structural key but decouple
   `Nullable` from `PrimaryKey`.**

   The virtual table builder could mark the first scalar output as
   primary key for API shape purposes while preserving the aggregate
   output's real nullability metadata.  This is a smaller RonSQL-side
   change if NDB API accepts a nullable synthetic primary-key column in
   this CTE virtual-table context.

   Costs / risks:

   - the metadata remains conceptually inconsistent: a primary key
     column that can be NULL;
   - other code may assume primary-key implies non-null;
   - this preserves the structural leak that caused Problem 1, so it
     must be paired with stricter planning guards.

3. **Do not use `lookupCte` for scalar cross joins; use a scalar
   `scanCte` child path instead.**

   This avoids dummy keys entirely but likely requires API/kernel work
   if child `scanCte` cannot currently express the parent relationship
   needed for the scalar cross-join shape.  It may be the cleanest
   long-term design, but it is probably larger than I.21 needs to be.

Recommendation for final plan: prefer the hidden dummy key if the
mapping changes are contained.  If that proves too invasive, use the
metadata-decoupling option as a temporary fix and document that the
hidden key remains the desired cleanup.

### Fix 3. Constrain top-level `GREATEST` / `LEAST` to scalar-CTE-only shapes

RonSQL should not silently reinterpret row-wise scalar functions as
aggregate functions.

Valid alternatives:

1. **Validate operands after name resolution and allow only scalar CTE
   outputs.**

   Top-level `GREATEST` / `LEAST` is accepted only when every column
   operand resolves to a scalar aggregate CTE output, and any constants
   are normal constants.  This supports the watermark query directly
   and rejects ordinary table columns.

   This is the most user-friendly option if the resolver already has
   enough information to identify scalar CTE outputs at the lowering
   point.

2. **Require an explicit aggregate wrapper for all non-scalar-CTE
   shapes.**

   For example:

   ```sql
   SELECT MAX(GREATEST(t.a, t.b)) FROM t;
   ```

   remains valid as an explicit aggregate expression, while:

   ```sql
   SELECT GREATEST(t.a, t.b) FROM t;
   ```

   is rejected by RonSQL unless/until normal projection expressions are
   supported.

   This is probably already aligned with RonSQL's aggregation-focused
   surface.

3. **Move the implicit wrapping out of the parser and into a
   scalar-CTE-specific preparation step.**

   The parser would keep `GREATEST` / `LEAST` as a scalar expression.
   The preparer would only wrap it when it has proven that the query
   has exactly one scalar row.  This has the cleanest layering but may
   require more AST handling than I.21 should take on.

Recommendation for final plan: implement operand validation in the
preparer and reject non-scalar-CTE top-level `GREATEST` / `LEAST`
with a clear permanent error.  If the current parser forces implicit
wrapping too early, move the validation as close as possible after
name resolution and before execution planning.

## Test plan

Add or extend MTR coverage, most likely in
`mysql-test/suite/ronsql/t/ronsql_cte_scalar.test`.

### Root scalar CTE predicates

1. **Root scalar CTE false equality filter**

```sql
WITH s AS (
  SELECT MAX(v) AS m FROM scalar_values)
SELECT m
FROM s
WHERE m = 999;
```

Expected: matches MySQL and returns no rows.

2. **Root scalar CTE true equality filter**

```sql
WITH s AS (
  SELECT MAX(v) AS m FROM scalar_values)
SELECT m
FROM s
WHERE m = 90;
```

Expected: matches MySQL and returns the scalar row.

3. **Root scalar CTE range filter**

```sql
WITH s AS (
  SELECT MAX(v) AS m FROM scalar_values)
SELECT m
FROM s
WHERE m < 90;
```

Expected: matches MySQL and returns no rows.  This guards against an
equality-only special case accidentally leaving other predicates
untested.

### Nullable scalar aggregate first output

4. **First scalar output NULL over empty input**

```sql
WITH s AS (
  SELECT MAX(v) AS m FROM scalar_values WHERE v > 1000000)
SELECT m
FROM s;
```

Expected: one row with `NULL`, matching MySQL.

5. **Nullable first output in scalar CTE cross join**

```sql
WITH a AS (
  SELECT MAX(v) AS m FROM scalar_values WHERE v > 1000000),
b AS (
  SELECT COUNT(*) AS c FROM scalar_values)
SELECT a.m, b.c
FROM a, b;
```

Expected: one row with `NULL` for `a.m` and the correct count for
`b.c`.

6. **Nullable first output through `GREATEST` / `LEAST`**

```sql
WITH a AS (
  SELECT MAX(v) AS m FROM scalar_values WHERE v > 1000000),
b AS (
  SELECT MAX(v) AS m FROM scalar_values)
SELECT GREATEST(a.m, b.m) AS watermark
FROM a, b;
```

Expected: MySQL-compatible NULL propagation for `GREATEST` / `LEAST`.
If RonSQL still rejects this shape because nullable top-level
GREATEST / LEAST is not supported, the rejection must be deliberate
and documented.

### Top-level `GREATEST` / `LEAST` guardrails

7. **Scalar CTE watermark shape remains accepted**

```sql
WITH a AS (
  SELECT MAX(x) AS x FROM scalar_values),
b AS (
  SELECT MAX(y) AS y FROM scalar_values)
SELECT GREATEST(a.x, b.y) AS watermark
FROM a, b;
```

Expected: matches MySQL.

8. **No-FROM scalar CTE synthesis remains accepted**

```sql
WITH a AS (
  SELECT MAX(x) AS x FROM scalar_values),
b AS (
  SELECT MAX(y) AS y FROM scalar_values)
SELECT GREATEST(a.x, b.y) AS watermark;
```

Expected: matches MySQL or the existing RonSQL-only no-FROM scalar
CTE semantics.

9. **Ordinary table top-level `GREATEST` rejected clearly**

```sql
SELECT GREATEST(v, 1) AS g
FROM scalar_values;
```

Expected: clear RonSQL permanent error unless/until normal projection
expressions are supported.  It must not silently aggregate.

10. **Grouped ordinary table top-level `GREATEST` rejected clearly**

```sql
SELECT grp, GREATEST(v, 1) AS g
FROM scalar_values
GROUP BY grp;
```

Expected: clear RonSQL permanent error.  This avoids silently
returning `MAX(GREATEST(v, 1))` per group.

11. **Explicit aggregate over `GREATEST` remains accepted if already
    supported**

```sql
SELECT SUM(GREATEST(v, 1)) AS s
FROM scalar_values;
```

Expected: matches MySQL if the existing aggregation expression path
supports the operand types.  This proves I.21 rejects only ambiguous
top-level row-wise scalar functions, not explicit aggregate
expressions.

## Implementation checklist

1. Add a helper that answers whether a CTE statement is a scalar
   aggregate CTE: no `GROUP BY`, at least one output, and every output
   aggregate.
2. In root CTE planning, disallow the I.7 root `lookupCte`
   optimisation when the referenced CTE is scalar.
3. Fix scalar CTE virtual-table metadata so user-visible aggregate
   output nullability is preserved.
4. Choose the scalar identity representation:
   hidden dummy key if contained, otherwise metadata decoupling with
   explicit guardrails.
5. Add validation for implicit top-level `GREATEST` / `LEAST`
   lowering.  Accept only scalar CTE output operands; reject ordinary
   row-producing table operands.
6. Add MTR tests above and re-record results.
7. Run focused MTR:

```bash
./mtr --suite=ronsql ronsql_cte_scalar
```

## Non-goals

- No general row-wise projection expression support.
- No new support for arbitrary non-scalar top-level `GREATEST` /
  `LEAST`.
- No general join-tree rewrite for scalar CTEs.
- No behavioural change to grouped CTE root lookup from Phase I.7.
