# Phase I.20 - CTE lookup key coverage and rewrite validation

## Status

**Planned.**  This phase follows review of the shipped Phase I.16
partial-key CTE join work.

I.16a added a clean guard for joined `CTE_LOOKUP` operations whose
join supplies fewer key predicates than the virtual CTE primary key.
I.16b / I.16c then rewrote supported partial-key INNER joins so the
multi-key CTE becomes a `CTE_SCAN` root and the original parent table
becomes a child.

The review found that I.16 currently treats **key count** as **key
coverage**.  That is not strong enough.  The virtual CTE primary key is
defined by the CTE body's `GROUP BY` output order, so RonSQL must also
validate which CTE output columns are bound and in what order.

## Problems

### 1. Full-key `CTE_LOOKUP` validates only count, not identity/order

The I.16a guard in `emit_child_ops()` currently compares:

```cpp
op.num_key_cols == cte_pk_cols
```

That avoids the opaque NDB API failure when too few keys are supplied,
but it still allows semantically wrong lookups.  Example:

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT ...
FROM cte_customer AS c
JOIN pairs ON pairs.amt = c.x AND pairs.k = c.c_id;
```

The CTE virtual primary key order is `(k, amt)`, but the join
conditions produce child key order `(amt, k)`.  `lookupCte()` receives
keys in `JoinCondition` order, so the query can silently bind the
wrong values.

Wrong-column full-count cases have the same issue:

```sql
JOIN pairs ON pairs.k = c.c_id AND pairs.cnt = c.region
```

This has two predicates for a two-column PK, but `cnt` is not part of
the virtual key.

### 2. I.16 rewrite detects partial key by count only

The I.16b/c rewrite in `load_join()` currently triggers only when:

```cpp
join_cols < cte_pk_cols
```

That misses malformed full-count shapes, such as reversed `(amt, k)` or
`(k, cnt)`.  These should not proceed as normal `CTE_LOOKUP`.  They
should either be reordered to the virtual PK order or rejected clearly.

### 3. N-table rewrite assumes the matched CTE joins to the original root

I.16c walks the join list and promotes the first matching multi-key CTE
to root.  It always demotes the original SQL root as the child of the
promoted CTE and attaches the matched join's flipped ON conditions to
that new child.

That is only valid when the matched CTE join's parent alias is the
original root alias.  This is valid:

```sql
FROM c
JOIN r ON r.r_id = c.c_region
JOIN pairs ON pairs.k = c.c_id
```

because `pairs` joins to `c`, the original root.

This is not valid:

```sql
FROM c
JOIN r ON r.r_id = c.c_region
JOIN pairs ON pairs.k = r.r_id
```

The matched CTE joins to `r`, not `c`.  Promoting `pairs` and demoting
`c` with ON conditions that reference `r` creates an invalid or
misleading AST for the planner.

## Required fixes

### 1. Add CTE virtual-PK coverage helper

Add a RonSQL helper that derives the ordered virtual CTE key columns
from the CTE body's `GROUP BY` list and compares them against a
`JoinOp` / `JoinClause` key list.

The helper should answer at least:

```cpp
enum class CteKeyCoverage {
  ExactOrdered,     // keys match the virtual PK in order
  ExactReorderable, // keys match the virtual PK but in another order
  Partial,          // some PK columns are missing
  WrongColumns      // full or partial count, but at least one key is not a PK
};
```

The implementation must compare CTE output column names, not just
counts.  The virtual PK columns are the CTE outputs corresponding to
the CTE body's `GROUP BY` columns, in the same order used by
`build_cte_virtual_tables()`.

### 2. Tighten `CTE_LOOKUP` emission

Before `lookupCte()` is emitted:

- `ExactOrdered` is accepted as-is.
- `ExactReorderable` should either:
  - reorder the `keys[]` array to virtual PK order before calling
    `lookupCte()`, or
  - reject clearly if reordering is too invasive for this phase.
- `Partial` should keep the I.16a clear error unless it was already
  rewritten to a `CTE_SCAN` root.
- `WrongColumns` must reject clearly with a message explaining that the
  join must bind the virtual CTE primary key columns derived from
  `GROUP BY`.

Recommendation: implement key reordering if it can be done locally in
the `CTE_LOOKUP` emission path.  It is the most SQL-compatible behaviour
and avoids making ON predicate order user-visible.  If not, reject
`ExactReorderable` in I.20 and track reordering as a follow-up.

### 3. Use coverage helper in the I.16 rewrite decision

In `load_join()`, replace:

```cpp
join_cols < cte_pk_cols
```

with coverage-based logic:

- `Partial` on an INNER join to a multi-key CTE is eligible for the
  I.16 root rewrite.
- `ExactOrdered` and accepted `ExactReorderable` should remain a
  `CTE_LOOKUP`.
- `WrongColumns` should not rewrite; reject clearly after planning, or
  reject immediately during the rewrite scan.

This keeps the rewrite focused on the real partial-key shape and avoids
masking invalid full-count joins.

### 4. Validate matched CTE parent alias before N-table rewrite

Before applying the I.16c rewrite, verify that every ON condition in
the matched CTE join uses the original root alias as its parent alias.

If the matched CTE joins to another table in the existing chain:

- do not apply the current simple rewrite;
- let the query hit a clear permanent error, or add a dedicated message
  explaining that partial-key CTE rewrite currently requires the CTE
  join to reference the original root table.

Do not attempt a general join-tree reordering in I.20.  Moving an
intermediate parent path under the CTE root is a larger planner problem
and should be its own phase if needed.

## Test plan

Extend `mysql-test/suite/ronsql/t/ronsql_cte_partial_key.test`.
The fixture should add one parent-side column that can match
`pairs.amt`, for example `cte_customer.c_amt BIGINT`, so full-key
lookup tests can bind both `(k, amt)` without inventing unrelated
tables.

### Positive tests

1. **Full-key CTE lookup with predicates in virtual PK order**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.amt = c.c_amt
GROUP BY c.c_id;
```

Expected: matches MySQL and remains a `CTE_LOOKUP` child.  This is the
baseline for exact ordered key coverage.

2. **Full-key CTE lookup with predicates in reversed order**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.amt = c.c_amt AND pairs.k = c.c_id
GROUP BY c.c_id;
```

Expected:

- If I.20 implements key reordering: result matches MySQL.
- If I.20 rejects reorderable keys: assert the clear rejection message.

3. **Full-key CTE lookup with extra residual predicate**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.amt = c.c_amt
WHERE pairs.cnt > 0
GROUP BY c.c_id;
```

Expected: matches MySQL.  The key coverage helper must validate the ON
keys without confusing a WHERE predicate on a CTE output with a lookup
key.

4. **Existing partial-key two-table rewrite still positive**

Keep the current I.16 Test 1 shape:

```sql
JOIN pairs ON pairs.k = c.c_id
```

Expected: still rewrites to `CTE_SCAN` root and matches MySQL.

5. **Existing N-table rewrite with CTE not in `joins[0]` still positive**

Keep the current I.16 Test 5 shape where `pairs` is later in the join
list but still joins to the original root alias `c`.

Expected: still rewrites and matches MySQL.

6. **Existing LEFT JOIN elsewhere in chain still positive**

Keep the current I.16 Test 4 shape: the multikey-CTE join is INNER,
while an unrelated real-table join is LEFT OUTER.

Expected: still rewrites and preserves the unrelated outer-join
semantics.

### Negative tests

7. **Full-count but wrong CTE column**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.cnt = c.c_region
GROUP BY c.c_id;
```

Expected: clear RonSQL permanent error, not a silent `lookupCte()`
wrong-key execution.

8. **Full-count but duplicate key column**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN pairs ON pairs.k = c.c_id AND pairs.k = c.c_region
GROUP BY c.c_id;
```

Expected: clear RonSQL permanent error because `amt` is missing even
though the predicate count equals the virtual PK column count.

9. **Partial-key CTE join whose parent alias is not the original root**

```sql
WITH pairs AS (
  SELECT o_custkey AS k, o_amt AS amt, COUNT(*) AS cnt
  FROM cte_orders GROUP BY o_custkey, o_amt)
SELECT c.c_id, r.r_name, SUM(pairs.cnt) AS s
FROM cte_customer AS c
JOIN cte_region AS r ON r.r_id = c.c_region
JOIN pairs ON pairs.k = r.r_id
GROUP BY c.c_id, r.r_name;
```

Expected: clear RonSQL permanent error explaining that I.16's
partial-key CTE root rewrite only supports CTE joins whose parent is
the original root alias.

10. **LEFT JOIN on the multikey CTE itself remains rejected**

Keep the existing I.16 Test 2 shape:

```sql
LEFT JOIN pairs ON pairs.k = c.c_id
```

Expected: still rejects clearly; the root rewrite must not be applied
to the CTE join itself when it is LEFT OUTER.

11. **No-key join to multikey CTE remains rejected**

If the parser/planner allows an unconditional join form such as
`JOIN pairs ON 1 = 1`, add it as a negative test.

Expected: clear permanent error.  This is `Partial` coverage with zero
bound virtual-PK columns.

## Non-goals

- No NDB API changes.
- No true non-root `CTE_SCAN` child support.
- No general bushy join reordering.
- No change to virtual CTE key derivation.

## Completion criteria

- `CTE_LOOKUP` is never emitted unless the join keys match the virtual
  CTE primary key columns, not merely the key count.
- Reversed full-key ON predicate order is either supported by local key
  reordering or rejected clearly.
- The I.16c rewrite runs only when the matched partial-key CTE join is
  attached to the original root alias.
- New and existing `ronsql_cte_partial_key` MTR coverage passes.
