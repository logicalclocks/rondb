# Phase I.22 - DECIMAL MIN/MAX 64-bit range guard

## Status

**Planned.**  This phase follows review of the shipped Phase I.6 F.1
DECIMAL widening work.

F.1 made RonSQL's CTE virtual-table type derivation match the current
kernel aggregation representation for `MIN` / `MAX` over DECIMAL:

- `DECIMAL(..., 0)` -> `BIGINT`
- `DECIMAL UNSIGNED (..., 0)` -> `BIGUNSIGNED`
- `DECIMAL(..., scale > 0)` -> `DOUBLE`

That is intentionally a representation-level fix.  It lets the
existing 8-byte CTE aggregate-result wire format and the inline-type
CTE filter opcode handle DECIMAL `MIN` / `MAX` outputs.

The review found one remaining correctness issue in the scale-zero
case: the kernel conversion path only supports integer DECIMAL values
that fit in 64 bits.  RonSQL now advertises all scale-zero DECIMAL
`MIN` / `MAX` outputs as supported, including declarations whose value
range is wider than the kernel can convert.

## Problem

The kernel loads DECIMAL values through the aggregation interpreter and
then converts them into the aligned register type:

- signed `DECIMAL(..., 0)` uses `decimal2longlong()`;
- unsigned `DECIMAL UNSIGNED (..., 0)` uses `decimal2ulonglong()`.

Those conversions can fail for valid MySQL DECIMAL values outside the
64-bit integer range.  Example source declarations:

```sql
DECIMAL(19, 0)
DECIMAL(20, 0) UNSIGNED
```

MySQL can compute `MIN` / `MAX` over those columns.  RonSQL F.1 would
derive a `BIGINT` / `BIGUNSIGNED` CTE virtual output and proceed to
execution, where DBTUP can return a decimal conversion overflow if the
stored value does not fit in the 64-bit target.

This phase does not try to preserve arbitrary precision DECIMAL
semantics.  That would require a wider aggregate-result representation
and is outside the current F.1 design.  I.22 should instead make the
limitation explicit and reject unsafe declarations at prepare time.

## Required fix

Add a RonSQL prepare-time guard in the DECIMAL scale-zero `MIN` / `MAX`
type derivation path.

The guard should reject a CTE aggregate output when all of the
following are true:

1. the aggregate function is `MIN` or `MAX`;
2. the source column type is `Decimal` or `Decimalunsigned`;
3. the source scale is zero;
4. the declared precision can represent values outside the kernel's
   target 64-bit integer range.

Recommended conservative thresholds:

- signed `DECIMAL(p, 0)` is accepted only for `p <= 18`;
- unsigned `DECIMAL UNSIGNED(p, 0)` is accepted only for `p <= 19`.

Rationale:

- signed `BIGINT` max is `9223372036854775807`, so a declared
  `DECIMAL(19,0)` can contain valid MySQL values that do not fit;
- unsigned `BIGINT` max is `18446744073709551615`, so
  `DECIMAL UNSIGNED(19,0)` is safe, but `DECIMAL UNSIGNED(20,0)` can
  contain values that do not fit.

The guard should be based on source column metadata, not on sampled
data.  A data-dependent runtime success is not enough because RonSQL
would still be accepting a query shape it cannot execute for the full
valid range of the table definition.

## Implementation notes

The current F.1 plumbing threads `out_scale` through
`resolve_chained_column_type()`.  I.22 also needs source precision.
There are two reasonable implementation options:

1. **Add `out_precision` beside `out_scale`.**

   Extend `resolve_chained_column_type()` to return both precision and
   scale.  Real-table columns set both from the dictionary column.
   Chained COLUMN outputs pass both through.  Aggregate outputs that
   have already widened to `BIGINT` / `BIGUNSIGNED` / `DOUBLE` can
   reset both to zero because the original DECIMAL source has already
   been validated at the layer where the aggregate was derived.

   This is the most consistent option if future phases will need more
   DECIMAL metadata.

2. **Validate directly in `build_cte_virtual_tables()` before
   widening.**

   The direct aggregate branch already resolves the source load column.
   If that source resolves to a real DECIMAL column, the code can read
   precision there and reject before setting the derived type.

   This is smaller, but chained CTE type derivation also has a
   `T_MIN` / `T_MAX` DECIMAL branch in `resolve_chained_column_type()`.
   If that path can still see raw DECIMAL sources, it needs the same
   guard or it can reintroduce the runtime overflow through a chained
   CTE.

Recommendation: add `out_precision` alongside `out_scale` and centralise
the guard in a small helper:

```cpp
static bool decimal_minmax_fits_64bit(
    NdbDictionary::Column::Type type,
    Int32 precision,
    Int32 scale);
```

Use the helper in both:

- `resolve_chained_column_type()` `T_MIN` / `T_MAX` DECIMAL branch;
- `build_cte_virtual_tables()` `T_MIN` / `T_MAX` DECIMAL branch.

The error should be a clear permanent RonSQL rejection, for example:

```text
MIN/MAX over DECIMAL(p,0) wider than 64-bit integer range is not yet supported.
```

Mention that preserving full DECIMAL precision is deferred to a wider
aggregate-result representation phase.

## Test plan

Extend `mysql-test/suite/ronsql/t/ronsql_cte_decimal.test` and record
the matching result file.

### Positive boundary tests

1. **Signed safe boundary**

```sql
CREATE TABLE dec_signed_ok (
  id INT NOT NULL,
  grp INT NOT NULL,
  v DECIMAL(18,0) NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;

WITH s AS (
  SELECT grp AS k, MIN(v) AS lo, MAX(v) AS hi
  FROM dec_signed_ok GROUP BY grp)
SELECT k, lo, hi FROM s;
```

Expected: matches MySQL and continues to expose the CTE output as
`BIGINT`.

2. **Unsigned safe boundary**

```sql
CREATE TABLE dec_unsigned_ok (
  id INT NOT NULL,
  grp INT NOT NULL,
  v DECIMAL(19,0) UNSIGNED NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;

WITH s AS (
  SELECT grp AS k, MIN(v) AS lo, MAX(v) AS hi
  FROM dec_unsigned_ok GROUP BY grp)
SELECT k, lo, hi FROM s;
```

Expected: matches MySQL and continues to expose the CTE output as
`BIGUNSIGNED`.

### Rejection tests

3. **Signed unsafe precision rejected at prepare time**

```sql
CREATE TABLE dec_signed_bad (
  id INT NOT NULL,
  grp INT NOT NULL,
  v DECIMAL(19,0) NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;

WITH s AS (
  SELECT grp AS k, MAX(v) AS hi
  FROM dec_signed_bad GROUP BY grp)
SELECT k, hi FROM s;
```

Expected: clear RonSQL permanent error before execution.  The query
must not reach the kernel and fail with a DBTUP decimal conversion
overflow.

4. **Unsigned unsafe precision rejected at prepare time**

```sql
CREATE TABLE dec_unsigned_bad (
  id INT NOT NULL,
  grp INT NOT NULL,
  v DECIMAL(20,0) UNSIGNED NOT NULL,
  PRIMARY KEY USING HASH (id)
) ENGINE=NDB;

WITH s AS (
  SELECT grp AS k, MAX(v) AS hi
  FROM dec_unsigned_bad GROUP BY grp)
SELECT k, hi FROM s;
```

Expected: same clear RonSQL permanent error.

5. **Chained CTE unsafe precision rejected**

```sql
WITH a AS (
  SELECT grp AS k, MAX(v) AS hi
  FROM dec_signed_bad GROUP BY grp),
b AS (
  SELECT MIN(a.hi) AS m
  FROM a)
SELECT m FROM b;
```

Expected: clear RonSQL permanent error.  This ensures the guard also
covers the recursive `resolve_chained_column_type()` path and not only
direct virtual-table construction.

### Regression tests

6. Existing F.1 tests continue to pass:

- `DECIMAL(10,0)` signed `MIN` / `MAX`;
- `DECIMAL(10,0) UNSIGNED` `MIN` / `MAX`;
- `DECIMAL(10,2)` `MIN` / `MAX` widened to `DOUBLE`;
- inline `WHERE` filter on widened DECIMAL output.

## Non-goals

- Do not preserve full DECIMAL precision for `MIN` / `MAX`.
- Do not change the kernel aggregate-result wire format.
- Do not change scale-positive DECIMAL widening to `DOUBLE`.
- Do not address text formatting differences caused by scale-positive
  DECIMAL widening to `DOUBLE`; that remains a known limitation outside
  I.22.

## Verification

Run the focused MTR:

```bash
./mtr --suite=ronsql ronsql_cte_decimal
```

If the guard is implemented in shared type-resolution code, also run a
focused chained-CTE regression file if one is split out later.
