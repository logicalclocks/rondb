# Phase I.18 — typed leaf-column register loads for embedded col-vs-col

## Status

Planned.

## Problem

Phase I.5 v5 fixes the linked side of embedded col-vs-col CASE /
GREATEST / LEAST conditions by replacing:

```text
READ_LINKED_TO_MEM(position)
READ_*_MEM_TO_REG(reg, offset=4)
```

with:

```text
READ_LINKED_COLUMN_TO_REG(reg, position, ndb_type)
```

That new linked-load opcode decodes by NDB type and sign-extends
signed sub-Bigint values correctly.

The leaf side still uses the normal interpreter's existing
`READ_ATTR_INTO_REG`:

```text
READ_ATTR_INTO_REG(reg, attrId)
```

For signed sub-Bigint columns, that path reads the raw attribute word
and zero-extends it when expanding into the normal interpreter's
64-bit register storage.  This is a pre-existing normal-interpreter
limitation, but I.5 v5 exposes it because the new tests compare
negative signed-sub-Bigint values in embedded col-vs-col atoms.

Example:

```sql
SUM(CASE WHEN c.c_tinyint > o.o_tinyint THEN o.o_int ELSE 0 END)
```

With `c.c_tinyint = 5` on the linked side and `o.o_tinyint = -20` on
the leaf side:

```text
linked side: READ_LINKED_COLUMN_TO_REG -> 5
leaf side:   READ_ATTR_INTO_REG        -> 236
comparison:  5 > 236                   -> false
```

Expected MySQL semantics compare `5 > -20`, which is true.

## Relationship to Phase I.5 v5

I.5 v5 should remain scoped to linked-column register loads.

Until I.18 lands, I.5 v5 SQL tests should keep negative signed
sub-Bigint values on the linked side only, and use `BIGINT` or
non-negative values on the leaf side for embedded col-vs-col
conditions.

## Desired Behaviour

Embedded normal-interpreter programs should be able to load leaf table
integer columns into registers with correct signedness and width:

| NDB type | Register value |
|----------|----------------|
| `Tinyint` | sign-extended `Int64` |
| `Tinyunsigned` | zero-extended `Uint64` / positive `Int64` |
| `Smallint` | sign-extended `Int64` |
| `Smallunsigned` | zero-extended `Uint64` / positive `Int64` |
| `Mediumint` | sign-extended `Int64` |
| `Mediumunsigned` | zero-extended `Uint64` / positive `Int64` |
| `Int` | sign-extended `Int64` |
| `Unsigned` | zero-extended `Uint64` / positive `Int64` |
| `Bigint` | `Int64` |

`Bigunsigned` above `INT64_MAX` remains a separate signedness-aware
comparison problem unless I.18 also introduces typed register compare
metadata.  The conservative initial scope should reject `Bigunsigned`
for embedded `BRANCH_*_REG_REG` comparisons, or only accept values
that fit in signed `Int64` if such a static guarantee exists.

## Design Options

### Option A — typed leaf-load opcode

Add one normal-interpreter opcode:

```text
READ_ATTR_TYPED_TO_REG(reg, attrId, ndb_type)
```

The handler should:

1. Read the leaf attribute value, preserving NULL semantics.
2. Decode according to the supplied NDB type.
3. Store a sign- or zero-extended 64-bit value in the existing normal
   interpreter register payload.

This mirrors `READ_LINKED_COLUMN_TO_REG` and keeps the existing
`BRANCH_*_REG_REG` instructions unchanged.  It is sufficient for all
integer types whose value domain fits in signed `Int64`.

This is the preferred first implementation because it keeps scope
small and directly fixes signed sub-Bigint leaf values.

### Option B — typed normal-interpreter registers

Extend the normal interpreter register representation to track
signedness/type, then make `BRANCH_*_REG_REG` compare using that
metadata.

This is more complete, and is required for full `Bigunsigned`
semantics, but it is much more invasive:

- register layout changes;
- all register producers must set type/signedness;
- arithmetic and branch handlers need metadata rules;
- validators and tests must cover mixed signed/unsigned comparisons.

Defer this unless `Bigunsigned > INT64_MAX` support is required in
embedded col-vs-col comparisons.

## Phase I.18a — conservative leaf typed-load opcode

Implement Option A for the integer family, excluding unsafe
`Bigunsigned` comparisons above `INT64_MAX`.

Files likely involved:

- `storage/ndb/include/kernel/Interpreter.hpp`
  - opcode constant;
  - encoder helper.
- `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp`
  - handler;
  - main interpreter dispatch;
  - aggregation embedded-interpreter dispatch table;
  - CTE filter dispatch if the opcode is valid there.
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp`
  - embedded-program whitelist / validation.
- `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp`
  - validation if separate.
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
  - use typed leaf load in `generate_embedded_condition` for
    col-vs-col atoms instead of `READ_ATTR_INTO_REG` when the column
    is a signed sub-Bigint type.

The handler can use the same decoding logic as aggregation
`kOpLoadCol` and `READ_LINKED_COLUMN_TO_REG`.

## Phase I.18b — RonSQL integration

Update embedded col-vs-col codegen:

```text
LeafTable side:
  before: READ_ATTR_INTO_REG(reg, attrId)
  after:  READ_ATTR_TYPED_TO_REG(reg, attrId, ndb_type)

Linked side:
  keep:   READ_LINKED_COLUMN_TO_REG(reg, position, ndb_type)
```

Word-count accounting remains one word per side if the new opcode is
one word.  If the opcode needs a parameter word, update
`generate_embedded_condition` atom sizing accordingly.

Restrict accepted types until comparison semantics are correct:

- allow signed and unsigned 8/16/24/32-bit;
- allow `Bigint`;
- reject `Bigunsigned` in this embedded register-compare path unless
  full typed comparison support is implemented.

## Phase I.18c — tests

Add focused MTR coverage after I.18a/b:

1. Negative signed `TINYINT` leaf compared to positive linked value.
2. Negative signed `SMALLINT` leaf.
3. Negative signed `MEDIUMINT` leaf.
4. Negative signed `INT` leaf.
5. Mixed signed leaf and linked operands where both are negative.
6. Unsigned 8/16/24/32-bit leaf operands with values above the signed
   range of the same width.
7. Re-run / expand Phase I.5 v5 tests so negative signed values appear
   on both linked and leaf sides.

Example regression:

```sql
SELECT c.c_id,
       SUM(CASE WHEN c.c_tinyint > o.o_tinyint THEN o.o_int ELSE 0 END) AS s
FROM v5_customer AS c
JOIN v5_orders AS o ON o.o_custkey = c.c_id
GROUP BY c.c_id;
```

with `c.c_tinyint = 5` and `o.o_tinyint = -20` must include the row.

## Completion Criteria

- Embedded col-vs-col CASE conditions compare signed sub-Bigint leaf
  values correctly.
- I.5 v5 tests no longer need to avoid negative leaf values.
- Existing linked-side typed-load tests still pass.
- `Bigunsigned` behavior is either correct through typed comparison
  support or rejected cleanly in RonSQL.
