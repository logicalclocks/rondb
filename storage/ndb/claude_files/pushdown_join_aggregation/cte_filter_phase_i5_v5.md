# Phase I.5 v5 — Typed linked-column register loads

## Status

**Planned.**  This is a kernel/API support phase for the Phase I.5
register-based CASE / GREATEST / LEAST work.  It closes the signed
sub-64-bit linked-column gap discovered during I.5 v2 planning.

## Problem

Phase I.5 v2a wants embedded CASE conditions such as:

```
SUM(CASE WHEN parent_i32 > child_i32 THEN child_i32 ELSE 0 END)
SUM(GREATEST(parent_i16, child_i16))
```

Leaf columns can already be loaded into interpreter registers with
`READ_ATTR_INTO_REG`; DBTUP performs type-aware conversion and signed
integer values become the register-machine BIGINT representation.

Linked columns are different.  The current embedded-interpreter path
uses:

```
READ_LINKED_TO_MEM(position)
READ_*_MEM_TO_REG_CONST(reg, 4)
```

where byte offset 4 skips the `AttrHeader` staged by
`READ_LINKED_TO_MEM`.  Existing heap-to-register loads cover unsigned
8/16/32-bit values and signed 64-bit values:

- `READ_UINT8_MEM_TO_REG`
- `READ_UINT16_MEM_TO_REG`
- `READ_UINT32_MEM_TO_REG`
- `READ_INT64_MEM_TO_REG`

That is not enough for signed `TINYINT`, `SMALLINT`, `MEDIUMINT`, or
`INT` linked columns.  Zero-extending a negative signed value would
make register comparisons wrong.

## Goal

Add compact, type-aware linked-column register loading so linked
integer columns of all supported widths can be compared through the
register machine:

- signed and unsigned 8/16/24/32/64-bit integer columns;
- loaded values represented in registers as BIGINT/BIGUNSIGNED-style
  integer values;
- usable by embedded interpreted programs and by every interpreter
  variant that may validate or execute such programs.

## Design Direction

Prefer **one typed linked-load instruction** over spending one opcode
per signed width.

Proposed instruction:

```
READ_LINKED_COLUMN_TO_REG
```

Conceptual operands:

- linked-buffer position;
- destination register;
- column type, or compact type class;
- optional signed/unsigned flag if not encoded by the column type.

The handler performs the full operation:

1. Locate linked attr entry by position, like `READ_LINKED_TO_MEM`.
2. Read `AttrHeader`.
3. If NULL, mark the destination register NULL.
4. Decode the value according to the supplied NDB column type.
5. Store the sign- or zero-extended integer value in the destination
   register.

For v5, target the integer family first:

| NDB type | Register result |
|----------|-----------------|
| `Tinyint` | signed BIGINT |
| `Tinyunsigned` | unsigned BIGUNSIGNED |
| `Smallint` | signed BIGINT |
| `Smallunsigned` | unsigned BIGUNSIGNED |
| `Mediumint` | signed BIGINT |
| `Mediumunsigned` | unsigned BIGUNSIGNED |
| `Int` | signed BIGINT |
| `Unsigned` | unsigned BIGUNSIGNED |
| `Bigint` | signed BIGINT |
| `Bigunsigned` | unsigned BIGUNSIGNED |

Float, double, decimal, string, and temporal types are out of scope
unless Phase I.5 v3 has already introduced the matching register
semantics.

## Opcode Encoding

Keep opcode pressure low.  Use one opcode plus one parameter word if
needed.

Possible encoding:

```
word0: READ_LINKED_COLUMN_TO_REG | (dst_reg << 6) | (position << 16)
word1: ndb_column_type | flags
```

The exact bit layout should follow existing interpreter conventions
in `Interpreter.hpp`.  If the type value and signedness fit in the
remaining bits of `word0`, a one-word encoding is acceptable, but do
not contort the encoding if it makes validation brittle.

## Interpreter Availability

The new instruction must be available consistently anywhere an
embedded or aggregation interpreted program can be validated or
executed.

Update all relevant tables and dispatch paths:

- `DbtupExecQuery.cpp`
  - main jump-table dispatch for interpreted execution;
  - CTE filter handler table if applicable;
  - aggregation interpreter handler table if applicable;
  - validation / word-count logic for the new one- or two-word
    instruction.
- `JoinAggInterpreter.cpp`
  - `validateEmbeddedProgram` whitelist;
  - word-count accounting for the instruction;
  - any program scanner that skips instruction payload words.
- `AggInterpreter.cpp`
  - whitelist / validation if aggregation programs can embed or
    inspect this instruction.
- `NdbInterpretedCode`
  - public or internal emit helper if RonSQL or tests need to emit
    the instruction through this API.
- `NdbAggregator` / RonSQL direct emit path
  - helper or direct word emission used by `generate_embedded_condition`.

The plan should be implemented so a program using the new instruction
does not pass validation in one interpreter path but fail dispatch in
another.

## RonSQL Integration

After v5 exists, Phase I.5 v2a can load linked col-vs-col operands
with:

```
READ_LINKED_COLUMN_TO_REG(lhs_position, r1, lhs_type)
READ_LINKED_COLUMN_TO_REG(rhs_position, r2, rhs_type)
BRANCH_*_REG_REG(r1, r2, label)
```

Leaf columns keep using:

```
READ_ATTR_INTO_REG(reg, attr_id)
```

Mixed leaf/linked conditions use one instruction per operand.

This removes the fragile `READ_LINKED_TO_MEM` + heap offset + typed
heap-load sequence from RonSQL's CASE-condition codegen and avoids
adding separate signed heap-load opcodes for `INT8/16/24/32`.

## Tests

Add kernel/NDB API coverage before relying on the opcode from RonSQL:

1. Linked signed `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`
   values loaded into registers and compared against leaf columns.
2. Negative values for every signed width, proving sign extension.
3. Unsigned values for every unsigned width, proving zero extension.
4. NULL linked value marks the destination register NULL and branches
   behave consistently with existing register NULL semantics.
5. Validation rejects unsupported type codes cleanly.
6. The same program shape is accepted by all relevant interpreter
   validators and dispatch tables.

Then add RonSQL Phase I.5 v2a/v2b tests using `INT`, `SMALLINT`, and
`TINYINT` linked operands, including negative values.

## Deliverables

- `Interpreter.hpp` — opcode definition, encoding helper, decoder
  helpers if needed.
- `DbtupExecQuery.cpp` — execution handler and dispatch table entries.
- `JoinAggInterpreter.cpp` / `AggInterpreter.cpp` — validation and
  word-count support.
- `NdbInterpretedCode` and/or `NdbAggregator` helper API, depending
  on the chosen RonSQL emit route.
- NDB API/kernel tests for the instruction.
- RonSQL plan update linking Phase I.5 v2a typed linked-column support
  to this v5 instruction.

## Sequencing

Implement v5 before enabling signed sub-64-bit linked-column operands
in I.5 v2a.  Until v5 lands, v2a may still handle:

- leaf-vs-leaf integer comparisons;
- leaf-vs-linked comparisons where the linked side is BIGINT or an
  unsigned width supported by existing heap-load instructions;
- any shape that uses `NdbAggregator::LoadLinkedColumn` in the normal
  aggregation program rather than embedded CASE codegen.

Do not silently zero-extend signed linked values as an interim shortcut.
