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

---

## Implementation plan

### Scope summary

This plan targets the integer family.  Float / double / decimal /
string / temporal types remain rejected with a clear message and are
deferred to v3 / I.6.  The single new opcode replaces the existing
two-instruction `READ_LINKED_TO_MEM + READ_*_MEM_TO_REG_CONST`
sequence in v2a's embedded-CASE col-vs-col path; v2b's pair-op
emission already uses `LoadLinkedColumn` + `READ_AGG_REG_TO_REG` and
is unaffected by v5.

### Opcode selection and encoding

Slot 44 in the interpreter opcode table is free
(`Interpreter.hpp:260` notes "44-46 free, both of them").  Use:

```cpp
static constexpr Uint32 READ_LINKED_COLUMN_TO_REG = 44;
```

One-word encoding (column type fits the spare upper byte cleanly):

```text
bits  0..5   opcode (6)
bits  6..8   dest reg (3)         — getReg1 reads here
bits  9..15  unused / reserved (7)
bits 16..23  linked position (8)  — matches READ_LINKED_TO_MEM convention
bits 24..31  NDB column type (8)  — NDB_TYPE_TINYINT..BIGUNSIGNED fit in 8 bits
```

Encoder helper added in `Interpreter.hpp` next to the other
`Read*MemToReg*` helpers:

```cpp
static Uint32 ReadLinkedColumnIntoReg(Uint32 RegDest,
                                      Uint32 Position,
                                      Uint32 NdbColumnType);

inline Uint32
Interpreter::ReadLinkedColumnIntoReg(Uint32 RegDest, Uint32 Position,
                                     Uint32 NdbColumnType) {
  return (NdbColumnType << 24)
       | (Position << 16)
       | (RegDest << 6)
       | READ_LINKED_COLUMN_TO_REG;
}
```

### Kernel handler

New handler in `DbtupExecQuery.cpp`, modelled on the existing
`handleReadLinkedToMem` (linked walk) and `handleReadInt64MemToReg`
(register write).  Sketch:

```cpp
static inline int handleReadLinkedColumnToReg(InterpreterContext& ctx) {
  Uint32 position = (ctx.theInstruction >> 16) & 0xFF;
  Uint32 type     = (ctx.theInstruction >> 24) & 0xFF;

  // 1. Walk the linked-attr buffer to position N (same loop shape as
  //    handleReadLinkedToMem at DbtupExecQuery.cpp:6685).
  const Uint32* linked   = ctx.req_struct->m_linked_attr_data;
  Uint32 linked_len      = ctx.req_struct->m_linked_attr_len;
  if (unlikely(linked == nullptr)) {
    ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
    return INTERP_CONTINUE;
  }
  const Uint32* p     = linked;
  const Uint32* p_end = linked + linked_len;
  Uint32 pos_count = 0;
  while (p < p_end) {
    if (pos_count == position) break;
    p += 2;                                      // tableId, schemaVersion
    p += 1 + AttributeHeader::getDataSize(*p);
    pos_count++;
  }
  if (unlikely(p >= p_end)) {
    ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
    return INTERP_CONTINUE;
  }

  // 2. Skip tableId, schemaVersion; inspect AttributeHeader.
  p += 2;
  AttributeHeader ah(*p);
  if (ah.isNULL()) {
    ctx.TregMemBuffer[ctx.theRegister] = NULL_INDICATOR;
    return INTERP_CONTINUE;
  }

  // 3. Decode by NDB column type.  Data starts at p + 1 (after AH).
  const char* data = (const char*)(p + 1);
  Int64 sval = 0;
  Uint64 uval = 0;
  bool is_unsigned = false;
  switch (type) {
    case NDB_TYPE_TINYINT:
      sval = *reinterpret_cast<const Int8*>(data); break;
    case NDB_TYPE_TINYUNSIGNED:
      uval = *reinterpret_cast<const Uint8*>(data); is_unsigned = true; break;
    case NDB_TYPE_SMALLINT:
      sval = sint2korr(data); break;
    case NDB_TYPE_SMALLUNSIGNED:
      uval = uint2korr(data); is_unsigned = true; break;
    case NDB_TYPE_MEDIUMINT:
      sval = sint3korr(data); break;
    case NDB_TYPE_MEDIUMUNSIGNED:
      uval = uint3korr(data); is_unsigned = true; break;
    case NDB_TYPE_INT:
      sval = sint4korr(data); break;
    case NDB_TYPE_UNSIGNED:
      uval = uint4korr(data); is_unsigned = true; break;
    case NDB_TYPE_BIGINT:
      memcpy(&sval, data, 8); break;
    case NDB_TYPE_BIGUNSIGNED:
      memcpy(&uval, data, 8); is_unsigned = true; break;
    default:
      return -ZNO_INSTRUCTION_ERROR;             // unsupported type
  }

  ctx.TregMemBuffer[ctx.theRegister] = NOT_NULL_INDICATOR;
  if (is_unsigned)
    *(Uint64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = uval;
  else
    *(Int64*)(ctx.TregMemBuffer + ctx.theRegister + 2) = sval;
  return INTERP_CONTINUE;
}
```

The register's NULL flag (bits at `TregMemBuffer[theRegister]`) is
written exactly the same way as `handleReadAggRegToReg`, so the
existing `BRANCH_REG_EQ_NULL` / `BRANCH_REG_NE_NULL` path picks
`NULL_INDICATOR` up correctly.

### Dispatch tables (kernel)

Three tables in `DbtupExecQuery.cpp` need the entry — main interpreter
(line ~8918), CTE filter handlers (line ~9074), and the aggregation
embedded handlers (line ~9078, the `s_agg_interp_handlers` family).
All three have an explicit slot 44 today (currently `nullptr`); fill
in:

```cpp
/*  44  READ_LINKED_COLUMN_TO_REG */
    &Dbtup::InterpreterContext::handleReadLinkedColumnToReg,
```

The dispatch code at the bottom of `interpreterNextLab` (line ~9337
area) does not need a new explicit `case`; it goes through the table.

### Validators

- **JoinAggInterpreter** (`validateEmbeddedProgram`) — add
  `Interpreter::READ_LINKED_COLUMN_TO_REG` to both the opcode
  whitelist switch (around line 540) and the word-count switch
  (one word per instruction).
- **AggInterpreter** — if the agg-interpreter validator has its own
  whitelist (mirror of the JoinAgg one), add the entry.
- **CTE filter validator / preparer** — if a separate whitelist
  exists in the CTE filter path, add it.  Search:
  ```
  rg "READ_INT64_MEM_TO_REG" storage/ndb/src/kernel
  ```
  and whitelist `READ_LINKED_COLUMN_TO_REG` everywhere the existing
  family is whitelisted.

### NdbInterpretedCode emit helper

Add to `NdbInterpretedCode.hpp` / `.cpp`:

```cpp
int read_linked_column_to_reg(Uint32 RegDest,
                              Uint32 Position,
                              const NdbDictionary::Column* col);
```

Implementation: validate the column type is in the integer family
(`Tinyint .. Bigunsigned`), look up the matching `NDB_TYPE_*`
constant, emit one word via `Interpreter::ReadLinkedColumnIntoReg`.

For RonSQL's direct emit path, the existing `aggregator->EmitEmbeddedWord(...)`
mechanism is sufficient — RonSQL builds the word with
`Interpreter::ReadLinkedColumnIntoReg(...)` and emits it.

### RonSQL preparer changes

Single call site changes in `RonSQLPreparer.cpp`:

`generate_embedded_condition` — the linked col-vs-col path
(`SideKind::LinkedParent` / `SideKind::LinkedCteCol`).  Today it
emits the two-word sequence:

```cpp
// READ_LINKED_TO_MEM(position)         -- 1 word
// READ_INT64_MEM_TO_REG(reg, /*offset=*/4) -- 1 word, BIGINT-only
```

After v5 collapse to one word per linked operand:

```cpp
// READ_LINKED_COLUMN_TO_REG(reg, position, col_type)  -- 1 word
```

Other call site updates:

- `validate_greatest_least_pair_loads` (RonSQLPreparer.cpp) — drop
  the per-operand "Bigint-only" restriction for linked operands.
  Leaf operands stay covered by `READ_ATTR_INTO_REG`.
- `compute_pair_op_needs_null_check` — unchanged; nullability is
  still expressed at the column descriptor level.
- v2a's "InlineLinked rejected" guard in
  `generate_embedded_condition` — keep, since `READ_LINKED_COLUMN_TO_REG`
  walks the linked-attr buffer rather than the inline CteLinkedAttr
  encoding.  Inline-typed CTE leaf columns still need a separate
  follow-up if v2a needs to reach them — out of scope for v5.

`raw_word_size` math: per linked-col-vs-col atom drops by 1 word
(2→1).  Update the embedded-CASE word-counting comments / accounting
in `generate_embedded_condition` accordingly so embedded-CASE skip
distances stay correct.

### Step ordering

Land in this order so each step is buildable and testable on its
own:

1. **Kernel-only** — opcode constant, encoder helper, handler,
   three dispatch tables, validator whitelists, word-count
   accounting.  Build green; existing tests still pass; no caller
   yet.
2. **NDB API helper** — `NdbInterpretedCode::read_linked_column_to_reg`
   and any block-level test that emits the opcode.  Add a focused
   block-test (signed/unsigned 8/16/24/32/64, NULL, unsupported
   type rejection) that runs the opcode end-to-end without
   touching RonSQL.
3. **RonSQL emit migration** — switch
   `generate_embedded_condition`'s linked col-vs-col path from the
   two-word `READ_LINKED_TO_MEM + READ_INT64_MEM_TO_REG` sequence
   to the new one-word opcode.  Update the linked-side type check
   to accept Tinyint .. Bigunsigned.  Update raw-word accounting.
4. **MTR coverage** — extend
   `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v2a.test`
   (or a new `_v5.test`) with linked col-vs-col cases over `INT
   NOT NULL`, `SMALLINT NOT NULL`, `MEDIUMINT NOT NULL`, signed and
   unsigned.  Mix nullable variants once v4/v7 NULL propagation is
   verified to flow through the new opcode (it should — same
   register NULL_INDICATOR convention).
5. **Doc + memory updates** — flip this doc's Status to Shipped,
   update the I.5 catalogue / index / project memory.

### Tests

#### Kernel / block tests (Step 2)

`storage/ndb/block_unit_test/test*.cpp` — add cases for each
integer width:

| Type | Value | Expected register repr |
|------|-------|------------------------|
| Tinyint | -128, 0, 127 | sign-extended Int64 |
| Tinyunsigned | 0, 255 | zero-extended Uint64 |
| Smallint | -32768, 32767 | sign-extended Int64 |
| Smallunsigned | 65535 | zero-extended Uint64 |
| Mediumint | -8388608, 8388607 | sign-extended Int64 |
| Mediumunsigned | 16777215 | zero-extended Uint64 |
| Int | INT32_MIN, -1, INT32_MAX | sign-extended Int64 |
| Unsigned | UINT32_MAX | zero-extended Uint64 |
| Bigint | INT64_MIN, INT64_MAX | identity |
| Bigunsigned | UINT64_MAX | identity |

Plus three negative cases:
- NULL linked column → register `is_null` set; downstream
  `BRANCH_REG_EQ_NULL` fires.
- Out-of-range position → register `is_null` set (defensive).
- Unsupported type code → handler returns `-ZNO_INSTRUCTION_ERROR`,
  validator rejects at parse / load time.

#### MTR (Step 4)

`mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v5.test` (or
extend `_v2a`) covering linked operands of each width:

| Test | Shape |
|------|-------|
| 1 | `SUM(GREATEST(c.parent_int, child.amt))` — INT NOT NULL parent linked, BIGINT child leaf |
| 2 | `SUM(GREATEST(c.parent_smallint, child.amt))` — SMALLINT linked |
| 3 | `SUM(GREATEST(c.parent_mediumint, child.amt))` — MEDIUMINT linked |
| 4 | Tinyint linked (signed and unsigned) |
| 5 | Mixed-width: parent INT vs child BIGINT — verify sign extension across widths |
| 6 | Negative-value rows for each signed width — ensures correct sign extension |
| 7 | Nullable INT linked + v7 `SetRegNull` propagation |

The fixture sits alongside the existing v2b / v4 / v6 fixtures —
reuse `cte_customer` / `cte_orders` shapes where possible, add
extra columns where needed.

### Risks

1. **`AttributeHeader` size assumption.**  The handler reads
   `AttributeHeader::getDataSize(*p)` to skip per-column data;
   confirm that for sub-32-bit columns the data is right-padded to
   a 4-byte word boundary in the linked buffer — otherwise the
   `data + 1/2/3 byte` reads need different alignment care.
2. **CTE leaf inline-typed columns.**  `READ_LINKED_COLUMN_TO_REG`
   walks the linked-attr buffer; CTE-leaf columns use the inline
   `CteLinkedAttr` encoding via `BRANCH_MEM_OP_ARG_INLINE_TYPE`.
   v5 does not unify those — v2a's `InlineLinked` rejection stays
   in place until a separate follow-up adds an inline-typed
   register-load opcode.
3. **Endianness / korr helpers.**  All `sint*korr` / `uint*korr`
   helpers handle MySQL on-disk little-endian → host-int
   conversion uniformly.  Confirm the linked-attr buffer stores
   values in the same wire format as `tabDescriptor`-driven
   reads.  Reading the existing `kOpLoadCol` aggregation handler
   (`AggInterpreter.cpp:1391-1500`) confirms this — it uses the
   same `sint*korr` family.
4. **Validator drift.**  Three whitelists must agree on the new
   opcode.  If one slips through, the program parses in one
   interpreter and fails dispatch in another — exactly the v5 plan
   warns against.  Step 1's "build green; no caller yet" milestone
   is the place to catch this with a focused `rg "READ_INT64_MEM_TO_REG"`
   walk.

### Deliverables (concrete files)

- `storage/ndb/include/kernel/Interpreter.hpp` — opcode constant,
  encoder helper inline.
- `storage/ndb/include/kernel/AttributeDescriptor.hpp` /
  `ndb_constants.h` — confirm `NDB_TYPE_*` constants are reachable;
  no new constants needed.
- `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp` —
  `handleReadLinkedColumnToReg`; three dispatch table entries.
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.cpp` —
  validator whitelist + word-count switch.
- `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.cpp` —
  validator whitelist (if separate from JoinAgg).
- `storage/ndb/src/kernel/blocks/dbtup/PushdownInterpreter.cpp` —
  if its validator overlaps; check via grep for
  `READ_INT64_MEM_TO_REG`.
- `storage/ndb/include/ndbapi/NdbInterpretedCode.hpp` and
  `storage/ndb/src/ndbapi/NdbInterpretedCode.cpp` — public emit
  helper.
- `storage/ndb/block_unit_test/<test>.cpp` — kernel block test for
  the new opcode.
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp` — collapse two-word
  to one-word emission; lift Bigint-only restriction on linked
  operands; update embedded-CASE word-count accounting.
- `mysql-test/suite/ronsql/t/ronsql_cte_greatest_least_v5.test` (or
  extend `_v2a.test`).
- This doc — flip Status to Shipped with a "What shipped" section.
- Catalogue / index / memory updates as v5 lands.

### Verification

```
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ronsql_cli rdrs2

# Step 2 block test (post step 1):
./storage/ndb/block_unit_test/<v5-block-test>

# Step 4 MTR (post step 3):
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_greatest_least_v5
./mtr --suite=ronsql                 # full suite — no regressions
./mtr --suite=ndb_push_agg           # block tests — no regressions
```

Single commit per step (kernel-only, then NDB-API helper + block
test, then RonSQL emit migration + MTR), or a single bundled
commit if the kernel changes are small enough — the user's pattern
elsewhere has been bundled per-phase.
