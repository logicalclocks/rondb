# Phase I.18 — typed normal-interpreter registers

## Status

**Kernel + NDB-API surface shipped.**  Option B (full typed registers)
landed across commits 0786adb0f19 → 2784d7746e5 on
`RONDB-1050-cte-filter`.  See **What shipped** below.  Outstanding:
RonSQL-side MTR coverage that drives the new producer/consumer paths
end-to-end (negative leaf values, float, Bigunsigned > INT64_MAX) —
queued as a follow-up since the kernel infrastructure is independently
verifiable.

The original plan (Option A — a single typed leaf-load opcode
mirroring v5) was rejected in favour of full typed registers.  This
turned out to be the correct fix not just for the v5 leaf-side
zero-extension bug but also for `Bigunsigned` semantics above
`INT64_MAX` and floating-point operands.

## What shipped

Commits on `RONDB-1050-cte-filter`:

| Commit | Scope |
|--------|-------|
| `7d7ec16f82a` | Phase doc (Option B design) |
| `0786adb0f19` | Type-word constants in `Interpreter.hpp` (`REG_TW_*_BYTE`, `REG_TYPE_NULL/INT/UINT/DOUBLE`) |
| `2903dd21e94` | Producers batch 1 — 6 typed `READ_UINT*_MEM_TO_REG` / `READ_UINT*_REG_TO_REG` handlers write `REG_TYPE_UINT` |
| `bab40b92982` | Producers batch 2 — `handleReadAttrIntoReg` descriptor-driven dispatch (10 NDB integer widths via `sint*korr`/`uint*korr`, Float→double, Bigint/Bigunsigned/Double 64-bit cell); `handleReadAggRegToReg` propagates `is_unsigned`; `handleReadLinkedColumnToReg` switches to `REG_TYPE_INT/UINT` |
| `ed93f65716a` | Consumers batch 1 — `compareTypedRegs` / `compareTypedRegConst` helpers; 12 type-aware branch handlers |
| `79b2ef09f59` | Consumers batch 2 — `applyTypedArith` helper; 10 type-aware arithmetic handlers (float promotion, mixed signed/unsigned → unsigned) |
| `025e11c8bc2` | Consumers batch 3a — `applyTypedBitwise` helper; 11 bitwise/shift handlers (reject float; arithmetic vs logical RSHIFT by signedness) |
| `3c655ee7864` | Consumers batch 3b — write-back handlers: strict-type on `WRITE_UINT*/INT64_REG_TO_MEM/REG`; float-reject on `WRITE_ATTR_FROM_REG`, `WRITE_INTERPRETER_OUTPUT`, `CONVERT_SIZE`; `READ_INTERPRETER_INPUT` marks `REG_TYPE_INT` |
| `570530f3a85` | New opcode `WRITE_REG_TO_MEM_ANY` (slot 62) — type-agnostic 8-byte register-to-memory escape hatch; encoder + handler + dispatch + `NdbInterpretedCode::write_reg_to_mem_any_const` |
| `2784d7746e5` | New opcode `LOAD_DOUBLE_CONST` (slot 45) — IEEE-754 double immediate load, marks `REG_TYPE_DOUBLE`; encoder + handler + dispatch + `NdbInterpretedCode::load_double_const`.  `LOAD_INT64_CONST` deliberately not added: existing `LOAD_CONST64` (slot 6) already produces `REG_TYPE_INT` since `NOT_NULL_INDICATOR == 1` is bit-identical to `REG_TYPE_INT` |

**Not added** — `LOAD_INT64_CONST` (slot was tentative, ended up
redundant under the typed-word convention).  Slot 46 stays free.

## Outstanding follow-ups

1. **RonSQL MTR coverage** — extend `ronsql_cte_greatest_least_v5.test`
   (or add a sibling test file) with negative leaf-side values
   (the original I.18 motivating bug), float operands, and
   `Bigunsigned > INT64_MAX`.  Drives the new producer/consumer
   paths end-to-end.  Requires RonSQL emit changes for floats
   (`LOAD_DOUBLE_CONST`) and unsigned constants if any RonSQL
   surface needs to load `Uint64 > INT64_MAX` constants — the
   current pipeline only encounters unsigneds via column reads,
   which already work.
2. **Optional**: a dedicated `LOAD_UINT64_CONST` if a future
   RonSQL surface ever needs to load a constant Uint64 > INT64_MAX
   (today nothing does — large unsigned constants only appear
   from column reads).

## Background

After Phase I.5 v5 the linked side decodes typed integer values
correctly (`READ_LINKED_COLUMN_TO_REG` → sign-extended `Int64` /
zero-extended `Uint64`).  The leaf side still goes through
`READ_ATTR_INTO_REG`, which calls `readAttributes` and copies the
raw 32-bit data word into the register's `Int64` slot via implicit
assignment.  For signed sub-Bigint columns with negative values the
high bits are zero rather than sign-extended; comparisons against a
correctly-decoded linked value give wrong results.

The fix is layered:

1. Each register acquires a type word: NULL flag, signed/unsigned
   flag, integer/float flag.
2. Every producer (load, read-attr, etc.) sets the type word to
   match the value it placed.
3. Every consumer (branch, arithmetic, write-back) honours the
   type word — correct sign extension on assignment, type-aware
   comparison, mixed-type arithmetic where necessary.
4. Two new producers — `LOAD_INT64_CONST` (signed) and
   `LOAD_DOUBLE_CONST` — round out the constant family so any
   constant value can be loaded with the right type.

## Register layout

Per register stride stays at 4 `Uint32` slots (16 bytes).  No
callers assume a 3-slot stride, and the 4-slot stride keeps the
existing `*(Int64*)(buf + reg + 2)` payload accesses unchanged.

```text
slot 0   type word (one byte per flag — see below)
slot 1   reserved / scratch (used today by readAttributes for the
         AttrHeader during the read-into-reg path; keep that)
slots 2-3   64-bit value
            (Int64  for signed integer
             Uint64 for unsigned integer
             double for floating point — bit pattern stored in 8B)
```

### Type word encoding — one byte per flag

Each flag is a separate byte of the slot 0 `Uint32`, so most CPUs
can read or write each flag with a single-byte load/store and avoid
shift / mask sequences:

```text
byte 0 (bits  0.. 7)   NOT_NULL byte    1 = register holds a value, 0 = NULL
byte 1 (bits  8..15)   UNSIGNED byte    1 = unsigned integer, 0 = signed/float/null
byte 2 (bits 16..23)   FLOAT byte       1 = double-precision float, 0 = integer/null
byte 3 (bits 24..31)   reserved         always 0 today
```

Resulting slot 0 `Uint32` values:

| Encoding | Hex value | Meaning |
|----------|-----------|---------|
| `0x00000000` | 0 | NULL (existing `NULL_INDICATOR`) |
| `0x00000001` | 1 | signed integer (existing `NOT_NULL_INDICATOR`) |
| `0x00000101` | 257 | unsigned integer (NEW) |
| `0x00010001` | 65537 | floating-point double (NEW) |

NULL stays at zero, matching the existing `NULL_INDICATOR` so:

- `slot == 0` still detects NULL.
- `slot != 0` still detects non-NULL (every non-NULL encoding has
  byte 0 = 1).
- `(left & right) != 0` still detects "both non-NULL" — every
  non-NULL encoding has byte 0 set, so the AND of byte 0 is
  non-zero whenever both operands are non-NULL.

Single-byte tests on most CPUs:

```cpp
const Uint8* tw = reinterpret_cast<const Uint8*>(&slot);
bool is_null     = tw[0] == 0;
bool is_unsigned = tw[1] != 0;
bool is_float    = tw[2] != 0;
```

Header constants:

```cpp
#define REG_TW_NOT_NULL_BYTE  0u
#define REG_TW_UNSIGNED_BYTE  1u
#define REG_TW_FLOAT_BYTE     2u
#define REG_TW_RESERVED_BYTE  3u

#define REG_TYPE_NULL         0x00000000u
#define REG_TYPE_INT          0x00000001u   /* byte 0 = 1 */
#define REG_TYPE_UINT         0x00000101u   /* byte 0 = 1, byte 1 = 1 */
#define REG_TYPE_DOUBLE       0x00010001u   /* byte 0 = 1, byte 2 = 1 */
```

## Backward compatibility

Existing producers that wrote `NOT_NULL_INDICATOR (= 1)` continue
to mean "signed integer" — `REG_TYPE_INT` is bit-identical.  No
changes required for handlers that already produce signed `Int64`
values (e.g. `READ_INT64_MEM_TO_REG`).  Producers that today write
a `Uint*` value into the `Int64` slot without sign extension
(e.g. `READ_UINT*_MEM_TO_REG`, `READ_ATTR_INTO_REG` for sub-Bigint
signed columns) need to be classified — either marked
`REG_TYPE_UINT` if the value really is unsigned, or sign-extended
into `Int64` if the source is signed.

Consumers that today blindly read `*(Int64*)(buf + reg + 2)` and
compare as signed `Int64` get refactored to read the type word
first and dispatch.  When both operands have matching type-word
bytes the consumer's existing fast path keeps working — a typical
"both signed integer" pair-op gets one extra `(left | right)` check
on the unsigned/float bytes and falls through to the existing
comparison.

Consumers that compare against `slot == NOT_NULL_INDICATOR` (the
literal `1`) rather than `slot != 0` need to be audited.  For
unsigned-int registers `slot == 0x101 != 1`, so any equality
comparison against the literal `1` would misclassify unsigned
values as NULL.  Plan grep audit:

```
rg "TregMemBuffer\[.*\] == NOT_NULL_INDICATOR|TregMemBuffer\[.*\] == 1"
```

## Two new producer opcodes

Both new opcodes go in slots 45 and 46 (currently unused —
`Interpreter.hpp:262` notes 45 / 46 are free; v5 took 44).

### `LOAD_INT64_CONST` (signed) — opcode 45

```text
LOAD_INT64_CONST
  Encoding: opcode | (RegDest << 6)
  Followed by 2 words containing the signed 64-bit immediate.

slot 0 ← REG_TYPE_INT
slots 2-3 ← Int64 immediate
```

Coexists with the existing `LOAD_CONST64` (opcode 6).  Audit
`LOAD_CONST64` first: if it already stores a signed `Int64` and
sets `REG_TYPE_INT` (which is what `NOT_NULL_INDICATOR == 1`
already means under the new convention) then `LOAD_CONST64` keeps
its semantics and `LOAD_INT64_CONST` is redundant — drop it.  If
`LOAD_CONST64` writes the slot 0 ambiguously (e.g. only sets the
NOT_NULL byte without committing to integer-vs-float), keep both
and use the new opcode whenever the producer wants an explicit
signed integer.

### `LOAD_DOUBLE_CONST` — opcode 46

```text
LOAD_DOUBLE_CONST
  Encoding: opcode | (RegDest << 6)
  Followed by 2 words containing the IEEE 754 double bit pattern.

slot 0 ← REG_TYPE_DOUBLE
slots 2-3 ← double immediate (bit pattern)
```

Required: there is no existing way to load a floating-point
constant into a register.

## Instruction inventory (handlers to update)

Producers (write a register's slot 0):

```
LOAD_CONST_NULL                    (3)    handleLoadConstNull
LOAD_CONST16                       (4)    handleLoadConst16
LOAD_CONST32                       (5)    handleLoadConst32
LOAD_CONST64                       (6)    handleLoadConst64
READ_ATTR_INTO_REG                 (1)    handleReadAttrIntoReg
READ_AGG_REG_TO_REG                (43)   handleReadAggRegToReg
READ_LINKED_COLUMN_TO_REG          (44)   handleReadLinkedColumnToReg
READ_UINT8_MEM_TO_REG              (49)   handleReadUint8MemToReg
READ_UINT16_MEM_TO_REG             (50)   handleReadUint16MemToReg
READ_UINT32_MEM_TO_REG             (51)   handleReadUint32MemToReg
READ_INT64_MEM_TO_REG              (52)   handleReadInt64MemToReg
READ_UINT8_REG_TO_REG              (49+OF) handleReadUint8RegToReg
READ_UINT16_REG_TO_REG             (50+OF) handleReadUint16RegToReg
READ_UINT32_REG_TO_REG             (51+OF) handleReadUint32RegToReg
READ_INT64_REG_TO_REG              (52+OF) handleReadInt64RegToReg
LOAD_INT64_CONST                   (45)   handleLoadInt64Const     (NEW)
LOAD_DOUBLE_CONST                  (46)   handleLoadDoubleConst    (NEW)
```

Consumers (compute against / write a register):

```
BRANCH_REG_EQ_NULL                 (10)   handleBranchRegEqNull
BRANCH_REG_NE_NULL                 (11)   handleBranchRegNeNull
BRANCH_EQ_REG_REG                  (12)   handleBranchEqRegReg
BRANCH_NE_REG_REG                  (13)   handleBranchNeRegReg
BRANCH_LT_REG_REG                  (14)   handleBranchLtRegReg
BRANCH_LE_REG_REG                  (15)   handleBranchLeRegReg
BRANCH_GT_REG_REG                  (16)   handleBranchGtRegReg
BRANCH_GE_REG_REG                  (17)   handleBranchGeRegReg
BRANCH_EQ/NE/LT/LE/GT/GE_REG_CONST       handleBranch*RegConst
ADD_REG_REG / ADD_REG_CONST        (7)    handleAddRegReg/Const
SUB_REG_REG / SUB_REG_CONST        (8)    handleSubRegReg/Const
LSHIFT_REG_REG / LSHIFT_REG_CONST  (28)   handleLshift*
RSHIFT_REG_REG / RSHIFT_REG_CONST  (29)   handleRshift*
MUL_REG_REG / MUL_REG_CONST        (30)   handleMul*
DIV_REG_REG / DIV_REG_CONST        (31)   handleDiv*
AND_REG_REG / AND_REG_CONST        (32)   handleAnd*
OR_REG_REG / OR_REG_CONST          (33)   handleOr*
XOR_REG_REG / XOR_REG_CONST        (34)   handleXor*
MOD_REG_REG / MOD_REG_CONST        (35)   handleMod*
NOT_REG_REG                        (36)   handleNotRegReg
WRITE_ATTR_FROM_REG                (2)    handleWriteAttrFromReg
WRITE_UINT8_REG_TO_MEM             (53)   handleWriteUint8RegToMem
WRITE_UINT16_REG_TO_MEM            (54)   handleWriteUint16RegToMem
WRITE_UINT32_REG_TO_MEM            (55)   handleWriteUint32RegToMem
WRITE_INT64_REG_TO_MEM             (56)   handleWriteInt64RegToMem
WRITE_UINT8_REG_TO_REG             (53+OF) handleWriteUint8RegToReg
WRITE_UINT16_REG_TO_REG            (54+OF) handleWriteUint16RegToReg
WRITE_UINT32_REG_TO_REG            (55+OF) handleWriteUint32RegToReg
WRITE_INT64_REG_TO_REG             (56+OF) handleWriteInt64RegToReg
WRITE_INTERPRETER_OUTPUT           (LOAD_CONST_MEM+OF)  handleWriteInterpreterOutput
READ_INTERPRETER_INPUT             (WRITE_ATTR_FROM_MEM+OF)  handleReadInterpreterInput
CONVERT_SIZE                       (60)   handleConvertSize
```

Roughly 50 handlers.  At 10 per commit the implementation fits in
five working batches plus a foundation commit and a final "two new
opcodes" commit.

## Commit plan

1. **Foundation commit.**  Add the `REG_TYPE_*` / `REG_TW_*`
   constants to `Interpreter.hpp` (or a kernel-private header).
   No handler changes yet — just the vocabulary.  Document that
   the existing `NULL_INDICATOR == 0` and
   `NOT_NULL_INDICATOR == 1` remain valid as `REG_TYPE_NULL` and
   `REG_TYPE_INT` so all current producers stay correct under
   the new convention.

2. **Producers batch 1** (≤10 handlers): the constant loaders +
   the simple typed register-to-memory readers
   (`LOAD_CONST_NULL`, `LOAD_CONST16`, `LOAD_CONST32`,
   `LOAD_CONST64`, `READ_INT64_MEM_TO_REG`,
   `READ_UINT8/16/32_MEM_TO_REG`, the corresponding
   `*_REG_TO_REG` variants).  Each writes the right slot 0 value
   (signed → `REG_TYPE_INT`, unsigned → `REG_TYPE_UINT`).

3. **Producers batch 2** (≤10): the attribute / linked / agg-reg
   readers (`READ_ATTR_INTO_REG` typed dispatch by column type,
   `READ_AGG_REG_TO_REG` carries source type from the
   aggregation register, `READ_LINKED_COLUMN_TO_REG` propagates
   `ndb_type` into the type word).

4. **Consumers batch 1: branches** (≤10): the
   `BRANCH_*_REG_REG` and `BRANCH_*_REG_CONST` family.
   Type-aware compare:
   - both signed → `Int64` compare;
   - both unsigned → `Uint64` compare;
   - mixed signed/unsigned → if signed operand is negative the
     result is decided immediately; otherwise compare both as
     `Uint64`;
   - either is float → convert both to `double` and compare.

5. **Consumers batch 2: arithmetic** (≤10): `ADD/SUB/MUL/DIV/MOD`,
   reg-reg and reg-const variants.  Result type follows the
   wider operand: float wins over int; unsigned wins over signed
   unless one is negative.  Document edge cases inline.

6. **Consumers batch 3: bitwise + shift + write-back** (≤10):
   `AND/OR/XOR/NOT/LSHIFT/RSHIFT` (integer-only — reject if
   either operand is float),
   `WRITE_UINT*_REG_TO_*`, `WRITE_INT64_REG_TO_*`,
   `WRITE_ATTR_FROM_REG`, `CONVERT_SIZE`,
   `WRITE_INTERPRETER_OUTPUT`, `READ_INTERPRETER_INPUT`.

7. **Two new opcodes**: `LOAD_INT64_CONST` and `LOAD_DOUBLE_CONST`
   in slots 45 and 46.  Encoder + handler + dispatch tables +
   validators.  Single commit.

8. **MTR coverage**: extend `ronsql_cte_greatest_least_v5.test`
   with negative leaf-side values (the case I.18 was originally
   about), plus floating-point and `Bigunsigned > INT64_MAX`
   coverage where RonSQL can drive it.  Final commit.

Each batch is one commit.  Total: 7-8 commits.

## Test strategy per batch

After each batch:

- Existing MTR tests still pass (existing handlers' default
  signed-integer behaviour preserved).
- Block tests still pass.
- Where a batch unlocks new behaviour (e.g. branches batch ships
  type-aware compare), add a focused test in the same commit.

The final MTR commit (step 8) is the comprehensive coverage that
this whole phase justifies.

## Risks

1. **Validators** — three (main interpreter, CTE filter, agg
   embedded) plus `JoinAggInterpreter::validateEmbeddedProgram`
   and `getInstructionPreProcessingInfo`.  The two new opcodes
   need to be whitelisted in all of them.  Same drift risk as v5.
2. **`READ_ATTR_INTO_REG`** is used by many programs not under
   our direct control (legacy filters, NDB API users).  The
   default behaviour must stay backward-compatible: signed
   integer columns produce `REG_TYPE_INT`; unsigned columns
   produce `REG_TYPE_UINT`.  That changes the slot 0 value for
   unsigned columns from `1` to `0x101` — any consumer that does
   `slot == NOT_NULL_INDICATOR` rather than `slot != 0` would
   misread unsigned registers as NULL.  Audit all `slot ==`
   comparisons before flipping the default.
3. **Integer constant folding** in handlers like
   `handleAddRegConst` reads the constant from the instruction
   word.  Constants in the instruction stream are signed today;
   the type word distinction is only between signed and unsigned
   *registers*, not constants.  Document this clearly in each
   reg-const handler.
4. **`Bigunsigned > INT64_MAX`** comparison: signed-vs-unsigned
   compare with one operand `> INT64_MAX` requires care.  The
   branch handler handles it explicitly via a "if signed operand
   is negative, signed < unsigned" short-circuit; otherwise both
   fit in `Uint64` and standard unsigned comparison works.

## Out of scope

- **DECIMAL** typed registers — DECIMAL has variable
  precision/scale and doesn't fit cleanly in 64 bits.  Defer.
- **String / temporal** typed registers — out of scope; these
  don't compare via the register-machine path.
- **Aggregation interpreter** (`AggInterpreter`) registers —
  they already have explicit `type` / `is_unsigned` / `is_null`
  fields on `Register`.  This phase is about the *normal*
  interpreter catching up.

## Sequencing within I.5 / I.18

I.5 v3 (Float / Decimal / VARCHAR) is the natural consumer of
this work for the floating-point side.  Once I.18 ships, RonSQL
can emit `LOAD_DOUBLE_CONST` in CASE conditions and pair-op
expressions with float operands.

I.18 supersedes the leaf-side `READ_ATTR_TYPED_TO_REG` follow-up
that was queued separately — the typed-register infrastructure
makes `READ_ATTR_INTO_REG` itself type-aware and removes the
need for a parallel typed opcode.
