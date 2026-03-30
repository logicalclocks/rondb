# Plan: Support parent-table columns in CASE conditions

## Problem

CASE expressions in aggregation queries can reference parent-table columns:
```sql
SELECT l.l_shipmode,
       SUM(CASE WHEN o.o_orderpriority = '1-URGENT' THEN 1 ELSE 0 END) AS urgent
FROM tpch_orders AS o
JOIN tpch_lineitem AS l ON l.l_orderkey = o.o_orderkey
GROUP BY l.l_shipmode
```

`o.o_orderpriority` is on the parent table (orders), not the leaf table (lineitem).
The CASE embedded interpreter currently only supports leaf-table columns via
`readAttributes(attrId)`. Parent columns arrive as linked attributes and aren't
accessible by attrId within the NDB interpreter.

## Architecture

The CASE condition is compiled into an **embedded interpreter** section within
the aggregation program. This section runs `interpreterNextLab()` (the standard
NDB interpreter) which can:
- Read local tuple columns via `readAttributes(attrId)` → `BRANCH_ATTR_OP_ARG`
- Access heap memory (`cheapMemory`) via `READ_ATTR_TO_MEM` etc.

Parent column values are in `JoinAggInterpreter::m_linked_attr_data`, which is
NOT accessible from `interpreterNextLab()`.

## Solution: Two-stage load + memory branch

### Stage 1: New AggInterpreter instruction — `kOpLoadLinkedToMem`

Before the `kOpEmbeddedInterp` section, emit a new instruction that copies the
linked column value from `m_linked_attr_data` into `block_tup->cheapMemory` at
a fixed offset (e.g., offset 0).

Format: `kOpLoadLinkedToMem | (linked_position << 16)`

Execution in JoinAggInterpreter:
1. Find linked column at `position` in `m_linked_attr_data`
2. Copy `[AttributeHeader + data]` into `block_tup->cheapMemory[0..]`
3. Store the data length for the embedded interpreter to use

### Stage 2: New NDB interpreter instruction — `BRANCH_MEM_OP_ARG`

A new interpreter opcode that works like `BRANCH_ATTR_OP_ARG` but reads the
column value from heap memory instead of calling `readAttributes()`.

Format (2 + N words):
- Word 0: `BRANCH_MEM_OP_ARG | (cond << 12) | (null_semantics << 6) | (branch_offset << 16)`
  (same layout as BRANCH_ATTR_OP_ARG)
- Word 1: `(memory_offset << 16) | arg_byte_len`
  (memory_offset replaces attrId, arg_byte_len replaces attrId+len encoding)
- Words 2..N: inline constant data (same as BRANCH_ATTR_OP_ARG)

Execution in `interpreterNextLab()`:
1. Read column data from `TheapMemoryChar[memory_offset]` (AttributeHeader + data)
2. Get type info from the AttributeHeader (type encoded in header by kOpLoadLinkedToMem)
3. Read inline constant
4. Compare using `NdbSqlUtil::cmp()` (need type — encode in instruction or use raw memcmp)
5. Branch based on condition

### Type comparison challenge

`BRANCH_ATTR_OP_ARG` gets type info from the table descriptor via attrId. For
`BRANCH_MEM_OP_ARG`, we don't have an attrId. Options:

**A. Encode type in instruction**: Add typeId to Word 1. The AggInterpreter
   knows the column type from the NdbDictionary::Column at program build time.

**B. Raw byte comparison**: For CHAR/VARCHAR (the common CASE condition type),
   a byte-level comparison suffices. Encode a "raw comparison" flag.

**C. Use attrId for type lookup only**: Pass the original attrId in the
   instruction for descriptor lookup but read data from memory. The attrId
   just determines comparison semantics.

Option C is simplest — reuse existing type lookup logic with minimal changes.

### Instruction format (Option C)

Word 0: `BRANCH_MEM_OP_ARG | (cond << 12) | (null_semantics << 6) | (branch_offset)`
Word 1: `(attrId << 16) | arg_byte_len`  — attrId for type lookup, data from memory
Words 2..N: inline constant data

The only difference from `BRANCH_ATTR_OP_ARG`: instead of `readAttributes(attrId)`
to get column data, read from `cheapMemory[0]` (where `kOpLoadLinkedToMem` placed it).

## Implementation Steps

### Step 1: Add `BRANCH_MEM_OP_ARG` to Interpreter.hpp
- New opcode constant (next available: check existing opcodes)
- Add `BranchMem()` and `BranchMem_2()` static helper methods

### Step 2: Add `kOpLoadLinkedToMem` to NdbAggregationCommon.hpp
- New aggregation opcode

### Step 3: Implement `kOpLoadLinkedToMem` in JoinAggInterpreter.cpp
- Find linked column by position in `m_linked_attr_data`
- Copy AttributeHeader + data into `block_tup->cheapMemory[0]`
- Must execute BEFORE the kOpEmbeddedInterp section

### Step 4: Implement `BRANCH_MEM_OP_ARG` in DbtupExecQuery.cpp
- Add case in `interpreterNextLab()` switch
- Read data from `TheapMemoryChar[0]` instead of `readAttributes()`
- Use `attrId` from instruction for type/charset lookup (same as BRANCH_ATTR_OP_ARG)
- Compare against inline constant, branch on result

### Step 5: Add NdbAggregator API methods
- `LoadLinkedToMem(Uint32 position)` — emits kOpLoadLinkedToMem
- Modify `EmitEmbeddedWord()` or add helper to emit BRANCH_MEM_OP_ARG words

### Step 6: Update RonSQLPreparer.cpp
- In `build_agg_linked_projections`: add linked projection for CASE parent columns
- In `generate_embedded_condition`: when CASE column is on parent table:
  - Emit `LoadLinkedToMem(linked_position)` before `EmbeddedInterp`
  - Inside embedded section: emit `BRANCH_MEM_OP_ARG` with attrId (for type) + constant
  - Use memory_offset=0 (where LoadLinkedToMem placed the data)

### Step 7: Test
- Run Demo 5 (TPC-H Q12 with CASE on parent column)
- Run existing testJoinAgg / testCaseAgg to ensure no regression
