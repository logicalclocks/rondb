# CTE Filter Phase D — Inline-Type Opcode for Synthesized Columns

## Context

Phase A of `cte_filter_plan.md` shipped `BRANCH_MEM_OP_ARG` (opcode 38). It
compares a linked-buffer entry to an inline constant. The opcode encodes
`[tableId, schemaVersion, attrId]` and the server (DBTUP) looks up the
column's type/charset descriptor via `tablerec[tableId]`.

`ronsql_cte_plan.md` Phase C delivered RonSQL support for that opcode, but
hit a sharp limitation: it only works for CTE outputs that are direct
column projections (`Outputs::Type::COLUMN`), where the linked-buffer slot
stores the source-column-typed value verbatim and the source column is a
real registered NDB column. **For aggregate outputs (SUM / MIN / MAX /
COUNT / AVG) the synthesized result type — Bigint for SUM(Int),
Bigunsigned for COUNT, Double for AVG — has no registered tableId, so the
lookup cannot resolve.**

Today such filters are rejected by `emit_cte_lookup_filter` with a
`require_prm` error. This phase lifts that restriction by adding a sibling
opcode that carries type info inline rather than indirecting through
`tablerec`.

## Goal

Add `BRANCH_MEM_OP_ARG_INLINE_TYPE` opcode + matching client-side emitter
+ RonSQL plumbing so RonSQL can push WHERE conjuncts on aggregate CTE
outputs down to the DBLQH/DBTUP filter interpreter.

## Scope

In scope:
- DBTUP: new handler `handleBranchMemOpArgInlineType` in
  `DbtupExecQuery.cpp`, registered in `s_cte_filter_handlers` only
  (not in the main / agg interpreter handler tables).
- NdbInterpretedCode: new public family `branch_linked_inline_<op>`
  (eq / ne / lt / le / gt / ge) sharing one private `branch_linked_inline_val`
  implementation. Mirrors the `branch_linked_mem_*` family.
- RonSQL: extend `emit_cte_lookup_filter` to dispatch on output kind —
  COLUMN goes through the existing `branch_linked_mem_*` path,
  AGGREGATE goes through the new inline path.
- Validator: update the CTE-safe opcode allow-list (server side via
  `s_cte_filter_handlers` table, client side via whatever
  `validateCteSafe` references — see Phase A note about a shared header).
- `testCteNdbApiFilter`: add a new test exercising filter on a SUM
  aggregate output. Confirms server-side handler works without a
  registered virt table column for that field.
- MTR `ronsql_cte_basic`: add a test exercising `WHERE sums.t > 100`
  on a SUM output.

Out of scope (deferred to a follow-up):
- DECIMAL aggregates (precision / scale). Encoding overhead and
  scarce real-world demand for filter-on-DECIMAL-aggregate. Reject
  explicitly in the new emitter.

CHAR / VARCHAR aggregates (MIN/MAX over text) **are in scope** —
deferring them invites architectural drift.  The kernel already has
`all_charsets[csNumber]` as the global charset registry (Dbdict and
DbtupMeta populate it as tables are loaded), so we encode the
`CHARSET_INFO::number` in the program and resolve server-side via
`all_charsets[csNumber]`.  At filter time the CTE body's source
table has already been loaded, so the entry is guaranteed live.

## Opcode design

**Reserved opcode index: 40.** Confirmed `nullptr` in both
`s_cte_filter_handlers` (DbtupExecQuery.cpp:8764) and
`s_agg_interp_handlers` (DbtupExecQuery.cpp:8917).  The comment at
`Interpreter.hpp:199` already notes `40-46 free, both of them`.
Cluster with the existing CTE opcodes 38 (`BRANCH_MEM_OP_ARG`) and
39 (`READ_LINKED_TO_MEM`).

### Architectural-validity audit (charset/length)

Audit of `Dblqh::buildCteLinkedBuffer` (`DblqhMain.cpp:18913+`):

- **Step 1 (parent linked projections)**: copied verbatim from the
  AttrInfo subroutine section; preserves source-table on-wire
  format (incl. VARCHAR length prefix and source charset).
- **Step 2 (GROUP BY keys)**: `memcpy` from groupData verbatim;
  preserves source-column on-wire format.  CHAR/VARCHAR GB keys
  work correctly through the inline opcode.
- **Step 3 (aggregate results)**: `AggResItem.value` is `Uint64`,
  always written as 8 bytes regardless of source type.  Implication:
  - SUM / COUNT: virt-table type derived as Bigint / Bigunsigned /
    Double — matches buffer (8 bytes).  ✅
  - MIN / MAX over numeric: virt-table type derived as source type
    (Int = 4 bytes etc.) — **does not match buffer** (8 bytes).
    Today's aggregator results read OK because LoadLinkedColumn /
    GroupByLinked ignore the claimed length, but a filter that
    relies on the virt-table descriptor would compare against
    the wrong byte count.
  - MIN / MAX over CHAR / VARCHAR: aggregator design (`AggResItem.
    value: Uint64`) cannot store variable-length strings, so this
    output kind doesn't exist in the agg-feed path.

This shapes the v1 RonSQL surface (see step 5) but does NOT
restrict the opcode itself — the opcode encodes type and charset
generically and works for any encodable type.

**Encoding** (4-word header — adds one word over `BRANCH_MEM_OP_ARG`'s
3-word data header to carry inline column metadata.  `columnSizeBytes`
fits in 16 bits because the largest NDB column is `Longvarchar` <=
65535+2 bytes; `csNumber` fits because MySQL charset numbers are
under 1024 even with extensions.  Packing them into one word keeps
the program tight):

```
word 0   opcode | cond | null-semantics | branch-offset   [same shape as BranchMem]
word 1   (typeId << 16) | argLen                          [reuse BranchMem_2 encoding]
word 2   (columnSizeBytes << 16) | csNumber               [packed metadata]
word 3+  inline constant data, padded to whole words
```

Reusing word-1's `BranchMem_2(typeId-as-attrId, argLen)` keeps the helper
extraction (`getBranchCol_AttrId`, `getBranchCol_Len`) reusable; we just
re-interpret "attrId" as "typeId" inside the new handler. (Alternative:
new accessor names. v1 reuses to keep the diff tight; a follow-up rename
is fine.)

`columnSizeBytes` is encoded explicitly because for VARCHAR it includes
the 1-or-2-byte length prefix on top of the max payload — the cmp
function expects the full on-wire size. For fixed-width numerics it
equals `argLen` and could be elided, but emitting it unconditionally
keeps the layout uniform.

`csNumber == 0` means "no charset" (numerics, binary). Non-zero means
look up `all_charsets[csNumber]`; the kernel already requires the entry
to be live (Dbdict / DbtupMeta populate it on table load).

**Server-side handler** (`handleBranchMemOpArgInlineType`):
```cpp
static inline int handleBranchMemOpArgInlineType(InterpreterContext& ctx) {
  thrjamDebug(ctx.tup->jamBuffer());
  const Uint32 ins2     = ctx.TcurrentProgram[ctx.TprogramCounter];
  const Uint32 typeId   = Interpreter::getBranchCol_AttrId(ins2);
  const Uint32 argLen   = Interpreter::getBranchCol_Len(ins2);
  const Uint32 meta     = ctx.TcurrentProgram[ctx.TprogramCounter + 1];
  const Uint32 colBytes = (meta >> 16) & 0xFFFF;
  const Uint32 csNumber = meta & 0xFFFF;

  const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(typeId);
  if (unlikely(sqlType.m_cmp == nullptr)) return -40;

  const CHARSET_INFO* cs = nullptr;
  if (csNumber != 0) {
    if (unlikely(csNumber >= NDB_ARRAY_SIZE(all_charsets))) return -40;
    cs = all_charsets[csNumber];
    if (unlikely(cs == nullptr)) return -40;
  }

  const Uint32* memData = (const Uint32*)&ctx.TheapMemoryChar[0];
  const AttributeHeader ah(memData[0]);
  const char* s1 = (const char*)&memData[1];
  const char* s2 = (const char*)&ctx.TcurrentProgram[ctx.TprogramCounter + 2];

  // ... null-semantics + m_cmp + branch dispatch
  // (same shape as handleBranchMemOpArg from this point onwards)
}
```

VARCHAR comparison flows through `NdbSqlUtil::Type::m_cmp` for type
`Varchar`, which already knows about the length-prefixed on-wire format
and uses `cs->coll->strnncoll` / equivalent for charset-aware compare.

`PC` advance: 1 (ins2) + 1 (meta) + ceil(argLen/4) data words.

## Implementation steps

1. **Pick opcode index, declare in `Interpreter.hpp`.**
   Search `s_cte_filter_handlers` and `s_main_handlers` for a slot
   that's `nullptr` in both. Add
   `BRANCH_MEM_OP_ARG_INLINE_TYPE = <index>` to `Interpreter::Opcode`.
2. **Add server handler.** New `handleBranchMemOpArgInlineType` in
   `DbtupExecQuery.cpp`, modeled on `handleBranchMemOpArg`. Wire into
   `s_cte_filter_handlers` at the chosen index; leave the agg / main
   tables at `nullptr`.
3. **Client-side emitter.** Add private
   `branch_linked_inline_val(branch_type, position, sourceColumn,
   val, valLen, label)` in `NdbInterpretedCode.cpp` plus six public
   wrappers (eq/ne/lt/le/gt/ge) and six declarations in
   `NdbInterpretedCode.hpp`.  The `sourceColumn` argument carries
   the synthesized (virt-table) column descriptor — the helper pulls
   `getType()`, `getSizeInBytes()`, and `getCharset()->number`
   out of it and encodes them in words 1-3.  Reject DECIMAL types
   with `error(QRY_OPERAND_HAS_WRONG_TYPE)` for v1; numeric and
   CHAR / VARCHAR types are accepted.  Always emits
   `READ_LINKED_TO_MEM` first (same as the existing family).
4. **Update CTE-safe validator.** Find where the client validates that
   a user's interpreted code uses only CTE-safe opcodes, and add the
   new index. Search hint:
   `grep -rn validateCteSafe storage/ndb/src/ndbapi/`.
5. **RonSQL emit.** In `emit_cte_lookup_filter`, dispatch on
   `Outputs::Type` for the matched output:
   - `COLUMN`: existing source-column path → `branch_linked_mem_*`.
   - `AGGREGATE / SUM`: virt-table type is Bigint / Bigunsigned /
     Double (matches the 8-byte buffer slot).  Pass the virt-table
     column descriptor (`vtcol`) to `branch_linked_inline_*`.
   - `AGGREGATE / COUNT`: virt-table type is Bigunsigned (matches
     8-byte slot).  Same path as SUM.
   - `AGGREGATE / MIN | MAX`: virt-table type follows source type
     (e.g. Int = 4 bytes), but buffer slot is 8 bytes — descriptor
     wouldn't match buffer encoding.  Continue to reject with
     `require_prm`.  Lift in a follow-up by either widening the
     virt-table type derivation to always 8-byte for MIN/MAX of
     numerics, or having the emitter widen at filter time.
   - `AGGREGATE / AVG`, DECIMAL aggregates: continue to reject.
6. **`testCteNdbApiFilter`.** Two new tests:
   - **6a (numeric SUM aggregate)**: filter `total > 40` on a SUM
     result (Bigint), built with the new
     `branch_linked_inline_*` family.  No `cte_virtual` MySQL table
     dependence for that column.
   - **6b (CHAR/VARCHAR GB key)**: filter on a CHAR or VARCHAR
     **GROUP BY key** column (where the linked-buffer slot
     preserves the source on-wire format with charset).  Uses the
     new inline opcode rather than the existing `branch_linked_mem_*`
     path, even though both would work — the goal is to exercise
     the charset-id round-trip through `csNumber →
     all_charsets[csNumber]` in the new handler before we ever
     ship aggregate-CHAR support.
7. **MTR tests.** Two new tests in `ronsql_cte_basic.test`:
   - **7a (numeric aggregate)**:
     ```sql
     WITH sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t
                   FROM cte_orders GROUP BY o_custkey)
     SELECT sums.k, SUM(sums.t)
     FROM cte_customer AS c
     JOIN sums ON sums.k = c.c_id
     WHERE sums.t > 100
     GROUP BY sums.k;
     ```
     Expected after `t > 100` filter: only k=100 (t=125) survives →
     row 100/125.
   - **7b (CHAR aggregate)**: filter `MAX(c_name) > 'B'` over the
     existing cte_customer / cte_orders fixture (c_name is
     VARCHAR(20)).  Concrete query TBD — requires a CTE that aggs
     over c_name and a main query that filters the result.  Goal is
     to exercise the charset round-trip in MTR.

## Risks & gotchas

- **Opcode index collision.** Audit both handler tables before
  picking; if no slot is free in [0, 127], the dispatch table needs
  to grow (currently `INTERP_HANDLER_TABLE_SIZE = 128`).
- **Reusing `BranchMem_2(attrId, len)` encoding.** The "attrId" field
  is 5 bits in the current encoding (typeIds fit comfortably:
  the largest enum is in the 30-40 range). Sanity check the field
  width before merging.
- **No charset path.** The handler must skip charset lookup entirely;
  passing a stray non-zero charset pos to `m_cmp` would dereference
  garbage. Validate in the new handler that charset is unused.
- **Validator must reject non-CTE-mode use.** The new opcode is
  CTE-filter-only because it skips schema-version checks. Outside
  CTE mode, dispatch table entry stays `nullptr` so the runtime
  guard fires (returns -ZNO_INSTRUCTION_ERROR).
- **AVG output type.** AVG produces Decimal in MySQL but RonSQL
  doesn't yet support AVG in CTE outputs (build_cte_virtual_tables
  rejects it). v1 of this phase doesn't change that — AVG stays
  unsupported.

## Verification

```bash
# Server + client builds
cd debug_build
make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd ronsql_cli rdrs2

# Block tests for the new opcode
make -j$(sysctl -n hw.ncpu) testCteNdbApiFilter testCteNdbApi
./runtime_output_directory/testCteNdbApiFilter -c localhost:1186 -m 3306 -v

# RonSQL MTR
cd mysql-test
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql                            # full regression
```

## Files touched

- `storage/ndb/include/kernel/Interpreter.hpp`
- `storage/ndb/src/kernel/blocks/dbtup/DbtupExecQuery.cpp`
- `storage/ndb/include/ndbapi/NdbInterpretedCode.hpp`
- `storage/ndb/src/ndbapi/NdbInterpretedCode.cpp`
- (validator location: TBD — Step 4 audit)
- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
- `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp`
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test`
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` (re-recorded)
