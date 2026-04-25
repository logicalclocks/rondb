# CTE Filter Phase D2 — MIN/MAX virt-type widening for inline-filter

## Context

Phase D (`cte_filter_phase_d.md`) shipped the inline-type opcode
`BRANCH_MEM_OP_ARG_INLINE_TYPE = 40` and a client-side
`branch_linked_inline_*` family. RonSQL dispatches `SUM` and `COUNT`
aggregate-output filters through this path — both produce 8-byte
`AggResItem.value: Uint64` results in the linked-attr buffer
(`Dblqh::buildCteLinkedBuffer` Step 3 / `cteLookupEmitResult` lines
19250–19271 — `AttributeHeader::init(..., attrId, 8); memcpy(..., 8)`,
unconditionally for every aggregate slot regardless of source type).

`MIN`/`MAX` are still rejected at `emit_cte_lookup_filter` with a clear
`require_prm` error:

```
"CTE_LOOKUP filter on aggregate output: only SUM and COUNT are
 supported.  MIN/MAX over numerics need widened virt-table type
 derivation; …"
```

Root cause: `build_cte_virtual_tables` (RonSQLPreparer.cpp:4992-4996)
sets `derived_type = source type` for `T_MIN`/`T_MAX`. For an `Int`
source the virt-table column claims 4 bytes — but the kernel writes 8.
The inline-type opcode trusts `columnSizeBytes` for the
length-walker and the per-row size check, so a 4-byte claim against an
8-byte payload is a hard mismatch.

## Goal

Lift the rejection so `WHERE max_col OP const` and `WHERE min_col OP
const` push down for numeric MIN/MAX. Non-numeric MIN/MAX (CHAR /
VARCHAR / DECIMAL) remain rejected — they need separate kernel-side
work because `AggResItem.value` is a `Uint64`, not a string buffer.

## Scope

In scope:
- `RonSQLPreparer::build_cte_virtual_tables` — widen `T_MIN` / `T_MAX`
  numeric source types to the 8-byte family, mirroring `T_SUM`
  exactly. Non-numeric source types (CHAR / VARCHAR / etc.) keep the
  current behaviour: preserve source type+length+charset (filter
  pushdown will still reject those — but the pass-through aggregator
  path that consumes MIN/MAX strings as the main query result is
  untouched).
- `RonSQLPreparer::emit_cte_lookup_filter` — accept `T_MIN` / `T_MAX`
  alongside `T_SUM` / `T_COUNT` when the (now widened) virt-column
  type is `Bigint` / `Bigunsigned` / `Double`. Reject non-widened
  MIN/MAX with a clear message so callers know it's the source-type
  shape, not an interpreter limitation.
- MTR: extend `ronsql_cte_basic.test` with Test 12 — `WHERE max_amt > N`
  on a CTE that emits `MAX(o_amt)`.
- Block test (optional): mirror Test 21 in
  `testCteNdbApiFilter.cpp` with a MAX aggregate to validate the
  encoding without going through RonSQL.

Out of scope:
- MIN/MAX over CHAR/VARCHAR/DECIMAL — these need DBLQH-side work
  on `AggResItem` so a wide value (or a pointer + length) can fit.
  Track separately if a use case appears.
- AVG / DECIMAL precision-scale encoding — already deferred to the
  AVG follow-up listed in `cte_filter_phase_d.md`.

## Safety check — does widening break existing tests?

Tests 1–10 in `ronsql_cte_basic.test` and Tests 1–22 in
`testCteNdbApiFilter` all consume MIN/MAX through:
1. The main aggregator, via `LoadLinkedColumn(pos, reg, vtcol)`.
   `LoadLinkedColumn` reads the buffer slot at `pos` and copies its
   payload bytes into the named register. The register width is
   determined by the runtime `Uint32 dataLen` from the buffer, not by
   the claimed virt-column type — confirmed by reading
   `JoinAggInterpreter::LoadLinkedColumn` impl in
   `AggInterpreter.cpp` (it walks linked buffer entries by header
   length, `data_len * 4`, not by an a-priori type assertion).
2. `ResultPrinter` for pass-through reads — but those go through the
   main aggregator's output schema, not the virt-table column type
   directly. Pass-through CTE shapes (Phase E) don't exist yet.

So widening the virt-column type from `Int` to `Bigint` for MAX(int_col)
changes the *claim* but not the bytes on the wire. Existing tests
should remain green. If a test fails at recording time it would be
because the CTE output column type is exposed somewhere RonSQL relies
on — flag that as a real find.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`:
  - `build_cte_virtual_tables` MIN/MAX branch (~line 4992): replace
    `derived_type = st` with the same widening switch as SUM, falling
    through to the original assignment for non-numeric source types.
  - `emit_cte_lookup_filter` aggregate-output check (~line 5179):
    accept T_MIN/T_MAX when `vtcol->getType()` is one of
    `Bigint`/`Bigunsigned`/`Double`; reject otherwise with a clear
    message that names the source-type shape.
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — add Test 12
  (MIN or MAX over Int with a numeric WHERE filter on the output).
- `mysql-test/suite/ronsql/r/ronsql_cte_basic.result` — re-record.
- `storage/ndb/block_unit_test/testCteNdbApiFilter.cpp` — optional
  Test 23 mirroring Test 21 with MAX in place of SUM.
- This phase doc.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2 \
                             testCteNdbApiFilter
cd debug_build/mysql-test && ./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql                    # full suite — no regressions
./mtr --suite=ndb_push_agg              # block test suite
```

After green, commit + push.
