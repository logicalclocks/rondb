# CTE filter — MIN/MAX over CHAR / VARCHAR / DECIMAL

## Status

**Historical umbrella plan.**  F.1 shipped first as the cheap
RonSQL-only DECIMAL widening.  F.2 / F.3 / S.1-S.6 have since been
developed in the dedicated Phase I.6 string MIN/MAX plans:

- `cte_filter_phase_i6_varchar.md`
- `cte_filter_phase_i6_string_minmax_fixups.md`
- `cte_filter_phase_i6_string_minmax_f4.md`

Use those newer files as the source of truth for current work.
This document is kept as background context and for the original
DECIMAL/string split.  The remaining VARCHAR / Longvarchar CTE
delivery issues are tracked in F.4.

| Commit | Scope |
|--------|-------|
| `354f2f811f0` | F.1 implementation: `resolve_chained_column_type` gains an `Int32& out_scale` parameter; T_MIN/T_MAX branch widens DECIMAL → BIGINT (scale==0) / Bigunsigned / DOUBLE (scale>0) mirroring the kernel's `AlignedType`.  `build_cte_virtual_tables` MIN/MAX branch applies the same widening so the virt-table column type matches the kernel's wire format and the inline-type CTE filter opcode accepts DECIMAL outputs |
| `82678211aff` + `7889daac005` + `d812019e9a4` + `974931810fb` | MTR — new `ronsql_cte_decimal.test` covers DECIMAL(N,0) MIN/MAX → BIGINT, DECIMAL UNSIGNED → Bigunsigned, DECIMAL(N,2) → DOUBLE, WHERE filter on the widened output (inline-type pushdown), and chained-CTE MIN(MAX(decimal)) walking through `resolve_chained_column_type` recursion |
| later Phase I.6 commits | F.2 / F.3 and S.1-S.6 implemented kernel string MIN/MAX support, RonSQL accept/render paths, and the first VARCHAR / Longvarchar hardening batches.  See the dedicated Phase I.6 plan documents above for exact scope and remaining work |

## Context

Phase D shipped the `BRANCH_MEM_OP_ARG_INLINE_TYPE` opcode and the
client-side `branch_linked_inline_*` family that pushes WHERE filters
on aggregate-output columns into DBLQH's per-CTE linked-attr buffer.
Phase D2 widened MIN/MAX virt-table types so numeric MIN/MAX work
through the same opcode.  The remaining gaps are MIN/MAX over:

1. **DECIMAL** — `RonSQLPreparer::emit_cte_lookup_filter` rejects
   today.  The kernel *already aggregates* DECIMAL via
   `AggInterpreter::AlignedType` (DECIMAL → BIGINT for scale==0,
   DOUBLE otherwise), so kernel-side support exists — RonSQL just
   needs to widen the virt-table column type the same way.
2. **CHAR / VARCHAR** — the kernel does NOT aggregate strings at all.
   `AggInterpreter::TypeSupported` rejects `NDB_TYPE_CHAR` and
   `NDB_TYPE_VARCHAR`, and `AggResItem`'s 8-byte value union has no
   place to put variable-length string payloads.  The CTE-filter
   rejection is the most user-visible symptom but isn't the root
   gap.

The earlier pointer in project memory (`project_cte_branch_state.md`)
characterised the whole feature as "would need wider `AggResItem`
encoding".  After investigation, that's only true for CHAR/VARCHAR;
DECIMAL is mostly a RonSQL-side fix.  This plan splits accordingly so
the cheap part doesn't block on the expensive part.

## Reference: where the code lives today

- **Rejection sites (RonSQL):**
  - `RonSQLPreparer.cpp:5713-5736` — `emit_cte_lookup_filter`
    aggregate-output branch.  Today's check accepts only Bigint /
    Bigunsigned / Double for MIN/MAX virt-column types; DECIMAL
    sources keep their original type and trip the require_prm.
- **Virt-table type derivation (RonSQL):**
  - `RonSQLPreparer.cpp:5293-5336` (`build_cte_virtual_tables`,
    MIN/MAX branch) — current behaviour: numeric sources widen to
    Bigint/Bigunsigned/Double; non-numeric sources preserve source
    type+length+charset.
- **Aggregator support matrix (kernel):**
  - `AggInterpreter.cpp:370-394` (`TypeSupported`) — accepted source
    types.  Includes DECIMAL.  Excludes CHAR / VARCHAR / VARBINARY.
  - `AggInterpreter.cpp:411-436` (`AlignedType`) — DECIMAL widening
    rule (scale==0 → BIGINT, else DOUBLE).
  - `AggInterpreter.cpp:707-882` (`Max`, `MaxBigint`, `MaxDouble`)
    and 884+ (`Min`, `MinBigint`, `MinDouble`) — only BIGINT / DOUBLE
    branches; DECIMAL goes through one of these after AlignedType.
- **Wire format (kernel):**
  - `DblqhMain.cpp:18920-19033` (`buildCteLinkedBuffer`) — Step 3
    emits 8-byte aggregate slots unconditionally:
    `AttributeHeader::init(..., attrId, 8); memcpy(..., &item.value, 8)`.
  - Same pattern at `DblqhMain.cpp:19303` (`cteLookupEmitResult`),
    19913 (`cteLookupAggFeed` linked buffer), 20118 / 20220
    (`cteScanEmitResults`).
- **AggResItem definition:**
  - `NdbAggregationCommon.hpp:136-151` — `Register` struct with
    `DataValue` union (`Int64 / Uint64 / double / void* val_ptr`),
    `DataType type`, `bool is_unsigned`, `bool is_null`.  The
    `val_ptr` slot exists but is unused on the kernel side today.
  - `AggInterpreter.hpp:128-130` — chunk allocator `m_mem_buf` of
    `MAX_AGG_RESULT_BATCH_BYTES` (8 KB); `MemAlloc` is the bump
    allocator.  Currently only used for GB key payloads, not agg
    results.

## Sub-phase F.1 — DECIMAL

**Scope: RonSQL only.  ~30 LOC.  Independent of F.2 / F.3.**

### Approach

Mirror `AlignedType`'s widening rule in
`build_cte_virtual_tables`'s MIN/MAX branch (around line 5304):
DECIMAL with scale==0 → Bigint (or Bigunsigned if source is
DECIMALUNSIGNED), DECIMAL with scale>0 → Double.  After widening,
the existing CTE-filter inline path accepts the column unchanged.

```cpp
// Inside the T_MIN / T_MAX branch (build_cte_virtual_tables):
case NdbDictionary::Column::Decimal:
case NdbDictionary::Column::Decimalunsigned: {
  Uint32 scale = src_scale;  // src->getScale() — needs threading
  bool is_unsigned = (st == NdbDictionary::Column::Decimalunsigned);
  if (scale == 0) {
    derived_type = is_unsigned ? Bigunsigned : Bigint;
  } else {
    derived_type = NdbDictionary::Column::Double;
  }
  derived_length = 1;
  have_derived = true;
  break;
}
```

Two wrinkles:
1. `resolve_chained_column_type` only returns (type, length, cs); it
   doesn't return scale.  Threading scale through the resolution
   path is one extra return value, mirrored in the chained-CTE walk.
2. Result type is reported to the caller as Bigint / Double — the
   user no longer sees a DECIMAL-typed CTE output column.  Same
   semantics as the kernel's actual aggregator output, so this
   isn't a behavioural regression — it's surfacing what was already
   true.  Document in the rejection error message that DECIMAL
   MIN/MAX returns widened types.

### Tests

- `ronsql_cte_basic.test` Test 13 (new): `MIN(decimal_col)` and
  `MAX(decimal_col)` with both scale==0 and scale>0 sources, with
  WHERE filter on the output.
- Verify result types match what the kernel produced before
  (BIGINT / DOUBLE).

### Files

- `storage/ndb/src/ronsql/RonSQLPreparer.cpp`
  - `resolve_chained_column_type` — add `Int32& out_scale` out param,
    mirror through chained walks.
  - `build_cte_virtual_tables` — DECIMAL widening branch in MIN/MAX.
- `storage/ndb/claude_files/pushdown_join_aggregation/cte_filter_phase_F1.md`
  — Phase F.1 doc (split out from this plan when ready to ship).
- `mysql-test/suite/ronsql/t/ronsql_cte_basic.test` — Test 13.

### Out of scope for F.1

- Preserving DECIMAL semantics (precision/scale) all the way to the
  client.  Would require either DECIMAL-typed virt columns (rejected
  by inline opcode today) or a separate result-rewriting pass.  The
  aggregator's BIGINT/DOUBLE widening is already lossy for high-scale
  DECIMAL inputs, so users shouldn't depend on full precision today
  anyway.

## Sub-phase F.2 — kernel-side MIN/MAX over CHAR / VARCHAR

**Status: superseded by dedicated Phase I.6 string MIN/MAX plans.**
The original outline below is historical and does not reflect all
fixups already shipped in S.1-S.6.

### What's needed

The kernel's aggregator must accept and compare string-typed columns
before any RonSQL filter can push down.  Three layers of changes:

1. **`AggInterpreter::TypeSupported`** — accept `NDB_TYPE_CHAR`,
   `NDB_TYPE_VARCHAR`, `NDB_TYPE_LONGVARCHAR`.  `Sum` stays
   unsupported (errors at compile time).  `Min / Max` must accept.
   `Count` is type-agnostic and already works.
2. **`AggResItem` storage for strings.**  The `DataValue.val_ptr`
   slot is the natural fit; backing store comes from `MemAlloc`
   (existing 8 KB chunk allocator).  Add a sibling field for the
   payload length, or repurpose `is_unsigned` (currently meaningless
   for strings — but cleaner to add `Uint16 length`).  Watch out
   for the kernel's `Register` being shared with the API client
   header — changing layout is an ABI break.  Cleanest: introduce
   a parallel `m_agg_results_strings` array indexed by agg slot,
   keep `Register` unchanged.
3. **`Min` / `Max` for strings.**  Use `cs->cset->strnncoll` (charset
   collation primitive) to compare incoming Register payload against
   stored result; on update, copy via `MemAlloc` + memcpy.  Need to
   thread the charset descriptor (`CHARSET_INFO*`) into the
   aggregator at program-build time — currently the agg's
   `LoadColumn` records column metadata, so the path exists.
4. **8 KB MemAlloc budget** — for large per-group string MIN/MAX
   over many groups, 8 KB across the whole batch is tight.  Two
   options:
   - Bump `MAX_AGG_RESULT_BATCH_BYTES` (page-resident, fixed cost
     per LDM thread — quantify before changing).
   - Detect overflow → eviction, just like `setMaxGroups`.
   F.2 picks one and documents it; eviction is the more general
   answer but heavier to implement.

### Wire format change (kernel → API)

The 8-byte `memcpy(..., &item.value, 8)` calls in
`buildCteLinkedBuffer` / `cteLookupEmitResult` /
`cteScanEmitResults` need a string variant: emit length-prefixed
payload for string-typed slots.  CteLinkedAttr already has typeId
in word0 and supports variable `dataSize` via the `AttributeHeader`
that follows — so the *receiver* side is uniform; only the emitter
needs to branch on `accumulators[i].type` for string types.

### Out of scope for F.2

- The CTE-filter inline opcode push-down (that's F.3).  After F.2
  ships, MIN/MAX over CHAR/VARCHAR works as a CTE *output* — RonSQL
  delivers the string result through the normal aggregator path —
  but `WHERE max_str_col > 'foo'` still rejects.

### Tests

- `testJoinAgg` C++ block tests with MIN/MAX over CHAR(N) and
  VARCHAR(N), both ASCII and non-ASCII charsets.
- MTR tests for the user-facing path: `SELECT MIN(name) FROM ...`
  with GROUP BY on a numeric column.

### Files

- `storage/ndb/src/kernel/blocks/dbtup/AggInterpreter.{hpp,cpp}` —
  TypeSupported, Min, Max, MemAlloc lifecycle.
- `storage/ndb/include/ndbapi/NdbAggregationCommon.hpp` — possibly
  no change if we use `val_ptr`; otherwise extend `Register`.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` — emit-side
  branches in the four 8-byte memcpy sites.
- `storage/ndb/include/ndbapi/NdbAggregator.hpp` — public API may
  need a charset parameter on `LoadColumn` for strings.
- `storage/ndb/block_unit_test/testJoinAgg.cpp` — string MIN/MAX
  coverage.

## Sub-phase F.3 — RonSQL CTE filter on string MIN/MAX outputs

**Status: superseded by dedicated Phase I.6 string MIN/MAX plans.**
The current remaining work is not the RonSQL accept-list itself, but
the F.4 kernel delivery paths for string aggregate payloads through
CTE materialisation, linked attrs, and redistribution.

### Approach

Once F.2 ships, the CTE virt-table column type for MIN/MAX over
CHAR/VARCHAR can stay as the source type (no widening — kernel emits
string payload natively).  The inline opcode encoding already
supports a charset number (`csNumber` slot in word1) and a variable
column size — Phase D's `branch_linked_inline_*` family was designed
for any encodable type.

Changes:
1. **`build_cte_virtual_tables` MIN/MAX branch** — drop the "preserve
   source type, but filter pushdown still rejects" comment for
   CHAR/VARCHAR.  Source-type preservation now corresponds to a
   real wire format.
2. **`emit_cte_lookup_filter` aggregate-output check** — accept
   string virt-column types (Char, Varchar, Longvarchar) for MIN/MAX
   alongside the numeric set.  Encode `inline_typeId` as the source
   type, `inline_columnSize` as `vtcol->getLength()`, `inline_csNumber`
   from the column's charset.
3. **`encode_constant`** — already handles string literals against a
   string column descriptor; no change expected, but verify on a
   STRING vs CHAR(N) compare with collation.

### Tests

- `ronsql_cte_basic.test` Tests 14, 15: WHERE `max_name = 'Charlie'`,
  WHERE `min_name LIKE 'A%'` (LIKE may need separate work — check
  whether the inline opcode supports it; if not, that's a follow-up).
- ASCII + utf8mb4 collation cases.

### Out of scope for F.3

- LIKE / pattern matching pushdown — separate inline opcode if
  needed; today's set is comparison only.
- DECIMAL precision-preserving compare (still Bigint/Double after F.1).

## Recommended order if proceeding

1. **F.1 first.**  Tiny, decouples DECIMAL from the bigger work,
   gives users an immediate win when they hit the rejection.
2. **F.2 only if a concrete use case appears.**  Real kernel work,
   touches the aggregator's hot path (Min/Max called per row),
   memory budget questions, ABI considerations.  Don't speculate.
3. **F.3 follows F.2.**  Mostly mechanical RonSQL once F.2 is real.

## Open questions

- **DECIMAL through CTE chain:** does
  `resolve_chained_column_type` correctly walk MIN(MIN(decimal_col))
  through two layers of widening?  Should fall out of mirroring
  `AlignedType` exactly, but verify with a chained-CTE test.
- **F.2 memory budget:** how many groups × max string length does
  a typical query produce?  8 KB MemAlloc is OK for 200 groups ×
  40 chars but fails for 1000 × 40 — eviction vs.  bump must be
  data-driven.
- **F.2 NULL semantics:** MIN/MAX over an all-NULL string group
  must return NULL.  AggResItem already has `is_null`; verify that
  the `val_ptr=nullptr, is_null=true` shape round-trips through
  the new wire format.

## Verification — when F.1 ships

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ronsql_cli rdrs2
./mtr --record --suite=ronsql ronsql_cte_basic
./mtr --suite=ronsql                    # no regressions
```

F.2 / F.3 verification matrices to be defined in their own per-phase
docs when sized for real.
