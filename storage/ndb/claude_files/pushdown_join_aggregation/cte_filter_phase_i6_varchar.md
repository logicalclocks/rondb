# Phase I.6 finish — VARCHAR / CHAR / Longvarchar MIN/MAX

## Status

**F.2 + F.3 + S.1-S.6 hardening shipped for scalar / grouped scan-aggregation surfaces.**  Join-aggregation linked-attr strings and CTE delivery substitution remain gated as F.4.  Sub-phases F.2 + F.3 of the I.6 plan
(`cte_filter_minmax_strings_plan.md`).  F.1 (DECIMAL widening)
shipped in `354f2f811f0`.  This doc takes the F.2 / F.3 sketch from
that plan to actionable, file-anchored per-commit work.

**Shipped on `RONDB-1050-cte-filter`:**
| Commit | Phase | Scope |
|--------|-------|-------|
| `4ac5821af5b` | plan | this document |
| `c081f0bd1bf` | F.2-K.1+K.2+K.3 | sidecar storage + TypeSupported / AlignedType arms + extended StringResult shape |
| `fe2e3011595` | F.2-K.4 | per-(group, slot) val_ptr execution: m_register_string_data scratch, kOpLoadCol arms, minMaxString, kOpMax/kOpMin dispatch, group-eviction free, thread_id plumbing |
| `f3d1abe8711` | F.2-K.5a | AGG_CHAR_RESULT marker (0xFF02) + hasStringSlots / stringPayloadSize / encodeStringPayload helpers |
| `614d13081ed` | F.2-K.5b | 8 kernel emit sites branch on hasStringSlots — append per-group string-payload region |
| `8ae43d0e53f` | F.2-K.5c | NdbReceiver recognizes both markers; PA_CHECK validation walks AGG_CHAR_RESULT correctly |
| `6c652f50256` | F.2-K.5d-1 | NdbAggregator parses AGG_CHAR_RESULT (single-source single-node); resolveStringSlots / freeStringSlots; destructor cleanup |
| `879c76004a7` | F.2-K.5d-3 | `NdbAggregator::Result::data_str(Uint32* len)` API |

**S.1 - S.6 hardening sweep shipped on top of F.2 + F.3:**

| Commit | Phase | Scope |
|--------|-------|-------|
| `5e6d1aa4eab` | F.3-R.1 | RonSQL CTE virt-table type passthrough for string MIN/MAX |
| `4d3d0e6dcba` | F.3-R.2 | `emit_cte_lookup_filter` accepts string virt-types |
| `9efdc033e9c` | initial test path | Plan B: kernel optimizer no longer rewrites string `kOpMax`/`kOpMin` to BIGINT variants; AggInterpreter / JoinAggInterpreter / NdbAggregator API tolerant of string types; first single-partition `testVarcharMinMax` block test + MTR wrapper |
| `d96e01add1d` | S.1 grouped ownership | Grouped string slots resolved before insert/merge; `ResultRecord::result_records_.len` matches public AggResItem array length; ownership symmetric across paths |
| `47841e2ca5e` | S.2 multi-source merge | Real charset-aware string MIN/MAX merge on the API side via `NdbSqlUtil::getType(...).m_cmp`; deep-copy of winners into API-owned memory; replaced buffers freed; multi-partition test no longer single-partition pinned |
| `7f1ced100f2` | S.3 scratch guard | `m_attr_read_pos` bounds check before advancing; clean interpreter-level error on overflow |
| `81d1832a099` | S.4 builder validation | API rejects unsupported string operations (e.g. `SUM(string)`) at finalize with `kErrUnsupportedStringOperation` |
| `a8db55676b7` | S.5 RonSQL printing | RonSQL `ResultPrinter` renders string aggregate results via `Result::data_str()`; `ronsql_minmax_string.test` |
| `09e5a8b90cd` | S.6 coverage | CHAR padding, VARCHAR varying lengths, Longvarchar payloads, NULL handling, scalar + grouped surfaces, multi-partition merge in `testVarcharMinMax` and `ronsql_minmax_string` |

Detailed sub-phase plan with anchors and rationale: `cte_filter_phase_i6_string_minmax_fixups.md`.

**Open follow-ups (out of F.2 / F.3 scope, tracked under F.4):**

- **Join aggregation linked-attr strings** — `JoinAggInterpreter::kOpLoadCol` rejects linked-attr string columns (`attrDescriptor == nullptr` arm).  Charset and prefix bytes need to be encoded in the linked-attr stream before MIN/MAX over a CTE string output can feed a downstream aggregator.
- **CTE_LOOKUP / CTE_SCAN delivery substitution** — `Dblqh::cteLookupEmitResult` and the CTE feed paths `memcpy(..., 8)` per AggResItem; for string slots they ship `val_ptr` bits.  Needs per-slot type-aware substitution applied to the CTE delivery path (analogous to AGG_CHAR_RESULT for the API drain).
- **CTE materialisation chain end-to-end** — Once the two above are wired, `WITH cte AS (... MIN/MAX(string) ...) SELECT ... FROM cte JOIN ...` becomes testable end-to-end; today this hangs or fails with `ZCTE_LOOKUP_OUTPUT_OVERFLOW`.

These are tracked under the F.4 plan doc (`cte_filter_phase_i6_string_minmax_f4.md`, to be written when work starts).

## Scope decisions

Resolves the open questions from `cte_filter_minmax_strings_plan.md`:

1. **CHAR + VARCHAR + Longvarchar all in scope.**  All three differ
   only in payload size and length-prefix width (CHAR fixed-width,
   VARCHAR with 1-byte prefix, Longvarchar with 2-byte prefix);
   the kernel comparison kernel uses `cs->coll->strnncoll`
   regardless, and the pool allocator (decision 3) handles any
   payload size up to Longvarchar's 65535-byte ceiling.

2. **Both AggInterpreter and JoinAggInterpreter are in scope.**
   They are sister classes with parallel `TypeSupported`,
   `AlignedType`, `MinBigint` / `MaxBigint`, etc.  Anchors:
   - `AggInterpreter.cpp:372-396` (TypeSupported), `413-438`
     (AlignedType), `707-882` (Max/MaxBigint/MaxDouble), `884+`
     (Min/MinBigint/MinDouble).
   - `JoinAggInterpreter.cpp:88` (TypeSupported), `126`
     (AlignedType), `361` (MaxBigint), `469` (MinBigint),
     dispatch around `1472-1485`.

   Every kernel-side change in this plan applies to both classes.
   String state lives on each class as its own
   `m_string_results[]` array; `Min` / `Max` string kernels are
   added to both dispatchers; the `Sum`-over-string rejection
   appears in both compile guards.

3. **Storage: sidecar array `m_string_results[]`** indexed by
   aggregator slot.  Each entry is a small struct:

   ```cpp
   struct StringResult {
     char* ptr;       // from lc_ndbd_pool_malloc; nullptr = unset
     Uint16 length;   // current payload length (excludes prefix)
     Uint16 size;     // allocated capacity at *ptr (multiple of 16)
   };
   ```

   Three fields, naturally padded to 16 bytes on 64-bit.  `Uint16`
   covers Longvarchar's 65535-byte ceiling exactly.

   Do **not** extend `Register`
   (`NdbAggregationCommon.hpp:136-151`) — its layout is shared
   with the API client header and the 8-byte `DataValue` union has
   no room for both length and capacity without ABI churn.

4. **Allocator: `lc_ndbd_pool_malloc` / `lc_ndbd_pool_free`** from
   `storage/ndb/src/kernel/vm/ndbd_malloc.hpp:147-152`.  Pool id
   `RG_QUERY_MEMORY` (same as DBSPJ / DBTC CTE allocations —
   `Dbspj.hpp:653`, `DbtcMain.cpp:28494`).

   **`thread_id` is a per-call parameter — never cached on the
   instance.**  Both `AggInterpreter` and `JoinAggInterpreter` are
   long-lived objects whose update methods can be invoked from
   different kernel threads over their lifetime.  Caching the
   constructor-time thread id would cause `lc_ndbd_pool_malloc` to
   route through the wrong per-thread structure and trip the
   pool's mutexes.  See
   `feedback_lc_ndbd_pool_malloc_thread_id.md`.

   Concretely, every public method on AggInterpreter /
   JoinAggInterpreter that may transitively call
   `lc_ndbd_pool_malloc` accepts a `Uint32 thread_id` parameter
   and forwards it through to the allocator at the moment of
   allocation.  The string Min / Max kernels are the only such
   sites in this plan; their callers (the per-row update entry
   points) already know the running thread because they were
   invoked from a kernel-block signal handler.

   `lc_ndbd_pool_free(void* mem)` takes no thread id, so
   destructor / cleanup paths need no plumbing.

   Do **not** repurpose the existing `MemAlloc` / `m_mem_buf`
   bump allocator (`AggInterpreter.cpp:2145`).  MemAlloc resets
   per batch (`m_alloc_len = 0` at lines 293, 2009) and serves
   GB-key payloads; mixing string-result lifetimes with that
   reset cycle would force every-batch reallocation even when
   the string already fits.

5. **Resize-on-grow-only with 16-byte alignment.**

   - On a new winner whose `new_len ≤ entry.size`: copy into
     `entry.ptr` in place, update `entry.length`, no allocator
     traffic.
   - On a new winner whose `new_len > entry.size`: free the old
     allocation, allocate a new one large enough.
   - **Allocation size is rounded up to a multiple of 16 bytes.**
     The smallest legal allocation accommodates the wire-format
     length prefix even for empty payloads — 1 byte for VARCHAR,
     2 bytes for Longvarchar — rounded to 16, so 16 bytes
     minimum in practice.  CHAR has no prefix but declared
     widths < 16 still round up.

   Allocation helper:

   ```cpp
   static inline Uint32 roundup_alloc_size(Uint32 payload_len,
                                           Uint32 prefix_bytes) {
     Uint32 raw = payload_len + prefix_bytes;
     if (raw == 0) raw = prefix_bytes ? prefix_bytes : 1;
     return (raw + 15u) & ~15u;
   }
   ```

   The buffer holds the length prefix (when applicable) followed
   by payload — same layout as the wire format — so the F.2-K.5
   emit branches do a single memcpy of `prefix_bytes + length`
   bytes.  The struct's `length` field is the payload length only;
   `size` is the buffer capacity (rounded to 16).

6. **No batch-end free.**  String result buffers persist across
   batches.  Per-batch reset (existing code at
   `AggInterpreter.cpp:299` "Reset all aggregation results";
   matching site in `JoinAggInterpreter.cpp`) zeroes
   `entry.length` (marking "no winner yet for this batch") but
   keeps `entry.ptr` and `entry.size`.  The first winner of the
   next batch reuses the buffer if it fits.  This amortises pool
   traffic across batches when group keys / value lengths stay
   stable.

   Free happens only at instance destruction.  No post-free
   clearing of the struct fields:

   ```cpp
   void AggInterpreter::release_string_results() {
     for (Uint32 i = 0; i < m_n_agg_results; i++) {
       if (m_string_results[i].ptr != nullptr) {
         lc_ndbd_pool_free(m_string_results[i].ptr);
       }
     }
   }
   ```

   Called from each class's destructor.  The array itself goes
   away with the instance; no caller reads `m_string_results[i]`
   after destruction.

   NULL-vs-empty-string distinction during a batch:
   - Unset (no non-NULL row seen yet this batch): the per-slot
     `Register::is_null` flag is true.  `entry.length` may be
     stale from the previous batch; the per-slot reset clears it.
   - Empty VARCHAR seen as winner: `entry.length == 0`,
     `Register::is_null` is false.
   - First winner of a batch always populates the buffer (even
     for length 0, so the wire-emit path can use the same
     branch).

Out of scope:
- LIKE / pattern-match push-down on string MIN/MAX outputs (no
  inline opcode today).
- Charset coercion across mixed-collation comparisons — kernel
  matches the source column's charset; mixed-charset MIN/MAX is
  a parser-time error today.
- DECIMAL precision-preserving compare — F.1 is final.

## Current state in the tree

Anchors used throughout this plan:

- **AggInterpreter** —
  - `TypeSupported` at `AggInterpreter.cpp:372-396`.
  - `AlignedType` at `AggInterpreter.cpp:413-438`.
  - `MemAlloc` + buffer at `AggInterpreter.cpp:2145-2152` and
    `AggInterpreter.hpp:122-130` (stays unchanged; serves GB-key
    payloads only).
  - Min / Max dispatch at `AggInterpreter.cpp:707-882` and
    `884+`.
  - Per-batch reset at `AggInterpreter.cpp:283-310` and
    `2009-2010`.
- **JoinAggInterpreter** —
  - `TypeSupported` at `JoinAggInterpreter.cpp:88`.
  - `AlignedType` at `JoinAggInterpreter.cpp:126`.
  - `MaxBigint` at `JoinAggInterpreter.cpp:361`,
    `MinBigint` at `469`.
  - Min / Max dispatch at `1472-1485` and around `1724-1738`.
  - Compile sites that gate on `TypeSupported` at `1261, 1281,
    1398`.
- **Wire format emission** in `DblqhMain.cpp` at lines `18920`,
  `19303`, `19913`, `20118`, `20220`.  All five sites use
  `AttributeHeader::init(..., attrId, 8); memcpy(..., &item.value, 8)`.
- **Pool allocator** declared in
  `storage/ndb/src/kernel/vm/ndbd_malloc.hpp:147-152`.
- **NdbAggregator API** in
  `storage/ndb/include/ndbapi/NdbAggregator.hpp` — `LoadColumn`
  records column metadata.  Charset capture goes here (used by
  both interpreters at compile time).
- **RonSQL virt-table MIN/MAX branch** in
  `RonSQLPreparer.cpp:5293-5336` (`build_cte_virtual_tables`).
- **RonSQL inline-filter aggregate-output check** in
  `RonSQLPreparer.cpp:5713-5736` (`emit_cte_lookup_filter`).
- **Phase D inline opcode** family `branch_linked_inline_*` already
  supports a charset number (`csNumber`) and variable column size
  — F.3 needs no new opcode.

## F.2 — Kernel-side MIN / MAX over CHAR / VARCHAR / Longvarchar

Per decision 2, every commit below applies to both
`AggInterpreter` and `JoinAggInterpreter` unless otherwise noted.

### Commit F.2-K.1 — Sidecar string-result storage (both classes)

Add `StringResult` (decision 3) and `m_string_results[]` to both
`AggInterpreter` and `JoinAggInterpreter`.  Add the
`release_string_results()` helper (decision 6) and call it from
each class's destructor.

Per-batch reset paths zero `entry.length` only — slot into the
existing reset loop in each class (the same loop that already
resets per-slot accumulator state).

`thread_id` plumbing (per decision 4):
- Identify each public update entry point that may need to
  allocate string memory.  Add `Uint32 thread_id` as a parameter.
- Walk callers to confirm they have the running thread id
  available.  The kernel-block invocations of these update
  methods already run inside a signal-handler context that knows
  the thread.  Plumb the value through.
- Do not store thread_id as a member.

Files: `AggInterpreter.{hpp,cpp}`, `JoinAggInterpreter.{hpp,cpp}`.
Test: build only; no behavioural change.

### Commit F.2-K.2 — `TypeSupported` / `AlignedType` accept strings (both classes)

In **both** `AggInterpreter.cpp` and `JoinAggInterpreter.cpp`:

- Extend `TypeSupported` (AggInterpreter.cpp:372,
  JoinAggInterpreter.cpp:88) with `NDB_TYPE_CHAR`,
  `NDB_TYPE_VARCHAR`, `NDB_TYPE_LONGVARCHAR`.
- Extend `AlignedType` (AggInterpreter.cpp:413,
  JoinAggInterpreter.cpp:126) with String arms returning the
  source type unchanged (CHAR → CHAR, VARCHAR → VARCHAR,
  Longvarchar → Longvarchar).
- Add a `Sum`-over-string guard at compile sites in each class
  with a clear error message.  `Count` is type-agnostic — no
  change.

Files: `AggInterpreter.cpp`, `JoinAggInterpreter.cpp`.  Test:
existing `testJoinAgg` and `testJoinAggSpj` still pass.

### Commit F.2-K.3 — Charset threading (both classes)

The string Min / Max kernels need a `CHARSET_INFO*` for
collation comparison.  Both classes' compile paths resolve the
source column via the table descriptor and have
`column->getCharset()` available.  Capture and store on the
per-register accumulator metadata in each class.

Files: `AggInterpreter.cpp`, `JoinAggInterpreter.cpp`, possibly
`NdbAggregator.cpp` (if column metadata capture lives there
shared between both interpreters).  Test: build only; charset
captured but not yet consumed.

### Commit F.2-K.4 — Min / Max string comparison kernels (both classes)

Add `MinString` / `MaxString` arms to the dispatch in both
classes:

```cpp
// Pseudocode for AggInterpreter::MaxString.  MinString flips the
// comparison sign.  JoinAggInterpreter has the same shape.
void AggInterpreter::MaxString(Uint32 slot,
                                const char* in_payload,
                                Uint16 in_len,
                                const CHARSET_INFO* cs,
                                Uint32 prefix_bytes,
                                Uint32 thread_id /* per-call */) {
  StringResult& cur = m_string_results[slot];
  if (/* not first row of this batch */) {
    int cmp = cs->coll->strnncoll(cs,
        (const uchar*)in_payload, in_len,
        (const uchar*)cur.ptr + prefix_bytes, cur.length,
        /*nchar*/ 0);
    if (cmp <= 0) return;
    Uint32 needed = roundup_alloc_size(in_len, prefix_bytes);
    if (needed > cur.size) {
      lc_ndbd_pool_free(cur.ptr);
      cur.ptr  = (char*)lc_ndbd_pool_malloc(needed,
                                            RG_QUERY_MEMORY,
                                            thread_id,
                                            /*clear=*/false);
      cur.size = needed;
    }
  } else {
    // First non-NULL row of this batch.
    Uint32 needed = roundup_alloc_size(in_len, prefix_bytes);
    if (cur.size < needed) {
      if (cur.ptr != nullptr) lc_ndbd_pool_free(cur.ptr);
      cur.ptr  = (char*)lc_ndbd_pool_malloc(needed,
                                            RG_QUERY_MEMORY,
                                            thread_id,
                                            /*clear=*/false);
      cur.size = needed;
    }
  }
  // Allocation failure handling: record error on instance, fail
  // this row's update, continue with other slots.  Same pattern
  // existing kernels use for MemAlloc failures.
  write_length_prefix(cur.ptr, in_len, prefix_bytes);
  memcpy(cur.ptr + prefix_bytes, in_payload, in_len);
  cur.length = in_len;
}
```

Hot-path notes:
- One `strnncoll` call per non-first row.  Common case
  (`cmp <= 0`) returns without any pool traffic OR copy.
- After a few rows the slot stabilises at the longest seen size;
  subsequent winner copies are in-place.
- VARCHAR length comes from the row's 1-byte prefix; Longvarchar
  from the 2-byte little-endian prefix; CHAR is the column's
  declared size.
- NULL handling: the existing per-slot `Register::is_null` flag
  is the source of truth.  First-row sentinel reuses that flag.

Files: `AggInterpreter.{hpp,cpp}`, `JoinAggInterpreter.{hpp,cpp}`.
Test: new `MaxVarcharAscii`, `MaxVarcharGrowShrink` block tests
on both `testJoinAgg` and `testJoinAggSpj`.

### Commit F.2-K.5 — Wire format: emit length-prefixed payload

Branch the five emission sites in `DblqhMain.cpp`
(`18920`, `19303`, `19913`, `20118`, `20220`) on
`accumulators[i].type`:

```cpp
if (is_numeric_type(accumulators[i].type)) {
  AttributeHeader::init(&hdr, attrId, /*size*/ 8);
  memcpy(dst, &item.value, 8);
} else {
  // String path: cur.ptr already contains [length_prefix][payload]
  // in wire format.  Just emit prefix+payload bytes.
  const StringResult& sr = m_string_results[i];
  Uint16 plen = sr.length;
  Uint32 prefix = (accumulators[i].type == NDB_TYPE_VARCHAR)     ? 1
                : (accumulators[i].type == NDB_TYPE_LONGVARCHAR) ? 2
                : 0;
  Uint32 payload_size = (accumulators[i].type == NDB_TYPE_CHAR)
                        ? accumulators[i].declared_size
                        : prefix + plen;
  AttributeHeader::init(&hdr, attrId, payload_size);
  if (sr.ptr != nullptr) {
    memcpy(dst, sr.ptr, prefix + plen);
  }
  if (accumulators[i].type == NDB_TYPE_CHAR &&
      plen < payload_size) {
    memset(dst + plen, ' ', payload_size - plen);
  }
}
```

`accumulators[i].declared_size` flow: source column is identified
by attrId; look it up at emit time via the table descriptor (no
new field on the per-slot struct needed).

Receiver side (`CteLinkedAttr` consumer) already supports
variable `dataSize` via `AttributeHeader`; no NDB-API change.

Files: `DblqhMain.cpp`.  Test: `MaxVarcharAscii` end-to-end
delivers correct payload through both AggInterpreter and
JoinAggInterpreter wire paths.

## F.3 — RonSQL CTE filter on string MIN/MAX outputs

Two RonSQL changes; both small once F.2 is in.

### Commit F.3-R.1 — virt-table type passthrough for string MIN/MAX

In `build_cte_virtual_tables` MIN/MAX branch at
`RonSQLPreparer.cpp:5293-5336`, drop the implicit "preserve
source type but filter rejects" carve-out for CHAR / VARCHAR /
Longvarchar.  Source-type preservation now corresponds to a
real wire format.

Files: `RonSQLPreparer.cpp`.  Test: build only.

### Commit F.3-R.2 — accept string virt-types in `emit_cte_lookup_filter`

In the aggregate-output branch at
`RonSQLPreparer.cpp:5713-5736`, extend the accepted virt-column
type set with `Char`, `Varchar`, and `Longvarchar`:

```cpp
case NdbDictionary::Column::Char:
case NdbDictionary::Column::Varchar:
case NdbDictionary::Column::Longvarchar:
  inline_typeId = (Uint32)vtcol->getType();
  inline_columnSize = (Uint32)vtcol->getLength();
  inline_csNumber = vtcol->getCharset()
                    ? vtcol->getCharset()->number : 0;
  break;
```

Phase D's `branch_linked_inline_*` opcode family already accepts
charset and variable column size — no new opcode needed.

`encode_constant` already handles string literals against a string
column descriptor; verify a literal-vs-VARCHAR(N) compare with
collation but no code change expected.

Files: `RonSQLPreparer.cpp`.  Test: new `ronsql_cte_varchar.test`.

## Test plan

### Kernel block tests (F.2)

Add to `storage/ndb/block_unit_test/testJoinAgg.cpp` (covers
AggInterpreter) **and** `testJoinAggSpj.cpp` (covers
JoinAggInterpreter):

| Test | Shape | Why |
|------|-------|-----|
| MaxVarcharAscii | `MAX(varchar_col)` ASCII, GROUP BY numeric | basic happy path |
| MaxVarcharUtf8 | Same with utf8mb4 collation | charset threading |
| MaxCharFixed | `MAX(char_col)` over `CHAR(8)` | fixed-length variant |
| MaxLongvarchar | `MAX(longvarchar_col)` | 2-byte length prefix |
| MinMaxNullOnly | All-NULL string group | NULL semantics round-trip |
| EmptyVsNull | Mix of `''` and `NULL` rows | empty-vs-NULL distinction |
| MaxVarcharLong | `MAX(varchar(255))` × 5000 groups | exercises pool alloc |
| MaxVarcharGrowShrink | Lengths grow then shrink — verifies size reuse + 16-byte rounding |
| MaxVarcharAcrossBatches | Multi-batch run — verifies buffer persistence across batches |
| MaxVarcharCrossThread | Update from one thread, then from another — verifies dynamic thread_id plumbing (no mutex issue) |

### MTR tests (F.3)

`mysql-test/suite/ronsql/t/ronsql_cte_varchar.test` (new):

| Test | SQL shape |
|------|-----------|
| 1 | `WITH cte AS (SELECT g, MAX(name) AS m FROM t GROUP BY g) SELECT * FROM cte` |
| 2 | Same with WHERE on `m` (`WHERE m = 'Charlie'`) — F.3 inline filter |
| 3 | Same with WHERE on `m` (`WHERE m > 'A'`) — collation-aware compare |
| 4 | Chained CTE: `MIN(MAX(name))` — verifies type passthrough through `resolve_chained_column_type` |
| 5 | utf8mb4 collation source column |
| 6 | CHAR(8) source column (fixed-length) |
| 7 | Longvarchar source column |
| 8 | All-NULL group returns NULL |
| 9 | Empty-string vs NULL distinction |

## Risks

1. **Pool allocator pressure.**  `lc_ndbd_pool_malloc` competes
   with other RG_QUERY_MEMORY allocations.  Without booking,
   concurrent queries could starve the aggregator.  Decision:
   ship without booking; if benchmarks show pool pressure, add
   `lc_ndbd_pool_book` based on `expected_groups × max_string_size`
   at compile time.
2. **`strnncoll` hot-path cost.**  Per-row collation compare is
   heavier than `int64` compare.  Acceptable for MIN/MAX but
   benchmark inside `testJoinAgg` before claiming "no regression".
3. **CHAR padding semantics.**  CHAR columns are space-padded to
   declared width on emit.  Verify `strnncoll` length-aware
   comparison matches mysql for `PAD SPACE` vs `NO PAD`
   collations.  ASCII tests pass first; utf8mb4 breaks first.
4. **Cross-batch buffer persistence.**  If `m_n_agg_results` or
   the aggregator program changes between batches (it shouldn't
   on a stable program but verify), persistent buffers could
   become misaligned with the new slot meaning.  Add an assertion
   that compile produces the same slot layout for the same
   program.
5. **Thread-id plumbing — full audit needed.**  Every public
   method on AggInterpreter / JoinAggInterpreter that may
   transitively call `lc_ndbd_pool_malloc` must accept a
   `Uint32 thread_id` parameter.  F.2-K.1 adds the field on the
   sidecar but the actual allocations happen in F.2-K.4.  Run a
   call-site audit between commits to make sure no path reaches
   the allocator with a stale or constant thread_id.
6. **CteLinkedAttr inline encoding for strings.**  Phase D's
   inline-type opcode was tested only with fixed-size payloads.
   Test the receiver path with real string payloads before
   declaring the API uniform.

## Implementation checklist

Order: F.2-K.1 through F.2-K.5 (kernel side, both classes), then
F.3-R.1 + F.3-R.2 (RonSQL side).

1. **F.2-K.1** — sidecar `m_string_results[]` on both classes,
   destructor cleanup, `thread_id` parameter added to relevant
   public methods.  Files: `AggInterpreter.{hpp,cpp}`,
   `JoinAggInterpreter.{hpp,cpp}`.  Test: build only.
2. **F.2-K.2** — `TypeSupported` / `AlignedType` accept strings;
   `Sum`-over-string rejection.  Files: same.  Test: existing
   `testJoinAgg` and `testJoinAggSpj` still pass.
3. **F.2-K.3** — charset threading on both classes.  Files:
   same plus possibly `NdbAggregator.cpp`.  Test: build only.
4. **F.2-K.4** — `MinString` / `MaxString` kernels on both classes
   with the resize-on-grow + 16-byte rounding pattern.  Files:
   same.  Test: `MaxVarcharAscii`, `MaxVarcharGrowShrink`.
5. **F.2-K.5** — wire-format string emit at the five DblqhMain
   sites.  Files: `DblqhMain.cpp`.  Test: end-to-end payload
   delivery.
6. Add the remaining block tests (utf8mb4, CHAR fixed,
   Longvarchar, NULL, EmptyVsNull, scale, cross-batch,
   cross-thread) on **both** `testJoinAgg` and `testJoinAggSpj`.
   Run the full block-test suite; commit when green.
7. **F.3-R.1** — virt-table type passthrough.  Files:
   `RonSQLPreparer.cpp`.  Test: build only.
8. **F.3-R.2** — filter-emit string acceptance.  Files: same.
   Test: new `ronsql_cte_varchar.test`.
9. Run broader regression set:
   ```bash
   ./mtr --suite=ronsql
   ./mtr --suite=ndb_push_agg
   ```
10. Update catalogue (`cte_filter_phase_i.md`) — flip I.6 status
    to "F.1 + F.2 + F.3 shipped".  Update CLAUDE.md and
    `cte_filter_minmax_strings_plan.md`.

## Estimated diff

- Kernel: ~400 LOC across `AggInterpreter.{hpp,cpp}`,
  `JoinAggInterpreter.{hpp,cpp}` (twin changes), and
  `DblqhMain.cpp`.
- RonSQL: ~30 LOC across `RonSQLPreparer.cpp`.
- Tests: ~600 LOC of new block tests (twin-side coverage) + MTR.

Total roughly 1000 LOC across 5–7 commits.
