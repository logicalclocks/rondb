# Phase I.6 string MIN/MAX F.4 — CTE delivery + linked-attr strings

## Status

**Planned.**  Builds on F.2 + F.3 + S.1-S.6 hardening (see
`cte_filter_phase_i6_varchar.md` and
`cte_filter_phase_i6_string_minmax_fixups.md`).  Without F.4, CTE
materialisation paths cannot consume string MIN/MAX outputs:

- `JoinAggInterpreter::kOpLoadCol` linked-attr arm rejects strings
  (`attrDescriptor == nullptr` branch returns
  `ZAGG_LOAD_COL_WRONG_TYPE`).  Anything that re-aggregates a CTE
  string output, or feeds it through a downstream WHERE filter,
  hits this rejection.
- `Dblqh::buildCteLinkedBuffer` and `Dblqh::emitCteGroupOutput`
  both ship `accumulators[i].value` as 8 raw bytes per aggregate
  slot.  For string slots that's `val_ptr` — meaningless on any
  consumer.
- Several scalar / legacy CTE_SCAN and CTE aggregation-feed paths
  still write `AttributeHeader(..., 8); memcpy(..., &item.value, 8)`
  directly instead of going through the shared helpers.
- CTE redistribution (`JOIN_AGG_REDISTRIBUTE_REQ`) ships raw
  `AggResItem[]` arrays across nodes.  For string slots this also
  ships `val_ptr` bits unless F.4 adds substitution / decode or a
  deliberate prepare-time/runtime rejection.

Symptom: queries of the form
`WITH cte AS (... MIN/MAX(string) ...) SELECT ... FROM cte JOIN ...`
hang behind retried `ZCTE_LOOKUP_OUTPUT_OVERFLOW` errors today.

## Goal

Wire string MIN/MAX results through every CTE consumer surface that
already works for numeric MIN/MAX:

- CTE_LOOKUP and CTE_SCAN row delivery to the API / DBSPJ.
- CTE_LOOKUP-fed downstream `JoinAggInterpreter` (re-aggregation of
  a CTE string output by the main query).
- CTE_LOOKUP-fed downstream filter (`branch_linked_inline_*` over a
  CTE string virtual column — F.3-R.2's accept-list already permits
  this; F.4 makes it actually work).

No wire-format change is needed — `CteLinkedAttr::encodeWord0`
already reserves 16 bits for `maxBytes` and 16 bits for `csNumber`.
The kernel just needs to:

1. Substitute `accumulators[i].value.val_ptr` payload bytes for the
   raw 8 bytes when emitting string slots.
2. Decode that payload on the receiver side (linked-attr load) and
   feed it into the existing `m_register_string_data[reg]` scratch
   slot the F.2-K.4b local-table arm uses.
3. Route every CTE output / feed / redistribution writer through a
   common string-aware helper, or explicitly reject string slots on
   paths that remain unsupported.

## Sub-phase outline

### F.4-K.1 — Substitute string payload at `buildCteLinkedBuffer`

`Dblqh::buildCteLinkedBuffer` (`DblqhMain.cpp:19097-19116`) writes
each aggregate slot as:

```cpp
outBuf[linkedPos++] = CteLinkedAttr::encodeWord0(typeId, 8);
outBuf[linkedPos++] = CteLinkedAttr::encodeWord1(0);
if (accumulators[i].is_null) {
  AttributeHeader::init(&outBuf[linkedPos], attrId, 0);
  linkedPos += 1;
} else {
  AttributeHeader::init(&outBuf[linkedPos], attrId, 8);
  memcpy(&outBuf[linkedPos + 1], &accumulators[i].value, 8);
  linkedPos += 3;
}
```

For `accumulators[i].type ∈ { NDB_TYPE_CHAR, NDB_TYPE_VARCHAR,
NDB_TYPE_LONGVARCHAR }` and non-null, replace the 8-byte memcpy
with:

- Decode val_ptr buffer header `[Uint16 payload_len][Uint16
  capacity]`; the bytes at `buf + 4` are `[prefix + payload]`.
- Compute `byte_size = prefix_bytes + payload_len`; `prefix_bytes`
  derived from type (0 / 1 / 2).
- Emit `encodeWord0(type, maxBytes)` and
  `encodeWord1(slot.charset->number)` — `slot.charset` and
  `slot.declared_size` are on `interp->m_string_results[i]` so the
  helper already has access.  `maxBytes` must be the virtual
  column's declared/max byte size, not the current row's runtime
  `byte_size`; the following `AttributeHeader` carries the runtime
  byte size.
- `AttributeHeader::init(&outBuf[linkedPos], attrId, byte_size)`.
- `memcpy(&outBuf[linkedPos + 1], buf + 4, byte_size)`.
- Advance `linkedPos` by `1 + ((byte_size + 3) >> 2)` words.

NULL string slots stay at `attrSize == 0`, like NULL numerics.

The `interp` parameter already carries the `JoinAggInterpreter`
reference, so the helper can reach `interp->m_string_results[i]`
directly via a public accessor (or a new `slot_charset(i)` /
`slot_prefix_bytes(i)` getter).

Implementation shape:

- Add a small helper in `DblqhMain.cpp` that emits one aggregate slot
  into a CTE linked-attr buffer:
  ```cpp
  bool emitCteLinkedAggSlot(const JoinAggInterpreter* interp,
                            const AggResItem& item,
                            Uint32 aggIdx,
                            Uint32 attrId,
                            Uint32* outBuf,
                            Uint32& linkedPos,
                            Uint32 maxWords);
  ```
- Numeric slots keep the existing `maxBytes=8`, `csNumber=0`,
  `attrSize=8` behaviour.
- String slots use metadata accessors on `JoinAggInterpreter`
  (`string_slot_info(i)` or equivalent) to obtain prefix bytes,
  declared/max bytes, and charset.

Files: `DblqhMain.cpp`, `JoinAggInterpreter.hpp` (new accessors).

### F.4-K.2 — Substitute string payload at `emitCteGroupOutput`

`Dblqh::emitCteGroupOutput` (`DblqhMain.cpp:19378-19400`) writes
each aggregate column to the API/DBSPJ TRANSID_AI as:

```cpp
AttributeHeader::init(&outBuf[outPos], attrId, 8);
memcpy(&outBuf[outPos + 1], &item.value, 8);
outPos += 3;
```

Same substitution as F.4-K.1 — for string slots, decode val_ptr
buffer, set `attrSize = byte_size`, copy payload bytes, advance by
`1 + ((byte_size + 3) >> 2)`.

This call site has three users (one for each `emitCteGroupOutput`
caller in DblqhMain.cpp:19457, 20233, 20349) — the fix lives in the
shared helper, so all three benefit.

Files: `DblqhMain.cpp`.

### F.4-K.2b — Replace remaining fixed-8-byte CTE emit paths

After K.1 and K.2, remove or guard every other direct CTE writer
that still assumes aggregate results are 8 bytes:

- scalar CTE_SCAN aggregation feed builds linked attrs directly in
  `DblqhMain.cpp` around the `n_gb_cols()==0` agg-feed path;
- CTE_SCAN legacy/no-AttrInfo grouped output path writes raw
  aggregate columns;
- CTE_SCAN legacy/no-AttrInfo scalar output path writes raw
  aggregate columns.

Preferred fix: route each path through the same helpers introduced
by K.1/K.2 so scalar/grouped and AttrInfo/no-AttrInfo paths behave
identically.  If any legacy path cannot be converted cleanly, add an
explicit string-slot rejection before it can emit `val_ptr` bytes.

Files: `DblqhMain.cpp`.

### F.4-K.3 — Accept linked-attr strings in `JoinAggInterpreter::kOpLoadCol`

`JoinAggInterpreter::kOpLoadCol` (`JoinAggInterpreter.cpp` ~line
1411) currently has:

```cpp
if (attrDescriptor == nullptr) {
  return ZAGG_LOAD_COL_WRONG_TYPE;
}
```

inside the CHAR / VARCHAR / Longvarchar arm — i.e. the linked-attr
path is rejected for strings.  Replace with the linked-attr decoder
pattern already used by `JoinAggInterpreter::initGBTypes` (lines
2131-2133, 2246-2248):

- The two header words BEFORE the `AttributeHeader` currently get
  skipped by the linked-attr buffer walker (`p += 2`) before
  `kOpLoadCol` copies the `AttributeHeader` and payload.  K.3 must
  save `word0` and `word1` before that skip and thread them into the
  string arm so the code can decode type metadata and charset:
  ```cpp
  Uint32 word0 = p[0];
  Uint32 word1 = p[1];
  Uint32 maxBytes = CteLinkedAttr::decodeMaxBytes(word0);
  Uint32 csNumber = CteLinkedAttr::decodeCsNumber(word1);
  const CHARSET_INFO* cs = (csNumber > 0 && csNumber < NDB_ARRAY_SIZE(all_charsets))
      ? all_charsets[csNumber] : nullptr;
  ```
- `prefix_bytes` derived from `type` (0 / 1 / 2) as in the local
  arm.
- Read `payload_len` from the `[prefix]` byte(s) at the start of
  the data segment for VARCHAR / Longvarchar.  For linked CHAR
  there is no `attrDescriptor`, so use the `AttributeHeader`
  runtime byte size (and the decoded `maxBytes` as declared size)
  instead of `AttributeDescriptor::getSizeInBytes`.
- Populate `m_register_string_data[reg]` exactly the way the local
  arm does — same `StringResult` shape, same `m_attr_read_pos`
  bump.

Files: `JoinAggInterpreter.cpp`, possibly `JoinAggInterpreter.hpp`
if the walker needs a small refactor to surface `word0` / `word1`.

### F.4-K.3b — Handle CTE redistribution of string aggregate slots

CTE redistribution currently sends raw `AggResItem[]` sections:

- scalar redistribution sends `interp->agg_results()` directly;
- grouped redistribution sends `data + keyLen` directly;
- the receiver calls `mergeOneGroup()` / `mergeScalarAccumulators()`
  over that raw array.

For string slots, raw arrays contain sender-local `val_ptr` values.
F.4 must either:

1. extend redistribution payloads with AGG_CHAR_RESULT-style appended
   string payload data and teach `mergeOneGroup()` /
   `mergeScalarAccumulators()` to resolve those slots into local
   buffers before merge; or
2. add a clear runtime / prepare-time rejection for string MIN/MAX CTE
   materialisation when redistribution can occur.

Preferred fix is option 1 so multi-node / multi-LDM string CTEs match
numeric CTE semantics.  Keep this as its own subphase because it is
independent of linked-attr decoding and protects cluster-wide CTE
materialisation, not just local feed paths.

Files: `DblqhMain.cpp`, `JoinAggInterpreter.{hpp,cpp}`.

### F.4-K.4 — Audit linked-attr stream consumers for fixed-8-byte assumptions

Anything that consumes the CTE linked-attr stream and previously
relied on the 8-byte aggregate-slot invariant needs an audit:

- `Dblqh::runCteFilter` (DblqhMain.cpp:19136-): the WHERE filter
  evaluator runs `NdbInterpretedCode` over the linked-attr buffer.
  Phase D's `branch_linked_inline_*` opcode family already decodes
  the CteLinkedAttr header, so this should Just Work — verify with
  a deliberate string-output filter test.
- `JoinAggInterpreter::initGBTypes` and friends — already CTE-
  marker-aware (the existing string-typed GB column path).  No
  change expected, but spot-check after K.3.
- all `memcpy(..., &item.value, 8)` sites in CTE_LOOKUP /
  CTE_SCAN / CTE aggregation-feed / redistribution code.  No direct
  writer should remain reachable for string slots after K.2b and
  K.3b.

Files: audit only, code change only if regressions surface.

### F.4-K.5 — Block-level NDB-API tests

Add a test mirroring the existing `testCteNdbApiFilter` /
`testCteNdbApi` patterns but exercising string MIN/MAX in the CTE
body and consuming it via:

1. Direct CTE_LOOKUP delivery to the API (validates F.4-K.2).
2. CTE_LOOKUP feeding a downstream `JoinAggInterpreter` MIN/MAX
   over the same string output (validates F.4-K.1 + K.3).
3. CTE_LOOKUP feeding a downstream WHERE filter against the string
   output (validates F.3-R.2's accept-list against the live wire
   format).
4. CTE_SCAN delivery (validates F.4-K.2's shared helper for the
   scan path).
5. Scalar CTE_SCAN and grouped CTE_SCAN legacy/no-AttrInfo paths if
   still reachable after K.2b.
6. Multi-fragment / multi-node redistribution where the string MIN
   and MAX winners are produced on different fragments (validates
   F.4-K.3b).

File: `storage/ndb/block_unit_test/testCteNdbApiVarcharMinMax.cpp`
(or extend `testCteNdbApi.cpp` with new test functions).

### F.4-R.1 — RonSQL re-enable hung scenarios

The `ronsql_cte_minmax_string.test` file was kept around as a
placeholder for the queries that today hang behind
`ZCTE_LOOKUP_OUTPUT_OVERFLOW` retries.  Once F.4-K.1 + K.2 + K.3
are in, those queries should run; record the result file and add
to the suite.

No code change expected in RonSQL itself — F.3-R.1 + F.3-R.2 +
S.5 already accept and render string CTE output.  This sub-phase
is test-only.

File: `mysql-test/suite/ronsql/t/ronsql_cte_minmax_string.test`.

## Sequencing

Land in roughly this order so each commit is testable in
isolation:

1. **F.4-K.1** — `buildCteLinkedBuffer` substitution.  Behaviour-
   neutral until K.3 also lands (linked-attr strings still rejected
   on the receiver side).
2. **F.4-K.2** — `emitCteGroupOutput` substitution.  Direct CTE-
   delivery-to-API queries that pass-through a string MIN/MAX
   become testable.
3. **F.4-K.2b** — replace remaining direct fixed-8-byte CTE emit
   paths or guard them.
4. **F.4-K.3** — `kOpLoadCol` linked-attr string arm.  Combined
   with K.1, downstream re-aggregation and filter evaluation work.
5. **F.4-K.3b** — redistribution payload substitution / decode, or
   explicit rejection if support is deferred.
6. **F.4-K.4** — audit; should be a no-op if K.1-K.3b are correct.
7. **F.4-K.5** — block tests that exercise each of K.1, K.2,
   K.2b, K.3, and K.3b in isolation.
8. **F.4-R.1** — RonSQL MTR coverage.

## Open design questions

- **Multi-LDM-thread coherency for `m_string_results[i].charset`.**
  The per-slot charset is captured on the first row that touches
  the slot.  In a multi-LDM scenario the source aggregator (CTE
  body) may run on a different LDM thread than the consumer
  (cteLookupAggFeed).  Confirm `m_string_results` is the source
  aggregator's, not a per-feed-thread copy — should be true since
  `interp` in `buildCteLinkedBuffer` is the source aggregator
  passed in, but verify with a multi-LDM test.

- **AGG_CHAR_RESULT-style marker for the linked-attr stream?**  Not
  needed: `CteLinkedAttr` already encodes `maxBytes` per slot, so
  variable-size aggregates don't need a separate marker.

- **`Dblqh::buildCteLinkedBuffer` step 1 (parent linked columns).**
  These flow verbatim from DBSPJ's `appendFromParent`.  If a parent
  row carries a string column with the CTE-marker encoding, no
  change here — DBSPJ already builds these.  String aggregates
  produced by a CTE and consumed by ANOTHER CTE's body (chained
  CTEs) would test this; tracked under F.4-K.5.

## Completion criteria

- `WITH cte AS (... MIN/MAX(string) ...) SELECT cte.s FROM cte JOIN
  real_tab ON ...` returns correct results, no retries.
- `WITH cte AS (... MIN/MAX(string) ...) SELECT MAX(cte.s) FROM cte
  JOIN real_tab ON ... GROUP BY ...` returns correct results.
- `WITH cte AS (... MIN/MAX(string) ...) SELECT cte.s FROM cte
  WHERE cte.s = 'literal'` filters correctly.
- The `ronsql_cte_minmax_string.test` placeholder runs end-to-end.
- `testCteNdbApiVarcharMinMax` (or equivalent) covers all delivery
  surfaces in F.4-K.5, including legacy/no-AttrInfo paths and
  multi-fragment redistribution.
- No regression in `testCteNdbApi`, `testCteNdbApiFilter`,
  `testCteNdbApiOuterJoin`.
