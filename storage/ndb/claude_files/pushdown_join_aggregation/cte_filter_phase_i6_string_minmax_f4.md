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
- Emit `encodeWord0(type, byte_size)` and
  `encodeWord1(slot.charset->number)` — `slot.charset` is on
  `interp->m_string_results[i]` so the helper already has access.
- `AttributeHeader::init(&outBuf[linkedPos], attrId, byte_size)`.
- `memcpy(&outBuf[linkedPos + 1], buf + 4, byte_size)`.
- Advance `linkedPos` by `1 + ((byte_size + 3) >> 2)` words.

NULL string slots stay at `attrSize == 0`, like NULL numerics.

The `interp` parameter already carries the `JoinAggInterpreter`
reference, so the helper can reach `interp->m_string_results[i]`
directly via a public accessor (or a new `slot_charset(i)` /
`slot_prefix_bytes(i)` getter).

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

- The two header words BEFORE the `AttributeHeader` have already
  been consumed by the linked-attr buffer walker before `kOpLoadCol`
  runs (see the surrounding code at JoinAggInterpreter.cpp:1233-
  1276).  The walker captures `word0` and `word1` and exposes them
  alongside the `AttributeHeader`.  K.3 needs to thread those two
  words into the kOpLoadCol arm so the string code can decode
  `csNumber` from `word1` and look up the charset:
  ```cpp
  Uint32 csNumber = CteLinkedAttr::decodeCsNumber(word1);
  const CHARSET_INFO* cs = (csNumber > 0 && csNumber < NDB_ARRAY_SIZE(all_charsets))
      ? all_charsets[csNumber] : nullptr;
  ```
- `prefix_bytes` derived from `type` (0 / 1 / 2) as in the local
  arm.
- Read `payload_len` from the `[prefix]` byte(s) at the start of
  the data segment, just like the local arm does today.
- Populate `m_register_string_data[reg]` exactly the way the local
  arm does — same `StringResult` shape, same `m_attr_read_pos`
  bump.

Files: `JoinAggInterpreter.cpp`, possibly `JoinAggInterpreter.hpp`
if the walker needs a small refactor to surface `word0` / `word1`.

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
3. **F.4-K.3** — `kOpLoadCol` linked-attr string arm.  Combined
   with K.1, downstream re-aggregation and filter evaluation work.
4. **F.4-K.4** — audit; should be a no-op if K.1-K.3 are correct.
5. **F.4-K.5** — block tests that exercise each of K.1, K.2, K.3
   in isolation.
6. **F.4-R.1** — RonSQL MTR coverage.

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
- `testCteNdbApiVarcharMinMax` (or equivalent) covers all four
  surfaces in F.4-K.5 against multi-fragment partitioning.
- No regression in `testCteNdbApi`, `testCteNdbApiFilter`,
  `testCteNdbApiOuterJoin`.
