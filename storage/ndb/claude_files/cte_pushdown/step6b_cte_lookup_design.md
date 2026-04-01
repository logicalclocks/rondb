# Phase 6B Detailed Design: CTE_LOOKUP_REQ with readAttributes

## Context

CTE_LOOKUP_REQ must produce normal TRANSID_AI rows (not AGG_RESULT format) using the same DBSPJ-DBLQH interpreted protocol as LQHKEYREQ. The AttrInfo section has the standard 5-word header, interpreter program (usually ExitOK), column reads, FLUSH_AI, and CORR_FACTOR.

The core challenge: the CTE hash table data isn't in a real NDB tuple — GROUP BY keys are AttributeHeader-encoded words, and aggregate results are AggResItem structs. We need a custom readAttributes function that reads from this format.

## CTE Virtual Column Mapping

The CTE has N "virtual columns" with IDs 0..N-1:

| Virtual ID | Source | Data Format in Hash Table |
|------------|--------|--------------------------|
| 0..K-1 | GROUP BY key columns | AttributeHeader-encoded (already in readAttributes output format) |
| K..N-1 | Aggregate result columns | AggResItem (type + value union + null flag) |

DBSPJ builds AttrInfo using these virtual IDs. When CTE_LOOKUP_REQ processes the column list, it maps each virtual ID to the right part of the group data.

## Data Layout in Hash Table

After `lookupGroup()` returns a pointer:
```
[key_data: AttributeHeader-encoded columns, total keyLen bytes]
  [AttrHeader(4B) + data(variable)] for GB col 0
  [AttrHeader(4B) + data(variable)] for GB col 1
  ...
[accumulator_data: AggResItem[], total val_len() bytes]
  [AggResItem(16B)] for agg result 0  (= virtual column K)
  [AggResItem(16B)] for agg result 1  (= virtual column K+1)
  ...
```

## readAttributes for CTE Groups

### Approach: Custom readAttributes with function pointer

Add a `ReadAttributesFn` function pointer to `KeyReqStruct` (or a new CTE-specific struct). The default is `Dbtup::readAttributes()`. For CTE lookups, it points to a new `cteReadAttributes()`.

**`cteReadAttributes()` logic:**
```
For each AttributeHeader in inBuffer:
  attrId = header.getAttributeId()

  if attrId is pseudo-column:
    Handle FLUSH_AI (send accumulated buffer as TRANSID_AI)
    Handle CORR_FACTOR32 (return correlation value)
    Handle other pseudo-columns

  else if attrId < n_gb_cols:
    // GROUP BY column — data is already AttributeHeader-encoded in key
    // Walk the key data to find the N-th AttributeHeader entry
    // Copy [AttrHeader + data] to output buffer

  else if attrId < n_gb_cols + n_agg_results:
    // Aggregate result — convert AggResItem to AttributeHeader + data
    idx = attrId - n_gb_cols
    AggResItem *item = &accumulators[idx]
    Write AttributeHeader(attrId, dataSize)
    Convert item->value to binary column data based on item->type
    (Int64 → 8 bytes, Uint64 → 8 bytes, double → 8 bytes, etc.)
    Handle item->is_null → set NULL in AttributeHeader

  else:
    Error — invalid attribute ID
```

### FLUSH_AI Handling

FLUSH_AI appears in the "final read" section of the AttrInfo program. When encountered:
1. Read 3 parameter words: resultRef, resultData, routeRef
2. Send accumulated output buffer as TRANSID_AI to resultRef
3. Reset output buffer for subsequent reads (CORR_FACTOR comes after FLUSH_AI)

This is the same `flush_read_buffer()` logic used by Dbtup. For CTE_LOOKUP we can call it directly or replicate the TRANSID_AI send pattern.

### CORR_FACTOR Handling

CORR_FACTOR32 is a pseudo-column that returns the correlation factor value (identifying which parent row triggered this lookup). DBSPJ sets this via `LqhKeyReq::setCorrFactorFlag`. For CTE_LOOKUP_REQ, the correlation comes from `senderData` in the request.

## Implementation Plan

### Files to Modify

1. **`DblqhMain.cpp`** — Rewrite `execCTE_LOOKUP_REQ()`:
   - Read sections: key (section 0), AttrInfo (section 1)
   - Look up group in hash table
   - If found: process AttrInfo program using `cteReadAttributes()`
   - Handle FLUSH_AI to send TRANSID_AI
   - Send CTE_LOOKUP_CONF
   - If not found: send CTE_LOOKUP_REF

2. **New: `CteReadAttributes.cpp`** (or in DblqhMain.cpp):
   - `cteReadAttributes()` function that reads virtual columns from CTE group data
   - Handles GROUP BY key columns (copy from key region)
   - Handles aggregate columns (convert AggResItem → AttributeHeader + data)
   - Handles pseudo-columns (FLUSH_AI, CORR_FACTOR)

3. **`CteLookup.hpp`** — Ensure signal has KeySectionNum=0, AttrInfoSectionNum=1

4. **`JoinAggInterpreter.hpp`** — `lookupGroup()` already added

5. **`Dblqh.hpp`** — `execCTE_LOOKUP_REQ()` already declared

6. **`DblqhInit.cpp`** — Signal already registered

### AggResItem → Column Data Conversion

```cpp
// Convert AggResItem to AttributeHeader + binary data
// Returns number of output words written (including header)
Uint32 writeAggResAsColumn(Uint32* outBuf, Uint32 attrId, const AggResItem& item) {
  AttributeHeader* ah = (AttributeHeader*)outBuf;
  if (item.is_null) {
    ah->init(attrId, 0);  // NULL: header only, size=0
    return 1;
  }
  switch (item.type) {
  case NDB_TYPE_BIGINT:
  case NDB_TYPE_BIGUNSIGNED:
    ah->init(attrId, 8);
    memcpy(outBuf + 1, &item.value.val_int64, 8);
    return 3;  // 1 header + 2 data words
  case NDB_TYPE_INT:
  case NDB_TYPE_UNSIGNED:
    ah->init(attrId, 4);
    outBuf[1] = (Uint32)item.value.val_int64;
    return 2;  // 1 header + 1 data word
  case NDB_TYPE_DOUBLE:
    ah->init(attrId, 8);
    memcpy(outBuf + 1, &item.value.val_double, 8);
    return 3;
  // ... other types as needed
  }
}
```

### 5-Section AttrInfo Processing

The AttrInfo section always starts with:
```
[0] InitReadLen     (usually 0)
[1] ExecRegionLen   (usually 1 = ExitOK)
[2] FinalUpdateLen  (always 0 for reads)
[3] FinalRLen       (column reads + FLUSH_AI + CORR_FACTOR)
[4] SubLen          (parameters, usually 0)
```

For Phase 6B, the processing is:
1. Skip InitRead section (length in word 0)
2. Skip interpreter program section (length in word 1) — just ExitOK for now
3. Skip FinalUpdate section (length in word 2, always 0)
4. Process FinalRead section (length in word 3):
   - For each word: extract attrId, call cteReadAttributes
   - FLUSH_AI: send buffer as TRANSID_AI
   - CORR_FACTOR: write correlation value
5. Skip Sub section (length in word 4)

Later phases can execute the interpreter program for filters.

## Verification

1. **Build** — ndbmtd compiles
2. **Existing tests** — no regression
3. **SignalSender test** — exercise the full path: SETUP → scan → COMPLETE → CTE_LOOKUP with AttrInfo → verify TRANSID_AI content
