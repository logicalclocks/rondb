# CTE Filter Phase E.1K Site 4 — chained outer-join null path encoding

## Context

Phase E.1K's `emitNullAttrinfo`
(`storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp:13810-13828`) takes
a `cteOrigin` parameter that defaults to `false`. The two call sites
in the chained-outer-join null path (`expand` direct `P_ATTRINFO`,
`appendFromParent` via `emitNullFromParent`) both rely on the default
— emitting `[0][0][AttrHeader-len-0]`.

That encoding has two latent problems:

1. **`cteOrigin = false` produces `tableId = 0`** in word 0. The
   receiver `JoinAggInterpreter::initGBTypes`
   (`JoinAggInterpreter.cpp:1960`) hits `require(tableId != 0)` and
   crashes if it ever walks to such an entry. The lenient sibling
   `initGBTypesForNullLocal` (line 2071) tolerates `tableId = 0` by
   falling back to `NDB_TYPE_UNSIGNED`, but the strict variant
   doesn't.
2. **`cteOrigin = true` produces `[MARKER_BIT][0]`** — marker bit set
   but `decodeTypeId(word0) = 0` (Undefined). Receiver's CTE branch
   sets `info.cmpFn = NdbSqlUtil::getType(0).m_cmp = nullptr`. Any
   subsequent compare on this GB column dereferences null.

The path is not exercised by current tests (per branch state), but
the fix is cheap and removes a latent crash for any
chained-outer-join-with-CTE-intermediate shape that becomes
load-bearing.

## Fix

**Encoding side.** Change `emitNullAttrinfo` so that when `cteOrigin =
true`, it encodes a safe inline type:

```cpp
meta[0] = CteLinkedAttr::encodeWord0(NDB_TYPE_BIGINT, 8);
meta[1] = CteLinkedAttr::encodeWord1(0);  // no charset
```

A NULL entry has no payload, so the choice of typeId is metadata-only.
`NDB_TYPE_BIGINT` gives the receiver a valid `cmpFn = cmpBigint`
fallback; `maxBytes = 8` matches the matched-CTE path's most common
shape (`AggResItem.value` as `Uint64`). Both `initGBTypes` and
`initGBTypesForNullLocal` decode the CTE branch cleanly
(`JoinAggInterpreter.cpp:1944-1953` and `2061-2068`).

**Call-site side.** Flip the default of `cteOrigin` from `false` to
`true` for `emitNullAttrinfo` and `emitNullFromParent` in
`Dbspj.hpp:1814-1820`. Both current call sites
(`DbspjMain.cpp:13887, 14100`) omit the argument, so they pick up the
new safe default automatically. No explicit `false` callers exist
today.

**Comment update.** Replace the existing comment at
`DbspjMain.cpp:14092-14098` (which says "cteOrigin defaults to false
here") with a note that `emitNullAttrinfo` now produces a safe CTE-
marker prefix unconditionally for the chained-outer-join null path.

## Files

- `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp` — flip defaults.
- `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` — encoding fix
  in `emitNullAttrinfo`; comment update at the call site.

## Verification

- `./mtr --suite=ronsql` — no regressions.
- `./mtr --suite=ndb_push_agg` — no regressions.

## What we're not doing

- **Per-node cteOrigin discrimination.** A more thorough fix would
  pass a `Uint64 cteNullNodes` bitmask through `expand →
  appendFromParent → emitNullFromParent → emitNullAttrinfo` so each
  NULL emit knows which specific tree node it represents and consults
  the bitmask. That requires touching `expand`'s signature and 13+
  callers; the receiver-side consequences are identical to the
  safe-default approach because NULL entries don't carry data — type
  is metadata only.
- **MTR test for the path.** RonSQL doesn't currently produce a query
  that hits this code path. Coverage lands with Phase H (test
  consolidation) or whenever a chained-outer-join CTE-intermediate
  shape ships end-to-end.
