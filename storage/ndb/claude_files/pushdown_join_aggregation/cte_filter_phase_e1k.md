# CTE Filter Phase E.1K — inline-encoded CTE virt-column type info in linked-attr buffers

## Sequencing

Lands **between** Phase E.1 (RonSQL `scanCte` as main-query root,
`cte_filter_phase_e1.md`) and Phase E.2 (RonSQL chained CTEs,
`cte_filter_phase_e2.md`). Phase E.1 ships with the
"CTE_SCAN root + real-table child + main aggregator on the leaf"
shape deferred (test block preserved as comments in
`ronsql_cte_scan.test`); Phase E.1K reinstates it once the kernel
gap is closed.

This is a kernel-only phase — no RonSQL changes.

## Context

When Phase E.1 wired `scanCte` as the main-query root, the dropped
MTR test in `ronsql_cte_scan.test` exposed a pre-existing kernel gap
that no prior NDB-API test exercised:

```sql
WITH sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t
              FROM cte_orders GROUP BY o_custkey)
SELECT s.k, SUM(s.t)
FROM sums AS s JOIN cte_customer AS c ON c.c_id = s.k
GROUP BY s.k;
```

Plan: `ops[0] = CTE_SCAN(sums)`, `ops[1] = readTuple(cte_customer)`
with `key = s.k` linked from the CTE_SCAN parent. The main aggregator
runs on `cte_customer` (the deepest leaf), with `GROUP BY s.k` — a
column that comes from the CTE_SCAN parent via DBSPJ's linked-attr
machinery. Result: error 1227 (`ZINVALID_SCHEMA_VERSION`).

## Root cause

Two paths build the linked-attr buffer that feeds
`JoinAggInterpreter::initGBTypes`, and they disagree on what to write
when the parent column is a CTE virtual column.

### Path 1 — same-fragment CTE feed

`Dblqh::cteScanAggFeed` (DblqhMain.cpp:19687) and
`Dblqh::buildCteLinkedBuffer` (DblqhMain.cpp:18928) write
`[tableId=0][schemaVersion=0][AttributeHeader][data]` per group entry.
Both fields zero is the "CTE virtual column" marker.

### Path 2 — DBSPJ child-link expansion

`Dbspj::appendFromParent` (DbspjMain.cpp:13855-13869) uses the parent
tree node's `m_primaryTableId` and pulls `m_currentSchemaVersion`
from `m_tableRecord[m_primaryTableId]`:

```cpp
case QueryPattern::P_ATTRINFO:
  if (addTableMeta) {
    primaryTabRec.i = treeNodePtr.p->m_primaryTableId;   // = 0 for CTE_SCAN
    ptrAss(primaryTabRec, m_tableRecord);
    return appendAttrinfoWithTableMeta(
        dst, ..., treeNodePtr.p->m_primaryTableId,
        primaryTabRec.p->m_currentSchemaVersion, ...);   // junk
  }
```

For a CTE_SCAN tree node, `cte_scan_build` (DbspjMain.cpp:6784-6786)
sets `m_primaryTableId = 0` and `m_schemaVersion = 0`. But
`appendAttrinfoWithTableMeta` reads `m_currentSchemaVersion` of
`tableRecord[0]`, which is whatever real table happens to occupy
slot 0 (or junk for an unused slot). The linked-attr buffer ends up
tagged `[tableId=0][schemaVersion=junk][AH][data]`.

### initGBTypes mismatch

`JoinAggInterpreter::initGBTypes` (JoinAggInterpreter.cpp:1889) reads
`tableId, tableVersion = p[0], p[1]` and:

```cpp
if (table_version_major(tableVersion) !=
    table_version_major(lqh->tablerec[tableId].schemaVersion)) {
  return ZINVALID_SCHEMA_VERSION;
}
Dbtup::Tablerec* tab = &block_tup->tablerec[tableId];
const Uint32* attrDesc = tab->tabDescriptor + linkedAttrId * ZAD_SIZE;
info.typeId = AttributeDescriptor::getType(attrDesc[0]);
...
```

Two problems:

1. The schema-version check fires for Path 2's `[0][junk]` entries —
   error 1227.
2. Even when the schema-version check passes (Path 1's `[0][0]` and
   `tablerec[0].schemaVersion == 0` from the unused slot),
   `tab->tabDescriptor + attrId * ZAD_SIZE` reads from whatever
   memory backs `tablerec[0]` — accidentally OK only because slot 0
   is unused and zero-initialised. Type info is wrong but harmlessly
   so for the integer cases tested.

### Why earlier phases didn't surface this

| Test family | Linked-attr origin | Path | Status |
|---|---|---|---|
| Phase A–D2 (CTE_LOOKUP) | real-table parent → CTE child | Path 2, real `[tableId][schemaVersion]` | works |
| `cteScanAggFeed` (E.1 Tests 1, 4) | same-fragment CTE feed | Path 1, `[0][0]` | works by accident |
| WHERE filter (E.1 Tests 2, 3) | inline-type opcode | bypasses `initGBTypes` | works |
| **CTE_SCAN parent → real-table child + main agg** | DBSPJ-built linked-attr | Path 2, `[0][junk]` | **fails 1227** |

## Goal

Make CTE virtual columns first-class in the linked-attr buffer so
both DBSPJ-built and DBLQH-built buffers carry enough information
for `initGBTypes` to recover the column type without dereferencing
`tablerec[0]`. Achieve this with **zero overhead for non-CTE
queries** by packing type metadata into the existing 96-bit per-entry
header.

## Design — inline encoding

Each linked-attr entry today is

```
[tableId : 32][schemaVersion : 32][AttrHeader : 32][data : variable]
```

Repurpose **bit 31 of `tableId`** as the "CTE virt-column" marker.
Real NDB tableIds fit comfortably in 31 bits, so bit 31 of any real
tableId is always 0 — the marker is unambiguous.

| Field | tableId bit 31 = 0 (real table, unchanged) | tableId bit 31 = 1 (CTE virt column) |
|---|---|---|
| Word 0 | tableId | `0x80000000 \| (typeId << 16) \| maxBytes` |
| Word 1 | schemaVersion | `(csNumber << 16) \| flags` |
| Word 2 | AttrHeader (attrId + dataSize) | AttrHeader (unchanged) |

Bit budget for the CTE case:

| Field | Bits | Notes |
|---|---|---|
| CTE marker | 1 (word 0 bit 31) | distinguishes virt vs real |
| `typeId` | 7 (word 0 bits 22-16) | NDB_TYPE_* fits comfortably |
| `maxBytes` | 16 (word 0 bits 15-0) | up to 64KB; VARCHAR-friendly |
| `csNumber` | 16 (word 1 bits 31-16) | MySQL collation IDs fit |
| flags | 16 (word 1 bits 15-0) | reserved for future extension |

No on-the-wire growth, no `JOIN_AGG_SETUP_REQ` extension.

### Receiver

`JoinAggInterpreter::initGBTypes`:

```cpp
Uint32 word0 = p[0];
Uint32 word1 = p[1];

if (word0 & 0x80000000u) {
  // CTE virt-column entry — type info inline.
  info.typeId   = (word0 >> 16) & 0x7Fu;
  info.maxBytes = word0 & 0xFFFFu;
  Uint32 csNumber = (word1 >> 16) & 0xFFFFu;
  info.cs = (csNumber != 0) ? get_charset(csNumber) : nullptr;
} else {
  // Existing real-table path.
  Uint32 tableId = word0;
  Uint32 tableVersion = word1;
  ndbrequire(tableId != 0);  // virt entries take the branch above
  if (unlikely(tableId >= lqh->ctabrecFileSize)) ...
  if (unlikely(table_version_major(tableVersion) !=
               table_version_major(lqh->tablerec[tableId].schemaVersion))) ...
  Dbtup::Tablerec* tab = &block_tup->tablerec[tableId];
  ...
}
```

The `ndbrequire(tableId != 0)` guards against any future writer
path that bypasses the marker — replaces today's accidental
"`tablerec[0]` happens to be zero" reliance.

### Writer sites — four of them

All four write CTE marker entries today (as `[0][0]` or
`[0][junk]`); they need to fill in inline type info.

| # | Site | File | Type-info source |
|---|---|---|---|
| 1 | `Dblqh::buildCteLinkedBuffer` (CTE_LOOKUP agg-feed + WHERE filter gate) | DblqhMain.cpp:18931 | source `JoinAggInterpreter` (the CTE body's aggregator) |
| 2 | `Dblqh::cteScanAggFeed` | DblqhMain.cpp:19780-19810 | same as #1 |
| 3 | `Dbspj::appendFromParent` with `addTableMeta=true` (cross-fragment via DBSPJ) | DbspjMain.cpp:13855-13869 | SPJ tree-node-stored virt-column metadata |
| 4 | `Dbspj::emitNullAttrinfo` (chained outer-join null path) | DbspjMain.cpp:13710 | parent tree node |

**Sites 1 & 2** already have access to the source aggregator's GB
col type info via the existing `JoinAggInterpreter::m_gb_types[i]`
(typeId, maxBytes, cs). Aggregate-result columns follow the
well-known widening rules (SUM/COUNT → 8-byte BIGINT, MIN/MAX →
source type, widened for numerics per Phase D2). Mirror the
widening in DBLQH via a small helper used by both sites.

**Site 3** needs the CTE node's virt-column types stored on the SPJ
tree node. Extend the `scanCte` / `lookupCte` QueryTree node
serialization with per-column `(typeId, maxBytes, csNumber)` —
~3 words per CTE virt column, **only for CTE ops** in the
QueryTree. The API knows these from the synthetic
`NdbDictionary::Table` (built by RonSQL's
`build_cte_virtual_tables`); store on `Dbspj::TreeNode` extensions
to `CteScanData` / `CteLookupData`. Read at `appendFromParent`
time.

**Site 4** writes a null marker; emit a CTE-marked null entry
(`word0 = 0x80000000`, no type info needed, AttrHeader marks NULL)
so the receiver's marker check stays consistent.

### Why the writers don't need a new signal

- Sites 1, 2: source aggregator's `m_gb_types` is already populated
  by the time these writers run (it ran `initGBTypes` against the
  CTE body's source table).
- Site 3: the QueryTree itself carries the CTE virt-column metadata
  per CTE op, not per query. Cost is paid only by queries that
  define a CTE.
- Site 4: emits a null with no type info — destination only needs
  the marker for consistency with non-null entries.

Result: **zero overhead for any query that doesn't use a CTE**.

## Step structure

| Step | Change | Layer |
|---|---|---|
| 1 | Define encoding constants (`CTE_MARKER_BIT`, encode/decode inlines) in a small shared header (e.g. `AttributeHeader.hpp` or a new `CteLinkedAttr.hpp`) | shared |
| 2 | Extend QueryTree node serialization for `scanCte`/`lookupCte` with per-column `(typeId, maxBytes, csNumber)`; populate from API-side virt table | NDB API + DBSPJ parser |
| 3 | `Dbspj::appendFromParent` writes CTE-marked entry when `treeNodePtr.p->m_primaryTableId == 0`, pulling type info from SPJ tree-node metadata | DBSPJ |
| 4 | `Dbspj::emitNullAttrinfo` emits CTE-marked null when called for a CTE-origin parent | DBSPJ |
| 5 | `Dblqh::buildCteLinkedBuffer` and `Dblqh::cteScanAggFeed` write CTE-marked entries with type info from source aggregator's `m_gb_types` (plus a small widening helper for aggregate-result columns) | DBLQH |
| 6 | `JoinAggInterpreter::initGBTypes` reads the CTE marker and decodes inline type info; add `ndbrequire(tableId != 0)` on the real-table branch | DBTUP |
| 7 | Add `ndbrequire(tableId < 0x80000000u)` at table creation as a defensive guard against a real tableId ever colliding with the CTE marker | DBLQH/DBTUP table-create paths |

## Tests

### Reinstate the dropped MTR test

In `mysql-test/suite/ronsql/t/ronsql_cte_scan.test`, restore the
test block (preserved as comments after Phase E.1):

```sql
WITH sums AS (SELECT o_custkey AS k, SUM(o_amt) AS t
              FROM cte_orders GROUP BY o_custkey)
SELECT s.k, SUM(s.t)
FROM sums AS s JOIN cte_customer AS c ON c.c_id = s.k
GROUP BY s.k;
```

Expected: `(100, 125), (200, 100), (300, 90)`.

### NDB-API direct test

Add to `testCteNdbApiOuterJoin.cpp` (or a new
`testCteNdbApiVirtCol.cpp` if scope grows): `scanCte` parent +
`readTuple` child + main aggregator on the child leaf, with `GROUP
BY` on the parent's CTE virtual column. Validates the kernel fix
without RonSQL in the loop.

### Regression coverage

```
./mtr --suite=ndb_push_agg
./mtr --suite=ronsql
```

All existing tests touching linked-attr buffers should stay green —
they use real-table tableIds (bit 31 = 0), so the encoding is
backward-compatible by construction.

Specific binaries to rebuild and run:
- `testJoinAgg`, `testJoinAggNdbApi` — non-CTE linked-attr paths
- `testCteNdbApiFilter` — Phase A–D2 CTE filter coverage
- `testCteNdbApiOuterJoin` — Phase 1–5 outer-join coverage
- `testMultiOuterJoinAggNdbApi` — chained outer joins
- `ronsql_cte_basic`, `ronsql_cte_scan` — RonSQL CTE coverage

## Cleanly-rejected shapes

- **Char/varchar CTE virt-columns in linked-attr context** —
  `csNumber` is encoded but the existing `AggResItem.value` is 8
  bytes and can't fit a wide value. Reject API-side at aggregator
  setup with a clear message. Numeric INT / BIGINT / DOUBLE GB
  columns cover the immediate need.
- **TypeIds requiring more than 7 bits** — none today; the encoding
  has 16 reserved flag bits in word 1 to extend later if needed.

## Files

- New: `storage/ndb/include/kernel/CteLinkedAttr.hpp` (or extension
  to `AttributeHeader.hpp`) — encoding constants and inline
  helpers.
- `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp` /
  `NdbQueryBuilderImpl.hpp` — extend `scanCte`/`lookupCte` QueryTree
  serialization with per-column type metadata.
- `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp` —
  parse the new QueryTree fields into `CteScanData` /
  `CteLookupData`; update `appendFromParent` and `emitNullAttrinfo`.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` —
  update `buildCteLinkedBuffer` and `cteScanAggFeed` writers.
- `storage/ndb/src/kernel/blocks/dbtup/JoinAggInterpreter.{hpp,cpp}`
  — `initGBTypes` decoder, `ndbrequire(tableId != 0)`, and any
  helper for the readGBKey path if it also reads tableId.
- `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp` (or table
  create site) — `ndbrequire(tableId < 0x80000000u)`.
- `mysql-test/suite/ronsql/t/ronsql_cte_scan.test` — restore the
  deferred test block.
- `mysql-test/suite/ronsql/r/ronsql_cte_scan.result` — re-record.
- `storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` — new
  NDB-API test for `scanCte` parent + real-table child + main agg.
- `storage/ndb/claude_files/pushdown_join_aggregation/CLAUDE.md`
  and `ronsql_cte_plan.md` — already updated to point at this
  doc.

## Risks

1. **Bit 31 of `tableId` collision.** Real NDB tableIds fit in 31
   bits trivially (max table count is far below 2^31). Add
   `ndbrequire(tableId < 0x80000000u)` at table creation as a
   defensive guard.
2. **All four writers must agree on the encoding.** Land Step 1's
   shared header first and route every writer through the inline
   helper so the encoding can't drift.
3. **`AggResItem.value` width.** Unchanged — 8 bytes still. Char/
   varchar CTE GB columns rejected at API setup per the
   "Cleanly-rejected shapes" section above.
4. **`m_primaryTableId == 0` predicate.** Relies on no non-CTE tree
   node ever setting `m_primaryTableId = 0`. Real tables always
   have `tableId > 0`; CTE tree nodes (`cte_lookup_build`,
   `cte_scan_build`, `cte_subtree_build`) explicitly set 0.
   Defensive `ndbassert` worth adding if a `m_isCteOp` flag is
   convenient.

## Overhead comparison

| Approach | Per non-CTE query | Per CTE query | Per CTE virt-col linked-attr entry |
|---|---|---|---|
| Original (extend `JOIN_AGG_SETUP_REQ`) | 6–9 wasted words always sent | 6–9 words used | 0 |
| **Inline encoding (this design)** | **0** | ~3 words per virt col in QueryTree | 0 (reuses existing 96 bits) |

Net: zero overhead for non-CTE queries; minimal QueryTree growth
for CTE queries that's bounded by the CTE definition, not by the
query depth or row count.

## Verification

```
cd debug_build && make -j$(sysctl -n hw.ncpu) ndbd ndbmtd ndb_mgmd \
    ronsql_cli rdrs2 testCteNdbApiOuterJoin testJoinAgg \
    testJoinAggNdbApi testCteNdbApiFilter testMultiOuterJoinAggNdbApi
cd debug_build/mysql-test
./mtr --record --suite=ronsql ronsql_cte_scan
./mtr --suite=ronsql
./mtr --suite=ndb_push_agg
```

Per-phase commit cadence on this branch: ship E.1K as a single
commit ("RONDB-1050: Phase E1K kernel CTE virt-col linked-attr
support"), then proceed to Phase E.2.
