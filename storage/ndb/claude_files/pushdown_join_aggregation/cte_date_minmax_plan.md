# D17 — MIN/MAX over a DATE column in a CTE (integer-day plan)

Status: **PLANNED** (deferred from the test-driven CTE phase; see
`cte_fix_plan.md` E4 and `cte_test_driven_findings.md` D17).

## Goal & scope

Support `MIN(date)` / `MAX(date)` over a `DATE` column in a CTE, end-to-end
through RonSQL (RDRS + ronsql_cli), for the two recorded shapes:

- **Shape A — DATE as a MIN/MAX aggregate input** (`body_agg.inc` probe):
  `WITH x AS (SELECT o_custkey AS k, MIN(o_orderdate) AS mn, MAX(o_orderdate) AS mx
  FROM orders GROUP BY o_custkey) SELECT MIN(x.mn), MAX(x.mx) FROM customer AS c
  JOIN x ON x.k = c.c_custkey;` — DATE MIN/MAX in the CTE body, re-aggregated in
  the main query. GROUP BY is over an INT key.
- **Shape B — GROUP BY a DATE column** (`body_index.inc` index-9):
  `WITH os AS (SELECT o_orderdate AS d, SUM(o_shippriority) AS t FROM orders
  WHERE ... GROUP BY o_orderdate) SELECT MIN(d), MAX(d), SUM(t), COUNT(*) FROM os;`
  — the CTE GROUP BY key is a DATE; the main query MIN/MAX's the DATE virt-column.
  This additionally needs **GROUP-BY-over-DATE** support.

**Approach (integer-day shortcut, chosen over native DATE):** treat a `DATE`
as its 3-byte packed value and reuse the existing Bigunsigned MIN/MAX + wire
path; RonSQL tags the result column as a date and unpacks it for display. No
new aggregate wire format (contrast the string MIN/MAX phase, I.6, which needed
`AGG_CHAR_RESULT`).

**Out of scope:** DATETIME / TIMESTAMP / TIME (kernel `TypeSupported` keeps
rejecting them); SUM / AVG over DATE (meaningless — stay rejected); exact date
arithmetic. COUNT(date) already works (COUNT never loads the value).

## DATE encoding (confirmed)

NDB stores `DATE` as a 3-byte little-endian `uint3korr` value
(`NdbSqlUtil::pack_date` / `unpack_date`, `cmpDate`):

```
w = (year << 9) | (month << 5) | day      // 3 bytes, little-endian
day   = w & 31
month = (w >> 5) & 15
year  = w >> 9
```

`w` is **monotonic** with chronological order (`cmpDate` just compares the two
`uint3korr` values), so integer MIN/MAX over `w` is exactly DATE MIN/MAX. Zero
date `0000-00-00` is `w == 0`.

## Current gaps (investigated)

| Layer | File | Gap |
|-------|------|-----|
| Kernel load | `AggInterpreterBase.cpp` `loadColumnTypedFromBuf` | no `NDB_TYPE_DATE` arm → `ZAGG_LOAD_COL_WRONG_TYPE` (line ~446) |
| Kernel type | `AggInterpreterBase.cpp` `AlignedType` | no `NDB_TYPE_DATE` mapping |
| Kernel GB types | `AggInterpreterBase.cpp` GB type switch (~746-770) | no `NDB_TYPE_DATE` arm (blocks Shape B GROUP BY date) |
| API | `NdbAggregator.cpp` `TypeSupported` | rejects `Date` → `LoadColumn` fails → "Failed writing aggregation program" |
| RonSQL type | `RonSQLPreparer.cpp` `build_cte_virtual_tables` / `resolve_chained_column_type` MIN/MAX arms | no Date case (default best-effort passthrough, not wired) |
| RonSQL display | `ResultPrinter.cpp` | no DATE unpacking → would print the raw packed integer |

## Phases

### Phase 1 — Kernel: DATE as a MIN/MAX aggregate input
`storage/ndb/src/kernel/blocks/dbtup/AggInterpreterBase.{hpp,cpp}`
- `AlignedType(NDB_TYPE_DATE, _) → NDB_TYPE_BIGUNSIGNED` (the register holds the
  packed `w` as an unsigned 64-bit value).
- `loadColumnTypedFromBuf`: add `case NDB_TYPE_DATE:` →
  `val.val_uint64 = uint3korr(&m_attr_read_buf[m_attr_read_pos + 1]); return 0;`
  (mirror the `NDB_TYPE_MEDIUMUNSIGNED` arm; register type already set to
  Bigunsigned by `AlignedType`).
- Column word-advance / skip logic: ensure `NDB_TYPE_DATE` counts as 3 bytes
  (1 word) wherever a per-column size switch advances `m_attr_read_pos`.
- MIN/MAX over Bigunsigned already exists — confirm the **unsigned** compare
  path (`val_uint64`) is used (not signed), so `0000-00-00` (w=0) sorts lowest.
- Block coverage: `testJoinAgg` / `testCaseAgg` with a DATE column (MIN/MAX,
  grouped + scalar, empty input → NULL).

### Phase 2 — NDB API: accept DATE
`storage/ndb/src/ndbapi/NdbAggregator.cpp`
- `TypeSupported`: add `case NdbDictionary::Column::Date: return true;`.
- `LoadColumn` already emits the column's type bits; the kernel's new Date arm
  reads it and the result returns as **Bigunsigned** (kernel-converted) — so the
  API result-parse path is unchanged (existing Bigunsigned handling). No new
  wire format, no `AggResItem` changes.
- Keep `Sum`/`Add`/… over a Date register rejected kernel-side (as for strings).
- Coverage: `testJoinAggNdbApi` / `testCteNdbApi` DATE MIN/MAX.

### Phase 3 — RonSQL: virt-column typing + date display tag
`storage/ndb/src/ronsql/RonSQLPreparer.{cpp,hpp}`, `ResultPrinter.hpp`
- `build_cte_virtual_tables` MIN/MAX arm: `case Date:` → `derived_type =
  Bigunsigned` (8-byte agg-slot wire layout), `derived_length = 1`, and carry a
  **date display tag** onto the virt column / `ColumnMetadata` (analogous to
  D15's scale carry). The wire type is Bigunsigned; the *display* type is DATE.
- `resolve_chained_column_type` MIN/MAX arm: same, for chained CTEs.
- `resolve_cte_output_columns_for_scope`: MIN/MAX already plumbs the source
  `dict_column`; for a DATE source that gives `ColumnMetadata` a Date-typed
  source column — derive `is_date` from `dict_column->getType() == Date`.
- `ResultPrinter::ColumnMetadata`: add `bool is_date` (or store the source NDB
  type) so the printer knows to unpack.

### Phase 4 — RonSQL ResultPrinter: DATE formatting
`storage/ndb/src/ronsql/ResultPrinter.{cpp,hpp}`
- New `aggregate_arg_is_date(out)` helper (mirror `aggregate_arg_scale`) →
  drives a `cmd.print_aggregate.is_date` flag at compile().
- `print_aggregate_result`: when the result is Bigunsigned **and** date-tagged,
  unpack and `snprintf(buf, "%04u-%02u-%02u", w>>9, (w>>5)&15, w&31)`; else
  existing. NULL → existing null path; `w==0` → `0000-00-00` (matches MySQL).
- `print_passthrough_value`: same DATE unpacking for a pass-through DATE
  virt-column (Shape B's GROUP BY date key projected, or scanCte passthrough).

### Phase 5 — GROUP-BY-over-DATE (Shape B prerequisite)
`storage/ndb/src/kernel/blocks/dbtup/AggInterpreterBase.cpp`, `RonSQLPreparer.cpp`
- GB type switch (`initGBTypes` / `publishGBTypes`, ~746-770): add
  `NDB_TYPE_DATE` as a fixed 3-byte non-charset key (`maxBytes = 3`, `cs =
  nullptr`). Grouping needs only **equality**, so `hashGroupKey` uses the raw
  `rondb_xxhash_std` over the 3 bytes (no `strnxfrm` — Date is not a charset
  type) and the bucket compare is a 3-byte memcmp. Deterministic across nodes
  (unlike the D26 charset case), so cross-node redistribution is low-risk —
  still verify on ng2r2/ng2r3/ng4r2.
- The DATE GROUP BY key virt-column (a PK col) is typed for the wire and gets
  the date display tag (Phase 3/4) so the projected key prints as `YYYY-MM-DD`.
- DBSPJ `cte_lookup_hash_key`: a DATE GB key routes via the same raw hash —
  confirm consistency (no per-thread strnxfrm buffer involved for non-charset).

### Phase 6 — MTR coverage + re-enable
- Re-enable `body_agg.inc` DATE MIN/MAX probe (Shape A) and `body_index.inc`
  index-9 (Shape B); add: direct-column DATE MIN/MAX, re-aggregated DATE
  MIN/MAX through CTE_LOOKUP, GROUP BY date, empty-input (NULL), and a
  `WHERE date >= '...'`-filtered variant.
- Record all 5 topologies (base + ng1r3/ng2r2/ng2r3/ng4r2). The multi-node
  runs are the GROUP-BY-date redistribution check.
- Regression: `--suite=ronsql` unchanged-green.

## Risks / edge cases
- **Signed vs unsigned MIN/MAX:** must use the unsigned path (`val_uint64`)
  since `AlignedType→Bigunsigned`; otherwise `w` near 2^23 could misorder.
- **Zero date** `0000-00-00` (w=0) and **NULL** date — verify both round-trip.
- **Type confusion:** the virt-column is Bigunsigned on the wire but DATE for
  display — the tag must not leak into arithmetic/filter paths (a DATE
  MIN/MAX output used in a WHERE/CASE is a follow-up, not v1).
- **DATETIME/TIMESTAMP/TIME** remain unsupported and must still reject cleanly
  (kernel `TypeSupported` + RonSQL).
- **Build:** kernel change ⇒ **ndbmtd rebuild** (not just ronsql_cli/rdrs2) +
  full 5-topology record.

## Effort
Medium, multi-layer: ~6 source files (kernel + NDB API + RonSQL preparer +
result printer), 6 phases, one kernel rebuild, 5-topology record. Smaller than
full native DATE (no new wire format — reuses the Bigunsigned path), but touches
every layer. Sequence Phases 1→4 first (Shape A, the core), then Phase 5 (Shape
B / GROUP BY date), then Phase 6.
