# RONDB-1072: JOIN_AGG_SETUP_REQ Column Metadata Plan

## Problem

Join aggregation currently resolves some column type metadata during execution.
This is fragile for CTE and DBSPJ paths because execution can happen in DBQLQH
or in paths that do not have a scanned-table `KeyReqStruct`. In particular,
linked columns can require metadata for both GROUP BY key handling and
aggregation program input handling.

The known failure mode is `JOIN_AGG_NULL_ROW_REQ`: DBSPJ sends linked parent
data directly to DBLQH/DBQLQH, then `JoinAggInterpreter` can try to resolve
linked column metadata from table objects while running in a context that has no
safe table metadata owner bound.

The long-term fix is to move all metadata needed by the aggregation interpreter
into `JOIN_AGG_SETUP_REQ`, cache it on the aggregation object, and make runtime
execution use that local cache instead of table records or linked-buffer metadata.

## Design Goals

- Metadata is produced once, before execution, by the component that has the
  dictionary column definitions.
- Runtime aggregation execution does not read `Dbtup::Tablerec` for column type
  metadata.
- Linked columns are handled for both GROUP BY keys and `kOpLoadCol` inputs.
- Absence of the metadata section means no metadata is needed by this program.
- No backwards compatibility mode is required.
- The metadata cache is safe for the duration of the query.

## Producer

The NDB API should create the new metadata section.

`NdbAggregator` already has the authoritative `NdbDictionary::Column` pointers
for normal columns and for linked parent columns:

- `LoadColumn()` records the local table column used by a load instruction.
- `LoadLinkedColumn()` receives the parent `Column*`.
- `GroupBy()` records the local table column used by a GROUP BY key.
- `GroupByLinked()` receives the parent `Column*`.

This is the best layer to encode metadata because linked parent columns may not
exist in the leaf table dictionary. DBTC should not rediscover this metadata.
DBTC should only split and forward the setup sections.

## Transport

Add a third optional long section to `JoinAggSetupReq`:

```text
section 0: aggregation program
section 1: receiver ids
section 2: column metadata
```

Suggested constants:

```cpp
static constexpr Uint32 ColumnMetaSectionNum = 2;
static constexpr Uint32 MaxSections = 3;
```

There are two transport levels:

1. The NDB API to DBTC query request has one combined aggregation payload for
   the whole query. That payload must contain metadata for every aggregation
   object in the query: the main SELECT aggregation, if present, and every CTE
   aggregation.
2. DBTC sends one `JOIN_AGG_SETUP_REQ` per aggregation object and per target
   node. DBTC must split the combined metadata payload and attach only the
   relevant metadata block to the specific setup request being sent.

So the API-side query section needs a container format, not a single flat
metadata table. DBTC should not rediscover metadata, but it must parse enough of
the container to select the correct block for each outgoing setup request.

DBTC should store the selected per-aggregation metadata blocks in QueryMemory
allocated by `lc_ndbd_pool_malloc`, not as persistent long sections. Section
memory is a more constrained resource, while QueryMemory can be extended more
easily and gives DBTC contiguous buffers. DBTC should create the optional
`JOIN_AGG_SETUP_REQ` section 2 only transiently when sending each setup signal,
then let normal signal section ownership release it.

Suggested API-to-DBTC metadata container:

```text
magic
version
block_count
block[block_count]
```

Each block:

```text
agg_kind       // main SELECT or CTE
cte_index      // RNIL for main SELECT, otherwise CTE index
entry_count
entry[entry_count]
```

When DBTC sends a main `JOIN_AGG_SETUP_REQ`, it attaches the block with
`agg_kind = main` and `cte_index = RNIL`. When DBTC sends a CTE setup request,
it attaches the block matching that CTE index. The attached
`JOIN_AGG_SETUP_REQ` section 2 can then use the simpler per-aggregation section
format described below.

## Section Format

Use a versioned word format for the per-aggregation section attached to
`JOIN_AGG_SETUP_REQ`:

```text
word 0: magic
word 1: version
word 2: entry_count
entry[entry_count]
```

Initial entry:

```text
source_kind
source_id
program_pos
gb_index
table_id
schema_version
column_id
type_id
max_bytes
charset_number
decimal_precision_scale
flags
```

`decimal_precision_scale` can use the high 16 bits for precision and low 16 bits
for scale. For non-decimal columns it is zero.

Suggested `source_kind` values:

```text
0: local table column
1: linked column position
2: CTE virtual/result column
```

Suggested flags:

```text
bit 0: unsigned
bit 1: nullable, if useful
bit 2: metadata used by GROUP BY
bit 3: metadata used by kOpLoadCol
```

## Runtime Keys and Validation Fields

Runtime lookup should use the same addressing model as the aggregation program:

```text
source_kind + source_id
```

For `kOpLoadCol`, `source_id` is the column id for local columns and the linked
position for linked columns. For GROUP BY linked columns, `source_id` is also the
linked position. For CTE virtual columns, `source_id` is the virtual/result
column position.

`table_id`, `schema_version`, and `column_id` are provenance and validation
fields, not the hot-path key. They allow setup-time checks and diagnostics, but
runtime execution should not require dictionary/table access once the metadata is
cached.

## Consumer

`DblqhProxy::execJOIN_AGG_SETUP_REQ()` should parse section 2, validate it
against the aggregation program, and store a compact metadata cache on
`AggInterpreterBase`.

The interpreter should expose cache lookups along these lines:

```cpp
const AggColumnMeta* findLoadColMeta(Uint32 sourceKind, Uint32 sourceId) const;
const AggColumnMeta* findGbMeta(Uint32 gbIndex) const;
```

GROUP BY metadata should be initialised from the setup cache before any rows are
processed. `kOpLoadCol` should use the setup cache when loading linked string
columns or any other metadata-sensitive column.

## Validation

Setup should fail if the aggregation program requires metadata and section 2 does
not provide it. Since there is no backwards compatibility requirement, there
should be no runtime table-metadata fallback for new metadata-dependent paths.

Validation should check:

- duplicate entries do not conflict
- metadata type matches the `kOpLoadCol` opcode type
- decimal precision/scale matches the decimal word following the load opcode
- every metadata-sensitive GROUP BY column has an entry
- every metadata-sensitive linked `kOpLoadCol` has an entry
- charset numbers are valid when nonzero

If section 2 is absent, the program must be metadata-free.

## Low-level Test Migration

A number of low-level tests construct aggregation programs and send raw signals
directly. Those tests must be kept and migrated as part of this change, not
disabled or replaced only by SQL-level coverage.

The migration should avoid copying the metadata wire format by hand into every
test. Add a small test-side helper or shared builder that can emit the same
column metadata container used by the NDB API path. Raw-signal tests should use
that helper to build:

- the whole-query API-to-DBTC metadata container when they test DBTC splitting
- the per-aggregation `JOIN_AGG_SETUP_REQ` section 2 when they test DBLQH setup
  directly

Test updates required:

- identify all tests that send `JOIN_AGG_SETUP_REQ`, `SCAN_TABREQ` with embedded
  join aggregation setup, or hand-built CTE aggregation definitions
- update tests with metadata-sensitive linked columns to include section 2
- keep tests without metadata-sensitive columns section-free to verify the
  "metadata-free program" path
- add negative setup tests for missing metadata when the program requires it
- add negative setup tests for duplicate/conflicting entries and opcode/type
  mismatch
- add CTE-specific raw-signal tests proving DBTC selects the right metadata
  block for each CTE setup request

This is important because raw-signal tests exercise failure handling, routing,
node failure, and setup edge cases that SQL tests do not cover reliably.

## Runtime Changes

After setup-time metadata is available, these runtime metadata sources should be
removed or reduced to defensive assertions:

- linked table lookup in `AggInterpreterBase::initGBTypes()`
- linked table lookup in `JoinAggInterpreter::initGBTypesForNullLocal()`
- table fallback in `AggInterpreterBase::initGBTypesFromTable()`
- linked-buffer metadata dependency for string `kOpLoadCol`

Linked buffers should continue to carry values and `AttributeHeader`s. They
should no longer be authoritative for type, charset, or declared-size metadata.

## Implementation Order

1. Add signal constants and DBTC forwarding for optional section 2.
2. Add NDB API metadata section generation from `NdbAggregator`.
3. Add shared/test metadata builders and migrate raw-signal tests that need
   metadata.
4. Add parser and cache storage in `AggInterpreterBase`.
5. Use the cache to initialise GROUP BY metadata.
6. Use the cache for metadata-sensitive linked `kOpLoadCol`.
7. Remove the runtime table-metadata fallbacks.
8. Add SQL tests for linked string GROUP BY, linked string MIN/MAX, null-extended
   rows, CTE lookup/scan, redistribution, empty result finalisation, and
   multi-node-group setups.
