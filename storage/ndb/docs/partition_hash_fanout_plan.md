# Partition Hash Fanout Plan

## Goal

RonDB currently routes hash-partitioned tables by hashing the primary key, or
by hashing an explicit partition key that is a subset of the primary key. This
works well for key lookups and for scans that can be pruned to one partition,
but single-partition scans can be too serial for workloads that normally scan a
few hundred rows for one entity.

Add a table-level partition hash mode that splits the primary key into:

* a base partition key: the first `x` primary-key columns
* a detail partition key: the next `y` primary-key columns
* a fanout: `z`, the number of partitions used for rows with the same base key

The routing hash is:

```
base_hash = Hash(primary_key_columns[0, x))
detail_hash = Hash(primary_key_columns[x, x + y))
hash = ((base_hash / z) * z) + (detail_hash % z)
```

For scans with equality on the first `x` primary-key columns, DBTC/DBSPJ should
scan the partition interval:

```
[base_partition, base_partition + z)
```

where `base_partition` is derived from `base_hash`.

The DBACC hash must remain the full primary-key hash. Only DIH routing and scan
pruning should use the new composed partition hash.

## Compatibility Model

Represent existing modes as special cases:

* normal full-primary-key hash: `(x = primary_key_column_count, y = 0, z = 1)`
* current explicit partition key: `(x = partition_key_column_count,
  y = primary_key_column_count - x, z = 1)`
* new fanout partitioning: `(x > 0, y > 0, z > 1)`

The first implementation should reject user-defined partitioning and fully
replicated tables, and should not support online changes to the new hash
metadata. This avoids mixing a new hash contract with paths where the client or
SQL layer already supplies exact fragment ids, or where data placement does not
benefit from the interval-pruned scan model.

Require the total number of partitions to be a multiple of the fanout:

```
partition_count % z == 0
```

This must be checked when creating a table and when an alter operation can
change the table's partition count or the `PARTITION_HASH` metadata. This keeps
intervals aligned and avoids relying on partially understood hash-map behavior
during reorg and partition-balance changes. This restriction can be relaxed
later after DIH/hash-map behavior is explicitly tested.

Index table descriptors should keep persistent copies of the partition hash
metadata when they inherit the base table's partitioning metadata. This matches
the existing index-table model for hash-map and partition-count metadata, and
keeps DBTC/DBSPJ table-record lookup local during ordered-index scans.

## Syntax

Use `NDB_TABLE` comment syntax. The existing parser supports bool modifiers and
simple string modifiers, so prefer one string value over nested syntax:

```
COMMENT="NDB_TABLE=PARTITION_HASH=1:1:8"
```

where the fields are:

```
base_pk_columns:detail_pk_columns:fanout
```

Example:

```
CREATE TABLE user_events (
  user_id BIGINT NOT NULL,
  event_ts BIGINT NOT NULL,
  event_id BIGINT NOT NULL,
  payload VARBINARY(200),
  PRIMARY KEY(user_id, event_ts, event_id)
) ENGINE=NDB
COMMENT="NDB_TABLE=PARTITION_HASH=1:2:8";
```

Validation:

* `base_pk_columns >= 1`
* `detail_pk_columns >= 0`
* `base_pk_columns + detail_pk_columns == number_of_primary_key_columns`
* `fanout >= 1`
* if `fanout > 1`, then `detail_pk_columns > 0`
* `fanout <= partition_count`
* `partition_count % fanout == 0`
* all fields fit in `Uint32`
* persisted compact metadata fits in the kernel signal format:
  `base_pk_columns <= 255`, `detail_pk_columns <= 255`, `fanout <= 65535`
* reject malformed strings, missing values, negative values, and extra fields
* reject use on user-defined partitioning
* reject use on fully replicated tables in the first implementation
* reject changing these values through online alter in the first version
* reject alters that would make `partition_count % fanout != 0`

Validation must exist below the SQL layer as well as in `ha_ndbcluster.cc`. The
public NDB API setter must not let direct
`NdbDictionary::Dictionary::createTable()` or `alterTable()` callers bypass the
SQL checks. DBDICT parse should also reject values that would truncate when
stored in `Uint8`/`Uint16` table records or packed into `TcSchVerReq`.

Comment parsing is anchored in:

* `storage/ndb/plugin/ha_ndbcluster.cc`
  * `ndb_table_modifiers`
  * `update_comment_info()`
  * create-table comment parsing
  * `inplace_parse_comment()`
* `storage/ndb/plugin/ndb_modifiers.h`
* `storage/ndb/plugin/ndb_modifiers.cc`

## Metadata Plan

Add explicit table metadata instead of overloading `m_noOfDistributionKeys` or
`m_distributionKey` further.

Suggested fields:

```
m_base_partition_key_count
m_detail_partition_key_count
m_base_partition_fanout
```

Add to:

* `storage/ndb/src/ndbapi/NdbDictionaryImpl.hpp`
  * `NdbTableImpl`
* `storage/ndb/src/ndbapi/NdbDictionaryImpl.cpp`
  * `NdbTableImpl::init()`
  * `NdbTableImpl::assign()`
  * `NdbTableImpl::equal()`
  * table descriptor parse/serialize
* `storage/ndb/include/ndbapi/NdbDictionary.hpp`
  * optional public `Table` setters/getters
* `storage/ndb/src/ndbapi/NdbDictionary.cpp`
  * setter/getter implementations
* `storage/ndb/include/kernel/signaldata/DictTabInfo.hpp`
  * new `Table` property ids after the current TTL fields
  * new `DictTabInfo::Table` members
  * `Table::init()`
  * `TableMapping`
* `storage/ndb/src/common/debugger/signaldata/DictTabInfo.cpp`
  * debugger/string mapping
* `storage/ndb/src/kernel/blocks/dbdict/Dbdict.cpp`
  * receive and store values in dictionary table records
  * build key descriptors with new hash-spec metadata
  * persistently copy base-table partition hash metadata into ordered-index
    table descriptors when the index inherits base-table partitioning
* `storage/ndb/include/ndb_version.h.in`
  * feature version gate

The internal default metadata should be normalized to
`(primary_key_column_count, 0, 1)` when a table has no explicit
`PARTITION_HASH` comment. This default is internal only: `PARTITION_HASH` should
not be shown as a generated canonical comment when it was not explicitly
specified. Current partition-key tables can either be represented internally as
`(partition_key_count, rest, 1)` or kept as the old representation with the new
fields set to the equivalent values. The implementation should choose one
canonical representation and keep API/kernel behavior identical.

## Hash Calculation Plan

### API

Current API hash anchors:

* `storage/ndb/src/ndbapi/Ndb.cpp`
  * `Ndb::computeHash(const NdbDictionary::Table *, Key_part_ptr, ...)`
  * `Ndb::computeHash(const NdbRecord *, ...)`
  * `Ndb::startTransaction(...)`
* `storage/ndb/src/ndbapi/NdbDictionaryImpl.cpp`
  * `createRecordInternal()`
  * index distribution-key inheritance
* `storage/ndb/src/ndbapi/NdbRecord.hpp`
  * `distkey_indexes`
  * `distkey_index_length`

Refactor hash input construction so API code can hash an arbitrary ordered
primary-key interval using the same varlen and charset normalization currently
used for distribution keys.

For a full key lookup:

```
full_pk_hash = Hash(PK[0, pk_count))
base_hash = Hash(PK[0, x))
detail_hash = y == 0 ? 0 : Hash(PK[x, x + y))
routing_hash = compose(base_hash, detail_hash, z)
```

For explicit `PARTITION_HASH` with `z > 1`, `y` must be greater than zero.
`y == 0` is only valid for default/full-primary-key behavior and for equivalent
`z == 1` modes.

`startTransaction()` should use `routing_hash` for the node hint. Operation
KeyInfo should still contain the full key.

For `NdbRecord`, either add explicit base/detail index arrays or derive them
from `key_indexes` plus the table metadata. The derived approach is less
intrusive if all supported layouts use primary-key prefix columns.

### DBTC

Current key-operation anchors:

* `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`
  * `Dbtc::hash()`
  * `Dbtc::handle_special_hash()`
  * `tckeyreq050Lab()`
  * `sendlqhkeyreq()`
* `storage/ndb/src/kernel/vm/SimulatedBlock.cpp`
  * `create_distr_key()`
* `storage/ndb/src/kernel/vm/KeyDescriptor.hpp`

DBTC already separates:

* `thashValue`: full primary-key hash sent to LQH/DBACC
* `tdistrHashValue`: hash or fragment id sent to DIH for routing

Keep:

```
thashValue = full_pk_hash
regCachePtr->hashValue = thashValue
LqhKeyReq::hashValue = thashValue
```

Change only:

```
tdistrHashValue = compose(base_hash, detail_hash, fanout)
DiGetNodesReq::hashValue = tdistrHashValue
```

Do not change DBACC or DBLQH hash semantics.

### DBACC Boundary

Do not route DBACC by the composed partition hash. DBACC bucket lookup and
stored element hash validation depend on the full primary-key hash. Relevant
paths include:

* `storage/ndb/src/kernel/blocks/dblqh/DblqhMain.cpp`
  * LQH stores and forwards `LqhKeyReq::hashValue`
* `storage/ndb/src/kernel/blocks/dbacc/DbaccMain.cpp`
  * `execACCKEYREQ`
  * element hash validation

## Scan Pruning Plan

### NDB API Scans

Current API scan-pruning anchors:

* `storage/ndb/src/ndbapi/NdbScanOperation.cpp`
  * `getPartValueFromInfo()`
  * `getDistKeyFromRange()`
  * `NdbIndexScanOperation::setBound()`
  * `ScanPruningState`
  * `scanIndexImpl()`

Extend the pruning state from:

```
unknown | one_partition | multi_partition | fixed
```

to represent:

```
unknown | one_partition | partition_interval | multi_partition | fixed
```

For an ordered index range, if both bounds exist, are equal on the first `x`
primary-key columns, and those columns map to the base partition key, compute
the base hash and send interval metadata to DBTC. For `fanout == 1`, preserve
the existing one-partition behavior. The first version should not attempt
partition pruning based only on equality in the detail-key columns.

This pruning does not need to be exposed to the SQL optimizer in the first
version. It is sufficient for NDB API/DBTC/DBSPJ to perform the pruning
internally after the access path has been selected.

`ScanTabReq` has one optional `distributionKey` and one request-info bit
stating that it is present. All 32 bits of `requestInfo` are used in newer
RonDB versions, so the interval mode cannot live there. Instead it uses the
high half of `storedProcId`: `storedProcId`, `batch_byte_size` and
`first_batch_size` only carry values below 65536, so their high 16 bits are
guaranteed zero from all older senders (`storedProcId` has been a constant
`0xFFFF` in every NDB API since 2004) and can carry extended request-info
bits, allocated downwards from bit 31:

```
ScanTabReq::setDistributionKeyFlag          existing requestInfo bit,
                                            distributionKey present
ScanTabReq::setDistributionKeyIntervalFlag  new, storedProcId bit 31:
                                            distributionKey is a grouped
                                            partition-hash base hash and the
                                            scan covers the fanout interval
                                            starting at it
ScanTabReq::setDistributionKeyPartIdFlag    new, storedProcId bit 30:
                                            distributionKey is a distinct
                                            fragment id and the scan covers
                                            exactly that fragment
```

Readers check the high half of `storedProcId` for non-zero before decoding
extended bits, and DBTC masks the low 16 bits (`getStoredProcId()`) when
storing the stored-procedure id.

The fanout itself is not carried in the signal; DBTC reads it from its table
record, which is safe because the metadata cannot be changed online. The
interval flag is only valid together with the distribution key flag on tables
with `fanout > 1`; DBTC rejects the scan with a schema-version error
otherwise. The NDB API only sets the flag when the fanout base-key pruning
path recognised the range, which is version-gated on
`ndbd_support_partition_hash_fanout()`.

The API only sets the extended flags when all data nodes support them, gated
on `ndbd_support_partition_hash_fanout()` (25.10.15+, 26.02.6+, 26.04.2+ and
anything above 26.04) — the same gate used for `PARTITION_HASH` table
creation. This gate is required for both flags since old data nodes store
`storedProcId` unmasked. Without support an interval-prunable scan simply
stays unpruned and a partition id scan keeps its legacy signal form, which
is correct on any version.

Existing `distributionKey` scans are not reinterpreted as fanout intervals.
On fanout tables DBTC rejects a hash-valued pruned scan without the interval
flag with a dedicated error (2203, `ZSCAN_PRUNE_PARTITION_HASH_ERROR`): rows
sharing a base partition key are spread over the fanout interval, so a scan
pruned to one partition by a hash value cannot be trusted to see all rows
the application intended. This fails loudly instead of silently returning
partial results, and also covers clients that are unaware of partition hash
fanout. Hash-valued explicit scan partitioning is therefore rejected on
fanout tables:

```
ScanOptions::SO_PART_INFO
  PartitionSpec::PS_DISTR_KEY_PART_PTR
  PartitionSpec::PS_DISTR_KEY_RECORD
```

Distinct-partition explicit scan partitioning works on fanout tables via the
partition id flag, which tells DBTC to resolve `distributionKey` as an exact
fragment id (`distr_key_indicator = 1` towards DIH) instead of mapping it
through the table distribution as a hash:

```
ScanOptions::SO_PARTITION_ID
ScanOptions::SO_PART_INFO with PartitionSpec::PS_USER_DEFINED
NdbOperation::setPartitionId(partitionId)
```

Scanning a distinct fragment enumerates fragments rather than locating rows
by key, so it is well defined regardless of fanout. This keeps the TTL purge
scans working on fanout tables: rest-server2 purges fragment by fragment
using `setPartitionId()`. DBTC validates the fragment id against the DIH
fragment count and rejects out-of-range ids with a fragment error. As a side
effect the fragment id resolution is also exact during hash-map changes,
where the legacy value-as-hash mapping could drift. mysqld only uses
`SO_PARTITION_ID` for user-defined partitioning, which is rejected on fanout
tables, so SQL scans cannot hit these paths.

In the NDB API, the pruning state distinguishes the two prune kinds
(`SPS_ONE_PARTITION` vs `SPS_PARTITION_HASH_INTERVAL`); MRR ranges only stay
pruned when both the prune kind and the prune value match, since a raw
distribution hash and a grouped base hash must never be merged.
`NdbScanOperation::getPruned()` reports interval-pruned scans as pruned, so
`Ndb_pruned_scan_count` includes them.

### DBTC Scans

Current DBTC scan anchors:

* `storage/ndb/src/kernel/blocks/dbtc/Dbtc.hpp`
  * `ScanRecord`
  * `ScanFragLocation`
  * `ScanFragRec`
* `storage/ndb/src/kernel/blocks/dbtc/DbtcMain.cpp`
  * `execSCAN_TABREQ()`
  * `execDIH_SCAN_TAB_CONF()`
  * `sendDihGetNodeReq()`
  * `sendFragScansLab()`
  * `sendScanFragReq()`

Partition-pruned scans store one `m_scan_dist_key` and one flag. The
implemented modes add `m_scan_dist_key_interval_flag` and
`m_scan_dist_key_part_id_flag`, parsed from the storedProcId extended bits,
giving:

```
scan_prune_type: none | one_fragment | one_fragment_by_id | fragment_interval
  none:               !m_scan_dist_key_flag
  one_fragment:       m_scan_dist_key_flag, value is hash or UD partition id,
                      tfragCount = 1
  one_fragment_by_id: m_scan_dist_key_flag && m_scan_dist_key_part_id_flag,
                      value is a distinct fragment id, tfragCount = 1,
                      distr_key_indicator = 1 towards DIH
  fragment_interval:  m_scan_dist_key_flag && m_scan_dist_key_interval_flag,
                      scanFirstHashValue = m_scan_dist_key,
                      tfragCount = table fanout
```

For `fragment_interval`:

* set `scanNoFrag = scan_fanout`
* resolve fragments by calling DIH for `scan_base_hash + i`
* keep `distr_key_indicator = 0` for normal hash partitioning
* for user-defined partitioning, keep the old exact-fragment path and reject
  interval pruning in the first implementation
* reuse the existing fragment-location list and MultiFrag grouping logic

DBTC only enters the interval path when `m_scan_dist_key_interval_flag` is
set. On tables with `fanout > 1`, `execSCAN_TABREQ` rejects a hash-valued
pruned scan (neither interval nor partition id flag) with error 2203, and
rejects malformed flag combinations (interval flag without the distribution
key flag or on tables without fanout, both extended flags together, or the
partition id flag without the distribution key flag) with a schema-version
error, so a hash-pruned one-fragment scan can never silently run against a
fanout table while distinct-fragment scans keep working.

The existing MultiFrag path can already group multiple fragment ids into
`SCAN_FRAGREQ`; the main work is getting the correct list of fragments instead
of forcing a single pruned fragment.

### DBSPJ

Current DBSPJ anchors:

* `storage/ndb/src/kernel/blocks/dbspj/Dbspj.hpp`
  * `ScanFragHandle`
  * `ScanFragData`
* `storage/ndb/src/kernel/blocks/dbspj/DbspjMain.cpp`
  * `parseScanFrag()`
  * `execDIH_SCAN_TAB_CONF()`
  * `scanFrag_parent_row()`
  * `scanFrag_send()`

DBSPJ currently assumes one pruned fragment for `T_CONST_PRUNE` and
`T_PRUNE_PATTERN`. Extend both so one pruning key maps to `fanout`
`ScanFragHandle`s.

For parent-row dependent pruning:

* compute the base hash from the correlated equality values
* resolve `fanout` fragments
* append/copy the same bounds and parameter sections to each fragment
* preserve section ownership and release behavior

This is the most delicate part of the feature. Implement after DBTC non-SPJ
scan pruning is stable.

## Implementation Phases

### Phase 1: Metadata and DDL

* Add `PARTITION_HASH` parsing.
* Add table metadata in NDB API and DictTabInfo.
* Add validation and version gating.
* Add comment preservation on alter.
* Check `partition_count % fanout == 0` both at create time and for alter paths
  that can change partition count or `PARTITION_HASH`.
* Add lower-layer validation in NDB API dictionary create/alter and DBDICT
  parse so direct API callers cannot bypass SQL validation.
* Persistently copy partition hash metadata to ordered-index table descriptors
  when index partitioning metadata is inherited from the base table.
* Reject online changes to the metadata.
* Add dictionary serialization/deserialization tests.

Exit criteria:

* valid DDL creates a table with persisted metadata
* invalid DDL fails with clear errors
* restart/reopen preserves metadata
* no hash behavior changes yet unless values are default
* tables without explicit `PARTITION_HASH` do not show a generated canonical
  `PARTITION_HASH` comment

### Phase 2: API Routing Hash

* Add shared helper for hashing primary-key intervals.
* Update `Ndb::computeHash()` and `startTransaction()` hinting.
* Update `NdbRecord` support.
* Keep operation KeyInfo unchanged.

Exit criteria:

* API-computed routing hash matches an independent test oracle
* default and `fanout = 1` cases match current behavior

### Phase 3: DBTC Key Operations

* Add kernel key-descriptor metadata.
* Compute composed `tdistrHashValue`.
* Preserve full-key `thashValue` for LQH/DBACC.
* Verify primary-key lookup, write, update, delete, and unique-index paths.

Exit criteria:

* key operations find the correct rows for `fanout > 1`
* DBACC still receives full-primary-key hashes
* API and DBTC route to the same fragment

### Phase 4: DBTC Scan Interval Pruning

* Extend `ScanTabReq` with the `DistributionKeyIntervalFlag` and
  `DistributionKeyPartIdFlag` extended bits so explicit scan partitioning
  APIs cannot be mistaken for fanout base-key interval pruning.
* Extend API scan-pruning state with `SPS_PARTITION_HASH_INTERVAL` and
  distinguish distinct-partition-id prune values.
* Extend DBTC scan state (`m_scan_dist_key_interval_flag`,
  `m_scan_dist_key_part_id_flag`) and DIH fragment resolution loop.
* Reject hash-valued one-partition pruned scans on fanout tables in DBTC
  with error 2203; distinct-fragment-id scans (setPartitionId(),
  SO_PARTITION_ID, e.g. TTL purge) keep working via the partition id flag.
* Reuse existing MultiFrag scan grouping.

Exit criteria:

* equality on base key scans exactly `fanout` fragments
* hash-valued explicit one-fragment scan partitioning on fanout tables is
  rejected clearly with error 2203
* setPartitionId()/SO_PARTITION_ID scans one distinct fragment, also on
  fanout tables, and TTL purge works on fanout tables
* `fanout = 1` is equivalent to a table without `PARTITION_HASH` (all hash
  paths gate on `fanout > 1`): full-PK hash routing, no interval pruning
  (verified by `ndb_partition_hash_fanout.test`)
* no rows are missed or duplicated

### Phase 5: DBSPJ

Status: DEFERRED to a future RonDB version (decision 2026-07-02). Pushed-join
pruning stays disabled for fanout tables (correct, unoptimized). Design notes
and machinery study are preserved in `partition_hash_fanout_spj_worklog.md`.

* Extend constant and pattern pruning to produce `fanout` fragments.
* Duplicate/share range and parameter sections correctly.
* Verify pushed joins with parent-row dependent pruning.

Exit criteria:

* SPJ scans return the same result as non-pushed execution
* overlapping intervals from multiple parent rows do not corrupt sections
* MultiFrag grouping still behaves correctly

### Phase 6: Reorg, Backup, Upgrade, and Performance

* Test with node restart and system restart.
* Test backup/restore.
* Test online reorg/hash-map changes allowed by the chosen restrictions.
* Add mixed-version rejection.
* Add performance measurements for target workloads.

Exit criteria:

* feature is blocked on old data nodes
* backup/restore preserves metadata
* scan latency improves for entity-range workloads without breaking throughput

## Test Plan

### Unit and Low-Level Tests

Hash helper tests:

* fixed-width integer PK columns
* varlen PK columns
* charset/collation-sensitive PK columns
* `y = 0`
* `z = 1`
* `z > 1`
* reject explicit `z > 1` with `y = 0`
* maximum supported primary-key column count
* malformed or null key parts return existing error behavior

Expected checks:

* base hash input contains exactly PK columns `[0, x)`
* detail hash input contains exactly PK columns `[x, x + y)`
* full-key hash is unchanged
* API helper and kernel helper produce identical routing hashes

Metadata tests:

* default table metadata is `(pk_count, 0, 1)`
* explicit `PARTITION_HASH` metadata survives dictionary round-trip
* implicit default metadata is not rendered as a generated table comment
* table equality and assign include the new fields
* index table metadata inherits the base-table hash spec where needed
* direct NDB API create rejects invalid partition hash metadata
* DBDICT rejects compact metadata overflow instead of truncating it
* direct NDB API alter rejects changing partition hash metadata
* direct NDB API alter rejects partition-count changes where
  `partition_count % fanout != 0`

### DDL and SQL Tests

Create-table positive cases:

```
PRIMARY KEY(a), PARTITION_HASH=1:0:1
PRIMARY KEY(a,b), PARTITION_HASH=1:1:2
PRIMARY KEY(a,b,c), PARTITION_HASH=1:2:4
PRIMARY KEY(a,b,c), PARTITION_HASH=2:1:4
```

Create-table negative cases:

```
PARTITION_HASH=
PARTITION_HASH=1
PARTITION_HASH=1:1
PARTITION_HASH=0:1:2
PARTITION_HASH=1:-1:2
PARTITION_HASH=1:1:0
PARTITION_HASH=1:1:2:3
PARTITION_HASH=abc
PARTITION_HASH=2:2:2 on PRIMARY KEY(a,b,c)
PARTITION_HASH=1:0:2
fanout > partition_count
partition_count % fanout != 0
user-defined partitioning with PARTITION_HASH
fully replicated table with PARTITION_HASH
```

Alter-table tests:

* changing unrelated comments preserves `PARTITION_HASH`
* changing `READ_BACKUP`, `TTL`, or `PARTITION_BALANCE` preserves
  `PARTITION_HASH`
* attempting to change `PARTITION_HASH` online is rejected
* attempting to alter the table into a partition count that is not a multiple
  of `fanout` is rejected
* attempting to make a `PARTITION_HASH` table fully replicated is rejected
* `SHOW CREATE TABLE` exposes the comment consistently
* `SHOW CREATE TABLE` does not add `PARTITION_HASH` for tables where it was not
  explicitly specified

### NDB API Key Operation Tests

Use tables where many rows share the same base key and have different detail
keys.

Operations:

* `startTransaction(table, Key_part_ptr, ...)`
* `startTransaction(NdbRecord, keyData, ...)`
* primary-key read
* insert
* update
* write
* delete
* simple read and committed read
* batch operations
* unique index read/update/delete

Checks:

* all rows are found
* no operation routes to a fragment that cannot contain the row
* API transaction hint and DBTC routing agree
* unique index path uses base-table partition metadata correctly
* `fanout = 1` matches existing partition-key behavior

### SQL Read and Write Tests

Use SQL tables with:

* integer base key and timestamp/detail key
* varchar base key
* composite base key
* nullable non-key columns
* blobs excluded from the primary key

Queries:

```
SELECT * FROM t WHERE entity_id = ?
SELECT * FROM t WHERE entity_id = ? AND ts BETWEEN ? AND ?
SELECT * FROM t WHERE entity_id = ? ORDER BY ts LIMIT 100
SELECT * FROM t WHERE entity_id IN (?, ?)
SELECT * FROM t WHERE ts = ?
UPDATE t SET payload = ? WHERE entity_id = ? AND ts = ? AND id = ?
DELETE FROM t WHERE entity_id = ? AND ts = ? AND id = ?
```

Checks:

* equality on the base key scans `fanout` fragments
* no equality on the base key scans all fragments
* equality on full PK still uses key lookup
* result sets match a table using current full-PK hash
* no duplicates with ordered and unordered scans

### Scan Pruning Tests

NDB API ordered-index and table scans:

* one range pruned to one interval
* multiple MRR ranges pruned to the same interval
* multiple MRR ranges pruned to different intervals
* one prunable and one non-prunable range
* range bounds equal on first `x` columns but open on detail columns
* range bounds not equal on first `x` columns
* equality only on detail-key columns
* `ScanOptions::SO_PART_INFO` with `PS_DISTR_KEY_PART_PTR`
* `ScanOptions::SO_PART_INFO` with `PS_DISTR_KEY_RECORD`
* `ScanOptions::SO_PARTITION_ID`
* old API `setPartitionId()`

Checks:

* same interval keeps pruning
* different intervals fall back or are represented safely
* mixed prunable/non-prunable falls back to all partitions
* equality only on detail-key columns does not trigger first-version pruning
* hash-valued explicit scan partitioning (SO_PART_INFO with distribution key
  values) fails with error 2203 on fanout tables and keeps one-fragment
  semantics on all other tables
* setPartitionId()/SO_PARTITION_ID scans exactly the given fragment on all
  tables including fanout tables, and rejects out-of-range fragment ids
* TTL purge scans work on fanout tables
* fanout interval pruning uses only the new `DistributionKeyIntervalFlag`
* `fanout = 1` keeps current behavior (full-PK hash, no pruning change)

### DBTC Kernel Tests

Instrumented or debug tests should verify:

* `thashValue` is full primary-key hash
* `tdistrHashValue` is composed partition hash
* `LqhKeyReq::hashValue` receives the full hash
* `DiGetNodesReq::hashValue` receives the routing hash
* pruned scan with interval sets `scanNoFrag = fanout`
* `sendDihGetNodeReq()` resolves all interval fragments
* MultiFrag grouping still groups by destination block ref

Failure injection:

* DIH returns moving fragment during reorg
* one interval fragment is unavailable
* API node failure during pruned interval scan
* scan close while multiple interval fragments are active

### DBSPJ Tests

Pushed join cases:

* parent lookup drives child scan by base key
* parent scan drives child scan by base key
* multiple parent rows map to the same interval
* multiple parent rows map to overlapping intervals
* multiple parent rows map to different intervals
* child has ordered bounds in addition to base-key equality

Checks:

* pushed and non-pushed plans return identical rows
* range numbers are preserved
* no section leaks or double frees
* scan repeat and close paths work with multiple fragment handles

### Restart, Backup, Upgrade, and Reorg Tests

Restart:

* data node restart
* API reconnect
* system restart
* table reopen after mysqld restart

Backup/restore:

* backup table with `PARTITION_HASH`
* restore into clean cluster
* verify metadata and data placement

Upgrade/version:

* create rejected if any data node lacks feature version
* old API client sees a clear failure or a compatible default
* rolling-upgrade scenario with feature unused
* rolling-upgrade scenario attempting to create feature table

Reorg/hash-map:

* add node or repartition where supported
* verify rows remain findable by key
* verify interval scans remain correct
* verify alters that change partition count recheck `partition_count % fanout`
* verify fully replicated conversion is rejected for `PARTITION_HASH` tables

### Performance Tests

Primary workload:

* table primary key `(entity_id, ts, id)`
* many rows per `entity_id`
* queries scan 100 to 1000 rows for one entity
* compare `fanout = 1`, `2`, `4`, `8`, `16`

Metrics:

* p50/p95/p99 latency for base-key scans
* throughput under concurrent entity scans
* CPU per query on TC, LQH, and SPJ threads
* fragments scanned per query
* network messages per query
* impact on primary-key lookup latency
* impact on insert/update/delete throughput

Expected outcome:

* moderate fanout improves entity-scan latency
* very high fanout shows overhead, giving guidance for recommended limits
* key lookup cost remains close to current behavior

## Deferred Questions

* Can the `partition_count % fanout == 0` restriction be relaxed after
  DIH/hash-map behavior is tested more thoroughly?
* Should a later version expose multi-fragment partition pruning to the SQL
  optimizer for costing?
* Should a later version support pruning based on equality in the detail-key
  columns?
* Should fully replicated tables support any form of `PARTITION_HASH` after the
  basic hash and scan-pruning behavior is stable?
