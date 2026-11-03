# DBSPJ query-shape and data correctness: weaknesses and new tests

Branch RONDB-1124-spj-fixes, HEAD 6598f4a65e3. Line numbers are from the working tree at the time of writing. Another agent was editing DbspjMain.cpp at the same time, so DbspjMain.cpp line numbers may be off by about 20; the function names are exact.

No C++ code was changed. No build or test was run. Every test is new; no existing test was edited.

## 1. New test files

| File | Suite / cluster | What it covers |
|---|---|---|
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_check.inc` | helper | Runs a query with the pushdown under test OFF, then ON, and writes both results to files. A Perl block compares the files, as multisets or in row order. It counts the tables EXPLAIN shows in pushed joins (and `Using pushed (join) aggregation`), and checks that `Ndb_pushed_queries_executed` grew. A failure is echoed with up to six differing rows and collected. |
| `.../ndb_opt/ndb_spj_shapes_init.inc`, `ndb_spj_shapes_end.inc` | helper | Set the defaults. At the end, echo `# FAILED shapes (...)` and `--die` if any shape failed. |
| `.../ndb_opt/ndb_spj_shapes_keytype.inc`, `ndb_spj_shapes_variant.inc`, `ndb_spj_shapes_dmlcmp.inc` | helper | Checks for one key data type, one table variant, and one DML statement. |
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_nulls.test` | ndb_opt (2 data nodes, 8 fragments) | NULL linked keys (29 shapes) |
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_keys.test` | ndb_opt | Linked keys of 18 data types and 6 collations (about 75 checks) |
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_outer.test` | ndb_opt | Outer, semi- and antijoins, 26 shapes x 2 passes. Pass 2 uses `max_4rows_in_spj_batches` in debug builds. |
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_fanout.test` + `.cnf` | ndb_opt with BatchSize=3, BatchByteSize=1024 | Multi-batch, bushy, skewed fan-out, wide rows, sorted roots with and without LIMIT (24 shapes) |
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_wide.test` | ndb_opt | Fragmented TRANSID_AI (Q-1) |
| `mysql-test/suite/ndb_opt/ndb_spj_shapes_variants.test` | ndb_opt | Covers:<br>- table variants: FR, RB, hidden PK, PARTITION BY KEY on a PK subset, PARTITION_HASH, PARTITION_BALANCE, TTL<br>- locking reads, including a second connection that must time out<br>- DML<br>- E-26<br>- odd partition counts (Q-8, last) |
| `mysql-test/suite/ndb_push_agg_dist/t/ndb_spj_shapes_agg.test` + `.cnf` | ndb_push_agg_dist (2 data nodes), DataMemory=400M, BatchSize=64 | Join aggregation:<br>- E-1 … E-30 from the coordinator<br>- Q-3, Q-5, Q-6, Q-7<br>- NULL groups (also of NOT NULL outer-joined columns), empty input, string MIN/MAX<br>(59 checks) |
| `mysql-test/suite/ndb_push_agg_dist/t/ndb_spj_shapes_agg_exact.test` | ndb_push_agg_dist; **disabled** in `collections/disabled_ndb.def` | Q-6a/b: SUM(BIGINT) beyond 64 bits fails with 1860 |

Aggregation checks use a pure MySQL reference run, with `ndb_join_pushdown=OFF` and `ndb_join_pushdown_aggregate=OFF`. The checked run sets `ndb_join_pushdown`, `ndb_join_pushdown_aggregate` and `ndb_join_pushdown_aggregate_outer_join` ON. Single-table shapes toggle `ndb_pushdown_aggregate` instead.

Every E-n shape requires `Using pushed join aggregation` (`$spj_expect_agg=1`), except E-10, E-30, Q-5 and COUNT(DISTINCT), which only print `agg=`.

## 2. Coverage map: existing tests vs new tests

Sources:
- existing pushed-join tests: `ndb_opt/join_pushdown.inc` (6k lines), `spj_rqg`, `bushy_joins`, `spj_skew`, `spj_dict`, `subquery`
- existing join-aggregation tests: `ndb_push_agg*/t`, `ndb_push_agg_dist/t`

| Category | Existing coverage | New coverage |
|---|---|---|
| NULL linked key into a lookup child, composite keys with a partial NULL | Yes (join_pushdown.inc L1556, L4350-4438, L1973-2075) | N1-N3, N8, N10, N11, N15 (from parent + grandparent) |
| NULL in the bounds of a scan child | Thin (L4089, L2212) | N4-N7, N9, N12-N14, N16 (pruned) |
| NULL-safe `<=>` as ref key | Only as a non-null-rejecting second key part (bka test) | N17-N20, agg E-15 |
| NULL / IS NULL bounds of the root index, NULL-first order | No | N21-N24 |
| Outer joins with zero-match parents, nested nests | Yes (L757-871, 3473-3864, 5765-5865), all recorded-only | O1-O14 x 2 passes, now self-verifying |
| Semi-join strategies | FirstMatch / Materialization / DupsWeedout (L5109-5510) | S1-S7 per strategy hint, with scan children |
| Antijoins (NOT EXISTS, LEFT JOIN … IS NULL, NOT IN with NULLs) | spj_rqg only | A1-A5, N26-N29, F13 |
| Bushy scans with uneven fan-out over many batches | bushy_joins, L3596-3946 (debug hooks only) | F8-F11 with BatchSize=3, also in release builds |
| More child rows than the batch size, rows wider than the batch byte limit | Partial (100 children, 3 wide rows) | F1-F3 (600 children), F14-F15 (rows of 2-3 KB against a 1 KB limit), W* (30 KB rows) |
| Sorted root scans with children, LIMIT, DESC | ORDER BY yes; LIMIT barely | F16-F22, N24, Q-8d |
| Fragmented TRANSID_AI (rows above 7400 words, 240 in VM_TRACE builds) | None | W1-W8 |
| Collations (case, accent, trailing space) on linked keys | None | L-* for 6 collations x {PK lookup, index scan, pruned scan, scan then lookup} |
| Key data types as *linked* keys (not constants) | Numeric and char only (ndb_endian) | K-* for 18 types, incl. DATETIME(6), TIME(3), TIMESTAMP(2), YEAR, ENUM, SET, BINARY, VARBINARY with trailing 0x00 |
| FULLY_REPLICATED, READ_BACKUP, hidden PK, PARTITION_HASH, TTL, PARTITION_BALANCE | Not tested (PARTITION_HASH: one join) | V-* (5 shapes each) |
| Odd partition count on 2 nodes | None | Q-8a-d |
| Locking reads | EXPLAIN only, never executed | L1-L3 results; L4/L5 check the locks |
| DML with a pushed read side | None | D1-D4 (INSERT…SELECT, CTAS, multi-table UPDATE and DELETE) |
| Pushed-join builder limits (MAX_REFERRED_FIELDS) | None | E-26 plus a control with 16 references |
| Self-verifying comparison of pushdown on vs off | push_agg only; join tests are recorded-only | All new tests |
| Join aggregation: empty input | Only via a constant PK miss | N4-N6 (pushed root filter) |
| Join aggregation: NULL group keys | Child column, inner join only | N1-N3 (root, leaf, outer child) |
| Join aggregation: collation GROUP BY | None | E-5a-f |
| Join aggregation: MIN/MAX of strings under collation | Default data, order-insensitive | M1, M2 (data where collation order differs from byte order) |
| Join aggregation: SUM beyond 64 bits | NDB API tests only | Q-6a/b; DECIMAL(38,6): Q-6c |
| Join aggregation: double constants in expressions | Not targeted | E-1a-f (+ an integer control) |
| Join aggregation: multi-batch self-continuation | None at SQL level | E-2a-c (60k-row root x 3 children), E-3b/c (20k-row root) |
| Join aggregation: chained / nested / sibling outer joins | 2-way only; 3-way disabled | E-3, E-6, E-8, E-9, Q-5 |
| Join aggregation: IN-list leaf filter, range / IN-list root | None | E-11a-c, E-12a-c |

## 3. Weaknesses

Each item gives: ID, location, description, triggering SQL, targeting test, expected vs suspected behaviour, and confidence.

### Q-1: lookup-query rows lose 12 bytes when sent as a fragmented TRANSID_AI

**Confidence:** high, from code reading.

**Location:**
- `storage/ndb/src/ndbapi/NdbReceiver.cpp:1651`, in `NdbReceiver::execTRANSID_AI(const Uint32*, Uint32, const NdbApiSignal*, Uint32 totalLen)`.
- For comparison: `NdbQueryOperation.cpp:5959` (`withCorrelation = isScanQuery()`) and `NdbQueryOperationImpl::execTRANSID_AI(ptr,len)` near line 6806.

**What happens:**
1. After reassembling a fragmented row for an `NDB_QUERY_OPERATION`, the API strips 3 words when `AttributeHeader(*(end-3)).getAttributeId() == CORR_FACTOR64` (0xFFE8). This is a guess based on the data.
2. Only scan queries append a correlation. The non-fragmented path strips only `if (isScanQuery())`.
3. So a row of a pushed lookup query loses its last 12 bytes when the user data three words from the end has 0xFFE8 in its upper half. An INT in -1572864..-1507329 at that position is enough.
4. Rows are sent fragmented above 7400 words, or above 240 words (960 bytes) in VM_TRACE builds.

**Expected:** identical rows. **Suspected:** a wrong or truncated last column, or an assert or crash in unpack.

**SQL:** `drv(InnoDB) STRAIGHT_JOIN wp ON wp.id=drv.x STRAIGHT_JOIN wc ON wc.id=wp.fk`. wp is the root of a pushed lookup query. The VARBINARY payloads repeat `00 00 E8 FF` in all four rotations.

**Tests:** `ndb_spj_shapes_wide` W1-W4. W5-W6 are the scan-query controls. W7-W8 make the DBSPJ-side reassembly fragment too.

**Fix direction (not done):** strip only when `getQueryDef().isScanQuery()`, as the non-fragmented path does.

### Q-2 (= E-15): NULL-safe `<=>` ref keys lose NULL⇔NULL matches when pushed

**Confidence:** medium-high. It depends on the optimizer using ref access for `<=>`, which it normally does.

**Location:**
- `storage/ndb/plugin/ha_ndbcluster_push.cc`: `is_pushable_as_child` / `is_field_item_pushable` / `build_key`. No code in the file looks at the ref's `null_rejecting` bits.
- `DbspjMain.cpp`: `lookup_parent_row` ("Key contain NULL values: Ignore impossible KEYREQ", about line 10103) and `scanFrag_parent_row` ("Key contain NULL values, ignoring it", about line 12918). Both drop every parent row whose linked key is NULL.

**What happens:** MySQL's ref with `<=>` looks up the rows whose key IS NULL, while the pushed child returns nothing for them.

**SQL:** `SELECT p.id,c2.id FROM p STRAIGHT_JOIN c2 FORCE INDEX(ix_b) ON c2.b <=> p.b`, with NULLs on both sides; also the LEFT JOIN and composite variants.

**Expected:** NULL rows match. **Suspected:** they are missing (inner join) or NULL-extended (LEFT JOIN).

**Tests:** `ndb_spj_shapes_nulls` N17-N20; `ndb_spj_shapes_agg` E-15.

**Fix direction:** do not push a child whose ref has a non-null-rejecting key part.

### Q-3 (= E-5): GROUP BY values that are equal under the collation split into several groups

There are two causes.

**(a) Kernel.**
- **Confidence:** high for accent and trailing-space variants.
- **Location:** `storage/ndb/src/kernel/blocks/dbtup/AggHashTable.hpp:766` (`if (kl != key_len) continue;`) and `:783` (`if (ds1 != ds2)`), in `GBHashTable::findInBucket`.
- **What happens:** the key lengths are compared before the collation compare. 'grav007' (2 words) and 'gráv007' or 'grav007  ' (3 words) hash to the same bucket but never match, so the same data node keeps two groups.

**(b) API.**
- **Confidence:** medium-high on 2 nodes, for case variants too.
- **Location:** `storage/ndb/src/ndbapi/NdbAggregator.cpp:209`. `initForResults()`, used for pushed join aggregation by `NdbQueryImpl` around line 4269, never builds `gb_cmp_`. So `hashGroupKey`/`groupKeysEqual` merge the partial groups from different nodes, or from evictions, by their bytes.
- **What happens:** node A's 'grav007' and node B's 'GRAV007' stay two result rows.

**Expected:** one row per group, as MySQL returns. **Suspected:** more rows, with the counts split.

**SQL:** `SELECT MIN(d.id),COUNT(*),SUM(f.qty) FROM d JOIN f ON f.did=d.id GROUP BY d.nm`. Variants: utf8mb4_0900_ai_ci, utf8mb4_general_ci, latin1_swedish_ci, a leaf column, and single-table aggregation.

**Tests:** `ndb_spj_shapes_agg` E-5a-f.

### E-1: double constants in a pushed aggregation turn aggregate slot 0 into a different aggregate in the API merge

**Confidence:** high. Confirmed by code reading.

**Location:** `NdbAggregator.cpp:239-253`, in `initForResults`. It decodes every program word as an opcode, data words included.

**What happens:**
1. `LoadDouble` (`NdbAggregator.cpp:1184`) stores the 8 raw bytes of the double.
2. The high word of 1.0 (0x3FF00000) decodes as op 15 (`kOpSumDouble`) for agg_id 0. The high word of 2.0 (0x40000000) decodes as op 16 (`kOpMaxBigint`) for agg_id 0. Most round constants give agg_id 0.
3. `emit_expr` (`ha_ndbcluster_push_agg.cc` about line 371) loads integer constants as doubles in a DECIMAL context, so `1 - l.disc` hits this too.
4. When partial groups are merged, from 2 nodes or after evictions, a MAX in slot 0 is summed, or a SUM is max'ed.

**SQL:** `SELECT d.g, MAX(f.qty), SUM(f.price*(1-f.disc)) … GROUP BY d.g`; `SELECT d.g, SUM(f.qty), SUM(f.price*2) …`.

**Tests:** E-1a-f; E-1e is the integer-constant control.

### Q-8: odd number of root fragments on 2 data nodes

**Confidence:** high for the debug assert, low for any wrong result in release builds.

**Location:** `NdbQueryOperation.cpp:3840` (`assert((rootFragments % cnt) == 0)`, upstream since 2017), `:3841` (`m_fragsPerWorker = rootFragments / cnt`) and `:3868` (`assert(m_workerCount * m_fragsPerWorker == rootFragments)`, RonDB).

**What happens:**
1. Take `PARTITION BY KEY() PARTITIONS 5|7`. KEY partitioning is pushable. Primaries are split 3/2 or 4/3.
2. A debug mysqld aborts.
3. A release build runs with fragsPerWorker = floor(n / nodes). DBTC still bundles per node, and the root buffer reserve from e598c22d2ec rounds up, so the result is probably correct.

**SQL:** `SELECT po7.id,p.id FROM po7 STRAIGHT_JOIN p FORCE INDEX(ix_b) ON p.b=po7.b`.

**Tests:** `ndb_spj_shapes_variants` Q-8a-d. They are the last section of the file because they may crash mysqld.

### E-26: more than 16 key fields that refer to tables before the root fail the statement

**Confidence:** high.

**Location:** `ha_ndbcluster_push.cc:2439` (`m_fld_refs >= MAX_REFERRED_FIELDS → return -1`). From there, `build_query()` fails, and `make_pushed_join()` (about line 660) prints the error and returns it from `ndbcluster_push_to_engine`. The statement fails instead of running without a pushed join.

**SQL:** an InnoDB table `drv18`, then `r26` (a scan root), then two 10-part-PK children binding 9 key parts each to `drv18` columns, 18 references in total.

**Expected:** 30 rows. **Suspected:** an error from the ON run, or already from EXPLAIN. The check include reports either.

**Tests:** E-26, plus E-26-control with exactly 16 references.

### E-2: join-aggregation self-continuation does not reset a child scan's counters

**Confidence:** medium (coordinator's finding; my reading agrees).

**Location:** `DbspjMain.cpp`, `handleJoinAggNextBatch` (about line 4020) → `cleanupBatch(requestPtr, done=false)` → `scanFrag_parent_batch_cleanup(done=false)`. That function resets `m_corrIdStart`, `m_totalRows` and `m_totalBytes` only when `done`.

**What happens:** the counters keep growing over the self-continued batches. Once the limit is reached, `scanFrag_getBatchSize` returns 0, `scanFrag_send` skips the scan, and child rows are dropped.

**Tests:** E-2a-c, run twice; the second pass uses `max_4rows_in_spj_batches` in debug builds. Root is 60k rows, with 3 index-scan children each and a lookup leaf.

### Q-4: unconditional info log in the join-aggregation hot path

**Confidence:** high. This is not a result-correctness issue.

**Location:** `DbspjMain.cpp`, `handleJoinAggNextBatch`, about line 4022: `g_eventLogger->info("AGGT(%u) SPJ agg self-continue …")`. The comment marks it "TEMPORARY fs_batch phase-timing probe". All other AGGT probes are behind `DEBUG_AGGT`.

**Effect:** it writes one cluster-log line for every self-continued batch of every join-aggregation and CTE-phase query. E-2 alone produces thousands.

**Test:** none. Look at the data node logs after `ndb_spj_shapes_agg`.

### Q-5: inner join followed by LEFT JOIN, 3 tables, under join aggregation

**Confidence:** medium. It may already be fixed.

**Location:** known COUNT(*) mismatch. Tests 9 and 15 in `ndb_push_agg_dist/t/ndb_push_agg_dist_extended.test` (lines 233-257 and 348-376) are commented out for it.

**Test:** Q-5 (`reg JOIN d LEFT JOIN f GROUP BY reg.region`). It only prints `agg=`, because a small root may be skipped by the too-few-rows rule.

### Q-6: SUM beyond 64 bits and DECIMAL(38,6) sums under join aggregation

**Confidence:** medium.

**What happens:** MySQL sums BIGINT and BIGINT UNSIGNED into DECIMAL. The pushed aggregation sums in 64 bits; per testCteSumOverflow it fails with 1860 or wraps.

**Expected:** the exact DECIMAL. **Suspected:** an error 1860-style failure of the statement, or a wrapped value.

**Tests:** Q-6a (sum near 2^62 x 250), Q-6b (near 2^63, unsigned), Q-6c (DECIMAL(38,6) values about 1e25), Q-6d (AVG).

### Q-7: DOUBLE group key with -0.0 and 0.0

**Confidence:** medium-low; it depends on MySQL grouping -0 with 0.

**What happens:** the kernel's raw path compares with `memcmp`, and the API's binary hash and merge do the same, so -0.0 and 0.0 become different groups. MySQL's GROUP BY on a DOUBLE treats them as one.

**Tests:** Q-7a (leaf), Q-7b (root).

### Q-9: two session options never reach pushed joins

**Confidence:** high that the behaviour differs; low severity.

**Location:** `ha_ndbcluster.cc` around lines 3676, 3959 and 4093. `ndb_ring_buffer_show_meta` and TTL-ignore (`should_ignore_ttl()`) are passed only to NdbOperation and NdbScanOperation. NdbQuery has no such options.

**Effect:** with `ndb_ring_buffer_show_meta=ON`, a pushed join hides the ring-buffer meta rows that the same non-pushed query shows. The binlog applier (TTL ignore) does not use pushed joins, so only the diagnostic variable is affected.

**Test:** none.

### Coordinator findings with tests added

The analysis behind these is the coordinator's; each test requires the aggregation to be pushed.

| Finding | Test |
|---|---|
| E-3 | E-3a-c (lookup chain; 20k-row root, half the links dangling) |
| E-4 | E-4a/b (CASE over a NULL-extended row) |
| E-6 | E-6a/b (ON filter on the leaf) |
| E-8 | E-8a (4-table inner nest inside an outer join), E-8b (inner after outer, on the root); not pushed since 2026-11-03, the result must match |
| E-9 | E-9a/b (sibling lookups re-parented into a chain); E-9a not pushed since 2026-11-03, the result must match |
| E-10 | E-10a/b, not required to push |
| E-11 | E-11a-c (IN lists of 5, 20 and 40 values) |
| E-12 | E-12a-c (BETWEEN and IN-list root) |
| E-30 | E-30 (HAVING; `agg=` is recorded, no failure) |

## 4. Checked and found fine (code reading)

- **Scan pruning on PARTITION_HASH fanout tables.** DBSPJ drops the prune pattern (`parseScanFrag`, about line 12185). Lookups use `handle_partition_hash`. V-fanout covers this.
- **F14 change to NdbQueryBuilder const operands.** Only RonSQL and the root KEYINFO are affected. mysqld uses `constValue(ptr,len)`, which is the generic operand, plus params for the root. C1 and C2 cover this.
- **`scanFrag_fixupBound` renumbering rewrite.** It matches upstream for the EQ-only bounds mysqld produces.
- **New `m_rows` gate in `scanFrag_execSCAN_FRAGCONF`, which requires T_USER_PROJECTION.** mysqld always calls `setResultRowRef`, so PI_ATTR_LIST is always present.
- **Lock modes.** A table whose lock mode is not committed-read is never pushed (`maybe_pushable_join`). L4 and L5 verify that the locks are really taken.
- **TTL visibility.** DBTUP enforces it for SPJ reads too, because SPJ never sets TTL-ignore. DBSPJ `getNodes` reads backups only for READ_BACKUP tables, which matches the DBTC TTL rule. V-ttl and V-ttl_rb cover this.
- **Hash and partition-hash computation for character-set keys.** It goes through `xfrm_key_hash` / `xfrm_attr_hash` (upstream). The L-* checks cover it in all shapes.
- **Fragmented TRANSID_AI into DBSPJ.** `assembleFragments` is used, and `m_buffer1` is static_asserted to be large enough. W7 and W8 cover it.
- **Equality-propagated substitute keys of a different type.** Impossible, because MySQL builds Item_equal only for fields that are `eq_def`.

## 5. How to run

Run from `mysql-test/` in the worktree. Run first **without** `--record`. A failing test dies at the end. The last result lines, and `var/log/<test>.log`, list the `# FAILED` shapes with the differing rows.

```
./mtr --suite=ndb_opt ndb_spj_shapes_nulls ndb_spj_shapes_keys ndb_spj_shapes_outer \
      ndb_spj_shapes_fanout ndb_spj_shapes_wide ndb_spj_shapes_variants
./mtr --suite=ndb_push_agg_dist ndb_spj_shapes_agg
# the disabled tests, to check a fix:
./mtr --suite=ndb_push_agg_dist --enable-disabled ndb_spj_shapes_agg_exact
```

The tables of the two `ndb_spj_shapes_agg*` tests are created by
`suite/ndb_push_agg_dist/include/spj_shapes_agg_{tables,e3_tables,e2_tables}.inc`.

To record, once a test passes:

```
./mtr --record --suite=ndb_opt ndb_spj_shapes_nulls
./mtr --record --suite=ndb_opt ndb_spj_shapes_keys
./mtr --record --suite=ndb_opt ndb_spj_shapes_outer
./mtr --record --suite=ndb_opt ndb_spj_shapes_fanout
./mtr --record --suite=ndb_opt ndb_spj_shapes_wide
./mtr --record --suite=ndb_opt ndb_spj_shapes_variants
./mtr --record --suite=ndb_push_agg_dist ndb_spj_shapes_agg
```

- **Result file location:** the ndb_opt results land at the suite root, for example `mysql-test/suite/ndb_opt/ndb_spj_shapes_nulls.result`. The ndb_push_agg_dist result lands in its `r/` directory.
- **Output size:** one line per shape: `# <label>: rows=… pushed=… exec=…[ agg=…]`. Expected failures can be handed to the developers as they are. A shape that EXPLAIN shows as not pushed when it is required fails with "EXPLAIN shows N pushed table(s)" or "does not show 'Using pushed join aggregation'". To accept such a plan, set `$spj_min_pushed= 0` or `$spj_expect_agg= 0` before that check.
- **Runtime:**
  - `ndb_spj_shapes_agg` loads 60k + 180k + 20k rows and runs the E-2 reference queries without SPJ, so allow several minutes.
  - `ndb_spj_shapes_variants` waits 4 x 3 s for lock time-outs.
  - `ndb_spj_shapes_fanout` restarts the cluster with its `.cnf`.
- **Debug builds:** both mysqld and the data nodes should be debug builds. That makes `max_4rows_in_spj_batches` effective and lowers the Q-1 fragmentation threshold to 960 bytes.
