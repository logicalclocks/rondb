# WP-F — IN lists: primary-key lookups first, multi-range index scans where lookups are impossible (F23 / F12)

Written 2026-09-23 from census run 4 (`m3_plan.md` §6); revised the same
day for the user's rule: **an IN list on primary-key columns is served
by a set of primary-key lookups; a scan is used only where lookups are
not an option.** The engine work of M3 starts here; F27 (the node
failure after query-memory exhaustion) is investigated in parallel
(`m3_experiments.md` X4).

**Status: F1a, F2 (incl. the DBLQH fix for multi-range scans with
pushdown aggregation) and F1b (aggregation on primary-key reads, kernel
+ API + RonSQL) done 2026-09-23 — tests green in the user's run. Every
RonSQL query requires all data nodes >= 26.10.0. Open: the JIT mirror of
`ronsql_in_list_ranges` recorded a fallback delta of 96 (the lookup
mirror 0) — attribute it (residual scan filters are the likely source);
F3 (SPJ roots) next, then the census rerun for the §0 targets.**

## 0. Contract and targets

Hopsworks batch serving sends `WHERE <entity key> IN (k1, …, kn) GROUP
BY <entity key>` with 10–1000 keys (S3, S10 batch, the batch snowflake
body S7), and the entity-table reads of a vector with the same list.
Today every IN list, on any column, is a full table scan with the OR
chain as the per-row filter (run 4, T=1, sf 1, 4 LDM threads):

| entry | keys | column | mechanism after WP-F | today | MySQL nopush | target |
|---|---:|---|---|---:|---:|---:|
| `core_in_pk100` | 100 | complete PK | **100 PK lookups** | 398 ms | 0.5 ms | ≤ 1 ms |
| `fs_hw_agg_batch10` | 10 | PK prefix (`customer_id` of `(customer_id, event_time)`) | multi-range scan | 216 ms | 0.78 ms | ≤ 2 ms |
| `fs_hw_agg_batch100` | 100 | PK prefix | multi-range scan | 989 ms | 7.7 ms | ≤ 15 ms |
| `fs_hw_agg_batch100_window` | 100 + range on `event_time` | PK prefix + next column | multi-range scan | 981 ms | 6.5 ms | ≤ 15 ms |
| `fs_hw_agg_batch1000` | 1000 | PK prefix | multi-range scan | 8.73 s | 95 ms | ≤ 200 ms |
| `fs_hw_strkey_batch100` | 100 | VARCHAR PK prefix | multi-range scan | 743 ms | 11.3 ms | ≤ 15 ms |
| `core_in_idx100` | 100 | secondary ordered index, GROUP BY | multi-range scan | 403 ms | 4.9 ms | ≤ 10 ms |
| `fs_hw_snow1_batch100` | 100 | complete PK of `customers_1`, in a CTE body (SPJ root) | multi-range scan (lookups cannot feed a CTE body, §2.6) | 37.7 ms | 45 ms | ≤ 10 ms |

Why lookups first: a PK read routes each key to exactly the fragment
holding it — N `LQHKEYREQ`, batched per node in one round trip, work
proportional to N and independent of the fragment count. A multi-range
scan sends every range to every fragment: N × fragments probes plus a
`SCAN_FRAGREQ` per fragment, so its cost grows with the cluster
(30 fragments on the 15-LDM benchmark configuration) while a lookup
batch does not. Scans remain for lists on a PK *prefix* or a secondary
index, where no single key identifies a row, and for CTE bodies.

The cost model behind today's numbers: scan(table) + rows × keys × 2.4 ns
(the OR chain is evaluated linearly per row; the compiled interpreter
changes it by 1.12× at 1000 keys).

Semantics to preserve, all checked against MySQL by the tests of §3:
duplicates in the list count once; `NULL` in the list matches nothing;
a value outside the column's domain (a missing key) matches nothing; string
keys use the column collation; an IN list combined with other conjuncts
(equalities on the other PK columns, a range on the next index column,
filters on other columns) keeps its meaning; `NOT IN`, `IN (subquery)`
and any OR that is not a pure IN shape are untouched.

## 1. The mechanism as found

- **Parser** (`RonSQLParser.y:665`): `col IN (a, b, c)` is rewritten at
  parse time into the left-deep tree `((col = a) OR (col = b)) OR (col
  = c)`; nothing downstream knows it was an IN.
- **Single-table planner** (`RonSQLPreparer::plan_index_and_filter` →
  `detect_pk_lookup` → `collect_toplevel_conditions` →
  `build_scan_config_candidates`, `RonSQLPreparer.cpp:3395–3760`):
  `detect_pk_lookup` takes the lookup path only when every PK column has
  a plain equality (and only for pass-through queries: "a non-aggregate
  WHERE fully consumed as PK equalities"). Otherwise the WHERE is
  AND-flattened into top-level conjuncts; per ordered index the
  candidate builder consumes a conjunct as a bound only if it is
  `col <op> const` with `op` in `= >= > <= <` (`:3655`). The OR node is
  one conjunct with `op == T_OR` → "Condition unfit to serve as bound"
  → residual filter, no bound on that column, goodness 0, TABLE_SCAN
  wins.
- **Single-table PK lookup execute** (`:8136–8240`): a pass-through
  facility — RecAttr `readTuple` + `getValue`s, or an NdbRecord
  `readTuple` with the residual conjuncts as an `OO_INTERPRETED` filter
  program (capped at `LOOKUP_FILTER_MAX_WORDS`). Aggregation is not
  available on a plain `NdbOperation`: on the single-table path the
  aggregation program rides on a scan (`scanOp->setAggregationCode(&aggregator)`,
  `:7588`).
- **Aggregation on lookups exists in the SPJ path.** A pushed query can
  attach the aggregation feed to a `readTuple` operation
  (`leafOpts.setAggregation(*cteAgg); qb->readTuple(srcTab, keys, &leafOpts)`,
  `:8922`; the scalar-CTE lookup root with `rootOpts.setAggregation`,
  `:9788`), the kernel's join aggregation (RONDB-733) runs the program
  on lookup rows, and `query->getAggregator()` (`:9096`) holds the
  merged result. One `NdbQuery` has one root key.
- **Single-table scan emit** (`:7740–7880`): one range — `readTuples`
  without `SF_MultiRange`, old-API `setBound` per bound column,
  `end_of_bound(0)`; residual conjuncts through `NdbScanFilter`
  (`apply_filter`, `:12967`: an OR is `begin(OR)` + one `cmp` per
  disjunct, the linear chain).
- **SPJ roots** (`select_root_scan_config` `:4207`, `emit_index_scan_root`
  `:9561`): the same candidate builder; one def-time `NdbQueryIndexBound`
  in `qb->scanIndex(idx, tab, &bound, &rootOpts)`; the query is created
  once at `m_trans->createQuery(queryDef)` (`:9028`) and executed right
  after.
- **NDB API multi-range.** Single-table: `SF_MultiRange` on `readTuples`,
  old-API `setBound`s per range and `end_of_bound(range_no)`
  (`NdbIndexScanOperation.hpp:153–167`; `NdbScanOperation.cpp:1140`
  refuses a second range without the flag); `MaxRangeNo = 0xfff`. SPJ:
  `NdbQuery::setBound(const NdbRecord*, const IndexBound*)` after
  `createQuery`, sequential range numbers, no def-time bound on the
  root, equality ranges sent once as `BoundEQ`, per-bound KEYINFO
  ≤ 0xFFFF words (`NdbQueryOperation.cpp:2962`); the path `ha_ndbcluster`
  uses for pushed MRR roots (`:14787`).
- **Data nodes.** DBTUX serves a multi-range scan by probing each range
  in turn within one fragment scan; every fragment receives every range
  (no per-range pruning). Rows arrive per range and the aggregation
  program groups them as before. `Ndb::computeHash` (`Ndb.hpp:1764`) and
  `PartitionSpec` exist for pruning a scan to one partition when the
  bound covers the distribution key.
- **Tables.** The framework's feature-group tables have ordered PKs
  `(entity key, event_time)` with the full PK as the distribution key
  (no `PARTITION BY`), plus a hash-only twin (`transactions_hash_1`,
  `USING HASH`, no ordered PRIMARY index). Whether Hopsworks' online
  tables use `USING HASH` or partition by the entity key is to be read
  off the Hopsworks DDL (`m3_experiments.md` gets an X5 for it): it
  decides whether prefix lists can ever be pruned per fragment (§2.5)
  and whether the hash-only case is the common one (then lookups are the
  only mechanism for complete-PK lists, and prefix lists fall back to
  the filter until an ordered index exists).

## 2. Design

### 2.1 Recognise the IN shape

A top-level conjunct is *IN-shaped* when it is a T_OR tree whose leaves
are all `T_EQUALS` with `args.left` the same `T_IDENTIFIER` column and
`args.right` a bound-eligible constant (`T_INT`, `T_FLOAT`, `T_STRING`,
`I_MYSQL_TIME`, `I_SUBQUERY` after substitution). `match_in_shape(ce,
&col_idx, values)` flattens the tree (`flatten_or_disjuncts` exists for
the CTE_LOOKUP DNF; its `MAX_WHERE_CONJUNCTS` cap does not apply, the
cap is §2.4's) and returns the constants in list order. Anything else
(`a = 1 OR b = 2`, a `NULL` leaf, a non-constant leaf, a nested AND)
stays a filter as today.

The values are encoded with the column's `encode_constant` (the encoder
the equality bound and the PK lookup already use; a value it cannot
encode makes the whole conjunct a filter, decided at plan time so
EXPLAIN tells the truth), **sorted by encoded key order and
de-duplicated** (a duplicate would be read or scanned twice), and `NULL`
leaves dropped; all-`NULL` stays a filter.

### 2.2 Choosing the mechanism

Decided in `plan_index_and_filter`, before the scan candidates, in this
order:

| the IN column and the other conjuncts | mechanism | phase |
|---|---|---|
| every PK column is bound: one IN-shaped conjunct on one PK column and plain equalities on the others (a single-column PK is the common case) | **N primary-key lookups**, N ≤ `IN_LOOKUPS_MAX` (4095 to start; batched per `execute` in chunks if the transporter says so) | F1 |
| the IN column is the leading column of an ordered index (a PK prefix, or a secondary index), or follows equality-bound leading columns of one | **multi-range index scan**, one range per value, N ≤ `MaxRangeNo` | F2 (single table), F3 (SPJ roots) |
| a CTE body whose root would be lookups | multi-range scan on the ordered PRIMARY index: a CTE body is one query feeding the materialisation, N lookup queries cannot (§2.6) | F3 |
| none of the above (unindexed column, hash-only PK with a prefix list, more values than the cap, a second IN conjunct) | filter, as today; F4 turns the linear chain into a branch tree | F4 |

`detect_pk_lookup` is extended rather than bypassed: "every PK column
has an equality" becomes "every PK column has an equality or exactly one
of them has an IN-shaped conjunct", and the lookup arm applies to
aggregate queries too (§2.3). The aggregate lookup is new; the
pass-through lookup generalises the existing one.

### 2.3 N primary-key lookups (F1a, F1b)

**Finding of the F1 research (2026-09-23).** Aggregation on a
primary-key read does not exist below RonSQL: the kernel runs the
aggregation interpreter only under the scan protocol
(`ScanTabReq::setAggregation` / `ScanFragReq::getAggregationFlag`;
`TcKeyReq` has no such flag), the join-aggregation feed on lookups is
driven by DBSPJ under a scan root (`JOIN_AGG_SETUP` lives on the
SCAN_TABREQ path), and `NdbQueryBuilder` rejects a lookup-rooted
aggregate query with `QRY_WRONG_OPERATION_TYPE` ("an aggregate query
must use the scan protocol", `NdbQueryBuilder.cpp:1800`); `NdbQueryOptions::setAggregation`
deep-copies the program and every `NdbQuery` creates its own
`NdbAggregator`, so N queries could not share one either. Hence:

- *Pass-through* complete-PK lists → N PK lookups now (RonSQL only,
  F1a below).
- *Aggregate* complete-PK lists → true PK lookups need **F1b, a kernel +
  API package**: a TCKEYREQ/LQHKEYREQ aggregation flag with the program
  in the ATTRINFO section, DBTUP running `AggInterpreter` on the one
  tuple and returning the aggregation record as the TRANSID_AI result,
  DBTC passing the flag through, `NdbOperation::setAggregationCode` on
  the API and the receiver feeding the record into an `NdbAggregator`
  shared by the N operations (the aggregator already merges per-fragment
  partials, so N per-operation partials are the same thing). Until F1b
  lands, the aggregate complete-PK list is served by the multi-range
  scan on the ordered PRIMARY index (F2) — the "scan is the only option"
  clause — and F1b's benefit is measured against that (`core_in_pk100`,
  `core_in_pk1000`). Per-key *pruned single-range scans* are not an
  alternative: each is a scan (`MaxNoOfConcurrentScans` per TC caps a
  batch at 256, setup cost per scan), so they lose to the multi-range
  scan above a few keys.

*Pass-through* (`SELECT cols … WHERE pk IN (…)`, optional residual
conjuncts): the existing single-row arms, repeated N times in one
`NdbTransaction` and one `execute(NoCommit)`: per key one
`getNdbOperation` / `readTuple(LM_CommittedRead)` with the key columns
`equal`ed (the IN value substituted on its column) and the
`register_passthrough_getvalues` reads; with residual conjuncts the
NdbRecord arm with the `OO_INTERPRETED` filter program built once and
shared by the N operations. Misses (a key not in the table) come back
as error 626 on that operation and print nothing. Rows print in
ascending key order (the sorted value list), which the tests fix as the
contract; a pass-through ORDER BY takes the existing client-side sort.

*Aggregate* (`SELECT COUNT(*), SUM(…) … WHERE pk IN (…) [GROUP BY pk]`):
the plain `NdbOperation` has no aggregation feed, so the lookups are
emitted as SPJ lookup roots — the mechanism `emit_root_op` already uses
for an aggregated single-row root: per key one `NdbQueryBuilder`
definition with a `readTuple` root carrying `setAggregation(agg)` and
the residual conjuncts as its interpreted code, `createQuery` for each,
one `execute`. The N queries share one `NdbAggregator`, so the kernel's
per-row aggregation records from every lookup land in one group table
exactly as the per-fragment records of one scan do; `GROUP BY pk` makes
each key its own group. **Research item for the F1 diff:** whether one
`NdbAggregator` may be attached to N queries (its receiver-side state,
`Finalize` once) — if not, the alternative is a per-query aggregator
merged through the multi-leaf merge (`leafAggs`, `:8616`), or the API
extension that gives `NdbOperation` the scan's `setAggregationCode`.
The per-query cost of `NdbQueryBuilder` + `createQuery` (tens of µs)
matters at N = 1000: measured in F1; if a batch of 1000 lookup queries
costs more than the pruned scan of §2.5, `IN_LOOKUPS_MAX` moves down
and the scan takes over above it — the user's rule holds where lookups
are the better mechanism, and the numbers say where that ends.

Types: the IN values go through `encode_constant` for the PK column
(ints at column width, padded CHAR, length-prefixed VARCHAR, packed
temporals), the same bytes the single-row lookup writes into the key
row today.

### 2.4 The range set (F2, F3)

For an IN-shaped conjunct consumed as an equality bound with N values
by the candidate builder (`build_scan_config_candidates`, `:3655`:
`wants_lbound = wants_ubound = true`, `later_columns_blocked = false`,
`condition_handling_map[i]` = the column, the candidate records the
conjunct as its multi-value one; one such conjunct per candidate, a
second stays a filter; scoring = equality points × 0.9 so a candidate
binding the same columns with plain equalities wins a tie), each range
repeats the other bounds — the equalities on earlier index columns and
the optional half-open range on the column after the IN column
(`batch100_window`: range k = `(customer_id = k, event_time >= X)`).
Cap `IN_RANGES_MAX` = `MaxRangeNo` (4095), lowered if the KEYINFO tests
of §3 say so. `ScanConfig` gains `in_cond_idx`, `in_values`,
`num_ranges` (the class has default member initialisers, so the
existing aggregate initialisations keep working); the struct is shared
by the single-table path (`m_scan_config`) and the SPJ roots
(`scope.body_scan_config`).

### 2.5 Multi-range scan, single table (F2)

`open_single_table_scan_op`: add `SF_MultiRange` to the scan flags and
loop `r = 0 … num_ranges − 1`: the bound `setBound`s in index-column
order with `in_values[r]` substituted, the NULL-excluding low bound for
a nullable high-only last column inside the loop (it belongs to every
range), `end_of_bound(r)`. The residual filter is unchanged minus the IN
conjunct. `SF_OrderBy` (pass-through ORDER BY by index order) with
multiple ranges is excluded in F2 (not a Hopsworks shape; F4 lifts it
with its own test).

*Per-fragment pruning (F5, from the numbers):* when the IN column(s)
are exactly the table's distribution key, group the values by partition
(`Ndb::computeHash` per value) and issue one `SF_MultiRange` scan per
partition with a `PartitionSpec`, each carrying only its own ranges —
lookup-like scaling for prefix lists. With the framework's tables
(distribution key = full PK incl. `event_time`) this does not apply;
it applies if Hopsworks partitions by the entity key (§1 last bullet).

### 2.6 SPJ roots: CTE bodies and join roots (F3)

`emit_index_scan_root` cannot pass N ranges at definition time: define
the root with `scanIndex(idx, tab, /*bound=*/NULL, &rootOpts)`, keep the
range set on the scope, and between `m_trans->createQuery(queryDef)`
(`:9028`) and `execute` call `query->setBound(idx->getDefaultRecord(),
&ib)` per range (`ib.range_no = r`, `low_key == high_key` pointing at
one key buffer laid out per the index's default `NdbRecord` —
`getOffset` / `setNull` / `getRecordRowLength` — both inclusive, so the
API sends one `BoundEQ` per column; the windowed shape uses distinct low
and high buffers with `high_key_count` one less). The residual filter
stays on `rootOpts`. `m_prunability` becomes `Prune_Unknown`, which an
unpruned multi-range scan is.

A CTE body with a complete-PK IN list (`fs_hw_snow1_batch100`:
`customers_1 WHERE customer_id IN (…) GROUP BY customer_id, region_id`)
would prefer lookups by the rule, but the body is one pushed query
whose root feeds the CTE materialisation; N lookup queries feeding one
CTE is not something the RONDB-1120 machinery does. The multi-range
scan on the ordered PRIMARY index is the option there, and is what §0
targets. A main-query join root with a complete-PK IN list is the same
situation today (one query tree per root key); N trees in one
transaction, merged, is F5 territory.

### 2.7 EXPLAIN

Lookups: `Execute as N primary key lookups (IN list on \`col\`, sorted,
deduplicated).` with the key conjuncts under `KEYS`. Scans: `Execute as
index scan.` as today plus `Ranges: N (IN list on \`col\`, sorted,
deduplicated)`, the IN conjunct printed under `CONDITIONS` as
`INDEX[k] (N values): …`. Fallbacks: `FILTER: … (IN list of N values not
used: <reason>)` with reasons `exceeds IN_LOOKUPS_MAX` / `exceeds
IN_RANGES_MAX`, `ORDER BY index order`, `second IN list`, `value not
encodable`. The join / CTE plan printer: `Body root: INDEX_SCAN using
PRIMARY, N ranges`. These strings are what the framework pins.

### 2.8 What does not change

The aggregation program and its kernel semantics (checked SUM, AVG,
MIN/MAX collations), GROUP BY, the API-side partial merge, the result
printer, the JIT (it gains reusable programs: the keys leave the
program), CTE_LOOKUP filters (Phase I.2 DNF, keyed from parent rows),
`NOT IN`, `IN (subquery)` (the I_SUBQUERY substitution yields one
constant list, so it becomes IN-shaped for free), every OR that is not
an IN shape.

## 3. Phases

### F1a — primary-key lookups for pass-through complete-PK IN lists (single table, RonSQL only)

`detect_pk_lookup` accepts one IN-shaped conjunct on a PK column (plain
equalities on the others); the execute arm issues N `readTuple`s in one
transaction with `AO_IgnoreError` (a missing key is a normal empty
result on that operation, not an aborted batch), prints the rows that
came back in the list order after de-duplication, honours LIMIT; a
pass-through ORDER BY keeps the scan path in this cut (the scan arm
owns the client-side sort). EXPLAIN: `Execute as N primary key lookups
(IN list on \`col\`).`

### F1b — aggregation on primary-key reads (kernel + API) — written 2026-09-23

As built (design from the code-path research of 2026-09-23):

- **Protocol.** A committed interpreted primary-key read carries the
  finalized `NdbAggregator` program after its five interpreted sections
  (the scan layout) and an aggregation-read flag: `TcKeyReq` attrLen
  bit 16 (long TCKEYREQ only; `getAggReadFlag`/`setAggReadFlag`),
  forwarded by DBTC (`CacheRecord::m_agg_read`, `sendlqhkeyreq`) as
  `LqhKeyReq` attrLen bit 15 (`SI_AGG_READ_SHIFT`; the dead short-request
  `SI_ATTR_LEN` field narrowed to 15 bits). Printers show `agg_read`.
- **DBLQH** (`execLQHKEYREQ`): `TcConnectionrec::m_agg_read` from the
  flag on every key request; refused with 1868 (`ZAGG_WRONG_OPERATION`)
  unless ZREAD, dirty, interpreted, and not join aggregation.
- **DBTUP**: `KeyReqStruct::m_agg_read`; `handleReadReq` refuses a
  non-interpreted aggregation read; the lookup arm of
  `interpreterStartLab` calls `handleAggReadRow` after the residual
  filter accepted the row: locate the program from the raw section
  lengths, validate magic / bounds (1868), build a one-row interpreter
  with the new non-fatal `PushdownInterpreterFactory::CreateAggForRead`
  (1870 on failure; JIT only for reusable programs, a cache hit), reset
  `read_length`, `ProcessRec` (its error is the read's error),
  `PrepareAggResIfNeeded(force)`, bounded teardown (at most one group),
  and `sendReadAttrinfo` of the record — one TRANSID_AI per accepted row,
  routed like any read reply. A missing / filtered / expired row never
  gets there (626 TCKEYREF).
- **NDB API**: `OperationOptions::OO_AGGREGATION` (0x1000000) with
  `aggregationCode`, appended last; an options struct of the previous
  size is still accepted (copied, without the option). Allowed on
  `ReadRequest` + `LM_CommittedRead` through the primary key only, with
  no other values read (read-mask columns, blobs, OO_GETVALUE,
  OO_GET_FINAL_VALUE, OO_LOCKHANDLE → 4574), a finalized program (4560)
  for the same table (241), and every data node >= 26.10.0
  (`ndbd_support_pk_read_aggregation` → 4575, like 4562 for scan
  aggregation; old data nodes would ignore the flag). The operation gets
  an AGG RecAttr (`getAggregationResult()`); the builder sets the
  interpreted flag and the 5-word header even without a filter, skips
  the AGG RecAttr in the read list and appends the program; short
  requests are refused; `NdbReceiver::unpackRow` hands an operation's
  record to that RecAttr (marker word kept for `ProcessRes`).
- **RonSQL**: `detect_pk_lookup` admits an aggregate when an IN list
  binds a PK column (a single-key aggregate stays a pruned index scan, so
  no existing plan changes); `execute_pk_lookup_aggregate` issues N reads
  with OO_AGGREGATION (+ OO_INTERPRETED for residuals) sharing one
  finalized reusable aggregator, `ProcessRes` per successful read,
  NoDataFound skipped, `PrepareResults`, printed by the ResultPrinter (so
  GROUP BY / ORDER BY / LIMIT behave as on the scan path). A cross-key
  64-bit SUM overflow (`ProcessRes` → -1860) and every other read error
  go through `throw_key_read_error`, which classifies the read's own
  error (the transaction error may be another read's 626) and logs it as
  the exception investigation does. EXPLAIN: `Execute as N aggregating
  primary key lookups (IN list on \`col\`: T values, N distinct).`
- **Tests**: `ronsql_in_list_agg_lookups` (+ `ronsql_jit` strict mirror):
  scalar / GROUP BY key / GROUP BY non-key / residual filter / all
  missing / string MIN-MAX + AVG / composite PK / hash-only PK / VARCHAR
  PK / ORDER BY alias + LIMIT / single key stays a scan / 1860 overflow
  across keys (CLI and HTTP) and recovery. `ronsql_in_list_ranges`:
  its two complete-PK aggregate cases now pin the aggregating lookups;
  its 501-range case moved to a (id, sub) key to stay a PK-prefix
  multi-range scan.

Original sketch:

The package sketched in §2.3. Scope: `TcKeyReq` / `LqhKeyReq` flag,
DBTC pass-through, DBLQH/DBTUP running the aggregation program on the
looked-up tuple (the interpreter and the record format exist; the
lookup-path plumbing does not), the API `NdbOperation::setAggregationCode`
and receiver → `NdbAggregator::ProcessRes`, then RonSQL's N-operation
aggregate arm. Tests at every level (testDict/testNdbApi-style API
test, MTR). Benefit: complete-PK aggregate lists (and single-row
aggregates) scale with N instead of with the fragment count; measured
against the F2 multi-range scan on `core_in_pk100` / `core_in_pk1000`.

### F2 — multi-range index scans for prefix and secondary-index lists (single table)

**Kernel finding (2026-09-23).** A multi-range scan with pushdown
aggregation did not work in the data nodes. DBLQH runs a multi-range
scan as one DBTUX scan per range (`accScanCloseConfLab` restarts the
next range while `primKeyLen` holds unread bounds). With aggregation on,
the end of every range reached the aggregation flush
(`Dbtup::SendAggResToAPI` from the `fragId == RNIL` branch of the scan
loop), so a scalar aggregate would have been emitted once per range —
its accumulators are not reset by the flush — and then the next range's
start failed the pushdown state-machine `ndbrequire(!m_has_pushdown ||
scanState == SCAN_FREE)` in `continueAfterReceivingAllAiLab`: a data
node failure on the second range. RonSQL had carried `SF_MultiRange` on
single-range aggregate scans until RONDB-983 without ever sending two
ranges; the MySQL handler's pushed aggregation issues one scan per range
(run 3's "313 scan batches per request"). The fix (two DBLQH hunks,
`DblqhMain.cpp`): flush only when no further range follows (the exact
negation of `accScanCloseConfLab`'s "another range" test), and let the
next range of a pushdown scan start from `WAIT_CLOSE_SCAN`. The
aggregation interpreter is created lazily on the first row and lives on
the scan record, so it carries across ranges; the GROUP BY mid-scan
batch flush drains the whole group map, so partial flushes inside a
range are unaffected. No per-feature version gate: RonSQL's
single-table IN-list ranges and this fix ship together, and no stable
release contains a RonSQL that sends a multi-range aggregate scan. Instead
(the user, 2026-09-23) every RonSQL query requires all data nodes to run
26.10.0 or later: `NDBD_RONSQL_MIN_VERSION` / `ndbd_support_ronsql`
(`ndb_version.h.in`), checked in the `RonSQLPreparer` constructor right
after `configure()` against `Ndb::getMinDbNodeVersion()`; below it (or
with no data node connected) the query is refused with a 503-class
error (`RonSQLErrorClass::RESOURCE`) — during a rolling upgrade to 26.10
RonSQL is unavailable until the last data node is upgraded.

Written 2026-09-23: `RonSQLPreparer.{hpp,cpp}` (`ScanConfig` IN fields,
`dedup_in_values` shared with F1a, the candidate builder, the ORDER BY
exclusion, the per-range emit, EXPLAIN `Ranges: N (IN list on \`col\`: T
values, N distinct; SF_MultiRange).`), `DblqhMain.cpp`,
tests `ronsql_in_list_ranges` + JIT mirror (strict), `fsq/cases/bench.go`
pins + golden (`fs_hw_agg_batch*`, `strkey_batch100` → index scan with
`SF_MultiRange`).

Files: `RonSQLPreparer.hpp` (`ScanConfig` fields), `RonSQLPreparer.cpp`
(candidate builder, range set, the single-table emit, EXPLAIN).

Tests: `ronsql_in_list_ranges.test` (composite PK `(k, ts)`, a secondary
index, a VARCHAR PK prefix, a nullable indexed column: IN on the leading
PK column with GROUP BY, IN + range on the next column, IN on a
secondary index, duplicates / `NULL` / out-of-domain / descending order,
mixed-case strings, IN on an unindexed column (filter), two IN
conjuncts, `FORCE INDEX` / `IGNORE INDEX`, 4096 values, EXPLAIN) and
its JIT mirror; `ronsql_fs` green (the S3 / S10-batch templates compare
strictly with MySQL).

Evidence: `fs_hw_agg_batch10 / 100 / 1000 / 100_window`,
`fs_hw_strkey_batch100`, `core_in_idx100` against §0; `scanPins` in
`fsq/cases/bench.go` flip to `Execute as index scan.` + `Ranges:`, golden
dump regenerated; `ndbinfo.jit` compiles per request → 0.

### F3 — SPJ roots (CTE bodies and join roots) by multi-range scan

Files: `RonSQLPreparer.cpp` (`emit_index_scan_root`, the
post-`createQuery` bound loop, scope state for the range set, the join
plan printer).

Tests: a `ronsql_cte` body include with IN lists on the body's leading
PK column and on a secondary index (`body_index.inc` pattern), a
snowflake with an IN-list body, an IN list on a main-query join root;
JIT mirror; `ronsql_fs` green.

Evidence: `fs_hw_snow1_batch100` ≤ 10 ms, pin `Body root: INDEX_SCAN
using PRIMARY`; the S7-batch templates.

### F4 — residual IN lists as a branch tree; ORDER BY by index order

For an IN-shaped conjunct that stays a filter, emit an
`NdbInterpretedCode` binary search over the sorted constants
(`branch_col_lt` / `branch_col_eq` with labels) instead of the linear
`NdbScanFilter` OR — O(log n) per row; lift the §2.5 ORDER BY exclusion
with an ordered multi-range test. Decided by the F1 / F2 numbers for the
fallback shapes.

### F5 — from the numbers

Per-partition pruned scans for prefix lists on tables partitioned by the
IN column (§2.5); N lookup trees for a main-query join root; a cross
product for two IN conjuncts; batching of lookups above the transporter
comfort size.

## 4. Verification (user-run, per phase)

```
cd debug_build && make -j ronsql_cli rdrs2 ndbmtd mysqld     # or the full build
cd mysql-test
./mtr --suite=ronsql ronsql_in_list_lookups --record          # F1, first time
./mtr --suite=ronsql,ronsql_jit ronsql_in_list_lookups
./mtr --suite=ronsql_fs,ronsql_fs_jit,ronsql_fs_ng2r2 --parallel=4
# framework: pins and golden dump (F2 onwards)
cd ../../tools/rondb-cli && go test ./internal/fsq/cases -run TestBenchRegistryGolden -update && go test ./internal/fsq/... ./internal/shell
# numbers, on the benchmark computer's kept cluster (m3_experiments.md X0)
rondb --mysql-port <p> --rdrs-port <r> --no-rondis -e ".bench_ronsql core_in_pk100 1 500" -e ".explain_ronsql core_in_pk100"
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py --build prod_build \
    --queries core_in_pk100,core_in_pk1000,core_in_idx100,fs_hw_agg_batch10,fs_hw_agg_batch100,fs_hw_agg_batch1000,fs_hw_agg_batch100_window,fs_hw_strkey_batch100,fs_hw_snow1_batch100 \
    --no-start --no-load --mysql-port <p> --mysql-sock <s> --rdrs-port <r> --connectstring <c> --threads 1,8 --seconds 10 --out /tmp/wpf
python3 storage/ndb/claude_files/compiled_interpreter/ronsql_bench_triage.py /tmp/wpf --baseline storage/ndb/claude_files/fs_ronsql/bench_results/2026-09-22-benchbox-run4
```

Acceptance for the package: the §0 targets met on the benchmark
computer (15-LDM configuration); pins flipped and recorded; the three
`ronsql_fs` suites green ×3; `.fs_verify --requirements --all --vectors`
unchanged (R-S3 / R-S10 SUPPORTED); the two fuzzers without new
unclassified failures; `findings/bench.md` F23 (and F12) closed with
the run numbers in `benchmarks.md` §8.

## 5. Risks and open questions

- **One `NdbAggregator` for N lookup queries** (§2.3): the first thing
  F1 establishes; the fallbacks are named there.
- **Per-query overhead at N = 1000 lookups** (`NdbQueryBuilder`,
  `createQuery`, N receivers): measured by `core_in_pk1000`; sets
  `IN_LOOKUPS_MAX` and the point where the scan takes over.
- **Transporter batch size**: 1000 `LQHKEYREQ` with interpreted
  programs in one `execute` — the send buffer and `MaxNoOfConcurrentOperations`
  limits; chunked execution if a limit shows.
- **KEYINFO size** for scans: 1000 INT ranges ≈ 4 k words, VARCHAR(100)
  ≈ 30 k words in one fragmented long signal; the 4096-value and the
  1000-value VARCHAR tests are the proof; the cap moves down if not.
- **Per-fragment probing** for scans: 1000 keys × 30 fragments ≈ 30 k
  probes ≈ 30 ms — inside the 200 ms target; F5 pruning removes it where
  the partitioning allows.
- **Hopsworks DDL** (hash-only PKs? partition by entity key?): decides
  how much of Hopsworks batch serving is the lookup case and whether
  prefix lists can be pruned (`m3_experiments.md` X5).
- **String collations**: the equality bound and the lookup key encoder
  already handle VARCHAR; mixed-case values in the tests cover it.
- **SPJ `setBound` and `Prune_Unknown`**: confirm all fragments run (row
  counts vs MySQL in the F3 tests).
