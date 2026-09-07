# Single-group CTEs: GROUP BY key equality-bound (the fs_point shape)

**Status: G1 + G2a + G3 IMPLEMENTED (September 2026, pending user
build + Test 28 + first --record of the sg family ×5); G2b re-scoped
to a deferred protocol decision; G4-G5 pending.**

G3 outcome notes: RonSQL classification `is_single_group_cte_body`
(grouped body, not single-row, every GROUP BY column resolved and
equality-bound to a constant in the ORIGINAL body WHERE — immune to
plan-time conjunct consumption into index bounds; constant set =
single-row's T_INT/T_FLOAT/T_STRING/I_MYSQL_TIME/I_SUBQUERY; AND-spine
walk only, both literal orders, bare/qualified spellings unified via
same_resolved_column) + `where_binds_column_to_const` helper.
defineCte OR-composes CTE_SINGLE_GROUP with the single-row and LIMIT
flags; EXPLAIN prints `[single-group body]` in the CTE-definitions
section via the same helper.  Classification is purely an
optimization: false negatives keep the grouped pipeline, a false
positive would fail cleanly with 1273.  MTR
`body_single_group_cte.inc` (sg-1..10 + controls sg-c1..c3, wrappers
×5 topology suites): fs_point native + EXPLAIN tag pin, INNER/LEFT
consumers with hit+miss, empty-group INNER/LEFT (srb-20 semantics
family), pass-through CTE_SCAN root, multi-GB-column all-bound,
AVG-in-body, ORDER BY/LIMIT composition, the G2a repeated-miss
fan-out (300 probes against an empty CTE), and the
correctly-NOT-flagged controls (partial bound with EXPLAIN absence
pin, inequality, OR).

G2 outcome notes — the audit re-scoped it honestly:
- **The dormant row cache CANNOT be populated without a protocol
  extension.**  CTE_LOOKUP result rows are FLUSH_AI'd from DBLQH
  straight to the API (`cteLookupEmitResult` final-read routing);
  DBSPJ receives only CONF/REF and residual bookkeeping TRANSID_AI —
  there is no row payload to cache.  The skeleton
  `cte_lookup_serve_cached_row` was also doubly flawed: it replayed
  the cached bytes with the ORIGINAL probe's correlation (a replayed
  row must carry the CURRENT parent's correlation for API join
  assembly), and its async self-TRANSID_AI held no outstanding count,
  racing batch completion.  The function and its never-taken branch
  are REMOVED; `m_cachedRowPtrI/Len` stay reserved.  **G2b (deferred,
  maintainer decision)**: a CACHE_FILL flag on CteLookupReq making
  DBLQH dual-ship the API payload to DBSPJ on the fill probe, plus
  DBSPJ impersonating the per-probe API delivery — the riskiest
  surface (receiver ids, correlation rewrite), only worth it if
  repeated-HIT probe workloads profile as material.
- **G2a (shipped): the MISS outcome is cacheable today.**  A miss
  arrives as CTE_LOOKUP_REF with the probe's correlation, needs no
  payload, and serving a repeated miss is a pure local skip — no send,
  no counter movement (the probe never incremented outstanding), with
  the REF arm's one side effect (outer-join agg-feed NULL-row
  injection via the scan ancestor's buffered row) replicated at the
  serve site.  One slot per CteContext: `m_cachedKeyPtrI/Len` + fill
  tree node + fill correlation + `CacheKind`
  (NONE/FILLING/MISS/ROW_EXISTS).  The first eligible probe claims the
  slot (FILLING); a GROUP_NOT_FOUND REF matching (tree node,
  correlation) makes it MISS; byte-identical keys from the same tree
  node are then served in `cte_lookup_send` after key
  expansion/stamping.  A CONF while FILLING parks the slot as
  ROW_EXISTS (terminal — CONF carries no correlation, so it cannot be
  attributed; other keys' misses lose only the optimization).
  Eligibility = (single-row || single-group) CTE, no
  `T_ATTRINFO_CONSTRUCTED` (a filter with parent linked operands makes
  outcomes row-dependent, and DBLQH maps filter-reject to
  GROUP_NOT_FOUND), no OUTER_CHAIN protocol (its miss is a row +
  CONF), per-tree-node slot ownership (nodes can carry different
  constant filters).  Test 28's seed extended with repeated grp-8
  misses (COUNT pinned identically whichever probe order wins the
  slot).

G1 outcome notes: `QN_CteSubtreeNode::CTE_SINGLE_GROUP` (0x4) →
`JoinAggSetupReq::CTE_SINGLE_GROUP_FLAG` (bit 28) → DBTC encode →
DblqhProxy decode + strategy-mask extension →
`JoinAggregationState::m_cte_single_group`.  Constant-owner arms
extended at all four seams: redistribute destination
(`m_cte_single_row || m_cte_limit || m_cte_single_group`), DBSPJ
grouped-probe routing (the limitCte branch), the debug ROUTE_FLAG
guard, and the three violation checks (redistribute entry, owner-side
checkCteReady, single-node COMPLETE arm) with new
`ZCTE_SINGLE_GROUP_VIOLATION` (1273).  ndberror.cpp gains BOTH 1273
and the previously-missing 1272 message.  Block test
testCteNdbApiFilter **Test 28**: (a) body root-scan filter `grp = 7`
over a 4-row mixed seed → one group, flag set, main probes hit ×3 +
miss ×1 → COUNT=3 (constant-owner redistribute + probe routing +
empty-at-owner miss); (b) the same body unfiltered (two groups) under
the flag → clean 1273 failure, zero rows, covering both check layers
(one-node two-group entry check; cross-node owner-side check on
distributed runs).

Maintainer direction: the
architecture already fast-paths CTEs without GROUP BY (one-node
materialization, no group-hash redistribution, DBSPJ-side result
caching); extend that eligibility to bodies whose GROUP BY key carries
an equality bound on that key — provably at most ONE group — dropping
two lifecycle phases.  The target workload is `fs_point`:

```sql
WITH cust_features AS (
  SELECT o_custkey AS k, COUNT(*) AS order_cnt, SUM(o_totalprice) AS total_spend,
         MIN(o_orderdate) AS first_order, MAX(o_orderdate) AS last_order
  FROM orders WHERE o_custkey = {KEY} GROUP BY o_custkey)
SELECT MAX(order_cnt), MAX(total_spend), MIN(first_order), MAX(last_order)
FROM cust_features;
```

`GROUP BY o_custkey` + `WHERE o_custkey = {KEY}` ⇒ the group key is a
constant ⇒ the CTE materializes at most one group, but today it runs
the FULL grouped pipeline: per-thread hash tables on every node,
thread-merge, group-hash redistribution to the key's home node,
FINAL_REP barrier, then a CTE_SCAN of EVERY node's (empty, except one)
state.  For a point lookup that is almost all overhead.

## What exists today (audited)

Three related mechanisms, one of them unfinished:

1. **Scalar CTEs (no GROUP BY, I.17/I.17e)**: aggregation states on
   all nodes; per-node partials ship to the CONSTANT owner (the DBTC
   node) via the `keyLen == 0` JoinAggRedistributeReq variant +
   `mergeScalarAccumulators` — a trivial one-record-per-node
   redistribution, no hashing.  Consumers: `cteScanShouldEmitScalar`
   makes only the owner emit; scalar probes stay local.
2. **Single-row CTEs (CTE_SINGLE_ROW)**: key-only group records with
   ZERO aggregate slots, constant owner, normal (but ≤1-record)
   merge→redistribute→FINAL_REP flow, `singleRowCteProbe` on the
   owner.  The constant-owner arm in `continueJoinAggRedistribute`
   (DblqhMain.cpp:22240) already fires for
   `m_cte_single_row || m_cte_limit` — adding a bit is a one-line
   extension.
3. **DBSPJ row cache — DORMANT**: `CteContext::m_cachedRowPtrI /
   m_cachedRowLen` (Dbspj.hpp:769), the serve path
   `cte_lookup_serve_cached_row` (constructs TRANSID_AI from the
   cached section, skipping the CTE_LOOKUP round-trip to the owner's
   DBLQH), the CTE_READY branch that prefers the cache
   (DbspjMain.cpp:6531), and the release sites all EXIST — but
   nothing in the tree ever POPULATES the cache.  It is a skeleton in
   the same sense the CTE_SINGLE_ROW wiring was before its kernel
   plan: the read side is finished, the write side was never built.

So "extend the eligibility" decomposes into: (a) classify the new
shape, (b) route it onto the constant-owner machinery, and (c) FINISH
the dormant cache and let both the new shape and the existing
single-row/scalar shapes benefit.

## Semantics

A CTE body is a **single-group CTE** when every GROUP BY column is
bound by equality to a CONSTANT in the body's WHERE (constants include
scalar-subquery substitutions, the fs_point `{KEY}` case).  Then:

- The group's key is KNOWN AT PREPARE TIME.  Nobody needs the group
  hash to find its home; any constant owner is as good as the "right"
  hash node because every consumer can be routed there too.
- At most one group exists; zero groups when no row matches (probe
  misses / LEFT NULL-extends / empty CTE_SCAN — the srb-20 semantics
  family, already exercised by LIMIT 0 and single-row-absent tests).
- Aggregate slots are REAL (unlike single-row's zero-slot records) —
  this is closest to the scalar path but with one GB key column, so
  probes carry a key that must still compare (a probe with a
  different key must miss; RonSQL only emits equality-bound
  consumers, but the kernel compare stays as defense).

## The two dropped phases

1. **Group-hash redistribution → scalar-style direct ship.**  Today
   each node hashes its (single) merged group and sends it to the hash
   home node.  With the flag: per-node partials go straight to the
   constant owner exactly like the scalar `keyLen == 0` variant —
   except the record carries the GB key entry, so the owner's normal
   grouped merge handles it (the existing keyed REDISTRIBUTE path with
   a constant destination — precisely what CTE_LIMIT already does at
   DblqhMain.cpp:22240; single-group rides the same arm).  Strictly
   this LIGHTENS rather than removes the phase (partials must merge
   somewhere as long as the body scan runs on all nodes) — the drop is
   the hashing, the fan-out, and the multi-owner bookkeeping.
2. **The all-node consumer round-trips → DBSPJ cached row.**  fs_point's
   main is a scalar re-aggregation over CTE_SCAN, which today scans
   every node's state to find one row.  With ≤1 group at a KNOWN
   constant owner: (a) RonSQL can emit the consumer as a keyed
   CTE_LOOKUP (complete-key by construction) instead of CTE_SCAN, and
   (b) the finished DBSPJ cache serves the SECOND and later probes
   from DBSPJ memory with no DBLQH round-trip at all.  For a comma-join
   or multi-probe consumer (fs_batch-style patterns over one entity)
   the cache collapses N probes to 1.

Additional candidate (measure before building): when the body's WHERE
equality is on the table's PARTITION KEY, the body scan itself prunes
to one node — partials exist on ONE node only and the direct-ship
phase becomes a no-op self-send.  That is the true "materialize on one
node, skip redistribution" endpoint, but it requires pruned-scan
placement awareness in the CTE setup (owner = the pruned node instead
of the DBTC node) and is separable as a v2.

## Design sketch

- **Wire**: `QN_CteSubtreeNode::CTE_SINGLE_GROUP` (0x4) →
  `JoinAggSetupReq::CTE_SINGLE_GROUP_FLAG` (next strategy bit) →
  `JoinAggregationState::m_cte_single_group` — the defineCte-flags
  pattern for the third time (single-row, limit precedents; no
  version gate, alpha precedent).  DBTC decode + DblqhProxy
  strategy-mask validation extended, one line each.
- **Kernel routing**: constant-owner arm becomes
  `m_cte_single_row || m_cte_limit || m_cte_single_group`; DBSPJ's
  `cte_lookup_send` grouped arm routes to the DBTC node for
  single-group CTEs (the `limitCte` branch, same seam).  Defensive
  `ZCTE_SINGLE_GROUP_VIOLATION`-style check (>1 group at the owner ⇒
  abort, the ZCTE_SINGLE_ROW_VIOLATION pattern) since RonSQL's
  classification is the only guarantee.
- **DBSPJ cache population (finishing the skeleton)**: on the first
  CTE_LOOKUP_CONF for an eligible CTE (single-row, and now
  single-group; scalar via its local path already), copy the returned
  row section into `m_cachedRowPtrI` before delivering it; misses
  (GROUP_NOT_FOUND) cache a zero-length marker so repeated misses skip
  the round-trip too (needs a distinct "cached miss" encoding —
  m_cachedRowLen == 0 with a valid flag, since RNIL currently means
  "no cache").  Cache is per-request (`CteContext` lives in the
  Request), so lifetime and invalidation are free — released with the
  request (:4602).  Keyed probes must only be served from cache when
  the KEY MATCHES the cached row's key — for single-group CTEs all
  probes carry the same constant key by construction, but the compare
  stays as the defense (cheap: memcmp of the key section against the
  cached row's key entry).
- **RonSQL** (`is_single_group_cte` classification in analyze/plan):
  grouped body + every GROUP BY column equality-bound to a constant in
  the body WHERE (the `collect_pk_equalities`-style conjunct walk, on
  GB columns instead of PK columns; substituted scalar-subquery
  results count, matching single-row candidacy).  Emit
  `defineCte(..., CTE_SINGLE_GROUP)`; consumers keep their current
  shapes in v1 (CTE_SCAN mains still work — non-owner states are
  empty, the scalar-emit precedent), with the CTE_SCAN→keyed-probe
  main rewrite for the fs_point shape as the follow-on once the cache
  serves probes.  EXPLAIN tag `[single-group body]` in the
  CTE-definitions section.
- **Interactions**: LIMIT on a single-group body is a no-op ≥1 / empty
  at 0 (flag composition with CTE_LIMIT is trivial — both route
  constant-owner); AVG works unchanged (finalize on the owner);
  ORDER BY over ≤1 group is the accepted no-op.  FRAGS_PER_WORKER
  stays pinned by the existing scanCte rule.

## Work items

- **G1 kernel**: flag plumb (QueryTree/JoinAgg/DBTC/DblqhProxy/state),
  constant-owner arm extension, violation check.
- **G2 DBSPJ cache**: population + cached-miss encoding + key-match
  defense; block test proving second-probe-serves-from-cache (DEB_CTE
  counter or ERROR_INSERT hook) and the miss-cache path.
- **G3 RonSQL**: classification + defineCte flag + EXPLAIN; MTR family
  ×5 (fs_point shape native, probe consumers, comma-join, LEFT JOIN,
  empty-group, multi-GB-column all-bound, rejection of partially-bound
  GB, composition with LIMIT/AVG).
- **G4 fs_point main rewrite** (separable): scalar re-aggregation over
  a single-group CTE emitted as keyed probe instead of CTE_SCAN.
- **G5 benchmarks**: `.bench_ronsql fs_point` before/after (µs
  percentiles + x-ronsql-phases), the number the whole feature is for.
- **v2 (deferred)**: partition-key-pruned placement (owner = the
  pruned node, true single-node materialization with no cross-node
  ship).

## Risks / watch items

- The dormant cache was never exercised — its serve path
  (`cte_lookup_serve_cached_row`) is untested code; G2's block test
  must treat it as new, not existing, coverage.
- Cached-miss encoding must not collide with the release logic
  (RNIL = absent vs valid-but-empty).
- The single-group VIOLATION check protects against RonSQL
  misclassification (e.g. a future non-constant substitution slipping
  through); without it a second group would silently vanish at the
  owner merge.
- Owner hot-spotting is bounded (≤1 group per query), unlike the LIMIT
  CTE's full-set transit — no new memory watch item.
