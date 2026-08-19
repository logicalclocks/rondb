# Non-Aggregate Pushdown — Phase 0 Detailed Plan

**Status: IMPLEMENTED (August 2026) — pending user build + MTR --record.**
**Parent:** `non_aggregate_pushdown_plan.md` (Phase 0 overview).

Implementation deltas vs the plan below:

- MTR case ids are `rpr-1..12 + rpr-P1` (`body_root_pk_residual.inc`)
  and `pt-1..6 + pt-P1/P2` (`body_passthrough_types.inc`), both in the
  `ronsql_cte` suite + 4 topology siblings (wrappers
  `ronsql_cte_dd_root_pk_residual.test` /
  `ronsql_cte_dd_passthrough_types.test`); findings files added.  The
  shared schema has `TIMESTAMP` / `TIMESTAMP(6)` (not `TIMESTAMP(3)`),
  hash-only PKs (a family-local ordered-PK `acct` table serves the
  index-scan branches, per the `body_main_root_index.inc` precedent),
  and no BIT/VARBINARY (family-local `bin1` serves the rejections).
- 0a: `build_root_residual` returns bool (false on flatten overflow →
  caller drops the PK-equality optimization); the over-cap `readTuple`
  fallback and the scan-child branch share the new
  `emit_pk_equality_index_scan_root` helper; the old branch-6 body was
  deleted into it.
- 0b: raw Date/Year arms feed the factored `print_temporal_packed`
  directly (a raw DATE/YEAR value IS the packed representation);
  Datetime2/Time2/Timestamp2 re-pack their big-endian bytes into the
  Uint64 form first.  The passthrough ctor takes the same
  col_idx-indexed metadata as the aggregate ctor;
  `passthrough_column_metadata(o)` maps each output in
  `print_passthrough_row`.
- The optional EXPLAIN add-on was skipped: the PK branches are
  emit-time decisions invisible to the planner-type EXPLAIN print
  (rpr-11/12 are correctness-only).  The optional b4 parity sweep
  (raw Year/Time2 in the aggregate GROUP BY print) was not done.

**Crash discovered on first record (pre-existing, fixed as part of
0a).**  rpr-1 crashed both data nodes at `DbspjMain.cpp:8748`
(`ndbrequire(m_aggNodes.get(nodeId))` in the aggregate-leaf
`lookup_send`).  Root cause: an **aggregate query without CTEs whose
root is PK-equality-covered** emitted a `readTuple` root, making the
whole NdbQuery a LookupQuery → TCKEYREQ path — and DBTC's
`JOIN_AGG_SETUP` only exists on the SCAN_TABREQ path
(`isScanQuery()` selects the protocol in `prepareSend`;
`NdbQueryBuilderImpl.hpp` even notes "Real-table PK/UniqueIndex main
roots without CTEs stay as TCKEYREQ").  DBSPJ then receives a
join-agg tree with an empty `m_aggNodes` and asserts.  Pre-existing
and reachable before 0a with a plain full-PK WHERE (the residual is
incidental); every previously-green PK-equality aggregate case
involves a CTE, whose materialisation scan makes the compound query
scan-rooted (fpw-6 / J14 / mri-5 shapes).  Fix, both sides:

- `RonSQLPreparer::emit_root_op` — the `readTuple` root branch now
  requires `lookup_root_supported`: nothing aggregates in this scope
  (pass-through), or main scope of a CTE-containing plan.  Aggregate
  no-CTE queries and CTE-body scopes (always aggregating, possibly
  op[0]) take the ordered-PK-index-scan / table-scan fallbacks.
- `NdbQueryDefImpl` ctor — prepare-time rejection
  (`QRY_WRONG_OPERATION_TYPE`) of main-aggregation query defs with no
  CTEs and a non-scan op[0], so API misuse gets a clean error instead
  of a node failure.
- **Kernel hardening (the crash itself must fail the query, not the
  node — the tree bits are API-controlled data, and an unpatched
  client library can still send the shape):**
  - `Dbspj::validateAggregateFlags` — new build-time check: a request
    with `RT_AGGREGATE` (main aggregate leaves) but an empty
    `m_aggNodes` (no `[nodeId, aggStateKey]` pairs — the
    lookup-protocol case, or a scan request missing its section) now
    returns `DbspjErr::InvalidAggregateFlags`, failing the query with
    a clean REF like the function's sibling malformed-tree checks.
  - `Dbspj::do_init(Request*, const LqhKeyReq*, ...)` — now clears
    `m_aggNodes` (the ScanFragReq `do_init` already did).  Request
    objects come from a TransientPool (no constructors), so a
    recycled lookup-protocol Request carried the previous occupant's
    bits — making the crash nondeterministic and, worse, potentially
    letting a stale bit *pass* the leaf-send check and feed a bogus
    agg state key.
  - The runtime `ndbrequire(m_aggNodes.get(nodeId))` sites
    (`lookup_send` :8748, `sendJoinAggNullRow`, `scanFrag_send`)
    stay: with the build gate + init fix, they can only fire on
    trusted-tier (DBTC-constructed aggStateKeys) inconsistencies,
    where crash-on-violation is this project's convention
    (`tiered_response_policy.md` — Tier A criteria; the
    API-controllable path is what needed the graceful failure).

rpr-1/2/9 now exercise the scan fallbacks; rpr-13/14 cover the
still-legal aggregate `readTuple` root + residual inside a
CTE-containing (scan-rooted) query.

**Post-review fixes (August 2026).**  A review after the first green
run found one correctness defect and two coverage gaps, all fixed:

- **Zero TIMESTAMP rendered as Unix epoch.**  MySQL reserves
  `tv_sec == 0` for the zero timestamp `0000-00-00 00:00:00`
  (`Field_timestampf::get_date_internal_at`, sql/field.cc); the
  decoders converted it through the epoch converter and printed
  `1970-01-01 00:00:00`.  New `ronsql_timestamp_tv_to_TIME` wrapper
  (zero guard + epoch conversion + fractional part) now used by all
  three decode sites: `print_temporal_packed` (raw + packed CTE
  outputs) and the two raw GROUP-BY print arms — the latter were the
  same pre-existing defect on the aggregate path.  Coverage: local
  `tz1` table (inserted under `sql_mode=''`), cases pt-7 (raw
  pass-through), pt-8 (packed CTE MIN — zero sorts below every valid
  epoch), pt-9 (aggregate GROUP BY over a zero TIMESTAMP, the raw
  group-by arm).
- **The over-cap cases never reached the 64-word branch.**  rpr-11/12
  were aggregate no-CTE shapes, so `lookup_root_supported` suppressed
  the readTuple branch before the word count ran.  Rewritten as
  lookup-ELIGIBLE shapes: rpr-11 pass-through CTE chain on a hash-PK
  root (over-cap → table-scan fallback), rpr-12 pass-through chain on
  the ordered-PK `acct` root (over-cap → PK-index-scan fallback), new
  rpr-15 aggregate + CTE over-cap (table-scan fallback inside the
  scan-rooted compound query).
- **Direct hardening-layer coverage.**  New `testJoinAggNdbApi`
  Test 24 builds a lookup-rooted aggregate query def (the rpr-1 crash
  shape) and asserts `prepare()` fails with
  `QRY_WRONG_OPERATION_TYPE` (4820) — pinning the API layer directly.
  The DBSPJ build gate remains deferred: `testJoinAggSpj` only speaks
  SCAN_TABREQ, and on the scan path DBTC itself constructs the
  aggStateKeys, so reaching the empty-`m_aggNodes` check requires a
  new raw lookup-protocol (TCKEYREQ + query tree) sender in that
  harness — a harness feature, not a test addition.

- **Out-of-range literals reclassified as permanent errors.**  The
  first version of rpr-11 used TINYINT literals above 127, exposing
  that `encode_constant`'s "literal out of range" errors (integer +
  the two DECIMAL-unsigned variants) were `RonSQLMaybeStaleSchema` —
  and burned 10 doomed retries.  All three now throw
  `RonSQLPermanentError`: the literal lies outside the column type's
  declared domain, a property of the query text, not a transient
  condition.  rpr-P2 pins the single-attempt "RPE" classification.
  Root cause of the *unbounded* retrying is a separate tracked defect:
  the join-path `unload_schema` comparison always reports "schema
  changed" (it compares against `m_indexes`, which only
  `load_single_table` populates, so a join root with any online
  ordered index trips `new_indexes_count >= old_indexes_count` with
  0 old entries).  Until that gets a join-aware version walk, EVERY
  `RonSQLMaybeStaleSchema` thrown from such a join query is
  retryable — tracked in `findings/root_pk_residual.md`.

Re-record needed after these fixes: both `ronsql_cte` families ×5
topology suites and `ndb_push_agg.testJoinAggNdbApi` (Test 24 adds an
output line).

Two independent pre-existing defects, both already causing wrong results
or hard failures in **shipped** query shapes, and both prerequisites for
every later phase of the non-aggregate work:

- **0a** — `emit_root_op`'s PK-equality-covered branches silently drop
  residual WHERE conjuncts (wrong results today in aggregate join/CTE
  queries and in the accepted E.3/I.8 passthrough shapes).
- **0b** — the passthrough `ResultPrinter` rejects temporal / DECIMAL
  columns outright and prints CTE temporal MIN/MAX outputs as raw packed
  integers (silent wrong output today in accepted passthrough shapes).

0a and 0b are independent; land 0a first (correctness severity).

---

## 0a — root residual conjuncts dropped under PK-equality cover

### The bug

`emit_root_op` (`RonSQLPreparer.cpp:6834-7192`) detects a full
PK-equality cover of the root's WHERE conjuncts via
`collect_pk_equalities` and then builds keys from the matched
equalities. Three branches never apply the *remaining* conjuncts:

| Branch | Lines | Emits | Residual today |
|---|---|---|---|
| Full-key `lookupCte` root | `:6960-7044` | `qb->lookupCte(...)` with typed const keys | **dropped** |
| Real-table `readTuple` root (PK covered, no scan child) | `:7127-7145` | `qb->readTuple(root_table, pk_keys, &rootOpts)` | **dropped** |
| PK-covered + scan child | `:7147-7177` | `qb->scanIndex(pk_ordered_idx, ..., equality bound)` | **dropped** |

Contrast with the two correct siblings: `emit_index_scan_root` routes
every `condition_handling_map[ci] == -1` conjunct into an
`NdbScanFilter` interpreted program (`:6790-6819`), and the
`scanTable` fallback applies the whole `where_ce` as a filter
(`:7179-7189`).

**Who reaches these branches today** (all currently return wrong
results when a residual exists):

- Aggregate join queries with a PK-equality-covered root:
  `SELECT SUM(o.amt) FROM customer c JOIN orders o ON ... WHERE
  c.c_custkey = 7 AND c.c_acctbal > 1000` — the `c_acctbal` conjunct
  vanishes. Both the `readTuple` branch (all-lookup children) and the
  PK-ordered-index branch (scan child) variants.
- E.3 passthrough with a full-key CTE root:
  `WITH c AS (SELECT g, COUNT(*) cnt FROM t GROUP BY g)
  SELECT g, cnt FROM c WHERE g = 5 AND cnt > 100` — full virt-PK cover
  on `g` fires the `lookupCte` branch; `cnt > 100` vanishes (MySQL
  returns nothing, RonSQL returns the row).
- I.8/I.11 chains whose real root is PK-covered with extra root
  conjuncts.

A second-order symptom of the same mechanics: contradictory equalities
(`WHERE pk = 1 AND pk = 2`) — `collect_pk_equalities` overwrites
`pk_const[k]` on the second visit (`:5953-5961`), the first equality is
dropped, and the query wrongly returns the `pk = 2` row instead of an
empty result.

### Root cause

`collect_pk_equalities` (`:5923-5962`) walks the (already
`simplify_ce`-simplified) `T_AND` tree recording `col = const` per PK
column, but records **only the const side** (`pk_const[k] =
const_side`). Nothing tracks *which conjuncts were consumed*, so no
caller can compute the residual, and none tries.

### Fix design

**Step 1 — consumption tracking.** Extend `collect_pk_equalities` with
a parallel out-array of the consumed `T_EQUALS` nodes:

```cpp
void collect_pk_equalities(ConditionalExpression* ce,
                           const NdbDictionary::Table* table,
                           ConditionalExpression* pk_const[],
                           ConditionalExpression* pk_eq_ce[] /* may be NULL */);
```

Setting `pk_const[k]` also sets `pk_eq_ce[k] = ce` (the equality node).
Call sites: `:2890` (`root_pk_equality_covered` — pass `NULL`),
`:6939` (CTE branch), `:7106` (real-table branch).

Overwrite semantics stay as-is: on `pk = 1 AND pk = 2`, the second
visit replaces both entries, the first equality lands in the residual,
and the filter correctly yields an empty result — the contradiction
case fixes itself.

**Step 2 — residual builder.** A small shared helper next to
`emit_index_scan_root`:

```cpp
ConditionalExpression*
build_root_residual(ConditionalExpression* where_ce_simplified,
                    ConditionalExpression* const consumed[], int nkeys);
```

AND-flattens `where_ce_simplified` into top-level conjuncts (same walk
`collect_pk_equalities` does), drops every conjunct that is
pointer-equal to a `consumed[k]`, and recombines the rest with the
synthetic-`T_AND` pattern from `:6799-6808`. Returns `NULL` when
nothing remains. Must operate on the **same simplified tree** the
equality collection ran on (`w` at `:6934` / `where_ce` at `:7101`) so
pointer identity holds.

**Step 3 — per-branch application.**

- **Real `readTuple` root (`:7127-7145`)**: when the residual is
  non-NULL, build `NdbInterpretedCode code(root_table)` +
  `NdbScanFilter` (`setSqlCmpSemantics`, `begin(AND)`,
  `apply_filter(&filter, scope, residual)`, `end`, `finalise`) and
  `rootOpts.setInterpretedCode(code)` — the exact `:7182-7188`
  pattern. **Size guard**: lookup-op programs ride inside every
  LQHKEYREQ; mirror ha_ndbcluster's 64-word policy cap
  (`ha_ndbcluster.cc:15778-15788`). Use `code.getWordsUsed()`; it
  overcounts by 2 words per label (label meta-info, see the
  `getWordsUsed()` note in this directory's CLAUDE.md) — conservative
  is fine. If over the cap, do **not** emit `readTuple`; fall through
  to the PK-ordered-index equality scan (next bullet, generalized), and
  from there to the `scanTable` fallback. That cascade
  (`readTuple`+small filter → PK-index equality scan+filter →
  `scanTable`+filter) is always correct, merely decreasingly optimal.
- **PK-covered + scan child `scanIndex` root (`:7147-7177`)**: attach
  the residual via the same `NdbScanFilter` block, unconditionally —
  scan-op programs travel in SCAN_FRAGREQ attrinfo sections, no size
  concern. This is a verbatim transplant of `emit_index_scan_root`'s
  residual block (`:6811-6818`). Factor the branch body into a helper
  (`emit_pk_equality_index_scan_root`) so the over-cap `readTuple`
  fallback above can reuse it even when there is no scan child.
- **Full-key `lookupCte` root (`:6960-7044`)**: compute the residual
  from the same simplified `w`; when non-NULL, emit exactly what the
  scalar-dummy branch (`:6881-6931`) and the `scanCte` fallback
  (`:7046-7056`) already do:
  `emit_cte_lookup_filter(code, scope, /*op_idx=*/0,
  cteVirtualTables[0], residual)` + `rootOpts.setInterpretedCode`.
  `emit_cte_lookup_filter` (`:7832`) re-simplifies internally, handles
  DNF, and **throws clean permanent errors** on atoms it cannot express
  (col-vs-col, non-DNF nesting — `:7826-7830`). That is a deliberate
  behavior change: conjuncts that were silently dropped now either
  filter correctly or reject cleanly. No new size cap — this reuses the
  Phase A CTE_LOOKUP filter envelope that branches 1 and 3 already
  ship.

**Runtime semantics to verify** (should hold by construction, but gets
an explicit test): a filtered-out lookup root behaves as a lookup miss
— DBSPJ treats the interpreter reject as "no row" (the
pushed-condition-on-lookup precedent in ha_ndbcluster guarantees the
kernel path), children never execute, aggregates run over the empty
set, the passthrough drain prints nothing (deferred header ⇒ empty
output matches the mysql baseline). Watch that the drain does not
misreport the miss as an error (`NextResult` must complete normally).

### Behavior changes to call out in the commit message

1. Queries that silently returned wrong results now return correct
   results (aggregate and passthrough).
2. Contradictory PK equalities now return empty results instead of the
   last-equality row.
3. Full-key-CTE-root queries whose residual contains a conjunct the CTE
   filter emitter cannot express now fail with that emitter's clean
   error instead of succeeding wrongly.

### Tests (0a)

New data-driven family `body_root_pk_residual.inc` in the `ronsql_cte`
suite (+ the 4 topology siblings), following the
`body_main_root_index.inc` precedent; plus aggregate-side cases. Cases:

| # | Shape | Verifies |
|---|---|---|
| r1 | agg join, PK-covered root + residual, all-lookup children | `readTuple` branch + filter (row accepted) |
| r2 | r1 with residual rejecting the row | empty aggregate over zero rows, no error |
| r3 | agg join, PK-covered root + residual, scan child | `scanIndex` equality branch + filter |
| r4 | E.3 passthrough, full-key CTE root + residual on aggregate output (accept + reject variants) | `lookupCte` branch + CTE filter |
| r5 | I.8 passthrough chain, PK-covered real root + residual | drain-side correctness incl. deferred header on empty |
| r6 | `WHERE pk = 1 AND pk = 2` | contradiction → empty result |
| r7 | residual with many OR disjuncts on a `readTuple` root exceeding the word cap | falls back to index/table scan, still correct |
| r8 | full-key CTE root + unsupportable residual atom | clean permanent error (no silent wrong result) |

All cases compare against MySQL via `ronsql_compare.inc`; r1-r6 are
**failing-first** (they demonstrate today's wrong results before the
fix). EXPLAIN currently prints CONDITIONS only for the scan-config
index root; extending the PK branches' EXPLAIN output to show the root
residual filter is a small optional add-on — include it if cheap,
otherwise note as follow-up.

---

## 0b — passthrough printer: type coverage + ColumnMetadata

### The bug(s)

The passthrough `ResultPrinter` constructor
(`ResultPrinter.cpp:165-189`) hardcodes `m_column_metadata = NULL`, and
`print_passthrough_value` (`:1149-1250`) supports only integers, Float,
Double (with the D15 DECIMAL-scale refinement), Char, Varchar and
Longvarchar. Two distinct failure classes in **currently accepted**
passthrough shapes (E.3 / I.8 / I.11 / I.12):

1. **Hard failure** — projecting a real-table DATE / YEAR / DATETIME2 /
   TIME2 / TIMESTAMP2 / DECIMAL column hits the `default:` throw at
   `:1243-1248` ("Unsupported column type in projection-only CTE_SCAN
   result." — a stale message; the path serves all passthrough
   shapes).
2. **Silent wrong output** — a CTE `MIN`/`MAX` over a temporal column
   returns a **Bigunsigned virt column carrying the packed value**
   (D17 + temporal extension). The aggregate printer decodes it via
   `ColumnMetadata::temporal` (`print_aggregate_result`,
   `:2121-2201`); the passthrough printer has no metadata, so
   `SELECT c.min_d FROM t JOIN c ...` prints the raw packed integer
   while MySQL prints `YYYY-MM-DD`. Reproduce and lock in as a
   failing-first test.

Everything needed already exists on the aggregate side:

- **Metadata build** (`RonSQLPreparer.cpp:5166-5217`): a
  `col_idx`-indexed `ColumnMetadata[]` (charset / precision / scale /
  temporal / temporal_fsp) built from
  `m_main_scope.resolved_columns[col_idx].dict_column` — which
  `resolve_cte_output_columns_for_scope` plumbs back to the *source*
  column even through a CTE hop, so temporal tagging works for CTE
  outputs.
- **Packed-temporal decode** (`print_aggregate_result`,
  `:2124-2199`): DATE / YEAR / DATETIME2 / TIME2 / TIMESTAMP2 from a
  packed Uint64, using MySQL's own codecs and the lock-free
  `ronsql_utc_sec_to_TIME` (`:67-79`).
- **Raw-byte decodes** in the GROUP BY print path: DECIMAL via
  `decimal_bin2str` (`:715-731`), DATE (`:782-790`), DATETIME2
  (`:805-817`), TIMESTAMP2 (`:818-833`). (Raw Year/Time2 are
  `feature_not_implemented` even there — see b4.)

### Fix design

**b1 — metadata plumbing.** Factor `:5166-5217` into a helper
(`build_result_column_metadata()`), call it from both branches of the
printer construction (`:5165-5233`), and give the passthrough ctor a
`const ColumnMetadata*` parameter. Keep the **same `col_idx` indexing**
in both modes (one semantic for `m_column_metadata`). In the
passthrough printer, precompute the per-output `col_idx` list once
(walk `m_query->outputs`; each output is `Outputs::Type::COLUMN` with
`o->column.col_idx` — the same mapping `execute_passthrough_drain`
uses at `:6570`), so `print_passthrough_row` can hand
`&m_column_metadata[col_idx]` to the value printer per position.
Columns without resolution keep `has_metadata = false` and behave as
today.

**b2 — shared packed-temporal decode.** Factor the `:2124-2199` switch
into a static helper:

```cpp
static void print_temporal_packed(std::ostream& out, Uint64 w,
                                  TemporalDisplay temporal, int fsp,
                                  const char* quote);
```

`print_aggregate_result` calls it (behavior unchanged — it currently
prints unquoted; pass `""`), and the passthrough path calls it with
`m_quote` so JSON output gets quoted temporals (TSV `m_quote` is
empty). This resolves the `:1291-1293` JSON caveat for temporals.

**b3 — `print_passthrough_value` extension.** New signature
`print_passthrough_value(std::ostream&, const NdbRecAttr*,
const ColumnMetadata* meta /* may be NULL */)`. Logic:

1. NULL handling unchanged (`:1152-1155`).
2. **Packed-CTE-output pre-check** (mirrors `:2121-2123`): if
   `meta != NULL && meta->temporal != NONE` and the attr type is
   `Bigunsigned`, decode via `print_temporal_packed(out,
   attr->u_64_value(), meta->temporal, meta->temporal_fsp, m_quote)`
   and return. (DECIMAL-widened CTE outputs carried as Double already
   work — the D15 arm reads scale/precision from the virt column via
   `attr->getColumn()`.)
3. **New raw arms**, metadata via `attr->getColumn()` (already the
   pattern in the Double/Char/Varchar arms):
   - `Date`: 3-byte value (`attr->u_medium_value()`, uint3korr
     semantics) → `(y = w>>9, m = (w>>5)&15, d = w&31)` →
     `YYYY-MM-DD`, quoted with `m_quote` — same output as `:782-790`.
   - `Year`: 1 byte; `0 → 0000`, else `w + 1900` → `YYYY`, quoted.
   - `Datetime2`: raw bytes at `attr->aRef()` + `col->getPrecision()`
     → `my_datetime_packed_from_binary` +
     `TIME_from_longlong_datetime_packed` + `my_TIME_to_str`, quoted
     (`:806-816` pattern).
   - `Time2`: raw bytes + precision → `my_time_packed_from_binary` +
     `TIME_from_longlong_time_packed` + `my_TIME_to_str`, quoted (new
     — mirrors `print_aggregate_result`'s TIME2 arm on raw bytes).
   - `Timestamp2`: raw bytes + precision → `my_timestamp_from_binary`
     + `ronsql_utc_sec_to_TIME` + `my_TIME_to_str`, quoted
     (`:819-832` pattern; UTC display per the
     `cte_date_minmax_plan.md` convention — suites run with
     `time_zone='+00:00'`).
   - `Decimal` / `Decimalunsigned`: `decimal_bin2str` with the
     column's precision/scale (`:721-728` pattern), **unquoted**
     (numeric, matching the GROUP BY print and MySQL TSV output).
4. **Kept rejections, better message**: `Olddecimal(unsigned)`, old
   `Datetime`/`Time`/`Timestamp`, `Bit`, `Binary`/`Varbinary`/
   `Longvarbinary`, `Blob`/`Text` still throw — but the message now
   names the column type and no longer claims "projection-only
   CTE_SCAN" (e.g. "Unsupported column type <T> in pass-through
   result."). This matches the aggregate GROUP BY print's envelope.

**b4 — optional parity sweep (separate commit).** The aggregate GROUP
BY raw print rejects `Year` (`:799-800`) and `Time2` (`:803-804`)
while `print_aggregate_result` supports both packed. Closing those two
gaps with the same helpers keeps agg/passthrough parity cheap. Do it
if it stays a ~30-line diff; otherwise defer.

### Tests (0b)

New family `passthrough_types.inc` (`ronsql_cte` suite ×5 topologies,
`time_zone='+00:00'` per suite convention):

| # | Case |
|---|---|
| t1 | I.8 chain projecting real-table DATE, YEAR, DATETIME2 (fsp 0/3/6), TIME2, TIMESTAMP2 columns |
| t2 | same for DECIMAL(p,s) incl. negative values and s=0 |
| t3 | E.3 shape: CTE `GROUP BY date_col`, selecting the raw date column (virt col typed Date → raw arm) |
| t4 | CTE `MIN(date)` / `MAX(timestamp2)` outputs selected through I.8 passthrough (the packed-Bigunsigned decode — **failing-first**: today prints raw integers) |
| t5 | NULLs in every new type (NULL representation unchanged) |
| t6 | LEFT JOIN NULL-substituted rows over temporal/DECIMAL columns (I.12 `effective_attrs` path → NULL, not a decode of garbage) |
| t7 | rejection: BIT and VARBINARY projections → clean error naming the type |

TEXT/TSV comparison via `ronsql_compare.inc`; add one JSON-format
spot-check of a temporal + string row if the harness supports it
(quoting correctness), otherwise verify JSON manually via rondb-cli
and note it in the test header.

---

## Sequencing / suggested commits

1. **0a-1**: `collect_pk_equalities` consumption tracking +
   `build_root_residual` + real-table branches (`readTuple` cascade +
   `scanIndex` residual) + MTR r1-r3, r5-r7.
2. **0a-2**: full-key `lookupCte` root residual via
   `emit_cte_lookup_filter` + MTR r4, r8.
3. **0b-1**: metadata build factored + passthrough ctor plumbing +
   `print_temporal_packed` extraction (no behavior change yet).
4. **0b-2**: `print_passthrough_value` new arms + packed pre-check +
   message fix + MTR t1-t7.
5. **0b-3** (optional): b4 parity sweep.

Each commit leaves the tree green; r1-r6/t4 are recorded as
failing-first against the pre-fix behavior in the commit message.

## Verification (user-run)

- `./mtr --suite=ronsql_cte body_root_pk_residual` and
  `passthrough_types` (first run with `--record`), then the ×5
  topology siblings.
- Regression: `./mtr --suite=ronsql ronsql_basic ronsql_cte_scan
  ronsql_cte_scalar ronsql_cte_outer_join` plus the aggregate
  `ronsql_cte` families (the 0a branches serve aggregate queries too).
- Spot-check `.explain_ronsql` on an r1-shaped query (root op choice
  unchanged; residual filter attached).

## Risks

- **0a is a behavior change** on three axes (see list above) — commit
  message must spell them out; a query relying on the buggy result
  will change.
- **Lookup-op program size**: the 64-word cap + scan fallback protects
  LQHKEYREQ; the fallback path must be tested (r7), not assumed.
- **Interpreter-reject mapping on lookup roots**: verified by test r2;
  if DBSPJ surfaces an error instead of a miss for the root-lookup
  case specifically, the fix is to treat that error as empty-result in
  the drain — decide only on evidence.
- **Pointer-identity residual subtraction** requires collection and
  subtraction to run on the same simplified CE tree — keep the
  `simplify_ce` call single-sited per branch.
- **Metadata indexing drift**: both printer modes now share
  `col_idx`-indexed metadata; the passthrough output→col_idx mapping
  must stay consistent with `execute_passthrough_drain`'s walk (same
  `outputs` order) — assert `num_cols` equality at setup.
