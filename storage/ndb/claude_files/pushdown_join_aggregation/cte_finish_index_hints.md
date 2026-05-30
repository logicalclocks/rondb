# CTE Finish Point — Index Hints, Composite Bounds, Body OR (W0–W3)

This is the closing chunk of CTE work for RONDB-1050: bringing CTE-body
filter/index support to a clean finish point and adding SQL-level index
selection. Four workstreams shipped together.

## Scope decisions (locked with the user)

- **CTEs** need only aggregation (already shipped) plus *fairly general*
  filters and indexes. The two CTE-body gaps closed here are **OR/DNF in the
  body WHERE** and **composite (multi-column) index bounds**.
- The **main query is feature-frozen** — maintained, not extended.
- RonSQL has **no cost-based optimiser**, so the SQL chooses the index via
  **MySQL-style `FORCE`/`USE`/`IGNORE INDEX`**, honored in both CTE bodies and
  the main query.
- **Join order is defined by the query.** The single exception is the existing
  I.16 auto-rewrite (a partial-key join to a multi-key CTE promotes that CTE to
  root). It is kept as-is and documented here as the one sanctioned exception.

## W0 — shared scan-config candidate generator

`RonSQLPreparer::build_scan_config_candidates()` is now the single generator
used by both the main-query path (`generate_scan_config_candidates`) and the
CTE-body path (`select_cte_body_scan_config`). It pushes a TABLE_SCAN candidate
plus one INDEX_SCAN candidate per usable ordered index, scored by the existing
goodness heuristic, and records bound-vs-residual routing in each candidate's
`condition_handling_map`. This is where the index-hint filter, composite-bound
scoring, and OR-aware conjunct handling all live once.

## W3 — composite (multi-column) index bounds in CTE bodies

The candidate scorer and the CTE-body INDEX_SCAN emit path already built
multi-column `NdbQueryIndexBound`s, but the shape had never been exercised and
hit a latent **NDB API bug**: `appendBoundValue()` stamped every constant bound
value with placeholder `AttributeHeader(0)`, relying on
`Dbspj::scanFrag_fixupBound()` to renumber attribute ids. That fixup runs only
for parent-row-driven child scans, so a **CTE-materialization root** index scan
with a multi-column constant bound kept attrId 0 for every key → DBTUX
duplicate-attribute rejection (**error 4259**). Single-column bounds worked only
because placeholder 0 is coincidentally correct.

Fix: `appendBoundPattern` already knows each value's index column (`keyNo`); it
now passes it to `appendBoundValue`, which stamps it as the attribute id.
Harmless for the child path (`fixupBound` overwrites with the identical value).
Only the last bound column may be a half-open range (earlier columns are
equalities, enforced by `later_columns_blocked`), keeping the single
`NdbQueryIndexBound` inclusivity flag per side correct.

MTR: `ronsql_cte_composite_index` (eq+eq, eq+range, gap column, half-open
middle, eq+half-open, main-query parity).

## W2 — OR / DNF in a CTE body's WHERE

Already functional via the shared `apply_filter` `NdbScanFilter::OR` path: a
single-table body's top-level OR stays on `join_where_ce[0]`, and a top-level OR
conjunct is never bound-eligible (op `T_OR`) so it routes to the residual filter
while sibling AND conjuncts still drive index bounds. No DNF factoring — matches
the main query. No production change; MTR: `ronsql_cte_or_body`.

## W1 — MySQL-style index hints (`FORCE` / `USE` / `IGNORE INDEX`)

Syntax (attached to the shared `table_ref`, so it parses anywhere a table
appears): `FROM t FORCE INDEX (i)`, `USE INDEX (i, ...)`, `IGNORE INDEX (i)`,
`USE INDEX ()` (force table scan). `INDEX` and `KEY` are synonyms.

- Honored only on **root scans** — the main-query root and each CTE-body root.
  A hint on a **joined** table is rejected (`reject_index_hints_on_joins`) with
  a clear message.
- `IGNORE` drops named indexes; `USE`/`FORCE` consider only named indexes
  (`USE` falls back to a table scan if none usable). `FORCE` that names no
  usable index throws a `RonSQLPermanentError` (distinguishing "not an available
  ordered index" from "no WHERE condition matches its leading column").
- Implementation: `FORCE`/`USE`/`IGNORE`/`INDEX`/`KEY` added to
  `keywords_implemented_in_ronsql` (still in the reserved list, as the
  KeywordsUnitTest requires); new tokens + `index_hint_opt` / `index_kw` /
  `index_name_list` grammar attached to a `table_ref_base` + hint wrapper;
  `TableRef` gains `HintKind` + `IndexHintList` (enumerators prefixed `HINT_`
  to dodge the `NONE`/`FORCE`/`USE`/`IGNORE` macros from MySQL/NDB headers);
  the hint plugs into the W0 shared generator.

MTR: `ronsql_index_hints` (FORCE/USE/IGNORE in main + CTE body, alias + KEY,
`USE INDEX ()`, plus three rejection cases).

### Not done (deliberate)

- Hints on joined / CTE_LOOKUP tables: rejected, not supported. The root-scan
  path is where the heuristic index choice the user wants to override lives.
- No EXPLAIN-level assertion that a specific index was chosen; the FORCE
  rejection cases prove the hint is parsed and enforced, positive cases prove
  correctness.

## Files

- `storage/ndb/src/ronsql/RonSQLPreparer.{cpp,hpp}` — shared generator, hint
  logic + errors, join-hint rejection, comment refresh.
- `storage/ndb/src/ronsql/RonSQLParser.y`, `Keywords.hpp`, `RonSQLCommon.hpp` —
  grammar/lexer/AST for the hint.
- `storage/ndb/src/ndbapi/NdbQueryBuilder.cpp`, `NdbQueryBuilderImpl.hpp` —
  W3 multi-column constant-bound attrId fix.
- `mysql-test/suite/ronsql/` — `ronsql_cte_composite_index`,
  `ronsql_cte_or_body`, `ronsql_index_hints`.
