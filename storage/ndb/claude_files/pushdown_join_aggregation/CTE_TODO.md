# CTE TODO - unfinished and deferred plan items

Last reviewed: 2026-05-08.

This file consolidates plan items that still appear unfinished after
reviewing the planning documents in this directory.  It intentionally
separates active RONDB-1050 wrap-up work from older deferred feature
notes and stale document-status cleanup.

## P0 - Required RONDB-1050 wrap-up

### Phase N final wrap-up

Source: `cte_filter_phase_n.md`, `cte_filter_phase_i.md`,
`cte_filter_phase_i25.md`.

Phase N is the active final wrap-up plan.  It contains the remaining
items that should be completed or explicitly deferred before closing
RONDB-1050:

- N.1: restore and fix the deferred string CTE query shapes from F.4:
  string CTE output joined to a non-unique ordered-index table,
  quote-safe compare helper for SQL text containing single-quoted
  literals, and the CTE output comparison matrix.
- N.2: decide whether RonSQL gets a SQL-facing CTE batch-size control,
  or document that batch-size tuning remains NDB-API-only.
- N.3: decide whether CTE scans get LIMIT / early-close handling now,
  or document deferral to broader LIMIT / ORDER BY pushdown work.
- N.4: audit multi-fragment / multi-node CTE coverage and either add
  reliable SQL-level coverage or document the block/full-suite coverage
  that represents it.

## P1 - Real deferred capabilities mentioned by plans

These are not required by the current Phase N wrap-up unless the
scope is deliberately expanded, but they are genuine product gaps
called out by the plans.

### CASE conditions over CTE aggregate outputs

Source: `cte_filter_phase_i4.md`, `cte_filter_phase_i.md`.

`CASE WHEN cte_agg_output ...` is still deliberately rejected.  The
implemented I.4 support covers CTE column projections in CASE
conditions, but not synthesized aggregate output slots.  The notes
also mention register-based CASE conditions and broader inequality
CASE codegen as separate follow-up work.

### MIN/MAX index optimization on nullable columns and WHERE composition

Source: `cte_filter_phase_i10.md`.

I.10 shipped scalar MIN/MAX via ordered index plus `maxRows=1` for
direct NOT NULL indexed columns.  Nullable indexed columns remain on
the baseline path because SQL MIN/MAX must ignore NULLs, and WHERE
composition is deferred because filtering can require scanning past
the first index entry.

### Projection-only CTE join shapes outside shipped coverage

Sources: `cte_filter_phase_i8.md`, `ronsql_cte_plan.md`,
`next_steps.md`.

I.8/I.11/I.12/I.24 expanded projection-only CTE support, but older
plans still name broader no-aggregate shapes as future work:

- real-table scan root plus CTE child outside the kernel-tested shapes;
- `FROM cte JOIN real_table` variants not covered by the shipped
  topology tests;
- multiple CTE/non-CTE projection-only joins beyond the currently
  accepted shapes;
- ORDER BY / LIMIT / DISTINCT / HAVING on projection-only CTE queries.

Before adding work here, verify the current RonSQL tests and accepted
shape guards, because several of the original I.8 follow-ups were
closed by later phases.

### CTE_SCAN as an outer-join child

Source: `next_steps.md`, `cte_outer_join_phase_3.md`,
`ronsql_cte_plan.md`.

CTE_LOOKUP outer-join NULL injection shipped, but CTE_SCAN as an
outer-join child remains deferred.  The notes describe it as a rare
cross-join-like shape requiring a `cte_scan_parent_row` handler,
per-parent state tracking, and a match-bit sweep.

### Full DECIMAL precision for AVG / DECIMAL metadata

Source: `cte_filter_phase_d2.md`, `cte_filter_phase_i22.md`.

Several numeric phases intentionally use widening or guards rather
than complete DECIMAL precision-scale preservation.  This is broader
than the CTE filter work, but it is still a documented follow-up for
full SQL-compatible DECIMAL aggregate metadata.

## P2 - Documentation status cleanup

These items look stale rather than unfinished implementation work.
They should be reconciled so old plans do not keep reporting already
shipped phases as pending.

### SCAN_NEXTREQ plan status

Source: `cte_nextreq_plan.md`, `cte_nextreq_phase_*.md`.

`cte_nextreq_plan.md` still lists phases 1-4 as pending.  Git history
contains shipped commits for SCAN_NEXTREQ flow control, state lifetime
fixes, and Phase 4 tests, including:

- `1d828114683` - SCAN_NEXTREQ flow control for CTE_SCAN main-SELECT root;
- `7c2a74b5276` - Phase 3 audit + CTE state lifetime fixes;
- `f799986156b` - Phase 4 tests + CTE_LOOKUP congestion-resume fix.

The docs should be updated to reflect what shipped and any remaining
coverage gaps.

### CTE filter A/B/C overview status

Source: `cte_filter_plan.md`.

The overview still marks phases A, B, and C as pending even though
later Phase I/M work depends on the CTE filter interpreter and the
aggregation-interpreter embedding work being present.  Reconcile this
overview with the actual shipped commits or mark it as historical.

### Phase J status

Source: `cte_filter_phase_j.md`.

The Phase J document still says "Plan only", but git history contains
`94c634c9dd5` ("RONDB-1050: RonSQL Phase J - LEFT-to-INNER promotion
when WHERE rejects NULL").  Update the status and verification notes
instead of treating Phase J as open work.

### Phase L status

Source: `cte_filter_phase_l.md`, `cte_filter_phase_l_commit5.md`.

The main Phase L document still says "Plan only", but git history
contains Phase L commits 1-5 covering DBLQH idempotency,
DBTC completion records, chained-CTE MTR coverage, owner routing, and
cleanup.  Reconcile the status and leave only any concrete residual
race or node-failure gaps, if such gaps still exist.

### Historical per-phase deferral notes

Sources: `cte_filter_phase_i5*.md`, `cte_filter_phase_i18.md`,
`cte_filter_phase_i6_string_minmax_*.md`, `CLAUDE.md`.

Several older documents still mention work that later phases appear to
have shipped, such as typed linked/leaf loads, NULL behaviour for
GREATEST/LEAST, string MIN/MAX work, and RonSQL MTR coverage.  These
should be marked shipped, superseded, or moved to Phase N if still
active.

## P3 - Non-CTE or broader project TODOs found while scanning

These came from planning files in the same directory but are not
clearly part of the RONDB-1050 CTE wrap-up:

- `mysql_handler_implementation.md`: EXPLAIN support and SQL-level
  MTR coverage for the MySQL handler aggregation path.
- `next_steps.md`: low-priority future join types such as ASOF JOIN,
  vector-search joins, and 64-bit `rowsExamined`.
- `star_schema_plan.md`: future star-schema work deferred to later
  tasks.

Keep these out of Phase N unless RONDB-1050 scope is explicitly
expanded.
