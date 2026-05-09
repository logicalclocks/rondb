# Phase N - RONDB-1050 final wrap-up

## Status

**Planned final phase.**  Phase N collects the remaining Phase I
catalogue items that are still worth closing before wrapping up
RONDB-1050:

- I.25: deferred string CTE query failures from F.4 coverage;
- I.13: SQL-facing batch-size control, or an explicit decision that it
  stays unsupported;
- I.14: early close of CTE scans, normally via LIMIT / close handling;
- I.15: multi-fragment / multi-data-node CTE behaviour coverage.

Everything else in the current Phase I catalogue is either shipped or
explicitly out of scope for this wrap-up.

## N.1 - Restore and fix deferred string CTE query shapes

Source item: I.25 (`cte_filter_phase_i25.md`).

### N.1a - CTE string output joined to non-unique indexed table

Restore the original F.4 query shape:

```sql
WITH s AS (
  SELECT grp, MIN(v) AS min_v, MAX(v) AS max_v
  FROM str_minmax
  GROUP BY grp
)
SELECT MIN(s.min_v) AS min_v, MAX(s.max_v) AS max_v
FROM s
JOIN str_minmax AS t ON t.grp = s.grp;
```

The primary-key helper-table variant already proves that linked string
CTE outputs can feed downstream aggregation.  This test must prove the
broader topology: CTE root driving a non-unique ordered-index child and
then aggregating linked CTE string outputs.  If the query returns
`NULL,NULL`, trace whether the indexed child produces rows and whether
the linked CTE projections arrive at the expected linked-column
positions in `JoinAggInterpreter`.

### N.1b - Quote-safe RonSQL compare helper

The compare helper must support SQL text containing single-quoted
literals.  Prefer passing the query to `mysql` through a temporary file
or another shell-safe mechanism instead of interpolating it into a
single-quoted `mysql -e '...'` command.

Restore the intended string-literal predicate once this is fixed:

```sql
WITH s AS (
  SELECT grp, MIN(v) AS min_v, MAX(v) AS max_v
  FROM str_minmax
  GROUP BY grp
)
SELECT grp, min_v, max_v
FROM s
WHERE min_v < 'beta';
```

Decide explicitly whether double-quoted string literals are supported
or rejected under RonSQL's parser / SQL-mode model.  If rejected, keep
a clear parser-error regression.

### N.1c - CTE output comparison matrix

Add focused MTR coverage for:

- `cte_col <op> cte_col`;
- `constant <op> cte_col`;
- `cte_col <op> constant`.

Cover accepted and rejected cases for signed integers, unsigned
integers, FLOAT / DOUBLE, DECIMAL after I.22 widening / guards,
CHAR / VARCHAR / Longvarchar, exposed date/time types where relevant,
and NULL semantics.  Unsupported families must reject at prepare time
with a message naming the comparison shape and type family.  Do not
silently coerce unsupported types to BIGINT.

## N.2 - Decide SQL surface for CTE batch size

Source item: I.13.

Decision: defer SQL-level batch-size control.  Batch-size tuning
remains an NDB API feature for RONDB-1050.

`testCteNdbApiFilter` covers `setBatchSize` at the NDB API layer, and
`ronsql_cte_multi_batch.test` covers SQL-visible multi-batch execution
with the normal RonSQL defaults.  RonSQL has no SQL surface today for
setting per-query CTE scan batch size, and Phase N should not introduce
a new user-visible knob without a clear operational contract.

The rejected alternatives were:

1. Add a session/system variable or documented hint that controls the
   NDB query batch size for RonSQL CTE scans and add a small MTR test;
2. add an internal test-only SQL hint.

Both alternatives are deferred.  A real SQL surface would have to say
whether it applies to CTE_SCAN roots only, CTE_LOOKUP-driven plans,
real-table children under CTE parents, ordinary pushed joins, or all
NdbQuery scans.  A test-only SQL hint would add parser and preparer
surface area solely for coverage that is already available through the
NDB API block tests.

If users later need this operationally, implement it as a general
RonSQL/NdbQuery scan tuning feature, not as a CTE-only special case.
Until then the supported coverage split is:

- NDB API tests verify explicit small-batch behaviour with
  `setBatchSize`;
- RonSQL MTR tests verify correct multi-batch behaviour under the
  default batch size;
- no SQL-facing batch-size tuning is advertised for RONDB-1050.

## N.3 - Early close / LIMIT interaction for CTE scans

Source item: I.14.

RonSQL currently drains CTE scans.  Phase N should close this as either:

1. a LIMIT-aware early close implementation for CTE main-root scans,
   including `query->close()` / kernel cleanup coverage; or
2. a documented deferral tied to the broader RonSQL LIMIT / ORDER BY
   pushdown work.

If implemented, verify that early close releases CTE scan iterator
state and aggregation state cleanly.  The test should include a CTE
result larger than one API batch so the close path is observable.

## N.4 - Multi-fragment / multi-node CTE behaviour

Source item: I.15.

Coverage audit: `cte_filter_phase_n4_coverage_audit.md`.

The block tests cover fragment-level and multi-node behaviour better
than ordinary MTR.  The audit records that no new normal MTR case is
needed for this item: existing MTR tests cover SQL-visible multi-batch
and chained-CTE behaviour, while fragment ownership, CTE_SCAN /
CTE_LOOKUP operator roles, indexed children, scalar redistribution, and
early close are covered by block/NDB API tests and full-suite
multi-node runs.

Required outcome:

- list the block tests that exercise multi-fragment / multi-node CTE
  materialisation, CTE_SCAN, CTE_LOOKUP, redistribution, and scalar
  CTE redistribution: done in the N.4 audit;
- add a RonSQL MTR case only if it can reliably run under the existing
  suite configuration: not added, because the existing MTR coverage is
  already sufficient for SQL-visible behaviour;
- otherwise document that SQL-level multi-node coverage is represented
  by block/unit tests and full-suite multi-node runs, not by a normal
  single-node MTR test: done in the N.4 audit.

## Suggested order

1. N.1b first, because quote-safe compare infrastructure is low-risk
   and unlocks the literal tests.
2. N.1a and N.1c next; both touch real string CTE behaviour.
3. N.4 coverage audit before final wrap-up, because it should mostly
   be verification and documentation.
4. N.2 is closed as an explicit deferral.  N.3 remains as the final
   implement-or-defer decision.

## Completion criteria

- `ronsql.ronsql_minmax_string` includes the restored non-unique-index
  string CTE aggregation shape and the string-literal predicate.
- CTE output comparisons have an explicit accepted/rejected matrix.
- Batch-size SQL surface is deliberately deferred; NDB API
  `setBatchSize` remains covered by block tests and RonSQL uses default
  batch sizing.
- Early-close / LIMIT interaction is either implemented with tests or
  deliberately deferred.
- Multi-fragment / multi-node coverage is either represented in MTR or
  documented against block/full-suite tests.
- `cte_filter_phase_i.md`, `CLAUDE.md`, and this document agree that
  Phase N is the final RONDB-1050 wrap-up phase.
