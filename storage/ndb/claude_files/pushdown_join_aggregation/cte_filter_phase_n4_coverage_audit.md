# Phase N.4 - Multi-fragment / multi-node CTE coverage audit

## Status

Completed as a coverage audit.  No new normal RonSQL MTR case is added
in this step.

The existing tests already cover the important SQL-visible shapes:
multi-batch CTE scans, chained CTE phases, CTE_LOOKUP below a
multi-batch parent, scalar CTEs, and scanCte parents with real-table
children.  The parts that are genuinely multi-node-specific, such as
fragment-per-node skip handling and scalar redistribution ownership,
are better asserted by block/NDB API tests and by full-suite runs on a
multi-node cluster than by an ordinary single-node MTR test.

## Decision

Do not add a new MTR just to claim multi-node coverage.  A normal MTR
run cannot reliably prove the node ownership and per-fragment
distribution properties this phase cares about.  Instead, record the
coverage split:

- SQL-facing multi-batch and chained-CTE behaviour is covered by
  RonSQL MTR tests.
- Kernel/NDB-API fragment, scanCte, lookupCte, indexed-child,
  outer-join, and early-close behaviour is covered by block tests.
- True multi-node routing and redistribution remains a full-suite /
  multi-node-cluster verification responsibility.

## Block / NDB API coverage

### Core CTE materialisation and lookup

`storage/ndb/block_unit_test/testCteNdbApi.cpp` covers the core CTE
operator roles:

- Test 2: base CTE materialisation plus CTE_LOOKUP.
- Test 3: two-level CTE chain.
- Test 5 and Test 6: nested and three-level CTE chains.
- Test 7: second CTE depends on the first through lookupCte.
- Test 9: scanCte plus lookupCte self-join.
- Test 10: one CTE reads another through scanCte agg-feed.
- Test 12: lookupCte as a CTE materialisation root with a child.
- Test 13: lookupCte as a main-query internal node.
- Test 14: lookupCte as a CTE materialisation internal node.
- Test 17: readTuple main root with lookupCte child.

### CTE_SCAN root and fragment ownership

`testCteNdbApi.cpp` also covers CTE_SCAN-specific roles:

- Test 8: scanCte as main-query root, including the fragment-per-node
  skip behaviour where only `rootFragId < numDataNodes` emits a
  CTE_SCAN_REQ and the remaining fragments complete with zero rows.
- Test 15: scanCte as a main aggregation leaf.
- Test 16: scanCte as a CTE materialisation root non-leaf with a child.
- Test 18: scanIndex inside CTE materialisation.
- Test 22: scanCte parent with ordered scanIndex child and linked
  index bounds.  This is the N.1 regression shape for a scanCte parent
  producing child batches.

### Scalar CTEs

`testCteNdbApi.cpp` covers scalar CTE operator shapes:

- Test 19: scalar MAX CTE using descending scanIndex and maxRows=1.
- Test 20: cross-join of two scalar CTEs through scanCte root plus
  lookupCte child.
- Test 21: GREATEST(MAX, MIN) through CASE in the aggregation
  interpreter.

`cte_filter_phase_i17_redistribute.md` documents the scalar
redistribution design: scalar aggregate CTE results are consolidated
onto the transaction owner node, and only that scalar owner emits the
cluster-wide row.

### CTE filter and multi-batch API coverage

`storage/ndb/block_unit_test/testCteNdbApiFilter.cpp` covers:

- Tests 6-12: CTE_LOOKUP_REQ interpreted-code filters.
- Tests 12-14: CTE_SCAN root filters, aggregate feed, and empty result.
- Test 15: CTE_SCAN filter across a batch boundary.
- Test 16: CTE_SCAN root large result.
- Test 17: CTE_SCAN root with `setBatchSize(50)` over 500 groups,
  forcing SCAN_NEXTREQ cycles.
- Test 18: early close after partially reading a CTE_SCAN root.
- Test 19: real-table main scan with CTE_LOOKUP child under a
  multi-batch parent.
- Test 20: chained CTEs with multi-batch main SELECT output.
- Tests 21-23: inline typed CTE_LOOKUP filter coverage after the
  multi-batch paths.

### CTE outer joins

`storage/ndb/block_unit_test/testCteNdbApiOuterJoin.cpp` covers:

- Tests 1-2: scanCte parent with INNER and LEFT JOIN readTuple child.
- Tests 3-4: real-table scan with lookupCte LEFT JOIN child, including
  small batch size.
- Test 5: CTE subtree feeding LEFT JOIN aggregation.
- Test 6: scanCte parent with main aggregation.

## RonSQL MTR coverage

### CTE_SCAN SQL shapes

`mysql-test/suite/ronsql/t/ronsql_cte_scan.test` covers:

- scanCte root with grouped aggregation.
- WHERE predicates on scanCte aggregate output and group-by columns.
- CTE_SCAN root plus real-table child and aggregation.
- main aggregation over scanCte root with no GROUP BY.
- chained CTE feeding a real-table join.
- projection-only SELECT over a CTE_SCAN root.
- projection-only SELECT with WHERE on CTE output.
- CTE_SCAN root filter operator and conjunct matrix.

### Multi-batch SQL shapes

`mysql-test/suite/ronsql/t/ronsql_cte_multi_batch.test` uses 300
groups and crosses the 256-row batch boundary.  It covers:

- CTE_SCAN root over 300 groups.
- CTE_SCAN WHERE with a filtered result crossing the batch boundary.
- chained CTEs where both phases exceed one API batch.
- CTE_LOOKUP below a multi-batch parent scan.

### Chained / redistribution SQL shapes

`mysql-test/suite/ronsql/t/ronsql_cte_chained.test` uses 300 groups
and records the intended redistribution coverage in the test comments:

- three-level chained CTE with three CTE phases;
- sibling CTEs in the same phase;
- skewed CTE with a single-group result and empty results on other
  nodes;
- empty-intermediate chain.

These tests exercise the SQL-visible behaviours around phase ordering,
redistribution batch completion, FINAL_REP fan-out, empty `gb_map`
handling, and single-owner LDM routing.

### Scalar CTE SQL shapes

`mysql-test/suite/ronsql/t/ronsql_cte_scalar.test` covers:

- scalar MAX over populated input;
- scalar MAX over empty input returning NULL;
- scalar COUNT over empty input returning 0;
- multiple scalar aggregate outputs from one CTE;
- cross-join of two scalar CTEs using comma syntax;
- SELECT with no FROM over scalar CTE qualifiers;
- scalar CTE root WHERE predicates;
- nullable scalar output feeding top-level GREATEST;
- rejection of grouped CTE qualifiers in no-FROM SELECT.

## Remaining verification responsibility

No extra single-node MTR is required for N.4.  Before final RONDB-1050
wrap-up, the expected verification is:

- all RonSQL MTR tests pass;
- the relevant NDB API block tests pass, especially `testCteNdbApi`,
  `testCteNdbApiFilter`, and `testCteNdbApiOuterJoin`;
- at least one full-suite run on the normal multi-node cluster
  topology passes.

The explicit N.4 conclusion is that coverage exists and is split in
the right place: SQL regressions in MTR, kernel fragment and ownership
semantics in block tests, and true multi-node integration in full-suite
runs.
