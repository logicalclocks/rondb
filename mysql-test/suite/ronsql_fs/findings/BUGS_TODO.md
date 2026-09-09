# RonSQL feature-store bugs and deferred coverage

This is the work queue, not a requirements-acceptance gate. Regression
runs may succeed when supported cases pass and known limitations are
reported separately. A known defect must not excuse unrelated failures.
Engine fixes belong in separate tasks.

The F0-F9 reproductions and original observations are in [smoke.md](smoke.md).
F8 was a framework fixture issue and is already fixed.

## Engine and protocol work

- [ ] F0: support the Hopsworks CTE-form collect query over a partial key.
- [ ] F1: fix reused string aggregate storage (client/data-node crashes
  and possible wrong values). Keep hazardous probes opt-in until fixed.
- [ ] F2: resolve DECIMAL MIN/MAX scale formatting differences.
- [ ] F3: resolve AVG formatting/precision differences.
- [ ] F4: resolve FLOAT display differences between MySQL and RonSQL.
- [ ] F5: preserve DECIMAL values beyond 2^53 cents exactly.
- [ ] F6: decide BIGINT SUM overflow behavior relative to MySQL widening;
  retain the explicit expected-rejection probe meanwhile.
- [ ] F7: support emitted VARBINARY/complex snowflake projections.
- [ ] F9: quote temporal MIN/MAX values in JSON output. Malformed JSON
  must be decoded as an error, never a successful result. The known F9
  cases may report non-failing KNOWN-ERROR only when quoting their named
  temporal fields restores valid JSON that fully agrees with MySQL.
  The original malformed response remains an error and is retained.
- [ ] HTTP status: distinguish invalid SQL/syntax from server failures
  instead of returning HTTP 500 for these client errors. Preserve the
  current permanent-error classification until the protocol is changed.

## Deferred framework coverage

- [ ] Optional ronsql_cli execution: the adapter exists, but .fs_verify
  currently compares MySQL with RonSQL through RDRS only. Do not claim
  that adding cli to --engines executes an additional comparison.
- [ ] E4: implement vector-level comparison (--vectors), consuming
  Case.Groups: each retains the original DTO, its bound production MySQL
  query (nil if absent), and its zero or more bound RonSQL templates.
  MySQL-only DTOs are retained; missing queries are not RonSQL fallbacks.
- [ ] E8: implement requirements mode (--requirements). For now these
  flags add no checks; a successful run establishes only L1 regression
  results, not full Hopsworks requirements acceptance.
- [ ] Report deferred modes explicitly when requested without making
  their absence fail otherwise successful regression runs.

## Shape reporting

Edge probes retain their EDGE label and SQL, but also contribute to the
serving shapes listed in their explicit associations. --shape selection
includes those probes once; JSON results expose the associations as shapes.
SHAPE summaries describe the selected checks, not complete requirements
acceptance. Known defects and skipped hazards remain non-failing but mark
their associated shapes UNSUPPORTED. Direct collect probes do not replace
the emitted S6 CTE cases; E8 acceptance remains deferred.
