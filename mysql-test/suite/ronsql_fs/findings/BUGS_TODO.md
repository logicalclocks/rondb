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
- [x] E4: implement vector-level comparison (--vectors) using bound DTO
  groups, reconstructed MySQL queries and independent data-model vectors.
  The six review corrections are applied; their verification is pending.
- [ ] E4 review verification (user-run): rebuild the CLI, run the fsq
  and shell unit tests, then verify ronsql_fs_templates and
  ronsql_fs_vectors against their existing recorded results.
- [ ] A6: execute captured Java queryOnline SQL from the Hopsworks golden
  fixtures against matching fixture data, and compare with both the
  reconstructed MySQL twin and the RonSQL vector path. Use the same
  keys/time and retain fixture provenance. E2 DTO conformance and the
  current data-model oracle do not replace this execution evidence.
- [ ] Track coverage of MySQL-only/point-read fallback and queryOnlineScan
  separately: vector mode skips MySQL-only groups and does not execute
  the scan twin. A skip does not establish why Hopsworks gated a template.
- [ ] E8: implement requirements mode (--requirements). This flag still
  adds no checks; successful selected L1 or L2 regression checks do not
  establish full Hopsworks requirements acceptance.
- [ ] Report deferred modes explicitly when requested without making
  their absence fail otherwise successful regression runs.

## Shape reporting

In L1, edge probes retain their EDGE label and SQL, but also contribute to the
serving shapes listed in their explicit associations. --shape selection
includes those probes once; JSON results expose the associations as shapes.
SHAPE summaries describe the selected checks, not complete requirements
acceptance. Known defects and skipped hazards remain non-failing but mark
their associated shapes UNSUPPORTED. Direct collect probes do not replace
the emitted S6 CTE cases; E8 acceptance remains deferred.

In vector mode, skipped MySQL-only groups are counted. A spec with no
compared cells, or an executable group with no compared cells, is UNTESTED
(non-failing), never SUPPORTED. Missing MySQL references are errors.
--allow-reject permits only clean rejections, reported as REJECT(allowed)
and UNSUPPORTED; malformed JSON and other errors remain failures, as do
earlier vector or data-model mismatches. LEFT-MISS applies only to missing
RonSQL snowflake chains against explicit MySQL NULLs.
