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
  The user-run A6 corpus reproduced the type-17 pass-through rejection
  for snowflake_binary DTO 1, single/1, single/2 and single/9; all ten
  MySQL twin comparisons passed. Golden mode recognizes only that DTO's
  named type-17 rejection as REJECT(expected); binary support remains open.
- [ ] F9: quote temporal MIN/MAX values in JSON output. Malformed JSON
  must be decoded as an error, never a successful result. The known F9
  cases may report non-failing KNOWN-ERROR only when quoting their named
  temporal fields restores valid JSON that fully agrees with MySQL.
  The original malformed response remains an error and is retained.
- [ ] F10 (bench.md): mysqld crashes (`NdbSqlUtil::likeLongvarchar` require in the
  ordered-scan sorted merge) on a pushed aggregate with a VARCHAR GROUP BY key and
  an IN list (`fs_hw_strkey_batch100`, pushdown ON). Run the fs_hw matrix with
  `--engines ronsql,mysqld_nopush` until fixed.
- [ ] F11 (bench.md): pushed point aggregates fail with NDB error 4120 'Scan already
  complete' (`fs_hw_agg_point`, `_filter`, `strkey_point` at sf 1); unpushed and the
  windowed / GREATEST variants work.
- [ ] F12 (bench.md): RonSQL executes `IN (k1..kn)` as a table scan with an OR filter:
  S3 batch serving is 200–1000× slower than MySQL (204 ms for 10 keys, 4.4 s for 1000).
  Index ranges per key needed; the fs_hw plan pins record the table scan as observed.
- [ ] F13 (bench.md): snowflake point reads cost ~350 µs of CTE_SCAN round trips over the
  2–3 PK reads MySQL does (483–525 µs vs 120–172 µs).
- [ ] HTTP status: distinguish invalid SQL/syntax from server failures
  instead of returning HTTP 500 for these client errors. Preserve the
  current permanent-error classification until the protocol is changed.

## Deferred framework coverage

- [ ] Optional ronsql_cli execution: the adapter exists, but .fs_verify
  currently compares MySQL with RonSQL through RDRS only. Do not claim
  that adding cli to --engines executes an additional comparison.
- [x] E4: implement vector-level comparison (--vectors) using bound DTO
  groups, reconstructed MySQL queries and independent data-model vectors.
  The six review corrections are applied; Go unit tests passed.
- [ ] E4 review verification (user-run): confirm ronsql_fs_templates and
  ronsql_fs_vectors against their existing recorded results after review.
  Passing A6 MTR runs do not replace these separate L1/L2 tests.
- [x] A6 implementation: --golden runs captured Java queryOnline against
  matching fixture data, compares the Go MySQL twin and RonSQL vectors,
  and retains shared keys/time, provenance and comparison reports.
- [x] A6 cluster regression (user-run, 2026-09-10): all 45 fixtures
  processed, zero failures; 183 MySQL passes, 125 RonSQL passes,
  15 expected rejections (12 F0, 3 F7) and 43 RonSQL-untested requests.
  No setup/cleanup errors or remaining owned databases were reported.
  Evidence and the empty-result F7 caveat are in phase_e4.md §7.
- [x] A6 Go unit tests: user confirmed the post-fix
  go test ./internal/fsq/... ./internal/shell run passed.
- [x] A6 cleanup verification: the passing MTR test independently checked
  zero remaining fixture databases.
- [ ] Evidence retention: preserve report artifacts and record binary
  revisions on subsequent runs.
- [x] A6 automation implementation: ronsql_fs_golden runs --golden,
  retains uniquely named report directories, compares the observed summary
  baseline and checks both fixture databases are gone.
- [x] A6 automation verification (user-confirmed, 2026-09-10):
  ronsql_fs_golden passed both once and with --repeat=2.
  Retain needed logs before another MTR invocation clears them.
- [ ] Track coverage of MySQL-only/point-read fallback and queryOnlineScan
  separately: --golden compares MySQL-only DTOs on both MySQL paths, but
  neither golden nor vector mode executes the pk-read fallback or scan
  twin. Absent templates do not establish why Hopsworks gated them.
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
