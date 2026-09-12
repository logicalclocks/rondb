# E3 — Executors, canonicalization, `.fs_verify`, golden MTR test (2026-09-09)

**Status: code written, NOT yet compiled or run.** Verification in §3;
results in §5.

## 1. What was built

| Path | Content |
|---|---|
| `internal/fsq/bind` | `Bind(sql, args)`: replaces the `?` markers outside literals/identifiers with scalars or lists (a list is wrapped in parentheses unless the marker already sits inside them: RonSQL `IN (?)` vs Calcite `IN ?`); literal renderers (`Int`, `Str` with `''` doubling and backslash doubling, `Timestamp` UTC with fraction only when non-zero, `Date`, `LiteralFor(offlineType, value)`); `Count`. Unit tests. |
| `internal/fsq/exec` | Engines behind one interface: **RDRS** (`POST /<APIVersion>/ronsql`, `outputFormat: JSON`, `x-ronsql-phases` parsed, `Classify` = HTTP 200 OK / 500 `Caught exception:` CLEAN-REJECT / 500 `RonSQLRetryableError` or 429 RETRY (3 attempts) / else ERROR; transport failure + failed probe = CRASH, otherwise TIMEOUT; `Header()` re-runs in TEXT for the empty-result schema), **MySQL** (one connection with `time_zone='+00:00'`, `sql.RawBytes` cells with explicit nullness, `ColumnTypes` for typing), **ronsql_cli** (one process per statement, exit 1 = CLEAN-REJECT, 3 = RETRY, signal = CRASH). `ParseJSONData` keeps output-name order and exact numeric tokens (`json.Number`) and rejects duplicate output names. Unit tests. |
| `internal/fsq/canon` | `Compare(ref=MySQL, got=RonSQL, opts)`: explicit nullness, DECIMAL as exact rationals (`10.5 = 10.50`), DOUBLE/FLOAT with relative tolerance (default 1e-9), TIMESTAMP with trailing fractional zeros removed, everything else exact bytes; output names exact (`--relaxed-headers` → HEADER-ONLY); rows ordered or as multisets; a multiset diff. Unit tests. |
| `internal/fsq/cases` | The deterministic case matrix: Hopsworks shapes emitted by the port over the E1 schema (`OnlineDB` = target database) and bound by key class — S1 (incl. AVG and DECIMAL variants), S2 windows at 1 h / 7 d / 30 d / 90 d, S3 batch 10 / 100 / 1000 (+ window), S4 filter operators incl. quoting, S6 CTE form (expected rejection F0) and the framework-built S6b, S7 INNER 1-/2-hop single and batch, S8 LEFT per-chain, S8b single-statement LEFT (derived from the INNER template), S9 composite key with and without window, S10 string keys incl. collation and quoting, and 28 edge-fixture cases (NULL propagation, DECIMAL/AVG/FLOAT formatting, big integers, F5 known-wrong, F6 expected error, quoting/IN/escapes/NULL-vs-`'NULL'`, TIMESTAMP(3)/(6) cutoffs, explicit-order collect, composite hops incl. swapped binding, F7 expected rejection, F1 hazards). `known.go` is the single expectation table. Unit tests pin determinism and the bound texts. |
| `internal/fsq/mtr` | `RenderTemplatesTest`: the golden MTR test from the matrix — `ronsql_compare.inc` blocks (strict; known-wrong recorded non-strictly; ordered → `$skip_sort`), rejection asserts through `ronsql_cli --execute-file` + `grep -qF` on the expected message, hazards as comments. |
| `internal/shell/fs_verify.go`, `fs.go`, `repl.go` | `.fs_verify` (flags per framework_design.md §10: `--shape`, `--case`, `--db`, `--sf`, `--engines`, `--threads`, `--timeout`, `--tolerance`, `--now`, `--include-hazards`, `--allow-reject`, `--relaxed-headers`, `--dump-dir`, `--json`, `--quiet`) printing `CASE …` lines, a `SUMMARY` line and per-shape `SHAPE <id> SUPPORTED|UNSUPPORTED|FAILED`; exit status non-zero on failures. `.fs_show` prints the bound statements. `.fs_emit_mtr --cases` also writes `t/ronsql_fs_templates.test`. |

Outcome vocabulary of a CASE line: `PASS`, `PASS(was-expected-reject)`,
`PASS(was-known-wrong)`, `PASS(was-known-error)`, `HEADER-ONLY`,
`REJECT(expected)`, `REJECT`, `REJECT(allowed)`, `KNOWN-WRONG`,
`KNOWN-ERROR` (verify-only `Case.KnownError`, F9), `WRONG-RESULT`, `MYSQL-ERROR`,
`CLEAN-REJECT`/`RETRY-EXHAUSTED`/`TIMEOUT`/`CRASH`/`ERROR` (engine
outcomes), `HAZARD-SKIPPED`.

## 2. Deviations / notes

- L1 compares the RonSQL statement text against the same text on MySQL.
  After the E3 review, `Case.Groups` preserves each original DTO and its
  bound production MySQL query alongside zero or more RonSQL templates.
  MySQL-only DTOs are retained and missing twins are explicit. L1/MTR
  still use the flattened RonSQL statements; E4 will consume the groups.
- The requirements manifest of the review (A7) is present in a first
  form: every case carries its shape id and the SHAPE lines classify
  SUPPORTED / UNSUPPORTED / FAILED; `--requirements` strict mode is E8.
- `.fs_verify` needs the MySQL and RDRS connections of the shell; the
  `cli` engine is optional (`--engines rdrs,mysql,cli` with `--cli
  <path>` or `RONSQL_CLI_EXE`, plus `NDB_CONNECTSTRING`).

## 3. Verification (user-run)

```
# unit tests (no cluster)
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/tools/rondb-cli
go test ./internal/fsq/...

# build, then generate the golden MTR test from the matrix
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/debug_build && make rondb-cli
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql
debug_build/runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis \
    -e ".fs_emit_mtr mysql-test/suite/ronsql_fs/include --sf 0.01 --db test --cases"
debug_build/runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis -e ".fs_show --shape S7" | head -30

# record and verify the golden test
cd debug_build/mysql-test
./mtr --suite=ronsql_fs ronsql_fs_templates --record
./mtr --suite=ronsql_fs ronsql_fs_templates

# interactive verification against the same cluster
./mtr --suite=ronsql_fs ronsql_fs_smoke --start-and-exit
../runtime_output_directory/rondb --mysql-port <MASTER_MYPORT> --rdrs-port <RDRS port> --no-rondis \
    -e ".fs_load 0.01 4 500 --db test"
../runtime_output_directory/rondb --mysql-port <MASTER_MYPORT> --rdrs-port <RDRS port> --no-rondis \
    -e ".fs_verify --db test --sf 0.01 --json /tmp/fs_verify.json --dump-dir /tmp/fs_verify"
```

Expected: the MTR test records with empty diffs for every strict block
(known rejections asserted, F4/F5 recorded, F1 commented); `.fs_verify`
prints one CASE line per case, `SUMMARY` with `pass=…
reject(expected)=3 known-wrong=2 hazard-skipped=2`, and `SHAPE` lines
with S6 and EDGE UNSUPPORTED (expected rejections / known-wrong) and the
rest SUPPORTED.

## 4. Failure guide

- `go test` compile errors: the shell file `fs_verify.go` and the
  `cases` package are the largest new pieces; paste the errors.
- `cases` unit test failing on a bound text: a binding or emitter
  detail — the test prints the statement.
- `.fs_verify` `MYSQL-ERROR` on a framework-built statement: the SQL of
  that case is wrong (the emitter-built ones passed the golden fixtures);
  `WRONG-RESULT` on an EDGE case not in the known table is a new finding.

## 5. Results

### Run 1 — `ronsql_fs_templates --record` (2026-09-09)

Failed at the fifth case, `S1-avg-k31`: MySQL prints `AVG(score)` over
DOUBLE as `68.625`, RonSQL as `68.6250` (F3), and the golden test compared
byte-strictly (the case note referred to the Go canonicalizer, which the
MTR path does not use).  Cases 1–4 (S1 aggregates over 300 / 1 / 0 rows
and a missing entity) were byte-identical.

Fix (framework): `cases.Case.Canon`, set to `CanonNumeric` automatically
for statements that use `AVG` or touch a DECIMAL / DOUBLE / FLOAT column of
the data set (schema-driven, `builder.frac`); the MTR generator renders it
as the compare include's sed hook, stripping trailing fractional zeros on
both sides (`68.6250` → `68.625`, `10.50` → `10.5`, `0.00` → `0`).  Nine
cases carry it: `S1-avg-k31`, `S1-decimal-k31`, `EDGE-null-decimal`,
`EDGE-null-avg`, `EDGE-all-null`, `EDGE-float-exact`, `EDGE-big-safe`, and
the two non-strict `EDGE-float-rounding` (F4) / `EDGE-decimal-large` (F5).
The generated test was patched by hand to the generator's exact output so
the rerun needs no rebuild; a regenerate after the rebuild must leave
`git diff` on the test file empty.

Pre-screened against the smoke evidence before the rerun: zero-row
pass-through results print nothing on both engines (INNER hops on NULL /
dangling keys are safe); `LIKE` goes through `NdbScanFilter::COND_LIKE`,
i.e. the data node's collation-aware wildcard compare.

### Run 2 — `--record` then verify (2026-09-09): PASS

Both the record run and the verify run passed.  The recorded result has
90 blocks (88 compare blocks, the data-set load and the cleanup); every
strict block diffed empty, including the nine numeric-canonicalized ones.
The only recorded diffs are the two non-strict known-wrong cases, exactly
as in the smoke run:

- `EDGE-float-rounding` (F4): `MAX(f_float)` RonSQL `123456.7890625` vs
  MySQL `123457` (FLOAT display precision; same value).
- `EDGE-decimal-large` (F5): `MAX(dec_big)` RonSQL `1000000000000000` vs
  MySQL `999999999999999.99` (wrong value; engine tree).

The three rejection asserts (`S6-cte-k21/k31/k16` → F0 message,
`EDGE-big-overflow` → F6, `EDGE-comp-binary` → F7) held through
`ronsql_cli`; the two F1 hazards stayed commented out.  Thirteen
pass-through statements returned zero rows on both engines with no
header on either side: `S6b-k16`, `S7-1hop-k13`, `S7-2hop-k13`, `S8-chains-k13`, `S8-chains-k13`, `S7-1hop-k29`, `S7-2hop-k29`, `S8-chains-k29`, `S8-chains-k29`, `S7-2hop-k16`, `S8-chains-k16`, `S7-2hop-k18`, `S8-chains-k18`.

Exit criterion of E3 for the MTR half is met: every supported Hopsworks
shape (S1–S4, S6b, S7, S8, S8b, S9, S10) and every edge fixture agrees
between MySQL and RonSQL on the deterministic data set under
`$strict_diff=yes`; the unsupported ones are rejection-asserted with a
findings entry.  Remaining: the interactive `.fs_verify` run (§3, last
block).

### Run 3 — `.fs_verify --db test --sf 0.01` (2026-09-09): 16 failures, all classified

`SUMMARY cases=89 error=5 hazard-skipped=2 known-wrong=2 pass=64
reject(expected)=5 wrong-result=11`; S3-b1000 (verify-only) passed in
171 ms.  The 16 failures fall into two classes:

1. **11 × `WRONG-RESULT output names differ`** (`S6b-k16`, `S7-*-k13/k29`,
   `S7-2hop-k16/k18`, `S8-chains-k13/k29/k16/k18`) — framework.  Every
   one is a pass-through statement that returns zero rows on both
   engines (the MTR run recorded them as `Number of output lines,
   including header: 0`).  RonSQL's JSON form of an empty result is
   `{"data":[]}` with no column list, and the TEXT header probe the
   verifier fell back to prints nothing for zero rows either, so the
   RonSQL side had no output names.  Fix: `canon.Compare` treats two
   empty results as equal with the note `empty result on both engines
   (RonSQL carries no column list)` (a non-empty MySQL result against an
   empty RonSQL one still fails, and an empty result with a different
   header still fails); the header probe call was removed from the
   verifier.  Output names of zero-row results are unverifiable by
   construction — noted in `framework_design.md` §7.
2. **5 × `ERROR malformed JSON result: invalid character '-' after object
   key:value pair`** (`EDGE-float-exact`, `EDGE-date-range`,
   `EDGE-ts3-cutoff`, `EDGE-ts6-cutoff`, `EDGE-ts0-batch`) — **engine,
   new finding F9**.  Every one aggregates `MIN`/`MAX` over a `DATE` or
   `TIMESTAMP(n)` column; `ResultPrinter::print_aggregate_value`
   (`ResultPrinter.cpp:2401`) prints the temporal value with an empty
   quote in every output format, so the JSON body carries a bare
   `1970-01-01` token.  TEXT output is correct, which is why the golden
   MTR test passes the same statements strictly.  RDRS defaults to JSON
   and Hopsworks never sets `outputFormat`, so a feature view with a
   `min`/`max` aggregate over a timestamp feature gets an unparsable
   serving response.  Framework: expectation-table entry `F9`, new
   verify-only `Case.KnownError` (the MTR TEXT compare stays strict),
   reported as `KNOWN-ERROR` (counted with the known outcomes, shape
   `UNSUPPORTED`), `PASS(was-known-error)` once fixed; the JSON parse
   error now quotes the text around the offset, and the dump directory
   keeps the raw RonSQL body of an execution error.

Also fixed from this run: `isFailure` now treats every `PASS(...)`
status as a pass (`PASS(was-known-wrong)` was counted as a failure);
the CLI no longer prints the flag list after a failed `-e` command
(`SilenceUsage`/`SilenceErrors`, flag errors keep the usage text).

### Run 4 — `.fs_verify` rerun after the fixes (2026-09-09): PASS

`SUMMARY cases=89 hazard-skipped=2 known-error=5 known-wrong=2 pass=75
reject(expected)=5`, exit status 0 — exactly the prediction: the eleven
zero-row cases pass with the empty-result note, the five temporal
MIN/MAX cases report `KNOWN-ERROR F9`, S6b/S7/S8 are `SUPPORTED`, S6 and
EDGE `UNSUPPORTED` (expected rejections / known outcomes only).

## 6. E3 exit

Both halves of the exit criterion hold: the golden MTR test compares
every supported Hopsworks shape and every edge fixture strictly and is
green; `.fs_verify` reproduces the same matrix live with typed
comparison, every non-pass status backed by an expectation-table entry.
Findings ledger after E3: F0–F9 (`findings/smoke.md`), of which the
engine tree owns F1 (crash), F5 (wrong value), F7 (unsupported type)
and F9 (invalid JSON); F2/F3/F4 are formatting; F6 expected; F8
framework, fixed.  Next: E4 (vector oracle).
