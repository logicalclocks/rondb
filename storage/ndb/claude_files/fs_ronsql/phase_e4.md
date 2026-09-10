# E4 — L2 vector-level equivalence (2026-09-10)

**Status: L2 implemented; six review corrections applied, user verification
pending. A6 captured Java SQL corpus regression passed; unit-test confirmation
and A6 MTR coverage remain pending; see §§6–7.**

Sections 1 and 5 describe the initial commit `12f397e589f` and its recorded
results. Those results do not verify the subsequent review corrections.
Sections 2–4 and 6–7 describe the revised behavior and remaining work.

## 1. Initial implementation (12f397e589f)

| Piece | Content |
|---|---|
| `internal/fsq/vector` | The L2 oracle of `framework_design.md` §8. `PlanFor(dto, batch, structFields)` derives the fold recipe of a DTO (kind aggregate / collect / snowflake / point-read, prefix, parameter names, prefixed aggregate outputs and which of them default to 0, the collect feature, order column, direction and struct fields). `FoldRonsql(plan, results, keys)` folds the template results: aggregate outputs get the join prefix, batch entities without a GROUP BY row get the empty-set defaults (COUNT 0, others NULL), collect rows are sorted by the order column (newest first, oldest first when ascending) and folded into one array feature whose elements carry exactly the struct fields, snowflake templates are overlaid by output alias (batch rows keyed by the appended root key). `FoldMysql(plan, result, keys)` folds the production statement (already-prefixed aliases, prefixed key columns in a batch, the `hopsworks_collect_rank` helper dropped, LEFT JOIN misses as NULL cells). `Compare(plan, ref, got, keys, types, policy, only)` compares per entity with the canonical cell comparison of `canon` (DECIMAL exact, DOUBLE tolerance, TIMESTAMP fractional zeros); `Policy.MissingEqualsNull` accepts a feature missing on one side against NULL on the other and counts it as a LEFT-MISS; ref features outside the compared set are reported as not served. Unit tests on synthetic results. |
| `internal/fsq/cases/specs.go` | The spec catalog: 22 feature-view specifications, one or more per shape (S1 point aggregate; S2 7/30-day windows; S3 batches of 10 and 100; S4 `>=`, `=` under the collation, `LIKE`, filter + window in a batch; S6 Hopsworks CTE collect (expected F0 rejection); S6b direct collect N=5 newest-first, ascending, N=50; S7 INNER 1-hop / 2-hop and a 2-hop batch; S8 LEFT per-chain 2-hop and batch; S9 composite key with and without a window; S10 string key single and batch). `SampleKeys(seed, count)` returns ≥ count keys: two customers of every key class first (empty / one / five / fifty / max history, day-bound and hourly spacing, NULL and dangling region, NULL and dangling country, upper-case string key, missing ids), then seeded random ids; account/currency pairs incl. an unknown currency and a missing account; string keys incl. the lower-case form of an upper-case key and `O'Brien`. `Units` groups keys per statement; `Bind` binds every DTO through the E3 binder (`Case.Groups` semantics: DTO + MySQL twin + RonSQL templates); `DirectCollect` rewrites the CTE template to its body (the S6b form) so the collect fold runs while F0 stands. |
| `internal/fsq/cases/expect.go` | Data-model expectations (`framework_design.md` §8: "two identical folds are not sufficient"): `Expected(key)` computes the vector from the data formulas for the aggregate outputs (rows admitted by the window and the filter predicate), the collect array (newest N rows, reversed when ascending), the snowflake chains (region / country by `RegionOf` / `CountryOf`, INNER dropping the whole row on a country miss, LEFT keeping the region chain), the composite-key history and the string-keyed transactions. `ExpectedFeatures` names the modelled features; root-FG features of a snowflake (served by the pk-read path, not by RonSQL) are outside the check. |
| `.fs_verify --vectors` (`internal/shell/fs_vectors.go`) | `[--spec ID \| --shape S7,S8] [--seed 1] [--count 120]` plus the L1 flags. Per spec: `SPEC <id> <shape> <status> keys=N units=U compared=C left-miss=L [not-served=a,b] [<ms>] [message]`, mismatch detail lines (`key= feature= mysql= ronsql=`, `EXPECT key= feature= data-model= mysql=`, at most 10), then `SUMMARY vectors specs=N …` and `SHAPE` lines; `--dump-dir` writes the detail and the statements of the first failing unit; `--json` the results. Statuses: `PASS`, `PASS(was-expected-reject)`, `REJECT(expected)`, `FAIL` (vector mismatch), `EXPECT-FAIL` (MySQL path disagrees with the data model), `REJECT`, `ERROR`/`TIMEOUT`/`CRASH`, `MYSQL-ERROR`, `BIND-ERROR`, `FOLD-ERROR`. `--quiet` prints only failing SPEC lines and omits latencies so the MTR-recorded output is stable. |
| `t/ronsql_fs_vectors.test` | Loads the data set, then drives `$RONDB_CLI -e ".fs_verify --vectors --db test --sf 0.01 --seed 1 --count 120 --quiet"`; the result records the SUMMARY and SHAPE lines; a failing spec fails the exec. |

## 2. Decisions / notes

- Feature names of a vector are the feature-view names: the join prefix
  applied to RonSQL aggregate outputs after the fetch, the MySQL aliases
  as emitted (`tx_count`), snowflake template aliases (`r_region_name`),
  `<prefix><collectFeatureName>` for the collect array with unprefixed
  struct field names inside the elements.
- Batch keying: RonSQL rows are keyed by the unprefixed parameter
  columns (`customer_id`), MySQL rows by the prefixed aliases
  (`tx_customer_id`, `p_customer_id`); both folds synthesize aggregate
  defaults for requested entities without a row, so a one-sided row is
  a mismatch and a missing entity on both sides compares equal.
- Snowflake root features (`p_tier`, `p_age`) appear in the MySQL nested
  statement but in no RonSQL template (Hopsworks serves them through the
  pk-read path): the oracle compares the union of template-served
  features and lists the rest once per spec as `not-served`.
- INNER vs LEFT: with INNER templates a miss anywhere drops the whole
  row on both engines (both sides Missing); with LEFT per-chain
  templates RonSQL leaves the missed chain's features Missing while
  MySQL's LEFT JOIN yields NULLs. Only this direction is accepted by
  `MissingEqualsNull`, enabled only for LEFT snowflake comparisons and
  counted per cell as `left-miss`. Returned rows must contain their
  declared columns; missing aggregate/collect fields are not LEFT misses.
  The independent MySQL model expects explicit NULLs for LEFT-hop misses
  and is compared strictly; a missing root entity still expects no row.
- Zero-row results carry no column list on RonSQL (E3 run 3), so a
  collect over an empty history folds to an empty array on both sides
  and an INNER miss to no features; the comparison does not need the
  header.
- `S6` (CTE form) is in the catalog with the expected F0 rejection so
  the run reports `PASS(was-expected-reject)` the day the engine accepts
  it; the folds are exercised through the S6b direct form.
- MySQL-only DTOs (composite batch aggregate, plain point reads) have no
  RonSQL vector and are counted as skipped groups inside a spec. Missing
  MySQL queries are `REFERENCE-ERROR`, not skips. A spec with no compared
  cells, or an executable group with no compared cells, is `UNTESTED`
  and non-failing, never `SUPPORTED`. The reason for gating is not inferred
  from an absent template. `queryOnlineScan` is not folded in E4.
- The spec-level key sampler is shared with E6 (`SampleKeys`), which will
  draw random specs and reuse `Bind` / `Plan` / `Expected`.

## 3. Verification (user-run)

```
# unit tests (no cluster)
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/tools/rondb-cli
go test ./internal/fsq/... ./internal/shell

# build the CLI
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/debug_build && make rondb-cli

# interactive run against a started cluster (ports from var/my.cnf)
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/debug_build/mysql-test
./mtr --suite=ronsql_fs ronsql_fs_smoke --start-and-exit
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_load 0.01 4 500 --db test"
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_verify --vectors --db test --sf 0.01 --seed 1 --count 120 --json /tmp/fs_vectors.json --dump-dir /tmp/fs_vectors"

# verify existing recorded output after the review corrections
./mtr --suite=ronsql_fs ronsql_fs_templates ronsql_fs_vectors
```

Expected after rebuilding (not yet reverified): 22 SPEC lines,
`V-S6-cte-n5 REJECT(expected) F0`, every other spec `PASS` with `left-miss` > 0 only on the S8 specs and `not-served`
listing the root features on the S7/S8 specs;
`SUMMARY vectors specs=22 pass=21 reject(expected)=1`; SHAPE S6
UNSUPPORTED, the rest SUPPORTED.

## 4. Failure guide

- `FAIL … key=K feature=F mysql=… ronsql=…`: the two paths disagree for
  an entity — an engine result difference (compare the L1 case of the
  same shape) or a fold bug; `--spec ID --count 5` narrows it, the dump
  has the bound statements.
- `EXPECT-FAIL`: the MySQL path disagrees with the data formulas — the
  formula port in `expect.go` or the data set (`.fs_load` checksum) is
  wrong before the engine is suspected.
- `FOLD-ERROR`: missing declared output/key columns, malformed
  aggregate/snowflake row widths, NULL/unrequested batch keys, duplicate entity rows within one
  template, or other invalid fold input. Separate snowflake templates
  may still overlay the same entity; collect arrays retain multiple rows.
- `BIND-ERROR`: a key literal the binder cannot type.
- `SAMPLE-ERROR`: count exceeds the finite base key domain or is invalid;
  no queries are issued for that spec. Reduce count or use matching data
  at a larger scale.
- `REFERENCE-ERROR`: a DTO lacks its production MySQL query.
- `UNTESTED`: no L2 evidence for all or part of a spec; inspect the
  `mysql-only-groups` and `uncompared-groups` counters. JSON names are
  `mysqlOnlyGroups` and `uncomparedGroups`; counts are across execution units.
- `REJECT(allowed)`: an unexpected clean rejection accepted under
  `--allow-reject`; non-failing but unsupported. This flag never excuses
  malformed JSON, other execution errors, or earlier mismatches.

## 5. Initial results (before review corrections)

### `go test` run 1 (2026-09-10)

- `cases`: one wrong assertion of mine (the newest collect row of
  customer 21 has amount 768; 836 is the fifth) — test fixed.
- `vector`: a real gap — the compared feature set was derived from the
  rows RonSQL returned, so a LEFT chain that returned no row was reported
  as `not-served` instead of a LEFT-MISS.  `PlanFor` now takes the served
  aliases from the templates' final SELECT list (`hw_cnt` and the batch
  root-key alias excluded); every served alias is compared for every key.
- `mtr/TestTemplatesGolden` (added by the E3 review commit): the
  checked-in `t/ronsql_fs_templates.test` differs from the renderer —
  it has neither the F2/F3 header lines nor any of the nine
  `--let $canonicalization_script` blocks.  The E3 commit's file was
  regenerated with a stale binary after the MTR record run, which had
  used the hand-patched copy; the "empty `git diff`" check at the time was
  vacuous because the file was untracked.  The committed test would fail
  MTR at `S1-avg-k31`.  Fix: regenerate with a rebuilt binary (§3); the
  recorded `.result` is unaffected (the `--let` and `--error` lines are
  not echoed) and is re-verified by running the templates test.

### Interactive run — `.fs_verify --vectors --seed 1 --count 120` (2026-09-10): PASS

`SUMMARY vectors specs=22 pass=21 reject(expected)=1`; SHAPE S6
UNSUPPORTED (the CTE form, F0), every other shape SUPPORTED; exit 0.
Per spec (from `--json`):

| Spec | keys | units | cells compared | left-miss | not-served | RDRS ms |
|---|---|---|---|---|---|---|
| V-S1-agg, V-S2-agg-w7d/w30d, V-S4-ge-300/eq-grocery/like-gro | 120 | 120 | 1080 | 0 | — | 190–220 |
| V-S3-agg-b10, V-S4-window-b10 | 120 | 12 | 1080 | 0 | — | ~200 |
| V-S3-agg-b100 | 120 | 2 | 1080 | 0 | — | 67 |
| V-S6-cte-n5 | 120 | 1 | 0 | — | — | REJECT(expected) F0 |
| V-S6b-direct-n5 / -asc-n5 / -n50 | 120 | 120 | 120 | 0 | — | 111–119 |
| V-S7-inner-1hop | 120 | 120 | 240 | 0 | p_age, p_tier | 298 |
| V-S7-inner-2hop | 120 | 120 | 480 | 0 | p_age, p_tier | 326 |
| V-S7-inner-2hop-b10 | 120 | 12 | 480 | 0 | p_age, p_tier | 31 |
| V-S8-left-2hop | 120 | 120 | 480 | 102 | p_age, p_tier | 595 |
| V-S8-left-2hop-b10 | 120 | 12 | 480 | 102 | p_age, p_tier | 63 |
| V-S9-hist, V-S9-hist-w90d | 120 | 120 | 480 | 0 | — | 162–170 |
| V-S10-str | 122 | 122 | 1098 | 0 | — | 151 |
| V-S10-str-b10 | 120 | 12 | 1080 | 0 | — | 64 |

Reading: the 21 passing specs reported identical vectors on the RonSQL
template path and the MySQL path for their sampled entities, and the
MySQL path agreed with the data-model expectation (no `EXPECT-FAIL`).
The 102 `left-miss` cells of the two S8
specs are the NULL / dangling region hops (all four features) and the
NULL / dangling country hops (two features) of the sampled keys: RonSQL's
per-chain templates leave them missing, MySQL's LEFT JOIN yields NULL —
the R6 Hopsworks semantic, accepted by the policy and reported, never a
mismatch.  The `not-served` root features (`p_age`, `p_tier`) are what
Hopsworks reads through the pk-read path.  Batch specs cost about a
tenth of the single-statement specs in RDRS time.

Initial regression result: 21 specs passed over ≥ 120 keys, while the
S6 CTE spec stopped at its expected F0 rejection without a vector
comparison. This is not evidence for the later review corrections or
the outstanding captured Java SQL execution requirement (A6).

### MTR — `ronsql_fs_vectors --record` then verify (2026-09-10): PASS

The recorded result holds only the SUMMARY line
(`specs=22 pass=21 reject(expected)=1`) and the ten SHAPE lines, as
designed: latencies are omitted in `--quiet` mode and PASS lines are not
printed, so the file stays stable while the catalog grows; a failing
spec makes the driver exit non-zero and the test fail.  The regenerated
`ronsql_fs_templates.test` (20 lines restored: the F2/F3 header and the
nine canonicalization blocks) was verified against its recorded result
without re-recording.  Next: E5 (benchmarks).

## 6. Review corrections and remaining evidence (2026-09-10)

Applied corrections (not yet built or tested after review):

1. Limit missing-versus-NULL acceptance to LEFT snowflake comparisons,
   validate declared columns, and compare the explicit MySQL model strictly.
2. Preserve earlier vector/data-model failures and their evidence when a
   later unit returns an expected or allowed rejection.
3. Return sampling errors for impossible counts. The base capacities are
   E + 2 customer keys and 3A + 2 account/currency keys; single-string
   spelling probes remain additional. After 1024 consecutive duplicate
   draws, fill in domain order. Normal seeded sampling is unchanged.
4. Report missing references as errors and zero-comparison coverage as
   non-failing UNTESTED; count MySQL-only and uncompared executable groups.
5. Reject duplicate entity rows within one result and unrequested/NULL
   batch keys before folding; validate aggregate/snowflake row widths and
   retain cross-template snowflake overlays and empty-set aggregate defaults.
6. Honor --allow-reject only for clean rejections. Known rejections keep
   their finding ID; errors and previously observed mismatches still fail.

Next evidence to collect:

- [ ] User-run verification in §3, including the new shell runner tests.
  Agent checks remain read-only patch/report inspection and
  `git diff --check`; user-run A6 cluster evidence is recorded in §7.
  No successful post-fix full unit-test or MTR output has been supplied.
- [x] A6 implementation: `.fs_verify --golden` executes captured Java
  `queryOnline` against fixture-backed data and compares the Go MySQL twin
  and RonSQL vectors using identical keys/time; reports retain provenance.
- [x] A6 cluster execution evidence: single-fixture and full-corpus
  user runs completed; the full run passes with F0/F7 expected rejections.
  Unit-test confirmation and automated regression coverage remain in §7.

These are tracked in `mysql-test/suite/ronsql_fs/findings/BUGS_TODO.md`.
Known engine defects remain separate tasks. Discovery/regression success
for supported checks does not establish full Hopsworks requirements
acceptance; E8 remains deferred.

## 7. A6 implementation and user verification

The twelve A6 implementation patches and follow-up fixes are applied.
The user-built runner has passed the cluster regression below; a successful
post-fix full unit-test run has not yet been reported.
The implementation adds manifest-checked capture loading, typed DTO
binding, deterministic fixture data and ownership-scoped loading/cleanup,
captured-Java versus Go-MySQL comparison, RonSQL vector comparison, serial
orchestration, live adapters, and `.fs_verify --golden` with JSON reporting.
The existing `--vectors` mode and its independent data-model oracle remain
separate; the A6 oracle is the captured Java MySQL execution result.

Data version `a6-v1` uses `2026-06-01T00:00:00Z` and fixed keys covering
present/absent entities, window boundaries, NULLs, string keys and composite
pairs. Batch DTOs retain batch syntax for both singleton and combined key
requests. SQL is bound, not rewritten to rename captured databases.
Fixtures run serially against their own matching data; existing databases
are refused and only successfully created databases are owned for cleanup.

Follow the [CLI run guide](../../../../tools/rondb-cli/README.md#hopsworks-golden-query-verification-a6).
Use a disposable cluster, a new report path outside the corpus, and actual
MySQL/RDRS ports (the examples use 3306/4406; MTR ports can differ).

### User-run cluster evidence (2026-09-10)

The initial `aggregate_single` run passed all four requests on both paths.
The first full run reproduced the existing F7 binary-projection rejection.
A narrowly scoped F7 expectation was added; the later CMake
`CONFIGURE_DEPENDS` fix made newly added Go sources participate in rebuilds.
The rebuilt client then produced:

```text
SUMMARY golden selected=45 attempted=45 failed=0 mysql-pass=183 ronsql-pass=125 ronsql-rejected=15 ronsql-untested=43
```

Evidence: user-run `debug_build/bin/rondb`, MySQL port 13001 and no-key
RDRS port 13005, report `/tmp/a6-corpus-003.json`. The report was inspected
read-only; its SHA-256 is
`c757dc9646d837704740c9ed08f238828f71c201c0533befe89f35574d7891d7`.
This is a local artifact, not a committed report; preserve it outside
temporary storage before treating this record as durable evidence.
CLI/server binary revisions were not captured.

The report uses schema 1, data `a6-v1`, tolerance `1e-9` and
`allowReject=false`. Java provenance identifies Hopsworks commit
`f85a653bcafd1058b4da5c31daf31de43f818bbf` with a dirty tracked worktree;
the exporter-source and tracked-diff hashes remain in the report.

- 27 statement fixtures ran 183 requests; all Java/Go MySQL comparisons passed.
- 18 fixtures were non-execution classifications: 14 gates and 4 definitions.
- 125 RonSQL requests passed; 15 were expected rejections (12 F0, 3 F7).
- 43 requests had no RonSQL template: both MySQL queries were compared,
  but RonSQL remained UNTESTED.
- No stop flags, setup/cleanup errors or remaining owned databases were
  recorded. This is report evidence, not an independent database inventory.
- Seven binary-fixture requests returned no RonSQL rows and were labelled
  `PASS(was-expected-reject)`. These empty-result comparisons do not retire
  F7: single/1, single/2 and single/9 still reject binary output.

This establishes A6 discovery/regression evidence for the captured corpus,
not full Hopsworks support. No successful post-fix full unit-test run or
new A6 MTR run is claimed.

Remaining evidence and follow-up:

- [ ] User: run `go test ./internal/fsq/... ./internal/shell` from
  `tools/rondb-cli`, rebuild the CLI, and perform the §3 E4 regression checks.
- [x] User: run `aggregate_single`, then the complete golden corpus.
- [x] Inspect both comparison outcomes, rejection/untested counts,
  selected versus attempted fixtures, and recorded cleanup errors.
- [ ] Preserve the JSON artifacts durably. Record CLI/server binary revisions
  with subsequent runs and independently confirm fixture database cleanup.
- [x] Add `ronsql_fs_golden` MTR regression coverage for the captured corpus.
  The proposed baseline uses the observed CLI summary above and expects
  zero remaining fixture databases; it has not yet been verified by MTR.
- [ ] User: rebuild, then run and repeat the new MTR test without recording
  over the baseline first. Engine defects remain separate tasks in
  `mysql-test/suite/ronsql_fs/findings/BUGS_TODO.md`.

From `debug_build/mysql-test`:

```bash
./mtr --suite=ronsql_fs ronsql_fs_golden
./mtr --suite=ronsql_fs --repeat=2 ronsql_fs_golden
```

The test locates the source corpus through `MYSQL_TEST_DIR` and uses the
suite's MySQL/no-key RDRS endpoints. It requires a source checkout and a
non-Windows host for the corpus symlink. Each invocation keeps its JSON in
a fresh `log/ronsql_fs_golden-*/report.json` directory under that worker's
MTR vardir, preventing overwrite on repeat. MTR may clear these logs on a
later invocation: archive needed reports before starting another run.
The test never pre-drops or cleans up leaked fixture databases itself.
Its `.result` pins coverage counts, not just exit status; engine support
improvements or corpus changes require reviewing the baseline differences.

Known F0 collect and narrowly matched F7 binary rejections may be non-failing;
`--allow-reject` extends that only to clean rejections. Malformed JSON and
other errors still fail A6.
MySQL-only DTOs are compared on MySQL but remain RonSQL UNTESTED; definition
and gate captures provide classification, not execution evidence.
`queryOnlineScan`, pk-read fallback execution and full E8 acceptance remain
outside this command. Old E4 results in §5 do not verify these new paths.
