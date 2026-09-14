# E6 — Spec-level random generator (2026-09-11)

**Status: DONE 2026-09-11 — generator agrees with the emitter on 2000
cases, 1 200 cluster cases across two seeds with zero unclassified
failures, F14 found, `ronsql_fs_fuzz_spec` recorded (see §5–6).**

## 1. What was built

| Piece | Content |
|---|---|
| `internal/fsq/fuzz/spec.go` | The seeded sampler of `random_generator.md` §4.1: case `(seed, i)` has its own PCG PRNG (`math/rand/v2`), so any case is regenerated without replaying the run; the id carries the grammar version (`spec-v1-<seed>-<i>`). 90 % serving cases: root entity group (customers 60 %, customers_str 20 %, balances 10 %, profiles 10 %) with 0–3 plain features on join 0; topology star / snowflake / both (55 / 35 / 10); star nodes (1–3) are aggregate (valid AggSpec inside the type matrix, window 60 %), collect (`<fg>_collect` array<struct> feature, N ∈ {1, 5, 50} within the width limit, ascending 50 %) or a plain self-join; the snowflake subtree is a prefixed self-join root with the FK chain regions → countries (depth 1–2), join types LEFT / INNER / mixed / RIGHT-FULL (60 / 30 / 7 / 3), join keys full / none / non-key column / unknown column (85 / 10 / 3 / 2); filters on aggregate/collect joins (0 / 1 / 2 / 3 leaves, OR node 3 %, feature-vs-feature 5 %, pinned to a join 50 %); mode single / batch 10 / 100 / 1000 (60 / 20 / 15 / 5); keys by class (ordinary 50 %, no-rows 10, exactly-N 10, bound-aligned 10, NULL hop 5, dangling hop 5, missing 10; balances: absent currency / account); helper options 10 % each with random label / helper flags. 10 % definition cases: a collect or aggregate definition on one history group, valid 60 % or one named violation (every `COLLECT_*` and `AGGREGATE_*` gate of `spec/validate.go`). |
| Expectation derivation | Each case carries what the ported emitter must produce: the gate code for a deliberate violation (`JOIN_ON_PARTIAL_PRIMARY_KEY`, `FOREIGN_KEY_NOT_PRIMARY_KEY`, `FEATURE_DOES_NOT_EXIST`, `COLLECT_UNSUPPORTED_ONLINE_FILTER`, `FEATURESTORE_ONLINE_NOT_ENABLED`, the definition gates) or the template count (aggregate: one unless batch with a composite key; collect: one unless batch; snowflake: one for all-INNER with a non-empty projection, one per node with a non-empty projection for all-LEFT, none for mixed / RIGHT / FULL / composite batch root — projections mirror `getTrainingDatasetFeatures` under the helper options), plus `KnownError F9` when MIN/MAX over the event time was sampled. String columns get one aggregate function each (F1 avoidance). |
| `internal/fsq/fuzz/shrink.go` | Delta debugging (`random_generator.md` §6): one-step candidates (drop a childless join, a filter, a selected feature, an aggregate entry or function, the window; halve the batch, then single; reduce collect N; clear helper options), accepted while the oracle keeps the failure class; `DirectCollect` rewrites the CTE collect template to the S6b form. |
| `.fs_fuzz` (`internal/shell/fs_fuzz.go`) | `spec --seed S --count N [--vectors] [--direct-collect] [--shrink] [--max-rows] [--dump-dir] [--json] [--ledger P --allow-known]`, `show --seed S --index I`, `replay --file results.json`. Per case: definition → `ValidateDefinition` vs the expected gate; serving → `emit.Build` vs the expected gate, template count vs the expectation, then every RonSQL template on MySQL and RDRS with the typed comparison (L1), `--vectors` folds the templates against the MySQL twin (L2, missing-equals-NULL only for all-LEFT subtrees). Outcomes: `PASS`, `GATED`, `NO-TEMPLATE`, `CLEAN-REJECT` (the CTE collect form, F0), `KNOWN-ERROR` (F9), `SKIP`; failures `GATE-MISMATCH`, `TEMPLATE-MISMATCH`, `BIND-ERROR`, `MYSQL-ERROR`, `ERROR`, `REJECT-UNEXPECTED`, `WRONG-RESULT`, `RETRY-EXHAUSTED`, `TIMEOUT`, `CRASH` (stops the run). `CASE` lines carry the shape signature; `SUMMARY fuzz-spec` counts per status; `SIGNATURE <hits> <signature>` de-duplicates failures; `--dump-dir` writes `<id>.json` (result + case) and `<id>.sql`; `--ledger` is a JSON `{"known": [{"signature", "finding"}]}` whose rows `--allow-known` turns into `KNOWN`. |
| `t/ronsql_fs_fuzz_spec.test` | Seed 1, 200 cases, 4 threads, quiet: the result records the SUMMARY (and SIGNATURE / failing CASE lines, which must be absent). |
| `findings/spec_fuzz.md` | The ledger (empty header until the first run). |

## 2. Decisions / notes

- The generator's expectations are the strongest self-check: the unit
  test `TestExpectationsAgreeWithEmitter` builds 600 cases and fails on
  any disagreement between the sampler's prediction and the emitter.
  Every disagreement found there is a generator bug (or a new emitter
  fact) and is fixed before the cluster ever runs.
- Snowflake subtrees follow the golden fixtures' layout: a label join 0
  over the entity group and a prefixed self-join root with the nested
  chain under it.  The data model has one FK chain per entity group
  (regions → countries), so subtrees are chains of depth 1–2;
  `transactions → merchants` is not sampled (its root key includes the
  event time).
- Every collect template is the Hopsworks CTE form and therefore
  `CLEAN-REJECT` (F0) until the engine accepts it; `--direct-collect`
  runs the S6b body instead so collect results are compared.  The MTR
  test keeps the Hopsworks form.
- Hazards (`random_generator.md` §5.4) are envelope-level (E7); the one
  spec-level hazard, F1, is avoided by construction (one function per
  string column).
- `--probe-gated` (running the would-be statement of a gated case) is
  not built in this cut.
- Shrinking re-classifies each candidate with the same engines; keys
  are kept, expectations relaxed (`Templates -1`); TIMEOUT / CRASH
  cases are not shrunk.

## 3. Verification (user-run)

```
# unit tests: determinism, expectations vs the emitter, keys, shrinker
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/tools/rondb-cli
go test ./internal/fsq/... ./internal/shell

# rebuild, then a look at one case
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/debug_build && make rondb-cli
../debug_build/runtime_output_directory/rondb --no-mysql --no-rdrs --no-rondis -e ".fs_fuzz show --seed 1 --index 4"

# interactive run against a started cluster with the data set loaded
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/debug_build/mysql-test
./mtr --suite=ronsql_fs ronsql_fs_smoke --start-and-exit
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis -e ".fs_load 0.01 4 500 --db test"
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_fuzz spec --seed 1 --count 200 --db test --sf 0.01 --json /tmp/fs_fuzz1.json --dump-dir /tmp/fs_fuzz1"
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_fuzz spec --seed 2 --count 1000 --db test --sf 0.01 --vectors --direct-collect --shrink --json /tmp/fs_fuzz2.json --dump-dir /tmp/fs_fuzz2 --quiet"

# record and verify the MTR driver test
./mtr --suite=ronsql_fs ronsql_fs_fuzz_spec --record
./mtr --suite=ronsql_fs ronsql_fs_fuzz_spec
```

Expected: no `GATE-MISMATCH` / `TEMPLATE-MISMATCH` (they would already
fail `go test`); `CLEAN-REJECT` for every collect case of the first run
(F0), `KNOWN-ERROR` for the rare temporal MIN/MAX cases (F9), `GATED`
and `NO-TEMPLATE` for the gated shapes; `PASS` for the rest.  Any
`WRONG-RESULT` / `REJECT-UNEXPECTED` / `ERROR` is a finding: send the
`CASE` and `SIGNATURE` lines and the dump.

## 4. Failure guide

- `GATE-MISMATCH` / `TEMPLATE-MISMATCH`: the generator's prediction and
  the emitter disagree — a generator bug; `.fs_fuzz show` prints the
  case.
- `BIND-ERROR`: the binder could not type a key or the fold could not
  read a result — framework.
- `MYSQL-ERROR`: the emitted statement is not valid MySQL — generator or
  emitter bug (the emitter is golden-tested, so suspect the generator's
  filter literals first).
- `REJECT-UNEXPECTED`, `WRONG-RESULT`, `ERROR`, `RETRY-EXHAUSTED`,
  `TIMEOUT`, `CRASH`: engine findings; `--shrink` gives the minimal
  case, the dump the statements.

## 5. Results

### `go test` run 1 (2026-09-11)

`TestExpectationsAgreeWithEmitter` reported 24 disagreements, all
generator-side, fixed before any cluster run: filters picked the
boolean `flag` / `is_active` columns (the emitter refuses boolean
literals — now the deliberate 5 % violation), a root without an FK chain
(balances) was labelled a snowflake (star path in the emitter), and two
deliberate violations in one case made gate precedence matter (now one
per case).  Run 2 green on 2000 cases.

### Run 1 — `.fs_fuzz spec --seed 1 --count 200` at sf 0.01 (2026-09-11)

`SUMMARY fuzz-spec seed=1 cases=200 clean-reject=27 gated=32
known-error=3 no-template=34 pass=94 wrong-result=10`, 9 signatures:

- **F14 (engine, new)**: 6 cases — every snowflake over the string-keyed
  `customers_str_1` returns no rows on RonSQL while MySQL returns the
  region / country row.  Diagnosed on the cluster: the CTE body alone
  returns its group, the integer-keyed twin returns the row, EXPLAIN is
  identical.  Ledger row in `findings/spec_fuzz.md`, expectation-table
  entry `F14`; the runner now reports the structural pattern as
  `KNOWN-WRONG`.
- **F8 rule (framework)**: 4 cases — `MIN` / `MAX` over `category`
  (grocery / Grocery are collation-equal) picked different
  representatives on the two engines; the generator now samples only
  `COUNT` over the collation-ambiguous columns.

Everything else behaved as predicted: 27 `CLEAN-REJECT` (the Hopsworks
CTE collect form, F0), 3 `KNOWN-ERROR` (temporal MIN/MAX, F9), 32
`GATED`, 34 `NO-TEMPLATE`, 94 `PASS` — no `GATE-MISMATCH`,
`TEMPLATE-MISMATCH`, `MYSQL-ERROR` or `REJECT-UNEXPECTED`.

### Run 2 — seed 1 rerun and seed 2 at scale (2026-09-11)

- Seed 1, 200 cases after the fixes: `clean-reject=27 gated=32
  known-error=3 known-wrong=6 no-template=34 pass=98` — the six F14
  cases now `KNOWN-WRONG`, the four collation cases `PASS`, no failure.
- Seed 2, 1000 cases, `--vectors --direct-collect --shrink`:
  `gated=199 known-error=30 known-wrong=26 no-template=201 pass=543
  wrong-result=1`.  With `--direct-collect` the collect cases run and
  pass (no `CLEAN-REJECT`); the L2 folds agreed on every vector.  The
  one failure was F3 at fuzz scale — `AVG(amount_dec)` of three rows,
  MySQL `299.716667` vs RonSQL `299.7167` — shrunk in 11 oracle calls to
  a single batch aggregate with one filter.  The canonicalizer now
  applies the F3 allowance (AVG outputs within 1e-4) in both the L1 and
  the L2 path; the engine-side F3 row stays open.

### Run 3 — seed 2 rerun and the MTR record (2026-09-11): PASS

Seed 2, 1000 cases, `--vectors --direct-collect`: `gated=199
known-error=30 known-wrong=26 no-template=201 pass=544`, no failure.
`ronsql_fs_fuzz_spec` recorded (seed 1, 200 cases: the SUMMARY line
`clean-reject=27 gated=32 known-error=3 known-wrong=6 no-template=34
pass=98`) and verified.

## 6. E6 exit

1 200 cases at MTR scale across two seeds with zero unclassified
failures: every non-pass outcome is a named gate, a missing template by
design, or a ledger entry (F0, F9, F14).  The one new engine finding,
F14 (string-keyed snowflake returns no rows), has a minimal repro and an
expectation-table entry.  Open items for later phases: `--probe-gated`,
a 10 k nightly run with `--shrink` and a dated seed, hazards (E7).
Next: E7 (envelope-level generator).
