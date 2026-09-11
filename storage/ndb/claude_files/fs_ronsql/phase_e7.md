# E7 — Envelope-level random generator (2026-09-11)

**Status: DONE 2026-09-11 — seed 1 and seed 2 green (1 200 cases),
`ronsql_fs_fuzz_env` recorded and verified; findings F14–F19 and one
resolved limit (see §5–6).  The `--include-hazards` run remains optional
(disposable cluster).**

## 1. What was built

| Piece | Content |
|---|---|
| `internal/fsq/fuzz/envelope.go` | The statement sampler of `random_generator.md` §5 over the feature-store schema: productions and weights of §5.2 — single-table aggregate with a PK-prefix WHERE (15 %), the S6b projection + ORDER BY + LIMIT (10 %), "CTE per feature group" (1–3 aggregating CTEs keyed by the entity, joined under an entity-table root or a CTE root, LEFT/INNER, aggregating main with GROUP BY, 20 %), snowflake from a CTE root with INNER (15 %) or LEFT (10 %) hops incl. string roots and batch roots, batch IN lists of 1/10/100/1000 (8 %), filters mixed in (IN lists, LIKE, IS [NOT] NULL, NOT, OR, `DATE_SUB` bounds, 8 %), HAVING / ORDER BY on GROUP BY columns and aggregate aliases / LIMIT with the key as tie-breaker (6 %), CTE-body ORDER BY / LIMIT (the F0 form and an aggregating top-N, 4 %), known-unsupported probes (4 %: partial-key CTE lookup, CTE_SCAN as outer-join child, INNER below LEFT, join without an index, GREATEST with `=`, GREATEST over a decimal, AVG over string / temporal, index hint on a joined table, cross-table WHERE, ORDER BY in a subquery, syntax probes: implicit alias, DISTINCT, BETWEEN, OFFSET, UNION, RIGHT JOIN). Every table carries `AS`, identifiers are backticked, columns qualified with several tables in scope. Each case is tagged with its constructs; case `(seed, i)` has its own PCG stream, id `env-v1-<seed>-<i>`. |
| Expectation table (`fuzz.Expectations`) | §5.3 as data: construct → reject / message pattern / ledger id, the patterns taken verbatim from `RonSQLPreparer.cpp` / `QueryPlanner.cpp` (the unit test reads the sources and fails on a pattern that is not there); plus the non-rejecting known findings F14 (string-keyed snowflake) and F9 (temporal MIN/MAX). |
| `internal/fsq/fuzz/hazards.go` | §5.4: the open HANG / CRASH rows of `_discovery_log.md` translated to this schema — D3, D4, D5, D6, D12, D18, D19, D20, D23 — each a single statement; the unit test parses the log and fails when an open HANG/CRASH row has no translation. |
| `.fs_fuzz envelope` (`internal/shell/fs_fuzz_env.go`) | `--seed S --count N [--include-hazards] [--only prod,prod] [--threads 4] [--timeout 15s] [--max-rows] [--dump-dir] [--json] [--ledger P --allow-known] [--quiet]`. Per case: MySQL (same text) → RDRS → classification: `PASS`, `CLEAN-REJECT` (the expected pattern of a tagged construct, or any table pattern for an untagged one), `KNOWN-WRONG` (F14), `KNOWN-ERROR` (F9), `PASS(was-expected-reject)` (an expected rejection now runs), `SKIP`; failures `REJECT-UNEXPECTED`, `REJECT-MISSING` (RonSQL ran what MySQL rejects), `WRONG-RESULT`, `MYSQL-ERROR`, `ERROR`, `RETRY-EXHAUSTED`, `TIMEOUT`, `CRASH` (stops the run). Hazards run after the random cases, single-threaded with a 10 s timeout and an RDRS probe after each; `PASS(hazard)` when one passes, `KNOWN-HAZARD` (non-failing) when it fails as recorded, and the loop stops at a crash. Output contract as the spec fuzzer (`CASE`, `SUMMARY fuzz-envelope`, `SIGNATURE`, dumps, JSON, ledger). |
| `t/ronsql_fs_fuzz_env.test` | Seed 1, 200 cases, hazards off, quiet. |
| `findings/envelope_fuzz.md` | The ledger (empty until the first run). |

## 2. Decisions / notes

- The generator builds statements directly (string assembly per
  production with construct tags) rather than through a general AST;
  the productions of §5.2 are small enough that a per-production
  renderer is clearer, and the syntax checker of §9 runs over the
  rendered text.  An AST-level shrinker is therefore not built; the
  cases are already minimal (≤ 3 CTEs, ≤ 2 hops).
- The "CTE per feature group" production keeps the entity bound inside
  the CTE bodies (IN lists) and lets the main aggregate with GROUP BY on
  the root key; a main-query WHERE on a parent column is D19 (a hazard)
  and is generated only as such.
- Probes outside both grammars (DISTINCT, BETWEEN, OFFSET, UNION, RIGHT
  JOIN, implicit alias) are classified by the RonSQL side alone: MySQL
  may run or reject them; `REJECT-MISSING` flags the case where RonSQL
  runs a statement MySQL rejects.
- Untagged rejections whose message matches an expectation-table row
  are `CLEAN-REJECT` with the row named, so a construct the sampler did
  not know it produced (e.g. a join on a column without an index) still
  lands in the right class; a rejection matching no row is
  `REJECT-UNEXPECTED` (a new table row or an engine finding).
- Hazards are not shrunk and never run by default; a `PASS(hazard)`
  is the signal to flip the discovery-log row to FIXED.

## 3. Verification (user-run)

```
cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/tools/rondb-cli
go test ./internal/fsq/fuzz/ ./internal/shell

cd /Users/mikael/mysql_trees/rondb_1121_fs_ronsql/debug_build && make rondb-cli
cd mysql-test
# (cluster started and data loaded as for E6)
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_fuzz envelope --seed 1 --count 200 --db test --sf 0.01 --json /tmp/fs_env1.json --dump-dir /tmp/fs_env1"
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_fuzz envelope --seed 2 --count 1000 --db test --sf 0.01 --json /tmp/fs_env2.json --dump-dir /tmp/fs_env2 --quiet"
# hazards: on a disposable cluster (a data-node crash ends the run)
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_fuzz envelope --seed 3 --count 20 --db test --sf 0.01 --include-hazards --json /tmp/fs_env_hazards.json"

./mtr --suite=ronsql_fs ronsql_fs_fuzz_env --record
./mtr --suite=ronsql_fs ronsql_fs_fuzz_env
```

Expected: `CLEAN-REJECT` for the probes and the CTE-form collect,
`KNOWN-WRONG` for string-keyed snowflakes (F14), `KNOWN-ERROR` for the
rare temporal MIN/MAX, `PASS` elsewhere.  Any `WRONG-RESULT`,
`REJECT-UNEXPECTED`, `REJECT-MISSING`, `MYSQL-ERROR`, `ERROR`,
`TIMEOUT` or `CRASH` is a finding; send the `SUMMARY`, the `SIGNATURE`
lines and the failing `CASE` lines.  The hazard run reports
`KNOWN-HAZARD` per recorded hazard and `PASS(hazard)` for any the engine
has fixed since the discovery log.

## 4. Failure guide

- `MYSQL-ERROR` on a non-probe production: the renderer produced text
  MySQL does not accept — a generator bug.
- `REJECT-UNEXPECTED`: a RonSQL rejection matching no expectation-table
  row — either a new row (engine limit) or an engine bug; the message
  names the construct.
- `WRONG-RESULT`: an engine finding on a shape beyond the Hopsworks
  emitter; cross-reference the D-ids of the discovery log.
- `TIMEOUT` / `CRASH` outside the hazard list: a new hazard — add a
  translation and a discovery-log row.

## 5. Results

### Run 1 — `.fs_fuzz envelope --seed 1 --count 200` at sf 0.01 (2026-09-11)

The run **crashed both data nodes** (F15) and then reported all 200 cases
as `MYSQL-ERROR` `Got error 4009 'No data node(s) available'`: RonSQL
brought the nodes down on one case, and because MySQL is queried first on
every subsequent case, the cascade masked the trigger.  The crash is a
real engine finding — `ndbmtd` error 2343 (failed ndbassert) at
`DbspjMain.cpp:15201`, stack `Dbspj::appendFromParent` ←
`Dbspj::sendJoinAggNullRow` ← `Dbspj::execCTE_LOOKUP_REF`: the null-row
path of a LEFT JOIN whose right side is a CTE_LOOKUP aggregation leaf,
which the `snow-left` and LEFT `cte-per-fg` productions exercise.  Ledger
`findings/envelope_fuzz.md` F15, `BUGS_TODO.md`.

Runner fix from this run: a MySQL error that means the cluster is down
(NDB 4009 / connection loss) now classifies as `CRASH` and stops the
run, instead of a cascade of `MYSQL-ERROR` lines that hides which
statement crashed the nodes.  A single-threaded rerun after a restart
therefore stops at the crashing case (the last `CASE` line before the
abort).

### Run 2 — F15 isolated single-threaded (2026-09-11)

`SUMMARY fuzz-envelope seed=1 cases=200 crash=1 known-error=1 pass=7
reject-unexpected=1 skip=190`: the crash-stop fired at case 9 (a victim
reporting the cluster-down MySQL error), and the trigger is its
predecessor, case 8 —

```
WITH `tx` AS (SELECT `customer_id` AS `k`, COUNT(*) AS `n`, SUM(`amount`) AS `s`
  FROM `transactions_1` WHERE `customer_id` IN (1012, 644, 91, 172, 486, 400, 1467, 880, 338, 1829) GROUP BY `customer_id`),
     `se` AS (SELECT `customer_id` AS `k`, COUNT(*) AS `n`, SUM(`duration`) AS `s`
  FROM `sessions_1` WHERE `customer_id` IN (1012, …, 1829) GROUP BY `customer_id`)
SELECT `c`.`customer_id`, MAX(`tx`.`n`), SUM(`tx`.`s`), MAX(`se`.`n`), SUM(`se`.`s`)
  FROM `customers_1` AS `c` LEFT JOIN `tx` ON `tx`.`k` = `c`.`customer_id`
  LEFT JOIN `se` ON `se`.`k` = `c`.`customer_id` GROUP BY `c`.`customer_id`;
```

which took 547 ms (the node dying) and then the cluster was down.  The IN
list includes ids above E (1012, 1467, 1829), so the LEFT join has misses
and the aggregating main emits a null row into the join-aggregation leaf.
F15 recorded with this exact statement.

Fix in the generator: `cte-per-fg` now emits INNER joins by default (the
LEFT-over-aggregated-CTE shape is F15 and lives in `fuzz/hazards.go`,
opt-in via `--include-hazards`); `snow-left` still provides the
`left-join` construct (projection-only main, no join-aggregation leaf,
verified safe in E3/E4).  A `reject-unexpected` also showed on case 8's
line (`Table not found.`) — an artifact of the node dying mid-request,
not a separate finding.

### Run 3 — envelope matrix after the F15 fix (2026-09-11)

`SUMMARY fuzz-envelope seed=1 cases=200 clean-reject=11 known-error=3
known-wrong=9 mysql-error=1 pass=162 pass(was-expected-reject)=1
reject-unexpected=13` — no crash (F15 gone from the default productions).
The 14 non-pass-but-flagged cases were classification gaps, all fixed:

- **HAVING (11)**: `HAVING <alias> > n` rejects with `Could not find
  column` — RonSQL resolves HAVING against base columns, not SELECT
  aliases (corrected-envelope: HAVING unsupported). Added as the
  expectation-table row `having` (tagged-only); ORDER BY on an alias +
  LIMIT without HAVING passes.
- **avg-string (2)**: AVG over a VARCHAR column rejects with the generic
  `Failed writing aggregation program. Please report a bug.` instead of
  the specific guard (F16); expectation row corrected, finding recorded.
- **orderby-in-subquery probe (1, MYSQL-ERROR)**: the probe put `LIMIT`
  in an `IN (subquery)`, which MySQL itself rejects; the `LIMIT` was
  dropped so MySQL accepts it and RonSQL's subquery ORDER BY rejection
  is what is tested.

Known-good outcomes: 9 `KNOWN-WRONG` (F14 string-keyed snowflakes), 3
`KNOWN-ERROR` (F9 temporal MIN/MAX), 11 `CLEAN-REJECT` (probes), 1
`PASS(was-expected-reject)` (a probe the engine now accepts).

### Run 4 — after the classification fixes (2026-09-11)

`SUMMARY … clean-reject=23 known-error=3 known-wrong=9 pass=162
pass(was-expected-reject)=1 reject-unexpected=2`: the HAVING and
avg-string cases now classify (CLEAN-REJECT), and 2 residual failures
surfaced a second finding — **F17**: HAVING combined with ORDER BY (on an
aggregate alias) and LIMIT drives an internal `Got record with fewer
aggregates than expected. Please report a bug.` error, where plain HAVING
rejects cleanly.  A HAVING case rejects whatever ORDER BY accompanies it, with one of two
messages depending on the ORDER BY target; the `having` expectation row
accepts both (alt-pattern) and F17 is recorded for the internal-error
variant.

### Run 5 — seed 1 green (2026-09-11)

A first multi-threaded rerun after the HAVING/F17 fix reported `crash=4`
across four unrelated productions; because each case draws from its own
PCG, those non-HAVING statements were byte-identical to the earlier
passing runs, so the crash was accumulated cluster damage (F15 had
crashed and restarted the nodes several times that session), not a new
deterministic bug.  A single-threaded run on a fresh cluster confirmed
it: `SUMMARY … clean-reject=25 known-error=3 known-wrong=9 pass=162
pass(was-expected-reject)=1`, zero failures.  Every non-pass outcome is a
ledger entry: 25 clean rejections (the probes, the CTE-collect form, and
HAVING), 9 F14 string-keyed snowflakes, 3 F9 temporal MIN/MAX, and one
probe the engine now accepts (`pass(was-expected-reject)`).

### Run 6 — seed 2 at scale, 1000 cases (2026-09-11)

Two passes.  The first surfaced, through the new result comparison of
expected-reject shapes that now run, two correctness regressions and one
lifted limit:

- **F18** — a partial-key CTE lookup (`probe,cte-partial-key`) now runs
  (it used to reject `Partial CTE lookup key not supported.`) and returns
  the wrong row count (MySQL 400, RonSQL 229).
- **F19** — CTE_SCAN as an outer-join child (`probe,cte-scan-outer-child`)
  now runs (it used to reject) with a divergent result.
- **inner-below-left** — an INNER JOIN under a LEFT JOIN now runs and
  agrees with MySQL: the limit is genuinely lifted; its rejection was
  retired.

Also, AVG over a temporal column shares the F16 generic-message bypass
(`Failed writing aggregation program`), folded into F16.  F18/F19 were
reclassified `KnownWrong` (a result mismatch on them is KNOWN-WRONG, like
F14).  The second pass is clean: `SUMMARY … clean-reject=105
known-error=6 known-wrong=40 pass=849`, zero failures — every non-pass
outcome a ledger entry (F0/F6/F7 probes clean-reject, F9 known-error,
F14/F18/F19 known-wrong).

The envelope fuzzer has surfaced six engine findings on this schema
(F14, F15, F16, F17, F18, F19) and one resolved limit; the result
comparison of "expected-reject-now-runs" cases (F18/F19) is what caught
the two silent wrong-result regressions.

### Run 7 — the MTR record (2026-09-11): PASS

`ronsql_fs_fuzz_env` recorded and verified; the result holds the seed-1
SUMMARY line `clean-reject=25 known-error=3 known-wrong=10 pass=162`,
zero failures.  (Both the CMake binary at `runtime_output_directory/rondb`
and MTR need rebuilding for the new files; building the Go binary
directly to that path is the reliable route until a full CMake
reconfigure.)

## 6. E7 exit

The envelope generator runs green over 1 200 cases across two seeds and
the MTR regression, with every non-pass outcome a ledger entry.  It
surfaced six engine findings on the feature-store schema — F14 (string
snowflake wrong result), F15 (data-node crash in the join-aggregation
null-row path), F16 (AVG-over-non-numeric generic message), F17
(HAVING + ORDER BY + LIMIT internal error), F18 (partial-key CTE lookup
wrong result), F19 (CTE_SCAN outer-join child wrong result) — and
confirmed one lifted limit (INNER-below-LEFT).  The result comparison of
"expected-reject-now-runs" cases is what caught F18/F19.  Open: the
`--include-hazards` run (documents F15 and the discovery-log HANG/CRASH
shapes) belongs on a disposable cluster and is the one remaining
optional step; the nightly 10 k / dated-seed loop is future work.

```
# after ./mtr --suite=ronsql_fs ronsql_fs_smoke --start-and-exit and .fs_load
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_fuzz envelope --seed 1 --count 200 --db test --sf 0.01 --threads 1 --json /tmp/fs_env1_st.json --dump-dir /tmp/fs_env1_st"
```

Send the last few `CASE` lines: the `CRASH` line names the statement.
Once F15 has its minimal repro, its shape moves to `fuzz/hazards.go` (out
of the default productions), so the default envelope run no longer takes
the cluster down and the MTR record can be produced.
