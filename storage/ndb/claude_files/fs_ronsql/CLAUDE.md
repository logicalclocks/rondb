# fs_ronsql — Feature-Store RonSQL test & benchmark framework (RONDB-1121)

Planning and phase notes for testing RonSQL with the SQL shapes that the
Hopsworks Feature Store online-serving builder generates.

- `fs_ronsql_plan.md` — master plan: survey, architecture decisions
  (D1-D7), planning phases P0-P5, execution phases E1-E8, risks, reuse
  map, command cheat sheet. Start here.
- `shape_catalog.md` — P1: every RonSQL statement shape emitted by
  `PreparedStatementBuilder.java` (S1-S10), `?` binding rules, MySQL
  twins, client post-processing, gates, engine-support status.
- `data_model.md` — P2: feature groups, DDL rules, closed-form data
  formulas, key classes, row counts per scale factor, loaders.
- `framework_design.md` — P3: Go package layout under
  `tools/rondb-cli/internal/fsq`, spec model, emitter-port contract,
  binding, executors, canonicalization, vector folds, CLI surface,
  MTR contract, unit-test matrix.
- `random_generator.md` — P4: spec-level and envelope-level fuzzers,
  seeding, outcome classification (RDRS error mapping), expectation
  table, hazards, shrinking, findings ledger, CLI.
- `benchmarks.md` — P5: the `fs_hw` registry entries generated from the
  emitter, placeholder scheme, runners, matrix-driver changes, plan pins,
  results and regression rule.
- `phase_e1.md` — E1: schema/DDL port, data formulas, loaders, `.fs_*`
  commands, `ronsql_fs` suite and smoke test; verification commands and
  results.
- `phase_e2.md` — E2: spec/view model (= golden fixture input), definition
  validators, emitter port, Calcite-shaped MySQL twins, golden conformance test.
- `phase_e3.md` — E3: binder, engines (RDRS JSON / MySQL / ronsql_cli),
  typed canonicalizer, case matrix + expectation table, `.fs_verify`,
  generated `ronsql_fs_templates.test`.
- `phase_e4.md` — E4: vector oracle (`fsq/vector` folds + policy compare),
  spec catalog with seeded keys and data-model expectations, `.fs_verify --vectors`,
  `t/ronsql_fs_vectors.test`; §7 the A6 golden runner (`.fs_verify --golden`)
- `phase_e5.md` — E5: the `fs_hw` benchmark registry generated from the emitter,
  placeholder resolver, `.bench_ronsql fs_hw`, matrix driver `--queries fs_hw --load fs`
- `phase_e6.md` — E6: spec-level fuzzer (`fsq/fuzz`: seeded sampler with emitter
  expectations, shrinker), `.fs_fuzz spec|show|replay`, `t/ronsql_fs_fuzz_spec.test`
- `phase_e7.md` — E7: envelope-level fuzzer (`fuzz/envelope.go` productions + expectation
  table, `fuzz/hazards.go` discovery-log translations), `.fs_fuzz envelope`,
  `t/ronsql_fs_fuzz_env.test`
- `phase_e8.md` — E8: requirements manifest (`cases/requirements.go`) and
  `.fs_verify --requirements` acceptance report, `emit.ConformFixture`, mirror suites
  `ronsql_fs_ng2r2` / `ronsql_fs_ng4r2` / `ronsql_fs_jit`, `scripts/refresh_golden_fixtures.sh`
- `adding_a_shape.md` — the one-page guide for adding a statement shape end to end
- `requirements_reports/<date>/<arm>.json` — `.fs_verify --requirements` reports per arm (base, jit, ng2r2)
- `ronsql_fs_support_plan.md` — the engine work plan derived from the findings: work packages A–I, milestones M1–M4, acceptance evidence per package
- `m1_plan.md` — M1 in implementation detail: proper HTTP error codes (M1.0), F9, F1 (verify-first), F0 collect CTE collapse, F7 binary projections
- `m2_plan.md` — M2 in implementation detail: exact DECIMAL / wide SUM (wire + kernel + API + printer), one overflow semantics, AVG / FLOAT display rules, JIT lowering, acceptance PASS
- `m3_plan.md` — M3 serving performance, starting with the M3.0 performance census: the whole registry (new `core` engine-primitive category + fs / offline_fs / tpch_cte / fs_hw) on the current engine, one matrix run, `ronsql_bench_triage.py` ranks what needs work; §6 = run 4 (2026-09-22, benchmark computer): findings F23–F25, F27, F13 closed
- `m3_experiments.md` — the experiment plans after run 4: X0 corrected cluster configuration, X1 the ~1 ms idle-wake stall (F25), X2 where the many-group cost is (F24), X3 point-shape throughput, X4 the data node failure after query-memory exhaustion (F27)
- `m3_wpf_plan.md` — WP-F in implementation detail (F23 / F12): IN lists on primary-key columns as a set of PK lookups (F1), multi-range index scans where lookups are impossible — PK prefix, secondary index (F2), CTE bodies / join roots (F3) — a branch-tree filter fallback (F4), pruning and more from the numbers (F5); mechanism as found, tests, targets
- `bench_results/<date>-<build>-run<N>/` — matrix runs: report.md, results.json, triage; run 4 carries the case logs behind F23–F27
- Later: `phase_e<N>.md` per execution phase.

Reference Hopsworks tree (read-only): `/Users/mikael/github/hopsworks_ronsql`.
Reusable infrastructure this framework builds on:
`mysql-test/suite/ronsql/include/ronsql_compare.inc`,
`mysql-test/suite/ronsql_cte/` (schema/data/body-include pattern),
`tools/rondb-cli/internal/shell/ronsql_bench.go`,
`storage/ndb/claude_files/compiled_interpreter/ronsql_bench_matrix.py`,
`mysql-test/suite/ronsql_cte/findings/_discovery_log.md` (ledger format).

Conventions: the user runs all builds and tests; every phase ends with
the exact commands. Table aliases in RonSQL need explicit `AS`. Join
shapes in MTR use `$suppress_ronsql_cli=yes` and `$strict_diff=yes`.
