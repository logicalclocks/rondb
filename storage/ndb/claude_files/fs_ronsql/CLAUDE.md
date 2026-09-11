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
