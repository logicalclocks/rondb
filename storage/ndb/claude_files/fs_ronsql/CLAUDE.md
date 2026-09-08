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
- Later: `benchmarks.md` (P5), `phase_e<N>.md`.

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
