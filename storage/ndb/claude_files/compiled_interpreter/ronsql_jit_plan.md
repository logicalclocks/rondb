# ronsql_jit / ronsql_cte_jit — full-corpus JIT enforcement for RonSQL

**Status: Slice 1 DONE & VERIFIED (2026-08-27 — all four suites
green at --parallel=6: both recorded jit arms, pin-stability re-runs,
and the first-ever pure-interpreter OFF runs of both corpora). The
recorded corpus census is below; slice 2 executes against it.**

**Goal (Mikael, 2026-08-25):** suites `ronsql_jit` and
`ronsql_cte_jit` that ENSURE every query in the `ronsql` and
`ronsql_cte` categories executes on compiled interpreter code —
extending Phase 6-2's conformance instrument from hand-picked
canaries to the full RonSQL query corpus. Successor milestone to
Phase 6 (`phase_6_plan.md` §5 holds the design notes recorded at
phase close).

## The three slices

**Slice 1 — census-first mirrors (enforcement OFF).** The
`ndb_push_agg`/`ndb_push_agg_jit` structure applied to the RonSQL
corpora:
- `suite/ronsql/my.cnf` and `suite/ronsql_cte/my.cnf` pin
  `CompiledInterpreter=OFF` (they had been running the JIT via the
  AUTO default since Phase 4 — the OFF arm is the first-ever pure
  interpreter run of these corpora; any result difference would be a
  JIT correctness bug, which five phases of differential testing say
  won't happen).
- NEW `suite/ronsql_jit` (55 mirrors) and `suite/ronsql_cte_jit`
  (25 mirrors + `ronsql_cte_jit_census` moved here natively — its
  must-JIT asserts belong in the JIT arm, and the OFF'd base suite
  would abort them). Each mirror is a one-line `--source` of the
  original bracketed by a recorded `programs_fallback` delta pin —
  the recorded value IS the pin, and the first recording IS the
  census: it enumerates, per test, every program the corpus submits
  that the bridge rejects.
- The ×4 `ronsql_cte_ng*` topology suites keep their own my.cnf
  (JIT on via AUTO): they test distribution, and running them with
  the JIT active is extra coverage, not a differential arm.

**Slice 2 — lower the gaps the census names.** Expected fronts, from
the Phase 6 backlog:
- Scan-filter (Phase 7) expansion: RonSQL WHERE filters emit beyond
  the v1 NULL-branch + comparison subset — the largest expected
  reject source (`testInterpreterTypedRegs` pinned 1888 counts of
  this family).
- CTE consumer filters (unparks Phase 6-5): structurally never reach
  the JIT today; consumer-side WHERE/HAVING shapes in the corpus
  need the CTE-filter compile + dispatch slice.
- Aggregation-op leftovers the census names (embedded
  `READ_AGG_REG_TO_REG` is already known from `testCteNdbApi`;
  GREATEST/LEAST, subquery shapes, whatever else appears).
Each front is its own implement → verify → commit slice, and the
pins flip from N to a smaller N (re-recorded deliberately) as gaps
close.

**Slice 3 — flip on enforcement.** A NEW error insert ("4060-lite"):
abort ONLY on a program-level miss (`m_jit_entry == 0` at an
aggregation dispatch; scan-filter equivalent for eligible filters) —
UNLIKE 4060 it lets by-design per-row fallbacks (NULL edges,
div-by-zero, ±2^53 guards) pass, because the corpora legitimately
contain them and "uses compiled interpreter code" must not outlaw
the per-row escape hatch. Armed suite-wide via the mirror wrappers
once slice 2 gets the pins to their floor; the remaining nonzero
pins become the committed exemption list.

## Slice 1 implementation record (2026-08-25)

- Both base my.cnf files gained the OFF pin with a pointer note;
  both jit suites `!include` the base config and override
  `CompiledInterpreter=ON` — same include-chain + later-wins
  mechanics as `ndb_push_agg_jit`.
- 80 mirror wrappers (55 + 25) in the exact 6-2 pattern; result
  files copied from the base suites (byte-identical bodies — the
  delta lines get their values on the first `--record`).
- `ronsql_cte_jit_census` moved (`git mv`) with no content change:
  its 4060 + counter-sum asserts now run under the suite whose
  config guarantees the JIT is on.
- Nothing else in either base corpus references `ndbinfo.jit` or
  arms JIT error inserts (checked), so the OFF arm is clean;
  `ronsql_hopsworks.pl` is referenced by absolute
  `$MYSQL_TEST_DIR` path, mirror-safe; base `disabled.def` is empty.

**Verification (Mikael runs; --parallel=6 for these suites on this
machine).**
1. Record the census (the 80 mirror pins; recording the census test
   too is safe — its assert.inc guards fail loudly even under
   --record):
   `./mtr --record --suite=ronsql_jit --parallel=6`
   `./mtr --record --suite=ronsql_cte_jit --parallel=6`
2. Normal runs prove pin stability:
   `./mtr --suite=ronsql_jit --parallel=6`
   `./mtr --suite=ronsql_cte_jit --parallel=6`
3. The OFF arms (first pure-interpreter run of these corpora):
   `./mtr --suite=ronsql --parallel=6`
   `./mtr --suite=ronsql_cte --parallel=6`

**Exit (slice 1) — MET (2026-08-27).** All four suites green; the
recorded deltas below are the corpus census.

## The corpus census (recorded 2026-08-27)

~1234 program rejects across 30 of 80 tests (50 tests are already
reject-free). Ranked:

| Test | Delta | | Test | Delta |
|---|---|---|---|---|
| ronsql_hopsworks | **304** | | ronsql_cte_greatest_least | 22 |
| ronsql_basic | **168** | | ronsql_orderby_stress | 16 |
| ronsql_emptytable_and_nulls | **144** | | ronsql_cte_scalar | 16 |
| ronsql_cte_greatest_least_v4 | **104** | | ronsql_constants | 16 |
| ronsql_cte_greatest_least_v5 | 54 | | ronsql_cte_greatest_least_v2a | 12 |
| ronsql_cte_dd_filter | 48 | | ronsql_cte_case | 10 |
| ronsql_cte_or_body | 44 | | ronsql_dbt3_1_2 | 8 |
| ronsql_cte_dd_bigquery | 44 | | ronsql_date_sub | 8 |
| ronsql_subquery_agg_ext | 38 | | ronsql_cte_greatest_least_v6 | 8 |
| ronsql_minmax_string | 36 | | ronsql_feature_view | 4 |
| ronsql_cte_dd_dtwide | 36 | | ronsql_cte_dd_chain_scalar | 4 |
| ronsql_cte_subquery | 32 | | ronsql_join_agg | 2 |
| ronsql_overflow | 24 | | ronsql_cte_dd_orderby_limit | 2 |
| ronsql_cte_greatest_least_v2b | 24 | | ronsql_cte_dd_main_root_index | 2 |
| ronsql_cte_dd_agg | 2 | | ronsql_cte_dd_index | 2 |

Family reading (from test names; slice 2 opens with a reason/opcode
attribution pass — 5119 or the fallback log — before lowering):
- **GREATEST/LEAST family ≈ 224** (v4 104, v5 54, v2b 24, base 22,
  v2a 12, v6 8): a coherent unlowered aggregation-op family — likely
  the single biggest structured win.
- **ronsql_hopsworks 304**: the feature-store workload test — the
  largest single source, mixed shapes, attribution needed first.
- **Core-shape tests** (ronsql_basic 168, emptytable_and_nulls 144):
  plain WHERE filters land here — expected to be dominated by the
  Phase 7 scan-filter v1 subset boundary.
- **Filter tests** (cte_dd_filter 48, cte_or_body 44): CTE-body WHERE
  = scan filters on the producer scan, plus consumer-side filters
  (the 6-5 surface).
- **Subquery family** (subquery_agg_ext 38, cte_subquery 32):
  decorrelation-generated shapes.
- **Strings/types/overflow** (minmax_string 36, dtwide 36,
  overflow 24): beyond-5F string shapes, wide types, overflow-probe
  programs.
- **The long tail of 2s** (dd_agg/index/orderby_limit/
  main_root_index/join_agg): likely one shared program shape each.

**Slice 2 order (provisional, re-rank after attribution):**
1. Attribution pass (5119 per top test → reason/opcode table).
2. GREATEST/LEAST lowering (~224, one family).
3. Scan-filter expansion for RonSQL's WHERE emission (ronsql_basic /
   emptytable_and_nulls / hopsworks share this).
4. CTE filters (6-5) for the consumer-side share of the filter tests.
5. The remaining named families by measured size.
