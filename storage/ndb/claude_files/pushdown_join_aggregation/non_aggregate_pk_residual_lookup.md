# PK lookup with residual WHERE via NdbRecord OO_INTERPRETED

**Status: SHIPPED (2026-08-25, recorded green ×5 topology suites +
regression pass).**  The named follow-up from
`non_aggregate_phase_1.md`'s v1 policy: a single-table non-aggregate
query whose WHERE covers the full primary key with equalities but
carries additional conjuncts no longer falls back to the scan-config
path (a full table scan on a hash-PK table) — the residual conjuncts
ride the PK lookup as an interpreted filter program.

## Why NdbRecord

The RecAttr-style `NdbOperation::readTuple` has no interpreted-code
facility; `OO_INTERPRETED` exists only on the NdbRecord API.  Verified
API facts this design rests on:

- `OO_INTERPRETED` is valid on NdbRecord PK **reads**: the support
  matrix at `NdbTransaction.hpp` (readTuple: OO_ABORTOPTION,
  OO_GETVALUE, OO_PARTITION_ID, OO_INTERPRETED) and the enforcement in
  `NdbOperationDefine.cpp` (accepts ReadRequest/ReadExclusive).  The
  "only supported for update operations" comment on
  `OperationOptions::interpretedCode` is stale.
- `OO_GETVALUE` extra gets are old-school NdbRecAttr reads
  (`NdbOperationDefine.cpp`: each GetValueSpec gets
  `getValue_NdbRecord(...)` returning `NdbRecAttr*`), so the Phase 0b
  RecAttr pass-through printer stays byte-for-byte unchanged.
- An all-zero result mask plus extra gets is legal
  (`NdbOperationExec.cpp` emits the NdbRecord read set only when
  `requestedCols > 0`), so the result record/row are pure scaffolding.
- Kernel semantics are exactly right for a filter: for ZREAD the
  initial-read section executes **after** the interpreted program
  (`DbtupExecQuery.cpp`), and on EXIT_REFUSE no reads run.
- **Error semantics**: `NdbScanFilter`'s reject path is
  `interpret_exit_nok()` with the default error code **626**, whose
  classification is `NdbError::NoDataFound` — a fetched-but-rejected
  row surfaces to the API identically to a missing row.  The existing
  lookup-arm error handling (dual `execute() != 0 ||
  op->getNdbError().code != 0` check + NoDataFound ⇒ empty result)
  therefore needed zero change.  Note the NdbRecord `readTuple`
  defaults to `AO_IgnoreError`, so `execute(Commit)` can return 0 with
  only the op error set — the dual check covers that shape.
- Usage pattern (default record + `getRecordRowLength` buffer + PK
  bytes written into the key row + `readTuple(rec, key_row, rec,
  result_row, LM, zero_mask, &opts)`) is proven by
  `testInterpreter.cpp`.
- `encode_constant`'s output is memcpy-compatible with NdbRecord row
  value storage for every supported PK type (little-endian ints at
  column width, space-padded CHAR, length-prefixed VARCHAR, packed
  temporals) — the same bytes previously went into `op->equal()`.

The NdbQueryBuilder alternative (Phase 0 already attaches interpreted
filters to a readTuple root on the pushed-query path) was rejected:
TCKEYREQ→DBSPJ hop, QueryTree serialization, and a drain rewrite,
against Phase 1's plain-NDB-API principle.

## As implemented

- **Detection** (`RonSQLPreparer::detect_pk_lookup`): classifies each
  top-level conjunct as key (`pk_col = const` on a not-yet-bound PK
  column) or residual, recording `m_pk_lookup_cond_map` (PK ordinal or
  −1, the `condition_handling_map` idiom) +
  `m_pk_lookup_has_residual`.  All PK columns bound ⇒ lookup even with
  residuals.  Duplicate and contradictory PK equalities become
  residuals — the filter re-checks them against the fetched row, so
  `pk = 77 AND pk = 78` correctly returns an empty result (the
  single-table twin of the Phase 0a contradictory-PK fix).  When
  residuals exist, a **trial build** of the program (NdbInterpretedCode
  + NdbScanFilter + the unchanged `apply_filter`) gates the decision:
  over the shared 64-word `LOOKUP_FILTER_MAX_WORDS` cap (hoisted from
  the Phase 0 join-root site) or any throw (unsupported constant
  types, e.g. TIME(3)) ⇒ scan-config fallback, keeping error timing
  and classification identical to the pre-lookup behavior — including
  for EXPLAIN, which runs the same prepare path.
- **Execution** (`execute_single_table_passthrough`): the residual-free
  RecAttr arm is byte-identical; the residual arm uses
  `table->getDefaultRecord()`, an arena key_row filled per PK column
  via `encode_constant` + `NdbDictionary::getValuePtr`, scaffolding
  result_row + all-zero mask, the filter program rebuilt per execute
  (mirroring the scan arm's per-execute NdbScanFilter build), extra
  gets from `build_passthrough_getvalue_specs` (the NdbRecord twin of
  `register_passthrough_getvalues`, preserving the outputs-order ==
  attrs-order printer contract), then `m_trans->readTuple(...,
  LM_CommittedRead, zero_mask, &opts)` with
  `OO_INTERPRETED|OO_GETVALUE`.  Both arms share the execute + print
  tail (phase stats, NoDataFound ⇒ empty, LIMIT-0 suppression,
  deferred TSV header).
- **EXPLAIN**: unchanged `KEYS (n):` print without residuals; with
  residuals `Execute as primary key lookup.` + `CONDITIONS (k keys and
  m filters):` with per-conjunct `KEY[ordinal]:` / `FILTER:` labels in
  the index-scan idiom, driven by `m_pk_lookup_cond_map`.

## MTR (`body_passthrough_single_table.inc` ×5 topology suites)

st-4 (accepting residual) and st-5 (rejecting residual) flip to
PK-lookup with **fatal** EXPLAIN greps + `ronsql_phase_rows.inc` pins
(st-4 rows=1 — the scan fallback also drained 1 row since the filter
ran server-side, so the EXPLAIN grep is the actual flip detector;
st-5 rows=0 pins the 626-reject ⇒ empty path).  New cases: st-15
multi-conjunct residual, st-16 OR + IS NULL nesting, st-17 composite
hash-PK + residual, st-18a duplicate / st-18b contradictory PK
equality, st-19 over-cap OR chain (pinned as "not a lookup"), st-20
unsupported-type residual (TIME(3): plan-time fallback keeps EXPLAIN
working, execution still fails like the scan path always has), st-21 /
st-22 local VARCHAR-PK table `vkey1` (plain lookup + residual — the
NdbRecord key_row length-prefix encoding is the one genuinely new
byte-level path).

## Verification (user-run)

- Build `ronsql_cli` + `rdrs2` (RonSQL-only change).
- `./mtr --record --suite=ronsql_cte
  ronsql_cte_dd_passthrough_single_table` + the same in
  `ronsql_cte_ng1r3/ng2r2/ng2r3/ng4r2`.
- Regression: `./mtr --suite=ronsql ronsql_basic` + full `ronsql_cte`
  (aggregate and scan arms untouched — baselines must not move).

## Risks / notes

- The 626 conflation (absent vs rejected) is intentional and
  commented at the execute site; if NdbScanFilter ever changed its
  reject code, the NoDataFound classification check is the guard.
- `LM_CommittedRead` + interpreted TCKEYREQ is the one combination not
  directly pinned by an existing API test (testInterpreter uses
  LM_Read); lock mode is orthogonal at the DBTUP interpreter level —
  first `--record` proves it.
- The trial build runs once per prepared query (≤64 words) and is
  discarded; the execute arm rebuilds — consistent with the scan arm's
  per-execute filter build.
