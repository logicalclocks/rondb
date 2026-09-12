# E8 — Hardening and CI (2026-09-11)

**Status: DONE 2026-09-12 — F20 root-caused and fixed in the NDB API
(regression `testDict -n InvalidateParkedByPointer` OK), the three
suites green ×3, requirements report verified, JIT census recorded
(delta 0 on every Hopsworks shape); F21 handled, F22 known (see §4–6).**

## 1. What was built

| Piece | Content |
|---|---|
| Requirements manifest (`internal/fsq/cases/requirements.go`, `req-v1`) | The versioned map of framework_design.md §9 / plan §3.1 A7: builder branches (R-S1 … R-S10), review corrections (R-A2 binary projections, R-A3 typed NULL / aliases, R-A5 types and composite hops, R-F1), and the Hopsworks gates (R-GATE-filter / -join / -silent / -definition) to mandatory evidence — case-matrix cases by shape and id (L1), vector specs by shape (L2), captured Java fixtures (conformance).  `Emitted` marks what Hopsworks emits today (must be SUPPORTED or HOPSWORKS-GATED for acceptance); R-S6b is `framework` and never satisfies an emitted requirement.  `ResolveRequirements`, `EvidenceClass`, `RequirementStatus` (FAILED > UNTESTED > UNSUPPORTED > GATED / SUPPORTED) and `Accepted`.  Self-check test: every id / shape / spec shape / fixture the manifest names exists, every Hopsworks-origin case is claimed, the folding rules hold. |
| `emit.ConformFixture` (`internal/fsq/emit/conform.go`) | Java conformance as a callable: runs the emitter (or the definition validator) on a fixture input and compares the complete output object with the captured expected object; `TestGoldenConformance` now uses it, so requirements evidence is exactly what the unit test asserts. |
| `.fs_verify --requirements` (`internal/shell/fs_requirements.go`) | The strict acceptance run: resolves the manifest, runs every required case on both engines (L1), every required spec with `--vectors` (L2), and the in-process conformance of every required fixture; refuses `--allow-reject` / `--relaxed-headers`; known rejections / known wrong results count as UNSUPPORTED, missing evidence as UNTESTED, any failure as FAILED.  Prints `REQUIREMENT <id> <status> <kind> cases=p/n specs=p/n fixtures=p/n [— first detail]` and `SUMMARY requirements version=req-v1 … acceptance=PASS|FAIL engine=<commit> hopsworks=<commit>`; `--json` writes the report with engine commit (`--engine-commit` or `git rev-parse HEAD`), Hopsworks commit and fixture provenance (from the corpus manifest), configuration (`--label` for the topology / JIT arm), per-requirement evidence and the raw case / spec results.  Exit status follows acceptance. |
| Topology mirrors `ronsql_fs_ng2r2`, `ronsql_fs_ng4r2` | `my.cnf` includes `suite/ronsql_fs/my.cnf` and overrides only the data-node count (4 / 8, NoOfReplicas 2); six one-line mirror tests `--source` the originals after `SELECT COUNT(*) AS data_nodes FROM ndbinfo.nodes` (the topology pin); `r/` generated from the base results with the pin prepended. |
| JIT mirror `ronsql_fs_jit` | `CompiledInterpreter=ON` on top of the base config; each mirror brackets the original with a fallback-delta pin (`<test>_jit_fallback_delta`, census first); strict compile arming is documented per test for when a delta reaches 0.  Results are recorded by the user (the deltas are measurements). |
| `scripts/refresh_golden_fixtures.sh` | Runs `HopsworksGoldenDump` through Maven in the Hopsworks checkout into a fresh directory, diffs provenance and file hashes against the imported corpus, and with `--apply` replaces it and runs the conformance test. |
| `adding_a_shape.md` | The one-page guide: catalog → data → cases → expectation table → specs → templates test → Java corpus → manifest → fuzzers → verify. |
| Flake controls | Schema-distribution suppressions in every test; the RDRS executor retries RonSQL retryable errors (four attempts) and now also the transient NDB dictionary conditions RDRS reports as permanent — `Schema cache for table not up to date`, `Invalid schema object version`, `Table definition has changed` — with a growing back-off (250 ms × attempt), found by the first parallel MTR run of the JIT mirror (`exec.Classify`, unit-tested); every runner (`.fs_verify`, `--vectors`, `--requirements`, `.fs_fuzz spec|envelope`) warms the RDRS dictionary serially per table before its parallel workers start (`fs_warm.go`), because the second JIT-mirror run showed the same window can crash RDRS (F20: null cached index entry in `NdbDictionaryImpl::getIndex`); `--nowarnings` for transient NDB warnings. |

## 2. Decisions / notes

- Java conformance evidence is the in-process comparison (identical to
  `TestGoldenConformance`); the cluster-side golden execution
  (`.fs_verify --golden`) remains a separate A6 check and its MTR test.
- A fixture that raises a gate on a requirement that declares no gate
  (or vice versa) is FAILED: the manifest's claim and the emitter's
  behaviour must agree.
- Requirement acceptance is expected to be FAIL on this engine: R-S6
  (F0), R-S10 snowflakes are not required but R-A2 (F7), R-A5 edge
  values (F4/F5/F6) and R-F1 (hazard) are UNSUPPORTED by design of the
  ledger.  That is the point of A7: the regression suites are green
  while the requirements report says exactly which emitted branches the
  engine does not yet serve.
- Branch requirements (R-S<n>) resolve `Shapes` on a case's PRIMARY
  shape only.  The first run resolved through `RelatedShapes` and one
  edge probe's finding (F9 temporal MIN/MAX, F7 binary projection, F6
  overflow) flipped R-S1 … R-S4, R-S7 and R-S8 to UNSUPPORTED at once,
  while the same probes are claimed explicitly by R-A5-types, R-A2-binary
  and R-F1.  Each finding is now attributed once, and every UNSUPPORTED
  row names its ledger ids (`findings=F4,F5,…`).  The regression SHAPE
  view keeps the RelatedShapes association.
- Mirrors source whole tests rather than body includes so the six
  ronsql_fs tests stay single-sourced; the `data_nodes` pin makes a
  misconfigured mirror fail loudly.

## 3. Verification (user-run)

```
cd tools/rondb-cli && go test ./internal/fsq/... ./internal/shell
cd ../../debug_build && make rondb-cli

# requirements run on the base topology (cluster started + loaded as before)
cd mysql-test
./mtr --suite=ronsql_fs ronsql_fs_smoke --start-and-exit
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis -e ".fs_load 0.01 4 500 --db test"
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_verify --requirements --all --vectors --db test --sf 0.01 --label base-interpreter --json /tmp/fs_req_base.json"

# mirror suites (the JIT suite records its deltas the first time)
./mtr --suite=ronsql_fs_ng2r2
./mtr --suite=ronsql_fs_ng4r2
./mtr --suite=ronsql_fs_jit --record
./mtr --suite=ronsql_fs,ronsql_fs_jit,ronsql_fs_ng2r2

# requirements on the other arms: start the suite's cluster, load, run with a label
./mtr --suite=ronsql_fs_jit ronsql_fs_smoke --start-and-exit
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis -e ".fs_load 0.01 4 500 --db test"
../runtime_output_directory/rondb --mysql-port 13001 --rdrs-port 13005 --no-rondis \
    -e ".fs_verify --requirements --all --vectors --db test --sf 0.01 --label jit --json /tmp/fs_req_jit.json"
```

Expected: the REQUIREMENT lines name each branch's status; acceptance
FAIL with the UNSUPPORTED rows being exactly the ledger's open findings
(F0 → R-S6, F7 → R-A2, F4/F5/F6 → R-A5-types, F1 → R-F1) and every
other emitted requirement SUPPORTED or HOPSWORKS-GATED.  Any FAILED or
UNTESTED row is a framework problem to fix.  Mirror suites green; the
JIT deltas recorded.

## 4. Results

### Run 1 — `.fs_verify --requirements --all --vectors`, base topology, interpreter (2026-09-11)

Engine `2c69712b45d2`, Hopsworks `f85a653bcafd`, sf 0.01, label
`base-interpreter`, report `/tmp/fs_req_base.json`:

`SUMMARY requirements version=req-v1 requirements=20 hopsworks-gated=4
supported=12 unsupported=4 acceptance=FAIL`

| status | requirements |
|---|---|
| SUPPORTED (12) | R-S1, R-S2, R-S3, R-S4, R-S5, R-S7, R-S8, R-S9, R-S10, R-A3-null, R-A5-composite; R-S6b (framework) |
| HOPSWORKS-GATED (4) | R-GATE-filter (3 fixtures), R-GATE-join (1), R-GATE-silent (3), R-GATE-definition (14) |
| UNSUPPORTED (4) | R-S6 `findings=F0` (the emitted CTE collect form; 0/3 cases, 0/1 spec); R-A2-binary `F7` (binary snowflake projection); R-A5-types `F4,F5,F6,F9` (7/15 edge cases: FLOAT display, DECIMAL beyond 2^53, BIGINT SUM overflow, temporal MIN/MAX JSON); R-F1 `F1` (the string re-use hazard, skipped without `--include-hazards`) |

Every L1 case, L2 spec and Java fixture the manifest names was
executed; nothing is UNTESTED or FAILED.  Acceptance is FAIL exactly
because of the open engine findings — the separation A7 asked for:
the regression suites are green while the requirements report names
the emitted branches the engine does not yet serve (F0 collect, F7
binary projections, the four type edges, the F1 hazard).

A first pass of the same run had resolved branch requirements through
`RelatedShapes` (R-S1 … R-S4, R-S7, R-S8 UNSUPPORTED via the edge
probes); the attribution fix in §2 produced the table above.

### Run 2 — mirror suites (2026-09-11): blocked by F20

The JIT mirror `ronsql_fs_jit.ronsql_fs_fuzz_env` failed twice and then
the base `ronsql_fs.ronsql_fs_fuzz_env` failed on its first attempt, all
three on **RDRS crashes** (F20, `findings/envelope_fuzz.md`): the NDB API
dictionary cache's stale-incarnation machinery from RONDB-1092
(#1013) — a null cached index entry dereferenced in
`NdbDictionaryImpl::getIndex`, and an abort in `GlobalDictCache::release`
from `releaseStaleTableReferences` at the RDRS request boundary — hit
whenever RonSQL requests follow `CREATE TABLE` while the RDRS FS-cache
thread's periodic failing `getTable(hopsworks.feature_view)` churns the
shared global cache.  The first occurrence looked like the benign
`Schema cache for table not up to date` reject (now retried) and the
JIT arm looked implicated (it was timing only).

Harness responses: the transient-error retry class and the serial RDRS
dictionary warm-up in every runner (they cover the benign face); the
crash faces cannot be avoided from the harness (no RDRS knob for the
FS-cache thread), so the E8 exit criterion (`ronsql_fs,ronsql_fs_jit,
ronsql_fs_ng2r2` green ×3) is **blocked by an engine bug**, not by the
framework: until F20 is fixed the fs suites are intermittently red and
MTR's automatic retry is the only mitigation.  The requirements run
(Run 1) is unaffected (long-lived RDRS, no DDL in the window).

**JIT census (recorded, `ronsql_fs_jit/r/`)** — the fallback-delta
pins from the `--record` run:

| test | `programs_fallback` delta |
|---|---|
| ronsql_fs_smoke | 0 |
| ronsql_fs_templates | 0 |
| ronsql_fs_golden | 0 |
| ronsql_fs_vectors | 0 |
| ronsql_fs_fuzz_spec | 24 |
| ronsql_fs_fuzz_env | 280 |

Every Hopsworks-shaped statement (the case matrix, the Java corpus, the
vector specs) runs on compiled programs — the JIT covers the whole
emitter output, so the four Hopsworks tests can be armed strict
(`jit_strict_arm.inc`) once F20 no longer makes the suite flaky.  The
fuzzers' broader statements (edge types, HAVING, probes) still fall back
(24 / 280 programs); those are the RONDB-1056 coverage gaps for the
envelope beyond Hopsworks.  All six JIT mirrors produced the same
SUMMARY / result lines as the interpreter arm: no JIT-specific
divergence in this corpus.

### F20 root cause and fix (2026-09-12, after the rebase onto the stable releases)

The rebase left the RONDB-1092 parking machinery unchanged, so F20 was
still present.  Root cause (findings/envelope_fuzz.md F20): a by-pointer
`invalidateTable(const Table*)` / `invalidateIndex(const Index*)` —
RonSQL's stale-schema unload path — was implemented as "drop the local
entry by name + release once".  After RONDB-1092 the stale entry may
already be *parked* and a newer incarnation cached under the same name,
so the call destroyed the newer entry (leaking its reference) and
released the parked reference, which `releaseStaleTableReferences()`
released again at the request boundary; the object was deleted under
another request's live pointer.  The RDRS TTL watcher's by-name
invalidate of a freshly created table (one with a TTL: `sessions_1`)
supplies the Invalid mark that starts the interleaving, which is why it
hit right after the test's `CREATE TABLE`s.

Fix (`storage/ndb/src/ndbapi/NdbDictionaryImpl.{hpp,cpp}`):
`detach_local_reference` releases the reference backing the given
pointer — the local entry if it still points at that object, else the
parked entry — exactly once, never touching another incarnation; a
pointer this Ndb no longer references is a no-op.  Null guards in
`getIndex` (`m_index`) and `park_stale_object`.  Regression test
`InvalidateParkedByPointer` in `storage/ndb/test/ndbapi/testDict.cpp`
(deterministic: park via a by-name invalidate on a second Ndb, then
invalidate the parked pointer; the current entry must survive).

First three-suite run with the fix (2026-09-11): no RDRS crash through
the run; the only failure was `ronsql_fs_ng2r2.ronsql_fs_smoke` on a
**recorded** (non-asserted) section — `SUM(dec_val)` prints
`6.0600000000000005` on 2 node groups vs `6.06` on 1 (F21, smoke.md: the
F5 DOUBLE path combines partial sums in topology order).  The
pre-generated mirror results were copied from the base topology; the
topology mirror suites therefore record their own results (`--record`),
which is the correct treatment of a section whose purpose is to record
the engine's output.  The topology pin still guards against a mirror
running on the wrong cluster.

The ng4r2 record run then surfaced **F22** (smoke.md): the BIGINT SUM
overflow probe `EDGE-big-overflow`, which asserts the clean NDB 1860
error, *succeeds* on 4 node groups — the overflow check is per-fragment
in the data node and the API-side merge adds partials unchecked; with
more fragments the probe's two rows (PK `(entity_id, seq)`) no longer
share one.  Per the user, overflow handling gets a general overhaul as a separate task;
F22 is therefore a *known* outcome: `EDGE-big-overflow` carries both the
F6 rejection and a pinned F22 wrong value, `.fs_verify` reports whichever
the topology produces as known, and the templates test records the case
instead of asserting the rejection, so `ronsql_fs_ng4r2` is green again.
The F21 DECIMAL-SUM value was removed
from the strict exact-float probe (`EDGE-float-exact`, smoke `PROBE
EDGE-FLOAT-A`) since it asserts exact floats and dates.

### Exit runs (2026-09-12): PASS

- `testDict -n InvalidateParkedByPointer T1` — `[OK]` (the F20
  regression: park through a by-name invalidate on a second Ndb, then
  invalidate the parked pointer; current entries survive, the second
  Ndb keeps working, no abort on teardown).
- `./mtr --suite=ronsql_fs,ronsql_fs_jit,ronsql_fs_ng2r2 --parallel=4
  --repeat=3` — **all green, three times**, no RDRS crash: the E8 exit
  criterion.  `ronsql_fs_ng4r2` recorded and green with F22 recorded as
  a known outcome.

## 6. E8 exit

Met: the three suites are green ×3 and the requirements report runs
(`acceptance=FAIL` by design, naming exactly the open engine findings).
Engine findings from this phase: **F20** (fixed here — the RONDB-1092
follow-up in the NDB API dictionary cache), **F21** (DECIMAL SUM on the
DOUBLE path is topology-dependent; handled in the probes), **F22**
(BIGINT SUM overflow undetected at the cross-fragment merge; known
until the overflow-handling overhaul, a separate task).  Open,
optional: the requirements reports on the JIT and ng2r2 arms
(`--label jit` / `--label ng2r2`), the batchpkread Go suite under ASAN
for F20, strict JIT arming of the four Hopsworks tests, and the E7
hazard run.
